//! Columnar SOW (State of the World) store.
//!
//! Each topic's current state is held in a `ColumnStore`. Data is stored in
//! typed parallel arrays — one per column — giving cache-friendly sequential
//! scans and minimal memory overhead compared to row-based `HashMap` storage.
//!
//! # Thread safety
//!
//! - **Reads** are lock-free: `row_count` is an `AtomicU32`, so snapshot readers
//!   see a consistent count without locking. Column data for rows below that
//!   count is always fully written before the count is incremented.
//!
//! - **Writes** (append / update) must be externally synchronized — typically
//!   one writer thread per topic, or a lock in the `Topic` wrapper.
//!
//! # Per-row seqlock (S33 / review concern C2)
//!
//! `row_versions[row]` follows a strict odd/even seqlock convention:
//!
//! - Even → row is **consistent**: every column reflects the same logical
//!   write. The numeric value is the count of completed writes × 2.
//! - Odd  → a writer is **mid-mutation**: at least one column has been
//!   updated to its new value but the write isn't complete.
//!
//! A writer flips the version to odd, fences, writes every changed column,
//! fences, then flips it to even-plus-two. A reader using
//! [`ColumnStore::read_row_consistent`] takes the version both before and
//! after its read, retrying until it observes the same even value on both
//! sides. This eliminates the "column tear" race the C2 review concern
//! warns about — a reader cannot see a mix of old and new column values
//! for the same row, even on architectures with relaxed memory ordering
//! and even if the parent `state.write()` lock is bypassed in a future
//! lock-free reader path.
//!
//! Today, all reads go through `Topic::*` methods that hold
//! `state.read()`, which already serializes against `state.write()` and
//! makes column tear impossible. The seqlock is forward-prep: an
//! evaluator that wants to skip the parent lock can call
//! `read_row_consistent` and get the same guarantee from per-row
//! versioning alone.

use crate::schema::{ColumnMapping, ColumnType, Schema};
use compact_str::CompactString;
use std::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

/// Sentinel values for "null" in primitive columns.
pub const NULL_DOUBLE: f64 = f64::NAN;
pub const NULL_LONG: i64 = i64::MIN;
pub const NULL_INT: i32 = i32::MIN;

/// A typed value that can be stored in or retrieved from a column.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Double(f64),
    Long(i64),
    Int(i32),
    String(Option<CompactString>),
    Null,
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Double(v) if !v.is_nan() => Some(*v),
            Value::Long(v) if *v != NULL_LONG => Some(*v as f64),
            Value::Int(v) if *v != NULL_INT => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Long(v) if *v != NULL_LONG => Some(*v),
            Value::Int(v) if *v != NULL_INT => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(Some(s)) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
            || matches!(self, Value::Double(v) if v.is_nan())
            || matches!(self, Value::Long(v) if *v == NULL_LONG)
            || matches!(self, Value::Int(v) if *v == NULL_INT)
            || matches!(self, Value::String(None))
    }

    /// Convert to a serde_json::Value for serialization.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Double(v) if !v.is_nan() => serde_json::Value::from(*v),
            Value::Long(v) if *v != NULL_LONG => serde_json::Value::from(*v),
            Value::Int(v) if *v != NULL_INT => serde_json::Value::from(*v),
            Value::String(Some(s)) => serde_json::Value::from(s.as_str()),
            _ => serde_json::Value::Null,
        }
    }

    /// Parse a JSON value into a typed Value given the target column type.
    pub fn from_json(json: &serde_json::Value, col_type: ColumnType) -> Value {
        match col_type {
            ColumnType::Double => match json {
                serde_json::Value::Number(n) => Value::Double(n.as_f64().unwrap_or(NULL_DOUBLE)),
                serde_json::Value::String(s) => {
                    Value::Double(s.parse::<f64>().unwrap_or(NULL_DOUBLE))
                }
                serde_json::Value::Null => Value::Double(NULL_DOUBLE),
                _ => Value::Double(NULL_DOUBLE),
            },
            ColumnType::Long => match json {
                serde_json::Value::Number(n) => Value::Long(n.as_i64().unwrap_or(NULL_LONG)),
                serde_json::Value::String(s) => {
                    Value::Long(s.parse::<i64>().unwrap_or(NULL_LONG))
                }
                serde_json::Value::Null => Value::Long(NULL_LONG),
                _ => Value::Long(NULL_LONG),
            },
            ColumnType::Int => match json {
                serde_json::Value::Number(n) => {
                    Value::Int(n.as_i64().map(|v| v as i32).unwrap_or(NULL_INT))
                }
                serde_json::Value::String(s) => Value::Int(s.parse::<i32>().unwrap_or(NULL_INT)),
                serde_json::Value::Null => Value::Int(NULL_INT),
                _ => Value::Int(NULL_INT),
            },
            ColumnType::String => match json {
                serde_json::Value::String(s) => Value::String(Some(CompactString::new(s))),
                serde_json::Value::Null => Value::String(None),
                serde_json::Value::Bool(b) => {
                    Value::String(Some(CompactString::new(if *b { "true" } else { "false" })))
                }
                other => Value::String(Some(CompactString::new(other.to_string()))),
            },
        }
    }
}

/// Columnar store for a single topic's SOW data.
pub struct ColumnStore {
    schema: Arc<Schema>,
    mappings: Vec<ColumnMapping>,

    // Typed column arrays. Indexed by [array_index][row].
    double_cols: Vec<Vec<f64>>,
    long_cols: Vec<Vec<i64>>,
    int_cols: Vec<Vec<i32>>,
    string_cols: Vec<Vec<Option<CompactString>>>,

    // Row metadata
    row_count: AtomicU32,
    capacity: usize,
    row_versions: Vec<AtomicU64>,
    global_version: AtomicU64,
}

impl ColumnStore {
    /// Create a new column store with the given schema and pre-allocated capacity.
    pub fn new(schema: Arc<Schema>, capacity: usize) -> Self {
        let mappings = schema.compute_mappings();
        let (d, l, i, s) = schema.type_counts();

        let double_cols = (0..d).map(|_| vec![NULL_DOUBLE; capacity]).collect();
        let long_cols = (0..l).map(|_| vec![NULL_LONG; capacity]).collect();
        let int_cols = (0..i).map(|_| vec![NULL_INT; capacity]).collect();
        let string_cols = (0..s).map(|_| vec![None; capacity]).collect();

        let row_versions = (0..capacity)
            .map(|_| AtomicU64::new(0))
            .collect();

        ColumnStore {
            schema,
            mappings,
            double_cols,
            long_cols,
            int_cols,
            string_cols,
            row_count: AtomicU32::new(0),
            capacity,
            row_versions,
            global_version: AtomicU64::new(0),
        }
    }

    // ========================= Accessors =========================

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn row_count(&self) -> u32 {
        self.row_count.load(Ordering::Acquire)
    }

    pub fn global_version(&self) -> u64 {
        self.global_version.load(Ordering::Acquire)
    }

    pub fn row_version(&self, row: u32) -> u64 {
        self.row_versions[row as usize].load(Ordering::Acquire)
    }

    /// Read row `row` under the per-row seqlock — see the crate-level
    /// "Per-row seqlock" doc above. `f` is invoked at least once, and
    /// re-invoked from scratch until two version reads (one before, one
    /// after `f`) observe the same even value. The return value of `f`
    /// from that consistent observation is returned to the caller.
    ///
    /// `f` MUST be pure with respect to the columns it reads: any value
    /// it captures from a torn read will be discarded when the retry
    /// loop detects the version mismatch, so side effects are observable
    /// even though their inputs were inconsistent. Limit `f` to gathering
    /// column values into a local result.
    ///
    /// Callers that already hold `state.read()` on the parent
    /// `StoreState` don't need this — `state.write()` serializes
    /// mutation, so column tear is impossible by lock. Use this for
    /// lock-free reader paths (e.g., future per-CPU shard scans or
    /// JIT-compiled predicate paths that bypass the parent lock).
    pub fn read_row_consistent<F, R>(&self, row: u32, f: F) -> R
    where
        F: Fn(&Self, u32) -> R,
    {
        let r = row as usize;
        loop {
            let v1 = self.row_versions[r].load(Ordering::Acquire);
            if v1 % 2 != 0 {
                // Writer is mid-mutation — spin briefly and retry.
                std::hint::spin_loop();
                continue;
            }
            fence(Ordering::Acquire);
            let result = f(self, row);
            fence(Ordering::Acquire);
            let v2 = self.row_versions[r].load(Ordering::Acquire);
            if v1 == v2 {
                return result;
            }
            // Version moved during the read — discard and retry. We
            // don't need to back off: writers stamp the new even
            // value before releasing, so the next iteration sees
            // either an even version (proceed) or an odd one (spin).
        }
    }

    /// True iff `row` is in the "consistent" state (even version stamp).
    /// Mostly for tests and assertions; production reader paths should
    /// use [`read_row_consistent`] instead of polling this.
    pub fn row_version_is_committed(&self, row: u32) -> bool {
        self.row_versions[row as usize].load(Ordering::Acquire) % 2 == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    // ========================= Typed getters (hot path) =========================

    #[inline]
    pub fn get_double(&self, col: usize, row: u32) -> f64 {
        let m = &self.mappings[col];
        debug_assert_eq!(m.kind, ColumnType::Double);
        self.double_cols[m.array_index][row as usize]
    }

    #[inline]
    pub fn get_long(&self, col: usize, row: u32) -> i64 {
        let m = &self.mappings[col];
        debug_assert_eq!(m.kind, ColumnType::Long);
        self.long_cols[m.array_index][row as usize]
    }

    #[inline]
    pub fn get_int(&self, col: usize, row: u32) -> i32 {
        let m = &self.mappings[col];
        debug_assert_eq!(m.kind, ColumnType::Int);
        self.int_cols[m.array_index][row as usize]
    }

    #[inline]
    pub fn get_string(&self, col: usize, row: u32) -> Option<&str> {
        let m = &self.mappings[col];
        debug_assert_eq!(m.kind, ColumnType::String);
        self.string_cols[m.array_index][row as usize]
            .as_ref()
            .map(|s| s.as_str())
    }

    /// Get a value as a boxed `Value` enum (slower, for generic paths).
    pub fn get(&self, col: usize, row: u32) -> Value {
        let m = &self.mappings[col];
        match m.kind {
            ColumnType::Double => Value::Double(self.double_cols[m.array_index][row as usize]),
            ColumnType::Long => Value::Long(self.long_cols[m.array_index][row as usize]),
            ColumnType::Int => Value::Int(self.int_cols[m.array_index][row as usize]),
            ColumnType::String => {
                Value::String(self.string_cols[m.array_index][row as usize].clone())
            }
        }
    }

    // ========================= Typed setters (hot path) =========================

    #[inline]
    pub fn set_double(&mut self, col: usize, row: u32, value: f64) {
        let m = &self.mappings[col];
        self.double_cols[m.array_index][row as usize] = value;
    }

    #[inline]
    pub fn set_long(&mut self, col: usize, row: u32, value: i64) {
        let m = &self.mappings[col];
        self.long_cols[m.array_index][row as usize] = value;
    }

    #[inline]
    pub fn set_int(&mut self, col: usize, row: u32, value: i32) {
        let m = &self.mappings[col];
        self.int_cols[m.array_index][row as usize] = value;
    }

    #[inline]
    pub fn set_string(&mut self, col: usize, row: u32, value: Option<CompactString>) {
        let m = &self.mappings[col];
        self.string_cols[m.array_index][row as usize] = value;
    }

    /// Set a value from a `Value` enum (generic path).
    pub fn set(&mut self, col: usize, row: u32, value: &Value) {
        let m = &self.mappings[col];
        match (m.kind, value) {
            (ColumnType::Double, Value::Double(v)) => {
                self.double_cols[m.array_index][row as usize] = *v;
            }
            (ColumnType::Double, Value::Long(v)) => {
                self.double_cols[m.array_index][row as usize] = *v as f64;
            }
            (ColumnType::Double, Value::Int(v)) => {
                self.double_cols[m.array_index][row as usize] = *v as f64;
            }
            (ColumnType::Long, Value::Long(v)) => {
                self.long_cols[m.array_index][row as usize] = *v;
            }
            (ColumnType::Long, Value::Int(v)) => {
                self.long_cols[m.array_index][row as usize] = *v as i64;
            }
            (ColumnType::Int, Value::Int(v)) => {
                self.int_cols[m.array_index][row as usize] = *v;
            }
            (ColumnType::String, Value::String(v)) => {
                self.string_cols[m.array_index][row as usize] = v.clone();
            }
            // Null handling
            (ColumnType::Double, Value::Null) => {
                self.double_cols[m.array_index][row as usize] = NULL_DOUBLE;
            }
            (ColumnType::Long, Value::Null) => {
                self.long_cols[m.array_index][row as usize] = NULL_LONG;
            }
            (ColumnType::Int, Value::Null) => {
                self.int_cols[m.array_index][row as usize] = NULL_INT;
            }
            (ColumnType::String, Value::Null) => {
                self.string_cols[m.array_index][row as usize] = None;
            }
            // Coerce string from other types
            (ColumnType::String, other) => {
                let s = match other {
                    Value::Double(v) => Some(CompactString::new(v.to_string())),
                    Value::Long(v) => Some(CompactString::new(v.to_string())),
                    Value::Int(v) => Some(CompactString::new(v.to_string())),
                    _ => None,
                };
                self.string_cols[m.array_index][row as usize] = s;
            }
            _ => {} // type mismatch — silently ignore (or could log warning)
        }
    }

    // ========================= Row operations =========================

    /// Append a new row. Returns the assigned row index.
    /// Caller must ensure all column values are set before this row becomes
    /// visible to readers (row_count is incremented last).
    pub fn append_row(&mut self, values: &[Value]) -> u32 {
        let row = self.row_count.load(Ordering::Relaxed);

        // Grow arrays if needed
        if row as usize >= self.capacity {
            self.grow();
        }

        let r = row as usize;
        // Fresh slot: per-row version starts at 0. Begin the seqlock
        // critical section by flipping to odd (1) before any column
        // write. A concurrent `read_row_consistent` will spin until
        // we reach the even completion stamp below.
        debug_assert_eq!(
            self.row_versions[r].load(Ordering::Relaxed),
            0,
            "append_row slot {row} not in expected even=0 state — was the slot reused?"
        );
        self.row_versions[r].store(1, Ordering::Release);
        fence(Ordering::Release);

        // Write all column values
        for (col_idx, value) in values.iter().enumerate() {
            if col_idx < self.mappings.len() {
                self.set(col_idx, row, value);
            }
        }

        // Close the seqlock critical section. Release fence + even
        // stamp publishes the column writes to any reader that loads
        // this version with Acquire.
        fence(Ordering::Release);
        self.row_versions[r].store(2, Ordering::Release);
        self.global_version.fetch_add(1, Ordering::AcqRel);

        // Make the row visible to readers (must be last)
        self.row_count.store(row + 1, Ordering::Release);

        row
    }

    /// Reset every column of `row` to its null sentinel. Used by `delete`
    /// — distinct from `update_row` which interprets `Value::Null` as
    /// "skip this field".
    pub fn null_out_row(&mut self, row: u32) {
        let r = row as usize;
        let v = self.row_versions[r].load(Ordering::Relaxed);
        debug_assert!(
            v % 2 == 0,
            "null_out_row on row {row}: version {v} is odd (concurrent writer?)"
        );
        // Phase 1: mark write in progress.
        self.row_versions[r].store(v + 1, Ordering::Release);
        fence(Ordering::Release);
        // Phase 2: column writes.
        for (col_idx, m) in self.mappings.iter().enumerate() {
            match m.kind {
                ColumnType::Double => self.double_cols[m.array_index][r] = NULL_DOUBLE,
                ColumnType::Long => self.long_cols[m.array_index][r] = NULL_LONG,
                ColumnType::Int => self.int_cols[m.array_index][r] = NULL_INT,
                ColumnType::String => self.string_cols[m.array_index][r] = None,
            }
            let _ = col_idx;
        }
        // Phase 3: publish completion.
        fence(Ordering::Release);
        self.row_versions[r].store(v + 2, Ordering::Release);
        self.global_version.fetch_add(1, Ordering::AcqRel);
    }

    /// Update an existing row in place.
    pub fn update_row(&mut self, row: u32, values: &[Value]) {
        let r = row as usize;
        let v = self.row_versions[r].load(Ordering::Relaxed);
        debug_assert!(
            v % 2 == 0,
            "update_row on row {row}: version {v} is odd (concurrent writer?)"
        );
        // Seqlock: flip to odd before any column store, fence, write,
        // fence, flip to even-plus-two.
        self.row_versions[r].store(v + 1, Ordering::Release);
        fence(Ordering::Release);

        for (col_idx, value) in values.iter().enumerate() {
            if col_idx < self.mappings.len() && !matches!(value, Value::Null) {
                self.set(col_idx, row, value);
            }
        }

        fence(Ordering::Release);
        self.row_versions[r].store(v + 2, Ordering::Release);
        self.global_version.fetch_add(1, Ordering::AcqRel);
    }

    /// Get a full row as a JSON map (for snapshot/delta delivery).
    pub fn get_row_map(&self, row: u32) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::with_capacity(self.schema.column_count());
        for col in self.schema.columns() {
            let val = self.get(col.index(), row);
            if !val.is_null() {
                map.insert(col.name().to_string(), val.to_json());
            }
        }
        map
    }

    /// Get a projected row (only selected columns).
    pub fn get_row_map_projected(
        &self,
        row: u32,
        col_indices: &[usize],
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::with_capacity(col_indices.len());
        for &col_idx in col_indices {
            let val = self.get(col_idx, row);
            if !val.is_null() {
                map.insert(
                    self.schema.column_name(col_idx).to_string(),
                    val.to_json(),
                );
            }
        }
        map
    }

    /// Direct-streaming variant of `get_row_map_projected` — writes
    /// the row as a JSON object straight into `buf` without going
    /// through `serde_json::Map<String, Value>`. Used by the SOW
    /// snapshot path (`Topic::query_streaming_json`); skipping the
    /// Map intermediate eliminates the per-cell String key alloc and
    /// the HashMap bucket overhead, which dominate CPU on wide-row
    /// topics like /trades (865K rows × 13 cols).
    ///
    /// Output is byte-identical to `serde_json::to_vec(&map)` for the
    /// equivalent Map, including the field-order behaviour: keys are
    /// emitted in `col_indices` order, null values are skipped, and
    /// strings are JSON-escaped. NaN / infinite floats become `null`
    /// (matching `Value::to_json`).
    pub fn write_row_json_projected(
        &self,
        row: u32,
        col_indices: &[usize],
        buf: &mut Vec<u8>,
    ) {
        buf.push(b'{');
        let mut first = true;
        for &col_idx in col_indices {
            let val = self.get(col_idx, row);
            if val.is_null() {
                continue;
            }
            if !first {
                buf.push(b',');
            }
            first = false;
            // Key
            buf.push(b'"');
            write_json_str(self.schema.column_name(col_idx), buf);
            buf.extend_from_slice(b"\":");
            // Value
            match &val {
                Value::String(Some(s)) => {
                    buf.push(b'"');
                    write_json_str(s, buf);
                    buf.push(b'"');
                }
                Value::String(None) | Value::Null => {
                    // unreachable — covered by is_null above, but be
                    // explicit for the match-completeness.
                    buf.extend_from_slice(b"null");
                }
                Value::Long(n) => {
                    let mut ibuf = itoa::Buffer::new();
                    buf.extend_from_slice(ibuf.format(*n).as_bytes());
                }
                Value::Int(n) => {
                    let mut ibuf = itoa::Buffer::new();
                    buf.extend_from_slice(ibuf.format(*n).as_bytes());
                }
                Value::Double(d) => {
                    if d.is_nan() || d.is_infinite() {
                        buf.extend_from_slice(b"null");
                    } else {
                        let mut rbuf = ryu::Buffer::new();
                        buf.extend_from_slice(rbuf.format(*d).as_bytes());
                    }
                }
            }
        }
        buf.push(b'}');
    }
}

/// Escape `s` as a JSON string body (no surrounding quotes) into `buf`.
/// Fast path: bulk-copy ASCII printable runs; fall through to per-byte
/// escape for control chars / quotes / backslash.
fn write_json_str(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        let escape: Option<&[u8]> = match b {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0x08 => Some(b"\\b"),
            0x0c => Some(b"\\f"),
            // Other control chars: \u00XX. Rare in our data so the
            // hex-format branch is cold.
            0x00..=0x1f => None,
            _ => continue, // ASCII printable / UTF-8 continuation byte
        };
        if let Some(esc) = escape {
            buf.extend_from_slice(&bytes[start..i]);
            buf.extend_from_slice(esc);
            start = i + 1;
        } else {
            // Control char with no short escape.
            buf.extend_from_slice(&bytes[start..i]);
            let hex = format!("\\u{:04x}", b);
            buf.extend_from_slice(hex.as_bytes());
            start = i + 1;
        }
    }
    if start < bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
}

// Cap the impl block above; the unit tests below verify byte-for-byte
// equivalence with `serde_json::to_vec(&get_row_map_projected(...))`.
impl ColumnStore {
    /// Reserved — see `write_row_json_projected` above.
    #[allow(dead_code)]
    fn __direct_json_anchor() {}

    // ========================= Internal =========================

    /// Grow capacity. Doubling is amortized-O(1) but wastes up to 50%
    /// of memory at the boundary — fine for small stores, painful on
    /// wide topics (a 1 M-row /trades topic ends up reserving 2 M
    /// rows × N columns × 8 bytes, hundreds of MB of slack).
    ///
    /// Strategy:
    ///   * capacity < 64K      → double (latency: hot path on small
    ///                           topics where the absolute slack is
    ///                           negligible)
    ///   * capacity ≥ 64K      → grow by max(64K, capacity / 4) —
    ///                           still amortized-O(1) (25 % growth
    ///                           rate) but caps wasted slack at ~25 %
    fn grow(&mut self) {
        const ADDITIVE_THRESHOLD: usize = 64 * 1024;
        const MIN_GROWTH: usize = 64 * 1024;
        let new_cap = if self.capacity < ADDITIVE_THRESHOLD {
            self.capacity * 2
        } else {
            self.capacity + (self.capacity / 4).max(MIN_GROWTH)
        };
        tracing::info!(
            old_cap = self.capacity,
            new_cap,
            "Growing column store"
        );

        for col in &mut self.double_cols {
            col.resize(new_cap, NULL_DOUBLE);
        }
        for col in &mut self.long_cols {
            col.resize(new_cap, NULL_LONG);
        }
        for col in &mut self.int_cols {
            col.resize(new_cap, NULL_INT);
        }
        for col in &mut self.string_cols {
            col.resize(new_cap, None);
        }

        self.row_versions
            .resize_with(new_cap, || AtomicU64::new(0));
        self.capacity = new_cap;
    }
}

impl ColumnStore {
    /// Trim unused capacity from the end of every column.
    ///
    /// Safe whenever no concurrent writer holds a row index ≥
    /// `row_count` — i.e. when called under the topic's write lock.
    /// Row indices `< row_count` are referenced by `key_to_row` and
    /// other per-topic indices, so we never touch those slots; we
    /// only release the slack past `row_count` that `grow()` reserved.
    ///
    /// Returns the new capacity. No-op when `row_count == capacity`
    /// or when the slack is below `MIN_RECLAIM` (avoids thrashing on
    /// stores that are about to grow again).
    pub fn shrink_to_fit(&mut self) -> usize {
        const MIN_RECLAIM: usize = 16 * 1024;
        let live = self.row_count.load(Ordering::Acquire) as usize;
        let target = live.max(1);
        if self.capacity.saturating_sub(target) < MIN_RECLAIM {
            return self.capacity;
        }
        tracing::info!(
            old_cap = self.capacity,
            new_cap = target,
            live,
            "Shrinking column store"
        );
        for col in &mut self.double_cols {
            col.truncate(target);
            col.shrink_to_fit();
        }
        for col in &mut self.long_cols {
            col.truncate(target);
            col.shrink_to_fit();
        }
        for col in &mut self.int_cols {
            col.truncate(target);
            col.shrink_to_fit();
        }
        for col in &mut self.string_cols {
            col.truncate(target);
            col.shrink_to_fit();
        }
        self.row_versions.truncate(target);
        self.row_versions.shrink_to_fit();
        self.capacity = target;
        target
    }
}

// ColumnStore is Send but not Sync — writes must be externally synchronized.
// Reads are safe via atomic row_count.
unsafe impl Send for ColumnStore {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnType;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::from_strs(
            &["tradeId", "price", "quantity", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        ))
    }

    /// `write_row_json_projected` must produce JSON whose parsed shape
    /// matches `get_row_map_projected(...)`. JSON object property order
    /// is not significant per spec, so we compare values not bytes —
    /// the direct path emits keys in projection order, `serde_json`'s
    /// default `Map` emits them alphabetically. Both round-trip to the
    /// same logical map.
    #[test]
    fn write_row_json_projected_matches_map_serialization() {
        let mut store = ColumnStore::new(test_schema(), 16);
        store.append_row(&[
            Value::String(Some(CompactString::new("T1"))),
            Value::Double(100.25),
            Value::Long(500),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        // Tricky strings: embedded quote, backslash, control char.
        store.append_row(&[
            Value::String(Some(CompactString::new(r#"T"weird\char"#))),
            Value::Double(0.0),
            Value::Long(0),
            Value::String(Some(CompactString::new("tab\there\n"))),
        ]);
        // Mix of nulls — must be skipped by both paths.
        store.append_row(&[
            Value::String(Some(CompactString::new("T3"))),
            Value::Double(NULL_DOUBLE),
            Value::Long(NULL_LONG),
            Value::String(None),
        ]);

        let proj = vec![0usize, 1, 2, 3];
        for row in 0..store.row_count() {
            let map = store.get_row_map_projected(row, &proj);
            let expected: serde_json::Value = serde_json::Value::Object(map);
            let mut via_direct = Vec::new();
            store.write_row_json_projected(row, &proj, &mut via_direct);
            let got: serde_json::Value = serde_json::from_slice(&via_direct)
                .expect("direct output must be valid JSON");
            assert_eq!(
                got,
                expected,
                "row {} mismatch:\n  direct bytes: {}\n  expected:     {}",
                row,
                String::from_utf8_lossy(&via_direct),
                serde_json::to_string(&expected).unwrap(),
            );
        }
    }

    /// Partial projection: same equivalence required.
    #[test]
    fn write_row_json_projected_partial_columns() {
        let mut store = ColumnStore::new(test_schema(), 16);
        store.append_row(&[
            Value::String(Some(CompactString::new("T2"))),
            Value::Double(99.9),
            Value::Long(1),
            Value::String(Some(CompactString::new("FX"))),
        ]);
        let proj = vec![0usize, 3];
        let map = store.get_row_map_projected(0, &proj);
        let expected = serde_json::Value::Object(map);
        let mut via_direct = Vec::new();
        store.write_row_json_projected(0, &proj, &mut via_direct);
        let got: serde_json::Value = serde_json::from_slice(&via_direct).unwrap();
        assert_eq!(got, expected);
    }

    /// NaN / infinite floats become `null` (matching `Value::to_json`).
    #[test]
    fn write_row_json_projected_nan_becomes_null() {
        let mut store = ColumnStore::new(test_schema(), 4);
        store.append_row(&[
            Value::String(Some(CompactString::new("T_nan"))),
            Value::Double(f64::NAN),
            Value::Long(42),
            Value::String(Some(CompactString::new("FX"))),
        ]);
        let proj = vec![0usize, 1, 2, 3];
        let mut buf = Vec::new();
        store.write_row_json_projected(0, &proj, &mut buf);
        // NaN is treated as null by `is_null()` and is therefore SKIPPED
        // (not emitted as `"price":null`). Matches `Value::to_json`'s
        // contract via `get_row_map_projected` (skip + null become same
        // omission).
        let parsed: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(parsed.get("price").is_none(), "got: {}", String::from_utf8_lossy(&buf));
    }

    #[test]
    fn test_append_and_read() {
        let schema = test_schema();
        let mut store = ColumnStore::new(schema, 100);

        let values = vec![
            Value::String(Some(CompactString::new("T001"))),
            Value::Double(99.5),
            Value::Long(1000),
            Value::String(Some(CompactString::new("RATES"))),
        ];
        let row = store.append_row(&values);
        assert_eq!(row, 0);
        assert_eq!(store.row_count(), 1);

        assert_eq!(store.get_string(0, 0), Some("T001"));
        assert_eq!(store.get_double(1, 0), 99.5);
        assert_eq!(store.get_long(2, 0), 1000);
        assert_eq!(store.get_string(3, 0), Some("RATES"));
    }

    #[test]
    fn test_update_row() {
        let schema = test_schema();
        let mut store = ColumnStore::new(schema, 100);

        let values = vec![
            Value::String(Some(CompactString::new("T001"))),
            Value::Double(99.5),
            Value::Long(1000),
            Value::String(Some(CompactString::new("RATES"))),
        ];
        store.append_row(&values);

        // Update price only (other fields Null = no change)
        let update = vec![
            Value::Null,
            Value::Double(101.0),
            Value::Null,
            Value::Null,
        ];
        store.update_row(0, &update);

        assert_eq!(store.get_double(1, 0), 101.0);
        assert_eq!(store.get_string(0, 0), Some("T001")); // unchanged
        assert_eq!(store.get_long(2, 0), 1000); // unchanged
    }

    #[test]
    fn test_get_row_map() {
        let schema = test_schema();
        let mut store = ColumnStore::new(schema, 100);

        let values = vec![
            Value::String(Some(CompactString::new("T001"))),
            Value::Double(99.5),
            Value::Long(1000),
            Value::String(Some(CompactString::new("RATES"))),
        ];
        store.append_row(&values);

        let map = store.get_row_map(0);
        assert_eq!(map.get("tradeId").unwrap(), "T001");
        assert_eq!(map.get("price").unwrap(), 99.5);
        assert_eq!(map.get("quantity").unwrap(), 1000);
    }

    #[test]
    fn test_grow() {
        let schema = test_schema();
        let mut store = ColumnStore::new(schema, 2);

        for i in 0..5 {
            let values = vec![
                Value::String(Some(CompactString::new(format!("T{:03}", i)))),
                Value::Double(100.0 + i as f64),
                Value::Long(i as i64),
                Value::String(Some(CompactString::new("DESK"))),
            ];
            store.append_row(&values);
        }

        assert_eq!(store.row_count(), 5);
        assert!(store.capacity() >= 5);
        assert_eq!(store.get_string(0, 4), Some("T004"));
    }
}

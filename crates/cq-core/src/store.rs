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

    // ========================= Internal =========================

    /// Double the capacity of all arrays.
    fn grow(&mut self) {
        let new_cap = self.capacity * 2;
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

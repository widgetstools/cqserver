//! Per-topic secondary indexes for equality predicates.
//!
//! A `SecondaryIndex` maintains a `Value → RoaringBitmap<row>` map per
//! indexed column. The store keeps the index in sync on every
//! mutation, and the query planner consults it when the WHERE clause
//! contains an equality on an indexed column — turning what would be
//! an O(rows) full scan into an O(matching rows) lookup.
//!
//! Limitations of this first cut:
//!   - Equality only (no range; range needs an ordered structure).
//!   - The planner only routes a *root* `col = lit` predicate through
//!     the index. AND/OR trees fall back to the full scan (the
//!     existing scan path is still O(rows) and correct).
//!   - Nulls are not indexed — they're filtered out at maintenance
//!     time. `IS NULL` predicates always go through the scan path.
//!
//! Both are easy to relax later; the index API was designed to be
//! agnostic to the planner's sophistication.
//!
//! Key representation: `IxKey` normalizes `f64` via `to_bits` (after
//! collapsing NaN) so it can be `Eq + Hash`. String keys reuse the
//! stored `CompactString` so the hot path is allocation-free.

use crate::store::{Value, NULL_DOUBLE, NULL_INT, NULL_LONG};
use compact_str::CompactString;
use roaring::RoaringBitmap;
use std::collections::{BTreeMap, HashMap};

/// Indexable value. Mirrors `Value` but is `Eq + Hash`. Returned as
/// `None` for nulls — the caller is expected to skip null
/// maintenance and route `IS NULL` to the scan path.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum IxKey {
    Int(i32),
    Long(i64),
    /// `f64::to_bits` representation, with NaN normalized away (the
    /// `from_value` constructor refuses NaN — they're treated as null).
    DoubleBits(u64),
    String(CompactString),
    Bool(bool),
    /// i64 microseconds since UNIX epoch — same representation as
    /// `Long`, but a separate variant so equality with a `Long` key
    /// at the same value doesn't accidentally collide.
    Timestamp(i64),
}

impl IxKey {
    /// Build an `IxKey` from a stored `Value`. Returns `None` for
    /// nulls (`Null` variant, `Double::NaN`, `Long::MIN_VALUE`, …) —
    /// the index never tracks null rows.
    pub fn from_value(v: &Value) -> Option<IxKey> {
        match v {
            Value::Null => None,
            Value::String(None) => None,
            Value::String(Some(s)) => Some(IxKey::String(s.clone())),
            Value::Long(n) if *n == NULL_LONG => None,
            Value::Long(n) => Some(IxKey::Long(*n)),
            Value::Int(n) if *n == NULL_INT => None,
            Value::Int(n) => Some(IxKey::Int(*n)),
            Value::Double(d) if d.is_nan() || *d == NULL_DOUBLE => None,
            Value::Double(d) => Some(IxKey::DoubleBits(d.to_bits())),
            Value::Bool(None) => None,
            Value::Bool(Some(b)) => Some(IxKey::Bool(*b)),
            Value::Timestamp(t) if *t == crate::store::NULL_TIMESTAMP => None,
            Value::Timestamp(t) => Some(IxKey::Timestamp(*t)),
            // Q10 — bytes columns aren't indexable (no canonical
            // ordering); skip nulls + index by base64 string so
            // equality lookups still work.
            Value::Bytes(None) => None,
            Value::Bytes(Some(b)) => {
                use base64::Engine;
                Some(IxKey::String(compact_str::CompactString::new(
                    base64::engine::general_purpose::STANDARD.encode(b),
                )))
            }
        }
    }
}

/// Ordered range-index key (S30). Mirrors `IxKey` but implements
/// `Ord` correctly for ranged lookups — including `f64` via a
/// total-order encoding (sign-bit flip for positives, all-bits flip
/// for negatives) so a BTreeMap walk in key order matches numeric
/// order. NaN is filtered out at maintenance time (treated as null,
/// same as `IxKey`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeKey {
    Int(i32),
    Long(i64),
    /// `f64` encoded into a u64 such that the natural `u64` order
    /// matches the `f64` total order.
    DoubleOrdered(u64),
    String(CompactString),
    /// Boolean keys order false < true (matches SQL semantics).
    Bool(bool),
    /// i64 microseconds since UNIX epoch — natural i64 ordering is
    /// chronological.
    Timestamp(i64),
}

/// Encode an `f64` into a `u64` whose `Ord` matches the `f64` total
/// order. Standard trick: flip the sign bit for non-negatives, flip
/// every bit for negatives. NaN is unsupported (the caller must
/// filter it out — see `RangeKey::from_value`).
#[inline]
fn f64_to_ordered_bits(f: f64) -> u64 {
    let bits = f.to_bits();
    if bits & (1u64 << 63) == 0 {
        // Non-negative: flip the sign bit so all positives > all negatives.
        bits ^ (1u64 << 63)
    } else {
        // Negative: flip every bit so larger magnitude maps to smaller u64.
        !bits
    }
}

impl RangeKey {
    /// Build a `RangeKey` from a stored `Value`. Returns `None` for
    /// nulls / NaN (same exclusion rules as `IxKey::from_value`).
    pub fn from_value(v: &Value) -> Option<RangeKey> {
        match v {
            Value::Null => None,
            Value::String(None) => None,
            Value::String(Some(s)) => Some(RangeKey::String(s.clone())),
            Value::Long(n) if *n == NULL_LONG => None,
            Value::Long(n) => Some(RangeKey::Long(*n)),
            Value::Int(n) if *n == NULL_INT => None,
            Value::Int(n) => Some(RangeKey::Int(*n)),
            Value::Double(d) if d.is_nan() || *d == NULL_DOUBLE => None,
            Value::Double(d) => Some(RangeKey::DoubleOrdered(f64_to_ordered_bits(*d))),
            Value::Bool(None) => None,
            Value::Bool(Some(b)) => Some(RangeKey::Bool(*b)),
            Value::Timestamp(t) if *t == crate::store::NULL_TIMESTAMP => None,
            Value::Timestamp(t) => Some(RangeKey::Timestamp(*t)),
            // Q10 — bytes: index by base64 string for equality lookup.
            Value::Bytes(None) => None,
            Value::Bytes(Some(b)) => {
                use base64::Engine;
                Some(RangeKey::String(compact_str::CompactString::new(
                    base64::engine::general_purpose::STANDARD.encode(b),
                )))
            }
        }
    }

    /// Build from a typed integer literal (i64). Used by the planner
    /// when the parsed predicate's RHS is a numeric literal — the
    /// column's actual type is decided at lookup time.
    pub fn from_long(n: i64) -> RangeKey {
        RangeKey::Long(n)
    }

    /// Build from a typed integer literal (i32).
    pub fn from_int(n: i32) -> RangeKey {
        RangeKey::Int(n)
    }

    /// Build from a typed double literal. Caller filters NaN.
    pub fn from_double(d: f64) -> Option<RangeKey> {
        if d.is_nan() {
            None
        } else {
            Some(RangeKey::DoubleOrdered(f64_to_ordered_bits(d)))
        }
    }

    /// Build from a string literal.
    pub fn from_string(s: &str) -> RangeKey {
        RangeKey::String(CompactString::new(s))
    }
}

/// Set of secondary indexes for one topic. Each indexed column gets
/// **two** parallel inner maps:
///
///   - A `HashMap<IxKey, RoaringBitmap>` for equality lookups
///     (`col = lit`, `col IN (...)`).
///   - A `BTreeMap<RangeKey, RoaringBitmap>` for ordered/range
///     lookups (`col > lit`, `col BETWEEN a AND b`) — S30. The
///     BTreeMap's natural key order matches numeric (and lex)
///     ordering, so a range walk produces rows in order.
///
/// Both maps are maintained in lockstep on every `add` / `remove`
/// call. The column set is fixed at topic construction (from config).
pub struct SecondaryIndex {
    /// Schema-column indices that are indexed. Same order as config
    /// supplies them.
    indexed_cols: Vec<usize>,
    /// Equality maps: one inner map per indexed column.
    by_col: HashMap<usize, HashMap<IxKey, RoaringBitmap>>,
    /// Range maps: one BTreeMap per indexed column. The B-tree's key
    /// ordering matches numeric ordering (see `RangeKey`), so
    /// `range(lo..=hi)` returns matching rows in column-value order.
    range_by_col: HashMap<usize, BTreeMap<RangeKey, RoaringBitmap>>,
}

impl SecondaryIndex {
    pub fn new(indexed_cols: Vec<usize>) -> Self {
        let mut by_col = HashMap::with_capacity(indexed_cols.len());
        let mut range_by_col = HashMap::with_capacity(indexed_cols.len());
        for &c in &indexed_cols {
            by_col.insert(c, HashMap::new());
            range_by_col.insert(c, BTreeMap::new());
        }
        Self {
            indexed_cols,
            by_col,
            range_by_col,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.indexed_cols.is_empty()
    }

    /// Schema columns that this index covers (by column index).
    pub fn indexed_columns(&self) -> &[usize] {
        &self.indexed_cols
    }

    /// Returns `true` if `col` is covered by this index.
    pub fn covers(&self, col: usize) -> bool {
        self.indexed_cols.iter().any(|&c| c == col)
    }

    /// Add a `(col, value) → row` mapping to BOTH the equality and
    /// range indexes. No-op for null values or uncovered columns.
    pub fn add(&mut self, col: usize, value: &Value, row: u32) {
        // Equality side.
        if let Some(inner) = self.by_col.get_mut(&col) {
            if let Some(key) = IxKey::from_value(value) {
                inner.entry(key).or_default().insert(row);
            }
        }
        // Range side.
        if let Some(inner) = self.range_by_col.get_mut(&col) {
            if let Some(rkey) = RangeKey::from_value(value) {
                inner.entry(rkey).or_default().insert(row);
            }
        }
    }

    /// Remove a `(col, value) → row` mapping from BOTH indexes.
    /// No-op for null values or uncovered columns. Empty-bitmap
    /// entries are dropped so neither map accumulates zombies
    /// under churn.
    pub fn remove(&mut self, col: usize, value: &Value, row: u32) {
        // Equality side.
        if let Some(inner) = self.by_col.get_mut(&col) {
            if let Some(key) = IxKey::from_value(value) {
                if let Some(rows) = inner.get_mut(&key) {
                    rows.remove(row);
                    if rows.is_empty() {
                        inner.remove(&key);
                    }
                }
            }
        }
        // Range side.
        if let Some(inner) = self.range_by_col.get_mut(&col) {
            if let Some(rkey) = RangeKey::from_value(value) {
                if let Some(rows) = inner.get_mut(&rkey) {
                    rows.remove(row);
                    if rows.is_empty() {
                        inner.remove(&rkey);
                    }
                }
            }
        }
    }

    /// Look up the row bitmap for `(col, value)`. Returns `None` if
    /// `col` isn't indexed, the value isn't indexable (null), or no
    /// rows have that value.
    pub fn rows_for(&self, col: usize, value: &Value) -> Option<&RoaringBitmap> {
        let inner = self.by_col.get(&col)?;
        let key = IxKey::from_value(value)?;
        inner.get(&key)
    }

    /// Same as `rows_for` but takes a pre-built `IxKey`. Used by the
    /// planner which already has the literal in hand from the parsed
    /// predicate.
    pub fn rows_for_key(&self, col: usize, key: &IxKey) -> Option<&RoaringBitmap> {
        let inner = self.by_col.get(&col)?;
        inner.get(key)
    }

    /// Query Guardrails G2: count of distinct values currently held for
    /// this column. Returns `None` when the column isn't indexed (the
    /// cost estimator falls back to "unknown" in that case and skips
    /// the row-count estimate). Cheap — just a map size lookup.
    pub fn distinct_value_count(&self, col: usize) -> Option<usize> {
        self.by_col.get(&col).map(|inner| inner.len())
    }

    /// Query Guardrails G2: total non-null row count for this column,
    /// summed across all distinct values. Returns `None` when the
    /// column isn't indexed.
    pub fn non_null_row_count(&self, col: usize) -> Option<u64> {
        let inner = self.by_col.get(&col)?;
        let mut sum = 0u64;
        for bm in inner.values() {
            sum += bm.len();
        }
        Some(sum)
    }

    /// True iff this column has a range index (S30). For now the
    /// range index is built for every indexed column, so this is
    /// just `covers(col)`. Kept as a distinct method so future
    /// configs that selectively enable range indexing can flip
    /// behavior without touching call sites.
    pub fn has_range(&self, col: usize) -> bool {
        self.range_by_col.contains_key(&col)
    }

    /// Range-lookup: returns the union of row bitmaps for every key
    /// in `[lo, hi]` (closed at both ends). `lo = None` means
    /// "from the smallest key"; `hi = None` means "to the largest."
    /// Both `None` returns every row across the indexed column —
    /// equivalent to an `IS NOT NULL` lookup.
    pub fn rows_in_range(
        &self,
        col: usize,
        lo: Option<RangeKey>,
        hi: Option<RangeKey>,
    ) -> Option<RoaringBitmap> {
        let inner = self.range_by_col.get(&col)?;
        let mut out = RoaringBitmap::new();
        let iter: Box<dyn Iterator<Item = (&RangeKey, &RoaringBitmap)>> = match (&lo, &hi) {
            (Some(l), Some(h)) => Box::new(inner.range(l.clone()..=h.clone())),
            (Some(l), None) => Box::new(inner.range(l.clone()..)),
            (None, Some(h)) => Box::new(inner.range(..=h.clone())),
            (None, None) => Box::new(inner.iter()),
        };
        for (_, bm) in iter {
            out |= bm;
        }
        Some(out)
    }

    /// Convenience: rows whose indexed-column value is strictly
    /// greater than `lo`. Implemented via `rows_in_range` with the
    /// next-larger-key trick (BTreeMap exposes only inclusive
    /// bounds via `Range<K>`; we use a half-open variant here).
    pub fn rows_greater_than(
        &self,
        col: usize,
        lo: RangeKey,
    ) -> Option<RoaringBitmap> {
        let inner = self.range_by_col.get(&col)?;
        use std::ops::Bound::{Excluded, Unbounded};
        let mut out = RoaringBitmap::new();
        for (_, bm) in inner.range((Excluded(lo), Unbounded)) {
            out |= bm;
        }
        Some(out)
    }

    /// Convenience: rows whose indexed-column value is strictly
    /// less than `hi`.
    pub fn rows_less_than(
        &self,
        col: usize,
        hi: RangeKey,
    ) -> Option<RoaringBitmap> {
        let inner = self.range_by_col.get(&col)?;
        use std::ops::Bound::{Excluded, Unbounded};
        let mut out = RoaringBitmap::new();
        for (_, bm) in inner.range((Unbounded, Excluded(hi))) {
            out |= bm;
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(Some(CompactString::new(v)))
    }

    #[test]
    fn null_values_are_not_indexed() {
        let mut idx = SecondaryIndex::new(vec![0]);
        idx.add(0, &Value::Null, 0);
        idx.add(0, &Value::String(None), 1);
        assert!(idx.rows_for(0, &Value::Null).is_none());
    }

    #[test]
    fn add_and_remove_round_trip() {
        let mut idx = SecondaryIndex::new(vec![0, 1]);
        idx.add(0, &s("AAPL"), 0);
        idx.add(0, &s("AAPL"), 1);
        idx.add(0, &s("MSFT"), 2);
        idx.add(1, &Value::Long(150), 0);
        idx.add(1, &Value::Long(150), 1);

        let rows = idx.rows_for(0, &s("AAPL")).expect("apple rows");
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(0));
        assert!(rows.contains(1));

        idx.remove(0, &s("AAPL"), 0);
        let rows = idx.rows_for(0, &s("AAPL")).expect("apple rows minus 0");
        assert_eq!(rows.len(), 1);
        assert!(rows.contains(1));

        idx.remove(0, &s("AAPL"), 1);
        // Bitmap dropped → lookup is now empty.
        assert!(idx.rows_for(0, &s("AAPL")).is_none());
    }

    #[test]
    fn uncovered_column_is_silent_noop() {
        let mut idx = SecondaryIndex::new(vec![0]);
        idx.add(1, &s("AAPL"), 0);
        assert!(idx.rows_for(1, &s("AAPL")).is_none());
    }

    #[test]
    fn double_with_nan_is_treated_as_null() {
        let mut idx = SecondaryIndex::new(vec![0]);
        idx.add(0, &Value::Double(f64::NAN), 0);
        assert!(idx.rows_for(0, &Value::Double(f64::NAN)).is_none());
    }
}

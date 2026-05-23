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
use std::collections::HashMap;

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
        }
    }
}

/// Set of secondary indexes for one topic. Internally a flat
/// `HashMap<(col_idx, IxKey), RoaringBitmap>` for the indexed columns;
/// the column set is fixed at topic construction (from config).
pub struct SecondaryIndex {
    /// Schema-column indices that are indexed. Same order as config
    /// supplies them.
    indexed_cols: Vec<usize>,
    /// One inner map per indexed column.
    by_col: HashMap<usize, HashMap<IxKey, RoaringBitmap>>,
}

impl SecondaryIndex {
    pub fn new(indexed_cols: Vec<usize>) -> Self {
        let mut by_col = HashMap::with_capacity(indexed_cols.len());
        for &c in &indexed_cols {
            by_col.insert(c, HashMap::new());
        }
        Self {
            indexed_cols,
            by_col,
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

    /// Add a `(col, value) → row` mapping. No-op for null values or
    /// uncovered columns.
    pub fn add(&mut self, col: usize, value: &Value, row: u32) {
        let Some(inner) = self.by_col.get_mut(&col) else {
            return;
        };
        let Some(key) = IxKey::from_value(value) else {
            return;
        };
        inner.entry(key).or_default().insert(row);
    }

    /// Remove a `(col, value) → row` mapping. No-op for null values
    /// or uncovered columns. The empty-bitmap entry is dropped so
    /// the map doesn't accumulate zombies under churn.
    pub fn remove(&mut self, col: usize, value: &Value, row: u32) {
        let Some(inner) = self.by_col.get_mut(&col) else {
            return;
        };
        let Some(key) = IxKey::from_value(value) else {
            return;
        };
        if let Some(rows) = inner.get_mut(&key) {
            rows.remove(row);
            if rows.is_empty() {
                inner.remove(&key);
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

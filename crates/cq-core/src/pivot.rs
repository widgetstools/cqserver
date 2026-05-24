//! Static PIVOT + UNPIVOT executors (worklog S43 / review C7 SP1+SP2).
//!
//! ## PIVOT
//!
//! `SELECT * FROM t PIVOT (SUM(qty) FOR desk IN ('A', 'B'))` transforms
//! a tall table into a wide one: anchor columns (every column except
//! `desk` and `qty`) become the row key, and the distinct `desk` values
//! listed in the IN-clause become new output columns whose values are
//! the aggregate applied to the bucket.
//!
//! - **Single-measure**: output cols = `anchor_cols ++ pivot_values`
//!   (one new column per literal, named after the literal).
//! - **Multi-measure** (`PIVOT (SUM(qty), SUM(notional) FOR desk IN ('A', 'B'))`):
//!   output cols = `anchor_cols ++ {pivot_value × agg_alias}` —
//!   named `"A_SUM(qty)"`, `"A_SUM(notional)"`, etc.
//! - **Out-of-list values**: rows whose pivot column value isn't in
//!   the literal IN-list are silently dropped (Snowflake / BigQuery
//!   convention).
//!
//! ## UNPIVOT
//!
//! `SELECT * FROM t UNPIVOT (val FOR colname IN (RATES, FX, EQUITIES))`
//! goes the other way: every input row explodes into N output rows
//! (one per listed source column), each carrying the anchor columns
//! plus two new columns: `colname = '<source_col_name>'` and
//! `val = <source_col_value>`. Rows whose source column value is NULL
//! are dropped (Snowflake default); to keep them, the user can
//! filter explicitly post-unpivot.

use std::collections::HashMap;

use compact_str::CompactString;
use serde_json::{Map, Value as JV};

use crate::query::{plan_candidates, AggState, ParsedQuery, PivotLiteral, QueryResult};
use crate::schema::ColumnType;
use crate::store::{ColumnStore, Value};

/// Anchor-key tuple: one entry per anchor column, in the order
/// `anchor_cols` declared. Hashable so we can bucket on it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AnchorKey(Vec<AnchorKeyPart>);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum AnchorKeyPart {
    Null,
    Long(i64),
    Int(i32),
    DoubleBits(u64),
    String(CompactString),
}

impl AnchorKeyPart {
    fn from_value(v: &Value) -> Self {
        use crate::store::{NULL_INT, NULL_LONG};
        match v {
            Value::Null => AnchorKeyPart::Null,
            Value::String(None) => AnchorKeyPart::Null,
            Value::String(Some(s)) => AnchorKeyPart::String(s.clone()),
            Value::Long(n) if *n == NULL_LONG => AnchorKeyPart::Null,
            Value::Long(n) => AnchorKeyPart::Long(*n),
            Value::Int(n) if *n == NULL_INT => AnchorKeyPart::Null,
            Value::Int(n) => AnchorKeyPart::Int(*n),
            Value::Double(d) if d.is_nan() => AnchorKeyPart::Null,
            Value::Double(d) => AnchorKeyPart::DoubleBits(d.to_bits()),
        }
    }

    fn to_json(&self) -> JV {
        match self {
            AnchorKeyPart::Null => JV::Null,
            AnchorKeyPart::Int(n) => JV::Number((*n as i64).into()),
            AnchorKeyPart::Long(n) => JV::Number((*n).into()),
            AnchorKeyPart::DoubleBits(b) => serde_json::Number::from_f64(f64::from_bits(*b))
                .map(JV::Number)
                .unwrap_or(JV::Null),
            AnchorKeyPart::String(s) => JV::String(s.to_string()),
        }
    }
}

/// Scan predicate-matched candidates and collect the distinct
/// values present in the pivot column. Sorted in natural order so
/// dynamic-pivot output column ordering is deterministic. (S45)
fn discover_pivot_values(query: &ParsedQuery, store: &ColumnStore) -> Vec<PivotLiteral> {
    let pivot = query.pivot.as_ref().expect("dynamic pivot requires spec");
    let schema = store.schema();
    let mut seen_strings: std::collections::BTreeSet<CompactString> =
        std::collections::BTreeSet::new();
    let mut seen_longs: std::collections::BTreeSet<i64> =
        std::collections::BTreeSet::new();
    let mut seen_double_bits: std::collections::BTreeSet<u64> =
        std::collections::BTreeSet::new();
    let candidates = plan_candidates(query, store, None);
    candidates.for_each(|row| {
        if !query.predicate.matches(store, row) {
            return;
        }
        match schema.column_type(pivot.pivot_col) {
            ColumnType::String => {
                if let Some(s) = store.get_string(pivot.pivot_col, row) {
                    seen_strings.insert(CompactString::new(s));
                }
            }
            ColumnType::Long => {
                let n = store.get_long(pivot.pivot_col, row);
                if n != crate::store::NULL_LONG {
                    seen_longs.insert(n);
                }
            }
            ColumnType::Int => {
                let n = store.get_int(pivot.pivot_col, row);
                if n != crate::store::NULL_INT {
                    seen_longs.insert(n as i64);
                }
            }
            ColumnType::Double => {
                let d = store.get_double(pivot.pivot_col, row);
                if !d.is_nan() {
                    // Sort by the f64 value, not by raw bits — to_bits
                    // preserves order only for non-negative f64. We
                    // collect via a BTreeSet keyed on bits then sort
                    // by value below.
                    seen_double_bits.insert(d.to_bits());
                }
            }
        }
    });
    match schema.column_type(pivot.pivot_col) {
        ColumnType::String => seen_strings.into_iter().map(PivotLiteral::String).collect(),
        ColumnType::Long | ColumnType::Int => {
            seen_longs.into_iter().map(PivotLiteral::Long).collect()
        }
        ColumnType::Double => {
            // Sort by the f64 value (BTreeSet sorts by bits which is
            // wrong for negatives). Stable enough for tests; produces
            // ascending ordering.
            let mut bits: Vec<u64> = seen_double_bits.into_iter().collect();
            bits.sort_by(|a, b| {
                f64::from_bits(*a)
                    .partial_cmp(&f64::from_bits(*b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            bits.into_iter().map(PivotLiteral::Double).collect()
        }
    }
}

/// Match a row's pivot column value to one of the literal IN-list
/// entries. Returns the index into `pivot_values`, or `None` if
/// the row's value isn't in the list (drop row).
fn match_pivot_value(
    store: &ColumnStore,
    pivot_col: usize,
    pivot_values: &[PivotLiteral],
    row: u32,
) -> Option<usize> {
    let schema = store.schema();
    match schema.column_type(pivot_col) {
        ColumnType::String => {
            let s = store.get_string(pivot_col, row)?;
            pivot_values.iter().position(|v| match v {
                PivotLiteral::String(target) => target.as_str() == s,
                _ => false,
            })
        }
        ColumnType::Long | ColumnType::Int => {
            let n = match schema.column_type(pivot_col) {
                ColumnType::Long => store.get_long(pivot_col, row),
                ColumnType::Int => store.get_int(pivot_col, row) as i64,
                _ => unreachable!(),
            };
            if n == crate::store::NULL_LONG {
                return None;
            }
            pivot_values.iter().position(|v| match v {
                PivotLiteral::Long(target) => *target == n,
                _ => false,
            })
        }
        ColumnType::Double => {
            let d = store.get_double(pivot_col, row);
            if d.is_nan() {
                return None;
            }
            let bits = d.to_bits();
            pivot_values.iter().position(|v| match v {
                PivotLiteral::Double(target) => *target == bits,
                _ => false,
            })
        }
    }
}

/// Output column label for `(value_idx, agg_idx)` in a (possibly
/// multi-measure) PIVOT. Single-measure: just the pivot literal's
/// label. Multi-measure: `"<literal>_<agg_alias>"`.
fn pivot_output_col_name(
    pivot: &crate::query::ParsedPivot,
    value_idx: usize,
    agg_idx: usize,
) -> String {
    let label = pivot.pivot_values[value_idx].as_column_label();
    if pivot.aggregates.len() == 1 {
        label
    } else {
        format!("{}_{}", label, pivot.aggregates[agg_idx].alias)
    }
}

/// Execute a static or dynamic PIVOT query. Returns one row per
/// distinct anchor tuple, with one column per (pivot_value × aggregate)
/// pair.
///
/// For **dynamic** pivots (`FOR col IN ANY` — S45), the executor
/// does a first pass over the predicate-matched rows to discover
/// the distinct pivot column values present in the data, then
/// runs the regular bucketing path against that discovered set.
/// The discovered values are sorted in natural order (string ascending,
/// numeric ascending) for stable output.
pub fn execute_pivot_query(query: &ParsedQuery, store: &ColumnStore) -> QueryResult {
    let pivot = query
        .pivot
        .as_ref()
        .expect("execute_pivot_query called without pivot spec");
    let schema = store.schema();

    // Dynamic PIVOT: first pass over candidates to discover the
    // distinct pivot values, then proceed with the static-list
    // bucketing logic. The discovered values become the effective
    // IN-list; the executor below reads `pivot.pivot_values`
    // unconditionally, so we build a new `ParsedPivot` with the
    // discovered list and recurse.
    if pivot.dynamic {
        let discovered = discover_pivot_values(query, store);
        if discovered.is_empty() {
            // No rows matched; nothing to project. Return an empty
            // output rather than an error.
            return QueryResult {
                rows: Vec::new(),
                total_matches: 0,
                source_rows: Vec::new(),
            };
        }
        let mut effective_pivot = pivot.clone();
        effective_pivot.pivot_values = discovered;
        effective_pivot.dynamic = false;
        let mut effective_query = query.clone();
        effective_query.pivot = Some(effective_pivot);
        return execute_pivot_query(&effective_query, store);
    }

    // Per (anchor_key, pivot_value_idx) → Vec<AggState> with one
    // slot per aggregate.
    type BucketKey = (AnchorKey, usize);
    let mut buckets: HashMap<BucketKey, Vec<AggState>> = HashMap::new();
    // Track order of first-seen anchor keys so output is deterministic.
    let mut anchor_order: Vec<AnchorKey> = Vec::new();
    let mut seen_anchors: std::collections::HashSet<AnchorKey> =
        std::collections::HashSet::new();

    // Pre-compute aggregate input column types — same shape as
    // `execute_aggregate_query`.
    let agg_col_types: Vec<Option<ColumnType>> = pivot
        .aggregates
        .iter()
        .map(|a| a.col.map(|c| schema.column_type(c)))
        .collect();

    let candidates = plan_candidates(query, store, None);
    candidates.for_each(|row| {
        if !query.predicate.matches(store, row) {
            return;
        }
        // Anchor key — recorded for EVERY row that passes the
        // predicate, even if its pivot value isn't in the IN-list.
        // Snowflake/DuckDB convention: the row's anchor still
        // surfaces in the output (with null buckets), since
        // anchors are implicitly the GROUP BY of "every column
        // except the pivot + agg inputs."
        let anchor_key = AnchorKey(
            pivot
                .anchor_cols
                .iter()
                .map(|&c| AnchorKeyPart::from_value(&store.get(c, row)))
                .collect(),
        );
        if seen_anchors.insert(anchor_key.clone()) {
            anchor_order.push(anchor_key.clone());
        }
        // Now try to match the pivot value. Out-of-list rows
        // contribute to anchor surfacing but NOT to any bucket.
        let Some(value_idx) = match_pivot_value(store, pivot.pivot_col, &pivot.pivot_values, row)
        else {
            return;
        };
        let states = buckets
            .entry((anchor_key, value_idx))
            .or_insert_with(|| {
                pivot
                    .aggregates
                    .iter()
                    .enumerate()
                    .map(|(i, a)| AggState::init(a.func, agg_col_types[i]))
                    .collect()
            });
        for (i, spec) in pivot.aggregates.iter().enumerate() {
            match spec.col {
                None => states[i].update(None), // COUNT(*)
                Some(c) => {
                    let v = store.get(c, row);
                    states[i].update(Some(&v));
                }
            }
        }
    });

    // Emit one row per anchor key (in first-seen order).
    let mut rows: Vec<Map<String, JV>> = Vec::with_capacity(anchor_order.len());
    for anchor in &anchor_order {
        let mut row_map = Map::new();
        // Anchor columns first.
        for (i, &c) in pivot.anchor_cols.iter().enumerate() {
            let col_name = schema.column_name(c).to_string();
            row_map.insert(col_name, anchor.0[i].to_json());
        }
        // One column per (pivot_value × agg) pair.
        for (vi, _value) in pivot.pivot_values.iter().enumerate() {
            for (ai, agg) in pivot.aggregates.iter().enumerate() {
                let col_name = pivot_output_col_name(pivot, vi, ai);
                let v = if let Some(states) =
                    buckets.get(&(anchor.clone(), vi))
                {
                    states[ai].finalize()
                } else {
                    // No matching rows for this (anchor, pivot_value):
                    // emit the aggregate's "no observations" value.
                    let mut empty_state = AggState::init(agg.func, agg_col_types[ai]);
                    let _ = &mut empty_state;
                    empty_state.finalize()
                };
                row_map.insert(col_name, v);
            }
        }
        rows.push(row_map);
    }

    let total = rows.len();
    QueryResult {
        rows,
        total_matches: total,
        // Pivot output rows are synthesized per anchor key, not per
        // source row. Same convention as aggregate output.
        source_rows: Vec::new(),
    }
}

/// Execute an UNPIVOT query. Each input row explodes into one output
/// row per listed source column whose value is non-null.
pub fn execute_unpivot_query(query: &ParsedQuery, store: &ColumnStore) -> QueryResult {
    let unpivot = query
        .unpivot
        .as_ref()
        .expect("execute_unpivot_query called without unpivot spec");
    let schema = store.schema();

    let candidates = plan_candidates(query, store, None);
    let mut rows: Vec<Map<String, JV>> = Vec::new();
    candidates.for_each(|row| {
        if !query.predicate.matches(store, row) {
            return;
        }
        // Pre-compute the anchor columns once per input row.
        let mut anchor_payload: Map<String, JV> = Map::with_capacity(unpivot.anchor_cols.len());
        for &c in &unpivot.anchor_cols {
            let v = store.get(c, row);
            if !v.is_null() {
                anchor_payload.insert(schema.column_name(c).to_string(), v.to_json());
            }
        }
        // Emit one row per source column whose value is non-null
        // (Snowflake's default — `INCLUDE NULLS` would change this).
        for &src in &unpivot.source_cols {
            let v = store.get(src, row);
            if v.is_null() {
                continue;
            }
            let mut out = anchor_payload.clone();
            out.insert(
                unpivot.name_col_name.clone(),
                JV::String(schema.column_name(src).to_string()),
            );
            out.insert(unpivot.value_col_name.clone(), v.to_json());
            rows.push(out);
        }
    });

    let total = rows.len();
    QueryResult {
        rows,
        total_matches: total,
        source_rows: Vec::new(),
    }
}


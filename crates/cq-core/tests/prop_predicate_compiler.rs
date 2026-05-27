//! Property tests for the predicate compiler — feeds random valid
//! SQL `WHERE` clauses through `Topic::query` and asserts:
//!
//!   1. **No panic.** Every well-formed query either compiles cleanly
//!      or returns a clean `QueryError`. The bug class we're catching:
//!      `WHERE v BETWEEN 100 AND 50` used to panic at the BTreeMap
//!      range path because `start > end` — surfaced by a hand-written
//!      e2e test in TH8. Property tests catch the FAMILY of bug, not
//!      just the one instance.
//!
//!   2. **Indexed vs non-indexed agreement.** A topic with an index
//!      on a column and an otherwise-identical topic without one must
//!      produce identical result sets for the SAME query. Any
//!      divergence is a range-index bug.
//!
//!   3. **Reference oracle for simple predicates.** For BETWEEN +
//!      strict comparison, we have a trivial in-test reference
//!      implementation (scan rows in Rust); the compiled query must
//!      agree.

use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use proptest::prelude::*;

/// Build two topics with identical schema + data: one indexed, one not.
/// Returns (indexed, plain).
fn build_pair(rows: &[(String, i64)]) -> (Topic, Topic) {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let mk = |name: &str, index: bool| {
        Topic::new(
            TopicConfig {
                name: name.into(),
                key_fields: vec!["k".into()],
                persist: false,
                conflation_ms: None,
                index_columns: if index { vec!["v".into()] } else { vec![] },
                expire_seconds: None,
            },
            schema.clone(),
            256,
        )
    };
    let indexed = mk("/p_idx", true);
    let plain = mk("/p_plain", false);
    for (k, v) in rows {
        let mut m = serde_json::Map::new();
        m.insert("k".into(), serde_json::Value::String(k.clone()));
        m.insert("v".into(), serde_json::Value::Number((*v).into()));
        indexed.upsert_map(&m).expect("idx upsert");
        plain.upsert_map(&m).expect("plain upsert");
    }
    (indexed, plain)
}

/// 1..=20 rows; key strings k00..=k19; values in [-50, 50] (allows
/// negatives so BETWEEN low > high happens naturally for some seeds).
fn rows_strategy() -> impl Strategy<Value = Vec<(String, i64)>> {
    prop::collection::vec(
        (any::<u8>().prop_map(|i| format!("k{:02}", i % 20)), (-50_i64..=50_i64)),
        1..=20,
    )
}

/// Collapse the fixture rows by key (last-write-wins). Mirrors
/// cqserver's upsert semantics so the property reference filters
/// the same data shape the server actually stores.
fn dedup_last_write(rows: &[(String, i64)]) -> Vec<(String, i64)> {
    use std::collections::HashMap;
    let mut latest: HashMap<&str, i64> = HashMap::new();
    for (k, v) in rows {
        latest.insert(k.as_str(), *v);
    }
    let mut out: Vec<(String, i64)> = latest
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]

    /// BETWEEN (any order of bounds) over Long column — must not
    /// panic, indexed and plain must agree, and the result must
    /// equal the trivial Rust filter for the SAME bounds.
    #[test]
    fn between_long_any_bounds_never_panics_and_agrees(
        rows in rows_strategy(),
        a in -75_i64..=75_i64,
        b in -75_i64..=75_i64,
    ) {
        let (idx, plain) = build_pair(&rows);
        let sql = format!("SELECT k, v FROM t WHERE v BETWEEN {a} AND {b}");
        let r_idx = idx.query(&sql).expect("indexed query");
        let r_plain = plain.query(&sql).expect("plain query");

        let rows_to_keys = |result: &cq_core::query::QueryResult| {
            let mut ks: Vec<String> = result.rows.iter()
                .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
                .collect();
            ks.sort();
            ks
        };
        let ks_idx = rows_to_keys(&r_idx);
        let ks_plain = rows_to_keys(&r_plain);
        prop_assert_eq!(&ks_idx, &ks_plain,
            "indexed vs plain disagree for BETWEEN {} AND {}", a, b);

        // Reference: cqserver collapses duplicate keys first
        // (last-write-wins), THEN filters. Mirror that ordering.
        let dedup = dedup_last_write(&rows);
        let mut ref_keys: Vec<String> = dedup.iter()
            .filter(|(_, v)| *v >= a && *v <= b)
            .map(|(k, _)| k.clone())
            .collect();
        ref_keys.sort();
        prop_assert_eq!(&ks_idx, &ref_keys,
            "result mismatches reference filter for BETWEEN {} AND {}", a, b);
    }

    /// `<` / `>` / `<=` / `>=` strict comparisons — every operator
    /// must agree between indexed/plain and the Rust reference.
    #[test]
    fn strict_comparisons_indexed_vs_plain(
        rows in rows_strategy(),
        v in -75_i64..=75_i64,
        op_idx in 0_usize..4,
    ) {
        let (idx, plain) = build_pair(&rows);
        let op = ["<", "<=", ">", ">="][op_idx];
        let sql = format!("SELECT k FROM t WHERE v {op} {v}");
        let r_idx = idx.query(&sql).expect("idx");
        let r_plain = plain.query(&sql).expect("plain");

        let mut ks_idx: Vec<String> = r_idx.rows.iter()
            .filter_map(|r| r.get("k").and_then(|x| x.as_str()).map(String::from))
            .collect();
        ks_idx.sort();
        let mut ks_plain: Vec<String> = r_plain.rows.iter()
            .filter_map(|r| r.get("k").and_then(|x| x.as_str()).map(String::from))
            .collect();
        ks_plain.sort();
        prop_assert_eq!(&ks_idx, &ks_plain, "{op}: idx vs plain", op = op);

        let pred: fn(i64, i64) -> bool = match op {
            "<" => |a, b| a < b,
            "<=" => |a, b| a <= b,
            ">" => |a, b| a > b,
            ">=" => |a, b| a >= b,
            _ => unreachable!(),
        };
        // Last-write-wins dedup first, then filter (mirror cqserver).
        let dedup = dedup_last_write(&rows);
        let mut ref_keys: Vec<String> = dedup.iter()
            .filter(|(_, val)| pred(*val, v))
            .map(|(k, _)| k.clone())
            .collect();
        ref_keys.sort();
        prop_assert_eq!(&ks_idx, &ref_keys, "{op} {v}: vs reference", op = op, v = v);
    }

    /// IS NULL / IS NOT NULL on indexed column — neither variant
    /// should panic; must match a column that has been published
    /// to (all non-null per fixture).
    #[test]
    fn is_null_predicates_never_panic(
        rows in rows_strategy(),
    ) {
        let (idx, plain) = build_pair(&rows);
        for sql in [
            "SELECT k FROM t WHERE v IS NULL",
            "SELECT k FROM t WHERE v IS NOT NULL",
        ] {
            let r_idx = idx.query(sql).expect(sql);
            let r_plain = plain.query(sql).expect(sql);
            prop_assert_eq!(r_idx.rows.len(), r_plain.rows.len(),
                            "{} idx vs plain", sql);
        }
    }

    /// AND / OR compositions over the indexed column — both engines
    /// must agree.
    #[test]
    fn and_or_compositions_indexed_vs_plain(
        rows in rows_strategy(),
        a in -50_i64..=50_i64,
        b in -50_i64..=50_i64,
        use_or in any::<bool>(),
    ) {
        let (idx, plain) = build_pair(&rows);
        let connector = if use_or { "OR" } else { "AND" };
        let sql = format!("SELECT k FROM t WHERE v >= {a} {connector} v < {b}");
        let r_idx = idx.query(&sql).expect("idx");
        let r_plain = plain.query(&sql).expect("plain");
        let mut ks_idx: Vec<String> = r_idx.rows.iter()
            .filter_map(|r| r.get("k").and_then(|x| x.as_str()).map(String::from))
            .collect();
        let mut ks_plain: Vec<String> = r_plain.rows.iter()
            .filter_map(|r| r.get("k").and_then(|x| x.as_str()).map(String::from))
            .collect();
        ks_idx.sort();
        ks_plain.sort();
        prop_assert_eq!(ks_idx, ks_plain,
            "AND/OR composition disagrees: WHERE v >= {} {} v < {}", a, connector, b);
    }
}

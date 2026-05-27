//! Property tests for Q7 window functions.
//!
//! Each property generates a random fixture (small fan-out so
//! shrinking is fast), executes the windowed query via cqserver,
//! and compares against a Rust-iterator reference implementation
//! that re-derives the expected per-row value the obvious way.
//!
//! Properties checked:
//!
//! 1. `ROW_NUMBER OVER (PARTITION BY p ORDER BY o ASC)` — within
//!    each partition, the assigned numbers are exactly `1..=N`
//!    (a permutation). Across all rows, no two rows in the same
//!    partition share a row_number; every partition's max
//!    row_number equals the partition's row count.
//!
//! 2. `RANK OVER (PARTITION BY p ORDER BY o)` is monotonic
//!    non-decreasing within each partition when iterated in
//!    order_by ascending order, and ties at the same `o` value
//!    receive the same rank.
//!
//! 3. `DENSE_RANK OVER (PARTITION BY p ORDER BY o)` is also
//!    monotonic non-decreasing and tie-equal, AND consecutive
//!    distinct `o` values receive consecutive integer ranks (no
//!    gaps). For a partition with K distinct `o` values, the
//!    maximum dense_rank is exactly K.
//!
//! Run with:
//!   cargo test --release --test prop_window_fns
//!   PROPTEST_CASES=500 cargo test --release --test prop_window_fns

use compact_str::CompactString;
use cq_core::query::{execute_query, parse_query};
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};
use proptest::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

/// One row in the fixture: (partition_key, order_value).
type Row = (String, i64);

fn fixture_strategy() -> impl Strategy<Value = Vec<Row>> {
    // Keep cardinality small so shrinking stays cheap:
    // - 1..=4 partitions
    // - 1..=8 rows per partition
    // - order_value in 0..=20 (lots of ties possible)
    prop::collection::vec(
        (
            prop_oneof!["P0", "P1", "P2", "P3"].prop_map(String::from),
            0i64..=20,
        ),
        1..=24,
    )
}

fn build_store(rows: &[Row]) -> (Arc<Schema>, ColumnStore) {
    let s = Arc::new(Schema::from_strs(
        &["p", "o"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(s.clone(), 32);
    for (p, o) in rows {
        store.append_row(&[
            Value::String(Some(CompactString::new(p))),
            Value::Long(*o),
        ]);
    }
    (s, store)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_shrink_iters: 1024,
        .. ProptestConfig::default()
    })]

    /// ROW_NUMBER per partition is exactly `1..=N` (permutation).
    #[test]
    fn row_number_is_partition_permutation(rows in fixture_strategy()) {
        let (schema, store) = build_store(&rows);
        let q = parse_query(
            "SELECT p, o, ROW_NUMBER() OVER (PARTITION BY p ORDER BY o ASC) AS rn FROM t",
            &schema,
        ).expect("parse");
        let r = execute_query(&q, &store);

        // Group output rows by partition; collect the rn values.
        let mut by_p: HashMap<String, Vec<u64>> = HashMap::new();
        for row in &r.rows {
            let p = row.get("p").unwrap().as_str().unwrap().to_string();
            let rn = row.get("rn").unwrap().as_u64().unwrap();
            by_p.entry(p).or_default().push(rn);
        }

        // Reference: per-partition row counts.
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for (p, _) in &rows {
            *counts.entry(p.as_str()).or_insert(0) += 1;
        }

        for (p, mut rns) in by_p {
            let expected_count = counts[p.as_str()];
            prop_assert_eq!(rns.len(), expected_count, "partition `{}` row count", p);
            rns.sort();
            let expected: Vec<u64> = (1..=expected_count as u64).collect();
            prop_assert_eq!(rns, expected, "ROW_NUMBER not 1..=N for partition `{}`", p);
        }
    }

    /// RANK ties at equal `o` values; monotonic non-decreasing
    /// when iterated in ORDER BY ASC.
    #[test]
    fn rank_ties_equal_and_monotonic(rows in fixture_strategy()) {
        let (schema, store) = build_store(&rows);
        let q = parse_query(
            "SELECT p, o, RANK() OVER (PARTITION BY p ORDER BY o ASC) AS rk FROM t",
            &schema,
        ).expect("parse");
        let r = execute_query(&q, &store);

        // Group by partition: list of (o, rank).
        let mut by_p: HashMap<String, Vec<(i64, u64)>> = HashMap::new();
        for row in &r.rows {
            let p = row.get("p").unwrap().as_str().unwrap().to_string();
            let o = row.get("o").unwrap().as_i64().unwrap();
            let rk = row.get("rk").unwrap().as_u64().unwrap();
            by_p.entry(p).or_default().push((o, rk));
        }

        for (p, mut entries) in by_p {
            entries.sort_by_key(|(o, _)| *o);
            // Monotonic non-decreasing.
            for w in entries.windows(2) {
                prop_assert!(
                    w[0].1 <= w[1].1,
                    "RANK not monotonic in partition `{}`: {:?}",
                    p, entries
                );
            }
            // Ties: same `o` → same rank.
            for w in entries.windows(2) {
                if w[0].0 == w[1].0 {
                    prop_assert_eq!(
                        w[0].1,
                        w[1].1,
                        "RANK tie violated in `{}` at o={}",
                        p,
                        w[0].0
                    );
                }
            }
            // First rank is 1.
            prop_assert_eq!(entries[0].1, 1, "first RANK in `{}` should be 1", p);
        }
    }

    /// DENSE_RANK ties + gap-free: K distinct `o` values per
    /// partition → max dense_rank == K.
    #[test]
    fn dense_rank_no_gaps(rows in fixture_strategy()) {
        let (schema, store) = build_store(&rows);
        let q = parse_query(
            "SELECT p, o, DENSE_RANK() OVER (PARTITION BY p ORDER BY o ASC) AS dr FROM t",
            &schema,
        ).expect("parse");
        let r = execute_query(&q, &store);

        let mut by_p: HashMap<String, Vec<(i64, u64)>> = HashMap::new();
        for row in &r.rows {
            let p = row.get("p").unwrap().as_str().unwrap().to_string();
            let o = row.get("o").unwrap().as_i64().unwrap();
            let dr = row.get("dr").unwrap().as_u64().unwrap();
            by_p.entry(p).or_default().push((o, dr));
        }

        for (p, mut entries) in by_p {
            entries.sort_by_key(|(o, _)| *o);
            // Distinct `o` count = expected max dense_rank.
            let distinct: std::collections::BTreeSet<i64> =
                entries.iter().map(|(o, _)| *o).collect();
            let expected_max = distinct.len() as u64;
            let actual_max = entries.iter().map(|(_, d)| *d).max().unwrap();
            prop_assert_eq!(
                expected_max,
                actual_max,
                "DENSE_RANK max in `{}` should equal distinct-o count; got entries {:?}",
                p,
                entries
            );
            // Ties + monotonic.
            for w in entries.windows(2) {
                prop_assert!(w[0].1 <= w[1].1);
                if w[0].0 == w[1].0 {
                    prop_assert_eq!(w[0].1, w[1].1);
                }
            }
        }
    }

    /// LAG(o, 1) within a partition matches the previous row's `o`
    /// in ORDER BY ASC traversal. With ties (multiple rows sharing
    /// the same `o`), the executor still preserves the sort order
    /// — so the multiset of `(o, lag)` pairs from cqserver must
    /// equal the multiset produced by sorted-iteration of the
    /// reference rows.
    #[test]
    fn lag_returns_previous_value(rows in fixture_strategy()) {
        let (schema, store) = build_store(&rows);
        let q = parse_query(
            "SELECT p, o, LAG(o, 1) OVER (PARTITION BY p ORDER BY o ASC) AS prev FROM t",
            &schema,
        ).expect("parse");
        let r = execute_query(&q, &store);

        // Reference: per partition, walk sorted; row i has LAG =
        // sorted[i-1] (or null for i==0). The multiset of
        // (o, lag) pairs is what we'll compare.
        let mut ref_by_p: HashMap<String, Vec<i64>> = HashMap::new();
        for (p, o) in &rows {
            ref_by_p.entry(p.clone()).or_default().push(*o);
        }
        let mut ref_pairs: HashMap<String, Vec<(i64, Option<i64>)>> = HashMap::new();
        for (p, vals) in ref_by_p {
            let mut sorted = vals;
            sorted.sort();
            let mut pairs = Vec::with_capacity(sorted.len());
            for (i, o) in sorted.iter().enumerate() {
                let lag = if i == 0 { None } else { Some(sorted[i - 1]) };
                pairs.push((*o, lag));
            }
            pairs.sort();
            ref_pairs.insert(p, pairs);
        }

        let mut actual_pairs: HashMap<String, Vec<(i64, Option<i64>)>> = HashMap::new();
        for row in &r.rows {
            let p = row.get("p").unwrap().as_str().unwrap().to_string();
            let o = row.get("o").unwrap().as_i64().unwrap();
            let prev = row.get("prev").and_then(|v| v.as_i64());
            actual_pairs.entry(p).or_default().push((o, prev));
        }
        for v in actual_pairs.values_mut() {
            v.sort();
        }
        prop_assert_eq!(actual_pairs, ref_pairs, "LAG (o, lag) multiset mismatch");
    }
}

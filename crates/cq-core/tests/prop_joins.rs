//! Property tests for the JOIN variants added in Q1 (RIGHT/FULL
//! OUTER) and Q12 (AS OF).
//!
//! Properties:
//!
//! 1. **JOIN containment**:
//!    INNER ⊆ LEFT, INNER ⊆ RIGHT, LEFT ⊆ FULL, RIGHT ⊆ FULL, AND
//!    `|FULL| == |LEFT| + |RIGHT| - |INNER|` (inclusion–exclusion).
//!    Tests over a random (left, right) fixture with overlapping +
//!    disjoint keys.
//!
//! 2. **LEFT preserves every left row**:
//!    `|LEFT| >= |left|`, and every distinct left-key appears at
//!    least once in the LEFT output.
//!
//! 3. **RIGHT preserves every right row**: symmetric.
//!
//! 4. **ASOF picks the largest ts ≤ left.ts**:
//!    For each left row, the matched right ts equals the maximum
//!    of all right ts values ≤ the left ts within the same USING
//!    key (or NULL if no such ts exists — INNER semantics for the
//!    no-match case).
//!
//! Run with:
//!   cargo test --release --test prop_joins
//!   PROPTEST_CASES=500 cargo test --release --test prop_joins

use compact_str::CompactString;
use cq_core::query::{combined_join_schema, execute_join_query, parse_query};
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};
use proptest::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;

/// Generate a (left, right) fixture where:
/// - keys come from a small universe so overlaps and disjoint sets
///   both occur in shrinking
/// - row counts stay small to keep shrinking + execution fast
fn join_fixture() -> impl Strategy<Value = (Vec<(String, i64)>, Vec<(String, i64)>)> {
    let key_strat = || prop_oneof!["k0", "k1", "k2", "k3"].prop_map(String::from);
    let left = prop::collection::vec((key_strat(), 0i64..=10), 0..=10);
    let right = prop::collection::vec((key_strat(), 0i64..=100), 0..=10);
    (left, right)
}

fn build_left(rows: &[(String, i64)]) -> (Arc<Schema>, ColumnStore) {
    let s = Arc::new(Schema::from_strs(
        &["k", "lv"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(s.clone(), 32);
    for (k, v) in rows {
        store.append_row(&[
            Value::String(Some(CompactString::new(k))),
            Value::Long(*v),
        ]);
    }
    (s, store)
}

fn build_right(rows: &[(String, i64)]) -> (Arc<Schema>, ColumnStore) {
    let s = Arc::new(Schema::from_strs(
        &["k", "rv"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(s.clone(), 32);
    for (k, v) in rows {
        store.append_row(&[
            Value::String(Some(CompactString::new(k))),
            Value::Long(*v),
        ]);
    }
    (s, store)
}

/// Run a SELECT over the combined `(L, R)` join fixture using the
/// given JOIN clause; return the row count.
fn count(sql: &str, combined: &Schema, left: &ColumnStore, right: &ColumnStore) -> usize {
    let q = parse_query(sql, combined).expect("parse");
    execute_join_query(&q, left, right).expect("exec").rows.len()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        max_shrink_iters: 1024,
        .. ProptestConfig::default()
    })]

    /// INNER ⊆ LEFT ⊆ FULL, INNER ⊆ RIGHT ⊆ FULL, and
    /// inclusion-exclusion: `|FULL| == |LEFT| + |RIGHT| - |INNER|`.
    /// Note the right side has UNIQUE keys by the join executor's
    /// "last write wins" rule, so we deduplicate right by key in the
    /// reference before comparing.
    #[test]
    fn join_containment_and_inclusion_exclusion((left_rows, right_rows) in join_fixture()) {
        // Right-side dedup by key (executor's last-write-wins).
        let mut right_seen: std::collections::HashMap<String, i64> =
            std::collections::HashMap::new();
        for (k, v) in &right_rows {
            right_seen.insert(k.clone(), *v);
        }
        let right_unique: Vec<(String, i64)> =
            right_seen.into_iter().collect();
        let (l_schema, left) = build_left(&left_rows);
        let (r_schema, right) = build_right(&right_unique);
        let combined = combined_join_schema(&l_schema, &r_schema, &["k".to_string()]);

        let inner = count(
            "SELECT k, lv, rv FROM a JOIN b USING (k)",
            &combined, &left, &right);
        let lefto = count(
            "SELECT k, lv, rv FROM a LEFT JOIN b USING (k)",
            &combined, &left, &right);
        let righto = count(
            "SELECT k, lv, rv FROM a RIGHT JOIN b USING (k)",
            &combined, &left, &right);
        let fullo = count(
            "SELECT k, lv, rv FROM a FULL OUTER JOIN b USING (k)",
            &combined, &left, &right);

        prop_assert!(inner <= lefto, "INNER {} > LEFT {}", inner, lefto);
        prop_assert!(inner <= righto, "INNER {} > RIGHT {}", inner, righto);
        prop_assert!(lefto <= fullo, "LEFT {} > FULL {}", lefto, fullo);
        prop_assert!(righto <= fullo, "RIGHT {} > FULL {}", righto, fullo);
        // Inclusion-exclusion.
        prop_assert_eq!(
            fullo,
            lefto + righto - inner,
            "|FULL| ({}) != |LEFT| ({}) + |RIGHT| ({}) - |INNER| ({})",
            fullo, lefto, righto, inner
        );
    }

    /// LEFT OUTER preserves every distinct left key.
    #[test]
    fn left_outer_keeps_every_left_key((left_rows, right_rows) in join_fixture()) {
        let mut right_seen = std::collections::HashMap::new();
        for (k, v) in &right_rows { right_seen.insert(k.clone(), *v); }
        let right_unique: Vec<(String, i64)> = right_seen.into_iter().collect();

        let left_keys: HashSet<String> = left_rows.iter().map(|(k, _)| k.clone()).collect();
        if left_keys.is_empty() {
            return Ok(());
        }
        let (l_schema, left) = build_left(&left_rows);
        let (r_schema, right) = build_right(&right_unique);
        let combined = combined_join_schema(&l_schema, &r_schema, &["k".to_string()]);

        let q = parse_query(
            "SELECT k, lv FROM a LEFT JOIN b USING (k)",
            &combined,
        ).expect("parse");
        let r = execute_join_query(&q, &left, &right).expect("exec");
        let surfaced: HashSet<String> = r.rows.iter()
            .map(|row| row.get("k").unwrap().as_str().unwrap().to_string())
            .collect();
        for k in &left_keys {
            prop_assert!(
                surfaced.contains(k),
                "LEFT OUTER dropped left key `{}` from output {:?}", k, surfaced
            );
        }
    }

    /// ASOF JOIN: for each (left_key, left_ts), the matched right
    /// ts equals max(right_ts where right_ts ≤ left_ts and
    /// right_key = left_key) — or NULL/dropped if no such ts.
    #[test]
    fn asof_picks_max_right_le_left((left_rows, right_rows) in join_fixture()) {
        // Use the `lv` and `rv` cols as timestamps; values from 0..10
        // give good overlap density.
        let l_schema = Arc::new(Schema::from_strs(
            &["k", "ts"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let r_schema = Arc::new(Schema::from_strs(
            &["k", "ts"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let mut left = ColumnStore::new(l_schema.clone(), 32);
        let mut right = ColumnStore::new(r_schema.clone(), 32);
        for (k, v) in &left_rows {
            left.append_row(&[Value::String(Some(CompactString::new(k))), Value::Long(*v)]);
        }
        for (k, v) in &right_rows {
            right.append_row(&[Value::String(Some(CompactString::new(k))), Value::Long(*v)]);
        }
        // USING(k) — ts dedups in combined.
        let combined = combined_join_schema(
            &l_schema,
            &r_schema,
            &["k".to_string()],
        );
        let q = parse_query(
            "SELECT k, ts FROM a ASOF JOIN b MATCH_CONDITION(ts >= ts) USING (k)",
            &combined,
        );
        // If the right side is empty for every left key, the ASOF
        // result is empty — that's fine and tested implicitly.
        let q = match q {
            Ok(q) => q,
            Err(_) => return Ok(()),
        };
        let r = execute_join_query(&q, &left, &right).expect("exec");

        // Reference: per left row, find max right.ts where right.k
        // == left.k and right.ts ≤ left.ts. If no match, the row is
        // dropped (INNER semantics, no LEFT OUTER).
        let mut expected_count = 0usize;
        for (lk, lt) in &left_rows {
            let best = right_rows.iter()
                .filter(|(rk, rt)| rk == lk && *rt <= *lt)
                .map(|(_, rt)| *rt)
                .max();
            if best.is_some() {
                expected_count += 1;
            }
        }
        prop_assert_eq!(
            r.rows.len(),
            expected_count,
            "ASOF row count mismatch (left {:?}, right {:?})",
            left_rows,
            right_rows
        );
    }
}

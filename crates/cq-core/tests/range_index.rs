//! S30 — SOW range index tests.
//!
//! Verifies that `SecondaryIndex`'s BTreeMap-backed range lookups
//! return exactly the same rows a full-scan with the same predicate
//! would emit, and that range queries through `Topic::query` route
//! through the index (catching planner regressions).

use std::sync::Arc;

use cq_core::sec_index::{RangeKey, SecondaryIndex};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use roaring::RoaringBitmap;
use serde_json::{json, Map};

// =========================================================================
// Low-level unit tests against `SecondaryIndex` directly
// =========================================================================

#[test]
fn range_index_returns_inclusive_between() {
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Long(10), 0);
    ix.add(0, &Value::Long(20), 1);
    ix.add(0, &Value::Long(30), 2);
    ix.add(0, &Value::Long(40), 3);

    let got = ix
        .rows_in_range(0, Some(RangeKey::Long(15)), Some(RangeKey::Long(35)))
        .expect("range hit");
    // 20 and 30 ∈ [15, 35]. 10 < 15 and 40 > 35 are excluded.
    let want: RoaringBitmap = [1u32, 2u32].into_iter().collect();
    assert_eq!(got, want);
}

#[test]
fn range_index_greater_than_is_strict() {
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Long(10), 0);
    ix.add(0, &Value::Long(20), 1);
    ix.add(0, &Value::Long(30), 2);

    let got = ix.rows_greater_than(0, RangeKey::Long(20)).expect("hit");
    let want: RoaringBitmap = [2u32].into_iter().collect();
    assert_eq!(got, want, "20 itself should NOT be in `> 20`");
}

#[test]
fn range_index_less_than_is_strict() {
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Long(10), 0);
    ix.add(0, &Value::Long(20), 1);
    ix.add(0, &Value::Long(30), 2);

    let got = ix.rows_less_than(0, RangeKey::Long(20)).expect("hit");
    let want: RoaringBitmap = [0u32].into_iter().collect();
    assert_eq!(got, want, "20 itself should NOT be in `< 20`");
}

#[test]
fn range_index_ge_le_are_inclusive() {
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Long(10), 0);
    ix.add(0, &Value::Long(20), 1);
    ix.add(0, &Value::Long(30), 2);

    // `>= 20` = rows_in_range(Some(20), None).
    let ge20 = ix
        .rows_in_range(0, Some(RangeKey::Long(20)), None)
        .expect("hit");
    let want_ge: RoaringBitmap = [1u32, 2u32].into_iter().collect();
    assert_eq!(ge20, want_ge);

    // `<= 20` = rows_in_range(None, Some(20)).
    let le20 = ix
        .rows_in_range(0, None, Some(RangeKey::Long(20)))
        .expect("hit");
    let want_le: RoaringBitmap = [0u32, 1u32].into_iter().collect();
    assert_eq!(le20, want_le);
}

#[test]
fn range_index_handles_negative_doubles_correctly() {
    // Total-ordering encoding for f64 — negatives must sort below
    // positives, and larger-magnitude negatives must sort below
    // smaller-magnitude ones.
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Double(-100.0), 0);
    ix.add(0, &Value::Double(-1.0), 1);
    ix.add(0, &Value::Double(0.0), 2);
    ix.add(0, &Value::Double(1.0), 3);
    ix.add(0, &Value::Double(100.0), 4);

    let lo = RangeKey::from_double(-2.0).unwrap();
    let hi = RangeKey::from_double(2.0).unwrap();
    let got = ix.rows_in_range(0, Some(lo), Some(hi)).expect("hit");
    // -1.0, 0.0, 1.0 ∈ [-2, 2]. -100 and 100 excluded.
    let want: RoaringBitmap = [1u32, 2u32, 3u32].into_iter().collect();
    assert_eq!(got, want);
}

#[test]
fn range_index_drops_empty_buckets_after_removes() {
    let mut ix = SecondaryIndex::new(vec![0]);
    use cq_core::store::Value;
    ix.add(0, &Value::Long(10), 0);
    ix.add(0, &Value::Long(20), 1);
    ix.add(0, &Value::Long(20), 2);
    // Remove the only row at 10 — that bucket must be dropped.
    ix.remove(0, &Value::Long(10), 0);
    // Remove one of the two rows at 20 — bucket stays.
    ix.remove(0, &Value::Long(20), 1);

    let got = ix
        .rows_in_range(0, None, Some(RangeKey::Long(20)))
        .expect("hit");
    let want: RoaringBitmap = [2u32].into_iter().collect();
    assert_eq!(got, want);
}

// =========================================================================
// End-to-end: range queries via Topic::query should match a full scan
// =========================================================================

fn make_topic(index_cols: Vec<String>) -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price", "qty"],
        &[ColumnType::String, ColumnType::Double, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/range-test".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: index_cols,
            expire_seconds: None,
        },
        schema,
        1024,
    )
}

fn publish(topic: &Topic, sym: &str, price: f64, qty: i64) {
    let mut m = Map::new();
    m.insert("symbol".into(), json!(sym));
    m.insert("price".into(), json!(price));
    m.insert("qty".into(), json!(qty));
    topic.upsert_map(&m).expect("publish");
}

#[test]
fn topic_query_between_returns_same_rows_as_full_scan() {
    // Two topics, same data, same query — one with `price` indexed,
    // one without. Results must be identical.
    let indexed = make_topic(vec!["price".into()]);
    let unindexed = make_topic(vec![]);
    for (i, p) in (1..=50).map(|i| (i, i as f64 * 10.0)) {
        publish(&indexed, &format!("S{i:03}"), p, i as i64);
        publish(&unindexed, &format!("S{i:03}"), p, i as i64);
    }
    let sql = "SELECT symbol, price FROM t WHERE price BETWEEN 150 AND 250";
    let ix_rows = indexed.query(sql).expect("indexed");
    let no_rows = unindexed.query(sql).expect("unindexed");

    let ix_set: std::collections::BTreeSet<String> = ix_rows
        .rows
        .iter()
        .filter_map(|r| r.get("symbol").and_then(|v| v.as_str()).map(String::from))
        .collect();
    let no_set: std::collections::BTreeSet<String> = no_rows
        .rows
        .iter()
        .filter_map(|r| r.get("symbol").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert_eq!(ix_set, no_set);
    // Sanity: BETWEEN 150..=250 on a 10×i sequence covers i=15..=25.
    assert_eq!(ix_set.len(), 11);
}

#[test]
fn topic_query_gt_lt_routes_through_range_index() {
    let topic = make_topic(vec!["qty".into()]);
    for i in 1..=20 {
        publish(&topic, &format!("S{i:03}"), 100.0, i as i64);
    }
    // `> 15` → 16, 17, 18, 19, 20 (5 rows).
    let gt = topic
        .query("SELECT symbol FROM t WHERE qty > 15")
        .expect("gt query");
    assert_eq!(gt.rows.len(), 5);
    // `< 5` → 1, 2, 3, 4 (4 rows).
    let lt = topic
        .query("SELECT symbol FROM t WHERE qty < 5")
        .expect("lt query");
    assert_eq!(lt.rows.len(), 4);
}

#[test]
fn range_index_stays_in_sync_after_updates_and_deletes() {
    let topic = make_topic(vec!["price".into()]);
    publish(&topic, "AAPL", 100.0, 10);
    publish(&topic, "MSFT", 200.0, 20);
    publish(&topic, "GOOG", 300.0, 30);

    // BETWEEN 150 AND 250 → MSFT only.
    let q1 = topic
        .query("SELECT symbol FROM t WHERE price BETWEEN 150 AND 250")
        .expect("q1");
    assert_eq!(q1.rows.len(), 1);
    assert_eq!(q1.rows[0].get("symbol").unwrap(), "MSFT");

    // Update MSFT's price to 50 — should drop out of [150, 250]
    // and into [< 100].
    publish(&topic, "MSFT", 50.0, 20);
    let q2 = topic
        .query("SELECT symbol FROM t WHERE price BETWEEN 150 AND 250")
        .expect("q2");
    assert!(q2.rows.is_empty(), "MSFT moved out of range");

    let q3 = topic
        .query("SELECT symbol FROM t WHERE price < 100")
        .expect("q3");
    let syms: std::collections::BTreeSet<&str> = q3
        .rows
        .iter()
        .filter_map(|r| r.get("symbol").and_then(|v| v.as_str()))
        .collect();
    assert!(syms.contains("MSFT"), "MSFT's new price 50 should be in `< 100`");

    // Delete AAPL — should disappear from any range that previously covered it.
    topic.delete("AAPL").expect("delete");
    let q4 = topic
        .query("SELECT symbol FROM t WHERE price BETWEEN 50 AND 150")
        .expect("q4");
    let syms_after: std::collections::BTreeSet<&str> = q4
        .rows
        .iter()
        .filter_map(|r| r.get("symbol").and_then(|v| v.as_str()))
        .collect();
    assert!(!syms_after.contains("AAPL"), "AAPL should be gone post-delete");
    // MSFT (50.0) is still in [50, 150].
    assert!(syms_after.contains("MSFT"));
}

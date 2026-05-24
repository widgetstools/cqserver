//! Static PIVOT + UNPIVOT executor tests (worklog S43).
//!
//! Each test seeds a topic, runs a PIVOT (or UNPIVOT) query, and
//! asserts against the expected output. The reference values come
//! from a hand-derived "manual GROUP BY + projection" rewrite —
//! verifying that PIVOT is equivalent to the GROUP-BY shape it's
//! meant to compile down to.

use std::collections::HashMap;
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use serde_json::{json, Map, Value};

/// Trades-shaped topic where each (trader, desk) pair is a distinct
/// row — i.e., the SOW carries one row per (trader, desk) and the
/// PIVOT folds the `desk` dimension into wide columns.
fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/trades".into(),
            key_fields: vec!["trader".into(), "desk".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    )
}

fn publish(topic: &Topic, trader: &str, desk: &str, qty: i64) {
    let mut m = Map::new();
    m.insert("trader".into(), json!(trader));
    m.insert("desk".into(), json!(desk));
    m.insert("qty".into(), json!(qty));
    topic.upsert_map(&m).expect("publish");
}

fn rows_to_map(
    rows: &[Map<String, Value>],
    key_field: &str,
) -> HashMap<String, Map<String, Value>> {
    rows.iter()
        .filter_map(|r| {
            r.get(key_field)
                .and_then(Value::as_str)
                .map(|k| (k.to_string(), r.clone()))
        })
        .collect()
}

#[test]
fn pivot_single_measure_buckets_by_pivot_value() {
    // 3 traders × 3 desks; pivot on desk for the desk-list ('RATES', 'FX').
    // EQUITIES rows fall outside the IN-list and must be dropped from
    // the output entirely.
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 200);
    publish(&topic, "alice", "EQUITIES", 999); // dropped
    publish(&topic, "bob", "RATES", 50);
    publish(&topic, "bob", "FX", 300);
    publish(&topic, "carol", "RATES", 25);

    let result = topic
        .query("SELECT * FROM t PIVOT (SUM(qty) FOR desk IN ('RATES', 'FX'))")
        .expect("pivot");
    let by_trader = rows_to_map(&result.rows, "trader");

    // Three anchor rows: alice, bob, carol. EQUITIES doesn't create
    // a row on its own because it's not in the IN-list AND alice
    // already has RATES + FX rows that anchor her.
    assert_eq!(by_trader.len(), 3);

    // alice → RATES=100, FX=200. EQUITIES=999 dropped.
    assert_eq!(by_trader["alice"].get("RATES").unwrap(), 100);
    assert_eq!(by_trader["alice"].get("FX").unwrap(), 200);
    // bob → RATES=50, FX=300.
    assert_eq!(by_trader["bob"].get("RATES").unwrap(), 50);
    assert_eq!(by_trader["bob"].get("FX").unwrap(), 300);
    // carol → RATES=25 only; FX bucket has no rows → SUM = NULL.
    assert_eq!(by_trader["carol"].get("RATES").unwrap(), 25);
    assert!(by_trader["carol"].get("FX").unwrap().is_null());
}

#[test]
fn pivot_multi_measure_namespaces_columns_by_pivot_value() {
    // Two measures: SUM(qty) AND COUNT(qty). Both apply per
    // (anchor=trader, pivot=desk) bucket. Output columns are
    // namespaced "<pivot_value>_<agg_alias>".
    //
    // With key=(trader, desk), each bucket has at most one row,
    // so COUNT(qty) is 1 where there's a row, 0 (or null) where
    // not. The point is to verify multi-column output naming,
    // not aggregate arithmetic that requires multiple rows per
    // bucket (the SOW shape implies one row per key).
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 200);
    publish(&topic, "bob", "RATES", 10);

    let result = topic
        .query(
            "SELECT * FROM t PIVOT (\
                SUM(qty) AS total, COUNT(qty) AS n \
                FOR desk IN ('RATES', 'FX')\
            )",
        )
        .expect("multi-measure pivot");
    let by_trader = rows_to_map(&result.rows, "trader");

    // alice: RATES row → total=100, n=1. FX row → total=200, n=1.
    assert_eq!(by_trader["alice"].get("RATES_total").unwrap(), 100);
    assert_eq!(by_trader["alice"].get("RATES_n").unwrap(), 1);
    assert_eq!(by_trader["alice"].get("FX_total").unwrap(), 200);
    assert_eq!(by_trader["alice"].get("FX_n").unwrap(), 1);
    // bob: RATES → total=10, n=1. FX → no row, total=null, n=0.
    assert_eq!(by_trader["bob"].get("RATES_total").unwrap(), 10);
    assert_eq!(by_trader["bob"].get("RATES_n").unwrap(), 1);
    assert!(by_trader["bob"].get("FX_total").unwrap().is_null());
    assert_eq!(by_trader["bob"].get("FX_n").unwrap(), 0);
}

#[test]
fn pivot_with_post_filter_predicate_filters_buckets() {
    // Snowflake's WHERE-pre-pivot semantics require a subquery
    // (`FROM (SELECT * FROM t WHERE ...) PIVOT (...)`). We don't
    // support subqueries yet. WHERE applied "around" PIVOT in CQ
    // applies to the underlying rows before bucketing — the same
    // place sqlparser puts WHERE inside the SELECT.
    //
    // Test approach: use WHERE on `qty` to drop low-qty trades
    // before the pivot bucketing kicks in.
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 200);
    publish(&topic, "bob", "RATES", 50);  // dropped by qty > 75
    publish(&topic, "bob", "FX", 300);

    // sqlparser-rs parses `... FROM t PIVOT(...) WHERE ...` with
    // the WHERE attached to the SELECT. The compile path
    // installs it on the ParsedQuery, where the pivot executor
    // applies it before bucketing.
    let result = topic
        .query("SELECT * FROM t PIVOT (SUM(qty) FOR desk IN ('RATES', 'FX')) WHERE qty > 75")
        .expect("pivot with post WHERE");
    let by_trader = rows_to_map(&result.rows, "trader");
    assert_eq!(by_trader["alice"].get("RATES").unwrap(), 100);
    assert_eq!(by_trader["alice"].get("FX").unwrap(), 200);
    // bob/RATES filtered out → bob's RATES bucket is empty → null.
    assert!(by_trader["bob"].get("RATES").unwrap().is_null());
    assert_eq!(by_trader["bob"].get("FX").unwrap(), 300);
}

#[test]
fn pivot_anchor_keys_are_inferred_from_remaining_columns() {
    // schema: (trader, desk, qty). pivot col = desk, agg input = qty.
    // Anchor = trader. Test the inference by checking the output
    // has trader + pivot value columns (no qty, no desk).
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);

    let result = topic
        .query("SELECT * FROM t PIVOT (SUM(qty) FOR desk IN ('RATES'))")
        .expect("pivot");
    assert_eq!(result.rows.len(), 1);
    let row = &result.rows[0];
    let keys: std::collections::BTreeSet<&str> = row.keys().map(String::as_str).collect();
    let expected: std::collections::BTreeSet<&str> = ["trader", "RATES"].into_iter().collect();
    assert_eq!(keys, expected, "row keys: {:?}", row.keys().collect::<Vec<_>>());
}

#[test]
fn dynamic_pivot_discovers_pivot_values_from_data() {
    // S45: `FOR desk IN ANY` — the executor discovers the distinct
    // desk values present and pivots across them.
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 200);
    publish(&topic, "alice", "EQUITIES", 50);
    publish(&topic, "bob", "RATES", 25);

    let result = topic
        .query("SELECT * FROM t PIVOT (SUM(qty) FOR desk IN (ANY))")
        .expect("dynamic pivot");
    let by_trader = rows_to_map(&result.rows, "trader");

    // Discovered values: EQUITIES, FX, RATES (BTreeSet natural
    // order). alice fills all three; bob only RATES.
    assert_eq!(by_trader["alice"].get("EQUITIES").unwrap(), 50);
    assert_eq!(by_trader["alice"].get("FX").unwrap(), 200);
    assert_eq!(by_trader["alice"].get("RATES").unwrap(), 100);
    assert!(by_trader["bob"].get("EQUITIES").unwrap().is_null());
    assert!(by_trader["bob"].get("FX").unwrap().is_null());
    assert_eq!(by_trader["bob"].get("RATES").unwrap(), 25);
}

#[test]
fn dynamic_pivot_on_empty_table_returns_empty() {
    // No rows → no discovered values → no anchors → empty output.
    let topic = make_topic();
    let result = topic
        .query("SELECT * FROM t PIVOT (SUM(qty) FOR desk IN (ANY))")
        .expect("dynamic pivot on empty");
    assert!(result.rows.is_empty());
}

#[test]
fn unpivot_explodes_one_row_into_n_rows_one_per_source_col() {
    // Build a wide-schema topic with explicit pivot columns
    // already present.
    let schema = Arc::new(Schema::from_strs(
        &["trader", "RATES", "FX", "EQUITIES"],
        &[ColumnType::String, ColumnType::Long, ColumnType::Long, ColumnType::Long],
    ));
    let topic = Topic::new(
        TopicConfig {
            name: "/wide".into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        16,
    );

    let mut m = Map::new();
    m.insert("trader".into(), json!("alice"));
    m.insert("RATES".into(), json!(100));
    m.insert("FX".into(), json!(200));
    m.insert("EQUITIES".into(), json!(50));
    topic.upsert_map(&m).expect("publish");

    let mut m = Map::new();
    m.insert("trader".into(), json!("bob"));
    m.insert("RATES".into(), json!(10));
    // bob has no FX or EQUITIES — those are NULL, dropped by UNPIVOT
    // (Snowflake default; user can opt in to NULLs explicitly).
    topic.upsert_map(&m).expect("publish");

    let result = topic
        .query("SELECT * FROM t UNPIVOT (qty FOR desk IN (RATES, FX, EQUITIES))")
        .expect("unpivot");
    let rows: Vec<&Map<String, Value>> = result.rows.iter().collect();

    // alice: 3 rows (RATES=100, FX=200, EQUITIES=50).
    // bob:   1 row  (RATES=10) — FX, EQUITIES were null.
    // Total: 4 rows.
    assert_eq!(rows.len(), 4, "expected 4 unpivoted rows, got {}", rows.len());

    let alice_rows: Vec<&&Map<String, Value>> = rows
        .iter()
        .filter(|r| r.get("trader").and_then(Value::as_str) == Some("alice"))
        .collect();
    assert_eq!(alice_rows.len(), 3);
    // Each alice row carries (trader, desk, qty).
    let desks: std::collections::HashSet<&str> = alice_rows
        .iter()
        .filter_map(|r| r.get("desk").and_then(Value::as_str))
        .collect();
    let expected: std::collections::HashSet<&str> =
        ["RATES", "FX", "EQUITIES"].iter().copied().collect();
    assert_eq!(desks, expected);

    let bob_rows: Vec<&&Map<String, Value>> = rows
        .iter()
        .filter(|r| r.get("trader").and_then(Value::as_str) == Some("bob"))
        .collect();
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(bob_rows[0].get("desk").unwrap(), "RATES");
    assert_eq!(bob_rows[0].get("qty").unwrap(), 10);
}

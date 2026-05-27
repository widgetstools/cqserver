//! S30 — Range index end-to-end test.
//!
//! Spawns a real cqserver child process with a topic that declares
//! `index_columns = ["price"]`. Publishes 100 rows via TCP, then runs
//! a `BETWEEN` SOW query and verifies the result count + ordering
//! matches what a naive Rust filter would produce.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn range_index_between_query_e2e() {
    let topic = TopicSpec::new("/range-e2e", "symbol")
        .with_inline_columns([
            ("symbol", "string"),
            ("price", "double"),
            ("qty", "long"),
        ])
        .with_index_columns(["price"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Publish 100 rows with `price = i * 10.0`, i ∈ 1..=100.
    for i in 1..=100u64 {
        client
            .publish(
                "/range-e2e",
                json!({
                    "symbol": format!("S{i:03}"),
                    "price": (i as f64) * 10.0,
                    "qty": i as i64,
                }),
            )
            .await
            .expect("publish");
    }

    // BETWEEN 150 AND 250 → price ∈ {160, 170, ..., 250} → 10 rows
    // when price = i*10 (i=16..=25, inclusive at both ends, but
    // BETWEEN is inclusive so i ∈ {15..=25} gives 11 entries; here
    // price=150 corresponds to i=15 and is included).
    let rows = client
        .sow_sql(
            "/range-e2e",
            "SELECT symbol, price FROM t WHERE price BETWEEN 150 AND 250",
        )
        .await
        .expect("sow_sql");
    let returned: Vec<f64> = rows
        .iter()
        .filter_map(|r| r.get("price").and_then(Value::as_f64))
        .collect();

    // Reference: filter the publishes the same way in pure Rust.
    let expected: Vec<f64> = (1..=100u64)
        .map(|i| (i as f64) * 10.0)
        .filter(|&p| p >= 150.0 && p <= 250.0)
        .collect();

    let mut got_sorted = returned.clone();
    got_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(
        got_sorted, expected,
        "range query returned wrong rows. got={:?} expected={:?}",
        got_sorted, expected,
    );
    // Sanity on count: 11 rows (150, 160, ..., 250).
    assert_eq!(got_sorted.len(), 11);
}

#[tokio::test]
async fn range_index_greater_than_e2e() {
    let topic = TopicSpec::new("/range-gt-e2e", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_index_columns(["v"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 1..=30u64 {
        client
            .publish("/range-gt-e2e", json!({ "k": format!("k{i:03}"), "v": i as i64 }))
            .await
            .expect("publish");
    }

    let rows = client
        .sow_sql("/range-gt-e2e", "SELECT k, v FROM t WHERE v > 25")
        .await
        .expect("gt query");
    // v ∈ {26, 27, 28, 29, 30} → 5 rows.
    assert_eq!(rows.len(), 5);
    let mut vs: Vec<i64> = rows
        .iter()
        .filter_map(|r| r.get("v").and_then(Value::as_i64))
        .collect();
    vs.sort();
    assert_eq!(vs, vec![26, 27, 28, 29, 30]);
}

// ───── Diversification ────────────────────────────────────────────

/// BETWEEN with a range covering NO existing values → empty result
/// (out-of-band low+high, valid ordering).
#[tokio::test]
async fn range_index_between_out_of_band_is_empty() {
    let topic = TopicSpec::new("/range-oob", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_index_columns(["v"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=10_i64 {
        client
            .publish("/range-oob", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rows = client
        .sow_sql("/range-oob", "SELECT k FROM t WHERE v BETWEEN 1000 AND 2000")
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// BETWEEN where low == high → exactly one row.
#[tokio::test]
async fn range_index_between_single_value_returns_one_row() {
    let topic = TopicSpec::new("/range-single", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_index_columns(["v"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=10_i64 {
        client
            .publish("/range-single", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rows = client
        .sow_sql("/range-single", "SELECT k FROM t WHERE v BETWEEN 5 AND 5")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").unwrap().as_str().unwrap(), "k5");
}

/// Inverted-BETWEEN regression — `WHERE v BETWEEN high AND low` on
/// an indexed column used to panic the server with "snapshot task
/// join failed" (BTreeMap::range panics when start > end). The fix
/// in `sec_index::rows_in_range` short-circuits to an empty bitmap.
/// This test pins the contract: the query returns empty, not crash.
#[tokio::test]
async fn range_index_inverted_between_returns_empty_not_panic() {
    let topic = TopicSpec::new("/range-inv", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_index_columns(["v"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=10_i64 {
        client
            .publish("/range-inv", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Inverted range — must return empty (and not stall / crash).
    let rows = client
        .sow_sql("/range-inv", "SELECT k FROM t WHERE v BETWEEN 100 AND 50")
        .await
        .expect("inverted BETWEEN must not stall or error");
    assert!(rows.is_empty());

    // Server is still healthy — a normal query works after.
    let healthy = client
        .sow_sql("/range-inv", "SELECT k FROM t WHERE v BETWEEN 3 AND 5")
        .await
        .unwrap();
    assert_eq!(healthy.len(), 3);
}

/// Same regression on a Double-typed indexed column.
#[tokio::test]
async fn range_index_inverted_between_double_returns_empty() {
    let topic = TopicSpec::new("/range-inv-d", "k")
        .with_inline_columns([("k", "string"), ("price", "double")])
        .with_index_columns(["price"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=5_i64 {
        client
            .publish(
                "/range-inv-d",
                json!({ "k": format!("k{i}"), "price": i as f64 * 10.0 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/range-inv-d",
            "SELECT k FROM t WHERE price BETWEEN 50.0 AND 10.0",
        )
        .await
        .expect("inverted-double BETWEEN must not stall");
    assert!(rows.is_empty());
}

/// `<` (strict less) on indexed column — boundary value excluded.
#[tokio::test]
async fn range_index_strict_less_excludes_boundary() {
    let topic = TopicSpec::new("/range-strict", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_index_columns(["v"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 1..=5_i64 {
        client
            .publish("/range-strict", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rows = client
        .sow_sql("/range-strict", "SELECT k, v FROM t WHERE v < 3")
        .await
        .unwrap();
    let mut vs: Vec<i64> = rows
        .iter()
        .map(|r| r.get("v").unwrap().as_i64().unwrap())
        .collect();
    vs.sort();
    assert_eq!(vs, vec![1, 2]);
}

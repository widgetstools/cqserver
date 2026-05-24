//! S30 — Range index end-to-end test.
//!
//! Spawns a real cqserver child process with a topic that declares
//! `index_columns = ["price"]`. Publishes 100 rows via TCP, then runs
//! a `BETWEEN` SOW query and verifies the result count + ordering
//! matches what a naive Rust filter would produce.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};

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

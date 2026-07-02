//! Regression: AMPS_PARITY.md §4 bug 2 (A1) — `ORDER BY <select alias>`
//! must not hang the SOW encoder, and a truly-unknown ORDER BY column
//! must return a clean error rather than looping forever.
//!
//! The original bug: `SELECT col, SUM(x) AS y FROM t GROUP BY col
//! ORDER BY y` hung the SOW encoder when `y` did not match a base
//! column. This test pins the fixed behaviour and guards against
//! regression. Every await is wrapped in a `tokio::time::timeout` so a
//! reintroduced hang surfaces as a test FAILURE, never a stuck run.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn order_by_select_alias_returns_within_deadline() {
    let topic = TopicSpec::new("/t", "k").with_inline_columns([
        ("k", "string"),
        ("grp", "long"),
        ("x", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..100 {
        client
            .publish("/t", json!({ "k": format!("k{i}"), "grp": i % 5, "x": i }))
            .await
            .expect("publish");
    }
    // Let the SOW settle before querying.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // ORDER BY on a SELECT-list alias must resolve and return, not hang.
    let fut = client.sow_sql("/t", "SELECT grp, SUM(x) AS y FROM t GROUP BY grp ORDER BY y");
    let res = tokio::time::timeout(Duration::from_secs(10), fut).await;
    let rows = res
        .expect("query hung >10s — the alias-hang bug")
        .expect("query errored");
    assert_eq!(rows.len(), 5, "expected 5 group rows, got {:?}", rows);

    // And truly-unknown columns must error cleanly instead of hanging.
    let fut = client.sow_sql(
        "/t",
        "SELECT grp, SUM(x) AS y FROM t GROUP BY grp ORDER BY nosuchcol",
    );
    let res = tokio::time::timeout(Duration::from_secs(10), fut).await;
    assert!(
        res.expect("unknown-column query hung >10s").is_err(),
        "unknown ORDER BY column must error, not hang"
    );
}

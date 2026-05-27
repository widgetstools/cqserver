//! R8 — uncorrelated `EXISTS` / `NOT EXISTS` subqueries. AMPS only
//! supports uncorrelated subqueries (the inner query doesn't
//! reference outer columns); we match that scope. The pre-flight
//! materializer runs the EXISTS once, gets a yes/no answer, and
//! substitutes a constant into the outer WHERE.
//!
//! Correlated EXISTS (`WHERE NOT EXISTS (SELECT 1 FROM trades t
//! WHERE t.position_id = p.position_id)`) is OUT OF SCOPE — neither
//! AMPS nor cqserver supports per-row inner evaluation today.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn exists_with_matching_rows_returns_all_outer() {
    let trades = TopicSpec::new("/r8_trades", "k")
        .with_inline_columns([("k", "string"), ("side", "string")]);
    let positions = TopicSpec::new("/r8_positions", "pk")
        .with_inline_columns([("pk", "string"), ("symbol", "string")]);
    let server = start_server(vec![trades, positions]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Trades has at least one BUY.
    client
        .publish("/r8_trades", json!({ "k": "t1", "side": "BUY" }))
        .await
        .unwrap();
    client
        .publish("/r8_trades", json!({ "k": "t2", "side": "SELL" }))
        .await
        .unwrap();
    // Positions has 3 rows.
    for (k, s) in [("p1", "AAPL"), ("p2", "MSFT"), ("p3", "GOOG")] {
        client
            .publish("/r8_positions", json!({ "pk": k, "symbol": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // EXISTS (SELECT * FROM trades WHERE side='BUY') is true →
    // every position row passes the outer WHERE.
    let rows = client
        .sow_sql(
            "/r8_positions",
            "SELECT pk FROM t WHERE EXISTS (SELECT * FROM r8_trades WHERE side = 'BUY')",
        )
        .await
        .expect("uncorrelated EXISTS must compile");
    assert_eq!(rows.len(), 3, "all 3 positions match (EXISTS = true)");
}

#[tokio::test]
async fn exists_with_no_matching_rows_returns_none() {
    let trades = TopicSpec::new("/r8_trades_none", "k")
        .with_inline_columns([("k", "string"), ("side", "string")]);
    let positions = TopicSpec::new("/r8_pos_none", "pk")
        .with_inline_columns([("pk", "string"), ("symbol", "string")]);
    let server = start_server(vec![trades, positions]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Trades has only SELL.
    client
        .publish("/r8_trades_none", json!({ "k": "t1", "side": "SELL" }))
        .await
        .unwrap();
    client
        .publish("/r8_pos_none", json!({ "pk": "p1", "symbol": "AAPL" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/r8_pos_none",
            "SELECT pk FROM t WHERE EXISTS (SELECT * FROM r8_trades_none WHERE side = 'BUY')",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "no BUY trades exist → EXISTS=false → 0 outer rows");
}

#[tokio::test]
async fn not_exists_inverts() {
    let watch = TopicSpec::new("/r8_watch", "sym")
        .with_inline_columns([("sym", "string")]);
    let trades = TopicSpec::new("/r8_trades_neg", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")]);
    let server = start_server(vec![watch, trades]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk) in [("t1", "FX"), ("t2", "RATES")] {
        client
            .publish("/r8_trades_neg", json!({ "k": k, "desk": desk }))
            .await
            .unwrap();
    }
    // No watch entries.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // NOT EXISTS on an empty topic → true → all trades pass.
    let rows = client
        .sow_sql(
            "/r8_trades_neg",
            "SELECT k FROM t WHERE NOT EXISTS (SELECT * FROM r8_watch)",
        )
        .await
        .expect("NOT EXISTS must compile");
    assert_eq!(rows.len(), 2, "watch empty → NOT EXISTS true → all trades");
}

#[tokio::test]
async fn exists_composed_with_other_predicate() {
    let positions = TopicSpec::new("/r8_compose", "pk").with_inline_columns([
        ("pk", "string"),
        ("desk", "string"),
        ("qty", "long"),
    ]);
    let flag = TopicSpec::new("/r8_flag", "k")
        .with_inline_columns([("k", "string"), ("on", "bool")]);
    let server = start_server(vec![positions, flag]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, qty) in [
        ("p1", "FX", 100_i64),
        ("p2", "FX", 5),
        ("p3", "RATES", 200),
    ] {
        client
            .publish("/r8_compose", json!({ "pk": k, "desk": desk, "qty": qty }))
            .await
            .unwrap();
    }
    client
        .publish("/r8_flag", json!({ "k": "fx_enabled", "on": true }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Flag exists AND qty > 10 → only p1 and p3 pass (p2's qty=5).
    let rows = client
        .sow_sql(
            "/r8_compose",
            "SELECT pk FROM t WHERE EXISTS (SELECT * FROM r8_flag WHERE on = TRUE) AND qty > 10",
        )
        .await
        .unwrap();
    let pks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("pk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(pks.contains("p1") && pks.contains("p3"));
    assert!(!pks.contains("p2"));
}

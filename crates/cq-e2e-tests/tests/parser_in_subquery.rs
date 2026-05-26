//! Q9 e2e — `WHERE col IN (SELECT col FROM topic)` materialises the
//! subquery at SOW time and substitutes the result as a literal IN
//! list.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn in_subquery_materialises_against_real_topic() {
    let trades = TopicSpec::new("/q9_trades", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("price", "double"),
    ]);
    let watch = TopicSpec::new("/q9_watch", "symbol").with_inline_columns([
        ("symbol", "string"),
    ]);
    let server = start_server(vec![trades, watch]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Watchlist: AAPL + MSFT.
    for s in ["AAPL", "MSFT"] {
        client
            .publish("/q9_watch", json!({ "symbol": s }))
            .await
            .unwrap();
    }
    // Trades on AAPL, MSFT, GOOGL.
    for (k, sym, px) in [
        ("t1", "AAPL", 150.0_f64),
        ("t2", "MSFT", 300.0),
        ("t3", "GOOGL", 2800.0),
    ] {
        client
            .publish("/q9_trades", json!({ "k": k, "symbol": sym, "price": px }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/q9_trades",
            "SELECT k, symbol FROM t \
             WHERE symbol IN (SELECT symbol FROM q9_watch)",
        )
        .await
        .expect("subquery sow");
    assert_eq!(rows.len(), 2, "expected only AAPL + MSFT trades, got {rows:?}");
    let syms: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(syms.contains("AAPL"));
    assert!(syms.contains("MSFT"));
    assert!(!syms.contains("GOOGL"));
}

#[tokio::test]
async fn in_subquery_empty_result_matches_no_rows() {
    let trades = TopicSpec::new("/q9b_trades", "k")
        .with_inline_columns([("k", "string"), ("symbol", "string")]);
    let watch = TopicSpec::new("/q9b_watch", "symbol")
        .with_inline_columns([("symbol", "string")]);
    let server = start_server(vec![trades, watch]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Empty watchlist; some trades.
    client
        .publish("/q9b_trades", json!({ "k": "t1", "symbol": "AAPL" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/q9b_trades",
            "SELECT k FROM t WHERE symbol IN (SELECT symbol FROM q9b_watch)",
        )
        .await
        .expect("empty subquery sow");
    assert_eq!(rows.len(), 0, "empty IN list must match no rows");
}

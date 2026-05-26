//! P13 e2e — `WHERE MATCHES_REGEX(col, '<pattern>')` filters rows
//! against a precompiled regex.

use cq_client::{Client, ClientError};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn matches_regex_filter_returns_pattern_subset() {
    let topic = TopicSpec::new("/regex-data", "k").with_inline_columns([
        ("k", "string"),
        ("symbol", "string"),
        ("desk", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, sym, desk) in [
        ("r1", "AAPL", "RATES"),
        ("r2", "MSFT", "EQUITIES"),
        ("r3", "AMZN", "TECH"),
        ("r4", "GOOGL", "RATES"),
        ("r5", "NVDA", "EQUITIES"),
    ] {
        client
            .publish(
                "/regex-data",
                json!({ "k": k, "symbol": sym, "desk": desk }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Symbols starting with A or M.
    let rows = client
        .sow_sql(
            "/regex-data",
            "SELECT k, symbol FROM t WHERE MATCHES_REGEX(symbol, '^[AM].*')",
        )
        .await
        .expect("regex filter sow");
    let syms: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(syms.contains("AAPL"));
    assert!(syms.contains("MSFT"));
    assert!(syms.contains("AMZN"));
    assert!(!syms.contains("GOOGL"));
    assert!(!syms.contains("NVDA"));

    // Invalid pattern is rejected at parse time (server error, not
    // a stall — see P7 for the wire-level contract).
    let r = client
        .sow_sql(
            "/regex-data",
            "SELECT k FROM t WHERE MATCHES_REGEX(symbol, '[unclosed')",
        )
        .await;
    assert!(
        matches!(r, Err(ClientError::Server(_))),
        "invalid regex should surface as a server error, got {r:?}"
    );
}

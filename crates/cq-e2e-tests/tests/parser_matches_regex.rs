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

// ───── Diversification ────────────────────────────────────────────

/// Anchored vs unanchored — `^X` only matches prefix, `X` matches
/// anywhere.
#[tokio::test]
async fn matches_regex_anchored_vs_substring_semantics() {
    let topic = TopicSpec::new("/regex-anchor", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, s) in [
        ("a1", "ABC"),
        ("a2", "XABC"),
        ("a3", "abcdef"),
    ] {
        client
            .publish("/regex-anchor", json!({ "k": k, "s": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let prefix = client
        .sow_sql("/regex-anchor", "SELECT k FROM t WHERE MATCHES_REGEX(s, '^ABC')")
        .await
        .unwrap();
    let keys: std::collections::HashSet<String> = prefix
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains("a1") && !keys.contains("a2"));

    let substring = client
        .sow_sql("/regex-anchor", "SELECT k FROM t WHERE MATCHES_REGEX(s, 'ABC')")
        .await
        .unwrap();
    let keys: std::collections::HashSet<String> = substring
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains("a1") && keys.contains("a2") && !keys.contains("a3"));
}

/// NULL column values — `MATCHES_REGEX(col, ...)` against NULL must
/// be FALSE (3VL not propagated to TRUE — row filtered out).
#[tokio::test]
async fn matches_regex_against_null_column_is_falsy() {
    let topic = TopicSpec::new("/regex-null", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/regex-null", json!({ "k": "with", "s": "hello" }))
        .await
        .unwrap();
    client
        .publish("/regex-null", json!({ "k": "without" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql("/regex-null", "SELECT k FROM t WHERE MATCHES_REGEX(s, '.*')")
        .await
        .unwrap();
    let keys: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains("with"));
    assert!(
        !keys.contains("without"),
        "NULL must NOT match even `.*` (no string to match against)"
    );
}

/// Combined with another predicate — `MATCHES_REGEX(...) AND ...`.
#[tokio::test]
async fn matches_regex_combined_with_other_predicate() {
    let topic = TopicSpec::new("/regex-and", "k").with_inline_columns([
        ("k", "string"),
        ("sym", "string"),
        ("px", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, sym, px) in [
        ("c1", "AAPL", 100.0),
        ("c2", "AAPL", 200.0),
        ("c3", "GOOG", 150.0),
        ("c4", "AMZN", 300.0),
    ] {
        client
            .publish("/regex-and", json!({ "k": k, "sym": sym, "px": px }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/regex-and",
            "SELECT k FROM t WHERE MATCHES_REGEX(sym, '^A') AND px > 150",
        )
        .await
        .unwrap();
    let keys: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(keys.contains("c2"), "AAPL & 200 > 150");
    assert!(keys.contains("c4"), "AMZN & 300 > 150");
    assert!(!keys.contains("c1"), "AAPL but 100 not > 150");
    assert!(!keys.contains("c3"), "GOOG doesn't start with A");
}

/// Regex matches every row → returns full topic; no rows → empty.
#[tokio::test]
async fn matches_regex_all_match_and_none_match() {
    let topic = TopicSpec::new("/regex-allnone", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, s) in [("a", "x"), ("b", "y"), ("c", "z")] {
        client
            .publish("/regex-allnone", json!({ "k": k, "s": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let all = client
        .sow_sql("/regex-allnone", "SELECT k FROM t WHERE MATCHES_REGEX(s, '.*')")
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    let none = client
        .sow_sql(
            "/regex-allnone",
            "SELECT k FROM t WHERE MATCHES_REGEX(s, '^Q')",
        )
        .await
        .unwrap();
    assert!(none.is_empty());
}

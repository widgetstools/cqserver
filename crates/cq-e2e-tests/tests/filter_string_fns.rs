//! e2e: string-function predicates (UPPER, LOWER, LENGTH, SUBSTR, CONCAT).
//!
//! Verifies that SOW queries using the structured string-expression
//! variants compile + evaluate correctly across the real wire.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn substr_and_concat_filters_via_wire() {
    let topic = TopicSpec::new("/strfns", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("symbol", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = [
        ("T1", "RATES", "AAPL"),
        ("T2", "RATES", "GOOGL"),
        ("T3", "EQUITIES", "MSFT"),
        ("T4", "RATES", "AMZN"),
    ];
    for (k, desk, symbol) in rows {
        client
            .publish(
                "/strfns",
                json!({ "k": k, "desk": desk, "symbol": symbol }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // SUBSTR-based filter: first 3 chars of symbol = "AAP" → only T1.
    let rows = client
        .sow("/strfns", Some("SUBSTR(symbol, 1, 3) = 'AAP'"))
        .await
        .expect("sow substr");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").unwrap(), "T1");

    // CONCAT-based filter: CONCAT(desk, ':', symbol) LIKE 'RATES:A%' →
    // matches T1 (AAPL) and T4 (AMZN) but not T2 (GOOGL).
    let rows = client
        .sow(
            "/strfns",
            Some("CONCAT(desk, ':', symbol) LIKE 'RATES:A%'"),
        )
        .await
        .expect("sow concat-like");
    let mut keys: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["T1".to_string(), "T4".to_string()]);

    // LENGTH still works for completeness.
    let rows = client
        .sow("/strfns", Some("LENGTH(symbol) = 4"))
        .await
        .expect("sow length");
    // AAPL=4, GOOGL=5, MSFT=4, AMZN=4 → T1, T3, T4.
    let mut keys: Vec<String> = rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["T1", "T3", "T4"]);
}

// ───── Diversification ────────────────────────────────────────────

/// UPPER + LOWER predicates — case-insensitive matching.
#[tokio::test]
async fn upper_and_lower_predicate_match_case_insensitively() {
    let topic = TopicSpec::new("/sf-case", "k")
        .with_inline_columns([("k", "string"), ("sym", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, sym) in [("a", "aapl"), ("b", "AAPL"), ("c", "AaPl"), ("d", "GOOG")] {
        client
            .publish("/sf-case", json!({ "k": k, "sym": sym }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let upper = client
        .sow("/sf-case", Some("UPPER(sym) = 'AAPL'"))
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = upper
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("a") && ks.contains("b") && ks.contains("c"));
    assert!(!ks.contains("d"));

    let lower = client
        .sow("/sf-case", Some("LOWER(sym) = 'goog'"))
        .await
        .unwrap();
    assert_eq!(lower.len(), 1);
    assert_eq!(lower[0].get("k").unwrap().as_str().unwrap(), "d");
}

/// LIKE patterns — wildcard match.
#[tokio::test]
async fn like_predicate_with_wildcards() {
    let topic = TopicSpec::new("/sf-like", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, s) in [("a", "ABC"), ("b", "ABCDEF"), ("c", "XYZ"), ("d", "A_B")] {
        client
            .publish("/sf-like", json!({ "k": k, "s": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    // 'AB%' — prefix match.
    let pref = client.sow("/sf-like", Some("s LIKE 'AB%'")).await.unwrap();
    let ks: std::collections::HashSet<String> = pref
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("a") && ks.contains("b"));
    assert!(!ks.contains("c") && !ks.contains("d"));

    // 'A_B' — underscore matches exactly one char.
    let one = client.sow("/sf-like", Some("s LIKE 'A_B'")).await.unwrap();
    let ks: std::collections::HashSet<String> = one
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("d"));
    assert!(!ks.contains("a"), "ABC doesn't match A_B (B comes after BC)");
}

/// NOT LIKE — complement matching.
#[tokio::test]
async fn not_like_predicate_excludes_matches() {
    let topic = TopicSpec::new("/sf-notlike", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, s) in [("a", "AAPL"), ("b", "MSFT"), ("c", "AMZN")] {
        client
            .publish("/sf-notlike", json!({ "k": k, "s": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow("/sf-notlike", Some("s NOT LIKE 'A%'"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").unwrap().as_str().unwrap(), "b");
}

/// LENGTH on empty string — returns 0.
#[tokio::test]
async fn length_predicate_handles_empty_strings() {
    let topic = TopicSpec::new("/sf-len-emp", "k")
        .with_inline_columns([("k", "string"), ("s", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, s) in [("a", ""), ("b", "x"), ("c", "yz")] {
        client
            .publish("/sf-len-emp", json!({ "k": k, "s": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let zero = client
        .sow("/sf-len-emp", Some("LENGTH(s) = 0"))
        .await
        .unwrap();
    assert_eq!(zero.len(), 1);
    assert_eq!(zero[0].get("k").unwrap().as_str().unwrap(), "a");
}

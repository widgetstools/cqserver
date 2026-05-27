//! R12 — `<bool-col> IS TRUE / IS NOT TRUE / IS FALSE / IS NOT FALSE`
//! predicates. AMPS uses these for boolean filter columns (
//! `restricted_flag IS TRUE`, `is_active IS NOT FALSE`, etc.).
//! SQL three-valued logic: NULL is neither true nor false.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn is_true_matches_only_true_rows() {
    let topic = TopicSpec::new("/r12_bool", "k")
        .with_inline_columns([("k", "string"), ("active", "bool")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.publish("/r12_bool", json!({ "k": "a", "active": true })).await.unwrap();
    client.publish("/r12_bool", json!({ "k": "b", "active": false })).await.unwrap();
    // NULL: omit the bool field.
    client.publish("/r12_bool", json!({ "k": "c" })).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // IS TRUE → only "a".
    let rows = client
        .sow_sql("/r12_bool", "SELECT k FROM r12_bool WHERE active IS TRUE")
        .await
        .expect("IS TRUE must compile");
    let ks: std::collections::HashSet<String> = rows.iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(ks, ["a".to_string()].into_iter().collect());

    // IS FALSE → only "b".
    let rows = client
        .sow_sql("/r12_bool", "SELECT k FROM r12_bool WHERE active IS FALSE")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows.iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(ks, ["b".to_string()].into_iter().collect());
}

#[tokio::test]
async fn is_not_true_includes_null_per_sql_three_valued_logic() {
    let topic = TopicSpec::new("/r12_b2", "k")
        .with_inline_columns([("k", "string"), ("flag", "bool")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.publish("/r12_b2", json!({ "k": "t", "flag": true })).await.unwrap();
    client.publish("/r12_b2", json!({ "k": "f", "flag": false })).await.unwrap();
    client.publish("/r12_b2", json!({ "k": "n" })).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // IS NOT TRUE matches FALSE + NULL (SQL semantics).
    let rows = client
        .sow_sql("/r12_b2", "SELECT k FROM r12_b2 WHERE flag IS NOT TRUE")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows.iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(ks, ["f", "n"].iter().map(|s| s.to_string()).collect());

    // IS NOT FALSE matches TRUE + NULL.
    let rows = client
        .sow_sql("/r12_b2", "SELECT k FROM r12_b2 WHERE flag IS NOT FALSE")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows.iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(ks, ["t", "n"].iter().map(|s| s.to_string()).collect());
}

#[tokio::test]
async fn is_true_inside_not_and_compound_clause() {
    // The R10 demo `WHERE compliance_status IN (…) AND NOT (restricted_flag IS TRUE)`
    // is the load-bearing pattern for fl-1 in the demo library.
    let topic = TopicSpec::new("/r12_compound", "k").with_inline_columns([
        ("k", "string"),
        ("status", "string"),
        ("restricted", "bool"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.publish("/r12_compound", json!({ "k": "a", "status": "BREACH", "restricted": false })).await.unwrap();
    client.publish("/r12_compound", json!({ "k": "b", "status": "BREACH", "restricted": true })).await.unwrap();
    client.publish("/r12_compound", json!({ "k": "c", "status": "OK", "restricted": false })).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/r12_compound",
            "SELECT k FROM r12_compound WHERE status IN ('BREACH','WARNING') AND NOT (restricted IS TRUE)",
        )
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows.iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(ks, ["a".to_string()].into_iter().collect(), "BREACH + not restricted");
}

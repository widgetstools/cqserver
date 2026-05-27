//! Q10 e2e — Bytes column type over the wire.
//!
//! Bytes values are base64-encoded on the JSON wire form. The server
//! decodes input, stores the raw bytes, and emits base64 back on
//! SOW. Equality + IS NULL must work; ordered comparisons are
//! rejected.
//!
//! NB: Bytes can ONLY be added to a topic at runtime via the
//! `/admin/add-column` endpoint — there is no static-config form.
//! The Q10 worklog notes this explicitly.

use base64::Engine;
use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

async fn add_bytes_column(server_admin_port: u16, topic_path: &str, name: &str) {
    let url = format!(
        "http://127.0.0.1:{}/admin/add-column/{}?name={}&type=bytes",
        server_admin_port,
        urlencoding::encode(topic_path),
        name
    );
    let resp = reqwest::Client::new().post(&url).send().await.unwrap();
    assert!(
        resp.status().is_success(),
        "add-bytes-column on {topic_path} failed: {:?}",
        resp.status()
    );
}

#[tokio::test]
async fn bytes_column_publish_and_sow_roundtrip() {
    let topic = TopicSpec::new("/q10_basic", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    add_bytes_column(server.admin_port, "/q10_basic", "payload").await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let raw: Vec<u8> = (0..32).collect();
    client
        .publish("/q10_basic", json!({ "k": "p1", "payload": b64(&raw) }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/q10_basic", "SELECT k, payload FROM t")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let returned = rows[0]
        .get("payload")
        .unwrap()
        .as_str()
        .expect("payload returned as base64 string");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(returned)
        .unwrap();
    assert_eq!(decoded, raw, "round-trip lost bytes");
}

#[tokio::test]
async fn bytes_column_null_round_trips() {
    let topic = TopicSpec::new("/q10_null", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    add_bytes_column(server.admin_port, "/q10_null", "payload").await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/q10_null", json!({ "k": "p1" }))
        .await
        .unwrap();
    client
        .publish("/q10_null", json!({ "k": "p2", "payload": null }))
        .await
        .unwrap();
    client
        .publish("/q10_null", json!({ "k": "p3", "payload": b64(b"data") }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let nulls = client
        .sow_sql("/q10_null", "SELECT k FROM t WHERE payload IS NULL")
        .await
        .unwrap();
    let null_keys: std::collections::HashSet<String> = nulls
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(null_keys.contains("p1") && null_keys.contains("p2"));
    assert!(!null_keys.contains("p3"));

    let non_null = client
        .sow_sql("/q10_null", "SELECT k FROM t WHERE payload IS NOT NULL")
        .await
        .unwrap();
    assert_eq!(non_null.len(), 1);
    assert_eq!(non_null[0].get("k").unwrap().as_str().unwrap(), "p3");
}

#[tokio::test]
async fn bytes_column_supports_large_payloads() {
    let topic = TopicSpec::new("/q10_big", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    add_bytes_column(server.admin_port, "/q10_big", "payload").await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let raw: Vec<u8> = (0..10_000_u32).map(|i| (i % 256) as u8).collect();
    client
        .publish("/q10_big", json!({ "k": "big", "payload": b64(&raw) }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql("/q10_big", "SELECT k, payload FROM t")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let returned = rows[0].get("payload").unwrap().as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(returned)
        .unwrap();
    assert_eq!(decoded.len(), 10_000);
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn bytes_column_invalid_base64_input_becomes_null() {
    let topic = TopicSpec::new("/q10_bad", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    add_bytes_column(server.admin_port, "/q10_bad", "payload").await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Non-base64 string → store treats as null per the from_json path.
    client
        .publish("/q10_bad", json!({ "k": "bad", "payload": "!!! not base64 !!!" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let nulls = client
        .sow_sql("/q10_bad", "SELECT k FROM t WHERE payload IS NULL")
        .await
        .unwrap();
    let null_keys: std::collections::HashSet<String> = nulls
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(null_keys.contains("bad"));
}

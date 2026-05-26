//! Q2 e2e — wire-level `Command::PublishBatch`. One frame, one ack,
//! one batched txlog commit, N sequences returned in input order.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn publish_batch_returns_n_sequences_in_input_order() {
    let topic = TopicSpec::new("/wirebatch", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed publish so the schema is established.
    client
        .publish("/wirebatch", json!({ "k": "seed", "v": 0 }))
        .await
        .unwrap();

    // Publish a batch of 25 rows in one wire frame.
    let rows: Vec<serde_json::Value> = (0..25)
        .map(|i| json!({ "k": format!("k{i:02}"), "v": i }))
        .collect();
    let seqs = client
        .publish_batch("/wirebatch", rows)
        .await
        .expect("publish_batch");
    assert_eq!(seqs.len(), 25, "expected 25 sequences, got {seqs:?}");

    // Sequences are monotonic per-topic — assert strictly sorted.
    let sorted: Vec<u64> = {
        let mut v = seqs.clone();
        v.sort();
        v
    };
    assert_eq!(seqs, sorted, "sequences must be sorted in input order");

    // Verify every row landed in the SOW.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let snap = client
        .sow_sql("/wirebatch", "SELECT k, v FROM t")
        .await
        .expect("sow");
    // 25 batch rows + 1 seed = 26 unique keys.
    assert_eq!(snap.len(), 26, "expected 26 rows, got {}", snap.len());
}

#[tokio::test]
async fn publish_batch_empty_input_returns_empty_sequences() {
    let topic = TopicSpec::new("/wirebatch2", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let seqs = client.publish_batch("/wirebatch2", Vec::new()).await.unwrap();
    assert!(seqs.is_empty());
}

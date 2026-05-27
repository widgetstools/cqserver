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

// ───── Diversification ────────────────────────────────────────────

/// Batch with duplicate keys — last write wins; final row count is
/// distinct-key count, not batch size.
#[tokio::test]
async fn publish_batch_with_duplicate_keys_collapses_to_distinct() {
    let topic = TopicSpec::new("/wirebatch-dup", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = vec![
        json!({ "k": "A", "v": 1 }),
        json!({ "k": "B", "v": 2 }),
        json!({ "k": "A", "v": 99 }), // overwrites
        json!({ "k": "C", "v": 3 }),
        json!({ "k": "B", "v": 22 }), // overwrites
    ];
    let seqs = client.publish_batch("/wirebatch-dup", rows).await.unwrap();
    assert_eq!(seqs.len(), 5, "every input row consumes a sequence");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let snap = client
        .sow_sql("/wirebatch-dup", "SELECT k, v FROM t")
        .await
        .unwrap();
    assert_eq!(snap.len(), 3);
    let by_k: std::collections::HashMap<String, i64> = snap
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("v").unwrap().as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_k["A"], 99);
    assert_eq!(by_k["B"], 22);
    assert_eq!(by_k["C"], 3);
}

/// Batch onto an unknown topic returns a clean error, no crash.
#[tokio::test]
async fn publish_batch_unknown_topic_errors_cleanly() {
    use cq_client::ClientError;
    let topic = TopicSpec::new("/wirebatch-real", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = vec![json!({ "k": "A", "v": 1 })];
    let r = client.publish_batch("/wirebatch-fake", rows).await;
    assert!(matches!(r, Err(ClientError::Server(_))), "expected server error, got {r:?}");

    // The real topic must still accept publishes after.
    let seqs = client
        .publish_batch("/wirebatch-real", vec![json!({ "k": "A", "v": 1 })])
        .await
        .unwrap();
    assert_eq!(seqs.len(), 1);
}

/// Large batch (1000 rows) — verifies wire frame size limits + ack
/// pipelining haven't introduced an upper bound that breaks bulk imports.
#[tokio::test]
async fn publish_batch_large_size_succeeds() {
    let topic = TopicSpec::new("/wirebatch-big", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows: Vec<serde_json::Value> = (0..1000)
        .map(|i| json!({ "k": format!("k{i:04}"), "v": i }))
        .collect();
    let seqs = client
        .publish_batch("/wirebatch-big", rows)
        .await
        .expect("1000-row batch");
    assert_eq!(seqs.len(), 1000);
    // Sequences strictly increasing.
    for w in seqs.windows(2) {
        assert!(w[0] < w[1], "seqs not monotonic: {} → {}", w[0], w[1]);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    let row = client
        .sow_sql("/wirebatch-big", "SELECT COUNT(*) AS c FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("c").unwrap().as_i64().unwrap(), 1000);
}

/// Interleave publish_batch with single publish — sequences must
/// remain monotonic across both paths.
#[tokio::test]
async fn publish_batch_and_single_publish_share_monotonic_seq() {
    let topic = TopicSpec::new("/wirebatch-mix", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let s1 = client
        .publish("/wirebatch-mix", json!({ "k": "a", "v": 1 }))
        .await
        .unwrap();
    let s_batch = client
        .publish_batch(
            "/wirebatch-mix",
            vec![
                json!({ "k": "b", "v": 2 }),
                json!({ "k": "c", "v": 3 }),
            ],
        )
        .await
        .unwrap();
    let s2 = client
        .publish("/wirebatch-mix", json!({ "k": "d", "v": 4 }))
        .await
        .unwrap();

    assert_eq!(s1, 1);
    assert_eq!(s_batch, vec![2, 3]);
    assert_eq!(s2, 4);
}

//! e2e: extended bookmark replay modes.
//!
//! Three modes beyond explicit-sequence:
//!   - `subscribe_since_timestamp(ts_ms)` — scan the txlog for the
//!     first entry at/after `ts_ms` and replay from there.
//!   - `subscribe_most_recent(client_name)` — resume from the
//!     highest sequence the server previously delivered to this
//!     client_name (across reconnects).
//!   - (Existing) explicit `bookmark: u64` — verbatim sequence.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::test]
async fn since_timestamp_replays_only_post_cutoff_entries() {
    let topic = TopicSpec::new("/ts-replay", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Phase 1: publish 5 rows BEFORE the cutoff.
    for i in 0..5 {
        client
            .publish("/ts-replay", json!({ "k": format!("pre-{i}"), "v": i as f64 }))
            .await
            .unwrap();
    }
    // Settle, then mark cutoff timestamp.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let cutoff_ms = now_ms();
    // Make sure the timestamp axis advances past the in-flight writes.
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Phase 2: publish 4 rows AFTER the cutoff.
    for i in 0..4 {
        client
            .publish("/ts-replay", json!({ "k": format!("post-{i}"), "v": (10 + i) as f64 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Now subscribe with since_timestamp_ms = cutoff. Expect 4 replay
    // deltas, all from the `post-*` set.
    let mut sub = client
        .subscribe_since_timestamp("/ts-replay", None, cutoff_ms)
        .await
        .expect("subscribe_since_timestamp");

    let mut seen_keys: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while seen_keys.len() < 4 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                    seen_keys.push(k.to_string());
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        seen_keys.len(),
        4,
        "expected 4 replay deltas for the `post-*` set, got {seen_keys:?}"
    );
    for k in &seen_keys {
        assert!(
            k.starts_with("post-"),
            "leaked a pre-cutoff key into the replay stream: {k}"
        );
    }
}

#[tokio::test]
async fn most_recent_resumes_from_last_delivered_sequence() {
    // First connection publishes + reads N rows live, recording
    // the last seq it saw. Second connection (same client_name)
    // uses MOST_RECENT and only sees rows published in between.
    let topic = TopicSpec::new("/mr-replay", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server(vec![topic]).await;

    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");

    // Connection #1: opens a sub with the named client and reads
    // its first round.
    let consumer1 = Client::connect(&server.tcp_url()).await.expect("consumer1");
    let mut sub1 = consumer1
        .subscribe_most_recent("/mr-replay", None, "client-A")
        .await
        .expect("first MOST_RECENT");

    // Publish 3 rows.
    for i in 0..3 {
        publisher
            .publish("/mr-replay", json!({ "k": format!("R1-{i}"), "v": i as f64 }))
            .await
            .unwrap();
    }
    // Drain consumer1's deltas (so the server records last_seq).
    let mut last_round1 = 0;
    let deadline = std::time::Instant::now() + Duration::from_millis(800);
    while last_round1 < 3 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(200), sub1.next_delta()).await
        {
            if d.data.get("k").is_some() {
                last_round1 += 1;
            }
        } else {
            break;
        }
    }
    assert_eq!(last_round1, 3, "consumer1 should have seen 3 deltas");
    drop(sub1);
    drop(consumer1);
    // Give the server a beat to register the disconnect.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish 4 more rows while client-A is "away".
    for i in 0..4 {
        publisher
            .publish("/mr-replay", json!({ "k": format!("R2-{i}"), "v": (10 + i) as f64 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Reconnect as client-A with MOST_RECENT. Should see only R2-*.
    let consumer2 = Client::connect(&server.tcp_url()).await.expect("consumer2");
    let mut sub2 = consumer2
        .subscribe_most_recent("/mr-replay", None, "client-A")
        .await
        .expect("second MOST_RECENT");

    let mut keys: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while keys.len() < 4 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(400), sub2.next_delta()).await
        {
            if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                keys.push(k.to_string());
            }
        } else {
            break;
        }
    }
    assert_eq!(
        keys.len(),
        4,
        "MOST_RECENT should have replayed exactly the 4 R2-* rows, got {keys:?}"
    );
    for k in &keys {
        assert!(
            k.starts_with("R2-"),
            "MOST_RECENT leaked an R1-* row: {k}"
        );
    }
}

#[tokio::test]
async fn most_recent_for_unknown_client_falls_through_to_snapshot_plus_live() {
    // Semantic decision: when MOST_RECENT can't resolve a prior
    // bookmark, fall through to the regular `SowAndSubscribe`
    // semantics — current SOW snapshot + live deltas. That gives
    // the new client the latest state immediately, which is what
    // most dashboard clients want on first connect.
    let topic = TopicSpec::new("/mr-newclient", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed some rows BEFORE the subscriber appears.
    for i in 0..3 {
        client
            .publish("/mr-newclient", json!({ "k": format!("seed-{i}"), "v": i as f64 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sub = client
        .subscribe_most_recent("/mr-newclient", None, "never-seen-before")
        .await
        .expect("MOST_RECENT for unknown client");

    // Drain the snapshot.
    let mut seen: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while seen.len() < 3 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(300), sub.next_delta()).await
        {
            if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                seen.push(k.to_string());
            }
        } else {
            break;
        }
    }
    assert_eq!(seen.len(), 3, "expected 3 snapshot rows, got {seen:?}");
    for k in &seen {
        assert!(k.starts_with("seed-"), "snapshot leaked unexpected key: {k}");
    }

    // Now publish one more — must arrive as a live delta.
    client
        .publish("/mr-newclient", json!({ "k": "live-1", "v": 100.0 }))
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_millis(800), sub.next_delta())
        .await
        .expect("timed out waiting for live delta")
        .expect("subscription closed");
    let k = received.data.get("k").unwrap().as_str().unwrap();
    assert_eq!(k, "live-1", "expected live-1 as the next delta after snapshot");
}

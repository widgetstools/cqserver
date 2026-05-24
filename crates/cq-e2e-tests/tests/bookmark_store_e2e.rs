//! S18 — client-side persistent bookmark store end-to-end.
//!
//! Scenario:
//!   1. Spin up a server with a persistent topic.
//!   2. Connect Client A with a `LocalBookmarkStore` rooted in a
//!      tempdir; subscribe and read N deltas, persisting the store.
//!   3. Drop Client A.
//!   4. Connect Client B against the same store path; subscribe to
//!      the same topic; expect to receive ONLY entries after the
//!      previously-recorded high-water (no replay of the first N).

use cq_client::{Client, LocalBookmarkStore};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn second_client_resumes_from_persisted_bookmark() {
    // Persistent topic so the server can replay strictly post-bookmark
    // entries from its txlog on the second subscribe.
    let topic = TopicSpec::new("/bm-e2e", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist();
    let server = start_server(vec![topic]).await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let tmp = tempfile::tempdir().expect("tempdir");
    let bm_path = tmp.path().join("bookmarks.json");

    // ─── Client A: subscribe to empty topic, then producer publishes
    //     10 rows → arrive as LIVE deltas (with sequence) on client A,
    //     bookmark records the high-water. ───
    let highest_seq_a = {
        let client = Client::connect(&server.tcp_url()).await.expect("client A");
        let store = LocalBookmarkStore::load(&bm_path).expect("load empty store");
        client.set_bookmark_store(store);

        let mut sub = client
            .sow_and_subscribe("/bm-e2e", None, None)
            .await
            .expect("subscribe A");

        // Settle the subscribe before publishing so deltas arrive live.
        tokio::time::sleep(Duration::from_millis(120)).await;

        for i in 0..10 {
            producer
                .publish("/bm-e2e", json!({ "k": format!("k{i:03}"), "v": i }))
                .await
                .unwrap();
        }

        let mut seen = 0u32;
        let mut highest_seq = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while seen < 10 && std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
                Ok(Some(d)) => {
                    if let Some(seq) = d.sequence {
                        if seq > highest_seq {
                            highest_seq = seq;
                        }
                    }
                    if d.data.get("k").is_some() {
                        seen += 1;
                    }
                }
                _ => break,
            }
        }
        sub.persist_bookmark().expect("persist");
        let snap = client.bookmark_store().expect("store").get("/bm-e2e");
        assert!(
            snap.unwrap_or(0) > 0,
            "expected non-zero stored bookmark after 10 live deltas; highest_seq = {}",
            highest_seq
        );
        highest_seq
    };

    // ─── Producer publishes 5 more rows AFTER client A closed. These
    //     are appended to the topic's txlog so the post-bookmark
    //     replay covers them. ───
    for i in 10..15 {
        producer
            .publish("/bm-e2e", json!({ "k": format!("k{i:03}"), "v": i }))
            .await
            .unwrap();
    }

    // ─── Client B: reopens the bookmark file, subscribes, and on the
    //     wire passes `bookmark = highest_seq_a` so the server replays
    //     strictly post-bookmark entries (k010..k014). ───
    let client_b = Client::connect(&server.tcp_url()).await.expect("client B");
    let store_b = LocalBookmarkStore::load(&bm_path).expect("reload store");
    assert_eq!(
        store_b.get("/bm-e2e").unwrap_or(0),
        highest_seq_a,
        "reloaded store should preserve client A's high-water"
    );
    client_b.set_bookmark_store(store_b);

    let mut sub_b = client_b
        .sow_and_subscribe("/bm-e2e", None, None)
        .await
        .expect("subscribe B");

    // Collect deltas + assert none of the pre-bookmark keys appear.
    let mut seen_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while seen_keys.len() < 5 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), sub_b.next_delta()).await {
            Ok(Some(d)) => {
                if let Some(k) = d.data.get("k").and_then(Value::as_str) {
                    seen_keys.insert(k.to_string());
                }
            }
            _ => break,
        }
    }
    // The post-bookmark keys (k010..k014) should all be present.
    for i in 10..15 {
        let k = format!("k{i:03}");
        assert!(
            seen_keys.contains(&k),
            "expected post-bookmark key {} in resumed stream; got: {:?}",
            k,
            seen_keys
        );
    }
    // Pre-bookmark keys (k000..k009) must NOT appear — they were
    // already covered.
    for i in 0..10 {
        let k = format!("k{i:03}");
        assert!(
            !seen_keys.contains(&k),
            "pre-bookmark key {} should NOT have been re-replayed; got: {:?}",
            k,
            seen_keys
        );
    }
}

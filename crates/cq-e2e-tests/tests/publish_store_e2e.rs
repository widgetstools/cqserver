//! S17 — client-side persistent publish buffer end-to-end.
//!
//! Scenario A: a publish recorded into the store is dropped from the
//! store after a successful ack.
//!
//! Scenario B: a process restart simulation — manually pre-populate
//! the on-disk store with 3 publishes (as if a previous Client died
//! after recording but before acking), reload into a fresh Client,
//! call `replay_publish_store`, and verify the server received all
//! three (the topic ends up with three rows).

use cq_client::{Client, LocalPublishStore};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};

#[tokio::test]
async fn publish_completes_drop_entries_from_store_on_ack() {
    let topic = TopicSpec::new("/pub-store-a", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let tmp = tempfile::tempdir().unwrap();
    let store = LocalPublishStore::load(tmp.path().join("publishes.json")).unwrap();
    client.set_publish_store(store.clone());

    for i in 0..5 {
        client
            .publish(
                "/pub-store-a",
                json!({ "k": format!("k{i}"), "v": i as i64 }),
            )
            .await
            .expect("publish");
    }

    // Each publish should have round-tripped through the store and
    // been dropped on ack — net pending count is zero.
    assert_eq!(
        store.pending_count(),
        0,
        "expected no pending entries after successful publishes"
    );
}

#[tokio::test]
async fn replay_publish_store_flushes_pre_existing_entries() {
    let topic = TopicSpec::new("/pub-store-b", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist();
    let server = start_server(vec![topic]).await;

    let tmp = tempfile::tempdir().unwrap();
    let store_path = tmp.path().join("publishes.json");

    // ─── "Previous process": record 3 publishes, persist, drop without
    //     completing — simulates a crash between record + ack. ───
    {
        let store = LocalPublishStore::load(&store_path).unwrap();
        for i in 0..3 {
            store.record(
                "/pub-store-b",
                json!({ "k": format!("orphan{i}"), "v": (i as i64) * 10 }),
            );
        }
        store.persist().expect("persist");
        assert_eq!(store.pending_count(), 3);
    }

    // ─── New Client picks up the on-disk store and replays. Server
    //     should receive all 3 entries; topic ends up with 3 rows. ───
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let reloaded = LocalPublishStore::load(&store_path).expect("reload");
    assert_eq!(reloaded.pending_count(), 3);
    client.set_publish_store(reloaded);

    let replayed = client.replay_publish_store().await.expect("replay");
    assert_eq!(replayed, 3);
    assert_eq!(
        client.publish_store().unwrap().pending_count(),
        0,
        "store should be empty after a successful replay"
    );

    // Verify the server actually has the 3 rows.
    let rows = client.sow("/pub-store-b", None).await.expect("sow");
    let keys: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|r| {
            r.get("k").and_then(Value::as_str).map(|s| s.to_string())
        })
        .collect();
    for i in 0..3 {
        let k = format!("orphan{i}");
        assert!(
            keys.contains(&k),
            "expected replayed key {} on the server; got: {:?}",
            k,
            keys
        );
    }
}

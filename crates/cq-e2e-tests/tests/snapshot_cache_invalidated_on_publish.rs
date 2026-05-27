//! Regression test for the snapshot-cache staleness bug:
//!
//!     T0: SOW (topic, sql)              → cache MISS → build → cache RESPONSE (TTL 500 ms)
//!     T1: publish updates the row
//!     T2: SOW (topic, sql) again        → must see post-publish row
//!                                          (NOT the cached pre-publish copy)
//!
//! Before the fix, the second SOW served the pre-publish entry from
//! the cache because invalidation only happened on TTL expiry.
//!
//! The cache lives in `cq-transport`; the only way to exercise it is
//! through the real wire path (start_server + Client).

use std::time::Duration;

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;

#[tokio::test]
async fn second_sow_after_publish_sees_post_publish_row() {
    let topic = TopicSpec::new("/cache-invalidation", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/cache-invalidation", json!({ "k": "K1", "v": 1 }))
        .await
        .expect("publish v=1");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // First SOW — populates the cache.
    let first = client
        .sow("/cache-invalidation", None)
        .await
        .expect("first sow");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["v"], serde_json::json!(1));

    // Update the row.
    client
        .publish("/cache-invalidation", json!({ "k": "K1", "v": 2 }))
        .await
        .expect("publish v=2");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Second SOW within the 500 ms cache TTL. Must see v=2, not the
    // cached v=1 entry. (Before the fix, this returned v=1.)
    let second = client
        .sow("/cache-invalidation", None)
        .await
        .expect("second sow");
    assert_eq!(second.len(), 1);
    assert_eq!(
        second[0]["v"],
        serde_json::json!(2),
        "second SOW must reflect the publish that happened between the two SOWs"
    );
}

#[tokio::test]
async fn batch_publish_also_invalidates_cache() {
    let topic = TopicSpec::new("/cache-invalidation-batch", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/cache-invalidation-batch", json!({ "k": "K1", "v": 1 }))
        .await
        .expect("seed");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _seed_sow = client
        .sow("/cache-invalidation-batch", None)
        .await
        .expect("seed sow"); // populates cache

    client
        .publish_batch(
            "/cache-invalidation-batch",
            vec![
                json!({ "k": "K1", "v": 10 }),
                json!({ "k": "K2", "v": 20 }),
            ],
        )
        .await
        .expect("batch publish");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = client
        .sow("/cache-invalidation-batch", None)
        .await
        .expect("post-batch sow");
    assert_eq!(after.len(), 2, "should see both rows from the batch");
    let mut vs: Vec<i64> = after.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    vs.sort();
    assert_eq!(vs, vec![10, 20]);
}

#[tokio::test]
async fn delete_also_invalidates_cache() {
    let topic = TopicSpec::new("/cache-invalidation-del", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/cache-invalidation-del", json!({ "k": "K1", "v": 1 }))
        .await
        .expect("publish");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let _seed_sow = client
        .sow("/cache-invalidation-del", None)
        .await
        .expect("seed sow"); // populates cache

    client
        .sow_delete("/cache-invalidation-del", "K1")
        .await
        .expect("delete K1");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let after = client
        .sow("/cache-invalidation-del", None)
        .await
        .expect("post-delete sow");
    assert!(
        after.is_empty(),
        "SOW after delete must NOT return the deleted row from a stale cache, got {after:?}"
    );
}

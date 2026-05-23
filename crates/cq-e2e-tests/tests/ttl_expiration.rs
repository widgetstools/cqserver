//! e2e: rows on a TTL-configured topic disappear after the TTL
//! window, even with no explicit deletion. SOW queries return
//! nothing and subscribers receive a `Remove` delta.

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn rows_expire_and_subscribers_see_remove() {
    let topic = TopicSpec::new("/ttl-rows", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_expire_seconds(1);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    let mut sub = subscriber
        .sow_and_subscribe("/ttl-rows", None, None)
        .await
        .expect("subscribe");

    publisher
        .publish("/ttl-rows", json!({ "k": "X", "v": 1.0 }))
        .await
        .unwrap();

    // Snapshot will be empty (sub came up before publish). Wait for
    // the live Add.
    let d = tokio::time::timeout(Duration::from_millis(800), sub.next_delta())
        .await
        .expect("timeout for Add")
        .expect("closed");
    assert!(matches!(d.delta_type, DeltaKind::Add | DeltaKind::SowSnapshot));

    // Wait > TTL + sweep tick (>= 1s + ~200ms slack).
    tokio::time::sleep(Duration::from_millis(1700)).await;

    // SOW query should return zero rows.
    let rows = publisher.sow("/ttl-rows", None).await.expect("sow");
    assert!(
        rows.is_empty(),
        "expected SOW empty after TTL expiry, got {rows:?}"
    );

    // Subscriber should have received a Remove delta during the
    // expiration window.
    let mut saw_remove = false;
    let deadline = std::time::Instant::now() + Duration::from_millis(800);
    while !saw_remove && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await
        {
            if matches!(d.delta_type, DeltaKind::Remove) {
                saw_remove = true;
                break;
            }
        } else {
            break;
        }
    }
    assert!(saw_remove, "subscriber never received Remove after TTL expiry");
}

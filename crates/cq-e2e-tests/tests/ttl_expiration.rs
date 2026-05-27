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

// ───── Diversification ────────────────────────────────────────────

/// Publishing again (touching the key) before TTL expiry resets the
/// last-touched clock — the row survives.
#[tokio::test]
async fn republish_before_ttl_resets_clock() {
    let topic = TopicSpec::new("/ttl-refresh", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_expire_seconds(2);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/ttl-refresh", json!({ "k": "X", "v": 1 }))
        .await
        .unwrap();
    // Refresh halfway through the TTL window.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    client
        .publish("/ttl-refresh", json!({ "k": "X", "v": 2 }))
        .await
        .unwrap();
    // Wait another 1s — original would have expired (2s), but refresh
    // happened at 1s so the row's clock restarted.
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let rows = client.sow("/ttl-refresh", None).await.unwrap();
    assert_eq!(rows.len(), 1, "refreshed row must survive: {rows:?}");
    assert_eq!(rows[0].get("v").unwrap().as_i64().unwrap(), 2);
}

/// Multiple rows expire independently — only the older row drops.
/// Uses generous margins because the TTL sweeper runs at ~1Hz: we
/// need to be well past `expire_seconds + sweep_period` to guarantee
/// the old row has been evicted.
#[tokio::test]
async fn ttl_expires_rows_independently() {
    let topic = TopicSpec::new("/ttl-indep", "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_expire_seconds(2);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/ttl-indep", json!({ "k": "old", "v": 1 }))
        .await
        .unwrap();
    // Wait 2.5s, then publish a younger row. `old` has aged through
    // its 2s TTL but the sweeper may not have fired yet — that's fine.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    client
        .publish("/ttl-indep", json!({ "k": "young", "v": 2 }))
        .await
        .unwrap();
    // Wait long enough that `old` is comfortably past TTL + sweep
    // period AND `young` is still inside its 2s window. With young
    // published at ~2.5s mark and TTL=2s, the window closes around
    // 4.5s. Wait 1.3s here → total 3.8s. young still alive, old well
    // past expiry + sweep tick.
    tokio::time::sleep(Duration::from_millis(1300)).await;

    let rows = client.sow("/ttl-indep", None).await.unwrap();
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("young"), "young row should still be alive: {ks:?}");
    assert!(!ks.contains("old"), "old row should have expired: {ks:?}");
}

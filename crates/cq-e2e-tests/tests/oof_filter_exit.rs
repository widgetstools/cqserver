//! e2e: a row that leaves a subscription's filter via an upsert
//! produces an `oof` delta, while a row that gets deleted produces
//! a `remove` delta. Pins the AMPS semantic distinction.

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn upsert_flip_emits_oof_delete_emits_remove() {
    let topic = TopicSpec::new("/oof-trades", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");
    let subscriber = Client::connect(&server.tcp_url()).await.expect("sub");

    // Seed two rows on RATES desk.
    for k in ["T1", "T2"] {
        publisher
            .publish(
                "/oof-trades",
                json!({ "k": k, "desk": "RATES", "price": 100.0 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Subscribe to RATES only.
    let mut sub = subscriber
        .sow_and_subscribe("/oof-trades", Some("desk = 'RATES'"), None)
        .await
        .expect("sub");

    // Drain the snapshot (2 Add deltas).
    let mut snapshot = 0;
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while snapshot < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await
        {
            if matches!(d.delta_type, DeltaKind::SowSnapshot | DeltaKind::Add) {
                snapshot += 1;
            }
        } else {
            break;
        }
    }
    assert_eq!(snapshot, 2, "expected 2 snapshot rows");

    // Flip T1 to EQUITIES → predicate-flip → should produce Oof.
    publisher
        .publish(
            "/oof-trades",
            json!({ "k": "T1", "desk": "EQUITIES", "price": 100.0 }),
        )
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_millis(500), sub.next_delta())
        .await
        .expect("timeout waiting for oof")
        .expect("sub closed");
    assert_eq!(
        d.delta_type,
        DeltaKind::Oof,
        "predicate-flip should produce Oof, got {:?}",
        d.delta_type
    );

    // Delete T2 → real delete → should produce Remove.
    publisher.sow_delete("/oof-trades", "T2").await.unwrap();
    let d = tokio::time::timeout(Duration::from_millis(500), sub.next_delta())
        .await
        .expect("timeout waiting for remove")
        .expect("sub closed");
    assert_eq!(
        d.delta_type,
        DeltaKind::Remove,
        "actual delete should produce Remove, got {:?}",
        d.delta_type
    );
}

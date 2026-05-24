//! e2e: queue lease + redelivery. Consumer A subscribes, receives a
//! message but doesn't ack; after the lease window expires, the
//! message is redelivered to consumer B.

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server_with, QueueSpec, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn queue_lease_redelivers_to_other_consumer() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/work").with_lease(150)],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
        },
    )
    .await;

    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let consumer_a = Client::connect(&server.tcp_url()).await.expect("a");
    let consumer_b = Client::connect(&server.tcp_url()).await.expect("b");

    let mut sub_a = consumer_a
        .sow_and_subscribe("/work", None, None)
        .await
        .expect("subscribe a");
    let mut sub_b = consumer_b
        .sow_and_subscribe("/work", None, None)
        .await
        .expect("subscribe b");

    // One publish — should go to A (first registered).
    publisher
        .publish("/work", json!({ "task": "T1" }))
        .await
        .expect("publish");

    let d_a = tokio::time::timeout(Duration::from_millis(500), sub_a.next_delta())
        .await
        .expect("a timeout")
        .expect("a closed");
    assert!(matches!(d_a.delta_type, DeltaKind::Add | DeltaKind::Update));
    assert_eq!(d_a.data.get("task").and_then(|v| v.as_str()), Some("T1"));
    let did = d_a
        .delivery_id
        .expect("expected delivery_id on leased queue delivery");

    // B should NOT have received anything yet.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), sub_b.next_delta())
            .await
            .is_err(),
        "B got the message before A's lease expired"
    );

    // Don't ack. Wait for lease to expire + sweeper to redeliver.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let d_b = tokio::time::timeout(Duration::from_millis(800), sub_b.next_delta())
        .await
        .expect("b timeout — redelivery never fired")
        .expect("b closed");
    assert_eq!(d_b.data.get("task").and_then(|v| v.as_str()), Some("T1"));
    let did_b = d_b.delivery_id.expect("expected delivery_id on redelivery");
    assert_ne!(did, did_b, "redelivery should get a fresh delivery_id");
}

#[tokio::test]
async fn queue_ack_prevents_redelivery() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/acked").with_lease(150)],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
        },
    )
    .await;

    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let consumer_a = Client::connect(&server.tcp_url()).await.expect("a");
    let consumer_b = Client::connect(&server.tcp_url()).await.expect("b");

    let mut sub_a = consumer_a
        .sow_and_subscribe("/acked", None, None)
        .await
        .expect("a sub");
    let mut sub_b = consumer_b
        .sow_and_subscribe("/acked", None, None)
        .await
        .expect("b sub");

    publisher
        .publish("/acked", json!({ "task": "T2" }))
        .await
        .expect("publish");

    let d = tokio::time::timeout(Duration::from_millis(500), sub_a.next_delta())
        .await
        .expect("timeout")
        .expect("closed");
    let did = d.delivery_id.expect("did missing");
    // Ack before lease expires.
    consumer_a.queue_ack("/acked", did).await.expect("ack");

    // After lease window, B should NOT have received a redelivery.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), sub_b.next_delta())
            .await
            .is_err(),
        "B got a redelivery after A acked"
    );
}

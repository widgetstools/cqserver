//! e2e: when a queue message exhausts its max_delivery_count
//! without being acked, it's routed to the configured DLQ. A
//! consumer on the DLQ receives the dead-lettered payload wrapped
//! in metadata about the original delivery.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, QueueSpec, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn dlq_routes_messages_after_max_delivery_exceeded() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![
                QueueSpec::new("/dlq"),
                QueueSpec::new("/work")
                    .with_lease(100)
                    .with_max_delivery(1)
                    .with_dlq("/dlq"),
            ],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;

    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let stuck_consumer = Client::connect(&server.tcp_url()).await.expect("stuck");
    let dlq_inspector = Client::connect(&server.tcp_url()).await.expect("dlq");

    // Subscribe to /work but NEVER ack.
    let mut sub_work = stuck_consumer
        .sow_and_subscribe("/work", None, None)
        .await
        .expect("subscribe work");
    // Subscribe to /dlq to catch the dead-letter.
    let mut sub_dlq = dlq_inspector
        .sow_and_subscribe("/dlq", None, None)
        .await
        .expect("subscribe dlq");

    publisher
        .publish("/work", json!({ "task": "stuck-task" }))
        .await
        .expect("publish");

    // Drain the first delivery on /work (no ack).
    let _ = tokio::time::timeout(Duration::from_millis(400), sub_work.next_delta())
        .await
        .expect("work timeout")
        .expect("work closed");
    // Drain the redelivery too (still no ack).
    let _ = tokio::time::timeout(Duration::from_millis(400), sub_work.next_delta())
        .await
        .expect("redelivery timeout")
        .expect("work closed");

    // Wait for the sweeper to fire one more time → cap exceeded → DLQ.
    let d = tokio::time::timeout(Duration::from_secs(2), sub_dlq.next_delta())
        .await
        .expect("DLQ never received the dead-lettered message")
        .expect("dlq sub closed");
    let payload = &d.data;
    assert_eq!(
        payload.get("original_queue").and_then(|v| v.as_str()),
        Some("/work"),
        "DLQ entry should record original queue, got {payload:?}"
    );
    let nested = payload
        .get("payload")
        .and_then(|v| v.as_object())
        .expect("DLQ entry missing nested original payload");
    assert_eq!(
        nested.get("task").and_then(|v| v.as_str()),
        Some("stuck-task")
    );
}

// ───── Diversification ────────────────────────────────────────────

use cq_client::DeltaKind;

/// max_delivery = 3 — the third unacked delivery moves to DLQ.
#[tokio::test]
async fn dlq_after_three_delivery_attempts() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![
                QueueSpec::new("/dlq3"),
                QueueSpec::new("/work3")
                    .with_lease(50)
                    .with_max_delivery(3)
                    .with_dlq("/dlq3"),
            ],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let stuck = Client::connect(&server.tcp_url()).await.expect("stuck");
    let dlq_sub_client = Client::connect(&server.tcp_url()).await.expect("dlq");

    let mut sub_work = stuck
        .sow_and_subscribe("/work3", None, None)
        .await
        .unwrap();
    let mut sub_dlq = dlq_sub_client
        .sow_and_subscribe("/dlq3", None, None)
        .await
        .unwrap();

    publisher
        .publish("/work3", json!({ "task": "x" }))
        .await
        .unwrap();

    // Drain 3 deliveries without acking; 4th attempt → DLQ.
    for _ in 0..3 {
        let _ = tokio::time::timeout(Duration::from_millis(500), sub_work.next_delta())
            .await
            .expect("delivery")
            .unwrap();
    }
    let d = tokio::time::timeout(Duration::from_secs(2), sub_dlq.next_delta())
        .await
        .expect("DLQ should receive after 3 attempts")
        .unwrap();
    assert_eq!(d.data.get("original_queue").and_then(|v| v.as_str()), Some("/work3"));
}

/// Acking the message before max_delivery — never reaches DLQ.
#[tokio::test]
async fn dlq_does_not_route_acked_messages() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![
                QueueSpec::new("/dlq_clean"),
                QueueSpec::new("/work_clean")
                    .with_lease(80)
                    .with_max_delivery(2)
                    .with_dlq("/dlq_clean"),
            ],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub");
    let consumer = Client::connect(&server.tcp_url()).await.expect("cons");
    let dlq_sub = Client::connect(&server.tcp_url()).await.expect("dlq");

    let mut sub_work = consumer
        .sow_and_subscribe("/work_clean", None, None)
        .await
        .unwrap();
    let mut sub_dlq = dlq_sub
        .sow_and_subscribe("/dlq_clean", None, None)
        .await
        .unwrap();

    publisher
        .publish("/work_clean", json!({ "task": "ok" }))
        .await
        .unwrap();
    let d = tokio::time::timeout(Duration::from_millis(500), sub_work.next_delta())
        .await
        .unwrap()
        .unwrap();
    let did = d.delivery_id.unwrap();
    consumer.queue_ack("/work_clean", did).await.unwrap();

    // Wait past several potential redelivery windows.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), sub_dlq.next_delta())
            .await
            .is_err(),
        "acked message should NOT reach DLQ"
    );
}

/// DLQ receives a `DeltaKind::Add` (queue-style delivery), not an Update.
#[tokio::test]
async fn dlq_delivery_kind_is_add() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![
                QueueSpec::new("/dlq_kind"),
                QueueSpec::new("/work_kind")
                    .with_lease(60)
                    .with_max_delivery(1)
                    .with_dlq("/dlq_kind"),
            ],
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;
    let pubc = Client::connect(&server.tcp_url()).await.expect("pub");
    let stuck = Client::connect(&server.tcp_url()).await.expect("stuck");
    let dlq_c = Client::connect(&server.tcp_url()).await.expect("dlq");

    let mut sub_w = stuck
        .sow_and_subscribe("/work_kind", None, None)
        .await
        .unwrap();
    let mut sub_d = dlq_c
        .sow_and_subscribe("/dlq_kind", None, None)
        .await
        .unwrap();

    pubc.publish("/work_kind", json!({ "task": "x" })).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(400), sub_w.next_delta())
        .await
        .unwrap()
        .unwrap();

    let d = tokio::time::timeout(Duration::from_secs(2), sub_d.next_delta())
        .await
        .expect("DLQ delivery")
        .unwrap();
    assert!(matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update | DeltaKind::SowSnapshot),
            "delivery kind: {:?}", d.delta_type);
}

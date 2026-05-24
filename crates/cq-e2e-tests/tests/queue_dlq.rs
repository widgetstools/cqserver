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

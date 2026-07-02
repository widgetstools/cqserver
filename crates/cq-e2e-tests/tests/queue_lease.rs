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
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
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
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
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

// ───── Diversification ────────────────────────────────────────────

/// Round-robin: with 2 consumers, 2 messages distribute across both
/// (not both to the first).
#[tokio::test]
async fn queue_distributes_across_consumers() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/work_rr").with_lease(1000)],
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
    let a = Client::connect(&server.tcp_url()).await.unwrap();
    let b = Client::connect(&server.tcp_url()).await.unwrap();
    let mut sub_a = a.sow_and_subscribe("/work_rr", None, None).await.unwrap();
    let mut sub_b = b.sow_and_subscribe("/work_rr", None, None).await.unwrap();

    publisher.publish("/work_rr", json!({ "task": "T1" })).await.unwrap();
    publisher.publish("/work_rr", json!({ "task": "T2" })).await.unwrap();

    let d1 = tokio::time::timeout(Duration::from_millis(500), sub_a.next_delta())
        .await
        .unwrap()
        .unwrap();
    let d2 = tokio::time::timeout(Duration::from_millis(500), sub_b.next_delta())
        .await
        .unwrap()
        .unwrap();
    let mut seen = vec![
        d1.data.get("task").and_then(|v| v.as_str()).unwrap().to_string(),
        d2.data.get("task").and_then(|v| v.as_str()).unwrap().to_string(),
    ];
    seen.sort();
    assert_eq!(seen, vec!["T1", "T2"], "both tasks distributed across consumers");
}

/// Consumer disconnects mid-lease → message redelivered to another
/// consumer once lease expires.
#[tokio::test]
async fn queue_redelivers_after_consumer_disconnects() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/disc_q").with_lease(150)],
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
    let pubc = Client::connect(&server.tcp_url()).await.unwrap();
    let b = Client::connect(&server.tcp_url()).await.unwrap();
    let mut sub_b = b.sow_and_subscribe("/disc_q", None, None).await.unwrap();

    {
        let a = Client::connect(&server.tcp_url()).await.unwrap();
        let mut sub_a = a.sow_and_subscribe("/disc_q", None, None).await.unwrap();
        pubc.publish("/disc_q", json!({ "task": "lost" })).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_millis(500), sub_a.next_delta())
            .await
            .unwrap()
            .unwrap();
        // A drops here — implicit consumer.
    }

    // After lease expiry, B should receive the redelivery.
    let d = tokio::time::timeout(Duration::from_secs(2), sub_b.next_delta())
        .await
        .expect("B should get redelivery after A's disconnect")
        .unwrap();
    assert_eq!(d.data.get("task").and_then(|v| v.as_str()), Some("lost"));
}

/// Lease extension: a consumer that extends its lease before the
/// window elapses keeps the message from being redelivered.
#[tokio::test]
async fn queue_lease_extension_defers_redelivery() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/ext_q").with_lease(200)],
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
    let consumer_a = Client::connect(&server.tcp_url()).await.expect("a");
    let consumer_b = Client::connect(&server.tcp_url()).await.expect("b");
    let mut sub_a = consumer_a.sow_and_subscribe("/ext_q", None, None).await.unwrap();
    let mut sub_b = consumer_b.sow_and_subscribe("/ext_q", None, None).await.unwrap();

    publisher.publish("/ext_q", json!({ "task": "slow" })).await.unwrap();
    let d = tokio::time::timeout(Duration::from_millis(500), sub_a.next_delta())
        .await
        .expect("a timeout")
        .expect("a closed");
    let did = d.delivery_id.expect("did missing");

    // Extend the lease well past the original 200ms window.
    tokio::time::sleep(Duration::from_millis(120)).await;
    consumer_a
        .queue_extend_lease("/ext_q", did, 1500)
        .await
        .expect("extend");

    // Past the ORIGINAL window — B must not see a redelivery.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(150), sub_b.next_delta())
            .await
            .is_err(),
        "extended lease was redelivered before the new window"
    );
    // Now ack to clean up.
    consumer_a.queue_ack("/ext_q", did).await.expect("ack");
}

/// Grouped delivery: messages sharing a group key all land on one
/// consumer (sticky), preserving their relative order.
#[tokio::test]
async fn queue_grouped_messages_stick_to_one_consumer() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/grp_q").with_lease(2000)],
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
    let a = Client::connect(&server.tcp_url()).await.unwrap();
    let b = Client::connect(&server.tcp_url()).await.unwrap();
    let mut sub_a = a.sow_and_subscribe("/grp_q", None, None).await.unwrap();
    let mut sub_b = b.sow_and_subscribe("/grp_q", None, None).await.unwrap();

    for i in 0..4 {
        publisher
            .publish_with_opts("/grp_q", json!({ "i": i }), 0, Some("ORD-1"))
            .await
            .unwrap();
    }

    // Collect whatever each consumer received within a short window.
    async fn drain(sub: &mut cq_client::Subscription) -> Vec<i64> {
        let mut out = Vec::new();
        while let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(250), sub.next_delta()).await
        {
            out.push(d.data.get("i").and_then(|v| v.as_i64()).unwrap());
        }
        out
    }
    let got_a = drain(&mut sub_a).await;
    let got_b = drain(&mut sub_b).await;

    let (winner, loser) = if got_a.len() == 4 { (got_a, got_b) } else { (got_b, got_a) };
    assert_eq!(winner, vec![0, 1, 2, 3], "grouped messages must arrive in order at one consumer");
    assert!(loser.is_empty(), "the other consumer must receive none of the group");
}

/// Multiple publishes interleaved with acks — every message either
/// gets acked once or redelivers; none are lost.
#[tokio::test]
async fn queue_no_message_loss_under_ack_pattern() {
    let server = start_server_with(
        Vec::<TopicSpec>::new(),
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: vec![QueueSpec::new("/no_loss").with_lease(2000)],
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
    let cons = Client::connect(&server.tcp_url()).await.expect("c");
    let mut sub = cons.sow_and_subscribe("/no_loss", None, None).await.unwrap();

    for i in 0..10 {
        pubc.publish("/no_loss", json!({ "task": format!("T{i}") }))
            .await
            .unwrap();
    }

    let mut received = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while received.len() < 10 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                let task = d.data.get("task").and_then(|v| v.as_str()).unwrap_or("").to_string();
                received.insert(task);
                if let Some(did) = d.delivery_id {
                    cons.queue_ack("/no_loss", did).await.ok();
                }
            }
            _ => break,
        }
    }
    assert_eq!(received.len(), 10, "got tasks: {received:?}");
}

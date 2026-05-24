//! e2e: pause/resume on a bookmark-replay subscription.
//!
//! Publish enough rows that the per-session outbound queue can't
//! buffer the whole replay at once. Subscribe with bookmark=0
//! (EPOCH replay), drain a chunk, send Pause; verify the receive
//! count stops growing; send Resume; verify the rest arrives.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn pause_then_resume_resumes_replay_from_same_point() {
    // Small outbound queue forces the replay's await-on-send to
    // gate on the consumer. Big-enough publish count so the
    // replay can't finish before the test pauses.
    let topic = TopicSpec::new("/pause-replay", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 128,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
        },
    )
    .await;

    let pub_client = Client::connect(&server.tcp_url()).await.expect("pub");
    let n = 600;
    for i in 0..n {
        pub_client
            .publish(
                "/pause-replay",
                json!({ "k": format!("k{i:04}"), "v": i as f64 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let sub_client = Client::connect(&server.tcp_url()).await.expect("sub");
    let mut sub = sub_client
        .sow_and_subscribe("/pause-replay", None, Some(0))
        .await
        .expect("EPOCH replay sub");

    // Drain a chunk so the replay is mid-flight.
    let mut received: Vec<String> = Vec::new();
    while received.len() < 50 {
        let d = tokio::time::timeout(Duration::from_millis(500), sub.next_delta())
            .await
            .expect("timeout draining initial chunk")
            .expect("closed");
        if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
            received.push(k.to_string());
        }
    }
    sub_client.pause_subscription(&sub.sub_id).await.unwrap();

    // Drain whatever was in flight when Pause arrived. The exact
    // count varies but should be bounded by the outbound queue
    // capacity (128).
    while received.len() < 600 {
        match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
            Ok(Some(d)) => {
                if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                    received.push(k.to_string());
                }
            }
            _ => break,
        }
    }
    let count_at_pause = received.len();
    assert!(
        count_at_pause < n,
        "pause never took effect — received {count_at_pause}/{n} before resume"
    );

    // Now verify the stream is genuinely paused: no more deltas
    // for at least 400ms.
    let stall = tokio::time::timeout(Duration::from_millis(400), sub.next_delta()).await;
    assert!(
        stall.is_err(),
        "delta arrived during pause — pause did not halt the replay"
    );

    // Resume; expect the rest.
    sub_client.resume_subscription(&sub.sub_id).await.unwrap();
    while received.len() < n {
        match tokio::time::timeout(Duration::from_millis(800), sub.next_delta()).await {
            Ok(Some(d)) => {
                if let Some(k) = d.data.get("k").and_then(|v| v.as_str()) {
                    received.push(k.to_string());
                }
            }
            _ => break,
        }
    }
    assert_eq!(
        received.len(),
        n,
        "expected {n} total deltas after resume, got {}",
        received.len()
    );
    let mut sorted = received.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), n, "duplicates in replay output");
}

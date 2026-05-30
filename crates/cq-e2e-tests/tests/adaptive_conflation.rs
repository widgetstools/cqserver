//! e2e: adaptive conflation.
//!
//! When the slow-consumer watcher sees a sub's outbound queue above
//! `adaptive_fill_threshold`, it widens the conflator's flush interval
//! (doubling per scan, capped at `adaptive_max_interval_ms`) to shed
//! load without disconnecting. When pressure clears, the interval
//! halves back toward the route's baseline. This test:
//!
//!  1. Opens a non-reading subscriber so its outbound queue fills.
//!  2. Floods the topic with publishes.
//!  3. Asserts that `/subscriptions` shows the sub's
//!     `conflationIntervalMs` grow beyond its starting value.
//!  4. Asserts `cq_subscription_conflation_interval_ms{topic=...}`
//!     appears in `/metrics`.

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, ServerOpts, SlowConsumerOpts, TopicSpec,
};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn open_silent_subscriber(port: u16, topic: &str) -> TcpStream {
    let mut s = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("tcp connect");
    let msg = json!({ "c": "sow_and_subscribe", "cid": "silent", "t": topic });
    let payload = serde_json::to_vec(&msg).unwrap();
    let len = (payload.len() as u32).to_be_bytes();
    s.write_all(&len).await.unwrap();
    s.write_all(&payload).await.unwrap();
    s
}

async fn subscriptions(server: &cq_e2e_tests::ServerHandle) -> Vec<Value> {
    reqwest::get(format!("{}/subscriptions", server.admin_url()))
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()
        .as_array()
        .cloned()
        .unwrap_or_default()
}

#[tokio::test]
async fn adaptive_conflation_widens_under_pressure() {
    let schema = json!({ "k": "string", "v": "double" });
    // Topic with 25ms baseline conflation. Adaptive should widen it
    // upward when the subscriber can't keep up.
    let topic = TopicSpec::new("/adaptive", "k")
        .with_schema(schema)
        .with_conflation(25);

    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 256,
            slow_consumer: Some(SlowConsumerOpts {
                scan_interval_s: 1,
                // Don't auto-disconnect — we want to observe widening.
                drops_per_sec_threshold: 1_000_000,
                auto_disconnect: false,
                adaptive_conflation: true,
                adaptive_max_interval_ms: 2_000,
                adaptive_fill_threshold: 0.3,
            }),
            tls: None,
            queues: Vec::new(),
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
        },
    )
    .await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let _silent = open_silent_subscriber(server.tcp_port, "/adaptive").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Confirm the sub starts at the baseline interval (25 ms).
    let subs0 = subscriptions(&server).await;
    let initial_ms = subs0
        .iter()
        .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/adaptive"))
        .and_then(|s| s.get("conflationIntervalMs").and_then(|v| v.as_u64()))
        .expect("missing initial conflation interval");
    assert_eq!(initial_ms, 25, "expected baseline conflation 25ms, got {initial_ms}");

    // Spawn a steady flood so the sub's queue stays above the fill
    // threshold while the watcher scans.
    let producer_for_flood = producer.clone();
    let _flood = tokio::spawn(async move {
        for r in 0..30 {
            for i in 0..3_000 {
                let _ = producer_for_flood
                    .publish(
                        "/adaptive",
                        json!({ "k": format!("k{i:05}"), "v": (r * 3_000 + i) as f64 }),
                    )
                    .await;
            }
        }
    });

    // Watcher scans every 1s; give it up to ~10 windows to ramp up.
    // Expected progression: 25 → 50 → 100 → 200 → 400 → 800 → 1600 → 2000(cap).
    let mut max_observed_ms: u64 = initial_ms;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let subs = subscriptions(&server).await;
        if let Some(ms) = subs
            .iter()
            .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/adaptive"))
            .and_then(|s| s.get("conflationIntervalMs").and_then(|v| v.as_u64()))
        {
            if ms > max_observed_ms {
                max_observed_ms = ms;
            }
            // Bail once we've clearly widened.
            if ms >= initial_ms * 4 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    assert!(
        max_observed_ms >= initial_ms * 4,
        "expected interval to widen ≥4× under pressure (baseline {initial_ms}ms), max observed {max_observed_ms}ms"
    );
    assert!(
        max_observed_ms <= 2_000,
        "interval exceeded configured max (2000ms), observed {max_observed_ms}ms"
    );

    // Adaptive-interval gauge should be present in metrics.
    let m: String = reqwest::get(format!("{}/metrics", server.admin_url()))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let has = m.lines().any(|l| {
        l.starts_with("cq_subscription_conflation_interval_ms")
            && l.contains("topic=\"/adaptive\"")
    });
    assert!(
        has,
        "expected cq_subscription_conflation_interval_ms in /metrics"
    );
}

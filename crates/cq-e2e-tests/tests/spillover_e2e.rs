//! S21 — slow-client offlining-to-disk end-to-end test.
//!
//! Scenario: start a server with `[transport.spillover]` enabled and a
//! deliberately small `outbound_queue_capacity` so a non-reading
//! subscriber backs the queue up immediately. Flood publishes from a
//! second client. Verify:
//!   - The `/subscriptions` admin endpoint reports `dropped = 0` for
//!     the silent subscriber (spillover absorbed what would have been
//!     drops).
//!   - The Prometheus `cq_spillover_writes_total` counter is non-zero.
//!   - A spillover file exists on disk under the configured directory.
//!   - After the publisher stops, the silent reader starts draining
//!     its socket and receives every frame the server produced —
//!     bounded by the publishes we issued.

use std::time::Duration;

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, ServerOpts, SpilloverOpts, TopicSpec,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

async fn metrics_lines(server: &cq_e2e_tests::ServerHandle) -> String {
    let url = format!("{}/metrics", server.admin_url());
    reqwest::get(&url).await.unwrap().text().await.unwrap()
}

async fn subscriptions(server: &cq_e2e_tests::ServerHandle) -> Vec<Value> {
    let url = format!("{}/subscriptions", server.admin_url());
    reqwest::get(&url)
        .await
        .expect("GET /subscriptions")
        .json::<Value>()
        .await
        .expect("json")
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Counter value parsed from `/metrics` text. Aggregates all labelled
/// rows that start with `name{`, plus the unlabelled `name ` row.
fn counter_value(metrics: &str, name: &str) -> u64 {
    let mut total: u64 = 0;
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        let prefix_labelled = format!("{}{{", name);
        let prefix_plain = format!("{} ", name);
        let (matches_prefix, after) = if line.starts_with(&prefix_labelled) {
            let after = line.split_once('}').map(|(_, rest)| rest).unwrap_or("");
            (true, after)
        } else if let Some(after) = line.strip_prefix(&prefix_plain) {
            (true, after)
        } else {
            (false, "")
        };
        if matches_prefix {
            // Last whitespace-separated token is the value.
            let token = after.split_whitespace().last().unwrap_or("0");
            // Counters in the prometheus exporter render as floats; truncate.
            if let Ok(v) = token.parse::<f64>() {
                total += v as u64;
            }
        }
    }
    total
}

#[tokio::test]
async fn spillover_absorbs_drops_for_silent_consumer() {
    let topic = TopicSpec::new("/spill-e2e", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
    ]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            // Small queue so backpressure trips immediately.
            outbound_queue_capacity: 64,
            spillover: Some(SpilloverOpts {
                max_bytes_per_sub: 8 * 1024 * 1024,
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let _silent = open_silent_subscriber(server.tcp_port, "/spill-e2e").await;
    // Give the subscribe time to register.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Flood publishes. We need enough to overflow both the per-session
    // mpsc queue AND the OS-level TCP socket buffer for the silent
    // peer (which never reads). Each delta is a small JSON frame, so
    // tens of thousands are needed before the writer task actually
    // backpressures the mpsc queue. With backpressure, try_send
    // returns Full and the spillover path engages.
    for i in 0..20_000 {
        let _ = producer
            .publish("/spill-e2e", json!({ "k": format!("k{i:05}"), "v": i }))
            .await;
    }
    // Give the evaluator + spillover writer a moment.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // /subscriptions should report dropped = 0 for the silent sub —
    // spillover absorbed everything that would have been dropped.
    let subs = subscriptions(&server).await;
    let silent = subs
        .iter()
        .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/spill-e2e"))
        .expect("missing /spill-e2e subscription");
    let dropped = silent.get("dropped").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(
        dropped, 0,
        "expected 0 drops with spillover enabled, got {dropped}"
    );

    // Prometheus: cq_spillover_writes_total should be non-zero.
    let m = metrics_lines(&server).await;
    let spilled = counter_value(&m, "cq_spillover_writes_total");
    let dropped_total = counter_value(&m, "cq_deltas_dropped_total");
    let delivered_total = counter_value(&m, "cq_deltas_delivered_total");
    let spillover_lines: String = m
        .lines()
        .filter(|l| l.contains("spillover") || l.contains("delta"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        spilled > 0,
        "expected cq_spillover_writes_total > 0, got {spilled}; \
         delivered={delivered_total}, dropped={dropped_total}\n\
         Metrics:\n{spillover_lines}"
    );

    // A spillover file should exist on disk.
    let spillover_root = server
        .config_dir
        .parent()
        .unwrap()
        .join("spillover");
    let mut found_file = false;
    if let Ok(entries) = std::fs::read_dir(&spillover_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(".spill") {
                found_file = true;
                break;
            }
        }
    }
    assert!(
        found_file,
        "expected a .spill file under {}",
        spillover_root.display()
    );
}

#[tokio::test]
async fn silent_consumer_drains_spilled_backlog_when_it_starts_reading() {
    let topic = TopicSpec::new("/spill-drain", "k").with_inline_columns([
        ("k", "string"),
        ("v", "long"),
    ]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 32,
            spillover: Some(SpilloverOpts::default()),
            ..ServerOpts::default()
        },
    )
    .await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let mut silent = open_silent_subscriber(server.tcp_port, "/spill-drain").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Enough publishes to overflow both the mpsc queue and the OS
    // socket buffer for the silent peer; otherwise the writer task
    // keeps draining and spillover never engages.
    let publish_n = 10_000u64;
    for i in 0..publish_n {
        let _ = producer
            .publish("/spill-drain", json!({ "k": format!("k{i:05}"), "v": i as i64 }))
            .await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Now start reading from the raw socket. Count length-prefixed
    // frames; we should eventually see the full backlog (snapshot is
    // empty since the topic was empty at subscribe time, so frames
    // are pure live + spilled-then-drained).
    let mut frames_received: u64 = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut len_buf = [0u8; 4];
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(
            Duration::from_millis(500),
            silent.read_exact(&mut len_buf),
        )
        .await
        {
            Ok(Ok(_)) => {
                let len = u32::from_be_bytes(len_buf) as usize;
                if len == 0 || len > 16 * 1024 * 1024 {
                    break;
                }
                let mut buf = vec![0u8; len];
                if silent.read_exact(&mut buf).await.is_err() {
                    break;
                }
                frames_received += 1;
                if frames_received >= publish_n {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        frames_received >= publish_n,
        "expected to drain ≥ {publish_n} frames; received {frames_received}"
    );

    // The spillover file should be empty (or absent) once the backlog
    // is drained. Pending bytes are visible via /metrics indirectly:
    // cq_spillover_drained_total should be > 0.
    let m = metrics_lines(&server).await;
    let drained = counter_value(&m, "cq_spillover_drained_total");
    assert!(
        drained > 0,
        "expected cq_spillover_drained_total > 0 once consumer caught up, got {drained}"
    );
}

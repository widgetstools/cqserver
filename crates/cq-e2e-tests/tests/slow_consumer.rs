//! e2e: slow-consumer detection + admin disconnect.
//!
//! Strategy: open a raw TCP socket and subscribe, but *never read*
//! incoming frames. The server's outbound queue for that session fills
//! up; once it's full, the topic's delta deliveries (conflated, via
//! `try_send`) start dropping. We then assert:
//!
//!   1. `/subscriptions` lists the sub with non-zero `dropped` and a
//!      `fillRatio` close to 1.0.
//!   2. `cq_subscription_dropped_total{topic="..."}` increments.
//!   3. `DELETE /subscriptions/{sid}` removes the sub from the registry
//!      and the topic stops trying to deliver to it.
//!   4. When the watcher's `auto_disconnect` is on, the sub vanishes
//!      from `/subscriptions` once the drop rate crosses the threshold,
//!      without an admin call.

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, topic_stats, ServerOpts, SlowConsumerOpts, TopicSpec,
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
    // Deliberately do not read.
    s
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

async fn metrics_lines(server: &cq_e2e_tests::ServerHandle) -> String {
    let url = format!("{}/metrics", server.admin_url());
    reqwest::get(&url).await.unwrap().text().await.unwrap()
}

fn flood_topic_with_publishes(
    client: &Client,
    topic: &str,
    n_keys: usize,
    rounds: usize,
) -> tokio::task::JoinHandle<()> {
    let client = client.clone();
    let topic = topic.to_string();
    tokio::spawn(async move {
        for r in 0..rounds {
            for i in 0..n_keys {
                // Reuse keys across rounds so the conflator does some
                // coalescing; but rounds × keys is still enough deltas
                // to overwhelm a non-reader.
                let _ = client
                    .publish(
                        &topic,
                        json!({
                            "k": format!("k{i:05}"),
                            "v": (r * n_keys + i) as f64
                        }),
                    )
                    .await;
            }
        }
    })
}

// ─── 1. Drops are observable via /subscriptions + metrics ─────────

#[tokio::test]
async fn slow_consumer_drops_are_observable() {
    let schema = json!({ "k": "string", "v": "double" });
    // Small queue + conflation so drops accrue fast.
    let topic = TopicSpec::new("/slow", "k").with_schema(schema).with_conflation(20);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 256,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
            admin_tls: None,
            transport_limits: None,
        },
    )
    .await;

    // Producer client.
    let producer = Client::connect(&server.tcp_url()).await.expect("producer");

    // The slow consumer: subscribe, then never read. Holding the
    // returned TcpStream alive keeps the session open on the server.
    let _silent = open_silent_subscriber(server.tcp_port, "/slow").await;
    // Wait long enough for the subscribe to register.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Flood deltas. With 5k keys * 4 rounds = 20k publishes, each
    // hitting the conflator's flush task → many drops on the silent
    // consumer's per-sub queue once it overflows.
    let flood = flood_topic_with_publishes(&producer, "/slow", 5_000, 4);
    flood.await.expect("flood done");

    // Give the conflator's flush a moment.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let subs = subscriptions(&server).await;
    assert!(!subs.is_empty(), "expected at least one subscription");
    let slow = subs
        .iter()
        .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/slow"))
        .expect("missing /slow subscription");
    let dropped = slow.get("dropped").and_then(|v| v.as_u64()).unwrap_or(0);
    let fill = slow.get("fillRatio").and_then(|v| v.as_f64()).unwrap_or(0.0);
    assert!(dropped > 0, "expected dropped > 0 for slow consumer, got {dropped}");
    assert!(
        fill > 0.5,
        "expected fillRatio > 0.5 for backed-up sub, got {fill}"
    );

    // Prometheus counter should reflect drops too.
    let m = metrics_lines(&server).await;
    let has_metric = m.lines().any(|l| {
        l.starts_with("cq_subscription_dropped_total") && l.contains("topic=\"/slow\"")
    });
    assert!(
        has_metric,
        "expected cq_subscription_dropped_total{{topic=\"/slow\"}} in /metrics"
    );
}

// ─── 2. Admin DELETE /subscriptions/{sid} unsubscribes ────────────

#[tokio::test]
async fn admin_delete_subscription_disconnects_slow_consumer() {
    let schema = json!({ "k": "string", "v": "double" });
    let topic = TopicSpec::new("/admin-cut", "k").with_schema(schema).with_conflation(20);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 512,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: None,
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
            admin_tls: None,
            transport_limits: None,
        },
    )
    .await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let _silent = open_silent_subscriber(server.tcp_port, "/admin-cut").await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Enough deltas to register the sub + accrue some queue depth.
    let flood = flood_topic_with_publishes(&producer, "/admin-cut", 2_000, 2);
    flood.await.expect("flood");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Pull the sub id.
    let subs = subscriptions(&server).await;
    let sid = subs
        .iter()
        .find(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/admin-cut"))
        .and_then(|s| s.get("subId").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .expect("sub id");

    // DELETE it. Sub-ids contain `:` (e.g. `sess-1:sub-2`), which is
    // a reserved char in URI authority; percent-encode it for the path.
    let encoded = sid.replace(':', "%3A");
    let url = format!("{}/subscriptions/{}", server.admin_url(), encoded);
    let resp = reqwest::Client::new()
        .delete(&url)
        .send()
        .await
        .expect("delete request");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "expected 200 for DELETE {url}"
    );

    // Sub should no longer appear in /subscriptions.
    let after = subscriptions(&server).await;
    assert!(
        after
            .iter()
            .all(|s| s.get("subId").and_then(|v| v.as_str()) != Some(sid.as_str())),
        "sub still listed after DELETE"
    );

    // Topic's sub count should also drop.
    let stats = topic_stats(&server, "/admin-cut").await.expect("topic stats");
    let sub_count = stats.get("subscriptions").and_then(|v| v.as_u64()).unwrap_or(0);
    assert_eq!(sub_count, 0, "topic should have no subs after admin DELETE");
}

// ─── 3. Auto-disconnect kicks in once the watcher sees drops ──────

#[tokio::test]
async fn auto_disconnect_kicks_in_when_drops_exceed_threshold() {
    let schema = json!({ "k": "string", "v": "double" });
    let topic = TopicSpec::new("/auto-cut", "k").with_schema(schema).with_conflation(20);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 256,
            slow_consumer: Some(SlowConsumerOpts {
                scan_interval_s: 1,
                // Lowered from 50 so we trigger reliably even when the
                // test binary is competing for CPU under the full
                // workspace test run.
                drops_per_sec_threshold: 10,
                auto_disconnect: true,
                ..SlowConsumerOpts::default()
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
            admin_token: None,
            admin_tls: None,
            transport_limits: None,
        },
    )
    .await;

    let producer = Client::connect(&server.tcp_url()).await.expect("producer");
    let _silent = open_silent_subscriber(server.tcp_port, "/auto-cut").await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Keep a flood spinning in the background so drops continue to
    // accrue between watcher scans. The task is detached — we only
    // need traffic to keep happening while we poll for disconnect.
    let producer_for_flood = producer.clone();
    let _flood = tokio::spawn(async move {
        for r in 0..20 {
            for i in 0..4_000 {
                let _ = producer_for_flood
                    .publish(
                        "/auto-cut",
                        json!({ "k": format!("k{i:05}"), "v": (r * 4_000 + i) as f64 }),
                    )
                    .await;
            }
        }
    });

    // Watcher scans every 1s. Give it 12s — plenty under any
    // reasonable workspace-concurrency load.
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    let mut got_disconnect = false;
    while std::time::Instant::now() < deadline {
        let subs = subscriptions(&server).await;
        let still_there = subs
            .iter()
            .any(|s| s.get("topic").and_then(|v| v.as_str()) == Some("/auto-cut"));
        if !still_there {
            got_disconnect = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        got_disconnect,
        "watcher did not auto-disconnect the slow consumer within 12s"
    );

    // Auto-disconnect counter should be present in metrics.
    let m = metrics_lines(&server).await;
    let has_metric = m.lines().any(|l| {
        l.starts_with("cq_slow_consumer_auto_disconnect_total")
            && l.contains("topic=\"/auto-cut\"")
    });
    assert!(
        has_metric,
        "expected cq_slow_consumer_auto_disconnect_total in /metrics"
    );
}

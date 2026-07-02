//! e2e: streaming + subscription paths.
//!
//! Covers the fixes from this session not already exercised by
//! `schema_and_nested.rs`:
//!   - Snapshot beyond the outbound-queue capacity completes via
//!     backpressure (no drops).
//!   - Many concurrent `sow_and_subscribe` calls all get their ack +
//!     full snapshot — regression for the "ack dropped on full queue"
//!     bug where in-flight snapshots filled the queue and subsequent
//!     acks went silently overboard.
//!   - A `sow_and_subscribe` Subscription continues receiving live
//!     Add/Update deltas after the snapshot phase ends.
//!   - `delta_subscribe` (no-SOW) only delivers live deltas.
//!   - Mixed numeric types round-trip cleanly.
//!   - Inline `columns = [...]` config form works equivalently to an
//!     external schema_file.
//!   - Persistent topic recovers rows from the txlog on restart.

use cq_client::{Client, DeltaKind, Subscription};
use cq_e2e_tests::{
    restart_kept, start_server, start_server_with, stop_keeping_dir, topic_stats,
    ServerOpts, TopicSpec,
};
use serde_json::{json, Value};
use std::time::Duration;

// ─── Helpers ──────────────────────────────────────────────────────

/// Drain `SowSnapshot` deltas from `sub` until `quiet` elapses without
/// one. Live (Add/Update) deltas during the snapshot phase are pushed
/// onto `live_overflow` so the caller can inspect both streams.
async fn drain_snapshot_with_overflow(
    sub: &mut Subscription,
    quiet: Duration,
    live_overflow: &mut Vec<cq_client::Delta>,
) -> usize {
    let mut snap = 0usize;
    loop {
        match tokio::time::timeout(quiet, sub.next_delta()).await {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => snap += 1,
            Ok(Some(d)) => {
                // Non-snapshot delta — push back via overflow so caller
                // sees it, and return: snapshot phase is over.
                live_overflow.push(d);
                return snap;
            }
            Ok(None) | Err(_) => return snap,
        }
    }
}

async fn count_snapshot(sub: &mut Subscription, quiet: Duration) -> usize {
    let mut overflow = Vec::new();
    drain_snapshot_with_overflow(sub, quiet, &mut overflow).await
}

// ─── 1. Snapshot beyond queue capacity ────────────────────────────

#[tokio::test]
async fn snapshot_beyond_queue_capacity_completes() {
    let schema = json!({
        "k": "string",
        "v": "double"
    });
    let topic = TopicSpec::new("/big", "k").with_schema(schema);
    // Set the outbound queue *smaller* than the snapshot. Before the
    // backpressure fix this would silently drop rows. Now the SOW
    // task awaits on send and the snapshot completes intact.
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 2_048,
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
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let n = 8_000;
    for i in 0..n {
        client
            .publish("/big", json!({ "k": format!("k{i:05}"), "v": i as f64 }))
            .await
            .expect("publish");
    }

    // Sanity: sow() should return them all (uses handle_sow path).
    let rows = client.sow("/big", None).await.expect("sow");
    assert_eq!(rows.len(), n, "sow() returned {}/{n}", rows.len());

    // The subscribe path also delivers the full snapshot via the
    // streaming Subscription channel.
    let mut sub = client
        .sow_and_subscribe("/big", None, None)
        .await
        .expect("sub");
    let snap = count_snapshot(&mut sub, Duration::from_millis(800)).await;
    assert_eq!(snap, n, "sow_and_subscribe got {snap}/{n}");
}

// ─── 2. Many concurrent subscribes all get their snapshots ────────

#[tokio::test]
async fn many_concurrent_subscribes_no_ack_drops() {
    let schema = json!({
        "k": "string",
        "v": "double"
    });
    let topic = TopicSpec::new("/concur", "k").with_schema(schema);
    // Modest queue so concurrent snapshots actually contend for it.
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 4_096,
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
        },
    )
    .await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let n_rows = 5_000;
    for i in 0..n_rows {
        client
            .publish("/concur", json!({ "k": format!("k{i:05}"), "v": i as f64 }))
            .await
            .expect("publish");
    }

    // Fire 8 sow_and_subscribe calls roughly in parallel.
    let mut tasks = Vec::new();
    for tid in 0..8 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut sub = c
                .sow_and_subscribe("/concur", None, None)
                .await
                .expect("sub");
            let snap = count_snapshot(&mut sub, Duration::from_millis(1_000)).await;
            (tid, snap)
        }));
    }
    for t in tasks {
        let (tid, snap) = t.await.expect("join");
        assert_eq!(
            snap, n_rows,
            "subscribe #{tid} only got {snap}/{n_rows} — ack or snapshot dropped"
        );
    }
}

// ─── 3. Subscription receives live deltas after snapshot ──────────

#[tokio::test]
async fn subscription_receives_live_deltas_after_snapshot() {
    let schema = json!({
        "k": "string",
        "v": "double"
    });
    let topic = TopicSpec::new("/stream", "k").with_schema(schema);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed.
    for i in 0..50 {
        client
            .publish("/stream", json!({ "k": format!("seed{i}"), "v": i as f64 }))
            .await
            .expect("publish");
    }

    let mut sub = client
        .sow_and_subscribe("/stream", None, None)
        .await
        .expect("sub");

    // Drain the snapshot first.
    let mut overflow = Vec::new();
    let snap = drain_snapshot_with_overflow(
        &mut sub,
        Duration::from_millis(500),
        &mut overflow,
    )
    .await;
    assert_eq!(snap, 50, "snapshot incomplete: {snap}");

    // Now publish new rows; they should arrive as live deltas on the
    // same Subscription.
    for i in 0..10 {
        client
            .publish("/stream", json!({ "k": format!("live{i}"), "v": 1000.0 + i as f64 }))
            .await
            .expect("publish");
    }

    let mut live_seen = overflow.len();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while live_seen < 10 && std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
            Ok(Some(d)) => {
                assert!(
                    matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update),
                    "expected Add/Update post-snapshot, got {:?}",
                    d.delta_type
                );
                live_seen += 1;
            }
            _ => break,
        }
    }
    assert_eq!(live_seen, 10, "expected 10 live deltas, saw {live_seen}");
}

// ─── 4. Subscribe (no-SOW) skips snapshot phase ───────────────────
//
// The Rust client's `subscribe()` sends the `Subscribe` command (vs
// `sow_and_subscribe`). The server's handle_subscribe sends an ack
// and registers the route — no group_begin/sow/group_end. Live
// publishes then arrive as deltas. This is the path used by the
// TS `client.subscribe(topic, cb, { deltasOnly: true })` API.

#[tokio::test]
async fn subscribe_no_sow_skips_snapshot() {
    let schema = json!({
        "k": "string",
        "v": "double"
    });
    let topic = TopicSpec::new("/deltas", "k").with_schema(schema);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Pre-seed — these rows should NOT arrive on the no-SOW sub.
    for i in 0..100 {
        client
            .publish("/deltas", json!({ "k": format!("pre{i}"), "v": i as f64 }))
            .await
            .expect("publish");
    }

    let mut sub = client.subscribe("/deltas", None).await.expect("sub");

    // Confirm the snapshot phase produced no deltas.
    let snap = count_snapshot(&mut sub, Duration::from_millis(400)).await;
    assert_eq!(snap, 0, "subscribe leaked {snap} snapshot rows");

    // Publish a fresh row and verify it arrives as a live delta.
    client
        .publish("/deltas", json!({ "k": "post-1", "v": 12345.0 }))
        .await
        .expect("publish post");

    // Capture up to 5 deltas (or 800ms quiet) and print what we saw,
    // then assert post-1 is in there.
    let mut seen: Vec<(DeltaKind, String)> = Vec::new();
    for _ in 0..5 {
        match tokio::time::timeout(Duration::from_millis(300), sub.next_delta()).await {
            Ok(Some(d)) => {
                let k = d.data.get("k").and_then(|v| v.as_str()).unwrap_or("").to_string();
                seen.push((d.delta_type, k));
            }
            _ => break,
        }
    }
    let post_seen = seen.iter().any(|(k, v)| {
        matches!(k, DeltaKind::Add | DeltaKind::Update) && v == "post-1"
    });
    assert!(
        post_seen,
        "expected an Add/Update for post-1, saw {seen:?}"
    );
    // None of the deltas should be SowSnapshot type — that's the
    // critical contract of `subscribe` (no-SOW).
    let any_snapshot = seen.iter().any(|(k, _)| matches!(k, DeltaKind::SowSnapshot));
    assert!(
        !any_snapshot,
        "subscribe must NOT deliver SowSnapshot deltas, saw {seen:?}"
    );
}

// ─── 5. Mixed-type roundtrip ──────────────────────────────────────

#[tokio::test]
async fn mixed_type_roundtrip() {
    let schema = json!({
        "k": "string",
        "ratio": "double",
        "count": "long",
        "level": "int",
        "label": "string"
    });
    let topic = TopicSpec::new("/typed", "k").with_schema(schema);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish(
            "/typed",
            json!({
                "k": "row1",
                "ratio": 1.5,
                "count": 9_876_543_210i64,
                "level": -7,
                "label": "hello"
            }),
        )
        .await
        .expect("publish");

    let rows = client.sow("/typed", None).await.expect("sow");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.get("k").and_then(|v| v.as_str()), Some("row1"));
    assert_eq!(r.get("ratio").and_then(|v| v.as_f64()), Some(1.5));
    assert_eq!(r.get("count").and_then(|v| v.as_i64()), Some(9_876_543_210));
    assert_eq!(r.get("level").and_then(|v| v.as_i64()), Some(-7));
    assert_eq!(r.get("label").and_then(|v| v.as_str()), Some("hello"));
}

// ─── 6. Inline columns config form ────────────────────────────────

#[tokio::test]
async fn inline_columns_config_works_like_schema_file() {
    let topic = TopicSpec::new("/inline", "k").with_inline_columns(vec![
        ("k", "string"),
        ("nested.a", "double"),
        ("nested.b", "long"),
        ("flat", "string"),
    ]);
    let server = start_server(vec![topic]).await;

    let stats = topic_stats(&server, "/inline").await.expect("stats");
    assert_eq!(
        stats.get("schemaDiscovered").and_then(|v| v.as_bool()),
        Some(true),
        "inline-columns topic should boot with schemaDiscovered=true"
    );
    assert_eq!(
        stats.get("columnCount").and_then(|v| v.as_u64()),
        Some(4),
        "should have 4 columns"
    );

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish(
            "/inline",
            json!({
                "k": "row1",
                "nested": { "a": 3.14, "b": 42 },
                "flat": "hi"
            }),
        )
        .await
        .expect("publish");

    // Filter on a dotted path declared via inline columns.
    let rows = client
        .sow("/inline", Some("nested.a > 0"))
        .await
        .expect("filtered sow");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("nested.a").and_then(|v| v.as_f64()),
        Some(3.14)
    );
}

// ─── 7. Persistent topic recovers after restart ───────────────────

#[tokio::test]
async fn persisted_topic_recovers_after_server_restart() {
    let schema = json!({
        "tradeId": "string",
        "qty": "long",
        "price": "double"
    });
    let topic = TopicSpec::new("/trades", "tradeId")
        .with_schema(schema.clone())
        .with_persist();
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..200 {
        client
            .publish(
                "/trades",
                json!({
                    "tradeId": format!("T{i:08}"),
                    "qty": (i + 1) * 100,
                    "price": 100.0 + i as f64 * 0.01
                }),
            )
            .await
            .expect("publish");
    }
    // Drop client to release the TCP connection cleanly before stopping
    // the server, otherwise the recovery test below sometimes races
    // OS-level socket teardown.
    drop(client);

    // Stop + restart on the same on-disk state.
    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;

    // Verify rows recovered.
    let stats = topic_stats(&server2, "/trades").await.expect("stats");
    assert_eq!(
        stats.get("rowCount").and_then(|v| v.as_u64()),
        Some(200),
        "expected 200 recovered rows"
    );

    let client = Client::connect(&server2.tcp_url()).await.expect("reconnect");
    let rows = client.sow("/trades", None).await.expect("sow");
    assert_eq!(rows.len(), 200);
    // Spot-check a row.
    let r = rows
        .iter()
        .find(|r| r.get("tradeId").and_then(|v| v.as_str()) == Some("T00000042"))
        .expect("missing T00000042 after recovery");
    assert_eq!(r.get("qty").and_then(|v| v.as_i64()), Some(43 * 100));
    let _: Value = json!(r); // touch
}

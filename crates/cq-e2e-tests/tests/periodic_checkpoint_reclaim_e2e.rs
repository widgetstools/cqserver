//! Task 3.1 — periodic in-process checkpoint + segment reclaim.
//!
//! Proves the two load-bearing properties of the runtime checkpointer
//! (the AMPS `sow-compact-action` equivalent):
//!
//!   1. BOUND: with `checkpoint_interval_secs = 1` + a tiny
//!      `segment_size` (forcing rapid rotation), continuous publishing
//!      produces many segments, but the on-disk txlog does NOT grow
//!      unbounded — the periodic checkpoint reclaims sealed segments so
//!      the segment count sawtooths / stays bounded — WHILE the server
//!      keeps serving. `cq_txlog_segments_reclaimed_total > 0`.
//!
//!   2. CRASH-SAFETY (the load-bearing one): after a periodic checkpoint
//!      has reclaimed segments, SIGKILL the server (not graceful
//!      SIGTERM), restart on the same dir, and assert EVERY published+
//!      acked row is recovered. If a reclaimed segment were actually
//!      needed, rows would be lost here — that would be a real safety
//!      bug in the reclaim, not a test artifact.

use cq_client::Client;
use cq_e2e_tests::{restart_kept, start_server_with, stop_keeping_dir, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::{Duration, Instant};

fn persistent_topic(name: &str) -> TopicSpec {
    TopicSpec::new(name, "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist()
}

/// Small segments + 1s checkpoint interval + reclaim enabled.
fn checkpoint_opts() -> ServerOpts {
    ServerOpts {
        // ~4 KiB segments: a handful of publishes seals a segment, so a
        // few thousand publishes over the run rotate through many.
        txlog_segment_size: Some(4096),
        txlog_snapshot_reclaim: true,
        txlog_checkpoint_interval_secs: 1,
        ..ServerOpts::default()
    }
}

/// Count `*.log` segment files under the server's txlog dir tree.
fn count_segments(root: &std::path::Path) -> usize {
    let mut n = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().map(|e| e == "log").unwrap_or(false) {
                n += 1;
            }
        }
    }
    n
}

async fn reclaimed_total(admin_url: &str) -> u64 {
    let body = reqwest::get(&format!("{admin_url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    parse_counter(&body, "cq_txlog_segments_reclaimed_total")
}

async fn checkpoint_total(admin_url: &str) -> u64 {
    let body = reqwest::get(&format!("{admin_url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    parse_counter(&body, "cq_txlog_checkpoint_total")
}

/// Sum every sample of a Prometheus counter by metric name (ignores
/// labels — the checkpoint counters are unlabeled today, but be robust).
fn parse_counter(body: &str, name: &str) -> u64 {
    let mut total = 0u64;
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        // `name value` or `name{labels} value`
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        if !(rest.starts_with(' ') || rest.starts_with('{')) {
            continue; // avoid matching a longer metric with same prefix
        }
        if let Some(v) = line.rsplit(' ').next() {
            if let Ok(f) = v.parse::<f64>() {
                total += f as u64;
            }
        }
    }
    total
}

#[tokio::test]
async fn periodic_checkpoint_bounds_txlog_while_serving() {
    let server = start_server_with(vec![persistent_topic("/ckpt-bound")], checkpoint_opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    let txlog_root = server.config_dir.parent().unwrap().join("txlog");
    let admin_url = server.admin_url();

    // Continuously publish for ~14s, sampling the on-disk segment count.
    // Keys cycle over a bounded set so the SOW stays small (fast
    // snapshot builds) while the *log* keeps rotating.
    let deadline = Instant::now() + Duration::from_secs(14);
    let mut seq = 0i64;
    let mut peak_segments = 0usize;
    let mut samples: Vec<usize> = Vec::new();
    while Instant::now() < deadline {
        for _ in 0..200 {
            let k = seq % 500; // 500 distinct keys, SOW bounded
            client
                .publish("/ckpt-bound", json!({ "k": format!("k{k:04}"), "v": seq }))
                .await
                .unwrap();
            seq += 1;
        }
        let segs = count_segments(&txlog_root);
        peak_segments = peak_segments.max(segs);
        samples.push(segs);
        tokio::time::sleep(Duration::from_millis(400)).await;
    }

    // Server is still serving: a SOW returns the (bounded) live state.
    let rows = client.sow("/ckpt-bound", None).await.unwrap();
    assert_eq!(rows.len(), 500, "all 500 distinct keys live");

    // Reclaim actually happened.
    let reclaimed = reclaimed_total(&admin_url).await;
    let checkpoints = checkpoint_total(&admin_url).await;
    assert!(
        checkpoints > 0,
        "periodic checkpoint must have fired (got {checkpoints})"
    );
    assert!(
        reclaimed > 0,
        "periodic reclaim must have deleted sealed segments (got {reclaimed})"
    );

    // The BOUND: the final segment count is far below what unbounded
    // growth would produce. Without reclaim, publishing thousands of
    // records into 4 KiB segments would leave dozens-to-hundreds of
    // segments; with reclaim the count stays small (roughly 1 active +
    // whatever rotated since the last checkpoint). We assert a generous
    // bound to keep the test robust while still proving boundedness.
    let final_segments = count_segments(&txlog_root);
    assert!(
        final_segments <= 20,
        "txlog must stay bounded, got {final_segments} segments (peak {peak_segments}, published {seq})"
    );
    // And we published far more than that bound's worth of segments.
    assert!(
        seq >= 2000,
        "sanity: published enough to rotate many segments (got {seq})"
    );

    drop(client);
    // graceful stop for this one (bound test doesn't need a crash).
    let mut server = server;
    let _ = server.send_sigterm_and_wait(Duration::from_secs(10));
}

#[tokio::test]
async fn periodic_checkpoint_reclaim_is_crash_safe() {
    let server = start_server_with(vec![persistent_topic("/ckpt-crash")], checkpoint_opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    let admin_url = server.admin_url();

    // Publish a KNOWN, fully-distinct set so recovery is exactly
    // checkable — every key must survive. Distinct keys also force the
    // log to keep growing (SOW == full history) so many segments seal
    // and get reclaimed.
    let total_keys = 3000i64;
    for i in 0..total_keys {
        client
            .publish("/ckpt-crash", json!({ "k": format!("k{i:05}"), "v": i }))
            .await
            .unwrap();
    }

    // Wait until at least one periodic checkpoint has reclaimed
    // segments, so the crash happens AFTER a reclaim (the scenario under
    // test). Interval is 1s; give it up to ~8s.
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut reclaimed = 0u64;
    while Instant::now() < deadline {
        reclaimed = reclaimed_total(&admin_url).await;
        if reclaimed > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reclaimed > 0,
        "a periodic checkpoint must have reclaimed segments before the crash"
    );

    // A few post-checkpoint publishes land in the surviving tail — these
    // exercise "snapshot + tail replay", the part that must reproduce
    // rows a reclaimed segment can no longer supply.
    for i in total_keys..(total_keys + 200) {
        client
            .publish("/ckpt-crash", json!({ "k": format!("k{i:05}"), "v": i }))
            .await
            .unwrap();
    }
    let published = total_keys + 200;
    // Let the tail fsync land, then also let one more checkpoint run so
    // the crash can land mid/after another reclaim cycle.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    drop(client);

    // CRASH: SIGKILL (stop_keeping_dir uses child.kill()), keep the dir.
    let kept = stop_keeping_dir(server).await;

    // Restart on the SAME on-disk state: snapshot.bin + surviving tail
    // replay must reproduce EVERY acked row. If the reclaim deleted a
    // segment whose rows the snapshot did not cover, they are gone now.
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/ckpt-crash", None).await.unwrap();
    assert_eq!(
        rows.len() as i64,
        published,
        "ALL {published} acked rows must survive the crash after a reclaim; got {} — \
         a lost row means a reclaimed segment was needed (safety bug)",
        rows.len()
    );

    // Spot-check a value that lived in an early (reclaimed) segment and a
    // late (tail) one, to prove the snapshot actually carries early data.
    let early = rows.iter().find(|r| r.get("k").and_then(|v| v.as_str()) == Some("k00007"));
    let late = rows
        .iter()
        .find(|r| r.get("k").and_then(|v| v.as_str()) == Some(&format!("k{:05}", published - 1)));
    assert!(early.is_some(), "early (reclaimed-segment) row recovered from snapshot");
    assert!(late.is_some(), "late (tail) row recovered from replay");
}

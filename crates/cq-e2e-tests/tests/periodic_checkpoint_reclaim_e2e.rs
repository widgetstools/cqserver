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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex as AsyncMutex;

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

/// Tiny segments so each publish nearly rotates the log: this widens the
/// straddle window (a checkpoint that samples segment `A` will, during its
/// snapshot fsync, almost certainly see the active publisher rotate `A →
/// A+1`), which is what makes the crash test reliably catch a `u64::MAX`
/// reclaim cutoff deleting the straddling segment.
fn crash_checkpoint_opts() -> ServerOpts {
    ServerOpts {
        // ~256 B segments: one or two publishes rotate a segment.
        txlog_segment_size: Some(256),
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

/// The load-bearing crash-safety test, hardened to catch the
/// straddling-segment data-loss race in the periodic reclaim.
///
/// The earlier version quiesced (a 1300ms sleep) before SIGKILL, letting a
/// final checkpoint re-snapshot everything and MASK the bug. This version
/// keeps a publisher ACTIVELY publishing (distinct keys → constant rotation)
/// and SIGKILLs in the window immediately after a reclaim that ran *during*
/// active publishing — i.e. while a segment straddling the checkpoint's
/// `snapshot.sequence` exists on disk. Then it restarts on the same dir and
/// asserts EVERY key the server acked is present.
///
/// Against the old `u64::MAX` reclaim cutoff the checkpoint prunes the
/// straddling segment, so the sequences appended into its tail after the
/// snapshot are lost → this test FAILS. Against the `snapshot.segment_id`
/// cutoff the straddling segment is retained → all acked rows recover.
#[tokio::test]
async fn periodic_checkpoint_reclaim_is_crash_safe() {
    let server =
        start_server_with(vec![persistent_topic("/ckpt-crash")], crash_checkpoint_opts()).await;
    let admin_url = server.admin_url();
    let tcp_url = server.tcp_url();

    // Shared record of every key the server ACKED, keyed by the ack
    // sequence the publish returned. Only acked keys are asserted on
    // recovery — an ack is the server's durability promise.
    let acked: Arc<AsyncMutex<Vec<(u64, String)>>> = Arc::new(AsyncMutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));

    // Several concurrent publishers of fully-distinct keys: SOW == full
    // history, so the log keeps rotating (segments keep sealing) right up to
    // the crash, and there is ALWAYS in-flight publishing during a
    // checkpoint's snapshot fsync — guaranteeing the sampled segment
    // straddles the snapshot's sequence.
    let mut publishers = Vec::new();
    for shard in 0..4u32 {
        let pub_acked = acked.clone();
        let pub_stop = stop.clone();
        let pub_url = tcp_url.clone();
        publishers.push(tokio::spawn(async move {
            let client = Client::connect(&pub_url).await.unwrap();
            let mut i: u64 = 0;
            while !pub_stop.load(Ordering::Relaxed) {
                // Disjoint keyspaces per shard so keys stay globally unique.
                let key = format!("s{shard}-k{i:07}");
                match client
                    .publish("/ckpt-crash", json!({ "k": key, "v": i }))
                    .await
                {
                    Ok(seq) => pub_acked.lock().await.push((seq, key)),
                    // A publish that errors (socket died at kill time) was
                    // NOT acked — do not record it. Stop this shard.
                    Err(_) => break,
                }
                i += 1;
            }
        }));
    }

    // Wait until SEVERAL periodic checkpoints have reclaimed segments WHILE
    // the publishers are still running — each such reclaim is a window in
    // which a straddling segment could be (wrongly) deleted. Requiring a few
    // cycles makes the race reliably reproduce. Interval is 1s; up to ~12s.
    let deadline = Instant::now() + Duration::from_secs(12);
    let mut reclaimed = 0u64;
    while Instant::now() < deadline {
        reclaimed = reclaimed_total(&admin_url).await;
        if reclaimed >= 5 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        reclaimed >= 5,
        "several periodic checkpoints must have reclaimed segments while \
         publishing (got {reclaimed})"
    );
    let checkpoints = checkpoint_total(&admin_url).await;
    assert!(checkpoints > 0, "a checkpoint fired (got {checkpoints})");

    // CRASH NOW, mid-flight: stop the publishers, snapshot the acked set,
    // and SIGKILL immediately with NO grace period — so no quiescent
    // checkpoint can re-snapshot a straddling tail and mask a lost segment.
    // Keep the post-stop work minimal (no network round-trips) so the kill
    // lands well inside the 1s checkpoint interval.
    stop.store(true, Ordering::Relaxed);
    for p in publishers {
        let _ = p.await;
    }
    let expected = acked.lock().await.clone();
    assert!(
        expected.len() >= 500,
        "publishers acked a meaningful number of rows before the crash (got {})",
        expected.len()
    );

    // SIGKILL (stop_keeping_dir uses child.kill()), keep the dir.
    let kept = stop_keeping_dir(server).await;

    // Restart on the SAME on-disk state: snapshot.bin + surviving tail
    // replay must reproduce EVERY acked row. If the reclaim deleted a
    // segment whose rows the snapshot did not cover, they are gone now.
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/ckpt-crash", None).await.unwrap();

    use std::collections::HashSet;
    let recovered: HashSet<String> = rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    // Every ACKED key must be present. Report the exact missing sequences
    // so a regression names the lost straddling-segment rows.
    let missing: Vec<&(u64, String)> =
        expected.iter().filter(|(_, k)| !recovered.contains(k)).collect();
    assert!(
        missing.is_empty(),
        "{} of {} acked rows LOST after crash following a reclaim during active \
         publishing — a lost row means the straddling segment was reclaimed \
         (data-loss bug). First few missing (seq,key): {:?}",
        missing.len(),
        expected.len(),
        &missing[..missing.len().min(8)]
    );

    // Sanity: the recovered set covers the full acked span (early keys that
    // lived in reclaimed segments through the latest acked tail key).
    let (min_seq, min_key) = expected.iter().min_by_key(|(s, _)| *s).unwrap();
    let (max_seq, max_key) = expected.iter().max_by_key(|(s, _)| *s).unwrap();
    assert!(
        recovered.contains(min_key),
        "earliest acked key {min_key} (seq {min_seq}, from a reclaimed segment) recovered"
    );
    assert!(
        recovered.contains(max_key),
        "latest acked key {max_key} (seq {max_seq}, from the surviving tail) recovered"
    );
}

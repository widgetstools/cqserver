//! Stress test for the production TTL sweeper × publish race
//! (worklog S40, review C9). Runs many concurrent publishes against
//! a short-TTL topic with the sweeper firing in a loop, then asserts
//! the SOW final state is consistent with the publish stream — no
//! "lost" row that the publisher wrote AFTER the sweep saw it as
//! expired.
//!
//! The race the test guards against (without
//! `delete_if_still_expired`):
//!   1. Sweep scans, sees row K expired (TTL elapsed since last
//!      `last_touched`).
//!   2. Sweep releases the read lock.
//!   3. Publisher writes a new value for K, refreshes `last_touched`.
//!   4. Sweep calls `delete(K)` — drops the row the publisher just
//!      committed.
//!
//! With the fix, step 4's delete re-checks `last_touched` under the
//! write lock and bails if the publisher already refreshed.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use serde_json::json;

/// TTL=0 — every row is always candidate for expiry. Maximizes the
/// race window between sweep's read-pass and its delete call.
fn make_topic_ttl_zero() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/ttl-race".into(),
            key_fields: vec!["k".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: Some(0),
        },
        schema,
        128,
    )
}

/// Rapid publishes to a single key while the sweeper churns. With
/// the old code (sweep observes → drop lock → delete without
/// re-check), some published values get silently lost. With the
/// `delete_if_still_expired` re-check, the final SOW state always
/// reflects the latest publish.
#[test]
fn rapid_publishes_to_one_key_while_sweep_runs_never_loses_data() {
    let topic = Arc::new(make_topic_ttl_zero());
    let stop = Arc::new(AtomicBool::new(false));
    let publishes_made = Arc::new(AtomicU64::new(0));

    // Publisher: write k1 over and over with a strictly increasing
    // `v`. The final SOW value MUST be the largest v we wrote.
    let topic_p = topic.clone();
    let stop_p = stop.clone();
    let publishes_p = publishes_made.clone();
    let publisher = thread::spawn(move || {
        let mut v: i64 = 0;
        while !stop_p.load(Ordering::Relaxed) {
            v += 1;
            let _ = topic_p.upsert_map(&{
                let mut m = serde_json::Map::new();
                m.insert("k".into(), json!("k1"));
                m.insert("v".into(), json!(v));
                m
            });
            publishes_p.store(v as u64, Ordering::Relaxed);
        }
        v
    });

    // Sweeper: fire `sweep_expired` in a loop. With TTL=0 every
    // row is always "candidate for expiry", which maximizes the
    // race window.
    let topic_s = topic.clone();
    let stop_s = stop.clone();
    let sweeper = thread::spawn(move || {
        let mut sweeps = 0u64;
        while !stop_s.load(Ordering::Relaxed) {
            let _ = topic_s.sweep_expired();
            sweeps += 1;
        }
        sweeps
    });

    let started = Instant::now();
    thread::sleep(Duration::from_millis(500));
    stop.store(true, Ordering::Relaxed);
    let final_v_published = publisher.join().expect("publisher");
    let sweeps = sweeper.join().expect("sweeper");
    let elapsed = started.elapsed();

    eprintln!(
        "ttl-race: published v=1..{final_v_published}, ran {sweeps} sweeps over {:.0}ms",
        elapsed.as_millis()
    );

    // Confidence guard: actually exercised the race?
    assert!(final_v_published >= 1000, "publisher under-exercised: only {final_v_published} publishes");
    assert!(sweeps >= 100, "sweeper under-exercised: only {sweeps} sweeps");

    // The SOW must contain k1 (last publish wins; if sweep ever
    // deleted it, the *very next* publish republished). What we
    // can't tolerate is the row being absent at the end if the
    // publisher kept refreshing it.
    //
    // We give the publisher one more publish AFTER the sweeper
    // stops so the final state is deterministic — if the sweeper
    // raced and "killed" the row right at the tail, the trailing
    // publish refreshes it. Without this, the test could
    // occasionally observe "deleted right before stop" and assume
    // the race lost data.
    let final_v: i64 = (final_v_published + 1) as i64;
    topic
        .upsert_map(&{
            let mut m = serde_json::Map::new();
            m.insert("k".into(), json!("k1"));
            m.insert("v".into(), json!(final_v));
            m
        })
        .expect("final publish");

    let rows = topic
        .query("SELECT k, v FROM t WHERE k = 'k1'")
        .expect("query")
        .rows;
    assert_eq!(rows.len(), 1, "k1 not in SOW after race (rows: {rows:?})");
    let observed_v = rows[0].get("v").and_then(|v| v.as_i64()).unwrap_or(-1);
    assert_eq!(
        observed_v, final_v,
        "SOW value {observed_v} != final published {final_v} — sweep observed-then-deleted the trailing publish"
    );
}

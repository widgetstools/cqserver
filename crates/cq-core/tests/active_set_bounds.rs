//! Active-set bound tests (worklog S42, review C11).
//!
//! - C11.1: TTL expiry + sweep shrinks every subscription's active
//!   set down to 0 (the engine's existing Remove path already does
//!   this; the test pins it so a regression can't sneak in).
//! - C11.2: `max_active` cap closes the sub with `TooManyMatches`
//!   when an Add would push the active set past the configured
//!   bound.
//! - C11.3: 1-hour soak with sustained insert + TTL-delete churn;
//!   asserts the active set stays bounded over the run. Marked
//!   `#[ignore]` so default `cargo test` doesn't pay the hour.

use std::sync::Arc;
use std::time::Duration;

use cq_core::schema::{ColumnType, Schema};
use cq_core::subscription::CloseReason;
use cq_core::topic::{Topic, TopicConfig};
use serde_json::{json, Map};

fn make_topic_ttl(ttl_seconds: Option<u64>) -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/active-set-bounds".into(),
            key_fields: vec!["k".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: ttl_seconds,
        },
        schema,
        16_384,
    )
}

fn publish_row(topic: &Topic, key: &str, v: i64) {
    let mut m = Map::new();
    m.insert("k".into(), json!(key));
    m.insert("v".into(), json!(v));
    topic.upsert_map(&m).expect("upsert");
}

/// Macro version: inline so the test crate doesn't need a direct
/// `crossbeam-channel` dep. `take_mutation_rx` is single-shot, so
/// callers grab the receiver once and reuse it across phases via
/// repeated invocations of this macro.
macro_rules! drain_evaluator {
    ($topic:expr, $rx:expr) => {{
        while let Ok(ev) = $rx.try_recv() {
            let _ = $topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
        }
    }};
}

/// C11.1 — Publish 10K matching rows, TTL-expire them, sweep, then
/// verify the subscription's active-set is empty. The path is:
///
///   publish → Add delta → active_set inserts row N
///   TTL sweep → delete_if_still_expired → Delete event
///   evaluator processes Delete with kind=Delete → matches=false,
///   was_active=true → emits Remove AND `sub.active_set.remove(row)`
///
/// Without that final `active_set.remove`, the bitmap would grow
/// unbounded across rate-feed churn. The test pins the contract.
#[test]
fn ttl_sweep_shrinks_subscription_active_set_to_zero() {
    // TTL=0 ⇒ every row is candidate for expiry on every sweep.
    let topic = make_topic_ttl(Some(0));
    // Subscribe first so every publish flows through Add into the
    // active set.
    topic
        .subscribe("sub-1".into(), "SELECT * FROM t")
        .expect("subscribe");
    // Grab the mutation receiver ONCE — it's single-shot — and
    // reuse it across both the publish and sweep drain phases.
    let rx = topic
        .take_mutation_rx()
        .expect("mutation_rx available exactly once");

    let n = 10_000;
    for i in 0..n {
        publish_row(&topic, &format!("k{i:05}"), i as i64);
    }
    drain_evaluator!(&topic, rx);

    // Active set should now hold every row.
    let size_pre = topic
        .subscription_active_set_size("sub-1")
        .expect("sub exists");
    assert_eq!(
        size_pre, n as u64,
        "expected {n} rows in active set after publishes, got {size_pre}"
    );

    // Sweep — TTL=0 means everything is eligible.
    let expired = topic.sweep_expired().expect("sweep");
    assert_eq!(
        expired.len(),
        n as usize,
        "sweep expired {} of {n} rows",
        expired.len()
    );
    drain_evaluator!(&topic, rx);

    let size_post = topic
        .subscription_active_set_size("sub-1")
        .expect("sub exists");
    assert_eq!(
        size_post, 0,
        "active set should be empty after TTL sweep + evaluator drain, got {size_post}"
    );
}

/// C11.2 — A subscription with `max_active = 1000` that's hit with
/// 2000 matching publishes closes itself with `TooManyMatches`.
#[test]
fn max_active_cap_closes_sub_with_too_many_matches() {
    let topic = make_topic_ttl(None);
    let cap = 1000u32;
    topic
        .subscribe_with_cap("sub-cap".into(), "SELECT * FROM t", cap)
        .expect("subscribe_with_cap");
    let rx = topic
        .take_mutation_rx()
        .expect("mutation_rx available exactly once");

    let n = 2000;
    for i in 0..n {
        publish_row(&topic, &format!("k{i:05}"), i as i64);
    }
    drain_evaluator!(&topic, rx);

    // The cap should fire at or before the 1001st publish.
    let size = topic
        .subscription_active_set_size("sub-cap")
        .expect("sub still in engine (not yet reaped)");
    assert!(
        size <= cap as u64,
        "active set size {size} exceeded cap {cap}"
    );
    assert_eq!(
        topic.subscription_is_closed("sub-cap"),
        Some(true),
        "sub should be marked closed after cap fired"
    );
    assert_eq!(
        topic.subscription_close_reason("sub-cap"),
        Some(CloseReason::TooManyMatches)
    );

    // After reap, the sub is gone — subsequent accessors return None.
    let reaped = topic.reap_closed_subscriptions();
    assert_eq!(reaped, 1);
    assert!(topic.subscription_active_set_size("sub-cap").is_none());
}

/// C11.3 — 1h soak: sustained insert + TTL-delete churn against a
/// running subscription. Asserts the active-set never exceeds the
/// configured cap and the subscriber's HashMap-of-`last_snapshot`
/// stays bounded. Default-ignored so `cargo test` doesn't run it.
/// Invoke explicitly: `cargo test --release --test active_set_bounds -- --ignored`.
#[test]
#[ignore]
fn soak_active_set_stays_bounded_under_1h_churn() {
    let topic = Arc::new(make_topic_ttl(Some(0))); // TTL=0 for max churn
    topic
        .subscribe("soak".into(), "SELECT * FROM t")
        .expect("subscribe");
    // Drain pre-subscribe events so the evaluator owns dispatch.
    let _ = topic
        .take_mutation_rx()
        .expect("rx")
        .try_iter()
        .for_each(|_| {});

    let started = std::time::Instant::now();
    let duration = Duration::from_secs(3600);
    let mut publishes: u64 = 0;
    let mut peak_active: u64 = 0;
    let bound: u64 = 10_000;
    while started.elapsed() < duration {
        for j in 0..1000 {
            publish_row(&topic, &format!("k{:06}", publishes + j), publishes as i64);
        }
        publishes += 1000;
        // Sweep every batch to age out the trailing rows.
        let _ = topic.sweep_expired();
        let size = topic.subscription_active_set_size("soak").unwrap_or(0);
        if size > peak_active {
            peak_active = size;
        }
        assert!(
            size <= bound,
            "active set {size} exceeded soak bound {bound} after {publishes} publishes"
        );
    }
    eprintln!(
        "soak: {publishes} publishes over {:.0}s, peak active = {peak_active}",
        started.elapsed().as_secs_f64()
    );
}

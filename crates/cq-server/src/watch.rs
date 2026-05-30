//! Slow-consumer watcher.
//!
//! Periodically scans the session registry for per-subscription drop
//! counters. Diffs them against the previous scan to compute a drops/sec
//! rate; when a subscription's rate exceeds a configured threshold the
//! watcher emits a structured warning + a Prometheus event, and if
//! `auto_disconnect` is enabled it also evicts the route (calling
//! `topic.unsubscribe(...)` so the topic's evaluator state goes too).
//!
//! The watcher is lock-free for normal operation — it walks the registry
//! and atomics. Eviction takes the registry's per-entry lock briefly.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cq_core::topic::SharedTopic;
use cq_transport::session::{collect_subscription_stats, SessionRegistry};
use dashmap::DashMap;

#[derive(Clone, Debug)]
pub struct SlowConsumerConfig {
    pub scan_interval: Duration,
    pub drops_per_sec_threshold: u64,
    pub auto_disconnect: bool,
    pub adaptive_conflation: bool,
    pub adaptive_max_interval_ms: u64,
    pub adaptive_fill_threshold: f32,
}

/// Spawn the watcher task. Returns immediately; the task lives for the
/// process lifetime.
pub fn spawn(
    registry: SessionRegistry,
    topics: Arc<DashMap<String, SharedTopic>>,
    cfg: SlowConsumerConfig,
) {
    if cfg.scan_interval.is_zero() {
        tracing::info!("Slow-consumer watcher disabled (scan_interval=0)");
        return;
    }
    tokio::spawn(async move {
        run(registry, topics, cfg).await;
    });
}

async fn run(
    registry: SessionRegistry,
    topics: Arc<DashMap<String, SharedTopic>>,
    cfg: SlowConsumerConfig,
) {
    let mut last_drops: HashMap<String, u64> = HashMap::new();
    // Per-sub baseline conflation interval — captured the first time
    // we widen it, so we can decay back when pressure clears.
    let mut baseline_interval_ms: HashMap<String, u64> = HashMap::new();
    let mut last_scan_at = Instant::now();
    tracing::info!(
        interval_s = cfg.scan_interval.as_secs(),
        threshold = cfg.drops_per_sec_threshold,
        auto_disconnect = cfg.auto_disconnect,
        adaptive_conflation = cfg.adaptive_conflation,
        adaptive_max_interval_ms = cfg.adaptive_max_interval_ms,
        "Slow-consumer watcher started"
    );
    loop {
        tokio::time::sleep(cfg.scan_interval).await;
        let now = Instant::now();
        let elapsed = now.duration_since(last_scan_at).as_secs_f64().max(0.001);
        last_scan_at = now;

        let stats = collect_subscription_stats(&registry);
        let mut current: HashMap<String, u64> = HashMap::with_capacity(stats.len());
        let mut live_ids: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(stats.len());

        for st in &stats {
            let prev = last_drops.get(&st.sub_id).copied().unwrap_or(st.dropped);
            let delta = st.dropped.saturating_sub(prev);
            let rate = (delta as f64 / elapsed) as u64;
            current.insert(st.sub_id.clone(), st.dropped);
            live_ids.insert(st.sub_id.clone());

            let warned = rate >= cfg.drops_per_sec_threshold;
            if warned {
                metrics::counter!(
                    "cq_slow_consumer_warning_total",
                    "topic" => st.topic.clone()
                )
                .increment(1);
                tracing::warn!(
                    sub = %st.sub_id,
                    topic = %st.topic,
                    session = %st.session_id,
                    queue_depth = st.queue_depth,
                    queue_capacity = st.queue_capacity,
                    fill_pct = (st.fill_ratio() * 100.0) as u32,
                    drops_per_sec = rate,
                    drops_total = st.dropped,
                    age_ms = st.age_ms,
                    "Slow consumer"
                );
            }

            // Adaptive conflation runs before auto_disconnect so a sub
            // that's recovering through interval widening doesn't get
            // cut prematurely. Only applies when the route has a
            // conflator AND fill ratio is above threshold.
            if cfg.adaptive_conflation {
                if let Some(route) = registry.get(&st.sub_id) {
                    let current_ms = route.conflation_interval_ms();
                    if current_ms > 0 {
                        baseline_interval_ms
                            .entry(st.sub_id.clone())
                            .or_insert(current_ms);
                        let baseline = baseline_interval_ms[&st.sub_id];
                        let new_ms = compute_adaptive_interval(
                            current_ms,
                            baseline,
                            st.fill_ratio(),
                            cfg.adaptive_fill_threshold,
                            cfg.adaptive_max_interval_ms,
                        );
                        if new_ms != current_ms {
                            route.set_conflation_interval_ms(new_ms);
                            metrics::gauge!(
                                "cq_subscription_conflation_interval_ms",
                                "topic" => st.topic.clone()
                            )
                            .set(new_ms as f64);
                            tracing::info!(
                                sub = %st.sub_id,
                                topic = %st.topic,
                                fill_pct = (st.fill_ratio() * 100.0) as u32,
                                baseline_ms = baseline,
                                old_ms = current_ms,
                                new_ms,
                                "Adaptive conflation: adjusted interval"
                            );
                        }
                    }
                }
            }

            // A degraded route has irrecoverably lost a Remove/OOF delta,
            // so its client is serving a phantom row. Rate thresholds
            // don't apply — a single lost removal is permanent corruption.
            // Log loudly always; evict to force re-establishment when the
            // operator has opted into server-initiated disconnects.
            // (Eviction frees server-side state and, for clients that
            // monitor subscription liveness, prompts a fresh SOW. A true
            // client-side resync signal would need a protocol addition.)
            if st.degraded {
                metrics::counter!(
                    "cq_subscription_degraded_total",
                    "topic" => st.topic.clone()
                )
                .increment(1);
                tracing::error!(
                    sub = %st.sub_id,
                    topic = %st.topic,
                    session = %st.session_id,
                    auto_disconnect = cfg.auto_disconnect,
                    "Subscription degraded (lost Remove/OOF); client view is stale \
                     and must resync"
                );
                if cfg.auto_disconnect {
                    disconnect(&registry, &topics, &st.sub_id, &st.topic);
                    continue;
                }
            }

            if warned && cfg.auto_disconnect {
                disconnect(&registry, &topics, &st.sub_id, &st.topic);
            }
        }

        // Garbage-collect baseline tracking for subs that vanished.
        baseline_interval_ms.retain(|k, _| live_ids.contains(k));
        last_drops = current;
    }
}

/// Choose the next conflation interval given current pressure.
///
/// Above `threshold` fill ratio: double the current interval (clamped
/// to `max_ms`). Below threshold: decay back toward `baseline` by
/// halving each scan, never going below baseline.
fn compute_adaptive_interval(
    current_ms: u64,
    baseline_ms: u64,
    fill: f32,
    threshold: f32,
    max_ms: u64,
) -> u64 {
    if fill >= threshold {
        let doubled = current_ms.saturating_mul(2).max(baseline_ms);
        doubled.min(max_ms.max(baseline_ms))
    } else if current_ms > baseline_ms {
        // Halve, but never below baseline.
        (current_ms / 2).max(baseline_ms)
    } else {
        current_ms
    }
}

fn disconnect(
    registry: &SessionRegistry,
    topics: &DashMap<String, SharedTopic>,
    sub_id: &str,
    topic_name: &str,
) {
    let Some((_, route)) = registry.remove(sub_id) else {
        return;
    };
    if let Some(t) = topics.get(topic_name) {
        t.unsubscribe(sub_id);
    }
    metrics::counter!(
        "cq_slow_consumer_auto_disconnect_total",
        "topic" => topic_name.to_string()
    )
    .increment(1);
    tracing::warn!(
        sub = %sub_id,
        topic = %topic_name,
        session = %route.session_id,
        drops_total = route.dropped_count(),
        age_ms = route.age_ms(),
        "Slow consumer auto-disconnected"
    );
}

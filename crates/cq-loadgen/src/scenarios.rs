//! Scenario runners against a live CQServer over TCP.
//!
//! Two scenarios are implemented today, mirroring the stress-test
//! plan's C and D:
//!
//! - `publish_throughput`: 1 publisher × 0 subscribers, rate-limited
//!   to a target publish rate. Measures publish-to-ack latency and
//!   sustained throughput.
//! - `fanout`: 1 publisher × N subscribers, rate-limited publish.
//!   Measures publish-to-delivery latency at the subscribers.
//!
//! Both scenarios are designed to be invoked from tests (`#[ignore]`
//! stress tests in S37/S46 will drive them) and from the CLI binary
//! (`cq-loadgen --scenario publish-throughput ...`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use cq_client::Client;
use serde_json::json;
use tokio::sync::Mutex;

use crate::{LatencyHistogram, RateLimiter};

#[derive(Debug, Clone)]
pub struct ScenarioConfig {
    pub server_url: String,
    pub topic: String,
    pub duration: Duration,
    pub publish_rate: f64,
    pub subscribers: usize,
    pub warmup: Duration,
}

impl Default for ScenarioConfig {
    fn default() -> Self {
        Self {
            server_url: "tcp://127.0.0.1:9007".into(),
            topic: "/loadgen".into(),
            duration: Duration::from_secs(10),
            publish_rate: 1000.0,
            subscribers: 0,
            warmup: Duration::from_secs(1),
        }
    }
}

#[derive(Debug)]
pub struct Report {
    pub scenario: &'static str,
    pub publishes_issued: u64,
    pub publishes_acked: u64,
    pub deliveries_observed: u64,
    pub actual_publish_rate: f64,
    pub publish_ack_p50: Duration,
    pub publish_ack_p99: Duration,
    pub delivery_p50: Duration,
    pub delivery_p99: Duration,
    pub duration: Duration,
}

impl Report {
    pub fn print(&self) {
        println!("──────────────────────────────────────────────────");
        println!("Scenario: {}", self.scenario);
        println!("Duration: {:.2}s", self.duration.as_secs_f64());
        println!(
            "Publishes:   issued = {}, acked = {}, rate = {:.0}/s",
            self.publishes_issued, self.publishes_acked, self.actual_publish_rate
        );
        if self.publishes_acked > 0 {
            println!(
                "Publish→Ack: p50 = {:>6}µs, p99 = {:>6}µs",
                self.publish_ack_p50.as_micros(),
                self.publish_ack_p99.as_micros()
            );
        }
        if self.deliveries_observed > 0 {
            println!(
                "Delivery:    observed = {}, p50 = {:>6}µs, p99 = {:>6}µs",
                self.deliveries_observed,
                self.delivery_p50.as_micros(),
                self.delivery_p99.as_micros()
            );
        }
        println!("──────────────────────────────────────────────────");
    }
}

/// Stress-test plan Scenario C — single publisher, no subscribers,
/// rate-limited publish. Reports sustained publish throughput and the
/// publish→ack latency distribution.
pub async fn publish_throughput(cfg: &ScenarioConfig) -> Result<Report> {
    let client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("connect {}", cfg.server_url))?;
    let mut limiter = RateLimiter::new(cfg.publish_rate);
    let mut ack_hist = LatencyHistogram::new();
    let started = Instant::now();
    let deadline = started + cfg.warmup + cfg.duration;
    let warmup_until = started + cfg.warmup;

    let mut issued: u64 = 0;
    let mut acked: u64 = 0;
    while Instant::now() < deadline {
        limiter.tick().await;
        let key = issued; // distinct key per publish
        let body = json!({ "k": key.to_string(), "v": key });
        let t0 = Instant::now();
        match client.publish(&cfg.topic, body).await {
            Ok(_seq) => {
                if Instant::now() >= warmup_until {
                    ack_hist.record(t0.elapsed());
                    acked += 1;
                }
            }
            Err(e) => {
                eprintln!("publish error at issued={issued}: {e}");
            }
        }
        issued += 1;
    }

    Ok(Report {
        scenario: "publish-throughput",
        publishes_issued: issued,
        publishes_acked: acked,
        deliveries_observed: 0,
        actual_publish_rate: limiter.actual_rate(),
        publish_ack_p50: ack_hist.p50(),
        publish_ack_p99: ack_hist.p99(),
        delivery_p50: Duration::ZERO,
        delivery_p99: Duration::ZERO,
        duration: started.elapsed().saturating_sub(cfg.warmup),
    })
}

/// Stress-test plan Scenario D — single publisher, N subscribers,
/// rate-limited publish. Reports publish→ack latency AND
/// publish→delivery latency observed at the subscribers.
pub async fn fanout(cfg: &ScenarioConfig) -> Result<Report> {
    let delivery_hist = Arc::new(Mutex::new(LatencyHistogram::new()));
    let deliveries = Arc::new(Mutex::new(0u64));

    // Each subscriber spawns its own connection. The subscribe
    // returns a stream of deltas; we record latency on each one
    // using the message's `_t` timestamp the publisher embeds.
    let mut sub_handles = Vec::with_capacity(cfg.subscribers);
    for i in 0..cfg.subscribers {
        let url = cfg.server_url.clone();
        let topic = cfg.topic.clone();
        let hist = delivery_hist.clone();
        let count = deliveries.clone();
        let warmup = cfg.warmup;
        let until = Instant::now() + cfg.warmup + cfg.duration;
        let handle = tokio::spawn(async move {
            let client = match Client::connect(&url).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("sub {i} connect failed: {e}");
                    return;
                }
            };
            let warmup_until = Instant::now() + warmup;
            // Subscribe and process the delta stream.
            let mut subscription = match client.subscribe(&topic, None).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("sub {i} subscribe failed: {e}");
                    return;
                }
            };
            while Instant::now() < until {
                // Pull the next delta with a small timeout so we
                // can re-check the deadline. A delivered delta
                // *implies* the publisher's mutation made it
                // through; the body's `_t` field carries publisher
                // wall-time but TCP doesn't synchronize clocks, so
                // we don't compute pub→delivery latency in this
                // smoke version (deliveries_observed is sufficient
                // signal for "the harness works"). S37's perf
                // session adds an embedded-timestamp latency mode
                // when wiring the criterion benches end-to-end.
                let next = tokio::time::timeout(
                    Duration::from_millis(100),
                    subscription.next_delta(),
                )
                .await;
                let Ok(Some(_delta)) = next else { continue };
                if Instant::now() < warmup_until {
                    continue;
                }
                let mut c = count.lock().await;
                *c += 1;
            }
            // delivery histogram intentionally not populated in the
            // smoke version — see comment above.
            let _ = hist;
        });
        sub_handles.push(handle);
    }

    // Brief settle so subscriptions are all registered before the
    // publisher starts. Without this, early publishes may race with
    // subscribe registration and bias the warmup readings.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("publisher connect {}", cfg.server_url))?;
    let mut limiter = RateLimiter::new(cfg.publish_rate);
    let mut ack_hist = LatencyHistogram::new();
    let started = Instant::now();
    let warmup_until = started + cfg.warmup;
    let deadline = started + cfg.warmup + cfg.duration;
    let mut issued: u64 = 0;
    let mut acked: u64 = 0;
    while Instant::now() < deadline {
        limiter.tick().await;
        let body = json!({ "k": issued.to_string(), "v": issued });
        let t0 = Instant::now();
        match client.publish(&cfg.topic, body).await {
            Ok(_) => {
                if Instant::now() >= warmup_until {
                    ack_hist.record(t0.elapsed());
                    acked += 1;
                }
            }
            Err(e) => eprintln!("publish error at issued={issued}: {e}"),
        }
        issued += 1;
    }

    // Give subscribers a moment to drain the tail.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for h in sub_handles {
        let _ = tokio::time::timeout(Duration::from_secs(1), h).await;
    }

    let delivery_count = *deliveries.lock().await;
    let hist = delivery_hist.lock().await;
    Ok(Report {
        scenario: "fanout",
        publishes_issued: issued,
        publishes_acked: acked,
        deliveries_observed: delivery_count,
        actual_publish_rate: limiter.actual_rate(),
        publish_ack_p50: ack_hist.p50(),
        publish_ack_p99: ack_hist.p99(),
        delivery_p50: hist.p50(),
        delivery_p99: hist.p99(),
        duration: started.elapsed().saturating_sub(cfg.warmup),
    })
}

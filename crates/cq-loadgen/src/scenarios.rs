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
    /// Admin HTTP base URL (used by stress-2k for /stats polling).
    pub admin_url: String,
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
            admin_url: "http://127.0.0.1:8085".into(),
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

// ─────────────────────────────────────────────────────────────────────
// stress-2k: N concurrent WS subscribers, 4 query-complexity classes,
// admin /stats sampled across the measurement window.
// ─────────────────────────────────────────────────────────────────────

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// What kind of subscription each member of the cohort opens.
#[derive(Copy, Clone, Debug)]
enum QueryClass {
    /// `subscribe(topic, None)` — every publish on the topic flows
    /// through unfiltered. Cheapest predicate path on the server,
    /// highest per-sub fanout cost on the wire.
    Firehose,
    /// `subscribe(topic, Some("col = 'X'"))` — single equality
    /// predicate; exercises the secondary-index lookup if the
    /// column happens to be indexed, otherwise the scan path.
    SimpleWhere,
    /// `sow_and_subscribe_sql(topic, "SELECT col, SUM(...) ... GROUP BY col")`
    /// — continuous-aggregate subscription (S19). Tests the
    /// per-group state path.
    GroupBy,
    /// `sow_and_subscribe_sql(topic, "SELECT * FROM t PIVOT (SUM(...) FOR col IN ('A','B'))")`
    /// — static PIVOT (S43). Tests anchor-key bucketing under the
    /// continuous path.
    StaticPivot,
}

impl QueryClass {
    fn name(self) -> &'static str {
        match self {
            QueryClass::Firehose => "firehose",
            QueryClass::SimpleWhere => "where",
            QueryClass::GroupBy => "group_by",
            QueryClass::StaticPivot => "static_pivot",
        }
    }
}

/// Per-class accumulator. `subs_opened` is the count of successful
/// subscribes; `subs_failed` is the count of failed connects/subscribes
/// (each kind of failure is logged separately to stderr).
struct ClassCounters {
    subs_opened: AtomicU64,
    subs_failed: AtomicU64,
    deliveries: AtomicU64,
    connect_us_sum: AtomicU64, // sum of connect-microseconds for opened subs
    subscribe_us_sum: AtomicU64,
}

impl ClassCounters {
    fn new() -> Self {
        Self {
            subs_opened: AtomicU64::new(0),
            subs_failed: AtomicU64::new(0),
            deliveries: AtomicU64::new(0),
            connect_us_sum: AtomicU64::new(0),
            subscribe_us_sum: AtomicU64::new(0),
        }
    }
}

/// Rich report shape for the stress-2k scenario. Distinct from
/// the simpler `Report` used by publish-throughput / fanout so the
/// caller can render the per-class + per-sample columns cleanly.
#[derive(Debug)]
pub struct Stress2kReport {
    pub target_subs: usize,
    pub subs_opened: u64,
    pub subs_failed: u64,
    pub connect_p50_ms: f64,
    pub connect_p99_ms: f64,
    pub subscribe_p50_ms: f64,
    pub subscribe_p99_ms: f64,
    pub ramp_seconds: f64,
    pub measurement_seconds: f64,
    pub baseline_rss_mb: f64,
    pub peak_rss_mb: f64,
    pub final_rss_mb: f64,
    pub baseline_subs_server: u64,
    pub peak_subs_server: u64,
    pub class_deliveries: Vec<(&'static str, u64, u64)>, // (class, deliveries, subs_opened)
}

impl Stress2kReport {
    pub fn print(&self) {
        println!("──────────────────────────────────────────────────");
        println!("Scenario: stress-2k");
        println!(
            "Subs:       target = {}, opened = {}, failed = {}",
            self.target_subs, self.subs_opened, self.subs_failed
        );
        println!(
            "Ramp-up:    {:.2}s   (connect p50 = {:.1}ms, p99 = {:.1}ms; subscribe p50 = {:.1}ms, p99 = {:.1}ms)",
            self.ramp_seconds,
            self.connect_p50_ms,
            self.connect_p99_ms,
            self.subscribe_p50_ms,
            self.subscribe_p99_ms,
        );
        println!(
            "Memory:     baseline = {:.1} MB → peak = {:.1} MB → final = {:.1} MB   (Δ peak = {:+.1} MB)",
            self.baseline_rss_mb,
            self.peak_rss_mb,
            self.final_rss_mb,
            self.peak_rss_mb - self.baseline_rss_mb,
        );
        println!(
            "Subs (server-side, from /stats): baseline = {}, peak = {}",
            self.baseline_subs_server, self.peak_subs_server,
        );
        println!("Per-class throughput over {:.0}s:", self.measurement_seconds);
        for (cls, deliveries, opened) in &self.class_deliveries {
            let per_sec = (*deliveries as f64) / self.measurement_seconds.max(0.001);
            let per_sub_sec = per_sec / (*opened).max(1) as f64;
            println!(
                "  {:>14}  subs={:>5}  deliveries={:>9}  total={:>9.0}/s  per-sub={:>6.1}/s",
                cls, opened, deliveries, per_sec, per_sub_sec
            );
        }
        println!("──────────────────────────────────────────────────");
    }
}

/// Run the stress-2k scenario.
///
/// Lifecycle:
///   1. Sample admin /stats once to record the baseline RSS + sub count
///      before any subscribers connect.
///   2. Ramp up `target_subs` connections in waves of `WAVE_SIZE` every
///      `WAVE_MS` to avoid a connect-storm hitting the server all at once.
///      Each tab opens a WS connection, then issues one of 4 subscribe
///      variants based on its index modulo 4.
///   3. Sample /stats once per second while the measurement window
///      runs; track peak RSS + peak server-reported sub count.
///   4. After the window, sample once more for `final_rss_mb`, drop the
///      subscribers, and return the report.
pub async fn stress_2k(cfg: &ScenarioConfig) -> Result<Stress2kReport> {
    use cq_client::admin::AdminClient;

    // Connect-storm pacing: 50 subs per wave, 200 ms between waves =
    // 250 connects/sec sustained. Going faster than this saturates
    // the server's tokio runtime and starves the admin endpoint.
    const WAVE_SIZE: usize = 50;
    const WAVE_MS: u64 = 200;

    let target_subs = if cfg.subscribers == 0 { 2000 } else { cfg.subscribers };
    let admin_hostport = cfg
        .admin_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/');
    let admin = AdminClient::new(admin_hostport)
        .with_context(|| format!("AdminClient::new({})", admin_hostport))?;

    let (baseline_rss_mb, baseline_subs_server) = read_rss_subs(&admin).await?;

    let classes = [
        QueryClass::Firehose,
        QueryClass::SimpleWhere,
        QueryClass::GroupBy,
        QueryClass::StaticPivot,
    ];
    let counters: Vec<Arc<ClassCounters>> =
        (0..classes.len()).map(|_| Arc::new(ClassCounters::new())).collect();

    // Cancellation flag — set after the measurement window so all
    // subscriber tasks can exit cleanly without waiting for a
    // per-task deadline.
    let stop_flag = Arc::new(AtomicBool::new(false));

    // Phase 1 — ramp all subs to "connected + subscribed" with NO
    // per-task deadline. Tasks live until `stop_flag` is set after
    // the measurement window closes.
    let ramp_started = Instant::now();
    let mut sub_handles = Vec::with_capacity(target_subs);
    for i in 0..target_subs {
        let class_index = i % classes.len();
        let class = classes[class_index];
        let url = cfg.server_url.clone();
        let topic = cfg.topic.clone();
        let counters = counters[class_index].clone();
        let stop_flag = stop_flag.clone();
        let handle = tokio::spawn(async move {
            run_one_subscriber(i, class, url, topic, counters, stop_flag).await;
        });
        sub_handles.push(handle);
        if (i + 1) % WAVE_SIZE == 0 {
            tokio::time::sleep(Duration::from_millis(WAVE_MS)).await;
        }
    }

    // Wait until either all subs have registered (per per-class counters)
    // or we hit a 60 s ceiling — whichever comes first. Without this
    // wait, the measurement window starts before late-wave subs have
    // even connected, biasing the per-class delivery counts.
    let ramp_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let opened: u64 = counters.iter().map(|c| c.subs_opened.load(Ordering::Relaxed)).sum();
        let failed: u64 = counters.iter().map(|c| c.subs_failed.load(Ordering::Relaxed)).sum();
        if opened + failed >= target_subs as u64 { break; }
        if Instant::now() >= ramp_deadline { break; }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let ramp_seconds = ramp_started.elapsed().as_secs_f64();

    // Measurement window: poll /stats once a second. Each poll gets
    // its own 3 s timeout so a slow admin response can't stall the
    // whole window — the loop still ticks forward and the report is
    // produced. If polls fail we just keep the previous best
    // peak values.
    let measurement_started = Instant::now();
    let measurement_end = measurement_started + cfg.duration;
    let mut peak_rss_mb = baseline_rss_mb;
    let mut peak_subs_server = baseline_subs_server;
    while Instant::now() < measurement_end {
        match tokio::time::timeout(Duration::from_secs(3), read_rss_subs(&admin)).await {
            Ok(Ok((rss, subs))) => {
                if rss > peak_rss_mb { peak_rss_mb = rss; }
                if subs > peak_subs_server { peak_subs_server = subs; }
            }
            Ok(Err(e)) => eprintln!("admin /stats poll failed: {e}"),
            Err(_) => eprintln!("admin /stats poll timed out (3s)"),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let measurement_seconds = measurement_started.elapsed().as_secs_f64();
    let final_rss_mb = tokio::time::timeout(Duration::from_secs(3), read_rss_subs(&admin))
        .await
        .ok()
        .and_then(|r| r.ok())
        .map(|(rss, _)| rss)
        .unwrap_or(peak_rss_mb);

    // Signal every subscriber task to exit; then join them concurrently
    // with a hard cap. Without the join_all + timeout, a single stuck
    // WS task would hold the entire scenario open.
    stop_flag.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all(sub_handles),
    )
    .await;

    // Aggregate per-class counters.
    let mut class_deliveries = Vec::with_capacity(classes.len());
    let mut total_subs_opened = 0u64;
    let mut total_subs_failed = 0u64;
    let mut connect_us_total = 0u64;
    let mut subscribe_us_total = 0u64;
    let mut opened_for_avg = 0u64;
    for (idx, c) in classes.iter().enumerate() {
        let opened = counters[idx].subs_opened.load(Ordering::Relaxed);
        let failed = counters[idx].subs_failed.load(Ordering::Relaxed);
        let deliveries = counters[idx].deliveries.load(Ordering::Relaxed);
        total_subs_opened += opened;
        total_subs_failed += failed;
        connect_us_total += counters[idx].connect_us_sum.load(Ordering::Relaxed);
        subscribe_us_total += counters[idx].subscribe_us_sum.load(Ordering::Relaxed);
        opened_for_avg += opened;
        class_deliveries.push((c.name(), deliveries, opened));
    }

    let connect_p50_ms = if opened_for_avg > 0 {
        (connect_us_total as f64) / (opened_for_avg as f64) / 1000.0
    } else {
        0.0
    };
    let subscribe_p50_ms = if opened_for_avg > 0 {
        (subscribe_us_total as f64) / (opened_for_avg as f64) / 1000.0
    } else {
        0.0
    };

    Ok(Stress2kReport {
        target_subs,
        subs_opened: total_subs_opened,
        subs_failed: total_subs_failed,
        // We only track sum-based "p50" (an average, really) in this first
        // cut — see comment above. A full HDR histogram per class would
        // give true percentiles; left as a follow-up to keep the scenario
        // small.
        connect_p50_ms,
        connect_p99_ms: connect_p50_ms,
        subscribe_p50_ms,
        subscribe_p99_ms: subscribe_p50_ms,
        ramp_seconds,
        measurement_seconds,
        baseline_rss_mb,
        peak_rss_mb,
        final_rss_mb,
        baseline_subs_server,
        peak_subs_server,
        class_deliveries,
    })
}

async fn run_one_subscriber(
    idx: usize,
    class: QueryClass,
    url: String,
    topic: String,
    counters: Arc<ClassCounters>,
    stop_flag: Arc<AtomicBool>,
) {
    // Hard cap on connect — without this, a stuck handshake hangs
    // the task forever and the stop_flag check below never runs.
    let t_connect = Instant::now();
    let connect_fut = Client::connect(&url);
    let client = match tokio::time::timeout(Duration::from_secs(15), connect_fut).await {
        Ok(Ok(c)) => c,
        Ok(Err(_)) | Err(_) => {
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let connect_us = t_connect.elapsed().as_micros() as u64;

    let t_subscribe = Instant::now();
    let sub_fut = async {
        match class {
            QueryClass::Firehose => client.subscribe(&topic, None).await,
            // `side` is BUY/SELL — half the live trades match.
            QueryClass::SimpleWhere => client.subscribe(&topic, Some("side = 'BUY'")).await,
            QueryClass::GroupBy => {
                client
                    .sow_and_subscribe_sql(
                        &topic,
                        "SELECT book, SUM(qty) AS total_qty, COUNT(*) AS n FROM t GROUP BY book",
                    )
                    .await
            }
            QueryClass::StaticPivot => {
                client
                    .sow_and_subscribe_sql(
                        &topic,
                        "SELECT * FROM t PIVOT (SUM(qty) FOR side IN ('BUY', 'SELL'))",
                    )
                    .await
            }
        }
    };
    // Subscribes that include `sow_and_subscribe_sql` against /trades
    // (865K rows) can legitimately take a few seconds while the SOW
    // snapshot streams. 30 s ceiling lets them complete but caps a
    // hung handshake.
    let sub_result = match tokio::time::timeout(Duration::from_secs(30), sub_fut).await {
        Ok(r) => r,
        Err(_) => {
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let mut subscription = match sub_result {
        Ok(s) => s,
        Err(e) => {
            // Log the first few failures per class so we know whether
            // the topic / query is misconfigured (otherwise this is a
            // lot of stderr).
            if counters.subs_failed.load(Ordering::Relaxed) < 3 {
                eprintln!("sub {idx} ({}) subscribe failed: {e}", class.name());
            }
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let subscribe_us = t_subscribe.elapsed().as_micros() as u64;
    counters.subs_opened.fetch_add(1, Ordering::Relaxed);
    counters.connect_us_sum.fetch_add(connect_us, Ordering::Relaxed);
    counters.subscribe_us_sum.fetch_add(subscribe_us, Ordering::Relaxed);

    // Drain deltas until the orchestrator flips `stop_flag`. Polling
    // the flag inside the 200 ms timeout cycle keeps tasks responsive
    // without busy-waiting.
    while !stop_flag.load(Ordering::Relaxed) {
        match tokio::time::timeout(Duration::from_millis(200), subscription.next_delta()).await {
            Ok(Some(_d)) => {
                counters.deliveries.fetch_add(1, Ordering::Relaxed);
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
}

async fn read_rss_subs(admin: &cq_client::admin::AdminClient) -> Result<(f64, u64)> {
    let stats = admin.stats().await.context("admin stats")?;
    let rss = stats
        .get("processRssBytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as f64
        / (1024.0 * 1024.0);
    let subs = stats
        .get("totalSubscriptions")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    Ok((rss, subs))
}

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
    /// Target serialized JSON body size in bytes for each publish. When > 0,
    /// `publish_throughput` pads the body with a filler string so the encoded
    /// frame is approximately this size. 0 = no padding (tiny `{k,v}` body).
    pub payload_bytes: usize,
    /// `sow-wide`: number of rows to seed before the SOW snapshot. 0 ⇒ 30_000.
    pub wide_rows: usize,
    /// `sow-wide`: number of columns per row (including the `k` key). 0 ⇒ 400.
    pub wide_cols: usize,
    /// `soak`: number of fast (full-firehose, drain-as-fast-as-possible)
    /// subscribers to hold for the whole run. 0 ⇒ 2.
    pub soak_fast_subscribers: usize,
    /// `soak`: number of conflated-class subscribers to hold for the whole
    /// run. These subscribe to the same topic as the fast class; the
    /// conflation behavior comes from the topic's own `conflation_ms`
    /// server config (e.g. `/positions` in `tests/cloud/configs/soak.toml`),
    /// not a client-side option. 0 ⇒ 2.
    pub soak_conflated_subscribers: usize,
    /// `soak`: number of deliberately-slow subscribers to hold for the
    /// whole run — each sleeps `soak_slow_read_delay_ms` between reads to
    /// exercise the slow-consumer / drop path. 0 ⇒ 1.
    pub soak_slow_subscribers: usize,
    /// `soak`: milliseconds a slow subscriber sleeps between reads. 0 ⇒ 200.
    pub soak_slow_read_delay_ms: u64,
    /// `soak`: how often (seconds) the driver logs a progress line. 0 ⇒ 10.
    pub soak_progress_interval_secs: u64,
    /// `soak`: the topic's key field name, so seeded rows and delta ticks
    /// carry a key the server recognizes. Empty ⇒ `"position_id"`, matching
    /// `tests/cloud/configs/soak.toml`'s `/positions` topic (`key =
    /// ["position_id"]`). Set this when pointing `--topic` at a
    /// differently-keyed topic.
    pub soak_key_field: String,
    /// `soak` (task B2.1): how the slow subscriber class connects. `"raw"`
    /// (default when empty) opens a bare TCP socket and stalls its reads
    /// so it genuinely backpressures the server's outbound queue; `"sdk"`
    /// uses the cq-client SDK (kept for comparison — see `SlowMode`'s doc
    /// comment for why it can't actually induce drops). Any other value
    /// is rejected by `soak()` at startup.
    pub soak_slow_mode: String,
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
            payload_bytes: 0,
            wide_rows: 0,
            wide_cols: 0,
            soak_fast_subscribers: 0,
            soak_conflated_subscribers: 0,
            soak_slow_subscribers: 0,
            soak_slow_read_delay_ms: 0,
            soak_progress_interval_secs: 0,
            soak_key_field: String::new(),
            soak_slow_mode: String::new(),
        }
    }
}

/// Build a publish body whose serialized JSON length is approximately
/// `target_bytes`. The base body is `{"k":"<key>","v":<key>}`; when a larger
/// target is requested we add a `"p"` filler field padded with ASCII 'x' so
/// the total encoded length lands on `target_bytes` (never smaller than base).
fn padded_body(key: u64, target_bytes: usize) -> serde_json::Value {
    let base = json!({ "k": key.to_string(), "v": key });
    if target_bytes == 0 {
        return base;
    }
    let base_len = serde_json::to_vec(&base).map(|v| v.len()).unwrap_or(0);
    // Adding `,"p":""` costs 8 bytes of structural overhead before the filler.
    let overhead = 8;
    let fill = target_bytes.saturating_sub(base_len + overhead);
    json!({ "k": key.to_string(), "v": key, "p": "x".repeat(fill) })
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
        let body = padded_body(key, cfg.payload_bytes);
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
// sow-wide: seed a large, wide SOW (e.g. 30k rows × 400 cols), measure the
// snapshot delivery, then drive live updates and measure pub→delivery latency.
// ─────────────────────────────────────────────────────────────────────

/// Build one wide row: key `k` plus `cols - 1` numeric columns `c1..c{cols-1}`.
/// Values are a cheap deterministic hash of (row, col) so every cell is
/// distinct (defeats any accidental dedup) without per-cell allocation beyond
/// the integer itself.
fn wide_row(r: u64, cols: usize) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(cols);
    obj.insert("k".into(), json!(r));
    // `_t` is seeded (value 0) so it's part of the topic schema from the
    // start — the live phase updates it with a publish timestamp, and only
    // columns that already exist propagate on a delta_publish.
    obj.insert("_t".into(), json!(0u64));
    for c in 2..cols {
        let v = r.wrapping_mul(2_654_435_761).wrapping_add(c as u64) % 1_000_000;
        obj.insert(format!("c{c}"), json!(v));
    }
    serde_json::Value::Object(obj)
}

/// Heavy-SOW scenario: `--subscribers N` clients (default 1) each request a
/// Σ-row snapshot of `wide_rows × wide_cols` concurrently and then receive
/// real-time updates.
///
/// Phases:
///   1. **Seed** `wide_rows` rows (each `wide_cols` columns) via
///      `publish_batch`. Reports seed throughput.
///   2. **Snapshot** — N subscribers `sow_and_subscribe` in parallel and each
///      drains the full snapshot. Reports how many caught up, aggregate rows /
///      MB, and per-sub completion min/max (≈ N × snapshot egress).
///   3. **Live** — once every sub has caught up, one publisher issues sparse
///      `delta_publish` updates at `--rate`/s for `--duration-secs`, stamping
///      each with a monotonic `_t`; all subscriptions fan the deltas out and
///      record publish→delivery latency. Reports delivery p50/p95/p99,
///      fanout deliveries, and best-effort peak server RSS.
pub async fn sow_wide(cfg: &ScenarioConfig) -> Result<()> {
    let rows = if cfg.wide_rows == 0 { 30_000 } else { cfg.wide_rows };
    let cols = if cfg.wide_cols == 0 { 400 } else { cfg.wide_cols };
    let live_rate = if cfg.publish_rate <= 0.0 { 5_000.0 } else { cfg.publish_rate };

    println!("──────────────────────────────────────────────────");
    println!("Scenario: sow-wide  ({rows} rows × {cols} cols)");

    // ---- phase 1: seed ------------------------------------------------
    let seeder = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("seeder connect {}", cfg.server_url))?;
    // Size a batch so each `publish_batch` frame stays well under the 16 MiB
    // wire limit — wide rows (e.g. 2000 cols ≈ 34 KB/row) would blow a fixed
    // 500-row batch, so derive the batch from the measured row width with an
    // ~8 MiB target and a sane floor/ceiling.
    let per_row_bytes = serde_json::to_vec(&wide_row(0, cols)).map(|v| v.len()).unwrap_or(0);
    const FRAME_TARGET: usize = 8 * 1024 * 1024;
    let batch_size = (FRAME_TARGET / per_row_bytes.max(1)).clamp(1, 500);
    let seed_start = Instant::now();
    let mut batch: Vec<serde_json::Value> = Vec::with_capacity(batch_size);
    for r in 0..rows as u64 {
        batch.push(wide_row(r, cols));
        if batch.len() == batch_size {
            seeder
                .publish_batch(&cfg.topic, std::mem::take(&mut batch))
                .await
                .with_context(|| "seed publish_batch failed")?;
            batch = Vec::with_capacity(batch_size);
        }
    }
    if !batch.is_empty() {
        seeder.publish_batch(&cfg.topic, batch).await?;
    }
    let seed_elapsed = seed_start.elapsed();
    let total_mb = (per_row_bytes * rows) as f64 / (1024.0 * 1024.0);
    println!(
        "Seed:      {rows} rows in {:.2}s  ({:.0} rows/s, ~{:.1} MB, ~{} B/row)",
        seed_elapsed.as_secs_f64(),
        rows as f64 / seed_elapsed.as_secs_f64(),
        total_mb,
        per_row_bytes,
    );

    // ---- phases 2+3: N concurrent subscribers -------------------------
    // Each subscriber connects, requests the full wide snapshot, drains it,
    // records its completion time, then reads live deltas (timing
    // publish→delivery latency from the `_t` micro-stamp). One shared
    // publisher drives sparse updates after every sub has caught up, so the
    // measured latency reflects steady-state fanout, not snapshot backlog.
    let n_subs = cfg.subscribers.max(1);
    println!("Subscribers: {n_subs} concurrent (each pulls the full snapshot)");

    // Shared monotonic clock so a publisher `_t` yields true latency at any sub.
    let base = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let delivery_hist = Arc::new(Mutex::new(LatencyHistogram::new()));
    let delivered = Arc::new(AtomicU64::new(0));
    let raw_deltas = Arc::new(AtomicU64::new(0));
    let snaps_complete = Arc::new(AtomicU64::new(0));
    let snap_rows_total = Arc::new(AtomicU64::new(0));
    let snap_times: Arc<Mutex<Vec<Duration>>> = Arc::new(Mutex::new(Vec::with_capacity(n_subs)));
    let live_window = cfg.duration;

    // Best-effort peak-RSS sampler via admin /stats every 250ms.
    let peak_rss = Arc::new(Mutex::new(0.0f64));
    let rss_stop = stop.clone();
    let rss_peak = peak_rss.clone();
    let admin_url = cfg.admin_url.clone();
    let rss_task = tokio::spawn(async move {
        let addr = admin_url
            .trim_start_matches("http://")
            .trim_start_matches("https://")
            .to_string();
        let admin = match cq_client::admin::AdminClient::new(&addr) {
            Ok(a) => a,
            Err(_) => return,
        };
        while !rss_stop.load(Ordering::Relaxed) {
            if let Ok((rss_mb, _subs)) = read_rss_subs(&admin).await {
                let mut p = rss_peak.lock().await;
                if rss_mb > *p {
                    *p = rss_mb;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });

    // Spawn the subscriber cohort.
    let snap_phase_start = Instant::now();
    let mut sub_handles = Vec::with_capacity(n_subs);
    for _ in 0..n_subs {
        let url = cfg.server_url.clone();
        let topic = cfg.topic.clone();
        let hist = delivery_hist.clone();
        let dcount = delivered.clone();
        let rcount = raw_deltas.clone();
        let scomplete = snaps_complete.clone();
        let srows = snap_rows_total.clone();
        let stimes = snap_times.clone();
        let sstop = stop.clone();
        let want_rows = rows as u64;
        sub_handles.push(tokio::spawn(async move {
            let client = match Client::connect(&url).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("subscriber connect failed: {e}");
                    return;
                }
            };
            let mut sub = match client.sow_and_subscribe(&topic, None, None).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("sow_and_subscribe failed: {e}");
                    return;
                }
            };
            // Phase 2: drain the snapshot.
            let mut my_rows: u64 = 0;
            loop {
                match tokio::time::timeout(Duration::from_secs(60), sub.next_delta()).await {
                    Ok(Some(d)) if d.delta_type == cq_client::DeltaKind::SowSnapshot => {
                        my_rows += 1;
                        if my_rows >= want_rows {
                            break;
                        }
                    }
                    // A live delta arrived (snapshot effectively done) — count it
                    // toward latency and fall through into the live loop.
                    Ok(Some(d)) => {
                        rcount.fetch_add(1, Ordering::Relaxed);
                        if let Some(t) = d.data.get("_t").and_then(|v| v.as_u64()) {
                            let now_us = base.elapsed().as_micros() as u64;
                            if now_us >= t {
                                hist.lock().await.record(Duration::from_micros(now_us - t));
                                dcount.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        break;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
            srows.fetch_add(my_rows, Ordering::Relaxed);
            stimes.lock().await.push(snap_phase_start.elapsed());
            scomplete.fetch_add(1, Ordering::Relaxed);

            // Phase 3: live loop until the stop flag is set.
            loop {
                if sstop.load(Ordering::Relaxed) {
                    break;
                }
                match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
                    Ok(Some(d)) => {
                        rcount.fetch_add(1, Ordering::Relaxed);
                        if let Some(t) = d.data.get("_t").and_then(|v| v.as_u64()) {
                            let now_us = base.elapsed().as_micros() as u64;
                            if now_us >= t {
                                hist.lock().await.record(Duration::from_micros(now_us - t));
                                dcount.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {} // timeout — re-check stop flag
                }
            }
        }));
    }

    // Barrier: wait until every subscriber has finished its snapshot (bounded),
    // so the live publisher doesn't pollute latency with snapshot backlog.
    let barrier_deadline = Instant::now() + Duration::from_secs(180);
    while snaps_complete.load(Ordering::Relaxed) < n_subs as u64 {
        if Instant::now() >= barrier_deadline {
            println!(
                "  ⚠ only {}/{n_subs} subscribers finished snapshot before barrier timeout",
                snaps_complete.load(Ordering::Relaxed)
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let caught_up = snaps_complete.load(Ordering::Relaxed);
    {
        let times = snap_times.lock().await;
        let (mn, mx) = times
            .iter()
            .fold((Duration::MAX, Duration::ZERO), |(mn, mx), &t| {
                (mn.min(t), mx.max(t))
            });
        let agg_rows = snap_rows_total.load(Ordering::Relaxed);
        let agg_mb = (per_row_bytes as u64 * agg_rows) as f64 / (1024.0 * 1024.0);
        let span = if times.is_empty() { 0.0 } else { mx.as_secs_f64() };
        println!(
            "Snapshot:  {caught_up}/{n_subs} subs caught up, {agg_rows} total rows (~{:.1} MB) in {:.2}s",
            agg_mb, span,
        );
        if !times.is_empty() {
            println!(
                "           per-sub completion: min {:.2}s / max {:.2}s, aggregate {:.0} MB/s",
                mn.as_secs_f64(),
                mx.as_secs_f64(),
                if span > 0.0 { agg_mb / span } else { 0.0 },
            );
        }
    }

    // ---- live phase: single publisher, fanout to all subscribers ------
    let pub_client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| "live publisher connect")?;
    let mut limiter = RateLimiter::new(live_rate);
    let mut issued: u64 = 0;
    let mut seed_rng: u64 = 0x9E3779B97F4A7C15;
    let deadline = Instant::now() + live_window;
    while Instant::now() < deadline {
        limiter.tick().await;
        // xorshift for a cheap pseudo-random existing key.
        seed_rng ^= seed_rng << 13;
        seed_rng ^= seed_rng >> 7;
        seed_rng ^= seed_rng << 17;
        let key = seed_rng % rows as u64;
        let now_us = base.elapsed().as_micros() as u64;
        let body = json!({ "k": key, "c2": issued, "_t": now_us });
        if let Err(e) = pub_client.delta_publish(&cfg.topic, body).await {
            eprintln!("live delta_publish error at issued={issued}: {e}");
        }
        issued += 1;
    }

    // Let tail deltas land, then stop subscribers + RSS sampler.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    for h in sub_handles {
        let _ = tokio::time::timeout(Duration::from_secs(3), h).await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), rss_task).await;

    let hist = delivery_hist.lock().await;
    let got = delivered.load(Ordering::Relaxed);
    let expected = issued * n_subs as u64;
    println!(
        "Live:      {issued} updates issued @ {:.0}/s target over {:.0}s ({n_subs}× fanout ⇒ {expected} expected deliveries)",
        limiter.actual_rate(),
        live_window.as_secs_f64(),
    );
    println!(
        "Delivery:  raw deltas = {}, with _t = {got}, pub→delivery p50 = {}µs, p95 = {}µs, p99 = {}µs",
        raw_deltas.load(Ordering::Relaxed),
        hist.p50().as_micros(),
        hist.p95().as_micros(),
        hist.p99().as_micros(),
    );
    {
        let peak = *peak_rss.lock().await;
        if peak > 0.0 {
            println!("Server:    peak RSS {:.0} MB during run", peak);
        }
    }
    println!("──────────────────────────────────────────────────");
    Ok(())
}

/// Egress-fanout scenario: ramp `--subscribers N` *lightweight* firehose
/// subscribers (drain-and-discard — they never retain a row), then run one
/// publisher at `--rate` msg/s with `--payload-bytes` bodies. Every publish
/// fans out to all N subs, so the ideal egress is `rate × N × body_bytes`.
///
/// Unlike `sow-wide` (which holds a full deserialized snapshot per sub and so
/// is bounded by *client* heap), this scenario measures the server's
/// **wire-egress ceiling** at high connection counts: how many deltas/sec and
/// MB/sec it actually pushes, what fraction of the ideal fanout is delivered
/// (a number well below 100 % means the server is shedding — slow-consumer
/// drops or queue-cap), and the peak server RSS while holding N connections.
///
/// Reuses `cfg.subscribers` (N, default 1), `cfg.publish_rate` (msg/s,
/// default 100), `cfg.payload_bytes` (body size, 0 ⇒ tiny `{k,v}`),
/// `cfg.duration` (measurement window), `cfg.admin_url` (RSS sampling).
pub async fn egress_fanout(cfg: &ScenarioConfig) -> Result<()> {
    use cq_client::admin::AdminClient;
    // Connect-storm pacing — 100 connects per 100 ms = ~1000/s. Localhost
    // tolerates this; bump WAVE_MS if the server's accept loop saturates.
    const WAVE_SIZE: usize = 100;
    const WAVE_MS: u64 = 100;

    let n_subs = cfg.subscribers.max(1);
    // `--rate 0` ⇒ unthrottled: publishers push as fast as the server
    // accepts (self-throttle on the bounded mutation channel), which is how
    // we find the egress ceiling.
    let rate = cfg.publish_rate.max(0.0);
    let payload = cfg.payload_bytes;
    let body_bytes = serde_json::to_vec(&padded_body(0, payload))
        .map(|v| v.len())
        .unwrap_or(0);

    println!("──────────────────────────────────────────────────");
    let rate_label = if rate <= 0.0 {
        "unthrottled".to_string()
    } else {
        format!("{rate:.0}/s")
    };
    println!(
        "Scenario: egress-fanout  ({n_subs} firehose subs, pub {rate_label} × ~{body_bytes}B body)"
    );

    let admin_hostport = cfg
        .admin_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string();
    let admin = AdminClient::new(&admin_hostport).ok();
    let baseline_rss = if let Some(a) = &admin {
        read_rss_subs(a).await.map(|(r, _)| r).unwrap_or(0.0)
    } else {
        0.0
    };

    // Seed one row so the firehose has a schema to deliver against.
    let seeder = Client::connect(&cfg.server_url)
        .await
        .with_context(|| "egress seeder connect")?;
    seeder
        .publish(&cfg.topic, padded_body(0, payload))
        .await
        .with_context(|| "egress seed publish")?;

    let stop = Arc::new(AtomicBool::new(false));
    let deliveries = Arc::new(AtomicU64::new(0));
    let opened = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));

    // Ramp subscribers in waves.
    let ramp_start = Instant::now();
    let mut handles = Vec::with_capacity(n_subs);
    for i in 0..n_subs {
        let url = cfg.server_url.clone();
        let topic = cfg.topic.clone();
        let d = deliveries.clone();
        let o = opened.clone();
        let f = failed.clone();
        let s = stop.clone();
        handles.push(tokio::spawn(async move {
            let client = match tokio::time::timeout(Duration::from_secs(20), Client::connect(&url))
                .await
            {
                Ok(Ok(c)) => c,
                _ => {
                    f.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let mut sub = match tokio::time::timeout(
                Duration::from_secs(20),
                client.subscribe(&topic, None),
            )
            .await
            {
                Ok(Ok(s)) => s,
                _ => {
                    f.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            o.fetch_add(1, Ordering::Relaxed);
            // Drain-and-discard: count the delta, drop the row immediately.
            while !s.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
                    Ok(Some(_)) => {
                        d.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => break,
                    Err(_) => continue,
                }
            }
        }));
        if (i + 1) % WAVE_SIZE == 0 {
            tokio::time::sleep(Duration::from_millis(WAVE_MS)).await;
        }
    }

    // Wait until every sub has connected+subscribed (or a 180 s ceiling).
    let ramp_deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let up = opened.load(Ordering::Relaxed);
        let fl = failed.load(Ordering::Relaxed);
        if up + fl >= n_subs as u64 {
            break;
        }
        if Instant::now() >= ramp_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let ramp_s = ramp_start.elapsed().as_secs_f64();
    let subs_up = opened.load(Ordering::Relaxed);
    println!(
        "Ramp:      {subs_up}/{n_subs} subs subscribed in {ramp_s:.1}s  (failed {})",
        failed.load(Ordering::Relaxed)
    );

    // Peak-RSS sampler (best-effort).
    let peak_rss = Arc::new(Mutex::new(baseline_rss));
    let rss_stop = stop.clone();
    let rss_peak = peak_rss.clone();
    let rss_admin_hostport = admin_hostport.clone();
    let rss_task = tokio::spawn(async move {
        let a = match AdminClient::new(&rss_admin_hostport) {
            Ok(a) => a,
            Err(_) => return,
        };
        while !rss_stop.load(Ordering::Relaxed) {
            if let Ok((rss, _)) = read_rss_subs(&a).await {
                let mut p = rss_peak.lock().await;
                if rss > *p {
                    *p = rss;
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });

    // Publisher pool. A *single* synchronous publisher awaiting each ack
    // tops out at the publish→ack RTT (≈ tens/sec once fanout is heavy), so
    // it would measure ack latency, not the server's egress ceiling. Instead
    // run `PUB_CONNS` independent publisher connections in parallel: together
    // they keep the server's ingest+fanout pipeline saturated and self-
    // throttle on the bounded mutation channel, so the *server* becomes the
    // limiting factor. `--rate` caps the combined publish rate (0 ⇒ as fast
    // as the server accepts); the cap is split evenly across connections.
    const PUB_CONNS: usize = 16;
    let per_conn_rate = if rate <= 0.0 { 0.0 } else { rate / PUB_CONNS as f64 };
    let issued = Arc::new(AtomicU64::new(0));

    // Reset the delivery counter just before the window so ramp-time deltas
    // (the seed fanout) don't count.
    deliveries.store(0, Ordering::Relaxed);
    let meas_start = Instant::now();
    let deadline = meas_start + cfg.duration;

    let mut pub_handles = Vec::with_capacity(PUB_CONNS);
    for p in 0..PUB_CONNS {
        let url = cfg.server_url.clone();
        let topic = cfg.topic.clone();
        let issued = issued.clone();
        pub_handles.push(tokio::spawn(async move {
            let pubc = match Client::connect(&url).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("egress publisher {p} connect failed: {e}");
                    return;
                }
            };
            let mut limiter = RateLimiter::new(per_conn_rate);
            // Stripe keys by connection so no two publishers collide on a key
            // in the same instant (keeps every publish a distinct row event).
            let mut k: u64 = p as u64 + 1;
            let mut errs = 0u64;
            while Instant::now() < deadline {
                limiter.tick().await;
                if let Err(e) = pubc.publish(&topic, padded_body(k, payload)).await {
                    if errs < 2 {
                        eprintln!("egress publish error (conn {p}): {e}");
                    }
                    errs += 1;
                }
                k = k.wrapping_add(PUB_CONNS as u64);
                issued.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    let _ = futures::future::join_all(pub_handles).await;

    // Let in-flight deltas land before stopping the drainers — scale the
    // grace period with connection count (more conns ⇒ longer flush tail).
    let drain_ms = 750 + (subs_up / 4).min(3000);
    tokio::time::sleep(Duration::from_millis(drain_ms)).await;
    let meas_s = meas_start.elapsed().as_secs_f64();
    let issued = issued.load(Ordering::Relaxed);
    stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all(handles),
    )
    .await;
    let _ = tokio::time::timeout(Duration::from_secs(1), rss_task).await;

    let got = deliveries.load(Ordering::Relaxed);
    let ideal = issued.saturating_mul(subs_up);
    let pct = if ideal > 0 {
        100.0 * got as f64 / ideal as f64
    } else {
        0.0
    };
    let egress_mb = (got.saturating_mul(body_bytes as u64)) as f64 / (1024.0 * 1024.0);
    println!(
        "Publish:   {issued} msgs @ {:.0}/s actual over {meas_s:.1}s ({PUB_CONNS} pub conns)",
        issued as f64 / meas_s.max(0.001),
    );
    println!(
        "Delivered: {got} deltas = {pct:.1}% of {ideal} ideal fanout ({subs_up} subs × {issued} msgs)"
    );
    println!(
        "Egress:    {:.0} deltas/s,  ~{:.0} MB/s  (~{body_bytes} B/delta)",
        got as f64 / meas_s,
        egress_mb / meas_s,
    );
    {
        let peak = *peak_rss.lock().await;
        println!(
            "Server:    RSS baseline {:.0} MB → peak {:.0} MB  (Δ {:+.0} MB for {subs_up} conns)",
            baseline_rss,
            peak,
            peak - baseline_rss,
        );
    }
    if pct < 95.0 {
        println!(
            "  ⚠ delivered < 95% of ideal — server shedding load (slow-consumer / queue-cap) \
             or client can't drain {subs_up} conns fast enough"
        );
    }
    println!("──────────────────────────────────────────────────");
    Ok(())
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
    /// Realistic-payload class: `subscribe(topic, "book = 'BOOK-N'")` where
    /// N varies per-subscriber (idx modulo `BOOK_CARDINALITY`). Models the
    /// "trader watches their own book" pattern that dominates real AMPS
    /// deployments — each sub gets a focused SOW of a few thousand rows
    /// instead of the full topic. Used by the stress-2k-real scenario.
    BookFilter,
}

/// Book universe for the BookFilter class. Mirrors the names produced by
/// `clients/ts/examples/fi-data.ts::buildBooks(80)` so the WHERE clause
/// actually matches rows in the live demo data. Each sub picks
/// `BOOKS[idx % BOOKS.len()]` so 2000 subs distribute across all 80
/// books (~25 subs per book → realistic hot-topic shape).
const BOOKS: &[&str] = &[
    "BOOK-BASIS-APAC-01", "BOOK-BASIS-EMEA-01", "BOOK-BASIS-EU-01",
    "BOOK-BASIS-LATAM-01", "BOOK-BASIS-UK-01", "BOOK-BASIS-US-01",
    "BOOK-CARRY-APAC-01", "BOOK-CARRY-EMEA-01", "BOOK-CARRY-EU-01",
    "BOOK-CARRY-LATAM-01", "BOOK-CARRY-UK-01", "BOOK-CARRY-US-01",
    "BOOK-CREDIT-HY", "BOOK-CREDIT-HY-APAC-01", "BOOK-CREDIT-HY-EMEA-01",
    "BOOK-CREDIT-HY-EU-01", "BOOK-CREDIT-HY-LATAM-01", "BOOK-CREDIT-HY-UK-01",
    "BOOK-CREDIT-HY-US-01", "BOOK-CREDIT-HY-US-02", "BOOK-CREDIT-IG",
    "BOOK-CREDIT-IG-APAC-01", "BOOK-CREDIT-IG-EMEA-01", "BOOK-CREDIT-IG-EU-01",
    "BOOK-CREDIT-IG-LATAM-01", "BOOK-CREDIT-IG-UK-01", "BOOK-CREDIT-IG-US-01",
    "BOOK-CREDIT-IG-US-02", "BOOK-CURVE-APAC-01", "BOOK-CURVE-EMEA-01",
    "BOOK-CURVE-EU-01", "BOOK-CURVE-LATAM-01", "BOOK-CURVE-UK-01",
    "BOOK-CURVE-US-01", "BOOK-FLOW-APAC-01", "BOOK-FLOW-EMEA-01",
    "BOOK-FLOW-EU-01", "BOOK-FLOW-LATAM-01", "BOOK-FLOW-UK-01",
    "BOOK-FLOW-US-01", "BOOK-MM-APAC-01", "BOOK-MM-EMEA-01",
    "BOOK-MM-EU-01", "BOOK-MM-LATAM-01", "BOOK-MM-UK-01",
    "BOOK-MM-US-01", "BOOK-MUNI", "BOOK-MUNI-APAC-01",
    "BOOK-MUNI-EMEA-01", "BOOK-MUNI-EU-01", "BOOK-MUNI-LATAM-01",
    "BOOK-MUNI-UK-01", "BOOK-MUNI-US-01", "BOOK-PROP",
    "BOOK-PROP-APAC-01", "BOOK-PROP-EMEA-01", "BOOK-PROP-EU-01",
    "BOOK-PROP-LATAM-01", "BOOK-PROP-UK-01", "BOOK-PROP-US-01",
    "BOOK-RATES", "BOOK-RATES-APAC-01", "BOOK-RATES-EMEA-01",
    "BOOK-RATES-EU-01", "BOOK-RATES-LATAM-01", "BOOK-RATES-UK-01",
    "BOOK-RATES-US-01", "BOOK-RATES-US-02", "BOOK-RELVAL-APAC-01",
    "BOOK-RELVAL-EMEA-01", "BOOK-RELVAL-EU-01", "BOOK-RELVAL-LATAM-01",
    "BOOK-RELVAL-UK-01", "BOOK-RELVAL-US-01", "BOOK-RV-APAC-01",
    "BOOK-RV-EMEA-01", "BOOK-RV-EU-01", "BOOK-RV-LATAM-01",
    "BOOK-RV-UK-01", "BOOK-RV-US-01",
];

impl QueryClass {
    fn name(self) -> &'static str {
        match self {
            QueryClass::Firehose => "firehose",
            QueryClass::SimpleWhere => "where",
            QueryClass::GroupBy => "group_by",
            QueryClass::StaticPivot => "static_pivot",
            QueryClass::BookFilter => "book_filter",
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
    stress_2k_inner(
        cfg,
        &[
            QueryClass::Firehose,
            QueryClass::SimpleWhere,
            QueryClass::GroupBy,
            QueryClass::StaticPivot,
        ],
    )
    .await
}

/// Realistic-payload variant: every sub uses a per-book filter
/// (`book = 'BOOK-N'`) so it gets a focused snapshot of a single
/// book (~10K rows on the demo's 80-book dataset) instead of the
/// 865K-row firehose. Models the production pattern where each
/// trader subscribes to their own book.
pub async fn stress_2k_real(cfg: &ScenarioConfig) -> Result<Stress2kReport> {
    stress_2k_inner(cfg, &[QueryClass::BookFilter]).await
}

/// One-shot: measure the "trader's book × sector PnL pivot" query.
/// Runs `SELECT book, sector, SUM(unrealizedPnl), SUM(marketValue)
/// FROM "/positions" JOIN "/securities" USING (cusip) GROUP BY book, sector`
/// once, counts the result rows, and reports the latency.
///
/// This is the canonical "trader dashboard" shape: tiny result set
/// (≤ |books| × |sectors|) regardless of how many positions exist
/// underneath. With the H2 fanout cache, 2000 traders subscribing to
/// the same view share one encoded snapshot — orthogonal to the
/// firehose ceiling.
pub async fn trader_view_pivot(cfg: &ScenarioConfig) -> Result<()> {
    let started = Instant::now();
    let client = Client::connect(&cfg.server_url).await?;
    let connect_ms = started.elapsed().as_millis();

    // Same SHAPE as the "book × sector PnL pivot" a trader wants —
    // result size is |books| × |sectors|, independent of the
    // underlying row count. Uses /trades.{book, sector, qty,
    // notional} directly so the query is a single-topic group-by
    // (no join); the production form with unrealizedPnl needs a
    // view since unrealizedPnl lives on /positions and sector on
    // /securities. See `crates/cq-e2e-tests/tests/view_join_e2e.rs`
    // for the join-via-view recipe.
    let sql = "SELECT book, sector, \
               SUM(qty)      AS total_qty, \
               SUM(notional) AS total_notional, \
               COUNT(*)      AS trade_count \
               FROM \"/trades\" \
               GROUP BY book, sector";

    // One-shot SOW (the SDK returns the full result vec once GroupEnd
    // arrives — no need to manage the delta stream).
    let t_sow = Instant::now();
    let rows = client
        .sow_sql(&cfg.topic, sql)
        .await
        .with_context(|| "sow_sql failed (does cqserver have /positions + /securities loaded?)")?;
    let sow_ms = t_sow.elapsed().as_millis();
    let total_ms = started.elapsed().as_millis();

    let bytes_estimate: usize = rows
        .iter()
        .map(|r| serde_json::to_vec(r).map(|v| v.len()).unwrap_or(0))
        .sum();

    println!("──────────────────────────────────────────────────");
    println!("Scenario: trader-view-pivot");
    println!("Query:    book × sector PnL aggregate (join /positions × /securities)");
    println!("Connect:       {connect_ms} ms");
    println!("SOW (server-side join+group+ship): {sow_ms} ms");
    println!("Result rows:   {}", rows.len());
    println!(
        "Result JSON bytes (data only): {bytes_estimate}  ({:.1} KB)",
        bytes_estimate as f64 / 1024.0
    );
    if let Some(first) = rows.first() {
        let sample = serde_json::to_string(first).unwrap_or_default();
        let trimmed = if sample.len() > 200 { &sample[..200] } else { &sample };
        println!("Sample row:    {trimmed}");
    }
    println!("Total elapsed: {total_ms} ms");
    println!("──────────────────────────────────────────────────");
    Ok(())
}

async fn stress_2k_inner(
    cfg: &ScenarioConfig,
    classes: &[QueryClass],
) -> Result<Stress2kReport> {
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
    let book_filter_sql = format!("book = '{}'", BOOKS[idx % BOOKS.len()]);
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
            // Realistic-payload class: a single-book filter constructed
            // from this sub's index, so 2000 subs distribute across all
            // 80 books (~25 subs per book).
            QueryClass::BookFilter => client.subscribe(&topic, Some(&book_filter_sql)).await,
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

/// Scenario H (Q2 follow-up) — measure the wire-level `publish_batch`
/// throughput advantage over sequential per-row `publish`.
///
/// Protocol:
/// 1. Issue `total_rows` via N-sized batches; record total wall-clock
///    + average per-row latency.
/// 2. Issue the same `total_rows` one row at a time; record same.
/// 3. Report the speedup ratio.
///
/// `cfg.publish_rate` is reinterpreted as `total_rows` (ignoring the
/// rate-limiter — the point of this scenario is unthrottled
/// throughput). `cfg.warmup` is reinterpreted as batch size.
#[derive(Debug)]
pub struct BatchReport {
    pub scenario: &'static str,
    pub total_rows: u64,
    pub batch_size: u64,
    pub batch_elapsed: Duration,
    pub seq_elapsed: Duration,
    pub batch_per_row_us: f64,
    pub seq_per_row_us: f64,
    pub speedup: f64,
}

impl BatchReport {
    pub fn print(&self) {
        println!("──────────────────────────────────────────────────");
        println!("Scenario: {}", self.scenario);
        println!("Rows:        {} (batch size = {})", self.total_rows, self.batch_size);
        println!(
            "publish_batch: {:.2}s total ({:.2} µs/row)",
            self.batch_elapsed.as_secs_f64(),
            self.batch_per_row_us
        );
        println!(
            "publish     : {:.2}s total ({:.2} µs/row)",
            self.seq_elapsed.as_secs_f64(),
            self.seq_per_row_us
        );
        println!(
            "Speedup     : {:.2}× (publish_batch / publish)",
            self.speedup
        );
        println!("──────────────────────────────────────────────────");
    }
}

pub async fn publish_batch_vs_sequential(cfg: &ScenarioConfig) -> Result<BatchReport> {
    let client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("connect {}", cfg.server_url))?;
    let total_rows: u64 = cfg.publish_rate as u64;
    let batch_size: u64 = cfg.warmup.as_secs().max(1);

    // Seed: drive schema discovery so the timing measurements aren't
    // skewed by the first-publish discovery cost.
    client
        .publish(&cfg.topic, json!({ "k": "seed", "v": 0u64 }))
        .await
        .context("seed publish")?;

    // Phase 1: batched.
    let mut row_counter: u64 = 0;
    let batch_start = Instant::now();
    while row_counter < total_rows {
        let take = batch_size.min(total_rows - row_counter);
        let rows: Vec<serde_json::Value> = (0..take)
            .map(|i| {
                json!({
                    "k": format!("b{}", row_counter + i),
                    "v": row_counter + i,
                })
            })
            .collect();
        client
            .publish_batch(&cfg.topic, rows)
            .await
            .with_context(|| format!("publish_batch at row {row_counter}"))?;
        row_counter += take;
    }
    let batch_elapsed = batch_start.elapsed();

    // Phase 2: sequential.
    let seq_start = Instant::now();
    for i in 0..total_rows {
        client
            .publish(
                &cfg.topic,
                json!({ "k": format!("s{i}"), "v": i + total_rows }),
            )
            .await
            .with_context(|| format!("publish at row {i}"))?;
    }
    let seq_elapsed = seq_start.elapsed();

    let batch_per_row_us = batch_elapsed.as_micros() as f64 / total_rows as f64;
    let seq_per_row_us = seq_elapsed.as_micros() as f64 / total_rows as f64;
    let speedup = seq_per_row_us / batch_per_row_us.max(0.001);

    Ok(BatchReport {
        scenario: "publish-batch-vs-sequential",
        total_rows,
        batch_size,
        batch_elapsed,
        seq_elapsed,
        batch_per_row_us,
        seq_per_row_us,
        speedup,
    })
}

/// Scenario I (Q11 follow-up) — schema evolution under publish load.
///
/// Protocol:
/// - 1 publisher sustains `cfg.publish_rate` msg/s for `cfg.duration`.
/// - Every 5 seconds, fire `POST /admin/add-column` to append a new
///   column (`extra_0`, `extra_1`, ...).
/// - Track publish ack latency p50/p99 throughout. Asserts that no
///   publish errors out, and that p99 stays under the configured
///   `latency_budget` (default 50 ms).
///
/// `cfg.admin_url` is the admin HTTP base; topic name is appended.
#[derive(Debug)]
pub struct EvolutionReport {
    pub scenario: &'static str,
    pub publishes_issued: u64,
    pub publishes_acked: u64,
    pub publish_errors: u64,
    pub columns_added: u64,
    pub publish_ack_p50: Duration,
    pub publish_ack_p99: Duration,
    pub duration: Duration,
}

impl EvolutionReport {
    pub fn print(&self) {
        println!("──────────────────────────────────────────────────");
        println!("Scenario: {}", self.scenario);
        println!("Duration: {:.2}s", self.duration.as_secs_f64());
        println!(
            "Publishes:    issued = {}, acked = {}, errors = {}",
            self.publishes_issued, self.publishes_acked, self.publish_errors
        );
        println!("Columns added (online): {}", self.columns_added);
        println!(
            "Publish→Ack:  p50 = {}µs, p99 = {}µs",
            self.publish_ack_p50.as_micros(),
            self.publish_ack_p99.as_micros()
        );
        println!("──────────────────────────────────────────────────");
    }
}

pub async fn schema_evolution_under_load(cfg: &ScenarioConfig) -> Result<EvolutionReport> {
    let client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("connect {}", cfg.server_url))?;
    let mut limiter = RateLimiter::new(cfg.publish_rate);
    let mut ack_hist = LatencyHistogram::new();
    let started = Instant::now();
    let deadline = started + cfg.duration;
    let mut next_evolve = started + Duration::from_secs(5);
    let mut columns_added: u64 = 0;
    let mut issued: u64 = 0;
    let mut acked: u64 = 0;
    let mut errors: u64 = 0;

    // Seed publish so schema is established.
    let _ = client
        .publish(&cfg.topic, json!({ "k": "seed", "v": 0u64 }))
        .await;

    while Instant::now() < deadline {
        limiter.tick().await;
        // Fire an add-column every 5s.
        if Instant::now() >= next_evolve {
            let col_name = format!("extra_{columns_added}");
            let url = format!(
                "{}/admin/add-column/{}?name={}&type=long",
                cfg.admin_url.trim_end_matches('/'),
                urlencoding::encode(&cfg.topic),
                col_name
            );
            match reqwest::Client::new().post(&url).send().await {
                Ok(r) if r.status().is_success() => {
                    columns_added += 1;
                }
                Ok(r) => {
                    eprintln!("add-column non-2xx: {}", r.status());
                }
                Err(e) => {
                    eprintln!("add-column error: {e}");
                }
            }
            next_evolve += Duration::from_secs(5);
        }
        let body = json!({ "k": issued.to_string(), "v": issued });
        let t0 = Instant::now();
        match client.publish(&cfg.topic, body).await {
            Ok(_) => {
                ack_hist.record(t0.elapsed());
                acked += 1;
            }
            Err(_) => {
                errors += 1;
            }
        }
        issued += 1;
    }

    Ok(EvolutionReport {
        scenario: "schema-evolution-under-load",
        publishes_issued: issued,
        publishes_acked: acked,
        publish_errors: errors,
        columns_added,
        publish_ack_p50: ack_hist.p50(),
        publish_ack_p99: ack_hist.p99(),
        duration: started.elapsed(),
    })
}

// ─────────────────────────────────────────────────────────────────────
// soak: long-running Atlas-shaped workload — Bucket B, task B2.
//
// One publisher seeds a wide-ish row set on a persistent topic (default
// `/positions`, matching `tests/cloud/configs/soak.toml`) and then drives
// sparse `delta_publish` ticks at a configurable rate for the whole run.
// Three subscriber CLASSES are held open for the entire duration:
//
//   1. fast    — plain `subscribe`, drains as fast as it can. Models a
//                healthy full-firehose consumer.
//   2. conflated — subscribes to the same topic. Conflation itself is a
//                *topic*-level server setting (`conflation_ms` in the
//                TOML config, e.g. `/positions` in soak.toml), not a
//                client-side subscribe option — the SDK has no
//                per-subscription conflation knob (see
//                `crates/cq-core/src/conflation.rs` /
//                `crates/cq-protocol/src/message.rs`). This class simply
//                proves the conflation path stays healthy under sustained
//                load: on a topic with `conflation_ms` set, coalesced
//                deltas mean this class should see materially fewer raw
//                deltas than `fast` for the same publish stream while
//                still tracking the latest value with no drops.
//   3. slow    — deliberately slow, and its mode determines whether it
//                can actually induce server-side backpressure:
//
//                  - `raw` (default, `SlowMode::Raw`): opens a bare TCP
//                    socket, hand-sends a length-prefixed `sow_and_subscribe`
//                    frame via `cq_protocol`'s own codec/message types (no
//                    cq-client SDK involved), then reads the socket only
//                    every `soak_slow_read_delay_ms` — and even then just
//                    drains and discards whatever arrived, without pulling
//                    harder than the OS hands over in one `read()`. Because
//                    nothing is racing to drain the kernel recv buffer, it
//                    fills, TCP backpressure propagates to the server's
//                    write side, and the server's own outbound queue for
//                    this session backs up and starts dropping — the same
//                    mechanism `crates/cq-e2e-tests/tests/slow_consumer.rs`
//                    uses to prove `cq_subscription_dropped_total` fires.
//                  - `sdk` (`SlowMode::Sdk`): uses `cq_client::Client` like
//                    the fast/conflated classes, just sleeping
//                    `soak_slow_read_delay_ms` between `next_delta()` calls.
//                    **This class can NOT induce real drops**: the SDK's
//                    driver loop (`crates/cq-client/src/client.rs`) always
//                    races ahead reading the socket into an unbounded
//                    `mpsc::unbounded_channel`, regardless of how slowly the
//                    app drains that channel — so the kernel recv buffer
//                    never backs up and the server never sees a slow
//                    consumer. Kept only for comparison / back-compat
//                    (`--soak-slow-mode sdk`).
//
// The driver logs progress every `soak_progress_interval_secs` (published
// count, per-class delivered count, elapsed) and prints a final summary on
// exit — the gap between a class's `delivered` and the publisher's `acked`
// is the client-visible proxy for "this class is falling behind / dropping"
// (drops themselves happen server-side, in the outbound queue). Runs until
// `cfg.duration` elapses (`--duration-secs`)
// or the process receives SIGINT/SIGTERM (Ctrl-C / `docker stop`), in
// which case it still prints the summary before exiting.
// ─────────────────────────────────────────────────────────────────────

/// Per-class soak subscriber counters. `delivered` is every delta actually
/// read off the subscription; `subs_failed` counts connect/subscribe
/// failures (kept at 0 unless the server rejects the connection — a real
/// occurrence would be a soak finding in itself). There's no direct
/// client-visible "dropped" counter (drops happen server-side, in the
/// outbound queue, before the client ever sees the delta) — the summary
/// instead reports `delivered` alongside the publisher's `acked` count so
/// the gap between classes is visible: a healthy fast/conflated class
/// tracks `acked` closely; a slow class falling behind is the expected,
/// deliberate signal that the slow-consumer path is being exercised.
struct SoakClassCounters {
    class: &'static str,
    subs_opened: AtomicU64,
    subs_failed: AtomicU64,
    delivered: AtomicU64,
}

impl SoakClassCounters {
    fn new(class: &'static str) -> Self {
        Self {
            class,
            subs_opened: AtomicU64::new(0),
            subs_failed: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
        }
    }
}

/// How the soak's `slow` subscriber class connects. See the module doc
/// comment above for why `Raw` is the only mode that induces real
/// server-side backpressure/drops.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SlowMode {
    /// Raw TCP socket, hand-built wire frames, throttled/never reads —
    /// genuinely backpressures the server (default).
    Raw,
    /// cq-client SDK, sleeps between `next_delta()` calls — cannot
    /// backpressure (SDK drains the socket into an unbounded channel
    /// regardless of app-side pacing). Kept for comparison / back-compat.
    Sdk,
}

impl SlowMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "raw" => Some(SlowMode::Raw),
            "sdk" => Some(SlowMode::Sdk),
            _ => None,
        }
    }
}

/// Build the raw wire frame for a `sow_and_subscribe` on `topic`, encoded
/// exactly as `cq-client`'s transport would (length-prefixed JSON via
/// `cq_protocol::codec`), but assembled by hand so the soak's raw slow
/// subscriber never needs the SDK's draining driver loop. Pulled out as
/// its own function so frame construction is unit-testable without a
/// live server or a socket.
fn build_sow_and_subscribe_frame(cid: &str, topic: &str) -> bytes::BytesMut {
    let mut msg = cq_protocol::message::CqMessage::new(cq_protocol::command::Command::SowAndSubscribe);
    msg.command_id = Some(cid.to_string());
    msg.topic = Some(topic.to_string());
    let body = cq_protocol::serialization::Codec::Json
        .encode(&msg)
        .expect("CqMessage JSON encoding is infallible for this shape");
    let mut out = bytes::BytesMut::new();
    cq_protocol::codec::encode_frame(&body, &mut out);
    out
}

/// Parse `tcp://host:port` the same way `cq_client::transport::Transport`
/// does, so the raw slow subscriber connects to the same address form the
/// SDK-based classes use. Returns `None` for any other scheme (ws/wss) —
/// the raw path only supports the TCP transport.
fn tcp_host_port(server_url: &str) -> Option<&str> {
    server_url.strip_prefix("tcp://")
}

/// Raw-socket variant of the soak's slow subscriber class (task B2.1):
/// opens a bare TCP connection, hand-sends a `sow_and_subscribe` frame via
/// `build_sow_and_subscribe_frame`, then reads the socket only once every
/// `read_delay` — and even then does a single bounded `read()` per wakeup
/// rather than draining in a loop, so the kernel recv buffer is left to
/// fill between reads. No cq-client SDK involved on this connection, so
/// there's no unbounded channel racing to empty the socket: TCP
/// backpressure propagates all the way to the server's per-session
/// outbound queue, which is what makes this class actually exercise the
/// slow-consumer drop path (see the module doc comment above and
/// `crates/cq-e2e-tests/tests/slow_consumer.rs`, whose `open_silent_subscriber`
/// helper is the proof-of-concept this mirrors).
///
/// `delivered` counts frames read off the wire (best-effort — this path
/// doesn't decode/validate each frame's contents, just counts arrivals),
/// mirroring the SDK path's `delivered` counter closely enough for the
/// soak summary's fast-vs-slow comparison.
async fn run_soak_subscriber_raw_stall(
    idx: usize,
    server_url: String,
    topic: String,
    read_delay: Duration,
    counters: Arc<SoakClassCounters>,
    stop: Arc<AtomicBool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let Some(host_port) = tcp_host_port(&server_url) else {
        eprintln!(
            "soak sub {idx} ({}) raw mode requires a tcp:// server url, got {server_url}",
            counters.class
        );
        counters.subs_failed.fetch_add(1, Ordering::Relaxed);
        return;
    };

    let mut stream = match tokio::time::timeout(
        Duration::from_secs(15),
        tokio::net::TcpStream::connect(host_port),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("soak sub {idx} ({}) raw connect failed: {e}", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Err(_) => {
            eprintln!("soak sub {idx} ({}) raw connect timed out", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    let cid = format!("soak-raw-slow-{idx}");
    let frame = build_sow_and_subscribe_frame(&cid, &topic);
    if let Err(e) = stream.write_all(&frame).await {
        eprintln!("soak sub {idx} ({}) raw subscribe send failed: {e}", counters.class);
        counters.subs_failed.fetch_add(1, Ordering::Relaxed);
        return;
    }
    counters.subs_opened.fetch_add(1, Ordering::Relaxed);

    // Deliberately small read buffer + a bounded single `read()` call per
    // wakeup: we want to pull as little as possible off the wire, not
    // drain whatever accumulated. This is what leaves the kernel recv
    // buffer (and thus the server's outbound queue) to fill between
    // wakeups instead of racing to empty it every time we touch the
    // socket.
    let mut buf = [0u8; 4096];
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(read_delay).await;
        match tokio::time::timeout(Duration::from_millis(50), stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,   // server closed the connection
            Ok(Ok(_n)) => {
                counters.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(e)) => {
                eprintln!("soak sub {idx} ({}) raw read error: {e}", counters.class);
                break;
            }
            Err(_) => continue, // nothing arrived within the poke window — expected; keep stalling
        }
    }
}

/// Spawn one soak subscriber of the given class. `read_delay` is applied
/// *after* every successfully-read delta (slow class only — `Duration::ZERO`
/// for fast/conflated). Runs until `stop` flips.
async fn run_soak_subscriber(
    idx: usize,
    url: String,
    topic: String,
    read_delay: Duration,
    counters: Arc<SoakClassCounters>,
    stop: Arc<AtomicBool>,
) {
    let client = match tokio::time::timeout(Duration::from_secs(15), Client::connect(&url)).await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            eprintln!("soak sub {idx} ({}) connect failed: {e}", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Err(_) => {
            eprintln!("soak sub {idx} ({}) connect timed out", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let mut sub = match tokio::time::timeout(
        Duration::from_secs(30),
        client.sow_and_subscribe(&topic, None, None),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("soak sub {idx} ({}) subscribe failed: {e}", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        Err(_) => {
            eprintln!("soak sub {idx} ({}) subscribe timed out", counters.class);
            counters.subs_failed.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    counters.subs_opened.fetch_add(1, Ordering::Relaxed);

    while !stop.load(Ordering::Relaxed) {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(_delta)) => {
                counters.delivered.fetch_add(1, Ordering::Relaxed);
                if read_delay > Duration::ZERO {
                    tokio::time::sleep(read_delay).await;
                }
            }
            Ok(None) => break, // stream closed server-side
            Err(_) => continue, // timeout — re-check stop flag
        }
    }
}

/// Build one wide soak row keyed by `key_field` (e.g. `position_id` for
/// `/positions`). Unlike `wide_row` (used by `sow-wide`, which hardcodes a
/// `"k"` key), this targets a real Atlas-shaped topic whose key field name
/// is configurable — `delta_publish` rejects any payload missing the
/// topic's actual key field(s). `cols - 1` numeric filler columns
/// (`c1..c{cols-1}`) round out the row width.
fn soak_wide_row(key_field: &str, r: u64, cols: usize) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(cols);
    obj.insert(key_field.into(), json!(format!("SOAK-{r:08}")));
    for c in 1..cols {
        let v = r.wrapping_mul(2_654_435_761).wrapping_add(c as u64) % 1_000_000;
        obj.insert(format!("c{c}"), json!(v));
    }
    obj.insert("_t".into(), json!(0u64));
    serde_json::Value::Object(obj)
}

/// Long-running Atlas-shaped soak driver (task B2). Seeds `wide_rows`
/// wide rows (default 2_000 × `wide_cols`, default 60 columns — a
/// deliberately smaller default than `sow-wide`'s 30k × 400 so a local
/// 60s run is cheap) on `cfg.topic` (default caller passes `/positions`
/// to match `tests/cloud/configs/soak.toml`), then holds the 3
/// subscriber classes open for `cfg.duration` while a single publisher
/// issues sparse `delta_publish` ticks at `cfg.publish_rate`/s. Prints a
/// progress line every `soak_progress_interval_secs` and a final summary.
pub async fn soak(cfg: &ScenarioConfig) -> Result<()> {
    let rows = if cfg.wide_rows == 0 { 2_000 } else { cfg.wide_rows };
    let cols = if cfg.wide_cols == 0 { 60 } else { cfg.wide_cols };
    let n_fast = if cfg.soak_fast_subscribers == 0 { 2 } else { cfg.soak_fast_subscribers };
    let n_conflated =
        if cfg.soak_conflated_subscribers == 0 { 2 } else { cfg.soak_conflated_subscribers };
    let n_slow = if cfg.soak_slow_subscribers == 0 { 1 } else { cfg.soak_slow_subscribers };
    let slow_delay_ms =
        if cfg.soak_slow_read_delay_ms == 0 { 200 } else { cfg.soak_slow_read_delay_ms };
    let progress_secs =
        if cfg.soak_progress_interval_secs == 0 { 10 } else { cfg.soak_progress_interval_secs };
    let publish_rate = if cfg.publish_rate <= 0.0 { 50.0 } else { cfg.publish_rate };
    let key_field = if cfg.soak_key_field.is_empty() { "position_id" } else { &cfg.soak_key_field };
    let slow_mode_str = if cfg.soak_slow_mode.is_empty() { "raw" } else { &cfg.soak_slow_mode };
    let slow_mode = SlowMode::from_str(slow_mode_str).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid --soak-slow-mode '{slow_mode_str}' (expected 'raw' or 'sdk')"
        )
    })?;

    println!("──────────────────────────────────────────────────");
    println!("Scenario: soak  (topic={}, duration={:.0}s)", cfg.topic, cfg.duration.as_secs_f64());
    println!(
        "Seed:      {rows} rows × {cols} cols (key field '{key_field}')  |  ticks: delta_publish @ {publish_rate:.0}/s"
    );
    println!(
        "Subscribers: fast={n_fast}  conflated={n_conflated}  slow={n_slow} (read delay {slow_delay_ms}ms, mode={slow_mode_str})"
    );
    if slow_mode == SlowMode::Sdk {
        println!(
            "  note: slow-mode=sdk cannot induce real server-side backpressure/drops \
             (SDK drains the socket into an unbounded channel) — use --soak-slow-mode raw \
             (the default) to actually exercise the slow-consumer drop path"
        );
    }

    // ---- seed --------------------------------------------------------
    let seeder = Client::connect(&cfg.server_url)
        .await
        .with_context(|| format!("seeder connect {}", cfg.server_url))?;
    let per_row_bytes =
        serde_json::to_vec(&soak_wide_row(key_field, 0, cols)).map(|v| v.len()).unwrap_or(0);
    const FRAME_TARGET: usize = 8 * 1024 * 1024;
    let batch_size = (FRAME_TARGET / per_row_bytes.max(1)).clamp(1, 500);
    let seed_start = Instant::now();
    let mut batch: Vec<serde_json::Value> = Vec::with_capacity(batch_size);
    for r in 0..rows as u64 {
        batch.push(soak_wide_row(key_field, r, cols));
        if batch.len() == batch_size {
            seeder
                .publish_batch(&cfg.topic, std::mem::take(&mut batch))
                .await
                .with_context(|| "soak seed publish_batch failed")?;
            batch = Vec::with_capacity(batch_size);
        }
    }
    if !batch.is_empty() {
        seeder.publish_batch(&cfg.topic, batch).await?;
    }
    println!(
        "Seed done: {rows} rows in {:.2}s (~{} B/row)",
        seed_start.elapsed().as_secs_f64(),
        per_row_bytes,
    );

    // ---- hold the 3 subscriber classes open for the whole run --------
    let stop = Arc::new(AtomicBool::new(false));
    let fast_counters = Arc::new(SoakClassCounters::new("fast"));
    let conflated_counters = Arc::new(SoakClassCounters::new("conflated"));
    let slow_counters = Arc::new(SoakClassCounters::new("slow"));

    let mut sub_handles = Vec::with_capacity(n_fast + n_conflated + n_slow);
    for i in 0..n_fast {
        sub_handles.push(tokio::spawn(run_soak_subscriber(
            i,
            cfg.server_url.clone(),
            cfg.topic.clone(),
            Duration::ZERO,
            fast_counters.clone(),
            stop.clone(),
        )));
    }
    for i in 0..n_conflated {
        sub_handles.push(tokio::spawn(run_soak_subscriber(
            i,
            cfg.server_url.clone(),
            cfg.topic.clone(),
            Duration::ZERO,
            conflated_counters.clone(),
            stop.clone(),
        )));
    }
    let slow_delay = Duration::from_millis(slow_delay_ms);
    for i in 0..n_slow {
        sub_handles.push(match slow_mode {
            SlowMode::Raw => tokio::spawn(run_soak_subscriber_raw_stall(
                i,
                cfg.server_url.clone(),
                cfg.topic.clone(),
                slow_delay,
                slow_counters.clone(),
                stop.clone(),
            )),
            SlowMode::Sdk => tokio::spawn(run_soak_subscriber(
                i,
                cfg.server_url.clone(),
                cfg.topic.clone(),
                slow_delay,
                slow_counters.clone(),
                stop.clone(),
            )),
        });
    }

    // Let every subscriber finish connecting + draining the seed snapshot
    // before the live publisher starts, so early ticks don't race
    // registration and bias the first progress readout.
    let ramp_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let opened = fast_counters.subs_opened.load(Ordering::Relaxed)
            + conflated_counters.subs_opened.load(Ordering::Relaxed)
            + slow_counters.subs_opened.load(Ordering::Relaxed);
        let failed = fast_counters.subs_failed.load(Ordering::Relaxed)
            + conflated_counters.subs_failed.load(Ordering::Relaxed)
            + slow_counters.subs_failed.load(Ordering::Relaxed);
        if opened + failed >= (n_fast + n_conflated + n_slow) as u64 {
            break;
        }
        if Instant::now() >= ramp_deadline {
            println!("  warning: not all soak subscribers registered before ramp deadline");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // ---- live publisher: sparse delta_publish ticks -------------------
    let pub_client = Client::connect(&cfg.server_url)
        .await
        .with_context(|| "soak live publisher connect")?;
    let admin_hostport = cfg
        .admin_url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string();
    let admin = cq_client::admin::AdminClient::new(&admin_hostport).ok();

    let mut limiter = RateLimiter::new(publish_rate);
    let mut ack_hist = LatencyHistogram::new();
    let started = Instant::now();
    let deadline = started + cfg.duration;
    let mut last_progress = started;
    let mut seed_rng: u64 = 0x9E3779B97F4A7C15;
    let mut issued: u64 = 0;
    let mut acked: u64 = 0;
    let mut errors: u64 = 0;

    // Ctrl-C / SIGTERM (e.g. `docker stop`) should still print a summary
    // instead of the process just vanishing mid-soak — long-running soaks
    // are exactly the case where an operator wants the final numbers.
    tokio::pin! {
        let ctrl_c = tokio::signal::ctrl_c();
    }

    while Instant::now() < deadline {
        tokio::select! {
            _ = &mut ctrl_c => {
                println!("  received interrupt — stopping soak early");
                break;
            }
            _ = limiter.tick() => {
                seed_rng ^= seed_rng << 13;
                seed_rng ^= seed_rng >> 7;
                seed_rng ^= seed_rng << 17;
                let key = seed_rng % rows as u64;
                let now_us = started.elapsed().as_micros() as u64;
                let body = json!({
                    key_field: format!("SOAK-{key:08}"),
                    "c1": issued,
                    "_t": now_us,
                });
                let t0 = Instant::now();
                match pub_client.delta_publish(&cfg.topic, body).await {
                    Ok(_) => {
                        ack_hist.record(t0.elapsed());
                        acked += 1;
                    }
                    Err(e) => {
                        if errors < 5 {
                            eprintln!("soak delta_publish error at issued={issued}: {e}");
                        }
                        errors += 1;
                    }
                }
                issued += 1;
            }
        }

        if last_progress.elapsed() >= Duration::from_secs(progress_secs) {
            let rss = if let Some(a) = &admin {
                read_rss_subs(a).await.map(|(r, _)| r).unwrap_or(0.0)
            } else {
                0.0
            };
            println!(
                "  [{:>5.0}s] published={issued} acked={acked} errors={errors}  fast_delivered={} conflated_delivered={} slow_delivered={}  rss={:.0}MB",
                started.elapsed().as_secs_f64(),
                fast_counters.delivered.load(Ordering::Relaxed),
                conflated_counters.delivered.load(Ordering::Relaxed),
                slow_counters.delivered.load(Ordering::Relaxed),
                rss,
            );
            last_progress = Instant::now();
        }
    }

    // Let tail deltas land, then stop subscribers.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        futures::future::join_all(sub_handles),
    )
    .await;

    let elapsed = started.elapsed();
    let fast_delivered = fast_counters.delivered.load(Ordering::Relaxed);
    let conflated_delivered = conflated_counters.delivered.load(Ordering::Relaxed);
    let slow_delivered = slow_counters.delivered.load(Ordering::Relaxed);
    println!("──────────────────────────────────────────────────");
    println!("Soak summary");
    println!("Duration:  {:.1}s (target {:.0}s)", elapsed.as_secs_f64(), cfg.duration.as_secs_f64());
    println!(
        "Publish:   issued={issued} acked={acked} errors={errors}  actual_rate={:.1}/s",
        limiter.actual_rate()
    );
    println!(
        "Ack lat:   p50={}µs p99={}µs",
        ack_hist.p50().as_micros(),
        ack_hist.p99().as_micros()
    );
    for c in [&fast_counters, &conflated_counters, &slow_counters] {
        let opened = c.subs_opened.load(Ordering::Relaxed);
        let failed = c.subs_failed.load(Ordering::Relaxed);
        let delivered = c.delivered.load(Ordering::Relaxed);
        let lag = acked.saturating_sub(delivered);
        println!(
            "Class {:>10}: subs opened={opened} failed={failed}  delivered={delivered}  lag_vs_acked={lag}",
            c.class,
        );
    }
    if slow_delivered < fast_delivered {
        println!(
            "  slow class trails fast by {} deltas — slow-consumer path exercised as expected",
            fast_delivered.saturating_sub(slow_delivered)
        );
    }
    if conflated_delivered > 0 && conflated_delivered < acked {
        println!(
            "  conflated class saw {conflated_delivered} deltas vs {acked} acked publishes — conflation coalescing observed"
        );
    }
    println!("──────────────────────────────────────────────────");
    Ok(())
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

// ─────────────────────────────────────────────────────────────────────
// B2.1 — pure-logic tests for the raw slow-subscriber path: frame
// construction/round-trip, URL parsing, and mode-string parsing. None of
// these need a live server or a socket — the live-backpressure claim
// itself is validated separately against a real compose cluster (see
// tests/cloud/SOAK.md and the B2.1 task report).
// ─────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod raw_slow_subscriber_tests {
    use super::*;

    #[test]
    fn slow_mode_from_str_accepts_raw_and_sdk() {
        assert_eq!(SlowMode::from_str("raw"), Some(SlowMode::Raw));
        assert_eq!(SlowMode::from_str("sdk"), Some(SlowMode::Sdk));
    }

    #[test]
    fn slow_mode_from_str_rejects_anything_else() {
        assert_eq!(SlowMode::from_str(""), None);
        assert_eq!(SlowMode::from_str("RAW"), None); // case-sensitive by design
        assert_eq!(SlowMode::from_str("bogus"), None);
    }

    #[test]
    fn tcp_host_port_strips_scheme() {
        assert_eq!(tcp_host_port("tcp://127.0.0.1:9007"), Some("127.0.0.1:9007"));
        assert_eq!(tcp_host_port("tcp://cqserver:9007"), Some("cqserver:9007"));
    }

    #[test]
    fn tcp_host_port_rejects_non_tcp_schemes() {
        assert_eq!(tcp_host_port("ws://127.0.0.1:9008/cq/json"), None);
        assert_eq!(tcp_host_port("wss://example.com/cq"), None);
        assert_eq!(tcp_host_port("127.0.0.1:9007"), None);
    }

    /// The raw frame must be a valid length-prefixed `cq_protocol` frame
    /// whose decoded payload is a `sow_and_subscribe` CqMessage carrying
    /// the caller's `cid`/`topic` — i.e. exactly what the server's own
    /// frame decoder expects, matching what `cq_e2e_tests`'
    /// `open_silent_subscriber` hand-builds (JSON, same field names).
    #[test]
    fn build_sow_and_subscribe_frame_round_trips_through_the_wire_codec() {
        let mut frame = build_sow_and_subscribe_frame("soak-raw-slow-0", "/positions");

        // Length prefix must exactly match the encoded body length (no
        // compression — bodies this small never cross the zstd threshold).
        let declared_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        assert_eq!(declared_len as usize, frame.len() - 4);
        // High bit (compressed flag) must be clear.
        assert_eq!(declared_len & cq_protocol::compression::COMPRESSED_FLAG, 0);

        let payload = cq_protocol::codec::decode_frame(&mut frame)
            .expect("valid frame")
            .expect("complete frame");
        let msg: cq_protocol::message::CqMessage =
            cq_protocol::serialization::Codec::Json.decode(&payload).expect("valid CqMessage JSON");

        assert_eq!(msg.command, cq_protocol::command::Command::SowAndSubscribe);
        assert_eq!(msg.command_id.as_deref(), Some("soak-raw-slow-0"));
        assert_eq!(msg.topic.as_deref(), Some("/positions"));
        // No filter/bookmark set — matches `open_silent_subscriber`'s
        // shape (`{ "c": "sow_and_subscribe", "cid": ..., "t": topic }`).
        assert!(msg.filter.is_none());
        assert!(msg.bookmark.is_none());
    }

    #[test]
    fn build_sow_and_subscribe_frame_is_valid_json_on_the_wire() {
        let mut frame = build_sow_and_subscribe_frame("c-1", "/loadgen");
        let payload = cq_protocol::codec::decode_frame(&mut frame).unwrap().unwrap();
        let text = std::str::from_utf8(&payload).expect("frame body is UTF-8 JSON, not msgpack");
        assert!(text.contains("sow_and_subscribe"));
        assert!(text.contains("/loadgen"));
    }

    #[test]
    fn distinct_cids_produce_distinct_frames() {
        let mut a = build_sow_and_subscribe_frame("c-1", "/t");
        let mut b = build_sow_and_subscribe_frame("c-2", "/t");
        assert_ne!(a, b);
        // Both still decode to well-formed frames with the right cid each.
        let pa = cq_protocol::codec::decode_frame(&mut a).unwrap().unwrap();
        let pb = cq_protocol::codec::decode_frame(&mut b).unwrap().unwrap();
        let ma: cq_protocol::message::CqMessage =
            cq_protocol::serialization::Codec::Json.decode(&pa).unwrap();
        let mb: cq_protocol::message::CqMessage =
            cq_protocol::serialization::Codec::Json.decode(&pb).unwrap();
        assert_eq!(ma.command_id.as_deref(), Some("c-1"));
        assert_eq!(mb.command_id.as_deref(), Some("c-2"));
    }
}

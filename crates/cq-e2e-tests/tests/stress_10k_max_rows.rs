//! Stress sweep: how many rows can the server serve to 10,000
//! concurrent subscribers?
//!
//! Two patterns are tested at each row count:
//!
//!   - **Per-book filter** ("trader watches their own book"). Each
//!     sub gets a focused snapshot of ~N/80 rows. Server-side
//!     workload scales as `subs × (N/80)` for snapshot encoding
//!     (cache hits only when subs of the SAME book overlap in
//!     time, which is ~125 subs per book at 10k subs).
//!
//!   - **Firehose** (all subs get every row). Server-side workload
//!     is `1 × N` for encode (TH6 fanout cache means one shared
//!     encoded snapshot serves all 10k subs that arrive within the
//!     500ms cache TTL), plus `subs × N` for socket writes.
//!
//! Run with:
//!   cargo test --release -p cq-e2e-tests --test stress_10k_max_rows \
//!     -- --ignored --nocapture
//!
//! Sweep marker `MAX_ROWS` defaults to a level that completes in a
//! reasonable time on a dev box. Bump it up to push harder.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TARGET_SUBS: usize = 10_000;
const N_BOOKS: usize = 80;

#[derive(Clone, Copy)]
enum Pattern {
    PerBookFilter,
    Firehose,
}

impl Pattern {
    fn name(self) -> &'static str {
        match self {
            Pattern::PerBookFilter => "per-book filter",
            Pattern::Firehose => "firehose (no filter)",
        }
    }
}

struct SweepResult {
    rows: usize,
    pattern: &'static str,
    subs_target: usize,
    subs_opened: u64,
    subs_failed: u64,
    snapshot_completed: u64,
    snapshot_p50_ms: u64,
    snapshot_p99_ms: u64,
    ramp_secs: f64,
    rss_mb: u64,
}

impl SweepResult {
    fn print(&self) {
        eprintln!(
            "  rows={:>7}  pattern={:<20}  opened={:>5}/{:<5}  snap-ok={:>5}  ramp={:>5.1}s  p50={:>5}ms  p99={:>6}ms  rss={:>5}MB",
            self.rows,
            self.pattern,
            self.subs_opened,
            self.subs_target,
            self.snapshot_completed,
            self.ramp_secs,
            self.snapshot_p50_ms,
            self.snapshot_p99_ms,
            self.rss_mb,
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn sweep_rows_at_10k_subs() {
    // Sweep levels. Stop early if a level fails to complete.
    let row_levels: &[usize] = &[10_000, 50_000, 100_000, 500_000];

    eprintln!("══════════════════════════════════════════════════════════════════════");
    eprintln!("STRESS SWEEP: row count × 10,000 subscribers");
    eprintln!("══════════════════════════════════════════════════════════════════════");

    for &rows in row_levels {
        for pattern in [Pattern::PerBookFilter, Pattern::Firehose] {
            eprintln!(
                "\n▶ rows={rows}, pattern={}, subs={TARGET_SUBS}",
                pattern.name()
            );
            let result = run_one_sweep(rows, pattern, TARGET_SUBS).await;
            result.print();
            // Bail out if the snapshot completion rate drops below 80%
            // for any reason — server is at its limit.
            if result.snapshot_completed * 100 < (result.subs_opened * 80) {
                eprintln!(
                    "  ⚠️  snapshot completion {} / {} subs (<80%) — stopping sweep",
                    result.snapshot_completed, result.subs_opened
                );
                eprintln!("\nMax rows comfortably served at 10k subs:");
                eprintln!("  pattern={}  rows < {}", pattern.name(), rows);
                return;
            }
        }
    }
    eprintln!(
        "\n✅  Sweep completed all levels up to {} rows without failure.",
        row_levels.last().unwrap()
    );
}

async fn run_one_sweep(rows: usize, pattern: Pattern, target_subs: usize) -> SweepResult {
    let topic = TopicSpec::new("/sweep10k", "k").with_inline_columns([
        ("k", "string"),
        ("book", "string"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;

    // Seed `rows` rows distributed across N_BOOKS books.
    let publisher = Client::connect(&server.tcp_url())
        .await
        .expect("publisher connect");
    let rows_per_book = rows.div_ceil(N_BOOKS);
    let seeded = publish_seed_rows(&publisher, rows_per_book).await;
    eprintln!("  seeded {seeded} rows ({rows_per_book} per book × {N_BOOKS} books)");
    drop(publisher);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let opened = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let snapshot_completed = Arc::new(AtomicU64::new(0));
    let snapshot_durations_us = Arc::new(Mutex::new(Vec::with_capacity(target_subs)));

    const WAVE_SIZE: usize = 50;
    const WAVE_MS: u64 = 200;

    let ramp_started = Instant::now();
    let mut handles = Vec::with_capacity(target_subs);
    for i in 0..target_subs {
        let url = server.tcp_url();
        let book = format!("BOOK-{:03}", i % N_BOOKS);
        let opened_inner = opened.clone();
        let failed_inner = failed.clone();
        let snap_done = snapshot_completed.clone();
        let snap_durs = snapshot_durations_us.clone();
        let pattern_local = pattern;
        let h = tokio::spawn(async move {
            let connect_fut = Client::connect(&url);
            let client = match tokio::time::timeout(Duration::from_secs(30), connect_fut).await {
                Ok(Ok(c)) => c,
                _ => {
                    failed_inner.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            let t_sow = Instant::now();
            let sow_result = match pattern_local {
                Pattern::PerBookFilter => {
                    let filter = format!("book = '{book}'");
                    tokio::time::timeout(
                        Duration::from_secs(60),
                        client.sow("/sweep10k", Some(&filter)),
                    )
                    .await
                }
                Pattern::Firehose => {
                    tokio::time::timeout(
                        Duration::from_secs(120),
                        client.sow("/sweep10k", None),
                    )
                    .await
                }
            };
            match sow_result {
                Ok(Ok(_rows)) => {
                    snap_done.fetch_add(1, Ordering::Relaxed);
                    let elapsed = t_sow.elapsed().as_micros() as u64;
                    if let Ok(mut g) = snap_durs.lock() {
                        g.push(elapsed);
                    }
                }
                _ => {
                    // Connected but snapshot timed out / errored.
                }
            }
            opened_inner.fetch_add(1, Ordering::Relaxed);
        });
        handles.push(h);
        if (i + 1) % WAVE_SIZE == 0 {
            tokio::time::sleep(Duration::from_millis(WAVE_MS)).await;
        }
        if (i + 1) % 2000 == 0 {
            let o = opened.load(Ordering::Relaxed);
            let f = failed.load(Ordering::Relaxed);
            let s = snapshot_completed.load(Ordering::Relaxed);
            eprintln!(
                "  ramp: spawned={}/{target_subs}  opened={o}  failed={f}  snap-ok={s}  elapsed={:.1}s",
                i + 1,
                ramp_started.elapsed().as_secs_f64()
            );
        }
    }

    // Drain — wait for every spawned task to complete (or hit its
    // timeout), with a hard ceiling to keep the test bounded.
    let drain_deadline = Instant::now() + Duration::from_secs(180);
    for h in handles {
        let remaining = drain_deadline.saturating_duration_since(Instant::now());
        let _ = tokio::time::timeout(remaining, h).await;
    }
    let ramp_secs = ramp_started.elapsed().as_secs_f64();

    // Compute snapshot latency percentiles.
    let mut durs: Vec<u64> = snapshot_durations_us.lock().unwrap().clone();
    durs.sort_unstable();
    let p50 = pct(&durs, 50);
    let p99 = pct(&durs, 99);

    let stats: serde_json::Value = match reqwest::get(format!("{}/stats", server.admin_url())).await {
        Ok(r) => r.json().await.unwrap_or(serde_json::Value::Null),
        Err(_) => serde_json::Value::Null,
    };
    let rss_mb = stats
        .get("rssMb")
        .or_else(|| stats.get("rss_mb"))
        .or_else(|| stats.get("rss"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    SweepResult {
        rows,
        pattern: pattern.name(),
        subs_target: target_subs,
        subs_opened: opened.load(Ordering::Relaxed),
        subs_failed: failed.load(Ordering::Relaxed),
        snapshot_completed: snapshot_completed.load(Ordering::Relaxed),
        snapshot_p50_ms: p50 / 1000,
        snapshot_p99_ms: p99 / 1000,
        ramp_secs,
        rss_mb,
    }
}

async fn publish_seed_rows(client: &Client, rows_per_book: usize) -> usize {
    // Batch publishes for speed — 500 rows per batch.
    let mut total = 0_usize;
    for b in 0..N_BOOKS {
        let book = format!("BOOK-{b:03}");
        let mut batch: Vec<serde_json::Value> = Vec::with_capacity(500);
        for j in 0..rows_per_book {
            batch.push(json!({
                "k": format!("{book}_{j:06}"),
                "book": book,
                "qty": j as i64,
            }));
            if batch.len() == 500 {
                let _ = client.publish_batch("/sweep10k", std::mem::take(&mut batch)).await;
            }
        }
        if !batch.is_empty() {
            let _ = client.publish_batch("/sweep10k", batch).await;
        }
        total += rows_per_book;
    }
    total
}

fn pct(sorted: &[u64], p: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as u64 * p) / 100).min(sorted.len() as u64 - 1) as usize;
    sorted[idx]
}

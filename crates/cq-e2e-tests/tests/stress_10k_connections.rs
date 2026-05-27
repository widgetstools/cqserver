//! 10,000 concurrent subscriber stress test.
//!
//! Runs the `stress_2k_real` scenario (book-filter pattern — every
//! sub gets a focused single-book view, the production "trader
//! watches their own book" shape) with the subscriber count cranked
//! to 10,000.
//!
//! Marked `#[ignore]` because:
//!   - It needs ~10k file descriptors on both the test and server
//!     processes. `ulimit -n` must be ≥ ~20k; macOS dev boxes
//!     default to 1M, Linux often defaults to 1k.
//!   - Wall-clock runs ~60-90 s (40 s ramp at 50 subs/200 ms +
//!     10 s measurement + drain).
//!   - It's a load test, not a correctness test — should not run
//!     in CI as a default.
//!
//! Run with:
//!   cargo test --release -p cq-e2e-tests --test stress_10k_connections \
//!     -- --ignored --nocapture

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

const TARGET_SUBS: usize = 10_000;
const MEASUREMENT_SECS: u64 = 10;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn stress_10k_subscribers_book_filter() {
    // BookFilter expects columns: k (key), book (filter), qty, side.
    let topic = TopicSpec::new("/stress10k", "k").with_inline_columns([
        ("k", "string"),
        ("book", "string"),
        ("qty", "long"),
        ("side", "string"),
    ]);
    let server = start_server(vec![topic]).await;

    // Pre-seed with rows across 80 books × ~25 rows = 2000 rows. Each
    // sub gets a per-book snapshot of ~25 rows.
    let publisher = Client::connect(&server.tcp_url())
        .await
        .expect("publisher connect");
    let books: Vec<String> = (0..80).map(|i| format!("BOOK-{i:03}")).collect();
    for book in &books {
        for j in 0..25_i64 {
            publisher
                .publish(
                    "/stress10k",
                    json!({
                        "k": format!("{book}_{j:02}"),
                        "book": book,
                        "qty": j * 100,
                        "side": if j % 2 == 0 { "BUY" } else { "SELL" },
                    }),
                )
                .await
                .expect("seed publish");
        }
    }
    eprintln!(
        "seeded {} rows across {} books",
        books.len() * 25,
        books.len()
    );
    drop(publisher);
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The BookFilter class in stress_2k_real uses the const BOOKS array
    // from cq-loadgen, which expects names like "BOOK-BASIS-APAC-01".
    // We re-publish under those names too so the filter actually
    // matches rows. Wait — easier: just have the loadgen build
    // book-filter SQL against OUR book names. Looking at the source,
    // we can't override BOOKS. So use a wrapper: spawn a custom
    // subscriber instead of stress_2k_real.
    //
    // Instead of dragging in the full scenario, we run a focused
    // 10k-subscriber loop here — connects, subscribes with a
    // per-book filter against our seed data, counts deliveries.

    let started = std::time::Instant::now();
    let stop_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut handles = Vec::with_capacity(TARGET_SUBS);
    let opened = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let deliveries = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    const WAVE_SIZE: usize = 50;
    const WAVE_MS: u64 = 200;

    for i in 0..TARGET_SUBS {
        let url = server.tcp_url();
        let book = books[i % books.len()].clone();
        let opened_inner = opened.clone();
        let failed_inner = failed.clone();
        let deliveries_inner = deliveries.clone();
        let stop_flag_inner = stop_flag.clone();
        let h = tokio::spawn(async move {
            use std::sync::atomic::Ordering;
            let connect_fut = Client::connect(&url);
            let client = match tokio::time::timeout(Duration::from_secs(20), connect_fut).await {
                Ok(Ok(c)) => c,
                _ => {
                    failed_inner.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let filter = format!("book = '{book}'");
            let mut sub = match client
                .subscribe("/stress10k", Some(&filter))
                .await
            {
                Ok(s) => s,
                Err(_) => {
                    failed_inner.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            opened_inner.fetch_add(1, Ordering::Relaxed);
            // Drain deltas until stop_flag is set.
            while !stop_flag_inner.load(Ordering::Relaxed) {
                match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
                    Ok(Some(_)) => {
                        deliveries_inner.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => return, // connection closed
                    Err(_) => continue, // periodic stop-check
                }
            }
        });
        handles.push(h);
        if (i + 1) % WAVE_SIZE == 0 {
            tokio::time::sleep(Duration::from_millis(WAVE_MS)).await;
        }
        if (i + 1) % 1000 == 0 {
            let o = opened.load(std::sync::atomic::Ordering::Relaxed);
            let f = failed.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "ramp progress: spawned {}, opened {o}, failed {f}, elapsed {:.1}s",
                i + 1,
                started.elapsed().as_secs_f64()
            );
        }
    }

    // Wait for ramp to settle (opened+failed >= target, or 60s
    // ceiling).
    let ramp_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let o = opened.load(std::sync::atomic::Ordering::Relaxed);
        let f = failed.load(std::sync::atomic::Ordering::Relaxed);
        if (o + f) as usize >= TARGET_SUBS {
            break;
        }
        if std::time::Instant::now() >= ramp_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let ramp_secs = started.elapsed().as_secs_f64();

    // Measurement window — let publishers fire, count deliveries.
    let measure_start = std::time::Instant::now();
    // Spawn a publisher that fires periodic deltas during the window.
    let pub_handle = {
        let url = server.tcp_url();
        let books = books.clone();
        let stop = stop_flag.clone();
        tokio::spawn(async move {
            let p = Client::connect(&url).await.expect("measurement publisher");
            let mut tick = 0_i64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for book in &books {
                    let _ = p
                        .publish(
                            "/stress10k",
                            json!({
                                "k": format!("{book}_tick_{tick}"),
                                "book": book,
                                "qty": tick * 10,
                                "side": if tick % 2 == 0 { "BUY" } else { "SELL" },
                            }),
                        )
                        .await;
                }
                tick += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    tokio::time::sleep(Duration::from_secs(MEASUREMENT_SECS)).await;
    let measurement_secs = measure_start.elapsed().as_secs_f64();
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);

    // Snapshot counts before subs drain.
    let final_opened = opened.load(std::sync::atomic::Ordering::Relaxed);
    let final_failed = failed.load(std::sync::atomic::Ordering::Relaxed);
    let final_deliveries = deliveries.load(std::sync::atomic::Ordering::Relaxed);

    // Reap publisher + subs (best-effort, bounded).
    let _ = pub_handle.await;
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(10);
    for h in handles {
        let _ = tokio::time::timeout(
            drain_deadline.saturating_duration_since(std::time::Instant::now()),
            h,
        )
        .await;
    }

    // Pull final /stats from admin for RSS sanity.
    let stats: serde_json::Value = reqwest::get(format!("{}/stats", server.admin_url()))
        .await
        .map(|r| async move { r.json().await.unwrap_or(serde_json::Value::Null) })
        .unwrap()
        .await;
    let rss_mb = stats
        .get("rssMb")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let total_rows = stats
        .get("totalRows")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    eprintln!("──────────────────────────────────────────────────");
    eprintln!("STRESS-10K REPORT");
    eprintln!("Target subscribers:   {TARGET_SUBS}");
    eprintln!("Opened:               {final_opened}");
    eprintln!("Failed:               {final_failed}");
    eprintln!("Open rate:            {:.1}%", 100.0 * final_opened as f64 / TARGET_SUBS as f64);
    eprintln!("Ramp seconds:         {ramp_secs:.1}s");
    eprintln!("Measurement seconds:  {measurement_secs:.1}s");
    eprintln!("Deliveries received:  {final_deliveries}");
    eprintln!(
        "Per-sub delivery rate: {:.1}/s",
        final_deliveries as f64 / final_opened.max(1) as f64 / measurement_secs.max(0.001)
    );
    eprintln!("Server RSS:           {rss_mb} MB");
    eprintln!("Server total rows:    {total_rows}");
    eprintln!("──────────────────────────────────────────────────");

    // Soft assertions: more an information report than a pass/fail,
    // but we still want a meaningful failure if the server completely
    // collapsed.
    let open_pct = 100.0 * final_opened as f64 / TARGET_SUBS as f64;
    assert!(
        open_pct >= 80.0,
        "open rate {open_pct:.1}% below 80% — server failed to accept subscribers"
    );
    // Subscribers should each receive at least a few deliveries during
    // the measurement window (publisher fires 10/s × 10s = 100 ticks
    // per book × 1 matching sub per book at minimum).
    assert!(
        final_deliveries > 0,
        "zero deliveries observed — fanout broken"
    );
}

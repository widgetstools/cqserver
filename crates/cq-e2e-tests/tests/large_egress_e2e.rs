//! Large data-egress correctness + throughput.
//!
//! Seeds a sizeable SOW, then has many connections concurrently pull the
//! whole thing through a **deliberately small outbound queue**. This
//! exercises, at scale and under concurrency:
//!   - the SOW backpressure path (snapshot >> queue capacity must still
//!     complete intact — no silent drops/truncation),
//!   - encode-once-fanout (concurrent identical SOWs share one server-side
//!     snapshot encode),
//!   - caps-off default (a large result is streamed, never truncated).
//!
//! Every subscriber must receive EXACTLY the seeded row count. Aggregate
//! egress throughput is printed.
//!
//! Scale is env-configurable for heavier manual runs (use `--release`):
//!   CQ_EGRESS_ROWS=200000 CQ_EGRESS_SUBS=16 \
//!     cargo test --release -p cq-e2e-tests --test large_egress_e2e -- --nocapture

use std::time::{Duration, Instant};

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec};
use serde_json::{json, Map, Value};

const NVAL: usize = 10; // double columns
const NLONG: usize = 3; // long columns

fn egress_topic(name: &str) -> TopicSpec {
    let mut cols: Vec<(String, String)> = vec![
        ("k".into(), "string".into()),
        ("sym".into(), "string".into()),
    ];
    for v in 0..NVAL {
        cols.push((format!("v{v}"), "double".into()));
    }
    for q in 0..NLONG {
        cols.push((format!("q{q}"), "long".into()));
    }
    TopicSpec::new(name, "k").with_inline_columns(cols)
}

fn row(i: usize) -> Value {
    let mut m = Map::new();
    m.insert("k".into(), json!(format!("k{i:08}")));
    m.insert("sym".into(), json!(format!("S{:04}", i % 5000)));
    for v in 0..NVAL {
        m.insert(format!("v{v}"), json!(i as f64 * 1.0001 + v as f64));
    }
    for q in 0..NLONG {
        m.insert(format!("q{q}"), json!((i as i64) * (q as i64 + 1)));
    }
    Value::Object(m)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

/// Connect with a few retries — at thousands of simultaneous subscribers a
/// burst of SYNs can exceed the OS listen backlog and get refused; a short
/// backoff lets the accept loop drain.
async fn connect_retry(url: &str) -> Client {
    let mut last = None;
    for attempt in 0..6u32 {
        match Client::connect(url).await {
            Ok(c) => return c,
            Err(e) => {
                last = Some(format!("{e:?}"));
                tokio::time::sleep(Duration::from_millis(20 * u64::from(attempt + 1))).await;
            }
        }
    }
    panic!("connect failed after retries: {last:?}");
}

// Scales to thousands of subscribers (each connects in parallel below). At
// high connection counts shrink the per-connection payload, e.g.:
//   CQ_EGRESS_SUBS=5000 CQ_EGRESS_ROWS=200 cargo test --release \
//     -p cq-e2e-tests --test large_egress_e2e -- --nocapture
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn large_sow_egress_to_many_subscribers_is_complete() {
    let rows_n = env_usize("CQ_EGRESS_ROWS", 50_000);
    let subs_n = env_usize("CQ_EGRESS_SUBS", 10);

    let server = start_server_with(
        vec![egress_topic("/egress")],
        ServerOpts {
            // Tiny queue vs a huge snapshot: every subscriber's SOW must
            // complete through backpressure, never dropping rows.
            outbound_queue_capacity: 256,
            ..ServerOpts::default()
        },
    )
    .await;
    let url = server.tcp_url();

    // ── seed N rows via batched publishes ──
    let seed_t = Instant::now();
    {
        let pubc = Client::connect(&url).await.expect("connect publisher");
        const BATCH: usize = 1000;
        let mut i = 0usize;
        while i < rows_n {
            let end = (i + BATCH).min(rows_n);
            let batch: Vec<Value> = (i..end).map(row).collect();
            pubc.publish_batch("/egress", batch).await.expect("publish_batch");
            i = end;
        }
    }
    let seed_secs = seed_t.elapsed().as_secs_f64();

    // Approximate wire bytes per row (one serialized row).
    let row_bytes = serde_json::to_vec(&row(0)).unwrap().len();

    // ── fan out: `subs_n` connections each pull the full SOW concurrently ──
    let egress_t = Instant::now();
    let mut handles = Vec::with_capacity(subs_n);
    for s in 0..subs_n {
        let url = url.clone();
        handles.push(tokio::spawn(async move {
            let c = connect_retry(&url).await;
            let t = Instant::now();
            let rows = c.sow("/egress", None).await.expect("sow");
            (s, rows.len(), t.elapsed())
        }));
    }

    let mut max_one = Duration::ZERO;
    for h in handles {
        let (s, got, dt) = h.await.expect("join");
        assert_eq!(
            got, rows_n,
            "subscriber {s} received {got}/{rows_n} rows — large egress dropped/truncated"
        );
        max_one = max_one.max(dt);
    }
    let egress_secs = egress_t.elapsed().as_secs_f64();

    let total_rows = rows_n * subs_n;
    let total_bytes = (total_rows as u64) * (row_bytes as u64);
    eprintln!(
        "\nLarge egress: {rows_n} rows × {subs_n} subs = {total_rows} row-deliveries\n\
         seed:    {seed_secs:.2}s ({:.0} rows/s ingest)\n\
         egress:  {egress_secs:.2}s  (slowest single SOW {:.2}s)\n\
         rate:    {:.0} rows/s, {:.1} MB/s  (~{} B/row, ~{:.0} MB total)\n",
        rows_n as f64 / seed_secs.max(1e-6),
        max_one.as_secs_f64(),
        total_rows as f64 / egress_secs.max(1e-6),
        (total_bytes as f64 / 1.048e6) / egress_secs.max(1e-6),
        row_bytes,
        total_bytes as f64 / 1.048e6,
    );

    // The live SOW count agrees too (sanity vs the fan-out path).
    let pubc = Client::connect(&url).await.expect("connect");
    assert_eq!(pubc.sow("/egress", None).await.unwrap().len(), rows_n);
}

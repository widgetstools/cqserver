//! On-demand ingest micro-benchmark to locate the wide-row seed
//! bottleneck (the Atlas /positions case: ~200-col rows + ~8 views).
//! Uses the Rust client + publish_batch so the TS transport is out of
//! the picture — isolating SERVER-side commit cost.
//!
//! Run:
//!   cargo test --release -p cq-e2e-tests --test bench_wide_ingest -- --ignored --nocapture
//!
//! Reads:
//!   A (wide, no views, 1 conn)   — baseline wide-row commit rate.
//!     Compare to the TS publisher's ~160 rows/s: if A >> 160, the TS
//!     client was the limiter; if A ~ 160, the server is.
//!   B (wide, 8 views, 1 conn)    — B/A = the view-fanout tax.
//!   C (wide, no views, 4 conns)  — C/A = parallelism win (or not, if
//!     the per-topic write lock serializes regardless).

use cq_client::Client;
use cq_e2e_tests::{start_server, start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Map, Value};
use std::time::{Duration, Instant};

const N: usize = 10_000;
const BATCH: usize = 200;
const NVAL: usize = 200; // value columns; + 1 key + 8 group cols ≈ 209 cols

fn wide_topic(name: &str) -> TopicSpec {
    let mut cols: Vec<(String, String)> = vec![("k".to_string(), "string".to_string())];
    for g in 0..8 {
        cols.push((format!("g{g}"), "long".to_string()));
    }
    for v in 0..NVAL {
        cols.push((format!("v{v}"), "double".to_string()));
    }
    TopicSpec::new(name, "k").with_inline_columns(cols)
}

fn wide_rows(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            let mut m = Map::new();
            m.insert("k".into(), json!(format!("k{i}")));
            // Low-cardinality group columns so the views stay small.
            for g in 0..8 {
                m.insert(format!("g{g}"), json!((i % (4 + g * 4)) as i64));
            }
            for v in 0..NVAL {
                m.insert(format!("v{v}"), json!((i as f64) * 1.0001 + v as f64));
            }
            Value::Object(m)
        })
        .collect()
}

fn rate(n: usize, d: Duration) -> String {
    format!("{:>9.0} rows/s  ({:.2}s)", n as f64 / d.as_secs_f64(), d.as_secs_f64())
}

async fn seed_one_conn(url: &str, topic: &str, rows: &[Value]) {
    let client = Client::connect(url).await.expect("connect");
    for chunk in rows.chunks(BATCH) {
        client.publish_batch(topic, chunk.to_vec()).await.expect("publish_batch");
    }
}

#[tokio::test]
#[ignore]
async fn bench_a_wide_no_views_1conn() {
    let server = start_server(vec![wide_topic("/w")]).await;
    let rows = wide_rows(N);
    let t = Instant::now();
    seed_one_conn(&server.tcp_url(), "/w", &rows).await;
    let d = t.elapsed();
    println!("\n[A] wide(~209col) no-views, 1 conn:   {}", rate(N, d));
}

#[tokio::test]
#[ignore]
async fn bench_b_wide_with_views_1conn() {
    let views: Vec<ViewSpec> = (0..8)
        .map(|g| {
            ViewSpec::new(
                format!("/wv_v{g}"),
                "/wv",
                format!("SELECT g{g}, SUM(v0) AS s, COUNT(*) AS n FROM t GROUP BY g{g}"),
            )
        })
        .collect();
    let opts = ServerOpts { views, ..ServerOpts::default() };
    let server = start_server_with(vec![wide_topic("/wv")], opts).await;
    let rows = wide_rows(N);
    let t = Instant::now();
    seed_one_conn(&server.tcp_url(), "/wv", &rows).await;
    let d = t.elapsed();
    println!("\n[B] wide(~209col) + 8 views, 1 conn:  {}", rate(N, d));
}

#[tokio::test]
#[ignore]
async fn bench_c_wide_no_views_4conn() {
    let server = start_server(vec![wide_topic("/wc")]).await;
    let rows = wide_rows(N);
    let conns = 4usize;
    let per = N / conns;
    let t = Instant::now();
    let mut handles = Vec::new();
    for c in 0..conns {
        let url = server.tcp_url();
        let slice: Vec<Value> = rows[c * per..(c + 1) * per].to_vec();
        handles.push(tokio::spawn(async move {
            seed_one_conn(&url, "/wc", &slice).await;
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    let d = t.elapsed();
    println!("\n[C] wide(~209col) no-views, 4 conns:  {}", rate(per * conns, d));
}

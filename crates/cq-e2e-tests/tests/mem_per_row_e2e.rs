//! Measure the *real* in-memory cost per SOW row (columnar arrays + secondary
//! index + key→row map + the store's growth slack), by reading the server
//! process RSS before and after seeding a large dataset.
//!
//! `#[ignore]` — it's a measurement, not a CI assertion. Run with:
//!   cargo test --release -p cq-e2e-tests --test mem_per_row_e2e \
//!     -- --ignored --nocapture
//!
//! Override scale with CQ_MEM_ROWS (default 500k).

use std::time::Duration;

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerHandle, ServerOpts, TopicSpec};
use serde_json::{json, Map, Value};

async fn rss_bytes(server: &ServerHandle) -> u64 {
    let v: Value = reqwest::get(format!("{}/stats", server.admin_url()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap_or(Value::Null);
    v.get("processRssBytes").and_then(|x| x.as_u64()).unwrap_or(0)
}

/// Seed `n` rows on a fresh server with the given schema, return bytes/row
/// from the RSS delta (real running footprint, incl. ~20–25% store slack).
async fn measure(label: &str, cols: Vec<(String, String)>, row: impl Fn(usize) -> Value, n: usize) {
    let topic = TopicSpec::new("/m", "k").with_inline_columns(cols.clone());
    let server = start_server_with(
        vec![topic],
        ServerOpts { ..ServerOpts::default() },
    )
    .await;
    let url = server.tcp_url();

    // Baseline after the (empty) store is up.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let base = rss_bytes(&server).await;

    // Seed.
    let c = Client::connect(&url).await.expect("connect");
    const BATCH: usize = 1000;
    let mut i = 0;
    while i < n {
        let end = (i + BATCH).min(n);
        let rows: Vec<Value> = (i..end).map(&row).collect();
        c.publish_batch("/m", rows).await.expect("publish_batch");
        i = end;
    }
    // Confirm the store actually holds them, then let memory settle.
    assert_eq!(c.sow("/m", None).await.unwrap().len(), n, "{label}: row count");
    tokio::time::sleep(Duration::from_millis(800)).await;
    let after = rss_bytes(&server).await;

    let delta = after.saturating_sub(base);
    let per_row = delta as f64 / n as f64;
    let wire = serde_json::to_vec(&row(0)).unwrap().len();
    // "Rows that fit" leaving headroom: usable = budget − 2GB overhead.
    let rows_in = |gb: f64| ((gb - 2.0).max(0.0) * 1.073e9 / per_row) as u64;
    eprintln!(
        "{label:<22} cols={:<3} wire≈{wire:>4}B  in-mem≈{per_row:>6.0} B/row  \
         (Δrss {:.2} GB / {n} rows)\n  → fits ~{} rows in 8GB(≈6 usable), \
         ~{} rows in 36GB(≈34 usable)",
        cols.len(),
        delta as f64 / 1.073e9,
        fmt(rows_in(8.0)),
        fmt(rows_in(36.0)),
    );
    drop(server);
}

fn fmt(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1e6)
    } else {
        format!("{:.0}K", n as f64 / 1e3)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn measure_bytes_per_row_by_schema() {
    let n = std::env::var("CQ_MEM_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(500_000);

    // Narrow: key + a few numbers (5 cols).
    let narrow: Vec<(String, String)> = vec![
        ("k".into(), "string".into()),
        ("a".into(), "long".into()),
        ("b".into(), "long".into()),
        ("c".into(), "double".into()),
        ("d".into(), "double".into()),
    ];
    measure(
        "narrow (5 col)",
        narrow,
        |i| json!({ "k": format!("k{i:08}"), "a": i as i64, "b": (i*2) as i64,
                    "c": i as f64 * 1.5, "d": i as f64 * 0.25 }),
        n,
    )
    .await;

    // Medium: 15 cols (2 string + 10 double + 3 long) — the egress-test shape.
    let mut medium: Vec<(String, String)> =
        vec![("k".into(), "string".into()), ("sym".into(), "string".into())];
    for v in 0..10 { medium.push((format!("v{v}"), "double".into())); }
    for q in 0..3 { medium.push((format!("q{q}"), "long".into())); }
    measure(
        "medium (15 col)",
        medium,
        |i| {
            let mut m = Map::new();
            m.insert("k".into(), json!(format!("k{i:08}")));
            m.insert("sym".into(), json!(format!("S{:04}", i % 5000)));
            for v in 0..10 { m.insert(format!("v{v}"), json!(i as f64 + v as f64)); }
            for q in 0..3 { m.insert(format!("q{q}"), json!((i as i64) * (q as i64 + 1))); }
            Value::Object(m)
        },
        n,
    )
    .await;

    // Wide: 100 cols (key + 80 double + 19 string) — Atlas /positions-ish.
    let mut wide: Vec<(String, String)> = vec![("k".into(), "string".into())];
    for v in 0..80 { wide.push((format!("v{v}"), "double".into())); }
    for s in 0..19 { wide.push((format!("s{s}"), "string".into())); }
    let wide_n = (n / 2).max(100_000); // wide rows are heavier — seed fewer
    measure(
        "wide (100 col)",
        wide,
        |i| {
            let mut m = Map::new();
            m.insert("k".into(), json!(format!("k{i:08}")));
            for v in 0..80 { m.insert(format!("v{v}"), json!(i as f64 * 1.0001 + v as f64)); }
            for s in 0..19 { m.insert(format!("s{s}"), json!(format!("val{}-{}", s, i % 1000))); }
            Value::Object(m)
        },
        wide_n,
    )
    .await;

    // Real-world wide: 200 cols (150 double + 30 string + 19 long) — Atlas
    // /positions shape (trader's book).
    let mut w200: Vec<(String, String)> = vec![("k".into(), "string".into())];
    for v in 0..150 { w200.push((format!("v{v}"), "double".into())); }
    for s in 0..30 { w200.push((format!("s{s}"), "string".into())); }
    for q in 0..19 { w200.push((format!("q{q}"), "long".into())); }
    measure(
        "real-world (200 col)",
        w200,
        |i| {
            let mut m = Map::new();
            m.insert("k".into(), json!(format!("k{i:08}")));
            for v in 0..150 { m.insert(format!("v{v}"), json!(i as f64 * 1.0001 + v as f64)); }
            for s in 0..30 { m.insert(format!("s{s}"), json!(format!("val{}-{}", s, i % 1000))); }
            for q in 0..19 { m.insert(format!("q{q}"), json!((i as i64) * (q as i64 + 1))); }
            Value::Object(m)
        },
        (n / 4).max(50_000),
    )
    .await;
}

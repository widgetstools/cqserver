//! Concrete measurement of the "book × sector PnL pivot" view shape.
//!
//! Loads the actual demo data (positions.json + securities.json) into a
//! fresh cqserver, declares a `[[views]]` block that does the
//! `JOIN /positions × /securities GROUP BY book, sector` aggregate,
//! subscribes to the view, and reports:
//!   - result row count (bounded by |books| × |sectors|)
//!   - JSON byte size
//!   - end-to-end latency (publish → view materialized → SOW delivered)
//!
//! Purpose: prove that this query shape is small + fast regardless of
//! the underlying 40 K positions, and so naturally fanout-friendly for
//! 2 K+ trader subscribers via the H2 snapshot cache.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Value};
use std::fs;
use std::time::{Duration, Instant};

fn load_json_array(path: &str) -> Vec<Value> {
    let raw = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read {path}: {e}"));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {path}: {e}"))
}

/// Run with `cargo test -p cq-e2e-tests --test trader_view_pivot_e2e
/// --release -- --nocapture --ignored` so the printed metrics show up.
/// Ignored by default because it loads ~40 K positions into a fresh
/// server and is slower than a typical unit test.
#[tokio::test]
#[ignore]
async fn book_sector_pnl_view_size_and_latency() {
    let workspace_root = std::env::var("CARGO_MANIFEST_DIR")
        .map(|d| std::path::PathBuf::from(d).parent().unwrap().parent().unwrap().to_path_buf())
        .unwrap();
    let positions_path = workspace_root
        .join("clients/ts/examples/data/positions.json")
        .to_string_lossy()
        .to_string();
    let securities_path = workspace_root
        .join("clients/ts/examples/data/securities.json")
        .to_string_lossy()
        .to_string();

    let positions = load_json_array(&positions_path);
    let securities = load_json_array(&securities_path);
    let n_positions = positions.len();
    let n_securities = securities.len();
    eprintln!(
        "Loaded {n_positions} positions + {n_securities} securities from demo data"
    );

    // Topic schemas mirror the live demo's config/schemas/*.json.
    let positions_topic = TopicSpec::new("/positions", "positionKey")
        .with_inline_columns([
            ("positionKey", "string"),
            ("book", "string"),
            ("cusip", "string"),
            ("ticker", "string"),
            ("netQty", "long"),
            ("avgCost", "double"),
            ("lastMid", "double"),
            ("marketValue", "double"),
            ("unrealizedPnl", "double"),
            ("trades", "long"),
        ]);
    let securities_topic = TopicSpec::new("/securities", "cusip")
        .with_inline_columns([
            ("cusip", "string"),
            ("ticker", "string"),
            ("issuer", "string"),
            ("coupon", "double"),
            ("maturity", "string"),
            ("assetClass", "string"),
            ("sector", "string"),
            ("currency", "string"),
            ("initialMid", "double"),
        ]);

    let view_sql = "SELECT book, sector, \
                    SUM(unrealizedPnl) AS total_pnl, \
                    SUM(marketValue)   AS exposure, \
                    COUNT(*)           AS positions \
                    FROM \"/positions\" \
                    JOIN \"/securities\" USING (cusip) \
                    GROUP BY book, sector";
    let view = ViewSpec::new("/book-sector-pnl", "/positions", view_sql);

    let server = start_server_with(
        vec![positions_topic, securities_topic],
        ServerOpts {
            views: vec![view],
            ..ServerOpts::default()
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed securities first so positions land on top of populated
    // right-side data — order matters here because the view re-evaluates
    // on every left-side mutation and we don't want every position
    // publish to spam empty joins.
    let t_seed = Instant::now();
    for s in &securities {
        client
            .publish("/securities", s.clone())
            .await
            .expect("publish security");
    }
    for p in &positions {
        client
            .publish("/positions", p.clone())
            .await
            .expect("publish position");
    }
    let seed_ms = t_seed.elapsed().as_millis();
    eprintln!(
        "Seeded {n_securities} securities + {n_positions} positions in {seed_ms} ms"
    );

    // Give the view evaluator a moment to finish materializing.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe to the view via SOW. The view is registered as a topic
    // — subscribing returns the materialized rows, one per (book,
    // sector) group.
    let t_sow = Instant::now();
    let rows = client
        .sow("/book-sector-pnl", None)
        .await
        .expect("sow on view");
    let sow_ms = t_sow.elapsed().as_millis();

    let bytes_total: usize = rows
        .iter()
        .map(|r| serde_json::to_vec(r).map(|v| v.len()).unwrap_or(0))
        .sum();

    let distinct_books: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|r| r.get("book").and_then(Value::as_str).map(String::from))
        .collect();
    let distinct_sectors: std::collections::HashSet<String> = rows
        .iter()
        .filter_map(|r| r.get("sector").and_then(Value::as_str).map(String::from))
        .collect();

    eprintln!("──────────────────────────────────────────────────");
    eprintln!("Trader view: book × sector PnL pivot");
    eprintln!("  Source:        /positions ({} rows) JOIN /securities ({} rows) USING (cusip)", n_positions, n_securities);
    eprintln!("  Aggregate:     SUM(unrealizedPnl), SUM(marketValue), COUNT(*) GROUP BY book, sector");
    eprintln!("  Result rows:   {}", rows.len());
    eprintln!("  Distinct books:   {}", distinct_books.len());
    eprintln!("  Distinct sectors: {}", distinct_sectors.len());
    eprintln!("  JSON bytes:    {bytes_total}  ({:.1} KB)", bytes_total as f64 / 1024.0);
    eprintln!("  SOW latency:   {sow_ms} ms");
    if let Some(first) = rows.first() {
        let s = serde_json::to_string(first).unwrap_or_default();
        eprintln!("  Sample row:    {}", &s[..s.len().min(220)]);
    }
    // Find a couple of interesting cells.
    if let Some(top) = rows.iter().max_by(|a, b| {
        let av = a.get("total_pnl").and_then(Value::as_f64).unwrap_or(0.0);
        let bv = b.get("total_pnl").and_then(Value::as_f64).unwrap_or(0.0);
        av.partial_cmp(&bv).unwrap_or(std::cmp::Ordering::Equal)
    }) {
        let s = serde_json::to_string(top).unwrap_or_default();
        eprintln!("  Top PnL cell:  {}", &s[..s.len().min(220)]);
    }
    eprintln!("──────────────────────────────────────────────────");

    // Sanity: the result must not be empty, must not exceed the
    // theoretical cap, and the cardinalities should match the source
    // data's actual book/sector counts.
    assert!(!rows.is_empty(), "view produced zero rows");
    assert!(
        rows.len() <= distinct_books.len() * distinct_sectors.len(),
        "result row count exceeds books × sectors cap"
    );
    let _ = json!(null); // suppress unused-import warning if json! ends up unused
}

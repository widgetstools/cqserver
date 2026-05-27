//! `cq-loadgen` — stress-test driver for CQServer (worklog S47).
//!
//! ```text
//! cq-loadgen \
//!     --server tcp://127.0.0.1:9007 \
//!     --topic /loadgen \
//!     --scenario publish-throughput \
//!     --rate 10000 \
//!     --duration 30s \
//!     --warmup 1s \
//!     --subscribers 0
//! ```
//!
//! Today's scenarios cover stress-test plan §1 C and D
//! (`publish-throughput`, `fanout`). Scenarios A, B, E, F, G are added
//! by their owning sessions (S38, S42, S46, S21) as `#[ignore]`
//! integration tests that import this crate.

use std::time::Duration;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use cq_loadgen::{scenarios, ScenarioConfig};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Scenario {
    /// 1 publisher × 0 subscribers — stress-plan Scenario C.
    PublishThroughput,
    /// 1 publisher × N subscribers — stress-plan Scenario D.
    Fanout,
    /// 2000+ concurrent subscribers spread across four query-complexity
    /// classes (firehose, WHERE, GROUP BY, static PIVOT). Samples admin
    /// `/stats` over the measurement window and reports peak / steady
    /// RSS, connect-time histogram, and per-class delivery throughput.
    Stress2k,
    /// Realistic-payload variant of stress2k: every subscriber issues
    /// `subscribe(topic, "book = 'BOOK-N'")` with N varying per-sub —
    /// the AMPS-typical "trader watches their own book" pattern.
    /// Each sub gets a focused snapshot (~10K rows on the demo's
    /// 80-book dataset) instead of the firehose's full 865K rows.
    Stress2kReal,
    /// One-shot measurement of the "trader dashboard" pivot:
    /// SELECT book, sector, SUM(pnl), SUM(exposure)
    /// FROM /positions JOIN /securities USING (cusip)
    /// GROUP BY book, sector.
    /// Reports rows + bytes + latency. Result set is small
    /// (|books| × |sectors|) regardless of underlying position count.
    TraderViewPivot,
    /// Q2 follow-up — measure wire-level `publish_batch` throughput
    /// vs sequential per-row `publish`. `--rate` is reinterpreted as
    /// total rows; `--warmup-secs` is reinterpreted as batch size.
    PublishBatchVsSeq,
    /// Q11 follow-up — sustain `--rate` msg/s while adding a column
    /// online every 5 seconds. Reports publish ack p50/p99 and the
    /// number of columns added in the window.
    SchemaEvolutionUnderLoad,
}

#[derive(Parser, Debug)]
#[command(version, about = "CQServer load generator (S47)")]
struct Args {
    #[arg(long, default_value = "tcp://127.0.0.1:9007")]
    server: String,

    #[arg(long, default_value = "/loadgen")]
    topic: String,

    #[arg(long, value_enum)]
    scenario: Scenario,

    /// Target publish rate in events/second (0 = unthrottled).
    #[arg(long, default_value_t = 1000.0)]
    rate: f64,

    /// How long the measurement window runs in seconds (after `--warmup`).
    #[arg(long, default_value_t = 10.0)]
    duration_secs: f64,

    /// Warmup window in seconds — measurements during this period are discarded.
    #[arg(long, default_value_t = 1.0)]
    warmup_secs: f64,

    /// Number of subscribers (used by `fanout` and `stress-2k`).
    /// For stress-2k, this is the total across all 4 classes (default 2000).
    #[arg(long, default_value_t = 0)]
    subscribers: usize,

    /// Admin-API URL for /stats polling (stress-2k only).
    #[arg(long, default_value = "http://127.0.0.1:8085")]
    admin_url: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let cfg = ScenarioConfig {
        server_url: args.server,
        topic: args.topic,
        duration: Duration::from_secs_f64(args.duration_secs),
        publish_rate: args.rate,
        subscribers: args.subscribers,
        warmup: Duration::from_secs_f64(args.warmup_secs),
        admin_url: args.admin_url,
    };
    match args.scenario {
        Scenario::PublishThroughput => scenarios::publish_throughput(&cfg).await?.print(),
        Scenario::Fanout => scenarios::fanout(&cfg).await?.print(),
        Scenario::Stress2k => {
            // stress2k has its own richer report shape — print directly.
            scenarios::stress_2k(&cfg).await?.print();
        }
        Scenario::Stress2kReal => {
            scenarios::stress_2k_real(&cfg).await?.print();
        }
        Scenario::TraderViewPivot => {
            scenarios::trader_view_pivot(&cfg).await?;
        }
        Scenario::PublishBatchVsSeq => {
            scenarios::publish_batch_vs_sequential(&cfg).await?.print();
        }
        Scenario::SchemaEvolutionUnderLoad => {
            scenarios::schema_evolution_under_load(&cfg).await?.print();
        }
    };
    Ok(())
}

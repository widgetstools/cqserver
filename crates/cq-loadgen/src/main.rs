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

    /// Number of subscribers (used by `fanout` only).
    #[arg(long, default_value_t = 0)]
    subscribers: usize,
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
    };
    let report = match args.scenario {
        Scenario::PublishThroughput => scenarios::publish_throughput(&cfg).await?,
        Scenario::Fanout => scenarios::fanout(&cfg).await?,
    };
    report.print();
    Ok(())
}

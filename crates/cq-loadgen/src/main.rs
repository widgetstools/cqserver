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
    /// Heavy SOW: seed `--rows` × `--cols` (default 30000 × 400),
    /// measure the full snapshot delivery, then drive `--rate` sparse
    /// `delta_publish` updates/s for `--duration-secs` and report
    /// publish→delivery latency on the live subscription.
    SowWide,
    /// Wire-egress ceiling: ramp `--subscribers N` lightweight firehose
    /// subscribers (drain-and-discard), then publish `--rate` msg/s of
    /// `--payload-bytes` bodies. Reports achieved deltas/s + MB/s, the
    /// delivered fraction vs ideal `rate × N` fanout, and peak server RSS.
    /// Use for 2000/5000/10000-connection egress tests.
    EgressFanout,
    /// Long-running Atlas-shaped soak (Bucket B, task B2). Seeds a wide
    /// row set on `--topic` (default caller should pass `/positions` to
    /// match `tests/cloud/configs/soak.toml`), then holds 3 subscriber
    /// classes open for `--duration-secs` while ticking sparse
    /// `delta_publish` updates at `--rate`/s: fast full-firehose
    /// (`--soak-fast-subscribers`), conflated (`--soak-conflated-subscribers`
    /// — conflation itself comes from the topic's server-side
    /// `conflation_ms`), and deliberately-slow
    /// (`--soak-slow-subscribers`, `--soak-slow-read-delay-ms`). Logs
    /// progress every `--soak-progress-interval-secs` and a final summary.
    Soak,
    /// Bucket B, task B3 — read Prometheus over a run window and emit a
    /// machine-checkable PASS/FAIL verdict for a soak: RSS slope ≈ 0
    /// after warmup, bounded delta drops, checkpoint+reclaim activity
    /// (txlog bounded), and p99 publish latency under target. Exits
    /// nonzero on FAIL so CI/the runbook can gate on it. Point
    /// `--prometheus-url` at the soak's Prometheus (see
    /// `tests/cloud/SOAK.md`) and either `--soak-analyze-last-minutes`
    /// or an explicit `--soak-analyze-start`/`--soak-analyze-end`
    /// window.
    SoakAnalyze,
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

    /// Target serialized JSON body size in bytes per publish (publish-throughput
    /// only). 0 = tiny `{k,v}` body; larger values pad with a filler field.
    #[arg(long, default_value_t = 0)]
    payload_bytes: usize,

    /// sow-wide: rows to seed before the snapshot. 0 ⇒ 30000.
    #[arg(long, default_value_t = 0)]
    rows: usize,

    /// sow-wide: columns per row (including the `k` key). 0 ⇒ 400.
    #[arg(long, default_value_t = 0)]
    cols: usize,

    /// soak: number of fast full-firehose subscribers held for the
    /// whole run. 0 ⇒ 2.
    #[arg(long, default_value_t = 0)]
    soak_fast_subscribers: usize,

    /// soak: number of conflated-class subscribers held for the whole
    /// run. 0 ⇒ 2.
    #[arg(long, default_value_t = 0)]
    soak_conflated_subscribers: usize,

    /// soak: number of deliberately-slow subscribers held for the whole
    /// run. 0 ⇒ 1.
    #[arg(long, default_value_t = 0)]
    soak_slow_subscribers: usize,

    /// soak: milliseconds a slow subscriber sleeps between reads. 0 ⇒ 200.
    #[arg(long, default_value_t = 0)]
    soak_slow_read_delay_ms: u64,

    /// soak: seconds between progress log lines. 0 ⇒ 10.
    #[arg(long, default_value_t = 0)]
    soak_progress_interval_secs: u64,

    /// soak: the topic's key field name (so seeded rows + delta ticks
    /// carry a key the server recognizes). Empty ⇒ "position_id" (matches
    /// `tests/cloud/configs/soak.toml`'s `/positions` topic).
    #[arg(long, default_value = "")]
    soak_key_field: String,

    /// soak-analyze: Prometheus base URL, e.g. http://localhost:9090.
    #[arg(long, default_value = "http://127.0.0.1:9090")]
    prometheus_url: String,

    /// soak-analyze: analyze the last N minutes ending now. Ignored if
    /// `--soak-analyze-start` / `--soak-analyze-end` are both given.
    #[arg(long, default_value_t = 60.0)]
    soak_analyze_last_minutes: f64,

    /// soak-analyze: explicit window start, Unix seconds. Overrides
    /// `--soak-analyze-last-minutes` when set together with `--soak-analyze-end`.
    #[arg(long)]
    soak_analyze_start: Option<f64>,

    /// soak-analyze: explicit window end, Unix seconds.
    #[arg(long)]
    soak_analyze_end: Option<f64>,

    /// soak-analyze: Prometheus query step in seconds. 0 ⇒ 10 (matches
    /// `tests/cloud/prometheus-soak.yml`'s scrape interval).
    #[arg(long, default_value_t = 0.0)]
    soak_analyze_step_secs: f64,

    /// soak-analyze: max allowed RSS growth after warmup exclusion, in
    /// MB/hour, before the rss_slope criterion FAILs (a leak signal).
    #[arg(long, default_value_t = 50.0)]
    soak_analyze_max_rss_growth_mb_per_hour: f64,

    /// soak-analyze: fraction (0.0-1.0) of the window treated as warmup
    /// and excluded from the RSS slope fit.
    #[arg(long, default_value_t = 0.10)]
    soak_analyze_warmup_fraction: f64,

    /// soak-analyze: max allowed ratio of dropped to delivered deltas
    /// over the window before delta_drop_ratio FAILs. Slow-consumer
    /// drops are expected — this bounds them, it doesn't require zero.
    #[arg(long, default_value_t = 0.05)]
    soak_analyze_max_drop_ratio: f64,

    /// soak-analyze: max allowed p99 publish latency in microseconds
    /// before p99_publish_latency FAILs.
    #[arg(long, default_value_t = 50_000.0)]
    soak_analyze_max_p99_latency_us: f64,

    /// soak-analyze: max allowed on-disk txlog growth after warmup
    /// exclusion, in MB/hour, before the txlog_bounded criterion FAILs
    /// (reclaim losing the race against the write rate — disk growing
    /// unboundedly).
    #[arg(long, default_value_t = 50.0)]
    soak_analyze_max_txlog_growth_mb_per_hour: f64,
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
        payload_bytes: args.payload_bytes,
        wide_rows: args.rows,
        wide_cols: args.cols,
        soak_fast_subscribers: args.soak_fast_subscribers,
        soak_conflated_subscribers: args.soak_conflated_subscribers,
        soak_slow_subscribers: args.soak_slow_subscribers,
        soak_slow_read_delay_ms: args.soak_slow_read_delay_ms,
        soak_progress_interval_secs: args.soak_progress_interval_secs,
        soak_key_field: args.soak_key_field,
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
        Scenario::SowWide => {
            scenarios::sow_wide(&cfg).await?;
        }
        Scenario::EgressFanout => {
            scenarios::egress_fanout(&cfg).await?;
        }
        Scenario::Soak => {
            scenarios::soak(&cfg).await?;
        }
        Scenario::SoakAnalyze => {
            use cq_loadgen::soak_analyze::{analyze_config_from_args, run};
            let analyze_cfg = analyze_config_from_args(
                args.prometheus_url,
                args.soak_analyze_last_minutes,
                args.soak_analyze_start,
                args.soak_analyze_end,
                args.soak_analyze_step_secs,
                args.soak_analyze_max_rss_growth_mb_per_hour,
                args.soak_analyze_warmup_fraction,
                args.soak_analyze_max_drop_ratio,
                args.soak_analyze_max_p99_latency_us,
                args.soak_analyze_max_txlog_growth_mb_per_hour,
            );
            let report = run(&analyze_cfg).await?;
            print!("{}", report.render());
            if !report.overall().is_pass() {
                std::process::exit(1);
            }
        }
    };
    Ok(())
}

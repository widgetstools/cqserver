//! Soak pass/fail analyzer (Bucket B, task B3).
//!
//! Reads Prometheus over a soak's run window and emits a
//! machine-checkable verdict, so a multi-day soak self-judges instead
//! of a human staring at Grafana. Four criteria, each PASS/FAIL with
//! the measured value and threshold printed:
//!
//! 1. **RSS slope ≈ 0 after warmup** — linear-fit `cq_process_rss_bytes`
//!    over the window excluding the first ~10% (warmup), FAIL if the
//!    fitted slope implies growth beyond a configurable MB/hour
//!    threshold (a leak).
//! 2. **Drops bounded (both routes)** — (`cq_deltas_dropped_total` +
//!    `cq_subscription_dropped_total`) vs `cq_deltas_delivered_total`.
//!    The direct (non-conflated) route drops via
//!    `cq_deltas_dropped_total` (see
//!    `crates/cq-transport/src/delivery.rs`); any topic with
//!    `conflation_ms` set (e.g. `/positions`, the soak's primary topic)
//!    drops via the differently-named `cq_subscription_dropped_total`
//!    instead (see `crates/cq-transport/src/session.rs`) — the
//!    conflator's flush loop returns before
//!    `cq_deltas_dropped_total`/`cq_deltas_delivered_total` are ever
//!    touched, so checking only the deltas counter would be a
//!    near-no-op for the shipped conflated topology. The slow-consumer
//!    policy *causes* some drops by design on both routes, so the
//!    check is that the summed drop ratio over the window stays under
//!    a configurable threshold, not that drops are zero. Both counters
//!    are Prometheus counters that only appear after their first
//!    increment, so an absent series (on either route) means zero
//!    drops occurred on that route — treated as a PASS contribution,
//!    not an error.
//! 3. **Txlog bounded** — asserts an actual bounded *byte count*, not
//!    just activity: `cq_txlog_bytes` (a per-topic on-disk-size gauge,
//!    summed across topics) is linear-fit over the window the same way
//!    as RSS (post-warmup exclusion), and FAILs if the fitted growth
//!    rate exceeds a configurable MB/hour threshold — this is the
//!    direct proof that reclaim is winning the race against the write
//!    rate, not just that reclaim activity happened at all (a reclaim
//!    that's losing the race — freeing less than is being written —
//!    would still fire checkpoints and reclaim segments, so activity
//!    alone can't tell the two cases apart). The pre-existing activity
//!    checks (`cq_txlog_checkpoint_total` increased, at least one
//!    `cq_txlog_segments_reclaimed_total` reclaim) are kept as
//!    complementary signals: activity-with-unbounded-growth is a
//!    distinct, actionable failure mode (reclaim runs but the write
//!    rate outpaces it) from no-activity-at-all (checkpointing itself
//!    is broken).
//! 4. **p99 publish latency under target** — `cq_publish_latency_us`.
//!    The server's Prometheus exporter is installed with no bucket
//!    config, so this renders as a Prometheus *summary*
//!    (`cq_publish_latency_us{quantile="0.99", ...}`), not a
//!    `_bucket` histogram — the analyzer queries the exported
//!    `quantile="0.99"` series directly (maxed across topics) rather
//!    than `histogram_quantile(...)`, which would match nothing. Must
//!    stay under a configurable microsecond target.
//!
//! The verdict math (linear fit, ratio/rate checks, threshold
//! comparisons) is factored into pure functions that take parsed
//! time-series vectors and return a [`CriterionResult`] — these are
//! unit-tested against synthetic series with no Prometheus required.
//! The impure half ([`fetch_range`], [`run`]) does the HTTP
//! `query_range` calls and glues real data into the pure functions.

use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// One (timestamp_secs, value) sample from a Prometheus range vector.
pub type Sample = (f64, f64);

/// Thresholds for the four verdict criteria. All configurable so CI /
/// the runbook can tune sensitivity without code changes.
#[derive(Debug, Clone)]
pub struct Thresholds {
    /// Max allowed RSS growth slope, in MB/hour, after warmup exclusion.
    pub max_rss_growth_mb_per_hour: f64,
    /// Fraction of the window (0.0..1.0) treated as warmup and excluded
    /// from the RSS slope fit.
    pub warmup_fraction: f64,
    /// Max allowed ratio of (drops delivered over the window) to
    /// (delivered over the window). E.g. 0.05 = drops must stay under
    /// 5% of delivered volume.
    pub max_drop_ratio: f64,
    /// Max allowed p99 publish latency, in microseconds.
    pub max_p99_publish_latency_us: f64,
    /// Max allowed on-disk txlog growth slope, in MB/hour, after warmup
    /// exclusion (same fit approach as RSS). This is the direct
    /// bounded-disk assertion: reclaim must free segments at least as
    /// fast as the write rate adds them, or this fails even though
    /// checkpoint/reclaim activity counters look healthy.
    pub max_txlog_growth_mb_per_hour: f64,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            max_rss_growth_mb_per_hour: 50.0,
            warmup_fraction: 0.10,
            max_drop_ratio: 0.05,
            max_p99_publish_latency_us: 50_000.0,
            max_txlog_growth_mb_per_hour: 50.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    Pass,
    Fail,
}

impl Verdict {
    pub fn is_pass(&self) -> bool {
        matches!(self, Verdict::Pass)
    }
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Pass => write!(f, "PASS"),
            Verdict::Fail => write!(f, "FAIL"),
        }
    }
}

/// Result of a single verdict criterion — name, human-readable measured
/// value, human-readable threshold, and pass/fail.
#[derive(Debug, Clone)]
pub struct CriterionResult {
    pub name: &'static str,
    pub measured: String,
    pub threshold: String,
    pub verdict: Verdict,
    /// Extra context printed under the table row (e.g. a note about a
    /// missing metric) — empty string if none.
    pub note: String,
}

impl CriterionResult {
    fn new(
        name: &'static str,
        measured: impl Into<String>,
        threshold: impl Into<String>,
        pass: bool,
    ) -> Self {
        Self {
            name,
            measured: measured.into(),
            threshold: threshold.into(),
            verdict: if pass { Verdict::Pass } else { Verdict::Fail },
            note: String::new(),
        }
    }

    fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = note.into();
        self
    }
}

/// Full analyzer report: one [`CriterionResult`] per check plus the
/// overall verdict (FAIL if any criterion fails).
#[derive(Debug, Clone)]
pub struct SoakReport {
    pub criteria: Vec<CriterionResult>,
}

impl SoakReport {
    pub fn overall(&self) -> Verdict {
        if self.criteria.iter().all(|c| c.verdict.is_pass()) {
            Verdict::Pass
        } else {
            Verdict::Fail
        }
    }

    /// Render the per-criterion table + final `SOAK VERDICT:` line.
    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str("──────────────────────────────────────────────────\n");
        out.push_str("Soak analyzer verdict\n");
        out.push_str("──────────────────────────────────────────────────\n");
        let name_w = self
            .criteria
            .iter()
            .map(|c| c.name.len())
            .max()
            .unwrap_or(10)
            .max(10);
        for c in &self.criteria {
            out.push_str(&format!(
                "{:<name_w$}  measured={:<22} threshold={:<22} {}\n",
                c.name,
                c.measured,
                c.threshold,
                c.verdict,
                name_w = name_w
            ));
            if !c.note.is_empty() {
                out.push_str(&format!(
                    "{:<name_w$}  note: {}\n",
                    "",
                    c.note,
                    name_w = name_w
                ));
            }
        }
        out.push_str("──────────────────────────────────────────────────\n");
        out.push_str(&format!("SOAK VERDICT: {}\n", self.overall()));
        out
    }
}

// ---------------------------------------------------------------------
// Pure verdict functions — unit-tested against synthetic series, no
// Prometheus required.
// ---------------------------------------------------------------------

/// Ordinary-least-squares linear fit `y = slope*x + intercept` over
/// `(x, y)` points. Returns `(slope, intercept)`. `x` is expected in
/// seconds; callers convert slope units (e.g. bytes/sec) as needed.
/// Returns `(0.0, mean_y)` for fewer than 2 points (nothing to fit).
fn linear_fit(points: &[Sample]) -> (f64, f64) {
    let n = points.len();
    if n < 2 {
        let mean_y = points.first().map(|(_, y)| *y).unwrap_or(0.0);
        return (0.0, mean_y);
    }
    let n_f = n as f64;
    let sum_x: f64 = points.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = points.iter().map(|(_, y)| y).sum();
    let sum_xx: f64 = points.iter().map(|(x, _)| x * x).sum();
    let sum_xy: f64 = points.iter().map(|(x, y)| x * y).sum();
    let denom = n_f * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        // All x identical (degenerate) — flat fit at mean y.
        return (0.0, sum_y / n_f);
    }
    let slope = (n_f * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n_f;
    (slope, intercept)
}

/// Shared warmup-exclusion + linear-fit step used by both the RSS-slope
/// and txlog-byte-growth criteria: discard the first `warmup_fraction`
/// of the window (by time span, not sample count) so startup ramp-up
/// doesn't get mistaken for unbounded growth, then OLS-fit the rest and
/// convert the slope from units/sec to MB/hour.
///
/// Returns `Err(CriterionResult)` (a ready-to-return failing result)
/// when there aren't enough samples to fit; `Ok(slope_mb_per_hour)`
/// otherwise.
fn post_warmup_growth_mb_per_hour(
    series: &[Sample],
    warmup_fraction: f64,
    name: &'static str,
    insufficient_note: &str,
    warmup_note: &str,
) -> Result<f64, CriterionResult> {
    if series.len() < 2 {
        return Err(
            CriterionResult::new(name, "insufficient samples", "n/a", false)
                .with_note(insufficient_note),
        );
    }
    let t0 = series.first().unwrap().0;
    let t1 = series.last().unwrap().0;
    let span = t1 - t0;
    let warmup_cutoff = t0 + span * warmup_fraction;
    let post_warmup: Vec<Sample> = series
        .iter()
        .copied()
        .filter(|(t, _)| *t >= warmup_cutoff)
        .collect();

    if post_warmup.len() < 2 {
        return Err(
            CriterionResult::new(name, "insufficient post-warmup samples", "n/a", false)
                .with_note(warmup_note),
        );
    }

    let (slope_per_sec, _intercept) = linear_fit(&post_warmup);
    Ok(slope_per_sec * 3600.0 / (1024.0 * 1024.0))
}

/// Criterion 1: RSS slope ≈ 0 after warmup exclusion.
///
/// `rss_series` is `(timestamp_secs, rss_bytes)` samples across the
/// whole window, in chronological order. The first `warmup_fraction`
/// of the window (by time span, not sample count) is discarded before
/// fitting, so startup allocation/JIT-style ramp-up doesn't get
/// mistaken for a leak.
pub fn check_rss_slope(rss_series: &[Sample], thresholds: &Thresholds) -> CriterionResult {
    const NAME: &str = "rss_slope";
    let slope_mb_per_hour = match post_warmup_growth_mb_per_hour(
        rss_series,
        thresholds.warmup_fraction,
        NAME,
        "need >= 2 cq_process_rss_bytes samples to fit a slope",
        "window too short relative to warmup_fraction to fit a slope",
    ) {
        Ok(v) => v,
        Err(result) => return result,
    };
    let pass = slope_mb_per_hour <= thresholds.max_rss_growth_mb_per_hour;

    CriterionResult::new(
        NAME,
        format!("{:.2} MB/hour", slope_mb_per_hour),
        format!("<= {:.2} MB/hour", thresholds.max_rss_growth_mb_per_hour),
        pass,
    )
}

/// Criterion 2: drops bounded relative to delivered volume, across
/// BOTH drop routes.
///
/// The server has two independent, differently-named drop counters
/// depending on the topic's delivery path:
///
/// - `cq_deltas_dropped_total` — the direct (non-conflated) route,
///   incremented in `crates/cq-transport/src/delivery.rs`.
/// - `cq_subscription_dropped_total` — the conflated route (any topic
///   with `conflation_ms` set, e.g. `/positions`, the soak's primary
///   topic), incremented in `crates/cq-transport/src/session.rs`. The
///   conflator's `try_submit_to_conflator` call returns *before*
///   `cq_deltas_delivered_total`/`cq_deltas_dropped_total` are ever
///   touched, so a conflated topic's drops are entirely invisible to
///   `cq_deltas_dropped_total` — checking only that counter would be a
///   near-no-op for the shipped conflated topology, reporting 0 drops
///   even while the conflated path sheds load.
///
/// This function sums both counters' window deltas into a single
/// `total_dropped` and divides by `delivered_delta` (from
/// `cq_deltas_delivered_total` — the only delivered counter the server
/// exports; there is no conflated/subscription-route "delivered"
/// counter to sum in, so this remains the best available denominator
/// even though it under-counts conflated-route delivered volume).
/// Slow-consumer drops are expected and healthy — the check is a
/// *ratio* threshold, not zero-drops.
///
/// Both `cq_deltas_dropped_total` and `cq_subscription_dropped_total`
/// are Prometheus counters that the server only starts exporting after
/// their first increment — a counter simply isn't registered until
/// something bumps it. So an EMPTY series for either is the expected,
/// healthy shape of "zero drops happened on that route in the window,"
/// not an error: each is treated as a delta of 0 independently, and
/// the ratio check proceeds on whatever sum results (and passes if
/// both are empty, since 0/delivered <= any threshold). Only an empty
/// `delivered_series` — which should always be present once any
/// publish has happened — degrades the result, and even then only to
/// a soft "insufficient data" note rather than a hard FAIL, since a
/// soak window with literally no delivered samples yet (e.g. queried
/// too early) isn't itself proof of a problem.
pub fn check_drop_ratio(
    deltas_dropped_series: &[Sample],
    subscription_dropped_series: &[Sample],
    delivered_series: &[Sample],
    thresholds: &Thresholds,
) -> CriterionResult {
    const NAME: &str = "drop_ratio";
    if delivered_series.is_empty() {
        return CriterionResult::new(NAME, "no samples", "n/a", false).with_note(
            "cq_deltas_delivered_total absent — insufficient data (expected once delta_publish \
             has happened; dropped-but-not-delivered would be unusual this early in a soak)",
        );
    }
    let deltas_dropped_delta = if deltas_dropped_series.is_empty() {
        0.0
    } else {
        counter_delta(deltas_dropped_series)
    };
    let subscription_dropped_delta = if subscription_dropped_series.is_empty() {
        0.0
    } else {
        counter_delta(subscription_dropped_series)
    };
    let dropped_delta = deltas_dropped_delta + subscription_dropped_delta;
    let delivered_delta = counter_delta(delivered_series);
    let ratio = dropped_delta / delivered_delta.max(1.0);
    let pass = ratio <= thresholds.max_drop_ratio;

    let mut result = CriterionResult::new(
        NAME,
        format!(
            "{:.4} (dropped={:.0} [deltas={:.0} subscription={:.0}], delivered={:.0})",
            ratio, dropped_delta, deltas_dropped_delta, subscription_dropped_delta, delivered_delta
        ),
        format!("<= {:.4}", thresholds.max_drop_ratio),
        pass,
    );
    if deltas_dropped_series.is_empty() || subscription_dropped_series.is_empty() {
        let absent = match (
            deltas_dropped_series.is_empty(),
            subscription_dropped_series.is_empty(),
        ) {
            (true, true) => "cq_deltas_dropped_total and cq_subscription_dropped_total are both",
            (true, false) => "cq_deltas_dropped_total is",
            (false, true) => "cq_subscription_dropped_total is",
            (false, false) => unreachable!(),
        };
        result.note = format!(
            "{absent} absent — a counter isn't registered until its first increment, so this is \
             treated as zero drops on that route (healthy), not an error. Covers both the \
             direct-delta route (cq_deltas_dropped_total) and the conflated/subscription route \
             (cq_subscription_dropped_total, e.g. /positions)."
        );
    }
    result
}

/// Sum of positive increments across a counter series, handling
/// Prometheus counter resets (a scrape-to-scrape decrease, e.g. from a
/// process restart) by treating the reset point's contribution as the
/// post-reset value itself rather than a negative delta.
fn counter_delta(series: &[Sample]) -> f64 {
    if series.len() < 2 {
        return series.first().map(|(_, v)| *v).unwrap_or(0.0);
    }
    let mut total = 0.0;
    for w in series.windows(2) {
        let (_, prev) = w[0];
        let (_, cur) = w[1];
        if cur >= prev {
            total += cur - prev;
        } else {
            // Counter reset — count the post-reset value as new growth.
            total += cur;
        }
    }
    total
}

/// Criterion 3: txlog bounded — a real byte-growth bound, plus
/// complementary checkpoint/reclaim activity checks.
///
/// `txlog_bytes_series` is `(timestamp_secs, total_bytes)` samples of
/// `cq_txlog_bytes` (summed across topics — see [`run`]'s PromQL), the
/// per-topic on-disk-size gauge the server emits on every checkpoint
/// tick. This is fit the same way as RSS (warmup-excluded OLS slope,
/// converted to MB/hour) and FAILs if the fitted growth rate exceeds
/// `thresholds.max_txlog_growth_mb_per_hour` — the direct proof that
/// disk is NOT growing unboundedly, which checkpoint/reclaim *activity*
/// counters alone cannot prove (a reclaim that's losing the race to the
/// write rate still fires checkpoints and frees segments each time, so
/// activity-only would falsely PASS while disk climbs forever).
///
/// The pre-existing proxies are kept as a second, independent signal:
/// checkpoints must have fired at least once over the window AND at
/// least one reclaim event must have happened. This catches a distinct
/// failure mode from the byte bound — checkpointing/reclaim itself
/// broken/disabled — which a byte-growth fit alone might not catch on
/// a short or slow-writing window (e.g. too little data written yet
/// for the slope to look alarming, but reclaim never ran at all).
/// Overall pass requires both the byte bound AND the activity checks.
pub fn check_txlog_bounded(
    checkpoint_series: &[Sample],
    reclaimed_series: &[Sample],
    txlog_bytes_series: &[Sample],
    thresholds: &Thresholds,
) -> CriterionResult {
    const NAME: &str = "txlog_bounded";
    if checkpoint_series.is_empty() || reclaimed_series.is_empty() {
        return CriterionResult::new(NAME, "no samples", "n/a", false).with_note(
            "need cq_txlog_checkpoint_total and cq_txlog_segments_reclaimed_total samples",
        );
    }
    let checkpoints = counter_delta(checkpoint_series);
    let reclaimed = counter_delta(reclaimed_series);
    let activity_pass = checkpoints >= 1.0 && reclaimed >= 1.0;

    let growth_mb_per_hour = match post_warmup_growth_mb_per_hour(
        txlog_bytes_series,
        thresholds.warmup_fraction,
        NAME,
        "need >= 2 cq_txlog_bytes samples to fit a growth slope",
        "window too short relative to warmup_fraction to fit a txlog growth slope",
    ) {
        Ok(v) => v,
        Err(mut result) => {
            // Preserve the checkpoint/reclaimed measured values in the
            // note even when the byte series itself is insufficient, so
            // the failure is diagnosable without re-running.
            result.note = format!(
                "{} (checkpoints={:.0} reclaimed={:.0})",
                result.note, checkpoints, reclaimed
            );
            return result;
        }
    };
    let bytes_bound_pass = growth_mb_per_hour <= thresholds.max_txlog_growth_mb_per_hour;
    let pass = activity_pass && bytes_bound_pass;

    let mut result = CriterionResult::new(
        NAME,
        format!(
            "growth={:.2} MB/hour checkpoints={:.0} reclaimed={:.0}",
            growth_mb_per_hour, checkpoints, reclaimed
        ),
        format!(
            "growth <= {:.2} MB/hour and checkpoints >= 1 and reclaimed >= 1",
            thresholds.max_txlog_growth_mb_per_hour
        ),
        pass,
    );
    if !bytes_bound_pass {
        result.note = "cq_txlog_bytes grew faster than the configured bound — reclaim is \
                       losing the race against the write rate, so disk is growing \
                       unboundedly even though checkpoint/reclaim activity may look healthy"
            .to_string();
    } else if !activity_pass {
        result.note = "cq_txlog_bytes stayed within the growth bound, but checkpoint/reclaim \
                       activity was missing over the window (checkpoints >= 1 and reclaimed \
                       >= 1 required) — checkpointing may be disabled or the window too short \
                       to observe a reclaim"
            .to_string();
    }
    result
}

/// Criterion 4: p99 publish latency under target.
///
/// `p99_series` is expected to already be the `quantile="0.99"` series
/// of the exported `cq_publish_latency_us` metric, maxed across the
/// `topic` label (see [`run`]'s PromQL). The server records
/// `cq_publish_latency_us` via `metrics::histogram!`
/// (`crates/cq-transport/src/router.rs`), but the Prometheus exporter
/// (`crates/cq-server/src/main.rs`, `PrometheusBuilder::new()`) is
/// built with no bucket configuration, so `metrics-exporter-prometheus`
/// renders histograms as Prometheus *summaries* — i.e. it exports
/// `cq_publish_latency_us{quantile="0.99", topic="..."}` (plus `_sum`
/// / `_count`), NOT a `cq_publish_latency_us_bucket` series. A
/// `histogram_quantile(0.99, rate(cq_publish_latency_us_bucket[...]))`
/// query therefore matches nothing against this server; the quantile
/// is already computed server-side into the `quantile` label, so this
/// function just takes the max observed value across the window (the
/// worst moment, which is what a soak gate cares about) and compares
/// it to the target. `0.99` is one of the exporter's default quantiles
/// (`[0.0, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0]`, set in
/// `PrometheusBuilder::new()`), so it's always present once any
/// `delta_publish` has happened.
pub fn check_p99_publish_latency(
    p99_series: &[Sample],
    thresholds: &Thresholds,
) -> CriterionResult {
    const NAME: &str = "p99_publish_latency";
    let valid: Vec<f64> = p99_series
        .iter()
        .map(|(_, v)| *v)
        .filter(|v| v.is_finite())
        .collect();
    if valid.is_empty() {
        return CriterionResult::new(NAME, "no samples", "n/a", false).with_note(
            "need cq_publish_latency_us histogram samples (histogram_quantile(0.99, ...))",
        );
    }
    let max_p99 = valid.iter().cloned().fold(f64::MIN, f64::max);
    let pass = max_p99 <= thresholds.max_p99_publish_latency_us;

    CriterionResult::new(
        NAME,
        format!("{:.0} us", max_p99),
        format!("<= {:.0} us", thresholds.max_p99_publish_latency_us),
        pass,
    )
}

/// Run all four criteria and assemble the final report. Pure — takes
/// pre-fetched series, does no I/O.
#[allow(clippy::too_many_arguments)]
pub fn analyze(
    rss_series: &[Sample],
    deltas_dropped_series: &[Sample],
    subscription_dropped_series: &[Sample],
    delivered_series: &[Sample],
    checkpoint_series: &[Sample],
    reclaimed_series: &[Sample],
    txlog_bytes_series: &[Sample],
    p99_series: &[Sample],
    thresholds: &Thresholds,
) -> SoakReport {
    SoakReport {
        criteria: vec![
            check_rss_slope(rss_series, thresholds),
            check_drop_ratio(
                deltas_dropped_series,
                subscription_dropped_series,
                delivered_series,
                thresholds,
            ),
            check_txlog_bounded(
                checkpoint_series,
                reclaimed_series,
                txlog_bytes_series,
                thresholds,
            ),
            check_p99_publish_latency(p99_series, thresholds),
        ],
    }
}

// ---------------------------------------------------------------------
// Impure half: Prometheus HTTP API client.
// ---------------------------------------------------------------------

/// A resolved query window: Unix-epoch start/end seconds + a scrape
/// step. `end - start` should cover the soak run; `step` should be >=
/// the scrape interval (10s in `tests/cloud/prometheus-soak.yml`).
#[derive(Debug, Clone, Copy)]
pub struct Window {
    pub start_unix: f64,
    pub end_unix: f64,
    pub step_secs: f64,
}

impl Window {
    /// The last `minutes` minutes, ending now.
    pub fn last_minutes(minutes: f64, step_secs: f64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs_f64();
        Self {
            start_unix: now - minutes * 60.0,
            end_unix: now,
            step_secs,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PromRangeResponse {
    status: String,
    data: Option<PromRangeData>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PromRangeData {
    result: Vec<PromRangeResult>,
}

#[derive(Debug, Deserialize)]
struct PromRangeResult {
    values: Vec<(f64, String)>,
}

/// Query `/api/v1/query_range` for `promql` over `window` and return
/// the first result series as `(timestamp, value)` samples. Prometheus
/// range-vector values are `[unix_ts, "string_value"]` pairs; NaN /
/// non-numeric strings (e.g. from a bucket with no samples yet) are
/// skipped.
pub async fn fetch_range(
    client: &reqwest::Client,
    prometheus_base_url: &str,
    promql: &str,
    window: Window,
) -> Result<Vec<Sample>> {
    let url = format!(
        "{}/api/v1/query_range",
        prometheus_base_url.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[
            ("query", promql.to_string()),
            ("start", format!("{:.3}", window.start_unix)),
            ("end", format!("{:.3}", window.end_unix)),
            ("step", format!("{:.3}", window.step_secs)),
        ])
        .send()
        .await
        .with_context(|| format!("query_range request failed: {url} query={promql}"))?;

    let status = resp.status();
    let body: PromRangeResponse = resp
        .json()
        .await
        .with_context(|| format!("query_range response wasn't valid JSON (status {status})"))?;

    if body.status != "success" {
        bail!(
            "prometheus query_range failed: {} (query={promql})",
            body.error.unwrap_or_else(|| "unknown error".into())
        );
    }
    let data = body
        .data
        .context("prometheus query_range: missing data field")?;
    let Some(first) = data.result.into_iter().next() else {
        // No series matched — return empty; callers surface this as a
        // "no samples" criterion failure rather than an error, since an
        // absent metric is itself diagnostic (e.g. wrong topic name).
        return Ok(Vec::new());
    };
    let samples = first
        .values
        .into_iter()
        .filter_map(|(ts, v)| {
            v.parse::<f64>()
                .ok()
                .filter(|f| f.is_finite())
                .map(|f| (ts, f))
        })
        .collect();
    Ok(samples)
}

/// Config for a live analyzer run against a real Prometheus.
#[derive(Debug, Clone)]
pub struct AnalyzeConfig {
    pub prometheus_url: String,
    pub window: Window,
    pub thresholds: Thresholds,
}

/// Build an [`AnalyzeConfig`] from CLI-shaped arguments. `start`/`end`
/// (both `Some`) take precedence over `last_minutes`; otherwise the
/// window is the last `last_minutes` minutes ending now.
#[allow(clippy::too_many_arguments)]
pub fn analyze_config_from_args(
    prometheus_url: String,
    last_minutes: f64,
    start: Option<f64>,
    end: Option<f64>,
    step_secs: f64,
    max_rss_growth_mb_per_hour: f64,
    warmup_fraction: f64,
    max_drop_ratio: f64,
    max_p99_publish_latency_us: f64,
    max_txlog_growth_mb_per_hour: f64,
) -> AnalyzeConfig {
    let step = if step_secs <= 0.0 { 10.0 } else { step_secs };
    let window = match (start, end) {
        (Some(s), Some(e)) => Window {
            start_unix: s,
            end_unix: e,
            step_secs: step,
        },
        _ => Window::last_minutes(last_minutes, step),
    };
    AnalyzeConfig {
        prometheus_url,
        window,
        thresholds: Thresholds {
            max_rss_growth_mb_per_hour,
            warmup_fraction,
            max_drop_ratio,
            max_p99_publish_latency_us,
            max_txlog_growth_mb_per_hour,
        },
    }
}

/// Fetch all six series from Prometheus and run [`analyze`]. This is
/// the only function in the module that talks to a network — every
/// verdict computation it calls into is separately unit-tested with
/// synthetic data.
pub async fn run(cfg: &AnalyzeConfig) -> Result<SoakReport> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("building reqwest client")?;

    let rss = fetch_range(
        &client,
        &cfg.prometheus_url,
        "cq_process_rss_bytes",
        cfg.window,
    )
    .await
    .context("fetching cq_process_rss_bytes")?;
    let deltas_dropped = fetch_range(
        &client,
        &cfg.prometheus_url,
        "sum(cq_deltas_dropped_total)",
        cfg.window,
    )
    .await
    .context("fetching cq_deltas_dropped_total")?;
    // Conflated topics (any topic with `conflation_ms` set, e.g.
    // `/positions` — the soak's primary topic) drop via a differently
    // named counter: the conflator's flush loop
    // (`crates/cq-transport/src/session.rs`) returns before
    // `cq_deltas_dropped_total`/`cq_deltas_delivered_total` are ever
    // touched, so its drops only show up here. Without this,
    // `check_drop_ratio` would be a near-no-op for the shipped
    // conflated topology.
    let subscription_dropped = fetch_range(
        &client,
        &cfg.prometheus_url,
        "sum(cq_subscription_dropped_total)",
        cfg.window,
    )
    .await
    .context("fetching cq_subscription_dropped_total")?;
    let delivered = fetch_range(
        &client,
        &cfg.prometheus_url,
        "sum(cq_deltas_delivered_total)",
        cfg.window,
    )
    .await
    .context("fetching cq_deltas_delivered_total")?;
    let checkpoints = fetch_range(
        &client,
        &cfg.prometheus_url,
        "cq_txlog_checkpoint_total",
        cfg.window,
    )
    .await
    .context("fetching cq_txlog_checkpoint_total")?;
    let reclaimed = fetch_range(
        &client,
        &cfg.prometheus_url,
        "cq_txlog_segments_reclaimed_total",
        cfg.window,
    )
    .await
    .context("fetching cq_txlog_segments_reclaimed_total")?;
    // Sum across topics: cq_txlog_bytes is per-topic (labeled `topic=`),
    // and the byte-bound check cares about total on-disk txlog size, not
    // any single topic's.
    let txlog_bytes = fetch_range(
        &client,
        &cfg.prometheus_url,
        "sum(cq_txlog_bytes)",
        cfg.window,
    )
    .await
    .context("fetching cq_txlog_bytes")?;
    // cq_publish_latency_us is recorded via metrics::histogram!, but the
    // Prometheus exporter is installed with no bucket configuration
    // (PrometheusBuilder::new() in crates/cq-server/src/main.rs), so
    // metrics-exporter-prometheus renders it as a Prometheus *summary*:
    // cq_publish_latency_us{quantile="0.99", topic="..."} (+ _sum/_count),
    // not a cq_publish_latency_us_bucket series. There is no
    // _bucket series for histogram_quantile() to consume, so the
    // quantile is already computed server-side in the `quantile` label
    // — query it directly and take the max across topics. 0.99 is one
    // of the exporter's default quantiles, so it's always present once
    // any delta_publish has happened.
    let p99_query = "max(cq_publish_latency_us{quantile=\"0.99\"})";
    let p99 = fetch_range(&client, &cfg.prometheus_url, p99_query, cfg.window)
        .await
        .context("fetching cq_publish_latency_us p99")?;

    Ok(analyze(
        &rss,
        &deltas_dropped,
        &subscription_dropped,
        &delivered,
        &checkpoints,
        &reclaimed,
        &txlog_bytes,
        &p99,
        &cfg.thresholds,
    ))
}

// ---------------------------------------------------------------------
// Unit tests — synthetic series only, no Prometheus required.
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn thresholds() -> Thresholds {
        Thresholds::default()
    }

    // ---- linear_fit ----------------------------------------------------

    #[test]
    fn linear_fit_recovers_known_slope() {
        // y = 2x + 5 exactly.
        let points: Vec<Sample> = (0..10).map(|i| (i as f64, 2.0 * i as f64 + 5.0)).collect();
        let (slope, intercept) = linear_fit(&points);
        assert!((slope - 2.0).abs() < 1e-9, "slope={slope}");
        assert!((intercept - 5.0).abs() < 1e-9, "intercept={intercept}");
    }

    #[test]
    fn linear_fit_flat_series_has_zero_slope() {
        let points: Vec<Sample> = (0..10).map(|i| (i as f64, 100.0)).collect();
        let (slope, _) = linear_fit(&points);
        assert!(slope.abs() < 1e-9, "slope={slope}");
    }

    // ---- criterion 1: RSS slope -----------------------------------------

    #[test]
    fn rss_flat_series_passes() {
        // Flat 500MB the whole window (with tiny scrape-jitter noise).
        let base = 500.0 * 1024.0 * 1024.0;
        let series: Vec<Sample> = (0..360)
            .map(|i| (i as f64 * 10.0, base + ((i % 3) as f64 - 1.0) * 1024.0))
            .collect();
        let result = check_rss_slope(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn rss_leaking_series_fails() {
        // Starts at 200MB, grows linearly to 200MB + 500MB over 1 hour
        // (3600s) => ~500MB/hour growth, way over the 50MB/hour default
        // threshold. 361 samples at 10s step = 3600s span.
        let base = 200.0 * 1024.0 * 1024.0;
        let growth_per_sec = 500.0 * 1024.0 * 1024.0 / 3600.0;
        let series: Vec<Sample> = (0..361)
            .map(|i| (i as f64 * 10.0, base + growth_per_sec * (i as f64 * 10.0)))
            .collect();
        let result = check_rss_slope(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn rss_warmup_ramp_then_flat_passes() {
        // First 10% of the window ramps up sharply (process startup
        // allocation), then goes flat. The warmup-exclusion should mean
        // this passes even though the *whole* series has a nonzero
        // slope.
        let mut series = Vec::new();
        // Warmup: 0..=60s, RSS ramps from 50MB to 300MB (would be a
        // massive slope if included).
        for i in 0..=6 {
            let t = i as f64 * 10.0;
            let rss = (50.0 + (250.0 / 6.0) * i as f64) * 1024.0 * 1024.0;
            series.push((t, rss));
        }
        // Steady state: 70..=600s, flat at 300MB.
        for i in 7..=60 {
            let t = i as f64 * 10.0;
            series.push((t, 300.0 * 1024.0 * 1024.0));
        }
        let result = check_rss_slope(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn rss_insufficient_samples_fails_closed() {
        let result = check_rss_slope(&[(0.0, 100.0)], &thresholds());
        assert_eq!(result.verdict, Verdict::Fail);
    }

    // ---- criterion 2: drop ratio -----------------------------------------

    #[test]
    fn bounded_drops_pass() {
        // Delivered grows fast; dropped grows slowly => small ratio.
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        // 1% of delivered by the end.
        let dropped: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 10.0))
            .collect();
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn runaway_drops_fail() {
        // Dropped grows just as fast as delivered => ratio ~1.0, way
        // over the 5% default threshold.
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 950.0))
            .collect();
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn zero_drops_pass() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = (0..10).map(|i| (i as f64 * 10.0, 0.0)).collect();
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    /// The load-bearing regression this fix exists for: `cq_deltas_dropped_total`
    /// is a Prometheus counter that Prometheus/the exporter only starts
    /// serving after its first `increment()` call — zero drops in the
    /// window means the series is entirely ABSENT, not present-with-zero
    /// samples. An absent dropped series must be treated as zero drops
    /// (PASS), never as a missing-metric FAIL or a crash.
    #[test]
    fn absent_dropped_counter_treated_as_zero_passes() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = Vec::new(); // absent — no drops ever happened.
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
        assert!(
            result.measured.contains("dropped=0"),
            "expected dropped=0 in measured value: {:?}",
            result
        );
        assert!(
            !result.note.is_empty(),
            "expected a note explaining the absent counter: {:?}",
            result
        );
    }

    /// Complement: an absent `cq_deltas_delivered_total` (which should
    /// normally always be present once any publish has happened) is a
    /// distinct case — insufficient data, not a healthy zero. This must
    /// degrade to a soft FAIL-with-note ("insufficient data"), not panic
    /// or silently pass.
    #[test]
    fn absent_delivered_counter_is_insufficient_data() {
        let delivered: Vec<Sample> = Vec::new();
        let dropped: Vec<Sample> = Vec::new();
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
        assert!(
            result.note.to_lowercase().contains("insufficient"),
            "expected an insufficient-data note: {:?}",
            result
        );
    }

    /// The load-bearing regression this fix exists for: topics with
    /// `conflation_ms` set (e.g. `/positions`, the soak's primary topic)
    /// drop via `cq_subscription_dropped_total`, NOT
    /// `cq_deltas_dropped_total` — the conflator path
    /// (`crates/cq-transport/src/session.rs`) returns before
    /// `cq_deltas_delivered_total`/`cq_deltas_dropped_total` are ever
    /// touched. If `cq_deltas_dropped_total` is absent (as it would be
    /// for an all-conflated soak) but `cq_subscription_dropped_total` is
    /// climbing, the ratio must reflect that — not silently read as
    /// zero drops.
    #[test]
    fn subscription_dropped_only_computes_correct_ratio() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        // cq_deltas_dropped_total: absent (conflated route never
        // increments it).
        let deltas_dropped: Vec<Sample> = Vec::new();
        // cq_subscription_dropped_total: climbs to 200 by the end — 200 /
        // 9000 delivered =~ 0.022, under the 5% default threshold, so
        // this should PASS, but on a *nonzero, correctly computed*
        // ratio, not a vacuous 0/9000.
        let subscription_dropped: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 20.0))
            .collect();
        let result = check_drop_ratio(
            &deltas_dropped,
            &subscription_dropped,
            &delivered,
            &thresholds(),
        );
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
        assert!(
            result.measured.contains("dropped=180"),
            "expected the subscription-dropped delta (180, i.e. 9*20) to be counted, not read as \
             zero: {:?}",
            result
        );
    }

    /// Both drop counters absent (nothing ever dropped, on either route)
    /// must still be a healthy PASS at ratio 0 — the "absent counter =
    /// 0, not error" semantics apply to `cq_subscription_dropped_total`
    /// exactly as they already do for `cq_deltas_dropped_total`.
    #[test]
    fn both_drop_counters_absent_treated_as_zero_passes() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let deltas_dropped: Vec<Sample> = Vec::new();
        let subscription_dropped: Vec<Sample> = Vec::new();
        let result = check_drop_ratio(
            &deltas_dropped,
            &subscription_dropped,
            &delivered,
            &thresholds(),
        );
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
        assert!(
            result.measured.contains("dropped=0"),
            "expected dropped=0 in measured value: {:?}",
            result
        );
    }

    /// Runaway `cq_subscription_dropped_total` alone (deltas-dropped
    /// stays at zero/absent the whole time — an all-conflated soak)
    /// must FAIL. This is the shape a real shedding conflated topology
    /// would produce, and is exactly what the pre-fix analyzer would
    /// have missed (it only looked at `cq_deltas_dropped_total`).
    #[test]
    fn runaway_subscription_drops_alone_fail() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let deltas_dropped: Vec<Sample> = Vec::new();
        // Grows just as fast as delivered => ratio ~1.0, way over 5%.
        let subscription_dropped: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 950.0))
            .collect();
        let result = check_drop_ratio(
            &deltas_dropped,
            &subscription_dropped,
            &delivered,
            &thresholds(),
        );
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn drop_ratio_handles_counter_reset() {
        // Delivered counter resets partway (server restart) then keeps
        // climbing — counter_delta should treat the reset as new growth
        // from zero, not as a negative delta that would understate the
        // denominator and (perversely) make the ratio look worse.
        let delivered: Vec<Sample> = vec![
            (0.0, 5000.0),
            (10.0, 8000.0),
            (20.0, 1000.0),
            (30.0, 4000.0),
        ];
        let dropped: Vec<Sample> = vec![(0.0, 10.0), (10.0, 20.0), (20.0, 0.0), (30.0, 10.0)];
        let result = check_drop_ratio(&dropped, &[], &delivered, &thresholds());
        // delivered_delta = 3000 + 1000 + 3000 = 7000; dropped_delta = 10 + 0(reset->0 counted as 0) + 10 = 20
        // ratio well under 5%.
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    // ---- criterion 3: txlog bounded ---------------------------------------

    /// A sawtooth `cq_txlog_bytes` series: ramps up as segments are
    /// written, then drops back down on each reclaim — net-flat over the
    /// full window. Reused across tests as the "healthy" byte series.
    fn sawtooth_txlog_bytes(n: usize, step_secs: f64, base: f64, sawtooth_amplitude: f64) -> Vec<Sample> {
        (0..n)
            .map(|i| {
                let t = i as f64 * step_secs;
                // Triangle wave with period 10 samples: up for 5, down for 5.
                let phase = i % 10;
                let frac = if phase < 5 {
                    phase as f64 / 5.0
                } else {
                    (10 - phase) as f64 / 5.0
                };
                (t, base + sawtooth_amplitude * frac)
            })
            .collect()
    }

    #[test]
    fn reclaim_events_present_pass() {
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 1.0), (30.0, 2.0)];
        let txlog_bytes: Vec<Sample> = vec![
            (0.0, 100.0),
            (10.0, 200.0),
            (20.0, 50.0),
            (30.0, 150.0),
        ];
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn no_reclaim_events_fail_txlog_unbounded() {
        // Checkpoints fire, but nothing ever gets reclaimed — segments
        // accumulate forever even though checkpointing "works". Byte
        // series is flat here just to isolate this failure mode from the
        // byte-growth one (tested separately below).
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0), (30.0, 0.0)];
        let txlog_bytes: Vec<Sample> = vec![(0.0, 100.0), (10.0, 100.0), (20.0, 100.0), (30.0, 100.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn no_checkpoints_fail_txlog_unbounded() {
        // Reclaimed stays at 0 because checkpointing itself never ran.
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)];
        let txlog_bytes: Vec<Sample> = vec![(0.0, 100.0), (10.0, 100.0), (20.0, 100.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn txlog_missing_series_fails_closed() {
        let result = check_txlog_bounded(&[], &[], &[], &thresholds());
        assert_eq!(result.verdict, Verdict::Fail);
    }

    /// The load-bearing regression this criterion exists to catch:
    /// reclaim fires and frees segments every checkpoint (activity looks
    /// perfectly healthy), but the write rate outpaces it — total disk
    /// still grows linearly and unboundedly. Activity-only checking would
    /// falsely PASS this; the byte-growth fit must FAIL it.
    #[test]
    fn linearly_growing_txlog_bytes_fails_even_with_healthy_activity() {
        let checkpoints: Vec<Sample> = (0..361).map(|i| (i as f64 * 10.0, i as f64)).collect();
        let reclaimed: Vec<Sample> = (0..361).map(|i| (i as f64 * 10.0, i as f64)).collect();
        // Starts at 100MB, grows linearly to 100MB + 500MB over 1 hour
        // (3600s) => ~500MB/hour, way over the 50MB/hour default
        // threshold — same shape as the rss_leaking_series_fails test.
        let base = 100.0 * 1024.0 * 1024.0;
        let growth_per_sec = 500.0 * 1024.0 * 1024.0 / 3600.0;
        let txlog_bytes: Vec<Sample> = (0..361)
            .map(|i| (i as f64 * 10.0, base + growth_per_sec * (i as f64 * 10.0)))
            .collect();
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(
            result.verdict,
            Verdict::Fail,
            "unbounded byte growth must fail even though checkpoint+reclaim activity is healthy: {:?}",
            result
        );
    }

    /// Complement: a sawtooth `cq_txlog_bytes` series (grows as segments
    /// are written, drops on each reclaim) with net-flat growth over the
    /// window, plus healthy activity, must PASS — this is what a
    /// correctly-bounded txlog looks like.
    #[test]
    fn sawtooth_txlog_bytes_with_activity_passes() {
        let checkpoints: Vec<Sample> = (0..60).map(|i| (i as f64 * 10.0, i as f64)).collect();
        let reclaimed: Vec<Sample> = (0..60).map(|i| (i as f64 * 10.0, i as f64)).collect();
        let txlog_bytes = sawtooth_txlog_bytes(60, 10.0, 100.0 * 1024.0 * 1024.0, 20.0 * 1024.0 * 1024.0);
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    /// A flat `cq_txlog_bytes` series (no growth at all) must also PASS —
    /// the simplest bounded case.
    #[test]
    fn flat_txlog_bytes_with_activity_passes() {
        let checkpoints: Vec<Sample> = (0..30).map(|i| (i as f64 * 10.0, i as f64)).collect();
        let reclaimed: Vec<Sample> = (0..30).map(|i| (i as f64 * 10.0, i as f64)).collect();
        let txlog_bytes: Vec<Sample> = (0..30)
            .map(|i| (i as f64 * 10.0, 200.0 * 1024.0 * 1024.0))
            .collect();
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    /// Warmup exclusion applies to the byte fit too: a sharp initial
    /// ramp (first checkpoint building up the first segments from an
    /// empty log) followed by sawtooth/flat steady state must PASS, the
    /// same way `rss_warmup_ramp_then_flat_passes` does for RSS.
    #[test]
    fn txlog_bytes_warmup_ramp_then_flat_passes() {
        let mut txlog_bytes = Vec::new();
        for i in 0..=6 {
            let t = i as f64 * 10.0;
            let bytes = (10.0 + (200.0 / 6.0) * i as f64) * 1024.0 * 1024.0;
            txlog_bytes.push((t, bytes));
        }
        for i in 7..=60 {
            let t = i as f64 * 10.0;
            txlog_bytes.push((t, 210.0 * 1024.0 * 1024.0));
        }
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (600.0, 5.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (600.0, 3.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed, &txlog_bytes, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    // ---- criterion 4: p99 publish latency ----------------------------------

    #[test]
    fn p99_under_target_passes() {
        let series: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect(); // 5ms
        let result = check_p99_publish_latency(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn p99_over_target_fails() {
        let series: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 80_000.0)).collect(); // 80ms > 50ms default
        let result = check_p99_publish_latency(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn p99_spike_in_otherwise_good_window_fails() {
        // Mostly under target, but one spike above it — analyzer should
        // key off the max (worst moment), not the average.
        let mut series: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect();
        series.push((200.0, 200_000.0));
        let result = check_p99_publish_latency(&series, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    /// The load-bearing regression this fix exists for: `cq_publish_latency_us`
    /// is exported as a Prometheus *summary* (no bucket config on the
    /// exporter — see `run`'s doc comment), so the analyzer queries
    /// `max(cq_publish_latency_us{quantile="0.99"})` instead of
    /// `histogram_quantile(0.99, rate(..._bucket[...]))`. This test feeds
    /// a synthetic `query_range` JSON payload shaped exactly like what
    /// Prometheus returns for that query — a single result series of
    /// `[unix_ts, "string_value"]` pairs, the same shape `fetch_range`
    /// parses via `PromRangeResponse` — through the real deserialization
    /// path (no network) and confirms it produces the right p99 value
    /// and passing verdict.
    #[test]
    fn summary_quantile_query_result_parses_into_p99_value() {
        // Shaped exactly like a real Prometheus query_range response body
        // for `max(cq_publish_latency_us{quantile="0.99"})`: values are
        // [unix_ts, "string"] pairs, matching PromRangeResult.values.
        let body = r#"{
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {},
                        "values": [
                            [1700000000.000, "1200.5"],
                            [1700000010.000, "1350.0"],
                            [1700000020.000, "980.25"],
                            [1700000030.000, "NaN"]
                        ]
                    }
                ]
            }
        }"#;

        let parsed: PromRangeResponse =
            serde_json::from_str(body).expect("synthetic payload must deserialize");
        assert_eq!(parsed.status, "success");
        let data = parsed.data.expect("data field must be present");
        let first = data
            .result
            .into_iter()
            .next()
            .expect("one result series expected");

        // Mirror fetch_range's own filter_map: parse to f64, skip
        // non-finite (the "NaN" entry, matching a bucket/quantile with
        // no samples yet).
        let samples: Vec<Sample> = first
            .values
            .into_iter()
            .filter_map(|(ts, v)| {
                v.parse::<f64>()
                    .ok()
                    .filter(|f| f.is_finite())
                    .map(|f| (ts, f))
            })
            .collect();

        assert_eq!(
            samples,
            vec![
                (1700000000.0, 1200.5),
                (1700000010.0, 1350.0),
                (1700000020.0, 980.25),
            ],
            "NaN sample must be filtered out same as fetch_range does"
        );

        let result = check_p99_publish_latency(&samples, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
        assert!(
            result.measured.contains("1350"),
            "expected max p99 value 1350us in measured output: {:?}",
            result
        );
    }

    // ---- full report / overall verdict -------------------------------------

    #[test]
    fn overall_pass_when_all_criteria_pass() {
        let rss: Vec<Sample> = (0..60)
            .map(|i| (i as f64 * 10.0, 500.0 * 1024.0 * 1024.0))
            .collect();
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = (0..10).map(|i| (i as f64 * 10.0, i as f64 * 5.0)).collect();
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (30.0, 2.0)];
        let txlog_bytes: Vec<Sample> = (0..60)
            .map(|i| (i as f64 * 10.0, 300.0 * 1024.0 * 1024.0))
            .collect();
        let p99: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect();

        let report = analyze(
            &rss,
            &dropped,
            &[],
            &delivered,
            &checkpoints,
            &reclaimed,
            &txlog_bytes,
            &p99,
            &thresholds(),
        );
        assert_eq!(report.overall(), Verdict::Pass, "{}", report.render());
        assert_eq!(report.criteria.len(), 4);
    }

    #[test]
    fn overall_fail_when_any_criterion_fails() {
        // Everything good except a leaking RSS series.
        let base = 200.0 * 1024.0 * 1024.0;
        let growth_per_sec = 500.0 * 1024.0 * 1024.0 / 3600.0;
        let rss: Vec<Sample> = (0..361)
            .map(|i| (i as f64 * 10.0, base + growth_per_sec * (i as f64 * 10.0)))
            .collect();
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = (0..10).map(|i| (i as f64 * 10.0, i as f64 * 5.0)).collect();
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (30.0, 2.0)];
        let txlog_bytes: Vec<Sample> = (0..60)
            .map(|i| (i as f64 * 10.0, 300.0 * 1024.0 * 1024.0))
            .collect();
        let p99: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect();

        let report = analyze(
            &rss,
            &dropped,
            &[],
            &delivered,
            &checkpoints,
            &reclaimed,
            &txlog_bytes,
            &p99,
            &thresholds(),
        );
        assert_eq!(report.overall(), Verdict::Fail);
        let rss_criterion = report
            .criteria
            .iter()
            .find(|c| c.name == "rss_slope")
            .unwrap();
        assert_eq!(rss_criterion.verdict, Verdict::Fail);
    }

    #[test]
    fn render_contains_soak_verdict_line() {
        let report = SoakReport {
            criteria: vec![CriterionResult::new("x", "1", "<= 2", true)],
        };
        let text = report.render();
        assert!(text.contains("SOAK VERDICT: PASS"));
    }

    // ---- Window ----------------------------------------------------------

    #[test]
    fn window_last_minutes_spans_correct_range() {
        let w = Window::last_minutes(30.0, 10.0);
        assert!((w.end_unix - w.start_unix - 1800.0).abs() < 1.0);
        assert_eq!(w.step_secs, 10.0);
    }
}

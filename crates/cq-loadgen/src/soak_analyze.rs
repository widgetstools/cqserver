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
//! 2. **Delta drops bounded** — `cq_deltas_dropped_total` vs
//!    `cq_deltas_delivered_total`. The slow-consumer policy *causes*
//!    some drops by design (see `crates/cq-transport/src/delivery.rs`),
//!    so the check is that the drop ratio over the window stays under
//!    a configurable threshold, not that drops are zero.
//! 3. **Txlog bounded** — `cq_txlog_checkpoint_total` must increase
//!    over the window (checkpoints fired) AND
//!    `cq_txlog_segments_reclaimed_total` must show at least one
//!    reclaim (segments were actually freed), so disk sawtooths
//!    instead of growing monotonically. There is currently no
//!    txlog-bytes-on-disk gauge exported by the server (grepped
//!    `crates/cq-server`, `crates/cq-txlog` — only the checkpoint/
//!    reclaim/error counters and a lock-hold-time histogram exist), so
//!    this criterion cannot directly assert a bounded byte count; a
//!    node-exporter-style disk-usage gauge or a dedicated txlog-bytes
//!    gauge is a follow-up if stronger proof is needed.
//! 4. **p99 publish latency under target** — `cq_publish_latency_us`
//!    histogram, `histogram_quantile(0.99, ...)` over the window, must
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
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            max_rss_growth_mb_per_hour: 50.0,
            warmup_fraction: 0.10,
            max_drop_ratio: 0.05,
            max_p99_publish_latency_us: 50_000.0,
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

/// Criterion 1: RSS slope ≈ 0 after warmup exclusion.
///
/// `rss_series` is `(timestamp_secs, rss_bytes)` samples across the
/// whole window, in chronological order. The first `warmup_fraction`
/// of the window (by time span, not sample count) is discarded before
/// fitting, so startup allocation/JIT-style ramp-up doesn't get
/// mistaken for a leak.
pub fn check_rss_slope(rss_series: &[Sample], thresholds: &Thresholds) -> CriterionResult {
    const NAME: &str = "rss_slope";
    if rss_series.len() < 2 {
        return CriterionResult::new(NAME, "insufficient samples", "n/a", false)
            .with_note("need >= 2 cq_process_rss_bytes samples to fit a slope");
    }
    let t0 = rss_series.first().unwrap().0;
    let t1 = rss_series.last().unwrap().0;
    let span = t1 - t0;
    let warmup_cutoff = t0 + span * thresholds.warmup_fraction;
    let post_warmup: Vec<Sample> = rss_series
        .iter()
        .copied()
        .filter(|(t, _)| *t >= warmup_cutoff)
        .collect();

    if post_warmup.len() < 2 {
        return CriterionResult::new(NAME, "insufficient post-warmup samples", "n/a", false)
            .with_note("window too short relative to warmup_fraction to fit a slope");
    }

    let (slope_bytes_per_sec, _intercept) = linear_fit(&post_warmup);
    let slope_mb_per_hour = slope_bytes_per_sec * 3600.0 / (1024.0 * 1024.0);
    let pass = slope_mb_per_hour <= thresholds.max_rss_growth_mb_per_hour;

    CriterionResult::new(
        NAME,
        format!("{:.2} MB/hour", slope_mb_per_hour),
        format!("<= {:.2} MB/hour", thresholds.max_rss_growth_mb_per_hour),
        pass,
    )
}

/// Criterion 2: delta drops bounded relative to delivered volume.
///
/// Both series are Prometheus counters (monotonic); this function
/// takes the `last - first` delta over the window for each and
/// computes `dropped_delta / max(delivered_delta, 1)`. Slow-consumer
/// drops are expected and healthy — the check is a *ratio* threshold,
/// not zero-drops.
pub fn check_drop_ratio(
    dropped_series: &[Sample],
    delivered_series: &[Sample],
    thresholds: &Thresholds,
) -> CriterionResult {
    const NAME: &str = "delta_drop_ratio";
    if dropped_series.is_empty() || delivered_series.is_empty() {
        return CriterionResult::new(NAME, "no samples", "n/a", false)
            .with_note("need cq_deltas_dropped_total and cq_deltas_delivered_total samples");
    }
    let dropped_delta = counter_delta(dropped_series);
    let delivered_delta = counter_delta(delivered_series);
    let ratio = dropped_delta / delivered_delta.max(1.0);
    let pass = ratio <= thresholds.max_drop_ratio;

    CriterionResult::new(
        NAME,
        format!(
            "{:.4} (dropped={:.0}, delivered={:.0})",
            ratio, dropped_delta, delivered_delta
        ),
        format!("<= {:.4}", thresholds.max_drop_ratio),
        pass,
    )
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

/// Criterion 3: txlog bounded via checkpoint + reclaim counters.
///
/// No txlog-bytes-on-disk gauge exists in the server today (see module
/// doc comment), so this asserts the two proxies that *do* exist:
/// checkpoints fired at least once, and at least one reclaim event
/// happened (segments were actually freed, not just checkpointed).
pub fn check_txlog_bounded(
    checkpoint_series: &[Sample],
    reclaimed_series: &[Sample],
) -> CriterionResult {
    const NAME: &str = "txlog_bounded";
    if checkpoint_series.is_empty() || reclaimed_series.is_empty() {
        return CriterionResult::new(NAME, "no samples", "n/a", false).with_note(
            "need cq_txlog_checkpoint_total and cq_txlog_segments_reclaimed_total samples",
        );
    }
    let checkpoints = counter_delta(checkpoint_series);
    let reclaimed = counter_delta(reclaimed_series);
    let pass = checkpoints >= 1.0 && reclaimed >= 1.0;

    let mut result = CriterionResult::new(
        NAME,
        format!("checkpoints={:.0} reclaimed={:.0}", checkpoints, reclaimed),
        "checkpoints >= 1 and reclaimed >= 1",
        pass,
    );
    result.note = "no txlog-bytes-on-disk gauge is exported today; this criterion proves \
                   checkpoint+reclaim activity (disk should sawtooth), not a bounded byte \
                   count directly — adding a size gauge (or scraping node_exporter disk \
                   usage for the txlog volume) is a follow-up for a stronger guarantee"
        .to_string();
    result
}

/// Criterion 4: p99 publish latency under target.
///
/// `p99_series` is expected to already be `histogram_quantile(0.99,
/// rate(cq_publish_latency_us_bucket[...]))` samples from Prometheus
/// (the quantile math happens server-side in PromQL — see
/// [`fetch_p99_publish_latency_us`]). This function just takes the max
/// observed p99 across the window (the worst moment, which is what a
/// soak gate cares about) and compares it to the target.
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
pub fn analyze(
    rss_series: &[Sample],
    dropped_series: &[Sample],
    delivered_series: &[Sample],
    checkpoint_series: &[Sample],
    reclaimed_series: &[Sample],
    p99_series: &[Sample],
    thresholds: &Thresholds,
) -> SoakReport {
    SoakReport {
        criteria: vec![
            check_rss_slope(rss_series, thresholds),
            check_drop_ratio(dropped_series, delivered_series, thresholds),
            check_txlog_bounded(checkpoint_series, reclaimed_series),
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
        },
    }
}

/// Fetch all five series from Prometheus and run [`analyze`]. This is
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
    let dropped = fetch_range(
        &client,
        &cfg.prometheus_url,
        "sum(cq_deltas_dropped_total)",
        cfg.window,
    )
    .await
    .context("fetching cq_deltas_dropped_total")?;
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
    // Server-side quantile: histogram_quantile needs the full bucket
    // set, so compute it in PromQL rather than pulling every bucket
    // series and refitting client-side.
    let p99_query = "histogram_quantile(0.99, sum(rate(cq_publish_latency_us_bucket[1m])) by (le))";
    let p99 = fetch_range(&client, &cfg.prometheus_url, p99_query, cfg.window)
        .await
        .context("fetching cq_publish_latency_us p99")?;

    Ok(analyze(
        &rss,
        &dropped,
        &delivered,
        &checkpoints,
        &reclaimed,
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
        let result = check_drop_ratio(&dropped, &delivered, &thresholds());
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
        let result = check_drop_ratio(&dropped, &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn zero_drops_pass() {
        let delivered: Vec<Sample> = (0..10)
            .map(|i| (i as f64 * 10.0, i as f64 * 1000.0))
            .collect();
        let dropped: Vec<Sample> = (0..10).map(|i| (i as f64 * 10.0, 0.0)).collect();
        let result = check_drop_ratio(&dropped, &delivered, &thresholds());
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
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
        let result = check_drop_ratio(&dropped, &delivered, &thresholds());
        // delivered_delta = 3000 + 1000 + 3000 = 7000; dropped_delta = 10 + 0(reset->0 counted as 0) + 10 = 20
        // ratio well under 5%.
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    // ---- criterion 3: txlog bounded ---------------------------------------

    #[test]
    fn reclaim_events_present_pass() {
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 1.0), (30.0, 2.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed);
        assert_eq!(result.verdict, Verdict::Pass, "{:?}", result);
    }

    #[test]
    fn no_reclaim_events_fail_txlog_unbounded() {
        // Checkpoints fire, but nothing ever gets reclaimed — segments
        // accumulate forever even though checkpointing "works".
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0), (30.0, 0.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed);
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn no_checkpoints_fail_txlog_unbounded() {
        // Reclaimed stays at 0 because checkpointing itself never ran.
        let checkpoints: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)];
        let reclaimed: Vec<Sample> = vec![(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)];
        let result = check_txlog_bounded(&checkpoints, &reclaimed);
        assert_eq!(result.verdict, Verdict::Fail, "{:?}", result);
    }

    #[test]
    fn txlog_missing_series_fails_closed() {
        let result = check_txlog_bounded(&[], &[]);
        assert_eq!(result.verdict, Verdict::Fail);
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
        let p99: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect();

        let report = analyze(
            &rss,
            &dropped,
            &delivered,
            &checkpoints,
            &reclaimed,
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
        let p99: Vec<Sample> = (0..20).map(|i| (i as f64 * 10.0, 5000.0)).collect();

        let report = analyze(
            &rss,
            &dropped,
            &delivered,
            &checkpoints,
            &reclaimed,
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

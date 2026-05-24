//! HDR histogram wrapper. Records latencies in microseconds (sufficient
//! resolution for sub-ms p99 measurements) up to 60 seconds.

use std::time::Duration;

use hdrhistogram::Histogram;

pub struct LatencyHistogram {
    inner: Histogram<u64>,
}

impl LatencyHistogram {
    /// New histogram covering 1 µs to 60 s with 3 significant-figure
    /// precision (HDR's standard tradeoff for latency work).
    pub fn new() -> Self {
        // 1 .. 60_000_000 µs, 3 sig figs → ~60 KB allocation.
        let inner = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)
            .expect("hdr bounds 1..60s @ 3 sig figs always succeed");
        Self { inner }
    }

    /// Record a latency. Values outside [1µs, 60s] are clamped to the
    /// nearest bound (saturated rather than dropped — a saturated bin
    /// surfaces in the p99.9 tail rather than silently disappearing).
    pub fn record(&mut self, latency: Duration) {
        let us = latency.as_micros().min(u64::MAX as u128) as u64;
        let us = us.clamp(1, 60_000_000);
        self.inner.record(us).expect("clamped value always recordable");
    }

    pub fn count(&self) -> u64 {
        self.inner.len()
    }

    pub fn p50(&self) -> Duration {
        Duration::from_micros(self.inner.value_at_quantile(0.50))
    }
    pub fn p95(&self) -> Duration {
        Duration::from_micros(self.inner.value_at_quantile(0.95))
    }
    pub fn p99(&self) -> Duration {
        Duration::from_micros(self.inner.value_at_quantile(0.99))
    }
    pub fn p999(&self) -> Duration {
        Duration::from_micros(self.inner.value_at_quantile(0.999))
    }
    pub fn max(&self) -> Duration {
        Duration::from_micros(self.inner.max())
    }
    pub fn mean(&self) -> Duration {
        Duration::from_micros(self.inner.mean() as u64)
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_track_known_distribution() {
        // Record 1000 samples linearly from 1ms to 1000ms. Each
        // sample N ms appears once, so the i-th percentile should
        // be roughly at value (i ms).
        let mut h = LatencyHistogram::new();
        for ms in 1..=1000 {
            h.record(Duration::from_millis(ms));
        }
        assert_eq!(h.count(), 1000);
        // HDR's 3-sig-fig precision: p50 of 1..1000 is ~500ms,
        // p95 is ~950ms, p99 is ~990ms. Allow ±1% slop.
        let tolerance_ms = 10;
        assert!(
            (h.p50().as_millis() as i64 - 500).abs() < tolerance_ms,
            "p50 = {}ms, expected ~500ms",
            h.p50().as_millis()
        );
        assert!(
            (h.p95().as_millis() as i64 - 950).abs() < tolerance_ms,
            "p95 = {}ms, expected ~950ms",
            h.p95().as_millis()
        );
        assert!(
            (h.p99().as_millis() as i64 - 990).abs() < tolerance_ms,
            "p99 = {}ms, expected ~990ms",
            h.p99().as_millis()
        );
    }

    #[test]
    fn empty_histogram_reports_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.p50(), Duration::ZERO);
    }

    #[test]
    fn out_of_range_values_clamp_into_top_bin() {
        let mut h = LatencyHistogram::new();
        h.record(Duration::from_secs(3600)); // 1h — far above the 60s ceiling
        assert_eq!(h.count(), 1);
        // The clamped value sits at the 60s upper bound.
        assert!(h.max() >= Duration::from_secs(50));
    }
}

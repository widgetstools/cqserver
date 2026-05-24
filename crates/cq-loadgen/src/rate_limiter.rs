//! Open-loop rate limiter.
//!
//! "Open loop" means the limiter gates events at a fixed per-second
//! rate regardless of how fast the downstream is acking. This is the
//! right shape for stress testing: the goal is to find the saturation
//! point, not the round-trip time. A closed-loop generator
//! (publish-wait-publish) self-throttles to whatever the server can
//! handle and never reveals where it breaks.
//!
//! Implementation: maintain a target interval `period = 1 / rate`.
//! Before each event, await whatever time remains until the next
//! scheduled slot. If we're already past the slot (we fell behind),
//! issue immediately and advance the schedule — the loss is recorded
//! by the caller's histogram, not by skipping events.

use std::time::{Duration, Instant};

use tokio::time::sleep;

pub struct RateLimiter {
    period: Duration,
    next_at: Instant,
    started_at: Instant,
    issued: u64,
}

impl RateLimiter {
    /// `rate` is events per second. A rate of 0 or below produces a
    /// limiter that never gates (useful for "max throughput" runs).
    pub fn new(rate: f64) -> Self {
        let period = if rate > 0.0 {
            Duration::from_secs_f64(1.0 / rate)
        } else {
            Duration::ZERO
        };
        let now = Instant::now();
        Self {
            period,
            next_at: now,
            started_at: now,
            issued: 0,
        }
    }

    /// Block until the next event is due, then return. Records the
    /// event for `actual_rate()` accounting.
    pub async fn tick(&mut self) {
        if self.period == Duration::ZERO {
            self.issued = self.issued.wrapping_add(1);
            return;
        }
        let now = Instant::now();
        if now < self.next_at {
            sleep(self.next_at - now).await;
        }
        self.next_at += self.period;
        self.issued = self.issued.wrapping_add(1);
    }

    /// Effective issued-events-per-second over the lifetime of this
    /// limiter. Useful for verifying the limiter held its target.
    pub fn actual_rate(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            self.issued as f64 / elapsed
        } else {
            0.0
        }
    }

    pub fn issued(&self) -> u64 {
        self.issued
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn limiter_holds_within_2_percent_of_target_rate() {
        // 1000 events/s for 250ms → expect ~250 events ±2%.
        let mut limiter = RateLimiter::new(1000.0);
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(250) {
            limiter.tick().await;
        }
        let elapsed = start.elapsed().as_secs_f64();
        let expected = 1000.0 * elapsed;
        let actual = limiter.issued() as f64;
        let drift = ((actual - expected) / expected).abs();
        assert!(
            drift < 0.02,
            "limiter drift {:.2}% > 2% (issued {actual} over {:.3}s, expected ~{expected:.0})",
            drift * 100.0,
            elapsed
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rate_zero_means_no_throttling() {
        let mut limiter = RateLimiter::new(0.0);
        let start = Instant::now();
        for _ in 0..1000 {
            limiter.tick().await;
        }
        // Should complete near-instantly (well under 50ms).
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "unlimited rate took {}ms — limiter is gating when it shouldn't",
            start.elapsed().as_millis()
        );
        assert_eq!(limiter.issued(), 1000);
    }
}

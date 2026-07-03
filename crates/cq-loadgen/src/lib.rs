//! Load-generation primitives for CQServer (worklog S47 / stress-test
//! plan §5).
//!
//! The crate exposes three building blocks:
//!
//! - [`LatencyHistogram`] — thin wrapper around an HDR histogram with
//!   convenient p50/p95/p99/p99.9 readouts.
//! - [`RateLimiter`] — token-bucket-style open-loop limiter that gates
//!   a producer at a fixed *rate*, regardless of how slowly the
//!   downstream is acking. Open-loop is what surfaces saturation;
//!   closed-loop only reports round-trip latency.
//! - Scenario runners (`scenarios::publish_throughput`,
//!   `scenarios::fanout`) — full async drivers that take a config and
//!   produce a [`Report`].
//!
//! The binary [`cq-loadgen`] (see `src/main.rs`) is a thin CLI wrapper
//! that selects a scenario and prints a report.

pub mod histogram;
pub mod rate_limiter;
pub mod scenarios;
pub mod soak_analyze;

pub use histogram::LatencyHistogram;
pub use rate_limiter::RateLimiter;
pub use scenarios::{Report, ScenarioConfig};

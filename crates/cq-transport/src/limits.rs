//! D3/P0.3 — connection & rate limits, enforced at accept time (and, for
//! per-user session caps, at Logon time).
//!
//! `ConnectionLimits` is constructed once per server process and shared
//! (via `Arc`) between the TCP and WebSocket accept loops so both
//! transports draw from the exact same budget — a client can't dodge
//! `max_connections` by switching transport.
//!
//! Four independent knobs, each individually disableable:
//!   - `max_connections`      — global cap on live connections. Backed by
//!     a `tokio::sync::Semaphore`; the accept loop acquires an owned
//!     permit and stores it on the connection task, so ANY exit path
//!     (clean close, read error, panic-unwind through the task) releases
//!     it automatically when the task's `Session`/permit is dropped.
//!   - `max_connections_per_ip` — cap on live connections from one peer
//!     IP. Backed by a `DashMap<IpAddr, usize>`; `0` disables the check.
//!     Enforced with an RAII guard that decrements on `Drop`.
//!   - `accept_rate_per_sec`  — cap on new accepts per rolling second.
//!     Simple fixed-window counter reset once a second has elapsed since
//!     the window opened. `0` disables the check.
//!   - `max_sessions_per_user` — cap on concurrently logged-on sessions
//!     for one authenticated username. Enforced at Logon time (identity
//!     isn't known at accept time). `0` disables the check.
//!
//! All four default to "off" except `max_connections`, which defaults to
//! 10000 (still generous, but a real ceiling instead of "no limit").

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why an accept (or logon) was rejected. Used only to select the
/// `reason` label on `cq_connections_rejected_total` and for log
/// messages — never sent to the client verbatim (though this task's
/// callers do send a close/error frame that names the same limit).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    MaxConnections,
    MaxConnectionsPerIp,
    AcceptRate,
    MaxSessionsPerUser,
}

impl RejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            RejectReason::MaxConnections => "max_connections",
            RejectReason::MaxConnectionsPerIp => "max_connections_per_ip",
            RejectReason::AcceptRate => "accept_rate_per_sec",
            RejectReason::MaxSessionsPerUser => "max_sessions_per_user",
        }
    }
}

/// Increment the shared rejection counter with the given reason label.
/// Centralized so every call site produces the exact same metric name.
pub fn record_rejection(reason: RejectReason) {
    metrics::counter!("cq_connections_rejected_total", "reason" => reason.as_str())
        .increment(1);
}

/// Config knobs for [`ConnectionLimits`]. Mirrors
/// `cq_server::config::TransportLimitsConfig` field-for-field; kept as a
/// separate plain struct here so cq-transport doesn't depend on
/// cq-server for its own config shape.
#[derive(Debug, Clone, Copy)]
pub struct LimitsConfig {
    pub max_connections: usize,
    pub max_connections_per_ip: usize,
    pub accept_rate_per_sec: u32,
    pub max_sessions_per_user: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        LimitsConfig {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: 0,
            accept_rate_per_sec: 0,
            max_sessions_per_user: 0,
        }
    }
}

/// Default `max_connections` when `[transport.limits]` (or the whole
/// table) is omitted from config.
pub const DEFAULT_MAX_CONNECTIONS: usize = 10_000;

/// Fixed-window accept-rate limiter. A window is one wall-clock second;
/// `count` resets to 0 whenever the window rolls over. `0` limit means
/// "disabled" and is handled by the caller before touching this type.
struct RateWindow {
    window_start_ms: AtomicU64,
    count: AtomicU64,
}

impl RateWindow {
    fn new() -> Self {
        RateWindow {
            window_start_ms: AtomicU64::new(now_ms()),
            count: AtomicU64::new(0),
        }
    }

    /// Returns `true` if this accept is admitted under `limit` accepts
    /// per rolling 1-second window; `false` if it must be rejected.
    fn try_admit(&self, limit: u32) -> bool {
        let now = now_ms();
        let start = self.window_start_ms.load(Ordering::Acquire);
        if now.saturating_sub(start) >= 1000 {
            // Roll the window over. Racing threads may both observe the
            // stale window and both reset — harmless, worst case we
            // undercount by a handful of accepts right at the boundary,
            // which only makes the limiter slightly more permissive for
            // one tick, never less safe.
            self.window_start_ms.store(now, Ordering::Release);
            self.count.store(0, Ordering::Release);
        }
        let prev = self.count.fetch_add(1, Ordering::AcqRel);
        prev < limit as u64
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// RAII guard for one accepted connection. Holding this keeps the
/// `max_connections` permit alive and (when per-IP limiting is enabled)
/// the per-IP counter incremented. Dropping it — on ANY exit path —
/// releases both automatically.
#[derive(Debug)]
pub struct ConnectionGuard {
    _permit: OwnedSemaphorePermit,
    // Held only for its Drop side effect (decrements the per-IP
    // counter); never read directly.
    _ip_guard: Option<IpGuard>,
}

/// Decrements the owning IP's counter in `per_ip` on drop. Removes the
/// map entry entirely once it hits zero so `per_ip` doesn't grow
/// unbounded across the lifetime of a long-running server with churny
/// client IPs.
#[derive(Debug)]
struct IpGuard {
    ip: IpAddr,
    per_ip: Arc<DashMap<IpAddr, usize>>,
}

impl Drop for IpGuard {
    fn drop(&mut self) {
        if let Some(mut entry) = self.per_ip.get_mut(&self.ip) {
            *entry = entry.saturating_sub(1);
            if *entry == 0 {
                drop(entry);
                self.per_ip.remove_if(&self.ip, |_, v| *v == 0);
            }
        }
    }
}

/// RAII guard for one logged-on user session. Dropping it decrements
/// the per-user session count. Held on the `Session` (or by the caller
/// of `handle_logon`) for the lifetime of the connection.
#[derive(Debug)]
pub struct UserSessionGuard {
    user: String,
    per_user: Arc<DashMap<String, usize>>,
}

impl Drop for UserSessionGuard {
    fn drop(&mut self) {
        if let Some(mut entry) = self.per_user.get_mut(&self.user) {
            *entry = entry.saturating_sub(1);
            if *entry == 0 {
                drop(entry);
                self.per_user.remove_if(&self.user, |_, v| *v == 0);
            }
        }
    }
}

/// Shared connection/rate-limit state for a server process. Constructed
/// once (`ConnectionLimits::new`) and shared via `Arc` between the TCP
/// and WebSocket accept loops, so both transports draw from the same
/// budget.
pub struct ConnectionLimits {
    config: LimitsConfig,
    max_connections_sem: Arc<Semaphore>,
    per_ip: Arc<DashMap<IpAddr, usize>>,
    accept_window: RateWindow,
    per_user: Arc<DashMap<String, usize>>,
}

impl ConnectionLimits {
    pub fn new(config: LimitsConfig) -> Arc<Self> {
        // A semaphore sized 0 would reject every connection outright,
        // which isn't what "max_connections = 0" should mean in this
        // config shape (0 is reserved for "disabled" on the OTHER three
        // knobs). max_connections has no "disabled" sentinel — it
        // always enforces a ceiling, defaulting to 10000. Guard against
        // a misconfigured 0 by treating it as "effectively unlimited"
        // rather than "always reject", since silently wedging every
        // connection is a worse failure mode than temporarily ignoring
        // the cap.
        let permits = if config.max_connections == 0 {
            usize::MAX >> 4 // enormous but not overflow-adjacent
        } else {
            config.max_connections
        };
        Arc::new(ConnectionLimits {
            config,
            max_connections_sem: Arc::new(Semaphore::new(permits)),
            per_ip: Arc::new(DashMap::new()),
            accept_window: RateWindow::new(),
            per_user: Arc::new(DashMap::new()),
        })
    }

    /// Number of currently-available `max_connections` permits. Exposed
    /// for tests / admin introspection.
    pub fn available_permits(&self) -> usize {
        self.max_connections_sem.available_permits()
    }

    /// Current live-connection count for one IP. Exposed for tests.
    pub fn connections_for_ip(&self, ip: IpAddr) -> usize {
        self.per_ip.get(&ip).map(|e| *e).unwrap_or(0)
    }

    /// Current live-session count for one user. Exposed for tests.
    pub fn sessions_for_user(&self, user: &str) -> usize {
        self.per_user.get(user).map(|e| *e).unwrap_or(0)
    }

    /// Try to admit a new accepted connection from `ip`. On success,
    /// returns a guard that must be held for the lifetime of the
    /// connection — dropping it releases the `max_connections` permit
    /// and (if enabled) decrements the per-IP counter. On failure,
    /// returns the specific reason so the caller can log + count +
    /// close the socket before doing any further (expensive) setup.
    ///
    /// Order of checks: accept-rate first (cheapest, protects against a
    /// connect storm before we touch the semaphore), then
    /// max_connections, then per-IP. A rejection at any stage does not
    /// consume budget from a later stage.
    pub fn try_accept(&self, ip: IpAddr) -> Result<ConnectionGuard, RejectReason> {
        if self.config.accept_rate_per_sec > 0
            && !self.accept_window.try_admit(self.config.accept_rate_per_sec)
        {
            return Err(RejectReason::AcceptRate);
        }

        let permit = self
            .max_connections_sem
            .clone()
            .try_acquire_owned()
            .map_err(|_| RejectReason::MaxConnections)?;

        let ip_guard = if self.config.max_connections_per_ip > 0 {
            let mut entry = self.per_ip.entry(ip).or_insert(0);
            if *entry >= self.config.max_connections_per_ip {
                drop(entry);
                // permit drops here, releasing the max_connections slot
                // we just took — this rejection shouldn't consume it.
                return Err(RejectReason::MaxConnectionsPerIp);
            }
            *entry += 1;
            drop(entry);
            Some(IpGuard {
                ip,
                per_ip: self.per_ip.clone(),
            })
        } else {
            None
        };

        Ok(ConnectionGuard {
            _permit: permit,
            _ip_guard: ip_guard,
        })
    }

    /// Try to register a new session for `user` at Logon time. On
    /// success, returns a guard that must be held for the session's
    /// lifetime; dropping it decrements the count. On failure, the
    /// logon should be rejected with an error frame naming the limit.
    pub fn try_register_user_session(&self, user: &str) -> Result<UserSessionGuard, RejectReason> {
        if self.config.max_sessions_per_user == 0 {
            // Disabled — always succeeds, but still return a guard so
            // callers have a uniform type. The guard's Drop is a no-op
            // in practice since we never incremented anything for this
            // user... except we *do* need symmetry: if disabled, don't
            // touch the map at all, and give back a "vacuous" guard tied
            // to a fresh (never-decremented) entry-less state. Simplest
            // correct approach: still track it — a later reconfigure
            // isn't a concern here since config is fixed per-process —
            // but skipping the map entirely avoids unbounded growth risk
            // when the feature is off. Use a dedicated empty map instead.
            return Ok(UserSessionGuard {
                user: user.to_string(),
                per_user: Arc::new(DashMap::new()),
            });
        }
        let mut entry = self.per_user.entry(user.to_string()).or_insert(0);
        if *entry >= self.config.max_sessions_per_user {
            return Err(RejectReason::MaxSessionsPerUser);
        }
        *entry += 1;
        drop(entry);
        Ok(UserSessionGuard {
            user: user.to_string(),
            per_user: self.per_user.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn ip(n: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, n))
    }

    #[test]
    fn max_connections_admits_up_to_the_cap_then_rejects() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 2,
            ..LimitsConfig::default()
        });

        let g1 = limits.try_accept(ip(1)).expect("1st accept ok");
        let g2 = limits.try_accept(ip(2)).expect("2nd accept ok");
        let err = limits.try_accept(ip(3)).expect_err("3rd should be rejected");
        assert_eq!(err, RejectReason::MaxConnections);

        // Freeing one slot lets a retry through.
        drop(g1);
        let g3 = limits.try_accept(ip(4)).expect("retry after close should succeed");
        drop(g2);
        drop(g3);
    }

    #[test]
    fn max_connections_default_is_generous() {
        let limits = ConnectionLimits::new(LimitsConfig::default());
        assert_eq!(limits.available_permits(), DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn max_connections_per_ip_zero_disables_the_check() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 100,
            max_connections_per_ip: 0,
            ..LimitsConfig::default()
        });
        let mut guards = Vec::new();
        for _ in 0..10 {
            guards.push(limits.try_accept(ip(9)).expect("per-ip disabled: no cap"));
        }
        // Disabled means the per-IP map isn't even consulted/updated —
        // there's no cap to track against.
        assert_eq!(limits.connections_for_ip(ip(9)), 0);
    }

    #[test]
    fn max_connections_per_ip_enforced_independently_per_address() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 100,
            max_connections_per_ip: 2,
            ..LimitsConfig::default()
        });

        let a1 = limits.try_accept(ip(1)).expect("ip1 #1");
        let _a2 = limits.try_accept(ip(1)).expect("ip1 #2");
        let err = limits.try_accept(ip(1)).expect_err("ip1 #3 rejected");
        assert_eq!(err, RejectReason::MaxConnectionsPerIp);

        // A different IP is unaffected.
        let _b1 = limits.try_accept(ip(2)).expect("ip2 #1 unaffected");

        // Freeing one ip1 slot lets a retry through, and the global
        // max_connections permit released by the rejected attempt above
        // must have been given back (i.e. the rejection didn't leak a
        // permit).
        drop(a1);
        let _a3 = limits.try_accept(ip(1)).expect("ip1 retry after close");
    }

    #[test]
    fn per_ip_rejection_does_not_leak_a_max_connections_permit() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 5,
            max_connections_per_ip: 1,
            ..LimitsConfig::default()
        });
        let before = limits.available_permits();
        let _keep = limits.try_accept(ip(1)).unwrap();
        let after_first = limits.available_permits();
        assert_eq!(before - after_first, 1);

        // This is rejected on the per-IP check, AFTER acquiring (and
        // then dropping) the semaphore permit. Available permits must
        // return to `after_first`, not stay decremented.
        let err = limits.try_accept(ip(1)).unwrap_err();
        assert_eq!(err, RejectReason::MaxConnectionsPerIp);
        assert_eq!(limits.available_permits(), after_first);
    }

    #[test]
    fn accept_rate_zero_disables_the_check() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 1000,
            accept_rate_per_sec: 0,
            ..LimitsConfig::default()
        });
        for i in 0..500 {
            limits
                .try_accept(ip((i % 250) as u8))
                .expect("rate disabled: unlimited within one tick");
        }
    }

    #[test]
    fn accept_rate_limits_bursts_within_one_window() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections: 1000,
            accept_rate_per_sec: 3,
            ..LimitsConfig::default()
        });
        // First 3 in this window succeed regardless of IP.
        for i in 0..3 {
            limits.try_accept(ip(i)).expect("within burst budget");
        }
        // The 4th in the same window is rejected.
        let err = limits.try_accept(ip(9)).unwrap_err();
        assert_eq!(err, RejectReason::AcceptRate);
    }

    #[test]
    fn max_sessions_per_user_admits_up_to_cap_then_rejects() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_sessions_per_user: 1,
            ..LimitsConfig::default()
        });
        let g1 = limits
            .try_register_user_session("alice")
            .expect("1st session ok");
        let err = limits
            .try_register_user_session("alice")
            .expect_err("2nd session over cap");
        assert_eq!(err, RejectReason::MaxSessionsPerUser);

        // A different user is unaffected.
        limits
            .try_register_user_session("bob")
            .expect("different user unaffected");

        // Dropping the first session's guard frees the slot.
        drop(g1);
        limits
            .try_register_user_session("alice")
            .expect("retry after logoff succeeds");
    }

    #[test]
    fn max_sessions_per_user_zero_disables_the_check() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_sessions_per_user: 0,
            ..LimitsConfig::default()
        });
        for _ in 0..50 {
            limits
                .try_register_user_session("alice")
                .expect("disabled: unlimited sessions per user");
        }
    }

    #[test]
    fn dropping_guard_removes_zeroed_ip_entry_to_avoid_unbounded_growth() {
        let limits = ConnectionLimits::new(LimitsConfig {
            max_connections_per_ip: 5,
            ..LimitsConfig::default()
        });
        let g = limits.try_accept(ip(42)).unwrap();
        assert_eq!(limits.connections_for_ip(ip(42)), 1);
        drop(g);
        assert_eq!(limits.connections_for_ip(ip(42)), 0);
        assert!(!limits.per_ip.contains_key(&ip(42)));
    }
}

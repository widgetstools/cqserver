//! Bidirectional heartbeat + idle-disconnect.
//!
//! The server pushes a `Command::Heartbeat` to every connected session on
//! a fixed interval. If no inbound frame arrives within `idle_timeout`,
//! the connection is signalled to close via a `Notify`.
//!
//! The read loop in each transport handler `tokio::select!`s between the
//! incoming-frame future and `cancel.notified()`, so an idle peer is
//! evicted promptly rather than lingering on the next read forever.

use crate::session::{encode_frame, now_ms, OutboundTx, SharedCodec};
use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Notify};

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub idle_timeout: Duration,
}

impl HeartbeatConfig {
    /// Production defaults: ping every 30s, disconnect on 65s idle.
    pub const DEFAULT: HeartbeatConfig = HeartbeatConfig {
        interval: Duration::from_secs(30),
        idle_timeout: Duration::from_secs(65),
    };

    /// Disabled — no server-initiated heartbeats and no idle timeout.
    /// Useful for tests that don't want the timing machinery in the way.
    pub const DISABLED: HeartbeatConfig = HeartbeatConfig {
        interval: Duration::ZERO,
        idle_timeout: Duration::ZERO,
    };

    pub fn is_enabled(&self) -> bool {
        !self.interval.is_zero()
    }
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Spawn the per-session heartbeat task. Returns a `Notify` that fires
/// once when the peer is declared idle (no inbound for `idle_timeout`)
/// so the transport's read loop can break out.
///
/// The task exits cleanly when `tx` is closed (peer disconnected by
/// other means) or after firing the cancellation notify.
pub fn spawn(
    session_id: String,
    tx: OutboundTx,
    last_inbound_ms: Arc<AtomicU64>,
    codec: SharedCodec,
    cfg: HeartbeatConfig,
) -> Arc<Notify> {
    let cancel = Arc::new(Notify::new());
    if !cfg.is_enabled() {
        return cancel;
    }

    let cancel_ret = cancel.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip the immediate first tick — we just opened the connection.
        ticker.tick().await;

        loop {
            ticker.tick().await;

            // Check idle.
            let now = now_ms();
            let last = last_inbound_ms.load(Ordering::Relaxed);
            let idle_ms = now.saturating_sub(last);
            if idle_ms > cfg.idle_timeout.as_millis() as u64 {
                tracing::warn!(
                    session = %session_id,
                    idle_ms,
                    "Peer idle past timeout; signalling disconnect"
                );
                cancel.notify_one();
                return;
            }

            // Push heartbeat using the negotiated codec.
            let msg = CqMessage::new(Command::Heartbeat);
            let frame = match encode_frame(*codec.lock(), &msg) {
                Some(f) => f,
                None => {
                    tracing::warn!(session = %session_id, "Heartbeat serialize failed");
                    continue;
                }
            };
            match tx.try_send(frame) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Peer can't keep up with even a heartbeat — treat
                    // as effectively idle.
                    tracing::warn!(session = %session_id, "Heartbeat queue full");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Outbound dropped — connection is closing. Exit.
                    return;
                }
            }
        }
    });

    cancel_ret
}

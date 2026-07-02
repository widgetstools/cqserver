//! Active-passive replication for cqserver.
//!
//! The primary's `Shipper` tails each persistent topic's txlog
//! directory and streams new entries to the standby's `Receiver`. The
//! receiver applies entries to its in-memory topics via the
//! cq-core `replay_*` API and tracks the highest sequence it has
//! durably absorbed.
//!
//! Wire protocol (per direction)
//! -----------------------------
//! Each frame is length-prefixed `[u32 BE][body]`. The body is a
//! MessagePack-encoded `ReplFrame`.
//!
//! On connect, the receiver opens the inbound stream and sends a
//! `ReplFrame::Hello { instance, highwater }` reporting its own instance id
//! and a nested map `topic -> origin -> last_seq` summarizing, per
//! originating instance, what it already has. The shipper resumes streaming
//! each `(topic, origin)` stream from `last_seq + 1` and skips re-shipping
//! anything that originated at the peer (loop avoidance). Subsequent frames
//! are `ReplFrame::Entry` records carrying the originating instance id, so
//! the receiver can dedup convergently per origin (AMPS-style).

pub mod filter;
pub mod receiver;
pub mod shipper;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One frame in the replication protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplFrame {
    /// Sent by the primary (shipper) as the very first frame when a
    /// replication token is configured. The standby (receiver) validates
    /// it with a constant-time compare before revealing any state via
    /// `Hello`. When no token is configured on either side this frame is
    /// never sent and the channel behaves as before.
    Auth { token: String },

    /// Sent by the receiver right after a connection is opened (and, when
    /// a token is configured, after a successful `Auth`). Reports the
    /// receiver's own `instance` id (so the shipper can avoid reflecting the
    /// peer's own entries back to it) and a nested `highwater` map
    /// `topic -> origin -> last_seq` so the shipper can resume each
    /// `(topic, origin)` stream past what the receiver already holds.
    Hello {
        instance: String,
        highwater: HashMap<String, HashMap<String, u64>>,
    },

    /// A single log entry. The shipper streams these in per-topic
    /// sequence order; the receiver applies them via `replay_*_origin`.
    /// `origin` is the AMPS-style publisher id of the instance that first
    /// produced the entry (empty for legacy / unnamed instances).
    Entry {
        sequence: u64,
        topic: String,
        key: String,
        origin: String,
        is_tombstone: bool,
        payload: Vec<u8>,
        /// `true` when `payload` is a sparse JSON delta (`JsonDelta` txlog
        /// entries) that must be MERGED into the receiver's existing row
        /// rather than replacing it. `#[serde(default)]` keeps the frame
        /// wire-compatible with pre-delta peers (absent ⇒ `false`); an
        /// older receiver ignores the field and misses the merge
        /// semantics, so upgrade receivers before enabling
        /// `sparse_delta_txlog` on a replication source.
        #[serde(default)]
        is_delta: bool,
    },

    /// Periodic ack from receiver — informational, the shipper uses it
    /// to update metrics.
    Ack { topic: String, sequence: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum ReplError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("decode: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("txlog: {0}")]
    TxLog(#[from] cq_txlog::TxLogError),
    #[error("frame too large: {0} bytes")]
    FrameTooLarge(usize),
    #[error("peer disconnected")]
    PeerDisconnected,
    #[error("replication auth failed: {0}")]
    Unauthorized(&'static str),
}

/// Cap individual frame size at 16MB — same as txlog entry cap.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

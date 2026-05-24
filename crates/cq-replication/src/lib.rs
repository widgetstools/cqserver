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
//! `ReplFrame::Hello { highwater }` with a map `topic -> last_seq`
//! summarizing what it already has. The shipper resumes streaming each
//! topic from `last_seq + 1`. Subsequent frames are `ReplFrame::Entry`
//! records.

pub mod filter;
pub mod receiver;
pub mod shipper;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One frame in the replication protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplFrame {
    /// Sent by the receiver right after a connection is opened. Lets the
    /// shipper skip everything already on the standby.
    Hello { highwater: HashMap<String, u64> },

    /// A single log entry. The shipper streams these in per-topic
    /// sequence order; the receiver applies them via `replay_*`.
    Entry {
        sequence: u64,
        topic: String,
        key: String,
        is_tombstone: bool,
        payload: Vec<u8>,
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
}

/// Cap individual frame size at 16MB — same as txlog entry cap.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

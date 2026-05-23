//! Subscription handle exposed to callers.

use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    Add,
    Update,
    Remove,
    Oof,
    SowSnapshot,
}

impl DeltaKind {
    pub fn from_str(s: &str) -> Self {
        match s {
            "add" => Self::Add,
            "update" => Self::Update,
            "remove" => Self::Remove,
            "oof" => Self::Oof,
            _ => Self::Add, // default — server uses these four
        }
    }
}

#[derive(Debug, Clone)]
pub struct Delta {
    pub delta_type: DeltaKind,
    pub sub_id: String,
    pub sequence: Option<u64>,
    pub data: Map<String, Value>,
    /// Queue-lease delivery id, when this delta came from a queue
    /// with leasing enabled. The consumer must echo it back via
    /// `Client::queue_ack` to commit the lease — otherwise the
    /// message will be redelivered after the lease window expires.
    pub delivery_id: Option<u64>,
}

/// Subscription handle. Calls to `next_delta()` await server-pushed
/// updates. Also tracks the highest sequence seen so callers can
/// resume by passing `last_sequence()` as the bookmark on reconnect.
pub struct Subscription {
    pub sub_id: String,
    pub(crate) rx: mpsc::UnboundedReceiver<Delta>,
    pub(crate) last_seq: Arc<AtomicU64>,
}

impl Subscription {
    pub async fn next_delta(&mut self) -> Option<Delta> {
        let d = self.rx.recv().await?;
        if let Some(seq) = d.sequence {
            // CAS-bump the recorded high-water so concurrent observers
            // see a monotonic view.
            let mut cur = self.last_seq.load(Ordering::Relaxed);
            while seq > cur {
                match self.last_seq.compare_exchange(
                    cur,
                    seq,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => cur = observed,
                }
            }
        }
        Some(d)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }
}

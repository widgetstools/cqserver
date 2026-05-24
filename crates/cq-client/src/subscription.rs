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
    /// Server-emitted schema change announcement (S44). The
    /// `Delta::schema_change` field carries the new/removed
    /// columns and version; `Delta::data` is empty. Always
    /// precedes any subsequent `Add`/`Update` whose payload uses
    /// the new columns.
    SchemaChange,
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
    /// Set only when `delta_type == DeltaKind::SchemaChange` (S44).
    /// Carries the structured added/removed columns + version. The
    /// SDK surfaces this to application code via the same
    /// `next_delta()` stream so callers don't need a second
    /// channel — they pattern-match `delta_type` and read
    /// `schema_change` when it's set.
    pub schema_change: Option<cq_protocol::schema_change::SchemaChangeBody>,
}

/// Subscription handle. Calls to `next_delta()` await server-pushed
/// updates. Also tracks the highest sequence seen so callers can
/// resume by passing `last_sequence()` as the bookmark on reconnect.
pub struct Subscription {
    pub sub_id: String,
    pub(crate) rx: mpsc::UnboundedReceiver<Delta>,
    pub(crate) last_seq: Arc<AtomicU64>,
    /// S18 — topic this subscription targets. Carried on the handle
    /// so `next_delta` can write the bookmark for the right key.
    pub(crate) topic: String,
    /// S18 — optional client-side persistent bookmark store. When
    /// set, every delta with a sequence updates the store entry for
    /// `topic` (monotonic) and triggers a lazy disk persist on next
    /// drop / explicit `persist`.
    pub(crate) bookmark_store: Option<crate::bookmark::LocalBookmarkStore>,
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
            // S18: record into the persistent bookmark store too. The
            // store is monotonic and self-deduplicating, so calling
            // it on every delta is cheap.
            if let Some(bm) = &self.bookmark_store {
                bm.record(&self.topic, seq);
            }
        }
        Some(d)
    }

    pub fn last_sequence(&self) -> u64 {
        self.last_seq.load(Ordering::Relaxed)
    }

    /// Force a disk persist of the bookmark store. The store is
    /// otherwise an in-memory map; explicit persist is how callers
    /// pin the latest high-water down for the next process restart.
    pub fn persist_bookmark(&self) -> Result<(), crate::bookmark::LocalBookmarkError> {
        if let Some(bm) = &self.bookmark_store {
            bm.persist()?;
        }
        Ok(())
    }
}

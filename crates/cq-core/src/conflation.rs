//! Per-subscription delta conflation.
//!
//! Coalesces deltas by row index, applying merge rules that preserve the
//! consumer's view of state:
//!
//! | Prior + Next      | Result                          |
//! |-------------------|---------------------------------|
//! | Add  + Update     | Add with the latest row_data    |
//! | Add  + Remove     | cancel — drop both              |
//! | Update + Update   | Update with the latest row_data |
//! | Update + Remove   | Remove                          |
//! | Remove + Add      | both emitted (see below)        |
//! | Remove + Update   | Update (treat as still-present) |
//! | anything + Oof    | Oof (consumer must evict)       |
//!
//! **Remove + Add and SOW slot reuse.** The conflator keys on the store
//! *row index*, but a deleted slot can be reclaimed and refilled with a
//! *different* SOW key (slot reuse). When a pending `Remove` for a slot is
//! followed by an `Add` for the same slot, the conflator cannot tell "same
//! key republished" from "different key reusing the slot", so it preserves
//! **both** — the `Remove` is moved to a `ready` buffer and the `Add`
//! becomes pending. Collapsing to a single `Add` (the old behaviour) would,
//! under reuse, silently lose the prior occupant's removal and leave a
//! stale row in the consumer's view. Emitting both is correct either way:
//! for a genuine same-key republish the consumer removes then re-adds the
//! key, ending in the same state.
//!
//! The conflator is pure data structure: callers submit deltas as they're
//! computed; a separate flush loop drains them at the configured interval
//! and forwards them through the transport.

use crate::subscription::{Delta, DeltaType};
use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Default)]
struct ConflatorInner {
    /// At most one coalesced delta per live slot.
    pending: HashMap<u32, Delta>,
    /// Deltas that must be emitted verbatim and not coalesced further —
    /// currently a `Remove` that was superseded by an `Add` for a reused
    /// slot. Drained ahead of `pending` so a removal precedes the add.
    ready: Vec<Delta>,
}

pub struct Conflator {
    inner: Mutex<ConflatorInner>,
}

impl Conflator {
    pub fn new() -> Self {
        Conflator {
            inner: Mutex::new(ConflatorInner::default()),
        }
    }

    /// Submit a delta. Coalesces with any prior pending delta for the same row.
    pub fn submit(&self, delta: Delta) {
        let mut inner = self.inner.lock();
        match inner.pending.remove(&delta.row) {
            None => {
                inner.pending.insert(delta.row, delta);
            }
            Some(prev) => {
                // Reused slot: a removal followed by an add for the same
                // slot is two distinct records (possibly different SOW
                // keys). Preserve the Remove verbatim and let the Add
                // become the new pending delta for the slot.
                if prev.delta_type == DeltaType::Remove
                    && delta.delta_type == DeltaType::Add
                {
                    inner.ready.push(prev);
                    inner.pending.insert(delta.row, delta);
                } else if let Some(merged) = merge(prev, delta) {
                    inner.pending.insert(merged.row, merged);
                }
                // None → Add+Remove cancelled
            }
        }
    }

    /// Drain all pending deltas. `ready` deltas (preserved removals) come
    /// first so a slot's removal is delivered before its reuse add.
    pub fn drain(&self) -> Vec<Delta> {
        let mut inner = self.inner.lock();
        let mut out = std::mem::take(&mut inner.ready);
        out.extend(inner.pending.drain().map(|(_, d)| d));
        out
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.lock();
        inner.pending.len() + inner.ready.len()
    }

    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock();
        inner.pending.is_empty() && inner.ready.is_empty()
    }
}

impl Default for Conflator {
    fn default() -> Self {
        Self::new()
    }
}

fn merge(prev: Delta, next: Delta) -> Option<Delta> {
    use DeltaType::*;
    match (prev.delta_type, next.delta_type) {
        // Record entered and then left before the consumer saw it — drop both.
        (Add, Remove) => None,
        // Record entered and was updated — emit as Add with latest data so
        // the consumer's first sight of it has the correct fields.
        (Add, Update) => {
            let mut merged = next;
            merged.delta_type = Add;
            Some(merged)
        }
        // Oof always wins — consumer needs to evict from local view.
        (_, Oof) => Some(next),
        // Default: latest delta wins. Covers Update+Update, Update+Remove,
        // Remove+Add, Remove+Update, etc.
        _ => Some(next),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(row: u32, dt: DeltaType, marker: &str) -> Delta {
        let mut m = serde_json::Map::new();
        m.insert("v".into(), serde_json::Value::String(marker.into()));
        Delta {
            subscription_id: "s".into(),
            delta_type: dt,
            row,
            sequence: row as u64 + 1,
            row_data: std::sync::Arc::new(m),
            encoded_body_json: None,
        }
    }

    #[test]
    fn single_delta_passes_through() {
        let c = Conflator::new();
        c.submit(delta(0, DeltaType::Add, "v1"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Add);
    }

    #[test]
    fn add_then_update_collapses_to_add_with_latest_data() {
        let c = Conflator::new();
        c.submit(delta(7, DeltaType::Add, "v1"));
        c.submit(delta(7, DeltaType::Update, "v2"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Add);
        assert_eq!(out[0].row_data.get("v").unwrap(), "v2");
    }

    #[test]
    fn add_then_remove_cancels() {
        let c = Conflator::new();
        c.submit(delta(3, DeltaType::Add, "v1"));
        c.submit(delta(3, DeltaType::Remove, "v1"));
        assert!(c.drain().is_empty());
    }

    #[test]
    fn update_then_update_keeps_latest() {
        let c = Conflator::new();
        c.submit(delta(1, DeltaType::Update, "old"));
        c.submit(delta(1, DeltaType::Update, "new"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Update);
        assert_eq!(out[0].row_data.get("v").unwrap(), "new");
    }

    #[test]
    fn update_then_remove_keeps_remove() {
        let c = Conflator::new();
        c.submit(delta(1, DeltaType::Update, "v1"));
        c.submit(delta(1, DeltaType::Remove, "v1"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Remove);
    }

    #[test]
    fn remove_then_add_on_reused_slot_emits_both() {
        // SOW slot reuse: slot 1's old occupant ("old") was deleted and the
        // slot refilled with a different record ("new"). Both the Remove and
        // the Add must survive — collapsing to a single Add would leak the
        // old occupant into the consumer's view forever.
        let c = Conflator::new();
        c.submit(delta(1, DeltaType::Remove, "old"));
        c.submit(delta(1, DeltaType::Add, "new"));
        let out = c.drain();
        assert_eq!(out.len(), 2, "expected both Remove and Add, got {out:?}");
        // Remove is delivered before the reuse Add.
        assert_eq!(out[0].delta_type, DeltaType::Remove);
        assert_eq!(out[0].row_data.get("v").unwrap(), "old");
        assert_eq!(out[1].delta_type, DeltaType::Add);
        assert_eq!(out[1].row_data.get("v").unwrap(), "new");
    }

    #[test]
    fn reused_slot_then_updated_keeps_remove_and_latest_add() {
        // Remove(old) -> Add(new) -> Update(new'): the preserved Remove plus
        // a single Add carrying the latest data.
        let c = Conflator::new();
        c.submit(delta(1, DeltaType::Remove, "old"));
        c.submit(delta(1, DeltaType::Add, "new"));
        c.submit(delta(1, DeltaType::Update, "new2"));
        let out = c.drain();
        assert_eq!(out.len(), 2, "got {out:?}");
        assert_eq!(out[0].delta_type, DeltaType::Remove);
        assert_eq!(out[1].delta_type, DeltaType::Add);
        assert_eq!(out[1].row_data.get("v").unwrap(), "new2");
    }

    #[test]
    fn different_rows_remain_separate() {
        let c = Conflator::new();
        c.submit(delta(0, DeltaType::Add, "a"));
        c.submit(delta(1, DeltaType::Add, "b"));
        c.submit(delta(0, DeltaType::Update, "a2"));
        let out = c.drain();
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn oof_always_wins() {
        let c = Conflator::new();
        c.submit(delta(0, DeltaType::Add, "v1"));
        c.submit(delta(0, DeltaType::Oof, "v1"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Oof);
    }

    #[test]
    fn drain_resets() {
        let c = Conflator::new();
        c.submit(delta(0, DeltaType::Add, "v1"));
        assert_eq!(c.drain().len(), 1);
        assert_eq!(c.drain().len(), 0);
    }
}

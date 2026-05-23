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
//! | Remove + Add      | Add (republished after delete)  |
//! | Remove + Update   | Update (treat as still-present) |
//! | anything + Oof    | Oof (consumer must evict)       |
//!
//! The conflator is pure data structure: callers submit deltas as they're
//! computed; a separate flush loop drains them at the configured interval
//! and forwards them through the transport.

use crate::subscription::{Delta, DeltaType};
use parking_lot::Mutex;
use std::collections::HashMap;

pub struct Conflator {
    pending: Mutex<HashMap<u32, Delta>>,
}

impl Conflator {
    pub fn new() -> Self {
        Conflator {
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Submit a delta. Coalesces with any prior pending delta for the same row.
    pub fn submit(&self, delta: Delta) {
        let mut pending = self.pending.lock();
        match pending.remove(&delta.row) {
            None => {
                pending.insert(delta.row, delta);
            }
            Some(prev) => {
                if let Some(merged) = merge(prev, delta) {
                    pending.insert(merged.row, merged);
                }
                // None → Add+Remove cancelled
            }
        }
    }

    /// Drain all pending deltas. Returns them in unspecified order.
    pub fn drain(&self) -> Vec<Delta> {
        let mut pending = self.pending.lock();
        pending.drain().map(|(_, d)| d).collect()
    }

    pub fn len(&self) -> usize {
        self.pending.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.lock().is_empty()
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
    fn remove_then_add_is_republish() {
        let c = Conflator::new();
        c.submit(delta(1, DeltaType::Remove, "v1"));
        c.submit(delta(1, DeltaType::Add, "v2"));
        let out = c.drain();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].delta_type, DeltaType::Add);
        assert_eq!(out[0].row_data.get("v").unwrap(), "v2");
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

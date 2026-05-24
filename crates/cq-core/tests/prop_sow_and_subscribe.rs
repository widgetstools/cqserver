//! Property-based test for the `sow_and_subscribe` atomicity contract
//! (review concern C1 / worklog session S32, test C1.1).
//!
//! For every random publish/update/delete sequence with a `subscribe`
//! injected at a random offset, the subscriber's reconstructed state
//! (initial snapshot + applied deltas) must equal the topic's final
//! SOW state computed by a fresh query.
//!
//! Catches: missed deltas, duplicate Adds, ordering inversions, and
//! any future change that re-introduces the snapshot-vs-registration
//! race the worklog C1 fix guards against.

use std::collections::HashMap;
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::subscription::DeltaType;
use cq_core::topic::{Topic, TopicConfig};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
enum Op {
    /// Insert or overwrite the row for the given key with the given value.
    Upsert { key: u8, value: i64 },
    /// Delete the row for the given key (no-op if absent).
    Delete { key: u8 },
}

prop_compose! {
    fn any_op()(
        op_tag in 0..3u8,
        key in 0u8..16u8,
        value in any::<i64>(),
    ) -> Op {
        // Skew toward upserts so the SOW reaches a non-trivial size.
        match op_tag {
            0 | 1 => Op::Upsert { key, value },
            _ => Op::Delete { key },
        }
    }
}

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let config = TopicConfig {
        name: "/prop-sas".into(),
        key_fields: vec!["k".into()],
        persist: false,
        conflation_ms: None,
        index_columns: vec![],
        expire_seconds: None,
    };
    Topic::new(config, schema, 256)
}

fn apply(topic: &Topic, op: &Op) {
    match op {
        Op::Upsert { key, value } => {
            let mut m = Map::new();
            m.insert("k".into(), json!(format!("k{:03}", key)));
            m.insert("v".into(), json!(value));
            topic.upsert_map(&m).expect("upsert");
        }
        Op::Delete { key } => {
            let _ = topic.delete(&format!("k{:03}", key));
        }
    }
}

/// Apply `op` to a HashMap that mirrors what the SOW state should be.
fn apply_to_reference(reference: &mut HashMap<String, i64>, op: &Op) {
    match op {
        Op::Upsert { key, value } => {
            reference.insert(format!("k{:03}", key), *value);
        }
        Op::Delete { key } => {
            reference.remove(&format!("k{:03}", key));
        }
    }
}

/// Macro to drain the topic's mutation channel into a delta vec for
/// `$sub_id`. Inline to avoid a typed function parameter that would
/// require the test crate to take a direct dep on `crossbeam_channel`.
macro_rules! drain_into {
    ($topic:expr, $rx:expr, $sub_id:expr) => {{
        let mut out: Vec<(DeltaType, String, Option<i64>)> = Vec::new();
        while let Ok(ev) = $rx.try_recv() {
            let deltas = $topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
            for d in deltas {
                if d.subscription_id == $sub_id {
                    let k = d
                        .row_data
                        .get("k")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .unwrap_or_default();
                    let v = d.row_data.get("v").and_then(Value::as_i64);
                    out.push((d.delta_type, k, v));
                }
            }
        }
        out
    }};
}

/// Reconstruct the subscriber's view of state by replaying the initial
/// snapshot followed by every delivered delta. The result must equal
/// what a fresh SOW query against the topic returns.
fn reconstruct(
    snapshot: &[Map<String, Value>],
    deltas: &[(DeltaType, String, Option<i64>)],
) -> HashMap<String, i64> {
    let mut state: HashMap<String, i64> = HashMap::new();
    for row in snapshot {
        let k = row
            .get("k")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_default();
        if let Some(v) = row.get("v").and_then(Value::as_i64) {
            state.insert(k, v);
        }
    }
    for (kind, k, v) in deltas {
        match kind {
            DeltaType::Add | DeltaType::Update => {
                if let Some(val) = v {
                    state.insert(k.clone(), *val);
                }
            }
            DeltaType::Remove | DeltaType::Oof => {
                state.remove(k);
            }
        }
    }
    state
}

/// Run a recompute over the operation sequence using a HashMap reference.
fn reference_recompute(ops: &[Op]) -> HashMap<String, i64> {
    let mut ref_state = HashMap::new();
    for op in ops {
        apply_to_reference(&mut ref_state, op);
    }
    ref_state
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// `subscribe` injected at any point in a random op stream must
    /// produce a snapshot+delta stream that, when applied client-side,
    /// equals the final SOW state.
    #[test]
    fn snapshot_plus_deltas_equals_final_sow_state(
        ops in prop::collection::vec(any_op(), 1..80),
        split_at in 0usize..80,
    ) {
        let topic = make_topic();
        let split = split_at.min(ops.len());

        // Apply the first `split` ops, then subscribe, then apply the rest.
        for op in &ops[..split] {
            apply(&topic, op);
        }

        // Sparse subscribe so Remove deltas carry the key (sparse
        // path emits `key_only_payload` from the per-row last-snapshot
        // cache when a row drops out, where the non-sparse path
        // re-reads the now-nulled store row and loses the key).
        let (snapshot, _q) = topic
            .subscribe_sparse("sub-prop".into(), "SELECT * FROM t")
            .expect("subscribe");

        // Take the mutation receiver — we own dispatch from here.
        let rx = topic
            .take_mutation_rx()
            .expect("mutation_rx available exactly once");

        // Drain anything left over from pre-subscribe publishes. With
        // the C1 fix these are gated out by `live_start_sequence`;
        // either way, draining now isolates the post-subscribe signal.
        let pre_subscribe_deltas = drain_into!(&topic, rx, "sub-prop");
        prop_assert!(
            pre_subscribe_deltas.is_empty(),
            "pre-subscribe events leaked into the subscription as {} deltas",
            pre_subscribe_deltas.len()
        );

        // Apply post-subscribe ops one at a time and drain each event
        // immediately. This mirrors the production evaluator: events
        // are processed as they're emitted, not in a delayed batch. A
        // delayed-batch dispatch would read store state that reflects
        // the *latest* mutation for every prior event, corrupting
        // sparse `last_snapshot` for any row updated-then-deleted in
        // the same batch (the Update reads post-delete nulls, so the
        // subsequent Remove's key_only_payload comes up empty).
        let mut post_deltas: Vec<(DeltaType, String, Option<i64>)> = Vec::new();
        for op in &ops[split..] {
            apply(&topic, op);
            let batch = drain_into!(&topic, rx, "sub-prop");
            post_deltas.extend(batch);
        }

        // Subscriber's reconstructed state from snapshot + deltas.
        let reconstructed = reconstruct(&snapshot, &post_deltas);

        // Reference state computed from scratch over the whole op log.
        let reference = reference_recompute(&ops);

        prop_assert_eq!(reconstructed, reference);
    }
}

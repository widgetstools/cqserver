//! Property tests pinning SOW + active-set + bookmark behavior against
//! trivial reference implementations (review concern C5 / worklog
//! session S34, tests C5.5 / C5.6 / C5.7 / C5.8).
//!
//! Each test compares `Topic`'s behavior against a `HashMap` or
//! `HashSet` reference that's transparently correct by inspection.
//! These are regression guards: any future change to the store or
//! subscription engine that drifts from the reference fails the test
//! at the smallest random sequence that exposes the drift, with the
//! shrunken input as a debug aid.
//!
//! C5.7 (interpreter vs compiled predicate differential) is documented
//! as deferred at the bottom of this file — the codebase has only one
//! predicate evaluation path today, so there's nothing to
//! differential-test against. The hook is here for when an alternative
//! path lands (e.g., a JIT-compiled fast path under S36-or-later).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::subscription::DeltaType;
use cq_core::topic::{Topic, TopicConfig};
use proptest::prelude::*;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone)]
enum Op {
    Upsert { key: u8, value: i64 },
    Delete { key: u8 },
}

prop_compose! {
    fn any_op()(
        op_tag in 0..4u8,
        key in 0u8..16u8,
        value in any::<i64>(),
    ) -> Op {
        // Skew toward upserts so the SOW has a non-trivial population
        // for the predicate filter to work against.
        if op_tag < 3 {
            Op::Upsert { key, value }
        } else {
            Op::Delete { key }
        }
    }
}

prop_compose! {
    /// Variant restricted to non-negative values, for use with the
    /// `WHERE v > N` predicate test until the compiler accepts
    /// `UnaryOp::Minus` literals.
    fn any_positive_op()(
        op_tag in 0..4u8,
        key in 0u8..16u8,
        value in 0i64..i64::MAX / 2,
    ) -> Op {
        if op_tag < 3 {
            Op::Upsert { key, value }
        } else {
            Op::Delete { key }
        }
    }
}

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let config = TopicConfig {
        name: "/prop-equivalence".into(),
        key_fields: vec!["k".into()],
        persist: false,
        conflation_ms: None,
        index_columns: vec![],
        expire_seconds: None,
    };
    Topic::new(config, schema, 256)
}

fn key_string(k: u8) -> String {
    format!("k{:03}", k)
}

fn apply(topic: &Topic, op: &Op) {
    match op {
        Op::Upsert { key, value } => {
            let mut m = Map::new();
            m.insert("k".into(), json!(key_string(*key)));
            m.insert("v".into(), json!(value));
            topic.upsert_map(&m).expect("upsert");
        }
        Op::Delete { key } => {
            let _ = topic.delete(&key_string(*key));
        }
    }
}

fn apply_to_reference_map(reference: &mut HashMap<String, i64>, op: &Op) {
    match op {
        Op::Upsert { key, value } => {
            reference.insert(key_string(*key), *value);
        }
        Op::Delete { key } => {
            reference.remove(&key_string(*key));
        }
    }
}

fn sow_to_map(topic: &Topic) -> HashMap<String, i64> {
    let result = topic.query("SELECT * FROM t").expect("query");
    let mut out = HashMap::new();
    for row in result.rows {
        let k = row
            .get("k")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_default();
        if let Some(v) = row.get("v").and_then(Value::as_i64) {
            out.insert(k, v);
        }
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// C5.5 — `Topic::query("SELECT * FROM t")` equals a `HashMap`
    /// reference after the same insert/update/delete sequence.
    #[test]
    fn sow_state_equals_hashmap_reference(
        ops in prop::collection::vec(any_op(), 1..120),
    ) {
        let topic = make_topic();
        let mut reference: HashMap<String, i64> = HashMap::new();
        for op in &ops {
            apply(&topic, op);
            apply_to_reference_map(&mut reference, op);
        }
        let sow = sow_to_map(&topic);
        prop_assert_eq!(sow, reference);
    }

    /// C5.6 — `Topic::query("SELECT * FROM t WHERE v > N")` equals a
    /// `HashSet` of keys whose current value satisfies the predicate.
    /// The predicate is randomized so the test covers cases where the
    /// filter includes everyone, no one, and a non-trivial subset.
    ///
    /// Threshold is constrained to non-negative because the predicate
    /// compiler currently rejects unary-minus literals
    /// (`InvalidLiteral("UnaryOp { op: Minus, ... }")`). Fixing that
    /// is a separate concern from S34's reference-equivalence guard
    /// rail — tracked but not blocking. Values are also drawn from a
    /// matching non-negative range so the comparison is meaningful
    /// (with all-positive values and a non-negative threshold the
    /// inclusion set varies non-trivially).
    #[test]
    fn predicate_filtered_sow_equals_hashset_reference(
        ops in prop::collection::vec(any_positive_op(), 1..120),
        threshold in 0i64..i64::MAX / 2,
    ) {
        let topic = make_topic();
        let mut reference: HashMap<String, i64> = HashMap::new();
        for op in &ops {
            apply(&topic, op);
            apply_to_reference_map(&mut reference, op);
        }
        let expected: HashSet<String> = reference
            .iter()
            .filter_map(|(k, v)| if *v > threshold { Some(k.clone()) } else { None })
            .collect();
        let sql = format!("SELECT * FROM t WHERE v > {threshold}");
        let result = topic.query(&sql).expect("filtered query");
        let observed: HashSet<String> = result
            .rows
            .iter()
            .filter_map(|r| r.get("k").and_then(Value::as_str).map(String::from))
            .collect();
        prop_assert_eq!(observed, expected);
    }

    /// C5.8 — subscribe_with_bookmark's `captured` value gates the
    /// live evaluator: events with `seq ≤ captured` are suppressed
    /// (assumed to be covered by the bookmark-replay path), and
    /// events with `seq > captured` are delivered live. This is the
    /// same `live_start_sequence` mechanism that powers S32 / C1, so
    /// the property is implied — this test pins it independently for
    /// the bookmark path so a future refactor can't break one
    /// without the other.
    ///
    /// Note: this is the in-memory portion of the contract. The
    /// txlog-driven replay of `bookmark < seq ≤ captured` is covered
    /// by the e2e `bookmark_replay` test (transport layer).
    #[test]
    fn subscribe_with_bookmark_suppresses_pre_captured_events(
        pre_ops in prop::collection::vec(any_op(), 1..40),
        post_ops in prop::collection::vec(any_op(), 1..40),
    ) {
        let topic = make_topic();

        // Apply pre-subscribe ops.
        for op in &pre_ops {
            apply(&topic, op);
        }

        let (_query, captured) = topic
            .subscribe_with_bookmark("sub-bm".into(), "SELECT * FROM t")
            .expect("subscribe_with_bookmark");

        // Drain pre-subscribe events (which were queued on the
        // mutation channel). All have `seq ≤ captured`, so the live
        // evaluator must suppress them.
        let rx = topic
            .take_mutation_rx()
            .expect("mutation_rx available exactly once");
        let mut pre_deltas = 0usize;
        while let Ok(ev) = rx.try_recv() {
            let deltas = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
            for d in deltas {
                if d.subscription_id.as_ref() == "sub-bm" && d.sequence <= captured {
                    pre_deltas += 1;
                }
            }
        }
        prop_assert_eq!(
            pre_deltas, 0,
            "live evaluator delivered {} pre-captured events to a bookmark sub",
            pre_deltas
        );

        // Apply post-subscribe ops one at a time and drain. Every
        // delta we see must have `seq > captured`. We don't enforce
        // a specific count because Delete-on-absent-key is a silent
        // no-op (no sequence allocated, no event emitted).
        let mut post_deltas: Vec<(DeltaType, u64)> = Vec::new();
        for op in &post_ops {
            apply(&topic, op);
            while let Ok(ev) = rx.try_recv() {
                let deltas = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
                for d in deltas {
                    if d.subscription_id.as_ref() == "sub-bm" {
                        post_deltas.push((d.delta_type, d.sequence));
                    }
                }
            }
        }
        for (kind, seq) in &post_deltas {
            prop_assert!(
                *seq > captured,
                "{:?} delta with seq {seq} ≤ captured {captured} leaked through bookmark gate",
                kind
            );
        }
    }
}

// ============================================================================
// C5.7 — Interpreter vs Compiled Predicate Differential — DEFERRED
// ============================================================================
//
// The codebase has a single predicate path: `CompiledPredicate` produced by
// `parse_query` and evaluated by `CompiledPredicate::matches`. There is no
// separate AST-walker / interpreter to differential-test against. If a JIT
// (cranelift) path lands per the worklog roadmap, OR if a literal AST walker
// is added as a debugging aid, the differential test belongs here:
//
//     proptest! {
//         #[test]
//         fn interpreter_and_compiled_agree(
//             rows in prop::collection::vec(any_row(), 1..50),
//             predicate in any_predicate(),
//         ) {
//             for row in &rows {
//                 prop_assert_eq!(
//                     interpreter_eval(&predicate, row),
//                     compiled_eval(&predicate, row),
//                 );
//             }
//         }
//     }
//
// Skipped today; revisit at S46 (predicate index) or when a JIT lands.

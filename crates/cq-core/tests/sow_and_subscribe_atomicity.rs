//! Deterministic test for the `sow_and_subscribe` atomicity contract
//! (review concern C1 / worklog session S32).
//!
//! Contract: for every publish whose store-mutation is visible in the
//! subscriber's initial snapshot, the subsequent live-delta stream must
//! NOT redeliver that publish. And for every publish that lands strictly
//! after the snapshot is taken, the live-delta stream MUST deliver it.
//!
//! This file deterministically exposes both halves of the contract by
//! keeping the topic's `MutationEvent` channel buffered, then doing
//! `subscribe` against the visible store state, then draining and
//! dispatching the buffered events. A correctly-gated subscription
//! suppresses every event whose sequence is ≤ the captured high-water.

use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::subscription::DeltaType;
use cq_core::topic::{Topic, TopicConfig};

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price", "quantity"],
        &[ColumnType::String, ColumnType::Double, ColumnType::Long],
    ));
    let config = TopicConfig {
        name: "/atomicity-test".into(),
        key_fields: vec!["symbol".into()],
        persist: false,
        conflation_ms: None,
        index_columns: vec![],
        expire_seconds: None,
    };
    Topic::new(config, schema, 256)
}

fn publish(topic: &Topic, sym: &str, price: f64, qty: i64) -> u64 {
    let mut m = serde_json::Map::new();
    m.insert("symbol".into(), sym.into());
    m.insert("price".into(), price.into());
    m.insert("quantity".into(), qty.into());
    topic.upsert_map(&m).expect("publish")
}

/// Pre-snapshot publishes must not be redelivered as live deltas.
///
/// Every `upsert_map` enqueues a `MutationEvent` on the topic's mutation
/// channel. If the subscriber's snapshot captures the row but the
/// evaluator later drains that buffered event and dispatches it, the
/// subscriber receives a duplicate. Under the C1 fix, the subscription
/// remembers the sequence high-water at registration time
/// (`live_start_sequence = captured + 1`) and the evaluator suppresses
/// any event with `sequence < live_start_sequence`.
#[test]
fn pre_snapshot_publishes_are_not_redelivered_as_live_deltas() {
    let topic = make_topic();
    let n = 100;

    // Pre-populate. Every publish queues a MutationEvent on the
    // channel — we don't drain it, so the events sit there waiting.
    for i in 0..n {
        publish(&topic, &format!("S{i:03}"), i as f64, i as i64);
    }

    // Subscribe AFTER the publishes. The snapshot must cover all N
    // rows; the subscription must reject the buffered events as
    // already-snapshotted.
    let (snapshot, _) = topic
        .subscribe("sub-1".into(), "SELECT * FROM t")
        .expect("subscribe");
    assert_eq!(snapshot.len(), n, "snapshot must cover all pre-publishes");

    // Drain the channel and dispatch every queued event through the
    // evaluator. A correctly gated subscription emits zero deltas for
    // sub-1 because every queued sequence is ≤ the high-water captured
    // at registration.
    let rx = topic
        .take_mutation_rx()
        .expect("mutation_rx available exactly once");
    let mut deltas_for_sub: Vec<(DeltaType, u64)> = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        let deltas = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
        for d in deltas {
            if d.subscription_id.as_ref() == "sub-1" {
                deltas_for_sub.push((d.delta_type, d.sequence));
            }
        }
    }

    assert!(
        deltas_for_sub.is_empty(),
        "subscriber received {} duplicate deltas for events already in its snapshot: {:?}",
        deltas_for_sub.len(),
        deltas_for_sub
    );
}

/// Post-snapshot publishes must be delivered as live deltas.
///
/// The flip side of the contract: a publish whose sequence is strictly
/// greater than the high-water captured at registration MUST flow
/// through the evaluator and surface as a delta. This guards against
/// over-aggressive gating that would drop legitimate live events.
#[test]
fn post_snapshot_publishes_are_delivered_as_live_deltas() {
    let topic = make_topic();

    // Pre-populate, then subscribe.
    publish(&topic, "PRE", 1.0, 1);
    let (snapshot, _) = topic
        .subscribe("sub-1".into(), "SELECT * FROM t")
        .expect("subscribe");
    assert_eq!(snapshot.len(), 1);

    // Drain pre-subscribe events first so they don't pollute the
    // post-subscribe count. With the C1 fix these are suppressed
    // anyway; without it they'd be duplicates handled by the
    // other test. Either way, clearing the channel before publishing
    // new rows isolates this test's signal.
    let rx = topic
        .take_mutation_rx()
        .expect("mutation_rx available exactly once");
    while let Ok(ev) = rx.try_recv() {
        let _ = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
    }

    // Publish AFTER subscription. Every new row must surface as a
    // delta when we dispatch.
    let n_new = 50;
    for i in 0..n_new {
        publish(&topic, &format!("POST{i:03}"), 10.0 + i as f64, i as i64);
    }

    let mut adds: Vec<u64> = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        let deltas = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
        for d in deltas {
            if d.subscription_id.as_ref() == "sub-1" && d.delta_type == DeltaType::Add {
                adds.push(d.sequence);
            }
        }
    }

    assert_eq!(
        adds.len(),
        n_new,
        "expected {n_new} live Add deltas for post-snapshot publishes, got {}: {:?}",
        adds.len(),
        adds
    );
}

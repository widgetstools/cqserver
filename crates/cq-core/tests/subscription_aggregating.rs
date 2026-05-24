//! S19 — continuous-aggregate subscription tests.
//!
//! A subscribe with `SELECT ... GROUP BY ...` should:
//!   1. Return the initial aggregate output as the snapshot.
//!   2. Emit per-group Add/Update/Remove deltas on subsequent
//!      mutations, with the delta carrying the group's current row.

use std::collections::HashMap;
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::subscription::DeltaType;
use cq_core::topic::{Topic, TopicConfig};
use serde_json::{json, Map, Value};

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/agg-sub".into(),
            key_fields: vec!["trader".into(), "desk".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    )
}

fn publish(topic: &Topic, trader: &str, desk: &str, qty: i64) {
    let mut m = Map::new();
    m.insert("trader".into(), json!(trader));
    m.insert("desk".into(), json!(desk));
    m.insert("qty".into(), json!(qty));
    topic.upsert_map(&m).expect("publish");
}

/// Drain the mutation channel through the evaluator, collecting
/// deltas dispatched to `sub_id`. Caller takes the rx once.
macro_rules! drain {
    ($topic:expr, $rx:expr, $sub_id:expr) => {{
        let mut out: Vec<(DeltaType, Map<String, Value>)> = Vec::new();
        while let Ok(ev) = $rx.try_recv() {
            for d in $topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind) {
                if d.subscription_id == $sub_id {
                    out.push((d.delta_type, (*d.row_data).clone()));
                }
            }
        }
        out
    }};
}

#[test]
fn aggregating_sub_initial_snapshot_has_one_row_per_group() {
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "bob", "RATES", 200);
    publish(&topic, "alice", "FX", 50);

    let (snapshot, _) = topic
        .subscribe("agg-sub".into(), "SELECT desk, COUNT(*) AS n FROM t GROUP BY desk")
        .expect("subscribe aggregate");
    // Two groups: RATES (2 rows) + FX (1 row).
    assert_eq!(snapshot.len(), 2);
    let by_desk: HashMap<String, i64> = snapshot
        .iter()
        .filter_map(|r| {
            let d = r.get("desk").and_then(Value::as_str)?.to_string();
            let n = r.get("n").and_then(Value::as_i64)?;
            Some((d, n))
        })
        .collect();
    assert_eq!(by_desk["RATES"], 2);
    assert_eq!(by_desk["FX"], 1);
}

#[test]
fn aggregating_sub_emits_update_when_a_group_changes() {
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 50);

    topic
        .subscribe("agg-sub".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // New publish adds to RATES — that group's total moves from 100 → 150.
    publish(&topic, "bob", "RATES", 50);
    let deltas = drain!(&topic, rx, "agg-sub");

    // Expect at least one Update for RATES (total=150). FX should NOT
    // appear because its group didn't change.
    let rates_updates: Vec<&(DeltaType, Map<String, Value>)> = deltas
        .iter()
        .filter(|(_, row)| row.get("desk").and_then(Value::as_str) == Some("RATES"))
        .collect();
    assert!(
        !rates_updates.is_empty(),
        "expected at least one delta for the RATES group"
    );
    let (kind, body) = rates_updates.last().unwrap();
    assert_eq!(*kind, DeltaType::Update);
    assert_eq!(body.get("total").unwrap(), 150);

    // FX shouldn't have any delta this round.
    let fx_changes = deltas
        .iter()
        .filter(|(_, row)| row.get("desk").and_then(Value::as_str) == Some("FX"))
        .count();
    assert_eq!(fx_changes, 0, "FX group shouldn't change on a RATES publish");
}

#[test]
fn aggregating_sub_emits_add_when_a_new_group_appears() {
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    topic
        .subscribe("agg-sub".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // Publish into a brand-new group `FX`.
    publish(&topic, "alice", "FX", 200);
    let deltas = drain!(&topic, rx, "agg-sub");

    // Expect an Add for FX. RATES might also surface (executor
    // re-runs the whole agg) but its total didn't change so we
    // wouldn't see an Update for it.
    let fx_add = deltas
        .iter()
        .find(|(k, row)| {
            *k == DeltaType::Add && row.get("desk").and_then(Value::as_str) == Some("FX")
        })
        .expect("expected an Add for the new FX group");
    assert_eq!(fx_add.1.get("total").unwrap(), 200);
}

#[test]
fn aggregating_sub_emits_remove_when_a_group_empties() {
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "alice", "FX", 50);
    topic
        .subscribe("agg-sub".into(), "SELECT desk, COUNT(*) AS n FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // Delete the only FX row → FX group disappears.
    topic.delete("alice|FX").expect("delete");
    let deltas = drain!(&topic, rx, "agg-sub");
    let fx_remove = deltas.iter().find(|(k, row)| {
        *k == DeltaType::Remove && row.get("desk").and_then(Value::as_str) == Some("FX")
    });
    assert!(
        fx_remove.is_some(),
        "expected Remove delta for the now-empty FX group; got: {:?}",
        deltas
    );
}

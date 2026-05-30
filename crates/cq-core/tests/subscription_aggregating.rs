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
                if d.subscription_id.as_ref() == $sub_id {
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

/// A topic keyed by `trader` only, so re-publishing the same trader
/// with a different `desk` MOVES that row between GROUP BY desk groups
/// (the key is stable; the group-by column changes).
fn make_move_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/agg-move".into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    )
}

/// Apply a delta stream to a client-side view keyed by `desk`,
/// mirroring how a real subscriber would maintain its picture.
fn apply_deltas(
    view: &mut HashMap<String, Map<String, Value>>,
    deltas: &[(DeltaType, Map<String, Value>)],
) {
    for (kind, row) in deltas {
        // A tombstoned-source group surfaces with a null `desk`; key it
        // by a sentinel so transient null-group Add/Remove pairs (under
        // evaluator lag) round-trip cleanly.
        let desk = match row.get("desk") {
            Some(Value::String(s)) => s.clone(),
            _ => "<null>".to_string(),
        };
        match kind {
            DeltaType::Add | DeltaType::Update => {
                view.insert(desk, row.clone());
            }
            DeltaType::Remove => {
                view.remove(&desk);
            }
            _ => {}
        }
    }
}

fn snapshot_by_desk(rows: &[Map<String, Value>]) -> HashMap<String, Map<String, Value>> {
    rows.iter()
        .map(|r| {
            (
                r.get("desk").and_then(Value::as_str).unwrap().to_string(),
                r.clone(),
            )
        })
        .collect()
}

/// Steady-state: the SECOND mutation into a group (after the seed
/// event) must still emit a correct running-total Update — i.e. the
/// incremental fast path, not just the one-time seed, is exercised.
#[test]
fn aggregating_sub_incremental_path_updates_running_total() {
    let topic = make_topic();
    publish(&topic, "alice", "RATES", 100);
    topic
        .subscribe("agg-sub".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // 1st event → seeds membership maps (total 100 → 150).
    publish(&topic, "bob", "RATES", 50);
    // 2nd event → incremental fast path (total 150 → 200).
    publish(&topic, "carol", "RATES", 50);

    let deltas = drain!(&topic, rx, "agg-sub");
    let last_rates = deltas
        .iter()
        .filter(|(_, row)| row.get("desk").and_then(Value::as_str) == Some("RATES"))
        .last()
        .expect("expected RATES deltas");
    assert_eq!(last_rates.0, DeltaType::Update);
    assert_eq!(last_rates.1.get("total").unwrap(), 200);
}

/// Updating a row's GROUP BY column moves it between groups: the old
/// group loses the row, the new group gains it.
#[test]
fn aggregating_sub_handles_group_move() {
    let topic = make_move_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "bob", "RATES", 40);
    topic
        .subscribe("agg".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // Move alice from RATES → FX (same key `alice`, new desk).
    publish(&topic, "alice", "FX", 100);
    let deltas = drain!(&topic, rx, "agg");

    // RATES should drop to 40 (Update), FX should appear at 100 (Add).
    let rates = deltas
        .iter()
        .filter(|(_, r)| r.get("desk").and_then(Value::as_str) == Some("RATES"))
        .last()
        .expect("RATES delta");
    assert_eq!(rates.0, DeltaType::Update);
    assert_eq!(rates.1.get("total").unwrap(), 40);

    let fx = deltas
        .iter()
        .find(|(k, r)| {
            *k == DeltaType::Add && r.get("desk").and_then(Value::as_str) == Some("FX")
        })
        .expect("FX Add delta");
    assert_eq!(fx.1.get("total").unwrap(), 100);
}

/// The incremental evaluator must converge to the exact same per-group
/// output as a full recompute. Drive a varied mutation stream through
/// one subscription, fold its deltas into a client-side view, then
/// compare against a freshly-subscribed snapshot (ground-truth full
/// recompute over the current store).
#[test]
fn incremental_aggregate_equivalent_to_full_recompute() {
    let topic = make_move_topic();
    publish(&topic, "alice", "RATES", 100);
    publish(&topic, "bob", "FX", 50);

    let (snapshot, _) = topic
        .subscribe("agg".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");
    let mut view = snapshot_by_desk(&snapshot);

    // Mix of: new group, group growth, group move, and a delete that
    // empties a group.
    publish(&topic, "carol", "EQ", 200); // new EQ group
    publish(&topic, "dave", "RATES", 25); // RATES grows
    publish(&topic, "alice", "FX", 100); // alice RATES→FX move
    publish(&topic, "bob", "FX", 75); // FX in-place change
    topic.delete("dave").expect("delete dave"); // RATES shrinks
    topic.delete("alice").expect("delete alice"); // FX shrinks
    topic.delete("bob").expect("delete bob"); // FX now empty → Remove

    let deltas = drain!(&topic, rx, "agg");
    apply_deltas(&mut view, &deltas);

    // Ground truth: a fresh subscription's snapshot is a full recompute
    // over the current store.
    let (truth_rows, _) = topic
        .subscribe("truth".into(), "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk")
        .expect("subscribe truth");
    let truth = snapshot_by_desk(&truth_rows);

    assert_eq!(
        view, truth,
        "incremental view diverged from full recompute\nview={:?}\ntruth={:?}",
        view, truth
    );
}

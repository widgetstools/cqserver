//! SOW slot reclamation: deleted store slots are returned to a free-list
//! and reused by later inserts (AMPS-style slot reuse), bounding store
//! growth to the live-set size while keeping subscription delivery correct.

use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{MutationKind, Topic, TopicConfig};
use serde_json::{json, Map, Value};

fn make_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/reclaim".into(),
            key_fields: vec!["k".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        16,
    )
}

fn row(k: &str, v: i64) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("k".into(), json!(k));
    m.insert("v".into(), json!(v));
    m
}

#[test]
fn delete_then_insert_reuses_slots_without_growth() {
    let topic = make_topic();
    let n = 200i64;

    // Insert N distinct keys.
    for i in 0..n {
        topic.upsert_map(&row(&format!("k{i}"), i)).unwrap();
    }
    assert_eq!(topic.row_count(), n as u32, "high-water after first insert");

    // Delete all of them — every slot is freed.
    for i in 0..n {
        topic.delete(&format!("k{i}")).unwrap();
    }

    // Insert N *new* distinct keys. With slot reclamation these reuse the
    // freed slots, so the store high-water must NOT grow to 2N.
    for i in n..(2 * n) {
        topic.upsert_map(&row(&format!("k{i}"), i)).unwrap();
    }
    assert_eq!(
        topic.row_count(),
        n as u32,
        "store high-water should stay bounded to the live-set size via slot reuse, not grow to 2N"
    );

    // And the live SOW is exactly the new N keys.
    let result = topic.query("SELECT * FROM t").unwrap();
    assert_eq!(result.rows.len(), n as usize);
}

#[test]
fn reused_slot_delivers_remove_of_old_then_add_of_new() {
    let topic = make_topic();
    // Subscribe before any data so the active set starts empty and we can
    // observe each delta as the event flows through the evaluator.
    topic.subscribe("s".into(), "SELECT * FROM t").expect("subscribe");
    let rx = topic.take_mutation_rx().expect("rx");

    // Insert A, evaluate -> Add(A) at some slot.
    topic.upsert_map(&row("A", 1)).unwrap();
    let ev = rx.recv().unwrap();
    let a_row = ev.row;
    let d = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].delta_type, cq_core::subscription::DeltaType::Add);
    assert_eq!(d[0].row_data.get("k").and_then(Value::as_str), Some("A"));

    // Delete A -> the slot is freed; evaluate -> Remove (empty body).
    topic.delete("A").unwrap();
    let ev = rx.recv().unwrap();
    assert_eq!(ev.kind, MutationKind::Delete);
    let d = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].delta_type, cq_core::subscription::DeltaType::Remove);

    // Insert B -> must reuse A's freed slot.
    topic.upsert_map(&row("B", 2)).unwrap();
    let ev = rx.recv().unwrap();
    assert_eq!(ev.row, a_row, "B should reuse the slot A vacated");
    let d = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].delta_type, cq_core::subscription::DeltaType::Add);
    // The reuse Add carries B's data, never A's.
    assert_eq!(d[0].row_data.get("k").and_then(Value::as_str), Some("B"));
    assert_eq!(d[0].row_data.get("v").and_then(Value::as_i64), Some(2));

    // Final SOW: only B.
    let result = topic.query("SELECT * FROM t").unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("k").and_then(Value::as_str), Some("B"));
}

fn make_grouped_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Topic::new(
        TopicConfig {
            name: "/grp".into(),
            key_fields: vec!["k".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        16,
    )
}

fn grow(k: &str, desk: &str, qty: i64) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("k".into(), json!(k));
    m.insert("desk".into(), json!(desk));
    m.insert("qty".into(), json!(qty));
    m
}

#[test]
fn aggregate_membership_correct_when_slot_reused_across_groups() {
    // A keyed row in group RATES is deleted; a *new* key joining a
    // different group (FX) reuses its slot. The incremental aggregate must
    // move the freed slot out of RATES and the reused slot into FX, not
    // conflate the two occupants.
    let topic = make_grouped_topic();
    topic.upsert_map(&grow("alice", "RATES", 100)).unwrap();
    topic.upsert_map(&grow("bob", "FX", 50)).unwrap();

    topic
        .subscribe(
            "agg".into(),
            "SELECT desk, COUNT(*) AS n, SUM(qty) AS total FROM t GROUP BY desk",
        )
        .expect("subscribe agg");
    let rx = topic.take_mutation_rx().expect("rx");

    // Delete alice (RATES slot freed), then add carol on FX reusing it.
    topic.delete("alice").unwrap();
    let ev = rx.recv().unwrap();
    let _ = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);

    topic.upsert_map(&grow("carol", "FX", 25)).unwrap();
    let ev = rx.recv().unwrap();
    let _ = topic.evaluate_row_kind(ev.row, ev.sequence, ev.kind);

    // Reference: a fresh aggregate over the live SOW. RATES is now empty
    // (alice gone), FX has bob(50) + carol(25) = 2 rows / 75.
    let result = topic
        .query("SELECT desk, COUNT(*) AS n, SUM(qty) AS total FROM t GROUP BY desk")
        .unwrap();
    let by_desk: std::collections::HashMap<String, (i64, i64)> = result
        .rows
        .iter()
        .filter_map(|r| {
            let d = r.get("desk").and_then(Value::as_str)?.to_string();
            let n = r.get("n").and_then(Value::as_i64)?;
            let t = r.get("total").and_then(Value::as_i64)?;
            Some((d, (n, t)))
        })
        .collect();
    assert_eq!(by_desk.get("FX"), Some(&(2, 75)));
    assert_eq!(by_desk.get("RATES"), None, "RATES group should be gone");
}

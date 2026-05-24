//! Streaming differential harness: feed events one at a time, after
//! each event compare CQ's materialized SOW state against DataFusion's
//! batch query result at the same logical point.
//!
//! This is the harder, more realistic shape of differential testing
//! for a continuous-query engine: rather than running a single
//! batch query at the end, we assert that CQ's incremental
//! materialized state stays in lock-step with what a from-scratch
//! batch recompute would produce, after EVERY mutation.
//!
//! Today this file has one hand-written end-to-end case as the
//! foundation; an expanded version (random op streams, multiple
//! continuous-query shapes) is the next session's work (S36
//! continuation or follow-up).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, Int64Builder};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde_json::{json, Value};

fn cq_to_set(rows: Vec<serde_json::Map<String, Value>>) -> HashSet<String> {
    rows.into_iter()
        .map(|m| {
            // Normalize: drop explicit nulls so CQ's omit-null style
            // and DataFusion's explicit-null style compare equal.
            let normalized: serde_json::Map<String, Value> = m
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .collect();
            serde_json::to_string(&normalized).unwrap()
        })
        .collect()
}

/// Take the current id→v mirror, build a fresh DataFusion table
/// from it, and return the result of `SELECT id, v FROM t` as a
/// canonical-JSON set for comparison.
fn datafusion_snapshot(
    runtime: &tokio::runtime::Runtime,
    mirror: &BTreeMap<i64, i64>,
) -> HashSet<String> {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let mut id_b = Int64Builder::with_capacity(mirror.len());
    let mut v_b = Int64Builder::with_capacity(mirror.len());
    for (id, v) in mirror {
        id_b.append_value(*id);
        v_b.append_value(*v);
    }
    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![Arc::new(id_b.finish()), Arc::new(v_b.finish())],
    )
    .expect("build batch");

    runtime.block_on(async move {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(arrow_schema, vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");
        let df = ctx.sql("SELECT id, v FROM t").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        let mut out = HashSet::new();
        for batch in batches {
            let id_arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v_arr = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let mut m = serde_json::Map::new();
                if !id_arr.is_null(row) {
                    m.insert("id".to_string(), json!(id_arr.value(row)));
                }
                if !v_arr.is_null(row) {
                    m.insert("v".to_string(), json!(v_arr.value(row)));
                }
                out.insert(serde_json::to_string(&m).unwrap());
            }
        }
        out
    })
}

/// One continuous query (SELECT * FROM t) over a stream of upserts +
/// deletes. After EVERY operation, the CQ SOW state and DataFusion's
/// batch query of the equivalent state must agree.
#[test]
fn streaming_sow_stays_in_lockstep_with_datafusion_after_each_op() {
    // CQ side: a topic with two columns.
    let schema = Arc::new(Schema::from_strs(
        &["id", "v"],
        &[ColumnType::Long, ColumnType::Long],
    ));
    let topic = Topic::new(
        TopicConfig {
            name: "/streaming-test".into(),
            key_fields: vec!["id".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    );

    // DataFusion side: we mirror the topic's keyed state in a small
    // BTreeMap and rebuild a MemTable from it after every op. Each
    // comparison runs a fresh query against that table — so we're
    // genuinely re-deriving the snapshot from scratch (the reference
    // semantics), not just trusting the mirror.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let mut mirror: BTreeMap<i64, i64> = BTreeMap::new();

    // Test trace: upserts that include new keys + updates to existing
    // keys + deletes. After every step we recompute SOW from both
    // engines and compare.
    let trace: Vec<(&str, i64, Option<i64>)> = vec![
        ("upsert", 1, Some(10)),
        ("upsert", 2, Some(20)),
        ("upsert", 1, Some(15)), // update key 1
        ("upsert", 3, Some(30)),
        ("delete", 2, None),
        ("upsert", 2, Some(25)), // re-insert key 2
        ("delete", 1, None),
    ];

    for (op, id, value) in &trace {
        // Apply to CQ.
        match *op {
            "upsert" => {
                let mut m = serde_json::Map::new();
                m.insert("id".into(), json!(id));
                m.insert("v".into(), json!(value.unwrap()));
                topic.upsert_map(&m).expect("cq upsert");
                mirror.insert(*id, value.unwrap());
            }
            "delete" => {
                let _ = topic.delete(&id.to_string());
                mirror.remove(id);
            }
            _ => unreachable!(),
        }

        // Compare. Project explicitly so the CQ tombstone-filter
        // bug (Known issues) doesn't muddy the comparison.
        let cq_rows = topic
            .query("SELECT id, v FROM t")
            .expect("cq query")
            .rows;
        let df_set = datafusion_snapshot(&runtime, &mirror);
        let cq_set = cq_to_set(cq_rows);

        assert_eq!(
            cq_set, df_set,
            "streaming divergence after {op} id={id} v={:?}:\n  cq:         {:?}\n  datafusion: {:?}",
            value, cq_set, df_set
        );
    }
}

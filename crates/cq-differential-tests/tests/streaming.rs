//! Streaming differential harness: feed events one at a time, after
//! each event compare CQ's materialized SOW state against DuckDB's
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

use std::collections::HashSet;
use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use duckdb::Connection;
use serde_json::{json, Value};

fn cq_to_set(rows: Vec<serde_json::Map<String, Value>>) -> HashSet<String> {
    rows.into_iter()
        .map(|m| {
            // Normalize: drop explicit nulls so CQ's omit-null style
            // and DuckDB's explicit-null style compare equal.
            let normalized: serde_json::Map<String, Value> = m
                .into_iter()
                .filter(|(_, v)| !v.is_null())
                .collect();
            serde_json::to_string(&normalized).unwrap()
        })
        .collect()
}

fn duckdb_rows_to_set(conn: &Connection, sql: &str) -> HashSet<String> {
    let mut stmt = conn.prepare(sql).expect("prep");
    let mut rows = stmt.query([]).expect("query");
    let names: Vec<String> = rows
        .as_ref()
        .map(|s| {
            s.column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();
    let mut out = HashSet::new();
    while let Some(row) = rows.next().expect("next") {
        let mut map = serde_json::Map::new();
        for (i, n) in names.iter().enumerate() {
            let v: duckdb::types::Value = row.get(i).expect("col");
            let jv = match v {
                duckdb::types::Value::Null => Value::Null,
                duckdb::types::Value::BigInt(n) => Value::Number(n.into()),
                duckdb::types::Value::Int(n) => Value::Number((n as i64).into()),
                duckdb::types::Value::Double(n) => {
                    serde_json::Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
                }
                duckdb::types::Value::Text(s) => Value::String(s),
                other => Value::String(format!("{other:?}")),
            };
            if !jv.is_null() {
                map.insert(n.clone(), jv);
            }
        }
        out.insert(serde_json::to_string(&map).unwrap());
    }
    out
}

/// One continuous query (SELECT * FROM t) over a stream of upserts +
/// deletes. After EVERY operation, the CQ SOW state and DuckDB's
/// batch query of the equivalent state must agree.
#[test]
fn streaming_sow_stays_in_lockstep_with_duckdb_after_each_op() {
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

    // DuckDB side: matching schema.
    let conn = Connection::open_in_memory().expect("open");
    conn.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", [])
        .expect("create");

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
            }
            "delete" => {
                let _ = topic.delete(&id.to_string());
            }
            _ => unreachable!(),
        }
        // Apply to DuckDB. UPSERT semantics: DELETE existing, then
        // INSERT (with PRIMARY KEY clearing handled implicitly by
        // INSERT OR REPLACE).
        match *op {
            "upsert" => {
                let v = value.unwrap();
                conn.execute(
                    "INSERT OR REPLACE INTO t (id, v) VALUES (?, ?)",
                    [
                        &(*id as i64) as &dyn duckdb::ToSql,
                        &v as &dyn duckdb::ToSql,
                    ],
                )
                .expect("dd upsert");
            }
            "delete" => {
                conn.execute(
                    "DELETE FROM t WHERE id = ?",
                    [&(*id as i64) as &dyn duckdb::ToSql],
                )
                .expect("dd delete");
            }
            _ => unreachable!(),
        }

        // Compare. Project explicitly so the CQ tombstone-filter
        // bug (Known issues) doesn't muddy the comparison.
        let cq_rows = topic
            .query("SELECT id, v FROM t")
            .expect("cq query")
            .rows;
        let dd_set = duckdb_rows_to_set(&conn, "SELECT id, v FROM t");
        let cq_set = cq_to_set(cq_rows);

        assert_eq!(
            cq_set, dd_set,
            "streaming divergence after {op} id={id} v={:?}:\n  cq:     {:?}\n  duckdb: {:?}",
            value, cq_set, dd_set
        );
    }
}

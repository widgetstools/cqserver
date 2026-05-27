//! S20 stress: a view's SOW converges to a from-scratch aggregate of
//! the source under concurrent multi-thread publish + delete pressure.
//!
//! Scenario: N writer threads each publish K rows into a shared source
//! topic (distinct trader keys per thread so writes don't collide on
//! the key index). A subset of threads also interleave deletes against
//! their own keys. A continuous-aggregate view runs in parallel,
//! re-aggregating on every source mutation. After every writer
//! completes, the test asserts that the view's SOW exactly equals
//! the source's from-scratch aggregate (the ground truth).

use std::sync::Arc;
use std::thread;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use cq_core::view::{spawn_view_runner, View};
use serde_json::{json, Map, Value};

fn make_source() -> Arc<Topic> {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ));
    Arc::new(Topic::new(
        TopicConfig {
            name: "/stress-trades".into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        4_096,
    ))
}

fn publish(topic: &Topic, trader: &str, desk: &str, qty: i64) {
    let mut m = Map::new();
    m.insert("trader".into(), json!(trader));
    m.insert("desk".into(), json!(desk));
    m.insert("qty".into(), json!(qty));
    topic.upsert_map(&m).expect("publish");
}

#[test]
fn view_converges_under_concurrent_pressure() {
    let src = make_source();
    let view_sql =
        "SELECT desk, SUM(qty) AS total, COUNT(*) AS n FROM t GROUP BY desk";

    // Build view topic + runner.
    let (view_topic, query, group_by_names) =
        View::build_view_topic(&src, view_sql, "/stress-view".into(), 256)
            .expect("build view");
    let view_topic_arc = Arc::new(view_topic);
    let (_tap_id, tap_rx) = src.register_view_tap(8_192);
    let view = View::new(
        src.clone(),
        view_topic_arc.clone(),
        query,
        group_by_names,
        None,
    )
    .expect("view");
    let _runner = spawn_view_runner(view.clone(), tap_rx);

    // 8 writer threads × 250 rows each — 2K source publishes total. A
    // small desk set (3 desks) plus mod-3 rotation ensures contention
    // on each group, so the view sees real updates not just inserts.
    let desks = ["RATES", "FX", "EQUITIES"];
    let threads_n = 8;
    let per_thread = 250;
    let mut handles = Vec::with_capacity(threads_n);
    for tid in 0..threads_n {
        let src = src.clone();
        let desks_local = desks;
        handles.push(thread::spawn(move || {
            for i in 0..per_thread {
                let trader = format!("t{}-r{}", tid, i);
                let desk = desks_local[(tid + i) % desks_local.len()];
                publish(&src, &trader, desk, (i as i64 + 1) * 7);
                // Occasionally delete one of this thread's earlier keys
                // to exercise the Remove path on the view.
                if i > 0 && i % 13 == 0 {
                    let target = format!("t{}-r{}", tid, i - 1);
                    let _ = src.delete(&target);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("writer thread");
    }

    // Wait for the view to converge. The runner is single-threaded
    // and the source's tap channel might still be draining when the
    // last writer returns; spin-wait with a generous budget.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let canonicalize = |rows: &[Map<String, Value>]| -> std::collections::BTreeMap<String, (i64, i64)> {
        rows.iter()
            .filter_map(|r| {
                let d = r.get("desk").and_then(Value::as_str)?.to_string();
                let t = r.get("total").and_then(Value::as_i64)?;
                let n = r.get("n").and_then(Value::as_i64)?;
                Some((d, (t, n)))
            })
            .collect()
    };

    let mut converged = false;
    while std::time::Instant::now() < deadline {
        let source_agg = src.query(
            "SELECT desk, SUM(qty) AS total, COUNT(*) AS n FROM t GROUP BY desk",
        ).expect("source aggregate");
        let view_rows = view_topic_arc
            .query("SELECT desk, total, n FROM t")
            .expect("view query");
        if canonicalize(&source_agg.rows) == canonicalize(&view_rows.rows) {
            converged = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(converged, "view never converged to source aggregate under concurrent pressure");
}

//! S20 — view materialization unit tests.
//!
//! A view is a derived topic. Each test:
//!   1. Builds a source topic (trader/desk/qty schema).
//!   2. Builds a view topic + `View` over a SELECT-GROUP-BY query.
//!   3. Publishes / deletes against the source.
//!   4. Verifies the view's SOW matches a from-scratch aggregate
//!      recompute over the same input log.

use std::sync::Arc;

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
            name: "/trades".into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    ))
}

fn publish(topic: &Topic, trader: &str, desk: &str, qty: i64) {
    let mut m = Map::new();
    m.insert("trader".into(), json!(trader));
    m.insert("desk".into(), json!(desk));
    m.insert("qty".into(), json!(qty));
    topic.upsert_map(&m).expect("publish");
}

/// Build a view + register it. Returns the constructed `Arc<View>`,
/// the shared view topic (for direct querying), and the runner join
/// handle. The runner is left running until the test ends; it'll
/// exit cleanly when the source topic is dropped.
fn build_view(
    source: Arc<Topic>,
    sql: &str,
    view_name: &str,
) -> (Arc<View>, Arc<Topic>) {
    let (view_topic, query, group_by_names) =
        View::build_view_topic(&source, sql, view_name.into(), 64)
            .expect("build view topic");
    let view_topic_arc = Arc::new(view_topic);
    let (_tap_id, tap_rx) = source.register_view_tap(1024);
    let view = View::new(
        source.clone(),
        view_topic_arc.clone(),
        query,
        group_by_names,
        None,
    )
    .expect("new view");
    let _ = spawn_view_runner(view.clone(), tap_rx);
    (view, view_topic_arc)
}

/// Run a closure repeatedly with a short backoff until the predicate
/// holds or the budget expires. Used to await async refreshes.
fn wait_for<F: FnMut() -> bool>(mut f: F) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if f() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    panic!("wait_for: predicate never held within budget");
}

fn view_totals(vtopic: &Topic) -> std::collections::HashMap<String, (i64, i64)> {
    vtopic
        .query("SELECT desk, total, n FROM t")
        .unwrap()
        .rows
        .into_iter()
        .filter_map(|r| {
            let d = r.get("desk").and_then(Value::as_str)?.to_string();
            let t = r.get("total").and_then(Value::as_i64)?;
            let n = r.get("n").and_then(Value::as_i64)?;
            Some((d, (t, n)))
        })
        .collect()
}

#[test]
fn debounce_coalesces_a_burst_into_few_refreshes() {
    let src = make_source();
    let (view, vtopic) = build_view(
        src.clone(),
        "SELECT desk, SUM(qty) AS total, COUNT(*) AS n FROM t GROUP BY desk",
        "/trades_by_desk_burst",
    );
    let baseline = view.refresh_count(); // initial population pass

    // Tight, continuous burst — no inter-row gap (microseconds each),
    // so it all lands well inside one QUIET_WINDOW.
    const N: i64 = 500;
    for i in 0..N {
        let desk = if i % 2 == 0 { "RATES" } else { "FX" };
        publish(&src, &format!("t{i}"), desk, i);
    }

    let expect_rates: i64 = (0..N).filter(|i| i % 2 == 0).sum();
    let expect_fx: i64 = (0..N).filter(|i| i % 2 == 1).sum();
    wait_for(|| {
        let m = view_totals(&vtopic);
        m.get("RATES").copied() == Some((expect_rates, N / 2))
            && m.get("FX").copied() == Some((expect_fx, N / 2))
    });

    // The debounce must collapse the 500-row burst into a handful of
    // refreshes — emphatically NOT one per row.
    let refreshes = view.refresh_count() - baseline;
    assert!(
        refreshes <= 10,
        "expected burst to coalesce into <=10 refreshes, got {refreshes}"
    );
}

#[test]
fn view_initial_population_matches_source_aggregate() {
    let src = make_source();
    publish(&src, "alice", "RATES", 100);
    publish(&src, "bob", "RATES", 200);
    publish(&src, "carol", "FX", 50);

    let (_view, vtopic) = build_view(
        src.clone(),
        "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
        "/trades_by_desk",
    );

    // View was seeded by `View::new` BEFORE any tap event fired, so
    // the SOW should already match.
    let result = vtopic
        .query("SELECT desk, total FROM t")
        .expect("query view");
    let by_desk: std::collections::HashMap<String, i64> = result
        .rows
        .into_iter()
        .filter_map(|r| {
            let d = r.get("desk").and_then(Value::as_str)?.to_string();
            let t = r.get("total").and_then(Value::as_i64)?;
            Some((d, t))
        })
        .collect();
    assert_eq!(by_desk.len(), 2);
    assert_eq!(by_desk["RATES"], 300);
    assert_eq!(by_desk["FX"], 50);
}

#[test]
fn view_reflects_subsequent_source_publishes() {
    let src = make_source();
    publish(&src, "alice", "RATES", 100);
    let (_view, vtopic) = build_view(
        src.clone(),
        "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
        "/trades_by_desk_2",
    );

    publish(&src, "bob", "RATES", 50); // RATES: 100 → 150
    publish(&src, "carol", "FX", 30); // FX: appears

    wait_for(|| {
        let result = vtopic.query("SELECT desk, total FROM t").unwrap();
        let m: std::collections::HashMap<String, i64> = result
            .rows
            .into_iter()
            .filter_map(|r| {
                let d = r.get("desk").and_then(Value::as_str)?.to_string();
                let t = r.get("total").and_then(Value::as_i64)?;
                Some((d, t))
            })
            .collect();
        m.get("RATES").copied() == Some(150) && m.get("FX").copied() == Some(30)
    });
}

#[test]
fn view_removes_group_when_source_delete_empties_it() {
    let src = make_source();
    publish(&src, "alice", "RATES", 100);
    publish(&src, "bob", "FX", 50);
    let (_view, vtopic) = build_view(
        src.clone(),
        "SELECT desk, COUNT(*) AS n FROM t GROUP BY desk",
        "/trades_by_desk_3",
    );

    // Delete the only FX row → FX group should vanish from the view.
    src.delete("bob").expect("delete");

    wait_for(|| {
        let result = vtopic.query("SELECT desk, n FROM t").unwrap();
        let desks: std::collections::HashSet<String> = result
            .rows
            .iter()
            .filter_map(|r| r.get("desk").and_then(Value::as_str).map(String::from))
            .collect();
        desks.contains("RATES") && !desks.contains("FX")
    });
}

#[test]
fn view_contents_match_from_scratch_recompute() {
    // Drive the source through a mixed publish + delete stream, then
    // verify the view's SOW exactly equals a re-execution of the same
    // aggregate query against the source AFTER all events are
    // applied. The recompute is the ground truth; the view's job is
    // to converge to it eventually.
    let src = make_source();
    let (_view, vtopic) = build_view(
        src.clone(),
        "SELECT desk, SUM(qty) AS total, COUNT(*) AS n FROM t GROUP BY desk",
        "/trades_by_desk_4",
    );

    // Drive a mixed sequence.
    publish(&src, "alice", "RATES", 100);
    publish(&src, "bob", "RATES", 200);
    publish(&src, "carol", "FX", 50);
    publish(&src, "alice", "EQUITIES", 75); // alice moves desks → RATES drops to 200
    src.delete("bob").expect("delete"); // RATES → 0 rows, group gone
    publish(&src, "dave", "FX", 25); // FX → 75

    // Wait for the view to converge.
    wait_for(|| {
        let view_rows = vtopic.query("SELECT desk FROM t").unwrap();
        let view_desks: std::collections::HashSet<String> = view_rows
            .rows
            .iter()
            .filter_map(|r| r.get("desk").and_then(Value::as_str).map(String::from))
            .collect();
        // Expected: FX, EQUITIES (RATES has no rows after the delete).
        view_desks == ["FX", "EQUITIES"].iter().map(|s| s.to_string()).collect()
    });

    // Final structural assertion: view row counts equal a from-scratch
    // aggregate of the source.
    let source_agg = src
        .query("SELECT desk, SUM(qty) AS total, COUNT(*) AS n FROM t GROUP BY desk")
        .expect("source aggregate");
    let view_rows = vtopic
        .query("SELECT desk, total, n FROM t")
        .expect("view query");
    let canonicalize = |rows: Vec<Map<String, Value>>| -> std::collections::BTreeMap<String, (i64, i64)> {
        rows.into_iter()
            .filter_map(|r| {
                let d = r.get("desk").and_then(Value::as_str)?.to_string();
                let t = r.get("total").and_then(Value::as_i64)?;
                let n = r.get("n").and_then(Value::as_i64)?;
                Some((d, (t, n)))
            })
            .collect()
    };
    assert_eq!(canonicalize(source_agg.rows), canonicalize(view_rows.rows));
}

//! S20 JOIN views — end-to-end view convergence with a two-topic join.
//!
//! Each test:
//!   1. Stands up two source topics (positions × securities), a
//!      Topic registry that resolves the right-side topic, and a
//!      joined view (`positions JOIN securities USING (cusip)`)
//!      with a GROUP BY over a right-side column.
//!   2. Publishes / mutates against EITHER side.
//!   3. Asserts the view's SOW converges to the from-scratch join
//!      aggregate.

use std::sync::Arc;

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_core::view::{spawn_view_runner_joined, View};
use serde_json::{json, Map, Value};

fn make_positions() -> SharedTopic {
    let schema = Arc::new(Schema::from_strs(
        &["positionKey", "cusip", "qty", "marketValue"],
        &[
            ColumnType::String,
            ColumnType::String,
            ColumnType::Long,
            ColumnType::Double,
        ],
    ));
    Arc::new(Topic::new(
        TopicConfig {
            name: "/positions".into(),
            key_fields: vec!["positionKey".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    ))
}

fn make_securities() -> SharedTopic {
    let schema = Arc::new(Schema::from_strs(
        &["cusip", "sector"],
        &[ColumnType::String, ColumnType::String],
    ));
    Arc::new(Topic::new(
        TopicConfig {
            name: "/securities".into(),
            key_fields: vec!["cusip".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        64,
    ))
}

fn push_position(t: &Topic, key: &str, cusip: &str, qty: i64, mv: f64) {
    let mut m = Map::new();
    m.insert("positionKey".into(), json!(key));
    m.insert("cusip".into(), json!(cusip));
    m.insert("qty".into(), json!(qty));
    m.insert("marketValue".into(), json!(mv));
    t.upsert_map(&m).expect("publish position");
}

fn push_security(t: &Topic, cusip: &str, sector: &str) {
    let mut m = Map::new();
    m.insert("cusip".into(), json!(cusip));
    m.insert("sector".into(), json!(sector));
    t.upsert_map(&m).expect("publish security");
}

/// Stand up a joined view via the public `build_view_topic_joined`
/// path. Returns the view-side `SharedTopic` for SOW assertions.
fn build_joined_view(
    positions: SharedTopic,
    securities: SharedTopic,
    sql: &str,
    view_name: &str,
) -> SharedTopic {
    let sec_for_resolver = securities.clone();
    let (view_topic, query, group_by_names, right_topic) = View::build_view_topic_joined(
        &positions,
        move |name| {
            if name == "/securities" || name == "securities" {
                Some(sec_for_resolver.clone())
            } else {
                None
            }
        },
        sql,
        view_name.into(),
        64,
    )
    .expect("build joined view");
    let view_topic_arc = Arc::new(view_topic);
    let left_tap = positions.register_view_tap(1024);
    let right_tap = right_topic.register_view_tap(1024);
    let view = View::new(
        positions,
        view_topic_arc.clone(),
        query,
        group_by_names,
        Some(right_topic),
    )
    .expect("new joined view");
    let _ = spawn_view_runner_joined(view, left_tap, right_tap);
    view_topic_arc
}

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

#[test]
fn joined_view_groups_by_right_side_column_on_initial_populate() {
    let positions = make_positions();
    let securities = make_securities();
    // Seed securities first so the join has matches at view-create time.
    push_security(&securities, "AAPL", "Tech");
    push_security(&securities, "MSFT", "Tech");
    push_security(&securities, "JPM", "Banks");
    push_security(&securities, "BAC", "Banks");
    push_position(&positions, "p1", "AAPL", 100, 15_000.0);
    push_position(&positions, "p2", "MSFT", 50, 18_000.0);
    push_position(&positions, "p3", "JPM", 200, 22_000.0);
    push_position(&positions, "p4", "BAC", 75, 5_500.0);

    let view = build_joined_view(
        positions.clone(),
        securities.clone(),
        "SELECT sector, SUM(marketValue) AS exposure FROM positions \
         JOIN securities USING (cusip) GROUP BY sector",
        "/exposure-by-sector",
    );

    // Initial populate happens synchronously inside `View::new`.
    let rows = view
        .query("SELECT sector, exposure FROM t")
        .expect("query view")
        .rows;
    let mut got: std::collections::BTreeMap<String, f64> = Default::default();
    for r in &rows {
        let sec = r.get("sector").and_then(Value::as_str).unwrap().to_string();
        let exp = r.get("exposure").and_then(Value::as_f64).unwrap();
        got.insert(sec, exp);
    }
    assert_eq!(got.get("Tech").copied(), Some(33_000.0));
    assert_eq!(got.get("Banks").copied(), Some(27_500.0));
}

#[test]
fn joined_view_refreshes_on_left_side_publish() {
    let positions = make_positions();
    let securities = make_securities();
    push_security(&securities, "AAPL", "Tech");
    push_position(&positions, "p1", "AAPL", 100, 1_000.0);
    let view = build_joined_view(
        positions.clone(),
        securities.clone(),
        "SELECT sector, SUM(marketValue) AS exposure FROM positions \
         JOIN securities USING (cusip) GROUP BY sector",
        "/exposure-left-publish",
    );

    // Publish a new AAPL position — Tech exposure should climb.
    push_position(&positions, "p2", "AAPL", 50, 4_000.0);
    wait_for(|| {
        let rows = view.query("SELECT sector, exposure FROM t").unwrap().rows;
        rows.iter().any(|r| {
            r.get("sector").and_then(Value::as_str) == Some("Tech")
                && r.get("exposure").and_then(Value::as_f64) == Some(5_000.0)
        })
    });
}

#[test]
fn joined_view_refreshes_on_right_side_publish() {
    let positions = make_positions();
    let securities = make_securities();
    // No matching security yet → join returns nothing.
    push_position(&positions, "p1", "JPM", 200, 22_000.0);
    push_position(&positions, "p2", "BAC", 75, 5_500.0);
    let view = build_joined_view(
        positions.clone(),
        securities.clone(),
        "SELECT sector, SUM(marketValue) AS exposure FROM positions \
         JOIN securities USING (cusip) GROUP BY sector",
        "/exposure-right-publish",
    );
    // At populate-time the view is empty (no securities).
    assert_eq!(
        view.query("SELECT sector FROM t").unwrap().rows.len(),
        0,
        "view should be empty before any securities arrive"
    );

    // Now arrival of /securities should trigger a view refresh and
    // populate Banks exposure end-to-end.
    push_security(&securities, "JPM", "Banks");
    push_security(&securities, "BAC", "Banks");
    wait_for(|| {
        let rows = view.query("SELECT sector, exposure FROM t").unwrap().rows;
        rows.iter().any(|r| {
            r.get("sector").and_then(Value::as_str) == Some("Banks")
                && r.get("exposure").and_then(Value::as_f64) == Some(27_500.0)
        })
    });
}

#[test]
fn joined_view_drops_groups_when_right_side_row_is_removed() {
    let positions = make_positions();
    let securities = make_securities();
    push_security(&securities, "AAPL", "Tech");
    push_position(&positions, "p1", "AAPL", 100, 1_000.0);
    let view = build_joined_view(
        positions.clone(),
        securities.clone(),
        "SELECT sector, SUM(marketValue) AS exposure FROM positions \
         JOIN securities USING (cusip) GROUP BY sector",
        "/exposure-right-delete",
    );

    // Pull the security row — Tech exposure must vanish from the view.
    securities.delete("AAPL").expect("delete security");
    wait_for(|| view.query("SELECT sector FROM t").unwrap().rows.is_empty());
}

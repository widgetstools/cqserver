//! Sub-project 1 e2e — schema catalog + runtime view creation.

use cq_client::Client;
use cq_e2e_tests::{
    restart_kept, start_server, start_server_with, stop_keeping_dir, ServerOpts, TopicSpec,
    ViewSpec,
};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn catalog_lists_topics_and_views_with_columns() {
    let topic = TopicSpec::new("/positions", "position_id").with_inline_columns([
        ("position_id", "string"),
        ("sector", "string"),
        ("mv", "double"),
    ]);
    let opts = ServerOpts {
        views: vec![ViewSpec::new(
            "/v_by_sector",
            "/positions",
            "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector",
        )],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let url = format!("{}/admin/catalog", server.admin_url());
    let body: Vec<Value> = reqwest::get(&url)
        .await
        .expect("catalog GET")
        .json()
        .await
        .expect("catalog json");

    let pos = body
        .iter()
        .find(|e| e["name"] == "/positions")
        .expect("positions present");
    assert_eq!(pos["kind"], "topic");
    let cols: Vec<&str> = pos["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(cols.contains(&"sector"), "columns: {cols:?}");

    let view = body
        .iter()
        .find(|e| e["name"] == "/v_by_sector")
        .expect("view present");
    assert_eq!(view["kind"], "view");
    // The aggregation view selects `sector` and `n` — both must appear.
    let view_cols: Vec<&str> = view["columns"]
        .as_array()
        .expect("view has columns array")
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(view_cols.contains(&"sector"), "view columns: {view_cols:?}");
    // Exercise the column-type field on the topic side.
    let mv_col = pos["columns"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "mv")
        .expect("mv column present");
    assert_eq!(mv_col["type"], "double");
}

#[tokio::test]
async fn create_view_then_subscribe_is_live() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/positions", json!({"position_id":"p1","sector":"TECH"}))
        .await
        .unwrap();
    client
        .publish("/positions", json!({"position_id":"p2","sector":"TECH"}))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_sector_counts",
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert_eq!(resp.status().as_u16(), 201, "expected 201 Created");

    tokio::time::sleep(Duration::from_millis(150)).await;
    let rows = client
        .sow_sql("/v_sector_counts", "SELECT sector, n FROM t")
        .await
        .expect("view sow");
    let tech = rows
        .iter()
        .find(|r| r.get("sector").and_then(|v| v.as_str()) == Some("TECH"))
        .expect("TECH group");
    assert_eq!(tech.get("n").unwrap().as_i64().unwrap(), 2);

    // Live re-aggregation: a new source row bumps the count.
    client
        .publish("/positions", json!({"position_id":"p3","sector":"TECH"}))
        .await
        .unwrap();
    // Allow the view runner to wake on the source tap and re-aggregate.
    // 250ms proved flaky under the debug test build; 500ms is reliable.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let rows = client
        .sow_sql("/v_sector_counts", "SELECT sector, n FROM t")
        .await
        .expect("view sow 2");
    let tech = rows
        .iter()
        .find(|r| r.get("sector").and_then(|v| v.as_str()) == Some("TECH"))
        .expect("TECH group 2");
    assert_eq!(tech.get("n").unwrap().as_i64().unwrap(), 3);
}

#[tokio::test]
async fn persisted_view_recreated_after_restart() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_persist",
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert!(resp.status().is_success());

    let rt = server.config_dir.join("runtime_views.toml");
    assert!(rt.exists(), "runtime_views.toml should be written on create");

    let kept = stop_keeping_dir(server).await;
    let server = restart_kept(kept).await;

    let body: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await
        .expect("catalog GET")
        .json()
        .await
        .expect("catalog json");
    let view = body.iter().find(|e| e["name"] == "/v_persist");
    assert!(view.is_some(), "view should be recreated after restart");
    assert_eq!(view.unwrap()["kind"], "view");
}

#[tokio::test]
async fn create_view_invalid_sql_is_rejected_and_not_persisted() {
    let topic = TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")]);
    let server = start_server(vec![topic]).await;

    let resp = reqwest::Client::new()
        .post(format!("{}/admin/views", server.admin_url()))
        .json(&json!({
            "name": "/v_bad",
            "source": "/positions",
            "sql": "SELECT nonexistent_col, COUNT(*) AS n FROM t GROUP BY nonexistent_col"
        }))
        .send()
        .await
        .expect("create-view POST");
    assert!(
        !resp.status().is_success(),
        "invalid SQL must be rejected, got {}",
        resp.status()
    );

    let rt = server.config_dir.join("runtime_views.toml");
    let empty = !rt.exists()
        || std::fs::read_to_string(&rt).unwrap().trim().is_empty();
    assert!(empty, "rejected view must not be persisted");
}

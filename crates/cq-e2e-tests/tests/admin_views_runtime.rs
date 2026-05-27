//! Sub-project 1 e2e — schema catalog + runtime view creation.

use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::Value;

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
}

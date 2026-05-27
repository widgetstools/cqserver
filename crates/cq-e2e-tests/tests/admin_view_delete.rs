//! Soft-delete e2e — DELETE /admin/views/{name} stops + de-registers
//! + de-persists a runtime-created view; it does not return on restart.

use cq_e2e_tests::{restart_kept, start_server, stop_keeping_dir, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

fn topic() -> TopicSpec {
    TopicSpec::new("/positions", "position_id")
        .with_inline_columns([("position_id", "string"), ("sector", "string")])
}

async fn create(admin_url: &str, name: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{admin_url}/admin/views"))
        .json(&json!({
            "name": name,
            "source": "/positions",
            "sql": "SELECT sector, COUNT(*) AS n FROM t GROUP BY sector"
        }))
        .send().await.expect("create");
    assert_eq!(resp.status().as_u16(), 201, "create should 201");
}

#[tokio::test]
async fn delete_removes_view_from_catalog_and_persistence() {
    let server = start_server(vec![topic()]).await;
    create(&server.admin_url(), "/v_del").await;
    tokio::time::sleep(Duration::from_millis(120)).await;

    let before: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(before.iter().any(|e| e["name"] == "/v_del"), "present pre-delete");

    let del = reqwest::Client::new()
        .delete(format!("{}/admin/views/{}", server.admin_url(), "%2Fv_del"))
        .send().await.expect("delete");
    assert_eq!(del.status().as_u16(), 200, "delete should 200");

    tokio::time::sleep(Duration::from_millis(80)).await;
    let after: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(!after.iter().any(|e| e["name"] == "/v_del"), "gone post-delete");
    let rt = server.config_dir.join("runtime_views.toml");
    let persisted = if rt.exists() { std::fs::read_to_string(&rt).unwrap() } else { String::new() };
    assert!(!persisted.contains("/v_del"), "de-persisted");

    let kept = stop_keeping_dir(server).await;
    let server = restart_kept(kept).await;
    let post: Vec<Value> = reqwest::get(format!("{}/admin/catalog", server.admin_url()))
        .await.unwrap().json().await.unwrap();
    assert!(!post.iter().any(|e| e["name"] == "/v_del"), "not resurrected on restart");
}

#[tokio::test]
async fn delete_unknown_view_returns_404() {
    let server = start_server(vec![topic()]).await;
    let del = reqwest::Client::new()
        .delete(format!("{}/admin/views/{}", server.admin_url(), "%2Fv_nope"))
        .send().await.expect("delete");
    assert_eq!(del.status().as_u16(), 404, "missing view should 404");
}

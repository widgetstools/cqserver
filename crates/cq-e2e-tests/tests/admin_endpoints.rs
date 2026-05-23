//! e2e: `POST /admin/rotate-journal/{topic}` and `GET /admin/replication`.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn rotate_journal_seals_active_segment() {
    let topic = TopicSpec::new("/rot-topic", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..5 {
        client
            .publish("/rot-topic", json!({ "k": format!("k{i}"), "v": i as f64 }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Force a rotation.
    let url = format!(
        "{}/admin/rotate-journal/{}",
        server.admin_url(),
        // URL-encode the leading `/` so axum's Path extractor gets it.
        "%2Frot-topic"
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .send()
        .await
        .expect("rotate post");
    assert_eq!(resp.status(), 200, "rotate-journal returned {:?}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body.get("rotated").unwrap(), true);
}

#[tokio::test]
async fn rotate_journal_404_for_unknown_topic() {
    let server = start_server(vec![]).await;
    let url = format!(
        "{}/admin/rotate-journal/{}",
        server.admin_url(),
        "%2Fnosuch"
    );
    let resp = reqwest::Client::new().post(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn replication_endpoint_lists_persistent_topics() {
    let topic_persist = TopicSpec::new("/rep-persist", "k")
        .with_inline_columns([("k", "string")])
        .with_persist();
    let topic_transient = TopicSpec::new("/rep-transient", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic_persist, topic_transient]).await;

    let url = format!("{}/admin/replication", server.admin_url());
    let resp = reqwest::get(&url).await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let topics = body.get("topics").unwrap().as_array().unwrap();
    let names: Vec<&str> = topics
        .iter()
        .filter_map(|t| t.get("topic").and_then(|v| v.as_str()))
        .collect();
    assert!(
        names.contains(&"/rep-persist"),
        "persistent topic missing from replication endpoint: {names:?}"
    );
    assert!(
        !names.contains(&"/rep-transient"),
        "non-persistent topic should not appear: {names:?}"
    );
}

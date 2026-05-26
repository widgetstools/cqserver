//! Q6 e2e — `/admin/clients` aggregates per-session stats.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::Value;
use std::time::Duration;

#[tokio::test]
async fn admin_clients_lists_active_sessions() {
    let topic = TopicSpec::new("/q6", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    // Two clients, both with logon names.
    let a = Client::connect(&server.tcp_url()).await.expect("connect a");
    let b = Client::connect(&server.tcp_url()).await.expect("connect b");
    a.logon_with("", "", Some("publisher-a".into()), None).await.ok();
    b.logon_with("", "", Some("subscriber-b".into()), None).await.ok();

    // b subscribes (creates a delivery route → an entry in
    // /admin/clients). a doesn't subscribe but its session still
    // exists; admin/clients only surfaces sessions with at least
    // one subscription (route-aggregated).
    let _sub = b.subscribe("/q6", None).await.expect("subscribe");

    // Push some data so we have non-zero last_seq.
    a.publish("/q6", serde_json::json!({ "k": "x", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let admin_url = format!("http://127.0.0.1:{}/admin/clients", server.admin_port);
    let body: Value = reqwest::get(&admin_url)
        .await
        .expect("admin/clients")
        .json()
        .await
        .expect("decode");

    let arr = body.as_array().expect("array");
    assert!(!arr.is_empty(), "expected at least one client entry");
    // At least one entry should carry our client_name.
    let has_subscriber = arr.iter().any(|c| {
        c.get("clientName").and_then(|v| v.as_str()) == Some("subscriber-b")
    });
    assert!(
        has_subscriber,
        "expected to find clientName=subscriber-b in {arr:#?}"
    );
    // Subscription count is >= 1 for the subscriber.
    for c in arr {
        if c.get("clientName").and_then(|v| v.as_str()) == Some("subscriber-b") {
            assert!(c.get("subscriptions").unwrap().as_u64().unwrap() >= 1);
        }
    }
}

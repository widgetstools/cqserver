//! Q11 e2e — online schema evolution via `POST /admin/add-column`.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn add_column_via_admin_endpoint_then_publish_populates_it() {
    let topic = TopicSpec::new("/q11", "k").with_inline_columns([
        ("k", "string"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed a row on the original 2-col schema.
    client
        .publish("/q11", json!({ "k": "AAPL", "price": 150.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Add a `desk` column online. Topic name is URL-encoded (the
    // leading slash becomes %2F).
    let admin_url = format!(
        "http://127.0.0.1:{}/admin/add-column/%2Fq11?name=desk&type=string",
        server.admin_port
    );
    let resp = reqwest::Client::new()
        .post(&admin_url)
        .send()
        .await
        .expect("add-column POST");
    assert!(
        resp.status().is_success(),
        "add-column failed: {:?}",
        resp.status()
    );

    // Publish a new row populating the new column.
    client
        .publish(
            "/q11",
            json!({ "k": "MSFT", "price": 300.0, "desk": "RATES" }),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // SOW with desk filter returns only MSFT.
    let rows = client
        .sow_sql("/q11", "SELECT k FROM t WHERE desk = 'RATES'")
        .await
        .expect("filtered sow");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").unwrap().as_str().unwrap(), "MSFT");

    // SOW without filter returns both rows (AAPL with desk omitted/null).
    let rows = client
        .sow_sql("/q11", "SELECT k, price, desk FROM t")
        .await
        .expect("all sow");
    assert_eq!(rows.len(), 2);
}

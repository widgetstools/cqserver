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

// ───── Diversification ────────────────────────────────────────────

/// Add a numeric (long) column online — pre-existing rows show null,
/// new publishes populate it.
#[tokio::test]
async fn add_long_column_pre_existing_rows_are_null_new_rows_populate() {
    let topic = TopicSpec::new("/q11_long", "k")
        .with_inline_columns([("k", "string"), ("name", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, name) in [("a", "Alice"), ("b", "Bob")] {
        client
            .publish("/q11_long", json!({ "k": k, "name": name }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let resp = reqwest::Client::new()
        .post(format!(
            "http://127.0.0.1:{}/admin/add-column/%2Fq11_long?name=score&type=long",
            server.admin_port
        ))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    client
        .publish("/q11_long", json!({ "k": "c", "name": "Carol", "score": 99 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Existing rows have no score; new row has score=99.
    let nulls = client
        .sow_sql("/q11_long", "SELECT k FROM t WHERE score IS NULL")
        .await
        .unwrap();
    let null_keys: std::collections::HashSet<String> = nulls
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(null_keys.contains("a") && null_keys.contains("b"));
    assert!(!null_keys.contains("c"));

    let with_score = client
        .sow_sql("/q11_long", "SELECT k FROM t WHERE score = 99")
        .await
        .unwrap();
    assert_eq!(with_score.len(), 1);
    assert_eq!(with_score[0].get("k").unwrap().as_str().unwrap(), "c");
}

/// Adding a duplicate column name fails with a clean error.
#[tokio::test]
async fn add_duplicate_column_name_rejected() {
    let topic = TopicSpec::new("/q11_dup", "k")
        .with_inline_columns([("k", "string"), ("name", "string")]);
    let server = start_server(vec![topic]).await;

    let url = format!(
        "http://127.0.0.1:{}/admin/add-column/%2Fq11_dup?name=name&type=string",
        server.admin_port
    );
    let resp = reqwest::Client::new().post(&url).send().await.unwrap();
    assert!(
        !resp.status().is_success(),
        "duplicate column should be rejected, got {:?}",
        resp.status()
    );
}

/// Add column to a non-existent topic — 404 or 400, not crash.
#[tokio::test]
async fn add_column_unknown_topic_errors() {
    let topic = TopicSpec::new("/q11_real", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let url = format!(
        "http://127.0.0.1:{}/admin/add-column/%2Fdoes_not_exist?name=foo&type=long",
        server.admin_port
    );
    let resp = reqwest::Client::new().post(&url).send().await.unwrap();
    assert!(
        !resp.status().is_success(),
        "unknown topic should error, got {:?}",
        resp.status()
    );
    // Real topic still usable.
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    client
        .publish("/q11_real", json!({ "k": "a" }))
        .await
        .unwrap();
}

/// Add column → publish 100 rows that populate it → SOW with predicate.
/// Verifies the column-store grow path handles many writes after
/// schema swap.
#[tokio::test]
async fn add_column_then_bulk_publish_populates_correctly() {
    let topic = TopicSpec::new("/q11_bulk", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let url = format!(
        "http://127.0.0.1:{}/admin/add-column/%2Fq11_bulk?name=v&type=long",
        server.admin_port
    );
    let resp = reqwest::Client::new().post(&url).send().await.unwrap();
    assert!(resp.status().is_success());

    for i in 0..100_i64 {
        client
            .publish("/q11_bulk", json!({ "k": format!("k{i:03}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let row = client
        .sow_sql("/q11_bulk", "SELECT SUM(v) AS s FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(row.get("s").unwrap().as_i64().unwrap(), (0..100).sum::<i64>());
}

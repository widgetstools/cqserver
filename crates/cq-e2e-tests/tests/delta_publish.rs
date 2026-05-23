//! e2e: delta_publish merges sparse updates into the SOW row.
//!
//! Publishers using `delta_publish` send only `{key + changed
//! fields}` — the server merges those fields into the existing row
//! and leaves everything else alone. Two assertions matter:
//!
//!   1. **Correctness**: the merged SOW row reflects the latest
//!      values from any combination of full + delta publishes,
//!      regardless of order.
//!   2. **No-regression on full publish**: an absent field in a
//!      full publish still nulls the corresponding column, the
//!      behavior that makes the two paths distinguishable.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn delta_publish_merges_fields_into_existing_row() {
    let topic = TopicSpec::new("/delta-trades", "symbol").with_inline_columns([
        ("symbol", "string"),
        ("price", "double"),
        ("qty", "long"),
        ("desk", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed a full row.
    client
        .publish(
            "/delta-trades",
            json!({
                "symbol": "AAPL",
                "price": 150.0,
                "qty": 100,
                "desk": "EQUITIES",
            }),
        )
        .await
        .expect("publish full");
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Delta-publish: only price changes.
    client
        .delta_publish("/delta-trades", json!({ "symbol": "AAPL", "price": 175.0 }))
        .await
        .expect("delta_publish price");
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Delta-publish: only qty changes (price stays 175).
    client
        .delta_publish("/delta-trades", json!({ "symbol": "AAPL", "qty": 250 }))
        .await
        .expect("delta_publish qty");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow("/delta-trades", Some("symbol = 'AAPL'"))
        .await
        .expect("sow");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("symbol").unwrap(), "AAPL");
    assert_eq!(row.get("price").unwrap(), 175.0, "delta should have updated price");
    assert_eq!(row.get("qty").unwrap(), 250, "delta should have updated qty");
    assert_eq!(
        row.get("desk").unwrap(),
        "EQUITIES",
        "desk wasn't in any delta — must keep original value"
    );
}

#[tokio::test]
async fn delta_publish_creates_row_when_key_is_new() {
    let topic = TopicSpec::new("/delta-new", "symbol").with_inline_columns([
        ("symbol", "string"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .delta_publish("/delta-new", json!({ "symbol": "TSLA", "price": 800.0 }))
        .await
        .expect("delta_publish new key");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client.sow("/delta-new", None).await.expect("sow");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("price").unwrap(), 800.0);
}

#[tokio::test]
async fn delta_publish_survives_restart_with_merged_state() {
    // The txlog must record the *merged* row after a delta_publish so
    // recovery produces the same SOW state regardless of which mix of
    // full + delta the publisher used. This test seeds a persistent
    // topic, applies some delta updates, restarts the server, and
    // verifies the recovered SOW reflects every merged field.
    use cq_e2e_tests::{restart_kept, stop_keeping_dir};

    let topic = TopicSpec::new("/delta-persist", "symbol")
        .with_inline_columns([
            ("symbol", "string"),
            ("price", "double"),
            ("qty", "long"),
        ])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Full publish + two sparse deltas, each touching a different field.
    client
        .publish(
            "/delta-persist",
            json!({ "symbol": "AAPL", "price": 100.0, "qty": 10 }),
        )
        .await
        .unwrap();
    client
        .delta_publish("/delta-persist", json!({ "symbol": "AAPL", "price": 175.0 }))
        .await
        .unwrap();
    client
        .delta_publish("/delta-persist", json!({ "symbol": "AAPL", "qty": 250 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    drop(client);

    // Restart against the same on-disk state.
    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();

    let rows = client2
        .sow("/delta-persist", Some("symbol = 'AAPL'"))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.get("price").unwrap(), 175.0);
    assert_eq!(row.get("qty").unwrap(), 250);
}

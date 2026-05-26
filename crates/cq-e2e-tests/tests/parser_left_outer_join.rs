//! P12 e2e — LEFT OUTER JOIN keeps unmatched left rows with right
//! columns as JSON null.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn left_outer_join_keeps_unmatched_left() {
    let positions = TopicSpec::new("/pos_lo", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/sec_lo", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let server = start_server(vec![positions, securities]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Securities has AAPL only; positions has AAPL + MSFT.
    client
        .publish("/sec_lo", json!({ "cusip": "AAPL", "sector": "Tech" }))
        .await
        .unwrap();
    for (k, c, mv) in [("p1", "AAPL", 10_000.0_f64), ("p2", "MSFT", 20_000.0)] {
        client
            .publish(
                "/pos_lo",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // INNER JOIN drops MSFT.
    let inner = client
        .sow_sql(
            "/pos_lo",
            "SELECT positionKey, cusip, sector FROM pos_lo JOIN sec_lo USING (cusip)",
        )
        .await
        .expect("inner sow");
    assert_eq!(inner.len(), 1);
    assert_eq!(inner[0].get("positionKey").unwrap().as_str().unwrap(), "p1");

    // LEFT OUTER JOIN keeps MSFT with sector = null.
    let outer = client
        .sow_sql(
            "/pos_lo",
            "SELECT positionKey, cusip, sector FROM pos_lo LEFT JOIN sec_lo USING (cusip)",
        )
        .await
        .expect("left-outer sow");
    assert_eq!(outer.len(), 2, "LEFT OUTER must keep both rows, got {outer:?}");
    let by_key: std::collections::HashMap<String, Value> = outer
        .into_iter()
        .map(|r| {
            (
                r.get("positionKey").unwrap().as_str().unwrap().to_string(),
                r.get("sector").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    assert_eq!(by_key["p1"].as_str().unwrap(), "Tech");
    assert!(
        by_key["p2"].is_null(),
        "unmatched MSFT position must have null sector, got {:?}",
        by_key["p2"]
    );
}

//! Q1 e2e — RIGHT OUTER + FULL OUTER JOIN.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn right_and_full_outer_join_keep_unmatched_sides() {
    let positions = TopicSpec::new("/pos_q1", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/sec_q1", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let server = start_server(vec![positions, securities]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Positions: AAPL, MSFT. Securities: AAPL, GOOG.
    // INNER → AAPL only. LEFT → AAPL+MSFT. RIGHT → AAPL+GOOG.
    // FULL → AAPL+MSFT+GOOG.
    for (c, s) in [("AAPL", "Tech"), ("GOOG", "Tech")] {
        client
            .publish("/sec_q1", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [("p1", "AAPL", 10_000.0_f64), ("p2", "MSFT", 20_000.0)] {
        client
            .publish(
                "/pos_q1",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let inner = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("inner sow");
    assert_eq!(inner.len(), 1, "INNER must keep only AAPL");

    let right = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 RIGHT JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("right sow");
    let right_by_cusip: std::collections::HashMap<String, Value> = right
        .into_iter()
        .map(|r| {
            (
                r.get("cusip").unwrap().as_str().unwrap().to_string(),
                r.get("marketValue").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    assert_eq!(right_by_cusip.len(), 2, "RIGHT must keep AAPL + GOOG");
    assert!(right_by_cusip.contains_key("AAPL"));
    assert!(right_by_cusip.contains_key("GOOG"));
    assert!(
        right_by_cusip["GOOG"].is_null(),
        "GOOG marketValue must be null (right-only)"
    );

    let full = client
        .sow_sql(
            "/pos_q1",
            "SELECT cusip, marketValue, sector FROM pos_q1 \
             FULL OUTER JOIN sec_q1 USING (cusip)",
        )
        .await
        .expect("full sow");
    assert_eq!(full.len(), 3, "FULL must keep AAPL + MSFT + GOOG");
    let cusips: std::collections::HashSet<String> = full
        .iter()
        .map(|r| r.get("cusip").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(cusips.contains("AAPL"));
    assert!(cusips.contains("MSFT"));
    assert!(cusips.contains("GOOG"));
}

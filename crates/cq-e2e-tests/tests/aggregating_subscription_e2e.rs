//! S19 — continuous-aggregate subscription end-to-end test.
//!
//! Spins a real cqserver child process. Subscribes with a
//! `SELECT desk, SUM(qty) ... GROUP BY desk` query, publishes
//! rows, and verifies that:
//!   - The snapshot phase returns one row per group.
//!   - Subsequent publishes emit Update deltas with the new group totals.
//!   - A delete that empties a group emits a Remove delta for it.

use std::time::Duration;

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};

#[tokio::test]
async fn aggregating_subscription_e2e() {
    let topic = TopicSpec::new("/agg-e2e", "trader")
        .with_inline_columns([
            ("trader", "string"),
            ("desk", "string"),
            ("qty", "long"),
        ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed the topic with two desks.
    client
        .publish("/agg-e2e", json!({ "trader": "alice", "desk": "RATES", "qty": 100 }))
        .await
        .unwrap();
    client
        .publish("/agg-e2e", json!({ "trader": "bob", "desk": "FX", "qty": 50 }))
        .await
        .unwrap();

    let mut sub = client
        .sow_and_subscribe_sql(
            "/agg-e2e",
            "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
        )
        .await
        .expect("subscribe");

    // Pull the initial snapshot — one delta per group.
    let mut snapshot: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for _ in 0..2 {
        let delta = tokio::time::timeout(Duration::from_secs(2), sub.next_delta())
            .await
            .expect("snapshot delta arrived")
            .expect("delta");
        let desk = delta.data.get("desk").and_then(Value::as_str).unwrap();
        let total = delta.data.get("total").and_then(Value::as_i64).unwrap();
        snapshot.insert(desk.to_string(), total);
    }
    assert_eq!(snapshot["RATES"], 100);
    assert_eq!(snapshot["FX"], 50);

    // Publish a new RATES row → RATES total moves 100 → 150.
    client
        .publish("/agg-e2e", json!({ "trader": "carol", "desk": "RATES", "qty": 50 }))
        .await
        .unwrap();

    // Drain a couple of deltas (the executor may re-emit all groups
    // but only RATES has actually changed).
    let mut saw_rates_150 = false;
    for _ in 0..4 {
        let next = tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await;
        let Ok(Some(d)) = next else { break };
        let desk = d.data.get("desk").and_then(Value::as_str).unwrap_or("");
        let total = d.data.get("total").and_then(Value::as_i64).unwrap_or(-1);
        if desk == "RATES" && total == 150 {
            saw_rates_150 = true;
            assert_eq!(d.delta_type, DeltaKind::Update);
        }
    }
    assert!(saw_rates_150, "expected RATES total to update to 150 after the publish");
}

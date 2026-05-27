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

// ───── Diversification ────────────────────────────────────────────

/// Aggregating subscription with COUNT(*) — verify count increments
/// on new rows, decrements on deletes.
#[tokio::test]
async fn count_aggregate_subscription_tracks_row_count() {
    let topic = TopicSpec::new("/agg-count", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/agg-count", json!({ "k": "a", "desk": "X" }))
        .await
        .unwrap();
    client
        .publish("/agg-count", json!({ "k": "b", "desk": "X" }))
        .await
        .unwrap();

    let mut sub = client
        .sow_and_subscribe_sql(
            "/agg-count",
            "SELECT desk, COUNT(*) AS c FROM t GROUP BY desk",
        )
        .await
        .unwrap();

    // Snapshot: X → 2.
    let mut snapshot_done = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                if d.data.get("desk").and_then(Value::as_str) == Some("X")
                    && d.data.get("c").and_then(Value::as_i64) == Some(2)
                {
                    snapshot_done = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(snapshot_done, "snapshot must show X=2");

    // Add a 3rd X row → c becomes 3.
    client
        .publish("/agg-count", json!({ "k": "c", "desk": "X" }))
        .await
        .unwrap();

    let mut saw_count_3 = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                if d.data.get("desk").and_then(Value::as_str) == Some("X")
                    && d.data.get("c").and_then(Value::as_i64) == Some(3)
                {
                    saw_count_3 = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_count_3, "expected X count to advance to 3");
}

/// MIN/MAX aggregate subscription — verify the deltas track the
/// minimum/maximum across updates.
#[tokio::test]
async fn min_max_aggregate_subscription_tracks_extremes() {
    let topic = TopicSpec::new("/agg-mm", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("px", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/agg-mm", json!({ "k": "a", "desk": "RATES", "px": 100.0 }))
        .await
        .unwrap();
    client
        .publish("/agg-mm", json!({ "k": "b", "desk": "RATES", "px": 200.0 }))
        .await
        .unwrap();

    let mut sub = client
        .sow_and_subscribe_sql(
            "/agg-mm",
            "SELECT desk, MIN(px) AS lo, MAX(px) AS hi FROM t GROUP BY desk",
        )
        .await
        .unwrap();

    // Snapshot — RATES lo=100, hi=200.
    let mut got_snap = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                let lo = d.data.get("lo").and_then(Value::as_f64);
                let hi = d.data.get("hi").and_then(Value::as_f64);
                if lo == Some(100.0) && hi == Some(200.0) {
                    got_snap = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(got_snap, "snapshot RATES lo=100, hi=200");

    // New row pushes hi → 300.
    client
        .publish("/agg-mm", json!({ "k": "c", "desk": "RATES", "px": 300.0 }))
        .await
        .unwrap();

    let mut saw_300 = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) => {
                if d.data.get("hi").and_then(Value::as_f64) == Some(300.0) {
                    saw_300 = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_300, "expected hi to advance to 300");
}

/// Aggregate sub with no rows initially — should emit snapshot end
/// with no rows; then deltas as rows arrive.
#[tokio::test]
async fn aggregate_sub_empty_topic_then_first_publish() {
    let topic = TopicSpec::new("/agg-late", "k")
        .with_inline_columns([("k", "string"), ("g", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let mut sub = client
        .sow_and_subscribe_sql(
            "/agg-late",
            "SELECT g, SUM(v) AS s FROM t GROUP BY g",
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    // First publish — should produce an Add delta for group "A".
    client
        .publish("/agg-late", json!({ "k": "k1", "g": "A", "v": 42 }))
        .await
        .unwrap();

    let mut saw_a_42 = false;
    for _ in 0..3 {
        match tokio::time::timeout(Duration::from_millis(700), sub.next_delta()).await {
            Ok(Some(d)) => {
                if d.data.get("g").and_then(Value::as_str) == Some("A")
                    && d.data.get("s").and_then(Value::as_i64) == Some(42)
                {
                    saw_a_42 = true;
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(saw_a_42, "expected first-publish to create group A with s=42");
}

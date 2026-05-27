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

// ───── Diversification ────────────────────────────────────────────

/// Empty right side → LEFT OUTER returns every left row with NULL for
/// right columns.
#[tokio::test]
async fn left_outer_with_empty_right_keeps_all_left_rows() {
    let l = TopicSpec::new("/p12_lall", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p12_rempty", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..3 {
        client
            .publish(
                "/p12_lall",
                json!({ "k": format!("k{i}"), "c": format!("c{i}") }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/p12_lall",
            "SELECT k, tag FROM p12_lall LEFT JOIN p12_rempty USING (c)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);
    for r in &rows {
        let tag = r.get("tag");
        assert!(tag.is_none() || tag.unwrap().is_null());
    }
}

/// Empty left side → LEFT OUTER returns empty (no left rows to project).
#[tokio::test]
async fn left_outer_with_empty_left_is_empty() {
    let l = TopicSpec::new("/p12_lemp", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p12_rfull", "c")
        .with_inline_columns([("c", "string"), ("tag", "string")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for c in ["A", "B", "C"] {
        client
            .publish("/p12_rfull", json!({ "c": c, "tag": c }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/p12_lemp",
            "SELECT k, tag FROM p12_lemp LEFT JOIN p12_rfull USING (c)",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// LEFT OUTER + WHERE filtering the joined columns — predicate on
/// right column should still drop unmatched rows (NULL fails > 0).
#[tokio::test]
async fn left_outer_with_filter_on_right_drops_unmatched() {
    let l = TopicSpec::new("/p12_lfilt", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p12_rfilt", "c")
        .with_inline_columns([("c", "string"), ("v", "long")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/p12_rfilt", json!({ "c": "A", "v": 100 }))
        .await
        .unwrap();
    client
        .publish("/p12_rfilt", json!({ "c": "B", "v": 50 }))
        .await
        .unwrap();
    for (k, c) in [("k1", "A"), ("k2", "B"), ("k3", "Z")] {
        client
            .publish("/p12_lfilt", json!({ "k": k, "c": c }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/p12_lfilt",
            "SELECT k, v FROM p12_lfilt LEFT JOIN p12_rfilt USING (c) WHERE v > 75",
        )
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("k1"), "k1 has v=100 > 75");
    assert!(!ks.contains("k2"), "k2 has v=50, fails > 75");
    assert!(!ks.contains("k3"), "k3 has NULL v, fails > 75");
}

/// LEFT OUTER with multi-key right (1:N match) — every match
/// surfaces, plus the unmatched left rows.
#[tokio::test]
async fn left_outer_with_right_dupes_uses_last_write_plus_unmatched() {
    let l = TopicSpec::new("/p12_l1n", "k")
        .with_inline_columns([("k", "string"), ("c", "string")]);
    let r = TopicSpec::new("/p12_r1n", "c")
        .with_inline_columns([("c", "string"), ("v", "long")]);
    let server = start_server(vec![l, r]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/p12_r1n", json!({ "c": "C1", "v": 1 }))
        .await
        .unwrap();
    client
        .publish("/p12_r1n", json!({ "c": "C1", "v": 99 }))
        .await
        .unwrap();
    client
        .publish("/p12_l1n", json!({ "k": "k_match", "c": "C1" }))
        .await
        .unwrap();
    client
        .publish("/p12_l1n", json!({ "k": "k_nomatch", "c": "C2" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/p12_l1n",
            "SELECT k, v FROM p12_l1n LEFT JOIN p12_r1n USING (c)",
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    let by_k: std::collections::HashMap<String, serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("v").cloned().unwrap_or(serde_json::Value::Null),
            )
        })
        .collect();
    assert_eq!(by_k["k_match"].as_i64().unwrap(), 99);
    assert!(by_k["k_nomatch"].is_null());
}

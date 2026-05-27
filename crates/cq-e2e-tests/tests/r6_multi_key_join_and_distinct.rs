//! R6 — multi-key JOIN ON + SELECT DISTINCT.
//!
//! Multi-key JOIN: `ON a.x = b.x AND a.y = b.y`. The P11 translator
//! already handles AND-trees of equi-joins via `collect_equi_using`;
//! this test pins the contract end-to-end. AMPS's `JOIN ... USING (x, y)`
//! is the canonical multi-key form and both engines should produce
//! identical rows.
//!
//! DISTINCT: `SELECT DISTINCT col, col FROM t`. AMPS supports
//! DISTINCT for de-duplication (it's distinct from COUNT(DISTINCT)
//! which we already have).

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn multi_key_join_on_matches_using() {
    let pos = TopicSpec::new("/r6_pos", "k").with_inline_columns([
        ("k", "string"),
        ("position_id", "string"),
        ("book_id", "string"),
        ("book_name", "string"),
    ]);
    let trd = TopicSpec::new("/r6_trd", "tk").with_inline_columns([
        ("tk", "string"),
        ("position_id", "string"),
        ("book_id", "string"),
        ("broker", "string"),
    ]);
    let server = start_server(vec![pos, trd]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r6_pos", json!({ "k": "p1", "position_id": "P1", "book_id": "B1", "book_name": "Alpha" }))
        .await
        .unwrap();
    client
        .publish("/r6_pos", json!({ "k": "p2", "position_id": "P2", "book_id": "B2", "book_name": "Beta" }))
        .await
        .unwrap();
    client
        .publish("/r6_trd", json!({ "tk": "t1", "position_id": "P1", "book_id": "B1", "broker": "GS" }))
        .await
        .unwrap();
    client
        .publish("/r6_trd", json!({ "tk": "t2", "position_id": "P2", "book_id": "B2", "broker": "MS" }))
        .await
        .unwrap();
    // Mismatched book — should NOT join even though position_id matches.
    client
        .publish("/r6_trd", json!({ "tk": "t3", "position_id": "P1", "book_id": "BOTHER", "broker": "JPM" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let on_rows = client
        .sow_sql(
            "/r6_pos",
            "SELECT book_name, broker FROM r6_pos p JOIN r6_trd t \
             ON p.position_id = t.position_id AND p.book_id = t.book_id",
        )
        .await
        .expect("multi-key JOIN ON must compile");
    let pairs: std::collections::HashSet<(String, String)> = on_rows
        .iter()
        .map(|r| {
            (
                r.get("book_name").unwrap().as_str().unwrap().to_string(),
                r.get("broker").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(pairs.contains(&("Alpha".into(), "GS".into())));
    assert!(pairs.contains(&("Beta".into(), "MS".into())));
    assert!(
        !pairs.iter().any(|(_, b)| b == "JPM"),
        "JPM trade had mismatched book_id, must not join"
    );

    // USING (...) form should produce identical rows.
    let using_rows = client
        .sow_sql(
            "/r6_pos",
            "SELECT book_name, broker FROM r6_pos JOIN r6_trd USING (position_id, book_id)",
        )
        .await
        .expect("multi-key USING must compile");
    let using_pairs: std::collections::HashSet<(String, String)> = using_rows
        .iter()
        .map(|r| {
            (
                r.get("book_name").unwrap().as_str().unwrap().to_string(),
                r.get("broker").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(pairs, using_pairs, "ON and USING must produce same rows");
}

#[tokio::test]
async fn distinct_single_column() {
    let topic = TopicSpec::new("/r6-distinct", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk) in [
        ("a", "FX"), ("b", "FX"), ("c", "RATES"), ("d", "EQUITIES"),
        ("e", "FX"), ("f", "RATES"),
    ] {
        client
            .publish("/r6-distinct", json!({ "k": k, "desk": desk }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql("/r6-distinct", "SELECT DISTINCT desk FROM t")
        .await
        .expect("SELECT DISTINCT must compile");
    let desks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("desk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(desks.len(), 3, "3 unique desks");
    assert!(desks.contains("FX") && desks.contains("RATES") && desks.contains("EQUITIES"));
}

#[tokio::test]
async fn distinct_multi_column() {
    let topic = TopicSpec::new("/r6-distinct-multi", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("side", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, side) in [
        ("a", "FX", "BUY"),
        ("b", "FX", "SELL"),
        ("c", "FX", "BUY"),    // dup of a's (desk, side)
        ("d", "RATES", "BUY"),
        ("e", "FX", "SELL"),   // dup of b
    ] {
        client
            .publish("/r6-distinct-multi", json!({ "k": k, "desk": desk, "side": side }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/r6-distinct-multi",
            "SELECT DISTINCT desk, side FROM t",
        )
        .await
        .expect("SELECT DISTINCT col, col must compile");
    let pairs: std::collections::HashSet<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r.get("desk").unwrap().as_str().unwrap().to_string(),
                r.get("side").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    // {(FX,BUY), (FX,SELL), (RATES,BUY)} → 3 unique pairs.
    assert_eq!(pairs.len(), 3);
}

//! P4 e2e — `LIMIT n OFFSET m` paginates after ORDER BY.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn offset_paginates_after_order_by() {
    let topic = TopicSpec::new("/offset-trades", "k").with_inline_columns([
        ("k", "string"),
        ("seq", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..20_i64 {
        client
            .publish(
                "/offset-trades",
                json!({ "k": format!("k{i:02}"), "seq": i }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Page 1 — first 5.
    let p1 = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 0",
        )
        .await
        .expect("page 1 sow");
    assert_eq!(p1.len(), 5);
    assert_eq!(p1[0].get("seq").unwrap().as_i64().unwrap(), 0);
    assert_eq!(p1[4].get("seq").unwrap().as_i64().unwrap(), 4);

    // Page 2 — seqs 5..9.
    let p2 = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 5",
        )
        .await
        .expect("page 2 sow");
    assert_eq!(p2.len(), 5);
    assert_eq!(p2[0].get("seq").unwrap().as_i64().unwrap(), 5);
    assert_eq!(p2[4].get("seq").unwrap().as_i64().unwrap(), 9);

    // Page beyond end — empty.
    let none = client
        .sow_sql(
            "/offset-trades",
            "SELECT k, seq FROM t ORDER BY seq ASC LIMIT 5 OFFSET 50",
        )
        .await
        .expect("beyond-end sow");
    assert_eq!(none.len(), 0);
}

// ───── Diversification ────────────────────────────────────────────

/// OFFSET 0 is a no-op — same result as no OFFSET at all.
#[tokio::test]
async fn offset_zero_equals_no_offset() {
    let topic = TopicSpec::new("/offset-zero", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..5_i64 {
        client
            .publish("/offset-zero", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let no_offset = client
        .sow_sql("/offset-zero", "SELECT k, v FROM t ORDER BY v ASC LIMIT 10")
        .await
        .unwrap();
    let with_zero = client
        .sow_sql(
            "/offset-zero",
            "SELECT k, v FROM t ORDER BY v ASC LIMIT 10 OFFSET 0",
        )
        .await
        .unwrap();
    assert_eq!(no_offset, with_zero);
}

/// OFFSET exactly equal to row count → empty result.
#[tokio::test]
async fn offset_equal_to_row_count_returns_empty() {
    let topic = TopicSpec::new("/offset-exact", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..3_i64 {
        client
            .publish("/offset-exact", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql(
            "/offset-exact",
            "SELECT k, v FROM t ORDER BY v ASC LIMIT 10 OFFSET 3",
        )
        .await
        .unwrap();
    assert!(rows.is_empty());
}

/// OFFSET combined with DESC ordering — pagination must walk the
/// reverse direction correctly.
#[tokio::test]
async fn offset_with_desc_order_paginates_from_largest() {
    let topic = TopicSpec::new("/offset-desc", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..10_i64 {
        client
            .publish("/offset-desc", json!({ "k": format!("k{i:02}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/offset-desc",
            "SELECT k, v FROM t ORDER BY v DESC LIMIT 3 OFFSET 2",
        )
        .await
        .unwrap();
    // DESC: [9,8,7,6,5,4,3,2,1,0]; OFFSET 2 LIMIT 3 → [7,6,5].
    let vs: Vec<i64> = rows
        .iter()
        .map(|r| r.get("v").unwrap().as_i64().unwrap())
        .collect();
    assert_eq!(vs, vec![7, 6, 5]);
}

/// OFFSET without LIMIT — returns everything past the offset.
#[tokio::test]
async fn offset_without_limit_returns_rest() {
    let topic = TopicSpec::new("/offset-no-lim", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for i in 0..7_i64 {
        client
            .publish("/offset-no-lim", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql("/offset-no-lim", "SELECT v FROM t ORDER BY v ASC OFFSET 4")
        .await
        .unwrap();
    let vs: Vec<i64> = rows
        .iter()
        .map(|r| r.get("v").unwrap().as_i64().unwrap())
        .collect();
    assert_eq!(vs, vec![4, 5, 6]);
}

//! e2e: aggregate SOW queries (GROUP BY + SUM/COUNT/AVG/MIN/MAX).
//!
//! Server-side aggregations let dashboards run "totals by desk" or
//! "max P&L by trader" against a topic without dragging every row
//! across the wire. The client uses `sow_sql` to pass a full SELECT
//! statement; the server detects aggregate syntax and switches to
//! the aggregator code path.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn group_by_desk_sum_count_avg() {
    let topic = TopicSpec::new("/agg-trades", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("qty", "long"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = [
        ("T1", "RATES", 100, 100.0),
        ("T2", "RATES", 50, 200.0),
        ("T3", "EQUITIES", 25, 300.0),
        ("T4", "EQUITIES", 75, 400.0),
        ("T5", "EQUITIES", 200, 500.0),
        ("T6", "TECH", 10, 1000.0),
    ];
    for (k, desk, qty, price) in rows {
        client
            .publish(
                "/agg-trades",
                json!({ "k": k, "desk": desk, "qty": qty, "price": price }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let rows = client
        .sow_sql(
            "/agg-trades",
            "SELECT desk, SUM(qty), COUNT(*), AVG(price) FROM t GROUP BY desk",
        )
        .await
        .expect("aggregate sow");
    assert_eq!(
        rows.len(),
        3,
        "expected 3 group rows, got {} : {:?}",
        rows.len(),
        rows
    );

    let mut by_desk = std::collections::HashMap::new();
    for r in &rows {
        let d = r.get("desk").unwrap().as_str().unwrap().to_string();
        by_desk.insert(d, r.clone());
    }

    let r = &by_desk["RATES"];
    assert_eq!(r.get("SUM(qty)").unwrap().as_i64().unwrap(), 150);
    assert_eq!(r.get("COUNT(*)").unwrap().as_u64().unwrap(), 2);
    assert!((r.get("AVG(price)").unwrap().as_f64().unwrap() - 150.0).abs() < 1e-9);

    let r = &by_desk["EQUITIES"];
    assert_eq!(r.get("SUM(qty)").unwrap().as_i64().unwrap(), 300);
    assert_eq!(r.get("COUNT(*)").unwrap().as_u64().unwrap(), 3);
    assert!((r.get("AVG(price)").unwrap().as_f64().unwrap() - 400.0).abs() < 1e-9);

    let r = &by_desk["TECH"];
    assert_eq!(r.get("SUM(qty)").unwrap().as_i64().unwrap(), 10);
    assert_eq!(r.get("COUNT(*)").unwrap().as_u64().unwrap(), 1);
    assert!((r.get("AVG(price)").unwrap().as_f64().unwrap() - 1000.0).abs() < 1e-9);
}

#[tokio::test]
async fn count_star_no_group_by_is_single_row() {
    let topic = TopicSpec::new("/agg-count", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for i in 0..15 {
        client
            .publish(
                "/agg-count",
                json!({ "k": format!("k{i}"), "v": i as f64 }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql("/agg-count", "SELECT COUNT(*) FROM t WHERE v >= 5")
        .await
        .expect("count(*) sow");
    assert_eq!(rows.len(), 1);
    let n = rows[0].get("COUNT(*)").unwrap().as_u64().unwrap();
    assert_eq!(n, 10, "expected 10 rows with v >= 5, got {n}");
}

#[tokio::test]
async fn min_max_string_groups() {
    let topic = TopicSpec::new("/agg-strmm", "k")
        .with_inline_columns([("k", "string"), ("desk", "string"), ("trader", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, desk, trader) in [
        ("T1", "RATES", "alice"),
        ("T2", "RATES", "charlie"),
        ("T3", "RATES", "bob"),
        ("T4", "EQUITIES", "zoe"),
        ("T5", "EQUITIES", "aaron"),
    ] {
        client
            .publish(
                "/agg-strmm",
                json!({ "k": k, "desk": desk, "trader": trader }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/agg-strmm",
            "SELECT desk, MIN(trader), MAX(trader) FROM t GROUP BY desk",
        )
        .await
        .expect("min/max sow");
    let mut by_desk = std::collections::HashMap::new();
    for r in &rows {
        let d = r.get("desk").unwrap().as_str().unwrap().to_string();
        by_desk.insert(d, r.clone());
    }
    assert_eq!(by_desk["RATES"].get("MIN(trader)").unwrap(), "alice");
    assert_eq!(by_desk["RATES"].get("MAX(trader)").unwrap(), "charlie");
    assert_eq!(by_desk["EQUITIES"].get("MIN(trader)").unwrap(), "aaron");
    assert_eq!(by_desk["EQUITIES"].get("MAX(trader)").unwrap(), "zoe");
}

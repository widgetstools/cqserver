//! R3 — aggregates over expressions: `SUM(a * b)`, `AVG(ABS(x))`,
//! `MAX(ROUND(rate, 0))`, etc. AMPS supports these natively for
//! VWAP, weighted-PnL, and "average absolute slippage" patterns
//! the demo library reaches for; pre-R3 cqserver rejected anything
//! that wasn't a bare column with "argument must be a column".

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn sum_of_product_vwap_pattern() {
    // VWAP-style: SUM(price * qty) / SUM(qty). This test pins
    // the SUM(price * qty) half; SUM(qty) is the existing bare-col path.
    let topic = TopicSpec::new("/r3-vwap", "k")
        .with_inline_columns([("k", "string"), ("price", "double"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, p, q) in [
        ("a", 100.0, 10_i64),
        ("b", 200.0, 5),
        ("c", 50.0, 20),
    ] {
        client
            .publish("/r3-vwap", json!({ "k": k, "price": p, "qty": q }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql(
            "/r3-vwap",
            "SELECT SUM(price * qty) AS notional, SUM(qty) AS shares FROM t",
        )
        .await
        .expect("SUM over expression must compile")
        .pop()
        .expect("one row");
    // notional = 100*10 + 200*5 + 50*20 = 1000 + 1000 + 1000 = 3000
    // shares   = 10 + 5 + 20 = 35
    assert_eq!(row.get("notional").unwrap().as_f64().unwrap(), 3000.0);
    assert_eq!(row.get("shares").unwrap().as_i64().unwrap(), 35);
}

#[tokio::test]
async fn avg_of_abs_for_average_absolute_slippage() {
    let topic = TopicSpec::new("/r3-avgabs", "k")
        .with_inline_columns([("k", "string"), ("slip", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, s) in [("a", 1.0), ("b", -3.0), ("c", -2.0), ("d", 4.0)] {
        client
            .publish("/r3-avgabs", json!({ "k": k, "slip": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql(
            "/r3-avgabs",
            "SELECT AVG(ABS(slip)) AS avg_abs_slip FROM t",
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
    // avg of {1, 3, 2, 4} = 2.5
    assert_eq!(row.get("avg_abs_slip").unwrap().as_f64().unwrap(), 2.5);
}

#[tokio::test]
async fn max_min_of_expression_with_group_by() {
    let topic = TopicSpec::new("/r3-grouped", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("a", "long"),
        ("b", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, desk, a, b) in [
        ("r1", "RATES", 10, 5),
        ("r2", "RATES", 20, 3),
        ("e1", "EQUITIES", 50, 2),
        ("e2", "EQUITIES", 10, 10),
    ] {
        client
            .publish("/r3-grouped", json!({ "k": k, "desk": desk, "a": a, "b": b }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let rows = client
        .sow_sql(
            "/r3-grouped",
            "SELECT desk, MAX(a * b) AS m FROM t GROUP BY desk",
        )
        .await
        .unwrap();
    let by_desk: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| {
            (
                r.get("desk").unwrap().as_str().unwrap().to_string(),
                r.get("m").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    // RATES: max(10*5=50, 20*3=60) = 60
    // EQUITIES: max(50*2=100, 10*10=100) = 100
    assert_eq!(by_desk["RATES"], 60.0);
    assert_eq!(by_desk["EQUITIES"], 100.0);
}

#[tokio::test]
async fn sum_of_expression_with_null_propagation() {
    let topic = TopicSpec::new("/r3-null", "k")
        .with_inline_columns([("k", "string"), ("a", "double"), ("b", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r3-null", json!({ "k": "x", "a": 2.0, "b": 3.0 }))
        .await
        .unwrap();
    // b missing → null. SUM(a + b) should skip this row entirely.
    client
        .publish("/r3-null", json!({ "k": "y", "a": 10.0 }))
        .await
        .unwrap();
    client
        .publish("/r3-null", json!({ "k": "z", "a": 5.0, "b": 7.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql("/r3-null", "SELECT SUM(a + b) AS s FROM t")
        .await
        .unwrap()
        .pop()
        .unwrap();
    // (2+3) + (NULL skipped) + (5+7) = 5 + 12 = 17
    assert_eq!(row.get("s").unwrap().as_f64().unwrap(), 17.0);
}

#[tokio::test]
async fn agg_over_expr_with_having() {
    let topic = TopicSpec::new("/r3-having", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("qty", "long"),
        ("price", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, desk, q, p) in [
        ("a", "BIG", 100_i64, 100.0),
        ("b", "BIG", 200, 200.0),
        ("c", "SMALL", 1, 1.0),
    ] {
        client
            .publish("/r3-having", json!({ "k": k, "desk": desk, "qty": q, "price": p }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r3-having",
            "SELECT desk, SUM(qty * price) AS gross FROM t GROUP BY desk HAVING SUM(qty * price) > 1000",
        )
        .await
        .unwrap();
    // BIG: 100*100 + 200*200 = 10000+40000 = 50000 ✓
    // SMALL: 1*1 = 1 ✗
    let desks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("desk").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(desks.contains("BIG"));
    assert!(!desks.contains("SMALL"));
}

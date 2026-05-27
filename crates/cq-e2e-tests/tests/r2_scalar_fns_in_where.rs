//! R2 — scalar functions + arithmetic in WHERE / HAVING. AMPS
//! supports `WHERE ABS(slip) > 5`, `WHERE qty * price > 1000`,
//! `WHERE col1 > col2`; pre-R2 cqserver rejected all of these with
//! "Unsupported expression: Expected column reference". The new
//! `NumExpr` compiler + `CompareNum`/`BetweenNum` predicates close
//! the gap.
//!
//! AMPS-style trade-filter patterns this test pins:
//!   - `WHERE ABS(slippage_bps) > 5`
//!   - `WHERE qty * price > 100000`
//!   - `WHERE ROUND(rate, 2) >= 1.5`
//!   - `WHERE FLOOR(score) = 7`
//!   - `WHERE CEIL(score) = 8`
//!   - `WHERE limit_used > limit_cap` (column-vs-column)
//!   - `WHERE qty + extra_qty BETWEEN 50 AND 100`

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn abs_in_where_filters_by_absolute_value() {
    let topic = TopicSpec::new("/r2-abs", "k")
        .with_inline_columns([("k", "string"), ("slip", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, s) in [("a", 1.0), ("b", -3.0), ("c", 8.0), ("d", -10.0), ("e", 0.5)] {
        client
            .publish("/r2-abs", json!({ "k": k, "slip": s }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/r2-abs", "SELECT k FROM t WHERE ABS(slip) > 5")
        .await
        .expect("ABS in WHERE must compile and match");
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("c"), "8 > 5");
    assert!(ks.contains("d"), "|-10| > 5");
    assert!(!ks.contains("a"));
    assert!(!ks.contains("b"));
    assert!(!ks.contains("e"));
}

#[tokio::test]
async fn arithmetic_in_where_filters_by_computed_value() {
    let topic = TopicSpec::new("/r2-mul", "k")
        .with_inline_columns([("k", "string"), ("qty", "long"), ("price", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, q, p) in [
        ("small", 10_i64, 5.0),
        ("med", 100, 50.0),
        ("big", 1000, 1000.0),
    ] {
        client
            .publish("/r2-mul", json!({ "k": k, "qty": q, "price": p }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/r2-mul", "SELECT k FROM t WHERE qty * price > 100000")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("big"), "1000*1000=1,000,000 > 100K");
    assert!(!ks.contains("med"), "100*50=5,000 not > 100K");
    assert!(!ks.contains("small"));
}

#[tokio::test]
async fn round_floor_ceil_in_where() {
    let topic = TopicSpec::new("/r2-rfc", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, v) in [("a", 7.2), ("b", 7.6), ("c", 8.1), ("d", 6.9)] {
        client
            .publish("/r2-rfc", json!({ "k": k, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let round_rows = client
        .sow_sql("/r2-rfc", "SELECT k FROM t WHERE ROUND(v) = 8")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = round_rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("b"), "round(7.6)=8");
    assert!(ks.contains("c"), "round(8.1)=8");

    let floor_rows = client
        .sow_sql("/r2-rfc", "SELECT k FROM t WHERE FLOOR(v) = 7")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = floor_rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("a") && ks.contains("b"), "floor(7.2/7.6)=7");
    assert!(!ks.contains("c") && !ks.contains("d"));

    let ceil_rows = client
        .sow_sql("/r2-rfc", "SELECT k FROM t WHERE CEIL(v) = 8")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = ceil_rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    // CEIL: 7.2→8, 7.6→8, 8.1→9, 6.9→7. Only a + b hit 8.
    assert!(ks.contains("a") && ks.contains("b"), "ceil(7.2)=ceil(7.6)=8");
    assert!(!ks.contains("c") && !ks.contains("d"));
}

#[tokio::test]
async fn column_vs_column_comparison_in_where() {
    let topic = TopicSpec::new("/r2-col-col", "k").with_inline_columns([
        ("k", "string"),
        ("used", "double"),
        ("cap", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, u, c) in [
        ("ok", 50.0, 100.0),
        ("breach1", 110.0, 100.0),
        ("breach2", 200.0, 150.0),
        ("eq", 100.0, 100.0),
    ] {
        client
            .publish("/r2-col-col", json!({ "k": k, "used": u, "cap": c }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/r2-col-col", "SELECT k FROM t WHERE used > cap")
        .await
        .expect("col-vs-col WHERE must compile");
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("breach1") && ks.contains("breach2"));
    assert!(!ks.contains("ok"));
    assert!(!ks.contains("eq"), "100 > 100 is false");
}

#[tokio::test]
async fn numexpr_between_filters_arithmetic_range() {
    let topic = TopicSpec::new("/r2-between", "k")
        .with_inline_columns([("k", "string"), ("a", "long"), ("b", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, a, b) in [("x", 10_i64, 20), ("y", 100, 50), ("z", 30, 40)] {
        client
            .publish("/r2-between", json!({ "k": k, "a": a, "b": b }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r2-between",
            "SELECT k FROM t WHERE a + b BETWEEN 50 AND 100",
        )
        .await
        .expect("NumExpr BETWEEN must compile");
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("z"), "30+40=70 in [50,100]");
    assert!(!ks.contains("x"), "10+20=30 below");
    assert!(!ks.contains("y"), "100+50=150 above");
}

#[tokio::test]
async fn numexpr_null_propagates_to_false() {
    // NULL in any input should produce NaN, which compares as false.
    let topic = TopicSpec::new("/r2-null", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r2-null", json!({ "k": "real", "v": 5.0 }))
        .await
        .unwrap();
    // No 'v' → null.
    client
        .publish("/r2-null", json!({ "k": "nullv" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/r2-null", "SELECT k FROM t WHERE ABS(v) > 0")
        .await
        .unwrap();
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("real"));
    assert!(!ks.contains("nullv"), "ABS(NULL) > 0 must be false");
}

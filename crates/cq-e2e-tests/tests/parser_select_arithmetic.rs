//! P2 e2e — scalar arithmetic in the SELECT list (`a + b AS sum`).
//!
//! The Atlas demo pre-computes `mv_x_pct`/`mv_abs` on the publisher
//! because cqserver couldn't evaluate arithmetic server-side. P2
//! enables `SELECT price * quantity AS notional FROM trades` so the
//! publisher can stop carrying derived columns.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn scalar_arithmetic_evaluates_per_row() {
    let topic = TopicSpec::new("/arith-trades", "k").with_inline_columns([
        ("k", "string"),
        ("price", "double"),
        ("quantity", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let rows = [
        ("T1", 100.0_f64, 10_i64),
        ("T2", 250.0, 4),
        ("T3", 75.5, 8),
    ];
    for (k, price, qty) in rows {
        client
            .publish(
                "/arith-trades",
                json!({ "k": k, "price": price, "quantity": qty }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Single computed column with alias.
    let out = client
        .sow_sql(
            "/arith-trades",
            "SELECT k, price, quantity, price * quantity AS notional FROM t",
        )
        .await
        .expect("arithmetic sow");
    assert_eq!(out.len(), 3);
    let by_k: std::collections::HashMap<String, &serde_json::Map<String, serde_json::Value>> = out
        .iter()
        .map(|r| (r.get("k").unwrap().as_str().unwrap().to_string(), r))
        .collect();
    for (k, price, qty) in rows {
        let row = by_k.get(k).expect("row");
        let notional = row.get("notional").and_then(|v| v.as_f64()).expect("notional");
        assert!(
            (notional - (price * qty as f64)).abs() < 1e-9,
            "notional={notional} expected={}",
            price * qty as f64
        );
    }

    // Parenthesised expression with division.
    let pct = client
        .sow_sql(
            "/arith-trades",
            "SELECT k, (price - quantity) / quantity AS pct_spread FROM t WHERE quantity > 0",
        )
        .await
        .expect("parenthesised arithmetic sow");
    assert_eq!(pct.len(), 3);
    for row in &pct {
        assert!(row.get("pct_spread").and_then(|v| v.as_f64()).is_some());
    }
}

// ───── Diversification ────────────────────────────────────────────

/// Division by zero — must surface as NULL or non-finite, never panic
/// or stall the wire.
#[tokio::test]
async fn arithmetic_division_by_zero_does_not_panic() {
    let topic = TopicSpec::new("/arith-divzero", "k")
        .with_inline_columns([("k", "string"), ("a", "long"), ("b", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, a, b) in [("d1", 100_i64, 4_i64), ("d2", 50, 0), ("d3", 25, 5)] {
        client
            .publish("/arith-divzero", json!({ "k": k, "a": a, "b": b }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/arith-divzero", "SELECT k, a / b AS ratio FROM t")
        .await
        .expect("div-by-zero arithmetic must not stall");
    assert_eq!(rows.len(), 3, "every row should come back");
    // d2 has b=0 — ratio is either NULL or non-finite; both acceptable.
    // d1 and d3 must have finite ratios.
    let by_k: std::collections::HashMap<String, &serde_json::Map<String, serde_json::Value>> =
        rows.iter()
            .map(|r| (r.get("k").unwrap().as_str().unwrap().to_string(), r))
            .collect();
    let d1_ratio = by_k["d1"].get("ratio").and_then(|v| v.as_f64()).unwrap();
    let d3_ratio = by_k["d3"].get("ratio").and_then(|v| v.as_f64()).unwrap();
    assert!(d1_ratio.is_finite() && d3_ratio.is_finite());
}

/// NULL operand propagation — `NULL + 5` → NULL (per ANSI SQL).
#[tokio::test]
async fn arithmetic_null_operand_propagates() {
    let topic = TopicSpec::new("/arith-null", "k")
        .with_inline_columns([("k", "string"), ("a", "double"), ("b", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/arith-null", json!({ "k": "n1", "a": 1.0, "b": 2.0 }))
        .await
        .unwrap();
    client
        .publish("/arith-null", json!({ "k": "n2", "a": 1.0 })) // b missing
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/arith-null", "SELECT k, a + b AS s FROM t")
        .await
        .expect("null arithmetic sow");
    assert_eq!(rows.len(), 2);
    let by_k: std::collections::HashMap<String, &serde_json::Map<String, serde_json::Value>> =
        rows.iter()
            .map(|r| (r.get("k").unwrap().as_str().unwrap().to_string(), r))
            .collect();
    assert_eq!(by_k["n1"].get("s").and_then(|v| v.as_f64()).unwrap(), 3.0);
    // n2's sum should be absent (NULL — cqserver omits null fields).
    let n2_s = by_k["n2"].get("s");
    assert!(
        n2_s.is_none() || n2_s.unwrap().is_null(),
        "NULL + b expected, got {n2_s:?}"
    );
}

/// Mixed int/double arithmetic — int*double widens to double.
#[tokio::test]
async fn arithmetic_int_double_coerces_to_double() {
    let topic = TopicSpec::new("/arith-coerce", "k")
        .with_inline_columns([("k", "string"), ("i", "long"), ("d", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/arith-coerce", json!({ "k": "x", "i": 3, "d": 2.5 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql("/arith-coerce", "SELECT i * d AS r FROM t")
        .await
        .unwrap();
    assert_eq!(rows[0].get("r").and_then(|v| v.as_f64()).unwrap(), 7.5);
}

/// R2 update — arithmetic in the WHERE clause is now supported via
/// the `NumExpr` path (was a clean error pre-R2; the demo library
/// reaches for AMPS-style `WHERE a + b > N` patterns). The test
/// now pins the positive behaviour.
#[tokio::test]
async fn arithmetic_in_where_filters_correctly() {
    let topic = TopicSpec::new("/arith-where", "k")
        .with_inline_columns([("k", "string"), ("a", "long"), ("b", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    for (k, a, b) in [("yes", 60_i64, 60), ("no", 30, 30), ("eq", 50, 50)] {
        client
            .publish("/arith-where", json!({ "k": k, "a": a, "b": b }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = client
        .sow_sql("/arith-where", "SELECT k FROM t WHERE a + b > 100")
        .await
        .expect("R2 — arithmetic in WHERE must compile");
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("yes"), "60+60=120 > 100");
    assert!(!ks.contains("no"), "30+30=60 not > 100");
    assert!(!ks.contains("eq"), "50+50=100, strict >");

    // SELECT-side arithmetic (P2) still works in the same query.
    let ok = client
        .sow_sql("/arith-where", "SELECT k, a + b AS s FROM t WHERE a + b > 100")
        .await
        .expect("SELECT-side arithmetic still works");
    assert_eq!(ok.len(), 1);
    assert_eq!(ok[0].get("s").and_then(|v| v.as_f64()).unwrap(), 120.0);
}

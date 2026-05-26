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

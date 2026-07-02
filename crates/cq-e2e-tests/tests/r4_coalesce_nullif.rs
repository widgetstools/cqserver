//! R4 — `COALESCE(a, b, ...)` + `NULLIF(a, b)` in SELECT, WHERE,
//! HAVING, and aggregate arguments. AMPS supports both as standard
//! SQL functions; the demo library leans on:
//!
//!   - `SELECT COALESCE(restriction_reason, 'NONE') AS reason …`
//!     — string fallback for nullable text columns
//!   - `SUM(price * qty) / NULLIF(SUM(qty), 0) AS vwap`
//!     — guard against divide-by-zero in VWAP
//!
//! Both shapes were rejected pre-R4. CASE WHEN is a follow-up.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn coalesce_string_fallback_in_select() {
    let topic = TopicSpec::new("/r4-coal-str", "k")
        .with_inline_columns([("k", "string"), ("reason", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r4-coal-str", json!({ "k": "a", "reason": "BLOCK" }))
        .await
        .unwrap();
    // reason absent → null.
    client
        .publish("/r4-coal-str", json!({ "k": "b" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r4-coal-str",
            "SELECT k, COALESCE(reason, 'NONE') AS r FROM t",
        )
        .await
        .expect("COALESCE in SELECT must compile");
    let by_k: std::collections::HashMap<String, String> = rows
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("r").unwrap().as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(by_k["a"], "BLOCK");
    assert_eq!(by_k["b"], "NONE", "COALESCE fell through to literal");
}

#[tokio::test]
async fn coalesce_numeric_fallback_in_select() {
    let topic = TopicSpec::new("/r4-coal-num", "k")
        .with_inline_columns([("k", "string"), ("override_qty", "long"), ("default_qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r4-coal-num", json!({ "k": "a", "override_qty": 99, "default_qty": 10 }))
        .await
        .unwrap();
    client
        .publish("/r4-coal-num", json!({ "k": "b", "default_qty": 10 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r4-coal-num",
            "SELECT k, COALESCE(override_qty, default_qty, 0) AS final_qty FROM t",
        )
        .await
        .unwrap();
    let by_k: std::collections::HashMap<String, f64> = rows
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("final_qty").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    assert_eq!(by_k["a"], 99.0);
    assert_eq!(by_k["b"], 10.0, "fell through to default_qty");
}

#[tokio::test]
async fn nullif_guards_divide_by_zero_per_row() {
    // Row-level NULLIF: `SELECT a / NULLIF(b, 0) AS r FROM t` — when
    // b is 0, the divisor becomes null and the division yields null
    // (no inf, no crash). The aggregate-wrap form
    // `NULLIF(SUM(qty), 0)` is handled by the post-aggregate
    // projection layer (Task 1.4) — see `nullif_after_aggregate` below.
    let topic = TopicSpec::new("/r4-nullif-row", "k")
        .with_inline_columns([("k", "string"), ("a", "double"), ("b", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r4-nullif-row", json!({ "k": "ok", "a": 100.0, "b": 5 }))
        .await
        .unwrap();
    client
        .publish("/r4-nullif-row", json!({ "k": "zero", "a": 100.0, "b": 0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/r4-nullif-row", "SELECT k, a / NULLIF(b, 0) AS r FROM t")
        .await
        .unwrap();
    let by_k: std::collections::HashMap<String, Option<f64>> = rows
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("r").and_then(|v| v.as_f64()),
            )
        })
        .collect();
    assert_eq!(by_k["ok"], Some(20.0));
    assert!(by_k["zero"].is_none(), "divide-by-zero guarded → null");
}

/// `NULLIF(SUM(...), 0)` — NULLIF wrapping an aggregate is a
/// post-aggregate scalar expression. Task 1.4 added the
/// "computed-over-aggregate" projection layer: scalar functions and
/// arithmetic wrapping aggregate calls are compiled into a
/// `PostAggExpr` evaluated over the finalised aggregate output row.
/// This test now verifies the divide-by-zero VWAP guard works
/// end-to-end: `SUM(qty)` = 0 → `NULLIF(SUM(qty),0)` = null →
/// `SUM(price*qty)/NULLIF(SUM(qty),0)` = null (no inf, no crash).
#[tokio::test]
async fn nullif_after_aggregate() {
    let topic = TopicSpec::new("/r4-agg-nullif", "k")
        .with_inline_columns([("k", "string"), ("price", "double"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/r4-agg-nullif", json!({ "k": "a", "price": 100.0, "qty": 0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    // NULLIF(SUM(qty), 0) → null when SUM(qty)=0; and the VWAP
    // ratio SUM(price*qty)/NULLIF(SUM(qty),0) → null too.
    let row = client
        .sow_sql(
            "/r4-agg-nullif",
            "SELECT SUM(price * qty) AS num, NULLIF(SUM(qty), 0) AS denom, \
             SUM(price * qty) / NULLIF(SUM(qty), 0) AS vwap FROM t",
        )
        .await
        .expect("scalar-over-aggregate projection must compile")
        .pop()
        .unwrap();
    let denom = row.get("denom");
    assert!(
        denom.is_none() || denom.unwrap().is_null(),
        "NULLIF(SUM(qty)=0, 0) must be null; got {denom:?}"
    );
    let vwap = row.get("vwap");
    assert!(
        vwap.is_none() || vwap.unwrap().is_null(),
        "divide by null denom must be null; got {vwap:?}"
    );
    // num is still computed: SUM(100*0) = 0.
    assert_eq!(row.get("num").unwrap().as_f64().unwrap(), 0.0);
}

#[tokio::test]
async fn nullif_passes_through_when_values_differ() {
    let topic = TopicSpec::new("/r4-nullif-pass", "k")
        .with_inline_columns([("k", "string"), ("a", "long"), ("b", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/r4-nullif-pass", json!({ "k": "x", "a": 5, "b": 3 }))
        .await
        .unwrap();
    client
        .publish("/r4-nullif-pass", json!({ "k": "y", "a": 7, "b": 7 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r4-nullif-pass",
            "SELECT k, NULLIF(a, b) AS n FROM t",
        )
        .await
        .unwrap();
    let by_k: std::collections::HashMap<String, Option<f64>> = rows
        .iter()
        .map(|r| {
            (
                r.get("k").unwrap().as_str().unwrap().to_string(),
                r.get("n").and_then(|v| v.as_f64()),
            )
        })
        .collect();
    assert_eq!(by_k["x"], Some(5.0), "5 ≠ 3, NULLIF returns 5");
    assert!(by_k["y"].is_none(), "7 = 7, NULLIF returns null");
}

#[tokio::test]
async fn coalesce_in_where_clause() {
    // `WHERE COALESCE(override_pct, default_pct) > 50` —
    // fallback before threshold check.
    let topic = TopicSpec::new("/r4-coal-where", "k").with_inline_columns([
        ("k", "string"),
        ("override_pct", "double"),
        ("default_pct", "double"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // a: override 60 > 50 → match
    client
        .publish("/r4-coal-where", json!({ "k": "a", "override_pct": 60.0, "default_pct": 10.0 }))
        .await
        .unwrap();
    // b: override null, default 75 > 50 → match (via fallback)
    client
        .publish("/r4-coal-where", json!({ "k": "b", "default_pct": 75.0 }))
        .await
        .unwrap();
    // c: override null, default 30 ≤ 50 → no match
    client
        .publish("/r4-coal-where", json!({ "k": "c", "default_pct": 30.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/r4-coal-where",
            "SELECT k FROM t WHERE COALESCE(override_pct, default_pct) > 50",
        )
        .await
        .expect("COALESCE in WHERE must compile");
    let ks: std::collections::HashSet<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(ks.contains("a") && ks.contains("b"));
    assert!(!ks.contains("c"));
}

/// Task 1.4 — the positive VWAP case: non-zero denominator, so the
/// full ratio is computed via the post-aggregate projection layer.
#[tokio::test]
async fn vwap_over_aggregates_nonzero_denominator() {
    let topic = TopicSpec::new("/r4-vwap", "k")
        .with_inline_columns([("k", "string"), ("price", "double"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    // price*qty: 100*2=200, 50*8=400 ; sum(price*qty)=600 ; sum(qty)=10
    // vwap = 600/10 = 60
    client
        .publish("/r4-vwap", json!({ "k": "a", "price": 100.0, "qty": 2 }))
        .await
        .unwrap();
    client
        .publish("/r4-vwap", json!({ "k": "b", "price": 50.0, "qty": 8 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let row = client
        .sow_sql(
            "/r4-vwap",
            "SELECT SUM(price * qty) / NULLIF(SUM(qty), 0) AS vwap FROM t",
        )
        .await
        .expect("VWAP scalar-over-aggregate must compile")
        .pop()
        .unwrap();
    assert_eq!(row.get("vwap").unwrap().as_f64().unwrap(), 60.0);
}

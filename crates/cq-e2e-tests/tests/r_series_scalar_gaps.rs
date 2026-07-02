//! R-series follow-up probes (task 1.4). The three "known engine
//! limitations" recorded in AMPS_PARITY_WORKLOG.md:1037-1050 plus the
//! scalar-over-aggregate projection shape. Each shape must either
//! WORK or ERROR CLEANLY — never hang, never silently mis-answer.
//!
//!   1. HAVING on an aggregate that is not also in SELECT.
//!   2. Scalar functions in ORDER BY (`ORDER BY ABS(col) DESC`).
//!   3. Scalar functions in SELECT projection (`SELECT ABS(col) AS x`).
//!
//! These are kept as permanent regression tests: passing probes lock
//! in behaviour, rejecting probes lock in the clean-error contract.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

/// Gap #1 — `HAVING SUM(qty) > 10` where `SUM(qty)` is NOT in the
/// SELECT list. cqserver clean-rejects with a message naming the
/// workaround (put the aggregate in SELECT). This is the
/// clean-reject contract; it must not hang or mis-answer.
#[tokio::test]
async fn having_on_aggregate_not_in_select_errors_cleanly() {
    let topic = TopicSpec::new("/rgap-having", "k")
        .with_inline_columns([("k", "string"), ("g", "string"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/rgap-having", json!({ "k": "a", "g": "x", "qty": 5 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let res = client
        .sow_sql(
            "/rgap-having",
            "SELECT g FROM t GROUP BY g HAVING SUM(qty) > 10",
        )
        .await;
    let err = res.expect_err("HAVING on aggregate not in SELECT must error, not hang");
    let msg = format!("{err}");
    assert!(
        msg.contains("HAVING references an aggregate not in SELECT"),
        "error must name the shape; got: {msg}"
    );
    assert!(
        msg.contains("add the aggregate to the SELECT list"),
        "error must name the workaround; got: {msg}"
    );
}

/// Sanity: HAVING on an aggregate that IS in SELECT still works —
/// the workaround the demo used. Guards against the clean-reject
/// above accidentally rejecting the supported form.
#[tokio::test]
async fn having_on_aggregate_in_select_still_works() {
    let topic = TopicSpec::new("/rgap-having-ok", "k")
        .with_inline_columns([("k", "string"), ("g", "string"), ("qty", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/rgap-having-ok", json!({ "k": "a", "g": "x", "qty": 5 }))
        .await
        .unwrap();
    client
        .publish("/rgap-having-ok", json!({ "k": "b", "g": "x", "qty": 20 }))
        .await
        .unwrap();
    client
        .publish("/rgap-having-ok", json!({ "k": "c", "g": "y", "qty": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql(
            "/rgap-having-ok",
            "SELECT g, SUM(qty) AS s FROM t GROUP BY g HAVING SUM(qty) > 10",
        )
        .await
        .expect("HAVING on SELECTed aggregate must compile");
    assert_eq!(rows.len(), 1, "only group x (sum 25) passes");
    assert_eq!(rows[0].get("g").unwrap().as_str().unwrap(), "x");
}

/// Gap #2 — `ORDER BY ABS(col) DESC`: scalar function in ORDER BY.
/// cqserver clean-rejects this shape (an unknown-column parse error);
/// it must never hang. Clean-reject is the accepted contract here —
/// the demo orders by the underlying column or a SELECT alias instead.
#[tokio::test]
async fn scalar_fn_in_order_by() {
    let topic = TopicSpec::new("/rgap-orderby", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/rgap-orderby", json!({ "k": "a", "v": -5.0 }))
        .await
        .unwrap();
    client
        .publish("/rgap-orderby", json!({ "k": "b", "v": 3.0 }))
        .await
        .unwrap();
    client
        .publish("/rgap-orderby", json!({ "k": "c", "v": -9.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let res = client
        .sow_sql("/rgap-orderby", "SELECT k, v FROM t ORDER BY ABS(v) DESC")
        .await;
    // The workaround: order by a SELECT alias instead. Clean-reject is
    // the contract; assert we got an error, not a hang or wrong answer,
    // and that the message names the workaround.
    let err = res.expect_err("ORDER BY ABS(v) must clean-reject, not hang");
    let msg = format!("{err}");
    assert!(
        msg.contains("scalar functions in ORDER BY are not supported"),
        "error must name the shape; got: {msg}"
    );
    assert!(
        msg.contains("ORDER BY a SELECT alias"),
        "error must name the workaround; got: {msg}"
    );
}

/// Gap #3 — `SELECT ABS(col) AS x`: scalar function in SELECT
/// projection. Task 1.4 implements this — ABS/ROUND/FLOOR/CEIL are
/// now first-class in the SELECT scalar path, so it computes.
#[tokio::test]
async fn scalar_fn_in_select_projection() {
    let topic = TopicSpec::new("/rgap-select", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client
        .publish("/rgap-select", json!({ "k": "a", "v": -5.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let rows = client
        .sow_sql("/rgap-select", "SELECT k, ABS(v) AS av FROM t")
        .await
        .expect("ABS in SELECT projection must compile (Task 1.4)");
    let r = rows.iter().find(|r| r.get("k").unwrap() == "a").unwrap();
    assert_eq!(r.get("av").unwrap().as_f64().unwrap(), 5.0, "ABS(-5)=5");
}

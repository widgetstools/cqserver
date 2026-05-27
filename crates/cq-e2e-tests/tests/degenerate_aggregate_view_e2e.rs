//! P5 e2e — degenerate-aggregate view (`SELECT SUM(x) FROM t` with no
//! GROUP BY) stays single-row across refreshes.
//!
//! AMPS_PARITY §4 bug 3 — before P5 the view's SOW grew by one row
//! per source publish because keyless upserts always appended. The
//! fix collapses keyless upserts onto row 0 so the view stays at
//! exactly one row holding the latest aggregate.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn degenerate_aggregate_view_stays_single_row() {
    let source = TopicSpec::new("/deg-trades", "k").with_inline_columns([
        ("k", "string"),
        ("qty", "long"),
    ]);
    let view = ViewSpec::new(
        "/v_total_qty",
        "/deg-trades",
        // No GROUP BY — degenerate aggregate.
        "SELECT SUM(qty) AS total FROM t",
    );

    let opts = ServerOpts {
        views: vec![view],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Publish 5 source rows; the running total should land at 150.
    for (i, qty) in [10_i64, 20, 30, 40, 50].iter().enumerate() {
        client
            .publish(
                "/deg-trades",
                json!({ "k": format!("t{i}"), "qty": qty }),
            )
            .await
            .expect("publish");
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    // SOW the degenerate-aggregate view — must be exactly one row,
    // and `total` must equal the latest cumulative sum.
    let snap = client
        .sow_sql("/v_total_qty", "SELECT total FROM t")
        .await
        .expect("view sow");
    assert_eq!(
        snap.len(),
        1,
        "degenerate-aggregate view must stay single-row, got {} rows: {:?}",
        snap.len(),
        snap
    );
    let total = snap[0].get("total").and_then(|v| v.as_i64()).unwrap();
    assert_eq!(total, 150, "running total mismatch");
}

// ───── Diversification ────────────────────────────────────────────

/// Updating an existing source row (same key) — running total must
/// reflect the delta, not duplicate the row in the view.
#[tokio::test]
async fn degenerate_view_handles_updates_correctly() {
    let source = TopicSpec::new("/deg-upd", "k")
        .with_inline_columns([("k", "string"), ("qty", "long")]);
    let view = ViewSpec::new("/v_deg_upd", "/deg-upd", "SELECT SUM(qty) AS s FROM t");
    let opts = ServerOpts { views: vec![view], ..ServerOpts::default() };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/deg-upd", json!({ "k": "a", "qty": 100 }))
        .await
        .unwrap();
    client
        .publish("/deg-upd", json!({ "k": "b", "qty": 200 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap1 = client
        .sow_sql("/v_deg_upd", "SELECT s FROM t")
        .await
        .unwrap();
    assert_eq!(snap1.len(), 1);
    assert_eq!(snap1[0].get("s").unwrap().as_i64().unwrap(), 300);

    // Update a's qty. The aggregating-view evaluator must apply the
    // delta (old=100 → new=50) so the running SUM moves 300 → 250.
    // Allow a longer settle window — the view's mutation tap is
    // async; some scenarios need several event-loop turns to drain.
    client
        .publish("/deg-upd", json!({ "k": "a", "qty": 50 }))
        .await
        .unwrap();

    let mut snap2 = Vec::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        snap2 = client
            .sow_sql("/v_deg_upd", "SELECT s FROM t")
            .await
            .unwrap();
        if snap2.len() == 1
            && snap2[0].get("s").and_then(|v| v.as_i64()) == Some(250)
        {
            break;
        }
    }
    assert_eq!(snap2.len(), 1, "still single row after update");
    assert_eq!(
        snap2[0].get("s").unwrap().as_i64().unwrap(),
        250,
        "after a:100→50 + b:200, SUM must be 250 (not the stale 300)"
    );
}

/// Degenerate view on an empty source — should still produce a single
/// row (with sum = 0 or null per ANSI).
#[tokio::test]
async fn degenerate_view_on_empty_source_produces_one_row() {
    let source = TopicSpec::new("/deg-empty", "k")
        .with_inline_columns([("k", "string"), ("qty", "long")]);
    let view = ViewSpec::new("/v_deg_empty", "/deg-empty", "SELECT COUNT(*) AS c FROM t");
    let opts = ServerOpts { views: vec![view], ..ServerOpts::default() };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    tokio::time::sleep(Duration::from_millis(150)).await;

    let snap = client
        .sow_sql("/v_deg_empty", "SELECT c FROM t")
        .await
        .unwrap();
    assert_eq!(snap.len(), 1, "COUNT(*) on empty source should be a single row with c=0");
    assert_eq!(snap[0].get("c").unwrap().as_i64().unwrap(), 0);
}

/// MIN + MAX in the same degenerate aggregate — multiple aggregates
/// in one row must all collapse to row 0, not split into multiple rows.
#[tokio::test]
async fn degenerate_view_multi_aggregate_stays_single_row() {
    let source = TopicSpec::new("/deg-multi", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let view = ViewSpec::new(
        "/v_deg_multi",
        "/deg-multi",
        "SELECT MIN(v) AS lo, MAX(v) AS hi, COUNT(*) AS n FROM t",
    );
    let opts = ServerOpts { views: vec![view], ..ServerOpts::default() };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (k, v) in [("a", 5_i64), ("b", 1), ("c", 100), ("d", 50)] {
        client
            .publish("/deg-multi", json!({ "k": k, "v": v }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let snap = client
        .sow_sql("/v_deg_multi", "SELECT lo, hi, n FROM t")
        .await
        .unwrap();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].get("lo").unwrap().as_i64().unwrap(), 1);
    assert_eq!(snap[0].get("hi").unwrap().as_i64().unwrap(), 100);
    assert_eq!(snap[0].get("n").unwrap().as_i64().unwrap(), 4);
}

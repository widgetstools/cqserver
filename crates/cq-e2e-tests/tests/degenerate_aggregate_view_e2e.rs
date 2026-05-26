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

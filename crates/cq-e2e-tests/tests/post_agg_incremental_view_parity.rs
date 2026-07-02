//! Task 1.4 (Finding 2) — post-aggregate projection (`apply_post_agg`)
//! through the INCREMENTAL view path.
//!
//! `apply_post_agg` is the shared finalizer used by three emit sites:
//! one-shot SOW, full-refresh, and — the riskiest, least-exercised
//! path — `aggregate_one_group`, which the materialized-view runner
//! uses to recompute a single group incrementally on each source
//! mutation (see `crates/cq-core/src/query.rs`). This test drives a
//! `[[views]]`-style view whose SQL includes a post-agg column
//! (`SUM(x)/NULLIF(SUM(y),0) AS ratio`) end-to-end:
//!
//!   (a) initial SOW of the view has the correct ratio per group,
//!       including a group where SUM(y)=0 → ratio is NULL;
//!   (b) a live delta after a source update carries a recomputed
//!       ratio equal to a fresh SOW of the view (snapshot/live-delta
//!       parity — the incremental path must not drift from the
//!       full-recompute path);
//!   (c) SUM(y) transitioning 0 → nonzero flips ratio NULL → a value
//!       in a live delta (not just eventually-consistent SOW).
//!
//! This is a characterization/parity test: it locks in behaviour. If
//! it fails, that is a real bug in `aggregate_one_group` /
//! `apply_post_agg` lockstep, not a test bug.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;

/// Poll the view's SOW until every group in `want` (grp -> (sx, ratio))
/// matches, or the deadline expires. Returns the last-seen snapshot,
/// keyed by group, for assertion/diagnostic purposes.
async fn poll_view_snapshot(
    client: &Client,
    view: &str,
    deadline: Duration,
) -> HashMap<String, (i64, Option<f64>)> {
    let start = std::time::Instant::now();
    let mut last: HashMap<String, (i64, Option<f64>)> = HashMap::new();
    loop {
        let snap = client.sow(view, None).await.unwrap();
        last = snap
            .iter()
            .map(|r| {
                let grp = r.get("grp").unwrap().as_str().unwrap().to_string();
                let sx = r.get("sx").unwrap().as_i64().unwrap();
                let ratio = r.get("ratio").and_then(|v| v.as_f64());
                (grp, (sx, ratio))
            })
            .collect();
        if start.elapsed() > deadline {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn post_agg_incremental_view_parity() {
    let source = TopicSpec::new("/pa-src", "k").with_inline_columns([
        ("k", "string"),
        ("grp", "string"),
        ("x", "long"),
        ("y", "long"),
    ]);
    let view = ViewSpec::new(
        "/pa-view",
        "/pa-src",
        "SELECT grp, SUM(x) AS sx, SUM(x) / NULLIF(SUM(y), 0) AS ratio FROM t GROUP BY grp",
    );
    let opts = ServerOpts {
        views: vec![view],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed: group A has y=0 (ratio must be NULL), group B has a
    // normal nonzero denom.
    client
        .publish("/pa-src", serde_json::json!({ "k": "a1", "grp": "A", "x": 10, "y": 0 }))
        .await
        .unwrap();
    client
        .publish("/pa-src", serde_json::json!({ "k": "b1", "grp": "B", "x": 20, "y": 4 }))
        .await
        .unwrap();

    // (a) Initial SOW: A.ratio is NULL (SUM(y)=0), B.ratio = 20/4 = 5.
    let snap = poll_view_snapshot(&client, "/pa-view", Duration::from_secs(5)).await;
    assert_eq!(snap.get("A").map(|v| v.0), Some(10), "A: sx should be 10; snap={snap:?}");
    assert_eq!(
        snap.get("A").and_then(|v| v.1),
        None,
        "A: ratio must be NULL when SUM(y)=0; snap={snap:?}"
    );
    assert_eq!(snap.get("B").map(|v| v.0), Some(20), "B: sx should be 20; snap={snap:?}");
    assert_eq!(
        snap.get("B").and_then(|v| v.1),
        Some(5.0),
        "B: ratio should be 20/4=5; snap={snap:?}"
    );

    // Now subscribe to the view for live deltas.
    let mut sub = client
        .sow_and_subscribe("/pa-view", None, None)
        .await
        .expect("subscribe to view");
    // Drain the initial snapshot deltas (2 groups).
    let mut seen_initial: HashMap<String, (i64, Option<f64>)> = HashMap::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while seen_initial.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await
        {
            if let Some(grp) = d.data.get("grp").and_then(Value::as_str) {
                let sx = d.data.get("sx").and_then(Value::as_i64).unwrap_or(-1);
                let ratio = d.data.get("ratio").and_then(Value::as_f64);
                seen_initial.insert(grp.to_string(), (sx, ratio));
            }
        }
    }
    assert_eq!(
        seen_initial.get("A").and_then(|v| v.1),
        None,
        "initial delta snapshot for A must carry NULL ratio; seen={seen_initial:?}"
    );

    // (b) + (c) — publish an update to group B that changes x AND y
    // but leaves the ratio identical (20/4=5 -> 40/8=5): the "inputs
    // change, ratio identical" case from the task spec.
    client
        .publish("/pa-src", serde_json::json!({ "k": "b1", "grp": "B", "x": 40, "y": 8 }))
        .await
        .unwrap();

    let mut saw_b_update = false;
    let mut b_observed: Vec<(i64, Option<f64>)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !saw_b_update && std::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await;
        let Ok(Some(d)) = next else { continue };
        if d.data.get("grp").and_then(Value::as_str) == Some("B") {
            let sx = d.data.get("sx").and_then(Value::as_i64).unwrap_or(-1);
            let ratio = d.data.get("ratio").and_then(Value::as_f64);
            b_observed.push((sx, ratio));
            if sx == 40 {
                saw_b_update = true;
                assert_eq!(
                    ratio,
                    Some(5.0),
                    "B: x,y both doubled (40/8) -> ratio must stay 5.0 in the live delta; observed={b_observed:?}"
                );
            }
        }
    }
    assert!(
        saw_b_update,
        "expected a live delta for group B reflecting sx=40; observed={b_observed:?}"
    );

    // Live delta must agree with a fresh SOW of the view (snapshot vs
    // live-delta parity across the incremental vs full-recompute
    // paths).
    let fresh = poll_view_snapshot(&client, "/pa-view", Duration::from_secs(3)).await;
    assert_eq!(
        fresh.get("B"),
        Some(&(40, Some(5.0))),
        "fresh SOW must agree with the live delta just observed; fresh={fresh:?}"
    );

    // (c) SUM(y) transitioning 0 -> nonzero must flip ratio NULL ->
    // value in a LIVE DELTA (not just eventual SOW consistency).
    client
        .publish("/pa-src", serde_json::json!({ "k": "a1", "grp": "A", "x": 10, "y": 2 }))
        .await
        .unwrap();

    let mut saw_a_flip = false;
    let mut a_observed: Vec<(i64, Option<f64>)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !saw_a_flip && std::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await;
        let Ok(Some(d)) = next else { continue };
        if d.data.get("grp").and_then(Value::as_str) == Some("A") {
            let sx = d.data.get("sx").and_then(Value::as_i64).unwrap_or(-1);
            let ratio = d.data.get("ratio").and_then(Value::as_f64);
            a_observed.push((sx, ratio));
            if ratio == Some(5.0) {
                saw_a_flip = true;
            }
        }
    }
    assert!(
        saw_a_flip,
        "A: SUM(y) 0->2 must flip ratio NULL->10/2=5.0 in a live delta; observed={a_observed:?}"
    );

    // Final fresh SOW must also agree.
    let final_snap = poll_view_snapshot(&client, "/pa-view", Duration::from_secs(3)).await;
    assert_eq!(
        final_snap.get("A"),
        Some(&(10, Some(5.0))),
        "final SOW must agree with the flip just observed; final_snap={final_snap:?}"
    );
}

//! Live, sorted, windowed GROUP BY level — the heart of live AG-Grid SSRM:
//! `SELECT sym, SUM(qty) AS total GROUP BY sym ORDER BY total DESC LIMIT 3`,
//! subscribed live. As trades land, group totals change, groups re-rank, and
//! the subscriber must receive window-scoped deltas (a group entering the
//! top-3 pushes one off).
//!
//! This proves the end-to-end behaviour cqserver supports TODAY (via the
//! aggregate full-recompute path). `RankedGroups` (cq-core) is the incremental
//! fast path that makes it scale to thousands of groups — wired underneath
//! this same observable contract.

use std::time::{Duration, Instant};

use cq_client::{Client, DeltaKind, Subscription};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Map, Value};

async fn drain_snapshot(sub: &mut Subscription, quiet: Duration) -> Vec<Map<String, Value>> {
    let mut rows = Vec::new();
    loop {
        match tokio::time::timeout(quiet, sub.next_delta()).await {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => rows.push(d.data),
            Ok(Some(_)) | Ok(None) | Err(_) => return rows,
        }
    }
}

async fn wait_for(sub: &mut Subscription, timeout: Duration, mut p: impl FnMut(&cq_client::Delta) -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(Some(d)) = tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            if p(&d) {
                return true;
            }
        }
    }
    false
}

fn sym(m: &Map<String, Value>) -> Option<String> {
    m.get("sym").and_then(|v| v.as_str()).map(|s| s.to_string())
}
fn total(m: &Map<String, Value>) -> Option<i64> {
    m.get("total").and_then(|v| v.as_i64())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_windowed_sorted_aggregate_ticks() {
    let topic = TopicSpec::new("/t", "id").with_inline_columns([
        ("id", "string"),
        ("sym", "string"),
        ("qty", "long"),
    ]);
    let server = start_server(vec![topic]).await;
    let pubc = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed: 5 symbols with distinct totals. AAA=500, BBB=400, CCC=300,
    // DDD=200, EEE=100.
    let seed = [("AAA", 500), ("BBB", 400), ("CCC", 300), ("DDD", 200), ("EEE", 100)];
    let mut id = 0;
    for (s, q) in seed {
        let mut m = Map::new();
        m.insert("id".into(), json!(format!("t{id}")));
        m.insert("sym".into(), json!(s));
        m.insert("qty".into(), json!(q));
        pubc.publish("/t", Value::Object(m)).await.unwrap();
        id += 1;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Subscribe to the top-3 symbols by total, descending.
    let sub_client = Client::connect(&server.tcp_url()).await.expect("connect");
    let mut sub = sub_client
        .sow_and_subscribe_sql(
            "/t",
            "SELECT sym, SUM(qty) AS total FROM t GROUP BY sym ORDER BY total DESC LIMIT 3",
        )
        .await
        .expect("subscribe windowed aggregate");

    let snap = drain_snapshot(&mut sub, Duration::from_millis(800)).await;
    let mut top: Vec<(String, i64)> =
        snap.iter().filter_map(|r| Some((sym(r)?, total(r)?))).collect();
    top.sort_by(|a, b| b.1.cmp(&a.1));
    assert_eq!(
        top,
        vec![("AAA".into(), 500), ("BBB".into(), 400), ("CCC".into(), 300)],
        "initial top-3 window"
    );

    // Now publish a big EEE trade: EEE 100 -> 100+450 = 550, the new #1.
    // It enters the top-3 window; CCC (300) falls out to #4.
    let mut m = Map::new();
    m.insert("id".into(), json!("boom"));
    m.insert("sym".into(), json!("EEE"));
    m.insert("qty".into(), json!(450));
    pubc.publish("/t", Value::Object(m)).await.unwrap();

    // The subscriber must see EEE become a top-3 group at 550.
    let saw_eee = wait_for(&mut sub, Duration::from_secs(5), |d| {
        d.delta_type != DeltaKind::SowSnapshot
            && sym(&d.data).as_deref() == Some("EEE")
            && total(&d.data) == Some(550)
    })
    .await;
    assert!(saw_eee, "EEE should enter the top-3 window at 550 after the live trade");

    // Cross-check via a fresh one-shot query: top-3 is now EEE, AAA, BBB.
    let q = sub_client
        .sow_sql("/t", "SELECT sym, SUM(qty) AS total FROM t GROUP BY sym ORDER BY total DESC LIMIT 3")
        .await
        .unwrap();
    let mut now: Vec<(String, i64)> =
        q.iter().filter_map(|r| Some((sym(r)?, total(r)?))).collect();
    now.sort_by(|a, b| b.1.cmp(&a.1));
    assert_eq!(
        now,
        vec![("EEE".into(), 550), ("AAA".into(), 500), ("BBB".into(), 400)],
        "top-3 after the live trade"
    );
}

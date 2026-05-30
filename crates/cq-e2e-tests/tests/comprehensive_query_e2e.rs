//! Comprehensive multi-connection feature tour.
//!
//! Boots ONE cqserver and opens **14 concurrent client connections**, each
//! demonstrating a different way to query cqserver. It doubles as living
//! documentation of the query surface:
//!
//!   c1  publisher — `publish` / `publish_batch` / `delta_publish` / `sow_delete`
//!   c2  plain one-shot SOW                (`sow`, no filter)
//!   c3  filtered one-shot SOW             (`sow` + WHERE)
//!   c4  aggregate SOW (GROUP BY)          (`sow_sql`, SUM/COUNT)
//!   c5  TopN SOW (ORDER BY … LIMIT)       (`sow_sql`)
//!   c6  sow_and_subscribe (SOW + live Add/Update)
//!   c7  delta_subscribe (live only, no snapshot)
//!   c8  continuous aggregate subscribe    (`sow_and_subscribe_sql` GROUP BY)
//!   c9  JOIN view subscribe               (trades ⨝ securities)
//!   c10 aggregate view subscribe          (materialized GROUP BY view)
//!   c11 historical as-of SOW              (`sow_as_of_sequence`)
//!   c12 OOF / Remove on `sow_delete`      (filtered subscribe)
//!   c13 conflated subscribe               (coalesced fast feed)
//!   c14 bookmark replay                   (`sow_and_subscribe` with bookmark)
//!
//! Everything runs against a single shared dataset so the assertions also
//! cross-check that the engine stays consistent across query shapes.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use cq_client::{Client, Delta, DeltaKind, Subscription};
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Map, Value};

// ───────────────────────── helpers ─────────────────────────

/// Drain `SowSnapshot` rows until `quiet` elapses without one (or a live
/// delta arrives, which ends the snapshot phase). Returns the snapshot rows.
async fn drain_snapshot(sub: &mut Subscription, quiet: Duration) -> Vec<Map<String, Value>> {
    let mut rows = Vec::new();
    loop {
        match tokio::time::timeout(quiet, sub.next_delta()).await {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => rows.push(d.data),
            Ok(Some(_)) | Ok(None) | Err(_) => return rows,
        }
    }
}

/// Poll `next_delta` until `pred` matches or `timeout` elapses.
async fn wait_for_delta(
    sub: &mut Subscription,
    timeout: Duration,
    mut pred: impl FnMut(&Delta) -> bool,
) -> Option<Delta> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) if pred(&d) => return Some(d),
            Ok(Some(_)) => continue,
            Ok(None) | Err(_) => continue,
        }
    }
    None
}

fn s(m: &Map<String, Value>, k: &str) -> Option<String> {
    m.get(k).and_then(|v| v.as_str()).map(|x| x.to_string())
}
fn i(m: &Map<String, Value>, k: &str) -> Option<i64> {
    m.get(k).and_then(|v| v.as_i64())
}
fn f(m: &Map<String, Value>, k: &str) -> Option<f64> {
    m.get(k).and_then(|v| v.as_f64())
}

async fn connect(url: &str) -> Client {
    Client::connect(url).await.expect("connect")
}

// ───────────────────────── the tour ─────────────────────────

#[tokio::test]
async fn comprehensive_multi_connection_feature_tour() {
    // ── topics + views ──
    let trades = TopicSpec::new("/trades", "trade_id")
        .with_inline_columns([
            ("trade_id", "string"),
            ("symbol", "string"),
            ("qty", "long"),
            ("price", "double"),
            ("desk", "string"),
        ])
        .with_index_columns(["symbol"])
        .with_persist(); // persistent → as-of + bookmark replay work
    let securities = TopicSpec::new("/securities", "symbol").with_inline_columns([
        ("symbol", "string"),
        ("sector", "string"),
    ]);
    let quotes = TopicSpec::new("/quotes", "symbol")
        .with_inline_columns([
            ("symbol", "string"),
            ("bid", "double"),
            ("ask", "double"),
        ])
        .with_conflation(120); // fast feed → coalesced per 120ms

    let v_desk = ViewSpec::new(
        "/v_desk",
        "/trades",
        "SELECT desk, COUNT(*) AS n, SUM(qty) AS total_qty FROM \"/trades\" GROUP BY desk",
    );
    let v_sector = ViewSpec::new(
        "/v_sector",
        "/trades",
        "SELECT sector, SUM(qty) AS qty FROM \"/trades\" \
         JOIN \"/securities\" USING (symbol) GROUP BY sector",
    );

    let server = start_server_with(
        vec![trades, securities, quotes],
        ServerOpts {
            views: vec![v_desk, v_sector],
            ..ServerOpts::default()
        },
    )
    .await;
    let url = server.tcp_url();

    // ── c1: publisher — seed the shared dataset ──
    let pubc = connect(&url).await;
    for (sym, sector) in [("AAPL", "Tech"), ("MSFT", "Tech"), ("JPM", "Banks")] {
        pubc.publish("/securities", json!({ "symbol": sym, "sector": sector }))
            .await
            .unwrap();
    }
    // Initial trades. desk EQ: AAPL,MSFT; desk RATES: JPM.
    let seed = [
        ("t1", "AAPL", 100i64, 190.0f64, "EQ"),
        ("t2", "MSFT", 200, 410.0, "EQ"),
        ("t3", "AAPL", 50, 191.0, "EQ"),
        ("t4", "JPM", 300, 150.0, "RATES"),
    ];
    let mut last_seq = 0u64;
    for (id, sym, qty, px, desk) in seed {
        last_seq = pubc
            .publish(
                "/trades",
                json!({ "trade_id": id, "symbol": sym, "qty": qty, "price": px, "desk": desk }),
            )
            .await
            .unwrap();
    }
    let baseline_seq = last_seq; // state "as of" the initial seed

    // Let the evaluator + view runners settle the seed.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ── c2: plain one-shot SOW — every trade row ──
    let c2 = connect(&url).await;
    let all = c2.sow("/trades", None).await.unwrap();
    assert_eq!(all.len(), 4, "plain SOW should return all 4 trades");

    // ── c3: filtered one-shot SOW — WHERE symbol = 'AAPL' ──
    let c3 = connect(&url).await;
    let aapl = c3.sow("/trades", Some("symbol = 'AAPL'")).await.unwrap();
    assert_eq!(aapl.len(), 2, "two AAPL trades (t1, t3)");
    assert!(aapl.iter().all(|r| s(r, "symbol").as_deref() == Some("AAPL")));

    // ── c4: aggregate SOW — GROUP BY desk (SUM, COUNT) ──
    let c4 = connect(&url).await;
    let by_desk = c4
        .sow_sql(
            "/trades",
            "SELECT desk, COUNT(*) AS n, SUM(qty) AS total_qty FROM trades GROUP BY desk",
        )
        .await
        .unwrap();
    let desk: HashMap<String, (i64, i64)> = by_desk
        .iter()
        .filter_map(|r| Some((s(r, "desk")?, (i(r, "n")?, i(r, "total_qty")?))))
        .collect();
    assert_eq!(desk.get("EQ"), Some(&(3, 350)), "EQ: 3 trades, qty 100+200+50");
    assert_eq!(desk.get("RATES"), Some(&(1, 300)));

    // ── c5: TopN SOW — ORDER BY price DESC LIMIT 2 ──
    let c5 = connect(&url).await;
    let top = c5
        .sow_sql(
            "/trades",
            "SELECT trade_id, price FROM trades ORDER BY price DESC LIMIT 2",
        )
        .await
        .unwrap();
    assert_eq!(top.len(), 2, "TopN should cap at 2 rows");
    assert_eq!(f(&top[0], "price"), Some(410.0), "highest price first (MSFT t2)");
    assert!(f(&top[1], "price").unwrap() >= 191.0);

    // ── c6: sow_and_subscribe — snapshot + live Add ──
    let c6 = connect(&url).await;
    let mut sub6 = c6
        .sow_and_subscribe("/trades", Some("desk = 'EQ'"), None)
        .await
        .unwrap();
    let snap6 = drain_snapshot(&mut sub6, Duration::from_millis(600)).await;
    assert_eq!(snap6.len(), 3, "EQ snapshot = t1,t2,t3");

    // ── c7: plain `subscribe` — live only, NO snapshot ──
    // (`subscribe` registers for live deltas without replaying the SOW;
    //  `delta_subscribe` is the *sparse* SOW+delta variant that does snapshot.)
    let c7 = connect(&url).await;
    let mut sub7 = c7.subscribe("/trades", None).await.unwrap();
    let snap7 = drain_snapshot(&mut sub7, Duration::from_millis(400)).await;
    assert!(snap7.is_empty(), "plain subscribe delivers no SOW snapshot");

    // ── c8: continuous aggregate subscribe — GROUP BY desk ──
    let c8 = connect(&url).await;
    let mut sub8 = c8
        .sow_and_subscribe_sql(
            "/trades",
            "SELECT desk, SUM(qty) AS total_qty FROM trades GROUP BY desk",
        )
        .await
        .unwrap();
    let snap8 = drain_snapshot(&mut sub8, Duration::from_millis(600)).await;
    let agg8: HashMap<String, i64> = snap8
        .iter()
        .filter_map(|r| Some((s(r, "desk")?, i(r, "total_qty")?)))
        .collect();
    assert_eq!(agg8.get("EQ"), Some(&350));
    assert_eq!(agg8.get("RATES"), Some(&300));

    // ── c9: JOIN view subscribe — trades ⨝ securities by sector ──
    let c9 = connect(&url).await;
    let mut sub9 = c9.sow_and_subscribe("/v_sector", None, None).await.unwrap();
    let snap9 = drain_snapshot(&mut sub9, Duration::from_millis(800)).await;
    let sector: HashMap<String, i64> = snap9
        .iter()
        .filter_map(|r| Some((s(r, "sector")?, i(r, "qty")?)))
        .collect();
    // Tech = AAPL(100+50)+MSFT(200) = 350; Banks = JPM(300).
    assert_eq!(sector.get("Tech"), Some(&350), "JOIN view Tech exposure");
    assert_eq!(sector.get("Banks"), Some(&300));

    // ── c10: aggregate view subscribe — materialized GROUP BY ──
    let c10 = connect(&url).await;
    let mut sub10 = c10.sow_and_subscribe("/v_desk", None, None).await.unwrap();
    let snap10 = drain_snapshot(&mut sub10, Duration::from_millis(800)).await;
    let vdesk: HashMap<String, i64> = snap10
        .iter()
        .filter_map(|r| Some((s(r, "desk")?, i(r, "total_qty")?)))
        .collect();
    assert_eq!(vdesk.get("EQ"), Some(&350));
    assert_eq!(vdesk.get("RATES"), Some(&300));

    // ── c12: filtered subscribe to observe a Remove on sow_delete ──
    let c12 = connect(&url).await;
    let mut sub12 = c12
        .sow_and_subscribe("/trades", Some("price > 100"), None)
        .await
        .unwrap();
    let snap12 = drain_snapshot(&mut sub12, Duration::from_millis(600)).await;
    assert_eq!(snap12.len(), 4, "all 4 seed trades have price > 100");

    // ── c13: conflated subscribe to /quotes ──
    let c13 = connect(&url).await;
    let mut sub13 = c13.sow_and_subscribe("/quotes", None, None).await.unwrap();
    let _ = drain_snapshot(&mut sub13, Duration::from_millis(300)).await; // empty SOW

    // ───── drive LIVE changes from the publisher (c1) ─────

    // New EQ trade → c6 sees an Add; c8's EQ group Updates.
    pubc.publish(
        "/trades",
        json!({ "trade_id": "t5", "symbol": "AAPL", "qty": 25, "price": 192.0, "desk": "EQ" }),
    )
    .await
    .unwrap();

    let add6 = wait_for_delta(&mut sub6, Duration::from_secs(4), |d| {
        d.delta_type == DeltaKind::Add && s(&d.data, "trade_id").as_deref() == Some("t5")
    })
    .await;
    assert!(add6.is_some(), "c6 should receive a live Add for t5");

    let upd8 = wait_for_delta(&mut sub8, Duration::from_secs(4), |d| {
        matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update)
            && s(&d.data, "desk").as_deref() == Some("EQ")
            && i(&d.data, "total_qty") == Some(375) // 350 + 25
    })
    .await;
    assert!(upd8.is_some(), "c8 EQ group should update to 375");

    // delta_subscribe (c7) sees the live trade too.
    let live7 = wait_for_delta(&mut sub7, Duration::from_secs(4), |d| {
        s(&d.data, "trade_id").as_deref() == Some("t5")
    })
    .await;
    assert!(live7.is_some(), "c7 (delta_subscribe) should see live t5");

    // sow_delete a matching row → c12 sees a Remove.
    pubc.sow_delete("/trades", "t4").await.unwrap();
    let rem12 = wait_for_delta(&mut sub12, Duration::from_secs(4), |d| {
        d.delta_type == DeltaKind::Remove
    })
    .await;
    assert!(rem12.is_some(), "c12 should receive a Remove after sow_delete(t4)");

    // Conflation: batch 10 updates to one symbol → c13 gets the latest,
    // coalesced into far fewer than 10 deltas.
    let batch: Vec<Value> = (0..10)
        .map(|n| json!({ "symbol": "AAPL", "bid": 100.0 + n as f64, "ask": 101.0 + n as f64 }))
        .collect();
    pubc.publish_batch("/quotes", batch).await.unwrap();
    let mut q_deltas = 0usize;
    let mut last_bid = None;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(400), sub13.next_delta()).await {
            Ok(Some(d)) if d.delta_type != DeltaKind::SowSnapshot => {
                q_deltas += 1;
                last_bid = f(&d.data, "bid");
            }
            _ => break,
        }
    }
    assert!(q_deltas >= 1, "conflated sub should receive at least one quote delta");
    assert!(q_deltas < 10, "conflation should coalesce (<10 deltas for 10 updates), got {q_deltas}");
    assert_eq!(last_bid, Some(109.0), "conflation keeps the latest bid");

    // ── c11: historical as-of SOW — state as of the seed baseline ──
    // (after the live t5 + delete above, the live SOW differs from as-of.)
    let c11 = connect(&url).await;
    let asof = c11
        .sow_as_of_sequence("/trades", baseline_seq, None)
        .await
        .unwrap();
    assert_eq!(asof.len(), 4, "as-of baseline = the original 4 trades (no t5, t4 present)");
    assert!(
        asof.iter().any(|r| s(r, "trade_id").as_deref() == Some("t4")),
        "t4 existed at the baseline even though it was later deleted"
    );
    assert!(
        asof.iter().all(|r| s(r, "trade_id").as_deref() != Some("t5")),
        "t5 did not exist at the baseline"
    );
    // Live SOW now reflects the changes: t5 added, t4 removed → still 4 rows.
    let live_now = c11.sow("/trades", None).await.unwrap();
    assert_eq!(live_now.len(), 4);
    assert!(live_now.iter().any(|r| s(r, "trade_id").as_deref() == Some("t5")));
    assert!(live_now.iter().all(|r| s(r, "trade_id").as_deref() != Some("t4")));

    // ── c14: bookmark replay — resume strictly after baseline_seq ──
    // A fresh sub with bookmark = baseline_seq replays every txlog entry
    // newer than the baseline (t5 add + t4 delete) before going live.
    let c14 = connect(&url).await;
    let mut sub14 = c14
        .sow_and_subscribe("/trades", None, Some(baseline_seq))
        .await
        .unwrap();
    let mut replay_ids = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(4);
    while replay_ids.len() < 2 && Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(500), sub14.next_delta()).await
        {
            if let Some(id) = s(&d.data, "trade_id") {
                replay_ids.push((id, d.delta_type));
            } else if d.data.contains_key("_key") {
                // tombstone replay carries the key under `_key`.
                replay_ids.push((s(&d.data, "_key").unwrap_or_default(), d.delta_type));
            }
        }
    }
    assert!(
        replay_ids.iter().any(|(id, _)| id == "t5"),
        "bookmark replay should include the post-baseline add t5; got {replay_ids:?}"
    );
}

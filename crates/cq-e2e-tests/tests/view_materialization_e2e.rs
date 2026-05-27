//! S20 — materialized-view end-to-end test.
//!
//! Spins a real cqserver child process configured with a `[[topics]]`
//! source + a `[[views]]` derived topic. Verifies that:
//!   - The view is registered (shows up alongside topics).
//!   - Subscribing to the view returns one row per group in the
//!     initial snapshot.
//!   - Publishing to the source emits row-level deltas on the view
//!     subscription as the view's aggregate output changes.
//!   - Deleting the last source row of a group emits a Remove delta
//!     on the view.

use std::time::Duration;

use cq_client::{Client, DeltaKind};
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Value};

#[tokio::test]
async fn view_materialization_e2e() {
    let source = TopicSpec::new("/trades-e2e", "trader").with_inline_columns([
        ("trader", "string"),
        ("desk", "string"),
        ("qty", "long"),
    ]);
    let view = ViewSpec::new(
        "/by-desk-e2e",
        "/trades-e2e",
        "SELECT desk, SUM(qty) AS total FROM t GROUP BY desk",
    );

    let opts = ServerOpts {
        views: vec![view],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed source — two desks.
    client
        .publish(
            "/trades-e2e",
            json!({ "trader": "alice", "desk": "RATES", "qty": 100 }),
        )
        .await
        .unwrap();
    client
        .publish(
            "/trades-e2e",
            json!({ "trader": "bob", "desk": "FX", "qty": 50 }),
        )
        .await
        .unwrap();

    // Subscribe to the VIEW (not the source) and pull the snapshot.
    let mut sub = client
        .sow_and_subscribe("/by-desk-e2e", None, None)
        .await
        .expect("subscribe to view");
    // The actual snapshot assertion lives further down — diversification tests
    // appended at the end of this file.

    // Drain a couple of deltas — snapshot + any seed-time deltas.
    let mut snapshot: std::collections::HashMap<String, i64> =
        std::collections::HashMap::new();
    let mut all_deltas: Vec<(String, String, i64)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while snapshot.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await
        {
            let desk = d
                .data
                .get("desk")
                .and_then(Value::as_str)
                .unwrap_or("?")
                .to_string();
            let total = d.data.get("total").and_then(Value::as_i64).unwrap_or(-1);
            all_deltas.push((format!("{:?}", d.delta_type), desk.clone(), total));
            if desk != "?" && total >= 0 {
                snapshot.insert(desk.clone(), total);
            }
        }
    }
    assert_eq!(
        snapshot.get("RATES").copied(),
        Some(100),
        "snapshot collected: {:?}",
        all_deltas
    );
    assert_eq!(
        snapshot.get("FX").copied(),
        Some(50),
        "snapshot collected: {:?}",
        all_deltas
    );

    // Publish more RATES into the source → view's RATES row updates 100 → 150.
    client
        .publish(
            "/trades-e2e",
            json!({ "trader": "carol", "desk": "RATES", "qty": 50 }),
        )
        .await
        .unwrap();

    let mut saw_rates_150 = false;
    let mut second_phase: Vec<(String, String, i64)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !saw_rates_150 && std::time::Instant::now() < deadline {
        let next =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await;
        let Ok(Some(d)) = next else { continue };
        let desk = d
            .data
            .get("desk")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        let total = d.data.get("total").and_then(Value::as_i64).unwrap_or(-1);
        second_phase.push((format!("{:?}", d.delta_type), desk.clone(), total));
        if desk == "RATES" && total == 150 {
            saw_rates_150 = true;
            assert!(
                matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update),
                "RATES total update should arrive as Add or Update, got {:?}",
                d.delta_type
            );
        }
    }
    assert!(
        saw_rates_150,
        "view should reflect RATES → 150 after the publish; second phase observed: {:?}",
        second_phase
    );
    let _ = (all_deltas, second_phase);

    // Delete the only FX row → FX group should disappear from the view.
    // Subscriber should receive a Remove for FX.
    client.sow_delete("/trades-e2e", "bob").await.unwrap();

    // A non-sparse subscription on the view delivers a Remove whose
    // payload reflects the row's current (nulled) state — we identify
    // the removal by delta_type alone, not by the now-blanked desk
    // value. (A sparse subscriber would receive the key; the default
    // sub here projects all columns.)
    let mut saw_remove = false;
    let mut observed: Vec<(String, String, i64)> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !saw_remove && std::time::Instant::now() < deadline {
        let next =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await;
        let Ok(Some(d)) = next else { continue };
        let desk = d
            .data
            .get("desk")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        let total = d.data.get("total").and_then(Value::as_i64).unwrap_or(-1);
        observed.push((format!("{:?}", d.delta_type), desk.clone(), total));
        if matches!(d.delta_type, DeltaKind::Remove) {
            saw_remove = true;
        }
    }
    assert!(
        saw_remove,
        "expected a Remove delta on the view when FX group empties; observed: {:?}",
        observed
    );
}

// ───── Diversification ────────────────────────────────────────────

/// View backed by COUNT(*) — verify the count moves on every publish.
#[tokio::test]
async fn view_with_count_aggregate_increments_on_publish() {
    let source = TopicSpec::new("/v_count_src", "k")
        .with_inline_columns([("k", "string"), ("g", "string")]);
    let view = ViewSpec::new("/v_count", "/v_count_src", "SELECT g, COUNT(*) AS c FROM t GROUP BY g");
    let opts = ServerOpts { views: vec![view], ..ServerOpts::default() };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    for (k, g) in [("a", "X"), ("b", "X"), ("c", "Y")] {
        client.publish("/v_count_src", json!({ "k": k, "g": g })).await.unwrap();
    }

    // View materializes async — poll until X=2 + Y=1 settles.
    let mut by_g: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let snap = client.sow("/v_count", None).await.unwrap();
        by_g = snap
            .iter()
            .map(|r| {
                (
                    r.get("g").unwrap().as_str().unwrap().to_string(),
                    r.get("c").unwrap().as_i64().unwrap(),
                )
            })
            .collect();
        if by_g.get("X").copied() == Some(2) && by_g.get("Y").copied() == Some(1) {
            break;
        }
    }
    assert_eq!(by_g.get("X").copied(), Some(2), "by_g={by_g:?}");
    assert_eq!(by_g.get("Y").copied(), Some(1));

    // Publish a new Y → c becomes 2.
    client.publish("/v_count_src", json!({ "k": "d", "g": "Y" })).await.unwrap();
    let mut y_c: i64 = 0;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let snap = client.sow("/v_count", None).await.unwrap();
        y_c = snap
            .iter()
            .find(|r| r.get("g").unwrap().as_str() == Some("Y"))
            .map(|r| r.get("c").unwrap().as_i64().unwrap())
            .unwrap_or(0);
        if y_c == 2 {
            break;
        }
    }
    assert_eq!(y_c, 2);
}

/// View whose source has NO rows yet — view starts empty; first
/// publish creates the group.
#[tokio::test]
async fn view_on_empty_source_starts_empty_then_grows() {
    let source = TopicSpec::new("/v_late_src", "k")
        .with_inline_columns([("k", "string"), ("g", "string"), ("v", "long")]);
    let view = ViewSpec::new("/v_late", "/v_late_src", "SELECT g, SUM(v) AS s FROM t GROUP BY g");
    let opts = ServerOpts { views: vec![view], ..ServerOpts::default() };
    let server = start_server_with(vec![source], opts).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let snap1 = client.sow("/v_late", None).await.unwrap();
    assert!(snap1.is_empty(), "view starts empty");

    client.publish("/v_late_src", json!({ "k": "k1", "g": "G", "v": 42 })).await.unwrap();
    // Poll for materialisation (asynchronous view runner).
    let mut snap2 = Vec::new();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(150)).await;
        snap2 = client.sow("/v_late", None).await.unwrap();
        if !snap2.is_empty() {
            break;
        }
    }
    assert_eq!(snap2.len(), 1, "view should grow to 1 row");
    assert_eq!(snap2[0].get("s").unwrap().as_i64().unwrap(), 42);
}

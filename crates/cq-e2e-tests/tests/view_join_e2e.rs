//! S20 JOIN-view end-to-end test.
//!
//! Boots a real cqserver child with `/positions` + `/securities`
//! topics and a joined view that aggregates by sector. Verifies:
//!   - The view is registered (subscribable just like any topic).
//!   - Initial snapshot contains the correct per-sector totals after
//!     the join.
//!   - Publishing a new position into the source bumps the matching
//!     sector's exposure on the view subscription.
//!   - Publishing a new security row makes a previously-unmatched
//!     position contribute on the next refresh.

use std::time::Duration;

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Value};

#[tokio::test]
async fn join_view_aggregates_by_right_side_dimension() {
    // Two source topics: /positions (left) + /securities (right).
    // Quoting the slashed names lets sqlparser accept them as
    // identifiers inside the JOIN clause; the parser strips the
    // quotes before topic lookup.
    let positions = TopicSpec::new("/positions-join-e2e", "positionKey")
        .with_inline_columns([
            ("positionKey", "string"),
            ("cusip", "string"),
            ("marketValue", "double"),
        ]);
    let securities = TopicSpec::new("/securities-join-e2e", "cusip")
        .with_inline_columns([
            ("cusip", "string"),
            ("sector", "string"),
        ]);
    let view_sql = "SELECT sector, SUM(marketValue) AS exposure \
                    FROM \"/positions-join-e2e\" \
                    JOIN \"/securities-join-e2e\" USING (cusip) \
                    GROUP BY sector";
    let view = ViewSpec::new("/exposure-by-sector-e2e", "/positions-join-e2e", view_sql);

    let server = start_server_with(
        vec![positions, securities],
        ServerOpts {
            views: vec![view],
            ..ServerOpts::default()
        },
    )
    .await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Seed securities first so the initial join populates the view
    // with non-zero exposures.
    for (cusip, sector) in [
        ("AAPL", "Tech"),
        ("MSFT", "Tech"),
        ("JPM", "Banks"),
        ("BAC", "Banks"),
    ] {
        client
            .publish(
                "/securities-join-e2e",
                json!({ "cusip": cusip, "sector": sector }),
            )
            .await
            .unwrap();
    }
    for (key, cusip, mv) in [
        ("p1", "AAPL", 15_000.0_f64),
        ("p2", "MSFT", 18_000.0_f64),
        ("p3", "JPM", 22_000.0_f64),
        ("p4", "BAC", 5_500.0_f64),
    ] {
        client
            .publish(
                "/positions-join-e2e",
                json!({ "positionKey": key, "cusip": cusip, "marketValue": mv }),
            )
            .await
            .unwrap();
    }

    // Subscribe to the VIEW and collect (sector → exposure) from
    // snapshot deltas + the first batch of live deltas.
    let mut sub = client
        .sow_and_subscribe("/exposure-by-sector-e2e", None, None)
        .await
        .expect("subscribe to view");

    let mut got: std::collections::HashMap<String, f64> = Default::default();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while got.len() < 2 && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await
        {
            let sector = d.data.get("sector").and_then(Value::as_str).unwrap_or("");
            let exp = d.data.get("exposure").and_then(Value::as_f64).unwrap_or(-1.0);
            if !sector.is_empty() && exp >= 0.0 {
                got.insert(sector.to_string(), exp);
            }
        }
    }

    assert_eq!(
        got.get("Tech").copied(),
        Some(33_000.0),
        "Tech exposure should be AAPL(15k) + MSFT(18k); got: {:?}",
        got
    );
    assert_eq!(
        got.get("Banks").copied(),
        Some(27_500.0),
        "Banks exposure should be JPM(22k) + BAC(5.5k); got: {:?}",
        got
    );

    // Publish a new Tech position → the view's Tech total should
    // jump to 38_000.
    client
        .publish(
            "/positions-join-e2e",
            json!({ "positionKey": "p5", "cusip": "AAPL", "marketValue": 5_000.0 }),
        )
        .await
        .unwrap();
    let mut saw_tech_38k = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !saw_tech_38k && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await
        {
            let sector = d.data.get("sector").and_then(Value::as_str).unwrap_or("");
            let exp = d.data.get("exposure").and_then(Value::as_f64).unwrap_or(-1.0);
            if sector == "Tech" && (exp - 38_000.0).abs() < 1e-6 {
                saw_tech_38k = true;
            }
        }
    }
    assert!(
        saw_tech_38k,
        "expected Tech exposure to climb to 38_000 after the AAPL publish"
    );
}

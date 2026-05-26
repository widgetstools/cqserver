//! P6 e2e — `[[views]]`-declared JOIN view must serve a fresh
//! subscriber's one-shot SOW correctly.
//!
//! AMPS_PARITY §4 bug 1 — admin reports the view holds 3 rows but a
//! fresh subscriber's `sow()` returns 0. Non-JOIN views with the same
//! group key work. This test pins the JOIN-view one-shot SOW path so
//! the regression can't slip back in.

use std::time::Duration;

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::json;

#[tokio::test]
async fn join_view_one_shot_sow_returns_rows() {
    let positions = TopicSpec::new("/positions_sow", "positionKey")
        .with_inline_columns([
            ("positionKey", "string"),
            ("cusip", "string"),
            ("marketValue", "double"),
        ]);
    let securities = TopicSpec::new("/securities_sow", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    // NB: bare topic names (no leading slash) in the JOIN SQL —
    // this is what the Atlas demo writes in its `[[views]]` config
    // (`FROM trades JOIN positions USING (...)`). The actual topics
    // carry a slash prefix; the slash-prefix asymmetry is what
    // AMPS_PARITY §4 bug 5 documents as the root cause of bug 1.
    let view_sql = "SELECT sector, SUM(marketValue) AS exposure \
                    FROM positions_sow \
                    JOIN securities_sow USING (cusip) \
                    GROUP BY sector";
    let view = ViewSpec::new("/exposure_by_sector_sow", "/positions_sow", view_sql);
    let server = start_server_with(
        vec![positions, securities],
        ServerOpts {
            views: vec![view],
            ..ServerOpts::default()
        },
    )
    .await;

    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");
    for (cusip, sector) in [
        ("AAPL", "Tech"),
        ("MSFT", "Tech"),
        ("JPM", "Banks"),
    ] {
        publisher
            .publish(
                "/securities_sow",
                json!({ "cusip": cusip, "sector": sector }),
            )
            .await
            .unwrap();
    }
    for (key, cusip, mv) in [
        ("p1", "AAPL", 10_000.0_f64),
        ("p2", "MSFT", 20_000.0_f64),
        ("p3", "JPM", 30_000.0_f64),
    ] {
        publisher
            .publish(
                "/positions_sow",
                json!({ "positionKey": key, "cusip": cusip, "marketValue": mv }),
            )
            .await
            .unwrap();
    }

    // Give the view runner time to materialise the snapshot.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Fresh connection — first thing it does is a one-shot SOW.
    let fresh = Client::connect(&server.tcp_url()).await.expect("fresh client");
    let snap = fresh
        .sow("/exposure_by_sector_sow", None)
        .await
        .expect("fresh subscriber sow");
    assert!(
        !snap.is_empty(),
        "fresh subscriber's SOW on a JOIN view must return rows; got 0 (AMPS_PARITY §4 bug 1)"
    );
    // Sanity — expect 2 sector rows (Tech + Banks).
    assert_eq!(snap.len(), 2, "expected 2 sectors, got {snap:?}");
}

#[tokio::test]
async fn join_view_sow_repeated_against_continuous_load() {
    // Stress variant: publishers continue ticking while a stream of
    // fresh subscribers each do a one-shot SOW. AMPS_PARITY §4 bug 1
    // hinted at a race in the JOIN-view SOW path under continuous
    // re-aggregation. This pins the regression.
    let positions = TopicSpec::new("/positions_race", "positionKey")
        .with_inline_columns([
            ("positionKey", "string"),
            ("cusip", "string"),
            ("marketValue", "double"),
        ]);
    let securities = TopicSpec::new("/securities_race", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let view_sql = "SELECT sector, SUM(marketValue) AS exposure \
                    FROM positions_race \
                    JOIN securities_race USING (cusip) \
                    GROUP BY sector";
    let view = ViewSpec::new("/exposure_race", "/positions_race", view_sql);
    let server = start_server_with(
        vec![positions, securities],
        ServerOpts {
            views: vec![view],
            ..ServerOpts::default()
        },
    )
    .await;
    let publisher = Client::connect(&server.tcp_url()).await.expect("publisher");
    for (c, s) in [("AAPL", "Tech"), ("JPM", "Banks")] {
        publisher
            .publish("/securities_race", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [("p1", "AAPL", 10_000.0), ("p2", "JPM", 20_000.0)] {
        publisher
            .publish(
                "/positions_race",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv as f64 }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Hammer the SOW endpoint while the publisher keeps ticking.
    let pub_handle = {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            for i in 0..20_i64 {
                let _ = publisher
                    .publish(
                        "/positions_race",
                        json!({
                            "positionKey": format!("p{i}_tick"),
                            "cusip": "AAPL",
                            "marketValue": (1000.0 + i as f64),
                        }),
                    )
                    .await;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
    };

    for _ in 0..10 {
        let fresh = Client::connect(&server.tcp_url()).await.expect("fresh");
        let snap = fresh
            .sow("/exposure_race", None)
            .await
            .expect("fresh sow");
        assert!(
            !snap.is_empty(),
            "fresh subscriber's SOW returned 0 rows under continuous load"
        );
    }
    let _ = pub_handle.await;
}

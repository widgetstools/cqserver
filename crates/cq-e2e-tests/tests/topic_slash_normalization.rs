//! P14 e2e — `[[views]]` config with bare-name JOIN SQL must resolve
//! against the slash-prefixed registry without the historical
//! dual-lookup workaround.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec, ViewSpec};
use serde_json::{json, Value};
use std::time::Duration;

#[tokio::test]
async fn view_bare_name_join_resolves_slash_prefixed_registry() {
    // Topics register with slash prefix; the JOIN SQL deliberately
    // uses bare names — the resolver must canonicalise both forms
    // to one registry key.
    let positions = TopicSpec::new("/p14_pos", "positionKey").with_inline_columns([
        ("positionKey", "string"),
        ("cusip", "string"),
        ("marketValue", "double"),
    ]);
    let securities = TopicSpec::new("/p14_sec", "cusip").with_inline_columns([
        ("cusip", "string"),
        ("sector", "string"),
    ]);
    let view_sql = "SELECT sector, SUM(marketValue) AS exposure \
                    FROM p14_pos JOIN p14_sec USING (cusip) \
                    GROUP BY sector";
    let view = ViewSpec::new("/p14_view", "/p14_pos", view_sql);

    let server = start_server_with(
        vec![positions, securities],
        ServerOpts {
            views: vec![view],
            ..ServerOpts::default()
        },
    )
    .await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    for (c, s) in [("AAPL", "Tech"), ("JPM", "Banks")] {
        client
            .publish("/p14_sec", json!({ "cusip": c, "sector": s }))
            .await
            .unwrap();
    }
    for (k, c, mv) in [("p1", "AAPL", 1_000.0_f64), ("p2", "JPM", 2_000.0)] {
        client
            .publish(
                "/p14_pos",
                json!({ "positionKey": k, "cusip": c, "marketValue": mv }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let snap = client
        .sow("/p14_view", None)
        .await
        .expect("view sow");
    assert_eq!(snap.len(), 2, "expected 2 sector rows, got {snap:?}");
    let by_sector: std::collections::HashMap<String, f64> = snap
        .into_iter()
        .map(|r| {
            (
                r.get("sector").unwrap().as_str().unwrap().to_string(),
                r.get("exposure").unwrap().as_f64().unwrap(),
            )
        })
        .collect();
    assert!((by_sector["Tech"] - 1_000.0).abs() < 1e-9);
    assert!((by_sector["Banks"] - 2_000.0).abs() < 1e-9);

    // Also exercise the JOIN SOW path with bare names (router's
    // peek_join → canonical right-topic lookup).
    let inline = client
        .sow_sql(
            "/p14_pos",
            "SELECT sector, SUM(marketValue) AS exposure \
             FROM p14_pos JOIN p14_sec USING (cusip) \
             GROUP BY sector",
        )
        .await
        .expect("inline JOIN sow");
    assert_eq!(inline.len(), 2);
    let _ = Value::Null; // silence unused
}

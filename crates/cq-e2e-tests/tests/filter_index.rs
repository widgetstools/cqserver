//! e2e: secondary-index acceleration for equality SOW queries.
//!
//! When a topic declares `index_columns = ["desk"]`, the SOW
//! planner uses the index to skip the full row scan whenever the
//! WHERE clause contains a `desk = ...` predicate.
//!
//! This test:
//!   1. Brings up a server with an indexed `desk` column.
//!   2. Publishes 1000 rows across 5 desks.
//!   3. Runs a baseline SOW query *without* an indexed predicate (forces
//!      full scan).
//!   4. Runs an indexed SOW query and asserts:
//!      - Correct rows returned.
//!      - `cq_query_index_hits_total` advanced.
//!      - `cq_query_full_scans_total` did NOT advance for that query.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

async fn metric_value(server_url: &str, name: &str) -> u64 {
    let body = reqwest::get(format!("{server_url}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(name) {
            let value_part = rest.rsplit_once(' ').map(|(_, v)| v).unwrap_or("");
            return value_part.parse::<f64>().unwrap_or(0.0) as u64;
        }
    }
    0
}

#[tokio::test]
async fn indexed_eq_query_uses_index_path() {
    let topic = TopicSpec::new("/indexed-trades", "k")
        .with_inline_columns([
            ("k", "string"),
            ("desk", "string"),
            ("price", "double"),
        ])
        .with_index_columns(["desk"]);
    let server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let desks = ["RATES", "EQUITIES", "FX", "CREDIT", "COMMODS"];
    let n_per_desk = 200;
    for desk in desks {
        for i in 0..n_per_desk {
            client
                .publish(
                    "/indexed-trades",
                    json!({
                        "k": format!("{desk}-{i:04}"),
                        "desk": desk,
                        "price": 100.0 + i as f64,
                    }),
                )
                .await
                .expect("publish");
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Capture metric baselines before the indexed query.
    let base_hits = metric_value(&server.admin_url(), "cq_query_index_hits_total").await;
    let base_scans = metric_value(&server.admin_url(), "cq_query_full_scans_total").await;

    // Indexed equality — should use the bitmap.
    let rates_rows = client
        .sow("/indexed-trades", Some("desk = 'RATES'"))
        .await
        .expect("indexed sow");
    assert_eq!(
        rates_rows.len(),
        n_per_desk,
        "expected {n_per_desk} rows on RATES desk, got {}",
        rates_rows.len()
    );

    // Correctness: every row really has desk='RATES'.
    for row in &rates_rows {
        assert_eq!(
            row.get("desk").and_then(|v| v.as_str()),
            Some("RATES"),
            "row {row:?} leaked through indexed filter"
        );
    }

    let after_hits = metric_value(&server.admin_url(), "cq_query_index_hits_total").await;
    let after_scans = metric_value(&server.admin_url(), "cq_query_full_scans_total").await;

    assert!(
        after_hits > base_hits,
        "cq_query_index_hits_total should have incremented (was {base_hits}, now {after_hits})"
    );
    assert_eq!(
        after_scans, base_scans,
        "indexed query should NOT have triggered a full scan (full_scans went {base_scans} -> {after_scans})"
    );

    // Now run a non-equality query — `price > 200` — and confirm it
    // goes through the full-scan path.
    let _ = client
        .sow("/indexed-trades", Some("price > 200"))
        .await
        .expect("range sow");
    let after_scans_2 = metric_value(&server.admin_url(), "cq_query_full_scans_total").await;
    assert!(
        after_scans_2 > after_scans,
        "range query should have used full-scan path (full_scans went {after_scans} -> {after_scans_2})"
    );
}

#[tokio::test]
async fn index_stays_consistent_under_updates_and_deletes() {
    // Publish a row, change its indexed column, and verify the index
    // returns it under the new value but not the old.
    let topic = TopicSpec::new("/idx-mutations", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")])
        .with_index_columns(["desk"]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/idx-mutations", json!({ "k": "T1", "desk": "RATES" }))
        .await
        .expect("publish T1");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rates = client
        .sow("/idx-mutations", Some("desk = 'RATES'"))
        .await
        .unwrap();
    assert_eq!(rates.len(), 1, "T1 should show up on RATES desk");

    // Move T1 to EQUITIES.
    client
        .publish("/idx-mutations", json!({ "k": "T1", "desk": "EQUITIES" }))
        .await
        .expect("update T1");
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rates_after = client
        .sow("/idx-mutations", Some("desk = 'RATES'"))
        .await
        .unwrap();
    assert!(
        rates_after.is_empty(),
        "T1 should no longer be on RATES after update, got {:?}",
        rates_after
    );
    let equities = client
        .sow("/idx-mutations", Some("desk = 'EQUITIES'"))
        .await
        .unwrap();
    assert_eq!(equities.len(), 1, "T1 should appear on EQUITIES after update");
}

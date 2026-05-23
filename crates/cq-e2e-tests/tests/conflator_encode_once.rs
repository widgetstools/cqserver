//! e2e: encode-once-fan-out reaches the conflator flush path.
//!
//! Variant of `encode_once_fanout.rs` for **conflated** topics. The
//! evaluator pre-encodes each delta's body and stamps it onto the
//! `Delta` it submits; the per-sub flush loop must reuse those bytes
//! rather than re-serializing per subscriber.
//!
//! With N subs on one conflated topic and K distinct-row publishes:
//!   - `cq_conflator_body_reuses_total` rises by ~ N * K
//!   - `cq_conflator_body_encodes_total` stays near 0 (only fallback
//!     path increments it — never reached when the evaluator
//!     pre-encodes, which is the production case)

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
async fn conflator_flush_reuses_pre_encoded_body() {
    // Topic with a tight conflation window so flushes fire quickly
    // during the test.
    let schema = json!({ "k": "string", "v": "double" });
    let topic = TopicSpec::new("/conf-share", "k")
        .with_schema(schema)
        .with_conflation(20);
    let server = start_server(vec![topic]).await;

    let n_subs: usize = 4;
    let n_rows: usize = 25;

    // Open N subs. All non-projecting so they share the row_data Arc
    // and (via the body cache) the encoded body bytes.
    let mut sub_clients = Vec::new();
    for _ in 0..n_subs {
        let c = Client::connect(&server.tcp_url()).await.expect("sub connect");
        let sub = c.subscribe("/conf-share", None).await.expect("subscribe");
        sub_clients.push((c, sub));
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    let base_reuses =
        metric_value(&server.admin_url(), "cq_conflator_body_reuses_total").await;
    let base_encodes =
        metric_value(&server.admin_url(), "cq_conflator_body_encodes_total").await;

    let pub_client = Client::connect(&server.tcp_url()).await.expect("pub connect");
    for i in 0..n_rows {
        pub_client
            .publish(
                "/conf-share",
                json!({ "k": format!("k{i:04}"), "v": i as f64 }),
            )
            .await
            .expect("publish");
    }

    // Conflation interval is 20ms — give multiple flush windows so
    // every pending row drains to its subscriber.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let new_reuses = metric_value(&server.admin_url(), "cq_conflator_body_reuses_total")
        .await
        .saturating_sub(base_reuses);
    let new_encodes = metric_value(&server.admin_url(), "cq_conflator_body_encodes_total")
        .await
        .saturating_sub(base_encodes);

    // Each publish produces one delta per sub; each delta's body was
    // pre-encoded by the evaluator, so the flush takes the reuse path.
    // Lower bound: n_rows × n_subs reuses — but in practice multiple
    // deltas may have coalesced within a single flush window, so we
    // assert "at least n_rows" reuses (one per row, one sub minimum).
    assert!(
        new_reuses >= n_rows as u64,
        "expected ≥{n_rows} conflator body reuses, got {new_reuses}"
    );

    // The fallback `encodes` path is only hit when a delta arrives
    // without `encoded_body_json` — should never happen in production
    // for JSON-codec subs going through the evaluator. Allow zero.
    assert_eq!(
        new_encodes, 0,
        "conflator fallback-encode path should not fire under normal evaluator flow, got {new_encodes}"
    );

    // Sanity: subs actually received frames.
    for (_c, mut sub) in sub_clients {
        let mut seen = 0;
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
                Ok(Some(_)) => seen += 1,
                _ => break,
            }
        }
        assert!(seen > 0, "conflated subscription received nothing");
    }
}

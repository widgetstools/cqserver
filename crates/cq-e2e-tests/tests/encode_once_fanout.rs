//! e2e: encode-once-fan-out delivery path.
//!
//! With many subscribers to the same topic, each delta event should
//! cause the row body to be serialized exactly once per evaluator pass
//! and the resulting bytes fanned out to every subscriber's queue.
//!
//! We verify this by counting `cq_delta_body_encodes_total` and
//! `cq_delta_body_reuses_total` in the Prometheus metrics endpoint:
//!
//!   - `encodes` should be roughly the number of distinct delta events.
//!   - `reuses` should be roughly `(num_subs - 1) * events`.
//!
//! N subscribers + 1 publish = 1 encode + (N-1) reuses.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::{json, Value};
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
            // Lines look like:
            //   cq_delta_body_encodes_total 42
            //   cq_delta_body_encodes_total{label="x"} 42
            let value_part = rest.rsplit_once(' ').map(|(_, v)| v).unwrap_or("");
            return value_part.parse::<f64>().unwrap_or(0.0) as u64;
        }
    }
    0
}

#[tokio::test]
async fn n_subs_reuse_encoded_body_for_shared_deltas() {
    // Topic with NO conflation — the fast path applies. Conflated
    // topics still encode per-flush.
    let schema = json!({ "k": "string", "v": "double" });
    let topic = TopicSpec::new("/shared", "k").with_schema(schema);
    let server = start_server(vec![topic]).await;

    // Open N subscribers (no projection — all should share the same
    // row body Arc), then publish K rows. Each publish fires one event,
    // delivered to all N subs. Expected:
    //   encodes ≈ K
    //   reuses  ≈ (N-1) * K
    let n_subs: usize = 5;
    let n_rows: usize = 30;

    let mut sub_clients = Vec::new();
    for _ in 0..n_subs {
        let c = Client::connect(&server.tcp_url()).await.expect("sub connect");
        let _sub = c.subscribe("/shared", None).await.expect("subscribe");
        sub_clients.push((c, _sub));
    }
    // Let subscriptions settle.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Baseline metric reads (some Add deltas may have fired during
    // subscribe seeding for pre-existing rows; we measure incremental
    // movement from the publish phase).
    let base_encodes = metric_value(&server.admin_url(), "cq_delta_body_encodes_total").await;
    let base_reuses = metric_value(&server.admin_url(), "cq_delta_body_reuses_total").await;

    // Publish K distinct rows.
    let pub_client = Client::connect(&server.tcp_url()).await.expect("pub connect");
    for i in 0..n_rows {
        pub_client
            .publish("/shared", json!({ "k": format!("k{i:04}"), "v": i as f64 }))
            .await
            .expect("publish");
    }
    // Give the evaluator a beat to process all mutations + emit deltas.
    tokio::time::sleep(Duration::from_millis(800)).await;

    let encodes = metric_value(&server.admin_url(), "cq_delta_body_encodes_total").await;
    let reuses = metric_value(&server.admin_url(), "cq_delta_body_reuses_total").await;

    let new_encodes = encodes.saturating_sub(base_encodes);
    let new_reuses = reuses.saturating_sub(base_reuses);

    // Encodes should be close to n_rows. Allow some slack: the
    // subscriptions themselves may have fired Add deltas, and the
    // evaluator may evaluate non-matching events too — but every
    // publish must produce at least one body encode.
    assert!(
        new_encodes >= n_rows as u64,
        "expected ≥{n_rows} body encodes after {n_rows} publishes, got {new_encodes}"
    );
    // Each publish fans out to n_subs; one encode + (n_subs - 1) reuses.
    let expected_reuses_min = (n_subs as u64 - 1) * n_rows as u64;
    assert!(
        new_reuses >= expected_reuses_min,
        "expected ≥{expected_reuses_min} reuses ({} subs × {} publishes - 1 per event), got {new_reuses}",
        n_subs,
        n_rows
    );
    // Sanity check on the ratio — reuses should be (n_subs - 1)
    // times encodes, roughly. If reuses are much lower than encodes,
    // the cache isn't actually being shared.
    assert!(
        new_reuses >= new_encodes,
        "expected reuses ≥ encodes (subs > 1), got encodes={new_encodes} reuses={new_reuses}"
    );

    // Sanity: each subscription still receives deltas (the bytes the
    // server fans out are actually wired correctly to every sub).
    for (_c, mut sub) in sub_clients {
        let mut seen = 0;
        let deadline = std::time::Instant::now() + Duration::from_millis(500);
        while std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await {
                Ok(Some(_)) => seen += 1,
                _ => break,
            }
        }
        assert!(seen > 0, "subscription received nothing after publishes");
    }

    // Touch a value to silence unused warnings on test types.
    let _: Value = json!({});
}

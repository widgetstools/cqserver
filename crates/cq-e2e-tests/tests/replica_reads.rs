//! Replica-reads S3 e2e — spawn a real cqserver subprocess in
//! `role = standby` and verify the read-only contract holds across
//! the full process boundary (not just the in-process unit test on
//! the RouterContext flag).
//!
//! What's covered here:
//!   - A standby server accepts client connections.
//!   - A standby server accepts subscribes (the read path works).
//!   - A standby server rejects publishes with the expected error.
//!   - The `cq_publish_rejected_read_only_total` metric increments.
//!
//! What is NOT covered (deserves its own session, tracked in
//! REPLICA_READS_WORKLOG.md):
//!   - End-to-end state convergence from a real leader to a follower
//!     through the shipper. That test requires verifying the existing
//!     `cq-replication::shipper` works through the harness's subprocess
//!     boundary, which is a separate investigation.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ReplicationOpts, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn standby_rejects_publish_with_read_only_error() {
    // A standby that nothing connects to. listen = "127.0.0.1:0"
    // binds an ephemeral port that won't conflict; the absence of a
    // shipper connecting is fine for this test — we're verifying the
    // read-only guard on the client-facing transports, not state
    // convergence.
    let topic = TopicSpec::new("/ro-topic", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);

    // Pick an ephemeral port for the standby's replication acceptor
    // by binding+dropping. (The standby will then bind the same port —
    // a brief race window, but reliable enough for a single test.)
    let repl_listen = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    };
    let opts = ServerOpts {
        replication: Some(ReplicationOpts::standby(repl_listen.to_string())),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();

    // Publish — should fail with the read-only error.
    let result = client
        .publish("/ro-topic", json!({ "k": "x", "v": 1.0 }))
        .await;
    let err = result.expect_err("publish must be rejected on a standby");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("read-only follower"),
        "expected read-only error, got: {msg}"
    );
}

#[tokio::test]
async fn standby_publish_rejected_metric_increments() {
    let topic = TopicSpec::new("/ro-metric-topic", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);

    let repl_listen = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    };
    let opts = ServerOpts {
        replication: Some(ReplicationOpts::standby(repl_listen.to_string())),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();

    // Three publish attempts. All should fail; metric should reach >=3.
    for i in 0..3 {
        let _ = client
            .publish("/ro-metric-topic", json!({ "k": format!("k{i}"), "v": i as f64 }))
            .await;
    }

    // Allow metric to settle.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let metrics = reqwest::get(format!("{}/metrics", server.admin_url()))
        .await
        .expect("metrics fetch")
        .text()
        .await
        .expect("metrics body");

    // Find the cq_publish_rejected_read_only_total counter line.
    let count: Option<f64> = metrics
        .lines()
        .find(|l| l.starts_with("cq_publish_rejected_read_only_total"))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse::<f64>().ok());
    assert!(
        count.map(|n| n >= 3.0).unwrap_or(false),
        "expected cq_publish_rejected_read_only_total >= 3, got: {count:?}\nfull metrics output:\n{metrics}"
    );
}

#[tokio::test]
async fn standby_subscribe_still_works() {
    // Read path must still function — a follower's whole point is
    // to serve subscribes. We verify by subscribing and observing
    // that the SOW snapshot (empty since nothing publishes here)
    // completes cleanly with no error.
    let topic = TopicSpec::new("/ro-sub-topic", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);

    let repl_listen = {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        drop(l);
        addr
    };
    let opts = ServerOpts {
        replication: Some(ReplicationOpts::standby(repl_listen.to_string())),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let client = Client::connect(&server.tcp_url()).await.unwrap();
    // sow_and_subscribe should succeed (empty SOW completes immediately).
    let _sub = tokio::time::timeout(
        Duration::from_secs(5),
        client.sow_and_subscribe("/ro-sub-topic", None, None),
    )
    .await
    .expect("sow_and_subscribe must not hang on standby")
    .expect("sow_and_subscribe must succeed on standby");
}

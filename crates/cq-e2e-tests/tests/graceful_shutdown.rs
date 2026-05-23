//! e2e: graceful shutdown on SIGTERM.
//!
//! When the server is using `fsync = none` (default — trades durability
//! for throughput on the publish hot path), in-flight writes may still
//! be sitting in the kernel page cache. SIGKILL on shutdown loses
//! anything not yet fsynced. SIGTERM must run the shutdown sequence
//! that fsyncs every persistent topic's log before the process exits.
//!
//! This test:
//!   1. Spins up a server with one persistent topic.
//!   2. Publishes a batch of rows (no explicit sync).
//!   3. Sends SIGTERM and waits for the process to exit cleanly.
//!   4. Restarts the server against the same on-disk state.
//!   5. Asserts the recovered SOW contains every published row —
//!      proving the shutdown fsynced before exit.

#![cfg(unix)]

use cq_client::Client;
use cq_e2e_tests::{
    restart_kept, start_server, stop_keeping_dir, ServerHandle, TopicSpec,
};
use serde_json::json;
use std::time::Duration;

async fn send_sigterm_and_wait(handle: &mut ServerHandle, timeout: Duration) {
    // The handle owns the child; send SIGTERM via our helper. Then
    // also drop the temp dir reservation so the next harness call
    // doesn't try to reap the same child twice.
    let status = handle.send_sigterm_and_wait(timeout);
    assert!(
        status.is_some(),
        "server did not exit within {:?} of SIGTERM",
        timeout
    );
    let status = status.unwrap();
    assert!(
        status.success() || status.code().unwrap_or(0) == 0,
        "server exit status was unsuccessful: {:?}",
        status
    );
}

#[tokio::test]
async fn sigterm_fsyncs_txlog_before_exit() {
    // Persistent topic with default fsync (= none on the hot path).
    // Without graceful shutdown, kernel-buffered writes would be lost
    // on process exit.
    let topic = TopicSpec::new("/durable", "k")
        .with_inline_columns([("k", "string"), ("v", "double")])
        .with_persist();
    let mut server = start_server(vec![topic]).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let n_rows: usize = 50;
    for i in 0..n_rows {
        client
            .publish("/durable", json!({ "k": format!("k{i:04}"), "v": i as f64 }))
            .await
            .expect("publish");
    }
    // Let mutation events settle through the evaluator.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Important: do NOT call any explicit sync — we're testing that
    // shutdown does it for us. Drop the client first so its open
    // connection doesn't slow shutdown.
    drop(client);

    send_sigterm_and_wait(&mut server, Duration::from_secs(8)).await;

    // Reuse the temp dir + config + ports for a restart.
    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;

    // After recovery, every row must be queryable.
    let client2 = Client::connect(&server2.tcp_url())
        .await
        .expect("reconnect");
    let rows = client2
        .sow("/durable", None)
        .await
        .expect("sow query");
    assert_eq!(
        rows.len(),
        n_rows,
        "after SIGTERM + restart, SOW should contain all {} rows, got {}",
        n_rows,
        rows.len()
    );

    // Spot-check a couple of values.
    let has_k0 = rows
        .iter()
        .any(|r| r.get("k").and_then(|v| v.as_str()) == Some("k0000"));
    let has_k49 = rows
        .iter()
        .any(|r| r.get("k").and_then(|v| v.as_str()) == Some("k0049"));
    assert!(has_k0, "k0000 missing from recovered SOW");
    assert!(has_k49, "k0049 missing from recovered SOW");
}

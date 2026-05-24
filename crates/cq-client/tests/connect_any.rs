//! Replica-reads S2 — initial-connect failover (`Client::connect_any`).
//!
//! These tests verify the "try each URL in random order, return the
//! first one that connects" behaviour. They use raw `TcpListener`
//! stubs (not the full cqserver router) because we only care about
//! whether the *transport-level connect* lands on a live socket —
//! the wire protocol is exercised exhaustively elsewhere.

use cq_client::Client;
use std::time::Duration;
use tokio::net::TcpListener;

/// All-dead URL list: connect_any returns an error (not a panic, not a
/// hang). The specific error is the last attempt's failure.
#[tokio::test]
async fn connect_any_all_dead_returns_error() {
    // Bind+drop to find ports that won't accept.
    let dead1 = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let dead2 = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let url1 = format!("tcp://{}", dead1);
    let url2 = format!("tcp://{}", dead2);

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        Client::connect_any(&[&url1, &url2]),
    )
    .await
    .expect("connect_any must not hang");
    assert!(
        result.is_err(),
        "connect_any against all-dead URLs should error"
    );
}

/// One live + one dead: connect_any lands on the live one regardless
/// of which order it tries first. Since the order is randomized, we
/// just assert "it succeeds" — the test exercises both orderings
/// across enough runs to catch a bug in either path.
#[tokio::test]
async fn connect_any_skips_dead_url() {
    // Live listener that just accepts and holds the socket open
    // (no protocol — connect_any only cares about the TCP-level
    // accept).
    let live_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_addr = live_listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = live_listener.accept().await {
            // Just hold the connection. The client driver will
            // proceed; we don't need to speak the protocol here
            // because connect_any only verifies the transport
            // connect succeeded.
            tokio::spawn(async move {
                let _hold = stream;
                tokio::time::sleep(Duration::from_secs(5)).await;
            });
        }
    });

    let dead_addr = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap()
    };
    let live = format!("tcp://{}", live_addr);
    let dead = format!("tcp://{}", dead_addr);

    // Run several times. With random ordering, sometimes the dead URL
    // is tried first (and skipped); sometimes the live one is. Every
    // call should still succeed.
    for i in 0..6 {
        let client = tokio::time::timeout(
            Duration::from_secs(2),
            Client::connect_any(&[&live, &dead]),
        )
        .await
        .unwrap_or_else(|_| panic!("connect_any iteration {i} timed out"));
        assert!(
            client.is_ok(),
            "connect_any iteration {i} failed: {:?}",
            client.err()
        );
        // Drop the client between iterations so the live listener
        // accepts a fresh one.
        drop(client);
        // Small spin to vary the time-based shuffle seed.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Empty URL list returns InvalidUrl rather than panicking.
#[tokio::test]
async fn connect_any_empty_list_errors() {
    let r = Client::connect_any(&[]).await;
    assert!(r.is_err(), "empty list should error");
}

//! D3/P0.3 — `[transport.limits]` end-to-end tests.
//!
//! Spins up a real cqserver child with tiny limit knobs (so the tests
//! run fast) and verifies, against a live TCP transport:
//!   (a) `max_connections` — a 5th connection is refused once 4 are
//!       held open; closing one frees a slot for a retry.
//!   (b) `max_connections_per_ip` — a 3rd connection from the same
//!       loopback address is refused once 2 are held open.
//!   (c) `max_sessions_per_user` — a 2nd concurrent Logon as the same
//!       user is rejected with an error ack.
//! Each scenario also asserts `cq_connections_rejected_total{reason=…}`
//! incremented on `/metrics`.

use cq_client::{Client, ClientConfig};
use cq_e2e_tests::{
    start_server_with, AuthOpts, ServerOpts, TopicSpec, TransportLimitsOpts, UserSpec,
};
use std::time::Duration;

async fn metrics_text(server: &cq_e2e_tests::ServerHandle) -> String {
    let url = format!("{}/metrics", server.admin_url());
    reqwest::get(&url).await.unwrap().text().await.unwrap()
}

/// Counter value parsed from `/metrics` text, summing every labelled
/// series that starts with `name{` plus an unlabelled `name ` row.
/// Mirrors the helper in `spillover_e2e.rs`.
fn counter_value(metrics: &str, name: &str) -> u64 {
    let mut total: u64 = 0;
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        let prefix_labelled = format!("{}{{", name);
        let prefix_plain = format!("{} ", name);
        let (matches_prefix, after) = if line.starts_with(&prefix_labelled) {
            let after = line.split_once('}').map(|(_, rest)| rest).unwrap_or("");
            (true, after)
        } else if let Some(after) = line.strip_prefix(&prefix_plain) {
            (true, after)
        } else {
            (false, "")
        };
        if matches_prefix {
            let token = after.split_whitespace().last().unwrap_or("0");
            if let Ok(v) = token.parse::<f64>() {
                total += v as u64;
            }
        }
    }
    total
}

/// Same as `counter_value` but restricted to lines carrying the given
/// `reason="..."` label, so we can distinguish which limit tripped.
fn counter_value_with_reason(metrics: &str, name: &str, reason: &str) -> u64 {
    let mut total: u64 = 0;
    let needle = format!(r#"reason="{reason}""#);
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(&format!("{name}{{")) && line.contains(&needle) {
            if let Some(v) = line
                .split_whitespace()
                .last()
                .and_then(|t| t.parse::<f64>().ok())
            {
                total += v as u64;
            }
        }
    }
    total
}

/// Open a raw TCP connection and try to complete an anonymous
/// version-negotiation handshake within `timeout`. Returns `true` if
/// the handshake completed (connection admitted), `false` if it errored
/// or timed out (connection rejected/closed by the server before the
/// handshake could finish).
async fn try_handshake(url: &str, timeout: Duration) -> Option<Client> {
    let client = match Client::connect_with(url, ClientConfig::default()).await {
        Ok(c) => c,
        Err(_) => return None,
    };
    match tokio::time::timeout(timeout, client.handshake_protocol()).await {
        Ok(Ok(_)) => Some(client),
        _ => None,
    }
}

#[tokio::test]
async fn max_connections_refuses_the_next_connection_then_admits_after_one_closes() {
    let topic = TopicSpec::new("/conn-cap", "k").with_inline_columns([("k", "string")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            transport_limits: Some(TransportLimitsOpts {
                max_connections: Some(4),
                ..Default::default()
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let short = Duration::from_secs(3);

    // Fill the cap with 4 live, handshaked connections.
    let mut held = Vec::new();
    for i in 0..4 {
        let c = try_handshake(&server.tcp_url(), short)
            .await
            .unwrap_or_else(|| panic!("connection {i} within cap should be admitted"));
        held.push(c);
    }

    // The 5th connection must be refused.
    let fifth = try_handshake(&server.tcp_url(), short).await;
    assert!(
        fifth.is_none(),
        "5th connection should have been refused (cap=4)"
    );

    // The rejection counter must reflect it.
    let m = metrics_text(&server).await;
    let rejected =
        counter_value_with_reason(&m, "cq_connections_rejected_total", "max_connections");
    assert!(
        rejected >= 1,
        "expected cq_connections_rejected_total{{reason=\"max_connections\"}} >= 1, got {rejected}\n{m}"
    );

    // Close one held connection, freeing a slot; a retry must now succeed.
    let closed = held.pop().expect("at least one held connection");
    drop(closed);
    // Give the server a moment to notice the socket close and release
    // the permit.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let retry = try_handshake(&server.tcp_url(), short).await;
    assert!(
        retry.is_some(),
        "retry after closing one connection should succeed"
    );
}

#[tokio::test]
async fn max_connections_per_ip_refuses_the_third_loopback_connection() {
    let topic = TopicSpec::new("/conn-ip-cap", "k").with_inline_columns([("k", "string")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            transport_limits: Some(TransportLimitsOpts {
                // Generous global cap so only the per-IP cap can trip.
                max_connections: Some(100),
                max_connections_per_ip: Some(2),
                ..Default::default()
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let short = Duration::from_secs(3);

    let _c1 = try_handshake(&server.tcp_url(), short)
        .await
        .expect("1st loopback connection admitted");
    let _c2 = try_handshake(&server.tcp_url(), short)
        .await
        .expect("2nd loopback connection admitted");

    let c3 = try_handshake(&server.tcp_url(), short).await;
    assert!(
        c3.is_none(),
        "3rd connection from the same IP should be refused (per-ip cap=2)"
    );

    let m = metrics_text(&server).await;
    let rejected = counter_value_with_reason(
        &m,
        "cq_connections_rejected_total",
        "max_connections_per_ip",
    );
    assert!(
        rejected >= 1,
        "expected cq_connections_rejected_total{{reason=\"max_connections_per_ip\"}} >= 1, got {rejected}\n{m}"
    );
}

#[tokio::test]
async fn max_sessions_per_user_rejects_a_second_concurrent_logon() {
    let topic = TopicSpec::new("/conn-user-cap", "k").with_inline_columns([("k", "string")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            auth: Some(AuthOpts::users(vec![UserSpec {
                username: "alice".into(),
                // bcrypt hash of "s3cret" at a low cost factor for fast
                // test startup — matches the pattern used elsewhere in
                // this harness's tests.
                password_hash: bcrypt::hash("s3cret", 4).unwrap(),
                entitlements: vec!["*:*".into()],
                row_filter: None,
            }])),
            transport_limits: Some(TransportLimitsOpts {
                max_sessions_per_user: Some(1),
                ..Default::default()
            }),
            ..ServerOpts::default()
        },
    )
    .await;

    let session1 = Client::connect(&server.tcp_url()).await.expect("connect 1");
    session1
        .logon("alice", "s3cret")
        .await
        .expect("1st logon for alice should succeed");

    let session2 = Client::connect(&server.tcp_url()).await.expect("connect 2");
    let result = session2.logon("alice", "s3cret").await;
    assert!(
        result.is_err(),
        "2nd concurrent logon as alice should be rejected (max_sessions_per_user=1)"
    );

    let m = metrics_text(&server).await;
    let rejected =
        counter_value_with_reason(&m, "cq_connections_rejected_total", "max_sessions_per_user");
    assert!(
        rejected >= 1,
        "expected cq_connections_rejected_total{{reason=\"max_sessions_per_user\"}} >= 1, got {rejected}\n{m}"
    );

    // Logging off session1 (dropping the client) frees the slot for a
    // retry — proves the guard actually releases on disconnect, not
    // just that the cap is enforced once.
    drop(session1);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let session3 = Client::connect(&server.tcp_url()).await.expect("connect 3");
    session3
        .logon("alice", "s3cret")
        .await
        .expect("logon after the first session disconnected should succeed");
}

/// Sanity check that `cq_connections_rejected_total` doesn't fire at
/// all under normal, unconstrained traffic — guards against a false
/// positive that would make the assertions above meaningless.
#[tokio::test]
async fn no_rejections_under_default_limits() {
    let topic = TopicSpec::new("/conn-default", "k").with_inline_columns([("k", "string")]);
    let server = start_server_with(vec![topic], ServerOpts::default()).await;

    for _ in 0..10 {
        let _c = Client::connect(&server.tcp_url())
            .await
            .expect("connect under default (10000) cap");
    }

    let m = metrics_text(&server).await;
    let rejected = counter_value(&m, "cq_connections_rejected_total");
    assert_eq!(rejected, 0, "no rejections expected under default limits");
}

//! Task 2.2 (D2 / P0.2) — TLS for the admin HTTP server.
//!
//! `[admin_tls] cert_file/key_file` mirrors `[transport.tls]`'s shape
//! exactly. This suite spins up a real release `cqserver` binary with
//! `admin_tls` configured (harness-generated self-signed cert) and
//! `admin_token` set, and proves:
//!   - a plain `http://` request against the admin port fails (no TLS
//!     handshake ever happens, so the peer never gets an HTTP
//!     response back — reqwest sees a connection error, not a 4xx/5xx).
//!   - an `https://` request with the right token succeeds against a
//!     protected route (`/stats`).
//!   - `https://` `/healthz` works with no token at all, same as the
//!     plain-HTTP path (Task 2.1's open-liveness-probe contract holds
//!     under TLS too).

use cq_e2e_tests::{admin_http_client, start_server_with, ServerOpts, TlsOpts, TopicSpec};

const TOKEN: &str = "admin-tls-token";

fn topic() -> TopicSpec {
    TopicSpec::new("/admin-tls", "k").with_inline_columns([("k", "string"), ("v", "double")])
}

async fn server_with_admin_tls() -> cq_e2e_tests::ServerHandle {
    start_server_with(
        vec![topic()],
        ServerOpts {
            admin_token: Some(TOKEN.to_string()),
            admin_tls: Some(TlsOpts::default()),
            ..ServerOpts::default()
        },
    )
    .await
}

/// A plain `http://` request against an `admin_tls`-enabled server
/// must not get a plaintext HTTP response of any kind — the listener
/// only speaks TLS. reqwest will fail to parse/complete the
/// "response" (the server closes the connection after a failed TLS
/// handshake, since what actually arrived was a raw HTTP request
/// line, not a TLS ClientHello).
#[tokio::test]
async fn plain_http_request_fails_when_admin_tls_enabled() {
    let server = server_with_admin_tls().await;
    // server.admin_url() returns https://... when admin_tls is set;
    // build the plain-http variant explicitly to prove it's rejected.
    let http_url = format!("http://127.0.0.1:{}/healthz", server.admin_port);

    let client = admin_http_client();
    let result = client.get(&http_url).send().await;
    assert!(
        result.is_err(),
        "plain HTTP request against an admin_tls-enabled server should fail, got {:?}",
        result.map(|r| r.status())
    );
}

/// `https://` with the correct bearer token succeeds against a
/// protected route.
#[tokio::test]
async fn https_with_token_succeeds_on_protected_route() {
    let server = server_with_admin_tls().await;
    let client = admin_http_client();
    let url = format!("{}/stats", server.admin_url());

    // Sanity: no token should still 401 over TLS (the auth guard runs
    // the same way regardless of transport).
    let resp = client.get(&url).send().await.expect("no-token https request");
    assert_eq!(resp.status(), 401, "no-token /stats over https should 401");

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("authed https request");
    assert!(
        resp.status().is_success(),
        "authed https /stats should succeed, got {:?}",
        resp.status()
    );
}

/// `/healthz` over `https://` stays open with no token, mirroring the
/// plain-HTTP contract (Task 2.1) under TLS.
#[tokio::test]
async fn https_healthz_open_without_token() {
    let server = server_with_admin_tls().await;
    let client = admin_http_client();
    let url = format!("{}/healthz", server.admin_url());

    let resp = client.get(&url).send().await.expect("https healthz request");
    assert_eq!(resp.status(), 200, "https healthz must stay open");
}

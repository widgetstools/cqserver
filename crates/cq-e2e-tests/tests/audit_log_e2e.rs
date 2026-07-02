//! Task 2.4 (D5/P0.5) — `[audit]` end-to-end.
//!
//! Configures a server with `[audit] sink = "file", path = ...`
//! (sugar over the S25 per-target sink machinery — see
//! `cq_server::logging`), then drives:
//!   (a) a failed logon (bad password) — asserts an
//!       `event="logon" outcome="fail"` line lands in the audit file
//!       with the attempted user + peer IP.
//!   (b) a successful logon — same event shape, `outcome="success"`.
//!   (c) an admin `rotate-journal` mutation (with the admin token) —
//!       asserts an `event="admin"` line with the route, actor, and
//!       args.
//!   (d) an entitlement denial (subscribe to a topic the user has no
//!       entitlement for) — asserts an `event="entitlement_denied"`
//!       line with user/op/topic/peer.
//!
//! `[audit]` is deliberately configured WITHOUT any `[[logging.sinks]]`
//! entries, to prove the sugar path works standalone (doesn't require
//! also hand-authoring a sink) — see the regression test in
//! `cq_server::logging::tests::install_with_audit_config_only_does_not_use_stderr_fallback`
//! for the unit-level version of this same property.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, AuditOpts, AuthOpts, ServerOpts, TopicSpec, UserSpec};
use std::time::Duration;

const TOKEN: &str = "audit-e2e-admin-token";

fn topic() -> TopicSpec {
    TopicSpec::new("/audit-e2e", "k")
        .with_inline_columns([("k", "string")])
        .with_persist()
}

async fn read_audit_file(server: &cq_e2e_tests::ServerHandle, audit_path: &str) -> String {
    // Give the tracing layer a moment to flush the file.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let root = server.config_dir.parent().expect("root").to_path_buf();
    let audit_file = root.join(audit_path);
    std::fs::read_to_string(&audit_file).unwrap_or_else(|e| {
        panic!(
            "expected audit file at {} to exist and be readable: {e}",
            audit_file.display()
        )
    })
}

/// (a) + (b) + (c) — failed logon, then an admin rotate-journal with
/// the correct token, both land in the audit file with the fields the
/// brief calls out (user, peer, route/actor/args).
#[tokio::test]
async fn failed_logon_and_admin_mutation_land_in_audit_file() {
    let pw_hash = bcrypt::hash("correct-horse", bcrypt::DEFAULT_COST).expect("bcrypt");
    let audit_path = "logs/audit.log".to_string();

    let opts = ServerOpts {
        auth: Some(AuthOpts {
            users: vec![UserSpec {
                username: "alice".into(),
                password_hash: pw_hash,
                entitlements: vec!["*:*".into()],
                row_filter: None,
            }],
            jwt: None,
        }),
        audit: Some(AuditOpts::file(audit_path.clone())),
        admin_token: Some(TOKEN.to_string()),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic()], opts).await;

    // (a) Failed logon: bad password.
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let _ = client.logon("alice", "WRONG-PASSWORD").await.err();

    // (c) Admin mutation: rotate-journal with the correct token.
    let rc = reqwest::Client::new();
    let rotate_url = format!("{}/admin/rotate-journal/%2Faudit-e2e", server.admin_url());
    let resp = rc
        .post(&rotate_url)
        .header("Authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("rotate-journal request");
    assert!(
        resp.status().is_success(),
        "rotate-journal should succeed, got {:?}",
        resp.status()
    );

    let contents = read_audit_file(&server, &audit_path).await;

    // --- logon-fail assertions -----------------------------------
    assert!(
        contents.contains("event=\"logon\"") || contents.contains("event=logon"),
        "expected a logon audit event; got: {contents}"
    );
    assert!(
        contents.contains("outcome=\"fail\"") || contents.contains("outcome=fail"),
        "expected outcome=fail for the bad-password logon; got: {contents}"
    );
    assert!(
        contents.contains("alice"),
        "expected attempted user `alice` in the audit log; got: {contents}"
    );
    assert!(
        contents.contains(&format!("peer_addr")),
        "expected a peer_addr field on the logon audit line; got: {contents}"
    );
    // The client always connects from loopback in this harness.
    assert!(
        contents.contains("127.0.0.1"),
        "expected the real peer IP (127.0.0.1) in the audit log; got: {contents}"
    );

    // --- admin-mutation assertions ---------------------------------
    assert!(
        contents.contains("event=\"admin\"") || contents.contains("event=admin"),
        "expected an admin audit event; got: {contents}"
    );
    assert!(
        contents.contains("rotate-journal"),
        "expected the rotate-journal route in the audit log; got: {contents}"
    );
    assert!(
        contents.contains("admin-token"),
        "expected actor=admin-token in the audit log; got: {contents}"
    );
    assert!(
        contents.contains("audit-e2e"),
        "expected the topic arg in the admin audit log; got: {contents}"
    );
}

/// (b) — a successful logon also lands in the audit file, with
/// outcome=success and the real peer IP.
#[tokio::test]
async fn successful_logon_lands_in_audit_file_with_peer_addr() {
    let pw_hash = bcrypt::hash("secret", bcrypt::DEFAULT_COST).expect("bcrypt");
    let audit_path = "logs/audit.log".to_string();
    let opts = ServerOpts {
        auth: Some(AuthOpts {
            users: vec![UserSpec {
                username: "bob".into(),
                password_hash: pw_hash,
                entitlements: vec!["*:*".into()],
                row_filter: None,
            }],
            jwt: None,
        }),
        audit: Some(AuditOpts::file(audit_path.clone())),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic()], opts).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.logon("bob", "secret").await.expect("logon");

    let contents = read_audit_file(&server, &audit_path).await;
    assert!(
        contents.contains("outcome=\"success\"") || contents.contains("outcome=success"),
        "expected outcome=success for the good logon; got: {contents}"
    );
    assert!(contents.contains("bob"), "expected user `bob`; got: {contents}");
    assert!(
        contents.contains("127.0.0.1"),
        "expected the real peer IP in the audit log; got: {contents}"
    );
}

/// (d) — a subscribe rejected by entitlements emits
/// `event=entitlement_denied` with user/op/topic/peer.
#[tokio::test]
async fn entitlement_denial_lands_in_audit_file() {
    let pw_hash = bcrypt::hash("secret", bcrypt::DEFAULT_COST).expect("bcrypt");
    let audit_path = "logs/audit.log".to_string();
    // "carol" has NO entitlements at all — any subscribe is denied.
    let opts = ServerOpts {
        auth: Some(AuthOpts {
            users: vec![UserSpec {
                username: "carol".into(),
                password_hash: pw_hash,
                entitlements: vec![],
                row_filter: None,
            }],
            jwt: None,
        }),
        audit: Some(AuditOpts::file(audit_path.clone())),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic()], opts).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.logon("carol", "secret").await.expect("logon should succeed");

    // carol has no entitlements, so this subscribe must be denied.
    let sub_result = client.subscribe("/audit-e2e", None).await;
    assert!(sub_result.is_err(), "expected subscribe to be denied for carol");

    let contents = read_audit_file(&server, &audit_path).await;
    assert!(
        contents.contains("entitlement_denied"),
        "expected an entitlement_denied audit event; got: {contents}"
    );
    assert!(
        contents.contains("carol"),
        "expected user `carol` in the entitlement-denial audit line; got: {contents}"
    );
    assert!(
        contents.contains("audit-e2e"),
        "expected the denied topic in the audit line; got: {contents}"
    );
    assert!(
        contents.contains("127.0.0.1"),
        "expected the real peer IP on the entitlement-denial audit line; got: {contents}"
    );
}

/// `[audit] sink = "syslog"` — config parsing + server boot only (no
/// live syslog daemon in CI). Confirms the server doesn't refuse to
/// start / doesn't panic when `[audit]` names the syslog sink; actual
/// delivery to a syslog daemon is covered by the unit tests in
/// `cq_server::logging::tests` (`audit_config_syslog_sink_parses`,
/// and the `SyslogWriter` connect/format logic).
#[tokio::test]
async fn syslog_audit_sink_config_does_not_prevent_server_boot() {
    let opts = ServerOpts {
        audit: Some(cq_e2e_tests::AuditOpts {
            sink: "syslog".into(),
            // Deliberately a path unlikely to have a listener; the
            // sink is best-effort (matches syslog's fire-and-forget
            // contract), so this must not fail server startup even if
            // nothing is listening.
            path: "/tmp/cqserver-audit-e2e-nonexistent.sock".into(),
        }),
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic()], opts).await;
    // /healthz succeeding proves the server came up despite the
    // syslog socket not existing.
    let resp = reqwest::get(format!("{}/healthz", server.admin_url()))
        .await
        .expect("healthz request");
    assert_eq!(resp.status(), 200);
}

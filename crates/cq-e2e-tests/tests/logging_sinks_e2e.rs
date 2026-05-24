//! S25 — per-target tracing sinks end-to-end.
//!
//! Configure two sinks: one routes everything except audit events
//! to stderr (default), the other routes ONLY `cq_audit` events to a
//! file under the test's tempdir. After a successful Logon, assert
//! the audit file contains the `Logon ok` line and stderr did NOT
//! (we don't have a stderr capture, but we can assert the file's
//! content matches the audit event shape).

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, AuthOpts, LogSinkSpec, ServerOpts, TopicSpec, UserSpec,
};
use std::time::Duration;

#[tokio::test]
async fn audit_events_route_to_dedicated_sink_file() {
    // Topic + a single user so we can drive a real Logon path that
    // emits the `cq_audit` event.
    let topic = TopicSpec::new("/audit-e2e", "k").with_inline_columns([("k", "string")]);

    // bcrypt hash of "secret".
    let pw_hash = bcrypt::hash("secret", bcrypt::DEFAULT_COST).expect("bcrypt");

    // The audit file lives under {root}/logs/audit.log. The harness
    // writes the config TOML under {root}/config/, so a path of
    // "logs/audit.log" resolves relative to the SERVER's CWD which
    // the harness sets to {root}.
    let audit_path = "logs/audit.log".to_string();

    let opts = ServerOpts {
        auth: Some(AuthOpts {
            users: vec![UserSpec {
                username: "alice".into(),
                password_hash: pw_hash,
                entitlements: vec![],
                row_filter: None,
            }],
            jwt: None,
        }),
        logging_sinks: vec![
            // Operational sink: everything except audit → stderr.
            LogSinkSpec::stderr("info,cq_audit=off"),
            // Audit sink: ONLY cq_audit events → file.
            LogSinkSpec::to_file(audit_path.clone(), "cq_audit=info"),
        ],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    client.logon("alice", "secret").await.expect("logon");

    // Give the tracing layer a moment to flush the file (BufWriter is
    // line-buffered on most platforms but tracing's MakeWriter calls
    // flush per event, so this is mostly belt + suspenders).
    tokio::time::sleep(Duration::from_millis(200)).await;

    let root = server.config_dir.parent().expect("root").to_path_buf();
    let audit_file = root.join(&audit_path);
    assert!(
        audit_file.exists(),
        "expected audit file at {} to exist",
        audit_file.display()
    );
    let contents = std::fs::read_to_string(&audit_file).expect("read audit log");
    assert!(
        contents.contains("Logon ok"),
        "expected `Logon ok` line in {}; got: {}",
        audit_file.display(),
        contents
    );
    assert!(
        contents.contains("alice"),
        "expected `alice` username in audit log; got: {}",
        contents
    );
}

#[tokio::test]
async fn failed_logon_emits_audit_event_to_file() {
    let topic = TopicSpec::new("/audit-fail", "k").with_inline_columns([("k", "string")]);
    let pw_hash = bcrypt::hash("secret", bcrypt::DEFAULT_COST).expect("bcrypt");
    let audit_path = "logs/audit.log".to_string();
    let opts = ServerOpts {
        auth: Some(AuthOpts {
            users: vec![UserSpec {
                username: "alice".into(),
                password_hash: pw_hash,
                entitlements: vec![],
                row_filter: None,
            }],
            jwt: None,
        }),
        logging_sinks: vec![
            LogSinkSpec::stderr("info,cq_audit=off"),
            LogSinkSpec::to_file(audit_path.clone(), "cq_audit=info"),
        ],
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    // Wrong password — server should still emit the audit event for
    // the failure, even though the client-side request returns an
    // error.
    let _ = client.logon("alice", "WRONG").await.err();

    tokio::time::sleep(Duration::from_millis(200)).await;
    let root = server.config_dir.parent().expect("root").to_path_buf();
    let audit_file = root.join(&audit_path);
    let contents = std::fs::read_to_string(&audit_file).expect("read audit log");
    assert!(
        contents.contains("Logon failed"),
        "expected `Logon failed` line in audit log; got: {}",
        contents
    );
}

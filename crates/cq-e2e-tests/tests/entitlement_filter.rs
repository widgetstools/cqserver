//! e2e: row-level entitlement filter is AND'd into client queries.
//!
//! Two users:
//!   - `alice` has `row_filter = "desk = 'RATES'"`
//!   - `bob`   has no row_filter — sees everything.
//! Both subscribe to the same topic with no client filter; alice
//! should see only RATES rows, bob should see all.

use cq_client::Client;
use cq_e2e_tests::{
    start_server_with, AuthOpts, ServerOpts, TopicSpec, UserSpec,
};
use serde_json::json;
use std::time::Duration;

fn bcrypt_hash(plain: &str) -> String {
    bcrypt::hash(plain, 4).expect("bcrypt")
}

#[tokio::test]
async fn row_filter_restricts_alice_to_rates_desk() {
    let topic = TopicSpec::new("/ent-trades", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("price", "double"),
    ]);

    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: Some(AuthOpts {
                users: vec![
                    UserSpec {
                        username: "alice".into(),
                        password_hash: bcrypt_hash("alice-pw"),
                        entitlements: vec!["*:*".into()],
                        row_filter: Some("desk = 'RATES'".into()),
                    },
                    UserSpec {
                        username: "bob".into(),
                        password_hash: bcrypt_hash("bob-pw"),
                        entitlements: vec!["*:*".into()],
                        row_filter: None,
                    },
                ],
                jwt: None,
            }),
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;

    // A publisher with full access — log in as bob.
    let publisher = Client::connect(&server.tcp_url()).await.expect("pub conn");
    publisher.logon("bob", "bob-pw").await.expect("bob logon");
    for (k, desk, price) in [
        ("T1", "RATES", 100.0),
        ("T2", "RATES", 200.0),
        ("T3", "EQUITIES", 300.0),
        ("T4", "EQUITIES", 400.0),
        ("T5", "CREDIT", 500.0),
    ] {
        publisher
            .publish(
                "/ent-trades",
                json!({ "k": k, "desk": desk, "price": price }),
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Alice has row_filter — should only see RATES rows.
    let alice = Client::connect(&server.tcp_url()).await.expect("alice conn");
    alice.logon("alice", "alice-pw").await.expect("alice logon");
    let alice_rows = alice.sow("/ent-trades", None).await.expect("alice sow");
    let mut alice_keys: Vec<String> = alice_rows
        .iter()
        .filter_map(|r| r.get("k").and_then(|v| v.as_str()).map(String::from))
        .collect();
    alice_keys.sort();
    assert_eq!(
        alice_keys,
        vec!["T1".to_string(), "T2".to_string()],
        "alice should only see RATES rows, got {alice_keys:?}"
    );

    // Bob has no row_filter — should see all 5 rows.
    let bob_rows = publisher.sow("/ent-trades", None).await.expect("bob sow");
    assert_eq!(
        bob_rows.len(),
        5,
        "bob without row_filter should see all rows"
    );

    // Alice trying to bypass with a contradictory filter — the server
    // AND's, so it can never widen.
    let alice_rows = alice
        .sow("/ent-trades", Some("desk = 'EQUITIES'"))
        .await
        .expect("alice sow with widening filter");
    assert!(
        alice_rows.is_empty(),
        "alice + (desk='EQUITIES' AND desk='RATES') should be empty, got {alice_rows:?}"
    );
}

// ───── Diversification ────────────────────────────────────────────

/// Row filter combined with client-supplied filter — narrows further, never widens.
#[tokio::test]
async fn row_filter_intersects_with_client_filter() {
    let topic = TopicSpec::new("/ent-narrow", "k").with_inline_columns([
        ("k", "string"),
        ("desk", "string"),
        ("price", "double"),
    ]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: Some(AuthOpts {
                users: vec![
                    UserSpec {
                        username: "alice".into(),
                        password_hash: bcrypt_hash("pw"),
                        entitlements: vec!["*:*".into()],
                        row_filter: Some("desk = 'RATES'".into()),
                    },
                    UserSpec {
                        username: "publisher".into(),
                        password_hash: bcrypt_hash("pw"),
                        entitlements: vec!["*:*".into()],
                        row_filter: None,
                    },
                ],
                jwt: None,
            }),
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;

    let publisher = Client::connect(&server.tcp_url()).await.unwrap();
    publisher.logon("publisher", "pw").await.unwrap();
    for (k, desk, p) in [
        ("T1", "RATES", 50.0),
        ("T2", "RATES", 150.0),
        ("T3", "RATES", 250.0),
        ("T4", "EQUITIES", 999.0),
    ] {
        publisher
            .publish("/ent-narrow", json!({ "k": k, "desk": desk, "price": p }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let alice = Client::connect(&server.tcp_url()).await.unwrap();
    alice.logon("alice", "pw").await.unwrap();
    // Client filter further restricts to price > 100.
    let rows = alice
        .sow("/ent-narrow", Some("price > 100"))
        .await
        .unwrap();
    let mut keys: Vec<String> = rows
        .iter()
        .map(|r| r.get("k").unwrap().as_str().unwrap().to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["T2", "T3"]);
}

/// Row filter that filters ALL rows for the user — empty result, no error.
#[tokio::test]
async fn row_filter_matching_no_rows_is_empty() {
    let topic = TopicSpec::new("/ent-empty", "k")
        .with_inline_columns([("k", "string"), ("desk", "string")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: Some(AuthOpts {
                users: vec![
                    UserSpec {
                        username: "u".into(),
                        password_hash: bcrypt_hash("pw"),
                        entitlements: vec!["*:*".into()],
                        row_filter: Some("desk = 'NONEXISTENT'".into()),
                    },
                ],
                jwt: None,
            }),
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;

    let admin = Client::connect(&server.tcp_url()).await.unwrap();
    admin.logon("u", "pw").await.unwrap();
    admin
        .publish("/ent-empty", json!({ "k": "a", "desk": "RATES" }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let rows = admin.sow("/ent-empty", None).await.unwrap();
    assert!(rows.is_empty(),
            "row filter matched no real desks, but got {rows:?}");
}

/// Per-action entitlements: a user granted only `publish:` can publish
/// but is denied subscribe/sow, and a user granted only `subscribe:` +
/// `sow:` can read but is denied publish. Proves each action is gated
/// independently — not the blanket `*:*` the other tests use.
#[tokio::test]
async fn per_action_entitlements_are_independent() {
    use cq_client::ClientError;
    let topic = TopicSpec::new("/perm", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: Some(AuthOpts {
                users: vec![
                    UserSpec {
                        username: "pubonly".into(),
                        password_hash: bcrypt_hash("pw"),
                        entitlements: vec!["publish:/perm".into()],
                        row_filter: None,
                    },
                    UserSpec {
                        username: "readonly".into(),
                        password_hash: bcrypt_hash("pw"),
                        entitlements: vec![
                            "subscribe:/perm".into(),
                            "sow:/perm".into(),
                        ],
                        row_filter: None,
                    },
                ],
                jwt: None,
            }),
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;

    // pubonly: publish succeeds, sow is forbidden.
    let pubonly = Client::connect(&server.tcp_url()).await.unwrap();
    pubonly.logon("pubonly", "pw").await.unwrap();
    pubonly
        .publish("/perm", json!({ "k": "a", "v": 1.0 }))
        .await
        .expect("pubonly may publish");
    let denied = pubonly.sow("/perm", None).await;
    assert!(
        matches!(denied, Err(ClientError::Server(_))),
        "pubonly must be denied sow, got {denied:?}"
    );

    // readonly: sow succeeds, publish is forbidden.
    let readonly = Client::connect(&server.tcp_url()).await.unwrap();
    readonly.logon("readonly", "pw").await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let rows = readonly.sow("/perm", None).await.expect("readonly may sow");
    assert_eq!(rows.len(), 1, "readonly should see the published row");
    let denied = readonly.publish("/perm", json!({ "k": "b", "v": 2.0 })).await;
    assert!(
        matches!(denied, Err(ClientError::Server(_))),
        "readonly must be denied publish, got {denied:?}"
    );
}

/// Wrong password → logon errors; no published data leaks.
#[tokio::test]
async fn bad_password_rejects_logon() {
    use cq_client::ClientError;
    let topic = TopicSpec::new("/ent-bad", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            outbound_queue_capacity: 1024,
            slow_consumer: None,
            tls: None,
            queues: Vec::new(),
            auth: Some(AuthOpts {
                users: vec![UserSpec {
                    username: "u".into(),
                    password_hash: bcrypt_hash("correct"),
                    entitlements: vec!["*:*".into()],
                    row_filter: None,
                }],
                jwt: None,
            }),
            txlog_archive: None,
            views: Vec::new(),
            spillover: None,
            logging_sinks: Vec::new(),
            replication: None,
            hard_max_sow_result_rows: None,
            admin_token: None,
        },
    )
    .await;
    let c = Client::connect(&server.tcp_url()).await.unwrap();
    let r = c.logon("u", "wrong-password").await;
    assert!(matches!(r, Err(ClientError::Server(_))), "got {r:?}");

    // Subsequent operations without successful logon should fail.
    let r2 = c.publish("/ent-bad", json!({ "k": "x" })).await;
    assert!(r2.is_err(), "operations after failed auth must error: {r2:?}");
}

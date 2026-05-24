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

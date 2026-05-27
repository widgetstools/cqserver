//! P7 e2e — failed SOW must not leave the snapshot-encoder cache in
//! `Building`, otherwise identical follow-up requests wait forever.
//!
//! AMPS_PARITY §4 bug 4 — a SOW that errored mid-encode used to wedge
//! the shared encode-once-fanout cache slot. The fix is to call
//! `abandon_snapshot_cache_slot` on the Err branch (already wired in
//! `deliver_streaming_snapshot`). This test pins that contract: an
//! intentionally-failing SOW, then the SAME SOW again — must not hang.

use cq_client::{Client, ClientError};
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;

#[tokio::test]
async fn failed_sow_does_not_wedge_cache() {
    let topic =
        TopicSpec::new("/wedge-test", "k").with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/wedge-test", json!({ "k": "a", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Submit a SOW that's guaranteed to fail at parse time
    // (`SELECT bogus_col FROM t` — unknown column).
    let bad_sql = "SELECT bogus_col FROM t";
    let r1 = tokio::time::timeout(
        Duration::from_secs(5),
        client.sow_sql("/wedge-test", bad_sql),
    )
    .await
    .expect("first failing SOW must not timeout");
    assert!(
        matches!(r1, Err(ClientError::Server(_)) | Ok(_)),
        "expected server error or empty result for parse failure, got {r1:?}"
    );

    // Hit the SAME failing SOW again — must not wait on a Building
    // slot left over from the first failure.
    let r2 = tokio::time::timeout(
        Duration::from_secs(5),
        client.sow_sql("/wedge-test", bad_sql),
    )
    .await
    .expect("second failing SOW must not wedge (AMPS_PARITY §4 bug 4)");
    assert!(
        matches!(r2, Err(ClientError::Server(_)) | Ok(_)),
        "second attempt: {r2:?}"
    );

    // Sanity: a valid SOW still works after the failure pair.
    let ok = tokio::time::timeout(
        Duration::from_secs(5),
        client.sow_sql("/wedge-test", "SELECT k, v FROM t"),
    )
    .await
    .expect("valid SOW must not be blocked by prior cache state")
    .expect("valid SOW should succeed");
    assert_eq!(ok.len(), 1);
}

// ───── Diversification ────────────────────────────────────────────

/// Five concurrent failing SOWs against the same (topic, sql) must
/// each return cleanly — none should wedge waiting on a Building
/// slot that another sibling abandoned.
#[tokio::test]
async fn concurrent_failing_sows_all_resolve() {
    let topic = TopicSpec::new("/wedge-concur", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/wedge-concur", json!({ "k": "a", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let bad_sql = "SELECT no_such_col FROM t";
    let mut handles = Vec::new();
    for _ in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                c.sow_sql("/wedge-concur", bad_sql),
            )
            .await
        }));
    }
    for h in handles {
        let r = h.await.expect("task join").expect("must not timeout");
        assert!(
            matches!(r, Err(ClientError::Server(_)) | Ok(_)),
            "all 5 concurrent failing SOWs must return: {r:?}"
        );
    }

    // Server still healthy after the storm.
    let ok = tokio::time::timeout(
        Duration::from_secs(3),
        client.sow_sql("/wedge-concur", "SELECT k FROM t"),
    )
    .await
    .expect("good SOW not blocked")
    .expect("good SOW ok");
    assert_eq!(ok.len(), 1);
}

/// Failed SOW carries a server error with the failing sub_id routed
/// back to the awaiting RPC (otherwise the client's snapshot_completion
/// would never resolve — P7's contract).
#[tokio::test]
async fn failed_sow_returns_server_error_with_message() {
    let topic = TopicSpec::new("/wedge-msg", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    let r = client
        .sow_sql("/wedge-msg", "SELECT col_that_does_not_exist FROM t")
        .await;
    match r {
        Err(ClientError::Server(msg)) => {
            // Server should mention either the column or "query error".
            assert!(
                msg.to_lowercase().contains("col")
                    || msg.to_lowercase().contains("query")
                    || msg.to_lowercase().contains("unknown"),
                "error message should reference the problem: {msg}"
            );
        }
        other => panic!("expected ClientError::Server, got {other:?}"),
    }
}

/// After publish → failing SOW → second SOW: the failing SOW must
/// not leave the cache holding the pre-failure snapshot for the
/// SUCCESS query.
#[tokio::test]
async fn failure_after_publish_does_not_serve_stale_to_followup() {
    let topic = TopicSpec::new("/wedge-mix", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    client
        .publish("/wedge-mix", json!({ "k": "a", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let _ok1 = client
        .sow_sql("/wedge-mix", "SELECT k, v FROM t")
        .await
        .unwrap();
    let _bad = client
        .sow_sql("/wedge-mix", "SELECT bogus FROM t")
        .await; // expect error

    client
        .publish("/wedge-mix", json!({ "k": "b", "v": 99 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let ok2 = client
        .sow_sql("/wedge-mix", "SELECT k, v FROM t")
        .await
        .unwrap();
    assert_eq!(ok2.len(), 2, "publish-after-failure must surface in next SOW");
}

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

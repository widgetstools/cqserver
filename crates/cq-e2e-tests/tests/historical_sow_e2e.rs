//! #24 — Historical / time-based SOW queries end-to-end.
//!
//! Verifies that a one-shot SOW carrying an "as-of sequence" or
//! "as-of timestamp" returns the topic's reconstructed point-in-time
//! state (last-writer-wins per key, tombstones remove keys), rather
//! than the live SOW. Requires a persistent topic so there's a txlog
//! to replay.

use cq_client::Client;
use cq_e2e_tests::{start_server, start_server_with, ServerOpts, TopicSpec};
use serde_json::json;
use std::time::Duration;

fn px_of(rows: &[serde_json::Map<String, serde_json::Value>], id: &str) -> Option<i64> {
    rows.iter()
        .find(|r| r.get("id").and_then(|v| v.as_str()) == Some(id))
        .and_then(|r| r.get("px").and_then(|v| v.as_i64()))
}

/// Regression: `hard_max_sow_result_rows = 0` means "cap disabled" (as the
/// docs state and the live-streaming path honors). The as-of/historical SOW
/// path must NOT truncate the result to zero rows when the cap is disabled.
#[tokio::test]
async fn sow_as_of_with_disabled_hard_cap_returns_all_rows() {
    let topic = TopicSpec::new("/asof_cap0", "id")
        .with_inline_columns([("id", "string"), ("px", "long")])
        .with_persist();
    let server = start_server_with(
        vec![topic],
        ServerOpts {
            // 0 = disabled. Pre-fix, the as-of path truncated to 0 rows.
            hard_max_sow_result_rows: Some(0),
            ..Default::default()
        },
    )
    .await;
    let c = Client::connect(&server.tcp_url()).await.expect("connect");

    c.publish("/asof_cap0", json!({ "id": "A", "px": 10 })).await.unwrap();
    c.publish("/asof_cap0", json!({ "id": "B", "px": 20 })).await.unwrap();
    let s_c = c.publish("/asof_cap0", json!({ "id": "C", "px": 30 })).await.unwrap();

    // As-of the latest write: all three keys must come back, not zero.
    let r = c.sow_as_of_sequence("/asof_cap0", s_c, None).await.unwrap();
    assert_eq!(
        r.len(),
        3,
        "disabled cap (0) must not truncate the as-of SOW to zero rows; got {r:?}"
    );
}

#[tokio::test]
async fn sow_as_of_sequence_reconstructs_point_in_time_state() {
    let topic = TopicSpec::new("/asof", "id")
        .with_inline_columns([("id", "string"), ("px", "long")])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let c = Client::connect(&server.tcp_url()).await.expect("connect");

    // Build a history on key A (3 successive values), then add B.
    let s_a10 = c.publish("/asof", json!({ "id": "A", "px": 10 })).await.unwrap();
    let s_a20 = c.publish("/asof", json!({ "id": "A", "px": 20 })).await.unwrap();
    let s_a30 = c.publish("/asof", json!({ "id": "A", "px": 30 })).await.unwrap();
    let s_b99 = c.publish("/asof", json!({ "id": "B", "px": 99 })).await.unwrap();

    // As-of the very first write: only A exists, at px=10.
    let r = c.sow_as_of_sequence("/asof", s_a10, None).await.unwrap();
    assert_eq!(r.len(), 1, "only A should exist as-of s_a10");
    assert_eq!(px_of(&r, "A"), Some(10));

    // As-of A's second write: A=20, B not yet present.
    let r = c.sow_as_of_sequence("/asof", s_a20, None).await.unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(px_of(&r, "A"), Some(20));

    // As-of B's write: A collapsed to its latest (30) plus B=99.
    let r = c.sow_as_of_sequence("/asof", s_b99, None).await.unwrap();
    assert_eq!(r.len(), 2, "both keys present as-of s_b99");
    assert_eq!(px_of(&r, "A"), Some(30));
    assert_eq!(px_of(&r, "B"), Some(99));

    // Live SOW agrees with the as-of-latest reconstruction.
    let live = c.sow("/asof", None).await.unwrap();
    assert_eq!(px_of(&live, "A"), Some(30));
    assert_eq!(px_of(&live, "B"), Some(99));

    let _ = s_a30;
}

#[tokio::test]
async fn sow_as_of_sequence_honors_tombstones_and_filter() {
    let topic = TopicSpec::new("/asof_del", "id")
        .with_inline_columns([("id", "string"), ("px", "long")])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let c = Client::connect(&server.tcp_url()).await.expect("connect");

    c.publish("/asof_del", json!({ "id": "A", "px": 10 })).await.unwrap();
    let s_b = c.publish("/asof_del", json!({ "id": "B", "px": 99 })).await.unwrap();
    // Delete A; its tombstone advances the sequence.
    let s_del = c.sow_delete("/asof_del", "A").await.unwrap();

    // Before the delete: A still present.
    let before = c.sow_as_of_sequence("/asof_del", s_b, None).await.unwrap();
    assert_eq!(before.len(), 2);
    assert_eq!(px_of(&before, "A"), Some(10));

    // After the delete: A is gone, only B survives.
    let after = c.sow_as_of_sequence("/asof_del", s_del, None).await.unwrap();
    assert_eq!(after.len(), 1, "A removed by tombstone as-of s_del");
    assert_eq!(px_of(&after, "B"), Some(99));

    // Filter applies to the reconstructed snapshot.
    let filtered = c
        .sow_as_of_sequence("/asof_del", s_b, Some("px > 50"))
        .await
        .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(px_of(&filtered, "B"), Some(99));
}

#[tokio::test]
async fn sow_as_of_timestamp_resolves_cutoff() {
    let topic = TopicSpec::new("/asof_ts", "id")
        .with_inline_columns([("id", "string"), ("px", "long")])
        .with_persist();
    let server = start_server(vec![topic]).await;
    let c = Client::connect(&server.tcp_url()).await.expect("connect");

    // Phase 1: write A. Then capture a cutoff strictly after it but
    // before phase 2, with a generous gap to avoid clock granularity
    // flakiness.
    c.publish("/asof_ts", json!({ "id": "A", "px": 10 })).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let cutoff_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    tokio::time::sleep(Duration::from_millis(80)).await;
    // Phase 2: write B after the cutoff.
    c.publish("/asof_ts", json!({ "id": "B", "px": 99 })).await.unwrap();

    let r = c.sow_as_of_timestamp("/asof_ts", cutoff_ms, None).await.unwrap();
    assert_eq!(r.len(), 1, "only phase-1 (A) is at/before the cutoff");
    assert_eq!(px_of(&r, "A"), Some(10));

    // A cutoff far in the future returns the current state.
    let now_plus = cutoff_ms + 60_000;
    let all = c.sow_as_of_timestamp("/asof_ts", now_plus, None).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(px_of(&all, "B"), Some(99));
}

#[tokio::test]
async fn sow_as_of_on_non_persistent_topic_errors() {
    // No `.with_persist()` → no txlog → historical SOW must error.
    let topic = TopicSpec::new("/asof_mem", "id")
        .with_inline_columns([("id", "string"), ("px", "long")]);
    let server = start_server(vec![topic]).await;
    let c = Client::connect(&server.tcp_url()).await.expect("connect");
    c.publish("/asof_mem", json!({ "id": "A", "px": 10 })).await.unwrap();

    let err = c.sow_as_of_sequence("/asof_mem", 1, None).await;
    assert!(err.is_err(), "as-of SOW on a non-persistent topic should error");
}

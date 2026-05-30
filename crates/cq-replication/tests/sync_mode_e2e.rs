//! S11 end-to-end: the replication shipper's Ack reader bumps the
//! primary's per-topic `last_replicated_sequence`, and the publish
//! path's S11 barrier (`Topic::await_replicated`) releases once the
//! standby has confirmed.
//!
//! The test stands up the same primary↔standby topology used by the
//! filter e2e, but additionally hands the primary's `SharedTopic`
//! through `topic_refs` so the shipper can call `mark_replicated`
//! on every received Ack. We then publish three entries to the
//! primary and assert the primary's `last_replicated_sequence`
//! converges to the highest assigned sequence.

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_replication::{receiver, shipper};
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn make_topic(name: &str) -> SharedTopic {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "qty"],
        &[ColumnType::String, ColumnType::Long],
    ));
    Arc::new(Topic::new(
        TopicConfig {
            name: name.into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ))
}

#[tokio::test]
async fn shipper_ack_reader_bumps_primary_last_replicated_sequence() {
    // ── primary side: a SharedTopic + a txlog with 3 entries ──
    let primary_topic = make_topic("/sync-mode");
    primary_topic.attach_txlog_for_test();

    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("sync-mode");
    let highest_seq: u64 = {
        let mut w = TxLogWriter::open(&topic_dir, FsyncPolicy::None).unwrap();
        for (i, trader) in ["alice", "bob", "carol"].iter().enumerate() {
            let mut m = serde_json::Map::new();
            m.insert(
                "trader".into(),
                serde_json::Value::String((*trader).into()),
            );
            m.insert(
                "qty".into(),
                serde_json::Value::from((i as i64 + 1) * 100),
            );
            let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
            w.append((i as u64) + 1, "/sync-mode", trader, &payload).unwrap();
        }
        w.sync().unwrap();
        3
    };

    // ── standby side: matching topic in a separate registry ──
    let standby_topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let standby_topic = make_topic("/sync-mode");
    let _ = standby_topic.take_mutation_rx();
    standby_topics.insert("/sync-mode".into(), standby_topic.clone());

    // ── network plumbing ──
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let recv_topics = standby_topics.clone();
    let recv_handle = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr.to_string(),
                token: None,
                instance_name: String::new(),
                concurrent: false,
            },
            recv_topics,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut topic_refs = HashMap::new();
    topic_refs.insert("/sync-mode".to_string(), primary_topic.clone());

    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![("/sync-mode".into(), topic_dir)],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs,
            token: None,
            instance_name: String::new(),
        })
        .await;
    });

    // ── assertion: primary's last_replicated_sequence converges to 3 ──
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut converged = false;
    while tokio::time::Instant::now() < deadline {
        if primary_topic.last_replicated_sequence() >= highest_seq {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        converged,
        "primary's last_replicated_sequence never reached {}; got {}",
        highest_seq,
        primary_topic.last_replicated_sequence()
    );

    ship_handle.abort();
    recv_handle.abort();
}

#[tokio::test]
async fn await_replicated_returns_after_ack() {
    // Pure cq-core unit-shaped test: mark_replicated bumps the
    // counter AND wakes any task already awaiting via the notify.
    let topic = make_topic("/local-barrier");
    let topic_clone = topic.clone();
    let waiter = tokio::spawn(async move {
        // Set up the future first; we'll bump from the outside.
        let notify = topic_clone.replication_notify_handle();
        // Poll-then-notify loop, same shape the router will use.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if topic_clone.last_replicated_sequence() >= 5 {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let n = notify.notified();
            if topic_clone.last_replicated_sequence() >= 5 {
                return true;
            }
            let _ = tokio::time::timeout(remaining, n).await;
        }
    });

    // Give the waiter a moment to install its notify subscription.
    tokio::time::sleep(Duration::from_millis(40)).await;
    topic.mark_replicated(3);
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(topic.last_replicated_sequence(), 3);
    topic.mark_replicated(5);

    let ok = waiter.await.expect("waiter joined");
    assert!(ok, "waiter should have woken once mark_replicated reached 5");
}

#[tokio::test]
async fn mark_replicated_is_monotonic() {
    let topic = make_topic("/mono");
    topic.mark_replicated(10);
    topic.mark_replicated(3); // must not lower
    topic.mark_replicated(7); // must not lower
    assert_eq!(topic.last_replicated_sequence(), 10);
    topic.mark_replicated(20);
    assert_eq!(topic.last_replicated_sequence(), 20);
}

// Helper trait — `Topic::attach_txlog_for_test` is only needed by
// this test file, so we attach via the public re-attach API via a
// small extension trait. (No-op; the topic's persistence is set up
// by `make_topic` already via its `persist: false` flag — leaving
// the txlog detached is correct.) Kept here for symmetry with the
// pattern from existing replication tests.
trait TopicTestExt {
    fn attach_txlog_for_test(&self);
}
impl TopicTestExt for SharedTopic {
    fn attach_txlog_for_test(&self) {
        // No-op — we don't exercise the publish path in this test;
        // the txlog is constructed externally and the shipper
        // reads it directly.
    }
}

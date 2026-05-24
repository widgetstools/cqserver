//! S12 end-to-end: with a per-destination filter on the shipper, only
//! matching entries reach the standby's SOW. Mirrors the existing
//! `end_to_end.rs` topology but adds a `desk = 'RATES'` filter; the
//! primary writes a mix of RATES and FX rows; the standby ends up
//! with only the RATES ones.

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_replication::filter::{FilterSpec, TransformSpec};
use cq_replication::{receiver, shipper};
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

fn build_standby(topic_name: &str) -> (Arc<DashMap<String, SharedTopic>>, SharedTopic) {
    let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let schema = Arc::new(Schema::from_strs(
        &["trader", "desk", "secret"],
        &[ColumnType::String, ColumnType::String, ColumnType::String],
    ));
    let topic: SharedTopic = Arc::new(Topic::new(
        TopicConfig {
            name: topic_name.into(),
            key_fields: vec!["trader".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ));
    let _ = topic.take_mutation_rx();
    topics.insert(topic_name.into(), topic.clone());
    (topics, topic)
}

fn write_log_entries(
    topic_dir: &std::path::Path,
    topic_name: &str,
    rows: &[(&str, &str, &str)],
) {
    let mut w = TxLogWriter::open(topic_dir, FsyncPolicy::None).unwrap();
    for (i, (trader, desk, secret)) in rows.iter().enumerate() {
        let mut m = serde_json::Map::new();
        m.insert("trader".into(), serde_json::Value::String((*trader).into()));
        m.insert("desk".into(), serde_json::Value::String((*desk).into()));
        m.insert("secret".into(), serde_json::Value::String((*secret).into()));
        let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
        w.append((i as u64) + 1, topic_name, trader, &payload).unwrap();
    }
    w.sync().unwrap();
}

async fn spawn_pair(
    topic_name: &str,
    topic_dir: std::path::PathBuf,
    standby_topics: Arc<DashMap<String, SharedTopic>>,
    filter: Option<FilterSpec>,
    transform: Option<TransformSpec>,
) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let recv_topics = standby_topics.clone();
    let recv_handle = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr.to_string(),
            },
            recv_topics,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let topic_name = topic_name.to_string();
    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![(topic_name, topic_dir)],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter,
            transform,
            topic_refs: Default::default(),
        })
        .await;
    });
    (ship_handle, recv_handle)
}

async fn wait_for<F: Fn() -> bool>(check: F, budget: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + budget;
    while tokio::time::Instant::now() < deadline {
        if check() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test]
async fn filter_drops_non_matching_entries_on_the_wire() {
    let (standby_topics, standby_topic) = build_standby("/repl-filter");
    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-filter");

    // 5 rows: 3 RATES (should ship) + 2 FX (should drop).
    write_log_entries(
        &topic_dir,
        "/repl-filter",
        &[
            ("alice", "RATES", "x"),
            ("bob", "FX", "x"),
            ("carol", "RATES", "x"),
            ("dave", "FX", "x"),
            ("eve", "RATES", "x"),
        ],
    );

    let filter = Some(FilterSpec {
        column: "desk".into(),
        value: "RATES".into(),
    });
    let (ship_h, recv_h) =
        spawn_pair("/repl-filter", topic_dir, standby_topics, filter, None).await;

    let ok = wait_for(|| standby_topic.row_count() == 3, Duration::from_secs(5)).await;
    assert!(
        ok,
        "expected exactly 3 RATES rows on the standby; got {}",
        standby_topic.row_count()
    );
    // Spot-check: no FX rows arrived.
    let fx = standby_topic
        .query("SELECT * FROM t WHERE desk = 'FX'")
        .unwrap();
    assert_eq!(fx.rows.len(), 0, "FX rows should not have shipped");

    ship_h.abort();
    recv_h.abort();
}

#[tokio::test]
async fn transform_strips_listed_fields_on_the_wire() {
    let (standby_topics, standby_topic) = build_standby("/repl-transform");
    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-transform");

    write_log_entries(
        &topic_dir,
        "/repl-transform",
        &[
            ("alice", "RATES", "redact-me"),
            ("bob", "RATES", "and-me"),
        ],
    );

    let transform = Some(TransformSpec {
        strip_fields: vec!["secret".into()],
    });
    let (ship_h, recv_h) =
        spawn_pair("/repl-transform", topic_dir, standby_topics, None, transform).await;

    let ok = wait_for(|| standby_topic.row_count() == 2, Duration::from_secs(5)).await;
    assert!(ok, "expected 2 rows on the standby");

    // Inspect the wire-replicated rows; `secret` should be absent (or
    // null after schema-coerce).
    let rows = standby_topic
        .query("SELECT * FROM t")
        .unwrap();
    for r in &rows.rows {
        // Either the field is missing entirely, or it was coerced to
        // null by the standby's schema (which still has the column
        // declared). Both shapes prove the transform redacted the
        // value end-to-end.
        match r.get("secret") {
            None => {}
            Some(serde_json::Value::Null) => {}
            Some(other) => panic!(
                "expected `secret` to be stripped/null on standby; got {other:?}"
            ),
        }
    }

    ship_h.abort();
    recv_h.abort();
}

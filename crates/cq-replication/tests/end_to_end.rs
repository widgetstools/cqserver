//! Replication end-to-end: a "primary" writes to a real txlog directory
//! while the `Shipper` streams over a TCP socket to the standby
//! `Receiver`, which replays into a `DashMap<topic, SharedTopic>`. We
//! assert the standby's SOW matches what the primary wrote.

use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_replication::{receiver, shipper};
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn shipper_to_receiver_replicates_publishes() {
    // --- standby side: build a topic registry that will receive replays.
    let standby_topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price"],
        &[ColumnType::String, ColumnType::Double],
    ));
    let standby_topic: SharedTopic = Arc::new(Topic::new(
        TopicConfig {
            name: "/repl-trades".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ));
    let _ = standby_topic.take_mutation_rx(); // ignore — we don't run an evaluator
    standby_topics.insert("/repl-trades".into(), standby_topic.clone());

    // --- primary side: log directory with three persisted entries.
    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-trades");
    {
        let mut w = TxLogWriter::open(&topic_dir, FsyncPolicy::None).unwrap();
        for (i, (sym, price)) in
            [("AAPL", 150.0), ("MSFT", 300.0), ("GOOGL", 2800.0)].iter().enumerate()
        {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), serde_json::Value::String((*sym).into()));
            m.insert("price".into(), serde_json::Value::from(*price));
            let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
            w.append((i as u64) + 1, "/repl-trades", sym, &payload).unwrap();
        }
        w.sync().unwrap();
    }

    // --- start receiver on an ephemeral port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // free the port; the receiver will rebind it. (Race-prone
                    // but practical for this test.)

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
    // Give the receiver a moment to bind.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // --- spawn the shipper pointing at the receiver.
    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![("/repl-trades".into(), topic_dir.clone())],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
        })
        .await;
    });

    // --- wait until the standby's SOW shows all three rows.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if standby_topic.row_count() == 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "standby never received the 3 replicated rows (got {})",
                standby_topic.row_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Verify content via a SOW query.
    let result = standby_topic
        .query("SELECT * FROM t WHERE symbol = 'GOOGL'")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("price").unwrap(), 2800.0);

    // Tear down.
    ship_handle.abort();
    recv_handle.abort();
}

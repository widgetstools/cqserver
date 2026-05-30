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
use parking_lot::Mutex;
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
                token: None,
                instance_name: String::new(),
                concurrent: false,
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
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: None,
            instance_name: String::new(),
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

/// Journal-cursor regression: after the shipper catches up and advances
/// its per-destination segment cursor, a SECOND wave of publishes appended
/// to the (active) txlog segment must still replicate. This guards the
/// optimization where the shipper skips already-shipped sealed segments
/// each poll cycle instead of rescanning the whole log.
#[tokio::test]
async fn shipper_replicates_second_wave_after_catchup() {
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
    let _ = standby_topic.take_mutation_rx();
    standby_topics.insert("/repl-trades".into(), standby_topic.clone());

    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-trades");

    // Wave 1: three rows.
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

    let ship_dir = topic_dir.clone();
    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![("/repl-trades".into(), ship_dir)],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: None,
            instance_name: String::new(),
        })
        .await;
    });

    // Wait for wave 1 to land.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if standby_topic.row_count() == 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("wave 1 never replicated (got {})", standby_topic.row_count());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wave 2: append three MORE rows to the same (now active) segment AFTER
    // the cursor has advanced past it.
    {
        let mut w = TxLogWriter::open(&topic_dir, FsyncPolicy::None).unwrap();
        for (i, (sym, price)) in
            [("AMZN", 175.0), ("NVDA", 900.0), ("META", 500.0)].iter().enumerate()
        {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), serde_json::Value::String((*sym).into()));
            m.insert("price".into(), serde_json::Value::from(*price));
            let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
            w.append((i as u64) + 4, "/repl-trades", sym, &payload).unwrap();
        }
        w.sync().unwrap();
    }

    // Wave 2 must replicate too — proves the cursor didn't skip new appends.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if standby_topic.row_count() == 6 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("wave 2 never replicated (got {})", standby_topic.row_count());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let result = standby_topic
        .query("SELECT * FROM t WHERE symbol = 'NVDA'")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].get("price").unwrap(), 900.0);

    ship_handle.abort();
    recv_handle.abort();
}

/// Same flow but with a matching shared token on both ends — proves the
/// shipper sends `Auth` first and the receiver accepts it, then streams
/// normally.
#[tokio::test]
async fn authenticated_replication_round_trip() {
    const TOKEN: &str = "shared-repl-secret";

    let standby_topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price"],
        &[ColumnType::String, ColumnType::Double],
    ));
    let standby_topic: SharedTopic = Arc::new(Topic::new(
        TopicConfig {
            name: "/repl-auth".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ));
    let _ = standby_topic.take_mutation_rx();
    standby_topics.insert("/repl-auth".into(), standby_topic.clone());

    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-auth");
    {
        let mut w = TxLogWriter::open(&topic_dir, FsyncPolicy::None).unwrap();
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), serde_json::Value::String("AAPL".into()));
        m.insert("price".into(), serde_json::Value::from(150.0));
        let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
        w.append(1, "/repl-auth", "AAPL", &payload).unwrap();
        w.sync().unwrap();
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let recv_topics = standby_topics.clone();
    let recv_handle = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr.to_string(),
                token: Some(TOKEN.into()),
                instance_name: String::new(),
                concurrent: false,
            },
            recv_topics,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![("/repl-auth".into(), topic_dir.clone())],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: Some(TOKEN.into()),
            instance_name: String::new(),
        })
        .await;
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if standby_topic.row_count() == 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("authenticated standby never received the replicated row");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    ship_handle.abort();
    recv_handle.abort();
}

/// S21 — full-mesh active-active: two writable nodes, each shipping its
/// local writes to the other AND receiving the other's writes on a
/// concurrent receiver. A publish on either node converges on both, and
/// neither node's write echoes back (replicated applies never re-enter
/// the local txlog, so each node ships only its own origin).
#[tokio::test]
async fn active_active_two_nodes_converge_without_echo() {
    // Build a persistent topic wired to a real txlog directory + origin id,
    // returning (topic, txlog_dir). The topic's local writes append to the
    // txlog (which the shipper tails); replicated applies do not.
    fn make_node(
        name: &str,
        origin: &str,
        dir: &std::path::Path,
    ) -> SharedTopic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut topic = Topic::new(
            TopicConfig {
                name: name.into(),
                key_fields: vec!["symbol".into()],
                persist: true,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: None,
            },
            schema,
            32,
        )
        .with_origin_id(origin);
        let writer = TxLogWriter::open(dir, FsyncPolicy::None).unwrap();
        topic.attach_txlog(Arc::new(Mutex::new(writer)));
        let topic: SharedTopic = Arc::new(topic);
        let _ = topic.take_mutation_rx(); // no evaluator in this test
        topic
    }

    const TOPIC: &str = "/aa-trades";
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let txdir_a = dir_a.path().join("aa-a");
    let txdir_b = dir_b.path().join("aa-b");

    let topic_a = make_node(TOPIC, "node-a", &txdir_a);
    let topic_b = make_node(TOPIC, "node-b", &txdir_b);

    let topics_a: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    topics_a.insert(TOPIC.into(), topic_a.clone());
    let topics_b: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    topics_b.insert(TOPIC.into(), topic_b.clone());

    // Reserve two ephemeral ports for the two receivers.
    let l_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a = l_a.local_addr().unwrap();
    let l_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b = l_b.local_addr().unwrap();
    drop(l_a);
    drop(l_b);

    // Concurrent receivers: node A listens on addr_a as "node-a", node B on
    // addr_b as "node-b".
    let ra_topics = topics_a.clone();
    let recv_a = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr_a.to_string(),
                token: None,
                instance_name: "node-a".into(),
                concurrent: true,
            },
            ra_topics,
        )
        .await;
    });
    let rb_topics = topics_b.clone();
    let recv_b = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr_b.to_string(),
                token: None,
                instance_name: "node-b".into(),
                concurrent: true,
            },
            rb_topics,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Shippers: A ships its local txlog to B's receiver, and vice versa.
    let ship_a = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr_b.to_string(),
            topics: vec![(TOPIC.into(), txdir_a.clone())],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: None,
            instance_name: "node-a".into(),
        })
        .await;
    });
    let ship_b = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr_a.to_string(),
            topics: vec![(TOPIC.into(), txdir_b.clone())],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: None,
            instance_name: "node-b".into(),
        })
        .await;
    });

    // Local write on each node (distinct keys so we can assert convergence).
    let row = |sym: &str, price: f64| {
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), serde_json::Value::String(sym.into()));
        m.insert("price".into(), serde_json::Value::from(price));
        m
    };
    topic_a.upsert_map(&row("AAA", 1.0)).unwrap();
    topic_b.upsert_map(&row("BBB", 2.0)).unwrap();

    // Both nodes must converge to both rows.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if topic_a.row_count() == 2 && topic_b.row_count() == 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "active-active did not converge (a={}, b={})",
                topic_a.row_count(),
                topic_b.row_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Let any (erroneous) echo settle, then assert no duplicate/extra rows.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(topic_a.row_count(), 2, "node-a gained an echoed row");
    assert_eq!(topic_b.row_count(), 2, "node-b gained an echoed row");

    // Each node sees the OTHER node's write with the right value.
    let b_on_a = topic_a
        .query("SELECT * FROM t WHERE symbol = 'BBB'")
        .unwrap();
    assert_eq!(b_on_a.rows.len(), 1);
    assert_eq!(b_on_a.rows[0].get("price").unwrap(), 2.0);
    let a_on_b = topic_b
        .query("SELECT * FROM t WHERE symbol = 'AAA'")
        .unwrap();
    assert_eq!(a_on_b.rows.len(), 1);
    assert_eq!(a_on_b.rows[0].get("price").unwrap(), 1.0);

    // Convergence dedup: each node tracked exactly the other origin's
    // high-water, and did NOT record its own origin as "foreign".
    let hw_a = topic_a.replicated_highwater_snapshot();
    assert_eq!(hw_a.get("node-b"), Some(&1));
    assert!(hw_a.get("node-a").is_none(), "own origin tracked as foreign");
    let hw_b = topic_b.replicated_highwater_snapshot();
    assert_eq!(hw_b.get("node-a"), Some(&1));
    assert!(hw_b.get("node-b").is_none(), "own origin tracked as foreign");

    ship_a.abort();
    ship_b.abort();
    recv_a.abort();
    recv_b.abort();
}

/// S20 — AMPS-style convergence: origin-tagged entries flow over the wire,
/// the standby tracks a per-origin high-water, and the shipper refuses to
/// reflect an entry back to the instance that produced it (loop avoidance).
#[tokio::test]
async fn origin_tagged_replication_dedups_and_avoids_loops() {
    let standby_topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price"],
        &[ColumnType::String, ColumnType::Double],
    ));
    // The standby is the named instance "node-b".
    let standby_topic: SharedTopic = Arc::new(Topic::new(
        TopicConfig {
            name: "/repl-origin".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ));
    let _ = standby_topic.take_mutation_rx();
    standby_topics.insert("/repl-origin".into(), standby_topic.clone());

    // Primary txlog: one stream, interleaved origins. The middle entry is
    // tagged with the standby's own id ("node-b") and must NOT be shipped
    // back to it.
    let primary_dir = tempdir().unwrap();
    let topic_dir = primary_dir.path().join("repl-origin");
    {
        let mut w = TxLogWriter::open(&topic_dir, FsyncPolicy::None).unwrap();
        let entry = |sym: &str, price: f64| {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), serde_json::Value::String(sym.into()));
            m.insert("price".into(), serde_json::Value::from(price));
            serde_json::to_vec(&serde_json::Value::Object(m)).unwrap()
        };
        w.append_with_origin(1, "/repl-origin", "AAPL", "node-a", &entry("AAPL", 1.0))
            .unwrap();
        w.append_with_origin(2, "/repl-origin", "LOOP", "node-b", &entry("LOOP", 9.0))
            .unwrap();
        w.append_with_origin(3, "/repl-origin", "MSFT", "node-a", &entry("MSFT", 3.0))
            .unwrap();
        w.sync().unwrap();
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let recv_topics = standby_topics.clone();
    let recv_handle = tokio::spawn(async move {
        let _ = receiver::run(
            receiver::ReceiverConfig {
                listen_addr: addr.to_string(),
                token: None,
                instance_name: "node-b".into(),
                concurrent: false,
            },
            recv_topics,
        )
        .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let ship_handle = tokio::spawn(async move {
        let _ = shipper::run(shipper::ShipperConfig {
            peer: addr.to_string(),
            topics: vec![("/repl-origin".into(), topic_dir.clone())],
            poll_interval: Duration::from_millis(20),
            reconnect_backoff: Duration::from_millis(100),
            filter: None,
            transform: None,
            topic_refs: Default::default(),
            token: None,
            instance_name: "node-a".into(),
        })
        .await;
    });

    // Only node-a's two entries arrive; the node-b-origin entry is skipped
    // by loop avoidance.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if standby_topic.row_count() == 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "standby never converged to 2 rows (got {})",
                standby_topic.row_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Give any erroneously-shipped loop entry a chance to land before we
    // assert its absence.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(standby_topic.row_count(), 2, "loop entry must not replicate");
    let loop_rows = standby_topic
        .query("SELECT * FROM t WHERE symbol = 'LOOP'")
        .unwrap();
    assert_eq!(loop_rows.rows.len(), 0, "node-b-origin entry leaked back");

    // The standby tracked node-a's high-water for convergent dedup.
    let hw = standby_topic.replicated_highwater_snapshot();
    assert_eq!(hw.get("node-a"), Some(&3));

    ship_handle.abort();
    recv_handle.abort();
}

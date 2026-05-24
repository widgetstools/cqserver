//! Integration tests: spin up the real TCP server in-process and
//! exercise the SDK against it.

use cq_client::{Client, ClientConfig, DeltaKind};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{SharedTopic, Topic, TopicConfig};
use cq_transport::auth::AuthStore;
use cq_transport::delivery::spawn_evaluator;
use cq_transport::heartbeat::HeartbeatConfig;
use cq_transport::queue::{new_queue_registry, Queue};
use cq_transport::router::RouterContext;
use cq_transport::session::new_registry;
use cq_transport::tcp;
use dashmap::DashMap;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Boot an isolated server: one SOW topic + one queue + a router
/// context wired up. Returns the bound TCP address.
async fn spawn_test_server() -> (String, Arc<DashMap<String, SharedTopic>>) {
    let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
    let schema = Arc::new(Schema::from_strs(
        &["symbol", "price"],
        &[ColumnType::String, ColumnType::Double],
    ));
    let topic: SharedTopic = Arc::new(Topic::new(
        TopicConfig {
            name: "/sdk-trades".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        32,
    ));
    let registry = new_registry();
    let mut_rx = topic.take_mutation_rx().unwrap();
    let _ev = spawn_evaluator(topic.clone(), mut_rx, registry.clone());
    topics.insert("/sdk-trades".into(), topic);

    let queues = new_queue_registry();
    queues.insert("/sdk-work".into(), Arc::new(Queue::new("/sdk-work")));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let ctx = RouterContext {
        topics: topics.clone(),
        sessions: registry,
        queues,
        auth: Arc::new(AuthStore::disabled()),
        sow_batch_size: cq_transport::session::DEFAULT_SOW_BATCH_SIZE,
        bookmark_store: cq_transport::router::new_bookmark_store(),
        spillover: None,
        read_only: false,
    };
    tokio::spawn(async move {
        loop {
            let (stream, peer) = match listener.accept().await {
                Ok(x) => x,
                Err(_) => return,
            };
            let ctx = ctx.clone();
            tokio::spawn(async move {
                let _ = tcp_handle(stream, peer.to_string(), ctx).await;
            });
        }
    });

    (format!("tcp://{}", addr), topics)
}

// Mirror of the TCP handler used by the production server. The
// transport crate's `handle_tcp_connection` isn't public, so we
// re-implement the minimum loop here using the public API.
async fn tcp_handle(
    stream: tokio::net::TcpStream,
    remote: String,
    ctx: RouterContext,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use bytes::BytesMut;
    use cq_protocol::codec::{decode_frame, encode_frame};
    use cq_protocol::message::CqMessage;
    use cq_transport::router::{cleanup_session, dispatch};
    use cq_transport::session::{OutboundFrame, Session};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut read_half, mut write_half) = stream.into_split();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<OutboundFrame>(8192);
    let mut session = Session::new(remote, tx);

    let writer = tokio::spawn(async move {
        let mut out = BytesMut::new();
        while let Some(frame) = rx.recv().await {
            out.clear();
            encode_frame(frame.as_bytes(), &mut out);
            if write_half.write_all(&out).await.is_err() {
                break;
            }
        }
    });

    // Heartbeat disabled for tests.
    let _ = cq_transport::heartbeat::spawn(
        session.id.clone(),
        session.tx.clone(),
        session.last_inbound_ms.clone(),
        session.codec.clone(),
        HeartbeatConfig::DISABLED,
    );

    let mut buf = BytesMut::with_capacity(8192);
    loop {
        match read_half.read_buf(&mut buf).await {
            Ok(0) => break,
            Ok(_) => session.touch_inbound(),
            Err(_) => break,
        }
        loop {
            match decode_frame(&mut buf)? {
                Some(payload) => {
                    match serde_json::from_slice::<CqMessage>(&payload) {
                        Ok(cq_msg) => dispatch(&mut session, cq_msg, &ctx),
                        Err(_) => continue,
                    }
                }
                None => break,
            }
        }
    }
    cleanup_session(&mut session, &ctx);
    writer.abort();
    Ok(())
}

#[tokio::test]
async fn rust_sdk_publish_and_subscribe_roundtrip() {
    let (addr, _topics) = spawn_test_server().await;
    let client = Client::connect_with(&addr, ClientConfig::default()).await.unwrap();

    // 1. Seed publish to drive schema discovery.
    let seq = client
        .publish("/sdk-trades", json!({"symbol":"SEED","price":1.0}))
        .await
        .unwrap();
    assert!(seq >= 1);

    // 2. Subscribe with a filter.
    let mut sub = client
        .sow_and_subscribe("/sdk-trades", Some("price > 100"), None)
        .await
        .unwrap();

    // 3. Publish a matching row → expect ADD delta.
    let pub_seq = client
        .publish("/sdk-trades", json!({"symbol":"AAPL","price":150.0}))
        .await
        .unwrap();
    let delta = tokio::time::timeout(Duration::from_secs(2), sub.next_delta())
        .await
        .expect("delta should arrive")
        .expect("Some delta");
    assert_eq!(delta.delta_type, DeltaKind::Add);
    assert_eq!(delta.data.get("symbol").unwrap(), "AAPL");
    assert_eq!(delta.sequence, Some(pub_seq));
    assert_eq!(sub.last_sequence(), pub_seq);

    // 4. SOW query returns the snapshot.
    let rows = client.sow("/sdk-trades", None).await.unwrap();
    assert_eq!(rows.len(), 2); // SEED + AAPL

    client.unsubscribe(&sub.sub_id).await.unwrap();
}

#[tokio::test]
async fn rust_sdk_queue_consumes_published_messages() {
    let (addr, _) = spawn_test_server().await;
    let client = Client::connect(&addr).await.unwrap();

    let mut sub = client.subscribe("/sdk-work", None).await.unwrap();

    // Publish two work items via a separate client.
    let producer = Client::connect(&addr).await.unwrap();
    producer.publish("/sdk-work", json!({"job": 1})).await.unwrap();
    producer.publish("/sdk-work", json!({"job": 2})).await.unwrap();

    let mut jobs = Vec::new();
    for _ in 0..2 {
        let d = tokio::time::timeout(Duration::from_secs(2), sub.next_delta())
            .await
            .unwrap()
            .unwrap();
        jobs.push(d.data.get("job").and_then(|v| v.as_u64()).unwrap());
    }
    jobs.sort();
    assert_eq!(jobs, vec![1, 2]);
}

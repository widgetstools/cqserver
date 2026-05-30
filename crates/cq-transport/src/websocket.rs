//! WebSocket transport using tokio-tungstenite.

use crate::auth::SharedAuth;
use crate::heartbeat::{self, HeartbeatConfig};
use crate::queue::QueueRegistry;
use crate::router::{cleanup_session, dispatch, RouterContext};
use crate::session::{
    OutboundFrame, Session, SessionRegistry, DEFAULT_OUTBOUND_QUEUE_CAPACITY,
    DEFAULT_SOW_BATCH_SIZE,
};
use cq_core::topic::SharedTopic;
use cq_protocol::message::CqMessage;
use cq_protocol::serialization::Codec;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Clone)]
pub struct WsConfig {
    pub listen_addr: String,
    pub path: String,
    pub outbound_queue_capacity: usize,
    pub sow_batch_size: usize,
    /// Shared MOST_RECENT bookmark store. See `TcpConfig::bookmark_store`.
    pub bookmark_store: Option<crate::router::BookmarkStore>,
    /// S21 spillover configuration. See `TcpConfig::spillover`.
    pub spillover: Option<crate::router::SpilloverContext>,
    /// Replica-reads S1. When `true`, publish + delta_publish are
    /// rejected with a `read-only follower` error. Set by main.rs
    /// when `ServerConfig.replication.role == Standby`.
    pub read_only: bool,
    /// Query Guardrails (G1+) structural limits. Defaults to
    /// `cq_core::query::QueryLimits::default()`.
    pub query_limits: cq_core::query::QueryLimits,
    /// Maximum inbound WebSocket message/frame size in bytes. Caps both
    /// `max_message_size` and `max_frame_size` so a single client cannot
    /// force unbounded buffering during the handshake/read path. Defaults
    /// to 16 MiB to match the TCP codec's `cq_protocol::codec::MAX_FRAME_SIZE`.
    pub max_message_bytes: usize,
    /// S11 synchronous-replication policy for `AckType::Replicated`
    /// publishes. See [`crate::router::SyncReplication`].
    pub sync_replication: crate::router::SyncReplication,
}

impl Default for WsConfig {
    fn default() -> Self {
        WsConfig {
            listen_addr: "0.0.0.0:9008".into(),
            path: "/cq/json".into(),
            outbound_queue_capacity: DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: None,
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            max_message_bytes: cq_protocol::codec::MAX_FRAME_SIZE,
            sync_replication: crate::router::SyncReplication::default(),
        }
    }
}

pub async fn start_ws_server(
    config: WsConfig,
    topics: Arc<DashMap<String, SharedTopic>>,
    registry: SessionRegistry,
    queues: QueueRegistry,
    heartbeat: HeartbeatConfig,
    auth: SharedAuth,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&config.listen_addr).await?;
    info!(addr = %config.listen_addr, "WebSocket server listening");

    let bookmark_store = config
        .bookmark_store
        .clone()
        .unwrap_or_else(crate::router::new_bookmark_store);

    loop {
        let (stream, addr) = listener.accept().await?;
        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: queues.clone(),
            auth: auth.clone(),
            sow_batch_size: config.sow_batch_size,
            bookmark_store: bookmark_store.clone(),
            spillover: config.spillover.clone(),
            read_only: config.read_only,
            query_limits: config.query_limits,
            sync_replication: config.sync_replication,
        };
        let queue_capacity = config.outbound_queue_capacity;
        let max_message_bytes = config.max_message_bytes;

        tokio::spawn(async move {
            let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
                max_message_size: Some(max_message_bytes),
                max_frame_size: Some(max_message_bytes),
                ..Default::default()
            };
            let ws = match tokio_tungstenite::accept_async_with_config(stream, Some(ws_config)).await
            {
                Ok(ws) => ws,
                Err(e) => {
                    warn!(addr = %addr, error = %e, "WebSocket handshake failed");
                    return;
                }
            };

            info!(addr = %addr, "WebSocket client connected");
            metrics::gauge!("cq_connections_active").increment(1.0);
            handle_ws_connection(ws, addr, ctx, heartbeat, queue_capacity).await;
            metrics::gauge!("cq_connections_active").decrement(1.0);
            info!(addr = %addr, "WebSocket client disconnected");
        });
    }
}

async fn handle_ws_connection(
    ws: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    addr: SocketAddr,
    ctx: RouterContext,
    heartbeat_cfg: HeartbeatConfig,
    queue_capacity: usize,
) {
    let (mut ws_tx, mut ws_rx) = ws.split();
    let (tx, mut rx) = mpsc::channel::<OutboundFrame>(queue_capacity);
    let mut session = Session::new(addr.to_string(), tx);

    let write_handle = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            let ws_msg = match frame {
                OutboundFrame::Text(s) => tokio_tungstenite::tungstenite::Message::Text(s.into()),
                OutboundFrame::Binary(b) => {
                    tokio_tungstenite::tungstenite::Message::Binary(b.into())
                }
            };
            if let Err(e) = ws_tx.send(ws_msg).await {
                warn!(error = %e, "Failed to send WebSocket message");
                break;
            }
        }
    });

    let cancel = heartbeat::spawn(
        session.id.clone(),
        session.tx.clone(),
        session.last_inbound_ms.clone(),
        session.codec.clone(),
        heartbeat_cfg,
    );

    // AMPS-parity slow-consumer disconnect: the delivery path notifies this
    // when a subscription's outbound capacity is exhausted, so we close the
    // connection instead of silently dropping frames.
    let disconnect = session.disconnect.clone();

    loop {
        tokio::select! {
            biased;
            _ = cancel.notified() => {
                info!(session = %session.id, "Idle timeout — disconnecting");
                break;
            }
            _ = disconnect.notified() => {
                warn!(session = %session.id, "Slow consumer capacity exceeded — disconnecting");
                break;
            }
            maybe_msg = ws_rx.next() => {
                let msg = match maybe_msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        warn!(error = %e, "WebSocket read error");
                        break;
                    }
                    None => break,
                };
                session.touch_inbound();
                // Pick codec from frame type. Binary frames imply
                // MessagePack; text frames imply JSON. Once set, future
                // outbound traffic on this session follows suit.
                let (bytes_opt, decoded_codec) =
                    match msg {
                        tokio_tungstenite::tungstenite::Message::Text(t) => {
                            (Some(t.as_bytes().to_vec()), Codec::Json)
                        }
                        tokio_tungstenite::tungstenite::Message::Binary(b) => {
                            (Some(b.to_vec()), Codec::MessagePack)
                        }
                        _ => (None, session.codec()),
                    };
                let Some(bytes) = bytes_opt else { continue };
                session.set_codec(decoded_codec);
                match decoded_codec.decode(&bytes) {
                    Ok(cq_msg) => dispatch(&mut session, cq_msg, &ctx),
                    Err(e) => {
                        let _ = session.send_message(&CqMessage::error(
                            None,
                            &format!("Invalid message: {}", e),
                        ));
                    }
                }
            }
        }
    }

    cleanup_session(&mut session, &ctx);
    write_handle.abort();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delivery::{deliver_delta, spawn_evaluator};
    use crate::session::{new_registry, DeliveryRoute};
    use compact_str::CompactString;
    use cq_core::schema::{ColumnType, Schema};
    use cq_core::store::Value;
    use cq_core::subscription::{Delta, DeltaType};
    use cq_core::topic::{Topic, TopicConfig};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn build_topic() -> Topic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "desk"],
            &[ColumnType::String, ColumnType::Double, ColumnType::String],
        ));
        Topic::new(
            TopicConfig {
                name: "/trades".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
            expire_seconds: None,
            },
            schema,
            32,
        )
    }

    #[tokio::test]
    async fn delta_delivery_end_to_end() {
        let registry = new_registry();
        let topic = build_topic();

        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(64);
        let sub_id = "sess-1:sub-1".to_string();
        topic
            .subscribe(sub_id.clone(), "SELECT * FROM t WHERE desk = 'RATES'")
            .unwrap();
        registry.insert(
            sub_id.clone(),
            DeliveryRoute::new(tx.clone(), "/trades".into()),
        );

        let row = topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        for d in topic.evaluate_row(row, topic.current_sequence()) {
            deliver_delta(&d, &registry);
        }

        let received = rx.recv().await.expect("ADD delta should be delivered");
        let msg: CqMessage = serde_json::from_slice(received.as_bytes()).unwrap();
        assert_eq!(msg.sub_id.as_deref(), Some(sub_id.as_str()));
        assert_eq!(msg.delta_type.as_deref(), Some("add"));
    }

    #[tokio::test]
    async fn backpressure_drops_when_queue_full() {
        let registry = new_registry();
        let sub_id = "sub-slow".to_string();

        let (tx, _rx_never_drained) = mpsc::channel::<OutboundFrame>(2);
        let route = DeliveryRoute::new(tx, "/trades".into());
        let dropped_counter = route.dropped.clone();
        registry.insert(sub_id.clone(), route);

        let make_delta = || Delta {
            subscription_id: sub_id.as_str().into(),
            delta_type: DeltaType::Add,
            row: 0,
            sequence: 0,
            row_data: std::sync::Arc::new(serde_json::Map::new()),
            encoded_body_json: None,
        };

        deliver_delta(&make_delta(), &registry);
        deliver_delta(&make_delta(), &registry);
        for _ in 0..100 {
            deliver_delta(&make_delta(), &registry);
        }
        assert_eq!(dropped_counter.load(Ordering::Relaxed), 100);
    }

    #[tokio::test]
    async fn conflation_coalesces_same_row_updates() {
        let registry = new_registry();
        let sub_id = "sub-conf".to_string();
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(64);

        let route = DeliveryRoute::with_conflation(
            tx,
            "/trades".into(),
            sub_id.clone(),
            Duration::from_millis(50),
        );
        registry.insert(sub_id.clone(), route);

        for i in 0..5 {
            let delta = Delta {
                subscription_id: sub_id.as_str().into(),
                delta_type: if i == 0 {
                    DeltaType::Add
                } else {
                    DeltaType::Update
                },
                row: 0,
                sequence: i as u64 + 1,
                row_data: std::sync::Arc::new({
                    let mut m = serde_json::Map::new();
                    m.insert("v".into(), serde_json::Value::from(i));
                    m
                }),
                encoded_body_json: None,
            };
            deliver_delta(&delta, &registry);
        }

        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("flush should have fired")
            .unwrap();
        let msg: CqMessage = serde_json::from_slice(received.as_bytes()).unwrap();
        assert_eq!(msg.delta_type.as_deref(), Some("add"));
        assert_eq!(msg.data.unwrap().get("v").unwrap(), 4);
    }

    /// When a conflated route receives a delta through
    /// `deliver_delta_cached`, the evaluator pre-encodes the body and
    /// attaches it to the delta. The conflator's flush loop must use
    /// the attached body rather than re-serializing — verified here
    /// by sending a delta with a custom raw-bytes payload that
    /// `serde_json::to_vec` would not produce, then checking the
    /// flushed frame embeds those exact bytes.
    #[tokio::test]
    async fn conflator_flush_uses_pre_encoded_body() {
        let registry = new_registry();
        let sub_id = "sub-precoded".to_string();
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(64);

        let route = DeliveryRoute::with_conflation(
            tx,
            "/trades".into(),
            sub_id.clone(),
            Duration::from_millis(40),
        );
        registry.insert(sub_id.clone(), route);

        // The map and the pre-encoded bytes disagree: the map says
        // {"v":1}, but the body claims {"sentinel":"used"}. If the
        // flush re-serializes from `row_data`, we'd see "v":1; if it
        // uses the attached body, we'll see the sentinel. This proves
        // the flush is honoring `encoded_body_json`.
        let row_data = {
            let mut m = serde_json::Map::new();
            m.insert("v".into(), serde_json::Value::from(1));
            std::sync::Arc::new(m)
        };
        let pre_encoded = std::sync::Arc::new(b"{\"sentinel\":\"used\"}".to_vec());

        let delta = Delta {
            subscription_id: sub_id.as_str().into(),
            delta_type: DeltaType::Add,
            row: 0,
            sequence: 1,
            row_data,
            encoded_body_json: Some(pre_encoded.clone()),
        };
        // Submit directly to the route's conflator — bypasses
        // deliver_delta so we control exactly what's stored.
        let route = registry.get(&sub_id).unwrap();
        route.conflator.as_ref().unwrap().submit(delta);
        drop(route);

        let received = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("flush should have fired")
            .unwrap();
        let txt = match &received {
            OutboundFrame::Text(s) => s.clone(),
            OutboundFrame::Binary(_) => panic!("expected text frame for JSON codec"),
        };
        assert!(
            txt.contains("\"d\":{\"sentinel\":\"used\"}"),
            "flushed frame should embed pre-encoded body verbatim, got: {txt}"
        );
        assert!(
            !txt.contains("\"v\":1"),
            "flushed frame must NOT re-serialize row_data, got: {txt}"
        );
    }

    #[tokio::test]
    async fn evaluator_loop_delivers_async() {
        let registry = new_registry();
        let topic: SharedTopic = Arc::new(build_topic());

        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(64);
        let sub_id = "sess-2:sub-1".to_string();
        topic
            .subscribe(sub_id.clone(), "SELECT * FROM t WHERE desk = 'RATES'")
            .unwrap();
        registry.insert(sub_id.clone(), DeliveryRoute::new(tx, "/trades".into()));

        let mut_rx = topic.take_mutation_rx().unwrap();
        let _handle = spawn_evaluator(topic.clone(), mut_rx, registry.clone());

        topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::String(Some(CompactString::new("RATES"))),
        ]);

        let received = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("evaluator should deliver within 500ms")
            .expect("frame");
        let msg: CqMessage = serde_json::from_slice(received.as_bytes()).unwrap();
        assert_eq!(msg.delta_type.as_deref(), Some("add"));
        drop(topic);
    }

    #[test]
    fn ws_default_caps_message_at_tcp_frame_size() {
        assert_eq!(
            WsConfig::default().max_message_bytes,
            cq_protocol::codec::MAX_FRAME_SIZE
        );
    }

    // A client message larger than the negotiated cap must be rejected by
    // the server read path rather than buffered. This drives both WS roles
    // over an in-memory duplex so it needs no sockets or full server setup.
    #[tokio::test]
    async fn oversized_inbound_message_is_rejected() {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

        const CAP: usize = 1024;
        let (server_io, client_io) = tokio::io::duplex(256 * 1024);

        let server = tokio::spawn(async move {
            let cfg = WebSocketConfig {
                max_message_size: Some(CAP),
                max_frame_size: Some(CAP),
                ..Default::default()
            };
            let mut ws = tokio_tungstenite::accept_async_with_config(server_io, Some(cfg))
                .await
                .expect("server handshake");
            // First read should surface the oversized frame as an error.
            ws.next().await
        });

        let (mut client, _resp) =
            tokio_tungstenite::client_async("ws://localhost/cq/json", client_io)
                .await
                .expect("client handshake");

        client
            .send(Message::Binary(vec![0u8; CAP * 4].into()))
            .await
            .expect("client send");
        let _ = client.flush().await;

        let server_result = server.await.expect("server task join");
        match server_result {
            Some(Err(_)) => {} // capacity exceeded → protocol error, as required
            other => panic!("expected oversized read to error, got {other:?}"),
        }
    }
}

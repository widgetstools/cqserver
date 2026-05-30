//! TCP transport with binary length-prefixed framing.
//!
//! Wire format: `[length: u32 BE][payload: JSON]`. Each payload is a
//! `CqMessage`. The handler shares the router with the WebSocket transport,
//! so every command behaves identically across both transports.
//!
//! Optional TLS: when `TcpConfig::tls_acceptor` is set, the listener
//! wraps every accepted `TcpStream` in a `tokio_rustls::TlsAcceptor`
//! before handing it to the connection handler — which is generic over
//! the underlying stream type, so the same code path serves both
//! plain TCP and TLS clients.

use crate::auth::SharedAuth;
use crate::heartbeat::{self, HeartbeatConfig};
use crate::queue::QueueRegistry;
use crate::router::{cleanup_session, dispatch, RouterContext};
use crate::session::{
    OutboundFrame, Session, SessionRegistry, DEFAULT_OUTBOUND_QUEUE_CAPACITY,
    DEFAULT_SOW_BATCH_SIZE,
};
use bytes::BytesMut;
use cq_core::topic::SharedTopic;
use cq_protocol::codec::decode_frame;
#[cfg(test)]
use cq_protocol::codec::encode_frame;
use cq_protocol::message::CqMessage;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
#[cfg(test)]
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct TcpConfig {
    pub listen_addr: String,
    pub outbound_queue_capacity: usize,
    pub sow_batch_size: usize,
    /// When `Some`, every accepted connection is wrapped in a TLS
    /// handshake before any application traffic flows. The acceptor is
    /// constructed once at startup and shared across all sessions.
    pub tls_acceptor: Option<TlsAcceptor>,
    /// Shared MOST_RECENT bookmark store. Created once per server
    /// (not per connection) so a client that reconnects with the
    /// same `client_name` finds its previous high-water in the same
    /// map. `None` means "instantiate a fresh per-server store at
    /// startup"; callers that want to share a store across multiple
    /// listeners (e.g. TCP + WebSocket on the same server) can set
    /// it explicitly.
    pub bookmark_store: Option<crate::router::BookmarkStore>,
    /// S21 spillover configuration. When `Some(_)`, every subscription
    /// the router creates is backed by a per-route disk overflow log
    /// + drain task; queue-full events spill to disk instead of
    /// dropping. `None` keeps the legacy "drop on full" behaviour.
    pub spillover: Option<crate::router::SpilloverContext>,
    /// Replica-reads S1. When `true`, publish + delta_publish are
    /// rejected with a `read-only follower` error. Set by main.rs
    /// when `ServerConfig.replication.role == Standby`.
    pub read_only: bool,
    /// Query Guardrails (G1+) structural limits. Defaults to
    /// `cq_core::query::QueryLimits::default()`.
    pub query_limits: cq_core::query::QueryLimits,
    /// S11 synchronous-replication policy for `AckType::Replicated`
    /// publishes. See [`crate::router::SyncReplication`].
    pub sync_replication: crate::router::SyncReplication,
}

impl Default for TcpConfig {
    fn default() -> Self {
        TcpConfig {
            listen_addr: "0.0.0.0:9007".into(),
            outbound_queue_capacity: DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            tls_acceptor: None,
            bookmark_store: None,
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        }
    }
}

pub async fn start_tcp_server(
    config: TcpConfig,
    topics: Arc<DashMap<String, SharedTopic>>,
    registry: SessionRegistry,
    queues: QueueRegistry,
    heartbeat: HeartbeatConfig,
    auth: SharedAuth,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(&config.listen_addr).await?;
    info!(
        addr = %config.listen_addr,
        tls = config.tls_acceptor.is_some(),
        "TCP server listening"
    );

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
        let tls_acceptor = config.tls_acceptor.clone();

        tokio::spawn(async move {
            info!(addr = %addr, "TCP client connected");
            metrics::gauge!("cq_connections_active").increment(1.0);
            match tls_acceptor {
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        handle_tcp_connection(
                            tls_stream,
                            addr.to_string(),
                            ctx,
                            heartbeat,
                            queue_capacity,
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(addr = %addr, error = %e, "TLS handshake failed");
                        metrics::counter!("cq_tls_handshake_errors_total").increment(1);
                    }
                },
                None => {
                    handle_tcp_connection(stream, addr.to_string(), ctx, heartbeat, queue_capacity)
                        .await;
                }
            }
            metrics::gauge!("cq_connections_active").decrement(1.0);
            info!(addr = %addr, "TCP client disconnected");
        });
    }
}

async fn handle_tcp_connection<S>(
    stream: S,
    remote: String,
    ctx: RouterContext,
    heartbeat_cfg: HeartbeatConfig,
    queue_capacity: usize,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (tx, mut rx) = mpsc::channel::<OutboundFrame>(queue_capacity);
    let mut session = Session::new(remote, tx);

    // S27: clone the session's compression handle so the write task
    // can read the currently-negotiated compression on every frame
    // without holding the Session lock. Atomic load is cheap.
    let compression_slot = session.compression.clone();
    let write_handle = tokio::spawn(async move {
        let mut out = BytesMut::new();
        while let Some(frame) = rx.recv().await {
            out.clear();
            let compression = crate::session::compression_from_u8(
                compression_slot.load(std::sync::atomic::Ordering::Acquire),
            );
            cq_protocol::codec::encode_frame_with(
                frame.as_bytes(),
                &mut out,
                compression,
            );
            if let Err(e) = write_half.write_all(&out).await {
                warn!(error = %e, "TCP write error");
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

    let mut buf = BytesMut::with_capacity(8192);
    'outer: loop {
        tokio::select! {
            biased;
            _ = cancel.notified() => {
                info!(session = %session.id, "Idle timeout — disconnecting");
                break 'outer;
            }
            read_result = read_half.read_buf(&mut buf) => {
                match read_result {
                    Ok(0) => break 'outer,
                    Ok(_) => {
                        session.touch_inbound();
                    }
                    Err(e) => {
                        warn!(error = %e, "TCP read error");
                        break 'outer;
                    }
                }

                loop {
                    match decode_frame(&mut buf) {
                        Ok(Some(payload)) => {
                            // S30 — decode with the session's negotiated
                            // codec. The Logon frame arrives in JSON (the
                            // default), and handle_logon switches the
                            // codec during dispatch, so subsequent frames
                            // are decoded with the negotiated codec.
                            match session.codec().decode(&payload) {
                                Ok(cq_msg) => dispatch(&mut session, cq_msg, &ctx),
                                Err(e) => {
                                    let _ = session.send_message(&CqMessage::error(
                                        None,
                                        &format!("Invalid message: {}", e),
                                    ));
                                }
                            }
                        }
                        Ok(None) => break, // need more bytes; loop back to outer select
                        Err(e) => {
                            error!(error = %e, "TCP frame decode error");
                            cleanup_session(&mut session, &ctx);
                            write_handle.abort();
                            return;
                        }
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
    use crate::session::new_registry;
    use cq_core::schema::{ColumnType, Schema};
    use cq_core::topic::{Topic, TopicConfig};
    use cq_protocol::command::Command;
    use std::sync::Arc;
    use std::time::Duration;

    /// Spin up the TCP server on an ephemeral port and run a publish →
    /// sow_and_subscribe → publish → expect ADD delta round-trip.
    #[tokio::test]
    async fn tcp_publish_subscribe_roundtrip() {
        // Set up topic registry.
        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/t".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
            expire_seconds: None,
            },
            schema,
            32,
        );
        let shared: SharedTopic = Arc::new(topic);
        let mut_rx = shared.take_mutation_rx().unwrap();

        let registry = new_registry();
        let _evaluator =
            crate::delivery::spawn_evaluator(shared.clone(), mut_rx, registry.clone());

        topics.insert("/t".into(), shared);

        // Bind on ephemeral port and capture it.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Spawn an accept loop that mirrors start_tcp_server.
        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_tcp_connection(
                stream,
                peer.to_string(),
                ctx,
                HeartbeatConfig::DISABLED,
                DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            )
            .await;
        });

        // Client.
        let client = TcpStream::connect(addr).await.unwrap();
        let (mut cr, mut cw) = client.into_split();

        // Frame writer helper.
        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, msg: &CqMessage) {
            let json = serde_json::to_vec(msg).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }

        // Frame reader helper.
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(payload)) => return serde_json::from_slice(&payload).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode error: {}", e),
                }
            }
        }

        let mut rbuf = BytesMut::with_capacity(4096);

        // 1. Subscribe to /t WHERE price > 100.
        let mut sub_msg = CqMessage::new(Command::SowAndSubscribe);
        sub_msg.topic = Some("/t".into());
        sub_msg.filter = Some("price > 100".into());
        sub_msg.command_id = Some("c1".into());
        write_msg(&mut cw, &sub_msg).await;

        // Expect: ack with sub_id, then group_begin, group_end (empty snapshot).
        let ack = read_msg(&mut cr, &mut rbuf).await;
        assert_eq!(ack.command, Command::Ack);
        let sub_id = ack.sub_id.clone().expect("ack must carry sub_id");
        let gb = read_msg(&mut cr, &mut rbuf).await;
        assert_eq!(gb.command, Command::GroupBegin);
        let ge = read_msg(&mut cr, &mut rbuf).await;
        assert_eq!(ge.command, Command::GroupEnd);

        // 2. Publish AAPL price=150 → expect ack and an ADD delta.
        let mut pub_msg = CqMessage::new(Command::Publish);
        pub_msg.topic = Some("/t".into());
        pub_msg.command_id = Some("c2".into());
        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("price".into(), 150.0.into());
        pub_msg.data = Some(serde_json::Value::Object(data));
        write_msg(&mut cw, &pub_msg).await;

        // The ack and the delta arrive in some order; collect 2 messages.
        let mut got_ack = false;
        let mut got_add = false;
        for _ in 0..2 {
            let m = tokio::time::timeout(
                Duration::from_millis(1000),
                read_msg(&mut cr, &mut rbuf),
            )
            .await
            .expect("response should arrive within 1s");
            match m.command {
                Command::Ack => got_ack = true,
                Command::Publish => {
                    // Delta wraps in Command::Publish per protocol.
                    assert_eq!(m.sub_id.as_deref(), Some(sub_id.as_str()));
                    assert_eq!(m.delta_type.as_deref(), Some("add"));
                    got_add = true;
                }
                other => panic!("unexpected command: {:?}", other),
            }
        }
        assert!(got_ack, "expected publish ack");
        assert!(got_add, "expected ADD delta");

        drop(cw);
        drop(cr);
        let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
    }

    /// Replica-reads S1: when the RouterContext is in read-only mode, a
    /// publish must be rejected with a clear "read-only follower" error
    /// regardless of topic validity. We don't even need a topic to exist
    /// — the guard fires before topic lookup. Verifies the same for
    /// delta_publish (which routes through the same inner handler with
    /// `delta_mode = true`).
    #[tokio::test]
    async fn tcp_read_only_rejects_publish() {
        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let registry = new_registry();

        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/t".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: None,
            },
            schema,
            32,
        );
        topics.insert("/t".into(), Arc::new(topic));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: true,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_tcp_connection(
                stream,
                peer.to_string(),
                ctx,
                HeartbeatConfig::DISABLED,
                DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            )
            .await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut cr, mut cw) = client.into_split();

        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, msg: &CqMessage) {
            let json = serde_json::to_vec(msg).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(payload)) => return serde_json::from_slice(&payload).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode error: {}", e),
                }
            }
        }
        let mut rbuf = BytesMut::with_capacity(4096);

        // Plain publish — should be rejected.
        let mut pub_msg = CqMessage::new(Command::Publish);
        pub_msg.topic = Some("/t".into());
        pub_msg.command_id = Some("p1".into());
        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("price".into(), 150.0.into());
        pub_msg.data = Some(serde_json::Value::Object(data));
        write_msg(&mut cw, &pub_msg).await;

        // Error responses use Command::Ack + Status::Error + reason field
        // (see CqMessage::error in cq-protocol).
        let m = tokio::time::timeout(Duration::from_secs(1), read_msg(&mut cr, &mut rbuf))
            .await
            .expect("response must arrive");
        assert_eq!(m.command, Command::Ack);
        assert_eq!(m.status, Some(cq_protocol::command::Status::Error));
        assert_eq!(m.command_id.as_deref(), Some("p1"));
        let err_msg = m.reason.as_deref().unwrap_or("");
        assert!(
            err_msg.contains("read-only follower"),
            "expected read-only follower error, got {err_msg:?}"
        );

        // Delta publish — same guard fires.
        let mut delta_msg = CqMessage::new(Command::DeltaPublish);
        delta_msg.topic = Some("/t".into());
        delta_msg.command_id = Some("p2".into());
        let mut d = serde_json::Map::new();
        d.insert("symbol".into(), "AAPL".into());
        d.insert("price".into(), 151.0.into());
        delta_msg.data = Some(serde_json::Value::Object(d));
        write_msg(&mut cw, &delta_msg).await;

        let m2 = tokio::time::timeout(Duration::from_secs(1), read_msg(&mut cr, &mut rbuf))
            .await
            .expect("response must arrive");
        assert_eq!(m2.command, Command::Ack);
        assert_eq!(m2.status, Some(cq_protocol::command::Status::Error));
        assert_eq!(m2.command_id.as_deref(), Some("p2"));
        assert!(m2
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("read-only follower"));

        drop(cw);
        drop(cr);
        let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
    }

    /// Query Guardrails G4: the runtime SOW row cap fires when a
    /// snapshot exceeds the configured limit. The client receives a
    /// partial result (some rows), then an error frame naming the
    /// cap, then group_end. Verifies through the real TCP stack so
    /// the abort path's wire shape is exercised end-to-end.
    #[tokio::test]
    async fn tcp_g4_runtime_row_cap_aborts_sow() {
        use cq_protocol::command::Status;

        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/cap".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: None,
            },
            schema,
            128,
        );
        let shared: SharedTopic = Arc::new(topic);
        // Publish 50 rows directly (no router involved — we want SOW
        // input, not the publish pipeline).
        for i in 0..50 {
            let mut row = serde_json::Map::new();
            row.insert("symbol".into(), format!("SYM{i:04}").into());
            row.insert("price".into(), (100.0 + i as f64).into());
            shared.upsert_map(&row).unwrap();
        }
        topics.insert("/cap".into(), shared);

        let registry = new_registry();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Tight cap: abort after 10 rows on the wire.
        let mut limits = cq_core::query::QueryLimits::default();
        limits.hard_max_sow_result_rows = 10;
        // Disable the estimate-side hard cap so we ONLY exercise the
        // runtime cap path (otherwise G3 would reject before G4 fires).
        limits.max_sow_estimated_rows = 0;
        limits.max_sow_estimated_bytes = 0;

        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: 4, // small batch so the cap fires mid-stream
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: limits,
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_tcp_connection(
                stream,
                peer.to_string(),
                ctx,
                HeartbeatConfig::DISABLED,
                DEFAULT_OUTBOUND_QUEUE_CAPACITY,
            )
            .await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut cr, mut cw) = client.into_split();

        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, msg: &CqMessage) {
            let json = serde_json::to_vec(msg).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(payload)) => return serde_json::from_slice(&payload).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode error: {}", e),
                }
            }
        }

        let mut sow_msg = CqMessage::new(Command::SowAndSubscribe);
        sow_msg.topic = Some("/cap".into());
        sow_msg.command_id = Some("c1".into());
        write_msg(&mut cw, &sow_msg).await;

        let mut rbuf = BytesMut::with_capacity(8192);

        // Collect frames until we see either the runtime-cap error
        // (Ack/status=Error) or group_end. Both should appear before
        // we time out.
        let mut rows_received = 0usize;
        let mut saw_cap_error = false;
        let mut saw_group_end = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline && !(saw_cap_error && saw_group_end) {
            let m = tokio::time::timeout(
                Duration::from_millis(800),
                read_msg(&mut cr, &mut rbuf),
            )
            .await;
            let m = match m {
                Ok(m) => m,
                Err(_) => break,
            };
            match m.command {
                Command::Ack => {
                    if matches!(m.status, Some(Status::Error)) {
                        let reason = m.reason.unwrap_or_default();
                        if reason.contains("hard_max_sow_result_rows") {
                            saw_cap_error = true;
                        }
                    }
                }
                Command::SowBatch => {
                    if let Some(serde_json::Value::Array(rows)) = m.data {
                        rows_received += rows.len();
                    }
                }
                Command::GroupBegin => {}
                Command::GroupEnd => {
                    saw_group_end = true;
                }
                _ => {}
            }
        }

        assert!(saw_cap_error, "expected runtime-cap error frame");
        assert!(saw_group_end, "expected group_end after abort");
        assert!(
            rows_received <= 14, // 10 cap + 1 batch of size <= 4 already in flight
            "expected at most 14 rows on the wire, got {rows_received}"
        );

        drop(cw);
        drop(cr);
        let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
    }

    /// Verify the server pushes heartbeats on the configured interval and
    /// closes the connection after the idle timeout elapses.
    #[tokio::test]
    async fn tcp_heartbeat_pings_and_idle_disconnects() {
        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let registry = new_registry();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let hb = HeartbeatConfig {
            interval: Duration::from_millis(80),
            idle_timeout: Duration::from_millis(200),
        };
        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_tcp_connection(stream, peer.to_string(), ctx, hb, DEFAULT_OUTBOUND_QUEUE_CAPACITY).await;
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut cr, _cw) = client.into_split();
        let mut rbuf = BytesMut::with_capacity(4096);

        // Read one frame — the first server-initiated heartbeat.
        // Block up to 500ms (well past the 80ms interval).
        async fn read_frame(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> Option<CqMessage> {
            loop {
                match decode_frame(buf) {
                    Ok(Some(payload)) => {
                        return Some(serde_json::from_slice(&payload).ok()?)
                    }
                    Ok(None) => match r.read_buf(buf).await {
                        Ok(0) => return None,
                        Ok(_) => {}
                        Err(_) => return None,
                    },
                    Err(_) => return None,
                }
            }
        }

        let hb_msg = tokio::time::timeout(Duration::from_millis(500), read_frame(&mut cr, &mut rbuf))
            .await
            .expect("heartbeat should arrive within 500ms")
            .expect("frame");
        assert_eq!(hb_msg.command, Command::Heartbeat);

        // Don't write anything; the server should disconnect after idle.
        // Reading more should yield EOF within idle_timeout + slack.
        let result = tokio::time::timeout(
            Duration::from_millis(600),
            async {
                // Drain frames until the server closes.
                while read_frame(&mut cr, &mut rbuf).await.is_some() {}
            },
        )
        .await;
        assert!(
            result.is_ok(),
            "server should have closed the connection by now"
        );

        let _ = tokio::time::timeout(Duration::from_secs(1), server).await;
    }

    /// Bookmark replay: publish 3 records, disconnect, reconnect with a
    /// bookmark at the first record, expect entries 2 and 3 to arrive as
    /// historical deltas, then a fresh live publish to arrive as a 4th
    /// delta (proving the seam between replay and live).
    #[tokio::test]
    async fn tcp_bookmark_replay_resumes_from_sequence() {
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("topic.log");

        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut topic = Topic::new(
            TopicConfig {
                name: "/persist".into(),
                key_fields: vec!["symbol".into()],
                persist: true,
                conflation_ms: None,
                index_columns: vec![],
            expire_seconds: None,
            },
            schema,
            32,
        );
        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open(&log_path, FsyncPolicy::None).unwrap(),
        ));
        topic.attach_txlog(writer);
        let shared: SharedTopic = Arc::new(topic);
        let mut_rx = shared.take_mutation_rx().unwrap();
        let registry = new_registry();
        let _evaluator =
            crate::delivery::spawn_evaluator(shared.clone(), mut_rx, registry.clone());
        topics.insert("/persist".into(), shared);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let base_ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let ctx = base_ctx.clone();
                tokio::spawn(async move {
                    handle_tcp_connection(
                        stream,
                        peer.to_string(),
                        ctx,
                        HeartbeatConfig::DISABLED,
                        DEFAULT_OUTBOUND_QUEUE_CAPACITY,
                    )
                    .await;
                });
            }
        });

        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, m: &CqMessage) {
            let json = serde_json::to_vec(m).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(p)) => return serde_json::from_slice(&p).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode: {}", e),
                }
            }
        }

        // First connection: publish 3 rows; record their assigned sequences
        // from the publish acks.
        let c1 = TcpStream::connect(addr).await.unwrap();
        let (mut c1r, mut c1w) = c1.into_split();
        let mut buf1 = BytesMut::with_capacity(4096);

        let mut sequences = Vec::new();
        for (i, sym) in ["AAPL", "MSFT", "GOOGL"].iter().enumerate() {
            let mut m = CqMessage::new(Command::Publish);
            m.topic = Some("/persist".into());
            m.command_id = Some(format!("p{}", i));
            let mut data = serde_json::Map::new();
            data.insert("symbol".into(), serde_json::Value::String((*sym).into()));
            data.insert(
                "price".into(),
                serde_json::Value::from(100.0 + i as f64 * 10.0),
            );
            m.data = Some(serde_json::Value::Object(data));
            write_msg(&mut c1w, &m).await;
            let ack = read_msg(&mut c1r, &mut buf1).await;
            assert_eq!(ack.command, Command::Ack);
            sequences.push(ack.sequence.expect("ack must carry sequence"));
        }
        assert_eq!(sequences, vec![1, 2, 3]);

        drop(c1w);
        drop(c1r);

        // Wait briefly for the disconnect to propagate so the next
        // connection doesn't see stale state.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Second connection: subscribe with bookmark = 1 → expect MSFT
        // (seq 2) and GOOGL (seq 3) as replay deltas, then a live ADD
        // for a fresh publish.
        let c2 = TcpStream::connect(addr).await.unwrap();
        let (mut c2r, mut c2w) = c2.into_split();
        let mut buf2 = BytesMut::with_capacity(4096);

        let mut sub = CqMessage::new(Command::SowAndSubscribe);
        sub.topic = Some("/persist".into());
        sub.command_id = Some("s1".into());
        sub.bookmark = Some(1);
        write_msg(&mut c2w, &sub).await;

        let ack = read_msg(&mut c2r, &mut buf2).await;
        assert_eq!(ack.command, Command::Ack);
        let captured = ack.sequence.expect("ack must report captured seq");
        assert_eq!(captured, 3);
        let sub_id = ack.sub_id.clone().unwrap();

        // Receive replayed deltas. Order is not strictly guaranteed, but
        // their sequences must be {2, 3}.
        let mut seen: Vec<u64> = Vec::new();
        for _ in 0..2 {
            let m = tokio::time::timeout(
                Duration::from_secs(1),
                read_msg(&mut c2r, &mut buf2),
            )
            .await
            .expect("replay frame timeout");
            assert_eq!(m.command, Command::Publish);
            assert_eq!(m.sub_id.as_deref(), Some(sub_id.as_str()));
            assert_eq!(m.delta_type.as_deref(), Some("add"));
            seen.push(m.sequence.unwrap());
        }
        seen.sort();
        assert_eq!(seen, vec![2, 3]);

        // Now publish a live record on a third connection — must arrive
        // as seq 4 on the bookmarked subscriber.
        let c3 = TcpStream::connect(addr).await.unwrap();
        let (_c3r, mut c3w) = c3.into_split();
        let mut live = CqMessage::new(Command::Publish);
        live.topic = Some("/persist".into());
        live.command_id = Some("p_live".into());
        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), serde_json::Value::String("NVDA".into()));
        data.insert("price".into(), serde_json::Value::from(900.0));
        live.data = Some(serde_json::Value::Object(data));
        write_msg(&mut c3w, &live).await;

        let live_delta = tokio::time::timeout(
            Duration::from_secs(2),
            read_msg(&mut c2r, &mut buf2),
        )
        .await
        .expect("live delta should arrive after replay");
        assert_eq!(live_delta.command, Command::Publish);
        assert_eq!(live_delta.delta_type.as_deref(), Some("add"));
        assert_eq!(live_delta.sequence, Some(4));

        drop(c2w);
        drop(c2r);
        drop(c3w);
        server.abort();
    }

    /// End-to-end auth: unauthenticated publish is rejected; logon with
    /// valid creds is accepted; entitlement violations are rejected;
    /// allowed operations succeed after logon.
    #[tokio::test]
    async fn tcp_logon_and_entitlements_gate_commands() {
        use crate::auth::{AuthStore, User};

        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        for name in ["/market-data", "/orders"] {
            let t = Topic::new(
                TopicConfig {
                    name: name.into(),
                    key_fields: vec!["symbol".into()],
                    persist: false,
                    conflation_ms: None,
                    index_columns: vec![],
            expire_seconds: None,
                },
                schema.clone(),
                32,
            );
            let shared: SharedTopic = Arc::new(t);
            let _ = shared.take_mutation_rx(); // discard rx — no evaluator needed
            topics.insert(name.into(), shared);
        }
        let registry = new_registry();

        // Alice: publish to /market-data only.
        let hash = bcrypt::hash("s3cret", 4).unwrap();
        let alice = User::from_parts(
            "alice".into(),
            hash,
            &["publish:/market-data".into()],
        )
        .unwrap();
        let auth = std::sync::Arc::new(AuthStore::new(true, vec![alice]));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: crate::queue::new_queue_registry(),
            auth: auth.clone(),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            let (stream, peer) = listener.accept().await.unwrap();
            handle_tcp_connection(stream, peer.to_string(), ctx, HeartbeatConfig::DISABLED, DEFAULT_OUTBOUND_QUEUE_CAPACITY)
                .await;
        });

        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, m: &CqMessage) {
            let json = serde_json::to_vec(m).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(p)) => return serde_json::from_slice(&p).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode: {}", e),
                }
            }
        }

        let client = TcpStream::connect(addr).await.unwrap();
        let (mut cr, mut cw) = client.into_split();
        let mut buf = BytesMut::with_capacity(4096);

        // 1. Publish without logging in → error.
        let mut pre = CqMessage::new(Command::Publish);
        pre.topic = Some("/market-data".into());
        pre.command_id = Some("pre".into());
        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("price".into(), 1.0.into());
        pre.data = Some(serde_json::Value::Object(data));
        write_msg(&mut cw, &pre).await;
        let ack = read_msg(&mut cr, &mut buf).await;
        assert_eq!(ack.command, Command::Ack);
        assert_eq!(
            ack.status.as_ref().map(|s| format!("{:?}", s)),
            Some("Error".to_string())
        );
        assert!(ack
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("Not authenticated"));

        // 2. Logon with wrong password → error.
        let mut bad = CqMessage::new(Command::Logon);
        bad.command_id = Some("bad".into());
        let mut creds = serde_json::Map::new();
        creds.insert("user".into(), "alice".into());
        creds.insert("password".into(), "wrong".into());
        bad.data = Some(serde_json::Value::Object(creds));
        write_msg(&mut cw, &bad).await;
        let bad_ack = read_msg(&mut cr, &mut buf).await;
        assert!(bad_ack
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("Invalid credentials"));

        // 3. Logon with correct password → ok.
        let mut good = CqMessage::new(Command::Logon);
        good.command_id = Some("good".into());
        let mut creds = serde_json::Map::new();
        creds.insert("user".into(), "alice".into());
        creds.insert("password".into(), "s3cret".into());
        good.data = Some(serde_json::Value::Object(creds));
        write_msg(&mut cw, &good).await;
        let good_ack = read_msg(&mut cr, &mut buf).await;
        assert_eq!(good_ack.command, Command::Ack);
        assert_ne!(
            good_ack.status.as_ref().map(|s| format!("{:?}", s)),
            Some("Error".to_string()),
            "logon should succeed"
        );

        // 4. Publish to /orders (NOT entitled) → forbidden.
        let mut p_orders = CqMessage::new(Command::Publish);
        p_orders.topic = Some("/orders".into());
        p_orders.command_id = Some("po".into());
        let mut d = serde_json::Map::new();
        d.insert("symbol".into(), "X".into());
        d.insert("price".into(), 1.0.into());
        p_orders.data = Some(serde_json::Value::Object(d));
        write_msg(&mut cw, &p_orders).await;
        let forbidden = read_msg(&mut cr, &mut buf).await;
        assert!(forbidden
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("Forbidden"));

        // 5. Publish to /market-data (entitled) → ok.
        let mut p_md = CqMessage::new(Command::Publish);
        p_md.topic = Some("/market-data".into());
        p_md.command_id = Some("pmd".into());
        let mut d = serde_json::Map::new();
        d.insert("symbol".into(), "AAPL".into());
        d.insert("price".into(), 100.0.into());
        p_md.data = Some(serde_json::Value::Object(d));
        write_msg(&mut cw, &p_md).await;
        let ok = read_msg(&mut cr, &mut buf).await;
        assert_eq!(ok.command, Command::Ack);
        assert_ne!(
            ok.status.as_ref().map(|s| format!("{:?}", s)),
            Some("Error".to_string()),
            "publish to allowed topic should succeed"
        );

        drop(cw);
        drop(cr);
        server.abort();
    }

    /// Two clients subscribe to the same queue; six publishes go in;
    /// expect each client to receive exactly three (round-robin) and
    /// for the union to be the full sequence 1..=6.
    #[tokio::test]
    async fn tcp_queue_competing_consumers_split_messages() {
        use crate::queue::{new_queue_registry, Queue};

        let topics: Arc<DashMap<String, SharedTopic>> = Arc::new(DashMap::new());
        let registry = new_registry();
        let queues = new_queue_registry();
        queues.insert("/work".into(), Arc::new(Queue::new("/work")));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let base_ctx = RouterContext {
            topics: topics.clone(),
            sessions: registry.clone(),
            queues: queues.clone(),
            auth: std::sync::Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: DEFAULT_SOW_BATCH_SIZE,
            bookmark_store: crate::router::new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: crate::router::SyncReplication::default(),
        };
        let server = tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(x) => x,
                    Err(_) => return,
                };
                let ctx = base_ctx.clone();
                tokio::spawn(async move {
                    handle_tcp_connection(
                        stream,
                        peer.to_string(),
                        ctx,
                        HeartbeatConfig::DISABLED,
                        DEFAULT_OUTBOUND_QUEUE_CAPACITY,
                    )
                    .await;
                });
            }
        });

        async fn write_msg(w: &mut tokio::net::tcp::OwnedWriteHalf, m: &CqMessage) {
            let json = serde_json::to_vec(m).unwrap();
            let mut out = BytesMut::new();
            encode_frame(&json, &mut out);
            w.write_all(&out).await.unwrap();
        }
        async fn read_msg(
            r: &mut tokio::net::tcp::OwnedReadHalf,
            buf: &mut BytesMut,
        ) -> CqMessage {
            loop {
                match decode_frame(buf) {
                    Ok(Some(p)) => return serde_json::from_slice(&p).unwrap(),
                    Ok(None) => {
                        let n = r.read_buf(buf).await.unwrap();
                        assert!(n > 0, "unexpected EOF");
                    }
                    Err(e) => panic!("decode: {}", e),
                }
            }
        }

        // Consumer A
        let a = TcpStream::connect(addr).await.unwrap();
        let (mut a_r, mut a_w) = a.into_split();
        let mut a_buf = BytesMut::with_capacity(4096);
        let mut sub_a = CqMessage::new(Command::SowAndSubscribe);
        sub_a.topic = Some("/work".into());
        sub_a.command_id = Some("sa".into());
        write_msg(&mut a_w, &sub_a).await;
        let ack_a = read_msg(&mut a_r, &mut a_buf).await;
        assert_eq!(ack_a.command, Command::Ack);

        // Consumer B
        let b = TcpStream::connect(addr).await.unwrap();
        let (mut b_r, mut b_w) = b.into_split();
        let mut b_buf = BytesMut::with_capacity(4096);
        let mut sub_b = CqMessage::new(Command::SowAndSubscribe);
        sub_b.topic = Some("/work".into());
        sub_b.command_id = Some("sb".into());
        write_msg(&mut b_w, &sub_b).await;
        let ack_b = read_msg(&mut b_r, &mut b_buf).await;
        assert_eq!(ack_b.command, Command::Ack);

        // Give the server a moment to register both consumers.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Producer
        let p = TcpStream::connect(addr).await.unwrap();
        let (_p_r, mut p_w) = p.into_split();
        for i in 1..=6u64 {
            let mut m = CqMessage::new(Command::Publish);
            m.topic = Some("/work".into());
            m.command_id = Some(format!("p{}", i));
            let mut d = serde_json::Map::new();
            d.insert("i".into(), serde_json::Value::from(i));
            m.data = Some(serde_json::Value::Object(d));
            write_msg(&mut p_w, &m).await;
        }

        // Collect three from each consumer (5s total budget).
        let mut got_a: Vec<u64> = Vec::new();
        let mut got_b: Vec<u64> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while got_a.len() < 3 || got_b.len() < 3 {
            let remaining = deadline - tokio::time::Instant::now();
            tokio::select! {
                m = tokio::time::timeout(remaining, read_msg(&mut a_r, &mut a_buf)) => {
                    if let Ok(m) = m {
                        if m.command == Command::Publish {
                            got_a.push(m.data.unwrap()["i"].as_u64().unwrap());
                        }
                    } else { break; }
                }
                m = tokio::time::timeout(remaining, read_msg(&mut b_r, &mut b_buf)) => {
                    if let Ok(m) = m {
                        if m.command == Command::Publish {
                            got_b.push(m.data.unwrap()["i"].as_u64().unwrap());
                        }
                    } else { break; }
                }
            }
        }

        assert_eq!(got_a.len(), 3, "A should receive 3 messages: {:?}", got_a);
        assert_eq!(got_b.len(), 3, "B should receive 3 messages: {:?}", got_b);
        let mut union: Vec<u64> =
            got_a.iter().chain(got_b.iter()).copied().collect();
        union.sort();
        assert_eq!(union, vec![1, 2, 3, 4, 5, 6]);

        drop(a_w);
        drop(a_r);
        drop(b_w);
        drop(b_r);
        drop(p_w);
        server.abort();
    }
}

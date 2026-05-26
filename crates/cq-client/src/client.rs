//! Connection state + request/response matching.

use crate::error::{ClientError, ClientResult};
use crate::subscription::{Delta, DeltaKind, Subscription};
use crate::transport::Transport;
use cq_protocol::command::{Command, Status};
use cq_protocol::message::CqMessage;
use cq_protocol::serialization::Codec;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde_json::{Map, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub codec: Codec,
    /// Per-RPC ack timeout. None disables the timeout entirely.
    pub ack_timeout: Option<Duration>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            codec: Codec::Json,
            ack_timeout: Some(Duration::from_secs(30)),
        }
    }
}

/// Async cqserver client. Cloning shares the underlying socket.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    out_tx: mpsc::UnboundedSender<CqMessage>,
    pending: Arc<DashMap<String, oneshot::Sender<CqMessage>>>,
    subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>>,
    /// S28 — wire-protocol version this connection negotiated on its
    /// last successful `Logon`. Defaults to
    /// [`cq_protocol::version::DEFAULT_LEGACY_VERSION`] until
    /// negotiation completes (so unauthenticated tests that never
    /// call `logon` still get a sensible value). Exposed via
    /// [`Client::protocol_version`].
    protocol_version: Arc<std::sync::atomic::AtomicU32>,
    /// S27 — wire-compression algorithm this connection negotiated.
    /// Shared with the driver loop so frames are encoded with the
    /// current setting. Encoded as a `u8` for cheap atomic
    /// load/store (see helpers in `transport::compression_*`).
    compression: Arc<std::sync::atomic::AtomicU8>,
    /// S18 — optional persistent bookmark store. When set, every
    /// `Subscription` this client creates carries a handle so its
    /// `next_delta` records the high-water sequence per topic.
    bookmark_store: parking_lot::RwLock<Option<crate::bookmark::LocalBookmarkStore>>,
    /// S17 — optional persistent publish buffer. When set, every
    /// `publish` records into the store before sending, then drops
    /// the entry on a successful ack. A reconnect-after-crash can
    /// replay surviving entries via `replay_publish_store`.
    publish_store: parking_lot::RwLock<Option<crate::publish_store::LocalPublishStore>>,
    /// `cid → completion signal` for one-shot SOW (snapshot) responses.
    /// The driver fires `Ok(None)` when it sees the matching `GroupEnd`,
    /// or `Ok(Some(err))` when it sees an error ack — letting the awaiting
    /// `sow_msg` short-circuit instead of waiting for a group_end that
    /// will never arrive (P7 — AMPS_PARITY §4 bug 4 client-side analogue).
    snapshot_completions: Arc<DashMap<String, oneshot::Sender<Option<String>>>>,
    /// `cid → rows seen so far` for one-shot SOW. The driver appends
    /// each `sow_row` event keyed by sub_id == cid.
    snapshot_buffers: Arc<DashMap<String, Mutex<Vec<Map<String, Value>>>>>,
    /// `cid → pre-allocated subscriber tx`. Subscribe paths create the
    /// channel before sending, insert here. When the matching ack
    /// arrives the driver atomically moves the tx into `subs` keyed by
    /// the server-assigned sid — guaranteeing the routing map is
    /// populated before any subsequent SOW frame is processed.
    pending_subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>>,
    next_cid: AtomicU64,
    codec: Codec,
    ack_timeout: Option<Duration>,
}

impl Client {
    /// Connect to `url`. Schemes: `tcp://host:port` or `ws://host:port/path`.
    pub async fn connect(url: &str) -> ClientResult<Self> {
        Self::connect_with(url, ClientConfig::default()).await
    }

    pub async fn connect_with(url: &str, cfg: ClientConfig) -> ClientResult<Self> {
        let transport = if let Some(rest) = url.strip_prefix("tcp://") {
            Transport::connect_tcp(rest).await?
        } else if url.starts_with("ws://") || url.starts_with("wss://") {
            Transport::connect_ws(url).await?
        } else {
            return Err(ClientError::InvalidUrl(url.into()));
        };
        Ok(Self::spawn(transport, cfg))
    }

    /// Replica-reads S2: connect to any one of `urls`, trying them in
    /// a randomized order. The first successful connection wins; if
    /// they all fail, returns the last error.
    ///
    /// Randomization matters when many clients start simultaneously:
    /// without it every client would hammer the first URL and only
    /// fail over when it dies, defeating the load-spread purpose.
    /// The shuffle here is process-time-seeded — not cryptographically
    /// random but more than sufficient to spread N clients across M
    /// followers when N >> M.
    ///
    /// This is *initial-connect* failover only. Live reconnect after
    /// a mid-stream disconnect is a separate concern handled by the
    /// reconnect layer (see worklog S2b — not yet shipped).
    pub async fn connect_any(urls: &[&str]) -> ClientResult<Self> {
        Self::connect_any_with(urls, ClientConfig::default()).await
    }

    pub async fn connect_any_with(urls: &[&str], cfg: ClientConfig) -> ClientResult<Self> {
        if urls.is_empty() {
            return Err(ClientError::InvalidUrl(
                "connect_any: empty url list".into(),
            ));
        }
        let order = shuffled_indices(urls.len());
        let mut last_err: Option<ClientError> = None;
        for i in order {
            match Self::connect_with(urls[i], cfg.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    tracing::debug!(url = urls[i], error = %e, "connect_any: attempt failed");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.expect("urls non-empty so at least one attempt ran"))
    }

    /// Connect over TLS. `addr` is `host:port`; `server_name` is the
    /// SNI value the client presents and the cert is validated against;
    /// `client_config` is built via
    /// [`transport::tls_client_config_with_roots`] (production) or
    /// [`transport::tls_client_config_dangerous_no_verify`] (tests).
    pub async fn connect_tls(
        addr: &str,
        server_name: &str,
        client_config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
    ) -> ClientResult<Self> {
        Self::connect_tls_with(addr, server_name, client_config, ClientConfig::default()).await
    }

    pub async fn connect_tls_with(
        addr: &str,
        server_name: &str,
        client_config: std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
        cfg: ClientConfig,
    ) -> ClientResult<Self> {
        let transport = Transport::connect_tls(addr, server_name, client_config).await?;
        Ok(Self::spawn(transport, cfg))
    }

    fn spawn(transport: Transport, cfg: ClientConfig) -> Self {
        let (out_tx, out_rx) = mpsc::unbounded_channel::<CqMessage>();
        let pending: Arc<DashMap<String, oneshot::Sender<CqMessage>>> = Arc::new(DashMap::new());
        let subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>> = Arc::new(DashMap::new());
        let snapshot_completions: Arc<DashMap<String, oneshot::Sender<Option<String>>>> =
            Arc::new(DashMap::new());
        let snapshot_buffers: Arc<DashMap<String, Mutex<Vec<Map<String, Value>>>>> =
            Arc::new(DashMap::new());
        let pending_subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>> =
            Arc::new(DashMap::new());

        let compression = Arc::new(std::sync::atomic::AtomicU8::new(0)); // None
        let inner = Arc::new(ClientInner {
            out_tx,
            pending: pending.clone(),
            subs: subs.clone(),
            snapshot_completions: snapshot_completions.clone(),
            snapshot_buffers: snapshot_buffers.clone(),
            pending_subs: pending_subs.clone(),
            protocol_version: Arc::new(std::sync::atomic::AtomicU32::new(
                cq_protocol::version::DEFAULT_LEGACY_VERSION,
            )),
            compression: compression.clone(),
            bookmark_store: parking_lot::RwLock::new(None),
            publish_store: parking_lot::RwLock::new(None),
            next_cid: AtomicU64::new(1),
            codec: cfg.codec,
            ack_timeout: cfg.ack_timeout,
        });

        tokio::spawn(driver_loop(
            transport,
            out_rx,
            pending,
            subs,
            snapshot_completions,
            snapshot_buffers,
            pending_subs,
            cfg.codec,
            compression,
        ));
        Client { inner }
    }

    fn next_cid(&self) -> String {
        let n = self.inner.next_cid.fetch_add(1, Ordering::Relaxed);
        format!("c-{}", n)
    }

    async fn rpc(&self, mut msg: CqMessage) -> ClientResult<CqMessage> {
        let cid = self.next_cid();
        msg.command_id = Some(cid.clone());
        self.rpc_with_cid(msg, cid).await
    }

    /// Send an already-cid-stamped message and await its ack. Used by
    /// `subscribe_inner` which assigns the cid before this call so it
    /// can pre-register a subscription channel.
    async fn rpc_with_cid(&self, msg: CqMessage, cid: String) -> ClientResult<CqMessage> {
        let (tx, rx) = oneshot::channel();
        self.inner.pending.insert(cid.clone(), tx);

        if self.inner.out_tx.send(msg).is_err() {
            self.inner.pending.remove(&cid);
            return Err(ClientError::ConnectionClosed);
        }

        let resp = match self.inner.ack_timeout {
            Some(t) => match tokio::time::timeout(t, rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => return Err(ClientError::ConnectionClosed),
                Err(_) => {
                    self.inner.pending.remove(&cid);
                    return Err(ClientError::Timeout);
                }
            },
            None => rx.await.map_err(|_| ClientError::ConnectionClosed)?,
        };

        if resp.status == Some(Status::Error) {
            return Err(ClientError::Server(
                resp.reason.unwrap_or_else(|| "unknown".into()),
            ));
        }
        Ok(resp)
    }

    // ==================== Public API ====================

    pub async fn logon(&self, user: &str, password: &str) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Logon);
        let mut creds = Map::new();
        creds.insert("user".into(), Value::String(user.into()));
        creds.insert("password".into(), Value::String(password.into()));
        m.data = Some(Value::Object(creds));
        m.protocol_versions = Some(cq_protocol::version::SUPPORTED_VERSIONS.to_vec());
        m.compressions = Some(cq_protocol::compression::SUPPORTED_COMPRESSIONS.to_vec());
        let resp = self.rpc(m).await?;
        self.capture_negotiated(&resp);
        Ok(())
    }

    /// S16 — authenticate using a JWT bearer token instead of
    /// username + password. The server must have a JWT validator
    /// configured (`[auth.jwt]`); on success the user identity and
    /// entitlements come from the token's claims.
    pub async fn logon_jwt(&self, token: &str) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Logon);
        let mut creds = Map::new();
        creds.insert("token".into(), Value::String(token.into()));
        m.data = Some(Value::Object(creds));
        m.protocol_versions = Some(cq_protocol::version::SUPPORTED_VERSIONS.to_vec());
        m.compressions = Some(cq_protocol::compression::SUPPORTED_COMPRESSIONS.to_vec());
        let resp = self.rpc(m).await?;
        self.capture_negotiated(&resp);
        Ok(())
    }

    /// Send an anonymous version-negotiation handshake. Useful when
    /// `auth.required = false` on the server — the client still wants
    /// to know which protocol version is active. Returns the
    /// negotiated version.
    pub async fn handshake_protocol(&self) -> ClientResult<u32> {
        let mut m = CqMessage::new(Command::Logon);
        m.protocol_versions = Some(cq_protocol::version::SUPPORTED_VERSIONS.to_vec());
        m.compressions = Some(cq_protocol::compression::SUPPORTED_COMPRESSIONS.to_vec());
        let resp = self.rpc(m).await?;
        self.capture_negotiated(&resp);
        Ok(self.protocol_version())
    }

    /// Wire-protocol version this connection negotiated on its last
    /// successful Logon. Defaults to
    /// [`cq_protocol::version::DEFAULT_LEGACY_VERSION`] until a
    /// Logon (or `handshake_protocol`) completes.
    pub fn protocol_version(&self) -> u32 {
        self.inner
            .protocol_version
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Active wire compression algorithm for this connection.
    pub fn compression(&self) -> cq_protocol::compression::Compression {
        compression_from_u8(self.inner.compression.load(std::sync::atomic::Ordering::Acquire))
    }

    /// S18 — attach a persistent bookmark store to this Client.
    /// Subsequent `sow_and_subscribe` / `subscribe` calls that omit
    /// an explicit `bookmark` consult the store; deltas with a
    /// sequence are recorded back to the store as they arrive. Call
    /// [`crate::Subscription::persist_bookmark`] before disconnect
    /// (or shutdown) to flush the in-memory map to disk.
    pub fn set_bookmark_store(&self, store: crate::bookmark::LocalBookmarkStore) {
        *self.inner.bookmark_store.write() = Some(store);
    }

    /// Clone of the attached bookmark store, if any. Useful for tests
    /// that need to assert the recorded high-water from the outside.
    pub fn bookmark_store(&self) -> Option<crate::bookmark::LocalBookmarkStore> {
        self.inner.bookmark_store.read().clone()
    }

    /// S17 — attach a persistent publish buffer. Every subsequent
    /// `publish` records into the store before sending; the entry is
    /// dropped on a successful ack. After a reconnect, call
    /// [`Client::replay_publish_store`] to flush surviving entries
    /// to the server.
    pub fn set_publish_store(&self, store: crate::publish_store::LocalPublishStore) {
        *self.inner.publish_store.write() = Some(store);
    }

    /// Clone of the attached publish store, if any.
    pub fn publish_store(&self) -> Option<crate::publish_store::LocalPublishStore> {
        self.inner.publish_store.read().clone()
    }

    /// S17 — replay every pending entry in the publish store back
    /// through the wire. Used by callers that reconnect after a
    /// crash: each surviving entry is `publish`-ed again (which
    /// records + acks + completes through the normal path). Returns
    /// the number of entries replayed.
    pub async fn replay_publish_store(&self) -> ClientResult<usize> {
        let store = match self.inner.publish_store.read().clone() {
            Some(s) => s,
            None => return Ok(0),
        };
        let pending = store.pending();
        let n = pending.len();
        for entry in pending {
            // Each call goes through the normal `publish` which
            // records + completes via the store. To avoid duplicating
            // the entry under a new id, drop the original entry
            // first; if the publish fails the caller can persist +
            // retry — at-least-once.
            store.complete(entry.id);
            self.publish(&entry.topic, entry.data).await?;
        }
        Ok(n)
    }

    fn capture_negotiated(&self, resp: &CqMessage) {
        if let Some(versions) = resp.protocol_versions.as_ref() {
            if let Some(&v) = versions.first() {
                self.inner
                    .protocol_version
                    .store(v, std::sync::atomic::Ordering::Release);
            }
        }
        if let Some(comps) = resp.compressions.as_ref() {
            if let Some(&c) = comps.first() {
                self.inner
                    .compression
                    .store(compression_to_u8(c), std::sync::atomic::Ordering::Release);
            }
        }
    }

    /// Publish a JSON record. Returns the assigned monotonic sequence.
    pub async fn publish(&self, topic: &str, data: Value) -> ClientResult<u64> {
        // S17: when a publish store is attached, record the publish
        // before sending it. The id is purely client-local — used to
        // drop the entry from the store on a successful ack.
        let store_id = {
            let store = self.inner.publish_store.read();
            store.as_ref().map(|s| s.record(topic, data.clone()))
        };
        let mut m = CqMessage::new(Command::Publish);
        m.topic = Some(topic.into());
        m.data = Some(data);
        let resp = self.rpc(m).await?;
        if let Some(id) = store_id {
            if let Some(store) = self.inner.publish_store.read().as_ref() {
                store.complete(id);
            }
        }
        Ok(resp.sequence.unwrap_or(0))
    }

    /// Q2 — atomic batched publish. Sends every record in `rows` to
    /// `topic` in a single wire frame; the server commits them as
    /// one batched upsert and returns a single Ack carrying the
    /// per-row sequences in input order. Saves N-1 round-trips
    /// compared to the SDK-level pipelining of `publish` × N.
    ///
    /// Returns the assigned sequences in input order. Errors on the
    /// first per-row failure (the surviving prefix is already
    /// durable on the txlog — same semantics AMPS exposes).
    pub async fn publish_batch(
        &self,
        topic: &str,
        rows: Vec<Value>,
    ) -> ClientResult<Vec<u64>> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let mut m = CqMessage::new(Command::PublishBatch);
        m.topic = Some(topic.into());
        m.batch = Some(rows);
        let resp = self.rpc(m).await?;
        Ok(resp.sequences.unwrap_or_default())
    }

    /// Sparse update: `data` must contain the topic's key field(s)
    /// plus only the fields that changed. The server merges those
    /// fields into the existing row, leaving unspecified fields
    /// untouched. Use this on wide-row topics where most fields are
    /// stable — saves wire bandwidth and publisher CPU.
    ///
    /// If no row matches the key yet, a new row is inserted with the
    /// supplied fields populated and the rest null (same semantics as
    /// a normal `publish` of the sparse payload).
    pub async fn delta_publish(&self, topic: &str, data: Value) -> ClientResult<u64> {
        let mut m = CqMessage::new(Command::DeltaPublish);
        m.topic = Some(topic.into());
        m.data = Some(data);
        let resp = self.rpc(m).await?;
        Ok(resp.sequence.unwrap_or(0))
    }

    /// One-shot SOW query. Returns the snapshot rows.
    pub async fn sow(
        &self,
        topic: &str,
        filter: Option<&str>,
    ) -> ClientResult<Vec<Map<String, Value>>> {
        self.sow_msg(|cid, m| {
            m.command_id = Some(cid.to_string());
            m.topic = Some(topic.into());
            if let Some(f) = filter {
                m.filter = Some(f.into());
            }
        })
        .await
    }

    /// Run an arbitrary SOW SQL query against `topic`. Use this when
    /// you need GROUP BY / aggregates (`SUM`, `COUNT`, `AVG`, `MIN`,
    /// `MAX`) — the filter-only `sow()` only builds `SELECT * FROM t
    /// WHERE ...`. The server rewrites the FROM clause to its
    /// canonical placeholder, so the table name in the SQL is
    /// cosmetic: write `FROM t`, `FROM trades`, or `FROM /yourtopic`
    /// interchangeably.
    pub async fn sow_sql(
        &self,
        topic: &str,
        sql: &str,
    ) -> ClientResult<Vec<Map<String, Value>>> {
        self.sow_msg(|cid, m| {
            m.command_id = Some(cid.to_string());
            m.topic = Some(topic.into());
            m.sql = Some(sql.into());
        })
        .await
    }

    /// Shared body for SOW-style requests: install snapshot
    /// buffer/completion, send the customized message, wait for
    /// group_end, return rows. The caller fills in the
    /// command-specific fields (`filter`, `sql`, etc.) via `prep`.
    async fn sow_msg(
        &self,
        prep: impl FnOnce(&str, &mut CqMessage),
    ) -> ClientResult<Vec<Map<String, Value>>> {
        let cid = self.next_cid();
        self.inner
            .snapshot_buffers
            .insert(cid.clone(), Mutex::new(Vec::new()));
        let (tx, rx) = oneshot::channel();
        self.inner.snapshot_completions.insert(cid.clone(), tx);

        let mut m = CqMessage::new(Command::Sow);
        prep(&cid, &mut m);
        if self.inner.out_tx.send(m).is_err() {
            self.inner.snapshot_buffers.remove(&cid);
            self.inner.snapshot_completions.remove(&cid);
            return Err(ClientError::ConnectionClosed);
        }

        let outcome: Option<String> = match self.inner.ack_timeout {
            Some(t) => match tokio::time::timeout(t, rx).await {
                Ok(Ok(v)) => v,
                Ok(Err(_)) => return Err(ClientError::ConnectionClosed),
                Err(_) => {
                    self.inner.snapshot_buffers.remove(&cid);
                    self.inner.snapshot_completions.remove(&cid);
                    return Err(ClientError::Timeout);
                }
            },
            None => rx.await.map_err(|_| ClientError::ConnectionClosed)?,
        };

        if let Some(err) = outcome {
            self.inner.snapshot_buffers.remove(&cid);
            return Err(ClientError::Server(err));
        }

        let rows = self
            .inner
            .snapshot_buffers
            .remove(&cid)
            .map(|(_, m)| m.into_inner())
            .unwrap_or_default();
        Ok(rows)
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        filter: Option<&str>,
    ) -> ClientResult<Subscription> {
        self.subscribe_inner(Command::Subscribe, topic, filter, None).await
    }

    pub async fn sow_and_subscribe(
        &self,
        topic: &str,
        filter: Option<&str>,
        bookmark: Option<u64>,
    ) -> ClientResult<Subscription> {
        self.subscribe_inner(Command::SowAndSubscribe, topic, filter, bookmark).await
    }

    /// Subscribe with a full SQL query (S19 / continuous aggregates).
    /// Required for `SELECT ... GROUP BY ...` subscriptions since
    /// the filter+options form can't express GROUP BY. The initial
    /// snapshot is the aggregate output; subsequent deltas carry
    /// per-group Add / Update / Remove as the underlying SOW state
    /// shifts.
    pub async fn sow_and_subscribe_sql(
        &self,
        topic: &str,
        sql: &str,
    ) -> ClientResult<Subscription> {
        self.subscribe_extended(
            Command::SowAndSubscribe,
            topic,
            None,
            SubscribeExtras {
                sql: Some(sql.to_string()),
                ..Default::default()
            },
        )
        .await
    }

    /// Subscribe with timestamp-based replay: every txlog entry with
    /// wall-clock timestamp >= `since_epoch_ms` is replayed, then the
    /// stream transitions to live. Requires the topic to be
    /// persistent (txlog-backed). Uses the `SowAndSubscribe` command
    /// internally because replay modes need the server's bookmark
    /// dispatch path.
    pub async fn subscribe_since_timestamp(
        &self,
        topic: &str,
        filter: Option<&str>,
        since_epoch_ms: u64,
    ) -> ClientResult<Subscription> {
        self.subscribe_extended(
            Command::SowAndSubscribe,
            topic,
            filter,
            SubscribeExtras {
                since_timestamp_ms: Some(since_epoch_ms),
                ..Default::default()
            },
        )
        .await
    }

    /// Subscribe with `MOST_RECENT` replay: the server resumes the
    /// stream from the highest sequence it previously delivered to
    /// this client (looked up by `client_name`). Falls through to a
    /// live-only stream when no prior delivery is recorded.
    pub async fn subscribe_most_recent(
        &self,
        topic: &str,
        filter: Option<&str>,
        client_name: &str,
    ) -> ClientResult<Subscription> {
        self.subscribe_extended(
            Command::SowAndSubscribe,
            topic,
            filter,
            SubscribeExtras {
                most_recent: true,
                client_name: Some(client_name.to_string()),
                ..Default::default()
            },
        )
        .await
    }

    async fn subscribe_extended(
        &self,
        command: Command,
        topic: &str,
        filter: Option<&str>,
        mut extras: SubscribeExtras,
    ) -> ClientResult<Subscription> {
        let (tx, rx) = mpsc::unbounded_channel();
        let cid = self.next_cid();
        self.inner.pending_subs.insert(cid.clone(), tx);

        // S18: if the SDK has a bookmark store AND the caller didn't
        // explicitly pass a bookmark, populate it from the persistent
        // store. The server replays everything strictly newer than
        // the stored sequence.
        let bm_store = self.inner.bookmark_store.read().clone();
        if extras.bookmark.is_none() {
            if let Some(store) = &bm_store {
                if let Some(stored) = store.get(topic) {
                    extras.bookmark = Some(stored);
                }
            }
        }

        let mut m = CqMessage::new(command);
        m.command_id = Some(cid.clone());
        m.topic = Some(topic.into());
        if let Some(f) = filter {
            m.filter = Some(f.into());
        }
        if let Some(b) = extras.bookmark {
            m.bookmark = Some(b);
        }
        if let Some(ts) = extras.since_timestamp_ms {
            m.since_timestamp_ms = Some(ts);
        }
        if extras.most_recent {
            m.most_recent = true;
        }
        if let Some(name) = extras.client_name {
            m.client_name = Some(name);
        }
        if extras.send_keys {
            m.send_keys = true;
        }
        if let Some(sql) = &extras.sql {
            m.sql = Some(sql.clone());
        }
        let ack = match self.rpc_with_cid(m, cid.clone()).await {
            Ok(a) => a,
            Err(e) => {
                self.inner.pending_subs.remove(&cid);
                return Err(e);
            }
        };
        let sub_id = ack
            .sub_id
            .ok_or_else(|| ClientError::UnexpectedResponse("missing sub_id in ack".into()))?;
        let last_seq = Arc::new(AtomicU64::new(extras.bookmark.unwrap_or(0)));
        Ok(Subscription {
            sub_id,
            rx,
            last_seq,
            topic: topic.to_string(),
            bookmark_store: bm_store,
        })
    }

    pub async fn delta_subscribe(
        &self,
        topic: &str,
        filter: Option<&str>,
    ) -> ClientResult<Subscription> {
        self.subscribe_inner(Command::DeltaSubscribe, topic, filter, None).await
    }

    /// Pause an in-flight bookmark-replay subscription. The server
    /// holds the replay cursor; no further deltas will flow until
    /// the next `resume_subscription` for the same `sub_id`. Live
    /// (post-replay) delivery isn't affected.
    pub async fn pause_subscription(&self, sub_id: &str) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Pause);
        m.sub_id = Some(sub_id.into());
        if self.inner.out_tx.send(m).is_err() {
            return Err(ClientError::ConnectionClosed);
        }
        Ok(())
    }

    /// Resume a previously paused subscription. Server replays from
    /// where it stopped.
    pub async fn resume_subscription(&self, sub_id: &str) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Resume);
        m.sub_id = Some(sub_id.into());
        if self.inner.out_tx.send(m).is_err() {
            return Err(ClientError::ConnectionClosed);
        }
        Ok(())
    }

    /// Commit a queue-message lease. Sends an `Ack` with the topic
    /// and delivery_id; the server removes the matching in-flight
    /// lease so the message isn't redelivered. Fire-and-forget — no
    /// response is expected. Safe to call repeatedly with the same
    /// id; duplicate acks are silently ignored server-side.
    pub async fn queue_ack(&self, queue: &str, delivery_id: u64) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Ack);
        m.topic = Some(queue.into());
        m.delivery_id = Some(delivery_id);
        if self.inner.out_tx.send(m).is_err() {
            return Err(ClientError::ConnectionClosed);
        }
        Ok(())
    }

    /// `delta_subscribe` with `send_keys` — initial snapshot delivers
    /// only the topic's key columns per row (no payload fields);
    /// subsequent updates remain sparse (changed fields only). Useful
    /// when a grid client just wants the key inventory upfront and
    /// will pull full rows on demand.
    pub async fn delta_subscribe_send_keys(
        &self,
        topic: &str,
        filter: Option<&str>,
    ) -> ClientResult<Subscription> {
        self.subscribe_extended(
            Command::DeltaSubscribe,
            topic,
            filter,
            SubscribeExtras {
                send_keys: true,
                ..Default::default()
            },
        )
        .await
    }
}

#[derive(Default)]
struct SubscribeExtras {
    bookmark: Option<u64>,
    since_timestamp_ms: Option<u64>,
    most_recent: bool,
    client_name: Option<String>,
    send_keys: bool,
    /// Full SQL passed verbatim to the server. Required for
    /// continuous-aggregate subscriptions (`SELECT ... GROUP BY ...`)
    /// since the options/filter shape can't express GROUP BY.
    sql: Option<String>,
}

impl Client {

    async fn subscribe_inner(
        &self,
        command: Command,
        topic: &str,
        filter: Option<&str>,
        bookmark: Option<u64>,
    ) -> ClientResult<Subscription> {
        // Pre-allocate the subscription channel and stash the tx in
        // `pending_subs` keyed by cid. The driver's Ack handler moves
        // it to `subs` keyed by the server-assigned sid, atomically
        // with respect to subsequent inbound messages.
        let (tx, rx) = mpsc::unbounded_channel();
        let cid = self.next_cid();
        self.inner.pending_subs.insert(cid.clone(), tx);

        // S18: if the SDK has a bookmark store AND the caller didn't
        // explicitly pass a bookmark, populate it from the persistent
        // store.
        let bm_store = self.inner.bookmark_store.read().clone();
        let bookmark = bookmark.or_else(|| {
            bm_store.as_ref().and_then(|s| s.get(topic))
        });

        let mut m = CqMessage::new(command);
        m.command_id = Some(cid.clone());
        m.topic = Some(topic.into());
        if let Some(f) = filter {
            m.filter = Some(f.into());
        }
        if let Some(b) = bookmark {
            m.bookmark = Some(b);
        }
        // `rpc_with_cid` skips the cid allocation since we already set it.
        let ack = match self.rpc_with_cid(m, cid.clone()).await {
            Ok(a) => a,
            Err(e) => {
                // Send failed before any ack — clean up the pending tx.
                self.inner.pending_subs.remove(&cid);
                return Err(e);
            }
        };
        let sub_id = ack
            .sub_id
            .ok_or_else(|| ClientError::UnexpectedResponse("missing sub_id in ack".into()))?;
        let last_seq = Arc::new(AtomicU64::new(bookmark.unwrap_or(0)));
        Ok(Subscription {
            sub_id,
            rx,
            last_seq,
            topic: topic.to_string(),
            bookmark_store: bm_store,
        })
    }

    pub async fn unsubscribe(&self, sub_id: &str) -> ClientResult<()> {
        let mut m = CqMessage::new(Command::Unsubscribe);
        m.sub_id = Some(sub_id.into());
        self.rpc(m).await?;
        self.inner.subs.remove(sub_id);
        Ok(())
    }

    pub async fn sow_delete(&self, topic: &str, key: &str) -> ClientResult<u64> {
        let mut m = CqMessage::new(Command::SowDelete);
        m.topic = Some(topic.into());
        let mut payload = Map::new();
        payload.insert("key".into(), Value::String(key.into()));
        m.data = Some(Value::Object(payload));
        let resp = self.rpc(m).await?;
        Ok(resp.sequence.unwrap_or(0))
    }

    pub async fn heartbeat(&self) -> ClientResult<()> {
        let m = CqMessage::new(Command::Heartbeat);
        self.rpc(m).await?;
        Ok(())
    }

    pub fn codec(&self) -> Codec {
        self.inner.codec
    }
}

/// Convert the in-memory `Compression` to the atomic-byte
/// representation used by the driver loop.
fn compression_to_u8(c: cq_protocol::compression::Compression) -> u8 {
    match c {
        cq_protocol::compression::Compression::None => 0,
        cq_protocol::compression::Compression::Zstd => 1,
    }
}

fn compression_from_u8(b: u8) -> cq_protocol::compression::Compression {
    match b {
        1 => cq_protocol::compression::Compression::Zstd,
        _ => cq_protocol::compression::Compression::None,
    }
}

async fn driver_loop(
    mut transport: Transport,
    mut out_rx: mpsc::UnboundedReceiver<CqMessage>,
    pending: Arc<DashMap<String, oneshot::Sender<CqMessage>>>,
    subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>>,
    snapshot_completions: Arc<DashMap<String, oneshot::Sender<Option<String>>>>,
    snapshot_buffers: Arc<DashMap<String, Mutex<Vec<Map<String, Value>>>>>,
    pending_subs: Arc<DashMap<String, mpsc::UnboundedSender<Delta>>>,
    codec: Codec,
    compression_slot: Arc<std::sync::atomic::AtomicU8>,
) {
    loop {
        tokio::select! {
            biased;
            outgoing = out_rx.recv() => {
                let Some(msg) = outgoing else { return; };
                let bytes = match codec.encode(&msg) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(error = %e, "encode failed");
                        continue;
                    }
                };
                let compression = compression_from_u8(
                    compression_slot.load(std::sync::atomic::Ordering::Acquire),
                );
                if let Err(e) = transport.send(codec, &bytes, compression).await {
                    tracing::warn!(error = %e, "send failed; closing");
                    return;
                }
            }
            incoming = transport.recv(codec) => {
                match incoming {
                    Ok((wire_codec, bytes)) => {
                        let msg = match wire_codec.decode(&bytes) {
                            Ok(m) => m,
                            Err(e) => {
                                tracing::warn!(error = %e, "decode failed");
                                continue;
                            }
                        };
                        handle_inbound(msg, &pending, &subs, &snapshot_completions, &snapshot_buffers, &pending_subs);
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "driver loop ending");
                        return;
                    }
                }
            }
        }
    }
}

fn handle_inbound(
    msg: CqMessage,
    pending: &DashMap<String, oneshot::Sender<CqMessage>>,
    subs: &DashMap<String, mpsc::UnboundedSender<Delta>>,
    snapshot_completions: &DashMap<String, oneshot::Sender<Option<String>>>,
    snapshot_buffers: &DashMap<String, Mutex<Vec<Map<String, Value>>>>,
    pending_subs: &DashMap<String, mpsc::UnboundedSender<Delta>>,
) {
    match msg.command {
        Command::Ack => {
            // Promote a pre-registered subscription channel (keyed by
            // cid) into the routing map under its server-assigned sid
            // BEFORE waking the rpc waiter. This guarantees the sid is
            // already routable when any subsequent SOW/Publish frame
            // for the same subscription is processed by this loop —
            // closing the race that used to drop snapshot rows whose
            // arrival landed in the same batch as the ack.
            if let (Some(cid), Some(sid)) = (msg.command_id.as_ref(), msg.sub_id.as_ref()) {
                if let Some((_, tx)) = pending_subs.remove(cid) {
                    subs.insert(sid.clone(), tx);
                }
            }
            if let Some(cid) = msg.command_id.as_ref() {
                // P7 — if this is an error ack for an in-flight SOW
                // (no group_end will ever arrive), short-circuit the
                // snapshot_completion with the error message so the
                // awaiting caller returns ClientError::Server promptly
                // instead of timing out.
                if msg.status == Some(Status::Error) {
                    if let Some((_, snap_tx)) = snapshot_completions.remove(cid) {
                        let err = msg.reason.clone().unwrap_or_else(|| "server error".into());
                        let _ = snap_tx.send(Some(err));
                    }
                }
                if let Some((_, sender)) = pending.remove(cid) {
                    let _ = sender.send(msg);
                    return;
                }
            }
            tracing::debug!("Ack with no matching pending request");
        }
        Command::Sow => {
            // Per-row SOW frame. Two consumers:
            //   1. Buffer keyed by sub_id (which the client set to the
            //      cid) — drained by the one-shot `sow()` API.
            //   2. If a `Subscription` is registered for this sub_id,
            //      also forward as a `SowSnapshot` delta so subscribe
            //      callers can read snapshot rows and live deltas
            //      through the same channel.
            if let (Some(sid), Some(data)) = (msg.sub_id.as_ref(), msg.data.as_ref()) {
                if let Value::Object(m) = data {
                    if let Some(buf) = snapshot_buffers.get(sid) {
                        buf.lock().push(m.clone());
                    }
                    if let Some(tx) = subs.get(sid) {
                        let _ = tx.send(Delta {
                            delta_type: DeltaKind::SowSnapshot,
                            sub_id: sid.clone(),
                            sequence: msg.sequence,
                            data: m.clone(),
                            delivery_id: None,
                            schema_change: None,
                        });
                    }
                }
            }
        }
        Command::SowBatch => {
            // Chunked SOW frame: data is a JSON array of row objects.
            // Explode into per-row state — same consumers as Sow.
            if let (Some(sid), Some(Value::Array(rows))) =
                (msg.sub_id.as_ref(), msg.data.as_ref())
            {
                let buf_handle = snapshot_buffers.get(sid);
                let sub_handle = subs.get(sid);
                for row_val in rows {
                    let Value::Object(m) = row_val else { continue };
                    if let Some(buf) = buf_handle.as_ref() {
                        buf.lock().push(m.clone());
                    }
                    if let Some(tx) = sub_handle.as_ref() {
                        let _ = tx.send(Delta {
                            delta_type: DeltaKind::SowSnapshot,
                            sub_id: sid.clone(),
                            sequence: None,
                            data: m.clone(),
                            delivery_id: None,
                            schema_change: None,
                        });
                    }
                }
            }
        }
        Command::GroupBegin => {
            // Nothing to do — the buffer is already pre-allocated.
        }
        Command::GroupEnd => {
            if let Some(sid) = msg.sub_id.as_ref() {
                if let Some((_, tx)) = snapshot_completions.remove(sid) {
                    let _ = tx.send(None);
                }
            }
        }
        Command::Publish => {
            // Server uses Publish as the delta envelope. Route by sub_id.
            if let Some(sid) = msg.sub_id.as_ref() {
                if let Some(tx) = subs.get(sid) {
                    let dt = msg
                        .delta_type
                        .as_deref()
                        .map(DeltaKind::from_str)
                        .unwrap_or(DeltaKind::Update);
                    let data = match msg.data {
                        Some(Value::Object(m)) => m,
                        _ => Map::new(),
                    };
                    let _ = tx.send(Delta {
                        delta_type: dt,
                        sub_id: sid.clone(),
                        sequence: msg.sequence,
                        data,
                        delivery_id: msg.delivery_id,
                        schema_change: None,
                    });
                }
            }
        }
        Command::SchemaChange => {
            // Server announces a schema change for this subscription.
            // Surface via the same delta stream — callers
            // pattern-match `delta_type == DeltaKind::SchemaChange`
            // and read the `schema_change` field. Frame ordering
            // guarantee (server emits SchemaChange BEFORE any delta
            // that references the new columns) is preserved by the
            // sub channel's FIFO semantics.
            if let Some(sid) = msg.sub_id.as_ref() {
                if let Some(tx) = subs.get(sid) {
                    let _ = tx.send(Delta {
                        delta_type: DeltaKind::SchemaChange,
                        sub_id: sid.clone(),
                        sequence: msg.sequence,
                        data: Map::new(),
                        delivery_id: None,
                        schema_change: msg.schema_change.clone(),
                    });
                }
            }
        }
        Command::Heartbeat => {
            // Server-initiated heartbeat — nothing to do; the read loop
            // touch is implicit.
        }
        _ => {
            tracing::debug!(?msg.command, "unhandled inbound command");
        }
    }
}

/// Return `0..n` in a randomized order. Process-time-seeded xorshift —
/// not crypto-grade, but sufficient to spread N clients across M
/// follower URLs in `connect_any`. Pure stdlib so cq-client doesn't
/// need to pull in `rand`.
fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut idx: Vec<usize> = (0..n).collect();
    if n < 2 {
        return idx;
    }
    let mut s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
        .wrapping_add(idx.as_ptr() as u64); // mix in a stack-ish address
    if s == 0 {
        s = 1;
    }
    // Fisher-Yates with a tiny xorshift PRNG.
    for i in (1..n).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = (s as usize) % (i + 1);
        idx.swap(i, j);
    }
    idx
}

#[cfg(test)]
mod shuffle_tests {
    use super::shuffled_indices;

    #[test]
    fn shuffled_indices_returns_permutation() {
        for n in 1..=8 {
            let idx = shuffled_indices(n);
            assert_eq!(idx.len(), n);
            let mut sorted = idx.clone();
            sorted.sort();
            assert_eq!(sorted, (0..n).collect::<Vec<_>>());
        }
    }

    #[test]
    fn shuffled_indices_empty_and_single_are_identity() {
        assert_eq!(shuffled_indices(0), Vec::<usize>::new());
        assert_eq!(shuffled_indices(1), vec![0]);
    }

    #[test]
    fn shuffled_indices_actually_varies() {
        // Across many calls we should see at least one non-identity
        // permutation. (Time-seeded so close calls can collide, but
        // 50 calls with a 5-elt array essentially never all match
        // identity in practice.)
        let mut saw_non_identity = false;
        for _ in 0..50 {
            let idx = shuffled_indices(5);
            if idx != vec![0, 1, 2, 3, 4] {
                saw_non_identity = true;
                break;
            }
            // Small spin to advance the clock between calls.
            for _ in 0..1000 {
                std::hint::spin_loop();
            }
        }
        assert!(saw_non_identity, "shuffle never varied across 50 calls");
    }
}

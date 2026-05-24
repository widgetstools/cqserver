//! Per-connection session state and the global subscription delivery registry.

use crate::auth::{Entitlement, Op};
use cq_core::conflation::Conflator;
use cq_core::subscription::{Delta, DeltaType};
use cq_protocol::message::CqMessage;
use cq_protocol::serialization::Codec;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

/// One outbound wire frame. The transport-specific writer knows how to
/// emit each variant on its medium:
///
/// - WebSocket: `Text` → `Message::Text(s)`, `Binary` → `Message::Binary(b)`.
/// - TCP: both variants are length-prefixed bytes; the underlying codec
///   is recorded on the session so the receiver can decode them, but the
///   framing is identical.
#[derive(Debug, Clone)]
pub enum OutboundFrame {
    Text(String),
    Binary(Vec<u8>),
}

impl OutboundFrame {
    pub fn len(&self) -> usize {
        match self {
            OutboundFrame::Text(s) => s.len(),
            OutboundFrame::Binary(b) => b.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            OutboundFrame::Text(s) => s.as_bytes(),
            OutboundFrame::Binary(b) => b.as_slice(),
        }
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

/// Default per-session outbound queue depth. Overridable from
/// `[transport].outbound_queue_capacity` in `cqserver.toml`.
///
/// H1: dropped from 8192 to 2048. At high subscriber counts (2K+),
/// each session's pre-allocated mpsc was the dominant per-sub
/// memory cost — 8K × Vec<OutboundFrame>-slot was ~32 KB per session
/// before any traffic flowed. 2K cuts that to ~8 KB, which is more
/// than enough headroom for a typical SOW burst given the streaming
/// SOW path uses `send().await` for natural backpressure. Slow
/// consumers still get the same drop semantics on live-delta
/// `try_send`. Operators with bursty workloads can dial it back up
/// in TOML.
pub const DEFAULT_OUTBOUND_QUEUE_CAPACITY: usize = 2048;

/// Default rows packed per outbound `sow_batch` frame on the
/// streaming SOW path. Overridable from `[transport].sow_batch_size`.
pub const DEFAULT_SOW_BATCH_SIZE: usize = 200;

pub type OutboundTx = mpsc::Sender<OutboundFrame>;

/// Encode `msg` into a wire frame using `codec`. JSON produces a Text
/// frame, MessagePack produces a Binary frame.
pub fn encode_frame(codec: Codec, msg: &CqMessage) -> Option<OutboundFrame> {
    match codec {
        Codec::Json => serde_json::to_string(msg).ok().map(OutboundFrame::Text),
        Codec::MessagePack => rmp_serde::to_vec_named(msg).ok().map(OutboundFrame::Binary),
        Codec::Bson | Codec::Fix => codec.encode(msg).ok().map(OutboundFrame::Binary),
    }
}

/// Pre-serialize a row body (the `d` field of a delta frame) to JSON
/// bytes. Used by the encode-once-fan-out delivery path so the same
/// row body can be embedded in many per-subscriber frames without
/// re-serializing the payload N times.
pub fn encode_row_body_json(
    data: &serde_json::Map<String, serde_json::Value>,
) -> Option<Vec<u8>> {
    serde_json::to_vec(data).ok()
}

/// Build a JSON delta frame by stitching a pre-encoded body into a
/// per-subscriber envelope. The envelope is tiny and string-built; the
/// body bytes (which can be many KB on wide-row topics) are reused.
///
/// `dt` must be a literal kind (`add`, `update`, `remove`, `oof`) — no
/// escaping is performed. `body` is assumed to be valid UTF-8 JSON.
pub fn build_json_delta_frame(
    sub_id: &str,
    dt: &str,
    seq: u64,
    body: &[u8],
) -> Option<OutboundFrame> {
    let body_str = std::str::from_utf8(body).ok()?;
    let mut buf = String::with_capacity(body_str.len() + 64 + sub_id.len());
    buf.push_str("{\"c\":\"publish\",\"sid\":\"");
    push_json_string_escaped(&mut buf, sub_id);
    buf.push_str("\",\"dt\":\"");
    buf.push_str(dt);
    buf.push_str("\",\"seq\":");
    buf.push_str(&seq.to_string());
    buf.push_str(",\"d\":");
    buf.push_str(body_str);
    buf.push('}');
    Some(OutboundFrame::Text(buf))
}

/// Build a JSON `sow_batch` frame from pre-encoded row JSON bytes.
/// Used by the streaming SOW snapshot path so we don't decode the row
/// bytes back into `serde_json::Map` just to re-encode them inside a
/// `CqMessage::sow_batch` envelope.
///
/// `rows` is a slice of already-valid JSON objects (e.g. produced by
/// `ColumnStore::write_row_json_projected`). The envelope is built as
/// a single `String` with a one-shot allocation sized for the
/// expected output.
///
/// Returns `None` if any row's bytes aren't valid UTF-8 (shouldn't
/// happen — our encoder writes only ASCII-safe UTF-8 plus
/// JSON-escaped string contents).
pub fn build_sow_batch_json_frame(sub_id: &str, rows: &[Vec<u8>]) -> Option<OutboundFrame> {
    // Pre-size: envelope overhead + sum of row sizes + separators.
    let body_size: usize = rows.iter().map(|r| r.len() + 1).sum();
    let mut buf = String::with_capacity(body_size + 48 + sub_id.len());
    buf.push_str("{\"c\":\"sow_batch\",\"sid\":\"");
    push_json_string_escaped(&mut buf, sub_id);
    buf.push_str("\",\"d\":[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        let s = std::str::from_utf8(row).ok()?;
        buf.push_str(s);
    }
    buf.push_str("]}");
    Some(OutboundFrame::Text(buf))
}

/// JSON-string-escape `s` into `out`. Sub-ids are server-assigned in
/// the form `sess-N:sub-M`, but we still escape defensively in case
/// future schemes include `"`, `\` or control chars.
fn push_json_string_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
}

/// Shared codec slot. Used by the heartbeat task (and any other
/// session-adjacent background task) to read whatever codec the session
/// currently negotiated.
pub type SharedCodec = Arc<parking_lot::Mutex<Codec>>;

/// Routing entry for a single subscription.
#[derive(Clone)]
pub struct DeliveryRoute {
    pub tx: OutboundTx,
    pub topic: String,
    pub dropped: Arc<AtomicU64>,
    /// Codec used by the owning session — determines whether outbound
    /// deltas are serialized as JSON text or MessagePack binary.
    pub codec: Codec,
    /// Optional per-subscription conflator. When `Some`, deltas are coalesced
    /// by row and a background flush task pushes them through `tx` at the
    /// configured interval. When `None`, callers send directly.
    pub conflator: Option<Arc<Conflator>>,
    /// Adjustable flush interval in milliseconds. `0` means "no
    /// conflation"; the value is read by the flush task each iteration
    /// so the watcher can widen the interval under back-pressure
    /// without recreating the task. `None` when the route was built
    /// without conflation.
    pub interval_ms: Option<Arc<AtomicU64>>,
    /// Wall-clock when the subscription was registered. Used by admin
    /// endpoints / slow-consumer watcher to compute "age".
    pub created_at: std::time::Instant,
    /// Owning session id, so the slow-consumer policy can disconnect
    /// the underlying connection if it decides to.
    pub session_id: String,
    /// Stable client identity, when known (from logon username or an
    /// explicit `client_name` field on the subscribe message). Used
    /// by the server-side bookmark store to power `MOST_RECENT`
    /// replay across reconnects. `None` for anonymous sessions.
    pub client_name: Option<String>,
    /// Highest delta sequence successfully enqueued for this route.
    /// Updated by the delivery layer; read by the bookmark-store
    /// updater so `MOST_RECENT` knows where the client last got to.
    pub last_seq: Arc<AtomicU64>,
    /// Per-(client_name, topic) bookmark store. The delivery layer
    /// updates the store on every successful send so `MOST_RECENT`
    /// resumption picks up from the right place after a reconnect.
    /// `None` when no client_name is known (anonymous session).
    pub bookmark_store: Option<crate::router::BookmarkStore>,
    /// When `true`, the bookmark-replay loop pauses between
    /// entries until a Resume command flips it back. Live
    /// delivery isn't paused (out of scope for S10 — matches the
    /// AMPS semantics where pause/resume covers replay only).
    pub paused: Arc<std::sync::atomic::AtomicBool>,
    /// Notified when `paused` transitions false → true → false (a
    /// resume). The replay loop awaits this notify when it sees
    /// the paused flag set.
    pub resume_notify: Arc<tokio::sync::Notify>,
    /// S21: per-route disk spillover. When `Some`, the delivery path
    /// pushes frames here instead of dropping on `try_send` failure;
    /// a background drain task feeds them back into `tx` as soon as
    /// the queue has room. `None` falls back to the legacy "drop on
    /// full" behaviour. Ordering is preserved by routing through
    /// spillover whenever any backlog is present.
    pub spillover: Option<Arc<crate::spillover::Spillover>>,
}

impl DeliveryRoute {
    pub fn new(tx: OutboundTx, topic: String) -> Self {
        Self::with_codec(tx, topic, Codec::Json)
    }

    pub fn with_codec(tx: OutboundTx, topic: String, codec: Codec) -> Self {
        DeliveryRoute {
            tx,
            topic,
            dropped: Arc::new(AtomicU64::new(0)),
            codec,
            conflator: None,
            interval_ms: None,
            created_at: std::time::Instant::now(),
            session_id: String::new(),
            client_name: None,
            last_seq: Arc::new(AtomicU64::new(0)),
            bookmark_store: None,
            paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            resume_notify: Arc::new(tokio::sync::Notify::new()),
            spillover: None,
        }
    }

    /// Attach a spillover file to this route. Subsequent `try_send`
    /// failures fall through to the spillover instead of dropping;
    /// the caller is expected to also spawn `spawn_spillover_drain`
    /// (in this module) so a backlog drains as the queue catches up.
    pub fn with_spillover(mut self, spillover: Arc<crate::spillover::Spillover>) -> Self {
        self.spillover = Some(spillover);
        self
    }

    pub fn with_client_name(mut self, client_name: Option<String>) -> Self {
        self.client_name = client_name;
        self
    }

    pub fn with_bookmark_store(
        mut self,
        store: Option<crate::router::BookmarkStore>,
    ) -> Self {
        self.bookmark_store = store;
        self
    }

    /// Update the conflator's flush interval. Takes effect from the
    /// next tick. No-op when this route was built without conflation.
    /// Used by the slow-consumer watcher to widen the interval under
    /// back-pressure.
    pub fn set_conflation_interval_ms(&self, ms: u64) {
        if let Some(i) = self.interval_ms.as_ref() {
            i.store(ms.max(1), std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Current flush interval in millis, or 0 if no conflator is active.
    pub fn conflation_interval_ms(&self) -> u64 {
        self.interval_ms
            .as_ref()
            .map(|i| i.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = session_id.into();
        self
    }

    /// Number of frames currently sitting in the outbound queue.
    pub fn queue_depth(&self) -> usize {
        self.tx.max_capacity().saturating_sub(self.tx.capacity())
    }

    /// Channel capacity (`max_capacity` from tokio).
    pub fn queue_capacity(&self) -> usize {
        self.tx.max_capacity()
    }

    /// Total frames dropped by this route since registration.
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Milliseconds since the route was registered.
    pub fn age_ms(&self) -> u64 {
        self.created_at.elapsed().as_millis() as u64
    }

    /// Build a route with conflation enabled. Spawns a flush task on the
    /// current tokio runtime that drains the conflator every `interval` and
    /// pushes deltas through `tx`. The task auto-terminates when the
    /// `DeliveryRoute` (and any clones) are dropped from the registry, via
    /// the `Weak` reference path.
    pub fn with_conflation(
        tx: OutboundTx,
        topic: String,
        sub_id: String,
        interval: Duration,
    ) -> Self {
        Self::with_conflation_codec(tx, topic, sub_id, interval, Codec::Json)
    }

    pub fn with_conflation_codec(
        tx: OutboundTx,
        topic: String,
        sub_id: String,
        interval: Duration,
        codec: Codec,
    ) -> Self {
        let conflator = Arc::new(Conflator::new());
        let dropped = Arc::new(AtomicU64::new(0));
        let interval_ms = Arc::new(AtomicU64::new(interval.as_millis().max(1) as u64));
        let last_seq = Arc::new(AtomicU64::new(0));

        spawn_flush_loop(
            sub_id,
            topic.clone(),
            Arc::downgrade(&conflator),
            tx.clone(),
            dropped.clone(),
            interval_ms.clone(),
            codec,
            last_seq.clone(),
            None, // client_name + bookmark_store are bound later via
            None, // `with_*` builders; the flush loop captures them
                  // through Arc cloning at spawn time. Conflated subs
                  // therefore only record `last_seq` locally — the
                  // MOST_RECENT bookmark store update flows through
                  // the direct-send path. (Acceptable: conflated subs
                  // are typically high-rate live streams where
                  // MOST_RECENT resume isn't the primary use case.)
        );

        DeliveryRoute {
            tx,
            topic,
            dropped,
            codec,
            conflator: Some(conflator),
            interval_ms: Some(interval_ms),
            created_at: std::time::Instant::now(),
            session_id: String::new(),
            client_name: None,
            last_seq,
            bookmark_store: None,
            paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            resume_notify: Arc::new(tokio::sync::Notify::new()),
            spillover: None,
        }
    }
}

/// Spawn the per-route spillover drain task. Wakes whenever the
/// outbound queue has space, reads frames back from disk in append
/// order, and `try_send`s them through the route. Exits when the
/// spillover has been empty for one full backoff cycle AND no new
/// writes arrive — re-armed by any subsequent overflow.
pub fn spawn_spillover_drain(
    sub_id: String,
    tx: OutboundTx,
    spillover: Arc<crate::spillover::Spillover>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff_ms: u64 = 5;
        loop {
            if spillover.is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(250);
                // Re-check after sleep so we can exit when the sub
                // has been closed (sender disconnect).
                if tx.is_closed() {
                    break;
                }
                continue;
            }
            backoff_ms = 5;
            // Peek the next frame. We do NOT advance the read cursor
            // until try_send succeeds; otherwise a Full receiver would
            // lose the frame entirely. Read+rollback isn't supported
            // by Spillover, so we instead poll the queue's capacity
            // and skip the read when there's no room.
            if tx.capacity() == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }
            let frame = match spillover.read_next_frame() {
                Ok(Some(f)) => f,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(sub = %sub_id, error = %e, "Spillover read failed");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            match tx.try_send(frame) {
                Ok(()) => {
                    metrics::counter!("cq_spillover_drained_total").increment(1);
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_lost)) => {
                    // The capacity check above raced with another
                    // sender (e.g., a parallel live publish). The
                    // frame has already been consumed from the
                    // spillover — losing it would be a bug. Best
                    // recovery: wait briefly and re-spool by writing
                    // it back. Order is preserved because we hold
                    // the only drain task.
                    metrics::counter!("cq_spillover_respool_total").increment(1);
                    let _ = spillover.write_frame(&_lost);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(sub = %sub_id, "Spillover drain: receiver closed");
                    break;
                }
            }
        }
    })
}

/// Spawn a background task that drains `conflator` every `interval` and
/// pushes deltas through `tx`. Exits when the conflator's last strong ref
/// is dropped (i.e., the route is evicted from the registry).
fn spawn_flush_loop(
    sub_id: String,
    topic: String,
    weak: Weak<Conflator>,
    tx: OutboundTx,
    dropped: Arc<AtomicU64>,
    interval_ms: Arc<AtomicU64>,
    codec: Codec,
    last_seq: Arc<AtomicU64>,
    client_name: Option<String>,
    bookmark_store: Option<crate::router::BookmarkStore>,
) {
    tokio::spawn(async move {
        // We read `interval_ms` every tick rather than caching it in a
        // `tokio::time::interval` instance, so the slow-consumer
        // watcher can widen the interval at runtime under
        // back-pressure. The atomic is checked once per sleep — cheap.
        loop {
            let ms = interval_ms.load(Ordering::Relaxed).max(1);
            tokio::time::sleep(Duration::from_millis(ms)).await;
            let conflator = match weak.upgrade() {
                Some(c) => c,
                None => return, // Route gone — exit.
            };

            let deltas = conflator.drain();
            drop(conflator); // Don't hold the strong ref while sending.

            for delta in deltas {
                let dt_str = match delta.delta_type {
                    DeltaType::Add => "add",
                    DeltaType::Update => "update",
                    DeltaType::Remove => "remove",
                    DeltaType::Oof => "oof",
                };
                // Fast path: JSON codec + pre-encoded body attached by
                // the evaluator. Skip serialization entirely and just
                // stitch the per-sub envelope.
                let frame = if matches!(codec, Codec::Json) {
                    match delta.encoded_body_json.as_ref() {
                        Some(body) => {
                            metrics::counter!("cq_conflator_body_reuses_total").increment(1);
                            match build_json_delta_frame(
                                &delta.subscription_id,
                                dt_str,
                                delta.sequence,
                                body,
                            ) {
                                Some(f) => f,
                                None => {
                                    tracing::warn!(
                                        sub = %sub_id,
                                        "Conflated delta envelope build failed"
                                    );
                                    continue;
                                }
                            }
                        }
                        None => {
                            // No pre-encoded body — fall back to a
                            // full encode. Happens when the delta was
                            // submitted via a path that didn't pre-
                            // serialize (e.g., tests).
                            metrics::counter!("cq_conflator_body_encodes_total").increment(1);
                            let mut msg = CqMessage::delta(
                                &delta.subscription_id,
                                dt_str,
                                (*delta.row_data).clone(),
                            );
                            msg.sequence = Some(delta.sequence);
                            match encode_frame(codec, &msg) {
                                Some(f) => f,
                                None => {
                                    tracing::warn!(
                                        sub = %sub_id,
                                        "Conflated delta serialize failed"
                                    );
                                    continue;
                                }
                            }
                        }
                    }
                } else {
                    metrics::counter!("cq_conflator_body_encodes_total").increment(1);
                    let mut msg = CqMessage::delta(
                        &delta.subscription_id,
                        dt_str,
                        (*delta.row_data).clone(),
                    );
                    msg.sequence = Some(delta.sequence);
                    match encode_frame(codec, &msg) {
                        Some(f) => f,
                        None => {
                            tracing::warn!(sub = %sub_id, "Conflated delta serialize failed");
                            continue;
                        }
                    }
                };
                match tx.try_send(frame) {
                    Ok(()) => {
                        last_seq.fetch_max(delta.sequence, Ordering::Relaxed);
                        if let (Some(cname), Some(store)) =
                            (client_name.as_deref(), bookmark_store.as_ref())
                        {
                            crate::router::record_bookmark(
                                store,
                                cname,
                                &topic,
                                delta.sequence,
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        dropped.fetch_add(1, Ordering::Relaxed);
                        metrics::counter!(
                            "cq_subscription_dropped_total",
                            "topic" => topic.clone()
                        )
                        .increment(1);
                        // Cap log spam: only log every 256th drop per sub.
                        let n = dropped.load(Ordering::Relaxed);
                        if n.is_power_of_two() {
                            tracing::warn!(
                                sub = %sub_id,
                                topic = %topic,
                                drops_so_far = n,
                                "Conflated delta dropped (outbound queue full)"
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return,
                }
            }
        }
    });
}

/// Helper used by `deliver_delta` when the route has a conflator. Returns
/// `true` if the delta was consumed (submitted to the conflator), `false`
/// if the caller should fall through to direct send.
pub fn try_submit_to_conflator(route: &DeliveryRoute, delta: &Delta) -> bool {
    if let Some(c) = route.conflator.as_ref() {
        c.submit(delta.clone());
        true
    } else {
        false
    }
}

/// Shared `sub_id → route` map.
pub type SessionRegistry = Arc<DashMap<String, DeliveryRoute>>;

pub fn new_registry() -> SessionRegistry {
    Arc::new(DashMap::new())
}

/// Snapshot stats for one entry in the session registry. Used by the
/// admin API and the slow-consumer watcher.
#[derive(Debug, Clone)]
pub struct SubscriptionStats {
    pub sub_id: String,
    pub topic: String,
    pub session_id: String,
    pub queue_depth: usize,
    pub queue_capacity: usize,
    pub dropped: u64,
    pub age_ms: u64,
    pub conflated: bool,
    /// Current flush interval (ms) when the route uses conflation;
    /// 0 when conflation is disabled.
    pub conflation_interval_ms: u64,
}

impl SubscriptionStats {
    /// Fill ratio of the outbound queue, in `[0.0, 1.0]`.
    pub fn fill_ratio(&self) -> f32 {
        if self.queue_capacity == 0 {
            0.0
        } else {
            self.queue_depth as f32 / self.queue_capacity as f32
        }
    }
}

/// Iterate every active route and produce a snapshot of its stats.
/// Cheap — just reads atomics and the mpsc capacity counters.
pub fn collect_subscription_stats(registry: &SessionRegistry) -> Vec<SubscriptionStats> {
    registry
        .iter()
        .map(|e| {
            let sub_id = e.key().clone();
            let r = e.value();
            SubscriptionStats {
                sub_id,
                topic: r.topic.clone(),
                session_id: r.session_id.clone(),
                queue_depth: r.queue_depth(),
                queue_capacity: r.queue_capacity(),
                dropped: r.dropped_count(),
                age_ms: r.age_ms(),
                conflated: r.conflator.is_some(),
                conflation_interval_ms: r.conflation_interval_ms(),
            }
        })
        .collect()
}

pub struct Session {
    pub id: String,
    pub remote_addr: String,
    pub tx: OutboundTx,
    pub subscriptions: Vec<String>,
    sub_seq: u64,
    pub dropped: Arc<AtomicU64>,
    /// Millis-since-epoch of the last inbound frame on this connection.
    /// Touched by the read loop, read by the heartbeat task to detect
    /// idle peers.
    pub last_inbound_ms: Arc<AtomicU64>,
    /// Authenticated user identity. `None` until a successful `Logon`.
    pub username: Option<String>,
    /// Cached entitlement list (copied out of `AuthStore` on logon) so
    /// per-message checks don't have to take the store's lock.
    pub entitlements: Vec<Entitlement>,
    /// Wire codec for this connection. Defaults to JSON; switched to
    /// MessagePack by the WebSocket transport when it sees the first
    /// binary frame from the client. Shared via `Arc` so the
    /// heartbeat task can encode in the negotiated codec.
    pub codec: SharedCodec,
    /// S28 — active wire-protocol version for this connection. Set on
    /// `Logon` via [`cq_protocol::version::negotiate`]; defaults to
    /// `DEFAULT_LEGACY_VERSION` for pre-S28 clients that don't send
    /// `protocol_versions`. Future feature gating consults this to
    /// decide whether to emit V≥2 fields.
    pub protocol_version: u32,
    /// S27 — active wire-compression algorithm for this connection.
    /// Set on `Logon` via [`cq_protocol::compression::negotiate`];
    /// defaults to `Compression::None` for pre-S27 clients. Shared
    /// via `Arc<AtomicU8>` so the TCP write loop (which lives in a
    /// separate task and doesn't own the Session) can read the
    /// current setting without taking a lock.
    pub compression: Arc<std::sync::atomic::AtomicU8>,
}

impl Session {
    pub fn new(remote_addr: String, tx: OutboundTx) -> Self {
        let id = format!("sess-{}", NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed));
        Session {
            id,
            remote_addr,
            tx,
            subscriptions: Vec::new(),
            sub_seq: 0,
            dropped: Arc::new(AtomicU64::new(0)),
            last_inbound_ms: Arc::new(AtomicU64::new(now_ms())),
            username: None,
            entitlements: Vec::new(),
            codec: Arc::new(parking_lot::Mutex::new(Codec::Json)),
            protocol_version: cq_protocol::version::DEFAULT_LEGACY_VERSION,
            compression: Arc::new(std::sync::atomic::AtomicU8::new(
                compression_to_u8(cq_protocol::compression::DEFAULT_LEGACY_COMPRESSION),
            )),
        }
    }

    pub fn codec(&self) -> Codec {
        *self.codec.lock()
    }

    pub fn set_codec(&self, codec: Codec) {
        *self.codec.lock() = codec;
    }

    /// Mark that an inbound frame just arrived. Called from the read loop.
    pub fn touch_inbound(&self) {
        self.last_inbound_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn is_authenticated(&self) -> bool {
        self.username.is_some()
    }

    /// True if the authenticated user is allowed to perform `op` on
    /// `topic`. Always returns `true` for un-gated sessions (the caller
    /// is responsible for deciding whether to consult this).
    pub fn can(&self, op: Op, topic: &str) -> bool {
        self.entitlements.iter().any(|e| e.matches(op, topic))
    }

    pub fn next_sub_id(&mut self) -> String {
        self.sub_seq += 1;
        format!("{}:sub-{}", self.id, self.sub_seq)
    }

    /// Push a pre-encoded frame onto the outbound queue. Full queue →
    /// drop + counter.
    pub fn send_frame(&self, frame: OutboundFrame) -> bool {
        match self.tx.try_send(frame) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(session = %self.id, "Outbound queue full; dropping frame");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Serialize `msg` using the session's codec and enqueue.
    pub fn send_message(&self, msg: &CqMessage) -> bool {
        let codec = self.codec();
        match encode_frame(codec, msg) {
            Some(frame) => self.send_frame(frame),
            None => {
                tracing::warn!(session = %self.id, "Failed to serialize message");
                false
            }
        }
    }

    /// Read the session's currently-active wire compression. Cheap
    /// atomic load; safe to call from any task holding the Session.
    pub fn compression(&self) -> cq_protocol::compression::Compression {
        compression_from_u8(self.compression.load(Ordering::Acquire))
    }

    /// Set the session's wire compression — called from the Logon
    /// handler once the negotiation has settled. Subsequent writes
    /// to the TCP outbound stream consult this value.
    pub fn set_compression(&self, c: cq_protocol::compression::Compression) {
        self.compression
            .store(compression_to_u8(c), Ordering::Release);
    }
}

/// Encode the `Compression` enum into a single byte for `AtomicU8`
/// transport. The enum has only two variants today; future entries
/// just need a non-conflicting discriminant.
pub fn compression_to_u8(c: cq_protocol::compression::Compression) -> u8 {
    match c {
        cq_protocol::compression::Compression::None => 0,
        cq_protocol::compression::Compression::Zstd => 1,
    }
}

/// Decode a compression byte back into the enum. Unknown bytes are
/// treated as `None` so a future build that adds a new algorithm
/// downgrades cleanly on an older receiver instead of panicking.
pub fn compression_from_u8(b: u8) -> cq_protocol::compression::Compression {
    match b {
        1 => cq_protocol::compression::Compression::Zstd,
        _ => cq_protocol::compression::Compression::None,
    }
}

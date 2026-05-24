//! Transport-agnostic command dispatcher.
//!
//! Both the WebSocket and TCP transports decode a `CqMessage` and then
//! call `dispatch`. Per-command handlers live here so the protocol
//! semantics stay in one place.

use crate::auth::{Op, SharedAuth};
use crate::queue::QueueRegistry;
use crate::session::{DeliveryRoute, Session, SessionRegistry};
use cq_core::topic::SharedTopic;
use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use dashmap::DashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// Cap on concurrent SOW-snapshot encoders. Each `sow_and_subscribe`
/// against a wide topic kicks off a streaming snapshot that encodes
/// the full result set as JSON; running too many in parallel saturates
/// the runtime (the stress-2k harness reproduces this with 8
/// concurrent PIVOT snapshots over /trades's 865K rows). Excess
/// requests queue on the semaphore — they don't fail, they just wait
/// in line. Tuned via the `CQSERVER_MAX_SNAPSHOT_ENCODERS` env var;
/// default 4.
fn snapshot_encoder_semaphore() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| {
        let n: usize = std::env::var("CQSERVER_MAX_SNAPSHOT_ENCODERS")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &usize| n > 0)
            .unwrap_or(4);
        Semaphore::new(n)
    })
}

// ── Encode-once-fanout snapshot cache ──────────────────────────
//
// Two subscribers arriving close in time with the same (topic, SQL)
// would otherwise each build a full snapshot. The cache lets the
// first arrival build, and every subsequent arrival within the TTL
// gets the SAME `Arc<Vec<Vec<u8>>>` back — one encode, N fanouts.
//
// Concurrency model: per-key tokio Notify. The first thread to claim
// a missing key transitions the entry to `Building` and computes the
// snapshot; subsequent threads see `Building`, await the notify, then
// read `Ready`. After `TTL`, the entry is treated as missing and the
// next caller rebuilds.

use parking_lot::Mutex as PMutex;
use std::collections::HashMap as StdHashMap;
use tokio::sync::Notify;

#[derive(Clone)]
struct CachedSnapshot {
    batches: Arc<Vec<Vec<Vec<u8>>>>,
    expires_at: Instant,
    /// Inserted-at — used by the byte-cap evictor to pick the
    /// oldest entry when over budget.
    inserted_at: Instant,
    /// Total byte size of every Vec<u8> in `batches`. Cached so we
    /// don't iterate on every eviction decision.
    bytes: usize,
}

enum SnapshotCacheState {
    Building(Arc<Notify>),
    Ready(CachedSnapshot),
}

fn snapshot_fanout_cache() -> &'static PMutex<StdHashMap<(String, String), SnapshotCacheState>> {
    static CACHE: OnceLock<PMutex<StdHashMap<(String, String), SnapshotCacheState>>> =
        OnceLock::new();
    CACHE.get_or_init(|| PMutex::new(StdHashMap::new()))
}

/// How long a freshly-built snapshot stays in the cache. Tuned via
/// `CQSERVER_SNAPSHOT_CACHE_TTL_MS`; default 500 ms — enough to
/// absorb a wave of subs joining together, short enough that the
/// data a new sub sees doesn't drift too far from "live".
fn snapshot_cache_ttl() -> Duration {
    let ms: u64 = std::env::var("CQSERVER_SNAPSHOT_CACHE_TTL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(500);
    Duration::from_millis(ms)
}

/// Maximum total bytes the snapshot cache may hold across all
/// entries. When inserting a new entry would push the total over the
/// cap, the evictor drops the oldest entries until it fits. Tuned via
/// `CQSERVER_SNAPSHOT_CACHE_MAX_BYTES`; default 256 MB. Bounding by
/// bytes (rather than by entry count) keeps a single wide-row
/// snapshot from monopolizing the cache.
fn snapshot_cache_max_bytes() -> usize {
    std::env::var("CQSERVER_SNAPSHOT_CACHE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}

/// Compute total bytes occupied by a snapshot. `Vec<Vec<Vec<u8>>>`
/// → sum of inner Vec<u8> lengths. Ignores per-Vec heap overhead;
/// content bytes dominate.
fn snapshot_byte_size(batches: &[Vec<Vec<u8>>]) -> usize {
    batches.iter().flatten().map(|r| r.len()).sum()
}

/// H3 (measurement): project the snapshot's zstd-compressed size by
/// compressing a SAMPLE of concatenated rows (not one row at a time
/// — the dictionary effect from neighboring rows is exactly what
/// permessage-deflate would exploit on the wire).
///
/// Samples up to 200 rows (or all rows in batch 0 if smaller) and
/// concatenates them with a `\n` separator before compressing. This
/// is bounded-CPU regardless of snapshot size while capturing the
/// realistic dictionary effect: identical column names, repeated
/// string values like book / sector / asset class.
///
/// The projection is: `compressed_sample.len() / sample_raw.len() *
/// total_raw_bytes`. For homogeneous-row workloads (every row in a
/// snapshot has the same schema and similar value distributions) the
/// sampled ratio is representative of the whole. Underestimates for
/// snapshots smaller than the sample; that's fine — the measurement
/// is most interesting at large scale.
fn measure_zstd_size(batches: &[Vec<Vec<u8>>]) -> Option<u64> {
    const SAMPLE_ROW_BUDGET: usize = 200;
    let mut sample: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut sampled_rows = 0usize;
    for row in batches.iter().flatten() {
        if sampled_rows >= SAMPLE_ROW_BUDGET {
            break;
        }
        sample.extend_from_slice(row);
        sample.push(b'\n');
        sampled_rows += 1;
    }
    if sample.is_empty() {
        return None;
    }
    let compressed = zstd::encode_all(sample.as_slice(), 3).ok()?;
    let ratio = (compressed.len() as f64) / (sample.len() as f64);
    let total_raw = snapshot_byte_size(batches) as f64;
    Some((total_raw * ratio) as u64)
}

/// Walk every Ready entry, sum their `bytes`, and return the total.
/// Holds the cache lock; cheap (a few dozen entries at most).
fn current_cache_bytes(
    cache: &StdHashMap<(String, String), SnapshotCacheState>,
) -> usize {
    cache
        .values()
        .filter_map(|s| match s {
            SnapshotCacheState::Ready(snap) => Some(snap.bytes),
            _ => None,
        })
        .sum()
}

/// Evict expired + oldest-inserted entries until `current_bytes +
/// incoming <= cap`. Returns nothing; caller has the lock and
/// decides what to do if the incoming alone exceeds the cap (we
/// don't refuse the insert — a single oversized snapshot is still
/// a cache hit for sibling subs).
fn evict_for_budget(
    cache: &mut StdHashMap<(String, String), SnapshotCacheState>,
    incoming_bytes: usize,
    cap: usize,
) {
    // Step 1: drop expired Ready entries unconditionally.
    let now = Instant::now();
    let expired: Vec<(String, String)> = cache
        .iter()
        .filter_map(|(k, v)| match v {
            SnapshotCacheState::Ready(s) if s.expires_at <= now => Some(k.clone()),
            _ => None,
        })
        .collect();
    for k in expired {
        cache.remove(&k);
    }
    // Step 2: while still over budget, drop the oldest Ready entry.
    while current_cache_bytes(cache) + incoming_bytes > cap {
        let oldest_key = cache
            .iter()
            .filter_map(|(k, v)| match v {
                SnapshotCacheState::Ready(s) => Some((k.clone(), s.inserted_at)),
                _ => None,
            })
            .min_by_key(|(_, t)| *t)
            .map(|(k, _)| k);
        match oldest_key {
            Some(k) => {
                cache.remove(&k);
            }
            None => break, // nothing more to evict; incoming will be over budget on its own
        }
    }
}

/// Try to satisfy a snapshot request from the cache. Returns
/// `Some(batches)` on hit; on miss, returns `None` and the caller
/// should build the snapshot itself, then call
/// `publish_snapshot_to_cache` so subsequent arrivals can reuse it.
///
/// Concurrent first-arrivers see `Building(notify)`; they await the
/// notify and then retry the cache.
async fn try_get_or_wait_snapshot(
    topic: &str,
    sql: &str,
) -> Option<Arc<Vec<Vec<Vec<u8>>>>> {
    let key = (topic.to_string(), sql.to_string());
    let notify = {
        let mut cache = snapshot_fanout_cache().lock();
        match cache.get(&key) {
            Some(SnapshotCacheState::Ready(snap)) if snap.expires_at > Instant::now() => {
                metrics::counter!("cq_snapshot_cache_hit").increment(1);
                return Some(snap.batches.clone());
            }
            Some(SnapshotCacheState::Building(n)) => {
                let n = n.clone();
                drop(cache);
                n
            }
            // Either no entry or an expired Ready — claim it.
            _ => {
                let notify = Arc::new(Notify::new());
                cache.insert(key.clone(), SnapshotCacheState::Building(notify.clone()));
                metrics::counter!("cq_snapshot_cache_miss").increment(1);
                return None; // caller will build + publish
            }
        }
    };
    // Wait for the builder to publish.
    notify.notified().await;
    metrics::counter!("cq_snapshot_cache_wait").increment(1);
    let cache = snapshot_fanout_cache().lock();
    if let Some(SnapshotCacheState::Ready(snap)) = cache.get(&key) {
        if snap.expires_at > Instant::now() {
            return Some(snap.batches.clone());
        }
    }
    None
}

/// Publish the just-built snapshot into the cache so waiters wake up
/// and subsequent arrivals within the TTL hit the cached copy.
/// Enforces `CQSERVER_SNAPSHOT_CACHE_MAX_BYTES` — when the incoming
/// snapshot would push total cache bytes over the cap, the oldest
/// Ready entries are evicted to make room. Concurrent waiters on
/// the same key are notified regardless of eviction outcome.
fn publish_snapshot_to_cache(topic: &str, sql: &str, batches: Arc<Vec<Vec<Vec<u8>>>>) {
    let key = (topic.to_string(), sql.to_string());
    let now = Instant::now();
    let expires_at = now + snapshot_cache_ttl();
    let bytes = snapshot_byte_size(&batches);
    let cap = snapshot_cache_max_bytes();
    // H3: project zstd size before we move `batches` into the
    // cache entry, since the measurement borrows by reference.
    let zstd_projection = measure_zstd_size(&batches);

    let mut cache = snapshot_fanout_cache().lock();
    // Wake any Building waiters BEFORE eviction — they may belong
    // to a key we're about to evict in extreme cases, but it's
    // their notify channel we hold.
    let notify_to_wake = match cache.get(&key) {
        Some(SnapshotCacheState::Building(n)) => Some(n.clone()),
        _ => None,
    };
    evict_for_budget(&mut cache, bytes, cap);
    cache.insert(
        key,
        SnapshotCacheState::Ready(CachedSnapshot {
            batches,
            expires_at,
            inserted_at: now,
            bytes,
        }),
    );
    let total_after = current_cache_bytes(&cache);
    drop(cache);
    metrics::gauge!("cq_snapshot_cache_bytes").set(total_after as f64);
    if let Some(zstd_bytes) = zstd_projection {
        metrics::gauge!("cq_snapshot_cache_bytes_zstd").set(zstd_bytes as f64);
        let pct = if bytes > 0 {
            (zstd_bytes as f64) * 100.0 / (bytes as f64)
        } else {
            0.0
        };
        metrics::gauge!("cq_snapshot_compression_ratio_pct").set(pct);
    }
    if let Some(n) = notify_to_wake {
        n.notify_waiters();
    }
}

/// Abandon a Building entry — e.g. when the build itself failed. Wake
/// any waiters so they fall back to building themselves.
fn abandon_snapshot_cache_slot(topic: &str, sql: &str) {
    let key = (topic.to_string(), sql.to_string());
    let mut cache = snapshot_fanout_cache().lock();
    let notify_to_wake = match cache.remove(&key) {
        Some(SnapshotCacheState::Building(n)) => Some(n),
        _ => None,
    };
    drop(cache);
    if let Some(n) = notify_to_wake {
        n.notify_waiters();
    }
}

/// Aggregate of the per-server registries the router consults. Keeps
/// the dispatch signature small as we add new resource types (topics,
/// queues, and future siblings).
#[derive(Clone)]
pub struct RouterContext {
    pub topics: Arc<DashMap<String, SharedTopic>>,
    pub sessions: SessionRegistry,
    pub queues: QueueRegistry,
    pub auth: SharedAuth,
    /// Rows per outbound `sow_batch` frame on the streaming SOW path.
    pub sow_batch_size: usize,
    /// Server-side `(client_name, topic) → last delivered sequence`.
    /// Used by `MOST_RECENT` replay: when a client reconnects with
    /// the same name and asks for `most_recent`, the server starts
    /// the replay from the stored sequence. In-memory; survives
    /// reconnects within the process lifetime but not restarts.
    pub bookmark_store: BookmarkStore,
    /// S21 spillover config. `Some(_)` enables per-subscription disk
    /// overflow: when an outbound queue fills, the delivery layer
    /// appends to a file under `directory` and a background drain
    /// task feeds frames back as the queue catches up. `None` keeps
    /// the legacy "drop on full" behaviour.
    pub spillover: Option<SpilloverContext>,
    /// Read-only mode (replica-reads S1). When `true`, the publish and
    /// delta_publish handlers reject every request with a clear
    /// "read-only follower" error so a misdirected publisher learns
    /// immediately instead of silently writing to a follower's
    /// in-memory state (which would never reach the leader's txlog
    /// and would diverge across followers). Set from
    /// `ServerConfig.replication.role == Standby` in main.rs.
    pub read_only: bool,
}

/// Server-wide spillover configuration, captured in [`RouterContext`]
/// and consulted at route construction time.
#[derive(Clone)]
pub struct SpilloverContext {
    /// Root directory under which per-route spillover files are created.
    /// Created on demand if missing.
    pub directory: std::path::PathBuf,
    /// Maximum on-disk bytes per subscription. Writes that would push
    /// the spool past this limit are dropped (and counted) so a
    /// hopelessly-slow consumer can't grow the spool indefinitely.
    pub max_bytes_per_sub: u64,
}

/// Process-wide map of `(client_name, topic_name) → max sequence
/// delivered`. Updated on every bookmark-replay completion + every
/// live delta. Looked up by `handle_subscribe` when the client asks
/// for `MOST_RECENT`.
pub type BookmarkStore = Arc<DashMap<(String, String), std::sync::atomic::AtomicU64>>;

pub fn new_bookmark_store() -> BookmarkStore {
    Arc::new(DashMap::new())
}

/// Record that a client just received delta with `seq` on `topic`.
/// Monotonic update — won't lower an already-higher mark.
pub fn record_bookmark(store: &BookmarkStore, client_name: &str, topic: &str, seq: u64) {
    use std::sync::atomic::Ordering;
    if client_name.is_empty() {
        return;
    }
    let key = (client_name.to_string(), topic.to_string());
    let entry = store
        .entry(key)
        .or_insert_with(|| std::sync::atomic::AtomicU64::new(0));
    let mut cur = entry.load(Ordering::Relaxed);
    while cur < seq {
        match entry.compare_exchange_weak(cur, seq, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => cur = observed,
        }
    }
}

pub fn lookup_bookmark(store: &BookmarkStore, client_name: &str, topic: &str) -> Option<u64> {
    use std::sync::atomic::Ordering;
    let key = (client_name.to_string(), topic.to_string());
    store.get(&key).map(|e| e.load(Ordering::Relaxed))
}

/// Scan the topic's txlog for the highest sequence whose write
/// timestamp is **strictly less than** `since_ms`, returning that
/// as the implicit bookmark (the replay path picks up at
/// `bookmark + 1`, so we want everything at/after the cutoff to
/// flow). Returns `Ok(None)` if every entry already happened after
/// the cutoff (replay everything) or if the topic isn't persistent.
fn resolve_timestamp_to_seq(
    topics: &Arc<DashMap<String, SharedTopic>>,
    topic_name: &str,
    since_ms: u64,
) -> Result<Option<u64>, String> {
    let topic = topics
        .get(topic_name)
        .ok_or_else(|| format!("topic not found: {topic_name}"))?
        .clone();
    let log_path = topic
        .txlog_path()
        .ok_or_else(|| "topic is not persistent".to_string())?;
    let mut reader = cq_txlog::reader::TxLogReader::open(&log_path)
        .map_err(|e| format!("open txlog: {e}"))?;
    let mut last_before: Option<u64> = None;
    loop {
        match reader.read_next() {
            Ok(Some(entry)) => {
                if entry.timestamp_ms < since_ms {
                    last_before = Some(entry.sequence);
                } else {
                    // Reader is sequential — once we cross the
                    // cutoff there are no more "before" entries.
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => return Err(format!("read txlog: {e}")),
        }
    }
    Ok(last_before)
}

pub fn dispatch(session: &mut Session, mut msg: CqMessage, ctx: &RouterContext) {
    // Gate every command except Logon and Heartbeat when auth is required.
    if ctx.auth.required
        && !session.is_authenticated()
        && !matches!(msg.command, Command::Logon | Command::Heartbeat)
    {
        let _ = session.send_message(&CqMessage::error(
            msg.command_id,
            "Not authenticated — send Logon first",
        ));
        return;
    }

    // Row-level entitlement: AND the user's row_filter into
    // `msg.filter` so it applies to every read-side command
    // (subscribe / sow / sow_and_subscribe / delta_subscribe /
    // sow_delete). Read-side only — writes aren't affected.
    if matches!(
        msg.command,
        Command::Subscribe
            | Command::Sow
            | Command::SowAndSubscribe
            | Command::DeltaSubscribe
            | Command::SowDelete
    ) {
        apply_entitlement_filter(&mut msg, &ctx.auth, session);
    }

    match msg.command {
        Command::Logon => handle_logon(session, msg, &ctx.auth),
        Command::Publish => handle_publish(session, msg, ctx),
        Command::DeltaPublish => handle_delta_publish(session, msg, ctx),
        Command::Sow => handle_sow(session, msg, &ctx.topics, &ctx.auth, ctx.sow_batch_size),
        Command::SowAndSubscribe => handle_sow_and_subscribe(session, msg, ctx, false),
        Command::DeltaSubscribe => handle_sow_and_subscribe(session, msg, ctx, true),
        Command::Subscribe => handle_subscribe(session, msg, ctx),
        Command::Unsubscribe => handle_unsubscribe(session, msg, &ctx.topics, &ctx.sessions),
        Command::SowDelete => handle_sow_delete(session, msg, &ctx.topics, &ctx.auth),
        Command::Heartbeat => {
            let _ = session.send_message(&CqMessage::ack_ok(msg.command_id));
        }
        Command::Ack => {
            // Consumer-side Ack: when `topic` is a queue and a
            // `delivery_id` is set, commit the matching lease so
            // the message isn't redelivered. Otherwise it's just an
            // ack frame the server can ignore (clients don't need
            // to ack server-originated messages today).
            if let (Some(topic), Some(did)) = (&msg.topic, msg.delivery_id) {
                if let Some(q) = ctx.queues.get(topic) {
                    q.ack(did);
                }
            }
        }
        Command::Pause => {
            if let Some(sid) = msg.sub_id.as_deref() {
                if let Some(route) = ctx.sessions.get(sid) {
                    route
                        .paused
                        .store(true, std::sync::atomic::Ordering::Release);
                }
            }
        }
        Command::Resume => {
            if let Some(sid) = msg.sub_id.as_deref() {
                if let Some(route) = ctx.sessions.get(sid) {
                    route
                        .paused
                        .store(false, std::sync::atomic::Ordering::Release);
                    route.resume_notify.notify_waiters();
                }
            }
        }
        _ => {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                &format!("Unsupported command: {:?}", msg.command),
            ));
        }
    }
}

/// Per-command entitlement check. Returns `true` if auth is not required
/// or the session is allowed; sends an error ack and returns `false`
/// otherwise.
fn check_entitlement(
    session: &Session,
    cid: &Option<String>,
    auth: &SharedAuth,
    op: Op,
    topic: &str,
) -> bool {
    if !auth.required {
        return true;
    }
    if session.can(op, topic) {
        return true;
    }
    let _ = session.send_message(&CqMessage::error(
        cid.clone(),
        &format!("Forbidden: missing {:?} entitlement for {}", op, topic),
    ));
    false
}

fn handle_logon(session: &mut Session, msg: CqMessage, auth: &SharedAuth) {
    // S28 — version negotiation runs FIRST, regardless of auth. A
    // client whose supported set is disjoint from ours has nothing
    // to gain from authenticating, so we fail before even reading
    // credentials.
    let client_versions = msg.protocol_versions.clone().unwrap_or_default();
    let outcome = cq_protocol::version::negotiate(
        &client_versions,
        cq_protocol::version::SUPPORTED_VERSIONS,
    );
    let negotiated = match outcome {
        cq_protocol::version::NegotiationOutcome::Negotiated(v) => v,
        cq_protocol::version::NegotiationOutcome::NoOverlap => {
            tracing::warn!(
                session = %session.id,
                client_versions = ?client_versions,
                server_versions = ?cq_protocol::version::SUPPORTED_VERSIONS,
                "Logon rejected: no overlapping protocol version"
            );
            metrics::counter!(
                "cq_logon_total",
                "result" => "version_mismatch"
            )
            .increment(1);
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "No mutually supported protocol version",
            ));
            return;
        }
    };
    session.protocol_version = negotiated;

    // S27 — compression negotiation, same handshake shape. An empty
    // client list (or pre-S27 client) implies Compression::None,
    // preserving wire compatibility for older clients.
    let client_compressions = msg
        .compressions
        .clone()
        .unwrap_or_default();
    let compression = match cq_protocol::compression::negotiate(
        &client_compressions,
        cq_protocol::compression::SUPPORTED_COMPRESSIONS,
    ) {
        cq_protocol::compression::NegotiationOutcome::Negotiated(c) => c,
        cq_protocol::compression::NegotiationOutcome::NoOverlap => {
            // Disjoint compressions can only happen if the client
            // explicitly rules out None — which is an unusual choice.
            // Reject so the client knows.
            tracing::warn!(
                session = %session.id,
                client = ?client_compressions,
                "Logon rejected: no overlapping compression algorithm"
            );
            metrics::counter!(
                "cq_logon_total",
                "result" => "compression_mismatch"
            )
            .increment(1);
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "No mutually supported compression",
            ));
            return;
        }
    };
    session.set_compression(compression);

    // When auth isn't required and no creds are supplied, the logon
    // is effectively a version-negotiation handshake — accept and
    // echo the negotiated version. (Existing entitlement-checked
    // commands still gate per-message; an unauthenticated session
    // just doesn't get user-specific entitlements.)
    let creds = msg.data.as_ref().and_then(|d| d.as_object());
    let user = creds.and_then(|m| m.get("user").and_then(|v| v.as_str()));
    let pass = creds.and_then(|m| m.get("password").and_then(|v| v.as_str()));
    // S16 — JWT path. The Logon frame may carry a `token` field
    // instead of (or alongside) `user`/`password`. When present and
    // the AuthStore has a JWT validator configured, verify the
    // token; on success the user identity comes from its claims.
    let token = creds.and_then(|m| m.get("token").and_then(|v| v.as_str()));
    if let Some(t) = token {
        if auth.has_jwt() {
            match auth.verify_jwt(t) {
                Some(matched) => {
                    session.username = Some(matched.username.clone());
                    session.entitlements = matched.entitlements.clone();
                    tracing::info!(
                        target: "cq_audit",
                        session = %session.id,
                        user = %matched.username,
                        entitlements = matched.entitlements.len(),
                        protocol_version = negotiated,
                        event = "logon_ok_jwt",
                        "Logon ok (jwt)"
                    );
                    metrics::counter!(
                        "cq_logon_total",
                        "result" => "ok_jwt"
                    )
                    .increment(1);
                    let mut ack = CqMessage::ack_ok(msg.command_id);
                    ack.protocol_versions = Some(vec![negotiated]);
                    ack.compressions = Some(vec![compression]);
                    let _ = session.send_message(&ack);
                    return;
                }
                None => {
                    tracing::warn!(
                        target: "cq_audit",
                        session = %session.id,
                        event = "logon_fail_jwt",
                        "Logon rejected: invalid JWT"
                    );
                    metrics::counter!(
                        "cq_logon_total",
                        "result" => "fail_jwt"
                    )
                    .increment(1);
                    let _ = session.send_message(&CqMessage::error(
                        msg.command_id,
                        "Invalid JWT",
                    ));
                    return;
                }
            }
        }
    }

    let credentials = match (user, pass) {
        (Some(u), Some(p)) => Some((u, p)),
        _ => None,
    };

    if credentials.is_none() {
        if auth.required {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "Missing user/password in logon data",
            ));
            return;
        }
        let mut ack = CqMessage::ack_ok(msg.command_id);
        ack.protocol_versions = Some(vec![negotiated]);
        ack.compressions = Some(vec![compression]);
        tracing::info!(
            session = %session.id,
            protocol_version = negotiated,
            compression = ?compression,
            "Logon ok (no credentials, auth not required)"
        );
        metrics::counter!("cq_logon_total", "result" => "ok").increment(1);
        let _ = session.send_message(&ack);
        return;
    }
    let (user, pass) = credentials.unwrap();

    match auth.verify(user, pass) {
        Some(matched) => {
            session.username = Some(matched.username.clone());
            session.entitlements = matched.entitlements.clone();
            // S25 audit event: explicit `target = "cq_audit"` so the
            // tracing-subscriber Registry can route it to the
            // dedicated audit sink (e.g., audit.log) while the same
            // session keeps its other events on the operational sink.
            tracing::info!(
                target: "cq_audit",
                session = %session.id,
                user = %matched.username,
                entitlements = matched.entitlements.len(),
                protocol_version = negotiated,
                event = "logon_ok",
                "Logon ok"
            );
            metrics::counter!("cq_logon_total", "result" => "ok").increment(1);
            let mut ack = CqMessage::ack_ok(msg.command_id);
            ack.protocol_versions = Some(vec![negotiated]);
            ack.compressions = Some(vec![compression]);
            let _ = session.send_message(&ack);
        }
        None => {
            tracing::warn!(
                target: "cq_audit",
                session = %session.id,
                attempted_user = %user,
                event = "logon_fail",
                "Logon failed"
            );
            metrics::counter!("cq_logon_total", "result" => "fail").increment(1);
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "Invalid credentials",
            ));
        }
    }
}

fn handle_publish(session: &mut Session, msg: CqMessage, ctx: &RouterContext) {
    handle_publish_inner(session, msg, ctx, false);
}

fn handle_delta_publish(session: &mut Session, msg: CqMessage, ctx: &RouterContext) {
    handle_publish_inner(session, msg, ctx, true);
}

fn handle_publish_inner(
    session: &mut Session,
    msg: CqMessage,
    ctx: &RouterContext,
    delta_mode: bool,
) {
    // Replica-reads S1: a follower must never accept a publish. We
    // reject before any topic/auth/payload validation so the error
    // shape is consistent — every misdirected publish gets the same
    // "publish to leader" message regardless of what else is wrong.
    // Metric counts the misroute so operators can tell when their LB
    // is sending writes to the wrong tier.
    if ctx.read_only {
        metrics::counter!("cq_publish_rejected_read_only_total").increment(1);
        let _ = session.send_message(&CqMessage::error(
            msg.command_id,
            "read-only follower; publish to leader",
        ));
        return;
    }

    let topic_name = match &msg.topic {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };

    if !check_entitlement(session, &msg.command_id, &ctx.auth, Op::Publish, &topic_name) {
        return;
    }

    let data_value = match &msg.data {
        Some(v @ serde_json::Value::Object(_)) => v.clone(),
        _ => {
            let _ = session
                .send_message(&CqMessage::error(msg.command_id, "Missing or invalid data"));
            return;
        }
    };

    // Queue path: each publish goes to exactly one consumer. Queues
    // don't support delta merges (no per-key state to merge into);
    // reject the request so the publisher knows it's a misuse.
    if let Some(queue) = ctx.queues.get(&topic_name) {
        if delta_mode {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "delta_publish not supported on queue topics",
            ));
            return;
        }
        let started = Instant::now();
        let seq = queue.publish(data_value, &ctx.sessions);
        let elapsed_us = started.elapsed().as_micros() as f64;
        metrics::histogram!("cq_publish_latency_us", "topic" => topic_name.clone())
            .record(elapsed_us);
        let mut ack = CqMessage::ack_ok(msg.command_id);
        ack.sequence = Some(seq);
        let _ = session.send_message(&ack);
        return;
    }

    // SOW topic path.
    let data = match data_value {
        serde_json::Value::Object(m) => m,
        _ => unreachable!(),
    };

    if let Some(topic) = ctx.topics.get(&topic_name) {
        let started = Instant::now();
        let result = if delta_mode {
            topic.delta_upsert_map(&data)
        } else {
            topic.upsert_map(&data)
        };
        match result {
            Ok(seq) => {
                let elapsed_us = started.elapsed().as_micros() as f64;
                let counter = if delta_mode {
                    "cq_delta_publish_total"
                } else {
                    "cq_publish_total"
                };
                metrics::counter!(counter, "topic" => topic_name.clone()).increment(1);
                metrics::histogram!("cq_publish_latency_us", "topic" => topic_name.clone())
                    .record(elapsed_us);
                let mut ack = CqMessage::ack_ok(msg.command_id);
                ack.sequence = Some(seq);
                let _ = session.send_message(&ack);
            }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!("Publish failed: {}", e),
                ));
            }
        }
    } else {
        let _ = session.send_message(&CqMessage::error(
            msg.command_id,
            &format!("Topic not found: {}", topic_name),
        ));
    }
}

fn handle_sow(
    session: &mut Session,
    msg: CqMessage,
    topics: &Arc<DashMap<String, SharedTopic>>,
    auth: &SharedAuth,
    sow_batch_size: usize,
) {
    let topic_name = match &msg.topic {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };

    if !check_entitlement(session, &msg.command_id, auth, Op::Sow, &topic_name) {
        return;
    }

    let sql = build_sql(&msg);
    let sub_id = msg.command_id.clone().unwrap_or_default();

    // Resolve the topic Arc before spawning so the task owns it.
    let topic = match topics.get(&topic_name) {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                &format!("Topic not found: {}", topic_name),
            ));
            return;
        }
    };

    let tx = session.tx.clone();
    let codec_slot = session.codec.clone();
    let session_id = session.id.clone();
    let topic_label = topic_name.clone();
    let batch_size = sow_batch_size;

    tokio::spawn(async move {
        deliver_streaming_snapshot(
            topic,
            sql,
            None, // one-shot SOW carries no ack — group_begin/sow/group_end is enough
            sub_id,
            topic_label,
            session_id,
            codec_slot,
            tx,
            batch_size,
        )
        .await;
    });
}

fn handle_sow_and_subscribe(
    session: &mut Session,
    msg: CqMessage,
    ctx: &RouterContext,
    sparse: bool,
) {
    let topic_name = match &msg.topic {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };

    if !check_entitlement(session, &msg.command_id, &ctx.auth, Op::Subscribe, &topic_name) {
        return;
    }

    // Queue subscribe path: no snapshot, no bookmark, no predicates.
    // The subscriber joins the queue's round-robin consumer set.
    if let Some(queue) = ctx.queues.get(&topic_name) {
        let sub_id = session.next_sub_id();
        session.subscriptions.push(sub_id.clone());
        ctx.sessions.insert(
            sub_id.clone(),
            DeliveryRoute::with_codec(
                session.tx.clone(),
                topic_name.clone(),
                session.codec(),
            )
            .with_session(session.id.clone()),
        );
        queue.add_consumer(sub_id.clone(), &ctx.sessions);
        let mut ack = CqMessage::ack_ok(msg.command_id);
        ack.sub_id = Some(sub_id);
        let _ = session.send_message(&ack);
        return;
    }

    let topics = &ctx.topics;
    let registry = &ctx.sessions;

    let sql = build_sql(&msg);

    // Resolve the effective replay starting point. Precedence (most
    // specific to least):
    //   1. Explicit `bookmark` (sequence number) — verbatim.
    //   2. `since_timestamp_ms` — scan the txlog for the first entry
    //      strictly at or after this wall-clock and replay from there.
    //   3. `most_recent: true` + `client_name` — look up the
    //      server-side per-client store; replay from there if present.
    //   4. None of the above — live subscription with snapshot.
    let resolved_bookmark: Option<u64> = if let Some(bm) = msg.bookmark {
        Some(bm)
    } else if let Some(since_ms) = msg.since_timestamp_ms {
        match resolve_timestamp_to_seq(topics, &topic_name, since_ms) {
            Ok(b) => b,
            Err(reason) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id.clone(),
                    &format!("since_timestamp_ms: {reason}"),
                ));
                return;
            }
        }
    } else if msg.most_recent {
        let cname = msg
            .client_name
            .as_deref()
            .or(session.username.as_deref())
            .unwrap_or("");
        if cname.is_empty() {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id.clone(),
                "most_recent requires client_name (via logon or msg)",
            ));
            return;
        }
        lookup_bookmark(&ctx.bookmark_store, cname, &topic_name)
    } else {
        None
    };

    // Bookmark path: replay txlog from `bookmark+1`, then go live. No
    // snapshot — the client is reconstructing from the delta stream.
    if let Some(bookmark) = resolved_bookmark {
        let cname = msg.client_name.clone().or_else(|| session.username.clone());
        handle_bookmark_subscribe(
            session,
            msg.command_id,
            topic_name,
            &sql,
            bookmark,
            topics,
            registry,
            cname,
            ctx.bookmark_store.clone(),
            ctx.spillover.as_ref(),
        );
        return;
    }

    let sub_id = session.next_sub_id();

    if let Some(topic) = topics.get(&topic_name) {
        let conflation_ms = topic.conflation_ms();
        // For `send_keys`, only the *snapshot* projection should be
        // keys-only — the live delta path still needs the original
        // projection (all columns by default) so sparse diff_update
        // can detect changes on any field.
        let snapshot_sql = if sparse && msg.send_keys {
            let key_cols = topic.key_column_names();
            if !key_cols.is_empty() {
                build_keys_only_sql(&sql, &key_cols)
            } else {
                sql.clone()
            }
        } else {
            sql.clone()
        };
        // Register the subscription WITHOUT materializing a Vec<Map>
        // snapshot — wire delivery uses `query_streaming` below, so
        // there's no caller for the materialized snapshot. This saves
        // ~1 GB transient heap per /trades-class subscriber.
        let subscribe_result = topic.subscribe_register(sub_id.clone(), &sql, sparse);
        match subscribe_result {
            Ok(_) => {
                session.subscriptions.push(sub_id.clone());
                registry.insert(
                    sub_id.clone(),
                    build_route_with_spillover(
                        session.tx.clone(),
                        topic_name.clone(),
                        sub_id.clone(),
                        conflation_ms,
                        session.codec(),
                        session.id.clone(),
                        msg.client_name.clone().or_else(|| session.username.clone()),
                        Some(ctx.bookmark_store.clone()),
                        ctx.spillover.as_ref(),
                    ),
                );
                metrics::gauge!("cq_subscriptions_active", "topic" => topic_name.clone())
                    .increment(1.0);

                // H4: ack-first, fast path + safety net.
                //
                // The previous wiring sent the ack from INSIDE the
                // spawned `deliver_streaming_snapshot` via
                // `tx.send().await`. That works but adds a
                // tokio-spawn scheduling step between
                // `subscribe_register` and the ack going on the
                // wire. Under heavy concurrent subscribe pressure
                // (1000+ pending spawns) that scheduling step can
                // add seconds before the ack hits the client.
                //
                // The fix: in the dispatch context, try_send the
                // ack synchronously (instant for the common case of
                // an empty/near-empty outbound queue). On a full
                // queue, fall back to a spawned awaited send so we
                // never silently drop. The spawned snapshot task is
                // then called with `ack_cmd_id=None` because the
                // ack is already handled.
                {
                    let mut ack = CqMessage::ack_ok(msg.command_id.clone());
                    ack.sub_id = Some(sub_id.clone());
                    let codec = session.codec();
                    if let Some(frame) = crate::session::encode_frame(codec, &ack) {
                        match session.tx.try_send(frame) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(frame)) => {
                                // Queue was full at try-time — fall back
                                // to an awaited send so the ack isn't
                                // dropped. Runs on a spawned task so the
                                // dispatch loop is never blocked here.
                                let tx = session.tx.clone();
                                tokio::spawn(async move {
                                    let _ = tx.send(frame).await;
                                });
                            }
                            Err(_) => {
                                // Channel closed — session is going
                                // away. The subscribe still succeeded
                                // server-side; cleanup happens on
                                // disconnect.
                            }
                        }
                    }
                }

                let tx = session.tx.clone();
                let codec_slot = session.codec.clone();
                let session_id = session.id.clone();
                let snapshot_sub_id = sub_id.clone();
                let snapshot_topic = topic_name.clone();
                let batch_size = ctx.sow_batch_size;
                let topic_for_task = topic.clone();
                let sql_for_task = snapshot_sql.clone();
                tokio::spawn(async move {
                    // ack_cmd_id = None: the ack was sent
                    // synchronously above.
                    deliver_streaming_snapshot(
                        topic_for_task,
                        sql_for_task,
                        None,
                        snapshot_sub_id,
                        snapshot_topic,
                        session_id,
                        codec_slot,
                        tx,
                        batch_size,
                    )
                    .await;
                });
            }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    None,
                    &format!("Subscribe error: {}", e),
                ));
            }
        }
    } else {
        let _ = session.send_message(&CqMessage::error(
            msg.command_id,
            &format!("Topic not found: {}", topic_name),
        ));
    }
}

fn handle_bookmark_subscribe(
    session: &mut Session,
    command_id: Option<String>,
    topic_name: String,
    sql: &str,
    bookmark: u64,
    topics: &Arc<DashMap<String, SharedTopic>>,
    registry: &SessionRegistry,
    client_name: Option<String>,
    bookmark_store: BookmarkStore,
    spillover_ctx: Option<&SpilloverContext>,
) {
    let sub_id = session.next_sub_id();

    let topic = match topics.get(&topic_name) {
        Some(t) => t,
        None => {
            let _ = session.send_message(&CqMessage::error(
                command_id,
                &format!("Topic not found: {}", topic_name),
            ));
            return;
        }
    };

    let log_path = match topic.txlog_path() {
        Some(p) => p,
        None => {
            let _ = session.send_message(&CqMessage::error(
                command_id,
                "Bookmark replay requires a persistent topic",
            ));
            return;
        }
    };

    let conflation_ms = topic.conflation_ms();
    let (query, captured) = match topic.subscribe_with_bookmark(sub_id.clone(), sql) {
        Ok(x) => x,
        Err(e) => {
            let _ = session.send_message(&CqMessage::error(
                command_id,
                &format!("Subscribe error: {}", e),
            ));
            return;
        }
    };

    session.subscriptions.push(sub_id.clone());
    registry.insert(
        sub_id.clone(),
        build_route_with_spillover(
            session.tx.clone(),
            topic_name.clone(),
            sub_id.clone(),
            conflation_ms,
            session.codec(),
            session.id.clone(),
            client_name,
            Some(bookmark_store),
            spillover_ctx,
        ),
    );
    metrics::gauge!("cq_subscriptions_active", "topic" => topic_name.clone()).increment(1.0);

    // Ack with the captured high-water sequence — the client knows that
    // every delta numbered ≤ captured is part of the replay window.
    let mut ack = CqMessage::ack_ok(command_id);
    ack.sub_id = Some(sub_id.clone());
    ack.sequence = Some(captured);
    let _ = session.send_message(&ack);

    // Capture handles needed inside the replay task before we lose
    // mutable access to `session`.
    let route = registry.get(&sub_id).map(|r| r.clone());
    let tx = session.tx.clone();
    let codec = session.codec();

    // Move the replay loop into a tokio task so we can:
    //   1. Yield between entries (avoids blocking the read loop).
    //   2. Await on the route's `resume_notify` when the client
    //      issues a Pause command mid-replay.
    let topic_name_for_task = topic_name.clone();
    let sub_id_for_task = sub_id.clone();
    let query_for_task = query.clone();
    let schema = topic.schema();
    tokio::spawn(async move {
        use crate::session::encode_frame;
        let mut reader = match cq_txlog::reader::TxLogReader::open(&log_path) {
            Ok(r) => r,
            Err(e) => {
                let err = CqMessage::error(None, &format!("Replay open failed: {e}"));
                if let Some(f) = encode_frame(codec, &err) {
                    let _ = tx.send(f).await;
                }
                return;
            }
        };
        let mut replayed: u64 = 0;
        loop {
            // Pause handshake: if paused, await Resume. The
            // `resume_notify` is `notify_waiters`-style — i.e. it
            // only wakes already-waiting callers — so we re-check
            // the atomic in a loop to be race-safe.
            if let Some(r) = &route {
                while r.paused.load(std::sync::atomic::Ordering::Acquire) {
                    r.resume_notify.notified().await;
                }
            }
            match reader.read_next() {
                Ok(Some(entry)) => {
                    if entry.sequence <= bookmark {
                        continue;
                    }
                    if entry.sequence > captured {
                        break;
                    }
                    if entry.is_tombstone() {
                        let mut row_data = serde_json::Map::new();
                        row_data.insert(
                            "_key".into(),
                            serde_json::Value::String(entry.key.clone()),
                        );
                        let mut m =
                            CqMessage::delta(&sub_id_for_task, "remove", row_data);
                        m.sequence = Some(entry.sequence);
                        if let Some(f) = encode_frame(codec, &m) {
                            let _ = tx.send(f).await;
                        }
                        replayed += 1;
                    } else {
                        let parsed: serde_json::Value =
                            match serde_json::from_slice(&entry.payload) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                        if let serde_json::Value::Object(map) = parsed {
                            if cq_core::predicate::predicate_matches_json(
                                &query_for_task.predicate,
                                &schema,
                                &map,
                            ) {
                                let mut m =
                                    CqMessage::delta(&sub_id_for_task, "add", map);
                                m.sequence = Some(entry.sequence);
                                if let Some(f) = encode_frame(codec, &m) {
                                    let _ = tx.send(f).await;
                                }
                                replayed += 1;
                            }
                        }
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let err = CqMessage::error(
                        None,
                        &format!("Replay aborted on corruption: {e}"),
                    );
                    if let Some(f) = encode_frame(codec, &err) {
                        let _ = tx.send(f).await;
                    }
                    break;
                }
            }
        }
        metrics::counter!(
            "cq_bookmark_replay_total",
            "topic" => topic_name_for_task.clone()
        )
        .increment(1);
        metrics::histogram!(
            "cq_bookmark_replay_entries",
            "topic" => topic_name_for_task.clone()
        )
        .record(replayed as f64);
        tracing::info!(
            sub = %sub_id_for_task,
            bookmark,
            captured,
            replayed,
            "Bookmark replay complete"
        );
    });
}

fn handle_subscribe(session: &mut Session, msg: CqMessage, ctx: &RouterContext) {
    let topic_name = match &msg.topic {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };

    if !check_entitlement(session, &msg.command_id, &ctx.auth, Op::Subscribe, &topic_name) {
        return;
    }

    // Queue subscribe — same shape as `sow_and_subscribe` for a queue.
    if let Some(queue) = ctx.queues.get(&topic_name) {
        let sub_id = session.next_sub_id();
        session.subscriptions.push(sub_id.clone());
        ctx.sessions.insert(
            sub_id.clone(),
            DeliveryRoute::with_codec(
                session.tx.clone(),
                topic_name.clone(),
                session.codec(),
            )
            .with_session(session.id.clone()),
        );
        queue.add_consumer(sub_id.clone(), &ctx.sessions);
        let mut ack = CqMessage::ack_ok(msg.command_id);
        ack.sub_id = Some(sub_id);
        let _ = session.send_message(&ack);
        return;
    }

    let topics = &ctx.topics;
    let registry = &ctx.sessions;
    let sql = build_sql(&msg);
    let sub_id = session.next_sub_id();

    if let Some(topic) = topics.get(&topic_name) {
        let conflation_ms = topic.conflation_ms();
        // Live-only subscribe (no snapshot delivery on this path) —
        // register without materializing any Vec<Map>.
        match topic.subscribe_register(sub_id.clone(), &sql, false) {
            Ok(_) => {
                session.subscriptions.push(sub_id.clone());
                registry.insert(
                    sub_id.clone(),
                    build_route_with_spillover(
                        session.tx.clone(),
                        topic_name.clone(),
                        sub_id.clone(),
                        conflation_ms,
                        session.codec(),
                        session.id.clone(),
                        msg.client_name.clone().or_else(|| session.username.clone()),
                        Some(ctx.bookmark_store.clone()),
                        ctx.spillover.as_ref(),
                    ),
                );
                metrics::gauge!("cq_subscriptions_active", "topic" => topic_name.clone())
                    .increment(1.0);
                // Ack via backpressure — see notes in handle_sow_and_subscribe.
                let tx = session.tx.clone();
                let codec_slot = session.codec.clone();
                let ack_cmd_id = msg.command_id.clone();
                let ack_sub_id = sub_id.clone();
                tokio::spawn(async move {
                    use crate::session::encode_frame;
                    let codec = *codec_slot.lock();
                    let mut ack = CqMessage::ack_ok(ack_cmd_id);
                    ack.sub_id = Some(ack_sub_id);
                    if let Some(f) = encode_frame(codec, &ack) {
                        let _ = tx.send(f).await;
                    }
                });
            }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    None,
                    &format!("Subscribe error: {}", e),
                ));
            }
        }
    } else {
        let _ = session.send_message(&CqMessage::error(
            msg.command_id,
            &format!("Topic not found: {}", topic_name),
        ));
    }
}

fn handle_unsubscribe(
    session: &mut Session,
    msg: CqMessage,
    topics: &Arc<DashMap<String, SharedTopic>>,
    registry: &SessionRegistry,
) {
    let sub_id = match &msg.sub_id {
        Some(id) => id.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing sub_id"));
            return;
        }
    };

    session.subscriptions.retain(|s| s != &sub_id);
    if let Some((_, route)) = registry.remove(&sub_id) {
        metrics::gauge!("cq_subscriptions_active", "topic" => route.topic.clone())
            .decrement(1.0);
        if let Some(topic) = topics.get(&route.topic) {
            topic.unsubscribe(&sub_id);
        }
        // Queue cleanup happens via `cleanup_session_with_queues` on
        // disconnect. Stale entries here are safe — the queue's
        // `deliver` skips routes that aren't in the session registry.
    }

    let _ = session.send_message(&CqMessage::ack_ok(msg.command_id));
}

fn handle_sow_delete(
    session: &mut Session,
    msg: CqMessage,
    topics: &Arc<DashMap<String, SharedTopic>>,
    auth: &SharedAuth,
) {
    let topic_name = match &msg.topic {
        Some(t) => t.clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };

    if !check_entitlement(session, &msg.command_id, auth, Op::Delete, &topic_name) {
        return;
    }

    let key = msg.data.as_ref().and_then(|d| {
        d.as_object()
            .and_then(|m| m.values().next())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    });

    if let Some(key) = key {
        if let Some(topic) = topics.get(&topic_name) {
            match topic.delete(&key) {
                Ok(seq_opt) => {
                    let mut ack = CqMessage::ack_ok(msg.command_id);
                    ack.sequence = seq_opt;
                    let _ = session.send_message(&ack);
                }
                Err(e) => {
                    let _ = session.send_message(&CqMessage::error(
                        msg.command_id,
                        &format!("Delete failed: {}", e),
                    ));
                }
            }
        }
    } else {
        let _ = session.send_message(&CqMessage::error(msg.command_id, "Missing key in data"));
    }
}

fn build_route_with_spillover(
    tx: crate::session::OutboundTx,
    topic: String,
    sub_id: String,
    conflation_ms: Option<u64>,
    codec: cq_protocol::serialization::Codec,
    session_id: String,
    client_name: Option<String>,
    bookmark_store: Option<BookmarkStore>,
    spillover_ctx: Option<&SpilloverContext>,
) -> DeliveryRoute {
    let route = match conflation_ms {
        Some(ms) if ms > 0 => DeliveryRoute::with_conflation_codec(
            tx.clone(),
            topic,
            sub_id.clone(),
            Duration::from_millis(ms),
            codec,
        )
        .with_session(session_id),
        _ => DeliveryRoute::with_codec(tx.clone(), topic, codec)
            .with_session(session_id),
    };
    let route = route
        .with_client_name(client_name)
        .with_bookmark_store(bookmark_store);

    // S21: opt-in disk spillover. Create a fresh per-route file
    // under the configured directory, attach it to the route, and
    // spawn the drain task that feeds frames back from disk as the
    // outbound queue catches up.
    if let Some(ctx) = spillover_ctx {
        let safe_id = sanitize_filename(&sub_id);
        let path = ctx.directory.join(format!("{}.spill", safe_id));
        match crate::spillover::Spillover::open(path, ctx.max_bytes_per_sub) {
            Ok(sp) => {
                let sp = std::sync::Arc::new(sp);
                let drain_handle = crate::session::spawn_spillover_drain(
                    sub_id.clone(),
                    tx.clone(),
                    sp.clone(),
                );
                // Detach the drain task; it exits naturally when the
                // outbound tx closes (sub teardown).
                drop(drain_handle);
                return route.with_spillover(sp);
            }
            Err(e) => {
                tracing::warn!(
                    sub = %sub_id,
                    error = %e,
                    "Spillover open failed; falling back to drop-on-full"
                );
            }
        }
    }
    route
}

/// Translate a subscription id into a filename-safe slug. Sub ids are
/// server-assigned (`sess-N:sub-M`) but defensively sanitise anything
/// outside `[A-Za-z0-9_-.]` to `_`.
fn sanitize_filename(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Replace the SELECT projection in `sql` with only the topic's key
/// columns. Preserves the rest of the statement (WHERE / ORDER BY /
/// LIMIT) verbatim. Used by `send_keys` delta-subscribe so the
/// initial snapshot delivers only key fields.
fn build_keys_only_sql(sql: &str, key_cols: &[String]) -> String {
    let upper = sql.to_ascii_uppercase();
    let Some(from_pos) = find_keyword(&upper, "FROM") else {
        return sql.to_string();
    };
    let cols_csv: Vec<String> = key_cols
        .iter()
        .map(|c| {
            // Dotted-path columns need backticks/quoting for sqlparser
            // since `.` is otherwise interpreted as table.column. The
            // existing SQL pipeline accepts unquoted identifiers in
            // most cases; key columns in cqserver are typically simple,
            // so leave as-is. Quote on demand if a `.` shows up.
            if c.contains('.') {
                format!("\"{}\"", c)
            } else {
                c.clone()
            }
        })
        .collect();
    let mut out = String::with_capacity(sql.len() + 32);
    out.push_str("SELECT ");
    out.push_str(&cols_csv.join(", "));
    out.push(' ');
    out.push_str(&sql[from_pos..]);
    out
}

/// AND the user's row-level entitlement filter (if any) into
/// `msg.filter`. Called on every command that admits a `filter`
/// parameter — `sow`, `subscribe`, `sow_and_subscribe`,
/// `delta_subscribe`, `sow_delete` — to enforce the restriction
/// server-side regardless of what the client sent.
fn apply_entitlement_filter(msg: &mut CqMessage, auth: &SharedAuth, session: &Session) {
    if !auth.required || session.username.is_none() {
        return;
    }
    let username = session.username.as_deref().unwrap_or("");
    let Some(ent_filter) = auth.row_filter_for(username) else {
        return;
    };
    if msg.sql.is_some() {
        // Raw-SQL clients aren't gated by the WHERE-rewrite path;
        // their full SELECT is forwarded as-is. We don't try to
        // splice into the parsed SQL here — that's S6's stretch
        // goal. For now, deny the raw SQL when a row filter is
        // configured to avoid silently leaking restricted rows.
        msg.sql = None;
        msg.filter = Some(ent_filter);
        return;
    }
    let combined = match msg.filter.as_deref() {
        Some(client) if !client.is_empty() => format!("({client}) AND ({ent_filter})"),
        _ => ent_filter,
    };
    msg.filter = Some(combined);
}

fn build_sql(msg: &CqMessage) -> String {
    // Inline `sql` field wins: the caller passes a full SELECT
    // verbatim (used for aggregate queries that can't fit the
    // projection-only options string). The FROM table is rewritten
    // to the canonical placeholder so the topic name (which may
    // contain `/`, etc.) doesn't reach the SQL parser.
    if let Some(raw) = msg.sql.as_deref() {
        return rewrite_from_to_t(raw);
    }

    // The FROM table name is irrelevant to execution — the topic was already
    // resolved from msg.topic. Use a fixed identifier to avoid feeding
    // arbitrary characters from the topic name (e.g. leading "/") into the
    // SQL parser. Sidesteps audit item C-4.
    let filter = msg.filter.as_deref().unwrap_or("");
    let options = msg.options.as_deref().unwrap_or("");

    let select = parse_option(options, "select").unwrap_or("*".into());
    let order_by = parse_option(options, "order_by");
    let top_n = parse_option(options, "top_n");

    let mut sql = format!("SELECT {} FROM t", select);
    if !filter.is_empty() {
        sql.push_str(&format!(" WHERE {}", filter));
    }
    if let Some(ob) = order_by {
        sql.push_str(&format!(" ORDER BY {}", ob));
    }
    if let Some(n) = top_n {
        sql.push_str(&format!(" LIMIT {}", n));
    }
    sql
}

/// Replace whatever follows `FROM` (up to the next clause keyword or
/// end-of-input) with the canonical placeholder identifier `t`. Lets
/// callers write `... FROM /agg-trades ...` without the topic name
/// being interpreted as a SQL identifier.
fn rewrite_from_to_t(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    let Some(from_pos) = find_keyword(&upper, "FROM") else {
        return sql.to_string();
    };
    let after_from = from_pos + 4;
    // Find the start of the next clause boundary.
    let clauses = [" WHERE ", " GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "];
    let mut end = sql.len();
    for kw in clauses {
        if let Some(p) = upper[after_from..].find(kw) {
            let abs = after_from + p;
            if abs < end {
                end = abs;
            }
        }
    }
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..after_from]);
    out.push_str(" t");
    out.push_str(&sql[end..]);
    out
}

/// Find a keyword as a standalone token (preceded by start-of-input
/// or whitespace, followed by whitespace). Avoids matching inside
/// identifiers like `from_user`.
fn find_keyword(upper: &str, kw: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let kw_bytes = kw.as_bytes();
    let mut i = 0;
    while i + kw_bytes.len() <= bytes.len() {
        if &bytes[i..i + kw_bytes.len()] == kw_bytes {
            let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
            let after = i + kw_bytes.len();
            let next_ok = after == bytes.len() || bytes[after].is_ascii_whitespace();
            if prev_ok && next_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn parse_option(options: &str, key: &str) -> Option<String> {
    for part in options.split(',') {
        let part = part.trim();
        if let Some(eq_pos) = part.find('=') {
            let k = &part[..eq_pos];
            if k == key {
                let v = &part[eq_pos + 1..];
                let v = v.trim_start_matches('[').trim_end_matches(']');
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Walk a session's subscription list on disconnect and remove each entry
/// from the registry + the owning topic / queue. Both transports call this
/// on connection close.
pub fn cleanup_session(session: &mut Session, ctx: &RouterContext) {
    for sub_id in session.subscriptions.drain(..) {
        if let Some((_, route)) = ctx.sessions.remove(&sub_id) {
            metrics::gauge!("cq_subscriptions_active", "topic" => route.topic.clone())
                .decrement(1.0);
            if let Some(topic) = ctx.topics.get(&route.topic) {
                topic.unsubscribe(&sub_id);
            }
            if let Some(queue) = ctx.queues.get(&route.topic) {
                queue.remove_consumer(&sub_id);
            }
        }
    }
}

/// Stream a SOW snapshot to one subscriber using chunked `SowBatch`
/// frames. Iterates the topic's column store and projects rows in
/// batches of `batch_size`, never materializing the full result. Each
/// `tx.send(...).await` is backpressure-aware, so a slow consumer
/// pauses iteration rather than dropping rows.
///
/// `ack_cmd_id == Some(cid)` triggers an ack frame (for
/// sow_and_subscribe / subscribe). For one-shot SOW it should be `None`.
#[allow(clippy::too_many_arguments)]
async fn deliver_streaming_snapshot(
    topic: SharedTopic,
    sql: String,
    ack_cmd_id: Option<String>,
    sub_id: String,
    topic_label: String,
    session_id: String,
    codec_slot: crate::session::SharedCodec,
    tx: crate::session::OutboundTx,
    batch_size: usize,
) {
    use crate::session::encode_frame;
    use std::time::Instant;

    let codec = *codec_slot.lock();
    let started = Instant::now();

    // Optional ack — guaranteed-delivery, awaited.
    if let Some(cid) = ack_cmd_id {
        let mut ack = CqMessage::ack_ok(Some(cid));
        ack.sub_id = Some(sub_id.clone());
        if let Some(f) = encode_frame(codec, &ack) {
            if tx.send(f).await.is_err() {
                return;
            }
        }
    }

    // Snapshot-encoder concurrency cap. Each in-flight snapshot holds
    // one permit for the duration of `query_streaming + WS drain`.
    // When the cap is reached, new SOW requests wait here instead of
    // piling onto the runtime in parallel. Permit is released on
    // function return (drop scope). Acquisition only fails if the
    // semaphore was closed — which we never do — so the unwrap is
    // safe.
    metrics::gauge!("cq_snapshot_queued").increment(1.0);
    let _permit = snapshot_encoder_semaphore()
        .acquire()
        .await
        .expect("snapshot semaphore closed");
    metrics::gauge!("cq_snapshot_queued").decrement(1.0);
    metrics::gauge!("cq_snapshot_active").increment(1.0);
    // Decrement on drop so the active count is always accurate even on
    // early returns.
    struct ActiveGuard;
    impl Drop for ActiveGuard {
        fn drop(&mut self) {
            metrics::gauge!("cq_snapshot_active").decrement(1.0);
        }
    }
    let _active_guard = ActiveGuard;

    // group_begin carries the row count when known. For the streaming
    // path we don't know it without scanning, so emit it as 0 — the
    // total is reported on group_end via the snapshot-completion log.
    if let Some(f) = encode_frame(codec, &CqMessage::group_begin(&sub_id, 0)) {
        if tx.send(f).await.is_err() {
            return;
        }
    }

    // Branch on codec: the JSON path uses `query_streaming_json` which
    // skips the `serde_json::Map<String, Value>` intermediate, writing
    // each row directly to a `Vec<u8>` in the columnar encoder. Saves
    // the per-cell `String` key alloc + HashMap bucket overhead that
    // dominates CPU on wide-row topics. Non-JSON codecs fall back to
    // the old `Map`-based path; they have different on-wire
    // serializations and don't share the win.
    let mut batches_sent: usize = 0;
    let mut rows_sent: usize = 0;
    use cq_protocol::serialization::Codec;
    let total: usize = if matches!(codec, Codec::Json) {
        // ── JSON fast path with encode-once-fanout cache ─────────
        //
        // The cache lets multiple subs that arrive within ~500 ms of
        // each other with the same (topic, sql) share one encoded
        // snapshot. On a hit we just iterate the cached `Arc<Vec<
        // Vec<u8>>>` and stream it to the wire — no encoder runs at
        // all, no semaphore wait. Misses behave like the pre-cache
        // path: spawn the encoder, drain batches, then publish the
        // result so the next subscriber wins the cache.
        let cached: Option<Arc<Vec<Vec<Vec<u8>>>>> =
            try_get_or_wait_snapshot(&topic_label, &sql).await;

        let batches: Arc<Vec<Vec<Vec<u8>>>> = if let Some(c) = cached {
            c
        } else {
            // Cache miss — we hold the Building slot. If anything
            // below fails we abandon it so waiters can retry.
            let topic_for_block = topic.clone();
            let sql_for_block = sql.clone();
            let topic_label_for_block = topic_label.clone();
            let collected: Result<Vec<Vec<Vec<u8>>>, String> =
                tokio::task::spawn_blocking(move || {
                    let mut all: Vec<Vec<Vec<u8>>> = Vec::new();
                    topic_for_block
                        .query_streaming_json(&sql_for_block, batch_size, |batch| {
                            all.push(batch);
                        })
                        .map_err(|e| {
                            format!("query error on {}: {}", topic_label_for_block, e)
                        })?;
                    Ok(all)
                })
                .await
                .unwrap_or_else(|_| Err("snapshot task join failed".into()));
            match collected {
                Ok(all) => {
                    let arc = Arc::new(all);
                    publish_snapshot_to_cache(&topic_label, &sql, arc.clone());
                    arc
                }
                Err(e) => {
                    abandon_snapshot_cache_slot(&topic_label, &sql);
                    if let Some(f) = encode_frame(codec, &CqMessage::error(None, &e)) {
                        let _ = tx.send(f).await;
                    }
                    return;
                }
            }
        };

        // Stream every cached batch as a sow_batch frame. Reading the
        // cache is read-only; encoder ran (or didn't) above.
        for batch in batches.iter() {
            let n = batch.len();
            if let Some(f) = crate::session::build_sow_batch_json_frame(&sub_id, batch) {
                if tx.send(f).await.is_err() {
                    tracing::warn!(
                        session = %session_id,
                        topic = %topic_label,
                        rows_sent,
                        "SOW snapshot aborted — session disconnected"
                    );
                    return;
                }
            }
            batches_sent += 1;
            rows_sent += n;
        }
        rows_sent
    } else {
        // ── Slow path for non-JSON codecs (bson, msgpack, fix) ─────
        let (batch_tx, mut batch_rx) =
            tokio::sync::mpsc::channel::<Vec<serde_json::Map<String, serde_json::Value>>>(4);
        let topic_for_block = topic.clone();
        let sql_for_block = sql.clone();
        let topic_label_for_block = topic_label.clone();
        let iter_handle: tokio::task::JoinHandle<Result<usize, String>> =
            tokio::task::spawn_blocking(move || {
                let mut err: Option<String> = None;
                let total = topic_for_block
                    .query_streaming(&sql_for_block, batch_size, |batch| {
                        if err.is_some() {
                            return;
                        }
                        if batch_tx.blocking_send(batch).is_err() {
                            err = Some("consumer dropped".into());
                        }
                    })
                    .map_err(|e| {
                        format!("query error on {}: {}", topic_label_for_block, e)
                    })?;
                if let Some(e) = err {
                    return Err(e);
                }
                Ok(total)
            });

        while let Some(batch) = batch_rx.recv().await {
            let n = batch.len();
            if let Some(f) = encode_frame(codec, &CqMessage::sow_batch(&sub_id, batch)) {
                if tx.send(f).await.is_err() {
                    tracing::warn!(
                        session = %session_id,
                        topic = %topic_label,
                        rows_sent,
                        "SOW snapshot aborted — session disconnected"
                    );
                    return;
                }
            }
            batches_sent += 1;
            rows_sent += n;
        }

        match iter_handle.await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                if let Some(f) = encode_frame(codec, &CqMessage::error(None, &e)) {
                    let _ = tx.send(f).await;
                }
                return;
            }
            Err(_) => return,
        }
    };

    if let Some(f) = encode_frame(codec, &CqMessage::group_end(&sub_id)) {
        let _ = tx.send(f).await;
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;
    metrics::counter!("cq_sow_rows_total", "topic" => topic_label.clone())
        .increment(total as u64);
    metrics::histogram!("cq_sow_query_latency_us", "topic" => topic_label.clone())
        .record(started.elapsed().as_micros() as f64);
    tracing::info!(
        session = %session_id,
        topic = %topic_label,
        rows = total,
        batches = batches_sent,
        elapsed_ms,
        "SOW snapshot delivered"
    );
}

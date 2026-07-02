//! Topic: a named, keyed collection of records with SOW storage + subscriptions.
//!
//! Concurrency model
//! -----------------
//! A `Topic` is internally synchronized. Multiple writers and subscribe
//! requests can run concurrently against a single `Arc<Topic>`:
//!
//! - `state` (store + key_to_row) is held under a `RwLock`. Writers take it
//!   briefly to mutate the column store; readers (queries, snapshot
//!   computation, evaluator row-reads) take a read guard.
//! - `sub_engine` is a separate `Mutex`. Subscribe / unsubscribe and the
//!   evaluator's `evaluate_row` are the only writers; they don't contend
//!   with publishers on the store path.
//!
//! Delta delivery is decoupled from publishing. `upsert` writes the store
//! and emits a `MutationEvent` on a `crossbeam_channel`. A long-lived
//! evaluator thread (owned by the server) reads events and calls
//! `evaluate_row` to produce deltas, which it then routes via the
//! transport's session registry. Publishers never wait on subscription
//! evaluation or delta serialization.

use crate::flatten::{flatten, FlattenConfig};
use crate::query::{execute_query, parse_query, ParsedQuery, QueryError, QueryResult};
use crate::schema::{ColumnType, Schema};
use crate::store::{ColumnStore, Value};
use crate::subscription::{Delta, Subscription, SubscriptionEngine};
use compact_str::CompactString;
use cq_txlog::writer::TxLogWriter;
use crossbeam_channel::{Receiver, Sender};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Returns true if `schema` is the default 1-column `_key: String`
/// placeholder used for topics whose columns will be discovered on first
/// publish.
fn is_placeholder_schema(schema: &Schema) -> bool {
    schema.column_count() == 1
        && schema.column_name(0) == "_key"
        && schema.column_type(0) == ColumnType::String
}

/// Configuration for a topic.
#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub name: String,
    pub key_fields: Vec<String>,
    pub persist: bool,
    pub conflation_ms: Option<u64>,
    /// Schema column names to maintain a secondary equality index for.
    /// The SOW query planner uses these to short-circuit a full scan
    /// when the WHERE clause contains an equality predicate on one of
    /// these columns. Empty (default) means no secondary indexes.
    pub index_columns: Vec<String>,
    /// Per-row TTL in seconds. When set, a background task scans the
    /// topic at most once per second and deletes rows whose `last
    /// touched` timestamp is older than this value. `None` (default)
    /// disables TTL — rows live until explicitly deleted.
    pub expire_seconds: Option<u64>,
}

/// Errors that can occur during a publish or delete that engages the
/// durability log.
#[derive(Debug, thiserror::Error)]
pub enum TopicError {
    #[error("txlog: {0}")]
    TxLog(#[from] cq_txlog::TxLogError),
    #[error("serialize: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("delta publish missing key field(s) for topic {topic}")]
    MissingKeyFields { topic: String },
}

/// What caused this mutation event. Lets the evaluator distinguish a
/// real delete (emit `Remove`) from a predicate-flip side effect of an
/// update (emit `Oof` — the row still exists, it just left the
/// subscriber's filtered view).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Upsert,
    Delete,
}

/// Event emitted to the evaluator after any mutation to the topic's store.
#[derive(Debug, Clone)]
pub struct MutationEvent {
    pub row: u32,
    /// Monotonic sequence assigned at the moment of the write. Forwarded
    /// to every emitted `Delta` so subscribers can bookmark on reconnect.
    pub sequence: u64,
    /// What kind of mutation triggered this event. See [`MutationKind`].
    pub kind: MutationKind,
    /// Columns the publisher actually wrote (S46 / review C3). `None`
    /// means "all columns" — used by full upserts and deletes, where
    /// every subscription needs to be evaluated. `Some(...)` is set
    /// by sparse-publish paths (`delta_upsert_map`): the dispatcher
    /// then routes through `PredicateIndex` to skip subscriptions
    /// whose predicate references no column in the changed set.
    pub changed_cols: Option<Vec<usize>>,
}

/// Internal: schema + store + key index. Schema is held here (not on `Topic`)
/// so it can be atomically replaced together with the store during schema
/// discovery on first publish.
struct StoreState {
    schema: Arc<Schema>,
    store: ColumnStore,
    key_to_row: HashMap<String, u32>,
    key_col_indices: Vec<usize>,
    /// Secondary equality indexes maintained alongside the store. Empty
    /// when `index_columns` is empty in `TopicConfig`. Maintained
    /// transactionally inside `write_store` / `delete` so a reader
    /// holding the state lock sees a consistent index.
    secondary_index: crate::sec_index::SecondaryIndex,
    /// Per-row "last touched" timestamps, indexed by row. Extended on
    /// `append_row`, refreshed on `update_row`. Used by the TTL
    /// sweeper to decide which rows have expired.
    last_touched: Vec<std::time::Instant>,
    /// SOW slot reclamation: store row indices freed by `delete` (the slot
    /// was `null_out_row`-ed and dropped from `key_to_row`). A later insert
    /// of a new key reuses one of these before growing the store, bounding
    /// store memory to the live-set high-water instead of leaking a slot per
    /// delete (AMPS-style slot reuse).
    free_slots: Vec<u32>,
}

impl StoreState {
    fn build(
        schema: Arc<Schema>,
        key_fields: &[String],
        index_columns: &[String],
        capacity: usize,
    ) -> Self {
        let key_col_indices: Vec<usize> = key_fields
            .iter()
            .filter_map(|k| schema.index_of(k))
            .collect();
        let indexed_cols: Vec<usize> = index_columns
            .iter()
            .filter_map(|name| schema.index_of(name))
            .collect();
        StoreState {
            store: ColumnStore::new(schema.clone(), capacity),
            key_to_row: HashMap::with_capacity(capacity),
            key_col_indices,
            secondary_index: crate::sec_index::SecondaryIndex::new(indexed_cols),
            last_touched: Vec::with_capacity(capacity),
            free_slots: Vec::new(),
            schema,
        }
    }
}

/// Default soft cap on un-drained mutation events (see
/// [`Topic::mutation_cap`]). Each event is tiny (a row id, sequence,
/// kind, and optional changed-column list), so 64Ki of backlog is a few
/// MB at most — enough to absorb bursts while still bounding memory if a
/// publisher durably outruns the single evaluator thread. Overridable
/// per topic via [`Topic::with_mutation_cap`].
const DEFAULT_MUTATION_BACKLOG_CAP: usize = 65_536;

/// A topic manages a SOW store, key index, and subscription engine.
pub struct Topic {
    config: TopicConfig,
    state: RwLock<StoreState>,
    /// Subscription engines, partitioned into `eval_lanes` lanes. A
    /// subscription is assigned to exactly one lane by a stable hash of
    /// its id (see `engine_for`), so its conflation state (`active_set`,
    /// `last_snapshot`, `topn`) is only ever touched by that lane's
    /// evaluator thread. This lets the per-topic evaluator fan out across
    /// cores while preserving per-subscription delta ordering: events
    /// flow to every lane in topic-sequence order, and each lane owns a
    /// disjoint subset of subscriptions. `len() == eval_lanes >= 1`; a
    /// single-lane topic behaves exactly as the historical single
    /// `sub_engine` Mutex.
    sub_engines: Vec<Mutex<SubscriptionEngine>>,
    /// Number of evaluator lanes (== `sub_engines.len()`). 1 means the
    /// historical single-threaded-per-topic evaluator.
    eval_lanes: usize,
    /// Live subscription count mirrored alongside the engine's
    /// `subscriptions` map. Updated on every add / remove /
    /// remove_by_prefix / reap_closed so `subscription_count()` can
    /// answer in O(1) without taking `sub_engine.lock()`. The admin
    /// `/stats` handler hammers this on every poll — taking the
    /// mutex was the dominant source of admin unresponsiveness under
    /// heavy WS load.
    active_subscriptions: std::sync::atomic::AtomicUsize,
    /// Mirror of `state.read().store.row_count()` updated lock-free
    /// after every successful upsert. The store's own row_count is
    /// already AtomicU32, but it lives behind the state RwLock —
    /// admin readers shouldn't have to take that lock just to count
    /// rows. We update this mirror after the write lock is released,
    /// so the worst-case staleness is one publish; that's fine for a
    /// stats endpoint.
    row_count_mirror: std::sync::atomic::AtomicU32,
    mutation_tx: Sender<MutationEvent>,
    mutation_rx_holder: Mutex<Option<Receiver<MutationEvent>>>,
    /// Soft cap on the number of un-drained `MutationEvent`s allowed to
    /// sit in `mutation_tx` before producers are throttled. The channel
    /// itself stays unbounded (so the send under `state.write()` can
    /// never block and deadlock the evaluator), but producers call
    /// `throttle_mutation_backlog` *before* taking the write lock and
    /// park there until the evaluator drains the backlog below this cap.
    /// Bounds steady-state memory without the lock-ordering hazard a
    /// bounded blocking send would introduce.
    mutation_cap: usize,
    /// Set true the first time `take_mutation_rx` hands the receiver to
    /// an evaluator. Until a consumer exists, throttling is a no-op:
    /// otherwise tests and bulk paths that publish without an evaluator
    /// would block forever waiting for a drain that never comes.
    mutation_consumer_active: AtomicBool,
    /// Secondary fan-out for derived consumers (S20 views). Every
    /// `MutationEvent` that the topic emits on `mutation_tx` is also
    /// `try_send`-fanned to every registered tap. Senders that fail
    /// to deliver (closed receiver, full bounded queue) are silently
    /// pruned on the next write; the regular subscription path is
    /// unaffected. Bounded senders are used so a slow view runner
    /// can't unbounded-grow memory on a hot publisher.
    view_taps: Mutex<Vec<(u64, Sender<MutationEvent>)>>,
    /// Monotonic id allocator for view taps; lets a specific tap be
    /// dropped via `unregister_view_tap(id)`.
    next_view_tap_id: std::sync::atomic::AtomicU64,
    /// S20-incremental — count of view-tap events dropped because a
    /// tap's bounded queue was full. A view runner snapshots this
    /// before/after absorbing a burst: any increase means it may have
    /// missed an event, so it must reconcile via a full refresh +
    /// membership reseed rather than trusting its incremental state.
    view_tap_drops: AtomicU64,
    txlog: Option<Arc<Mutex<TxLogWriter>>>,
    /// When `true`, `delta_upsert_map` journals the sparse `{key + changed
    /// fields}` payload as published (`JsonDelta`, merge-on-replay) instead
    /// of the fully-merged row. Kills the write amplification on wide-row
    /// topics with hot delta publishers (a 16-field tick on a 200-column
    /// row journals ~16 fields, not ~200). Trade-off: journal entries are
    /// no longer self-contained — replay/bookmark consumers merge them
    /// onto prior state — so it is opt-in per topic. Full publishes and
    /// deletes journal exactly as before regardless of this flag.
    sparse_delta_txlog: bool,
    /// Monotonic sequence counter. Persistent topics seed this from the
    /// txlog's `max_sequence` on attach so that post-recovery numbering
    /// continues uninterrupted.
    next_sequence: AtomicU64,
    /// Highest sequence ACTUALLY APPLIED to the store (live publish OR
    /// replay). Drives multi-path dedup independently of
    /// `next_sequence` — the latter gets seeded from the txlog's
    /// `max_sequence` at `attach_txlog` time, so it can't double as
    /// the dedup watermark (it would silently suppress every
    /// recovery replay because `entry.sequence <= seeded next_sequence`
    /// is true for every entry). Initialized to 0; bumped by replay
    /// paths and by live `write_store`.
    last_applied_sequence: AtomicU64,
    /// S11 — highest sequence the configured replication destination
    /// has confirmed it applied. Bumped by the shipper's Ack reader;
    /// awaited by the publish path in sync-replication mode so the
    /// publisher's ack only returns after the standby has durably
    /// observed the entry. `0` if no replication is configured or
    /// no Ack has arrived yet.
    last_replicated_sequence: AtomicU64,
    /// S11 — wakeup channel for the await side of the replication
    /// barrier. The shipper's Ack reader calls `notify_waiters`
    /// every time it bumps `last_replicated_sequence`; the router's
    /// `finish_publish_ack` waiter loops on the notify until the
    /// observed value reaches the publish's target sequence.
    replication_notify: Arc<tokio::sync::Notify>,
    /// S20 — AMPS-style instance identity stamped on every local write so
    /// downstream peers can attribute and dedup it. Empty means "unnamed"
    /// (single-node / legacy); replication loop-avoidance and convergent
    /// dedup are only meaningful once a non-empty id is configured.
    origin_id: String,
    /// S20 — per-origin high-water mark of sequences this topic has applied
    /// from *foreign* origins (origin != `origin_id`), driving idempotent,
    /// convergent replication dedup. First writer per (origin, sequence)
    /// wins; a re-delivered entry with `sequence <= highwater[origin]` is a
    /// no-op. Local writes are tracked by `last_applied_sequence`, not here.
    replicated_highwater: Mutex<HashMap<String, u64>>,
}

/// P14 — canonical topic-name form. AMPS_PARITY §4 bug 5 documented
/// the slash-prefix asymmetry: topics register as `/positions` but
/// SQL clauses (`FROM positions JOIN ...`) often write bare names.
/// Centralising the form here lets the registry insert/lookup paths
/// agree without per-call-site dual-lookup helpers.
///
/// Rules:
/// - Empty input stays empty (caller is expected to error before this).
/// - A leading `/` is preserved.
/// - Any other input gets `/` prepended.
///
/// `canonicalize_topic("trades") == "/trades"`
/// `canonicalize_topic("/trades") == "/trades"`
pub fn canonicalize_topic(name: &str) -> String {
    if name.is_empty() {
        return String::new();
    }
    if name.starts_with('/') {
        name.to_string()
    } else {
        format!("/{name}")
    }
}

impl Topic {
    pub fn new(config: TopicConfig, schema: Arc<Schema>, capacity: usize) -> Self {
        let state = StoreState::build(
            schema,
            &config.key_fields,
            &config.index_columns,
            capacity,
        );
        let (mutation_tx, mutation_rx) = crossbeam_channel::unbounded();
        Topic {
            config,
            state: RwLock::new(state),
            sub_engines: vec![Mutex::new(SubscriptionEngine::new())],
            eval_lanes: 1,
            active_subscriptions: std::sync::atomic::AtomicUsize::new(0),
            row_count_mirror: std::sync::atomic::AtomicU32::new(0),
            mutation_tx,
            mutation_rx_holder: Mutex::new(Some(mutation_rx)),
            mutation_cap: DEFAULT_MUTATION_BACKLOG_CAP,
            mutation_consumer_active: AtomicBool::new(false),
            view_taps: Mutex::new(Vec::new()),
            next_view_tap_id: std::sync::atomic::AtomicU64::new(0),
            view_tap_drops: AtomicU64::new(0),
            txlog: None,
            sparse_delta_txlog: false,
            next_sequence: AtomicU64::new(0),
            last_applied_sequence: AtomicU64::new(0),
            last_replicated_sequence: AtomicU64::new(0),
            replication_notify: Arc::new(tokio::sync::Notify::new()),
            origin_id: String::new(),
            replicated_highwater: Mutex::new(HashMap::new()),
        }
    }

    /// Set this topic's originating instance id (AMPS-style publisher id).
    /// Stamped onto every local txlog write so downstream peers can attribute
    /// and dedup it, and used by the shipper to avoid reflecting an entry back
    /// to the peer it came from. Empty (the default) disables origin tagging.
    /// Returns `self` for builder-style use.
    pub fn with_origin_id(mut self, origin_id: impl Into<String>) -> Self {
        self.origin_id = origin_id.into();
        self
    }

    /// This topic's originating instance id. Empty if unnamed.
    pub fn origin_id(&self) -> &str {
        &self.origin_id
    }

    /// Journal `delta_upsert_map` payloads sparsely (`JsonDelta`,
    /// merge-on-replay) instead of as the fully-merged row. Opt-in per
    /// topic; see the `sparse_delta_txlog` field docs for the trade-off.
    pub fn with_sparse_delta_txlog(mut self, on: bool) -> Self {
        self.sparse_delta_txlog = on;
        self
    }

    /// Set the number of evaluator lanes for this topic. Partitions the
    /// subscription engine into `lanes` independent shards (each with its
    /// own `Mutex<SubscriptionEngine>`) so the server can run one
    /// evaluator thread per lane. Must be called before any subscription
    /// is registered (i.e. during bootstrap, before the evaluator(s) are
    /// spawned) — re-sharding a populated engine is not supported.
    /// `lanes` is clamped to `>= 1`. Returns `self` for builder-style use.
    pub fn with_eval_lanes(mut self, lanes: usize) -> Self {
        let lanes = lanes.max(1);
        self.sub_engines = (0..lanes)
            .map(|_| Mutex::new(SubscriptionEngine::new()))
            .collect();
        self.eval_lanes = lanes;
        self
    }

    /// Number of evaluator lanes (== number of subscription-engine
    /// shards). 1 means the historical single-threaded-per-topic
    /// evaluator.
    pub fn eval_lanes(&self) -> usize {
        self.eval_lanes
    }

    /// Route a subscription id to its owning lane's engine via a stable
    /// hash. The same id always maps to the same lane, so a
    /// subscription's conflation state lives on exactly one evaluator
    /// thread. With `eval_lanes == 1` this is always `sub_engines[0]`.
    fn engine_for(&self, sub_id: &str) -> &Mutex<SubscriptionEngine> {
        if self.sub_engines.len() == 1 {
            return &self.sub_engines[0];
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        sub_id.hash(&mut h);
        let lane = (h.finish() as usize) % self.sub_engines.len();
        &self.sub_engines[lane]
    }

    /// Attach a persistence log to this topic. Should be called before the
    /// topic is published to (typically during server bootstrap). The
    /// topic's sequence counter is seeded from the log's `max_sequence`.
    pub fn attach_txlog(&mut self, writer: Arc<Mutex<TxLogWriter>>) {
        let max_seq = writer.lock().max_sequence();
        self.next_sequence.store(max_seq, Ordering::Relaxed);
        self.txlog = Some(writer);
    }

    pub fn has_txlog(&self) -> bool {
        self.txlog.is_some()
    }

    /// Force the attached txlog writer to roll to a new segment now.
    /// Used by the admin endpoint so operators can seal the current
    /// segment on demand. No-op when no txlog is attached.
    pub fn force_rotate_txlog(&self) -> Result<(), TopicError> {
        if let Some(w) = &self.txlog {
            w.lock().force_rotate()?;
        }
        Ok(())
    }

    /// Flush and fsync the attached txlog writer (if any). Called on
    /// graceful shutdown to guarantee that every entry the topic
    /// reported "persisted" is durable on disk before the process
    /// exits — even when the configured fsync policy is `none` (which
    /// trades durability for throughput on the hot write path).
    /// No-op for non-persistent topics.
    pub fn flush_txlog(&self) -> Result<(), TopicError> {
        if let Some(w) = &self.txlog {
            w.lock().sync()?;
        }
        Ok(())
    }

    /// Release any over-provisioned tail slack in the column store.
    /// Takes the topic write lock so it cannot race with publishers;
    /// reads are unaffected (they see the same `row_count`).
    ///
    /// Returns `(old_capacity, new_capacity)`.
    pub fn shrink_store(&self) -> (usize, usize) {
        let mut state = self.state.write();
        let old = state.store.capacity();
        let new = state.store.shrink_to_fit();
        (old, new)
    }

    /// Highest sequence ever assigned by this topic. Bookmarks reference
    /// this value.
    pub fn current_sequence(&self) -> u64 {
        self.next_sequence.load(Ordering::Relaxed)
    }

    /// Derive the key string for a JSON record using this topic's
    /// configured key fields. Returns `None` for keyless topics OR when
    /// any key field is missing/null in the input. Mirrors the
    /// all-or-nothing semantics used internally by `upsert` so the txlog
    /// and the in-memory index always agree on a row's key.
    pub fn compute_key_from_map(
        &self,
        map: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<String> {
        let key_fields = &self.config.key_fields;
        if key_fields.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(key_fields.len());
        for field in key_fields {
            let part = map.get(field).and_then(|v| match v {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })?;
            parts.push(part);
        }
        Some(if parts.len() == 1 {
            parts.remove(0)
        } else {
            parts.join("|")
        })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Returns the topic's current schema. Cheap clone of an `Arc`.
    pub fn schema(&self) -> Arc<Schema> {
        self.state.read().schema.clone()
    }

    pub fn row_count(&self) -> u32 {
        // Lock-free read via the mirror. Worst-case staleness: one
        // publish lag, which the admin endpoint can tolerate.
        self.row_count_mirror
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Live subscription count. Reads an atomic — does not lock the
    /// subscription engine. Safe to call from the admin endpoint
    /// while a data-path thread holds `sub_engine.lock()`.
    pub fn subscription_count(&self) -> usize {
        self.active_subscriptions
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn conflation_ms(&self) -> Option<u64> {
        self.config.conflation_ms
    }

    /// Run one pass of TTL expiration. Any row whose `last_touched`
    /// is older than `config.expire_seconds` is eligible for delete;
    /// the actual delete re-checks `last_touched` **under the same
    /// write lock** that mutates it, so a publish racing the sweeper
    /// can't lose data:
    ///
    /// - Sweep observes K1 expired in its read-lock pass.
    /// - Sweep releases the read lock.
    /// - Publisher refreshes K1 under the write lock (bumps
    ///   `last_touched` to "now").
    /// - Sweep re-takes the write lock, re-checks K1's
    ///   `last_touched`: now ≥ ttl is false → SKIP. K1 survives.
    ///
    /// Without this re-check (the old code path), the sweep would
    /// proceed to delete K1 after the publish, silently dropping a
    /// row the publisher just wrote. Worklog S40 / review C9.
    pub fn sweep_expired(&self) -> Result<Vec<String>, TopicError> {
        let Some(ttl_s) = self.config.expire_seconds else {
            return Ok(Vec::new());
        };
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(ttl_s);
        let candidate_keys: Vec<String> = {
            let state = self.state.read();
            state
                .key_to_row
                .iter()
                .filter_map(|(k, &row)| {
                    state
                        .last_touched
                        .get(row as usize)
                        .filter(|ts| now.duration_since(**ts) >= ttl)
                        .map(|_| k.clone())
                })
                .collect()
        };
        let mut actually_expired = Vec::with_capacity(candidate_keys.len());
        for k in &candidate_keys {
            if self.delete_if_still_expired(k, ttl, now)? {
                actually_expired.push(k.clone());
            }
        }
        if !actually_expired.is_empty() {
            metrics::counter!(
                "cq_topic_ttl_expired_total",
                "topic" => self.config.name.clone()
            )
            .increment(actually_expired.len() as u64);
        }
        Ok(actually_expired)
    }

    /// Delete `key` ONLY if its `last_touched` (read under the same
    /// write lock that mutates it) still satisfies `now - last_touched
    /// >= ttl` relative to the sweeper-observed `sweep_observed_at`.
    /// If the row was republished between the sweep's read-lock scan
    /// and this call, the re-check fails and the delete is suppressed.
    /// Returns `true` iff a delete actually happened.
    fn delete_if_still_expired(
        &self,
        key: &str,
        ttl: std::time::Duration,
        sweep_observed_at: std::time::Instant,
    ) -> Result<bool, TopicError> {
        // Ingress backpressure (see `throttle_mutation_backlog`): the TTL
        // sweeper emits a Delete event on success, so gate it too.
        self.throttle_mutation_backlog();
        let mut state = self.state.write();
        let Some(row) = state.key_to_row.get(key).copied() else {
            // Row already gone (e.g., concurrent delete won). Not an
            // error — just not our responsibility to expire.
            return Ok(false);
        };
        let last_touched = match state.last_touched.get(row as usize) {
            Some(ts) => *ts,
            None => return Ok(false),
        };
        // Re-check: is the row STILL expired? `last_touched > sweep_observed_at`
        // means a publish refreshed it after the sweep saw it; bail.
        // Otherwise use the original sweep timestamp to decide
        // expiry, not "now" — that way two sweepers that fire close
        // together don't double-evaluate against a sliding clock.
        if last_touched > sweep_observed_at {
            return Ok(false);
        }
        if sweep_observed_at.duration_since(last_touched) < ttl {
            return Ok(false);
        }
        // Still expired. Remove key, null row, allocate seq, log,
        // emit event — all under the write lock we're already
        // holding. Inlined from `delete()` because we already own
        // the lock and don't want to drop + re-acquire it.
        state.key_to_row.remove(key);
        if !state.secondary_index.is_empty() {
            let indexed: Vec<usize> = state.secondary_index.indexed_columns().to_vec();
            for col in indexed {
                let v = state.store.get(col, row);
                state.secondary_index.remove(col, &v, row);
            }
        }
        state.store.null_out_row(row);
        state.free_slots.push(row);

        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(log) = &self.txlog {
            let mut log = log.lock();
            log.append_with_origin(seq, self.name(), key, &self.origin_id, &[])?;
        }
        let event = MutationEvent {
            row,
            sequence: seq,
            kind: MutationKind::Delete,
            // Deletes null every column — and the evaluator's
            // Delete branch forces `matches = false` regardless of
            // predicate, so any sub that had this row in its
            // active set fires. None = "all subs need to be
            // checked" — index pruning doesn't apply here.
            changed_cols: None,
        };
        self.dispatch_mutation_event(event.clone());
        self.fanout_view_tap(&event);
        drop(state);
        Ok(true)
    }

    /// Topic's configured TTL, if any. Exposed so the server can
    /// decide whether to spawn a per-topic sweeper at startup.
    pub fn expire_seconds(&self) -> Option<u64> {
        self.config.expire_seconds
    }

    /// Names of the topic's key columns, in declaration order.
    /// Useful for callers building keys-only projections.
    pub fn key_column_names(&self) -> Vec<String> {
        let state = self.state.read();
        state
            .key_col_indices
            .iter()
            .map(|&i| state.schema.column_name(i).to_string())
            .collect()
    }

    pub fn config(&self) -> &TopicConfig {
        &self.config
    }

    /// Take ownership of the mutation receiver. Returns `None` if already
    /// taken. Called exactly once at server startup per topic; the receiver
    /// is then passed to the evaluator thread.
    pub fn take_mutation_rx(&self) -> Option<Receiver<MutationEvent>> {
        let rx = self.mutation_rx_holder.lock().take();
        if rx.is_some() {
            // A consumer now exists, so backlog will actually drain —
            // enable producer throttling. Until this point throttling
            // is a no-op (see `throttle_mutation_backlog`).
            self.mutation_consumer_active.store(true, Ordering::Release);
        }
        rx
    }

    /// Override the per-topic mutation backlog cap. Builder-style;
    /// intended for callers (e.g. server bootstrap) that want a tighter
    /// or looser bound than [`DEFAULT_MUTATION_BACKLOG_CAP`].
    pub fn with_mutation_cap(mut self, cap: usize) -> Self {
        self.mutation_cap = cap.max(1);
        self
    }

    /// Block the calling producer *before* it takes `state.write()` until
    /// the evaluator has drained the mutation backlog below `mutation_cap`.
    ///
    /// This is the ingress side of the backpressure scheme. The mutation
    /// channel stays unbounded so the actual `send` (which happens under
    /// `state.write()` to keep sequence allocation and enqueue atomic)
    /// can never block — blocking there would deadlock the evaluator,
    /// which needs `state.read()` to drain. Instead we gate here, holding
    /// no locks, so the evaluator (a dedicated OS thread, not a tokio
    /// worker) is always free to make progress and unblock us.
    ///
    /// No-op until a consumer has taken the receiver: otherwise tests and
    /// bulk paths that publish without an evaluator would park forever.
    fn throttle_mutation_backlog(&self) {
        if !self.mutation_consumer_active.load(Ordering::Acquire) {
            return;
        }
        if self.mutation_tx.len() < self.mutation_cap {
            return;
        }
        metrics::counter!(
            "cq_mutation_backlog_throttle_total",
            "topic" => self.config.name.clone()
        )
        .increment(1);
        let mut spins = 0u32;
        while self.mutation_tx.len() >= self.mutation_cap {
            if spins < 128 {
                std::hint::spin_loop();
                spins += 1;
            } else {
                std::thread::sleep(std::time::Duration::from_micros(50));
            }
        }
    }

    /// Attach a secondary tap on this topic's mutation stream. Returns
    /// a `Receiver` that gets a copy of every subsequent `MutationEvent`
    /// the topic emits. Used by S20 view runners to wake up and
    /// re-aggregate; multiple taps are supported (one per view).
    ///
    /// `cap` is the bounded queue depth — a slow view runner that
    /// can't keep up will drop events when its queue fills (the
    /// metric `cq_topic_view_tap_drops_total` ticks). View runners
    /// must be coalescing — re-aggregation reads current store state
    /// every tick, so a dropped tap event just delays the next
    /// refresh by one tick of the next event the runner does
    /// observe.
    pub fn register_view_tap(&self, cap: usize) -> (u64, Receiver<MutationEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        let id = self
            .next_view_tap_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.view_taps.lock().push((id, tx));
        (id, rx)
    }

    /// Drop the view-tap `Sender` registered under `id`; the matching
    /// runner's `Receiver` then disconnects and its thread exits. No-op if
    /// the id isn't present.
    pub fn unregister_view_tap(&self, id: u64) {
        self.view_taps.lock().retain(|(tid, _)| *tid != id);
    }

    /// Fan out one mutation event to every registered view tap.
    /// Best-effort: a tap whose receiver is closed is pruned; a tap
    /// whose bounded queue is full drops the event and increments the
    /// drop counter. The hot write path stays non-blocking.
    fn fanout_view_tap(&self, event: &MutationEvent) {
        let mut taps = self.view_taps.lock();
        if taps.is_empty() {
            return;
        }
        let topic_name = self.config.name.clone();
        taps.retain(|(_, tx)| match tx.try_send(event.clone()) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                metrics::counter!(
                    "cq_topic_view_tap_drops_total",
                    "topic" => topic_name.clone()
                )
                .increment(1);
                self.view_tap_drops.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
        });
    }

    /// S20-incremental — total view-tap events dropped (queue-full) since
    /// construction. A view runner watches this to decide whether its
    /// incremental membership is still trustworthy after a burst.
    pub fn view_tap_drops(&self) -> u64 {
        self.view_tap_drops.load(Ordering::Relaxed)
    }

    /// S20-incremental — run `f` against the live store under the read
    /// lock, passing the set of live (non-tombstoned) row indices and the
    /// highest sequence already applied to this snapshot. Used by the
    /// materialized-view runner's incremental path to seed group
    /// membership and recompute individual groups without re-querying
    /// the whole SOW, and to skip tap events whose effect the snapshot
    /// already reflects (`ev.sequence <= applied_seq`). Read locks
    /// compose, so concurrent view refreshes and SOW queries don't block
    /// each other.
    pub(crate) fn with_live_store<R>(
        &self,
        f: impl FnOnce(&ColumnStore, &roaring::RoaringBitmap, u64) -> R,
    ) -> R {
        let state = self.state.read();
        let live: roaring::RoaringBitmap = state.key_to_row.values().copied().collect();
        // Read the applied watermark while holding the read lock so it's
        // consistent with the snapshot `f` observes.
        let applied_seq = self.last_applied_sequence.load(Ordering::Acquire);
        f(&state.store, &live, applied_seq)
    }

    /// Compute a key from a `values` vector using the current key column
    /// indices. Caller must hold the appropriate state guard.
    fn compute_key_with(values: &[Value], key_col_indices: &[usize]) -> Option<String> {
        if key_col_indices.is_empty() {
            return None;
        }
        let mut parts = Vec::with_capacity(key_col_indices.len());
        for &idx in key_col_indices {
            let part = values.get(idx).and_then(|v| match v {
                Value::String(Some(s)) => Some(s.to_string()),
                Value::Long(n) => Some(n.to_string()),
                Value::Int(n) => Some(n.to_string()),
                Value::Double(n) => Some(n.to_string()),
                _ => None,
            })?;
            parts.push(part);
        }
        Some(if parts.len() == 1 {
            parts.remove(0)
        } else {
            parts.join("|")
        })
    }

    // ==================== Schema discovery ====================

    /// If the topic is still on its placeholder schema and has no data /
    /// subscribers, derive a real schema from the top-level keys of `map`
    /// and atomically replace the underlying store. Idempotent: a no-op
    /// once a real schema is installed or once data exists.
    /// Q11 — online schema evolution: append a new column to this
    /// topic. Every existing row gets the new column set to its
    /// type's null sentinel; new publishes can populate it
    /// immediately. Existing subscribers / parsed queries keep
    /// working because column appending preserves every existing
    /// column index — only the column count grows.
    ///
    /// Errors if the column name already exists. Other in-flight
    /// publishes block on the state.write() lock for the duration
    /// of the swap (typically O(rows) to copy column data into the
    /// new store).
    pub fn add_column(
        &self,
        name: &str,
        col_type: ColumnType,
    ) -> Result<(), TopicError> {
        let mut state = self.state.write();
        if state.schema.index_of(name).is_some() {
            return Err(TopicError::Serialize(serde_json::Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("column `{name}` already exists"),
                ),
            )));
        }
        // Build new schema = old cols ++ new col.
        let mut new_names: Vec<CompactString> = state
            .schema
            .columns()
            .iter()
            .map(|c| CompactString::new(c.name()))
            .collect();
        let mut new_types: Vec<ColumnType> =
            state.schema.columns().iter().map(|c| c.col_type()).collect();
        new_names.push(CompactString::new(name));
        new_types.push(col_type);
        let new_schema = Arc::new(Schema::new(new_names, new_types));

        let capacity = state.store.capacity();
        let row_count = state.store.row_count();
        let mut new_state = StoreState::build(
            new_schema.clone(),
            &self.config.key_fields,
            &self.config.index_columns,
            capacity,
        );

        // Copy every existing row into the new store. The new column
        // gets the type's null sentinel automatically (ColumnStore::new
        // initialises with nulls).
        let old_col_count = state.schema.column_count();
        for row in 0..row_count {
            let mut values: Vec<Value> = Vec::with_capacity(old_col_count + 1);
            for c in 0..old_col_count {
                values.push(state.store.get(c, row));
            }
            // The new column starts NULL.
            values.push(Value::Null);
            Self::commit_values_locked(&mut new_state, values);
        }
        // Preserve last_touched timestamps (best effort: copy what we
        // have, default the rest to now).
        let now = std::time::Instant::now();
        for slot in &mut new_state.last_touched {
            *slot = now;
        }
        *state = new_state;
        tracing::info!(
            topic = %self.config.name,
            new_col = %name,
            new_col_type = ?col_type,
            "Schema evolved: column added"
        );
        Ok(())
    }

    pub fn maybe_discover_schema(&self, map: &serde_json::Map<String, serde_json::Value>) {
        // Cheap pre-check without taking the write lock.
        {
            let state = self.state.read();
            if !is_placeholder_schema(&state.schema) || state.store.row_count() > 0 {
                return;
            }
        }
        // Subscriptions hold ParsedQuery with pre-resolved column indices
        // against the placeholder schema — installing a new schema would
        // invalidate them. Refuse and keep the placeholder.
        if self.subscription_count() > 0 {
            return;
        }

        let mut state = self.state.write();
        // Re-check under the write lock.
        if !is_placeholder_schema(&state.schema) || state.store.row_count() > 0 {
            return;
        }

        let mut names: Vec<CompactString> = Vec::with_capacity(map.len());
        let mut types: Vec<ColumnType> = Vec::with_capacity(map.len());
        for (name, value) in map {
            names.push(CompactString::new(name));
            types.push(ColumnType::from_json(value));
        }
        if names.is_empty() {
            return; // nothing to infer
        }

        let new_schema = Arc::new(Schema::new(names, types));
        let capacity = state.store.capacity();
        *state = StoreState::build(
            new_schema,
            &self.config.key_fields,
            &self.config.index_columns,
            capacity,
        );

        tracing::info!(
            topic = %self.config.name,
            cols = state.schema.column_count(),
            "Schema discovered from first publish"
        );
    }

    // ==================== Write API ====================

    /// Mutation logic shared by the live publish path and the recovery
    /// replay path. Holds `state.write()` for its entire duration, so the
    /// secondary index update is transactional w.r.t. concurrent readers.
    /// Returns the row index that was written or updated.
    fn commit_values_locked(state: &mut StoreState, values: Vec<Value>) -> u32 {
        let key = Self::compute_key_with(&values, &state.key_col_indices);
        let now = std::time::Instant::now();
        if let Some(key) = &key {
            if let Some(&existing_row) = state.key_to_row.get(key) {
                Self::reindex_row(state, existing_row, &values);
                state.store.update_row(existing_row, &values);
                if let Some(slot) = state.last_touched.get_mut(existing_row as usize) {
                    *slot = now;
                }
                existing_row
            } else if let Some(slot) = state.free_slots.pop() {
                // SOW slot reclamation: refill a slot freed by a prior
                // delete instead of growing the store. `write_row_at` does a
                // full overwrite (the slot was null_out_row-ed when freed),
                // and the slot's secondary-index entries were already removed
                // by the delete, so indexing the new occupant is clean.
                state.store.write_row_at(slot, values);
                state.key_to_row.insert(key.clone(), slot);
                Self::index_new_row_from_store(state, slot);
                if let Some(t) = state.last_touched.get_mut(slot as usize) {
                    *t = now;
                } else {
                    Self::push_last_touched(state, slot, now);
                }
                slot
            } else {
                let row = state.store.append_row_owned(values);
                state.key_to_row.insert(key.clone(), row);
                Self::index_new_row_from_store(state, row);
                Self::push_last_touched(state, row, now);
                row
            }
        } else {
            // P5 — keyless topics behave as a single row. Without a
            // key, every upsert collapses onto row 0 instead of
            // appending. This is what AMPS does for a degenerate-
            // aggregate view (`SELECT SUM(x) FROM t` with no GROUP
            // BY) and matches the AMPS_PARITY §4 bug 3 fix: the
            // view's SOW stays at exactly one row across refreshes.
            // Topics that genuinely want append-only semantics must
            // declare a key field.
            if state.store.row_count() > 0 {
                let row = 0u32;
                Self::reindex_row(state, row, &values);
                state.store.update_row(row, &values);
                if let Some(slot) = state.last_touched.get_mut(row as usize) {
                    *slot = now;
                }
                row
            } else {
                let row = state.store.append_row_owned(values);
                Self::index_new_row_from_store(state, row);
                Self::push_last_touched(state, row, now);
                row
            }
        }
    }

    /// Live publish path. Allocates the sequence and optionally appends
    /// to the txlog **inside the same `state.write()` lock** that commits
    /// the row, then emits the `MutationEvent` before releasing.
    ///
    /// This is the atomicity boundary the `sow_and_subscribe` contract
    /// (C1 / S32) relies on: a subscriber holding `state.read()` cannot
    /// observe `next_sequence = N` without also seeing every mutation
    /// whose sequence is ≤ N in its snapshot, because all of those
    /// mutations have already committed under `state.write()`. Setting
    /// `Subscription::live_start_sequence = captured + 1` then makes the
    /// evaluator suppress redelivery of any event already covered by the
    /// snapshot — eliminating both the missed-update and duplicate-Add
    /// races the contract guards against.
    ///
    /// `log_args = Some((key, payload))` — append `(seq, key, payload)`
    /// to the topic's txlog if one is attached; `None` skips the log
    /// (test-only typed upsert path).
    fn write_store(
        &self,
        values: Vec<Value>,
        log_args: Option<(&str, &[u8])>,
        emit_event: bool,
    ) -> Result<(u32, u64), TopicError> {
        let log_args = log_args.map(|(k, p)| (k, p, cq_txlog::PayloadFormat::Json));
        self.write_store_with_changed(values, log_args, emit_event, None)
    }

    fn write_store_with_changed(
        &self,
        values: Vec<Value>,
        log_args: Option<(&str, &[u8], cq_txlog::PayloadFormat)>,
        emit_event: bool,
        changed_cols: Option<Vec<usize>>,
    ) -> Result<(u32, u64), TopicError> {
        // Ingress backpressure: park here (holding no locks) if the
        // evaluator is behind, so the unbounded mutation channel can't
        // grow without bound. Must precede `state.write()`.
        if emit_event {
            self.throttle_mutation_backlog();
        }
        let mut state = self.state.write();
        // Sequence is allocated under state.write() so subscribers cannot
        // observe `next_sequence` advance past N without also seeing the
        // mutation for N in their snapshot. fetch_add returns the prior
        // value; we use `+ 1` so sequences are 1-based and monotonic.
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((key, payload, format)) = log_args {
            if let Some(log) = &self.txlog {
                let mut log = log.lock();
                log.append_with_origin_format(
                    seq,
                    self.name(),
                    key,
                    &self.origin_id,
                    payload,
                    format,
                )?;
            }
        }
        let row = Self::commit_values_locked(&mut state, values);
        // Refresh the lock-free row-count mirror right after the
        // store has been mutated, while we still hold the write
        // lock. Readers see the new count as soon as the lock drops.
        let new_count = state.store.row_count();
        if emit_event {
            let event = MutationEvent {
                row,
                sequence: seq,
                kind: MutationKind::Upsert,
                changed_cols: changed_cols.clone(),
            };
            self.dispatch_mutation_event(event.clone());
            self.fanout_view_tap(&event);
        }
        drop(state);
        self.row_count_mirror
            .store(new_count, std::sync::atomic::Ordering::Release);
        Ok((row, seq))
    }

    /// Recovery replay path. Uses the caller-provided `sequence` (from
    /// the txlog entry) instead of allocating a new one; never writes to
    /// the txlog (the entry is already there); never emits a mutation
    /// event (evaluators aren't running yet during recovery).
    fn write_store_replay(&self, values: Vec<Value>, _sequence: u64) -> u32 {
        let mut state = self.state.write();
        let row = Self::commit_values_locked(&mut state, values);
        let new_count = state.store.row_count();
        drop(state);
        self.row_count_mirror
            .store(new_count, std::sync::atomic::Ordering::Release);
        row
    }

    /// Extend the `last_touched` Vec to cover `row`, setting the
    /// entry to `now`. New rows are always at the tail of the store
    /// so we just `push` — defending against any unexpected ordering
    /// with a `resize_with` first.
    fn push_last_touched(state: &mut StoreState, row: u32, now: std::time::Instant) {
        let needed = row as usize + 1;
        if state.last_touched.len() < needed {
            state
                .last_touched
                .resize_with(needed, std::time::Instant::now);
        }
        if let Some(slot) = state.last_touched.get_mut(row as usize) {
            *slot = now;
        }
    }

    /// Add a freshly-appended row to every secondary index that
    /// covers one of its columns. Cheap when no indexes are
    /// configured (the index's covered set is empty).
    fn index_new_row_from_store(state: &mut StoreState, row: u32) {
        if state.secondary_index.is_empty() {
            return;
        }
        let indexed: Vec<usize> = state.secondary_index.indexed_columns().to_vec();
        for col in indexed {
            let v = state.store.get(col, row);
            state.secondary_index.add(col, &v, row);
        }
    }

    /// Re-index an existing row whose column values are about to change.
    /// Reads the *old* values from the store and removes them from the
    /// index, then adds the *new* values from `new_values`. Values that
    /// are unchanged still pass through both calls but they're
    /// idempotent (remove-then-add returns to the same membership).
    ///
    /// Note: relies on `Value::Null` being the "skip" sentinel for
    /// `update_row` — when `new_values[col]` is Null, the store keeps
    /// the old value, so the index also stays put. We model that by
    /// re-using the old value as the "new" value for the index in
    /// that case.
    fn reindex_row(state: &mut StoreState, row: u32, new_values: &[Value]) {
        if state.secondary_index.is_empty() {
            return;
        }
        let indexed: Vec<usize> = state.secondary_index.indexed_columns().to_vec();
        for col in indexed {
            let old_val = state.store.get(col, row);
            state.secondary_index.remove(col, &old_val, row);
            let new_val = match new_values.get(col) {
                // `Null` is the "no change" sentinel for update_row
                // (see store.rs:343). Re-index the old value back in.
                Some(Value::Null) | None => old_val,
                Some(v) => v.clone(),
            };
            state.secondary_index.add(col, &new_val, row);
        }
    }


    fn bump_sequence_to(&self, seq: u64) {
        // Bump both watermarks: `next_sequence` so subsequent live
        // publishes don't reuse a sequence the replay already
        // consumed, and `last_applied_sequence` so the multi-path
        // dedup gate (`replay_upsert_map`, `replay_delete`)
        // correctly suppresses re-applications of already-applied
        // sequences.
        for atom in [&self.next_sequence, &self.last_applied_sequence] {
            let mut current = atom.load(Ordering::Relaxed);
            while current < seq {
                match atom.compare_exchange(
                    current,
                    seq,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }
    }

    /// Test-friendly upsert from typed values. Generates a sequence and
    /// emits a mutation event but does **not** write to the txlog —
    /// typed values can't be losslessly serialized to JSON without going
    /// through the schema. Production publishes should use `upsert_map`.
    pub fn upsert(&self, values: Vec<Value>) -> u32 {
        let (row, _seq) = self
            .write_store(values, None, true)
            .expect("typed upsert path does not write to txlog and cannot error");
        row
    }

    /// Publish from a JSON map. Triggers schema discovery on the first
    /// publish to a placeholder-schema topic, writes to the durability
    /// log (if attached), updates the store, and emits a mutation event
    /// — all carrying the same monotonic sequence. Returns the assigned
    /// sequence.
    ///
    /// Nested input objects are flattened to dotted-path keys before
    /// processing — e.g. `{"trade": {"price": 100}}` becomes
    /// `{"trade.price": 100}` internally. The column store stays flat;
    /// nesting on the wire is purely an ergonomic convenience for
    /// publishers.
    pub fn upsert_map(
        &self,
        map: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<u64, TopicError> {
        let flat_owned;
        let flat: &serde_json::Map<String, serde_json::Value> = if map_has_nesting(map) {
            flat_owned = flatten_publish_map(map);
            &flat_owned
        } else {
            map
        };

        self.maybe_discover_schema(flat);

        let key = self.compute_key_from_map(flat).unwrap_or_default();
        // Only materialize the txlog payload when a log is actually
        // attached. Without persistence this per-row clone + JSON
        // re-serialize is pure waste — the dominant cost when seeding a
        // wide-row universe. Serialize the map directly: `to_vec(flat)`
        // yields the same bytes as `to_vec(&Value::Object(flat.clone()))`
        // without copying the map.
        let payload = if self.has_txlog() {
            Some(serde_json::to_vec(flat)?)
        } else {
            None
        };

        let values: Vec<Value> = {
            let state = self.state.read();
            state
                .schema
                .columns()
                .iter()
                .map(|col| {
                    flat.get(col.name())
                        .map(|v| Value::from_json(v, col.col_type()))
                        .unwrap_or(Value::Null)
                })
                .collect()
        };
        // Sequence allocation + txlog append + store mutation + event
        // emission all happen inside write_store under state.write(),
        // so a concurrent subscriber observing next_sequence will also
        // see this mutation in its snapshot (S32 atomicity contract).
        let log_args = payload.as_deref().map(|p| (key.as_str(), p));
        let (_row, seq) = self.write_store(values, log_args, true)?;
        Ok(seq)
    }

    /// Batch full-publish: commit every row in `maps` under a **single**
    /// `state.write()` lock instead of re-acquiring it per row. This is the
    /// seed-path optimization — for a wide-row universe the per-row lock,
    /// schema lookup, and row-count-mirror update dominate; amortizing them
    /// across the whole frame is the difference between a multi-minute and a
    /// sub-second seed.
    ///
    /// Semantics match calling [`upsert_map`] once per row, with one
    /// deliberate strengthening: the whole frame commits atomically w.r.t.
    /// `state.read()`, so a subscriber's snapshot never observes a partial
    /// batch. Per-row sequences are still allocated monotonically and one
    /// `MutationEvent` per row is emitted in order (S32 contract).
    ///
    /// Returns the assigned sequence for each row, in input order.
    pub fn upsert_map_batch(
        &self,
        maps: &[&serde_json::Map<String, serde_json::Value>],
    ) -> Result<Vec<u64>, TopicError> {
        if maps.is_empty() {
            return Ok(Vec::new());
        }

        // Flatten nested rows up front (owned); flat rows borrow as-is, so
        // the common all-flat case allocates nothing.
        let flattened: Vec<Option<serde_json::Map<String, serde_json::Value>>> = maps
            .iter()
            .map(|m| {
                if map_has_nesting(m) {
                    Some(flatten_publish_map(m))
                } else {
                    None
                }
            })
            .collect();
        let rows: Vec<&serde_json::Map<String, serde_json::Value>> = maps
            .iter()
            .zip(flattened.iter())
            .map(|(orig, flat)| flat.as_ref().unwrap_or(orig))
            .collect();

        // Schema discovery is first-publish-only and self-locks; mirror the
        // per-row path by letting the FIRST row define the schema (later
        // rows are pulled against it).
        self.maybe_discover_schema(rows[0]);

        // Phase 1: convert every row to its per-column `Value` vec under a
        // read lock, so concurrent publishers can convert in parallel — only
        // the commit needs exclusivity.
        let value_rows: Vec<Vec<Value>> = {
            let state = self.state.read();
            rows.iter()
                .map(|flat| {
                    state
                        .schema
                        .columns()
                        .iter()
                        .map(|col| {
                            flat.get(col.name())
                                .map(|v| Value::from_json(v, col.col_type()))
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect()
        };

        // Throttle once (not per row) before taking the lock. The unbounded
        // mutation channel's sends below never block under the lock; this
        // keeps the evaluator backlog bounded the same way the single-publish
        // path does.
        self.throttle_mutation_backlog();

        let has_txlog = self.has_txlog();

        // Pre-serialize each row's txlog key + payload OUTSIDE the write
        // lock. Serialization is the costly part and depends on neither the
        // sequence nor the store, so doing it here keeps it off the critical
        // section that serializes every writer. The ordered append itself
        // still happens under `state.write()` below. On a serialization
        // failure we commit only the already-serialized prefix, matching the
        // per-row path's "surviving prefix stays durable" contract.
        let mut log_entries: Vec<(String, Vec<u8>)> = Vec::new();
        let mut commit_len = rows.len();
        let mut serialize_err: Option<TopicError> = None;
        if has_txlog {
            log_entries.reserve(rows.len());
            for (i, flat) in rows.iter().enumerate() {
                let key = self.compute_key_from_map(flat).unwrap_or_default();
                match serde_json::to_vec(flat) {
                    Ok(payload) => log_entries.push((key, payload)),
                    Err(e) => {
                        serialize_err = Some(e.into());
                        commit_len = i;
                        break;
                    }
                }
            }
        }

        let mut seqs = Vec::with_capacity(rows.len());
        let mut result: Result<(), TopicError> = Ok(());

        // Phase 2: commit the whole frame under one write lock, holding the
        // txlog lock once for the batch (not re-acquiring it per row).
        let mut state = self.state.write();
        let mut log_guard = if has_txlog {
            self.txlog.as_ref().map(|l| l.lock())
        } else {
            None
        };
        for (idx, values) in value_rows.into_iter().enumerate() {
            if idx >= commit_len {
                break;
            }
            let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some(log) = log_guard.as_mut() {
                let (key, payload) = &log_entries[idx];
                if let Err(e) =
                    log.append_with_origin(seq, self.name(), key, &self.origin_id, payload)
                {
                    result = Err(e.into());
                    break;
                }
            }
            let row = Self::commit_values_locked(&mut state, values);
            let event = MutationEvent {
                row,
                sequence: seq,
                kind: MutationKind::Upsert,
                changed_cols: None,
            };
            self.dispatch_mutation_event(event.clone());
            self.fanout_view_tap(&event);
            seqs.push(seq);
        }
        drop(log_guard);
        // Refresh the lock-free row-count mirror for the rows that committed,
        // even on a mid-batch error, so the count never under-reports.
        let new_count = state.store.row_count();
        drop(state);
        self.row_count_mirror
            .store(new_count, std::sync::atomic::Ordering::Release);

        // First error wins: an append error already short-circuited above;
        // otherwise surface any serialization failure now that the durable
        // prefix is committed.
        match result {
            Err(e) => Err(e),
            Ok(()) => match serialize_err {
                Some(e) => Err(e),
                None => Ok(seqs),
            },
        }
    }

    /// Delta-publish: merge a sparse `{key + changed fields}` payload
    /// into the existing row for that key, or create a new row when
    /// the key isn't yet present. Absent fields keep their current
    /// stored value (full publishes treat absent fields as null and
    /// overwrite — the key behavioural difference).
    ///
    /// By default the txlog records the *fully-merged* row so a fresh
    /// replay produces the same SOW state regardless of how the rows got
    /// there (full publishes, deltas, or a mix) and every entry is
    /// self-contained. With `sparse_delta_txlog` enabled the journal
    /// instead records the sparse payload as published (`JsonDelta`) and
    /// recovery merges it — same final state, a fraction of the journal
    /// bytes on wide-row topics.
    pub fn delta_upsert_map(
        &self,
        map: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<u64, TopicError> {
        let flat_owned;
        let flat: &serde_json::Map<String, serde_json::Value> = if map_has_nesting(map) {
            flat_owned = flatten_publish_map(map);
            &flat_owned
        } else {
            map
        };

        self.maybe_discover_schema(flat);

        // `compute_key_from_map` returns `None` both for genuinely keyless
        // topics (no `key_fields` configured) and for keyed topics whose
        // configured key field(s) are absent from this payload. Keyless
        // topics must keep working (fall back to key `""`, a single shared
        // row is the intended semantics there); keyed topics must reject
        // the publish instead of silently merging every keyless delta into
        // one phantom `""`-keyed row — AMPS rejects these outright.
        let key = match self.compute_key_from_map(flat) {
            Some(k) => k,
            None if self.config.key_fields.is_empty() => String::new(),
            None => {
                return Err(TopicError::MissingKeyFields {
                    topic: self.config.name.clone(),
                });
            }
        };

        // Build the fully-merged row + the per-column value vec under
        // one read pass. For columns the publisher supplied: use the
        // new value AND record the column index in `changed_cols` so
        // the evaluator's `PredicateIndex` (S46) can skip
        // subscriptions whose predicates reference no changed
        // column.
        let (values, merged_payload_map, changed_cols) = {
            let state = self.state.read();
            let existing_row = state.key_to_row.get(&key).copied();
            let mut payload = serde_json::Map::with_capacity(state.schema.column_count());
            let mut changed_cols: Vec<usize> = Vec::new();
            let values: Vec<Value> = state
                .schema
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let name = col.name();
                    if let Some(v) = flat.get(name) {
                        let typed = Value::from_json(v, col.col_type());
                        payload.insert(name.to_string(), typed.to_json());
                        changed_cols.push(i);
                        typed
                    } else if let Some(row) = existing_row {
                        let typed = state.store.get(i, row);
                        payload.insert(name.to_string(), typed.to_json());
                        typed
                    } else {
                        // Brand-new row, no existing value.
                        payload.insert(name.to_string(), serde_json::Value::Null);
                        Value::Null
                    }
                })
                .collect();
            (values, payload, changed_cols)
        };

        let (payload_bytes, payload_format) = if self.sparse_delta_txlog {
            // Journal the delta AS PUBLISHED (key + changed fields). ~12×
            // smaller than the merged row on wide topics with narrow ticks;
            // recovery merges it via `replay_delta_map_origin`.
            (
                serde_json::to_vec(&serde_json::Value::Object(flat.clone()))?,
                cq_txlog::PayloadFormat::JsonDelta,
            )
        } else {
            (
                serde_json::to_vec(&serde_json::Value::Object(merged_payload_map))?,
                cq_txlog::PayloadFormat::Json,
            )
        };
        let (_row, seq) = self.write_store_with_changed(
            values,
            Some((&key, &payload_bytes, payload_format)),
            true,
            Some(changed_cols),
        )?;
        Ok(seq)
    }

    /// Delete a record by key. Writes a tombstone to the txlog (if
    /// attached) and nulls the corresponding store row.
    ///
    /// The store slot is *not* reused for subsequent inserts. Reusing it
    /// would race with the async evaluator: a writer can reach the same
    /// slot before the evaluator dispatches the original mutation's
    /// Remove delta, and the evaluator would then see the new row's data
    /// when it processes the stale event — silently dropping the Remove.
    /// Safe slot reuse requires either generation-tagged events with
    /// synchronous catch-up, or carrying snapshots inside events; both
    /// are deferred to a future compaction pass.
    pub fn delete(&self, key: &str) -> Result<Option<u64>, TopicError> {
        // Ingress backpressure (see `throttle_mutation_backlog`): block
        // before the write lock if the evaluator is behind.
        self.throttle_mutation_backlog();
        // Sequence allocation, txlog append, store mutation, and event
        // emission all happen under a single state.write() lock so a
        // concurrent subscriber holding state.read() cannot observe
        // next_sequence advance past the delete's sequence without
        // also seeing the row already removed from its snapshot
        // (S32 atomicity contract).
        let mut state = self.state.write();
        let Some(row) = state.key_to_row.remove(key) else {
            return Ok(None);
        };
        if !state.secondary_index.is_empty() {
            let indexed: Vec<usize> = state.secondary_index.indexed_columns().to_vec();
            for col in indexed {
                let v = state.store.get(col, row);
                state.secondary_index.remove(col, &v, row);
            }
        }
        state.store.null_out_row(row);
        state.free_slots.push(row);

        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(log) = &self.txlog {
            let mut log = log.lock();
            log.append_with_origin(seq, self.name(), key, &self.origin_id, &[])?;
        }

        let event = MutationEvent {
            row,
            sequence: seq,
            kind: MutationKind::Delete,
            changed_cols: None,
        };
        self.dispatch_mutation_event(event.clone());
        self.fanout_view_tap(&event);
        drop(state);
        Ok(Some(seq))
    }

    /// Recovery-only: apply a replayed JSON entry directly to the store
    /// using the sequence carried by the log entry. Does **not** write to
    /// the txlog (the entry is already there) and does **not** emit a
    /// mutation event (evaluators aren't running yet during recovery).
    pub fn replay_upsert_map(
        &self,
        sequence: u64,
        map: &serde_json::Map<String, serde_json::Value>,
    ) {
        self.replay_upsert_map_origin("", sequence, map);
    }

    /// Origin-aware recovery/replication upsert. `origin` is the AMPS-style
    /// publisher id that produced the entry. Own-origin / legacy ("") entries
    /// dedup on the single-source `last_applied_sequence` watermark; entries
    /// from a foreign origin dedup on the per-origin `replicated_highwater`
    /// map (first-writer-wins, convergent — re-delivery over an alternate
    /// path is a no-op).
    pub fn replay_upsert_map_origin(
        &self,
        origin: &str,
        sequence: u64,
        map: &serde_json::Map<String, serde_json::Value>,
    ) {
        if self.replay_is_duplicate(origin, sequence) {
            metrics::counter!(
                "cq_topic_replay_dedup_total",
                "topic" => self.config.name.clone()
            )
            .increment(1);
            return;
        }
        let flat_owned;
        let map: &serde_json::Map<String, serde_json::Value> = if map_has_nesting(map) {
            flat_owned = flatten_publish_map(map);
            &flat_owned
        } else {
            map
        };
        self.maybe_discover_schema(map);
        let values: Vec<Value> = {
            let state = self.state.read();
            state
                .schema
                .columns()
                .iter()
                .map(|col| {
                    map.get(col.name())
                        .map(|v| Value::from_json(v, col.col_type()))
                        .unwrap_or(Value::Null)
                })
                .collect()
        };
        self.write_store_replay(values, sequence);
        self.replay_note_applied(origin, sequence);
    }

    /// Recovery/replication path for a **sparse JSON delta** payload
    /// (`0x03` / `JsonDelta` entries): merge the `{key + changed fields}`
    /// map into the existing row for that key — absent columns keep their
    /// stored value (or null when the key is new). Mirrors the live
    /// `delta_upsert_map` merge; same dedup + watermark semantics as
    /// [`Topic::replay_upsert_map_origin`].
    pub fn replay_delta_map_origin(
        &self,
        origin: &str,
        sequence: u64,
        map: &serde_json::Map<String, serde_json::Value>,
    ) {
        if self.replay_is_duplicate(origin, sequence) {
            metrics::counter!(
                "cq_topic_replay_dedup_total",
                "topic" => self.config.name.clone()
            )
            .increment(1);
            return;
        }
        let flat_owned;
        let map: &serde_json::Map<String, serde_json::Value> = if map_has_nesting(map) {
            flat_owned = flatten_publish_map(map);
            &flat_owned
        } else {
            map
        };
        self.maybe_discover_schema(map);
        // Same all-or-nothing semantics as the live `delta_upsert_map`
        // path: a keyed topic whose configured key field(s) are absent
        // from this entry must not be merged into a phantom `""`-keyed
        // row. Recovery must stay lenient (never abort a replay over one
        // bad entry) — skip it with a warning instead of panicking or
        // erroring out.
        let key = match self.compute_key_from_map(map) {
            Some(k) => k,
            None if self.config.key_fields.is_empty() => String::new(),
            None => {
                tracing::warn!(
                    topic = %self.config.name,
                    origin = %origin,
                    sequence,
                    "skipping delta replay entry missing configured key field(s)"
                );
                return;
            }
        };
        let values: Vec<Value> = {
            let state = self.state.read();
            let existing_row = state.key_to_row.get(&key).copied();
            state
                .schema
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    if let Some(v) = map.get(col.name()) {
                        Value::from_json(v, col.col_type())
                    } else if let Some(row) = existing_row {
                        state.store.get(i, row)
                    } else {
                        Value::Null
                    }
                })
                .collect()
        };
        self.write_store_replay(values, sequence);
        self.replay_note_applied(origin, sequence);
    }

    /// Recovery-only delete. Nulls the row if present; updates the
    /// sequence high-water mark.
    pub fn replay_delete(&self, sequence: u64, key: &str) {
        self.replay_delete_origin("", sequence, key);
    }

    /// Origin-aware recovery/replication delete. See
    /// [`Topic::replay_upsert_map_origin`] for the dedup semantics.
    pub fn replay_delete_origin(&self, origin: &str, sequence: u64, key: &str) {
        if self.replay_is_duplicate(origin, sequence) {
            metrics::counter!(
                "cq_topic_replay_dedup_total",
                "topic" => self.config.name.clone()
            )
            .increment(1);
            return;
        }
        {
            let mut state = self.state.write();
            if let Some(row) = state.key_to_row.remove(key) {
                if !state.secondary_index.is_empty() {
                    let indexed: Vec<usize> =
                        state.secondary_index.indexed_columns().to_vec();
                    for col in indexed {
                        let v = state.store.get(col, row);
                        state.secondary_index.remove(col, &v, row);
                    }
                }
                state.store.null_out_row(row);
                state.free_slots.push(row);
            }
        }
        self.replay_note_applied(origin, sequence);
    }

    /// Idempotent-convergence dedup gate. Own-origin / legacy ("") entries
    /// use the single-source `last_applied_sequence` watermark; foreign
    /// origins use the per-origin `replicated_highwater` map.
    fn replay_is_duplicate(&self, origin: &str, sequence: u64) -> bool {
        if origin.is_empty() || origin == self.origin_id {
            let hw = self.last_applied_sequence.load(Ordering::Acquire);
            sequence <= hw && hw > 0
        } else {
            self.replicated_highwater
                .lock()
                .get(origin)
                .is_some_and(|&hw| sequence <= hw)
        }
    }

    /// Hand a mutation event to the evaluator channel. The send can only
    /// fail if the receiver was never taken or the evaluator thread died —
    /// a catastrophic condition (no further deltas will ever be delivered)
    /// that was previously discarded silently via `let _ =`. Log it once
    /// so it's diagnosable instead of presenting as a mysteriously frozen
    /// subscription. Throttled to a single process-wide warning to avoid
    /// flooding the hot path if it does happen.
    fn dispatch_mutation_event(&self, event: MutationEvent) {
        if self.mutation_tx.send(event).is_err() {
            use std::sync::atomic::{AtomicBool, Ordering};
            static WARNED: AtomicBool = AtomicBool::new(false);
            if !WARNED.swap(true, Ordering::Relaxed) {
                tracing::error!(
                    topic = %self.name(),
                    "Mutation event could not be delivered to the evaluator \
                     (channel disconnected); subscribers will stop receiving deltas"
                );
            }
        }
    }

    /// Advance the watermark for an applied entry. Own-origin / legacy
    /// entries bump `next_sequence` + `last_applied_sequence`; foreign
    /// origins advance only their `replicated_highwater` slot (their
    /// sequence space is independent of ours).
    fn replay_note_applied(&self, origin: &str, sequence: u64) {
        if origin.is_empty() || origin == self.origin_id {
            self.bump_sequence_to(sequence);
        } else {
            let mut map = self.replicated_highwater.lock();
            let hw = map.entry(origin.to_string()).or_insert(0);
            if sequence > *hw {
                *hw = sequence;
            }
        }
    }

    /// Snapshot of per-origin high-water marks for foreign origins this
    /// topic has applied. Used by the replication receiver to advertise
    /// per-origin resume points in its `Hello` handshake.
    pub fn replicated_highwater_snapshot(&self) -> HashMap<String, u64> {
        self.replicated_highwater.lock().clone()
    }

    // ==================== SOW snapshot / fast restart ====================

    /// Build a point-in-time SOW snapshot of this topic: every live
    /// (non-tombstoned) row serialized to its JSON-object payload, the
    /// sequence watermark and active segment id those rows reflect, and the
    /// per-origin replication high-water marks. Returns `None` for a
    /// non-persistent topic (no txlog → nothing to fast-restart from).
    ///
    /// Consistency: the store is read under `state.read()`, and the
    /// sequence + segment id are sampled while that read lock is held. A
    /// publish takes `state.write()` to allocate its sequence and append to
    /// the txlog (which is where a rotation happens), so it cannot run
    /// concurrently with this method — the captured `(rows, sequence,
    /// segment_id)` triple is therefore internally consistent. The lock
    /// acquisition order (state → txlog) matches the publish path, so there
    /// is no deadlock.
    pub fn build_snapshot(&self) -> Option<cq_txlog::snapshot::Snapshot> {
        let txlog = self.txlog.as_ref()?;
        let state = self.state.read();
        let sequence = self.next_sequence.load(Ordering::Acquire);
        let origin_highwater = self.replicated_highwater.lock().clone();
        let segment_id = txlog.lock().current_segment();
        let mut rows = Vec::with_capacity(state.key_to_row.len());
        for &row in state.key_to_row.values() {
            let map = state.store.get_row_map(row);
            if let Ok(bytes) = serde_json::to_vec(&map) {
                rows.push(bytes);
            }
        }
        drop(state);
        Some(cq_txlog::snapshot::Snapshot {
            sequence,
            segment_id,
            origin_highwater,
            rows,
        })
    }

    /// Build and atomically write a SOW snapshot into the topic's txlog
    /// directory. Returns `Ok(true)` if a snapshot was written, `Ok(false)`
    /// for a non-persistent topic. Called on graceful shutdown (and by the
    /// periodic checkpointer) so the next start loads the snapshot and
    /// replays only the txlog tail instead of the whole log.
    pub fn write_snapshot(&self) -> Result<bool, TopicError> {
        let Some(snapshot) = self.build_snapshot() else {
            return Ok(false);
        };
        let dir = self
            .txlog
            .as_ref()
            .expect("build_snapshot returned Some => txlog present")
            .lock()
            .dir()
            .to_path_buf();
        snapshot
            .write_atomic(&dir)
            .map_err(|e| TopicError::TxLog(cq_txlog::TxLogError::Io(e)))?;
        Ok(true)
    }

    /// Reclaim disk by deleting every sealed segment older than the active
    /// one. Those segments are fully covered by a current snapshot, so
    /// replay never needs them again. Returns the number of segments
    /// removed (and any per-file errors, so the caller can log them); a
    /// failure to delete one segment does not abort the rest.
    ///
    /// **Irreversible.** Only call this *after* a durable snapshot has been
    /// written, and only when the segments are not still required by a
    /// replication standby (a far-behind standby would need a base SOW
    /// resync instead of log shipping). No-op for a non-persistent topic.
    pub fn reclaim_sealed_segments(&self) -> Result<usize, TopicError> {
        let Some(txlog) = self.txlog.as_ref() else {
            return Ok(0);
        };
        // `u64::MAX` clamps to the active segment inside the writer, so
        // this deletes exactly the sealed segments (best-effort per file).
        let removed = txlog.lock().prune_segments_below(u64::MAX)?;
        Ok(removed as usize)
    }

    /// Rebuild this topic's store from a loaded snapshot, then set the
    /// recovery baseline so subsequent txlog tail-replay applies only
    /// entries newer than the snapshot.
    ///
    /// Must be called during recovery, before tail-replay and before the
    /// evaluator(s) are spawned — it writes rows via the replay path
    /// (no txlog append, no mutation events) and bypasses the sequence
    /// dedup gate (the snapshot rows have no individual sequence; the whole
    /// snapshot is the authoritative state up to `snapshot.sequence`).
    pub fn apply_snapshot(&self, snapshot: &cq_txlog::snapshot::Snapshot) {
        for payload in &snapshot.rows {
            if let Ok(serde_json::Value::Object(map)) =
                serde_json::from_slice::<serde_json::Value>(payload)
            {
                self.apply_snapshot_row(&map);
            }
        }
        // Restore foreign-origin replication dedup watermarks.
        {
            let mut hw = self.replicated_highwater.lock();
            for (origin, &mark) in &snapshot.origin_highwater {
                let slot = hw.entry(origin.clone()).or_insert(0);
                if mark > *slot {
                    *slot = mark;
                }
            }
        }
        // Baseline the own-origin watermarks. `bump_sequence_to` only ever
        // raises, so this composes safely with the `next_sequence` seeded
        // from the txlog's `max_sequence` at `attach_txlog` time.
        self.bump_sequence_to(snapshot.sequence);
    }

    /// Apply a single snapshot row directly to the store: flatten + schema
    /// discovery + value coercion, then a dedup-free, txlog-free,
    /// event-free store write (the same primitive recovery replay uses).
    fn apply_snapshot_row(&self, map: &serde_json::Map<String, serde_json::Value>) {
        let flat_owned;
        let map: &serde_json::Map<String, serde_json::Value> = if map_has_nesting(map) {
            flat_owned = flatten_publish_map(map);
            &flat_owned
        } else {
            map
        };
        self.maybe_discover_schema(map);
        let values: Vec<Value> = {
            let state = self.state.read();
            state
                .schema
                .columns()
                .iter()
                .map(|col| {
                    map.get(col.name())
                        .map(|v| Value::from_json(v, col.col_type()))
                        .unwrap_or(Value::Null)
                })
                .collect()
        };
        self.write_store_replay(values, 0);
    }

    // ==================== Evaluator API ====================

    /// Re-evaluate every registered subscription against `row` and return
    /// the resulting deltas. `sequence` is forwarded onto every emitted
    /// delta. Defaults `kind` to `Upsert` — preserves the old behavior
    /// for callers that haven't switched to the kind-aware path.
    pub fn evaluate_row(&self, row: u32, sequence: u64) -> Vec<Delta> {
        self.evaluate_row_kind(row, sequence, MutationKind::Upsert)
    }

    /// Variant that takes the originating mutation kind, so the engine
    /// can emit `Oof` for predicate-flip exits vs `Remove` for real
    /// deletes (see [`MutationKind`]). Iterates every registered
    /// subscription — used by full-row publishes, deletes, recovery
    /// replay, and tests that don't know the changed-column set.
    pub fn evaluate_row_kind(
        &self,
        row: u32,
        sequence: u64,
        kind: MutationKind,
    ) -> Vec<Delta> {
        let state = self.state.read();
        // All-subscriptions path: fan across every lane. With
        // `eval_lanes == 1` this is a single engine. The sharded
        // evaluator does NOT use this — it calls
        // `evaluate_row_kind_indexed_lane` so each thread touches only
        // its own engine.
        let mut deltas = Vec::new();
        for engine in &self.sub_engines {
            let mut engine = engine.lock();
            deltas.extend(engine.evaluate_row_kind(row, sequence, &state.store, kind));
        }
        deltas
    }

    /// Index-routed variant (S46 / review C3): only evaluates
    /// subscriptions whose predicate references at least one of the
    /// columns in `changed_cols`. Used by the sparse-publish path
    /// (`delta_upsert_map`) to skip work for subscriptions that
    /// can't possibly be affected by the mutation. `None` for
    /// `changed_cols` falls through to the all-subs path, so this
    /// is a strict superset of `evaluate_row_kind`'s semantics.
    ///
    /// Fans across every lane (all subscriptions). The per-lane
    /// evaluator threads use [`evaluate_row_kind_indexed_lane`] instead
    /// so each thread only locks its own engine.
    pub fn evaluate_row_kind_indexed(
        &self,
        row: u32,
        sequence: u64,
        kind: MutationKind,
        changed_cols: Option<&[usize]>,
    ) -> Vec<Delta> {
        let state = self.state.read();
        let mut deltas = Vec::new();
        for engine in &self.sub_engines {
            let mut engine = engine.lock();
            deltas.extend(engine.evaluate_row_kind_indexed(
                row,
                sequence,
                &state.store,
                kind,
                changed_cols,
            ));
        }
        deltas
    }

    /// Evaluate a mutation against only the subscriptions owned by a
    /// single lane. The sharded evaluator spawns one thread per lane;
    /// each thread calls this with its own `lane` index so it only locks
    /// `sub_engines[lane]`. Because a subscription is pinned to one lane
    /// (see `engine_for`) and events reach every lane in topic-sequence
    /// order, this preserves per-subscription delta ordering while
    /// spreading the matching/delta-build work across cores.
    pub fn evaluate_row_kind_indexed_lane(
        &self,
        lane: usize,
        row: u32,
        sequence: u64,
        kind: MutationKind,
        changed_cols: Option<&[usize]>,
    ) -> Vec<Delta> {
        let state = self.state.read();
        let mut engine = self.sub_engines[lane].lock();
        engine.evaluate_row_kind_indexed(row, sequence, &state.store, kind, changed_cols)
    }

    // ==================== Query API ====================

    /// Execute a pre-parsed query against this topic's store, with
    /// the tombstone filter applied at the aggregate level. View
    /// runners (S20) use this on every source mutation to recompute
    /// their aggregate output; threading the topic's live-rows
    /// bitmap into the aggregate executor ensures tombstoned source
    /// rows don't bucket into a phantom null-key group.
    ///
    /// For non-aggregate queries, behaves the same as
    /// `execute_query_with_index` — the row-oriented path's tombstone
    /// filter is applied separately by `Topic::query` so an opt-out
    /// caller (e.g. internal cross-checks) can still read raw rows.
    pub fn execute_parsed_query(&self, parsed: &ParsedQuery) -> QueryResult {
        let state = self.state.read();
        let live_rows: roaring::RoaringBitmap =
            state.key_to_row.values().copied().collect();
        crate::query::execute_query_with_index_filtered(
            parsed,
            &state.store,
            Some(&state.secondary_index),
            Some(&live_rows),
        )
    }

    /// S20 — execute a JOIN query that uses this topic as the LEFT
    /// side and `right` as the RIGHT side. Both stores are read
    /// under their topics' read locks for the duration of the join
    /// (read locks compose: writers on either side will queue, but
    /// concurrent readers are fine). The aggregate / projection
    /// stages run over the materialized combined row set.
    pub fn execute_join_query(
        &self,
        parsed: &ParsedQuery,
        right: &Topic,
    ) -> Result<QueryResult, QueryError> {
        let left_state = self.state.read();
        let right_state = right.state.read();
        crate::query::execute_join_query(parsed, &left_state.store, &right_state.store)
    }

    /// Query Guardrails G2: parse `sql` against this topic's schema
    /// and return a structural cost estimate (row count, byte count,
    /// confidence). Does NOT execute the query — purely consults the
    /// secondary index for distinct-value counts and group cardinality.
    /// Used by `POST /admin/explain` and (in G3) by the subscribe-time
    /// enforcement gate.
    pub fn estimate_cost(
        &self,
        sql: &str,
    ) -> Result<crate::cost_estimator::QueryCostEstimate, QueryError> {
        let state = self.state.read();
        let parsed = parse_query(sql, &state.schema)?;
        // JOIN cost is left for a follow-up — the right-side topic
        // lookup needs the topic registry, which the Topic alone
        // doesn't carry. /admin/explain accepts non-join SQL today.
        Ok(crate::cost_estimator::estimate_cost(
            &parsed,
            &state.store,
            &state.schema,
            Some(&state.secondary_index),
            None,
        ))
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult, QueryError> {
        let state = self.state.read();
        let parsed = parse_query(sql, &state.schema)?;
        let mut result = crate::query::execute_query_with_index(
            &parsed,
            &state.store,
            Some(&state.secondary_index),
        );
        // Tombstone filter: rows removed via `delete()` or TTL sweep
        // are nulled in place (the row index isn't reused) and so
        // still surface in the raw store scan. Drop them by row-
        // index lookup against `state.key_to_row`'s value set —
        // robust against projections that omit the key column
        // (the pre-fix code re-derived the key from the projection
        // and silently dropped every row when that re-derivation
        // returned `None`).
        //
        // Skip for aggregate queries AND pivot/unpivot queries —
        // their output rows are synthesized (one row per group key
        // or one row per (input_row × source_col)), not in lockstep
        // with `state.store` row indices, so per-source-row
        // tombstone semantics don't apply.
        let synth_output = parsed.is_aggregate()
            || !parsed.group_by.is_empty()
            || parsed.is_pivot();
        if !synth_output && !self.config.key_fields.is_empty() {
            let live_rows: std::collections::HashSet<u32> =
                state.key_to_row.values().copied().collect();
            // Walk rows + source_rows in lockstep; retain only
            // entries whose source row index is still live.
            let mut kept_rows = Vec::with_capacity(result.rows.len());
            let mut kept_src = Vec::with_capacity(result.source_rows.len());
            for (row_map, src) in result.rows.into_iter().zip(result.source_rows.into_iter()) {
                if live_rows.contains(&src) {
                    kept_rows.push(row_map);
                    kept_src.push(src);
                }
            }
            result.rows = kept_rows;
            result.source_rows = kept_src;
            result.total_matches = result.rows.len();
        }
        Ok(result)
    }

    /// Streaming SOW: walk rows that match `sql`, project each into a
    /// `Map`, and invoke `emit` with batches of up to `batch_size` rows.
    /// The state lock is held for the entire iteration so the snapshot
    /// is point-in-time consistent (subsequent writers block until
    /// completion). Returns the total number of matching rows.
    ///
    /// Avoids the `Vec<Map<...>>` materialization that `query()` does
    /// — at 40k × 100 wide rows the saved peak heap is on the order of
    /// 50 MB. If `ORDER BY` or `LIMIT` are present the inner
    /// implementation falls back to `query()` (we'd otherwise need a
    /// heap or sort buffer the size of the result anyway).
    pub fn query_streaming<F>(
        &self,
        sql: &str,
        batch_size: usize,
        mut emit: F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(Vec<serde_json::Map<String, serde_json::Value>>),
    {
        let state = self.state.read();
        let query = parse_query(sql, &state.schema)?;
        // Sort / limit / aggregation need a buffer the size of the
        // result. Use the materialized path for those — it still
        // benefits from the chunked emission below.
        let needs_full_buffer = !query.order_by.is_empty()
            || query.limit.is_some()
            || query.is_aggregate()
            || !query.group_by.is_empty()
            // P2 — computed columns (`SELECT a + b AS sum FROM t`)
            // need the full executor's row-build path; the direct
            // columnar stream below only emits raw projected cells.
            || !query.computed.is_empty()
            // Q3 — PIVOT/UNPIVOT queries also need the buffered
            // path; the fast path emits raw projected cells and
            // bypasses execute_pivot_query entirely.
            || query.is_pivot()
            // Q7 — window functions need the buffered path so we can
            // partition + sort before emitting.
            || !query.windows.is_empty();
        if needs_full_buffer {
            let mut result = crate::query::execute_query_with_index(
                &query,
                &state.store,
                Some(&state.secondary_index),
            );
            // Drop tombstoned rows by row-index lookup (same filter
            // shape as `query()` — robust against projections that
            // omit the key column).
            let is_aggregate = query.is_aggregate() || !query.group_by.is_empty();
            if !is_aggregate && !self.config.key_fields.is_empty() {
                let live_rows: std::collections::HashSet<u32> =
                    state.key_to_row.values().copied().collect();
                let mut kept_rows = Vec::with_capacity(result.rows.len());
                for (row_map, src) in
                    result.rows.into_iter().zip(result.source_rows.iter().copied())
                {
                    if live_rows.contains(&src) {
                        kept_rows.push(row_map);
                    }
                }
                result.rows = kept_rows;
            }
            let total = result.rows.len();
            drop(state);
            for chunk in result.rows.chunks(batch_size.max(1)) {
                emit(chunk.to_vec());
            }
            return Ok(total);
        }

        let proj_indices: Vec<usize> = if query.projection.is_empty() {
            (0..state.schema.column_count()).collect()
        } else {
            query.projection.clone()
        };
        let cap = batch_size.max(1);
        let mut batch: Vec<serde_json::Map<String, serde_json::Value>> = Vec::with_capacity(cap);
        let mut total: usize = 0;
        let candidates = crate::query::plan_candidates(
            &query,
            &state.store,
            Some(&state.secondary_index),
        );
        // Tombstone filter: bitmap of live rows from key_to_row.
        // Cheap to build (up to row_count entries), checked O(1) in
        // the hot loop.
        let live_rows: std::collections::HashSet<u32> =
            state.key_to_row.values().copied().collect();
        let has_keys = !self.config.key_fields.is_empty();
        candidates.for_each(|row| {
            if has_keys && !live_rows.contains(&row) {
                return;
            }
            if !query.predicate.matches(&state.store, row) {
                return;
            }
            batch.push(state.store.get_row_map_projected(row, &proj_indices));
            if batch.len() >= cap {
                total += batch.len();
                let taken = std::mem::replace(&mut batch, Vec::with_capacity(cap));
                emit(taken);
            }
        });
        if !batch.is_empty() {
            total += batch.len();
            emit(batch);
        }
        Ok(total)
    }

    /// JSON-bytes streaming variant of `query_streaming`. Each emitted
    /// batch is a `Vec<Vec<u8>>` where every inner `Vec<u8>` is a
    /// pre-serialized JSON object (one per row). Skips the
    /// `serde_json::Map<String, Value>` intermediate that
    /// `query_streaming` builds — eliminates the per-cell String key
    /// allocation and the HashMap bucket overhead, both of which
    /// dominate CPU on wide-row topics (e.g. /trades, 865K × 13 cols).
    ///
    /// Used by the SOW snapshot delivery path. Where the existing
    /// `query_streaming` was producing `Vec<Map>` only to immediately
    /// `serde_json::to_vec` each row downstream, this path skips both
    /// steps: column-store values → JSON bytes in a single sweep.
    ///
    /// Falls back to the materialized executor (and serializes each
    /// row at the end) when the query requires a full result buffer
    /// (ORDER BY / LIMIT / GROUP BY / aggregate). Those paths are not
    /// the wide-row bottleneck so the optimization isn't needed there.
    pub fn query_streaming_json<F>(
        &self,
        sql: &str,
        batch_size: usize,
        mut emit: F,
    ) -> Result<usize, QueryError>
    where
        F: FnMut(Vec<Vec<u8>>),
    {
        let state = self.state.read();
        let query = parse_query(sql, &state.schema)?;
        let needs_full_buffer = !query.order_by.is_empty()
            || query.limit.is_some()
            || query.is_aggregate()
            || !query.group_by.is_empty()
            // P2 — computed columns (`SELECT a + b AS sum FROM t`)
            // need the full executor's row-build path; the direct
            // columnar stream below only emits raw projected cells.
            || !query.computed.is_empty()
            // Q3 — PIVOT/UNPIVOT queries also need the buffered
            // path; the fast path emits raw projected cells and
            // bypasses execute_pivot_query entirely.
            || query.is_pivot()
            // Q7 — window functions need the buffered path so we can
            // partition + sort before emitting.
            || !query.windows.is_empty();
        if needs_full_buffer {
            // Same shape as query_streaming for the buffered path —
            // run the executor, drop tombstones, then serialize each
            // row to bytes. The wide-row CPU win doesn't apply here
            // (these queries are bounded by group cardinality, not
            // row count), but we keep the API surface uniform.
            let mut result = crate::query::execute_query_with_index(
                &query,
                &state.store,
                Some(&state.secondary_index),
            );
            // Synth-output queries (aggregate / GROUP BY / PIVOT /
            // UNPIVOT) emit rows that don't map 1:1 to source rows,
            // so the tombstone filter doesn't apply — applying it
            // would silently drop every output row because
            // source_rows is empty for synth output.
            let synth_output = query.is_aggregate()
                || !query.group_by.is_empty()
                || query.is_pivot();
            if !synth_output && !self.config.key_fields.is_empty() {
                let live_rows: std::collections::HashSet<u32> =
                    state.key_to_row.values().copied().collect();
                let mut kept = Vec::with_capacity(result.rows.len());
                for (row_map, src) in
                    result.rows.into_iter().zip(result.source_rows.iter().copied())
                {
                    if live_rows.contains(&src) {
                        kept.push(row_map);
                    }
                }
                result.rows = kept;
            }
            let total = result.rows.len();
            drop(state);
            let cap = batch_size.max(1);
            let mut batch: Vec<Vec<u8>> = Vec::with_capacity(cap);
            for row in result.rows {
                let bytes = serde_json::to_vec(&row).unwrap_or_else(|_| b"{}".to_vec());
                batch.push(bytes);
                if batch.len() >= cap {
                    emit(std::mem::replace(&mut batch, Vec::with_capacity(cap)));
                }
            }
            if !batch.is_empty() {
                emit(batch);
            }
            return Ok(total);
        }

        let proj_indices: Vec<usize> = if query.projection.is_empty() {
            (0..state.schema.column_count()).collect()
        } else {
            query.projection.clone()
        };
        let cap = batch_size.max(1);
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(cap);
        let mut total: usize = 0;
        let candidates = crate::query::plan_candidates(
            &query,
            &state.store,
            Some(&state.secondary_index),
        );
        let live_rows: std::collections::HashSet<u32> =
            state.key_to_row.values().copied().collect();
        let has_keys = !self.config.key_fields.is_empty();
        // Reused buffer for the direct-stream encoder. Each row's bytes
        // are drained into a fresh `Vec<u8>` for the batch; the encoder
        // appends to this then we `std::mem::take`.
        let mut row_buf: Vec<u8> = Vec::with_capacity(256);
        candidates.for_each(|row| {
            if has_keys && !live_rows.contains(&row) {
                return;
            }
            if !query.predicate.matches(&state.store, row) {
                return;
            }
            row_buf.clear();
            state
                .store
                .write_row_json_projected(row, &proj_indices, &mut row_buf);
            // Shrink each emitted row's allocation to its actual size
            // so the eventual `Arc<Vec<u8>>` (for the fanout cache)
            // doesn't carry over the per-row buffer's capacity.
            let mut taken = std::mem::take(&mut row_buf);
            taken.shrink_to_fit();
            batch.push(taken);
            row_buf = Vec::with_capacity(256);
            if batch.len() >= cap {
                total += batch.len();
                emit(std::mem::replace(&mut batch, Vec::with_capacity(cap)));
            }
        });
        if !batch.is_empty() {
            total += batch.len();
            emit(batch);
        }
        Ok(total)
    }

    // ==================== Subscription API ====================

    /// Subscribe: compute snapshot, register subscription, seed active set.
    pub fn subscribe(
        &self,
        sub_id: String,
        sql: &str,
    ) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, ParsedQuery), QueryError> {
        self.subscribe_inner(sub_id, sql, false, false)
    }

    /// Subscribe in sparse-delta mode. Update deltas will only carry
    /// changed fields (plus the topic's key columns for correlation);
    /// Remove sends key-only.
    pub fn subscribe_sparse(
        &self,
        sub_id: String,
        sql: &str,
    ) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, ParsedQuery), QueryError> {
        self.subscribe_inner(sub_id, sql, true, false)
    }

    /// Like `subscribe_sparse` but the initial snapshot delivers only
    /// the topic's key column(s) per matching row — no payload
    /// fields. Subsequent updates remain sparse.
    pub fn subscribe_sparse_send_keys(
        &self,
        sub_id: String,
        sql: &str,
    ) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, ParsedQuery), QueryError> {
        self.subscribe_inner(sub_id, sql, true, true)
    }

    /// Register a subscription without materializing the initial
    /// snapshot as a `Vec<Map>`. Use this from the transport layer when
    /// wire delivery uses `query_streaming` — it skips the entire
    /// `execute_query(...)` call for non-aggregate queries, which is
    /// the dominant cost on wide topics (e.g. `/trades` with 800K
    /// rows would otherwise allocate ~1 GB of `serde_json::Map`s
    /// per subscriber just to discard them).
    ///
    /// Aggregate / GROUP BY subscriptions still run `execute_query`
    /// internally to seed `last_emitted` — but their result is
    /// bounded by group cardinality, not row count, so it's safe.
    ///
    /// Returns the parsed query; the caller should drive wire
    /// delivery via `query_streaming`.
    pub fn subscribe_register(
        &self,
        sub_id: String,
        sql: &str,
        sparse: bool,
    ) -> Result<ParsedQuery, QueryError> {
        let state = self.state.read();
        let mut query = parse_query(sql, &state.schema)?;
        // PIVOT queries are re-evaluated on every mutation the same way
        // GROUP BY aggregates are — the executor (`execute_pivot_query`)
        // is keyed by anchor columns and the resulting rows have one
        // entry per anchor key. We borrow the aggregating evaluator's
        // diff machinery by seeding `query.group_by` with the pivot's
        // anchor columns; the dispatch in
        // `execute_query_with_index_filtered` checks `query.pivot`
        // first, so SQL routing still hits the pivot executor — only
        // the live-evaluator diff path uses the synthetic group_by.
        if let Some(pivot) = &query.pivot {
            if query.group_by.is_empty() {
                query.group_by = pivot.anchor_cols.clone();
            }
        }
        let is_aggregate =
            query.is_aggregate() || !query.group_by.is_empty() || query.pivot.is_some();
        let captured = self.next_sequence.load(Ordering::SeqCst);
        let mut engine = self.engine_for(&sub_id).lock();
        let mut sub =
            Subscription::new(sub_id.clone(), query.clone()).with_live_start(captured + 1);
        if sparse {
            sub = sub.into_sparse(state.key_col_indices.clone());
        }
        if is_aggregate {
            // Aggregate/PIVOT subs need a seed snapshot for
            // `last_emitted`. Cardinality is bounded by GROUP BY (or by
            // distinct anchor keys for PIVOT), not by row count. The
            // live-rows filter keeps tombstoned source rows out of a
            // phantom null-key group (matches the view path + the
            // incremental membership seed below).
            let live_rows: roaring::RoaringBitmap =
                state.key_to_row.values().copied().collect();
            let result = crate::query::execute_query_with_index_filtered(
                &query,
                &state.store,
                Some(&state.secondary_index),
                Some(&live_rows),
            );
            let mut sub_with_agg = sub.into_aggregating();
            let group_names: Vec<String> = sub_with_agg
                .query
                .group_by
                .iter()
                .map(|&i| state.schema.column_name(i).to_string())
                .collect();
            if let Some(agg) = sub_with_agg.aggregate.as_mut() {
                for row in &result.rows {
                    let k = crate::subscription::group_key_canonical(row, &group_names);
                    agg.last_emitted.insert(k, row.clone());
                }
            }
            // S19-incremental — seed per-group row membership so live
            // events recompute only the group(s) they touch. No-op for
            // shapes the incremental evaluator can't maintain.
            crate::subscription::seed_aggregate_membership(
                &mut sub_with_agg,
                &state.store,
                &live_rows,
            );
            engine.add(sub_with_agg);
        } else {
            engine.add(sub);
            engine.seed_active_set(&sub_id, &state.store);
        }
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(query)
    }

    fn subscribe_inner(
        &self,
        sub_id: String,
        sql: &str,
        sparse: bool,
        send_keys_only_snapshot: bool,
    ) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, ParsedQuery), QueryError> {
        let state = self.state.read();
        let query = parse_query(sql, &state.schema)?;
        let is_aggregate = query.is_aggregate() || !query.group_by.is_empty();
        let live_rows: roaring::RoaringBitmap =
            state.key_to_row.values().copied().collect();
        // Aggregate snapshots run the tombstone-filtered executor so
        // deleted source rows don't bucket into a phantom null-key
        // group; this also keeps the snapshot, `last_emitted`, and the
        // incremental membership seed in lockstep.
        let mut result = if is_aggregate {
            crate::query::execute_query_with_index_filtered(
                &query,
                &state.store,
                Some(&state.secondary_index),
                Some(&live_rows),
            )
        } else {
            execute_query(&query, &state.store)
        };
        // Drop tombstoned rows from the snapshot by row-index
        // lookup — same shape as `query()` / streaming. Robust to
        // projections that exclude the key column. Aggregate /
        // GROUP BY queries already filtered above.
        if !is_aggregate && !self.config.key_fields.is_empty() {
            let mut kept = Vec::with_capacity(result.rows.len());
            for (row_map, src) in
                result.rows.into_iter().zip(result.source_rows.iter().copied())
            {
                if live_rows.contains(src) {
                    kept.push(row_map);
                }
            }
            result.rows = kept;
        }
        // Capture sequence high-water UNDER state.read(). Writers
        // increment next_sequence INSIDE state.write() (see
        // write_store + delete), so the value we observe here is ≥
        // every sequence whose mutation is already visible in
        // `state.store` — equivalently, every sequence whose
        // mutation is included in the snapshot we just executed.
        // Setting live_start = captured + 1 makes the evaluator
        // suppress redelivery of those events (which are still
        // queued on mutation_tx) while passing every strictly-newer
        // event through. This is the S32 / C1 atomicity contract.
        let captured = self.next_sequence.load(Ordering::SeqCst);
        let mut engine = self.engine_for(&sub_id).lock();
        let mut sub =
            Subscription::new(sub_id.clone(), query.clone()).with_live_start(captured + 1);
        if sparse {
            sub = sub.into_sparse(state.key_col_indices.clone());
        }
        // S19: continuous-aggregate subscriptions seed their
        // `last_emitted` map from the initial snapshot, then re-run
        // on every mutation. The evaluator's aggregate branch
        // computes the diff and emits per-group deltas.
        if is_aggregate {
            let mut sub_with_agg = sub.into_aggregating();
            // Pre-populate last_emitted with the snapshot rows so
            // the first post-subscribe mutation only emits *real*
            // changes (Add for new groups, Update on shifted
            // aggregates, Remove on vanished). Without this seed,
            // the first event would re-emit every snapshot group
            // as an Add — duplicating the snapshot delivery.
            let group_names: Vec<String> = sub_with_agg
                .query
                .group_by
                .iter()
                .map(|&i| state.schema.column_name(i).to_string())
                .collect();
            if let Some(agg) = sub_with_agg.aggregate.as_mut() {
                for row in &result.rows {
                    let k = crate::subscription::group_key_canonical(row, &group_names);
                    agg.last_emitted.insert(k, row.clone());
                }
            }
            // S19-incremental — seed per-group row membership so live
            // events recompute only the group(s) they touch.
            crate::subscription::seed_aggregate_membership(
                &mut sub_with_agg,
                &state.store,
                &live_rows,
            );
            engine.add(sub_with_agg);
        } else {
            engine.add(sub);
            engine.seed_active_set(&sub_id, &state.store);
        }
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let snapshot_rows = if send_keys_only_snapshot {
            let key_names: Vec<String> = state
                .key_col_indices
                .iter()
                .map(|&i| state.schema.column_name(i).to_string())
                .collect();
            result
                .rows
                .into_iter()
                .map(|row| {
                    let mut out = serde_json::Map::with_capacity(key_names.len());
                    for k in &key_names {
                        if let Some(v) = row.get(k) {
                            out.insert(k.clone(), v.clone());
                        }
                    }
                    out
                })
                .collect()
        } else {
            result.rows
        };
        Ok((snapshot_rows, query))
    }

    /// Subscribe and capture the sequence high-water at the moment of
    /// registration. The caller is expected to stream every log entry
    /// with `bookmark < seq <= captured` to the subscriber; live events
    /// at `seq > captured` flow through the regular evaluator path. Live
    /// events at `seq <= captured` are silently suppressed by the engine
    /// (they're covered by the replay).
    ///
    /// Returns `(parsed_query, captured_sequence)`.
    pub fn subscribe_with_bookmark(
        &self,
        sub_id: String,
        sql: &str,
    ) -> Result<(ParsedQuery, u64), QueryError> {
        let state = self.state.read();
        let query = parse_query(sql, &state.schema)?;
        let captured = self.next_sequence.load(Ordering::SeqCst);
        let mut engine = self.engine_for(&sub_id).lock();
        let sub = Subscription::new(sub_id.clone(), query.clone())
            .with_live_start(captured + 1);
        engine.add(sub);
        engine.seed_active_set(&sub_id, &state.store);
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok((query, captured))
    }

    /// Filesystem path of this topic's transaction log, if attached.
    /// Bookmark replay opens a fresh `TxLogReader` against this path.
    pub fn txlog_path(&self) -> Option<std::path::PathBuf> {
        self.txlog.as_ref().map(|w| w.lock().path().to_path_buf())
    }

    pub fn unsubscribe(&self, sub_id: &str) {
        let mut engine = self.engine_for(sub_id).lock();
        // Flip the closed flag BEFORE removing the entry. Any evaluator
        // pass that already captured a reference to this sub from the
        // engine (we hold the lock so this can't be in progress, but
        // belt + braces — keeps the close-then-remove ordering
        // explicit) sees the flag and bails out of work for it.
        engine.mark_closed(sub_id);
        if engine.remove(sub_id).is_some() {
            self.active_subscriptions
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }

    pub fn unsubscribe_prefix(&self, prefix: &str) {
        // A prefix can match subscriptions in any lane (the id→lane hash
        // ignores the prefix structure), so sweep every lane.
        let mut removed = 0usize;
        for engine in &self.sub_engines {
            removed += engine.lock().remove_by_prefix(prefix);
        }
        if removed > 0 {
            self.active_subscriptions
                .fetch_sub(removed, std::sync::atomic::Ordering::AcqRel);
        }
    }

    /// Mark a subscription as closed without removing the engine
    /// entry. Cheap; safe to call from a disconnect handler that may
    /// race with the evaluator. The next event evaluator pass skips
    /// the sub, and the next [`reap_closed_subscriptions`] call
    /// drops it. See S38 / review C8.
    pub fn close_subscription(&self, sub_id: &str) -> bool {
        self.engine_for(sub_id).lock().mark_closed(sub_id)
    }

    /// Drop every subscription whose `closed` flag is set. Intended to
    /// be invoked periodically (e.g., once per second from a per-topic
    /// reaper task) and after large disconnect storms.
    pub fn reap_closed_subscriptions(&self) -> usize {
        let mut reaped = 0usize;
        for engine in &self.sub_engines {
            reaped += engine.lock().reap_closed();
        }
        if reaped > 0 {
            self.active_subscriptions
                .fetch_sub(reaped, std::sync::atomic::Ordering::AcqRel);
        }
        reaped
    }

    /// Subscribe with an active-set cap (S42 / C11). Beyond the cap,
    /// the sub is closed with reason `TooManyMatches` and the
    /// client is expected to narrow its filter.
    pub fn subscribe_with_cap(
        &self,
        sub_id: String,
        sql: &str,
        max_active: u32,
    ) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, ParsedQuery), QueryError> {
        let state = self.state.read();
        let query = parse_query(sql, &state.schema)?;
        let mut result = execute_query(&query, &state.store);
        let is_aggregate = query.is_aggregate() || !query.group_by.is_empty();
        if !is_aggregate && !self.config.key_fields.is_empty() {
            let live_rows: std::collections::HashSet<u32> =
                state.key_to_row.values().copied().collect();
            let mut kept = Vec::with_capacity(result.rows.len());
            for (row_map, src) in
                result.rows.into_iter().zip(result.source_rows.iter().copied())
            {
                if live_rows.contains(&src) {
                    kept.push(row_map);
                }
            }
            result.rows = kept;
        }
        let captured = self.next_sequence.load(Ordering::SeqCst);
        let mut engine = self.engine_for(&sub_id).lock();
        let sub = Subscription::new(sub_id.clone(), query.clone())
            .with_live_start(captured + 1)
            .with_max_active(max_active);
        engine.add(sub);
        engine.seed_active_set(&sub_id, &state.store);
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok((result.rows, query))
    }

    /// Read the active-set size of a subscription. Returns `None` if
    /// the subscription doesn't exist (or has been reaped). Intended
    /// for tests and memory-bound assertions.
    pub fn subscription_active_set_size(&self, sub_id: &str) -> Option<u64> {
        self.engine_for(sub_id)
            .lock()
            .get(sub_id)
            .map(|s| s.active_set.len())
    }

    /// Read the close reason of a subscription, if it has been
    /// closed via an enforcement path (today: `TooManyMatches`).
    pub fn subscription_close_reason(
        &self,
        sub_id: &str,
    ) -> Option<crate::subscription::CloseReason> {
        self.engine_for(sub_id)
            .lock()
            .get(sub_id)
            .and_then(|s| s.close_reason())
    }

    /// Read the closed-ness of a subscription. Useful for tests that
    /// publish, then observe whether the cap fired.
    pub fn subscription_is_closed(&self, sub_id: &str) -> Option<bool> {
        self.engine_for(sub_id)
            .lock()
            .get(sub_id)
            .map(|s| s.is_closed())
    }

    // ==================== Stats ====================

    pub fn stats(&self) -> serde_json::Value {
        let state = self.state.read();
        let sub_count: usize = self.sub_engines.iter().map(|e| e.lock().count()).sum();
        serde_json::json!({
            "name": self.config.name,
            "rowCount": state.store.row_count(),
            "columnCount": state.schema.column_count(),
            "keyFields": self.config.key_fields,
            "subscriptions": sub_count,
            "globalVersion": state.store.global_version(),
            "capacity": state.store.capacity(),
            "schemaDiscovered": !is_placeholder_schema(&state.schema),
        })
    }

    // ==================== S11 replication barrier ====================

    /// Highest sequence the replication destination has confirmed it
    /// applied. `0` if no replication is configured or no Ack has
    /// landed yet. The async wait helper lives in `cq-transport`
    /// (which has tokio as a dependency); cq-core exposes the
    /// observable atomic + a notify so the caller can `await` on it.
    pub fn last_replicated_sequence(&self) -> u64 {
        self.last_replicated_sequence.load(Ordering::Acquire)
    }

    /// Mark sequence `seq` as replicated (monotonic — `seq < current`
    /// is a no-op). Called by the replication shipper's Ack reader.
    /// Wakes every task currently awaiting via `replication_notify_handle`.
    pub fn mark_replicated(&self, seq: u64) {
        let mut cur = self.last_replicated_sequence.load(Ordering::Relaxed);
        while seq > cur {
            match self.last_replicated_sequence.compare_exchange(
                cur,
                seq,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
        self.replication_notify.notify_waiters();
    }

    /// Cheap clone of the shared `Notify`. Callers (router publish
    /// path) hold this + the `last_replicated_sequence` accessor to
    /// implement the async barrier without dragging tokio into
    /// cq-core's compile-time dep tree.
    pub fn replication_notify_handle(&self) -> Arc<tokio::sync::Notify> {
        self.replication_notify.clone()
    }
}

/// Topics are shared via `Arc`. Internal locks make `Topic` `Send + Sync`.
pub type SharedTopic = Arc<Topic>;

// ───────── nested-publish helpers ─────────

/// `true` if any top-level value in the map is an object or array —
/// the only cases where the flattener changes the result. Avoids
/// allocating a new map when publishers already send flat rows
/// (overwhelmingly the common case at high publish rates).
fn map_has_nesting(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.values()
        .any(|v| matches!(v, serde_json::Value::Object(_) | serde_json::Value::Array(_)))
}

/// Flatten nested objects and arrays to dotted-path / bracket-indexed
/// keys, returning a new `serde_json::Map`. The column store works on
/// flat columns; nested publish payloads are an ergonomic convenience.
fn flatten_publish_map(
    map: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    let value = serde_json::Value::Object(map.clone());
    let flat = flatten(&value, &FlattenConfig::default());
    let mut out = serde_json::Map::with_capacity(flat.len());
    for (k, v) in flat {
        out.insert(k, v);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnType;
    use crate::subscription::DeltaType;
    use compact_str::CompactString;

    // ───── Q11 — schema evolution: online add column ─────────────────

    #[test]
    fn add_column_appends_with_null_default_preserving_existing_rows() {
        let topic = make_topic();
        // Publish 2 rows on the original 3-col schema (symbol, price,
        // quantity).
        let mut r1 = serde_json::Map::new();
        r1.insert("symbol".into(), "AAPL".into());
        r1.insert("price".into(), 150.0.into());
        r1.insert("quantity".into(), 100i64.into());
        topic.upsert_map(&r1).unwrap();
        let mut r2 = serde_json::Map::new();
        r2.insert("symbol".into(), "MSFT".into());
        r2.insert("price".into(), 300.0.into());
        r2.insert("quantity".into(), 50i64.into());
        topic.upsert_map(&r2).unwrap();
        assert_eq!(topic.row_count(), 2);

        // Add a `desk` column online.
        topic
            .add_column("desk", ColumnType::String)
            .expect("add_column");
        let schema = topic.schema();
        assert_eq!(schema.column_count(), 4);
        assert_eq!(schema.column_name(3), "desk");

        // Existing rows still resolvable via the original key + cols.
        // cqserver omits null fields from the projected map (sparse
        // semantics), so `desk` is ABSENT on rows published before
        // the column was added — equivalent to JSON null on the wire.
        let rows = topic.query("SELECT symbol, price, desk FROM t").unwrap();
        assert_eq!(rows.rows.len(), 2);
        for row in &rows.rows {
            assert!(
                !row.contains_key("desk"),
                "desk should be absent (=null) for pre-evolution rows, got {row:?}"
            );
        }

        // New publishes can populate the new column.
        let mut r3 = serde_json::Map::new();
        r3.insert("symbol".into(), "GOOGL".into());
        r3.insert("price".into(), 2800.0.into());
        r3.insert("quantity".into(), 10i64.into());
        r3.insert("desk".into(), "RATES".into());
        topic.upsert_map(&r3).unwrap();
        let rows = topic
            .query("SELECT symbol, desk FROM t WHERE desk = 'RATES'")
            .unwrap();
        assert_eq!(rows.rows.len(), 1);
        assert_eq!(rows.rows[0].get("symbol").unwrap().as_str().unwrap(), "GOOGL");
    }

    #[test]
    fn add_column_rejects_duplicate_name() {
        let topic = make_topic();
        let r = topic.add_column("symbol", ColumnType::String);
        assert!(r.is_err(), "duplicate column name must error");
    }

    // ───── P14 — canonicalize_topic ─────────────────────────────────

    #[test]
    fn canonicalize_topic_round_trips() {
        assert_eq!(canonicalize_topic("trades"), "/trades");
        assert_eq!(canonicalize_topic("/trades"), "/trades");
        // Idempotent.
        assert_eq!(
            canonicalize_topic(&canonicalize_topic("trades")),
            "/trades"
        );
        // Multi-level paths are preserved as-is when slash-prefixed.
        assert_eq!(canonicalize_topic("/v/by_desk"), "/v/by_desk");
        assert_eq!(canonicalize_topic("v/by_desk"), "/v/by_desk");
        // Empty stays empty.
        assert_eq!(canonicalize_topic(""), "");
    }

    // ───── P5 — keyless topics upsert into row 0 (no append) ────────

    fn make_keyless_topic() -> Topic {
        let schema = Arc::new(Schema::from_strs(
            &["total"],
            &[ColumnType::Double],
        ));
        let config = TopicConfig {
            name: "/v_total".into(),
            key_fields: Vec::new(),
            persist: false,
            conflation_ms: None,
            index_columns: Vec::new(),
            expire_seconds: None,
        };
        Topic::new(config, schema, 16)
    }

    #[test]
    fn keyless_topic_collapses_to_single_row() {
        // AMPS_PARITY §4 bug 3 — without a key, a degenerate-aggregate
        // view used to grow by one row per refresh. After P5, upserts
        // with no key overwrite row 0 so the view stays single-row.
        let topic = make_keyless_topic();
        for v in &[10.0_f64, 20.0, 30.0, 60.0] {
            let mut m = serde_json::Map::new();
            m.insert("total".into(), (*v).into());
            topic.upsert_map(&m).expect("upsert");
        }
        assert_eq!(
            topic.row_count(),
            1,
            "keyless topic must collapse to one row, got {}",
            topic.row_count()
        );
        let snap = topic.query("SELECT total FROM t").expect("query");
        assert_eq!(snap.rows.len(), 1);
        let total = snap.rows[0].get("total").and_then(|v| v.as_f64()).unwrap();
        assert!((total - 60.0).abs() < 1e-9, "row 0 must hold latest value");
    }

    fn make_topic() -> Topic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity"],
            &[ColumnType::String, ColumnType::Double, ColumnType::Long],
        ));
        let config = TopicConfig {
            name: "/market-data".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        };
        Topic::new(config, schema, 100)
    }

    fn row_map(sym: &str, px: f64) -> serde_json::Map<String, serde_json::Value> {
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), sym.into());
        m.insert("price".into(), px.into());
        m.insert("quantity".into(), 1i64.into());
        m
    }

    #[test]
    fn foreign_origin_replays_dedup_per_origin() {
        let topic = make_topic().with_origin_id("self");
        // Two distinct foreign origins with overlapping sequence numbers.
        topic.replay_upsert_map_origin("node-a", 1, &row_map("AAPL", 10.0));
        topic.replay_upsert_map_origin("node-b", 1, &row_map("MSFT", 20.0));
        // Both applied — independent sequence spaces, no cross-suppression.
        assert_eq!(topic.row_count(), 2);

        // Re-delivery of node-a seq 1 (alternate path) is a convergent no-op.
        topic.replay_upsert_map_origin("node-a", 1, &row_map("AAPL", 999.0));
        let res = topic.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 10.0);

        // A higher sequence from node-a is applied (first-writer-wins only
        // suppresses already-seen sequences).
        topic.replay_upsert_map_origin("node-a", 2, &row_map("AAPL", 30.0));
        let res = topic.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 30.0);

        let hw = topic.replicated_highwater_snapshot();
        assert_eq!(hw.get("node-a"), Some(&2));
        assert_eq!(hw.get("node-b"), Some(&1));
    }

    #[test]
    fn own_origin_replay_does_not_touch_foreign_highwater() {
        let topic = make_topic().with_origin_id("self");
        // An entry tagged with our own origin uses the single-source gate
        // (last_applied_sequence / next_sequence), not the per-origin map.
        topic.replay_upsert_map_origin("self", 5, &row_map("AAPL", 10.0));
        assert!(topic.replicated_highwater_snapshot().is_empty());
        // Re-applying seq <= last_applied is suppressed.
        topic.replay_upsert_map_origin("self", 5, &row_map("AAPL", 999.0));
        let res = topic.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 10.0);
    }

    #[test]
    fn delta_upsert_merges_into_existing_row() {
        let topic = make_topic();
        // Seed a full row.
        let mut full = serde_json::Map::new();
        full.insert("symbol".into(), "AAPL".into());
        full.insert("price".into(), 150.0.into());
        full.insert("quantity".into(), 100i64.into());
        topic.upsert_map(&full).unwrap();

        // Delta-publish only the price change.
        let mut sparse = serde_json::Map::new();
        sparse.insert("symbol".into(), "AAPL".into());
        sparse.insert("price".into(), 175.0.into());
        topic.delta_upsert_map(&sparse).unwrap();

        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        // Updated field.
        assert_eq!(row.get("price").unwrap(), 175.0);
        // Quantity was NOT in the sparse payload — must keep its
        // original value, not become null.
        assert_eq!(row.get("quantity").unwrap(), 100);
    }

    #[test]
    fn delta_upsert_inserts_when_key_is_new() {
        let topic = make_topic();
        // No existing row — delta_publish becomes a regular insert.
        let mut sparse = serde_json::Map::new();
        sparse.insert("symbol".into(), "TSLA".into());
        sparse.insert("price".into(), 800.0.into());
        topic.delta_upsert_map(&sparse).unwrap();

        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'TSLA'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price").unwrap(), 800.0);
    }

    #[test]
    fn subscribe_sparse_send_keys_strips_payload_from_snapshot() {
        // Seed two rows, then sparse+send_keys subscribe → snapshot
        // should carry only the key column.
        let topic = make_topic();
        let mut m1 = serde_json::Map::new();
        m1.insert("symbol".into(), "AAPL".into());
        m1.insert("price".into(), 150.0.into());
        m1.insert("quantity".into(), 100i64.into());
        topic.upsert_map(&m1).unwrap();
        let mut m2 = serde_json::Map::new();
        m2.insert("symbol".into(), "MSFT".into());
        m2.insert("price".into(), 300.0.into());
        m2.insert("quantity".into(), 50i64.into());
        topic.upsert_map(&m2).unwrap();

        let (rows, _q) = topic
            .subscribe_sparse_send_keys("sub-sk".into(), "SELECT * FROM t")
            .expect("subscribe");
        assert_eq!(rows.len(), 2);
        for r in &rows {
            assert_eq!(r.len(), 1, "expected only key column, got {:?}", r);
            assert!(r.contains_key("symbol"));
        }
    }

    #[test]
    fn ttl_sweep_deletes_old_rows() {
        // Build a topic with a 0-second TTL — every row is
        // immediately eligible. After a sweep, all keys should be
        // gone.
        let schema = Arc::new(crate::schema::Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/ttl-test".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: Some(0),
            },
            schema,
            32,
        );
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), "AAPL".into());
        m.insert("price".into(), 150.0.into());
        topic.upsert_map(&m).unwrap();
        assert_eq!(topic.row_count(), 1);

        // TTL=0, so any positive elapsed time is "expired". Let a
        // millisecond pass so `now.duration_since(touched)` is >0.
        std::thread::sleep(std::time::Duration::from_millis(2));
        let deleted = topic.sweep_expired().unwrap();
        assert_eq!(deleted, vec!["AAPL".to_string()]);
        // After the sweep, querying returns nothing.
        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert!(result.rows.is_empty(), "expected no rows after TTL sweep");
    }

    #[test]
    fn replay_dedup_skips_already_applied_sequence() {
        // Apply seq=5 once → row appears. Apply same seq again →
        // skip (state unchanged, no second mutation).
        let topic = make_topic();
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), "AAPL".into());
        m.insert("price".into(), 150.0.into());
        m.insert("quantity".into(), 100i64.into());
        topic.replay_upsert_map(5, &m);
        assert_eq!(topic.row_count(), 1);
        assert_eq!(topic.current_sequence(), 5);

        // Apply same seq with a different payload — should be dropped.
        let mut m2 = serde_json::Map::new();
        m2.insert("symbol".into(), "AAPL".into());
        m2.insert("price".into(), 999.0.into());
        m2.insert("quantity".into(), 100i64.into());
        topic.replay_upsert_map(5, &m2);
        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get("price").unwrap(),
            150.0,
            "dedup should have prevented the second apply"
        );
    }

    #[test]
    fn ttl_disabled_keeps_rows_indefinitely() {
        let schema = Arc::new(crate::schema::Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let topic = Topic::new(
            TopicConfig {
                name: "/no-ttl".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: None,
            },
            schema,
            32,
        );
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), "AAPL".into());
        m.insert("price".into(), 150.0.into());
        topic.upsert_map(&m).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let deleted = topic.sweep_expired().unwrap();
        assert!(deleted.is_empty(), "no-ttl topic should never sweep");
    }

    #[test]
    fn test_upsert_and_query() {
        let topic = make_topic();

        let row = topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::Long(100),
        ]);
        assert_eq!(row, 0);
        assert_eq!(topic.row_count(), 1);

        let row = topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(155.0),
            Value::Long(100),
        ]);
        assert_eq!(row, 0); // same key → same row
        assert_eq!(topic.row_count(), 1);

        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price").unwrap(), 155.0);
    }

    #[test]
    fn upsert_map_batch_matches_per_row_upserts() {
        let topic = make_topic();
        let rx = topic.take_mutation_rx().expect("rx should be available");

        let row = |sym: &str, px: f64, qty: i64| {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), sym.into());
            m.insert("price".into(), px.into());
            m.insert("quantity".into(), qty.into());
            m
        };
        let r0 = row("AAPL", 150.0, 100);
        let r1 = row("MSFT", 300.0, 50);
        let r2 = row("GOOG", 99.0, 7);
        let maps = [&r0, &r1, &r2];

        let seqs = topic.upsert_map_batch(&maps).unwrap();
        // One sequence per row, strictly increasing.
        assert_eq!(seqs.len(), 3);
        assert!(seqs[0] < seqs[1] && seqs[1] < seqs[2]);
        // Three distinct keys → three rows.
        assert_eq!(topic.row_count(), 3);

        // One mutation event per row, in input/row order.
        for expected_row in 0..3u32 {
            let e = rx.recv().unwrap();
            assert_eq!(e.row, expected_row);
            assert_eq!(e.kind, MutationKind::Upsert);
        }

        // Values landed correctly.
        let res = topic.query("SELECT * FROM t WHERE symbol = 'MSFT'").unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0].get("price").unwrap(), 300.0);
    }

    #[test]
    fn upsert_map_batch_last_write_wins_on_duplicate_key() {
        let topic = make_topic();
        let row = |sym: &str, px: f64| {
            let mut m = serde_json::Map::new();
            m.insert("symbol".into(), sym.into());
            m.insert("price".into(), px.into());
            m.insert("quantity".into(), 1i64.into());
            m
        };
        let r0 = row("AAPL", 150.0);
        let r1 = row("AAPL", 175.0); // same key, later in the frame
        let seqs = topic.upsert_map_batch(&[&r0, &r1]).unwrap();
        assert_eq!(seqs.len(), 2);
        // Same key collapses onto one row; the later value wins.
        assert_eq!(topic.row_count(), 1);
        let res = topic.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(res.rows.len(), 1);
        assert_eq!(res.rows[0].get("price").unwrap(), 175.0);
    }

    #[test]
    fn upsert_map_batch_empty_is_noop() {
        let topic = make_topic();
        let seqs = topic.upsert_map_batch(&[]).unwrap();
        assert!(seqs.is_empty());
        assert_eq!(topic.row_count(), 0);
    }

    fn make_topic_lanes(lanes: usize) -> Topic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity"],
            &[ColumnType::String, ColumnType::Double, ColumnType::Long],
        ));
        let config = TopicConfig {
            name: "/market-data".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        };
        Topic::new(config, schema, 100).with_eval_lanes(lanes)
    }

    #[test]
    fn sharded_engine_routes_subs_to_distinct_lanes() {
        let topic = make_topic_lanes(4);
        assert_eq!(topic.eval_lanes(), 4);

        // Register several subscriptions; each is pinned to one lane by
        // a stable hash of its id. A subsequent per-sub op (active-set
        // size) must hit the same lane — if `engine_for` were
        // inconsistent between add and lookup, this would return None.
        let mut lanes_used = std::collections::HashSet::new();
        for i in 0..16 {
            let id = format!("sub-{i}");
            topic.subscribe(id.clone(), "SELECT * FROM t").unwrap();
            assert!(
                topic.subscription_active_set_size(&id).is_some(),
                "sub {id} not findable in its lane"
            );
            // Recompute the lane the same way `engine_for` does.
            let mut h = std::collections::hash_map::DefaultHasher::new();
            id.hash(&mut h);
            lanes_used.insert((h.finish() as usize) % 4);
        }
        assert!(
            lanes_used.len() > 1,
            "expected subscriptions to spread across lanes, got {lanes_used:?}"
        );
    }

    #[test]
    fn sharded_evaluate_matches_single_lane() {
        // Identical data + subscriptions on a 1-lane and a 4-lane topic
        // must produce the same set of deltas for a given mutation.
        let single = make_topic_lanes(1);
        let sharded = make_topic_lanes(4);

        let seed = |t: &Topic| {
            t.upsert(vec![
                Value::String(Some(CompactString::new("AAPL"))),
                Value::Double(150.0),
                Value::Long(100),
            ]);
            t.upsert(vec![
                Value::String(Some(CompactString::new("MSFT"))),
                Value::Double(300.0),
                Value::Long(50),
            ]);
            for i in 0..8 {
                t.subscribe(format!("hi-{i}"), "SELECT * FROM t WHERE price > 200")
                    .unwrap();
                t.subscribe(format!("lo-{i}"), "SELECT * FROM t WHERE price <= 200")
                    .unwrap();
            }
        };
        seed(&single);
        seed(&sharded);

        // Flip AAPL above the 200 threshold on both topics.
        let mutate = |t: &Topic| -> Vec<(String, String, u32)> {
            let row = t.upsert(vec![
                Value::String(Some(CompactString::new("AAPL"))),
                Value::Double(250.0),
                Value::Long(100),
            ]);
            let mut out: Vec<(String, String, u32)> = t
                .evaluate_row_kind_indexed(row, t.current_sequence(), MutationKind::Upsert, None)
                .into_iter()
                .map(|d| (d.subscription_id.to_string(), format!("{:?}", d.delta_type), d.row))
                .collect();
            out.sort();
            out
        };

        assert_eq!(mutate(&single), mutate(&sharded));
    }

    #[test]
    fn prefix_unsub_and_reap_span_all_lanes() {
        let topic = make_topic_lanes(4);
        for i in 0..12 {
            topic
                .subscribe(format!("sess-A:{i}"), "SELECT * FROM t")
                .unwrap();
        }
        topic
            .subscribe("sess-B:0".into(), "SELECT * FROM t")
            .unwrap();
        assert_eq!(topic.subscription_count(), 13);

        // Prefix removal must sweep every lane, not just lane 0.
        topic.unsubscribe_prefix("sess-A:");
        assert_eq!(topic.subscription_count(), 1);

        // Close + reap the remaining sub (which may live in any lane).
        assert!(topic.close_subscription("sess-B:0"));
        let reaped = topic.reap_closed_subscriptions();
        assert_eq!(reaped, 1);
        assert_eq!(topic.subscription_count(), 0);
    }

    #[test]
    fn test_subscribe_and_deltas() {
        let topic = make_topic();

        topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::Long(100),
        ]);
        topic.upsert(vec![
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Double(300.0),
            Value::Long(50),
        ]);

        let (snapshot, _) = topic
            .subscribe("sub-1".into(), "SELECT * FROM t WHERE price > 200")
            .unwrap();
        assert_eq!(snapshot.len(), 1);

        let row = topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(250.0),
            Value::Long(100),
        ]);
        let deltas = topic.evaluate_row(row, topic.current_sequence());
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Add);
    }

    #[test]
    fn test_mutation_channel_emits_events() {
        let topic = make_topic();
        let rx = topic.take_mutation_rx().expect("rx should be available");

        topic.upsert(vec![
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Double(150.0),
            Value::Long(100),
        ]);
        topic.upsert(vec![
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Double(300.0),
            Value::Long(50),
        ]);

        let e1 = rx.recv().unwrap();
        let e2 = rx.recv().unwrap();
        assert_eq!(e1.row, 0);
        assert_eq!(e2.row, 1);
    }

    #[test]
    fn test_take_mutation_rx_only_once() {
        let topic = make_topic();
        assert!(topic.take_mutation_rx().is_some());
        assert!(topic.take_mutation_rx().is_none());
    }

    #[test]
    fn mutation_backlog_throttles_producer_without_deadlock() {
        use std::sync::atomic::AtomicUsize;
        let cap = 8usize;
        let topic = Arc::new(make_topic().with_mutation_cap(cap));
        let rx = topic.take_mutation_rx().expect("rx should be available");

        // Slow drainer forces the producer to throttle. If the gate were
        // wired wrong (e.g. blocking send under the write lock), the
        // evaluator-side drain and the producer would deadlock and this
        // test would hang.
        let drained = Arc::new(AtomicUsize::new(0));
        let drained2 = drained.clone();
        let drainer = std::thread::spawn(move || {
            let mut n = 0usize;
            while rx.recv().is_ok() {
                n += 1;
                drained2.store(n, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_micros(100));
                if n == 100 {
                    break;
                }
            }
        });

        for i in 0..100u32 {
            topic.upsert(vec![
                Value::String(Some(CompactString::new(format!("S{i}")))),
                Value::Double(i as f64),
                Value::Long(i as i64),
            ]);
            // The unbounded channel must never grow far past the cap: the
            // producer parks before each publish until the backlog drains
            // below `cap`, then enqueues exactly one more.
            assert!(
                topic.mutation_tx.len() <= cap + 1,
                "backlog grew past cap: {}",
                topic.mutation_tx.len()
            );
        }

        drainer.join().unwrap();
        assert_eq!(drained.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn mutation_backlog_not_throttled_without_consumer() {
        // No consumer ever takes the receiver, so throttling must be a
        // no-op — otherwise publishing past the cap would block forever.
        let topic = make_topic().with_mutation_cap(4);
        for i in 0..50u32 {
            topic.upsert(vec![
                Value::String(Some(CompactString::new(format!("S{i}")))),
                Value::Double(i as f64),
                Value::Long(i as i64),
            ]);
        }
        // All 50 events accumulated unbounded because nobody is draining.
        assert_eq!(topic.mutation_tx.len(), 50);
    }

    fn make_placeholder_topic() -> Topic {
        let schema = Arc::new(Schema::from_strs(&["_key"], &[ColumnType::String]));
        let config = TopicConfig {
            name: "/auto".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        };
        Topic::new(config, schema, 32)
    }

    #[test]
    fn test_schema_discovery_on_first_publish() {
        let topic = make_placeholder_topic();
        assert!(is_placeholder_schema(&topic.schema()));

        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("price".into(), 150.5.into());
        data.insert("quantity".into(), 100.into());

        topic.upsert_map(&data).unwrap();

        // Schema replaced with three columns.
        let schema = topic.schema();
        assert!(!is_placeholder_schema(&schema));
        assert_eq!(schema.column_count(), 3);
        assert!(schema.has_column("symbol"));
        assert!(schema.has_column("price"));
        assert!(schema.has_column("quantity"));

        // Data lands cleanly under the discovered schema.
        let result = topic
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price").unwrap(), 150.5);

        // Subsequent publishes don't re-discover.
        let second_schema_ptr = Arc::as_ptr(&topic.schema());
        let mut data2 = serde_json::Map::new();
        data2.insert("symbol".into(), "MSFT".into());
        data2.insert("price".into(), 300.0.into());
        data2.insert("quantity".into(), 50.into());
        topic.upsert_map(&data2).unwrap();
        assert_eq!(Arc::as_ptr(&topic.schema()), second_schema_ptr);
        assert_eq!(topic.row_count(), 2);
    }

    #[test]
    fn test_discovery_skipped_when_real_schema_present() {
        let topic = make_topic(); // explicit schema
        let schema_before = topic.schema();
        assert!(!is_placeholder_schema(&schema_before));

        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("extra".into(), "ignored".into());
        topic.upsert_map(&data).unwrap();

        // Schema must not have changed — `extra` does not become a column.
        let schema_after = topic.schema();
        assert_eq!(Arc::as_ptr(&schema_before), Arc::as_ptr(&schema_after));
        assert!(!schema_after.has_column("extra"));
    }

    #[test]
    fn test_discovery_skipped_when_subscriptions_present() {
        let topic = make_placeholder_topic();
        // Subscribe BEFORE any publish — predicate is compiled against the
        // placeholder schema. Discovery must respect that and not swap.
        let _ = topic.subscribe("sub-1".into(), "SELECT * FROM t").unwrap();

        let mut data = serde_json::Map::new();
        data.insert("symbol".into(), "AAPL".into());
        data.insert("price".into(), 150.0.into());
        topic.upsert_map(&data).unwrap();

        // Still on placeholder.
        assert!(is_placeholder_schema(&topic.schema()));
    }

    #[test]
    fn flush_txlog_makes_unsynced_writes_durable() {
        // FsyncPolicy::None means the OS may still be buffering on
        // shutdown. flush_txlog() must force the bytes to disk so a
        // subsequent reader sees the full entry.
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades-shutdown.log");
        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open(&path, FsyncPolicy::None).unwrap(),
        ));

        let mut topic = make_topic();
        topic.attach_txlog(writer.clone());

        let mut payload = serde_json::Map::new();
        payload.insert("symbol".into(), "TSLA".into());
        payload.insert("price".into(), 800.0.into());
        topic.upsert_map(&payload).unwrap();

        // Explicit shutdown-style flush — no other sync calls before this.
        topic.flush_txlog().expect("flush should succeed");
        drop(topic);
        drop(writer);

        let entries = TxLogReader::open(&path).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 1, "flush_txlog should make writes durable");
        assert_eq!(entries[0].key, "TSLA");
    }

    #[test]
    fn flush_txlog_is_noop_for_non_persistent_topic() {
        let topic = make_topic();
        // No txlog attached → no-op, no error.
        topic.flush_txlog().expect("non-persistent flush should succeed");
    }

    #[test]
    fn test_publish_writes_to_attached_txlog() {
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.log");

        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open(&path, FsyncPolicy::None).unwrap(),
        ));

        let mut topic = make_topic();
        topic.attach_txlog(writer.clone());

        // Single-phase publish — upsert_map writes the txlog itself now.
        let mut payload = serde_json::Map::new();
        payload.insert("symbol".into(), "AAPL".into());
        payload.insert("price".into(), 150.0.into());
        let seq = topic.upsert_map(&payload).unwrap();
        assert_eq!(seq, 1);

        // Flush before reopening.
        writer.lock().sync().unwrap();
        drop(topic);

        let entries = TxLogReader::open(&path).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].key, "AAPL");
        let decoded: serde_json::Value = serde_json::from_slice(&entries[0].payload).unwrap();
        assert_eq!(decoded.get("symbol").unwrap(), "AAPL");
    }

    #[test]
    fn test_recovery_replays_into_store() {
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("trades.log");

        // Write three publishes + one tombstone to the log.
        {
            let mut w = TxLogWriter::open(&path, FsyncPolicy::None).unwrap();
            let mut seq = 0u64;
            for (sym, price) in [("AAPL", 150.0), ("MSFT", 300.0), ("GOOGL", 2800.0)] {
                let mut m = serde_json::Map::new();
                m.insert("symbol".into(), sym.into());
                m.insert("price".into(), price.into());
                let payload = serde_json::to_vec(&serde_json::Value::Object(m)).unwrap();
                seq += 1;
                w.append(seq, "/trades", sym, &payload).unwrap();
            }
            seq += 1;
            w.append(seq, "/trades", "MSFT", &[]).unwrap(); // tombstone
            w.sync().unwrap();
        }

        // Fresh topic; replay log into it via the recovery API.
        let topic = make_topic();
        let mut reader = TxLogReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 4);
        for e in &entries {
            if e.is_tombstone() {
                topic.replay_delete(e.sequence, &e.key);
            } else {
                let v: serde_json::Value = serde_json::from_slice(&e.payload).unwrap();
                if let serde_json::Value::Object(m) = v {
                    topic.replay_upsert_map(e.sequence, &m);
                }
            }
        }

        // AAPL and GOOGL remain visible; MSFT was tombstoned (key gone).
        let result = topic.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price").unwrap(), 150.0);

        let result = topic.query("SELECT * FROM t WHERE symbol = 'GOOGL'").unwrap();
        assert_eq!(result.rows.len(), 1);

        // After recovery, next sequence > max replayed sequence.
        assert!(topic.current_sequence() >= 4);

        // A re-publish under the MSFT key should land as a fresh row (the
        // old MSFT slot was nulled, key index removed).
        topic
            .upsert_map(&serde_json::Map::from_iter([
                ("symbol".into(), "MSFT".into()),
                ("price".into(), 305.0.into()),
            ]))
            .unwrap();
        let result = topic.query("SELECT * FROM t WHERE symbol = 'MSFT'").unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].get("price").unwrap(), 305.0);
    }
}

#[cfg(test)]
mod view_tap_tests {
    use super::*;
    use crate::schema::ColumnType;

    fn make_topic() -> Topic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity"],
            &[ColumnType::String, ColumnType::Double, ColumnType::Long],
        ));
        let config = TopicConfig {
            name: "/market-data".into(),
            key_fields: vec!["symbol".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        };
        Topic::new(config, schema, 100)
    }

    fn publish(topic: &Topic, symbol: &str, price: f64) {
        let mut m = serde_json::Map::new();
        m.insert("symbol".into(), symbol.into());
        m.insert("price".into(), price.into());
        m.insert("quantity".into(), 100i64.into());
        topic.upsert_map(&m).expect("upsert");
    }

    #[test]
    fn unregister_view_tap_drops_sender_and_disconnects_receiver() {
        let topic = make_topic();
        let (id, rx) = topic.register_view_tap(16);
        publish(&topic, "AAPL", 150.0);
        assert!(rx.recv_timeout(std::time::Duration::from_millis(200)).is_ok());

        topic.unregister_view_tap(id);
        publish(&topic, "MSFT", 300.0);
        assert!(rx.recv_timeout(std::time::Duration::from_millis(200)).is_err());
    }

    #[test]
    fn build_snapshot_returns_none_without_txlog() {
        let topic = make_topic(); // no txlog attached
        assert!(topic.build_snapshot().is_none());
        assert!(!topic.write_snapshot().unwrap());
    }

    /// Sparse-delta journaling: with `sparse_delta_txlog` enabled, a
    /// `delta_upsert_map` journals the `{key + changed fields}` payload as
    /// published (JsonDelta, `0x03`) instead of the fully-merged row —
    /// the fix for wide-row topics whose ticks touch a handful of fields
    /// amplifying into full-row journal writes.
    #[test]
    fn sparse_delta_txlog_journals_delta_as_published_and_replay_merges() {
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::{FsyncPolicy, PayloadFormat};
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap(),
        ));
        let mut topic = make_topic().with_sparse_delta_txlog(true);
        topic.attach_txlog(writer.clone());

        // Full publish establishes the row; sparse delta ticks one field.
        publish(&topic, "AAPL", 150.0); // symbol/price/quantity=100
        let mut delta = serde_json::Map::new();
        delta.insert("symbol".into(), "AAPL".into());
        delta.insert("price".into(), 175.5.into());
        topic.delta_upsert_map(&delta).expect("delta upsert");
        writer.lock().sync().unwrap();

        // The journal must hold the delta AS PUBLISHED (2 fields), not the
        // merged 3-column row.
        let mut r = TxLogReader::open(dir.path()).unwrap();
        let e1 = r.read_next().unwrap().expect("full entry");
        assert_eq!(e1.payload_format, PayloadFormat::Json);
        let e2 = r.read_next().unwrap().expect("delta entry");
        assert_eq!(e2.payload_format, PayloadFormat::JsonDelta);
        let m: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(&e2.payload).unwrap();
        assert_eq!(m.len(), 2, "sparse payload: key + changed field only");
        assert_eq!(m.get("price").unwrap().as_f64().unwrap(), 175.5);
        assert!(m.get("quantity").is_none(), "untouched column not journaled");

        // Recovery: replay both entries into a fresh topic — the delta
        // must MERGE (quantity survives from the full publish).
        drop(topic);
        let restarted = make_topic();
        let mut r = TxLogReader::open(dir.path()).unwrap();
        while let Some(e) = r.read_next().unwrap() {
            if e.payload_format == PayloadFormat::JsonDelta {
                if let Ok(serde_json::Value::Object(m)) =
                    serde_json::from_slice::<serde_json::Value>(&e.payload)
                {
                    restarted.replay_delta_map_origin(&e.origin, e.sequence, &m);
                }
            } else if let Ok(serde_json::Value::Object(m)) =
                serde_json::from_slice::<serde_json::Value>(&e.payload)
            {
                restarted.replay_upsert_map_origin(&e.origin, e.sequence, &m);
            }
        }
        assert_eq!(restarted.row_count(), 1);
        let res = restarted.query("SELECT * FROM t WHERE symbol = 'AAPL'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 175.5, "delta applied");
        assert_eq!(
            res.rows[0].get("quantity").unwrap(),
            100,
            "column absent from the delta keeps its pre-delta value"
        );
    }

    /// A delta replayed for a key with no prior row creates the row,
    /// nulling every column the delta doesn't carry (same semantics as a
    /// live `delta_upsert_map` on an unknown key).
    #[test]
    fn sparse_delta_replay_for_unknown_key_creates_row_with_nulls() {
        let topic = make_topic();
        let mut delta = serde_json::Map::new();
        delta.insert("symbol".into(), "TSLA".into());
        delta.insert("price".into(), 800.0.into());
        topic.replay_delta_map_origin("", 1, &delta);
        assert_eq!(topic.row_count(), 1);
        let res = topic.query("SELECT * FROM t WHERE symbol = 'TSLA'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 800.0);
        assert!(
            res.rows[0]
                .get("quantity")
                .map(|v| v.is_null())
                .unwrap_or(true),
            "column never published is null (or omitted from the result row)"
        );
        // Dedup gate: replaying the same sequence again is a no-op.
        let mut delta2 = serde_json::Map::new();
        delta2.insert("symbol".into(), "TSLA".into());
        delta2.insert("price".into(), 999.0.into());
        topic.replay_delta_map_origin("", 1, &delta2);
        let res = topic.query("SELECT * FROM t WHERE symbol = 'TSLA'").unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 800.0, "duplicate seq ignored");
    }

    /// `delta_upsert_map` must reject a delta that omits the topic's
    /// configured key field(s) instead of silently falling back to key
    /// `""` and merging every keyless delta into one phantom row — AMPS
    /// rejects these outright.
    #[test]
    fn delta_upsert_without_key_fields_is_rejected() {
        let topic = make_topic(); // keyed on "symbol"
        let mut delta = serde_json::Map::new();
        delta.insert("price".into(), 42.0.into()); // no "symbol"
        let err = topic
            .delta_upsert_map(&delta)
            .expect_err("keyless delta must be rejected");
        assert!(matches!(err, TopicError::MissingKeyFields { .. }));
        assert_eq!(topic.row_count(), 0, "no phantom row created");
    }

    /// Replaying a delta that's missing the configured key field(s) must
    /// not panic and must not create a phantom `""`-keyed row. Recovery
    /// is lenient by design (never abort a replay over one bad entry) —
    /// the bad entry is skipped with a warning instead.
    #[test]
    fn sparse_delta_replay_without_key_fields_is_skipped_not_phantom() {
        let topic = make_topic(); // keyed on "symbol"
        let mut delta = serde_json::Map::new();
        delta.insert("price".into(), 42.0.into()); // no "symbol"
        topic.replay_delta_map_origin("", 1, &delta);
        assert_eq!(topic.row_count(), 0, "no phantom row created on replay");
    }

    /// The `snapshot_reclaim` contract: after a durable snapshot, sealed
    /// segments may be deleted and a crash-recovery (snapshot load + tail
    /// replay over the *surviving* segments) still rebuilds full state.
    #[test]
    fn snapshot_then_reclaim_prunes_sealed_segments_and_recovery_survives() {
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::snapshot::Snapshot;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        // Tiny segments so the pre-snapshot history spans several files.
        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, 512).unwrap(),
        ));
        let mut topic = make_topic();
        topic.attach_txlog(writer.clone());

        // Enough distinct keys to seal at least two segments.
        for i in 0..40 {
            publish(&topic, &format!("SYM{i:03}"), i as f64);
        }
        writer.lock().sync().unwrap();
        let sealed_before = writer.lock().current_segment();
        assert!(sealed_before > 2, "history spans several segments");

        // Durable snapshot, then reclaim everything the snapshot covers.
        assert!(topic.write_snapshot().unwrap());
        let removed = topic.reclaim_sealed_segments().unwrap();
        assert_eq!(removed as u64, sealed_before - 1, "all sealed segments deleted");

        // Post-snapshot tail: one update + one new key.
        publish(&topic, "SYM000", 999.0);
        publish(&topic, "NEWKEY", 1.0);
        writer.lock().sync().unwrap();
        drop(topic);

        // Crash recovery path: snapshot + tail replay over what's left.
        let snap = Snapshot::load(dir.path()).unwrap().expect("snapshot");
        let restarted = make_topic();
        restarted.apply_snapshot(&snap);
        assert_eq!(restarted.row_count(), 40, "snapshot restores all 40 rows");

        let mut reader = TxLogReader::open(dir.path()).unwrap();
        reader.skip_to_segment(snap.segment_id);
        while let Some(e) = reader.read_next().unwrap() {
            if e.is_tombstone() {
                restarted.replay_delete_origin(&e.origin, e.sequence, &e.key);
            } else if let Ok(serde_json::Value::Object(m)) =
                serde_json::from_slice::<serde_json::Value>(&e.payload)
            {
                restarted.replay_upsert_map_origin(&e.origin, e.sequence, &m);
            }
        }
        assert_eq!(restarted.row_count(), 41, "tail applied on top of snapshot");
        let res = restarted
            .query("SELECT * FROM t WHERE symbol = 'SYM000'")
            .unwrap();
        assert_eq!(
            res.rows[0].get("price").unwrap(),
            999.0,
            "tail update wins over snapshot value"
        );
    }

    #[test]
    fn snapshot_round_trip_rebuilds_store() {
        use cq_txlog::snapshot::Snapshot;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        {
            let writer = Arc::new(PlMutex::new(
                TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap(),
            ));
            let mut topic = make_topic();
            topic.attach_txlog(writer.clone());
            publish(&topic, "AAPL", 150.0);
            publish(&topic, "MSFT", 300.0);
            publish(&topic, "GOOGL", 2800.0);
            writer.lock().sync().unwrap();
            assert!(topic.write_snapshot().unwrap());
        }

        let snap = Snapshot::load(dir.path()).unwrap().expect("snapshot present");
        assert_eq!(snap.rows.len(), 3);
        assert_eq!(snap.sequence, 3);
        assert_eq!(snap.segment_id, 1);

        // Fresh topic rebuilt purely from the snapshot — no txlog replay.
        let restored = make_topic();
        restored.apply_snapshot(&snap);
        assert_eq!(restored.row_count(), 3);
        // Sequence numbering resumes after the snapshot's high-water.
        assert_eq!(restored.current_sequence(), 3);
        let res = restored
            .query("SELECT * FROM t WHERE symbol = 'GOOGL'")
            .unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 2800.0);
    }

    #[test]
    fn snapshot_plus_tail_replay_dedups_and_applies_tail() {
        use cq_txlog::reader::TxLogReader;
        use cq_txlog::snapshot::Snapshot;
        use cq_txlog::writer::TxLogWriter;
        use cq_txlog::FsyncPolicy;
        use parking_lot::Mutex as PlMutex;

        let dir = tempfile::tempdir().unwrap();
        let writer = Arc::new(PlMutex::new(
            TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap(),
        ));
        let mut topic = make_topic();
        topic.attach_txlog(writer.clone());

        // Pre-snapshot state: two distinct keys.
        publish(&topic, "AAPL", 150.0);
        publish(&topic, "MSFT", 300.0);
        writer.lock().sync().unwrap();
        assert!(topic.write_snapshot().unwrap());
        let snap = Snapshot::load(dir.path()).unwrap().expect("snapshot");
        assert_eq!(snap.sequence, 2);

        // Tail (written after the snapshot): update AAPL + add TSLA.
        publish(&topic, "AAPL", 175.0);
        publish(&topic, "TSLA", 800.0);
        writer.lock().sync().unwrap();
        drop(topic);

        // Simulate restart: rebuild from snapshot, then replay only the tail.
        let restarted = make_topic();
        restarted.apply_snapshot(&snap);
        assert_eq!(restarted.row_count(), 2, "snapshot alone restores 2 rows");

        let mut reader = TxLogReader::open(dir.path()).unwrap();
        reader.skip_to_segment(snap.segment_id);
        let mut frames = 0;
        while let Some(e) = reader.read_next().unwrap() {
            if e.is_tombstone() {
                restarted.replay_delete_origin(&e.origin, e.sequence, &e.key);
            } else if let Ok(serde_json::Value::Object(m)) =
                serde_json::from_slice::<serde_json::Value>(&e.payload)
            {
                restarted.replay_upsert_map_origin(&e.origin, e.sequence, &m);
            }
            frames += 1;
        }
        // The snapshot's segment is the active one, so all four frames are
        // read; the two pre-snapshot frames (seq <= 2) are deduped by the
        // baseline, the two tail frames (seq 3,4) are applied.
        assert_eq!(frames, 4);
        assert_eq!(restarted.row_count(), 3);

        // AAPL reflects the POST-snapshot update — proving the tail applied
        // AND the pre-snapshot AAPL@150 was deduped (not re-applied over it).
        let res = restarted
            .query("SELECT * FROM t WHERE symbol = 'AAPL'")
            .unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 175.0);
        let res = restarted
            .query("SELECT * FROM t WHERE symbol = 'TSLA'")
            .unwrap();
        assert_eq!(res.rows[0].get("price").unwrap(), 800.0);
    }
}

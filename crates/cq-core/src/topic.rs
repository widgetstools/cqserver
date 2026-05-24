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
use std::sync::atomic::{AtomicU64, Ordering};
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
            schema,
        }
    }
}

/// A topic manages a SOW store, key index, and subscription engine.
pub struct Topic {
    config: TopicConfig,
    state: RwLock<StoreState>,
    sub_engine: Mutex<SubscriptionEngine>,
    mutation_tx: Sender<MutationEvent>,
    mutation_rx_holder: Mutex<Option<Receiver<MutationEvent>>>,
    /// Secondary fan-out for derived consumers (S20 views). Every
    /// `MutationEvent` that the topic emits on `mutation_tx` is also
    /// `try_send`-fanned to every registered tap. Senders that fail
    /// to deliver (closed receiver, full bounded queue) are silently
    /// pruned on the next write; the regular subscription path is
    /// unaffected. Bounded senders are used so a slow view runner
    /// can't unbounded-grow memory on a hot publisher.
    view_taps: Mutex<Vec<Sender<MutationEvent>>>,
    txlog: Option<Arc<Mutex<TxLogWriter>>>,
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
    /// every time it bumps `last_replicated_sequence`; the publish
    /// path's `await_replicated` loops on the notify until the
    /// observed value reaches its target.
    replication_notify: Arc<tokio::sync::Notify>,
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
            sub_engine: Mutex::new(SubscriptionEngine::new()),
            mutation_tx,
            mutation_rx_holder: Mutex::new(Some(mutation_rx)),
            view_taps: Mutex::new(Vec::new()),
            txlog: None,
            next_sequence: AtomicU64::new(0),
            last_applied_sequence: AtomicU64::new(0),
            last_replicated_sequence: AtomicU64::new(0),
            replication_notify: Arc::new(tokio::sync::Notify::new()),
        }
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
        self.state.read().store.row_count()
    }

    pub fn subscription_count(&self) -> usize {
        self.sub_engine.lock().count()
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

        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(log) = &self.txlog {
            let mut log = log.lock();
            log.append(seq, self.name(), key, &[])?;
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
        let _ = self.mutation_tx.send(event.clone());
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
        self.mutation_rx_holder.lock().take()
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
    pub fn register_view_tap(&self, cap: usize) -> Receiver<MutationEvent> {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        self.view_taps.lock().push(tx);
        rx
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
        taps.retain(|tx| match tx.try_send(event.clone()) {
            Ok(()) => true,
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                metrics::counter!(
                    "cq_topic_view_tap_drops_total",
                    "topic" => topic_name.clone()
                )
                .increment(1);
                true
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => false,
        });
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
        if self.sub_engine.lock().count() > 0 {
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
    fn commit_values_locked(state: &mut StoreState, values: &[Value]) -> u32 {
        let key = Self::compute_key_with(values, &state.key_col_indices);
        let now = std::time::Instant::now();
        if let Some(key) = &key {
            if let Some(&existing_row) = state.key_to_row.get(key) {
                Self::reindex_row(state, existing_row, values);
                state.store.update_row(existing_row, values);
                if let Some(slot) = state.last_touched.get_mut(existing_row as usize) {
                    *slot = now;
                }
                existing_row
            } else {
                let row = state.store.append_row(values);
                state.key_to_row.insert(key.clone(), row);
                Self::index_new_row(state, row, values);
                Self::push_last_touched(state, row, now);
                row
            }
        } else {
            let row = state.store.append_row(values);
            Self::index_new_row(state, row, values);
            Self::push_last_touched(state, row, now);
            row
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
        self.write_store_with_changed(values, log_args, emit_event, None)
    }

    fn write_store_with_changed(
        &self,
        values: Vec<Value>,
        log_args: Option<(&str, &[u8])>,
        emit_event: bool,
        changed_cols: Option<Vec<usize>>,
    ) -> Result<(u32, u64), TopicError> {
        let mut state = self.state.write();
        // Sequence is allocated under state.write() so subscribers cannot
        // observe `next_sequence` advance past N without also seeing the
        // mutation for N in their snapshot. fetch_add returns the prior
        // value; we use `+ 1` so sequences are 1-based and monotonic.
        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((key, payload)) = log_args {
            if let Some(log) = &self.txlog {
                let mut log = log.lock();
                log.append(seq, self.name(), key, payload)?;
            }
        }
        let row = Self::commit_values_locked(&mut state, &values);
        if emit_event {
            let event = MutationEvent {
                row,
                sequence: seq,
                kind: MutationKind::Upsert,
                changed_cols: changed_cols.clone(),
            };
            let _ = self.mutation_tx.send(event.clone());
            self.fanout_view_tap(&event);
        }
        drop(state);
        Ok((row, seq))
    }

    /// Recovery replay path. Uses the caller-provided `sequence` (from
    /// the txlog entry) instead of allocating a new one; never writes to
    /// the txlog (the entry is already there); never emits a mutation
    /// event (evaluators aren't running yet during recovery).
    fn write_store_replay(&self, values: &[Value], _sequence: u64) -> u32 {
        let mut state = self.state.write();
        Self::commit_values_locked(&mut state, values)
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
    fn index_new_row(state: &mut StoreState, row: u32, values: &[Value]) {
        if state.secondary_index.is_empty() {
            return;
        }
        let indexed: Vec<usize> = state.secondary_index.indexed_columns().to_vec();
        for col in indexed {
            if let Some(v) = values.get(col) {
                state.secondary_index.add(col, v, row);
            }
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
        let payload = serde_json::to_vec(&serde_json::Value::Object(flat.clone()))?;

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
        let (_row, seq) = self.write_store(values, Some((&key, &payload)), true)?;
        Ok(seq)
    }

    /// Delta-publish: merge a sparse `{key + changed fields}` payload
    /// into the existing row for that key, or create a new row when
    /// the key isn't yet present. Absent fields keep their current
    /// stored value (full publishes treat absent fields as null and
    /// overwrite — the key behavioural difference).
    ///
    /// The txlog records the *fully-merged* row so a fresh replay
    /// produces the same SOW state regardless of how the rows got
    /// there (full publishes, deltas, or a mix). Saves publisher
    /// bandwidth without sacrificing recovery determinism.
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

        let key = self.compute_key_from_map(flat).unwrap_or_default();

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

        let payload_bytes =
            serde_json::to_vec(&serde_json::Value::Object(merged_payload_map))?;
        let (_row, seq) = self.write_store_with_changed(
            values,
            Some((&key, &payload_bytes)),
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

        let seq = self.next_sequence.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(log) = &self.txlog {
            let mut log = log.lock();
            log.append(seq, self.name(), key, &[])?;
        }

        let event = MutationEvent {
            row,
            sequence: seq,
            kind: MutationKind::Delete,
            changed_cols: None,
        };
        let _ = self.mutation_tx.send(event.clone());
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
        // Multi-path dedup: an entry forwarded via two routes
        // (e.g., A→B and A→C→B in an active/active topology) can
        // arrive twice. Apply once-only by gating on the topic's
        // sequence high-water.
        if sequence <= self.last_applied_sequence.load(Ordering::Acquire)
            && self.last_applied_sequence.load(Ordering::Acquire) > 0
        {
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
        self.write_store_replay(&values, sequence);
        self.bump_sequence_to(sequence);
    }

    /// Recovery-only delete. Nulls the row if present; updates the
    /// sequence high-water mark.
    pub fn replay_delete(&self, sequence: u64, key: &str) {
        if sequence <= self.last_applied_sequence.load(Ordering::Acquire)
            && self.last_applied_sequence.load(Ordering::Acquire) > 0
        {
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
            }
        }
        self.bump_sequence_to(sequence);
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
        let mut engine = self.sub_engine.lock();
        engine.evaluate_row_kind(row, sequence, &state.store, kind)
    }

    /// Index-routed variant (S46 / review C3): only evaluates
    /// subscriptions whose predicate references at least one of the
    /// columns in `changed_cols`. Used by the sparse-publish path
    /// (`delta_upsert_map`) to skip work for subscriptions that
    /// can't possibly be affected by the mutation. `None` for
    /// `changed_cols` falls through to the all-subs path, so this
    /// is a strict superset of `evaluate_row_kind`'s semantics.
    pub fn evaluate_row_kind_indexed(
        &self,
        row: u32,
        sequence: u64,
        kind: MutationKind,
        changed_cols: Option<&[usize]>,
    ) -> Vec<Delta> {
        let state = self.state.read();
        let mut engine = self.sub_engine.lock();
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
            || !query.group_by.is_empty();
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
        let mut engine = self.sub_engine.lock();
        let mut sub =
            Subscription::new(sub_id.clone(), query.clone()).with_live_start(captured + 1);
        if sparse {
            sub = sub.into_sparse(state.key_col_indices.clone());
        }
        if is_aggregate {
            // Aggregate/PIVOT subs need a seed snapshot for
            // `last_emitted`. Cardinality is bounded by GROUP BY (or by
            // distinct anchor keys for PIVOT), not by row count.
            let result = execute_query(&query, &state.store);
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
            engine.add(sub_with_agg);
        } else {
            engine.add(sub);
            engine.seed_active_set(&sub_id, &state.store);
        }
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
        let mut result = execute_query(&query, &state.store);
        // Drop tombstoned rows from the snapshot by row-index
        // lookup — same shape as `query()` / streaming. Robust to
        // projections that exclude the key column. Aggregate /
        // GROUP BY queries skip this filter (their output rows
        // don't map to a single source row).
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
        let mut engine = self.sub_engine.lock();
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
            engine.add(sub_with_agg);
        } else {
            engine.add(sub);
            engine.seed_active_set(&sub_id, &state.store);
        }
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
        let mut engine = self.sub_engine.lock();
        let sub = Subscription::new(sub_id.clone(), query.clone())
            .with_live_start(captured + 1);
        engine.add(sub);
        engine.seed_active_set(&sub_id, &state.store);
        Ok((query, captured))
    }

    /// Filesystem path of this topic's transaction log, if attached.
    /// Bookmark replay opens a fresh `TxLogReader` against this path.
    pub fn txlog_path(&self) -> Option<std::path::PathBuf> {
        self.txlog.as_ref().map(|w| w.lock().path().to_path_buf())
    }

    pub fn unsubscribe(&self, sub_id: &str) {
        let mut engine = self.sub_engine.lock();
        // Flip the closed flag BEFORE removing the entry. Any evaluator
        // pass that already captured a reference to this sub from the
        // engine (we hold the lock so this can't be in progress, but
        // belt + braces — keeps the close-then-remove ordering
        // explicit) sees the flag and bails out of work for it.
        engine.mark_closed(sub_id);
        engine.remove(sub_id);
    }

    pub fn unsubscribe_prefix(&self, prefix: &str) {
        self.sub_engine.lock().remove_by_prefix(prefix);
    }

    /// Mark a subscription as closed without removing the engine
    /// entry. Cheap; safe to call from a disconnect handler that may
    /// race with the evaluator. The next event evaluator pass skips
    /// the sub, and the next [`reap_closed_subscriptions`] call
    /// drops it. See S38 / review C8.
    pub fn close_subscription(&self, sub_id: &str) -> bool {
        self.sub_engine.lock().mark_closed(sub_id)
    }

    /// Drop every subscription whose `closed` flag is set. Intended to
    /// be invoked periodically (e.g., once per second from a per-topic
    /// reaper task) and after large disconnect storms.
    pub fn reap_closed_subscriptions(&self) -> usize {
        self.sub_engine.lock().reap_closed()
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
        let mut engine = self.sub_engine.lock();
        let sub = Subscription::new(sub_id.clone(), query.clone())
            .with_live_start(captured + 1)
            .with_max_active(max_active);
        engine.add(sub);
        engine.seed_active_set(&sub_id, &state.store);
        Ok((result.rows, query))
    }

    /// Read the active-set size of a subscription. Returns `None` if
    /// the subscription doesn't exist (or has been reaped). Intended
    /// for tests and memory-bound assertions.
    pub fn subscription_active_set_size(&self, sub_id: &str) -> Option<u64> {
        self.sub_engine
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
        self.sub_engine
            .lock()
            .get(sub_id)
            .and_then(|s| s.close_reason())
    }

    /// Read the closed-ness of a subscription. Useful for tests that
    /// publish, then observe whether the cap fired.
    pub fn subscription_is_closed(&self, sub_id: &str) -> Option<bool> {
        self.sub_engine.lock().get(sub_id).map(|s| s.is_closed())
    }

    // ==================== Stats ====================

    pub fn stats(&self) -> serde_json::Value {
        let state = self.state.read();
        let engine = self.sub_engine.lock();
        serde_json::json!({
            "name": self.config.name,
            "rowCount": state.store.row_count(),
            "columnCount": state.schema.column_count(),
            "keyFields": self.config.key_fields,
            "subscriptions": engine.count(),
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

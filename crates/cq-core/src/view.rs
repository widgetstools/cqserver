//! S20 — materialized views.
//!
//! A view is a derived topic whose contents are kept in sync with a
//! continuous aggregate query against a source topic. Architecturally:
//!
//!   - `View::new(source, view_topic, query, group_by_names)` snapshots
//!     the source SOW (via `execute_parsed_query`) and seeds the view
//!     topic with one row per group.
//!   - A per-view runner thread waits on a "view tap" — a bounded
//!     side-channel that receives every `MutationEvent` the source
//!     emits. On each event it re-runs the aggregate, diffs against
//!     `last_emitted`, and applies one `upsert_map` per added/changed
//!     group + one `delete` per vanished group.
//!   - The view topic's own subscription engine emits row-level deltas
//!     to view subscribers as usual — view subscribers see the view as
//!     just another topic, with row-level Add/Update/Remove semantics
//!     instead of group-level diffs.
//!
//! ### Schema derivation
//!
//!   - Each `group_by` column inherits its type from the source schema.
//!   - Aggregate output columns map by function + input type:
//!     * `COUNT(*)` / `COUNT(col)` → `Long`
//!     * `SUM(int|long)` → `Long`
//!     * `SUM(double|float)` → `Double`
//!     * `AVG(_)` → `Double`
//!     * `MIN/MAX(col)` → same type as `col`
//!
//! The view's key fields default to the `group_by` column names so each
//! group occupies its own row in the view SOW.
//!
//! ### Performance shape
//!
//! Re-aggregation is lazy and O(source_rows) per source event. Acceptable
//! for low-to-medium cardinality dashboards; truly incremental view
//! maintenance (per-group running state, applied delta-by-delta) is a
//! follow-up optimization that mirrors the S19 follow-up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use parking_lot::Mutex;

/// Idle gap with no new tap events that triggers a view refresh. During
/// a continuous bulk seed events never pause until the end, so the
/// runner refreshes ~once after the burst instead of per insert.
const QUIET_WINDOW: Duration = Duration::from_millis(75);
/// Upper bound on how stale a view may get under *unbroken* load: at
/// most one full refresh per this interval per view.
const MAX_REFRESH_DELAY: Duration = Duration::from_secs(1);

use crate::query::{
    aggregate_one_group, build_group_membership, combined_join_schema,
    group_key_canonical_from_store, parse_query, peek_join, AggFn, ParsedQuery, QueryError,
};
use crate::schema::{ColumnType, Schema};
use crate::subscription::group_key_canonical;
use crate::topic::{MutationEvent, MutationKind, SharedTopic, Topic, TopicConfig, TopicError};
use roaring::RoaringBitmap;

/// One materialized view. Wraps the source topic, the derived
/// view topic, and the canonical "last emitted" map keyed by group.
pub struct View {
    pub view_topic: SharedTopic,
    pub source_topic: SharedTopic,
    /// S20 — optional right-side topic when the view's SQL uses
    /// `... FROM A JOIN B USING (col)`. `None` for the common
    /// single-source view.
    pub right_topic: Option<SharedTopic>,
    pub query: ParsedQuery,
    /// `group_by` column names, in declaration order. Used to canonicalize
    /// each output row's group identity (passed to `group_key_canonical`).
    pub group_by_names: Vec<String>,
    /// Names of the columns that form the view's primary key. Mirrors
    /// `group_by_names` today, stored separately so we don't conflate
    /// "row identity in the view SOW" with "group identity in the source".
    pub view_key_names: Vec<String>,
    state: Mutex<ViewState>,
    /// Count of completed refreshes. Observability + lets tests assert
    /// the debounce coalesced a burst into few refreshes.
    refresh_count: AtomicU64,
}

#[derive(Default)]
struct ViewState {
    /// Canonical group key → last-emitted row map.
    last_emitted: HashMap<String, serde_json::Map<String, serde_json::Value>>,
    /// S22-incremental — per-group set of source row indices currently
    /// matching the view's predicate. Lets one mutation recompute only
    /// the group(s) it touches. Seeded from a full scan on the first
    /// incremental pass (and re-seeded after any reconciliation).
    group_rows: HashMap<String, RoaringBitmap>,
    /// S22-incremental — reverse map: source row index → its current
    /// group key. Used to move a mutated row between groups.
    row_group: HashMap<u32, String>,
    /// S22-incremental — false until membership maps have been seeded.
    /// Reset to false whenever a dropped tap event forces a full
    /// reconciliation, so the next incremental pass re-seeds first.
    incremental_ready: bool,
    /// S22-incremental — highest source sequence already reflected in
    /// `last_emitted` / membership. Tap events with `sequence <= seed_seq`
    /// were captured by the seed snapshot and must be skipped to avoid
    /// double-counting (the tap is registered before the seed read, so a
    /// publish can land in both).
    seed_seq: u64,
    /// Last `Topic::view_tap_drops` value the runner observed. An
    /// increase means events were missed → reconcile instead of trust.
    last_seen_drops: u64,
}

/// Errors that can surface during view construction or refresh.
#[derive(Debug, thiserror::Error)]
pub enum ViewError {
    #[error("query: {0}")]
    Query(#[from] QueryError),
    #[error("topic: {0}")]
    Topic(#[from] TopicError),
    #[error("view {view}: query must be an aggregate (SELECT ... GROUP BY ...)")]
    NotAggregate { view: String },
    #[error("view {view}: SQL has a JOIN; use build_view_topic_joined")]
    JoinWithoutRightTopic { view: String },
    #[error("view {view}: expected a JOIN clause in the SQL but found none")]
    ExpectedJoin { view: String },
    #[error("view {view}: right-side JOIN topic `{right}` not found in the registry")]
    RightTopicNotFound { view: String, right: String },
}

impl View {
    /// Build the view topic's schema from the *effective query
    /// schema* (single-source schema for non-join views; the
    /// combined `left ∪ right` schema for JOIN views) + parsed
    /// aggregate query. Group-by columns and aggregate input
    /// columns are resolved against this schema, so the derived
    /// output schema preserves their declared types even when they
    /// originate on the right side of a join.
    pub fn derive_view_schema(
        effective_schema: &Schema,
        query: &ParsedQuery,
    ) -> Schema {
        let mut names: Vec<String> = Vec::new();
        let mut types: Vec<ColumnType> = Vec::new();
        for &gi in &query.group_by {
            names.push(effective_schema.column_name(gi).to_string());
            types.push(effective_schema.column_type(gi));
        }
        for agg in &query.aggregates {
            names.push(agg.alias.clone());
            let ty = match (agg.func, agg.col) {
                (AggFn::Count, _) => ColumnType::Long,
                (AggFn::Sum, Some(c)) => match effective_schema.column_type(c) {
                    ColumnType::Double => ColumnType::Double,
                    _ => ColumnType::Long,
                },
                (AggFn::Avg, _) => ColumnType::Double,
                (AggFn::Min, Some(c)) | (AggFn::Max, Some(c)) => {
                    effective_schema.column_type(c)
                }
                // No-column MIN/MAX is rejected at parse time; the
                // SUM(None) branch is similarly defensive.
                (AggFn::Min, None) | (AggFn::Max, None) | (AggFn::Sum, None) => ColumnType::Long,
                // P8 — STDDEV / VARIANCE always return Double.
                (AggFn::Stddev, _)
                | (AggFn::StddevSamp, _)
                | (AggFn::Variance, _)
                | (AggFn::VarianceSamp, _) => ColumnType::Double,
                // P9 — PERCENTILE_CONT / MEDIAN always return Double.
                (AggFn::PercentileCont, _) => ColumnType::Double,
                // P10 — COUNT(DISTINCT col) returns Long.
                (AggFn::CountDistinct, _) => ColumnType::Long,
            };
            types.push(ty);
        }
        // Scalar-over-aggregate projections (`post_agg`), e.g.
        // `SUM(x) / NULLIF(SUM(y), 0) AS ratio`. `PostAggExpr::eval`
        // performs f64 arithmetic (or soft-nulls on div-by-zero /
        // missing refs / type errors), so the output column is always
        // Double — matching how AVG/STDDEV/PERCENTILE aggregate aliases
        // are typed above. Without this, the view topic's schema lacks
        // these columns and `upsert_map` silently drops them from every
        // stored row (full-refresh AND incremental).
        for pa in &query.post_agg {
            names.push(pa.alias.clone());
            types.push(ColumnType::Double);
        }
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        Schema::from_strs(&name_refs, &types)
    }

    /// Parse the view's SQL against the source schema and prepare a
    /// brand-new view `Topic`. The single-source variant — for
    /// JOIN-bearing SQL use `build_view_topic_joined`.
    pub fn build_view_topic(
        source: &Topic,
        sql: &str,
        view_name: String,
        capacity: usize,
    ) -> Result<(Topic, ParsedQuery, Vec<String>), ViewError> {
        let source_schema = source.schema();
        let query = parse_query(sql, &source_schema)?;
        if query.join.is_some() {
            return Err(ViewError::JoinWithoutRightTopic { view: view_name });
        }
        if query.group_by.is_empty() && query.aggregates.is_empty() {
            return Err(ViewError::NotAggregate { view: view_name });
        }
        let group_by_names: Vec<String> = query
            .group_by
            .iter()
            .map(|&i| source_schema.column_name(i).to_string())
            .collect();
        let view_schema = Self::derive_view_schema(&source_schema, &query);
        let view_topic = Topic::new(
            TopicConfig {
                name: view_name,
                key_fields: group_by_names.clone(),
                persist: false,
                conflation_ms: None,
                index_columns: Vec::new(),
                expire_seconds: None,
            },
            Arc::new(view_schema),
            capacity,
        );
        Ok((view_topic, query, group_by_names))
    }

    /// S20 — build a JOIN view topic. Resolves the right-side topic
    /// from the SQL via `peek_join`, looks up its schema, builds the
    /// combined `left ∪ right` schema, parses the query against
    /// THAT, and constructs the view topic. Callers (server
    /// startup) typically pass a `resolve_right` closure that hits
    /// the global topic registry; for tests, pass a closure that
    /// returns the in-process right-side `&Topic`.
    pub fn build_view_topic_joined(
        source: &Topic,
        right_resolver: impl FnOnce(&str) -> Option<SharedTopic>,
        sql: &str,
        view_name: String,
        capacity: usize,
    ) -> Result<(Topic, ParsedQuery, Vec<String>, SharedTopic), ViewError> {
        let (_, right_topic_name, using) = peek_join(sql)?.ok_or_else(|| {
            ViewError::ExpectedJoin {
                view: view_name.clone(),
            }
        })?;
        let right_topic = right_resolver(&right_topic_name).ok_or_else(|| {
            ViewError::RightTopicNotFound {
                view: view_name.clone(),
                right: right_topic_name.clone(),
            }
        })?;
        let left_schema = source.schema();
        let right_schema = right_topic.schema();
        let combined = Arc::new(combined_join_schema(&left_schema, &right_schema, &using));
        let query = parse_query(sql, &combined)?;
        if query.join.is_none() {
            return Err(ViewError::ExpectedJoin { view: view_name });
        }
        if query.group_by.is_empty() && query.aggregates.is_empty() {
            return Err(ViewError::NotAggregate { view: view_name });
        }
        let group_by_names: Vec<String> = query
            .group_by
            .iter()
            .map(|&i| combined.column_name(i).to_string())
            .collect();
        let view_schema = Self::derive_view_schema(&combined, &query);
        let view_topic = Topic::new(
            TopicConfig {
                name: view_name,
                key_fields: group_by_names.clone(),
                persist: false,
                conflation_ms: None,
                index_columns: Vec::new(),
                expire_seconds: None,
            },
            Arc::new(view_schema),
            capacity,
        );
        Ok((view_topic, query, group_by_names, right_topic))
    }

    /// Wire up the view runner against an already-registered view
    /// topic. Performs the initial population pass so the view SOW
    /// reflects the source's current contents before this returns.
    /// For JOIN views, pass the right-side topic; the refresh path
    /// then routes through `execute_join_query` against both stores.
    pub fn new(
        source_topic: SharedTopic,
        view_topic: SharedTopic,
        query: ParsedQuery,
        group_by_names: Vec<String>,
        right_topic: Option<SharedTopic>,
    ) -> Result<Arc<Self>, ViewError> {
        let view_key_names = group_by_names.clone();
        let view = Arc::new(View {
            view_topic,
            source_topic,
            right_topic,
            query,
            group_by_names,
            view_key_names,
            state: Mutex::new(ViewState::default()),
            refresh_count: AtomicU64::new(0),
        });
        view.refresh()?;
        // Eagerly seed incremental membership from the same source state
        // the initial refresh observed, so the first mutation has a
        // correct "old group" to move rows out of (a lazy seed taken
        // after the event already applied would miss vanished groups).
        if view.supports_incremental() {
            view.seed_membership();
        }
        Ok(view)
    }

    /// Re-execute the view query against the source store (and the
    /// right-side topic if a JOIN is configured), diff against the
    /// last-emitted snapshot, and apply per-group upsert/delete to
    /// the view topic.
    pub fn refresh(&self) -> Result<(), ViewError> {
        let result = match (&self.query.join, &self.right_topic) {
            (Some(_), Some(right)) => self.source_topic.execute_join_query(
                &self.query,
                right.as_ref(),
            )?,
            (None, _) => self.source_topic.execute_parsed_query(&self.query),
            (Some(_), None) => {
                return Err(ViewError::JoinWithoutRightTopic {
                    view: self.view_topic.name().to_string(),
                });
            }
        };

        let mut state = self.state.lock();
        let mut next: HashMap<String, serde_json::Map<String, serde_json::Value>> =
            HashMap::with_capacity(result.rows.len());
        for row in result.rows {
            let k = group_key_canonical(&row, &self.group_by_names);
            next.insert(k, row);
        }

        let mut to_upsert: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
        let mut to_delete: Vec<String> = Vec::new();
        for (k, row) in &next {
            match state.last_emitted.get(k) {
                Some(prev) if prev == row => {}
                _ => to_upsert.push(row.clone()),
            }
        }
        for (k, row) in &state.last_emitted {
            if !next.contains_key(k) {
                if let Some(key) = self.compose_view_key(row) {
                    to_delete.push(key);
                }
            }
        }
        state.last_emitted = next;
        drop(state);

        for row in to_upsert {
            self.view_topic.upsert_map(&row)?;
        }
        for key in to_delete {
            self.view_topic.delete(&key)?;
        }
        metrics::counter!(
            "cq_view_refresh_total",
            "view" => self.view_topic.name().to_string()
        )
        .increment(1);
        self.refresh_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Number of completed *full* refreshes since construction (includes
    /// the initial population in `View::new` and any drop-triggered
    /// reconciliation). Incremental per-event updates do not bump this —
    /// they're the cheap steady-state path.
    pub fn refresh_count(&self) -> u64 {
        self.refresh_count.load(Ordering::Relaxed)
    }

    /// S22 — whether this view can be maintained incrementally (recompute
    /// only the group a mutation touches) rather than by full re-scan.
    /// Mirrors the subscription engine's `aggregate_needs_full_recompute`
    /// classification: JOIN / PIVOT / LIMIT / OFFSET / window / implicit
    /// single-group shapes always full-recompute.
    pub fn supports_incremental(&self) -> bool {
        self.right_topic.is_none()
            && self.query.join.is_none()
            && self.query.pivot.is_none()
            && self.query.unpivot.is_none()
            && self.query.limit.is_none()
            && self.query.offset.is_none()
            && self.query.windows.is_empty()
            && !self.query.group_by.is_empty()
    }

    /// S22 — (re)seed incremental membership from the current source
    /// snapshot and record the applied sequence watermark. Called once
    /// in `View::new` and again by the runner after a full-refresh
    /// reconciliation (tap drops may have desynced the maps). Assumes
    /// `last_emitted` already matches this snapshot (the preceding
    /// `refresh()` ensures that).
    fn seed_membership(&self) {
        let mut state = self.state.lock();
        self.source_topic.with_live_store(|store, live, applied_seq| {
            let (gr, rg) = build_group_membership(&self.query, store, Some(live));
            state.group_rows = gr;
            state.row_group = rg;
            state.seed_seq = applied_seq;
            state.incremental_ready = true;
        });
    }

    /// S22 — apply a single source mutation to the view incrementally.
    /// Updates per-group membership for the mutated row, recomputes only
    /// the affected group(s), and applies one upsert/delete per changed
    /// group to the view topic. Membership is seeded lazily on the first
    /// call (and after any reconciliation). Only valid when
    /// `supports_incremental()` — the runner gates on that.
    fn apply_event_incremental(&self, ev: &MutationEvent) -> Result<(), ViewError> {
        let group_cols = self.query.group_by.clone();
        let mut state = self.state.lock();

        let (to_upsert, to_delete): (
            Vec<serde_json::Map<String, serde_json::Value>>,
            Vec<String>,
        ) = self.source_topic.with_live_store(|store, live, _applied_seq| {
            // Skip events already folded into the seed snapshot (the tap
            // is registered before the seed read, so a publish can appear
            // both in the seed and in the tap queue).
            if ev.sequence <= state.seed_seq {
                return (Vec::new(), Vec::new());
            }
            let row = ev.row;
            // A delete (or a stale upsert whose row was since tombstoned —
            // the tap lags the writer) is no longer a live member. Gating
            // on `live` keeps membership consistent with the full-scan
            // path (`build_group_membership` filters by live rows too) and
            // avoids fabricating a phantom group from a nulled row under a
            // `True` predicate.
            let matches = ev.kind != MutationKind::Delete
                && live.contains(row)
                && self.query.predicate.matches(store, row);
            let new_key = if matches {
                Some(group_key_canonical_from_store(store, row, &group_cols))
            } else {
                None
            };
            let old_key = state.row_group.get(&row).cloned();
            if old_key.is_none() && new_key.is_none() {
                return (Vec::new(), Vec::new());
            }

            // Phase 1 — update membership; collect dirty group keys.
            let mut dirty: Vec<String> = Vec::with_capacity(2);
            match (&old_key, &new_key) {
                (Some(ok), Some(nk)) if ok == nk => dirty.push(nk.clone()),
                _ => {
                    if let Some(ok) = &old_key {
                        if let Some(bm) = state.group_rows.get_mut(ok) {
                            bm.remove(row);
                            if bm.is_empty() {
                                state.group_rows.remove(ok);
                            }
                        }
                        dirty.push(ok.clone());
                    }
                    if let Some(nk) = &new_key {
                        state.group_rows.entry(nk.clone()).or_default().insert(row);
                        dirty.push(nk.clone());
                    }
                }
            }
            match &new_key {
                Some(nk) => {
                    state.row_group.insert(row, nk.clone());
                }
                None => {
                    state.row_group.remove(&row);
                }
            }

            // Phase 2 — recompute each dirty group, diff against last_emitted.
            let mut to_upsert = Vec::new();
            let mut to_delete = Vec::new();
            for key in &dirty {
                let recomputed = match state.group_rows.get(key) {
                    Some(bm) if !bm.is_empty() => aggregate_one_group(&self.query, store, bm),
                    _ => None,
                };
                match recomputed {
                    Some(new_row) => match state.last_emitted.get(key) {
                        Some(prev) if prev == &new_row => {}
                        _ => {
                            state.last_emitted.insert(key.clone(), new_row.clone());
                            to_upsert.push(new_row);
                        }
                    },
                    None => {
                        if let Some(prev) = state.last_emitted.remove(key) {
                            if let Some(k) = self.compose_view_key(&prev) {
                                to_delete.push(k);
                            }
                        }
                    }
                }
            }
            (to_upsert, to_delete)
        });

        drop(state);
        for row in to_upsert {
            self.view_topic.upsert_map(&row)?;
        }
        for key in to_delete {
            self.view_topic.delete(&key)?;
        }
        Ok(())
    }

    /// Compose the view's primary-key string from a row map, using the
    /// view's key columns. Mirrors `Topic::compute_key_from_map`'s
    /// concatenation semantics so `view_topic.delete(&k)` finds the
    /// row `upsert_map` last wrote.
    fn compose_view_key(
        &self,
        row: &serde_json::Map<String, serde_json::Value>,
    ) -> Option<String> {
        let mut parts: Vec<String> = Vec::with_capacity(self.view_key_names.len());
        for name in &self.view_key_names {
            let v = row.get(name)?;
            let part = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => return None,
                _ => return None,
            };
            parts.push(part);
        }
        Some(parts.join("|"))
    }
}

/// Spawn the per-view runner thread. The thread waits on `tap_rx` —
/// the bounded side-channel that the source topic fans every mutation
/// event to — and calls `view.refresh()` on each event. A drained
/// `tap_rx` (source topic dropped) cleanly exits the loop.
///
/// The runner coalesces queued events between refreshes: when several
/// publishes land before the runner wakes up, it sees only one
/// effective refresh — since `refresh()` reads the *current* source
/// state, redundant per-event passes would just write the same
/// answer to the view multiple times.
/// Bursts larger than this take the full-recompute path even when the
/// view is incremental-capable: at that point one O(rows) scan is
/// cheaper than N per-group recomputes, and it also re-seeds membership.
const INCREMENTAL_BATCH_CAP: usize = 256;

pub fn spawn_view_runner(
    view: Arc<View>,
    tap_rx: Receiver<MutationEvent>,
) -> std::io::Result<JoinHandle<()>> {
    let view_name = view.view_topic.name().to_string();
    std::thread::Builder::new()
        .name(format!("view:{}", view_name))
        .spawn(move || {
            let incremental = view.supports_incremental();
            tracing::info!(view = %view_name, incremental, "View runner started");
            // Debounce: block for the first event, then absorb the burst
            // (up to QUIET_WINDOW of silence or MAX_REFRESH_DELAY since
            // the first event). Then either replay the burst
            // incrementally (cheap steady state) or, for large bursts /
            // non-incremental shapes / dropped tap events, do one full
            // re-scan that also re-seeds incremental membership.
            'outer: loop {
                let first = match tap_rx.recv() {
                    Ok(ev) => ev,
                    Err(_) => break, // all tap senders dropped → exit
                };
                let mut batch: Vec<MutationEvent> = vec![first];
                let deadline = Instant::now() + MAX_REFRESH_DELAY;
                let disconnected = loop {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break false; // hit the staleness cap under unbroken load
                    }
                    match tap_rx.recv_timeout(QUIET_WINDOW.min(remaining)) {
                        Ok(ev) => batch.push(ev),
                        Err(RecvTimeoutError::Timeout) => break false,
                        Err(RecvTimeoutError::Disconnected) => break true,
                    }
                };

                run_view_batch(&view, incremental, &batch, &view_name);

                if disconnected {
                    break 'outer;
                }
            }
            tracing::info!(view = %view_name, "View runner exiting");
        })
}

/// Process one absorbed burst: incremental replay when safe, otherwise a
/// full re-scan that also invalidates (re-seeds) incremental membership.
fn run_view_batch(view: &Arc<View>, incremental: bool, batch: &[MutationEvent], view_name: &str) {
    // Detect dropped tap events since the last burst: if any were
    // dropped, our incremental membership may have missed a mutation, so
    // we must reconcile with a full scan rather than trust it.
    let drops_now = view.source_topic.view_tap_drops();
    let dropped = {
        let mut state = view.state.lock();
        let d = drops_now != state.last_seen_drops;
        state.last_seen_drops = drops_now;
        d
    };

    let use_incremental =
        incremental && !dropped && batch.len() <= INCREMENTAL_BATCH_CAP;

    if use_incremental {
        for ev in batch {
            if let Err(e) = view.apply_event_incremental(ev) {
                tracing::warn!(view = %view_name, error = %e, "View incremental update failed");
            }
        }
    } else {
        if let Err(e) = view.refresh() {
            tracing::warn!(view = %view_name, error = %e, "View refresh failed");
        }
        // The full scan rebuilt `last_emitted`; re-seed membership from
        // the same snapshot so the incremental path can resume.
        if incremental {
            view.seed_membership();
        }
    }
}

/// S20 — for JOIN views, the runner needs to wake on EITHER source
/// or right-side mutations. We merge two `crossbeam_channel`
/// `Receiver`s onto a single internal channel and hand the merged
/// receiver to `spawn_view_runner`. Both tap channels are bounded
/// (the source registers via `Topic::register_view_tap`); a
/// background select-thread forwards events from each tap to the
/// merged channel.
pub fn spawn_view_runner_joined(
    view: Arc<View>,
    left_tap: Receiver<MutationEvent>,
    right_tap: Receiver<MutationEvent>,
) -> std::io::Result<JoinHandle<()>> {
    let (merged_tx, merged_rx) = crossbeam_channel::bounded::<MutationEvent>(1024);
    // Forward thread: select on either tap; exit when both are closed.
    let view_name = view.view_topic.name().to_string();
    std::thread::Builder::new()
        .name(format!("view-fanin:{}", view_name))
        .spawn(move || loop {
            crossbeam_channel::select! {
                recv(left_tap) -> msg => match msg {
                    Ok(ev) => { let _ = merged_tx.send(ev); }
                    Err(_) => return,
                },
                recv(right_tap) -> msg => match msg {
                    Ok(ev) => { let _ = merged_tx.send(ev); }
                    Err(_) => return,
                },
            }
        })?;
    spawn_view_runner(view, merged_rx)
}

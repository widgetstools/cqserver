//! Continuous query subscription engine.
//!
//! Each subscription tracks an "active set" of row indices matching its
//! predicate. On every mutation, the engine evaluates the predicate against
//! the mutated row and computes a delta (ADD / UPDATE / REMOVE).

use crate::query::{ParsedQuery, project_row};
use crate::store::{ColumnStore, Value};
use roaring::RoaringBitmap;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Delta type for subscription updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaType {
    /// Record now matches the subscription (new or changed into match).
    Add,
    /// Record still matches but field values changed.
    Update,
    /// Record no longer matches (deleted or predicate no longer applies).
    Remove,
    /// Record fell out of a TOP N result set (out of focus).
    Oof,
}

/// A single delta event to be delivered to a subscriber.
#[derive(Debug, Clone)]
pub struct Delta {
    /// `Arc<str>` so the per-delta clone on the evaluator hot path is a
    /// refcount bump rather than a heap allocation (the same id is cloned
    /// for every delta a subscription emits).
    pub subscription_id: std::sync::Arc<str>,
    pub delta_type: DeltaType,
    /// Row index in the source `ColumnStore`. Used as the coalescing key
    /// when conflation is enabled — two deltas with the same `row` refer
    /// to the same logical record.
    pub row: u32,
    /// Monotonic per-topic sequence assigned at the moment of the source
    /// write. Forwarded as `seq` in the delivered `CqMessage` so clients
    /// can bookmark and replay on reconnect.
    pub sequence: u64,
    /// The projected row payload. Wrapped in `Arc` so the evaluator can
    /// hand the *same* projection result to every subscriber whose
    /// projection produces identical output — overwhelmingly the common
    /// case (no explicit projection means "all columns"). Delivery uses
    /// `Arc::ptr_eq` to dedup encode work: each unique payload is
    /// serialized once per evaluator pass and fanned out to every sub
    /// holding the same Arc.
    pub row_data: std::sync::Arc<serde_json::Map<String, serde_json::Value>>,
    /// Pre-encoded JSON bytes of `row_data`. Optional — populated by the
    /// transport's evaluator pass when a sub's outbound codec is JSON,
    /// so both the direct-send and conflator-flush paths can stitch a
    /// per-sub envelope around the same body without re-serializing.
    /// `None` means "encode at delivery time" (e.g., MessagePack subs,
    /// or unit tests that build Deltas directly).
    pub encoded_body_json: Option<std::sync::Arc<Vec<u8>>>,
}

/// Total-order comparator across `Value` variants, used by `SortKey` to
/// rank rows for `ORDER BY` + `LIMIT` subscriptions. `f64` uses
/// `total_cmp` so NaN doesn't break the order. `Null` sorts before any
/// non-null value of the same column.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Double(x), Double(y)) => x.total_cmp(y),
        (Long(x), Long(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (String(x), String(y)) => x.cmp(y),
        // Mixed-type compare shouldn't happen in practice (a column has one
        // type) — fall through to Equal so neither row dominates.
        _ => Ordering::Equal,
    }
}

/// One ORDER BY column's value + direction.
#[derive(Debug, Clone)]
struct SortElement {
    value: Value,
    asc: bool,
}

impl PartialEq for SortElement {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for SortElement {}
impl PartialOrd for SortElement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SortElement {
    fn cmp(&self, other: &Self) -> Ordering {
        let c = compare_values(&self.value, &other.value);
        if self.asc {
            c
        } else {
            c.reverse()
        }
    }
}

/// Lexicographic key over the ORDER BY columns. Equal keys can still be
/// distinguished by their row index in the `BTreeSet<(SortKey, u32)>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey(Vec<SortElement>);

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> Ordering {
        for (a, b) in self.0.iter().zip(&other.0) {
            let c = a.cmp(b);
            if c != Ordering::Equal {
                return c;
            }
        }
        Ordering::Equal
    }
}

fn build_sort_key(store: &ColumnStore, row: u32, order_by: &[(usize, bool)]) -> SortKey {
    let elements = order_by
        .iter()
        .map(|&(col, asc)| SortElement {
            value: store.get(col, row),
            asc,
        })
        .collect();
    SortKey(elements)
}

/// State maintained per TOP-N subscription. The `ranked` set holds every
/// matching row sorted by the query's ORDER BY clauses; the top `limit`
/// entries are the visible result. `current_topn` caches the set of
/// row indices in the visible window so we can diff entrants vs. evictions
/// after each mutation.
#[derive(Default)]
pub struct TopNState {
    ranked: BTreeSet<(SortKey, u32)>,
    row_to_key: HashMap<u32, SortKey>,
    current_topn: HashSet<u32>,
}

/// A registered subscription with its query and active set.
pub struct Subscription {
    /// `Arc<str>` so cloning the id into each emitted `Delta` is a refcount
    /// bump, not an allocation. Cold paths that need an owned `String`
    /// (registry keys) convert at the boundary.
    pub id: std::sync::Arc<str>,
    pub query: ParsedQuery,
    pub active_set: RoaringBitmap,
    /// When `true`, Update deltas carry only fields whose value changed
    /// since the last emission for that row, plus the topic key columns
    /// so consumers can correlate. Add still emits the full projection;
    /// Remove sends key-only.
    pub sparse: bool,
    /// Topic key column indices, included in every sparse delta payload.
    pub key_cols: Vec<usize>,
    /// Last emitted values per row (schema-column-indexed). Only populated
    /// when `sparse` is true.
    pub last_snapshot: HashMap<u32, HashMap<usize, Value>>,
    /// TOP-N tracking state. `Some` iff the query has a positive `LIMIT`.
    pub topn: Option<TopNState>,
    /// Live deltas with sequence < this value are suppressed. Used by the
    /// bookmark-replay path: when a subscriber catches up via the txlog,
    /// the live path must not duplicate the historical events that the
    /// replay already covered.
    pub live_start_sequence: u64,
    /// Cancellation flag — set when the subscriber disconnects or
    /// explicitly unsubscribes. Once set, the evaluator skips this
    /// sub on every subsequent event and the next `reap_closed` pass
    /// drops it from the engine. `Arc<AtomicBool>` so the close
    /// signal can be raised from outside the engine lock (e.g., from
    /// the transport layer's disconnect handler) without contending
    /// for the engine mutex. See S38 / review concern C8.
    pub closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Optional cap on the active-set size. `None` means unbounded
    /// (current default). When set, the evaluator marks the sub
    /// closed (reason `TooManyMatches`) the moment an Add would
    /// push the active set past the cap. The client gets the
    /// reason via `close_reason` and can either narrow its filter
    /// or accept the bound. See S42 / review concern C11.
    pub max_active: Option<u32>,
    /// Reason the sub was closed, if known. Populated by enforcement
    /// paths (today: `TooManyMatches`). Read by the transport layer
    /// when emitting the terminate-subscription frame.
    pub close_reason: std::sync::Arc<parking_lot::Mutex<Option<CloseReason>>>,
    /// Continuous-aggregate state (S19). `Some` iff the query has
    /// aggregates or a GROUP BY. The evaluator's aggregate-mode
    /// branch re-runs `execute_aggregate_query` on every mutation
    /// and diffs against the previously-emitted per-group output
    /// to produce Add/Update/Remove deltas keyed by group.
    pub aggregate: Option<AggregateSubState>,
}

/// Snapshot of the last-emitted aggregate output, keyed by a
/// canonical group-key string (JSON of the GROUP BY columns).
#[derive(Default, Clone)]
pub struct AggregateSubState {
    /// Last-emitted row per group. Canonical key (see
    /// `group_key_canonical`) → JSON map of the group's row.
    pub last_emitted: HashMap<String, serde_json::Map<String, serde_json::Value>>,
    /// S19-incremental — per-group set of source row indices that
    /// currently match the predicate. Lets a single mutation recompute
    /// only the group(s) it touches instead of rescanning every row.
    /// Lazily built on the first incremental evaluation (see
    /// `incremental_ready`); empty for full-recompute shapes (JOIN,
    /// PIVOT, LIMIT, implicit single-group), which never read it.
    group_rows: HashMap<String, RoaringBitmap>,
    /// S19-incremental — reverse map: source row index → the canonical
    /// group key it currently belongs to. Used to move a mutated row
    /// between groups (and to detect when it leaves the result set).
    row_group: HashMap<u32, String>,
    /// S19-incremental — false until the membership maps above have
    /// been seeded from a full scan. The first evaluation on a
    /// supported shape does that seed (and a full diff); subsequent
    /// evaluations take the incremental path.
    incremental_ready: bool,
}

/// Canonicalize a group-by row's identity for diffing. Picks only
/// the group-by columns in declared order and serializes them as
/// JSON. Stable regardless of value ordering inside the row map.
pub fn group_key_canonical(
    row: &serde_json::Map<String, serde_json::Value>,
    group_by_col_names: &[String],
) -> String {
    let mut parts: Vec<(&str, &serde_json::Value)> = Vec::with_capacity(group_by_col_names.len());
    for n in group_by_col_names {
        let v = row.get(n).unwrap_or(&serde_json::Value::Null);
        parts.push((n.as_str(), v));
    }
    serde_json::to_string(&parts).unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The subscription's active set hit `max_active`. Client should
    /// narrow its filter and re-subscribe.
    TooManyMatches,
}

impl Subscription {
    pub fn new(id: String, query: ParsedQuery) -> Self {
        let topn = match query.limit {
            Some(n) if n > 0 => Some(TopNState::default()),
            _ => None,
        };
        Subscription {
            id: std::sync::Arc::from(id),
            query,
            active_set: RoaringBitmap::new(),
            sparse: false,
            key_cols: Vec::new(),
            last_snapshot: HashMap::new(),
            topn,
            live_start_sequence: 0,
            closed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            max_active: None,
            close_reason: std::sync::Arc::new(parking_lot::Mutex::new(None)),
            aggregate: None,
        }
    }

    /// Enable continuous-aggregate mode (S19). Activated by the
    /// subscribe path when the query has aggregates or a GROUP BY;
    /// the evaluator's aggregate branch re-runs the aggregate
    /// executor on each event and emits per-group diff deltas.
    pub fn into_aggregating(mut self) -> Self {
        self.aggregate = Some(AggregateSubState::default());
        self
    }

    /// Cap the subscription's active-set size. Once the active set
    /// would grow past `cap`, the evaluator marks this sub closed
    /// with reason `TooManyMatches` instead of inserting. `None`
    /// (the default) means unbounded.
    pub fn with_max_active(mut self, cap: u32) -> Self {
        self.max_active = Some(cap);
        self
    }

    /// Last-known close reason, if any.
    pub fn close_reason(&self) -> Option<CloseReason> {
        *self.close_reason.lock()
    }

    /// Cheap clone of the cancellation flag. Hand to a session /
    /// transport-layer handle so it can mark the sub closed on
    /// client disconnect without acquiring `sub_engine.lock()`.
    pub fn close_handle(&self) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
        self.closed.clone()
    }

    /// True iff the subscription has been marked closed. Cheap atomic
    /// load; safe to call from the hot evaluator path.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Set the cancellation flag. Idempotent.
    pub fn close(&self) {
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Convert this subscription to sparse-delta mode with the given
    /// topic key column indices.
    pub fn into_sparse(mut self, key_cols: Vec<usize>) -> Self {
        self.sparse = true;
        self.key_cols = key_cols;
        self
    }

    /// Suppress live deltas whose sequence is below `seq`. The bookmark
    /// path uses this so the live evaluator doesn't re-deliver events
    /// already covered by the replay stream.
    pub fn with_live_start(mut self, seq: u64) -> Self {
        self.live_start_sequence = seq;
        self
    }

    pub fn is_topn(&self) -> bool {
        self.topn.is_some()
    }
}

/// Resolve the effective projection columns for a query. Empty projection
/// means "all columns" — expand to the full schema range.
fn effective_projection(query: &ParsedQuery, schema_column_count: usize) -> Vec<usize> {
    if query.projection.is_empty() {
        (0..schema_column_count).collect()
    } else {
        query.projection.clone()
    }
}

/// Capture current values for `columns` from `row`, indexed by schema
/// column index.
fn snapshot_columns(
    store: &ColumnStore,
    row: u32,
    columns: &[usize],
) -> HashMap<usize, Value> {
    columns.iter().map(|&c| (c, store.get(c, row))).collect()
}

/// Result of diffing a row against its last sparse snapshot.
struct SparseDiff {
    payload: serde_json::Map<String, serde_json::Value>,
    new_snapshot: HashMap<usize, Value>,
    any_changed: bool,
}

/// Build a sparse Update diff: emit only columns whose value changed vs
/// `prev`, plus all `key_cols` (always included for correlation). The
/// caller suppresses the delta entirely when `any_changed` is false.
fn diff_update(
    prev: Option<&HashMap<usize, Value>>,
    store: &ColumnStore,
    row: u32,
    proj_cols: &[usize],
    key_cols: &[usize],
) -> SparseDiff {
    let mut out = serde_json::Map::new();
    let mut new_snap = HashMap::with_capacity(proj_cols.len());
    let schema = store.schema();
    let mut any_changed = false;

    for &col_idx in proj_cols {
        let val = store.get(col_idx, row);
        new_snap.insert(col_idx, val.clone());
        let changed = match prev {
            Some(p) => p.get(&col_idx) != Some(&val),
            None => true,
        };
        if changed {
            any_changed = true;
            if !val.is_null() {
                out.insert(schema.column_name(col_idx).to_string(), val.to_json());
            }
        } else if key_cols.contains(&col_idx) && !val.is_null() {
            // Key columns ride along on every emitted update.
            out.insert(schema.column_name(col_idx).to_string(), val.to_json());
        }
    }

    // Include key cols not in the projection (for correlation + remove).
    for &k in key_cols {
        if !new_snap.contains_key(&k) {
            let v = store.get(k, row);
            new_snap.insert(k, v.clone());
            if any_changed && !v.is_null() {
                out.insert(schema.column_name(k).to_string(), v.to_json());
            }
        }
    }

    SparseDiff {
        payload: out,
        new_snapshot: new_snap,
        any_changed,
    }
}

/// Build a key-only payload from `prev_snapshot` for a Remove delta.
fn key_only_payload(
    prev: &HashMap<usize, Value>,
    key_cols: &[usize],
    store: &ColumnStore,
) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    let schema = store.schema();
    for &k in key_cols {
        if let Some(v) = prev.get(&k) {
            if !v.is_null() {
                out.insert(schema.column_name(k).to_string(), v.to_json());
            }
        }
    }
    out
}

/// Continuous-aggregate evaluation. Diffs the recomputed group result
/// against `sub.aggregate.last_emitted` and emits one delta per affected
/// group:
///
/// - **Add** — group key not in `last_emitted` (new group).
/// - **Update** — group key present but its aggregate values changed.
/// - **Remove** — group key in `last_emitted` but no longer present
///   (last row of that group was removed).
///
/// Dispatcher: supported single-topic `GROUP BY` shapes take the
/// incremental path (recompute only the group(s) a mutation touches);
/// everything else (JOIN, PIVOT, LIMIT/OFFSET, window fns, implicit
/// single-group) falls back to the O(rows) full recompute, which is
/// always correct.
fn evaluate_aggregating(
    sub: &mut Subscription,
    row: u32,
    sequence: u64,
    kind: crate::topic::MutationKind,
    store: &ColumnStore,
    deltas: &mut Vec<Delta>,
) {
    // Supported shapes are seeded at subscribe time
    // (`seed_aggregate_membership`) and flagged ready. If a shape needs
    // full recompute, or membership wasn't seeded for any reason, take
    // the always-correct O(rows) path.
    let ready = sub
        .aggregate
        .as_ref()
        .map(|a| a.incremental_ready)
        .unwrap_or(false);
    if !ready || aggregate_needs_full_recompute(&sub.query) {
        evaluate_aggregating_full(sub, sequence, store, deltas);
        return;
    }
    evaluate_aggregating_incremental(sub, row, sequence, kind, store, deltas);
}

/// Seed the incremental aggregate membership maps from the current
/// store, filtered to live (non-tombstoned) rows. No-op for query
/// shapes the incremental evaluator can't maintain (`join`, `pivot`,
/// `limit`, …) — those keep using the full-recompute path. Call once
/// at subscribe, after `last_emitted` is seeded from the same filtered
/// snapshot so the two agree.
pub fn seed_aggregate_membership(
    sub: &mut Subscription,
    store: &ColumnStore,
    live_rows: &RoaringBitmap,
) {
    if aggregate_needs_full_recompute(&sub.query) {
        return;
    }
    let (group_rows, row_group) =
        crate::query::build_group_membership(&sub.query, store, Some(live_rows));
    if let Some(agg) = sub.aggregate.as_mut() {
        agg.group_rows = group_rows;
        agg.row_group = row_group;
        agg.incremental_ready = true;
    }
}

/// Shapes the incremental evaluator can't maintain by single-row
/// group membership. A subscription's query is fixed, so this is a
/// stable per-sub classification: a sub is either always-incremental
/// or always-full.
fn aggregate_needs_full_recompute(query: &crate::query::ParsedQuery) -> bool {
    query.join.is_some()
        || query.pivot.is_some()
        || query.unpivot.is_some()
        // LIMIT/OFFSET select a subset of groups by sort/insertion
        // order — a single group's change can shift the visible
        // window, which incremental per-group diff can't track.
        || query.limit.is_some()
        || query.offset.is_some()
        || !query.windows.is_empty()
        // Implicit single-group (e.g. COUNT(*) with no GROUP BY) has
        // the "empty input still emits one row" rule; cheap anyway.
        || query.group_by.is_empty()
}

/// Incremental path: a single mutation can only change the group the
/// row left and/or the group it joined. Recompute just those, diff
/// each against `last_emitted`, emit per-group Add/Update/Remove.
fn evaluate_aggregating_incremental(
    sub: &mut Subscription,
    row: u32,
    sequence: u64,
    kind: crate::topic::MutationKind,
    store: &ColumnStore,
    deltas: &mut Vec<Delta>,
) {
    let group_cols = sub.query.group_by.clone();
    // A delete nulls the row in place — it leaves the result set even
    // under a `True` predicate, mirroring the row-oriented path.
    let matches = if kind == crate::topic::MutationKind::Delete {
        false
    } else {
        sub.query.predicate.matches(store, row)
    };
    let new_key = if matches {
        Some(crate::query::group_key_canonical_from_store(store, row, &group_cols))
    } else {
        None
    };

    // Phase 1 — update membership bitmaps; collect the dirty groups.
    let Some(agg) = sub.aggregate.as_mut() else {
        // Structurally guaranteed by the dispatcher (only called when
        // aggregate.is_some()). Degrade to a no-op instead of panicking
        // the evaluator thread if that invariant is ever violated.
        tracing::error!(
            sub = %sub.id,
            "evaluate_aggregating_incremental called without aggregate state"
        );
        return;
    };
    let old_key = agg.row_group.get(&row).cloned();
    if old_key.is_none() && new_key.is_none() {
        // Row isn't a member and still isn't — nothing this sub cares
        // about changed.
        return;
    }
    let mut dirty: Vec<String> = Vec::with_capacity(2);
    match (&old_key, &new_key) {
        (Some(ok), Some(nk)) if ok == nk => dirty.push(nk.clone()),
        _ => {
            if let Some(ok) = &old_key {
                if let Some(bm) = agg.group_rows.get_mut(ok) {
                    bm.remove(row);
                    if bm.is_empty() {
                        agg.group_rows.remove(ok);
                    }
                }
                dirty.push(ok.clone());
            }
            if let Some(nk) = &new_key {
                agg.group_rows.entry(nk.clone()).or_default().insert(row);
                dirty.push(nk.clone());
            }
        }
    }
    match &new_key {
        Some(nk) => {
            agg.row_group.insert(row, nk.clone());
        }
        None => {
            agg.row_group.remove(&row);
        }
    }

    // Phase 2 — recompute each dirty group and diff against the last
    // emitted output for that group.
    let sub_id = sub.id.clone();
    for key in &dirty {
        let recomputed = match sub.aggregate.as_ref().unwrap().group_rows.get(key) {
            Some(bm) if !bm.is_empty() => {
                crate::query::aggregate_one_group(&sub.query, store, bm)
            }
            // Group is empty (last member left) or failed HAVING.
            _ => None,
        };
        let agg = sub.aggregate.as_mut().unwrap();
        match recomputed {
            Some(new_row) => match agg.last_emitted.get(key) {
                None => {
                    deltas.push(Delta {
                        subscription_id: sub_id.clone(),
                        delta_type: DeltaType::Add,
                        row: 0,
                        sequence,
                        row_data: std::sync::Arc::new(new_row.clone()),
                        encoded_body_json: None,
                    });
                    agg.last_emitted.insert(key.clone(), new_row);
                }
                Some(prev) if *prev != new_row => {
                    deltas.push(Delta {
                        subscription_id: sub_id.clone(),
                        delta_type: DeltaType::Update,
                        row: 0,
                        sequence,
                        row_data: std::sync::Arc::new(new_row.clone()),
                        encoded_body_json: None,
                    });
                    agg.last_emitted.insert(key.clone(), new_row);
                }
                _ => {}
            },
            None => {
                if let Some(prev) = agg.last_emitted.remove(key) {
                    deltas.push(Delta {
                        subscription_id: sub_id.clone(),
                        delta_type: DeltaType::Remove,
                        row: 0,
                        sequence,
                        row_data: std::sync::Arc::new(prev),
                        encoded_body_json: None,
                    });
                }
            }
        }
    }
}

/// Full O(rows) recompute. Correct for every shape; used as the
/// fallback for query shapes the incremental path doesn't maintain.
fn evaluate_aggregating_full(
    sub: &mut Subscription,
    sequence: u64,
    store: &ColumnStore,
    deltas: &mut Vec<Delta>,
) {
    let Some(agg_state) = sub.aggregate.as_mut() else {
        tracing::error!(sub = %sub.id, "evaluate_aggregating_full called without aggregate state");
        return;
    };
    // Re-run the aggregate executor. This is the heavy part.
    let result = crate::query::execute_query_with_index(&sub.query, store, None);

    // Build canonical group-key → row map for the current output.
    let schema = store.schema();
    let group_names: Vec<String> = sub
        .query
        .group_by
        .iter()
        .map(|&i| schema.column_name(i).to_string())
        .collect();
    let mut current: HashMap<String, serde_json::Map<String, serde_json::Value>> =
        HashMap::with_capacity(result.rows.len());
    for row in result.rows {
        let k = group_key_canonical(&row, &group_names);
        current.insert(k, row);
    }

    // Emit Add / Update for groups in current.
    for (k, row) in &current {
        match agg_state.last_emitted.get(k) {
            None => deltas.push(Delta {
                subscription_id: sub.id.clone(),
                delta_type: DeltaType::Add,
                row: 0,
                sequence,
                row_data: std::sync::Arc::new(row.clone()),
                encoded_body_json: None,
            }),
            Some(prev) if prev != row => deltas.push(Delta {
                subscription_id: sub.id.clone(),
                delta_type: DeltaType::Update,
                row: 0,
                sequence,
                row_data: std::sync::Arc::new(row.clone()),
                encoded_body_json: None,
            }),
            _ => {}
        }
    }
    // Emit Remove for groups in last_emitted but not in current.
    for (k, prev) in &agg_state.last_emitted {
        if !current.contains_key(k) {
            deltas.push(Delta {
                subscription_id: sub.id.clone(),
                delta_type: DeltaType::Remove,
                row: 0,
                sequence,
                // Body carries the last-known group payload so the
                // client knows WHICH group is gone — the group-by
                // columns identify it.
                row_data: std::sync::Arc::new(prev.clone()),
                encoded_body_json: None,
            });
        }
    }

    agg_state.last_emitted = current;
}

/// TOP-N evaluation for a single mutation event. Updates the subscription's
/// `ranked` set with the mutated row's new key (or removes it if the
/// predicate no longer matches), recomputes the visible top-N window,
/// then diffs the previous window against the new one to emit
/// Add / Update / Oof / Remove deltas.
fn evaluate_topn(
    sub: &mut Subscription,
    row: u32,
    sequence: u64,
    store: &ColumnStore,
    deltas: &mut Vec<Delta>,
) {
    let limit = sub.query.limit.unwrap_or(0);
    if limit == 0 {
        return;
    }
    let matches = sub.query.predicate.matches(store, row);

    // 1. Update `ranked` for the mutated row.
    let Some(topn) = sub.topn.as_mut() else {
        tracing::error!(sub = %sub.id, "evaluate_topn called without topn state");
        return;
    };
    if let Some(old_key) = topn.row_to_key.remove(&row) {
        topn.ranked.remove(&(old_key, row));
    }
    if matches {
        let new_key = build_sort_key(store, row, &sub.query.order_by);
        topn.ranked.insert((new_key.clone(), row));
        topn.row_to_key.insert(row, new_key);
        sub.active_set.insert(row);
    } else {
        sub.active_set.remove(row);
    }

    // 2. Recompute the visible window.
    let new_topn: HashSet<u32> = topn
        .ranked
        .iter()
        .take(limit)
        .map(|(_, r)| *r)
        .collect();
    let old_topn = std::mem::replace(&mut topn.current_topn, new_topn.clone());

    // Snapshot membership for the mutated row before we move on.
    let mutated_was_in = old_topn.contains(&row);
    let mutated_now_in = new_topn.contains(&row);

    // 3. Rows that entered the window → Add.
    for &r in new_topn.iter() {
        if !old_topn.contains(&r) {
            let row_data = std::sync::Arc::new(project_row(&sub.query, store, r));
            deltas.push(Delta {
                subscription_id: sub.id.clone(),
                delta_type: DeltaType::Add,
                row: r,
                sequence,
                row_data,
                encoded_body_json: None,
            });
        }
    }

    // 4. Rows that left the window → Oof if still in ranked (rank dropped),
    //    Remove if predicate no longer matches.
    for &r in old_topn.iter() {
        if !new_topn.contains(&r) {
            let still_ranked = topn.row_to_key.contains_key(&r);
            let delta_type = if still_ranked {
                DeltaType::Oof
            } else {
                DeltaType::Remove
            };
            let row_data = std::sync::Arc::new(project_row(&sub.query, store, r));
            deltas.push(Delta {
                subscription_id: sub.id.clone(),
                delta_type,
                row: r,
                sequence,
                row_data,
                encoded_body_json: None,
            });
        }
    }

    // 5. Mutated row stayed inside the window → Update.
    if mutated_was_in && mutated_now_in {
        let row_data = std::sync::Arc::new(project_row(&sub.query, store, row));
        deltas.push(Delta {
            subscription_id: sub.id.clone(),
            delta_type: DeltaType::Update,
            row,
            sequence,
            row_data,
            encoded_body_json: None,
        });
    }
}

/// Manages all active subscriptions for a single topic.
pub struct SubscriptionEngine {
    subscriptions: HashMap<String, Subscription>,
    /// Reverse-index from column id to subscription ids whose
    /// predicate references that column (S46 / review concern C3).
    /// Used by `evaluate_row_kind` to skip subscriptions that
    /// cannot possibly be affected by a sparse mutation that
    /// touches a disjoint column set.
    predicate_index: crate::predicate_index::PredicateIndex,
}

impl SubscriptionEngine {
    pub fn new() -> Self {
        SubscriptionEngine {
            subscriptions: HashMap::new(),
            predicate_index: crate::predicate_index::PredicateIndex::new(),
        }
    }

    /// Register a new subscription. Returns the subscription ID.
    /// The caller should separately compute and deliver the initial snapshot.
    pub fn add(&mut self, sub: Subscription) {
        self.predicate_index.add(&sub.id, &sub.query.predicate);
        // Registry stays String-keyed (cold path); convert at the boundary.
        self.subscriptions.insert(sub.id.to_string(), sub);
    }

    /// Remove a subscription by ID.
    pub fn remove(&mut self, id: &str) -> Option<Subscription> {
        self.predicate_index.remove(id);
        self.subscriptions.remove(id)
    }

    /// Remove all subscriptions whose ID starts with a given prefix
    /// (e.g., session-based cleanup). Returns the number removed so
    /// callers can keep an out-of-band count (Topic's atomic
    /// `active_subscriptions`) in sync without re-locking the engine.
    pub fn remove_by_prefix(&mut self, prefix: &str) -> usize {
        let to_drop: Vec<String> = self
            .subscriptions
            .keys()
            .filter(|id| id.starts_with(prefix))
            .cloned()
            .collect();
        for id in &to_drop {
            self.predicate_index.remove(id);
        }
        self.subscriptions.retain(|id, _| !id.starts_with(prefix));
        to_drop.len()
    }

    /// Mark the named subscription as closed without removing it. The
    /// next event evaluator pass will skip it (see `evaluate_row_kind`);
    /// the next `reap_closed` pass will drop it. Returns `true` if the
    /// subscription existed.
    pub fn mark_closed(&mut self, id: &str) -> bool {
        if let Some(sub) = self.subscriptions.get(id) {
            sub.close();
            true
        } else {
            false
        }
    }

    /// Drop every subscription whose `closed` flag is set. Returns the
    /// number reaped. Cheap to call periodically (e.g., from a
    /// per-topic background tick or after a known disconnect storm).
    pub fn reap_closed(&mut self) -> usize {
        let to_drop: Vec<String> = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| sub.is_closed())
            .map(|(id, _)| id.clone())
            .collect();
        for id in &to_drop {
            self.predicate_index.remove(id);
            self.subscriptions.remove(id);
        }
        to_drop.len()
    }

    /// Seed the active set for a subscription by scanning the store.
    /// Call this after the subscription is registered to build its
    /// initial matching set.
    pub fn seed_active_set(&mut self, sub_id: &str, store: &ColumnStore) {
        if let Some(sub) = self.subscriptions.get_mut(sub_id) {
            let schema_cols = store.schema().column_count();
            let proj = effective_projection(&sub.query, schema_cols);
            let row_count = store.row_count();
            let cap = sub.max_active;
            for row in 0..row_count {
                if sub.query.predicate.matches(store, row) {
                    // S42 / C11: enforce active-set cap on the
                    // snapshot too. If the matching snapshot is
                    // already past the cap, we close the sub and
                    // stop seeding — the client gets a no-snapshot
                    // start + `TooManyMatches` close reason, which
                    // is the right signal to narrow the filter.
                    if let Some(c) = cap {
                        if sub.active_set.len() >= c as u64 {
                            *sub.close_reason.lock() = Some(CloseReason::TooManyMatches);
                            sub.close();
                            return;
                        }
                    }
                    sub.active_set.insert(row);
                    if sub.sparse {
                        let snap = snapshot_columns(store, row, &proj);
                        sub.last_snapshot.insert(row, snap);
                    }
                    if let Some(topn) = sub.topn.as_mut() {
                        let key = build_sort_key(store, row, &sub.query.order_by);
                        topn.ranked.insert((key.clone(), row));
                        topn.row_to_key.insert(row, key);
                    }
                }
            }
            // After populating `ranked`, prime `current_topn` with the
            // first N entries so subsequent diffs can detect entrants/
            // evictions.
            if let Some(topn) = sub.topn.as_mut() {
                let limit = sub.query.limit.unwrap_or(0);
                topn.current_topn = topn.ranked.iter().take(limit).map(|(_, r)| *r).collect();
            }
        }
    }

    /// Evaluate a row mutation against all subscriptions.
    /// Returns a list of deltas to deliver. `sequence` is forwarded onto
    /// every emitted `Delta` so subscribers can bookmark.
    ///
    /// This is the hot path — called from the mutation channel consumer thread(s).
    pub fn evaluate_row(
        &mut self,
        row: u32,
        sequence: u64,
        store: &ColumnStore,
    ) -> Vec<Delta> {
        self.evaluate_row_kind(row, sequence, store, crate::topic::MutationKind::Upsert)
    }

    /// Variant of `evaluate_row` that takes the originating mutation
    /// kind. Lets the engine emit `Oof` for predicate-flip exits
    /// (`MutationKind::Upsert`) versus `Remove` for actual deletes
    /// (`MutationKind::Delete`). Iterates every registered
    /// subscription.
    pub fn evaluate_row_kind(
        &mut self,
        row: u32,
        sequence: u64,
        store: &ColumnStore,
        kind: crate::topic::MutationKind,
    ) -> Vec<Delta> {
        self.evaluate_row_kind_indexed(row, sequence, store, kind, None)
    }

    /// Index-routed variant (S46 / review C3). When `changed_cols`
    /// is `Some(cols)`, restricts the per-sub loop to subscriptions
    /// whose predicate references at least one column in `cols`
    /// (plus any "always" subs whose predicate is `True`). When
    /// `None`, iterates every registered subscription — equivalent
    /// to `evaluate_row_kind`.
    pub fn evaluate_row_kind_indexed(
        &mut self,
        row: u32,
        sequence: u64,
        store: &ColumnStore,
        kind: crate::topic::MutationKind,
        changed_cols: Option<&[usize]>,
    ) -> Vec<Delta> {
        let mut deltas = Vec::new();
        let schema_cols = store.schema().column_count();

        // Lazy cache: the full row map (no projection) is identical
        // across every non-projecting subscriber. Compute it at most
        // once per evaluator pass, wrap in `Arc`, and hand the same
        // pointer to each of those subs. The delivery path then
        // detects the shared Arc and encodes the body just once.
        let mut shared_full_row: Option<std::sync::Arc<serde_json::Map<String, serde_json::Value>>> = None;
        let full_row = |cache: &mut Option<std::sync::Arc<serde_json::Map<String, serde_json::Value>>>| -> std::sync::Arc<serde_json::Map<String, serde_json::Value>> {
            if let Some(a) = cache.as_ref() {
                return a.clone();
            }
            let a = std::sync::Arc::new(store.get_row_map(row));
            *cache = Some(a.clone());
            a
        };

        // Determine the set of sub IDs to evaluate. When the caller
        // passes `changed_cols`, the `PredicateIndex` returns only
        // subs whose predicate references at least one of those
        // columns (plus "always" subs with no column refs). When
        // `None`, we iterate every registered sub (equivalent to
        // pre-S46 behavior). Collected up-front because the loop
        // body holds `&mut sub` from `self.subscriptions`, which
        // can't coexist with a borrow into `self.predicate_index`.
        let affected_ids: Vec<String> = match changed_cols {
            Some(cols) => self.predicate_index.affected(Some(cols)),
            None => self.subscriptions.keys().cloned().collect(),
        };
        for id in &affected_ids {
            let Some(sub) = self.subscriptions.get_mut(id) else { continue };
            // Cancellation gate (S38 / C8): if the client has
            // disconnected or unsubscribed, skip every per-event
            // computation for this sub. The entry is removed
            // outright on the next `reap_closed` pass; here we
            // only need to avoid wasted predicate eval and Arc
            // bumping while the sub is in the closed→reaped
            // window.
            if sub.is_closed() {
                continue;
            }
            // Bookmark-replay subs skip live events that fall inside the
            // window the replay already covered.
            if sequence < sub.live_start_sequence {
                continue;
            }
            // S19: continuous-aggregate subs re-run the aggregate
            // executor against the current store, diff against
            // their last-emitted per-group state, and emit
            // Add/Update/Remove deltas per affected group. The
            // row-oriented logic below doesn't apply — group state
            // changes don't map to a single source row.
            if sub.aggregate.is_some() {
                evaluate_aggregating(sub, row, sequence, kind, store, &mut deltas);
                continue;
            }
            if sub.topn.is_some() {
                evaluate_topn(sub, row, sequence, store, &mut deltas);
                continue;
            }

            // A delete mutation forces `matches = false` — the row
            // has been nulled in place, so even a predicate of
            // `True` shouldn't keep it in any subscription's view.
            let matches = if kind == crate::topic::MutationKind::Delete {
                false
            } else {
                sub.query.predicate.matches(store, row)
            };
            let was_active = sub.active_set.contains(row);

            if matches && !was_active {
                // S42 / C11: enforce the optional active-set cap. If
                // adding `row` would exceed `max_active`, mark the
                // sub closed with reason `TooManyMatches` and don't
                // emit any further deltas for it. The next event
                // sees `is_closed()` and skips. The transport layer
                // reads `close_reason` to emit a terminate frame.
                if let Some(cap) = sub.max_active {
                    if sub.active_set.len() >= cap as u64 {
                        *sub.close_reason.lock() = Some(CloseReason::TooManyMatches);
                        sub.close();
                        continue;
                    }
                }
                sub.active_set.insert(row);
                let row_data = if sub.query.projection.is_empty() {
                    full_row(&mut shared_full_row)
                } else {
                    std::sync::Arc::new(project_row(&sub.query, store, row))
                };

                if sub.sparse {
                    let proj = effective_projection(&sub.query, schema_cols);
                    let snap = snapshot_columns(store, row, &proj);
                    sub.last_snapshot.insert(row, snap);
                }

                deltas.push(Delta {
                    subscription_id: sub.id.clone(),
                    delta_type: DeltaType::Add,
                    row,
                    sequence,
                    row_data,
                    encoded_body_json: None,
                });
            } else if matches && was_active {
                let row_data = if sub.sparse {
                    let proj = effective_projection(&sub.query, schema_cols);
                    let prev = sub.last_snapshot.get(&row);
                    let diff = diff_update(prev, store, row, &proj, &sub.key_cols);
                    sub.last_snapshot.insert(row, diff.new_snapshot);
                    if !diff.any_changed {
                        continue;
                    }
                    std::sync::Arc::new(diff.payload)
                } else if sub.query.projection.is_empty() {
                    full_row(&mut shared_full_row)
                } else {
                    std::sync::Arc::new(project_row(&sub.query, store, row))
                };
                deltas.push(Delta {
                    subscription_id: sub.id.clone(),
                    delta_type: DeltaType::Update,
                    row,
                    sequence,
                    row_data,
                    encoded_body_json: None,
                });
            } else if !matches && was_active {
                sub.active_set.remove(row);
                let row_data = if sub.sparse {
                    let prev = sub.last_snapshot.remove(&row).unwrap_or_default();
                    std::sync::Arc::new(key_only_payload(&prev, &sub.key_cols, store))
                } else if sub.query.projection.is_empty() {
                    full_row(&mut shared_full_row)
                } else {
                    std::sync::Arc::new(project_row(&sub.query, store, row))
                };
                // Real delete → Remove; predicate flip on an upsert →
                // Oof. The two are semantically different: Remove
                // says "the row is gone from the SOW", Oof says
                // "the row still exists but your filter no longer
                // includes it" — so the client can decide whether to
                // re-query or just drop the local copy.
                let delta_type = match kind {
                    crate::topic::MutationKind::Delete => DeltaType::Remove,
                    crate::topic::MutationKind::Upsert => DeltaType::Oof,
                };
                deltas.push(Delta {
                    subscription_id: sub.id.clone(),
                    delta_type,
                    row,
                    sequence,
                    row_data,
                    encoded_body_json: None,
                });
            }
            // !matches && !was_active → no delta needed
        }

        deltas
    }

    /// Number of active subscriptions.
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Get a reference to a subscription by ID.
    pub fn get(&self, id: &str) -> Option<&Subscription> {
        self.subscriptions.get(id)
    }
}

impl Default for SubscriptionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate::CompiledPredicate;
    use crate::query::ParsedQuery;
    use crate::schema::{ColumnType, Schema};
    use crate::store::{ColumnStore, Value};
    use compact_str::CompactString;
    use std::sync::Arc;

    fn make_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["tradeId", "price", "desk"],
            &[ColumnType::String, ColumnType::Double, ColumnType::String],
        ));
        let mut store = ColumnStore::new(schema.clone(), 100);

        store.append_row(&[
            Value::String(Some(CompactString::new("T001"))),
            Value::Double(100.0),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("T002"))),
            Value::Double(200.0),
            Value::String(Some(CompactString::new("EQUITIES"))),
        ]);

        (schema, store)
    }

    fn make_sparse_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["tradeId", "price", "qty", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        ));
        let mut store = ColumnStore::new(schema.clone(), 100);
        store.append_row(&[
            Value::String(Some(CompactString::new("T001"))),
            Value::Double(100.0),
            Value::Long(10),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        (schema, store)
    }

    fn pred_eq_desk_rates() -> CompiledPredicate {
        CompiledPredicate::EqString {
            col: 3,
            value: CompactString::new("RATES"),
        }
    }

    #[test]
    fn sparse_update_sends_only_changed_fields_plus_key() {
        let (_, mut store) = make_sparse_store();
        let mut engine = SubscriptionEngine::new();

        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![], // SELECT *
            predicate: pred_eq_desk_rates(),
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        // Seed: row 0 is already in store and matches predicate.
        let sub = Subscription::new("s1".into(), query).into_sparse(vec![0]);
        engine.add(sub);
        engine.seed_active_set("s1", &store);

        // Update only `price`.
        store.update_row(
            0,
            &[Value::Null, Value::Double(105.0), Value::Null, Value::Null],
        );
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert_eq!(d.delta_type, DeltaType::Update);
        // Payload: key + changed field only.
        assert!(d.row_data.contains_key("tradeId"));
        assert!(d.row_data.contains_key("price"));
        assert!(!d.row_data.contains_key("qty"));
        assert!(!d.row_data.contains_key("desk"));
        assert_eq!(d.row_data.get("price").unwrap(), 105.0);
    }

    #[test]
    fn sparse_remove_sends_key_only() {
        let (_, mut store) = make_sparse_store();
        let mut engine = SubscriptionEngine::new();

        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: pred_eq_desk_rates(),
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s1".into(), query).into_sparse(vec![0]));
        engine.seed_active_set("s1", &store);

        // Flip desk so the row falls out of the predicate. Treated
        // as an upsert (the row still exists), the engine emits Oof.
        store.update_row(
            0,
            &[
                Value::Null,
                Value::Null,
                Value::Null,
                Value::String(Some(CompactString::new("EQUITIES"))),
            ],
        );
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        let d = &deltas[0];
        assert_eq!(d.delta_type, DeltaType::Oof);
        // Only the key field (tradeId).
        assert_eq!(d.row_data.len(), 1);
        assert!(d.row_data.contains_key("tradeId"));
    }

    #[test]
    fn sparse_suppresses_noop_updates() {
        let (_, mut store) = make_sparse_store();
        let mut engine = SubscriptionEngine::new();

        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: pred_eq_desk_rates(),
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s1".into(), query).into_sparse(vec![0]));
        engine.seed_active_set("s1", &store);

        // Bump the row's version without changing any value (re-write the
        // same data). The sparse subscriber should receive nothing.
        store.update_row(
            0,
            &[
                Value::String(Some(CompactString::new("T001"))),
                Value::Double(100.0),
                Value::Long(10),
                Value::String(Some(CompactString::new("RATES"))),
            ],
        );
        let deltas = engine.evaluate_row(0, 1, &store);
        assert!(deltas.is_empty());
    }

    #[test]
    fn non_sparse_update_unchanged_behavior() {
        // Regression: existing non-sparse subscribers must still get full
        // row data on Update.
        let (_, mut store) = make_sparse_store();
        let mut engine = SubscriptionEngine::new();

        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: pred_eq_desk_rates(),
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s1".into(), query));
        engine.seed_active_set("s1", &store);

        store.update_row(
            0,
            &[Value::Null, Value::Double(110.0), Value::Null, Value::Null],
        );
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        // Non-sparse: every projected column (= all columns) present.
        let d = &deltas[0];
        assert!(d.row_data.contains_key("tradeId"));
        assert!(d.row_data.contains_key("price"));
        assert!(d.row_data.contains_key("qty"));
        assert!(d.row_data.contains_key("desk"));
    }

    #[test]
    fn test_subscription_lifecycle() {
        let (_schema, mut store) = make_store();

        let mut engine = SubscriptionEngine::new();

        // Subscribe: desk = 'RATES'
        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: CompiledPredicate::EqString {
                col: 2,
                value: CompactString::new("RATES"),
            },
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        let sub = Subscription::new("sub-1".into(), query);
        engine.add(sub);
        engine.seed_active_set("sub-1", &store);

        // Verify active set
        let sub = engine.get("sub-1").unwrap();
        assert!(sub.active_set.contains(0));   // T001 is RATES
        assert!(!sub.active_set.contains(1));  // T002 is EQUITIES

        // Add a new RATES trade
        store.append_row(&[
            Value::String(Some(CompactString::new("T003"))),
            Value::Double(150.0),
            Value::String(Some(CompactString::new("RATES"))),
        ]);
        let deltas = engine.evaluate_row(2, 1, &store);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Add);
        assert_eq!(deltas[0].row_data.get("tradeId").unwrap(), "T003");

        // Update T001 price (still RATES → UPDATE)
        store.update_row(0, &[
            Value::Null,
            Value::Double(105.0),
            Value::Null,
        ]);
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Update);

        // Change T001 desk to EQUITIES — row still exists, just leaves
        // the subscription's filter → Oof.
        store.update_row(0, &[
            Value::Null,
            Value::Null,
            Value::String(Some(CompactString::new("EQUITIES"))),
        ]);
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Oof);
    }

    #[test]
    fn filter_exit_emits_oof_not_remove() {
        let (_schema, mut store) = make_store();
        let mut engine = SubscriptionEngine::new();
        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: CompiledPredicate::EqString {
                col: 2,
                value: CompactString::new("RATES"),
            },
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s".into(), query));
        engine.seed_active_set("s", &store);

        // Flip desk to EQUITIES via Upsert mutation kind → Oof.
        store.update_row(
            0,
            &[
                Value::Null,
                Value::Null,
                Value::String(Some(CompactString::new("EQUITIES"))),
            ],
        );
        let deltas = engine.evaluate_row_kind(
            0,
            1,
            &store,
            crate::topic::MutationKind::Upsert,
        );
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Oof);
    }

    #[test]
    fn delete_emits_remove_not_oof() {
        // Even with the predicate that previously matched the row,
        // a Delete mutation kind forces a Remove emission — the
        // row is gone, the subscriber can't keep it.
        let (_schema, store) = make_store();
        let mut engine = SubscriptionEngine::new();
        let query = ParsedQuery {
            topic: "trades".into(),
            projection: vec![],
            predicate: CompiledPredicate::EqString {
                col: 2,
                value: CompactString::new("RATES"),
            },
            order_by: vec![],
            order_by_aliases: vec![],
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s".into(), query));
        engine.seed_active_set("s", &store);
        let deltas = engine.evaluate_row_kind(
            0,
            1,
            &store,
            crate::topic::MutationKind::Delete,
        );
        assert_eq!(deltas.len(), 1, "expected one delta on delete, got {deltas:?}");
        assert_eq!(deltas[0].delta_type, DeltaType::Remove);
    }

    // ===== TOP-N tests =====

    fn make_ranked_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut store = ColumnStore::new(schema.clone(), 100);
        for (sym, price) in [("A", 50.0), ("B", 30.0), ("C", 40.0), ("D", 20.0)] {
            store.append_row(&[
                Value::String(Some(CompactString::new(sym))),
                Value::Double(price),
            ]);
        }
        (schema, store)
    }

    fn topn_query(limit: usize) -> ParsedQuery {
        ParsedQuery {
            topic: "t".into(),
            projection: vec![],
            predicate: CompiledPredicate::True,
            order_by: vec![(1, false)], // price DESC
            order_by_aliases: vec![],
            limit: Some(limit),
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        }
    }

    #[test]
    fn topn_seed_picks_highest_n() {
        let (_, store) = make_ranked_store();
        let mut engine = SubscriptionEngine::new();
        engine.add(Subscription::new("s1".into(), topn_query(2)));
        engine.seed_active_set("s1", &store);

        let sub = engine.get("s1").unwrap();
        let topn = sub.topn.as_ref().unwrap();
        // Rows: A=50, B=30, C=40, D=20. DESC top-2 → A (row 0), C (row 2).
        assert!(topn.current_topn.contains(&0));
        assert!(topn.current_topn.contains(&2));
        assert!(!topn.current_topn.contains(&1));
        assert!(!topn.current_topn.contains(&3));
    }

    #[test]
    fn topn_new_high_row_displaces_lowest_in_window() {
        let (_, mut store) = make_ranked_store();
        let mut engine = SubscriptionEngine::new();
        engine.add(Subscription::new("s1".into(), topn_query(2)));
        engine.seed_active_set("s1", &store);

        // Append E=100 → must enter top-2; C (price=40) drops out → Oof.
        let row = store.append_row(&[
            Value::String(Some(CompactString::new("E"))),
            Value::Double(100.0),
        ]);
        let deltas = engine.evaluate_row(row, 1, &store);

        let by_type: Vec<_> = deltas.iter().map(|d| (d.delta_type, d.row)).collect();
        // Should be exactly one Add (the new row) + one Oof (row 2, "C").
        assert_eq!(deltas.len(), 2);
        assert!(by_type.contains(&(DeltaType::Add, row)));
        assert!(by_type.contains(&(DeltaType::Oof, 2)));
    }

    #[test]
    fn topn_update_inside_window_emits_update_only() {
        let (_, mut store) = make_ranked_store();
        let mut engine = SubscriptionEngine::new();
        engine.add(Subscription::new("s1".into(), topn_query(2)));
        engine.seed_active_set("s1", &store);

        // Row 0 is "A" with price=50, currently in top-2. Bump to 60 —
        // still in top-2, just a value change.
        store.update_row(0, &[Value::Null, Value::Double(60.0)]);
        let deltas = engine.evaluate_row(0, 1, &store);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Update);
        assert_eq!(deltas[0].row, 0);
    }

    #[test]
    fn topn_predicate_drop_emits_remove() {
        let (_, mut store) = make_ranked_store();
        let mut engine = SubscriptionEngine::new();
        // Sub: price > 25 ORDER BY price DESC LIMIT 2.
        let query = ParsedQuery {
            topic: "t".into(),
            projection: vec![],
            predicate: CompiledPredicate::GtDouble { col: 1, value: 25.0 },
            order_by: vec![(1, false)],
            order_by_aliases: vec![],
            limit: Some(2),
            aggregates: Vec::new(),
            group_by: Vec::new(),
            pivot: None,
            unpivot: None,
            join: None,
            computed: Vec::new(),
            having: None,
            windows: Vec::new(),
            offset: None,
        };
        engine.add(Subscription::new("s1".into(), query));
        engine.seed_active_set("s1", &store);
        // Top-2 by price: A (50) row 0, C (40) row 2.

        // Drop A's price to 10 → no longer matches predicate. Now top-2 of
        // remaining matches is C (40), B (30). C stays, B enters, A leaves
        // with Remove (predicate flipped).
        store.update_row(0, &[Value::Null, Value::Double(10.0)]);
        let deltas = engine.evaluate_row(0, 1, &store);
        let by_type: Vec<_> = deltas.iter().map(|d| (d.delta_type, d.row)).collect();
        assert!(by_type.contains(&(DeltaType::Remove, 0)));
        assert!(by_type.contains(&(DeltaType::Add, 1))); // B (row 1) enters
    }

    #[test]
    fn topn_re_rank_inside_window_emits_update_not_oof() {
        let (_, mut store) = make_ranked_store();
        let mut engine = SubscriptionEngine::new();
        engine.add(Subscription::new("s1".into(), topn_query(2)));
        engine.seed_active_set("s1", &store);
        // Top-2 DESC: A (50) row 0, C (40) row 2.

        // Bump C's price to 100 — still in top-2, just re-ranks above A.
        store.update_row(2, &[Value::Null, Value::Double(100.0)]);
        let deltas = engine.evaluate_row(2, 1, &store);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].delta_type, DeltaType::Update);
        assert_eq!(deltas[0].row, 2);
    }
}

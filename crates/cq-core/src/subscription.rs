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
    pub subscription_id: String,
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
    pub id: String,
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
}

impl Subscription {
    pub fn new(id: String, query: ParsedQuery) -> Self {
        let topn = match query.limit {
            Some(n) if n > 0 => Some(TopNState::default()),
            _ => None,
        };
        Subscription {
            id,
            query,
            active_set: RoaringBitmap::new(),
            sparse: false,
            key_cols: Vec::new(),
            last_snapshot: HashMap::new(),
            topn,
            live_start_sequence: 0,
        }
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
    let topn = sub.topn.as_mut().expect("evaluate_topn requires topn state");
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
}

impl SubscriptionEngine {
    pub fn new() -> Self {
        SubscriptionEngine {
            subscriptions: HashMap::new(),
        }
    }

    /// Register a new subscription. Returns the subscription ID.
    /// The caller should separately compute and deliver the initial snapshot.
    pub fn add(&mut self, sub: Subscription) {
        self.subscriptions.insert(sub.id.clone(), sub);
    }

    /// Remove a subscription by ID.
    pub fn remove(&mut self, id: &str) -> Option<Subscription> {
        self.subscriptions.remove(id)
    }

    /// Remove all subscriptions whose ID starts with a given prefix
    /// (e.g., session-based cleanup).
    pub fn remove_by_prefix(&mut self, prefix: &str) {
        self.subscriptions.retain(|id, _| !id.starts_with(prefix));
    }

    /// Seed the active set for a subscription by scanning the store.
    /// Call this after the subscription is registered to build its
    /// initial matching set.
    pub fn seed_active_set(&mut self, sub_id: &str, store: &ColumnStore) {
        if let Some(sub) = self.subscriptions.get_mut(sub_id) {
            let schema_cols = store.schema().column_count();
            let proj = effective_projection(&sub.query, schema_cols);
            let row_count = store.row_count();
            for row in 0..row_count {
                if sub.query.predicate.matches(store, row) {
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
    /// (`MutationKind::Delete`).
    pub fn evaluate_row_kind(
        &mut self,
        row: u32,
        sequence: u64,
        store: &ColumnStore,
        kind: crate::topic::MutationKind,
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

        for sub in self.subscriptions.values_mut() {
            // Bookmark-replay subs skip live events that fall inside the
            // window the replay already covered.
            if sequence < sub.live_start_sequence {
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: None,
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: Some(limit),
            aggregates: Vec::new(),
            group_by: Vec::new(),
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
            limit: Some(2),
            aggregates: Vec::new(),
            group_by: Vec::new(),
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

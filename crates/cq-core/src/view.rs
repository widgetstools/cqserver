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
use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam_channel::Receiver;
use parking_lot::Mutex;

use crate::query::{
    combined_join_schema, parse_query, peek_join, AggFn, ParsedQuery, QueryError,
};
use crate::schema::{ColumnType, Schema};
use crate::subscription::group_key_canonical;
use crate::topic::{MutationEvent, SharedTopic, Topic, TopicConfig, TopicError};

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
}

#[derive(Default)]
struct ViewState {
    /// Canonical group key → last-emitted row map.
    last_emitted: HashMap<String, serde_json::Map<String, serde_json::Value>>,
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
            };
            types.push(ty);
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
        });
        view.refresh()?;
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
pub fn spawn_view_runner(view: Arc<View>, tap_rx: Receiver<MutationEvent>) -> JoinHandle<()> {
    let view_name = view.view_topic.name().to_string();
    std::thread::Builder::new()
        .name(format!("view:{}", view_name))
        .spawn(move || {
            tracing::info!(view = %view_name, "View runner started");
            while let Ok(_event) = tap_rx.recv() {
                // Drain any other queued events before the refresh.
                while tap_rx.try_recv().is_ok() {}
                if let Err(e) = view.refresh() {
                    tracing::warn!(view = %view_name, error = %e, "View refresh failed");
                }
            }
            tracing::info!(view = %view_name, "View runner exiting");
        })
        .expect("view runner spawn")
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
) -> JoinHandle<()> {
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
        })
        .expect("view fan-in spawn");
    spawn_view_runner(view, merged_rx)
}

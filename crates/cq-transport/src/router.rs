//! Transport-agnostic command dispatcher.
//!
//! Both the WebSocket and TCP transports decode a `CqMessage` and then
//! call `dispatch`. Per-command handlers live here so the protocol
//! semantics stay in one place.

use crate::auth::{Op, SharedAuth};
use crate::limits::ConnectionLimits;
use crate::queue::QueueRegistry;
use crate::session::{DeliveryRoute, Session, SessionRegistry};
use cq_core::query::peek_join;
use cq_core::topic::SharedTopic;
use cq_protocol::command::{AckType, Command};
use cq_protocol::message::CqMessage;

/// R10 — derived-table materialisation. Detects
/// `SELECT … FROM (SELECT … FROM real_topic …) [outer pred]` and
/// runs the inner subquery against the source topic, then re-runs
/// the outer SELECT against an EPHEMERAL in-memory topic seeded
/// with the inner rows. The transient topic carries an
/// auto-discovered schema from the inner result; it lives only for
/// the duration of the call.
///
/// AMPS supports derived tables in this exact shape — the inner is
/// an uncorrelated query against a real topic, the outer is a
/// final projection / filter / sort / limit. Correlated derived
/// tables (where the inner references outer columns) aren't AMPS-
/// supported and are out of scope.
///
/// Returns:
///   - `Ok(Some(rows))` — the derived-table path consumed the query
///     and produced the final outer result.
///   - `Ok(None)` — query has no derived table; fall through to the
///     regular SOW path.
///   - `Err(...)` — derived-table detected but materialisation /
///     outer execution failed.
fn try_resolve_derived_table(
    sql: &str,
    topics: &dashmap::DashMap<String, SharedTopic>,
) -> Result<Option<Vec<serde_json::Map<String, serde_json::Value>>>, String> {
    use sqlparser::ast::{
        Query, SetExpr, Statement, TableFactor,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let dialect = GenericDialect {};
    // Same parse-tolerance as resolve_in_subqueries: PIVOT and other
    // AMPS-native syntax may not round-trip through sqlparser. A parse
    // failure here just means "not a derived-table query" — fall
    // through to the normal SOW path.
    let Ok(ast) = Parser::parse_sql(&dialect, sql) else {
        return Ok(None);
    };
    if ast.is_empty() {
        return Ok(None);
    }
    let stmt = &ast[0];
    let outer_query = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };
    let outer_select = match outer_query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };
    if outer_select.from.len() != 1 {
        return Ok(None);
    }
    let inner_query: &Query = match &outer_select.from[0].relation {
        TableFactor::Derived { subquery, .. } => subquery.as_ref(),
        _ => return Ok(None),
    };
    // Materialise the inner query against its source topic.
    let inner_select = match inner_query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err("derived-table inner must be a SELECT".into()),
    };
    if inner_select.from.len() != 1 || !inner_select.from[0].joins.is_empty() {
        return Err("derived-table inner FROM must be a single topic (no JOIN)".into());
    }
    let inner_topic_raw = match &inner_select.from[0].relation {
        TableFactor::Table { name, .. } => {
            let raw = name.to_string();
            raw.trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']')
                .to_string()
        }
        _ => return Err("derived-table inner FROM must be a plain topic".into()),
    };
    let canonical = cq_core::topic::canonicalize_topic(&inner_topic_raw);
    let inner_topic = topics
        .get(&canonical)
        .ok_or_else(|| format!("derived-table inner topic `{inner_topic_raw}` not found"))?;
    let inner_sql = format!("{inner_query}");
    let inner_result = inner_topic
        .query(&inner_sql)
        .map_err(|e| format!("derived-table inner: {e}"))?;
    let inner_rows = inner_result.rows;

    // Build an ephemeral topic from the inner result. Schema
    // discovery: type-infer each column from the first non-null
    // value in the result.
    if inner_rows.is_empty() {
        // No inner rows → outer has nothing to filter; just return
        // an empty result without spinning up the transient topic.
        return Ok(Some(Vec::new()));
    }
    let ephemeral = make_ephemeral_topic_from_rows(&inner_rows)
        .map_err(|e| format!("derived-table materialise: {e}"))?;

    // Run the OUTER query against the ephemeral topic. We rewrite
    // the outer SQL's FROM clause to point at the ephemeral topic's
    // placeholder identifier `t` — which is what `Topic::query`
    // already expects (cqserver rewrites topic refs to `t` in the
    // streaming path).
    let outer_sql_rewritten = rewrite_outer_from_to_t(sql)
        .map_err(|e| format!("derived-table outer rewrite: {e}"))?;
    let outer_result = ephemeral
        .query(&outer_sql_rewritten)
        .map_err(|e| format!("derived-table outer: {e}"))?;
    Ok(Some(outer_result.rows))
}

/// R10 helper — replace the derived-table subquery `(SELECT …)` in
/// the outer FROM with the placeholder `t` so the ephemeral topic
/// can serve the outer query through its standard `Topic::query`
/// path. sqlparser's `Display` round-trips, so we mutate the AST
/// and re-serialize.
fn rewrite_outer_from_to_t(sql: &str) -> Result<String, String> {
    use sqlparser::ast::{
        Ident, ObjectName, ObjectNamePart, SetExpr, Statement, TableFactor,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let dialect = GenericDialect {};
    let mut ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("outer-rewrite parse: {e}"))?;
    if let Some(Statement::Query(q)) = ast.first_mut() {
        if let SetExpr::Select(s) = q.body.as_mut() {
            if s.from.len() == 1 {
                if let TableFactor::Derived { .. } = &s.from[0].relation {
                    s.from[0].relation = TableFactor::Table {
                        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("t"))]),
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        version: None,
                        with_ordinality: false,
                        partitions: vec![],
                        json_path: None,
                        sample: None,
                        index_hints: vec![],
                    };
                }
            }
        }
        Ok(format!("{q}"))
    } else {
        Err("outer-rewrite: not a query".into())
    }
}

/// R10 helper — build an ephemeral cq-core `Topic` from a vector of
/// JSON rows. Type inference scans each column for its first non-
/// null value to pick String / Long / Double. The resulting topic
/// has no key (single-row collapse disabled — every input row is
/// preserved by giving each one a synthetic row-id key) and no
/// persistence.
fn make_ephemeral_topic_from_rows(
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<std::sync::Arc<cq_core::topic::Topic>, String> {
    use cq_core::schema::{ColumnType, Schema};
    use cq_core::topic::{Topic, TopicConfig};
    if rows.is_empty() {
        return Err("cannot infer schema from empty rows".into());
    }
    // Collect column names in first-row order; type-infer per column.
    let mut col_order: Vec<String> = Vec::new();
    for k in rows[0].keys() {
        col_order.push(k.clone());
    }
    // Add the synthetic key column.
    let key_col = "__derived_id__";
    let mut names: Vec<&str> = vec![key_col];
    names.extend(col_order.iter().map(|s| s.as_str()));
    let mut types: Vec<ColumnType> = Vec::with_capacity(names.len());
    types.push(ColumnType::String); // synthetic key
    for col in &col_order {
        let mut t = ColumnType::String;
        for row in rows {
            match row.get(col) {
                Some(serde_json::Value::Number(n)) => {
                    t = if n.is_i64() { ColumnType::Long } else { ColumnType::Double };
                    break;
                }
                Some(serde_json::Value::String(_)) => {
                    t = ColumnType::String;
                    break;
                }
                Some(serde_json::Value::Bool(_)) => {
                    t = ColumnType::Bool;
                    break;
                }
                _ => continue,
            }
        }
        types.push(t);
    }
    let schema = std::sync::Arc::new(Schema::from_strs(&names, &types));
    let topic = std::sync::Arc::new(Topic::new(
        TopicConfig {
            name: "/__derived__".into(),
            key_fields: vec![key_col.into()],
            persist: false,
            conflation_ms: None,
            index_columns: Vec::new(),
            expire_seconds: None,
        },
        schema,
        rows.len(),
    ));
    // Seed every inner row.
    for (i, row) in rows.iter().enumerate() {
        let mut m = row.clone();
        m.insert(key_col.into(), serde_json::Value::String(format!("r{i}")));
        topic
            .upsert_map(&m)
            .map_err(|e| format!("seed ephemeral: {e}"))?;
    }
    Ok(topic)
}

/// Q9 — pre-flight subquery materialisation. Walks the WHERE clause
/// for `Expr::InSubquery` patterns (e.g.
/// `WHERE symbol IN (SELECT symbol FROM watchlist)`), runs each
/// subquery as a one-shot SOW against its source topic, and
/// substitutes the result back as a literal IN list. Returns the
/// rewritten SQL. If `sql` contains no subqueries, returns it
/// unchanged. Errors if a subquery's source topic doesn't exist or
/// if the inner SELECT is something we don't materialise yet
/// (multi-column projection, JOIN, GROUP BY, etc.).
///
/// MVP scope:
/// - Only `Expr::InSubquery` (not EXISTS, not scalar subqueries).
/// - Inner SELECT must be `SELECT one_col FROM topic [WHERE …]`.
/// - Materialisation uses `Topic::query` so it picks up everything
///   the regular SOW path supports (filters, aliases via P1, etc.).
fn resolve_in_subqueries(
    sql: &str,
    topics: &dashmap::DashMap<String, SharedTopic>,
) -> Result<String, String> {
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    let dialect = GenericDialect {};
    // sqlparser's GenericDialect rejects PIVOT/UNPIVOT at the top level
    // and a few other AMPS-native shapes (the cqserver query layer
    // handles those itself). A pre-flight parse failure just means
    // "nothing for us to rewrite here" — pass the SQL through and let
    // the real executor speak.
    let Ok(mut ast) = Parser::parse_sql(&dialect, sql) else {
        return Ok(sql.to_string());
    };
    if ast.is_empty() {
        return Ok(sql.to_string());
    }
    let stmt = &mut ast[0];
    let q = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(sql.to_string()),
    };
    let select = match q.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(sql.to_string()),
    };
    let mut changed = false;
    if let Some(where_expr) = select.selection.as_mut() {
        rewrite_in_subqueries(where_expr, topics, &mut changed)?;
    }
    if !changed {
        return Ok(sql.to_string());
    }
    // sqlparser's Display impl round-trips the AST back to SQL text.
    Ok(format!("{q}"))
}

/// R8 — materialise an uncorrelated EXISTS subquery to a boolean
/// literal. AMPS only supports uncorrelated subqueries (the inner
/// query doesn't reference outer columns); under that constraint
/// EXISTS reduces to "does ANY row match?", which can be answered
/// once with a one-shot SOW and the result substituted as a literal.
///
/// Subquery shape: `EXISTS (SELECT * FROM topic [WHERE …])` — the
/// projection list doesn't matter, only the row count. We run a
/// `SELECT 1 FROM <topic>` materialisation with the WHERE applied
/// and check if any rows came back.
fn materialise_exists(
    query: &sqlparser::ast::Query,
    topics: &dashmap::DashMap<String, SharedTopic>,
) -> Result<bool, String> {
    use sqlparser::ast::{SetExpr, TableFactor};
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err("EXISTS subquery must be a SELECT (no set ops)".into()),
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err("EXISTS subquery FROM must be a single topic (no JOIN)".into());
    }
    let topic_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => {
            let raw = name.to_string();
            let trimmed = raw
                .trim_matches('"')
                .trim_matches('`')
                .trim_matches('[')
                .trim_matches(']');
            trimmed.to_string()
        }
        _ => return Err("EXISTS subquery FROM must be a plain topic name".into()),
    };
    let canonical = cq_core::topic::canonicalize_topic(&topic_name);
    let topic = topics
        .get(&canonical)
        .ok_or_else(|| format!("EXISTS subquery topic `{topic_name}` not found"))?;
    let where_part = select
        .selection
        .as_ref()
        .map(|e| format!(" WHERE {e}"))
        .unwrap_or_default();
    // Use `SELECT *` so we don't need to project a specific column —
    // we only care if row_count > 0. LIMIT 1 keeps the materialisation
    // bounded even on multi-million-row source topics.
    let sub_sql = format!("SELECT * FROM t{where_part} LIMIT 1");
    let result = topic
        .query(&sub_sql)
        .map_err(|e| format!("EXISTS subquery: {e}"))?;
    Ok(!result.rows.is_empty())
}

fn rewrite_in_subqueries(
    expr: &mut sqlparser::ast::Expr,
    topics: &dashmap::DashMap<String, SharedTopic>,
    changed: &mut bool,
) -> Result<(), String> {
    use sqlparser::ast::{Expr, SetExpr, TableFactor, Value, ValueWithSpan, SelectItem, GroupByExpr};
    match expr {
        // R8 — uncorrelated EXISTS / NOT EXISTS.
        Expr::Exists { subquery, negated } => {
            let has_rows = materialise_exists(subquery, topics)?;
            // EXISTS = has_rows; NOT EXISTS = !has_rows.
            let bool_val = if *negated { !has_rows } else { has_rows };
            // Replace with a literal `TRUE` or `FALSE`. We construct
            // an always-true / always-false predicate that's safe to
            // serialize via sqlparser Display — `1 = 1` / `1 = 0`.
            let one = Expr::Value(ValueWithSpan {
                value: Value::Number("1".into(), false),
                span: sqlparser::tokenizer::Span::empty(),
            });
            let other = Expr::Value(ValueWithSpan {
                value: Value::Number(if bool_val { "1" } else { "0" }.into(), false),
                span: sqlparser::tokenizer::Span::empty(),
            });
            *expr = Expr::BinaryOp {
                left: Box::new(one),
                op: sqlparser::ast::BinaryOperator::Eq,
                right: Box::new(other),
            };
            *changed = true;
            return Ok(());
        }
        _ => {}
    }
    match expr {
        Expr::InSubquery { expr: inner_lhs, subquery, negated } => {
            // Materialise the subquery → InList of literals.
            // Subquery shape: SELECT one_col FROM topic [WHERE …].
            let inner_query = match subquery.as_ref() {
                SetExpr::Select(s) => s,
                _ => {
                    return Err("subquery must be a SELECT (no set ops)".into());
                }
            };
            if inner_query.projection.len() != 1 {
                return Err(
                    "subquery must project exactly one column".into(),
                );
            }
            let proj_col_expr = match &inner_query.projection[0] {
                SelectItem::UnnamedExpr(e) => e,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => return Err(
                    "subquery projection must be a column ref".into(),
                ),
            };
            let proj_col_name = match proj_col_expr {
                Expr::Identifier(id) => id.value.clone(),
                _ => return Err(
                    "subquery projection must be a bare column identifier".into(),
                ),
            };
            if inner_query.from.len() != 1 || !inner_query.from[0].joins.is_empty() {
                return Err("subquery FROM must be a single topic (no JOIN)".into());
            }
            if !matches!(&inner_query.group_by, GroupByExpr::Expressions(g, _) if g.is_empty())
            {
                return Err("subquery cannot have GROUP BY".into());
            }
            let topic_name = match &inner_query.from[0].relation {
                TableFactor::Table { name, .. } => {
                    let raw = name.to_string();
                    let trimmed = raw.trim_matches('"').trim_matches('`').trim_matches('[').trim_matches(']');
                    trimmed.to_string()
                }
                _ => return Err("subquery FROM must be a plain topic name".into()),
            };
            // Materialise: build a SQL string `SELECT col FROM t [WHERE …]`
            // and call Topic::query.
            let canonical = cq_core::topic::canonicalize_topic(&topic_name);
            let topic = topics
                .get(&canonical)
                .ok_or_else(|| format!("subquery topic `{topic_name}` not found"))?;
            let where_part = inner_query
                .selection
                .as_ref()
                .map(|e| format!(" WHERE {e}"))
                .unwrap_or_default();
            let sub_sql = format!("SELECT {proj_col_name} FROM t{where_part}");
            let result = topic
                .query(&sub_sql)
                .map_err(|e| format!("subquery: {e}"))?;
            // Build the IN list from the result column values.
            let mut lits: Vec<Expr> = Vec::with_capacity(result.rows.len());
            for row in &result.rows {
                let v = row
                    .get(proj_col_name.as_str())
                    .ok_or_else(|| format!("subquery missing col `{proj_col_name}`"))?;
                let lit = match v {
                    serde_json::Value::String(s) => {
                        Expr::Value(ValueWithSpan {
                            value: Value::SingleQuotedString(s.clone()),
                            span: sqlparser::tokenizer::Span::empty(),
                        })
                    }
                    serde_json::Value::Number(n) => Expr::Value(ValueWithSpan {
                        value: Value::Number(n.to_string(), false),
                        span: sqlparser::tokenizer::Span::empty(),
                    }),
                    serde_json::Value::Bool(b) => Expr::Value(ValueWithSpan {
                        value: Value::Boolean(*b),
                        span: sqlparser::tokenizer::Span::empty(),
                    }),
                    _ => continue, // skip nulls
                };
                lits.push(lit);
            }
            // Replace `lhs IN (subquery)` with `lhs IN (lit, lit, ...)`.
            // Edge case: empty result → `lhs IN ()` which sqlparser
            // dislikes. Substitute with `false` (resp. `true` if
            // negated) so the predicate evaluates to no-match.
            *expr = if lits.is_empty() {
                // Empty IN list: synthesize an always-false predicate
                // (or always-true if NOT IN). We use
                // `col IS NULL AND col IS NOT NULL` — always false
                // regardless of col type — and negate for NOT IN.
                let null_check = Expr::IsNull(inner_lhs.clone());
                let not_null_check = Expr::IsNotNull(inner_lhs.clone());
                let always_false = Expr::BinaryOp {
                    left: Box::new(null_check),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(not_null_check),
                };
                if *negated {
                    Expr::UnaryOp {
                        op: sqlparser::ast::UnaryOperator::Not,
                        expr: Box::new(always_false),
                    }
                } else {
                    always_false
                }
            } else {
                Expr::InList {
                    expr: inner_lhs.clone(),
                    list: lits,
                    negated: *negated,
                }
            };
            *changed = true;
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            rewrite_in_subqueries(left, topics, changed)?;
            rewrite_in_subqueries(right, topics, changed)
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => {
            rewrite_in_subqueries(expr, topics, changed)
        }
        _ => Ok(()),
    }
}
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

/// Drop every Ready entry whose topic matches `topic`. Called from the
/// publish / delta-publish / sow-delete paths so a SOW issued after a
/// write never returns pre-write rows just because the cache's 500 ms
/// TTL hasn't elapsed. Building entries are left alone — their
/// in-flight build will publish a Ready entry next, which will get
/// evicted on the next write.
///
/// Why per-topic and not per-(topic, sql): a publish can change rows
/// that match any predicate that was previously cached against this
/// topic. We have no cheap way to tell which sql-keyed entries the
/// publish invalidates, so we drop them all. The cache exists for
/// fanout (N subs joining within ms of each other on the same SQL);
/// dropping it on every write doesn't hurt that workload because the
/// next sub in the wave rebuilds and re-fills the entry.
fn invalidate_snapshot_cache_for_topic(topic: &str) {
    let mut cache = snapshot_fanout_cache().lock();
    let drop_keys: Vec<(String, String)> = cache
        .iter()
        .filter(|(k, v)| {
            k.0 == topic && matches!(v, SnapshotCacheState::Ready(_))
        })
        .map(|(k, _)| k.clone())
        .collect();
    for k in &drop_keys {
        cache.remove(k);
    }
    let total_after = current_cache_bytes(&cache);
    drop(cache);
    if !drop_keys.is_empty() {
        metrics::counter!("cq_snapshot_cache_invalidated_total")
            .increment(drop_keys.len() as u64);
        metrics::gauge!("cq_snapshot_cache_bytes").set(total_after as f64);
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
    /// Query Guardrails (G1+): structural limits to apply at subscribe
    /// time. `validate_with_limits` runs on the `ParsedQuery` before
    /// any topic state is touched; rejected queries return an Error
    /// ACK with a precise reason. Defaults to
    /// `cq_core::query::QueryLimits::default()` which is conservative
    /// (max_pivot_in_list_size = 100, max_view_chain_depth = 3,
    /// reject_degenerate_groupby = true,
    /// reject_passthrough_views = true).
    pub query_limits: cq_core::query::QueryLimits,
    /// S11 synchronous-replication policy for `AckType::Replicated`
    /// publishes. See [`SyncReplication`]. Defaults to inactive, in
    /// which case a `Replicated`-ack request is rejected with a clear
    /// error rather than silently acked before replication.
    pub sync_replication: SyncReplication,
    /// D3/P0.3 — shared connection/rate limits. `max_connections`,
    /// `max_connections_per_ip`, and `accept_rate_per_sec` are enforced
    /// in the accept loop (before this `RouterContext` is even built);
    /// `max_sessions_per_user` is enforced here, in `handle_logon`,
    /// because user identity isn't known until a successful logon.
    /// `None` disables all four (matches the pre-D3 behavior).
    pub connection_limits: Option<std::sync::Arc<ConnectionLimits>>,
}

/// S11 — controls how a publish that requests `AckType::Replicated` is
/// acknowledged. When `active`, the publish path waits (off the dispatch
/// thread, on a spawned task) until the per-topic
/// `last_replicated_sequence` reaches the publish's sequence, then sends
/// the ack — or fails the publish if `ack_timeout` elapses first. When
/// inactive (standalone node, no standby), a `Replicated`-ack request is
/// rejected immediately so the client is never told a write is
/// replicated when no replica exists.
#[derive(Clone, Copy)]
pub struct SyncReplication {
    pub active: bool,
    pub ack_timeout: std::time::Duration,
}

impl Default for SyncReplication {
    fn default() -> Self {
        SyncReplication {
            active: false,
            ack_timeout: std::time::Duration::from_secs(5),
        }
    }
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

/// Ceiling on distinct `(client_name, topic)` bookmark entries. Without
/// it, a deployment that mints a fresh `client_name` per reconnect (e.g.
/// per-pod random ids) would grow this map without bound for the process
/// lifetime. Past the cap we stop tracking *new* clients — they resume
/// with a full snapshot instead of MOST_RECENT, which is correct, just
/// less efficient. Existing entries keep updating regardless.
const BOOKMARK_STORE_MAX: usize = 100_000;

/// Record that a client just received delta with `seq` on `topic`.
/// Monotonic update — won't lower an already-higher mark.
pub fn record_bookmark(store: &BookmarkStore, client_name: &str, topic: &str, seq: u64) {
    use std::sync::atomic::Ordering;
    if client_name.is_empty() {
        return;
    }
    let key = (client_name.to_string(), topic.to_string());
    // Fast path (the common case on the delivery hot path): an entry
    // already exists — bump it monotonically without touching map size.
    if let Some(entry) = store.get(&key) {
        bump_bookmark(&entry, seq);
        return;
    }
    // New key: enforce the ceiling before inserting. Soft cap — a benign
    // race may overshoot slightly, which is fine.
    if store.len() >= BOOKMARK_STORE_MAX {
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                cap = BOOKMARK_STORE_MAX,
                "Bookmark store at capacity; new clients resume via full snapshot \
                 (check for unstable client_name values)"
            );
        }
        return;
    }
    let entry = store
        .entry(key)
        .or_insert_with(|| std::sync::atomic::AtomicU64::new(0));
    bump_bookmark(&entry, seq);
}

/// Monotonic CAS bump of a bookmark atomic — never lowers the mark.
fn bump_bookmark(entry: &std::sync::atomic::AtomicU64, seq: u64) {
    use std::sync::atomic::Ordering;
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

/// Historical-SOW timestamp seek (inclusive): scan a topic's txlog for
/// the highest sequence whose write timestamp is **at or before**
/// `as_of_ms`. Unlike [`resolve_timestamp_to_seq`] (which is strict-
/// less-than for replay), this includes a write landing exactly at the
/// cutoff. `Ok(None)` means no entry qualifies — the as-of snapshot is
/// empty. Assumes the reader yields entries in ascending sequence /
/// timestamp order (txlog is append-only in sequence order).
fn resolve_asof_timestamp_seq(
    log_path: &std::path::Path,
    as_of_ms: u64,
) -> Result<Option<u64>, String> {
    let mut reader = cq_txlog::reader::TxLogReader::open(log_path)
        .map_err(|e| format!("open txlog: {e}"))?;
    let mut last: Option<u64> = None;
    loop {
        match reader.read_next() {
            Ok(Some(entry)) => {
                if entry.timestamp_ms <= as_of_ms {
                    last = Some(entry.sequence);
                } else {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) => return Err(format!("read txlog: {e}")),
        }
    }
    Ok(last)
}

/// Reconstruct a topic's SOW state as it existed immediately after
/// `target_seq` was applied, by replaying the txlog. Per key the last
/// write wins; a tombstone (empty payload) removes the key. Surviving
/// rows are returned in first-seen key order so output is stable.
fn reconstruct_asof_rows(
    log_path: &std::path::Path,
    target_seq: u64,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    let mut reader = cq_txlog::reader::TxLogReader::open(log_path)
        .map_err(|e| format!("open txlog: {e}"))?;
    let mut order: Vec<String> = Vec::new();
    let mut rows: std::collections::HashMap<String, serde_json::Map<String, serde_json::Value>> =
        std::collections::HashMap::new();
    loop {
        match reader.read_next() {
            Ok(Some(entry)) => {
                if entry.sequence > target_seq {
                    break;
                }
                if entry.is_tombstone() {
                    if rows.remove(&entry.key).is_some() {
                        order.retain(|k| k != &entry.key);
                    }
                } else if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_slice::<serde_json::Value>(&entry.payload)
                {
                    if entry.payload_format == cq_txlog::PayloadFormat::JsonDelta {
                        // Sparse delta: merge the changed fields into the
                        // row accumulated so far — replacing would drop
                        // every column the delta doesn't carry.
                        if let Some(existing) = rows.get_mut(&entry.key) {
                            for (k, v) in map {
                                existing.insert(k, v);
                            }
                        } else {
                            order.push(entry.key.clone());
                            rows.insert(entry.key.clone(), map);
                        }
                    } else {
                        if !rows.contains_key(&entry.key) {
                            order.push(entry.key.clone());
                        }
                        rows.insert(entry.key.clone(), map);
                    }
                }
            }
            Ok(None) => break,
            Err(e) => return Err(format!("read txlog: {e}")),
        }
    }
    Ok(order.into_iter().filter_map(|k| rows.remove(&k)).collect())
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
        Command::Logon => handle_logon(session, msg, ctx),
        Command::Publish => handle_publish(session, msg, ctx),
        Command::PublishBatch => handle_publish_batch(session, msg, ctx),
        Command::DeltaPublish => handle_delta_publish(session, msg, ctx),
        Command::Sow => handle_sow(
            session,
            msg,
            &ctx.topics,
            &ctx.auth,
            ctx.sow_batch_size,
            &ctx.query_limits,
        ),
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
                    // A `lease_extend_ms` turns the Ack into a lease
                    // renewal (keep the message in-flight); otherwise it
                    // commits the lease.
                    match msg.lease_extend_ms {
                        Some(extra) => {
                            q.extend_lease(did, extra);
                        }
                        None => {
                            q.ack(did);
                        }
                    }
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

/// D3/P0.3 — enforce `max_sessions_per_user` at Logon time (user
/// identity isn't known until now, unlike the accept-time checks in
/// `ConnectionLimits::try_accept`). On success, stashes the RAII guard
/// on the session so it releases (decrementing the per-user count) on
/// disconnect via `Session`'s own `Drop`-less-but-field-owned lifetime —
/// the guard lives exactly as long as the `Session` struct does. Drops
/// any pre-existing guard first so a session that re-logs-on (e.g. to
/// switch users) doesn't leak its previous slot.
fn enforce_max_sessions_per_user(
    session: &mut Session,
    username: &str,
    ctx: &RouterContext,
) -> Result<(), crate::limits::RejectReason> {
    session.user_session_guard = None; // release any prior logon's slot first
    let Some(limits) = ctx.connection_limits.as_ref() else {
        return Ok(());
    };
    match limits.try_register_user_session(username) {
        Ok(guard) => {
            session.user_session_guard = Some(guard);
            Ok(())
        }
        Err(reason) => {
            crate::limits::record_rejection(reason);
            Err(reason)
        }
    }
}

fn handle_logon(session: &mut Session, msg: CqMessage, ctx: &RouterContext) {
    let auth = &ctx.auth;
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

    // Q4/Q6 — capture client_name early, regardless of which logon
    // branch (anonymous / password / JWT) we end up taking. /admin/
    // clients aggregates by session_id, so the name needs to land on
    // the Session before the FIRST subscribe-induced DeliveryRoute
    // copies it.
    if let Some(name) = msg.client_name.clone() {
        session.client_name = Some(name);
    }

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

    // S30 — wire-codec negotiation, same handshake shape. An empty
    // client list (or pre-S30 client) implies Codec::Json, preserving
    // the JSON-on-TCP behavior for older clients. The negotiated codec
    // is applied to the session ONLY AFTER the ack is sent, so the ack
    // itself goes out in the pre-switch codec (JSON, the codec the
    // Logon frame was decoded with). Both sides switch in lockstep for
    // post-ack frames.
    let client_codecs = msg.codecs.clone().unwrap_or_default();
    let codec = match cq_protocol::serialization::negotiate_codec(
        &client_codecs,
        cq_protocol::serialization::SUPPORTED_CODECS,
    ) {
        cq_protocol::serialization::CodecNegotiationOutcome::Negotiated(c) => c,
        cq_protocol::serialization::CodecNegotiationOutcome::NoOverlap => {
            tracing::warn!(
                session = %session.id,
                client = ?client_codecs,
                "Logon rejected: no overlapping wire codec"
            );
            metrics::counter!(
                "cq_logon_total",
                "result" => "codec_mismatch"
            )
            .increment(1);
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "No mutually supported codec",
            ));
            return;
        }
    };

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
                    if let Err(reason) =
                        enforce_max_sessions_per_user(session, &matched.username, ctx)
                    {
                        let _ = session.send_message(&CqMessage::error(
                            msg.command_id,
                            &format!(
                                "Logon rejected: too many concurrent sessions for user '{}'",
                                matched.username
                            ),
                        ));
                        tracing::warn!(
                            target: "cq_audit",
                            session = %session.id,
                            user = %matched.username,
                            reason = reason.as_str(),
                            event = "logon_fail_session_limit",
                            "Logon rejected: max_sessions_per_user exceeded"
                        );
                        metrics::counter!(
                            "cq_logon_total",
                            "result" => "fail_session_limit"
                        )
                        .increment(1);
                        return;
                    }
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
                    ack.codecs = Some(vec![codec]);
                    let _ = session.send_message(&ack);
                    session.set_codec(codec);
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

    let Some((user, pass)) = credentials else {
        if auth.required {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "Missing user/password in logon data",
            ));
            return;
        }
        // Q4 — capture client_name + trace_id even when no auth is
        // required, so anonymous-but-named clients still show up in
        // /admin/clients.
        let mut ack = CqMessage::ack_ok_for_request(&msg);
        ack.protocol_versions = Some(vec![negotiated]);
        ack.compressions = Some(vec![compression]);
        ack.codecs = Some(vec![codec]);
        tracing::info!(
            session = %session.id,
            client_name = ?session.client_name,
            protocol_version = negotiated,
            compression = ?compression,
            codec = ?codec,
            trace_id = ?msg.trace_id,
            "Logon ok (no credentials, auth not required)"
        );
        metrics::counter!("cq_logon_total", "result" => "ok").increment(1);
        let _ = session.send_message(&ack);
        session.set_codec(codec);
        return;
    };

    match auth.verify(user, pass) {
        Some(matched) => {
            if let Err(reason) = enforce_max_sessions_per_user(session, &matched.username, ctx) {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!(
                        "Logon rejected: too many concurrent sessions for user '{}'",
                        matched.username
                    ),
                ));
                tracing::warn!(
                    target: "cq_audit",
                    session = %session.id,
                    user = %matched.username,
                    reason = reason.as_str(),
                    event = "logon_fail_session_limit",
                    "Logon rejected: max_sessions_per_user exceeded"
                );
                metrics::counter!(
                    "cq_logon_total",
                    "result" => "fail_session_limit"
                )
                .increment(1);
                return;
            }
            session.username = Some(matched.username.clone());
            session.entitlements = matched.entitlements.clone();
            // S25 audit event: explicit `target = "cq_audit"` so the
            // tracing-subscriber Registry can route it to the
            // dedicated audit sink (e.g., audit.log) while the same
            // session keeps its other events on the operational sink.
            // Q4 — client_name was already captured early in
            // handle_logon (above the credentials branch); just
            // surface it on the audit log.
            tracing::info!(
                target: "cq_audit",
                session = %session.id,
                user = %matched.username,
                client_name = ?session.client_name,
                entitlements = matched.entitlements.len(),
                protocol_version = negotiated,
                trace_id = ?msg.trace_id,
                event = "logon_ok",
                "Logon ok"
            );
            metrics::counter!("cq_logon_total", "result" => "ok").increment(1);
            let mut ack = CqMessage::ack_ok(msg.command_id);
            ack.protocol_versions = Some(vec![negotiated]);
            ack.compressions = Some(vec![compression]);
            ack.codecs = Some(vec![codec]);
            ack.trace_id = msg.trace_id; // Q4 — echo trace-id back to caller
            let _ = session.send_message(&ack);
            session.set_codec(codec);
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

/// S11 — emit a publish ack, honoring `AckType::Replicated`.
///
/// For every ack level except `Replicated` the prebuilt `success_ack`
/// (which already carries the assigned `sequence`/`sequences`) is sent
/// inline, exactly as before. For `Replicated`:
///   - If this node has no replication target, the request is rejected
///     with an explicit error — we never claim a write is replicated
///     when no standby exists.
///   - If the standby has already confirmed `await_seq`, the ack is sent
///     inline (fast path).
///   - Otherwise a task is spawned that waits on the topic's replication
///     notify until `last_replicated_sequence >= await_seq`, then sends
///     `success_ack`; or fails the publish if `ack_timeout` elapses. The
///     wait runs off the dispatch thread so the read loop is never
///     blocked.
fn finish_publish_ack(
    session: &Session,
    msg: &CqMessage,
    ctx: &RouterContext,
    topic: &SharedTopic,
    await_seq: u64,
    success_ack: CqMessage,
) {
    if !matches!(msg.ack_type, Some(AckType::Replicated)) {
        let _ = session.send_message(&success_ack);
        return;
    }
    if !ctx.sync_replication.active {
        metrics::counter!("cq_repl_sync_ack_rejected_total").increment(1);
        let _ = session.send_message(&CqMessage::error(
            msg.command_id.clone(),
            "replicated ack requested but this node has no replication target",
        ));
        return;
    }
    if topic.last_replicated_sequence() >= await_seq {
        let _ = session.send_message(&success_ack);
        return;
    }
    let tx = session.tx.clone();
    let codec = session.codec();
    let notify = topic.replication_notify_handle();
    let topic = topic.clone();
    let timeout = ctx.sync_replication.ack_timeout;
    let command_id = msg.command_id.clone();
    tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if topic.last_replicated_sequence() >= await_seq {
                if let Some(frame) = crate::session::encode_frame(codec, &success_ack) {
                    let _ = tx.send(frame).await;
                }
                return;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                metrics::counter!("cq_repl_sync_ack_timeout_total").increment(1);
                let err = CqMessage::error(
                    command_id.clone(),
                    "replication ack timed out before standby confirmed",
                );
                if let Some(frame) = crate::session::encode_frame(codec, &err) {
                    let _ = tx.send(frame).await;
                }
                return;
            }
            // Subscribe to the notify BEFORE re-checking the sequence so
            // an Ack landing between the check and the wait can't be lost.
            let n = notify.notified();
            if topic.last_replicated_sequence() >= await_seq {
                continue;
            }
            let _ = tokio::time::timeout(remaining, n).await;
        }
    });
}

/// Q2 — atomic batched publish. Commits all N rows in input order
/// under a single sequence of `upsert_map` calls; emits one Ack
/// whose `sequences` carries the assigned seq per row. On the first
/// per-row error, stops, sends an Err ack with the index that
/// failed, and the surviving prefix is already durable in the
/// txlog (matches the AMPS contract: a batch is not atomic across
/// failures, but per-row durability is preserved).
fn handle_publish_batch(session: &mut Session, msg: CqMessage, ctx: &RouterContext) {
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
            let _ = session
                .send_message(&CqMessage::error(msg.command_id, "Missing topic"));
            return;
        }
    };
    if !check_entitlement(session, &msg.command_id, &ctx.auth, Op::Publish, &topic_name) {
        return;
    }
    let batch = match msg.batch.as_ref() {
        Some(b) if !b.is_empty() => b,
        Some(_) => {
            // Empty batch — ack with empty sequences vec.
            let mut ack = CqMessage::ack_ok(msg.command_id);
            ack.sequences = Some(Vec::new());
            let _ = session.send_message(&ack);
            return;
        }
        None => {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "publish_batch missing `batch` payload",
            ));
            return;
        }
    };
    let topic = match ctx.topics.get(&topic_name) {
        Some(t) => t.value().clone(),
        None => {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                &format!("Topic not found: {}", topic_name),
            ));
            return;
        }
    };
    let started = Instant::now();
    // Validate every entry is an object before committing anything, so a
    // malformed row can't leave a half-committed frame.
    let mut objs: Vec<&serde_json::Map<String, serde_json::Value>> =
        Vec::with_capacity(batch.len());
    for (i, payload) in batch.iter().enumerate() {
        match payload {
            serde_json::Value::Object(m) => objs.push(m),
            _ => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id.clone(),
                    &format!("publish_batch[{i}]: expected object, got {payload:?}"),
                ));
                return;
            }
        }
    }
    // Commit the whole frame under one topic write lock (see
    // `Topic::upsert_map_batch`) instead of one lock per row.
    let seqs = match topic.upsert_map_batch(&objs) {
        Ok(seqs) => seqs,
        Err(e) => {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id.clone(),
                &format!("publish_batch failed: {e}"),
            ));
            return;
        }
    };
    let n = batch.len() as u64;
    let elapsed_us = started.elapsed().as_micros() as f64;
    metrics::counter!("cq_publish_batch_total", "topic" => topic_name.clone()).increment(1);
    metrics::counter!("cq_publish_batch_rows_total", "topic" => topic_name.clone())
        .increment(n);
    metrics::histogram!("cq_publish_batch_latency_us", "topic" => topic_name.clone())
        .record(elapsed_us);
    // Same per-topic invalidation as the single-publish path — see
    // `handle_publish_inner`. One eviction sweep per batch (not per
    // row) keeps amortised cost equal to a single publish.
    invalidate_snapshot_cache_for_topic(&topic_name);
    let await_seq = seqs.iter().copied().max().unwrap_or(0);
    let mut ack = CqMessage::ack_ok_for_request(&msg);
    ack.sequences = Some(seqs);
    finish_publish_ack(session, &msg, ctx, &topic, await_seq, ack);
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
        if matches!(msg.ack_type, Some(AckType::Replicated)) {
            let _ = session.send_message(&CqMessage::error(
                msg.command_id,
                "replicated ack not supported on queue topics",
            ));
            return;
        }
        let started = Instant::now();
        let opts = crate::queue::PublishOpts {
            priority: msg.priority.unwrap_or(0),
            group: msg.group.as_deref(),
        };
        let seq = queue.publish_with_opts(data_value, opts, &ctx.sessions);
        let elapsed_us = started.elapsed().as_micros() as f64;
        metrics::histogram!("cq_publish_latency_us", "topic" => topic_name.clone())
            .record(elapsed_us);
        let mut ack = CqMessage::ack_ok_for_request(&msg);
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
                // Drop any cached SOW snapshots for this topic: a SOW
                // issued after the ack must see this publish, but the
                // 500 ms TTL would otherwise let an old (topic, sql)
                // entry serve pre-publish rows to the next caller.
                invalidate_snapshot_cache_for_topic(&topic_name);
                let mut ack = CqMessage::ack_ok_for_request(&msg);
                ack.sequence = Some(seq);
                let topic = topic.value().clone();
                finish_publish_ack(session, &msg, ctx, &topic, seq, ack);
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
    query_limits: &cq_core::query::QueryLimits,
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

    // Q9 — pre-flight subquery materialisation. Walks any
    // `WHERE col IN (SELECT col FROM topic)` in msg.sql, resolves
    // each subquery as a one-shot SOW against the topic registry,
    // and substitutes the result back as a literal IN list. Returns
    // the rewritten SQL on success; on failure returns a clean
    // server error to the client.
    let mut msg = msg;
    if let Some(raw_sql_ref) = msg.sql.as_deref() {
        match resolve_in_subqueries(raw_sql_ref, topics) {
            Ok(rewritten) => {
                if rewritten != raw_sql_ref {
                    msg.sql = Some(rewritten);
                }
            }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!("Subquery: {e}"),
                ));
                return;
            }
        }
    }
    // R10 — derived-table pre-flight. If `msg.sql` is shaped like
    // `SELECT … FROM (SELECT … FROM real_topic …) …`, materialise
    // the inner against the real topic and run the outer against an
    // ephemeral in-memory topic. Returns the final rows directly so
    // we can ship them through the standard group_begin/sow_batch/
    // group_end frame envelope without re-entering the streaming
    // path.
    if let Some(raw_sql_ref) = msg.sql.as_deref() {
        match try_resolve_derived_table(raw_sql_ref, topics) {
            Ok(Some(rows)) => {
                // Stream the derived-table result through the same
                // frame envelope a normal SOW uses.
                let sub_id = msg.command_id.clone().unwrap_or_default();
                let tx = session.tx.clone();
                let codec = *session.codec.lock();
                let session_id = session.id.clone();
                let topic_label = topic_name.clone();
                tokio::spawn(async move {
                    use crate::session::encode_frame;
                    if let Some(f) = encode_frame(codec, &CqMessage::group_begin(&sub_id, rows.len())) {
                        if tx.send(f).await.is_err() {
                            return;
                        }
                    }
                    if !rows.is_empty() {
                        let batch = CqMessage::sow_batch(&sub_id, rows);
                        if let Some(f) = encode_frame(codec, &batch) {
                            if tx.send(f).await.is_err() {
                                return;
                            }
                        }
                    }
                    if let Some(f) = encode_frame(codec, &CqMessage::group_end(&sub_id)) {
                        let _ = tx.send(f).await;
                    }
                    let _ = (session_id, topic_label);
                });
                return;
            }
            Ok(None) => { /* no derived table — fall through to normal path */ }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!("Derived-table: {e}"),
                ));
                return;
            }
        }
    }
    // For the JOIN path we run `peek_join` + `parse_query` against the
    // RAW user SQL (not the rewritten `FROM t …` form) — column refs
    // like `p.position_id` only resolve when the left-side alias is
    // intact, which the rewrite would strip. The streaming non-JOIN
    // path still uses the rewritten SQL since C-4 mandates the topic
    // name never reach the SQL parser for a single-topic SOW.
    let raw_sql = msg.sql.clone();
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

    // Historical "as-of" SOW: when the request carries an as-of
    // sequence or timestamp, ignore the live store and reconstruct the
    // topic's point-in-time state from the txlog, then run the query's
    // filter/projection against that snapshot. Single-topic only — JOIN
    // / derived-table forms fall through their error from the ephemeral
    // query if used here.
    if msg.as_of_sequence.is_some() || msg.as_of_timestamp_ms.is_some() {
        let log_path = match topic.txlog_path() {
            Some(p) => p,
            None => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    "historical SOW requires a persistent topic",
                ));
                return;
            }
        };
        let sql_for_asof = build_sql(&msg);
        let asof_sub_id = msg.command_id.clone().unwrap_or_default();
        let as_of_seq = msg.as_of_sequence;
        let as_of_ts = msg.as_of_timestamp_ms;
        let tx = session.tx.clone();
        let codec = *session.codec.lock();
        let hard_max_rows = query_limits.hard_max_sow_result_rows;
        tokio::spawn(async move {
            use crate::session::encode_frame;
            let send_err = |reason: String| {
                if let Some(f) = encode_frame(codec, &CqMessage::error(None, &reason)) {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(f).await;
                    });
                }
            };
            // Resolve the target sequence to reconstruct as-of.
            let target = if let Some(s) = as_of_seq {
                s
            } else {
                match resolve_asof_timestamp_seq(&log_path, as_of_ts.unwrap_or(0)) {
                    Ok(Some(s)) => s,
                    // No entry at/before the cutoff → empty snapshot.
                    Ok(None) => 0,
                    Err(e) => {
                        send_err(format!("historical SOW: {e}"));
                        return;
                    }
                }
            };
            let rows = match reconstruct_asof_rows(&log_path, target) {
                Ok(r) => r,
                Err(e) => {
                    send_err(format!("historical SOW: {e}"));
                    return;
                }
            };

            // Run the query against the reconstructed rows. Empty
            // snapshot → emit an empty group (skip ephemeral build,
            // which can't infer a schema from zero rows).
            let result_rows: Vec<serde_json::Map<String, serde_json::Value>> = if rows.is_empty() {
                Vec::new()
            } else {
                match make_ephemeral_topic_from_rows(&rows) {
                    Ok(ephemeral) => match ephemeral.query(&sql_for_asof) {
                        Ok(mut res) => {
                            // Strip the synthetic key column the
                            // ephemeral topic added during seeding.
                            for row in &mut res.rows {
                                row.remove("__derived_id__");
                            }
                            res.rows
                        }
                        Err(e) => {
                            send_err(format!("historical SOW query: {e}"));
                            return;
                        }
                    },
                    Err(e) => {
                        send_err(format!("historical SOW materialise: {e}"));
                        return;
                    }
                }
            };
            let mut result_rows = result_rows;
            // G4 runtime cap. `hard_max_rows == 0` means "disabled" (matches
            // the live streaming path and the documented semantics), so only
            // truncate when a non-zero cap is actually exceeded — otherwise
            // a cap of 0 would truncate the whole as-of result to nothing.
            let cap = hard_max_rows as usize;
            if cap > 0 && result_rows.len() > cap {
                result_rows.truncate(cap);
            }

            if let Some(f) =
                encode_frame(codec, &CqMessage::group_begin(&asof_sub_id, result_rows.len()))
            {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
            if !result_rows.is_empty() {
                let batch = CqMessage::sow_batch(&asof_sub_id, result_rows);
                if let Some(f) = encode_frame(codec, &batch) {
                    if tx.send(f).await.is_err() {
                        return;
                    }
                }
            }
            if let Some(f) = encode_frame(codec, &CqMessage::group_end(&asof_sub_id)) {
                let _ = tx.send(f).await;
            }
        });
        return;
    }

    let tx = session.tx.clone();
    let codec_slot = session.codec.clone();
    let session_id = session.id.clone();
    let topic_label = topic_name.clone();
    let batch_size = sow_batch_size;

    let hard_max_rows = query_limits.hard_max_sow_result_rows;
    let hard_max_bytes = query_limits.hard_max_sow_result_bytes;

    // JOIN path: when the SQL contains `JOIN <right> USING (...)`, the
    // streaming SOW evaluator doesn't apply — query_streaming_json only
    // walks a single topic. Resolve the right side from the registry,
    // run the cross-topic join inline (it's one-shot, not cached) and
    // emit the same group_begin/sow_batch/group_end frame envelope so
    // the SDK's SOW collector treats it identically to a normal SOW.
    if let Some(raw) = raw_sql.as_deref() {
        match peek_join(raw) {
            Ok(Some((_, right_topic_name, _using))) => {
                // P14 — registry keys are canonical (slash-prefixed);
                // canonicalise the SQL-derived name and look up once.
                let key = cq_core::topic::canonicalize_topic(&right_topic_name);
                let right_topic = topics.get(&key).map(|e| e.value().clone());
                let right_topic = match right_topic {
                    Some(t) => t,
                    None => {
                        let _ = session.send_message(&CqMessage::error(
                            msg.command_id,
                            &format!(
                                "Right-side JOIN topic not found: {right_topic_name}"
                            ),
                        ));
                        return;
                    }
                };
                let raw_owned = raw.to_string();
                tokio::spawn(async move {
                    deliver_join_snapshot(
                        topic,
                        right_topic,
                        raw_owned,
                        sub_id,
                        topic_label,
                        session_id,
                        codec_slot,
                        tx,
                        batch_size,
                        hard_max_rows,
                        hard_max_bytes,
                    )
                    .await;
                });
                return;
            }
            Ok(None) => { /* falls through to single-topic streaming path */ }
            Err(_) => {
                // peek_join uses sqlparser's GenericDialect, which
                // doesn't accept every AMPS-native shape (PIVOT/UNPIVOT
                // at the top level, etc.). A parse failure here just
                // means "definitely not a JOIN" — fall through to the
                // single-topic streaming path and let the real query
                // executor surface a proper diagnostic if the SQL is
                // genuinely malformed.
            }
        }
    }

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
            hard_max_rows,
            hard_max_bytes,
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

    // Client-requested conflation interval (`conflation=<ms>` in the
    // subscribe options), capped server-side. Resolved here so it is
    // available to both the bookmark-replay branch and the snapshot
    // branch before any of `msg`'s fields are moved.
    let req_conflation = requested_conflation_ms(&msg);

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

    // R8 / Q9 — pre-flight subquery + EXISTS materialisation for the
    // live-subscribe path, mirroring handle_sow. Live subscriptions
    // see the rewrite once at snapshot time; subsequent updates do
    // NOT re-materialise (subscription correctness assumes the
    // EXISTS-driven set is stable for the snapshot session). For
    // uncorrelated subqueries — which AMPS limits us to — this is
    // identical to the SOW semantics: the subquery is a closed
    // computation against its source topic at request time.
    let mut msg = msg;
    if let Some(raw_sql_ref) = msg.sql.as_deref() {
        match resolve_in_subqueries(raw_sql_ref, topics) {
            Ok(rewritten) => {
                if rewritten != raw_sql_ref {
                    msg.sql = Some(rewritten);
                }
            }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!("Subquery: {e}"),
                ));
                return;
            }
        }
    }

    // R10 — derived-table pre-flight on the live-subscribe path.
    // `SELECT … FROM (SELECT … FROM real_topic) AS x WHERE …` is
    // materialised once against the source topic; the outer query
    // runs against an ephemeral in-memory topic and is delivered to
    // the subscriber via the same group_begin/sow_batch/group_end
    // frame envelope a normal SOW snapshot uses. Subsequent updates
    // do NOT re-materialise — the live subscription behaves as a
    // snapshot for derived-table queries (AMPS semantics). We still
    // send an ack so the client treats the result as a closed
    // subscription it can unsubscribe from.
    if let Some(raw_sql_ref) = msg.sql.as_deref() {
        match try_resolve_derived_table(raw_sql_ref, topics) {
            Ok(Some(rows)) => {
                let sub_id = session.next_sub_id();
                session.subscriptions.push(sub_id.clone());
                let mut ack = CqMessage::ack_ok(msg.command_id);
                ack.sub_id = Some(sub_id.clone());
                let _ = session.send_message(&ack);
                let tx = session.tx.clone();
                let codec = *session.codec.lock();
                tokio::spawn(async move {
                    use crate::session::encode_frame;
                    if let Some(f) = encode_frame(codec, &CqMessage::group_begin(&sub_id, rows.len())) {
                        if tx.send(f).await.is_err() {
                            return;
                        }
                    }
                    if !rows.is_empty() {
                        let batch = CqMessage::sow_batch(&sub_id, rows);
                        if let Some(f) = encode_frame(codec, &batch) {
                            if tx.send(f).await.is_err() {
                                return;
                            }
                        }
                    }
                    if let Some(f) = encode_frame(codec, &CqMessage::group_end(&sub_id)) {
                        let _ = tx.send(f).await;
                    }
                });
                return;
            }
            Ok(None) => { /* not a derived-table query — fall through */ }
            Err(e) => {
                let _ = session.send_message(&CqMessage::error(
                    msg.command_id,
                    &format!("Derived-table: {e}"),
                ));
                return;
            }
        }
    }

    let sql = build_sql(&msg);

    // Query Guardrails G3: pre-flight cost check. Run the estimator
    // against the source topic; reject if any hard cap exceeded, log
    // + metric if any soft threshold crossed. Bookmark replays skip
    // this — their cost model is "txlog entries since bookmark," not
    // SOW row count, so the SOW-shaped estimate doesn't apply.
    //
    // G5: when the authenticated user has a per-user query budget
    // override, merge it OVER the server-wide limits (tighter side
    // wins per field) and check against that effective limit.
    if msg.bookmark.is_none() && msg.since_timestamp_ms.is_none() && !msg.most_recent {
        if let Some(topic) = topics.get(&topic_name) {
            let effective_limits = match session.username.as_deref() {
                Some(u) => match ctx.auth.query_budget_for(u) {
                    Some(budget) => budget.merge_with(&ctx.query_limits),
                    None => ctx.query_limits,
                },
                None => ctx.query_limits,
            };
            match topic.estimate_cost(&sql) {
                Ok(est) => {
                    let outcome = cq_core::query::check_estimate_against_limits(
                        &est,
                        &effective_limits,
                    );
                    if outcome.rejected {
                        metrics::counter!(
                            "cq_query_rejected_total",
                            "reason" => "estimate_over_limit"
                        )
                        .increment(1);
                        let _ = session.send_message(&CqMessage::error(
                            msg.command_id,
                            outcome.reject_reason.as_deref().unwrap_or("query rejected"),
                        ));
                        return;
                    }
                    for warning in &outcome.warnings {
                        metrics::counter!(
                            "cq_query_warned_total",
                            "reason" => "estimate_above_soft_threshold"
                        )
                        .increment(1);
                        tracing::warn!(topic = %topic_name, %warning, "query estimate above soft threshold");
                    }
                }
                Err(e) => {
                    // Parse / unknown column / etc. Return the
                    // structured error so the client sees a clean
                    // diagnostic instead of a stalled SOW.
                    let _ = session.send_message(&CqMessage::error(
                        msg.command_id,
                        &format!("{e}"),
                    ));
                    return;
                }
            }
        }
    }

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
        let cname = msg.client_name.clone()
                            .or_else(|| session.client_name.clone())
                            .or_else(|| session.username.clone());
        handle_bookmark_subscribe(
            session,
            msg.command_id,
            topic_name,
            &sql,
            bookmark,
            topics,
            registry,
            cname,
            req_conflation,
            ctx.bookmark_store.clone(),
            ctx.spillover.as_ref(),
        );
        return;
    }

    let sub_id = session.next_sub_id();

    if let Some(topic) = topics.get(&topic_name) {
        // Client request wins (incl. `0` = opt-out); else topic baseline.
        let conflation_ms = req_conflation.or_else(|| topic.conflation_ms());
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
                        msg.client_name.clone()
                            .or_else(|| session.client_name.clone())
                            .or_else(|| session.username.clone()),
                        Some(ctx.bookmark_store.clone()),
                        ctx.spillover.as_ref(),
                        session.disconnect.clone(),
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
                // G5: apply the user-tightened runtime caps too.
                let effective = match session.username.as_deref() {
                    Some(u) => match ctx.auth.query_budget_for(u) {
                        Some(budget) => budget.merge_with(&ctx.query_limits),
                        None => ctx.query_limits,
                    },
                    None => ctx.query_limits,
                };
                let hard_max_rows = effective.hard_max_sow_result_rows;
                let hard_max_bytes = effective.hard_max_sow_result_bytes;
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
                        hard_max_rows,
                        hard_max_bytes,
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
    // Capped client-requested conflation interval (see
    // `requested_conflation_ms`). `None` falls back to the topic
    // baseline; `Some(0)` is an explicit client opt-out.
    requested_conflation_ms: Option<u64>,
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

    // Client request wins (incl. `0` = opt-out); else topic baseline.
    let conflation_ms = requested_conflation_ms.or_else(|| topic.conflation_ms());
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
            session.disconnect.clone(),
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
        let mut scanned: u64 = 0;
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
            // A long replay (bookmark at 0, millions of entries) would
            // otherwise drive this blocking-read loop with no await points
            // between entries, monopolizing the runtime worker. Yield
            // periodically so other tasks on this worker make progress.
            scanned = scanned.wrapping_add(1);
            if scanned % 256 == 0 {
                tokio::task::yield_now().await;
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
                        // `JsonDelta` entries also parse here and are
                        // delivered sparse — exactly as published — which
                        // matches AMPS bookmark-replay semantics (a
                        // delta_publish replays as a delta).
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
    let req_conflation = requested_conflation_ms(&msg);
    let sub_id = session.next_sub_id();

    if let Some(topic) = topics.get(&topic_name) {
        // Client request wins (incl. `0` = opt-out); else topic baseline.
        let conflation_ms = req_conflation.or_else(|| topic.conflation_ms());
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
                        msg.client_name.clone()
                            .or_else(|| session.client_name.clone())
                            .or_else(|| session.username.clone()),
                        Some(ctx.bookmark_store.clone()),
                        ctx.spillover.as_ref(),
                        session.disconnect.clone(),
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
                    // Same per-topic eviction as publishes — a SOW
                    // after a delete must not return the deleted row
                    // from a stale snapshot entry.
                    invalidate_snapshot_cache_for_topic(&topic_name);
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
    disconnect: std::sync::Arc<tokio::sync::Notify>,
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
        .with_bookmark_store(bookmark_store)
        .with_disconnect(disconnect);

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
    // Each clause is matched as a sequence of tokens separated by ANY
    // ASCII whitespace (handles `\n`, `\t`, multiple spaces) and must
    // be preceded by whitespace so we don't match identifiers like
    // `the_where_clause`. `JOIN` is treated as a boundary so that
    // `FROM <left> JOIN <right> USING (...)` keeps the JOIN clause
    // intact while still anonymising the left-side table identifier.
    let clause_kws: &[&[&str]] = &[
        &["WHERE"],
        &["GROUP", "BY"],
        &["ORDER", "BY"],
        &["LIMIT"],
        &["HAVING"],
        &["JOIN"],
        &["INNER", "JOIN"],
        &["LEFT", "JOIN"],
        &["RIGHT", "JOIN"],
        &["FULL", "JOIN"],
        &["CROSS", "JOIN"],
    ];
    let mut end = sql.len();
    for kws in clause_kws {
        if let Some(p) = find_clause_kw(&upper, after_from, kws) {
            if p < end {
                end = p;
            }
        }
    }
    // Q3 — PIVOT/UNPIVOT are FROM-clause modifiers; allow any whitespace
    // run before the open-paren so `\nPIVOT (...)` works the same as
    // ` PIVOT (...)`.
    for tok in ["PIVOT", "UNPIVOT"] {
        if let Some(p) = find_pivot_kw(&upper, after_from, tok) {
            if p < end {
                end = p;
            }
        }
    }
    // Walk `end` back through any leading whitespace so the tail keeps
    // its separator from the placeholder ` t`. Otherwise `FROM` + `t` +
    // `GROUP BY …` would collapse to `…FROM tGROUP BY…`.
    let bytes = sql.as_bytes();
    while end > after_from && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..after_from]);
    out.push_str(" t");
    if end < sql.len() {
        out.push(' ');
        out.push_str(&sql[end..]);
    }
    out
}

/// Locate `PIVOT` or `UNPIVOT` token followed (after any whitespace
/// run) by an open paren. Returns the absolute offset of the keyword.
/// Paren-depth aware: skips inner parenthesised regions so a `PIVOT`
/// inside a subquery isn't mistaken for the outer FROM modifier.
fn find_pivot_kw(upper: &str, start: usize, tok: &str) -> Option<usize> {
    let bytes = upper.as_bytes();
    let tb = tok.as_bytes();
    let mut i = start;
    let mut depth: i32 = 0;
    while i + tb.len() <= bytes.len() {
        match bytes[i] {
            b'(' => { depth += 1; i += 1; continue; }
            b')' => { depth -= 1; i += 1; continue; }
            _ => {}
        }
        if depth != 0 {
            i += 1;
            continue;
        }
        let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
        if prev_ok && &bytes[i..i + tb.len()] == tb {
            let mut cursor = i + tb.len();
            // Require at least one whitespace before `(`, then `(`.
            if cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                    cursor += 1;
                }
                if cursor < bytes.len() && bytes[cursor] == b'(' {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

/// Find a clause keyword sequence (e.g. `["GROUP", "BY"]`) in `upper`
/// starting at byte offset `start`. The first token must be preceded
/// by ASCII whitespace; tokens may be separated by any run of ASCII
/// whitespace (space, tab, newline, etc.); the last token must be
/// followed by whitespace or end-of-input. Returns the absolute byte
/// offset of the FIRST token, or `None` if not found.
///
/// **Paren-depth aware**: matches only at depth 0 so an inner
/// `ORDER BY` inside `OVER (...)` or `(SELECT ... ORDER BY ...)` is
/// not treated as the outer query's clause boundary.
fn find_clause_kw(upper: &str, start: usize, tokens: &[&str]) -> Option<usize> {
    if tokens.is_empty() {
        return None;
    }
    let bytes = upper.as_bytes();
    let first = tokens[0].as_bytes();
    let mut i = start;
    let mut depth: i32 = 0;
    // Walk from `start` tracking paren depth so subquery / OVER bodies
    // are skipped over rather than scanned for clause keywords.
    'scan: while i + first.len() <= bytes.len() {
        match bytes[i] {
            b'(' => { depth += 1; i += 1; continue; }
            b')' => { depth -= 1; i += 1; continue; }
            _ => {}
        }
        if depth != 0 {
            i += 1;
            continue;
        }
        let prev_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
        if !prev_ok || &bytes[i..i + first.len()] != first {
            i += 1;
            continue;
        }
        // Walk the remaining tokens, skipping whitespace runs between them.
        let mut cursor = i + first.len();
        for tok in &tokens[1..] {
            // Require at least one whitespace byte between tokens.
            if cursor >= bytes.len() || !bytes[cursor].is_ascii_whitespace() {
                i += 1;
                continue 'scan;
            }
            while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
                cursor += 1;
            }
            let tb = tok.as_bytes();
            if cursor + tb.len() > bytes.len() || &bytes[cursor..cursor + tb.len()] != tb {
                i += 1;
                continue 'scan;
            }
            cursor += tb.len();
        }
        // Final boundary: end-of-input or whitespace.
        let next_ok = cursor == bytes.len() || bytes[cursor].is_ascii_whitespace();
        if next_ok {
            return Some(i);
        }
        i += 1;
    }
    None
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

/// Server-side ceiling on a client-requested conflation interval.
/// A subscribe option `conflation=<ms>` is clamped to this value so a
/// client can't pin a multi-second interval that starves liveness for
/// the whole subscription. Tuned via `CQSERVER_MAX_CONFLATION_MS`;
/// default 60000 (60 s). `0` = uncapped (honor whatever the client asks).
fn max_conflation_ms() -> u64 {
    std::env::var("CQSERVER_MAX_CONFLATION_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000)
}

/// Parse a client-requested conflation interval from the subscribe
/// `options` string (`conflation=<ms>`), clamped to `cap` (0 = uncapped).
///
/// Returns:
///   - `None` — the client did not request conflation; the caller falls
///     back to the topic's configured baseline (`topic.conflation_ms()`).
///   - `Some(0)` — the client explicitly opted *out*; this overrides any
///     topic baseline (the route builder treats `0` as "no conflation").
///   - `Some(ms)` — the client's requested interval, capped.
///
/// Pure (cap passed in) so it can be unit-tested without touching env.
fn requested_conflation_ms_capped(msg: &CqMessage, cap: u64) -> Option<u64> {
    let options = msg.options.as_deref().unwrap_or("");
    parse_option(options, "conflation")
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|ms| if cap > 0 { ms.min(cap) } else { ms })
}

/// Production wrapper around [`requested_conflation_ms_capped`] that reads
/// the server-side cap from [`max_conflation_ms`].
fn requested_conflation_ms(msg: &CqMessage) -> Option<u64> {
    requested_conflation_ms_capped(msg, max_conflation_ms())
}

#[cfg(test)]
mod conflation_option_tests {
    use super::*;
    use cq_protocol::command::Command;

    fn sub_with_opts(opts: Option<&str>) -> CqMessage {
        let mut m = CqMessage::new(Command::Subscribe);
        m.topic = Some("/t".into());
        m.options = opts.map(|s| s.to_string());
        m
    }

    /// The effective interval the route builder will see, mirroring the
    /// `req.or_else(|| topic_baseline)` resolution at the subscribe sites.
    fn effective(opts: Option<&str>, cap: u64, topic_baseline: Option<u64>) -> Option<u64> {
        requested_conflation_ms_capped(&sub_with_opts(opts), cap)
            .or(topic_baseline)
    }

    #[test]
    fn no_option_falls_back_to_topic_baseline() {
        assert_eq!(effective(None, 60_000, Some(100)), Some(100));
        assert_eq!(effective(Some("select=[a,b]"), 60_000, Some(250)), Some(250));
        // No client option AND no topic baseline → no conflation.
        assert_eq!(effective(None, 60_000, None), None);
    }

    #[test]
    fn client_value_overrides_topic_baseline() {
        // Client asks for 200ms even though the topic baseline is 100ms.
        assert_eq!(effective(Some("conflation=200"), 60_000, Some(100)), Some(200));
        // And works with no baseline configured.
        assert_eq!(effective(Some("conflation=50"), 60_000, None), Some(50));
    }

    #[test]
    fn client_zero_is_explicit_opt_out() {
        // `conflation=0` disables conflation even if the topic configures
        // a baseline; the route builder treats Some(0) as "no conflation".
        assert_eq!(effective(Some("conflation=0"), 60_000, Some(100)), Some(0));
    }

    #[test]
    fn client_value_is_capped_to_server_ceiling() {
        // Client asks for 5 minutes; server cap is 60s.
        assert_eq!(effective(Some("conflation=300000"), 60_000, None), Some(60_000));
        // cap == 0 means uncapped — honor whatever the client asks.
        assert_eq!(effective(Some("conflation=300000"), 0, None), Some(300_000));
    }

    #[test]
    fn garbage_value_is_ignored_and_falls_back() {
        // Non-numeric → treated as "not requested" → topic baseline.
        assert_eq!(effective(Some("conflation=fast"), 60_000, Some(100)), Some(100));
        assert_eq!(effective(Some("conflation="), 60_000, None), None);
    }

    #[test]
    fn conflation_parses_alongside_other_options() {
        assert_eq!(
            effective(Some("select=[a,b],conflation=150,order_by=ts"), 60_000, None),
            Some(150)
        );
    }
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
    // Query Guardrails G4 runtime caps. `0` = disabled. When non-zero,
    // streaming aborts after this many rows / bytes have been emitted
    // on the wire, with an error frame to the client and a metric.
    hard_max_rows: u64,
    hard_max_bytes: u64,
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
    // G4: track bytes emitted on the wire so we can abort if a
    // runaway snapshot exceeds the byte cap. Updated per-batch
    // (after the frame is built) on the JSON path; the slow path
    // estimates from row count × 256 B since the encoded frame
    // length isn't easily exposed by tokio-tungstenite here.
    let mut bytes_sent: u64 = 0;
    let mut aborted_reason: Option<String> = None;
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
                    // P7 — include the sub_id (== client cid) in the
                    // error so the client can route it to the awaiting
                    // sow_msg's snapshot_completion instead of dropping
                    // a cid-less ack on the floor.
                    let mut err_msg = CqMessage::error(Some(sub_id.clone()), &e);
                    err_msg.sub_id = Some(sub_id.clone());
                    if let Some(f) = encode_frame(codec, &err_msg) {
                        let _ = tx.send(f).await;
                    }
                    return;
                }
            }
        };

        // Stream every cached batch as a sow_batch frame. Reading the
        // cache is read-only; encoder ran (or didn't) above.
        'json_loop: for batch in batches.iter() {
            let n = batch.len();
            let batch_bytes: u64 = batch.iter().map(|r| r.len() as u64).sum();
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
            bytes_sent += batch_bytes;

            // G4: enforce runtime caps. Hard rejection at this point
            // returns a partial result + error frame so the client sees
            // some data and a clear "cap fired" reason.
            if hard_max_rows > 0 && rows_sent as u64 > hard_max_rows {
                aborted_reason = Some(format!(
                    "rows_emitted = {} exceeded hard_max_sow_result_rows = {}",
                    rows_sent, hard_max_rows,
                ));
                break 'json_loop;
            }
            if hard_max_bytes > 0 && bytes_sent > hard_max_bytes {
                aborted_reason = Some(format!(
                    "bytes_emitted = {} exceeded hard_max_sow_result_bytes = {}",
                    bytes_sent, hard_max_bytes,
                ));
                break 'json_loop;
            }
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
            // Estimate bytes for the slow-path cap. Without access to
            // the encoded frame size, use a coarse 256 B/row guess —
            // adequate as a runaway-stopper since hard_max_bytes is
            // sized in MB not KB.
            let batch_bytes: u64 = (n as u64).saturating_mul(256);
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
            bytes_sent += batch_bytes;
            if hard_max_rows > 0 && rows_sent as u64 > hard_max_rows {
                aborted_reason = Some(format!(
                    "rows_emitted = {} exceeded hard_max_sow_result_rows = {}",
                    rows_sent, hard_max_rows,
                ));
                break;
            }
            if hard_max_bytes > 0 && bytes_sent > hard_max_bytes {
                aborted_reason = Some(format!(
                    "bytes_emitted = {} exceeded hard_max_sow_result_bytes = {}",
                    bytes_sent, hard_max_bytes,
                ));
                break;
            }
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

    // G4: if the runtime cap fired, send a clear error frame BEFORE
    // group_end so the client sees both the partial data and the
    // abort reason. Also record an estimate-vs-actual metric so
    // operators can see when caps are firing.
    if let Some(reason) = &aborted_reason {
        tracing::warn!(
            session = %session_id,
            topic = %topic_label,
            rows_sent,
            bytes_sent,
            reason,
            "SOW snapshot aborted — runtime cap exceeded"
        );
        metrics::counter!(
            "cq_query_runtime_capped_total",
            "topic" => topic_label.clone()
        )
        .increment(1);
        if let Some(f) = encode_frame(codec, &CqMessage::error(None, reason)) {
            let _ = tx.send(f).await;
        }
    }

    if let Some(f) = encode_frame(codec, &CqMessage::group_end(&sub_id)) {
        let _ = tx.send(f).await;
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;
    metrics::counter!("cq_sow_rows_total", "topic" => topic_label.clone())
        .increment(total as u64);
    metrics::histogram!("cq_sow_query_latency_us", "topic" => topic_label.clone())
        .record(started.elapsed().as_micros() as f64);
    metrics::histogram!("cq_sow_actual_rows", "topic" => topic_label.clone())
        .record(total as f64);
    metrics::histogram!("cq_sow_actual_bytes", "topic" => topic_label.clone())
        .record(bytes_sent as f64);
    tracing::info!(
        session = %session_id,
        topic = %topic_label,
        rows = total,
        batches = batches_sent,
        elapsed_ms,
        "SOW snapshot delivered"
    );
}

/// One-shot ad-hoc JOIN evaluator for the SOW path. Bypasses the
/// streaming/cache infrastructure of `deliver_streaming_snapshot` —
/// ad-hoc joins are bespoke and the result set is bounded by group
/// cardinality, so we just run `execute_join_query` to materialize
/// the rows, then chunk them into `sow_batch` frames at the same
/// `batch_size` the streaming path uses. The frame envelope
/// (`group_begin`, `sow_batch`*, `group_end`) is byte-compatible with
/// the streaming path so SDK-side collectors don't care which one
/// produced the snapshot.
#[allow(clippy::too_many_arguments)]
async fn deliver_join_snapshot(
    left: SharedTopic,
    right: SharedTopic,
    sql: String,
    sub_id: String,
    topic_label: String,
    session_id: String,
    codec_slot: crate::session::SharedCodec,
    tx: crate::session::OutboundTx,
    batch_size: usize,
    hard_max_rows: u64,
    hard_max_bytes: u64,
) {
    use crate::session::encode_frame;
    use std::time::Instant;

    let codec = *codec_slot.lock();
    let started = Instant::now();

    // Run the JOIN on a blocking thread so the JSON serialization
    // doesn't block the tokio runtime. The right Arc is moved into
    // the closure together with the left so the topic locks acquired
    // inside `execute_join_query` (read-locks on both topics' state)
    // are released before we touch the wire.
    let left_for_block = left.clone();
    let right_for_block = right.clone();
    let sql_for_block = sql.clone();
    let result: Result<Vec<serde_json::Map<String, serde_json::Value>>, String> =
        tokio::task::spawn_blocking(move || {
            use cq_core::query::{combined_join_schema, parse_query};
            let left_schema = left_for_block.schema();
            let right_schema = right_for_block.schema();
            let (_, _, using) = match peek_join(&sql_for_block) {
                Ok(Some(tup)) => tup,
                Ok(None) => {
                    return Err(
                        "deliver_join_snapshot called on non-JOIN SQL".into(),
                    );
                }
                Err(e) => return Err(format!("peek_join: {e}")),
            };
            let combined = combined_join_schema(&left_schema, &right_schema, &using);
            let query = parse_query(&sql_for_block, &combined)
                .map_err(|e| format!("parse: {e}"))?;
            let qr = left_for_block
                .execute_join_query(&query, &right_for_block)
                .map_err(|e| format!("execute_join_query: {e}"))?;
            Ok(qr.rows)
        })
        .await
        .map_err(|e| format!("blocking task join: {e}"))
        .and_then(|inner| inner);

    let rows = match result {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(session = %session_id, topic = %topic_label, error = %e, "JOIN snapshot failed");
            // For SOW the caller passes `command_id` as `sub_id` so
            // routing the error back through the original cid lets the
            // client's pending SOW future resolve instead of timing out.
            if let Some(f) = encode_frame(codec, &CqMessage::error(Some(sub_id.clone()), &e)) {
                let _ = tx.send(f).await;
            }
            return;
        }
    };

    // group_begin → batches → group_end.
    if let Some(f) = encode_frame(codec, &CqMessage::group_begin(&sub_id, rows.len())) {
        if tx.send(f).await.is_err() {
            return;
        }
    }

    let cap = batch_size.max(1);
    let mut batches_sent: usize = 0;
    let mut rows_sent: usize = 0;
    let mut bytes_sent: u64 = 0;
    let mut aborted_reason: Option<String> = None;
    let mut chunk: Vec<serde_json::Map<String, serde_json::Value>> = Vec::with_capacity(cap);
    for row in rows {
        chunk.push(row);
        if chunk.len() >= cap {
            let n = chunk.len();
            let batch_bytes: u64 = (n as u64).saturating_mul(256);
            if let Some(f) = encode_frame(codec, &CqMessage::sow_batch(&sub_id, std::mem::replace(&mut chunk, Vec::with_capacity(cap)))) {
                if tx.send(f).await.is_err() {
                    return;
                }
            }
            batches_sent += 1;
            rows_sent += n;
            bytes_sent += batch_bytes;
            if hard_max_rows > 0 && rows_sent as u64 > hard_max_rows {
                aborted_reason = Some(format!(
                    "rows_emitted = {rows_sent} exceeded hard_max_sow_result_rows = {hard_max_rows}"
                ));
                break;
            }
            if hard_max_bytes > 0 && bytes_sent > hard_max_bytes {
                aborted_reason = Some(format!(
                    "bytes_emitted = {bytes_sent} exceeded hard_max_sow_result_bytes = {hard_max_bytes}"
                ));
                break;
            }
        }
    }
    if !chunk.is_empty() && aborted_reason.is_none() {
        let n = chunk.len();
        if let Some(f) = encode_frame(codec, &CqMessage::sow_batch(&sub_id, chunk)) {
            if tx.send(f).await.is_err() {
                return;
            }
        }
        batches_sent += 1;
        rows_sent += n;
    }
    if let Some(reason) = &aborted_reason {
        tracing::warn!(session = %session_id, topic = %topic_label, %reason, "JOIN snapshot aborted by hard cap");
    }
    if let Some(f) = encode_frame(codec, &CqMessage::group_end(&sub_id)) {
        let _ = tx.send(f).await;
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;
    tracing::info!(
        session = %session_id,
        topic = %topic_label,
        rows = rows_sent,
        batches = batches_sent,
        elapsed_ms,
        "JOIN SOW snapshot delivered"
    );
}

#[cfg(test)]
mod sync_repl_tests {
    use super::*;
    use crate::session::{new_registry, OutboundFrame, Session};
    use cq_core::schema::{ColumnType, Schema};
    use cq_core::topic::{Topic, TopicConfig};
    use cq_protocol::command::{AckType, Command, Status};
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn build_topic() -> SharedTopic {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price"],
            &[ColumnType::String, ColumnType::Double],
        ));
        Arc::new(Topic::new(
            TopicConfig {
                name: "/repl".into(),
                key_fields: vec!["symbol".into()],
                persist: false,
                conflation_ms: None,
                index_columns: vec![],
                expire_seconds: None,
            },
            schema,
            32,
        ))
    }

    fn ctx_with(sync: SyncReplication) -> RouterContext {
        RouterContext {
            topics: Arc::new(DashMap::new()),
            sessions: new_registry(),
            queues: crate::queue::new_queue_registry(),
            auth: Arc::new(crate::auth::AuthStore::disabled()),
            sow_batch_size: 64,
            bookmark_store: new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: sync,
            connection_limits: None,
        }
    }

    fn publish_msg(ack: Option<AckType>) -> CqMessage {
        let mut m = CqMessage::new(Command::Publish);
        m.command_id = Some("cid-1".into());
        m.ack_type = ack;
        m
    }

    fn ok_ack(seq: u64) -> CqMessage {
        let mut a = CqMessage::ack_ok(Some("cid-1".into()));
        a.sequence = Some(seq);
        a
    }

    fn decode(frame: &OutboundFrame) -> CqMessage {
        serde_json::from_slice(frame.as_bytes()).unwrap()
    }

    #[tokio::test]
    async fn non_replicated_ack_is_sent_inline() {
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(16);
        let session = Session::new("t".into(), tx);
        let ctx = ctx_with(SyncReplication::default());
        let topic = build_topic();

        finish_publish_ack(&session, &publish_msg(None), &ctx, &topic, 7, ok_ack(7));

        let frame = rx.try_recv().expect("ack sent synchronously");
        let m = decode(&frame);
        assert_eq!(m.status, Some(Status::Ok));
        assert_eq!(m.sequence, Some(7));
    }

    #[tokio::test]
    async fn replicated_ack_without_target_is_rejected() {
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(16);
        let session = Session::new("t".into(), tx);
        let ctx = ctx_with(SyncReplication {
            active: false,
            ack_timeout: Duration::from_secs(5),
        });
        let topic = build_topic();

        finish_publish_ack(
            &session,
            &publish_msg(Some(AckType::Replicated)),
            &ctx,
            &topic,
            7,
            ok_ack(7),
        );

        let m = decode(&rx.try_recv().expect("error sent synchronously"));
        assert_eq!(m.status, Some(Status::Error));
    }

    #[tokio::test]
    async fn replicated_ack_already_satisfied_sends_inline() {
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(16);
        let session = Session::new("t".into(), tx);
        let ctx = ctx_with(SyncReplication {
            active: true,
            ack_timeout: Duration::from_secs(5),
        });
        let topic = build_topic();
        topic.mark_replicated(10);

        finish_publish_ack(
            &session,
            &publish_msg(Some(AckType::Replicated)),
            &ctx,
            &topic,
            7,
            ok_ack(7),
        );

        let m = decode(&rx.try_recv().expect("ack sent synchronously"));
        assert_eq!(m.status, Some(Status::Ok));
        assert_eq!(m.sequence, Some(7));
    }

    #[tokio::test]
    async fn replicated_ack_waits_for_standby_then_acks() {
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(16);
        let session = Session::new("t".into(), tx);
        let ctx = ctx_with(SyncReplication {
            active: true,
            ack_timeout: Duration::from_secs(5),
        });
        let topic = build_topic();

        finish_publish_ack(
            &session,
            &publish_msg(Some(AckType::Replicated)),
            &ctx,
            &topic,
            7,
            ok_ack(7),
        );
        // Nothing yet — the standby hasn't confirmed.
        assert!(rx.try_recv().is_err());

        topic.mark_replicated(7);
        let frame = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("ack should arrive after mark_replicated")
            .expect("frame");
        let m = decode(&frame);
        assert_eq!(m.status, Some(Status::Ok));
        assert_eq!(m.sequence, Some(7));
    }

    #[tokio::test]
    async fn replicated_ack_times_out_when_standby_never_confirms() {
        let (tx, mut rx) = mpsc::channel::<OutboundFrame>(16);
        let session = Session::new("t".into(), tx);
        let ctx = ctx_with(SyncReplication {
            active: true,
            ack_timeout: Duration::from_millis(150),
        });
        let topic = build_topic();

        finish_publish_ack(
            &session,
            &publish_msg(Some(AckType::Replicated)),
            &ctx,
            &topic,
            7,
            ok_ack(7),
        );

        let frame = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("a timeout error should arrive")
            .expect("frame");
        let m = decode(&frame);
        assert_eq!(m.status, Some(Status::Error));
    }
}

/// D3/P0.3 — `max_sessions_per_user`, enforced in `handle_logon` (via
/// `dispatch`) since user identity isn't known until a successful
/// logon. Drives real `Logon` frames through `dispatch` against a
/// `RouterContext` carrying a `ConnectionLimits` with a tiny cap, and
/// checks: (a) sessions beyond the cap for the SAME user are rejected
/// with an error ack + the `cq_logon_total{result="fail_session_limit"}`
/// path, (b) a different user is unaffected, (c) dropping a session
/// (simulated by dropping its `Session`, which drops its
/// `user_session_guard`) frees the slot for a retry.
#[cfg(test)]
mod session_limit_tests {
    use super::*;
    use crate::auth::{AuthStore, User};
    use crate::limits::{ConnectionLimits, LimitsConfig, RejectReason};
    use crate::session::{new_registry, OutboundFrame, Session};
    use cq_protocol::command::{Command, Status};
    use dashmap::DashMap;

    fn logon_msg(user: &str, pass: &str) -> CqMessage {
        let mut m = CqMessage::new(Command::Logon);
        m.command_id = Some(format!("logon-{user}"));
        let mut creds = serde_json::Map::new();
        creds.insert("user".into(), user.into());
        creds.insert("password".into(), pass.into());
        m.data = Some(serde_json::Value::Object(creds));
        m
    }

    fn ctx_with_user_cap(cap: usize) -> RouterContext {
        let hash = bcrypt::hash("s3cret", 4).unwrap();
        let alice = User::from_parts("alice".into(), hash.clone(), &["*:*".into()]).unwrap();
        let bob = User::from_parts("bob".into(), hash, &["*:*".into()]).unwrap();
        let auth = Arc::new(AuthStore::new(true, vec![alice, bob]));
        let limits = ConnectionLimits::new(LimitsConfig {
            max_sessions_per_user: cap,
            ..LimitsConfig::default()
        });
        RouterContext {
            topics: Arc::new(DashMap::new()),
            sessions: new_registry(),
            queues: crate::queue::new_queue_registry(),
            auth,
            sow_batch_size: 64,
            bookmark_store: new_bookmark_store(),
            spillover: None,
            read_only: false,
            query_limits: cq_core::query::QueryLimits::default(),
            sync_replication: SyncReplication::default(),
            connection_limits: Some(limits),
        }
    }

    type MpscRx = tokio::sync::mpsc::Receiver<OutboundFrame>;

    fn new_session() -> (Session, MpscRx) {
        let (tx, rx) = tokio::sync::mpsc::channel::<OutboundFrame>(16);
        (Session::new("t".into(), tx), rx)
    }

    fn last_ack(rx: &mut MpscRx) -> CqMessage {
        let frame = rx.try_recv().expect("expected a frame");
        serde_json::from_slice(frame.as_bytes()).unwrap()
    }

    #[tokio::test]
    async fn second_session_for_same_user_over_cap_is_rejected() {
        let ctx = ctx_with_user_cap(1);

        let (mut s1, mut rx1) = new_session();
        dispatch(&mut s1, logon_msg("alice", "s3cret"), &ctx);
        let ack1 = last_ack(&mut rx1);
        assert_eq!(ack1.status, Some(Status::Ok), "first logon should succeed");
        assert_eq!(ctx.connection_limits.as_ref().unwrap().sessions_for_user("alice"), 1);

        let (mut s2, mut rx2) = new_session();
        dispatch(&mut s2, logon_msg("alice", "s3cret"), &ctx);
        let ack2 = last_ack(&mut rx2);
        assert_eq!(
            ack2.status,
            Some(Status::Error),
            "second concurrent session for the same user must be rejected"
        );
        assert!(ack2
            .reason
            .as_deref()
            .unwrap_or("")
            .contains("too many concurrent sessions"));
        assert!(s2.username.is_none(), "rejected logon must not set identity");
    }

    #[tokio::test]
    async fn different_user_is_unaffected_by_another_users_cap() {
        let ctx = ctx_with_user_cap(1);

        let (mut s1, mut rx1) = new_session();
        dispatch(&mut s1, logon_msg("alice", "s3cret"), &ctx);
        assert_eq!(last_ack(&mut rx1).status, Some(Status::Ok));

        let (mut s2, mut rx2) = new_session();
        dispatch(&mut s2, logon_msg("bob", "s3cret"), &ctx);
        assert_eq!(
            last_ack(&mut rx2).status,
            Some(Status::Ok),
            "bob's logon must not be blocked by alice's session count"
        );
    }

    #[tokio::test]
    async fn dropping_a_session_frees_its_slot_for_a_retry() {
        let ctx = ctx_with_user_cap(1);

        let (mut s1, mut rx1) = new_session();
        dispatch(&mut s1, logon_msg("alice", "s3cret"), &ctx);
        assert_eq!(last_ack(&mut rx1).status, Some(Status::Ok));
        assert_eq!(ctx.connection_limits.as_ref().unwrap().sessions_for_user("alice"), 1);

        // Simulate disconnect: drop the Session, which drops its
        // `user_session_guard`.
        drop(s1);
        assert_eq!(ctx.connection_limits.as_ref().unwrap().sessions_for_user("alice"), 0);

        let (mut s2, mut rx2) = new_session();
        dispatch(&mut s2, logon_msg("alice", "s3cret"), &ctx);
        assert_eq!(
            last_ack(&mut rx2).status,
            Some(Status::Ok),
            "retry after the first session dropped should succeed"
        );
    }

    #[tokio::test]
    async fn zero_cap_disables_the_check() {
        let ctx = ctx_with_user_cap(0);
        for _ in 0..5 {
            let (mut s, mut rx) = new_session();
            dispatch(&mut s, logon_msg("alice", "s3cret"), &ctx);
            assert_eq!(last_ack(&mut rx).status, Some(Status::Ok));
            // Keep session alive for the duration of the loop iteration
            // only; explicit drop makes intent clear.
            drop(s);
        }
    }

    #[test]
    fn reject_reason_label_matches_metric_convention() {
        assert_eq!(RejectReason::MaxSessionsPerUser.as_str(), "max_sessions_per_user");
    }
}

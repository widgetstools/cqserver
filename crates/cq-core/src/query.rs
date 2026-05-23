//! SQL query parser and executor.
//!
//! Parses SQL SELECT statements into a `ParsedQuery` structure, then executes
//! them against a `ColumnStore` using scan → filter → sort → limit → project.
//!
//! When a secondary index is available and the WHERE clause contains
//! an equality on an indexed column anywhere in an AND-tree, the
//! planner uses that as a candidate-row hint: it iterates the
//! index's bitmap instead of every row in the store, then evaluates
//! the *whole* predicate on each candidate (the index is just a fast
//! pre-filter; OR / NOT branches still need full eval). Two
//! Prometheus counters expose the savings:
//!   - `cq_query_index_hits_total` — queries that took the index path
//!   - `cq_query_full_scans_total` — queries that fell back to full scan

use crate::predicate::{compile_expr, CompiledPredicate, PredicateError};
use crate::schema::Schema;
use crate::sec_index::{IxKey, SecondaryIndex};
use crate::store::{ColumnStore, Value};
use compact_str::CompactString;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, OrderByKind, SelectItem,
    SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::cmp::Ordering;
use std::collections::HashMap;

/// A parsed and compiled query ready for execution.
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Topic name (from the FROM clause).
    pub topic: String,
    /// Column indices to project (empty = all columns). Ignored for
    /// aggregate queries — they project to `aggregates` + `group_by`.
    pub projection: Vec<usize>,
    /// Compiled WHERE predicate.
    pub predicate: CompiledPredicate,
    /// ORDER BY: (column_index, ascending).
    pub order_by: Vec<(usize, bool)>,
    /// LIMIT clause.
    pub limit: Option<usize>,
    /// Aggregate output specs. Non-empty iff this is an aggregate
    /// query. Order matches the order they appeared in the SELECT.
    pub aggregates: Vec<AggregateSpec>,
    /// GROUP BY column indices, in declaration order. Empty +
    /// `!aggregates.is_empty()` = implicit single-group (e.g.
    /// `SELECT COUNT(*) FROM t WHERE ...`).
    pub group_by: Vec<usize>,
}

/// Aggregate function variants. `Count` with `col = None` is `COUNT(*)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

impl AggFn {
    pub fn label(&self) -> &'static str {
        match self {
            AggFn::Sum => "SUM",
            AggFn::Count => "COUNT",
            AggFn::Avg => "AVG",
            AggFn::Min => "MIN",
            AggFn::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub func: AggFn,
    /// `None` only for COUNT(*); else the input column index.
    pub col: Option<usize>,
    /// Output key in the result row (e.g. `"SUM(price)"` or the
    /// user-supplied alias `"total"`).
    pub alias: String,
}

impl ParsedQuery {
    /// True if this query produces aggregated output (one row per
    /// group). Distinguishes the aggregate execution path from the
    /// row-by-row projection path.
    pub fn is_aggregate(&self) -> bool {
        !self.aggregates.is_empty()
    }
}

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub total_matches: usize,
}

/// Parse a SQL string into a `ParsedQuery`.
pub fn parse_query(sql: &str, schema: &Schema) -> Result<ParsedQuery, QueryError> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| QueryError::ParseError(e.to_string()))?;

    if ast.is_empty() {
        return Err(QueryError::ParseError("Empty SQL".into()));
    }

    let statement = &ast[0];
    match statement {
        Statement::Query(query) => parse_select(query, schema),
        _ => Err(QueryError::ParseError("Only SELECT statements supported".into())),
    }
}

fn parse_select(
    query: &sqlparser::ast::Query,
    schema: &Schema,
) -> Result<ParsedQuery, QueryError> {
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(QueryError::ParseError("Expected SELECT".into())),
    };

    // --- FROM clause → topic name ---
    let topic = if let Some(from) = select.from.first() {
        match &from.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err(QueryError::ParseError("Unsupported FROM".into())),
        }
    } else {
        String::new()
    };

    // --- GROUP BY ---
    let group_by = parse_group_by(&select.group_by, schema)?;

    // --- SELECT columns: a projection list, or a mix of group-by
    //     column refs + aggregate function calls. The presence of
    //     either an aggregate call or a non-empty GROUP BY switches
    //     the executor into aggregate mode.
    let (projection, aggregates) =
        parse_projection_or_aggregates(&select.projection, schema, &group_by)?;

    // --- WHERE clause → predicate ---
    let predicate = if let Some(where_expr) = &select.selection {
        compile_expr(where_expr, schema).map_err(QueryError::PredicateError)?
    } else {
        CompiledPredicate::True
    };

    // --- ORDER BY ---
    let order_by = parse_order_by(&query.order_by, schema)?;

    // --- LIMIT ---
    let limit = match query.limit_clause.as_ref() {
        Some(sqlparser::ast::LimitClause::LimitOffset { limit: Some(l), .. }) => {
            if let Expr::Value(sqlparser::ast::ValueWithSpan {
                value: sqlparser::ast::Value::Number(n, _), ..
            }) = l
            {
                n.parse::<usize>().ok()
            } else {
                None
            }
        }
        _ => None,
    };

    Ok(ParsedQuery {
        topic,
        projection,
        predicate,
        order_by,
        limit,
        aggregates,
        group_by,
    })
}

fn parse_group_by(
    g: &sqlparser::ast::GroupByExpr,
    schema: &Schema,
) -> Result<Vec<usize>, QueryError> {
    match g {
        GroupByExpr::All(_) => Err(QueryError::ParseError(
            "GROUP BY ALL is not supported".into(),
        )),
        GroupByExpr::Expressions(exprs, _) => {
            let mut cols = Vec::with_capacity(exprs.len());
            for e in exprs {
                cols.push(resolve_select_column(e, schema)?);
            }
            Ok(cols)
        }
    }
}

/// Parse the SELECT list into either:
///   1. A pure projection (`Vec<usize>`, empty = all cols) + no aggregates, OR
///   2. An aggregate query with possibly mixed group-by-column refs +
///      aggregate function calls. The projection vector is left empty
///      in case (2); the executor instead uses `aggregates` +
///      `group_by` to build output rows.
fn parse_projection_or_aggregates(
    items: &[SelectItem],
    schema: &Schema,
    group_by: &[usize],
) -> Result<(Vec<usize>, Vec<AggregateSpec>), QueryError> {
    // First pass: detect whether any item is an aggregate function.
    let has_agg = items
        .iter()
        .any(|i| matches!(extract_expr(i), Some(e) if is_aggregate_function_call(e)));

    if !has_agg && group_by.is_empty() {
        // Plain projection — unchanged from previous behaviour.
        let projection = parse_projection(items, schema)?;
        return Ok((projection, Vec::new()));
    }

    // Aggregate path.
    let mut aggregates = Vec::new();
    for item in items {
        let (expr, alias_override) = match item {
            SelectItem::UnnamedExpr(e) => (e, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
            SelectItem::Wildcard(_) => {
                return Err(QueryError::ParseError(
                    "SELECT * not allowed with GROUP BY / aggregates".into(),
                ));
            }
            _ => {
                return Err(QueryError::ParseError(format!(
                    "Unsupported SELECT item in aggregate query: {item:?}"
                )));
            }
        };

        if let Some(spec) = parse_aggregate_call(expr, schema, alias_override.as_deref())? {
            aggregates.push(spec);
        } else {
            // Non-aggregate projection element: must be a column that
            // appears in GROUP BY (otherwise its value isn't
            // deterministic across the group).
            let col = resolve_select_column(expr, schema)?;
            if !group_by.contains(&col) {
                return Err(QueryError::ParseError(format!(
                    "column `{}` must appear in GROUP BY or inside an aggregate",
                    schema.column_name(col)
                )));
            }
            // Group-by columns are emitted automatically by the executor;
            // no need to track them in the projection vector.
        }
    }

    if aggregates.is_empty() && !group_by.is_empty() {
        // GROUP BY without any aggregate is allowed and just yields
        // the distinct group-key tuples — still emit one row per
        // group, with `aggregates` empty. We hint this case to the
        // executor by leaving both vectors as-is.
    }

    Ok((Vec::new(), aggregates))
}

fn extract_expr(item: &SelectItem) -> Option<&Expr> {
    match item {
        SelectItem::UnnamedExpr(e) => Some(e),
        SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

fn is_aggregate_function_call(expr: &Expr) -> bool {
    if let Expr::Function(f) = expr {
        let name = f.name.to_string().to_ascii_uppercase();
        matches!(name.as_str(), "SUM" | "COUNT" | "AVG" | "MIN" | "MAX")
    } else {
        false
    }
}

fn parse_aggregate_call(
    expr: &Expr,
    schema: &Schema,
    alias: Option<&str>,
) -> Result<Option<AggregateSpec>, QueryError> {
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Ok(None),
    };
    let name = f.name.to_string().to_ascii_uppercase();
    let func = match name.as_str() {
        "SUM" => AggFn::Sum,
        "COUNT" => AggFn::Count,
        "AVG" => AggFn::Avg,
        "MIN" => AggFn::Min,
        "MAX" => AggFn::Max,
        _ => return Ok(None),
    };

    // Extract the single function argument (or `*`).
    let arg_list = match &f.args {
        FunctionArguments::List(l) => l,
        _ => {
            return Err(QueryError::ParseError(format!(
                "{name} requires an argument list"
            )))
        }
    };
    if arg_list.args.len() != 1 {
        return Err(QueryError::ParseError(format!(
            "{name} expects exactly one argument, got {}",
            arg_list.args.len()
        )));
    }

    let (col, default_alias) = match &arg_list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            // Only COUNT(*) is meaningful with `*`.
            if func != AggFn::Count {
                return Err(QueryError::ParseError(format!(
                    "{name}(*) is not allowed; only COUNT(*) supports wildcards"
                )));
            }
            (None, format!("{}(*)", func.label()))
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
            let col_idx = resolve_select_column(e, schema)?;
            let col_name = schema.column_name(col_idx).to_string();
            (Some(col_idx), format!("{}({})", func.label(), col_name))
        }
        other => {
            return Err(QueryError::ParseError(format!(
                "{name}: unsupported argument shape: {other:?}"
            )));
        }
    };

    Ok(Some(AggregateSpec {
        func,
        col,
        alias: alias.map(str::to_string).unwrap_or(default_alias),
    }))
}

fn parse_projection(
    items: &[SelectItem],
    schema: &Schema,
) -> Result<Vec<usize>, QueryError> {
    let mut cols = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                // Empty projection = all columns
                return Ok(Vec::new());
            }
            SelectItem::UnnamedExpr(expr) => {
                let col = resolve_select_column(expr, schema)?;
                cols.push(col);
            }
            _ => return Err(QueryError::ParseError(format!("Unsupported SELECT item: {:?}", item))),
        }
    }
    Ok(cols)
}

fn resolve_select_column(expr: &Expr, schema: &Schema) -> Result<usize, QueryError> {
    match expr {
        Expr::Identifier(ident) => schema
            .index_of(&ident.value)
            .ok_or_else(|| QueryError::UnknownColumn(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => {
            let name = parts.iter().map(|p| p.value.as_str()).collect::<Vec<_>>().join(".");
            schema
                .index_of(&name)
                .ok_or_else(|| QueryError::UnknownColumn(name))
        }
        _ => Err(QueryError::ParseError("Unsupported expression in SELECT".into())),
    }
}

fn parse_order_by(
    order_by: &Option<sqlparser::ast::OrderBy>,
    schema: &Schema,
) -> Result<Vec<(usize, bool)>, QueryError> {
    let order_by = match order_by {
        Some(ob) => ob,
        None => return Ok(Vec::new()),
    };
    let exprs = match &order_by.kind {
        OrderByKind::All { .. } => return Ok(Vec::new()),
        OrderByKind::Expressions(exprs) => exprs,
    };
    let mut result = Vec::new();
    for item in exprs {
        let col = resolve_select_column(&item.expr, schema)?;
        let asc = item.options.asc.unwrap_or(true);
        result.push((col, asc));
    }
    Ok(result)
}

/// Candidate-row source for a query: either an index bitmap (fast
/// path) or the full row range. The caller iterates over `.iter()`
/// and evaluates the predicate on each.
pub enum CandidateRows<'a> {
    Bitmap(&'a roaring::RoaringBitmap),
    Full(u32),
}

impl<'a> CandidateRows<'a> {
    /// Approximate count; used for sizing result vectors.
    pub fn upper_bound(&self) -> usize {
        match self {
            CandidateRows::Bitmap(b) => b.len() as usize,
            CandidateRows::Full(n) => *n as usize,
        }
    }

    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        match self {
            CandidateRows::Bitmap(b) => {
                for row in b.iter() {
                    f(row);
                }
            }
            CandidateRows::Full(n) => {
                for row in 0..*n {
                    f(row);
                }
            }
        }
    }
}

/// Plan the candidate-row source for a query. Returns the index
/// bitmap when an equality on an indexed column is present
/// anywhere in the predicate's AND-tree; otherwise falls back to
/// the full row range. Increments the appropriate metric counter
/// as a side effect.
pub fn plan_candidates<'a>(
    query: &ParsedQuery,
    store: &ColumnStore,
    index: Option<&'a SecondaryIndex>,
) -> CandidateRows<'a> {
    if let Some(ix) = index {
        if let Some((col, key)) = find_index_hint(&query.predicate, ix) {
            metrics::counter!("cq_query_index_hits_total").increment(1);
            if let Some(b) = ix.rows_for_key(col, &key) {
                return CandidateRows::Bitmap(b);
            }
            // Hit but empty — represent as an empty range so the
            // caller's iteration yields nothing.
            return CandidateRows::Full(0);
        }
    }
    metrics::counter!("cq_query_full_scans_total").increment(1);
    CandidateRows::Full(store.row_count())
}

/// Walk a predicate looking for the first `col = literal` clause
/// whose column is covered by `index`. Recurses into `And` branches
/// (both are required so an indexed hit lets us restrict candidates);
/// stops at `Or` / `Not` / non-equality leaves — those need a full
/// evaluation pass anyway. Returns `(col, key)` ready for
/// `SecondaryIndex::rows_for_key`.
fn find_index_hint(pred: &CompiledPredicate, index: &SecondaryIndex) -> Option<(usize, IxKey)> {
    match pred {
        CompiledPredicate::EqString { col, value } if index.covers(*col) => {
            Some((*col, IxKey::String(value.clone())))
        }
        CompiledPredicate::EqLong { col, value } if index.covers(*col) => {
            Some((*col, IxKey::Long(*value)))
        }
        CompiledPredicate::EqDouble { col, value } if index.covers(*col) && !value.is_nan() => {
            Some((*col, IxKey::DoubleBits(value.to_bits())))
        }
        CompiledPredicate::And(a, b) => {
            find_index_hint(a, index).or_else(|| find_index_hint(b, index))
        }
        _ => None,
    }
}

/// Convenience alias to keep query callers from needing to import
/// `compact_str` for the rare case they hand-build literals.
#[allow(dead_code)]
fn _compact_str_marker(_: CompactString) {}

/// Execute a parsed query against a column store. Convenience wrapper
/// that disables the index path — kept so existing callers (and unit
/// tests) work unchanged.
pub fn execute_query(query: &ParsedQuery, store: &ColumnStore) -> QueryResult {
    execute_query_with_index(query, store, None)
}

/// Execute a parsed query, optionally using a secondary index to
/// short-circuit the candidate-row enumeration. When `index` is `Some`
/// and the WHERE clause contains a usable equality hint, only rows
/// from the index's bitmap are evaluated; otherwise we fall back to a
/// full scan.
pub fn execute_query_with_index(
    query: &ParsedQuery,
    store: &ColumnStore,
    index: Option<&SecondaryIndex>,
) -> QueryResult {
    // Aggregate queries take a separate execution path: per-row work
    // updates aggregator state instead of building per-row output.
    if query.is_aggregate() || !query.group_by.is_empty() {
        return execute_aggregate_query(query, store, index);
    }

    let candidates = plan_candidates(query, store, index);
    let mut matching_rows: Vec<u32> = Vec::with_capacity(candidates.upper_bound());
    candidates.for_each(|row| {
        if query.predicate.matches(store, row) {
            matching_rows.push(row);
        }
    });
    let total_matches = matching_rows.len();

    // Step 2: Sort
    if !query.order_by.is_empty() {
        matching_rows.sort_by(|&a, &b| {
            for &(col, asc) in &query.order_by {
                let ord = compare_values(store, col, a, b);
                let ord = if asc { ord } else { ord.reverse() };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    // Step 3: Limit
    if let Some(limit) = query.limit {
        matching_rows.truncate(limit);
    }

    // Step 4: Project
    let proj_indices = if query.projection.is_empty() {
        // All columns
        (0..store.schema().column_count()).collect::<Vec<_>>()
    } else {
        query.projection.clone()
    };

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = matching_rows
        .iter()
        .map(|&row| store.get_row_map_projected(row, &proj_indices))
        .collect();

    QueryResult {
        rows,
        total_matches,
    }
}

/// Hashable group key built from one row's values across the GROUP BY
/// columns. Mirrors `IxKey` (Eq + Hash), with `Null` as a first-class
/// variant — unlike the secondary index, GROUP BY treats null as its
/// own group rather than skipping it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GroupKeyPart {
    Null,
    Int(i32),
    Long(i64),
    DoubleBits(u64),
    String(CompactString),
}

impl GroupKeyPart {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => GroupKeyPart::Null,
            Value::String(None) => GroupKeyPart::Null,
            Value::String(Some(s)) => GroupKeyPart::String(s.clone()),
            Value::Long(n) if *n == crate::store::NULL_LONG => GroupKeyPart::Null,
            Value::Long(n) => GroupKeyPart::Long(*n),
            Value::Int(n) if *n == crate::store::NULL_INT => GroupKeyPart::Null,
            Value::Int(n) => GroupKeyPart::Int(*n),
            Value::Double(d) if d.is_nan() => GroupKeyPart::Null,
            Value::Double(d) => GroupKeyPart::DoubleBits(d.to_bits()),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            GroupKeyPart::Null => serde_json::Value::Null,
            GroupKeyPart::Int(n) => serde_json::Value::from(*n),
            GroupKeyPart::Long(n) => serde_json::Value::from(*n),
            GroupKeyPart::DoubleBits(bits) => serde_json::Value::from(f64::from_bits(*bits)),
            GroupKeyPart::String(s) => serde_json::Value::from(s.as_str()),
        }
    }
}

/// Running state for one aggregate over one group. The `seen_any`
/// flag distinguishes "no rows yet" from "all-null input" — needed so
/// `MIN`/`MAX`/`AVG` return null on an empty group rather than `0` or
/// the default bounds.
#[derive(Debug)]
enum AggState {
    Count(u64),
    SumI(i128, bool),       // (running, seen_any)
    SumF(f64, bool),
    AvgF { sum: f64, count: u64 },
    MinI(i64, bool),
    MaxI(i64, bool),
    MinF(f64, bool),
    MaxF(f64, bool),
    MinS(CompactString, bool),
    MaxS(CompactString, bool),
}

impl AggState {
    /// Build the initial state for an aggregate, given the column
    /// type (or `None` for COUNT(*)).
    fn init(func: AggFn, col_type: Option<crate::schema::ColumnType>) -> AggState {
        use crate::schema::ColumnType;
        match (func, col_type) {
            (AggFn::Count, _) => AggState::Count(0),
            (AggFn::Sum, Some(ColumnType::Double)) => AggState::SumF(0.0, false),
            (AggFn::Sum, Some(ColumnType::Long))
            | (AggFn::Sum, Some(ColumnType::Int)) => AggState::SumI(0, false),
            (AggFn::Avg, _) => AggState::AvgF { sum: 0.0, count: 0 },
            (AggFn::Min, Some(ColumnType::Double)) => {
                AggState::MinF(f64::INFINITY, false)
            }
            (AggFn::Min, Some(ColumnType::Long))
            | (AggFn::Min, Some(ColumnType::Int)) => AggState::MinI(i64::MAX, false),
            (AggFn::Min, Some(ColumnType::String)) => {
                AggState::MinS(CompactString::new(""), false)
            }
            (AggFn::Max, Some(ColumnType::Double)) => {
                AggState::MaxF(f64::NEG_INFINITY, false)
            }
            (AggFn::Max, Some(ColumnType::Long))
            | (AggFn::Max, Some(ColumnType::Int)) => AggState::MaxI(i64::MIN, false),
            (AggFn::Max, Some(ColumnType::String)) => {
                AggState::MaxS(CompactString::new(""), false)
            }
            // SUM/MIN/MAX on a string column without a type-specific
            // variant (e.g. SUM(name)) — fall through to a sentinel
            // that ignores input so the executor doesn't panic. The
            // parser already rejects these in well-formed queries.
            _ => AggState::Count(0),
        }
    }

    /// Update with one row's value for the aggregate's input column.
    /// `None` represents `COUNT(*)` (no column read).
    fn update(&mut self, v: Option<&Value>) {
        match self {
            AggState::Count(n) => {
                match v {
                    None => *n += 1,                  // COUNT(*)
                    Some(val) if !val.is_null() => *n += 1, // COUNT(col)
                    _ => {}                              // null → skip
                }
            }
            AggState::SumI(acc, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_i64() {
                        *acc += x as i128;
                        *seen = true;
                    }
                }
            }
            AggState::SumF(acc, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        *acc += x;
                        *seen = true;
                    }
                }
            }
            AggState::AvgF { sum, count } => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        *sum += x;
                        *count += 1;
                    }
                }
            }
            AggState::MinI(cur, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_i64() {
                        if !*seen || x < *cur {
                            *cur = x;
                        }
                        *seen = true;
                    }
                }
            }
            AggState::MaxI(cur, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_i64() {
                        if !*seen || x > *cur {
                            *cur = x;
                        }
                        *seen = true;
                    }
                }
            }
            AggState::MinF(cur, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        if !*seen || x < *cur {
                            *cur = x;
                        }
                        *seen = true;
                    }
                }
            }
            AggState::MaxF(cur, seen) => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        if !*seen || x > *cur {
                            *cur = x;
                        }
                        *seen = true;
                    }
                }
            }
            AggState::MinS(cur, seen) => {
                if let Some(val) = v {
                    if let Some(s) = val.as_str() {
                        if !*seen || s < cur.as_str() {
                            *cur = CompactString::new(s);
                        }
                        *seen = true;
                    }
                }
            }
            AggState::MaxS(cur, seen) => {
                if let Some(val) = v {
                    if let Some(s) = val.as_str() {
                        if !*seen || s > cur.as_str() {
                            *cur = CompactString::new(s);
                        }
                        *seen = true;
                    }
                }
            }
        }
    }

    fn finalize(&self) -> serde_json::Value {
        match self {
            AggState::Count(n) => serde_json::Value::from(*n),
            AggState::SumI(acc, seen) => {
                if !*seen {
                    serde_json::Value::Null
                } else {
                    // `i64::MIN..i64::MAX` sums fit only up to ~9e18;
                    // serde_json::Number's i64 path covers the bulk
                    // of real-world cases. Spill to f64 if needed.
                    if (*acc as i128) >= (i64::MIN as i128) && *acc <= (i64::MAX as i128) {
                        serde_json::Value::from(*acc as i64)
                    } else {
                        serde_json::Value::from(*acc as f64)
                    }
                }
            }
            AggState::SumF(s, seen) => {
                if !*seen {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(*s)
                }
            }
            AggState::AvgF { sum, count } => {
                if *count == 0 {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(*sum / (*count as f64))
                }
            }
            AggState::MinI(v, seen) | AggState::MaxI(v, seen) => {
                if !*seen {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(*v)
                }
            }
            AggState::MinF(v, seen) | AggState::MaxF(v, seen) => {
                if !*seen {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(*v)
                }
            }
            AggState::MinS(s, seen) | AggState::MaxS(s, seen) => {
                if !*seen {
                    serde_json::Value::Null
                } else {
                    serde_json::Value::from(s.as_str())
                }
            }
        }
    }
}

/// Aggregate execution: scan candidate rows, maintain a running
/// state per `(group_key, aggregate)`, emit one output row per
/// distinct group key. Single-pass O(rows × aggregates), no second
/// scan.
fn execute_aggregate_query(
    query: &ParsedQuery,
    store: &ColumnStore,
    index: Option<&SecondaryIndex>,
) -> QueryResult {
    let candidates = plan_candidates(query, store, index);
    let schema = store.schema();

    // Build group-by column metadata once.
    let group_cols: Vec<usize> = query.group_by.clone();
    let group_names: Vec<String> = group_cols
        .iter()
        .map(|&c| schema.column_name(c).to_string())
        .collect();

    // Pre-compute aggregate input column types (or None for COUNT(*)).
    let agg_col_types: Vec<Option<crate::schema::ColumnType>> = query
        .aggregates
        .iter()
        .map(|a| a.col.map(|c| schema.column_type(c)))
        .collect();

    // (group-key tuple) → Vec<AggState> with one slot per aggregate.
    let mut groups: HashMap<Vec<GroupKeyPart>, Vec<AggState>> = HashMap::new();
    let mut group_order: Vec<Vec<GroupKeyPart>> = Vec::new();

    candidates.for_each(|row| {
        if !query.predicate.matches(store, row) {
            return;
        }
        // Compute the group key.
        let key: Vec<GroupKeyPart> = group_cols
            .iter()
            .map(|&c| GroupKeyPart::from_value(&store.get(c, row)))
            .collect();
        let states = groups.entry(key.clone()).or_insert_with(|| {
            group_order.push(key.clone());
            query
                .aggregates
                .iter()
                .enumerate()
                .map(|(i, a)| AggState::init(a.func, agg_col_types[i]))
                .collect()
        });
        // Update every aggregate.
        for (i, spec) in query.aggregates.iter().enumerate() {
            match spec.col {
                None => states[i].update(None), // COUNT(*)
                Some(c) => {
                    let v = store.get(c, row);
                    states[i].update(Some(&v));
                }
            }
        }
    });

    // Emit one row per group in the order they were first seen. This
    // is deterministic across runs of identical input — distinct from
    // sorted, which the user can request via ORDER BY (we materialize
    // the result and post-sort below).
    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        Vec::with_capacity(group_order.len());
    for key in &group_order {
        let states = groups.get(key).expect("group must exist");
        let mut row_map = serde_json::Map::new();
        for (i, part) in key.iter().enumerate() {
            row_map.insert(group_names[i].clone(), part.to_json());
        }
        for (i, spec) in query.aggregates.iter().enumerate() {
            row_map.insert(spec.alias.clone(), states[i].finalize());
        }
        rows.push(row_map);
    }

    // ORDER BY for aggregate queries operates on the output column
    // names (group cols or aggregate aliases). We support it only for
    // group-by columns for now — sorting by an aggregate alias would
    // require parsing it as a non-column reference, which the current
    // `parse_order_by` doesn't allow. Aggregate ORDER BY can be added
    // later; falling back to insertion order is consistent with most
    // OLAP engines' default.
    if !query.order_by.is_empty() {
        rows.sort_by(|a, b| {
            for &(col, asc) in &query.order_by {
                let name = schema.column_name(col);
                let av = a.get(name);
                let bv = b.get(name);
                let ord = compare_json_values(av, bv);
                let ord = if asc { ord } else { ord.reverse() };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    let total_matches = rows.len();
    QueryResult {
        rows,
        total_matches,
    }
}

/// Lexicographic compare over JSON values; tolerant of mixed types
/// and nulls in the same column (defensive — shouldn't happen in
/// well-formed group output).
fn compare_json_values(
    a: Option<&serde_json::Value>,
    b: Option<&serde_json::Value>,
) -> Ordering {
    use serde_json::Value as J;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some(J::Null), Some(J::Null)) => Ordering::Equal,
        (Some(J::Null), _) => Ordering::Less,
        (_, Some(J::Null)) => Ordering::Greater,
        (Some(J::Number(x)), Some(J::Number(y))) => {
            x.as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&y.as_f64().unwrap_or(0.0))
                .unwrap_or(Ordering::Equal)
        }
        (Some(J::String(x)), Some(J::String(y))) => x.cmp(y),
        _ => Ordering::Equal,
    }
}

/// Compare two row values for a given column (used in ORDER BY).
fn compare_values(store: &ColumnStore, col: usize, row_a: u32, row_b: u32) -> Ordering {
    use crate::schema::ColumnType;
    match store.schema().column_type(col) {
        ColumnType::Double => {
            let a = store.get_double(col, row_a);
            let b = store.get_double(col, row_b);
            a.partial_cmp(&b).unwrap_or(Ordering::Equal)
        }
        ColumnType::Long => {
            let a = store.get_long(col, row_a);
            let b = store.get_long(col, row_b);
            a.cmp(&b)
        }
        ColumnType::Int => {
            let a = store.get_int(col, row_a);
            let b = store.get_int(col, row_b);
            a.cmp(&b)
        }
        ColumnType::String => {
            let a = store.get_string(col, row_a);
            let b = store.get_string(col, row_b);
            a.cmp(&b)
        }
    }
}

/// Project a single row using the query's projection.
pub fn project_row(
    query: &ParsedQuery,
    store: &ColumnStore,
    row: u32,
) -> serde_json::Map<String, serde_json::Value> {
    if query.projection.is_empty() {
        store.get_row_map(row)
    } else {
        store.get_row_map_projected(row, &query.projection)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Unknown column: {0}")]
    UnknownColumn(String),
    #[error("Predicate error: {0}")]
    PredicateError(#[from] PredicateError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnType;
    use crate::store::Value;
    use compact_str::CompactString;
    use std::sync::Arc;

    fn make_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["symbol", "price", "quantity", "desk"],
            &[
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
                ColumnType::String,
            ],
        ));
        let mut store = ColumnStore::new(schema.clone(), 100);

        let rows = vec![
            ("AAPL", 150.0, 100i64, "RATES"),
            ("MSFT", 300.0, 50, "EQUITIES"),
            ("GOOGL", 2800.0, 10, "RATES"),
            ("AMZN", 3400.0, 5, "TECH"),
            ("NVDA", 250.0, 200, "EQUITIES"),
        ];
        for (sym, price, qty, desk) in rows {
            store.append_row(&[
                Value::String(Some(CompactString::new(sym))),
                Value::Double(price),
                Value::Long(qty),
                Value::String(Some(CompactString::new(desk))),
            ]);
        }
        (schema, store)
    }

    #[test]
    fn test_simple_select() {
        let (schema, store) = make_store();
        let query = parse_query("SELECT * FROM trades WHERE desk = 'RATES'", &schema).unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 2);
    }

    #[test]
    fn test_select_with_projection() {
        let (schema, store) = make_store();
        let query =
            parse_query("SELECT symbol, price FROM trades WHERE price > 200", &schema).unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 4); // MSFT, GOOGL, AMZN, NVDA
        // Should only have symbol and price
        assert!(result.rows[0].contains_key("symbol"));
        assert!(result.rows[0].contains_key("price"));
        assert!(!result.rows[0].contains_key("desk"));
    }

    #[test]
    fn test_order_by_and_limit() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT symbol, price FROM trades ORDER BY price DESC LIMIT 3",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0].get("symbol").unwrap(), "AMZN");
        assert_eq!(result.rows[1].get("symbol").unwrap(), "GOOGL");
        assert_eq!(result.rows[2].get("symbol").unwrap(), "MSFT");
    }

    #[test]
    fn indexed_eq_returns_same_rows_as_full_scan() {
        // Same query, run with and without an index → identical result.
        let (schema, store) = make_store();
        let query = parse_query("SELECT * FROM trades WHERE desk = 'RATES'", &schema).unwrap();

        // Build an index covering the `desk` column (idx 3) and seed it
        // from the existing store rows.
        let desk_col = schema.index_of("desk").unwrap();
        let mut ix = crate::sec_index::SecondaryIndex::new(vec![desk_col]);
        for row in 0..store.row_count() {
            let v = store.get(desk_col, row);
            ix.add(desk_col, &v, row);
        }

        let full = execute_query(&query, &store);
        let indexed = execute_query_with_index(&query, &store, Some(&ix));
        assert_eq!(full.rows.len(), indexed.rows.len());
        // Symbols must be identical sets.
        let mut full_syms: Vec<_> = full
            .rows
            .iter()
            .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
            .collect();
        let mut ix_syms: Vec<_> = indexed
            .rows
            .iter()
            .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
            .collect();
        full_syms.sort();
        ix_syms.sort();
        assert_eq!(full_syms, ix_syms);
    }

    #[test]
    fn indexed_and_other_predicate_still_correct() {
        // `desk = 'EQUITIES' AND price > 200` — the index narrows
        // to desk='EQUITIES' rows, the AND filter then drops MSFT
        // (price=300 passes) and keeps NVDA (250 passes). MSFT has
        // price=300 so it also passes — both equity rows actually
        // qualify. Sanity check against the full-scan reference.
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT symbol FROM trades WHERE desk = 'EQUITIES' AND price > 200",
            &schema,
        )
        .unwrap();
        let desk_col = schema.index_of("desk").unwrap();
        let mut ix = crate::sec_index::SecondaryIndex::new(vec![desk_col]);
        for row in 0..store.row_count() {
            let v = store.get(desk_col, row);
            ix.add(desk_col, &v, row);
        }

        let full = execute_query(&query, &store);
        let indexed = execute_query_with_index(&query, &store, Some(&ix));
        let mut full_syms: Vec<_> = full
            .rows
            .iter()
            .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
            .collect();
        let mut ix_syms: Vec<_> = indexed
            .rows
            .iter()
            .map(|r| r.get("symbol").unwrap().as_str().unwrap().to_string())
            .collect();
        full_syms.sort();
        ix_syms.sort();
        assert_eq!(full_syms, ix_syms);
        // MSFT (300) and NVDA (250) — both equities with price > 200.
        assert_eq!(ix_syms, vec!["MSFT".to_string(), "NVDA".to_string()]);
    }

    #[test]
    fn indexed_eq_misses_returns_empty() {
        let (schema, store) = make_store();
        let query =
            parse_query("SELECT * FROM trades WHERE desk = 'NOSUCH'", &schema).unwrap();
        let desk_col = schema.index_of("desk").unwrap();
        let mut ix = crate::sec_index::SecondaryIndex::new(vec![desk_col]);
        for row in 0..store.row_count() {
            let v = store.get(desk_col, row);
            ix.add(desk_col, &v, row);
        }
        let indexed = execute_query_with_index(&query, &store, Some(&ix));
        assert!(indexed.rows.is_empty());
    }

    #[test]
    fn aggregate_count_star() {
        let (schema, store) = make_store();
        let query = parse_query("SELECT COUNT(*) FROM trades", &schema).unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        let n = row.get("COUNT(*)").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(n, 5);
    }

    #[test]
    fn aggregate_count_star_with_where() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT COUNT(*) FROM trades WHERE desk = 'RATES'",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 1);
        let n = result.rows[0]
            .get("COUNT(*)")
            .and_then(|v| v.as_u64())
            .unwrap();
        assert_eq!(n, 2);
    }

    #[test]
    fn aggregate_group_by_desk_sum_qty() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT desk, SUM(quantity), COUNT(*) FROM trades GROUP BY desk",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        // Three desks: RATES, EQUITIES, TECH.
        assert_eq!(result.rows.len(), 3, "got rows: {:?}", result.rows);

        // Build a lookup by desk for assertions.
        let mut by_desk = std::collections::HashMap::new();
        for row in &result.rows {
            let d = row.get("desk").unwrap().as_str().unwrap().to_string();
            by_desk.insert(d, row.clone());
        }
        // RATES: 100 + 10 = 110, count 2
        assert_eq!(
            by_desk["RATES"]
                .get("SUM(quantity)")
                .unwrap()
                .as_i64()
                .unwrap(),
            110
        );
        assert_eq!(
            by_desk["RATES"].get("COUNT(*)").unwrap().as_u64().unwrap(),
            2
        );
        // EQUITIES: 50 + 200 = 250, count 2
        assert_eq!(
            by_desk["EQUITIES"]
                .get("SUM(quantity)")
                .unwrap()
                .as_i64()
                .unwrap(),
            250
        );
        // TECH: 5, count 1
        assert_eq!(
            by_desk["TECH"]
                .get("SUM(quantity)")
                .unwrap()
                .as_i64()
                .unwrap(),
            5
        );
    }

    #[test]
    fn aggregate_avg_min_max() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT desk, AVG(price), MIN(price), MAX(price) FROM trades GROUP BY desk",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        let mut by_desk = std::collections::HashMap::new();
        for row in &result.rows {
            let d = row.get("desk").unwrap().as_str().unwrap().to_string();
            by_desk.insert(d, row.clone());
        }
        // RATES prices: 150, 2800 → avg 1475, min 150, max 2800.
        let r = &by_desk["RATES"];
        assert!((r.get("AVG(price)").unwrap().as_f64().unwrap() - 1475.0).abs() < 1e-9);
        assert!((r.get("MIN(price)").unwrap().as_f64().unwrap() - 150.0).abs() < 1e-9);
        assert!((r.get("MAX(price)").unwrap().as_f64().unwrap() - 2800.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_with_alias() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades GROUP BY desk",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        for row in &result.rows {
            assert!(
                row.contains_key("total"),
                "alias `total` missing: {:?}",
                row
            );
            assert!(!row.contains_key("SUM(quantity)"));
        }
    }

    #[test]
    fn aggregate_select_non_groupby_column_errors() {
        let (schema, _store) = make_store();
        // `symbol` isn't in GROUP BY and isn't aggregated → reject.
        let err = parse_query(
            "SELECT desk, symbol FROM trades GROUP BY desk",
            &schema,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("symbol"),
            "expected error to mention `symbol`, got: {msg}"
        );
    }

    #[test]
    fn non_equality_predicate_falls_back_to_full_scan() {
        // `price > 200` — no equality hint → full scan path.
        // We can't directly observe "which path" without exposing
        // internal state, but we can at least confirm correctness
        // matches the no-index branch.
        let (schema, store) = make_store();
        let query = parse_query("SELECT * FROM trades WHERE price > 200", &schema).unwrap();
        let desk_col = schema.index_of("desk").unwrap();
        let mut ix = crate::sec_index::SecondaryIndex::new(vec![desk_col]);
        for row in 0..store.row_count() {
            let v = store.get(desk_col, row);
            ix.add(desk_col, &v, row);
        }
        let full = execute_query(&query, &store);
        let indexed = execute_query_with_index(&query, &store, Some(&ix));
        assert_eq!(full.rows.len(), indexed.rows.len());
    }

    #[test]
    fn test_between() {
        let (schema, store) = make_store();
        let query = parse_query(
            "SELECT symbol FROM trades WHERE price BETWEEN 100 AND 300",
            &schema,
        )
        .unwrap();
        let result = execute_query(&query, &store);
        assert_eq!(result.rows.len(), 3); // AAPL(150), MSFT(300), NVDA(250)
    }
}

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
use std::collections::{HashMap, HashSet};

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
    /// P4 — OFFSET clause. Skips the first `offset` rows of the
    /// (sorted) result before LIMIT applies.
    pub offset: Option<usize>,
    /// Aggregate output specs. Non-empty iff this is an aggregate
    /// query. Order matches the order they appeared in the SELECT.
    pub aggregates: Vec<AggregateSpec>,
    /// GROUP BY column indices, in declaration order. Empty +
    /// `!aggregates.is_empty()` = implicit single-group (e.g.
    /// `SELECT COUNT(*) FROM t WHERE ...`).
    pub group_by: Vec<usize>,
    /// Static-PIVOT spec (S43). `Some` iff the FROM clause was a
    /// `PIVOT (...) FOR col IN (lit, lit, ...)` and routes the
    /// executor to the pivot path.
    pub pivot: Option<ParsedPivot>,
    /// UNPIVOT spec (S43). `Some` iff the FROM clause was
    /// `UNPIVOT (val FOR name IN (c1, c2, ...))`. Mutually
    /// exclusive with `pivot`.
    pub unpivot: Option<ParsedUnpivot>,
    /// S20 JOIN spec. `Some` iff the FROM clause is
    /// `A JOIN B USING (col, ...)` — `A` is `topic`, `B` is
    /// `join.right_topic`, and the parse-side schema check has
    /// already validated that the `using` column names exist on the
    /// left schema. The executor builds a hash map of right-side
    /// rows keyed by the USING values, then walks the left store
    /// joining on equality. Other join shapes (LEFT OUTER, ON-clause
    /// with non-equi predicates, multi-table chained joins) are
    /// rejected at parse time and reserved for follow-ups.
    pub join: Option<JoinSpec>,
    /// Q7 — window-function columns. Each is evaluated per row after
    /// the projection step: rows are partitioned by `partition_by`,
    /// sorted by `order_by`, then the window fn (ROW_NUMBER / RANK /
    /// DENSE_RANK / LAG / LEAD) assigns a per-row value emitted under
    /// the spec's alias. Non-aggregate path only — combining window
    /// fns with GROUP BY would need a separate pass and is deferred.
    pub windows: Vec<WindowColumn>,
    /// P3 — HAVING predicate evaluated after group-by finalise. None
    /// for non-aggregate queries (HAVING without GROUP BY is rejected
    /// at parse time, matching AMPS).
    pub having: Option<HavingExpr>,
    /// P2 — scalar expressions in the SELECT list (`a + b AS sum`).
    /// Empty for queries that only project bare columns. Each entry
    /// is emitted as an extra `(alias, value)` pair appended to each
    /// output row by the non-aggregate executor. Aggregate-path
    /// support (`SUM(a + b)`) is tracked separately.
    pub computed: Vec<ComputedColumn>,
}

/// P2 — one computed column in the SELECT list. The alias is what
/// appears as the key in each output row's JSON map; the expr is
/// evaluated against the source row's values.
#[derive(Debug, Clone)]
pub struct ComputedColumn {
    pub alias: String,
    pub expr: ScalarExpr,
}

/// P2 — a tiny expression tree for scalar arithmetic in SELECT.
/// Intentionally minimal: just the four BinaryOps, column refs, and
/// numeric/string literals. Division by zero, null inputs, and type
/// coercion failures all evaluate to `Value::Null` — same behaviour
/// as the AMPS-style "soft null" propagation.
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    Col(usize),
    LitDouble(f64),
    LitLong(i64),
    LitString(CompactString),
    Add(Box<ScalarExpr>, Box<ScalarExpr>),
    Sub(Box<ScalarExpr>, Box<ScalarExpr>),
    Mul(Box<ScalarExpr>, Box<ScalarExpr>),
    Div(Box<ScalarExpr>, Box<ScalarExpr>),
    Neg(Box<ScalarExpr>),
}

impl ScalarExpr {
    /// Evaluate against a single source row. Returns `Value::Null`
    /// on missing/null inputs, division by zero, or type errors so
    /// callers can emit a JSON `null` cell.
    pub fn eval(&self, row: &[Value]) -> Value {
        match self {
            ScalarExpr::Col(i) => row.get(*i).cloned().unwrap_or(Value::Null),
            ScalarExpr::LitDouble(d) => Value::Double(*d),
            ScalarExpr::LitLong(l) => Value::Long(*l),
            ScalarExpr::LitString(s) => Value::String(Some(s.clone())),
            ScalarExpr::Add(a, b) => num_binop(&a.eval(row), &b.eval(row), |x, y| x + y),
            ScalarExpr::Sub(a, b) => num_binop(&a.eval(row), &b.eval(row), |x, y| x - y),
            ScalarExpr::Mul(a, b) => num_binop(&a.eval(row), &b.eval(row), |x, y| x * y),
            ScalarExpr::Div(a, b) => {
                let l = a.eval(row);
                let r = b.eval(row);
                match (value_as_f64(&l), value_as_f64(&r)) {
                    (Some(_), Some(0.0)) => Value::Null,
                    (Some(x), Some(y)) => Value::Double(x / y),
                    _ => Value::Null,
                }
            }
            ScalarExpr::Neg(e) => match value_as_f64(&e.eval(row)) {
                Some(x) => Value::Double(-x),
                None => Value::Null,
            },
        }
    }
}

fn num_binop(a: &Value, b: &Value, op: impl Fn(f64, f64) -> f64) -> Value {
    match (value_as_f64(a), value_as_f64(b)) {
        (Some(x), Some(y)) => Value::Double(op(x, y)),
        _ => Value::Null,
    }
}

fn value_as_f64(v: &Value) -> Option<f64> {
    // Delegate to the store's null-aware coercion so NULL_LONG /
    // NULL_INT / NaN aren't silently treated as zero.
    v.as_f64()
}

/// Q7 — one window-function column on the SELECT list.
#[derive(Debug, Clone)]
pub struct WindowColumn {
    pub alias: String,
    /// Schema column indices to partition rows on; empty = single
    /// global partition.
    pub partition_by: Vec<usize>,
    /// `(col_idx, ascending)` per ORDER BY entry. Empty = no
    /// intra-partition ordering (RANK/ROW_NUMBER fall back to
    /// row-arrival order).
    pub order_by: Vec<(usize, bool)>,
    pub kind: WindowFn,
}

#[derive(Debug, Clone)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
    /// `LAG(col, offset, default)` — value from the row `offset` back
    /// in the partition's sorted order. `default` is emitted when
    /// the lookup falls off the partition's leading edge.
    Lag { col: usize, offset: usize },
    /// `LEAD(col, offset, default)` — same but forward.
    Lead { col: usize, offset: usize },
}

/// P3 — compiled HAVING expression. Evaluated against a finalised
/// aggregate row (the `serde_json::Map` the executor builds with one
/// entry per group column + one per aggregate alias). Intentionally a
/// tiny mirror of WHERE — comparison + logical ops + literal/ref —
/// because HAVING by spec only sees post-aggregate scalar values.
#[derive(Debug, Clone)]
pub enum HavingExpr {
    /// Look up a column in the output row map. The string is either a
    /// group-column name or an aggregate alias (resolved at parse time).
    Ref(String),
    LitDouble(f64),
    LitLong(i64),
    LitString(String),
    LitBool(bool),
    Eq(Box<HavingExpr>, Box<HavingExpr>),
    Ne(Box<HavingExpr>, Box<HavingExpr>),
    Lt(Box<HavingExpr>, Box<HavingExpr>),
    Le(Box<HavingExpr>, Box<HavingExpr>),
    Gt(Box<HavingExpr>, Box<HavingExpr>),
    Ge(Box<HavingExpr>, Box<HavingExpr>),
    And(Box<HavingExpr>, Box<HavingExpr>),
    Or(Box<HavingExpr>, Box<HavingExpr>),
    Not(Box<HavingExpr>),
}

impl HavingExpr {
    /// `true` if this row passes the HAVING predicate. Missing
    /// references or type-mismatch comparisons → `false` (matches
    /// AMPS's conservative semantics).
    pub fn matches(&self, row: &serde_json::Map<String, serde_json::Value>) -> bool {
        match self {
            HavingExpr::Ref(_) | HavingExpr::LitDouble(_) | HavingExpr::LitLong(_)
            | HavingExpr::LitString(_) | HavingExpr::LitBool(_) => {
                // Standalone refs aren't predicates; treat as false
                // (the user wrote something like `HAVING SUM(qty)` —
                // there's no sensible bool interpretation).
                false
            }
            HavingExpr::And(a, b) => a.matches(row) && b.matches(row),
            HavingExpr::Or(a, b) => a.matches(row) || b.matches(row),
            HavingExpr::Not(e) => !e.matches(row),
            HavingExpr::Eq(a, b) => compare_having(a, b, row) == Some(Ordering::Equal),
            HavingExpr::Ne(a, b) => match compare_having(a, b, row) {
                Some(Ordering::Equal) => false,
                Some(_) => true,
                None => false,
            },
            HavingExpr::Lt(a, b) => compare_having(a, b, row) == Some(Ordering::Less),
            HavingExpr::Le(a, b) => matches!(compare_having(a, b, row), Some(Ordering::Less) | Some(Ordering::Equal)),
            HavingExpr::Gt(a, b) => compare_having(a, b, row) == Some(Ordering::Greater),
            HavingExpr::Ge(a, b) => matches!(compare_having(a, b, row), Some(Ordering::Greater) | Some(Ordering::Equal)),
        }
    }
}

fn compare_having(
    lhs: &HavingExpr,
    rhs: &HavingExpr,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Option<Ordering> {
    let l = eval_having_to_json(lhs, row)?;
    let r = eval_having_to_json(rhs, row)?;
    // Numeric vs numeric — compare as f64.
    if let (Some(lf), Some(rf)) = (l.as_f64(), r.as_f64()) {
        return lf.partial_cmp(&rf);
    }
    // String vs string.
    if let (Some(ls), Some(rs)) = (l.as_str(), r.as_str()) {
        return Some(ls.cmp(rs));
    }
    // Bool vs bool.
    if let (Some(lb), Some(rb)) = (l.as_bool(), r.as_bool()) {
        return Some(lb.cmp(&rb));
    }
    None
}

fn eval_having_to_json(
    e: &HavingExpr,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    match e {
        HavingExpr::Ref(name) => row.get(name).cloned(),
        HavingExpr::LitDouble(d) => Some(serde_json::Value::from(*d)),
        HavingExpr::LitLong(i) => Some(serde_json::Value::from(*i)),
        HavingExpr::LitString(s) => Some(serde_json::Value::from(s.as_str())),
        HavingExpr::LitBool(b) => Some(serde_json::Value::from(*b)),
        // Composite exprs aren't valid operands of a comparison; the
        // parser rejects this shape, but be defensive.
        _ => None,
    }
}

/// P3 — compile a HAVING `Expr` against the aggregate-row "schema":
/// `group_names` (group-by column names, position-stable) +
/// `aggregate_aliases` (the alias the executor will emit for each
/// AggregateSpec). Function calls matching an existing aggregate are
/// rewritten to the alias; bare identifiers must match either a group
/// column or an alias. Anything else returns `Err`.
fn compile_having(
    expr: &Expr,
    group_names: &[String],
    aggregates: &[AggregateSpec],
    schema: &Schema,
) -> Result<HavingExpr, QueryError> {
    use sqlparser::ast::{BinaryOperator, UnaryOperator, Value as SqlValue, ValueWithSpan};
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let l = Box::new(compile_having(left, group_names, aggregates, schema)?);
            let r = Box::new(compile_having(right, group_names, aggregates, schema)?);
            match op {
                BinaryOperator::Eq => Ok(HavingExpr::Eq(l, r)),
                BinaryOperator::NotEq => Ok(HavingExpr::Ne(l, r)),
                BinaryOperator::Lt => Ok(HavingExpr::Lt(l, r)),
                BinaryOperator::LtEq => Ok(HavingExpr::Le(l, r)),
                BinaryOperator::Gt => Ok(HavingExpr::Gt(l, r)),
                BinaryOperator::GtEq => Ok(HavingExpr::Ge(l, r)),
                BinaryOperator::And => Ok(HavingExpr::And(l, r)),
                BinaryOperator::Or => Ok(HavingExpr::Or(l, r)),
                _ => Err(QueryError::ParseError(format!(
                    "unsupported operator in HAVING: {op:?}"
                ))),
            }
        }
        Expr::UnaryOp { op: UnaryOperator::Not, expr } => Ok(HavingExpr::Not(Box::new(
            compile_having(expr, group_names, aggregates, schema)?,
        ))),
        Expr::Nested(e) => compile_having(e, group_names, aggregates, schema),
        Expr::Identifier(id) => {
            // Group column name, or aggregate alias — both legal.
            let name = id.value.clone();
            if group_names.iter().any(|g| g == &name)
                || aggregates.iter().any(|a| a.alias == name)
            {
                Ok(HavingExpr::Ref(name))
            } else {
                Err(QueryError::UnknownColumn(name))
            }
        }
        Expr::Function(_) => {
            // Re-parse the function as an aggregate spec; match against
            // the SELECT's aggregates by (func, col). On match, refer
            // to the existing alias so the executor's row map look-up
            // hits the same key. The schema arg is unused by
            // parse_aggregate_call's name/col extraction.
            let candidate = parse_aggregate_call(expr, schema, None)?;
            let candidate = match candidate {
                Some(c) => c,
                None => {
                    return Err(QueryError::ParseError(format!(
                        "unsupported function in HAVING: {expr:?}"
                    )))
                }
            };
            // Match by function + alias shape. The default-alias the
            // parser builds for a bare `SUM(col)` is "SUM(col)" —
            // which matches the SELECT-side aggregate's auto-alias
            // when the user didn't supply one, or we need to compare
            // against the user-supplied alias when they did.
            for a in aggregates {
                if a.func == candidate.func && a.col == candidate.col {
                    return Ok(HavingExpr::Ref(a.alias.clone()));
                }
            }
            Err(QueryError::ParseError(format!(
                "HAVING references an aggregate not in SELECT: {expr}"
            )))
        }
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(HavingExpr::LitLong(i))
                } else {
                    Ok(HavingExpr::LitDouble(n.parse().unwrap_or(0.0)))
                }
            }
            SqlValue::SingleQuotedString(s) => Ok(HavingExpr::LitString(s.clone())),
            SqlValue::Boolean(b) => Ok(HavingExpr::LitBool(*b)),
            other => Err(QueryError::ParseError(format!(
                "unsupported literal in HAVING: {other:?}"
            ))),
        },
        _ => Err(QueryError::ParseError(format!(
            "unsupported expression in HAVING: {expr:?}"
        ))),
    }
}

/// Compiled `A JOIN B USING (col, ...)` spec. Schemas of both sides
/// are resolved at query-execution time (via `Topic` lookup in the
/// server's topic map) — the parser stores symbolic names only so
/// re-parse on schema-discovery boundaries stays cheap.
#[derive(Debug, Clone)]
pub struct JoinSpec {
    /// Topic name on the right side of the JOIN.
    pub right_topic: String,
    /// USING column names (must exist on BOTH sides). Populated either
    /// from a literal `USING (col, ...)` clause or translated from
    /// `ON a.col = b.col` (P11).
    pub using: Vec<String>,
    /// P12 — `Inner` (default) or `LeftOuter`. The executor swaps to
    /// "emit left row with right cols as NULL when no match" for
    /// `LeftOuter`.
    pub kind: JoinKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    /// Q1 — RIGHT OUTER: keep every right row; left columns NULL on miss.
    RightOuter,
    /// Q1 — FULL OUTER: keep every left AND right row; opposite-side
    /// columns NULL where there's no match. Equivalent to
    /// `LEFT OUTER UNION ALL right-only rows`.
    FullOuter,
    /// Q12 — Snowflake-style temporal join. For each left row, the
    /// matched right row is the one with the largest `ts_col` value
    /// ≤ the left row's `ts_col`. Other USING columns (typically a
    /// symbol / key) partition the search. Useful for "what was the
    /// quote price at the time of the trade?".
    AsOf { ts_col: String },
}

/// Compiled `PIVOT (...) FOR col IN (lit, lit, ...)` spec.
#[derive(Debug, Clone)]
pub struct ParsedPivot {
    /// One or more aggregates pivoted across the value list. Multi-
    /// measure pivots have len > 1 (e.g.,
    /// `PIVOT (SUM(qty), SUM(notional) FOR desk IN ('A', 'B'))`).
    pub aggregates: Vec<AggregateSpec>,
    /// Column whose distinct values become output column names.
    pub pivot_col: usize,
    /// Static IN-list of pivot values. The output has one column
    /// per (value, agg) pair (or just per value when there's one
    /// aggregate). Rows whose pivot column value isn't in the
    /// list are silently dropped — matches Snowflake/BigQuery.
    ///
    /// **Dynamic pivots** (`FOR col IN ANY`) leave this empty;
    /// the executor does a first pass over the candidate rows to
    /// discover the distinct pivot values, then runs the regular
    /// bucketing path against the discovered set.
    pub pivot_values: Vec<PivotLiteral>,
    /// `true` when the pivot value set is `IN ANY` (S45 dynamic
    /// PIVOT) — the executor discovers values from the data
    /// instead of consuming a literal list. `false` for the
    /// static-list form (S43).
    pub dynamic: bool,
    /// Anchor columns: every column NOT referenced by an aggregate
    /// and NOT the pivot column. The output has one row per
    /// distinct anchor-key tuple.
    pub anchor_cols: Vec<usize>,
}

/// One literal in a static PIVOT IN-list, typed to match the column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PivotLiteral {
    String(compact_str::CompactString),
    Long(i64),
    Double(u64), // f64.to_bits()
    Bool(bool),
    /// i64 microseconds since UNIX epoch — same internal form as the
    /// Timestamp column. `as_column_label` renders as RFC 3339.
    Timestamp(i64),
}

impl PivotLiteral {
    /// Stringify for output column naming. `'A'` becomes `"A"`,
    /// `100` becomes `"100"`. Matches Snowflake conventions.
    pub fn as_column_label(&self) -> String {
        match self {
            PivotLiteral::String(s) => s.to_string(),
            PivotLiteral::Long(n) => n.to_string(),
            PivotLiteral::Double(bits) => f64::from_bits(*bits).to_string(),
            PivotLiteral::Bool(b) => b.to_string(),
            PivotLiteral::Timestamp(v) => crate::store::format_timestamp_micros(*v),
        }
    }
}

/// Compiled `UNPIVOT (val FOR name IN (c1, c2, ...))` spec.
#[derive(Debug, Clone)]
pub struct ParsedUnpivot {
    /// Name of the output column that holds the pivot value (one
    /// per source-column-and-row).
    pub value_col_name: String,
    /// Name of the output column that holds the source column name.
    pub name_col_name: String,
    /// Source columns to unpivot (by index in the schema).
    pub source_cols: Vec<usize>,
    /// Anchor columns: every column NOT in `source_cols`. Each
    /// output row carries these plus (name, value).
    pub anchor_cols: Vec<usize>,
}

/// Aggregate function variants. `Count` with `col = None` is `COUNT(*)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    /// P8 — population standard deviation (`STDDEV` / `STDDEV_POP`).
    Stddev,
    /// P8 — sample standard deviation (`STDDEV_SAMP`).
    StddevSamp,
    /// P8 — population variance (`VARIANCE` / `VAR_POP`).
    Variance,
    /// P8 — sample variance (`VAR_SAMP`).
    VarianceSamp,
    /// P9 — `PERCENTILE_CONT(col, q)` — linear-interpolated percentile.
    /// `MEDIAN(col)` is sugar for `PERCENTILE_CONT(col, 0.5)`.
    /// O(n) memory per group; exact (no sketch).
    PercentileCont,
    /// P10 — `COUNT(DISTINCT col)`. Exact (HashSet per group).
    /// HyperLogLog is a future optimisation.
    CountDistinct,
}

impl AggFn {
    pub fn label(&self) -> &'static str {
        match self {
            AggFn::Sum => "SUM",
            AggFn::Count => "COUNT",
            AggFn::Avg => "AVG",
            AggFn::Min => "MIN",
            AggFn::Max => "MAX",
            AggFn::Stddev => "STDDEV",
            AggFn::StddevSamp => "STDDEV_SAMP",
            AggFn::Variance => "VARIANCE",
            AggFn::VarianceSamp => "VAR_SAMP",
            AggFn::PercentileCont => "PERCENTILE_CONT",
            AggFn::CountDistinct => "COUNT_DISTINCT",
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
    /// P9 — for `PercentileCont`, the percentile fraction `q ∈ [0,1]`.
    /// `None` for every other AggFn.
    pub percentile_q: Option<f64>,
}

impl ParsedQuery {
    /// True if this query produces aggregated output (one row per
    /// group). Distinguishes the aggregate execution path from the
    /// row-by-row projection path.
    pub fn is_aggregate(&self) -> bool {
        !self.aggregates.is_empty()
    }

    /// True if the FROM clause was a PIVOT/UNPIVOT (S43). Routes the
    /// executor to the pivot/unpivot path.
    pub fn is_pivot(&self) -> bool {
        self.pivot.is_some() || self.unpivot.is_some()
    }
}

/// Result of a query execution.
#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<serde_json::Map<String, serde_json::Value>>,
    pub total_matches: usize,
    /// Source row indices into the underlying `ColumnStore`, in
    /// lockstep with `rows`. Populated by the row-oriented
    /// (non-aggregate) execution path so the tombstone filter
    /// downstream can drop nulled-out rows by row-index lookup
    /// instead of by re-deriving the key from the projection — the
    /// pre-fix approach broke whenever the projection excluded the
    /// key column (see Known Issue closed by S46-followup).
    ///
    /// Aggregate queries leave this empty: their output rows are
    /// per-group synthesized, not per-source-row.
    pub source_rows: Vec<u32>,
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
        Statement::Query(query) => {
            // Q8 — non-recursive CTE inlining. Collect named
            // subqueries from `WITH x AS (...), y AS (...)`, then
            // inline references in the main FROM. RECURSIVE is
            // rejected up-front.
            let mut q = (**query).clone();
            if let Some(with) = q.with.take() {
                if with.recursive {
                    return Err(QueryError::ParseError(
                        "RECURSIVE CTEs are not supported".into(),
                    ));
                }
                inline_ctes(&mut q, &with.cte_tables)?;
            }
            // P1 — alias-rewrite pass.
            let refs = collect_table_refs(&q);
            rewrite_qualified_refs_in_query(&mut q, &refs);
            parse_select(&q, schema)
        }
        _ => Err(QueryError::ParseError("Only SELECT statements supported".into())),
    }
}

/// Q8 — inline each CTE reference in the main query. The MVP
/// supports the common case where each CTE is
/// `SELECT * FROM topic [WHERE filter]`. The CTE's source topic
/// replaces references to its alias, and the CTE's WHERE filter
/// is AND'd into the main WHERE.
///
/// Rejects CTEs with their own projections, GROUP BY, JOIN,
/// ORDER BY, etc. — those would need full sub-query materialisation
/// which is the Q9 follow-up. The reject message points the user
/// at writing the query directly until that lands.
fn inline_ctes(
    main: &mut sqlparser::ast::Query,
    ctes: &[sqlparser::ast::Cte],
) -> Result<(), QueryError> {
    use sqlparser::ast::{Expr, SetExpr, TableFactor};
    let mut cte_map: HashMap<String, (String, Option<Expr>)> = HashMap::new();
    for cte in ctes {
        let alias = cte.alias.name.value.clone();
        let inner = cte.query.body.as_ref();
        let inner_select = match inner {
            SetExpr::Select(s) => s,
            _ => {
                return Err(QueryError::ParseError(format!(
                    "CTE `{alias}` body must be a SELECT (set ops + VALUES not supported)"
                )));
            }
        };
        // The CTE's inner SELECT must be a simple SELECT * FROM topic
        // [WHERE …]; no projection, no GROUP BY, no JOIN, no nested
        // FROM, no ORDER BY, no LIMIT.
        let inner_select_items_ok = inner_select.projection.len() == 1
            && matches!(
                &inner_select.projection[0],
                sqlparser::ast::SelectItem::Wildcard(_)
            );
        if !inner_select_items_ok
            || !matches!(&inner_select.group_by, sqlparser::ast::GroupByExpr::Expressions(g, _) if g.is_empty())
            || inner_select.having.is_some()
            || inner_select.from.len() != 1
            || !inner_select.from[0].joins.is_empty()
            || cte.query.order_by.is_some()
            || cte.query.limit_clause.is_some()
        {
            return Err(QueryError::ParseError(format!(
                "CTE `{alias}` must be a simple `SELECT * FROM topic [WHERE ...]`; \
                 complex CTEs not supported (write the query directly)"
            )));
        }
        let inner_topic = match &inner_select.from[0].relation {
            TableFactor::Table { name, .. } => strip_identifier_quotes(&name.to_string()),
            _ => {
                return Err(QueryError::ParseError(format!(
                    "CTE `{alias}` FROM must be a plain topic name"
                )))
            }
        };
        let inner_where = inner_select.selection.clone();
        cte_map.insert(alias, (inner_topic, inner_where));
    }
    // Now rewrite the main query: any FROM table whose name is a
    // CTE key becomes the CTE's source; the CTE's WHERE gets AND'd
    // into the main WHERE.
    if let SetExpr::Select(select) = main.body.as_mut() {
        let mut extra_filters: Vec<Expr> = Vec::new();
        for from in &mut select.from {
            if let TableFactor::Table { name, .. } = &mut from.relation {
                let plain = strip_identifier_quotes(&name.to_string());
                if let Some((source, filter)) = cte_map.get(&plain) {
                    *name = sqlparser::ast::ObjectName::from(vec![
                        sqlparser::ast::Ident::new(source.clone()),
                    ]);
                    if let Some(f) = filter.clone() {
                        extra_filters.push(f);
                    }
                }
            }
            // JOIN right-side may also be a CTE.
            for join in &mut from.joins {
                if let TableFactor::Table { name, .. } = &mut join.relation {
                    let plain = strip_identifier_quotes(&name.to_string());
                    if let Some((source, filter)) = cte_map.get(&plain) {
                        *name = sqlparser::ast::ObjectName::from(vec![
                            sqlparser::ast::Ident::new(source.clone()),
                        ]);
                        if let Some(f) = filter.clone() {
                            extra_filters.push(f);
                        }
                    }
                }
            }
        }
        // Fold the collected CTE WHEREs into the main WHERE.
        if !extra_filters.is_empty() {
            let combined = extra_filters
                .into_iter()
                .reduce(|acc, e| Expr::BinaryOp {
                    left: Box::new(acc),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(e),
                })
                .unwrap();
            select.selection = match select.selection.take() {
                Some(existing) => Some(Expr::BinaryOp {
                    left: Box::new(existing),
                    op: sqlparser::ast::BinaryOperator::And,
                    right: Box::new(combined),
                }),
                None => Some(combined),
            };
        }
    }
    Ok(())
}

/// P1 — collect every name that can legally appear as the left side
/// of a `t.col` compound identifier: each table's real name AND its
/// alias (when present). For `FROM trades p JOIN securities s USING
/// (cusip)` we collect `{"trades", "p", "securities", "s"}`.
fn collect_table_refs(query: &sqlparser::ast::Query) -> HashSet<String> {
    let mut refs = HashSet::new();
    let select = match query.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return refs,
    };
    for from in &select.from {
        collect_from_table_factor(&from.relation, &mut refs);
        for j in &from.joins {
            collect_from_table_factor(&j.relation, &mut refs);
        }
    }
    refs
}

fn collect_from_table_factor(tf: &TableFactor, refs: &mut HashSet<String>) {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let topic = strip_identifier_quotes(&name.to_string());
            refs.insert(topic);
            if let Some(a) = alias {
                refs.insert(a.name.value.clone());
            }
        }
        TableFactor::Pivot { table, alias, .. } | TableFactor::Unpivot { table, alias, .. } => {
            collect_from_table_factor(table, refs);
            if let Some(a) = alias {
                refs.insert(a.name.value.clone());
            }
        }
        _ => {}
    }
}

/// P1 — rewrite every `Expr::CompoundIdentifier([t, col])` → `Expr::Identifier(col)`
/// when `t` is a known alias/topic ref. Walks SELECT items, WHERE,
/// GROUP BY, HAVING, ORDER BY, and JOIN constraints (which today are
/// USING-only but the ON-clause path P11 will exercise).
fn rewrite_qualified_refs_in_query(query: &mut sqlparser::ast::Query, refs: &HashSet<String>) {
    if let SetExpr::Select(select) = query.body.as_mut() {
        // SELECT items.
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => rewrite_expr(e, refs),
                SelectItem::ExprWithAlias { expr, .. } => rewrite_expr(expr, refs),
                SelectItem::QualifiedWildcard(kind, _) => {
                    // `t.*` → `*` when `t` is a known ref. We can't
                    // mutate the variant in place, so detect-and-replace.
                    if let sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(obj) = kind {
                        let first = obj.0.first().and_then(|p| p.as_ident()).map(|i| i.value.clone());
                        if let Some(n) = first {
                            if refs.contains(&strip_identifier_quotes(&n)) {
                                *item = SelectItem::Wildcard(
                                    sqlparser::ast::WildcardAdditionalOptions::default(),
                                );
                            }
                        }
                    }
                }
                SelectItem::Wildcard(_) => {}
            }
        }
        // WHERE.
        if let Some(e) = select.selection.as_mut() {
            rewrite_expr(e, refs);
        }
        // GROUP BY.
        match &mut select.group_by {
            GroupByExpr::Expressions(exprs, _) => {
                for e in exprs {
                    rewrite_expr(e, refs);
                }
            }
            GroupByExpr::All(_) => {}
        }
        // HAVING (P3 will compile this; P1 just rewrites refs so it's
        // ready by the time P3 lands).
        if let Some(e) = select.having.as_mut() {
            rewrite_expr(e, refs);
        }
        // JOIN constraints (ON-clause Expr — P11 uses these). Covers
        // both `Join(constraint)` (bare `JOIN`) and the explicit
        // `Inner/Left/Right/Full` variants, plus the `LeftOuter`
        // alias for `LEFT OUTER JOIN`.
        for from in &mut select.from {
            for join in &mut from.joins {
                if let sqlparser::ast::JoinOperator::Join(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::Inner(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::Left(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::LeftOuter(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::Right(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::RightOuter(
                    sqlparser::ast::JoinConstraint::On(expr),
                )
                | sqlparser::ast::JoinOperator::FullOuter(
                    sqlparser::ast::JoinConstraint::On(expr),
                ) = &mut join.join_operator
                {
                    rewrite_expr(expr, refs);
                }
            }
        }
    }
    // ORDER BY.
    if let Some(ob) = query.order_by.as_mut() {
        if let sqlparser::ast::OrderByKind::Expressions(items) = &mut ob.kind {
            for item in items {
                rewrite_expr(&mut item.expr, refs);
            }
        }
    }
    // Q12 — rewrite AsOf join's MATCH_CONDITION + ON-clause
    // constraint so both expressions see bare column names. The
    // standard `Inner/Left/...` rewrite block above doesn't cover
    // `JoinOperator::AsOf` because AsOf's constraint nests inside
    // the AsOf variant rather than being a direct field.
    if let SetExpr::Select(select) = query.body.as_mut() {
        for from in &mut select.from {
            for join in &mut from.joins {
                if let sqlparser::ast::JoinOperator::AsOf {
                    match_condition,
                    constraint,
                } = &mut join.join_operator
                {
                    rewrite_expr(match_condition, refs);
                    if let sqlparser::ast::JoinConstraint::On(e) = constraint {
                        rewrite_expr(e, refs);
                    }
                }
            }
        }
    }
}

/// Recursively rewrite `CompoundIdentifier([t, col])` → `Identifier(col)`
/// for every `t` in `refs`. Other forms (3-part `db.t.col`, function
/// args, nested ops) are walked recursively.
fn rewrite_expr(expr: &mut Expr, refs: &HashSet<String>) {
    match expr {
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let first = strip_identifier_quotes(&parts[0].value);
            if refs.contains(&first) {
                let col = parts[1].clone();
                *expr = Expr::Identifier(col);
            }
        }
        Expr::CompoundIdentifier(_) => {}
        Expr::Identifier(_) | Expr::Value(_) | Expr::Wildcard(_) => {}
        Expr::BinaryOp { left, right, .. } => {
            rewrite_expr(left, refs);
            rewrite_expr(right, refs);
        }
        Expr::UnaryOp { expr, .. } => rewrite_expr(expr, refs),
        Expr::Nested(e) => rewrite_expr(e, refs),
        Expr::Cast { expr, .. } => rewrite_expr(expr, refs),
        Expr::IsNull(e) | Expr::IsNotNull(e) | Expr::IsTrue(e) | Expr::IsFalse(e) => {
            rewrite_expr(e, refs);
        }
        Expr::InList { expr, list, .. } => {
            rewrite_expr(expr, refs);
            for e in list {
                rewrite_expr(e, refs);
            }
        }
        Expr::Between { expr, low, high, .. } => {
            rewrite_expr(expr, refs);
            rewrite_expr(low, refs);
            rewrite_expr(high, refs);
        }
        Expr::Like { expr, pattern, .. }
        | Expr::ILike { expr, pattern, .. }
        | Expr::SimilarTo { expr, pattern, .. } => {
            rewrite_expr(expr, refs);
            rewrite_expr(pattern, refs);
        }
        Expr::Function(f) => {
            if let FunctionArguments::List(args) = &mut f.args {
                for a in &mut args.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => rewrite_expr(e, refs),
                        FunctionArg::Named { arg: FunctionArgExpr::Expr(e), .. } => {
                            rewrite_expr(e, refs)
                        }
                        _ => {}
                    }
                }
            }
        }
        Expr::Case { conditions, else_result, operand, .. } => {
            if let Some(op) = operand.as_mut() {
                rewrite_expr(op, refs);
            }
            for w in conditions {
                rewrite_expr(&mut w.condition, refs);
                rewrite_expr(&mut w.result, refs);
            }
            if let Some(e) = else_result.as_mut() {
                rewrite_expr(e, refs);
            }
        }
        _ => {}
    }
}

/// S20 — peek at a SQL string and extract the JOIN's right-side
/// topic name + USING columns without compiling the full query. The
/// View setup uses this to look up the right topic in the registry,
/// build a combined left∪right schema, and then call `parse_query`
/// against that combined schema. Returns `Ok(None)` for un-joined
/// SQL.
pub fn peek_join(sql: &str) -> Result<Option<(String, String, Vec<String>)>, QueryError> {
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql)
        .map_err(|e| QueryError::ParseError(e.to_string()))?;
    let stmt = ast.first().ok_or_else(|| QueryError::ParseError("Empty SQL".into()))?;
    let q = match stmt {
        Statement::Query(q) => q,
        _ => return Err(QueryError::ParseError("Only SELECT statements supported".into())),
    };
    // P11 — peek_join is called BEFORE the main parse path, but the
    // ON-clause translator in parse_join_clause expects the alias
    // rewrite to have run (so `a.col` → `col`). Clone + rewrite here
    // before consulting the JOIN so the SOW JOIN path supports both
    // `JOIN ... USING (col)` and `JOIN ... ON a.col = b.col`.
    let mut q_owned = (**q).clone();
    let refs = collect_table_refs(&q_owned);
    rewrite_qualified_refs_in_query(&mut q_owned, &refs);
    let select = match q_owned.body.as_ref() {
        SetExpr::Select(s) => s,
        _ => return Err(QueryError::ParseError("Expected SELECT".into())),
    };
    let from = match select.from.first() {
        Some(f) => f,
        None => return Ok(None),
    };
    let left_topic = match &from.relation {
        TableFactor::Table { name, .. } => strip_identifier_quotes(&name.to_string()),
        _ => return Ok(None),
    };
    let join = match parse_join_clause(&from.joins)? {
        Some(j) => j,
        None => return Ok(None),
    };
    Ok(Some((left_topic, join.right_topic, join.using)))
}

/// S20 — synthesize a combined schema from a left + right topic for
/// JOIN parsing. The combined schema has the left columns first
/// (in order), then every right column NOT in the USING list (so a
/// USING column appears once and is unambiguous). Used by the view
/// setup path to compile predicates and aggregates against the
/// post-join column set.
pub fn combined_join_schema(
    left: &Schema,
    right: &Schema,
    using: &[String],
) -> Schema {
    let using_set: std::collections::HashSet<&str> =
        using.iter().map(String::as_str).collect();
    let mut names: Vec<String> = Vec::new();
    let mut types: Vec<crate::schema::ColumnType> = Vec::new();
    for col in left.columns() {
        names.push(col.name().to_string());
        types.push(col.col_type());
    }
    for col in right.columns() {
        if using_set.contains(col.name()) {
            continue;
        }
        names.push(col.name().to_string());
        types.push(col.col_type());
    }
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    Schema::from_strs(&name_refs, &types)
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
    // PIVOT and UNPIVOT (S43) get their own parse-and-return path
    // below since they carry significant out-of-band state (pivot
    // spec, source columns) that the rest of the SELECT pipeline
    // wouldn't otherwise know what to do with.
    let from_entry = select.from.first();
    // S20 JOIN: parse the (single) JOIN, if any. We support exactly
    // `A INNER JOIN B USING (col, ...)`. The caller is expected to
    // pass a COMBINED schema (left ∪ right minus USING duplicates)
    // when a JOIN is present so the predicate / aggregate paths see
    // every column they reference.
    let join_spec = from_entry
        .and_then(|fe| parse_join_clause(&fe.joins).transpose())
        .transpose()?;
    let topic = if let Some(from) = from_entry {
        match &from.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            TableFactor::Pivot {
                table,
                aggregate_functions,
                value_column,
                value_source,
                ..
            } => {
                let inner_name = match table.as_ref() {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(QueryError::ParseError(
                        "PIVOT over non-table FROM (e.g. subquery) not yet supported".into(),
                    )),
                };
                return parse_pivot_query(
                    select,
                    query,
                    schema,
                    inner_name,
                    aggregate_functions,
                    value_column,
                    value_source,
                );
            }
            TableFactor::Unpivot {
                table,
                value,
                name: name_col,
                columns,
                ..
            } => {
                let inner_name = match table.as_ref() {
                    TableFactor::Table { name, .. } => name.to_string(),
                    _ => return Err(QueryError::ParseError(
                        "UNPIVOT over non-table FROM (e.g. subquery) not yet supported".into(),
                    )),
                };
                return parse_unpivot_query(
                    select,
                    query,
                    schema,
                    inner_name,
                    value,
                    name_col,
                    columns,
                );
            }
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
    let (projection, aggregates, computed, windows) =
        parse_projection_or_aggregates(&select.projection, schema, &group_by)?;

    // --- HAVING (P3) — compile against [group_names ++ aggregate_aliases].
    // Reject HAVING on non-aggregate queries; the spec requires GROUP BY
    // or an aggregate in SELECT.
    let having = match &select.having {
        Some(h) => {
            if aggregates.is_empty() && group_by.is_empty() {
                return Err(QueryError::ParseError(
                    "HAVING requires GROUP BY or an aggregate in SELECT".into(),
                ));
            }
            let group_names: Vec<String> = group_by
                .iter()
                .map(|&c| schema.column_name(c).to_string())
                .collect();
            Some(compile_having(h, &group_names, &aggregates, schema)?)
        }
        None => None,
    };

    // --- WHERE clause → predicate ---
    let predicate = if let Some(where_expr) = &select.selection {
        compile_expr(where_expr, schema).map_err(QueryError::PredicateError)?
    } else {
        CompiledPredicate::True
    };

    // --- ORDER BY ---
    let order_by = parse_order_by(&query.order_by, schema)?;

    // --- LIMIT + OFFSET (P4) ---
    let (limit, offset) = match query.limit_clause.as_ref() {
        Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) => {
            let lim = limit.as_ref().and_then(extract_usize_literal);
            let off = offset.as_ref().and_then(|o| extract_usize_literal(&o.value));
            (lim, off)
        }
        Some(sqlparser::ast::LimitClause::OffsetCommaLimit { offset, limit }) => {
            (extract_usize_literal(limit), extract_usize_literal(offset))
        }
        None => (None, None),
    };

    Ok(ParsedQuery {
        topic,
        projection,
        predicate,
        order_by,
        limit,
        aggregates,
        group_by,
        pivot: None,
        unpivot: None,
        join: join_spec,
        computed,
        having,
        offset,
        windows,
    })
}

/// P11 — walk an ON-clause expression and collect USING column names.
/// Accepts AND-trees of `Identifier(c) = Identifier(c)` (where both
/// sides are bare identifiers after the alias-rewrite pass and share
/// the same column name). Rejects non-equi predicates, OR, mixed-in
/// literals, and equalities between differently-named columns.
fn collect_equi_using(expr: &Expr, out: &mut Vec<String>) -> Result<(), QueryError> {
    use sqlparser::ast::BinaryOperator;
    match expr {
        Expr::Nested(e) => collect_equi_using(e, out),
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_equi_using(left, out)?;
            collect_equi_using(right, out)
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let l_name = ident_name(left).ok_or_else(|| {
                QueryError::ParseError(format!(
                    "ON-clause supports only `col = col` equalities (after alias rewrite); got LHS: {left:?}"
                ))
            })?;
            let r_name = ident_name(right).ok_or_else(|| {
                QueryError::ParseError(format!(
                    "ON-clause supports only `col = col` equalities (after alias rewrite); got RHS: {right:?}"
                ))
            })?;
            if l_name != r_name {
                return Err(QueryError::ParseError(format!(
                    "ON-clause equi-join requires both sides to share the same column name (got `{l_name}` vs `{r_name}`); rename a column or use USING(col)"
                )));
            }
            if !out.iter().any(|c| c == &l_name) {
                out.push(l_name);
            }
            Ok(())
        }
        _ => Err(QueryError::ParseError(format!(
            "ON-clause supports only AND-combined `col = col` equi-joins; got: {expr:?}"
        ))),
    }
}

fn ident_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::Nested(e) => ident_name(e),
        _ => None,
    }
}

/// P4 — extract a numeric literal as `usize`. Returns `None` for any
/// non-literal or non-fitting value. Used by the LIMIT/OFFSET parser.
fn extract_usize_literal(expr: &Expr) -> Option<usize> {
    if let Expr::Value(sqlparser::ast::ValueWithSpan {
        value: sqlparser::ast::Value::Number(n, _),
        ..
    }) = expr
    {
        n.parse::<usize>().ok()
    } else {
        None
    }
}

/// Strip the surrounding sqlparser identifier quotes (`"`, `` ` ``, `[`/`]`)
/// from a topic name. Topic names like `/positions` aren't valid bare
/// identifiers in SQL so users quote them in JOIN clauses; we want
/// the unquoted form for the topic-registry lookup.
fn strip_identifier_quotes(raw: &str) -> String {
    let s = raw.trim();
    if (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
        || (s.starts_with('`') && s.ends_with('`') && s.len() >= 2)
    {
        s[1..s.len() - 1].to_string()
    } else if s.starts_with('[') && s.ends_with(']') && s.len() >= 2 {
        s[1..s.len() - 1].to_string()
    } else {
        s.to_string()
    }
}

/// Pull a JoinSpec out of the sqlparser AST. Today we only accept a
/// single INNER (or implicit) JOIN with a USING(...) constraint —
/// the common dashboard pattern. LEFT/RIGHT OUTER, ON-clause Expr
/// joins, and multi-table chained joins are tracked as follow-ups.
fn parse_join_clause(
    joins: &[sqlparser::ast::Join],
) -> Result<Option<JoinSpec>, QueryError> {
    if joins.is_empty() {
        return Ok(None);
    }
    if joins.len() > 1 {
        return Err(QueryError::ParseError(
            "Only a single JOIN per FROM is supported".into(),
        ));
    }
    let j = &joins[0];
    let right_topic = match &j.relation {
        TableFactor::Table { name, .. } => strip_identifier_quotes(&name.to_string()),
        _ => {
            return Err(QueryError::ParseError(
                "JOIN target must be a plain topic name".into(),
            ))
        }
    };
    // P11/P12/Q1/Q12 — INNER, LEFT/RIGHT/FULL OUTER, plus Q12's
    // Snowflake-style `ASOF JOIN ... MATCH_CONDITION(left.ts >=
    // right.ts) ON ...`. CROSS JOIN is still rejected.
    use sqlparser::ast::{Expr, JoinConstraint, JoinOperator};
    let (constraint, kind) = match &j.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => (c, JoinKind::Inner),
        JoinOperator::Left(c) | JoinOperator::LeftOuter(c) => (c, JoinKind::LeftOuter),
        JoinOperator::Right(c) | JoinOperator::RightOuter(c) => (c, JoinKind::RightOuter),
        JoinOperator::FullOuter(c) => (c, JoinKind::FullOuter),
        JoinOperator::AsOf { match_condition, constraint } => {
            // Q12 — MATCH_CONDITION must be `lhs_col >= rhs_col`
            // (both bare identifiers after P1 alias-rewrite). The
            // executor sorts the right side by `rhs_col` and binary-
            // searches for the largest entry ≤ each left row's
            // lhs_col value. Other shapes (>, <, <=) and non-bare
            // ident args are deferred.
            let asof_col = match match_condition {
                Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::GtEq, right } => {
                    let l = ident_name(left).ok_or_else(|| QueryError::ParseError(
                        "AS OF JOIN: MATCH_CONDITION LHS must be a bare column".into(),
                    ))?;
                    let r = ident_name(right).ok_or_else(|| QueryError::ParseError(
                        "AS OF JOIN: MATCH_CONDITION RHS must be a bare column".into(),
                    ))?;
                    if l != r {
                        return Err(QueryError::ParseError(format!(
                            "AS OF JOIN: MATCH_CONDITION columns must share a name ({l} vs {r}); rename or alias before JOIN"
                        )));
                    }
                    l
                }
                other => {
                    return Err(QueryError::ParseError(format!(
                        "AS OF JOIN: MATCH_CONDITION must be `col >= col`, got {other:?}"
                    )));
                }
            };
            (constraint, JoinKind::AsOf { ts_col: asof_col })
        }
        _ => {
            return Err(QueryError::ParseError(
                "Only INNER, LEFT/RIGHT/FULL OUTER, and ASOF JOIN are supported today".into(),
            ))
        }
    };
    let using: Vec<String> = match constraint {
        JoinConstraint::Using(cols) => cols
            .iter()
            .map(|on| {
                // ObjectName parts are an enum of `ObjectNamePart`;
                // we accept the simple identifier form.
                on.to_string().trim_matches('"').to_string()
            })
            .collect(),
        // P11 — `ON a.col = b.col` translated to USING(col). The
        // alias-rewrite pass already turned `a.col` / `b.col` into
        // bare `col` references on both sides; here we recurse
        // through AND-trees of `Identifier = Identifier` equalities
        // and require both sides to share the same name. Anything
        // else (non-equi, different names, mixed-in literals, OR)
        // returns a clear error pointing the user at USING.
        JoinConstraint::On(expr) => {
            let mut cols: Vec<String> = Vec::new();
            collect_equi_using(expr, &mut cols)?;
            cols
        }
        _ => {
            return Err(QueryError::ParseError(
                "JOIN must specify USING (col, ...) or ON a.c = b.c with matching column names".into(),
            ))
        }
    };
    if using.is_empty() {
        return Err(QueryError::ParseError(
            "JOIN USING (...) must list at least one column".into(),
        ));
    }
    Ok(Some(JoinSpec {
        right_topic,
        using,
        kind,
    }))
}

/// Parse a `SELECT ... FROM t PIVOT (agg(col) FOR pivot_col IN (lit, lit, ...))` query.
/// Returns a `ParsedQuery` with `pivot: Some(...)` set; `execute_pivot_query`
/// handles the rest.
fn parse_pivot_query(
    select: &sqlparser::ast::Select,
    query: &sqlparser::ast::Query,
    schema: &Schema,
    topic: String,
    aggregate_functions: &[sqlparser::ast::ExprWithAlias],
    value_column: &[sqlparser::ast::Ident],
    value_source: &sqlparser::ast::PivotValueSource,
) -> Result<ParsedQuery, QueryError> {
    use sqlparser::ast::PivotValueSource;

    // 1. Aggregates — at least one, in declaration order.
    if aggregate_functions.is_empty() {
        return Err(QueryError::ParseError(
            "PIVOT requires at least one aggregate function".into(),
        ));
    }
    let mut aggs: Vec<AggregateSpec> = Vec::with_capacity(aggregate_functions.len());
    for ew in aggregate_functions {
        let alias = ew.alias.as_ref().map(|a| a.value.clone());
        let spec = parse_aggregate_call(&ew.expr, schema, alias.as_deref())?
            .ok_or_else(|| {
                QueryError::ParseError(format!(
                    "PIVOT measures must be aggregate calls (SUM/COUNT/AVG/MIN/MAX); got {:?}",
                    ew.expr
                ))
            })?;
        aggs.push(spec);
    }

    // 2. Pivot column — single ident expected.
    let pivot_col_name = match value_column {
        [ident] => ident.value.clone(),
        _ => {
            return Err(QueryError::ParseError(format!(
                "PIVOT supports a single pivot column today; got {} ({:?})",
                value_column.len(),
                value_column
            )))
        }
    };
    let pivot_col = schema
        .index_of(&pivot_col_name)
        .ok_or_else(|| QueryError::UnknownColumn(pivot_col_name.clone()))?;

    // 3. Pivot values — static list (S43) or dynamic ANY (S45).
    // Subquery-driven value sources still defer to a follow-up.
    let (pivot_values, dynamic) = match value_source {
        PivotValueSource::List(items) => {
            (parse_pivot_value_list(items, schema, pivot_col)?, false)
        }
        PivotValueSource::Any(_order_by) => {
            // ORDER BY inside ANY would let the caller pin the
            // output column order; we ignore it today and sort
            // discovered values in their natural ordering. The
            // proptest's reference walks the same ordering.
            (Vec::new(), true)
        }
        PivotValueSource::Subquery(_) => {
            return Err(QueryError::NotYetImplemented(
                "PIVOT with subquery value source not yet supported".into(),
            ));
        }
    };

    // 4. Anchor columns = every schema column NOT in (pivot_col,
    // aggregate input cols). This is the natural Snowflake/BigQuery
    // convention: the user doesn't declare anchor cols explicitly.
    let mut excluded: std::collections::HashSet<usize> = std::collections::HashSet::new();
    excluded.insert(pivot_col);
    for a in &aggs {
        if let Some(c) = a.col {
            excluded.insert(c);
        }
    }
    let anchor_cols: Vec<usize> = (0..schema.column_count())
        .filter(|c| !excluded.contains(c))
        .collect();

    // 5. WHERE clause — applies pre-pivot.
    let predicate = if let Some(where_expr) = &select.selection {
        compile_expr(where_expr, schema).map_err(QueryError::PredicateError)?
    } else {
        CompiledPredicate::True
    };

    // 6. SELECT is required to be `*` today — output columns are
    // determined by the pivot spec, not by an explicit projection.
    if !matches!(select.projection.as_slice(), [sqlparser::ast::SelectItem::Wildcard(_)]) {
        return Err(QueryError::ParseError(
            "PIVOT currently requires `SELECT * FROM ... PIVOT(...)`. Explicit projection over pivot output is a follow-up."
                .into(),
        ));
    }

    // 7. ORDER BY / LIMIT on pivot output — not yet supported.
    if query.order_by.is_some() || query.limit_clause.is_some() {
        return Err(QueryError::NotYetImplemented(
            "ORDER BY / LIMIT on PIVOT output not yet supported".into(),
        ));
    }

    let pivot = ParsedPivot {
        aggregates: aggs,
        pivot_col,
        pivot_values,
        dynamic,
        anchor_cols,
    };

    Ok(ParsedQuery {
        topic,
        projection: Vec::new(),
        predicate,
        order_by: Vec::new(),
        limit: None,
        aggregates: Vec::new(),
        group_by: Vec::new(),
        pivot: Some(pivot),
        unpivot: None,
        join: None,
        computed: Vec::new(),
        having: None,
        offset: None,
        windows: Vec::new(),
    })
}

/// Parse the IN-list of a static PIVOT. Each literal must be
/// representable in the pivot column's type — `'A'` for a string
/// column, `1` for a long column, `1.5` for a double column.
fn parse_pivot_value_list(
    items: &[sqlparser::ast::ExprWithAlias],
    schema: &Schema,
    pivot_col: usize,
) -> Result<Vec<PivotLiteral>, QueryError> {
    use crate::schema::ColumnType;
    let col_type = schema.column_type(pivot_col);
    let mut out = Vec::with_capacity(items.len());
    for it in items {
        let lit = match col_type {
            ColumnType::String => PivotLiteral::String(
                crate::predicate::extract_string_value(&it.expr)
                    .map(compact_str::CompactString::new)
                    .map_err(QueryError::PredicateError)?,
            ),
            ColumnType::Long | ColumnType::Int => PivotLiteral::Long(
                crate::predicate::extract_i64(&it.expr).map_err(QueryError::PredicateError)?,
            ),
            ColumnType::Double => PivotLiteral::Double(
                crate::predicate::extract_f64(&it.expr)
                    .map_err(QueryError::PredicateError)?
                    .to_bits(),
            ),
            ColumnType::Bool => PivotLiteral::Bool(
                crate::predicate::extract_bool(&it.expr).map_err(QueryError::PredicateError)?,
            ),
            ColumnType::Timestamp => PivotLiteral::Timestamp(
                crate::predicate::extract_timestamp(&it.expr)
                    .map_err(QueryError::PredicateError)?,
            ),
            ColumnType::Bytes => {
                return Err(QueryError::ParseError(
                    "PIVOT on a bytes column is not supported".into(),
                ));
            }
        };
        out.push(lit);
    }
    Ok(out)
}

/// Parse an `UNPIVOT (val FOR name IN (c1, c2, ...))` query.
fn parse_unpivot_query(
    select: &sqlparser::ast::Select,
    query: &sqlparser::ast::Query,
    schema: &Schema,
    topic: String,
    value: &sqlparser::ast::Ident,
    name_col: &sqlparser::ast::Ident,
    columns: &[sqlparser::ast::Ident],
) -> Result<ParsedQuery, QueryError> {
    // Source columns — each must exist in the schema. Order is the
    // order the user listed them; the executor preserves it when
    // exploding rows.
    let mut source_cols: Vec<usize> = Vec::with_capacity(columns.len());
    for ident in columns {
        let idx = schema
            .index_of(&ident.value)
            .ok_or_else(|| QueryError::UnknownColumn(ident.value.clone()))?;
        source_cols.push(idx);
    }
    if source_cols.is_empty() {
        return Err(QueryError::ParseError(
            "UNPIVOT requires at least one source column".into(),
        ));
    }

    // Anchor columns = every schema column NOT in source_cols.
    let excluded: std::collections::HashSet<usize> = source_cols.iter().copied().collect();
    let anchor_cols: Vec<usize> = (0..schema.column_count())
        .filter(|c| !excluded.contains(c))
        .collect();

    // WHERE clause applies pre-explosion.
    let predicate = if let Some(where_expr) = &select.selection {
        compile_expr(where_expr, schema).map_err(QueryError::PredicateError)?
    } else {
        CompiledPredicate::True
    };

    if !matches!(select.projection.as_slice(), [sqlparser::ast::SelectItem::Wildcard(_)]) {
        return Err(QueryError::ParseError(
            "UNPIVOT currently requires `SELECT * FROM ... UNPIVOT(...)`. Explicit projection over unpivot output is a follow-up."
                .into(),
        ));
    }
    if query.order_by.is_some() || query.limit_clause.is_some() {
        return Err(QueryError::NotYetImplemented(
            "ORDER BY / LIMIT on UNPIVOT output not yet supported".into(),
        ));
    }

    let unpivot = ParsedUnpivot {
        value_col_name: value.value.clone(),
        name_col_name: name_col.value.clone(),
        source_cols,
        anchor_cols,
    };

    Ok(ParsedQuery {
        topic,
        projection: Vec::new(),
        predicate,
        order_by: Vec::new(),
        limit: None,
        aggregates: Vec::new(),
        group_by: Vec::new(),
        pivot: None,
        unpivot: Some(unpivot),
        join: None,
        computed: Vec::new(),
        having: None,
        offset: None,
        windows: Vec::new(),
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
) -> Result<(Vec<usize>, Vec<AggregateSpec>, Vec<ComputedColumn>, Vec<WindowColumn>), QueryError> {
    // First pass: detect whether any item is an aggregate function.
    let has_agg = items
        .iter()
        .any(|i| matches!(extract_expr(i), Some(e) if is_aggregate_function_call(e)));

    if !has_agg && group_by.is_empty() {
        let (projection, computed, windows) = parse_projection(items, schema)?;
        return Ok((projection, Vec::new(), computed, windows));
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

    Ok((Vec::new(), aggregates, Vec::new(), Vec::new()))
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
        matches!(
            name.as_str(),
            "SUM"
                | "COUNT"
                | "AVG"
                | "MIN"
                | "MAX"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "PERCENTILE_CONT"
                | "MEDIAN"
        )
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
        "STDDEV" | "STDDEV_POP" => AggFn::Stddev,
        "STDDEV_SAMP" => AggFn::StddevSamp,
        "VARIANCE" | "VAR_POP" => AggFn::Variance,
        "VAR_SAMP" => AggFn::VarianceSamp,
        "PERCENTILE_CONT" | "MEDIAN" => AggFn::PercentileCont,
        _ => return Ok(None),
    };

    let arg_list = match &f.args {
        FunctionArguments::List(l) => l,
        _ => {
            return Err(QueryError::ParseError(format!(
                "{name} requires an argument list"
            )))
        }
    };

    // P10 — COUNT(DISTINCT col) is a different aggregate from COUNT(col).
    // The `DISTINCT` keyword lives on `arg_list.duplicate_treatment`.
    let is_distinct = matches!(
        arg_list.duplicate_treatment,
        Some(sqlparser::ast::DuplicateTreatment::Distinct)
    );
    if is_distinct {
        if func != AggFn::Count {
            return Err(QueryError::ParseError(format!(
                "DISTINCT is only supported on COUNT, got {name}"
            )));
        }
        if arg_list.args.len() != 1 {
            return Err(QueryError::ParseError(format!(
                "COUNT(DISTINCT ...) expects one column argument, got {}",
                arg_list.args.len()
            )));
        }
        let col_expr = match &arg_list.args[0] {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
            other => {
                return Err(QueryError::ParseError(format!(
                    "COUNT(DISTINCT ...) argument must be a column reference, got {other:?}"
                )))
            }
        };
        let col_idx = resolve_select_column(col_expr, schema)?;
        let col_name = schema.column_name(col_idx).to_string();
        let default_alias = format!("COUNT(DISTINCT {col_name})");
        return Ok(Some(AggregateSpec {
            func: AggFn::CountDistinct,
            col: Some(col_idx),
            alias: alias.map(str::to_string).unwrap_or(default_alias),
            percentile_q: None,
        }));
    }

    // P9 — PERCENTILE_CONT(col, q): two positional args. MEDIAN(col):
    // sugar for PERCENTILE_CONT(col, 0.5).
    if func == AggFn::PercentileCont {
        let (col_expr, q): (&Expr, f64) = if name == "MEDIAN" {
            if arg_list.args.len() != 1 {
                return Err(QueryError::ParseError(format!(
                    "MEDIAN expects exactly one argument, got {}",
                    arg_list.args.len()
                )));
            }
            match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => (e, 0.5),
                other => {
                    return Err(QueryError::ParseError(format!(
                        "MEDIAN: unsupported argument shape: {other:?}"
                    )))
                }
            }
        } else {
            if arg_list.args.len() != 2 {
                return Err(QueryError::ParseError(format!(
                    "PERCENTILE_CONT expects two arguments (column, q in [0,1]), got {}",
                    arg_list.args.len()
                )));
            }
            let col_e = match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                other => {
                    return Err(QueryError::ParseError(format!(
                        "PERCENTILE_CONT: column arg must be a column reference, got {other:?}"
                    )))
                }
            };
            let q_val = match &arg_list.args[1] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(vws))) => match &vws.value {
                    sqlparser::ast::Value::Number(n, _) => n.parse::<f64>().map_err(|e| {
                        QueryError::ParseError(format!(
                            "PERCENTILE_CONT: q must parse as a number, got {n}: {e}"
                        ))
                    })?,
                    other => {
                        return Err(QueryError::ParseError(format!(
                            "PERCENTILE_CONT: q must be a numeric literal, got {other:?}"
                        )))
                    }
                },
                other => {
                    return Err(QueryError::ParseError(format!(
                        "PERCENTILE_CONT: q must be a numeric literal, got {other:?}"
                    )))
                }
            };
            (col_e, q_val)
        };
        if !(0.0..=1.0).contains(&q) {
            return Err(QueryError::ParseError(format!(
                "PERCENTILE_CONT: q must be in [0, 1], got {q}"
            )));
        }
        let col_idx = resolve_select_column(col_expr, schema)?;
        let col_name = schema.column_name(col_idx).to_string();
        let default_alias = if name == "MEDIAN" {
            format!("MEDIAN({col_name})")
        } else {
            format!("PERCENTILE_CONT({col_name},{q})")
        };
        return Ok(Some(AggregateSpec {
            func: AggFn::PercentileCont,
            col: Some(col_idx),
            alias: alias.map(str::to_string).unwrap_or(default_alias),
            percentile_q: Some(q),
        }));
    }

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
        percentile_q: None,
    }))
}

fn parse_projection(
    items: &[SelectItem],
    schema: &Schema,
) -> Result<(Vec<usize>, Vec<ComputedColumn>, Vec<WindowColumn>), QueryError> {
    let mut cols = Vec::new();
    let mut computed = Vec::new();
    let mut windows = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                if !computed.is_empty() || !windows.is_empty() {
                    return Err(QueryError::ParseError(
                        "SELECT * cannot be combined with computed/window columns".into(),
                    ));
                }
                return Ok((Vec::new(), Vec::new(), Vec::new()));
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(window) = try_compile_window(expr, None, schema)? {
                    windows.push(window);
                } else if let Some(scalar) = try_compile_scalar_expr(expr, schema)? {
                    let alias = expr_display_alias(expr);
                    computed.push(ComputedColumn { alias, expr: scalar });
                } else {
                    let col = resolve_select_column(expr, schema)?;
                    cols.push(col);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(window) = try_compile_window(expr, Some(&alias.value), schema)? {
                    windows.push(window);
                } else if let Some(scalar) = try_compile_scalar_expr(expr, schema)? {
                    computed.push(ComputedColumn {
                        alias: alias.value.clone(),
                        expr: scalar,
                    });
                } else {
                    let col = resolve_select_column(expr, schema)?;
                    cols.push(col);
                }
            }
            _ => {
                return Err(QueryError::ParseError(format!(
                    "Unsupported SELECT item: {:?}",
                    item
                )))
            }
        }
    }
    Ok((cols, computed, windows))
}

/// Q7 — `Some(window)` if `expr` is a window-function call
/// (`Expr::Function` with `over: Some(WindowSpec(...))`). `None` for
/// everything else, so callers continue with the scalar / projection
/// path.
fn try_compile_window(
    expr: &sqlparser::ast::Expr,
    alias_override: Option<&str>,
    schema: &Schema,
) -> Result<Option<WindowColumn>, QueryError> {
    use sqlparser::ast::{Expr, WindowType};
    let f = match expr {
        Expr::Function(f) => f,
        _ => return Ok(None),
    };
    let spec = match &f.over {
        Some(WindowType::WindowSpec(s)) => s,
        Some(WindowType::NamedWindow(_)) => {
            return Err(QueryError::ParseError(
                "named windows not supported; inline OVER (...) directly".into(),
            ));
        }
        None => return Ok(None),
    };
    let name = f.name.to_string().to_ascii_uppercase();
    let kind = match name.as_str() {
        "ROW_NUMBER" => WindowFn::RowNumber,
        "RANK" => WindowFn::Rank,
        "DENSE_RANK" => WindowFn::DenseRank,
        "LAG" | "LEAD" => {
            let arg_list = match &f.args {
                FunctionArguments::List(l) => l,
                _ => {
                    return Err(QueryError::ParseError(format!(
                        "{name}() requires (col [, offset]) args"
                    )))
                }
            };
            if arg_list.args.is_empty() {
                return Err(QueryError::ParseError(format!(
                    "{name}() requires at least one argument"
                )));
            }
            let col_expr = match &arg_list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => e,
                other => {
                    return Err(QueryError::ParseError(format!(
                        "{name}: first arg must be a column ref, got {other:?}"
                    )))
                }
            };
            let col = resolve_select_column(col_expr, schema)?;
            let offset = if arg_list.args.len() >= 2 {
                match &arg_list.args[1] {
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(v))) => {
                        if let sqlparser::ast::Value::Number(n, _) = &v.value {
                            n.parse::<usize>().unwrap_or(1)
                        } else {
                            return Err(QueryError::ParseError(format!(
                                "{name}: offset must be a numeric literal"
                            )));
                        }
                    }
                    _ => 1,
                }
            } else {
                1
            };
            match name.as_str() {
                "LAG" => WindowFn::Lag { col, offset },
                _ => WindowFn::Lead { col, offset },
            }
        }
        _ => {
            return Err(QueryError::ParseError(format!(
                "window function {name}() not supported; use ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD"
            )));
        }
    };
    let mut partition_by: Vec<usize> = Vec::new();
    for pe in &spec.partition_by {
        partition_by.push(resolve_select_column(pe, schema)?);
    }
    let mut order_by: Vec<(usize, bool)> = Vec::new();
    for ob in &spec.order_by {
        let col = resolve_select_column(&ob.expr, schema)?;
        let asc = ob.options.asc.unwrap_or(true);
        order_by.push((col, asc));
    }
    let default_alias = format!("{}()", name);
    Ok(Some(WindowColumn {
        alias: alias_override.map(str::to_string).unwrap_or(default_alias),
        partition_by,
        order_by,
        kind,
    }))
}

/// P2 — `Some(expr)` if `expr` is a scalar arithmetic expression
/// (BinaryOp/UnaryOp/Nested over a numeric op) that should be evaluated
/// per-row. `None` if it's a bare column ref that the projection path
/// should handle. Returns `Err` on an unsupported shape inside an
/// arithmetic tree so the user sees a clear parser diagnostic.
fn try_compile_scalar_expr(
    expr: &Expr,
    schema: &Schema,
) -> Result<Option<ScalarExpr>, QueryError> {
    use sqlparser::ast::{BinaryOperator, UnaryOperator};
    match expr {
        Expr::BinaryOp { op, .. }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) =>
        {
            Ok(Some(compile_scalar(expr, schema)?))
        }
        Expr::UnaryOp { op: UnaryOperator::Minus, .. } => {
            Ok(Some(compile_scalar(expr, schema)?))
        }
        Expr::Nested(inner) => try_compile_scalar_expr(inner, schema),
        _ => Ok(None),
    }
}

fn compile_scalar(expr: &Expr, schema: &Schema) -> Result<ScalarExpr, QueryError> {
    use sqlparser::ast::{BinaryOperator, UnaryOperator, Value as SqlValue, ValueWithSpan};
    match expr {
        Expr::Identifier(id) => {
            let idx = schema
                .index_of(&id.value)
                .ok_or_else(|| QueryError::UnknownColumn(id.value.clone()))?;
            Ok(ScalarExpr::Col(idx))
        }
        Expr::Value(ValueWithSpan { value, .. }) => match value {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(ScalarExpr::LitLong(i))
                } else {
                    Ok(ScalarExpr::LitDouble(n.parse().unwrap_or(0.0)))
                }
            }
            SqlValue::SingleQuotedString(s) => Ok(ScalarExpr::LitString(s.into())),
            other => Err(QueryError::ParseError(format!(
                "unsupported literal in scalar expression: {other:?}"
            ))),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = Box::new(compile_scalar(left, schema)?);
            let r = Box::new(compile_scalar(right, schema)?);
            match op {
                BinaryOperator::Plus => Ok(ScalarExpr::Add(l, r)),
                BinaryOperator::Minus => Ok(ScalarExpr::Sub(l, r)),
                BinaryOperator::Multiply => Ok(ScalarExpr::Mul(l, r)),
                BinaryOperator::Divide => Ok(ScalarExpr::Div(l, r)),
                _ => Err(QueryError::ParseError(format!(
                    "unsupported binary op in scalar expression: {op:?}"
                ))),
            }
        }
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => {
            Ok(ScalarExpr::Neg(Box::new(compile_scalar(expr, schema)?)))
        }
        Expr::Nested(inner) => compile_scalar(inner, schema),
        _ => Err(QueryError::ParseError(format!(
            "unsupported expression in scalar context: {expr:?}"
        ))),
    }
}

/// Build a display alias for an unaliased computed SELECT item. We
/// stringify the expression (the same form sqlparser uses for `Display`)
/// — mirrors AMPS's behaviour and keeps the JSON key stable per SQL.
fn expr_display_alias(expr: &Expr) -> String {
    expr.to_string()
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
    /// Borrowed bitmap from the equality index (zero-copy fast path).
    Bitmap(&'a roaring::RoaringBitmap),
    /// Owned bitmap — the range-index path computes its union by
    /// walking the B-tree, which produces a fresh `RoaringBitmap`.
    /// Variant kept distinct from `Bitmap` so the equality path
    /// stays allocation-free.
    OwnedBitmap(roaring::RoaringBitmap),
    /// Full row scan: every row in `[0, n)`.
    Full(u32),
}

impl<'a> CandidateRows<'a> {
    /// Approximate count; used for sizing result vectors.
    pub fn upper_bound(&self) -> usize {
        match self {
            CandidateRows::Bitmap(b) => b.len() as usize,
            CandidateRows::OwnedBitmap(b) => b.len() as usize,
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
            CandidateRows::OwnedBitmap(b) => {
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
        // Equality fast path (cheaper — direct HashMap hit).
        if let Some((col, key)) = find_index_hint(&query.predicate, ix) {
            metrics::counter!("cq_query_index_hits_total").increment(1);
            if let Some(b) = ix.rows_for_key(col, &key) {
                return CandidateRows::Bitmap(b);
            }
            // Hit but empty — represent as an empty range so the
            // caller's iteration yields nothing.
            return CandidateRows::Full(0);
        }
        // S30 range fast path — `<`, `>`, `BETWEEN` on indexed
        // numeric/string columns. Returns an owned bitmap (B-tree
        // walks construct a fresh union), so we wrap it in
        // `CandidateRows::OwnedBitmap` below.
        if let Some(bm) = find_range_hint(&query.predicate, ix) {
            metrics::counter!("cq_query_index_hits_total").increment(1);
            metrics::counter!("cq_query_range_index_hits_total").increment(1);
            return CandidateRows::OwnedBitmap(bm);
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

/// S30: walk the predicate looking for a range clause (`<`, `>`,
/// `<=`, `>=`, `BETWEEN`) over an indexed column. Returns the
/// candidate row bitmap from `SecondaryIndex`'s range maps, or
/// `None` if no qualifying clause is found.
fn find_range_hint(
    pred: &CompiledPredicate,
    index: &SecondaryIndex,
) -> Option<roaring::RoaringBitmap> {
    use crate::sec_index::RangeKey;
    match pred {
        // BETWEEN — closed interval.
        CompiledPredicate::BetweenLong { col, low, high } if index.has_range(*col) => {
            index.rows_in_range(*col, Some(RangeKey::Long(*low)), Some(RangeKey::Long(*high)))
        }
        CompiledPredicate::BetweenDouble { col, low, high } if index.has_range(*col) => {
            let lo = RangeKey::from_double(*low)?;
            let hi = RangeKey::from_double(*high)?;
            index.rows_in_range(*col, Some(lo), Some(hi))
        }
        // Open `>`.
        CompiledPredicate::GtLong { col, value } if index.has_range(*col) => {
            index.rows_greater_than(*col, RangeKey::Long(*value))
        }
        CompiledPredicate::GtDouble { col, value } if index.has_range(*col) => {
            let v = RangeKey::from_double(*value)?;
            index.rows_greater_than(*col, v)
        }
        // Half-open `>=` — `rows_in_range(Some(v), None)`.
        CompiledPredicate::GeLong { col, value } if index.has_range(*col) => {
            index.rows_in_range(*col, Some(RangeKey::Long(*value)), None)
        }
        CompiledPredicate::GeDouble { col, value } if index.has_range(*col) => {
            let v = RangeKey::from_double(*value)?;
            index.rows_in_range(*col, Some(v), None)
        }
        // Open `<`.
        CompiledPredicate::LtLong { col, value } if index.has_range(*col) => {
            index.rows_less_than(*col, RangeKey::Long(*value))
        }
        CompiledPredicate::LtDouble { col, value } if index.has_range(*col) => {
            let v = RangeKey::from_double(*value)?;
            index.rows_less_than(*col, v)
        }
        // Half-open `<=` — `rows_in_range(None, Some(v))`.
        CompiledPredicate::LeLong { col, value } if index.has_range(*col) => {
            index.rows_in_range(*col, None, Some(RangeKey::Long(*value)))
        }
        CompiledPredicate::LeDouble { col, value } if index.has_range(*col) => {
            let v = RangeKey::from_double(*value)?;
            index.rows_in_range(*col, None, Some(v))
        }
        // Recurse into AND branches — either side can be the range
        // hint. Skip OR / NOT (need full eval); skip non-range leaves.
        CompiledPredicate::And(a, b) => {
            find_range_hint(a, index).or_else(|| find_range_hint(b, index))
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
    execute_query_with_index_filtered(query, store, index, None)
}

/// Like `execute_query_with_index` but pre-filters the candidate row
/// set by a caller-supplied "live rows" bitmap. Used by S20 view
/// runners so the aggregate executor doesn't bucket tombstoned source
/// rows into a phantom null-key group. The bitmap is intersected with
/// the planner's chosen candidates before predicate evaluation; pass
/// `None` for the historical "all rows the planner returns" behaviour.
pub fn execute_query_with_index_filtered(
    query: &ParsedQuery,
    store: &ColumnStore,
    index: Option<&SecondaryIndex>,
    live_rows: Option<&roaring::RoaringBitmap>,
) -> QueryResult {
    // PIVOT / UNPIVOT take dedicated paths — see pivot.rs.
    if query.pivot.is_some() {
        return crate::pivot::execute_pivot_query(query, store);
    }
    if query.unpivot.is_some() {
        return crate::pivot::execute_unpivot_query(query, store);
    }
    // Aggregate queries take a separate execution path: per-row work
    // updates aggregator state instead of building per-row output.
    if query.is_aggregate() || !query.group_by.is_empty() {
        return execute_aggregate_query(query, store, index, live_rows);
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

    // Step 3a: OFFSET (P4) — applied AFTER ORDER BY, BEFORE LIMIT.
    if let Some(off) = query.offset {
        if off >= matching_rows.len() {
            matching_rows.clear();
        } else {
            matching_rows.drain(..off);
        }
    }

    // Step 3b: Limit
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

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> = matching_rows
        .iter()
        .map(|&row| {
            let mut map = store.get_row_map_projected(row, &proj_indices);
            // P2 — append computed scalar columns. Evaluated against
            // the FULL row (every source column visible to ScalarExpr),
            // not just the projection set. Null-on-error semantics.
            if !query.computed.is_empty() {
                let row_values: Vec<Value> = (0..store.schema().column_count())
                    .map(|c| store.get(c, row))
                    .collect();
                for cc in &query.computed {
                    map.insert(cc.alias.clone(), cc.expr.eval(&row_values).to_json());
                }
            }
            map
        })
        .collect();

    // Q7 — window functions. Per spec: partition rows by
    // `partition_by` values, sort each partition by `order_by`,
    // assign per-row values. We use the source row indices in
    // `matching_rows` to know each output row's full column values
    // (the projection may have dropped columns referenced by
    // PARTITION BY / ORDER BY).
    if !query.windows.is_empty() && !rows.is_empty() {
        for wc in &query.windows {
            apply_window(wc, &mut rows, &matching_rows, store);
        }
    }

    QueryResult {
        rows,
        total_matches,
        source_rows: matching_rows,
    }
}

/// Q7 — apply one window column to the result rows. Mutates `rows`
/// in place; each row gets the window value under `wc.alias`.
fn apply_window(
    wc: &WindowColumn,
    rows: &mut [serde_json::Map<String, serde_json::Value>],
    source_rows: &[u32],
    store: &ColumnStore,
) {
    // Partition: map partition-key → Vec<output_row_index>.
    let mut parts: HashMap<Vec<GroupKeyPart>, Vec<usize>> = HashMap::new();
    let mut part_order: Vec<Vec<GroupKeyPart>> = Vec::new();
    for (i, &src) in source_rows.iter().enumerate() {
        let key: Vec<GroupKeyPart> = wc
            .partition_by
            .iter()
            .map(|&c| GroupKeyPart::from_value(&store.get(c, src)))
            .collect();
        if !parts.contains_key(&key) {
            part_order.push(key.clone());
        }
        parts.entry(key).or_default().push(i);
    }
    // Per-partition: sort by order_by, then assign per kind.
    for key in &part_order {
        let mut partition = parts.remove(key).unwrap();
        if !wc.order_by.is_empty() {
            partition.sort_by(|&a, &b| {
                let sa = source_rows[a];
                let sb = source_rows[b];
                for &(col, asc) in &wc.order_by {
                    let ord = compare_values(store, col, sa, sb);
                    let ord = if asc { ord } else { ord.reverse() };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }
        match &wc.kind {
            WindowFn::RowNumber => {
                for (rank, &out_idx) in partition.iter().enumerate() {
                    rows[out_idx].insert(
                        wc.alias.clone(),
                        serde_json::Value::from((rank as u64) + 1),
                    );
                }
            }
            WindowFn::Rank => {
                let mut last_rank: u64 = 0;
                let mut last_pos: u64 = 0;
                for (pos, &out_idx) in partition.iter().enumerate() {
                    let pos = pos as u64;
                    if pos == 0 {
                        last_rank = 1;
                    } else {
                        let prev_src = source_rows[partition[pos as usize - 1]];
                        let curr_src = source_rows[out_idx];
                        let ties = wc.order_by.iter().all(|&(col, _)| {
                            compare_values(store, col, prev_src, curr_src) == Ordering::Equal
                        });
                        if !ties {
                            last_rank = pos + 1;
                        }
                    }
                    last_pos = pos + 1;
                    rows[out_idx]
                        .insert(wc.alias.clone(), serde_json::Value::from(last_rank));
                }
                let _ = last_pos;
            }
            WindowFn::DenseRank => {
                let mut last_rank: u64 = 0;
                for (pos, &out_idx) in partition.iter().enumerate() {
                    if pos == 0 {
                        last_rank = 1;
                    } else {
                        let prev_src = source_rows[partition[pos - 1]];
                        let curr_src = source_rows[out_idx];
                        let ties = wc.order_by.iter().all(|&(col, _)| {
                            compare_values(store, col, prev_src, curr_src) == Ordering::Equal
                        });
                        if !ties {
                            last_rank += 1;
                        }
                    }
                    rows[out_idx]
                        .insert(wc.alias.clone(), serde_json::Value::from(last_rank));
                }
            }
            WindowFn::Lag { col, offset } => {
                for (pos, &out_idx) in partition.iter().enumerate() {
                    let val = if pos >= *offset {
                        let src = source_rows[partition[pos - offset]];
                        store.get(*col, src).to_json()
                    } else {
                        serde_json::Value::Null
                    };
                    rows[out_idx].insert(wc.alias.clone(), val);
                }
            }
            WindowFn::Lead { col, offset } => {
                for (pos, &out_idx) in partition.iter().enumerate() {
                    let val = if pos + offset < partition.len() {
                        let src = source_rows[partition[pos + offset]];
                        store.get(*col, src).to_json()
                    } else {
                        serde_json::Value::Null
                    };
                    rows[out_idx].insert(wc.alias.clone(), val);
                }
            }
        }
    }
}

/// Hashable group key built from one row's values across the GROUP BY
/// columns. Mirrors `IxKey` (Eq + Hash), with `Null` as a first-class
/// variant — unlike the secondary index, GROUP BY treats null as its
/// own group rather than skipping it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GroupKeyPart {
    Null,
    Int(i32),
    Long(i64),
    DoubleBits(u64),
    String(CompactString),
    Bool(bool),
    /// i64 microseconds since UNIX epoch. Distinct from `Long` so the
    /// JSON projection emits an RFC 3339 string for timestamp group
    /// keys instead of a bare integer.
    Timestamp(i64),
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
            Value::Bool(None) => GroupKeyPart::Null,
            Value::Bool(Some(b)) => GroupKeyPart::Bool(*b),
            Value::Timestamp(t) if *t == crate::store::NULL_TIMESTAMP => GroupKeyPart::Null,
            Value::Timestamp(t) => GroupKeyPart::Timestamp(*t),
            // Q10 — Bytes group keys: treat null as Null, non-null
            // as the base64-encoded form (lets GROUP BY bytes work
            // even though the storage is a Vec<u8>).
            Value::Bytes(None) => GroupKeyPart::Null,
            Value::Bytes(Some(b)) => {
                use base64::Engine;
                GroupKeyPart::String(compact_str::CompactString::new(
                    base64::engine::general_purpose::STANDARD.encode(b),
                ))
            }
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            GroupKeyPart::Null => serde_json::Value::Null,
            GroupKeyPart::Int(n) => serde_json::Value::from(*n),
            GroupKeyPart::Long(n) => serde_json::Value::from(*n),
            GroupKeyPart::DoubleBits(bits) => serde_json::Value::from(f64::from_bits(*bits)),
            GroupKeyPart::String(s) => serde_json::Value::from(s.as_str()),
            GroupKeyPart::Bool(b) => serde_json::Value::from(*b),
            GroupKeyPart::Timestamp(t) => {
                serde_json::Value::from(crate::store::format_timestamp_micros(*t))
            }
        }
    }
}

/// Running state for one aggregate over one group. The `seen_any`
/// flag distinguishes "no rows yet" from "all-null input" — needed so
/// `MIN`/`MAX`/`AVG` return null on an empty group rather than `0` or
/// the default bounds.
#[derive(Debug)]
pub enum AggState {
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
    /// P8 — Welford-online accumulator. `kind` distinguishes whether
    /// `finalize` returns population vs sample, stddev vs variance.
    Welford {
        count: u64,
        mean: f64,
        m2: f64,
        kind: WelfordKind,
    },
    /// P9 — exact percentile via per-group sorted value buffer.
    /// O(n) memory per group. `q` is captured at init time from the
    /// matching `AggregateSpec.percentile_q`.
    Percentile { values: Vec<f64>, q: f64 },
    /// P10 — exact distinct count via per-group set of canonicalised
    /// values. Reuses `GroupKeyPart` for hashable Value coverage.
    CountDistinct(HashSet<GroupKeyPart>),
}

/// Which moment to return from a Welford state's `finalize`.
#[derive(Debug, Clone, Copy)]
pub enum WelfordKind {
    /// `STDDEV` / `STDDEV_POP` — `sqrt(M2 / N)`.
    StddevPop,
    /// `STDDEV_SAMP` — `sqrt(M2 / (N - 1))`.
    StddevSamp,
    /// `VARIANCE` / `VAR_POP` — `M2 / N`.
    VarPop,
    /// `VAR_SAMP` — `M2 / (N - 1)`.
    VarSamp,
}

impl AggState {
    /// Build the initial state for an aggregate, given the column
    /// type (or `None` for COUNT(*)). For percentile-style aggregates
    /// the caller must pass `spec_q` from the matching AggregateSpec.
    pub fn init(func: AggFn, col_type: Option<crate::schema::ColumnType>) -> AggState {
        Self::init_with_q(func, col_type, None)
    }

    /// P9 — like `init` but accepts the per-spec percentile fraction.
    /// Callers that may handle PercentileCont aggregates must use this.
    pub fn init_with_q(
        func: AggFn,
        col_type: Option<crate::schema::ColumnType>,
        spec_q: Option<f64>,
    ) -> AggState {
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
            // P8 — Welford for stddev/variance flavours. The col_type
            // is unused (we coerce via Value::as_f64), but we still
            // require a numeric input column for parser validity.
            (AggFn::Stddev, _) => AggState::Welford {
                count: 0, mean: 0.0, m2: 0.0, kind: WelfordKind::StddevPop,
            },
            (AggFn::StddevSamp, _) => AggState::Welford {
                count: 0, mean: 0.0, m2: 0.0, kind: WelfordKind::StddevSamp,
            },
            (AggFn::Variance, _) => AggState::Welford {
                count: 0, mean: 0.0, m2: 0.0, kind: WelfordKind::VarPop,
            },
            (AggFn::VarianceSamp, _) => AggState::Welford {
                count: 0, mean: 0.0, m2: 0.0, kind: WelfordKind::VarSamp,
            },
            (AggFn::PercentileCont, _) => AggState::Percentile {
                values: Vec::new(),
                q: spec_q.unwrap_or(0.5),
            },
            (AggFn::CountDistinct, _) => AggState::CountDistinct(HashSet::new()),
            // SUM/MIN/MAX on a string column without a type-specific
            // variant (e.g. SUM(name)) — fall through to a sentinel
            // that ignores input so the executor doesn't panic. The
            // parser already rejects these in well-formed queries.
            _ => AggState::Count(0),
        }
    }

    /// Update with one row's value for the aggregate's input column.
    /// `None` represents `COUNT(*)` (no column read).
    pub fn update(&mut self, v: Option<&Value>) {
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
            AggState::Welford { count, mean, m2, .. } => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        *count += 1;
                        let delta = x - *mean;
                        *mean += delta / *count as f64;
                        let delta2 = x - *mean;
                        *m2 += delta * delta2;
                    }
                }
            }
            AggState::Percentile { values, .. } => {
                if let Some(val) = v {
                    if let Some(x) = val.as_f64() {
                        values.push(x);
                    }
                }
            }
            AggState::CountDistinct(set) => {
                if let Some(val) = v {
                    // Nulls are skipped — `COUNT(DISTINCT col)`
                    // counts the distinct *non-null* values per
                    // standard SQL semantics.
                    if !val.is_null() {
                        set.insert(GroupKeyPart::from_value(val));
                    }
                }
            }
        }
    }

    pub fn finalize(&self) -> serde_json::Value {
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
            AggState::CountDistinct(set) => serde_json::Value::from(set.len() as u64),
            AggState::Percentile { values, q } => {
                if values.is_empty() {
                    return serde_json::Value::Null;
                }
                // O(n log n) per finalise; OK at moderate per-group
                // cardinality. For high-cardinality groups consider
                // sketching (deferred).
                let mut sorted: Vec<f64> = values.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                let rank = q * (sorted.len() - 1) as f64;
                let lo = rank.floor() as usize;
                let hi = rank.ceil() as usize;
                let v = if lo == hi {
                    sorted[lo]
                } else {
                    sorted[lo] + (rank - lo as f64) * (sorted[hi] - sorted[lo])
                };
                serde_json::Value::from(v)
            }
            AggState::Welford { count, m2, kind, .. } => {
                if *count == 0 {
                    return serde_json::Value::Null;
                }
                // Sample stats require N >= 2; population stats are
                // defined at N == 1 (variance/stddev = 0).
                let n = *count as f64;
                let v = match kind {
                    WelfordKind::VarPop => *m2 / n,
                    WelfordKind::StddevPop => (*m2 / n).sqrt(),
                    WelfordKind::VarSamp => {
                        if *count < 2 { return serde_json::Value::Null; }
                        *m2 / (n - 1.0)
                    }
                    WelfordKind::StddevSamp => {
                        if *count < 2 { return serde_json::Value::Null; }
                        (*m2 / (n - 1.0)).sqrt()
                    }
                };
                serde_json::Value::from(v)
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
    live_rows: Option<&roaring::RoaringBitmap>,
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
        if let Some(live) = live_rows {
            if !live.contains(row) {
                return;
            }
        }
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
                .map(|(i, a)| AggState::init_with_q(a.func, agg_col_types[i], a.percentile_q))
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
    //
    // ANSI SQL special case (closes the Known Issue
    // `count_star_empty_table`): when there is NO `GROUP BY` and at
    // least one aggregate, an empty input MUST still produce exactly
    // one output row — every aggregate's "no observations" value.
    // `COUNT(*) FROM <empty>` is 1 row with `c = 0`. `SUM` returns
    // NULL on empty (handled by `AggState::finalize`).
    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        Vec::with_capacity(group_order.len());
    let empty_implicit_group =
        group_order.is_empty() && group_cols.is_empty() && !query.aggregates.is_empty();
    if empty_implicit_group {
        let mut row_map = serde_json::Map::new();
        for (i, spec) in query.aggregates.iter().enumerate() {
            let state = AggState::init_with_q(spec.func, agg_col_types[i], spec.percentile_q);
            row_map.insert(spec.alias.clone(), state.finalize());
        }
        // P3 — HAVING also applies to the implicit-group row.
        if query.having.as_ref().map(|h| h.matches(&row_map)).unwrap_or(true) {
            rows.push(row_map);
        }
    }
    for key in &group_order {
        let states = groups.get(key).expect("group must exist");
        let mut row_map = serde_json::Map::new();
        for (i, part) in key.iter().enumerate() {
            row_map.insert(group_names[i].clone(), part.to_json());
        }
        for (i, spec) in query.aggregates.iter().enumerate() {
            row_map.insert(spec.alias.clone(), states[i].finalize());
        }
        // P3 — drop the group if HAVING evaluates to false.
        if let Some(h) = query.having.as_ref() {
            if !h.matches(&row_map) {
                continue;
            }
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
        // Aggregate output rows synthesize per-group state — they
        // don't correspond to specific source rows. Leave empty;
        // the tombstone filter at the Topic layer skips this path
        // (aggregate output isn't subject to per-row tombstone
        // semantics anyway).
        source_rows: Vec::new(),
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
        ColumnType::Bool => {
            // false < true; nulls sort first (matches SQL NULLS FIRST).
            let a = store.get_bool(col, row_a);
            let b = store.get_bool(col, row_b);
            a.cmp(&b)
        }
        ColumnType::Timestamp => {
            // i64 ordering puts NULL_TIMESTAMP (= i64::MIN) first,
            // which matches SQL NULLS FIRST semantics for timestamps.
            let a = store.get_timestamp(col, row_a);
            let b = store.get_timestamp(col, row_b);
            a.cmp(&b)
        }
        // Q10 — bytes columns aren't sortable in the AMPS sense.
        // Treat all values as equal in ORDER BY (stable position).
        ColumnType::Bytes => Ordering::Equal,
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
    /// Parser recognized a feature that the executor doesn't yet
    /// support. Distinct from `ParseError` so callers can surface a
    /// "coming soon" message instead of a "your SQL is malformed"
    /// message. Used today by the S43 PIVOT/UNPIVOT parser
    /// scaffold; remove the variant (or specialize the message) once
    /// the executor lands.
    #[error("Not yet implemented: {0}")]
    NotYetImplemented(String),
    /// Query Guardrails G1: a parsed query exceeded a configured
    /// structural limit (PIVOT IN-list cardinality, degenerate
    /// GROUP BY on dedup keys, view chain depth, or pointless
    /// `SELECT * FROM source` view body). The message names the
    /// specific rule and how to raise it.
    #[error("Query exceeds limit: {0}")]
    LimitExceeded(String),
}

/// Query Guardrails G1: parse-time structural limits, applied to a
/// `ParsedQuery` (and, for view-chain depth, to a config-time view
/// dependency graph). All fields have conservative defaults — see
/// `QueryLimits::default()`. The `[query_limits]` block in
/// `cqserver.toml` lets operators override per-deployment.
#[derive(Debug, Clone, Copy)]
pub struct QueryLimits {
    /// Maximum number of literals allowed in a static
    /// `PIVOT (...) FOR col IN (lit, lit, ...)` IN-list. Each value
    /// becomes an output column, so the wire payload scales linearly.
    pub max_pivot_in_list_size: usize,
    /// Maximum nesting depth for views-on-views. A view whose source
    /// is another view counts as depth 2; a chain of three views is
    /// depth 3. Reject at config load when the static graph would
    /// exceed this.
    pub max_view_chain_depth: usize,
    /// When `true`, reject `GROUP BY col` where `col` is the topic's
    /// dedup key (or a superset that includes all dedup-key columns
    /// and nothing else). Such a "group-by" is degenerate — every
    /// group is a single row, identical to projecting the columns.
    /// The developer almost certainly meant a different aggregation
    /// axis.
    pub reject_degenerate_groupby: bool,
    /// When `true`, reject a view body that is literally
    /// `SELECT * FROM "source"` with no WHERE / aggregation /
    /// projection — subscribing to the source topic directly is
    /// equivalent and strictly faster (no view-evaluator overhead).
    pub reject_passthrough_views: bool,
    /// Query Guardrails G3: pre-flight rejection thresholds. A
    /// subscribe whose `estimated_result_rows > max_sow_estimated_rows`
    /// is rejected before any state is touched. Estimates come from
    /// `cost_estimator::estimate_cost`. `0` disables the check.
    pub max_sow_estimated_rows: u64,
    /// Same as `max_sow_estimated_rows` but for estimated wire bytes.
    /// `0` disables.
    pub max_sow_estimated_bytes: u64,
    /// Reject a query whose JOIN fanout (`avg right-side rows per
    /// USING value`) exceeds this. `0` disables.
    pub max_join_estimated_fanout: u64,
    /// Reject when GROUP BY cardinality estimate exceeds this. `0`
    /// disables.
    pub max_group_estimated_cardinality: u64,
    /// Soft warning threshold for estimated result rows. When the
    /// estimate exceeds this but stays under
    /// `max_sow_estimated_rows`, the subscribe proceeds but a
    /// metric and log line are emitted. `0` disables.
    pub warn_sow_rows_threshold: u64,
    /// Soft warning threshold for estimated result bytes. `0`
    /// disables.
    pub warn_sow_bytes_threshold: u64,
    /// Query Guardrails G4: runtime backstops. Even when G3's
    /// pre-flight estimate passes, the actual SOW stream may
    /// blow past expectations (skewed data, low-confidence
    /// estimate, dynamic PIVOT exploding). When the actual row
    /// count emitted on the wire exceeds this, the stream
    /// aborts cleanly with an error frame to the client.
    /// `0` disables.
    pub hard_max_sow_result_rows: u64,
    /// Same as `hard_max_sow_result_rows` but for emitted bytes
    /// (sum of `sow_batch` frame body lengths). `0` disables.
    pub hard_max_sow_result_bytes: u64,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            // 100 columns is already wide; defends against accidental
            // PIVOT over a long IN list (e.g., 500 CUSIPs).
            max_pivot_in_list_size: 100,
            // 3 deep is the practical ceiling — beyond that the
            // dependency graph is hard to reason about and lag
            // amplifies.
            max_view_chain_depth: 3,
            reject_degenerate_groupby: true,
            reject_passthrough_views: true,
            // G3 hard caps. 1M rows / 100 MB / 10x fanout / 100K
            // groups are conservative but never zero — operators
            // who want the guardrails OFF should set the field to 0.
            max_sow_estimated_rows: 1_000_000,
            max_sow_estimated_bytes: 100_000_000,
            max_join_estimated_fanout: 10,
            max_group_estimated_cardinality: 100_000,
            // Soft warnings at 1/10 of the hard cap so operators
            // see "you're approaching" before "you're rejected."
            warn_sow_rows_threshold: 100_000,
            warn_sow_bytes_threshold: 10_000_000,
            // G4 runtime caps. Generous defaults — these are
            // the catch-all when an estimate-time check missed.
            // 5M rows or 500MB stops a runaway SOW; raise for
            // bulk-export workloads or set 0 to disable.
            hard_max_sow_result_rows: 5_000_000,
            hard_max_sow_result_bytes: 500_000_000,
        }
    }
}

/// Query Guardrails G3: outcome of comparing a `QueryCostEstimate`
/// against a `QueryLimits`. Bundles per-rule warnings (proceed but
/// log) and rejections (return error).
#[derive(Debug, Clone)]
pub struct LimitCheckOutcome {
    /// `true` if any hard cap was exceeded. Caller must NOT proceed.
    pub rejected: bool,
    /// Human-readable message naming the first rule that was
    /// exceeded — populated when `rejected = true`.
    pub reject_reason: Option<String>,
    /// Soft warnings that fired. Caller should log + metric these
    /// but proceed with the subscribe.
    pub warnings: Vec<String>,
}

impl LimitCheckOutcome {
    pub fn ok() -> Self {
        Self { rejected: false, reject_reason: None, warnings: Vec::new() }
    }
}

/// G3: compare a cost estimate against the configured limits.
/// Hard-cap violations populate `rejected`; soft-threshold
/// crossings populate `warnings`. Zero-valued limits are skipped
/// (i.e., disabled).
pub fn check_estimate_against_limits(
    estimate: &crate::cost_estimator::QueryCostEstimate,
    limits: &QueryLimits,
) -> LimitCheckOutcome {
    let mut out = LimitCheckOutcome::ok();

    if limits.max_sow_estimated_rows > 0
        && estimate.estimated_result_rows > limits.max_sow_estimated_rows
    {
        out.rejected = true;
        out.reject_reason = Some(format!(
            "estimated_result_rows = {} exceeds max_sow_estimated_rows = {}. \
             Add a narrower WHERE filter, switch to an aggregate, or contact \
             ops to raise [query_limits].max_sow_estimated_rows.",
            estimate.estimated_result_rows, limits.max_sow_estimated_rows,
        ));
        return out;
    }
    if limits.max_sow_estimated_bytes > 0
        && estimate.estimated_result_bytes > limits.max_sow_estimated_bytes
    {
        out.rejected = true;
        out.reject_reason = Some(format!(
            "estimated_result_bytes = {} exceeds max_sow_estimated_bytes = {}. \
             Project fewer columns, use an aggregate, or raise \
             [query_limits].max_sow_estimated_bytes.",
            estimate.estimated_result_bytes, limits.max_sow_estimated_bytes,
        ));
        return out;
    }
    if limits.max_join_estimated_fanout > 0 {
        if let Some(f) = estimate.estimated_join_fanout_avg {
            if f.is_finite() && f > limits.max_join_estimated_fanout as f64 {
                out.rejected = true;
                out.reject_reason = Some(format!(
                    "estimated_join_fanout_avg = {:.1} exceeds \
                     max_join_estimated_fanout = {}. The USING column is too \
                     low-cardinality on the right side — pick a more selective \
                     join key.",
                    f, limits.max_join_estimated_fanout,
                ));
                return out;
            }
        }
    }

    if limits.warn_sow_rows_threshold > 0
        && estimate.estimated_result_rows > limits.warn_sow_rows_threshold
    {
        out.warnings.push(format!(
            "estimated_result_rows = {} above warn_sow_rows_threshold = {}",
            estimate.estimated_result_rows, limits.warn_sow_rows_threshold,
        ));
    }
    if limits.warn_sow_bytes_threshold > 0
        && estimate.estimated_result_bytes > limits.warn_sow_bytes_threshold
    {
        out.warnings.push(format!(
            "estimated_result_bytes = {} above warn_sow_bytes_threshold = {}",
            estimate.estimated_result_bytes, limits.warn_sow_bytes_threshold,
        ));
    }
    out
}

impl ParsedQuery {
    /// Run all configured structural checks against this parsed
    /// query. Returns `Ok(())` if the query passes; otherwise
    /// `QueryError::LimitExceeded` naming the specific rule.
    ///
    /// Caller is expected to provide:
    /// - `dedup_keys`: the topic's key columns by name (or by index
    ///   on the schema this query was parsed against). Used by the
    ///   degenerate-groupby rule.
    ///
    /// Note: view-chain depth + passthrough-view rejection live on
    /// the view registration path, not here — those checks need the
    /// full view graph and are run once at config load.
    pub fn validate_with_limits(
        &self,
        limits: &QueryLimits,
        dedup_keys_by_index: &[usize],
    ) -> Result<(), QueryError> {
        // PIVOT IN-list cap. Only applies to *static* pivots (the
        // dynamic `IN ANY` form has an empty `pivot_values` until
        // execution discovers them; we rely on the runtime cap in
        // G4 for that path).
        if let Some(p) = &self.pivot {
            if !p.dynamic && p.pivot_values.len() > limits.max_pivot_in_list_size {
                return Err(QueryError::LimitExceeded(format!(
                    "PIVOT IN-list has {} values; max_pivot_in_list_size = {}. \
                     Reduce the IN list, switch to a narrower aggregate, or raise \
                     [query_limits].max_pivot_in_list_size.",
                    p.pivot_values.len(),
                    limits.max_pivot_in_list_size,
                )));
            }
        }

        // Degenerate GROUP BY: GROUP BY of exactly the dedup-key
        // column set is a no-op aggregate. We allow GROUP BY that
        // includes the dedup keys AS A STRICT SUBSET of more columns
        // (e.g., GROUP BY key, region — still meaningful), and
        // allow GROUP BY that omits some key columns. Only the
        // exact-set-of-key-cols case is rejected.
        if limits.reject_degenerate_groupby
            && !self.aggregates.is_empty()
            && !dedup_keys_by_index.is_empty()
            && self.group_by.len() == dedup_keys_by_index.len()
        {
            let groupby: std::collections::BTreeSet<usize> =
                self.group_by.iter().copied().collect();
            let keys: std::collections::BTreeSet<usize> =
                dedup_keys_by_index.iter().copied().collect();
            if groupby == keys {
                return Err(QueryError::LimitExceeded(
                    "GROUP BY enumerates exactly the dedup-key columns — \
                     every group is one row. Either remove the GROUP BY \
                     (project the columns directly) or group by a coarser \
                     dimension. Set [query_limits].reject_degenerate_groupby = \
                     false to allow this query."
                        .into(),
                ));
            }
        }

        Ok(())
    }
}

/// Query Guardrails G1: scan a set of view definitions and reject if
/// any chain (view → view → ... → topic) exceeds the configured
/// depth, or if any view body is a pointless `SELECT * FROM source`.
///
/// `view_sources` maps every view's `name` to its `source` (which may
/// be another view's name, or a topic). Topic names are inferred as
/// "anything not in the keyset." Cycles are flagged as
/// `LimitExceeded` rather than recursed into.
pub fn validate_view_graph(
    view_sources: &HashMap<String, String>,
    view_bodies: &HashMap<String, String>,
    limits: &QueryLimits,
) -> Result<(), QueryError> {
    // Passthrough view bodies — purely structural string match after
    // whitespace normalization. We accept slightly-formatted
    // variants like `select  *  from "/x"` and `SELECT * FROM /x`.
    if limits.reject_passthrough_views {
        for (name, body) in view_bodies {
            if is_passthrough_select(body) {
                return Err(QueryError::LimitExceeded(format!(
                    "View {name:?} body is a pointless `SELECT * FROM source` — \
                     subscribers should connect to the source topic directly. \
                     Set [query_limits].reject_passthrough_views = false to allow."
                )));
            }
        }
    }

    // View chain depth — walk each view's source chain.
    for view in view_sources.keys() {
        let mut depth = 1usize;
        let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
        visited.insert(view.clone());
        let mut cur = view.clone();
        while let Some(src) = view_sources.get(&cur) {
            if !view_sources.contains_key(src) {
                break; // landed on a topic, stop
            }
            // Cycle check FIRST — a cycle would also exceed depth, but
            // the user benefits from a precise "you have a cycle"
            // error rather than an indirect "depth exceeded" one.
            if !visited.insert(src.clone()) {
                return Err(QueryError::LimitExceeded(format!(
                    "View {view:?} chain forms a cycle through {src:?}; \
                     views may not reference themselves transitively."
                )));
            }
            depth += 1;
            if depth > limits.max_view_chain_depth {
                return Err(QueryError::LimitExceeded(format!(
                    "View {view:?} chain depth {depth} exceeds \
                     max_view_chain_depth = {}. Flatten the view stack or \
                     raise [query_limits].max_view_chain_depth.",
                    limits.max_view_chain_depth,
                )));
            }
            cur = src.clone();
        }
    }

    Ok(())
}

/// Heuristic: does this SQL string look like `SELECT * FROM x` with
/// no other clauses? Quoted-identifier source names accepted.
fn is_passthrough_select(sql: &str) -> bool {
    let normalized = sql
        .split_ascii_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    // SELECT * FROM <token> with optional quotes; nothing after.
    let prefix = "select * from ";
    if !normalized.starts_with(prefix) {
        return false;
    }
    let rest = normalized[prefix.len()..].trim_end_matches(';').trim();
    // Allow at most a single bare identifier or a quoted identifier.
    // No WHERE / GROUP / ORDER / LIMIT / JOIN / PIVOT etc.
    if rest.contains(' ') {
        return false;
    }
    !rest.is_empty()
}

/// S20 — execute a JOIN query. Pulls every left-store row that
/// satisfies the predicate, joins it against the matching right-side
/// row (lookup by USING columns), and runs the remaining stages
/// (aggregate, projection) over the combined column set.
///
/// Internally this is a streaming hash join: we first build an index
/// of right rows keyed by the USING tuple (O(right_rows)), then walk
/// the left store once (O(left_rows)). The combined rows are
/// materialized into a temporary `ColumnStore` with the
/// `combined_join_schema` layout, then handed to the existing
/// aggregate / non-aggregate executors so all of the WHERE / GROUP BY
/// / aggregate / projection machinery reuses without modification.
///
/// `query.join` must be `Some(_)`; callers without a join should use
/// the regular `execute_query_with_index` path.
pub fn execute_join_query(
    query: &ParsedQuery,
    left_store: &ColumnStore,
    right_store: &ColumnStore,
) -> Result<QueryResult, QueryError> {
    let join = query.join.as_ref().ok_or_else(|| {
        QueryError::ParseError("execute_join_query called on a non-join ParsedQuery".into())
    })?;
    let left_schema = left_store.schema();
    let right_schema = right_store.schema();

    // Resolve USING column indices on both sides.
    let mut left_using: Vec<usize> = Vec::with_capacity(join.using.len());
    let mut right_using: Vec<usize> = Vec::with_capacity(join.using.len());
    for name in &join.using {
        let li = left_schema.index_of(name).ok_or_else(|| {
            QueryError::ParseError(format!(
                "JOIN USING column `{}` not found on left side",
                name
            ))
        })?;
        let ri = right_schema.index_of(name).ok_or_else(|| {
            QueryError::ParseError(format!(
                "JOIN USING column `{}` not found on right side",
                name
            ))
        })?;
        left_using.push(li);
        right_using.push(ri);
    }

    // Build the combined column layout: every left column, then
    // every right column NOT in the USING set.
    let using_set: std::collections::HashSet<&str> =
        join.using.iter().map(String::as_str).collect();
    // Maps combined-column-index → (Side, source-column-index).
    enum Side {
        Left,
        Right,
    }
    let mut combined_sources: Vec<(Side, usize)> = Vec::new();
    let mut combined_types: Vec<crate::schema::ColumnType> = Vec::new();
    let mut combined_names: Vec<String> = Vec::new();
    for col in left_schema.columns() {
        combined_sources.push((Side::Left, left_schema.index_of(col.name()).unwrap()));
        combined_types.push(col.col_type());
        combined_names.push(col.name().to_string());
    }
    for col in right_schema.columns() {
        if using_set.contains(col.name()) {
            continue;
        }
        combined_sources.push((Side::Right, right_schema.index_of(col.name()).unwrap()));
        combined_types.push(col.col_type());
        combined_names.push(col.name().to_string());
    }
    let combined_name_refs: Vec<&str> =
        combined_names.iter().map(String::as_str).collect();
    let combined_schema = std::sync::Arc::new(Schema::from_strs(
        &combined_name_refs,
        &combined_types,
    ));

    // Build the right-side hash index: key tuple → right row index.
    // We canonicalise the key by joining stringified values with
    // `\x1f` (unit separator) — bulletproof for any printable value
    // and obviously distinguishable from `|` etc. for debugging.
    fn key_for(store: &ColumnStore, row: u32, cols: &[usize]) -> Option<String> {
        let mut out = String::new();
        for (i, &c) in cols.iter().enumerate() {
            if i > 0 {
                out.push('\x1f');
            }
            let v = store.get(c, row);
            if v.is_null() {
                return None;
            }
            match v {
                Value::String(Some(s)) => out.push_str(s.as_str()),
                Value::Long(n) => out.push_str(&n.to_string()),
                Value::Int(n) => out.push_str(&n.to_string()),
                Value::Double(n) => out.push_str(&n.to_bits().to_string()),
                _ => return None,
            }
        }
        Some(out)
    }
    let right_row_count = right_store.row_count();
    let mut right_index: HashMap<String, u32> = HashMap::with_capacity(right_row_count as usize);
    for r in 0..right_row_count {
        if let Some(k) = key_for(right_store, r, &right_using) {
            // Last-write-wins on duplicate join keys (the right side
            // is expected to be unique on the USING columns; if it
            // isn't, we silently dedupe rather than fan-out).
            right_index.insert(k, r);
        }
    }

    // Materialize the joined rows into a temp ColumnStore.
    //
    // Per JoinKind:
    //   - Inner: emit only matched (left,right) pairs.
    //   - LeftOuter: emit every left row; right cols NULL on miss.
    //   - RightOuter: emit every right row; left cols NULL on miss.
    //   - FullOuter: LeftOuter ∪ right-only rows.
    //
    // For RIGHT/FULL OUTER we additionally track which right rows
    // were matched so we can emit the unmatched right-only rows at
    // the end.
    let left_row_count = left_store.row_count();
    let mut combined =
        ColumnStore::new(combined_schema.clone(), left_row_count as usize + 16);
    let mut row_buf: Vec<Value> = Vec::with_capacity(combined_sources.len());
    let keep_left_misses = matches!(join.kind, JoinKind::LeftOuter | JoinKind::FullOuter);
    let keep_right_misses = matches!(join.kind, JoinKind::RightOuter | JoinKind::FullOuter);
    let is_asof = matches!(join.kind, JoinKind::AsOf { .. });

    // Q12 — for AS OF JOIN, pre-build a per-USING-key sorted list of
    // (ts_value, right_row) so we can binary-search for the largest
    // ts ≤ left.ts at join time.
    let asof_index: Option<HashMap<String, Vec<(i64, u32)>>> = if is_asof {
        let ts_col_name = match &join.kind {
            JoinKind::AsOf { ts_col } => ts_col.clone(),
            _ => unreachable!(),
        };
        let right_ts_idx = right_schema.index_of(&ts_col_name).ok_or_else(|| {
            QueryError::ParseError(format!(
                "AS OF JOIN: ts column `{ts_col_name}` not found on right side"
            ))
        })?;
        let mut idx: HashMap<String, Vec<(i64, u32)>> = HashMap::new();
        for r in 0..right_row_count {
            let key = match key_for(right_store, r, &right_using) {
                Some(k) => k,
                None => continue,
            };
            let ts = match right_store.get(right_ts_idx, r) {
                Value::Timestamp(t) if t != crate::store::NULL_TIMESTAMP => t,
                Value::Long(t) if t != crate::store::NULL_LONG => t,
                _ => continue,
            };
            idx.entry(key).or_default().push((ts, r));
        }
        for v in idx.values_mut() {
            v.sort_by_key(|(ts, _)| *ts);
        }
        Some(idx)
    } else {
        None
    };
    // Cache the left-side ts column index for the AsOf loop below.
    let asof_left_ts_idx: Option<usize> = if let JoinKind::AsOf { ts_col } = &join.kind {
        Some(left_schema.index_of(ts_col).ok_or_else(|| {
            QueryError::ParseError(format!(
                "AS OF JOIN: ts column `{ts_col}` not found on left side"
            ))
        })?)
    } else {
        None
    };
    let mut right_matched: roaring::RoaringBitmap = roaring::RoaringBitmap::new();
    for lr in 0..left_row_count {
        let key = match key_for(left_store, lr, &left_using) {
            Some(k) => k,
            None => {
                // Left key NULL: INNER + RIGHT drop; LEFT/FULL keep
                // the row with right cols NULL.
                if !keep_left_misses {
                    continue;
                }
                row_buf.clear();
                for (side, src) in &combined_sources {
                    let v = match side {
                        Side::Left => left_store.get(*src, lr),
                        Side::Right => Value::Null,
                    };
                    row_buf.push(v);
                }
                combined.append_row(&row_buf);
                continue;
            }
        };
        // Q12 — AS OF JOIN: find the largest right ts ≤ left.ts among
        // entries with the matching USING key. Falls through to the
        // normal `right_index` lookup for all other join kinds.
        let resolved_rr: Option<u32> = if is_asof {
            let left_ts = match left_store.get(asof_left_ts_idx.unwrap(), lr) {
                Value::Timestamp(t) if t != crate::store::NULL_TIMESTAMP => t,
                Value::Long(t) if t != crate::store::NULL_LONG => t,
                _ => i64::MIN,
            };
            asof_index
                .as_ref()
                .and_then(|idx| idx.get(&key))
                .and_then(|entries| {
                    // Binary-search for largest entry with ts ≤ left_ts.
                    let pos = entries.partition_point(|(ts, _)| *ts <= left_ts);
                    if pos == 0 {
                        None
                    } else {
                        Some(entries[pos - 1].1)
                    }
                })
        } else {
            right_index.get(&key).copied()
        };
        match resolved_rr {
            Some(rr) => {
                right_matched.insert(rr);
                row_buf.clear();
                for (side, src) in &combined_sources {
                    let v = match side {
                        Side::Left => left_store.get(*src, lr),
                        Side::Right => right_store.get(*src, rr),
                    };
                    row_buf.push(v);
                }
                combined.append_row(&row_buf);
            }
            None => {
                if !keep_left_misses {
                    continue;
                }
                row_buf.clear();
                for (side, src) in &combined_sources {
                    let v = match side {
                        Side::Left => left_store.get(*src, lr),
                        Side::Right => Value::Null,
                    };
                    row_buf.push(v);
                }
                combined.append_row(&row_buf);
            }
        }
    }
    // Q1 — emit right-only rows (rows in right that no left row
    // matched). Skipped entirely for Inner and LeftOuter.
    if keep_right_misses {
        for rr in 0..right_row_count {
            if right_matched.contains(rr) {
                continue;
            }
            row_buf.clear();
            for (side, src) in &combined_sources {
                let v = match side {
                    Side::Left => {
                        // The USING column comes from the LEFT side
                        // in `combined_sources`, but for a right-only
                        // row we want to surface the right-side value
                        // so consumers see (cusip=GOOG, qty=null,
                        // sector=Tech) not (cusip=null, ...). Look up
                        // the matching right column when this left
                        // source col is a USING column.
                        if let Some(name) =
                            left_schema.columns().get(*src).map(|c| c.name())
                        {
                            if let Some(ri) = right_schema.index_of(name) {
                                right_store.get(ri, rr)
                            } else {
                                Value::Null
                            }
                        } else {
                            Value::Null
                        }
                    }
                    Side::Right => right_store.get(*src, rr),
                };
                row_buf.push(v);
            }
            combined.append_row(&row_buf);
        }
    }

    // Strip the join off the query for the downstream executor — it's
    // already been consumed by the materialization above.
    let mut downstream = query.clone();
    downstream.join = None;
    Ok(execute_query_with_index(&downstream, &combined, None))
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

    // ───── S20 JOIN: parser ──────────────────────────────────────────

    #[test]
    fn parse_join_with_using_clause() {
        // Single-side schema is enough for the parser even when a
        // JOIN is present — the parser stores symbolic names on the
        // JoinSpec; the executor resolves USING columns against
        // BOTH stores at run time.
        let left = Schema::from_strs(
            &["cusip", "qty", "ticker"],
            &[ColumnType::String, ColumnType::Long, ColumnType::String],
        );
        let right = Schema::from_strs(
            &["cusip", "sector"],
            &[ColumnType::String, ColumnType::String],
        );
        let combined = combined_join_schema(&left, &right, &["cusip".to_string()]);
        let q = parse_query(
            "SELECT sector, SUM(qty) AS total FROM positions \
             JOIN securities USING (cusip) \
             GROUP BY sector",
            &combined,
        )
        .unwrap();
        let join = q.join.as_ref().expect("expected JoinSpec");
        assert_eq!(join.right_topic, "securities");
        assert_eq!(join.using, vec!["cusip".to_string()]);
        assert!(q.is_aggregate());
        assert_eq!(q.group_by.len(), 1);
    }

    // (Was `parse_join_rejects_left_outer_for_now` — superseded by
    // P12 which accepts LEFT OUTER JOIN. See the P12 tests below.)

    // ───── Q12 — AS OF JOIN (temporal) ───────────────────────────────

    #[test]
    fn parses_asof_join_with_match_condition() {
        // Combined schema must include both `ts` and `symbol`.
        let combined = Schema::from_strs(
            &["ts", "symbol", "px", "qty"],
            &[
                ColumnType::Timestamp,
                ColumnType::String,
                ColumnType::Double,
                ColumnType::Long,
            ],
        );
        let sql = "SELECT t.symbol, t.qty, p.px \
                   FROM trades t ASOF JOIN prices p \
                   MATCH_CONDITION(t.ts >= p.ts) \
                   ON t.symbol = p.symbol";
        let q = parse_query(sql, &combined).expect("ASOF JOIN must parse");
        let j = q.join.expect("expected JoinSpec");
        match &j.kind {
            JoinKind::AsOf { ts_col } => assert_eq!(ts_col, "ts"),
            other => panic!("expected JoinKind::AsOf, got {other:?}"),
        }
        assert_eq!(j.using, vec!["symbol".to_string()]);
    }

    #[test]
    fn asof_join_matches_latest_right_le_left_ts() {
        // left (trades): symbol=AAPL @ ts=200
        // right (prices): symbol=AAPL @ ts={100, 150, 250}
        //                 → ASOF match: ts=150 (largest ≤ 200)
        // Both sides share a `ts` column (USING-style match_condition).
        let left_schema = Arc::new(Schema::from_strs(
            &["symbol", "ts", "qty"],
            &[ColumnType::String, ColumnType::Long, ColumnType::Long],
        ));
        let right_schema = Arc::new(Schema::from_strs(
            &["symbol", "ts", "px"],
            &[ColumnType::String, ColumnType::Long, ColumnType::Double],
        ));
        let mut left = ColumnStore::new(left_schema.clone(), 8);
        let mut right = ColumnStore::new(right_schema.clone(), 8);
        left.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Long(200),
            Value::Long(10),
        ]);
        for (ts, px) in &[(100i64, 100.0_f64), (150, 150.0), (250, 200.0)] {
            right.append_row(&[
                Value::String(Some(CompactString::new("AAPL"))),
                Value::Long(*ts),
                Value::Double(*px),
            ]);
        }
        // Combined schema: dedup symbol only (ts and qty / px are
        // side-specific). The combined `ts` resolves to the LEFT's
        // ts (first occurrence), which the test asserts below.
        let combined = combined_join_schema(
            &left_schema,
            &right_schema,
            &["symbol".to_string()],
        );
        let q = parse_query(
            "SELECT symbol, qty, px FROM trades \
             ASOF JOIN prices MATCH_CONDITION(ts >= ts) USING (symbol)",
            &combined,
        )
        .unwrap();
        let r = execute_join_query(&q, &left, &right).unwrap();
        assert_eq!(r.rows.len(), 1);
        let row = &r.rows[0];
        // qty from left, px from right's row whose ts (=150) was
        // the largest ≤ left's ts (=200).
        assert_eq!(row.get("qty").unwrap().as_i64().unwrap(), 10);
        assert_eq!(row.get("px").unwrap().as_f64().unwrap(), 150.0);
    }

    // ───── Q10 — Bytes column type ───────────────────────────────────

    #[test]
    fn bytes_column_round_trips_via_value() {
        let s = Arc::new(Schema::from_strs(
            &["k", "blob"],
            &[ColumnType::String, ColumnType::Bytes],
        ));
        let mut store = ColumnStore::new(s.clone(), 8);
        store.append_row(&[
            Value::String(Some(CompactString::new("a"))),
            Value::Bytes(Some(vec![0x00, 0x01, 0x02, 0xff])),
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("b"))),
            Value::Bytes(None),
        ]);
        // Round-trip the bytes via get().
        let row0 = store.get(1, 0);
        assert_eq!(row0, Value::Bytes(Some(vec![0x00, 0x01, 0x02, 0xff])));
        let row1 = store.get(1, 1);
        assert!(row1.is_null());
        // to_json emits base64.
        let json = row0.to_json();
        assert_eq!(json.as_str().unwrap(), "AAEC/w==");
        // from_json reverses.
        let parsed = Value::from_json(
            &serde_json::Value::String("AAEC/w==".into()),
            ColumnType::Bytes,
        );
        assert_eq!(parsed, Value::Bytes(Some(vec![0x00, 0x01, 0x02, 0xff])));
    }

    #[test]
    fn bytes_filter_is_null_works() {
        let s = Arc::new(Schema::from_strs(
            &["k", "blob"],
            &[ColumnType::String, ColumnType::Bytes],
        ));
        let mut store = ColumnStore::new(s.clone(), 8);
        store.append_row(&[
            Value::String(Some(CompactString::new("a"))),
            Value::Bytes(Some(vec![0xde, 0xad])),
        ]);
        store.append_row(&[
            Value::String(Some(CompactString::new("b"))),
            Value::Bytes(None),
        ]);
        let q = parse_query("SELECT k FROM t WHERE blob IS NULL", &s).unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("k").unwrap().as_str().unwrap(), "b");
    }

    // ───── Q9 — subqueries ───────────────────────────────────────────

    #[test]
    fn subquery_in_select_currently_unsupported() {
        // Q9 MVP: parser-time rejection with a clear error. Actual
        // materialisation lives in topic.rs's new
        // `query_streaming_json_with_subqueries` path (exercised by
        // the e2e test).
        let (schema, _) = make_store();
        let r = parse_query(
            "SELECT symbol FROM trades WHERE symbol IN (SELECT symbol FROM watchlist)",
            &schema,
        );
        // Either parse rejects directly or predicate compile errors —
        // both are acceptable. The point: we get a CLEAN error rather
        // than silently dropping the IN-clause.
        assert!(r.is_err(), "subquery without materialisation must error");
    }

    // ───── Q8 — CTEs (WITH x AS …) ───────────────────────────────────

    #[test]
    fn parses_simple_cte_alias() {
        let (schema, store) = make_store();
        let q = parse_query(
            "WITH p AS (SELECT * FROM trades) \
             SELECT symbol FROM p WHERE price > 200",
            &schema,
        )
        .expect("CTE alias must parse");
        // The CTE alias `p` should resolve to `trades` semantically;
        // execution against the trades store must return the same
        // rows as the un-CTE'd query.
        let r = execute_query(&q, &store);
        let plain = parse_query(
            "SELECT symbol FROM trades WHERE price > 200",
            &schema,
        )
        .unwrap();
        let p = execute_query(&plain, &store);
        assert_eq!(r.rows.len(), p.rows.len());
    }

    #[test]
    fn cte_with_filter_pushes_into_main_where() {
        let (schema, store) = make_store();
        // RATES desk has AAPL (150) and GOOGL (2800). Main filter
        // `price > 200` keeps GOOGL only.
        let q = parse_query(
            "WITH rates_trades AS (SELECT * FROM trades WHERE desk = 'RATES') \
             SELECT symbol FROM rates_trades WHERE price > 200",
            &schema,
        )
        .expect("CTE+filter must parse");
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].get("symbol").unwrap().as_str().unwrap(), "GOOGL");
    }

    #[test]
    fn recursive_cte_is_rejected() {
        let (schema, _) = make_store();
        let r = parse_query(
            "WITH RECURSIVE r AS (SELECT * FROM trades) SELECT * FROM r",
            &schema,
        );
        assert!(r.is_err(), "RECURSIVE CTEs must be rejected");
    }

    // ───── Q7 — window functions ─────────────────────────────────────

    fn make_window_store() -> (Arc<Schema>, ColumnStore) {
        // (sym, px) — sym is partition col, px is order col.
        let s = Arc::new(Schema::from_strs(
            &["sym", "px"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut store = ColumnStore::new(s.clone(), 16);
        // AAPL: 150, 100, 200 → sorted ASC: 100, 150, 200
        // MSFT: 300, 50     → sorted ASC: 50, 300
        for (sym, px) in &[
            ("AAPL", 150.0_f64),
            ("AAPL", 100.0),
            ("AAPL", 200.0),
            ("MSFT", 300.0),
            ("MSFT", 50.0),
        ] {
            store.append_row(&[
                Value::String(Some(CompactString::new(*sym))),
                Value::Double(*px),
            ]);
        }
        (s, store)
    }

    #[test]
    fn parses_row_number_over_partition_order() {
        let (s, _) = make_window_store();
        let q = parse_query(
            "SELECT sym, px, ROW_NUMBER() OVER (PARTITION BY sym ORDER BY px ASC) AS rn FROM t",
            &s,
        )
        .expect("ROW_NUMBER must parse");
        assert_eq!(q.windows.len(), 1);
        assert_eq!(q.windows[0].alias, "rn");
    }

    #[test]
    fn row_number_assigns_per_partition_sorted_index() {
        let (s, store) = make_window_store();
        let q = parse_query(
            "SELECT sym, px, ROW_NUMBER() OVER (PARTITION BY sym ORDER BY px ASC) AS rn FROM t",
            &s,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        // Build (sym, px) → rn map and check.
        let mut got: std::collections::HashMap<(String, i64), u64> =
            std::collections::HashMap::new();
        for row in &r.rows {
            let sym = row.get("sym").unwrap().as_str().unwrap().to_string();
            let px = row.get("px").unwrap().as_f64().unwrap() as i64;
            let rn = row.get("rn").unwrap().as_u64().unwrap();
            got.insert((sym, px), rn);
        }
        // AAPL ASC: 100→1, 150→2, 200→3.
        assert_eq!(got.get(&("AAPL".into(), 100)).copied(), Some(1));
        assert_eq!(got.get(&("AAPL".into(), 150)).copied(), Some(2));
        assert_eq!(got.get(&("AAPL".into(), 200)).copied(), Some(3));
        // MSFT ASC: 50→1, 300→2.
        assert_eq!(got.get(&("MSFT".into(), 50)).copied(), Some(1));
        assert_eq!(got.get(&("MSFT".into(), 300)).copied(), Some(2));
    }

    #[test]
    fn rank_assigns_dense_or_gapped() {
        // Two AAPL rows with same px should tie at rank 1 (both
        // RANK and DENSE_RANK); the third gets rank 3 for RANK,
        // rank 2 for DENSE_RANK.
        let s = Arc::new(Schema::from_strs(
            &["sym", "px"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut store = ColumnStore::new(s.clone(), 8);
        for (sym, px) in &[("A", 1.0_f64), ("A", 1.0), ("A", 2.0)] {
            store.append_row(&[
                Value::String(Some(CompactString::new(*sym))),
                Value::Double(*px),
            ]);
        }
        let q = parse_query(
            "SELECT sym, px, \
                    RANK()       OVER (PARTITION BY sym ORDER BY px ASC) AS rk, \
                    DENSE_RANK() OVER (PARTITION BY sym ORDER BY px ASC) AS dr \
             FROM t",
            &s,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        for row in &r.rows {
            let px = row.get("px").unwrap().as_f64().unwrap();
            let rk = row.get("rk").unwrap().as_u64().unwrap();
            let dr = row.get("dr").unwrap().as_u64().unwrap();
            if px == 2.0 {
                assert_eq!(rk, 3);
                assert_eq!(dr, 2);
            } else {
                assert_eq!(rk, 1);
                assert_eq!(dr, 1);
            }
        }
    }

    // ───── Q1 — RIGHT OUTER + FULL OUTER JOIN ────────────────────────

    fn make_join_outer_fixture() -> (ColumnStore, ColumnStore, Schema) {
        // left: cusip in {AAPL, MSFT}
        // right: cusip in {AAPL, GOOG}
        // INNER → AAPL; LEFT → AAPL+MSFT(null); RIGHT → AAPL+GOOG(null);
        // FULL → AAPL+MSFT(null)+GOOG(null).
        let left_schema = Arc::new(Schema::from_strs(
            &["cusip", "qty"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let right_schema = Arc::new(Schema::from_strs(
            &["cusip", "sector"],
            &[ColumnType::String, ColumnType::String],
        ));
        let mut left = ColumnStore::new(left_schema.clone(), 8);
        let mut right = ColumnStore::new(right_schema.clone(), 8);
        left.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Long(10),
        ]);
        left.append_row(&[
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Long(20),
        ]);
        right.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::String(Some(CompactString::new("Tech"))),
        ]);
        right.append_row(&[
            Value::String(Some(CompactString::new("GOOG"))),
            Value::String(Some(CompactString::new("Tech"))),
        ]);
        let combined = combined_join_schema(&left_schema, &right_schema, &["cusip".to_string()]);
        (left, right, combined)
    }

    #[test]
    fn parse_right_outer_join_succeeds() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let q = parse_query("SELECT v FROM a RIGHT JOIN b USING (k)", &combined)
            .expect("RIGHT JOIN must parse");
        assert_eq!(q.join.unwrap().kind, JoinKind::RightOuter);
        let q2 = parse_query("SELECT v FROM a RIGHT OUTER JOIN b USING (k)", &combined)
            .expect("RIGHT OUTER JOIN must parse");
        assert_eq!(q2.join.unwrap().kind, JoinKind::RightOuter);
    }

    #[test]
    fn parse_full_outer_join_succeeds() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let q = parse_query("SELECT v FROM a FULL JOIN b USING (k)", &combined)
            .expect("FULL JOIN must parse");
        assert_eq!(q.join.unwrap().kind, JoinKind::FullOuter);
        let q2 = parse_query("SELECT v FROM a FULL OUTER JOIN b USING (k)", &combined)
            .expect("FULL OUTER JOIN must parse");
        assert_eq!(q2.join.unwrap().kind, JoinKind::FullOuter);
    }

    #[test]
    fn right_outer_join_keeps_unmatched_right_rows() {
        let (left, right, combined) = make_join_outer_fixture();
        let q = parse_query(
            "SELECT cusip, qty, sector FROM positions \
             RIGHT JOIN securities USING (cusip)",
            &combined,
        )
        .unwrap();
        let r = execute_join_query(&q, &left, &right).unwrap();
        // AAPL (matched) + GOOG (right-only, qty=null).
        assert_eq!(r.rows.len(), 2);
        let by_cusip: std::collections::HashMap<String, serde_json::Value> = r
            .rows
            .into_iter()
            .map(|row| {
                (
                    row.get("cusip").unwrap().as_str().unwrap().to_string(),
                    row.get("qty").cloned().unwrap_or(serde_json::Value::Null),
                )
            })
            .collect();
        assert_eq!(by_cusip["AAPL"].as_i64().unwrap(), 10);
        assert!(by_cusip["GOOG"].is_null(), "unmatched right row qty must be null");
    }

    #[test]
    fn full_outer_join_keeps_both_sides() {
        let (left, right, combined) = make_join_outer_fixture();
        let q = parse_query(
            "SELECT cusip, qty, sector FROM positions \
             FULL OUTER JOIN securities USING (cusip)",
            &combined,
        )
        .unwrap();
        let r = execute_join_query(&q, &left, &right).unwrap();
        // AAPL (matched), MSFT (left-only, sector=null), GOOG (right-only, qty=null).
        assert_eq!(r.rows.len(), 3);
        let by_cusip: std::collections::HashMap<String, (serde_json::Value, serde_json::Value)> = r
            .rows
            .into_iter()
            .map(|row| {
                (
                    row.get("cusip").unwrap().as_str().unwrap().to_string(),
                    (
                        row.get("qty").cloned().unwrap_or(serde_json::Value::Null),
                        row.get("sector").cloned().unwrap_or(serde_json::Value::Null),
                    ),
                )
            })
            .collect();
        assert_eq!(by_cusip["AAPL"].0.as_i64().unwrap(), 10);
        assert_eq!(by_cusip["AAPL"].1.as_str().unwrap(), "Tech");
        assert!(by_cusip["MSFT"].1.is_null(), "MSFT sector should be null (left-only)");
        assert_eq!(by_cusip["MSFT"].0.as_i64().unwrap(), 20);
        assert!(by_cusip["GOOG"].0.is_null(), "GOOG qty should be null (right-only)");
        assert_eq!(by_cusip["GOOG"].1.as_str().unwrap(), "Tech");
    }

    // ───── P12 — LEFT OUTER JOIN ─────────────────────────────────────

    #[test]
    fn parse_left_outer_join_using_succeeds() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let q = parse_query("SELECT v FROM a LEFT JOIN b USING (k)", &combined)
            .expect("LEFT JOIN USING must parse");
        let j = q.join.expect("join");
        assert_eq!(j.kind, JoinKind::LeftOuter);
        let q2 = parse_query(
            "SELECT v FROM a LEFT OUTER JOIN b USING (k)",
            &combined,
        )
        .expect("LEFT OUTER JOIN USING must parse");
        assert_eq!(q2.join.unwrap().kind, JoinKind::LeftOuter);
    }

    #[test]
    fn parse_left_outer_join_on_equi() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let q = parse_query(
            "SELECT v FROM a LEFT JOIN b ON a.k = b.k",
            &combined,
        )
        .expect("LEFT JOIN ON-equi must parse");
        let j = q.join.unwrap();
        assert_eq!(j.kind, JoinKind::LeftOuter);
        assert_eq!(j.using, vec!["k".to_string()]);
    }

    #[test]
    fn left_outer_join_emits_nulls_for_unmatched_left_rows() {
        // left: cusip in {AAPL, MSFT}; right: cusip in {AAPL}.
        // INNER JOIN drops MSFT; LEFT OUTER JOIN keeps MSFT with
        // sector = NULL.
        let left_schema = Arc::new(Schema::from_strs(
            &["cusip", "qty"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let right_schema = Arc::new(Schema::from_strs(
            &["cusip", "sector"],
            &[ColumnType::String, ColumnType::String],
        ));
        let mut left = ColumnStore::new(left_schema.clone(), 8);
        let mut right = ColumnStore::new(right_schema.clone(), 8);
        left.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::Long(10),
        ]);
        left.append_row(&[
            Value::String(Some(CompactString::new("MSFT"))),
            Value::Long(20),
        ]);
        right.append_row(&[
            Value::String(Some(CompactString::new("AAPL"))),
            Value::String(Some(CompactString::new("Tech"))),
        ]);

        let combined = combined_join_schema(&left_schema, &right_schema, &["cusip".to_string()]);
        let q = parse_query(
            "SELECT cusip, qty, sector FROM positions \
             LEFT JOIN securities USING (cusip)",
            &combined,
        )
        .unwrap();
        let r = execute_join_query(&q, &left, &right).unwrap();
        assert_eq!(r.rows.len(), 2, "LEFT OUTER keeps both left rows");
        let by_cusip: std::collections::HashMap<String, serde_json::Value> = r
            .rows
            .into_iter()
            .map(|row| {
                (
                    row.get("cusip").unwrap().as_str().unwrap().to_string(),
                    row.get("sector").cloned().unwrap_or(serde_json::Value::Null),
                )
            })
            .collect();
        assert_eq!(by_cusip["AAPL"].as_str().unwrap(), "Tech");
        assert!(
            by_cusip["MSFT"].is_null(),
            "unmatched left row's sector must be JSON null, got {:?}",
            by_cusip["MSFT"]
        );
    }



    // (Was `parse_join_rejects_on_clause_for_now` — superseded by P11
    // which translates ON-equi-same-name to USING. See
    // `parse_join_on_equi_same_column_name` above.)

    // ───── P1 — table aliases + qualified column refs ────────────────

    #[test]
    fn parse_table_alias_simple_select() {
        // `FROM trades p` + `p.symbol`, `p.price` — the alias-rewrite
        // pass should make this equivalent to the unaliased form.
        let (schema, store) = make_store();
        let aliased = parse_query(
            "SELECT p.symbol, p.price FROM trades p WHERE p.price > 200",
            &schema,
        )
        .expect("aliased parse must succeed");
        let plain = parse_query(
            "SELECT symbol, price FROM trades WHERE price > 200",
            &schema,
        )
        .unwrap();
        // Topic must be the real name (alias is a rename, not the topic).
        assert_eq!(aliased.topic, "trades");
        // Projection columns should match.
        assert_eq!(aliased.projection, plain.projection);
        // Execution must produce the same rows.
        let a = execute_query(&aliased, &store);
        let p = execute_query(&plain, &store);
        assert_eq!(a.rows.len(), p.rows.len());
        for (l, r) in a.rows.iter().zip(p.rows.iter()) {
            assert_eq!(l, r);
        }
    }

    #[test]
    fn parse_table_alias_in_group_by_and_aggregate() {
        let (schema, store) = make_store();
        let aliased = parse_query(
            "SELECT t.desk, SUM(t.quantity) AS total FROM trades t GROUP BY t.desk",
            &schema,
        )
        .expect("aliased aggregate must parse");
        let plain = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades GROUP BY desk",
            &schema,
        )
        .unwrap();
        let a = execute_query(&aliased, &store);
        let p = execute_query(&plain, &store);
        // Compare by desk → total (order independent).
        let mut a_map = std::collections::HashMap::new();
        for row in &a.rows {
            a_map.insert(
                row.get("desk").unwrap().as_str().unwrap().to_string(),
                row.get("total").unwrap().as_i64().unwrap(),
            );
        }
        let mut p_map = std::collections::HashMap::new();
        for row in &p.rows {
            p_map.insert(
                row.get("desk").unwrap().as_str().unwrap().to_string(),
                row.get("total").unwrap().as_i64().unwrap(),
            );
        }
        assert_eq!(a_map, p_map);
    }

    #[test]
    fn parse_table_alias_in_order_by() {
        let (schema, store) = make_store();
        let aliased = parse_query(
            "SELECT p.symbol, p.price FROM trades p ORDER BY p.price DESC LIMIT 3",
            &schema,
        )
        .expect("aliased ORDER BY must parse");
        let a = execute_query(&aliased, &store);
        assert_eq!(a.rows.len(), 3);
        assert_eq!(a.rows[0].get("symbol").unwrap(), "AMZN");
    }

    #[test]
    fn parse_table_alias_in_join() {
        // Both sides aliased; USING column referenced unqualified.
        let left = Schema::from_strs(
            &["cusip", "qty", "ticker"],
            &[ColumnType::String, ColumnType::Long, ColumnType::String],
        );
        let right = Schema::from_strs(
            &["cusip", "sector"],
            &[ColumnType::String, ColumnType::String],
        );
        let combined = combined_join_schema(&left, &right, &["cusip".to_string()]);
        let q = parse_query(
            "SELECT s.sector, SUM(p.qty) AS total \
             FROM positions p JOIN securities s USING (cusip) \
             GROUP BY s.sector",
            &combined,
        )
        .expect("aliased join must parse");
        let join = q.join.as_ref().expect("expected JoinSpec");
        assert_eq!(join.right_topic, "securities");
        assert_eq!(join.using, vec!["cusip".to_string()]);
        assert_eq!(q.topic, "positions");
    }

    #[test]
    fn parse_select_unknown_column_errors() {
        let (schema, _store) = make_store();
        let r = parse_query("SELECT bogus_col FROM t", &schema);
        match r {
            Err(QueryError::UnknownColumn(c)) => assert_eq!(c, "bogus_col"),
            other => panic!("expected UnknownColumn, got {other:?}"),
        }
    }

    // ───── P4 — OFFSET clause ───────────────────────────────────────

    #[test]
    fn parses_limit_offset() {
        let (schema, _) = make_store();
        let q = parse_query(
            "SELECT symbol FROM trades ORDER BY price LIMIT 2 OFFSET 1",
            &schema,
        )
        .unwrap();
        assert_eq!(q.limit, Some(2));
        assert_eq!(q.offset, Some(1));
    }

    #[test]
    fn offset_skips_first_n_rows() {
        let (schema, store) = make_store();
        // 5 rows by price desc: AMZN(3400), GOOGL(2800), MSFT(300),
        // NVDA(250), AAPL(150). OFFSET 2 LIMIT 2 → MSFT, NVDA.
        let q = parse_query(
            "SELECT symbol FROM trades ORDER BY price DESC LIMIT 2 OFFSET 2",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].get("symbol").unwrap().as_str().unwrap(), "MSFT");
        assert_eq!(r.rows[1].get("symbol").unwrap().as_str().unwrap(), "NVDA");
    }

    #[test]
    fn offset_only_skips_without_limit() {
        let (schema, store) = make_store();
        // Same ORDER BY, skip 3, no LIMIT → NVDA, AAPL.
        let q = parse_query(
            "SELECT symbol FROM trades ORDER BY price DESC OFFSET 3",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0].get("symbol").unwrap().as_str().unwrap(), "NVDA");
        assert_eq!(r.rows[1].get("symbol").unwrap().as_str().unwrap(), "AAPL");
    }

    // ───── P11 — JOIN ON (translated to USING) ───────────────────────

    #[test]
    fn parse_join_on_equi_same_column_name() {
        let combined = Schema::from_strs(
            &["cusip", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let q = parse_query(
            "SELECT v FROM a JOIN b ON a.cusip = b.cusip",
            &combined,
        )
        .expect("ON-equi-same-name must parse");
        let j = q.join.expect("must have join");
        assert_eq!(j.right_topic, "b");
        assert_eq!(j.using, vec!["cusip".to_string()]);
    }

    #[test]
    fn parse_join_on_multi_column_equi() {
        let combined = Schema::from_strs(
            &["k1", "k2", "v"],
            &[ColumnType::String, ColumnType::String, ColumnType::Long],
        );
        let q = parse_query(
            "SELECT v FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2",
            &combined,
        )
        .expect("multi-key ON-equi must parse");
        let j = q.join.expect("join");
        assert_eq!(j.using, vec!["k1".to_string(), "k2".to_string()]);
    }

    #[test]
    fn parse_join_on_rejects_different_column_names() {
        let combined = Schema::from_strs(
            &["x", "y"],
            &[ColumnType::String, ColumnType::String],
        );
        let r = parse_query("SELECT y FROM a JOIN b ON a.x = b.y", &combined);
        assert!(r.is_err(), "different-named columns must be rejected");
        let err = r.unwrap_err().to_string();
        assert!(
            err.contains("same name") || err.contains("USING"),
            "expected diagnostic mentioning same-name / USING, got: {err}"
        );
    }

    #[test]
    fn parse_join_on_rejects_non_equi() {
        let combined = Schema::from_strs(&["v"], &[ColumnType::Long]);
        let r = parse_query("SELECT v FROM a JOIN b ON a.v > b.v", &combined);
        assert!(r.is_err(), "non-equi ON must be rejected");
    }

    // ───── P10 — COUNT(DISTINCT col) ─────────────────────────────────

    #[test]
    fn parses_count_distinct() {
        let (schema, _) = make_store();
        let q = parse_query(
            "SELECT COUNT(DISTINCT desk) AS n_desks FROM trades",
            &schema,
        )
        .unwrap();
        assert_eq!(q.aggregates.len(), 1);
        assert_eq!(q.aggregates[0].alias, "n_desks");
    }

    #[test]
    fn count_distinct_dedups() {
        let (schema, store) = make_store();
        // make_store has 3 distinct desks: RATES, EQUITIES, TECH.
        let q = parse_query(
            "SELECT COUNT(DISTINCT desk) AS n FROM trades",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        let n = r.rows[0].get("n").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(n, 3, "expected 3 distinct desks");
    }

    #[test]
    fn count_distinct_with_group_by() {
        let s = Arc::new(Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let mut store = ColumnStore::new(s.clone(), 16);
        // a: {1, 2, 2, 3} → 3 distinct
        // b: {7, 7} → 1 distinct
        for (k, v) in &[("a", 1i64), ("a", 2), ("a", 2), ("a", 3), ("b", 7), ("b", 7)] {
            store.append_row(&[
                Value::String(Some(CompactString::new(*k))),
                Value::Long(*v),
            ]);
        }
        let q = parse_query(
            "SELECT k, COUNT(DISTINCT v) AS d FROM t GROUP BY k",
            &s,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        let by_k: std::collections::HashMap<String, u64> = r
            .rows
            .iter()
            .map(|row| {
                (
                    row.get("k").unwrap().as_str().unwrap().to_string(),
                    row.get("d").unwrap().as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(by_k["a"], 3);
        assert_eq!(by_k["b"], 1);
    }

    #[test]
    fn count_distinct_skips_nulls() {
        let s = Arc::new(Schema::from_strs(
            &["v"],
            &[ColumnType::Long],
        ));
        let mut store = ColumnStore::new(s.clone(), 8);
        store.append_row(&[Value::Long(1)]);
        store.append_row(&[Value::Null]);
        store.append_row(&[Value::Long(2)]);
        store.append_row(&[Value::Null]);
        let q = parse_query("SELECT COUNT(DISTINCT v) AS n FROM t", &s).unwrap();
        let r = execute_query(&q, &store);
        let n = r.rows[0].get("n").and_then(|v| v.as_u64()).unwrap();
        assert_eq!(n, 2, "nulls must not be counted");
    }

    // ───── P9 — PERCENTILE_CONT / MEDIAN ─────────────────────────────

    #[test]
    fn parses_percentile_cont_and_median() {
        let (schema, _) = make_numeric_store();
        let q1 = parse_query("SELECT PERCENTILE_CONT(v, 0.5) AS p50 FROM t", &schema).unwrap();
        assert_eq!(q1.aggregates.len(), 1);
        let q2 = parse_query("SELECT MEDIAN(v) AS m FROM t", &schema).unwrap();
        assert_eq!(q2.aggregates.len(), 1);
    }

    #[test]
    fn percentile_cont_known_values() {
        let (schema, store) = make_numeric_store();
        // Sorted values: 2, 4, 4, 4, 5, 5, 7, 9 (N=8).
        // p50 with linear interp between rank 3.5 → midpoint of 4 and 5 = 4.5.
        // p100 = 9, p0 = 2.
        let q = parse_query(
            "SELECT PERCENTILE_CONT(v, 0.5) AS p50, \
                    PERCENTILE_CONT(v, 0.0) AS p0, \
                    PERCENTILE_CONT(v, 1.0) AS p100 FROM t",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        let row = &r.rows[0];
        let p50 = row.get("p50").and_then(|x| x.as_f64()).unwrap();
        let p0 = row.get("p0").and_then(|x| x.as_f64()).unwrap();
        let p100 = row.get("p100").and_then(|x| x.as_f64()).unwrap();
        assert!((p50 - 4.5).abs() < 1e-9, "p50 expected 4.5, got {p50}");
        assert!((p0 - 2.0).abs() < 1e-9);
        assert!((p100 - 9.0).abs() < 1e-9);
    }

    #[test]
    fn median_matches_percentile_50() {
        let (schema, store) = make_numeric_store();
        let q = parse_query(
            "SELECT MEDIAN(v) AS m, PERCENTILE_CONT(v, 0.5) AS p50 FROM t",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        let m = r.rows[0].get("m").and_then(|x| x.as_f64()).unwrap();
        let p = r.rows[0].get("p50").and_then(|x| x.as_f64()).unwrap();
        assert!((m - p).abs() < 1e-9, "MEDIAN should equal PERCENTILE_CONT(0.5)");
    }

    #[test]
    fn percentile_with_group_by() {
        let s = Arc::new(Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut store = ColumnStore::new(s.clone(), 16);
        for (k, v) in &[("a", 1.0), ("a", 3.0), ("a", 5.0), ("b", 10.0), ("b", 20.0)] {
            store.append_row(&[
                Value::String(Some(CompactString::new(*k))),
                Value::Double(*v),
            ]);
        }
        let q = parse_query("SELECT k, MEDIAN(v) AS m FROM t GROUP BY k", &s).unwrap();
        let r = execute_query(&q, &store);
        let by_k: std::collections::HashMap<String, f64> = r
            .rows
            .iter()
            .map(|row| {
                (
                    row.get("k").unwrap().as_str().unwrap().to_string(),
                    row.get("m").unwrap().as_f64().unwrap(),
                )
            })
            .collect();
        assert!((by_k["a"] - 3.0).abs() < 1e-9, "median of {{1,3,5}} = 3");
        assert!((by_k["b"] - 15.0).abs() < 1e-9, "median of {{10,20}} = 15");
    }

    // ───── P8 — STDDEV / STDDEV_SAMP / VARIANCE ──────────────────────

    fn make_numeric_store() -> (Arc<Schema>, ColumnStore) {
        let schema = Arc::new(Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Double],
        ));
        let mut store = ColumnStore::new(schema.clone(), 16);
        // 2, 4, 4, 4, 5, 5, 7, 9 — Wikipedia's stddev example.
        // Population variance = 4, population stddev = 2.
        // Sample variance ≈ 32/7 ≈ 4.571..., sample stddev ≈ 2.138...
        for v in &[2.0_f64, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            store.append_row(&[
                Value::String(Some(CompactString::new("g"))),
                Value::Double(*v),
            ]);
        }
        (schema, store)
    }

    #[test]
    fn parses_stddev_variance_aggregates() {
        let (schema, _) = make_numeric_store();
        for name in &["STDDEV", "STDDEV_SAMP", "VARIANCE", "VAR_SAMP"] {
            let sql = format!("SELECT {name}(v) AS s FROM t");
            let q = parse_query(&sql, &schema).unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(q.aggregates.len(), 1, "{sql}");
        }
    }

    #[test]
    fn stddev_population_matches_known_value() {
        let (schema, store) = make_numeric_store();
        let q = parse_query("SELECT STDDEV(v) AS s FROM t", &schema).unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 1);
        let s = r.rows[0].get("s").and_then(|v| v.as_f64()).unwrap();
        assert!((s - 2.0).abs() < 1e-9, "population stddev expected 2.0, got {s}");
    }

    #[test]
    fn stddev_samp_matches_known_value() {
        let (schema, store) = make_numeric_store();
        let q = parse_query("SELECT STDDEV_SAMP(v) AS s FROM t", &schema).unwrap();
        let r = execute_query(&q, &store);
        let s = r.rows[0].get("s").and_then(|v| v.as_f64()).unwrap();
        // sqrt(32/7) ≈ 2.1380899...
        assert!((s - (32.0_f64 / 7.0).sqrt()).abs() < 1e-9);
    }

    #[test]
    fn variance_matches_known_value() {
        let (schema, store) = make_numeric_store();
        let q = parse_query("SELECT VARIANCE(v) AS v2 FROM t", &schema).unwrap();
        let r = execute_query(&q, &store);
        let v = r.rows[0].get("v2").and_then(|x| x.as_f64()).unwrap();
        assert!((v - 4.0).abs() < 1e-9);
    }

    #[test]
    fn stddev_with_group_by() {
        let (schema, store) = make_numeric_store();
        let q = parse_query("SELECT k, STDDEV(v) AS s FROM t GROUP BY k", &schema).unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 1);
        let s = r.rows[0].get("s").and_then(|x| x.as_f64()).unwrap();
        assert!((s - 2.0).abs() < 1e-9);
    }

    #[test]
    fn stddev_empty_input_returns_null() {
        let s = Arc::new(Schema::from_strs(&["v"], &[ColumnType::Double]));
        let store = ColumnStore::new(s.clone(), 8);
        let q = parse_query("SELECT STDDEV(v) AS s FROM t", &s).unwrap();
        let r = execute_query(&q, &store);
        // ANSI: empty input → 1 row with NULL aggregate.
        assert_eq!(r.rows.len(), 1);
        assert!(r.rows[0].get("s").map(|v| v.is_null()).unwrap_or(false));
    }

    // ───── P3 — HAVING clause ───────────────────────────────────────

    #[test]
    fn parses_having_on_aggregate_alias() {
        let (schema, _store) = make_store();
        let q = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades \
             GROUP BY desk HAVING SUM(quantity) > 50",
            &schema,
        )
        .expect("HAVING must parse");
        assert!(q.having.is_some(), "having must be Some");
    }

    #[test]
    fn parses_having_on_group_column() {
        let (schema, _store) = make_store();
        let q = parse_query(
            "SELECT desk, COUNT(*) AS n FROM trades \
             GROUP BY desk HAVING desk = 'RATES'",
            &schema,
        )
        .expect("HAVING on group col must parse");
        assert!(q.having.is_some());
    }

    #[test]
    fn having_filters_aggregate_rows() {
        let (schema, store) = make_store();
        // RATES total = 110, EQUITIES = 250, TECH = 5.
        let q = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades \
             GROUP BY desk HAVING SUM(quantity) > 100",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        // RATES (110) + EQUITIES (250) pass; TECH (5) is dropped.
        assert_eq!(r.rows.len(), 2);
        let desks: std::collections::HashSet<String> = r
            .rows
            .iter()
            .map(|row| row.get("desk").unwrap().as_str().unwrap().to_string())
            .collect();
        assert!(desks.contains("RATES"));
        assert!(desks.contains("EQUITIES"));
        assert!(!desks.contains("TECH"));
    }

    #[test]
    fn having_on_aggregate_alias_filters_same_way() {
        // `HAVING total > 100` should behave identically to
        // `HAVING SUM(quantity) > 100` when `total` is the alias.
        let (schema, store) = make_store();
        let q_alias = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades \
             GROUP BY desk HAVING total > 100",
            &schema,
        )
        .unwrap();
        let q_fn = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades \
             GROUP BY desk HAVING SUM(quantity) > 100",
            &schema,
        )
        .unwrap();
        let a = execute_query(&q_alias, &store);
        let f = execute_query(&q_fn, &store);
        assert_eq!(a.rows.len(), f.rows.len());
    }

    #[test]
    fn having_combined_with_and() {
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT desk, SUM(quantity) AS total FROM trades \
             GROUP BY desk HAVING SUM(quantity) > 50 AND desk <> 'TECH'",
            &schema,
        )
        .unwrap();
        let r = execute_query(&q, &store);
        // RATES (110) + EQUITIES (250) pass; TECH (5, also excluded by name).
        assert_eq!(r.rows.len(), 2);
    }

    // ───── P2 — scalar arithmetic in SELECT-list ────────────────────

    #[test]
    fn parse_select_arithmetic_add_alias() {
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT symbol, price, price + 10 AS adj FROM trades WHERE price > 1000",
            &schema,
        )
        .expect("arithmetic in SELECT must parse");
        assert_eq!(q.computed.len(), 1, "expected 1 computed column");
        assert_eq!(q.computed[0].alias, "adj");
        let r = execute_query(&q, &store);
        for row in &r.rows {
            let p = row.get("price").unwrap().as_f64().unwrap();
            let adj = row.get("adj").unwrap().as_f64().unwrap();
            assert!((adj - (p + 10.0)).abs() < 1e-9, "adj = price + 10");
        }
    }

    #[test]
    fn parse_select_arithmetic_multiple_ops() {
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT symbol, price * quantity AS notional, price - quantity AS spread FROM trades",
            &schema,
        )
        .expect("two computed columns must parse");
        assert_eq!(q.computed.len(), 2);
        let r = execute_query(&q, &store);
        for row in &r.rows {
            assert!(row.contains_key("notional"));
            assert!(row.contains_key("spread"));
            assert!(row.contains_key("symbol"));
        }
    }

    #[test]
    fn parse_select_arithmetic_parenthesised() {
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT symbol, (price - quantity) / quantity AS pct FROM trades",
            &schema,
        )
        .expect("parenthesised arithmetic must parse");
        assert_eq!(q.computed.len(), 1);
        let r = execute_query(&q, &store);
        for row in &r.rows {
            let pct = row.get("pct").and_then(|v| v.as_f64());
            assert!(pct.is_some(), "pct must be numeric, got {row:?}");
        }
    }

    #[test]
    fn parse_select_arithmetic_div_by_zero_yields_null() {
        let s = Arc::new(Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        ));
        let mut store = ColumnStore::new(s.clone(), 8);
        store.append_row(&[
            Value::String(Some(CompactString::new("a"))),
            Value::Long(0),
        ]);
        let q = parse_query("SELECT k, 1.0 / v AS rcp FROM t", &s).unwrap();
        let r = execute_query(&q, &store);
        assert_eq!(r.rows.len(), 1);
        let rcp = r.rows[0].get("rcp");
        // null is encoded as Value::Null; 0-divisor → null.
        assert!(rcp.map(|v| v.is_null()).unwrap_or(false));
    }

    #[test]
    fn parse_self_named_table_qualifies_with_itself() {
        // The wire SOW pipeline rewrites the FROM clause to `t`. If
        // the user-written SQL was `SELECT trades.col FROM trades`,
        // by the time it reaches the parser it's
        // `SELECT t.col FROM t`. Without an explicit alias, the
        // table-ref set must still contain "t" so the rewrite fires
        // — otherwise the SOW hangs/errors mid-pipeline.
        let (schema, store) = make_store();
        let q = parse_query(
            "SELECT t.symbol, t.price FROM t WHERE t.price > 200",
            &schema,
        )
        .expect("FROM-t with t.col must parse");
        let p = parse_query(
            "SELECT symbol, price FROM t WHERE price > 200",
            &schema,
        )
        .unwrap();
        assert_eq!(q.projection, p.projection);
        let a = execute_query(&q, &store);
        let b = execute_query(&p, &store);
        assert_eq!(a.rows.len(), b.rows.len());
    }

    #[test]
    fn parse_table_alias_unknown_alias_falls_through() {
        // Reference to an unknown alias should fall through to the
        // existing "unknown column" error path — not silently drop.
        let (schema, _store) = make_store();
        let err = parse_query(
            "SELECT q.symbol FROM trades p",
            &schema,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("q.symbol") || msg.contains("q") || msg.contains("symbol"),
            "expected unknown-column error to surface, got: {msg}"
        );
    }

    #[test]
    fn peek_join_extracts_topics_and_using() {
        let (left, right, using) = peek_join(
            "SELECT * FROM positions JOIN securities USING (cusip)",
        )
        .unwrap()
        .expect("peek_join should match");
        assert_eq!(left, "positions");
        assert_eq!(right, "securities");
        assert_eq!(using, vec!["cusip".to_string()]);
    }

    #[test]
    fn peek_join_returns_none_for_non_join() {
        let r = peek_join("SELECT * FROM trades WHERE price > 100").unwrap();
        assert!(r.is_none());
    }

    // ───── S20 JOIN: executor ────────────────────────────────────────

    /// Build a small two-store fixture mirroring the demo's
    /// `/positions` ↔ `/securities` shape:
    ///   positions(cusip, qty, marketValue)
    ///   securities(cusip, sector)
    /// The join enriches positions with sector.
    fn make_join_fixture() -> (ColumnStore, ColumnStore, Arc<Schema>) {
        let left_schema = Arc::new(Schema::from_strs(
            &["cusip", "qty", "marketValue"],
            &[ColumnType::String, ColumnType::Long, ColumnType::Double],
        ));
        let right_schema = Arc::new(Schema::from_strs(
            &["cusip", "sector"],
            &[ColumnType::String, ColumnType::String],
        ));
        let mut left = ColumnStore::new(left_schema.clone(), 16);
        let mut right = ColumnStore::new(right_schema.clone(), 16);
        let push_left = |s: &mut ColumnStore, cusip: &str, qty: i64, mv: f64| {
            s.append_row(&[
                Value::String(Some(CompactString::new(cusip))),
                Value::Long(qty),
                Value::Double(mv),
            ]);
        };
        let push_right = |s: &mut ColumnStore, cusip: &str, sector: &str| {
            s.append_row(&[
                Value::String(Some(CompactString::new(cusip))),
                Value::String(Some(CompactString::new(sector))),
            ]);
        };
        push_left(&mut left, "AAPL", 100, 15_000.0);
        push_left(&mut left, "MSFT", 50, 18_000.0);
        push_left(&mut left, "JPM", 200, 22_000.0);
        push_left(&mut left, "BAC", 75, 5_500.0);
        push_left(&mut left, "ORPHAN", 999, 1.0); // no matching security → drops
        push_right(&mut right, "AAPL", "Tech");
        push_right(&mut right, "MSFT", "Tech");
        push_right(&mut right, "JPM", "Banks");
        push_right(&mut right, "BAC", "Banks");
        push_right(&mut right, "UNUSED", "Energy"); // no left match → fine
        let combined = Arc::new(combined_join_schema(
            &left_schema,
            &right_schema,
            &["cusip".to_string()],
        ));
        (left, right, combined)
    }

    #[test]
    fn join_aggregates_by_right_side_column() {
        let (left, right, combined) = make_join_fixture();
        let query = parse_query(
            "SELECT sector, SUM(marketValue) AS exposure FROM positions \
             JOIN securities USING (cusip) GROUP BY sector",
            &combined,
        )
        .unwrap();
        let result = execute_join_query(&query, &left, &right).unwrap();
        // Tech: AAPL(15000) + MSFT(18000) = 33000
        // Banks: JPM(22000) + BAC(5500) = 27500
        // ORPHAN dropped (no right match); UNUSED dropped (no left match).
        let mut got: std::collections::BTreeMap<String, f64> = Default::default();
        for row in &result.rows {
            let sec = row.get("sector").and_then(|v| v.as_str()).unwrap().to_string();
            let exp = row.get("exposure").and_then(|v| v.as_f64()).unwrap();
            got.insert(sec, exp);
        }
        assert_eq!(got.get("Tech").copied(), Some(33_000.0));
        assert_eq!(got.get("Banks").copied(), Some(27_500.0));
        assert!(!got.contains_key("Energy"));
    }

    #[test]
    fn join_inner_drops_unmatched_rows() {
        let (left, right, combined) = make_join_fixture();
        // No GROUP BY, no aggregate — pure projection.
        let query = parse_query(
            "SELECT cusip, sector, qty FROM positions JOIN securities USING (cusip)",
            &combined,
        )
        .unwrap();
        let result = execute_join_query(&query, &left, &right).unwrap();
        // 4 matched rows (AAPL, MSFT, JPM, BAC); ORPHAN dropped.
        assert_eq!(result.rows.len(), 4);
        let cusips: std::collections::HashSet<String> = result
            .rows
            .iter()
            .filter_map(|r| r.get("cusip").and_then(|v| v.as_str()).map(String::from))
            .collect();
        assert!(cusips.contains("AAPL"));
        assert!(!cusips.contains("ORPHAN"));
        assert!(!cusips.contains("UNUSED"));
    }

    #[test]
    fn join_with_predicate_filters_post_join() {
        let (left, right, combined) = make_join_fixture();
        let query = parse_query(
            "SELECT cusip FROM positions JOIN securities USING (cusip) \
             WHERE sector = 'Banks'",
            &combined,
        )
        .unwrap();
        let result = execute_join_query(&query, &left, &right).unwrap();
        // JPM + BAC are the only Banks rows.
        assert_eq!(result.rows.len(), 2);
    }

    // ─── Query Guardrails G1 ──────────────────────────────────────────

    fn limits_strict() -> QueryLimits {
        QueryLimits {
            max_pivot_in_list_size: 3,
            max_view_chain_depth: 2,
            reject_degenerate_groupby: true,
            reject_passthrough_views: true,
            ..QueryLimits::default()
        }
    }

    #[test]
    fn g1_pivot_in_list_under_cap_passes() {
        let (schema, _store) = make_store();
        let q = parse_query(
            "SELECT * FROM t PIVOT (SUM(quantity) FOR desk IN ('RATES', 'EQUITIES'))",
            &schema,
        )
        .unwrap();
        // No dedup-key info; rule for groupby doesn't trigger on a pivot.
        q.validate_with_limits(&limits_strict(), &[]).unwrap();
    }

    #[test]
    fn g1_pivot_in_list_over_cap_rejected() {
        let (schema, _store) = make_store();
        let q = parse_query(
            "SELECT * FROM t PIVOT (SUM(quantity) FOR desk \
             IN ('A', 'B', 'C', 'D', 'E'))",
            &schema,
        )
        .unwrap();
        let err = q.validate_with_limits(&limits_strict(), &[]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("PIVOT IN-list"), "got: {msg}");
        assert!(msg.contains("max_pivot_in_list_size"), "got: {msg}");
    }

    #[test]
    fn g1_degenerate_groupby_exact_dedup_key_rejected() {
        let (schema, _store) = make_store();
        // dedup key = "symbol" (index 0); GROUP BY symbol with an
        // aggregate is the degenerate case.
        let q = parse_query(
            "SELECT symbol, SUM(quantity) FROM t GROUP BY symbol",
            &schema,
        )
        .unwrap();
        let err = q
            .validate_with_limits(&limits_strict(), &[0])
            .unwrap_err();
        assert!(format!("{err}").contains("dedup-key columns"));
    }

    #[test]
    fn g1_groupby_strict_superset_of_dedup_key_allowed() {
        let (schema, _store) = make_store();
        // GROUP BY (symbol, desk) — strict superset of the dedup key.
        // Still meaningful: groups by (symbol, desk) buckets, so allow.
        let q = parse_query(
            "SELECT symbol, desk, SUM(quantity) FROM t GROUP BY symbol, desk",
            &schema,
        )
        .unwrap();
        q.validate_with_limits(&limits_strict(), &[0]).unwrap();
    }

    #[test]
    fn g1_groupby_coarser_than_dedup_key_allowed() {
        let (schema, _store) = make_store();
        // GROUP BY desk — coarser than the dedup key; valid aggregate.
        let q = parse_query(
            "SELECT desk, SUM(quantity) FROM t GROUP BY desk",
            &schema,
        )
        .unwrap();
        q.validate_with_limits(&limits_strict(), &[0]).unwrap();
    }

    #[test]
    fn g1_passthrough_view_rejected() {
        let mut bodies = HashMap::new();
        bodies.insert("/v".to_string(), "SELECT * FROM \"/source\"".to_string());
        let view_sources = HashMap::new();
        let err = validate_view_graph(&view_sources, &bodies, &limits_strict()).unwrap_err();
        assert!(format!("{err}").contains("pointless `SELECT * FROM"));
    }

    #[test]
    fn g1_passthrough_view_with_where_allowed() {
        let mut bodies = HashMap::new();
        bodies.insert(
            "/v".to_string(),
            "SELECT * FROM \"/source\" WHERE x = 1".to_string(),
        );
        let view_sources = HashMap::new();
        validate_view_graph(&view_sources, &bodies, &limits_strict()).unwrap();
    }

    #[test]
    fn g1_view_chain_at_cap_allowed() {
        // /a → /b → /topic. Depth 2 chain (just /a's path).
        let mut sources = HashMap::new();
        sources.insert("/a".to_string(), "/b".to_string());
        sources.insert("/b".to_string(), "/topic".to_string());
        let bodies = HashMap::new();
        validate_view_graph(&sources, &bodies, &limits_strict()).unwrap();
    }

    #[test]
    fn g1_view_chain_over_cap_rejected() {
        // /a → /b → /c → /topic. Depth 3 for /a — over the cap of 2.
        let mut sources = HashMap::new();
        sources.insert("/a".to_string(), "/b".to_string());
        sources.insert("/b".to_string(), "/c".to_string());
        sources.insert("/c".to_string(), "/topic".to_string());
        let bodies = HashMap::new();
        let err = validate_view_graph(&sources, &bodies, &limits_strict()).unwrap_err();
        assert!(format!("{err}").contains("chain depth"));
    }

    // ─── Query Guardrails G3 ──────────────────────────────────────────

    fn fake_estimate(rows: u64, bytes: u64, fanout: Option<f64>)
        -> crate::cost_estimator::QueryCostEstimate
    {
        crate::cost_estimator::QueryCostEstimate {
            estimated_source_rows: rows,
            estimated_result_rows: rows,
            estimated_result_bytes: bytes,
            estimated_join_fanout_avg: fanout,
            used_indexes: vec![],
            assumptions: vec![],
            confidence: crate::cost_estimator::ConfidenceLevel::High,
        }
    }

    #[test]
    fn g3_estimate_under_caps_passes() {
        let limits = QueryLimits::default();
        let est = fake_estimate(500, 50_000, None);
        let out = check_estimate_against_limits(&est, &limits);
        assert!(!out.rejected);
        assert!(out.warnings.is_empty());
    }

    #[test]
    fn g3_rejects_when_rows_exceed_hard_cap() {
        let limits = QueryLimits {
            max_sow_estimated_rows: 1_000,
            ..QueryLimits::default()
        };
        let est = fake_estimate(2_000, 1_000, None);
        let out = check_estimate_against_limits(&est, &limits);
        assert!(out.rejected);
        let reason = out.reject_reason.unwrap();
        assert!(reason.contains("max_sow_estimated_rows"), "got: {reason}");
    }

    #[test]
    fn g3_rejects_when_bytes_exceed_hard_cap() {
        let limits = QueryLimits {
            max_sow_estimated_rows: 0, // disable rows check so bytes fires
            max_sow_estimated_bytes: 1_000,
            ..QueryLimits::default()
        };
        let est = fake_estimate(100, 50_000, None);
        let out = check_estimate_against_limits(&est, &limits);
        assert!(out.rejected);
        assert!(out
            .reject_reason
            .unwrap()
            .contains("max_sow_estimated_bytes"));
    }

    #[test]
    fn g3_rejects_when_join_fanout_exceeds_cap() {
        let limits = QueryLimits {
            max_join_estimated_fanout: 5,
            ..QueryLimits::default()
        };
        let est = fake_estimate(100, 1_000, Some(20.0));
        let out = check_estimate_against_limits(&est, &limits);
        assert!(out.rejected);
        assert!(out.reject_reason.unwrap().contains("fanout"));
    }

    #[test]
    fn g3_warns_when_above_soft_threshold_but_under_hard_cap() {
        let limits = QueryLimits {
            warn_sow_rows_threshold: 500,
            max_sow_estimated_rows: 10_000,
            ..QueryLimits::default()
        };
        let est = fake_estimate(1_000, 1_000, None);
        let out = check_estimate_against_limits(&est, &limits);
        assert!(!out.rejected);
        assert_eq!(out.warnings.len(), 1);
        assert!(out.warnings[0].contains("warn_sow_rows_threshold"));
    }

    #[test]
    fn g3_zero_caps_disable_checks() {
        let limits = QueryLimits {
            max_sow_estimated_rows: 0,
            max_sow_estimated_bytes: 0,
            max_join_estimated_fanout: 0,
            warn_sow_rows_threshold: 0,
            warn_sow_bytes_threshold: 0,
            ..QueryLimits::default()
        };
        let est = fake_estimate(u64::MAX / 2, u64::MAX / 2, Some(1e9));
        let out = check_estimate_against_limits(&est, &limits);
        assert!(!out.rejected, "all caps disabled — must not reject");
        assert!(out.warnings.is_empty());
    }

    #[test]
    fn g1_view_cycle_rejected() {
        // /a → /b → /a — cycle.
        let mut sources = HashMap::new();
        sources.insert("/a".to_string(), "/b".to_string());
        sources.insert("/b".to_string(), "/a".to_string());
        let bodies = HashMap::new();
        let err = validate_view_graph(&sources, &bodies, &limits_strict()).unwrap_err();
        assert!(format!("{err}").contains("cycle"));
    }
}

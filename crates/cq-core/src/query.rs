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
}

/// Compiled `A JOIN B USING (col, ...)` spec. Schemas of both sides
/// are resolved at query-execution time (via `Topic` lookup in the
/// server's topic map) — the parser stores symbolic names only so
/// re-parse on schema-discovery boundaries stays cheap.
#[derive(Debug, Clone)]
pub struct JoinSpec {
    /// Topic name on the right side of the JOIN.
    pub right_topic: String,
    /// USING column names (must exist on BOTH sides). Today only
    /// INNER JOIN + USING is supported; LEFT OUTER + ON-with-Expr
    /// are tracked as follow-ups.
    pub using: Vec<String>,
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
}

impl PivotLiteral {
    /// Stringify for output column naming. `'A'` becomes `"A"`,
    /// `100` becomes `"100"`. Matches Snowflake conventions.
    pub fn as_column_label(&self) -> String {
        match self {
            PivotLiteral::String(s) => s.to_string(),
            PivotLiteral::Long(n) => n.to_string(),
            PivotLiteral::Double(bits) => f64::from_bits(*bits).to_string(),
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
        Statement::Query(query) => parse_select(query, schema),
        _ => Err(QueryError::ParseError("Only SELECT statements supported".into())),
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
    let select = match q.body.as_ref() {
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
        pivot: None,
        unpivot: None,
        join: join_spec,
    })
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
    // Accept `[INNER] JOIN ... USING (col, ...)` only.
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    let constraint = match &j.join_operator {
        JoinOperator::Inner(c) | JoinOperator::Join(c) => c,
        _ => {
            return Err(QueryError::ParseError(
                "Only INNER JOIN is supported today".into(),
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
        _ => {
            return Err(QueryError::ParseError(
                "JOIN must specify USING (col, ...)".into(),
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
        // `matching_rows` is the post-sort, post-limit list of row
        // indices the projection just walked — in lockstep with
        // `rows`. Callers (Topic::query + streaming + subscribe)
        // use this to apply the tombstone filter by row index.
        source_rows: matching_rows,
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
}

impl AggState {
    /// Build the initial state for an aggregate, given the column
    /// type (or `None` for COUNT(*)).
    pub fn init(func: AggFn, col_type: Option<crate::schema::ColumnType>) -> AggState {
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
            let state = AggState::init(spec.func, agg_col_types[i]);
            row_map.insert(spec.alias.clone(), state.finalize());
        }
        rows.push(row_map);
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
        }
    }
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
    let left_row_count = left_store.row_count();
    let mut combined =
        ColumnStore::new(combined_schema.clone(), left_row_count as usize + 16);
    let mut row_buf: Vec<Value> = Vec::with_capacity(combined_sources.len());
    for lr in 0..left_row_count {
        let key = match key_for(left_store, lr, &left_using) {
            Some(k) => k,
            None => continue, // left key NULL → inner join drops
        };
        let rr = match right_index.get(&key).copied() {
            Some(rr) => rr,
            None => continue, // no right match → inner join drops
        };
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

    #[test]
    fn parse_join_rejects_left_outer_for_now() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let r = parse_query(
            "SELECT v FROM a LEFT JOIN b USING (k)",
            &combined,
        );
        assert!(r.is_err(), "LEFT OUTER JOIN must be rejected today");
    }

    #[test]
    fn parse_join_rejects_on_clause_for_now() {
        let combined = Schema::from_strs(
            &["k", "v"],
            &[ColumnType::String, ColumnType::Long],
        );
        let r = parse_query("SELECT v FROM a JOIN b ON a.k = b.k", &combined);
        assert!(r.is_err(), "ON-clause JOIN must be rejected today");
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

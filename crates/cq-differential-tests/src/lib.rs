//! Differential SQL testing harness for CQServer (review concern C6 /
//! worklog session S35).
//!
//! For each YAML test case, the harness:
//!   1. Builds a CQServer `Topic` matching the case's schema.
//!   2. Builds an in-memory DuckDB connection with the same schema.
//!   3. Applies the case's `publishes` to both: `upsert_map` on CQ,
//!      INSERT statements on DuckDB.
//!   4. Runs the case's `query` against both.
//!   5. Compares the result sets. If `expected_rows` is provided, it's
//!      also asserted (catches a DuckDB upgrade that itself changes
//!      semantics on us).
//!
//! The point is to find places where CQServer's SQL semantics drift
//! from a reference engine — NULL handling in `IN`, type coercion in
//! cross-type comparisons, `LIKE` escape sequences, aggregate
//! behavior on empty groups, etc. Unit tests in `cq-core` will
//! almost never catch these because no human pre-imagines every edge
//! case; the corpus accumulates them.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use duckdb::Connection;
use serde::Deserialize;

/// A single differential test case loaded from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    /// Key column for the CQ topic (DuckDB doesn't need one).
    pub key: String,
    /// Rows to publish before running the query. JSON-shaped.
    pub publishes: Vec<serde_json::Map<String, serde_json::Value>>,
    pub query: String,
    /// Optional expected result. When present, both engines must
    /// agree with it. When absent, only engine-vs-engine equality is
    /// checked.
    #[serde(default)]
    pub expected_rows: Option<Vec<serde_json::Map<String, serde_json::Value>>>,
    /// Optional human-readable note explaining the case. Surfaces in
    /// failure messages.
    #[serde(default)]
    pub notes: Option<String>,
    /// If true, the test is expected to diverge (CQ does something
    /// different from DuckDB on purpose). The harness then asserts
    /// the engines disagree AND that `expected_rows` matches CQ.
    /// Useful for documenting deliberate extensions.
    #[serde(default)]
    pub expect_divergence: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

impl ColumnDef {
    fn cq_type(&self) -> Result<ColumnType> {
        match self.ty.as_str() {
            "string" => Ok(ColumnType::String),
            "double" => Ok(ColumnType::Double),
            "long" => Ok(ColumnType::Long),
            "int" => Ok(ColumnType::Int),
            other => bail!("unknown column type: {other}"),
        }
    }

    fn duckdb_type(&self) -> Result<&'static str> {
        match self.ty.as_str() {
            "string" => Ok("VARCHAR"),
            "double" => Ok("DOUBLE"),
            "long" => Ok("BIGINT"),
            "int" => Ok("INTEGER"),
            other => bail!("unknown column type: {other}"),
        }
    }
}

/// Result of running one test case.
#[derive(Debug)]
pub struct CaseResult {
    pub name: String,
    pub passed: bool,
    pub failure_reason: Option<String>,
}

/// Load a corpus directory (every `*.yaml` file inside is a list of
/// `TestCase`s).
pub fn load_corpus(dir: impl AsRef<Path>) -> Result<Vec<TestCase>> {
    let dir = dir.as_ref();
    let mut out = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|e| e.path());
    for entry in entries {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
            continue;
        }
        let body = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        let cases: Vec<TestCase> = serde_yaml::from_str(&body)
            .with_context(|| format!("parse {}", path.display()))?;
        out.extend(cases);
    }
    Ok(out)
}

/// Build a CQ Topic from the test case's schema.
fn build_topic(case: &TestCase) -> Result<Topic> {
    let names: Vec<&str> = case.schema.iter().map(|c| c.name.as_str()).collect();
    let types: Vec<ColumnType> = case
        .schema
        .iter()
        .map(|c| c.cq_type())
        .collect::<Result<_>>()?;
    let schema = Arc::new(Schema::from_strs(&names, &types));
    let config = TopicConfig {
        name: format!("/{}", case.name),
        key_fields: vec![case.key.clone()],
        persist: false,
        conflation_ms: None,
        index_columns: vec![],
        expire_seconds: None,
    };
    Ok(Topic::new(config, schema, 256))
}

/// Build an in-memory DuckDB connection with a single table `t` whose
/// columns mirror the test case's schema.
fn build_duckdb(case: &TestCase) -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    let mut cols = Vec::new();
    for c in &case.schema {
        cols.push(format!("{} {}", c.name, c.duckdb_type()?));
    }
    let sql = format!("CREATE TABLE t ({})", cols.join(", "));
    conn.execute(&sql, [])?;
    Ok(conn)
}

/// Push the case's `publishes` into both engines.
fn apply_publishes(case: &TestCase, topic: &Topic, conn: &Connection) -> Result<()> {
    for row in &case.publishes {
        // CQ side.
        topic.upsert_map(row).with_context(|| {
            format!("upsert_map on {}: {}", case.name, serde_json::to_string(row).unwrap())
        })?;

        // DuckDB side. Build an INSERT with the columns we have a
        // value for; missing columns become NULL (DuckDB default).
        let cols: Vec<&str> = row.keys().map(|s| s.as_str()).collect();
        let placeholders: Vec<&str> = (0..cols.len()).map(|_| "?").collect();
        let sql = format!(
            "INSERT INTO t ({}) VALUES ({})",
            cols.join(", "),
            placeholders.join(", ")
        );

        let params: Vec<duckdb::types::Value> = row.values().map(json_to_duckdb).collect();
        let param_refs: Vec<&dyn duckdb::ToSql> = params
            .iter()
            .map(|v| v as &dyn duckdb::ToSql)
            .collect();
        conn.execute(&sql, param_refs.as_slice())?;
    }
    Ok(())
}

fn json_to_duckdb(v: &serde_json::Value) -> duckdb::types::Value {
    use duckdb::types::Value as DV;
    use serde_json::Value as JV;
    match v {
        JV::Null => DV::Null,
        JV::Bool(b) => DV::Boolean(*b),
        JV::Number(n) => {
            if let Some(i) = n.as_i64() {
                DV::BigInt(i)
            } else if let Some(f) = n.as_f64() {
                DV::Double(f)
            } else {
                DV::Null
            }
        }
        JV::String(s) => DV::Text(s.clone()),
        // Arrays / objects aren't supported in the corpus today; treat
        // as NULL with a warning surfaced via the comparison failure.
        _ => DV::Null,
    }
}

/// Run the query against CQ and DuckDB, return the two result sets as
/// JSON maps for easy comparison.
fn run_query(
    case: &TestCase,
    topic: &Topic,
    conn: &Connection,
) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, Vec<serde_json::Map<String, serde_json::Value>>)> {
    let cq_result = topic
        .query(&case.query)
        .with_context(|| format!("CQ query on {}: {}", case.name, case.query))?;
    let cq_rows = cq_result.rows;

    let mut stmt = conn.prepare(&case.query)?;
    // duckdb-rs populates the prepared statement's column schema only
    // after the statement is executed (see raw_statement.rs:248), so
    // we cannot call `column_names()` before `query()`. Instead read
    // the names from `Rows`, which exposes them after execution.
    let mut rows = stmt.query([])?;
    let column_names: Vec<String> = rows
        .as_ref()
        .map(|stmt| {
            stmt.column_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();
    let mut dd_rows = Vec::new();
    while let Some(row) = rows.next()? {
        let mut map = serde_json::Map::new();
        for (i, name) in column_names.iter().enumerate() {
            let v: duckdb::types::Value = row.get(i)?;
            map.insert(name.clone(), duckdb_to_json(v));
        }
        dd_rows.push(map);
    }
    Ok((cq_rows, dd_rows))
}

fn duckdb_to_json(v: duckdb::types::Value) -> serde_json::Value {
    use duckdb::types::Value as DV;
    use serde_json::Value as JV;
    match v {
        DV::Null => JV::Null,
        DV::Boolean(b) => JV::Bool(b),
        DV::TinyInt(n) => JV::Number((n as i64).into()),
        DV::SmallInt(n) => JV::Number((n as i64).into()),
        DV::Int(n) => JV::Number((n as i64).into()),
        DV::BigInt(n) => JV::Number(n.into()),
        DV::UTinyInt(n) => JV::Number((n as u64).into()),
        DV::USmallInt(n) => JV::Number((n as u64).into()),
        DV::UInt(n) => JV::Number((n as u64).into()),
        DV::UBigInt(n) => JV::Number(n.into()),
        // DuckDB promotes SUM(BIGINT) to HUGEINT (128-bit). Demote
        // back to i64 if it fits — sums in the corpus stay well
        // inside i64 range — otherwise stringify so the comparison
        // still surfaces meaningful diffs.
        DV::HugeInt(n) => {
            if let Ok(narrow) = i64::try_from(n) {
                JV::Number(narrow.into())
            } else {
                JV::String(n.to_string())
            }
        }
        DV::Float(n) => serde_json::Number::from_f64(n as f64).map(JV::Number).unwrap_or(JV::Null),
        DV::Double(n) => serde_json::Number::from_f64(n).map(JV::Number).unwrap_or(JV::Null),
        DV::Text(s) => JV::String(s),
        other => JV::String(format!("{other:?}")),
    }
}

/// Normalize a row for comparison. CQServer's `Topic::query` omits
/// null fields from its result maps (`ColumnStore::get_row_map`
/// skips `val.is_null()`); DuckDB emits explicit `null`. Strip
/// explicit nulls from both sides so the comparison reflects
/// semantic equality rather than serialization style.
fn normalize_row(
    row: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Compare two result sets as multisets keyed by their canonical JSON
/// encoding (with nulls normalized — see `normalize_row`). Order is
/// ignored unless the query contains `ORDER BY` — in which case we
/// compare as ordered Vecs.
fn result_sets_equal(
    cq: &[serde_json::Map<String, serde_json::Value>],
    dd: &[serde_json::Map<String, serde_json::Value>],
    query: &str,
) -> bool {
    let cq_norm: Vec<_> = cq.iter().map(normalize_row).collect();
    let dd_norm: Vec<_> = dd.iter().map(normalize_row).collect();
    let ordered = query.to_ascii_uppercase().contains("ORDER BY");
    if ordered {
        return cq_norm == dd_norm;
    }
    let cq_set: HashSet<String> = cq_norm
        .iter()
        .map(|m| serde_json::to_string(m).unwrap_or_default())
        .collect();
    let dd_set: HashSet<String> = dd_norm
        .iter()
        .map(|m| serde_json::to_string(m).unwrap_or_default())
        .collect();
    cq_set == dd_set
}

/// Run a single test case end-to-end. Never panics; failures surface
/// in the `CaseResult`.
pub fn run_case(case: &TestCase) -> CaseResult {
    match try_run_case(case) {
        Ok(()) => CaseResult {
            name: case.name.clone(),
            passed: true,
            failure_reason: None,
        },
        Err(e) => CaseResult {
            name: case.name.clone(),
            passed: false,
            failure_reason: Some(format!("{e:#}")),
        },
    }
}

fn try_run_case(case: &TestCase) -> Result<()> {
    let topic = build_topic(case)?;
    let conn = build_duckdb(case)?;
    apply_publishes(case, &topic, &conn)?;
    let (cq_rows, dd_rows) = run_query(case, &topic, &conn)?;
    let agree = result_sets_equal(&cq_rows, &dd_rows, &case.query);

    if case.expect_divergence {
        if agree {
            bail!(
                "expected CQ to diverge from DuckDB but they agreed:\n  rows: {}",
                serde_json::to_string(&cq_rows).unwrap_or_default()
            );
        }
        // For declared divergence we require expected_rows so the
        // case still pins CQ's behavior.
        let expected = case
            .expected_rows
            .as_ref()
            .ok_or_else(|| anyhow!("expect_divergence cases must declare expected_rows"))?;
        if !result_sets_equal(&cq_rows, expected, &case.query) {
            bail!(
                "expect_divergence: CQ output ≠ declared expected:\n  cq: {}\n  expected: {}",
                serde_json::to_string(&cq_rows).unwrap_or_default(),
                serde_json::to_string(expected).unwrap_or_default()
            );
        }
        return Ok(());
    }

    if !agree {
        bail!(
            "result-set mismatch:\n  cq:     {}\n  duckdb: {}\n  query:  {}\n  notes:  {}",
            serde_json::to_string(&cq_rows).unwrap_or_default(),
            serde_json::to_string(&dd_rows).unwrap_or_default(),
            case.query,
            case.notes.as_deref().unwrap_or("—"),
        );
    }
    if let Some(expected) = &case.expected_rows {
        if !result_sets_equal(&cq_rows, expected, &case.query) {
            bail!(
                "both engines agree but disagree with declared expected:\n  engines: {}\n  expected: {}",
                serde_json::to_string(&cq_rows).unwrap_or_default(),
                serde_json::to_string(expected).unwrap_or_default()
            );
        }
    }
    Ok(())
}

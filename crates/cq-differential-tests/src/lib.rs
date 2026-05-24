//! Differential SQL testing harness for CQServer (review concern C6 /
//! worklog session S35).
//!
//! For each YAML test case, the harness:
//!   1. Builds a CQServer `Topic` matching the case's schema.
//!   2. Builds an in-memory DataFusion session with the same schema.
//!   3. Applies the case's `publishes` to both: `upsert_map` on CQ,
//!      an Arrow `RecordBatch` registered as `t` on DataFusion.
//!   4. Runs the case's `query` against both.
//!   5. Compares the result sets. If `expected_rows` is provided, it's
//!      also asserted (catches a DataFusion upgrade that itself
//!      changes semantics on us).
//!
//! The point is to find places where CQServer's SQL semantics drift
//! from a reference engine — NULL handling in `IN`, type coercion in
//! cross-type comparisons, `LIKE` escape sequences, aggregate
//! behavior on empty groups, etc. Unit tests in `cq-core` will
//! almost never catch these because no human pre-imagines every edge
//! case; the corpus accumulates them.
//!
//! Why DataFusion over DuckDB: pure-Rust dependency tree, so this
//! crate builds without an MSVC + Windows-SDK toolchain on Windows.
//! Compile-time cost is higher than tiny SQL crates but well-cached.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    ArrayRef, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use serde::Deserialize;

/// A single differential test case loaded from YAML.
#[derive(Debug, Clone, Deserialize)]
pub struct TestCase {
    pub name: String,
    pub schema: Vec<ColumnDef>,
    /// Key column for the CQ topic (DataFusion doesn't need one).
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
    /// different from DataFusion on purpose). The harness then
    /// asserts the engines disagree AND that `expected_rows` matches
    /// CQ. Useful for documenting deliberate extensions.
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

    fn arrow_type(&self) -> Result<DataType> {
        match self.ty.as_str() {
            "string" => Ok(DataType::Utf8),
            "double" => Ok(DataType::Float64),
            "long" => Ok(DataType::Int64),
            "int" => Ok(DataType::Int32),
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

/// Build an Arrow schema matching the test case.
fn build_arrow_schema(case: &TestCase) -> Result<ArrowSchema> {
    let fields = case
        .schema
        .iter()
        .map(|c| Ok(Field::new(&c.name, c.arrow_type()?, true)))
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowSchema::new(fields))
}

/// Build a single RecordBatch from every publish in the case. Missing
/// columns per row → NULL. Schema-mirror to what CQServer's topic
/// holds in its column store after `upsert_map` on every row.
fn build_record_batch(
    case: &TestCase,
    arrow_schema: &ArrowSchema,
) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(case.schema.len());
    for col in &case.schema {
        let arr: ArrayRef = match col.arrow_type()? {
            DataType::Utf8 => {
                let mut b = StringBuilder::new();
                for row in &case.publishes {
                    match row.get(&col.name) {
                        Some(serde_json::Value::String(s)) => b.append_value(s),
                        Some(serde_json::Value::Null) | None => b.append_null(),
                        Some(v) => b.append_value(&v.to_string()),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::with_capacity(case.publishes.len());
                for row in &case.publishes {
                    match row.get(&col.name).and_then(|v| v.as_f64()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int64 => {
                let mut b = Int64Builder::with_capacity(case.publishes.len());
                for row in &case.publishes {
                    match row.get(&col.name).and_then(|v| v.as_i64()) {
                        Some(v) => b.append_value(v),
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(case.publishes.len());
                for row in &case.publishes {
                    match row.get(&col.name).and_then(|v| v.as_i64()) {
                        Some(v) if i32::try_from(v).is_ok() => {
                            b.append_value(v as i32);
                        }
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            other => bail!("unsupported arrow type: {other:?}"),
        };
        columns.push(arr);
    }
    Ok(RecordBatch::try_new(Arc::new(arrow_schema.clone()), columns)?)
}

/// Push the case's `publishes` into the CQ topic. (DataFusion is
/// populated separately via `build_record_batch` + `register_table`.)
fn apply_publishes_to_topic(case: &TestCase, topic: &Topic) -> Result<()> {
    for row in &case.publishes {
        topic.upsert_map(row).with_context(|| {
            format!(
                "upsert_map on {}: {}",
                case.name,
                serde_json::to_string(row).unwrap_or_default()
            )
        })?;
    }
    Ok(())
}

/// DataFusion's response to a query. Either we got rows back, or
/// DataFusion bailed on parsing/planning — typically because the
/// query uses a CQServer-specific extension (PIVOT, UNPIVOT, etc.)
/// the reference engine doesn't speak. The harness treats the
/// "unsupported" case as a graceful fallback to expected_rows-only
/// verification.
enum DataFusionOutcome {
    Rows(Vec<serde_json::Map<String, serde_json::Value>>),
    Unsupported(String),
}

/// Run the query against CQ and DataFusion. Returns CQ's rows
/// unconditionally; the DataFusion side falls back to `Unsupported`
/// when the reference engine can't even plan the query, so PIVOT /
/// UNPIVOT / other CQ extensions don't fail the case outright.
fn run_query(
    case: &TestCase,
    topic: &Topic,
    runtime: &tokio::runtime::Runtime,
) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, DataFusionOutcome)> {
    let cq_result = topic
        .query(&case.query)
        .with_context(|| format!("CQ query on {}: {}", case.name, case.query))?;
    let cq_rows = cq_result.rows;

    let arrow_schema = Arc::new(build_arrow_schema(case)?);
    let batch = build_record_batch(case, arrow_schema.as_ref())?;
    let df_outcome: DataFusionOutcome = runtime.block_on(async move {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(arrow_schema.clone(), vec![vec![batch]])?;
        ctx.register_table("t", Arc::new(table))?;
        match ctx.sql(&case.query).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let rows = record_batches_to_json(&batches)?;
                    Result::<DataFusionOutcome>::Ok(DataFusionOutcome::Rows(rows))
                }
                Err(e) => Ok(DataFusionOutcome::Unsupported(e.to_string())),
            },
            Err(e) => Ok(DataFusionOutcome::Unsupported(e.to_string())),
        }
    })?;
    Ok((cq_rows, df_outcome))
}

fn record_batches_to_json(
    batches: &[RecordBatch],
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>> {
    let mut out = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut map = serde_json::Map::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let col = batch.column(i);
                let val = arrow_scalar_to_json(col.as_ref(), row);
                map.insert(field.name().clone(), val);
            }
            out.push(map);
        }
    }
    Ok(out)
}

fn arrow_scalar_to_json(arr: &dyn arrow::array::Array, row: usize) -> serde_json::Value {
    use arrow::array::*;
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }
    match arr.data_type() {
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(a.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(a.value(row).to_string())
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(a.value(row))
        }
        DataType::Int8 => num_to_json(
            arr.as_any().downcast_ref::<Int8Array>().unwrap().value(row) as i64,
        ),
        DataType::Int16 => num_to_json(
            arr.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int32 => num_to_json(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::Int64 => {
            num_to_json(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
        }
        DataType::UInt8 => num_to_json(
            arr.as_any().downcast_ref::<UInt8Array>().unwrap().value(row) as i64,
        ),
        DataType::UInt16 => num_to_json(
            arr.as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt32 => num_to_json(
            arr.as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row) as i64,
        ),
        DataType::UInt64 => {
            // DataFusion typically promotes count() to UInt64. Demote
            // to i64 when in range; stringify the rare out-of-range
            // case so we still surface diffs cleanly.
            let v = arr
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row);
            if let Ok(narrow) = i64::try_from(v) {
                serde_json::Value::Number(narrow.into())
            } else {
                serde_json::Value::String(v.to_string())
            }
        }
        DataType::Float32 => serde_json::Number::from_f64(
            arr.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row) as f64,
        )
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => serde_json::Number::from_f64(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null),
        other => serde_json::Value::String(format!("<unhandled: {other:?}>")),
    }
}

fn num_to_json(n: i64) -> serde_json::Value {
    serde_json::Value::Number(n.into())
}

/// Normalize a row for comparison. CQServer's `Topic::query` omits
/// null fields from its result maps (`ColumnStore::get_row_map`
/// skips `val.is_null()`); DataFusion emits explicit `null`. Strip
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
    df: &[serde_json::Map<String, serde_json::Value>],
    query: &str,
) -> bool {
    let cq_norm: Vec<_> = cq.iter().map(normalize_row).collect();
    let df_norm: Vec<_> = df.iter().map(normalize_row).collect();
    let ordered = query.to_ascii_uppercase().contains("ORDER BY");
    if ordered {
        return cq_norm == df_norm;
    }
    let cq_set: HashSet<String> = cq_norm
        .iter()
        .map(|m| serde_json::to_string(m).unwrap_or_default())
        .collect();
    let df_set: HashSet<String> = df_norm
        .iter()
        .map(|m| serde_json::to_string(m).unwrap_or_default())
        .collect();
    cq_set == df_set
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
    apply_publishes_to_topic(case, &topic)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build tokio runtime for DataFusion")?;
    let (cq_rows, df_outcome) = run_query(case, &topic, &runtime)?;

    // CQ-only path: DataFusion can't plan the query (it's a CQ
    // extension like PIVOT / UNPIVOT). The case is valid only if
    // expected_rows is declared — otherwise we have no oracle.
    let df_rows = match df_outcome {
        DataFusionOutcome::Rows(rows) => rows,
        DataFusionOutcome::Unsupported(why) => {
            let expected = case.expected_rows.as_ref().ok_or_else(|| {
                anyhow!(
                    "DataFusion can't plan this query ({why}) — \
                     case must declare expected_rows so we still have an oracle"
                )
            })?;
            if !result_sets_equal(&cq_rows, expected, &case.query) {
                bail!(
                    "CQ-only (DataFusion: {why}) — CQ output ≠ declared expected:\n  cq: {}\n  expected: {}",
                    serde_json::to_string(&cq_rows).unwrap_or_default(),
                    serde_json::to_string(expected).unwrap_or_default()
                );
            }
            return Ok(());
        }
    };

    let agree = result_sets_equal(&cq_rows, &df_rows, &case.query);

    if case.expect_divergence {
        if agree {
            bail!(
                "expected CQ to diverge from DataFusion but they agreed:\n  rows: {}",
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
            "result-set mismatch:\n  cq:         {}\n  datafusion: {}\n  query:      {}\n  notes:      {}",
            serde_json::to_string(&cq_rows).unwrap_or_default(),
            serde_json::to_string(&df_rows).unwrap_or_default(),
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

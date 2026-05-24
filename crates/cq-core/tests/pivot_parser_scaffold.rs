//! Parser-level smoke tests (originally S43 scaffold; now post-S43
//! flips into asserting actual executor behavior).
//!
//! Lives separately from `pivot_executor.rs` so the two failure
//! modes — "SQL doesn't parse at all" vs "SQL parses but executes
//! wrong" — surface from distinct test files.

use std::sync::Arc;

use cq_core::query::parse_query;
use cq_core::schema::{ColumnType, Schema};

fn pivot_schema() -> Arc<Schema> {
    Arc::new(Schema::from_strs(
        &["trader", "desk", "qty"],
        &[ColumnType::String, ColumnType::String, ColumnType::Long],
    ))
}

#[test]
fn pivot_sql_parses_into_a_pivot_query() {
    let schema = pivot_schema();
    let sql = "SELECT * FROM trades \
               PIVOT (SUM(qty) FOR desk IN ('RATES', 'FX', 'EQUITIES'))";
    let q = parse_query(sql, &schema).expect("parse PIVOT");
    let pivot = q.pivot.as_ref().expect("pivot spec present");
    assert!(!pivot.dynamic, "static IN-list parsed as dynamic");
    assert_eq!(pivot.pivot_values.len(), 3);
    assert!(q.unpivot.is_none());
}

#[test]
fn unpivot_sql_parses_into_an_unpivot_query() {
    let schema = Arc::new(Schema::from_strs(
        &["trader", "rates", "fx", "equities"],
        &[ColumnType::String, ColumnType::Long, ColumnType::Long, ColumnType::Long],
    ));
    let sql = "SELECT * FROM wide_trades UNPIVOT (qty FOR desk IN (rates, fx, equities))";
    let q = parse_query(sql, &schema).expect("parse UNPIVOT");
    let unpivot = q.unpivot.as_ref().expect("unpivot spec present");
    assert_eq!(unpivot.value_col_name, "qty");
    assert_eq!(unpivot.name_col_name, "desk");
    assert_eq!(unpivot.source_cols.len(), 3);
    assert!(q.pivot.is_none());
}

#[test]
fn dynamic_pivot_in_any_parses_with_dynamic_flag_set() {
    let schema = pivot_schema();
    let sql = "SELECT * FROM trades PIVOT (SUM(qty) FOR desk IN (ANY))";
    let q = parse_query(sql, &schema).expect("parse PIVOT IN (ANY)");
    let pivot = q.pivot.as_ref().expect("pivot spec present");
    assert!(pivot.dynamic, "IN (ANY) should set dynamic=true");
    assert!(pivot.pivot_values.is_empty(), "dynamic spec has no pre-known values");
}

#[test]
fn non_pivot_select_still_parses_normally() {
    // Sanity: the pivot path didn't accidentally hijack normal
    // SELECT parsing.
    let schema = pivot_schema();
    let q = parse_query("SELECT trader, qty FROM trades WHERE desk = 'RATES'", &schema)
        .expect("plain select should still parse");
    assert_eq!(q.topic, "trades");
    assert!(q.pivot.is_none());
    assert!(q.unpivot.is_none());
}

//! Filter evaluation hot path (review C12.1 / worklog S37).
//!
//! Measures the cost of evaluating a 5-clause WHERE predicate against
//! a row stored in a `ColumnStore`. The schema mimics a realistic
//! market-data row (10 columns: 4 strings, 3 doubles, 3 longs) and the
//! predicate touches columns of three distinct types to exercise the
//! full compiled-predicate dispatch path.

use std::sync::Arc;

use compact_str::CompactString;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use cq_core::query::parse_query;
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};

const ROWS: usize = 10_000;

fn build_store() -> (Arc<Schema>, ColumnStore) {
    let schema = Arc::new(Schema::from_strs(
        &[
            "symbol", "desk", "trader", "book",
            "price", "notional", "spread",
            "qty", "ts", "version",
        ],
        &[
            ColumnType::String, ColumnType::String, ColumnType::String, ColumnType::String,
            ColumnType::Double, ColumnType::Double, ColumnType::Double,
            ColumnType::Long, ColumnType::Long, ColumnType::Long,
        ],
    ));
    let mut store = ColumnStore::new(schema.clone(), ROWS);
    for i in 0..ROWS {
        let desk = if i % 3 == 0 { "RATES" } else if i % 3 == 1 { "EQUITIES" } else { "FX" };
        store.append_row(&[
            Value::String(Some(CompactString::new(format!("SYM{:05}", i % 500)))),
            Value::String(Some(CompactString::new(desk))),
            Value::String(Some(CompactString::new(format!("T{:03}", i % 20)))),
            Value::String(Some(CompactString::new(format!("BK{:02}", i % 8)))),
            Value::Double(50.0 + (i as f64 * 0.001)),
            Value::Double(1000.0 + (i as f64)),
            Value::Double(0.05 + (i as f64 * 0.0001)),
            Value::Long((i % 1000) as i64),
            Value::Long(1_700_000_000 + i as i64),
            Value::Long((i % 50) as i64),
        ]);
    }
    (schema, store)
}

fn bench_filter_5_predicates_10_cols(c: &mut Criterion) {
    let (schema, store) = build_store();
    let sql = "SELECT * FROM t WHERE desk = 'RATES' AND price > 100.0 AND qty < 500 AND symbol LIKE 'SYM%' AND notional > 5000.0";
    let parsed = parse_query(sql, &schema).expect("parse");
    let predicate = parsed.predicate.clone();

    c.bench_function("filter_eval_5pred_10col", |b| {
        b.iter(|| {
            let mut matches = 0u32;
            let row_count = store.row_count();
            for row in 0..row_count {
                if predicate.matches(&store, row) {
                    matches += 1;
                }
            }
            black_box(matches)
        });
    });
}

criterion_group!(benches, bench_filter_5_predicates_10_cols);
criterion_main!(benches);

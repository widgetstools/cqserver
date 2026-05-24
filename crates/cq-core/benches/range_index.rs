//! S30 range-index bench — proves the indexed path is meaningfully
//! faster than a full scan at scale, and provides a baseline for
//! the bench-regression CI guard rail (S37).
//!
//! Layout: 100K rows, single `long` column. Two benches per
//! selectivity:
//!   - `range_index_*`: index covers the column → planner uses
//!     the B-tree walk.
//!   - `full_scan_*`: index does NOT cover the column → planner
//!     falls through to a full scan.

use std::sync::Arc;

use compact_str::CompactString;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};
use cq_core::query::{execute_query_with_index, parse_query};
use cq_core::sec_index::SecondaryIndex;

const ROWS: usize = 100_000;

fn build_store() -> (Arc<Schema>, ColumnStore, SecondaryIndex) {
    // 100K rows, 1000 distinct `v` values (each appears 100 times).
    // This is the realistic shape where a range index wins — when
    // every value is unique, the B-tree walk + per-key bitmap union
    // costs more than a flat scan. With repetition, each key's
    // bitmap is denser and there are fewer of them in any range.
    let schema = Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(schema.clone(), ROWS);
    let mut ix = SecondaryIndex::new(vec![1]); // index `v`
    let cardinality = 1000i64;
    for i in 0..ROWS as u64 {
        let key = format!("k{i:08}");
        let v = (i as i64) % cardinality;
        let row = store.append_row(&[
            Value::String(Some(CompactString::new(&key))),
            Value::Long(v),
        ]);
        ix.add(1, &Value::Long(v), row);
    }
    (schema, store, ix)
}

fn bench_range_scan(c: &mut Criterion) {
    let (schema, store, ix) = build_store();
    let mut group = c.benchmark_group("range_index_100k");
    group.sample_size(20);

    // Different selectivities to characterize the win.
    // Cardinality = 1000 distinct values (built above).
    for (label, sql) in [
        // 1% selectivity: 10 distinct keys × 100 rows each = ~1K rows
        ("between_1pct", "SELECT * FROM t WHERE v BETWEEN 100 AND 110"),
        // 10% selectivity: 100 keys × 100 rows = ~10K rows
        ("between_10pct", "SELECT * FROM t WHERE v BETWEEN 100 AND 200"),
        // 1% tail: 10 keys × 100 rows = ~1K
        ("gt_high", "SELECT * FROM t WHERE v > 989"),
    ] {
        let query = parse_query(sql, &schema).expect("parse");

        group.bench_with_input(
            BenchmarkId::new("with_index", label),
            &query,
            |b, q| {
                b.iter(|| {
                    let r = execute_query_with_index(q, &store, Some(&ix));
                    black_box(r.rows.len())
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full_scan", label),
            &query,
            |b, q| {
                b.iter(|| {
                    let r = execute_query_with_index(q, &store, None);
                    black_box(r.rows.len())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_range_scan);
criterion_main!(benches);

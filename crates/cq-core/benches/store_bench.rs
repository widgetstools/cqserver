use criterion::{black_box, criterion_group, criterion_main, Criterion};
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};
use compact_str::CompactString;
use std::sync::Arc;

fn bench_append(c: &mut Criterion) {
    let schema = Arc::new(Schema::from_strs(
        &["tradeId", "price", "quantity", "desk"],
        &[
            ColumnType::String,
            ColumnType::Double,
            ColumnType::Long,
            ColumnType::String,
        ],
    ));

    c.bench_function("append_row", |b| {
        let mut store = ColumnStore::new(schema.clone(), 1_000_000);
        let mut id = 0u64;
        b.iter(|| {
            let values = vec![
                Value::String(Some(CompactString::new(format!("T{:08}", id)))),
                Value::Double(99.5 + (id as f64 * 0.01)),
                Value::Long(1000 + id as i64),
                Value::String(Some(CompactString::new("RATES"))),
            ];
            black_box(store.append_row(&values));
            id += 1;
        });
    });
}

fn bench_scan(c: &mut Criterion) {
    let schema = Arc::new(Schema::from_strs(
        &["tradeId", "price", "quantity", "desk"],
        &[
            ColumnType::String,
            ColumnType::Double,
            ColumnType::Long,
            ColumnType::String,
        ],
    ));
    let mut store = ColumnStore::new(schema.clone(), 100_000);

    for i in 0..100_000u64 {
        store.append_row(&[
            Value::String(Some(CompactString::new(format!("T{:08}", i)))),
            Value::Double(50.0 + (i as f64 * 0.001)),
            Value::Long(i as i64),
            Value::String(Some(CompactString::new(if i % 2 == 0 { "RATES" } else { "EQUITIES" })))
        ]);
    }

    c.bench_function("scan_100k_double_gt", |b| {
        b.iter(|| {
            let mut count = 0u32;
            let row_count = store.row_count();
            for row in 0..row_count {
                if store.get_double(1, row) > 75.0 {
                    count += 1;
                }
            }
            black_box(count)
        });
    });
}

/// 1M-row full-scan snapshot, no predicate. Establishes the
/// raw-iteration ceiling for `Topic::query` (review C12.3 / S37).
fn bench_full_scan_1m_rows(c: &mut Criterion) {
    let schema = Arc::new(Schema::from_strs(
        &["k", "price", "qty"],
        &[ColumnType::String, ColumnType::Double, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(schema.clone(), 1_000_000);
    for i in 0..1_000_000u64 {
        store.append_row(&[
            Value::String(Some(CompactString::new(format!("k{:08}", i)))),
            Value::Double(i as f64 * 0.5),
            Value::Long(i as i64),
        ]);
    }
    let mut group = c.benchmark_group("snapshot_scan_1m");
    group.sample_size(10); // 1M-row scans are slow; cut samples to keep wall-clock sane
    group.bench_function("full_scan_double_gt", |b| {
        b.iter(|| {
            let mut count = 0u32;
            let row_count = store.row_count();
            for row in 0..row_count {
                if store.get_double(1, row) > 250_000.0 {
                    count += 1;
                }
            }
            black_box(count)
        });
    });
    group.finish();
}

/// Single-row insert into an already-1M-row topic (review C12.2 / S37).
/// Catches regressions in the hot-write path under realistic SOW size.
fn bench_insert_into_1m_row_topic(c: &mut Criterion) {
    let schema = Arc::new(Schema::from_strs(
        &["k", "price", "qty"],
        &[ColumnType::String, ColumnType::Double, ColumnType::Long],
    ));
    let mut store = ColumnStore::new(schema.clone(), 2_000_000);
    for i in 0..1_000_000u64 {
        store.append_row(&[
            Value::String(Some(CompactString::new(format!("k{:08}", i)))),
            Value::Double(i as f64 * 0.5),
            Value::Long(i as i64),
        ]);
    }
    let mut id = 1_000_000u64;
    let mut group = c.benchmark_group("insert_into_1m");
    group.sample_size(20);
    group.bench_function("append_after_1m", |b| {
        b.iter(|| {
            let v = vec![
                Value::String(Some(CompactString::new(format!("k{:08}", id)))),
                Value::Double(id as f64 * 0.5),
                Value::Long(id as i64),
            ];
            black_box(store.append_row(&v));
            id += 1;
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_append,
    bench_scan,
    bench_full_scan_1m_rows,
    bench_insert_into_1m_row_topic
);
criterion_main!(benches);

//! Fan-out throughput with the `PredicateIndex` (worklog S46, review
//! test C3.1). Measures `evaluate_row_kind_indexed` against
//! 100/1K/10K subscriptions whose predicates partition the
//! key-space — the dispatch must NOT degrade linearly with sub
//! count when the index can prune most subs from a given mutation.
//!
//! Setup: each sub's predicate is `WHERE k = 'k{i}'` over column
//! `k`. A sparse update touching column `k` only would still
//! affect every sub (they all reference k), so we partition by
//! splitting subs across two columns (`k` and `v`) and sending
//! changed_cols=[v] so half the subs are pruned.

use std::sync::Arc;

use compact_str::CompactString;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use cq_core::predicate::CompiledPredicate;
use cq_core::schema::{ColumnType, Schema};
use cq_core::store::{ColumnStore, Value};
use cq_core::subscription::{Subscription, SubscriptionEngine};
use cq_core::query::ParsedQuery;

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_strs(
        &["k", "v"],
        &[ColumnType::String, ColumnType::Long],
    ))
}

/// Build a subscription whose predicate references either column
/// `k` (col 0) or column `v` (col 1), chosen by `i % 2`. With
/// `changed_cols = [1]` (only `v`), half the subs prune out.
fn make_sub(i: usize, schema: &Arc<Schema>) -> Subscription {
    let predicate = if i % 2 == 0 {
        CompiledPredicate::EqString {
            col: 0,
            value: CompactString::new(format!("k{i:05}")),
        }
    } else {
        CompiledPredicate::EqLong {
            col: 1,
            value: i as i64,
        }
    };
    let query = ParsedQuery {
        topic: "t".into(),
        projection: vec![],
        predicate,
        order_by: vec![],
        limit: None,
        aggregates: vec![],
        group_by: vec![],
        pivot: None,
        unpivot: None,
    };
    let _ = schema;
    Subscription::new(format!("sub-{i:05}"), query)
}

fn populate_store(schema: &Arc<Schema>) -> ColumnStore {
    let mut store = ColumnStore::new(schema.clone(), 1024);
    for i in 0..100 {
        store.append_row(&[
            Value::String(Some(CompactString::new(format!("k{i:05}")))),
            Value::Long(i as i64),
        ]);
    }
    store
}

fn bench_fanout(c: &mut Criterion) {
    let schema = schema();
    let store = populate_store(&schema);
    let mut group = c.benchmark_group("fanout_indexed");
    group.sample_size(20);

    for &n in &[100usize, 1_000, 10_000] {
        // Build the engine with N subs.
        let mut engine = SubscriptionEngine::new();
        for i in 0..n {
            engine.add(make_sub(i, &schema));
        }

        // Two benches per N: (a) all-subs path (no changed_cols),
        // (b) indexed path (changed_cols = [1], so only v-predicate
        // subs evaluate — ~half).
        group.bench_with_input(
            BenchmarkId::new("all_subs", n),
            &n,
            |b, _| {
                b.iter(|| {
                    let deltas = engine.evaluate_row_kind(
                        black_box(0),
                        black_box(1),
                        &store,
                        cq_core::topic::MutationKind::Upsert,
                    );
                    black_box(deltas.len())
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("indexed_changed_col_v", n),
            &n,
            |b, _| {
                let changed = vec![1usize];
                b.iter(|| {
                    let deltas = engine.evaluate_row_kind_indexed(
                        black_box(0),
                        black_box(1),
                        &store,
                        cq_core::topic::MutationKind::Upsert,
                        Some(&changed),
                    );
                    black_box(deltas.len())
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);

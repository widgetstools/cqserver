//! Subscription registration on a 100K-row snapshot (review C12.4-adj
//! / worklog S37). Measures the cost of:
//!
//!   topic.subscribe(...)
//!     → parse_query
//!     → execute_query (snapshot scan)
//!     → engine.add + seed_active_set
//!
//! against a topic pre-populated with 100K rows. This is the
//! dominant cost a fresh subscriber observes; regressions here
//! manifest as user-visible lag at logon.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use cq_core::schema::{ColumnType, Schema};
use cq_core::topic::{Topic, TopicConfig};
use serde_json::{json, Map};

const ROWS: usize = 100_000;

fn prep_topic() -> Topic {
    let schema = Arc::new(Schema::from_strs(
        &["k", "v", "desk"],
        &[ColumnType::String, ColumnType::Long, ColumnType::String],
    ));
    let topic = Topic::new(
        TopicConfig {
            name: "/bench-subscribe".into(),
            key_fields: vec!["k".into()],
            persist: false,
            conflation_ms: None,
            index_columns: vec![],
            expire_seconds: None,
        },
        schema,
        ROWS + 1024,
    );
    for i in 0..ROWS {
        let desk = if i % 3 == 0 { "RATES" } else if i % 3 == 1 { "EQUITIES" } else { "FX" };
        let mut m = Map::new();
        m.insert("k".into(), json!(format!("k{i:06}")));
        m.insert("v".into(), json!(i as i64));
        m.insert("desk".into(), json!(desk));
        topic.upsert_map(&m).expect("upsert");
    }
    topic
}

fn bench_subscribe_register_select_all(c: &mut Criterion) {
    let topic = prep_topic();
    let mut counter = 0u64;
    c.bench_function("subscribe_register_100k_select_all", |b| {
        b.iter(|| {
            counter += 1;
            let sub_id = format!("bench-{counter}");
            let (rows, _q) = topic
                .subscribe(sub_id.clone(), "SELECT * FROM t")
                .expect("subscribe");
            let n = rows.len();
            topic.unsubscribe(&sub_id);
            black_box(n)
        });
    });
}

fn bench_subscribe_register_with_filter(c: &mut Criterion) {
    let topic = prep_topic();
    let mut counter = 0u64;
    c.bench_function("subscribe_register_100k_desk_filter", |b| {
        b.iter(|| {
            counter += 1;
            let sub_id = format!("bench-flt-{counter}");
            let (rows, _q) = topic
                .subscribe(sub_id.clone(), "SELECT * FROM t WHERE desk = 'RATES'")
                .expect("subscribe");
            let n = rows.len();
            topic.unsubscribe(&sub_id);
            black_box(n)
        });
    });
}

criterion_group!(
    benches,
    bench_subscribe_register_select_all,
    bench_subscribe_register_with_filter
);
criterion_main!(benches);

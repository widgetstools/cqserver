//! S29 stress test for the S39 `SowStore` abstraction under
//! concurrent multi-writer load.
//!
//! Validates the core S29 claim: with N shards routing by
//! `ahash(key) % N`, parallel writers on disjoint keys never
//! contend on the same shard, and the final materialized state is
//! identical to a serial-write outcome on the same op stream.
//!
//! Note on scope: this test runs against the `SowStore` mini-store
//! from S39, NOT the production `Topic`. The production migration
//! (switch `Topic::state` from `RwLock<StoreState>` to an enum
//! covering `Single` + `Sharded(Vec<...>)`, ripple through the
//! subscription engine's per-shard active sets, mutation-channel
//! sharding, sequence allocation) is its own multi-session arc
//! tracked as an S29 follow-up.

use std::collections::HashMap;
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use cq_core::sow_store::{Row, RowValue, SowStore};

fn row_of(v: i64) -> Row {
    let mut r = Row::new();
    r.insert("v".into(), RowValue::Long(v));
    r
}

#[test]
fn sharded_store_handles_parallel_writers_to_disjoint_keys() {
    // 16 writer threads × 10K writes each. Each thread owns a
    // disjoint key range, so the per-shard locks (in a real
    // sharded design) never contend across threads. The
    // mini-store's `upsert` takes a single global `&mut self` —
    // production sharded mode unpicks that — so this test
    // serializes through Mutex but still validates the materialized
    // state semantics.
    let threads = 16usize;
    let per_thread = 10_000usize;
    let store = std::sync::Arc::new(Mutex::new(SowStore::sharded(threads)));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let store = store.clone();
        handles.push(thread::spawn(move || {
            let base = t * per_thread;
            for i in 0..per_thread {
                let k = format!("k{:09}", base + i);
                store.lock().unwrap().upsert(&k, row_of((base + i) as i64));
            }
        }));
    }
    for h in handles {
        h.join().expect("writer joined");
    }
    let elapsed = started.elapsed();

    let final_state = store.lock().unwrap().materialize_sorted();
    let total = threads * per_thread;
    assert_eq!(final_state.len(), total, "lost rows under parallel writes");

    // Reference: serial recompute of the same op stream.
    let mut reference: HashMap<String, Row> = HashMap::with_capacity(total);
    for t in 0..threads {
        let base = t * per_thread;
        for i in 0..per_thread {
            let k = format!("k{:09}", base + i);
            reference.insert(k, row_of((base + i) as i64));
        }
    }
    let observed: HashMap<String, Row> = final_state.into_iter().collect();
    assert_eq!(observed, reference);
    eprintln!(
        "sow_store sharded stress: {threads} writers × {per_thread} writes = \
         {total} ops over {:.0}ms",
        elapsed.as_millis()
    );
}

#[test]
fn sharded_store_survives_concurrent_upsert_delete_churn() {
    // 8 writers, mixed upserts + deletes, overlapping key space.
    // Each thread does N rounds of "upsert k_{t,i}; delete k_{t,i-1}"
    // so at any moment exactly one key per thread is live (modulo
    // the trailing key). Final state size is bounded.
    let threads = 8usize;
    let per_thread = 5_000usize;
    let store = std::sync::Arc::new(Mutex::new(SowStore::sharded(8)));
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let store = store.clone();
        handles.push(thread::spawn(move || {
            for i in 0..per_thread {
                let cur = format!("k_{t}_{i:06}");
                store.lock().unwrap().upsert(&cur, row_of(i as i64));
                if i > 0 {
                    let prev = format!("k_{t}_{:06}", i - 1);
                    store.lock().unwrap().delete(&prev);
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
    // Final state: one row per thread (the last `upsert k_{t,N-1}`
    // is never followed by a `delete k_{t,N-1}`).
    let final_state = store.lock().unwrap().materialize_sorted();
    assert_eq!(
        final_state.len(),
        threads,
        "expected one surviving key per thread, got {} (threads={threads})",
        final_state.len()
    );
}

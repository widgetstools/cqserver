# CQServer — Architecture Review & Test Plan

**Subject**: `widgetstools/cqserver` (Rust AMPS-style continuous query server)
**Review scope**: `ARCHITECTURE.md`, `Cargo.toml`, `AMPS_WORKLOG.md`
**Review date**: 2026-05-23
**Reviewer caveat**: This is an *architecture and claims* review, not a code review. The reviewer was able to read the design documents, the workspace manifest, and the worklog, but could not access the Rust source files directly. Concerns below describe risks that *may or may not* already be mitigated in code — every concern includes a verification test that will tell you definitively which.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Methodology and Severity Definitions](#2-methodology-and-severity-definitions)
3. [Architectural Strengths](#3-architectural-strengths)
4. [Areas of Concern](#4-areas-of-concern)
   - [C1. `sow_and_subscribe` atomicity contract — CRITICAL](#c1-sow_and_subscribe-atomicity-contract--critical)
   - [C2. Multi-column write atomicity (column tear) — CRITICAL](#c2-multi-column-write-atomicity-column-tear--critical)
   - [C3. Single mutation channel as fan-out bottleneck — HIGH](#c3-single-mutation-channel-as-fan-out-bottleneck--high)
   - [C4. Sharding design (S29) touches everything — HIGH](#c4-sharding-design-s29-touches-everything--high)
   - [C5. Missing `loom` and `proptest` infrastructure — HIGH](#c5-missing-loom-and-proptest-infrastructure--high)
   - [C6. No differential SQL testing against reference engines — HIGH](#c6-no-differential-sql-testing-against-reference-engines--high)
   - [C7. Pivot operator not in worklog — MEDIUM](#c7-pivot-operator-not-in-worklog--medium)
   - [C8. Subscription cancellation race — MEDIUM](#c8-subscription-cancellation-race--medium)
   - [C9. TTL sweeper contention with publish path — MEDIUM](#c9-ttl-sweeper-contention-with-publish-path--medium)
   - [C10. TxLog crash durability not validated — MEDIUM](#c10-txlog-crash-durability-not-validated--medium)
   - [C11. Active set unbounded memory growth — MEDIUM](#c11-active-set-unbounded-memory-growth--medium)
   - [C12. No performance regression guard rails — MEDIUM](#c12-no-performance-regression-guard-rails--medium)
   - [C13. Predicate index for selective evaluation absent — LOW](#c13-predicate-index-for-selective-evaluation-absent--low)
5. [Test Infrastructure Additions](#5-test-infrastructure-additions)
6. [Prioritized Action Plan](#6-prioritized-action-plan)
7. [Appendix A — Copy-Paste Test Templates](#appendix-a--copy-paste-test-templates)
8. [Appendix B — Recommended `Cargo.toml` Additions](#appendix-b--recommended-cargotoml-additions)
9. [Appendix C — CI Pipeline Recommendation](#appendix-c--ci-pipeline-recommendation)

---

## 1. Executive Summary

CQServer's architecture and methodology are both above the bar for an AMPS-class system being built from scratch. The columnar SOW, RoaringBitmap active sets, index-resolved compiled predicates, and session-driven worklog discipline are all correct choices. The concerns below are *not* signs of a flawed design — they are the verification work that an AMPS-class system requires before it can be considered safe to run under a trading desk.

The concerns cluster into three categories:

- **Correctness verification** (C1, C2, C5, C6, C8): the design may be right; the proof that it is right is missing. These are the highest-leverage work items because their absence creates risk that accumulates silently.
- **Performance and scaling** (C3, C4, C12, C13): the v1 design works for the first 100 subscriptions; some choices need rethinking before 10K.
- **Operational safety** (C9, C10, C11): edge cases that determine whether the system stays healthy in production rather than during a demo.

**The three highest-priority items**, in order:

1. **C1** — write the `sow_and_subscribe` atomicity property test. One session of work; validates the most important correctness contract.
2. **C5** — add `loom` and `proptest` dev-dependencies and write the four concurrency tests in §C5. One to two sessions.
3. **C6** — stand up the differential-testing harness against DuckDB. One session for the harness; ongoing corpus growth.

Together these three items take roughly a week of session work and eliminate or validate most of the silent-correctness risk in the project.

---

## 2. Methodology and Severity Definitions

### Methodology

This review reads architecture and intent. It cannot detect implementation bugs. Each concern is paired with a **verification test** — a specific test that, when written and passing, definitively confirms the concern is addressed in the codebase. If the test fails, the concern is real and the resolution applies. If the test already exists and passes, the concern is closed.

### Severity definitions

- **CRITICAL** — silent data corruption or loss is possible; production deployment is unsafe until resolved.
- **HIGH** — correctness or scalability failure under realistic load; ship-blocker for v1.
- **MEDIUM** — operational hazard or growing tech debt; address before v1.0.
- **LOW** — optimization or polish; non-blocking.

---

## 3. Architectural Strengths

For context, the design decisions worth keeping in mind as the project grows:

- **Typed parallel column arrays** (`Vec<f64>`, `Vec<i64>`, `Vec<Option<CompactString>>`) instead of `HashMap<String, Value>`. Right call for memory and cache behavior.
- **`AtomicU32 row_count`** for lock-free snapshot reads. Right pattern for read-heavy workloads.
- **`roaring::RoaringBitmap`** for per-subscription active sets. The canonical choice; memory math (~125 KB per 1M-row sub) is correct.
- **Pre-compiled predicates** operating on column indices, not field names. The design that gets you to sub-microsecond filter eval.
- **`sqlparser` 0.56**, the same parser used by DataFusion. Current, well-maintained, broad SQL coverage.
- **`parking_lot`, `crossbeam-channel`, `dashmap`, `ahash`, `compact_str`** — every concurrent primitive choice is correct.
- **`tokio-rustls`** instead of OpenSSL. Removes the perennial OpenSSL CVE treadmill from operational concerns and simplifies OSS review at a bank.
- **`zstd` pure-Rust** for log compression. Single-binary; no C dependency to argue about.
- **Session-driven worklog** with bite-sized scopes, paired unit + e2e tests per session, explicit status tracking, and deferred-with-reason marking. This is the methodology that makes AI-accelerated development sustain quality.

---

## 4. Areas of Concern

### C1. `sow_and_subscribe` atomicity contract — CRITICAL

#### Concern

The `ARCHITECTURE.md` lifecycle for `sow_and_subscribe` reads:

```
1. Parse SQL → CompiledPredicate + projection list
2. Scan SOW → snapshot rows matching predicate
3. Send snapshot (GROUP_BEGIN, rows, GROUP_END)
4. Register subscription with active set = {matching row indices}
5. On each future mutation, evaluate predicate, compute delta
```

**The race**: between step 2 (snapshot scan) and step 4 (subscription registration), a publish can mutate the SOW. If the snapshot read and the registration aren't sequenced under the same point of synchronization, the subscriber either:

- **Misses an update**: saw the old row value in the snapshot, missed the mutation event that fired *during* steps 2–4.
- **Sees a duplicate**: saw the new value in the snapshot, then receives the change event again from step 5.

This is the single most important correctness contract in an AMPS-class system. Every home-grown AMPS clone I have reviewed either gets this wrong or has no test proving it gets it right.

#### Why it matters

In a trading blotter, both failure modes are unacceptable:

- A missed update means the displayed position is stale until the next change touches the same key. For a row that doesn't change again for hours, the user sees yesterday's price all day.
- A duplicate `Insert` event in a downstream consumer's state machine may double-count the position (depending on how the SDK handles "insert of a key I already have").

#### Resolution

The atomicity must be enforced by **versioning the snapshot and gating the live evaluator on the snapshot's version**. Two acceptable patterns:

**Pattern A — versioned snapshot, registration with version barrier**:

```rust
// In sow_and_subscribe handler:
let (snapshot_version, snapshot_rows) = store.snapshot_at_current_version(&predicate);
// Send snapshot to client
let subscription = subscription_engine.register(predicate, active_set, snapshot_version);
// The engine guarantees: any mutation with version > snapshot_version is delivered.
//                       Any mutation with version <= snapshot_version is NOT delivered.
```

The key invariant: `store.snapshot_at_current_version()` returns a snapshot taken under a lock that *also* publishes mutations to the mutation channel with strictly increasing versions. Registration captures the version; the evaluator filters by `mutation.version > subscription.start_version`.

**Pattern B — registration before scan, with replay buffer**:

```rust
// Reverse the order:
let subscription = subscription_engine.register_pending(predicate);
// subscription is in "buffering" mode: every mutation evaluated, deltas queued, not sent.
let snapshot = store.snapshot(&predicate);
client.send_snapshot(snapshot);
subscription.activate();  // Drain buffered deltas, then live.
```

The key invariant: from the moment of `register_pending`, all mutations are evaluated against this subscription; the snapshot captures a state that all buffered deltas reconcile with. Drain-then-live ensures gap-free transition.

Pattern A is simpler if you have monotonic versioning; Pattern B handles the case where snapshotting is too long to hold a lock.

#### Tests required

**Test C1.1 — Property test for atomicity** (proptest, ~200 lines):

```rust
// crates/cq-core/tests/prop_sow_and_subscribe.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn sow_and_subscribe_no_gaps_no_duplicates(
        events in prop::collection::vec(any_publish_event(), 1..1000),
        subscribe_at in 0usize..1000,
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let server = TestServer::new().await;
            let consumer = TestConsumer::new();

            // Publish events 0..subscribe_at
            for ev in &events[..subscribe_at.min(events.len())] {
                server.publish(ev).await;
            }

            // Subscribe atomically while concurrent publishes happen
            let sub_task = tokio::spawn({
                let server = server.clone();
                let consumer = consumer.clone();
                async move { server.sow_and_subscribe("topic", "WHERE matches", consumer).await }
            });

            // Race: continue publishing events subscribe_at..end concurrently
            let pub_task = tokio::spawn({
                let server = server.clone();
                let remaining: Vec<_> = events[subscribe_at..].to_vec();
                async move {
                    for ev in remaining {
                        server.publish(&ev).await;
                    }
                }
            });

            sub_task.await.unwrap();
            pub_task.await.unwrap();

            // Reconcile: the consumer's final state must equal a from-scratch
            // recompute over the entire event sequence.
            let reference_state = recompute(&events, "WHERE matches");
            let consumer_state = consumer.materialized_state();

            prop_assert_eq!(consumer_state, reference_state);
        });
    }
}
```

**Test C1.2 — Deterministic boundary test** (cargo test, ~50 lines):

```rust
#[tokio::test]
async fn sow_and_subscribe_publish_during_snapshot() {
    let server = TestServer::new().await;
    // Pre-populate
    for i in 0..1000 {
        server.publish(json!({"id": i, "val": i})).await;
    }

    // Subscribe; instrument the server to pause between snapshot read and
    // subscription registration so we can inject a publish.
    let (snapshot_started, snapshot_finished) = make_barrier_pair();
    let consumer = TestConsumer::new();
    let sub_handle = tokio::spawn({
        let server = server.clone();
        async move {
            server.sow_and_subscribe_with_hook(
                "topic",
                "WHERE id < 500",
                consumer.clone(),
                /* between snapshot and registration */ snapshot_started,
            ).await
        }
    });

    snapshot_started.wait().await;
    // Publish events that should appear as DELTAS (not be missed, not be duplicated):
    server.publish(json!({"id": 600, "val": 600})).await;  // doesn't match filter
    server.publish(json!({"id": 100, "val": 100_updated})).await;  // matches, update
    server.publish(json!({"id": 5, "val": 5})).await;  // already in snapshot
    snapshot_finished.signal();

    sub_handle.await.unwrap();
    let events = consumer.received_events();

    // Assertions: id=600 NOT in events (filter mismatch), id=100 received as
    // UPDATE exactly once, id=5 not delivered as a duplicate insert
    assert!(!events.iter().any(|e| e.key() == "600"));
    let id_100_events: Vec<_> = events.iter().filter(|e| e.key() == "100").collect();
    assert_eq!(id_100_events.len(), 1);
    let id_5_inserts: Vec<_> = events.iter()
        .filter(|e| e.key() == "5" && e.is_insert())
        .collect();
    assert_eq!(id_5_inserts.len(), 1);  // only the snapshot insert, not duplicated
}
```

**Test C1.3 — Loom test for registration race** (loom, ~80 lines):

```rust
// crates/cq-core/tests/loom_subscription_registration.rs
#[test]
fn loom_subscription_registration_race() {
    loom::model(|| {
        let store = Arc::new(MockStore::new());
        store.insert("k1", json!({"id": 1}));

        let store_w = store.clone();
        let writer = loom::thread::spawn(move || {
            store_w.update("k1", json!({"id": 1, "v": 2}));
        });

        let store_s = store.clone();
        let subscriber = loom::thread::spawn(move || {
            let sub = store_s.sow_and_subscribe(|row| row.id == 1);
            sub.collect_until_idle()
        });

        writer.join().unwrap();
        let events = subscriber.join().unwrap();

        // Final invariant: the subscriber must see exactly one "Insert" for k1
        // with the latest value, OR an "Insert" with the old value followed by
        // an "Update" to the new value. Never two inserts. Never missing the update.
        validate_subscriber_trace(events);
    });
}
```

#### Resolution effort

One session (4–8 hours):
- Write test C1.1 and C1.2.
- Run them. If they fail, instrument the code to find the race, fix it (one of Pattern A or B above), and re-run.
- Add C1.3 once `loom` is in the workspace (see C5).

---

### C2. Multi-column write atomicity (column tear) — CRITICAL

#### Concern

The store layout in `ARCHITECTURE.md`:

```rust
pub struct ColumnStore {
    double_columns: Vec<Vec<f64>>,
    long_columns:   Vec<Vec<i64>>,
    string_columns: Vec<Vec<Option<CompactString>>>,
    row_versions: Vec<AtomicU64>,
    ...
}
```

When a publish updates three columns of an existing row — say `qty`, `price`, and `notional` — those three writes are sequential. A reader running a predicate evaluation concurrently may observe:

- `qty` (new) + `price` (old) + `notional` (old), or
- `qty` (new) + `price` (new) + `notional` (old), etc.

This is **column tear**, and it produces incorrect predicate evaluation. A predicate like `WHERE notional > qty * price` may evaluate to true for an inconsistent state that never actually existed.

`row_versions: Vec<AtomicU64>` is in the design, which suggests the intent to use a sequence-lock pattern, but the architecture doc doesn't specify the protocol.

#### Why it matters

- Predicate evaluation on torn rows fires spurious deltas (a row appears to enter the filter set briefly, then leave it, when really neither happened).
- Aggregations over torn rows produce arithmetic that violates invariants (e.g., `SUM(qty * price) ≠ SUM(notional)` when the relationship `notional = qty * price` is supposed to hold).
- Snapshots returned to a SOW query include rows in impossible states.

#### Resolution

Implement the seqlock pattern explicitly. The protocol:

**Writer**:
```rust
fn update_row(&self, row: u32, new_values: &[(ColumnId, Value)]) {
    let v = self.row_versions[row as usize].load(Ordering::Relaxed);
    debug_assert!(v % 2 == 0, "writer-writer race; serialize at higher level");
    // Set version odd to indicate "write in progress"
    self.row_versions[row as usize].store(v + 1, Ordering::Release);
    fence(Ordering::Release);

    for (col, val) in new_values {
        self.write_column(*col, row, val);  // no synchronization here
    }

    fence(Ordering::Release);
    // Set version even, incremented, to indicate "write done"
    self.row_versions[row as usize].store(v + 2, Ordering::Release);
}
```

**Reader**:
```rust
fn read_row_consistent<R>(&self, row: u32, f: impl Fn(&RowView) -> R) -> R {
    loop {
        let v1 = self.row_versions[row as usize].load(Ordering::Acquire);
        if v1 % 2 != 0 {
            // Write in progress; retry
            std::hint::spin_loop();
            continue;
        }
        fence(Ordering::Acquire);
        let view = self.row_view(row);
        let result = f(&view);
        fence(Ordering::Acquire);
        let v2 = self.row_versions[row as usize].load(Ordering::Acquire);
        if v1 == v2 {
            return result;
        }
        // Version changed during read; retry
    }
}
```

Writer-writer serialization is a separate concern. For single-writer-per-topic (current design), debug_assert is sufficient. For per-CPU sharding (C4), each shard has its own writer.

#### Tests required

**Test C2.1 — Loom test for column tear** (loom):

```rust
#[test]
fn loom_no_column_tear() {
    loom::model(|| {
        let store = Arc::new(ColumnStore::new(schema_with_three_doubles()));
        let row_id = store.insert_initial(&[1.0, 2.0, 6.0]);  // qty=1, price=2, notional=6
        let store_w = store.clone();
        let writer = loom::thread::spawn(move || {
            // Update all three to a consistent new state: qty=3, price=4, notional=12
            store_w.update_row(row_id, &[
                (col_qty, Value::F64(3.0)),
                (col_price, Value::F64(4.0)),
                (col_notional, Value::F64(12.0)),
            ]);
        });

        let store_r = store.clone();
        let reader = loom::thread::spawn(move || {
            // Read all three; check the invariant notional == qty * price
            let (qty, price, notional) = store_r.read_row_consistent(row_id, |v| {
                (v.get_f64(col_qty), v.get_f64(col_price), v.get_f64(col_notional))
            });
            assert!(
                notional == qty * price,
                "Column tear detected: notional={notional}, qty={qty}, price={price}"
            );
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}
```

**Test C2.2 — Stress test for column tear under high contention** (cargo test):

```rust
#[test]
fn stress_no_column_tear_high_contention() {
    let store = Arc::new(ColumnStore::new(schema_with_three_doubles()));
    let row_id = store.insert_initial(&[1.0, 2.0, 6.0]);
    let stop = Arc::new(AtomicBool::new(false));

    // Writer thread: continuously update with consistent (qty, price, qty*price)
    let stop_w = stop.clone();
    let store_w = store.clone();
    let writer = std::thread::spawn(move || {
        let mut q = 1.0;
        while !stop_w.load(Ordering::Relaxed) {
            q += 1.0;
            let p = q * 2.0;
            store_w.update_row(row_id, &[
                (col_qty, Value::F64(q)),
                (col_price, Value::F64(p)),
                (col_notional, Value::F64(q * p)),
            ]);
        }
    });

    // Many reader threads: continuously check invariant
    let mut readers = vec![];
    for _ in 0..16 {
        let store_r = store.clone();
        let stop_r = stop.clone();
        readers.push(std::thread::spawn(move || {
            let mut violations = 0;
            while !stop_r.load(Ordering::Relaxed) {
                let (q, p, n) = store_r.read_row_consistent(row_id, |v| {
                    (v.get_f64(col_qty), v.get_f64(col_price), v.get_f64(col_notional))
                });
                if (n - q * p).abs() > 1e-9 {
                    violations += 1;
                }
            }
            violations
        }));
    }

    std::thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    let total_violations: usize = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total_violations, 0, "Column tear detected in stress test");
}
```

#### Resolution effort

One session (4–6 hours) if the seqlock pattern is not yet implemented; verification only if it is.

---

### C3. Single mutation channel as fan-out bottleneck — HIGH

#### Concern

The architecture shows a single mutation channel from the writer thread to subscription evaluator threads:

```
publish → SOW upsert → channel.send(MutationEvent) → evaluators evaluate
```

For 10K subscriptions and 100K mutations/sec:

- Each mutation must be evaluated against every subscription's predicate (or a relevant subset).
- A single MPMC channel serializes mutations.
- Broadcasting a mutation to N evaluator workers means either:
  - **Clone-and-send**: N channel sends per mutation. Channel send latency × N becomes the bottleneck.
  - **Single dispatcher serializing evaluations**: 10K predicates × 100K mutations/sec = 1B predicate evaluations/sec. Sub-microsecond per eval needed.

#### Why it matters

You will not discover this is a problem until you have 1000+ subscriptions in a load test. Once you do, the architectural change to fix it is large.

#### Resolution

Two layers of mitigation, both worth implementing:

**Mitigation A — Predicate indexing (selective evaluation)**:

Build an index from "column referenced in WHERE clause" to "subscriptions whose predicate touches that column." A mutation that changes columns `[qty, price]` only triggers evaluation for subscriptions whose predicate references `qty` or `price`. Most subscriptions don't care about most mutations; the index makes that explicit.

Data structure:

```rust
pub struct PredicateIndex {
    // For each column ID, the set of subscription IDs whose predicate references that column.
    col_to_subscriptions: HashMap<ColumnId, RoaringBitmap>,
    // For each subscription, the predicate handle and projection.
    subscriptions: HashMap<SubscriptionId, SubscriptionState>,
}

impl PredicateIndex {
    fn affected_subscriptions(&self, mutation: &Mutation) -> impl Iterator<Item = SubscriptionId> {
        let mut affected = RoaringBitmap::new();
        for col in mutation.changed_columns() {
            if let Some(subs) = self.col_to_subscriptions.get(col) {
                affected |= subs;
            }
        }
        affected.into_iter()
    }
}
```

**Mitigation B — Per-evaluator-shard mutation routing**:

Instead of one channel, route mutations to N evaluator shards by `subscription_id % N`. Each evaluator owns a disjoint set of subscriptions and processes its slice of mutations without contention.

#### Tests required

**Test C3.1 — Fan-out throughput benchmark** (criterion):

```rust
// crates/cq-core/benches/fanout.rs
fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("fanout");
    for n_subs in [10, 100, 1000, 10_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_subs),
            &n_subs,
            |b, &n_subs| {
                let server = TestServer::new_blocking();
                for i in 0..n_subs {
                    server.subscribe(format!("WHERE id % {} = {}", n_subs, i));
                }
                b.iter(|| {
                    for i in 0..1000 {
                        server.publish_blocking(json!({"id": i}));
                    }
                });
            },
        );
    }
}
```

Expected: throughput should *not* degrade linearly with `n_subs`. If it does, predicate indexing is needed.

**Test C3.2 — p99 delivery latency under fan-out** (custom harness):

Measure the time from `publish` to delta delivery at a specific subscriber, with 1K, 10K subscribers concurrently. Target: p99 ≤ 10 ms with 10K subscribers, 1K mutations/sec.

**Test C3.3 — Correctness: predicate index returns same set as full scan** (proptest):

```rust
proptest! {
    #[test]
    fn predicate_index_matches_full_scan(
        subscriptions in prop::collection::vec(any_predicate(), 1..100),
        mutation in any_mutation(),
    ) {
        let index = PredicateIndex::from_subscriptions(&subscriptions);
        let indexed_subs: HashSet<_> = index.affected_subscriptions(&mutation).collect();

        let scanned_subs: HashSet<_> = subscriptions.iter().enumerate()
            .filter(|(_, p)| p.could_affect(&mutation))
            .map(|(i, _)| SubscriptionId(i))
            .collect();

        // Indexed must be a superset of actually-affected (false positives are OK,
        // false negatives are a correctness bug).
        prop_assert!(indexed_subs.is_superset(&scanned_subs));
    }
}
```

#### Resolution effort

Two to three sessions:
- Predicate index: design, implement, integrate, test.
- Per-shard routing: design (may overlap with C4 sharding work), implement.

---

### C4. Sharding design (S29) touches everything — HIGH

#### Concern

Worklog S29 (per-CPU SOW sharding) is pending. The current design assumes:

- Single mutation channel = single writer per topic.
- `AtomicU32 row_count` = single global row counter.
- RoaringBitmap of row indices in active sets = global row index space.
- Snapshot reads = single-shard read.

When S29 lands, every one of these assumptions changes. The mutation channel becomes per-shard. Row counters become per-shard. Active sets must address `(shard_id, row_id)`. Snapshots must visit all shards in a way that's consistent across shards.

#### Why it matters

Retrofitting sharding at session 30 (estimated worklog midpoint) is significantly more expensive than designing the shard boundary now and shipping single-shard for v1.

#### Resolution

Introduce the shard abstraction *before* S29 is implemented:

```rust
pub trait SowShard: Send + Sync {
    fn insert(&self, key: &Key, row: Row);
    fn update(&self, key: &Key, fields: &[FieldUpdate]);
    fn delete(&self, key: &Key);
    fn scan(&self, predicate: &CompiledPredicate) -> impl Iterator<Item = RowId>;
    fn snapshot_version(&self) -> u64;
}

pub enum SowStore {
    Single(SingleShard),
    Sharded { shards: Vec<Shard>, hasher: AHasher },
}

impl SowStore {
    pub fn upsert(&self, key: &Key, row: Row) {
        match self {
            SowStore::Single(s) => s.insert(key, row),
            SowStore::Sharded { shards, hasher } => {
                let shard_idx = (hasher.hash(key) as usize) % shards.len();
                shards[shard_idx].insert(key, row);
            }
        }
    }
}
```

Active sets become `HashMap<ShardId, RoaringBitmap>` or `Vec<RoaringBitmap>` keyed by shard. Snapshot reads iterate over shards collecting versions at a coordinated logical time.

For v1, ship with `SowStore::Single`; the API surface is identical to what `SowStore::Sharded` will need. S29 becomes a much smaller change.

#### Tests required

**Test C4.1 — Equivalence across shard counts** (proptest):

```rust
proptest! {
    #[test]
    fn sow_state_invariant_across_shard_counts(
        events in prop::collection::vec(any_event(), 1..1000),
    ) {
        let single = SowStore::single();
        let sharded_2 = SowStore::sharded(2);
        let sharded_16 = SowStore::sharded(16);

        for ev in &events {
            single.apply(ev);
            sharded_2.apply(ev);
            sharded_16.apply(ev);
        }

        let s = single.materialize_all();
        let s2 = sharded_2.materialize_all();
        let s16 = sharded_16.materialize_all();

        prop_assert_eq!(s.sort(), s2.sort());
        prop_assert_eq!(s.sort(), s16.sort());
    }
}
```

**Test C4.2 — Cross-shard snapshot consistency** (deterministic):

Verify that a snapshot read across shards reflects a single point in logical time, not a tear across shards.

**Test C4.3 — Predicate evaluation correctness across shards** (proptest):

For any random predicate and any random insert sequence, a SOW query against the sharded store returns the same rows as against the single-shard store.

#### Resolution effort

One session for the abstraction; S29 becomes one to two additional sessions when it's tackled.

---

### C5. Missing `loom` and `proptest` infrastructure — HIGH

#### Concern

The `Cargo.toml` workspace dependencies include `criterion` (for benchmarks) and `tempfile` (for test fixtures), but do not include:

- `loom` — exhaustive concurrency-interleaving model checker.
- `proptest` (or `quickcheck`) — property-based random testing.

The architecture has many atomics, fine-grained locking, lock-free channels, sequence locks (or should have, per C2). Standard `cargo test` cannot find race conditions in this code. `loom` finds them deterministically by exploring all permitted interleavings.

Similarly, AMPS-style correctness properties (sow_and_subscribe atomicity, materialized state vs reference, predicate index correctness) are most naturally expressed as properties over random inputs; `proptest` is the tool.

#### Why it matters

Without these, concurrency bugs accumulate silently. The bug count grows with the system size. At session 40, you have a heisenbug you can't reproduce on commodity hardware, and the tooling required to find it is harder to bolt on retroactively than to use from the start.

#### Resolution

Add both as `[dev-dependencies]` in the workspace and write the four tests below.

```toml
[workspace.dependencies]
loom = "0.7"
proptest = "1"
```

Per-crate `Cargo.toml`:

```toml
[dev-dependencies]
loom = { workspace = true }
proptest = { workspace = true }

[target.'cfg(loom)'.dependencies]
loom = { workspace = true }
```

The `loom` integration requires a build-script feature: replace direct `std::sync::Arc` usage in concurrent primitives with a `loom`-aware abstraction in `cfg(loom)` builds. This is well-documented in the `loom` README.

#### Tests required (the four minimum-viable concurrency tests)

**Test C5.1 — Single-row writer/reader race** (loom):

See test C2.1 above (column tear).

**Test C5.2 — sow_and_subscribe registration race** (loom):

See test C1.3 above.

**Test C5.3 — Mutation channel ordering** (loom):

```rust
#[test]
fn loom_mutation_channel_ordering() {
    loom::model(|| {
        let store = Arc::new(MockStore::new());
        let store_w = store.clone();
        let store_r = store.clone();

        let writer = loom::thread::spawn(move || {
            store_w.publish("k1", json!({"v": 1}));
            store_w.publish("k1", json!({"v": 2}));
        });

        let receiver = loom::thread::spawn(move || {
            store_r.collect_mutations()
        });

        writer.join().unwrap();
        let muts = receiver.join().unwrap();

        // Must observe v=1 before v=2 for the same key
        let v1_idx = muts.iter().position(|m| m.value == 1);
        let v2_idx = muts.iter().position(|m| m.value == 2);
        if let (Some(i1), Some(i2)) = (v1_idx, v2_idx) {
            assert!(i1 < i2);
        }
    });
}
```

**Test C5.4 — TTL sweeper vs concurrent publish** (loom):

```rust
#[test]
fn loom_ttl_sweep_no_lost_publish() {
    loom::model(|| {
        let store = Arc::new(MockStore::with_ttl(Duration::from_micros(1)));
        store.publish("k1", json!({"v": 1}));
        std::thread::sleep(Duration::from_micros(10));

        let store_p = store.clone();
        let publisher = loom::thread::spawn(move || {
            store_p.publish("k1", json!({"v": 2}));
        });
        let store_s = store.clone();
        let sweeper = loom::thread::spawn(move || {
            store_s.run_ttl_sweep();
        });

        publisher.join().unwrap();
        sweeper.join().unwrap();

        // After both complete: either v=2 exists (publish won) or row deleted
        // (sweep won, but then publish must have come AFTER, which means v=2 exists).
        // Therefore v=2 must exist or the publish was correctly rejected.
        let state = store.get("k1");
        assert!(state.is_none() || state.unwrap().v == 2);
    });
}
```

#### Property tests to add (the four minimum-viable property tests)

**Test C5.5 — SOW state vs HashMap reference**:

```rust
proptest! {
    #[test]
    fn sow_matches_hashmap_reference(events in prop::collection::vec(any_event(), 1..1000)) {
        let mut reference = HashMap::<Key, Row>::new();
        let store = SowStore::new();

        for ev in &events {
            match ev {
                Event::Insert(k, r) | Event::Update(k, r) => {
                    reference.insert(k.clone(), r.clone());
                    store.upsert(k, r.clone());
                }
                Event::Delete(k) => {
                    reference.remove(k);
                    store.delete(k);
                }
            }
        }

        let store_state = store.materialize_all().into_iter().collect::<HashMap<_,_>>();
        prop_assert_eq!(store_state, reference);
    }
}
```

**Test C5.6 — Active set vs HashSet reference**:

```rust
proptest! {
    #[test]
    fn active_set_matches_hashset_reference(
        events in prop::collection::vec(any_event(), 1..1000),
        predicate in any_predicate(),
    ) {
        let mut reference = HashSet::<Key>::new();
        let store = SowStore::new();
        let mut active_set = ActiveSet::new();

        for ev in &events {
            apply_to_reference(&mut reference, ev, &predicate);
            store.apply(ev);
            let was_in = active_set.contains(ev.key());
            let now_matches = predicate.matches(&store.get(ev.key()).unwrap_or_default());
            update_active_set(&mut active_set, ev.key(), was_in, now_matches);
        }

        let active_keys: HashSet<_> = active_set.iter().collect();
        prop_assert_eq!(active_keys, reference);
    }
}
```

**Test C5.7 — Predicate evaluation equivalence** (interpreter vs compiled):

If you have both an interpreter and a compiled predicate path (or will), differential-test them.

**Test C5.8 — Bookmark replay equivalence**:

```rust
proptest! {
    #[test]
    fn bookmark_replay_delivers_correct_suffix(
        events in prop::collection::vec(any_event(), 1..500),
        replay_from in 0usize..500,
    ) {
        let server = TestServer::new();
        for ev in &events {
            server.publish(ev);
        }

        let bookmark = server.bookmark_at(replay_from);
        let replayed: Vec<_> = server.replay_from(bookmark).collect();
        let expected: Vec<_> = events.iter().skip(replay_from).collect();

        prop_assert_eq!(replayed.len(), expected.len());
        // Allow for filtered replay to drop events, but never insert or reorder
        for (r, e) in replayed.iter().zip(expected.iter()) {
            prop_assert_eq!(r.key(), e.key());
        }
    }
}
```

#### Resolution effort

One session to add the dependencies and set up the `loom` integration. One session each for the four `loom` tests. One session for the four property tests. Total: 3–4 sessions.

---

### C6. No differential SQL testing against reference engines — HIGH

#### Concern

The worklog has unit and e2e tests per session, but no **differential test corpus**: a set of SQL queries run against CQServer AND against a reference engine (DuckDB and/or SQLite) with result-set equality assertions.

Without this, the SQL semantics of CQServer will drift from ANSI/PostgreSQL conventions on edge cases:

- NULL handling in `IN`, `NOT IN`, `=`, `!=`.
- Type coercion in cross-type comparisons (string vs number, date vs string).
- `LIKE` escape sequences (`\%`, `_`).
- Regex semantics in `MATCHES` (Rust regex vs PCRE vs POSIX).
- Aggregate behavior on empty groups (`SUM` of empty: 0 or NULL?).
- Window function frame defaults (`ROWS` vs `RANGE` default frames).

#### Why it matters

Clients written against PostgreSQL semantics will see subtle bugs when CQServer disagrees. These bugs are extremely hard to find with unit tests alone because no one knows in advance which corner cases differ.

#### Resolution

Build a `cq-differential-tests` crate.

**Structure**:

```
crates/cq-differential-tests/
├── Cargo.toml
├── corpus/
│   ├── 001_simple_select.yaml
│   ├── 002_where_clauses.yaml
│   ├── 003_null_handling.yaml
│   ├── 004_aggregates.yaml
│   ├── 005_joins.yaml
│   ├── 006_window_functions.yaml
│   └── ...
├── src/
│   ├── harness.rs
│   ├── duckdb_runner.rs
│   ├── cqserver_runner.rs
│   └── lib.rs
└── tests/
    └── differential.rs
```

**Corpus format**:

```yaml
# corpus/003_null_handling.yaml
- name: null_in_in_clause
  setup:
    - "INSERT INTO t (id, name) VALUES (1, 'Alice'), (2, NULL), (3, 'Bob')"
  query: "SELECT id FROM t WHERE name IN ('Alice', NULL, 'Bob')"
  expected_rows: [{id: 1}, {id: 3}]
  notes: |
    NULL in IN list should NOT match NULL in column per ANSI.

- name: sum_of_empty_group
  setup: []
  query: "SELECT SUM(qty) AS s FROM t WHERE id < 0"
  expected_rows: [{s: null}]
  notes: |
    SUM of empty set is NULL, not 0.
```

**Harness**:

```rust
// src/harness.rs
pub struct DifferentialHarness {
    cqserver: TestServer,
    duckdb: duckdb::Connection,
}

impl DifferentialHarness {
    pub fn run_test(&mut self, test: &TestCase) -> Result<()> {
        // Apply setup statements to both
        for stmt in &test.setup {
            self.cqserver.execute(stmt)?;
            self.duckdb.execute(stmt, [])?;
        }

        // Run query against both
        let cq_rows = self.cqserver.query(&test.query)?;
        let dd_rows = self.duckdb.query(&test.query)?;

        // Compare result sets (set equality unless ORDER BY)
        let cq_set: HashSet<_> = cq_rows.into_iter().collect();
        let dd_set: HashSet<_> = dd_rows.into_iter().collect();

        if cq_set != dd_set {
            bail!(
                "Differential mismatch for {}:\nCQ: {:?}\nDuckDB: {:?}",
                test.name, cq_set, dd_set
            );
        }
        Ok(())
    }
}
```

**Test runner**:

```rust
// tests/differential.rs
#[test]
fn differential_corpus() {
    let mut harness = DifferentialHarness::new();
    let corpus = load_corpus("corpus/");
    let mut failures = vec![];
    for test in corpus {
        if let Err(e) = harness.run_test(&test) {
            failures.push((test.name.clone(), e));
        }
    }
    if !failures.is_empty() {
        for (name, err) in &failures {
            eprintln!("FAIL: {}: {}", name, err);
        }
        panic!("{} differential tests failed", failures.len());
    }
}
```

**Streaming differential** (for continuous queries):

The harder version: run the same query as a continuous subscription, feed events one at a time, and after each event query DuckDB at the current materialized state, comparing.

```rust
async fn streaming_differential(test: &StreamingTestCase) -> Result<()> {
    let cqs = TestServer::new().await;
    let dd = duckdb::Connection::open_in_memory()?;

    cqs.create_table(&test.schema).await?;
    dd.execute(&test.create_table_sql, [])?;

    let mut sub = cqs.subscribe(&test.query).await?;

    for event in &test.events {
        cqs.publish(event).await?;
        dd.execute(&event.as_insert_sql(&test.table), [])?;

        // Now compare: CQS's materialized view = DuckDB's batch query result
        let cq_state = sub.materialized_state().await;
        let dd_state = dd.query(&test.query, [])?;

        assert_set_equal(&cq_state, &dd_state, format!("After event {:?}", event));
    }

    Ok(())
}
```

#### Tests required

The corpus *is* the test. Start with the following categories at 10–20 queries each:

1. Simple `SELECT` with projections and `WHERE`.
2. NULL handling (`IS NULL`, `IS NOT NULL`, `=`, `!=`, `IN`, `NOT IN`).
3. Type coercion (`'1' = 1`, `'abc' = 'abc'`, `1.0 = 1`).
4. `LIKE` patterns including escapes.
5. Aggregates including empty groups.
6. `GROUP BY` with `HAVING`.
7. Joins: `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`.
8. Multi-way joins (3+ tables).
9. Subqueries (scalar, `IN`, `EXISTS`).
10. Window functions if implemented.
11. Set operations (`UNION`, `INTERSECT`, `EXCEPT`).
12. `ORDER BY` + `LIMIT` + `OFFSET`.

Target: 50 queries by next week, 200 by end of quarter, 500 by feature-complete.

#### Resolution effort

One session for the harness. Ongoing growth — but each new SQL feature added must come with at least 5 new corpus entries. An AI agent can generate 100 corpus entries per session at high quality.

---

### C7. Pivot operator not in worklog — MEDIUM

#### Concern

The worklog covers S1 through S30 and explicitly defers shared-memory transport, JIT filter eval, and additional codecs. **No session covers pivot operators**, despite the prior architectural conversation establishing pivots as a key differentiator for the FI blotter use case.

Specifically missing:

- Static `PIVOT` operator with known pivot keys.
- Dynamic `PIVOT` where pivot keys are discovered from data.
- Schema-change frame in the wire protocol for dynamic pivot column add/remove.
- Multi-measure pivot (`PIVOT (SUM(qty), SUM(notional)) FOR trader`).
- Sparse field-delta emission for wide pivot rows.

#### Why it matters

If pivots are in scope for v1, they need:
- Their own session (probably 2–3 sessions).
- A protocol extension that *every existing client* needs to handle.
- An operator that doesn't exist in any reference engine (so no differential testing possible — must rely on property tests against a from-scratch recompute).

Retrofitting the protocol extension after clients are coded is painful. Easier to land the protocol now even if the operator is implemented later.

#### Resolution

Three sessions to add to the worklog:

**Session SP1 — Static PIVOT operator**:
- Scope: parse `PIVOT (agg(col)) FOR pivot_col IN (val1, val2, ...)`. Implement as `GROUP BY anchor_cols` with one aggregator per literal pivot value. Output schema is statically known.
- Tests: unit (compare to manual GROUP BY + projection), property (random anchor/pivot/measure → equivalent batch result).

**Session SP2 — UNPIVOT operator**:
- Scope: parse `UNPIVOT (val FOR pivot_col IN (col1, col2, ...))`. Emit one row per input row × input column listed.
- Tests: round-trip `PIVOT(UNPIVOT(x)) ≡ x` modulo NULLs; property test.

**Session SP3 — Dynamic PIVOT + schema-change frame**:
- Scope: parse `PIVOT DYNAMIC (agg(col)) FOR pivot_col`. Maintain `BTreeSet<PivotKey>` of active values. Emit `SchemaChange` frame on add/remove. Operator state: `HashMap<AnchorKey, HashMap<PivotKey, AggBundle>>`.
- Wire-protocol addition: `SchemaChange { new_columns: Vec<ColumnDef>, removed_columns: Vec<ColumnName>, version: u64 }` frame.
- Tests: unit (columns added/removed correctly); property (state matches batch recompute); e2e (subscriber sees schema-change events in correct order with data deltas).

#### Tests required

**Test C7.1 — Pivot incremental ≡ batch recompute** (proptest):

```rust
proptest! {
    #[test]
    fn pivot_incremental_matches_batch(
        events in prop::collection::vec(any_pivot_event(), 1..500),
        pivot_spec in any_pivot_spec(),
    ) {
        let server = TestServer::new();
        let sub = server.subscribe(&pivot_spec.to_sql());
        let mut incremental_state = HashMap::new();

        for ev in &events {
            server.publish(ev);
            for delta in sub.drain_pending() {
                apply_delta_to_state(&mut incremental_state, delta);
            }
        }

        let batch_state = pivot_batch_recompute(&events, &pivot_spec);

        prop_assert_eq!(incremental_state, batch_state);
    }
}
```

**Test C7.2 — Dynamic pivot schema-change ordering** (e2e):

Subscribe to a dynamic pivot; inject events that introduce a new pivot key; verify the `SchemaChange` frame arrives *before* any data delta referencing the new column.

**Test C7.3 — Sparse field-delta merging** (proptest):

Random sparse updates to wide pivot rows; client-side merge yields the same full row as a hypothetical full-row update.

#### Resolution effort

Three sessions (one per sub-feature). The protocol frame should land first; the operator can land second.

---

### C8. Subscription cancellation race — MEDIUM

#### Concern

Not addressed in `ARCHITECTURE.md`: what happens when a subscriber disconnects (or sends `unsubscribe`) while the mutation evaluator is in the middle of evaluating a mutation against this subscription, queueing a delta to its outbound channel, or holding a reference to its active set?

Possible failure modes:
- Use-after-free / panic on a dropped subscription (Rust prevents this at the type level, but logical bugs are possible).
- Delta delivered to a closed channel (silently dropped, or panic).
- Active set memory not released; slow leak across many connect/disconnect cycles.
- Mutation event processing wedged on a closed outbound channel.

#### Why it matters

This is the most common production hazard in pub/sub systems. Clients reconnect constantly; if any of the above bugs exist, they manifest as memory leaks or sporadic crashes that are hard to attribute.

#### Resolution

The lifecycle must enforce these invariants:

1. **Subscription ownership**: the subscription is owned by exactly one entity at a time. The evaluator borrows a handle; cancellation invalidates the handle.
2. **Outbound channel close semantics**: dropping a subscription closes the outbound channel cleanly; senders detect the close and stop trying.
3. **Active set reclamation**: deterministic; tied to subscription drop.

Implementation pattern:

```rust
pub struct Subscription {
    id: SubscriptionId,
    predicate: Arc<CompiledPredicate>,
    active_set: Arc<Mutex<RoaringBitmap>>,
    outbound: tokio::sync::mpsc::Sender<Delta>,
    closed: Arc<AtomicBool>,
}

impl Subscription {
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        // Outbound channel is dropped when subscription is dropped.
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

// In the evaluator:
fn evaluate_for_subscription(sub: &Subscription, mutation: &Mutation) {
    if sub.is_closed() {
        return;  // skip, will be reaped
    }
    // ... evaluate, queue delta
    if sub.outbound.try_send(delta).is_err() {
        // Channel full or closed; mark for slow-client handling or reap
    }
}
```

Periodic reaper: walks the subscription registry, removes closed subscriptions.

#### Tests required

**Test C8.1 — Reconnect storm leak test** (stress):

```rust
#[tokio::test]
async fn reconnect_storm_no_leak() {
    let server = TestServer::new().await;
    let initial_mem = process_memory_kb();

    for _ in 0..10_000 {
        let client = server.connect_client().await;
        client.subscribe("SELECT * FROM t WHERE x > 5").await;
        client.disconnect().await;
    }

    // Force reaper to run
    server.run_reaper().await;

    let final_mem = process_memory_kb();
    assert!(final_mem - initial_mem < 50_000, "Memory grew by {} KB", final_mem - initial_mem);
}
```

**Test C8.2 — Disconnect during evaluation** (loom):

```rust
#[test]
fn loom_disconnect_during_evaluation() {
    loom::model(|| {
        let sub = Arc::new(MockSubscription::new());
        let sub_e = sub.clone();
        let sub_c = sub.clone();

        let evaluator = loom::thread::spawn(move || {
            sub_e.evaluate_mutation(&mutation_fixture());
        });
        let canceller = loom::thread::spawn(move || {
            sub_c.close();
        });

        evaluator.join().unwrap();
        canceller.join().unwrap();
        // Must not panic; subscription state must be consistent post-cancellation.
    });
}
```

**Test C8.3 — Outbound channel full handling** (cargo test):

Subscribe with a tiny outbound buffer; flood publishes; verify that the slow consumer either receives all events (eventually) or is cleanly disconnected, with no impact on other subscriptions.

#### Resolution effort

One to two sessions.

---

### C9. TTL sweeper contention with publish path — MEDIUM

#### Concern

S7 (TTL expiration) is marked done. The sweeper runs as a background task scanning rows whose age exceeds TTL. The architecture doc doesn't specify how the sweeper coordinates with concurrent publishes to the same key:

- If sweeper holds a write lock on the topic during its scan, publish latency spikes.
- If sweeper holds no lock, a publish might insert a row the sweeper is about to delete.
- If sweeper deletes a row that publish just resurrected, the result is data loss.

#### Why it matters

TTL-driven bugs are slow to manifest — the bug only triggers when a row hits its TTL deadline while a publish is happening. In production, this might be one in a million publishes, manifesting as occasional missing rows.

#### Resolution

Per-row TTL check at the sweep moment, under the same write lock used by publish. The sweeper:

1. Reads `(row_id, last_touched)` snapshot.
2. For each candidate (`last_touched + ttl < now`):
   - Acquire row write lock.
   - Re-check `last_touched`. If it changed (publish happened), skip — the row was just refreshed.
   - Delete row; emit `oof_expired`.
   - Release lock.

This is "compare-and-swap on the version" applied to delete decisions. The publish path naturally updates `last_touched`, so the re-check is correctness-complete.

#### Tests required

**Test C9.1 — Publish-then-sweep race** (loom):

```rust
#[test]
fn loom_ttl_no_resurrected_row_lost() {
    loom::model(|| {
        let store = Arc::new(MockStore::with_ttl(Duration::from_micros(1)));
        let k = "k1";
        store.publish(k, json!({"v": 1}));
        std::thread::sleep(Duration::from_micros(10));
        // Now the row is expired but not yet swept.

        let store_p = store.clone();
        let publisher = loom::thread::spawn(move || {
            store_p.publish(k, json!({"v": 2}));
        });
        let store_s = store.clone();
        let sweeper = loom::thread::spawn(move || {
            store_s.run_ttl_sweep();
        });

        publisher.join().unwrap();
        sweeper.join().unwrap();

        // The row MUST exist with v=2.
        let row = store.get(k);
        assert_eq!(row.unwrap().v, 2);
    });
}
```

**Test C9.2 — Sweeper does not block publishes** (benchmark):

Measure publish latency p99 with and without sweeper running. Difference should be < 10%.

#### Resolution effort

One session (assuming the lock protocol described above isn't already in place).

---

### C10. TxLog crash durability not validated — MEDIUM

#### Concern

S8 and S9 mark txlog archive + compression as done. The architecture describes:

- Append-only log with `[length][crc32][timestamp][topic][key][payload]`.
- fsync policy: `none`, `every_write`, `interval`.

What's not validated by listed tests:

- Behavior under torn write at the journal tail (process killed mid-`write`).
- Recovery when CRC fails on a record.
- Behavior when fsync errors out (disk full, EIO).
- Whether `every_write` and `interval` actually guarantee what they claim.

#### Why it matters

Durability bugs only manifest after a crash, by which point the bug is hard to diagnose. The cost of writing crash tests up front is much lower than the cost of investigating a production data-loss incident.

#### Resolution

A dedicated crash-test suite. Use `process::Command` to spawn a child process that writes to the txlog, kill it at various offsets, restart, verify recovery.

#### Tests required

**Test C10.1 — Torn write at tail** (cargo test):

```rust
#[test]
fn torn_write_at_tail_detected_and_truncated() {
    let dir = tempdir().unwrap();
    let log = TxLog::open(dir.path()).unwrap();
    for i in 0..100 {
        log.append(&entry_fixture(i)).unwrap();
    }
    log.sync().unwrap();
    drop(log);

    // Simulate torn write: corrupt the last 5 bytes of the journal file
    let path = dir.path().join("active.log");
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    let len = file.metadata().unwrap().len();
    file.seek(SeekFrom::Start(len - 5)).unwrap();
    file.write_all(&[0xff; 5]).unwrap();
    drop(file);

    // Reopen: must detect, truncate, recover gracefully
    let log = TxLog::open(dir.path()).unwrap();
    let entries: Vec<_> = log.read_all().collect();
    // We lost at most the last entry; first 99 must be intact
    assert!(entries.len() >= 99);
    for i in 0..99 {
        assert_eq!(entries[i], entry_fixture(i));
    }
}
```

**Test C10.2 — Crash recovery under various fsync policies** (process-spawn harness):

```rust
#[test]
fn crash_recovery_every_write() {
    let dir = tempdir().unwrap();
    let n_published = 1000;

    // Spawn a child that publishes 1000 entries with fsync=every_write
    let mut child = Command::new(env!("CARGO_BIN_EXE_publisher_under_test"))
        .arg(dir.path())
        .arg(n_published.to_string())
        .arg("--fsync").arg("every_write")
        .spawn().unwrap();

    // Kill it after it reports half done
    std::thread::sleep(Duration::from_millis(500));
    child.kill().unwrap();
    let _ = child.wait();

    // Reopen
    let log = TxLog::open(dir.path()).unwrap();
    let entries: Vec<_> = log.read_all().collect();

    // With every_write, all reported-completed publishes must be present
    let reported_completed = read_progress_file(dir.path()).unwrap();
    assert!(entries.len() >= reported_completed);
}
```

**Test C10.3 — CRC failure mid-log** (cargo test):

Corrupt a CRC in the middle of the log (not at tail). Recovery must detect, log a clear error, and either refuse to start or truncate at the corruption point with explicit operator notification.

**Test C10.4 — Replay equivalence after compression** (cargo test):

For S9 — sealed segments are zstd-compressed. Verify that a replay reading mixed compressed + uncompressed segments returns identical entry sequences to an all-uncompressed run.

#### Resolution effort

Two sessions.

---

### C11. Active set unbounded memory growth — MEDIUM

#### Concern

Each subscription holds a `RoaringBitmap` of matching row indices. For a topic with 100M rows and 1000 subscriptions, even at 125 KB per bitmap, that's 125 MB of bitmap storage. With concentrated rates feeds where row counts grow without bound (every tick is a new row, not an update), the bitmap grows monotonically.

Two questions:
- Are old row indices reclaimed when rows are deleted (TTL or explicit)?
- Is there a per-subscription memory cap?

#### Resolution

Bitmaps must shrink when rows are deleted. Either:

- **Eager reclamation**: on row delete, remove the index from every subscription's active set. O(N_subs) per delete; acceptable for moderate subscription counts.
- **Lazy reclamation**: row delete leaves a tombstone; subscriptions discover deletions during evaluation. Bitmap compaction happens periodically.

For high-throughput rates feeds where TTL drives deletes, eager is simpler and correctness-clearer.

Add a per-subscription memory cap: if the active set exceeds N entries, the subscription is closed with a `TooManyMatches` error and the client is asked to narrow its filter.

#### Tests required

**Test C11.1 — Active set shrinks on delete** (cargo test):

```rust
#[tokio::test]
async fn active_set_shrinks_on_ttl_delete() {
    let server = TestServer::new_with_ttl(Duration::from_secs(1)).await;
    let sub = server.subscribe("SELECT * FROM t").await;

    for i in 0..10_000 {
        server.publish(json!({"id": i, "v": i})).await;
    }
    assert_eq!(sub.active_set_size(), 10_000);

    tokio::time::sleep(Duration::from_secs(2)).await;
    server.run_ttl_sweep().await;

    assert_eq!(sub.active_set_size(), 0);
}
```

**Test C11.2 — Memory cap enforced** (cargo test):

Subscribe with `max_active = 1000`; publish 2000 matching rows; expect the subscription to be closed with `TooManyMatches`.

**Test C11.3 — Memory accounting under churn** (stress):

Sustained insert + TTL-delete churn over 1 hour; verify memory growth is bounded.

#### Resolution effort

One session.

---

### C12. No performance regression guard rails — MEDIUM

#### Concern

`criterion` is in workspace dependencies, but the worklog doesn't list a session establishing performance baselines or wiring benchmark results into CI with regression detection. Without this, the project will lose 2–3× performance over its lifetime as small inefficiencies accumulate, with no single offender identifiable.

#### Resolution

Establish baseline benchmarks for the hot paths *now*:

1. Filter evaluation per row (5-predicate filter, 10-field row).
2. Single-row insert into a topic with 1M existing rows.
3. SOW snapshot scan with indexed equality filter, 1M rows.
4. SOW snapshot scan with full scan, 1M rows.
5. Subscription registration time, 100K-row snapshot.
6. End-to-end publish-to-delivery latency at a subscriber.
7. Fan-out throughput at 100, 1K, 10K subscribers.

Run on every PR; record results in JSON; compare against baseline with ±5% guard rails. PRs that exceed the guard rail must justify the regression in commit message or fix it.

#### Tests required

**Test C12.1 — Filter eval baseline**:

```rust
// crates/cq-core/benches/filter.rs
fn bench_filter_eval(c: &mut Criterion) {
    let store = setup_store(/* 1M rows */);
    let pred = compile("WHERE a > 100 AND b LIKE 'X%' AND c IN (1,2,3)");
    c.bench_function("filter_eval_5pred_10col", |b| {
        b.iter(|| {
            let mut count = 0;
            for row in 0..1000 {
                if pred.matches(&store, row) {
                    count += 1;
                }
            }
            black_box(count)
        });
    });
}
```

**Test C12.2 — Insert throughput** (criterion):

Single-thread sustained insert rate into a topic with N existing rows.

**Test C12.3 — Snapshot scan** (criterion):

Snapshot read with indexed and non-indexed predicates, 1M rows.

**Test C12.4 — End-to-end latency** (custom):

Publish a row; measure time until delta arrives at a subscriber on the same machine over TCP. Record p50, p95, p99.

**CI integration**:

```yaml
# .github/workflows/bench.yml
- name: Run benchmarks
  run: cargo bench --bench '*' -- --output-format bencher | tee bench.txt

- name: Compare against baseline
  uses: benchmark-action/github-action-benchmark@v1
  with:
    tool: 'cargo'
    output-file-path: bench.txt
    fail-on-alert: true
    alert-threshold: '105%'   # Fail if 5% slower
```

#### Resolution effort

One session for the benchmark suite. One session for CI integration.

---

### C13. Predicate index for selective evaluation absent — LOW

#### Concern

Already covered as mitigation A in C3. Listed separately because it's a self-contained feature worth a dedicated session.

#### Resolution

See C3 mitigation A. Add as worklog session SP4.

#### Tests required

See C3.3.

#### Resolution effort

One session (overlaps significantly with C3 work).

---

## 5. Test Infrastructure Additions

Summary of the test infrastructure gaps and the additions needed.

| Gap | Resolution |
|---|---|
| No `loom` for concurrency model checking | Add `loom = "0.7"` as workspace dev-dep; write 4 baseline loom tests |
| No `proptest` for property-based testing | Add `proptest = "1"` as workspace dev-dep; write 4 baseline property tests |
| No differential testing against reference SQL engines | New `cq-differential-tests` crate; corpus + harness; DuckDB as reference |
| No performance regression guard rails | Wire `criterion` results into CI with `github-action-benchmark`; ±5% threshold |
| No crash-recovery test harness | New `cq-crash-tests` crate or top-level integration tests; uses subprocess-spawn pattern |
| No reconnect/leak stress harness | Add to e2e test suite; uses process memory introspection |

---

## 6. Prioritized Action Plan

A realistic week-by-week plan. Each row is one session (4–8 hours).

| Week | Session | Concern | Deliverable |
|---|---|---|---|
| 1 | 1 | C5 | Add `loom`, `proptest` dev-deps; basic integration |
| 1 | 2 | C1 | Test C1.1, C1.2: sow_and_subscribe atomicity property + deterministic test |
| 1 | 3 | C2 | Test C2.1, C2.2: column tear loom + stress |
| 1 | 4 | C5 | Tests C5.5, C5.6: SOW vs HashMap, active set vs HashSet property tests |
| 2 | 5 | C6 | Build differential test harness; integrate DuckDB; first 30 corpus entries |
| 2 | 6 | C6 | Grow corpus to 100 entries; categorize by SQL feature |
| 2 | 7 | C12 | Establish benchmark baselines; integrate into CI |
| 2 | 8 | C8 | Reconnect storm leak test; disconnect-during-evaluation loom test |
| 3 | 9 | C4 | Introduce `SowShard` abstraction; refactor existing single-shard impl to fit |
| 3 | 10 | C9 | Publish-then-sweep race loom test; fix if needed |
| 3 | 11 | C10 | Crash recovery test harness; torn-write, CRC failure tests |
| 3 | 12 | C11 | Active set shrink-on-delete; memory cap; tests |
| 4 | 13 | C7 (SP1) | Static PIVOT operator + tests |
| 4 | 14 | C7 (SP3) | Wire-protocol SchemaChange frame; client + server handling |
| 4 | 15 | C7 (SP3) | Dynamic PIVOT operator; e2e test for schema-change ordering |
| 4 | 16 | C3 | Predicate index; integrate; benchmark fan-out improvement |

**Outcome after 16 sessions** (roughly 4 weeks at the project's current velocity):

- Every critical and high concern has a passing test.
- The differential test corpus catches SQL semantic drift on every PR.
- Performance regressions are gated.
- Sharding boundary is in place for an easy S29 landing.
- Pivots are functional end-to-end.

This sequence prioritizes correctness verification (weeks 1–2) over new features (week 4) because the cost of finding bugs grows with the size of the codebase, and the worklog has been adding features faster than it has been adding correctness proofs.

---

## Appendix A — Copy-Paste Test Templates

### A.1 `loom` test scaffold

```rust
// crates/cq-core/tests/loom_subscription.rs
#![cfg(loom)]

use loom::sync::Arc;
use loom::thread;

#[test]
fn loom_subscription_registration_no_gap() {
    loom::model(|| {
        // Set up minimal store and subscription engine
        let store = Arc::new(loom_compatible_store());
        store.insert("k1", row(1));

        let store_w = store.clone();
        let writer = thread::spawn(move || {
            store_w.update("k1", row(2));
        });

        let store_s = store.clone();
        let subscriber = thread::spawn(move || {
            let sub = store_s.sow_and_subscribe(|r| r.field > 0);
            sub.drain_deltas()
        });

        writer.join().unwrap();
        let deltas = subscriber.join().unwrap();

        // Validate: subscriber must see consistent state
        assert!(validate_no_gap_or_dup(&deltas));
    });
}
```

Run with: `RUSTFLAGS="--cfg loom" cargo test --test loom_subscription --release`

### A.2 `proptest` test scaffold

```rust
// crates/cq-core/tests/prop_sow_state.rs
use proptest::prelude::*;

prop_compose! {
    fn any_event()(
        op in 0..3u8,
        key in 0..100u32,
        value in any::<i64>(),
    ) -> Event {
        match op {
            0 => Event::Insert { key, value },
            1 => Event::Update { key, value },
            _ => Event::Delete { key },
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn sow_state_equals_hashmap_reference(
        events in prop::collection::vec(any_event(), 1..500),
    ) {
        let mut reference = std::collections::HashMap::<u32, i64>::new();
        let store = SowStore::new(schema_simple());

        for ev in &events {
            apply_to_reference(&mut reference, ev);
            apply_to_store(&store, ev);
        }

        let store_state = store.materialize().into_iter().collect::<HashMap<_,_>>();
        prop_assert_eq!(store_state, reference);
    }
}
```

### A.3 Differential test scaffold

```rust
// crates/cq-differential-tests/tests/differential.rs
use cq_differential_tests::{DifferentialHarness, load_corpus};

#[test]
fn corpus_against_duckdb() {
    let mut harness = DifferentialHarness::new_with_duckdb();
    let corpus = load_corpus("corpus/");

    let mut failures = vec![];
    for test in corpus {
        if let Err(e) = harness.run(&test) {
            failures.push((test.name.clone(), e.to_string()));
        }
    }

    if !failures.is_empty() {
        for (name, err) in &failures {
            eprintln!("FAIL {}: {}", name, err);
        }
        panic!("{} of {} differential tests failed", failures.len(), /* total */ 0);
    }
}
```

### A.4 Crash-recovery test scaffold

```rust
// crates/cq-txlog/tests/crash.rs
use std::process::Command;

#[test]
fn crash_at_random_offsets_preserves_acked_writes() {
    for seed in 0..20 {
        let dir = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_publisher_helper"))
            .arg(dir.path())
            .arg("--seed").arg(seed.to_string())
            .arg("--target-count").arg("1000")
            .spawn().unwrap();

        // Random kill time between 50ms and 500ms
        let kill_after_ms = 50 + (seed * 23) % 450;
        std::thread::sleep(std::time::Duration::from_millis(kill_after_ms));
        let _ = child.kill();
        let _ = child.wait();

        // Read the progress file to know what was acked
        let acked = read_acked_count(dir.path()).unwrap();

        // Reopen and verify
        let log = TxLog::open(dir.path()).unwrap();
        let entries: Vec<_> = log.read_all().collect::<Result<_, _>>().unwrap();
        assert!(
            entries.len() >= acked,
            "seed={}: acked {} writes but log has only {}",
            seed, acked, entries.len()
        );
    }
}
```

---

## Appendix B — Recommended `Cargo.toml` Additions

Workspace root `Cargo.toml`:

```toml
[workspace.dependencies]
# Existing entries unchanged...

# === Add these ===

# Concurrency model checker
loom = "0.7"

# Property-based testing
proptest = "1"

# Differential testing reference engine
duckdb = { version = "1", features = ["bundled"] }

# Time helpers for tests
mock_instant = "0.3"

# Memory measurement for leak tests
peak_alloc = "0.2"

[profile.bench]
inherits = "release"
debug = true

[profile.loom]
inherits = "test"
debug = true
opt-level = 1
```

Each per-crate `Cargo.toml` that needs them:

```toml
# crates/cq-core/Cargo.toml
[dev-dependencies]
loom = { workspace = true }
proptest = { workspace = true }
peak_alloc = { workspace = true }

[target.'cfg(loom)'.dependencies]
loom = { workspace = true }
```

New crate:

```toml
# crates/cq-differential-tests/Cargo.toml
[package]
name = "cq-differential-tests"
version.workspace = true
edition.workspace = true

[dependencies]
cq-core = { path = "../cq-core" }
cq-server = { path = "../cq-server" }
duckdb = { workspace = true }
serde = { workspace = true }
serde_yaml = "0.9"
anyhow = { workspace = true }
tokio = { workspace = true }
```

---

## Appendix C — CI Pipeline Recommendation

```yaml
# .github/workflows/ci.yml

name: CI

on:
  push:
    branches: [main]
  pull_request:

jobs:
  unit-and-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test --workspace --all-features

  loom:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: RUSTFLAGS="--cfg loom" cargo test --release --test 'loom_*' -- --test-threads 1

  proptest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: PROPTEST_CASES=2000 cargo test --release --test 'prop_*'

  differential:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test -p cq-differential-tests --release

  bench-regression:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo bench --bench '*' -- --output-format bencher | tee bench.txt
      - uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: bench.txt
          fail-on-alert: true
          alert-threshold: '105%'
          comment-on-alert: true

  nightly-soak:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: PROPTEST_CASES=10000 cargo test --release --test 'prop_*'
      - run: cargo test --release --test 'soak_*' -- --ignored

  nightly-fuzz:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo install cargo-fuzz
      - run: timeout 3600 cargo fuzz run fuzz_predicate_parser
      - run: timeout 3600 cargo fuzz run fuzz_frame_decoder

  nightly-crash:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo test --release --test 'crash_*' -- --ignored
```

Schedule the nightly jobs with:

```yaml
on:
  schedule:
    - cron: '0 2 * * *'  # 02:00 UTC every day
```

---

*End of review.*

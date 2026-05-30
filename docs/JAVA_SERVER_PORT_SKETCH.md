# GC-Free Off-Heap Java Port — Feasibility Sketch

**Status:** advisory / decision-support. No code committed.
**Scope:** porting the CQServer **engine** (not the client SDK) from Rust to JVM.
**TL;DR:** technically achievable, but only by writing allocation-free, off-heap
"mechanical-sympathy" Java that abandons most idiomatic JVM patterns. The effort lands you
*near* where the Rust engine already is, while discarding a working, tested codebase and its
compile-time safety guarantees. Recommended only if a hard external constraint (team, platform
mandate) forces the JVM.

---

## 1. Why "just rewrite it in Java" is the wrong framing

The server's value proposition is **low, predictable tail latency** for conflated fanout. Plain
idiomatic Java (objects on the heap, autoboxing, short-lived garbage) directly attacks that
proposition via GC jitter and cache-hostile memory layout (see
[`AMPS_PARITY.md`](AMPS_PARITY.md) and the latency discussion that prompted this doc).

Every production low-latency JVM system (LMAX Disruptor, Aeron, Chronicle Queue/Map) wins by
**not** writing normal Java: off-heap memory, object pooling, single-writer designs, zero
allocation on the hot path. So the real question is not "rewrite in Java?" but **"are we willing
to write GC-free off-heap Java, and is the result worth abandoning the Rust engine?"** This sketch
quantifies that.

---

## 2. Target architecture (what the port must look like)

| Layer | Approach on the JVM |
|---|---|
| Memory | Off-heap arenas via `java.lang.foreign` (Panama, JDK 21+) or Agrona `UnsafeBuffer`. Heap reserved for control plane only. |
| GC | Choose a low-pause collector (ZGC generational) **and** keep the hot path allocation-free so the collector almost never runs there. Goal: zero garbage per message processed. |
| Concurrency | Single-writer-per-shard (Disruptor-style ring buffers) instead of shared mutable state + locks. Cross-thread handoff via lock-free SPSC/MPSC queues (Agrona `ManyToOneRingBuffer`). |
| Transport | Netty (epoll/io_uring) or raw NIO with pooled direct `ByteBuffer`s; zero-copy framing. |
| Serialization | Hand-rolled flyweight codecs reading/writing directly off the buffer (SBE-style), never materializing POJOs on the hot path. |
| Startup | GraalVM `native-image` or AppCDS + tiered-compilation tuning to blunt JIT warmup on failover. |

The defining constraint: **the hot path (parse → match → conflate → frame → send) must allocate
zero heap objects.** Everything there becomes flyweights over off-heap buffers.

---

## 3. Component-by-component port map

### 3.1 SOW store (`cq-core/src/store.rs`, `topic.rs`)
Today: columnar in-memory store; strings/bytes *moved* into columns to avoid clones (tasks
#14–16); rows addressed by key index.

- **Java reality:** a Java `HashMap<String, Row>` of POJO rows is a non-starter — object headers,
  boxing, and pointer-chasing destroy cache locality and balloon the heap for millions of rows.
- **Required:** off-heap columnar storage. Each column is a `MemorySegment`; variable-length
  values (strings/bytes) live in a separate arena with (offset,len) descriptors in a fixed-width
  column. A custom open-addressing primitive hash index (`long`→`int` row slot) keyed on the SOW
  key, off-heap, no `j.u.HashMap`.
- **Effort:** **High.** This is the heart of the system and the part where Java helps you least.
  You are re-implementing what Rust's `Vec<u8>` + slices give for free, plus manual lifetime
  management of the arenas.

### 3.2 Evaluator sharding (`cq-transport/src/delivery.rs`, `topic.rs` lanes — task #19)
Today: subscriptions partitioned into lanes; one dispatcher fans `Arc<MutationEvent>` to N lane
channels (`crossbeam`); each lane owns its `SubscriptionEngine` under `parking_lot::Mutex`;
per-key ordering preserved by single-producer sequence allocation.

- **Java mapping is actually clean here** — this is the Disruptor's home turf. Dispatcher →
  per-lane SPSC ring buffer → single consumer thread per lane. Single-writer-per-lane removes the
  need for the mutex entirely.
- **The catch — `Arc<MutationEvent>` fanout:** Rust shares one immutable event by atomic refcount
  across lanes for free. On the JVM you must either (a) publish the event into each lane's ring
  as a flyweight copy off-heap (no shared mutable heap object), or (b) share an off-heap segment
  with manual reference counting and a free-list — reintroducing exactly the lifetime bookkeeping
  the borrow checker did for you.
- **Lost guarantee:** Rust *proves* at compile time that a subscription lives in exactly one lane
  and its conflation state is single-threaded. In Java that invariant is a code-review/comment
  discipline; a stray cross-lane access is a runtime data race, not a compile error.
- **Effort:** **Medium.** Mechanically the best-fit component, but the fanout memory model is
  fiddly.

### 3.3 Snapshot fanout cache (`cq-transport/src/router.rs`)
Today: keyed by `(topic, sql)`, stores `Arc<Vec<Vec<Vec<u8>>>>`, TTL + byte-cap eviction, shared
read-only across many subscribers.

- **Required:** off-heap byte arena per cached snapshot + manual refcount so concurrent readers
  can't free it mid-send. A `ConcurrentHashMap` may key the cache (control plane, fine on heap)
  but the payload bytes must be off-heap to avoid GC pressure from large multi-MB snapshots.
- **Effort:** **Medium**, dominated by the refcount/eviction-vs-in-flight-reader race.

### 3.4 Conflation (`cq-core/src/conflation.rs`)
Today: per-subscription merge table (Add+Update→Add, Add+Remove→cancel, …), latest-wins.

- **Required:** the merge map must be off-heap or pooled (it's per-subscription and churns on
  every event). A pooled primitive-keyed map reused across flush cycles.
- **Effort:** **Medium.**

### 3.5 Transactions / txlog (`cq-txlog/`)
Today: append-only log; lazy/cheap payload in `upsert_map`; batch-commit under one write lock
(#15); reader replays for recovery/replication.

- **Java reality:** Java *can* do mmap (`MemorySegment.mapped`) and `fsync` (`force()`), but with
  less direct control over the page cache and write barriers than Rust. Chronicle Queue is the
  off-the-shelf analogue if you accept a dependency.
- **Effort:** **Medium**, lower if you adopt Chronicle Queue rather than hand-roll.

### 3.6 Replication (`cq-replication/`)
Today: shipper/receiver, sync-barrier in publish path (#13), split-brain fencing (#20),
active-active + failover (#21).

- **Mostly IO + protocol** — Java is competitive here; Netty handles the wire. The sync-barrier
  latency cost is again GC-sensitive but not layout-sensitive.
- **Effort:** **Medium**, the most "ordinary Java" part of the system.

### 3.7 Query / SQL (`cq-core/src/query.rs`, `predicate.rs`, `view.rs`)
Today: `sqlparser`-based SOW SQL, predicate matching with index acceleration (#17), incremental
aggregates (#18), incremental views (#22), PIVOT/UNPIVOT.

- **Parser:** off-the-shelf (Apache Calcite / JSqlParser) — control plane, heap is fine; parsing
  happens once per query, not per row.
- **Predicate evaluation hot path:** must be allocation-free flyweight matching against off-heap
  rows. The InString matcher (#7) and OR/IN index acceleration (#17) reappear as primitive-array
  / off-heap-index work.
- **Effort:** **High** for the hot-path matchers and incremental maintenance; **Low–Medium** for
  the parser front-end.

### 3.8 Transports (`cq-transport/src/{tcp,websocket}.rs`)
- Netty with `epoll`/`io_uring` + pooled direct buffers; WebSocket via Netty codec. Frame-size
  limits (#10), binary-on-TCP handshake (#30), zstd via `zstd-jni` (off-heap).
- **Effort:** **Low–Medium** — Netty is mature and a good fit.

### 3.9 Auth / entitlements (`cq-transport/src/auth.rs`)
- JWT (HS/RS256, #27/#29), per-action entitlements. Nimbus JOSE or similar. Control plane, heap
  is fine.
- **Effort:** **Low.**

---

## 4. Effort summary

| Component | Java fit | Effort | Main risk |
|---|---|---|---|
| SOW store (columnar, off-heap) | Poor | **High** | Manual arena lifetimes; the core differentiator |
| Evaluator sharding | Good (Disruptor) | Medium | `Arc` fanout → manual refcount; lost single-lane proof |
| Snapshot fanout cache | OK | Medium | Refcount vs eviction races |
| Conflation | OK | Medium | Pooling the merge map |
| txlog | OK | Medium | Lower with Chronicle Queue |
| Replication | Good | Medium | Sync-barrier latency under GC |
| Query/predicate hot path | Poor | **High** | Allocation-free matchers + incremental views |
| SQL parser front-end | Good | Low–Med | Calcite/JSqlParser integration |
| Transports | Good | Low–Med | Netty zero-copy framing |
| Auth/entitlements | Good | Low | — |

**Rough order of magnitude:** a faithful, GC-free port is a **multi-engineer, multi-quarter**
effort — comparable to rebuilding the engine from scratch, because the off-heap store, hot-path
matchers, and lane memory model cannot be machine-translated from the Rust. The IO/control-plane
layers (transport, replication, auth, parser) are the cheap 30%; the data plane is the expensive
70% and exactly where Java fights you.

---

## 5. What you give up

- **Compile-time data-race freedom.** The borrow checker currently proves the lane/conflation
  invariants. In Java these become discipline + tests; races surface at runtime under load.
- **Deterministic deallocation.** Replaced by manual arena/refcount management — i.e. you do the
  borrow checker's job by hand, with `Cleaner`/try-with-resources as a weaker safety net and
  use-after-free / double-free now possible.
- **A working, tested system.** `cq-core`, `cq-transport`, `cq-replication`, `cq-txlog` plus their
  test suites and the 30 hardening/feature tasks (#1–#30) would be re-derived.
- **Peak throughput-per-core**, hence higher node count / cloud cost for the same load.

## 6. What you gain

- Larger hiring pool; faster development **for ordinary (non-hot-path) code**.
- Best-in-class observability (JFR, async-profiler, JMC).
- Mature off-heap/low-latency ecosystem to lean on: **Agrona, Disruptor, Aeron, Chronicle,
  Netty, SBE, zstd-jni**.
- Cross-platform bytecode (though GraalVM native-image partly reverses this for deployment).

---

## 7. Recommendation

Keep the **engine in Rust** (the AMPS-equivalent tier is C++/Rust for the same reasons). Invest
Java effort where it pays off and carries none of these drawbacks:

1. **Java client SDK** — already shipped (`client-sdks/java`), now at parity with the TS/Rust SDKs
   (HA failover, heartbeat, conflation, resume tracking).
2. **JVM-native integrations** — Kafka Connect / Flink / Spark connectors, a JDBC-ish query
   facade, Spring Boot starter — all client-side, all idiomatic Java, all GC-tolerant.

If a hard mandate *requires* the engine on the JVM, treat it as a **from-scratch off-heap build**
(budget per §4), adopt Agrona+Disruptor+Netty+Chronicle from day one, and gate it behind a
latency-parity benchmark against the Rust engine before committing — not as a "rewrite of the Rust
code in Java," which would silently reintroduce GC on the hot path and miss the SLA.

# AMPS-Style Continuous Query Server — Feature Specification & Test Strategy

A granular feature reference distilled from 60East Technologies' AMPS product, written as an implementation specification for building an equivalent continuous-query server in **Rust**. Each feature section is followed by Rust implementation notes and a dedicated test strategy.

---

## Table of Contents

1. [Core Engine & Messaging Model](#1-core-engine--messaging-model)
2. [Message Types & Codecs](#2-message-types--codecs)
3. [Content Filtering & Continuous Query](#3-content-filtering--continuous-query)
4. [State of the World (SOW)](#4-state-of-the-world-sow)
5. [Views, Aggregation & Analytics](#5-views-aggregation--analytics)
6. [Transaction Log & Bookmark Subscriptions](#6-transaction-log--bookmark-subscriptions)
7. [Replication & High Availability](#7-replication--high-availability)
8. [Message Queues](#8-message-queues)
9. [Delta Messaging & Out-of-Focus Tracking](#9-delta-messaging--out-of-focus-tracking)
10. [Client SDK & Connection Semantics](#10-client-sdk--connection-semantics)
11. [Authentication, Authorization & Entitlements](#11-authentication-authorization--entitlements)
12. [Slow Client Management & Backpressure](#12-slow-client-management--backpressure)
13. [Operational & Admin Surface](#13-operational--admin-surface)
14. [Configuration Model](#14-configuration-model)
15. [Transports & Wire Protocol](#15-transports--wire-protocol)
16. [Performance Engineering](#16-performance-engineering)
17. [Cross-Cutting Rust Implementation Notes](#17-cross-cutting-rust-implementation-notes)
18. [Comprehensive Test Strategy](#18-comprehensive-test-strategy)

---

## 1. Core Engine & Messaging Model

### Feature Detail

- **Hybrid platform**: combines a message bus, queue, in-memory database, view server, analytics engine, and event-processing engine behind one wire protocol.
- **Topic namespace**: dotted/slashed string topic identifiers. Topics may be:
  - *Unmanaged*: pure pub/sub passthrough, no state retained.
  - *SOW-backed*: each distinct key is materialized as a row.
  - *View*: derived from one or more underlying SOW topics.
  - *Queue*: backed by the transaction log with delivery tracking.
- **Topic matching**: literal topic, prefix wildcard, and regex match on subscription requests.
- **Per-topic message type**: every topic declares one codec (JSON, BSON, FIX, etc.). Cross-type projection is allowed at view boundaries.
- **Per-topic Key expression**: an XPath-like field reference (or a composite of fields) defines record identity for SOW-backed topics.
- **Command verbs** (the engine's first-class operations):
  - `logon`, `logoff`
  - `publish`, `delta_publish`
  - `subscribe`, `unsubscribe`
  - `sow` (one-shot snapshot query)
  - `sow_and_subscribe` (atomic snapshot + live)
  - `sow_delete` (by key, by filter, or by bookmark)
  - `group_begin` / `group_end` (snapshot batching markers)
  - `start_timer`, `stop_timer` (heartbeats)
  - `ack` (response correlation)
  - `stats` (admin query)
- **Sequence numbering**: every published message gets a `(publisher_name, sequence)` pair used for dedup, replication ordering, and bookmark assignment.
- **Acknowledgement model**: publishers may request any subset of `received`, `parsed`, `persisted`, `processed`, `stats`, `completed` acks. Each ack is correlated by `command_id`.

### Rust Implementation Notes

- Single-binary server using `tokio` as the async runtime.
- Per-connection task with a bounded `mpsc` channel for outbound frames.
- Topic registry behind a `DashMap<TopicId, Arc<TopicState>>`; topic IDs interned as `u32` to keep hot-path comparisons branch-free.
- Command dispatch as a tagged enum (`Command::Publish { … }`, `Command::Subscribe { … }`, …) with a single match in the protocol layer.
- Use `bytes::Bytes` for zero-copy payload sharing between subscribers.
- Sequence numbers as `(ClientId, u64)`; pack into a `u128` for cheap comparison.

### Tests

| Layer | Test |
|---|---|
| Unit | Command parser round-trips for every verb |
| Unit | Topic-name regex/prefix matcher matches the same set as `regex` crate reference impl |
| Unit | Sequence pair total ordering is monotonic across reboots when keyed by publisher |
| Integration | End-to-end publish-then-subscribe with one publisher and N subscribers; assert message order preserved per publisher |
| Integration | Mixed verb session: logon → subscribe → publish → unsubscribe → logoff with correct acks |
| Property | For any sequence of (publish, subscribe) interleavings, no subscriber misses a message published *after* its subscribe was acked |
| Concurrency (loom) | Two threads pushing to the per-subscription queue cannot reorder messages from the same publisher |
| Fuzz | Random command frames must never panic the dispatcher |

---

## 2. Message Types & Codecs

### Feature Detail

- **Built-in codecs**: JSON, BSON, MessagePack, BFlat (60East's compact binary), FIX, NVFIX, XML, Protocol Buffers, opaque binary blobs.
- **Field path syntax** (uniform across codecs):
  - `/field` — top-level field
  - `/parent/child` — nested object access
  - `/array/0/field` — positional array access
  - `/35` — FIX tag access
- **Type system**: each codec maps onto an internal type lattice (`Null`, `Bool`, `Int64`, `Float64`, `String`, `Bytes`, `Timestamp`, `Array`, `Object`). Comparisons in filters are evaluated against this normalized type.
- **Per-topic message type**: declared in config. Pub/sub respects type; cross-type projection happens at view boundaries.
- **Cross-type projection**: views can project FIX into JSON, BSON into MessagePack, etc., emitting the canonical wire form for the projected message type.
- **Custom codec modules**: pluggable codec interface so a site can add a proprietary format.

### Rust Implementation Notes

- Trait `MessageCodec` with methods:
  - `parse(&self, bytes: &[u8]) -> Result<Document>`
  - `serialize(&self, doc: &Document, out: &mut BytesMut)`
  - `extract(&self, bytes: &[u8], path: &FieldPath) -> Option<Value>` (cheap path extraction without full parse, for filtering)
- `Document` as a borrowed view over the underlying bytes where possible (`serde_json::Value` for JSON, custom for BSON via `bson` crate, custom FIX parser).
- For FIX, build a perfect-hash tag lookup so `/35` is O(1).
- Compile-time codec registry via a build-script feature flag matrix so unused codecs aren't linked in.

### Tests

| Layer | Test |
|---|---|
| Unit | Round-trip parse/serialize for each codec with golden corpus |
| Unit | Path extraction returns identical result whether via full parse or cheap path extractor |
| Unit | Type coercion: comparing `"42"` (string) vs `42` (int) follows documented rule (no implicit coercion → false) |
| Property | For any JSON value, `parse(serialize(v)) == v` |
| Property | Path extraction is monotone: removing a field from the document makes that path return `None` |
| Fuzz | Malformed input for each codec returns a typed error rather than panicking |
| Conformance | FIX parsing matches QuickFIX reference output for SOH-delimited corpus |
| Benchmark | Path extraction ≤ 200 ns for 10 KB JSON, ≤ 50 ns for FIX tag |

---

## 3. Content Filtering & Continuous Query

### Feature Detail

- **Filter expressions** evaluated server-side, before egress, on every command that admits a `filter` parameter (subscribe, sow, sow_and_subscribe, sow_delete, view, replication leg).
- **Comparison operators**: `=`, `!=`, `<`, `<=`, `>`, `>=`, `LIKE`, `NOT LIKE`, `MATCHES` (regex), `IN (...)`, `NOT IN (...)`, `BETWEEN ... AND ...`.
- **Boolean operators**: `AND`, `OR`, `NOT`, parenthesized grouping.
- **String functions**: `UPPER`, `LOWER`, `SUBSTR`, `LENGTH`, `STRLEN`, `CONCAT`, `INSTR`.
- **Numeric functions**: `ABS`, `ROUND`, `FLOOR`, `CEILING`, `MOD`, arithmetic `+ - * /`.
- **Date/time functions**: `NOW()`, `DATE`, `TIME`, `EPOCH`, arithmetic on timestamps.
- **Existence**: `EXISTS(/field)`, `IS NULL`, `IS NOT NULL`.
- **User-defined functions**: pluggable module exposing additional functions.
- **Compilation**: filters parse to an AST, optimize (constant folding, short-circuit reorder), then compile to bytecode (or threaded interpreter) evaluated per message.
- **Continuous query semantics**: when a subscription carries a filter, the engine re-evaluates the filter on every update. A record may enter the filter set, update within it, or leave it (see §9 OOF).
- **Indexed acceleration**: if a SOW topic has a hash index on field `/x` and the filter contains `/x = const`, the engine uses the index to short-circuit candidate enumeration for snapshot queries.

### Rust Implementation Notes

- Hand-written recursive-descent parser; lexer with `logos`.
- AST → typed IR with each node carrying its inferred type.
- Code generation: emit a `Vec<Op>` for a register-based VM; one register per AST node. Evaluate over a `&Document`.
- Optional Cranelift JIT for hot paths (filters with > 100k evaluations/sec).
- Hash-index integration: at filter-compile time, identify equality predicates on indexed fields; rewrite the snapshot scan plan to walk the index.
- Cache compiled filter bytecode keyed by canonicalized AST hash so identical subscriptions reuse it.

### Tests

| Layer | Test |
|---|---|
| Unit | Parser accepts every example from the spec; rejects malformed input with typed errors |
| Unit | Each operator returns the documented result for every type pair in the lattice |
| Unit | Short-circuit evaluation: `false AND /missing/field` does not error |
| Unit | NULL semantics follow SQL: `/x = NULL` is `false`, must use `IS NULL` |
| Property | For any AST, evaluating the AST and evaluating its compiled bytecode produce identical results on random documents |
| Property | Constant-folding optimizer preserves semantics for random expressions |
| Property | Index-rewritten plan returns the same row set as the naive scan |
| Fuzz | Random filter strings either parse-error cleanly or evaluate without panicking |
| Differential | Run identical filter against in-memory reference evaluator and JIT-compiled evaluator; outputs must match for 1M random docs |
| Benchmark | Filter eval cost ≤ 500 ns for a typical 5-predicate filter on a 10-field JSON doc |

---

## 4. State of the World (SOW)

### Feature Detail

- **Definition**: SOW is the engine's database side. Each SOW topic is a key→latest-message store.
- **Per-topic configuration**:
  - `Topic` / `Name` — literal or regex
  - `MessageType` — codec
  - `Key` — one or more field paths, hashed/concatenated to form the row identity
  - `FileName` — backing memory-mapped file path
  - `InitialSize`, `IncrementSize`, `MinSlabFreeSpace` — storage tuning
  - `Durability` — `transient` (memory only) or `durable` (mmap-backed)
  - `Expiration` — TTL in seconds, per-record
  - `SlabSize`, `RecordSize` — fixed-size record optimization
  - `Index` — secondary hash indexes on declared field paths
- **Operations**:
  - **Upsert**: each publish replaces the record for that key (or creates it).
  - **Delete**: `sow_delete` by key, by filter, or by bookmark.
  - **Snapshot query** (`sow`): one-shot result set, optionally filtered, projected, ordered, paginated.
  - **Atomic snapshot + subscribe** (`sow_and_subscribe`): the engine guarantees no message published during the snapshot is lost or duplicated.
  - **Group delivery markers**: `group_begin` / `group_end` bracket the snapshot batch so the client knows when the historical phase ends and live deltas begin.
- **Indexes**: hash indexes on declared fields auto-selected by the query planner for equality predicates.
- **Pagination**: snapshot queries accept `top_n`, `skip_n`, and ordering by one or more fields.
- **Projection**: snapshot queries can request a subset of fields (server-side strips before egress).
- **Persistence**: SOW files are memory-mapped slab allocators. On startup the file is mapped and live; tx-log replay forward-rolls any unmaterialized updates.

### Rust Implementation Notes

- Backing store: `memmap2::MmapMut` with custom slab allocator.
- Slab layout: header (key hash, key offset, value offset, length, version, flags), followed by inline key + value bytes.
- Concurrent access: per-shard `parking_lot::RwLock` with consistent hashing of keys → shards (default 64 shards).
- Hash indexes: `DashMap<FieldValueHash, RecordSlot>` per indexed field.
- Snapshot iteration uses a versioned snapshot read so a long-running query doesn't block writers.
- For `sow_and_subscribe`, take a versioned snapshot and a subscription enrollment under the same per-topic lock acquisition window; live deltas observed after the snapshot version are merged into the outbound stream.

### Tests

| Layer | Test |
|---|---|
| Unit | Insert/upsert/delete/get round-trips for every codec |
| Unit | Key derivation is deterministic across reboots |
| Unit | Expiration TTL fires within ±1 second of configured deadline |
| Unit | Hash index returns identical row set to full scan for every operator that admits index use |
| Integration | `sow_and_subscribe` delivers exactly the snapshot rows + every subsequent update, with no gaps and no duplicates, under 10 concurrent publishers |
| Integration | After restart, mmap-backed SOW returns identical contents to pre-restart query |
| Property | For any sequence of upserts and deletes, the SOW state matches a `HashMap` reference model |
| Concurrency (loom) | `sow_and_subscribe` enrollment race: no scheduling can cause a duplicate or gap |
| Crash | Kill -9 mid-write; on restart, SOW + tx-log replay produces a state that contains every acknowledged write |
| Benchmark | 1M-row snapshot scan with filter on indexed field ≤ 100 ms |
| Benchmark | Upsert throughput ≥ 1M ops/sec on a single shard with 50 B records |

---

## 5. Views, Aggregation & Analytics

### Feature Detail

- **Materialized views**: declarative aggregation defined in config; the engine materializes the view as another SOW topic.
- **View grammar**:
  - `UnderlyingTopic` — one or more source SOW topics
  - `MessageType` — output codec (may differ from underlying)
  - `Projection` — list of output field expressions, including aggregates
  - `Grouping` — GROUP BY field expressions
  - `Filter` — WHERE clause on underlying rows
  - `Join` — join across multiple underlying topics on keys
- **Aggregate functions**: `SUM`, `COUNT`, `MIN`, `MAX`, `AVG`, `FIRST`, `LAST`, `STDDEV`, `VARIANCE`.
- **Expression projection**: arbitrary arithmetic in projections, e.g. `SUM(/qty * /price) AS /notional`.
- **Conditional projection**: `CASE WHEN … THEN … ELSE … END` expressions.
- **User-defined aggregates**: pluggable additional aggregate functions.
- **Incremental maintenance**: every underlying row update produces an incremental view update — *not* a full recompute. The engine maintains per-group running aggregates.
- **View emits as SOW**: views are subscribable, queryable, and continuous-queryable just like raw SOW topics.
- **Conflated views**: optional emission interval — e.g. emit aggregated changes every 100 ms rather than on every input update.
- **Per-subscription aggregation**: a client can request ad-hoc aggregation without a server-side view definition. Caveats: cannot be a bookmark subscription, and cannot use the `replace` option except to change pagination.

### Rust Implementation Notes

- Each materialized view is its own actor: subscribes to underlying SOW updates, maintains a `HashMap<GroupKey, AggregatorState>`, emits to its own SOW topic.
- Aggregator trait:
  ```rust
  trait Aggregator {
      fn add(&mut self, value: &Value);
      fn remove(&mut self, value: &Value); // for delta maintenance
      fn finalize(&self) -> Value;
  }
  ```
- `SUM`, `COUNT`, `AVG` are straightforwardly invertible (`add`/`remove`).
- `MIN`/`MAX` require auxiliary structures (multiset, treap, or recompute) to handle deletion.
- For joins, maintain hash indexes on join keys for both sides; an update on either side enumerates matches and re-emits affected groups.
- Conflation: each view holds a coalescing buffer keyed by group key; a periodic tick flushes coalesced updates to the view's SOW.
- Consider Apache Arrow's compute kernels for batch finalization if you need very high group counts.

### Tests

| Layer | Test |
|---|---|
| Unit | Each aggregator's `add`/`remove`/`finalize` matches a from-scratch recompute over the same multiset |
| Unit | `CASE` expression matches reference evaluator |
| Integration | Materialized view contents match `SELECT … GROUP BY …` reference run on the same input data, after every step |
| Integration | Subscribing to a view receives a snapshot of current aggregates + live updates |
| Property | For any random sequence of underlying upserts/deletes, the incrementally maintained view equals a from-scratch recompute |
| Property | Join view: bidirectional update (left side, right side) produces identical final state regardless of update order |
| Stress | 10M underlying rows, 1K groups, sustained update rate; verify group state converges after the input stream ends |
| Determinism | Same input event log produces same view state across runs |
| Benchmark | Single-input-row update propagates to view emission in ≤ 50 µs |

---

## 6. Transaction Log & Bookmark Subscriptions

### Feature Detail

- **Transaction log (tx-log)**: a durable, append-only, sequential record of every published message.
- **Journal files**: rolled on size or time threshold; live files in `JournalDirectory`, archived files optionally moved to `JournalArchiveDirectory` on slower storage.
- **Compression**: journals may be compressed on rotation; the engine caches decompressed chunks in memory during replay.
- **Bookmarks**: every persisted message gets a globally unique identifier of the form `{publisher_id}|{sequence}|` — totally ordered within an instance.
- **Replay modes** (four):
  1. **EPOCH** — replay from the beginning of all retained history, then catch up to live.
  2. **NOW** — start at the live tail, no history (`0|1|` on the wire).
  3. **MOST_RECENT** — last bookmark previously delivered to this client (looked up by client name in its BookmarkStore).
  4. **Explicit** — by specific bookmark, or by wall-clock timestamp (engine seeks to the first message at or after that time).
- **Pause / resume**: a bookmark subscription can be paused and resumed mid-stream by the client.
- **Filtered replay**: same filter grammar applies to replay subscriptions.
- **`fully_durable`** option: replay messages only released to the client after they've been confirmed persisted at all configured sync-replication destinations.
- **Tagging**: replay messages carry a header distinguishing historical from live, so clients can skip warm-up logic on live messages.
- **Auditing / backtesting use cases**: tx-log replay is suitable for full-fidelity replay into a test system.

### Rust Implementation Notes

- Journal format: framed records with `(record_len: u32, bookmark: (u32, u64), header: variable, payload: bytes)`.
- Append path: serialize → batch into a write buffer → `fdatasync` on a configurable cadence (or per-message for sync persistence).
- Index: maintain an in-memory `BTreeMap<Bookmark, FileOffset>` for the live journal, plus per-archive-file footer index for O(log n) seek.
- Replay reader: memory-map the journal file, iterate from the seek point, apply filter, emit.
- For pause/resume, replay state is held in the subscription actor; on pause, the reader yields; on resume, it picks up at the saved offset.
- Use `crc32fast` for per-record checksums to detect torn writes.

### Tests

| Layer | Test |
|---|---|
| Unit | Bookmark ordering is total within a publisher and across publishers (lexicographic over `(publisher, seq)`) |
| Unit | Journal record framing round-trips |
| Unit | Each of EPOCH, NOW, MOST_RECENT, explicit, timestamp seek positions the reader correctly |
| Integration | Publish N messages → restart server → subscribe EPOCH → receive same N messages in order |
| Integration | Pause-resume mid-replay receives the suffix exactly once with no gap |
| Integration | Filtered replay returns the same row set as filtering the snapshot of replay output |
| Crash | Torn write at the journal tail is detected and truncated on next startup |
| Crash | Power loss simulation (no fsync) loses at most the last `commit_interval` of writes; never corrupts earlier records |
| Property | For any sequence of publishes and a random bookmark X, replay-from-X delivers exactly the messages with bookmark > X |
| Benchmark | Append throughput ≥ 500K msgs/sec single-threaded with batched fsync |

---

## 7. Replication & High Availability

### Feature Detail

- **Replication leg**: a directed link from instance A to instance B, fed from A's tx-log.
- **Modes**:
  - **Synchronous**: publisher's `persisted` ack waits until configured downstream destinations have also confirmed persistence.
  - **Asynchronous**: local persistence is sufficient for ack; replication catches up out of band.
- **Per-destination configuration**:
  - `SyncType` — sync or async
  - `Filter` — replicate only matching messages
  - `Transform` — change message type or restructure
  - `BookmarkStore` for the leg — tracks the upstream bookmark already shipped to this destination
- **Multi-destination**: a primary can have many destinations, each with its own sync/async, filter, and transform.
- **Topology**: N-way active/active and active/passive both supported. The engine resolves duplicates via `(publisher_name, sequence)` so a message replicated via multiple paths is deduped at the destination.
- **Link downgrade policies**:
  - Auto-downgrade sync→async when a destination is offline beyond a configurable threshold.
  - Auto-upgrade async→sync when the destination returns and catches up.
- **Failover (client side)**:
  - `HAClient` wraps a list of connection strings, a `ServerChooser` strategy, and a `DelayStrategy` for reconnect backoff.
  - On reconnect: resume publish from the PublishStore (retransmit unacked), resume subscriptions from the BookmarkStore.
- **Guaranteed publish**:
  - Publisher holds the message in its PublishStore until persisted ack.
  - Server dedupes by `(client_name, sequence)`.
  - Unique client name across the cluster is mandatory.
- **Client stores**:
  - **PublishStore** — local persistent buffer of unacked outbound messages.
  - **BookmarkStore** — last delivered bookmark per subscription.
  - Both stores have memory-backed and file-backed variants.

### Rust Implementation Notes

- Replication leg as a state machine: `Disconnected → Connecting → Authenticating → Catching Up → Live`.
- Use the same tx-log replay machinery internally; the destination is just another consumer.
- Sync ack barrier: maintain a per-leg `last_persisted_bookmark`; a publish's `persisted` ack is delayed until all sync legs cross that bookmark.
- Downgrade policy: timer-driven; on threshold breach, atomically swap `SyncType` and emit a control event for monitoring.
- Client `PublishStore` / `BookmarkStore`: file-backed with a `sled` or hand-rolled WAL.
- `ServerChooser` and `DelayStrategy` as traits with default implementations (round-robin, weighted, exponential backoff, fibonacci backoff).

### Tests

| Layer | Test |
|---|---|
| Unit | Bookmark dedup correctly drops `(publisher, seq)` already seen |
| Unit | Sync ack barrier releases ack only after all sync legs cross the bookmark |
| Integration | A → B async replication: 1M messages → B's tx-log identical to A's |
| Integration | A → B sync replication: publisher ack latency ≥ B persistence latency |
| Integration | A ↔ A' active/active: a message published to either side propagates to the other and is not echoed back as a duplicate |
| Integration | Client failover: kill A, client reconnects to B, publish resumes from PublishStore, subscriptions resume from last bookmark, zero loss/duplication |
| Chaos | Random network partitions for 30s windows; assert no messages lost (sync) or eventually delivered (async) |
| Chaos | Slow follower: B's I/O artificially slowed; auto-downgrade fires at threshold, auto-upgrade fires after catch-up |
| Property | For any partition scenario, after partition heal, both sides converge to identical tx-logs |
| Property | `(client_name, sequence)` is sufficient to deterministically dedup across arbitrary replication topologies |
| Benchmark | Sync replication adds ≤ 1 ms publish latency over a 100 µs LAN |

---

## 8. Message Queues

### Feature Detail

- **Storage**: queue messages live in the tx-log; queue state tracks which have been delivered and acknowledged.
- **Definition**: a queue is "all messages in topic X matching filter Y" — content-based queue selection at the engine, not at the publisher.
- **Multiple consumers**: work-distribution semantics — each message goes to exactly one consumer (at-least-once with redelivery).
- **Backlog**: max in-flight messages per consumer.
- **Lease time**: if a consumer doesn't ack within the lease, the message is redelivered (possibly to a different consumer).
- **Max delivery count**: after N redeliveries, the message goes to dead-letter or is dropped per policy.
- **Fairness strategies**: round-robin, weighted, lowest-backlog-first.
- **Browse mode**: peek without consuming.
- **Dead-letter**: failed/expired queue messages routed to a configured dead-letter topic.
- **Per-consumer filter**: consumers may additionally filter the queue's logical stream at subscribe time (intersection with the queue's own filter).
- **Queue replication**: queue state replicates with the tx-log; on failover, the secondary resumes delivery with the exact lease/ack state.
- **Ordering**: publication order preserved by default. Priority-field-based reordering optional.

### Rust Implementation Notes

- Queue actor owns: a cursor into the tx-log, a `BTreeMap<DeliveryId, LeaseRecord>` for in-flight, a per-consumer state map.
- Delivery loop: pull next eligible message from cursor → evaluate per-consumer filters → select consumer per fairness policy → emit with `delivery_id` → record lease.
- Ack path: drop the lease record; advance the consumed-watermark.
- Lease timer: a min-heap keyed by lease expiry; expired entries re-enter the delivery queue with incremented redelivery count.
- DLQ: a sibling SOW topic.

### Tests

| Layer | Test |
|---|---|
| Unit | Each fairness strategy distributes N messages to K consumers per documented rule |
| Unit | Lease expiry redelivers the message after exactly the lease timeout |
| Unit | Max-delivery-count routes to DLQ on the (N+1)th attempt |
| Integration | One publisher, ten consumers: every message delivered to exactly one consumer, all acked, none lost |
| Integration | Consumer crash mid-lease: message redelivers to another consumer after lease expiry |
| Integration | Browse mode does not affect delivery state |
| Property | For any sequence of (publish, consume, ack, crash) events, every acked-by-server message was delivered at least once; every committed-ack message is never redelivered |
| Property | Queue ordering preserved per publisher under all fairness strategies |
| Chaos | Repeated consumer kills during sustained load; no message loss, no permanent stuck leases |
| Failover | Primary dies while messages in flight; secondary resumes with identical lease state |
| Benchmark | Throughput ≥ 200K messages/sec with 10 consumers at 1 KB messages |

---

## 9. Delta Messaging & Out-of-Focus Tracking

### Feature Detail

- **Delta publish** (`delta_publish`):
  - Publisher sends only changed fields plus the key.
  - Server applies them as a field-level merge against the existing SOW record.
  - The full merged record is persisted; subscribers configured for delta receive only the diff plus the key.
- **Delta subscribe**:
  - `send_keys` option — initial snapshot delivers only key fields, no value payload.
  - Subsequent updates deliver only changed fields.
  - Substantial bandwidth win for wide records (hundreds to thousands of fields).
- **Out-of-Focus (OOF) events**:
  - Generated when a record leaves the filter set, is deleted, or expires.
  - Header `c=oof_filter` — record updated such that it no longer matches the subscription's filter.
  - Header `c=oof_delete` — record was deleted.
  - Header `c=oof_expired` — record TTL expired.
  - Subscribers must opt in with the `oof` flag.
- **Use cases**:
  - Grid components need explicit "leave row" events so they don't keep stale data.
  - Bandwidth optimization on wide rows where most fields are stable.

### Rust Implementation Notes

- Delta computation: structural diff between old and new `Document` representations; emit a sparse map of changed paths.
- For wide records, store a per-record version vector or per-field version stamp to fast-path "no change" detection.
- OOF emission: filter evaluator returns one of `{Match, NoMatch, WasMatchNowGone}`. The last triggers an OOF emission to subscriptions that opted in.
- A subscription's per-key filter membership is tracked in a `HashSet<KeyHash>` (or roaring bitmap for large topics).

### Tests

| Layer | Test |
|---|---|
| Unit | Delta diff of two documents equals the field-wise set difference |
| Unit | Applying the diff to the old document yields the new document |
| Unit | OOF emitted exactly once per (record leaves filter) transition; not re-emitted on subsequent unrelated updates |
| Integration | Delta subscription with `send_keys` delivers initial keys, then field-level diffs that, when merged client-side, equal the full record |
| Integration | OOF subscription on a filter `/state = 'open'` emits enter/update/oof_filter/oof_delete events matching reference set-tracking |
| Property | For any sequence of updates and a continuous-query filter, the sequence of enter/update/oof events reconstructs the correct membership set at every point |
| Property | Delta publish followed by delta subscribe end-to-end equals full publish + full subscribe in observed semantics |
| Stress | 1000-field records, 90% fields stable, sustained delta updates — bandwidth ≤ 10% of full-publish baseline |

---

## 10. Client SDK & Connection Semantics

### Feature Detail

- **Client object**: long-lived connection wrapper with command-issuing methods.
- **HAClient**: client + failover infrastructure:
  - `ServerChooser` — strategy for selecting next server on disconnect (round-robin, weighted, custom).
  - `DelayStrategy` — backoff schedule (constant, exponential, fibonacci).
  - `connectAndLogon()` — atomic connect + authenticate.
- **MessageStream / MessageHandler**: iterator-style and callback-style consumption APIs.
- **PublishStore**: local durable buffer of unacked outbound messages; replays on reconnect.
- **BookmarkStore**: last-received bookmark per subscription; used for MOST_RECENT replay on resubscribe.
- **Heartbeats**: client/server keepalive interval; missed heartbeats trigger reconnect.
- **Compression**: per-connection compression negotiation (lz4, zlib).
- **TLS**: TLS on TCP transport.
- **Authenticator / AuthenticationHandler**: pluggable auth callback (password, Kerberos, custom token).
- **ConnectionStateListener**: observer for connect/disconnect/lifecycle events.
- **ExceptionListener / FailedWriteHandler**: async error sinks.
- **Native object hydration**: messages converted to language-native types (Maps, Documents).

### Rust Implementation Notes

- Async client built on `tokio` with `tokio_rustls` for TLS.
- Public API:
  ```rust
  pub struct HaClient { /* ... */ }
  impl HaClient {
      pub async fn connect_and_logon(&self) -> Result<()>;
      pub async fn publish<T: Serialize>(&self, topic: &str, msg: &T) -> Result<Ack>;
      pub async fn subscribe(&self, req: SubscribeRequest) -> Result<MessageStream>;
      pub async fn sow(&self, req: SowRequest) -> Result<SnapshotStream>;
      pub async fn sow_and_subscribe(&self, req: SowAndSubscribeRequest) -> Result<MessageStream>;
  }
  ```
- `MessageStream` implements `Stream<Item = Result<Message>>`.
- `PublishStore` and `BookmarkStore` are traits with file-backed (`sled`) and in-memory implementations.
- Reconnection logic in a background task; on reconnect, re-establish subscriptions from `BookmarkStore` and replay unacked publishes from `PublishStore`.

### Tests

| Layer | Test |
|---|---|
| Unit | Each `DelayStrategy` produces the documented sequence of delays |
| Unit | `PublishStore` persists across process restart; unacked messages survive |
| Unit | `BookmarkStore` resumes from the correct last bookmark |
| Integration | Kill server during publish: client retries on reconnect, server eventually acks, store clears |
| Integration | Kill server during subscribe: client reconnects, resumes from last bookmark, receives gap-free stream |
| Integration | Server returns no-auth on logon: client emits auth error and does not retry |
| Integration | TLS handshake against a server with a self-signed cert configured in trust store |
| Property | For any sequence of (publish, server-kill, ack) events, every successfully completed publish (returned `Ok`) is durably persisted on the server |
| Property | Stream consumer that drops mid-iteration does not leak server-side subscriptions |
| Long-running | 24-hour sustained subscribe with periodic injected disconnects; zero message loss, zero unbounded memory growth |
| Benchmark | Connection setup + logon ≤ 5 ms |

---

## 11. Authentication, Authorization & Entitlements

### Feature Detail

- **Authentication** (pluggable):
  - Anonymous
  - Password (client supplies credentials at logon)
  - Kerberos / GSSAPI
  - Custom modules (LDAP, AD, OAuth, Okta tokens, mTLS subject)
- **Authorization (Entitlements)**:
  - Per-topic, per-command ACLs: publish, subscribe, sow, sow_delete, delta_publish, replication source/destination.
  - **Filter-based row-level entitlements** — the entitlement module can inject a server-side filter into every subscription, restricting which rows a user can see (e.g. "only your book").
  - **Field-level projection** — the entitlement module can inject a projection list, stripping forbidden fields before egress.
  - Group/role mapping via the module.
  - **Action entitlements** — admin actions (disconnect client, downgrade link, rotate journal) gated by separate permissions.
- **Audit hooks**: every command can be logged with the authenticated identity, optionally to a separate audit sink.

### Rust Implementation Notes

- Trait `Authenticator`:
  ```rust
  trait Authenticator: Send + Sync {
      async fn authenticate(&self, creds: &LogonCredentials) -> Result<Identity>;
  }
  ```
- Trait `Entitlement`:
  ```rust
  trait Entitlement: Send + Sync {
      fn check(&self, identity: &Identity, action: Action, topic: &str) -> Decision;
      fn rewrite_subscribe(&self, identity: &Identity, req: &mut SubscribeRequest);
  }
  ```
- Built-in implementations: file-backed users/groups, mTLS subject extraction, JWT validator.
- Filter rewrite: append the entitlement filter as `AND` to the client's filter; same for projection lists.

### Tests

| Layer | Test |
|---|---|
| Unit | Each auth module accepts valid credentials and rejects invalid ones |
| Unit | Permission matrix: (user × command × topic) returns the expected decision |
| Unit | Filter rewrite: client filter `X` combined with entitlement filter `Y` = `X AND Y` |
| Integration | A subscribe with a too-permissive filter is silently narrowed by the entitlement layer; client sees only its allowed rows |
| Integration | Field projection: forbidden fields are absent from delivered messages |
| Property | An unauthenticated client cannot induce any state change |
| Property | For any (identity, filter, projection) combination, the delivered rows are a subset of what the un-entitled query would return |
| Security | Fuzz the auth handler with malformed credentials; never panics, never grants access on error |
| Audit | Every privileged action produces an audit record with identity, timestamp, command, target |

---

## 12. Slow Client Management & Backpressure

### Feature Detail

- **Per-client memory limits**: `MaxQueuedBytes`, `MaxQueuedMessages`.
- **Two intervention modes**:
  1. **Offlining**: when a client's outbound queue exceeds the in-memory limit, the engine spills to a per-client overflow file in `OfflineDir`. The client continues to receive messages, just from disk.
  2. **Disconnect**: at a higher threshold, the engine closes the connection.
- **Conflation as backpressure**:
  - Subscriptions may request server-side conflation (`conflation=100ms`).
  - **Conflation key**: which field(s) define "same logical update"; for orders, typically the order key; for ticks, the symbol. Updates with the same conflation key within the interval are coalesced; only the latest is delivered.
- **Per-subscription send buffer**: outbound queue depth visible in admin stats.
- **Backpressure signaling**: clients can read their own queue depth and back off publishing.

### Rust Implementation Notes

- Per-subscription outbound channel as a bounded `mpsc` with a configurable capacity.
- Overflow handler: when push would block, route to a per-client overflow file (`bincode` framed) and continue.
- Conflation buffer: `HashMap<ConflationKey, PendingMessage>` per subscription, drained on a `tokio::time::interval` tick.
- Disconnect path: drop the subscription, close the socket, emit a `ClientDisconnected` admin event.

### Tests

| Layer | Test |
|---|---|
| Unit | Offlining threshold triggers spill to overflow file; messages still delivered in order |
| Unit | Disconnect threshold closes the connection cleanly |
| Unit | Conflation buffer keyed by `K` retains only the latest value per K within the interval |
| Integration | Slow client + fast publisher: server memory stays bounded; fast client unaffected |
| Integration | Conflated subscription: receiver gets ≤ ceil(updates × interval / total_time) messages, with the latest state per key |
| Property | Conflation never delivers an older value after a newer one for the same conflation key |
| Property | Disconnect of slow client does not affect any other subscription's delivery |
| Stress | One subscriber blocked on `recv`; publisher sustains 1M msg/sec; server memory rises to `MaxQueuedBytes` then plateaus (no OOM) |
| Failure | Disk full during offlining → controlled disconnect, no crash |

---

## 13. Operational & Admin Surface

### Feature Detail

- **Admin HTTP endpoint** (per instance):
  - `/amps/instance/...` — live JSON stats: connections, subscriptions, topics, SOW sizes, replication lag, memory, CPU.
  - `/amps/instance/clients/...` — per-client introspection: queue depth, last activity, offlining state.
  - `/amps/instance/replication/...` — replication link health and lag.
  - `/amps/instance/control/...` — runtime actions: disconnect client, downgrade link, rotate journal.
- **Action invocation**: scriptable runtime ops (rotate logs, snapshot SOW, force compaction).
- **Logging**:
  - Per-target rule-based logging (file, syslog, network).
  - Filters on log target so e.g. one file gets only auth events.
- **Stats command**: a wire-protocol `stats` command surfaces a subset of the admin stats without HTTP.
- **Module hot-reload**: some modules support reload without restart (auth, entitlement).
- **Web UI**: a vendor-supplied web UI (Galvanometer) for topology, live stats, ad-hoc queries.

### Rust Implementation Notes

- Admin HTTP via `axum`; reuse the same auth chain.
- Stats via a periodic snapshot of atomic counters into a `serde_json::Value` tree; cheap to serve.
- Control endpoints: each action posts a typed message onto an internal control channel; handlers in the relevant subsystem execute it.
- Use `tracing` + `tracing-subscriber` for structured logging; layered sinks for per-event-type routing.
- Optional Prometheus exporter at `/metrics`.

### Tests

| Layer | Test |
|---|---|
| Unit | Stats snapshot is internally consistent (no negative counts, no impossible sums) |
| Integration | Each admin endpoint returns documented JSON shape under various states (idle, loaded, partitioned) |
| Integration | `disconnect_client` action actually drops the named connection within 1 second |
| Integration | `rotate_journal` produces a new journal file and seals the previous one |
| Property | Stats counters reconcile against external observation (e.g. published message count == counter delta) |
| Security | Admin endpoints require admin entitlement; anonymous access returns 401/403 |
| Chaos | Spam admin endpoints during heavy load; data-plane throughput unaffected (admin is on a separate runtime if needed) |

---

## 14. Configuration Model

### Feature Detail

- **Single XML config file** per instance. Top-level elements:
  - `InstanceName`
  - `Admin` (admin HTTP listener)
  - `Transports` (TCP, TCPS, WebSocket, shared-memory)
  - `MessageTypes` (codec registration)
  - `Modules` (loadable extensions)
  - `Authentication`
  - `Entitlement`
  - `SOW` (with nested `Topic` and `View`)
  - `TransactionLog`
  - `Replication` (with nested `Destination`)
  - `Queue`
  - `Logging`
  - `MemoryLimits`
  - `SlowClientManagement`
- **Variable substitution**: `%n` (topic name), environment variable `$VAR`.
- **Conditional includes**: include sub-files based on environment.
- **Validation tool**: `ampServer -tc` validates config without starting.

### Rust Implementation Notes

- Use TOML or YAML rather than XML for native Rust ergonomics, *but* preserve the structural shape so AMPS users have a familiar mental model.
- `serde`-driven config struct with `#[serde(deny_unknown_fields)]` to catch typos.
- `--check-config` CLI flag for offline validation.
- Variable substitution via `envsubst`-style preprocessor or `tinytemplate`.

### Tests

| Layer | Test |
|---|---|
| Unit | Every config element parses to the expected struct |
| Unit | Unknown fields trigger an error (no silent ignore) |
| Unit | Variable substitution resolves all known placeholders; unresolved placeholders error out |
| Unit | Cross-field validation: e.g. `View.UnderlyingTopic` must reference an existing `Topic`; missing reference errors out |
| Property | Round-trip parse(serialize(config)) == config |
| Conformance | A curated corpus of valid configs all load; a corpus of invalid configs all reject with informative messages |

---

## 15. Transports & Wire Protocol

### Feature Detail

- **Transports**:
  - TCP, TCPS (TLS)
  - WebSocket / WSS (browser clients)
  - Shared memory (same-host clients, sub-microsecond latency)
  - Multicast (publish-side fanout where applicable)
- **Wire protocol**:
  - Native binary framing (length-prefixed header + payload).
  - JSON-headered variant available for easier debugging.
  - Each command carries a `command_id` for ack correlation.
  - Headers carry: topic, filter, options, bookmarks, sequence numbers, ack flags, projection, ordering, pagination.
- **Heartbeats**: keepalive frames at a configurable interval.
- **Compression**: per-connection negotiation (lz4, zlib).
- **Backwards compatibility**: protocol carries a version field; server negotiates the highest mutually supported version.

### Rust Implementation Notes

- Frame: `[len: u32 LE][header_len: u16][header_bytes][payload_bytes]`.
- Header as a small key-value structure; pack into a `BytesMut` for zero-copy reads.
- Codec layer using `tokio_util::codec::Framed`.
- WebSocket via `tokio-tungstenite`; map WS message boundaries 1:1 to protocol frames.
- Shared-memory transport via a SPSC ring buffer in an `mmap`'d file with futex-based wakeups.

### Tests

| Layer | Test |
|---|---|
| Unit | Frame round-trip: encode → decode produces the original frame |
| Unit | Partial frame reads correctly buffered until complete |
| Unit | Oversized frame rejected per max-frame limit |
| Integration | Each transport carries the same protocol semantics: same publish-subscribe test passes over TCP, TLS, WS, shmem |
| Integration | Version negotiation: client v2, server v3 → both speak v2 |
| Fuzz | Random bytes on the wire never cause panics or unbounded memory |
| Compat | Old-client / new-server and new-client / old-server matrix tests pass for every released version |
| Benchmark | TCP loopback round-trip ≤ 50 µs for a 100-byte publish + ack |
| Benchmark | Shared-memory round-trip ≤ 1 µs |

---

## 16. Performance Engineering

### Feature Detail

- **Memory-mapped SOW files**: zero-copy reads from disk-backed state.
- **Per-CPU sharding**: hot SOW topics partitioned across shards for write parallelism.
- **Lock-free internal queues**: outbound queues per subscription.
- **JIT-compiled filter evaluation** for hot paths.
- **NUMA-aware thread placement**: workers pinned per NUMA node when configured.
- **Background SOW compaction**: free-space reclamation without blocking writers.
- **Batched fsync**: configurable commit interval trades durability for throughput.
- **Vectored I/O**: scatter-gather writes for journal append.
- **Designed scale**: hundreds of thousands of concurrent subscriptions per instance on commodity Linux hardware.

### Rust Implementation Notes

- Use `crossbeam-channel` or `flume` for hot outbound queues.
- Pin tokio workers to cores via `core_affinity`.
- For SOW shards, use `parking_lot::RwLock` (lighter than `std::sync::RwLock`).
- Optional `mimalloc` or `jemalloc` global allocator.
- Profile with `pprof-rs` for CPU; `heaptrack` for memory; `tokio-console` for async stalls.
- Lock-free where it pays: SPSC ring buffers for per-subscription outbound queues are typically the biggest win.

### Tests

| Layer | Test |
|---|---|
| Benchmark | Single publisher, 64-byte JSON: ≥ 1M msg/sec to a no-op subscriber |
| Benchmark | 100k concurrent subscriptions, 1 msg/sec each: p99 publish-to-deliver latency ≤ 10 ms |
| Benchmark | Sustained 24h soak at 50% rated throughput; no memory growth, no fsync stalls |
| Benchmark | SOW snapshot of 1M rows with indexed predicate ≤ 100 ms |
| Benchmark | Filter eval ≤ 500 ns for 5-predicate filter |
| Regression | Every benchmark tracked in CI with ±5% guard rails; degradations block merges |
| Profiling | Periodic flamegraph capture under load; no single function > 30% of CPU |

---

## 17. Cross-Cutting Rust Implementation Notes

### Crate selection

| Concern | Crate |
|---|---|
| Async runtime | `tokio` |
| Channels | `tokio::sync::mpsc`, `flume`, `crossbeam-channel` |
| Locks | `parking_lot` |
| Byte buffers | `bytes` |
| Memory map | `memmap2` |
| Serialization | `serde`, `serde_json`, `bson`, `rmp-serde` (MessagePack), `bincode` |
| Codec layer | `tokio_util::codec` |
| TLS | `tokio-rustls` |
| WebSocket | `tokio-tungstenite` |
| HTTP admin | `axum` |
| Logging | `tracing`, `tracing-subscriber` |
| Metrics | `prometheus`, `metrics-rs` |
| Config | `serde` over YAML/TOML, `config` crate |
| Errors | `thiserror`, `anyhow` |
| Lexing | `logos` |
| Concurrency testing | `loom` |
| Property testing | `proptest`, `quickcheck` |
| Fuzzing | `cargo-fuzz`, `arbitrary` |
| Benchmarking | `criterion`, `iai` |
| Concurrent maps | `dashmap` |
| Crash recovery store | `sled`, or custom WAL |
| JIT (optional) | `cranelift` |

### Workspace layout

```
amps-rs/
├── crates/
│   ├── protocol/        # frame codec, command enums, type lattice
│   ├── codec-json/      # JSON message codec
│   ├── codec-bson/      # BSON message codec
│   ├── codec-fix/       # FIX message codec
│   ├── filter/          # filter parser, IR, VM
│   ├── sow/             # state-of-the-world store + indexes
│   ├── txlog/           # transaction log + bookmark replay
│   ├── view/            # incremental view engine
│   ├── queue/           # message queues
│   ├── replication/     # replication legs + HA
│   ├── auth/            # auth + entitlements
│   ├── transport-tcp/   # TCP/TLS server transport
│   ├── transport-ws/    # WebSocket transport
│   ├── transport-shm/   # shared-memory transport
│   ├── admin/           # admin HTTP + metrics
│   ├── server/          # binary: composes all of the above
│   ├── client/          # Rust client SDK
│   └── client-stores/   # PublishStore, BookmarkStore
└── xtask/               # build/test/bench orchestration
```

### Design invariants worth encoding as type-level guarantees

- A `Bookmark` is a newtype, never a raw tuple — prevents accidental swapping.
- A `TopicId` is interned; the registry returns a `TopicId`, never a `String`, to the hot path.
- A `Subscription` holds a `tokio::sync::mpsc::Sender<Frame>` — bounded; cannot be made unbounded.
- A `PersistedAck` is only constructible by the tx-log writer after a successful fsync.
- Auth identity flows through the call stack as a function argument, never via thread-local.

---

## 18. Comprehensive Test Strategy

This section is the cross-cutting test plan — categories that apply across feature areas.

### 18.1 Unit Tests (`cargo test`)

For every public function in every crate:

- Happy path.
- Boundary conditions (empty input, max-size input, single element, off-by-one).
- Error path (every documented error variant constructible by some input).
- Type-system invariants (e.g. `Bookmark::compare` total).

Coverage target: ≥ 90% line coverage on `protocol`, `filter`, `sow`, `txlog` crates. Use `cargo-llvm-cov`.

### 18.2 Integration Tests

Per-crate `tests/` directory with multi-component scenarios:

- Embed the server in-process; run a client against it.
- Each user-visible feature has at least one end-to-end test.
- Use `testcontainers-rs` if you need to test against external systems (e.g. an LDAP for the auth module).
- Tests must clean up sockets, files, ports — use `tempfile` for state directories.

Naming convention: `tests/it_<area>_<scenario>.rs`.

### 18.3 Property-Based Tests (`proptest`)

For every algorithm whose correctness is expressible as a property:

- Filter evaluator: `for all (filter, doc): jit_eval(filter, doc) == interpreter_eval(filter, doc)`.
- SOW vs reference `HashMap`: `for all sequence of upserts/deletes: sow_state == hashmap_state`.
- View vs from-scratch recompute: `for all sequence of updates: view_aggregates == groupby_recompute`.
- Bookmark replay: `for all (publish_history, replay_point): replayed_messages == filter(history, bookmark > replay_point)`.
- Conflation: `for all updates: conflated_output[k].timestamp == max(updates[k].timestamp)`.

Property tests should run with `PROPTEST_CASES=10000` in nightly CI and `PROPTEST_CASES=256` in PR CI.

### 18.4 Concurrency Tests (`loom`)

For lock-free or fine-grained-locking code paths:

- `sow_and_subscribe` atomicity: no interleaving causes gap or duplicate.
- Outbound queue: no reordering of single-publisher messages.
- Replication leg sync-ack barrier: ack never released before all sync legs confirm.
- Lease expiry vs ack race in queues: no double-delivery on the boundary.

Loom tests are exhaustive across permitted interleavings; keep them small (≤ 100 lines, ≤ 4 threads).

### 18.5 Fuzz Tests (`cargo fuzz`)

For every parser and protocol surface:

- `fuzz_filter_parser`: random strings → must parse-error cleanly or evaluate without panicking.
- `fuzz_frame_decoder`: random byte sequences → no panics, no unbounded allocation.
- `fuzz_json_codec`: random bytes → parse cleanly or error.
- `fuzz_config_loader`: random YAML/TOML → parse cleanly or error.
- `fuzz_filter_eval`: random (filter, document) → no panics, deterministic result.

Run for hours in nightly CI; corpus checked into the repo and grown over time.

### 18.6 Chaos / Fault Injection

For HA and replication:

- Network: inject latency, packet loss, partitions using `tc-netem` or in-process bridge.
- Disk: simulate ENOSPC, EIO via a shim file system layer.
- Process: kill -9 at random points; verify recovery invariants.
- Clock: skew the clock forward/backward; verify lease and expiry behavior.

Framework: use a custom harness or `madsim` for deterministic simulation.

Required invariants under chaos:

1. **No data loss**: every `persisted`-acked message is recoverable after any single-node failure.
2. **No duplication**: subscribers with a `BookmarkStore` never see a message twice.
3. **No silent partition divergence**: after partition heal, replicas converge to identical state.
4. **No stuck queues**: any in-flight queue message is eventually delivered or DLQ'd.

### 18.7 Crash Recovery Tests

For every persistent subsystem (tx-log, SOW, queue state, client stores):

- Crash after every observable step in a write path (parse, write, sync, ack) — verify recovery.
- Inject torn writes at the journal tail — verify truncation on recovery.
- Inject corrupted blocks mid-file — verify detection (CRC) and graceful error.
- Verify SOW + tx-log consistency on every recovery: SOW state == replayed tx-log state.

### 18.8 Conformance / Compatibility Tests

- A test corpus of canonical messages, configs, and command sequences.
- Run the full corpus against every release; track diffs.
- For protocol versions, run an N×N matrix of `client_v_i` against `server_v_j` for all supported versions.

### 18.9 Performance / Load Tests

- `criterion` micro-benchmarks for hot paths (filter eval, codec parse, SOW upsert).
- `iai` instruction-count benchmarks for noise-free regression detection.
- End-to-end load tests with a load-generator client:
  - Publish-only throughput.
  - Subscribe-only throughput.
  - Mixed pub/sub at varying ratios.
  - Snapshot query latency under load.
  - Sustained 24h soak.
- All benchmarks tracked in CI; ±5% regression guard rails on the headline numbers.

### 18.10 Long-Running Soak

- 24-hour and 7-day soak at 50% rated capacity.
- Inject periodic disconnects, slow consumers, journal rotations.
- Watch for: memory growth, file-descriptor leaks, latency drift, lock contention growth.

### 18.11 Security Tests

- Auth fuzzing: malformed credentials must never grant access and never panic.
- Entitlement boundary: every (identity, topic, action) tuple must be checked; missing check is a test failure.
- TLS: cipher suite negotiation, cert chain validation, SNI handling.
- Input size limits: oversized frames, oversized fields, oversized filter expressions all rejected with bounded resource use.

### 18.12 Determinism / Reproducibility Tests

- For an input event log, the server produces a byte-identical tx-log across runs (modulo timestamps).
- For a view definition + input log, the view's final SOW state is byte-identical.
- Required for any correctness debugging, post-mortem analysis, and replay-based testing.

### 18.13 CI Pipeline Composition

| Stage | Jobs | Time budget |
|---|---|---|
| Per-commit | fmt, clippy, unit, integration, doc-build | ≤ 10 min |
| Per-PR | adds: property (256 cases), benchmark smoke | ≤ 30 min |
| Nightly | adds: property (10k cases), fuzz (1h each), chaos (full suite), soak (1h) | ≤ 4 h |
| Weekly | adds: 24h soak, full compat matrix, full fuzz corpus | ≤ 30 h |

### 18.14 Test Data Management

- Golden corpora in `testdata/` per crate: canonical messages, configs, command sequences.
- Property-test failures auto-shrink and are written to `proptest-regressions/` and checked in.
- Fuzz corpora versioned and stored in a separate repo or LFS to avoid bloating the main checkout.
- Benchmark history stored as a time series (e.g. Prometheus + Grafana, or `cargo-criterion` JSON output archived).

---

## Appendix A — Feature Coverage Checklist

A binary checklist for tracking implementation progress. Each row is either fully implemented (with tests) or not.

```
[ ] 1.  Core engine: topics, commands, sequence numbers, acks
[ ] 2.  Codecs: JSON, BSON, MessagePack, FIX, NVFIX, XML, ProtoBuf
[ ] 3.  Filter parser, IR, VM
[ ] 4.  Filter index acceleration
[ ] 5.  SOW: upsert, delete, snapshot, atomic snapshot+subscribe
[ ] 6.  SOW: hash indexes, secondary indexes
[ ] 7.  SOW: TTL expiration
[ ] 8.  SOW: mmap persistence + recovery
[ ] 9.  Views: projection, grouping, filter, aggregates
[ ] 10. Views: joins
[ ] 11. Views: conflated emission
[ ] 12. Per-subscription aggregation
[ ] 13. Transaction log: append, rotation, archive
[ ] 14. Transaction log: compression
[ ] 15. Bookmark subscriptions: EPOCH, NOW, MOST_RECENT, explicit, timestamp
[ ] 16. Bookmark pause/resume
[ ] 17. Replication: sync, async
[ ] 18. Replication: per-destination filter & transform
[ ] 19. Replication: link downgrade/upgrade
[ ] 20. Replication: dedup across multi-path topologies
[ ] 21. HA client: ServerChooser, DelayStrategy, PublishStore, BookmarkStore
[ ] 22. Queues: definition, fairness, lease, redelivery, DLQ
[ ] 23. Queue replication & failover
[ ] 24. Delta publish
[ ] 25. Delta subscribe / send_keys
[ ] 26. OOF events (filter, delete, expired)
[ ] 27. Authentication: pluggable, password, mTLS, custom
[ ] 28. Entitlements: per-topic ACL, filter rewrite, projection rewrite
[ ] 29. Slow client: offlining, disconnect
[ ] 30. Conflation: interval, key
[ ] 31. Admin HTTP: stats, control, replication health
[ ] 32. Logging: per-target, filtered
[ ] 33. Config: load, validate, substitute
[ ] 34. Transports: TCP, TLS, WebSocket, shared memory
[ ] 35. Wire protocol: framing, versioning, heartbeats, compression
[ ] 36. Performance: per-CPU sharding, mmap, batched fsync, JIT (optional)
```

---

## Appendix B — Suggested Implementation Order

A staged path that gets you to a useful prototype quickly and a production system eventually.

**Phase 1 — Minimal pub/sub (week 1–2)**
- Protocol framing
- TCP transport
- One codec (JSON)
- In-memory pub/sub
- Subscribe with literal topic, no filter
- Tests: 1, 2 (JSON), 34 (TCP), 35 (basic)

**Phase 2 — Content filtering (week 3–4)**
- Filter parser → IR → interpreter
- Subscribe with filter
- Tests: 3

**Phase 3 — SOW (week 5–8)**
- In-memory SOW (no persistence yet)
- Upsert/delete
- `sow` snapshot
- `sow_and_subscribe`
- Group markers
- Tests: 5

**Phase 4 — Persistence (week 9–12)**
- Transaction log: append, rotation
- SOW mmap backing
- Crash recovery
- Bookmark subscriptions: EPOCH and NOW
- Tests: 8, 13, 15 (partial)

**Phase 5 — Views (week 13–16)**
- View engine: projection, grouping, filter
- Aggregates: SUM, COUNT, MIN, MAX, AVG
- Incremental maintenance
- Tests: 9, 11

**Phase 6 — Delta & OOF (week 17–18)**
- Delta publish/subscribe
- OOF events
- Tests: 24–26

**Phase 7 — Replication (week 19–24)**
- Async replication
- Sync replication
- Client failover (HAClient)
- Tests: 17, 18, 21

**Phase 8 — Queues (week 25–28)**
- Queue definition, delivery, lease
- DLQ
- Queue replication
- Tests: 22, 23

**Phase 9 — Security & ops (week 29–32)**
- Auth pluggable
- Entitlements with filter rewrite
- Admin HTTP
- Slow client management
- Tests: 27, 28, 29, 31

**Phase 10 — Hardening (week 33+)**
- Additional codecs
- WebSocket and shared-memory transports
- JIT filter eval
- Joins in views
- Long-running soak, fuzz, chaos suite
- Tests: remaining

---

*End of specification.*

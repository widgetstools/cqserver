# CQServer v1 Compliance Roadmap

Tracks gaps between [ARCHITECTURE.md](ARCHITECTURE.md) and the current
implementation. Tiered by user-visible impact, not by code size.

Source-of-truth audit: 2026-05-21.

---

## Tier 0 — Blocks the core value proposition

These items must land before CQServer can honestly call itself a continuous
query server. Without them, `sow_and_subscribe` ships a snapshot and falls
silent.

### T0-1. Wire delta delivery end-to-end
- **Problem.** [websocket.rs:417-434](crates/cq-transport/src/websocket.rs#L417-L434)
  `deliver_delta` is a `tracing::trace!` stub. Computed deltas are dropped.
- **Required.**
  1. Introduce a session registry: `Arc<DashMap<SubId, mpsc::UnboundedSender<String>>>`
     (or bounded — see T0-3) owned at the server level.
  2. Register each new sub_id in the registry at `handle_sow_and_subscribe` /
     `handle_subscribe`, and remove on unsubscribe + session drop.
  3. `deliver_delta` looks up the sender by `delta.subscription_id`, serializes
     `CqMessage::delta(...)`, sends.
- **Exit criteria.** Integration test: publish → second client receives an
  `add` delta within 10ms; updating the row produces an `update`; changing it
  out of the predicate produces a `remove`.

### T0-2. Implement `delta_subscribe` (changed-fields-only)
- **Problem.** Enum variant exists ([command.rs:18](crates/cq-protocol/src/command.rs#L18))
  but `handle_command` falls through to "Unsupported command".
- **Required.**
  1. Subscription engine tracks last-emitted values per (sub, row) for the
     projected columns. A `HashMap<(SubId, RowId), Vec<Value>>` keyed by
     subscription is the minimum; a row-version compare gates the diff.
  2. On `Update` delta type, emit only columns whose value changed since the
     last emission.
  3. Wire `Command::DeltaSubscribe` to the same path as `SowAndSubscribe`,
     with a `sparse_deltas: true` flag on `Subscription`.

### T0-3. Backpressure on the outbound side
- **Problem.** [websocket.rs:68](crates/cq-transport/src/websocket.rs#L68) uses
  `mpsc::unbounded_channel`. A slow consumer = unbounded memory growth.
- **Required.**
  1. Replace with bounded channel (default ~10k messages — make it config).
  2. On `try_send` failure, apply conflation policy: drop oldest, coalesce by
     key (latest wins per row), or disconnect after grace period. Default:
     coalesce-by-key with high-water mark.
  3. Expose `backlog_depth` per subscription via stats.
- **New file.** `crates/cq-transport/src/backpressure.rs` per spec.

### T0-4. Conflation
- **Problem.** `conflation_ms` is read from TOML and dropped on the floor.
- **Required.** New module `crates/cq-core/src/conflation.rs` per spec layout.
  Implement two strategies:
  - `interval(ms)`: timer-driven flush, coalesce same-key updates within window.
  - `max_backlog(n)`: triggered by T0-3's bounded channel high-water.
- **Integration.** Conflator sits between `SubscriptionEngine::evaluate_row`
  output and `deliver_delta`. Per-subscription instance.

---

## Tier 1 — Documented v1 features not yet present

### T1-1. Transaction log (real implementation)
- **Problem.** [writer.rs:21](crates/cq-txlog/src/writer.rs#L21) is a sequence
  counter that writes nothing. No `reader.rs`, `segment.rs`, `index.rs`.
- **Required.** Match the spec layout:
  - `writer.rs`: append `[length u32][crc32 u32][timestamp u64][topic str][key str][payload bytes]`
    to current segment file. Rotate at `segment_size` (256MB default).
    Three fsync policies: `none`, `every_write`, `interval(ms)`.
  - `segment.rs`: segment file naming (`000001.log`), discovery on startup,
    rotation, retention cleanup.
  - `reader.rs`: sequential replay from a given sequence number.
  - `index.rs`: sparse index — every Nth entry, `(sequence → file_offset)`.
- **Integration.** `Topic::upsert` calls `txlog.append(...)` when
  `config.persist` is true, *before* visibility to subscribers (for durability
  guarantee). Wire in startup recovery: `cq-server/src/main.rs` replays each
  persistent topic's log into the store before binding listeners.
- **Exit criteria.** Kill -9 the server mid-publish stream; on restart, SOW
  matches pre-crash state for persistent topics.

### T1-2. Admin HTTP API
- **Problem.** No `admin.rs`, no listener. Spec lists `transport.admin` on
  port 8085 as v1.
- **Required.** New `crates/cq-server/src/admin.rs`. Endpoints:
  - `GET /stats` — aggregate: connection count, topic count, total rows.
  - `GET /topics` — list of topics with per-topic stats (mirror `Topic::stats`).
  - `GET /topics/:name` — single topic detail.
  - `GET /metrics` — Prometheus exposition (see T1-3).
- **Stack.** `axum` (already a sibling of tokio) or hand-rolled with
  `hyper` — axum is simpler and matches the stated tokio-first ethos.

### T1-3. Metrics emission
- **Problem.** `metrics` + `metrics-exporter-prometheus` are declared deps but
  zero counters/histograms exist in code.
- **Required.** Instrument hot paths:
  - `cq.publish.count` (per topic) — counter
  - `cq.publish.latency` — histogram (lock acquire → delta computed)
  - `cq.subscriptions.active` — gauge
  - `cq.deltas.delivered` / `cq.deltas.dropped_backpressure` — counters
  - `cq.connections.active` — gauge
  - `cq.sow.query_latency` — histogram
- Initialize `PrometheusBuilder` in `main.rs`, expose under `/metrics` via T1-2.

### T1-4. Mutation channel + evaluator threads
- **Problem.** [topic.rs:163](crates/cq-core/src/topic.rs#L163) evaluates
  subscriptions inline under the writer's `RwLock`. As subscription count
  grows, publish latency grows linearly. Spec shows a channel-decoupled
  design.
- **Required.**
  1. After `store.append_row`/`update_row`, send `MutationEvent { row, version }`
     on `crossbeam_channel::Sender` owned by the topic.
  2. Spawn 1..N evaluator tasks per topic that consume events and call
     `sub_engine.evaluate_row` against a read snapshot (column data is
     stable for `row < row_count`).
  3. `Topic::upsert` becomes write-fast: lock, write store, push event,
     unlock. Evaluation happens off the critical path.
- **Caveat.** Requires `SubscriptionEngine` to be `Send + Sync` or sharded
  per evaluator. Simplest first pass: 1 evaluator per topic.

### T1-5. Schema discovery on first publish
- **Problem.** [main.rs:91-94](crates/cq-server/src/main.rs#L91-L94) returns
  `[_key: String]` for every auto-created topic. Useless for real payloads.
- **Required.** On first publish to a topic without a configured schema:
  flatten the JSON via `flatten::flatten`, infer types via
  `ColumnType::from_json`, build a `Schema`, install in topic. Subsequent
  publishes widen types via `ColumnType::widen` if needed. Document the
  policy: schema-on-first-write is permissive; schema-from-config is strict.

### T1-6. TCP transport command routing
- **Problem.** [tcp.rs:69-80](crates/cq-transport/src/tcp.rs#L69-L80) decodes
  a frame, ignores its command, returns a blank ack.
- **Required.** Either:
  - Lift the per-command handlers out of `websocket.rs` into a transport-
    agnostic router (the empty `router.rs` is the natural home), or
  - Inline the same dispatch into the TCP handler.
- **Recommendation.** Extract — it kills duplication and gives `router.rs`
  a reason to exist.

### T1-7. Heartbeat scheduling
- Server only acks inbound heartbeats. Spec says bidirectional. Add a per-
  session timer that pushes a heartbeat every N seconds and disconnects if
  no inbound traffic for 2×N.

### T1-8. TOP N + OOF deltas
- **Problem.** ORDER BY + LIMIT work for one-shot queries; the subscription
  engine maintains an unranked `RoaringBitmap` and never emits `OOF`.
- **Required.** When a subscription has `ORDER BY + LIMIT N`:
  - Maintain a sorted skiplist/BTreeSet of `(sort_key, row)` capped at N.
  - On mutation: if row enters top-N, emit `Add`; if it exits, emit `Oof`;
    if it's already in and re-ranks, emit `Update`.
- **Effort.** Substantial — defer until T0 + T1-1..T1-3 are in.

---

## Tier 2 — Correctness / robustness bugs surfaced by the audit

| # | Location | Issue | Fix |
|---|---|---|---|
| C-1 | [store.rs:343](crates/cq-core/src/store.rs#L343) | `update_row` treats `Value::Null` as "no change" — caller can't actually nullify a field | Add `Value::Unset` sentinel for "skip", reserve `Value::Null` for "set to null" |
| C-2 | [topic.rs:186-198](crates/cq-core/src/topic.rs#L186-L198) | `delete` keeps the row, just nulls columns + drops the key index. Re-publish of same key creates a new row, orphaning the dead one | Use a tombstone bitmap or a free-row list and reuse the slot |
| C-3 | [topic.rs:79-112 vs 115-137](crates/cq-core/src/topic.rs#L79-L137) | `compute_key` and `compute_key_from_row` differ in how they handle numeric keys — insert may key a row by `"123"`, lookup later by `None` | Unify both paths through one helper that handles all `Value` variants identically |
| C-4 | [websocket.rs:373-394](crates/cq-transport/src/websocket.rs#L373-L394) | `build_sql` concatenates `topic`/`filter`/`options` strings into SQL without escaping — quote or space in topic name breaks the parse | Quote the table name; pass `filter` through `sqlparser` first and reject malformed input before re-emitting |
| C-5 | [predicate.rs:116](crates/cq-core/src/predicate.rs#L116) | `EqDouble` uses `(v - value).abs() < f64::EPSILON`, which is wrong for non-near-unit magnitudes | Either bit-exact compare (the AMPS convention) or `f64::total_cmp == Equal`. Bit-exact is faster and matches "stored value equals literal" semantics |
| C-6 | [predicate.rs:225-242](crates/cq-core/src/predicate.rs#L225-L242) | LIKE → regex compiler escapes `_` correctly but doesn't honor SQL backslash-escape sequences (`\%`, `\_`) | Accept SQL standard escape with ESCAPE clause, or document the limitation |
| C-7 | [predicate.rs:283-285](crates/cq-core/src/predicate.rs#L283-L285) | Predicate compiler swallows `IS NULL` on numeric columns as `IsNull { col }`, but `matches` evaluates that against the schema type (good) — fine, leaving as audit note |
| C-8 | [websocket.rs:111-116](crates/cq-transport/src/websocket.rs#L111-L116) | Disconnect cleanup iterates every topic and calls `unsubscribe` per topic per sub_id (O(topics × subs)) | Use the session registry from T0-1 to look up the topic of each sub_id directly |

---

## Tier 3 — Cleanup / hygiene

- Remove the unused imports flagged by warnings in `cq-core`, `cq-transport`
  (see `cargo build` output).
- Delete the dead `compute_key_from_row` ([topic.rs:115](crates/cq-core/src/topic.rs#L115))
  or wire it in (it's the natural impl for C-3).
- The empty `router.rs` is dead weight until T1-6 lands.
- The unused `_schema` binding in [subscription.rs:191](crates/cq-core/src/subscription.rs#L191).
- `cq-protocol/src/serialization.rs` is in the spec but unused — drop from
  spec or stub for FIX/binary v2.

---

## Suggested order

1. **T0-1** (delta delivery) — fixes the most embarrassing functional gap.
   Tests for `sow_and_subscribe` continuous behavior become possible after this.
2. **T1-4** (mutation channel) — best done *with* T0-1; both touch the same
   path and refactoring once is cheaper than twice.
3. **T0-3** + **T0-4** (backpressure + conflation) — these are the
   prerequisites for any production load test, and they share types.
4. **T1-1** (txlog) — biggest chunk; can be parallelized with T1-2/T1-3.
5. **T1-2** + **T1-3** (admin + metrics) — small, high-leverage. Operationally
   you need these before T1-1's recovery story can be verified.
6. **T0-2** (`delta_subscribe`) and **T1-8** (OOF / TOP N) — feature
   completeness pass.
7. **T1-5** (schema discovery), **T1-6** (TCP routing), **T1-7** (heartbeat)
   — quality-of-life.
8. **Tier 2** correctness bugs — fold into whichever Tier 0/1 item touches
   the same file.

---

## Out of scope for v1 (per spec, included for tracking)

- `cq-replication` crate (v2: active-passive; v3: active-active)
- FIX / NVFIX serialization (v2)
- Binary serialization (v2)
- Message queues / competing consumers (v2)
- Entitlements / auth (`Logon` command) (v2)
- Client SDKs (v2)
- Bookmark / replay (v2)
- Aggregate (GROUP BY) subscriptions (v3)

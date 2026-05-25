# Query Guardrails Worklog

**Goal.** Detect and refuse queries that would degrade cqserver before
they consume server resources. Today the H1/H2/H4 work prevents
RAM exhaustion via byte-caps and queue-caps, but a developer can
still ship a query that quietly:

- Joins on a low-cardinality column and produces a near-Cartesian
  intermediate row set.
- Groups by a high-cardinality column and creates millions of
  group cells maintained incrementally.
- Pivots over a huge IN list and emits very wide rows.
- Subscribes to an unfiltered SOW on a large topic and starves the
  snapshot encoder semaphore for minutes.

cqserver should give the application developer **immediate, explicit
feedback** ("your query is estimated at 4 M rows — reduce filter
selectivity or contact ops to raise the limit") rather than silent
slow degradation.

**Why this matters.** AMPS-replacement positioning means engineers
on the consuming side will iterate on subscribe queries throughout
development. Without guardrails, the first they learn that
`GROUP BY tradeId` is a mistake is when their dashboard freezes in
production. With guardrails, they get a clear error at first
attempt against a dev instance.

**Scope guard.** This worklog covers ONLY:

- Static (parse-time) query rejections that need no I/O.
- Cardinality-based cost estimation using existing index stats.
- Subscribe-time enforcement of configurable limits.
- Runtime soft/hard caps on SOW row count and group cardinality.
- Per-user query budgets layered onto the entitlements system.
- Observability for cost overruns.

Out of scope:

- Full SQL cost optimizer (join-order selection, predicate
  pushdown, etc.). We are not building a query planner; we are
  building a defensive estimator.
- Dynamic per-query rate limiting / quota enforcement across
  multiple subscribers.
- Adaptive limits that respond to system load (would be a follow-up).
- Killing already-running queries (today's runtime caps abort
  cleanly at the next chunk boundary, no preemption).
- Cross-instance budget coordination (each instance enforces
  independently).

---

## Existing defenses (verified before scoping)

What's already in the tree on `msrv-1.78` that this worklog
builds on, NOT replaces:

- **H2 byte-cap snapshot fanout cache** (default 256 MB) — bounds
  RAM used by cached encoded snapshots. Evicts oldest first.
- **H1 outbound queue cap** (default 2048 frames) — bounds
  per-session buffer memory.
- **Snapshot encoder semaphore** (`CQSERVER_MAX_SNAPSHOT_ENCODERS`,
  default 4) — prevents N parallel encodings from saturating CPU.
- **Slow-consumer auto-disconnect** (S26) — drops subs that fall
  behind a configurable drop-per-sec threshold.
- **S21 disk spillover** — slow consumers spill to disk instead
  of dropping or unbounded RAM growth.
- **Parser-level rejection of arbitrary JOIN `ON` clauses** —
  only `INNER JOIN ... USING (col)` is currently accepted, so
  a true unconstrained Cartesian product can't be expressed.
- **Per-topic `expire_seconds`** — bounds row count via TTL.
- **Entitlements + per-user `row_filter`** — admin can already
  refuse `Subscribe` on a topic or force a per-user WHERE
  predicate.

This worklog adds *visibility and limits* on top of these — none
of the existing defenses are being touched.

---

## Threat model

What we're defending against, ordered by how easy it is to hit
accidentally:

| # | Threat | Today's symptom | What the developer should see instead |
|---|---|---|---|
| 1 | Unfiltered SOW on huge topic | 16 s subscribe time, encoder semaphore starvation | "Estimated 865 K rows / 430 MB exceeds `max_sow_estimated_bytes`" |
| 2 | `JOIN USING (low_cardinality_col)` | View materialization stores millions of joined rows | "Estimated join fanout 100× — reduce join cardinality" |
| 3 | `GROUP BY high_cardinality_col` | View memory grows unboundedly | "Estimated 865 K groups exceeds `max_group_estimated_cardinality`" |
| 4 | `PIVOT (...) FOR col IN (lit, lit, ...)` huge IN list | Wide rows × many subs = large fanout | "IN list of 500 exceeds `max_pivot_in_list_size`" |
| 5 | Dynamic PIVOT `FOR col IN ANY` on high-cardinality column | Discovers it's huge at run-time, then OOMs the view | Combined with #4 — cap distinct-value count at evaluation start |
| 6 | View on view on view chain | Incremental maintenance amplifies cost layer-by-layer | "View depth 4 exceeds `max_view_chain_depth`" |
| 7 | `SELECT *` on a wide row (300-col schema) | Bytes-per-row balloons | Soft-warn metric only — not always wrong |

---

## Sessions

### G1 — Parse-time validators (no I/O, deterministic rejection)

**Goal.** Catch the cheapest, most-obvious mistakes immediately at
SQL parse time. No index lookups, no cardinality estimates —
purely structural rules.

**Validators to add:**

- **PIVOT IN-list cap.** `PIVOT (...) FOR col IN (lit, ..., lit)`
  with more than `max_pivot_in_list_size` (config, default 100)
  literals → reject at parse time with a clear message.
- **Dedup-key GROUP BY.** `GROUP BY col` where `col` is one of
  the topic's `key_fields` AND no other group-by columns are
  present → reject (this is a no-op aggregate, the developer
  almost certainly meant something else).
- **View chain depth.** When loading config, walk the view
  dependency graph and reject chains deeper than
  `max_view_chain_depth` (config, default 3).
- **SELECT * on view source.** A view whose body is literally
  `SELECT * FROM source` — reject as pointless (subscribe to
  the source directly).

**Files touched:**
- `crates/cq-core/src/query.rs` — extend the parser to plumb
  through the limits and emit `QueryParseError::TooManyPivotValues`
  / `::DegenerateGroupBy` etc.
- `crates/cq-server/src/config.rs` — new `[query_limits]` block
  with conservative defaults.
- `crates/cq-server/src/main.rs` — load + plumb limits into the
  query parser path.

**Test plan:**
- Unit tests in `query.rs` for each validator: feed it a query
  that should fail, assert the error variant. Same query just
  under the limit → succeeds.
- Config tests: TOML missing `[query_limits]` → defaults applied.
- One e2e in `cq-e2e-tests` that boots a server with
  `max_pivot_in_list_size = 5`, issues a 10-value PIVOT, asserts
  the subscribe ACK is an error with the expected message.

**Definition of done:**
- All new validators have unit tests.
- Existing `query.rs` tests still pass (the validators only kick
  in when the limit is exceeded; defaults are permissive).
- `cargo test --workspace` green.
- One e2e exercises end-to-end rejection through the wire.

**Estimated effort:** ~1 day.

---

### G2 — Cost estimator + `/admin/explain` endpoint

**Goal.** Given a SQL query, return an *estimate* of how expensive
it will be to execute, without actually subscribing or
materializing anything. Builds on the existing
`index_columns` config + range/equality indexes.

**Estimator inputs:**
- Topic's current row count (already exposed via `/topics`).
- Schema (column types known).
- Available range/equality indexes (from `index_columns`).
- Distinct-value count per indexed column (cardinality stats —
  needs new accounting in `ColumnStore`; cheap to maintain on
  insert).

**Estimator outputs:**

```rust
pub struct QueryCostEstimate {
    pub estimated_source_rows: u64,         // after WHERE filter
    pub estimated_join_fanout_avg: Option<f64>, // for joins
    pub estimated_result_rows: u64,         // after GROUP BY / PIVOT
    pub estimated_result_bytes: u64,        // result_rows × bytes_per_row
    pub used_indexes: Vec<String>,          // which indexes the estimator consulted
    pub assumptions: Vec<String>,           // e.g., "no index on `book`, assumed full scan"
    pub confidence: ConfidenceLevel,        // Low / Medium / High
}
```

**New admin endpoint:**

```
POST /admin/explain
Content-Type: application/json
{
  "topic": "/positions",
  "sql": "SELECT book, SUM(unrealizedPnl) ... GROUP BY book"
}
```

Returns the `QueryCostEstimate` as JSON. Standard SQL-engine
operational tool; cqserver should have it for any non-trivial
deployment.

**Files touched:**
- `crates/cq-core/src/store.rs` — maintain per-indexed-column
  distinct-value count. Cheap to update on insert/update/delete.
- `crates/cq-core/src/query.rs` — new `estimate_cost()` function
  on `ParsedQuery` taking `&TopicRegistry`.
- `crates/cq-server/src/admin.rs` — new `POST /admin/explain`
  handler.

**Test plan:**
- Unit tests for the estimator with synthetic topics where the
  expected row count is known.
- e2e: spawn a server, load 1000 rows, issue various queries via
  `/admin/explain`, assert the estimate is within ±20% of actual.
- Cardinality-stats correctness test: insert 100 rows × 10
  distinct values for column X, assert
  `column_distinct_count("X") == 10`.

**Definition of done:**
- Estimator returns reasonable estimates for queries against
  the demo data (`/positions`, `/trades`).
- `/admin/explain` endpoint returns valid JSON.
- New unit + e2e tests pass.

**Estimated effort:** ~2-3 days.

---

### G3 — Subscribe-time enforcement using G2 estimates

**Goal.** Before opening a subscription, run G2's estimator
against the query. If any threshold is exceeded, reject the
subscribe with a clear error referencing the specific limit.
Otherwise proceed (optionally with a warning in the response).

**Flow:**

```
Client sends Subscribe(topic, sql)
    ↓
Server parses (G1 validators run here)
    ↓
Server estimates cost (G2)
    ↓
Compare against [query_limits] config:
  - estimated_result_rows  > max_sow_estimated_rows?     → reject
  - estimated_result_bytes > max_sow_estimated_bytes?    → reject
  - join_fanout_avg        > max_join_estimated_fanout?  → reject
  - group_cardinality      > max_group_estimated_cardinality? → reject
  - result_rows > warn_threshold?                        → warn + proceed
    ↓
If proceeding: include cost_estimate in the Subscribe ACK.
If rejecting: send Error with reason referencing the specific limit.
```

**New protocol field on the ACK:**

```json
{
  "command": "ack",
  "status": "ok" | "ok_with_warning" | "error",
  "sub_id": "...",
  "cost_estimate": {
    "estimated_result_rows": 1234,
    "estimated_result_bytes": 87654,
    "used_indexes": ["book"],
    "confidence": "high"
  },
  "warnings": ["estimate exceeds warn_sow_rows_threshold"],  // optional
  "reason": "estimated 4_000_000 rows exceeds max_sow_estimated_rows=1_000_000"  // when status=error
}
```

**Config block (in `cqserver.toml`):**

```toml
[query_limits]
# Hard rejection thresholds.
max_sow_estimated_rows           = 1_000_000
max_sow_estimated_bytes          = 100_000_000     # 100 MB
max_join_estimated_fanout        = 10              # avg right rows per USING value
max_group_estimated_cardinality  = 100_000

# Soft thresholds (log + metric, but proceed).
warn_sow_rows_threshold          = 100_000
warn_sow_bytes_threshold         = 10_000_000      # 10 MB

# Bypass for trusted admin clients (entitlement keyword).
# Users with this entitlement skip the checks entirely.
bypass_entitlement = "query_no_limits"
```

**Files touched:**
- `crates/cq-protocol/src/message.rs` — `CqMessage` gains
  optional `cost_estimate: Option<CostEstimate>` and
  `warnings: Vec<String>` fields. Backward-compatible via
  `serde(default)`.
- `crates/cq-transport/src/router.rs::handle_subscribe` — invoke
  the estimator, apply limits, populate the ACK.
- `crates/cq-server/src/config.rs` — `[query_limits]` block
  + `QueryLimits` struct passed through `RouterContext`.
- `crates/cq-server/src/main.rs` — wire it through.

**Test plan:**
- Unit tests: with limits set artificially low, subscribe to a
  query that would exceed → assert ACK is an Error with the
  specific reason string.
- Just under the limit → success with `cost_estimate` populated.
- Just over the soft threshold but under the hard limit → success
  with `warnings` populated.
- One e2e through `cq-client` validates the wire-level error
  text matches the limit name.

**Definition of done:**
- All four hard limits enforced.
- Two soft warnings (rows, bytes) emitted as metrics + log.
- `cost_estimate` returned on every successful subscribe.
- `cargo test --workspace` green.

**Estimated effort:** ~1-2 days.

---

### G4 — Runtime soft + hard limits + observability

**Goal.** Even with good pre-flight estimates, reality can
diverge (skewed data, estimator confidence too high). Add
runtime caps that abort cleanly at chunk boundaries when the
actual cost exceeds configured limits.

**Runtime caps:**

- **`hard_max_sow_result_rows`** (config, default 5 M) — SOW
  streaming aborts after this many rows are emitted with a
  clear error sent to the subscriber. The partial result is
  delivered; the subscription is then dropped.
- **`hard_max_sow_result_bytes`** (config) — same shape but
  byte-counted.
- **`max_group_cardinality_runtime`** (config) — when a view's
  group count exceeds this during materialization, the view
  stops adding new groups and emits a `cq_view_cardinality_capped_total`
  metric. Existing groups continue to update.
- **`subscribe_timeout_server_side`** (config, default 30 s) —
  if a SOW takes longer than this, abort. Today the encoder
  semaphore can hold a permit indefinitely.

**New metrics (Prometheus):**

- `cq_query_cost_estimate_rows{topic, sql_hash}` — histogram of
  estimated result rows per subscribe.
- `cq_query_cost_estimate_bytes{topic, sql_hash}` — same for bytes.
- `cq_query_actual_rows{topic, sql_hash}` — histogram of actual
  rows emitted. Operator can compare estimate-vs-actual.
- `cq_query_rejected_total{reason}` — count of subscribes
  rejected by limit, tagged by which limit fired.
- `cq_query_warned_total{reason}` — count of soft warnings.
- `cq_view_cardinality_capped_total{view}` — when a runtime
  group-cap fires.

**Files touched:**
- `crates/cq-transport/src/router.rs::deliver_streaming_snapshot` —
  track running row/byte count, abort on cap.
- `crates/cq-core/src/aggregate.rs` (or wherever views materialize)
  — track group count, refuse new groups at the cap.
- `crates/cq-server/src/admin.rs` — `/metrics` already wires
  Prometheus; new metrics need declaring.

**Test plan:**
- Unit test: trigger the SOW row cap with a synthetic topic
  whose row count is known. Verify the partial result + error
  delivery.
- Unit test: trigger the group-cardinality cap on a view; verify
  the metric advances and no new groups appear.
- e2e: run a SOW with the row cap set to 10, publish 100 rows,
  subscribe with `SELECT *`, assert the client receives exactly
  10 rows followed by an error frame.

**Definition of done:**
- All four runtime caps wired.
- All new metrics exposed under `/metrics`.
- Tests cover the abort path for at least the row + group caps.

**Estimated effort:** ~1-2 days.

---

### G5 — Per-user query budgets via entitlements

**Goal.** Different users get different limits. A trader power-user
might be allowed `max_sow_rows = 1 M`; a casual viewer is capped
at `max_sow_rows = 10 K`. The default user inherits the global
`[query_limits]` values; specific users override per-key.

**Config:**

```toml
[[auth.users]]
username = "trader-alice"
password_hash = "..."
entitlements = [{ topic = "/positions", op = "Subscribe" }, ...]
# New: per-user query budget overrides.
query_budget = { max_sow_estimated_rows = 1_000_000, max_join_estimated_fanout = 50 }

[[auth.users]]
username = "viewer-bob"
password_hash = "..."
entitlements = [{ topic = "/positions", op = "Subscribe" }]
query_budget = { max_sow_estimated_rows = 10_000 }   # tighter
```

Special entitlement keyword `query_no_limits` (already proposed
in G3) bypasses all checks — used for admin/ops users.

**Files touched:**
- `crates/cq-transport/src/auth.rs` — `User` struct gains
  `query_budget: Option<QueryBudget>`.
- `crates/cq-server/src/config.rs` — config parsing for the
  override.
- `crates/cq-transport/src/router.rs::handle_subscribe` — when
  the user has a budget, merge it with the global limits
  (tighter of the two wins) before running G3 checks.

**Test plan:**
- Unit tests for the merge logic.
- e2e: configure two users, one with a tight cap and one without.
  Subscribe the same query as both; the first gets rejected, the
  second succeeds.

**Definition of done:**
- Per-user budget overrides parsed from config.
- Merge logic tested.
- e2e validates differentiated behaviour.

**Estimated effort:** ~1 day.

---

## Order of execution

G1 → G2 → G3 → G4 → G5. Strict serial because:
- G2 depends on G1's parse tree (validators run before estimation).
- G3 uses G2's estimator.
- G4 reuses G3's config block but adds runtime-side enforcement.
- G5 layers user-specific overrides on top of G3.

G1 alone is independently shippable and useful — it catches the
worst foot-guns at zero runtime cost. G4 alone (runtime caps
without estimation) is also independently useful as a backstop.

## Status

| # | Session | Status |
|---|---|---|
| G1 | Parse-time validators | ✅ done — `QueryLimits` + `validate_with_limits` + `validate_view_graph` in cq-core; `[query_limits]` block in `cqserver.toml`; wired through `WsConfig`/`TcpConfig` to `RouterContext` (ready for G3); view-graph validation runs at server startup. 10 unit tests in `query::tests::g1_*`. |
| G2 | Cost estimator + `/admin/explain` | ⏳ pending |
| G3 | Subscribe-time enforcement | ⏳ pending |
| G4 | Runtime caps + observability | ⏳ pending |
| G5 | Per-user query budgets | ⏳ pending |

(Update this table at the end of each session.)

## Related worklogs

- `HIGH_SCALE_WORKLOG.md` — H1/H2/H4 RAM + queue protections this
  worklog builds on.
- `REPLICA_READS_WORKLOG.md` — replica-reads roadmap; the
  guardrails apply equally on followers (each follower runs the
  estimator independently).

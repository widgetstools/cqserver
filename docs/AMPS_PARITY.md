# AMPS Feature Parity — cqserver Gap Analysis

This document catalogues what 60East AMPS supports versus what cqserver supports today, based on the limitations we hit while wiring the Atlas demo (ex01–ex08). The goal is a clear-eyed punch list — what we'd need to build to reach AMPS parity, and which gaps the Atlas demo currently has to work around.

> AMPS = the commercial Advanced Message Processing System from 60East Technologies. cqserver is an OSS messaging server modeled on AMPS's wire protocol and feature shape.

---

## 1. SQL surface — what AMPS supports

AMPS exposes a SQL-92-flavoured **content-filtering and projection language** that runs against streaming topics. The same SQL is reused for SOW queries, subscription filters, aggregates, and views.

### 1.1 SELECT-list features

| Feature | AMPS | cqserver | Notes |
|---|---|---|---|
| Bare column projection | ✓ | ✓ | `SELECT a, b FROM t` |
| `SELECT *` | ✓ | ✓ | |
| Column aliases (`AS`) | ✓ | ✓ | |
| Table aliases (`FROM t alias`) | ✓ | ✗ | cqserver's parser has no alias-resolution table; demo must strip `alias.col` → `col` client-side |
| Qualified column refs (`p.col`) | ✓ | ✗ | Same reason as above |
| Scalar functions (`ABS`, `UPPER`, `SUBSTR`, etc.) | ✓ (≈80) | partial | cqserver supports a small handful in WHERE; SELECT-list scalars are spotty |
| Arithmetic in SELECT (`a + b`, `a * b`) | ✓ | ✗ | Must be pre-computed on the publisher (Atlas does this for `mv_x_pct`) |
| `DISTINCT` | ✓ | ✗ | |
| `COUNT(DISTINCT col)` | ✓ | ✗ | |

### 1.2 WHERE clause

| Feature | AMPS | cqserver |
|---|---|---|
| Comparison ops `= <> < <= > >= ` | ✓ | ✓ |
| `AND` / `OR` / `NOT` | ✓ | ✓ |
| `IN (…)` | ✓ | ✓ |
| `BETWEEN … AND …` | ✓ | ✓ |
| `LIKE` with `%` / `_` | ✓ | ✓ |
| `IS NULL` / `IS NOT NULL` | ✓ | ✓ |
| Regex match (`MATCHES_REGEX`, `LIKE_REGEX`) | ✓ | ✗ |
| Nested/dotted paths (`risk.duration > 10`) | ✓ | ✓ (via schema_file) |
| Bound parameters | ✓ | ✗ |

### 1.3 Aggregation & GROUP BY

| Feature | AMPS | cqserver |
|---|---|---|
| `GROUP BY col1, col2, …` | ✓ | ✓ |
| `SUM`, `COUNT(*)`, `COUNT(col)`, `AVG`, `MIN`, `MAX` | ✓ | ✓ |
| `STDDEV` / `STDDEV_SAMP` / `VARIANCE` | ✓ | ✗ |
| `PERCENTILE_CONT`, `PERCENTILE_DISC`, `MEDIAN` | ✓ | ✗ |
| `STRING_AGG` / `GROUP_CONCAT` | ✓ | ✗ |
| `HAVING` | ✓ | ✗ |
| Aggregates over expressions (`SUM(a * b)`) | ✓ | ✗ |
| Degenerate aggregate (`SELECT SUM(x) FROM t`, no GROUP BY) | ✓ | ⚠ topic stores 1 row by empty key but SOW iterator returns nothing reliably |

### 1.4 JOIN

| Feature | AMPS | cqserver |
|---|---|---|
| `INNER JOIN … ON …` | ✓ | ✗ |
| `INNER JOIN … USING (col, …)` | ✓ | ✓ |
| `LEFT OUTER JOIN` | ✓ | ✗ |
| `RIGHT OUTER JOIN` | ✓ | ✗ |
| `FULL OUTER JOIN` | ✓ | ✗ |
| Multi-key join | ✓ | ✓ (multi-column USING) |
| `AS OF JOIN` (temporal) | ✓ | ✗ |
| Broadcast / hash hint | ✓ | ✗ |
| JOIN in ad-hoc SOW (not just view) | ✓ | ✓ (added 2026-05-26) |
| JOIN view (declared in config) | ✓ | ✓ |

### 1.5 ORDER BY / LIMIT / OFFSET

| Feature | AMPS | cqserver |
|---|---|---|
| `ORDER BY col ASC/DESC` | ✓ | ✓ |
| `ORDER BY <select-alias>` matching a base column | ✓ | ✓ |
| `ORDER BY <select-alias>` NOT matching a base column | ✓ | ✗ **hangs the SOW encoder** |
| `ORDER BY <function expression>` (e.g. `ABS(x)`) | ✓ | ✗ |
| `LIMIT n` / `TOP n` | ✓ | ✓ |
| `OFFSET n` | ✓ | ✗ |

### 1.6 Pivot / Unpivot

| Feature | AMPS | cqserver |
|---|---|---|
| `PIVOT(col IN (val1, val2, …))` | ✓ | ✓ (per `query.rs` PIVOT parser) but limited |
| `UNPIVOT` | ✓ | partial |
| Pivot with dynamic value list | ✓ | ✗ |
| Pivot as view | ✓ | ⚠ untested in cqserver |

### 1.7 Window functions

| Feature | AMPS | cqserver |
|---|---|---|
| `OVER (PARTITION BY … ORDER BY …)` | ✓ | ✗ |
| `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` | ✓ | ✗ |
| `ROWS BETWEEN n PRECEDING AND CURRENT ROW` | ✓ | ✗ |
| Rolling aggregates over a window | ✓ | ✗ |

### 1.8 Subqueries & CTEs

| Feature | AMPS | cqserver |
|---|---|---|
| `WHERE col IN (SELECT …)` | ✓ | ✗ |
| `EXISTS (SELECT …)` | ✓ | ✗ |
| Scalar subqueries in SELECT | ✓ | ✗ |
| CTEs (`WITH x AS …`) | ✓ | ✗ |

---

## 2. Topic & data-model features

| Feature | AMPS | cqserver | Notes |
|---|---|---|---|
| SOW (state-of-the-world) topic | ✓ | ✓ | |
| `conflation_ms` server-side conflation | ✓ | ✓ | |
| TTL expiry (`expire_seconds`) | ✓ | ✓ | |
| Persistence to disk | ✓ | ✓ | txlog |
| Replication (master/replica) | ✓ | partial | `cq_replication` crate exists |
| Sharding | ✓ | partial | `shards` config exists |
| Queue topics (point-to-point) | ✓ | ✓ | per `cq_transport::queue` |
| Materialized views | ✓ | ✓ | |
| View-on-view (layered views) | ✓ | ✗ | cqserver views must source from raw topics |
| Schema discovery from first publish | ✓ | ✓ | |
| Schema declaration via JSON file | ✓ | ✓ | |
| Schema evolution (online add column) | ✓ | ✗ | |
| Native column types | Null, Bool, Int, Long, Double, String, Bytes, Timestamp, Array, Object | Null, Bool, Int, Long, Double, String, Timestamp | Missing: Bytes, Array, Object as first-class (only via dotted paths) |
| Indexed columns (secondary index) | ✓ | ✓ | `index_columns` |
| Bookmarks per-subscriber | ✓ | ✓ | `bm` field |
| Out-of-focus (OOF) on filter exit | ✓ | ✓ | |
| Delta publishing (partial-row update) | ✓ | ✓ | `delta_publish` |
| Server-side filtering before egress | ✓ | ✓ | |

---

## 3. AMPS client SDK — full feature catalogue

This is the **C++/Java/Python/.NET/JS surface** AMPS publishes. Each row is either supported by our `@cqserver/client` TS SDK, partially supported, or not supported.

### 3.1 Connection

| Feature | AMPS | cqserver SDK |
|---|---|---|
| WebSocket transport | ✓ | ✓ |
| TCP transport | ✓ | ✓ |
| TLS / SSL | ✓ | ✗ |
| Connection name (echoed in logs) | ✓ | ✗ |
| Auto-reconnect with backoff | ✓ | ✓ |
| `connect()` / `connectAsync()` | ✓ | ✓ |
| Heartbeats (configurable interval) | ✓ | ✓ |
| Server-side idle timeout | ✓ | ✓ |
| Connection failure handler / observer | ✓ | partial (`onClose`) |
| Logon / authentication | ✓ | ✓ (`logon`) |
| Resume after disconnect with bookmark | ✓ | ⚠ partial — bookmarks accepted but the resume flow isn't end-to-end tested |
| HA / failover across multiple URIs | ✓ | ✗ |

### 3.2 Publish

| Feature | AMPS | cqserver SDK |
|---|---|---|
| `publish(topic, data)` | ✓ | ✓ |
| Awaited ack with sequence number | ✓ | ✓ |
| Fire-and-forget no-ack | ✓ | ✓ (`publishUnacked`, added today) |
| Batched publish | ✓ | ✗ |
| Compressed publish | ✓ | ✗ |
| Persisted publish (durable) | ✓ | partial |
| Delta publish (partial update) | ✓ | ✗ at the SDK level |
| Bookmark-based pub-on-connect (idempotent) | ✓ | ✗ |
| Publish to queue | ✓ | ✓ |

### 3.3 Subscribe

| Feature | AMPS | cqserver SDK |
|---|---|---|
| `subscribe(topic)` — live deltas only | ✓ | ✓ |
| `sow(topic)` — one-shot snapshot | ✓ | ✓ |
| `sowAndSubscribe(topic)` | ✓ | ✓ |
| `deltaSubscribe(topic)` | ✓ | ✓ |
| Server-side filter `filter` | ✓ | ✓ |
| Server-side projection / column select | ✓ | partial |
| Server-side aggregation in sub | ✓ | partial (via view, not ad-hoc) |
| Server-side `top_n` / `order_by` | ✓ | partial |
| Server-side conflation | ✓ | ✓ |
| Bookmark resume | ✓ | ✓ |
| Timestamp seek (`replay_from`) | ✓ | ✓ (`replay_from` field exists) |
| OOF (out-of-focus) handling | ✓ | ✓ |
| Async iterator over deltas | (Java/Python idiomatic) | ✓ |
| Pause / resume subscription | ✓ | ✗ |
| Atomic multi-topic subscribe | ✓ | ✗ |

### 3.4 SOW deletion / mutation

| Feature | AMPS | cqserver SDK |
|---|---|---|
| `sow_delete(topic, filter)` | ✓ | ✓ |
| `sow_delete_by_data` | ✓ | ✗ |
| `sow_delete_by_keys` | ✓ | ✗ |

### 3.5 Queue / messaging primitives

| Feature | AMPS | cqserver SDK |
|---|---|---|
| Queue subscribe | ✓ | ✓ |
| Manual acknowledgement | ✓ | ⚠ partial — server has the queue, ack roundtrip not fully wired |
| `max_in_flight` | ✓ | ✗ |
| Lease / requeue on disconnect | ✓ | partial |

### 3.6 Operational features

| Feature | AMPS | cqserver SDK |
|---|---|---|
| Per-client metrics | ✓ | partial |
| Client logging hooks | ✓ | ✗ |
| Trace-id propagation | ✓ | ✗ |
| Heartbeat callbacks | ✓ | ✗ |
| Stats RPC | ✓ | ✗ |
| Admin RPC (rotate journal, shrink store) | ✓ | ✓ (HTTP, not SDK) |

---

## 3.7 SOW caps & slow-consumer capacity (aligned 2026-05-30)

AMPS imposes **no size or cost cap** on a SOW query — it streams the entire
result and protects the instance purely through slow-consumer capacity
management: it offlines a backed-up client's messages to disk, then
**disconnects** the client when the disk cushion (`MessageDiskLimit`) is
exhausted. It never rejects a query pre-flight on size, and never silently
desyncs a subscriber.

cqserver now matches this:

| Concern | AMPS | cqserver |
|---|---|---|
| Hard cap on SOW result rows/bytes | none | `hard_max_sow_result_rows/_bytes` **default 0 = disabled**; opt-in only |
| Pre-flight estimate rejection | none | `max_sow_estimated_*` (G3) **default 0 = disabled**; opt-in only |
| Memory full → offline to disk | `MessageMemoryLimit` | per-sub `outbound_queue_capacity` → `[transport.spillover]` |
| Disk cushion full → **disconnect** | `MessageDiskLimit` | spillover over-cap → connection closed (`cq_slow_consumer_disconnect_total`) |
| Client-bounded result | `top_n` / `skip_n` | SQL `LIMIT` / `OFFSET` + TopN subs |
| Per-client capacity share | `ClientMaxCapacity` (50%) | ✗ (per-sub queue only) — open gap |

Notes:
- The G1 **structural** guardrails (PIVOT IN-list size, view-chain depth,
  degenerate GROUP BY, pass-through views) are query *validity* checks, not
  egress caps, and keep their protective defaults.
- The disk-full→disconnect parity applies to routes **with** spillover
  configured. Without spillover (no disk cushion) and for conflated routes,
  overflow falls back to a best-effort drop + `degraded` flag (the
  slow-consumer watcher can force-resync). **For full parity, configure
  `[transport.spillover]`.**
- Earlier same-day fix: an as-of/historical SOW truncated to zero rows when
  `hard_max_sow_result_rows = 0` (the "disabled" value) — now gated on
  `cap > 0`, matching the live-streaming path.

---

## 4. Known cqserver bugs (server-side, not parser)

Hit while wiring Atlas:

1. **JOIN view SOW delivery** — `[[views]]`-declared JOIN views (e.g. `/v_trades_by_compliance`) populate correctly (admin shows 3 rows) but a fresh subscriber's SOW returns 0 rows. Non-JOIN views with the same group key (`/v_compliance_counts`) work fine. Likely in the view-topic SOW iterator under continuous re-aggregation.
2. **ORDER BY alias hang** — `SELECT col, SUM(x) AS y FROM t GROUP BY col ORDER BY y` hangs the SOW encoder when `y` is a SELECT-list alias that doesn't match a base column. Same query with `ORDER BY <base column>` returns instantly.
3. **Degenerate aggregate SOW** — `SELECT SUM(x) FROM t` (no GROUP BY) creates a topic that grows by one "row" per refresh instead of upserting the single empty-key row. Workaround: roll up on the client from a GROUP BY view with a tiny key cardinality.
4. **Snapshot encode cache stickiness** — a SOW request that fails server-side (e.g. parser bug) can leave its `Building` slot in the encode-once-fanout cache, so the next identical request waits forever. Bouncing the server clears it.
5. **Slash-prefix asymmetry** — topics register as `/positions`, JOIN view config writes `FROM positions`. We patched `init_view` and the SOW JOIN resolver to try both forms; the rest of the registry should normalise too.

---

## 5. Honest assessment

cqserver covers the **filter + project + GROUP BY + simple JOIN + materialized view** core well enough for the Atlas demo to land, but it's far from AMPS-parity:

- **Parser**: no table aliases, no qualified column refs, no scalar expressions in SELECT, no arithmetic in aggregates, no HAVING, no OFFSET, no functions in ORDER BY, no window functions, no subqueries.
- **JOIN**: only `INNER JOIN … USING (col)`. No `ON`, no `LEFT/RIGHT/FULL OUTER`, no temporal `AS OF`.
- **Aggregates**: only `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`. No `STDDEV`, no percentiles, no distinct-count.
- **Engine**: degenerate-group aggregates and JOIN-view SOW delivery are both buggy under load; the encode cache can wedge.
- **Operationally**: TLS, batched publish, HA failover across URIs, pause/resume subscriptions, and atomic multi-topic subscribe are all missing.

For the Atlas demo we work around these by:
- Pre-computing arithmetic columns on the publisher (`mv_x_pct`, `mv_abs`)
- Rolling up 8-row aggregate views on the client to derive grand totals
- Stripping table aliases client-side (`stripAliases` in ex08)
- Declaring `[[views]]` for every server-side aggregate the demo needs

If we want the Query Builder tab to faithfully demonstrate "arbitrary SQL works against cqserver", the realistic roadmap is roughly:

1. **Parser** — add table-alias resolution, qualified column refs, scalar expressions, `HAVING`. Highest ROI.
2. **JOIN** — support `ON a = b` syntactically (translate to USING when sides match), then `LEFT OUTER`.
3. **Engine** — fix the degenerate-aggregate SOW path and the JOIN-view SOW delivery race.
4. **Aggregates** — add `STDDEV` and `PERCENTILE_CONT` to round out the trading-floor demo's slippage panel.
5. **SDK** — TLS, batched publish, pause/resume, HA failover.

Numbers 1 and 3 unlock most of the demo library's queries. Numbers 2, 4, 5 are AMPS-parity polish.

# cqserver — client SDK guide

**How to connect, authenticate, publish, and — above all — run every
kind of query cqserver supports, from each client SDK we ship.**

This is the companion to [`USER_GUIDE.md`](USER_GUIDE.md). The user
guide is operator-focused (install, config, persistence, replication,
ops). *This* document is application-developer focused: it is the
single place to learn **what query shapes exist, which API call runs
each one, and exactly how each SDK expresses it.**

SDKs covered:

| SDK | Path | Transport | Concurrency model |
|---|---|---|---|
| **Rust** | `crates/cq-client` | TCP, WebSocket, TLS | `async` / Tokio |
| **TypeScript** | `client-sdks/ts` (`@cqserver/client`) | TCP (Node), WS (browser + Node), TLS | `async` / Promises |
| **Python** | `client-sdks/python` (`cqclient`) | TCP | threaded / blocking |
| **Go** | `client-sdks/go` (`cqclient`) | TCP | goroutine + channels |
| **Java** | `client-sdks/java` (`io.cqserver.client`) | TCP | thread + blocking |

> The Rust and TypeScript SDKs are the reference implementations and
> expose the **full** surface (continuous SQL aggregates, replay modes,
> bookmark stores, queue acks, batched publish). The Python, Go, and
> Java SDKs are dependency-free "core" clients: connect, auth, publish,
> one-shot SOW (incl. SQL), and live filtered subscribe. The
> [capability matrix](#12-per-sdk-capability-matrix) at the end says
> precisely which call exists where.

---

## Table of contents

1. [The mental model: three ways to run a query](#1-the-mental-model-three-ways-to-run-a-query)
2. [Choosing a query mode (decision tree)](#2-choosing-a-query-mode-decision-tree)
3. [Connecting and authenticating](#3-connecting-and-authenticating)
4. [Query kind 1 — one-shot filtered snapshot (`sow`)](#4-query-kind-1--one-shot-filtered-snapshot-sow)
5. [Query kind 2 — one-shot SQL: aggregates, GROUP BY, PIVOT, JOIN (`sow_sql`)](#5-query-kind-2--one-shot-sql-aggregates-group-by-pivot-join-sow_sql)
6. [Query kind 3 — historical / as-of SOW](#6-query-kind-3--historical--as-of-sow)
7. [Query kind 4 — live filtered subscription (`sow_and_subscribe`)](#7-query-kind-4--live-filtered-subscription-sow_and_subscribe)
8. [Query kind 5 — continuous SQL aggregates (`sow_and_subscribe_sql`)](#8-query-kind-5--continuous-sql-aggregates-sow_and_subscribe_sql)
9. [Query kind 6 — live joins via materialized views](#9-query-kind-6--live-joins-via-materialized-views)
10. [Reading the delta stream](#10-reading-the-delta-stream)
11. [Replay and resume (bookmark / timestamp / most-recent)](#11-replay-and-resume-bookmark--timestamp--most-recent)
12. [Per-SDK capability matrix](#12-per-sdk-capability-matrix)
13. [Filter and SQL language reference](#13-filter-and-sql-language-reference)
14. [Patterns, gotchas, and performance](#14-patterns-gotchas-and-performance)

---

## 1. The mental model: three ways to run a query

cqserver is a stateful pub/sub database: every topic holds the current
**state-of-the-world (SOW)** — one row per key — in memory, and
evaluates queries continuously against mutations. Before you write a
line of SDK code, internalize this: **there is no single "query" call.
There are three distinct execution modes, and you pick one by choosing
a different SDK method.** The server does *not* infer the mode from
your SQL — the wire command you send *is* the mode.

| Mode | SDK methods | Returns | JOIN? | GROUP BY? | Live updates? |
|---|---|---|---|---|---|
| **A. One-shot SOW** | `sow`, `sow_sql`, `sow_as_of_*` | a finite list of rows, then done | ✅ yes | ✅ yes | ❌ no |
| **B. Live subscription** | `subscribe`, `sow_and_subscribe`, `sow_and_subscribe_sql`, `delta_subscribe` | a snapshot (optional) then an endless delta stream | ❌ no | ✅ yes (aggregates) | ✅ yes |
| **C. Materialized view** | (server config) + `sow_and_subscribe` on the view topic | same as B, but the server pre-computes the join/aggregate | ✅ yes (view-only) | ✅ yes | ✅ yes |

The single most important consequence:

> **A JOIN can only be evaluated in Mode A (one-shot) or Mode C
> (a pre-registered view). The live ad-hoc subscription path (Mode B)
> binds one topic's schema at a time and rejects multi-topic column
> references.** If you `sow_and_subscribe` a query that references two
> topics you will get an error like `Unknown column: trade_id`. To get
> *live* joined data, register a view (§9).

Why: the live evaluator matches each incoming mutation against the
subscription's predicate using the mutating topic's columns only. A
one-shot SOW, by contrast, runs the full query planner (which can
stream the right-hand topic and fan out matches) because it executes
once against a frozen snapshot rather than per-mutation.

---

## 2. Choosing a query mode (decision tree)

```
Do you need the result to keep updating as data changes?
│
├─ NO  → Mode A: one-shot SOW.
│        ├─ Plain row filter?            → sow(topic, filter)
│        ├─ Aggregate / GROUP BY / PIVOT
│        │  / JOIN / projection?          → sow_sql(topic, sql)
│        └─ State as of a past point?     → sow_as_of_sequence / sow_as_of_timestamp
│
└─ YES → Do you need a JOIN across topics?
         │
         ├─ YES → Mode C: register a [[views]] block server-side,
         │        then sow_and_subscribe(viewTopic).
         │
         └─ NO  → Mode B: live subscription.
                  ├─ Want the current snapshot first, then live?  → sow_and_subscribe(topic, filter)
                  ├─ Only future changes, no snapshot?            → subscribe / delta_subscribe
                  └─ GROUP BY aggregate that ticks live?          → sow_and_subscribe_sql(topic, sql)   [Rust/TS]
```

Quick gut-check examples:

- *"Give me all positions in BOOK-RATES right now for a report."* →
  Mode A, `sow("/positions", "book LIKE 'BOOK-RATES%'")`.
- *"Run this analyst's ad-hoc JOIN once and show the grid."* → Mode A,
  `sow_sql`.
- *"Stream me every trade as it happens for symbol AAPL."* → Mode B,
  `sow_and_subscribe("/trades", "symbol = 'AAPL'")`.
- *"Keep a live book×sector P&L total on screen."* → Mode B aggregate,
  `sow_and_subscribe_sql`, **or** a view if it needs a JOIN.
- *"Live positions enriched with security reference data."* → Mode C,
  a view that joins `/positions ⨝ /securities`.

---

## 3. Connecting and authenticating

Every SDK follows the same shape: connect → (optionally) log on →
issue commands. Authentication is only required if the server sets
`[auth].required = true`; otherwise `logon` is an anonymous protocol
handshake you can skip.

### Rust

```rust
use cq_client::Client;

let c = Client::connect("tcp://127.0.0.1:9007").await?;   // or ws://host:9008/cq/json
c.logon("alice", "secret").await?;                        // omit if auth disabled
// JWT instead of password:
c.logon_jwt("eyJhbGciOi...").await?;
// HA: try several URLs in randomized order, first success wins.
let c = Client::connect_any(&[
    "tcp://replica1:9007", "tcp://replica2:9007",
]).await?;
```

### TypeScript

```ts
import { Client } from '@cqserver/client';

const c = await Client.connect('tcp://127.0.0.1:9007');   // Node
// const c = await Client.connect('ws://localhost:9008/cq/json'); // browser
await c.logon('alice', 'secret');                          // omit if auth disabled
// HA failover:
const c2 = await Client.connectAny(['tcp://r1:9007', 'tcp://r2:9007']);
```

### Python

```python
from cqclient import Client

c = Client.connect("127.0.0.1", 9007)        # or Client.connect_url("tcp://127.0.0.1:9007")
c.logon("alice", "secret")                   # omit if auth disabled
# context-manager form closes the socket for you:
with Client.connect("127.0.0.1", 9007) as c:
    ...
```

### Go

```go
import "time"
import cqclient "github.com/widgetstools/cqserver/client-sdks/go"

c, err := cqclient.Connect("127.0.0.1", 9007, 5*time.Second)
if err != nil { panic(err) }
defer c.Close()
// Logon(user, password, token, clientName); empty user == anonymous handshake.
if _, err := c.Logon("alice", "secret", "", "reporter-1"); err != nil { panic(err) }
```

### Java

```java
import io.cqserver.client.CqClient;

CqClient c = CqClient.connectUrl("tcp://127.0.0.1:9007");
c.logon("alice", "secret", null, "reporter-1");   // (user, password, token, clientName)
// HA: CqClient.connectAny(List.of("tcp://r1:9007", "tcp://r2:9007"));
```

> **Heartbeats.** Subscriber-only connections must keep the socket
> warm or the server idle-disconnects them (~65 s). The TS and Java
> SDKs heartbeat automatically (default 25 s). In Rust/Python/Go,
> either keep publishing or call the heartbeat path periodically on an
> idle subscriber connection.

---

## 4. Query kind 1 — one-shot filtered snapshot (`sow`)

**Mode A.** Evaluate `SELECT * FROM topic WHERE <filter>` against the
current SOW, return the matching rows, done. No subscription is
registered; no live deltas follow. This is the cheapest read.

Pass `null`/`None`/`""` (no filter) to get the entire topic snapshot.

### Rust

```rust
// Filtered:
let rows = c.sow("/positions", Some("book LIKE 'BOOK-RATES%'")).await?;
// Whole topic:
let all = c.sow("/positions", None).await?;
for row in &rows {
    println!("{}", row["position_id"]);
}
```

### TypeScript

```ts
const rows = await c.sow('/positions', { filter: "book LIKE 'BOOK-RATES%'" });
const all  = await c.sow('/positions');           // no filter
console.log(rows.length, 'rows');
```

### Python

```python
rows = c.sow("/positions", filter="book LIKE 'BOOK-RATES%'")
all_rows = c.sow("/positions")                    # no filter
print(len(rows), "rows")
```

### Go

```go
rows, err := c.Sow("/positions", "book LIKE 'BOOK-RATES%'")
allRows, err := c.Sow("/positions", "")           // empty filter == no filter
```

### Java

```java
List<Map<String,Object>> rows = c.sow("/positions", "book LIKE 'BOOK-RATES%'");
List<Map<String,Object>> all  = c.sow("/positions", null);
```

---

## 5. Query kind 2 — one-shot SQL: aggregates, GROUP BY, PIVOT, JOIN (`sow_sql`)

**Mode A, with the full query planner.** When you need anything beyond
`SELECT * WHERE` — column projection, `GROUP BY`, aggregate functions,
`PIVOT`, `ORDER BY`, `LIMIT`, subqueries, **or a JOIN** — use the SQL
form. The server rewrites the `FROM` clause to the resolved topic, so
the table name in your SQL is cosmetic: `FROM t`, `FROM positions`, and
`FROM "/positions"` all work.

This is the **only one-shot path that supports joins**, and the only
ad-hoc way to get joined data at all (the live path can't — see §1).

### Aggregate / GROUP BY

```rust
// Rust
let rows = c.sow_sql("/trades", r#"
    SELECT book, sector,
           SUM(qty)      AS total_qty,
           SUM(notional) AS total_notional,
           COUNT(*)      AS trades
    FROM trades
    GROUP BY book, sector
    ORDER BY total_notional DESC
"#).await?;
```

```ts
// TypeScript — note: sow() takes { sql } in the options object.
const rows = await c.sow('/trades', { sql: `
    SELECT book, sector, SUM(qty) AS total_qty, COUNT(*) AS trades
    FROM trades GROUP BY book, sector` });
```

```python
# Python
rows = c.sow_sql("/trades", """
    SELECT book, sector, SUM(qty) AS total_qty, COUNT(*) AS trades
    FROM trades GROUP BY book, sector""")
```

```go
// Go
rows, err := c.SowSQL("/trades", `
    SELECT book, sector, SUM(qty) AS total_qty, COUNT(*) AS trades
    FROM trades GROUP BY book, sector`)
```

```java
// Java
var rows = c.sowSql("/trades", """
    SELECT book, sector, SUM(qty) AS total_qty, COUNT(*) AS trades
    FROM trades GROUP BY book, sector""");
```

### JOIN (one-shot only)

```rust
let rows = c.sow_sql("/positions", r#"
    SELECT p.position_id, p.book_name, p.market_value_usd,
           t.trade_id, t.side, t.quantity, t.price
    FROM positions
    JOIN trades USING (position_id)
"#).await?;
```

The same string works through `sow_sql` / `SowSQL` / `sowSql` /
`sow('/positions', { sql })` in every SDK. The driving topic you pass
as the first argument is the left side of the join.

### PIVOT

```sql
-- Static: one output column per literal.
SELECT * FROM trades PIVOT (SUM(qty) FOR side IN ('BUY', 'SELL'))

-- Dynamic: server discovers the distinct values at run time.
SELECT * FROM trades PIVOT (SUM(qty) FOR side IN ANY)
```

Run it through `sow_sql`. (The `IN ANY` list is capped by
`[query_limits].max_pivot_in_list_size`, default 100.)

> **Result-shape tip for grids.** A JOIN fans out — one position with N
> trades yields N rows that share `position_id`. If your UI keys rows
> by a single id (e.g. AG Grid's `getRowId`), key on a **composite**
> (`${position_id}|${trade_id}`) or the grid will collapse the fan-out
> to one visible row per position. This bit us in the demo; see §14.

---

## 6. Query kind 3 — historical / as-of SOW

**Mode A against a past point in time.** For persistent (txlog-backed)
topics, you can ask for the SOW as it existed immediately after a given
sequence, or as of a wall-clock timestamp. Useful for "what did the
book look like at 15:00?" reconciliation reads.

Currently exposed on the **Rust SDK**:

```rust
// As of a specific txlog sequence:
let rows = c.sow_as_of_sequence("/trades", 12_345, Some("book = 'BOOK-RATES-01'")).await?;

// As of epoch-millis wall-clock time:
let five_min_ago = now_ms() - 5 * 60 * 1000;
let rows = c.sow_as_of_timestamp("/trades", five_min_ago, None).await?;
```

The topic must be `persist = true` server-side. In the lighter SDKs
(TS/Python/Go/Java), reach for live replay instead (§11) — subscribe
from a bookmark/timestamp and drain the replayed rows — or add the
as-of methods following the Rust signatures.

---

## 7. Query kind 4 — live filtered subscription (`sow_and_subscribe`)

**Mode B, the workhorse.** Register a continuous query: the server
first streams the **SOW snapshot** (every currently matching row), then
streams **live deltas** (`add` / `update` / `remove` / `oof`) forever
as the topic mutates. Single-topic only (no JOIN — see §1).

Three flavors:

| Method | Initial snapshot? | Then live? |
|---|---|---|
| `sow_and_subscribe` | ✅ yes | ✅ yes |
| `subscribe` | ❌ no | ✅ yes |
| `delta_subscribe` | ❌ no | ✅ yes (sparse, changed-fields-only updates) |

### Rust

```rust
use cq_client::DeltaKind;

let mut sub = c.sow_and_subscribe("/trades", Some("symbol = 'AAPL'"), None).await?;
while let Some(delta) = sub.next_delta().await {
    match delta.delta_type {
        DeltaKind::SowSnapshot          => render_initial(&delta.data),
        DeltaKind::Add | DeltaKind::Update => upsert_row(&delta.data),
        DeltaKind::Remove | DeltaKind::Oof => drop_row(&delta.data),
        DeltaKind::SchemaChange         => {}
    }
}
c.unsubscribe(&sub.sub_id).await?;
```

### TypeScript

```ts
const sub = await c.sowAndSubscribe('/trades', { filter: "symbol = 'AAPL'" });
for await (const d of sub) {
    if (d.deltaType === 'add' || d.deltaType === 'update') upsertRow(d.data);
    else if (d.deltaType === 'remove' || d.deltaType === 'oof') dropRow(d.data);
}
// Or, snapshot-then-live split:
await sub.whenSnapshotComplete();   // resolves at group_end
```

> In the **TS SDK** snapshot rows arrive as `deltaType: 'add'`; the
> end of the snapshot is signalled separately via
> `whenSnapshotComplete()` / `isSnapshotComplete`. In Python/Go they
> arrive as `delta_type == 'sow'`. In Rust they are
> `DeltaKind::SowSnapshot`. See §10.

### Python

```python
sub = c.sow_and_subscribe("/trades", filter="symbol = 'AAPL'")
for delta in sub:                      # blocks; or sub.next_delta(timeout=...)
    if delta.delta_type == "sow":
        render_initial(delta.data)
    elif delta.delta_type in ("add", "update"):
        upsert_row(delta.data)
    elif delta.delta_type in ("remove", "oof"):
        drop_row(delta.data)
c.unsubscribe(sub)
```

### Go

```go
sub, err := c.SowAndSubscribe("/trades", "symbol = 'AAPL'", 0)   // bookmark 0 == none
for {
    d, err := sub.NextDelta(0)        // 0 == block forever
    if err != nil { break }           // subscription closed
    switch d.DeltaType {
    case "sow", "add", "update":
        upsertRow(d.Data)
    case "remove", "oof":
        dropRow(d.Data)
    }
}
c.Unsubscribe(sub)
```

### Java

```java
Subscription sub = c.sowAndSubscribe("/trades", "symbol = 'AAPL'", null);
Delta d;
while ((d = sub.nextDelta(-1)) != null) {   // negative timeout == block
    switch (d.deltaType) {
        case "sow", "add", "update" -> upsertRow(d.data);
        case "remove", "oof"        -> dropRow(d.data);
    }
}
c.unsubscribe(sub);
```

### Live-only (no snapshot)

Swap `sow_and_subscribe` → `subscribe` (Rust/Py/Go/TS) /
`delta_subscribe` (Rust/TS) when you don't need the historical state,
only future changes. `delta_subscribe` additionally sends **sparse**
updates (only changed columns), saving bandwidth on wide rows.

---

## 8. Query kind 5 — continuous SQL aggregates (`sow_and_subscribe_sql`)

**Mode B with GROUP BY that stays live.** This is the powerful one:
the server maintains the aggregate's per-group state **incrementally**,
emitting `add` / `update` / `remove` deltas as group totals shift with
each underlying row mutation. You get a live pivot/rollup without
recomputing anything client-side.

Supported aggregates: `SUM`, `COUNT`, `COUNT(*)`, `AVG`, `MIN`, `MAX`.
Single-topic only (no JOIN — use a view for joined live aggregates).

### Rust

```rust
let mut sub = c.sow_and_subscribe_sql("/trades", r#"
    SELECT book, sector,
           SUM(qty)      AS total_qty,
           SUM(notional) AS total_notional,
           COUNT(*)      AS trades
    FROM trades
    GROUP BY book, sector
"#).await?;
while let Some(d) = sub.next_delta().await {
    // Each delta is one group row: Add (new group), Update (totals
    // changed), Remove (group emptied).
    apply_group_delta(&d);
}
```

### TypeScript

```ts
// The SQL goes in the options object on sowAndSubscribe.
const sub = await c.sowAndSubscribe('/trades', { sql: `
    SELECT book, sector, SUM(qty) AS total_qty, COUNT(*) AS trades
    FROM trades GROUP BY book, sector` });
for await (const d of sub) applyGroupDelta(d);
```

> **Availability.** Continuous SQL aggregates are exposed in the
> **Rust** (`sow_and_subscribe_sql`) and **TypeScript**
> (`sowAndSubscribe(topic, { sql })`) SDKs. The Python/Go/Java core
> SDKs expose SQL only on the **one-shot** `sow_sql` path; for a live
> aggregate from those, either poll `sow_sql` on an interval or
> register a server-side **view** (§9) and `sow_and_subscribe` the
> view topic (which needs no client-side SQL at all).

---

## 9. Query kind 6 — live joins via materialized views

**Mode C.** Since the live subscription path can't join (§1), the way
to get **live joined or enriched data** is to pre-register the join as
a server-side **view**. The server materializes it into a derived
topic and maintains it incrementally; clients subscribe to that topic
like any other — no special client API, works from **every** SDK.

Server config (`cqserver.toml`):

```toml
[[views]]
name             = "/book-sector-pnl"
source           = "/positions"            # left/driving topic
sql              = """
SELECT book, sector,
       SUM(unrealizedPnl) AS pnl,
       SUM(marketValue)   AS exposure
FROM "/positions" JOIN "/securities" USING (cusip)
GROUP BY book, sector
"""
initial_capacity = 10000
tap_capacity     = 1024
```

Then, from any SDK, it's a plain live subscription:

```rust
let mut sub = c.sow_and_subscribe("/book-sector-pnl", None, None).await?;
```
```ts
const sub = await c.sowAndSubscribe('/book-sector-pnl');
```
```python
sub = c.sow_and_subscribe("/book-sector-pnl")
```

The view ticks live: when a position or security row changes, the
server recomputes only the affected groups and pushes deltas to every
subscriber. This is the recommended pattern for dashboards that need
joined, continuously-updating data.

---

## 10. Reading the delta stream

A subscription yields a sequence of `Delta` objects. The fields are
consistent across SDKs (names differ by language convention):

| Field | Meaning |
|---|---|
| `delta_type` / `deltaType` / `DeltaType` | what kind of change — see below |
| `data` | the row object (full row for add/update; key fields for remove) |
| `sequence` | monotonic per-topic sequence number (for bookmarks) |
| `sub_id` / `subId` / `SubID` | the subscription this belongs to |

### Delta types

| Type | When | Action |
|---|---|---|
| **snapshot** (`SowSnapshot` in Rust; `sow` in Py/Go; `add` in TS/Java) | initial SOW rows, before live | seed your view |
| `add` | a new row entered the result set | insert |
| `update` | an existing row's values changed (still matches) | upsert |
| `remove` | a row was deleted from the topic | delete |
| `oof` | "out of focus" — row still exists but no longer matches your filter | delete from *your* view |
| `schema_change` (Rust/TS) | a new column appeared on the topic | update column model |

**`oof` is the subtle one.** It means a row you were shown has been
updated such that it *no longer satisfies your WHERE clause* — e.g. you
subscribed to `price > 100` and the price dropped to 90. The row is not
deleted server-side; it just left your view. Treat it like a remove
*for your subscription*.

### Snapshot-vs-live boundary

The server frames the initial snapshot with `group_begin` … rows …
`group_end`. SDKs surface the boundary differently:

- **Rust** — snapshot rows are `DeltaKind::SowSnapshot`; the first
  non-snapshot delta marks live.
- **TypeScript** — snapshot rows are `deltaType: 'add'`; await
  `sub.whenSnapshotComplete()` or check `sub.isSnapshotComplete` to
  know when live begins.
- **Python / Go** — snapshot rows are `delta_type == "sow"`; the first
  `"add"`/`"update"`/`"remove"` is live.
- **Java** — snapshot rows are `"sow"`/`"add"`; `getLastSequence()`
  tracks progress.

If you only want the final list and not the live stream, use a Mode A
`sow` instead — it returns the rows directly with no delta loop.

---

## 11. Replay and resume (bookmark / timestamp / most-recent)

Persistent topics let a subscriber resume from a past point instead of
only "now". All three modes start a normal live stream after the
historical replay drains.

### Bookmark (resume from a sequence)

Every delta carries a `sequence`. Persist the highest one you
processed; on reconnect, pass it as the `bookmark` so the server
replays everything strictly newer, then goes live.

```rust
// Rust: bookmark is the 3rd arg of sow_and_subscribe.
let sub = c.sow_and_subscribe("/trades", None, Some(last_seq)).await?;
```
```ts
const sub = await c.sowAndSubscribe('/trades', { bookmark: lastSeq });
```
```python
sub = c.sow_and_subscribe("/trades", bookmark=last_seq)
```
```go
sub, _ := c.SowAndSubscribe("/trades", "", lastSeq)   // bookmark arg
```
```java
// Java: track sub.getLastSequence(); pass getLastSequence()+1 on reconnect.
Subscription sub = c.sowAndSubscribe("/trades", null, lastSeq + 1);
```

### Timestamp replay (Rust)

```rust
let since_ms = now_ms() - 5 * 60 * 1000;
let sub = c.subscribe_since_timestamp("/trades", None, since_ms).await?;
```

### most_recent (Rust — resume by client name)

The server tracks a high-water mark per `client_name`; resume from
wherever this named client last received:

```rust
c.logon_with("alice", "secret", Some("dash-1".into()), None).await?;
let sub = c.subscribe_most_recent("/trades", None, "dash-1").await?;
```

### Persistent bookmark store (Rust)

For resume across process restarts, attach a disk-backed store; the SDK
records the high-water automatically and reads it back on the next run:

```rust
use cq_client::bookmark::LocalBookmarkStore;
c.set_bookmark_store(LocalBookmarkStore::open("./bookmark.json")?);
let sub = c.sow_and_subscribe("/trades", None, None).await?;  // resumes from disk
```

### Pause / resume an in-flight replay (Rust)

```rust
c.pause_subscription(&sub.sub_id).await?;
// ... drain your local queue ...
c.resume_subscription(&sub.sub_id).await?;
```

---

## 12. Per-SDK capability matrix

✅ = first-class method · ⚙️ = achievable via a lower-level path · ❌ = not in this SDK

| Capability | Rust | TS | Python | Go | Java |
|---|:--:|:--:|:--:|:--:|:--:|
| Connect TCP | ✅ | ✅ | ✅ | ✅ | ✅ |
| Connect WebSocket | ✅ | ✅ | ❌ | ❌ | ❌ |
| Connect TLS | ✅ | ✅ (`tls://`/`wss://`) | ❌ | ❌ | ❌ |
| HA `connect_any` | ✅ | ✅ | ❌ | ❌ | ✅ |
| Logon (user/pass) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Logon JWT | ✅ | ⚙️ | ✅ (token) | ✅ (token) | ✅ (token) |
| `publish` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `delta_publish` (sparse) | ✅ | ⚙️ | ✅ | ✅ | ✅ |
| `publish_batch` | ✅ | ✅ | ❌ | ❌ | ❌ |
| **`sow` (filter)** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **`sow_sql` (GROUP BY / PIVOT / JOIN)** | ✅ | ✅ | ✅ | ✅ | ✅ |
| `sow_as_of_sequence` / `_timestamp` | ✅ | ❌ | ❌ | ❌ | ❌ |
| **`subscribe` / `sow_and_subscribe`** | ✅ | ✅ | ✅ | ✅ | ✅ |
| `delta_subscribe` (live-only sparse) | ✅ | ✅ | ❌ | ❌ | ❌ |
| **`sow_and_subscribe_sql` (live aggregate)** | ✅ | ✅ (`{ sql }`) | ❌ | ❌ | ❌ |
| Subscribe to a **view** (live join) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Bookmark replay | ✅ | ✅ | ✅ | ✅ | ✅ |
| Timestamp replay | ✅ | ❌ | ❌ | ❌ | ❌ |
| `most_recent` resume | ✅ | ❌ | ❌ | ❌ | ❌ |
| Persistent bookmark store | ✅ | ❌ | ❌ | ❌ | ❌ |
| Pause / resume | ✅ | ❌ | ❌ | ❌ | ❌ |
| Queue ack / lease extend | ✅ | ⚙️ | ❌ | ❌ | ❌ |
| Client-requested conflation | ⚙️ | ✅ (`conflationMs`) | ❌ | ❌ | ✅ (`conflationMs`) |
| Auto-heartbeat | ❌ | ✅ | ❌ | ❌ | ✅ |

The headline takeaway: **all five SDKs can run every *query kind*** —
filtered snapshot, SQL/JOIN snapshot, live filtered subscription, and
live joins via views. The Rust and TS SDKs add the live-aggregate
shortcut (`sow_and_subscribe_sql`) and the advanced replay/ops surface.

---

## 13. Filter and SQL language reference

### Filter expressions (the `filter` argument)

A filter is a SQL-92 `WHERE`-clause fragment evaluated per row.

```sql
symbol = 'AAPL'
price > 100 AND volume > 1000
sector IN ('Banks', 'Tech')
book LIKE 'BOOK-RATES%'            -- % = any, _ = single char
risk.duration BETWEEN 5 AND 10    -- nested fields use dotted paths
last_updated IS NOT NULL
UPPER(symbol) = 'AAPL'
NOT (side = 'SELL')
```

Operators: `= != < <= > >=`, `IN (...)`, `BETWEEN x AND y`,
`LIKE`, `IS NULL` / `IS NOT NULL`, `AND` / `OR` / `NOT`, parentheses,
and the scalar string functions `UPPER` / `LOWER`.

### SQL queries (the `sql` argument)

The SQL path accepts a much broader subset for one-shot `sow_sql` (and
live `sow_and_subscribe_sql` minus joins):

- **Projection**: `SELECT a, b, c FROM t`
- **Aggregates**: `SUM`, `COUNT`, `COUNT(*)`, `AVG`, `MIN`, `MAX`
- **Grouping**: `GROUP BY`, `HAVING`
- **Ordering / limiting**: `ORDER BY ... [DESC]`, `LIMIT`, `OFFSET`
- **PIVOT**: `PIVOT (agg FOR col IN ('a','b'))` and `IN ANY`
- **Joins** (one-shot only): `INNER JOIN ... USING (col)` / `ON a = b`
- **Subqueries / derived tables** (one-shot)

The `FROM` table name is rewritten to the topic you pass as the first
argument, so write whatever reads naturally.

> For the authoritative, code-grounded list of what the SQL engine
> supports versus AMPS, see [`docs/AMPS_PARITY.md`](AMPS_PARITY.md).

---

## 14. Patterns, gotchas, and performance

**1. "My JOIN subscription returns no rows / errors with `Unknown
column`."** You sent a multi-topic query down the live path (Mode B).
Joins only run one-shot (`sow_sql`, Mode A) or as a view (Mode C). See
§1 and §9. This is the single most common mistake.

**2. JOIN fan-out collapses in the grid.** A position with N trades
yields N rows sharing `position_id`. If your UI dedupes by a single id,
build a composite key (`${position_id}|${trade_id}`). Mode A returns
all the rows correctly — the collapse is purely a client-side id
problem.

**3. Forgetting the snapshot/live boundary.** If your view double-
counts or flickers, you're probably treating snapshot rows as live
adds *and* re-adding them when live begins. Use the boundary signal
(§10): Rust `SowSnapshot`, TS `whenSnapshotComplete()`, Py/Go `"sow"`.

**4. Idle subscriber gets disconnected (~65 s).** Heartbeat. TS/Java do
it for you; in Rust/Python/Go keep the connection active or ping
periodically.

**5. Big snapshots are slow.** The server serializes the whole result
per subscribe. Mitigate by (a) projecting fewer columns via `sow_sql`,
(b) adding a selective `WHERE` so an index is used, or (c) moving the
shape into a view many subscribers share. Watch
`cq_query_index_hits_total` vs `cq_query_full_scans_total` in
`/metrics`.

**6. Use `delta_publish` for wide rows.** If a feed only knows a few
changed columns, `delta_publish` merges them server-side without
overwriting the rest — less bandwidth, less publisher CPU.

**7. Conflation for fast feeds / slow UIs.** Set `conflation_ms` on the
topic (server) or request `conflationMs` per-subscription (TS/Java) so
a fast publisher doesn't overwhelm a slow consumer; the server
coalesces to the latest value per key per interval.

**8. Pick the cheapest mode.** A one-time report wants `sow`, not
`sow_and_subscribe` + immediate unsubscribe. A live dashboard wants one
subscription held open, not `sow` on a polling timer.

---

## Related documents

- [`USER_GUIDE.md`](USER_GUIDE.md) — install, config, persistence, replication, ops
- [`AMPS_PARITY.md`](AMPS_PARITY.md) — SQL capability comparison vs AMPS
- SDK READMEs: [`client-sdks/ts/README.md`](../client-sdks/ts/README.md),
  [`client-sdks/python/`](../client-sdks/python/),
  [`client-sdks/go/README.md`](../client-sdks/go/README.md),
  [`client-sdks/java/README.md`](../client-sdks/java/README.md)
- Runnable examples: `client-sdks/*/examples/` (Quickstart in each SDK)

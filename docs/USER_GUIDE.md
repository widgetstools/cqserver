# cqserver — user guide

A practical, end-to-end guide to operating cqserver: from a cold
install through publishing your first row, querying it, persisting
it, replaying it after restart, and integrating with the client
SDKs we ship.

This is the **operator + application-developer** guide. For internal
design rationale, see [`ARCHITECTURE.md`](../ARCHITECTURE.md). For
the admin web UI, see [`docs/admin-ui.md`](admin-ui.md). For
multi-host deployments, see [`docs/deploy/replica-reads.md`](deploy/replica-reads.md).

---

## Table of contents

1. [What cqserver is](#1-what-cqserver-is)
2. [Installation](#2-installation)
3. [Your first server](#3-your-first-server)
4. [Topics and schemas](#4-topics-and-schemas)
5. [Publishing data](#5-publishing-data)
6. [Initial data load](#6-initial-data-load)
7. [Subscribing and querying](#7-subscribing-and-querying)
8. [Persistence](#8-persistence)
9. [Event replay](#9-event-replay)
10. [Queues](#10-queues)
11. [Authentication and entitlements](#11-authentication-and-entitlements)
12. [TLS](#12-tls)
13. [Replication](#13-replication)
14. [Operations and monitoring](#14-operations-and-monitoring)
15. [Rust SDK](#15-rust-sdk)
16. [TypeScript SDK](#16-typescript-sdk)
17. [Python SDK](#17-python-sdk)
18. [Troubleshooting](#18-troubleshooting)
19. [Appendix — wire protocol reference](#19-appendix--wire-protocol-reference)

---

## 1. What cqserver is

**cqserver is a stateful pub/sub server with continuous SQL queries.**
It holds the current state-of-the-world (SOW) for each topic in
memory, evaluates SQL-like filters on every mutation, and streams
fine-grained delta events to whichever subscribers' queries match.

Three things make it different from a generic message bus:

- **Stateful topics.** Each topic is a keyed table that holds the
  *current* value per key. New subscribers get the full snapshot
  before live deltas — no replay-from-start required.
- **Content-aware filtering.** Subscribers express interest with
  `WHERE` clauses, `GROUP BY`, `PIVOT`, joins. The server only
  delivers rows whose values match.
- **Optional persistence.** Topics marked `persist = true` write
  every mutation to a per-topic transaction log; on restart the SOW
  is replayed from the log, and subscribers can replay history from
  any sequence or timestamp.

Typical use cases: real-time trading dashboards, risk + position
aggregation, internal pub/sub for microservice fanout, market-data
distribution to many concurrent clients, time-series staging before
downstream analytics.

### Architecture in one paragraph

A `cqserver` process exposes three listener ports: **TCP** (length-
prefixed frames) and **WebSocket** (JSON / MessagePack frames) for
client traffic, plus an **admin HTTP** port for `/healthz`,
`/stats`, `/metrics`, the admin UI, and ops actions. Each
configured `[[topics]]` becomes a columnar in-memory store keyed by
its `key_fields`. Publishes upsert rows into that store; subscribes
register a compiled predicate that the dispatcher evaluates on
every mutation. Persistent topics also write to a per-topic
transaction log (`txlog/<topic-slug>/*.log` segment files) and
restore from it on restart. Replication is active-passive
shipper/receiver — leaders ship the txlog to followers, which apply
in-memory; subscribers can connect to any follower.

---

## 2. Installation

### 2.1 Requirements

- Rust 1.78+ (the repo's `rust-toolchain.toml` enforces this).
- Linux or macOS. Windows isn't tested; WSL2 works.
- A reasonable file-descriptor ulimit (`ulimit -n 65536` for any
  sub-count above ~500).

### 2.2 Build from source

```sh
git clone https://github.com/widgetstools/cqserver.git
cd cqserver
git checkout msrv-1.78
cargo build --release -p cq-server
```

The release binary lives at `target/release/cqserver`.

If you also want the load generator:
```sh
cargo build --release -p cq-loadgen
```

### 2.3 Container image (optional)

The repo includes a minimal Debian-slim runtime at
[`tests/cloud/Dockerfile.runtime`](../tests/cloud/Dockerfile.runtime).
It expects the binary to be pre-built on the host:

```sh
cargo build --release -p cq-server
docker build -f tests/cloud/Dockerfile.runtime -t cqserver:local .
docker run --rm -it -p 8085:8085 -p 9007:9007 \
  -v $(pwd)/config:/etc/cqserver:ro \
  cqserver:local --config /etc/cqserver/cqserver.toml
```

Production container deploys (multi-stage build, Helm chart, etc.)
are tracked in [`PRODUCTION_READINESS.md`](../PRODUCTION_READINESS.md)
as P2.1.

### 2.4 Verifying the build

```sh
./target/release/cqserver --help 2>&1 | head -3
# Outputs: nothing — there's no --help today. The only flag is
# --config <path>; everything else comes from the TOML.

./target/release/cqserver --config config/cqserver.toml
# Should start; ^C to stop.
```

---

## 3. Your first server

### 3.1 The minimal config

Create `config/cqserver.toml`:

```toml
# Network listeners.
tcp_addr       = "0.0.0.0:9007"
websocket_addr = "0.0.0.0:9008"
websocket_path = "/cq/json"
admin_addr     = "127.0.0.1:8085"   # ← keep admin on loopback by default

# Persistent topics' txlog directory.
[txlog]
directory = "/var/lib/cqserver/txlog"

# One topic to get started with.
[[topics]]
name             = "/quotes"
key              = ["symbol"]
persist          = false
initial_capacity = 1000
columns          = [
  { name = "symbol",    type = "string" },
  { name = "bid",       type = "double" },
  { name = "ask",       type = "double" },
  { name = "timestamp", type = "string" },
]
```

### 3.2 Starting the server

```sh
./target/release/cqserver --config config/cqserver.toml
```

You should see (truncated):

```
INFO cqserver: Topic registry initialized topics=1
INFO cqserver::admin: Admin HTTP server listening addr=127.0.0.1:8085
INFO cq_transport::tcp: TCP server listening addr=0.0.0.0:9007 tls=false
INFO cq_transport::websocket: WebSocket server listening addr=0.0.0.0:9008
```

### 3.3 Verifying it's up

From another terminal:

```sh
# Liveness:
curl -fsS http://127.0.0.1:8085/healthz
# → ok

# Topic registry:
curl -fsS http://127.0.0.1:8085/topics | python3 -m json.tool

# Aggregate stats:
curl -fsS http://127.0.0.1:8085/stats | python3 -m json.tool
```

### 3.4 The admin UI

The admin port also serves the operator console at `/ui/`:

```sh
open http://127.0.0.1:8085/ui/    # macOS
xdg-open http://127.0.0.1:8085/ui/  # Linux
```

The UI gives you a live view of every topic, subscription, queue,
replication peer, and the running config. See [`docs/admin-ui.md`](admin-ui.md)
for the full reference.

To enable the UI you need the bundle built once:

```sh
cd clients/admin-ui
npm install
npm run build
cd ../..
# Restart cqserver from the repo root so `clients/admin-ui/dist` is
# discoverable; or set CQSERVER_ADMIN_UI_DIR explicitly.
```

---

## 4. Topics and schemas

### 4.1 What a topic is

A **topic** is a named, keyed collection. Each row has a value for
every column in the topic's schema; the row's identity is the
concatenation of its `key_fields`. Publishing a row with an
existing key **upserts** — the old value is replaced; subscribers
see one `Update` delta, not an `Add` then a `Remove`.

```
/quotes  key=[symbol]
   symbol  bid     ask     timestamp
   AAPL    150.10  150.12  2026-05-25T15:00:00Z   ← row 1
   MSFT    400.50  400.55  2026-05-25T15:00:00Z   ← row 2
```

### 4.2 Defining a topic

In `cqserver.toml`:

```toml
[[topics]]
# Required:
name             = "/quotes"            # topic name (used in publish + subscribe)
key              = ["symbol"]           # ordered list of column names

# Recommended:
initial_capacity = 1000                 # SOW preallocates for this many rows

# Optional:
persist          = false                # write to txlog (see §8)
conflation_ms    = 50                   # coalesce rapid updates per key (§4.5)
expire_seconds   = 3600                 # drop rows untouched for N seconds (§4.6)
index_columns    = ["sector"]           # secondary equality+range index (§4.7)

# Schema — choose ONE of:
columns = [                             # ← inline (good for short schemas)
  { name = "symbol",    type = "string" },
  { name = "bid",       type = "double" },
  { name = "ask",       type = "double" },
  { name = "timestamp", type = "string" },
]
# OR
schema_file = "schemas/quotes.json"     # ← external (good for wide / nested)
```

Composite keys are just multi-element `key` arrays:

```toml
key = ["book", "cusip"]   # /positions keyed by (book, cusip)
```

### 4.3 Column types

| Type | Description | JSON form |
|---|---|---|
| `string` | UTF-8 text | `"AAPL"` |
| `long` | 64-bit signed integer | `100` |
| `int` | 32-bit signed integer | `100` |
| `double` | 64-bit float | `150.12` |
| `bool` | true/false | `true` |
| `bytes` | arbitrary binary | base64-string |
| `timestamp` | ISO 8601 datetime | `"2026-05-25T15:00:00Z"` |

Nested objects are flattened to dotted-path columns:

```json
{
  "risk": { "duration": 5.2, "convexity": 0.34 }
}
```

becomes columns `risk.duration` (double) and `risk.convexity` (double).
A `WHERE risk.duration > 4` filter works regardless of whether the
client publishes the nested form or the flat form.

### 4.4 External schema files

For schemas wider than a handful of columns, use a JSON schema file.
Path is relative to the cqserver.toml file:

```toml
[[topics]]
name        = "/trades"
key         = ["tradeId"]
persist     = true
schema_file = "schemas/trades.json"
```

`schemas/trades.json`:
```json
{
  "tradeId":      "string",
  "timestamp":    "string",
  "cusip":        "string",
  "side":         "string",
  "qty":          "long",
  "price":        "double",
  "notional":     "double",
  "trader":       "string",
  "book":         "string",
  "sector":       "string"
}
```

Nested:
```json
{
  "positionKey": "string",
  "marketValue": "double",
  "risk": {
    "duration":  "double",
    "convexity": "double"
  }
}
```

### 4.5 Conflation

`conflation_ms = N` coalesces rapid updates to the **same key** into
one delivery every N milliseconds per subscriber. Useful when the
publisher rate exceeds what a subscriber's UI can render.

```toml
[[topics]]
name          = "/market-data"
key           = ["symbol"]
conflation_ms = 100        # subscriber sees at most one update / 100ms / symbol
```

Conflation is per-subscriber-per-key — fast subscribers still get
everything; only slow ones get the coalesced view. The server-side
slow-consumer watcher (configurable under `[transport.slow_consumer]`)
can also widen this interval adaptively.

### 4.6 TTL

`expire_seconds = N` drops any row that hasn't been updated in N
seconds. Useful for time-windowed views (e.g., "last hour of
trades"). The TTL sweep runs once per second; granularity is
~1 second.

### 4.7 Secondary indexes

`index_columns = ["col", ...]` builds equality + range indexes on
the listed columns. Queries with `WHERE col = X` or
`WHERE col BETWEEN X AND Y` use the index instead of full-scanning.
Highly recommended on columns referenced by `WHERE` clauses in
production subscribers; the planner reports
`cq_query_index_hits_total` vs `cq_query_full_scans_total` in
metrics.

```toml
[[topics]]
name          = "/positions"
key           = ["positionKey"]
index_columns = ["book", "sector"]   # WHERE book = 'X' uses the index
```

---

## 5. Publishing data

### 5.1 Wire protocol overview

Clients connect via TCP or WebSocket, then send command frames.
Each frame is JSON (default) or MessagePack — the codec is
negotiated at connect time.

Two publish flavors:
- **Full publish** (`Command::Publish`): the message body is the
  complete row. Missing columns become null.
- **Delta publish** (`Command::DeltaPublish`): the message body is
  a partial row. Existing column values are preserved for fields
  not present; only listed fields are updated.

Both upsert by key. A publish is acknowledged with the sequence
number assigned by the server.

### 5.2 Single publish

Rust SDK:
```rust
use cq_client::Client;

let c = Client::connect("tcp://127.0.0.1:9007").await?;
let seq = c.publish("/quotes", serde_json::json!({
    "symbol":    "AAPL",
    "bid":       150.10,
    "ask":       150.12,
    "timestamp": "2026-05-25T15:00:00Z",
})).await?;
println!("published at seq={seq}");
```

TypeScript:
```ts
const c = await Client.connect('tcp://127.0.0.1:9007');
const seq = await c.publish('/quotes', {
    symbol: 'AAPL', bid: 150.10, ask: 150.12,
    timestamp: '2026-05-25T15:00:00Z'
});
```

Python:
```python
c = await Client.connect("tcp://127.0.0.1:9007")
seq = await c.publish("/quotes", {
    "symbol": "AAPL", "bid": 150.10, "ask": 150.12,
    "timestamp": "2026-05-25T15:00:00Z",
})
```

### 5.3 Delta publish

When the publisher only knows a subset of columns — e.g., a market-
data feed knows the new bid/ask but not the timestamp from another
upstream — use delta-publish to update those columns without
overwriting others:

```rust
// Bid moved; ask + timestamp unchanged on the row:
c.delta_publish("/quotes", serde_json::json!({
    "symbol": "AAPL",
    "bid":    150.11,
})).await?;
```

Delta-publish requires the row to already exist (the server has
to find it by key); publishing a delta for a non-existent key
creates the row with only the listed columns set and the rest null.

### 5.4 Publish-and-confirm guarantees

The server acks a publish only after:

1. The row is committed to the in-memory SOW.
2. (If persistent) the txlog entry is written to the OS page cache.
3. (Optional) the entry is acked back from at least one replication
   peer — enabled via the S11 `last_replicated_sequence` barrier;
   off by default.

For at-least-once durability across crashes, also enable
`fsync_on_publish = true` under `[txlog.fsync]`. This trades latency
(every publish becomes a disk-flush) for stronger guarantees.
Default is `none` — fsync runs on a background timer.

### 5.5 Publishing rates

A single client publishing to a non-persistent topic typically
sustains 50K-200K msgs/sec on commodity hardware before TCP buffer
backpressure kicks in. Persistent topics drop to 10K-50K depending
on fsync policy and txlog disk speed. For higher rates, parallelize
across topics or shard by key — there's no global publish lock.

---

## 6. Initial data load

When standing up cqserver with existing data (a CSV file, a JSON
dump, a database snapshot), the recommended pattern is:

### 6.1 Bulk-publish before subscribers connect

Subscribers see every row in their initial SOW snapshot regardless
of when it was published, so it's fine to load data first and open
the listeners to subscribers after.

```python
import asyncio, json
from cqclient import Client

async def bulk_load():
    c = await Client.connect("tcp://127.0.0.1:9007")
    with open("securities.json") as f:
        rows = json.load(f)
    for batch in chunks(rows, 1000):
        for row in batch:
            await c.publish("/securities", row)
    print(f"loaded {len(rows)} rows")
    await c.close()

asyncio.run(bulk_load())
```

For very large loads (10M+ rows), publish in parallel across N
client connections — the server's per-topic write path is
single-threaded but N publishers × M topics give linear throughput
up to the disk bandwidth ceiling.

### 6.2 The demo data loader

The repo ships a working example in TypeScript at
[`client-sdks/ts/examples/load-fi-data.ts`](../client-sdks/ts/examples/load-fi-data.ts).
It reads pre-generated JSON files and publishes ~947K rows across
seven topics in under a minute. Pattern:

```ts
import { Client } from '@cqserver/client';

const c = await Client.connect('tcp://127.0.0.1:9007');

const trades = JSON.parse(fs.readFileSync('trades.json', 'utf8'));
for (const t of trades) {
    await c.publish('/trades', t);
}
```

### 6.3 Loading into a persistent topic on first boot

If `persist = true`, the first publish creates the txlog segment
and the row is durable. On subsequent restarts the server replays
the entire txlog to reconstruct the SOW. **The initial-load pattern
is identical**; there's no separate "bootstrap mode."

For very large persistent topics (1B+ rows), txlog replay on
restart can take minutes. If startup time matters, consider:

- Splitting the topic across multiple cqserver instances (shard by
  key).
- Using `txlog.archive_directory` to roll sealed segments off the
  hot disk so the replay only re-reads the active segment.

### 6.4 Loading from a relational database

A common pattern when seeding cqserver from an existing system:

```python
import asyncpg, asyncio
from cqclient import Client

async def seed_from_postgres():
    pg = await asyncpg.connect(dsn=...)
    cq = await Client.connect("tcp://127.0.0.1:9007")

    async for row in pg.cursor("SELECT * FROM positions"):
        await cq.publish("/positions", dict(row))

    await pg.close(); await cq.close()
```

CSV is similar with `csv.DictReader`. Confirm the source's column
names match the topic schema exactly — cqserver silently drops
unknown columns and writes nulls for missing ones.

---

## 7. Subscribing and querying

### 7.1 Subscribe with a filter

The simplest subscription: get every row whose value matches a
WHERE clause, plus all subsequent changes.

```rust
let sub = c.sow_and_subscribe("/quotes", Some("symbol = 'AAPL'"), None).await?;
while let Some(delta) = sub.next_delta().await {
    println!("{:?} {:?}", delta.delta_type, delta.data);
}
```

You receive:

1. The initial **SOW snapshot** — every matching row as an `Add`
   delta, framed by `GroupBegin` / `GroupEnd`.
2. Then **live deltas** as the topic mutates — `Add` / `Update` /
   `Remove` / `Oof`.

`Oof` ("out of focus") means a row that was previously in your
filter set has been updated such that it no longer matches —
analogous to a remove from the subscriber's view, but the row
itself still exists on the server.

### 7.2 Filter expression language

Supported operators in `WHERE`:

- Comparison: `=`, `!=`, `<`, `<=`, `>`, `>=`
- Set membership: `IN ('A', 'B', 'C')`
- Range: `BETWEEN x AND y`
- Pattern: `LIKE 'pat%'` (with `%` wildcard, `_` single-char)
- Null: `IS NULL`, `IS NOT NULL`
- Boolean: `AND`, `OR`, `NOT`, parenthesization
- String functions: `UPPER(col) = 'AAPL'`, `LOWER(col) LIKE 'x%'`

Examples:
```sql
symbol = 'AAPL'
price > 100 AND volume > 1000
sector IN ('Banks', 'Tech')
book LIKE 'BOOK-RATES%'
risk.duration BETWEEN 5 AND 10
last_updated IS NOT NULL
```

### 7.3 SOW-with-SQL: aggregates, GROUP BY, PIVOT

For projections, aggregates, group-by, or pivots, use
`sow_and_subscribe_sql` which takes a full SELECT:

```rust
let sub = c.sow_and_subscribe_sql("/trades", "
    SELECT book, sector,
           SUM(qty)      AS total_qty,
           SUM(notional) AS total_notional,
           COUNT(*)      AS trades
    FROM \"/trades\"
    GROUP BY book, sector
").await?;
```

The subscription becomes a **continuous aggregate**: the server
maintains the per-group state incrementally, and you receive
`Add` / `Update` / `Remove` deltas as group totals change with
each underlying row mutation.

Supported aggregate functions: `SUM`, `COUNT`, `COUNT(*)`,
`AVG`, `MIN`, `MAX`.

### 7.4 PIVOT

For wide-output tables — book × sector matrices, side-by-side
buy/sell columns, etc.:

```sql
-- Static PIVOT: one column per literal in the IN list.
SELECT * FROM "/trades"
PIVOT (SUM(qty) FOR side IN ('BUY', 'SELL'))

-- Dynamic PIVOT: server discovers distinct values at execution.
SELECT * FROM "/trades"
PIVOT (SUM(qty) FOR side IN ANY)
```

The IN list is capped by `[query_limits].max_pivot_in_list_size`
(default 100) — see Query Guardrails (§14.4).

### 7.5 Joins

INNER JOIN on equality is supported:

```sql
SELECT book, sector,
       SUM(unrealizedPnl) AS pnl,
       SUM(marketValue)   AS exposure
FROM "/positions" JOIN "/securities" USING (cusip)
GROUP BY book, sector
```

Join queries can only run as **views** (server-side materialized);
ad-hoc subscribe-with-SQL doesn't currently support joins because
the SOW path holds a reference to one topic at a time.

To register a view:

```toml
[[views]]
name             = "/book-sector-pnl"
source           = "/positions"        # left side of the join
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

Subscribers then `sow_and_subscribe("/book-sector-pnl", None, None)`
— the view is a regular topic from the client's perspective.

### 7.6 Delta-only subscriptions

If you don't want the SOW snapshot — e.g., you're tailing for
display and don't need historical state — use `subscribe`:

```rust
let sub = c.subscribe("/quotes", Some("price > 100")).await?;
// No initial snapshot; only live deltas from now forward.
```

### 7.7 Unsubscribing

```rust
c.unsubscribe(&sub.sub_id).await?;
```

The subscription drops cleanly; subsequent deltas for that sub_id
are discarded server-side.

---

## 8. Persistence

### 8.1 What persistent means

A topic with `persist = true` writes every mutation to a per-topic
transaction log under `[txlog].directory`. On restart, cqserver:

1. Reads the txlog segments for each persistent topic.
2. Replays each entry through `replay_upsert_map` / `replay_delete`.
3. Reconstructs the in-memory SOW exactly as it was before shutdown.

Non-persistent topics live only in memory; their state is gone
after restart.

```toml
[[topics]]
name    = "/trades"
key     = ["tradeId"]
persist = true   # ← every publish writes to txlog/trades/*.log
```

### 8.2 Segment files

The txlog is segmented — each segment is a file like
`txlog/<topic-slug>/00000001.log`. Segments roll automatically when
they hit a configurable size cap:

```toml
[txlog]
directory       = "/var/lib/cqserver/txlog"
segment_size    = 268435456   # 256 MB — defaults to 16 MB; raise for fewer files
```

### 8.3 fsync policy

By default, txlog writes go to the OS page cache and fsync happens
on a background timer. For stricter durability:

```toml
[txlog.fsync]
mode = "always"           # fsync on every publish (slowest, safest)
# or
mode = "interval"
interval_ms = 100         # fsync every 100ms (default; covers ~all crash modes)
# or
mode = "none"             # rely on OS — risk losing recent writes on power loss
```

The `always` mode is the choice for financial systems where a
single lost write is unacceptable. The cost is ~5-10× higher
publish latency on most SSDs.

### 8.4 Archiving sealed segments

Once a segment rolls (a new segment starts because the current one
hit the size cap), the old one is **sealed** — closed for writes,
opened for read-only replay. Optional automatic archival moves
sealed segments to a separate directory and (optionally) compresses
them with zstd:

```toml
[txlog]
directory          = "/var/lib/cqserver/txlog"
archive_directory  = "/var/lib/cqserver/txlog-archive"
archive_compress   = true              # zstd-compress on the move
```

Archived segments are still replay-able — the bookmark / timestamp
replay paths transparently read them through their compressed form.

### 8.5 Shrinking a topic's SOW

Over time, deletes leave gaps in the row index that the SOW doesn't
reclaim automatically (the row index is monotonic — reusing slots
would invalidate any in-flight subscription that holds row IDs).
For long-lived persistent topics, an operator-triggered shrink
compacts the SOW:

```sh
# All topics at once:
curl -X POST http://127.0.0.1:8085/admin/shrink-store-all

# Just one:
curl -X POST http://127.0.0.1:8085/admin/shrink-store/%2Fpositions
```

Returns the before/after row count. Shrink doesn't affect the
txlog; it's purely an in-memory operation.

### 8.6 Backup

There's no built-in backup automation yet (tracked in
[`PRODUCTION_READINESS.md`](../PRODUCTION_READINESS.md) as P0.4).
For now, treat the `txlog/` directory as the database — `rsync` /
`tar` of a stopped server's txlog dir restores everything. With a
running server, snapshot at the filesystem level (LVM, ZFS, EBS
snapshot) — cqserver's segments are append-only so an
in-flight snapshot is consistent at the last-sealed segment
boundary.

---

## 9. Event replay

The transaction log lets a subscriber resume from any past point
in the topic's history.

### 9.1 Bookmark replay

Each row's sequence number is monotonic per topic. To replay from
a specific sequence:

```rust
// Resume from after the last sequence we processed:
let sub = c.subscribe_with_bookmark("/trades", None, 12345).await?;
```

The server replays every txlog entry whose sequence > 12345, then
transitions to live. The subscriber sees a `Replay` delta for each
historical entry followed by live `Add` / `Update` / `Remove`.

### 9.2 Timestamp replay

If you've forgotten the sequence but know the wall-clock time:

```rust
// Replay everything from 5 minutes ago forward:
let five_min_ago_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64
                    - 5 * 60 * 1000;
let sub = c.subscribe_since_timestamp("/trades", None, five_min_ago_ms).await?;
```

The server scans the txlog for the first entry whose timestamp is
≥ `since_timestamp_ms`, then replays from there. Granularity is
the resolution of the txlog timestamp (millisecond).

### 9.3 most_recent — pick up where you left off

For applications that periodically reconnect, cqserver tracks the
high-water mark per `client_name`:

```rust
// First connection — server records the high-water as the client
// processes deltas.
let c = Client::connect("tcp://127.0.0.1:9007").await?;
c.logon("alice", "secret").await?;  // client_name = "alice"
let sub = c.sow_and_subscribe("/trades", None, None).await?;
// ... process some deltas, then disconnect ...

// Reconnection:
let c2 = Client::connect("tcp://127.0.0.1:9007").await?;
c2.logon("alice", "secret").await?;
let sub2 = c2.subscribe_most_recent("/trades", None).await?;
// ↑ resumes from the last sequence "alice" successfully received
```

The high-water store is in-memory only — it survives connection
drops but not server restarts. For persistent client-resume
behavior across server restarts, persist the sequence on the
client side and use bookmark replay.

### 9.4 Pause / resume mid-stream

A subscriber that wants to stop receiving deltas temporarily (e.g.,
to drain its application's queue before a backpressure cycle):

```rust
c.pause(&sub.sub_id).await?;
// ... do other work ...
c.resume(&sub.sub_id).await?;
// Server replays anything missed during the pause.
```

The server queues missed mutations for the paused subscription up
to a configurable cap; if the subscriber stays paused long enough
to fill the cap, older entries get dropped (the bookmark on resume
captures the dropped sequences so the application can detect the
gap).

### 9.5 Configuring replay limits

```toml
[transport]
# How many txlog entries one bookmark replay is allowed to read
# before forcing a chunk boundary. Larger = faster replay but more
# memory; smaller = smoother backpressure.
replay_chunk_size = 1000
```

---

## 10. Queues

Queue topics are the load-balancing variant: each published message
goes to **exactly one** of the subscribers (round-robin) rather
than fanning out.

### 10.1 Defining a queue

```toml
[[queues]]
name              = "/orders-to-process"
lease_ms          = 5000      # delivered messages must be acked within 5s
max_delivery_count = 5         # after 5 failed redeliveries → DLQ
dlq               = "/orders-dlq"   # optional dead-letter queue
```

### 10.2 Publishing to a queue

Identical to a regular topic from the client's perspective:

```rust
c.publish("/orders-to-process", json!({ "orderId": "ord-001", "qty": 100 })).await?;
```

### 10.3 Consuming from a queue

```rust
// All connected consumers share the message stream round-robin.
let sub = c.sow_and_subscribe("/orders-to-process", None, None).await?;
while let Some(msg) = sub.next_delta().await {
    process_order(&msg.data).await?;
    // Acknowledge so the lease releases:
    c.ack_message(&msg.delivery_id.unwrap()).await?;
}
```

If the consumer dies before acking, the lease expires (`lease_ms`)
and the message is redelivered to another consumer. After
`max_delivery_count` redeliveries it routes to the configured DLQ
(or is dropped if no DLQ is set).

### 10.4 Use cases

- **Work distribution**: N workers process incoming work, one task
  per worker at a time.
- **At-least-once processing**: leases + redelivery + DLQ give the
  standard "process or fail loudly" semantics.
- **Backpressure-friendly fanout** when only one consumer should
  see each message but you want HA via redundant consumers.

---

## 11. Authentication and entitlements

By default cqserver accepts unauthenticated connections on the
configured listeners. For production, enable auth and bind the
admin port to loopback or front it with a reverse-proxy that
enforces auth.

### 11.1 Bcrypt user/password

```toml
[auth]
required = true               # all commands except Heartbeat require Logon

[[auth.users]]
username      = "alice"
password_hash = "$2b$12$..."  # bcrypt hash; generate with `htpasswd -nBC 12 alice`
entitlements  = ["subscribe:*", "publish:/orders", "sow:*"]
```

Then from the client:

```rust
c.logon("alice", "secret").await?;
// Subsequent commands run as alice.
```

### 11.2 Entitlements syntax

Each entitlement is `op:pattern`:

| `op` | Allows |
|---|---|
| `publish` | upsert + delta-publish |
| `subscribe` | subscribe + delta-subscribe |
| `sow` | one-shot SOW reads |
| `delete` | row deletion |
| `admin` | reserved for future admin commands |

Pattern matching:
- `*` — matches everything
- `/orders` — exact match
- `/market-*` — prefix match (anything starting with `/market-`)
- `*:*` and `*` — convenience for "everything"

Multiple entitlements OR together. A user with `["publish:/orders",
"subscribe:*"]` can publish only to `/orders` but subscribe to any
topic.

### 11.3 Row-level filtering

For tenant isolation, attach a SQL WHERE fragment that's AND'd into
every query the user runs:

```toml
[[auth.users]]
username      = "team-rates"
password_hash = "..."
entitlements  = ["subscribe:/trades", "sow:/trades"]
row_filter    = "book LIKE 'BOOK-RATES%'"
```

`team-rates` subscribing to `/trades WHERE side = 'BUY'` actually
runs `WHERE side = 'BUY' AND book LIKE 'BOOK-RATES%'` server-side
— the user can't accidentally (or maliciously) see other desks'
trades.

### 11.4 JWT (HS256)

For SSO / token-based auth:

```toml
[auth.jwt]
secret             = "${CQSERVER_JWT_SECRET}"   # via env var
username_claim     = "sub"
entitlements_claim = "entitlements"
issuer             = "https://auth.example.com/"  # optional
audience           = "cqserver"                    # optional
```

The client logs on with a token instead of a password:

```rust
let token = "eyJ...";
c.logon_jwt(token).await?;
```

The server validates the JWT signature + expiry + (optional) issuer
and audience, then constructs the User from the claim names.

### 11.5 Per-user query budgets

For preventing one user from consuming the whole server's
encoder bandwidth (the G5 guardrail):

```toml
[[auth.users]]
username     = "viewer-bob"
password_hash = "..."
entitlements = ["subscribe:*"]
query_budget = { max_sow_estimated_rows = 10_000, hard_max_sow_result_rows = 50_000 }
```

A user can only **tighten** the global `[query_limits]` defaults
via `query_budget`; they can never loosen them.

---

## 12. TLS

### 12.1 Generate a cert

For testing:
```sh
openssl req -x509 -newkey rsa:4096 -keyout cqserver.key -out cqserver.crt \
  -sha256 -days 365 -nodes -subj "/CN=cqserver"
```

For production: use Let's Encrypt, your corporate PKI, or
ACM/equivalent.

### 12.2 Enable on the TCP transport

```toml
[transport.tls]
cert_file = "/etc/cqserver/tls/cqserver.crt"
key_file  = "/etc/cqserver/tls/cqserver.key"
```

The TCP listener accepts only TLS connections after this. Clients
must use the TLS connect path:

```rust
use std::sync::Arc;
use cq_client::transport;

let client_cfg = transport::tls_client_config_with_roots(
    &["/path/to/ca.pem"],
)?;
let c = Client::connect_tls(
    "cqserver.example.com:9007",
    "cqserver.example.com",     // SNI
    Arc::new(client_cfg),
).await?;
```

For development / self-signed certs:
```rust
let client_cfg = transport::tls_client_config_dangerous_no_verify();
// ↑ DO NOT use this in production.
```

### 12.3 WebSocket TLS

WebSocket TLS isn't built into cqserver — front the WebSocket
listener with nginx or HAProxy doing `wss://` termination. See
[`docs/deploy/replica-reads.md`](deploy/replica-reads.md) for
example L4 LB configs that also work for SSL termination.

---

## 13. Replication

cqserver supports active-passive replication: one leader process
ships its persistent topics' txlog to one or more follower
processes, which apply the entries in-memory. Followers reject
publishes; clients can subscribe to any follower for read-fanout
scaling.

This is just a quick reference — for the full deployment guide
including L4 LB configs, monitoring, and failure modes, see
[`docs/deploy/replica-reads.md`](deploy/replica-reads.md).

### 13.1 Leader config

```toml
[replication]
role  = "primary"
peers = [
  "follower1.internal:9010",
  "follower2.internal:9010",
]
```

### 13.2 Follower config

```toml
[replication]
role   = "standby"
listen = "0.0.0.0:9010"
```

Topics must be `persist = true` to be replicated. Schema and key
columns must match exactly between leader and follower or the
follower silently drops entries for unknown topics.

### 13.3 Connecting clients to followers

Multi-URI client connect picks a follower at random; on disconnect,
reconnects to another:

```rust
let c = Client::connect_any(&[
    "tcp://follower1.internal:9007",
    "tcp://follower2.internal:9007",
]).await?;
```

Initial-connect failover is shipped; live reconnect-on-loss is
tracked in [`REPLICA_READS_WORKLOG.md`](../REPLICA_READS_WORKLOG.md) §S2b.

### 13.4 Monitoring replication

The admin UI's Replication page surfaces:

- Role + peer + listen (top card)
- Per-topic shipped / applied / acked sequences
- Lag (shipped − applied) tinted red when growing

The metrics behind these (`cq_repl_shipped_max_sequence`,
`cq_repl_applied_max_sequence`, `cq_repl_acked_max_sequence`,
`cq_repl_connect_total`, `cq_repl_reconnect_total`) are also
scrape-able directly from `/metrics`.

---

## 14. Operations and monitoring

### 14.1 The admin UI

`http://<admin-host>:8085/ui/` (default port 8085) opens the
operator console. Built on Vite + React + AG-Grid; covered in full
in [`docs/admin-ui.md`](admin-ui.md). Highlights:

| Page | Purpose |
|---|---|
| Overview | RSS, sub count, topic count, publish rate, snapshot cache |
| Topics | AG-Grid of every topic |
| Subscriptions | Live wire view; per-row drop subscription |
| Views | Materialized continuous queries + their SQL |
| Queues | Per-queue depth + consumers + sequence |
| Replication | Topology + per-topic lag |
| Metrics | Prometheus series browser; pin sparklines |
| Explain | Run `POST /admin/explain` from a form |
| Config | Live read-only TOML view + find-in-file |

Windows-style keyboard shortcuts: `Ctrl+K` palette, `Ctrl+/`
cheat sheet, `Alt+1..9` jump nav, `F5` refresh, `Ctrl+F` focus
filter.

### 14.2 Prometheus metrics

`GET /metrics` returns Prometheus text-format exposition. Most
useful series:

| Metric | Type | Meaning |
|---|---|---|
| `process_rss_bytes` | gauge | resident memory |
| `cq_topic_row_count{topic}` | gauge | rows per topic |
| `cq_subscription_count` | gauge | active subscriptions |
| `cq_publish_total{topic}` | counter | cumulative publishes |
| `cq_publish_latency_us{topic}` | histogram | publish ack latency |
| `cq_sow_actual_rows{topic}` | histogram | SOW result size |
| `cq_sow_query_latency_us{topic}` | histogram | SOW latency |
| `cq_repl_shipped_max_sequence{topic}` | gauge | per-topic shipped seq |
| `cq_repl_applied_max_sequence{topic}` | gauge | per-topic applied seq |
| `cq_query_rejected_total{reason}` | counter | guardrail rejections |
| `cq_query_runtime_capped_total{topic}` | counter | runtime cap fires |
| `cq_snapshot_cache_bytes` | gauge | encoder cache size |

### 14.3 Admin API

| Endpoint | Method | Description |
|---|---|---|
| `/healthz` | GET | liveness — returns `"ok"` |
| `/stats` | GET | aggregate stats JSON |
| `/topics` | GET | per-topic array |
| `/subscriptions` | GET | per-subscription array |
| `/subscriptions/:sub_id` | DELETE | drop a subscription |
| `/queues` | GET | per-queue array |
| `/metrics` | GET | Prometheus text |
| `/admin/rotate-journal/:topic` | POST | force-seal active txlog segment |
| `/admin/shrink-store/:topic` | POST | compact a topic's SOW |
| `/admin/shrink-store-all` | POST | compact every topic |
| `/admin/replication` | GET | role + per-topic seqs |
| `/admin/views` | GET | per-view array |
| `/admin/config` | GET | rendered TOML (text/plain) |
| `/admin/explain` | POST | cost-estimate `{topic, sql}` |

### 14.4 Query guardrails

The G1–G5 guardrails (`QUERY_GUARDRAILS_WORKLOG.md`) enforce
structural + cost-based limits on subscribe queries:

```toml
[query_limits]
# Parse-time (G1):
max_pivot_in_list_size           = 100
max_view_chain_depth             = 3
reject_degenerate_groupby        = true
reject_passthrough_views         = true

# Estimate-based (G3):
max_sow_estimated_rows           = 1_000_000
max_sow_estimated_bytes          = 100_000_000
max_join_estimated_fanout        = 10
max_group_estimated_cardinality  = 100_000

# Soft warnings (G3):
warn_sow_rows_threshold          = 100_000
warn_sow_bytes_threshold         = 10_000_000

# Runtime caps (G4):
hard_max_sow_result_rows         = 5_000_000
hard_max_sow_result_bytes        = 500_000_000
```

`POST /admin/explain` previews any subscribe query's cost before
committing.

---

## 15. Rust SDK

### 15.1 Installation

Add to `Cargo.toml`:

```toml
[dependencies]
cq-client = { path = "path/to/cqserver/crates/cq-client" }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
```

(Crate-on-crates.io publishing is a P2 item.)

### 15.2 Full example

```rust
use cq_client::{Client, DeltaKind};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Single-host connect.
    let c = Client::connect("tcp://127.0.0.1:9007").await?;

    // Optional: with auth.
    c.logon("alice", "secret").await?;

    // Publish one row.
    let seq = c.publish("/quotes", json!({
        "symbol": "AAPL", "bid": 150.10, "ask": 150.12
    })).await?;
    println!("published at seq={seq}");

    // Continuous subscribe.
    let mut sub = c.sow_and_subscribe(
        "/quotes",
        Some("bid > 100"),
        None,    // bookmark
    ).await?;

    while let Some(delta) = sub.next_delta().await {
        match delta.delta_type {
            DeltaKind::Add | DeltaKind::Update => {
                println!("{:?} {:?}", delta.delta_type, delta.data);
            }
            DeltaKind::Remove | DeltaKind::Oof => {
                println!("gone: {:?}", delta.data);
            }
            DeltaKind::SowSnapshot => {
                // Mid-snapshot marker; ignore.
            }
            DeltaKind::SchemaChange => {
                println!("schema change: {:?}", delta.schema_change);
            }
        }
    }

    Ok(())
}
```

### 15.3 Method reference

| Method | Description |
|---|---|
| `connect(url)` / `connect_with(url, cfg)` | TCP or WebSocket |
| `connect_tls(addr, sni, cfg)` | TLS over TCP |
| `connect_any(&urls)` | Multi-URI failover (replica-reads) |
| `logon(user, pw)` / `logon_jwt(tok)` | Authenticate |
| `publish(topic, data)` | Upsert |
| `delta_publish(topic, data)` | Partial update |
| `subscribe(topic, filter)` | Live deltas only |
| `sow_and_subscribe(topic, filter, bookmark)` | Snapshot + live |
| `sow_and_subscribe_sql(topic, sql)` | With GROUP BY / aggregates |
| `subscribe_since_timestamp(topic, filter, ms)` | Timestamp replay |
| `subscribe_most_recent(topic, filter)` | Resume from high-water |
| `sow(topic, filter)` | One-shot snapshot only |
| `sow_sql(topic, sql)` | One-shot SQL query |
| `unsubscribe(sub_id)` | Drop subscription |
| `sow_delete(topic, key)` | Delete a row |
| `pause(sub_id)` / `resume(sub_id)` | Mid-stream pause |

### 15.4 Bookmark store

For applications that need to resume across restarts:

```rust
use cq_client::bookmark::LocalBookmarkStore;

let store = LocalBookmarkStore::open("./bookmark.json")?;
let c = Client::connect("tcp://127.0.0.1:9007").await?;
c.attach_bookmark_store(store);

let sub = c.sow_and_subscribe("/trades", None, None).await?;
// Every received delta updates the bookmark on disk; on next
// run, subscribe_most_recent reads it back automatically.
```

---

## 16. TypeScript SDK

### 16.1 Installation

```sh
npm install @cqserver/client
```

(Or `path` to the workspace `client-sdks/ts/` during dev.)

### 16.2 Full example (Node)

```ts
import { Client } from '@cqserver/client';

const c = await Client.connect('tcp://127.0.0.1:9007');

await c.logon('alice', 'secret');

const seq = await c.publish('/quotes', {
    symbol: 'AAPL',
    bid: 150.10,
    ask: 150.12,
});

const sub = await c.sowAndSubscribe('/quotes', { filter: 'bid > 100' });
for await (const delta of sub) {
    console.log(delta.deltaType, delta.data);
}
```

### 16.3 Browser (WebSocket)

```ts
const c = await Client.connect('ws://localhost:9008/cq/json');
// Everything else is identical.
```

### 16.4 SharedWorker for multiple tabs

To avoid each browser tab opening its own WebSocket:

```ts
import { SharedWorkerClient } from '@cqserver/client/shared-worker';

const c = await SharedWorkerClient.connect('ws://localhost:9008/cq/json');
// All tabs sharing the same SharedWorker share one underlying WS;
// each tab's subscriptions multiplex on top.
```

See [`client-sdks/ts/README.md`](../client-sdks/ts/README.md) for the
SharedWorker pattern + browser-specific caveats.

### 16.5 Method reference (camelCase, matching the JS convention)

The TypeScript API mirrors the Rust SDK exactly with camelCase
names:

| TS | Rust equivalent |
|---|---|
| `connect(url)` | `connect` |
| `connectAny([url, ...])` | `connect_any` |
| `logon(u, p)` / `logonJwt(t)` | `logon` / `logon_jwt` |
| `publish(t, d)` / `deltaPublish(t, d)` | `publish` / `delta_publish` |
| `subscribe(t, opts)` | `subscribe` |
| `sowAndSubscribe(t, opts)` | `sow_and_subscribe` |
| `sowAndSubscribeSql(t, sql)` | `sow_and_subscribe_sql` |
| `unsubscribe(subId)` | `unsubscribe` |

Subscription iterators use `for await (const delta of sub)`; each
delta has `deltaType`, `data`, `sequence`, `subId` fields.

---

## 17. Python SDK

### 17.1 Installation

```sh
cd client-sdks/python
pip install -e .
```

(Wheel publishing to PyPI is a P2 item.)

### 17.2 Async example

```python
import asyncio
from cqclient import Client

async def main():
    c = await Client.connect("tcp://127.0.0.1:9007")
    await c.logon("alice", "secret")

    seq = await c.publish("/quotes", {
        "symbol": "AAPL", "bid": 150.10, "ask": 150.12,
    })
    print(f"published at seq={seq}")

    sub = await c.sow_and_subscribe("/quotes", filter="bid > 100")
    async for delta in sub:
        print(delta.delta_type, delta.data)

asyncio.run(main())
```

### 17.3 Sync wrapper

For scripts and notebooks where async isn't ergonomic:

```python
from cqclient import SyncClient

c = SyncClient.connect("tcp://127.0.0.1:9007")
c.publish("/quotes", {"symbol": "AAPL", "bid": 150.10, "ask": 150.12})

for delta in c.sow_and_subscribe("/quotes", filter="bid > 100"):
    print(delta.delta_type, delta.data)
```

### 17.4 Method reference

| Python | Rust equivalent |
|---|---|
| `Client.connect(url)` | `connect` |
| `client.logon(u, p)` | `logon` |
| `client.publish(t, dict)` | `publish` |
| `client.subscribe(t, filter=...)` | `subscribe` |
| `client.sow_and_subscribe(t, filter=...)` | `sow_and_subscribe` |
| `client.delta_subscribe(t, filter=...)` | `subscribe` (delta-only) |
| `client.sow(t, filter=...)` | `sow` |
| `client.unsubscribe(sub_id)` | `unsubscribe` |
| `client.sow_delete(t, key)` | `sow_delete` |

### 17.5 Admin client

```python
from cqclient.admin import AdminClient

a = AdminClient.from_url("http://127.0.0.1:8085")
print(await a.healthz())
print(await a.stats())
print(await a.topics())
```

---

## 18. Troubleshooting

### "Address already in use" on startup

Another process is on the configured port. Identify it:

```sh
lsof -iTCP:9007 -sTCP:LISTEN | head -3
lsof -iTCP:8085 -sTCP:LISTEN | head -3
```

Common cause: a previous cqserver instance didn't shut down cleanly.
`kill` the listed pid, or change `tcp_addr` / `admin_addr`.

### Subscriber sees empty SOW snapshot

- **The topic has no rows.** Check `/topics` for the topic's
  `rowCount`.
- **The filter excludes everything.** Try the same subscribe with no
  filter and confirm rows arrive.
- **You're authenticated as a user with a `row_filter` that excludes
  every row.** Check `/admin/config` for the user's `row_filter`.

### Publish ack timeout

- **The topic doesn't exist.** Confirm with
  `curl http://127.0.0.1:8085/topics`. If missing, your config
  didn't load — check `/admin/config`.
- **You're missing the `publish` entitlement.** Look in the
  server's log for `entitlement check failed`.
- **The server is overloaded.** Watch `cq_publish_latency_us` and
  `cq_publish_total` — if latency is climbing into seconds, the
  encoder semaphore is saturated.

### Slow subscribe-time on a wide topic

The server's snapshot encoder serializes the entire SOW per query.
Three mitigations:

1. **Project fewer columns** via SQL (`SELECT col1, col2 FROM t WHERE ...`).
2. **Add a WHERE filter** to narrow the result. Confirm
   `cq_query_index_hits_total` in `/metrics` increments when you
   add the filter.
3. **Use a continuous aggregate** via view if many subscribers want
   the same shape.

### Replication lag growing

Check the leader's log for `Replication shipper disconnected;
reconnecting`. Common causes:

- Network partition between leader and follower.
- Follower disk full → can't write the receiver's apply path.
- Follower's `[txlog].directory` not writable.

Watch `cq_repl_reconnect_total` — if it's incrementing, the network
is flapping.

### "Query rejected: estimated_result_rows exceeds…"

A query guardrail (G3) fired. The error message names the specific
limit — see §14.4. Either:

1. Add a more selective WHERE filter.
2. Switch to a continuous aggregate.
3. Have ops raise the limit in `[query_limits]` for everyone, or
   in `[[auth.users]].query_budget` for one user.

### Out-of-memory under load

- Confirm `[transport].outbound_queue_capacity` is reasonable
  (~2048 is the current default; lower it for many-subscriber
  fanout).
- Check the snapshot cache via `cq_snapshot_cache_bytes` —
  capped by H2 at 256 MB default; you can lower it via
  `CQSERVER_SNAPSHOT_CACHE_MAX_BYTES` env var.
- Profile: `perf` / `Instruments` / `dhat` against the running
  process.

---

## 19. Appendix — wire protocol reference

The wire protocol is documented as code in the `cq-protocol` crate.
This appendix is a high-level summary; clients shouldn't need to
implement it directly (use one of the SDKs).

### 19.1 Framing

- **TCP**: length-prefixed frames. `[u32 BE][body]`. Body is JSON
  or MessagePack per the session codec.
- **WebSocket**: each WS message is one body, framed by WS itself.

### 19.2 Command set

| Command | Direction | Body |
|---|---|---|
| `Logon` | C→S | `{ user, password }` or `{ token }` |
| `Publish` | C→S | `{ topic, data, command_id? }` |
| `DeltaPublish` | C→S | `{ topic, data, command_id? }` |
| `Subscribe` | C→S | `{ topic, filter?, sql?, options... }` |
| `SowAndSubscribe` | C→S | same as Subscribe + snapshot |
| `DeltaSubscribe` | C→S | live deltas only (no snapshot) |
| `Sow` | C→S | one-shot snapshot only |
| `SowDelete` | C→S | `{ topic, key }` |
| `Unsubscribe` | C→S | `{ sub_id }` |
| `Pause` / `Resume` | C→S | `{ sub_id }` |
| `Heartbeat` | bidirectional | empty |
| `Ack` | S→C | `{ status, sequence?, sub_id?, reason? }` |
| `GroupBegin` / `GroupEnd` | S→C | snapshot bracketing |
| `SowBatch` | S→C | batched snapshot rows |
| `SchemaChange` | S→C | new column appeared |

### 19.3 Codecs

| Codec | Configured via |
|---|---|
| JSON | default; one connection, ~30% CPU savings on small messages |
| MessagePack | explicit `Codec = "MessagePack"` at connect |
| BSON | explicit `Codec = "Bson"` |
| FIX 4.x | S23; specific to FIX-adjacent integrations |

The codec is fixed for the lifetime of a session — negotiated at
connect, not switched mid-stream.

### 19.4 Sequence numbers

Each persistent topic maintains a monotonically increasing 64-bit
sequence counter. Every publish/delete advances it. Subscribers
receive deltas in sequence order; bookmark replay uses the
sequence directly. Non-persistent topics still issue sequences
internally for ordering but don't survive restart.

### 19.5 Protocol versioning

Clients send their protocol version in the `Logon` command body.
The server picks the lower of (client_version, server_version) and
emits frames in that dialect. Forward compatibility: a newer server
keeps speaking the older dialect to older clients indefinitely.

---

## Related documents

- [`ARCHITECTURE.md`](../ARCHITECTURE.md) — system design rationale
- [`docs/admin-ui.md`](admin-ui.md) — operator console reference
- [`docs/deploy/replica-reads.md`](deploy/replica-reads.md) — multi-host deployment guide
- [`PRODUCTION_READINESS.md`](../PRODUCTION_READINESS.md) — gap analysis + roadmap
- Worklogs at the repo root: `HIGH_SCALE_WORKLOG.md`, `REPLICA_READS_WORKLOG.md`, `QUERY_GUARDRAILS_WORKLOG.md`, `ADMIN_UI_WORKLOG.md`, `CLOUD_REPLICATION_TEST_WORKLOG.md`
- SDK READMEs: [`client-sdks/python/README.md`](../client-sdks/python/README.md), [`client-sdks/ts/README.md`](../client-sdks/ts/README.md)

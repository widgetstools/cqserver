# CQServer — Continuous Query Server

## Vision

CQServer is a high-performance, content-aware messaging server written in Rust.
It replaces AMPS (Advanced Message Processing System) by 60East Technologies,
providing the same core value proposition — **stateful pub/sub with continuous
queries** — with modern tooling, memory safety, and an open architecture.

The server understands message payloads. It doesn't just route bytes between
publishers and subscribers; it maintains an in-memory **State of the World (SOW)**
per topic, evaluates SQL-like predicates on every mutation, and delivers
fine-grained deltas to subscribers whose queries match the change.

---

## Core Concepts

### Topics

A **topic** is a named, keyed collection of records. Each topic has:

| Property | Description |
|---|---|
| `name` | Unique identifier (e.g., `/market-data`, `/orders`) |
| `key` | One or more fields that uniquely identify a record (e.g., `/symbol`) |
| `schema` | Column names and types, discovered on first publish or configured |
| `storage` | Columnar SOW store holding current state |
| `txlog` | Optional append-only transaction log for persistence and replay |

Topics are created explicitly via configuration or implicitly on first publish
(configurable policy).

### Message Flow

```
Publisher ──publish──▶ CQServer ──▶ SOW upsert
                                  ├──▶ TxLog append (if persistent)
                                  ├──▶ Subscription evaluation
                                  │    ├─ predicate match? → delta(ADD/UPDATE)
                                  │    └─ was matching, now isn't? → delta(REMOVE)
                                  └──▶ Delta delivery to subscribers
```

### Subscription Modes

| Mode | Description |
|---|---|
| `subscribe` | Receive all future publishes on a topic (optionally content-filtered) |
| `sow` | One-shot query: get current SOW snapshot, then disconnect |
| `sow_and_subscribe` | Snapshot + continuous deltas. The primary pattern. |
| `sow_and_delta_subscribe` | Like sow_and_subscribe but deltas only contain changed fields |

### Delta Types

| Type | Meaning |
|---|---|
| `ADD` | Record now matches the subscription predicate (new or changed into match) |
| `UPDATE` | Record still matches but one or more projected fields changed |
| `REMOVE` | Record no longer matches (deleted or changed out of match) |
| `OOF` (Out of Focus) | Record left the subscription's result set (e.g., dropped out of TOP N) |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         CQServer Process                        │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────────────┐  │
│  │ TCP      │  │WebSocket │  │ Admin HTTP API               │  │
│  │Transport │  │Transport │  │ (stats, topic mgmt, config)  │  │
│  └────┬─────┘  └────┬─────┘  └──────────────────────────────┘  │
│       │              │                                          │
│       ▼              ▼                                          │
│  ┌─────────────────────────┐                                    │
│  │    Session Manager      │  ← per-connection state,           │
│  │    (async, tokio)       │    auth, rate limits                │
│  └────────────┬────────────┘                                    │
│               │                                                 │
│               ▼                                                 │
│  ┌─────────────────────────┐    ┌───────────────────────┐       │
│  │   Protocol Handler      │───▶│  Command Router       │       │
│  │   (frame decode/encode) │    │  publish / subscribe  │       │
│  └─────────────────────────┘    │  sow / unsubscribe    │       │
│                                 │  delta_subscribe      │       │
│                                 └───────┬───────────────┘       │
│                                         │                       │
│               ┌─────────────────────────┼────────────┐          │
│               ▼                         ▼            ▼          │
│  ┌────────────────────┐  ┌──────────────────┐  ┌──────────┐    │
│  │  Topic Registry     │  │ Subscription     │  │ TxLog    │    │
│  │  ┌──────────────┐  │  │ Engine           │  │ Manager  │    │
│  │  │ Topic A      │  │  │ ┌──────────────┐ │  └──────────┘    │
│  │  │ ┌──────────┐ │  │  │ │ Active Sets  │ │                  │
│  │  │ │SOW Store │ │  │  │ │ Delta Compute│ │                  │
│  │  │ │(columnar)│ │  │  │ │ Conflation   │ │                  │
│  │  │ └──────────┘ │  │  │ │ Batch/Coalesce││                  │
│  │  └──────────────┘  │  │ └──────────────┘ │                  │
│  │  ┌──────────────┐  │  └──────────────────┘                  │
│  │  │ Topic B      │  │                                        │
│  │  └──────────────┘  │         ▲                              │
│  └────────────────────┘         │                              │
│               │                 │                              │
│               └─────────────────┘                              │
│           mutation events via lock-free channel                 │
│                                                                │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Replication Engine (active-passive / active-active)     │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │   │
│  │  │ Log Shipper  │  │ Log Receiver │  │ Conflict Res. │  │   │
│  │  └─────────────┘  └──────────────┘  └───────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

```
cqserver/
├── Cargo.toml                 # Workspace root
├── crates/
│   ├── cq-core/               # Storage, schema, predicates, subscriptions
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── schema.rs      # Column types, schema model
│   │   │   ├── store.rs       # Columnar SOW store
│   │   │   ├── predicate.rs   # SQL predicate compiler
│   │   │   ├── query.rs       # Query executor (filter, sort, project, limit)
│   │   │   ├── subscription.rs # Active sets, delta computation
│   │   │   ├── topic.rs       # Topic abstraction (SOW + key + config)
│   │   │   ├── conflation.rs  # Rate-limited delta delivery
│   │   │   └── flatten.rs     # JSON flattener (dot/bracket notation)
│   │   └── Cargo.toml
│   │
│   ├── cq-protocol/           # Wire protocol, message types
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── command.rs     # Command enum (Publish, Subscribe, SOW, etc.)
│   │   │   ├── message.rs     # CqMessage: header + payload
│   │   │   ├── codec.rs       # Frame encoding/decoding (length-prefixed)
│   │   │   └── serialization.rs # JSON, FIX, binary payload codecs
│   │   └── Cargo.toml
│   │
│   ├── cq-transport/          # TCP + WebSocket async transport
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── tcp.rs         # Tokio TCP listener + framed codec
│   │   │   ├── websocket.rs   # tokio-tungstenite WebSocket listener
│   │   │   ├── session.rs     # Per-connection session state
│   │   │   └── backpressure.rs # Flow control
│   │   └── Cargo.toml
│   │
│   ├── cq-txlog/              # Transaction log (persistence + replay)
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── writer.rs      # Append-only log writer (mmap or direct IO)
│   │   │   ├── reader.rs      # Sequential replay reader
│   │   │   ├── segment.rs     # Log segment management (rotation, cleanup)
│   │   │   └── index.rs       # Sparse index for fast seek
│   │   └── Cargo.toml
│   │
│   ├── cq-replication/        # HA replication
│   │   ├── src/
│   │   │   ├── lib.rs
│   │   │   ├── shipper.rs     # Outbound log shipping
│   │   │   ├── receiver.rs    # Inbound log application
│   │   │   └── conflict.rs    # Conflict resolution (last-writer-wins, vector clocks)
│   │   └── Cargo.toml
│   │
│   └── cq-server/             # Main binary: wires everything together
│       ├── src/
│       │   ├── main.rs
│       │   ├── config.rs      # YAML/TOML config parsing
│       │   ├── admin.rs       # Admin HTTP API (stats, topic list, etc.)
│       │   └── router.rs      # Command dispatch
│       └── Cargo.toml
│
├── config/
│   └── cqserver.toml          # Default configuration
├── ARCHITECTURE.md
└── README.md
```

---

## Columnar SOW Store

### Why Columnar?

A topic with 1M records × 2000 columns stored as `HashMap<String, HashMap<String, Value>>`
would consume ~64 GB. Columnar storage with typed primitive arrays reduces this to ~16 GB
while enabling SIMD-friendly scans.

### Storage Layout

```rust
pub struct ColumnStore {
    schema: Arc<Schema>,
    
    // Typed column arrays — one per column in the schema.
    // Indexed by [column_index][row_index].
    double_columns: Vec<Vec<f64>>,       // ColumnType::Double
    long_columns:   Vec<Vec<i64>>,       // ColumnType::Long
    int_columns:    Vec<Vec<i32>>,       // ColumnType::Int
    string_columns: Vec<Vec<Option<CompactString>>>,  // ColumnType::String
    
    // Row metadata
    row_count: AtomicU32,                // Current number of rows
    row_versions: Vec<AtomicU64>,        // Per-row version for change detection
    global_version: AtomicU64,           // Monotonic mutation counter
    
    // Column → backing array index mapping
    col_to_array: Vec<(ColumnKind, usize)>,  // schema col index → (kind, array_index)
}
```

### Key Design Decisions

1. **Separate typed arrays** — no `enum Value` wrapper per cell. A `f64` column is
   a contiguous `Vec<f64>`, giving cache-line-friendly sequential scans and potential
   SIMD vectorization.

2. **`AtomicU32` row count** — allows lock-free snapshot reads. Writers increment
   atomically after writing all column values for a row.

3. **Per-row versions** — subscription evaluation can check if a row changed since
   last evaluation without comparing all column values.

4. **`CompactString`** — small-string optimization to avoid heap allocation for
   short values (trade IDs, currency codes, status strings).

---

## Content Filtering (Predicate Compiler)

### Input

SQL WHERE clause, parsed by `sqlparser-rs`:

```sql
SELECT symbol, price, quantity
FROM /market-data
WHERE desk = 'RATES' AND notional > 1000000
ORDER BY price DESC
LIMIT 100
```

### Compilation

The parser produces a `CompiledPredicate` that operates on column indices, not names.
This eliminates string lookups on the hot path:

```rust
pub enum CompiledPredicate {
    // Leaf comparisons — operate on column index directly
    EqDouble { col: usize, value: f64 },
    GtLong   { col: usize, value: i64 },
    EqString { col: usize, value: CompactString },
    Like     { col: usize, pattern: Regex },
    InString { col: usize, values: HashSet<CompactString> },
    IsNull   { col: usize },
    Between  { col: usize, low: f64, high: f64 },
    
    // Combinators
    And(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Or(Box<CompiledPredicate>, Box<CompiledPredicate>),
    Not(Box<CompiledPredicate>),
    
    // Always true (no WHERE clause)
    True,
}

impl CompiledPredicate {
    pub fn matches(&self, store: &ColumnStore, row: u32) -> bool { ... }
}
```

---

## Subscription Engine

### Lifecycle

```
Client sends: sow_and_subscribe(topic, filter, projection)
                │
                ▼
Server:  1. Parse SQL → CompiledPredicate + projection list
         2. Scan SOW → snapshot rows matching predicate
         3. Send snapshot (GROUP_BEGIN, rows, GROUP_END)
         4. Register subscription with active set = {matching row indices}
         5. On each future mutation to the topic:
            a. Evaluate predicate against mutated row
            b. Compare with active set → compute delta type
            c. If delta exists, queue for delivery
```

### Active Set

Each subscription maintains a `RoaringBitmap` of row indices currently matching
its predicate. This is memory-efficient (1M rows ≈ 125 KB per subscription) and
supports fast set operations.

### Mutation → Delta Pipeline

```
Writer thread                    Subscription evaluator threads
    │                                     │
    │  publish(topic, record)             │
    │      │                              │
    │      ├─▶ SOW upsert (ColumnStore)   │
    │      │                              │
    │      └─▶ channel.send(MutationEvent)│
    │                                     │
    │                            channel.recv()
    │                                     │
    │                            for each subscription:
    │                              predicate.matches(row)?
    │                              active_set.contains(row)?
    │                              → compute DeltaType
    │                              → queue delta
    │                                     │
    │                            batch flush → deliver deltas
```

### Conflation

Conflation controls how fast deltas are delivered to slow consumers:

| Strategy | Behavior |
|---|---|
| `none` | Deliver every delta immediately |
| `interval(ms)` | Batch deltas, flush every N ms. Coalesce same-key updates. |
| `max_backlog(n)` | If more than N deltas pending, coalesce by key (latest wins) |

---

## Wire Protocol

### Frame Format (TCP)

```
┌──────────┬───────────┬──────────────────────────────┐
│ Length(4) │ Header    │ Payload                      │
│ u32 BE   │ (variable)│ (JSON / FIX / binary)        │
└──────────┴───────────┴──────────────────────────────┘
```

Header fields (encoded as key-value pairs or fixed-position bytes):

| Field | Type | Description |
|---|---|---|
| `command` | u8 | Command type (publish, subscribe, sow, ack, etc.) |
| `command_id` | u64 | Client-assigned correlation ID |
| `sub_id` | u64 | Subscription ID (for delta delivery) |
| `topic` | string | Topic name |
| `filter` | string | SQL WHERE clause (optional) |
| `options` | string | Comma-separated options (projection, order, limit) |
| `ack_type` | u8 | Requested acknowledgment level |
| `status` | u8 | Response status (ok, error, etc.) |

### WebSocket

WebSocket uses the same logical message structure, serialized as JSON:

```json
{
  "command": "sow_and_subscribe",
  "commandId": "cmd-1",
  "topic": "/market-data",
  "filter": "desk = 'RATES' AND notional > 1000000",
  "options": "select=[symbol,price,quantity],top_n=100,conflation=500ms"
}
```

### Command Set

| Command | Direction | Description |
|---|---|---|
| `publish` | Client → Server | Publish/upsert a record to a topic |
| `subscribe` | Client → Server | Content-filtered pub/sub |
| `sow` | Client → Server | One-shot SOW query |
| `sow_and_subscribe` | Client → Server | Snapshot + continuous deltas |
| `delta_subscribe` | Client → Server | Like sow_and_subscribe, deltas are sparse (changed fields only) |
| `unsubscribe` | Client → Server | Remove a subscription |
| `heartbeat` | Bidirectional | Keep-alive |
| `logon` | Client → Server | Authentication |
| `ack` | Server → Client | Acknowledgment (processed, persisted, replicated) |
| `group_begin` | Server → Client | Start of SOW snapshot batch |
| `group_end` | Server → Client | End of SOW snapshot batch |
| `sow_delete` | Client → Server | Delete a record from SOW by key |

---

## Transaction Log

### Purpose

The transaction log provides **durability** and **replay**. Without it, the SOW
is purely in-memory and lost on restart.

### Design

- **Append-only** log file per topic, segmented by size (default 256 MB per segment)
- Each entry: `[length][crc32][timestamp][topic][key][payload]`
- On startup: replay log to reconstruct SOW
- **Sparse index**: every Nth entry gets an index entry for fast seek
- **Compaction**: periodically rewrite log keeping only latest version per key
- **fsync policy**: configurable per topic (`none`, `every_write`, `interval`)

### Recovery

```
Startup:
  1. Load topic configuration
  2. For each persistent topic:
     a. Find latest log segments
     b. Replay entries → SOW upsert (skip subscription evaluation)
     c. Build key index
  3. Enable subscription evaluation
  4. Accept client connections
```

---

## Replication

### Active-Passive

- **Primary**: accepts writes, ships log entries to standby
- **Standby**: applies log entries, serves read-only SOW queries
- **Failover**: standby promotes to primary (manual or via health check)
- **Catch-up**: new standby replays full log from primary

### Active-Active (future)

- Both nodes accept writes
- Conflict resolution: last-writer-wins (LWW) using hybrid logical clocks
- Vector clocks for causal ordering within a topic

---

## Configuration

```toml
[server]
name = "cqserver-1"

[[transport]]
type = "tcp"
listen = "0.0.0.0:9007"
max_connections = 10000

[[transport]]
type = "websocket"
listen = "0.0.0.0:9008"
path = "/cq/json"

[transport.admin]
type = "http"
listen = "0.0.0.0:8085"

[[topic]]
name = "/market-data"
key = ["/symbol"]
persist = true
conflation = "100ms"

[[topic]]
name = "/orders"
key = ["/orderId"]
persist = true

[txlog]
directory = "./data/txlog"
segment_size = "256MB"
fsync = "interval"
fsync_interval_ms = 100
compaction_interval = "1h"

[replication]
role = "primary"              # primary | standby | standalone
peer = "cqserver-2:9010"
```

---

## Performance Targets

| Metric | Target |
|---|---|
| Publish throughput | > 1M messages/sec (single topic, single writer) |
| Subscription evaluation | > 500K rows/sec per subscription |
| SOW query latency (1M rows, simple predicate) | < 50ms |
| Memory per 1M rows × 2000 columns | < 16 GB |
| Connection capacity | > 10,000 concurrent |
| Delta delivery latency (publish → subscriber) | < 1ms p99 (no conflation) |
| Startup recovery (1M rows from txlog) | < 10 seconds |

---

## Technology Choices

| Component | Choice | Rationale |
|---|---|---|
| Language | Rust | No GC, deterministic latency, memory safety, zero-cost abstractions |
| Async runtime | tokio | Industry standard for async IO in Rust |
| SQL parser | sqlparser-rs | Same parser used by DataFusion/Apache Arrow |
| Serialization | serde + serde_json | Fast, ergonomic, extensible to other formats |
| WebSocket | tokio-tungstenite | Async WebSocket on tokio |
| TCP framing | tokio-util (LengthDelimitedCodec) | Battle-tested frame codec |
| Bitmap sets | roaring-rs | Memory-efficient active sets for subscriptions |
| String storage | compact_str | Small-string optimization, avoids heap for ≤24 bytes |
| Hashing | ahash / FxHashMap | Faster than default SipHash for known-safe keys |
| Config | toml | Simple, readable, Rust-native |
| Logging | tracing | Structured, async-aware, span-based |
| Metrics | metrics + metrics-exporter-prometheus | Prometheus-compatible |

---

## AMPS Feature Parity Checklist

| AMPS Feature | CQServer Status | Notes |
|---|---|---|
| SOW (State of the World) | v1 | Columnar store |
| Content filtering (WHERE) | v1 | sqlparser-rs based |
| `subscribe` | v1 | |
| `sow` (one-shot query) | v1 | |
| `sow_and_subscribe` | v1 | |
| `delta_subscribe` | v1 | Changed-fields-only deltas |
| `sow_delete` | v1 | |
| Conflation | v1 | interval + max_backlog |
| Transaction log | v1 | Append-only + compaction |
| Multiple transports (TCP, WS) | v1 | |
| JSON serialization | v1 | |
| FIX / NVFIX serialization | v2 | |
| Binary serialization | v2 | |
| Message queues | v2 | Competing consumer pattern |
| Entitlements / auth | v2 | |
| Replication (active-passive) | v2 | |
| Replication (active-active) | v3 | |
| Regex content filters | v1 | LIKE clause |
| TOP N subscriptions | v1 | ORDER BY + LIMIT |
| OOF (Out of Focus) deltas | v1 | For TOP N |
| Admin API | v1 | HTTP REST |
| Client SDKs (JS, Python, Java) | v2 | |
| Bookmark / replay | v2 | |
| Aggregate subscriptions | v3 | GROUP BY with live updates |

# cqserver

A high-performance, content-aware messaging server written in Rust. Drop-in
replacement for [AMPS](https://www.crankuptheamps.com/) — stateful pub/sub
with continuous SQL queries over an in-memory **State-of-the-World**.

The server doesn't just route bytes. It parses every message into a typed
columnar store, evaluates SQL-like predicates on every mutation, and delivers
fine-grained deltas to subscribers whose queries match the change. SOW
snapshots stream row-by-row; live deltas fan out via an encode-once-fanout
path; persistent topics back the store with an append-only txlog.

```
┌─────────────┐  publish    ┌──────────────────────────────────────────┐
│ Publishers  │ ──────────▶ │  cqserver                                │
└─────────────┘             │   ├─ columnar SOW per topic              │
                            │   ├─ subscription evaluator (per topic) │
                            │   ├─ optional txlog (append-only, mmap-free) │
                            │   ├─ views (single-source + JOIN USING) │
                            │   └─ admin HTTP + Prometheus            │
┌─────────────┐  WS / TCP   │                                          │
│ Subscribers │ ◀──────────▶│                                          │
└─────────────┘             └──────────────────────────────────────────┘
```

---

## Highlights

- **AMPS-compatible feature surface** — SOW, content filters, `sow_and_subscribe`,
  delta_subscribe, sparse deltas, OOF events, GROUP BY aggregates, PIVOT /
  UNPIVOT, materialized views (single-source and INNER JOIN USING), queues
  with leases / DLQ / max-redelivery, bookmark replay, txlog persistence,
  primary/standby replication.
- **Streaming SOW** — snapshots fan out row-by-row, never materialized into a
  per-subscriber `Vec`. Eliminates the GB-class transient peak you'd see on
  wide topics.
- **Pure-Rust** — no C/C++ toolchain anywhere in the dependency tree. Builds
  clean on Linux, macOS, and Windows (including 24H2+).
- **Differential testing** — every SQL test case runs against both cqserver
  *and* DataFusion (Apache Arrow). 33 corpus cases cover NULL handling, LIKE,
  ORDER BY ties, aggregate edges, IN-clause semantics, PIVOT/UNPIVOT. Lives
  in [crates/cq-differential-tests/](crates/cq-differential-tests/), excluded
  from the default workspace because Arrow / DataFusion transitives now
  require cargo 1.85+. Build standalone with a newer toolchain.
- **Multi-language clients** — Rust (`cq-client`), TypeScript / Node, Python.
  A React demo — `cq · atlas`, an eight-chapter field guide built on a
  SharedWorker data layer and AG Grid — lives at
  [clients/examples-web/](clients/examples-web/). Launch via
  `./start-atlas-demo.sh` at the repo root.

---

## Quick start

### Prerequisites

| Tool | Minimum | Why |
|---|---|---|
| Rust toolchain | 1.78 (workspace MSRV) | Build the server + Rust client |
| Node.js | 18 | TS publisher + Vite for the React demo |
| `curl.exe` | bundled with Win10 1803+ / Linux / macOS | Health probes in the demo scripts |

No MSVC C++ workload, no OpenSSL, no Python — all dependencies are pure Rust
and pure Node.

### Build

```sh
cargo build --release -p cq-server
```

The first build pulls Arrow + DataFusion + tokio. Subsequent builds are cached
and finish in seconds.

### Run the demo end-to-end

The included demo spins up the server, generates a fixed-income dataset
(500 instruments × 80 books = 40 K positions, 800 K trades, live market-data
ticks), and serves both an admin dashboard and a React blotter.

| OS | Start | Stop |
|---|---|---|
| macOS / Linux | `./start-demo.sh` | `./stop-demo.sh` |
| Windows (`cmd.exe`) | `.\start-demo.bat` | `.\stop-demo.bat` |
| Windows (WSL) | as Linux | as Linux |

Then open:

| URL | What you'll see |
|---|---|
| <http://127.0.0.1:8085/> | Admin dashboard — topics, subscriptions, RSS, Prometheus metrics |
| <http://127.0.0.1:8085/fi-demo> | Galvanometer-style FI trading view |
| <http://127.0.0.1:5173/> | React demo: market data, trades, positions, aggregations, pivot |

Logs land in `.demo-run/*.log`; PIDs in `.demo-run/*.pid`.

### Talk to the server directly

```sh
# Publish a row to /trades over the WebSocket JSON protocol
echo '{"c":"publish","t":"/trades","d":{"tradeId":"T1","ticker":"AAPL","qty":100,"price":150.25}}' \
  | websocat ws://127.0.0.1:9008/cq/json

# Subscribe with a continuous SQL query
echo '{"c":"sow_and_subscribe","cid":"s1","t":"/trades","f":"qty > 50"}' \
  | websocat ws://127.0.0.1:9008/cq/json
```

Or use one of the client SDKs:

```ts
// client-sdks/ts
import { CqClient } from '@cqserver/client';
const c = new CqClient('ws://127.0.0.1:9008/cq/json');
await c.connect();
await c.subscribe('/trades', { onUpdate: (row) => console.log(row) });
```

```rust
// clients/cq-client
let client = cq_client::Client::connect("tcp://127.0.0.1:9007").await?;
let mut sub = client.sow_and_subscribe("/trades", Some("qty > 50"), None).await?;
while let Some(delta) = sub.next_delta().await { /* … */ }
```

---

## Project layout

```
cqserver/
├─ Cargo.toml                   # Rust workspace root
├─ crates/
│  ├─ cq-core/                  # SOW columnar store, query engine, views, subscriptions
│  ├─ cq-protocol/              # Wire-format messages (JSON + binary)
│  ├─ cq-transport/             # TCP + WebSocket + delivery + heartbeat
│  ├─ cq-txlog/                 # Append-only log, recovery, segment rotation
│  ├─ cq-replication/           # Primary → standby shipper + receiver
│  ├─ cq-server/                # Binary: config, admin HTTP, runtime wiring
│  ├─ cq-client/                # Rust client SDK
│  ├─ cq-loadgen/               # CLI load generator
│  ├─ cq-e2e-tests/             # End-to-end integration tests
│  └─ cq-differential-tests/    # SQL semantics tests (cqserver vs DataFusion)
├─ clients/
│  ├─ ts/                       # TypeScript client + Node demo publishers
│  ├─ react-demo/               # React + AG Grid + Vite demo
│  └─ python/                   # Async Python client (asyncio)
├─ config/
│  └─ cqserver.toml             # Demo config (topics, txlog paths, ports)
├─ start-demo.sh / .bat         # Demo launcher
├─ stop-demo.sh / .bat          # Demo teardown
└─ ARCHITECTURE.md              # Design notes
   ROADMAP.md                   # AMPS-parity roadmap
   DEMO.md                      # FI demo walkthrough
   AMPS_WORKLOG.md              # Session-by-session progress log
```

---

## Reference docs

- **[ARCHITECTURE.md](ARCHITECTURE.md)** — core concepts (topics, SOW, views,
  subscriptions, evaluator threads, replication).
- **[DEMO.md](DEMO.md)** — FI demo walkthrough with screenshots.
- **[ROADMAP.md](ROADMAP.md)** — AMPS-parity gap analysis, tiered by user impact.
- **[AMPS_WORKLOG.md](AMPS_WORKLOG.md)** — session-by-session implementation log
  (S1 — S45+).
- **[cqserver-stress-test-plan.md](cqserver-stress-test-plan.md)** — load /
  durability scenarios.

---

## Building from source on Windows

cqserver and all its tests are pure Rust + pure Node — **no MSVC C/C++
workload is required**.

```cmd
git clone https://github.com/widgetstools/cqserver.git
cd cqserver
cargo build --release -p cq-server
.\start-demo.bat
```

The demo scripts use `cmd.exe`, `wmic`, `netstat`, `taskkill`, `curl.exe` —
all stock Windows tools. **No PowerShell**, so corporate execution-policy /
AppLocker / ConstrainedLanguage rules don't apply.

Note: `wmic` is being deprecated by Microsoft and is **not installed by
default on Windows 11 24H2+**. On those builds, add it via *Settings → Apps →
Optional Features → "WMIC"* (one-time, ~2 minutes).

---

## Tests

```sh
# Unit + integration tests for the core engine and transport
cargo test -p cq-core -p cq-transport

# End-to-end (spawns a real cqserver child process per test)
cargo test -p cq-e2e-tests

# SQL differential harness (cqserver vs DataFusion, 33 corpus cases)
# Excluded from the default workspace -- needs cargo 1.85+ for Arrow's
# edition-2024 transitives. Build standalone:
(cd crates/cq-differential-tests && cargo test)

# All default-workspace tests (builds clean on cargo 1.78+)
cargo test --workspace
```

---

## License

Apache 2.0. See individual crate `Cargo.toml` files for per-crate license metadata.

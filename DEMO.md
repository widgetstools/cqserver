# cqserver — Fixed-Income Demo

Two showcases bundled together:

1. **Admin UI** at `http://localhost:8085/` — Galvanometer-style dashboard for
   browsing topics, inspecting SOW snapshots, live-tailing message streams,
   and watching Prometheus headline metrics.
2. **FI Demo UI** at `http://localhost:8085/fi-demo` — a live fixed-income
   trading view (market data ticks, recent fills, positions with P&L, plus
   aggregations by book / sector / ticker).

A Node-based generator builds realistic data — **500 instruments × 80 books =
40,000 positions**, backed by ~100k synthetic trades whose aggregate qty and
weighted-avg fill price reproduce each position. A live publisher layers
market-data ticks and continuing fills on top.

## Prerequisites

- Rust toolchain (stable)
- Node.js ≥ 18
- macOS or Linux (paths below assume macOS)

## 1. Start the server

From the repo root:

```sh
cargo run --release -p cq-server -- --config config/cqserver.toml
```

The default config exposes:

- TCP        — `127.0.0.1:9007`   (native clients)
- WebSocket  — `127.0.0.1:9008/cq/json` (browser UI)
- Admin HTTP — `127.0.0.1:8085`   (admin UI + `/stats`, `/topics`, `/metrics`)

It declares four FI topics: `/securities`, `/fi-market-data` (100 ms
conflation), `/trades` (persisted to txlog), `/positions` (50 ms conflation).

> **Important** — open the demo UIs *after* the loader has run for the first
> time. Topics use schema-on-first-publish, and any subscriber present when
> the topic is still on its placeholder schema blocks discovery. Order is:
> start server → load data → open UIs.

## 2. Generate the JSON data files (one-shot)

From `clients/ts`:

```sh
cd clients/ts
npm install            # first time only
npm run generate-fi-data
```

This writes four files under `clients/ts/examples/data/`:

| File | Rows | Approx size |
|---|---|---|
| `securities.json` | 500 | ~90 KB |
| `fi-market-data.json` | 500 | ~90 KB |
| `positions.json` | 40,000 | ~9 MB |
| `trades.json` | ~100,000 | ~27 MB |

The data is internally consistent: every position's `netQty` equals the
signed sum of its related trades, and `avgCost` is the weighted-avg of the
fills that built the position up. `marketValue = netQty × lastMid / 100`
holds row-by-row.

Tunable via env: `TARGET_POSITIONS`, `TARGET_SECURITIES`, `TARGET_BOOKS`,
`MIN_TRADES_PER_POS`, `MAX_TRADES_PER_POS`, `OUTPUT_DIR`.

## 3. Load the JSON files into the server

```sh
npm run load-fi-data
```

Reads the four JSON files and bulk-publishes them. With pipelined publishes
(500 in-flight per chunk by default), the full ~140k rows load in a few
seconds. Override target with `CQ_URL=tcp://host:9007`.

> If you ran the loader against a stale server (UI already subscribed when
> the topic had no schema), `/topics` will show `schemaDiscovered=false` and
> SOW snapshots will be empty. Fix: close any browser tabs pointing at the
> demo, restart the server, then re-run the loader.

## 4. Run the live publisher (optional)

To layer market-data ticks and continuing fills on top:

```sh
npm run publish-fi-demo
```

- Streams bid/ask/mid/yield ticks (default 20 Hz; override with `TICK_RATE`)
- Emits new fills (default 10 Hz; override with `TRADE_RATE`)
- On each tick, refreshes `marketValue` + `unrealizedPnl` for every position
  holding that cusip
- New fills land against an existing (book, cusip) position chosen at random

The publisher also has its own seed phase, so you can skip step 3 and run
just this script — but loading from JSON is faster and lets you pre-inspect
the data.

## 5. Open the UIs

- **Admin**: <http://localhost:8085/>
  Click **Inspect** on any topic for a one-shot SOW snapshot, or **Live** to
  stream deltas in real time.
- **FI demo**: <http://localhost:8085/fi-demo>
  Mid moves flash green/red, P&L is colored by sign, four aggregation panels
  recompute every second.

## 6. Sanity-check from the CLI (optional)

A small Python script that does direct SOW queries over TCP and prints
top positions, P&L by book, and a filtered `netQty > 0` query:

```sh
python3 /tmp/cq_fi_check.py
```

Expected output after the loader has run: 500 securities, ~100k trades,
40,000 non-empty positions, realistic P&L per book.

## What the demo shows

| Capability | Where to see it |
|---|---|
| Keyed columnar SOW | Admin → Inspect any topic |
| Live delta delivery | Admin → Live; FI demo |
| Bulk load throughput | Loader output (msg/s reported per chunk) |
| Conflation (50/100 ms) | Smooth UI updates at high publish rates |
| Content filtering | Try `netQty > 0` in `cq_fi_check.py` |
| Schema discovery | First publish on a topic locks the schema |
| Persisted topic + replay | Restart server — `/trades` recovers from txlog |
| Prometheus metrics | <http://localhost:8085/metrics> |
| Cross-language clients | Same demo can drive from `clients/python` or `crates/cq-client` |

## Troubleshooting

- **`Address already in use` on 9007/9008/8085** — another instance is
  still running. Find it with `lsof -ti :8085` and stop it.
- **Publisher / loader exits with `ECONNREFUSED`** — server isn't up yet;
  start it first and wait for the `listening` log line.
- **SOW snapshots come back as `{}`** — schema didn't discover. A subscriber
  attached before the first publish (typically a browser tab still pointing
  at the demo). Close any open demo UIs, restart the server, run the loader
  again, then re-open the UIs.
- **Empty `/positions` in the UI** — make sure the loader ran successfully.
  `curl http://127.0.0.1:8085/stats` should show `totalRows` ≥ ~140,000.

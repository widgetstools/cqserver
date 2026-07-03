# tests/cloud — soak topology (Bucket B, task B1)

A single persistent `cqserver` node, Atlas-shaped, under a long-running
(hours-to-days) load with Prometheus scraping its metrics the whole
time. This complements the C0 replication harness
(`docker-compose.local.yml`), which validates protocol correctness in
a few minutes — the soak topology validates **stability over time**:
memory growth, disk-bounded txlog, latency drift, connection churn.

```
            ┌───────────────────────────────┐
            │           cqserver            │  :8085 admin/metrics/ui
            │  persistent Atlas topics       │  :9007 tcp   :9008 ws
            │  checkpoint_interval_secs=10   │
            └───────────┬─────────┬──────────┘
                         │         │
                 scrape  │         │  cq-loadgen --scenario soak
                    :8085│         │  (fast / conflated / slow subs)
                         ▼         ▼
                  ┌────────────┐ ┌──────────────┐
                  │ prometheus │ │ load-driver  │
                  │  :9090     │ │ (cq-loadgen) │
                  └────────────┘ └──────────────┘
```

## Why `checkpoint_interval_secs`

`config/cqserver.toml` (the interactive Atlas demo config) only
checkpoints on graceful shutdown, so a server left running for days
would grow its txlog unboundedly on disk — this is exactly the "the
`/positions` log had reached 288 GB" failure mode the txlog docs
warn about. `tests/cloud/configs/soak.toml` sets:

```toml
[txlog]
snapshot_on_shutdown     = true
snapshot_reclaim         = true    # required for the periodic reclaim below
checkpoint_interval_secs = 10      # AMPS sow-compact-action equivalent
```

Every 10s the server fsyncs the log, writes a durable `snapshot.bin`,
and deletes the now-redundant sealed segments — bounding on-disk
growth to roughly one live segment for the entire soak, without a
restart.

## Quick start

```sh
# Build the release binaries the images expect (Dockerfile.runtime /
# Dockerfile.loadgen mount pre-built host binaries rather than building
# in-Docker — see each Dockerfile's header comment for why). This only
# works directly when the host toolchain already targets linux/amd64
# (the container platform) — see "Cross-compiling from macOS" below if
# it doesn't.
cargo build --release -p cq-server -p cq-loadgen

# Bring the soak topology + load driver up (load-driver runs for
# SOAK_DURATION_SECS, default 60s, then exits after printing its summary;
# cqserver + prometheus keep running under `restart: unless-stopped`).
docker compose -f tests/cloud/docker-compose.soak.yml up -d --build

# cqserver health:
curl -fsS http://127.0.0.1:8085/healthz

# Prometheus readiness + confirm the cqserver target is UP:
curl -fsS http://127.0.0.1:9090/-/ready
curl -s 'http://127.0.0.1:9090/api/v1/targets' | python3 -m json.tool

# Watch the load driver's progress + final summary:
docker compose -f tests/cloud/docker-compose.soak.yml logs -f load-driver

# Watch it run. Admin UI:
open http://127.0.0.1:8085/ui/
# Prometheus UI:
open http://127.0.0.1:9090/

# Tear down (drops the txlog + prometheus TSDB volumes too):
docker compose -f tests/cloud/docker-compose.soak.yml down -v
```

### Overriding the load driver's shape

`load-driver` reads `SOAK_*` env vars (see `docker-compose.soak.yml`) —
override them on the `up`/`run` invocation instead of editing the compose
file, e.g. a 1-hour soak with a heavier subscriber cohort:

```sh
SOAK_DURATION_SECS=3600 SOAK_FAST_SUBSCRIBERS=10 SOAK_CONFLATED_SUBSCRIBERS=10 \
  docker compose -f tests/cloud/docker-compose.soak.yml up -d --build load-driver
```

To re-run the driver against an already-running `cqserver` (e.g. after
tuning `SOAK_*`), `up` it again — it's `restart: "no"` so a finished
container is replaced, not restarted in place:

```sh
SOAK_DURATION_SECS=120 docker compose -f tests/cloud/docker-compose.soak.yml up -d --build load-driver
```

### Cross-compiling from macOS

`Dockerfile.runtime` / `Dockerfile.loadgen` both expect a linux/amd64
`target/release/*` binary already on the host — on Apple Silicon, `cargo
build --release` produces an `aarch64-apple-darwin` Mach-O binary that
won't run in the (linux/amd64) container. Cross-compile via a
`rust:1-bookworm` container instead (colima's docker daemon on this
project only mounts `$HOME` by default, so bind-mount the repo from a
`$HOME`-relative path or copy the source into a named volume first):

```sh
docker volume create cq-build-src
docker volume create cq-build-cargo-registry
docker volume create cq-build-target

tar --exclude='target' --exclude='.git' -cf - . \
  | docker run --rm --platform linux/amd64 -i -v cq-build-src:/work alpine sh -c "cd /work && tar -xf -"

docker run --rm --platform linux/amd64 \
  -v cq-build-src:/work \
  -v cq-build-cargo-registry:/usr/local/cargo/registry \
  -v cq-build-target:/work/target \
  -w /work rust:1-bookworm \
  bash -c "apt-get update -qq && apt-get install -y -qq pkg-config libssl-dev >/dev/null && cargo build --release -p cq-server -p cq-loadgen"

# Copy the cross-built binaries back onto the host for the Dockerfile
# `COPY target/release/...` steps to pick up:
mkdir -p target/release
docker run --rm -v cq-build-target:/t -v "$(pwd)/target/release":/out alpine \
  sh -c "cp /t/release/cqserver /t/release/cq-loadgen /out/"
```

## Services

| service | image | role |
|---|---|---|
| `txlog-perms-fix` | built from `Dockerfile.runtime`, `user: root` | one-shot init container; `chown -R cq:cq` the txlog named volume before `cqserver` starts, then exits. Needed because Docker creates named volumes root-owned, which shadows the image's `chown` at mount time and would otherwise make `cqserver` (running as the unprivileged `cq` user) fail to write its txlog with a permission error on first boot |
| `cqserver` | built from `Dockerfile.runtime` | persistent node under `tests/cloud/configs/soak.toml`; `restart: unless-stopped` so a soak survives a container crash; waits on `txlog-perms-fix` completing successfully |
| `load-driver` | built from `Dockerfile.loadgen` | **B2** — runs `cq-loadgen --scenario soak` against `cqserver:9007` (tcp) / `cqserver:8085` (admin). Seeds wide `/positions` rows, then ticks `delta_publish` while holding 3 subscriber classes open (fast firehose, conflated, deliberately-slow) for `SOAK_DURATION_SECS`; logs progress + a final summary. `restart: "no"` — it's a bounded-duration run, not a daemon; re-`up` it to run again |
| `prometheus` | `prom/prometheus:v2.55.1` | scrapes `cqserver:8085/metrics` every 10s per `prometheus-soak.yml`; 30d retention so a multi-day soak's whole history stays queryable |

## Files

```
tests/cloud/
├── docker-compose.soak.yml   ← this topology
├── Dockerfile.loadgen        ← packages cq-loadgen for the load-driver service
├── prometheus-soak.yml       ← Prometheus scrape config (10s interval)
├── configs/
│   └── soak.toml             ← persistent Atlas-shaped config, checkpoint_interval_secs=10
└── SOAK.md                   ← you are here
```

`soak.toml` reuses `config/schemas/*.json` (mounted read-only at
`/etc/cqserver/schemas`) — the same schema files
`config/cqserver.toml` uses for the interactive Atlas demo — so the
soak's `/positions`, `/trades`, `/securities`, `/risk` topics and the
`/v_net_exposure`, `/v_book_totals`, `/v_trades_by_compliance` views
match the demo's shapes exactly. `max_sow_estimated_bytes` /
`hard_max_sow_result_bytes` are raised the same way, since Atlas rows
are 200+ columns wide.

## Analyzing a soak run (Bucket B, task B3)

`cq-loadgen --scenario soak-analyze` reads Prometheus over the run
window and prints a machine-checkable verdict, so a multi-day soak
self-judges instead of a human staring at Grafana. Four criteria, each
PASS/FAIL with measured value + threshold:

| criterion | metric(s) | what FAILs it |
|---|---|---|
| `rss_slope` | `cq_process_rss_bytes` | linear-fit slope (after excluding the first 10% of the window as warmup) implies growth beyond `--soak-analyze-max-rss-growth-mb-per-hour` (default 50 MB/hour) — a leak |
| `drop_ratio` | `cq_deltas_dropped_total`, `cq_subscription_dropped_total`, `cq_deltas_delivered_total` | (deltas-route drops + conflated/subscription-route drops) / delivered ratio over the window exceeds `--soak-analyze-max-drop-ratio` (default 0.05). Covers both drop counters: `cq_deltas_dropped_total` for the direct (non-conflated) route and `cq_subscription_dropped_total` for the conflated route (any topic with `conflation_ms` set, e.g. `/positions` — the soak's primary topic). The conflator (`crates/cq-transport/src/session.rs`) never touches `cq_deltas_dropped_total`/`cq_deltas_delivered_total`, so checking only the deltas counter would be a near-no-op for the shipped conflated topology. The slow-consumer class is *expected* to cause some drops — this bounds them, it doesn't require zero |
| `txlog_bounded` | `cq_txlog_bytes` (per-topic on-disk-size gauge, summed), `cq_txlog_checkpoint_total`, `cq_txlog_segments_reclaimed_total` | linear-fit slope of `cq_txlog_bytes` (after excluding the first 10% of the window as warmup) implies growth beyond `--soak-analyze-max-txlog-growth-mb-per-hour` (default 50 MB/hour) — reclaim is losing the race against the write rate, so disk grows unboundedly — **or** no checkpoints fired, or no segments were ever reclaimed, over the window (activity check, kept as a complementary signal) |
| `p99_publish_latency` | `cq_publish_latency_us` (`histogram_quantile(0.99, ...)`) | the worst p99 sample in the window exceeds `--soak-analyze-max-p99-latency-us` (default 50000 = 50ms) |

Run it after (or during) a soak, pointed at the soak's Prometheus:

```sh
# Against the docker-compose soak topology's Prometheus (port 9090
# above), analyzing the last hour:
cargo run -p cq-loadgen -- --scenario soak-analyze \
  --prometheus-url http://127.0.0.1:9090 \
  --soak-analyze-last-minutes 60

# Or an explicit window (Unix seconds) — useful for a multi-day soak
# where you want to analyze a specific day's slice:
cargo run -p cq-loadgen -- --scenario soak-analyze \
  --prometheus-url http://127.0.0.1:9090 \
  --soak-analyze-start 1735689600 --soak-analyze-end 1735776000
```

It exits nonzero on `SOAK VERDICT: FAIL`, so it composes directly into
CI or a runbook gate, e.g.:

```sh
cq-loadgen --scenario soak-analyze --prometheus-url http://127.0.0.1:9090 \
  --soak-analyze-last-minutes 1440 || echo "soak failed — see verdict table above"
```

The verdict math (linear fit, drop ratio, checkpoint/reclaim presence,
txlog byte-growth bound, p99 threshold) lives in
`crates/cq-loadgen/src/soak_analyze.rs` as pure functions and is
unit-tested against synthetic metric series (leaking RSS → FAIL, flat
RSS → PASS, runaway drops → FAIL, bounded drops → PASS, drops via
`cq_subscription_dropped_total` alone correctly ratio'd (not read as
zero) → PASS/FAIL as appropriate, both drop counters absent → PASS,
no reclaim events → FAIL, reclaim events present → PASS, linearly-growing
`cq_txlog_bytes` → FAIL even with healthy checkpoint/reclaim activity,
sawtooth/flat `cq_txlog_bytes` → PASS) — no live Prometheus needed for
`cargo test -p cq-loadgen`.

**Byte-bound gap closed**: the server now exports `cq_txlog_bytes{topic=...}`
— a per-topic gauge of the txlog directory's total on-disk size (sum of
segment file sizes + the durable snapshot file), emitted on every
periodic checkpoint tick (`crates/cq-server/src/main.rs`,
`run_checkpointer`; computed by `Topic::txlog_disk_bytes()` in
`crates/cq-core/src/topic.rs`). `txlog_bounded` now asserts a real
bounded-disk guarantee via a linear fit of this gauge (summed across
topics), the same warmup-excluded-slope approach as `rss_slope` — not
just checkpoint/reclaim activity, which can look perfectly healthy
while the write rate outpaces reclaim and disk grows unboundedly
anyway. The checkpoint/reclaim activity checks are kept as a
complementary signal (they catch a different failure mode: checkpointing
disabled/broken outright).

## What this does NOT do (yet)

- **Single node** — no replication/failover in this topology; that's
  Bucket C (HA/failover).
- **No per-client conflation knob** — the "conflated" subscriber class
  subscribes the same way as "fast"; conflation comes entirely from the
  topic's own `conflation_ms` (set on `/positions` in `soak.toml`). The
  SDK has no client-side conflation option to select — see the `soak`
  scenario's doc comment in `crates/cq-loadgen/src/scenarios.rs` for
  the trace of where that's defined server-side.

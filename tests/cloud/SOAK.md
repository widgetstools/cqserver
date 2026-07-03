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

## What this does NOT do (yet)

- **Single node** — no replication/failover in this topology; that's
  Bucket C (HA/failover).
- **No automated pass/fail** — this is an operator-driven harness for
  watching Prometheus/the admin UI over a long run, not a CI gate. The
  load driver's own summary line is the closest thing to a pass/fail
  signal today (issued/acked/errors + per-class delivered counts);
  analyzing the Prometheus history over a multi-day run is Bucket B's
  next task (B3).
- **No per-client conflation knob** — the "conflated" subscriber class
  subscribes the same way as "fast"; conflation comes entirely from the
  topic's own `conflation_ms` (set on `/positions` in `soak.toml`). The
  SDK has no client-side conflation option to select — see the `soak`
  scenario's doc comment in `crates/cq-loadgen/src/scenarios.rs` for
  the trace of where that's defined server-side.

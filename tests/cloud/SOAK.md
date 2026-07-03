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
                 scrape  │         │  healthz polls (B1) /
                    :8085│         │  real load (B2)
                         ▼         ▼
                  ┌────────────┐ ┌──────────────┐
                  │ prometheus │ │ load-driver  │
                  │  :9090     │ │ (placeholder)│
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
# Build the release binary the image expects (Dockerfile.runtime
# mounts a pre-built host binary rather than building in-Docker —
# see the Dockerfile header comment for why).
cargo build --release -p cq-server

# Bring the soak topology up.
docker compose -f tests/cloud/docker-compose.soak.yml up -d --build

# cqserver health:
curl -fsS http://127.0.0.1:8085/healthz

# Prometheus readiness + confirm the cqserver target is UP:
curl -fsS http://127.0.0.1:9090/-/ready
curl -s 'http://127.0.0.1:9090/api/v1/targets' | python3 -m json.tool

# Watch it run. Admin UI:
open http://127.0.0.1:8085/ui/
# Prometheus UI:
open http://127.0.0.1:9090/

# Tear down (drops the txlog + prometheus TSDB volumes too):
docker compose -f tests/cloud/docker-compose.soak.yml down -v
```

## Services

| service | image | role |
|---|---|---|
| `txlog-perms-fix` | built from `Dockerfile.runtime`, `user: root` | one-shot init container; `chown -R cq:cq` the txlog named volume before `cqserver` starts, then exits. Needed because Docker creates named volumes root-owned, which shadows the image's `chown` at mount time and would otherwise make `cqserver` (running as the unprivileged `cq` user) fail to write its txlog with a permission error on first boot |
| `cqserver` | built from `Dockerfile.runtime` | persistent node under `tests/cloud/configs/soak.toml`; `restart: unless-stopped` so a soak survives a container crash; waits on `txlog-perms-fix` completing successfully |
| `load-driver` | same runtime image | **placeholder in B1** — a healthz poll loop. B2 swaps `command:` for the real soak driver against `cqserver:9007`/`:9008`; same network/service name so no topology changes are needed |
| `prometheus` | `prom/prometheus:v2.55.1` | scrapes `cqserver:8085/metrics` every 10s per `prometheus-soak.yml`; 30d retention so a multi-day soak's whole history stays queryable |

## Files

```
tests/cloud/
├── docker-compose.soak.yml   ← this topology
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

- **No real load** — B1's `load-driver` only polls `/healthz`. B2
  implements the actual soak workload (sustained publish rate +
  subscriber churn).
- **Single node** — no replication/failover in this topology; that's
  Bucket C (HA/failover).
- **No automated pass/fail** — this is an operator-driven harness for
  watching Prometheus/the admin UI over a long run, not a CI gate.

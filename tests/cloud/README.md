# tests/cloud — replication test harness

Implements **C0** from [`CLOUD_REPLICATION_TEST_WORKLOG.md`](../../CLOUD_REPLICATION_TEST_WORKLOG.md):
a local docker-compose cluster that validates replication +
replica-reads on a developer laptop with zero cloud spend.

```
            ┌─────────────────┐
            │     leader      │  :8085 admin/ui  :9007 tcp  :9008 ws
            │  role=primary   │  ships → follower1:9010 + follower2:9010
            └────────┬────────┘
       ┌─────────────┴──────────────┐
       ▼                            ▼
  ┌──────────┐                ┌──────────┐
  │ follower1│  :8086 admin   │ follower2│  :8087 admin
  │role=stby │  rcv :9010     │role=stby │  rcv :9010
  └──────────┘                └──────────┘
```

## Quick start

```sh
# Build cqserver release binary + start 3-node cluster + wait healthy
make local-up

# Run convergence assertions (publish to leader → assert both
# followers catch up → SOW comparison → failure injection)
make local-test

# Tear it all down (removes volumes too)
make local-down
```

Open admin UIs in three browser tabs to watch the replication
sequence numbers tick in lockstep:

- http://127.0.0.1:8085/ui/  — leader
- http://127.0.0.1:8086/ui/  — follower1
- http://127.0.0.1:8087/ui/  — follower2

Each follower's `Replication` page shows the shipped / applied /
acked sequences per topic.

## What this proves

| Behavior | How |
|---|---|
| Multi-peer shipper fans out 1 → N | `[replication].peers` array in `configs/leader.toml`; `assert-converged.sh` verifies both followers receive every published row |
| Follower rejects publishes | `role = "standby"` blocks publish at the router; covered by the existing `tcp::tests::tcp_read_only_rejects_publish` integration test |
| Hello-with-highwater resume | `assert-converged.sh` stops follower2, publishes more rows, restarts follower2, asserts catch-up |
| Sequence monotonicity | Followers' `cq_repl_applied_max_sequence` only goes up |
| Multi-URI client failover | The `--scenario stress2k-real` smoke variant connects via the multi-URI list (TODO C0+: wired in once `connect_any` lands in the loadgen harness) |

## What this does NOT prove

Cloud-specific behavior — see C1 in the worklog. Specifically:

- Real-network latency / packet loss
- NIC saturation arithmetic at 2K+ subs
- TLS handshake costs at scale
- Cross-host clock skew
- L4 LB behaviour

Loopback can't disprove or confirm any of those. C0 validates the
*protocol*; C1 validates the *deployment shape*.

## Files

```
tests/cloud/
├── README.md                      ← you are here
├── SOAK.md                        ← Bucket B soak/scale topology (see below)
├── Makefile                       ← build / up / test / down / logs
├── Dockerfile.runtime             ← thin Debian runtime; binary mounted in
├── docker-compose.local.yml       ← 3-node compose topology (C0)
├── docker-compose.soak.yml        ← soak topology (B1): cqserver + load-driver + prometheus
├── prometheus-soak.yml            ← Prometheus scrape config for the soak topology
├── configs/
│   ├── leader.toml                ← role=primary, peers=[follower1, follower2]
│   ├── follower1.toml             ← role=standby, listen=:9010
│   ├── follower2.toml             ← role=standby, listen=:9010
│   └── soak.toml                  ← persistent Atlas-shaped config, checkpoint_interval_secs=10
└── scripts/
    └── assert-converged.sh        ← test driver: publish + wait + verify
```

## Soak / scale topology (Bucket B)

`docker-compose.soak.yml` stands up a single persistent `cqserver`
node with periodic txlog checkpointing (`checkpoint_interval_secs`)
so a multi-day run stays disk-bounded, plus a Prometheus instance
scraping `:8085/metrics`. See [`SOAK.md`](./SOAK.md) for the full
writeup, including why `checkpoint_interval_secs` matters and how to
bring it up / verify it / tear it down.

```sh
docker compose -f tests/cloud/docker-compose.soak.yml up -d --build
curl -fsS http://127.0.0.1:8085/healthz
curl -fsS http://127.0.0.1:9090/-/ready
docker compose -f tests/cloud/docker-compose.soak.yml down -v
```

## Troubleshooting

**`make local-up` hangs on healthcheck**

```sh
make local-status                  # short summary
docker logs cq-c0-leader           # full leader log
docker logs cq-c0-follower1
```

Common cause: stale ports from a previous run. Run `make local-down`
first.

**Followers stay at sequence 0**

The leader's shipper failed to connect. Two usual reasons:

1. **`peers` not set** in `configs/leader.toml` — confirm with
   `curl http://127.0.0.1:8085/admin/replication | python3 -m json.tool`.
2. **Follower receiver port mismatch** — confirm with
   `docker logs cq-c0-follower1 | grep listen`. The follower
   should log `Replication receiver listening addr=0.0.0.0:9010`.

**`assert-converged.sh` exits "no rows" but publisher ran**

The publisher uses the `publish-throughput` scenario which publishes
at `rate × duration_secs`. If rate is 500 and duration is < 2 seconds,
fewer rows arrive than the script expects. Set `N_ROWS=200
DEADLINE_SEC=20 ./scripts/assert-converged.sh` to dial it back.

## CI

`.github/workflows/cloud-c0.yml` runs this harness on every PR that
touches `crates/cq-replication/**`, `crates/cq-server/**`, or
`tests/cloud/**`. Build + up + test + down completes in < 5 min on
the GitHub Actions free runner.

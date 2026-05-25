# Production Readiness — gap analysis + roadmap

**Status as of `msrv-1.78` HEAD:** cqserver is dev-validated and
feature-rich, but there is real non-functional work between this point
and "we'd let a regulated production environment depend on this."

This document is the honest punch-list. Grouped by what would **block**
a deploy (P0), what would **hurt** under one (P1), and what would
**help** but isn't blocking (P2). Each item is concrete enough that a
session-scoped worklog could be written against it.

---

## What we have today

For grounding — these are the capabilities that already shipped on
`msrv-1.78`, recorded in their respective worklogs:

| Area | State | Worklog |
|---|---|---|
| Memory caps (H1 + H2 + H4) | ✅ 2K-sub stress: 934 MB peak firehose / 137 MB realistic | [`HIGH_SCALE_WORKLOG.md`](HIGH_SCALE_WORKLOG.md) |
| Replication transport | ✅ Active-passive shipper + receiver | `crates/cq-replication/` |
| Replica reads (S1 + S2a) | ✅ `role = "standby"` rejects publishes; `Client::connect_any` initial failover | [`REPLICA_READS_WORKLOG.md`](REPLICA_READS_WORKLOG.md) |
| Query guardrails (G1–G5) | ✅ Parse rules + cost estimator + subscribe gate + runtime caps + per-user budgets | [`QUERY_GUARDRAILS_WORKLOG.md`](QUERY_GUARDRAILS_WORKLOG.md) |
| Admin UI (U1–U7 + hotkeys) | ✅ Galvanometer-class console served from `/ui` under the admin port | [`ADMIN_UI_WORKLOG.md`](ADMIN_UI_WORKLOG.md) + [`docs/admin-ui.md`](docs/admin-ui.md) |
| Auth | ✅ bcrypt + JWT (HS256) + entitlements + row_filter | `crates/cq-transport/src/auth.rs` |
| TLS on TCP | ✅ Cert/key per `[transport.tls]` | `crates/cq-transport/src/tls.rs` |
| Prometheus metrics | ✅ `/metrics` text format | `crates/cq-server/src/admin.rs` |
| Allocator | ✅ jemalloc with tuned decay + background threads | `crates/cq-server/src/main.rs` |
| Test coverage | ✅ 127 cq-core / 39 cq-transport / 13 cq-server / multi e2e | workspace |
| Differential SQL tests | ✅ Every SQL fixture cross-checked against DataFusion | `crates/cq-differential-tests/` |

The interesting realization: the **feature** work is essentially done.
Almost everything below is **non-functional** work — security,
operability, governance — that doesn't show up in feature lists but is
exactly what every production deploy demands.

---

## P0 — would block a production deploy

Any decent security / ops review refuses to sign off until these land.

### P0.1 Admin port has no authentication

Anyone on the network reaching the admin port (default `:8085`) can:
- Drop subscriptions (`DELETE /subscriptions/:id`)
- Rotate topic journals (`POST /admin/rotate-journal/:topic`)
- Shrink stores (`POST /admin/shrink-store-all`)
- Read the entire `cqserver.toml` (`GET /admin/config`)
- Enumerate every topic, view, queue, subscription, and replication peer
- Inspect arbitrary topic state via the admin UI

**Action:** add JWT or mTLS to the admin server. Default
`admin_addr = "127.0.0.1:8085"` so the unauthenticated path is
loopback-only by default. Document the reverse-proxy pattern for
production fronting (nginx + OAuth proxy, or AWS ALB + Cognito, etc).

### P0.2 Admin port has no TLS

TCP transport supports TLS; admin HTTP doesn't. Production browsers
won't load `http://admin.cqserver.example` without warnings; ops
tooling shouldn't either. Add an optional `[admin.tls]` block mirroring
`[transport.tls]`.

### P0.3 No connection / rate limits

No `max_connections`, no per-IP throttle, no `max_sessions_per_user`.
A misbehaving client (or attacker) can exhaust:
- The tokio runtime's accept-loop budget
- The file-descriptor ceiling (default ulimit is 1024 on many distros)
- The per-session memory budget × N connections

**Action:** new `[transport.limits]` config with:
- `max_connections` — hard cap on the accept loop
- `max_connections_per_ip` — soft cap with progressive backoff
- `accept_rate_per_sec` — token-bucket on new TCP accepts
- `max_sessions_per_user` — once auth lands, cap per-user concurrency

### P0.4 Backup / restore procedure isn't documented

Persistent topics live in per-topic `txlog/{slug}/*.log` segment files.
The on-disk format is durable but operators need:
- A documented snapshot strategy (rsync? LVM snapshot? tar?)
- Point-in-time recovery procedure (truncate txlog to a target seq + reload SOW)
- A validated "restore from cold" runbook (write it as a script; gate it on a CI smoke test)

### P0.5 No audit log

Nothing records:
- Who dropped which subscription, when, from which IP
- Who rotated which journal, shrunk which store
- Who logged in successfully (or unsuccessfully), and from where
- Who escalated their entitlements via JWT claim manipulation

Required for SOX / SOC2 / any regulated buyer. Add an `[audit]` block
with a sink (file path or syslog) and one log line per admin action +
auth event.

### P0.6 Graceful shutdown under load isn't verified

`SIGTERM` handling exists; the empty-queue path is covered by tests.
We haven't validated:
- 2K-sub stress + SIGTERM → does every in-flight publish reach the txlog before exit?
- Persistent-topic fsync deadline under load — is the 60s shutdown budget enough?
- Replication-in-flight on SIGTERM — are unacknowledged shipper entries durable?

**Action:** add a `cargo test --test graceful_shutdown_under_2k_load`
test that publishes during stress, sends SIGTERM, restarts, and asserts
no published row is missing from the post-restart SOW.

### P0.7 Secrets are in TOML

`auth.users.password_hash`, `auth.jwt.secret`, and (future) TLS key
paths all live in `cqserver.toml`. Production needs:
- HashiCorp Vault / AWS Secrets Manager / k8s Secrets / GCP Secret Manager integration
- At minimum, `file://` indirection so plaintext doesn't sit in the TOML
- `env://VAR` substitution exists today for non-secret config; document the secret subset

---

## P1 — would hurt under a production deploy

Survivable, but you'd regret it within the first month.

### P1.1 Live reconnect-on-loss in the Rust client (S2b deferred)

Replica-reads relies on this — when a follower restarts or the L4 LB
fails over, every subscriber should resume transparently. Today the
application has to handle reconnect itself.

Tracked in [`REPLICA_READS_WORKLOG.md`](REPLICA_READS_WORKLOG.md) §S2b.

### P1.2 Multi-peer shipper

The shipper has one `peer` field per process. Multi-follower fanout
from a single leader requires multiple shipper processes. A
`peers: Vec<ReplicationPeer>` extension on `[replication]` would fix
this.

### P1.3 No auto-failover

Leader dies → operator manually promotes a follower. Not always wrong
(consensus is genuinely hard, see Raft / Paxos), but operators need:
- A documented failover runbook with under-5-minute MTTR
- A scripted promotion step (`cqserver-promote --instance follower-2`)
- A chaos-style test that exercises it

### P1.4 Config hot-reload

Adding a topic / user / view requires restart. At minimum,
**user + entitlements + TLS certs** should be live-reloadable:
- On-call needs to revoke a compromised credential without dropping every connection
- Certificate renewal shouldn't drop every TCP session

`SIGHUP` → re-read `cqserver.toml` → diff users/entitlements/certs →
apply.

### P1.5 Per-user resource quotas beyond G5

G5 covers per-user query cost. It doesn't cover:
- A user pushing 100K small publishes/sec (publish-rate cap)
- A user pinning 10K subscriptions (session-count cap, P0.3 covers this partly)
- A user's subscriptions collectively consuming 5 GB of outbound queue (memory-quota cap)

### P1.6 OpenTelemetry tracing

Today `tracing` outputs structured logs. Production wants spans
exportable to Jaeger / Tempo / Honeycomb. A publish → router → topic
→ evaluator → fanout → subscriber path should be one continuous
trace.

`tracing-opentelemetry` integration with OTLP gRPC export.

### P1.7 Structured JSON logging

Current text format is operator-friendly. Centralized aggregation
(ELK / Loki / Datadog) wants JSON. Add a `[logging] format = "json"`
option (S25 sink format hook already exists).

### P1.8 Soak tests in CI

Today CI runs unit + integration + e2e. Production-grade CI also runs:
- 24-hour continuous 2K-sub workload before each release
- Alert on slow RSS climb (regression watchdog)
- Alert on FD leak (open file count vs. expected baseline)
- Alert on segment-file growth not matching mutation rate

### P1.9 Performance regression gates

`stress2k` baseline numbers committed as a build artifact. PRs that
regress p50/p99 latency or peak RSS by more than X% fail CI. Avoid the
"oops, our subscribe latency doubled three releases ago" failure mode.

### P1.10 Distinct `/readyz` vs `/healthz`

- `/livez` → process is alive (current `/healthz`)
- `/readyz` → ready to accept traffic — replication caught up, startup
  txlog replay finished, all configured topics registered

Kubernetes / load balancers need this distinction to route traffic
correctly during startup and rolling restarts.

---

## P2 — would help, but isn't blocking

### P2.1 Container image + Helm chart

Today: a binary launched from a shell. Standard production deploy:
- Docker / OCI image (multi-stage Rust build → minimal base)
- Helm chart with sensible defaults (PVC for txlog, ConfigMap for cqserver.toml, Secret for JWT key)
- Kubernetes manifests (StatefulSet for leader/follower, Service for L4 LB)

### P2.2 systemd unit

For bare-metal deploys, drop-in `cqserver.service` with proper
`ExecStop=`, `Restart=on-failure`, `LimitNOFILE=65536`, etc.

### P2.3 Java / C# / Go client SDKs

Today: Rust, TypeScript, Python. Banks and other regulated buyers will
expect Java. Modern Go shops will expect Go. The wire protocol (S28
versioning) is documented enough that this is mechanical work.

### P2.4 Active-active replication

Consensus + conflict resolution. Out of scope for most use cases but
limits geographic deployment shapes. Would consume a multi-week project
in its own right.

### P2.5 Cold-tier storage

Sealed + archived txlog segments → S3 / Azure Blob / GCS for cheap
long-term retention. Today archive directory is local-filesystem-only.

### P2.6 Cross-region replication

Existing shipper assumes low-latency LAN. Geo deployments need:
- Larger reconnect backoff
- Documented split-brain story
- Compression on the wire (H3 zstd measurement done; lib swap deferred)

### P2.7 Schema migration story

What happens when an operator changes a topic's `key_fields` between
restarts? Today: silent. Need a startup-time check that compares the
declared schema against any persistent txlog header and refuses to
start (or migrates) on mismatch.

### P2.8 End-to-end checksum

Optional row checksum (e.g. xxhash64) carried alongside `sequence` so
subscribers can detect transit corruption — useful when traffic
crosses untrusted network segments.

### P2.9 Supply chain hygiene

- `cargo audit` in CI
- `cargo deny --check licenses` in CI
- SBOM generation (`cargo sbom` or equivalent) on release
- Dependency pinning via `Cargo.lock` already present; document the freeze policy

---

## Concrete next-step worklogs (in execution order)

The path I'd take from here to "production-ready" — roughly 6 weeks of
focused work:

| # | Worklog | Closes | Effort |
|---|---|---|---|
| 1 | **`AUTH_HARDENING_WORKLOG.md`** | P0.1, P0.2, P0.3, P0.5 | ~1 week |
| 2 | **`OPS_READINESS_WORKLOG.md`** | P0.4, P0.6, P1.7, P1.10, P2.1, P2.2 | ~1 week |
| 3 | **`HA_WORKLOG.md`** | P1.1 (S2b), P1.2, P1.3 | ~2 weeks |
| 4 | **`OBSERVABILITY_WORKLOG.md`** | P1.6, P1.8, P1.9 | ~1 week |
| 5 | **`SECRETS_AND_RELOAD_WORKLOG.md`** | P0.7, P1.4 | ~1 week |

The first two worklogs (~2 weeks) cover the items that would actually
**fail a security review**. The rest are about operational confidence
under sustained production load.

---

## Definition of "production-worthy"

For lockdown: cqserver passes our internal definition of production
when all of the following are true:

- [ ] Admin endpoints require authentication (P0.1) and TLS (P0.2)
- [ ] Connection limits + per-IP rate-limiting enforced (P0.3)
- [ ] Backup + restore runbook is scripted and exercised in CI (P0.4)
- [ ] Audit log captures every admin action + auth event (P0.5)
- [ ] Graceful shutdown under 2K-sub stress is test-gated (P0.6)
- [ ] Secrets come from a secret manager, not TOML (P0.7)
- [ ] `/readyz` is distinct from `/healthz` (P1.10)
- [ ] 24-hour soak test runs in CI on every release branch (P1.8)
- [ ] Performance regression gates fail PRs that hurt p50/p99 (P1.9)
- [ ] Container image + Helm chart published per release (P2.1)

The 10-item checklist above is the minimum. Everything else in this
document is value-add beyond minimum, not value-required.

---

## Related documents

- [`ARCHITECTURE.md`](ARCHITECTURE.md) — system design reference
- [`HIGH_SCALE_WORKLOG.md`](HIGH_SCALE_WORKLOG.md) — H1–H6 (memory + scale)
- [`REPLICA_READS_WORKLOG.md`](REPLICA_READS_WORKLOG.md) — S1–S3 (replication for read fan-out)
- [`QUERY_GUARDRAILS_WORKLOG.md`](QUERY_GUARDRAILS_WORKLOG.md) — G1–G5 (query cost guardrails)
- [`ADMIN_UI_WORKLOG.md`](ADMIN_UI_WORKLOG.md) — U1–U7 + hotkey follow-up
- [`docs/admin-ui.md`](docs/admin-ui.md) — operator console deploy + reference
- [`docs/deploy/replica-reads.md`](docs/deploy/replica-reads.md) — multi-host deployment guide

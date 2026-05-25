# Cloud Replication Test Worklog

**Goal.** Validate cqserver's replication + replica-reads behaviour at
realistic scale (multi-host, real network, real NIC bandwidth) without
turning the cloud bill into a recurring nightmare. Three stages of
escalating confidence + cost, each independently shippable.

The work is non-functional — the replication code already exists
([`crates/cq-replication/`](crates/cq-replication/) and [`REPLICA_READS_WORKLOG.md`](REPLICA_READS_WORKLOG.md))
— and unblocks the deferred S3b multi-instance state-convergence test
plus the soak items in [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md) (P1.8).

**Why this matters.** Today our 2K-sub stress numbers come from
loopback. Loopback effectively has infinite bandwidth, so the entire
*reason* for replica-reads — splitting NIC egress across hosts —
can't be validated on a developer laptop. Without a real-network
measurement we're trusting our extrapolation; with one, we have
defensible operator numbers.

**Cost calculus (self-contained for future reference).** The
production-realistic minimum experiment is `1 leader + 2 followers +
1 loadgen` for ~2 hours per run. At AWS Spot pricing this is ~$0.33/hr
total → **~$0.66 per focused measurement run**. A 24-hour pre-release
soak is ~$8. A 7-day soak is ~$55. The cost-vs-confidence ratio is
excellent **if** we stay disciplined about teardown — the bills
explode from forgotten instances, not from experiments.

---

## Scope guard

In scope:
- A local docker-compose cluster that validates ~80% of replication
  correctness at $0/hr.
- A Terraform-managed AWS Spot bench for the cloud-specific 20%
  (real network behaviour, real NIC bandwidth, TLS at scale).
- A pre-release soak workflow that runs the same bench for 24 hours
  on a release-tag push.
- Cost guardrails strong enough that an idle bench burns < $1/day
  worst case.
- Result publishing so a measurement run yields versioned, comparable
  artifacts (peak RSS, replication lag, NIC utilization, subscribe
  p50/p99).

Out of scope:
- **Active-active replication.** Single-leader only. Multi-master is
  P2 in `PRODUCTION_READINESS.md` and a multi-week project on its
  own.
- **Cross-region testing.** Same-AZ only for this worklog. Cross-region
  is a P2 item in its own right; the current shipper isn't tuned for
  WAN latency.
- **Kubernetes / Helm.** This worklog is plain EC2 + Terraform.
  Containerized deploys are P2.1 in `PRODUCTION_READINESS.md`.
- **Chaos testing** (network partition, kill -9 mid-fsync, etc.).
  Worth doing later in an `HA_WORKLOG`; this worklog measures the
  happy path + graceful failover only.

---

## Existing pieces (verified before scoping)

What's already in the tree that this worklog builds on:

- **`crates/cq-replication/`** — shipper + receiver, Hello-with-
  highwater protocol, filter + transform on the wire (S12), per-topic
  sequence tracking. Loopback-tested.
- **Replica-reads S1** — `role = "standby"` server mode that rejects
  publishes with `"read-only follower"`. Wire-validated through the
  TCP boundary.
- **`Client::connect_any`** (S2a) — initial-connect failover across
  a multi-URI list. Random-order selection.
- **`/admin/replication`** — JSON endpoint reporting role + peer +
  listen + per-topic sequences. Consumed by the admin UI's
  Replication page.
- **`cq-loadgen`** with `stress2k` + `stress2k-real` scenarios. The
  realistic scenario produces the 2K-sub-with-book-filter workload
  we already used for local stress.
- **Per-topic metrics** — `cq_repl_shipped_max_sequence`,
  `cq_repl_applied_max_sequence`, `cq_repl_acked_max_sequence`,
  `cq_repl_connect_total`, `cq_repl_reconnect_total`,
  `cq_repl_session_error_total`. All consumed by the
  Replication admin screen.
- **TLS on TCP** (`[transport.tls]` block) — required for any
  internet-exposed bench.

What's deliberately **missing** and what we'll add:

1. No multi-process orchestration locally — every existing test runs
   in-process or against `start-demo.sh`. We'll add docker-compose.
2. No infrastructure-as-code. We'll add Terraform modules.
3. No result-publishing path. We'll add S3-backed artifacts.
4. No budget alarms / auto-teardown. We'll add CloudWatch + a Lambda
   watchdog.

---

## Cost guardrails (non-negotiable)

These are the rules that keep the project sustainable:

1. **Spot, always.** Reserve nothing. A 2-hour interruption is a
   "rerun and move on," not a recoverable engineering loss.
2. **One AZ, one VPC, no NAT Gateway.** All replication traffic stays
   intra-AZ (free). No internet egress = no surprise data-transfer
   charges. Inbound SSH via instance public IPs + security-group
   allowlist.
3. **Terraform `apply` and `destroy`, never console clicks.** Every
   bench artifact is in `tests/cloud/` and reproducible from `main`.
4. **Tag everything** with `project=cqserver-cloud-test owner=$USER
   auto-shutdown=true expires=<iso8601>`. Cost Explorer slices on
   `project` give an exact dollar figure per session.
5. **Auto-shutdown Lambda.** Any tagged instance running > 6 hours
   with < 5% CPU → terminate. Catches forgotten benches even when
   the operator forgot.
6. **Hard timeout on the wrapper script.** `run-cloud-stress.sh`
   schedules an EventBridge rule for `now + 4h` that calls a
   `terraform destroy` Lambda. Triple-belt-and-braces:
   wrapper-cleanup → idle-Lambda → hard-timeout-Lambda.
7. **AWS Budget alarm** at $20/month soft and $50/month hard. Email
   alert + slack webhook + auto-terminate-everything-tagged at the
   hard limit.
8. **Stop, don't terminate, for paused work.** A stopped EC2 is $0
   compute (just EBS, ~$0.10/GB-month). Lets us resume mid-experiment
   without re-bootstrapping.

These cost ~half a day to set up once and then run themselves. The
ROI is enormous.

---

## Sessions

### C0 — Local docker-compose cluster

**Goal.** Validate ~80% of replication correctness on a developer
laptop. Zero cloud spend. Should run in < 5 minutes in CI on the
GitHub Actions free tier.

**Topology:**
```
            ┌────────────┐
            │  leader    │  cqserver:leader
            │  :9007/:9008│ tcp + ws
            │  :8085     │ admin + /ui
            │  :9010     │ shipper -> followers
            └─────┬──────┘
       ┌──────────┴──────────┐
       ▼                     ▼
  ┌─────────┐           ┌─────────┐
  │follower1│           │follower2│
  │:9017/...│           │:9027/...│
  └─────────┘           └─────────┘
       ▲                     ▲
       └──────── loadgen ────┘
       (cq-loadgen container, multi-URI client_any)
```

**Deliverables:**
- `tests/cloud/docker-compose.local.yml` — 4 services (leader, two
  followers, loadgen), single bridge network, all on loopback.
- Per-service `cqserver.toml` rendered from a small Jinja-ish
  template so leader vs. follower differ only in `[replication]`.
- `tests/cloud/Makefile` with `make local-up`, `make local-test`,
  `make local-down`, `make local-logs`.
- A small assertion script `tests/cloud/scripts/assert-converged.sh`
  that publishes 1000 rows to the leader, waits for both followers'
  `cq_repl_applied_max_sequence` to catch up, then SOWs both
  followers and asserts byte-identical results.
- GitHub Actions workflow `.github/workflows/cloud-c0.yml` that
  builds the release binary in a builder stage, copies it into a
  thin runner image, spins up the compose stack, runs the assertion,
  tears down. Pass / fail is the CI gate.

**Test plan:**
- Local: `make local-up && make local-test` exits 0.
- Failure injection: `docker compose stop follower1` mid-test,
  publish more rows, restart follower1, assert it catches up to the
  leader's current sequence within a deadline.
- `cq-loadgen --scenario=stress2k-real` against the follower-fronting
  multi-URI list, 100 subs (scaled down for laptop CPU budget).
  Confirms `Client::connect_any` correctly spreads load across
  followers.

**Definition of done:**
- `make local-test` is green on macOS + Linux.
- CI workflow runs on every PR; failure blocks merge.
- A README in `tests/cloud/` explains how to reproduce locally and
  what each assertion proves.

**Estimated effort:** ~2 days.

---

### C1 — AWS Spot 4-node bench (Terraform-managed)

**Goal.** Single command spins up a 4-node spot cluster in one VPC /
one AZ, deploys the release binary, runs a measurement, captures
results to S3, tears the cluster down. Tear-down is the safety net,
not a "nice to have."

**Topology (AWS, us-east-1, single AZ):**
```
VPC (10.42.0.0/16) — single AZ
├── leader      c6i.2xlarge spot  10.42.1.10
├── follower1   c6i.2xlarge spot  10.42.1.11
├── follower2   c6i.2xlarge spot  10.42.1.12
└── loadgen     t3.large    spot  10.42.1.20
SG allows :22 from $RUNNER_IP, intra-VPC any-any.
```

**Deliverables:**
- `tests/cloud/terraform/aws/` Terraform module with variables for
  region / AZ / instance types / cluster size. Outputs the four
  instance public IPs.
- `tests/cloud/cloud-init/cqserver-leader.tftpl` and matching
  follower / loadgen user-data templates. User-data:
  1. Pulls a pre-built release binary from a tagged S3 object
     (`s3://cqserver-builds/v0.x.y/cqserver-linux-x86_64`).
  2. Renders `cqserver.toml` from the template with the right role.
  3. Installs a systemd unit `cqserver.service` and starts it.
- `tests/cloud/scripts/run-cloud-stress.sh`:
  ```
  ./run-cloud-stress.sh \
      --duration=2h \
      --scenario=stress2k-real \
      --subs=2000 \
      --tag-expires=$(date -d "+4 hours" -Iseconds)
  ```
  Does: `terraform apply` → wait for `/healthz` on all nodes →
  `cargo run -p cq-loadgen` from the loadgen box → collect
  `/metrics` + `/stats` + `/admin/replication` snapshots every
  10 s → push results to `s3://cqserver-results/<timestamp>/` →
  `terraform destroy`.
- A **hard timeout** belt-and-braces. The wrapper schedules an
  EventBridge rule for `now + duration_capped_at_4h` that
  invokes `cqserver-test-emergency-teardown` Lambda. Even if the
  wrapper crashes, the cluster dies on the timer.
- Idle-watchdog Lambda: scans for `auto-shutdown=true` tagged
  instances every 30 minutes; terminates any with > 6h uptime and
  < 5% CPU for the last hour.
- Cost dashboard: a one-page `cqserver-cloud-test` Cost Explorer
  saved view filtered by the `project` tag.

**Test plan:**
- First production run answers a concrete question: **"What is the
  per-follower NIC utilization at 2K total subs spread across two
  followers via `connect_any`?"** Result published as a stress run
  artifact with peak RSS, replication lag percentile distribution,
  NIC bytes-out histogram from CloudWatch.
- Failure-injection variant: send SIGTERM to one follower mid-run
  via `aws ssm send-command`. Confirms (a) `connect_any` clients
  fail over within a deadline, (b) post-restart the follower catches
  up cleanly, (c) loadgen's per-class delivery counts stay within
  ±5% of the no-failure baseline.

**Definition of done:**
- `./run-cloud-stress.sh --duration=2h` completes end-to-end
  including teardown without operator intervention.
- A `cargo test` smoke test runs the whole loop with `--duration=10m`
  on a release-branch CI job (skipped on regular PRs).
- Three independent runs are within ±10% on the headline numbers
  (RSS, subscribe p50, per-follower NIC bytes/sec) — i.e. the
  measurement is repeatable.
- Cost Explorer shows the project tag's spend; runaway-cost
  guardrails verified manually (delete a `terraform destroy`
  invocation; idle Lambda terminates the cluster within an hour).

**Estimated effort:** ~3 days.

---

### C2 — Pre-release soak workflow

**Goal.** Every release-tag push runs a 24-hour soak on the same
bench. Slow leaks (RSS, FD, segment file growth) get caught before
shipping. Cost capped per release at ~$10.

**Deliverables:**
- `.github/workflows/cloud-soak.yml`:
  - Triggered on release-tag push (`v[0-9]+.[0-9]+.[0-9]+`).
  - **Manual approval gate** before launching the cluster — avoids
    accidental "merge → $$$" surprises.
  - Uploads the release binary to S3, runs the wrapper with
    `--duration=24h`, polls every 15 min for liveness, captures
    daily summary metrics.
  - On completion, publishes the result tarball as a release-asset
    artifact attached to the tag.
- `tests/cloud/scripts/soak-summarize.py`: post-run report. Reads
  the metric snapshots, computes:
  - Peak / steady-state / final RSS per follower
  - Replication lag p50 / p95 / p99 per topic
  - Per-sub delivery rate over the run
  - Open file count vs. baseline
  - Anything that drifted monotonically over 24h (leak indicator)
- A simple regression gate: compare the new run's metrics to the
  previous tagged release's run; fail the gate if peak RSS climbed
  > 25% or subscribe p99 > 50%.

**Test plan:**
- Dry-run on a release-candidate tag with `--duration=2h` (saves $);
  verify the workflow gates + manual approval + result publishing
  end-to-end.
- One real 24-hour soak on the next actual release tag, results
  archived. Regression gate baseline established.

**Definition of done:**
- Workflow lives on `main`, gated by manual approval.
- A real release run has executed end-to-end and published its
  artifact.
- Regression gate has both PASS (no drift) and FAIL (planted
  regression) test runs to prove it isn't always green.

**Estimated effort:** ~1 day.

---

## Order of execution

C0 → C1 → C2, strict serial:

- C0 alone covers ~80% of correctness at $0 — most operator
  questions are answered before any cloud spend.
- C1 needs C0's docker-compose YAML as the bootstrap reference;
  same `cqserver.toml` templates are reused.
- C2 needs C1's wrapper script + Terraform module; just adds the
  GitHub Actions glue.

C0 is also independently valuable as a **CI gate** — even if C1
never lands, every PR gets multi-instance replication validated for
free.

---

## Initial questions C1 will answer (write these down now)

So the first cloud-spend isn't aimless:

1. **NIC math:** at 2K subs split across 2 followers via
   `connect_any`, what's each follower's steady-state outbound
   bandwidth (bytes/sec)? Does it match our earlier extrapolation
   from loopback testing?
2. **Failover latency:** when one follower is SIGKILL'd, how long
   before `connect_any` clients are back on the surviving follower
   delivering live deltas?
3. **Replication lag distribution:** under steady 2K-sub realistic
   load + a publisher running ~10K msg/sec, what does
   `shipped_seq - applied_seq` look like (p50, p95, p99) per topic?
4. **TLS overhead:** repeat (1) with TLS enabled on TCP. What's the
   CPU + bandwidth cost as a percentage?
5. **Follower restart catch-up:** if a follower is offline for 5
   minutes during heavy publish, how long does it take to catch up
   after reconnect?

Each question becomes a one-line wrapper invocation + a result
artifact + a paragraph in a follow-up document.

---

## Status

| # | Session | Status |
|---|---|---|
| C0 | Local docker-compose cluster + CI gate | ✅ done — enabler `[replication].peers: Vec<String>` shipped (one shipper task per peer); `tests/cloud/` harness with `Dockerfile.runtime`, `docker-compose.local.yml`, per-service TOML configs, `assert-converged.sh`, `Makefile` targets, GitHub Actions workflow at `.github/workflows/cloud-c0.yml`. End-to-end verified on host (no Docker required): leader fans out to 2 followers, both apply every entry (rows=502 seq=502 across all three), follower2 caught up via Hello-with-highwater within ~1.4s after kill+restart. |
| C0.5 | Two-Mac lab (real LAN network, $0) | ✅ done — `tests/cloud/lab/` with `lab-up-leader.sh` / `lab-up-follower.sh` / `lab-stress.sh` / `lab-down.sh` + templated `leader.toml.template` / `follower.toml.template` using cqserver's existing `${VAR}` substitution. Script renders `PEERS_TOML_LIST` from `FOLLOWER1_IP` + optional `FOLLOWER2_IP`. Added `cqserver --config <path>` CLI so the binary runs from any CWD. Smoke-verified: rendered config exposes `peers = ["192.0.2.99:9010"]` via `/admin/replication`; shipper retries to unreachable peer as expected. Covers real-LAN-network testing (the cloud-specific gap C1 was meant for) at $0 incremental cost when a second Mac is available. |
| C1 | AWS Spot 4-node bench (Terraform + wrapper + cost guardrails) | ⏳ pending |
| C2 | Pre-release 24-hour soak workflow | ⏳ pending |

## Related worklogs + documents

- [`REPLICA_READS_WORKLOG.md`](REPLICA_READS_WORKLOG.md) — the
  S1 / S2a / S3a work this validates. S3b multi-instance
  state-convergence is exactly what C0 unblocks.
- [`HIGH_SCALE_WORKLOG.md`](HIGH_SCALE_WORKLOG.md) — single-host
  scale targets; C1 measures whether multi-host pushes past them.
- [`PRODUCTION_READINESS.md`](PRODUCTION_READINESS.md) — P1.8 (soak
  in CI) and P1.10 (`/readyz` distinct from `/healthz`) are closed
  by this worklog.
- [`docs/deploy/replica-reads.md`](docs/deploy/replica-reads.md) —
  the operator guide; C1's results become the "expected numbers"
  table in this guide.

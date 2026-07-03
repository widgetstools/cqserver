# Cloud Soak (Bucket B) + HA/Failover (Bucket C) Implementation Plan

> **For agentic workers:** execute task-by-task via superpowers:subagent-driven-development.
> Companion to `2026-07-02-production-readiness-remaining.md` (Buckets B + C).

**Goal:** Build a credential-agnostic AWS (EC2 + Terraform) harness that lets the operator run
the 24h→7-day soak and the multi-node HA chaos validation with one command, and build the HA
feature (promote / mid-stream failover / multi-peer shipper) that Bucket C validates.

**Target infra:** AWS EC2 + Terraform (matches `tests/cloud/` + `CLOUD_REPLICATION_TEST_WORKLOG.md`;
plain EC2, NOT k8s). Cost per that worklog: ~$0.33/hr spot → 24h ≈ $8, 7-day ≈ $55.

**Execution model:** I build + LOCALLY validate (Terraform validate/plan, docker-compose cluster
runs locally, analyzer unit-tested, HA feature has local multi-node integration tests). The
operator supplies AWS credentials and triggers the actual cloud run + teardown. No task's "done"
claims a cloud run passed — only that the harness is proven runnable and the feature is
locally-verified.

## Global Constraints
- Reuse `tests/cloud/` (Dockerfile.runtime, docker-compose.local.yml, configs, lab/, scripts) —
  extend, don't rebuild.
- All Terraform credential-agnostic: no hardcoded account/keys; use standard AWS provider env/profile.
- Every server config used is a real committed `config/*.toml`; secrets via `env://` (P0.7).
- Local validation gates each task: `terraform validate`, `docker compose config`, analyzer unit
  tests, HA integration tests must pass before a task is "done."
- Commits: conventional, `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

## Phase B — Cloud Soak Harness (do first; the production gate)

### B1. Soak/scale cluster topology (docker-compose + Dockerfile)
Extend `tests/cloud/docker-compose.local.yml` into a soak topology: one `cqserver` node (persistent
Atlas topics, `checkpoint_interval_secs` set so txlog stays bounded), one load-driver container,
one Prometheus scraping `:8085/metrics`. Deliver `tests/cloud/docker-compose.soak.yml`. Gate:
`docker compose -f … config` valid + cluster comes up locally + `/healthz` green + Prometheus
scrapes cqserver metrics.

### B2. `cq-loadgen` soak driver
A long-running driver (new `cq-loadgen` subcommand or scenario) that runs the Atlas-shaped
workload: wide rows, delta ticks, materialized views, 3 subscriber classes (fast / conflated /
deliberately-slow), configurable duration + rate. Emits its own progress + records acked
sequences. Gate: runs a 60s local soak against the B1 cluster with no panics; publishes + all 3
subscriber classes active.

### B3. Soak pass/fail analyzer
A tool (Rust bin or script) that reads Prometheus over the run window and emits a verdict:
RSS slope ≈ 0 after warmup (no leak), zero unexplained delta drops beyond the slow-consumer
policy, txlog disk sawtooths (bounded by checkpoint, not monotonic growth), p99 delivery under
target. Unit-test the verdict logic against synthetic metric series (leak → FAIL, flat → PASS).
Gate: analyzer unit tests pass; runs against the B2 local 60s soak and prints a verdict.

### B4. AWS Terraform (credential-agnostic)
`tests/cloud/terraform/` — EC2 spot instances (server + driver), security groups, the runtime
Docker image, user-data that pulls the image + config and starts the soak, Prometheus, and an
S3 (or artifact) drop for results. Variables for instance type / duration / region; no baked
credentials. Gate: `terraform init && terraform validate` clean; `terraform plan` renders with a
dummy tfvars (no apply — apply is the operator's).

### B5. Launch runbook + teardown + scheduled-workflow stub
`docs/RUNBOOK-cloud-soak.md`: exact operator commands (creds → `terraform apply` → watch →
collect results → `terraform destroy`), the ~$8/$55 cost note, and how to read the analyzer
verdict. Plus a `workflow_dispatch` GitHub Actions stub (guarded on a secret being present) that
wires the same flow for a future scheduled run. Gate: runbook commands dry-run-checked; workflow
YAML lints.

---

## Phase C — HA / Failover (feature build + cloud chaos validation)

### C1. Multi-peer shipper (leader → N followers)
Today one shipper ships to one peer. Generalize to fan-out to N configured followers, each with
independent ack tracking. Local integration test: 1 leader + 2 followers in-process, publish,
assert both followers converge. Gate: test green.

### C2. `cqserver-promote` + follower→leader promotion
A promotion mechanism (admin endpoint or CLI) that turns a follower into a leader: stop applying
inbound replication, open for writes, (optionally) start shipping to remaining peers. Local test:
leader + follower, promote the follower, assert it accepts writes and preserves all replicated
state. Gate: test green.

### C3. Mid-stream client failover (SDK)
Extend the SDK reconnect path: on connection loss, reconnect (existing `connect_any` covers
initial connect) AND resubscribe from the last bookmark, dedup by sequence, so a subscriber
survives a leader→follower cutover with no gap/dup. Local test: subscribe, kill the server,
promote a follower, assert the client resumes with no lost/duplicated rows.

### C4. `/livez` + `/readyz` split
`/livez` = process alive; `/readyz` = txlog replay done + replication caught up (not ready during
catch-up). Local test asserts readyz flips only after replay/catch-up. Gate: test green.

### C5. Chaos harness + failover runbook
A chaos script (extends the B cluster to leader+follower+client) that kills -9 the leader under
active load, runs `cqserver-promote`, and asserts every acked-write is present post-failover
(publisher ledger vs promoted-node SOW) with MTTR measured. `docs/RUNBOOK-ha-failover.md`
documents the manual + scripted path. Gate: the chaos test passes on the LOCAL compose cluster
(the cloud multi-AZ run is the operator's).

---

## Sequencing
Phase B first (harness is the production gate and mostly extends existing scaffolding). Phase C
after — it's new feature code; per the parent roadmap, the operator should run B's soak to prove
single-node stability before depending on C's failover. Each phase ends with a gate; the final
whole-branch review precedes merge.

## What is NOT in scope (explicit)
- The actual cloud RUN (soak execution, multi-AZ chaos) — operator-triggered with their creds.
- Auto-failover consensus, active-active, cross-region (deferred per parent roadmap).
- Kubernetes/Helm (this harness is plain EC2 + Terraform by design).

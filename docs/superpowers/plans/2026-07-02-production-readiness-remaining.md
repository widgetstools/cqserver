# CQServer Production-Readiness — Remaining Roadmap

**Status as of 2026-07-02:** The [production-readiness plan](2026-07-02-production-readiness.md)
Phases 1–2 and Task 3.1 are **DONE and merged to `main`** (32 commits, 942 tests green).
That closed every known correctness bug and the entire P0 security/ops register
(`PRODUCTION_READINESS.md` P0.1–P0.7). The subagent review loop additionally caught and
fixed **two silent data-loss bugs** (view-schema post-agg column drop; reclaim-below-live-segment)
and **one hollow durability claim** (force-rotate that didn't fsync).

This doc tracks what remains to move cqserver from **"hardened & deployable"** to
**"proven for production."** It is organized by *kind of work*, because the remaining items
are not all the same: some are pure code/CI, some need infrastructure + wall-clock time, and
one is a multi-week feature build.

---

## Bucket A — Code / CI (no new infrastructure; do next)

The cheapest way to turn "hardened" into "measured," plus one honesty gap in the durability story.

### A1. Wire stress scenarios A–G as runnable benchmarks + baselines (plan Phase 3.2 / 3.5)
- Source: `cqserver-stress-test-plan.md` scenarios A–G. Scaffolding exists in `cq-loadgen`.
- Deliver: each scenario a runnable `cargo run -p cq-loadgen` command; results JSON committed
  under `bench/results/`; a CI perf-regression gate (fail PR on p50/p99 or RSS regression >10%
  vs committed `stress2k` baseline).
- Targets (from the plan): A 10k idle conns; B 10k subs; C 500k pub/s; D 10k×10k fan-out p99≤50ms;
  E reconnect storm; F wide-row 100k upd/s × 1000 subs p99≤20ms; G slow-consumer isolation.
- Note: F and G are the ones most likely to expose the wide-row egress wall → feed Phase 4.

### A2. Fix the dead `Persisted` ack tier (honesty completion of the crash-durability work)
- Today `AckType::{Received,Processed,Persisted}` are declared but **only `None`/`Replicated`
  are wired**; the SDK exposes no way to request an fsync-before-ack. So an "ack" only means
  "appended to txlog (OS page cache)," not durable on disk (default `FsyncPolicy::None`).
- Deliver: honor `AckType::Persisted` on the publish path (fsync the txlog entry before acking),
  and expose a persist-ack option on the client SDK `publish`. Add an e2e test: a `Persisted`-acked
  row survives a SIGKILL (power-loss sim) with `fsync=none` globally.
- Why it matters: it makes "acked = durable" a guarantee a caller can actually request — the
  natural completion of Task 2.6's finding.

### A3. Run the already-written 1h soak + differential correctness tier (plan Phase 3.3 partial / 3.4)
- `crates/cq-core/tests/active_set_bounds.rs::soak_active_set_stays_bounded_under_1h_churn`
  is written and gated in the nightly lane — just needs a green nightly run recorded.
- `CQSERVER-TEST-PLAN-001-view-server.md` correctness + fan-out differential tiers: execute at
  least those two; resolve its §10 open items (independent SOW-recompute oracle).

---

## Bucket B — Needs a machine + wall-clock time (a resourcing decision)

These cannot run meaningfully on a macOS dev box — OS `ulimit`s cap connection counts and a
multi-day run needs a dedicated host. **This is the single most important gate before real
production use**: the soak is what converts "deployable" into "battle-tested."

### B1. Soak ladder 24h → 7-day (plan Phase 3.3)
- Workload: Atlas-shaped (wide rows, deltas, views, 3 subscriber classes incl. one deliberately slow).
- Watch: RSS slope (~0 after warmup), `cq_topic_view_tap_drops_total`, dropped-delta counters,
  txlog disk (must sawtooth with the Task-3.1 periodic checkpoint, not grow), p99 delivery.
- Prereq already done: Task 3.1 periodic checkpoint+reclaim bounds the txlog without a restart.
- Infra options: the repo's `CLOUD_REPLICATION_TEST_WORKLOG.md` C1/C2 stub (AWS spot + Terraform),
  a CI runner, or a spare Linux box. **Decision needed: where it runs + who/what watches it.**

### B2. High-connection scenarios (A/E from A1, at true scale)
- 10k concurrent connections + reconnect storm need a Linux host with tuned `ulimit -n` (65536+).
  Run these on the same infra as B1.

---

## Bucket C — HA / Failover (a multi-week feature build; sequence after B)

The biggest remaining *feature* gap, but only required to survive a node dying — a single-node
deployment can go live without it. Sequence it **after** the soak proves single-node stability.

- `cqserver-promote` script + failover runbook (<5-min MTTR by hand, <30s scripted).
- Chaos test: kill -9 the leader under load, promote a follower, assert zero acked-write loss
  (publisher ledger vs recovered SOW).
- Client mid-stream failover: SDK resubscribe-with-bookmark on reconnect (initial-connect
  `connect_any` exists; extend to the reconnect path), dedup via sequence.
- Multi-peer shipper (one leader → N followers in-process).
- `/livez` vs `/readyz` split (readyz = replay done + replication caught up); k8s/systemd units.
- Explicitly deferred beyond "production-worthy v1": auto-failover consensus, active-active,
  cross-region.

---

## Long-tail parity backlog (post-production; tracked, not scheduled)

From the AMPS-parity audit + review follow-ups — none block production, address by demand:
`replace` filter option, regex topic subscriptions, select lists, `rate` replay pacing,
pause/resume e2e, STRING_AGG, correlated subqueries, Array/Object column types, continuous
window functions, wire formats (FIX/protobuf), Java/Go SDKs, cold-tier archive, row checksums.

Minor review follow-ups also captured (non-blocking): `MAX_WIRE_JSON_DEPTH` hardcoded (128);
`__postagg_*` hidden-alias silent collision; empty-vs-missing env-var share one error variant;
fixed-window rate limiter ~2x boundary burst; `PRODUCTION_READINESS.md` P0.x status sweep
(mark resolved). A pre-existing broken bench (`crates/cq-core/benches/fanout_predicate_index.rs`,
`ParsedQuery` literal missing fields) predates this work and should be fixed in a cleanup pass.

---

## Recommended order

1. **Bucket A now** — measurement + the ack-durability honesty fix (pure code/CI).
2. **Bucket B** — make the resourcing call and run the soak; this is the real production gate.
3. **Bucket C** — HA, as a deliberate project once single-node stability is proven.

## Definition of "production-worthy" (the final gate)
All of Bucket A green + a signed-off **7-day soak** (B1) with flat RSS and zero unexplained drops.
HA (Bucket C) is required only for deployments that cannot tolerate a single-node outage.

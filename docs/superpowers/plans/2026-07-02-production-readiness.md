# CQServer Production Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Take cqserver from "stable for demos" to "deployable for production trading workloads" — every known bug fixed, every P0 hardening item closed, endurance proven by soak evidence, and a definition of done that is measurable.

**Architecture:** Six gated phases. Each phase has hard exit criteria; do not start phase N+1 work before phase N's gate passes (exception: independent workstreams marked ∥ may run in parallel). Phases 1–2 are fully task-detailed here; phases 3–6 are specified to the acceptance-criteria level and each gets its own detailed sub-plan when its phase opens (they depend on results from earlier phases).

**Tech Stack:** Rust workspace (tokio, parking_lot, rayon on ssrm), cq-loadgen for stress, Prometheus metrics, existing e2e test harness.

## Global Constraints

- **Branch:** all fixes land on `main` first when they apply there; ssrm-specific fixes (mmap, running aggregates) land on `ssrm`. Phase 3 gates the ssrm→main convergence.
- **TDD:** every bug fix starts with a failing test reproducing it. No fix without a regression test.
- **No regressions:** full workspace suite green after every task (`cargo test --workspace`).
- **Commits:** one commit per task, conventional-commit style, `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Sources of record:** `PRODUCTION_READINESS.md` (P0/P1/P2 register), `docs/AMPS_PARITY.md` + `AMPS_PARITY_WORKLOG.md` (known bugs), `cqserver-stress-test-plan.md` (scenarios A–G), `CQSERVER-TEST-PLAN-001-view-server.md` (differential tiers), `docs/superpowers/plans/2026-05-31-cqserver-egress-and-view-perf.md` (egress wall).
- **Definition of production-worthy (the final gate):** all Phase 1–5 exit criteria met + a signed-off 7-day soak.

---

## Phase 1 — Correctness & Safety Bugs (fix everything that can hang, wedge, or corrupt)

**Exit criteria:** zero known ways for a client request to hang a server thread; zero known silent-data-corruption paths; all fixes carry regression tests; full suite green on both branches.

### Task 1.1: Diagnose + fix the ORDER BY alias hang (A1)

The single worst open bug: `SELECT col, SUM(x) AS y FROM t GROUP BY col ORDER BY y` hangs the SOW encoder when `y` doesn't match a base column (`docs/AMPS_PARITY.md:75`).

**Files:**
- Test: `crates/cq-e2e-tests/tests/order_by_alias_hang.rs` (create)
- Likely fix: `crates/cq-core/src/query.rs` (ORDER BY column resolution) — confirm via diagnosis
- Modify after diagnosis: wherever the encoder loops/waits on a missing sort column

**Interfaces:**
- Produces: ORDER BY resolution that treats SELECT-list aliases as first-class sort keys, and returns a clean `QueryError::UnknownOrderByColumn` (new variant) for genuinely unknown names — never a hang.

- [ ] **Step 1: Write the failing test (with a timeout so the hang becomes a failure, not a stuck CI)**

```rust
//! Regression: AMPS_PARITY.md §4 bug 2 — ORDER BY <select alias> must not hang.
use std::time::Duration;

#[tokio::test]
async fn order_by_select_alias_returns_within_deadline() {
    let (server, client) = common::start_server_and_client().await; // existing e2e helper
    for i in 0..100 {
        client.publish("/t", serde_json::json!({"k": format!("k{i}"), "grp": i % 5, "x": i})).await.unwrap();
    }
    let fut = client.sow_sql("/t", "SELECT grp, SUM(x) AS y FROM t GROUP BY grp ORDER BY y");
    let res = tokio::time::timeout(Duration::from_secs(10), fut).await;
    let rows = res.expect("query hung >10s — the alias-hang bug").expect("query errored");
    assert_eq!(rows.len(), 5);
    // and truly-unknown columns error cleanly instead of hanging:
    let fut = client.sow_sql("/t", "SELECT grp, SUM(x) AS y FROM t GROUP BY grp ORDER BY nosuchcol");
    let res = tokio::time::timeout(Duration::from_secs(10), fut).await;
    assert!(res.expect("unknown-column query hung").is_err(), "must error, not hang");
}
```

- [ ] **Step 2: Run to verify it fails the way the bug report says**

Run: `cargo test -p cq-e2e-tests --test order_by_alias_hang -- --nocapture`
Expected: FAIL via the 10s timeout (`query hung`), NOT via assertion. If it passes, the bug was fixed incidentally — bisect worklog P/Q/R commits to confirm, convert the test to a keeper regression test, mark A1 closed, and skip Steps 3–4.

- [ ] **Step 3: Diagnose.** Instrument the encoder path: run the failing query with `RUST_LOG=cq_core=trace`, capture where it loops. Check `query.rs`'s ORDER BY resolution: the sort key is looked up against base schema columns; when absent it likely falls into a retry/wait loop in the SOW encode task. The fix shape: resolve ORDER BY names against (1) SELECT aliases, (2) aggregate output columns, (3) base columns — in that order; unknown → `Err(QueryError::UnknownOrderByColumn(name))` propagated to the client as a clean error frame.

- [ ] **Step 4: Implement, run test (PASS), run `cargo test -p cq-core -p cq-e2e-tests`, commit** (`fix(query): resolve ORDER BY against select aliases; error, never hang, on unknown columns`).

### Task 1.2: Reject delta publishes missing key fields (the phantom-row footgun)

`delta_upsert_map` falls back to key `""` when key fields are absent (`compute_key_from_map` → `None` → `unwrap_or_default()`), silently merging all keyless deltas into one phantom row. AMPS rejects these.

**Files:**
- Modify: `crates/cq-core/src/topic.rs` — `delta_upsert_map` (both branches; on ssrm it's ~line 1864, on main ~line 1397)
- Modify: `crates/cq-core/src/topic.rs` — add `TopicError::MissingKeyFields` variant to the error enum (~line 67)
- Test: add to the existing topic test module (next to the sparse-delta tests added 2026-07-02)

**Interfaces:**
- Produces: `TopicError::MissingKeyFields { topic: String }`; router surfaces it as an error ack to the publisher (the `?` path already converts TopicError → error frame).

- [ ] **Step 1: Failing test**

```rust
#[test]
fn delta_upsert_without_key_fields_is_rejected() {
    let topic = make_topic(); // keyed on "symbol"
    let mut delta = serde_json::Map::new();
    delta.insert("price".into(), 42.0.into()); // no "symbol"
    let err = topic.delta_upsert_map(&delta).expect_err("keyless delta must be rejected");
    assert!(matches!(err, TopicError::MissingKeyFields { .. }));
    assert_eq!(topic.row_count(), 0, "no phantom row created");
}
```

- [ ] **Step 2: Run → FAIL** (currently returns Ok and creates the `""`-keyed row).
- [ ] **Step 3: Implement:**

```rust
let key = match self.compute_key_from_map(flat) {
    Some(k) => k,
    None => {
        return Err(TopicError::MissingKeyFields {
            topic: self.config.name.clone(),
        })
    }
};
```
(Keep keyless *topics* working: `compute_key_from_map` already returns `None` only when key fields are configured-but-absent vs. topic-has-no-keys — verify that distinction; if it conflates them, gate the rejection on `!self.config.key_fields.is_empty()`.)
- [ ] **Step 4: PASS + full suite + commit** (`fix(topic): reject delta publish missing key fields instead of merging into phantom "" row`). Apply on **both** main and ssrm (implementations diverged 2026-07-02 — see memory `txlog-fix-dual-branches`).

### Task 1.3: Bound JSON nesting at the wire codec (A8/E12 — DoS vector)

Flattener is capped at 100 levels (TH10) but a 500-level publish still stalls in `serde_json` recursion at decode (`AMPS_PARITY_WORKLOG.md:765`).

**Files:**
- Modify: `crates/cq-protocol/src/serialization.rs` (binary codec decode) and `crates/cq-transport/src/websocket.rs` (JSON text frames)
- Test: `crates/cq-e2e-tests/tests/deep_nesting_bounded.rs` (create)

- [ ] **Step 1: Failing test** — build a 500-level-nested JSON publish programmatically (`(0..500).fold(json!(1), |acc,_| json!({"n": acc}))`), send it, assert the server responds with a clean error frame within 2s and stays healthy (`/healthz` still ok, a normal publish still works).
- [ ] **Step 2: Run → FAIL** (stall/timeout).
- [ ] **Step 3: Implement:** use `serde_json::Deserializer::from_slice(..)` wrapped with `serde_stacker` OR pre-scan bytes for `{`/`[` depth > 128 before parsing (cheap linear scan, no dependency) → reject with `"nesting depth exceeds 128"` error frame.
- [ ] **Step 4: PASS + commit.**

### Task 1.4: Fix `NULLIF`/scalar-over-aggregate projection (C7/E14) — or reject cleanly

`NULLIF(SUM(qty),0)` needs computed-over-aggregate projection; today it's an `#[ignore]`d test. Decision gate: implement the projection layer (preferred, ~2 days: evaluate scalar exprs over the aggregate output row before emit) **or** make the parser reject it with a clear error naming the workaround. Either outcome un-ignores `r4_coalesce_nullif.rs::nullif_after_aggregate` (adjusted to the chosen behavior). Same treatment for the three remaining R-series gaps (`HAVING` on non-SELECT agg, scalar fns in ORDER BY, scalar fns in SELECT — `AMPS_PARITY_WORKLOG.md:1037–1050`): each either works or errors cleanly; none may hang or silently mis-answer.

### Task 1.5: Un-ignore and gate the recovery/robustness tests

The five `#[ignore]`d tests exist because they're slow/environment-dependent — not because they fail. Wire them into a **nightly** CI lane (not per-PR): `cargo test --release -p cq-e2e-tests -- --ignored` for `trader_view_pivot_e2e`, `mem_per_row_e2e`, `stress_10k_connections` (with `ulimit -n 65536` in the runner), `stress_10k_max_rows`, `bench_wide_ingest`, plus `active_set_bounds::bounds_active_set_over_one_hour`. Each gets a pass/fail assertion (e.g. mem_per_row: RSS/row below a committed baseline +10%) so "nightly" means *gating*, not just logging.

**Phase 1 gate check:** run the new tests + full suite on both branches; update `docs/AMPS_PARITY.md` §4 to mark A1 fixed; commit.

---

## Phase 2 — Security & Operational Hardening (the P0 register)

These are `PRODUCTION_READINESS.md` P0.1–P0.7, verbatim scope. **Exit criteria:** a hostile network position gains nothing from the admin port; a rogue client cannot exhaust the server; an operator can restore from backup following a tested runbook.

### Task 2.1: Admin API authentication (D1 / P0.1)
- Config: `admin_token` exists (`config.rs:26`) — verify it actually gates every route except `GET /healthz` (audit `crates/cq-server/src/admin.rs` route registration; the doc says it does, the PRODUCTION_READINESS doc says there's no auth — **resolve the contradiction first**: write an e2e test hitting each admin route with/without the bearer token and assert 401/200).
- Change the default `admin_addr` to `127.0.0.1:8085` in `config.rs` default + shipped `cqserver.toml` (currently `0.0.0.0:8085` — network-exposed by default).
- Test: `admin_auth_e2e.rs` — every mutating route (`DELETE /subscriptions/:id`, `POST /admin/rotate-journal/:topic`, `POST /admin/shrink-store-all`, view create/delete, `add-column`) returns 401 without token when `admin_token` set; `/healthz` stays open.

### Task 2.2: Admin TLS (D2 / P0.2)
- Add `[admin.tls] cert_file/key_file` mirroring `[transport.tls]`; reuse `crates/cq-transport/src/tls.rs` acceptor. Test: e2e starts admin with TLS, plain HTTP fails, HTTPS + token succeeds.

### Task 2.3: Connection & rate limits (D3/B3 / P0.3)
- New `[transport.limits]`: `max_connections` (default 10000), `max_connections_per_ip` (default 0=off), `accept_rate_per_sec` (default 0=off), `max_sessions_per_user` (default 0=off). Enforce in the accept loops (`tcp.rs`, `websocket.rs`) before session allocation; over-limit → close with a logged reason + `cq_connections_rejected_total{reason}` counter.
- Tests: e2e with `max_connections = 4` — 5th connect refused, one closes, 5th retry succeeds; per-IP variant.

### Task 2.4: Audit log (D5 / P0.5)
- `[audit] sink = "file"|"syslog", path = ...`. One structured line per: logon success/fail (user, IP), admin mutation (route, actor, args), subscription drop, entitlement denial. Implement as a dedicated `tracing` target (`target: "audit"`) routed by the existing S25 logging-sink machinery — no new plumbing.
- Test: e2e performs logon-fail + admin rotate; asserts both lines land in the audit file with actor + IP.

### Task 2.5: Secrets out of TOML (D7 / P0.7)
- Minimal viable: `env://VAR` and `file:///path` indirection for `auth.jwt.secret`, `users[].password_hash`, TLS key paths. One resolver function in `config.rs` applied at load; unit tests for all three forms + missing-var error.

### Task 2.6: Graceful shutdown under load — verified (D6 / P0.6)
- Test: `graceful_shutdown_under_load.rs` — 2k subscriptions + publisher at full tilt, send SIGTERM, wait for exit, restart, assert: (a) every acked publish present in post-restart SOW, (b) shutdown completed within the 120s budget, (c) zero corrupt txlog entries (reader replays clean).
- Fix whatever it finds. (This is a test-first discovery task by design.)

### Task 2.7: Backup/restore runbook + script (D4 / P0.4)
- `scripts/backup-cqserver.sh`: quiesce-free snapshot = force-rotate all journals (admin endpoint exists) → copy sealed segments + `snapshot.bin` → verify with a `TxLogReader` scan. `scripts/restore-cqserver.sh`: place files, start, verify row counts vs manifest. `docs/RUNBOOK-backup-restore.md` documents both plus point-in-time (truncate to seq). CI smoke: run backup on a seeded server, restore into a fresh dir, diff SOW row counts.

**Phase 2 gate:** all P0.x items in `PRODUCTION_READINESS.md` flipped to done with linked tests; a port-scan of a default-config server exposes nothing mutable unauthenticated.

---

## Phase 3 — Endurance & Scale Proof ∥ (evidence, not vibes)

**Exit criteria:** the stress-test-plan scenarios pass at their stated targets; a 24-hour and then 7-day soak completes with flat RSS and zero unexplained drops; results are committed as baselines.

- **3.1 Runtime checkpoint + reclaim** (prereq for long soaks; also the "no daily restart" feature): periodic in-process `write_snapshot()` + `prune_segments_below(snapshot.segment_id)` on a `[txlog] checkpoint_interval_s` timer, per topic, off the hot path (spawn_blocking; topic read-lock held only during snapshot build — measure and document the pause). This is the AMPS `sow-compact`-action equivalent. Reuses the 2026-07-02 pruning primitive + its safety contract (prune only below crash-durable snapshot). Tests: interval fires → sealed segments pruned while server serves; crash mid-checkpoint recovers from previous snapshot.
- **3.2 Execute stress scenarios A–G** from `cqserver-stress-test-plan.md` at stated targets (A: 10k idle conns; B: 10k subs; C: 500k pub/s; D: 10k×10k fan-out p99≤50ms; E: reconnect storm; F: wide-row 100k upd/s × 1000 subs, 1h, p99≤20ms; G: slow-consumer isolation). Each scenario becomes a runnable `cq-loadgen` command with a results JSON committed under `bench/results/`. F and G are the ones most likely to fail today → feed Phase 4.
- **3.3 Soak ladder:** 1h (already scripted as `active_set_bounds`) → 24h → 7-day, with the Atlas-shaped workload (wide rows, deltas, views, 3 subscriber classes incl. one deliberately slow). Watch: RSS slope (must be ~0 after warmup), `cq_topic_view_tap_drops_total`, dropped-delta counters, txlog disk (must sawtooth with checkpoints, not grow), p99 delivery. Automate via the C2 cloud workflow stub in `CLOUD_REPLICATION_TEST_WORKLOG.md`.
- **3.4 Differential tiers** from `CQSERVER-TEST-PLAN-001` (correctness/window-shift/fan-out/persistence/scale) — run at least correctness + fan-out tiers; resolve its §10 open items (independent SOW oracle).
- **3.5 CI perf gates** (E20/P1.9): commit stress2k baselines; fail PRs regressing p50/p99 or RSS >10%.
- **3.6 Branch convergence:** with soak + scenarios green on ssrm, merge ssrm→main (per memory `txlog-fix-dual-branches`: prefer ssrm code in conflicts; `0x02` marker reserved for its binary format). Production runs one branch, not two.

---

## Phase 4 — Performance Walls ∥ (open only what Phase 3 measurements confirm)

**Exit criteria:** scenario F (wide-row high-rate) and G (slow-consumer isolation) pass at target; no per-delta O(row-width) work on the hot path.

- **4.1 mmap delta-apply memmove wall** (ssrm; `2026-05-31-cqserver-egress-and-view-perf.md:610` — per-delta O(SOW columns) memmove): implement the var-width arena redesign (Tier 1b follow-up) so a delta touches O(changed cells). This is its own sub-plan (write it when Phase 3 confirms it's still the top profile entry; the plan doc already names the call site).
- **4.2 Egress snapshot single-encode cost**: version-gated cache (Part 2) landed; measure cache hit rate under scenario D; if misses dominate, add incremental snapshot patching.
- **4.3 Snapshot-cache invalidation O(N) walk** (B1): only if profiles show it.
- **4.4 Per-user quotas** (B2/E17/P1.5): publish-rate cap + outbound-memory quota per user, enforced where `throttle_mutation_backlog` and the delivery queues already sit.

---

## Phase 5 — HA & Failover

**Exit criteria:** leader kill → follower promoted and serving in <5 min by runbook (scripted path <30s); no acked-write loss in the chaos test.

- **5.1 Failover runbook + `cqserver-promote` script** (D9): documented + chaos-tested (kill -9 the leader under load; promote; verify acked writes present via publisher ledger).
- **5.2 Client mid-stream failover**: SDKs resubscribe with bookmark on reconnect (`connect_any` exists for initial connect; extend to reconnect path), dedup via sequence — test with a bounce mid-stream, assert no gap/dup at the client.
- **5.3 Multi-peer shipper** (one leader → N followers in-process).
- **5.4 Health split** `/livez` vs `/readyz` (E21) — readyz = replay done + replication caught up; k8s/systemd units consume it (E22/E23 as stretch).
- Defer (explicitly out of scope for "production-worthy v1"): auto-failover consensus, active-active (E25), cross-region (E27).

---

## Phase 6 — Operability Polish (P1 register)

Hot-reload of users/entitlements/certs via SIGHUP (D10); OpenTelemetry export (E18); JSON log format (E19); config hot-reload test; schema-mismatch-on-restart guard (E28: refuse to start on key_fields change vs txlog header rather than silence); supply-chain lane (`cargo audit` + `cargo deny` in CI, E30).

**Long-tail parity backlog (post-production, tracked not planned):** `replace` filter option, regex topic subscriptions, select lists, `rate` replay pacing, pause/resume e2e, STRING_AGG, correlated subqueries, Array/Object columns, continuous window functions, wire formats (FIX/protobuf), Java/Go SDKs (E24), cold-tier archive (E26), row checksums (E29).

---

## Program order & effort (calendar-honest, one engineer + agents)

| Phase | Duration | Can start |
|---|---|---|
| 1 Correctness | ~1 week | now |
| 2 Hardening P0 | ~2 weeks | now ∥ with 1 |
| 3 Endurance | ~2 weeks elapsed (soaks are wall-clock) | after 1; 3.1 can start now |
| 4 Perf walls | 1–3 weeks (scope = what 3 finds) | after 3 measurements |
| 5 HA | ~2 weeks | after 3 |
| 6 Polish | ~1 week | anytime ∥ |

**Total: ~6–8 working weeks to the final gate** (7-day soak signed off), assuming Phase 4 confirms only the known memmove wall.

## Self-review notes
- Spec coverage: all inventory items A1–E30 are either tasked (A1→1.1, A8/E12→1.3, C7/E14→1.4, C4/C5→1.5, D1–D7→2.1–2.7, B2/B3→2.3/4.4, C1/C2/C9→3.2–3.4, E20→3.5, D8–D10→5.x/6, E17–E21→4.4/5.4/6) or explicitly deferred to the tracked long-tail backlog with rationale.
- Fixed-vs-open: items the inventory marked ✅ FIXED (A2–A7, A9, E1–E5 partials) are deliberately not re-tasked; Task 1.1 Step 2 handles the one ambiguous case (A1) by testing before fixing.
- Contradiction flagged for resolution in-task: `admin_token` exists in config.rs but PRODUCTION_READINESS.md says the port is unauthenticated (Task 2.1 resolves empirically).

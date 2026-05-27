# Replica-Reads Worklog

**Goal.** Allow cqserver to scale subscriber fan-out across multiple
hosts by adding a "read-replica" deployment mode following the AMPS
production pattern: one leader accepts publishes; N followers carry
the full state via the existing `cq-replication` shipper and serve
subscribes; clients connect via a multi-URI list and fail over
automatically.

**Why this shape, not topic-prefix sharding.** Our bottleneck is
read fan-out (1 publisher → 2 000+ subscribers per topic).
Prefix-sharding addresses the write path. Replication-based fanout
is the right shape for read-heavy workloads; it's also what AMPS
actually does in production deployments. See
`HIGH_SCALE_WORKLOG.md#h6` for the rejected alternative.

**Scope guard.** This worklog covers ONLY:
- Server-side: a follower mode that rejects publishes and serves
  subscribes against state driven by the replication receiver.
- Client-side: multi-URI connect with reconnect-on-loss.
- Operator docs + a 1-leader-2-followers e2e test.

Out of scope:
- Active-active multi-leader
- Dynamic follower-discovery (Consul / DNS-SRV)
- Topic-prefix sharding (the H6.1 primitive stays; it's not the
  path forward for the 2K-sub problem)
- Auto-scaling / orchestration (Kubernetes HPA, Nomad, etc.)
- Catch-up resync from cold (depends on the existing shipper's
  Hello-with-highwater protocol, which only resumes from the txlog
  high-water — a follower that has fallen too far behind requires
  manual reseed today).

---

## Existing pieces (verified before scoping)

What's already in the tree on `msrv-1.78`:

- `cq-replication::shipper::run(ShipperConfig)` — connects to a peer,
  exchanges Hello + highwater, streams missing entries, accepts acks
  back. Filter + transform supported.
- `cq-replication::receiver::run(ReceiverConfig, topics)` — listens
  on a TCP port, accepts one primary, applies entries via
  `replay_upsert_map` / `replay_delete`.
- `ServerConfig.replication: ReplicationConfig` with role
  `Standalone` / `Primary` / `Standby`.
- `cq-server::main::run_replication` already dispatches to shipper
  vs receiver based on role.
- WS + TCP transports run regardless of role (so a standby already
  accepts client connections today).
- `/admin/replication` endpoint exposes per-topic shipped / acked /
  applied sequences for monitoring.

What's **missing**:

1. The standby has no guard against client publishes — a misdirected
   publisher would create a split-brain by writing only to the
   standby's in-memory state.
2. The client has no multi-URI failover; connection-loss requires
   the application to handle reconnect.
3. No operator-facing docs explaining how to deploy a
   leader + N followers topology.
4. No e2e test that exercises multi-instance state convergence
   across cqserver processes.

---

## Sessions

### S1 — Read-only server mode (publish-rejection guard)

**Goal.** Make `role = standby` actually safe: a standby rejects
publish + delta_publish with a clear error so a misdirected
publisher learns immediately.

**Files touched:**
- `crates/cq-transport/src/router.rs` — add `read_only: bool` to
  `RouterContext`; guard `handle_publish_inner` (and the queue-publish
  path inside it) with a single early-return when set.
- `crates/cq-server/src/main.rs` — populate `read_only` from
  `server_config.replication.role == ReplicationRole::Standby`.
- `crates/cq-transport/src/websocket.rs` / `tcp.rs` — pass through to
  the constructed RouterContext. (Whichever module owns ctx
  construction.)

**Test plan:**
- Unit test in `router.rs`: build a RouterContext with `read_only =
  true`, send a Publish, assert the response is an error frame whose
  message starts with `"read-only follower"`.
- Same test for DeltaPublish.
- Existing publish tests with `read_only = false` (default) still pass.

**Definition of done:**
- All cq-transport tests green.
- `cargo build --workspace` clean.
- Manual smoke: start a `role = standby` server, attempt a publish
  via `cq-client`, see the error.

**Estimated effort:** ~3 hours.

---

### S2a — Initial-connect failover (`Client::connect_any`) — ✅ done

**Goal.** `Client::connect_any(&[uri1, uri2, uri3, ...])` tries the
URLs in randomized order and returns the first successful client.
This is the foundational primitive for any multi-follower deployment
— without it, every client would hammer the first URL in the list.

**What landed.**
- `crates/cq-client/src/client.rs::connect_any` and
  `::connect_any_with(urls, cfg)`.
- `shuffled_indices(n)` helper — stdlib-only xorshift Fisher-Yates
  seeded from process clock + a stack-ish address; no `rand` dep.
- Unit tests: permutation invariant, edge cases (n=0, n=1), and a
  variability sanity check.
- Integration tests in `crates/cq-client/tests/connect_any.rs`:
  - all-dead URL list returns an error (no hang, no panic)
  - one-live + one-dead succeeds across multiple runs (catches a
    bug in either ordering path)
  - empty URL list returns InvalidUrl

### S2b — Live reconnect-on-loss — ⏳ deferred to a follow-up session

**Why deferred.** The existing `Client::spawn(transport, cfg)` is
single-shot: the driver loop ends when the transport dies and the
client becomes unusable. Adding "reconnect on disconnect" requires
restructuring the driver to be re-entrant and re-subscribing the
client's active subscriptions against the new socket — that's a
larger and riskier change than the connect-any primitive and
deserves its own session with its own test plan.

**Sketch for S2b:**
- Wrap the driver loop in a supervisor that, on transport-EOF or
  IO error, restarts using the stored `connect_any` URL list.
- Maintain a record of active subscriptions (topic + filter/sql +
  options) so they can be re-issued after reconnect.
- Surface a "reconnecting" state on the public Client so application
  code can pause publishes that would queue indefinitely.
- Test: spawn 2 listeners, connect, kill the one the client landed
  on, assert subs continue to deliver via the other within a
  deadline.

### S2c — TypeScript client mirror — ⏳ deferred to a follow-up session

Mirror `connectAny(urls)` in `client-sdks/ts/src/cq-client.ts`. Same
random-order initial-connect semantics. Tests with jest + mock
servers. Done after S2b so we can choose whether to also mirror
the reconnect behaviour.

---

### S3a — Operator docs + follower-mode e2e — ✅ done

**What landed.**
- `docs/deploy/replica-reads.md` — full operator runbook covering
  architecture, leader/follower TOML, L4 LB configs (HAProxy,
  nginx stream, AWS NLB), monitoring + alerting, failure modes
  (leader down, follower down, partition, cold-start).
- `cq-e2e-tests::ReplicationOpts` + `ServerOpts.replication` —
  harness support for spawning a server with a `[replication]`
  block.
- `crates/cq-e2e-tests/tests/replica_reads.rs` — 3 e2e tests:
  1. `standby_rejects_publish_with_read_only_error` — verifies a
     `role = standby` server returns the expected error to a real
     wire-level publish through the SDK.
  2. `standby_publish_rejected_metric_increments` — verifies the
     `cq_publish_rejected_read_only_total` counter advances.
  3. `standby_subscribe_still_works` — verifies the read path
     remains functional on a follower.

### S3b — Multi-instance state-convergence e2e — ⏳ deferred to a follow-up session

**Why deferred.** The bigger e2e originally planned for S3 (spawn
1 leader + 2 followers, publish on leader, observe SOW + deltas
on both followers, kill one mid-stream and verify the other still
serves) depends on:
- Confirming the existing `cq-replication::shipper` works through
  the harness subprocess boundary (no e2e tests currently exercise
  it; only unit/integration tests against in-process topics).
- Coordinating dynamic ports so the primary's `peer` setting
  matches the standby's `listen` setting at boot.
- Time-bounded waits for replication lag to settle before assertions.

Each of those is plausible but warrants careful test-isolation
work — flaky multi-process e2e tests are worse than no test at
all. Tracking as a separate session.

**Sketch for S3b:**
1. Add `ReplicationOpts::primary(peer)` already done; use it.
2. Spawn leader first, observe its `replication.peer` port.
3. Spawn follower with `replication.listen = <port>`.
4. Wait for `cq_repl_connect_total` to advance to 1 on the leader.
5. Publish a known row on leader; subscribe to follower; assert
   the row appears.
6. Kill follower; verify leader's `cq_repl_reconnect_total`
   advances. Restart follower; verify state catches up.

---

## Order of execution

S1 → S2 → S3. Each session is independently shippable; S2 doesn't
strictly depend on S1 (a multi-URI client works against a single
standalone server too), but S1 is smaller so it goes first to
keep momentum.

## Status

| # | Session | Status |
|---|---|---|
| S1 | Read-only server mode | ✅ done — `read_only` flag on `RouterContext`, fired before topic lookup, `Standby` role wires it via `WsConfig`/`TcpConfig`. Test: `tcp::tests::tcp_read_only_rejects_publish`. |
| S2a | `Client::connect_any` initial-connect failover | ✅ done — random-order multi-URI connect (stdlib-only shuffle, no `rand` dep). Tests in `connect_any.rs`. |
| S2b | Live reconnect-on-loss | ⏳ deferred to follow-up session |
| S2c | TypeScript client mirror | ⏳ deferred (do after S2b) |
| S3a | Operator docs + follower-mode e2e | ✅ done — `docs/deploy/replica-reads.md`, harness `ReplicationOpts`, 3 e2e tests in `replica_reads.rs`. |
| S3b | Multi-instance state-convergence e2e | ⏳ deferred (depends on confirming shipper through subprocess boundary) |

(Update this table at the end of each session.)

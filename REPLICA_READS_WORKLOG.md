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

### S2 — Multi-URI client with random-order failover

**Goal.** `Client::connect_any(&[uri1, uri2, uri3, ...])` that picks
a URI at random (load-spreading across many simultaneous clients),
tries it, and on disconnect picks another. Same model AMPS HAClient
uses; no directory service required.

**Files touched:**
- `crates/cq-client/src/lib.rs` (or wherever `connect` lives) — new
  `connect_any` method.
- `crates/cq-client/src/reconnect.rs` (new or existing) — reconnect
  loop with exponential backoff, capped retries.
- `clients/ts/src/cq-client.ts` — mirror in the TypeScript client
  (config takes `urls?: string[]` instead of `url: string`).

**Test plan:**
- Rust unit test: stub two listeners, one refuses, one accepts;
  `connect_any` lands on the accepting one. Random-order means the
  test must accept either order being tried first.
- Rust integration test: spawn two listeners, connect, kill the one
  the client landed on, assert the client reconnects to the other
  within a deadline.
- TS test: same shape, jest + mock servers.

**Definition of done:**
- All cq-client tests green.
- Both Rust + TS clients accept a multi-URI list.
- Manual smoke: start leader + 2 followers, connect via multi-URI,
  kill one follower, observe seamless failover.

**Estimated effort:** ~1 day.

---

### S3 — Operator docs + 1-leader-2-followers e2e test

**Goal.** Make the deployment story executable end-to-end and
write down how to run it in production.

**Files touched:**
- `docs/deploy/replica-reads.md` — new. Cover:
  - Architecture diagram (leader, followers, LB, clients)
  - TOML fragments for `role = primary` and `role = standby`
  - Example L4 LB configs: HAProxy `mode tcp`, nginx `stream`,
    AWS NLB target group
  - Monitoring expectations: which metrics to scrape from
    `/admin/replication` and what alerts to set on lag
  - Failure modes: leader down, follower down, network partition,
    cold-start a fresh follower
- `crates/cq-e2e-tests/tests/replica_reads.rs` — new. Spawns:
  - 1 leader on dynamic ports
  - 2 followers configured to receive from the leader
  - Publishes a known sequence of mutations on the leader
  - Subscribes via both followers, asserts SOW + live deltas match
    byte-for-byte
  - Kills one follower mid-stream, asserts the other still serves

**Test plan:**
- The new e2e test itself is the test plan; CI green = done.
- Doc review: manually walk a fresh reader through the deploy guide
  on a Linux box, see that the topology comes up.

**Definition of done:**
- `cargo test -p cq-e2e-tests replica_reads` passes locally.
- `docs/deploy/replica-reads.md` exists and is linked from the main
  README's "Operations" section.

**Estimated effort:** ~1 day.

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
| S2 | Multi-URI client + reconnect | ⏳ pending |
| S3 | Operator docs + e2e test | ⏳ pending |

(Update this table at the end of each session.)

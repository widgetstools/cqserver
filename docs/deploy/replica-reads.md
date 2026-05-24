# Read-replica deployment

This guide describes how to deploy cqserver as one leader plus N
followers to scale subscriber fan-out across multiple hosts. Each
host contributes its own NIC, CPU, and RAM; subscriber connections
are spread across followers via an L4 load balancer or a multi-URI
client.

This is the deployment shape recommended above ~5 K concurrent
subscribers per host. Below that ceiling a single, well-tuned
standalone instance is fine.

---

## Architecture

```
                    publishers
                        │
                        ▼
                  ┌──────────┐
                  │ leader   │   role = primary
                  │ (writes) │   accepts publishes, ships journal
                  └─────┬────┘
       replication ↙    ↓    ↘ replication
            ┌─────┐  ┌─────┐  ┌─────┐
            │ F1  │  │ F2  │  │ F3  │   role = standby
            └──┬──┘  └──┬──┘  └──┬──┘   accepts subscribes only;
               │        │        │     publishes rejected
               ▼        ▼        ▼
            ┌────────────────────────┐
            │ L4 load balancer       │   (or DNS round-robin /
            │ (or no LB; clients     │    multi-URI client)
            │  use connect_any)      │
            └─────────────┬──────────┘
                          ▼
                    subscribers
```

**Key properties:**
- Leader owns the *write* path. Every mutation flows through it.
- Each follower carries the *full* state via the existing
  `cq-replication` shipper / receiver. There is no topic sharding.
- Followers reject publishes with `"read-only follower; publish to
  leader"` so a misdirected publisher fails fast.
- Clients use `Client::connect_any(&[follower-urls])` (Rust) or
  the equivalent on other SDKs, OR connect through an L4 LB that
  distributes across followers.

---

## Leader configuration

```toml
# cqserver.toml  (leader)

tcp_addr       = "0.0.0.0:9007"
websocket_addr = "0.0.0.0:9008"
websocket_path = "/cq/json"
admin_addr     = "0.0.0.0:8085"

[transport]
outbound_queue_capacity = 2048

[txlog]
directory = "/var/lib/cqserver/txlog"

[replication]
role = "primary"
# Each follower's receiver listens on its own host:port. The
# shipper opens one TCP connection per follower (use multiple
# `[replication]` blocks if you need more than one follower —
# see "Multiple followers" below).
peer = "follower-1.internal:9010"

# … your [[topics]] / [[views]] / [[queues]] follow as normal …
```

Note: the current shipper code targets ONE peer per primary process
(see `crates/cq-replication/src/shipper.rs::ShipperConfig::peer`).
Multi-follower fan-out from one leader currently requires either
running multiple `cq-shipper` companion processes or extending
`ShipperConfig` to take a list of peers — flagged as a follow-up
task in `REPLICA_READS_WORKLOG.md`.

---

## Follower configuration

```toml
# cqserver.toml  (follower)

tcp_addr       = "0.0.0.0:9007"
websocket_addr = "0.0.0.0:9008"
websocket_path = "/cq/json"
admin_addr     = "0.0.0.0:8085"

[transport]
outbound_queue_capacity = 2048

[txlog]
directory = "/var/lib/cqserver/txlog-follower"

[replication]
role   = "standby"
listen = "0.0.0.0:9010"   # leader's shipper connects here

# Identical [[topics]] section to the leader. Schema must match,
# otherwise replays will fail to apply. (Future work: ship the
# schema alongside the highwater so followers auto-discover.)
```

When a follower boots:
1. The transports start in **read-only mode**. Subscribe + sow work
   normally; publish + delta_publish return
   `"read-only follower; publish to leader"`.
2. The replication receiver listens on `replication.listen`.
3. When the leader's shipper connects, the follower sends a
   `Hello { highwater }` summarizing what it already has. The
   shipper resumes from there.
4. Every applied entry is acked so the leader's S11 barrier
   releases (if you have publishers in sync mode).

---

## Client connection patterns

### Option A — Multi-URI client (no LB needed)

```rust
use cq_client::Client;

let follower_urls = vec![
    "tcp://follower-1.internal:9007",
    "tcp://follower-2.internal:9007",
    "tcp://follower-3.internal:9007",
];
let urls: Vec<&str> = follower_urls.iter().map(|s| s.as_str()).collect();
let client = Client::connect_any(&urls).await?;
```

`connect_any` randomizes the order on each call, so when many
processes start simultaneously they spread across followers
instead of all hammering the first URL. **Reconnect-on-loss is
not yet shipped** (worklog S2b); applications need their own
reconnect logic if a follower dies mid-stream.

### Option B — L4 load balancer

Put an L4 LB in front of the followers. cqserver uses TCP (or
WebSocket over TCP) framing — L4 is sufficient, you don't need
L7. Tested shapes:

**HAProxy:**
```haproxy
frontend cqserver_tcp
    bind *:9007
    mode tcp
    default_backend cqserver_followers

backend cqserver_followers
    mode tcp
    balance leastconn          # or roundrobin
    server f1 follower-1.internal:9007 check
    server f2 follower-2.internal:9007 check
    server f3 follower-3.internal:9007 check
```

**nginx stream module:**
```nginx
stream {
    upstream cqserver_followers {
        least_conn;
        server follower-1.internal:9007;
        server follower-2.internal:9007;
        server follower-3.internal:9007;
    }
    server {
        listen 9007;
        proxy_pass cqserver_followers;
    }
}
```

**AWS NLB:** create a target group with the three follower
EC2 instances, listener forwarding TCP/9007. `least_outstanding_requests`
algorithm spreads new subscribers across the healthy targets.

A handful of important LB knobs:

- **Idle timeout**: cqserver subscriptions are long-lived. Default
  60 s idle timeouts (AWS NLB) will kill subs that aren't actively
  receiving deltas. Bump to 3600 s+ or rely on cqserver's heartbeat
  (configured under `heartbeat_interval_s`) which sends ping
  frames at a configured cadence.
- **Hash stickiness**: NOT needed. Subscribers are stateless across
  followers — every follower carries the full state. If a follower
  dies, the client reconnects via the LB and gets routed to another
  follower whose SOW is byte-for-byte equivalent (modulo ms-scale
  replication lag).
- **Health checks**: poll `/healthz` on the admin port (8085 by
  default). A standalone TCP health check on 9007 is also fine —
  cqserver's TCP accept loop comes up shortly before the admin
  server.

---

## Monitoring

Metrics exposed under `GET /metrics` (Prometheus text format) and
relevant to a replica-reads deployment:

| Metric | What it tells you |
|---|---|
| `cq_repl_shipped_max_sequence{topic}` | Highest seq the leader has shipped for `topic`. |
| `cq_repl_applied_max_sequence{topic}` | Highest seq the follower has applied for `topic`. |
| `cq_repl_acked_max_sequence{topic}` | Highest seq the follower has acked back to the leader. |
| `cq_repl_acks_received_total{topic}` | Cumulative acks. Stuck = follower not advancing. |
| `cq_repl_connect_total` | Shipper-side connects to its peer. Spikes = leader↔follower flapping. |
| `cq_repl_reconnect_total` | Shipper reconnect attempts. Steady increase = network instability. |
| `cq_repl_session_error_total` | Receiver-side session ends. Should be near zero in steady state. |
| `cq_publish_rejected_read_only_total` | Publishes a follower refused. **Non-zero = misconfigured client or LB.** |

Alert ideas:

- `(cq_repl_shipped_max_sequence - cq_repl_applied_max_sequence) > N`
  for more than M minutes — replication lag growing.
- `rate(cq_publish_rejected_read_only_total[5m]) > 0` — somebody
  is publishing to a follower (LB misroute or client bug).
- `rate(cq_repl_reconnect_total[5m]) > 1` — replication link is
  flapping.

The `/admin/replication` endpoint provides the same per-topic
sequence data as a single JSON blob — handy for quick `curl`
checks during incidents.

---

## Failure modes and recovery

### Leader dies

Followers continue serving subscribes against their last-known
state. **No new publishes can land** until the leader is back. When
the leader returns, the shipper reconnects, sends Hello, and
follower state resumes streaming from the last applied sequence —
no manual intervention.

Active-active leader failover (promoting a follower to leader on
demand) is NOT yet supported. That's a separate piece of work
(consensus + split-brain prevention) and is out of scope for the
replica-reads model.

### Follower dies

The L4 LB or the multi-URI client routes new subscriber connections
to the surviving followers. Existing subscriber connections to the
dead follower drop and need to reconnect — application code is
responsible until S2b ships. Once the follower is back, the
shipper reconnects (with the follower's current highwater) and
state resumes; the follower then accepts new subscribers via the LB.

### Network partition between leader and a follower

The shipper retries with backoff (default 2s). While partitioned,
the follower serves stale state — subscribers see no new deltas
on topics that have been mutated on the leader during the partition.
When the link heals the follower catches up via the Hello +
resume-from-highwater protocol.

This is **at-least-once**, not exactly-once: a publisher with a
write that's been acked but not yet replicated will lose its
mutation if the leader is destroyed mid-replication. For
applications that need stricter guarantees, configure the publisher
to wait for replication ack (`mark_replicated` / S11 barrier).

### Cold-start a fresh follower

Today, a follower started against an empty txlog will receive
*everything* from the leader on first connection — the Hello
frame's highwater map omits unknown topics, so the shipper streams
from seq=1. On large topics this is a long initial replication
window during which the new follower is *not* yet useful for
subscribes (its state is incomplete).

Recommended cold-start procedure:
1. Boot the new follower with `role = standby` and an empty txlog.
2. Wait for `cq_repl_applied_max_sequence` to catch up to the
   leader's `cq_repl_shipped_max_sequence` (typically a few seconds
   to a few minutes depending on the topic sizes).
3. Add the follower to the LB / multi-URI client list.

Future work (out of scope here): bootstrap from a leader-side
snapshot rather than replaying the full journal.

---

## What this deployment shape doesn't address

- **Multi-leader / active-active.** Out of scope. Use the single
  leader for all writes.
- **Geo-distributed deployments.** Followers within a single
  region behave well; cross-region followers will see higher lag
  and need tuning of `cq_repl_reconnect_backoff`.
- **Per-topic sharding.** The H6.1 prefix-shard primitive
  (`/admin/shard-for/:topic`) exists in the tree but is **not**
  the answer for read fan-out. See `HIGH_SCALE_WORKLOG.md` for
  the rationale.
- **Auto-scaling.** Bring followers up and tear them down via
  whatever orchestrator you already use (Kubernetes, Nomad,
  Terraform). cqserver doesn't manage its own topology.

---

## Worklog & open items

Tracked in `REPLICA_READS_WORKLOG.md`:

- S1 ✅ Read-only follower mode (publish-rejection guard).
- S2a ✅ `Client::connect_any` initial-connect failover.
- S2b ⏳ Live reconnect-on-loss in the client.
- S2c ⏳ TypeScript client mirror.
- S3 ⏳ Multi-instance state-convergence e2e test.
- Multi-peer shipper (one leader → N followers in one process)
  remains a follow-up.

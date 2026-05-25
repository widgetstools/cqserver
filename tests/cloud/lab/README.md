# tests/cloud/lab — two-Mac replication + stress lab

The cheapest way to validate cqserver's replica-reads pattern across
a **real network** with **real NIC bandwidth**: two Macs on the same
LAN running native binaries — no Docker, no VMs, no cloud spend.

This sits between [C0](../README.md) (loopback docker-compose) and
[C1](../../../CLOUD_REPLICATION_TEST_WORKLOG.md) (AWS Spot) in the
[`CLOUD_REPLICATION_TEST_WORKLOG.md`](../../../CLOUD_REPLICATION_TEST_WORKLOG.md)
plan. It answers everything C0 can't (real network + NIC math) at
$0 incremental cost.

```
   Mac A (192.168.1.10) "leader"        Mac B (192.168.1.20) "follower"
   ─────────────────────────────         ─────────────────────────────
   cqserver --role=primary               cqserver --role=standby
     peers = ["192.168.1.20:9010"]         listen = "0.0.0.0:9010"
     :9007  tcp clients                    :9007  subscriber clients
     :8085  admin/ui                       :8085  admin/ui
                                           :9010  replication receiver
   cq-loadgen (publisher)                cq-loadgen (subscribers)
     publishes 200/sec to leader            2000 subs via stress2k-real
     via tcp://127.0.0.1:9007               via tcp://192.168.1.10:9007 ← LAN

                       \\ replication TCP ↓
                       └──── /lab-orders txlog ────┘
                          shipper → receiver
```

The subscribers connect to the **follower's** TCP, so all subscriber
egress flows through Mac B's NIC. The leader's egress is just the
replication stream + admin polls. That's exactly the replica-reads
deployment shape.

## What you need

- Two Macs on the same physical LAN (Wi-Fi works but wired Ethernet
  is dramatically better for stress numbers — see "Wi-Fi caveats"
  below).
- Each Mac with the cqserver release binary built. Either:
  - `cargo build --release -p cq-server -p cq-loadgen` on each Mac
    (Rust 1.78+), or
  - `scp` the binary from one Mac to the other (faster — Macs use
    the same arch).
- macOS firewall allowing inbound TCP on `:9007`, `:9010`, and
  `:8085` for the cqserver binary. macOS will prompt the first time
  cqserver tries to listen; click "Allow."

## Setup

### Step 1 — find each Mac's LAN IP

On each Mac:

```sh
# Wi-Fi:
ipconfig getifaddr en0

# Wired Ethernet (Thunderbolt / USB-C adapter):
ipconfig getifaddr en1
```

Note the two IPs. Call them `LEADER_IP` and `FOLLOWER_IP`. We'll use
`192.168.1.10` and `192.168.1.20` in the examples below — substitute
your actual values.

### Step 2 — confirm connectivity

From Mac A (will be the leader):
```sh
ping -c 3 192.168.1.20    # follower IP
```

You want sub-2 ms latency on a good LAN. Wi-Fi typically sits at
1-5 ms, Ethernet at 0.2-0.5 ms.

### Step 3 — clone + build on each Mac

```sh
# On both Macs:
git clone https://github.com/widgetstools/cqserver.git
cd cqserver
git checkout msrv-1.78
cargo build --release -p cq-server -p cq-loadgen
```

Alternative — build once, scp:

```sh
# Build on Mac A:
cargo build --release -p cq-server -p cq-loadgen

# Copy binary to Mac B (same arch, both Apple Silicon or both Intel):
scp target/release/cqserver target/release/cq-loadgen \
    user@192.168.1.20:~/cqserver-bin/

# On Mac B, point the lab at the scp'd binary:
export LAB_BINARY=~/cqserver-bin/cqserver
export LOADGEN_BINARY=~/cqserver-bin/cq-loadgen
```

## Running

### Step 4 — start the follower (Mac B)

```sh
cd cqserver/tests/cloud/lab
./scripts/lab-up-follower.sh
```

The script will:
- Render `configs/follower.toml.template` into `/tmp/cqserver-lab/follower/cqserver.toml`
- Print the bind address + admin URL
- Exec `cqserver` in the foreground (Ctrl+C to stop)

You should see in the logs:
```
INFO cqserver: Replication role role=Standby
INFO cq_replication::receiver: Replication receiver listening addr=0.0.0.0:9010
```

Leave that terminal open.

If macOS prompts "Do you want to allow cqserver to accept incoming
connections?" → click **Allow**.

### Step 5 — start the leader (Mac A)

```sh
cd cqserver/tests/cloud/lab
FOLLOWER1_IP=192.168.1.20 ./scripts/lab-up-leader.sh
```

(Replace `192.168.1.20` with your follower Mac's actual IP.)

Within a couple of seconds you should see in the leader's log:
```
INFO cq_replication::shipper: Replication shipper connected peer=192.168.1.20:9010
```

And on the follower:
```
INFO cq_replication::receiver: Replication primary connected peer=192.168.1.10:51234
```

If the leader keeps logging `Replication shipper disconnected; reconnecting`,
the follower's `:9010` isn't reachable. See "Troubleshooting" below.

### Step 6 — verify with the admin UI

Open in a browser on either Mac:

- Leader UI:   `http://192.168.1.10:8085/ui/`
- Follower UI: `http://192.168.1.20:8085/ui/`

The Replication page on each side should show:
- Leader: `role=primary`, `peers=[192.168.1.20:9010]`
- Follower: `role=standby`, `listen=0.0.0.0:9010`

### Step 7 — run the stress

From Mac A (where the leader runs):

```sh
FOLLOWER1_IP=192.168.1.20 ./scripts/lab-stress.sh
```

This does three things:

1. Publishes ~400 rows to `/lab-orders` on the leader (via loopback)
2. Waits for the follower to apply every entry
3. Spins up 2 000 subscribers against the **follower** via the LAN,
   running the realistic `stress2k-real` scenario for 30 seconds

Results land in `tests/cloud/lab/results/<timestamp>/` as JSON
snapshots from `/stats`, `/metrics`, and `/admin/replication` on
both nodes at three points: baseline / post-publish / post-stress.

Knobs (all optional):
```sh
SUBS=500              # 500 subs instead of 2000
DURATION_SECS=60      # 60-second measurement window
PUBLISH_RATE=1000     # higher publish rate
PUBLISH_DURATION=10   # publish for longer
./scripts/lab-stress.sh
```

### Step 8 — teardown

On each Mac, Ctrl+C the cqserver process, or:

```sh
./scripts/lab-down.sh           # stop processes, keep txlog
./scripts/lab-down.sh --purge   # stop + delete /tmp/cqserver-lab/*
```

## What the results tell you

Each results directory contains:

| File | Contents |
|---|---|
| `leader-stats-baseline.json` | RSS / topic-count / sub-count before any traffic |
| `follower-stats-post-stress.json` | RSS / sub-count after 2K subs ramped |
| `*-metrics-*.txt` | Full Prometheus dump at three points |
| `*-replication-*.json` | `/admin/replication` snapshot incl. per-topic sequences |
| `phase1-publish.log` | Loadgen output for the publish phase |
| `phase2-stress.log` | Loadgen output for the subscribe phase (peak RSS, rates, per-class deliveries) |

The headline questions:

- **Follower NIC math:** `phase2-stress.log` reports per-class
  delivery rate × number of subs in that class. Multiply by the
  average row size in `phase2-stress.log` to get steady-state
  follower egress in bytes/sec. Compare to the follower Mac's NIC
  capability (Wi-Fi-6 ≈ 1 Gbps; wired Gigabit = 1 Gbps; Thunderbolt
  10 GbE = 10 Gbps).

- **Replication lag:** parse `cq_repl_shipped_max_sequence` vs
  `cq_repl_applied_max_sequence` for `/lab-orders` across the
  `metrics-*.txt` snapshots; the difference is the lag in
  sequences. With a 200/sec publish rate on a low-latency LAN you
  should see < 100 sequence lag (sub-500ms wall-clock).

- **Memory under realistic load:** compare baseline vs
  post-stress `processRssBytes` on the follower. From our
  loopback `stress2k-real` measurement this was +118 MB on a
  2K-sub run. On a real NIC the number should be similar — the
  test is mostly verifying it doesn't unexpectedly balloon.

## Single-Mac smoke check

You don't need a second Mac to verify the *scripts* wire up
correctly. Start the leader alone, pointing at an unreachable
peer, and verify it boots + the multi-peer config rendered:

```sh
FOLLOWER1_IP=192.0.2.99 \
LAB_BIND_IP=127.0.0.1 \
LAB_TXLOG_DIR=/tmp/cqserver-lab-smoke/leader/txlog \
./scripts/lab-up-leader.sh
```

In another terminal:

```sh
curl -fsS http://127.0.0.1:8085/admin/replication | python3 -m json.tool
```

You want to see:

```json
{
  "role": "primary",
  "peers": ["192.0.2.99:9010"],
  ...
}
```

The shipper will log `Replication shipper disconnected; reconnecting`
every couple of seconds (expected — the peer doesn't exist). That
confirms script + config + multi-peer plumbing all work.

To validate the actual replication round-trip on one Mac, use the
[loopback `c0` docker-compose harness](../README.md) instead — it
spins up leader + 2 followers in separate containers with distinct
ports. The `lab/` scripts here run on the standard `:9007 / :8085 /
:9010` port layout per process, so running two on the same Mac
would port-conflict.

## Wi-Fi caveats

Wi-Fi between two Macs goes:

- Mac A Wi-Fi NIC → router → Mac B Wi-Fi NIC

Most home routers do this at 200-600 Mbps real-world even on
Wi-Fi-6. A 2K-sub stress with 50-100 KB/sec/sub gives 100-200
MB/sec → **likely saturates the Wi-Fi link, not the cqserver**.
That's actually fine — it confirms the network is the bottleneck,
not the server.

For honest cqserver perf numbers under load, run on:

1. **Wired Ethernet via Thunderbolt → USB-C → 10 GbE dongle** —
   gives sustained 10 Gbps. Both Macs need the dongle. ~$200/Mac
   one-time cost.
2. **Wired Ethernet via built-in 1 GbE** — Mac Studio / Mac mini
   ports. Gives 1 Gbps reliably.
3. **Thunderbolt cable directly between two Macs** — Thunderbolt
   Bridge. macOS detects the cable and auto-configures a 10 Gbps
   link. Single cable, no dongles needed. **Best bang for $0.**

The lab scripts don't care which transport — they bind to the IP
you give them.

## Troubleshooting

**`Replication shipper disconnected; reconnecting` repeats forever**

The leader can't reach the follower's :9010. Diagnose:

```sh
# From the leader Mac, can you reach the follower's port?
nc -zv 192.168.1.20 9010

# If "Connection refused" → follower isn't running, or it's
# listening on 127.0.0.1 only:
ssh follower-mac 'lsof -iTCP:9010 -sTCP:LISTEN'
# Want to see *:9010 (or 0.0.0.0:9010), not 127.0.0.1:9010.

# If "Operation timed out" → macOS firewall is blocking it on the
# follower side:
ssh follower-mac 'sudo /usr/libexec/ApplicationFirewall/socketfilterfw --getglobalstate'
# Either disable the firewall while testing:
ssh follower-mac 'sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate off'
# Or whitelist the binary:
ssh follower-mac 'sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /path/to/cqserver'
```

**`leader-stats-baseline.json` is empty / curl 7 connection refused**

The leader didn't start. Common cause: another process is using
`:9007` (e.g. you have the demo running from `start-demo.sh`).
Either stop the conflict or change `LAB_BIND_IP` to bind to a
different interface.

**Follower row count stays at 0 after publish**

The follower's `/lab-orders` topic schema doesn't match the
leader's. Both configs ship the same inline columns, so this
shouldn't happen unless you edited the templates. Check
`docker logs follower` (sorry — `cqserver` log on the follower
Mac) for `Replicated entry payload was not a JSON object` or
similar warnings.

## What this lab does NOT test

- **Variable cloud-network latency.** Both Macs see consistent
  sub-millisecond LAN latency. Inter-AZ in cloud is 1-5 ms;
  inter-region is 50-100 ms. C1 in the worklog covers this.
- **Spot interruption / instance churn.** Macs don't get pre-empted.
- **CPU / RAM contention.** Both Macs are single-tenant; cloud
  hosts have noisy-neighbor effects.

For those, see C1.

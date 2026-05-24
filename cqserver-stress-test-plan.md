# CQServer Stress Test Plan

A practical plan for stress-testing CQServer with thousands of subscribers, optimized for low cost. Designed for the current 32 GB target.

---

## Table of Contents

1. [What "Stress Test" Actually Means](#1-what-stress-test-actually-means)
2. [Phase 1 — Local Stress Test (Free)](#2-phase-1--local-stress-test-free)
3. [Phase 2 — Cloud Stress Test (Cheap)](#3-phase-2--cloud-stress-test-cheap)
4. [Cloud Cost Comparison](#4-cloud-cost-comparison)
5. [Load Generator Design](#5-load-generator-design)
6. [Metrics & Observability](#6-metrics--observability)
7. [What to Watch For in Results](#7-what-to-watch-for-in-results)
8. [Cost Ceiling Per Test Campaign](#8-cost-ceiling-per-test-campaign)
9. [Appendix — Tuning Cheatsheet](#9-appendix--tuning-cheatsheet)
10. [Appendix — Sample Load Generator (Rust)](#10-appendix--sample-load-generator-rust)
11. [Appendix — Sample Grafana Dashboard](#11-appendix--sample-grafana-dashboard)

---

## 1. What "Stress Test" Actually Means

Define the scenarios before paying for hardware. Each scenario stresses a different bottleneck.

### Scenario A — Connection capacity

- **Goal**: how many concurrent connections can the server hold without falling over?
- **Setup**: N idle clients each holding one connection, all logged on, no subscriptions.
- **Measure**: server memory per connection, file descriptor usage, time to logon under contention.
- **Pass criteria**: 10,000 connections, < 2 GB server memory overhead from connection state alone.

### Scenario B — Subscription capacity

- **Goal**: how many concurrent subscriptions can a single topic support?
- **Setup**: N clients, each subscribed to the same topic with a unique filter. No publishes.
- **Measure**: memory per subscription, time to register the N-th subscription, predicate index size.
- **Pass criteria**: 10,000 subscriptions, < 5 GB total memory, sub-millisecond registration up to the N-th.

### Scenario C — Publish throughput

- **Goal**: how fast can the server ingest publishes?
- **Setup**: 1 publisher; no subscribers.
- **Measure**: messages/sec sustained, p99 publish-to-ack latency, txlog write rate.
- **Pass criteria**: ≥ 500K messages/sec sustained, p99 ack ≤ 1 ms.

### Scenario D — Fan-out throughput (the big one)

- **Goal**: N subscribers × M publishes/sec. How does the system scale?
- **Setup**: 1K, 5K, 10K subscribers; sweep publish rate from 1K to 100K/sec.
- **Measure**: end-to-end publish-to-delivery latency (p50, p95, p99), dropped messages, queue depths, CPU utilization.
- **Pass criteria**: 10K subscribers + 10K publishes/sec, p99 delivery ≤ 50 ms.

### Scenario E — Reconnect storm

- **Goal**: does the server cleanly handle mass reconnects?
- **Setup**: 10K clients connect, subscribe, hold for 30 s, drop all connections, reconnect.
- **Measure**: time to full reconvergence, memory leaks, file descriptor leaks.
- **Pass criteria**: Reconverges in ≤ 30 s, no monotonic memory or FD growth.

### Scenario F — Wide-row, high-update-rate (rates-like)

- **Goal**: simulate the rates feed pattern that motivated the project.
- **Setup**: 1 topic, 1000 keys, each row has 100+ columns. 100K updates/sec. 1000 subscribers, each filtering for a subset.
- **Measure**: same as Scenario D but with realistic message shape.
- **Pass criteria**: stable for 1 hour, no memory growth, p99 delivery ≤ 20 ms.

### Scenario G — Slow-consumer isolation

- **Goal**: a slow subscriber must not degrade other subscribers.
- **Setup**: 1000 subscribers, 1 of them artificially slow (reads at 10% of publish rate).
- **Measure**: latency to fast subscribers as slow subscriber's queue depth grows.
- **Pass criteria**: fast-subscriber latency unaffected (variance < 10%).

---

## 2. Phase 1 — Local Stress Test (Free)

Start here. A modern laptop or workstation can simulate enough load to find structural issues for free.

### What a 32 GB machine can realistically do

Single-host, server + load generator co-located on the same box:

| Resource | Available | Reasonable upper bound for testing |
|---|---|---|
| RAM | 32 GB | Server: 16 GB; load gen: 8 GB; OS/buffer: 8 GB |
| File descriptors | OS default 1024 | Raise to 1,000,000 (see Appendix §9) |
| Ephemeral ports | ~28K | Raise via `net.ipv4.ip_local_port_range` |
| Concurrent connections | 10K–30K | Linux: 30K easily, MacOS: 10K limit |
| Concurrent tokio tasks | Millions in theory | Practical: 100K-200K before scheduler overhead |
| Publish throughput | 100K–500K/sec | Single-publisher to local server |

### Why local first

- **Free**. Iterate fast on tuning and bug-finding without paying for cloud.
- **Lower noise**. No network jitter, no shared hypervisor. Easier to isolate code bugs from infrastructure noise.
- **Faster cycle**. Edit → rebuild → run. No VM provisioning per iteration.

### Local setup steps

```bash
# 1. Raise file descriptor limits (per session)
ulimit -n 1048576

# 2. Raise kernel limits (Linux; requires root, persists)
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535"
sudo sysctl -w net.ipv4.tcp_tw_reuse=1
sudo sysctl -w net.ipv4.tcp_fin_timeout=15
sudo sysctl -w fs.file-max=2097152

# 3. macOS equivalents
sudo launchctl limit maxfiles 1048576 unlimited
sudo sysctl -w kern.maxfiles=2097152

# 4. Start server with release build
cargo build --release -p cq-server
./target/release/cq-server --config config/cqserver.toml

# 5. In another shell, start the load generator
./target/release/cq-loadgen \
    --subscribers 1000 \
    --publishers 1 \
    --topic /market-data \
    --filter "WHERE desk='RATES'" \
    --duration 60s \
    --publish-rate 10000 \
    --metrics-port 9090
```

### What local can NOT do well

- Network-realistic latency (everything is loopback).
- Multi-tenant CPU contention (your laptop isn't a shared cloud host).
- Sustained 24-hour soak (you need your laptop).
- Scenarios needing > 30K connections (hits OS limits even with tuning).

For everything below 10K subscribers and < 4 hours runtime, local is usually sufficient.

---

## 3. Phase 2 — Cloud Stress Test (Cheap)

Three topologies, in increasing realism and cost.

### Topology A — Single VM, co-located

Server and load generator on one VM. Same shape as local but on someone else's hardware.

**When to use**: Reproducible runs you want to share, longer soaks, machine bigger than your laptop.

**Sizing**: 16 vCPU / 32 GB matches the local target. Allocate 16 GB to server, 8 GB to load gen.

**Cost**: ~$0.10/hour on Hetzner dedicated, ~$0.20-0.30/hour on AWS spot, ~$0.70/hour AWS on-demand.

### Topology B — Two VMs, split (recommended baseline)

Server on VM-A. Load generator on VM-B. Same VPC / same region for low network latency.

**When to use**: Realistic network shape; load gen CPU doesn't steal from server CPU; observability is clean.

**Sizing**:
- VM-A (server): 16 vCPU / 32 GB.
- VM-B (load gen): 8 vCPU / 16 GB. (Load generation is lighter than server work.)

**Cost**: roughly 1.5× Topology A. Still cheap.

### Topology C — Distributed load generation

Server on VM-A. Multiple load generator VMs (B, C, D, …) coordinated by a controller.

**When to use**: Need > 30K concurrent connections, or need geographic distribution to simulate WAN latency.

**Sizing**:
- VM-A (server): 16 vCPU / 32 GB.
- VMs B–E (load gen workers): 4 vCPU / 8 GB each, four of them.

**Cost**: roughly 2.5× Topology B. Still well under $20 for a long test session.

---

## 4. Cloud Cost Comparison

**Caveat**: cloud pricing fluctuates. Verify current rates before committing. Numbers below are 2025-era reference points that are roughly stable in 2026.

### Single VM, 16 vCPU / 32 GB, US East, hourly rate

| Provider | Instance | On-demand | Spot/Preemptible | Notes |
|---|---|---|---|---|
| **Hetzner** | CCX33 (AMD) | ~€0.10/hr | n/a | Dedicated vCPU. EU-only DCs. **Cheapest sustained.** |
| **Hetzner Cloud** | CPX51 | ~€0.10/hr | n/a | Shared vCPU; performance variable |
| **AWS EC2** | c6i.4xlarge | $0.68/hr | $0.15–$0.30/hr | Spot ideal for stress tests |
| **AWS EC2** | c7g.4xlarge (Graviton) | $0.58/hr | $0.13–$0.25/hr | ARM; Rust builds clean for it |
| **GCP** | n2-standard-16 | $0.78/hr | $0.16/hr | Preemptible = spot equivalent |
| **GCP** | c3-standard-22 | $1.05/hr | $0.21/hr | Newer Sapphire Rapids |
| **Azure** | F16s_v2 | $0.68/hr | $0.10–$0.20/hr | |
| **DigitalOcean** | Premium AMD 16cpu/32gb | $0.43/hr | n/a | Hourly; simple billing |
| **Vultr** | Dedicated 16/32 | $0.36/hr | n/a | High-frequency CPUs available |
| **OVH** | b3-32 | ~$0.30/hr | n/a | EU/CA DCs, dedicated cores |
| **Linode/Akamai** | Premium 32GB | $0.40/hr | n/a | |

### What this means in practice

For a **4-hour stress test session** (Topology B: one server + one load gen VM, both at 16/32):

| Path | Cost |
|---|---|
| AWS spot (c6i.4xlarge × 2) | ~$2.50 |
| GCP preemptible (n2-standard-16 × 2) | ~$1.30 |
| Hetzner dedicated (CCX33 × 2) | ~€0.80 (~$0.90) |
| Vultr dedicated (16/32 × 2) | ~$2.90 |
| AWS on-demand (c6i.4xlarge × 2) | ~$5.50 |
| Azure on-demand (F16s_v2 × 2) | ~$5.50 |

For a **24-hour soak test** (Topology B):

| Path | Cost |
|---|---|
| Hetzner dedicated | ~€5 (~$5.50) |
| GCP preemptible | ~$8 (assuming no preemption mid-run) |
| AWS spot (large savings plan unlikely; spot may preempt) | ~$12 |
| AWS on-demand | ~$33 |

### Spot/preemptible warning

Spot and preemptible instances **can be interrupted with little notice** (AWS: 2 minutes; GCP: 30 seconds). For:

- **Short bursts (≤ 4 hours)** of stress tests: spot is fine; if preempted, restart.
- **Soak tests (12+ hours)**: spot will probably preempt. Use Hetzner dedicated, or AWS on-demand, or design the test to checkpoint and resume.

### Recommendation for minimal cost

1. **Iterate locally** until the test scripts and scenarios are stable.
2. **Move to Hetzner dedicated** (CCX line) for everything beyond what local can do. $0.10/hour, no preemption risk, plenty of compute. Total cost for a week of stress testing: under $20.
3. **Use AWS spot or GCP preemptible** only if you need a specific provider's tooling (e.g., the company already has AWS budget you can charge).
4. **Use on-demand or reserved instances only if** you're running production-like loadtests as part of CI for weeks at a time, or you need a specific instance type not available on Hetzner.

---

## 5. Load Generator Design

The load generator is itself a small system. Done wrong, it bottlenecks before the server does. Done right, it tells you exactly where the server bottlenecks.

### Principles

1. **One async runtime, many tasks.** Tokio scales to hundreds of thousands of tasks; don't spawn an OS thread per subscriber.
2. **Pre-allocate state.** Don't allocate inside the hot loop.
3. **Histograms, not averages.** Latency averages hide the p99. Use HDR histograms.
4. **Open-loop, not closed-loop.** A closed-loop generator (publish, wait for ack, publish next) tells you the round-trip time. An open-loop generator (publish at fixed rate regardless of acks) tells you the system's saturation point — which is what stress testing is about.
5. **Separate publisher and subscriber processes.** Different bottlenecks, different metrics.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Load Generator Process                                       │
│                                                              │
│  ┌─────────────────┐    ┌──────────────────┐                │
│  │ Subscribe Pool  │    │  Publish Pool    │                │
│  │ N async tasks   │    │  M async tasks   │                │
│  │                 │    │                  │                │
│  │ Each task:      │    │ Each task:       │                │
│  │  - connect      │    │  - connect       │                │
│  │  - subscribe    │    │  - rate-limit    │                │
│  │  - record       │    │  - publish       │                │
│  │    e2e latency  │    │  - record send   │                │
│  └────────┬────────┘    └────────┬─────────┘                │
│           │                      │                          │
│           └──────────┬───────────┘                          │
│                      ▼                                      │
│           ┌──────────────────────┐                          │
│           │  HDR Histograms      │                          │
│           │  Per-scenario        │                          │
│           └──────────┬───────────┘                          │
│                      ▼                                      │
│           ┌──────────────────────┐                          │
│           │  /metrics endpoint    │                         │
│           │  (Prometheus format) │                          │
│           └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

### CLI shape

```
cq-loadgen \
    --server tcp://host:9007 \
    --scenario fanout              # capacity | fanout | wide-row | reconnect
    --subscribers 10000 \
    --publishers 4 \
    --publish-rate 10000 \
    --message-size 256 \
    --duration 600s \
    --warmup 30s \
    --metrics-port 9090 \
    --histogram-out latencies.hgrm
```

See Appendix §10 for sample Rust implementation.

### Per-scenario sizing

| Scenario | Subscribers | Publishers | Publish rate | Duration |
|---|---|---|---|---|
| A — Connection capacity | 10,000 | 0 | n/a | 5 min |
| B — Subscription capacity | 10,000 | 0 | n/a | 5 min |
| C — Publish throughput | 0 | 1 | max | 5 min |
| D — Fan-out | 1K / 5K / 10K | 1 | sweep 1K → 100K/s | 5 min each |
| E — Reconnect storm | 10,000 (cycled) | 0 | n/a | 10 min |
| F — Wide-row rates | 1000 | 4 | 100K/s | 1 hour |
| G — Slow consumer | 1000 (1 slow) | 1 | 10K/s | 5 min |

Each scenario goes through `warmup → measure → cooldown` phases. Measurement starts after warm-up so JIT-like effects (page cache, allocator warmup, tokio scheduler heuristics) stabilize.

---

## 6. Metrics & Observability

### What to measure

**Server-side** (instrumented via `metrics` + `metrics-exporter-prometheus`, already in your `Cargo.toml`):

- `cq_publish_total` (counter, by topic)
- `cq_publish_duration_seconds` (histogram, by topic, by ack_level)
- `cq_subscriptions_active` (gauge, by topic)
- `cq_connections_active` (gauge)
- `cq_delta_emitted_total` (counter, by subscription)
- `cq_delta_queue_depth` (gauge, by subscription — sample top-N)
- `cq_predicate_eval_duration_seconds` (histogram)
- `cq_txlog_append_duration_seconds` (histogram)
- `cq_txlog_fsync_duration_seconds` (histogram)
- `cq_sow_row_count` (gauge, by topic)
- `cq_memory_bytes` (gauge — total RSS)

**Load-generator-side**:

- `loadgen_publish_total` (counter)
- `loadgen_publish_to_ack_seconds` (histogram)
- `loadgen_publish_to_delta_seconds` (histogram) — the end-to-end metric
- `loadgen_subscription_active` (gauge)
- `loadgen_connection_errors_total` (counter)
- `loadgen_delta_dropped_total` (counter — sequence gap detection)

**OS-side** (via `node_exporter`):

- CPU % per core
- Memory used / available
- TCP connections (`netstat -an | wc -l`)
- File descriptors used
- Network bytes in/out
- Disk I/O (if txlog active)
- Context switches/sec

### Collection stack (free)

```
┌────────────────────┐   ┌────────────────────┐
│ cq-server          │   │ cq-loadgen         │
│ /metrics :9091     │   │ /metrics :9090     │
└─────────┬──────────┘   └─────────┬──────────┘
          │                        │
          └────────┬───────────────┘
                   ▼
           ┌──────────────┐
           │ Prometheus   │  scrape interval: 5s
           │ (single VM)  │
           └──────┬───────┘
                  ▼
           ┌──────────────┐
           │ Grafana       │  dashboards
           │ (same VM)     │
           └──────────────┘
```

All free. Run Prometheus + Grafana in Docker on the load-gen VM (or even on the server VM if you have headroom):

```yaml
# docker-compose.yml
version: '3'
services:
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9292:9090"
  grafana:
    image: grafana/grafana-oss
    ports:
      - "3000:3000"
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
```

### Latency measurement — getting it right

End-to-end latency requires synchronized clocks. Two options:

**Option A (preferred): same machine**
Server and load gen on the same machine. Clock is identical. Publish embeds `Instant::now()`; subscriber computes delta on receipt. Accurate to microseconds.

**Option B: cross-machine**
Use chronyd or systemd-timesyncd to NTP-sync. Accuracy is millisecond-class, which is fine for a system targeting < 50 ms p99. For sub-ms accuracy across hosts, you need PTP, which is more involved.

For most stress tests, Option A or NTP-synced Option B is sufficient.

---

## 7. What to Watch For in Results

The metrics matter less than the patterns. Things to look for:

### Healthy patterns

- **Linear or sublinear scaling**: doubling subscribers doubles (or less than doubles) end-to-end latency. Good — fan-out work is being parallelized.
- **Latency histograms with tight tails**: p99 within 2× of p50. Indicates no GC-like pauses, no lock contention spikes.
- **Stable memory under steady load**: memory rises during ramp, then flat. Indicates no leaks.
- **CPU utilization scales with load**: at 10% target rate, CPU at 10%; at 100%, near saturation. Indicates work is proportional to load.

### Warning signs

- **Latency cliff**: p50 stable, then suddenly p99 jumps 10×+ at some subscriber count or publish rate. Indicates a hidden serialization point, probably a single lock or single channel (see Concern C3 from the review).
- **Memory growth without load growth**: leak. Look at active set reclamation (see Concern C11).
- **Connection acceptance latency growing**: backlog filling. Either OS-level (somaxconn) or app-level (accept loop too slow).
- **High variance run-to-run**: scheduler thrashing or non-deterministic GC-like behavior. Rust shouldn't have this; if you see it, look at allocator behavior (jemalloc vs glibc malloc) and contended locks.
- **Slow consumer affects fast consumers**: violates the design contract. Likely the outbound channel back-pressure is propagating into the evaluator thread.

### The single most important graph

**End-to-end publish-to-delivery latency histogram, faceted by subscriber count.**

Plot p50, p95, p99, p99.9 on the Y-axis, subscriber count on the X-axis (log scale: 100, 1K, 5K, 10K). 

If the lines are flat: great. If they slope: the slope tells you how badly the system scales. If they have a knee: you've found the saturation point.

---

## 8. Cost Ceiling Per Test Campaign

Concrete numbers for budgeting purposes (using cheapest reasonable cloud — Hetzner — unless noted).

| Campaign | Topology | Duration | Cost |
|---|---|---|---|
| Quick sanity (Scenarios A, B, C) | Local | 30 min | $0 |
| Fan-out sweep (Scenario D) | Local | 2 hr | $0 |
| Fan-out sweep with 10K subs | Hetzner Topology B | 2 hr | ~$0.40 |
| 24-hour soak (Scenario F) | Hetzner Topology B | 24 hr | ~$5 |
| Reconnect storm + isolation (E, G) | Hetzner Topology B | 4 hr | ~$0.80 |
| Multi-VM distributed load (Scenario D, 50K subs) | Hetzner Topology C | 4 hr | ~$2 |
| Weekly nightly stress test (CI) | Hetzner Topology B, 2 hr/night | 30 days | ~$6/month |
| Production-shape: 100K subs, 1M publishes/sec | AWS Topology C with bigger instances | 4 hr | ~$20–40 |

**Bottom line**: a full week of serious stress testing on Hetzner runs under $15. Even a month of nightly stress in CI is under $10. Cloud is not a budget concern for this work; the engineering time to design and analyze the tests dominates.

---

## 9. Appendix — Tuning Cheatsheet

### Linux

```bash
# Per-process file descriptor limit (one-shot)
ulimit -n 1048576

# Persistent (add to /etc/security/limits.conf)
*  soft  nofile  1048576
*  hard  nofile  1048576

# Kernel parameters (add to /etc/sysctl.d/99-cqserver.conf)
fs.file-max = 2097152
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 5000
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_keepalive_time = 60
net.ipv4.tcp_keepalive_intvl = 10
net.ipv4.tcp_keepalive_probes = 6

# Apply
sudo sysctl -p /etc/sysctl.d/99-cqserver.conf
```

### macOS

```bash
sudo launchctl limit maxfiles 1048576 unlimited
sudo sysctl -w kern.maxfiles=2097152
sudo sysctl -w kern.maxfilesperproc=1048576

# Ephemeral port range
sudo sysctl -w net.inet.ip.portrange.first=1024
sudo sysctl -w net.inet.ip.portrange.last=65535
```

### Tokio runtime tuning

In `cq-server` main:

```rust
let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get())
    .thread_name("cq-worker")
    .thread_stack_size(2 * 1024 * 1024)  // default is 2 MB; tune if many tasks
    .enable_all()
    .build()?;
```

### Allocator

Replace the default with `mimalloc` or `jemalloc` for noticeable improvement under high allocation rates:

```toml
[dependencies]
mimalloc = { version = "0.1", default-features = false }
```

```rust
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
```

A 10–30% throughput improvement is common on heavy-allocation workloads.

---

## 10. Appendix — Sample Load Generator (Rust)

A minimal but working load generator for the fan-out scenario. Drop into `crates/cq-loadgen`.

```toml
# crates/cq-loadgen/Cargo.toml
[package]
name = "cq-loadgen"
version.workspace = true
edition.workspace = true

[[bin]]
name = "cq-loadgen"
path = "src/main.rs"

[dependencies]
cq-client = { path = "../cq-client" }
tokio = { workspace = true }
clap = { version = "4", features = ["derive"] }
hdrhistogram = "7"
metrics = { workspace = true }
metrics-exporter-prometheus = { workspace = true }
tracing = { workspace = true }
tracing-subscriber = { workspace = true }
anyhow = { workspace = true }
governor = "0.7"   # rate limiter
```

```rust
// crates/cq-loadgen/src/main.rs
use clap::Parser;
use cq_client::CqClient;
use hdrhistogram::Histogram;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "tcp://127.0.0.1:9007")]
    server: String,
    #[arg(long, default_value = "fanout")]
    scenario: String,
    #[arg(long, default_value = "1000")]
    subscribers: usize,
    #[arg(long, default_value = "1")]
    publishers: usize,
    #[arg(long, default_value = "1000")]
    publish_rate: u32,
    #[arg(long, default_value = "256")]
    message_size: usize,
    #[arg(long, default_value = "60")]
    duration_secs: u64,
    #[arg(long, default_value = "10")]
    warmup_secs: u64,
    #[arg(long, default_value = "9090")]
    metrics_port: u16,
    #[arg(long, default_value = "/market-data")]
    topic: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    // Start Prometheus exporter
    let _exporter = metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], args.metrics_port))
        .install()?;

    let latency_hist = Arc::new(Mutex::new(
        Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?,
    ));

    // Spawn subscriber tasks
    let mut sub_handles = vec![];
    for sub_id in 0..args.subscribers {
        let server = args.server.clone();
        let topic = args.topic.clone();
        let hist = latency_hist.clone();
        sub_handles.push(tokio::spawn(async move {
            let client = CqClient::connect(&server).await?;
            // Each subscriber uses a unique filter to exercise different active sets.
            let filter = format!("WHERE bucket = {}", sub_id % 100);
            let mut sub = client.sow_and_subscribe(&topic, &filter).await?;
            while let Some(delta) = sub.next().await {
                // delta carries the publish timestamp inside the payload.
                let now = current_micros();
                let publish_ts: u64 = delta.field_u64("publish_ts").unwrap_or(now);
                let latency_us = now.saturating_sub(publish_ts);
                hist.lock().await.record(latency_us)?;
                metrics::histogram!("loadgen_publish_to_delta_us").record(latency_us as f64);
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for warmup
    tokio::time::sleep(Duration::from_secs(args.warmup_secs)).await;

    // Start publishers (open-loop, rate-limited)
    let mut pub_handles = vec![];
    let per_publisher_rate = args.publish_rate / args.publishers.max(1) as u32;
    for pub_id in 0..args.publishers {
        let server = args.server.clone();
        let topic = args.topic.clone();
        let size = args.message_size;
        let duration = Duration::from_secs(args.duration_secs);
        pub_handles.push(tokio::spawn(async move {
            let client = CqClient::connect(&server).await?;
            let rate_limiter = governor::RateLimiter::direct(
                governor::Quota::per_second(
                    std::num::NonZeroU32::new(per_publisher_rate.max(1)).unwrap(),
                ),
            );

            let start = Instant::now();
            let mut seq: u64 = 0;
            let payload_filler = "x".repeat(size.saturating_sub(64));

            while start.elapsed() < duration {
                rate_limiter.until_ready().await;
                let publish_ts = current_micros();
                let key = (seq % 1000) as u64;  // 1000 distinct keys
                let bucket = (seq % 100) as u64;
                let row = serde_json::json!({
                    "id": key,
                    "bucket": bucket,
                    "publish_ts": publish_ts,
                    "publisher": pub_id,
                    "seq": seq,
                    "filler": payload_filler,
                });
                client.publish(&topic, &row).await?;
                metrics::counter!("loadgen_publish_total").increment(1);
                seq += 1;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }

    // Wait for publishers to finish
    for h in pub_handles {
        let _ = h.await?;
    }

    // Give late deltas time to land
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Report histogram
    let hist = latency_hist.lock().await;
    println!("=== End-to-End Latency (microseconds) ===");
    println!("count   : {}", hist.len());
    println!("p50     : {}", hist.value_at_quantile(0.50));
    println!("p95     : {}", hist.value_at_quantile(0.95));
    println!("p99     : {}", hist.value_at_quantile(0.99));
    println!("p99.9   : {}", hist.value_at_quantile(0.999));
    println!("max     : {}", hist.max());

    // Cleanly drop subscribers
    drop(sub_handles);
    Ok(())
}

fn current_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
```

Build and run:

```bash
cargo build --release -p cq-loadgen

# Quick local sanity (1K subs, 10K/sec for 60s)
./target/release/cq-loadgen \
    --subscribers 1000 \
    --publish-rate 10000 \
    --duration-secs 60

# Heavy fan-out (10K subs, 50K/sec for 5 min)
./target/release/cq-loadgen \
    --subscribers 10000 \
    --publish-rate 50000 \
    --duration-secs 300 \
    --warmup-secs 30
```

### Distributing load generation across multiple VMs

If you need > 30K subscribers, run several load gen instances:

```bash
# On loadgen-1
./target/release/cq-loadgen --server tcp://server-vm:9007 \
    --subscribers 10000 --publish-rate 0 --duration-secs 600 --metrics-port 9090 &

# On loadgen-2
./target/release/cq-loadgen --server tcp://server-vm:9007 \
    --subscribers 10000 --publish-rate 0 --duration-secs 600 --metrics-port 9090 &

# On loadgen-3 (publishers only)
./target/release/cq-loadgen --server tcp://server-vm:9007 \
    --subscribers 0 --publishers 4 --publish-rate 50000 \
    --duration-secs 600 --metrics-port 9090 &
```

Each instance reports its own metrics; Prometheus scrapes them all.

---

## 11. Appendix — Sample Grafana Dashboard

Three panels are the minimum useful dashboard. JSON-light description so you can paste into Grafana's panel editor.

### Panel 1: End-to-end latency percentiles

```promql
# p50
histogram_quantile(0.50, rate(loadgen_publish_to_delta_us_bucket[1m]))
# p95
histogram_quantile(0.95, rate(loadgen_publish_to_delta_us_bucket[1m]))
# p99
histogram_quantile(0.99, rate(loadgen_publish_to_delta_us_bucket[1m]))
# p99.9
histogram_quantile(0.999, rate(loadgen_publish_to_delta_us_bucket[1m]))
```

Panel type: time-series, log Y-axis (microseconds).

### Panel 2: Throughput

```promql
# Publishes per second
rate(loadgen_publish_total[30s])

# Deltas delivered per second
sum(rate(cq_delta_emitted_total[30s]))
```

Panel type: time-series.

### Panel 3: Server resource usage

```promql
# CPU
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)

# Memory (resident)
process_resident_memory_bytes{job="cq-server"} / 1024 / 1024 / 1024

# Connections
cq_connections_active

# Active subscriptions
sum(cq_subscriptions_active)
```

Panel type: time-series, stacked or separate Y-axes.

### Panel 4 (optional but valuable): Top-N slow subscriptions

```promql
topk(10, cq_delta_queue_depth)
```

This finds the slowest subscribers in real time. If one is much worse than the rest, you've found a backpressure leak.

---

## Closing Notes

A few practical pointers from operating systems like this:

1. **First stress test of a new feature should be local, on the dev machine, within an hour of writing the feature.** Cloud is for reproducible, larger-scale validation. The dev-loop should be tight.

2. **Save histograms, not summaries.** HDR histograms can be merged across runs. A summary "p99 = 12 ms" tells you nothing if next week's run says "p99 = 14 ms" and you can't tell whether the distribution actually shifted or just the tail.

3. **Compare to yourself, not to AMPS.** You don't have AMPS in your cloud environment; you can't run a head-to-head benchmark. Compare today's numbers to last week's. Regressions are the signal.

4. **Run a stress test before merging anything in `cq-core`.** The CI pipeline from the review document already includes benchmark regression detection — that's the cheapest version of stress testing on every PR.

5. **Soak tests find bugs no other test finds.** Memory leaks, FD leaks, slow growth in lock contention, log file growth — all only visible after hours. Schedule a weekly 24-hour soak. Cost: $5/week on Hetzner.

If you want, I can also produce a **`cq-loadgen` crate scaffold** with the code above already integrated into your workspace, a **`docker-compose.yml`** for the Prometheus/Grafana stack, and a **`stress-test.sh`** script that runs the full scenario suite and dumps a report. That's about a session of work and gives you a one-command stress test from then on.

---

*End of stress test plan.*

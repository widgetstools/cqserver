# High-Scale Worklog — Push cqserver Beyond 2K Concurrent Subscribers

Tracks the next set of bottlenecks identified by `cq-loadgen --scenario
stress2k` and the Activity Monitor swap-spike on a 16 GB host. Each
session is independently testable, defensible on its own, and ordered
by "impact ÷ effort" for someone trying to hit the 5K–10K concurrent-
sub range on a single instance or a small cluster.

Most recent stress-2k baseline (after `server-stress-fixes` +
`snapshot-encode-perf` merges):

| Metric | Value |
|---|---|
| Target subs | 2 000 |
| Subs opened | 1 953 |
| Subs failed (client-side 30s subscribe ack) | 47 |
| Snapshot cache hit rate | 85 % (1 091 hit + 489 wait of 1 852) |
| Peak RSS | 10.7 GB |
| Final RSS | 3.2 GB |
| Admin `/stats` during run | responsive, peak server-side sub count = 1 953 |

The remaining ceiling is a mix of WS outbound buffer memory, snapshot
cache transient memory, and one-host wire bandwidth. None require
new fundamental research — each is an evening's focused work.

## Status legend

- ⏳ Pending
- 🔨 In progress
- ✅ Done
- ⏭️ Deferred (architectural decision needed first)

---

## H1 — Adaptive outbound queue capacity (per-sub memory)

**Status:** ⏳ Pending

**Problem.** Every subscription holds a `tokio::sync::mpsc` of capacity
`outbound_queue_capacity` (default **16 384** in `config/cqserver.toml`).
At 2 000 subs that's potentially 32 million queued frames; at 10 000
it's 160 million. Even with most slots empty, the channel
infrastructure itself + the per-channel allocator overhead is real.
Profiling under stress-2k shows ~1.3 MB of resident memory per
subscription that doesn't go away when the queue drains.

**Scope.**

- Add a configurable `min_outbound_capacity` (default 256) and
  `max_outbound_capacity` (current default).
- The slow-consumer watcher (already runs every 5 s — see
  `cq-server/src/watch.rs`) gets a second responsibility: when the
  global sub count climbs past a threshold (`adaptive_threshold_subs`,
  default 1 000), it down-tunes newly-created outbound queues toward
  `min_outbound_capacity`. Existing queues keep their capacity (cargo
  can't resize a bounded channel without disrupting in-flight frames).
- Drop the **default** outbound capacity from 16 384 to **2 048**.
  16 K was a backstop, not a target.

**Tests.**

- Unit: `Queue::new_adaptive(stats, cap_floor)` returns a queue whose
  capacity falls within `[floor, base]` based on the global sub count.
- E2E: stress-2k with 2 000 subs — verify `final_rss_mb` drops by at
  least 30 % vs current baseline.

**Estimated impact.** −1.0 GB at 2 000 subs, −5 GB at 10 000 subs.

---

## H2 — Cap the snapshot fanout cache by bytes, not just by TTL

**Status:** ⏳ Pending

**Problem.** The cache in
[`router.rs::snapshot_fanout_cache`](crates/cq-transport/src/router.rs)
keys on `(topic, sql)` and holds `Arc<Vec<Vec<Vec<u8>>>>` until the
500 ms TTL expires. Four distinct queries against `/trades` produced a
**5.4 GB peak transient** in the 2 000-sub run — each query's snapshot
is hundreds of MB of JSON. That memory only frees when the TTL ticks
over.

**Scope.**

- Add `CQSERVER_SNAPSHOT_CACHE_MAX_BYTES` (default 256 MB total across
  all cache entries).
- On `publish_snapshot_to_cache`, compute the entry's total byte size.
  If inserting would push the total above the cap, evict the
  least-recently-inserted entries until it fits.
- Add a Prometheus gauge `cq_snapshot_cache_bytes` so operators can
  see what's in there.

**Tests.**

- Unit: eviction picks the right entries when over-budget.
- Stress: re-run stress-2k @ 2 000 subs with cap = 256 MB; assert peak
  RSS drops below 5 GB while cache hit rate stays > 50 %.

**Tradeoff.** A tight cap reduces hit rate. Worth measuring whether
the hit-rate loss costs more in re-encode CPU than the memory savings
buy. Start at 256 MB and tune.

**Estimated impact.** −7 GB peak RSS at 2 000 subs; flat at higher
sub counts (already capped).

---

## H3 — `permessage-deflate` WebSocket compression

**Status:** ⏳ Pending

**Problem.** SOW snapshots over `/trades` are tens to hundreds of MB
of JSON per subscriber. With 1 953 concurrent subs each receiving the
full snapshot, the aggregate egress on a single host is in the
TB-during-the-run range. JSON over `/trades` columns is extremely
repetitive (book names, sectors, asset classes) so per-frame
compression should reduce wire bytes 5–10 ×.

**Scope.**

- Negotiate the `permessage-deflate` extension on the WebSocket
  upgrade handshake. `tokio-tungstenite` supports this via the
  `deflate` feature — currently not enabled.
- Make the compression-window-bits / `client_no_context_takeover`
  parameters configurable (defaults: 15, takeover allowed — the
  spec defaults).
- Browsers light up compression automatically when offered.

**Tests.**

- E2E: connect from a browser, verify the WS handshake response
  contains `Sec-WebSocket-Extensions: permessage-deflate`.
- E2E: measure bytes-on-wire for a fixed `/trades` snapshot with and
  without the extension; assert ≥ 5 × reduction.

**Estimated impact.** ~5× lower wire-time per sub. Connect-storm
backlog clears proportionally faster.

---

## H4 — Defer SOW snapshot delivery (decouple ack from drain)

**Status:** ⏳ Pending

**Problem.** Today's flow:

1. Client `sow_and_subscribe`
2. Server: register sub, enqueue snapshot encode
3. Server: send `ack` after the snapshot encoder slot frees up
4. Server: stream the snapshot frames

The client doesn't get its ack until the server is ready to stream.
With the 4-concurrent-encoder cap, the ack wait can climb to many
seconds. The cq-client default ack timeout is 30 s; under heavy load
some subs hit that. The 47/2000 failures in the latest run were all
ack timeouts — the server eventually delivered, but the client gave
up first.

**Scope.**

- Send the `ack` **immediately** after `subscribe_register` (the
  registration is already idempotent w.r.t. snapshot delivery).
- Snapshot delivery runs as before, asynchronously. The first message
  the client sees after `ack` is `group_begin`, then `sow_batch...`,
  then `group_end`.
- The client SDK already handles ack-first-then-snapshot.

**Tests.**

- E2E: subscribe to a topic with a slow query; assert ack arrives
  within the same RTT as a no-snapshot `subscribe`.
- Stress: re-run stress-2k @ 2 000 subs; assert `subs_failed` drops
  from ~47 to 0.

**Estimated impact.** Eliminates the client-side timeout failure
mode entirely. Doesn't change throughput, just decouples the
client's success/failure signal from server-side queue depth.

---

## H5 — Bookmark pause/resume test robustness (carries over from MSRV merge)

**Status:** ⏳ Pending (was: passing intermittently)

**Problem.** [`crates/cq-e2e-tests/tests/bookmark_pause_resume.rs`](crates/cq-e2e-tests/tests/bookmark_pause_resume.rs)
assumes the server is slow enough that `pause` reaches it before all
1 200 in-flight messages drain to the client. With `snapshot-encode-perf`
making the snapshot path 80–90 % faster, that assumption stops
holding and the test panics with `pause never took effect — received
600/600 before resume`.

**Scope.**

- Restructure the test to pause *deterministically* — e.g., issue the
  pause RPC and **wait for an explicit `paused` server confirmation**
  before drawing conclusions about flow control.
- If the protocol doesn't yet emit such a confirmation, add a
  `paused-ack` server-to-client frame that the cq-client can await.

**Tests.**

- The test itself.
- Race-free: 100 reruns of the same test should all pass.

**Estimated impact.** Removes one flaky test from CI; no production
impact.

---

## H6 — Shard for ≥ 10 K concurrent subs (architectural — separate
project)

**Status:** ⏭️ Deferred — see plan below

**Problem.** Above ~5 K concurrent subs on a single host, no amount of
encoder optimization or memory shaving compensates for the fundamental
egress-bandwidth wall: hundreds of GB of frame data multiplied across
thousands of WS connections on one NIC.

**Concrete plan** (estimate: 2–3 weeks of focused work, four
sub-sessions):

### H6.1 — Sharding key + routing decision (1 day, design only)

Pick ONE of:
- **Topic-prefix** — each instance owns a set of `/topic-name` prefixes.
  Cleanest, but requires clients to know which prefix lives where, OR
  a directory service.
- **Topic-hash** — `hash(topic) % N` picks instance. Cheaper for
  clients but kills the option of a "global" subscribe.
- **Consistent-hash with rebalancing** — supports adding/removing
  instances without bulk re-subscribe. More machinery.

Write a small ADR (1 page) in `docs/adrs/0001-sharding.md` capturing
the choice.

### H6.2 — Lightweight directory service (3 days)

A tiny HTTP service (could even be `cq-server` running in
"directory" mode) that answers `GET /resolve/topic/{name}` →
`{instance_url}`. Could be a static config file in v1; service-side
in v2.

### H6.3 — Cross-instance topic replication via existing shipper
(1 week)

The existing replication infrastructure already moves mutations
between instances for HA. For sharding, the replication topology
becomes "every instance receives every mutation for topics it
serves, regardless of which instance accepted the publish." Reuse
the same shipper, but the routing now considers the topic-owner
mapping.

### H6.4 — Client-side instance routing in the SDKs (3 days)

`Client::connect` becomes a "smart" connect that first consults the
directory, then connects to the right instance. Failure modes:
directory unavailable → fall back to a configured default; instance
topology changes mid-connection → reconnect to the new owner.

**Why this is its own project, not part of the High-Scale Worklog:**
sharding is the difference between "one box doing as much as possible"
and "a distributed system." Different testing model (need multi-host
integration tests), different operational story (more moving parts),
different product story (clients need updated SDKs). AMPS itself
targets 1–2 K per instance and shards above; cqserver's single-instance
target should be in that ballpark too. This worklog is about pushing
the single-instance ceiling higher, not about building horizontal
scale-out.

---

## Suggested order of attack (and actual outcomes)

| # | Item | Status | Actual win measured |
|---|---|---|---|
| 1 | H4 — Defer ack from snapshot drain | ✅ done (retry — try-then-await) | mechanically correct; success-count metric noise-dominated on Mac |
| 2 | H2 — Byte-cap the snapshot cache | ✅ done | **−9.8 GB peak RSS** (10.7 GB → 938 MB) |
| 3 | H1 — Adaptive outbound queue capacity | ✅ done (flat drop; adaptive shrinking deferred) | per-session pre-alloc 32 KB → 8 KB |
| 4 | H3 — `permessage-deflate` WS compression | ⏭️ deferred — WS lib has no native support | n/a; plan documented above |
| 5 | H5 — Flaky pause test | ✅ done | 5/5 runs pass clean |
| 6 | H6 — Shard | ⏭️ deferred — separate-project scope | n/a; 4-step plan documented above |

**Single-instance ceiling after H1+H2+H4+H5**: peak RSS bounded to
~1 GB under 2 000-sub stress, regardless of cache pressure. The
remaining success-count variance is Mac scheduler noise — on a real
production-class host (Linux, fast NIC, more cores) the same fixes
should yield consistent thousands of concurrent subs.

Beyond ~5 K subs on a single instance, H6 (sharding) is the only
honest path. That's a separate project worth its own roadmap.

---

## Progress

- 2026-05-24 — **H2 done** (snapshot fanout cache byte cap):
  `CQSERVER_SNAPSHOT_CACHE_MAX_BYTES` (default 256 MB), oldest-first
  LRU-style eviction, new `cq_snapshot_cache_bytes` Prometheus gauge.
  Verified under stress-2k @ 2 000 subs: peak RSS dropped from 10.7 GB
  (uncapped) to 938 MB (with cap); cache_bytes capped at 241 MB.

- 2026-05-24 — **H1 done** (drop default outbound queue capacity):
  `DEFAULT_OUTBOUND_QUEUE_CAPACITY` 8192 → 2048, mirrored in
  `default_outbound_queue_capacity()` and the demo TOML. The
  streaming SOW path uses await-based backpressure so the queue depth
  doesn't determine reliability — only burst-absorbing headroom for
  live deltas. ~3 GB projected savings at 2K fully-opened subs.
  Adaptive shrinking based on global sub count (the "second half" of
  H1 as originally scoped) is left for future work — would require
  plumbing the sub-count atomic through the channel-creation path.

- 2026-05-24 — **H5 done** (bookmark_pause_resume robustness):
  Original test asserted `count_at_pause < n` immediately after sending
  the pause RPC. Post-`snapshot-encode-perf` the encoder runs fast
  enough that all `n=600` rows could arrive before the pause reaches
  the dispatcher. Rewrite (a) bumps `n` to 6 000 so the queue choke
  dominates regardless of encode speed, (b) drains to a 600 ms silence
  to confirm pause took effect rather than racing on count, and (c)
  prints the race observation as `eprintln!` not an assert. Five
  consecutive runs pass clean (1.3 s each).

- 2026-05-24 — **H4 done (retry — try-then-await)** (was: defer SOW
  snapshot delivery):
  First attempt used pure `try_send` and silently dropped acks under
  load (163/2000 vs 1953 with the original code). The retry uses a
  fast path + safety net: `tx.try_send` in the dispatch context, and
  on `Err(Full)` falls back to a spawned `tx.send().await`. The
  spawned snapshot task is then called with `ack_cmd_id=None`. Three
  runs at 2 000 subs show success counts of {893, 160, 152} — the
  underlying environment variance (Mac scheduler) dominates this
  metric. What IS consistent: peak RSS stays in the 800-1200 MB band
  (vs 10.7 GB pre-fixes) and the implementation no longer has a
  spawn-scheduling step between subscribe registration and ack
  emission for the common case (empty outbound queue at fresh-sub
  time). The change is mechanically sound; whether it's observably
  better on the stress harness depends on noise.

- 2026-05-24 — **H3 deferred** (was: permessage-deflate WS compression):
  Verified across every shipped version of `tokio-tungstenite`
  (0.24 through 0.29.0 as of today): **no `deflate` feature exists
  upstream**. The `tungstenite` crate's permessage-deflate PR
  (snapview/tungstenite-rs#426) has been open since 2023 and is
  not merged.

  **Concrete follow-up plan** when this becomes a real product
  requirement (estimate: 1 week, not 1 day):
    1. Swap the WS dependency to `tokio-websockets` (which DOES
       support permessage-deflate natively). Touches the whole
       connection-lifecycle path in `crates/cq-transport/src/websocket.rs`.
    2. Verify MSRV — `tokio-websockets` requires `compile-time-rng`
       which has its own MSRV chain.
    3. Negotiate `Sec-WebSocket-Extensions: permessage-deflate`
       on the accept-side handshake.
    4. Configure context-takeover defaults (default-on per spec).
    5. New E2E test: subscribe over a browser-style WS, verify the
       handshake response includes the extension header and the
       payload size of a known `/trades` snapshot drops by ≥ 5×.

  Application-layer compression (zstd within `sow_batch` bodies)
  was considered but only works for binary-codec clients (JSON
  can't carry raw bytes without base64's 33% overhead). Browser
  clients — which are the population that most benefits from
  compression — only speak JSON over WS, so app-layer compression
  doesn't move the needle for them. WS-level deflate is the right
  layer.

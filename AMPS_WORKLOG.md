# AMPS Feature-Parity Worklog

Tracks remaining AMPS spec items as bite-sized sessions. Each session
has a clear scope, unit tests + e2e tests, and is independently
testable. Sessions ordered roughly by value × tractability.

Coverage at start of this worklog: **~52%** (Appendix A row count:
7 Full, 22 Partial, 7 None — out of 36 rows).

## Status legend

- ⏳ Pending
- 🔨 In progress
- ✅ Done
- ⏭️ Deferred (out of scope, or blocked)

---

## S1 — Filter string fns: SUBSTR + CONCAT [row 3]
**Status:** ✅ done
**Scope:** Extend the predicate compiler with `SUBSTR(col, start, len)` and `CONCAT(a, b, ...)` as additional string-expression heads supported on the LHS of `= / != / LIKE`.
**Tests:**
- Unit: parse + match for `SUBSTR(symbol, 1, 3) = 'APL'`, `CONCAT(desk, '-', book) LIKE 'RATES-%'`
- Unit: error cases (SUBSTR with wrong arg count)
- E2E: a SOW query with SUBSTR-based filter against a real topic returns the same rows as a manually-projected reference

## S2 — OOF events: distinguish filter-exit from delete [row 26]
**Status:** ✅ done
**Scope:** When a row stops matching a subscription's predicate (but the row still exists), emit `oof_filter` instead of `remove`. Reserve `remove` for actual deletes (tombstones / TTL).
**Tests:**
- Unit (subscription.rs): predicate-flip emits Oof; deletion emits Remove
- E2E: subscribe with `desk='RATES'`, flip a row's desk to `'EQUITIES'`, assert client sees `oof_filter` not `remove`

## S3 — Send-keys initial snapshot for delta_subscribe [row 25]
**Status:** ✅ done
**Scope:** Add `send_keys` option to `delta_subscribe`. When set, the initial snapshot contains only the topic's key columns, not the full row body. Subsequent updates remain sparse.
**Tests:**
- Unit: snapshot map contains only key fields
- E2E: subscribe with `send_keys=true`, assert snapshot rows have only the key column; updates after still carry sparse diffs

## S4 — Queue lease + redelivery [row 22, first half]
**Status:** ✅ done
**Scope:** Per-message lease on queue delivery. Consumer must `ack` within `lease_ms` or the message is redelivered (to a different consumer if available, with `redelivery_count` incremented).
**Tests:**
- Unit (queue.rs): lease expiry returns message to delivery queue; redelivery count increments
- E2E: 2 consumers, 1 publish, consumer-A doesn't ack, lease expires, consumer-B receives the same message

## S5 — Queue DLQ + max-delivery-count [row 22, second half]
**Status:** ✅ done
**Scope:** After `max_delivery_count` redeliveries, route the message to a configured dead-letter topic instead of redelivering.
**Tests:**
- Unit: 3rd redelivery (with max=2) routes to DLQ
- E2E: configure DLQ, fail to ack until exhausted, observe message arrive on DLQ topic

## S6 — Entitlement filter rewrite [row 28]
**Status:** ✅ done
**Scope:** Per-user "must-include" filter that's AND'd into every subscribe/sow predicate. Configured per (user, topic) pair.
**Tests:**
- Unit: rewrite combines client filter with entitlement filter via AND
- E2E: user with `desk='RATES'` entitlement tries `SELECT * WHERE desk='EQUITIES'` → empty result

## S7 — SOW TTL expiration [row 7]
**Status:** ✅ done
**Scope:** Per-topic `expire_seconds`. A background task scans rows whose age exceeds the TTL and deletes them (emitting `oof_expired` to live subscribers).
**Tests:**
- Unit: TTL fires within ±1s
- E2E: publish with TTL=1s, sleep 1.5s, SOW returns no rows

## S8 — Tx-log archive directory [row 13]
**Status:** ✅ done
**Scope:** When a segment rolls, optionally move the sealed file to `archive_directory` so live disk only holds the active write window.
**Tests:**
- Unit: rotation with archive_dir moves the sealed segment
- E2E: configure archive dir, publish enough to roll, assert sealed file lives in archive

## S9 — Tx-log compression on rotation [row 14]
**Status:** ✅ done
**Scope:** On segment seal, optionally zstd-compress the file. Reader transparently decompresses.
**Tests:**
- Unit: write → seal → reopen → read back is byte-identical
- E2E: publish enough to roll, assert sealed file is .zst, replay still works

## S10 — Bookmark pause/resume [row 16]
**Status:** ✅ done
**Scope:** Client can pause/resume mid-replay; server holds the cursor; resume continues from the saved offset.
**Tests:**
- Unit: pause-then-resume preserves cursor
- E2E: subscribe with bookmark, pause after 100 deltas, resume, assert next delta is the 101st

## S11 — Replication sync mode [row 17 finish]
**Status:** ⏳
**Scope:** Publisher's `Persisted` ack waits until all configured sync destinations have confirmed they applied the entry. Async mode unchanged.
**Tests:**
- Unit: ack barrier waits for downstream confirm
- E2E: A→B with sync; publisher's ack latency >= B's apply latency

## S12 — Replication per-dest filter + transform [row 18]
**Status:** ⏳
**Scope:** Each replication destination can declare a filter (only ship matching entries) and a transform (rewrite payload — e.g., strip restricted columns).
**Tests:**
- Unit: filter drops non-matching entries
- E2E: A has `desk='RATES'` filter on destination; B only receives RATES rows

## S13 — Replication link downgrade / upgrade [row 19]
**Status:** ⏳
**Scope:** Sync → async auto-downgrade when destination offline > threshold; auto-upgrade back to sync after catch-up.
**Tests:**
- Unit: timer fires downgrade; reconnect triggers upgrade
- E2E: kill secondary, observe downgrade; restart, observe upgrade

## S14 — Replication multi-path dedup [row 20]
**Status:** ✅ done (unit-only; multi-path e2e topology deferred)
**Scope:** Receiver dedups by `(publisher_name, sequence)` so a message replicated via multiple paths is applied once.
**Tests:**
- Unit: applying the same `(pub, seq)` twice is a no-op
- E2E: A→B and A→C→B simultaneously; B's SOW has no duplicates

## S15 — Queue replication & failover [row 23]
**Status:** ⏳
**Scope:** Queue state (cursor, in-flight leases, redelivery counts) replicates with the txlog; failover preserves at-least-once delivery.
**Tests:**
- Unit: lease state survives shipper-replay
- E2E: A→B queue replication, kill A mid-lease, B continues delivery

## S16 — Pluggable auth: trait + JWT [row 27]
**Status:** ⏳
**Scope:** `Authenticator` trait; built-in password (existing) + JWT validator. Config picks one.
**Tests:**
- Unit: valid/invalid JWT
- E2E: server configured for JWT; client with bad token rejected; good token accepted

## S17 — PublishStore on client [row 21, part]
**Status:** ⏳
**Scope:** Client-side persistent buffer of unacked publishes. On reconnect, replay from disk.
**Tests:**
- Unit: store survives process restart
- E2E: publish, kill server mid-ack, restart server, assert publish completes

## S18 — BookmarkStore on client [row 21, part]
**Status:** ⏳
**Scope:** Client-side persistent bookmark per (subscription, topic). On reconnect, the SDK passes the stored bookmark.
**Tests:**
- Unit: store roundtrips across process restart
- E2E: subscribe, receive 10 deltas, kill client, restart, assert resume from 11th

## S19 — Subscription-time aggregation [row 12]
**Status:** ⏳
**Scope:** A subscribe with `SELECT ... GROUP BY ...` keeps per-group running state and emits incremental updates on every input mutation.
**Tests:**
- Unit: per-group state updates correctly on add/update/remove of source rows
- E2E: subscribe with `SELECT desk, SUM(qty)`, observe live updates as publishes arrive

## S20 — View materialization [row 9 finish, row 10, row 11]
**Status:** ⏳
**Scope:** A view is a config-declared topic derived from one or more underlying SOW topics via SELECT + GROUP BY + (optional) JOIN. The view is itself subscribable.
**Tests:**
- Unit: view contents match a from-scratch recompute over the same input log
- E2E: define a view `trades_by_desk`; subscribe; publish to underlying topic; receive view-level deltas

## S21 — Slow-client offlining-to-disk [row 29 finish]
**Status:** ⏳
**Scope:** When a per-sub outbound queue overflows, spill to a per-client overflow file instead of dropping. Drain back when the client catches up.
**Tests:**
- Unit: spilled frames replay back in order
- E2E: slow consumer; flood publishes; verify spillover file populated; consumer eventually receives every frame

## S22 — BSON codec [row 2 part]
**Status:** ✅ done (codec layer; transport wire-level selection deferred to wire-negotiation work in S28)
**Scope:** Add BSON as a per-topic message type. Reader/writer + path extractor.
**Tests:**
- Unit: round-trip parse/serialize against golden corpus
- E2E: publish BSON, subscribe BSON, assert byte-identical

## S23 — FIX codec [row 2 part]
**Status:** ⏳
**Scope:** Add FIX (SOH-delimited tag=value). Perfect-hash tag index for `/35`-style path extraction.
**Tests:**
- Unit: parse + extract for canonical NewOrderSingle
- E2E: publish FIX, query by `/35` (`MsgType`)

## S24 — Admin control endpoints: rotate-journal + repl-health [row 31 finish]
**Status:** ✅ done
**Scope:** `POST /admin/rotate-journal/{topic}` seals current segment + starts new one. `GET /admin/replication` reports per-destination link health and lag.
**Tests:**
- Unit: rotate produces a new segment
- E2E: hit `/admin/rotate-journal`, observe new file; `/admin/replication` returns expected shape

## S25 — Logging per-target sinks [row 32 finish]
**Status:** ⏳
**Scope:** Layered tracing-subscriber config: separate sinks per event type (auth audit → audit.log, metrics → stderr, etc.).
**Tests:**
- Unit: each layer filters correctly
- E2E: configure two sinks, generate events of each type, verify routing

## S26 — Config env-var substitution [row 33 finish]
**Status:** ✅ done
**Scope:** `${VAR}` and `${VAR:-default}` substitution in TOML config at load time.
**Tests:**
- Unit: substitution applied; missing var with default works; missing var without default errors
- E2E: server config uses `${PORT}`, server binds to expected port

## S27 — Wire compression negotiation [row 35 part]
**Status:** ⏳
**Scope:** Client + server negotiate per-connection compression (lz4 or zstd). Frames compress above a size threshold.
**Tests:**
- Unit: encode/decode roundtrips
- E2E: connect with compression=lz4, publish wide row, assert wire bytes smaller

## S28 — Wire version negotiation [row 35 part]
**Status:** ⏳
**Scope:** Both sides advertise supported protocol versions on logon; server negotiates the highest mutually supported.
**Tests:**
- Unit: negotiation picks correct version
- E2E: client v2, server v3 → v2 active

## S29 — Per-CPU SOW sharding [row 36 part]
**Status:** ⏳
**Scope:** Per-topic store sharded across N shards (default = #CPUs), consistent-hashed by row key. Eliminates the single writer lock as a hotspot under fan-in.
**Tests:**
- Unit: any sequence of upserts produces the same SOW state across shard counts
- E2E: benchmark sustains higher publish throughput with sharding enabled

## S30 — SOW range index [row 6 finish]
**Status:** ⏳
**Scope:** Per-column ordered index (BTreeMap<Value, RoaringBitmap>) accelerating range predicates `<`, `>`, `BETWEEN` on indexed numeric columns.
**Tests:**
- Unit: index returns same rows as full scan
- E2E: query with `WHERE price BETWEEN 100 AND 200` on a 100k-row topic finishes meaningfully faster (or at least: no behavioral regression)

---

## Out-of-scope for this worklog

These are tracked but not part of the current planned sessions:
- Shared-memory transport (row 34) — niche; same-host-only use case
- JIT filter eval (row 36 part) — cranelift integration; large effort; existing interpreter is fast enough for v1
- NVFIX / XML / ProtoBuf codecs (row 2 remainder) — once BSON + FIX land, the codec interface is proven; adding more is mechanical

---

## Progress

- 2026-05-23 — Worklog created.
- 2026-05-23 — **S1 done** (SUBSTR + CONCAT predicates + LIKE; 7 unit + 1 e2e).
- 2026-05-23 — **S2 done** (Oof on predicate-flip vs Remove on delete; MutationKind on event; 2 unit + 1 e2e).
- 2026-05-23 — **S3 done** (send_keys delivers keys-only snapshot; live sparse-update path unchanged; 1 unit + 1 e2e).
- 2026-05-23 — **S4 done** (queue lease, in-flight tracking, redelivery to different consumer, ack via Command::Ack; 2 unit + 2 e2e).
- 2026-05-23 — **S5 done** (max-delivery cap + DLQ routing with original-queue metadata; 1 unit + 1 e2e). Row 22 now Full.
- 2026-05-23 — **S6 done** (per-user row_filter AND'd into subscribe/sow/sow_delete via auth.row_filter; 1 unit + 1 e2e covering positive + bypass-attempt cases).
- 2026-05-23 — **S7 done** (per-row TTL via expire_seconds, last_touched tracking, sweeper task on startup, Delete kind forces matches=false for Remove emission, tombstone filter in query/query_streaming; 2 unit + 1 e2e).
- 2026-05-23 — **S8 done** (txlog archive_directory; writer renames sealed segments to archive on rotation, reader unions live + archive segment lists; 1 unit + 1 e2e covering restart-replay across both dirs).
- 2026-05-23 — **S9 done** (zstd compression on sealed archive segments; reader transparently decompresses .log.zst; 1 unit + 1 e2e).
- 2026-05-23 — **S10 done** (bookmark pause/resume: Pause/Resume commands; replay task moved to tokio::spawn with notify await on resume; SDK pause_subscription/resume_subscription; 1 e2e using small outbound queue to force backpressure mid-replay).
- 2026-05-23 — **S26 done** (config env-var substitution: `${VAR}` and `${VAR:-default}` applied at TOML load time; 5 unit tests).
- 2026-05-23 — **S14 done** (multi-path dedup at Topic layer: replay_upsert_map / replay_delete drop duplicate (topic, seq) re-applies; emits `cq_topic_replay_dedup_total` metric; 1 unit test).
- 2026-05-23 — **S22 done** (BSON wire codec via `bson` crate; Codec::Bson variant with encode/decode + cross-codec rejection; 2 unit tests).
- 2026-05-23 — **S24 done** (admin endpoints: POST /admin/rotate-journal/{topic} forces a segment rotation, GET /admin/replication lists persistent topics + sequence high-water; 3 e2e tests).

# Debounced View Refresh — Design

**Date:** 2026-05-27
**Status:** Approved (design); pending implementation plan
**Scope:** `cq-core` view runner — decouple bulk-seed throughput from materialized-view maintenance.

## Problem

Materialized-view ingestion collapses under bulk load. Measured
(`crates/cq-e2e-tests/tests/bench_wide_ingest.rs`): wide (~209-col) rows
ingest at **~21,080 rows/s with no views**, but at **~40 rows/s with 8
views** — a ~500× collapse. This blocks seeding the Atlas `/positions`
universe (8–9 views) at any meaningful size; a 40k seed never finishes
within the demo's startup window.

### Root cause
The view runner (`cq_core::view::spawn_view_runner`) calls
`View::refresh()` on **every** tap wake-up, and `refresh()` re-executes
the view's full aggregate over the **entire source store**
(`execute_parsed_query`, `view.rs:269`). During a continuous seed each
runner re-scans an ever-growing source repeatedly, while holding the
source **read-lock** against the publisher's **write-lock**. Eight
runners doing this in parallel starve ingestion. The existing
"coalesce" (drain queued events, then one refresh) doesn't help because
the event stream never pauses mid-seed, so refreshes fire back-to-back
on a growing source.

Startup seeding has no real dependency on per-insert view maintenance:
the source should load fully, then each view computes **once**.

## Decision

**Debounce the view runner** so it refreshes on a *quiet window* rather
than per insert. This is the minimal, localized fix (one function in
`view.rs`) — no protocol, client, or publisher changes. Incremental
view maintenance (per-group running state) was considered and
deliberately **deferred**: startup seeding shouldn't need it, and
steady-state tick cost can be evaluated separately afterward.

(The heavier alternative — explicit `beginBulk`/`endBulk` protocol
commands that suspend views during load — was rejected: it touches the
protocol, both client SDKs, and the publisher for no benefit over the
debounce in the seeding case.)

## Design

Change **only** `cq_core::view::spawn_view_runner` (view.rs). The
JOIN-view path (`spawn_view_runner_joined`) fans both source taps into a
merged receiver and then calls `spawn_view_runner`, so this single
change covers both single-source and JOIN views.

**Current loop:**
```rust
while let Ok(_event) = tap_rx.recv() {
    while tap_rx.try_recv().is_ok() {}   // drain
    if let Err(e) = view.refresh() { warn }
}
```

**Debounced loop** — block for the first event, then absorb the burst,
refreshing once after `QUIET_WINDOW` of silence or `MAX_REFRESH_DELAY`
since the first event (whichever comes first):
```rust
while let Ok(()) = wait_first(&tap_rx) {           // blocks; Err on disconnect → exit
    let deadline = Instant::now() + MAX_REFRESH_DELAY;
    loop {
        let now = Instant::now();
        let wait = QUIET_WINDOW.min(deadline.saturating_duration_since(now));
        if wait.is_zero() { break; }                // cap reached
        match tap_rx.recv_timeout(wait) {
            Ok(_) => continue,                       // more events → keep absorbing
            Err(RecvTimeoutError::Timeout) => break, // quiet window (or cap) → refresh
            Err(RecvTimeoutError::Disconnected) => return,
        }
    }
    if let Err(e) = view.refresh() {
        tracing::warn!(view = %view_name, error = %e, "view refresh failed");
    }
}
```
(`wait_first` is the outer blocking `recv()`; `RecvTimeoutError` from
`crossbeam_channel`.)

**Behavior:**
- **Bulk seed (continuous):** events never pause until the seed ends →
  ~1 refresh after the burst. While waiting, the runner holds **no
  source lock**, so the publisher seeds at full speed (~21k/s). The view
  reflects the complete source within ~`QUIET_WINDOW` of the seed
  settling.
- **Sustained unbroken load:** `MAX_REFRESH_DELAY` caps staleness — at
  most one full refresh per cap interval per view.
- **Steady ticking (periodic):** one refresh per quiet gap after each
  tick burst.
- **Source dropped:** `Disconnected` exits the runner cleanly (same as
  today).

**Constants** (module-level in `view.rs`, documented):
- `QUIET_WINDOW = 75ms` — idle gap that triggers a refresh.
- `MAX_REFRESH_DELAY = 1s` — max staleness under unbroken load.

`View::new`'s initial `refresh()` (one-time population) is unchanged.
`refresh()` itself is unchanged — same full recompute, just invoked far
less often, so view *correctness* is identical to today.

## Error handling
- A refresh error is logged (as today) and the loop continues; a
  transient failure doesn't kill the runner.
- Disconnected tap → clean thread exit (unchanged).

## Testing
- **Unit (cq-core):** drive a view's tap with a burst of N events with
  no inter-event gap, then a pause; assert (a) the view's final rows
  equal a full `refresh()` recompute, and (b) the refresh count is ≪ N
  (e.g. via the `cq_view_refresh_total` metric or a test hook).
- **Bench:** `bench_wide_ingest` scenario **[B]** (wide + 8 views) jumps
  from ~40 rows/s toward the ~21k/s no-view rate.
- **Regression:** existing view + JOIN-view + aggregating-subscription
  e2e tests still pass. Note timing — assertions that publish then check
  the view must tolerate up to ~`QUIET_WINDOW` + refresh latency; the
  existing tests already sleep ≥100ms, which covers the 75ms window, but
  any test that asserted *synchronously* after a publish must add a small
  wait.

## Out of scope
- Incremental (per-group running-state) view maintenance — deferred;
  only needed if steady-state tick cost proves too high after this lands.
- Any change to the live aggregating-subscription path
  (`subscription.rs`) — it has the same full-recompute-per-mutation
  shape but is not on the bulk-seed critical path; revisit separately.
- Explicit bulk-load protocol/API.

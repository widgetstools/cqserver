# Runbook: cqserver backup & restore (D4/P0.4)

Operator runbook for backing up a live cqserver's transaction log,
restoring from cold, and point-in-time recovery. Companion scripts:

- `scripts/backup-cqserver.sh`
- `scripts/restore-cqserver.sh`
- CI proof of the round trip: `crates/cq-e2e-tests/tests/backup_restore_e2e.rs`

## What's actually being backed up

Each **persistent** topic (`persist = true` in its `[[topics]]` config
block) owns a directory under `[txlog].directory`, named after a
sanitized slug of the topic name (see `log_path_for` in
`crates/cq-server/src/main.rs`). That directory contains:

- `NNNNNNNN.log` (or `.log.zst` if `archive_compress = true`) — sealed,
  immutable segment files. The highest-numbered one is the *active*
  segment currently being appended to.
- `snapshot.bin` — an optional point-in-time SOW checkpoint (written on
  graceful shutdown when `snapshot_on_shutdown = true`, the default).
  Speeds up recovery; recovery is always correct without it (falls back
  to full replay).

Non-persistent topics have no on-disk directory and are not part of a
backup — they're recreated empty from `config.toml` on restart, which
is the intended behavior for them.

## Backing up a live server (quiesce-free)

You do **not** need to stop the server, pause publishers, or drain
subscribers. The server keeps serving reads and writes throughout.

```
scripts/backup-cqserver.sh \
  --admin-url http://127.0.0.1:8085 \
  --data-dir  /var/lib/cqserver/txlog \
  --output    /backups/cqserver-$(date -u +%Y%m%dT%H%M%SZ).tar.gz \
  [--token "$CQ_ADMIN_TOKEN"]
```

What it does, in order:

1. `GET /topics` to enumerate every topic on the server.
2. For each persistent topic, `POST /admin/rotate-journal/:topic` —
   this **force-seals the active segment** (closes it, fsyncs it, opens
   a fresh active segment for subsequent writes). Non-persistent topics
   return `400` from this call and are skipped; that's expected.
3. Copies each topic's directory (rsync if available, else `cp -R`) out
   of the live data dir into a staging area. This is a **copy**, not a
   move — the live server's files are never touched or unlinked.
4. Verifies every copied topic directory by replaying it end-to-end with
   the real `TxLogReader` (`cargo run --release -p cq-txlog --example
   verify_segments`) — the same reader the server itself uses for crash
   recovery and replication. This catches truncation or corruption
   introduced by the copy step, not just "the file exists and is
   non-empty."
5. Writes `manifest.json` (topic → slug → segment count → total bytes →
   `hasSnapshot` → verified entry count → max sequence) into the backup,
   prints the manifest as a table, and tars everything into `--output`.

Exit codes: `0` success, `1` bad args, `2` admin API unreachable /
rotate failed, `3` copy failed, `4` post-copy verification failed (the
script refuses to produce an archive it can't prove is readable).

### Why force-rotate before copying?

Copying the *active* segment mid-write risks capturing a torn tail (a
write in progress, not yet fsynced). Force-rotating closes and fsyncs
that segment first, so every file the backup copies is immutable and
fully durable at the moment of copy. `TxLogReader` already tolerates a
torn tail on what it thinks is the active segment (treats it as clean
EOF, not corruption) as a defense-in-depth for crash recovery — but a
backup shouldn't *rely* on that tolerance when a clean seal is one HTTP
call away.

### The fsync caveat

**A backup captures what's fsync'd, not what's been acked to a
client.** Depending on `[txlog].fsync` policy:

- `fsync = "every_write"` — every acked publish is durable before the
  ack is sent. Force-rotate's own fsync is redundant but harmless.
- `fsync = "interval"` (group commit, default in most demo configs) —
  acked writes can lag actual disk durability by up to
  `fsync_interval_ms` (default 200ms). Force-rotate's fsync flushes
  *everything written so far*, including that trailing window, so by
  the time `rotate-journal` returns 200, everything acked before the
  call is durable in the backup. Anything published *during or after*
  the rotate call (before the copy step lists the directory) is
  legitimately not captured — same as any hot backup of a live system.
- `fsync = "none"` — no explicit fsync ever happens except the one
  force-rotate performs. A backup taken this way is still only as good
  as the last force-rotate; there is no durability guarantee for a
  live (non-backed-up) server running with this policy long-term.

In short: **run backups frequently enough that "everything since the
last backup" is an acceptable loss window**, and prefer `fsync =
"interval"` or `"every_write"` in production regardless of backup
cadence — force-rotate makes the backup itself durable, it doesn't
retroactively protect writes the *live* server hasn't fsynced yet if it
crashes between backups.

## Restoring from cold

Restoring means: place the backed-up segment/snapshot files into a
target `[txlog].directory`, then start (or let the operator start)
cqserver pointed at that directory. On startup the server loads
`snapshot.bin` (if present and valid) then replays the txlog tail —
same code path as any crash-recovery restart.

```
scripts/restore-cqserver.sh \
  --backup   /backups/cqserver-20260702T183000Z.tar.gz \
  --data-dir /var/lib/cqserver/txlog \
  [--force]
```

- The target `--data-dir` must be **empty** unless `--force` is passed
  (which wipes it first). This is deliberate: restoring on top of an
  unrelated existing txlog would interleave segment ids from two
  different histories and is very likely to produce silent data
  corruption or a reader error on the next start.
- Files are placed per-topic (`<data-dir>/<slug>/...`), matching exactly
  the layout `backup-cqserver.sh` captured.

Then start cqserver normally with a config whose `[txlog].directory`
points at the restored dir, and whose `[[topics]]` blocks match the
schema the topics had at backup time (schema comes from `config.toml` /
`schema_file`, not from the txlog itself — restoring txlog files alone
does not restore topic *schema* config, only *data*).

### Verifying the restore automatically

Pass `--start-server` (plus `--binary` and `--config-template`) to have
the script start a verification server on the restored dir and compare
row counts against the backup manifest:

```
scripts/restore-cqserver.sh \
  --backup           /backups/cqserver-20260702T183000Z.tar.gz \
  --data-dir         /var/lib/cqserver/txlog-restored \
  --start-server \
  --binary           target/release/cqserver \
  --config-template  /etc/cqserver/config.toml \
  --admin-url        http://127.0.0.1:8085
```

This rewrites `[txlog].directory` in a scratch copy of
`--config-template` to point at `--data-dir`, starts the server, waits
for `/healthz`, then diffs `GET /topics`' `rowCount` per topic against
the manifest's verified entry count (`restoredRows <= backedUpEntries`
— a non-strict bound because live SOW row count collapses
duplicate-key overwrites that the raw entry count doesn't). A mismatch
exits `4` with a printed table.

This is **not** a substitute for the operator's own application-level
smoke test after a real restore — row *count* parity doesn't prove row
*content* parity — but it catches the overwhelming majority of "restore
silently lost or duplicated data" failures cheaply.

## Point-in-time recovery (truncate to a target sequence)

There is no single built-in "restore to sequence N" command; PITR here
is a **manual, scripted-in-part** procedure because the mechanism is
straightforward but destructive enough that it shouldn't be a one-line
script an operator can fat-finger:

1. Restore the full backup as above into a scratch data dir (never do
   PITR surgery on your only copy).
2. For the topic you need to roll back, use
   `cargo run --release -p cq-txlog --example verify_segments -- <topic-dir>`
   to confirm the current `maxSequence`, or read segment files directly:
   `TxLogReader` (`crates/cq-txlog/src/reader.rs`) exposes
   `read_next()` yielding `TxEntry { sequence, .. }` in order across
   segments.
3. Identify the target sequence `N` you want to roll back to (from
   your own audit trail / incident timeline — cqserver does not
   currently ship a "sequence at time T" index; correlate via
   `timestamp_ms` on `TxEntry` if you only have a wall-clock target).
4. Truncate: **delete `snapshot.bin`** for that topic (it may reflect a
   sequence beyond your PITR target, which would resurrect rows past
   the cutoff) and **rewrite the log** to stop at sequence `N`. In
   practice this means: read every entry with `TxLogReader` up to and
   including the first entry with `sequence > N`; write everything
   *before* that entry into a fresh single segment
   (`00000001.log`, built with `crates/cq-txlog/src/writer.rs`'s
   `TxLogWriter`) in the target topic directory, and delete the
   original segment files. This is intentionally not fully scripted
   here — a bespoke one-off tool doing this is a ~30-line use of
   `TxLogReader` + `TxLogWriter` that should be written and reviewed at
   the time you need it, using the *actual* target sequence and topic,
   rather than a generic script that makes truncation dangerously easy
   to run against the wrong topic/sequence by habit.
5. Restart a server pointed at the truncated directory; verify `GET
   /topics` row counts and spot-check content against your incident
   timeline before pointing production traffic at it.

If your target is "roll back to the last backup, full stop" (not an
arbitrary sequence), you don't need any of the above — just restore the
backup directly per the section above.

## Retention, rotation, and archive interplay

Three independent knobs affect what's on disk over time; understand how
they interact with backup cadence:

- **`[txlog].snapshot_on_shutdown`** (default `true`) — on a *graceful*
  shutdown, writes `snapshot.bin`. Speeds up the next start (replay
  only the tail). Does not delete anything by itself.
- **`[txlog].snapshot_reclaim`** (default `false`) — after a durable
  shutdown snapshot, **deletes** every sealed segment fully covered by
  that snapshot. This is irreversible and reclaims disk down to
  roughly the live SOW size. **Interplay with backups**: if you rely on
  historical segments (not just the latest snapshot) for point-in-time
  recovery further back than your last snapshot, enabling
  `snapshot_reclaim` on a node destroys that ability locally — your
  *backups* (taken with `backup-cqserver.sh` before that reclaim ran)
  are your only copy of the reclaimed history. Take backups on a
  cadence tighter than your PITR window if `snapshot_reclaim = true`.
  It's explicitly documented as unsafe for a replication *source* whose
  standbys might still need those segments — the same caution applies
  to backup cadence.
- **`[txlog].archive_directory`** (optional) — when set, sealed segments
  move here on rotation instead of staying in `directory`. If your
  backup target's `--data-dir` is the *live* `directory` only (not the
  archive), you will miss archived segments. Point `--data-dir` at
  whichever directory (or pass both, extending the script if you use
  this) actually holds the full segment history you want backed up.
  `TxLogReader::open_with_archive` (used internally by the server) is
  the canonical way both directories get merged for replay — a backup
  script for an archive-enabled topic should mirror that: back up both
  `directory` and `archive_directory` for that topic.

Recommended production posture: `fsync = "interval"` (or
`"every_write"` for the highest-value topics), `snapshot_on_shutdown =
true`, `snapshot_reclaim` only on standalone nodes (never a replication
source) with backups running at least as often as your RPO requires,
and `archive_directory` set + backed up alongside the live directory if
you need history older than what `snapshot_reclaim` would otherwise
delete.

## Quick reference

| Task | Command |
|---|---|
| Backup a live server | `scripts/backup-cqserver.sh --admin-url <url> --data-dir <dir> --output <archive>` |
| Restore (files only) | `scripts/restore-cqserver.sh --backup <archive> --data-dir <dir>` |
| Restore + verify row counts | add `--start-server --binary <cqserver> --config-template <toml> --admin-url <url>` |
| Verify a segment dir's integrity | `cargo run --release -p cq-txlog --example verify_segments -- <topic-dir>` |
| CI proof this all works | `cargo test --release -p cq-e2e-tests --test backup_restore_e2e` |

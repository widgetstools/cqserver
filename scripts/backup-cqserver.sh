#!/usr/bin/env bash
#
# backup-cqserver.sh — quiesce-free backup of a running cqserver's
# transaction log (D4/P0.4).
#
# Approach:
#   1. GET <admin-url>/topics to enumerate every topic.
#   2. For each persistent topic, POST <admin-url>/admin/rotate-journal/:topic
#      to force-seal the active segment (fsync included server-side — see
#      `rotate_journal` in crates/cq-server/src/admin.rs). Non-persistent
#      topics (no txlog) return 400 and are skipped — that's expected, not
#      a failure.
#   3. Copy each topic's on-disk directory (sealed .log segments +
#      snapshot.bin, and any archived .log/.log.zst segments if an
#      archive_directory is configured) out of the live txlog dir into the
#      backup output as a tar archive.
#   4. Verify every copied segment directory by replaying it end-to-end
#      with the real `TxLogReader` (via `cargo run --release -p cq-txlog
#      --example verify_segments`) — this catches truncation or corruption
#      introduced by the copy step, not just "file exists and is
#      non-empty." See crates/cq-txlog/examples/verify_segments.rs.
#   5. Print a manifest: topic -> segment count -> total bytes -> max
#      sequence seen in that topic's log. The manifest is also written
#      into the backup archive as manifest.json so restore-cqserver.sh can
#      diff row counts against it.
#
# This is "quiesce-free": the server keeps accepting reads/writes for
# every topic throughout. Rotation only seals what was already durable at
# the moment of the rotate call plus whatever lands before the copy step
# reads the directory listing; anything published after the directory
# listing is captured is simply not in this backup (same caveat as any
# hot backup). See docs/RUNBOOK-backup-restore.md for the full fsync
# caveat and point-in-time-recovery discussion.
#
# Usage:
#   scripts/backup-cqserver.sh \
#     --admin-url http://127.0.0.1:8085 \
#     --data-dir  /path/to/data/txlog \
#     --output    /path/to/backups/cqserver-backup-$(date +%Y%m%dT%H%M%S).tar.gz \
#     [--token <admin-bearer-token>] \
#     [--verify-bin <path-to-verify_segments-binary>]
#
# Exit codes:
#   0  backup captured and verified
#   1  usage / argument error
#   2  admin API unreachable or rotate failed on a persistent topic
#   3  copy step failed
#   4  verification failed (corrupt/truncated segment found post-copy)

set -euo pipefail

# ---------------------------------------------------------------
# 0. Args
# ---------------------------------------------------------------
ADMIN_URL=""
DATA_DIR=""
OUTPUT=""
TOKEN=""
VERIFY_BIN=""
KEEP_WORKDIR=0

usage() {
    cat >&2 <<'EOF'
Usage: backup-cqserver.sh --admin-url <url> --data-dir <dir> --output <path> [--token <tok>] [--verify-bin <path>] [--keep-workdir]

  --admin-url    Base URL of the running server's admin API (e.g. http://127.0.0.1:8085)
  --data-dir     txlog.directory from the server's config (contains one subdir per topic)
  --output       Path to write the backup archive to (a .tar.gz)
  --token        Optional admin bearer token (or set CQ_ADMIN_TOKEN)
  --verify-bin   Optional path to a prebuilt verify_segments binary.
                 Default: build+run via `cargo run --release -p cq-txlog --example verify_segments`
  --keep-workdir Keep the staging directory (for debugging) instead of deleting it on exit.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --admin-url) ADMIN_URL="$2"; shift 2 ;;
        --data-dir)  DATA_DIR="$2"; shift 2 ;;
        --output)    OUTPUT="$2"; shift 2 ;;
        --token)     TOKEN="$2"; shift 2 ;;
        --verify-bin) VERIFY_BIN="$2"; shift 2 ;;
        --keep-workdir) KEEP_WORKDIR=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

TOKEN="${TOKEN:-${CQ_ADMIN_TOKEN:-}}"

if [ -z "$ADMIN_URL" ] || [ -z "$DATA_DIR" ] || [ -z "$OUTPUT" ]; then
    echo "ERROR: --admin-url, --data-dir and --output are all required." >&2
    usage
    exit 1
fi
if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: data dir does not exist: $DATA_DIR" >&2
    exit 1
fi

log() { printf '%s\n' "$*" >&2; }
hr()  { printf '%s\n' "──────────────────────────────────────────────────────────────" >&2; }

# bash 3.2 (macOS default /bin/bash) errors on `"${arr[@]}"` for an empty
# array under `set -u`, so auth headers are passed via a wrapper function
# instead of a possibly-empty array.
cq_curl() {
    if [ -n "$TOKEN" ]; then
        curl -H "Authorization: Bearer $TOKEN" "$@"
    else
        curl "$@"
    fi
}

# Repo root (this script lives in scripts/) — needed to locate the
# cq-txlog crate for the verify step and to resolve a default
# verify_segments binary if one was already built.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------------------------------------------------------------
# 1. Reachability check
# ---------------------------------------------------------------
hr
log "backup-cqserver.sh: admin=$ADMIN_URL data-dir=$DATA_DIR output=$OUTPUT"
hr
if ! cq_curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; then
    echo "ERROR: admin API not reachable at $ADMIN_URL/healthz" >&2
    exit 2
fi

TOPICS_JSON="$(cq_curl -fsS "$ADMIN_URL/topics")"
TOPIC_NAMES=()
while IFS= read -r name; do
    [ -n "$name" ] && TOPIC_NAMES+=("$name")
done < <(printf '%s' "$TOPICS_JSON" | jq -r '.[].name')

if [ "${#TOPIC_NAMES[@]}" -eq 0 ]; then
    log "WARNING: server reports zero topics — backup will contain no per-topic data."
fi

# ---------------------------------------------------------------
# 2. Force-rotate every persistent topic's journal so the active
#    segment is sealed (+ fsynced) before we copy anything.
# ---------------------------------------------------------------
ROTATED=()
SKIPPED_NO_TXLOG=()
for name in "${TOPIC_NAMES[@]:-}"; do
    # Topic names are like "/positions" and axum's `:topic` path segment
    # is matched literally against the DashMap key (leading slash
    # included) — stripping the slash 404s. Percent-encode it instead
    # (axum decodes path params before matching).
    encoded="$(printf '%s' "$name" | sed 's#^/#%2F#')"
    resp_tmp="$(mktemp -t cq-backup-rotate-resp.XXXXXX)"
    http_code="$(cq_curl -s -o "$resp_tmp" -w '%{http_code}' \
        -X POST "$ADMIN_URL/admin/rotate-journal/$encoded")"
    body="$(cat "$resp_tmp" 2>/dev/null || true)"
    rm -f "$resp_tmp"
    case "$http_code" in
        200)
            ROTATED+=("$name")
            log "rotated: $name"
            ;;
        400)
            SKIPPED_NO_TXLOG+=("$name")
            log "skipped (not persistent): $name"
            ;;
        *)
            echo "ERROR: rotate-journal failed for topic '$name' (HTTP $http_code): $body" >&2
            exit 2
            ;;
    esac
done

# ---------------------------------------------------------------
# 3. Stage: copy each persistent topic's directory tree out of the
#    live data dir. We copy (not move) so the live server is
#    untouched. rsync is preferred for its checksum/partial-file
#    safety; fall back to cp -R if rsync isn't installed.
# ---------------------------------------------------------------
WORKDIR="$(mktemp -d -t cqserver-backup.XXXXXX)"
cleanup() {
    if [ "$KEEP_WORKDIR" -eq 0 ]; then
        rm -rf "$WORKDIR"
    else
        log "keeping workdir: $WORKDIR"
    fi
}
trap cleanup EXIT

STAGE="$WORKDIR/stage"
mkdir -p "$STAGE"

copy_tree() {
    local src="$1" dst="$2"
    mkdir -p "$dst"
    if command -v rsync >/dev/null 2>&1; then
        rsync -a "$src"/ "$dst"/
    else
        cp -R "$src"/. "$dst"/
    fi
}

declare -a MANIFEST_LINES=()
TOTAL_TOPICS_BACKED_UP=0

for name in "${TOPIC_NAMES[@]:-}"; do
    slug="$(printf '%s' "$name" | sed -E 's/[^A-Za-z0-9_.-]/_/g' | sed 's/^_*//')"
    src_dir="$DATA_DIR/$slug"
    if [ ! -d "$src_dir" ]; then
        # Non-persistent topics (skipped above) legitimately have no
        # on-disk directory. Anything else missing is worth a loud
        # warning but not necessarily fatal (e.g. a topic created after
        # the /topics snapshot we read).
        continue
    fi
    dst_dir="$STAGE/topics/$slug"
    if ! copy_tree "$src_dir" "$dst_dir"; then
        echo "ERROR: failed to copy $src_dir -> $dst_dir" >&2
        exit 3
    fi

    seg_count="$(find "$dst_dir" -maxdepth 1 -type f \( -name '*.log' -o -name '*.log.zst' \) | wc -l | tr -d ' ')"
    total_bytes="$(find "$dst_dir" -maxdepth 1 -type f \( -name '*.log' -o -name '*.log.zst' -o -name 'snapshot.bin' \) -exec stat -f%z {} \; 2>/dev/null | awk '{s+=$1} END {print s+0}')"
    if [ -z "$total_bytes" ] || [ "$total_bytes" = "0" ]; then
        # GNU stat fallback (Linux CI runners).
        total_bytes="$(find "$dst_dir" -maxdepth 1 -type f \( -name '*.log' -o -name '*.log.zst' -o -name 'snapshot.bin' \) -exec stat -c%s {} \; 2>/dev/null | awk '{s+=$1} END {print s+0}')"
    fi
    has_snapshot="false"
    [ -f "$dst_dir/snapshot.bin" ] && has_snapshot="true"

    MANIFEST_LINES+=("$(jq -n \
        --arg topic "$name" \
        --arg slug "$slug" \
        --argjson segCount "${seg_count:-0}" \
        --argjson totalBytes "${total_bytes:-0}" \
        --argjson hasSnapshot "$has_snapshot" \
        '{topic:$topic, slug:$slug, segmentCount:$segCount, totalBytes:$totalBytes, hasSnapshot:$hasSnapshot}')")
    TOTAL_TOPICS_BACKED_UP=$((TOTAL_TOPICS_BACKED_UP + 1))
done

# ---------------------------------------------------------------
# 4. Verify: replay every copied topic directory with the real
#    TxLogReader. Records maxSequence per topic into the manifest.
# ---------------------------------------------------------------
hr
log "Verifying ${TOTAL_TOPICS_BACKED_UP} backed-up topic dir(s) with TxLogReader..."
hr

run_verify() {
    local dir="$1"
    if [ -n "$VERIFY_BIN" ]; then
        "$VERIFY_BIN" "$dir"
    else
        ( cd "$ROOT" && cargo run --release -q -p cq-txlog --example verify_segments -- "$dir" )
    fi
}

FINAL_MANIFEST="[]"
VERIFY_FAILED=0
for i in "${!MANIFEST_LINES[@]:-}"; do
    entry="${MANIFEST_LINES[$i]}"
    slug="$(printf '%s' "$entry" | jq -r '.slug')"
    topic="$(printf '%s' "$entry" | jq -r '.topic')"
    dir="$STAGE/topics/$slug"

    if ! verify_out="$(run_verify "$dir" 2>&1)"; then
        echo "ERROR: verification FAILED for topic '$topic' (dir: $dir):" >&2
        echo "$verify_out" >&2
        VERIFY_FAILED=1
        continue
    fi
    # verify_segments prints one JSON line; take the last line in case
    # cargo run emitted build noise on stdout (it shouldn't with -q, but
    # be defensive).
    verify_json="$(printf '%s\n' "$verify_out" | tail -n1)"
    if ! printf '%s' "$verify_json" | jq -e . >/dev/null 2>&1; then
        echo "ERROR: verify_segments produced non-JSON output for topic '$topic':" >&2
        echo "$verify_out" >&2
        VERIFY_FAILED=1
        continue
    fi
    max_seq="$(printf '%s' "$verify_json" | jq -r '.maxSequence')"
    entries="$(printf '%s' "$verify_json" | jq -r '.entries')"
    log "verified: $topic  entries=$entries  maxSequence=$max_seq"

    entry_with_seq="$(printf '%s' "$entry" | jq --argjson maxSeq "$max_seq" --argjson entries "$entries" '. + {maxSequence: $maxSeq, verifiedEntries: $entries}')"
    FINAL_MANIFEST="$(printf '%s' "$FINAL_MANIFEST" | jq --argjson e "$entry_with_seq" '. + [$e]')"
done

if [ "$VERIFY_FAILED" -ne 0 ]; then
    echo "ERROR: one or more topic backups failed verification — refusing to produce archive." >&2
    exit 4
fi

# ---------------------------------------------------------------
# 5. Write manifest.json + tar up the staging dir.
# ---------------------------------------------------------------
BACKUP_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq -n \
    --arg createdAt "$BACKUP_TS" \
    --arg adminUrl "$ADMIN_URL" \
    --arg dataDir "$DATA_DIR" \
    --argjson topics "$FINAL_MANIFEST" \
    --argjson rotated "$(printf '%s\n' "${ROTATED[@]:-}" | jq -R . | jq -s 'map(select(length>0))')" \
    --argjson skippedNoTxlog "$(printf '%s\n' "${SKIPPED_NO_TXLOG[@]:-}" | jq -R . | jq -s 'map(select(length>0))')" \
    '{createdAt:$createdAt, adminUrl:$adminUrl, sourceDataDir:$dataDir, rotatedTopics:$rotated, skippedNonPersistentTopics:$skippedNoTxlog, topics:$topics}' \
    > "$STAGE/manifest.json"

mkdir -p "$(dirname "$OUTPUT")"
tar -czf "$OUTPUT" -C "$STAGE" .

# ---------------------------------------------------------------
# 6. Print manifest summary.
# ---------------------------------------------------------------
hr
log "BACKUP MANIFEST"
hr
printf '%-24s %8s %14s %12s\n' "TOPIC" "SEGMENTS" "BYTES" "MAXSEQ" >&2
printf '%-24s %8s %14s %12s\n' "-----" "--------" "-----" "------" >&2
printf '%s' "$FINAL_MANIFEST" | jq -c '.[]' | while IFS= read -r e; do
    t="$(printf '%s' "$e" | jq -r '.topic')"
    s="$(printf '%s' "$e" | jq -r '.segmentCount')"
    b="$(printf '%s' "$e" | jq -r '.totalBytes')"
    m="$(printf '%s' "$e" | jq -r '.maxSequence')"
    printf '%-24s %8s %14s %12s\n' "$t" "$s" "$b" "$m" >&2
done
hr
log "Backup archive: $OUTPUT ($(du -h "$OUTPUT" 2>/dev/null | cut -f1))"
log "Topics backed up: $TOTAL_TOPICS_BACKED_UP  (skipped non-persistent: ${#SKIPPED_NO_TXLOG[@]:-0})"
log "backup-cqserver.sh: OK"

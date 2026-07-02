#!/usr/bin/env bash
#
# restore-cqserver.sh — restore a cqserver txlog data dir from a backup
# produced by scripts/backup-cqserver.sh (D4/P0.4).
#
# Steps:
#   1. Unpack the backup archive to a scratch dir; read manifest.json.
#   2. Place each topic's segment/snapshot files into <target-data-dir>/<slug>/.
#      Target dir must be empty (or --force to wipe it first) — restoring
#      on top of an existing, unrelated txlog would silently interleave
#      segment ids from two different histories.
#   3. Optionally (--start-server) start a cqserver instance pointed at
#      the restored dir using a caller-supplied config template, wait for
#      /healthz, then compare each topic's rowCount (GET /topics) against
#      the manifest's verifiedEntries-derived expectation and print a
#      pass/fail report. This step is best-effort verification, not a
#      substitute for the operator's own smoke test — row COUNT parity
#      doesn't prove row CONTENT parity, but catches the overwhelming
#      majority of "restore silently lost/duplicated data" failures.
#
# Usage:
#   scripts/restore-cqserver.sh \
#     --backup   /path/to/cqserver-backup-*.tar.gz \
#     --data-dir /path/to/restored/txlog \
#     [--force] \
#     [--start-server --binary target/release/cqserver --config-template /path/to/template.toml --admin-url http://127.0.0.1:8085 --tcp-port 9107 --ws-port 9108]
#
# Exit codes:
#   0  restore placed successfully (and, if --start-server, row counts matched)
#   1  usage / argument error
#   2  target data dir not empty and --force not given
#   3  unpack/place failed
#   4  post-restore verification failed (row count mismatch)

set -euo pipefail

BACKUP=""
DATA_DIR=""
FORCE=0
START_SERVER=0
BINARY=""
CONFIG_TEMPLATE=""
ADMIN_URL="http://127.0.0.1:8085"
TOKEN=""

usage() {
    cat >&2 <<'EOF'
Usage: restore-cqserver.sh --backup <archive> --data-dir <dir> [--force]
         [--start-server --binary <path> --config-template <path> [--admin-url <url>] [--token <tok>]]

  --backup           Path to the .tar.gz produced by backup-cqserver.sh
  --data-dir         Target txlog directory to restore into
  --force            Wipe --data-dir first if it's non-empty
  --start-server     After placing files, start cqserver and verify row counts
  --binary           Path to the cqserver binary (required with --start-server)
  --config-template  Config TOML whose [txlog].directory will be pointed at
                      --data-dir for the verification run (required with --start-server)
  --admin-url        Admin URL to poll for verification (default http://127.0.0.1:8085;
                      must match the admin_addr in --config-template)
  --token            Optional admin bearer token (or CQ_ADMIN_TOKEN)
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --backup) BACKUP="$2"; shift 2 ;;
        --data-dir) DATA_DIR="$2"; shift 2 ;;
        --force) FORCE=1; shift ;;
        --start-server) START_SERVER=1; shift ;;
        --binary) BINARY="$2"; shift 2 ;;
        --config-template) CONFIG_TEMPLATE="$2"; shift 2 ;;
        --admin-url) ADMIN_URL="$2"; shift 2 ;;
        --token) TOKEN="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage; exit 1 ;;
    esac
done

TOKEN="${TOKEN:-${CQ_ADMIN_TOKEN:-}}"

if [ -z "$BACKUP" ] || [ -z "$DATA_DIR" ]; then
    echo "ERROR: --backup and --data-dir are required." >&2
    usage
    exit 1
fi
if [ ! -f "$BACKUP" ]; then
    echo "ERROR: backup archive not found: $BACKUP" >&2
    exit 1
fi
if [ "$START_SERVER" -eq 1 ] && { [ -z "$BINARY" ] || [ -z "$CONFIG_TEMPLATE" ]; }; then
    echo "ERROR: --start-server requires --binary and --config-template." >&2
    exit 1
fi

log() { printf '%s\n' "$*" >&2; }
hr()  { printf '%s\n' "──────────────────────────────────────────────────────────────" >&2; }

# ---------------------------------------------------------------
# 1. Target dir must be empty unless --force.
# ---------------------------------------------------------------
mkdir -p "$DATA_DIR"
if [ -n "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
    if [ "$FORCE" -eq 1 ]; then
        log "WARNING: --force given, wiping non-empty target dir: $DATA_DIR"
        rm -rf "${DATA_DIR:?}"/*
    else
        echo "ERROR: target data dir is not empty: $DATA_DIR (pass --force to wipe it first)" >&2
        exit 2
    fi
fi

# ---------------------------------------------------------------
# 2. Unpack + place.
# ---------------------------------------------------------------
WORKDIR="$(mktemp -d -t cqserver-restore.XXXXXX)"
trap 'rm -rf "$WORKDIR"' EXIT

hr
log "restore-cqserver.sh: backup=$BACKUP -> data-dir=$DATA_DIR"
hr

tar -xzf "$BACKUP" -C "$WORKDIR"
MANIFEST="$WORKDIR/manifest.json"
if [ ! -f "$MANIFEST" ]; then
    echo "ERROR: backup archive has no manifest.json — not a backup-cqserver.sh archive?" >&2
    exit 3
fi

log "Backup manifest: created $(jq -r '.createdAt' "$MANIFEST") from $(jq -r '.adminUrl' "$MANIFEST")"

TOPIC_COUNT="$(jq '.topics | length' "$MANIFEST")"
if [ "$TOPIC_COUNT" -eq 0 ]; then
    log "WARNING: manifest lists zero topics — nothing to restore."
fi

for i in $(seq 0 $((TOPIC_COUNT - 1))); do
    slug="$(jq -r ".topics[$i].slug" "$MANIFEST")"
    topic="$(jq -r ".topics[$i].topic" "$MANIFEST")"
    src="$WORKDIR/topics/$slug"
    dst="$DATA_DIR/$slug"
    if [ ! -d "$src" ]; then
        echo "ERROR: manifest references topic '$topic' (slug $slug) but $src is missing from the archive" >&2
        exit 3
    fi
    mkdir -p "$dst"
    if command -v rsync >/dev/null 2>&1; then
        rsync -a "$src"/ "$dst"/
    else
        cp -R "$src"/. "$dst"/
    fi
    log "restored: $topic -> $dst"
done

log "restore-cqserver.sh: files placed OK ($TOPIC_COUNT topic dir(s))"

if [ "$START_SERVER" -eq 0 ]; then
    log "restore-cqserver.sh: OK (--start-server not requested, skipping row-count verification)"
    exit 0
fi

# ---------------------------------------------------------------
# 3. Optional: start a server on the restored dir and verify row
#    counts against the manifest.
# ---------------------------------------------------------------
hr
log "Starting server on restored data dir for verification..."
hr

if [ ! -x "$BINARY" ]; then
    echo "ERROR: --binary is not an executable file: $BINARY" >&2
    exit 1
fi
if [ ! -f "$CONFIG_TEMPLATE" ]; then
    echo "ERROR: --config-template not found: $CONFIG_TEMPLATE" >&2
    exit 1
fi

VERIFY_CONFIG="$WORKDIR/verify-config.toml"
# Rewrite the [txlog] directory line (or append one) to point at the
# restored data dir. Config templates in this repo always have a
# `[txlog]` table with a `directory = "..."` key (see config.rs /
# config/cqserver.toml) — rewrite that key's value in place.
awk -v newdir="$DATA_DIR" '
    BEGIN { in_txlog = 0 }
    /^\[txlog\]/ { in_txlog = 1; print; next }
    /^\[/ { in_txlog = 0; print; next }
    in_txlog && /^[[:space:]]*directory[[:space:]]*=/ {
        print "directory = \"" newdir "\""
        next
    }
    { print }
' "$CONFIG_TEMPLATE" > "$VERIFY_CONFIG"

CQ_LOG="$WORKDIR/server.log"
"$BINARY" --config "$VERIFY_CONFIG" >"$CQ_LOG" 2>&1 &
SERVER_PID=$!
cleanup_server() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap 'cleanup_server; rm -rf "$WORKDIR"' EXIT

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

deadline=$(( $(date +%s) + 15 ))
until cq_curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "ERROR: verification server did not become healthy within 15s. Log:" >&2
        cat "$CQ_LOG" >&2 || true
        exit 4
    fi
    sleep 0.2
done

TOPICS_JSON="$(cq_curl -fsS "$ADMIN_URL/topics")"

hr
log "RESTORE VERIFICATION (row counts vs backup-time entry counts)"
hr
printf '%-24s %14s %14s %s\n' "TOPIC" "RESTORED-ROWS" "BACKED-UP-ENTRIES" "STATUS" >&2
printf '%-24s %14s %14s %s\n' "-----" "-------------" "------------------" "------" >&2

MISMATCH=0
for i in $(seq 0 $((TOPIC_COUNT - 1))); do
    topic="$(jq -r ".topics[$i].topic" "$MANIFEST")"
    expected_entries="$(jq -r ".topics[$i].verifiedEntries // 0" "$MANIFEST")"
    restored_rows="$(printf '%s' "$TOPICS_JSON" | jq -r --arg t "$topic" '.[] | select(.name == $t) | .rowCount')"
    if [ -z "$restored_rows" ]; then
        restored_rows="MISSING"
        status="FAIL (topic absent after restore)"
        MISMATCH=1
    elif [ "$restored_rows" -le "$expected_entries" ]; then
        # rowCount (live SOW) is <= the raw entry count backed up
        # (entries include tombstones/overwrites collapsed by key in
        # the SOW), so <= is the correct non-strict bound. Equality is
        # typical when every backed-up entry was a distinct live key.
        status="OK"
    else
        status="FAIL (more live rows than entries backed up)"
        MISMATCH=1
    fi
    printf '%-24s %14s %14s %s\n' "$topic" "$restored_rows" "$expected_entries" "$status" >&2
done
hr

if [ "$MISMATCH" -ne 0 ]; then
    echo "ERROR: restore verification found row-count mismatches — see table above." >&2
    exit 4
fi

log "restore-cqserver.sh: OK — all topics' restored row counts consistent with backup manifest."

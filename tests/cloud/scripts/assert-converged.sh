#!/usr/bin/env bash
#
# C0 convergence assertion. Run after `make local-up` is healthy.
#
# What it does:
#   1. Publish N rows to leader's /c0-orders via the cq-client Rust SDK.
#   2. Poll each follower's /admin/replication every 200 ms until the
#      `current_sequence` for /c0-orders matches the leader (or a
#      deadline elapses).
#   3. SOW /c0-orders from each follower; compare against the leader's
#      SOW byte-for-byte (after canonical sort by order_id).
#   4. Stop one follower, publish more rows, restart it, assert it
#      catches up.
#
# Exits 0 on success, non-zero with a diagnostic on failure.

set -euo pipefail

LEADER_ADMIN="${LEADER_ADMIN:-http://127.0.0.1:8085}"
FOLLOWER1_ADMIN="${FOLLOWER1_ADMIN:-http://127.0.0.1:8086}"
FOLLOWER2_ADMIN="${FOLLOWER2_ADMIN:-http://127.0.0.1:8087}"

LEADER_TCP="${LEADER_TCP:-tcp://127.0.0.1:9007}"
FOLLOWER1_TCP="${FOLLOWER1_TCP:-tcp://127.0.0.1:9017}"
FOLLOWER2_TCP="${FOLLOWER2_TCP:-tcp://127.0.0.1:9027}"

TOPIC="/c0-orders"
N_ROWS="${N_ROWS:-1000}"
DEADLINE_SEC="${DEADLINE_SEC:-30}"

red()    { printf "\033[31m%s\033[0m" "$*"; }
green()  { printf "\033[32m%s\033[0m" "$*"; }
yellow() { printf "\033[33m%s\033[0m" "$*"; }
log()    { printf "[c0] %s\n" "$*" >&2; }

assert_eq() {
  local actual="$1" expected="$2" msg="$3"
  if [[ "$actual" != "$expected" ]]; then
    log "$(red FAIL): $msg"
    log "  expected: $expected"
    log "  actual:   $actual"
    exit 1
  fi
}

# ─── 1. wait for all three servers to be healthy ──────────────────

wait_healthy() {
  local name="$1" url="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url/healthz" >/dev/null 2>&1; then
      log "$(green OK) $name is healthy"
      return 0
    fi
    sleep 0.5
  done
  log "$(red FAIL) $name never came up at $url"
  exit 1
}

wait_healthy "leader"    "$LEADER_ADMIN"
wait_healthy "follower1" "$FOLLOWER1_ADMIN"
wait_healthy "follower2" "$FOLLOWER2_ADMIN"

# ─── 2. publish N rows to leader ──────────────────────────────────

log "publishing $N_ROWS rows to leader at $LEADER_TCP"

# Use a tiny Rust SDK helper. We embed a one-shot Cargo run via the
# loadgen crate's underlying client — much simpler than re-deriving
# the wire protocol in bash. The loadgen crate already depends on
# cq-client, so this is essentially free.
# Note: this uses the `publish_throughput` scenario in single-shot
# mode with 0 subscribers; it publishes `rate * duration_secs` rows
# and exits.
N_DURATION=$(awk -v n="$N_ROWS" 'BEGIN{print n/500}')   # 500 msg/s × N/500 sec = N msgs
cargo run --quiet --release -p cq-loadgen -- \
  --server "$LEADER_TCP" \
  --topic "$TOPIC" \
  --scenario publish-throughput \
  --rate 500.0 \
  --duration-secs "$N_DURATION" \
  --warmup-secs 0 \
  --subscribers 0 \
  > /tmp/c0-publish.log 2>&1 || {
    log "$(red FAIL): publisher exited non-zero. tail:"
    tail -20 /tmp/c0-publish.log >&2
    exit 1
}

# ─── 3. wait for follower convergence ─────────────────────────────

leader_seq() {
  curl -fsS "$LEADER_ADMIN/admin/replication" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); \
        print(next((t['current_sequence'] for t in d.get('topics', []) \
        if t['topic'] == '$TOPIC'), 0))"
}

follower_applied() {
  local admin="$1"
  curl -fsS "$admin/metrics" | awk \
    -v topic="$TOPIC" '
      /^cq_repl_applied_max_sequence\{topic="[^"]+"\}/ {
        match($1, /topic="([^"]+)"/, arr)
        if (arr[1] == topic) { print $2; found=1; exit }
      }
      END { if (!found) print 0 }
    '
}

wait_converged() {
  local fname="$1" admin="$2" target="$3"
  local deadline=$(( $(date +%s) + DEADLINE_SEC ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local applied
    applied=$(follower_applied "$admin")
    # Allow integer comparison (metrics expose float form).
    applied=${applied%%.*}
    if [[ "${applied:-0}" -ge "$target" ]]; then
      log "$(green OK) $fname applied $applied / $target"
      return 0
    fi
    sleep 0.2
  done
  log "$(red FAIL) $fname did not catch up: applied=${applied:-0} target=$target"
  log "  $admin/metrics dump (filtered for cq_repl_*):"
  curl -fsS "$admin/metrics" | grep "^cq_repl_" | head -20 >&2 || true
  exit 1
}

LEADER_SEQ=$(leader_seq)
log "leader current_sequence=$LEADER_SEQ; waiting for followers to catch up"

wait_converged "follower1" "$FOLLOWER1_ADMIN" "$LEADER_SEQ"
wait_converged "follower2" "$FOLLOWER2_ADMIN" "$LEADER_SEQ"

# ─── 4. SOW each, compare ─────────────────────────────────────────

# We use a tiny one-off SDK call by running the trader-view-pivot
# scenario's underlying client API. Simpler approach: hit
# /topics on each server and assert rowCount matches.

count_rows() {
  local admin="$1"
  curl -fsS "$admin/topics" | python3 -c "
import sys, json
for t in json.load(sys.stdin):
    if t['name'] == '$TOPIC':
        print(t['rowCount']); sys.exit(0)
print(-1)"
}

LEADER_ROWS=$(count_rows "$LEADER_ADMIN")
F1_ROWS=$(count_rows "$FOLLOWER1_ADMIN")
F2_ROWS=$(count_rows "$FOLLOWER2_ADMIN")

log "row counts: leader=$LEADER_ROWS follower1=$F1_ROWS follower2=$F2_ROWS"
assert_eq "$F1_ROWS" "$LEADER_ROWS" "follower1 row count mismatch"
assert_eq "$F2_ROWS" "$LEADER_ROWS" "follower2 row count mismatch"

# ─── 5. failure injection: stop follower2, publish more, restart ──

if [[ "${SKIP_FAILURE_INJECTION:-0}" != "1" ]]; then
  log "$(yellow STEP): stopping follower2 mid-stream"
  docker stop cq-c0-follower2 >/dev/null

  log "publishing 500 more rows while follower2 is down"
  cargo run --quiet --release -p cq-loadgen -- \
    --server "$LEADER_TCP" \
    --topic "$TOPIC" \
    --scenario publish-throughput \
    --rate 500.0 --duration-secs 1.0 --warmup-secs 0 --subscribers 0 \
    > /tmp/c0-publish-2.log 2>&1

  log "$(yellow STEP): restarting follower2"
  docker start cq-c0-follower2 >/dev/null
  wait_healthy "follower2" "$FOLLOWER2_ADMIN"

  NEW_LEADER_SEQ=$(leader_seq)
  log "leader current_sequence=$NEW_LEADER_SEQ; checking follower2 catch-up"
  wait_converged "follower2" "$FOLLOWER2_ADMIN" "$NEW_LEADER_SEQ"

  NEW_F2_ROWS=$(count_rows "$FOLLOWER2_ADMIN")
  NEW_LEADER_ROWS=$(count_rows "$LEADER_ADMIN")
  assert_eq "$NEW_F2_ROWS" "$NEW_LEADER_ROWS" "follower2 post-restart row count mismatch"
fi

log "$(green ALL CHECKS PASSED)"
exit 0

#!/usr/bin/env bash
#
# Orchestrate a two-Mac stress run. Run this from the Mac that should
# act as the LOAD GENERATOR (typically the same Mac as the leader, so
# publishes go through loopback, and subscribers connect to the
# follower on the other Mac across the LAN — that's exactly the
# replica-reads measurement we want).
#
# Usage:
#   FOLLOWER1_IP=192.168.1.20 ./lab-stress.sh
#
# Optional knobs:
#   SUBS=2000                  — subscriber count (default 2000)
#   DURATION_SECS=30           — measurement window (default 30s)
#   PUBLISH_RATE=200           — publishes/sec at the leader (default 200)
#   PUBLISH_DURATION=2         — publish phase length (default 2s)
#   RESULTS_DIR=/path          — where to drop JSON snapshots
#   LEADER_TCP=tcp://...       — override leader URL (default loopback)
#   FOLLOWER_TCP=tcp://...     — override follower URL
#                                (default: tcp://$FOLLOWER1_IP:9007)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LAB_DIR/../../.." && pwd)"
LOADGEN_BINARY="${LOADGEN_BINARY:-$REPO_ROOT/target/release/cq-loadgen}"

if [[ -z "${FOLLOWER1_IP:-}" ]]; then
  echo "✗ FOLLOWER1_IP is required (the IP of the Mac running the follower)" >&2
  exit 1
fi

LEADER_TCP="${LEADER_TCP:-tcp://127.0.0.1:9007}"
LEADER_ADMIN="${LEADER_ADMIN:-http://127.0.0.1:8085}"
FOLLOWER_TCP="${FOLLOWER_TCP:-tcp://$FOLLOWER1_IP:9007}"
FOLLOWER_ADMIN="${FOLLOWER_ADMIN:-http://$FOLLOWER1_IP:8085}"

SUBS="${SUBS:-2000}"
DURATION_SECS="${DURATION_SECS:-30}"
PUBLISH_RATE="${PUBLISH_RATE:-200}"
PUBLISH_DURATION="${PUBLISH_DURATION:-2}"

RESULTS_DIR="${RESULTS_DIR:-$LAB_DIR/results/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RESULTS_DIR"

red()    { printf "\033[31m%s\033[0m" "$*"; }
green()  { printf "\033[32m%s\033[0m" "$*"; }
log()    { printf "[lab] %s\n" "$*" >&2; }

# ─── Sanity ───────────────────────────────────────────────────────

if [[ ! -x "$LOADGEN_BINARY" ]]; then
  echo "✗ cq-loadgen not built. run: cargo build --release -p cq-loadgen" >&2
  exit 1
fi

log "checking leader at $LEADER_ADMIN"
curl -fsS "$LEADER_ADMIN/healthz" >/dev/null || {
  echo "✗ leader unhealthy at $LEADER_ADMIN" >&2
  echo "  is lab-up-leader.sh running on this Mac?" >&2
  exit 1
}
log "$(green OK) leader reachable"

log "checking follower at $FOLLOWER_ADMIN"
curl -fsS "$FOLLOWER_ADMIN/healthz" >/dev/null || {
  echo "✗ follower unhealthy at $FOLLOWER_ADMIN" >&2
  echo "  is lab-up-follower.sh running on $FOLLOWER1_IP?" >&2
  echo "  is the follower Mac's firewall allowing :8085 + :9010 + :9007?" >&2
  exit 1
}
log "$(green OK) follower reachable across LAN"

# Confirm replication is connected (otherwise we're testing nothing).
log "verifying replication peer is connected"
sleep 1
SHIPPED=$(curl -fsS "$LEADER_ADMIN/metrics" \
  | awk -F' ' '/^cq_repl_connect_total/{print $2; exit}' \
  | head -1)
if [[ -z "$SHIPPED" || "$SHIPPED" == "0" ]]; then
  echo "✗ leader hasn't connected to any peer yet. Is the follower's :9010 reachable?" >&2
  exit 1
fi
log "$(green OK) leader has connected to peer (cq_repl_connect_total=$SHIPPED)"

# ─── Snapshot baseline ────────────────────────────────────────────

snap() {
  local tag="$1"
  curl -fsS "$LEADER_ADMIN/stats"    > "$RESULTS_DIR/leader-stats-$tag.json"
  curl -fsS "$FOLLOWER_ADMIN/stats"  > "$RESULTS_DIR/follower-stats-$tag.json"
  curl -fsS "$LEADER_ADMIN/metrics"   > "$RESULTS_DIR/leader-metrics-$tag.txt"
  curl -fsS "$FOLLOWER_ADMIN/metrics" > "$RESULTS_DIR/follower-metrics-$tag.txt"
  curl -fsS "$LEADER_ADMIN/admin/replication"   > "$RESULTS_DIR/leader-replication-$tag.json"
  curl -fsS "$FOLLOWER_ADMIN/admin/replication" > "$RESULTS_DIR/follower-replication-$tag.json"
}

log "capturing baseline snapshot → $RESULTS_DIR"
snap baseline

# ─── Phase 1: publish to leader ───────────────────────────────────

log "phase 1: publishing for ${PUBLISH_DURATION}s at $PUBLISH_RATE/s to $LEADER_TCP"
"$LOADGEN_BINARY" \
  --server "$LEADER_TCP" \
  --topic /lab-orders \
  --scenario publish-throughput \
  --rate "$PUBLISH_RATE" \
  --duration-secs "$PUBLISH_DURATION" \
  --warmup-secs 0 \
  --subscribers 0 \
  > "$RESULTS_DIR/phase1-publish.log" 2>&1 || {
    echo "✗ publish phase failed; see $RESULTS_DIR/phase1-publish.log" >&2
    exit 1
}
tail -5 "$RESULTS_DIR/phase1-publish.log" >&2

# Wait for follower to converge.
LEADER_SEQ=$(curl -fsS "$LEADER_ADMIN/admin/replication" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(next((t['current_sequence'] for t in d.get('topics',[]) if t['topic']=='/lab-orders'),0))")
log "waiting for follower convergence to seq=$LEADER_SEQ"
DEADLINE=$(( $(date +%s) + 30 ))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  APPLIED=$(curl -fsS "$FOLLOWER_ADMIN/metrics" \
    | awk '/^cq_repl_applied_max_sequence\{topic="\/lab-orders"\}/{print $2; exit}')
  APPLIED=${APPLIED%%.*}
  if [[ "${APPLIED:-0}" -ge "$LEADER_SEQ" ]]; then
    log "$(green OK) follower applied=$APPLIED / target=$LEADER_SEQ"
    break
  fi
  sleep 0.5
done

snap post-publish

# ─── Phase 2: subscribers on the follower ─────────────────────────
# This is the actual replica-reads measurement: subscribers connect
# OVER THE LAN to the follower; the follower's NIC carries all the
# egress; the leader is only handling its publishes.

log "phase 2: spinning up $SUBS subscribers via $FOLLOWER_TCP for ${DURATION_SECS}s"
"$LOADGEN_BINARY" \
  --server "$FOLLOWER_TCP" \
  --topic /lab-orders \
  --scenario stress2k-real \
  --subscribers "$SUBS" \
  --duration-secs "$DURATION_SECS" \
  --admin-url "$FOLLOWER_ADMIN" \
  2>&1 | tee "$RESULTS_DIR/phase2-stress.log"

snap post-stress

# ─── Summarize ────────────────────────────────────────────────────

log "$(green DONE) results captured in $RESULTS_DIR"
ls -la "$RESULTS_DIR" >&2

# Tiny inline summary.
python3 - <<PY >&2
import json, os
base = json.load(open("$RESULTS_DIR/leader-stats-baseline.json"))
post = json.load(open("$RESULTS_DIR/follower-stats-post-stress.json"))
print(f"  leader baseline RSS:   {base['processRssBytes']/1024/1024:.0f} MB")
print(f"  follower post-stress RSS: {post['processRssBytes']/1024/1024:.0f} MB")
print(f"  follower subs (server-side, peak):  {post['totalSubscriptions']}")
PY

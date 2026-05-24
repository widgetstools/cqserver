#!/usr/bin/env bash
# Stop the cqserver demo.
#
# Phase 1 — graceful: kill PIDs recorded by `start-demo.sh` in
#   `.demo-run/*.pid` and their direct children.
#
# Phase 2 — port sweep: anything still listening on the demo ports
#   (9007/9008/8085/5173) or connected as a TCP client to the cqserver
#   TCP port (9007), which catches the publisher / loader / any other
#   process that wasn't tracked by start-demo.sh.
#
# The port sweep is bounded to the demo's specific ports. If you have
# another process on these ports outside the demo, edit the lists below.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$ROOT/.demo-run"

# Ports the demo BINDS (anything listening here is presumed demo state).
LISTEN_PORTS=(9007 9008 8085 5173)
# cqserver's TCP data port — any ESTABLISHED client here is a demo
# client process (publisher, loader, etc.). We do NOT sweep clients of
# 9008 because those are browser tabs.
CLIENT_PORT=9007

c_blue=$'\033[34m'; c_dim=$'\033[2m'; c_green=$'\033[32m'
c_red=$'\033[31m';  c_yellow=$'\033[33m'; c_reset=$'\033[0m'

# Kill helper: signal a PID + its direct children, escalate to SIGKILL
# after ~0.9s if it doesn't exit.
kill_pid() {
  local pid="$1" desc="$2"
  if ! kill -0 "$pid" 2>/dev/null; then
    return
  fi
  printf "  stopping %s (pid=%s)\n" "$desc" "$pid"
  # `pkill -P` is bounded to direct descendants of this specific PID, so
  # we can kill npx → node wrappers without touching unrelated processes.
  pkill -P "$pid" 2>/dev/null || true
  kill "$pid" 2>/dev/null || true
  for _ in 1 2 3; do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.3
  done
  if kill -0 "$pid" 2>/dev/null; then
    pkill -9 -P "$pid" 2>/dev/null || true
    kill -9 "$pid" 2>/dev/null || true
    printf "  ${c_red}✗${c_reset} %s force-killed (pid=%s)\n" "$desc" "$pid"
  else
    printf "  ${c_green}✓${c_reset} %s stopped (pid=%s)\n" "$desc" "$pid"
  fi
}

# ── Capture client PIDs BEFORE we kill the server ──────────────
# Once the server dies, the ESTABLISHED TCP entries disappear, so we
# need to snapshot the publisher / loader PIDs up front.
listener_pid=$(lsof -ti :"$CLIENT_PORT" -sTCP:LISTEN 2>/dev/null | head -1 || true)
client_pids=$(lsof -ti :"$CLIENT_PORT" -sTCP:ESTABLISHED 2>/dev/null | sort -u || true)

# ── Phase 1: tracked PIDs from start-demo.sh ───────────────────
if [ -d "$RUN_DIR" ]; then
  printf "${c_blue}▸ Phase 1: tracked processes${c_reset}\n"
  for name in react-demo publisher server; do
    pidfile="$RUN_DIR/${name}.pid"
    if [ -f "$pidfile" ]; then
      pid=$(cat "$pidfile" 2>/dev/null || echo "")
      if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill_pid "$pid" "$name"
      else
        printf "  ${c_dim}%s already stopped${c_reset}\n" "$name"
      fi
      rm -f "$pidfile"
    fi
  done
else
  printf "${c_dim}No .demo-run dir; skipping tracked-process phase.${c_reset}\n"
fi

# ── Phase 2: sweep demo ports + binary paths ───────────────────
printf "${c_blue}▸ Phase 2: port + binary sweep${c_reset}\n"

# 2a) TCP clients of cqserver (publisher, loader, sanity scripts).
if [ -n "${client_pids:-}" ]; then
  for pid in $client_pids; do
    # Skip the server itself; it appears as ESTABLISHED for every active
    # client connection.
    [ "$pid" = "${listener_pid:-}" ] && continue
    kill_pid "$pid" "client of :$CLIENT_PORT"
  done
fi

# 2b) Kill cqserver by binary path. This catches the server even when
# its expected ports (9007/9008/8085) were never bound or were bound by
# something else — situations where the port sweep below would miss it.
# Matching is anchored to this repo's target/ dir so we never touch
# cqserver instances from other checkouts.
for pat in "$ROOT/target/release/cqserver" "$ROOT/target/debug/cqserver"; do
  for pid in $(pgrep -f "$pat" 2>/dev/null); do
    kill_pid "$pid" "cqserver ($pat)"
  done
done

# 2c) Listeners on every demo port (catches vite + anything else still up).
for port in "${LISTEN_PORTS[@]}"; do
  for pid in $(lsof -ti :"$port" -sTCP:LISTEN 2>/dev/null); do
    kill_pid "$pid" "listener on :$port"
  done
done

# Final state summary.
remaining=0
for port in "${LISTEN_PORTS[@]}"; do
  if lsof -ti :"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    remaining=$((remaining + 1))
  fi
done
if [ "$remaining" -eq 0 ]; then
  printf "${c_green}Done — all demo ports free.${c_reset}\n"
else
  printf "${c_yellow}Done — %s demo port(s) still bound (check manually).${c_reset}\n" "$remaining"
fi

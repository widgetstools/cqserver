#!/usr/bin/env bash
#
# Clean teardown for the lab: stop any running cqserver (matched by
# binary path, so we don't kill unrelated processes), optionally
# delete the persistent txlog dirs.
#
# Usage:
#   ./lab-down.sh             # stop processes
#   ./lab-down.sh --purge     # stop + delete txlog dirs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LAB_DIR/../../.." && pwd)"
LAB_BINARY="${LAB_BINARY:-$REPO_ROOT/target/release/cqserver}"

PURGE=0
if [[ "${1:-}" == "--purge" ]]; then
  PURGE=1
fi

PIDS=$(pgrep -fl "$LAB_BINARY" || true)
if [[ -z "$PIDS" ]]; then
  echo "→ no cqserver processes matching $LAB_BINARY"
else
  echo "→ killing cqserver:"
  echo "$PIDS"
  echo "$PIDS" | awk '{print $1}' | xargs kill 2>/dev/null || true
  sleep 0.5
  STRAGGLERS=$(pgrep -fl "$LAB_BINARY" || true)
  if [[ -n "$STRAGGLERS" ]]; then
    echo "→ stragglers; SIGKILL:"
    echo "$STRAGGLERS" | awk '{print $1}' | xargs kill -9 2>/dev/null || true
  fi
fi

if [[ $PURGE -eq 1 ]]; then
  for d in /tmp/cqserver-lab/leader /tmp/cqserver-lab/follower; do
    if [[ -d "$d" ]]; then
      echo "→ rm -rf $d"
      rm -rf "$d"
    fi
  done
fi

echo "✓ done"

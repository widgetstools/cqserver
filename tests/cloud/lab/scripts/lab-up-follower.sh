#!/usr/bin/env bash
#
# Start a cqserver follower for the two-Mac lab. No env vars required;
# the follower just listens on :9010 and waits for the leader's
# shipper to connect.
#
# Usage on Mac B (the follower):
#   ./lab-up-follower.sh
#
# Optional:
#   LAB_TXLOG_DIR=/path        — txlog directory
#   LAB_BIND_IP=0.0.0.0        — listen address
#   LAB_BINARY=/path/cqserver  — binary path

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LAB_DIR/../../.." && pwd)"

LAB_BINARY="${LAB_BINARY:-$REPO_ROOT/target/release/cqserver}"
LAB_TXLOG_DIR="${LAB_TXLOG_DIR:-/tmp/cqserver-lab/follower/txlog}"
LAB_BIND_IP="${LAB_BIND_IP:-0.0.0.0}"

if [[ ! -x "$LAB_BINARY" ]]; then
  echo "✗ binary not found at $LAB_BINARY" >&2
  echo "  run: cargo build --release -p cq-server" >&2
  exit 1
fi

export LAB_TXLOG_DIR LAB_BIND_IP

mkdir -p "$LAB_TXLOG_DIR"
RUN_DIR="$(dirname "$LAB_TXLOG_DIR")"
mkdir -p "$RUN_DIR"
CONFIG_PATH="$RUN_DIR/cqserver.toml"

cp "$LAB_DIR/configs/follower.toml.template" "$CONFIG_PATH"

echo "→ lab-up-follower"
echo "    bind        $LAB_BIND_IP"
echo "    repl listen $LAB_BIND_IP:9010 (leader connects here)"
echo "    txlog       $LAB_TXLOG_DIR"
echo "    config      $CONFIG_PATH"
echo "    binary      $LAB_BINARY"
echo "    admin       http://$LAB_BIND_IP:8085/  (UI at /ui)"
echo
echo "  Discover this Mac's LAN IP for the leader to use:"
echo "    ipconfig getifaddr en0    # Wi-Fi"
echo "    ipconfig getifaddr en1    # Ethernet"
echo

exec "$LAB_BINARY" --config "$CONFIG_PATH"

#!/usr/bin/env bash
#
# Start a cqserver leader for the two-Mac lab. Reads $FOLLOWER1_IP +
# optional $FOLLOWER2_IP from the environment, renders the leader
# config from the template, exec's cqserver.
#
# Usage on Mac A (the leader):
#   export FOLLOWER1_IP=192.168.1.20
#   ./lab-up-leader.sh
#
# Or for two followers:
#   export FOLLOWER1_IP=192.168.1.20 FOLLOWER2_IP=192.168.1.21
#   ./lab-up-leader.sh
#
# Optional:
#   LAB_TXLOG_DIR=/path        — txlog directory (default /tmp/cqserver-lab/leader/txlog)
#   LAB_BIND_IP=0.0.0.0        — listen address (default 0.0.0.0)
#   LAB_BINARY=/path/cqserver  — binary path (default: from repo)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$LAB_DIR/../../.." && pwd)"

LAB_BINARY="${LAB_BINARY:-$REPO_ROOT/target/release/cqserver}"
LAB_TXLOG_DIR="${LAB_TXLOG_DIR:-/tmp/cqserver-lab/leader/txlog}"
LAB_BIND_IP="${LAB_BIND_IP:-0.0.0.0}"

if [[ -z "${FOLLOWER1_IP:-}" ]]; then
  echo "✗ FOLLOWER1_IP must be set to the follower Mac's LAN IP" >&2
  echo "  e.g.  FOLLOWER1_IP=192.168.1.20 $0" >&2
  exit 1
fi

if [[ ! -x "$LAB_BINARY" ]]; then
  echo "✗ binary not found at $LAB_BINARY" >&2
  echo "  run: cargo build --release -p cq-server" >&2
  exit 1
fi

# Build the peers TOML array, omitting any empty entries.
PEERS_TOML_LIST='['
PEERS_TOML_LIST+="\"${FOLLOWER1_IP}:9010\""
if [[ -n "${FOLLOWER2_IP:-}" ]]; then
  PEERS_TOML_LIST+=", \"${FOLLOWER2_IP}:9010\""
fi
PEERS_TOML_LIST+=']'

export PEERS_TOML_LIST LAB_TXLOG_DIR LAB_BIND_IP

mkdir -p "$LAB_TXLOG_DIR"
RUN_DIR="$(dirname "$LAB_TXLOG_DIR")"
mkdir -p "$RUN_DIR"
CONFIG_PATH="$RUN_DIR/cqserver.toml"

# Note: cqserver does ${VAR} substitution at startup, so we just copy
# the template through. The substitution sees PEERS_TOML_LIST in env
# and inlines it.
cp "$LAB_DIR/configs/leader.toml.template" "$CONFIG_PATH"

echo "→ lab-up-leader"
echo "    bind        $LAB_BIND_IP"
echo "    peers       $PEERS_TOML_LIST"
echo "    txlog       $LAB_TXLOG_DIR"
echo "    config      $CONFIG_PATH"
echo "    binary      $LAB_BINARY"
echo "    admin       http://$LAB_BIND_IP:8085/  (UI at /ui)"
echo

exec "$LAB_BINARY" --config "$CONFIG_PATH"

#!/usr/bin/env bash
# Start the full cqserver FI demo end-to-end:
#   1. cqserver (Rust release binary)
#   2. Generate JSON tables (idempotent)
#   3. Load JSON into the server
#   4. Live publisher (market-data ticks + trades)
#   5. React demo dev server
#
# PIDs and logs are written under .demo-run/ so stop-demo.sh can shut
# everything down cleanly.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$ROOT/.demo-run"
SERVER_BIN="$ROOT/target/release/cqserver"
SERVER_CFG="$ROOT/config/cqserver.toml"
ADMIN_URL="http://127.0.0.1:8085"

mkdir -p "$RUN_DIR"

c_blue=$'\033[34m'; c_dim=$'\033[2m'; c_green=$'\033[32m'; c_red=$'\033[31m'; c_reset=$'\033[0m'
step()  { printf "${c_blue}▸ %s${c_reset}\n" "$*"; }
info()  { printf "  ${c_dim}%s${c_reset}\n" "$*"; }
ok()    { printf "  ${c_green}✓ %s${c_reset}\n" "$*"; }
fail()  { printf "  ${c_red}✗ %s${c_reset}\n" "$*"; exit 1; }

# ──────────────────────────────────────────────────────────────────
# Pre-flight checks
# ──────────────────────────────────────────────────────────────────

step "Pre-flight"

# Refuse to start on top of a previous run.
for name in server publisher react-demo; do
  pidfile="$RUN_DIR/${name}.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    fail "$name already running (pid=$(cat "$pidfile")); run ./stop-demo.sh first"
  fi
done

# Ports must be free. Only LISTENing sockets count — CLOSE_WAIT
# stragglers from a recently-killed cqserver (browsers in particular
# leave these around) don't actually block a fresh bind.
for port in 9007 9008 8085 5173; do
  if lsof -ti :"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "Port $port already in use (pid=$(lsof -ti :$port -sTCP:LISTEN | head -1))"
  fi
done
ok "Ports 9007 9008 8085 5173 free"

# Build server if it's missing.
if [ ! -x "$SERVER_BIN" ]; then
  info "cqserver binary not found — building release..."
  (cd "$ROOT" && cargo build --release -p cq-server)
fi
ok "cqserver binary present"

# Make sure JS deps are installed.
for d in client-sdks/ts clients/react-demo; do
  if [ ! -d "$ROOT/$d/node_modules" ]; then
    info "Installing JS deps in $d..."
    (cd "$ROOT/$d" && npm install --silent)
  fi
done
ok "JS deps installed"

# ──────────────────────────────────────────────────────────────────
# 1. cqserver
# ──────────────────────────────────────────────────────────────────

step "Starting cqserver"
(
  cd "$ROOT"
  exec "$SERVER_BIN" --config "$SERVER_CFG" >"$RUN_DIR/server.log" 2>&1
) &
echo $! > "$RUN_DIR/server.pid"
info "pid=$(cat "$RUN_DIR/server.pid")  log=$RUN_DIR/server.log"

# Wait for healthz.
for _ in $(seq 1 60); do
  if curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
if ! curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; then
  fail "cqserver did not come up — check $RUN_DIR/server.log"
fi
ok "cqserver healthy"

# ──────────────────────────────────────────────────────────────────
# 2. Generate JSON data
# ──────────────────────────────────────────────────────────────────

step "Generating FI demo JSON"
(cd "$ROOT/client-sdks/ts" && npm run --silent generate-fi-data) > "$RUN_DIR/generate.log" 2>&1
ok "JSON written to client-sdks/ts/examples/data/"

# ──────────────────────────────────────────────────────────────────
# 3. Load JSON into server
# ──────────────────────────────────────────────────────────────────

step "Loading data into cqserver"
(cd "$ROOT/client-sdks/ts" && npm run --silent load-fi-data) > "$RUN_DIR/load.log" 2>&1
ok "$(grep -E '^Loaded in' "$RUN_DIR/load.log" || echo loaded)"

# ──────────────────────────────────────────────────────────────────
# 4. Live publisher
# ──────────────────────────────────────────────────────────────────

step "Starting live publisher"
(
  cd "$ROOT/client-sdks/ts"
  exec npx --no-install tsx examples/fi-publisher.ts >"$RUN_DIR/publisher.log" 2>&1
) &
echo $! > "$RUN_DIR/publisher.pid"
info "pid=$(cat "$RUN_DIR/publisher.pid")  log=$RUN_DIR/publisher.log"

# Wait for the publisher to reach the streaming phase before continuing.
for _ in $(seq 1 60); do
  if grep -q "Streaming:" "$RUN_DIR/publisher.log" 2>/dev/null; then break; fi
  sleep 0.25
done
ok "Publisher streaming"

# ──────────────────────────────────────────────────────────────────
# 5. React demo dev server
# ──────────────────────────────────────────────────────────────────

step "Starting React blotter dev server"
(
  cd "$ROOT/clients/react-demo"
  exec npx --no-install vite >"$RUN_DIR/react-demo.log" 2>&1
) &
echo $! > "$RUN_DIR/react-demo.pid"
info "pid=$(cat "$RUN_DIR/react-demo.pid")  log=$RUN_DIR/react-demo.log"

for _ in $(seq 1 40); do
  if grep -q "Local:" "$RUN_DIR/react-demo.log" 2>/dev/null; then break; fi
  sleep 0.25
done
ok "React dev server up"

# ──────────────────────────────────────────────────────────────────
# Summary
# ──────────────────────────────────────────────────────────────────

cat <<EOF

${c_green}Demo running.${c_reset}

  Admin UI       $ADMIN_URL/
  FI dashboard   $ADMIN_URL/fi-demo
  React blotter  http://127.0.0.1:5173/

  Logs           $RUN_DIR/
  Stop           ./stop-demo.sh
EOF

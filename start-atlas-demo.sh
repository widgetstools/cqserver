#!/usr/bin/env bash
# Start the cqserver Atlas demo end-to-end:
#   1. cqserver (Rust release binary)
#   2. Build & run the Atlas publisher (rich 200-col positions / trades,
#      ticks server-side at ~2 Hz)
#   3. examples-web dev server (browser subscribes via WS, every cell
#      live from cqserver)
#
# Coexists with start-demo.sh (fi-publisher + react-demo). Use:
#   ./stop-demo.sh           # tears down whichever variant is running
#   ./start-atlas-demo.sh    # this script
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
RUN_DIR="$ROOT/.demo-run"
SERVER_BIN="$ROOT/target/release/cqserver"
SERVER_CFG="$ROOT/config/cqserver.toml"
ADMIN_URL="http://127.0.0.1:8085"
PUB_BUNDLE="$RUN_DIR/atlas-publisher.mjs"

mkdir -p "$RUN_DIR"

c_blue=$'\033[34m'; c_dim=$'\033[2m'; c_green=$'\033[32m'; c_red=$'\033[31m'; c_reset=$'\033[0m'
step()  { printf "${c_blue}▸ %s${c_reset}\n" "$*"; }
info()  { printf "  ${c_dim}%s${c_reset}\n" "$*"; }
ok()    { printf "  ${c_green}✓ %s${c_reset}\n" "$*"; }
fail()  { printf "  ${c_red}✗ %s${c_reset}\n" "$*"; exit 1; }

step "Pre-flight"

for name in server publisher examples-web; do
  pidfile="$RUN_DIR/${name}.pid"
  if [ -f "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then
    fail "$name already running (pid=$(cat "$pidfile")); run ./stop-demo.sh first"
  fi
done

for port in 9007 9008 8085 5175; do
  if lsof -ti :"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "Port $port already in use (pid=$(lsof -ti :$port -sTCP:LISTEN | head -1))"
  fi
done
ok "Ports 9007 9008 8085 5175 free"

if [ ! -x "$SERVER_BIN" ]; then
  info "cqserver binary not found — building release..."
  (cd "$ROOT" && cargo build --release -p cq-server)
fi
ok "cqserver binary present"

for d in client-sdks/ts clients/examples-web; do
  if [ ! -d "$ROOT/$d/node_modules" ]; then
    info "Installing JS deps in $d..."
    (cd "$ROOT/$d" && npm install --silent)
  fi
done
ok "JS deps installed"

# Make sure client-sdks/ts is built — the publisher bundle pulls from dist/.
if [ ! -f "$ROOT/client-sdks/ts/dist/index.js" ]; then
  info "Building @cqserver/client (client-sdks/ts) ..."
  (cd "$ROOT/client-sdks/ts" && npm run --silent build)
fi
ok "@cqserver/client built"

step "Building Atlas publisher bundle"
(cd "$ROOT/clients/examples-web" && node scripts/build-publisher.mjs) > "$RUN_DIR/publisher-build.log" 2>&1
[ -f "$PUB_BUNDLE" ] || fail "publisher bundle missing — see $RUN_DIR/publisher-build.log"
ok "$(ls -lh "$PUB_BUNDLE" | awk '{print $5, $9}')"

# The Atlas schema re-keys /trades (trade_id) and /positions (position_id).
# Any txlog written under the older fi-publisher key scheme would replay
# into a store with a different key, which is both slow and meaningless
# for a demo. Wipe so cqserver starts from a clean SOW each run.
step "Clearing stale txlog state"
if [ -d "$ROOT/data/txlog" ]; then
  for topic in trades positions; do
    if [ -d "$ROOT/data/txlog/$topic" ] && [ -n "$(ls -A "$ROOT/data/txlog/$topic" 2>/dev/null)" ]; then
      seg_count=$(ls -1 "$ROOT/data/txlog/$topic" | wc -l | tr -d ' ')
      rm -f "$ROOT/data/txlog/$topic"/*
      info "wiped $seg_count segment(s) under data/txlog/$topic/"
    fi
  done
fi
ok "txlog clean"

step "Starting cqserver"
(
  cd "$ROOT"
  exec "$SERVER_BIN" --config "$SERVER_CFG" >"$RUN_DIR/server.log" 2>&1
) &
echo $! > "$RUN_DIR/server.pid"
info "pid=$(cat "$RUN_DIR/server.pid")  log=$RUN_DIR/server.log"

# Healthz wait — 60s is generous for a cold start with txlog recovery.
# On a clean Atlas start this completes in well under a second.
for _ in $(seq 1 240); do
  if curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
if ! curl -fsS "$ADMIN_URL/healthz" >/dev/null 2>&1; then
  fail "cqserver did not come up — check $RUN_DIR/server.log"
fi
ok "cqserver healthy"

step "Starting Atlas publisher"
(
  cd "$ROOT"
  exec node "$PUB_BUNDLE" >"$RUN_DIR/publisher.log" 2>&1
) &
echo $! > "$RUN_DIR/publisher.pid"
info "pid=$(cat "$RUN_DIR/publisher.pid")  log=$RUN_DIR/publisher.log"

# Wait for the publisher to finish seeding before bringing up the UI.
# Generous (5 min): a large POSITIONS universe seeds for a while before
# the tick loop starts.
for _ in $(seq 1 600); do
  if grep -q "tick loop:" "$RUN_DIR/publisher.log" 2>/dev/null; then break; fi
  sleep 0.5
done
grep -q "tick loop:" "$RUN_DIR/publisher.log" || fail "Atlas publisher did not start ticking — check $RUN_DIR/publisher.log"
ok "Atlas publisher ticking"

step "Starting examples-web dev server"
(
  cd "$ROOT/clients/examples-web"
  exec npx --no-install vite >"$RUN_DIR/examples-web.log" 2>&1
) &
echo $! > "$RUN_DIR/examples-web.pid"
info "pid=$(cat "$RUN_DIR/examples-web.pid")  log=$RUN_DIR/examples-web.log"

for _ in $(seq 1 40); do
  if grep -q "Local:" "$RUN_DIR/examples-web.log" 2>/dev/null; then break; fi
  sleep 0.25
done
ok "examples-web ready → http://127.0.0.1:5175/"

printf "\n${c_green}Atlas demo up.${c_reset}\n"
printf "  cqserver admin → ${ADMIN_URL}\n"
printf "  examples-web   → http://127.0.0.1:5175/\n"
printf "  logs           → ${RUN_DIR}/*.log\n"
printf "Stop with: ./stop-demo.sh\n"

#!/usr/bin/env bash
#
# nightly-tests.sh — gate the slow / environment-dependent tests that
# are #[ignore]d from the default `cargo test` run.
#
# These tests exist because they're slow or need special environment
# setup (huge fd counts, an hour of wall clock, a fresh release
# binary) — NOT because they're flaky or expected to fail. This
# script turns them into pass/fail GATES, not just logs: it runs each
# one, checks assertions (including a memory-per-row regression
# ceiling), prints a summary table, and exits nonzero if anything
# fails.
#
# Usage:
#   scripts/nightly-tests.sh
#
# Env vars:
#   NIGHTLY_SOAK=1        Also run the 1-hour active-set soak test
#                          (cq-core active_set_bounds::
#                          soak_active_set_stays_bounded_under_1h_churn).
#                          Off by default so the default lane stays
#                          well under 30 minutes.
#   NIGHTLY_SKIP="a,b"    Comma-separated list of test keys (see the
#                          TESTS table below) to skip. Use this ONLY
#                          for tests that fail for environment reasons
#                          on the current box (e.g. a laptop that
#                          can't sustain 10k live sockets under
#                          thermal/scheduler pressure) — never to
#                          paper over a real regression. Document why
#                          in the PR/commit that sets it.
#   CQ_MEM_ROWS           Forwarded to mem_per_row_e2e (defaults to
#                          that test's own default, 500_000).
#
# Exit code: 0 if every non-skipped test passed (and, for
# mem_per_row_e2e, every schema shape's bytes/row stayed at or below
# its recorded baseline ceiling). Nonzero otherwise.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BASELINE_FILE="$ROOT/bench/baselines/mem_per_row.txt"
SUMMARY_TXT="$(mktemp -t nightly-tests-summary.XXXXXX)"
trap 'rm -f "$SUMMARY_TXT"' EXIT

NIGHTLY_SOAK="${NIGHTLY_SOAK:-0}"
NIGHTLY_SKIP="${NIGHTLY_SKIP:-}"

log() { printf '%s\n' "$*" >&2; }
hr()  { printf '%s\n' "──────────────────────────────────────────────────────────────" >&2; }

# ---------------------------------------------------------------
# 0. Raise ulimit where possible. Stress tests open ~10k+ sockets
#    in-process plus the spawned server subprocess needs its own
#    headroom. Best-effort: don't fail the whole lane if the shell
#    won't let us raise it (e.g. a locked-down CI runner) — the
#    individual stress tests will surface the failure clearly if fd
#    exhaustion is the actual cause, and NIGHTLY_SKIP is there for
#    that case.
# ---------------------------------------------------------------
DESIRED_NOFILE=65536
CURRENT_NOFILE="$(ulimit -n)"
if [ "$CURRENT_NOFILE" -lt "$DESIRED_NOFILE" ]; then
    if ulimit -n "$DESIRED_NOFILE" 2>/dev/null; then
        log "ulimit -n raised: $CURRENT_NOFILE -> $(ulimit -n)"
    else
        log "WARNING: could not raise ulimit -n above $CURRENT_NOFILE (wanted $DESIRED_NOFILE)."
        log "         10k-connection stress tests may fail for fd-exhaustion reasons."
    fi
else
    log "ulimit -n already $CURRENT_NOFILE (>= $DESIRED_NOFILE), leaving as-is."
fi

# ---------------------------------------------------------------
# 1. Build the release binary FIRST. The e2e tests spawn
#    target/release/cqserver as a subprocess (falling back to
#    target/debug only if release is absent) — an out-of-date
#    release binary would silently exercise old server code while
#    the tests appear to pass against current test-harness code.
# ---------------------------------------------------------------
hr
log "Building release binary (cargo build --release -p cq-server)..."
hr
cargo build --release -p cq-server

BUILD_STAMP_BIN="$ROOT/target/release/cqserver"
if [ ! -x "$BUILD_STAMP_BIN" ]; then
    log "FATAL: expected release binary not found at $BUILD_STAMP_BIN after build."
    exit 1
fi
log "Release binary built: $BUILD_STAMP_BIN ($(date -r "$BUILD_STAMP_BIN" 2>/dev/null || true))"

# ---------------------------------------------------------------
# 2. Test table. Each row: key|crate|test-binary|test-filter-or-empty
#    "test-filter" narrows to a single #[ignore]d fn when a file has
#    more than one (bench_wide_ingest, loadgen_scenarios); empty runs
#    every #[ignore]d test in that binary.
# ---------------------------------------------------------------
TESTS=(
    "trader_view_pivot_e2e|cq-e2e-tests|trader_view_pivot_e2e|"
    "mem_per_row_e2e|cq-e2e-tests|mem_per_row_e2e|"
    "stress_10k_connections|cq-e2e-tests|stress_10k_connections|"
    "stress_10k_max_rows|cq-e2e-tests|stress_10k_max_rows|"
    "bench_wide_ingest_a|cq-e2e-tests|bench_wide_ingest|bench_a_wide_no_views_1conn"
    "bench_wide_ingest_b|cq-e2e-tests|bench_wide_ingest|bench_b_wide_with_views_1conn"
    "bench_wide_ingest_c|cq-e2e-tests|bench_wide_ingest|bench_c_wide_no_views_4conn"
    "loadgen_scenario_h|cq-e2e-tests|loadgen_scenarios|scenario_h_publish_batch_vs_sequential_smoke"
    "loadgen_scenario_i|cq-e2e-tests|loadgen_scenarios|scenario_i_schema_evolution_under_load_smoke"
)
if [ "$NIGHTLY_SOAK" = "1" ]; then
    TESTS+=("active_set_bounds_soak|cq-core|active_set_bounds|soak_active_set_stays_bounded_under_1h_churn")
else
    log "NIGHTLY_SOAK not set — skipping the 1-hour active_set_bounds soak test."
    log "  (set NIGHTLY_SOAK=1 to include it; keeps the default lane under 30 min)"
fi

is_skipped() {
    local key="$1"
    [ -z "$NIGHTLY_SKIP" ] && return 1
    IFS=',' read -ra SKIP_ARR <<< "$NIGHTLY_SKIP"
    for s in "${SKIP_ARR[@]}"; do
        s="$(echo "$s" | xargs)" # trim whitespace
        [ "$s" = "$key" ] && return 0
    done
    return 1
}

# ---------------------------------------------------------------
# 3. Run each test, capturing pass/fail + output for the mem_per_row
#    baseline check.
# ---------------------------------------------------------------
declare -a RESULT_KEYS=()
declare -a RESULT_STATUS=()   # PASS | FAIL | SKIP
declare -a RESULT_DETAIL=()
OVERALL_FAIL=0

run_one() {
    local key="$1" crate="$2" test_bin="$3" filter="$4"
    local out
    out="$(mktemp -t "nightly-${key}.XXXXXX")"

    if is_skipped "$key"; then
        log "SKIP  $key  (listed in NIGHTLY_SKIP)"
        RESULT_KEYS+=("$key"); RESULT_STATUS+=("SKIP"); RESULT_DETAIL+=("listed in NIGHTLY_SKIP")
        rm -f "$out"
        return 0
    fi

    hr
    log "RUN   $key  (cargo test --release -p $crate --test $test_bin -- --ignored --nocapture ${filter})"
    hr

    local cmd=(cargo test --release -p "$crate" --test "$test_bin" -- --ignored --nocapture)
    if [ -n "$filter" ]; then
        cmd+=("$filter")
    fi

    local start_ts end_ts elapsed
    start_ts="$(date +%s)"
    if "${cmd[@]}" 2>&1 | tee "$out" >&2; then
        end_ts="$(date +%s)"
        elapsed=$((end_ts - start_ts))

        if [ "$key" = "mem_per_row_e2e" ]; then
            if check_mem_per_row_baseline "$out"; then
                RESULT_KEYS+=("$key"); RESULT_STATUS+=("PASS"); RESULT_DETAIL+=("${elapsed}s, within baseline ceilings")
            else
                RESULT_KEYS+=("$key"); RESULT_STATUS+=("FAIL"); RESULT_DETAIL+=("${elapsed}s, EXCEEDED baseline ceiling(s) — see log above")
                OVERALL_FAIL=1
            fi
        else
            RESULT_KEYS+=("$key"); RESULT_STATUS+=("PASS"); RESULT_DETAIL+=("${elapsed}s")
        fi
    else
        end_ts="$(date +%s)"
        elapsed=$((end_ts - start_ts))
        RESULT_KEYS+=("$key"); RESULT_STATUS+=("FAIL"); RESULT_DETAIL+=("${elapsed}s, cargo test exited nonzero")
        OVERALL_FAIL=1
    fi
    rm -f "$out"
}

# ---------------------------------------------------------------
# mem_per_row_e2e gate: parse "<label> ... in-mem≈ NNNN B/row" lines
# from the test's --nocapture output and compare each schema shape
# against its ceiling in bench/baselines/mem_per_row.txt.
# ---------------------------------------------------------------
check_mem_per_row_baseline() {
    local out_file="$1"

    if [ ! -f "$BASELINE_FILE" ]; then
        log "FATAL: baseline file missing: $BASELINE_FILE"
        return 1
    fi

    local ok=0
    # Lines look like:
    #   real-world (200 col)   cols=200 wire≈2349B  in-mem≈  6570 B/row  (Δrss ...)
    while IFS= read -r line; do
        # Extract the label (everything before the first run of 2+ spaces).
        local label bytes_per_row shape_key ceiling observed_int ceiling_int
        label="$(printf '%s' "$line" | sed -E 's/^([A-Za-z0-9()_ .-]+[a-z0-9)])[[:space:]]{2,}.*/\1/')"
        bytes_per_row="$(printf '%s' "$line" | grep -oE 'in-mem≈[[:space:]]*[0-9]+' | grep -oE '[0-9]+')"
        [ -z "$bytes_per_row" ] && continue

        shape_key="$(printf '%s' "$label" \
            | tr '[:upper:]' '[:lower:]' \
            | sed -E 's/[()]//g' \
            | sed -E 's/[[:space:]]+/-/g' \
            | sed -E 's/-+$//')"

        ceiling="$(grep -E "^${shape_key}=" "$BASELINE_FILE" | head -1 | cut -d= -f2)"
        if [ -z "$ceiling" ]; then
            log "  WARN: no baseline ceiling for shape '$shape_key' (label: '$label') — cannot gate this shape."
            ok=1
            continue
        fi

        # Integer compare (bash has no float math); ceiling has one
        # decimal place in the baseline file, so compare in tenths.
        observed_int=$((bytes_per_row * 10))
        ceiling_int="$(printf '%s' "$ceiling" | awk '{printf "%d", $1 * 10}')"

        if [ "$observed_int" -le "$ceiling_int" ]; then
            log "  OK   $shape_key: observed=${bytes_per_row} B/row  ceiling=${ceiling} B/row"
        else
            log "  FAIL $shape_key: observed=${bytes_per_row} B/row EXCEEDS ceiling=${ceiling} B/row"
            ok=1
        fi
    done < <(grep -E 'in-mem≈' "$out_file" || true)

    if ! grep -qE 'in-mem≈' "$out_file"; then
        log "FATAL: mem_per_row_e2e produced no 'in-mem≈' lines — output format changed or test didn't run to completion."
        return 1
    fi

    return "$ok"
}

for row in "${TESTS[@]}"; do
    IFS='|' read -r key crate test_bin filter <<< "$row"
    run_one "$key" "$crate" "$test_bin" "$filter"
done

# ---------------------------------------------------------------
# 4. Summary table.
# ---------------------------------------------------------------
hr
log "NIGHTLY TEST SUMMARY"
hr
printf '%-28s %-6s %s\n' "TEST" "STATUS" "DETAIL" >&2
printf '%-28s %-6s %s\n' "----" "------" "------" >&2
for i in "${!RESULT_KEYS[@]}"; do
    printf '%-28s %-6s %s\n' "${RESULT_KEYS[$i]}" "${RESULT_STATUS[$i]}" "${RESULT_DETAIL[$i]}" >&2
done
hr

{
    printf '%-28s %-6s %s\n' "TEST" "STATUS" "DETAIL"
    printf '%-28s %-6s %s\n' "----" "------" "------"
    for i in "${!RESULT_KEYS[@]}"; do
        printf '%-28s %-6s %s\n' "${RESULT_KEYS[$i]}" "${RESULT_STATUS[$i]}" "${RESULT_DETAIL[$i]}"
    done
} > "$SUMMARY_TXT"

# Persist the summary somewhere the caller (CI workflow) can pick up
# as an artifact, without clobbering if the dir doesn't exist yet.
mkdir -p "$ROOT/target/nightly-tests"
cp "$SUMMARY_TXT" "$ROOT/target/nightly-tests/summary.txt"
log "Summary written to target/nightly-tests/summary.txt"

if [ "$OVERALL_FAIL" -ne 0 ]; then
    log "NIGHTLY LANE: FAILED"
    exit 1
fi
log "NIGHTLY LANE: PASSED"
exit 0

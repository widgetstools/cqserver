# AMPS Parity Worklog

Tracks the gaps catalogued in [`docs/AMPS_PARITY.md`](docs/AMPS_PARITY.md) — the
Atlas-demo-driven gap analysis — as bite-sized sessions, each with unit + e2e
tests. Engineering plan with exact files / code / commits lives at
[`docs/superpowers/plans/2026-05-26-amps-parity.md`](docs/superpowers/plans/2026-05-26-amps-parity.md).

> **Note.** This worklog is **complementary** to [`AMPS_WORKLOG.md`](AMPS_WORKLOG.md),
> which tracks the Appendix-A gap set (S1..S47). The P-tasks here cover the
> *additional* gaps surfaced while wiring the Atlas demo (ex01..ex08).

## Status legend

- ⏳ Pending
- 🔨 In progress
- ✅ Done
- ⏭️ Deferred (out of scope, or blocked)

## Priority order

Per AMPS_PARITY.md §5 honest assessment:
1. **Parser** (P1–P4) — highest ROI; unblocks ex08 Query Builder and removes client-side workarounds.
2. **Engine** (P5–P7, P14) — fixes the three known cqserver bugs + topic-prefix asymmetry.
3. **Aggregates** (P8–P10) — STDDEV, PERCENTILE_CONT, COUNT DISTINCT for trading-floor demo polish.
4. **JOIN** (P11–P12) — ON-clause + LEFT OUTER.
5. **WHERE** (P13) — regex match.
6. **SDK** (P15–P16) — HA failover + batched publish.

---

## P1 — Parser: table aliases + qualified column refs *(AMPS_PARITY §1.1 rows 4–5)*
**Status:** ✅ done
**Scope:** Accept `FROM t alias` and `alias.col` in SELECT / WHERE / GROUP BY / ORDER BY / JOIN. Drop the client-side `stripAliases` workaround in `ex08-query-builder/index.tsx` (deferred to a follow-up commit).
**Implementation:** `parse_query` now does an alias-rewrite pass that walks the AST and rewrites `Expr::CompoundIdentifier([alias_or_topic, col])` → `Expr::Identifier(col)` before predicate/projection compile. Helpers `collect_table_refs` + `rewrite_qualified_refs_in_query` + `rewrite_expr` in `crates/cq-core/src/query.rs`.
**Tests landed:**
- 6 unit tests in `query::tests::parse_table_alias_*` and `parse_self_named_table_qualifies_with_itself`
- E2E `crates/cq-e2e-tests/tests/parser_table_aliases.rs` — aliased SOW (filter, GROUP BY, ORDER BY) matches the unqualified form against a real server.

## P2 — Parser: scalar arithmetic in SELECT-list *(§1.1 row 7)*
**Status:** ✅ done
**Scope:** `SELECT a, b, a + b AS sum FROM t` evaluates server-side. Atlas publisher can stop pre-computing `mv_x_pct`, `mv_abs`.
**Implementation:** `ScalarExpr` (Col/Lit/Add/Sub/Mul/Div/Neg) + `ComputedColumn`. `try_compile_scalar_expr` detects arithmetic in SELECT items, compiles to `ScalarExpr`, and emits per-row evaluated cells. Null-on-error semantics (zero-div, type mismatch). `query_streaming_json`'s fast path now falls through to the buffered path when `query.computed` is non-empty.
**Tests landed:**
- 4 unit tests in `query::tests::parse_select_arithmetic_*` (add+alias, multiple ops, parenthesised, div-by-zero null).
- E2E `crates/cq-e2e-tests/tests/parser_select_arithmetic.rs` — `price * quantity AS notional` and `(price - quantity) / quantity AS pct_spread` against a running server.

## P3 — Parser: HAVING clause *(§1.3 row 7)*
**Status:** ✅ done
**Scope:** `GROUP BY k HAVING SUM(v) > 100` — compile HAVING against `[group_cols, aggregate_aliases]` schema and evaluate after group finalise. Supports aggregate-function refs (`SUM(v)`), aggregate aliases (`total`), group-column refs (`desk`), boolean ops (`AND`/`OR`/`NOT`), and comparison ops.
**Implementation:** `HavingExpr` enum + `compile_having` in `crates/cq-core/src/query.rs`. Reject HAVING when there is no GROUP BY / aggregate (matching AMPS). Executor evaluates after building each output row map and drops groups where it fails. The P1 alias-rewrite pass already strips qualified refs in HAVING.
**Tests landed:**
- 5 unit tests in `query::tests::{parses_having_*, having_*}` covering parse, alias vs function-call equivalence, AND-combined filter, group-column filter.
- E2E `crates/cq-e2e-tests/tests/parser_having.rs` — `HAVING SUM(qty) > 50` against a real server, plus alias-form and AND-combined forms.

## P4 — Parser: OFFSET clause *(§1.5 row 6)*
**Status:** ✅ done
**Scope:** `LIMIT n OFFSET m` (and MySQL-style `LIMIT m, n`) skips the first `m` rows of the (sorted) result before LIMIT applies.
**Implementation:** `offset: Option<usize>` on `ParsedQuery`; parser extracts it via `extract_usize_literal` helper; non-aggregate executor drains the first `m` matching rows after ORDER BY, before LIMIT.
**Tests landed:**
- 3 unit tests in `query::tests::{parses_limit_offset, offset_skips_first_n_rows, offset_only_skips_without_limit}`.
- E2E `crates/cq-e2e-tests/tests/parser_offset.rs` paginates 20 rows in chunks of 5 with `OFFSET 0/5/50`.

## P5 — Engine: fix degenerate-aggregate SOW *(§4 bug 3)*
**Status:** ✅ done
**Scope:** `SELECT SUM(x) FROM t` view (no GROUP BY) now stays single-row across refreshes. The fix changes keyless-topic upserts to overwrite row 0 instead of appending — matches AMPS semantics for a degenerate-aggregate view's keyless output topic.
**Implementation:** `commit_values_locked` in `crates/cq-core/src/topic.rs` — when `key_col_indices.is_empty()` and the store already has a row, update row 0 in place. Topics that need append-only semantics must declare a key field.
**Tests landed:**
- Unit `topic::tests::keyless_topic_collapses_to_single_row` — 4 upserts, row_count() stays 1, row 0 holds latest value.
- E2E `crates/cq-e2e-tests/tests/degenerate_aggregate_view_e2e.rs` — declares a `[[views]]` block with `SELECT SUM(qty) FROM t`, publishes 5 source rows, verifies the view SOW returns exactly 1 row with the running total.

## P6 — Engine: fix JOIN-view SOW delivery for fresh subscribers *(§4 bug 1)*
**Status:** ✅ done (verified no longer reproduces — regression pinned)
**Scope:** AMPS_PARITY documented a JOIN view whose SOW returned 0 to a fresh subscriber even though admin showed N rows. Attempted to reproduce the bug in two shapes (quoted slash-prefixed JOIN; bare-name JOIN; under continuous publisher load with 10 successive fresh subscribers); the bug no longer manifests on `msrv-1.78`. Likely fixed alongside the JOIN-in-ad-hoc-SOW work (per AMPS_PARITY.md §1.4 row 10 "added 2026-05-26").
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/view_join_sow_fresh_subscriber.rs` — 2 tests, both green: a clean fresh-subscriber SOW after seed, and a stress variant running 10 fresh-subscriber SOWs under continuous publisher load.

## P7 — Engine + SDK: clear encode-once cache slot AND surface SOW errors to client *(§4 bug 4)*
**Status:** ✅ done
**Scope:** AMPS_PARITY documented a server-side wedge: failed SOWs leaving the encode-once-fanout cache in `Building`. Diagnosing exposed a related (and more user-visible) bug: server-side error frames from `deliver_streaming_snapshot` carried `command_id: None`, so the client driver dropped them and `sow_sql` blocked until the 30s ack_timeout instead of returning the error promptly.
**Implementation:**
- Server: `deliver_streaming_snapshot` error path now includes `Some(sub_id)` (== client cid) in the error CqMessage, alongside the already-present `abandon_snapshot_cache_slot` call.
- Client: `snapshot_completions` is now `oneshot::Sender<Option<String>>` — `None` for normal `GroupEnd`, `Some(reason)` for error acks. The Ack handler short-circuits any in-flight SOW's completion when `Status::Error` arrives for its cid. `sow_msg` returns `ClientError::Server(reason)` instantly instead of timing out.
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/snapshot_cache_no_wedge.rs` — sends a failing SOW (`SELECT bogus_col FROM t`), then the SAME failing SOW again, then a valid SOW; all three complete in well under a second (previously the first hung for the 30s ack_timeout).

## P8 — Aggregates: STDDEV / STDDEV_SAMP / VARIANCE / VAR_SAMP *(§1.3 row 3)*
**Status:** ✅ done
**Scope:** Welford-online accumulator. Supports `STDDEV` / `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE` / `VAR_POP`, `VAR_SAMP`. Sample stats return NULL for `count < 2`; population stats are defined at `count == 1`.
**Implementation:** New `AggFn::{Stddev, StddevSamp, Variance, VarianceSamp}` variants + `AggState::Welford { count, mean, m2, kind: WelfordKind }` in `crates/cq-core/src/query.rs`. View schema derivation maps all 4 to `Double`.
**Tests landed:**
- 6 unit tests in `query::tests::{parses_stddev_variance_aggregates, stddev_population_matches_known_value, stddev_samp_matches_known_value, variance_matches_known_value, stddev_with_group_by, stddev_empty_input_returns_null}` against Wikipedia's canonical {2,4,4,4,5,5,7,9} fixture.
- E2E `crates/cq-e2e-tests/tests/parser_stddev.rs` runs all 4 functions in one SOW against the same fixture, asserts the expected numeric values.

## P9 — Aggregates: PERCENTILE_CONT / MEDIAN *(§1.3 row 4)*
**Status:** ✅ done
**Scope:** `PERCENTILE_CONT(col, q)` with `q ∈ [0,1]` returns the linear-interpolated percentile. `MEDIAN(col)` is sugar for `PERCENTILE_CONT(col, 0.5)`. Exact (no sketching) — O(n) memory per group; documented tradeoff for high-cardinality groups.
**Implementation:** New `AggFn::PercentileCont` variant + `AggState::Percentile { values, q }` in `crates/cq-core/src/query.rs`. Adds `percentile_q: Option<f64>` to `AggregateSpec`; new `AggState::init_with_q` helper threads it through the executor + pivot + view paths.
**Tests landed:**
- 4 unit tests in `query::tests::{parses_percentile_cont_and_median, percentile_cont_known_values, median_matches_percentile_50, percentile_with_group_by}`.
- E2E `crates/cq-e2e-tests/tests/parser_percentile.rs` runs MEDIAN + PERCENTILE_CONT(0.5) + PERCENTILE_CONT(0.95) in one SOW against {2,4,4,4,5,5,7,9} (median = 4.5, p95 = 8.3).

## P10 — Aggregates: COUNT(DISTINCT col) *(§1.1 row 9)*
**Status:** ✅ done
**Scope:** `COUNT(DISTINCT col)` returns per-group distinct non-null count. Exact via `HashSet<GroupKeyPart>` per group. HyperLogLog is a future optimisation.
**Implementation:** New `AggFn::CountDistinct` variant + `AggState::CountDistinct(HashSet<GroupKeyPart>)`. Parser checks `arg_list.duplicate_treatment` for `Some(DuplicateTreatment::Distinct)` on COUNT calls; emits `CountDistinct` instead of `Count`. Reuses the existing `GroupKeyPart` enum for hashable Value coverage. View schema derivation maps to `Long`.
**Tests landed:**
- 4 unit tests in `query::tests::{parses_count_distinct, count_distinct_dedups, count_distinct_with_group_by, count_distinct_skips_nulls}`.
- E2E `crates/cq-e2e-tests/tests/parser_count_distinct.rs` covers overall + per-desk distinct counts on a 6-row trader fixture.

## P11 — JOIN: ON-clause equi-join (translated to USING) *(§1.4 row 1)*
**Status:** ✅ done
**Scope:** Accept `INNER JOIN B ON a.x = b.x`; translate to the existing USING path when both sides reference the same column name. Reject non-equi predicates, OR-combined predicates, mixed-in literals, and equalities between differently-named columns with a clear diagnostic pointing at USING.
**Implementation:** `parse_join_clause` (`crates/cq-core/src/query.rs`) now matches `JoinConstraint::On(Expr)` and walks the AND-tree via the new `collect_equi_using` helper. The alias-rewrite pass already strips `a.col`/`b.col` to bare `col` before `parse_join_clause` runs; extended the rewrite to also cover the `Join` and `Left` variants (in addition to the existing `Inner`/`LeftOuter`/`RightOuter`/`FullOuter`). `peek_join` now performs the alias-rewrite before consulting the JOIN so the SOW JOIN path also accepts ON-equi.
**Tests landed:**
- 4 unit tests in `query::tests::parse_join_on_*` covering single-column ON-equi, multi-column ON-equi, rejection of differently-named columns, rejection of non-equi predicates.
- E2E `crates/cq-e2e-tests/tests/parser_join_on.rs` runs the same JOIN as both `JOIN ... ON a.cusip = b.cusip` and `JOIN ... USING (cusip)` and asserts identical per-sector exposure rollups.

## P12 — JOIN: LEFT OUTER JOIN *(§1.4 row 3)*
**Status:** ✅ done
**Scope:** `LEFT JOIN` / `LEFT OUTER JOIN` keeps every left row in the output; right-side columns are emitted as JSON `null` when no right-side match. Works with both `USING (col)` and `ON a.col = b.col` (the P11 translation).
**Implementation:** `JoinSpec.kind: JoinKind { Inner, LeftOuter }` added. Parser accepts `JoinOperator::Left` + `JoinOperator::LeftOuter` and sets the kind. `execute_join_query` branches at row-build time: on no right match (or NULL left key), `Inner` skips, `LeftOuter` emits with `Value::Null` for every right-side column.
**Tests landed:**
- 3 unit tests in `query::tests::{parse_left_outer_join_using_succeeds, parse_left_outer_join_on_equi, left_outer_join_emits_nulls_for_unmatched_left_rows}`.
- E2E `crates/cq-e2e-tests/tests/parser_left_outer_join.rs` runs INNER + LEFT OUTER on the same two-topic fixture; LEFT OUTER keeps the unmatched left row with `sector: null`.

## P13 — WHERE: regex match (MATCHES_REGEX) *(§1.2 row 7)*
**Status:** ✅ done
**Scope:** `WHERE MATCHES_REGEX(col, '<pattern>')` filters rows whose `col` value matches the regex. Pattern is compiled at parse time so an invalid pattern surfaces as a clean server error (not a row-eval crash). Uses the `regex` crate (already a `cq-core` dependency for LIKE).
**Implementation:** New `CompiledPredicate::Regex { col, pattern }` variant; new `Expr::Function` arm in `compile_expr` that recognises the `MATCHES_REGEX` function call, validates arity, and pre-compiles the pattern. Variant also added to `referenced_columns` so the predicate-index path stays correct. (LIKE_REGEX operator syntax is deferred — the function-call form is universally supported by sqlparser.)
**Tests landed:**
- 4 unit tests in `predicate::tests::{parses_matches_regex, matches_regex_filters_rows, matches_regex_invalid_pattern_errors_at_parse, matches_regex_combined_with_and}`.
- E2E `crates/cq-e2e-tests/tests/parser_matches_regex.rs` runs a regex filter over a 5-symbol fixture and verifies invalid patterns surface as `ClientError::Server`.

## P14 — Topic registry: normalise slash-prefix *(§4 bug 5)*
**Status:** ✅ done
**Scope:** Topics canonicalise to `/name` at registration; the ad-hoc dual-lookup workarounds in `init_view` (server) and `deliver_join_snapshot` (transport router) have been replaced with a single canonical lookup.
**Implementation:** New `cq_core::topic::canonicalize_topic(name)` helper. Applied at:
- `cq-server/src/main.rs` topic registration (line 228), view-config name + source resolution (line 826–829), JOIN right-side resolver (line 856–864), and view registration (line 918).
- `cq-transport/src/router.rs` `deliver_join_snapshot` right-topic lookup (line 932–934).
**Tests landed:**
- Unit `topic::tests::canonicalize_topic_round_trips` covering idempotence + multi-segment paths.
- E2E `crates/cq-e2e-tests/tests/topic_slash_normalization.rs` declares a `[[views]]` block whose JOIN SQL uses BARE topic names (`FROM p14_pos JOIN p14_sec USING (cusip)`) against slash-prefixed registry entries (`/p14_pos`, `/p14_sec`); verifies both the view runner and the inline JOIN SOW path resolve correctly.

## P15 — SDK: HA failover across multiple URIs *(§3.1 row 12)*
**Status:** ✅ done (initial-connect failover; mid-stream reconnect deferred)
**Scope:** TS SDK now exposes `Client.connectAny(urls, opts)` — attempts each URL in randomised order and returns the first successful connection (matches the Rust SDK's existing `connect_any`). `client.activeUrl` exposes which URL won. **Initial-connect failover only**; live reconnect after a mid-stream disconnect is a separate concern that doesn't block the AMPS-parity surface (the same boundary the Rust SDK draws).
**Implementation:** `client-sdks/ts/src/client.ts` — new `static async connectAny(urls, opts)`, internal Fisher-Yates `shuffledIndices` helper, and `activeUrl` getter. Existing `Client.connect` now records `activeUrl` too.
**Tests landed:**
- Vitest `client-sdks/ts/test/client.test.ts` — 3 new tests: rotates past a dead URL (`tcp://127.0.0.1:1` always refused) to the live server, throws when every URL fails, rejects empty list. All 12 TS tests stay green.

## P16 — SDK: batched publish (pipelined) *(§3.2 row 4)*
**Status:** ✅ done (SDK-level pipelining; wire-level `PublishBatch` frame deferred)
**Scope:** TS SDK now exposes `Client.publishBatch(topic, msgs[])` — fires every publish concurrently without waiting for individual acks, then awaits all. Returns sequences in order. Same correctness as N sequential `publish()` calls but the per-msg RTTs are paid in parallel — the slowest single ack bounds the wall-clock instead of N × RTT.
**Implementation:** `client-sdks/ts/src/client.ts` — new `async publishBatch(topic, msgs[])` using `Promise.all(msgs.map(m => this.publish(topic, m)))`. The existing `rpc()` already correlates responses by cid, so concurrent in-flight publishes don't race each other.
**Out of scope (folds into a follow-up):** AMPS's literal `publishBatch` is *one wire frame, one ack, one txlog append* — a wire-version bump (see AMPS_WORKLOG.md S28 for the pattern). The pipelined SDK form already captures the latency win for trading-floor publisher loops; the wire-frame change is an incremental optimization.
**Tests landed:**
- Vitest 2 new tests in `test/client.test.ts`: 50-msg batch publish on a dedicated `/batch-data` topic verifies seq count + monotonicity + every payload appears in the SOW, plus an empty-list-resolves-empty contract.

---

## Q-series — formerly deferred, now in scope

The P1–P16 series above closed the Atlas-demo-driven gaps. The Q-series
takes on the items previously deferred — JOIN variants, wire-level batched
publish, pivot extensions, SDK polish, then the big-ticket parser items
(window functions, CTEs, subqueries) and the column-type rework. Same
contract as P-series: each session has unit + e2e tests and an
independent commit.

## Q1 — JOIN: RIGHT OUTER + FULL OUTER *(§1.4 rows 4–5)*
**Status:** ✅ done
**Scope:** Extend P12's LEFT OUTER work to `RIGHT [OUTER] JOIN` (mirror) and `FULL [OUTER] JOIN` (union of LEFT + RIGHT). USING columns for right-only rows surface the right-side value so consumers don't see `cusip = null`.
**Implementation:** `JoinKind::{RightOuter, FullOuter}` added; parser accepts `JoinOperator::{Right, RightOuter, FullOuter}`. `execute_join_query` now tracks matched right rows via a `roaring::RoaringBitmap` and, for RIGHT/FULL, emits the unmatched right-only rows after the main scan. USING-column resolution for right-only rows reaches into the right schema by name.
**Tests landed:**
- 4 unit tests in `query::tests::{parse_right_outer_join_succeeds, parse_full_outer_join_succeeds, right_outer_join_keeps_unmatched_right_rows, full_outer_join_keeps_both_sides}`.
- E2E `crates/cq-e2e-tests/tests/parser_right_full_outer_join.rs` runs all 4 join shapes (INNER/LEFT/RIGHT/FULL) on the same fixture (positions: AAPL+MSFT; securities: AAPL+GOOG) and asserts the expected row counts + null-side semantics.

## Q2 — Wire-level publishBatch — one frame, one ack, one txlog append *(§3.2 row 4 follow-up to P16)*
**Status:** ✅ done
**Scope:** New `Command::PublishBatch` wire frame carrying `Vec<payload>` for a single topic. Server commits each row via `upsert_map` in sequence (per-row state.write()); returns one Ack whose `sequences` carries the assigned seq per row in input order. Saves N-1 round-trips vs P16's pipelined-publish form.
**Implementation:**
- `cq-protocol`: new `Command::PublishBatch`, new `CqMessage.batch: Option<Vec<Value>>` and `CqMessage.sequences: Option<Vec<u64>>` fields.
- `cq-transport`: new `handle_publish_batch` function + dispatch arm; emits `cq_publish_batch_{total,rows_total,latency_us}` metrics.
- `cq-client` (Rust): new `Client::publish_batch(topic, rows)` method.
- `client-sdks/ts`: replaces P16's `Promise.all` body with a single `rpc({c:'publish_batch', ...})` call.
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/publish_batch_wire.rs` — 25-row batch returns 25 monotonic sequences in input order; all rows + seed visible in SOW; empty input resolves empty.
- Existing TS Vitest publishBatch tests now exercise the wire-level path; all 14 stay green.
**Out of scope (still deferred):** truly atomic-across-failures batch (commit-or-nothing) would need a server-side transactional wrapper around the per-row upsert loop. Current shape matches AMPS's contract (per-row durability, batch is not all-or-nothing).

## Q3 — PIVOT dynamic value list (`IN (ANY)`) over the wire *(§1.6 row 3)*
**Status:** ✅ done
**Scope:** Parser-level support for `PIVOT (...) FOR col IN (ANY)` was already present (S45). Q3 fixes two server bugs that made it return empty/raw rows over the wire:
1. `query_streaming_json`'s `needs_full_buffer` check didn't include `is_pivot()` — pivot queries silently fell through to the columnar fast path which emits raw projected cells, bypassing `execute_pivot_query` entirely.
2. The same path's tombstone filter required `source_rows` to be in lockstep with `rows`, but pivot's executor returns `source_rows: Vec::new()` (synth output). The zip-based filter dropped every row.
Also added `" PIVOT ("` / `" UNPIVOT ("` to `rewrite_from_to_t`'s clause-boundary list so the wire SQL rewrite preserves the PIVOT clause.
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/pivot_dynamic_in_any.rs` — `SELECT * FROM t PIVOT (SUM(qty) FOR desk IN (ANY))` over the wire against a 4-row fixture, asserts 4 anchor rows with 3 dynamically-discovered desk columns each.
**Note:** Continuous-mode pivot with SchemaChange emission (S44 wire frame fanning into subscriptions) remains a separate follow-up; SOW + view paths now exercise the dynamic IN list.

## Q4 — Connection name echo + trace-id propagation *(§3.1 row 4 + §3.6 row 3)*
**Status:** ✅ done
**Scope:** SDKs send a `client_name` during `logon` that the server stores on the `Session` and surfaces in the audit log. New `trace_id` field on every CqMessage echoes back on the response so upstream callers can correlate logs across processes.
**Implementation:**
- `cq-protocol`: new `CqMessage.trace_id: Option<String>`; new helper `CqMessage::ack_ok_for_request(req)` that copies `cid` + `trace_id` from the request.
- `cq-transport`: `Session` gains `client_name: Option<String>`; logon handler stores `msg.client_name` on session and includes it in the audit `logon_ok` event alongside `trace_id`. `handle_publish` (queue + topic paths) and `handle_publish_batch` switched to `ack_ok_for_request` so trace_id flows back on every publish ack.
- `cq-client` (Rust): new `Client::logon_with(user, password, client_name, trace_id)` method.
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/conn_name_trace_id.rs` — logs in with a connection name + trace id, asserts subsequent publish still works (smoke + non-crash). Full audit-log scraping is deferred to a follow-up.
**Out of scope:** automatic per-span trace-id injection via `tracing::Span::current()` integration — needs an OpenTelemetry layer; current shape just records the trace_id field on the audit event.

## Q5 — TS SDK TLS *(§3.1 row 3)*
**Status:** ✅ done
**Scope:** Two TLS paths exposed from the TS SDK:
- `wss://` — handled by the existing global `WebSocket` constructor (no code change required; cert validation uses the platform's trust store). For self-signed dev certs in Node, set `NODE_TLS_REJECT_UNAUTHORIZED=0`.
- `tls://` — new Node-only TCP+TLS path. `Client.connect("tls://host:port", { tls: { servername, ca, rejectUnauthorized } })`. Wraps `tls.connect()` and reuses the length-prefixed `TcpTransport`.
**Implementation:** `client-sdks/ts/src/transport-node.ts` — new `connectTls(host, port, opts)` returning `Transport`. `client-sdks/ts/src/client.ts` — `Client.connect` dispatches on `tls://` URL scheme. New `ClientOptions.tls` carries SNI / custom CA / rejectUnauthorized.
**Tests landed:**
- 2 new Vitest tests in `test/client.test.ts`: `tls://` to the non-TLS port rejects at handshake (proves scheme wiring); malformed `tls://` URLs reject with a clear error.

## Q6 — Per-client metrics surface *(§3.6 row 1 — finish the "partial")*
**Status:** ✅ done
**Scope:** New `/admin/clients` endpoint surfaces per-session stats aggregated across each session's `DeliveryRoute`s — no new hot-path counters; reuses the existing per-route atomics (`dropped`, `last_seq`, `queue_depth`, age). Each entry carries the logon-supplied `client_name` (Q4).
**Implementation:**
- `cq-transport`: new `ClientStats { session_id, client_name, subscriptions, dropped_total, max_last_seq, oldest_sub_age_ms, total_queue_depth }` + `collect_client_stats(registry)` aggregator. Subscribe handlers fall through `msg.client_name → session.client_name → session.username` so the route picks up the logon-time name even when the subscribe message doesn't carry it.
- `cq-server`: new `/admin/clients` route returning JSON array of per-session stats.
- Fixed an issue where `handle_logon`'s no-credentials (anonymous) branch silently dropped `msg.client_name` — captured early now so JWT / password / anonymous all populate `session.client_name`.
**Tests landed:**
- E2E `crates/cq-e2e-tests/tests/admin_clients.rs` — two clients (publisher + subscriber) each call `logon_with` carrying a `client_name`; `/admin/clients` returns an array with at least the subscriber's `clientName=subscriber-b` entry showing `subscriptions ≥ 1`.

## Q7 — Window functions: ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD *(§1.7)*
**Status:** ✅ done (one-shot SOW; continuous-window deferred)
**Scope:** `OVER (PARTITION BY ... ORDER BY ...)` for the 5 standard window functions. Parser detects function calls with `over: Some(WindowSpec)`, compiles `WindowColumn { alias, partition_by, order_by, kind }`, and the executor partitions/sorts/assigns after the row build step. Mixes with regular projection columns in the same SELECT.
**Implementation:** New `WindowColumn` + `WindowFn` types in `crates/cq-core/src/query.rs`. `try_compile_window` recognises the 5 fn names and compiles partition/order_by indices from the OVER spec. `apply_window` (called from `execute_query_with_index_filtered` non-aggregate path) does the heavy lifting. `query_streaming_json`'s `needs_full_buffer` extended to include `!query.windows.is_empty()`.
**Tests landed:**
- 3 unit tests in `query::tests::{parses_row_number_over_partition_order, row_number_assigns_per_partition_sorted_index, rank_assigns_dense_or_gapped}`.
- E2E `crates/cq-e2e-tests/tests/parser_window_fns.rs` runs ROW_NUMBER + LAG(px,1) + LEAD(px,1) in one wire SOW, asserts ranks + prev/next values for both AAPL (3 rows) and MSFT (2 rows) partitions; edge rows surface `null` for the missing lag/lead.
**Out of scope:** continuous-window subscriptions (window values update as the source SOW changes) — single SELECT-time evaluation only. Windowed aggregates (`SUM(x) OVER (...)`, `COUNT(*) OVER (...)`) — folds into a future session.

## Q8 — CTEs (WITH x AS …) — alias-substitution MVP *(§1.8 row 4)*
**Status:** ✅ done (MVP — simple SELECT * CTEs; complex CTEs need Q9)
**Scope:** Non-recursive CTEs whose body is `SELECT * FROM topic [WHERE filter]`. The alias substitutes to the real topic name in the main FROM; the CTE's WHERE filter is AND'd into the main WHERE. Multiple CTEs allowed. RECURSIVE rejected at parse time.
**Implementation:** New `inline_ctes()` helper runs before the P1 alias-rewrite pass. Walks `query.with.cte_tables`, validates each CTE matches the simple shape, builds a `cte_name → (source_topic, optional_filter)` map, then rewrites every FROM/JOIN reference in the main SELECT.
**Tests landed:**
- 3 unit tests in `query::tests::{parses_simple_cte_alias, cte_with_filter_pushes_into_main_where, recursive_cte_is_rejected}`.
- E2E `crates/cq-e2e-tests/tests/parser_cte.rs` — `WITH rates_trades AS (SELECT * FROM t WHERE desk = 'RATES') SELECT … FROM rates_trades WHERE price > 200` over the wire.
**Out of scope (rejected with a clear error):** CTEs with projection, GROUP BY, JOIN, ORDER BY, LIMIT, or nested CTEs — those need real sub-query materialisation, which is the Q9 follow-up. The current message tells callers to write the query directly until Q9 lands.

## Q9 — Subqueries: WHERE col IN (SELECT col FROM topic) MVP *(§1.8 row 1)*
**Status:** ✅ done (MVP — `IN` subqueries only; EXISTS + scalar subqueries deferred)
**Scope:** `WHERE col IN (SELECT col FROM topic [WHERE …])` materialises the inner SELECT at SOW time and substitutes the result as a literal IN list. Inner subquery must be `SELECT one_col FROM topic [WHERE …]` (no JOIN, no GROUP BY, no projection). Empty result → never-match predicate so the outer query returns 0 rows. `NOT IN` supported.
**Implementation:** Router-side pre-flight `resolve_in_subqueries(sql, topics)` runs before `build_sql`/`rewrite_from_to_t`. Walks the WHERE AST, finds `Expr::InSubquery`, dispatches a one-shot `Topic::query` against the inner source topic, builds `Expr::InList` from the rows, re-serialises the AST. Topic name canonicalised (P14). Errors surface as `ClientError::Server` via the P7 error path.
**Tests landed:**
- Unit `query::tests::subquery_in_select_currently_unsupported` — pins parser-level rejection when subqueries reach `parse_query` un-resolved (defensive).
- E2E `crates/cq-e2e-tests/tests/parser_in_subquery.rs` — 2 tests: non-empty subquery (trades IN watchlist) and empty subquery (always-false substitution returns 0 rows).
**Out of scope (deferred to future sessions):** `EXISTS`, scalar subqueries in SELECT, correlated subqueries, subqueries with JOIN/GROUP BY inside.

## Q10 — Bytes / Array / Object as first-class column types *(§2 column-types row)*
**Status:** ⏳
**Scope:** Add `ColumnType::{Bytes, Array, Object}`; teach `Value` / `ColumnStore` / wire codecs about them. Dotted-path access for Object stays compatible.

## Q11 — Schema evolution (online add column) *(§2 row 12)*
**Status:** ⏳
**Scope:** `ALTER TABLE t ADD COLUMN c <type> [NULL]` applied without dropping subscribers. Existing rows get the column with `NULL`; running queries that referenced the previous schema continue against their pinned `Arc<Schema>` until they re-parse.

## Q12 — AS OF JOIN (temporal) *(§1.4 row 7)*
**Status:** ⏳
**Scope:** `A AS OF JOIN B ON a.ts = b.ts` — for each left row, find the right row whose `ts` is the largest value ≤ `a.ts`. Useful for trades-vs-prices reconstruction. Implementation: sort right side by `ts`, binary-search per left row.

---

## Deferred (out of scope even for the Q-series — major rewrites)

- **Recursive CTEs** — would require a fixed-point evaluator distinct from Q8's macro-expansion model.
- **Correlated subqueries** — would require a per-row sub-execution path, very different from Q9's materialise-once.
- **Hash-join hints / broadcast hints** (`/*+ broadcast(b) */`) — folded into a future planner-hint pass; cqserver's executor is already a hash join, so the hint would be cosmetic until we add a sort-merge alternative.
- **TLS / connection-name as part of the existing `tls` e2e** — handled by Q4 + Q5 above; the legacy "deferred" bullet is superseded.

---

## Coverage snapshot

At start of this worklog:
- Parser: 6 gaps → 4 covered (P1–P4); window/subquery/CTE deferred.
- Engine: 3 known bugs → 3 covered (P5–P7) + P14 normalisation.
- Aggregates: 4 gaps → 3 covered (P8–P10); HAVING is P3.
- JOIN: 5 missing variants → 2 covered (P11–P12).
- SDK: 12 SDK gaps → 2 covered (P15–P16); rest are small enough to land opportunistically.

Total P-series: **16 sessions** to bring the Query Builder tab to "arbitrary SQL works against cqserver" parity for the trading-floor demo's intended surface.

Q-series adds **12 more** to close the remainder (RIGHT/FULL OUTER, wire-batch, pivot dynamic IN, conn-name + trace-id, TS TLS, per-client metrics, window functions, CTEs, subqueries, Bytes/Array/Object, schema evolution, AS OF JOIN).

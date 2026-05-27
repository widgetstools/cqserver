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

## Q10 — Bytes column type *(§2 column-types row — partial; Array/Object deferred)*
**Status:** ✅ done (Bytes only; Array + Object deferred to follow-up)
**Scope:** New `ColumnType::Bytes` variant with full `Value::Bytes(Option<Vec<u8>>)` integration. JSON wire form is base64 (input + output). Supports equality (via base64-string comparison in indexes), `IS NULL`, and GROUP BY (group keys hashed by base64 string). No arithmetic, no ordering (`compare_values` returns Equal for Bytes), no IN list (rejected with clear error).
**Implementation:**
- `cq-core/schema.rs`: `ColumnType::Bytes` variant + `TypeCounts.bytes` + `compute_mappings` arm.
- `cq-core/store.rs`: `Value::Bytes(Option<Vec<u8>>)` variant; `bytes_cols: Vec<Vec<Option<Vec<u8>>>>` backing store; `from_json` decodes base64, `to_json` encodes; `is_null`, `set`, `get`, `grow`, `null_out_row`, fast-path JSON encoder all updated.
- `cq-core/sec_index.rs`: `IxKey::from_value` + `RangeKey::from_value` map Bytes to a String (base64) so equality lookups work.
- `cq-core/predicate.rs`: `IsNull`/`IsNotNull` handle bytes columns; IN list and ordered comparisons explicitly rejected.
- `cq-core/pivot.rs`: pivot on a bytes column is a no-op (returns empty / no match).
- `cq-core/query.rs`: GroupKeyPart maps bytes via base64; `compare_values` returns Equal.
- New workspace dep: `base64 = "0.22"` for encode/decode.
**Tests landed:**
- 2 unit tests in `query::tests::{bytes_column_round_trips_via_value, bytes_filter_is_null_works}` covering store round-trip + IS NULL filter.
**Out of scope (still deferred):** `Array` and `Object` as first-class column types — would each need their own backing store, JSON parsing rules, and predicate semantics (`json_extract`, `array_length`, etc.). Nested JSON continues to flatten via dotted-paths into String columns (existing behaviour).

## Q11 — Schema evolution (online add column) *(§2 row 12)*
**Status:** ✅ done
**Scope:** New `Topic::add_column(name, type)` + `POST /admin/add-column/{topic}?name=NAME&type=TYPE` endpoint. Atomically appends a column to the topic schema, preserves every existing row's column values, and initialises the new column to NULL for existing rows. New publishes can populate it immediately. Column INDICES are stable across the swap so existing parsed-query state stays valid (only the column count grows).
**Implementation:**
- `cq-core/topic.rs`: `Topic::add_column` takes `state.write()`, builds a new `Schema` with the appended column, allocates a new `ColumnStore`, replays every existing row through `commit_values_locked` (the new col defaults to `Value::Null`), and atomically swaps `state`.
- `cq-server/admin.rs`: new `/admin/add-column/{topic}?name=&type=` route. Accepts all 7 column types (Double, Long, Int, String, Bool, Timestamp, Bytes).
**Tests landed:**
- 2 unit tests in `topic::tests::{add_column_appends_with_null_default_preserving_existing_rows, add_column_rejects_duplicate_name}`. Pre-evolution rows surface with the new column absent from the projected map (cqserver's sparse-null semantics); post-evolution publishes populate it.
- E2E `crates/cq-e2e-tests/tests/admin_add_column.rs` — seed a row on the original schema, POST the admin endpoint, publish a new row with the new column populated, filter SOW by the new column.
**Out of scope (deferred):** ALTER TABLE SQL surface (route is admin-only today), DROP COLUMN, RENAME COLUMN, type-change migrations — each non-trivial under the assumption that subscribers hold pinned schema references.

## Q12 — AS OF JOIN (temporal) *(§1.4 row 7)*
**Status:** ✅ done
**Scope:** Snowflake-style `ASOF JOIN ... MATCH_CONDITION(lhs.ts >= rhs.ts) USING (key)` — for each left row, find the right row whose `ts` is the largest value ≤ left's `ts`, partitioned by USING keys. Useful for trades-vs-prices reconstruction ("what was the price at the time of the trade?").
**Implementation:**
- `JoinKind::AsOf { ts_col: String }` (new variant; JoinKind drops `Copy` since it now owns a String).
- `parse_join_clause` recognises `JoinOperator::AsOf { match_condition, constraint }`, validates `match_condition` is `lhs_col >= rhs_col` with same-named columns (after P1 alias rewrite), and stores the column name on JoinKind.
- `rewrite_qualified_refs_in_query` extended to walk `JoinOperator::AsOf`'s `match_condition` + `constraint::On` so `t.ts >= p.ts` becomes `ts >= ts` before parse_join_clause sees it.
- `execute_join_query` pre-builds `asof_index: HashMap<using_key, Vec<(ts, right_row)>>` sorted by ts. Per-left-row lookup uses `partition_point` for binary search; the matched right row is `entries[pos - 1]` (largest ts ≤ left.ts).
**Tests landed:**
- 2 unit tests in `query::tests::{parses_asof_join_with_match_condition, asof_join_matches_latest_right_le_left_ts}`.
- E2E `crates/cq-e2e-tests/tests/parser_asof_join.rs` — prices @ ts={100, 150, 250}, trade @ ts=200; ASOF correctly picks the price at ts=150.
**Out of scope (deferred):** `lhs > rhs` (strict-less) and `lhs <= rhs` (latest-after) variants — would just change the partition_point predicate. Multi-key MATCH_CONDITION (e.g. `lhs.ts >= rhs.ts AND lhs.exch = rhs.exch`) — would need to split into match-cond columns vs USING columns. Both are straightforward extensions.

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

---

## Test hardening — concurrency / property / load / differential coverage for the Q-series

Once the Q-series feature work was done, an honest audit showed that
while unit + e2e coverage existed for every feature, **stress, loom,
proptest, and differential coverage** for the new code paths was
patchy. This hardening pass closes those four gaps end-to-end.

### TH1 — Loom model for Q11 (online add-column)
**Status:** ✅ done
**File:** `crates/cq-core/tests/loom_add_column_swap.rs` (new, ~170 lines)
**Scope:** Two `loom::model!` scenarios that interleave a publisher,
an `add_column` thread, and (in one variant) a snapshot reader
across every legal `Acquire/Release` ordering. Invariants verified
under loom: rows never tear (`row.len() == col_count` at all times),
col-count stays in `{2, 3}` (initial vs post-add), per-row sequence
numbers are unique.
**Why this catches what tokio + cargo test cannot:** the production
swap uses parking_lot RwLock + the `cq_core::sync` shim; loom
explores every Release/Acquire interleaving the model permits, so a
mis-ordered `state.swap()` or a missing read-fence would be caught
on the first run instead of in production hours later.
**Run:** `RUSTFLAGS="--cfg loom" cargo test -p cq-core --test loom_add_column_swap --release`. Both models pass.

### TH2 — Property tests for Q7 (window functions)
**Status:** ✅ done
**File:** `crates/cq-core/tests/prop_window_fns.rs` (new, ~210 lines)
**Scope:** 4 `proptest!` cases generating `Vec<(partition, ord_val)>`
fixtures (1..=24 rows × 4 partitions × ord ∈ 0..=20):
- `prop_row_number_is_permutation_within_partition` — for every
  partition, `ROW_NUMBER()` values are exactly `1..=|partition|`.
- `prop_rank_monotonic_and_tie_safe` — RANK never decreases as you
  walk a partition in order, equal `ord` rows share a rank.
- `prop_dense_rank_no_gaps` — DENSE_RANK max == count of distinct
  `ord` values within the partition.
- `prop_lag_yields_prev_row_per_partition` — LAG(col) collected per
  partition equals the partition's `ord` values shifted by 1, with
  the first row NULL. Built by per-partition multiset compare
  (initial indexed-lookup ref had a false-fail on ties — fixed).
**Why it matters:** the executor has a single hand-written
`apply_window` for all 5 window fns; without a property oracle, the
unit tests pin a handful of inputs but never explore the corner
between empty partitions, all-equal rows, single-row partitions,
etc. All 4 props pass.

### TH3 — Property tests for Q1/Q12 (OUTER + AS-OF JOIN)
**Status:** ✅ done
**File:** `crates/cq-core/tests/prop_joins.rs` (new, ~200 lines)
**Scope:** 3 `proptest!` cases generating `(left, right)` fixtures
of `(key, value)` pairs (4 keys, 0..=10 rows per side):
- `prop_full_outer_contains_both_sides_minus_inner` —
  `|FULL OUTER| == |LEFT OUTER| + |RIGHT OUTER| − |INNER|` after
  right-side dedup (matching the executor's last-write-wins
  semantics).
- `prop_left_outer_preserves_every_left_key` — every left row
  appears exactly once in the LEFT OUTER result (matched or
  null-padded).
- `prop_asof_picks_max_right_ts_le_left_ts` — for every left row,
  the ASOF-matched right row is the one with the maximum
  `right.ts ≤ left.ts` (or NULL if none).
**Why it matters:** the join executors share infrastructure
(hash-bucket build, null-padding), but each variant has its own
matching predicate; properties caught a tuple-destructure typo in
the test harness on first run, and would catch any future change
that breaks inclusion-exclusion or asof semantics. All 3 pass.

### TH4 — Loadgen scenarios H + I (wire-batch & schema-evolution stress)
**Status:** ✅ done
**Files:**
- `crates/cq-loadgen/src/scenarios.rs` — added
  `publish_batch_vs_sequential()` + `BatchReport`,
  `schema_evolution_under_load()` + `EvolutionReport`.
- `crates/cq-loadgen/src/main.rs` — `Scenario::PublishBatchVsSeq`
  and `Scenario::SchemaEvolutionUnderLoad` enum variants + match
  arms.
- `crates/cq-loadgen/Cargo.toml` — added `reqwest` + `urlencoding`
  (admin REST round-trip).
- `crates/cq-e2e-tests/tests/loadgen_scenarios.rs` (new, ~70
  lines) — `#[ignore]` smoke tests that wire the scenarios through
  `start_server` so the API stays honest under refactors.
- `crates/cq-e2e-tests/Cargo.toml` — `cq-loadgen` added as dev-dep.
**Measured on this run:**
- Scenario H: 15.14× speedup (`publish_batch` 2.54 µs/row vs
  sequential `publish` 38.46 µs/row over 5 000 rows). Validates
  Q2's wire-batch frame is actually amortising the per-row cost.
- Scenario I: 6 001 / 6 001 publishes acked through 2 online
  `add_column` events in a 12-second window; p50 = 148 µs,
  p99 = 1.3 ms. Validates Q11's atomic schema-swap doesn't drop
  in-flight publishes under sustained load.
**Why these matter:** unit + e2e tests for Q2/Q11 exercise the
correctness boundary (does it produce the right rows?). Only a
load-driven scenario exposes the **throughput** and
**under-load-stability** failure modes.

### TH5 — Differential corpus growth (advanced aggregates + window fns vs DataFusion)
**Status:** ✅ done
**Files:**
- `crates/cq-differential-tests/corpus/009_advanced_aggregates.yaml`
  (new) — 14 cases covering `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`,
  `VAR_SAMP`, `COUNT(DISTINCT)` × {string, int, NULL handling,
  GROUP BY}, `PERCENTILE_CONT` × {0.25, 0.5, 0.95}, `MEDIAN` × {odd
  N, even N interpolation}. The PERCENTILE_CONT cases use CQ's
  positional `(col, q)` syntax (which DataFusion can't plan); the
  harness gracefully falls back to `expected_rows`-only verification
  in that scenario.
- `crates/cq-differential-tests/corpus/010_having_offset.yaml`
  (new) — 9 cases covering `HAVING SUM > k`, `HAVING COUNT = k`,
  `HAVING AVG >=`, `HAVING MAX >`, no-rows-match (`expected_rows:
  []`), `LIMIT … OFFSET …`, `OFFSET alone`, `OFFSET` past row
  count, `OFFSET 0` identity.
- `crates/cq-differential-tests/corpus/011_window_functions.yaml`
  (new) — 9 cases covering `ROW_NUMBER` × {PARTITION BY, global},
  `RANK` × {ties, per-partition}, `DENSE_RANK`, `LAG` × {default
  offset, offset 2}, `LEAD` × {default offset, offset 2}. Each case
  carries an explicit outer `ORDER BY` because the harness's
  uppercase-grep for "ORDER BY" treats any query containing one
  (including inside `OVER(...)`) as ordered-comparison; without the
  outer ORDER BY the rows-equal-but-out-of-order verdict would be
  a false failure.
**Naming gotcha sidestepped:** CQ treats `STDDEV` / `VARIANCE` as
*population* (kdb/q tradition), DataFusion treats them as *sample*
(ANSI). The corpus uses only the unambiguous `*_POP` / `*_SAMP`
forms so cases pass without an `expect_divergence` annotation.
**Run:** `cargo test -p cq-differential-tests --release` — corpus
grew **37 → 65 entries; 65/65 passing**, including all 28 new
hardening cases.
**Why these matter:** unit tests pin a handful of hand-picked
inputs per feature. A reference engine (DataFusion 42) catches
edge cases nobody pre-imagines — NULL handling in `COUNT(DISTINCT)`,
empty `HAVING` filters, tied-rank behaviour, lag/lead boundaries.
The corpus accumulates those.

### Hardening coverage matrix

| Feature       | Unit | E2E | Stress | Loom | Proptest | Diff vs DF |
|---------------|------|-----|--------|------|----------|------------|
| P8 STDDEV/VAR | ✅   | ✅  | —      | —    | —        | ✅ (TH5)   |
| P9 PERCENTILE | ✅   | ✅  | —      | —    | —        | ✅ (TH5)   |
| P10 COUNT(D)  | ✅   | ✅  | —      | —    | —        | ✅ (TH5)   |
| P3 HAVING     | ✅   | ✅  | —      | —    | —        | ✅ (TH5)   |
| P4 OFFSET     | ✅   | ✅  | —      | —    | —        | ✅ (TH5)   |
| Q1 R/F OUTER  | ✅   | ✅  | —      | —    | ✅ (TH3) | —*         |
| Q2 wire batch | ✅   | ✅  | ✅(TH4)| —    | —        | —          |
| Q7 window     | ✅   | ✅  | —      | —    | ✅ (TH2) | ✅ (TH5)   |
| Q11 schema    | ✅   | ✅  | ✅(TH4)| ✅(TH1)| —      | —          |
| Q12 ASOF JOIN | ✅   | ✅  | —      | —    | ✅ (TH3) | —*         |

\* JOIN-based features aren't in the differential corpus because the
harness builds a single topic per case; multi-table cross-engine
checks would require a harness extension (separate work item).

### TH6 — Snapshot-cache invalidation on write (bug fix)
**Status:** ✅ done
**Files:**
- `crates/cq-transport/src/router.rs` — new
  `invalidate_snapshot_cache_for_topic(topic)` helper. Called from
  the success path of `handle_publish_inner`, `handle_publish_batch`,
  and `handle_sow_delete`. Walks the global snapshot-fanout cache,
  drops every Ready entry whose key's topic component matches.
  Building entries are left alone — their in-flight build will land
  next, get its own ack, then be evicted by the next write.
- `crates/cq-e2e-tests/tests/snapshot_cache_invalidated_on_publish.rs`
  (new) — 3 regression tests covering:
  - `second_sow_after_publish_sees_post_publish_row` — SOW → publish
    → SOW within the 500 ms cache TTL: second SOW must return the
    post-publish row, not the cached pre-publish copy.
  - `batch_publish_also_invalidates_cache` — same invariant via
    `publish_batch`.
  - `delete_also_invalidates_cache` — SOW → sow_delete → SOW: the
    deleted row must not surface from a stale snapshot entry.
**Discovery path:** the full-suite test run (`cargo test --workspace
--release`) surfaced a deterministic failure in
`filter_index::index_stays_consistent_under_updates_and_deletes`
(part of the initial commit, predating all P/Q work). Bisecting
in-process vs wire-path: a fresh unit test against `Topic` directly
passed, proving the topic + store + index pipeline was correct. A
debug wire-trace showed the same SOW filter alternated stale / fresh
results depending on whether the predicate had been queried before.
Grepping for `snapshot_cache` pointed at `snapshot_fanout_cache`
in `router.rs:255` — keyed by `(topic, sql)`, TTL 500 ms, and
**never invalidated on writes** (only on TTL expiry or byte-cap
eviction). The cache was added for the fanout case (S21 era: N subs
joining within ms of each other share one encoded snapshot) but
applies unconditionally in `deliver_streaming_snapshot`, so a
one-shot SOW after a publish would also get a stale entry.
**Why per-topic, not per-(topic, sql):** a publish can change rows
that match arbitrary cached predicates — there's no cheap way to
tell *which* sql-keyed entries it invalidates, so we drop them all
for that topic. The fanout workload still benefits: the next sub in
a wave rebuilds the entry and re-fills it for the rest of the wave.
**Metric:** new `cq_snapshot_cache_invalidated_total` counter +
`cq_snapshot_cache_bytes` gauge update on each eviction sweep.
**Validation:** `cargo test --workspace --release --no-fail-fast`
→ 516 / 516 passing (was 512 pass + 1 fail before the fix). The
originally-broken
`filter_index::index_stays_consistent_under_updates_and_deletes`
now passes alongside the 3 new dedicated regression tests.

### TH7 — Test-diversification pass for every P/Q feature
**Status:** ✅ done
**Goal:** Each P/Q feature had a single "happy path" e2e test —
broad coverage, but no edge-case / error-path / interaction
verification. This pass extends every feature file with 3-5
diversified tests covering NULL handling, empty inputs, boundary
conditions, type variations, error paths, and feature-interaction
shapes.
**Net change:** **516 → 601 workspace tests (+85)**, organised into
8 batches across 25 e2e files. 0 failures, 4 ignored (deliberately).
The diversification surfaced several real cqserver limitations
worth pinning as positive tests:
- `WHERE <arith-expr>` is not supported (P2 added SELECT-arithmetic
  only) — test asserts a clean error is returned.
- `ORDER BY <aggregate-expr>` is unsupported — tests adjusted to
  use SELECT alias or skip the chain.
- `ASOF JOIN` is INNER (drops unmatched left rows), not OUTER —
  test asserts the actual behaviour.
- `RIGHT JOIN` expands right rows by left matches (duplicate keys
  on left blow up the right-side rowcount) — test asserts the
  multiplicative cardinality.
- `PIVOT ... WHERE` syntax order is rejected — test reframed as a
  pivot-scale check instead.
- `Bytes` columns are runtime-only (no static config form) — test
  uses `/admin/add-column?type=bytes` per the worklog Q10 note.

**Batches:**
1. **Parser P1-P4, P13** (`parser_*.rs`) — 25 tests. NULL operands,
   division by zero, anchored vs unanchored regex, OFFSET edge
   cases (zero, exact-N, no-LIMIT, DESC), HAVING + WHERE + LIMIT
   composition, aliased compound predicates.
2. **Aggregates P8-P10** (`parser_stddev/percentile/count_distinct.rs`)
   — 15 tests. Single-row stddev/var = 0, NULL-skipping COUNT
   DISTINCT, MEDIAN even-vs-odd, PERCENTILE_CONT at q={0, 0.25,
   0.5, 0.95, 1} with invalid-q rejection, GROUP BY per-partition
   aggregates, empty-topic semantics.
3. **JOIN P11, P12, Q1, Q12** — 19 tests. NULL keys drop in INNER,
   empty-side variants, 1:N expansion, last-write-wins on right
   duplicates, FULL OUTER inclusion-exclusion at the wire level,
   FULL OUTER null-padding both directions, ASOF partition-by-key,
   ASOF exact-ts-equals-match, ASOF drops unmatched left rows.
4. **Engine fixes P5, P6, P7, P14** — 14 tests. Degenerate-view
   handles UPDATES (not just appends), multi-aggregate row, empty
   source, concurrent failing SOWs all resolve, failure-after-publish
   doesn't serve stale, bare-name publish routing.
5. **SDK + wire P15, P16, Q2** — 9 tests (P15 already 3; Q2 added
   4). Batch + duplicates collapse, batch onto unknown topic clean
   error, 1000-row batch, monotonic-seq across mixed paths.
6. **Wire/admin Q3, Q4, Q6** — 12 tests. PIVOT single-distinct +
   empty-topic + 8-distinct-values, logon with no-metadata + long
   metadata + persistent ops, admin/clients empty + multi-sub +
   disconnect-reaping.
7. **Q7 window, Q8 CTE, Q9 subquery** — 12 tests. RANK vs DENSE_RANK
   tie semantics on the wire, LAG offset 2, window-on-empty, CTE +
   main filter composition, multi-CTE chain, RECURSIVE rejection,
   NOT IN subquery, IN combined with another predicate.
8. **Q10 Bytes (new file), Q11 schema-evo** — 9 tests. Bytes
   round-trip + NULL + 10KB payload + invalid-base64-becomes-null,
   add long column + NULL semantics for pre-existing rows + bulk
   publish after add + duplicate-name rejection + unknown-topic
   rejection.

**Auxiliary suites unaffected:** differential corpus 65/65,
loom 2/2, loadgen smokes 2/2, prop tests 7/7. All hold after the
diversification.

### TH8 — Test-diversification pass for non-P/Q feature areas
**Status:** ✅ done
**Goal:** TH7 covered every P/Q feature; the rest of cqserver
(persistence, queues, auth, views, indexing, string-fns, TTL,
delta-publish) had the same broad-but-thin coverage — 20 / 37
non-P/Q e2e files had only a single test each. This pass extends
the highest-impact files with edge-case + interaction + error-path
tests.
**Net change:** **601 → 634 workspace tests (+33)**, 0 failures.
Total e2e + unit coverage now 634 (workspace) + 65 (differential)
+ 2 (loom) + 2 (loadgen smokes) + 7 (proptests) = **710 tests**.

**Batches:**
| Batch | Files | New tests | Highlights |
|---|---|---|---|
| A — Persistence | `bookmark_store_e2e.rs`, `txlog_archive.rs`, `txlog_compression.rs` | +7 | LocalBookmarkStore multi-topic + missing-file + monotonic-record, no-archived-segments recovery, mixed live+archived segments, last-write-wins recovery, empty-topic restart |
| B — Streaming | `aggregating_subscription_e2e.rs`, `send_keys.rs` | +6 | COUNT(*) sub increments, MIN/MAX sub tracks extremes, late-arrival first-publish creates group, send_keys empty-topic then first-publish, send_keys + filter, send_keys snapshot omits payload |
| C — Queue | `queue_dlq.rs`, `queue_lease.rs` | +6 | DLQ after 3 attempts, acked never reaches DLQ, DLQ delivery kind, round-robin distributes, disconnected consumer redelivers, no-loss under ack pattern |
| D — Auth | `entitlement_filter.rs` | +3 | Row filter intersects with client filter, row filter matching no rows is empty, bad password rejects logon (+ subsequent ops fail) |
| E — Views | `view_materialization_e2e.rs` | +2 | View with COUNT(*) increments on publish (with retry-poll for async settle), view on empty source starts empty then grows |
| F — Indexing | `range_index_e2e.rs` | +2 | BETWEEN out-of-band returns empty, BETWEEN single-value, strict `<` excludes boundary |
| G — Misc | `filter_string_fns.rs`, `ttl_expiration.rs` | +7 | UPPER/LOWER case-insensitive, LIKE wildcards (`%` + `_`), NOT LIKE, LENGTH on empty string, republish-before-TTL resets clock, TTL expires rows independently |

**Real findings surfaced:**
- A `BETWEEN low AND high` query where `low > high` causes a server-side
  panic ("snapshot task join failed") — this is a real cqserver bug
  but outside this batch's scope. Test reframed to avoid the crash
  while still exercising the empty-result path with a valid range.
- View materialisation is asynchronous; tests need retry-polling
  rather than single-shot `sleep(N)` waits to be reliable under load.
  Adopted a 20×150ms poll loop pattern in view tests.
- TTL sweep runs at ~1Hz; tests with TTL=2s need ≥3s wait margin
  past the published timestamp to deterministically observe expiry.

**Auxiliary suites still green:** differential 65/65 · loom 2/2 ·
loadgen smokes 2/2 · property tests 7/7. Build + test cycle remained
healthy throughout — every batch was validated before moving on.

### TH9 — Deep-dive coverage: predicate proptest, wire negatives, loom, restart-recovery
**Status:** ✅ done
**Goal:** TH7/TH8 broadened coverage to ~600 tests but were all the
same shape: positive-path or edge-case wire tests. This pass adds
the THREE qualitatively-different test classes the suite was still
missing: property-based fuzz for the predicate compiler, wire-level
negative tests for the codec, and loom models for the remaining
concurrency-critical paths — plus restart-after-damage recovery.
**Net change:** **634 → 657 workspace tests (+23)** + **+4 cq-core
proptests** + **+5 new loom models** + **+9 wire-negative tests**
+ **+8 restart-recovery tests** (1 ignored documenting a real bug).
Total now **726 tests** across all suites. Pass rate 657/657 (0
fail, 5 ignored).

**Real bugs surfaced and FIXED:**

1. **`cq-core/src/sec_index.rs::rows_in_range`** — `BetweenLong`
   on an indexed column with `low > high` panicked the server
   ("snapshot task join failed") because `BTreeMap::range(l..=h)`
   panics when `l > h`. Originally surfaced in TH8 by accident
   when I wrote the inverted-BETWEEN test, sidestepped at the
   time. Now fixed: detect the inverted range up front and return
   an empty bitmap. Regression test:
   [parser_range_index_inverted_between_returns_empty_not_panic](crates/cq-e2e-tests/tests/range_index_e2e.rs).

2. **`cq-core/src/predicate.rs::extract_string_value`** — negative
   integer literals in WHERE clauses (`WHERE v < -44`) errored
   with `InvalidLiteral` because sqlparser models `-44` as
   `UnaryOp { Minus, Number("44") }`, and the literal extractor
   only handled `Number` (no UnaryOp arm). Found by the new
   `prop_predicate_compiler` proptest on its first run. Fixed by
   adding `UnaryOp::Minus` / `UnaryOp::Plus` cases that prepend
   the sign and delegate to the inner extractor.

**Real bugs surfaced and DOCUMENTED (not fixed in this batch):**

3. **Deeply-nested JSON publish stalls.** `crates/cq-e2e-tests/
   tests/wire_negative.rs::moderately_nested_json_flattens_or_
   errors_cleanly` notes that 50 levels works; 500 levels stalls
   the publish path (5s timeout). The flattener is likely O(n²)
   or hits a stack-depth issue.
4. **Garbage-tail txlog recovery refuses to boot.** `crates/
   cq-e2e-tests/tests/restart_recovery_edge_cases.rs::
   restart_after_garbage_appended_to_txlog_does_not_crash`
   is `#[ignore]`d — appending 64 bytes of `0xFF` after the last
   record prevents the server from coming up (healthz never ready).
   Power-loss + filesystem padding can produce trailing garbage,
   so this is a legit robustness gap.

**Sub-batches:**

| Batch | Files | Tests | What it validates |
|---|---|---|---|
| #1 Parser/predicate fuzz | `crates/cq-core/tests/prop_predicate_compiler.rs` (new) | +4 proptests | BETWEEN with random bounds (low/high any order) never panics, indexed vs plain agree, strict comparisons agree with a Rust reference (last-write-wins dedup), IS NULL/IS NOT NULL never panic, AND/OR compositions agree. 64 cases per property → ~256 random inputs. |
| #2 Wire negatives | `crates/cq-e2e-tests/tests/wire_negative.rs` (new) | +9 | Oversized frame, zero-length frame, non-JSON payload, unknown command, truncated payload + disconnect, non-object publish data, NaN string in Double column, moderately-nested JSON, concurrent malicious clients don't block legitimate traffic. After every negative exchange, the harness verifies a fresh SDK client can still publish + SOW. |
| #3 Loom models | `crates/cq-core/tests/loom_concurrent_ops.rs` (new) | +5 loom | `delete` vs `snapshot` all-or-nothing, `delete` vs per-key read all-or-nothing, two-publisher last-writer-wins on same key, two publishers + observer (3 legal states), writer vs concurrent reader (no torn rows). |
| #4 Restart recovery | `crates/cq-e2e-tests/tests/restart_recovery_edge_cases.rs` (new) | +7 + 1 ignored | Truncated-segment recovery (loses ≤1 partial record), corrupt/truncated/empty bookmark files load as empty store, fast-publish-then-kill durability, empty persistent topic recovery, mixed populated+empty topics restart, double-restart preserves state. |

**Harness extension:** `KeptDir::root()` + `KeptDir::txlog_dir()`
public accessors added to `cq-e2e-tests/src/lib.rs` so recovery
tests can inject damage into the on-disk state between stop and
restart.

**Lessons from the run:**
- The proptest harness caught a real bug on its FIRST run (the
  `UnaryOp(Minus)` literal-extractor gap). The whole point of
  property tests is to catch the class of bug — this is exactly
  what we hoped for.
- The reference implementation needs to mirror cqserver's
  semantics, not just SQL semantics. cqserver collapses keys with
  last-write-wins on upsert; a naive `filter()` over the raw
  fixture rows gave false-fail. Added `dedup_last_write` helper.
- Loom models for distinct concurrency invariants (delete-vs-read,
  multi-write, write-vs-read) each catch a different family of
  bug. The 5 new models together cover the production paths that
  weren't already covered by `loom_add_column_swap` (TH1).
- Restart-recovery tests are the most fragile to write — small
  amounts of trailing garbage can prevent server boot. Surfaced a
  real bug; documented via `#[ignore]` for follow-up.

**Total project test count after TH9:** 657 workspace + 65 differential
+ 7 loom (2 add-col + 5 concurrent-ops) + 11 proptests (4 window +
3 join + 4 predicate) + 2 loadgen smokes = **742 tests**.

### TH10 — Fix the remaining real bugs surfaced by TH9
**Status:** ✅ done
**Goal:** TH9 surfaced two real bugs that were documented but not
fixed: (1) garbage appended to a txlog segment prevented server
boot, (2) deeply-nested JSON publishes stalled. Both are fixed in
this pass; one downstream gap (wire-codec recursion at 500+ depth)
is documented as a follow-up.

**Bug 4 — Garbage-tail txlog blocks recovery (FIXED).**
[crates/cq-txlog/src/reader.rs::SegmentReader::read_next](crates/cq-txlog/src/reader.rs)
now treats two failure modes on the ACTIVE segment as torn-tail
EOF (returning `Ok(None)`, with a `tracing::warn!` for operator
visibility):
- `frame_len > MAX_ENTRY_SIZE` — header decodes to an impossibly
  large length (e.g. trailing `0xFF` bytes decode to 4 GiB).
- `frame_len == 0` — a real entry always carries a positive body
  (sequence + topic + key + payload); zero-length is unambiguously
  trailing garbage (e.g. zero-padded filesystem sectors after a
  power loss).

CRC mismatches remain a hard error — these are ambiguous between
torn-tail garbage and mid-log corruption, and silently swallowing a
mid-log integrity violation would be much worse than the operator
having to truncate one segment. Sealed (non-active) segments treat
all three failure modes as hard errors, since by definition a
sealed segment cannot legitimately have trailing garbage.

**Test impact:**
- `restart_after_garbage_appended_to_txlog_does_not_crash` (TH9)
  flipped from `#[ignore]` to passing; recovers all 5 acked
  publishes after 64 bytes of `0xFF` appended.
- New `restart_after_zero_byte_garbage_recovers` covers the
  zero-length variant.
- One existing test (`tests/crash_recovery::crc_corruption_mid_log_surfaces_error`)
  needed an offset adjustment: it flipped a HEADER byte at offset
  200, which now decodes to an oversized length and gets tolerated
  as torn-tail. Updated to flip at offset 12 (4 bytes into the
  first record's body — guaranteed CRC mismatch). The mid-log
  integrity guarantee remains pinned.

**Bug 3 — Deep-JSON flattener stall (FIXED at the flattener layer; partial).**
[crates/cq-core/src/flatten.rs::FlattenConfig](crates/cq-core/src/flatten.rs)
gained a `max_depth` field (default 32 — well past any realistic
publish payload nesting). `flatten_recursive` short-circuits when
the depth would exceed the cap, silently dropping deeper subtrees
(same shape as the existing `max_array_index` truncation).

**Test impact:**
- New cq-core unit tests `deeply_nested_input_is_bounded_by_max_depth`
  + `flatten_with_custom_depth_cap_truncates_at_boundary` pin the
  bounded-time contract (a 200-level input now flattens in <1s).
- `wire_negative.rs::deeply_nested_json_is_depth_capped_and_never_stalls`
  raised from the previous 50-level "stays alive" to 100-level
  "completes within 3s". The flattener-layer fix bounds the
  cq-core publish path; the e2e contract holds end-to-end at 100
  levels.

**Follow-up (NOT in this pass):** at 500+ levels the bottleneck
shifts to the WIRE serialization layer — `serde_json::to_vec` /
`from_slice` recursion + the codec's frame-encode path don't honour
the flattener's depth cap. A 500-level publish still stalls, just
in a different codepath. Filing as a separate follow-up: the wire
codec should bound JSON nesting at decode time with a clean error.

**Validation:**
- Workspace: **661 / 661 passing**, 0 failures (was 657 before
  this pass; +4 tests came from the new flatten unit tests + the
  flipped-from-ignore garbage-tail e2e + the new zero-byte garbage
  e2e).
- Loom: 7 / 7 (no regression).
- Differential corpus: 65 / 65.
- Proptests: 11 / 11.

**Bug-fix scorecard for the entire TH7 → TH10 arc:**

| # | Bug | Severity | Status |
|---|---|---|---|
| 1 | Snapshot cache returns stale rows after publish (TH6) | High (data correctness) | ✅ fixed |
| 2 | Inverted-BETWEEN crashes server (TH9 #1) | High (DoS via valid SQL) | ✅ fixed |
| 3 | Negative integer literals rejected (TH9 #1) | Medium (parser surface) | ✅ fixed |
| 4 | Garbage-tail txlog blocks recovery (TH9 #4 → TH10) | High (data inaccessible after power-loss) | ✅ fixed |
| 5 | Deep-JSON publish stalls — flattener layer (TH9 #2 → TH10) | Medium (DoS vector) | ✅ fixed |
| 6 | Deep-JSON publish stalls — wire-codec layer | Low (same DoS, different codepath) | Documented; follow-up |

**Total project test count after TH10:** 661 workspace + 65
differential + 7 loom + 11 proptests + 2 loadgen smokes =
**746 tests**.

---

## R-series — 95% AMPS-parity push

**Goal:** Close the gap between cqserver's SQL surface and AMPS's
SQL surface, focusing strictly on AMPS-native query shapes. The
demo Query Library audit (see `scripts/test-query-library.mjs`)
showed only 6/28 patterns worked pre-R-series; the rest hit
parser/feature gaps. Per the AMPS_PARITY doc roadmap, ~6-8 P/Q-style
sessions close most of the SQL gap (R1-R8), and 2-3 more close the
substantial-work items (R9-R10).

**Scope discipline:** Postgres-only (`~*`, `COALESCE`-with-`INTERVAL`,
`NOT EXISTS` correlated), Snowflake-only (`[BROADCAST]` hints,
`PIVOT (col)` shorthand without explicit `FOR ... IN`), and
Spark-only (`NTILE` with non-integer bucket) syntax is OUT OF SCOPE.
Only patterns AMPS actually evaluates are targeted.

### R1 — `ORDER BY <select-alias>` resolution
**Status:** ✅ done
**Files:** [crates/cq-core/src/query.rs](crates/cq-core/src/query.rs)
([parse_order_by](crates/cq-core/src/query.rs) extended to take SELECT
items, returns parallel `Vec<Option<String>>` alias side-channel; main
aggregate sort path keys by alias when present).
**Test:** `parser_having::order_by_select_alias_of_aggregate_sorts_correctly`.
**Unblocks demo:** `ag-1` PnL-by-book pattern (`ORDER BY day_pnl DESC`
where `day_pnl` is the alias for `SUM(day_pnl)`).

### R2 — Scalar functions + arithmetic in WHERE / HAVING
**Status:** ✅ done
**Files:** [crates/cq-core/src/predicate.rs](crates/cq-core/src/predicate.rs).
New `NumExpr` AST (`Col`/`Lit`/`Add`/`Sub`/`Mul`/`Div`/`Neg`/`Abs`/
`Round`/`Floor`/`Ceil`); `CompareNum` + `BetweenNum` predicate
variants. `compile_comparison` and `Expr::Between` arms detect
structured LHS and dispatch through `try_compile_num_expr`. f64
NaN propagation gives SQL-correct null-handling (NaN ≠ anything).
**Test file:** `r2_scalar_fns_in_where.rs` — 6 tests covering ABS,
arithmetic, ROUND/FLOOR/CEIL, col-vs-col, BETWEEN-num, null propagation.
**Bug-find side-effect:** the TH7-era test
`arithmetic_in_where_returns_clean_error` (which pinned the
"unsupported, must error" pre-R2 behaviour) was updated to pin the
new positive behaviour.
**Unblocks demo:** `fl-1` (ABS in WHERE), `mx-3` (col-vs-col WHERE).

### R3 — Aggregates over numeric expressions
**Status:** ✅ done
**Files:** [crates/cq-core/src/query.rs](crates/cq-core/src/query.rs).
`AggregateSpec` gains an `expr: Option<NumExpr>` slot (parallel to
`col`); `parse_aggregate_call` falls back to `try_compile_num_expr`
when bare-column resolution fails; the executor evaluates the expr
per row and feeds the f64 result into `AggState` as a Double.
**Test file:** `r3_agg_over_expr.rs` — 5 tests covering `SUM(a * b)`,
`AVG(ABS(x))`, `MAX(a * b)` with GROUP BY, NULL propagation, HAVING
combined with aggregate-over-expression.
**Unblocks demo:** ag-1, ag-3, ag-4, ag-5 (numerator part), pv-1, pv-3
— most aggregate-over-expression patterns the library leans on.

### R4 — `COALESCE` / `NULLIF` (CASE WHEN deferred)
**Status:** ✅ done (CASE WHEN — separate follow-up)
**Files:**
- [crates/cq-core/src/query.rs](crates/cq-core/src/query.rs) —
  extended `ScalarExpr` with `Coalesce(Vec)` + `NullIf(a, b)`,
  `compile_scalar`'s Function arm dispatches by name,
  `try_compile_scalar_expr` recognises both.
- [crates/cq-core/src/predicate.rs](crates/cq-core/src/predicate.rs) —
  extended `NumExpr` with `Coalesce(Vec)` + `NullIf(a, b)`;
  `try_compile_num_expr`'s Function arm dispatches to a new
  `try_compile_multi_arg_num_fn` helper for the 1+ / 2-arg cases.
**Test file:** `r4_coalesce_nullif.rs` — 5 passing tests + 1
`#[ignore]`d "known limit" test documenting the
post-aggregate-wrap case (`NULLIF(SUM(qty), 0)` — needs a
computed-over-aggregate projection layer; tracked for a later
R-series item).
**Unblocks demo:** `fl-4` (COALESCE on text), per-row NULLIF guards.
**Doesn't unblock yet:** `ag-5` / `pv-2` (NULLIF wrapping aggregates).

### R5 — `NTILE(n)` + `LAG(col, n, default)` 3-arg form
**Status:** ✅ done
**Files:** [crates/cq-core/src/query.rs](crates/cq-core/src/query.rs).
`WindowFn::Lag/Lead { col, offset, default: Option<Value> }` accepts
the 3-arg form `LAG(col, n, default)`; the default is emitted for the
first `n` rows of every partition instead of NULL. `WindowFn::Ntile {
buckets }` distributes a partition's rows across `n` buckets per the
SQL spec (`floor((row_index * n) / partition_size) + 1`).
**Test file:** `r5_ntile_and_lag_default.rs` — 6 tests.

### R6 — Multi-key `JOIN ON` + `DISTINCT` underscore-topic fixture
**Status:** ✅ done
**Notes:** The multi-key ON form (`JOIN trades ON a.k1=b.k1 AND a.k2=b.k2`)
and `DISTINCT` projection were already supported; this entry verified
both with a new e2e file. Topic names use underscores (`/r6_pos`) —
SQL parser rejects hyphenated topic identifiers.
**Test file:** `r6_multi_key_join_and_distinct.rs` — 3 tests.

### R7 — `NOW()` for AMPS-form time-window expressions
**Status:** ✅ done
**Files:** [crates/cq-core/src/predicate.rs](crates/cq-core/src/predicate.rs)
— `NumExpr::NowMicros(i64)` is evaluated at parse time via
`SystemTime::now()` so every comparison against a single query uses a
single baseline; [crates/cq-core/src/store.rs](crates/cq-core/src/store.rs)
— `Value::as_f64` extended for `Timestamp` columns so
`WHERE ts > NOW() - 86400000000` works.
**Test file:** `r7_now_time.rs` — 2 tests covering `NOW() -
<microseconds>` filtering and compile-time-frozen behaviour.

### R8 — Uncorrelated `EXISTS` / `NOT EXISTS`
**Status:** ✅ done
**Files:** [crates/cq-transport/src/router.rs](crates/cq-transport/src/router.rs)
— `materialise_exists()` runs the inner SELECT once at request time
and the WHERE-clause rewriter substitutes the boolean result as
`1 = 1` / `1 = 0`. The pre-flight rewriter walks BinaryOp / UnaryOp /
Nested wrappers so `WHERE NOT EXISTS (...)` is recognised whether
sqlparser produces `Exists{negated: true}` or `UnaryOp(Not, Exists{
negated: false})`.
**Live-subscribe extension:** the same rewriter runs in
`handle_sow_and_subscribe` so `sowAndSubscribe(topic, {sql})` users
get the same semantics — uncorrelated EXISTS resolves once at
snapshot time.
**Test file:** `r8_exists.rs` — 4 tests.

### R9 — Aggregate-OVER-window with `ROWS BETWEEN` frames
**Status:** ✅ done
**Files:** [crates/cq-core/src/query.rs](crates/cq-core/src/query.rs).
`WindowFn::FrameAgg { agg: FrameAggKind, col, frame_preceding }` with
the `FrameAggKind` enum (Sum/Avg/Min/Max/Count). `extract_frame_preceding`
handles `ROWS BETWEEN k PRECEDING AND CURRENT ROW` (k may be a literal
or `UNBOUNDED`); the parser branch detects SUM/AVG/MIN/MAX/COUNT with
`over: Some()` and routes them to the rolling-frame evaluator. A
prerequisite fix in `is_aggregate_function_call` makes window-function
calls (`over: Some(...)`) not match as plain aggregates, preventing
misleading "column must appear in GROUP BY" errors.
**Test file:** `r9_window_frames.rs` — 4 tests covering rolling AVG,
rolling SUM with PARTITION BY, rolling MIN/MAX, cumulative SUM.

### R10 — `FROM (SELECT …)` derived tables
**Status:** ✅ done
**Files:** [crates/cq-transport/src/router.rs](crates/cq-transport/src/router.rs)
— `try_resolve_derived_table()` materialises the inner subquery
against its source topic, builds an ephemeral in-memory `Topic` from
the result rows (`make_ephemeral_topic_from_rows()` — type-infers
each column from the first non-null value), then runs the outer SQL
against the ephemeral via the standard `Topic::query` path
(`rewrite_outer_from_to_t()` replaces the `(SELECT ...)` AST node with
a `t` table reference). Wired into both `handle_sow` and
`handle_sow_and_subscribe`; the live-subscribe path also sends an ack
with a `sub_id` so clients can `unsubscribe()` cleanly.
**Test file:** `r10_derived_tables.rs` — 4 tests covering outer
filter, outer LIMIT, empty-inner short-circuit, and pass-through.

### R11 — Multi-line SQL clause-boundary parsing
**Status:** ✅ done — fixes a latent rewrite-bug that masked half the
R-series wins in the live-subscribe code path.
**Problem:** `rewrite_from_to_t()` searched for clause boundaries with
literal `" GROUP BY "` (single-space delimited). Multi-line SQL with
`\nGROUP BY` failed the match, leaving `end = sql.len()` and dropping
the entire tail (GROUP BY, HAVING, ORDER BY, etc.) when the outer
FROM-table identifier was replaced with `t`. Symptom: the cqserver
parser saw `SELECT … FROM t` and rejected with the misleading
"column must appear in GROUP BY" error.
**Files:** [crates/cq-transport/src/router.rs](crates/cq-transport/src/router.rs)
— new `find_clause_kw()` matches keyword sequences separated by any
ASCII whitespace run, and is **paren-depth aware** so an inner
`ORDER BY` inside `OVER (...)` or a derived `(SELECT … ORDER BY …)`
is not treated as the outer query's boundary. `find_pivot_kw()` for
`PIVOT (...)` / `UNPIVOT (...)` got the same treatment. The rewriter
also walks back over leading whitespace so `... FROM t GROUP BY ...`
keeps its separator. Pre-flight rewriters (`resolve_in_subqueries`,
`try_resolve_derived_table`) and `peek_join` now tolerate sqlparser
parse failures — AMPS-native shapes (top-level PIVOT, etc.) that
GenericDialect rejects fall through to the cqserver-native executor
instead of erroring out.
**Test file:** `r11_multiline_sql.rs` — 3 tests covering multi-line
GROUP BY, multi-line WHERE/HAVING/ORDER/LIMIT stack, and multi-line
PIVOT-as-FROM-modifier.
**Unblocks demo:** every aggregate / pivot / derived-table query in
the library — those are all multi-line by convention.

### R12 — `IS TRUE` / `IS NOT TRUE` / `IS FALSE` / `IS NOT FALSE`
**Status:** ✅ done
**Files:** [crates/cq-core/src/predicate.rs](crates/cq-core/src/predicate.rs)
— new `CompiledPredicate::IsBool { col, want_true, negated }` variant
with SQL three-valued semantics: `IS TRUE` matches only true (NULL
and FALSE both fail); `IS NOT TRUE` matches false OR NULL.
**Test file:** `r12_is_bool.rs` — 3 tests covering the basic forms,
SQL three-valued logic for NULL handling, and the demo's compound
pattern `WHERE x IN (...) AND NOT (flag IS TRUE)`.

### Demo library rewrite (AMPS-only dialect)
**Status:** ✅ done — [clients/examples-web/src/lib/queries/library.ts](
clients/examples-web/src/lib/queries/library.ts).
Dropped Postgres `~*` (→ `MATCHES_REGEX(col, '(?i)pattern')`),
Snowflake `[BROADCAST]` hint (dropped — AMPS join planner doesn't
need a hint), `INTERVAL '1 day'` literals (→ `NOW() -
86400000000`), `CREATE [MATERIALIZED] VIEW` DDL (AMPS configures
views in TOML; the demo now queries the pre-registered `/v_*`
topics directly), and post-GROUP BY `PIVOT (col)` shorthand (→ AMPS
PIVOT-as-FROM-modifier `FROM topic PIVOT (SUM(v) FOR c IN (...))`).
JOIN queries against non-existent topics (`issuers`, `research_notes`)
were rewired to `securities`. Derived tables now carry the SQL-required
alias (`FROM (SELECT ...) AS g`).
**Wire harness:** [clients/examples-web/scripts/sync-queries.mjs](
clients/examples-web/scripts/sync-queries.mjs) generates the
audit's `queries.mjs` from the TS source; the topic-detection
helper in `query-router-shared.mjs` learned about the `/v_*`
materialised views.

### R13 — SDK error-ack routing for SOW + JOIN demo fixups
**Status:** ✅ done
**Problem:** A JOIN whose `combined_join_schema` parse fails (e.g.
unknown column on either side) emits a server `error` ack tagged
with the original SOW cid. The TypeScript SDK's `dispatch()` only
checked the `pending` RPC map for cid-keyed messages — SOW
completions live in a separate `snapshotCompletions` map, so the
error ack was silently dropped and the SOW caller waited out the
full 30s ack timeout. Manifested as `timeout after 15000ms (sow)` in
the demo audit even though the server had already failed in <50ms.
**Files:**
- [client-sdks/ts/src/client.ts](client-sdks/ts/src/client.ts) — added
  a parallel `snapshotRejecters` map populated by every `sow()` call;
  the ack dispatcher consults it on `{status: 'error'}` and rejects
  the SOW promise with the server-supplied reason.
- [clients/examples-web/src/lib/queries/library.ts](
  clients/examples-web/src/lib/queries/library.ts) — `jn-2` HAVING
  references `COUNT(*)`, which the parser requires also be in the
  SELECT projection; added `COUNT(*) AS n_trades` to the select. `jn-3`
  asked for columns (`country`, `region`) that the demo's `/securities`
  schema doesn't declare; switched to `issuer`, `sector`, `currency`.
- [crates/cq-transport/src/router.rs](crates/cq-transport/src/router.rs)
  — `deliver_join_snapshot`'s error path now sends
  `CqMessage::error(Some(sub_id.clone()), &e)` instead of
  `Some(None)` so the cid actually reaches the client.

### Audit results
| Stage | Pass | % |
|-------|------|---|
| Pre-R-series (P/Q/TH end-state) | 6/28 | 21% |
| After R1-R10 (engine only) | 9/28 | 32% |
| After R11 (multi-line fix) | 17/28 | 60% |
| After R12 (`IS TRUE`) + library rewrite | 25/28 | 89% |
| **After R13 (SDK error routing + JOIN demo fixups)** | **28/28** | **100%** |

### Known engine limitations (not blocking the demo)
The library is now AMPS-compliant end-to-end, but there are still
engine shapes the demo had to work around rather than triggering:
- `HAVING <agg>` where `<agg>` is not also in `SELECT` — works in AMPS,
  fails in cqserver with "HAVING references an aggregate not in
  SELECT". Demo uses the trivial workaround of putting the aggregate
  in SELECT.
- Scalar functions in `ORDER BY` (`ORDER BY ABS(col) DESC`) — not
  accepted; demo orders by the underlying column or by a SELECT alias.
- Scalar functions in `SELECT` projection (`SELECT ABS(col) AS x`) —
  not accepted yet; demo computes derived values client-side or via
  the existing aggregate paths.

These three are good follow-up R-series candidates if the user wants
to keep climbing toward 99% AMPS.

### Validation
- Workspace: **707 passed, 0 failed, 7 ignored** (was 661 before
  R-series; +46 tests from R1-R12 e2e files + the parser_having R1
  test).
- Demo audit: **28/28 passing, 0 failed**.
- E2E test files: `r2_scalar_fns_in_where`, `r3_agg_over_expr`,
  `r4_coalesce_nullif`, `r5_ntile_and_lag_default`,
  `r6_multi_key_join_and_distinct`, `r7_now_time`, `r8_exists`,
  `r9_window_frames`, `r10_derived_tables`, `r11_multiline_sql`,
  `r12_is_bool`.

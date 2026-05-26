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
**Status:** ⏳
**Scope:** Accept `INNER JOIN B ON a.x = b.x`; translate same-named-column equi-joins to the existing USING path. Reject non-equi or rename-required cases with a clear error.

## P12 — JOIN: LEFT OUTER JOIN *(§1.4 row 3)*
**Status:** ⏳
**Scope:** When no right-side match, emit the left row with right-side columns as NULL.

## P13 — WHERE: regex match (MATCHES_REGEX / LIKE_REGEX) *(§1.2 row 7)*
**Status:** ⏳
**Scope:** `regex` crate; compile pattern at parse time; reject bad patterns there, not at row eval.

## P14 — Topic registry: normalise slash-prefix *(§4 bug 5)*
**Status:** ⏳
**Scope:** Topics canonicalise to `/name` at registration; remove the ad-hoc dual-lookup workarounds in `init_view` and the SOW JOIN resolver.

## P15 — SDK: HA failover across multiple URIs *(§3.1 row 12)*
**Status:** ⏳
**Scope:** TS SDK accepts `uris: string[]`; rotates on connection loss with exponential backoff between full passes. E2E against a 2-instance replica-reads topology.

## P16 — SDK: batched publish *(§3.2 row 4)*
**Status:** ⏳
**Scope:** `publishBatch(topic, msgs[])` — one wire frame, one ack, one txlog append. Bumps the wire version (see S28 in AMPS_WORKLOG for the negotiation pattern).

---

## Deferred (out of scope for this worklog)

- **Window functions** (`OVER (PARTITION BY ...)`, ROW_NUMBER, LAG/LEAD) — multi-month parser project, low Atlas demo ROI.
- **Subqueries / CTEs** (`WITH x AS`, scalar subqueries, EXISTS) — same.
- **FULL OUTER / RIGHT OUTER / AS OF JOIN** — niche after P12 (LEFT OUTER) lands.
- **Bytes / Array / Object as first-class column types** — schema-evolution sized rewrite.
- **Schema evolution (online add column)** — separate plan.
- **SDK-side TLS, batched compressed publish, connection-name echo, per-client metrics, trace-id propagation** — small follow-ups; server-side TLS already has `crates/cq-e2e-tests/tests/tls.rs`.
- **Pivot dynamic value list**, **pivot-as-view** — folded into AMPS_WORKLOG S45's follow-up.

---

## Coverage snapshot

At start of this worklog:
- Parser: 6 gaps → 4 covered (P1–P4); window/subquery/CTE deferred.
- Engine: 3 known bugs → 3 covered (P5–P7) + P14 normalisation.
- Aggregates: 4 gaps → 3 covered (P8–P10); HAVING is P3.
- JOIN: 5 missing variants → 2 covered (P11–P12).
- SDK: 12 SDK gaps → 2 covered (P15–P16); rest are small enough to land opportunistically.

Total: **16 sessions** to bring the Query Builder tab to "arbitrary SQL works against cqserver" parity for the trading-floor demo's intended surface.

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
**Status:** ⏳
**Scope:** `SELECT a, b, a + b AS sum FROM t` evaluates server-side. Adds `ScalarExpr` / `ComputedColumn`. Lets the Atlas publisher stop pre-computing `mv_x_pct`, `mv_abs`.
**Tests:** parse, eval round-trip, e2e against running server.

## P3 — Parser: HAVING clause *(§1.3 row 7)*
**Status:** ⏳
**Scope:** `GROUP BY k HAVING SUM(v) > 100` — compile HAVING against `[group_cols, aggregate_aliases]` schema and evaluate after group finalise.

## P4 — Parser: OFFSET clause *(§1.5 row 6)*
**Status:** ⏳
**Scope:** `LIMIT n OFFSET m` skips the first `m` rows of the ordered result.

## P5 — Engine: fix degenerate-aggregate SOW *(§4 bug 3)*
**Status:** ⏳
**Scope:** `SELECT SUM(x) FROM t` (no GROUP BY) should upsert the single empty-key row, not grow by one per refresh.

## P6 — Engine: fix JOIN-view SOW delivery for fresh subscribers *(§4 bug 1)*
**Status:** ⏳
**Scope:** A `[[views]]`-declared INNER JOIN view populates correctly (admin shows N rows) but a fresh subscriber's SOW returns 0 rows. Root-cause and fix the view sow_iter path.

## P7 — Engine: clear encode-once cache slot on SOW failure *(§4 bug 4)*
**Status:** ⏳
**Scope:** A failed SOW request must not leave its `Building` slot in the encode-once-fanout cache (currently wedges all identical follow-ups until restart).

## P8 — Aggregates: STDDEV / STDDEV_SAMP / VARIANCE *(§1.3 row 3)*
**Status:** ⏳
**Scope:** Welford-online accumulator with merge support for the incremental aggregator.

## P9 — Aggregates: PERCENTILE_CONT / MEDIAN *(§1.3 row 4)*
**Status:** ⏳
**Scope:** Exact percentile via per-group value vector. Documented O(n) memory tradeoff.

## P10 — Aggregates: COUNT(DISTINCT col) *(§1.1 row 9)*
**Status:** ⏳
**Scope:** Exact distinct count via per-group set. HyperLogLog optimization out of scope.

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

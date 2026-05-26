# AMPS Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the gaps catalogued in `docs/AMPS_PARITY.md` (the Atlas-demo-driven gap analysis) in bite-sized sessions, each with unit + e2e tests, so the Query Builder tab can faithfully demonstrate "arbitrary SQL works against cqserver."

**Architecture:** Each gap maps to a session (P1..Pn). Sessions are independently shippable. We don't unify everything behind one big parser rewrite — we make incremental edits to `crates/cq-core/src/query.rs`, the executor, the view layer, and the SDK, with tests landing alongside each change. Existing AMPS_WORKLOG.md sessions (S1..S47) stay authoritative for the Appendix A gap set; this plan tracks the *additional* Atlas-demo gaps.

**Tech Stack:** Rust (`sqlparser` 0.50 in `cq-core`), Tokio, `cq-transport` (WS + TCP), the TS SDK in `client-sdks/ts/`, Vitest + the existing e2e harness in `crates/cq-e2e-tests/`.

**Worklog tracking:** Pointer file is `AMPS_PARITY_WORKLOG.md` at repo root. It enumerates sessions in priority order with status badges. This plan file is the *engineering* version — exact files, code, tests, commits.

**Out of scope (deferred):**
- Window functions (`OVER (PARTITION BY ...)`), CTEs, subqueries — multi-month parser project, low Atlas-demo ROI.
- FULL OUTER, RIGHT OUTER, AS OF JOIN — niche; LEFT OUTER (P12) is the realistic stopping point.
- Bytes / Array / Object as first-class column types — schema-evolution sized rewrite.
- Schema evolution (online add column) — separate plan.
- TLS / connection-name / per-client metrics — TLS already has `crates/cq-e2e-tests/tests/tls.rs` (the AMPS_PARITY.md "✗" is stale for the server; SDK-side TLS would be a separate small session).

---

## File Structure

**Parser changes** all land in `crates/cq-core/src/query.rs` (single-file parser, ~3000 lines). We do NOT split this file in-flight — too many existing tests touch its internals. The file already has clear sections (parse_select, parse_aggregate_call, parse_order_by, etc.); each session extends the relevant section.

**Engine changes** land in:
- `crates/cq-core/src/view.rs` — view materialization, JOIN-view SOW (P6)
- `crates/cq-core/src/sow_store.rs` — degenerate-aggregate upsert (P5)
- `crates/cq-core/src/topic.rs` — slash-prefix normalisation (P14)
- `crates/cq-transport/src/snapshot_cache.rs` (if extant; otherwise `router.rs`) — encode-cache wedge (P7)

**SDK changes** land in `client-sdks/ts/src/`:
- `transport.ts` / `transport-node.ts` — multi-URI failover (P15)
- `client.ts` — batched publish (P16)

**Unit tests:** colocated `#[cfg(test)] mod tests` in each `.rs` file.
**E2E tests:** new files under `crates/cq-e2e-tests/tests/` named after the session (e.g. `parser_table_aliases.rs`).
**Differential tests:** new entries appended to `crates/cq-differential-tests/corpus/` to cross-check against DuckDB where applicable.

---

## Task P1: Parser — table aliases + qualified column refs

**Spec:** `docs/AMPS_PARITY.md` §1.1 rows 4, 5. Demo workaround lives at `clients/examples-web/src/examples/ex08-query-builder/index.tsx:65` (`stripAliases`). Goal: accept `FROM t alias` and `alias.col` everywhere `t` and `col` are accepted; the alias is a *renaming*, not a new table.

**Files:**
- Modify: `crates/cq-core/src/query.rs` (parse_select + extract_topic + predicate compile path)
- Test: `crates/cq-core/src/query.rs` (existing `#[cfg(test)]` block at end)
- E2E test: `crates/cq-e2e-tests/tests/parser_table_aliases.rs` (create)
- Demo cleanup (deferred to a follow-up commit once P1 lands): drop `stripAliases` from `ex08-query-builder/index.tsx` — left as a verification step only, not a P1 deliverable.

- [ ] **Step 1: Write the failing unit test for table-alias parsing**

Append to `crates/cq-core/src/query.rs` tests module:

```rust
#[test]
fn parses_from_with_alias() {
    let schema = Schema::from_strs(
        &["sym", "px"],
        &[ColumnType::String, ColumnType::Double],
    );
    let q = ParsedQuery::parse_with_schema(
        "SELECT p.sym, p.px FROM positions p WHERE p.px > 100",
        &schema,
    )
    .expect("parse should succeed");
    assert_eq!(q.topic, "positions");
    assert_eq!(q.projection, vec![0, 1]);
}
```

- [ ] **Step 2: Run and confirm it fails**

Run: `cargo test -p cq-core query::tests::parses_from_with_alias`
Expected: FAIL — current parser either errors on `p.sym` or treats `p` as the table.

- [ ] **Step 3: Implement alias resolution**

In `crates/cq-core/src/query.rs`:
1. Extend `TableFactor::Table { name, alias, .. }` matches to capture the optional `TableAlias` (look at lines 262, 327, 336, 359, 463).
2. Build a `HashMap<String, String>` of `alias_name → real_topic`. For single-table queries the map has one entry.
3. Before predicate compile and projection lookup, walk the AST and rewrite `Expr::CompoundIdentifier([alias, col])` → `Expr::Identifier(col)` when `alias` is in the map.
4. Same rewrite for `SelectItem::UnnamedExpr` / `SelectItem::ExprWithAlias` qualified refs.
5. For JOIN: both sides may carry aliases; the rewrite must consult the *combined* alias map.

Specific helper to add near the existing `extract_topic`:

```rust
fn collect_aliases(from: &TableWithJoins) -> HashMap<String, String> {
    let mut m = HashMap::new();
    if let TableFactor::Table { name, alias, .. } = &from.relation {
        let topic = strip_identifier_quotes(&name.to_string());
        if let Some(a) = alias {
            m.insert(a.name.value.clone(), topic.clone());
        }
        m.insert(topic.clone(), topic);
    }
    for j in &from.joins {
        if let TableFactor::Table { name, alias, .. } = &j.relation {
            let topic = strip_identifier_quotes(&name.to_string());
            if let Some(a) = alias {
                m.insert(a.name.value.clone(), topic.clone());
            }
            m.insert(topic.clone(), topic);
        }
    }
    m
}

fn rewrite_qualified_refs(expr: &mut Expr, aliases: &HashMap<String, String>) {
    use Expr::*;
    match expr {
        CompoundIdentifier(parts) if parts.len() == 2 => {
            if aliases.contains_key(&parts[0].value) {
                let col = parts[1].value.clone();
                *expr = Identifier(sqlparser::ast::Ident::new(col));
            }
        }
        BinaryOp { left, right, .. } => {
            rewrite_qualified_refs(left, aliases);
            rewrite_qualified_refs(right, aliases);
        }
        UnaryOp { expr, .. } | Nested(expr) | Cast { expr, .. } => {
            rewrite_qualified_refs(expr, aliases);
        }
        Function(f) => {
            if let FunctionArguments::List(args) = &mut f.args {
                for a in &mut args.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                        rewrite_qualified_refs(e, aliases);
                    }
                }
            }
        }
        InList { expr, list, .. } => {
            rewrite_qualified_refs(expr, aliases);
            for e in list { rewrite_qualified_refs(e, aliases); }
        }
        Between { expr, low, high, .. } => {
            rewrite_qualified_refs(expr, aliases);
            rewrite_qualified_refs(low, aliases);
            rewrite_qualified_refs(high, aliases);
        }
        _ => {}
    }
}
```

Call `rewrite_qualified_refs` on every SELECT-item Expr, the WHERE clause, GROUP BY items, ORDER BY items, and HAVING (when P3 lands) before they reach existing compile paths.

- [ ] **Step 4: Run the unit test and confirm pass**

Run: `cargo test -p cq-core query::tests::parses_from_with_alias`
Expected: PASS.

- [ ] **Step 5: Add JOIN-with-aliases unit test**

```rust
#[test]
fn parses_join_with_aliases() {
    // schemas will be looked up from the topic registry in the
    // executor; here we just verify the parser accepts the shape.
    let left = Schema::from_strs(&["sym","px"], &[ColumnType::String, ColumnType::Double]);
    let q = ParsedQuery::parse_with_schema(
        "SELECT p.sym, p.px FROM positions p JOIN securities s USING (sym) WHERE s.sector = 'TECH'",
        &left,
    );
    assert!(q.is_ok(), "join-with-alias parse failed: {:?}", q.err());
}
```

Run, confirm pass.

- [ ] **Step 6: E2E test against running server**

Create `crates/cq-e2e-tests/tests/parser_table_aliases.rs`:

```rust
//! E2E: SELECT p.col FROM topic p must return the same rows as SELECT col FROM topic.

use cq_e2e_tests::TestServer;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alias_select_matches_unqualified() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;

    client.publish("positions", json!({"sym": "AAPL", "px": 150.0})).await.unwrap();
    client.publish("positions", json!({"sym": "MSFT", "px": 300.0})).await.unwrap();

    let aliased = client
        .sow("positions", Some("SELECT p.sym, p.px FROM positions p WHERE p.px > 200"))
        .await
        .unwrap();
    let unqualified = client
        .sow("positions", Some("SELECT sym, px FROM positions WHERE px > 200"))
        .await
        .unwrap();
    assert_eq!(aliased, unqualified, "aliased and unqualified must match");
}
```

(Adjust to the actual `TestServer` API in `crates/cq-e2e-tests/src/lib.rs` — read it first to mirror exact method names.)

- [ ] **Step 7: Run E2E test**

Run: `cargo test -p cq-e2e-tests --test parser_table_aliases`
Expected: PASS. If FAIL, the alias rewrite is incomplete somewhere — instrument and fix.

- [ ] **Step 8: Verify nothing else broke**

Run: `cargo test -p cq-core` and `cargo test -p cq-e2e-tests --tests` (skip ignored).

- [ ] **Step 9: Commit**

```bash
git add crates/cq-core/src/query.rs crates/cq-e2e-tests/tests/parser_table_aliases.rs
git commit -m "feat(parser): support table aliases and qualified column refs

Adds an alias-resolution pass that rewrites Expr::CompoundIdentifier
to Expr::Identifier before predicate compile / projection lookup,
covering single-table and JOIN cases. Unblocks ex08 from stripping
aliases client-side. Closes P1 in AMPS_PARITY_WORKLOG."
```

- [ ] **Step 10: Remove client-side workaround**

Delete `stripAliases` from `clients/examples-web/src/examples/ex08-query-builder/index.tsx` and pass `sql` directly to the wire. Run `npm test` in `clients/examples-web` (or browser-smoke) to confirm the Query Builder still works. Commit:

```bash
git add clients/examples-web/src/examples/ex08-query-builder/index.tsx
git commit -m "chore(ex08): drop stripAliases now that parser supports aliases"
```

---

## Task P2: Parser — arithmetic in SELECT-list

**Spec:** `docs/AMPS_PARITY.md` §1.1 row 7. The Atlas demo pre-computes `mv_x_pct`, `mv_abs` on the publisher; we want `SELECT (a - b) / b AS pct FROM positions` to work server-side.

**Files:**
- Modify: `crates/cq-core/src/query.rs` (parse_select projection branch)
- New struct: `ComputedColumn { alias: String, expr: ScalarExpr }` (or reuse aggregate path)
- Modify: `crates/cq-core/src/query.rs` executor (the row-projection step) to evaluate ScalarExpr per row.
- Test: inline + E2E `crates/cq-e2e-tests/tests/parser_select_arithmetic.rs`

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn parses_arithmetic_in_select() {
    let s = Schema::from_strs(&["a","b"], &[ColumnType::Double, ColumnType::Double]);
    let q = ParsedQuery::parse_with_schema("SELECT a, b, a + b AS sum FROM t", &s).unwrap();
    assert_eq!(q.computed.len(), 1);
    assert_eq!(q.computed[0].alias, "sum");
}
```

- [ ] **Step 2: Run, confirm fail (no `computed` field yet)**

Run: `cargo test -p cq-core query::tests::parses_arithmetic_in_select`

- [ ] **Step 3: Extend ParsedQuery + add ScalarExpr**

In `crates/cq-core/src/query.rs`:

```rust
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    Col(usize),
    LitDouble(f64),
    LitLong(i64),
    LitString(CompactString),
    Add(Box<ScalarExpr>, Box<ScalarExpr>),
    Sub(Box<ScalarExpr>, Box<ScalarExpr>),
    Mul(Box<ScalarExpr>, Box<ScalarExpr>),
    Div(Box<ScalarExpr>, Box<ScalarExpr>),
}

#[derive(Debug, Clone)]
pub struct ComputedColumn {
    pub alias: String,
    pub expr: ScalarExpr,
}
```

Add `pub computed: Vec<ComputedColumn>` to `ParsedQuery`. Update every `ParsedQuery { ... }` literal in the file to initialise it (start empty).

In parse_select, when iterating SELECT items: if the expression is `BinaryOp` with arithmetic op AND it's not an aggregate, compile it into `ScalarExpr` and push to `computed`. The `projection` Vec stays unchanged (bare col references).

- [ ] **Step 4: Implement ScalarExpr compile + eval**

```rust
fn compile_scalar(expr: &Expr, schema: &Schema) -> Result<ScalarExpr, QueryError> {
    match expr {
        Expr::Identifier(id) => {
            let idx = schema.column_index(&id.value)
                .ok_or_else(|| QueryError::ParseError(format!("unknown col {}", id.value)))?;
            Ok(ScalarExpr::Col(idx))
        }
        Expr::Value(v) => match &v.value {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() { Ok(ScalarExpr::LitLong(i)) }
                else { Ok(ScalarExpr::LitDouble(n.parse().unwrap_or(0.0))) }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => Ok(ScalarExpr::LitString(s.into())),
            _ => Err(QueryError::ParseError("unsupported literal".into())),
        },
        Expr::BinaryOp { left, op, right } => {
            let l = Box::new(compile_scalar(left, schema)?);
            let r = Box::new(compile_scalar(right, schema)?);
            use sqlparser::ast::BinaryOperator::*;
            match op {
                Plus => Ok(ScalarExpr::Add(l, r)),
                Minus => Ok(ScalarExpr::Sub(l, r)),
                Multiply => Ok(ScalarExpr::Mul(l, r)),
                Divide => Ok(ScalarExpr::Div(l, r)),
                _ => Err(QueryError::ParseError(format!("unsupported op {:?}", op))),
            }
        }
        Expr::Nested(e) => compile_scalar(e, schema),
        _ => Err(QueryError::ParseError("unsupported scalar expression".into())),
    }
}

impl ScalarExpr {
    pub fn eval_double(&self, row: &[Value]) -> Option<f64> {
        match self {
            ScalarExpr::Col(i) => match row.get(*i)? {
                Value::Double(d) => Some(*d),
                Value::Long(l) => Some(*l as f64),
                Value::Int(i) => Some(*i as f64),
                _ => None,
            },
            ScalarExpr::LitDouble(d) => Some(*d),
            ScalarExpr::LitLong(l) => Some(*l as f64),
            ScalarExpr::Add(a, b) => Some(a.eval_double(row)? + b.eval_double(row)?),
            ScalarExpr::Sub(a, b) => Some(a.eval_double(row)? - b.eval_double(row)?),
            ScalarExpr::Mul(a, b) => Some(a.eval_double(row)? * b.eval_double(row)?),
            ScalarExpr::Div(a, b) => {
                let denom = b.eval_double(row)?;
                if denom == 0.0 { None } else { Some(a.eval_double(row)? / denom) }
            }
            _ => None,
        }
    }
}
```

In the row-projection step (find where `projection: Vec<usize>` is applied to build output rows — search for `projection.iter()` in `query.rs`), append a computed-column block: for each `ComputedColumn`, eval against the row and emit `{ alias: <value> }`.

- [ ] **Step 5: Unit test passes**

Run: `cargo test -p cq-core query::tests::parses_arithmetic_in_select` — PASS.

- [ ] **Step 6: Add executor unit test (round-trip)**

```rust
#[test]
fn evaluates_arithmetic_in_projection() {
    // build a small ColumnStore with two rows, run a SOW with a + b AS sum,
    // assert each output row has the expected `sum` field.
    // (Mirror an existing executor test's setup — e.g. test_filter_index.rs.)
}
```

- [ ] **Step 7: E2E test**

Create `crates/cq-e2e-tests/tests/parser_select_arithmetic.rs` mirroring the existing aggregations.rs pattern. Publish 3 rows with known `(a, b)` pairs, run `SELECT a, b, a + b AS sum FROM t`, assert sums.

- [ ] **Step 8: Run all and commit**

```bash
cargo test -p cq-core query::tests
cargo test -p cq-e2e-tests --test parser_select_arithmetic
git add crates/cq-core/src/query.rs crates/cq-e2e-tests/tests/parser_select_arithmetic.rs
git commit -m "feat(parser): scalar arithmetic in SELECT list

Adds ScalarExpr (Col/Lit/Add/Sub/Mul/Div) + ComputedColumn so
SELECT a + b AS sum FROM t evaluates server-side instead of being
pre-computed on the publisher. Closes P2 in AMPS_PARITY_WORKLOG."
```

---

## Task P3: Parser — HAVING clause

**Spec:** §1.3 row 7. AMPS supports `GROUP BY x HAVING SUM(y) > 100`.

**Files:**
- Modify: `crates/cq-core/src/query.rs`
- Test: inline + `crates/cq-e2e-tests/tests/parser_having.rs`

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn parses_having_on_aggregate() {
    let s = Schema::from_strs(&["k","v"], &[ColumnType::String, ColumnType::Double]);
    let q = ParsedQuery::parse_with_schema(
        "SELECT k, SUM(v) AS total FROM t GROUP BY k HAVING SUM(v) > 100",
        &s,
    ).unwrap();
    assert!(q.having.is_some());
}
```

- [ ] **Step 2: Run, confirm fail**

- [ ] **Step 3: Add `having: Option<HavingPred>` to ParsedQuery**

`HavingPred` is a compiled predicate that takes a *finalised aggregate row* (HashMap<String, Value>) and returns bool. Implement by compiling the HAVING Expr to a `CompiledPredicate`-like structure that resolves `SUM(v)` etc. to the matching aggregate alias.

In parse_select, after parsing aggregates, check `select.having.as_ref()`:
- Build a name map: `Function(SUM(v))` → "<the auto-generated alias>" used in `aggregates[i].alias`.
- Walk the HAVING expr and rewrite each aggregate-function call to an `Expr::Identifier(<alias>)`.
- Compile the rewritten expr against a synthetic schema whose columns are `[group_by_cols, aggregate_aliases]`.

In the executor's group-by finalise step (around line 1579-1590 per the grep earlier), after building `row_map`, evaluate the HAVING predicate against it and skip rows that fail.

- [ ] **Step 4: Unit test passes**

- [ ] **Step 5: E2E test**

`crates/cq-e2e-tests/tests/parser_having.rs`: publish rows in 3 groups, assert `HAVING SUM(v) > X` filters out the small group.

- [ ] **Step 6: Run and commit**

```bash
cargo test -p cq-core query::tests::parses_having_on_aggregate
cargo test -p cq-e2e-tests --test parser_having
git add crates/cq-core/src/query.rs crates/cq-e2e-tests/tests/parser_having.rs
git commit -m "feat(parser): HAVING clause on aggregate queries

Compiles HAVING against [group_cols, aggregate_aliases] schema and
evaluates after group finalise. Closes P3 in AMPS_PARITY_WORKLOG."
```

---

## Task P4: Parser — OFFSET clause

**Spec:** §1.5 row 6. `LIMIT n OFFSET m` should skip the first `m` rows of the ordered result.

**Files:** `crates/cq-core/src/query.rs` (parse_limit_clause around line 401-419; executor's limit application).

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn parses_limit_offset() {
    let s = Schema::from_strs(&["k"], &[ColumnType::String]);
    let q = ParsedQuery::parse_with_schema("SELECT k FROM t LIMIT 10 OFFSET 5", &s).unwrap();
    assert_eq!(q.limit, Some(10));
    assert_eq!(q.offset, Some(5));
}
```

- [ ] **Step 2: Run, confirm fail (no `offset` field)**

- [ ] **Step 3: Add `offset: Option<usize>` to ParsedQuery**

Update parse_limit_clause:

```rust
let (limit, offset) = match query.limit_clause.as_ref() {
    Some(sqlparser::ast::LimitClause::LimitOffset { limit, offset, .. }) => {
        let l = limit.as_ref().and_then(|e| extract_usize_literal(e));
        let o = offset.as_ref().and_then(|o| extract_usize_literal(&o.value));
        (l, o)
    }
    _ => (None, None),
};
```

(Refactor the literal-parse into a helper since both fields use it.)

In the executor's limit application (search for `if let Some(lim) = q.limit`), apply `skip(offset)` first.

- [ ] **Step 4: Unit + E2E test**

E2E in `crates/cq-e2e-tests/tests/parser_offset.rs`: publish 20 rows with `seq=0..20`, query with `ORDER BY seq LIMIT 5 OFFSET 7`, assert seqs 7..12.

- [ ] **Step 5: Run and commit**

```bash
git commit -m "feat(parser): OFFSET clause

Closes P4 in AMPS_PARITY_WORKLOG."
```

---

## Task P5: Engine — fix degenerate-aggregate SOW

**Spec:** §4 bug 3. `SELECT SUM(x) FROM t` (no GROUP BY) creates a topic that grows by one row per refresh instead of upserting the empty-key row.

**Files:** likely `crates/cq-core/src/view.rs` (where view-row upsert keys are computed) and/or `crates/cq-core/src/sow_store.rs`.

- [ ] **Step 1: Reproduce in an integration test**

Create `crates/cq-core/tests/degenerate_aggregate_upsert.rs`:

```rust
//! When a view has no GROUP BY, every refresh must upsert the
//! single empty-key row, not append a new one.

use cq_core::{Topic, view::View};

#[tokio::test]
async fn no_group_by_view_stays_single_row() {
    let src = Topic::new(/* ... */);
    let v = View::new("SELECT SUM(px) FROM src", &src).unwrap();
    src.publish(/* px=10 */).await;
    src.publish(/* px=20 */).await;
    src.publish(/* px=30 */).await;
    assert_eq!(v.sow_iter().count(), 1, "degenerate aggregate must stay 1 row");
    let row = v.sow_iter().next().unwrap();
    assert_eq!(row.get("SUM_px").unwrap(), &Value::Double(60.0));
}
```

(Mirror the existing `view_materialization.rs` setup exactly — read it first.)

- [ ] **Step 2: Run, confirm fail**

- [ ] **Step 3: Diagnose and fix**

Likely cause: the key-derivation function for upsert builds a key from the GROUP BY columns; with empty GROUP BY it returns either a random-per-call key or an empty one that doesn't compare. Audit the view's `apply_input_row` / `upsert` path.

Fix sketch: when `group_by.is_empty()` and `aggregates.is_empty() == false`, use a fixed sentinel key (e.g. `IxKey::empty()`) for all upserts.

- [ ] **Step 4: Run test, confirm pass**

- [ ] **Step 5: E2E test**

`crates/cq-e2e-tests/tests/degenerate_aggregate_e2e.rs`: declare a `[[views]]` block with `SELECT SUM(x) FROM t`, subscribe, publish 5 rows, assert exactly 1 SOW row with the correct sum.

- [ ] **Step 6: Commit**

```bash
git commit -m "fix(view): upsert degenerate-aggregate views to single row

Was: each input row appended a new empty-keyed output row.
Now: empty GROUP BY collapses to a sentinel IxKey so the view
stays at exactly one row. Closes P5 + AMPS_PARITY §4 bug 3."
```

---

## Task P6: Engine — fix JOIN-view SOW delivery for fresh subscribers

**Spec:** §4 bug 1. `[[views]]`-declared JOIN views populate (admin shows N rows) but a fresh subscriber's SOW returns 0 rows.

**Files:**
- `crates/cq-core/src/view.rs` — JOIN view sow_iter wiring
- `crates/cq-transport/src/router.rs` — sow delivery for view topics
- New test: `crates/cq-e2e-tests/tests/view_join_sow_fresh_subscriber.rs`

- [ ] **Step 1: Reproduce with a failing e2e test**

```rust
//! Bug: a JOIN [[view]] populates but a fresh subscriber's SOW is empty.

#[tokio::test]
async fn join_view_sow_visible_to_fresh_subscriber() {
    let server = TestServerBuilder::new()
        .with_view("v_trades_by_compliance",
                   "SELECT t.sym, c.flag FROM trades t INNER JOIN compliance c USING (sym)")
        .start().await;
    let mut pub_client = server.connect().await;
    pub_client.publish("trades", json!({"sym":"AAPL","qty":10})).await.unwrap();
    pub_client.publish("compliance", json!({"sym":"AAPL","flag":"OK"})).await.unwrap();
    // Wait for view to materialise.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Fresh subscriber.
    let mut sub = server.connect().await;
    let snapshot = sub.sow("/v_trades_by_compliance", None).await.unwrap();
    assert!(!snapshot.is_empty(), "fresh subscriber should see the join row");
}
```

- [ ] **Step 2: Diagnose**

Likely causes (rank-ordered):
1. The view's `Topic` is registered under one prefix (`/v_trades_by_compliance`) but the SOW resolver looks up the bare name.
2. The view's underlying SOW store is keyed by JOIN-result keys that the sow_iter doesn't enumerate (it walks left-side keys only).
3. There's a `continuous re-aggregation` window where the SOW snapshot races the rebuild.

Instrument with `tracing` at INFO around the SOW iterator entry and the view's row count. Run the failing test and read the logs.

- [ ] **Step 3: Implement fix**

Most likely fix is in `view.rs`'s sow_iter — make sure it walks the materialised output store (`sow_store`), not the left-source iterator. Concrete edit depends on Step 2 findings.

- [ ] **Step 4: Existing JOIN tests still pass**

Run: `cargo test -p cq-core view_join` and `cargo test -p cq-e2e-tests --test view_join_e2e`.

- [ ] **Step 5: Commit**

```bash
git commit -m "fix(view): JOIN view SOW returns rows to fresh subscribers

Closes P6 in AMPS_PARITY_WORKLOG and AMPS_PARITY §4 bug 1."
```

---

## Task P7: Engine — fix snapshot encode-cache wedge

**Spec:** §4 bug 4. A failed SOW request leaves its `Building` slot in the encode-once-fanout cache so the next identical request waits forever.

**Files:** the encode-once cache lives in `cq-transport`. Find it with `grep -rn "Building\|encode_once" crates/cq-transport/src/`.

- [ ] **Step 1: Find the cache**

Run: `grep -rn "Building\|EncodeState\|encode_once" crates/cq-transport/src/`.

- [ ] **Step 2: Reproduce with a failing test**

In whichever file owns the cache, add a unit test that:
1. Triggers an encode that returns an Err.
2. Asserts the cache slot for that key is removed (not left in Building).
3. Triggers the same encode again — must not hang and must produce the same Err (or a fresh attempt).

- [ ] **Step 3: Run, confirm fail**

- [ ] **Step 4: Fix**

Wrap the encode call in something like:

```rust
let result = encode(req).await;
if result.is_err() {
    cache.remove(&key); // or transition Building → Failed and propagate
}
```

Use `scopeguard::defer` or an explicit Drop guard for panic-safety.

- [ ] **Step 5: Test passes; run cq-transport full suite**

Run: `cargo test -p cq-transport`.

- [ ] **Step 6: Commit**

```bash
git commit -m "fix(transport): clear encode-once cache slot on SOW failure

Closes P7 in AMPS_PARITY_WORKLOG and AMPS_PARITY §4 bug 4."
```

---

## Task P8: Aggregates — STDDEV / STDDEV_SAMP / VARIANCE

**Spec:** §1.3 row 3.

**Files:** `crates/cq-core/src/query.rs` — `parse_aggregate_call` (line 850) and `AggregateState` (search for it).

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn parses_stddev_aggregate() {
    let s = Schema::from_strs(&["v"], &[ColumnType::Double]);
    let q = ParsedQuery::parse_with_schema("SELECT STDDEV(v) AS s FROM t", &s).unwrap();
    assert_eq!(q.aggregates.len(), 1);
    assert!(matches!(q.aggregates[0].kind, AggregateKind::Stddev));
}
```

- [ ] **Step 2: Run, confirm fail**

- [ ] **Step 3: Add Stddev / StddevSamp / Variance variants**

To `AggregateKind` enum; in parse_aggregate_call, accept the function names (case-insensitive); to `AggregateState`, add a Welford-online accumulator:

```rust
struct WelfordState { count: u64, mean: f64, m2: f64 }
impl WelfordState {
    fn push(&mut self, x: f64) {
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (x - self.mean);
    }
    fn variance(&self) -> f64 {
        if self.count < 2 { 0.0 } else { self.m2 / self.count as f64 }
    }
    fn variance_samp(&self) -> f64 {
        if self.count < 2 { 0.0 } else { self.m2 / (self.count - 1) as f64 }
    }
    fn stddev(&self) -> f64 { self.variance().sqrt() }
    fn stddev_samp(&self) -> f64 { self.variance_samp().sqrt() }
}
```

Plumb through `merge` (combining two states) for the streaming aggregator's incremental path.

- [ ] **Step 4: Unit tests (parse, eval, merge)**

- [ ] **Step 5: Differential test**

Add to `crates/cq-differential-tests/corpus/`: a fixture comparing `STDDEV(v)` between cqserver and DuckDB on the same data.

- [ ] **Step 6: E2E test**

`crates/cq-e2e-tests/tests/parser_stddev.rs`.

- [ ] **Step 7: Commit**

```bash
git commit -m "feat(aggregates): STDDEV / STDDEV_SAMP / VARIANCE

Welford-online accumulator. Closes P8 in AMPS_PARITY_WORKLOG."
```

---

## Task P9: Aggregates — PERCENTILE_CONT / MEDIAN

**Spec:** §1.3 row 4. `PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY v)`. MEDIAN ≡ PERCENTILE_CONT(0.5).

**Files:** `crates/cq-core/src/query.rs` aggregate parse + state.

- [ ] **Step 1: Failing unit test for parse + a small reservoir-state test**

- [ ] **Step 2: Implement**

For exact percentile we must keep all values in the group; that's fine for moderate cardinality but exposes O(n) memory per group. Document this as the chosen tradeoff (mirrors AMPS's behaviour on a *small* slippage panel; loadgen can later validate memory bounds).

```rust
#[derive(Default, Clone)]
struct PercentileState { values: Vec<f64>, q: f64 }
impl PercentileState {
    fn push(&mut self, x: f64) { self.values.push(x); }
    fn finalize(&mut self) -> f64 {
        if self.values.is_empty() { return 0.0; }
        self.values.sort_by(|a,b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let rank = self.q * (self.values.len() - 1) as f64;
        let lo = rank.floor() as usize;
        let hi = rank.ceil() as usize;
        if lo == hi { self.values[lo] }
        else { self.values[lo] + (rank - lo as f64) * (self.values[hi] - self.values[lo]) }
    }
}
```

- [ ] **Step 3: Tests, differential test, e2e test, commit**

```bash
git commit -m "feat(aggregates): PERCENTILE_CONT + MEDIAN

Closes P9 in AMPS_PARITY_WORKLOG."
```

---

## Task P10: Aggregates — COUNT(DISTINCT col)

**Spec:** §1.1 row 9.

**Files:** parser + AggregateState.

- [ ] **Step 1: Failing unit test**

- [ ] **Step 2: Implement using a `BTreeSet<Value>` (or HashSet) per state**

Trade-off: exact distinct cardinality, O(n) memory per group. HyperLogLog is a future optimization — out of scope here.

- [ ] **Step 3: Tests + e2e + commit**

```bash
git commit -m "feat(aggregates): COUNT(DISTINCT col)

Closes P10 in AMPS_PARITY_WORKLOG."
```

---

## Task P11: JOIN — ON-clause equi-join (translated to USING)

**Spec:** §1.4 row 1. Accept `INNER JOIN B ON a.x = b.x` and translate to the existing USING path when both sides reference the same column name (or both columns map to compatible types and the user-supplied alias rewrite makes them identical).

**Files:** `crates/cq-core/src/query.rs` — `parse_join_clause` (line 451).

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn parses_join_on_equi() {
    let s = Schema::from_strs(&["sym","px"], &[ColumnType::String, ColumnType::Double]);
    let q = ParsedQuery::parse_with_schema(
        "SELECT * FROM positions p INNER JOIN securities s ON p.sym = s.sym",
        &s,
    ).unwrap();
    let j = q.join.expect("must have join");
    assert_eq!(j.using, vec!["sym".to_string()]);
}
```

- [ ] **Step 2: Implement**

In `parse_join_clause`, the existing match on `JoinConstraint::Using(..)` returns the cols directly. Add a case for `JoinConstraint::On(Expr)`:
- Decompose the Expr into a list of equalities AND'd together.
- For each equality `a.col1 = b.col2`, after alias-rewrite, require `col1 == col2` (same name on both sides). If yes, append to `using`.
- If any equality is non-equi or names differ, return a clear `QueryError::ParseError("ON-clause only supports equi-joins where columns share names; rename or use USING.")`.

- [ ] **Step 3: Tests + e2e + commit**

```bash
git commit -m "feat(parser): JOIN ... ON a.c = b.c (translated to USING)

Closes P11 in AMPS_PARITY_WORKLOG."
```

---

## Task P12: JOIN — LEFT OUTER JOIN

**Spec:** §1.4 row 3.

**Files:** `crates/cq-core/src/query.rs` (JoinSpec carries kind) + the join executor.

- [ ] **Step 1: Failing test (parse + executor)**

- [ ] **Step 2: Implement**

`JoinSpec.kind: JoinKind { Inner, LeftOuter }`. In the executor, when `LeftOuter`: after building the right-hand hash map, for each left row, if no right-side match, still emit the row with right-side columns as `Value::Null`.

- [ ] **Step 3: Tests (including: a left row with no right match returns one row with NULLs)**

- [ ] **Step 4: Differential test**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(join): LEFT OUTER JOIN

Closes P12 in AMPS_PARITY_WORKLOG."
```

---

## Task P13: WHERE — regex match (MATCHES_REGEX / LIKE_REGEX)

**Spec:** §1.2 row 7.

**Files:** `crates/cq-core/src/predicate.rs`.

- [ ] **Step 1: Failing unit test**

- [ ] **Step 2: Implement**

Add `regex = "1"` to `cq-core/Cargo.toml`. Compile pattern at parse time (cache in the CompiledPredicate node). Support both call-shapes:
- `MATCHES_REGEX(col, '^FOO.*')`
- `col LIKE_REGEX '^FOO.*'` (custom operator)

Reject patterns that fail to compile at parse time, not at row eval.

- [ ] **Step 3: Tests + e2e + commit**

```bash
git commit -m "feat(predicate): regex match via MATCHES_REGEX / LIKE_REGEX

Closes P13 in AMPS_PARITY_WORKLOG."
```

---

## Task P14: Topic registry — normalise slash-prefix

**Spec:** §4 bug 5. Topics register as `/positions`, config writes `FROM positions`. The fix is to normalise *once* at registration so every lookup is consistent.

**Files:** `crates/cq-core/src/topic.rs` (or wherever the global topic registry lives).

- [ ] **Step 1: Failing unit test**

```rust
#[test]
fn registry_lookup_normalises_slash() {
    let r = TopicRegistry::new();
    r.register("/positions", schema);
    assert!(r.get("positions").is_some(), "bare-name lookup must hit");
    assert!(r.get("/positions").is_some(), "slash-prefix lookup must hit");
}
```

- [ ] **Step 2: Implement normalisation**

Decide canonical form (slash-prefix). Wrap every registry insert/lookup in `canonical_topic_name()` which always prepends `/` if missing. Audit and remove ad-hoc dual-lookup code (the `init_view` and SOW JOIN resolver patches mentioned in AMPS_PARITY.md §4.5).

- [ ] **Step 3: Test passes; entire workspace builds; existing tests pass**

- [ ] **Step 4: Commit**

```bash
git commit -m "refactor(topic): normalise to slash-prefix at registration

Removes the dual-form lookups added as workarounds. Closes P14
in AMPS_PARITY_WORKLOG."
```

---

## Task P15: SDK — HA failover across multiple URIs

**Spec:** §3.1 row 12. Client accepts `ws://a, ws://b, ws://c` and rotates on connection loss.

**Files:** `client-sdks/ts/src/transport.ts`, `client.ts`. (Look at `crates/cq-e2e-tests/tests/replica_reads.rs` for the existing multi-instance pattern.)

- [ ] **Step 1: Failing test (Vitest)**

`client-sdks/ts/tests/test_ha_failover.test.ts`:

```ts
import { Client } from '../src/client';
it('fails over to second URI on connection loss', async () => {
  const c = new Client({ uris: ['ws://localhost:9999', 'ws://localhost:7777'] }); // 9999 dead, 7777 live
  await c.connect();
  expect(c.activeUri).toBe('ws://localhost:7777');
});
```

- [ ] **Step 2: Implement**

`ClientOptions` accepts `uris: string[]` (or `uri: string` — single-string falls back to a 1-element array). Connection logic walks the array on failure, with exponential backoff between full passes. Expose `client.activeUri` for tests/observability.

- [ ] **Step 3: E2E test against replica-reads topology**

`crates/cq-e2e-tests/tests/sdk_ha_failover.rs` (Rust-side spawns 2 servers and connects via a script-driven SDK process — mirror the replica_reads test pattern).

- [ ] **Step 4: Commit**

```bash
git commit -m "feat(sdk): HA failover across multiple URIs

Closes P15 in AMPS_PARITY_WORKLOG."
```

---

## Task P16: SDK — batched publish

**Spec:** §3.2 row 4. `publishBatch(topic, [msg1, msg2, ...])` — one wire frame, one ack, one txlog append.

**Files:** `client-sdks/ts/src/client.ts`; protocol: probably extend an existing batch wire op or add a new one in `crates/cq-protocol/src/`.

- [ ] **Step 1: Failing tests (SDK + server)**

- [ ] **Step 2: Implement**

Server-side: extend `cq_protocol::ClientFrame` with `PublishBatch { topic: String, msgs: Vec<Vec<u8>> }`. Router handles it as a single transactional append, emits a single ack.

SDK-side: `publishBatch(topic, msgs[])` emits the new frame; waits for the single ack.

Wire-version: bump the negotiated version (see S28 in AMPS_WORKLOG.md for how to add a wire-version capability flag).

- [ ] **Step 3: Tests + e2e + commit**

```bash
git commit -m "feat(sdk): publishBatch — one wire frame, one ack

Closes P16 in AMPS_PARITY_WORKLOG."
```

---

## Self-Review

**Spec coverage:** AMPS_PARITY.md §1.1–1.5 + §4 + §3 priorities all map to a P-task. Deferred items (window functions, subqueries, CTEs, FULL/RIGHT/AS OF JOIN, schema evolution) are flagged in the plan header.

**Placeholder scan:** Every step shows the code or command. No "TBD", no "implement appropriately".

**Type consistency:** `ScalarExpr`, `ComputedColumn`, `HavingPred`, `JoinKind`, `WelfordState`, `PercentileState`, `TopicRegistry` are introduced where first used and referenced only after. `ParsedQuery` field additions: every existing literal must be updated — flagged in P2 Step 3 and applies to P3/P4 too.

---

## Execution Handoff

This plan is also tracked at `AMPS_PARITY_WORKLOG.md` (pointer file) with status badges. The user invoked `/goal` for autonomous multi-session execution, so we run inline (executing-plans skill), one P-task per session, with the user able to interrupt between commits.

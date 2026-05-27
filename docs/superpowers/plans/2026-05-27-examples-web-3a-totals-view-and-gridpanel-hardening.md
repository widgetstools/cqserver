# examples-web 3a — Server Totals View + GridPanel Render Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove ex01's client-side grand-total summation (rule-3 violation) by sourcing KPIs from a new server `/v_book_totals` degenerate-aggregate view, and stop `GridPanel` from re-rendering on every tick by memoizing it with stable props.

**Architecture:** Add a no-GROUP-BY aggregate view to `cqserver.toml` that emits one continuously-updated totals row; subscribe to it in ex01 and read the row directly (no client sum). Wrap `GridPanel` in `React.memo` and feed it a stable module-level `getRowId` so topic-bound grids render once and update only via `applyTransactionAsync`.

**Tech Stack:** Rust (cqserver config — no code), React 19 + TypeScript + Vite (examples-web), the existing `useFilteredSubscription` hook + AG Grid imperative binding.

This is **Plan 3a of Sub-project 3** in `docs/superpowers/specs/2026-05-27-server-driven-examples-rewrite-design.md`. The Query Builder catalog + live-everywhere work is a separate Plan 3b. Sub-project 1 (the `/admin/catalog` + `POST /admin/views` server endpoints) is already landed.

**Verification note:** examples-web has **no test runner** (only `dev`/`build`/`typecheck`). Client tasks are verified with `npm run typecheck` (which runs `tsc -b`) + `npm run build`, plus a final manual browser check. The config view is verified by booting cqserver and querying the catalog. Capability already proven by `crates/cq-e2e-tests/tests/degenerate_aggregate_view_e2e.rs` (degenerate aggregate views stay single-row and update live).

---

## File Structure

- **Modify** `config/cqserver.toml` — add a `[[views]]` block `/v_book_totals` (degenerate aggregate over `/positions`).
- **Modify** `clients/examples-web/src/lib/use-filtered-subscription.ts` — add `/v_book_totals` to the `TopicName` union and the `KEY_OF` map (single-row view → constant key).
- **Modify** `clients/examples-web/src/examples/ex01-live-pnl/index.tsx` — subscribe to `/v_book_totals`; replace the `kpis` summation loop with a read of the single totals row; hoist `getRowId` to a stable module-level constant.
- **Modify** `clients/examples-web/src/components/panels/GridPanel.tsx` — wrap the export in `React.memo` (generics preserved).

---

## Task 1: Add the `/v_book_totals` server view

**Files:**
- Modify: `config/cqserver.toml` (append a `[[views]]` block alongside the existing views, e.g. right after the `/v_pnl_by_book` block).

The aggregate aliases MUST match the field names ex01 already reads from `/v_pnl_by_book` (`exposure_gross`, `market_value`, `unrealized_pnl`, `realized_pnl`, `day_pnl`, `ytd_pnl`, `var_95`, `n_positions`), so the KPI rewrite in Task 3 reads identical keys. Source columns (`exposure_gross`, `market_value_usd`, `unrealized_pnl_usd`, `realized_pnl_usd`, `day_pnl`, `ytd_pnl`, `var_1d_95`) are confirmed present on `/positions`.

- [ ] **Step 1: Add the view block**

Append to `config/cqserver.toml` (in the `[[views]]` region):

```toml
[[views]]
name = "/v_book_totals"
source = "/positions"
sql = """
  SELECT SUM(exposure_gross)      AS exposure_gross,
         SUM(market_value_usd)    AS market_value,
         SUM(unrealized_pnl_usd)  AS unrealized_pnl,
         SUM(realized_pnl_usd)    AS realized_pnl,
         SUM(day_pnl)             AS day_pnl,
         SUM(ytd_pnl)             AS ytd_pnl,
         SUM(var_1d_95)           AS var_95,
         COUNT(*)                 AS n_positions
  FROM positions
"""
```

- [ ] **Step 2: Build the server and boot it against this config**

Run:
```bash
cargo build --release -p cq-server
./target/release/cqserver --config config/cqserver.toml &
CQ_PID=$!
sleep 2
```
Expected: startup logs show `Materialized view ready` for `/v_book_totals` and **no** `view '/v_book_totals': ...` error. (If boot fails with a parse/limit error, the SQL or a column name is wrong — fix before continuing.)

- [ ] **Step 3: Confirm the view is registered via the catalog**

Run:
```bash
curl -s localhost:8085/admin/catalog | jq '.[] | select(.name=="/v_book_totals") | {name, kind, cols: (.columns|map(.name))}'
```
Expected: one object, `kind: "view"`, `cols` containing `exposure_gross`, `market_value`, `unrealized_pnl`, `realized_pnl`, `day_pnl`, `ytd_pnl`, `var_95`, `n_positions`.

- [ ] **Step 4: Stop the server**

Run: `kill $CQ_PID 2>/dev/null; wait $CQ_PID 2>/dev/null; true`

- [ ] **Step 5: Commit**

```bash
git add config/cqserver.toml
git commit -m "feat(config): /v_book_totals degenerate-aggregate view for ex01 KPIs

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Register `/v_book_totals` in the client subscription hook

**Files:**
- Modify: `clients/examples-web/src/lib/use-filtered-subscription.ts` — the `TopicName` union (around lines 36-51) and the `KEY_OF` map (around lines 54-70).

The view emits exactly one row (degenerate aggregate). The client row-id extractor must return a **stable constant** so the single row is matched as an update across delta batches (never re-added).

- [ ] **Step 1: Add the topic to the `TopicName` union**

In the `TopicName` type, add a member alongside the other `/v_*` views:

```ts
  | '/v_book_totals'
```

- [ ] **Step 2: Add the `KEY_OF` entry**

In the `KEY_OF` record, add (a constant key — the view is single-row):

```ts
  '/v_book_totals': () => 'TOTAL',
```

- [ ] **Step 3: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: no errors. (A missing `KEY_OF` entry for the new union member would be a `Record` exhaustiveness type error — so this step proves both edits are consistent.)

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/lib/use-filtered-subscription.ts
git commit -m "feat(examples-web): allow /v_book_totals subscriptions

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Source ex01 KPIs from the totals view (remove client summation)

**Files:**
- Modify: `clients/examples-web/src/examples/ex01-live-pnl/index.tsx` — add a `/v_book_totals` subscription (near the existing `sectorSub`/`bookSub`/`complianceSub` around lines 30-32) and replace the `kpis` `useMemo` (around lines 37-61).

Keep `bookSub` — it still drives the Book Contribution waterfall (`bookPnL`). Only the KPI grand-total **summation** is removed.

- [ ] **Step 1: Add the totals subscription**

Next to the existing subs:

```ts
  const sectorSub = useFilteredSubscription('/v_pnl_by_sector', null);
  const bookSub = useFilteredSubscription('/v_pnl_by_book', null);
  const complianceSub = useFilteredSubscription('/v_compliance_counts', null);
  // Server-side grand totals — a single-row degenerate-aggregate view.
  // The KPI strip reads this row directly; no client-side summation.
  const totalsSub = useFilteredSubscription('/v_book_totals', null);
```

- [ ] **Step 2: Replace the `kpis` useMemo body**

Replace the existing `kpis` `useMemo` (the version that loops `for (const r of bookSub.rows) { gross += ... }`) with this version that reads the single totals row:

```ts
  const kpis = useMemo<Kpi[]>(() => {
    // The totals view emits exactly one row holding the live grand totals.
    const t = totalsSub.rows[0] ?? {};
    const gross = num(t.exposure_gross);
    const net   = num(t.market_value);
    const upnl  = num(t.unrealized_pnl);
    const rpnl  = num(t.realized_pnl);
    const day   = num(t.day_pnl);
    const ytd   = num(t.ytd_pnl);
    const var95 = num(t.var_95);
    const nPos  = num(t.n_positions);
    const breachRow = complianceSub.rows.find((r) => r.compliance_status === 'BREACH');
    const breaches = num(breachRow?.n_positions);
    return [
      { label: 'Gross Exposure',   value: gross,    kind: 'ccy',         sub: `${nPos} positions` },
      { label: 'Net MV',           value: net,      kind: 'signed-ccy',  delta: day * 0.1 },
      { label: 'Unrealized PnL',   value: upnl,     kind: 'signed-ccy',  delta: day },
      { label: 'Realized PnL',     value: rpnl,     kind: 'signed-ccy' },
      { label: 'Day PnL',          value: day,      kind: 'signed-ccy',  delta: day * 0.04, sub: 'vs t-1 close' },
      { label: 'YTD PnL',          value: ytd,      kind: 'signed-ccy',  sub: 'inception' },
      { label: 'VaR (1d, 95%)',    value: var95,    kind: 'ccy',         sub: 'sum of pos VaR' },
      { label: 'Compliance Brchs', value: breaches, kind: 'count',       sub: 'BREACH status' },
    ];
  }, [totalsSub.rows, complianceSub.rows]);
```

(The returned KPI array is unchanged from the current code — only the value derivation changed from a `bookSub.rows` loop to reads off `totalsSub.rows[0]`. If the current `Kpi` objects differ in any field, preserve the CURRENT objects and only swap the value expressions.)

- [ ] **Step 3: Remove the now-stale "sum 8 rows" comment**

Delete the block comment in this file that explains summing `/v_pnl_by_book`'s 8 rows on the client for grand totals (it described the behavior we just removed). Leave surrounding comments intact.

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build`
Expected: typecheck clean; build succeeds. (If `num` is reported unused because it no longer appears elsewhere — it is still used by `sectorPnL`/`bookPnL`, so it should remain used; if not, leave it, it's imported/defined locally.)

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/examples/ex01-live-pnl/index.tsx
git commit -m "refactor(ex01): KPIs from server /v_book_totals, no client summation

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Memoize GridPanel + stabilize ex01's getRowId

**Files:**
- Modify: `clients/examples-web/src/components/panels/GridPanel.tsx` — wrap the export in `React.memo`, preserving generics.
- Modify: `clients/examples-web/src/examples/ex01-live-pnl/index.tsx` — hoist the inline `getRowId` arrow to a stable module-level constant so `React.memo` actually holds for the Positions grid.

- [ ] **Step 1: Memoize the GridPanel export**

In `GridPanel.tsx`, the component is currently declared `export function GridPanel<T extends Record<string, unknown>>({ ... }: GridPanelProps<T>) { ... }`.

Rename the declaration to `function GridPanelInner<...>(...)` (keep the body identical), and add a memoized export below it that preserves the generic signature:

```ts
// Memoized so a topic/liveSubscription-bound grid renders once (to seed
// the SOW) and thereafter updates only via applyTransactionAsync. The
// live tick counter lives in the GridStatsBadge leaf, so it no longer
// drags the whole panel into a per-tick re-render. React.memo only
// holds when callers pass STABLE props (notably getRowId) — see ex01.
export const GridPanel = React.memo(GridPanelInner) as typeof GridPanelInner;
```

Add the `React` import if not already present: at the top, the file imports hooks from `'react'`. Add a default import — change the existing `import { useEffect, ... } from 'react';` to also bring in the namespace:

```ts
import React, { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
```

(If a default `React` import already exists, don't duplicate it.)

- [ ] **Step 2: Hoist ex01's getRowId to a stable constant**

In `ex01-live-pnl/index.tsx`, at module scope (top level, after imports, near the existing `const num = ...` helper), add:

```ts
// Stable identity so React.memo on GridPanel holds — an inline
// `getRowId={(r) => ...}` would be a fresh function each render and
// defeat the memo, re-applying columnDefs on every parent re-render.
const positionRowId = (r: Record<string, unknown>) => r.position_id as string;
```

Then in the Positions `GridPanel` usage, replace `getRowId={(r) => r.position_id as string}` with:

```tsx
          getRowId={positionRowId}
```

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build`
Expected: typecheck clean; build succeeds. (The `as typeof GridPanelInner` cast preserves the generic call signature so existing `<GridPanel ...>` usages across examples still type-check.)

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/components/panels/GridPanel.tsx clients/examples-web/src/examples/ex01-live-pnl/index.tsx
git commit -m "perf(examples-web): memoize GridPanel + stable getRowId in ex01

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Manual end-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Launch the full demo**

Run: `./start-atlas-demo.sh`
Expected: cqserver (admin `:8085`), the Atlas publisher (ticks `/positions`), and examples-web (`:5175`) all come up. (`./stop-demo.sh` to tear down afterward.)

- [ ] **Step 2: Verify the totals view is live**

Run:
```bash
curl -s localhost:8085/admin/catalog | jq '.[] | select(.name=="/v_book_totals").kind'
```
Expected: `"view"`.

- [ ] **Step 3: Verify ex01 KPIs in the browser**

Open `http://localhost:5175`, go to the Live PnL tab. Expected: the KPI strip (Gross Exposure, Net MV, Unrealized PnL, …, Compliance Brchs) shows non-zero values and updates as the publisher ticks — now driven by the single `/v_book_totals` row, not client summation.

- [ ] **Step 4: Verify the Positions grid no longer storm-renders**

With React DevTools (Profiler/“Highlight updates”) on the Positions grid: cells flash on value change (via `applyTransactionAsync`), but the `GridPanel` component itself should not re-render on every tick (only `GridStatsBadge` updates the tick counter). This confirms the memo + stable `getRowId` hold.

- [ ] **Step 5: Tear down**

Run: `./stop-demo.sh`

No commit (verification only). If Step 3 or 4 fails, fix the relevant task before considering 3a complete.

---

## Self-Review (completed by author)

**Spec coverage** (design doc, Sub-project 3, ex01 + GridPanel items):
- ex01 client-side grand-total summation → server `/v_book_totals` view → Tasks 1-3. ✅
- GridPanel `React.memo` + stabilized `getRowId` so view-backed grids seed once → Task 4. ✅
- Tick-badge isolation (the original fix) already landed pre-plan (committed in `d2e3e50`); this plan builds on it. ✅
- Query Builder catalog + live/static, and other examples' getRowId stabilization → deliberately deferred to Plan 3b (out of scope here). Noted.

**Placeholder scan:** No TBD/TODO/"handle errors"; every code step is concrete. ✅

**Type/name consistency:** `/v_book_totals` view aliases (`exposure_gross`, `market_value`, `unrealized_pnl`, `realized_pnl`, `day_pnl`, `ytd_pnl`, `var_95`, `n_positions`) match exactly the keys read in Task 3's `kpis` and the field names ex01 already used from `/v_pnl_by_book`. `TopicName` union member `/v_book_totals` (Task 2) matches the subscription call in Task 3. `GridPanelInner`/`GridPanel` rename (Task 4) is internally consistent. ✅

**Scope:** Single coherent slice (totals view + memo). Self-contained and independently shippable; no dependency on the not-yet-built catalog/admin-screen. ✅

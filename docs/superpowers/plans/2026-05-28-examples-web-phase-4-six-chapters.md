# examples-web Phase 4 — Six Middle Chapters

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the six middle Atlas chapters (Tape, Lens, Heat, View, Join, Slip) to real cqserver data using the Plan A primitives (`useChapterScope`, `<DataTable liveSubscription>`, per-chapter scope file). After this plan, only stations 01-07 of 08 render real chapters; chapter 08 (Query Builder) is the only `ComingSoon` stub left for Plan C.

**Architecture:** Each chapter is a pair of files — `src/atlas/scopes/<name>.ts` (chips, KPI mapping, column defs) + `src/atlas/chapters/<Name>Chapter.tsx` (worker subscriptions + UI composition). The component flow is identical to Pulse: `useChapterScope(CHIPS)` for filter state, one filtered `useSubscription` for the primary topic feeding `<DataTable liveSubscription>`, and view subscriptions for KPI rows / chip option lists. Chapters whose primary subscription is itself a small aggregate view (`/v_net_exposure`, `/v_slippage_venue_algo`, etc.) compute their KPI strip from the same view rows in a `useMemo` — that's still server-aggregated data; only the headline rollup is local. Tape uses `useFilteredAggregate` (the SQL-flavour worker hook from Phase 2) for its KPI strip.

**Tech Stack:** React 19 + TypeScript + Vite, AG-Grid v35, `useSubscription` / `useFilteredAggregate` over the SharedWorker port.

---

## Pre-flight

`cd /Users/develop/cqserver`. Verify the tip:

```bash
git log --oneline -5
# Tip should be 3d9843a5 fix(atlas): default BOOK chip to a real seeded book name
```

Demo must be up with cqserver + publisher running (so all six views have rows). If not: `POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh`.

**Pre-existing WIP everywhere.** Precise `git add <paths>` only — never `-A`.

**Atomicity:** every commit keeps the demo working. Each task migrates ONE chapter and immediately wires its `<Chapter />` component into `AtlasPreviewApp.tsx`'s switch (replacing that chapter's `<ComingSoon />`).

---

## Cqserver views consumed (locked, from `/admin/catalog`)

- `/v_cross_asset_pivot` — `asset_class`, `currency`, `market_value_usd`, `unrealized_pnl_usd`, `var_1d_95`, `exposure_gross`, `n_positions`.
- `/v_heatmap_sector_region` — `issuer_sector`, `issuer_region`, `weighted_sum`, `weight`, `n_positions`.
- `/v_net_exposure` — `book_id`, `book_name`, `asset_class`, `currency`, `net_mv_usd`, `gross_exposure`, `net_dv01`, `sum_var`, `worst_util_pct`, `n_positions`.
- `/v_trades_by_compliance` — `compliance_status`, `n_trades`, `total_fees`, `avg_slip_arr`.
- `/v_slippage_venue_algo` — `execution_venue`, `execution_algo`, `n_trades`, `avg_slip_arr`, `avg_slip_vwap`, `max_slip_arr`, `min_slip_arr`, `total_fees`.
- `/trades` — large topic. Filterable by `side`, `status`, `compliance_review_status`.

Seeded BOOKS list (from `clients/examples-web/src/lib/refdata.ts`): `Global Macro`, `Equity Long-Short`, `Credit Relative Val`, `Vol Arbitrage`, `Index Replication`, `Rates Curve`, `EM Sovereign`, `High-Yield Carry`. Used as a chip-default reference.

---

## File map

| Path | Status |
|---|---|
| `clients/examples-web/src/atlas/scopes/tape.ts` | new |
| `clients/examples-web/src/atlas/chapters/TapeChapter.tsx` | new |
| `clients/examples-web/src/atlas/scopes/lens.ts` | new |
| `clients/examples-web/src/atlas/chapters/LensChapter.tsx` | new |
| `clients/examples-web/src/atlas/scopes/heat.ts` | new |
| `clients/examples-web/src/atlas/chapters/HeatChapter.tsx` | new |
| `clients/examples-web/src/atlas/scopes/view.ts` | new |
| `clients/examples-web/src/atlas/chapters/ViewChapter.tsx` | new |
| `clients/examples-web/src/atlas/scopes/join.ts` | new |
| `clients/examples-web/src/atlas/chapters/JoinChapter.tsx` | new |
| `clients/examples-web/src/atlas/scopes/slip.ts` | new |
| `clients/examples-web/src/atlas/chapters/SlipChapter.tsx` | new |
| `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx` | modified (6 times, once per chapter) |

Each chapter task creates a scope + component pair AND wires it into AtlasPreviewApp's switch in the same commit.

---

## Task 1: Tape (Chapter 02 — Trade Blotter)

Primary topic `/trades` filtered server-side by chips. KPI strip from `useFilteredAggregate('/trades', sql)` (the Phase 2 SQL-flavour worker hook).

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/tape.ts`
- Create: `clients/examples-web/src/atlas/chapters/TapeChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `tape.ts`**

```ts
/**
 * Tape (Chapter 02 — Trade Blotter) scope.
 * Live trade tape filtered server-side; KPI strip from a continuous SQL aggregate.
 */
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const TAPE_CHIPS: readonly ChipSpec[] = [
  { key: 'SIDE', column: 'side', source: '/trades' },
  { key: 'STATUS', column: 'status', source: '/trades' },
];

export const TAPE_COL_DEFS: ColDef[] = [
  { field: 'trade_id', headerName: 'trade_id', width: 130, cellStyle: { color: '#f4a52b' } },
  { field: 'position_id', headerName: 'position_id', width: 130 },
  { field: 'symbol', headerName: 'symbol', width: 90 },
  { field: 'side', headerName: 'side', width: 70,
    cellStyle: (p) =>
      p.value === 'BUY' ? { color: '#7ec96a' } : p.value === 'SELL' ? { color: '#ff6062' } : null },
  { field: 'quantity', headerName: 'qty', width: 100, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'price', headerName: 'price', width: 100, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toFixed(4) ?? '—' },
  { field: 'notional_usd', headerName: 'notional_usd', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'status', headerName: 'status', width: 110,
    cellStyle: { color: '#f4a52b', letterSpacing: '.1em' } },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

export function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `TapeChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { useFilteredAggregate } from '@/lib/use-filtered-aggregate';
import { TAPE_CHIPS, TAPE_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/tape';

const tradeRowId = (r: Row): string => String(r.trade_id ?? '');

export function TapeChapter() {
  const scope = useChapterScope(TAPE_CHIPS);

  // /trades is a big topic — we still subscribe to ALL of it for chip options
  // (it's the only source of distinct side/status values). The primary
  // subscription below is server-filtered.
  const allTradesSub = useSubscription('/trades', null);
  const tradesSub = useSubscription('/trades', scope.filterExpression, tradeRowId);

  // Server-side aggregate for KPIs — re-emits whenever any matching trade changes.
  const aggSql = useMemo(() => {
    const where = scope.filterExpression ? `WHERE ${scope.filterExpression}` : '';
    return `SELECT COUNT(*) AS n_trades,
                   SUM(notional_usd) AS total_notional,
                   AVG(slippage_arrival_bps) AS avg_slip,
                   SUM(total_fees) AS total_fees
            FROM trades ${where}`;
  }, [scope.filterExpression]);
  const agg = useFilteredAggregate('/trades', aggSql);

  const chipOptions = useMemo(
    () => ({
      SIDE: ['All', ...distinctValues(allTradesSub.rows, 'side')],
      STATUS: ['All', ...distinctValues(allTradesSub.rows, 'status')],
    }),
    [allTradesSub.rows],
  );

  const kpis = useMemo<Kpi[]>(() => {
    const r = (agg.row ?? {}) as Record<string, unknown>;
    return [
      { label: 'N TRADES', value: fmtCount(Number(r.n_trades ?? 0)), caption: 'in scope', emphasis: true },
      { label: 'NOTIONAL', value: fmtMillions(Number(r.total_notional ?? 0)), caption: 'sum · usd', emphasis: true },
      { label: 'AVG SLIP', value: fmtBps(Number(r.avg_slip ?? 0)), caption: 'arrival · weighted' },
      { label: 'FEES', value: fmtMillions(Number(r.total_fees ?? 0)), caption: 'sum · usd' },
    ];
  }, [agg.row]);

  const heroValue = useMemo(() => {
    const r = (agg.row ?? {}) as Record<string, unknown>;
    return fmtCount(Number(r.n_trades ?? 0));
  }, [agg.row]);

  const status =
    tradesSub.status === 'live'
      ? `${tradesSub.size.toLocaleString()} trades · live`
      : `${tradesSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 02 — TAPE"
        title="tape."
        sub="The live trade tape — every execution flowing through the firm, server-filtered by side and status. Aggregate KPIs come from a continuous SQL aggregate on /trades; nothing summed in the browser."
        hero={<HeroMetric label="TRADES" value={heroValue} detail="in current scope" />}
      />
      <FilterRail
        chips={[...TAPE_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`TRADES · 8 of 205 cols`}
        status={status}
        colDefs={TAPE_COL_DEFS}
        getRowId={tradeRowId}
        liveSubscription={tradesSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Open `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`. Add the import:

```tsx
import { TapeChapter } from '../chapters/TapeChapter';
```

And change the `<main>` body's conditional. The current single-line ternary becomes a chained ternary:

```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/tape.ts \
        clients/examples-web/src/atlas/chapters/TapeChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): TapeChapter — Chapter 02 trade blotter on real /trades

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Lens (Chapter 03 — Cross-Asset Pivot)

Primary subscription `/v_cross_asset_pivot` — already a small aggregate view (one row per asset_class × currency). KPI strip rolls up its rows in `useMemo`. No chips for Phase 4; Phase 6 polish can add drill-through.

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/lens.ts`
- Create: `clients/examples-web/src/atlas/chapters/LensChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `lens.ts`**

```ts
import type { ColDef } from 'ag-grid-community';

export const LENS_COL_DEFS: ColDef[] = [
  { field: 'asset_class', headerName: 'asset_class', width: 130, cellStyle: { color: '#f4a52b' } },
  { field: 'currency', headerName: 'ccy', width: 80 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'market_value_usd', headerName: 'market_value', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'unrealized_pnl_usd', headerName: 'unrealized_pnl', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
  { field: 'exposure_gross', headerName: 'gross', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'var_1d_95', headerName: 'var_1d', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(1)}M`;
}

export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `LensChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { LENS_COL_DEFS, fmtMillions, fmtSignedMillions, fmtCount } from '../scopes/lens';

const pivotRowId = (r: Row): string =>
  `${String(r.asset_class ?? '')}|${String(r.currency ?? '')}`;

export function LensChapter() {
  const scope = useChapterScope([]); // no chips for Phase 4
  const pivotSub = useSubscription('/v_cross_asset_pivot', null, pivotRowId);

  // Headline rollup from the view rows. The view IS already server-aggregated;
  // this just sums the buckets for a one-line headline. No raw-topic aggregation.
  const totals = useMemo(() => {
    let mv = 0, pnl = 0, var95 = 0, gross = 0, n = 0;
    for (const r of pivotSub.rows) {
      mv += Number(r.market_value_usd ?? 0);
      pnl += Number(r.unrealized_pnl_usd ?? 0);
      var95 += Number(r.var_1d_95 ?? 0);
      gross += Number(r.exposure_gross ?? 0);
      n += Number(r.n_positions ?? 0);
    }
    return { mv, pnl, var95, gross, n };
  }, [pivotSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(pivotSub.rows.length), caption: 'asset × ccy', emphasis: true },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across all buckets' },
      { label: 'MARKET VALUE', value: fmtMillions(totals.mv), caption: 'sum · usd', emphasis: true },
      { label: 'UNREALISED', value: fmtSignedMillions(totals.pnl), caption: 'sum · usd', emphasis: true },
      { label: 'EXPOSURE', value: fmtMillions(totals.gross), caption: 'gross · sum' },
      { label: 'VaR (1d)', value: fmtMillions(totals.var95), caption: 'sum of buckets' },
    ],
    [pivotSub.rows.length, totals],
  );

  const status =
    pivotSub.status === 'live'
      ? `${pivotSub.size.toLocaleString()} buckets · live`
      : `${pivotSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 03 — LENS"
        title="lens."
        sub="Cross-asset pivot — the firm's book sliced by asset class × currency. Each bucket is server-computed; the table is the materialized view itself."
        hero={<HeroMetric label="UNREALISED" value={fmtSignedMillions(totals.pnl)} detail="across all buckets" />}
      />
      <FilterRail
        chips={[]}
        state={scope.state}
        options={{}}
        onChange={scope.setState}
        subscriptionSummary={`/v_cross_asset_pivot · ${pivotSub.size} rows`}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`PIVOT · ${LENS_COL_DEFS.length} cols`}
        status={status}
        colDefs={LENS_COL_DEFS}
        getRowId={pivotRowId}
        liveSubscription={pivotSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Add the import:
```tsx
import { LensChapter } from '../chapters/LensChapter';
```

Extend the chained ternary with the `'lens'` case:
```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : active === 'lens' ? <LensChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/lens.ts \
        clients/examples-web/src/atlas/chapters/LensChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): LensChapter — Chapter 03 cross-asset pivot on /v_cross_asset_pivot

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Heat (Chapter 04 — Ticking Heatmap)

Primary subscription `/v_heatmap_sector_region` — sector × region grid. No chips; table is the heatmap itself (Phase 6 polish can add a heatmap-style cell renderer).

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/heat.ts`
- Create: `clients/examples-web/src/atlas/chapters/HeatChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `heat.ts`**

```ts
import type { ColDef } from 'ag-grid-community';

export const HEAT_COL_DEFS: ColDef[] = [
  { field: 'issuer_sector', headerName: 'sector', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'issuer_region', headerName: 'region', width: 140 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'weight', headerName: 'weight', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtPct(p.value as number) },
  { field: 'weighted_sum', headerName: 'weighted_sum', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
];

export function fmtPct(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${(n * 100).toFixed(2)}%`;
}

export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `HeatChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { HEAT_COL_DEFS, fmtSignedMillions, fmtCount } from '../scopes/heat';

const heatmapRowId = (r: Row): string =>
  `${String(r.issuer_sector ?? '')}|${String(r.issuer_region ?? '')}`;

export function HeatChapter() {
  const scope = useChapterScope([]);
  const heatSub = useSubscription('/v_heatmap_sector_region', null, heatmapRowId);

  const totals = useMemo(() => {
    let n = 0, weight = 0, weightedSum = 0;
    let sectors = new Set<string>();
    let regions = new Set<string>();
    for (const r of heatSub.rows) {
      n += Number(r.n_positions ?? 0);
      weight += Number(r.weight ?? 0);
      weightedSum += Number(r.weighted_sum ?? 0);
      if (r.issuer_sector) sectors.add(String(r.issuer_sector));
      if (r.issuer_region) regions.add(String(r.issuer_region));
    }
    return { n, weight, weightedSum, nSectors: sectors.size, nRegions: regions.size };
  }, [heatSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'CELLS', value: fmtCount(heatSub.rows.length), caption: 'sector × region', emphasis: true },
      { label: 'SECTORS', value: fmtCount(totals.nSectors), caption: 'distinct' },
      { label: 'REGIONS', value: fmtCount(totals.nRegions), caption: 'distinct' },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across all cells' },
      { label: 'WEIGHTED SUM', value: fmtSignedMillions(totals.weightedSum), caption: 'sum · usd', emphasis: true },
    ],
    [heatSub.rows.length, totals],
  );

  const status =
    heatSub.status === 'live'
      ? `${heatSub.size.toLocaleString()} cells · live`
      : `${heatSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 04 — HEAT"
        title="heat."
        sub="The sector × region heatmap, recomputed by cqserver whenever any position mutates. Every cell is a continuous group aggregate; the browser just renders what the view emits."
        hero={<HeroMetric label="CELLS" value={fmtCount(heatSub.rows.length)} detail={`${totals.nSectors} sectors × ${totals.nRegions} regions`} />}
      />
      <FilterRail
        chips={[]}
        state={scope.state}
        options={{}}
        onChange={scope.setState}
        subscriptionSummary={`/v_heatmap_sector_region · ${heatSub.size} cells`}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`HEATMAP · ${HEAT_COL_DEFS.length} cols`}
        status={status}
        colDefs={HEAT_COL_DEFS}
        getRowId={heatmapRowId}
        liveSubscription={heatSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Add import:
```tsx
import { HeatChapter } from '../chapters/HeatChapter';
```

Extend ternary with `'heat'`:
```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : active === 'lens' ? <LensChapter />
  : active === 'heat' ? <HeatChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/heat.ts \
        clients/examples-web/src/atlas/chapters/HeatChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): HeatChapter — Chapter 04 sector×region heatmap

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: View (Chapter 05 — Materialized View)

Primary subscription `/v_net_exposure` — net exposure per (book × asset × ccy). Chips on BOOK + ASSET_CLASS + CURRENCY, sourced from the same view's distinct columns.

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/view.ts`
- Create: `clients/examples-web/src/atlas/chapters/ViewChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `view.ts`**

```ts
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const VIEW_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', source: '/v_net_exposure' },
  { key: 'ASSET', column: 'asset_class', source: '/v_net_exposure' },
  { key: 'CCY', column: 'currency', source: '/v_net_exposure' },
];

export const VIEW_COL_DEFS: ColDef[] = [
  { field: 'book_name', headerName: 'book', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'asset_class', headerName: 'asset', width: 120 },
  { field: 'currency', headerName: 'ccy', width: 70 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'net_mv_usd', headerName: 'net_mv', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
  { field: 'gross_exposure', headerName: 'gross', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'net_dv01', headerName: 'dv01', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtSigned(p.value as number) },
  { field: 'sum_var', headerName: 'var', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'worst_util_pct', headerName: 'worst_util', width: 130, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toFixed(1) ?? '—' },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(1)}M`;
}

export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

export function fmtSigned(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n >= 0 ? `+${n.toFixed(0)}` : `−${Math.abs(n).toFixed(0)}`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `ViewChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { VIEW_CHIPS, VIEW_COL_DEFS, fmtMillions, fmtSignedMillions, fmtCount } from '../scopes/view';

const exposureRowId = (r: Row): string =>
  `${String(r.book_name ?? '')}|${String(r.asset_class ?? '')}|${String(r.currency ?? '')}`;

export function ViewChapter() {
  const scope = useChapterScope(VIEW_CHIPS);
  // Unfiltered view sub: source for chip options.
  const allSub = useSubscription('/v_net_exposure', null);
  // Filtered view sub: drives the table.
  const filteredSub = useSubscription('/v_net_exposure', scope.filterExpression, exposureRowId);

  const chipOptions = useMemo(
    () => ({
      BOOK: ['All', ...distinctValues(allSub.rows, 'book_name')],
      ASSET: ['All', ...distinctValues(allSub.rows, 'asset_class')],
      CCY: ['All', ...distinctValues(allSub.rows, 'currency')],
    }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let mv = 0, gross = 0, dv01 = 0, varSum = 0, n = 0;
    for (const r of filteredSub.rows) {
      mv += Number(r.net_mv_usd ?? 0);
      gross += Number(r.gross_exposure ?? 0);
      dv01 += Number(r.net_dv01 ?? 0);
      varSum += Number(r.sum_var ?? 0);
      n += Number(r.n_positions ?? 0);
    }
    return { mv, gross, dv01, varSum, n };
  }, [filteredSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(filteredSub.rows.length), caption: 'in scope', emphasis: true },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across buckets' },
      { label: 'NET MV', value: fmtSignedMillions(totals.mv), caption: 'sum · usd', emphasis: true },
      { label: 'GROSS', value: fmtMillions(totals.gross), caption: 'sum · usd' },
      { label: 'DV01', value: totals.dv01.toFixed(0), caption: 'sum' },
      { label: 'VaR', value: fmtMillions(totals.varSum), caption: 'sum · usd' },
    ],
    [filteredSub.rows.length, totals],
  );

  const status =
    filteredSub.status === 'live'
      ? `${filteredSub.size.toLocaleString()} buckets · live`
      : `${filteredSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 05 — VIEW"
        title="view."
        sub="/v_net_exposure — book × asset × currency net positions, server-aggregated. Cqserver recomputes only the affected bucket on every position mutation."
        hero={<HeroMetric label="NET MV" value={fmtSignedMillions(totals.mv)} detail={`across ${filteredSub.size} buckets`} />}
      />
      <FilterRail
        chips={[...VIEW_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`NET EXPOSURE · ${VIEW_COL_DEFS.length} cols`}
        status={status}
        colDefs={VIEW_COL_DEFS}
        getRowId={exposureRowId}
        liveSubscription={filteredSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Add import:
```tsx
import { ViewChapter } from '../chapters/ViewChapter';
```

Extend ternary with `'view'`:
```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : active === 'lens' ? <LensChapter />
  : active === 'heat' ? <HeatChapter />
  : active === 'view' ? <ViewChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/view.ts \
        clients/examples-web/src/atlas/chapters/ViewChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): ViewChapter — Chapter 05 net exposure materialized view

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Join (Chapter 06 — Joins)

Primary subscription `/v_trades_by_compliance` — joined view (positions × trades grouped by compliance status). One chip on COMPLIANCE.

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/join.ts`
- Create: `clients/examples-web/src/atlas/chapters/JoinChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `join.ts`**

```ts
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const JOIN_CHIPS: readonly ChipSpec[] = [
  { key: 'COMPLIANCE', column: 'compliance_status', source: '/v_trades_by_compliance' },
];

export const JOIN_COL_DEFS: ColDef[] = [
  { field: 'compliance_status', headerName: 'compliance_status', width: 220,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.1em' }
        : { color: '#f4a52b', letterSpacing: '.1em' } },
  { field: 'n_trades', headerName: 'n_trades', width: 130, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'total_fees', headerName: 'total_fees', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'avg_slip_arr', headerName: 'avg_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

export function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `JoinChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { JOIN_CHIPS, JOIN_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/join';

const joinRowId = (r: Row): string => String(r.compliance_status ?? '');

export function JoinChapter() {
  const scope = useChapterScope(JOIN_CHIPS);
  const allSub = useSubscription('/v_trades_by_compliance', null);
  const joinSub = useSubscription('/v_trades_by_compliance', scope.filterExpression, joinRowId);

  const chipOptions = useMemo(
    () => ({ COMPLIANCE: ['All', ...distinctValues(allSub.rows, 'compliance_status')] }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let trades = 0, fees = 0, slipSum = 0, slipN = 0;
    for (const r of joinSub.rows) {
      trades += Number(r.n_trades ?? 0);
      fees += Number(r.total_fees ?? 0);
      const slip = Number(r.avg_slip_arr ?? 0);
      if (Number.isFinite(slip)) { slipSum += slip; slipN += 1; }
    }
    return { trades, fees, avgSlip: slipN > 0 ? slipSum / slipN : 0 };
  }, [joinSub.rows]);

  const breachRow = useMemo(
    () => joinSub.rows.find((r) => r.compliance_status === 'BREACH'),
    [joinSub.rows],
  );

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(joinSub.rows.length), caption: 'compliance states', emphasis: true },
      { label: 'TRADES', value: fmtCount(totals.trades), caption: 'joined' },
      { label: 'FEES', value: fmtMillions(totals.fees), caption: 'sum · usd', emphasis: true },
      { label: 'AVG SLIP', value: fmtBps(totals.avgSlip), caption: 'arrival · mean' },
      { label: 'BREACH TRADES', value: fmtCount(Number(breachRow?.n_trades ?? 0)), caption: 'flagged', emphasis: true },
    ],
    [joinSub.rows.length, totals, breachRow],
  );

  const status =
    joinSub.status === 'live'
      ? `${joinSub.size.toLocaleString()} buckets · live`
      : `${joinSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 06 — JOIN"
        title="join."
        sub="/v_trades_by_compliance — trades joined to positions on position_id, grouped by the position-side compliance status. The view recomputes when either side mutates."
        hero={<HeroMetric label="BREACH" value={fmtCount(Number(breachRow?.n_trades ?? 0))} detail="trades flagged" />}
      />
      <FilterRail
        chips={[...JOIN_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`TRADES BY COMPLIANCE · ${JOIN_COL_DEFS.length} cols`}
        status={status}
        colDefs={JOIN_COL_DEFS}
        getRowId={joinRowId}
        liveSubscription={joinSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Add import:
```tsx
import { JoinChapter } from '../chapters/JoinChapter';
```

Extend ternary with `'join'`:
```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : active === 'lens' ? <LensChapter />
  : active === 'heat' ? <HeatChapter />
  : active === 'view' ? <ViewChapter />
  : active === 'join' ? <JoinChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/join.ts \
        clients/examples-web/src/atlas/chapters/JoinChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): JoinChapter — Chapter 06 trades joined by compliance

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Slip (Chapter 07 — Slippage Aggregation)

Primary subscription `/v_slippage_venue_algo` — slippage stats per (venue × algo). Chips on VENUE + ALGO sourced from the view itself.

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/slip.ts`
- Create: `clients/examples-web/src/atlas/chapters/SlipChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`

- [ ] **Step 1: Write `slip.ts`**

```ts
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const SLIP_CHIPS: readonly ChipSpec[] = [
  { key: 'VENUE', column: 'execution_venue', source: '/v_slippage_venue_algo' },
  { key: 'ALGO', column: 'execution_algo', source: '/v_slippage_venue_algo' },
];

export const SLIP_COL_DEFS: ColDef[] = [
  { field: 'execution_venue', headerName: 'venue', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'execution_algo', headerName: 'algo', width: 140 },
  { field: 'n_trades', headerName: 'n_trades', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'avg_slip_arr', headerName: 'avg_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value > 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
  { field: 'avg_slip_vwap', headerName: 'avg_slip_vwap', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'max_slip_arr', headerName: 'max_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'min_slip_arr', headerName: 'min_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'total_fees', headerName: 'total_fees', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

export function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Write `SlipChapter.tsx`**

```tsx
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { SLIP_CHIPS, SLIP_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/slip';

const slipRowId = (r: Row): string =>
  `${String(r.execution_venue ?? '')}|${String(r.execution_algo ?? '')}`;

export function SlipChapter() {
  const scope = useChapterScope(SLIP_CHIPS);
  const allSub = useSubscription('/v_slippage_venue_algo', null);
  const slipSub = useSubscription('/v_slippage_venue_algo', scope.filterExpression, slipRowId);

  const chipOptions = useMemo(
    () => ({
      VENUE: ['All', ...distinctValues(allSub.rows, 'execution_venue')],
      ALGO: ['All', ...distinctValues(allSub.rows, 'execution_algo')],
    }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let trades = 0, fees = 0, slipSum = 0, slipN = 0, worst = -Infinity;
    for (const r of slipSub.rows) {
      trades += Number(r.n_trades ?? 0);
      fees += Number(r.total_fees ?? 0);
      const slip = Number(r.avg_slip_arr ?? 0);
      if (Number.isFinite(slip)) { slipSum += slip; slipN += 1; if (slip > worst) worst = slip; }
    }
    return {
      trades, fees,
      avgSlip: slipN > 0 ? slipSum / slipN : 0,
      worst: Number.isFinite(worst) ? worst : 0,
    };
  }, [slipSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(slipSub.rows.length), caption: 'venue × algo', emphasis: true },
      { label: 'TRADES', value: fmtCount(totals.trades), caption: 'in scope' },
      { label: 'AVG SLIP', value: fmtBps(totals.avgSlip), caption: 'arrival · mean', emphasis: true },
      { label: 'WORST', value: fmtBps(totals.worst), caption: 'arrival · max', emphasis: true },
      { label: 'FEES', value: fmtMillions(totals.fees), caption: 'sum · usd' },
    ],
    [slipSub.rows.length, totals],
  );

  const status =
    slipSub.status === 'live'
      ? `${slipSub.size.toLocaleString()} buckets · live`
      : `${slipSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 07 — SLIP"
        title="slip."
        sub="/v_slippage_venue_algo — execution-quality stats grouped by venue × algo, server-aggregated. Every row updates whenever its bucket sees a new fill."
        hero={<HeroMetric label="WORST SLIP" value={fmtBps(totals.worst)} detail="arrival · current scope" />}
      />
      <FilterRail
        chips={[...SLIP_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`SLIPPAGE · ${SLIP_COL_DEFS.length} cols`}
        status={status}
        colDefs={SLIP_COL_DEFS}
        getRowId={slipRowId}
        liveSubscription={slipSub}
      />
    </>
  );
}
```

- [ ] **Step 3: Wire into `AtlasPreviewApp.tsx`**

Add import:
```tsx
import { SlipChapter } from '../chapters/SlipChapter';
```

Extend ternary with `'slip'`:
```tsx
{active === 'pulse' ? <PulseChapter />
  : active === 'tape' ? <TapeChapter />
  : active === 'lens' ? <LensChapter />
  : active === 'heat' ? <HeatChapter />
  : active === 'view' ? <ViewChapter />
  : active === 'join' ? <JoinChapter />
  : active === 'slip' ? <SlipChapter />
  : <ComingSoon id={active} />}
```

- [ ] **Step 4: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/slip.ts \
        clients/examples-web/src/atlas/chapters/SlipChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): SlipChapter — Chapter 07 slippage by venue × algo

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Manual smoke verification

**Files:** none.

- [ ] **Step 1: Reload the demo if needed**

```bash
cd /Users/develop/cqserver
./stop-demo.sh 2>/dev/null
POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh
```

- [ ] **Step 2: Walk every station 01-07 at `#atlas`**

Browse to `http://localhost:5175/#atlas`. Click each of the seven migrated stations (01-07). For each:

- ChapterHead text reads correctly with chapter-specific kicker + title + sub.
- Hero metric shows a real live value.
- KPI strip has six (or five) populated values.
- Filter chips show real distinct values from the bound view (for chapters with chips).
- POSITIONS / TRADES / VIEW / etc. table populates with live rows in the right column subset.
- Cells flash on value change.

**Chapter 08 (QUERY)** still shows `QUERY · arriving in a later phase` — Plan C migrates it.

- [ ] **Step 3: Verify chip-driven filtering**

In Tape (02): toggle SIDE → BUY. The trade table should re-snapshot showing only buy-side trades. The KPI strip's `N TRADES` and `NOTIONAL` should drop to roughly half.

In View (05): toggle BOOK → `Rates Curve`. The exposure table should drop to ~4 buckets (rates curve × 4 asset/ccy combos).

In Slip (07): toggle VENUE to one of the listed venues. The table drops to the algos used at that venue.

- [ ] **Step 4: Verify legacy app still works**

`http://localhost:5175/` (no hash) — the 8-tab dock still renders every chapter with real data. Plan B doesn't touch the legacy app.

---

## Self-Review (completed by author)

**Spec coverage:**
- Every chapter follows the Plan A pattern: scope file → chapter component → wired into AtlasPreviewApp. ✅
- Each chapter consumes the SharedWorker via `useSubscription` (with `getRowId` for stable applyTransactionAsync matching). ✅
- KPI strips read from either dedicated aggregate views (Pulse, Tape via SQL aggregate) or roll up small view-row sets in useMemo (Lens, Heat, View, Join, Slip). The latter is server-aggregated data summarised locally — no raw-topic aggregation crosses the wire. ✅
- The other chapter (08 Query) remains on ComingSoon — Plan C scope. ✅

**Placeholder scan:** no "TBD" / "fill in later" / placeholders. Every code block is complete. The few `(p.value as number)?.toLocaleString(...)` casts are real number formatters not placeholders. ✅

**Type consistency:**
- All chapters use the same `Row` type from `@/lib/use-subscription`. ✅
- All chapters use the same `Kpi` type from `KpiStrip.tsx`. ✅
- All chapters' rowId derivations are stable module-level functions matching their primary subscription's natural key. ✅
- Each chapter's `useChapterScope([...])` spread is consistent (FilterRail expects mutable array). ✅
- The chained ternary in AtlasPreviewApp.tsx extends one case per task; final form lives in Task 6's step 3. ✅

**Scope:** Plan B builds 6 chapters and one final integration. Each task is independent; commit boundaries keep `#atlas` working at every step. Plan C handles Query Builder + the swap of `/`. ✅

# examples-web Phase 3 — Pulse Chapter End-to-End on Real Data

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Atlas Pulse chapter at `#atlas` consume live cqserver data — chips populated from `/v_pnl_by_book`/`/v_pnl_by_sector`/`/v_compliance_counts`, KPIs from `/v_book_totals`, positions grid filtered server-side via the chip selections — and retire Phase 1's placeholder layer. This task also lands the reusable chapter primitives (`useChapterScope`, `<DataTable>`-with-`liveSubscription` mode) that Plan B uses to replicate the pattern across the other six chapters.

**Architecture:** A new `useChapterScope(chips)` hook owns the chip-state + filter-expression machinery so every chapter component looks identical at the call site (just a `useChapterScope(pulseScope.chips)` + four `useSubscription` calls). `<DataTable>` grows a `liveSubscription` mode that consumes the worker-backed `SubscriptionHandle` via `subscribeSnapshotChunks` (one `applyTransactionAsync({add: chunk})` per ~500 rows) and `subscribeDeltas` (one batched transaction per coalesce window). Pulse's KPI strip reads the single aggregate row of `/v_book_totals` and maps the seven SUM columns into the six display KPIs declared in `pulseScope.kpis`. The Phase 1 placeholder data and `<PulsePreview>` are deleted.

**Tech Stack:** React 19 + TypeScript + Vite (examples-web), AG-Grid v35 Theming API, `useSubscription` over the SharedWorker port (Phase 2).

---

## Pre-flight

Implementer should `cd /Users/develop/cqserver` and verify the branch tip:

```bash
git log --oneline -5
# Expected: 5fb654fe fix(atlas): anchor AppShell height ...
```

`vite dev` is running from Phase 2's smoke check; if not, restart with `RESEED=1 POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh` from the repo root. The cqserver process must be up and the publisher must have seeded `/positions`, `/trades`, etc. — the new chapter has no placeholder fallback.

**Pre-existing WIP everywhere.** Use precise `git add <paths>` only — never `-A`, never `.`.

**Atomicity:** every commit must keep the demo working. At every commit boundary `http://localhost:5175/` (legacy app) and `http://localhost:5175/#atlas` (Atlas preview) must render. The dangerous commit is Task 6 (the PulseChapter swap + Phase 1 cleanup), gated by Tasks 1-5 being in.

---

## Cqserver schema (locked, referenced in tasks below)

`/v_book_totals` (single-row, source `/positions`) — columns:
- `exposure_gross`, `market_value`, `unrealized_pnl`, `realized_pnl`, `day_pnl`, `ytd_pnl`, `var_95`, `n_positions`

`/v_pnl_by_book` (one row per book) — `book_name`, `unrealized_pnl`, `day_pnl`.

`/v_pnl_by_sector` (one row per sector) — `issuer_sector`, `day_pnl`.

`/v_compliance_counts` (one row per compliance bucket) — `compliance_status`, `n_positions`.

`/positions` (~40k rows, 206 columns) — `position_id` (PK), `book_name`, `issuer_sector`, `compliance_status`, `issuer_name`, `symbol`, `asset_class`, `market_value_usd`, `day_pnl`, `unrealized_pnl_usd`, `var_1d_95`, etc.

---

## File map

| Path | Status | Responsibility |
|---|---|---|
| `clients/examples-web/src/atlas/hooks/useChapterScope.ts` | new | Chip state + composed SQL `WHERE` expression + human summary. Reusable across every chapter. |
| `clients/examples-web/src/atlas/components/DataTable.tsx` | modified | Add `liveSubscription` mode: chunked SOW + delta batches via `applyTransactionAsync`. |
| `clients/examples-web/src/atlas/scopes/pulse.ts` | new | `pulseScope` declaration: chips (BOOK/SECTOR/COMPLIANCE), KPI mapping, positions column list. |
| `clients/examples-web/src/atlas/chapters/PulseChapter.tsx` | new | Real Pulse chapter — composes ChapterHead/FilterRail/KpiStrip/DataTable against worker subs. |
| `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx` | modified | Import `PulseChapter` instead of `PulsePreview`. |
| `clients/examples-web/src/atlas/preview/PulsePreview.tsx` | **deleted** | Replaced by `PulseChapter`. |
| `clients/examples-web/src/atlas/preview/placeholderData.ts` | **deleted** | Real data layer; no placeholders. |

---

## Task 1: `useChapterScope` hook

**Files:**
- Create: `clients/examples-web/src/atlas/hooks/useChapterScope.ts`

Holds chip state (initialised from `chip.default`), exposes a `setChip(key, value)` callback for `FilterRail`'s `onChange`, derives the SQL `WHERE` expression that `useSubscription` consumes, and a one-line human summary for the FilterRail's `subscriptionSummary` slot.

- [ ] **Step 1: Write the file**

```ts
/**
 * Chapter scope — single source of truth for a chapter's chip
 * selections, the resulting SQL WHERE expression, and a human-readable
 * summary. Every Atlas chapter consumes this hook so the filter
 * rail / subscription wiring looks identical across chapters.
 *
 * Initial state comes from each chip's `default` field (Phase 1's
 * `ChipSpec`). `setChip(key, value)` is the per-chip setter; `setState`
 * accepts FilterRail's full-record `onChange` callback.
 */
import { useCallback, useMemo, useState } from 'react';
import type { ChipSpec } from '../types';

export interface ChapterScopeHandle {
  /** chip.key → current selection. undefined or 'All' = no constraint. */
  state: Record<string, string | undefined>;
  /** Composed SQL WHERE expression, or null if every chip is unconstrained. */
  filterExpression: string | null;
  /** Human-readable summary: `book_name = 'RATES-US' · issuer_sector = 'Tech'`. */
  summary: string;
  /** Update one chip's value. Pass undefined to clear it. */
  setChip: (key: string, value: string | undefined) => void;
  /** Replace the whole state record — wired to FilterRail's onChange. */
  setState: (next: Record<string, string | undefined>) => void;
}

export function useChapterScope(
  chips: readonly ChipSpec[],
): ChapterScopeHandle {
  const initial = useMemo<Record<string, string | undefined>>(() => {
    const out: Record<string, string | undefined> = {};
    for (const c of chips) {
      if (c.default) out[c.key] = c.default;
    }
    return out;
  }, [chips]);

  const [state, setRawState] = useState<Record<string, string | undefined>>(initial);

  const setChip = useCallback((key: string, value: string | undefined) => {
    setRawState((prev) => ({ ...prev, [key]: value }));
  }, []);

  const { filterExpression, summary } = useMemo(() => {
    const parts: string[] = [];
    for (const c of chips) {
      const v = state[c.key];
      if (v == null || v === '' || v === 'All') continue;
      // Single-quote escape: SQL single-quote → '' (matches cqserver's parser).
      const escaped = v.replace(/'/g, "''");
      parts.push(`${c.column} = '${escaped}'`);
    }
    return {
      filterExpression: parts.length === 0 ? null : parts.join(' AND '),
      summary: parts.length === 0 ? '(unfiltered)' : parts.join(' · '),
    };
  }, [chips, state]);

  return { state, filterExpression, summary, setChip, setState: setRawState };
}

/**
 * Extract sorted distinct string values from a column across a snapshot
 * of view rows. Used by chapter components to derive chip option lists
 * from their bound view subscriptions.
 */
export function distinctValues(
  rows: readonly Record<string, unknown>[],
  column: string,
): string[] {
  const set = new Set<string>();
  for (const r of rows) {
    const v = r[column];
    if (v != null && v !== '') set.add(String(v));
  }
  return Array.from(set).sort();
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/hooks/useChapterScope.ts
git commit -m "feat(atlas): useChapterScope — chip state + filter expression

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Extend `<DataTable>` with `liveSubscription` mode

**Files:**
- Modify: `clients/examples-web/src/atlas/components/DataTable.tsx`

When the new `liveSubscription` prop is set the grid binds imperatively: `subscribeSnapshotChunks` feeds `applyTransactionAsync({add: chunk})` per chunk (so a 40k-row SOW paints progressively at 80 chunks × 500 rows), and `subscribeDeltas` feeds `applyTransactionAsync({add, update, remove})` per coalesce window. When the prop is absent the table renders the static `rows` prop just like Phase 1.

- [ ] **Step 1: Replace the file**

```tsx
import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';
import type { SubscriptionHandle } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface DataTableProps<T extends Record<string, unknown>> {
  /** Title strip above the grid, e.g. 'POSITIONS · 23 of 207 cols'. */
  title?: string;
  /** Right-aligned status, e.g. '4,827 rows · ticking'. */
  status?: string;
  /** Static rows. Ignored when `liveSubscription` is set. */
  rows?: T[];
  colDefs: ColDef[];
  /** Stable row id extractor — required when `liveSubscription` is set so
   *  `applyTransactionAsync({update})` can match incoming rows. */
  getRowId?: (row: T) => string;
  /**
   * Per-component cqserver subscription handle (from `useSubscription` /
   * `useFilteredSubscription`). When set, the grid:
   *   - seeds itself by consuming `subscribeSnapshotChunks` and calling
   *     `applyTransactionAsync({add: chunk})` per chunk;
   *   - applies live deltas via `subscribeDeltas` once SOW completes.
   * `rows` is ignored.
   */
  liveSubscription?: SubscriptionHandle;
}

export function DataTable<T extends Record<string, unknown>>({
  title,
  status,
  rows,
  colDefs,
  getRowId,
  liveSubscription,
}: DataTableProps<T>) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

  const apiRef = useRef<GridApi<T> | null>(null);
  const seededRef = useRef<SubscriptionHandle | null>(null);
  const [seeded, setSeeded] = useState(false);

  // Imperative wiring: only runs when liveSubscription is set.
  useEffect(() => {
    if (!liveSubscription) return;
    // Subscription identity changed (e.g. filter swap rebuilt the sub).
    if (seededRef.current !== liveSubscription) {
      seededRef.current = liveSubscription;
      setSeeded(false);
      const api = apiRef.current;
      if (api) api.setGridOption('rowData', []);
    }
    const unsubChunks = liveSubscription.subscribeSnapshotChunks((chunk, more) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({ add: chunk as unknown as T[] });
      if (!more) setSeeded(true);
    });
    const unsubDeltas = liveSubscription.subscribeDeltas((batch) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({
        add: batch.add as unknown as T[],
        update: batch.update as unknown as T[],
        remove: batch.remove as unknown as T[],
      });
    });
    // Replay any chunks that landed before we attached: if the worker
    // already finished SOW for this handle, the snapshot accessor still
    // has them all — apply them in one shot.
    if (liveSubscription.getStatus() === 'live' && !seeded) {
      const snap = liveSubscription.getSnapshot();
      if (snap.length > 0) {
        const api = apiRef.current;
        if (api) {
          api.applyTransactionAsync({ add: snap as unknown as T[] });
          setSeeded(true);
        }
      }
    }
    return () => {
      unsubChunks();
      unsubDeltas();
    };
  }, [liveSubscription, seeded]);

  const handleGridReady = (e: GridReadyEvent<T>) => {
    apiRef.current = e.api;
  };

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '18px 24px 0',
      }}
    >
      {(title || status) && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingBottom: 12,
          }}
        >
          {title ? (
            <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{title}</div>
          ) : (
            <div />
          )}
          {status ? <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div> : null}
        </div>
      )}
      <div style={{ flex: 1, minHeight: 280, width: '100%', height: '100%' }}>
        <AgGridReact<T>
          theme={theme}
          rowData={liveSubscription ? undefined : (rows ?? [])}
          columnDefs={colDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
          onGridReady={handleGridReady}
        />
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/DataTable.tsx
git commit -m "feat(atlas): DataTable gains liveSubscription mode for chunked SOW

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Pulse chapter scope declaration

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/pulse.ts`

Static declaration the Pulse chapter component consumes. Lists the chips (each with `column` for the filter expression and `source` for the option list), the KPI mapping from `/v_book_totals` columns to display values, and the column subset shown in the positions grid.

- [ ] **Step 1: Write the file**

```ts
/**
 * Pulse (Chapter 01 — Live Book) scope.
 *
 * Every datum the chapter component needs to render against real
 * cqserver data: chip definitions for the filter rail, KPI mapping
 * for the strip, column subset for the positions table.
 */
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

/** Chips render the FilterRail and drive the WHERE expression sent to
 *  /positions. Each chip's options come from the `source` view. */
export const PULSE_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', source: '/v_pnl_by_book', default: 'RATES-US' },
  { key: 'SECTOR', column: 'issuer_sector', source: '/v_pnl_by_sector' },
  { key: 'COMPLIANCE', column: 'compliance_status', source: '/v_compliance_counts' },
];

/**
 * Map of column names on `/v_book_totals` → display label / formatter
 * for the KPI strip. The chapter component reads the single aggregate
 * row of /v_book_totals and produces a `Kpi[]` from this mapping.
 *
 * `/v_compliance_counts` provides the BREACH count separately because it
 * lives on a different view (one row per status bucket).
 */
export interface PulseKpiDef {
  label: string;
  caption?: string;
  /** Field on /v_book_totals to read, or '__breaches__' for the synthetic
   *  breach count derived from /v_compliance_counts. */
  source: string;
  /** Display formatter. */
  format: 'currency-m' | 'currency-m-signed' | 'count';
  /** Apply amber colour to the value. */
  emphasis?: boolean;
}

export const PULSE_KPIS: readonly PulseKpiDef[] = [
  { label: 'NET MV', source: 'market_value', format: 'currency-m', caption: 'market_value · sum', emphasis: true },
  { label: 'EXPOSURE', source: 'exposure_gross', format: 'currency-m', caption: 'gross · sum' },
  { label: 'DAY PnL', source: 'day_pnl', format: 'currency-m-signed', caption: 'today', emphasis: true },
  { label: 'YTD PnL', source: 'ytd_pnl', format: 'currency-m-signed', caption: 'inception', emphasis: true },
  { label: 'VaR (1d)', source: 'var_95', format: 'currency-m', caption: '95% confidence' },
  { label: 'BREACHES', source: '__breaches__', format: 'count', caption: 'compliance' },
];

/** Column subset shown in the Pulse positions table. The full /positions
 *  topic has 206 columns; we show the eight that matter for a live read. */
export const PULSE_COL_DEFS: ColDef[] = [
  { field: 'position_id', headerName: 'position_id', width: 110, cellStyle: { color: '#f4a52b' } },
  { field: 'book_name', headerName: 'book', width: 110 },
  { field: 'symbol', headerName: 'symbol', width: 80 },
  { field: 'issuer_sector', headerName: 'sector', width: 120 },
  { field: 'asset_class', headerName: 'asset_class', width: 110 },
  {
    field: 'market_value_usd',
    headerName: 'market_value',
    width: 140,
    type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number),
    cellClass: 'ag-right-aligned-cell',
  },
  {
    field: 'day_pnl',
    headerName: 'day_pnl',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0
        ? { color: '#ff6062' }
        : typeof p.value === 'number'
          ? { color: '#f4a52b' }
          : null,
  },
  {
    field: 'compliance_status',
    headerName: 'status',
    width: 100,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.1em' }
        : { color: '#f4a52b', letterSpacing: '.1em' },
  },
];

/** Format a raw USD amount as `+$1.21M` / `-$0.04M`. */
export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

/** Format a raw USD amount as `$82.1M`. */
export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(1)}M`;
}

/** Format an integer count as `4,827`. */
export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/pulse.ts
git commit -m "feat(atlas): pulse scope — chips, KPI mapping, column defs

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `PulseChapter` component

**Files:**
- Create: `clients/examples-web/src/atlas/chapters/PulseChapter.tsx`

Composes the chapter against real subscriptions. Opens four worker subs — `/v_book_totals`, `/v_pnl_by_book`, `/v_pnl_by_sector`, `/v_compliance_counts` — for the KPI row + chip option lists, and one filtered sub on `/positions` for the data table. The filter expression comes from `useChapterScope`.

- [ ] **Step 1: Write the file**

```tsx
/**
 * Pulse — Chapter 01, Live Book. The first Atlas chapter on real
 * cqserver data. Pattern:
 *   - 4 view subscriptions seed KPIs + chip option lists
 *   - 1 filtered subscription on /positions drives the data table
 *   - `useChapterScope` owns the chip state and composes the WHERE
 *     expression every chip toggle re-emits
 */
import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import {
  PULSE_CHIPS,
  PULSE_KPIS,
  PULSE_COL_DEFS,
  fmtSignedMillions,
  fmtMillions,
  fmtCount,
} from '../scopes/pulse';

const positionRowId = (r: Row): string => String(r.position_id ?? '');

export function PulseChapter() {
  const scope = useChapterScope(PULSE_CHIPS);

  // View subscriptions — small row counts, used to derive chip options
  // and the aggregate KPI row.
  const bookSub = useSubscription('/v_pnl_by_book', null);
  const sectorSub = useSubscription('/v_pnl_by_sector', null);
  const complianceSub = useSubscription('/v_compliance_counts', null);
  const totalsSub = useSubscription('/v_book_totals', null);

  // Primary subscription — /positions filtered server-side by the chip selection.
  const positionsSub = useSubscription('/positions', scope.filterExpression, positionRowId);

  // Derive chip option lists from the view snapshots.
  const chipOptions = useMemo(
    () => ({
      BOOK: ['All', ...distinctValues(bookSub.rows, 'book_name')],
      SECTOR: ['All', ...distinctValues(sectorSub.rows, 'issuer_sector')],
      COMPLIANCE: ['All', ...distinctValues(complianceSub.rows, 'compliance_status')],
    }),
    [bookSub.rows, sectorSub.rows, complianceSub.rows],
  );

  // Derive KPI values from the aggregate row + breach count.
  const kpis = useMemo<Kpi[]>(() => {
    const t = (totalsSub.rows[0] ?? {}) as Record<string, unknown>;
    const breachRow = complianceSub.rows.find((r) => r.compliance_status === 'BREACH');
    const breaches = breachRow ? Number(breachRow.n_positions) : 0;
    return PULSE_KPIS.map((def): Kpi => {
      const raw =
        def.source === '__breaches__' ? breaches : Number(t[def.source] ?? 0);
      const value =
        def.format === 'currency-m'
          ? fmtMillions(raw)
          : def.format === 'currency-m-signed'
            ? fmtSignedMillions(raw)
            : fmtCount(raw);
      return {
        label: def.label,
        value,
        caption: def.caption,
        emphasis: def.emphasis,
      };
    });
  }, [totalsSub.rows, complianceSub.rows]);

  // Hero metric — unrealized PnL with the live tick count from the
  // positions sub (poor man's stand-in until Phase 6's chapter scope).
  const heroValue = useMemo(() => {
    const t = (totalsSub.rows[0] ?? {}) as Record<string, unknown>;
    return fmtSignedMillions(Number(t.unrealized_pnl ?? 0));
  }, [totalsSub.rows]);

  const status =
    positionsSub.status === 'live'
      ? `${positionsSub.size.toLocaleString()} rows · live`
      : `${positionsSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 01 — LIVE BOOK"
        title="pulse."
        sub="A continuous read of the firm's book — KPIs, sector ladder, book contribution, breaches. Every figure server-computed by a materialized view; nothing aggregated in the browser."
        hero={<HeroMetric label="UNREALISED PnL" value={heroValue} detail="from /v_book_totals" />}
      />
      <FilterRail
        chips={[...PULSE_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`POSITIONS · 8 of 206 cols`}
        status={status}
        colDefs={PULSE_COL_DEFS}
        getRowId={positionRowId}
        liveSubscription={positionsSub}
      />
    </>
  );
}
```

- [ ] **Step 2: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/chapters/PulseChapter.tsx
git commit -m "feat(atlas): PulseChapter — Chapter 01 on real cqserver data

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Swap `AtlasPreviewApp` from `PulsePreview` to `PulseChapter` + delete placeholder files

**Files:**
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`
- Delete: `clients/examples-web/src/atlas/preview/PulsePreview.tsx`
- Delete: `clients/examples-web/src/atlas/preview/placeholderData.ts`

- [ ] **Step 1: Patch `AtlasPreviewApp.tsx`**

Open `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`. Replace the existing import line:

```tsx
import { PulsePreview } from './PulsePreview';
```

with:

```tsx
import { PulseChapter } from '../chapters/PulseChapter';
```

Then in the `<main>` body, swap the conditional:

```tsx
{active === 'pulse' ? <PulsePreview /> : <ComingSoon id={active} />}
```

becomes:

```tsx
{active === 'pulse' ? <PulseChapter /> : <ComingSoon id={active} />}
```

Also update the `AppShell` hint from `"phase 1 preview · placeholder data"` to `"phase 3 · pulse live"`:

```tsx
<AppShell hint="phase 3 · pulse live">
```

And drop the `cadence="250ms cadence" tickStats="placeholder"` props from `<Footer>` (the legacy placeholder hints are now misleading; leave Footer's status default `'LIVE'`):

```tsx
<Footer />
```

The file's final shape after edits:

```tsx
import { useState } from 'react';
import { AppShell } from '../components/AppShell';
import { StationsNav } from '../components/StationsNav';
import { Footer } from '../components/Footer';
import { PulseChapter } from '../chapters/PulseChapter';
import type { ChapterId } from '../types';

/** Stub for any chapter that hasn't been migrated yet (Phase 3 = Pulse only). */
function ComingSoon({ id }: { id: ChapterId }) {
  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: 'var(--atlas-fg-faint)',
        fontSize: 11,
        letterSpacing: '.3em',
      }}
    >
      {id.toUpperCase()} · arriving in a later phase
    </div>
  );
}

export function AtlasPreviewApp() {
  const [active, setActive] = useState<ChapterId>('pulse');

  return (
    <AppShell hint="phase 3 · pulse live">
      <StationsNav active={active} onChange={setActive} />
      <main style={{ position: 'relative', zIndex: 1, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {active === 'pulse' ? <PulseChapter /> : <ComingSoon id={active} />}
      </main>
      <Footer />
    </AppShell>
  );
}
```

- [ ] **Step 2: Delete the placeholder files**

```bash
git rm clients/examples-web/src/atlas/preview/PulsePreview.tsx
git rm clients/examples-web/src/atlas/preview/placeholderData.ts
```

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -4`
Expected: clean. If a stray import of `PulsePreview` or `placeholderData` surfaces (it shouldn't — both files were Phase 1 only), surface as BLOCKED.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
# git rm already staged the deletions
git commit -m "feat(atlas): swap PulsePreview → PulseChapter; drop placeholder data

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Manual smoke verification

**Files:** none.

- [ ] **Step 1: Make sure the demo is up**

```bash
cd /Users/develop/cqserver
./stop-demo.sh 2>/dev/null
POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh
# RESEED=1 only if the txlog has bloated again (see Phase 2 task 16 notes).
```

- [ ] **Step 2: Open the Pulse chapter**

Browse to `http://localhost:5175/#atlas`. Expected:

- Top bar shows `cq · atlas` left, `cqserver · ws://127.0.0.1:9008 · phase 3 · pulse live` right.
- Stations rail with PULSE active in amber.
- Chapter head: `CHAPTER 01 — LIVE BOOK`, amber `pulse.` headline, descriptive sub.
- Hero metric reads the live `unrealized_pnl` value from `/v_book_totals` (a real `+$X.YYM` number, not the hard-coded `+$3.21M` from Phase 1).
- Filter rail: BOOK chip active with `RATES-US` (default), SECTOR + COMPLIANCE show `All`. Clicking BOOK opens a picker populated from `/v_pnl_by_book`'s book_name column — should show RATES-US, CREDIT-IG, EQTY-VOL, FX-MACRO (or whatever the seeded universe contains).
- KPI strip shows live values from `/v_book_totals` (NET MV, EXPOSURE, DAY PnL, YTD PnL, VaR, BREACHES).
- POSITIONS table populates progressively — chunks of ~500 rows paint in as the SOW streams. Row count visible in the status line ('N,NNN rows · live'). Click a different BOOK chip: the table clears, status flips to `snapshotting…`, then re-populates filtered.
- Live ticks visible: cells flash amber on value change; KPI numbers nudge over time.

- [ ] **Step 3: Verify chip-driven server-side filtering**

In DevTools → Network tab, filter for the WebSocket connection (single ws://127.0.0.1:9008/cq/json frame). Open Frames view. Toggle the BOOK chip from RATES-US → CREDIT-IG. Expected:

- A new `sow_and_subscribe` message goes out with `"f": "book_name = 'CREDIT-IG'"`.
- The old subscription is unsubscribed.
- Only matching rows arrive on the wire — confirms the chip is server-evaluated, not client-side filtered.

- [ ] **Step 4: Verify the other 7 stations still stub**

Click each station 02-08 in the rail. Each should show `<NAME> · arriving in a later phase` centered in faint grey. This confirms Plan A doesn't accidentally break the StationsNav routing.

- [ ] **Step 5: Verify the legacy app is untouched**

Browse to `http://localhost:5175/` (no hash). The legacy 8-tab dock-based demo should render exactly as it did at the end of Phase 2 — every tab populates, every grid ticks. This is the regression-free guarantee for Plans B+C.

If anything fails, fix the relevant earlier task before declaring Plan A done.

---

## Self-Review (completed by author)

**Spec coverage** (against the master spec's Phase 3 row + the ChapterScope section):

- **Per-chapter ChapterScope declaration** — `src/atlas/scopes/pulse.ts` defines `PULSE_CHIPS`, `PULSE_KPIS`, `PULSE_COL_DEFS`. Task 3. ✅
- **Server-side filtering via FilterRail chips** — `useChapterScope` builds a SQL WHERE expression from active chips; passed to `useSubscription` on `/positions`. Tasks 1, 4. ✅
- **KPIs from materialized views, not client-side aggregation** — `/v_book_totals` aggregate row + `/v_compliance_counts` breach row, mapped via `PULSE_KPIS`. Nothing reduce()'d in React. ✅
- **Chunked progressive SOW via `applyTransactionAsync`** — `<DataTable liveSubscription={...}>` consumes `subscribeSnapshotChunks` and applies each chunk as a transaction. Task 2. ✅
- **No row mirror on main** — DataTable is fully imperative when a liveSubscription is set; no React rowData prop is involved. ✅
- **Chapter pattern reusable** — `useChapterScope`, `distinctValues`, `<DataTable liveSubscription>` are the three new primitives. Plan B (six chapters) consumes them identically. ✅
- **Placeholder layer deleted** — `PulsePreview.tsx` and `placeholderData.ts` are gone. Task 5. ✅

**Placeholder scan:** no "TBD", "TODO", "fill in later". Every code block is concrete. The `'__breaches__'` source marker in `PULSE_KPIS` is a documented sentinel, not a TBD. ✅

**Type / name consistency:**
- `ChapterScopeHandle` defined in Task 1, consumed in Task 4. Field names (`state`, `filterExpression`, `summary`, `setChip`, `setState`) match throughout. ✅
- `SubscriptionHandle` (from Phase 2) is the only handle type used by `<DataTable liveSubscription>`. ✅
- `Kpi` (from Phase 1's `KpiStrip.tsx`) is the consumer shape `PulseChapter` produces — verified the field names (`label`, `value`, `caption`, `emphasis`). ✅
- The `Row` type from `@/lib/use-subscription` is used consistently — `positionRowId(r: Row)`, `useSubscription` returns rows typed `Row`, `DataTable<Row>` constraint matches. ✅
- Column names referenced in `PULSE_COL_DEFS` and KPI mapping match cqserver's actual schema (verified against `cqserver.toml` and `ex01-live-pnl/index.tsx`). ✅

**Scope:** strictly Pulse + the primitives. The other 7 stations stay on `ComingSoon` — Plan B picks them up. The legacy `/` route is untouched — Plan C handles the swap. Every commit boundary keeps both URLs working. ✅

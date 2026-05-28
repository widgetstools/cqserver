# Phase 5 + 6 — Query Chapter + Atlas-as-default Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the Atlas redesign: add the eighth chapter (QUERY — ad-hoc SQL), swap `/` from the legacy 8-tab dock UI to the Atlas app, and delete every legacy file that is now orphaned.

**Architecture:** Build QueryChapter as the existing Atlas pattern (ChapterHead + FilterRail-shaped library rail + KpiStrip + body) with three child components — `QueryLibrary` (catalog rail), `SqlEditor` (textarea + Run), `QueryResult` (cols-inferred grid that handles both live `useLiveQuery` mode and static `runOneShotSql` mode). Rename `AtlasPreviewApp` → `AtlasApp`, render it at `/` directly, drop the legacy `LegacyApp` fork in `App.tsx`, and remove every file that the legacy fork was the only consumer of.

**Tech Stack:** React 19, TypeScript, AG-Grid v33 community + enterprise, Vite 7, existing SharedWorker data layer.

---

## File structure

**Create:**
- `clients/examples-web/src/atlas/chapters/QueryChapter.tsx` — chapter shell + run-state machine
- `clients/examples-web/src/atlas/components/QueryLibrary.tsx` — left rail: search + grouped-by-feature catalog + click selection
- `clients/examples-web/src/atlas/components/SqlEditor.tsx` — top right: SQL textarea + Run button + status / error strip
- `clients/examples-web/src/atlas/components/QueryResult.tsx` — bottom right: cols-inferred result grid with live + static modes
- `clients/examples-web/src/atlas/scopes/query.ts` — catalog re-export, KPI defs, run-mode helpers
- `clients/examples-web/src/atlas/app/AtlasApp.tsx` — renamed from `preview/AtlasPreviewApp.tsx`

**Modify:**
- `clients/examples-web/src/App.tsx` — drop `LegacyApp` fork; render `AtlasApp` directly
- `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx` — moved (then deleted)
- `clients/examples-web/index.html` — title update; drop legacy stylesheets if any imported there
- `clients/examples-web/src/main.tsx` — ensure no legacy css/theme imports remain

**Delete (legacy):**
- `clients/examples-web/src/examples/` — all 8 example folders + `ExampleCanvas.tsx` + `registry.tsx` + `shared.ts`
- `clients/examples-web/src/components/atlas/AtlasHeader.tsx`, `ContextBar.tsx`, `DockSurface.tsx`
- `clients/examples-web/src/components/panels/` — `PanelChrome.tsx`, `KpiPanel.tsx`, `MarkdownPanel.tsx`, `GridPanel.tsx`, `SqlPanel.tsx`
- `clients/examples-web/src/components/theme/` — `ThemeProvider.tsx` (Atlas uses tokens.css, not radix theme)
- `clients/examples-web/src/components/ui/` — shadcn primitives only used by legacy
- `clients/examples-web/src/lib/features.ts` — feature dots metadata used by legacy tabstrip
- `clients/examples-web/src/lib/use-filtered-subscription.ts` — alias; drop if no atlas/ file imports it
- `clients/examples-web/src/lib/use-live-query.ts` — only QueryChapter uses it now; KEEP if QueryChapter does, otherwise drop
- Legacy stylesheets — `atlas-tabstrip*`, `atlas-tab*`, `fade-up` keyframes specific to legacy, if not used by Atlas

The exact delete list depends on `grep` results in Task 7. Each delete is verified by ripgrep showing zero in-Atlas references before the file is removed.

---

## Task 1: Query scope — catalog + KPI defs + run-mode types

**Files:**
- Create: `clients/examples-web/src/atlas/scopes/query.ts`

The query scope needs to expose the existing query library, declare the run-mode discriminated union the chapter will use, and provide formatters consistent with other scopes. The catalog itself stays in `src/lib/queries/library.ts` (still consumed; do not duplicate) — the scope re-exports it for the Atlas import path.

- [ ] **Step 1: Write the scope file**

```ts
// clients/examples-web/src/atlas/scopes/query.ts

/**
 * Query (Chapter 08 — Ad-Hoc SQL) scope.
 *
 * The chapter renders a pre-built catalog on the left, an editable SQL
 * editor on the top right, and a result grid below. cqserver's
 * sub-time JOIN evaluator is only on the one-shot SOW path, so the
 * runner forks by mode:
 *   - 'live'   — single-topic SELECT / WHERE / GROUP BY. Live
 *                sowAndSubscribe via useLiveQuery; the grid ticks.
 *   - 'static' — multi-topic JOIN queries. One-shot runOneShotSql
 *                against the left topic; grid renders frozen.
 *
 * Mode is auto-detected per query by inspecting the SQL for JOIN
 * keywords (matches the legacy ex08 heuristic verbatim).
 */
import type { QueryFeature } from '@/lib/queries/library';

export { QUERIES, type QueryEntry, type QueryFeature } from '@/lib/queries/library';

export const FEATURE_LABEL: Record<QueryFeature, string> = {
  join: 'Joins',
  filter: 'Filters',
  agg: 'Aggregations',
  pivot: 'Pivots',
  view: 'Views',
  window: 'Window Functions',
};

export const FEATURE_ORDER: QueryFeature[] = [
  'join', 'filter', 'agg', 'pivot', 'view', 'window',
];

/** Heuristic — JOIN keyword anywhere outside a string literal forces
 *  static mode. Lifted verbatim from legacy ex08-query-builder; the
 *  pattern is `\bJOIN\b` case-insensitive and applies after a crude
 *  comment-strip. */
export function detectRunMode(sql: string): 'live' | 'static' {
  const stripped = sql
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
  return /\bJOIN\b/i.test(stripped) ? 'static' : 'live';
}

/** Pick the first topic referenced after `FROM`. The query runner uses
 *  this as the subscription topic for live mode and the SOW target for
 *  static mode. */
export function detectFromTopic(sql: string): string {
  const m = sql.match(/\bFROM\s+\/?([a-zA-Z_][\w.]*)/i);
  return m ? `/${m[1].replace(/^\//, '')}` : '/positions';
}

export function fmtMs(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  if (n < 1) return `<1 ms`;
  if (n < 1000) return `${Math.round(n)} ms`;
  return `${(n / 1000).toFixed(2)} s`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

/**
 * Strip `<alias>.` prefixes from column references so cqserver's
 * parser (which doesn't carry an alias-resolution table) sees the
 * bare column names from the combined JOIN schema. Ported verbatim
 * from legacy ex08; half the catalog queries use `p`/`t` aliases and
 * would otherwise trip "Unknown column" on the live and static paths.
 */
export function stripAliases(sql: string): string {
  const aliasRe = /(?:from|join)\s+(\w+)\s+(\w+)\b/gi;
  const aliases: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = aliasRe.exec(sql)) !== null) {
    const alias = m[2]!;
    const lower = alias.toLowerCase();
    // SQL clause-starting keywords that occasionally land in m[2] —
    // not real aliases.
    if ([
      'using', 'on', 'where', 'inner', 'left', 'right', 'full',
      'outer', 'cross', 'group', 'order', 'limit', 'having',
    ].includes(lower)) continue;
    aliases.push(alias);
  }
  if (aliases.length === 0) return sql;
  let out = sql;
  for (const a of aliases) {
    const escA = a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    out = out.replace(new RegExp(`\\b${escA}\\.`, 'g'), '');
  }
  // Drop the alias tokens from FROM/JOIN clauses too.
  out = out.replace(/(\bfrom\s+\w+)\s+\w+\b/gi, (full, head) => {
    const tail = full.slice(head.length).trim().toLowerCase();
    return [
      'where','group','order','limit','having','using','on','join',
      'inner','left','right','full','outer','cross',
    ].includes(tail) ? full : head;
  });
  out = out.replace(/(\bjoin\s+\w+)\s+\w+\b/gi, (full, head) => {
    const tail = full.slice(head.length).trim().toLowerCase();
    return [
      'on','using','where','group','order','limit','having','join',
      'inner','left','right','full','outer','cross',
    ].includes(tail) ? full : head;
  });
  return out;
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/scopes/query.ts
git commit -m "feat(atlas): scope for Chapter 08 Query — catalog re-export + run-mode heuristic"
```

---

## Task 2: QueryLibrary component — left rail

**Files:**
- Create: `clients/examples-web/src/atlas/components/QueryLibrary.tsx`

The library rail mirrors the FilterRail's visual position (left edge under ChapterHead) but holds a search input and feature-grouped catalog instead of chips. Visually: amber group headers, mono entries, the selected entry highlighted in amber.

- [ ] **Step 1: Write the component**

```tsx
// clients/examples-web/src/atlas/components/QueryLibrary.tsx
/**
 * QueryLibrary — left rail catalog for Chapter 08. Groups the global
 * query library by feature (Joins / Filters / Aggregations / Pivots /
 * Views / Window Functions); search filters in place. Click an entry
 * to select it — selection drives the SQL editor on the right.
 */
import { useMemo, useState } from 'react';
import {
  QUERIES,
  FEATURE_LABEL,
  FEATURE_ORDER,
  type QueryEntry,
  type QueryFeature,
} from '../scopes/query';

interface QueryLibraryProps {
  selectedId: string;
  onSelect: (q: QueryEntry) => void;
}

export function QueryLibrary({ selectedId, onSelect }: QueryLibraryProps) {
  const [filter, setFilter] = useState('');
  const filtered = useMemo<QueryEntry[]>(() => {
    const needle = filter.trim().toLowerCase();
    if (!needle) return QUERIES;
    return QUERIES.filter((q) =>
      q.title.toLowerCase().includes(needle) ||
      q.synopsis.toLowerCase().includes(needle) ||
      q.sql.toLowerCase().includes(needle),
    );
  }, [filter]);

  const groups = useMemo(() => {
    const byFeature = new Map<QueryFeature, QueryEntry[]>();
    for (const q of filtered) {
      const arr = byFeature.get(q.feature) ?? [];
      arr.push(q);
      byFeature.set(q.feature, arr);
    }
    return FEATURE_ORDER
      .map((f) => ({ feature: f, entries: byFeature.get(f) ?? [] }))
      .filter((g) => g.entries.length > 0);
  }, [filtered]);

  return (
    <aside
      style={{
        position: 'relative',
        zIndex: 1,
        width: 280,
        minWidth: 280,
        display: 'flex',
        flexDirection: 'column',
        borderRight: '1px solid var(--atlas-rule)',
        minHeight: 0,
      }}
    >
      <div style={{ padding: '14px 16px 10px', borderBottom: '1px solid var(--atlas-rule)' }}>
        <div style={{ fontSize: 10, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)', paddingBottom: 8 }}>
          QUERY LIBRARY · {QUERIES.length}
        </div>
        <input
          type="text"
          placeholder="search…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          style={{
            width: '100%',
            background: 'transparent',
            border: '1px solid var(--atlas-rule)',
            color: 'var(--atlas-fg)',
            fontFamily: 'var(--atlas-font)',
            fontSize: 11,
            padding: '6px 8px',
            outline: 'none',
          }}
        />
      </div>
      <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
        {groups.map((g) => (
          <div key={g.feature} style={{ padding: '6px 0' }}>
            <div style={{
              padding: '6px 16px',
              fontSize: 10,
              letterSpacing: '.18em',
              color: 'var(--atlas-amber)',
            }}>
              {FEATURE_LABEL[g.feature]}
            </div>
            {g.entries.map((q) => {
              const selected = q.id === selectedId;
              return (
                <button
                  key={q.id}
                  onClick={() => onSelect(q)}
                  style={{
                    display: 'block',
                    width: '100%',
                    textAlign: 'left',
                    background: selected ? 'var(--atlas-amber-soft)' : 'transparent',
                    border: 'none',
                    borderLeft: selected ? '2px solid var(--atlas-amber)' : '2px solid transparent',
                    padding: '6px 14px',
                    color: selected ? 'var(--atlas-amber)' : 'var(--atlas-fg)',
                    fontFamily: 'var(--atlas-font)',
                    fontSize: 11,
                    cursor: 'pointer',
                  }}
                  title={q.synopsis}
                >
                  {q.title}
                </button>
              );
            })}
          </div>
        ))}
        {groups.length === 0 && (
          <div style={{ padding: '12px 16px', fontSize: 10, color: 'var(--atlas-fg-faint)' }}>
            no matches
          </div>
        )}
      </div>
    </aside>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/QueryLibrary.tsx
git commit -m "feat(atlas): QueryLibrary — left-rail catalog with search + feature groups"
```

---

## Task 3: SqlEditor component — top right

**Files:**
- Create: `clients/examples-web/src/atlas/components/SqlEditor.tsx`

A plain controlled `<textarea>` styled in JetBrains Mono with amber-tinted line numbers gutter, a Run button, and an error / status strip beneath. No CodeMirror or Monaco — keep the dependency footprint flat; the legacy ex08 used the same approach.

- [ ] **Step 1: Write the component**

```tsx
// clients/examples-web/src/atlas/components/SqlEditor.tsx
/**
 * SqlEditor — controlled-textarea SQL editor for Chapter 08. Plain
 * <textarea> rather than CodeMirror/Monaco so the bundle stays flat;
 * the legacy ex08 used the same approach. Run button hands the
 * current text to the chapter's onRun handler. Status strip beneath
 * shows the active run's elapsed time, row count, or any error.
 */
import { useEffect, useRef } from 'react';

interface SqlEditorProps {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  /** Right-side status, e.g. '5,210 rows · 32 ms · live' or '—'. */
  status?: string;
  /** Error message; renders red when set. */
  error?: string | null;
  /** Disables the Run button while a query is opening. */
  busy?: boolean;
}

export function SqlEditor({ value, onChange, onRun, status, error, busy }: SqlEditorProps) {
  const taRef = useRef<HTMLTextAreaElement | null>(null);

  // Cmd/Ctrl+Enter to run.
  useEffect(() => {
    const ta = taRef.current;
    if (!ta) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        onRun();
      }
    };
    ta.addEventListener('keydown', onKey);
    return () => ta.removeEventListener('keydown', onKey);
  }, [onRun]);

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      minHeight: 0,
      borderBottom: '1px solid var(--atlas-rule)',
    }}>
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '10px 18px',
        borderBottom: '1px solid var(--atlas-rule)',
      }}>
        <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>
          SQL · editable · ⌘↩ to run
        </div>
        <button
          onClick={onRun}
          disabled={busy}
          style={{
            background: 'var(--atlas-amber)',
            color: 'var(--atlas-ink)',
            border: 'none',
            padding: '5px 14px',
            fontFamily: 'var(--atlas-font)',
            fontSize: 11,
            fontWeight: 700,
            letterSpacing: '.18em',
            cursor: busy ? 'wait' : 'pointer',
            opacity: busy ? 0.6 : 1,
          }}
        >
          {busy ? 'RUNNING…' : 'RUN'}
        </button>
      </div>
      <textarea
        ref={taRef}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        spellCheck={false}
        style={{
          flex: 1,
          minHeight: 180,
          width: '100%',
          padding: '12px 18px',
          background: 'var(--atlas-ink-2)',
          color: 'var(--atlas-fg)',
          border: 'none',
          outline: 'none',
          resize: 'none',
          fontFamily: 'var(--atlas-font)',
          fontSize: 12,
          lineHeight: 1.55,
          tabSize: 2,
        }}
      />
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        padding: '8px 18px',
        fontSize: 10,
        borderTop: '1px solid var(--atlas-rule)',
        background: 'var(--atlas-surface)',
      }}>
        <div style={{ color: error ? 'var(--atlas-neg)' : 'var(--atlas-fg-faint)' }}>
          {error ?? status ?? '—'}
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/SqlEditor.tsx
git commit -m "feat(atlas): SqlEditor — controlled textarea + Run button + status strip"
```

---

## Task 4: QueryResult component — result grid (live + static modes)

**Files:**
- Create: `clients/examples-web/src/atlas/components/QueryResult.tsx`

Holds the dual-mode grid. In live mode, takes a `SubscriptionHandle` and renders like `DataTable` does (boundRows seed + applyTransactionAsync deltas). In static mode, takes a flat `Row[]` and renders once. Column defs are inferred from the first row each Run — number columns get a thousands-separator formatter, anything ending `_bps` gets bps, etc.

- [ ] **Step 1: Write the component**

```tsx
// clients/examples-web/src/atlas/components/QueryResult.tsx
/**
 * QueryResult — result grid for Chapter 08, dual-mode.
 *
 *   - live mode  : bound to a SubscriptionHandle from useLiveQuery;
 *                  seeds rowData from getSnapshot() then ticks via
 *                  applyTransactionAsync — same race-safe pattern
 *                  DataTable uses.
 *   - static mode: takes a flat Row[] (the SOW result of a one-shot
 *                  multi-topic JOIN) and renders frozen.
 *
 * Column defs are inferred per Run from the first row: number cols
 * get a thousands-separator formatter; *_bps gets bps; *_usd / pnl /
 * fees / notional get currency.
 */
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
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface QueryResultProps {
  title?: string;
  status?: string;
  /** Set in live mode. Ignored in static mode. */
  liveSubscription?: SubscriptionHandle;
  /** Set in static mode. Ignored in live mode. */
  staticRows?: Row[];
  /** Stable row id extractor. Required in live mode. */
  getRowId?: (row: Row) => string;
}

export function QueryResult({
  title,
  status,
  liveSubscription,
  staticRows,
  getRowId,
}: QueryResultProps) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const apiRef = useRef<GridApi<Row> | null>(null);
  const seededRef = useRef<SubscriptionHandle | null>(null);
  const [boundRows, setBoundRows] = useState<Row[] | null>(null);

  // Live-mode seed + delta wiring — copies the DataTable race-safe
  // pattern verbatim. Identity change wipes; getSnapshot drives the
  // seed; subscribeSnapshotChunks + subscribeStatus retrigger seed
  // checks; subscribeDeltas does applyTransactionAsync.
  useEffect(() => {
    if (!liveSubscription) return;
    if (seededRef.current !== liveSubscription) {
      seededRef.current = liveSubscription;
      setBoundRows(null);
      apiRef.current?.setGridOption('rowData', []);
    }
    const trySeed = () => {
      if (seededRef.current !== liveSubscription) return;
      if (boundRows !== null) return;
      const snap = liveSubscription.getSnapshot();
      if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
      setBoundRows(snap as Row[]);
    };
    trySeed();
    const offS = liveSubscription.subscribeStatus(trySeed);
    const offC = liveSubscription.subscribeSnapshotChunks(() => trySeed());
    const offD = liveSubscription.subscribeDeltas((batch) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({
        add: batch.add as Row[],
        update: batch.update as Row[],
        remove: batch.remove as Row[],
      });
    });
    return () => { offS(); offC(); offD(); };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveSubscription]);

  // Effective rows — live mode reads from boundRows; static mode reads
  // from props directly.
  const effective = liveSubscription ? (boundRows ?? []) : (staticRows ?? []);

  // Cols inferred from the first row each render — cheap, no useMemo
  // gating because empty → populated is a single transition.
  const colDefs = useMemo<ColDef[]>(() => inferColDefs(effective), [effective.length === 0 ? null : effective[0]]); // eslint-disable-line react-hooks/exhaustive-deps

  // Inject per-column flash when bound to a live sub — same AG-Grid
  // v35 requirement DataTable handles.
  const flashColDefs = useMemo<ColDef[]>(
    () => liveSubscription
      ? colDefs.map((c) => ({ ...c, enableCellChangeFlash: true }))
      : colDefs,
    [colDefs, liveSubscription],
  );

  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: Row }) => getRowId(data) : undefined),
    [getRowId],
  );

  return (
    <div style={{
      flex: 1,
      display: 'flex',
      flexDirection: 'column',
      minHeight: 0,
      padding: '12px 18px 0',
    }}>
      {(title || status) && (
        <div style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          paddingBottom: 10,
        }}>
          <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>
            {title ?? 'RESULT'}
          </div>
          {status && (
            <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div>
          )}
        </div>
      )}
      <div style={{ flex: 1, minHeight: 180, width: '100%', height: '100%' }}>
        <AgGridReact<Row>
          theme={theme}
          rowData={effective}
          columnDefs={flashColDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
          onGridReady={(e: GridReadyEvent<Row>) => { apiRef.current = e.api; }}
        />
      </div>
    </div>
  );
}

function inferColDefs(rows: Row[]): ColDef[] {
  if (!rows || rows.length === 0) return [];
  const sample = rows[0];
  return Object.keys(sample).map((k): ColDef => {
    const probe = rows.find((r) => r[k] != null)?.[k];
    const isNumber = typeof probe === 'number';
    const lk = k.toLowerCase();
    let valueFormatter: ColDef['valueFormatter'];
    if (isNumber) {
      if (/bps$/.test(lk)) {
        valueFormatter = (p) => fmtBps(p.value as number);
      } else if (/_usd$|notional|exposure|mv$|pnl|fees|var/.test(lk)) {
        valueFormatter = (p) => fmtMillions(p.value as number);
      } else {
        valueFormatter = (p) => (p.value as number)?.toLocaleString('en-US', { maximumFractionDigits: 2 }) ?? '—';
      }
    }
    return {
      field: k,
      headerName: k,
      width: 140,
      type: isNumber ? 'numericColumn' : undefined,
      valueFormatter,
    };
  });
}

function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/atlas/components/QueryResult.tsx
git commit -m "feat(atlas): QueryResult — dual-mode result grid (live + static, cols inferred)"
```

---

## Task 5: QueryChapter — wire everything + add to routing

**Files:**
- Create: `clients/examples-web/src/atlas/chapters/QueryChapter.tsx`
- Modify: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx` (will be moved in Task 6; mutate in place first)

The chapter owns: the currently selected QueryEntry, the editor's draft SQL, the run-state machine (live vs static), an error string, and the busy flag. Run handler:

1. Snap mode from `detectRunMode(editorValue)`.
2. Live: build `LiveQuerySpec` { topic, sql, getRowId: adhocRowId }; flip `liveSpec` so `useLiveQuery` opens.
3. Static: `setBusy(true)`; `await runOneShotSql(topic, sql)`; set `staticRows` + `elapsedMs`; clear busy.

- [ ] **Step 1: Write the chapter**

```tsx
// clients/examples-web/src/atlas/chapters/QueryChapter.tsx
/**
 * Query — Chapter 08, Ad-Hoc SQL. The catalog rail on the left, an
 * editable SQL editor top right, a result grid bottom right. The
 * runner forks by mode (see scopes/query.ts comment): live for
 * single-topic queries, static for multi-topic JOIN queries.
 */
import { useMemo, useState } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { QueryLibrary } from '../components/QueryLibrary';
import { SqlEditor } from '../components/SqlEditor';
import { QueryResult } from '../components/QueryResult';
import {
  QUERIES,
  detectRunMode,
  detectFromTopic,
  stripAliases,
  fmtCount,
  fmtMs,
  type QueryEntry,
} from '../scopes/query';
import { useLiveQuery, type LiveQuerySpec } from '@/lib/use-live-query';
import { runOneShotSql, type Row } from '@/lib/use-subscription';

const adhocRowId = (r: Row): string =>
  String(
    r.position_id ?? r.trade_id ?? r.cusip ?? r.book_name ?? r.symbol ??
    (r.book_id != null && r.cusip != null ? `${r.book_id}|${r.cusip}` : undefined) ??
    (r.issuer_sector != null && r.issuer_region != null
      ? `${r.issuer_sector}|${r.issuer_region}` : undefined) ??
    JSON.stringify(r),
  );

interface StaticRun { mode: 'static'; rows: Row[]; elapsedMs: number; qid: number; }
interface LiveRun { mode: 'live'; spec: LiveQuerySpec; qid: number; }
type Run = StaticRun | LiveRun;

export function QueryChapter() {
  const [selected, setSelected] = useState<QueryEntry>(QUERIES[0]!);
  const [editorValue, setEditorValue] = useState<string>(QUERIES[0]!.sql);
  const [run, setRun] = useState<Run | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const liveSpec = run?.mode === 'live' ? run.spec : null;
  const live = useLiveQuery(liveSpec);

  const runQuery = async () => {
    setError(null);
    // Strip `alias.` prefixes so cqserver's parser doesn't trip on
    // `p.symbol`-style references (it has no alias-resolution table).
    const wireSql = stripAliases(editorValue);
    const mode = detectRunMode(wireSql);
    const topic = detectFromTopic(wireSql);
    const qid = Date.now();
    if (mode === 'live') {
      setRun({ mode: 'live', spec: { topic, sql: wireSql, getRowId: adhocRowId }, qid });
      return;
    }
    setBusy(true);
    const started = performance.now();
    try {
      const rows = await runOneShotSql(topic, wireSql);
      const elapsedMs = performance.now() - started;
      setRun({ mode: 'static', rows: rows as Row[], elapsedMs, qid });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRun(null);
    } finally {
      setBusy(false);
    }
  };

  const onSelectQuery = (q: QueryEntry) => {
    setSelected(q);
    setEditorValue(q.sql);
    setError(null);
    setRun(null);
  };

  const liveError = live?.error ?? null;
  const surfacedError = error ?? liveError;

  const rowCount = run?.mode === 'static'
    ? run.rows.length
    : run?.mode === 'live' ? live?.size ?? 0 : 0;
  const elapsed = run?.mode === 'static' ? fmtMs(run.elapsedMs) : '—';
  const status = surfacedError
    ? `error · ${surfacedError}`
    : run?.mode === 'live'
      ? `${rowCount.toLocaleString()} rows · live`
      : run?.mode === 'static'
        ? `${rowCount.toLocaleString()} rows · ${elapsed} · static (JOIN)`
        : 'press RUN';

  const kpis = useMemo<Kpi[]>(() => [
    { label: 'CATALOG', value: fmtCount(QUERIES.length), caption: 'pre-built queries', emphasis: true },
    { label: 'MODE', value: run?.mode?.toUpperCase() ?? '—', caption: 'live = stream · static = SOW' },
    { label: 'ROWS', value: fmtCount(rowCount), caption: 'result' },
    { label: 'ELAPSED', value: elapsed, caption: 'one-shot run' },
    { label: 'STATE', value: surfacedError ? 'ERROR' : busy ? 'BUSY' : run ? 'OK' : 'IDLE',
      caption: 'runner', emphasis: !!surfacedError || busy },
  ], [run, rowCount, elapsed, busy, surfacedError]);

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 08 — QUERY"
        title="query."
        sub="Pick from the catalog or write your own. Single-topic queries open a live sowAndSubscribe and tick on every match; multi-topic JOIN queries fall back to a one-shot SOW because cqserver's join evaluator is on the static path only."
        hero={<HeroMetric label="RESULT" value={fmtCount(rowCount)} detail={status} />}
      />
      <KpiStrip kpis={kpis} />
      <div style={{
        position: 'relative',
        zIndex: 1,
        flex: 1,
        display: 'flex',
        flexDirection: 'row',
        minHeight: 0,
      }}>
        <QueryLibrary selectedId={selected.id} onSelect={onSelectQuery} />
        <div style={{
          flex: 1,
          display: 'flex',
          flexDirection: 'column',
          minHeight: 0,
        }}>
          <SqlEditor
            value={editorValue}
            onChange={setEditorValue}
            onRun={runQuery}
            status={status}
            error={surfacedError}
            busy={busy}
          />
          <QueryResult
            title={run?.mode === 'live' ? 'RESULT · live · ticking' : 'RESULT · static'}
            status={status}
            liveSubscription={run?.mode === 'live' ? live ?? undefined : undefined}
            staticRows={run?.mode === 'static' ? run.rows : undefined}
            getRowId={adhocRowId}
          />
        </div>
      </div>
    </>
  );
}
```

- [ ] **Step 2: Add to AtlasPreviewApp routing**

Edit `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`:
1. Import the chapter: `import { QueryChapter } from '../chapters/QueryChapter';`
2. Add the branch in the chained ternary just before `<ComingSoon id={active} />`:
```tsx
   : active === 'query' ? <QueryChapter />
```
3. Update the `hint` prop on `<AppShell>` from `"phase 4 · chapters 01–07 live"` to `"phase 5 · chapters 01–08 live"`.

- [ ] **Step 3: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/atlas/chapters/QueryChapter.tsx \
        clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx
git commit -m "feat(atlas): QueryChapter — Chapter 08 ad-hoc SQL (live + static modes)"
```

---

## Task 6: Rename AtlasPreviewApp → AtlasApp; swap `/` to Atlas

**Files:**
- Create: `clients/examples-web/src/atlas/app/AtlasApp.tsx`
- Modify: `clients/examples-web/src/App.tsx`
- Modify: `clients/examples-web/index.html` (title + any stylesheet drops)
- Delete: `clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx`
- Delete: `clients/examples-web/src/atlas/preview/` (empty after move)

The legacy fork in App.tsx goes away. `/` and `/#atlas` both render `AtlasApp`. The hash check is dropped.

- [ ] **Step 1: Move AtlasPreviewApp to its production home**

```bash
mkdir -p clients/examples-web/src/atlas/app
git mv clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx \
       clients/examples-web/src/atlas/app/AtlasApp.tsx
rmdir clients/examples-web/src/atlas/preview
```

- [ ] **Step 2: Rename the exported component**

Edit `clients/examples-web/src/atlas/app/AtlasApp.tsx`:
- Rename `export function AtlasPreviewApp()` → `export function AtlasApp()`.
- Update the `hint` to `"chapters 01–08 live"` (drop the "phase 5" suffix; it's no longer a preview).

- [ ] **Step 3: Rewrite App.tsx**

Replace the whole file with:

```tsx
// clients/examples-web/src/App.tsx
/**
 * App entrypoint. The Atlas chapter app is now the only UI; the
 * legacy 8-tab dock was retired in Phase 5 (see git history for the
 * old shell at AtlasPreviewApp + LegacyApp).
 */
import { AtlasApp } from '@/atlas/app/AtlasApp';

export function App() {
  return <AtlasApp />;
}
```

- [ ] **Step 4: Update index.html**

Open `clients/examples-web/index.html`. If the `<title>` references the legacy name, swap it to `cq · atlas`. If any `<link rel="stylesheet">` points at a legacy css file that's about to be deleted in Task 7, delete that line too. Otherwise leave the file alone.

- [ ] **Step 5: Typecheck**

Run: `cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit`
Expected: zero errors. If errors point at imports of `AtlasPreviewApp` from outside the renamed file, fix them — there shouldn't be any other consumers.

- [ ] **Step 6: Smoke run**

Run: `cd clients/examples-web && npx vite build 2>&1 | tail -20`
Expected: clean build, no missing-import errors.

- [ ] **Step 7: Commit**

```bash
git add clients/examples-web/src/atlas/app/AtlasApp.tsx \
        clients/examples-web/src/App.tsx \
        clients/examples-web/index.html
git rm clients/examples-web/src/atlas/preview/AtlasPreviewApp.tsx 2>/dev/null || true
git commit -m "feat(atlas): swap / to AtlasApp — drop legacy LegacyApp fork"
```

---

## Task 7: Retire legacy files

**Files:**
- Delete: `clients/examples-web/src/examples/` (entire directory)
- Delete: `clients/examples-web/src/components/atlas/AtlasHeader.tsx`, `ContextBar.tsx`, `DockSurface.tsx`
- Delete: `clients/examples-web/src/components/panels/` (entire directory)
- Delete: `clients/examples-web/src/components/theme/` (entire directory)
- Delete: `clients/examples-web/src/components/ui/` (only files that grep shows no Atlas consumer for)
- Delete: `clients/examples-web/src/lib/features.ts`
- Delete: `clients/examples-web/src/lib/use-filtered-subscription.ts` (if no consumer)
- Delete: legacy CSS blocks in `src/styles/globals.css` (atlas-tabstrip*, atlas-tab*, legacy panel chrome)

Run-don't-guess: every delete is preceded by a grep showing zero references from within `src/atlas/` or any other surviving file.

- [ ] **Step 1: Enumerate what's still referenced**

```bash
cd clients/examples-web
# What's reachable from the new App.tsx?
echo "=== Atlas imports ==="
rg --type ts --type tsx '^import.*from' src/atlas src/App.tsx src/main.tsx | \
  awk -F"'" '{print $2}' | sort -u

# What's referenced from src/atlas/?
echo "=== referenced legacy paths ==="
rg "@/(components|examples|lib)/" src/atlas src/App.tsx -o -N | sort -u
```

Use the second list to filter Task 7's deletes — anything that shows up there is still in use.

Expected: `@/components/panels/*`, `@/components/atlas/{AtlasHeader,ContextBar,DockSurface}`, `@/examples/*`, `@/lib/features`, `@/components/theme/*` should NOT appear. If any do appear, fix the references first (most likely Atlas was relying on a util that needs migrating).

- [ ] **Step 2: Delete src/examples/**

```bash
git rm -r clients/examples-web/src/examples
```

- [ ] **Step 3: Delete legacy atlas-named components**

```bash
git rm clients/examples-web/src/components/atlas/AtlasHeader.tsx \
       clients/examples-web/src/components/atlas/ContextBar.tsx \
       clients/examples-web/src/components/atlas/DockSurface.tsx
# If src/components/atlas/ has any remaining files, leave them.
# Otherwise:
rmdir clients/examples-web/src/components/atlas 2>/dev/null || true
```

- [ ] **Step 4: Delete legacy panels, theme, ui**

```bash
git rm -r clients/examples-web/src/components/panels
git rm -r clients/examples-web/src/components/theme

# UI primitives — check first which (if any) the atlas/ tree uses.
rg "@/components/ui/" clients/examples-web/src/atlas | head -20
# If empty, delete the whole folder:
# git rm -r clients/examples-web/src/components/ui
# Otherwise delete only the unreferenced files.
```

- [ ] **Step 5: Delete lib files only used by legacy**

```bash
# features.ts — only the tabstrip uses FEATURE_META.
rg "from '@/lib/features'" clients/examples-web/src/atlas
# If empty:
git rm clients/examples-web/src/lib/features.ts

# use-filtered-subscription — alias for use-subscription; check atlas.
rg "from '@/lib/use-filtered-subscription'" clients/examples-web/src/atlas
# If empty:
git rm clients/examples-web/src/lib/use-filtered-subscription.ts
```

- [ ] **Step 6: Strip legacy CSS from globals.css**

Open `clients/examples-web/src/styles/globals.css`. Remove any rule scoped to `.atlas-tabstrip*`, `.atlas-tab*`, `.atlas-tab-feature`, `.atlas-tab-live`, `.atlas-tab-serial` — these classes belong to the deleted legacy tabstrip. Keep `.atlas-root` and any rule that doesn't reference deleted classes. If unsure, leave the rule; an unused CSS rule is harmless next to a missing-class error.

- [ ] **Step 7: Typecheck**

```bash
cd clients/examples-web && npx tsc -p tsconfig.app.json --noEmit
```
Expected: zero errors. If any "Cannot find module '@/...'" error appears, restore the file (`git checkout HEAD -- <path>`) and investigate which Atlas file still references it.

- [ ] **Step 8: Vite build**

```bash
cd clients/examples-web && npx vite build 2>&1 | tail -30
```
Expected: clean build. Warnings about chunk size are fine.

- [ ] **Step 9: Commit**

```bash
git add -u
# Confirm git status shows only deletions + the globals.css edit.
git commit -m "chore(atlas): retire legacy 8-tab dock + examples + panels + theme

The Atlas chapter app at / is now the sole UI; nothing references the
legacy DockSurface / ExampleCanvas / panel chrome / feature dots
anymore. Drop them, plus the use-filtered-subscription alias and the
legacy CSS blocks in globals.css.

Files preserved:
  - src/atlas/                    — the chapter app
  - src/lib/worker/               — SharedWorker data layer
  - src/lib/use-subscription.ts   — canonical subscription hook
  - src/lib/use-live-query.ts     — used by QueryChapter
  - src/lib/use-filtered-aggregate.ts — used by TapeChapter
  - src/lib/queries/library.ts    — catalog (consumed by scopes/query.ts)
  - src/lib/schema/, refdata.ts   — shared by chapters + publisher
  - src/styles/tokens.css         — design tokens (Atlas-root scoped)
  - src/styles/globals.css        — kept; only legacy rules stripped"
```

---

## Task 8: Final smoke + docs update

**Files:**
- Modify: `clients/examples-web/CLAUDE.md` (if it documents the legacy app)
- Modify: `README.md` at repo root (if it points at /#atlas or 8-tab dock)
- Modify: `docs/superpowers/SESSION-HANDOFF-2026-05-28.md` (mark Phase 5+6 complete)

- [ ] **Step 1: Final typecheck + vite build**

```bash
cd clients/examples-web
npx tsc -p tsconfig.app.json --noEmit
npx vite build 2>&1 | tail -20
```
Expected: both clean.

- [ ] **Step 2: Manual smoke**

Start the demo and click through every chapter:
```bash
cd /Users/develop/cqserver
RESEED=0 POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh
# Open http://localhost:5175/
```
Check that:
- `/` renders Atlas (not legacy) at first paint.
- All 8 chapters appear in the stations rail and switch.
- Pulse: ladder + bars + grid populate; ticking visible on grid + bars subtly settle.
- Tape: STATUS chip defaults to FILLED; grid populates; FEES KPI is sane.
- Lens: pivot grid populates.
- Heat: matrix renders with amber + red cells; cells pulse on tick.
- View: grid populates with chip filtering.
- Join: all three panes populate; chip filters LHS + MID but not RHS.
- Slip: 60/40 split renders; bars on the right rank by |slip|.
- Query: catalog renders; click "Equi-join: positions × trades" → RUN → static result populates; click "Per-book aggregate" (or any single-topic query) → RUN → live result ticks.

- [ ] **Step 3: Update docs/superpowers/SESSION-HANDOFF-2026-05-28.md**

Append at the bottom under a new `## Update — Phase 5+6 done` header noting:
- The handoff is now historical.
- `AtlasPreviewApp` is `AtlasApp` at `src/atlas/app/AtlasApp.tsx`.
- `/` renders Atlas; legacy retired.
- Phase 5 commit range for future archeology.

- [ ] **Step 4: Update CLAUDE.md / README if needed**

Run:
```bash
rg -i "legacy app|8-tab|dock|#atlas|AtlasPreviewApp|ex01|ex02" clients/examples-web/CLAUDE.md README.md 2>&1 | head -20
```
For each hit, edit out the legacy reference or rephrase to point at the Atlas chapter system.

- [ ] **Step 5: Commit**

```bash
git add -u
git commit -m "docs: mark Atlas redesign complete (Phase 5+6)"
```

---

## Done state

After all 8 tasks:
- `/` is the Atlas chapter app.
- 8 chapters live (01 PULSE through 08 QUERY).
- Zero legacy files remain (`src/examples/`, `src/components/panels/`, `src/components/theme/`, legacy `src/components/atlas/` subset all gone).
- `npx tsc` clean. `npx vite build` clean.
- Typecheck-only signals you'd expect: every chapter has `getRowId`, every DataTable/QueryResult bound to a live sub injects per-column flash, every chapter uses ChapterHead + (FilterRail|QueryLibrary) + KpiStrip + body.

# examples-web 3b — Query Builder Schema Catalog — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the Query Builder a live schema catalog — a panel listing every cqserver topic and view with its fields/types (from `GET /admin/catalog`), with click-to-insert into the SQL editor — so users author queries against the real schema, including admin-created views.

**Architecture:** A Vite dev proxy exposes the cqserver admin HTTP port (`:8085`) same-origin under `/cq-admin` (avoids CORS). A small `catalog.ts` client + `useCatalog` hook fetch `/admin/catalog`. A presentational `CatalogPanel` renders topics/views grouped, expandable to fields, with a refresh button and a click-to-insert callback. ex08 mounts the panel in its dock and appends inserted tokens to the editor. The existing live (single-topic) / static (JOIN) result handling in ex08 is unchanged.

**Tech Stack:** React 19 + TypeScript + Vite (examples-web), `fetch`, existing UI atoms (`PanelChrome`, `Badge`, lucide icons), the existing `SqlPanel`/`DockSurface`.

This is **Plan 3b of Sub-project 3** in `docs/superpowers/specs/2026-05-27-server-driven-examples-rewrite-design.md`. Plan 3a (totals view + GridPanel memo) is landed. Sub-project 1 supplies the `GET /admin/catalog` endpoint this consumes (shape: `[{ name, kind: "topic"|"view", columns: [{name, type}] }]`).

**Verification note:** examples-web has **no test runner** (only `dev`/`build`/`typecheck`). Each task is verified with `npm run typecheck` (runs `tsc -b`) and, for component/wiring tasks, `npm run build`. End-to-end behavior is a final manual browser check (requires the demo running: `./start-atlas-demo.sh`). Run all `npm` commands from `clients/examples-web/`.

---

## File Structure

- **Modify** `clients/examples-web/vite.config.ts` — add `server.proxy['/cq-admin']` → `http://127.0.0.1:8085`.
- **Create** `clients/examples-web/src/lib/catalog.ts` — catalog types + `fetchCatalog()` + admin-base resolution.
- **Create** `clients/examples-web/src/lib/use-catalog.ts` — `useCatalog()` React hook (`{entries, loading, error, refresh}`).
- **Create** `clients/examples-web/src/examples/ex08-query-builder/CatalogPanel.tsx` — presentational catalog tree + refresh + click-to-insert.
- **Modify** `clients/examples-web/src/examples/ex08-query-builder/index.tsx` — mount `CatalogPanel` in the dock; append inserted tokens to `editorValue`.

---

## Task 1: Vite dev proxy to the admin port

**Files:**
- Modify: `clients/examples-web/vite.config.ts` — the existing `server: { port: 5175, strictPort: false }` block.

- [ ] **Step 1: Add the proxy**

In `vite.config.ts`, replace the existing `server` block:

```ts
  server: {
    port: 5175,
    strictPort: false,
  },
```

with:

```ts
  server: {
    port: 5175,
    strictPort: false,
    proxy: {
      // The query-builder schema catalog is served by cqserver's admin
      // HTTP port (:8085), a different origin from this dev server
      // (:5175). Proxy it same-origin under /cq-admin so the browser
      // fetch needs no CORS. `${CQ_ADMIN_URL}` can override the base for
      // standalone deploys (see src/lib/catalog.ts).
      '/cq-admin': {
        target: 'http://127.0.0.1:8085',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/cq-admin/, ''),
      },
    },
  },
```

- [ ] **Step 2: Typecheck (vite.config is TS)**

Run: `cd clients/examples-web && npm run typecheck`
Expected: no errors. (`proxy` is a valid key on Vite's `server` options type; a typo would be a type error here.)

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/vite.config.ts
git commit -m "feat(examples-web): vite dev proxy /cq-admin -> cqserver admin :8085

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Catalog client (`catalog.ts`)

**Files:**
- Create: `clients/examples-web/src/lib/catalog.ts`.

- [ ] **Step 1: Write the file**

Create `clients/examples-web/src/lib/catalog.ts`:

```ts
/**
 * Schema catalog client. Fetches cqserver's `GET /admin/catalog` —
 * every topic + materialized view with its column list and types —
 * for the Query Builder's authoring panel.
 *
 * In dev, Vite proxies `/cq-admin` → the admin HTTP port (see
 * vite.config.ts), so this fetch is same-origin (no CORS). A
 * standalone deployment can set `window.CQ_ADMIN_URL` to a reachable
 * admin origin (which must then return CORS headers).
 */

/** One column of a topic/view schema. `type` is a lowercase scalar
 *  type string: "double" | "long" | "int" | "string" | "bool" |
 *  "timestamp" | "bytes". */
export interface CatalogColumn {
  name: string;
  type: string;
}

/** A topic or materialized view, with its columns. `kind` lets the UI
 *  group views separately from raw topics. */
export interface CatalogEntry {
  name: string;
  kind: 'topic' | 'view';
  columns: CatalogColumn[];
}

function adminBase(): string {
  if (typeof window === 'undefined') return '/cq-admin';
  return (window as unknown as { CQ_ADMIN_URL?: string }).CQ_ADMIN_URL ?? '/cq-admin';
}

/** Fetch the full catalog. Throws on a non-2xx response or network
 *  failure; callers surface that as an error state. */
export async function fetchCatalog(signal?: AbortSignal): Promise<CatalogEntry[]> {
  const res = await fetch(`${adminBase()}/admin/catalog`, { signal });
  if (!res.ok) {
    throw new Error(`catalog fetch failed: HTTP ${res.status}`);
  }
  return (await res.json()) as CatalogEntry[];
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/catalog.ts
git commit -m "feat(examples-web): catalog client for /admin/catalog

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `useCatalog` hook

**Files:**
- Create: `clients/examples-web/src/lib/use-catalog.ts`.

- [ ] **Step 1: Write the hook**

Create `clients/examples-web/src/lib/use-catalog.ts`:

```ts
import { useCallback, useEffect, useState } from 'react';
import { fetchCatalog, type CatalogEntry } from './catalog';

export interface CatalogState {
  entries: CatalogEntry[];
  loading: boolean;
  error: string | null;
  /** Re-fetch the catalog (e.g. after a view is created elsewhere). */
  refresh: () => void;
}

/**
 * Fetch the schema catalog on mount and expose a manual `refresh`.
 * Aborts the in-flight request on unmount / re-fetch so a late
 * response never sets state on an unmounted component.
 */
export function useCatalog(): CatalogState {
  const [entries, setEntries] = useState<CatalogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);

  const refresh = useCallback(() => setNonce((n) => n + 1), []);

  useEffect(() => {
    const ac = new AbortController();
    setLoading(true);
    setError(null);
    fetchCatalog(ac.signal)
      .then((e) => {
        if (ac.signal.aborted) return;
        setEntries(e);
        setLoading(false);
      })
      .catch((err: unknown) => {
        if (ac.signal.aborted) return;
        setError(err instanceof Error ? err.message : String(err));
        setLoading(false);
      });
    return () => ac.abort();
  }, [nonce]);

  return { entries, loading, error, refresh };
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/use-catalog.ts
git commit -m "feat(examples-web): useCatalog hook (fetch + refresh)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `CatalogPanel` component

**Files:**
- Create: `clients/examples-web/src/examples/ex08-query-builder/CatalogPanel.tsx`.

Presentational tree: topics group + views group, each entry expandable to its columns. Clicking a table name inserts it WITHOUT the leading slash (the builder writes `FROM positions`, not `/positions`); clicking a column inserts the bare column name. A refresh button re-fetches. Uses the existing `PanelChrome`, `Badge`, `cn`, and lucide icons to match the sibling Library panel's style.

- [ ] **Step 1: Write the component**

Create `clients/examples-web/src/examples/ex08-query-builder/CatalogPanel.tsx`:

```tsx
import { useMemo, useState } from 'react';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';
import { ChevronDown, ChevronRight, RefreshCw, Table2, Layers } from 'lucide-react';
import { useCatalog } from '@/lib/use-catalog';
import type { CatalogEntry } from '@/lib/catalog';

interface CatalogPanelProps {
  /** Insert a token (table name sans slash, or a column name) into the
   *  SQL editor at the caller's discretion. */
  onInsert: (token: string) => void;
}

/** Strip the leading slash so a catalog name like `/positions` inserts
 *  as the bare `positions` the builder's FROM clause expects. */
function tableToken(name: string): string {
  return name.replace(/^\//, '');
}

export function CatalogPanel({ onInsert }: CatalogPanelProps) {
  const { entries, loading, error, refresh } = useCatalog();
  const [open, setOpen] = useState<Set<string>>(new Set());

  const { topics, views } = useMemo(() => {
    const topics: CatalogEntry[] = [];
    const views: CatalogEntry[] = [];
    for (const e of entries) (e.kind === 'view' ? views : topics).push(e);
    const byName = (a: CatalogEntry, b: CatalogEntry) => a.name.localeCompare(b.name);
    topics.sort(byName);
    views.sort(byName);
    return { topics, views };
  }, [entries]);

  const toggle = (name: string) =>
    setOpen((s) => {
      const n = new Set(s);
      if (n.has(name)) n.delete(name);
      else n.add(name);
      return n;
    });

  const renderEntry = (e: CatalogEntry) => {
    const isOpen = open.has(e.name);
    return (
      <div key={e.name} className="mb-0.5">
        <div className="flex items-center gap-1 px-2 py-1 group">
          <button
            onClick={() => toggle(e.name)}
            className="text-muted-foreground hover:text-foreground"
            aria-label={isOpen ? 'collapse' : 'expand'}
          >
            {isOpen ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
          </button>
          <button
            onClick={() => onInsert(tableToken(e.name))}
            className="flex-1 flex items-center gap-1.5 text-left text-[11.5px] font-medium text-foreground hover:text-accent-foreground"
            title={`Insert ${tableToken(e.name)}`}
          >
            {e.kind === 'view' ? <Layers size={11} /> : <Table2 size={11} />}
            <span className="truncate">{tableToken(e.name)}</span>
            <span className="ml-auto font-mono text-[9px] text-muted-foreground">
              {e.columns.length}
            </span>
          </button>
        </div>
        {isOpen ? (
          <div className="ml-5 border-l border-border pl-2">
            {e.columns.map((c) => (
              <button
                key={c.name}
                onClick={() => onInsert(c.name)}
                className="w-full flex items-center gap-1.5 px-1 py-0.5 text-left text-[11px] text-muted-foreground hover:text-accent-foreground hover:bg-accent rounded-sm"
                title={`Insert ${c.name}`}
              >
                <span className="truncate">{c.name}</span>
                <span className="ml-auto font-mono text-[9px] text-muted-foreground/70">
                  {c.type}
                </span>
              </button>
            ))}
          </div>
        ) : null}
      </div>
    );
  };

  return (
    <PanelChrome
      title="Schema Catalog"
      right={
        <button
          onClick={refresh}
          className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground"
          title="Refresh catalog"
        >
          <RefreshCw size={11} />
          refresh
        </button>
      }
    >
      <div className="overflow-y-auto py-1 h-full">
        {error ? (
          <div className="p-3 text-[11px]">
            <Badge variant="err" className="!text-[9px]">unavailable</Badge>
            <p className="text-muted-foreground mt-2 leading-relaxed">
              Couldn't reach the cqserver admin catalog ({error}). Is cqserver
              running and the dev proxy pointing at its admin port?
            </p>
          </div>
        ) : loading ? (
          <div className="p-3 text-[11px] text-muted-foreground">Loading catalog…</div>
        ) : (
          <>
            <div className="px-3 pt-1 pb-0.5 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground">
              Topics <span className="ml-1">{topics.length}</span>
            </div>
            {topics.map(renderEntry)}
            <div className="px-3 pt-2 pb-0.5 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground">
              Views <span className="ml-1">{views.length}</span>
            </div>
            {views.map(renderEntry)}
          </>
        )}
      </div>
    </PanelChrome>
  );
}
```

- [ ] **Step 2: Verify the lucide icons exist**

Run: `cd clients/examples-web && node -e "const i=require('lucide-react'); for (const n of ['ChevronDown','ChevronRight','RefreshCw','Table2','Layers']) if(!i[n]) throw new Error('missing icon '+n); console.log('icons OK')"`
Expected: `icons OK`. (If `Table2` or `Layers` is missing in the installed lucide version, substitute `Table` / `List` — both are stable lucide names — and update the imports + usages accordingly.)

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build`
Expected: typecheck clean; build succeeds.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/examples/ex08-query-builder/CatalogPanel.tsx
git commit -m "feat(examples-web): CatalogPanel — topics/views tree with click-to-insert

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Mount the catalog in the Query Builder dock

**Files:**
- Modify: `clients/examples-web/src/examples/ex08-query-builder/index.tsx` — import `CatalogPanel`; add a `catalog` panel to the `panels` array and a layout step to the `layout` array.

ex08 already owns `editorValue` / `setEditorValue` (state passed as `value` to `SqlPanel`, with `onChange={setEditorValue}`). Inserting = appending a token to `editorValue`; `SqlPanel` syncs its internal editor text from the `value` prop.

- [ ] **Step 1: Read the file first**

Read `clients/examples-web/src/examples/ex08-query-builder/index.tsx` and locate: the import block, the `panels: DockPanelSpec[]` array (entries `library`, `editor`, `results`, `synopsis`, `notes`), and the `layout: DockLayoutStep[]` array.

- [ ] **Step 2: Import CatalogPanel**

Add to the imports at the top of the file:

```ts
import { CatalogPanel } from './CatalogPanel';
```

- [ ] **Step 3: Add an insert helper + the catalog panel spec**

Inside `QueryBuilderCanvas`, near the other handlers (e.g. just before the `panels` array is built), add:

```ts
  // Append a catalog token to the editor with sensible spacing. The
  // SqlPanel mirrors the `value` prop into its editor, so updating
  // editorValue inserts the token live.
  const insertToken = (token: string) =>
    setEditorValue((v) => (v && !v.endsWith(' ') && !v.endsWith('\n') ? `${v} ${token}` : `${v}${token}`));
```

Then add this entry to the `panels` array (place it as the first element so it leads the rail, or alongside `library` — match the array's object style):

```tsx
    {
      id: 'catalog',
      title: 'Schema Catalog',
      render: () => <CatalogPanel onInsert={insertToken} />,
    },
```

- [ ] **Step 4: Add a layout step**

In the `layout` array, dock the catalog below the library panel (the left rail). Add after the existing `{ id: 'library' }` step:

```ts
    { id: 'catalog', relativeTo: 'library', direction: 'below' },
```

(If `library` is not the first/anchor panel in the layout, instead dock `catalog` relative to whichever panel anchors the left rail — match the existing `relativeTo` usage in this array. The goal: the catalog sits in the left rail near the Library, not overlapping the editor/results.)

- [ ] **Step 5: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build`
Expected: typecheck clean; build succeeds.

- [ ] **Step 6: Commit**

```bash
git add clients/examples-web/src/examples/ex08-query-builder/index.tsx
git commit -m "feat(examples-web): mount Schema Catalog in the Query Builder dock

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Manual end-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Launch the demo**

Run: `./start-atlas-demo.sh` (from repo root). Tear down later with `./stop-demo.sh`.

- [ ] **Step 2: Catalog panel populates**

Open `http://localhost:5175` → Query Builder tab. Expected: the Schema Catalog panel lists Topics (positions, trades, securities, fi-market-data) and Views (the `/v_*` set, shown sans slash). Expanding an entry shows its columns with types.

- [ ] **Step 3: Click-to-insert works**

Click a table name and a few column names. Expected: the tokens append into the SQL editor with spacing. Construct e.g. `SELECT issuer_sector, market_value_usd FROM positions` partly via clicks, then press Run ▸ — expected: a live result grid (single-topic → live).

- [ ] **Step 4: Created views appear after refresh**

Create a view out-of-band, then hit the panel's refresh:
```bash
curl -s -XPOST localhost:8085/admin/views -H 'content-type: application/json' \
  -d '{"name":"/v_demo_sectors","source":"/positions","sql":"SELECT issuer_sector, COUNT(*) AS n FROM t GROUP BY issuer_sector"}'
```
Click the catalog's **refresh**. Expected: `v_demo_sectors` now appears under Views with columns `issuer_sector`, `n`. Selecting it and running `SELECT issuer_sector, n FROM v_demo_sectors` streams live.

- [ ] **Step 5: Tear down**

Run: `./stop-demo.sh`

No commit (verification only). If any step fails, fix the relevant task before considering 3b complete.

---

## Self-Review (completed by author)

**Spec coverage** (design doc, Sub-project 3 — Query Builder catalog):
- Catalog panel: all topics + views + fields from `/admin/catalog` → Tasks 2-5. ✅
- Click-to-insert for authoring → Task 4 (`onInsert`) + Task 5 (`insertToken`). ✅
- Browser reaches admin HTTP cross-port → Task 1 (Vite proxy). ✅
- Live single-topic / static JOIN result handling → already present in ex08 (unchanged); the catalog feeds authoring. Noted, not re-implemented (YAGNI).
- Created views queryable → Task 6 verifies refresh surfaces a runtime-created view (consumes Sub-project 1's `POST /admin/views`).

**Placeholder scan:** No TBD/TODO; every code step is complete. The two conditional fallbacks (icon-name substitution in Task 4; `relativeTo` anchor in Task 5) give concrete alternatives, not vague instructions. ✅

**Type/name consistency:** `CatalogColumn`/`CatalogEntry` defined in Task 2, imported in Tasks 3-4; `fetchCatalog` (Task 2) used by `useCatalog` (Task 3) used by `CatalogPanel` (Task 4); `CatalogPanel`'s `onInsert` (Task 4) supplied by `insertToken` (Task 5); proxy path `/cq-admin` (Task 1) matches `adminBase()` default (Task 2). ✅

**Scope:** Catalog authoring only. Does not touch the live/static run logic, the admin create-view screen (Sub-project 2), or other examples. Self-contained. ✅

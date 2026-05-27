# Admin UI — Create-View Authoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator author a materialized view through the admin UI — pick a source, write the aggregate SQL, and create it — instead of editing `cqserver.toml` or curling `POST /admin/views`.

**Architecture:** Add `catalog()` + `createView()` to the admin-ui API client, then an inline "Create view" form on the existing `ViewsPage`. The form's source picker is populated from `GET /admin/catalog` (topics); submit calls `POST /admin/views`; on success it invalidates the React-Query `['views']`/`['catalog']` caches so the existing list refreshes and the new view appears. Server validation errors (bad SQL → 400, name collision → 409) surface inline.

**Tech Stack:** React 19 + TypeScript + Vite, react-router, TanStack React Query, shadcn/Radix UI, Tailwind (clients/admin-ui).

This is **Sub-project 2** of the design in `docs/superpowers/specs/2026-05-27-server-driven-examples-rewrite-design.md`. It consumes Sub-project 1's `GET /admin/catalog` and `POST /admin/views`. Deletion is out of scope (deferred per spec).

**Connectivity:** No proxy work needed — admin-ui's `adminBase` already resolves to the dev proxy `/admin-api` (→ `:8085`) in dev and same-origin (`''`) when served under `/ui` in prod (`src/lib/admin.ts:18-34`). `postJson` already surfaces a server error body as the thrown `Error.message` (`admin.ts:135-152`), so our 400/409 text bodies display.

**Verification note:** admin-ui has **no test runner** (scripts: `dev`/`build`/`preview`/`lint`). `npm run build` runs `tsc -b && vite build`, so it doubles as the typecheck gate. End-to-end behavior is a final manual check in the browser. Run `npm` commands from `clients/admin-ui/`.

---

## File Structure

- **Modify** `clients/admin-ui/src/lib/admin.ts` — add `CatalogColumn`/`CatalogEntry` types and `catalog()` + `createView()` to `adminApi`.
- **Modify** `clients/admin-ui/src/pages/ViewsPage.tsx` — add a `CreateViewForm` component (co-located) and render it; update the empty-state copy.

---

## Task 1: Add `catalog()` + `createView()` to the API client

**Files:**
- Modify: `clients/admin-ui/src/lib/admin.ts` — add types near the other response shapes (after `ViewInfo`, ~line 123) and methods to the `adminApi` object (~line 163-184).

- [ ] **Step 1: Add the catalog types**

After the `ViewInfo` interface (around line 123), add:

```ts
export interface CatalogColumn {
  name: string;
  type: string;
}

export interface CatalogEntry {
  name: string;
  kind: 'topic' | 'view';
  columns: CatalogColumn[];
}

export interface CreateViewRequest {
  name: string;
  source: string;
  sql: string;
}
```

- [ ] **Step 2: Add the API methods**

In the `adminApi` object literal, add these two entries (e.g. right after the `views:` line):

```ts
  catalog: () => get<CatalogEntry[]>('/admin/catalog'),
  createView: (body: CreateViewRequest) => postJson<ViewInfo>('/admin/views', body),
```

(`get`, `postJson`, and `ViewInfo` already exist in this file.)

- [ ] **Step 3: Build (typecheck + bundle)**

Run: `cd clients/admin-ui && npm run build 2>&1 | tail -6`
Expected: `tsc -b` passes and `vite build` succeeds (no type errors). A successful build is the gate.

- [ ] **Step 4: Commit**

```bash
git add clients/admin-ui/src/lib/admin.ts
git commit -m "feat(admin-ui): catalog() + createView() API client methods

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Inline Create-View form on ViewsPage

**Files:**
- Modify: `clients/admin-ui/src/pages/ViewsPage.tsx` — add imports, a `CreateViewForm` component, render it above the list/detail grid, and update the empty-state copy.

- [ ] **Step 1: Confirm color-token class names**

The form shows error text (red) and a success tick (green). Confirm the exact Tailwind token classes this app uses before writing them, by reading `clients/admin-ui/src/index.css` (or `globals.css`) and grepping existing pages:

Run:
```bash
cd clients/admin-ui && grep -rEn "text-err|text-destructive|text-ok|text-success|--destructive|--ok|--success" src/index.css src/**/*.css src/pages 2>/dev/null | head
```
Use whatever the app already uses: if `text-destructive` exists use it for errors (shadcn default), else if `text-err` is defined use that. For success use `text-ok`/`text-success`/`text-emerald-500` — whichever is defined; if none, fall back to `text-primary`. In the code below, the placeholders `ERR_CLASS` and `OK_CLASS` MUST be replaced with the confirmed class names.

- [ ] **Step 2: Add imports**

At the top of `ViewsPage.tsx`, extend the existing imports:
- Change `import { useQuery } from '@tanstack/react-query';` to:
  ```ts
  import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
  ```
- Change the lucide import `import { ArrowUpRight, Eye, RefreshCw } from 'lucide-react';` to add `Plus`:
  ```ts
  import { ArrowUpRight, Eye, Plus, RefreshCw } from 'lucide-react';
  ```

- [ ] **Step 3: Add the `CreateViewForm` component**

Add this component at the bottom of `ViewsPage.tsx` (near `ViewDetail`/`DetailCell`). Replace `ERR_CLASS`/`OK_CLASS` with the classes confirmed in Step 1:

```tsx
function CreateViewForm() {
  const qc = useQueryClient();
  const catalog = useQuery({
    queryKey: ['catalog'],
    queryFn: adminApi.catalog,
    refetchInterval: 10_000,
  });
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [source, setSource] = useState('');
  const [sql, setSql] = useState('');

  const sources = useMemo(
    () =>
      (catalog.data ?? [])
        .filter((e) => e.kind === 'topic')
        .map((e) => e.name)
        .sort(),
    [catalog.data],
  );

  const create = useMutation({
    mutationFn: () => adminApi.createView({ name: name.trim(), source, sql }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['views'] });
      qc.invalidateQueries({ queryKey: ['catalog'] });
      setName('');
      setSql('');
    },
  });

  const canSubmit = !!name.trim() && !!source && !!sql.trim() && !create.isPending;
  const inputCls =
    'h-8 px-2 rounded-md border border-border bg-input font-mono text-[12.5px] text-foreground focus:outline-none focus:ring-1 focus:ring-ring';

  if (!open) {
    return (
      <Button variant="secondary" size="sm" className="mb-3" onClick={() => setOpen(true)}>
        <Plus size={11} /> New view
      </Button>
    );
  }

  return (
    <Card className="mb-3">
      <CardHeader className="pb-2 border-b border-border flex flex-row items-center justify-between">
        <CardTitle>Create view</CardTitle>
        <Button variant="ghost" size="sm" onClick={() => setOpen(false)}>
          Cancel
        </Button>
      </CardHeader>
      <CardContent className="pt-3 space-y-2.5">
        <div className="grid grid-cols-2 gap-2.5">
          <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
            View name
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="/v_my_view"
              className={inputCls}
            />
          </label>
          <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
            Source topic
            <select
              value={source}
              onChange={(e) => setSource(e.target.value)}
              className={inputCls}
            >
              <option value="">Select a source…</option>
              {sources.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
        </div>
        <label className="flex flex-col gap-1 text-[11px] text-muted-foreground">
          SQL — aggregate query; FROM is interpreted as the source topic
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            rows={5}
            spellCheck={false}
            placeholder={'SELECT issuer_sector, COUNT(*) AS n\nFROM positions\nGROUP BY issuer_sector'}
            className="px-2 py-1.5 rounded-md border border-border bg-input font-mono text-[12px] text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
        </label>
        <div className="flex items-center gap-2">
          <Button size="sm" onClick={() => create.mutate()} disabled={!canSubmit}>
            {create.isPending ? 'Creating…' : 'Create view'}
          </Button>
          {create.isError ? (
            <span className="text-[11.5px] ERR_CLASS">{(create.error as Error).message}</span>
          ) : null}
          {create.isSuccess ? <span className="text-[11.5px] OK_CLASS">Created ✓</span> : null}
        </div>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 4: Render the form on the page**

In `ViewsPage`'s returned JSX, render `<CreateViewForm />` immediately AFTER the header `</div>` (the flex header block ending around line 62) and BEFORE the `{list.length === 0 ? (...)}` conditional. Insert on its own line:

```tsx
      <CreateViewForm />

```

- [ ] **Step 5: Update the empty-state copy**

In the empty-state `CardContent` (around lines 66-70), change the non-loading message from:

```tsx
              : 'No views configured. Declare a [[views]] block in cqserver.toml.'}
```
to:
```tsx
              : 'No views yet. Use “New view” above to create one, or declare a [[views]] block in cqserver.toml.'}
```

- [ ] **Step 6: Build (typecheck + bundle)**

Run: `cd clients/admin-ui && npm run build 2>&1 | tail -6`
Expected: `tsc -b` passes, `vite build` succeeds. Fix any unused-import or token-class issue. (If `ERR_CLASS`/`OK_CLASS` were left literally in, the build still passes — they'd just be unknown Tailwind classes that render unstyled — so DOUBLE-CHECK they were replaced with the real classes from Step 1.)

- [ ] **Step 7: Commit**

```bash
git add clients/admin-ui/src/pages/ViewsPage.tsx
git commit -m "feat(admin-ui): inline Create-View form on the Views page

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Manual end-to-end verification

**Files:** none (verification only).

- [ ] **Step 1: Launch cqserver + admin UI**

The admin UI is served by cqserver under `/ui` once built, OR run its dev server. Easiest full check:
```bash
# from repo root — ensure cqserver is running (admin :8085)
./start-atlas-demo.sh        # or: ./target/release/cqserver --config config/cqserver.toml
# in another shell, run the admin-ui dev server (proxies /admin-api -> :8085)
cd clients/admin-ui && npm run dev   # serves on :5174
```

- [ ] **Step 2: Create a view through the UI**

Open the admin UI (`http://localhost:5174` dev, or `http://localhost:8085/ui` if built) → **Views**. Click **New view**. Pick a source (e.g. `/positions`), name it `/v_ui_demo`, enter:
```sql
SELECT issuer_sector, COUNT(*) AS n FROM positions GROUP BY issuer_sector
```
Click **Create view**. Expected: success tick; the new `/v_ui_demo` appears in the Registered views list within a moment (cache invalidation), selectable with its SQL + row count.

- [ ] **Step 3: Error cases surface inline**

- Re-submit the same name → expect an inline error containing "collides" (HTTP 409).
- Enter invalid SQL (e.g. `SELECT nope FROM positions GROUP BY nope`) under a new name → expect an inline 400 error with the parser message; no view created.

- [ ] **Step 4: Persistence**

Confirm `config/runtime_views.toml` now contains `/v_ui_demo`. (Restart cqserver and reload Views — it should still be listed, proving Sub-project 1's persistence + recreation.)

- [ ] **Step 5: Tear down**

Stop the dev server and `./stop-demo.sh`.

No commit (verification only). If any step fails, fix the relevant task.

---

## Self-Review (completed by author)

**Spec coverage** (design doc, Sub-project 2):
- New authoring surface: source picker (from `/admin/catalog` topics), SQL editor, save → `POST /admin/views` → Tasks 1-2. ✅
- Lists existing views: the existing ViewsPage list, refreshed via React-Query invalidation on create. ✅
- Server validation errors surfaced inline → `postJson` error message in the mutation's `isError` branch. ✅
- No delete control → not added (deferred per spec). ✅
- Consumes Sub-project 1 endpoints (`/admin/catalog`, `POST /admin/views`) → Task 1 client methods. ✅

**Placeholder scan:** No TBD/TODO. The only intentional placeholders are `ERR_CLASS`/`OK_CLASS`, which Step 1 resolves to real, confirmed token classes before Step 3 writes them, and Step 6 double-checks. ✅

**Type/name consistency:** `CatalogEntry`/`CreateViewRequest`/`ViewInfo` (Task 1) are the types `adminApi.catalog`/`adminApi.createView` return/accept and that `CreateViewForm` (Task 2) consumes; React-Query keys `['views']`/`['catalog']`/`['topics']` match the existing `ViewsPage` query keys so invalidation actually refreshes the list. ✅

**Scope:** Two files, one screen. No routing/nav changes (form is inline on the existing Views page). No delete. Self-contained. ✅

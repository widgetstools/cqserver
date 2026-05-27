# Server-Driven Examples Rewrite — Design

**Date:** 2026-05-27
**Status:** Approved (design); pending implementation plan
**Scope owner:** examples-web demo app + supporting cqserver admin endpoints

## Goal

Rewrite the cqserver example web app (`clients/examples-web`) so it strictly
obeys four architectural rules, demonstrating that cqserver — not the browser —
does all data work:

1. **`rowData` seeds the initial SOW only.** A grid sets `rowData` once from the
   snapshot, never again.
2. **All ticking data is applied via the grid's `applyTransaction` /
   `applyTransactionAsync`.** Post-SOW changes never flow through the `rowData`
   prop.
3. **No data shaping or aggregation on the client.** cqserver features (filters,
   GROUP BY aggregates, pivots, materialized views) produce the exact rows; the
   client only renders them and lets AG-Grid sort/select.
4. **The Query Builder result grid is live** — it receives realtime updates, not
   a frozen snapshot, for every query shape the server can stream.

## Decisions (locked during brainstorming)

- **Rewrite scope:** Keep the existing data layer (`cq-store`,
  `useFilteredSubscription`, `useLiveQuery`, `GridPanel`'s imperative SOW-seed +
  `applyTransactionAsync` binding), the design system, and the dock shell.
  Rewrite the example components and add server + admin capabilities. The data
  layer already enforces rules 1 & 2 correctly.
- **Live joins:** Direct JOINs cannot stream on the `sowAndSubscribe` path
  (the live subscribe path parses against a single topic's schema). Live
  cross-topic data comes from **materialized views**. Users author views ahead
  of time through an **admin screen**; views are **persisted to a server-side
  config file** and **recreated on restart**.
- **Ad-hoc joins in the builder:** A JOIN the user types that has no backing
  view runs as a one-shot `sow()` snapshot, rendered with a clear "static —
  declare a view to go live" badge.
- **View persistence location:** A **dedicated `runtime_views.toml`** (path
  configurable, default `<config_dir>/runtime_views.toml`), separate from the
  hand-authored `cqserver.toml`, written programmatically (atomic temp+rename).
- **View deletion:** **Deferred.** v1 is create + persist + restart-recreate.
  No live teardown of a running view's runner/evaluator threads.

## Server capability ground truth (from source)

The live `sowAndSubscribe` path streams continuous deltas for these shapes,
registered by `Topic::subscribe_register` (`crates/cq-core/src/topic.rs:1641`):

- SELECT projections, WHERE filters, GROUP BY + aggregates (SUM/COUNT/AVG/MIN/
  MAX), HAVING, COUNT(DISTINCT), PIVOT/UNPIVOT (group-based).

These are **snapshot-only** (one-shot, no live deltas) because they need a
result-sized buffer or single-topic schema:

- ORDER BY, LIMIT/OFFSET, computed/arithmetic columns, `WHERE IN (SELECT…)`
  subqueries, derived tables (`crates/cq-core/src/topic.rs:1366-1380`,
  `crates/cq-transport/src/router.rs:1750-1826`).
- **All cross-topic JOINs** (INNER/LEFT/RIGHT/FULL/ASOF) — handled only by
  `deliver_join_snapshot` (`crates/cq-transport/src/router.rs:1640-1676`) or by
  a **materialized view** (e.g. `/v_trades_by_compliance` joins trades⋈positions
  and streams live).

Views are loaded at boot from `ServerConfig.views: Vec<ViewEntry>`
(`crates/cq-server/src/config.rs:626`) and each is stood up by the
runtime-callable `init_view(cfg, topics, registry)`
(`crates/cq-server/src/main.rs:818`), which inserts the view into the live
`topics` map, spawns the runner + evaluator threads, and makes it subscribable
immediately. `AdminState` already holds `topics` + `registry`
(`crates/cq-server/src/admin.rs:35`). `init_view` spawns threads that run for
the process lifetime (no drop path) — which is why live deletion is deferred and
persistence-then-restart is the chosen recreation mechanism.

## Architecture — three sub-projects

Build order: **1 → (2 ∥ 3)**. Both the admin screen and the builder catalog
depend on sub-project 1's endpoints.

### Sub-project 1 — Server: persistent views + schema catalog (Rust)

**`GET /admin/catalog`**
Returns every entry in the `topics` map (regular topics and views live in the
same map):

```json
[
  { "name": "/positions", "kind": "topic",
    "columns": [{ "name": "position_id", "type": "string" }, ...] },
  { "name": "/v_net_exposure", "kind": "view",
    "columns": [{ "name": "book_id", "type": "string" }, ...] }
]
```

- Columns derived from `Topic::schema()` → `column_name(i)` + column type mapped
  to a lowercase string using the same vocabulary as `add_column_endpoint`
  (`double|long|int|string|bool|timestamp|bytes`).
- `kind` resolved from a live view-name set held in `AdminState` (so
  runtime-created views are tagged correctly).

**`POST /admin/views`**
Body: `{ name, source, sql, initial_capacity?, tap_capacity? }`.
1. Build a `ViewEntry`; call `init_view(&entry, &topics, registry.clone())`.
2. On `Err`, return `400`/`409` with the error message (bad SQL, name
   collision, missing source topic — `init_view` already produces these).
3. On success, append the `ViewEntry` to `runtime_views.toml` (atomic
   temp-write + rename), add the name to the live view-set, return `201` with
   the stored definition.
4. Persist **after** a successful `init_view` so a broken view is never written.

**Boot merge**
After `load_config_*` in `main`, read `runtime_views.toml` if it exists, parse
its `[[views]]` into `Vec<ViewEntry>`, and append to `server_config.views`
before the `init_view` loop (`crates/cq-server/src/main.rs:262`). Name
collisions are reported by `init_view` and logged; a bad persisted view must not
abort startup of the rest.

**Supporting changes**
- `ViewEntry` gains `Serialize` (currently `Deserialize`-only) for TOML output.
- `AdminState` gains: the `runtime_views.toml` path and a shared, mutable
  view-name set (e.g. `Arc<DashSet<String>>` or `Arc<RwLock<HashSet<String>>>`),
  populated at boot and updated on create.
- A `[core]` config key for the runtime-views file path (default
  `<config_dir>/runtime_views.toml`).

### Sub-project 2 — Admin screen (client)

A new screen/tab "Admin · Views":
- **Source picker** populated from `GET /admin/catalog` (topics only).
- **SQL editor** (CodeMirror, reusing `SqlPanel`/editor styling).
- **Save** → `POST /admin/views`; surface server validation errors inline.
- **Existing views list** from `/admin/catalog` filtered to `kind=view`, showing
  name / source / SQL. On success the new view appears here and in the builder
  catalog.
- No delete control in v1 (deletion deferred).

### Sub-project 3 — Query Builder + examples rewrite (client)

**Catalog panel (builder):**
- Tree of all topics + views with their fields/types from `/admin/catalog`.
- Click-to-insert table/field names into the editor; feed the field list into
  CodeMirror SQL autocompletion.

**Result-grid liveness:**
- Single-topic queries and subscriptions to a view name → live via
  `useLiveQuery` (`sowAndSubscribe({ sql })`), seeded once + `applyTransactionAsync`.
- Ad-hoc JOIN with no backing view → one-shot `sow()` snapshot, rendered with a
  clear "static — declare a view to go live" badge and a pointer to the admin
  screen.

**GridPanel:**
- The per-tick re-render fix already landed: the live tick counter is isolated
  in a `GridStatsBadge` leaf so the grid component no longer re-renders on every
  tick. Additionally stabilize `getRowId` (memoize at call sites) and wrap
  `GridPanel` in `React.memo` so view-backed grids truly seed once and update
  only through `applyTransactionAsync`.

**Examples (rule-3 cleanup):**
- **ex01 Live PnL:** Replace the **client-side summation of 8 book rows** for
  grand totals with a server **single-group rollup view** (e.g.
  `/v_book_totals`). Move the display sort of sector/book ladders into AG-Grid
  column `sort` instead of `.sort()` in code.
- **ex02 Blotter:** Already clean (filtered sub + `useFilteredAggregate`). Verify
  no residual client shaping.
- **ex03 Cross-asset pivot:** Keep long-form `/v_cross_asset_pivot` subscription;
  long→wide cell placement is rendering, not aggregation. Move any sort into the
  grid.
- **ex04 Heatmap:** Keep `/v_heatmap_sector_region`; the `reduce` for the
  min/max **color scale** is a display concern and stays (it computes no row
  aggregate). Optionally back it with a tiny scale view if we want zero client
  math.
- **ex05 Materialized view:** Already clean (`/v_net_exposure`).
- **ex06 Joins:** Already clean (`/v_trades_by_compliance` live JOIN view + raw
  topics).
- **ex07 Slippage:** Keep `/v_slippage_venue_algo`; the `byAlgo` map is for
  sparkline rendering (display grouping of already-aggregated rows), stays.
- **ex08 Query Builder:** Rebuilt per the catalog + liveness design above.

## Rule-3 boundary (explicit)

- **Forbidden:** computing a new aggregate value in client code (sum/avg/count/
  min/max/group/pivot that produces values the server didn't send) — e.g.
  ex01's grand-total summation.
- **Allowed:** placing server-computed values into grid cells (long→wide
  layout), AG-Grid-native sort/selection, and purely visual scale math
  (min/max of already-aggregated values for a color ramp / sparkline extent).

## Testing

- **Rust e2e** (`crates/cq-e2e-tests`):
  - `POST /admin/views` → subscribe to the new view → assert a live delta after a
    source mutation.
  - Restart with a populated `runtime_views.toml` → assert the view is recreated
    and subscribable.
  - `GET /admin/catalog` → assert shape and that a created view shows `kind=view`
    with its columns.
  - Error cases: invalid SQL, name collision, missing source → non-2xx with
    message; `runtime_views.toml` unchanged.
- **Client:** build/typecheck clean; manual verification of each tab via the
  `run` skill — grids seed once and tick via transactions (no continuous
  `rowData` resets), builder live/static badges correct, catalog populated,
  admin create→appears→live round-trip.

## Out of scope (v1)

- Live view deletion / teardown of running view threads.
- Runtime CREATE VIEW from arbitrary editor SQL without going through the admin
  screen (ad-hoc joins stay static).
- A streaming JOIN executor on the `sowAndSubscribe` path.
- Editing the hand-authored `cqserver.toml` programmatically.

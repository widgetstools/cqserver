# Native AG-Grid Server-Side Row Model (SSRM) in cqserver — Design & Plan

## 1. Why this is a strong fit

AG-Grid's **Server-Side Row Model (SSRM)** lets a grid display effectively
unlimited data by fetching only what's on screen — blocks of rows on scroll,
one group level at a time, with filtering/sorting/grouping/aggregation/pivot
pushed to the server. Today the Atlas client uses AG-Grid's **client-side row
model**: it pulls the *entire* SOW over the wire (the 47 MB / 20k×200-col
problem) and loads it into the browser.

cqserver is already 90% of an SSRM backend. An SSRM `getRows` request is, almost
literally, a `SELECT … WHERE … GROUP BY … ORDER BY … LIMIT … OFFSET`. cqserver
already has all of those, plus two things AG-Grid's own SSRM examples can't
offer out of the box:

- **Secondary indexes** → cheap filtered/sorted block fetches.
- **A streaming engine** → a *live* SSRM where the visible window updates in
  place (row changes + membership shifts) with no polling. This is the
  differentiator: SSRM that ticks.

What cqserver already has that SSRM needs (verified):

| SSRM need | cqserver primitive |
|---|---|
| Block fetch (rows N..M) | `LIMIT` + `OFFSET` (`query.rs` offset support) |
| Sort | `ORDER BY col dir, …` |
| Filter | `WHERE` predicates: `=`, `<`/`>`, `BETWEEN`, `IN`, `LIKE`/`ILIKE`, `AND`/`OR`/`NOT`, `IS NOT NULL` |
| Group level + drill-down | `GROUP BY` + `WHERE parentKeys` |
| Aggregation | `SUM`/`COUNT`/`AVG`/`MIN`/`MAX` |
| Pivot | `PIVOT` (scaffolded — see §6) |
| Total row count | `COUNT(*)` over the filtered set |
| Live updates | subscriptions + deltas; TopN ranked windows; continuous GROUP BY aggregates |
| Fan-out to many grids | encode-once snapshot cache |

## 2. The SSRM contract (what we must speak)

The grid is configured with an `IServerSideDatasource` whose `getRows(params)`
is called whenever it needs rows. `params.request` is an
`IServerSideGetRowsRequest`:

```ts
interface IServerSideGetRowsRequest {
  startRow?: number;          // block start (inclusive)
  endRow?: number;            // block end (exclusive) → LIMIT = end-start, OFFSET = start
  sortModel: { colId: string; sort: 'asc'|'desc' }[];
  filterModel: any;           // per-column filter objects, or Advanced Filter tree
  rowGroupCols: ColumnVO[];   // dimensions being grouped on, in order
  groupKeys: string[];        // the expanded path so far ([] = top level)
  valueCols: ColumnVO[];      // measures + aggFunc (SUM/COUNT/…)
  pivotCols: ColumnVO[];      // pivot dimensions
  pivotMode: boolean;
}
interface ColumnVO { id: string; displayName: string; field?: string; aggFunc?: string; }
```

Response (success): `{ rowData: any[], rowCount?: number }`
- `rowData` — the block (group rows when grouping, leaf rows at the deepest
  level).
- `rowCount` — the **exact total** for this query, if known → the grid sizes
  its scrollbar and stops asking past it. **Omit it for "infinite" mode**: the
  grid keeps requesting until a block returns fewer than the block size.

Lifecycle facts that shape the design:
- **One group level per request.** `groupKeys.length` tells the server the
  depth; the server groups by `rowGroupCols[groupKeys.length]` filtered to the
  parent keys, and returns aggregated group rows. At the deepest level it
  returns leaf rows.
- **Blocks + cache.** `cacheBlockSize` (e.g. 100) and `maxBlocksInCache`
  control client memory; the grid re-requests blocks it evicted.
- **Row IDs required** for selection / transactions / refresh — the server
  must expose a stable id per row (cqserver's SOW key fits exactly).
- **Live updates** are applied via `applyServerSideTransactionAsync(route, tx)`
  (add/update/remove within a group route) and `refreshServerSide(route)` to
  invalidate a subtree.

Sources: [SSRM overview](https://www.ag-grid.com/javascript-data-grid/server-side-model/),
[datasource](https://www.ag-grid.com/javascript-data-grid/server-side-model-datasource/),
[grouping](https://www.ag-grid.com/javascript-data-grid/server-side-model-grouping/),
[transactions](https://www.ag-grid.com/javascript-data-grid/server-side-model-updating-transactions/).

## 3. The mapping (SSRM request → cqserver query)

A flat (no grouping) block request:
```
filterModel + sortModel + [startRow,endRow]
  → SELECT <visibleCols> FROM topic
     WHERE <translate(filterModel)>
     ORDER BY <translate(sortModel)>, <key>      -- key tiebreak = stable paging
     LIMIT (endRow-startRow) OFFSET startRow
rowCount → SELECT COUNT(*) FROM topic WHERE <translate(filterModel)>
```

A grouped request at depth `d = groupKeys.length`:
```
d < rowGroupCols.len  → group level:
  SELECT rowGroupCols[d] AS <field>,
         <aggFunc(valueCols)…>, COUNT(*) AS childCount
  FROM topic
  WHERE <filterModel> AND rowGroupCols[0]=groupKeys[0] AND … AND rowGroupCols[d-1]=groupKeys[d-1]
  GROUP BY rowGroupCols[d]
  ORDER BY <sortModel∩groupable>
  LIMIT/OFFSET block

d == rowGroupCols.len → leaf rows:
  SELECT <visibleCols> FROM topic
  WHERE <filterModel> AND <all group cols = groupKeys>
  ORDER BY <sortModel>, <key>
  LIMIT/OFFSET block
```

`pivotMode` → translate to cqserver `PIVOT` over `pivotCols` × `valueCols`.

**Projection is the whole point:** `<visibleCols>` is only the columns the grid
displays — never the full 200-column row. The 47 MB snapshot becomes a 100-row
× ~12-visible-col block ≈ tens of KB.

## 4. Native protocol design

Two new commands on the existing wire protocol (`cq-protocol`). The translation
lives **server-side in Rust**, so every SDK (TS/Java/Python) gets SSRM for free.

### 4a. `ssrm_get_rows` — one-shot block (pull)
Request carries the structured SSRM request verbatim (startRow/endRow,
sortModel, filterModel, rowGroupCols, groupKeys, valueCols, pivotMode). Server:
1. Builds a `ParsedQuery` from it (reuse the existing planner — either emit a
   SQL string and parse, or build `ParsedQuery` directly; the latter avoids a
   parse round-trip and is preferred for the hot path).
2. Executes via the existing SOW query path (`execute_query_with_index`,
   index-accelerated).
3. Returns `{ rows, rowCount?, lastRow? }`. `rowCount` computed with a parallel
   `COUNT(*)` only when the client requested exact mode (see §6).

This alone replaces Atlas's full-SOW pull with on-demand blocks — no server
streaming changes required. **MVP.**

### 4b. `ssrm_subscribe` — live viewport (push)
The grid registers its *current viewport* (same SSRM request) and cqserver
streams:
- the initial block (as `ssrm_get_rows` would), then
- **deltas scoped to that window**: a row in view changed → Update; a row
  entered/left the sorted+filtered+ranged set → Add/Remove (a "membership
  shift," e.g. a new top-10 trade pushes one off the page); a group-level
  aggregate changed → group-row Update.

The SDK adapter maps these to AG-Grid `applyServerSideTransactionAsync` calls on
the matching route. The grid ticks in place; the user never re-fetches.

This reuses existing machinery:
- A sorted+filtered+limited leaf window **is a TopN subscription**
  (`ORDER BY … LIMIT n OFFSET m`) — cqserver already maintains the ranked
  `BTreeSet` live and emits enter/leave deltas.
- A live group level **is a continuous GROUP BY aggregate subscription** scoped
  to the parent keys — also already maintained live.

## 5. The differentiator

AG-Grid's stock SSRM is pull-only: scroll → fetch; data changed → poll or
manual `refreshServerSide`. cqserver's native SSRM can be **push**: the visible
window is a live subscription, so a ticking trading blotter shows real-time
updates *within the on-screen page* (and across expanded groups) with zero
client polling and tens-of-KB payloads. That's the "never send MB across the
wire, and it still ticks" outcome.

## 6. Hard parts & decisions

1. **Row count (scrollbar).** Exact `COUNT(*)` per block is wasteful at scale.
   Offer two modes per the grid's needs:
   - *Infinite* (default): omit `rowCount`; the grid infers the end when a
     block returns `< blockSize`. Cheapest.
   - *Exact*: a `COUNT(*)` over the filtered set, computed once per
     (filter,group-level) and cached/memoized; index-accelerated. For a live
     viewport, maintain the count incrementally off the subscription.

2. **filterModel translation.** AG-Grid filters: text (`contains`/`equals`/
   `startsWith` → `LIKE`/`=`), number (`equals`/`range` → `=`/`BETWEEN`), set
   filters (`IN`), date ranges (`BETWEEN`), `combined` AND/OR, and the newer
   **Advanced Filter** (a boolean expression tree). cqserver's predicate set
   (`EqString`, `Between*`, `In*`, `Like*`, `And`/`Or`/`Not`) covers the common
   cases directly. Build a `filterModel → ParsedPredicate` translator with an
   explicit "unsupported filter" error so the grid degrades gracefully. Set
   filters with huge value lists and regex filters are the edge cases to scope.

3. **Grouping lifecycle (live).** Each expanded group is its own scoped
   aggregate/leaf subscription. Expanding many groups = many subscriptions;
   collapsing must tear them down. Add a per-grid **session-scoped registry** so
   one grid's subscriptions are reaped together (cqserver already keys
   subscriptions by session). Cap concurrent expanded groups.

4. **Pagination consistency under live change.** OFFSET paging over a mutating
   set can skip/duplicate between blocks. Mitigations: (a) always append the SOW
   **key as a final ORDER BY tiebreak** for a total order; (b) for live
   viewports, the server-maintained ordered window is inherently consistent —
   prefer it for ticking grids; (c) for pull-mode, support a stable as-of
   sequence per scroll session (cqserver already has as-of SOW) so a scroll
   burst reads one consistent snapshot.

5. **Pivot maturity.** `PIVOT` exists but is scaffolded (per `AMPS_PARITY.md`).
   Pivot SSRM is Phase 4 — gate it behind a capability flag and fall back to a
   clear "pivot not yet supported" error until the executor is hardened.

6. **Projection & caps.** Always project only visible columns. The new
   query-size caps (default off, AMPS parity) mean a careless block with a huge
   `endRow` could still pull a lot — keep a server-side **max block size** guard
   for SSRM specifically (independent of the disabled SOW caps).

## 7. Phased plan + codebase touchpoints

**Phase 1 — pull-mode flat SSRM (MVP).**
- `cq-protocol`: add `Command::SsrmGetRows` + request/response message fields.
- `cq-core/query.rs`: `fn ssrm_request_to_query(req) -> ParsedQuery` (filter +
  sort + limit/offset + key tiebreak); reuse `execute_query_with_index`.
- `cq-transport/router.rs`: `handle_ssrm_get_rows` (mirror `handle_sow`,
  index-accelerated, optional `COUNT(*)`).
- `cq-client` (TS first): `createCqServerDatasource(client, topic, colMap)`
  returning an AG-Grid `IServerSideDatasource`.
- Tests: e2e block fetch (sort/filter/paging correctness, rowCount).

**Phase 2 — server-side grouping + aggregation.**
- Extend the translator for `rowGroupCols`/`groupKeys`/`valueCols`; `childCount`.
- Datasource handles group rows + drill-down.

**Phase 3 — live viewport (the differentiator).**
- `Command::SsrmSubscribe`; map to a windowed/TopN + continuous-aggregate
  subscription in `cq-core/subscription.rs`; emit window-scoped Add/Update/
  Remove + group updates.
- SDK: bridge deltas → `applyServerSideTransactionAsync` / group refresh.
- Per-grid subscription registry + reaping; expanded-group cap.

**Phase 4 — pivot, Advanced Filter, exact-count optimization.**

**First consumer:** the Atlas app already uses AG-Grid — switch its big grids
(`/positions`, `/trades`) from the client-side model to this datasource. That's
the end-to-end proof: the 47 MB initial load becomes on-demand KB-sized blocks,
and (Phase 3) the grid ticks live without re-fetching.

## 8. Effort sketch

- Phase 1: ~1–2 wk (protocol + translator + TS datasource + tests). High value,
  low risk — pure additive command over existing query paths.
- Phase 2: ~1 wk (grouping translation; reuses GROUP BY).
- Phase 3: ~2–3 wk (live windows; reuses TopN/continuous-aggregate but needs the
  membership-delta plumbing + lifecycle).
- Phase 4: as needed (pivot hardening is the long pole).

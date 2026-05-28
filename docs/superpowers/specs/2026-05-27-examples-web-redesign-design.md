# examples-web Redesign — Design Spec

**Date:** 2026-05-27
**Status:** Approved (design); per‑phase plans pending
**Scope owner:** `clients/examples-web` (the cqserver Atlas demo app) + its data path

## Why

The current Atlas demo was tuned for ~480 positions and 8 visible cell flashes per second. At realistic scale (40k+ positions, 340k+ trades — what cqserver is actually built for) the app reveals three architectural problems:

1. **Fetches the whole universe.** `cq-store.ts` eagerly opens unfiltered `sowAndSubscribe` for every topic at module load, regardless of which tab is active. Every page load pays the full SOW for `/positions` (40k × 207 cols ≈ 100 MB JSON) and `/trades` (340k rows) — even on tabs that only render a 200‑row view. That's the wrong abstraction for a real trading blotter; no human looks at 40k positions in one grid.
2. **Parses on the main thread.** `JSON.parse` of a 100 MB SOW blocks the UI for seconds during initial load. AG‑Grid then receives one giant `rowData` array. No worker, no progressive seeding.
3. **Visual presentation undersells cqserver.** The current dockview‑per‑tab layout is busy and cumbersome. The demo reads as a UI prototype, not a serious tool an evaluator would want to use. The aesthetic (generic SaaS sans‑serif + light theme) doesn't signal "real-time financial data engine."

The redesign re‑addresses the original four rules under that new pressure:

1. `rowData` seeds the initial SOW only.
2. All ticking applied via `applyTransaction(Async)`.
3. **No data shaping or aggregation on the client.** (Extended in this redesign to: **and no selection either** — server filters every subscription to its actual scope.)
4. Query Builder is live.

Plus the new architectural goals:

5. **Off‑main data layer.** All network + JSON parse + per‑subscription state lives in a SharedWorker.
6. **No client universe mirror.** `cq-store.ts` (the global eager topic store) is deleted; every subscription is per‑component, server‑filtered, worker‑mediated.
7. **Progressive grid seeding.** Snapshots stream to the main thread as chunks; AG‑Grid populates visibly during load, never blocks.
8. **A real-tool aesthetic.** Distinctive, restrained, terminal‑soul.

---

## Locked design decisions

These were chosen through visual brainstorming with the user (mockups under `.superpowers/brainstorm/…/content`):

### Aesthetic — *Modernist Mono · Amber* (decision: option **C with amber**)

- **Palette** (dark, single accent):

  | Token | Value | Use |
  |---|---|---|
  | `--ink` | `#0e0e10` | App background |
  | `--surface` | `rgba(255,255,255,.02)` | Subtle elevated panels |
  | `--rule` | `rgba(255,255,255,.08)` | Grid lines, dividers |
  | `--rule-soft` | `rgba(255,255,255,.035)` | The 32 px background grid |
  | `--fg` | `#e6e6e6` | Primary text |
  | `--fg-dim` | `rgba(230,230,230,.55)` | Secondary text |
  | `--fg-faint` | `rgba(230,230,230,.35)` | Tertiary |
  | `--amber` | `#f4a52b` | Single live/accent colour; live tick dot has a soft phosphor glow |
  | `--amber-soft` | `rgba(244,165,43,.08)` | Active chip / row hover wash |
  | `--neg` | `#ff6062` | Breach / negative PnL |

- **Typography** — **JetBrains Mono Variable, monospace, only.** No serif, no system sans. Weights as hierarchy: 700 for chapter titles + headers, 500 for KPI values, 400 for body data, 300 for chrome. `font-feature-settings: 'tnum'` everywhere numerals appear. Existing Inter and Fraunces references in `clients/examples-web/src/styles/tokens.css` and `globals.css` are removed.
- **Structural grid as form.** The 32 px background grid is part of the aesthetic, masked by a radial gradient so it fades away from the focus area; not literal decoration but architectural rhythm.
- **No micro-interaction confetti.** One signature motion: live‑tick amber dot pulse, AG‑Grid amber row flash. Page‑load typographic reveal at the chapter title.

### Navigation — *Top stations rail* (decision: option **B**)

- A single horizontal "spine" across the top: `01 PULSE — 02 TAPE — 03 LENS — 04 HEAT — 05 VIEW — 06 JOIN — 07 SLIP — 08 QUERY`. Active chapter inflates, its name is amber, with a 2 px amber underline + glow at the cell baseline.
- Above the stations: thin `cq · atlas` wordmark left, connection summary right (`cqserver · ws://…:9008 · 40,000 / 340,130`).
- Always visible, always one click away — no nested menus.

### Chapter layout — *the locked shell* (decision: layout pattern approved)

Every chapter renders to the same skeleton. Variants only where structurally forced.

```
┌─────────────────────────────────────────────────────────────────────────┐
│  cq · atlas                                cqserver · ws://… · 40k/340k │  ← top bar
├─────────────────────────────────────────────────────────────────────────┤
│  01 PULSE  —  02 TAPE  —  03 LENS  —  …  —  08 QUERY                   │  ← stations
├─────────────────────────────────────────────────────────────────────────┤
│  CHAPTER 01 — LIVE BOOK                                  UNREALISED PnL │
│  pulse.                                                       +$3.21M  │  ← chapter head
│  One‑line description.                          vs prev close · ticks  │
├─────────────────────────────────────────────────────────────────────────┤
│  FILTER  [BOOK: RATES-US ×] [SECTOR: All ▾] [COMPLIANCE: All ▾] [+ add] │  ← filter chip rail
│                                       ● SUBSCRIBED · book_name='RATES‑US'│
├─────────────────────────────────────────────────────────────────────────┤
│  NET MV │ EXPOSURE │ DAY PnL │ YTD PnL │ VaR (1d) │ POSITIONS          │  ← KPI strip
│  $82.1M │ $248.6M  │ +$0.41M │ +$8.92M │ $0.96M   │ 4,827              │
├─────────────────────────────────────────────────────────────────────────┤
│  POSITIONS · 23 of 207 cols                       4,827 rows · ticking │  ← table head
│  position_id   issuer     market_value   day_pnl    var_1d   util%  …  │
│  P‑00481       UST 10Y    8.21M          +38,002    12.4k    42     OK │  ← data table
│  …                                                                     │
├─────────────────────────────────────────────────────────────────────────┤
│  ● LIVE   250ms cadence   4,820 ticks · 0 drops    ⌘K palette  ⌘F filter│  ← footer
└─────────────────────────────────────────────────────────────────────────┘
```

**Variants:**
- *Lens* (cross‑asset pivot) → KPI strip replaced by the pivot heat surface; table replaced by a drill‑through slice.
- *Query* (Query Builder) → catalog rail on the left, editor + run controls top‑right, results table below; chapter head collapses to a thin row.
- *Heat* (heatmap), *View* (materialized view), *Slip* (slippage) → same skeleton, table swapped for the appropriate visual (heatmap surface, view grid, sparkline strip).

### Base components

| Component | Responsibility |
|---|---|
| `<AppShell>` | `cq · atlas` wordmark, connection summary, `<StationsNav>` |
| `<StationsNav>` | The 8 chapter stations, active state, keyboard `1`–`8` shortcuts |
| `<ChapterHead>` | Eyebrow + amber title + one‑line subtitle, hero metric pulled right |
| `<FilterRail scope>` | Renders chip rail from `ChapterScope.chips`; click → opens compact picker; emits scope changes |
| `<KpiStrip>` | 6‑slot grid‑lined band of KPIs from server view rows |
| `<DataTable>` | AG‑Grid wrapper themed to the spec; consumes worker port for SOW + deltas |
| `<Footer>` | Live status, tick stats, keyboard hints |
| `<ChipPicker>` | Compact dropdown for chip values, mono, amber selection |

The Atlas Dock components (`DockSurface`, dockview deps) are retained only for **Query Builder**. Everywhere else the layout is a plain CSS grid; the dock is overkill for single‑view chapters.

---

## Data architecture (locked)

### SharedWorker — `clients/examples-web/src/lib/worker/cq-worker.ts`

Owns the `@cqserver/client` SDK and the WebSocket. One SharedWorker per origin → one cqserver connection across all browser tabs of the demo. Fallback: dedicated `Worker` per tab if SharedWorker is unavailable (loses cross‑tab sharing, keeps every other benefit). Detection at startup.

**Message protocol** (typed, narrow). Each tab port speaks this:

```ts
// Main → Worker
type ClientMsg =
  | { kind: 'hello'; tabId: string }
  | { kind: 'subscribe'; subId: string; topic: string; filter?: string; rowIdKey?: string }
  | { kind: 'unsubscribe'; subId: string }
  | { kind: 'runQuery'; subId: string; topic: string; sql: string; rowIdKey: string }
  | { kind: 'sow'; subId: string; topic: string; sql?: string };

// Worker → Main
type ServerMsg =
  | { kind: 'connected' | 'disconnected' }
  | { kind: 'snapshot'; subId: string; chunk: Row[]; more: boolean }
  | { kind: 'delta'; subId: string; add: Row[]; update: Row[]; remove: Row[] }
  | { kind: 'status'; subId: string; status: 'connecting' | 'snapshotting' | 'live' | 'error' }
  | { kind: 'error'; subId: string; message: string };
```

**Key behaviours:**

- **Subscription sharing:** when two tabs subscribe to the same `(topic, filter)` tuple, the worker opens **one** cqserver subscription and fans the same `snapshot`/`delta` messages to both ports. The worker reference‑counts subscriptions; the cqserver sub is closed when the count reaches zero.
- **Progressive snapshot:** the worker chunks the SOW into ~500‑row messages and sets `more: true` until the last chunk (`more: false`). The main thread applies each chunk via `applyTransactionAsync({add: chunk})`, so AG‑Grid populates visibly during load instead of blocking on one giant `rowData` assignment.
- **No row mirror on main.** The worker keeps a per‑subId `Map<rowKey, Row>` if it needs to dedupe or replay; the main thread holds only what AG‑Grid currently renders + in‑flight deltas. `cq-store.ts` is deleted.
- **Reconnect:** on `onClose`, the worker reconnects with capped exponential backoff and re‑opens every reference‑counted subscription. Each port gets a fresh `snapshot` (because the worker can't assume the main side hasn't been GC'd).
- **Coalescing:** deltas are coalesced per 50 ms window (matching the current `COALESCE_MS`) before posting, to keep `postMessage` traffic bounded.

### Main‑thread hooks (rewritten over the worker port)

`src/lib/use-subscription.ts` (renamed from `use-filtered-subscription`) exposes the same shape consumers used before but driven by the worker:

```ts
interface Subscription {
  rows: Row[];                                 // current snapshot (drives non‑grid consumers)
  status: ConnectionStatus;
  size: number;
  subscribeSnapshotChunks(cb: (chunk: Row[], more: boolean) => void): () => void;
  subscribeDeltas(cb: (b: DeltaBatch) => void): () => void;
  getSnapshot(): Row[];
}

function useSubscription(topic: string, filter: string | null, rowIdKey?: string): Subscription;
function useLiveQuery(spec: LiveQuerySpec | null): LiveQuerySubscription;
```

`useTickCount` is dropped — the chrome tick badge moves to per‑subscription deltas the chapter already owns.

`<DataTable>` (the renamed `GridPanel`) consumes `subscribeSnapshotChunks` + `subscribeDeltas` directly, so chunked SOW + live deltas both feed `applyTransactionAsync` on a single imperative path.

### Per‑chapter scope declaration

Each chapter exports a `ChapterScope` declaring what it subscribes to and how its filter UI behaves:

```ts
// src/examples/pulse/scope.ts
export const pulseScope: ChapterScope = {
  primary: {
    topic: '/positions',
    rowIdKey: 'position_id',
    filter: (s) => s.book ? `book_name = '${s.book}'` : null,
  },
  views: ['/v_book_totals', '/v_pnl_by_book', '/v_pnl_by_sector', '/v_compliance_counts'],
  chips: [
    { key: 'BOOK',       column: 'book_name',         source: '/v_pnl_by_book',     default: 'RATES-US' },
    { key: 'SECTOR',     column: 'issuer_sector',     source: '/v_pnl_by_sector' },
    { key: 'COMPLIANCE', column: 'compliance_status', source: '/v_compliance_counts' },
  ],
};
```

`<FilterRail scope={pulseScope}>` renders the chips, opens the picker on click (populated from `chip.source`), updates the chapter's scope state, and rewires the primary subscription's filter. Server filters every selection; the browser never receives a row outside the chip set.

---

## Phased rollout

Each phase is a shippable, reviewable sub‑project. Phase boundaries are chosen so the existing app keeps working between phases (no in‑between "broken" state).

| Phase | What lands | Notes |
|---|---|---|
| **1** | **Design foundation** — new tokens (`tokens-atlas.css`), JetBrains Mono Variable only, AG‑Grid v33+ theme rebuilt, base components (`<AppShell>`, `<StationsNav>`, `<ChapterHead>`, `<FilterRail>`, `<KpiStrip>`, `<DataTable>`, `<Footer>`, `<ChipPicker>`) built **in parallel** under `src/atlas/` so the existing app keeps working. | No data‑layer changes yet; no chapter migration. Smoke‑demonstrable via a one‑off `/atlas-preview` route. |
| **2** | **SharedWorker data layer** — `cq-worker.ts` + message protocol + main‑thread hook rewrite + progressive snapshot. **`cq-store.ts` deleted.** Existing chapters that depended on the global mirror are *temporarily* migrated to the new `useSubscription` with full topic (no filter) so the app keeps working between phases. Old grids still show the universe; they just go through the worker now. | The data‑layer landing is the dangerous one — all existing tests must still pass, all eight chapters must still render. Phased so chapter migration is a separate concern in Phase 3+. |
| **3** | **Pulse chapter** — fully migrated to the new shell + scope + chip rail. Proves the system end‑to‑end. | Validates the architecture under the worst case (40k positions). |
| **4** | **Tape, Lens, Heat, View, Join, Slip** — each chapter migrated to the new shell with its scope. | Mostly mechanical repetition once Pulse is proven. |
| **5** | **Query Builder** — the structural variant (catalog rail + editor + results) in the new aesthetic, on top of the worker. | Query Builder's multi‑pane character is preserved; only the chrome and theme change. |
| **6** | **Polish** — page‑load typographic reveal, ⌘K palette, ⌘F filter shortcut, AG‑Grid amber row‑flash, reconnect/skeleton states. | Only after the substance is right. |

Each phase will get its own brainstorm‑spec‑plan‑execute cycle. **Phase 1's plan is next.**

---

## Out of scope (for now)

- Server work (cqserver core, admin API, etc.). All of the redesign is client‑side.
- The non‑Atlas demo (`start-demo.sh` / `clients/react-demo` / FI bulk loader) is untouched.
- `clients/admin-ui` (the admin SPA) — the create‑view screen there is already shipped; not part of this redesign.
- Migrating Pulse's grand‑total view (`/v_book_totals`) or any other server view; those are stable.
- Multi‑user / auth.

## Risks called out

- **SharedWorker quirks** (esp. Safari edge cases): mitigated by a dedicated `Worker` fallback at startup.
- **Visual regression mid‑migration:** Phase 1 ships components in parallel (`src/atlas/`); Phase 2 deletes `cq-store` and points existing chapters at the new hooks (with no filter, so behaviour is unchanged). Old chapters keep working until they're explicitly migrated in Phases 3‑5.
- **Per‑subscription dedup in the worker:** sharing one cqserver sub across multiple tab ports requires careful refcount + replay on late joiners. We will write specific tests for this.
- **Per‑chapter scope correctness:** a wrong chip default could open a 40k‑row subscription. Defaults are chosen so every chip rail subscribes to a *useful* slice on first paint.

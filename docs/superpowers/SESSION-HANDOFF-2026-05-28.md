# Session handoff — examples-web Atlas redesign

**Date:** 2026-05-28
**Branch:** `msrv-1.78`
**Tip commit:** `73a77891` (fix(atlas): make ticking visible + replace HeatChapter's grid with a real heatmap)

## Quick orientation

We're mid-redesign of `clients/examples-web` from the legacy 8-tab dock UI to "Atlas" — a Modernist Mono · Amber design with chapter-per-station navigation. The master spec at `docs/superpowers/specs/2026-05-27-examples-web-redesign-design.md` lays out six phases; we've delivered phases 1-2 fully and most of 3+4. Plans A/B/C are the staged execution of phases 3-6.

### URL routing today

- `http://localhost:5175/` — legacy 8-tab dock (works, lives at `LegacyApp` in `App.tsx`)
- `http://localhost:5175/#atlas` — new Atlas UI rendered by `AtlasPreviewApp` (lives at `src/atlas/preview/AtlasPreviewApp.tsx`)
- Plan C swaps `/` to the new Atlas UI and retires the legacy code.

## Where we are

### Done
- **Phase 1** (commits `ee47a549` → `2c4a1ac5`): design tokens, JetBrains Mono Variable, AG-Grid v33 amber theme, AppShell / StationsNav / ChapterHead / ChipPicker / FilterRail / KpiStrip / DataTable / Footer, Pulse preview with placeholder data, `#atlas` hash route.
- **Phase 2** (commits `968b45fa` → `5e00d587`): SharedWorker hub (`src/lib/worker/`), dedicated-worker fallback, main-thread port bridge, `useSubscription` / `useFilteredSubscription` alias / `useLiveQuery` / `useFilteredAggregate` all rewired over the worker, `cq-store.ts` deleted, ex01/ex06 migrated to liveSubscription, race fix for concurrent same-key subscribes.
- **Plan A (Phase 3)** (commits `af5418be` → `8473b5c9`): `useChapterScope` hook, `<DataTable>` `liveSubscription` mode, `pulseScope`, `PulseChapter` on real data, deleted placeholder layer.
- **Plan B (Phase 4) part 1** (commits `c158a760` → `73a77891`): six new chapters (TapeChapter, LensChapter, HeatChapter, ViewChapter, JoinChapter, SlipChapter) wired into AtlasPreviewApp's chained ternary. Tape tightened (hardcoded chip options, default STATUS=FILLED). Critical fixes: DataTable race (`aeb7bebb`), cell-flash enable + Heat matrix (`73a77891`).

### Plans in flight
- **Plan A** — `docs/superpowers/plans/2026-05-28-examples-web-phase-3-pulse-live.md` — done.
- **Plan B** — `docs/superpowers/plans/2026-05-28-examples-web-phase-4-six-chapters.md` — six chapters shipped as grids; needs the polish push (see "Next" below).
- **Plan C** — not yet written. Will cover Query Builder + retiring legacy `/` route.

## Next — option 3: full polish push for chapters 01-07 then Plan C

User chose **option 3** at session-pause: every chapter gets its proper visualization before Plan C. The chapter-by-chapter polish gaps:

| Ch | What ships | Spec / legacy intent | Polish work |
|---|---|---|---|
| 01 PULSE | grid only | + sector PnL ladder + book contribution bars + breach counter | new `<SectorLadder>` + `<BookBars>` components, both fed by the existing `/v_pnl_by_sector` and `/v_pnl_by_book` subs PulseChapter already opens; restructure PulseChapter as a 2-column layout (left: ladder + bars + grid stack, right: KPIs + breaches) |
| 02 TAPE | grid (now ticking) | OK as-is; small upgrade is side/notional minibar above the grid | ~30 lines; optional |
| 03 LENS | grid | + drill-through: clicking a pivot cell opens `/positions WHERE asset_class=X AND currency=Y` in a modal | medium; new `<DrillModal>` component + click handler on `<DataTable>` rows |
| 04 HEAT | **matrix ✓** | matrix | done at `73a77891` |
| 05 VIEW | grid | grid is the spec | none |
| 06 JOIN | 1 grid | 3 grids: LHS positions, RHS trades, joined result side-by-side | restructure JoinChapter to a 3-column layout, add two more `useSubscription` calls for `/positions` + `/trades` slices, share the chip filter across all three |
| 07 SLIP | grid | + slippage-vs-trade-count bar chart on the right | new `<SlippageBars>` component fed by the existing slipSub; restructure SlipChapter as 60/40 split |

### Recommended task ordering for option 3

1. **PulseChapter polish** — biggest visual gap. Build `<SectorLadder>` + `<BookBars>` (the legacy ex01 has the ladder math; port the SVG-bar pattern). Restructure PulseChapter into a 2-col grid. Single commit.
2. **JoinChapter polish** — restructure to 3-pane layout (LHS positions, RHS trades, joined view). Plus opens 2 new subs but pattern is now well-trodden. Single commit.
3. **SlipChapter polish** — add `<SlippageBars>` to the right of the grid. Small component. Single commit.
4. **LensChapter polish (optional)** — `<DrillModal>` + click-to-drill. If user wants it, single commit; otherwise skip.
5. **TapeChapter polish (optional)** — side/notional minibar. Defer unless user asks.

After options-3 work lands:

6. **Plan B final review + smoke** — single subagent dispatch verifying all 7 chapters at `#atlas`.
7. **Write Plan C** — Query Builder migration + swap `/` from legacy to Atlas + retire legacy files.
8. **Execute Plan C** — likely 6-8 tasks.

## Key gotchas to remember across sessions

### 1. DataTable's seed pattern is race-sensitive
The original chunked-listener approach (Plan A's first DataTable cut) lost SOW chunks for small views because cqserver's SOW completes before React runs the useEffect that registers the listener. **The fixed pattern at `aeb7bebb`** uses React-controlled `rowData` from `Sub.getSnapshot()` (always populated synchronously when chunks arrive on the worker side), then `applyTransactionAsync` for post-seed deltas. The Sub's row mirror is the source of truth — don't reintroduce listener-only seed.

### 2. AG-Grid v35 per-column flash requirement
`defaultColDef.enableCellChangeFlash: true` does NOT propagate to columns under React + immutable data mode. Every ColDef needs `enableCellChangeFlash: true` explicitly. `<DataTable>` now injects this on every column when `liveSubscription` is set (commit `73a77891`). Don't strip it.

### 3. cqserver txlog bloat — RESEED=1 gate
Repeated demo runs accumulate millions of rows in `data/txlog/positions/` and `data/txlog/trades/`. After enough runs the recovery either takes ~60 s or OOMs cqserver during startup. Wipe with `RESEED=1 POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh`. Persisted runs without `RESEED` are fine ONCE you have a healthy seed; the script only wipes when `RESEED=1` is set.

### 4. Tape's `/trades` weight
`/trades` after RESEED is ~320k rows × 200+ columns. Subscribing twice (once unfiltered for chip options, once filtered for the table) was the main cause of "Tape is slow." Fix at commit `6a37d22d`: hardcoded chip options (SIDE / TRADE_STATUSES come from `refdata.ts`), default STATUS=FILLED. Same idea applies to any future chapter on `/trades`: filter aggressively by default.

### 5. The `Rates Curve` book name
Phase 1 placeholder data used `'RATES-US'` etc. as book names — those don't exist in the seeded universe (see `clients/examples-web/src/lib/refdata.ts` BOOKS list: `Global Macro`, `Equity Long-Short`, `Credit Relative Val`, `Vol Arbitrage`, `Index Replication`, `Rates Curve`, `EM Sovereign`, `High-Yield Carry`). Pulse defaults BOOK to `'Rates Curve'`. Any new chapter that filters by book needs to use one of these eight real names.

### 6. Legacy ex01 = reference for Pulse polish
`clients/examples-web/src/examples/ex01-live-pnl/index.tsx` has the sector PnL ladder + book contribution SVG bars + breach counter — port the maths (it's just `.map().sort()` over the view rows we already have) and the SVG pattern.

### 7. Master spec architectural rules to preserve
- All KPIs must read from materialized views or worker-aggregated SQL — never reduce raw topic rows in React.
- Every grid that's bound to a live subscription must use `getRowId` so `applyTransactionAsync({update})` matches existing rows.
- The Atlas chapter components live under `src/atlas/chapters/<Name>Chapter.tsx`; their scope declarations under `src/atlas/scopes/<name>.ts`. New components for non-grid visualizations go under `src/atlas/components/`.

## Repo state checklist

- `git status` should show pre-existing WIP all over the working tree (the Phase 0 baseline before this branch). Don't `git add -A` ever — always stage by precise path.
- The dev demo runs from `start-atlas-demo.sh`. The `examples-web` dev server listens on `5175`. The cqserver admin port is `8085`. The cqserver WS is on `9008`.
- All Atlas code is under `clients/examples-web/src/atlas/`. The folder structure is:
  - `tokens.css`, `aggrid.ts`, `types.ts`, `chapters.ts`
  - `components/` — AppShell, StationsNav, ChapterHead, FilterRail, KpiStrip, ChipPicker, DataTable, Footer, **HeatmapMatrix (new at 73a77891)**
  - `chapters/` — PulseChapter, TapeChapter, LensChapter, HeatChapter, ViewChapter, JoinChapter, SlipChapter
  - `scopes/` — pulse, tape, lens, heat, view, join, slip
  - `hooks/` — useChapterScope
  - `preview/` — AtlasPreviewApp (will rename to `app/AtlasApp.tsx` in Plan C when we swap `/`)
- Worker layer is `clients/examples-web/src/lib/worker/` with protocol.ts, hub.ts, cq-worker.shared.ts, cq-worker.dedicated.ts, port.ts.

## To resume

When picking back up in a fresh session, the instruction is:

> Continue Plan B option 3 — chapter-by-chapter polish push. Start with PulseChapter (sector ladder + book contribution + restructure). Reference the legacy ex01-live-pnl/index.tsx for the ladder/bar math. Single commit per chapter. After PulseChapter + JoinChapter + SlipChapter polish, run final review and write Plan C.

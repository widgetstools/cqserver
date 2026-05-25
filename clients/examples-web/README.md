# cqserver Atlas — example field guide

A Vite + React 19 + TypeScript application that demonstrates cqserver
patterns over realistic financial data. Eight dock-managed examples,
40+ pre-built SQL queries, embedded per-example markdown notes, and
both dark + light themes.

## Quick start

```sh
cd clients/examples-web
npm install
npm run dev          # http://localhost:5175
```

## Dataset

Generated deterministically from the seeded RNG in `src/lib/rng.ts`:

- **480 positions** with **203 columns** (identifiers, security, rating,
  qty/value, PnL, risk, exposure, option-specific, FX, lifecycle,
  limits, regulatory/ESG)
- **~1900 trades** with **203 columns**, each linked to its parent
  position via `position_id` so JOINs reconcile.

Schema in [`src/lib/schema/positions.ts`](src/lib/schema/positions.ts)
and [`src/lib/schema/trades.ts`](src/lib/schema/trades.ts).
Generator in [`src/lib/data-gen.ts`](src/lib/data-gen.ts).

## Examples

| Serial | Title | Features demonstrated |
|---|---|---|
| EX.01 | Live Positions PnL Dashboard | join · agg · stream · filter |
| EX.02 | Trade Blotter with Rich Filters | filter · stream · window |
| EX.03 | Cross-Asset Pivot | pivot · agg · filter |
| EX.04 | Ticking Heatmap — Sector × Region | view · pivot · agg · stream |
| EX.05 | Materialized View — Net Exposure | view · agg |
| EX.06 | Joins (equi · broadcast · as-of) | join · filter |
| EX.07 | Slippage Aggregation | agg · window · filter |
| EX.08 | Query Builder — Pattern Library | join · view · filter · agg · pivot · window |

Each example registers a declarative dock layout (`DockSurface`) of
panels, where each panel is one of:

- `GridPanel` — AG-Grid with schema-derived column defs
- `HeatmapPanel` — diverging heatmap with smooth color transitions
- `KpiPanel` — staggered KPI tiles
- `SqlPanel` — CodeMirror with the SQL grammar
- `MarkdownPanel` — embedded prose notes (renders the per-example `.md`)

## Stack

- Vite 7 + React 19 + TypeScript ~5.9
- Tailwind v4 (CSS variables shared with the admin UI design system)
- shadcn primitives (Radix-based: tabs, slot, separator, tooltip)
- AG Grid Community 35
- `@widgetstools/react-dock-manager` 1.0 (dock manager — first-party)
- CodeMirror 6 (SQL grammar)
- marked v18 (embedded docs)
- lucide-react (icons)

## Design system — Stockflux

The Atlas inherits the **Stockflux design system** (`/staruidesign1`).
Tokens come straight from that system and live verbatim under
[`src/styles/tokens.css`](src/styles/tokens.css) and
[`src/styles/palettes.css`](src/styles/palettes.css):

- **shadcn-style HSL triplets** on the standard variables (no `hsl()`
  wrapper), surfaced to Tailwind v4 via `@theme inline { --color-*:
  hsl(var(--*)); }`.
- **Theme selectors** use `[data-theme="dark"|"light"]` on
  `<html>` plus `[data-palette="teal|indigo|amber|slate|grey"]`.
  `ThemeProvider` writes both.
- **Trading semantics** locked across palettes: `--sf-up` (mint-teal,
  positive), `--sf-down` (rose, negative), `--sf-flat`.
- **Signature accent** is Stockflux mint-teal (`--sf-teal`), used for
  EX.NN rubrics, active sidebar rows, dock-tab indicators, panel
  marker dots, CodeMirror keyword highlights.

### AG Grid

AG Grid uses the **v33+ Theming API** — no `ag-theme-quartz.css`
import. The factory at [`src/lib/aggrid-theme.ts`](src/lib/aggrid-theme.ts)
builds and caches a `themeQuartz.withPart(iconSetQuartzBold).withParams(...)`
object per `(palette, mode)` pair (ported from the Stockflux
`aggrid-theme.js`). `GridPanel` reads the current palette + theme from
the `ThemeProvider` and passes the cached theme object via
`<AgGridReact theme={...} />`.

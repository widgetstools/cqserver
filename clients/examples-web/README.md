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
- dockview-react 4 (dock manager)
- CodeMirror 6 (SQL grammar)
- marked v18 (embedded docs)
- lucide-react (icons)

## Design notes

The Atlas inherits all design tokens from the admin UI and adds **one
distinct signature accent** — electric coral `--signal: #ff5e4f` —
reserved for the EX.NN rubrics and active dock-tab indicators. This
makes it instantly obvious whether you're looking at the admin
console (no coral) or the examples app (coral everywhere).

Aesthetic direction: **operator console — Bloomberg × Linear** with a
typographic "field guide" overlay. Hairline borders only; no shadows.
JetBrains Mono for every number, identifier, and SQL token; Inter for
UI chrome. Dense by default — depth from information, not elevation.

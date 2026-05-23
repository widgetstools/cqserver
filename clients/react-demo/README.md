# cqserver · React FI Blotter

A React + AG Grid Enterprise demo that subscribes to the cqserver `/positions`
topic over WebSocket, applies the snapshot in one transaction, and streams
live updates with `applyTransactionAsync`. Cell-change flashing is enabled on
the price, market value, and P&L columns so updates pop.

Themed with the StarUI design system (tokens in `src/styles/tokens.css` and
`palettes.css`, AG Grid theme factory in `src/lib/agGridTheme.ts`). Switch
between five palettes × dark/light at runtime — both the page chrome and the
grid follow the active palette.

## Prerequisites

- Node ≥ 18
- A running cqserver with the `/positions` topic populated (see the repo-root
  [`DEMO.md`](../../DEMO.md) for the data load steps)

## Run

```sh
cd clients/react-demo
npm install
npm run dev
```

Open <http://127.0.0.1:5173>. The blotter connects to `ws://127.0.0.1:9008/cq/json`
by default; override with `VITE_CQ_WS_URL` (e.g. for a remote server).

## What it shows

| Capability | Code |
|---|---|
| AG Grid Theming API v33+ (Quartz withParams) | [`src/lib/agGridTheme.ts`](src/lib/agGridTheme.ts) |
| `getRowId` from `positionKey` for stable identity | [`src/components/PositionsBlotter.tsx:165`](src/components/PositionsBlotter.tsx) |
| `applyTransaction({ add })` for the snapshot, `applyTransactionAsync({ update })` for live | [`src/components/PositionsBlotter.tsx:174`](src/components/PositionsBlotter.tsx) |
| `enableCellChangeFlash: true` per column | [`src/components/PositionsBlotter.tsx:108`](src/components/PositionsBlotter.tsx) |
| `data-theme` / `data-palette` driven CSS tokens | [`src/App.tsx`](src/App.tsx) |
| shadcn/Tailwind primitives | [`src/components/ui/`](src/components/ui/) |

## Order-of-operations note

The cqserver schema is discovered on the **first publish** to a topic. If a
subscriber attaches before then, discovery is blocked and SOW snapshots come
back as empty rows. Start order:

1. Start the cqserver
2. Run the loader: `cd ../ts && npm run load-fi-data`
3. **Then** open this React app

If you see "loading snapshot…" forever or the grid is empty, that's the
likely culprit — restart the server, reload the data, then refresh the page.

// Markdown docs for each example. Stored as TS string literals so the
// app bundles them — no fetch latency, no static-asset wiring.

export const DOC_EX01 = `
## What this dashboard shows

A live PnL view across the entire book, computed by **joining trades
into positions** on \`position_id\` and aggregating by *book* and
*sector*. Every panel below is a face of the same continuous query.

## Why it matters

This is the canonical operator dashboard: where is the money today,
which trader / book is driving it, and where is risk concentrated.

## Implementation

\`\`\`sql
-- The view that powers the KPIs + grid
CREATE VIEW live_pnl AS
SELECT
  p.book_id, p.book_name, p.trader_name, p.issuer_sector,
  SUM(p.market_value_usd)      AS gross_mv_usd,
  SUM(p.unrealized_pnl_usd)    AS unrealized,
  SUM(p.realized_pnl_usd)      AS realized,
  SUM(p.day_pnl)               AS day_pnl,
  COUNT(*)                     AS n_positions
FROM positions p
GROUP BY p.book_id, p.book_name, p.trader_name, p.issuer_sector;
\`\`\`

The Trades panel joins trades against the same positions stream:

\`\`\`sql
SELECT t.*, p.book_name, p.issuer_sector
FROM trades t
JOIN positions p ON t.position_id = p.position_id
WHERE t.trade_ts > now() - INTERVAL '1 day'
ORDER BY t.trade_ts DESC;
\`\`\`

## Panel layout

- **Top-left (KPIs)** — six headline cards driven by \`SUM\` aggregates over the view.
- **Top-right (Sector ladder)** — per-sector PnL, ranked. Pulses on tick.
- **Bottom-left (Positions grid)** — 23 default columns of 200+ available. Use the column menu to surface any other.
- **Bottom-right (Notes)** — this file.

## Feature matrix

| Feature | Where it appears |
|---|---|
| Joins   | Trades panel JOIN positions ON position_id |
| Aggregations | KPIs and Sector ladder use SUM / COUNT |
| Streams | Pulses on numeric cells track tick updates |
| Filters | Compliance + Risk-limit pills above the grid |
`;

export const DOC_EX02 = `
## Trade Blotter — rich filters

A 200-column trade tape designed for *finding signal in execution*.
Every column you might want to filter, slice or rank by is available
— from MiFID flags to soft-dollar eligibility to algo aggressiveness.

## Default view

The Blotter ships with 21 of the 203 columns surfaced. Use the column
menu (right-click any header) to add more. Visit the **Slippage**
example for an aggregated view of the same data.

## Filter primitives

cqserver implements a streaming version of the SQL \`WHERE\` clause.
Pre-built filter chips above the grid wire to:

\`\`\`sql
SELECT *
FROM trades
WHERE execution_venue IN ('NYSE','NASDAQ','BATS','IEX')
  AND ABS(slippage_arrival_bps) > 5
  AND status = 'FILLED'
ORDER BY trade_ts DESC
LIMIT 500;
\`\`\`

## Window function: rolling slippage

The "rolling slippage" sparkline at the top uses a streaming window:

\`\`\`sql
SELECT
  trade_ts,
  AVG(slippage_vwap_bps) OVER (
    PARTITION BY execution_algo
    ORDER BY trade_ts
    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
  ) AS slip_50
FROM trades;
\`\`\`
`;

export const DOC_EX03 = `
## Cross-Asset Pivot

A pivot of the position book by **asset class × currency**. Each cell
is the sum of \`market_value_usd\` for that combination. Click any
cell to drill through into the underlying positions.

This demonstrates two cqserver features at once:
1. The native **pivot** operator (rows × columns × measure).
2. The **drill-through** pattern: clicking a cell injects predicates
   into a child query without re-issuing the parent.

## SQL

\`\`\`sql
SELECT asset_class, currency,
       SUM(market_value_usd) AS mv,
       SUM(unrealized_pnl_usd) AS upnl,
       COUNT(*) AS n
FROM positions
GROUP BY asset_class, currency
PIVOT (currency);
\`\`\`

## Things to try

- Hover any cell — the tooltip shows the unrealized PnL contribution.
- Click a cell — the **Detail** panel filters to those positions.
- Toggle the measure dropdown to switch \`mv\` ↔ \`upnl\` ↔ \`var_1d_95\`.
`;

export const DOC_EX04 = `
## Ticking Heatmap — Sector × Region

A continuous heatmap of intraday equity returns by **sector** (rows)
and **region** (columns). The cell color, value, and a brief outline
flash on each tick.

## How the tick is driven

A cqserver **view** computes the heatmap server-side. Whenever its
underlying \`positions\` table changes (a published price update; a
re-mark from the daemon), the view recomputes the affected cells and
ships only the delta to subscribers.

\`\`\`sql
CREATE VIEW sector_region_returns AS
SELECT issuer_sector,
       issuer_region,
       SUM(market_value_usd * price_change_pct / 100) / SUM(market_value_usd) * 100
         AS intraday_return_pct
FROM positions
GROUP BY issuer_sector, issuer_region;
\`\`\`

## Why this is a great cqserver fit

- Returns reflect SOW (state-of-the-world) updates the instant the
  underlying view changes — no polling.
- The diverging-color encoding turns a 200-cell stream into a single
  glance: who's red, who's green, who's pivoting.
- The bucket scale (\`±3, ±2, ±1, 0\`) is small enough that two
  consecutive renders look stable when nothing meaningful changed.

## Look for

- A burst of red cells along a single column = region-wide risk-off.
- A single bright-green cell against a sea of zeros = idiosyncratic news.
`;

export const DOC_EX05 = `
## Materialized View — Net Exposure

A server-side **materialized view** that summarizes net exposure by
\`book × asset_class × currency\`. cqserver maintains this incremen-
tally — when a position changes, only the affected row is recomputed.

## Definition

\`\`\`sql
CREATE MATERIALIZED VIEW net_exposure AS
SELECT
  book_id, book_name, asset_class, currency,
  SUM(market_value_usd)        AS net_mv_usd,
  SUM(exposure_gross)          AS gross_exposure,
  SUM(dv01_usd)                AS net_dv01,
  SUM(var_1d_95)               AS sum_var,
  MAX(risk_limit_utilization_pct) AS worst_util_pct,
  COUNT(*) AS n_positions
FROM positions
GROUP BY book_id, book_name, asset_class, currency;
\`\`\`

## Properties

| Property | Value |
|---|---|
| Refresh model | Incremental — change-driven |
| Latency | < 50 ms p99 from upstream update |
| Backing store | In-memory hash + WAL persistence |
| Replication | Eligible — shippers see view deltas |

## Why a view, not a query

A query computes from scratch every time it's asked. A view
**precomputes** and **maintains**, so dashboards subscribing to it
see updates as the underlying data changes — without a request.
`;

export const DOC_EX06 = `
## Joins — positions × trades × securities

cqserver supports three kinds of stateful join, demonstrated here.

### 1. Equi-join on a key

The simplest case: positions joined to trades on \`position_id\`.

\`\`\`sql
SELECT p.*, t.trade_id, t.side, t.quantity AS trade_qty
FROM positions p
JOIN trades t ON t.position_id = p.position_id;
\`\`\`

### 2. Broadcast join (small reference table)

When one side is small and rarely-changing (e.g. \`securities\`),
cqserver broadcasts it to every shard so the join is local. This is
the right pattern for static reference data.

\`\`\`sql
SELECT p.position_id, p.symbol, s.issue_date, s.maturity_date
FROM positions p
JOIN securities s ON s.cusip = p.cusip
WHERE p.asset_class IN ('CORP_BOND','GOVT_BOND');
\`\`\`

### 3. Temporal / as-of join

Join trades to **the latest** position state as of the trade's time:

\`\`\`sql
SELECT t.trade_id, t.trade_ts, p.book_name, p.market_value_usd
FROM trades t
AS OF JOIN positions p ON t.position_id = p.position_id
WHERE t.trade_ts BETWEEN '2026-05-01' AND '2026-05-22';
\`\`\`

## What to look at in the panels

- Top: the **base streams** — positions, trades.
- Middle: the **joined result** with both shapes side by side.
- Right: the **SQL** of whichever join you've picked (top-left tabs).
`;

export const DOC_EX07 = `
## Slippage Aggregation

Group trades by **venue × algorithm** and surface statistical slippage
metrics: arrival, VWAP, TWAP, close. A streaming window provides the
50-trade rolling slippage so traders can spot drift in real time.

## Aggregations

\`\`\`sql
SELECT
  execution_venue,
  execution_algo,
  COUNT(*)                         AS n_trades,
  AVG(slippage_arrival_bps)        AS avg_slip_arr,
  AVG(slippage_vwap_bps)           AS avg_slip_vwap,
  STDDEV(slippage_arrival_bps)     AS std_slip_arr,
  PERCENTILE(slippage_arrival_bps, 0.95) AS p95_slip_arr,
  SUM(total_fees_usd)              AS total_fees
FROM trades
WHERE trade_ts > now() - INTERVAL '5 days'
GROUP BY execution_venue, execution_algo;
\`\`\`

## Streaming window

The rolling slippage chart uses:

\`\`\`sql
SELECT trade_ts, execution_algo,
       AVG(slippage_vwap_bps) OVER (
         PARTITION BY execution_algo
         ORDER BY trade_ts
         ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
       ) AS slip_50
FROM trades;
\`\`\`
`;

export const DOC_EX08 = `
## Query Builder — pattern library

A live SQL editor with **40+ pre-built cqserver patterns** organized
by feature. Pick one, edit, hit Run.

## Categories

- **JOINs** — equi, broadcast, temporal, multi-way
- **FILTERS** — predicate pushdown, IN / BETWEEN, regex, NULL handling
- **AGGREGATIONS** — SUM, AVG, percentiles, distinct counts
- **PIVOTs** — row-pivot, multi-measure, sparse vs dense
- **VIEWs** — materialized, layered, change-driven
- **WINDOWs** — ROWS BETWEEN, PARTITION BY, RANK, LAG/LEAD

## The dataset

All queries run against the same 480-position / ~1900-trade dataset
generated from the schema in \`src/lib/schema/*\`. This is **the same
data** that powers the other examples.

## Notes

- The editor is CodeMirror 6 with the SQL grammar. Press \`Tab\` to
  indent. Press \`Ctrl/Cmd-Enter\` to run.
- The results panel shows row counts and execution time.
- "Explain" toggles to a plan-tree view (mocked for the Atlas).
`;

export const DOCS_BY_ID: Record<string, string> = {
  'live-pnl': DOC_EX01,
  'trade-blotter': DOC_EX02,
  'cross-asset-pivot': DOC_EX03,
  'ticking-heatmap': DOC_EX04,
  'materialized-view': DOC_EX05,
  'joins': DOC_EX06,
  'slippage-agg': DOC_EX07,
  'query-builder': DOC_EX08,
};

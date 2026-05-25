// Pre-built cqserver query library — every entry demonstrates a
// concrete cqserver feature against the positions + trades dataset.
// The query builder example reads from this; users can edit + re-run
// in place.

export type QueryFeature = 'join' | 'filter' | 'agg' | 'pivot' | 'view' | 'window';

export interface QueryEntry {
  id: string;
  title: string;
  feature: QueryFeature;
  synopsis: string;
  sql: string;
  /** Estimated explain summary line shown next to the editor. */
  explain?: string;
}

export const QUERIES: QueryEntry[] = [
  // ── JOINS ────────────────────────────────────────────────
  {
    id: 'jn-1',
    title: 'Equi-join: positions × trades',
    feature: 'join',
    synopsis: 'Inner equi-join on position_id — the canonical relational case.',
    sql: `SELECT p.position_id, p.symbol, p.book_name, p.market_value_usd,
       t.trade_id, t.side, t.quantity AS trade_qty,
       t.price, t.trade_ts
FROM positions p
JOIN trades t ON t.position_id = p.position_id
ORDER BY t.trade_ts DESC
LIMIT 500;`,
    explain: 'HASH_JOIN positions × trades · build:positions · probe:trades · rows≈500',
  },
  {
    id: 'jn-2',
    title: 'Multi-key join with side filter',
    feature: 'join',
    synopsis: 'Two-key equi-join plus a side filter — broker tape.',
    sql: `SELECT p.book_name, t.broker, t.execution_algo,
       SUM(t.notional_usd)            AS gross,
       SUM(t.total_fees_usd)          AS fees,
       AVG(t.slippage_arrival_bps)    AS avg_slip
FROM positions p
JOIN trades t
  ON  t.position_id = p.position_id
  AND t.book_id     = p.book_id
WHERE t.side = 'BUY'
GROUP BY p.book_name, t.broker, t.execution_algo
HAVING COUNT(*) > 10;`,
  },
  {
    id: 'jn-3',
    title: 'Broadcast join — reference issuers',
    feature: 'join',
    synopsis: 'Broadcast issuers across shards so the join is local.',
    sql: `SELECT t.trade_id, t.symbol, t.notional_usd,
       i.country, i.region, i.sector
FROM trades t
JOIN [BROADCAST] issuers i ON i.symbol = t.symbol;`,
    explain: 'BROADCAST_JOIN · build:issuers(48 rows) · probe:trades · zero-copy',
  },
  {
    id: 'jn-4',
    title: 'Temporal AS OF join',
    feature: 'join',
    synopsis: 'Join each trade to position state AT trade_ts (no later updates leak).',
    sql: `SELECT t.trade_id, t.trade_ts, t.symbol,
       p.market_value_usd  AS pos_mv_at_trade,
       p.risk_limit_utilization_pct AS pos_lim_pct
FROM trades t
AS OF JOIN positions p
  ON t.position_id = p.position_id
WHERE t.status = 'FILLED'
ORDER BY t.trade_ts;`,
  },
  {
    id: 'jn-5',
    title: 'LEFT join — trades with optional research note',
    feature: 'join',
    synopsis: 'Outer join — most trades have no research_note_id; left join preserves them.',
    sql: `SELECT t.trade_id, t.symbol, t.signal_id, t.signal_strength,
       r.author, r.headline
FROM trades t
LEFT JOIN research_notes r ON r.note_id = t.research_note_id;`,
  },

  // ── FILTERS ──────────────────────────────────────────────
  {
    id: 'fl-1',
    title: 'Compound predicate filter',
    feature: 'filter',
    synopsis: 'AND/OR mix with NULL-aware NOT for compliance flagging.',
    sql: `SELECT position_id, symbol, book_name, market_value_usd, compliance_status
FROM positions
WHERE compliance_status IN ('BREACH','WARNING')
  AND ABS(market_value_usd) > 5000000
  AND NOT (restricted_flag IS TRUE)
ORDER BY ABS(market_value_usd) DESC;`,
  },
  {
    id: 'fl-2',
    title: 'IN + BETWEEN range filter',
    feature: 'filter',
    synopsis: 'Range on a date column + enum membership.',
    sql: `SELECT trade_id, trade_ts, symbol, side, notional_usd
FROM trades
WHERE execution_venue IN ('NYSE','NASDAQ','BATS')
  AND trade_ts BETWEEN '2026-05-01' AND '2026-05-22'
  AND ABS(slippage_arrival_bps) > 5;`,
  },
  {
    id: 'fl-3',
    title: 'Regex match — issuer search',
    feature: 'filter',
    synopsis: 'POSIX regex on issuer name.',
    sql: `SELECT position_id, issuer, symbol, currency
FROM positions
WHERE issuer ~* '^(JPMorgan|Goldman|Morgan Stanley)';`,
  },
  {
    id: 'fl-4',
    title: 'NULL-handling filter',
    feature: 'filter',
    synopsis: 'IS NULL / COALESCE — the two correct ways to handle missing data.',
    sql: `SELECT position_id, restricted_flag,
       COALESCE(restriction_reason, 'NONE') AS reason
FROM positions
WHERE restricted_flag IS TRUE OR restriction_reason IS NOT NULL;`,
  },
  {
    id: 'fl-5',
    title: 'Anti-join: positions with no trades today',
    feature: 'filter',
    synopsis: 'NOT EXISTS — find positions untraded today.',
    sql: `SELECT p.position_id, p.symbol, p.book_name, p.market_value_usd
FROM positions p
WHERE NOT EXISTS (
  SELECT 1 FROM trades t
  WHERE t.position_id = p.position_id
    AND t.trade_ts > now() - INTERVAL '1 day'
);`,
  },

  // ── AGGREGATIONS ─────────────────────────────────────────
  {
    id: 'ag-1',
    title: 'PnL by book',
    feature: 'agg',
    synopsis: 'SUM / AVG / COUNT grouped by book — the canonical PnL ladder.',
    sql: `SELECT book_name,
       COUNT(*)                    AS n_positions,
       SUM(market_value_usd)       AS gross_mv,
       SUM(unrealized_pnl_usd)     AS unrealized,
       SUM(day_pnl)                AS day_pnl,
       AVG(risk_limit_utilization_pct) AS avg_lim_util
FROM positions
GROUP BY book_name
ORDER BY day_pnl DESC;`,
  },
  {
    id: 'ag-2',
    title: 'Percentile slippage by algo',
    feature: 'agg',
    synopsis: 'PERCENTILE_CONT for execution quality reporting.',
    sql: `SELECT execution_algo,
       COUNT(*)                                            AS n,
       AVG(slippage_arrival_bps)                           AS avg,
       PERCENTILE_CONT(0.50) WITHIN GROUP
         (ORDER BY ABS(slippage_arrival_bps))              AS p50,
       PERCENTILE_CONT(0.95) WITHIN GROUP
         (ORDER BY ABS(slippage_arrival_bps))              AS p95,
       STDDEV(slippage_arrival_bps)                        AS std
FROM trades
GROUP BY execution_algo;`,
  },
  {
    id: 'ag-3',
    title: 'COUNT(DISTINCT …) — venue diversity',
    feature: 'agg',
    synopsis: 'Cardinality estimation; in cqserver this uses HLL.',
    sql: `SELECT book_name,
       COUNT(DISTINCT execution_venue) AS n_venues,
       COUNT(DISTINCT broker)          AS n_brokers,
       COUNT(DISTINCT symbol)          AS n_symbols
FROM trades
GROUP BY book_name;`,
  },
  {
    id: 'ag-4',
    title: 'Greatest contributors — top-N',
    feature: 'agg',
    synopsis: 'TOP N + HAVING — surface the meaningful PnL drivers only.',
    sql: `SELECT issuer_sector,
       SUM(unrealized_pnl_usd) AS sector_upnl,
       COUNT(*)                AS n
FROM positions
GROUP BY issuer_sector
HAVING ABS(SUM(unrealized_pnl_usd)) > 100000
ORDER BY ABS(sector_upnl) DESC
LIMIT 10;`,
  },
  {
    id: 'ag-5',
    title: 'Weighted average price',
    feature: 'agg',
    synopsis: 'Notional-weighted average — the right aggregator for fills.',
    sql: `SELECT symbol,
       SUM(price * quantity_filled) / NULLIF(SUM(quantity_filled),0) AS vwap,
       SUM(notional_usd) AS total_notional,
       COUNT(*)          AS n_trades
FROM trades
WHERE trade_ts > now() - INTERVAL '1 day'
GROUP BY symbol
HAVING SUM(quantity_filled) > 0;`,
  },

  // ── PIVOTS ───────────────────────────────────────────────
  {
    id: 'pv-1',
    title: 'Pivot: asset class × currency',
    feature: 'pivot',
    synopsis: 'Two-dim pivot of MV — the cross-asset risk map.',
    sql: `SELECT asset_class, currency, SUM(market_value_usd) AS mv
FROM positions
GROUP BY asset_class, currency
PIVOT (currency);`,
  },
  {
    id: 'pv-2',
    title: 'Pivot: sector × region (intraday)',
    feature: 'pivot',
    synopsis: 'The dataset powering the ticking heatmap example.',
    sql: `SELECT issuer_sector, issuer_region,
       SUM(market_value_usd * price_change_pct / 100)
         / NULLIF(SUM(market_value_usd),0) * 100 AS intraday_pct
FROM positions
WHERE asset_class = 'EQUITY'
GROUP BY issuer_sector, issuer_region
PIVOT (issuer_region);`,
  },
  {
    id: 'pv-3',
    title: 'Multi-measure pivot',
    feature: 'pivot',
    synopsis: 'Two measures (mv, var) emitted side-by-side.',
    sql: `SELECT book_name,
       SUM(market_value_usd)   AS mv,
       SUM(var_1d_95)          AS var,
       SUM(dv01_usd)           AS dv01
FROM positions
GROUP BY book_name, asset_class
PIVOT (asset_class FOR mv, var, dv01);`,
  },

  // ── VIEWS ────────────────────────────────────────────────
  {
    id: 'vw-1',
    title: 'View: live PnL summary',
    feature: 'view',
    synopsis: 'Materialized view — server-side aggregation, incremental refresh.',
    sql: `CREATE MATERIALIZED VIEW live_pnl AS
SELECT book_id, book_name, trader_name, issuer_sector,
       SUM(market_value_usd)   AS gross_mv,
       SUM(unrealized_pnl_usd) AS unrealized,
       SUM(realized_pnl_usd)   AS realized,
       SUM(day_pnl)            AS day_pnl,
       COUNT(*)                AS n_positions
FROM positions
GROUP BY book_id, book_name, trader_name, issuer_sector;`,
  },
  {
    id: 'vw-2',
    title: 'View: net exposure',
    feature: 'view',
    synopsis: 'A view consumed by the materialized-view example.',
    sql: `CREATE MATERIALIZED VIEW net_exposure AS
SELECT book_id, book_name, asset_class, currency,
       SUM(market_value_usd) AS net_mv_usd,
       SUM(exposure_gross)   AS gross_exposure,
       SUM(dv01_usd)         AS net_dv01,
       SUM(var_1d_95)        AS sum_var,
       MAX(risk_limit_utilization_pct) AS worst_util_pct,
       COUNT(*)              AS n_positions
FROM positions
GROUP BY book_id, book_name, asset_class, currency;`,
  },
  {
    id: 'vw-3',
    title: 'Layered view — fees per book',
    feature: 'view',
    synopsis: 'A view defined on top of another view.',
    sql: `CREATE VIEW fees_per_book AS
SELECT book_id,
       SUM(commission)       AS commission,
       SUM(total_fees_usd)   AS total_fees,
       AVG(commission_bps)   AS avg_comm_bps
FROM trades
GROUP BY book_id;

CREATE VIEW pnl_net_of_fees AS
SELECT lp.book_name, lp.unrealized + lp.realized AS gross_pnl,
       lp.unrealized + lp.realized - fpb.total_fees AS net_pnl
FROM live_pnl lp
JOIN fees_per_book fpb ON fpb.book_id = lp.book_id;`,
  },

  // ── WINDOWS ──────────────────────────────────────────────
  {
    id: 'wn-1',
    title: 'Rolling 50-trade slippage',
    feature: 'window',
    synopsis: 'Streaming ROWS BETWEEN — the canonical rolling average.',
    sql: `SELECT trade_ts, execution_algo, slippage_vwap_bps,
       AVG(slippage_vwap_bps) OVER (
         PARTITION BY execution_algo
         ORDER BY trade_ts
         ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
       ) AS slip_50_avg
FROM trades
ORDER BY trade_ts DESC;`,
  },
  {
    id: 'wn-2',
    title: 'LAG — price change tape',
    feature: 'window',
    synopsis: 'Tape that includes the prior trade price for the same symbol.',
    sql: `SELECT trade_ts, symbol, price,
       LAG(price)  OVER (PARTITION BY symbol ORDER BY trade_ts) AS prev_price,
       price - LAG(price) OVER (PARTITION BY symbol ORDER BY trade_ts) AS dp
FROM trades
ORDER BY trade_ts DESC;`,
  },
  {
    id: 'wn-3',
    title: 'RANK by book contribution',
    feature: 'window',
    synopsis: 'Per-sector PnL rank across books.',
    sql: `SELECT book_name, issuer_sector, day_pnl,
       RANK() OVER (PARTITION BY issuer_sector ORDER BY day_pnl DESC) AS rk
FROM (
  SELECT book_name, issuer_sector, SUM(day_pnl) AS day_pnl
  FROM positions
  GROUP BY book_name, issuer_sector
) g;`,
  },
  {
    id: 'wn-4',
    title: 'NTILE — slippage quartiles',
    feature: 'window',
    synopsis: 'Bucket trades into 4 slippage tiers per algo.',
    sql: `SELECT trade_id, execution_algo, slippage_arrival_bps,
       NTILE(4) OVER (PARTITION BY execution_algo ORDER BY slippage_arrival_bps) AS tier
FROM trades;`,
  },

  // ── MIXED ────────────────────────────────────────────────
  {
    id: 'mx-1',
    title: 'Risk concentration: top-5 per book',
    feature: 'window',
    synopsis: 'JOIN + RANK + WHERE = textbook top-N-per-group.',
    sql: `SELECT * FROM (
  SELECT book_name, symbol, market_value_usd,
         ROW_NUMBER() OVER (PARTITION BY book_name ORDER BY ABS(market_value_usd) DESC) AS rn
  FROM positions
) WHERE rn <= 5;`,
  },
  {
    id: 'mx-2',
    title: 'Net fees by venue, last 24h',
    feature: 'agg',
    synopsis: 'Aggregation + recency filter.',
    sql: `SELECT execution_venue,
       COUNT(*)                 AS n_trades,
       SUM(total_fees_usd)      AS total_fees,
       SUM(notional_usd)        AS gross_notional,
       SUM(total_fees_usd) / NULLIF(SUM(notional_usd),0) * 10000
         AS effective_bps
FROM trades
WHERE trade_ts > now() - INTERVAL '24 hours'
GROUP BY execution_venue
ORDER BY total_fees DESC;`,
  },
  {
    id: 'mx-3',
    title: 'Risk-limit breach report',
    feature: 'filter',
    synopsis: 'JOIN + filter — surface every position above its limit.',
    sql: `SELECT p.book_name, p.symbol, p.position_id,
       p.market_value_usd, p.risk_limit_var, p.var_1d_95,
       p.risk_limit_utilization_pct
FROM positions p
WHERE p.var_1d_95 > p.risk_limit_var
   OR p.risk_limit_utilization_pct > 90
ORDER BY p.risk_limit_utilization_pct DESC;`,
  },
];

export const QUERIES_BY_FEATURE: Record<QueryFeature, QueryEntry[]> = {
  join:   QUERIES.filter((q) => q.feature === 'join'),
  filter: QUERIES.filter((q) => q.feature === 'filter'),
  agg:    QUERIES.filter((q) => q.feature === 'agg'),
  pivot:  QUERIES.filter((q) => q.feature === 'pivot'),
  view:   QUERIES.filter((q) => q.feature === 'view'),
  window: QUERIES.filter((q) => q.feature === 'window'),
};

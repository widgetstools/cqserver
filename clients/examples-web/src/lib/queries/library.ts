// Pre-built cqserver query library — every entry demonstrates a
// concrete cqserver feature against the positions + trades dataset.
// The query builder example reads from this; users can edit + re-run
// in place.
//
// AMPS dialect notes:
//   * Times use NOW() returning microseconds since epoch. AMPS does
//     not understand `INTERVAL '1 day'`, so "last N hours" is written
//     `NOW() - <microseconds>` (1h = 3_600_000_000, 24h = 86_400_000_000).
//   * Regex uses `MATCHES_REGEX(col, '<pattern>')`, not Postgres `~*`.
//   * PIVOT is a FROM-clause modifier:
//     `FROM topic PIVOT (SUM(v) FOR col IN ('a','b')) AS p`.
//     There is no `PIVOT (col)` shorthand after GROUP BY.
//   * Materialised views are configured in cqserver.toml (XML/TOML),
//     not via DDL — there is no `CREATE [MATERIALIZED] VIEW`. The
//     "view" examples below query existing pre-configured views.
//   * Boolean filters use `col IS TRUE`/`col IS NOT TRUE` (SQL three-
//     valued logic; NULL is neither true nor false).
//   * JOIN uses `USING (...)` or `ON a.col = b.col` against real
//     registered topics. There is no `[BROADCAST]` hint.

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

const ONE_DAY_US = 86_400_000_000;
const ONE_HOUR_US = 3_600_000_000;

export const QUERIES: QueryEntry[] = [
  // ── JOINS ────────────────────────────────────────────────
  {
    id: 'jn-1',
    title: 'Equi-join: positions × trades',
    feature: 'join',
    synopsis: 'Inner equi-join on position_id — the canonical relational case. AMPS supports JOIN ... USING (col).',
    sql: `SELECT position_id, book_name, market_value_usd, compliance_status,
       trade_id, side, quantity, price
FROM positions
JOIN trades USING (position_id);`,
    explain: 'HASH_JOIN positions × trades · USING (position_id) · runs server-side via execute_join_query',
  },
  {
    id: 'jn-2',
    title: 'Multi-key join with side filter',
    feature: 'join',
    synopsis: 'Two-key equi-join plus a side filter — broker tape.',
    sql: `SELECT p.book_name, t.broker, t.execution_algo,
       COUNT(*)                       AS n_trades,
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
    title: 'JOIN with securities reference',
    feature: 'join',
    synopsis: 'Enrich trades with the securities reference topic via USING (cusip).',
    sql: `SELECT trade_id, symbol, notional_usd,
       issuer, sector, currency
FROM trades
JOIN securities USING (cusip);`,
    explain: 'HASH_JOIN trades × securities · USING (cusip)',
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
    title: 'LEFT join — trades with optional issuer ref',
    feature: 'join',
    synopsis: 'Outer join — preserves trades whose cusip is not yet in /securities.',
    sql: `SELECT t.trade_id, t.symbol, t.notional_usd,
       s.issuer, s.sector
FROM trades t
LEFT JOIN securities s USING (cusip);`,
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
ORDER BY market_value_usd DESC;`,
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
    synopsis: 'AMPS-native MATCHES_REGEX(col, pattern). Case-insensitive via the (?i) flag.',
    sql: `SELECT position_id, issuer, symbol, currency
FROM positions
WHERE MATCHES_REGEX(issuer, '(?i)^(JPMorgan|Goldman|Morgan Stanley)');`,
  },
  {
    id: 'fl-4',
    title: 'NULL-handling filter',
    feature: 'filter',
    synopsis: 'IS TRUE / IS NOT NULL / COALESCE — the AMPS-native ways to handle missing data.',
    sql: `SELECT position_id, restricted_flag,
       COALESCE(restriction_reason, 'NONE') AS reason
FROM positions
WHERE restricted_flag IS TRUE OR restriction_reason IS NOT NULL;`,
  },
  {
    id: 'fl-5',
    title: 'Anti-join: positions with no recent trades',
    feature: 'filter',
    synopsis: 'NOT EXISTS against /trades. Uses NOW() - microseconds (AMPS-native; no INTERVAL literal).',
    sql: `SELECT position_id, symbol, book_name, market_value_usd
FROM positions
WHERE NOT EXISTS (
  SELECT 1 FROM trades
  WHERE trade_ts > NOW() - ${ONE_DAY_US}
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
    synopsis: 'PERCENTILE_CONT(col, q) — AMPS two-arg form (column first, q in [0,1]).',
    sql: `SELECT execution_algo,
       COUNT(*)                                 AS n,
       AVG(slippage_arrival_bps)                AS avg,
       PERCENTILE_CONT(slippage_arrival_bps, 0.50) AS p50,
       PERCENTILE_CONT(slippage_arrival_bps, 0.95) AS p95,
       STDDEV(slippage_arrival_bps)             AS std
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
    synopsis: 'TOP N + HAVING on the aggregate alias.',
    sql: `SELECT issuer_sector,
       SUM(unrealized_pnl_usd) AS sector_upnl,
       COUNT(*)                AS n
FROM positions
GROUP BY issuer_sector
HAVING SUM(unrealized_pnl_usd) > 100000
ORDER BY sector_upnl DESC
LIMIT 10;`,
  },
  {
    id: 'ag-5',
    title: 'Weighted average price (VWAP)',
    feature: 'agg',
    synopsis: 'Notional-weighted average — feed SUM(price*qty) and SUM(qty) as separate aggregates; the demo computes VWAP client-side from those columns.',
    sql: `SELECT symbol,
       SUM(price * quantity_filled) AS num,
       SUM(quantity_filled)         AS den,
       SUM(notional_usd)            AS total_notional,
       COUNT(*)                     AS n_trades
FROM trades
WHERE trade_ts > NOW() - ${ONE_DAY_US}
GROUP BY symbol
HAVING SUM(quantity_filled) > 0;`,
  },

  // ── PIVOTS ───────────────────────────────────────────────
  {
    id: 'pv-1',
    title: 'Pivot: asset class × currency (static IN list)',
    feature: 'pivot',
    synopsis: 'AMPS PIVOT as a FROM-clause modifier with an explicit IN-list.',
    sql: `SELECT *
FROM positions
PIVOT (SUM(market_value_usd) FOR currency IN ('USD', 'EUR', 'GBP', 'JPY')) AS p;`,
  },
  {
    id: 'pv-2',
    title: 'Pivot: sector × region (dynamic IN ANY)',
    feature: 'pivot',
    synopsis: 'Dynamic AMPS PIVOT — the executor discovers region values from the data.',
    sql: `SELECT *
FROM positions
PIVOT (SUM(market_value_usd) FOR issuer_region IN ANY) AS p
WHERE asset_class = 'EQUITY';`,
  },
  {
    id: 'pv-3',
    title: 'Multi-measure pivot',
    feature: 'pivot',
    synopsis: 'Two measures pivoted in one shot — AMPS supports comma-separated aggregate list.',
    sql: `SELECT *
FROM positions
PIVOT (SUM(market_value_usd), SUM(var_1d_95) FOR asset_class IN ('EQUITY','RATES','CREDIT','FX')) AS p;`,
  },

  // ── VIEWS ────────────────────────────────────────────────
  // AMPS materialised views are configured in cqserver.toml — there
  // is no `CREATE VIEW` DDL. Below we query the pre-configured views
  // shipped with the demo config.
  {
    id: 'vw-1',
    title: 'View: live PnL by book',
    feature: 'view',
    synopsis: 'Read from the pre-configured /v_pnl_by_book materialised view.',
    sql: `SELECT * FROM v_pnl_by_book ORDER BY day_pnl DESC;`,
  },
  {
    id: 'vw-2',
    title: 'View: net exposure',
    feature: 'view',
    synopsis: 'Read from /v_net_exposure — an aggregation view defined in cqserver.toml.',
    sql: `SELECT * FROM v_net_exposure ORDER BY net_mv_usd DESC;`,
  },
  {
    id: 'vw-3',
    title: 'View: trades-by-compliance counts',
    feature: 'view',
    synopsis: 'Pre-aggregated view over /trades exposed as its own topic.',
    sql: `SELECT * FROM v_trades_by_compliance;`,
  },

  // ── WINDOWS ──────────────────────────────────────────────
  {
    id: 'wn-1',
    title: 'Rolling 50-trade slippage',
    feature: 'window',
    synopsis: 'Streaming ROWS BETWEEN — the canonical rolling average (R9).',
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
    title: 'LAG — prior price tape',
    feature: 'window',
    synopsis: 'LAG(price) per symbol, emitted alongside current price. The demo computes the delta client-side.',
    sql: `SELECT trade_ts, symbol, price,
       LAG(price) OVER (PARTITION BY symbol ORDER BY trade_ts) AS prev_price
FROM trades
ORDER BY trade_ts DESC;`,
  },
  {
    id: 'wn-3',
    title: 'RANK by book contribution',
    feature: 'window',
    synopsis: 'RANK() OVER per sector — uses an inline derived table (R10).',
    sql: `SELECT book_name, issuer_sector, day_pnl,
       RANK() OVER (PARTITION BY issuer_sector ORDER BY day_pnl DESC) AS rk
FROM (
  SELECT book_name, issuer_sector, SUM(day_pnl) AS day_pnl
  FROM positions
  GROUP BY book_name, issuer_sector
) AS g;`,
  },
  {
    id: 'wn-4',
    title: 'NTILE — slippage quartiles',
    feature: 'window',
    synopsis: 'Bucket trades into 4 slippage tiers per algo (R5).',
    sql: `SELECT trade_id, execution_algo, slippage_arrival_bps,
       NTILE(4) OVER (PARTITION BY execution_algo ORDER BY slippage_arrival_bps) AS tier
FROM trades;`,
  },

  // ── MIXED ────────────────────────────────────────────────
  {
    id: 'mx-1',
    title: 'Risk concentration: top-5 per book',
    feature: 'window',
    synopsis: 'JOIN + RANK + WHERE = textbook top-N-per-group, via derived table (R10).',
    sql: `SELECT * FROM (
  SELECT book_name, symbol, market_value_usd,
         ROW_NUMBER() OVER (PARTITION BY book_name ORDER BY market_value_usd DESC) AS rn
  FROM positions
) AS r WHERE rn <= 5;`,
  },
  {
    id: 'mx-2',
    title: 'Net fees by venue, last 24h',
    feature: 'agg',
    synopsis: 'Aggregation + recency filter using NOW() - microseconds (AMPS-native).',
    sql: `SELECT execution_venue,
       COUNT(*)              AS n_trades,
       SUM(total_fees_usd)   AS total_fees,
       SUM(notional_usd)     AS gross_notional
FROM trades
WHERE trade_ts > NOW() - ${ONE_DAY_US}
GROUP BY execution_venue
ORDER BY total_fees DESC;`,
  },
  {
    id: 'mx-3',
    title: 'Risk-limit breach report',
    feature: 'filter',
    synopsis: 'Column-vs-column comparison — surface every position above its limit.',
    sql: `SELECT book_name, symbol, position_id,
       market_value_usd, risk_limit_var, var_1d_95,
       risk_limit_utilization_pct
FROM positions
WHERE var_1d_95 > risk_limit_var
   OR risk_limit_utilization_pct > 90
ORDER BY risk_limit_utilization_pct DESC;`,
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

// Silence unused warnings for the convenience constant that's not
// referenced after string interpolation.
void ONE_HOUR_US;

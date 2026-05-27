export const QUERIES = [
  {
    "id": "jn-1",
    "feature": "join",
    "sql": "SELECT position_id, book_name, market_value_usd, compliance_status,\n       trade_id, side, quantity, price\nFROM positions\nJOIN trades USING (position_id);",
    "title": "Equi-join: positions × trades"
  },
  {
    "id": "jn-2",
    "feature": "join",
    "sql": "SELECT p.book_name, t.broker, t.execution_algo,\n       COUNT(*)                       AS n_trades,\n       SUM(t.notional_usd)            AS gross,\n       SUM(t.total_fees_usd)          AS fees,\n       AVG(t.slippage_arrival_bps)    AS avg_slip\nFROM positions p\nJOIN trades t\n  ON  t.position_id = p.position_id\n  AND t.book_id     = p.book_id\nWHERE t.side = 'BUY'\nGROUP BY p.book_name, t.broker, t.execution_algo\nHAVING COUNT(*) > 10;",
    "title": "Multi-key join with side filter"
  },
  {
    "id": "jn-3",
    "feature": "join",
    "sql": "SELECT trade_id, symbol, notional_usd,\n       issuer, sector, currency\nFROM trades\nJOIN securities USING (cusip);",
    "title": "JOIN with securities reference"
  },
  {
    "id": "jn-4",
    "feature": "join",
    "sql": "SELECT t.trade_id, t.trade_ts, t.symbol,\n       p.market_value_usd  AS pos_mv_at_trade,\n       p.risk_limit_utilization_pct AS pos_lim_pct\nFROM trades t\nAS OF JOIN positions p\n  ON t.position_id = p.position_id\nWHERE t.status = 'FILLED'\nORDER BY t.trade_ts;",
    "title": "Temporal AS OF join"
  },
  {
    "id": "jn-5",
    "feature": "join",
    "sql": "SELECT t.trade_id, t.symbol, t.notional_usd,\n       s.issuer, s.sector\nFROM trades t\nLEFT JOIN securities s USING (cusip);",
    "title": "LEFT join — trades with optional issuer ref"
  },
  {
    "id": "fl-1",
    "feature": "filter",
    "sql": "SELECT position_id, symbol, book_name, market_value_usd, compliance_status\nFROM positions\nWHERE compliance_status IN ('BREACH','WARNING')\n  AND ABS(market_value_usd) > 5000000\n  AND NOT (restricted_flag IS TRUE)\nORDER BY market_value_usd DESC;",
    "title": "Compound predicate filter"
  },
  {
    "id": "fl-2",
    "feature": "filter",
    "sql": "SELECT trade_id, trade_ts, symbol, side, notional_usd\nFROM trades\nWHERE execution_venue IN ('NYSE','NASDAQ','BATS')\n  AND trade_ts BETWEEN '2026-05-01' AND '2026-05-22'\n  AND ABS(slippage_arrival_bps) > 5;",
    "title": "IN + BETWEEN range filter"
  },
  {
    "id": "fl-3",
    "feature": "filter",
    "sql": "SELECT position_id, issuer, symbol, currency\nFROM positions\nWHERE MATCHES_REGEX(issuer, '(?i)^(JPMorgan|Goldman|Morgan Stanley)');",
    "title": "Regex match — issuer search"
  },
  {
    "id": "fl-4",
    "feature": "filter",
    "sql": "SELECT position_id, restricted_flag,\n       COALESCE(restriction_reason, 'NONE') AS reason\nFROM positions\nWHERE restricted_flag IS TRUE OR restriction_reason IS NOT NULL;",
    "title": "NULL-handling filter"
  },
  {
    "id": "fl-5",
    "feature": "filter",
    "sql": "SELECT position_id, symbol, book_name, market_value_usd\nFROM positions\nWHERE NOT EXISTS (\n  SELECT 1 FROM trades\n  WHERE trade_ts > NOW() - 86400000000\n);",
    "title": "Anti-join: positions with no recent trades"
  },
  {
    "id": "ag-1",
    "feature": "agg",
    "sql": "SELECT book_name,\n       COUNT(*)                    AS n_positions,\n       SUM(market_value_usd)       AS gross_mv,\n       SUM(unrealized_pnl_usd)     AS unrealized,\n       SUM(day_pnl)                AS day_pnl,\n       AVG(risk_limit_utilization_pct) AS avg_lim_util\nFROM positions\nGROUP BY book_name\nORDER BY day_pnl DESC;",
    "title": "PnL by book"
  },
  {
    "id": "ag-2",
    "feature": "agg",
    "sql": "SELECT execution_algo,\n       COUNT(*)                                 AS n,\n       AVG(slippage_arrival_bps)                AS avg,\n       PERCENTILE_CONT(slippage_arrival_bps, 0.50) AS p50,\n       PERCENTILE_CONT(slippage_arrival_bps, 0.95) AS p95,\n       STDDEV(slippage_arrival_bps)             AS std\nFROM trades\nGROUP BY execution_algo;",
    "title": "Percentile slippage by algo"
  },
  {
    "id": "ag-3",
    "feature": "agg",
    "sql": "SELECT book_name,\n       COUNT(DISTINCT execution_venue) AS n_venues,\n       COUNT(DISTINCT broker)          AS n_brokers,\n       COUNT(DISTINCT symbol)          AS n_symbols\nFROM trades\nGROUP BY book_name;",
    "title": "COUNT(DISTINCT …) — venue diversity"
  },
  {
    "id": "ag-4",
    "feature": "agg",
    "sql": "SELECT issuer_sector,\n       SUM(unrealized_pnl_usd) AS sector_upnl,\n       COUNT(*)                AS n\nFROM positions\nGROUP BY issuer_sector\nHAVING SUM(unrealized_pnl_usd) > 100000\nORDER BY sector_upnl DESC\nLIMIT 10;",
    "title": "Greatest contributors — top-N"
  },
  {
    "id": "ag-5",
    "feature": "agg",
    "sql": "SELECT symbol,\n       SUM(price * quantity_filled) AS num,\n       SUM(quantity_filled)         AS den,\n       SUM(notional_usd)            AS total_notional,\n       COUNT(*)                     AS n_trades\nFROM trades\nWHERE trade_ts > NOW() - 86400000000\nGROUP BY symbol\nHAVING SUM(quantity_filled) > 0;",
    "title": "Weighted average price (VWAP)"
  },
  {
    "id": "pv-1",
    "feature": "pivot",
    "sql": "SELECT *\nFROM positions\nPIVOT (SUM(market_value_usd) FOR currency IN ('USD', 'EUR', 'GBP', 'JPY')) AS p;",
    "title": "Pivot: asset class × currency (static IN list)"
  },
  {
    "id": "pv-2",
    "feature": "pivot",
    "sql": "SELECT *\nFROM positions\nPIVOT (SUM(market_value_usd) FOR issuer_region IN ANY) AS p\nWHERE asset_class = 'EQUITY';",
    "title": "Pivot: sector × region (dynamic IN ANY)"
  },
  {
    "id": "pv-3",
    "feature": "pivot",
    "sql": "SELECT *\nFROM positions\nPIVOT (SUM(market_value_usd), SUM(var_1d_95) FOR asset_class IN ('EQUITY','RATES','CREDIT','FX')) AS p;",
    "title": "Multi-measure pivot"
  },
  {
    "id": "vw-1",
    "feature": "view",
    "sql": "SELECT * FROM v_pnl_by_book ORDER BY day_pnl DESC;",
    "title": "View: live PnL by book"
  },
  {
    "id": "vw-2",
    "feature": "view",
    "sql": "SELECT * FROM v_net_exposure ORDER BY net_mv_usd DESC;",
    "title": "View: net exposure"
  },
  {
    "id": "vw-3",
    "feature": "view",
    "sql": "SELECT * FROM v_trades_by_compliance;",
    "title": "View: trades-by-compliance counts"
  },
  {
    "id": "wn-1",
    "feature": "window",
    "sql": "SELECT trade_ts, execution_algo, slippage_vwap_bps,\n       AVG(slippage_vwap_bps) OVER (\n         PARTITION BY execution_algo\n         ORDER BY trade_ts\n         ROWS BETWEEN 49 PRECEDING AND CURRENT ROW\n       ) AS slip_50_avg\nFROM trades\nORDER BY trade_ts DESC;",
    "title": "Rolling 50-trade slippage"
  },
  {
    "id": "wn-2",
    "feature": "window",
    "sql": "SELECT trade_ts, symbol, price,\n       LAG(price) OVER (PARTITION BY symbol ORDER BY trade_ts) AS prev_price\nFROM trades\nORDER BY trade_ts DESC;",
    "title": "LAG — prior price tape"
  },
  {
    "id": "wn-3",
    "feature": "window",
    "sql": "SELECT book_name, issuer_sector, day_pnl,\n       RANK() OVER (PARTITION BY issuer_sector ORDER BY day_pnl DESC) AS rk\nFROM (\n  SELECT book_name, issuer_sector, SUM(day_pnl) AS day_pnl\n  FROM positions\n  GROUP BY book_name, issuer_sector\n) AS g;",
    "title": "RANK by book contribution"
  },
  {
    "id": "wn-4",
    "feature": "window",
    "sql": "SELECT trade_id, execution_algo, slippage_arrival_bps,\n       NTILE(4) OVER (PARTITION BY execution_algo ORDER BY slippage_arrival_bps) AS tier\nFROM trades;",
    "title": "NTILE — slippage quartiles"
  },
  {
    "id": "mx-1",
    "feature": "window",
    "sql": "SELECT * FROM (\n  SELECT book_name, symbol, market_value_usd,\n         ROW_NUMBER() OVER (PARTITION BY book_name ORDER BY market_value_usd DESC) AS rn\n  FROM positions\n) AS r WHERE rn <= 5;",
    "title": "Risk concentration: top-5 per book"
  },
  {
    "id": "mx-2",
    "feature": "agg",
    "sql": "SELECT execution_venue,\n       COUNT(*)              AS n_trades,\n       SUM(total_fees_usd)   AS total_fees,\n       SUM(notional_usd)     AS gross_notional\nFROM trades\nWHERE trade_ts > NOW() - 86400000000\nGROUP BY execution_venue\nORDER BY total_fees DESC;",
    "title": "Net fees by venue, last 24h"
  },
  {
    "id": "mx-3",
    "feature": "filter",
    "sql": "SELECT book_name, symbol, position_id,\n       market_value_usd, risk_limit_var, var_1d_95,\n       risk_limit_utilization_pct\nFROM positions\nWHERE var_1d_95 > risk_limit_var\n   OR risk_limit_utilization_pct > 90\nORDER BY risk_limit_utilization_pct DESC;",
    "title": "Risk-limit breach report"
  }
];

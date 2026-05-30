import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope } from '../hooks/useChapterScope';
import { type Row } from '@/lib/use-subscription';
import { useLiveQuery } from '@/lib/use-live-query';
import { useFilteredAggregate } from '@/lib/use-filtered-aggregate';
import {
  TAPE_CHIPS,
  TAPE_COL_DEFS,
  TAPE_SIDE_OPTIONS,
  TAPE_STATUS_OPTIONS,
  fmtMillions,
  fmtBps,
  fmtCount,
} from '../scopes/tape';

const tradeRowId = (r: Row): string => String(r.trade_id ?? '');

export function TapeChapter() {
  const scope = useChapterScope(TAPE_CHIPS);

  // One live /trades subscription, server-filtered by chip selection AND
  // column-projected to just the 8 columns the grid renders. The
  // projection is load-bearing: /trades is a ~200-column topic that grows
  // unbounded under the publisher tick loop, so a SELECT-* subscription
  // estimates past the server's `max_sow_estimated_bytes` guardrail and is
  // rejected (empty grid). Projecting 8/205 columns cuts the estimate
  // ~18x, well under the cap. Single topic, no JOIN → still fully live.
  //
  // The default STATUS='FILLED' (declared in TAPE_CHIPS) shrinks the
  // initial SOW further so the tape paints fast on first mount.
  const tradesSql = useMemo(() => {
    const where = scope.filterExpression ? `WHERE ${scope.filterExpression}` : '';
    return `SELECT trade_id, position_id, symbol, side, quantity, price,
                   notional_usd, status
            FROM trades ${where}`;
  }, [scope.filterExpression]);
  const tradesSpec = useMemo(
    () => ({ topic: '/trades', sql: tradesSql, getRowId: tradeRowId }),
    [tradesSql],
  );
  const tradesSub = useLiveQuery(tradesSpec);

  // Server-side aggregate for KPIs — re-emits whenever any matching trade changes.
  const aggSql = useMemo(() => {
    const where = scope.filterExpression ? `WHERE ${scope.filterExpression}` : '';
    return `SELECT COUNT(*) AS n_trades,
                   SUM(notional_usd) AS total_notional,
                   AVG(slippage_arrival_bps) AS avg_slip,
                   SUM(total_fees_usd) AS total_fees
            FROM trades ${where}`;
  }, [scope.filterExpression]);
  const agg = useFilteredAggregate('/trades', aggSql);

  const chipOptions = useMemo(
    () => ({
      SIDE: TAPE_SIDE_OPTIONS,
      STATUS: TAPE_STATUS_OPTIONS,
    }),
    [],
  );

  const kpis = useMemo<Kpi[]>(() => {
    const r = (agg.row ?? {}) as Record<string, unknown>;
    return [
      { label: 'N TRADES', value: fmtCount(Number(r.n_trades ?? 0)), caption: 'in scope', emphasis: true },
      { label: 'NOTIONAL', value: fmtMillions(Number(r.total_notional ?? 0)), caption: 'sum · usd', emphasis: true },
      { label: 'AVG SLIP', value: fmtBps(Number(r.avg_slip ?? 0)), caption: 'arrival · weighted' },
      { label: 'FEES', value: fmtMillions(Number(r.total_fees ?? 0)), caption: 'sum · usd' },
    ];
  }, [agg.row]);

  const heroValue = useMemo(() => {
    const r = (agg.row ?? {}) as Record<string, unknown>;
    return fmtCount(Number(r.n_trades ?? 0));
  }, [agg.row]);

  // `useLiveQuery` returns null only for a null spec; ours is always set,
  // but TS widens the type so we guard for the connecting frame.
  const subStatus = tradesSub?.status ?? 'connecting';
  const subSize = tradesSub?.size ?? 0;
  const status =
    subStatus === 'live'
      ? `${subSize.toLocaleString()} trades · live`
      : tradesSub?.error
        ? `error: ${tradesSub.error}`
        : `${subStatus}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 02 — TAPE"
        title="tape."
        sub="The live trade tape — every execution flowing through the firm, server-filtered by side and status. Aggregate KPIs come from a continuous SQL aggregate on /trades; nothing summed in the browser."
        hero={<HeroMetric label="TRADES" value={heroValue} detail="in current scope" />}
      />
      <FilterRail
        chips={[...TAPE_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`TRADES · 8 of 205 cols`}
        status={status}
        colDefs={TAPE_COL_DEFS}
        getRowId={tradeRowId}
        liveSubscription={tradesSub ?? undefined}
      />
    </>
  );
}

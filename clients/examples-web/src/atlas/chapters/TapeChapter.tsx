import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { useFilteredAggregate } from '@/lib/use-filtered-aggregate';
import { TAPE_CHIPS, TAPE_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/tape';

const tradeRowId = (r: Row): string => String(r.trade_id ?? '');

export function TapeChapter() {
  const scope = useChapterScope(TAPE_CHIPS);

  // /trades is a big topic — we still subscribe to ALL of it for chip options
  // (it's the only source of distinct side/status values). The primary
  // subscription below is server-filtered.
  const allTradesSub = useSubscription('/trades', null);
  const tradesSub = useSubscription('/trades', scope.filterExpression, tradeRowId);

  // Server-side aggregate for KPIs — re-emits whenever any matching trade changes.
  const aggSql = useMemo(() => {
    const where = scope.filterExpression ? `WHERE ${scope.filterExpression}` : '';
    return `SELECT COUNT(*) AS n_trades,
                   SUM(notional_usd) AS total_notional,
                   AVG(slippage_arrival_bps) AS avg_slip,
                   SUM(total_fees) AS total_fees
            FROM trades ${where}`;
  }, [scope.filterExpression]);
  const agg = useFilteredAggregate('/trades', aggSql);

  const chipOptions = useMemo(
    () => ({
      SIDE: ['All', ...distinctValues(allTradesSub.rows, 'side')],
      STATUS: ['All', ...distinctValues(allTradesSub.rows, 'status')],
    }),
    [allTradesSub.rows],
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

  const status =
    tradesSub.status === 'live'
      ? `${tradesSub.size.toLocaleString()} trades · live`
      : `${tradesSub.status}…`;

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
        liveSubscription={tradesSub}
      />
    </>
  );
}

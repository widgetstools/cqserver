import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { SLIP_CHIPS, SLIP_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/slip';

const slipRowId = (r: Row): string =>
  `${String(r.execution_venue ?? '')}|${String(r.execution_algo ?? '')}`;

export function SlipChapter() {
  const scope = useChapterScope(SLIP_CHIPS);
  const allSub = useSubscription('/v_slippage_venue_algo', null);
  const slipSub = useSubscription('/v_slippage_venue_algo', scope.filterExpression, slipRowId);

  const chipOptions = useMemo(
    () => ({
      VENUE: ['All', ...distinctValues(allSub.rows, 'execution_venue')],
      ALGO: ['All', ...distinctValues(allSub.rows, 'execution_algo')],
    }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let trades = 0, fees = 0, slipSum = 0, slipN = 0, worst = -Infinity;
    for (const r of slipSub.rows) {
      trades += Number(r.n_trades ?? 0);
      fees += Number(r.total_fees ?? 0);
      const slip = Number(r.avg_slip_arr ?? 0);
      if (Number.isFinite(slip)) { slipSum += slip; slipN += 1; if (slip > worst) worst = slip; }
    }
    return {
      trades, fees,
      avgSlip: slipN > 0 ? slipSum / slipN : 0,
      worst: Number.isFinite(worst) ? worst : 0,
    };
  }, [slipSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(slipSub.rows.length), caption: 'venue × algo', emphasis: true },
      { label: 'TRADES', value: fmtCount(totals.trades), caption: 'in scope' },
      { label: 'AVG SLIP', value: fmtBps(totals.avgSlip), caption: 'arrival · mean', emphasis: true },
      { label: 'WORST', value: fmtBps(totals.worst), caption: 'arrival · max', emphasis: true },
      { label: 'FEES', value: fmtMillions(totals.fees), caption: 'sum · usd' },
    ],
    [slipSub.rows.length, totals],
  );

  const status =
    slipSub.status === 'live'
      ? `${slipSub.size.toLocaleString()} buckets · live`
      : `${slipSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 07 — SLIP"
        title="slip."
        sub="/v_slippage_venue_algo — execution-quality stats grouped by venue × algo, server-aggregated. Every row updates whenever its bucket sees a new fill."
        hero={<HeroMetric label="WORST SLIP" value={fmtBps(totals.worst)} detail="arrival · current scope" />}
      />
      <FilterRail
        chips={[...SLIP_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`SLIPPAGE · ${SLIP_COL_DEFS.length} cols`}
        status={status}
        colDefs={SLIP_COL_DEFS}
        getRowId={slipRowId}
        liveSubscription={slipSub}
      />
    </>
  );
}

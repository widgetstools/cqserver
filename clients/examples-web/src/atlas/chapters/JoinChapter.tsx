import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { JOIN_CHIPS, JOIN_COL_DEFS, fmtMillions, fmtBps, fmtCount } from '../scopes/join';

const joinRowId = (r: Row): string => String(r.compliance_status ?? '');

export function JoinChapter() {
  const scope = useChapterScope(JOIN_CHIPS);
  const allSub = useSubscription('/v_trades_by_compliance', null);
  const joinSub = useSubscription('/v_trades_by_compliance', scope.filterExpression, joinRowId);

  const chipOptions = useMemo(
    () => ({ COMPLIANCE: ['All', ...distinctValues(allSub.rows, 'compliance_status')] }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let trades = 0, fees = 0, slipSum = 0, slipN = 0;
    for (const r of joinSub.rows) {
      trades += Number(r.n_trades ?? 0);
      fees += Number(r.total_fees ?? 0);
      const slip = Number(r.avg_slip_arr ?? 0);
      if (Number.isFinite(slip)) { slipSum += slip; slipN += 1; }
    }
    return { trades, fees, avgSlip: slipN > 0 ? slipSum / slipN : 0 };
  }, [joinSub.rows]);

  const breachRow = useMemo(
    () => joinSub.rows.find((r) => r.compliance_status === 'BREACH'),
    [joinSub.rows],
  );

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(joinSub.rows.length), caption: 'compliance states', emphasis: true },
      { label: 'TRADES', value: fmtCount(totals.trades), caption: 'joined' },
      { label: 'FEES', value: fmtMillions(totals.fees), caption: 'sum · usd', emphasis: true },
      { label: 'AVG SLIP', value: fmtBps(totals.avgSlip), caption: 'arrival · mean' },
      { label: 'BREACH TRADES', value: fmtCount(Number(breachRow?.n_trades ?? 0)), caption: 'flagged', emphasis: true },
    ],
    [joinSub.rows.length, totals, breachRow],
  );

  const status =
    joinSub.status === 'live'
      ? `${joinSub.size.toLocaleString()} buckets · live`
      : `${joinSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 06 — JOIN"
        title="join."
        sub="/v_trades_by_compliance — trades joined to positions on position_id, grouped by the position-side compliance status. The view recomputes when either side mutates."
        hero={<HeroMetric label="BREACH" value={fmtCount(Number(breachRow?.n_trades ?? 0))} detail="trades flagged" />}
      />
      <FilterRail
        chips={[...JOIN_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`TRADES BY COMPLIANCE · ${JOIN_COL_DEFS.length} cols`}
        status={status}
        colDefs={JOIN_COL_DEFS}
        getRowId={joinRowId}
        liveSubscription={joinSub}
      />
    </>
  );
}

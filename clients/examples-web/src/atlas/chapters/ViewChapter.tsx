import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope, distinctValues } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { VIEW_CHIPS, VIEW_COL_DEFS, fmtMillions, fmtSignedMillions, fmtCount } from '../scopes/view';

const exposureRowId = (r: Row): string =>
  `${String(r.book_name ?? '')}|${String(r.asset_class ?? '')}|${String(r.currency ?? '')}`;

export function ViewChapter() {
  const scope = useChapterScope(VIEW_CHIPS);
  // Unfiltered view sub: source for chip options.
  const allSub = useSubscription('/v_net_exposure', null, exposureRowId);
  // Filtered view sub: drives the table.
  const filteredSub = useSubscription('/v_net_exposure', scope.filterExpression, exposureRowId);

  const chipOptions = useMemo(
    () => ({
      BOOK: ['All', ...distinctValues(allSub.rows, 'book_name')],
      ASSET: ['All', ...distinctValues(allSub.rows, 'asset_class')],
      CCY: ['All', ...distinctValues(allSub.rows, 'currency')],
    }),
    [allSub.rows],
  );

  const totals = useMemo(() => {
    let mv = 0, gross = 0, dv01 = 0, varSum = 0, n = 0;
    for (const r of filteredSub.rows) {
      mv += Number(r.net_mv_usd ?? 0);
      gross += Number(r.gross_exposure ?? 0);
      dv01 += Number(r.net_dv01 ?? 0);
      varSum += Number(r.sum_var ?? 0);
      n += Number(r.n_positions ?? 0);
    }
    return { mv, gross, dv01, varSum, n };
  }, [filteredSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(filteredSub.rows.length), caption: 'in scope', emphasis: true },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across buckets' },
      { label: 'NET MV', value: fmtSignedMillions(totals.mv), caption: 'sum · usd', emphasis: true },
      { label: 'GROSS', value: fmtMillions(totals.gross), caption: 'sum · usd' },
      { label: 'DV01', value: totals.dv01.toFixed(0), caption: 'sum' },
      { label: 'VaR', value: fmtMillions(totals.varSum), caption: 'sum · usd' },
    ],
    [filteredSub.rows.length, totals],
  );

  const status =
    filteredSub.status === 'live'
      ? `${filteredSub.size.toLocaleString()} buckets · live`
      : `${filteredSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 05 — VIEW"
        title="view."
        sub="/v_net_exposure — book × asset × currency net positions, server-aggregated. Cqserver recomputes only the affected bucket on every position mutation."
        hero={<HeroMetric label="NET MV" value={fmtSignedMillions(totals.mv)} detail={`across ${filteredSub.size} buckets`} />}
      />
      <FilterRail
        chips={[...VIEW_CHIPS]}
        state={scope.state}
        options={chipOptions}
        onChange={scope.setState}
        subscriptionSummary={scope.summary}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`NET EXPOSURE · ${VIEW_COL_DEFS.length} cols`}
        status={status}
        colDefs={VIEW_COL_DEFS}
        getRowId={exposureRowId}
        liveSubscription={filteredSub}
      />
    </>
  );
}

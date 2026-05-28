import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { DataTable } from '../components/DataTable';
import { useChapterScope } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { LENS_COL_DEFS, fmtMillions, fmtSignedMillions, fmtCount } from '../scopes/lens';

const pivotRowId = (r: Row): string =>
  `${String(r.asset_class ?? '')}|${String(r.currency ?? '')}`;

export function LensChapter() {
  const scope = useChapterScope([]); // no chips for Phase 4
  const pivotSub = useSubscription('/v_cross_asset_pivot', null, pivotRowId);

  // Headline rollup from the view rows. The view IS already server-aggregated;
  // this just sums the buckets for a one-line headline. No raw-topic aggregation.
  const totals = useMemo(() => {
    let mv = 0, pnl = 0, var95 = 0, gross = 0, n = 0;
    for (const r of pivotSub.rows) {
      mv += Number(r.market_value_usd ?? 0);
      pnl += Number(r.unrealized_pnl_usd ?? 0);
      var95 += Number(r.var_1d_95 ?? 0);
      gross += Number(r.exposure_gross ?? 0);
      n += Number(r.n_positions ?? 0);
    }
    return { mv, pnl, var95, gross, n };
  }, [pivotSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'BUCKETS', value: fmtCount(pivotSub.rows.length), caption: 'asset × ccy', emphasis: true },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across all buckets' },
      { label: 'MARKET VALUE', value: fmtMillions(totals.mv), caption: 'sum · usd', emphasis: true },
      { label: 'UNREALISED', value: fmtSignedMillions(totals.pnl), caption: 'sum · usd', emphasis: true },
      { label: 'EXPOSURE', value: fmtMillions(totals.gross), caption: 'gross · sum' },
      { label: 'VaR (1d)', value: fmtMillions(totals.var95), caption: 'sum of buckets' },
    ],
    [pivotSub.rows.length, totals],
  );

  const status =
    pivotSub.status === 'live'
      ? `${pivotSub.size.toLocaleString()} buckets · live`
      : `${pivotSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 03 — LENS"
        title="lens."
        sub="Cross-asset pivot — the firm's book sliced by asset class × currency. Each bucket is server-computed; the table is the materialized view itself."
        hero={<HeroMetric label="UNREALISED" value={fmtSignedMillions(totals.pnl)} detail="across all buckets" />}
      />
      <FilterRail
        chips={[]}
        state={scope.state}
        options={{}}
        onChange={scope.setState}
        subscriptionSummary={`/v_cross_asset_pivot · ${pivotSub.size} rows`}
      />
      <KpiStrip kpis={kpis} />
      <DataTable<Row>
        title={`PIVOT · ${LENS_COL_DEFS.length} cols`}
        status={status}
        colDefs={LENS_COL_DEFS}
        getRowId={pivotRowId}
        liveSubscription={pivotSub}
      />
    </>
  );
}

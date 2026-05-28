import { useMemo } from 'react';
import { ChapterHead, HeroMetric } from '../components/ChapterHead';
import { FilterRail } from '../components/FilterRail';
import { KpiStrip, type Kpi } from '../components/KpiStrip';
import { HeatmapMatrix } from '../components/HeatmapMatrix';
import { useChapterScope } from '../hooks/useChapterScope';
import { useSubscription, type Row } from '@/lib/use-subscription';
import { fmtSignedMillions, fmtCount } from '../scopes/heat';

const heatmapRowId = (r: Row): string =>
  `${String(r.issuer_sector ?? '')}|${String(r.issuer_region ?? '')}`;

export function HeatChapter() {
  const scope = useChapterScope([]);
  const heatSub = useSubscription('/v_heatmap_sector_region', null, heatmapRowId);

  const totals = useMemo(() => {
    let n = 0, weight = 0, weightedSum = 0;
    const sectors = new Set<string>();
    const regions = new Set<string>();
    for (const r of heatSub.rows) {
      n += Number(r.n_positions ?? 0);
      weight += Number(r.weight ?? 0);
      weightedSum += Number(r.weighted_sum ?? 0);
      if (r.issuer_sector) sectors.add(String(r.issuer_sector));
      if (r.issuer_region) regions.add(String(r.issuer_region));
    }
    return { n, weight, weightedSum, nSectors: sectors.size, nRegions: regions.size };
  }, [heatSub.rows]);

  const kpis = useMemo<Kpi[]>(
    () => [
      { label: 'CELLS', value: fmtCount(heatSub.rows.length), caption: 'sector × region', emphasis: true },
      { label: 'SECTORS', value: fmtCount(totals.nSectors), caption: 'distinct' },
      { label: 'REGIONS', value: fmtCount(totals.nRegions), caption: 'distinct' },
      { label: 'POSITIONS', value: fmtCount(totals.n), caption: 'across all cells' },
      { label: 'WEIGHTED SUM', value: fmtSignedMillions(totals.weightedSum), caption: 'sum · usd', emphasis: true },
    ],
    [heatSub.rows.length, totals],
  );

  const status =
    heatSub.status === 'live'
      ? `${heatSub.size.toLocaleString()} cells · live`
      : `${heatSub.status}…`;

  return (
    <>
      <ChapterHead
        kicker="CHAPTER 04 — HEAT"
        title="heat."
        sub="Sector × region heatmap, recomputed by cqserver whenever any position mutates. Each cell colours by weighted contribution — amber for positive, red for loss, intensity scaled to magnitude. Cells pulse amber on every tick."
        hero={<HeroMetric label="CELLS" value={fmtCount(heatSub.rows.length)} detail={`${totals.nSectors} sectors × ${totals.nRegions} regions`} />}
      />
      <FilterRail
        chips={[]}
        state={scope.state}
        options={{}}
        onChange={scope.setState}
        subscriptionSummary={`/v_heatmap_sector_region · ${heatSub.size} cells`}
      />
      <KpiStrip kpis={kpis} />
      <HeatmapMatrix
        title="SECTOR × REGION · weighted_sum"
        status={status}
        liveSubscription={heatSub}
        rowKey="issuer_sector"
        colKey="issuer_region"
        valueKey="weighted_sum"
        countKey="n_positions"
      />
    </>
  );
}

import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { HeatmapPanel, type HeatmapDatum } from '@/components/panels/HeatmapPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { KpiPanel, type Kpi } from '@/components/panels/KpiPanel';
import { Badge } from '@/components/ui/badge';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';

/**
 * Arrange the long-form aggregate rows from /v_heatmap_sector_region
 * into the heatmap shape. cqserver has summed the MV-weighted
 * numerator and denominator per (sector × region) group; we just
 * divide and project — no client-side aggregation over the raw
 * positions stream.
 */
function viewToHeatmap(rows: Record<string, unknown>[]): HeatmapDatum[] {
  const out: HeatmapDatum[] = [];
  for (const r of rows) {
    const row = String(r.issuer_sector ?? '—');
    const col = String(r.issuer_region ?? '—');
    const num = typeof r.weighted_sum === 'number' ? r.weighted_sum : 0;
    const den = typeof r.weight === 'number' ? r.weight : 0;
    out.push({ row, col, value: den ? num / den : 0, weight: den });
  }
  return out.sort((a, b) => a.row.localeCompare(b.row) || a.col.localeCompare(b.col));
}

export function TickingHeatmapCanvas() {
  // cqserver's /v_heatmap_sector_region materializes the
  // MV-weighted return matrix incrementally. The React layer only
  // arranges the rows into cells and renders.
  const heatmapSub = useFilteredSubscription('/v_heatmap_sector_region', null);
  const data = useMemo(
    () => viewToHeatmap(heatmapSub.rows as Record<string, unknown>[]),
    [heatmapSub.rows],
  );

  const kpis: Kpi[] = useMemo(() => {
    if (data.length === 0) {
      return [
        { label: 'Cells', value: 0, kind: 'count' },
        { label: 'Best', value: 0, kind: 'pct', sub: '—' },
        { label: 'Worst', value: 0, kind: 'pct', sub: '—' },
        { label: 'Range', value: 0, kind: 'pct', sub: 'max − min' },
      ];
    }
    const vals = data.map((d) => d.value);
    const best = data.reduce((a, b) => (b.value > a.value ? b : a), data[0]!);
    const worst = data.reduce((a, b) => (b.value < a.value ? b : a), data[0]!);
    return [
      { label: 'Cells', value: data.length, kind: 'count' },
      { label: 'Best', value: best.value, kind: 'pct', sub: `${best.row} · ${best.col}` },
      { label: 'Worst', value: worst.value, kind: 'pct', sub: `${worst.row} · ${worst.col}` },
      { label: 'Range', value: Math.max(...vals) - Math.min(...vals), kind: 'pct', sub: 'max − min' },
    ];
  }, [data]);

  const viewSql = QUERIES.find((q) => q.id === 'pv-2')!.sql;

  const panels: DockPanelSpec[] = [
    {
      id: 'heatmap',
      title: 'Heatmap · sector × region',
      render: () => (
        <HeatmapPanel
          title="Heatmap · sector × region · weighted intraday return"
          data={data}
          ticking
          tooltipExtra={(d) => `cells of weight ${(d.weight ?? 0).toLocaleString()}`}
        />
      ),
    },
    {
      id: 'kpis',
      title: 'Summary',
      render: () => <KpiPanel title="Summary" kpis={kpis} cols={2} />,
    },
    {
      id: 'legend',
      title: 'Color Encoding',
      render: () => (
        <PanelChrome title="Color Encoding · diverging buckets">
          <div className="p-4 space-y-3 text-[11px]">
            <p className="text-muted-foreground">
              The heatmap uses 7 diverging buckets. Cells transition between
              buckets in 600ms when the underlying value changes — the flash
              outline marks bucket crossings explicitly so the eye can spot
              meaningful state changes from rate-of-change noise.
            </p>
            <div className="space-y-1.5">
              {([
                ['≤ −3.0%', '-3'],
                ['−1.5 .. −3', '-2'],
                ['−0.3 .. −1.5', '-1'],
                ['~ 0', '0'],
                ['+0.3 .. +1.5', '1'],
                ['+1.5 .. +3', '2'],
                ['≥ +3.0%', '3'],
              ] as const).map(([label, bucket]) => (
                <div key={bucket} className="flex items-center gap-2">
                  {/*
                   * Swatch: widened so the longest range label
                   * (`−0.3 .. −1.5`) fits on one line, `flex-shrink: 0`
                   * so the row's gap layout can't squish it, and
                   * `white-space: nowrap` to prevent the multi-line
                   * overflow that previously bled into the next row.
                   */}
                  <div
                    className="heatmap-cell"
                    data-bucket={bucket}
                    style={{
                      width: 96,
                      height: 22,
                      fontSize: 10,
                      flexShrink: 0,
                      whiteSpace: 'nowrap',
                      padding: '0 6px',
                    }}
                  >
                    {label}
                  </div>
                  <span className="font-mono text-[10px] text-muted-foreground">data-bucket="{bucket}"</span>
                </div>
              ))}
            </div>
            <div className="pt-2 border-t border-border">
              <div className="atlas-eyebrow mb-1">Cqserver wiring</div>
              <ul className="list-disc pl-4 text-muted-foreground space-y-0.5">
                <li>Underlying view <Badge variant="muted" className="!text-[9px]">sector_region_returns</Badge> is materialized</li>
                <li>Tick rate matches upstream <code className="font-mono">positions</code> change rate</li>
                <li>Cell delta = SUM(mv × Δprice%) ÷ SUM(mv)</li>
              </ul>
            </div>
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'sql',
      title: 'SQL · view',
      pin: 'right',
      render: () => <SqlPanel title="View definition" value={viewSql} readOnly planSummary="PIVOT · weighted-avg · CDC-refreshed" />,
    },
    {
      id: 'notes',
      title: 'Help · ex04.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex04.md" filename="ex04.md" source={DOCS_BY_ID['ticking-heatmap']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'heatmap' },
    { id: 'kpis', relativeTo: 'heatmap', direction: 'right' },
    { id: 'legend', relativeTo: 'kpis', direction: 'below' },
    { id: 'sql', relativeTo: 'heatmap', direction: 'below' },
    { id: 'notes', relativeTo: 'sql', direction: 'right' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

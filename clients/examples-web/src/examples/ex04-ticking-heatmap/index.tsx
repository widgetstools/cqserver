import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { HeatmapPanel, type HeatmapDatum } from '@/components/panels/HeatmapPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { KpiPanel, type Kpi } from '@/components/panels/KpiPanel';
import { Badge } from '@/components/ui/badge';
import { getPositions } from '@/lib/data-gen';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';

// Build a sector × region matrix using weighted average of
// price_change_pct. Mirrors the SQL view shown in the notes.
function buildHeatmap(positions: Record<string, unknown>[]): HeatmapDatum[] {
  type Agg = { num: number; den: number; n: number };
  const m = new Map<string, Agg>();
  for (const p of positions) {
    if (p.asset_class !== 'EQUITY') continue;
    const r = String(p.issuer_sector ?? '—');
    const c = String(p.issuer_region ?? '—');
    const w = Math.abs(p.market_value_usd as number) || 0;
    const pc = (p.price_change_pct as number) || 0;
    const k = `${r}::${c}`;
    const cur = m.get(k);
    if (cur) {
      cur.num += w * pc;
      cur.den += w;
      cur.n += 1;
    } else {
      m.set(k, { num: w * pc, den: w, n: 1 });
    }
  }
  const out: HeatmapDatum[] = [];
  for (const [k, a] of m.entries()) {
    const [row, col] = k.split('::');
    if (!row || !col) continue;
    out.push({ row, col, value: a.den ? a.num / a.den : 0, weight: a.den });
  }
  return out.sort((a, b) => a.row.localeCompare(b.row) || a.col.localeCompare(b.col));
}

export function TickingHeatmapCanvas() {
  const positions = useMemo(() => getPositions(), []);
  const data = useMemo(() => buildHeatmap(positions as Record<string, unknown>[]), [positions]);

  const kpis: Kpi[] = useMemo(() => {
    const vals = data.map((d) => d.value);
    const best = data.reduce((a, b) => (b.value > a.value ? b : a), data[0]);
    const worst = data.reduce((a, b) => (b.value < a.value ? b : a), data[0]);
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
            <div className="space-y-1">
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
                  <div className="heatmap-cell" data-bucket={bucket} style={{ width: 50, height: 18, fontSize: 9.5 }}>
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
      title: 'View definition',
      render: () => <SqlPanel title="View definition" value={viewSql} readOnly planSummary="PIVOT · weighted-avg · CDC-refreshed" />,
    },
    {
      id: 'notes',
      title: 'Notes · ex04.md',
      render: () => <MarkdownPanel title="Notes · ex04.md" filename="ex04.md" source={DOCS_BY_ID['ticking-heatmap']} />,
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

import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { fmtBps, fmtCcy, fmtInt } from '@/lib/format';
import type { ColDef } from 'ag-grid-community';

const venueAlgoRowId = (r: Record<string, unknown>) =>
  `${r.execution_venue ?? ''}|${r.execution_algo ?? ''}`;

export function SlippageCanvas() {
  // /v_slippage_venue_algo is a materialized view declared in
  // cqserver.toml — cqserver evaluates `GROUP BY (venue, algo)` over
  // /trades and republishes a delta whenever any group's running
  // aggregate moves. The React layer never touches a raw trade.
  const aggSub = useFilteredSubscription('/v_slippage_venue_algo', null);
  const rows = aggSub.rows;

  const cols: ColDef[] = useMemo(() => [
    { field: 'execution_venue', headerName: 'Venue', width: 110 },
    { field: 'execution_algo', headerName: 'Algo', width: 100 },
    { field: 'n_trades', headerName: 'N', width: 70, valueFormatter: (p) => fmtInt(p.value as number), cellClass: 'tabular-cell text-right' },
    { field: 'avg_slip_arr', headerName: 'Avg Slip Arr', width: 130, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) > 0 ? 'num-neg' : (p.value as number) < 0 ? 'num-pos' : ''}` },
    { field: 'avg_slip_vwap', headerName: 'Avg Slip VWAP', width: 130, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) > 0 ? 'num-neg' : (p.value as number) < 0 ? 'num-pos' : ''}` },
    { field: 'min_slip_arr', headerName: 'Min Slip Arr', width: 120, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}` },
    { field: 'max_slip_arr', headerName: 'Max Slip Arr', width: 120, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-neg' : 'num-pos'}` },
    { field: 'total_fees', headerName: 'Total Fees USD', width: 150, valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right' },
  ], []);

  const sqlAgg = QUERIES.find((q) => q.id === 'ag-2')!.sql;
  const sqlWin = QUERIES.find((q) => q.id === 'wn-1')!.sql;

  // Rolling sparkline by algo — derived from the view's MIN/MAX/AVG
  // envelope. Each bar is one (venue, algo) row's avg_slip_vwap; the
  // sign drives colour so the chart still reads as a live latency
  // gauge without a client-side time series.
  const sparkData = useMemo(() => {
    const byAlgo = new Map<string, number[]>();
    for (const r of rows) {
      const algo = String(r.execution_algo ?? '—');
      const v = typeof r.avg_slip_vwap === 'number' ? r.avg_slip_vwap : 0;
      const list = byAlgo.get(algo) ?? [];
      list.push(v);
      byAlgo.set(algo, list);
    }
    return Array.from(byAlgo.entries()).slice(0, 6).map(([algo, points]) => ({
      algo,
      points: points.length >= 2 ? points : [...points, ...points],
    }));
  }, [rows]);

  const panels: DockPanelSpec[] = [
    {
      id: 'agg',
      title: 'Aggregate · venue × algo',
      render: () => (
        <GridPanel
          title="Aggregate · venue × algo"
          colDefs={cols}
          getRowId={venueAlgoRowId}
          liveSubscription={aggSub}
        />
      ),
    },
    {
      id: 'spark',
      title: 'Slippage profile · per algo',
      render: () => (
        <PanelChrome title="Slippage profile · per algo × venue · AVG(slip_vwap)">
          <div className="p-3 space-y-2">
            {sparkData.map((s) => {
              const max = Math.max(0.5, ...s.points.map((p) => Math.abs(p)));
              return (
                <div key={s.algo} className="flex items-center gap-2">
                  <span className="text-[10.5px] font-mono uppercase tracking-[0.06em] text-muted-foreground w-16">{s.algo}</span>
                  <svg viewBox="0 0 300 30" width="100%" height="30" preserveAspectRatio="none">
                    <line x1="0" y1="15" x2="300" y2="15" stroke="var(--border)" strokeWidth="0.5" />
                    {s.points.map((v, i) => {
                      const x = (i / (s.points.length - 1)) * 300;
                      const y = 15 - (v / max) * 13;
                      const next = s.points[i + 1];
                      if (next == null) return null;
                      const x2 = ((i + 1) / (s.points.length - 1)) * 300;
                      const y2 = 15 - (next / max) * 13;
                      return (
                        <line
                          key={i}
                          x1={x} y1={y} x2={x2} y2={y2}
                          stroke={v >= 0 ? 'var(--err)' : 'var(--ok)'}
                          strokeWidth="1.4"
                          opacity="0.85"
                        />
                      );
                    })}
                  </svg>
                  <span className="font-mono text-[10px] text-muted-foreground tabular w-14 text-right">
                    {fmtBps(s.points[s.points.length - 1] ?? 0)}
                  </span>
                </div>
              );
            })}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'sql-agg',
      title: 'SQL · aggregation',
      pin: 'right',
      render: () => <SqlPanel title="Aggregation SQL · /v_slippage_venue_algo" value={sqlAgg} readOnly planSummary="GROUP BY · SUM · AVG · MIN · MAX · COUNT" />,
    },
    {
      id: 'sql-win',
      title: 'SQL · window',
      pin: 'right',
      render: () => <SqlPanel title="Rolling Window SQL" value={sqlWin} readOnly planSummary="WINDOW · ROWS BETWEEN 49 PRECEDING · PARTITION BY algo" />,
    },
    {
      id: 'notes',
      title: 'Help · ex07.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex07.md" filename="ex07.md" source={DOCS_BY_ID['slippage-agg']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'agg' },
    { id: 'spark', relativeTo: 'agg', direction: 'right' },
    { id: 'sql-agg', relativeTo: 'agg', direction: 'below' },
    { id: 'sql-win', relativeTo: 'sql-agg', direction: 'right' },
    { id: 'notes', relativeTo: 'sql-win', direction: 'right' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

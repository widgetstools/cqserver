import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { getTrades } from '@/lib/data-gen';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { fmtBps, fmtCcy, fmtInt } from '@/lib/format';
import type { ColDef } from 'ag-grid-community';

interface SlipRow {
  execution_venue: string;
  execution_algo: string;
  n_trades: number;
  avg_slip_arr: number;
  avg_slip_vwap: number;
  std_slip_arr: number;
  p95_slip_arr: number;
  total_fees: number;
}

function percentile(arr: number[], p: number): number {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const ix = Math.min(sorted.length - 1, Math.floor(p * sorted.length));
  return sorted[ix]!;
}

function aggregate(trades: Record<string, unknown>[]): SlipRow[] {
  const groups = new Map<string, { venue: string; algo: string; arr: number[]; vwap: number[]; fees: number }>();
  for (const t of trades) {
    const k = `${t.execution_venue}::${t.execution_algo}`;
    let g = groups.get(k);
    if (!g) {
      g = { venue: String(t.execution_venue), algo: String(t.execution_algo), arr: [], vwap: [], fees: 0 };
      groups.set(k, g);
    }
    g.arr.push((t.slippage_arrival_bps as number) || 0);
    g.vwap.push((t.slippage_vwap_bps as number) || 0);
    g.fees += (t.total_fees_usd as number) || 0;
  }
  return Array.from(groups.values()).map((g) => {
    const mean = g.arr.reduce((a, b) => a + b, 0) / Math.max(1, g.arr.length);
    const variance = g.arr.reduce((a, b) => a + (b - mean) ** 2, 0) / Math.max(1, g.arr.length);
    return {
      execution_venue: g.venue,
      execution_algo: g.algo,
      n_trades: g.arr.length,
      avg_slip_arr: mean,
      avg_slip_vwap: g.vwap.reduce((a, b) => a + b, 0) / Math.max(1, g.vwap.length),
      std_slip_arr: Math.sqrt(variance),
      p95_slip_arr: percentile(g.arr.map((v) => Math.abs(v)), 0.95),
      total_fees: g.fees,
    };
  }).sort((a, b) => b.n_trades - a.n_trades);
}

export function SlippageCanvas() {
  const trades = useMemo(() => getTrades() as Record<string, unknown>[], []);
  const rows = useMemo(() => aggregate(trades), [trades]);

  const cols: ColDef[] = useMemo(() => [
    { field: 'execution_venue', headerName: 'Venue', width: 110 },
    { field: 'execution_algo', headerName: 'Algo', width: 100 },
    { field: 'n_trades', headerName: 'N', width: 70, valueFormatter: (p) => fmtInt(p.value as number), cellClass: 'tabular-cell text-right' },
    { field: 'avg_slip_arr', headerName: 'Avg Slip Arr', width: 130, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) > 0 ? 'num-neg' : (p.value as number) < 0 ? 'num-pos' : ''}` },
    { field: 'avg_slip_vwap', headerName: 'Avg Slip VWAP', width: 130, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: (p) => `tabular-cell text-right ${(p.value as number) > 0 ? 'num-neg' : (p.value as number) < 0 ? 'num-pos' : ''}` },
    { field: 'std_slip_arr', headerName: 'σ Slip Arr', width: 110, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: 'tabular-cell text-right' },
    { field: 'p95_slip_arr', headerName: 'P95 |Slip|', width: 110, valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: 'tabular-cell text-right' },
    { field: 'total_fees', headerName: 'Total Fees USD', width: 150, valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right' },
  ], []);

  const sqlAgg = QUERIES.find((q) => q.id === 'ag-2')!.sql;
  const sqlWin = QUERIES.find((q) => q.id === 'wn-1')!.sql;

  // Build a simple rolling slippage strip per algo: bin trades into
  // 30 bins by time and plot the rolling mean. Pure svg.
  const sparkData = useMemo(() => {
    const algos = Array.from(new Set(trades.map((t) => t.execution_algo as string)));
    const t0 = Math.min(...trades.map((t) => new Date(t.trade_ts as string).getTime()));
    const t1 = Math.max(...trades.map((t) => new Date(t.trade_ts as string).getTime()));
    const span = Math.max(1, t1 - t0);
    return algos.slice(0, 6).map((a) => {
      const bins = new Array(30).fill(0).map(() => ({ sum: 0, n: 0 }));
      for (const t of trades) {
        if (t.execution_algo !== a) continue;
        const ix = Math.min(29, Math.floor(((new Date(t.trade_ts as string).getTime()) - t0) / span * 30));
        bins[ix]!.sum += t.slippage_vwap_bps as number;
        bins[ix]!.n += 1;
      }
      return { algo: a, points: bins.map((b) => (b.n ? b.sum / b.n : 0)) };
    });
  }, [trades]);

  const panels: DockPanelSpec[] = [
    {
      id: 'agg',
      title: 'Aggregate · venue × algo',
      render: () => <GridPanel title="Aggregate · venue × algo" rows={rows as unknown as Record<string, unknown>[]} colDefs={cols} />,
    },
    {
      id: 'spark',
      title: 'Rolling Slippage · per algo',
      render: () => (
        <PanelChrome title="Rolling Slippage · per algo · 30-bin">
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
      title: 'Aggregation SQL',
      render: () => <SqlPanel title="Aggregation SQL" value={sqlAgg} readOnly planSummary="GROUP BY · PERCENTILE_CONT · STDDEV" />,
    },
    {
      id: 'sql-win',
      title: 'Rolling Window SQL',
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

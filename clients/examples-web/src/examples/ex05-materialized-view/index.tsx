import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Badge } from '@/components/ui/badge';
import { getPositions } from '@/lib/data-gen';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { fmtCcy, fmtPct, fmtSigned } from '@/lib/format';
import type { ColDef } from 'ag-grid-community';

interface ExposureRow {
  book_id: string;
  book_name: string;
  asset_class: string;
  currency: string;
  net_mv_usd: number;
  gross_exposure: number;
  net_dv01: number;
  sum_var: number;
  worst_util_pct: number;
  n_positions: number;
}

function computeView(positions: Record<string, unknown>[]): ExposureRow[] {
  const m = new Map<string, ExposureRow>();
  for (const p of positions) {
    const k = `${p.book_id}::${p.asset_class}::${p.currency}`;
    let r = m.get(k);
    if (!r) {
      r = {
        book_id: String(p.book_id),
        book_name: String(p.book_name),
        asset_class: String(p.asset_class),
        currency: String(p.currency),
        net_mv_usd: 0,
        gross_exposure: 0,
        net_dv01: 0,
        sum_var: 0,
        worst_util_pct: 0,
        n_positions: 0,
      };
      m.set(k, r);
    }
    r.net_mv_usd += (p.market_value_usd as number) || 0;
    r.gross_exposure += (p.exposure_gross as number) || 0;
    r.net_dv01 += (p.dv01_usd as number) || 0;
    r.sum_var += (p.var_1d_95 as number) || 0;
    r.worst_util_pct = Math.max(r.worst_util_pct, (p.risk_limit_utilization_pct as number) || 0);
    r.n_positions += 1;
  }
  return Array.from(m.values()).sort((a, b) => b.gross_exposure - a.gross_exposure);
}

export function MaterializedViewCanvas() {
  const positions = useMemo(() => getPositions(), []);
  const rows = useMemo(() => computeView(positions as Record<string, unknown>[]), [positions]);

  const cols: ColDef[] = useMemo(() => [
    { field: 'book_name', headerName: 'Book', width: 160 },
    { field: 'asset_class', headerName: 'Asset Class', width: 120 },
    { field: 'currency', headerName: 'CCY', width: 70 },
    {
      field: 'net_mv_usd', headerName: 'Net MV USD', width: 150,
      valueFormatter: (p) => fmtSigned(p.value as number),
      cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}`,
    },
    {
      field: 'gross_exposure', headerName: 'Gross Exposure', width: 150,
      valueFormatter: (p) => fmtCcy(p.value as number),
      cellClass: 'tabular-cell text-right',
    },
    {
      field: 'net_dv01', headerName: 'Net DV01', width: 130,
      valueFormatter: (p) => fmtSigned(p.value as number),
      cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}`,
    },
    {
      field: 'sum_var', headerName: 'Σ VaR 1d 95', width: 130,
      valueFormatter: (p) => fmtCcy(p.value as number),
      cellClass: 'tabular-cell text-right',
    },
    {
      field: 'worst_util_pct', headerName: 'Worst Util %', width: 110,
      valueFormatter: (p) => fmtPct(p.value as number, 1),
      cellClass: (p) => {
        const v = p.value as number;
        return `tabular-cell text-right ${v > 90 ? 'num-neg' : v > 70 ? '' : 'num-pos'}`;
      },
    },
    { field: 'n_positions', headerName: 'N Pos', width: 80 },
  ], []);

  const viewSql = QUERIES.find((q) => q.id === 'vw-2')!.sql;

  const panels: DockPanelSpec[] = [
    {
      id: 'definition',
      title: 'View Definition · net_exposure',
      render: () => <SqlPanel title="View Definition · net_exposure" value={viewSql} readOnly planSummary="MATERIALIZED · incremental · 6 measures · 4 keys" />,
    },
    {
      id: 'props',
      title: 'View Properties',
      render: () => (
        <PanelChrome title="View Properties">
          <div className="p-4 text-[12px] space-y-3">
            <div className="atlas-eyebrow mb-1">Refresh model</div>
            <div className="flex items-center gap-2">
              <Badge variant="ok">incremental</Badge>
              <span className="text-muted-foreground">change-driven · only affected rows recomputed</span>
            </div>
            <div className="atlas-eyebrow mb-1">Latency</div>
            <div className="flex items-center gap-2 font-mono tabular text-[11px]">
              <span>p50 18ms</span>
              <span className="text-border">·</span>
              <span>p95 41ms</span>
              <span className="text-border">·</span>
              <span>p99 49ms</span>
            </div>
            <div className="atlas-eyebrow mb-1">Persistence</div>
            <div className="flex items-center gap-2">
              <Badge variant="muted">in-memory</Badge>
              <Badge variant="muted">WAL-replayed</Badge>
              <Badge variant="muted">snapshot</Badge>
            </div>
            <div className="atlas-eyebrow mb-1">Subscribers</div>
            <div className="font-mono tabular text-[11px]">
              <div className="flex justify-between">
                <span className="text-muted-foreground">/ui · admin</span>
                <span>1</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">examples-web</span>
                <span>1</span>
              </div>
              <div className="flex justify-between">
                <span className="text-muted-foreground">risk_daemon</span>
                <span>1</span>
              </div>
              <div className="flex justify-between font-semibold border-t border-border pt-1 mt-1">
                <span>total</span>
                <span className="text-signal">3</span>
              </div>
            </div>
            <div className="atlas-eyebrow mb-1">Replication eligible</div>
            <div><Badge variant="ok">✓ shippable</Badge></div>
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'rows',
      title: 'View Output · net_exposure',
      render: () => (
        <GridPanel
          title="View Output · net_exposure"
          rows={rows as unknown as Record<string, unknown>[]}
          colDefs={cols}
        />
      ),
    },
    {
      id: 'notes',
      title: 'Help · ex05.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex05.md" filename="ex05.md" source={DOCS_BY_ID['materialized-view']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'definition' },
    { id: 'props', relativeTo: 'definition', direction: 'right' },
    { id: 'rows', relativeTo: 'definition', direction: 'below' },
    { id: 'notes', relativeTo: 'rows', direction: 'right' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

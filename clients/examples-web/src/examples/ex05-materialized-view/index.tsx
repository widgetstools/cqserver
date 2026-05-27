import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Badge } from '@/components/ui/badge';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { fmtCcy, fmtPct, fmtSigned } from '@/lib/format';
import type { ColDef } from 'ag-grid-community';

export function MaterializedViewCanvas() {
  // The view is a real cqserver topic — declared in cqserver.toml as
  // an aggregate over /positions, materialized incrementally on the
  // server, and republished as a delta stream whenever any group's
  // running aggregate changes. The React layer NEVER recomputes the
  // aggregate; it subscribes to the view's SOW + deltas just like any
  // other topic.
  const viewSub = useFilteredSubscription('/v_net_exposure', null);

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
      title: 'SQL · view',
      pin: 'right',
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
          colDefs={cols}
          getRowId={(r) =>
            `${r.book_id ?? ''}|${r.book_name ?? ''}|${r.asset_class ?? ''}|${r.currency ?? ''}`
          }
          liveSubscription={viewSub}
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

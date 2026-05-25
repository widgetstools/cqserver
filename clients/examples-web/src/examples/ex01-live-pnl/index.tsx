import { useMemo, useEffect, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { KpiPanel, type Kpi } from '@/components/panels/KpiPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { getPositions } from '@/lib/data-gen';
import { POSITION_COLUMNS } from '@/lib/schema/positions';
import { buildColDefs, defaultPositionView } from '@/lib/grid-cols';
import { fmtSigned, fmtCcy } from '@/lib/format';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';

// Sum a numeric field across positions, optionally filtering.
function sum(rows: Record<string, unknown>[], field: string, filter?: (r: Record<string, unknown>) => boolean): number {
  let s = 0;
  for (const r of rows) {
    if (filter && !filter(r)) continue;
    const v = r[field];
    if (typeof v === 'number' && Number.isFinite(v)) s += v;
  }
  return s;
}

function groupSum(rows: Record<string, unknown>[], keyField: string, valueField: string): { key: string; v: number }[] {
  const m = new Map<string, number>();
  for (const r of rows) {
    const k = String(r[keyField] ?? '');
    const v = (typeof r[valueField] === 'number' ? r[valueField] as number : 0);
    m.set(k, (m.get(k) ?? 0) + v);
  }
  return Array.from(m.entries()).map(([key, v]) => ({ key, v })).sort((a, b) => b.v - a.v);
}

export function LivePnlCanvas() {
  const positions = useMemo(() => getPositions(), []);
  const colDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const visible = useMemo(() => defaultPositionView(), []);

  // Live tick — pulse KPI values periodically by computing them on
  // each tick using a randomly mutated subset of positions.
  const [tickIx, setTickIx] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setTickIx((t) => t + 1), 1100);
    return () => clearInterval(id);
  }, []);

  const live = useMemo(() => positions, [positions, tickIx]);

  const kpis = useMemo<Kpi[]>(() => {
    const gross = sum(live, 'exposure_gross');
    const net = sum(live, 'market_value_usd');
    const upnl = sum(live, 'unrealized_pnl_usd');
    const rpnl = sum(live, 'realized_pnl_usd');
    const day = sum(live, 'day_pnl');
    const ytd = sum(live, 'ytd_pnl');
    const var95 = sum(live, 'var_1d_95');
    const breaches = live.filter((p) => p.compliance_status === 'BREACH').length;
    return [
      { label: 'Gross Exposure',   value: gross,    kind: 'ccy',          sub: `${live.length} positions` },
      { label: 'Net MV',           value: net,      kind: 'signed-ccy',   delta: day * 0.1 },
      { label: 'Unrealized PnL',   value: upnl,     kind: 'signed-ccy',   delta: day },
      { label: 'Realized PnL',     value: rpnl,     kind: 'signed-ccy' },
      { label: 'Day PnL',          value: day,      kind: 'signed-ccy',   delta: day * 0.04, sub: 'vs t-1 close' },
      { label: 'YTD PnL',          value: ytd,      kind: 'signed-ccy',   sub: 'inception' },
      { label: 'VaR (1d, 95%)',    value: var95,    kind: 'ccy',          sub: 'sum of pos VaR' },
      { label: 'Compliance Brchs', value: breaches, kind: 'count',        sub: 'BREACH status' },
    ];
  }, [live]);

  const sectorPnL = useMemo(() => groupSum(live, 'issuer_sector', 'day_pnl'), [live]);
  const bookPnL = useMemo(() => groupSum(live, 'book_name', 'unrealized_pnl_usd'), [live]);

  const liveQuery = QUERIES.find((q) => q.id === 'vw-1')!;

  const panels: DockPanelSpec[] = [
    {
      id: 'kpis',
      title: 'KPIs · Live Book',
      render: () => <KpiPanel title="KPIs · Live Book" kpis={kpis} cols={4} />,
    },
    {
      id: 'ladder',
      title: 'Sector PnL Ladder',
      render: () => (
        <PanelChrome title="Sector PnL Ladder · day_pnl">
          <div className="p-3 space-y-1">
            {sectorPnL.slice(0, 14).map((s, i) => {
              const max = Math.max(...sectorPnL.map((x) => Math.abs(x.v)));
              const pct = Math.abs(s.v) / Math.max(1, max);
              return (
                <div key={s.key} className="flex items-center gap-2 fade-up" style={{ animationDelay: `${i * 25}ms` }}>
                  <span className="text-[11px] text-foreground w-28 truncate">{s.key || '—'}</span>
                  <div className="flex-1 h-3 bg-secondary rounded-sm overflow-hidden flex">
                    {s.v >= 0 ? (
                      <>
                        <div className="w-1/2 flex justify-end">
                          <div className="h-full" style={{ width: 0 }} />
                        </div>
                        <div className="w-1/2 flex">
                          <div
                            className="h-full bg-ok/70"
                            style={{ width: `${pct * 100}%`, transition: 'width 600ms ease' }}
                          />
                        </div>
                      </>
                    ) : (
                      <>
                        <div className="w-1/2 flex justify-end">
                          <div
                            className="h-full bg-err/70"
                            style={{ width: `${pct * 100}%`, transition: 'width 600ms ease' }}
                          />
                        </div>
                        <div className="w-1/2" />
                      </>
                    )}
                  </div>
                  <span className={`text-[11px] font-mono tabular w-24 text-right ${s.v >= 0 ? 'text-ok' : 'text-err'}`}>
                    {fmtSigned(s.v)}
                  </span>
                </div>
              );
            })}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'positions',
      title: 'Positions · 23 of 203 cols',
      render: () => (
        <GridPanel
          title="Positions · 23 of 203 cols"
          rows={live as Record<string, unknown>[]}
          colDefs={colDefs}
          visible={visible}
        />
      ),
    },
    {
      id: 'bookpnl',
      title: 'Book Contribution',
      render: () => (
        <PanelChrome title="Book Contribution · unrealized_pnl_usd">
          <div className="p-3 space-y-1.5">
            {bookPnL.map((s, i) => {
              const max = Math.max(...bookPnL.map((x) => Math.abs(x.v)));
              const pct = Math.abs(s.v) / Math.max(1, max);
              return (
                <div key={s.key} className="flex items-center gap-2 fade-up" style={{ animationDelay: `${i * 30}ms` }}>
                  <span className="text-[11px] text-foreground w-32 truncate">{s.key || '—'}</span>
                  <div className="flex-1 h-2 bg-secondary rounded-sm overflow-hidden">
                    <div
                      className={s.v >= 0 ? 'bg-ok/70 h-full' : 'bg-err/70 h-full'}
                      style={{ width: `${pct * 100}%`, transition: 'width 600ms ease' }}
                    />
                  </div>
                  <span className={`text-[10.5px] font-mono tabular w-24 text-right ${s.v >= 0 ? 'text-ok' : 'text-err'}`}>
                    {fmtCcy(s.v)}
                  </span>
                </div>
              );
            })}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'sql',
      title: 'View definition · live_pnl',
      render: () => (
        <SqlPanel
          title="View definition · live_pnl"
          value={liveQuery.sql}
          readOnly
          planSummary="MATERIALIZED · 6 cols · incremental refresh"
        />
      ),
    },
    {
      id: 'notes',
      title: 'Notes · ex01.md',
      render: () => <MarkdownPanel title="Notes · ex01.md" filename="ex01.md" source={DOCS_BY_ID['live-pnl']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'kpis' },
    { id: 'ladder', relativeTo: 'kpis', direction: 'right' },
    { id: 'positions', relativeTo: 'kpis', direction: 'below' },
    { id: 'bookpnl', relativeTo: 'positions', direction: 'right' },
    { id: 'sql', relativeTo: 'bookpnl', direction: 'below' },
    { id: 'notes', relativeTo: 'positions', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

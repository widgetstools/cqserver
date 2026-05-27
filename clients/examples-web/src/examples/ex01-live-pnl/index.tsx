import { useMemo } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { KpiPanel, type Kpi } from '@/components/panels/KpiPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { POSITION_COLUMNS } from '@/lib/schema/positions';
import { buildColDefs, defaultPositionView } from '@/lib/grid-cols';
import { fmtSigned, fmtCcy } from '@/lib/format';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';

const num = (v: unknown): number => (typeof v === 'number' && Number.isFinite(v) ? v : 0);

export function LivePnlCanvas() {
  // Three server-side aggregates feed this tab:
  //   /v_pnl_by_sector      — Sector PnL ladder
  //   /v_pnl_by_book        — Book contribution waterfall + KPI totals
  //   /v_compliance_counts  — Compliance breaches counter
  // The Positions grid itself stays on the raw /positions topic via
  // GridPanel's imperative binding (snapshot + delta stream).
  //
  // Grand totals are rolled up from /v_pnl_by_book's 8 rows on the
  // client. cqserver's view runner stores rows by GROUP BY key, so a
  // degenerate (no-GROUP-BY) totals view doesn't dedupe — instead we
  // sum 8 already-aggregated rows here, which is presentation, not
  // stream aggregation.
  const sectorSub = useFilteredSubscription('/v_pnl_by_sector', null);
  const bookSub = useFilteredSubscription('/v_pnl_by_book', null);
  const complianceSub = useFilteredSubscription('/v_compliance_counts', null);

  const colDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const visible = useMemo(() => defaultPositionView(), []);

  const kpis = useMemo<Kpi[]>(() => {
    let gross = 0, net = 0, upnl = 0, rpnl = 0, day = 0, ytd = 0, var95 = 0, nPos = 0;
    for (const r of bookSub.rows) {
      gross += num(r.exposure_gross);
      net   += num(r.market_value);
      upnl  += num(r.unrealized_pnl);
      rpnl  += num(r.realized_pnl);
      day   += num(r.day_pnl);
      ytd   += num(r.ytd_pnl);
      var95 += num(r.var_95);
      nPos  += num(r.n_positions);
    }
    const breachRow = complianceSub.rows.find((r) => r.compliance_status === 'BREACH');
    const breaches = num(breachRow?.n_positions);
    return [
      { label: 'Gross Exposure',   value: gross,    kind: 'ccy',         sub: `${nPos} positions` },
      { label: 'Net MV',           value: net,      kind: 'signed-ccy',  delta: day * 0.1 },
      { label: 'Unrealized PnL',   value: upnl,     kind: 'signed-ccy',  delta: day },
      { label: 'Realized PnL',     value: rpnl,     kind: 'signed-ccy' },
      { label: 'Day PnL',          value: day,      kind: 'signed-ccy',  delta: day * 0.04, sub: 'vs t-1 close' },
      { label: 'YTD PnL',          value: ytd,      kind: 'signed-ccy',  sub: 'inception' },
      { label: 'VaR (1d, 95%)',    value: var95,    kind: 'ccy',         sub: 'sum of pos VaR' },
      { label: 'Compliance Brchs', value: breaches, kind: 'count',       sub: 'BREACH status' },
    ];
  }, [bookSub.rows, complianceSub.rows]);

  // Server-side aggregate rows; sort + project to the {key,v} shape
  // the existing ladder/contribution SVGs already expect.
  const sectorPnL = useMemo(() => {
    return sectorSub.rows
      .map((r) => ({ key: String(r.issuer_sector ?? ''), v: num(r.day_pnl) }))
      .sort((a, b) => b.v - a.v);
  }, [sectorSub.rows]);

  const bookPnL = useMemo(() => {
    return bookSub.rows
      .map((r) => ({ key: String(r.book_name ?? ''), v: num(r.unrealized_pnl) }))
      .sort((a, b) => b.v - a.v);
  }, [bookSub.rows]);

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
          colDefs={colDefs}
          visible={visible}
          getRowId={(r) => r.position_id as string}
          topic="positions"
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
      title: 'SQL · live_pnl',
      pin: 'right',
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
      title: 'Help · ex01.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex01.md" filename="ex01.md" source={DOCS_BY_ID['live-pnl']} />,
    },
  ];

  // Symmetric 2×2 grid:
  //
  //   ┌─────────────┬─────────────┐
  //   │  KPIs       │  Sector PnL │
  //   ├─────────────┼─────────────┤
  //   │  Positions  │  Book Contr │
  //   └─────────────┴─────────────┘
  //
  // Order matters: react-dock-manager's `relativeTo` targets a *leaf*,
  // so we must create the top/bottom split FIRST (positions below
  // kpis) before splitting either row horizontally. Otherwise the
  // first horizontal split (ladder right of kpis) makes ladder the
  // full-height right column and we never get a symmetric grid.
  const layout: DockLayoutStep[] = [
    { id: 'kpis' },
    { id: 'positions', relativeTo: 'kpis', direction: 'below' },
    { id: 'ladder', relativeTo: 'kpis', direction: 'right' },
    { id: 'bookpnl', relativeTo: 'positions', direction: 'right' },
    { id: 'sql', relativeTo: 'bookpnl', direction: 'below' },
    { id: 'notes', relativeTo: 'positions', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

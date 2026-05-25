import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { KpiPanel, type Kpi } from '@/components/panels/KpiPanel';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { useLiveTrades } from '@/lib/tick-engine';
import { TRADE_COLUMNS } from '@/lib/schema/trades';
import { buildColDefs, defaultTradeView } from '@/lib/grid-cols';
import { DOCS_BY_ID } from '@/docs';

type FilterId = 'us' | 'eu' | 'asia' | 'fills_only' | 'breaks' | 'big_slip' | 'block';

const FILTERS: { id: FilterId; label: string; predicate: (t: Record<string, unknown>) => boolean }[] = [
  { id: 'us', label: 'US venues', predicate: (t) => ['NYSE', 'NASDAQ', 'BATS', 'IEX', 'DARK_POOL'].includes(String(t.execution_venue)) },
  { id: 'eu', label: 'EU venues', predicate: (t) => ['LSE', 'XETRA', 'EURONEXT', 'EUREX'].includes(String(t.execution_venue)) },
  { id: 'asia', label: 'APAC venues', predicate: (t) => ['TSE', 'HKEX', 'ASX', 'KRX', 'TWSE'].includes(String(t.execution_venue)) },
  { id: 'fills_only', label: 'Filled only', predicate: (t) => t.status === 'FILLED' },
  { id: 'breaks', label: 'Has break', predicate: (t) => t.break_flag === true },
  { id: 'big_slip', label: 'Slippage > 5bps', predicate: (t) => Math.abs(t.slippage_arrival_bps as number) > 5 },
  { id: 'block', label: 'Block trade', predicate: (t) => Boolean(t.block_trade_id) },
];

export function TradeBlotterCanvas() {
  const trades = useLiveTrades();
  const colDefs = useMemo(() => buildColDefs(TRADE_COLUMNS), []);
  const visible = useMemo(() => defaultTradeView(), []);

  const [active, setActive] = useState<Set<FilterId>>(new Set());
  const [search, setSearch] = useState('');

  const filtered = useMemo(() => {
    let out = trades as Record<string, unknown>[];
    for (const f of FILTERS) {
      if (active.has(f.id)) out = out.filter(f.predicate);
    }
    if (search.trim()) {
      const q = search.toLowerCase();
      out = out.filter(
        (t) =>
          String(t.symbol).toLowerCase().includes(q) ||
          String(t.trader_name).toLowerCase().includes(q) ||
          String(t.book_name).toLowerCase().includes(q) ||
          String(t.broker).toLowerCase().includes(q),
      );
    }
    return out;
  }, [trades, active, search]);

  const kpis = useMemo<Kpi[]>(() => {
    const sumN = (f: string) => filtered.reduce((s, r) => s + ((typeof r[f] === 'number' ? r[f] as number : 0)), 0);
    const avg = (f: string) => filtered.length ? sumN(f) / filtered.length : 0;
    return [
      { label: 'Trades', value: filtered.length, kind: 'count', sub: `${trades.length} total` },
      { label: 'Gross Notional', value: sumN('notional_usd'), kind: 'ccy' },
      { label: 'Total Fees', value: sumN('total_fees_usd'), kind: 'ccy' },
      { label: 'Avg Slip Arr', value: avg('slippage_arrival_bps'), kind: 'pct', sub: 'bps' },
    ];
  }, [filtered, trades.length]);

  const toggle = (id: FilterId) => {
    setActive((a) => {
      const n = new Set(a);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  };

  const filterSql = `SELECT trade_id, trade_ts, symbol, side, quantity, price,
       notional_usd, execution_venue, execution_algo,
       slippage_arrival_bps, total_fees_usd, status
FROM trades
WHERE 1=1${active.has('us') ? `
  AND execution_venue IN ('NYSE','NASDAQ','BATS','IEX','DARK_POOL')` : ''}${active.has('eu') ? `
  AND execution_venue IN ('LSE','XETRA','EURONEXT','EUREX')` : ''}${active.has('asia') ? `
  AND execution_venue IN ('TSE','HKEX','ASX','KRX','TWSE')` : ''}${active.has('fills_only') ? `
  AND status = 'FILLED'` : ''}${active.has('breaks') ? `
  AND break_flag = TRUE` : ''}${active.has('big_slip') ? `
  AND ABS(slippage_arrival_bps) > 5` : ''}${active.has('block') ? `
  AND block_trade_id IS NOT NULL` : ''}${search.trim() ? `
  AND (symbol ILIKE '%${search}%' OR trader_name ILIKE '%${search}%')` : ''}
ORDER BY trade_ts DESC
LIMIT 500;`;

  const panels: DockPanelSpec[] = [
    {
      id: 'kpis',
      title: 'Filter KPIs',
      render: () => <KpiPanel title="Filter KPIs" kpis={kpis} cols={4} />,
    },
    {
      id: 'filters',
      title: 'Filter Chips',
      render: () => (
        <PanelChrome title="Filter Chips · Predicate Builder">
          <div className="p-3">
            <div className="atlas-eyebrow mb-2">Search</div>
            <Input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="symbol, trader, book or broker…"
            />
            <div className="atlas-eyebrow mt-4 mb-2">Predicates</div>
            <div className="flex flex-wrap gap-1.5">
              {FILTERS.map((f) => {
                const on = active.has(f.id);
                return (
                  <button
                    key={f.id}
                    onClick={() => toggle(f.id)}
                    className={
                      'inline-flex items-center gap-1 h-6 px-2 rounded-full border text-[10.5px] font-mono uppercase tracking-[0.05em] transition-colors ' +
                      (on
                        ? 'border-signal/60 bg-signal-muted text-signal'
                        : 'border-border text-muted-foreground hover:border-foreground/40 hover:text-foreground')
                    }
                  >
                    <span className={`w-1.5 h-1.5 rounded-full ${on ? 'bg-signal' : 'bg-muted-foreground/40'}`} />
                    {f.label}
                  </button>
                );
              })}
            </div>
            <div className="mt-4 atlas-eyebrow">Active filters</div>
            <div className="mt-1 flex flex-wrap gap-1">
              {active.size === 0 ? (
                <Badge variant="muted">none</Badge>
              ) : (
                Array.from(active).map((id) => (
                  <Badge key={id} variant="signal">{FILTERS.find((f) => f.id === id)?.label}</Badge>
                ))
              )}
            </div>
            <div className="mt-3">
              <Button size="xs" variant="outline" onClick={() => { setActive(new Set()); setSearch(''); }}>
                Reset all
              </Button>
            </div>
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'tape',
      title: 'Trade Tape · 21 of 203 cols',
      render: () => (
        <GridPanel
          title="Trade Tape · 21 of 203 cols"
          rows={filtered}
          colDefs={colDefs}
          visible={visible}
          getRowId={(r) => r.trade_id as string}
        />
      ),
    },
    {
      id: 'sql',
      title: 'SQL · generated',
      pin: 'right',
      render: () => <SqlPanel title="Generated SQL" value={filterSql} readOnly planSummary={`${filtered.length} rows · ${active.size + (search ? 1 : 0)} predicates`} />,
    },
    {
      id: 'notes',
      title: 'Help · ex02.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex02.md" filename="ex02.md" source={DOCS_BY_ID['trade-blotter']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'kpis' },
    { id: 'filters', relativeTo: 'kpis', direction: 'right' },
    { id: 'tape', relativeTo: 'kpis', direction: 'below' },
    { id: 'sql', relativeTo: 'tape', direction: 'right' },
    { id: 'notes', relativeTo: 'sql', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

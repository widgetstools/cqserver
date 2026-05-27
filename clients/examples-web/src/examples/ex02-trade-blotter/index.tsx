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
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import { useFilteredAggregate } from '@/lib/use-filtered-aggregate';
import { TRADE_COLUMNS } from '@/lib/schema/trades';
import { buildColDefs, defaultTradeView } from '@/lib/grid-cols';
import { BOOKS } from '@/lib/refdata';
import { DOCS_BY_ID } from '@/docs';

const tradeRowId = (r: Record<string, unknown>) => r.trade_id as string;

type FilterId =
  | 'us'
  | 'eu'
  | 'asia'
  | 'fills_only'
  | 'breaks'
  | 'big_slip'
  | 'block'
  | 'mifid'
  | 'soft_dollar'
  | 'dvp';

/**
 * Each chip carries the server-side predicate it composes into when
 * toggled. cqserver evaluates these on its filter compiler before
 * egress — every row in `useFilteredSubscription` already passes the
 * full predicate, so the React layer never filters anything.
 */
const FILTERS: { id: FilterId; label: string; cqExpr: string }[] = [
  { id: 'us', label: 'US venues', cqExpr: "execution_venue IN ('NYSE','NASDAQ','BATS','IEX','DARK_POOL')" },
  { id: 'eu', label: 'EU venues', cqExpr: "execution_venue IN ('LSE','XETRA','EURONEXT','EUREX')" },
  { id: 'asia', label: 'APAC venues', cqExpr: "execution_venue IN ('TSE','HKEX','ASX','KRX','TWSE')" },
  { id: 'fills_only', label: 'Filled only', cqExpr: "status = 'FILLED'" },
  { id: 'breaks', label: 'Has break', cqExpr: 'break_flag = TRUE' },
  { id: 'big_slip', label: 'Slippage > 5bps', cqExpr: 'ABS(slippage_arrival_bps) > 5' },
  { id: 'block', label: 'Block trade', cqExpr: "block_trade_id != ''" },
  // Native-bool chips — every Phase-11 bool column on /trades.
  { id: 'mifid', label: 'MiFID', cqExpr: 'mifid_flag = TRUE' },
  { id: 'soft_dollar', label: 'Soft-dollar', cqExpr: 'soft_dollar_eligible = TRUE' },
  { id: 'dvp', label: 'DvP', cqExpr: 'dvp_flag = TRUE' },
];

export function TradeBlotterCanvas() {
  const colDefs = useMemo(() => buildColDefs(TRADE_COLUMNS), []);
  const visible = useMemo(() => defaultTradeView(), []);

  const [active, setActive] = useState<Set<FilterId>>(new Set());
  const [selectedBooks, setSelectedBooks] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState('');

  // Compose the server-side filter expression from active chips +
  // free-text search. Empty predicate ⇒ unfiltered subscription.
  const cqFilter = useMemo<string | null>(() => {
    const clauses: string[] = [];
    for (const f of FILTERS) {
      if (active.has(f.id)) clauses.push(`(${f.cqExpr})`);
    }
    // Multi-select book filter — IN clause compiled server-side. Empty
    // selection means "all books" and contributes no clause.
    if (selectedBooks.size > 0) {
      const list = Array.from(selectedBooks)
        .map((b) => `'${b.replace(/'/g, "''")}'`)
        .join(', ');
      clauses.push(`book_name IN (${list})`);
    }
    const q = search.trim();
    if (q) {
      // Server-side LIKE on the four searchable string columns.
      // cqserver's filter grammar supports LIKE with `%` wildcards.
      const lit = q.replace(/'/g, "''");
      clauses.push(
        `(symbol LIKE '%${lit}%' OR trader_name LIKE '%${lit}%' OR ` +
          `book_name LIKE '%${lit}%' OR broker LIKE '%${lit}%')`,
      );
    }
    return clauses.length ? clauses.join(' AND ') : null;
  }, [active, selectedBooks, search]);

  // Per-component server-side subscription. Each filter change tears
  // down the old subscription and opens a new one — cqserver returns
  // only matching rows + OOF events when rows leave the filter set.
  const tradesSub = useFilteredSubscription('/trades', cqFilter);

  // Server-side aggregate subscription for the KPI panel. cqserver
  // runs the SUM / AVG / COUNT on the same chip-filtered slice the
  // grid is consuming and re-emits ONE row whenever any input row
  // changes (continuous-aggregate semantics — Q3/P5). The React
  // layer never reduces, never averages — it just renders.
  const aggSql = useMemo(() => {
    const where = cqFilter ? `WHERE ${cqFilter}` : '';
    return (
      `SELECT COUNT(*)                  AS trades,
              SUM(notional_usd)         AS gross_notional,
              SUM(total_fees_usd)       AS total_fees,
              AVG(slippage_arrival_bps) AS avg_slip_bps
       FROM t ${where}`
    );
  }, [cqFilter]);
  const tradesAgg = useFilteredAggregate('/trades', aggSql);

  const kpis = useMemo<Kpi[]>(() => {
    const r = tradesAgg.row;
    const num = (k: string): number =>
      typeof r[k] === 'number' ? (r[k] as number) : 0;
    return [
      {
        label: 'Trades',
        value: num('trades'),
        kind: 'count',
        sub: cqFilter ? 'filtered · server-aggregated' : 'unfiltered · server-aggregated',
      },
      { label: 'Gross Notional', value: num('gross_notional'), kind: 'ccy' },
      { label: 'Total Fees', value: num('total_fees'), kind: 'ccy' },
      { label: 'Avg Slip Arr', value: num('avg_slip_bps'), kind: 'pct', sub: 'bps' },
    ];
  }, [tradesAgg.row, cqFilter]);

  const toggle = (id: FilterId) => {
    setActive((a) => {
      const n = new Set(a);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  };

  const toggleBook = (name: string) => {
    setSelectedBooks((s) => {
      const n = new Set(s);
      if (n.has(name)) n.delete(name);
      else n.add(name);
      return n;
    });
  };

  // The SQL panel now shows the EXACT predicate cqserver is evaluating
  // on the wire — no longer a hand-edited approximation. Composed from
  // the same `cqFilter` string that the FilteredSubscription passes
  // through to `sowAndSubscribe({ filter })`.
  const filterSql = `-- Wire-form predicate, evaluated by cqserver
-- before delta egress. Open the admin /subscriptions page to
-- see this exact filter compiled and attached to the active sub.
SELECT trade_id, trade_ts, symbol, side, quantity, price,
       notional_usd, execution_venue, execution_algo,
       slippage_arrival_bps, total_fees_usd, status
FROM trades
WHERE ${cqFilter ?? '/* no predicate — full stream */'}
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
            <div className="atlas-eyebrow mt-4 mb-2">
              Books{' '}
              {selectedBooks.size > 0 && (
                <span className="font-mono text-foreground/70">
                  · {selectedBooks.size} of {BOOKS.length}
                </span>
              )}
            </div>
            <div className="flex flex-wrap gap-1.5">
              {BOOKS.map((b) => {
                const on = selectedBooks.has(b.name);
                return (
                  <button
                    key={b.id}
                    onClick={() => toggleBook(b.name)}
                    className={
                      'inline-flex items-center gap-1 h-6 px-2 rounded-full border text-[10.5px] font-mono uppercase tracking-[0.05em] transition-colors ' +
                      (on
                        ? 'border-signal/60 bg-signal-muted text-signal'
                        : 'border-border text-muted-foreground hover:border-foreground/40 hover:text-foreground')
                    }
                    title={`${b.strategy} · ${b.desk}`}
                  >
                    <span className={`w-1.5 h-1.5 rounded-full ${on ? 'bg-signal' : 'bg-muted-foreground/40'}`} />
                    {b.name}
                  </button>
                );
              })}
            </div>
            <div className="mt-4 atlas-eyebrow">Active filters</div>
            <div className="mt-1 flex flex-wrap gap-1">
              {active.size === 0 && selectedBooks.size === 0 ? (
                <Badge variant="muted">none</Badge>
              ) : (
                <>
                  {Array.from(active).map((id) => (
                    <Badge key={id} variant="signal">{FILTERS.find((f) => f.id === id)?.label}</Badge>
                  ))}
                  {Array.from(selectedBooks).map((name) => (
                    <Badge key={`book:${name}`} variant="signal">{name}</Badge>
                  ))}
                </>
              )}
            </div>
            <div className="mt-3">
              <Button
                size="xs"
                variant="outline"
                onClick={() => {
                  setActive(new Set());
                  setSelectedBooks(new Set());
                  setSearch('');
                }}
              >
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
          colDefs={colDefs}
          visible={visible}
          getRowId={tradeRowId}
          liveSubscription={tradesSub}
          tickTopic="trades"
        />
      ),
    },
    {
      id: 'sql',
      title: 'SQL · generated',
      pin: 'right',
      render: () => (
        <SqlPanel
          title="Generated SQL"
          value={filterSql}
          readOnly
          planSummary={`${tradesSub.size} rows · ${active.size + (selectedBooks.size > 0 ? 1 : 0) + (search ? 1 : 0)} predicates`}
        />
      ),
    },
    {
      id: 'notes',
      title: 'Help · ex02.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex02.md" filename="ex02.md" source={DOCS_BY_ID['trade-blotter']} />,
    },
  ];

  // Layout proportions:
  //
  //   ┌─────────────────────────┬──────────────┐ ┐
  //   │ Filter KPIs (≈25 % H)   │              │ │
  //   ├─────────────────────────┤ Filter Chips │ │ SQL + Help pinned
  //   │                         │  (≈25 % W)   │ │ on the right edge
  //   │ Trade Tape (≈75 % H)    │              │ │
  //   │                         │              │ │
  //   └─────────────────────────┴──────────────┘ ┘
  //
  // Filter Chips is the new panel on step 2 → `size: 0.25` makes it
  // 25 % of the root split. Trade Tape is the new panel on step 3 →
  // `size: 0.75` makes it 75 % of the left column (KPIs gets 25 %).
  const layout: DockLayoutStep[] = [
    { id: 'kpis' },
    { id: 'filters', relativeTo: 'kpis', direction: 'right', size: 0.25 },
    { id: 'tape', relativeTo: 'kpis', direction: 'below', size: 0.75 },
    { id: 'sql', relativeTo: 'tape', direction: 'right' },
    { id: 'notes', relativeTo: 'sql', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

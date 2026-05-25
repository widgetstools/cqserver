import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { GridPanel } from '@/components/panels/GridPanel';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { QUERIES, type QueryEntry, type QueryFeature } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { Search, ChevronDown, ChevronRight } from 'lucide-react';
import { cn } from '@/lib/utils';
import { getPositions, getTrades } from '@/lib/data-gen';
import type { ColDef } from 'ag-grid-community';
import { fmtCcy, fmtSigned, fmtBps } from '@/lib/format';

const FEATURE_LABEL: Record<QueryFeature, string> = {
  join: 'Joins',
  filter: 'Filters',
  agg: 'Aggregations',
  pivot: 'Pivots',
  view: 'Views',
  window: 'Window Functions',
};

const FEATURE_ORDER: QueryFeature[] = ['join', 'filter', 'agg', 'pivot', 'view', 'window'];

// Tiny mock execution. Returns a precomputed result-set per query
// based on the actual seeded dataset, so even un-evaluated SQL has a
// plausible answer the user can inspect.
function executeMock(q: QueryEntry): { rows: Record<string, unknown>[]; cols: ColDef[]; elapsedMs: number } {
  const positions = getPositions() as Record<string, unknown>[];
  const trades = getTrades() as Record<string, unknown>[];
  const start = performance.now();

  let rows: Record<string, unknown>[] = [];
  let cols: ColDef[] = [];

  // Each query id maps to a small JS implementation.
  switch (q.id) {
    case 'ag-1': {
      const m = new Map<string, { book_name: string; n: number; gross: number; upnl: number; day: number; util: number; utilN: number }>();
      for (const p of positions) {
        const k = String(p.book_name);
        let r = m.get(k);
        if (!r) { r = { book_name: k, n: 0, gross: 0, upnl: 0, day: 0, util: 0, utilN: 0 }; m.set(k, r); }
        r.n += 1;
        r.gross += (p.market_value_usd as number) || 0;
        r.upnl += (p.unrealized_pnl_usd as number) || 0;
        r.day += (p.day_pnl as number) || 0;
        r.util += (p.risk_limit_utilization_pct as number) || 0;
        r.utilN += 1;
      }
      rows = Array.from(m.values()).map((r) => ({
        book_name: r.book_name,
        n_positions: r.n,
        gross_mv: r.gross,
        unrealized: r.upnl,
        day_pnl: r.day,
        avg_lim_util: r.util / Math.max(1, r.utilN),
      })).sort((a, b) => (b.day_pnl as number) - (a.day_pnl as number));
      cols = [
        { field: 'book_name', headerName: 'Book', width: 160 },
        { field: 'n_positions', headerName: 'N', width: 70, cellClass: 'tabular-cell text-right' },
        { field: 'gross_mv', headerName: 'Gross MV', valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right', width: 140 },
        { field: 'unrealized', headerName: 'Unrealized', valueFormatter: (p) => fmtSigned(p.value as number), cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}`, width: 130 },
        { field: 'day_pnl', headerName: 'Day PnL', valueFormatter: (p) => fmtSigned(p.value as number), cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}`, width: 130 },
        { field: 'avg_lim_util', headerName: 'Avg Lim Util %', valueFormatter: (p) => (p.value as number).toFixed(1) + '%', cellClass: 'tabular-cell text-right', width: 130 },
      ];
      break;
    }
    case 'ag-3': {
      const m = new Map<string, { book: string; v: Set<string>; b: Set<string>; s: Set<string> }>();
      for (const t of trades) {
        const k = String(t.book_name);
        let r = m.get(k);
        if (!r) { r = { book: k, v: new Set(), b: new Set(), s: new Set() }; m.set(k, r); }
        r.v.add(String(t.execution_venue));
        r.b.add(String(t.broker));
        r.s.add(String(t.symbol));
      }
      rows = Array.from(m.values()).map((r) => ({ book_name: r.book, n_venues: r.v.size, n_brokers: r.b.size, n_symbols: r.s.size }));
      cols = [
        { field: 'book_name', headerName: 'Book', width: 160 },
        { field: 'n_venues', headerName: 'N Venues', width: 110, cellClass: 'tabular-cell text-right' },
        { field: 'n_brokers', headerName: 'N Brokers', width: 110, cellClass: 'tabular-cell text-right' },
        { field: 'n_symbols', headerName: 'N Symbols', width: 110, cellClass: 'tabular-cell text-right' },
      ];
      break;
    }
    case 'mx-3': {
      rows = positions.filter((p) =>
        (p.var_1d_95 as number) > (p.risk_limit_var as number) ||
        (p.risk_limit_utilization_pct as number) > 90,
      );
      cols = [
        { field: 'book_name', headerName: 'Book', width: 150 },
        { field: 'symbol', headerName: 'Symbol', width: 110 },
        { field: 'position_id', headerName: 'Position ID', width: 130 },
        { field: 'market_value_usd', headerName: 'MV USD', valueFormatter: (p) => fmtSigned(p.value as number), cellClass: (p) => `tabular-cell text-right ${(p.value as number) >= 0 ? 'num-pos' : 'num-neg'}`, width: 130 },
        { field: 'risk_limit_var', headerName: 'Lim VaR', valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right', width: 130 },
        { field: 'var_1d_95', headerName: 'VaR 1d 95', valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right', width: 130 },
        { field: 'risk_limit_utilization_pct', headerName: 'Util %', valueFormatter: (p) => (p.value as number).toFixed(1) + '%', cellClass: 'tabular-cell text-right', width: 100 },
      ];
      break;
    }
    case 'mx-2': {
      const m = new Map<string, { venue: string; n: number; fees: number; gross: number }>();
      for (const t of trades) {
        const k = String(t.execution_venue);
        let r = m.get(k);
        if (!r) { r = { venue: k, n: 0, fees: 0, gross: 0 }; m.set(k, r); }
        r.n += 1;
        r.fees += (t.total_fees_usd as number) || 0;
        r.gross += (t.notional_usd as number) || 0;
      }
      rows = Array.from(m.values()).map((r) => ({
        execution_venue: r.venue,
        n_trades: r.n,
        total_fees: r.fees,
        gross_notional: r.gross,
        effective_bps: r.gross > 0 ? r.fees / r.gross * 10000 : 0,
      })).sort((a, b) => (b.total_fees as number) - (a.total_fees as number));
      cols = [
        { field: 'execution_venue', headerName: 'Venue', width: 120 },
        { field: 'n_trades', headerName: 'N', width: 80, cellClass: 'tabular-cell text-right' },
        { field: 'total_fees', headerName: 'Total Fees USD', valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right', width: 150 },
        { field: 'gross_notional', headerName: 'Gross Notional', valueFormatter: (p) => fmtCcy(p.value as number), cellClass: 'tabular-cell text-right', width: 150 },
        { field: 'effective_bps', headerName: 'Effective bps', valueFormatter: (p) => fmtBps(p.value as number, 2), cellClass: 'tabular-cell text-right', width: 130 },
      ];
      break;
    }
    case 'jn-1':
    default: {
      // Fallback: trade × position equi-join sample.
      const pIx = new Map(positions.map((p) => [p.position_id as string, p]));
      rows = trades.slice(0, 200).map((t) => ({
        position_id: t.position_id,
        symbol: t.symbol,
        book_name: pIx.get(t.position_id as string)?.book_name,
        market_value_usd: pIx.get(t.position_id as string)?.market_value_usd,
        trade_id: t.trade_id,
        side: t.side,
        trade_qty: t.quantity,
        price: t.price,
        trade_ts: t.trade_ts,
      }));
      cols = Object.keys(rows[0] ?? {}).map((k) => ({
        field: k,
        headerName: k.toUpperCase().replace(/_/g, ' '),
        width: 130,
        valueFormatter: (p: { value: unknown }) => {
          const v = p.value;
          if (typeof v === 'number') return v.toLocaleString('en-US', { maximumFractionDigits: 2 });
          return v == null ? '—' : String(v);
        },
        cellClass: (p: { value: unknown }) => typeof p.value === 'number' ? 'tabular-cell text-right' : '',
      }));
    }
  }
  return { rows, cols, elapsedMs: performance.now() - start };
}

export function QueryBuilderCanvas() {
  const [filter, setFilter] = useState('');
  const [selectedId, setSelectedId] = useState<string>(QUERIES[0]!.id);
  const [openGroups, setOpenGroups] = useState<Set<QueryFeature>>(new Set(['join', 'agg']));
  const [editorValue, setEditorValue] = useState<string>(QUERIES[0]!.sql);
  const [result, setResult] = useState<{ rows: Record<string, unknown>[]; cols: ColDef[]; elapsedMs: number; qid: string } | null>(null);

  const filtered = useMemo(() => {
    if (!filter.trim()) return QUERIES;
    const q = filter.toLowerCase();
    return QUERIES.filter(
      (qq) =>
        qq.title.toLowerCase().includes(q) ||
        qq.synopsis.toLowerCase().includes(q) ||
        qq.sql.toLowerCase().includes(q) ||
        qq.feature.includes(q),
    );
  }, [filter]);

  const groups = useMemo(() => {
    const m = new Map<QueryFeature, QueryEntry[]>();
    for (const q of filtered) {
      const arr = m.get(q.feature) ?? [];
      arr.push(q);
      m.set(q.feature, arr);
    }
    return m;
  }, [filtered]);

  const selected = QUERIES.find((q) => q.id === selectedId) ?? QUERIES[0]!;

  const select = (q: QueryEntry) => {
    setSelectedId(q.id);
    setEditorValue(q.sql);
    setResult(null);
  };

  const run = (sql: string) => {
    const r = executeMock(selected);
    setResult({ ...r, qid: selected.id });
    void sql;
  };

  const panels: DockPanelSpec[] = [
    {
      id: 'library',
      title: 'Query Library · 40+ patterns',
      render: () => (
        <PanelChrome title={`Query Library · ${QUERIES.length} patterns`}>
          <div className="p-2 border-b border-border">
            <div className="relative">
              <Search size={11} className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="Filter library…"
                className="pl-6 h-7 text-[11.5px]"
              />
            </div>
          </div>
          <div className="overflow-y-auto py-1" style={{ maxHeight: 'calc(100% - 50px)' }}>
            {FEATURE_ORDER.map((f) => {
              const list = groups.get(f) ?? [];
              if (list.length === 0) return null;
              const open = openGroups.has(f);
              return (
                <div key={f} className="mb-1">
                  <button
                    onClick={() => setOpenGroups((s) => {
                      const n = new Set(s);
                      if (n.has(f)) n.delete(f); else n.add(f);
                      return n;
                    })}
                    className="w-full flex items-center gap-1.5 px-3 py-1 text-[10px] font-mono uppercase tracking-[0.1em] text-muted-foreground hover:text-foreground"
                  >
                    {open ? <ChevronDown size={11} /> : <ChevronRight size={11} />}
                    <span>{FEATURE_LABEL[f]}</span>
                    <span className="ml-auto font-mono">{list.length}</span>
                  </button>
                  {open ? (
                    <div>
                      {list.map((q) => (
                        <div
                          key={q.id}
                          onClick={() => select(q)}
                          className={cn(
                            'px-3 py-1.5 cursor-pointer border-l-2 transition-colors',
                            q.id === selectedId
                              ? 'border-signal bg-signal-muted/40 text-foreground'
                              : 'border-transparent hover:bg-accent text-muted-foreground hover:text-foreground',
                          )}
                        >
                          <div className="text-[11.5px] font-medium leading-tight">{q.title}</div>
                          <div className="text-[10px] text-muted-foreground mt-0.5 leading-snug truncate">
                            {q.synopsis}
                          </div>
                          <div className="text-[9.5px] font-mono uppercase tracking-[0.06em] text-muted-foreground/80 mt-0.5">
                            {q.id}
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'editor',
      title: `${selected.id} · ${selected.title}`,
      render: () => (
        <SqlPanel
          title={`${selected.id} · ${selected.title}`}
          value={editorValue}
          onChange={setEditorValue}
          onRun={(sql) => run(sql)}
          planSummary={selected.explain ?? `${selected.feature.toUpperCase()} · ${selected.id}`}
        />
      ),
    },
    {
      id: 'results',
      title: 'Results',
      render: () => (
        <PanelChrome
          title="Results"
          right={
            result ? (
              <div className="flex items-center gap-1.5">
                <Badge variant="ok" className="!text-[9px]">OK</Badge>
                <span className="font-mono text-[10px] text-muted-foreground">
                  {result.rows.length} rows · {result.elapsedMs.toFixed(1)} ms
                </span>
              </div>
            ) : (
              <span className="font-mono text-[10px] text-muted-foreground">no results — press Run ▸</span>
            )
          }
        >
          {result ? (
            <GridPanel
              title=""
              rows={result.rows}
              colDefs={result.cols}
            />
          ) : (
            <div className="p-6 text-center">
              <div className="atlas-eyebrow mb-2">empty</div>
              <p className="text-[11.5px] text-muted-foreground max-w-xs mx-auto">
                Select a pattern on the left, edit if you like, then press <code className="font-mono text-signal">Run ▸</code>.
                Results display here with row count + execution time.
              </p>
            </div>
          )}
        </PanelChrome>
      ),
    },
    {
      id: 'synopsis',
      title: 'Pattern Notes',
      render: () => (
        <PanelChrome title="Pattern Notes">
          <div className="p-4 space-y-3 text-[12px]">
            <div className="atlas-eyebrow !text-[9px]">/{selected.id}</div>
            <h3 className="text-[14px] font-semibold leading-snug">{selected.title}</h3>
            <p className="text-muted-foreground leading-relaxed">{selected.synopsis}</p>
            <div className="flex flex-wrap gap-1">
              <span className="feature-tag" data-kind={selected.feature}>{selected.feature}</span>
              <Badge variant="muted">id · {selected.id}</Badge>
            </div>
            {selected.explain ? (
              <>
                <div className="atlas-eyebrow !text-[9px] mt-3">Estimated plan</div>
                <code className="block bg-muted border border-border rounded-sm px-2 py-1.5 text-[10.5px] font-mono leading-relaxed">
                  {selected.explain}
                </code>
              </>
            ) : null}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'notes',
      title: 'Notes · ex08.md',
      render: () => <MarkdownPanel title="Notes · ex08.md" filename="ex08.md" source={DOCS_BY_ID['query-builder']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'library' },
    { id: 'editor', relativeTo: 'library', direction: 'right' },
    { id: 'synopsis', relativeTo: 'editor', direction: 'right' },
    { id: 'results', relativeTo: 'editor', direction: 'below' },
    { id: 'notes', relativeTo: 'synopsis', direction: 'below' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { getPositions, getTrades } from '@/lib/data-gen';
import { POSITION_COLUMNS } from '@/lib/schema/positions';
import { TRADE_COLUMNS } from '@/lib/schema/trades';
import { buildColDefs } from '@/lib/grid-cols';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';

type JoinKind = 'equi' | 'broadcast' | 'asof';

const JOIN_SQL: Record<JoinKind, string> = {
  equi: QUERIES.find((q) => q.id === 'jn-1')!.sql,
  broadcast: QUERIES.find((q) => q.id === 'jn-3')!.sql,
  asof: QUERIES.find((q) => q.id === 'jn-4')!.sql,
};

const JOIN_LABEL: Record<JoinKind, string> = {
  equi: 'EQUI · positions × trades',
  broadcast: 'BROADCAST · trades × issuers',
  asof: 'AS OF · trades at trade_ts',
};

// Simulate each join in JS so the user sees the resulting shape.
function joinEqui(positions: Record<string, unknown>[], trades: Record<string, unknown>[]) {
  const pIx = new Map(positions.map((p) => [p.position_id as string, p]));
  return trades.slice(0, 800).map((t) => {
    const p = pIx.get(t.position_id as string);
    return {
      trade_id: t.trade_id,
      symbol: t.symbol,
      side: t.side,
      trade_qty: t.quantity,
      price: t.price,
      notional_usd: t.notional_usd,
      pos_book_name: p?.book_name,
      pos_mv_usd: p?.market_value_usd,
      pos_compliance: p?.compliance_status,
    };
  });
}

function joinBroadcast(trades: Record<string, unknown>[]) {
  // For the broadcast demo, fake an issuers table from refdata.
  return trades.slice(0, 800).map((t) => ({
    trade_id: t.trade_id,
    symbol: t.symbol,
    notional_usd: t.notional_usd,
    iss_country: t.issuer_country,
    iss_region: t.issuer_region,
    iss_sector: t.issuer_sector,
  }));
}

function joinAsOf(positions: Record<string, unknown>[], trades: Record<string, unknown>[]) {
  const pIx = new Map(positions.map((p) => [p.position_id as string, p]));
  return trades.slice(0, 800)
    .filter((t) => t.status === 'FILLED')
    .map((t) => {
      const p = pIx.get(t.position_id as string);
      return {
        trade_id: t.trade_id,
        trade_ts: t.trade_ts,
        symbol: t.symbol,
        pos_mv_at_trade: p?.market_value_usd,
        pos_lim_pct: p?.risk_limit_utilization_pct,
      };
    });
}

export function JoinsCanvas() {
  const positions = useMemo(() => getPositions() as Record<string, unknown>[], []);
  const trades = useMemo(() => getTrades() as Record<string, unknown>[], []);
  const [kind, setKind] = useState<JoinKind>('equi');

  const posDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const trdDefs = useMemo(() => buildColDefs(TRADE_COLUMNS), []);

  const joined = useMemo(() => {
    switch (kind) {
      case 'equi': return joinEqui(positions, trades);
      case 'broadcast': return joinBroadcast(trades);
      case 'asof': return joinAsOf(positions, trades);
    }
  }, [kind, positions, trades]);

  const joinedCols = useMemo(() => {
    if (!joined.length) return [];
    return Object.keys(joined[0]!).map((k) => ({
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
  }, [joined]);

  const panels: DockPanelSpec[] = [
    {
      id: 'sql',
      title: 'SQL · join',
      pin: 'right',
      render: () => (
        <PanelChrome
          title={`Join SQL · ${JOIN_LABEL[kind]}`}
          right={
            <Tabs value={kind} onValueChange={(v) => setKind(v as JoinKind)}>
              <TabsList className="border-0 gap-0">
                <TabsTrigger value="equi" className="!h-6 !px-2 !text-[9.5px]">equi</TabsTrigger>
                <TabsTrigger value="broadcast" className="!h-6 !px-2 !text-[9.5px]">broadcast</TabsTrigger>
                <TabsTrigger value="asof" className="!h-6 !px-2 !text-[9.5px]">as-of</TabsTrigger>
              </TabsList>
              <TabsContent value="equi" />
            </Tabs>
          }
        >
          <Tabs value={kind} className="h-full">
            <TabsContent value={kind} className="h-full">
              <SqlPanel title="" value={JOIN_SQL[kind]} readOnly />
            </TabsContent>
          </Tabs>
        </PanelChrome>
      ),
    },
    {
      id: 'props',
      title: 'Join Properties',
      render: () => (
        <PanelChrome title={`Join Properties · ${kind.toUpperCase()}`}>
          <div className="p-4 text-[12px] space-y-3">
            {kind === 'equi' ? (
              <>
                <div>The textbook hash-join: build a hash on positions, probe with trades.</div>
                <div className="atlas-eyebrow mb-1">Complexity</div>
                <div className="font-mono">O(|trades| + |positions|)</div>
                <div className="atlas-eyebrow mb-1">Stream semantics</div>
                <Badge variant="muted">retains updates · monotonic</Badge>
              </>
            ) : kind === 'broadcast' ? (
              <>
                <div>For tiny right-hand sides (e.g. 48 issuers), the broadcast join sends a copy to every shard so the join is local. No reshuffle.</div>
                <div className="atlas-eyebrow mb-1">When to use</div>
                <div>RHS &lt; 10k rows · low change-rate · used by many readers.</div>
                <div className="atlas-eyebrow mb-1">Refresh model</div>
                <Badge variant="muted">snapshot · re-broadcast on change</Badge>
              </>
            ) : (
              <>
                <div>Temporal join: for each trade, fetch the position state <em>as it was</em> at <code className="font-mono">trade_ts</code> — not the current state.</div>
                <div className="atlas-eyebrow mb-1">Why</div>
                <div>Compliance audits demand it. The current-state join would leak future information.</div>
                <div className="atlas-eyebrow mb-1">Backing</div>
                <Badge variant="muted">interval skip-list · ms lookups</Badge>
              </>
            )}
          </div>
        </PanelChrome>
      ),
    },
    {
      id: 'lpos',
      title: 'LHS · positions',
      render: () => (
        <GridPanel
          title="LHS · positions (sample of 500)"
          rows={positions.slice(0, 500)}
          colDefs={posDefs}
          visible={['position_id', 'book_name', 'symbol', 'asset_class', 'market_value_usd', 'compliance_status']}
        />
      ),
    },
    {
      id: 'rtrd',
      title: 'RHS · trades',
      render: () => (
        <GridPanel
          title="RHS · trades (sample of 800)"
          rows={trades.slice(0, 800)}
          colDefs={trdDefs}
          visible={['trade_id', 'position_id', 'side', 'quantity', 'price', 'notional_usd', 'status']}
        />
      ),
    },
    {
      id: 'joined',
      title: `Result · ${joined.length} rows`,
      render: () => (
        <GridPanel
          title={`Joined Result · ${joined.length} rows`}
          rows={joined as Record<string, unknown>[]}
          colDefs={joinedCols}
        />
      ),
    },
    {
      id: 'notes',
      title: 'Help · ex06.md',
      pin: 'right',
      render: () => <MarkdownPanel title="Help · ex06.md" filename="ex06.md" source={DOCS_BY_ID['joins']} />,
    },
  ];

  const layout: DockLayoutStep[] = [
    { id: 'sql' },
    { id: 'props', relativeTo: 'sql', direction: 'right' },
    { id: 'lpos', relativeTo: 'sql', direction: 'below' },
    { id: 'rtrd', relativeTo: 'lpos', direction: 'right' },
    { id: 'joined', relativeTo: 'lpos', direction: 'below' },
    { id: 'notes', relativeTo: 'joined', direction: 'right' },
  ];

  return <DockSurface panels={panels} layout={layout} />;
}

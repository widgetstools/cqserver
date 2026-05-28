import { useMemo, useState } from 'react';
import { DockSurface, type DockPanelSpec, type DockLayoutStep } from '@/components/atlas/DockSurface';
import { GridPanel } from '@/components/panels/GridPanel';
import { SqlPanel } from '@/components/panels/SqlPanel';
import { MarkdownPanel } from '@/components/panels/MarkdownPanel';
import { PanelChrome } from '@/components/panels/PanelChrome';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Badge } from '@/components/ui/badge';
import { POSITION_COLUMNS } from '@/lib/schema/positions';
import { TRADE_COLUMNS } from '@/lib/schema/trades';
import { buildColDefs } from '@/lib/grid-cols';
import { QUERIES } from '@/lib/queries/library';
import { DOCS_BY_ID } from '@/docs';
import { useFilteredSubscription } from '@/lib/use-filtered-subscription';
import type { ColDef } from 'ag-grid-community';
import { fmtBps, fmtCcy, fmtInt } from '@/lib/format';

const positionRowId = (r: Record<string, unknown>) => r.position_id as string;
const tradeRowId = (r: Record<string, unknown>) => r.trade_id as string;
const complianceRowId = (r: Record<string, unknown>) => String(r.compliance_status ?? '');

type JoinKind = 'equi' | 'broadcast' | 'asof';

const JOIN_SQL: Record<JoinKind, string> = {
  equi: `-- Wire-form (declared in cqserver.toml as /v_trades_by_compliance)
SELECT compliance_status,
       COUNT(*)                  AS n_trades,
       SUM(total_fees_usd)       AS total_fees,
       AVG(slippage_arrival_bps) AS avg_slip_arr
FROM trades JOIN positions USING (position_id)
GROUP BY compliance_status;`,
  broadcast: QUERIES.find((q) => q.id === 'jn-3')!.sql,
  asof: QUERIES.find((q) => q.id === 'jn-4')!.sql,
};

const JOIN_LABEL: Record<JoinKind, string> = {
  equi: 'EQUI · trades × positions',
  broadcast: 'BROADCAST · trades × issuers',
  asof: 'AS OF · trades at trade_ts',
};

export function JoinsCanvas() {
  // /v_trades_by_compliance is cqserver's JOIN view — `trades JOIN
  // positions USING (position_id)` aggregated by the position-side
  // `compliance_status` column. The view runner recomputes whenever
  // either side mutates, and republishes only the affected groups.
  const positionsSub = useFilteredSubscription('/positions', null);
  const tradesSub = useFilteredSubscription('/trades', null);
  const joinSub = useFilteredSubscription('/v_trades_by_compliance', null);

  // LHS / RHS samples for the source-side grids. We sample the raw
  // streams via a server-side LIMIT-equivalent: open with no filter,
  // GridPanel binds via topic= so they tick live.
  const [kind, setKind] = useState<JoinKind>('equi');

  const posDefs = useMemo(() => buildColDefs(POSITION_COLUMNS), []);
  const trdDefs = useMemo(() => buildColDefs(TRADE_COLUMNS), []);

  const joined = joinSub.rows;

  const joinedCols = useMemo<ColDef[]>(() => [
    { field: 'compliance_status', headerName: 'Compliance', width: 140 },
    {
      field: 'n_trades', headerName: 'N Trades', width: 110,
      valueFormatter: (p) => fmtInt(p.value as number),
      cellClass: 'tabular-cell text-right',
    },
    {
      field: 'total_fees', headerName: 'Total Fees USD', width: 160,
      valueFormatter: (p) => fmtCcy(p.value as number),
      cellClass: 'tabular-cell text-right',
    },
    {
      field: 'avg_slip_arr', headerName: 'Avg Slip Arr', width: 140,
      valueFormatter: (p) => fmtBps(p.value as number, 2),
      cellClass: (p) => `tabular-cell text-right ${(p.value as number) > 0 ? 'num-neg' : (p.value as number) < 0 ? 'num-pos' : ''}`,
    },
  ], []);

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
      // Pinned to the right edge so the metadata strip stays adjacent
      // to the SQL/help docs strip and the main grid keeps full width.
      pin: 'right',
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
          title="LHS · positions"
          colDefs={posDefs}
          visible={['position_id', 'book_name', 'symbol', 'asset_class', 'market_value_usd', 'compliance_status']}
          getRowId={positionRowId}
          liveSubscription={positionsSub}
        />
      ),
    },
    {
      id: 'rtrd',
      title: 'RHS · trades',
      render: () => (
        <GridPanel
          title="RHS · trades"
          colDefs={trdDefs}
          visible={['trade_id', 'position_id', 'side', 'quantity', 'price', 'notional_usd', 'status']}
          getRowId={tradeRowId}
          liveSubscription={tradesSub}
        />
      ),
    },
    {
      id: 'joined',
      title: `Result · ${joined.length} rows`,
      render: () => (
        <GridPanel
          title={`Joined Result · ${joined.length} rows`}
          colDefs={joinedCols}
          getRowId={complianceRowId}
          liveSubscription={joinSub}
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

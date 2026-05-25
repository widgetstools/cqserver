import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  type ColDef,
  type ICellRendererParams,
  ClientSideRowModelModule,
  ModuleRegistry,
  themeQuartz,
} from 'ag-grid-community';
import { AlertTriangle, ListChecks, RefreshCw, Trash2 } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi, type SubscriptionInfo } from '@/lib/admin';
import { formatCount, formatDuration, formatPercent } from '@/lib/utils';
import { useTheme } from '@/components/theme/ThemeProvider';

ModuleRegistry.registerModules([ClientSideRowModelModule]);

const SLOW_THRESHOLD = 100; // dropped frames before badge turns red
const FILL_WARN = 0.5;
const FILL_DANGER = 0.85;

export function SubscriptionsPage() {
  const { theme } = useTheme();
  const queryClient = useQueryClient();
  const [filterText, setFilterText] = useState('');

  const subs = useQuery({
    queryKey: ['subscriptions'],
    queryFn: adminApi.subscriptions,
    refetchInterval: 2_000,
  });

  const dropMutation = useMutation({
    mutationFn: (subId: string) => adminApi.dropSubscription(subId),
    onSettled: () => {
      queryClient.invalidateQueries({ queryKey: ['subscriptions'] });
    },
  });

  const rows = subs.data ?? [];

  // Aggregate health summary.
  const stats = useMemo(() => {
    let totalDropped = 0;
    let slow = 0;
    let near = 0;
    let conflated = 0;
    for (const s of rows) {
      totalDropped += s.dropped;
      if (s.dropped >= SLOW_THRESHOLD) slow++;
      if (s.fillRatio >= FILL_WARN) near++;
      if (s.conflated) conflated++;
    }
    return { totalDropped, slow, near, conflated };
  }, [rows]);

  const cols = useMemo<ColDef<SubscriptionInfo>[]>(
    () => [
      {
        field: 'subId',
        headerName: 'Sub ID',
        flex: 1.6,
        minWidth: 180,
        cellClass: 'tabular-cell',
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => (
          <span className="font-mono text-foreground">{p.value as string}</span>
        ),
      },
      {
        field: 'topic',
        headerName: 'Topic',
        flex: 1.4,
        cellClass: 'tabular-cell',
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => (
          <span className="font-mono">{p.value as string}</span>
        ),
      },
      {
        field: 'sessionId',
        headerName: 'Session',
        flex: 1.1,
        cellClass: 'tabular-cell',
      },
      {
        field: 'queueDepth',
        headerName: 'Queue',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        flex: 0.9,
        valueFormatter: (p) => `${formatCount(p.value as number)}`,
      },
      {
        field: 'fillRatio',
        headerName: 'Fill',
        type: 'rightAligned',
        flex: 0.7,
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => {
          const r = p.value as number;
          const pct = formatPercent(r * 100, 0);
          const cls =
            r >= FILL_DANGER ? 'text-err' : r >= FILL_WARN ? 'text-warn' : 'text-muted-foreground';
          return <span className={`font-mono tabular ${cls}`}>{pct}</span>;
        },
      },
      {
        field: 'dropped',
        headerName: 'Drops',
        type: 'rightAligned',
        flex: 0.8,
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => {
          const n = p.value as number;
          const cls = n >= SLOW_THRESHOLD ? 'text-err' : n > 0 ? 'text-warn' : 'text-muted-foreground';
          return <span className={`font-mono tabular ${cls}`}>{formatCount(n)}</span>;
        },
      },
      {
        field: 'ageMs',
        headerName: 'Age',
        type: 'rightAligned',
        flex: 0.7,
        cellClass: 'tabular-cell',
        valueFormatter: (p) => formatDuration(p.value as number),
      },
      {
        field: 'conflated',
        headerName: 'Conflation',
        flex: 0.9,
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => {
          if (p.value) {
            const ms = p.data?.conflationIntervalMs ?? 0;
            return <Badge variant="primary">{ms}ms</Badge>;
          }
          return <span className="text-muted-foreground text-[10.5px]">—</span>;
        },
      },
      {
        headerName: '',
        width: 60,
        cellRenderer: (p: ICellRendererParams<SubscriptionInfo>) => (
          <button
            className="inline-flex items-center justify-center h-6 w-6 rounded-sm text-muted-foreground hover:text-err hover:bg-err-muted transition-colors"
            title="Drop subscription"
            disabled={dropMutation.isPending}
            onClick={() => {
              if (
                window.confirm(
                  `Drop subscription ${p.data?.subId} on ${p.data?.topic}?`,
                )
              ) {
                dropMutation.mutate(p.data!.subId);
              }
            }}
          >
            <Trash2 size={12} />
          </button>
        ),
      },
    ],
    [dropMutation],
  );

  const defaultColDef = useMemo<ColDef>(
    () => ({
      sortable: true,
      resizable: true,
      menuTabs: ['filterMenuTab'],
    }),
    [],
  );

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <ListChecks size={16} className="text-primary" />
            Subscriptions
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            Live wire view of every active subscription.{' '}
            <span className="font-mono">{formatCount(rows.length)}</span> total.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <input
            value={filterText}
            onChange={(e) => setFilterText(e.target.value)}
            placeholder="Filter…"
            className="h-7 w-44 px-2 rounded-md border border-border bg-input text-[12px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
          />
          <Button
            variant="secondary"
            size="sm"
            onClick={() => subs.refetch()}
            disabled={subs.isFetching}
          >
            <RefreshCw size={11} className={subs.isFetching ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Summary strip */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-2.5 mb-4">
        <SummaryStat label="Active" value={formatCount(rows.length)} />
        <SummaryStat
          label="Conflated"
          value={formatCount(stats.conflated)}
          tone="muted"
        />
        <SummaryStat
          label="Near full (>50%)"
          value={formatCount(stats.near)}
          tone={stats.near > 0 ? 'warn' : 'muted'}
        />
        <SummaryStat
          label="Slow (≥ 100 drops)"
          value={formatCount(stats.slow)}
          tone={stats.slow > 0 ? 'err' : 'muted'}
        />
      </div>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2 border-b border-border">
          <CardTitle>Session routes</CardTitle>
          <span className="text-[11px] text-muted-foreground font-mono">
            polling 2s
          </span>
        </CardHeader>
        <CardContent className="p-0">
          <div
            className={theme === 'dark' ? 'ag-theme-quartz-dark' : 'ag-theme-quartz'}
            style={{ height: 'calc(100vh - 320px)', minHeight: 320 }}
          >
            <AgGridReact<SubscriptionInfo>
              theme={themeQuartz}
              rowData={rows}
              columnDefs={cols}
              defaultColDef={defaultColDef}
              animateRows={false}
              suppressCellFocus={true}
              getRowId={(p) => p.data.subId}
              quickFilterText={filterText}
            />
          </div>
        </CardContent>
      </Card>

      {/* Slow-consumer hint */}
      {stats.slow > 0 ? (
        <div className="mt-4 flex items-start gap-2 rounded-md border border-err/30 bg-err-muted px-3 py-2 text-[11.5px]">
          <AlertTriangle size={14} className="text-err shrink-0 mt-0.5" />
          <div>
            <div className="font-medium text-err mb-0.5">
              {stats.slow} slow consumer{stats.slow === 1 ? '' : 's'}
            </div>
            <div className="text-muted-foreground">
              These subscriptions have dropped ≥ 100 frames each. Consider lowering
              their query selectivity, enabling per-topic conflation, or dropping
              them via the trash icon to free encoder bandwidth.
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SummaryStat({
  label,
  value,
  tone = 'muted',
}: {
  label: string;
  value: string;
  tone?: 'muted' | 'warn' | 'err' | 'ok';
}) {
  const toneCls =
    tone === 'err'
      ? 'text-err'
      : tone === 'warn'
      ? 'text-warn'
      : tone === 'ok'
      ? 'text-ok'
      : 'text-foreground';
  return (
    <div className="flex flex-col gap-1.5 rounded-md border border-border bg-card px-3.5 py-2.5">
      <span className="text-[10.5px] uppercase tracking-[0.1em] text-muted-foreground font-medium">
        {label}
      </span>
      <span className={`font-mono tabular text-[22px] font-semibold leading-none ${toneCls}`}>
        {value}
      </span>
    </div>
  );
}

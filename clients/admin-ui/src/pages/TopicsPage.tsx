import { useQuery } from '@tanstack/react-query';
import { useMemo, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  type ColDef,
  type GridApi,
  type GridReadyEvent,
  type ICellRendererParams,
  ClientSideRowModelModule,
  themeQuartz,
} from 'ag-grid-community';
import { ModuleRegistry } from 'ag-grid-community';
import { Database, Filter, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { adminApi, type TopicInfo } from '@/lib/admin';
import { formatCount } from '@/lib/utils';
import { useTheme } from '@/components/theme/ThemeProvider';

ModuleRegistry.registerModules([ClientSideRowModelModule]);

export function TopicsPage() {
  const { theme } = useTheme();
  const [gridApi, setGridApi] = useState<GridApi | null>(null);
  const [filterText, setFilterText] = useState('');

  const topics = useQuery({
    queryKey: ['topics'],
    queryFn: adminApi.topics,
    refetchInterval: 5_000,
  });

  const rows = topics.data ?? [];

  const cols = useMemo<ColDef<TopicInfo>[]>(
    () => [
      {
        field: 'name',
        headerName: 'Topic',
        flex: 2,
        minWidth: 220,
        cellClass: 'tabular-cell',
        cellRenderer: (p: ICellRendererParams<TopicInfo>) => (
          <span className="font-mono text-foreground">{p.value}</span>
        ),
      },
      {
        field: 'rowCount',
        headerName: 'Rows',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        valueFormatter: (p) => formatCount(p.value as number),
        flex: 1,
        sort: 'desc',
      },
      {
        field: 'capacity',
        headerName: 'Capacity',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        valueFormatter: (p) => formatCount(p.value as number),
        flex: 1,
      },
      {
        field: 'columnCount',
        headerName: 'Cols',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        width: 80,
        flex: 0,
      },
      {
        field: 'keyFields',
        headerName: 'Key',
        valueFormatter: (p) =>
          Array.isArray(p.value) ? (p.value as string[]).join(', ') : '',
        flex: 1.2,
        minWidth: 130,
        cellClass: 'tabular-cell',
      },
      {
        field: 'subscriptions',
        headerName: 'Subs',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        valueFormatter: (p) => formatCount(p.value as number),
        flex: 0.7,
      },
      {
        field: 'globalVersion',
        headerName: 'Seq',
        type: 'rightAligned',
        cellClass: 'tabular-cell',
        valueFormatter: (p) => formatCount(p.value as number),
        flex: 1.1,
      },
      {
        field: 'schemaDiscovered',
        headerName: 'Schema',
        flex: 0.9,
        cellRenderer: (p: ICellRendererParams<TopicInfo>) =>
          p.value ? (
            <Badge variant="ok">discovered</Badge>
          ) : (
            <Badge variant="muted">pending</Badge>
          ),
      },
    ],
    [],
  );

  const defaultColDef = useMemo<ColDef>(
    () => ({
      sortable: true,
      resizable: true,
      filter: true,
      menuTabs: ['filterMenuTab'],
    }),
    [],
  );

  const onReady = (e: GridReadyEvent<TopicInfo>) => {
    setGridApi(e.api);
  };

  const onFilter = (q: string) => {
    setFilterText(q);
    gridApi?.setGridOption('quickFilterText', q);
  };

  const visibleRows = useMemo(() => rows.length, [rows]);

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-baseline justify-between mb-4">
        <div>
          <h1 className="text-[18px] font-semibold tracking-tight leading-none flex items-center gap-2">
            <Database size={16} className="text-primary" />
            Topics
          </h1>
          <p className="text-[11.5px] text-muted-foreground mt-1.5">
            All registered topics on this instance.{' '}
            <span className="font-mono">{formatCount(visibleRows)}</span> total.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Filter
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground"
            />
            <input
              value={filterText}
              onChange={(e) => onFilter(e.target.value)}
              placeholder="Filter…"
              data-page-filter
              className="h-7 w-44 pl-7 pr-2 rounded-md border border-border bg-input text-[12px] text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-ring"
            />
          </div>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => topics.refetch()}
            disabled={topics.isFetching}
          >
            <RefreshCw size={11} className={topics.isFetching ? 'animate-spin' : ''} />
            Refresh
          </Button>
        </div>
      </div>

      {/* Grid card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2 border-b border-border">
          <CardTitle>Topic registry</CardTitle>
          <span className="text-[11px] text-muted-foreground font-mono">
            polling 5s
          </span>
        </CardHeader>
        <CardContent className="p-0">
          <div
            className={
              theme === 'dark' ? 'ag-theme-quartz-dark' : 'ag-theme-quartz'
            }
            style={{ height: 'calc(100vh - 240px)', minHeight: 320 }}
          >
            <AgGridReact<TopicInfo>
              theme={themeQuartz}
              rowData={rows}
              columnDefs={cols}
              defaultColDef={defaultColDef}
              animateRows={false}
              suppressCellFocus={true}
              getRowId={(p) => p.data.name}
              onGridReady={onReady}
            />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

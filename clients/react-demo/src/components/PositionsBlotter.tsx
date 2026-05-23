import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  ModuleRegistry,
  AllCommunityModule,
  type ColDef,
  type GridReadyEvent,
  type GetRowIdParams,
  type ValueFormatterParams,
  type CellClassParams,
  type CellStyle,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAgGridTheme, type Palette, type ThemeMode } from '@/lib/agGridTheme';
import { useCqClient } from '@/lib/CqClientContext';

// v33+ requires explicit module registration. Doing it once at module
// load is safe — the registry guards against double-registration.
ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface PositionRow {
  positionKey: string;
  book: string;
  cusip: string;
  ticker: string;
  netQty: number;
  avgCost: number;
  lastMid: number;
  marketValue: number;
  unrealizedPnl: number;
  trades: number;
}

interface BlotterProps {
  palette: Palette;
  mode: ThemeMode;
  /** Optional — when set, parent receives row-count updates. */
  onRowCount?: (n: number) => void;
}

const fmtInt = (v: number | null | undefined) =>
  v == null ? '' : Number(v).toLocaleString();
const fmtMoney = (v: number | null | undefined) =>
  v == null
    ? ''
    : Number(v).toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
const fmtPx = (v: number | null | undefined) =>
  v == null
    ? ''
    : Number(v).toLocaleString(undefined, {
        minimumFractionDigits: 4,
        maximumFractionDigits: 4,
      });

const pnlClass = (p: CellClassParams<PositionRow, number>) => {
  const v = p.value;
  if (v == null) return undefined;
  return v > 0 ? 'pos' : v < 0 ? 'neg' : undefined;
};

const qtyClass = (p: CellClassParams<PositionRow, number>) => {
  const v = p.value;
  if (v == null) return undefined;
  return v > 0 ? 'pos' : v < 0 ? 'neg' : undefined;
};

export function PositionsBlotter({ palette, mode, onRowCount }: BlotterProps) {
  const client = useCqClient();
  const gridRef = useRef<AgGridReact<PositionRow>>(null);
  const [rowData, setRowData] = useState<PositionRow[] | undefined>(undefined);
  // Buffer live updates that arrive before the snapshot has been
  // committed to the grid — flushed once rowData is applied.
  const pendingUpdates = useRef<PositionRow[]>([]);
  const snapshotApplied = useRef(false);

  const theme = useMemo(() => getAgGridTheme(palette, mode), [palette, mode]);

  // Columns — `enableCellChangeFlash` is set on price / mark-to-market /
  // P&L columns so live updates pop visually.
  const NUM_STYLE: CellStyle = { fontVariantNumeric: 'tabular-nums' };
  const NUM_BOLD_STYLE: CellStyle = { fontVariantNumeric: 'tabular-nums', fontWeight: 500 };
  const columnDefs = useMemo<ColDef<PositionRow>[]>(
    () => [
      {
        field: 'book',
        headerName: 'Book',
        width: 180,
        pinned: 'left',
        filter: 'agTextColumnFilter',
        cellStyle: NUM_STYLE,
      },
      {
        field: 'ticker',
        headerName: 'Ticker',
        width: 170,
        pinned: 'left',
        filter: 'agTextColumnFilter',
      },
      {
        field: 'cusip',
        headerName: 'CUSIP',
        width: 110,
        filter: 'agTextColumnFilter',
      },
      {
        field: 'netQty',
        headerName: 'Net Qty',
        width: 140,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtInt(p.value),
        cellClass: qtyClass,
        cellStyle: NUM_BOLD_STYLE,
      },
      {
        field: 'avgCost',
        headerName: 'Avg Cost',
        width: 110,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtPx(p.value),
        cellStyle: NUM_STYLE,
      },
      {
        field: 'lastMid',
        headerName: 'Last Mid',
        width: 110,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtPx(p.value),
        cellStyle: NUM_BOLD_STYLE,
      },
      {
        field: 'marketValue',
        headerName: 'Market Value',
        width: 150,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtMoney(p.value),
        cellStyle: NUM_STYLE,
      },
      {
        field: 'unrealizedPnl',
        headerName: 'Unrealized P&L',
        width: 150,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtMoney(p.value),
        cellClass: pnlClass,
        cellStyle: NUM_BOLD_STYLE,
      },
      {
        field: 'trades',
        headerName: 'Trades',
        width: 90,
        type: 'numericColumn',
        filter: 'agNumberColumnFilter',
        valueFormatter: (p: ValueFormatterParams<PositionRow, number>) => fmtInt(p.value),
      },
    ],
    [],
  );

  const defaultColDef = useMemo<ColDef>(
    () => ({
      sortable: true,
      resizable: true,
      // Per the AG Grid v33 docs, set the global default for cell-change
      // flash off; individual columns above opt in. This keeps tickers /
      // qty / avgCost from flashing.
      enableCellChangeFlash: false,
      filterParams: { buttons: ['reset', 'apply'] },
    }),
    [],
  );

  // Row identity — drives applyTransaction updates.
  const getRowId = useMemo(
    () => (params: GetRowIdParams<PositionRow>) => String(params.data.positionKey),
    [],
  );

  // Subscribe on mount; tear down on unmount. The client is shared at
  // the app level — we just attach our callbacks.
  useEffect(() => {
    const unsub = client.subscribe('/positions', {
      onSnapshotStart: () => {
        snapshotApplied.current = false;
        pendingUpdates.current = [];
      },
      onSnapshot: (rows) => {
        // Initial data goes through the `rowData` prop — AG Grid takes
        // ownership and uses `getRowId` to track identity.
        setRowData(rows as unknown as PositionRow[]);
        onRowCount?.(rows.length);
      },
      onUpdate: (row) => {
        const api = gridRef.current?.api;
        // Buffer updates until the snapshot rowData has been applied.
        if (!api || !snapshotApplied.current) {
          pendingUpdates.current.push(row as unknown as PositionRow);
          return;
        }
        // Use the async transaction API — AG Grid coalesces all updates
        // arriving within `asyncTransactionWaitMillis` into a single
        // layout pass. At 1k+ updates/sec the sync variant blocks JS,
        // backs up the WS receive buffer, and the server starts dropping
        // delta frames. Async runs roughly one render per frame.
        api.applyTransactionAsync({ update: [row as unknown as PositionRow] });
      },
    });
    return unsub;
  }, [client, onRowCount]);

  const onGridReady = (_e: GridReadyEvent) => {
    // No-op for now — the subscription useEffect handles wire-up.
  };

  // After the snapshot rowData lands, mark the grid live and drain any
  // updates that arrived while we were applying it.
  useEffect(() => {
    if (rowData === undefined) return;
    const api = gridRef.current?.api;
    if (!api) return;
    snapshotApplied.current = true;
    if (pendingUpdates.current.length > 0) {
      const drained = pendingUpdates.current;
      pendingUpdates.current = [];
      api.applyTransaction({ update: drained });
    }
  }, [rowData]);

  return (
    <AgGridReact<PositionRow>
      ref={gridRef}
      theme={theme}
      rowData={rowData}
      columnDefs={columnDefs}
      defaultColDef={defaultColDef}
      getRowId={getRowId}
      onGridReady={onGridReady}
      asyncTransactionWaitMillis={60}
      rowBuffer={20}
      animateRows={false}
      sideBar={{ toolPanels: ['columns', 'filters'], defaultToolPanel: '' }}
      statusBar={{
        statusPanels: [
          { statusPanel: 'agTotalRowCountComponent', align: 'left' },
          { statusPanel: 'agFilteredRowCountComponent', align: 'left' },
          { statusPanel: 'agSelectedRowCountComponent', align: 'center' },
          { statusPanel: 'agAggregationComponent', align: 'right' },
        ],
      }}
    />
  );
}

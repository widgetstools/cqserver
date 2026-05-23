import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import type {
  ColDef,
  GetRowIdParams,
  ValueFormatterParams,
  CellStyle,
} from 'ag-grid-community';
import { getAgGridTheme, type Palette, type ThemeMode } from '@/lib/agGridTheme';
import { useCqClient } from '@/lib/CqClientContext';

interface MarketRow {
  cusip: string;
  ticker: string;
  assetClass: string;
  sector: string;
  bid: number;
  ask: number;
  mid: number;
  yieldPct: number;
}

interface Props {
  palette: Palette;
  mode: ThemeMode;
}

const fmtPx = (v: number | null | undefined) =>
  v == null
    ? ''
    : Number(v).toLocaleString(undefined, { minimumFractionDigits: 4, maximumFractionDigits: 4 });

export function MarketDataBlotter({ palette, mode }: Props) {
  const client = useCqClient();
  const gridRef = useRef<AgGridReact<MarketRow>>(null);
  const [rowData, setRowData] = useState<MarketRow[] | undefined>(undefined);
  const pendingUpdates = useRef<MarketRow[]>([]);
  const snapshotApplied = useRef(false);

  const theme = useMemo(() => getAgGridTheme(palette, mode), [palette, mode]);

  const NUM_STYLE: CellStyle = { fontVariantNumeric: 'tabular-nums' };
  const NUM_BOLD: CellStyle = { fontVariantNumeric: 'tabular-nums', fontWeight: 500 };

  const columnDefs = useMemo<ColDef<MarketRow>[]>(
    () => [
      { field: 'cusip', headerName: 'CUSIP', width: 110, filter: 'agTextColumnFilter' },
      { field: 'ticker', headerName: 'Ticker', width: 170, filter: 'agTextColumnFilter' },
      { field: 'sector', headerName: 'Sector', width: 110, filter: 'agSetColumnFilter' },
      {
        field: 'bid',
        headerName: 'Bid',
        width: 100,
        type: 'numericColumn',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<MarketRow, number>) => fmtPx(p.value),
        cellStyle: NUM_STYLE,
      },
      {
        field: 'mid',
        headerName: 'Mid',
        width: 100,
        type: 'numericColumn',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<MarketRow, number>) => fmtPx(p.value),
        cellStyle: NUM_BOLD,
      },
      {
        field: 'ask',
        headerName: 'Ask',
        width: 100,
        type: 'numericColumn',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<MarketRow, number>) => fmtPx(p.value),
        cellStyle: NUM_STYLE,
      },
      {
        field: 'yieldPct',
        headerName: 'Yield %',
        width: 100,
        type: 'numericColumn',
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<MarketRow, number>) =>
          p.value == null
            ? ''
            : Number(p.value).toLocaleString(undefined, {
                minimumFractionDigits: 3,
                maximumFractionDigits: 3,
              }),
        cellStyle: NUM_STYLE,
      },
    ],
    [],
  );

  const defaultColDef = useMemo<ColDef>(
    () => ({ sortable: true, resizable: true, enableCellChangeFlash: false }),
    [],
  );

  const getRowId = useMemo(
    () => (p: GetRowIdParams<MarketRow>) => String(p.data.cusip),
    [],
  );

  useEffect(() => {
    const unsub = client.subscribe('/fi-market-data', {
      onSnapshot: (rows) => setRowData(rows as unknown as MarketRow[]),
      onUpdate: (row) => {
        const api = gridRef.current?.api;
        if (!api || !snapshotApplied.current) {
          pendingUpdates.current.push(row as unknown as MarketRow);
          return;
        }
        api.applyTransactionAsync({ update: [row as unknown as MarketRow] });
      },
    });
    return unsub;
  }, [client]);

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
    <AgGridReact<MarketRow>
      ref={gridRef}
      theme={theme}
      rowData={rowData}
      columnDefs={columnDefs}
      defaultColDef={defaultColDef}
      getRowId={getRowId}
      asyncTransactionWaitMillis={60}
      animateRows={false}
    />
  );
}

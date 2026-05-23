import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import type {
  ColDef,
  GetRowIdParams,
  ValueFormatterParams,
  CellClassParams,
  CellStyle,
} from 'ag-grid-community';
import { getAgGridTheme, type Palette, type ThemeMode } from '@/lib/agGridTheme';
import { useCqClient } from '@/lib/CqClientContext';

interface PositionRow {
  positionKey: string;
  book: string;
  cusip: string;
  ticker: string;
  netQty: number;
  marketValue: number;
  unrealizedPnl: number;
  trades: number;
}

interface SecurityRow {
  cusip: string;
  sector: string;
}

interface TradeRow {
  tradeId: string;
  ticker: string;
  timestamp: string;
}

interface BookAggRow {
  book: string;
  unrealizedPnl: number;
  exposure: number;
  positions: number;
}

interface SectorAggRow {
  sector: string;
  exposure: number;
  positions: number;
}

interface TopPositionRow {
  positionKey: string;
  book: string;
  ticker: string;
  marketValue: number;
  unrealizedPnl: number;
}

interface TradeCountRow {
  ticker: string;
  count: number;
}

const NUM_STYLE: CellStyle = { fontVariantNumeric: 'tabular-nums' };
const NUM_BOLD: CellStyle = { fontVariantNumeric: 'tabular-nums', fontWeight: 500 };
const fmtMoney = (v: number | null | undefined) =>
  v == null
    ? ''
    : Number(v).toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const fmtInt = (v: number | null | undefined) =>
  v == null ? '' : Number(v).toLocaleString();

const pnlClass = (p: CellClassParams<unknown, number>) => {
  const v = p.value;
  if (v == null) return undefined;
  return v > 0 ? 'pos' : v < 0 ? 'neg' : undefined;
};

interface Props {
  palette: Palette;
  mode: ThemeMode;
}

export function AggregationsGrids({ palette, mode }: Props) {
  const client = useCqClient();
  const positions = useRef<Map<string, PositionRow>>(new Map());
  const securities = useRef<Map<string, SecurityRow>>(new Map());
  const trades = useRef<TradeRow[]>([]);
  // Row state for each grid — updated by the 1 Hz tick.
  const [byBook, setByBook] = useState<BookAggRow[]>([]);
  const [bySector, setBySector] = useState<SectorAggRow[]>([]);
  const [topPositions, setTopPositions] = useState<TopPositionRow[]>([]);
  const [tradeCounts, setTradeCounts] = useState<TradeCountRow[]>([]);

  const theme = useMemo(() => getAgGridTheme(palette, mode), [palette, mode]);

  // Subscriptions: snapshot /positions and /securities; deltas-only on
  // /trades because we only need the last 60s and the snapshot is huge.
  useEffect(() => {
    const unsubPos = client.subscribe('/positions', {
      onSnapshot: (rows) => {
        positions.current = new Map(
          (rows as unknown as PositionRow[]).map((p) => [p.positionKey, p]),
        );
      },
      onUpdate: (row) => {
        const p = row as unknown as PositionRow;
        positions.current.set(p.positionKey, p);
      },
    });
    const unsubSec = client.subscribe('/securities', {
      onSnapshot: (rows) => {
        securities.current = new Map(
          (rows as unknown as SecurityRow[]).map((s) => [s.cusip, s]),
        );
      },
      onUpdate: (row) => {
        const s = row as unknown as SecurityRow;
        securities.current.set(s.cusip, s);
      },
    });
    const unsubTrd = client.subscribe(
      '/trades',
      {
        onUpdate: (row) => {
          const t = row as unknown as TradeRow;
          trades.current.unshift(t);
          if (trades.current.length > 5000) trades.current.length = 5000;
        },
      },
      { deltasOnly: true },
    );

    // Recompute every second.
    const timer = setInterval(() => {
      const bookMap = new Map<string, BookAggRow>();
      const sectorMap = new Map<string, SectorAggRow>();
      const positionsArr: TopPositionRow[] = [];
      for (const p of positions.current.values()) {
        // Book
        const ba = bookMap.get(p.book) ?? {
          book: p.book,
          unrealizedPnl: 0,
          exposure: 0,
          positions: 0,
        };
        ba.unrealizedPnl += p.unrealizedPnl ?? 0;
        ba.exposure += Math.abs(p.marketValue ?? 0);
        ba.positions += 1;
        bookMap.set(p.book, ba);

        // Sector
        const sector = securities.current.get(p.cusip)?.sector ?? '—';
        const sa = sectorMap.get(sector) ?? { sector, exposure: 0, positions: 0 };
        sa.exposure += Math.abs(p.marketValue ?? 0);
        sa.positions += 1;
        sectorMap.set(sector, sa);

        positionsArr.push({
          positionKey: p.positionKey,
          book: p.book,
          ticker: p.ticker,
          marketValue: p.marketValue ?? 0,
          unrealizedPnl: p.unrealizedPnl ?? 0,
        });
      }
      setByBook(
        [...bookMap.values()].sort((a, b) => Math.abs(b.unrealizedPnl) - Math.abs(a.unrealizedPnl)),
      );
      setBySector(
        [...sectorMap.values()].sort((a, b) => b.exposure - a.exposure),
      );
      setTopPositions(
        positionsArr
          .sort((a, b) => Math.abs(b.marketValue) - Math.abs(a.marketValue))
          .slice(0, 50),
      );

      const cutoff = Date.now() - 60_000;
      const cm = new Map<string, number>();
      for (const t of trades.current) {
        const ts = new Date(t.timestamp).getTime();
        if (Number.isFinite(ts) && ts >= cutoff) {
          cm.set(t.ticker, (cm.get(t.ticker) ?? 0) + 1);
        }
      }
      setTradeCounts(
        [...cm.entries()]
          .map(([ticker, count]) => ({ ticker, count }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 25),
      );
    }, 1000);

    return () => {
      unsubPos();
      unsubSec();
      unsubTrd();
      clearInterval(timer);
    };
  }, [client]);

  const bookCols = useMemo<ColDef<BookAggRow>[]>(
    () => [
      { field: 'book', headerName: 'Book', flex: 1, minWidth: 150 },
      {
        field: 'unrealizedPnl',
        headerName: 'P&L',
        type: 'numericColumn',
        width: 130,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<BookAggRow, number>) => fmtMoney(p.value),
        cellClass: pnlClass as ColDef<BookAggRow>['cellClass'],
        cellStyle: NUM_BOLD,
        sort: 'desc',
      },
      {
        field: 'exposure',
        headerName: 'Exposure',
        type: 'numericColumn',
        width: 140,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<BookAggRow, number>) => fmtMoney(p.value),
        cellStyle: NUM_STYLE,
      },
      {
        field: 'positions',
        headerName: 'Pos',
        type: 'numericColumn',
        width: 80,
        valueFormatter: (p: ValueFormatterParams<BookAggRow, number>) => fmtInt(p.value),
        cellStyle: NUM_STYLE,
      },
    ],
    [],
  );

  const sectorCols = useMemo<ColDef<SectorAggRow>[]>(
    () => [
      { field: 'sector', headerName: 'Sector', flex: 1, minWidth: 130 },
      {
        field: 'exposure',
        headerName: 'Exposure',
        type: 'numericColumn',
        width: 140,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<SectorAggRow, number>) => fmtMoney(p.value),
        cellStyle: NUM_BOLD,
        sort: 'desc',
      },
      {
        field: 'positions',
        headerName: 'Pos',
        type: 'numericColumn',
        width: 80,
        valueFormatter: (p: ValueFormatterParams<SectorAggRow, number>) => fmtInt(p.value),
        cellStyle: NUM_STYLE,
      },
    ],
    [],
  );

  const topPositionCols = useMemo<ColDef<TopPositionRow>[]>(
    () => [
      { field: 'book', headerName: 'Book', width: 150 },
      { field: 'ticker', headerName: 'Ticker', flex: 1, minWidth: 140 },
      {
        field: 'marketValue',
        headerName: 'Market Value',
        type: 'numericColumn',
        width: 140,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<TopPositionRow, number>) => fmtMoney(p.value),
        cellStyle: NUM_STYLE,
        sort: 'desc',
        comparator: (a, b) => Math.abs(b) - Math.abs(a),
      },
      {
        field: 'unrealizedPnl',
        headerName: 'P&L',
        type: 'numericColumn',
        width: 120,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<TopPositionRow, number>) => fmtMoney(p.value),
        cellClass: pnlClass as ColDef<TopPositionRow>['cellClass'],
        cellStyle: NUM_BOLD,
      },
    ],
    [],
  );

  const tradeCountCols = useMemo<ColDef<TradeCountRow>[]>(
    () => [
      { field: 'ticker', headerName: 'Ticker', flex: 1, minWidth: 150 },
      {
        field: 'count',
        headerName: 'Trades · 60s',
        type: 'numericColumn',
        width: 130,
        enableCellChangeFlash: true,
        valueFormatter: (p: ValueFormatterParams<TradeCountRow, number>) => fmtInt(p.value),
        cellStyle: NUM_BOLD,
        sort: 'desc',
      },
    ],
    [],
  );

  const defaultColDef = useMemo<ColDef>(
    () => ({ sortable: true, resizable: true, enableCellChangeFlash: false }),
    [],
  );

  return (
    <div
      className="grid grid-cols-2 gap-2 p-2 h-full"
      style={{ gridTemplateRows: '1fr 1fr' }}
    >
      <GridCard title="P&L by book">
        <AgGridReact<BookAggRow>
          theme={theme}
          rowData={byBook}
          columnDefs={bookCols}
          defaultColDef={defaultColDef}
          getRowId={(p: GetRowIdParams<BookAggRow>) => p.data.book}
          animateRows={false}
        />
      </GridCard>
      <GridCard title="Exposure by sector">
        <AgGridReact<SectorAggRow>
          theme={theme}
          rowData={bySector}
          columnDefs={sectorCols}
          defaultColDef={defaultColDef}
          getRowId={(p: GetRowIdParams<SectorAggRow>) => p.data.sector}
          animateRows={false}
        />
      </GridCard>
      <GridCard title="Top positions">
        <AgGridReact<TopPositionRow>
          theme={theme}
          rowData={topPositions}
          columnDefs={topPositionCols}
          defaultColDef={defaultColDef}
          getRowId={(p: GetRowIdParams<TopPositionRow>) => p.data.positionKey}
          animateRows={false}
        />
      </GridCard>
      <GridCard title="Trade count · last 60s">
        <AgGridReact<TradeCountRow>
          theme={theme}
          rowData={tradeCounts}
          columnDefs={tradeCountCols}
          defaultColDef={defaultColDef}
          getRowId={(p: GetRowIdParams<TradeCountRow>) => p.data.ticker}
          animateRows={false}
        />
      </GridCard>
    </div>
  );
}

function GridCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div
      className="flex flex-col rounded-md overflow-hidden"
      style={{ background: 'var(--sf-bg-3)', border: '1px solid var(--sf-border)' }}
    >
      <div
        className="px-2 py-1.5 text-[10px] uppercase tracking-wider font-medium"
        style={{
          color: 'var(--sf-t-2)',
          borderBottom: '1px solid var(--sf-border)',
        }}
      >
        {title}
      </div>
      <div className="flex-1 min-h-0">{children}</div>
    </div>
  );
}

/**
 * Dynamic pivot demo over the live cqserver `/positions` stream.
 *
 * Data flow:
 *   1. Subscribe to `/positions` via the shared CqClient. The snapshot
 *      seeds the grid; live `publish` deltas land via
 *      `applyTransactionAsync` and pulse the affected cells.
 *   2. Subscribe to `/securities` so we can enrich each position with a
 *      `sector` field — the server's SQL doesn't (yet) do JOINs across
 *      topics, so the join happens client-side. The cqserver
 *      worklog-style SQL preview below shows the eventual server-side
 *      shape (a PIVOT over `/positions` with `sector` projected from
 *      the join'd `/securities` view).
 *   3. AG Grid pivot mode draws the table — row groups on the user's
 *      chosen row dimension, dynamic pivot columns from the chosen col
 *      dimension, and the measure aggregated by the chosen function.
 *
 * The "Underlying cqserver query" block at the top renders the
 * equivalent S19 (continuous-aggregate subscription) + S45 (dynamic
 * PIVOT) SQL the server would run if JOIN-based views were on
 * (currently deferred — see worklog S20). For now the same shape is
 * achieved client-side via the join + ag-grid pivot.
 */

import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  ModuleRegistry,
  AllCommunityModule,
  type ColDef,
  type GetRowIdParams,
  type ValueFormatterParams,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAgGridTheme, type Palette, type ThemeMode } from '@/lib/agGridTheme';
import { useCqClient } from '@/lib/CqClientContext';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

// ── Position + Security row shapes (from cqserver schemas) ────────────────────

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
  ticker?: string;
  sector?: string;
  assetClass?: string;
}

/** Row shape the grid actually consumes — positions enriched with sector / assetClass. */
interface EnrichedRow {
  positionKey: string;
  book: string;
  cusip: string;
  ticker: string;
  sector: string;
  assetClass: string;
  netQty: number;
  marketValue: number;
  unrealizedPnl: number;
  trades: number;
}

// ── Pivot config (user-toggleable) ────────────────────────────────────────────

type RowDim = 'book' | 'sector' | 'assetClass';
type ColDim = 'sector' | 'assetClass' | 'book';
type Measure =
  | 'sumMarketValue'
  | 'sumUnrealizedPnl'
  | 'sumNetQty'
  | 'sumTrades'
  | 'count';

const ROW_DIM_LABEL: Record<RowDim, string> = {
  book: 'Book',
  sector: 'Sector',
  assetClass: 'Asset class',
};
const COL_DIM_LABEL: Record<ColDim, string> = {
  sector: 'Sector',
  assetClass: 'Asset class',
  book: 'Book',
};
const MEASURE_LABEL: Record<Measure, string> = {
  sumMarketValue: 'SUM(marketValue)',
  sumUnrealizedPnl: 'SUM(unrealizedPnl)',
  sumNetQty: 'SUM(netQty)',
  sumTrades: 'SUM(trades)',
  count: 'COUNT(*)',
};

const MEASURE_FIELD: Record<Measure, keyof EnrichedRow> = {
  sumMarketValue: 'marketValue',
  sumUnrealizedPnl: 'unrealizedPnl',
  sumNetQty: 'netQty',
  sumTrades: 'trades',
  count: 'positionKey',
};

const MEASURE_AGGFUNC: Record<Measure, string> = {
  sumMarketValue: 'sum',
  sumUnrealizedPnl: 'sum',
  sumNetQty: 'sum',
  sumTrades: 'sum',
  count: 'count',
};

// ── SQL preview helper ────────────────────────────────────────────────────────

function buildSql(row: RowDim, col: ColDim, measure: Measure): string {
  const aliasMap: Record<Measure, string> = {
    sumMarketValue: 'SUM(marketValue) AS exposure',
    sumUnrealizedPnl: 'SUM(unrealizedPnl) AS pnl',
    sumNetQty: 'SUM(netQty) AS qty',
    sumTrades: 'SUM(trades) AS trade_count',
    count: 'COUNT(*) AS n',
  };
  // For `sector` / `assetClass` the field comes from /securities;
  // for `book` it lives directly on /positions. With S20 JOIN-views
  // live the server can resolve either side natively.
  const colFromView = col === 'sector' || col === 'assetClass';
  const rowFromView = row === 'sector' || row === 'assetClass';
  const fromClause = colFromView || rowFromView
    ? '"/positions" JOIN "/securities" USING (cusip)'
    : '/positions';
  return [
    `-- S19 continuous-aggregate subscription over a S45 dynamic PIVOT`,
    `-- ${colFromView || rowFromView ? 'with the S20 JOIN-view materializing the /positions × /securities join' : ''}`,
    `--   client.sow_and_subscribe_sql("/positions", sql);`,
    ``,
    `SELECT *`,
    `FROM   ${fromClause}`,
    `PIVOT  (${aliasMap[measure]}`,
    `        FOR ${col} IN (ANY))`,
    `GROUP BY ${row}`,
  ].join('\n');
}

// ── Format the AG Grid {count, value} aggregator wrapper ──────────────────────

function unwrapAgg(raw: unknown): number | null {
  let v: unknown = raw;
  if (v && typeof v === 'object' && 'value' in (v as Record<string, unknown>)) {
    v = (v as { value: unknown }).value;
  }
  if (typeof v !== 'number' || !Number.isFinite(v)) return null;
  return v;
}

const fmtCompact = (raw: unknown): string => {
  const v = unwrapAgg(raw);
  if (v == null) return '—';
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return (v / 1_000_000).toFixed(2) + 'M';
  if (abs >= 1_000) return (v / 1_000).toFixed(1) + 'k';
  return v.toFixed(0);
};

// ── Component ────────────────────────────────────────────────────────────────

interface Props {
  palette: Palette;
  mode: ThemeMode;
}

export function PivotPanel({ palette, mode }: Props) {
  const client = useCqClient();
  const gridRef = useRef<AgGridReact<EnrichedRow>>(null);

  const [row, setRow] = useState<RowDim>('book');
  const [col, setCol] = useState<ColDim>('sector');
  const [measure, setMeasure] = useState<Measure>('sumMarketValue');
  const [paused, setPaused] = useState(false);
  const [tickCount, setTickCount] = useState(0);
  const [rowCount, setRowCount] = useState(0);

  const theme = useMemo(() => getAgGridTheme(palette, mode), [palette, mode]);

  // ── Subscriptions ────────────────────────────────────────────────────────
  // Securities live in their own map so we can join positions client-side.
  const securities = useRef<Map<string, SecurityRow>>(new Map());
  // Pending position updates that arrived before the snapshot landed go
  // here; we apply them once the snapshot is in.
  const pendingUpdates = useRef<EnrichedRow[]>([]);
  const snapshotApplied = useRef(false);
  const [seedRows, setSeedRows] = useState<EnrichedRow[] | undefined>(undefined);
  // Sample-payload state for the "Sample payload" disclosure. Keep a
  // ref alongside the state so effects can detect first-fill without
  // a re-render dance.
  const [samplePayload, setSamplePayload] = useState<EnrichedRow | null>(null);
  const samplePayloadRef = useRef<EnrichedRow | null>(null);

  const enrich = (p: PositionRow): EnrichedRow => {
    const sec = securities.current.get(p.cusip);
    return {
      positionKey: p.positionKey,
      book: p.book,
      cusip: p.cusip,
      ticker: p.ticker,
      sector: sec?.sector ?? '—',
      assetClass: sec?.assetClass ?? '—',
      netQty: p.netQty,
      marketValue: p.marketValue,
      unrealizedPnl: p.unrealizedPnl,
      trades: p.trades,
    };
  };

  useEffect(() => {
    // Securities first so the snapshot enrich() can resolve sector.
    // (If positions arrive before /securities, the row gets '—' for
    // sector and will be fixed by the next update tick from
    // /positions — acceptable for a demo.)
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
    const unsubPos = client.subscribe('/positions', {
      onSnapshot: (rows) => {
        const enriched = (rows as unknown as PositionRow[]).map(enrich);
        setSeedRows(enriched);
        setRowCount(enriched.length);
      },
      onUpdate: (row) => {
        const er = enrich(row as unknown as PositionRow);
        if (paused) return;
        setTickCount((t) => t + 1);
        // Capture the latest live row for the "Sample payload"
        // disclosure. The ref lets the React render cycle stay
        // cheap (no setState per tick); the panel snapshots the
        // ref on each render below.
        samplePayloadRef.current = er;
        const api = gridRef.current?.api;
        if (!api || !snapshotApplied.current) {
          pendingUpdates.current.push(er);
          return;
        }
        api.applyTransactionAsync({ update: [er] });
      },
    });
    return () => {
      unsubPos();
      unsubSec();
    };
  }, [client, paused]);

  // Drain queued updates once the snapshot has been applied.
  useEffect(() => {
    if (seedRows === undefined) return;
    const api = gridRef.current?.api;
    if (!api) return;
    snapshotApplied.current = true;
    if (pendingUpdates.current.length > 0) {
      const drained = pendingUpdates.current;
      pendingUpdates.current = [];
      api.applyTransaction({ update: drained });
    }
  }, [seedRows]);

  // ── Column defs (derived from the dimension toggles) ─────────────────────
  const colDefs = useMemo<ColDef<EnrichedRow>[]>(() => {
    const measureField = MEASURE_FIELD[measure];
    const measureAgg = MEASURE_AGGFUNC[measure];
    return [
      {
        field: 'book',
        rowGroup: row === 'book',
        pivot: col === 'book',
        enableRowGroup: true,
        enablePivot: true,
        hide: true,
      },
      {
        field: 'sector',
        rowGroup: row === 'sector',
        pivot: col === 'sector',
        enableRowGroup: true,
        enablePivot: true,
        hide: true,
      },
      {
        field: 'assetClass',
        rowGroup: row === 'assetClass',
        pivot: col === 'assetClass',
        enableRowGroup: true,
        enablePivot: true,
        hide: true,
      },
      // Value column. With `pivotMode: true` AG Grid auto-recognizes
      // columns that carry an `aggFunc` and generates one display
      // column per distinct pivot-field value, applying the
      // agg-func across each (rowGroup × pivot) cell.
      //
      // `equals` is the load-bearing piece for "flash only on real
      // change". AG Grid's built-in `avg` (and the running variants)
      // returns a fresh `{count, value}` wrapper on every refresh;
      // without an `equals` callback AG Grid compares by reference,
      // sees a new object every tick, and lights up every cell as
      // changed — flashing the whole row/column. Compare on the
      // unwrapped numeric value so cells flash if and only if the
      // displayed number actually moved.
      {
        colId: measure,
        field: measureField,
        headerName: MEASURE_LABEL[measure],
        aggFunc: measureAgg,
        type: 'numericColumn',
        enableValue: true,
        enableCellChangeFlash: true,
        equals: (a, b) => {
          const av = unwrapAgg(a);
          const bv = unwrapAgg(b);
          if (av === null && bv === null) return true;
          if (av === null || bv === null) return false;
          return av === bv;
        },
        valueFormatter: (p: ValueFormatterParams<EnrichedRow>) => fmtCompact(p.value),
      },
    ];
  }, [row, col, measure]);

  const defaultColDef = useMemo<ColDef>(
    () => ({ sortable: true, resizable: true, filter: true, minWidth: 80 }),
    [],
  );

  const getRowId = useMemo(
    () => (p: GetRowIdParams<EnrichedRow>) => String(p.data.positionKey),
    [],
  );

  const sqlText = useMemo(() => buildSql(row, col, measure), [row, col, measure]);

  // Seed the "Sample payload" slot from the initial snapshot so it
  // shows something useful even before the first live tick.
  useEffect(() => {
    if (samplePayload === null && seedRows && seedRows.length > 0) {
      setSamplePayload(seedRows[0]);
    }
  }, [seedRows, samplePayload]);
  // Refresh the sample once a second from the ref the subscription
  // path keeps current. We don't `setState` per tick — only when
  // the user can actually see the change. 1Hz is plenty for an
  // inspection panel; cheap enough that an idle React tree won't
  // notice.
  useEffect(() => {
    const id = setInterval(() => {
      const latest = samplePayloadRef.current;
      if (latest) setSamplePayload(latest);
    }, 1000);
    return () => clearInterval(id);
  }, []);

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="flex h-full flex-col">
      {/* Controls */}
      <div
        className="flex items-center gap-3 px-3 py-1.5 text-[11px]"
        style={{
          borderBottom: '1px solid var(--sf-border)',
          background: 'var(--sf-bg-2)',
          color: 'var(--sf-t-2)',
        }}
      >
        <DimDropdown
          label="Rows"
          value={row}
          onChange={setRow}
          options={Object.entries(ROW_DIM_LABEL) as Array<[RowDim, string]>}
        />
        <DimDropdown
          label="Cols"
          value={col}
          onChange={setCol}
          options={Object.entries(COL_DIM_LABEL) as Array<[ColDim, string]>}
        />
        <DimDropdown
          label="Measure"
          value={measure}
          onChange={setMeasure}
          options={Object.entries(MEASURE_LABEL) as Array<[Measure, string]>}
        />
        <div className="ml-auto flex items-center gap-2">
          <span style={{ color: 'var(--sf-t-3)' }}>{rowCount} positions</span>
          <button
            onClick={() => setPaused((p) => !p)}
            className="text-[10px] px-2 py-0.5 rounded-full border"
            style={
              paused
                ? {
                    borderColor: 'var(--sf-warn)',
                    color: 'var(--sf-warn)',
                    background: 'var(--sf-warn-bg, transparent)',
                  }
                : {
                    borderColor: 'var(--sf-buy, #16a34a)',
                    color: 'var(--sf-buy, #16a34a)',
                  }
            }
          >
            {paused ? 'Paused' : `Live · ${tickCount} ticks`}
          </button>
        </div>
      </div>

      {/* SQL preview */}
      <details
        open
        className="shrink-0"
        style={{ borderBottom: '1px solid var(--sf-border)', background: 'var(--sf-bg-2)' }}
      >
        <summary
          className="px-3 py-1 cursor-pointer select-none text-[10px] uppercase tracking-widest"
          style={{ color: 'var(--sf-t-3)' }}
        >
          Underlying cqserver query — click to collapse
        </summary>
        <pre
          className="px-3 py-2 text-[10.5px] font-mono leading-tight overflow-x-auto m-0 whitespace-pre"
          style={{ background: 'var(--sf-bg-1)', color: 'var(--sf-t-1)' }}
        >
{sqlText}
        </pre>
      </details>

      {/* Sample payload — the literal shape consumed by the grid. */}
      <details
        className="shrink-0"
        style={{ borderBottom: '1px solid var(--sf-border)', background: 'var(--sf-bg-2)' }}
      >
        <summary
          className="px-3 py-1 cursor-pointer select-none text-[10px] uppercase tracking-widest"
          style={{ color: 'var(--sf-t-3)' }}
        >
          Sample payload (1 row / 1 Hz) — click to expand
        </summary>
        <pre
          className="px-3 py-2 text-[10.5px] font-mono leading-tight overflow-x-auto m-0 whitespace-pre"
          style={{ background: 'var(--sf-bg-1)', color: 'var(--sf-t-1)' }}
        >
{`// EnrichedRow — TypeScript interface
interface EnrichedRow {
  positionKey:    string;   // server key (book|cusip)
  book:           string;   // from /positions
  cusip:          string;   // from /positions  (= USING column)
  ticker:         string;   // from /positions
  sector:         string;   // from /securities via JOIN USING (cusip)
  assetClass:     string;   // from /securities via JOIN USING (cusip)
  netQty:         number;
  marketValue:    number;
  unrealizedPnl:  number;
  trades:         number;
}

// Live row from the feed:
${samplePayload ? JSON.stringify(samplePayload, null, 2) : '// (no row yet — connect to cqserver to start receiving)'}`}
        </pre>
      </details>

      {/* Pivot grid */}
      <div className="flex-1 min-h-0">
        <AgGridReact<EnrichedRow>
          ref={gridRef}
          theme={theme}
          rowData={seedRows}
          columnDefs={colDefs}
          defaultColDef={defaultColDef}
          getRowId={getRowId}
          pivotMode
          rowGroupPanelShow="always"
          pivotPanelShow="always"
          suppressAggFuncInHeader
          asyncTransactionWaitMillis={60}
          autoGroupColumnDef={{ minWidth: 220, pinned: 'left' }}
          animateRows={false}
        />
      </div>
    </div>
  );
}

// ── Small dropdown helper ────────────────────────────────────────────────────

function DimDropdown<T extends string>({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: T;
  onChange: (v: T) => void;
  options: Array<[T, string]>;
}) {
  return (
    <label className="flex items-center gap-1.5">
      <span
        className="uppercase tracking-widest text-[10px]"
        style={{ color: 'var(--sf-t-3)' }}
      >
        {label}
      </span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
        className="rounded px-2 py-0.5 text-[11px] focus:outline-none"
        style={{
          background: 'var(--sf-bg-1)',
          color: 'var(--sf-t-1)',
          border: '1px solid var(--sf-border)',
        }}
      >
        {options.map(([v, lbl]) => (
          <option key={v} value={v}>
            {lbl}
          </option>
        ))}
      </select>
    </label>
  );
}

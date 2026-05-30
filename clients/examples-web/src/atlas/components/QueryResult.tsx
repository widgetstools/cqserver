/**
 * QueryResult — result grid for Chapter 08, dual-mode.
 *
 * Live: SOW seeds rowData once; ticks via gridApi.applyTransaction.
 * Static: rowData set once per query run.
 */
import { useCallback, useEffect, useMemo, useRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
  type GetRowIdParams,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';
import {
  ATLAS_GRID_SIDEBAR,
  ATLAS_LIVE_DEFAULT_COL_DEF,
  ATLAS_LIVE_GRID_PROPS,
  ATLAS_STATUS_BAR,
  enrichColDefsForLiveFlash,
} from '../aggridLive';
import { useLiveGridSync } from '../hooks/useLiveGridSync';
import { useAtlasTheme } from '../theme/ThemeContext';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface QueryResultProps {
  title?: string;
  status?: string;
  liveSubscription?: SubscriptionHandle;
  staticRows?: Row[];
  getRowId: (row: Row) => string;
  /** Hide the title/status strip — status lives in the SQL toolbar instead. */
  compact?: boolean;
}

export function QueryResult({
  title,
  status,
  liveSubscription,
  staticRows,
  getRowId,
  compact,
}: QueryResultProps) {
  const { theme } = useAtlasTheme();
  const gridTheme = useMemo(() => getAtlasGridTheme(theme), [theme]);
  const apiRef = useRef<GridApi<Row> | null>(null);

  const { sowRowData, onGridReady: syncOnGridReady } = useLiveGridSync(
    liveSubscription,
    apiRef,
    getRowId,
  );

  const staticSeedRef = useRef<Row[] | undefined>(undefined);
  useEffect(() => {
    if (liveSubscription) {
      staticSeedRef.current = undefined;
      return;
    }
    const next = staticRows ?? [];
    if (next === staticSeedRef.current) return;
    staticSeedRef.current = next;
    apiRef.current?.setGridOption('rowData', next);
  }, [staticRows, liveSubscription]);

  const rowData = liveSubscription ? sowRowData : staticRows;

  const colKeySig = useMemo(() => {
    const sample = rowData?.[0];
    if (!sample) return '';
    return Object.keys(sample).sort().join('\0');
  }, [rowData]);

  // Keyed on the column signature only: the column shape is what drives
  // the defs, and in live mode `rowData` (the SOW seed) is set once but
  // its reference can churn — re-running the O(rows×cols) inference on a
  // mere reference change is wasted work. `colKeySig` already captures
  // every structural change we care about.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const colDefs = useMemo<ColDef[]>(() => inferColDefs(rowData ?? []), [colKeySig]);

  const flashColDefs = useMemo<ColDef[]>(
    () => (liveSubscription ? enrichColDefsForLiveFlash(colDefs) : colDefs),
    [colDefs, liveSubscription],
  );

  const defaultColDef = useMemo<ColDef>(
    () => (liveSubscription ? ATLAS_LIVE_DEFAULT_COL_DEF : {}),
    [liveSubscription],
  );

  const agGetRowId = useMemo(
    () => (params: GetRowIdParams<Row>) => getRowId(params.data),
    [getRowId],
  );

  const handleGridReady = useCallback(
    (e: GridReadyEvent<Row>) => {
      syncOnGridReady(e.api);
      if (!liveSubscription && staticRows && staticRows.length > 0) {
        e.api.setGridOption('rowData', staticRows);
      }
    },
    [syncOnGridReady, liveSubscription, staticRows],
  );

  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: compact ? '6px 14px 0' : '12px 18px 0',
      }}
    >
      {!compact && (title || status) && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingBottom: 10,
          }}
        >
          <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>
            {title ?? 'RESULT'}
          </div>
          {status ? (
            <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div>
          ) : null}
        </div>
      )}
      <div style={{ flex: 1, minHeight: compact ? 0 : 180, width: '100%', height: '100%' }}>
        <AgGridReact<Row>
          theme={gridTheme}
          rowData={rowData}
          columnDefs={flashColDefs}
          defaultColDef={defaultColDef}
          rowHeight={26}
          headerHeight={28}
          getRowId={agGetRowId}
          onGridReady={handleGridReady}
          statusBar={ATLAS_STATUS_BAR}
          sideBar={ATLAS_GRID_SIDEBAR}
          {...(liveSubscription ? ATLAS_LIVE_GRID_PROPS : { animateRows: false })}
        />
      </div>
    </div>
  );
}

function inferColDefs(rows: Row[]): ColDef[] {
  if (!rows || rows.length === 0) return [];
  const sample = rows[0];
  // Probe only a bounded prefix for type inference: scanning the whole
  // result set (worst case O(rows) per column when a column is mostly
  // null) is pointless — the first non-null in the first ~100 rows is a
  // reliable type witness.
  const probeRows = rows.length > 100 ? rows.slice(0, 100) : rows;
  return Object.keys(sample).map((k): ColDef => {
    const probe = probeRows.find((r) => r[k] != null)?.[k];
    const isNumber = typeof probe === 'number';
    const lk = k.toLowerCase();
    let valueFormatter: ColDef['valueFormatter'];
    if (isNumber) {
      if (/bps$/.test(lk)) {
        valueFormatter = (p) => fmtBps(p.value as number);
      } else if (/_usd$|notional|exposure|mv$|pnl|fees|var/.test(lk)) {
        valueFormatter = (p) => fmtMillions(p.value as number);
      } else {
        valueFormatter = (p) =>
          (p.value as number)?.toLocaleString('en-US', { maximumFractionDigits: 2 }) ?? '—';
      }
    }
    return {
      field: k,
      headerName: k,
      width: 140,
      type: isNumber ? 'numericColumn' : undefined,
      valueFormatter,
    };
  });
}

function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}

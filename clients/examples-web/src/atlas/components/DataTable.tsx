import { useEffect, useMemo, useRef } from 'react';
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
  ATLAS_LIVE_DEFAULT_COL_DEF,
  ATLAS_LIVE_GRID_PROPS,
  ATLAS_STATUS_BAR,
  enrichColDefsForLiveFlash,
} from '../aggridLive';
import { useLiveGridSync } from '../hooks/useLiveGridSync';
import { useAtlasTheme } from '../theme/ThemeContext';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

const EMPTY_ROWS: Row[] = [];

interface DataTableProps<T extends Row> {
  title?: string;
  status?: string;
  rows?: T[];
  colDefs: ColDef[];
  /** Stable row id — required so applyTransaction can match rows without full reloads. */
  getRowId: (row: T) => string;
  liveSubscription?: SubscriptionHandle;
}

export function DataTable<T extends Row>({
  title,
  status,
  rows,
  colDefs,
  getRowId,
  liveSubscription,
}: DataTableProps<T>) {
  const { theme } = useAtlasTheme();
  const gridTheme = useMemo(() => getAtlasGridTheme(theme), [theme]);
  const apiRef = useRef<GridApi<T> | null>(null);

  const agGetRowId = useMemo(
    () => (params: GetRowIdParams<T>) => getRowId(params.data),
    [getRowId],
  );

  const { sowRowData, onGridReady: syncOnGridReady } = useLiveGridSync<T>(
    liveSubscription,
    apiRef,
    getRowId,
  );

  const flashColDefs = useMemo<ColDef[]>(
    () => (liveSubscription ? enrichColDefsForLiveFlash(colDefs) : colDefs),
    [colDefs, liveSubscription],
  );

  const defaultColDef = useMemo<ColDef>(
    () => (liveSubscription ? ATLAS_LIVE_DEFAULT_COL_DEF : {}),
    [liveSubscription],
  );

  // Static rows: push into the grid only when the rows reference changes.
  const staticRowsRef = useRef<T[] | undefined>(undefined);
  useEffect(() => {
    if (liveSubscription) return;
    const next = rows ?? (EMPTY_ROWS as T[]);
    if (next === staticRowsRef.current) return;
    staticRowsRef.current = next;
    apiRef.current?.setGridOption('rowData', next);
  }, [rows, liveSubscription]);

  const handleGridReady = (e: GridReadyEvent<T>) => {
    syncOnGridReady(e.api);
    if (!liveSubscription && rows && rows.length > 0) {
      e.api.setGridOption('rowData', rows);
    }
  };

  const rowData = liveSubscription ? sowRowData : rows;

  return (
    <div
      style={{
        position: 'relative',
        zIndex: 1,
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '18px 24px 0',
      }}
    >
      {(title || status) && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            paddingBottom: 12,
          }}
        >
          {title ? (
            <div style={{ fontSize: 11, letterSpacing: '.22em', color: 'var(--atlas-fg-dim)' }}>{title}</div>
          ) : (
            <div />
          )}
          {status ? <div style={{ fontSize: 10, color: 'var(--atlas-fg-faint)' }}>{status}</div> : null}
        </div>
      )}
      <div style={{ flex: 1, minHeight: 280, width: '100%', height: '100%' }}>
        <AgGridReact<T>
          theme={gridTheme}
          rowData={rowData}
          columnDefs={flashColDefs}
          defaultColDef={defaultColDef}
          rowHeight={26}
          headerHeight={28}
          getRowId={agGetRowId}
          onGridReady={handleGridReady}
          statusBar={ATLAS_STATUS_BAR}
          {...(liveSubscription ? ATLAS_LIVE_GRID_PROPS : { animateRows: false })}
        />
      </div>
    </div>
  );
}

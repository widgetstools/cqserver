import { useMemo } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface DataTableProps<T extends Record<string, unknown>> {
  /** Title strip above the grid, e.g. 'POSITIONS · 23 of 207 cols'. */
  title?: string;
  /** Right-aligned status, e.g. '4,827 rows · ticking'. */
  status?: string;
  rows: T[];
  colDefs: ColDef[];
  getRowId?: (row: T) => string;
}

export function DataTable<T extends Record<string, unknown>>({
  title,
  status,
  rows,
  colDefs,
  getRowId,
}: DataTableProps<T>) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

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
          theme={theme}
          rowData={rows}
          columnDefs={colDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
        />
      </div>
    </div>
  );
}

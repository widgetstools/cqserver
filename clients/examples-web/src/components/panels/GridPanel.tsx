import { useMemo, type ReactNode } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from 'ag-grid-community';
import 'ag-grid-community/styles/ag-grid.css';
import 'ag-grid-community/styles/ag-theme-quartz.css';
import { useTheme } from '@/components/theme/ThemeProvider';
import { PanelChrome } from './PanelChrome';

ModuleRegistry.registerModules([AllCommunityModule]);

interface GridPanelProps<T> {
  title: string;
  rows: T[];
  colDefs: ColDef[];
  /** Optional filter: only columns whose `field` is in this list will be shown. */
  visible?: string[];
  right?: ReactNode;
  /** Apply row-level click → highlight. */
  onRowClick?: (row: T) => void;
}

export function GridPanel<T extends Record<string, unknown>>({
  title,
  rows,
  colDefs,
  visible,
  right,
  onRowClick,
}: GridPanelProps<T>) {
  const { theme } = useTheme();

  const cols = useMemo<ColDef[]>(() => {
    if (!visible) return colDefs;
    const ix = new Map(colDefs.map((c) => [c.field, c]));
    return visible.map((f) => ix.get(f)).filter((c): c is ColDef => !!c);
  }, [colDefs, visible]);

  return (
    <PanelChrome
      title={title}
      right={
        right ?? (
          <span className="font-mono text-[10px] text-muted-foreground">
            {rows.length.toLocaleString()} rows · {cols.length} cols
          </span>
        )
      }
    >
      <div className={`ag-theme-${theme === 'dark' ? 'quartz-dark' : 'quartz'} w-full h-full`}>
        <AgGridReact
          theme="legacy"
          rowData={rows}
          columnDefs={cols}
          rowHeight={28}
          headerHeight={28}
          suppressMovableColumns={false}
          animateRows={false}
          onRowClicked={(e) => onRowClick?.(e.data as T)}
        />
      </div>
    </PanelChrome>
  );
}

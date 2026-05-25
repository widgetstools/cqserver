import { useMemo, type ReactNode } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
} from 'ag-grid-community';
import { useTheme } from '@/components/theme/ThemeProvider';
import { getAgGridTheme } from '@/lib/aggrid-theme';
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

/**
 * GridPanel — consumes the AG Grid v33+ Theming API.
 *
 * The grid receives a `theme={...}` object built by the Stockflux
 * factory in `@/lib/aggrid-theme`, parameterised by the current
 * `(palette, mode)` from the Atlas ThemeProvider. No legacy
 * `ag-grid.css` / `ag-theme-quartz.css` imports — the v33+ API
 * generates all styling from the theme object's parameters.
 */
export function GridPanel<T extends Record<string, unknown>>({
  title,
  rows,
  colDefs,
  visible,
  right,
  onRowClick,
}: GridPanelProps<T>) {
  const { theme, palette } = useTheme();

  const cols = useMemo<ColDef[]>(() => {
    if (!visible) return colDefs;
    const ix = new Map(colDefs.map((c) => [c.field, c]));
    return visible.map((f) => ix.get(f)).filter((c): c is ColDef => !!c);
  }, [colDefs, visible]);

  const agTheme = useMemo(() => getAgGridTheme(palette, theme), [palette, theme]);

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
      <div className="w-full h-full">
        <AgGridReact
          theme={agTheme}
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

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
  /**
   * Stable row identifier extractor. When set, AG-Grid uses it to
   * track row updates across rowData re-renders (vs. naive index
   * matching). Required for `enableCellChangeFlash` to actually
   * flash cells when their value changes.
   */
  getRowId?: (row: T) => string;
  /**
   * When `getRowId` is set, list of column field names that should
   * flash on value change. Any column not in this list won't flash
   * even when its value updates.
   */
  flashColumns?: readonly string[];
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
  getRowId,
  flashColumns,
}: GridPanelProps<T>) {
  const { theme, palette } = useTheme();

  // Apply visibility filter + opt cells into cellFlash where requested.
  const cols = useMemo<ColDef[]>(() => {
    const flash = new Set(flashColumns ?? []);
    const ix = new Map(colDefs.map((c) => [c.field, c]));
    const ordered = visible ? visible.map((f) => ix.get(f)).filter((c): c is ColDef => !!c) : colDefs;
    if (!getRowId || flash.size === 0) return ordered;
    return ordered.map((c) =>
      c.field && flash.has(c.field) ? { ...c, enableCellChangeFlash: true } : c,
    );
  }, [colDefs, visible, flashColumns, getRowId]);

  const agTheme = useMemo(() => getAgGridTheme(palette, theme), [palette, theme]);

  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

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
          getRowId={agGetRowId}
          onRowClicked={(e) => onRowClick?.(e.data as T)}
        />
      </div>
    </PanelChrome>
  );
}

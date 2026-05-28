import React, { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { useTheme } from '@/components/theme/ThemeProvider';
import { getAgGridTheme } from '@/lib/aggrid-theme';
import { PanelChrome } from './PanelChrome';
import type { DeltaBatch, Row } from '@/lib/use-subscription';
import type { FilteredSubscription } from '@/lib/use-filtered-subscription';

// Enterprise module unlocks the bottom status bar (`statusBar` prop +
// `ag*Component` panels) and the row-range / aggregation widgets that
// summarise selected cells. The trial-licence watermark is acceptable
// for this demo; production deployments swap in `LicenseManager.setLicenseKey(...)`.
ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface GridPanelProps<T> {
  title: string;
  /**
   * Static row array — used only when `liveSubscription` is unset.
   * When `liveSubscription` is set, rowData is seeded from the
   * cqserver SOW and updated via `applyTransactionAsync`; this prop
   * is ignored.
   *
   * Kept optional so demo panels that drive themselves entirely
   * from a cqserver subscription don't have to pass `rows={[]}` as
   * dead placeholder.
   */
  rows?: T[];
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
   * even when its value updates. Omit to flash every column whose
   * value changes (the default).
   */
  flashColumns?: readonly string[];
  /**
   * A per-component server-side subscription (from
   * `useFilteredSubscription` or `useSubscription`). When set, the
   * grid seeds rowData once from the subscription's snapshot and
   * applies every delta batch via `applyTransactionAsync`. This is
   * the only imperative binding mode; if it's omitted the `rows`
   * prop drives the grid in React-controlled mode.
   */
  liveSubscription?: FilteredSubscription;
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
function GridPanelInner<T extends Record<string, unknown>>({
  title,
  rows,
  colDefs,
  visible,
  right,
  onRowClick,
  getRowId,
  flashColumns,
  liveSubscription,
}: GridPanelProps<T>) {
  const { theme, palette } = useTheme();

  // Apply visibility filter and per-column flash setting. AG Grid v35's
  // `defaultColDef.enableCellChangeFlash` does NOT propagate to all
  // columns when used with React + immutable data mode — flash is only
  // emitted reliably when each column carries `enableCellChangeFlash`
  // explicitly. So when the grid carries a `getRowId` (i.e., the rows
  // are live), opt every column in. `flashColumns`, if specified,
  // narrows that set.
  const cols = useMemo<ColDef[]>(() => {
    const ix = new Map(colDefs.map((c) => [c.field, c]));
    const ordered = visible
      ? visible.map((f) => ix.get(f)).filter((c): c is ColDef => !!c)
      : colDefs;
    if (!getRowId) return ordered;
    const flashSet = flashColumns ? new Set(flashColumns) : null;
    return ordered.map((c) => ({
      ...c,
      enableCellChangeFlash: flashSet
        ? c.field
          ? flashSet.has(c.field)
          : false
        : true,
    }));
  }, [colDefs, visible, flashColumns, getRowId]);

  const agTheme = useMemo(() => getAgGridTheme(palette, theme), [palette, theme]);

  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

  // Default column behaviour. enableCellChangeFlash makes every column
  // flash on value change when AG-Grid sees the row as an update
  // (requires getRowId so the row is matched, not replaced). The flash
  // CSS is provided by the Quartz theme.
  const defaultColDef = useMemo<ColDef>(
    () => ({ enableCellChangeFlash: !!getRowId }),
    [getRowId],
  );

  const isImperative = liveSubscription !== undefined;
  if (isImperative && !getRowId) {
    // Surface the misuse loudly: an imperatively-bound grid without
    // getRowId can't match transaction updates to existing rows.
    throw new Error(
      '<GridPanel liveSubscription={...}> requires getRowId — applyTransactionAsync needs a stable row id',
    );
  }
  const apiRef = useRef<GridApi<T> | null>(null);
  const [boundRows, setBoundRows] = useState<T[] | null>(null);
  // Re-seed when the bound source identity changes (filter change on
  // a FilteredSubscription rebuilds the underlying sub).
  const seededRef = useRef<unknown>(null);

  // The only imperative source is liveSubscription. When unset, the
  // grid renders the static `rows` prop in React-controlled mode.
  const source: {
    getSnapshot: () => Row[];
    getStatus: () => string;
    getSize: () => number;
    subscribeStatus: (cb: () => void) => () => void;
    subscribeBatchedDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  } | null = useMemo(() => {
    if (liveSubscription) return liveSubscription as unknown as typeof source;
    return null;
  }, [liveSubscription]);

  useEffect(() => {
    if (!source) return;

    // Re-seed when the source identity changes (e.g. a
    // FilteredSubscription tears down and rebuilds after a filter
    // toggle).
    if (seededRef.current !== source) {
      seededRef.current = source;
      // Wipe the grid: a new subscription means a fresh SOW.
      const api = apiRef.current;
      if (api) {
        api.setGridOption('rowData', []);
      }
      setBoundRows(null);
    }

    const trySeed = () => {
      if (seededRef.current !== source) return;
      if (boundRows !== null) return;
      if (source.getStatus() !== 'live' && source.getSize() === 0) return;
      setBoundRows(source.getSnapshot() as T[]);
    };
    trySeed();
    const unsubStatus = source.subscribeStatus(trySeed);

    // After the seed, every change flows through here as a batched
    // transaction. AG-Grid's async queue coalesces multiple
    // applyTransactionAsync calls per animation frame for us.
    const unsubBatch = source.subscribeBatchedDeltas((batch) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({
        add: batch.add as unknown as T[],
        update: batch.update as unknown as T[],
        remove: batch.remove as unknown as T[],
      });
    });

    return () => {
      unsubStatus();
      unsubBatch();
    };
    // boundRows intentionally not a dep — its setter inside trySeed
    // would otherwise loop the effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [source]);

  // For derived (non-topic) panels, the parent owns rowData and
  // re-renders push a new array reference. AG-Grid's prop diff is
  // fine here because the row count is small (aggregations are
  // bounded by group cardinality, joins by sample size).
  const effectiveRowData = isImperative ? boundRows ?? undefined : rows ?? [];
  const handleGridReady = (e: GridReadyEvent<T>) => {
    apiRef.current = e.api;
  };

  // Bottom status bar — surfaces total-vs-filtered counts, the active
  // selection size, and live aggregates (sum/avg/min/max/count) when
  // numeric cells are range-selected. All four panels are stock AG
  // Grid Enterprise widgets; no custom React components required.
  const statusBar = useMemo(
    () => ({
      statusPanels: [
        { statusPanel: 'agTotalAndFilteredRowCountComponent', align: 'left' as const },
        { statusPanel: 'agFilteredRowCountComponent' },
        { statusPanel: 'agSelectedRowCountComponent' },
        { statusPanel: 'agAggregationComponent', align: 'right' as const },
      ],
    }),
    [],
  );

  return (
    <PanelChrome
      title={title}
      right={
        right ?? (
          <span className="font-mono text-[10px] text-muted-foreground tabular-nums">
            {(isImperative ? (boundRows ?? []) : (rows ?? [])).length.toLocaleString()} rows · {cols.length} cols
          </span>
        )
      }
    >
         <div className="w-full h-full">
        <AgGridReact
          theme={agTheme}
          rowData={effectiveRowData}
          columnDefs={cols}
          defaultColDef={defaultColDef}
          rowHeight={28}
          headerHeight={28}
          suppressMovableColumns={false}
          animateRows={false}
          statusBar={statusBar}
          cellSelection={{ handle: { mode: 'range' } }}
          getRowId={agGetRowId}
          onGridReady={handleGridReady}
          onRowClicked={(e) => onRowClick?.(e.data as T)}
        />
      </div>
    </PanelChrome>
  );
}

// Memoized so a topic/liveSubscription-bound grid renders once (to seed
// the SOW) and thereafter updates only via applyTransactionAsync — the
// live tick counter lives in GridStatsBadge, not here. React.memo only
// holds when callers pass STABLE props (notably getRowId); see ex01.
export const GridPanel = React.memo(GridPanelInner) as typeof GridPanelInner;

/**
 * Shared AG Grid live-update helpers — prevent cell flash when values
 * are unchanged (agg wrappers, null/undefined, redundant transactions).
 */
import type { ColDef, GridOptions } from 'ag-grid-community';
import type { DeltaBatch, Row } from '@/lib/use-subscription';
import { AtlasGridSettingsPanel } from './components/AtlasGridSettingsPanel';

/** Unwrap AG Grid aggFunc result objects before comparing. */
export function unwrapCellValue(raw: unknown): unknown {
  if (raw != null && typeof raw === 'object' && 'value' in (raw as Record<string, unknown>)) {
    return (raw as { value: unknown }).value;
  }
  return raw;
}

/** ColDef.equals — compare displayed cell values, not object identity. */
export function cellValuesEqual(a: unknown, b: unknown): boolean {
  const av = unwrapCellValue(a);
  const bv = unwrapCellValue(b);
  if (av === bv) return true;
  if (av == null && bv == null) return true;
  if (typeof av === 'number' && typeof bv === 'number') {
    return av === bv || (Number.isNaN(av) && Number.isNaN(bv));
  }
  return false;
}

export function rowsDiffer(prev: Row, next: Row): boolean {
  // Hot path (runs per row, per coalesce flush). Rows are plain JSON
  // objects with a stable schema, so iterate keys directly instead of
  // allocating a Set + two Object.keys arrays on every comparison.
  for (const k in prev) {
    if (!cellValuesEqual(prev[k], next[k])) return true;
  }
  // Catch keys present only in `next` (a column that appeared). `k in
  // prev` already covered the shared keys above.
  for (const k in next) {
    if (!(k in prev) && !cellValuesEqual(prev[k], next[k])) return true;
  }
  return false;
}

export const ATLAS_LIVE_DEFAULT_COL_DEF: ColDef = {
  equals: cellValuesEqual,
};

/** Enable flash on live grids without losing value-aware equals. */
export function enrichColDefsForLiveFlash(colDefs: ColDef[]): ColDef[] {
  return colDefs.map((c) => ({
    ...c,
    enableCellChangeFlash: true,
    equals: c.equals ?? cellValuesEqual,
  }));
}

export function filterUnchangedDeltaBatch<T extends Row>(
  batch: DeltaBatch,
  getExisting: (row: T) => T | undefined,
): DeltaBatch {
  const keepUpdate = (row: T) => {
    const prev = getExisting(row);
    if (!prev) return true;
    return rowsDiffer(prev, row);
  };
  return {
    add: batch.add,
    update: batch.update.filter((r) => keepUpdate(r as T)),
    remove: batch.remove,
  };
}

/** AG Grid defaults: cellFlashDuration 500ms, cellFadeDuration 1000ms — halved here. */
export const ATLAS_LIVE_GRID_PROPS = {
  asyncTransactionWaitMillis: 60,
  animateRows: false,
  cellFlashDuration: 250,
  cellFadeDuration: 500,
} as const;

/** Bottom status bar — row counts + selection + range aggregation. */
export const ATLAS_STATUS_BAR: GridOptions['statusBar'] = {
  statusPanels: [
    { statusPanel: 'agTotalRowCountComponent', align: 'left' },
    { statusPanel: 'agFilteredRowCountComponent', align: 'left' },
    { statusPanel: 'agSelectedRowCountComponent', align: 'center' },
    { statusPanel: 'agAggregationComponent', align: 'right' },
  ],
};

/** Right side bar — custom scrollable grid settings (column visibility + layout). */
export const ATLAS_GRID_SIDEBAR: GridOptions['sideBar'] = {
  toolPanels: [
    {
      id: 'atlasGridSettings',
      labelDefault: 'Grid Settings',
      labelKey: 'gridSettings',
      iconKey: 'columns',
      toolPanel: AtlasGridSettingsPanel,
    },
  ],
  defaultToolPanel: '',
};

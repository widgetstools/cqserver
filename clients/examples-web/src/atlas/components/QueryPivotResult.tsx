/**
 * QueryPivotResult — AG Grid pivot view for AMPS PIVOT query output.
 * Server returns wide rows; we unpivot to long form for pivotMode.
 *
 * Live updates use applyTransaction on unpivoted leaf rows only — never
 * replace rowData on every tick (that re-aggregates the whole pivot and
 * flashes every cell because agg wrappers are new objects each refresh).
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
  type GetRowIdParams,
  type ValueFormatterParams,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';
import {
  ATLAS_LIVE_DEFAULT_COL_DEF,
  ATLAS_LIVE_GRID_PROPS,
  ATLAS_STATUS_BAR,
  cellValuesEqual,
  filterUnchangedDeltaBatch,
  unwrapCellValue,
} from '../aggridLive';
import { useAtlasTheme } from '../theme/ThemeContext';
import {
  unpivotRowId,
  unpivotWideRows,
  type ParsedPivotSpec,
  type PivotDisplayConfig,
} from '../scopes/pivotGrid';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface QueryPivotResultProps {
  pivotSpec: ParsedPivotSpec;
  pivotDisplay: PivotDisplayConfig;
  liveSubscription?: SubscriptionHandle;
  staticRows?: Row[];
  compact?: boolean;
}

function fmtPivotValue(raw: unknown): string {
  const v = unwrapCellValue(raw);
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  const abs = Math.abs(v);
  if (abs >= 1_000_000) return `$${(v / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `$${(v / 1_000).toFixed(1)}k`;
  return v.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

function unpivotBatch(
  wideRows: Row[],
  spec: ParsedPivotSpec,
  rowGroupFields: string[],
): Row[] {
  if (wideRows.length === 0) return [];
  return unpivotWideRows(wideRows, spec, rowGroupFields);
}

export function QueryPivotResult({
  pivotSpec,
  pivotDisplay,
  liveSubscription,
  staticRows,
  compact,
}: QueryPivotResultProps) {
  const { theme } = useAtlasTheme();
  const gridTheme = useMemo(() => getAtlasGridTheme(theme), [theme]);
  const apiRef = useRef<GridApi<Row> | null>(null);
  const seededRef = useRef(false);
  const [seedLongRows, setSeedLongRows] = useState<Row[] | undefined>(undefined);

  const rowGroupFields = pivotDisplay.rowGroupFields;
  const pivotCtx = useMemo(
    () => ({ spec: pivotSpec, rowGroupFields }),
    [pivotSpec, rowGroupFields],
  );

  // Static result: seed once per staticRows reference change.
  useEffect(() => {
    if (liveSubscription) return;
    const long = unpivotBatch(staticRows ?? [], pivotCtx.spec, pivotCtx.rowGroupFields);
    setSeedLongRows(long);
    apiRef.current?.setGridOption('rowData', long);
  }, [staticRows, liveSubscription, pivotCtx]);

  // Live: seed SOW once, then applyTransaction on unpivoted leaf rows.
  useEffect(() => {
    if (!liveSubscription) {
      seededRef.current = false;
      setSeedLongRows(undefined);
      return;
    }

    seededRef.current = false;
    setSeedLongRows(undefined);
    apiRef.current?.setGridOption('rowData', []);

    const seedFromSnapshot = () => {
      if (seededRef.current) return;
      const snap = liveSubscription.getSnapshot();
      if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
      seededRef.current = true;
      const long = unpivotBatch(snap, pivotCtx.spec, pivotCtx.rowGroupFields);
      setSeedLongRows(long);
      apiRef.current?.setGridOption('rowData', long);
    };

    seedFromSnapshot();
    const offStatus = liveSubscription.subscribeStatus(seedFromSnapshot);
    const offChunks = liveSubscription.subscribeSnapshotChunks(seedFromSnapshot);
    const offDeltas = liveSubscription.subscribeDeltas((batch) => {
      if (!seededRef.current || !apiRef.current) return;
      const { spec, rowGroupFields: groups } = pivotCtx;
      const api = apiRef.current;

      const toLong = (wide: Row[]) => unpivotBatch(wide, spec, groups);
      const getExisting = (row: Row) => {
        const id = unpivotRowId(row);
        return api.getRowNode(id)?.data as Row | undefined;
      };

      const longBatch = {
        add: toLong(batch.add),
        update: toLong(batch.update),
        remove: toLong(batch.remove),
      };
      const filtered = filterUnchangedDeltaBatch(longBatch, getExisting);

      if (
        filtered.add.length === 0 &&
        filtered.update.length === 0 &&
        filtered.remove.length === 0
      ) {
        return;
      }

      api.applyTransactionAsync(filtered);
    });

    return () => {
      offStatus();
      offChunks();
      offDeltas();
      seededRef.current = false;
    };
  }, [liveSubscription, pivotCtx]);

  const colDefs = useMemo<ColDef[]>(() => {
    const measureLabel =
      pivotSpec.measures.length === 1
        ? pivotSpec.measures[0]!.shortLabel
        : 'Value';
    const valueEquals = cellValuesEqual;
    return [
      ...rowGroupFields.map(
        (field): ColDef => ({
          field,
          rowGroup: true,
          hide: true,
          enableRowGroup: true,
          headerName: field,
        }),
      ),
      {
        field: '__pivotKey',
        pivot: true,
        hide: true,
        enablePivot: true,
        headerName: pivotSpec.pivotField,
      },
      {
        field: '__value',
        headerName: measureLabel,
        aggFunc: 'sum',
        enableValue: true,
        type: 'numericColumn',
        enableCellChangeFlash: !!liveSubscription,
        equals: valueEquals,
        valueFormatter: (p: ValueFormatterParams<Row>) => fmtPivotValue(p.value),
      },
    ];
  }, [rowGroupFields, pivotSpec, liveSubscription]);

  const defaultColDef = useMemo<ColDef>(
    () => ({
      sortable: true,
      resizable: true,
      minWidth: 88,
      filter: false,
      ...ATLAS_LIVE_DEFAULT_COL_DEF,
    }),
    [],
  );

  const agGetRowId = useMemo(
    () => (params: GetRowIdParams<Row>) => unpivotRowId(params.data),
    [],
  );

  const handleGridReady = (e: GridReadyEvent<Row>) => {
    apiRef.current = e.api;
    if (seedLongRows && seedLongRows.length > 0) {
      e.api.setGridOption('rowData', seedLongRows);
      return;
    }
    if (liveSubscription && !seededRef.current) {
      const snap = liveSubscription.getSnapshot();
      if (snap.length > 0 || liveSubscription.getStatus() === 'live') {
        seededRef.current = true;
        const long = unpivotBatch(snap, pivotCtx.spec, pivotCtx.rowGroupFields);
        setSeedLongRows(long);
        e.api.setGridOption('rowData', long);
      }
      return;
    }
    if (!liveSubscription && staticRows && staticRows.length > 0) {
      const long = unpivotBatch(staticRows, pivotCtx.spec, pivotCtx.rowGroupFields);
      setSeedLongRows(long);
      e.api.setGridOption('rowData', long);
    }
  };

  const wideCount = liveSubscription
    ? liveSubscription.size
    : (staticRows?.length ?? 0);
  const longCount = seedLongRows?.length ?? 0;
  const pivotCaption = `${rowGroupFields.join(' × ')} → ${pivotSpec.pivotField}`;

  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: compact ? '4px 14px 0' : '12px 18px 0',
      }}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 12,
          paddingBottom: compact ? 4 : 8,
          flexShrink: 0,
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
          <span
            style={{
              fontSize: 9,
              letterSpacing: '.18em',
              color: 'var(--feature-pivot, #c026d3)',
              flexShrink: 0,
            }}
          >
            PIVOT GRID
          </span>
          <span
            style={{
              fontSize: 10,
              color: 'var(--atlas-fg-faint)',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {pivotCaption}
            {pivotSpec.dynamic ? ' · dynamic IN ANY' : ''}
          </span>
        </div>
        <span style={{ fontSize: 10, color: 'var(--atlas-fg-faint)', flexShrink: 0 }}>
          {wideCount.toLocaleString()} wide → {longCount.toLocaleString()} cells
        </span>
      </div>
      <div style={{ flex: 1, minHeight: 0, width: '100%' }}>
        <AgGridReact<Row>
          theme={gridTheme}
          rowData={seedLongRows}
          columnDefs={colDefs}
          defaultColDef={defaultColDef}
          getRowId={agGetRowId}
          pivotMode
          suppressAggFuncInHeader
          rowGroupPanelShow="never"
          pivotPanelShow="never"
          autoGroupColumnDef={{ minWidth: 168, pinned: 'left' }}
          rowHeight={26}
          headerHeight={28}
          onGridReady={handleGridReady}
          statusBar={ATLAS_STATUS_BAR}
          {...ATLAS_LIVE_GRID_PROPS}
        />
      </div>
    </div>
  );
}

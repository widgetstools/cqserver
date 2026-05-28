/**
 * QueryResult — result grid for Chapter 08, dual-mode.
 *
 *   - live mode  : bound to a SubscriptionHandle from useLiveQuery;
 *                  seeds rowData from getSnapshot() then ticks via
 *                  applyTransactionAsync — same race-safe pattern
 *                  DataTable uses.
 *   - static mode: takes a flat Row[] (the SOW result of a one-shot
 *                  multi-topic JOIN) and renders frozen.
 *
 * Column defs are inferred per Run from the first row: number cols
 * get a thousands-separator formatter; *_bps gets bps; *_usd / pnl /
 * fees / notional get currency.
 */
import { useEffect, useMemo, useRef, useState } from 'react';
import { AgGridReact } from 'ag-grid-react';
import {
  AllCommunityModule,
  ModuleRegistry,
  type ColDef,
  type GridApi,
  type GridReadyEvent,
} from 'ag-grid-community';
import { AllEnterpriseModule } from 'ag-grid-enterprise';
import { getAtlasGridTheme } from '../aggrid';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface QueryResultProps {
  title?: string;
  status?: string;
  /** Set in live mode. Ignored in static mode. */
  liveSubscription?: SubscriptionHandle;
  /** Set in static mode. Ignored in live mode. */
  staticRows?: Row[];
  /** Stable row id extractor. Required in live mode. */
  getRowId?: (row: Row) => string;
}

export function QueryResult({
  title,
  status,
  liveSubscription,
  staticRows,
  getRowId,
}: QueryResultProps) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const apiRef = useRef<GridApi<Row> | null>(null);
  const seededRef = useRef<SubscriptionHandle | null>(null);
  const [boundRows, setBoundRows] = useState<Row[] | null>(null);

  // Live-mode seed + delta wiring — mirrors DataTable race-safe
  // pattern. Identity change wipes; getSnapshot drives the seed;
  // subscribeSnapshotChunks + subscribeStatus retrigger seed checks;
  // subscribeDeltas does applyTransactionAsync.
  useEffect(() => {
    if (!liveSubscription) return;
    if (seededRef.current !== liveSubscription) {
      seededRef.current = liveSubscription;
      setBoundRows(null);
      apiRef.current?.setGridOption('rowData', []);
    }
    const trySeed = () => {
      if (seededRef.current !== liveSubscription) return;
      if (boundRows !== null) return;
      const snap = liveSubscription.getSnapshot();
      if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
      setBoundRows(snap as Row[]);
    };
    trySeed();
    const offS = liveSubscription.subscribeStatus(trySeed);
    const offC = liveSubscription.subscribeSnapshotChunks(() => trySeed());
    const offD = liveSubscription.subscribeDeltas((batch) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({
        add: batch.add as Row[],
        update: batch.update as Row[],
        remove: batch.remove as Row[],
      });
    });
    return () => { offS(); offC(); offD(); };
    // boundRows intentionally excluded — see DataTable for rationale.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveSubscription]);

  // Effective rows — live mode reads from boundRows; static mode
  // takes the prop array directly.
  const effective = liveSubscription
    ? (boundRows ?? [])
    : (staticRows ?? []);

  // Cols inferred from the first row of the current dataset.
  const sampleRow = effective.length > 0 ? effective[0] : null;
  const colDefs = useMemo<ColDef[]>(
    () => inferColDefs(effective),
    // Recompute only when the sample row's identity changes — most
    // tick batches reuse the same prototype shape so this is cheap.
    [sampleRow],
  );

  // Inject per-column flash when bound to a live sub — same AG-Grid
  // v35 requirement DataTable handles.
  const flashColDefs = useMemo<ColDef[]>(
    () =>
      liveSubscription
        ? colDefs.map((c) => ({ ...c, enableCellChangeFlash: true }))
        : colDefs,
    [colDefs, liveSubscription],
  );

  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: Row }) => getRowId(data) : undefined),
    [getRowId],
  );

  return (
    <div
      style={{
        flex: 1,
        display: 'flex',
        flexDirection: 'column',
        minHeight: 0,
        padding: '12px 18px 0',
      }}
    >
      {(title || status) && (
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
      <div style={{ flex: 1, minHeight: 180, width: '100%', height: '100%' }}>
        <AgGridReact<Row>
          theme={theme}
          rowData={effective}
          columnDefs={flashColDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
          onGridReady={(e: GridReadyEvent<Row>) => { apiRef.current = e.api; }}
        />
      </div>
    </div>
  );
}

function inferColDefs(rows: Row[]): ColDef[] {
  if (!rows || rows.length === 0) return [];
  const sample = rows[0];
  return Object.keys(sample).map((k): ColDef => {
    const probe = rows.find((r) => r[k] != null)?.[k];
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

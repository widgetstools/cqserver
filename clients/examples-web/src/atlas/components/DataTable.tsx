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
import type { SubscriptionHandle } from '@/lib/use-subscription';

ModuleRegistry.registerModules([AllCommunityModule, AllEnterpriseModule]);

interface DataTableProps<T extends Record<string, unknown>> {
  /** Title strip above the grid, e.g. 'POSITIONS · 23 of 207 cols'. */
  title?: string;
  /** Right-aligned status, e.g. '4,827 rows · ticking'. */
  status?: string;
  /** Static rows. Ignored when `liveSubscription` is set. */
  rows?: T[];
  colDefs: ColDef[];
  /** Stable row id extractor — required when `liveSubscription` is set so
   *  `applyTransactionAsync({update})` can match incoming rows. */
  getRowId?: (row: T) => string;
  /**
   * Per-component cqserver subscription handle (from `useSubscription` /
   * `useFilteredSubscription`). When set, the grid:
   *   - waits for the worker to finish SOW (or has rows + 'live' status);
   *   - seeds rowData ONCE via React state (`boundRows`) so the snapshot
   *     lands without depending on chunk-listener registration timing;
   *   - applies all subsequent deltas via `applyTransactionAsync`.
   *
   * This pattern is the legacy GridPanel pattern — proven to populate
   * tiny views (≤100 rows) AND large topics (40k+) reliably without the
   * race condition that empty grids on small views had in the chunked-
   * listener-only approach.
   * `rows` is ignored when `liveSubscription` is set.
   */
  liveSubscription?: SubscriptionHandle;
}

export function DataTable<T extends Record<string, unknown>>({
  title,
  status,
  rows,
  colDefs,
  getRowId,
  liveSubscription,
}: DataTableProps<T>) {
  const theme = useMemo(() => getAtlasGridTheme(), []);
  const agGetRowId = useMemo(
    () => (getRowId ? ({ data }: { data: T }) => getRowId(data) : undefined),
    [getRowId],
  );

  const apiRef = useRef<GridApi<T> | null>(null);
  const seededRef = useRef<SubscriptionHandle | null>(null);
  const [boundRows, setBoundRows] = useState<T[] | null>(null);

  useEffect(() => {
    if (!liveSubscription) return;

    // Subscription identity changed (e.g. filter swap rebuilt the sub):
    // wipe the grid and reset the seed gate.
    if (seededRef.current !== liveSubscription) {
      seededRef.current = liveSubscription;
      setBoundRows(null);
      const api = apiRef.current;
      if (api) api.setGridOption('rowData', []);
    }

    // Seed gate. The Sub's row mirror always reflects the worker's
    // current snapshot, so we can safely read it whenever:
    //   - the sub is 'live' (SOW done), OR
    //   - rows have already started arriving (so the partial SOW is
    //     visible immediately, then later snapshot chunks land via
    //     this same callback).
    const trySeed = () => {
      if (seededRef.current !== liveSubscription) return;
      if (boundRows !== null) return;
      const snap = liveSubscription.getSnapshot();
      if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
      setBoundRows(snap as T[]);
    };
    trySeed();
    const unsubStatus = liveSubscription.subscribeStatus(trySeed);
    const unsubChunks = liveSubscription.subscribeSnapshotChunks(() => trySeed());

    // Post-seed live deltas — coalesced 50ms batches from the worker.
    const unsubDeltas = liveSubscription.subscribeDeltas((batch) => {
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
      unsubChunks();
      unsubDeltas();
    };
    // boundRows intentionally excluded — its setter inside trySeed would
    // otherwise loop the effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [liveSubscription]);

  const handleGridReady = (e: GridReadyEvent<T>) => {
    apiRef.current = e.api;
  };

  // When liveSubscription is set, boundRows drives rowData (null until
  // seed; then the snapshot array reference). Deltas after seed apply
  // through `applyTransactionAsync` against AG-Grid's internal state —
  // they don't re-trigger React rendering.
  const effectiveRowData = liveSubscription
    ? (boundRows ?? undefined)
    : (rows ?? []);

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
          rowData={effectiveRowData}
          columnDefs={colDefs}
          rowHeight={26}
          headerHeight={28}
          animateRows={false}
          getRowId={agGetRowId}
          onGridReady={handleGridReady}
        />
      </div>
    </div>
  );
}

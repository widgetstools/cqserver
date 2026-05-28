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
   *   - seeds itself by consuming `subscribeSnapshotChunks` and calling
   *     `applyTransactionAsync({add: chunk})` per chunk;
   *   - applies live deltas via `subscribeDeltas` once SOW completes.
   * `rows` is ignored.
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
  const [seeded, setSeeded] = useState(false);

  // Imperative wiring: only runs when liveSubscription is set.
  useEffect(() => {
    if (!liveSubscription) return;
    // Subscription identity changed (e.g. filter swap rebuilt the sub).
    if (seededRef.current !== liveSubscription) {
      seededRef.current = liveSubscription;
      setSeeded(false);
      const api = apiRef.current;
      if (api) api.setGridOption('rowData', []);
    }
    const unsubChunks = liveSubscription.subscribeSnapshotChunks((chunk, more) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({ add: chunk as unknown as T[] });
      if (!more) setSeeded(true);
    });
    const unsubDeltas = liveSubscription.subscribeDeltas((batch) => {
      const api = apiRef.current;
      if (!api) return;
      api.applyTransactionAsync({
        add: batch.add as unknown as T[],
        update: batch.update as unknown as T[],
        remove: batch.remove as unknown as T[],
      });
    });
    // Replay any chunks that landed before we attached: if the worker
    // already finished SOW for this handle, the snapshot accessor still
    // has them all — apply them in one shot.
    if (liveSubscription.getStatus() === 'live' && !seeded) {
      const snap = liveSubscription.getSnapshot();
      if (snap.length > 0) {
        const api = apiRef.current;
        if (api) {
          api.applyTransactionAsync({ add: snap as unknown as T[] });
          setSeeded(true);
        }
      }
    }
    return () => {
      unsubChunks();
      unsubDeltas();
    };
  }, [liveSubscription, seeded]);

  const handleGridReady = (e: GridReadyEvent<T>) => {
    apiRef.current = e.api;
  };

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
          rowData={liveSubscription ? undefined : (rows ?? [])}
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

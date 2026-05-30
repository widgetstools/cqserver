/**
 * Wire a cqserver live subscription to an AG Grid instance:
 *   1. Seed rowData exactly once when SOW completes (via rowData prop
 *      and/or gridApi.setGridOption).
 *   2. Apply every subsequent delta through gridApi.applyTransactionAsync
 *      only — never refresh rowData from subscription snapshots.
 *   3. Drop update rows whose cell values are unchanged so flash only
 *      fires on cells that actually moved.
 */
import { useEffect, useRef, useState, type RefObject } from 'react';
import type { GridApi } from 'ag-grid-community';
import { filterUnchangedDeltaBatch } from '../aggridLive';
import type { SubscriptionHandle, Row } from '@/lib/use-subscription';

export function useLiveGridSync<T extends Row>(
  liveSubscription: SubscriptionHandle | undefined,
  apiRef: RefObject<GridApi<T> | null>,
  getRowId?: (row: T) => string,
) {
  const subRef = useRef<SubscriptionHandle | null>(null);
  const seededRef = useRef(false);
  const getRowIdRef = useRef(getRowId);
  getRowIdRef.current = getRowId;

  /** Written once at SOW; passed to AgGridReact rowData and never updated for deltas. */
  const [sowRowData, setSowRowData] = useState<T[] | undefined>(undefined);

  useEffect(() => {
    if (!liveSubscription) {
      subRef.current = null;
      seededRef.current = false;
      setSowRowData(undefined);
      return;
    }

    if (subRef.current !== liveSubscription) {
      subRef.current = liveSubscription;
      seededRef.current = false;
      setSowRowData(undefined);
      apiRef.current?.setGridOption('rowData', [] as T[]);
    }

    const seedFromSnapshot = () => {
      if (seededRef.current) return;
      const snap = liveSubscription.getSnapshot() as T[];
      if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
      seededRef.current = true;
      setSowRowData(snap);
      apiRef.current?.setGridOption('rowData', snap);
    };

    seedFromSnapshot();
    const offStatus = liveSubscription.subscribeStatus(seedFromSnapshot);
    const offChunks = liveSubscription.subscribeSnapshotChunks(seedFromSnapshot);
    const offDeltas = liveSubscription.subscribeDeltas((batch) => {
      if (!seededRef.current) return;
      const api = apiRef.current;
      if (!api) return;

      const rowIdFn = getRowIdRef.current;
      const filtered =
        rowIdFn != null
          ? filterUnchangedDeltaBatch<T>(batch, (row) => {
              const id = rowIdFn(row);
              const node = api.getRowNode(id);
              return node?.data;
            })
          : batch;

      if (
        filtered.add.length === 0 &&
        filtered.update.length === 0 &&
        filtered.remove.length === 0
      ) {
        return;
      }

      api.applyTransactionAsync({
        add: filtered.add as T[],
        update: filtered.update as T[],
        remove: filtered.remove as T[],
      });
    });

    return () => {
      offStatus();
      offChunks();
      offDeltas();
    };
  }, [liveSubscription, apiRef]);

  const onGridReady = (api: GridApi<T>) => {
    apiRef.current = api;
    if (!liveSubscription || seededRef.current) return;
    const snap = liveSubscription.getSnapshot() as T[];
    if (liveSubscription.getStatus() !== 'live' && snap.length === 0) return;
    seededRef.current = true;
    setSowRowData(snap);
    api.setGridOption('rowData', snap);
  };

  return { sowRowData, onGridReady };
}

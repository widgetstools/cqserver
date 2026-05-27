/**
 * `useLiveQuery` — open a per-component cqserver subscription whose
 * SOW + delta stream are driven by AD-HOC SQL on any topic. Each
 * Run-button press tears down the previous subscription and opens a
 * new one. The result is a `FilteredSubscription`-shaped object that
 * plugs straight into `<GridPanel liveSubscription={…}>` — same
 * imperative SOW-seed + `applyTransactionAsync` path the rest of the
 * Atlas grids use.
 *
 * Why this exists (versus the existing `useFilteredSubscription`):
 *   - `useFilteredSubscription` hard-codes the topic enum + a fixed
 *     `KEY_OF` map per topic. Aggregate / JOIN / GROUP BY queries
 *     don't have a stable per-topic row id — the caller supplies the
 *     extractor that matches the SELECT-list shape.
 *   - cqserver supports `sowAndSubscribe(topic, { sql })` so the
 *     server runs the same continuous-aggregate evaluator that
 *     materializes the demo views; the browser just receives one
 *     delta per group whenever it changes. No client-side
 *     aggregation, no client-side shaping — same architectural
 *     promise as every other Atlas tab.
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import type { Delta } from '@cqserver/client';
import {
  type ConnectionStatus,
  type DeltaBatch,
  type Row,
  getCqClient,
  onCqClientChange,
} from './cq-store';
import type { FilteredSubscription } from './use-filtered-subscription';

const COALESCE_MS = 50;

export interface LiveQuerySpec {
  /** Topic the SQL targets (server-side topic resolution is by the
   *  envelope, not the FROM clause). Example: '/positions'. */
  topic: string;
  /** Full SELECT clause. Use `FROM t` — cqserver rewrites it. */
  sql: string;
  /** Stable row-id extractor matching the SELECT-list shape. */
  getRowId: (row: Row) => string;
}

class LiveQuerySub {
  private rows = new Map<string, Row>();
  private snap: Row[] = [];
  private listeners = new Set<() => void>();
  private dirty = false;
  private coalesceTimer: ReturnType<typeof setTimeout> | null = null;
  private status: ConnectionStatus = 'idle';
  private statusListeners = new Set<() => void>();
  private batchListeners = new Set<(b: DeltaBatch) => void>();
  private pendingAdds: Row[] = [];
  private pendingUpdates: Row[] = [];
  private pendingRemoves: Row[] = [];
  private livePhase = false;
  private currentSubId: string | null = null;
  private abortController: AbortController | null = null;
  private clientUnsubscribe: (() => void) | null = null;
  private closed = false;
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private errorMessage: string | null = null;
  private errorListeners = new Set<() => void>();

  constructor(
    private readonly topic: string,
    private readonly sql: string,
    private readonly getRowId: (row: Row) => string,
  ) {
    this.clientUnsubscribe = onCqClientChange(() => this.rebind());
    this.rebind();
  }

  // ─── Public API ──────────────────────────────────────────────
  subscribe = (cb: () => void): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };

  subscribeStatus = (cb: () => void): (() => void) => {
    this.statusListeners.add(cb);
    return () => {
      this.statusListeners.delete(cb);
    };
  };

  subscribeBatchedDeltas = (cb: (b: DeltaBatch) => void): (() => void) => {
    this.batchListeners.add(cb);
    return () => {
      this.batchListeners.delete(cb);
    };
  };

  subscribeError = (cb: () => void): (() => void) => {
    this.errorListeners.add(cb);
    return () => {
      this.errorListeners.delete(cb);
    };
  };

  getSnapshot = (): Row[] => this.snap;
  getStatus = (): ConnectionStatus => this.status;
  getSize = (): number => this.rows.size;
  getError = (): string | null => this.errorMessage;

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.clientUnsubscribe?.();
    this.clientUnsubscribe = null;
    this.abortCurrent();
    if (this.coalesceTimer != null) {
      clearTimeout(this.coalesceTimer);
      this.coalesceTimer = null;
    }
    if (this.closeTimer != null) {
      clearTimeout(this.closeTimer);
      this.closeTimer = null;
    }
  }

  scheduleClose(): void {
    if (this.closed || this.closeTimer != null) return;
    this.closeTimer = setTimeout(() => {
      this.closeTimer = null;
      this.close();
    }, 100);
  }

  cancelDeferredClose(): void {
    if (this.closeTimer != null) {
      clearTimeout(this.closeTimer);
      this.closeTimer = null;
    }
  }

  // ─── Lifecycle ───────────────────────────────────────────────
  private async rebind(): Promise<void> {
    this.abortCurrent();
    if (this.closed) return;
    const client = getCqClient();
    if (!client) {
      this.setStatus('connecting');
      return;
    }
    const abort = new AbortController();
    this.abortController = abort;
    this.setStatus('snapshotting');
    this.rows.clear();
    this.snap = [];
    this.pendingAdds = [];
    this.pendingUpdates = [];
    this.pendingRemoves = [];
    this.livePhase = false;
    this.setError(null);

    try {
      const sub = await client.sowAndSubscribe(this.topic, { sql: this.sql });
      if (abort.signal.aborted) {
        await client.unsubscribe(sub.subId).catch(() => {});
        return;
      }
      this.currentSubId = sub.subId;
      void sub.whenSnapshotComplete().then(() => {
        if (abort.signal.aborted) return;
        this.snap = Array.from(this.rows.values());
        this.notifyAll();
        this.livePhase = true;
        this.setStatus('live');
      });
      for await (const delta of sub) {
        if (abort.signal.aborted) break;
        this.applyDelta(delta);
      }
      if (!abort.signal.aborted) this.setStatus('disconnected');
    } catch (err) {
      if (!abort.signal.aborted) {
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[useLiveQuery] ${this.topic} sub failed`, this.sql, err);
        this.setError(msg);
        this.setStatus('disconnected');
      }
    }
  }

  private abortCurrent(): void {
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
    if (this.currentSubId) {
      const client = getCqClient();
      const sid = this.currentSubId;
      if (client) void client.unsubscribe(sid).catch(() => {});
      this.currentSubId = null;
    }
  }

  private applyDelta(d: Delta): void {
    const data = d.data as Row;
    const key = this.getRowId(data);
    if (d.deltaType === 'remove' || d.deltaType === 'oof') {
      this.rows.delete(key);
      if (this.livePhase) this.pendingRemoves.push(data);
    } else if (d.deltaType === 'add') {
      this.rows.set(key, data);
      if (this.livePhase) this.pendingAdds.push(data);
    } else {
      this.rows.set(key, data);
      if (this.livePhase) this.pendingUpdates.push(data);
    }
    this.scheduleFlush();
  }

  private scheduleFlush(): void {
    this.dirty = true;
    if (this.coalesceTimer != null) return;
    this.coalesceTimer = setTimeout(() => {
      this.coalesceTimer = null;
      if (this.dirty) {
        this.snap = Array.from(this.rows.values());
        this.dirty = false;
        for (const cb of this.listeners) cb();
      }
      this.drainBatch();
    }, COALESCE_MS);
  }

  private drainBatch(): void {
    if (
      this.pendingAdds.length === 0 &&
      this.pendingUpdates.length === 0 &&
      this.pendingRemoves.length === 0
    ) {
      return;
    }
    const batch: DeltaBatch = {
      add: this.pendingAdds,
      update: this.pendingUpdates,
      remove: this.pendingRemoves,
    };
    this.pendingAdds = [];
    this.pendingUpdates = [];
    this.pendingRemoves = [];
    for (const cb of this.batchListeners) cb(batch);
  }

  private setStatus(s: ConnectionStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb();
  }

  private setError(msg: string | null): void {
    if (this.errorMessage === msg) return;
    this.errorMessage = msg;
    for (const cb of this.errorListeners) cb();
  }

  private notifyAll(): void {
    for (const cb of this.listeners) cb();
  }
}

export interface LiveQueryHandle extends FilteredSubscription {
  /** Server-side error string if the subscription couldn't start
   *  (e.g. parser error, unknown column). null if alive. */
  error: string | null;
}

/**
 * Open a live cqserver query. Pass `null` to clear the current
 * subscription (e.g. before the user presses Run). Re-runs by
 * changing `spec` to a new `{topic, sql, getRowId}` triple.
 */
export function useLiveQuery(spec: LiveQuerySpec | null): LiveQueryHandle | null {
  const [sub, setSub] = useState<LiveQuerySub | null>(() =>
    spec ? new LiveQuerySub(spec.topic, spec.sql, spec.getRowId) : null,
  );
  const keyRef = useRef<LiveQuerySpec | null>(spec);

  useEffect(() => {
    const same =
      keyRef.current?.topic === spec?.topic &&
      keyRef.current?.sql === spec?.sql &&
      keyRef.current?.getRowId === spec?.getRowId;
    if (same) return;
    keyRef.current = spec;
    setSub((prev) => {
      prev?.close();
      return spec ? new LiveQuerySub(spec.topic, spec.sql, spec.getRowId) : null;
    });
  }, [spec]);

  useEffect(() => {
    sub?.cancelDeferredClose();
    return () => {
      sub?.scheduleClose();
    };
  }, [sub]);

  // Hook subscriptions for React reactivity. When `sub` is null we
  // still need to call hooks unconditionally — pass through a noop
  // subscriber so the hook order stays stable.
  const noop = (): (() => void) => () => {};
  const zero = (): number => 0;
  const idleStatus = (): ConnectionStatus => 'idle';
  const rowsVersion = useSyncExternalStore(
    sub?.subscribe ?? noop,
    sub ? () => sub.getSnapshot().length : zero,
    zero,
  );
  const status = useSyncExternalStore(
    sub?.subscribeStatus ?? noop,
    sub ? sub.getStatus : idleStatus,
    idleStatus,
  );
  const error = useSyncExternalStore(
    sub?.subscribeError ?? noop,
    sub ? sub.getError : () => null,
    () => null,
  );

  return useMemo<LiveQueryHandle | null>(() => {
    if (!sub) return null;
    return {
      rows: sub.getSnapshot(),
      status,
      size: sub.getSize(),
      subscribeBatchedDeltas: sub.subscribeBatchedDeltas,
      subscribeStatus: sub.subscribeStatus,
      getSnapshot: sub.getSnapshot,
      getStatus: sub.getStatus,
      getSize: sub.getSize,
      error,
    };
    // `rowsVersion` is the React-reactivity trigger (row count proxy)
    // — included so the wrapper re-creates when the snapshot grows.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sub, status, error, rowsVersion]);
}

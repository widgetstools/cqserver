/**
 * `useSubscription` — open a cqserver subscription over the
 * SharedWorker port. Replaces the legacy `useFilteredSubscription`;
 * the legacy file is now a thin re-export so existing chapters keep
 * compiling unchanged until Phase 3+ migrates them.
 *
 * Surface (strict superset of the legacy `FilteredSubscription`):
 *   rows                       — current snapshot, rebuilt on each
 *                                main-thread coalesce flush.
 *   status                     — 'connecting' | 'snapshotting' | 'live' | …
 *   size                       — rows.length, exposed for fast badges.
 *   subscribeSnapshotChunks(cb)— NEW. Fires once per chunk during the
 *                                SOW (more=true), then once with more=false.
 *   subscribeDeltas(cb)        — Live `{add, update, remove}` batches.
 *   subscribeBatchedDeltas(cb) — Legacy alias for `subscribeDeltas`.
 *   subscribeStatus(cb)        — Reactive status changes.
 *   getSnapshot() / getStatus() / getSize() — imperative accessors.
 *
 * Lifecycle:
 *   - filter / sql change → close current sub, open a new one
 *   - worker reconnect    → sub re-opens automatically (worker drives it)
 *   - component unmounts  → sub closes (deferred to survive StrictMode)
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { getCqPort } from './worker/port';
import type {
  ConnectionStatus,
  Row,
  ServerMsg,
} from './worker/protocol';

export type { ConnectionStatus, Row } from './worker/protocol';

export interface DeltaBatch {
  add: Row[];
  update: Row[];
  remove: Row[];
}

export interface SubscriptionHandle {
  rows: Row[];
  status: ConnectionStatus;
  size: number;
  /** Reactive status subscriber. */
  subscribeStatus: (cb: () => void) => () => void;
  /** Per-chunk SOW notifications. `more` is false on the final chunk. */
  subscribeSnapshotChunks: (cb: (chunk: Row[], more: boolean) => void) => () => void;
  /** Coalesced live delta batches. */
  subscribeDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  /** Legacy alias for `subscribeDeltas` — Phase 1 / 2 grids call this. */
  subscribeBatchedDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  getSnapshot: () => Row[];
  getStatus: () => ConnectionStatus;
  getSize: () => number;
}

class Sub {
  private rows = new Map<string, Row>();
  private snap: Row[] = [];
  private status: ConnectionStatus = 'connecting';
  private errMsg: string | null = null;
  private listeners = new Set<() => void>();
  private statusListeners = new Set<() => void>();
  private chunkListeners = new Set<(chunk: Row[], more: boolean) => void>();
  private deltaListeners = new Set<(b: DeltaBatch) => void>();
  private subId = `s${Math.random().toString(36).slice(2, 10)}`;
  private off: (() => void) | null = null;
  private closed = false;
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private rowKey: (r: Row) => string;

  constructor(
    private readonly topic: string,
    private readonly filter: string | null,
    private readonly sql: string | null,
    rowIdKey: ((r: Row) => string) | undefined,
  ) {
    this.rowKey = rowIdKey ?? ((r) => {
      // Best-effort fallback so legacy chapters compile. Server-side
      // filter views already give us stable keys for most topics; this
      // path is taken only when a hook caller doesn't pass a getRowId.
      const candidate = r.position_id ?? r.trade_id ?? r.cusip ?? r.symbol;
      return candidate != null ? String(candidate) : JSON.stringify(r);
    });
    this.open();
  }

  private open(): void {
    if (this.closed) return;
    const port = getCqPort();
    this.off = port.onMessage((m) => this.onMsg(m));
    const req =
      this.sql != null
        ? { kind: 'subscribe' as const, subId: this.subId, topic: this.topic, sql: this.sql }
        : this.filter != null
          ? { kind: 'subscribe' as const, subId: this.subId, topic: this.topic, filter: this.filter }
          : { kind: 'subscribe' as const, subId: this.subId, topic: this.topic };
    port.send(req);
  }

  private onMsg(m: ServerMsg): void {
    switch (m.kind) {
      case 'status':
        if (m.subId !== this.subId) return;
        this.setStatus(m.status);
        return;
      case 'snapshot':
        if (m.subId !== this.subId) return;
        for (const r of m.chunk) this.rows.set(this.rowKey(r), r);
        for (const cb of this.chunkListeners) cb(m.chunk, m.more);
        if (!m.more) {
          this.snap = Array.from(this.rows.values());
          this.notifyRows();
        }
        return;
      case 'delta':
        if (m.subId !== this.subId) return;
        // cqserver's `publish` messages don't always carry a `dt` field;
        // when missing, the SDK defaults to deltaType='update' even for
        // rows newly entering the filter. Reconcile against our local
        // row mirror so AG-Grid never gets an update/remove for a key
        // it hasn't seen, or an add for a key it already has.
        {
          const realAdds: Row[] = [];
          const realUpdates: Row[] = [];
          const realRemoves: Row[] = [];
          for (const r of m.add) {
            const key = this.rowKey(r);
            const isNew = !this.rows.has(key);
            this.rows.set(key, r);
            (isNew ? realAdds : realUpdates).push(r);
          }
          for (const r of m.update) {
            const key = this.rowKey(r);
            const isNew = !this.rows.has(key);
            this.rows.set(key, r);
            (isNew ? realAdds : realUpdates).push(r);
          }
          for (const r of m.remove) {
            const key = this.rowKey(r);
            if (this.rows.has(key)) {
              this.rows.delete(key);
              realRemoves.push(r);
            }
            // else: server is removing a row we never saw — drop silently.
          }
          this.snap = Array.from(this.rows.values());
          for (const cb of this.deltaListeners) {
            cb({ add: realAdds, update: realUpdates, remove: realRemoves });
          }
          this.notifyRows();
        }
        return;
      case 'error':
        if (m.subId && m.subId !== this.subId) return;
        // Preserve the cqserver error string so the editor / status
        // strip can show it. setStatus fires after errMsg is set so a
        // status listener that reads getErrorMessage() sees the new
        // value on the same notification.
        this.errMsg = m.message;
        this.setStatus('error');
        return;
      case 'disconnected':
        this.setStatus('disconnected');
        return;
      case 'connected':
      case 'hello-ack':
      case 'pong':
        return;
    }
  }

  private setStatus(s: ConnectionStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb();
  }
  private notifyRows(): void {
    for (const cb of this.listeners) cb();
  }

  // Public surface
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
  subscribeSnapshotChunks = (cb: (chunk: Row[], more: boolean) => void): (() => void) => {
    this.chunkListeners.add(cb);
    return () => {
      this.chunkListeners.delete(cb);
    };
  };
  subscribeDeltas = (cb: (b: DeltaBatch) => void): (() => void) => {
    this.deltaListeners.add(cb);
    return () => {
      this.deltaListeners.delete(cb);
    };
  };

  getSnapshot = (): Row[] => this.snap;
  getStatus = (): ConnectionStatus => this.status;
  getSize = (): number => this.rows.size;
  getErrorMessage = (): string | null => this.errMsg;

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const port = getCqPort();
    port.send({ kind: 'unsubscribe', subId: this.subId });
    this.off?.();
    this.off = null;
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
}

export function useSubscription(
  topic: string,
  filter: string | null,
  rowIdKey?: (r: Row) => string,
): SubscriptionHandle {
  const [sub, setSub] = useState<Sub>(() => new Sub(topic, filter, null, rowIdKey));
  const keyRef = useRef<{ topic: string; filter: string | null }>({ topic, filter });

  useEffect(() => {
    if (keyRef.current.topic === topic && keyRef.current.filter === filter) return;
    const next = new Sub(topic, filter, null, rowIdKey);
    keyRef.current = { topic, filter };
    setSub((prev) => {
      prev.close();
      return next;
    });
  }, [topic, filter, rowIdKey]);

  useEffect(() => {
    sub.cancelDeferredClose();
    return () => {
      sub.scheduleClose();
    };
  }, [sub]);

  const rows = useSyncExternalStore(sub.subscribe, sub.getSnapshot, () => [] as Row[]);
  const status = useSyncExternalStore(
    sub.subscribeStatus,
    sub.getStatus,
    () => 'connecting' as ConnectionStatus,
  );

  const wrapper = useMemo<SubscriptionHandle>(
    () => ({
      rows: [],
      status: 'connecting' as ConnectionStatus,
      size: 0,
      subscribeStatus: sub.subscribeStatus,
      subscribeSnapshotChunks: sub.subscribeSnapshotChunks,
      subscribeDeltas: sub.subscribeDeltas,
      subscribeBatchedDeltas: sub.subscribeDeltas, // legacy alias
      getSnapshot: sub.getSnapshot,
      getStatus: sub.getStatus,
      getSize: sub.getSize,
    }),
    [sub],
  );
  wrapper.rows = rows;
  wrapper.status = status;
  wrapper.size = rows.length;
  return wrapper;
}

/** SQL-flavour subscription used by `useLiveQuery` and
 *  `useFilteredAggregate`. Same `Sub` class, distinct entry point so the
 *  call sites read naturally. */
export function makeSqlSub(topic: string, sql: string, rowIdKey?: (r: Row) => string): Sub {
  return new Sub(topic, null, sql, rowIdKey);
}
export type { Sub };

/**
 * One-shot SQL fetch over the worker port. Resolves with the rows
 * once the SOW completes; rejects on error. The temporary
 * subscription is closed automatically before the promise resolves
 * — no live deltas are streamed.
 *
 * Used by ex08-query-builder's "Run (static)" button, which mirrors
 * the legacy `Client.sow(topic, { sql })` semantics.
 */
export function runOneShotSql(topic: string, sql: string): Promise<Row[]> {
  return new Promise((resolve, reject) => {
    // Use the worker's one-shot `sow` path (wire `c:'sow'`), NOT a live
    // subscription. The server evaluates JOINs and derived tables only on
    // the one-shot SOW path; routing these through `sow_and_subscribe`
    // fails with errors like "Unknown column: trade_id" because the live
    // evaluator binds against a single topic's schema.
    const port = getCqPort();
    const reqId = `q${Math.random().toString(36).slice(2, 10)}`;
    const collected: Row[] = [];
    // Single teardown path shared by success, error and timeout so the
    // port listener and timer are never leaked, no matter which fires.
    let settled = false;
    const finish = (fn: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      off();
      fn();
    };
    const off = port.onMessage((m) => {
      if (m.kind === 'snapshot' && m.subId === reqId) {
        for (const r of m.chunk) collected.push(r);
        if (!m.more) finish(() => resolve(collected));
      } else if (m.kind === 'error' && m.subId === reqId) {
        finish(() => reject(new Error(m.message)));
      }
    });
    // Without a timeout a dropped connection (no reply ever arrives)
    // would leak the listener and leave the promise pending forever.
    const timer = setTimeout(
      () => finish(() => reject(new Error(`one-shot query timed out (topic=${topic})`))),
      30_000,
    );
    port.send({ kind: 'sow', reqId, topic, sql });
  });
}

/**
 * `useLiveQuery` — open a server-side continuous SQL subscription
 * over the SharedWorker port. Same `{ topic, sql, getRowId }` spec the
 * legacy hook accepted, same `LiveQueryHandle` return shape so
 * `ex08-query-builder` and downstream consumers keep compiling.
 *
 * Errors from the SDK (parser failures, unknown columns, etc.) surface
 * as `{ kind: 'error' }` messages from the worker and land in the
 * handle's `error` field.
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { makeSqlSub, type Sub, type SubscriptionHandle, type Row } from './use-subscription';
import type { ConnectionStatus } from './worker/protocol';

/**
 * Stable identities for the fallback `useSyncExternalStore` slots used
 * while `wrap === null`. The subscribe/getSnapshot/getServerSnapshot
 * args must NOT change identity between renders — passing a fresh
 * inline function each render makes `useSyncExternalStore` re-subscribe
 * every commit, and the fresh empty `[]` snapshot trips React's
 * Object.is change detection on every read, looping forever.
 */
const NOOP_SUB = (): (() => void) => () => {};
const EMPTY_ROWS: Row[] = [];
const EMPTY_SNAPSHOT = (): Row[] => EMPTY_ROWS;
const IDLE_STATUS = (): ConnectionStatus => 'connecting';
const NULL_ERROR = (): string | null => null;

export interface LiveQuerySpec {
  topic: string;
  sql: string;
  getRowId: (row: Row) => string;
}

export interface LiveQueryHandle extends SubscriptionHandle {
  /** Server-side error string if the subscription couldn't start. null otherwise. */
  error: string | null;
}

class LiveQueryWrapper {
  readonly sub: Sub;
  private errMsg: string | null = null;
  private errListeners = new Set<() => void>();
  private off: (() => void) | null = null;

  constructor(spec: LiveQuerySpec) {
    this.sub = makeSqlSub(spec.topic, spec.sql, spec.getRowId);
    // Mirror status='error' into a separate errMsg so the UI can surface
    // the message even after status transitions back.
    this.off = this.sub.subscribeStatus(() => {
      if (this.sub.getStatus() === 'error' && this.errMsg == null) {
        this.errMsg = this.sub.getErrorMessage() ?? 'query failed';
        for (const cb of this.errListeners) cb();
      }
    });
  }

  subscribeError = (cb: () => void): (() => void) => {
    this.errListeners.add(cb);
    return () => {
      this.errListeners.delete(cb);
    };
  };
  getError = (): string | null => this.errMsg;

  close(): void {
    this.off?.();
    this.sub.close();
  }
  scheduleClose(): void {
    this.sub.scheduleClose();
  }
  cancelDeferredClose(): void {
    this.sub.cancelDeferredClose();
  }
}

export function useLiveQuery(spec: LiveQuerySpec | null): LiveQueryHandle | null {
  const [wrap, setWrap] = useState<LiveQueryWrapper | null>(() =>
    spec ? new LiveQueryWrapper(spec) : null,
  );
  const keyRef = useRef<LiveQuerySpec | null>(spec);

  useEffect(() => {
    const same =
      keyRef.current?.topic === spec?.topic &&
      keyRef.current?.sql === spec?.sql &&
      keyRef.current?.getRowId === spec?.getRowId;
    if (same) return;
    keyRef.current = spec;
    setWrap((prev) => {
      prev?.close();
      return spec ? new LiveQueryWrapper(spec) : null;
    });
  }, [spec]);

  useEffect(() => {
    wrap?.cancelDeferredClose();
    return () => {
      wrap?.scheduleClose();
    };
  }, [wrap]);

  const rows = useSyncExternalStore(
    wrap ? wrap.sub.subscribe : NOOP_SUB,
    wrap ? wrap.sub.getSnapshot : EMPTY_SNAPSHOT,
    EMPTY_SNAPSHOT,
  );
  const status = useSyncExternalStore(
    wrap ? wrap.sub.subscribeStatus : NOOP_SUB,
    wrap ? wrap.sub.getStatus : IDLE_STATUS,
    IDLE_STATUS,
  );
  const error = useSyncExternalStore(
    wrap ? wrap.subscribeError : NOOP_SUB,
    wrap ? wrap.getError : NULL_ERROR,
    NULL_ERROR,
  );

  // Stable handle identity (same pattern as useSubscription) — a fresh
  // object every rows/status tick makes useLiveGridSync think the sub
  // changed and wipe the Query tab grid.
  const handle = useMemo<LiveQueryHandle | null>(() => {
    if (!wrap) return null;
    return {
      rows: [] as Row[],
      status: 'connecting' as ConnectionStatus,
      size: 0,
      error: null,
      subscribeStatus: wrap.sub.subscribeStatus,
      subscribeSnapshotChunks: wrap.sub.subscribeSnapshotChunks,
      subscribeDeltas: wrap.sub.subscribeDeltas,
      subscribeBatchedDeltas: wrap.sub.subscribeDeltas,
      getSnapshot: wrap.sub.getSnapshot,
      getStatus: wrap.sub.getStatus,
      getSize: wrap.sub.getSize,
    };
  }, [wrap]);

  if (!handle) return null;
  handle.rows = rows;
  handle.status = status;
  handle.size = rows.length;
  handle.error = error;
  return handle;
}

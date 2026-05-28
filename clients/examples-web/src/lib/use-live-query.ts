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
        this.errMsg = 'query failed';
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

  const noop = (): (() => void) => () => {};
  const idleStatus = (): ConnectionStatus => 'connecting';
  const empty = (): Row[] => [];
  const rows = useSyncExternalStore(
    wrap ? wrap.sub.subscribe : noop,
    wrap ? wrap.sub.getSnapshot : empty,
    empty,
  );
  const status = useSyncExternalStore(
    wrap ? wrap.sub.subscribeStatus : noop,
    wrap ? wrap.sub.getStatus : idleStatus,
    idleStatus,
  );
  const error = useSyncExternalStore(
    wrap ? wrap.subscribeError : noop,
    wrap ? wrap.getError : () => null,
    () => null,
  );

  return useMemo<LiveQueryHandle | null>(() => {
    if (!wrap) return null;
    return {
      rows,
      status,
      size: rows.length,
      subscribeStatus: wrap.sub.subscribeStatus,
      subscribeSnapshotChunks: wrap.sub.subscribeSnapshotChunks,
      subscribeDeltas: wrap.sub.subscribeDeltas,
      subscribeBatchedDeltas: wrap.sub.subscribeDeltas,
      getSnapshot: wrap.sub.getSnapshot,
      getStatus: wrap.sub.getStatus,
      getSize: wrap.sub.getSize,
      error,
    };
  }, [wrap, rows, status, error]);
}

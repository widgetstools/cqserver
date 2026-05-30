/**
 * `useFilteredAggregate` — open a server-side continuous aggregate
 * subscription. The result is a single up-to-date row (the SELECT-list
 * aliases). cqserver re-emits the group whenever it changes; the
 * worker coalesces and the hook surfaces the latest row only.
 *
 * Backed by the same SharedWorker port as `useSubscription` and
 * `useLiveQuery`. The legacy cqStore client is no longer imported.
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { makeSqlSub, type Sub, type Row } from './use-subscription';
import type { ConnectionStatus } from './worker/protocol';

export type { Row, ConnectionStatus };

export interface AggregateHandle {
  /** Latest single-row aggregate, or null until SOW lands. */
  row: Row | null;
  status: ConnectionStatus;
  /** Imperative accessor for the latest row (null before SOW). */
  getRow: () => Row | null;
  /** Reactive subscriber for the row. */
  subscribe: (cb: () => void) => () => void;
  subscribeStatus: (cb: () => void) => () => void;
}

class AggregateSub {
  private row: Row | null = null;
  readonly sub: Sub;
  private listeners = new Set<() => void>();
  private off: (() => void) | null = null;

  constructor(topic: string, sql: string) {
    // The aggregate sub is always single-row by design. The constant
    // rowKey collapses the MAIN-thread mirror to one entry; the empty
    // `keyCols` makes the WORKER mirror collapse the same way. Without the
    // latter the worker heuristic JSON.stringify's each re-emitted group,
    // so its mirror grew one entry per tick, unbounded, for the life of
    // the subscription (and replayed that whole history on late join).
    this.sub = makeSqlSub(topic, sql, () => 'AGG', []);
    // Mirror snapshot/deltas into our `row`.
    this.off = this.sub.subscribe(() => {
      const snap = this.sub.getSnapshot();
      this.row = snap.length > 0 ? snap[snap.length - 1]! : null;
      for (const cb of this.listeners) cb();
    });
  }

  subscribe = (cb: () => void): (() => void) => {
    this.listeners.add(cb);
    return () => {
      this.listeners.delete(cb);
    };
  };
  getRow = (): Row | null => this.row;
  subscribeStatus = (cb: () => void): (() => void) => this.sub.subscribeStatus(cb);
  getStatus = (): ConnectionStatus => this.sub.getStatus();

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

export function useFilteredAggregate(topic: string, sql: string): AggregateHandle {
  const [agg, setAgg] = useState<AggregateSub>(() => new AggregateSub(topic, sql));
  const keyRef = useRef<{ topic: string; sql: string }>({ topic, sql });

  useEffect(() => {
    if (keyRef.current.topic === topic && keyRef.current.sql === sql) return;
    const next = new AggregateSub(topic, sql);
    keyRef.current = { topic, sql };
    setAgg((prev) => {
      prev.close();
      return next;
    });
  }, [topic, sql]);

  useEffect(() => {
    agg.cancelDeferredClose();
    return () => {
      agg.scheduleClose();
    };
  }, [agg]);

  const status = useSyncExternalStore(
    agg.subscribeStatus,
    agg.getStatus,
    () => 'connecting' as ConnectionStatus,
  );
  const row = useSyncExternalStore(
    agg.subscribe,
    agg.getRow,
    () => null as Row | null,
  );

  return useMemo<AggregateHandle>(
    () => ({
      row,
      status,
      getRow: agg.getRow,
      subscribe: agg.subscribe,
      subscribeStatus: agg.subscribeStatus,
    }),
    [agg, row, status],
  );
}

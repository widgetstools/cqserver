/**
 * `useFilteredAggregate` — open a per-component cqserver subscription
 * whose snapshot + delta stream are SERVER-COMPUTED aggregates over
 * a topic with a dynamic WHERE clause. Returns the single
 * aggregate-row state-of-the-world (typed as `Record<string, unknown>`
 * matching the SELECT-list aliases).
 *
 * Why this exists: the demo's KPI panels need running aggregates over
 * a chip-filtered slice of a topic (e.g. SUM(notional_usd) WHERE
 * book_name IN (...) AND status = 'FILLED'). The chip filter changes
 * dynamically — building a materialized view per filter combination
 * is impractical. cqserver supports continuous-aggregate
 * subscriptions with arbitrary SQL; this hook drives one per
 * (topic, sql) pair and tears it down when either changes.
 *
 * The whole point of cqserver is that aggregates live server-side
 * and re-emit only the changed group. The browser receives ONE
 * delta per coalesce flush carrying the up-to-date aggregate row —
 * no reduce(), no map(), no group()-by anywhere in the React tree.
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import type { Delta } from '@cqserver/client';
import { type ConnectionStatus, type Row, getCqClient, onCqClientChange } from './cq-store';

class AggregateSub {
  /** Single-row aggregate result. */
  private agg: Row = {};
  private snapVersion = 0;
  private listeners = new Set<() => void>();
  private status: ConnectionStatus = 'idle';
  private statusListeners = new Set<() => void>();
  private abortController: AbortController | null = null;
  private currentSubId: string | null = null;
  private clientUnsubscribe: (() => void) | null = null;
  private closed = false;
  private closeTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly topic: string,
    private readonly sql: string,
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

  /** Snapshot version — bumped on every delta so React re-renders. */
  getSnapshotVersion = (): number => this.snapVersion;
  getAggregate = (): Row => this.agg;
  getStatus = (): ConnectionStatus => this.status;

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.clientUnsubscribe?.();
    this.clientUnsubscribe = null;
    this.abortCurrent();
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
    this.agg = {};
    this.bumpAndNotify();
    try {
      const sub = await client.sowAndSubscribe(this.topic, { sql: this.sql });
      if (abort.signal.aborted) {
        await client.unsubscribe(sub.subId).catch(() => {});
        return;
      }
      this.currentSubId = sub.subId;
      void sub.whenSnapshotComplete().then(() => {
        if (abort.signal.aborted) return;
        this.setStatus('live');
      });
      for await (const delta of sub) {
        if (abort.signal.aborted) break;
        this.applyDelta(delta);
      }
      if (!abort.signal.aborted) this.setStatus('disconnected');
    } catch (err) {
      if (!abort.signal.aborted) {
        console.warn(
          `[useFilteredAggregate] ${this.topic} sub failed`,
          this.sql,
          err,
        );
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
    // Continuous-aggregate subscriptions with no GROUP BY produce
    // exactly one row whose contents replace the previous aggregate
    // wholesale. `remove` / `oof` shouldn't happen on a degenerate
    // single-group aggregate, but treat them defensively as "no
    // matching rows now" → empty aggregate.
    if (d.deltaType === 'remove' || d.deltaType === 'oof') {
      this.agg = {};
    } else {
      this.agg = d.data as Row;
    }
    this.bumpAndNotify();
  }

  private bumpAndNotify(): void {
    this.snapVersion++;
    for (const cb of this.listeners) cb();
  }

  private setStatus(s: ConnectionStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb();
  }
}

export interface FilteredAggregate {
  /** Aggregate row — keys match the SELECT-list aliases in the SQL. */
  row: Row;
  status: ConnectionStatus;
}

/**
 * Subscribe to a server-side aggregate. The `sql` argument is the
 * FULL SELECT — e.g. `SELECT COUNT(*) AS n, SUM(notional_usd) AS gn
 * FROM t WHERE book_name = 'FX-Spot'`. Use `FROM t` as the FROM
 * clause; cqserver rewrites it to the actual topic before parsing.
 *
 * Re-subscribes whenever `topic` or `sql` changes. Returns the
 * current aggregate row + status. The row is a plain object — the
 * caller indexes into it by SELECT alias.
 */
export function useFilteredAggregate(topic: string, sql: string): FilteredAggregate {
  const [sub, setSub] = useState<AggregateSub>(() => new AggregateSub(topic, sql));
  const keyRef = useRef<{ topic: string; sql: string }>({ topic, sql });

  useEffect(() => {
    if (keyRef.current.topic === topic && keyRef.current.sql === sql) return;
    const next = new AggregateSub(topic, sql);
    keyRef.current = { topic, sql };
    setSub((prev) => {
      prev.close();
      return next;
    });
  }, [topic, sql]);

  useEffect(() => {
    sub.cancelDeferredClose();
    return () => {
      sub.scheduleClose();
    };
  }, [sub]);

  // Use the version counter as the snapshot — it changes on every
  // delta, which is what triggers React re-renders. The actual row
  // object is grabbed live from the sub each render.
  useSyncExternalStore(sub.subscribe, sub.getSnapshotVersion, () => 0);
  const status = useSyncExternalStore(
    sub.subscribeStatus,
    sub.getStatus,
    () => 'idle' as ConnectionStatus,
  );

  return useMemo<FilteredAggregate>(
    () => ({ row: sub.getAggregate(), status }),
    // `sub.getAggregate()` returns the current value every render;
    // we memo on (sub, snapVersion, status) by reading them above
    // so useSyncExternalStore already triggers the re-render.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sub, sub.getSnapshotVersion(), status],
  );
}

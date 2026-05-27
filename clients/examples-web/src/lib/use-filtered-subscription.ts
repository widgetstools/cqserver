/**
 * `useFilteredSubscription` — open a per-component cqserver
 * subscription with a server-side filter expression. Each instance
 * holds its OWN `sowAndSubscribe`; cqserver applies the predicate
 * before egress so only matching rows + OOF transitions hit the wire.
 *
 * Architecturally this is the right pattern for the Atlas demo: chip
 * toggles in ex02, drill-throughs in ex03, sample slicing in ex06
 * are all server-evaluated, which is the whole point of cqserver.
 *
 * Surface mirrors the global `cqStore[topic]` API so `<GridPanel>`
 * can consume either:
 *
 *   const sub = useFilteredSubscription('/trades', 'mifid_flag = TRUE');
 *   // sub.snapshot — current rows (rebuilt per coalesce flush)
 *   // sub.subscribeBatchedDeltas(cb) — fired per coalesce window
 *   // sub.status — 'connecting' | 'snapshotting' | 'live' | …
 *
 * Lifecycle:
 *   - filter changes → close existing sub, open a new one
 *   - client reconnect → resubscribe automatically
 *   - component unmounts → unsubscribe
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

const COALESCE_MS = 50;

type TopicName =
  | '/positions'
  | '/trades'
  | '/securities'
  | '/fi-market-data'
  // Materialized views — cqserver declares these in cqserver.toml.
  // The view's key is its GROUP BY tuple, so the row-id extractor
  // composes the same tuple in the same column order.
  | '/v_net_exposure'
  | '/v_slippage_venue_algo'
  | '/v_pnl_by_sector'
  | '/v_pnl_by_book'
  | '/v_compliance_counts'
  | '/v_cross_asset_pivot'
  | '/v_heatmap_sector_region'
  | '/v_trades_by_compliance'
  | '/v_book_totals';

/** Stable row-id derivation per topic — must match the server's key. */
const KEY_OF: Record<TopicName, (r: Row) => string> = {
  '/positions': (r) => String(r.position_id ?? r.positionKey ?? `${r.book_id ?? r.book}|${r.cusip}`),
  '/trades': (r) => String(r.trade_id ?? r.tradeId),
  '/securities': (r) => String(r.cusip ?? r.symbol),
  '/fi-market-data': (r) => String(r.cusip ?? r.symbol),
  '/v_net_exposure': (r) =>
    `${r.book_id ?? ''}|${r.book_name ?? ''}|${r.asset_class ?? ''}|${r.currency ?? ''}`,
  '/v_slippage_venue_algo': (r) =>
    `${r.execution_venue ?? ''}|${r.execution_algo ?? ''}`,
  '/v_pnl_by_sector': (r) => String(r.issuer_sector ?? ''),
  '/v_pnl_by_book': (r) => `${r.book_id ?? ''}|${r.book_name ?? ''}`,
  '/v_compliance_counts': (r) => String(r.compliance_status ?? ''),
  '/v_cross_asset_pivot': (r) => `${r.asset_class ?? ''}|${r.currency ?? ''}`,
  '/v_heatmap_sector_region': (r) =>
    `${r.issuer_sector ?? ''}|${r.issuer_region ?? ''}`,
  '/v_trades_by_compliance': (r) => String(r.compliance_status ?? ''),
  '/v_book_totals': () => 'TOTAL',
};

class FilteredSub {
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
  // Pending close timer. React 18 StrictMode dev mounts components
  // twice (mount→unmount→remount) so a synchronous `close()` on
  // unmount tears down the sub before the remount can use it. We
  // schedule the real close on the next macrotask; the remount's
  // `cancelClose()` aborts the schedule, so the sub survives.
  private closeTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private readonly topic: TopicName,
    private readonly filter: string | null,
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

  getSnapshot = (): Row[] => this.snap;
  getStatus = (): ConnectionStatus => this.status;
  getSize = (): number => this.rows.size;

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

  /**
   * Defer the real close. If `cancelDeferredClose()` runs before the
   * timer fires (StrictMode remount), the sub survives.
   */
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

  // ─── Subscription lifecycle ─────────────────────────────────
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

    try {
      const sub = await client.sowAndSubscribe(this.topic, {
        filter: this.filter ?? undefined,
      });
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
        console.warn(`[useFilteredSubscription] ${this.topic} failed`, err);
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

  // ─── Delta application (mirrors TopicStore) ─────────────────
  private applyDelta(d: Delta): void {
    const data = d.data as Row;
    const key = KEY_OF[this.topic](data);
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

  private notifyAll(): void {
    for (const cb of this.listeners) cb();
  }
}

/**
 * React hook — opens a filtered subscription, returns a snapshot +
 * delta-batch interface. Re-subscribes whenever `filter` changes.
 */
export interface FilteredSubscription {
  rows: Row[];
  status: ConnectionStatus;
  size: number;
  /** Subscribe to delta batches for AG-Grid `applyTransactionAsync`. */
  subscribeBatchedDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  /** Subscribe to status changes (used by GridPanel for the seed gate). */
  subscribeStatus: (cb: () => void) => () => void;
  /** Direct snapshot accessor (non-reactive). */
  getSnapshot: () => Row[];
  /** Imperative status accessor (non-reactive). */
  getStatus: () => ConnectionStatus;
  /** Imperative row count (non-reactive). */
  getSize: () => number;
}

export function useFilteredSubscription(
  topic: TopicName,
  filter: string | null,
): FilteredSubscription {
  // Hold the active FilteredSub in state so identity changes trigger
  // a re-render. `useRef` would be invisible to React's
  // `useSyncExternalStore`, which is the bug fixed here.
  const [sub, setSub] = useState<FilteredSub>(() => new FilteredSub(topic, filter));
  const keyRef = useRef<{ topic: TopicName; filter: string | null }>({ topic, filter });

  // When topic / filter changes, tear down the old sub and switch to
  // a new one. Side effect lives in useEffect so render stays pure
  // (no resource creation during render → StrictMode-safe).
  useEffect(() => {
    if (keyRef.current.topic === topic && keyRef.current.filter === filter) {
      return; // first render — nothing to swap.
    }
    const next = new FilteredSub(topic, filter);
    keyRef.current = { topic, filter };
    setSub((prev: FilteredSub) => {
      prev.close();
      return next;
    });
  }, [topic, filter]);

  // Final unmount: defer the close. React 18 StrictMode mounts in
  // dev with mount→unmount→remount. A synchronous close() would
  // tear down the sub before the remount could resume it; the
  // deferred close lets the remount cancel the schedule and keep
  // the connection alive.
  useEffect(() => {
    sub.cancelDeferredClose();
    return () => {
      sub.scheduleClose();
    };
  }, [sub]);

  const rows = useSyncExternalStore(sub.subscribe, sub.getSnapshot, () => []);
  const status = useSyncExternalStore(
    sub.subscribeStatus,
    sub.getStatus,
    () => 'idle' as ConnectionStatus,
  );

  // The wrapper object's IDENTITY must be stable per `sub` instance so
  // consumers (e.g. GridPanel) that use `liveSubscription` as a
  // useMemo / useEffect dependency don't re-run every coalesce flush.
  // We memoize on `sub` and mutate the live `rows / status / size`
  // fields in place each render — the wrapper isn't React state, so
  // the in-place mutation is allowed (and required, otherwise we'd
  // either return a fresh object per render or accept stale data).
  const wrapper = useMemo<FilteredSubscription>(
    () => ({
      rows: [],
      status: 'idle' as ConnectionStatus,
      size: 0,
      subscribeBatchedDeltas: sub.subscribeBatchedDeltas,
      subscribeStatus: sub.subscribeStatus,
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

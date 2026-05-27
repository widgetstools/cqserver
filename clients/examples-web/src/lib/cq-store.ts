/**
 * Live cqserver-backed reactive store, driven by the official
 * `@cqserver/client` SDK over WebSocket.
 *
 * All eight Atlas examples read positions / trades / securities /
 * fi-market-data from this module. We open one shared `Client` against
 * cqserver, call `sowAndSubscribe` on each topic, and mirror server
 * state into a keyed Map per topic. React panels subscribe via
 * `useSyncExternalStore` (50ms-coalesced flushes) and AG-Grid panels
 * additionally tap the raw delta stream for cell-flash.
 *
 * There is no client-side mutation anywhere. Every value visible in
 * the UI arrives over the wire — the atlas-publisher running alongside
 * cqserver is the only source of truth.
 */
import { useSyncExternalStore } from 'react';
import { Client, type Delta } from '@cqserver/client';
import type { Position, Trade } from './data-gen';

export type { Position, Trade } from './data-gen';

export type ConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'snapshotting'
  | 'live'
  | 'disconnected';

export type Row = Record<string, unknown>;

/**
 * Batched delta delivery for grids that drive themselves via
 * `applyTransactionAsync`. Lists are stable per flush — each batch
 * carries every row that changed since the last coalesce flush.
 */
export interface DeltaBatch {
  add: Row[];
  update: Row[];
  remove: Row[];
}

// ── Configuration ───────────────────────────────────────────────
const DEFAULT_WS_URL = 'ws://127.0.0.1:9008/cq/json';
const COALESCE_MS = 50;

function resolveWsUrl(): string {
  if (typeof window === 'undefined') return DEFAULT_WS_URL;
  const override = (window as unknown as { CQ_WS_URL?: string }).CQ_WS_URL;
  return override ?? DEFAULT_WS_URL;
}

// ── Topic store ─────────────────────────────────────────────────
class TopicStore {
  private rows = new Map<string, Row>();
  private snap: Row[] = [];
  private listeners = new Set<() => void>();
  private dirty = false;
  private coalesceTimer: ReturnType<typeof setTimeout> | null = null;
  private status: ConnectionStatus = 'idle';
  private statusListeners = new Set<() => void>();
  private tickCount = 0;
  private deltaListeners = new Set<(row: Row) => void>();
  private tickListeners = new Set<() => void>();
  // Post-SOW batched delta delivery for AG-Grid `applyTransactionAsync`.
  // Accumulates per coalesce window; fired on flushNow.
  private batchListeners = new Set<(b: DeltaBatch) => void>();
  private pendingAdds: Row[] = [];
  private pendingUpdates: Row[] = [];
  private pendingRemoves: Row[] = [];
  /** True after the initial SOW completes; gates batch delivery so
   *  the SOW burst doesn't trickle out as 40 000 add transactions. */
  private livePhase = false;

  constructor(
    public readonly topic: string,
    private readonly keyOf: (r: Row) => string,
  ) {}

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

  subscribeTicks = (cb: () => void): (() => void) => {
    this.tickListeners.add(cb);
    return () => {
      this.tickListeners.delete(cb);
    };
  };

  subscribeDeltas = (cb: (row: Row) => void): (() => void) => {
    this.deltaListeners.add(cb);
    return () => {
      this.deltaListeners.delete(cb);
    };
  };

  /**
   * Subscribe to batched deltas — fired once per coalesce window post-SOW.
   * AG-Grid panels use this to apply changes via
   * `gridApi.applyTransactionAsync({ add, update, remove })`, avoiding the
   * O(N) rowData diff that the React-prop path triggers.
   */
  subscribeBatchedDeltas = (cb: (b: DeltaBatch) => void): (() => void) => {
    this.batchListeners.add(cb);
    return () => {
      this.batchListeners.delete(cb);
    };
  };

  /** Called by the subscription driver after `group_end` fires. */
  markLive(): void {
    this.livePhase = true;
  }

  getSnapshot = (): Row[] => this.snap;
  getStatus = (): ConnectionStatus => this.status;
  getRowsMap = (): ReadonlyMap<string, Row> => this.rows;
  getSize = (): number => this.rows.size;
  getTickCount = (): number => this.tickCount;

  setStatus(s: ConnectionStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb();
  }

  applyDelta(d: Delta): void {
    const data = d.data as Row;
    const key = this.keyOf(data);
    if (d.deltaType === 'remove' || d.deltaType === 'oof') {
      this.rows.delete(key);
      if (this.livePhase) this.pendingRemoves.push(data);
    } else if (d.deltaType === 'add') {
      this.rows.set(key, data);
      // Post-SOW adds (new rows after the snapshot) go to the
      // batch's `add` slot. SOW-phase adds are picked up by the
      // initial-snapshot seed in GridPanel instead.
      if (this.livePhase) this.pendingAdds.push(data);
    } else {
      this.rows.set(key, data);
      if (this.livePhase) this.pendingUpdates.push(data);
    }
    this.tickCount++;
    // Legacy per-row listeners (still used by anything that hasn't
    // switched to batched mode). Suppressed during SOW.
    if (this.livePhase && d.deltaType !== 'add') {
      for (const cb of this.deltaListeners) cb(data);
    }
    this.scheduleFlush();
  }

  /** Force a flush immediately (e.g., once `group_end` fires). */
  flushNow(): void {
    this.dirty = false;
    if (this.coalesceTimer != null) {
      clearTimeout(this.coalesceTimer);
      this.coalesceTimer = null;
    }
    this.snap = Array.from(this.rows.values());
    for (const cb of this.listeners) cb();
    for (const cb of this.tickListeners) cb();
    this.drainBatch();
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
        for (const cb of this.tickListeners) cb();
      }
      this.drainBatch();
    }, COALESCE_MS);
  }

  /** Move the pending {add, update, remove} buffers into one batch
   *  notification for every batched-delta listener, then reset the
   *  buffers. No-op if nothing changed since the last drain. */
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
}

// ── Topic instances ─────────────────────────────────────────────
const positionsStore = new TopicStore(
  '/positions',
  (r) => String(r.position_id ?? r.positionKey ?? `${r.book_id ?? r.book}|${r.cusip}`),
);
const tradesStore = new TopicStore(
  '/trades',
  (r) => String(r.trade_id ?? r.tradeId),
);
const securitiesStore = new TopicStore(
  '/securities',
  (r) => String(r.cusip ?? r.symbol),
);
const marketDataStore = new TopicStore(
  '/fi-market-data',
  (r) => String(r.cusip ?? r.symbol),
);

const ALL_STORES = [positionsStore, tradesStore, securitiesStore, marketDataStore];

// ── Connection bootstrap with auto-reconnect ───────────────────
//
// The browser never publishes, only subscribes — and the cqserver
// idle timer kicks silent peers after ~65s. The Client now sends a
// heartbeat every 25s on its own (see @cqserver/client opts), but
// real-world WS connections still die from sleep / network change /
// server restart, so we wrap the subscription loop in a supervisor
// that reconnects with capped exponential backoff.
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 8_000;

let currentClient: Client | null = null;
let reconnectAttempts = 0;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
const clientChangeListeners = new Set<() => void>();

/**
 * Returns the active `Client`, or `null` if the connection is down
 * (initial connect not yet completed, or mid-reconnect). Used by
 * per-component filtered subscriptions to piggy-back on the shared
 * WebSocket.
 */
export function getCqClient(): Client | null {
  return currentClient;
}

/**
 * Listen for client lifecycle changes — fires whenever a fresh
 * `Client` is bound or the active one is torn down. Per-component
 * subs use this to rebuild themselves across reconnects.
 */
export function onCqClientChange(cb: () => void): () => void {
  clientChangeListeners.add(cb);
  return () => {
    clientChangeListeners.delete(cb);
  };
}

function broadcastClientChange(): void {
  for (const cb of clientChangeListeners) cb();
}

async function bootstrap(): Promise<void> {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  const url = resolveWsUrl();
  for (const s of ALL_STORES) s.setStatus('connecting');
  let client: Client;
  try {
    client = await Client.connect(url, { heartbeatIntervalMs: 25_000 });
  } catch (err) {
    console.warn('[cq-store] connect failed; scheduling retry', err);
    for (const s of ALL_STORES) s.setStatus('disconnected');
    scheduleReconnect();
    return;
  }
  currentClient = client;
  reconnectAttempts = 0;
  broadcastClientChange();

  client.onClose(() => {
    if (currentClient === client) {
      console.warn('[cq-store] connection closed; reconnecting');
      currentClient = null;
      for (const s of ALL_STORES) s.setStatus('disconnected');
      broadcastClientChange();
      scheduleReconnect();
    }
  });

  // Drive each topic in its own async loop. Each subscription consumes
  // the SOW snapshot then transitions to live deltas — both arrive on
  // the same async iterator because of the dispatch fix in @cqserver/client.
  void driveSubscription(client, positionsStore);
  void driveSubscription(client, tradesStore);
  void driveSubscription(client, securitiesStore);
  void driveSubscription(client, marketDataStore);
}

function scheduleReconnect(): void {
  if (reconnectTimer) return;
  reconnectAttempts++;
  const delay = Math.min(
    RECONNECT_MAX_MS,
    RECONNECT_BASE_MS * 2 ** Math.min(reconnectAttempts - 1, 6),
  );
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void bootstrap();
  }, delay);
}

async function driveSubscription(client: Client, store: TopicStore): Promise<void> {
  try {
    store.setStatus('snapshotting');
    const sub = await client.sowAndSubscribe(store.topic);
    void sub.whenSnapshotComplete().then(() => {
      // Force a flush right at the snapshot boundary so the grid shows
      // the full universe immediately, not on the next coalesce tick.
      store.flushNow();
      // Switch the store into live-phase BEFORE setStatus so any
      // grids that subscribe on the 'live' transition see batched
      // deltas from the very first post-SOW frame.
      store.markLive();
      store.setStatus('live');
    });
    for await (const delta of sub) {
      store.applyDelta(delta);
    }
    // Iterator ended — either the subscription closed cleanly or the
    // transport dropped. The client.onClose handler is responsible for
    // the reconnect path; here we just reflect the per-topic state.
    store.setStatus('disconnected');
  } catch (err) {
    console.error(`[cq-store] subscription ${store.topic} failed`, err);
    store.setStatus('disconnected');
  }
}

if (typeof window !== 'undefined') void bootstrap();

// ── Public hooks ────────────────────────────────────────────────
export function usePositions(): Row[] {
  return useSyncExternalStore(positionsStore.subscribe, positionsStore.getSnapshot, () => []);
}

export function useTrades(): Row[] {
  return useSyncExternalStore(tradesStore.subscribe, tradesStore.getSnapshot, () => []);
}

export function useSecurities(): Row[] {
  return useSyncExternalStore(securitiesStore.subscribe, securitiesStore.getSnapshot, () => []);
}

export function useMarketData(): Row[] {
  return useSyncExternalStore(marketDataStore.subscribe, marketDataStore.getSnapshot, () => []);
}

// Backwards-compat aliases — the legacy tick-engine hooks read straight
// off the cqserver store now.
export const useLivePositions = usePositions as () => Position[];
export const useLiveTrades = useTrades as () => Trade[];

export type CqTopic = 'positions' | 'trades' | 'securities' | 'marketData';

const storeByTopic: Record<CqTopic, TopicStore> = {
  positions: positionsStore,
  trades: tradesStore,
  securities: securitiesStore,
  marketData: marketDataStore,
};

const noopSubscribe = (): (() => void) => () => {};
const zeroSnapshot = (): number => 0;

export function useTickCount(topic: CqTopic | undefined): number {
  const store = topic ? storeByTopic[topic] : null;
  return useSyncExternalStore(
    store ? store.subscribeTicks : noopSubscribe,
    store ? store.getTickCount : zeroSnapshot,
    zeroSnapshot,
  );
}

export function useCqStatus(): ConnectionStatus {
  return useSyncExternalStore(
    positionsStore.subscribeStatus,
    positionsStore.getStatus,
    () => 'idle' as ConnectionStatus,
  );
}

export const cqStore = {
  positions: positionsStore,
  trades: tradesStore,
  securities: securitiesStore,
  marketData: marketDataStore,
};

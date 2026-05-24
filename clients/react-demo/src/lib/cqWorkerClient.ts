/**
 * Drop-in replacement for `CqClient` that proxies to a SharedWorker.
 * Components keep calling `client.subscribe(...)` exactly as before —
 * the WebSocket, JSON parsing, and topic-level deduplication happen
 * inside `cq-worker.ts` instead of on the main React thread.
 *
 * The public shape (subscribe, onStatus, connect, close) matches
 * cqClient.ts 1:1 so swapping is a one-line change in
 * CqClientContext.tsx.
 */

import type {
  ConnectionStatus,
  Row,
  SubscribeOptions,
  SubscriberCallbacks,
} from './cqClient';

interface SubState {
  subId: string;
  topic: string;
  cb: SubscriberCallbacks;
}

export class CqWorkerClient {
  private worker: SharedWorker;
  private port: MessagePort;
  private url: string;
  private status: ConnectionStatus = 'idle';
  private statusListeners = new Set<(s: ConnectionStatus) => void>();
  private subs = new Map<string, SubState>();
  private nextSubId = 1;
  private pingTimer: ReturnType<typeof setInterval> | null = null;

  constructor(url: string) {
    this.url = url;
    // Vite resolves `new URL('./cq-worker.ts', import.meta.url)` at
    // build time. Module type lets the worker use ES module syntax.
    this.worker = new SharedWorker(
      new URL('./cq-worker.ts', import.meta.url),
      // Bump the name to force a fresh SharedWorker instance after
      // worker-side code changes (browsers identify SharedWorkers by
      // URL + name and reuse them across reloads — bumping the suffix
      // is the simplest way to evict the old code).
      { type: 'module', name: 'cq-shared-worker-v4' },
    );
    this.port = this.worker.port;
    this.port.onmessage = (ev) => this.dispatch(ev.data);
    this.port.start();
  }

  onStatus(cb: (s: ConnectionStatus) => void): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => {
      this.statusListeners.delete(cb);
    };
  }

  private setStatus(s: ConnectionStatus) {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb(s);
  }

  connect() {
    // Idempotent on the worker side — first tab triggers the WS,
    // subsequent calls are no-ops there.
    this.port.postMessage({ type: 'connect', url: this.url });
    // Start a lightweight presence heartbeat so the worker can reap
    // this tab if the page is torn down without a clean unsubscribe
    // (browser crash, force-close, etc.). Without this the worker
    // keeps its WebSocket open indefinitely on an abandoned port.
    if (this.pingTimer == null) {
      this.pingTimer = setInterval(() => {
        try {
          this.port.postMessage({ type: 'ping' });
        } catch {
          /* port dead — clearInterval on the next close() */
        }
      }, 10_000);
    }
  }

  close() {
    // IMPORTANT: do NOT close the MessagePort. React's
    // CqClientProvider runs this on effect cleanup, which can fire
    // in dev (HMR, ancestor re-renders that change effect deps).
    // Permanently closing the port here makes subsequent subscribes
    // postMessage into the void → updates silently stop in every
    // remaining panel. The port is held by `this.worker` and gets
    // torn down when the browser disposes the tab.
    //
    // We do still want to drop server-side subscriptions for any
    // sub that's about to be re-issued on the next mount, but the
    // worker's per-subId unsubscribe is already idempotent — letting
    // the next mount issue fresh subscribes is enough.
    for (const sub of this.subs.values()) {
      try {
        this.port.postMessage({ type: 'unsubscribe', subId: sub.subId });
      } catch {
        /* port already dead — nothing to do */
      }
    }
    this.subs.clear();
    if (this.pingTimer != null) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  subscribe(
    topic: string,
    cb: SubscriberCallbacks,
    opts: SubscribeOptions = {},
  ): () => void {
    const subId = `t${this.nextSubId++}`;
    this.subs.set(subId, { subId, topic, cb });
    this.port.postMessage({
      type: 'subscribe',
      subId,
      topic,
      deltasOnly: !!opts.deltasOnly,
    });
    return () => {
      this.subs.delete(subId);
      this.port.postMessage({ type: 'unsubscribe', subId });
    };
  }

  private dispatch(m: Record<string, unknown>) {
    const type = m.type as string;
    if (type === 'status') {
      this.setStatus(m.status as ConnectionStatus);
      return;
    }
    if (type === 'snapshot') {
      const sub = this.subs.get(m.subId as string);
      if (!sub) return;
      const rows = (m.rows as Row[]) ?? [];
      sub.cb.onSnapshotStart?.(rows.length);
      sub.cb.onSnapshot?.(rows);
      return;
    }
    if (type === 'update') {
      // Legacy single-row path. Kept for any worker build that still
      // emits unbatched updates; new builds use 'updates' below.
      const sub = this.subs.get(m.subId as string);
      if (!sub) return;
      const row = m.row as Row | undefined;
      if (row) sub.cb.onUpdate(row);
      return;
    }
    if (type === 'updates') {
      // Batched-per-frame delivery from the worker. We unpack the
      // array and call onUpdate per row so consumers don't need to
      // change. Components that buffer to applyTransactionAsync
      // (Pivot, Positions, MarketData, Trades) still get the same
      // semantics — just with far fewer cross-thread postMessage
      // hops.
      const sub = this.subs.get(m.subId as string);
      if (!sub) return;
      const rows = m.rows as Row[] | undefined;
      if (!rows || rows.length === 0) return;
      for (const row of rows) sub.cb.onUpdate(row);
      return;
    }
  }
}

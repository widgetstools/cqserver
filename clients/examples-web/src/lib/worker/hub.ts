/**
 * cqserver worker hub — pure logic, no SharedWorker/DedicatedWorker
 * globals. Two tiny entry files (`cq-worker.shared.ts`,
 * `cq-worker.dedicated.ts`) construct the singleton hub and feed it
 * MessagePort-shaped objects via `attachPort`.
 *
 * Subscription sharing: two ports asking for the same
 * `(topic, filter, sql)` triple share one upstream cqserver sub. The
 * hub keeps a `subs` map keyed by that triple; each port's subscribe
 * call binds a local subId → canonical subId so per-port unsubscribes
 * decrement the canonical refcount and tear the cqserver sub down
 * only when the count hits zero.
 */
import { Client, Subscription } from '@cqserver/client';
import type { ClientMsg, ServerMsg } from './protocol';

const DEFAULT_WS_URL = 'ws://127.0.0.1:9008/cq/json';
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 8_000;

export interface HubPort {
  postMessage(msg: ServerMsg): void;
  addEventListener(type: 'message', cb: (e: MessageEvent<ClientMsg>) => void): void;
  start?(): void;
}

interface PortState {
  id: string;
  port: HubPort;
  /** Local subId on the port → canonical key in `subs`. */
  subs: Map<string, string>;
}

interface SharedSub {
  key: string;
  topic: string;
  filter?: string;
  sql?: string;
  refs: Set<{ portId: string; portSubId: string }>;
  /** Once the cqserver sub is up. null while still snapshotting/connecting. */
  sub: Subscription | null;
  /** Set after `whenSnapshotComplete()`. */
  isLive: boolean;
}

class Hub {
  private client: Client | null = null;
  private clientPromise: Promise<Client> | null = null;
  private ports = new Map<string, PortState>();
  private subs = new Map<string, SharedSub>();
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private static portCounter = 0;

  attachPort(port: HubPort, autoId = `p${++Hub.portCounter}`): void {
    port.start?.();
    const state: PortState = { id: autoId, port, subs: new Map() };
    this.ports.set(autoId, state);
    port.addEventListener('message', (e) => this.onClientMsg(state, e.data));
    if (this.client) port.postMessage({ kind: 'connected' });
    void this.ensureClient();
  }

  private broadcast(msg: ServerMsg): void {
    for (const p of this.ports.values()) p.port.postMessage(msg);
  }

  private send(portId: string, msg: ServerMsg): void {
    const p = this.ports.get(portId);
    if (p) p.port.postMessage(msg);
  }

  private onClientMsg(state: PortState, msg: ClientMsg): void {
    switch (msg.kind) {
      case 'hello':
        this.send(state.id, { kind: 'hello-ack', sharedWorker: true });
        return;
      case 'ping':
        this.send(state.id, { kind: 'pong' });
        return;
      case 'subscribe':
        void this.handleSubscribe(state, msg);
        return;
      case 'unsubscribe':
        this.handleUnsubscribe(state, msg.subId);
        return;
    }
  }

  // ─── Subscribe path ──────────────────────────────────────────
  private subKey(topic: string, filter?: string, sql?: string): string {
    return `${topic}\x00${filter ?? ''}\x00${sql ?? ''}`;
  }

  private async handleSubscribe(
    state: PortState,
    msg: Extract<ClientMsg, { kind: 'subscribe' }>,
  ): Promise<void> {
    if (state.subs.has(msg.subId)) {
      this.send(state.id, {
        kind: 'error',
        subId: msg.subId,
        message: `duplicate subscribe for subId ${msg.subId}`,
      });
      return;
    }
    const key = this.subKey(msg.topic, msg.filter, msg.sql);
    state.subs.set(msg.subId, key);
    let shared = this.subs.get(key);
    if (!shared) {
      shared = {
        key,
        topic: msg.topic,
        filter: msg.filter,
        sql: msg.sql,
        refs: new Set(),
        sub: null,
        isLive: false,
      };
      this.subs.set(key, shared);
    }
    shared.refs.add({ portId: state.id, portSubId: msg.subId });
    this.send(state.id, { kind: 'status', subId: msg.subId, status: 'snapshotting' });
    try {
      const client = await this.ensureClient();
      // Only the FIRST refer opens the upstream subscription.
      if (!shared.sub) await this.openShared(shared, client);
      else if (shared.isLive) {
        // Already live — Task 4 will replay the SOW for this newcomer.
        // For Task 3 we just promote it; replay lands in Task 4.
        this.send(state.id, { kind: 'status', subId: msg.subId, status: 'live' });
      }
    } catch (err) {
      this.send(state.id, {
        kind: 'error',
        subId: msg.subId,
        message: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private async openShared(shared: SharedSub, client: Client): Promise<void> {
    const sub = await client.sowAndSubscribe(shared.topic, {
      filter: shared.filter,
      sql: shared.sql,
    });
    shared.sub = sub;
    void sub.whenSnapshotComplete().then(() => {
      shared.isLive = true;
      for (const ref of shared.refs) {
        this.send(ref.portId, {
          kind: 'status',
          subId: ref.portSubId,
          status: 'live',
        });
      }
    });
    // Drain deltas. Task 4 chunks the SOW; Task 5 coalesces live deltas.
    // For Task 3 we just keep the loop alive so the SDK doesn't leak the sub.
    void (async () => {
      try {
        for await (const _delta of sub) {
          // intentionally empty in Task 3 — Tasks 4-5 install the
          // actual delta routing.
          void _delta;
        }
      } catch (err) {
        for (const ref of shared.refs) {
          this.send(ref.portId, {
            kind: 'error',
            subId: ref.portSubId,
            message: err instanceof Error ? err.message : String(err),
          });
        }
      }
    })();
  }

  // ─── Unsubscribe path ────────────────────────────────────────
  private handleUnsubscribe(state: PortState, portSubId: string): void {
    const key = state.subs.get(portSubId);
    if (!key) return;
    state.subs.delete(portSubId);
    const shared = this.subs.get(key);
    if (!shared) return;
    for (const ref of shared.refs) {
      if (ref.portId === state.id && ref.portSubId === portSubId) {
        shared.refs.delete(ref);
        break;
      }
    }
    if (shared.refs.size === 0) {
      const upstreamId = shared.sub?.subId;
      this.subs.delete(key);
      shared.sub = null;
      if (this.client && upstreamId) {
        void this.client.unsubscribe(upstreamId).catch(() => {});
      }
    }
  }

  // ─── Connection lifecycle ────────────────────────────────────
  private ensureClient(): Promise<Client> {
    if (this.client) return Promise.resolve(this.client);
    if (this.clientPromise) return this.clientPromise;
    const url =
      (globalThis as { CQ_WS_URL?: string }).CQ_WS_URL ?? DEFAULT_WS_URL;
    this.clientPromise = Client.connect(url, { heartbeatIntervalMs: 25_000 })
      .then((c) => {
        this.client = c;
        this.reconnectAttempts = 0;
        c.onClose(() => this.handleClose());
        this.broadcast({ kind: 'connected' });
        return c;
      })
      .catch((err) => {
        // eslint-disable-next-line no-console
        console.warn('[cq-worker] connect failed', err);
        this.clientPromise = null;
        this.broadcast({
          kind: 'disconnected',
          reason: err instanceof Error ? err.message : String(err),
        });
        this.scheduleReconnect();
        throw err;
      });
    return this.clientPromise;
  }

  private handleClose(): void {
    this.client = null;
    this.clientPromise = null;
    // Mark every shared sub as torn down — Task 6 will resubscribe.
    for (const shared of this.subs.values()) {
      shared.sub = null;
      shared.isLive = false;
    }
    this.broadcast({ kind: 'disconnected' });
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;
    this.reconnectAttempts++;
    const delay = Math.min(
      RECONNECT_MAX_MS,
      RECONNECT_BASE_MS * 2 ** Math.min(this.reconnectAttempts - 1, 6),
    );
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.ensureClient().catch(() => {
        /* failure already broadcast + reschedules */
      });
    }, delay);
  }
}

export const hub = new Hub();

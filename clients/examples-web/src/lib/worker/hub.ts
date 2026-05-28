/**
 * cqserver worker hub — pure logic, no SharedWorker/DedicatedWorker
 * globals. Two tiny entry files (`cq-worker.shared.ts`,
 * `cq-worker.dedicated.ts`) construct the singleton hub and feed it
 * MessagePort-shaped objects via `attachPort`.
 *
 * Lifecycle:
 *   - First port attaches → `connect()` runs, hub holds a `Client`.
 *   - Subsequent ports attach → each gets `{kind:'connected'}` immediately
 *     (the SDK client is already up).
 *   - Connection drops → every port hears `{kind:'disconnected'}`; the
 *     hub schedules a reconnect; on success every port resubscribes its
 *     subs (Task 6).
 *   - Last port detaches → SDK client is kept alive (it costs nothing to
 *     hold a WS, and the next tab is probably one click away). Subs that
 *     no port references are unsubscribed lazily by Task 3.
 */
import { Client } from '@cqserver/client';
import type { ClientMsg, ServerMsg } from './protocol';

const DEFAULT_WS_URL = 'ws://127.0.0.1:9008/cq/json';
const RECONNECT_BASE_MS = 500;
const RECONNECT_MAX_MS = 8_000;

/** A port we talk to (either a SharedWorker's `MessagePort` or the
 *  dedicated worker's `self`). Both shapes expose the same surface. */
export interface HubPort {
  postMessage(msg: ServerMsg): void;
  addEventListener(type: 'message', cb: (e: MessageEvent<ClientMsg>) => void): void;
  // Optional — only SharedWorker MessagePorts need `start()`.
  start?(): void;
}

interface PortState {
  id: string;
  port: HubPort;
}

class Hub {
  private client: Client | null = null;
  private clientPromise: Promise<Client> | null = null;
  private ports = new Map<string, PortState>();
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  /** Called by the worker entry on every new port. */
  attachPort(port: HubPort, autoId = `p${++Hub.portCounter}`): void {
    port.start?.();
    const state: PortState = { id: autoId, port };
    this.ports.set(autoId, state);
    port.addEventListener('message', (e) => this.onClientMsg(state, e.data));
    // Tell the port whether we're already connected.
    if (this.client) port.postMessage({ kind: 'connected' });
    void this.ensureClient();
  }

  private static portCounter = 0;

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
        // Task 3 will store tabId for diagnostics; Phase 2 ignores it.
        this.send(state.id, { kind: 'hello-ack', sharedWorker: true });
        return;
      case 'ping':
        this.send(state.id, { kind: 'pong' });
        return;
      case 'subscribe':
      case 'unsubscribe':
        // Task 3 implements these. For now, surface an error so a
        // mis-wired call doesn't fail silently.
        this.send(state.id, {
          kind: 'error',
          subId: 'subId' in msg ? msg.subId : undefined,
          message: 'subscribe/unsubscribe not yet implemented',
        });
        return;
    }
  }

  /** Resolves with the active SDK client. Concurrent calls share a
   *  single in-flight connect; reconnects replace the inner promise. */
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
        /* failure already broadcast + reschedules itself */
      });
    }, delay);
  }
}

/** Singleton. Both worker entry files attach ports to the same instance. */
export const hub = new Hub();

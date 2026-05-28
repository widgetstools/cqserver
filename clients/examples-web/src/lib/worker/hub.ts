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
import type { Delta } from '@cqserver/client';
import type { ClientMsg, ServerMsg, Row } from './protocol';
import { SOW_CHUNK_ROWS, COALESCE_MS } from './protocol';

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
  /** Resolves when `openShared` completes for this key. Lets a second
   *  port subscribing to the same `(topic, filter, sql)` triple wait
   *  on the first port's open instead of racing it and leaking a
   *  duplicate upstream sub on the server. */
  openingPromise: Promise<void> | null;
  /** Set after `whenSnapshotComplete()`. */
  isLive: boolean;
  /** Worker-side row mirror for SOW replay on late join + Task 6 resub. */
  rows: Map<string, Row>;
  pendingAdd: Row[];
  pendingUpdate: Row[];
  pendingRemove: Row[];
  coalesceTimer: ReturnType<typeof setTimeout> | null;
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

  private rowKey(row: Row): string {
    // Best-effort key: prefer common id columns, then composite. The
    // main thread also derives keys when applying deltas, but the
    // worker only uses this for its own row mirror (replay on
    // resubscribe). It does NOT need to match the chapter's getRowId.
    if (typeof row.position_id === 'string') return row.position_id;
    if (typeof row.trade_id === 'string') return row.trade_id;
    if (typeof row.cusip === 'string') return String(row.cusip);
    // Fallback: stringify a stable subset.
    return JSON.stringify(row);
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
        openingPromise: null,
        isLive: false,
        rows: new Map(),
        pendingAdd: [],
        pendingUpdate: [],
        pendingRemove: [],
        coalesceTimer: null,
      };
      this.subs.set(key, shared);
    }
    shared.refs.add({ portId: state.id, portSubId: msg.subId });
    this.send(state.id, { kind: 'status', subId: msg.subId, status: 'snapshotting' });
    try {
      const client = await this.ensureClient();
      // Capture pre-await state so we know whether to do the late-join
      // replay below. A "true late joiner" is a port that arrives after
      // openShared has already completed (`shared.sub != null && isLive`);
      // the first opener and any racing ports that join mid-open are
      // already in `shared.refs` when openShared's `flushSowChunks`
      // walks it, so they're covered by the fan-out.
      const needLateReplay = shared.sub != null && shared.isLive;
      if (shared.sub == null) {
        // Race guard: if another port is mid-open for this key, await
        // its promise instead of starting a duplicate sowAndSubscribe.
        if (shared.openingPromise == null) {
          shared.openingPromise = this.openShared(shared, client).finally(() => {
            shared.openingPromise = null;
          });
        }
        await shared.openingPromise;
      }
      if (needLateReplay) {
        // Replay the worker's mirrored rows so the newcomer paints in.
        this.sendSowChunked({ portId: state.id, portSubId: msg.subId }, Array.from(shared.rows.values()));
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
    const sowBuffer: Row[] = [];
    let sowDone = false;
    void sub.whenSnapshotComplete().then(() => {
      sowDone = true;
      this.flushSowChunks(shared, sowBuffer);
      sowBuffer.length = 0;
      shared.isLive = true;
      for (const ref of shared.refs) {
        this.send(ref.portId, { kind: 'status', subId: ref.portSubId, status: 'live' });
      }
    });
    void (async () => {
      try {
        for await (const d of sub) {
          const row = d.data as Row;
          const key = this.rowKey(row);
          if (d.deltaType === 'remove' || d.deltaType === 'oof') {
            shared.rows.delete(key);
          } else {
            shared.rows.set(key, row);
          }
          if (!sowDone) {
            if (d.deltaType !== 'remove' && d.deltaType !== 'oof') sowBuffer.push(row);
          } else {
            this.enqueueDelta(shared, d.deltaType, row);
          }
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

  /** Stream the in-memory row mirror out to every ref'd port in chunks. */
  private flushSowChunks(shared: SharedSub, override?: Row[]): void {
    const source = override ?? Array.from(shared.rows.values());
    for (const ref of shared.refs) {
      this.sendSowChunked(ref, source);
    }
  }

  private sendSowChunked(
    ref: { portId: string; portSubId: string },
    rows: Row[],
  ): void {
    // Empty SOW still needs the terminating `more:false` so the main
    // thread can flip to live without an awkward "did the snapshot
    // ever land?" timeout.
    if (rows.length === 0) {
      this.send(ref.portId, {
        kind: 'snapshot',
        subId: ref.portSubId,
        chunk: [],
        more: false,
      });
      return;
    }
    for (let i = 0; i < rows.length; i += SOW_CHUNK_ROWS) {
      const slice = rows.slice(i, i + SOW_CHUNK_ROWS);
      const more = i + SOW_CHUNK_ROWS < rows.length;
      this.send(ref.portId, {
        kind: 'snapshot',
        subId: ref.portSubId,
        chunk: slice,
        more,
      });
    }
  }

  private enqueueDelta(shared: SharedSub, kind: Delta['deltaType'], row: Row): void {
    if (kind === 'remove' || kind === 'oof') shared.pendingRemove.push(row);
    else if (kind === 'add') shared.pendingAdd.push(row);
    else shared.pendingUpdate.push(row);
    if (shared.coalesceTimer == null) {
      shared.coalesceTimer = setTimeout(() => this.flushPending(shared), COALESCE_MS);
    }
  }

  private flushPending(shared: SharedSub): void {
    shared.coalesceTimer = null;
    if (
      shared.pendingAdd.length === 0 &&
      shared.pendingUpdate.length === 0 &&
      shared.pendingRemove.length === 0
    ) {
      return;
    }
    const add = shared.pendingAdd;
    const update = shared.pendingUpdate;
    const remove = shared.pendingRemove;
    shared.pendingAdd = [];
    shared.pendingUpdate = [];
    shared.pendingRemove = [];
    for (const ref of shared.refs) {
      this.send(ref.portId, {
        kind: 'delta',
        subId: ref.portSubId,
        add,
        update,
        remove,
      });
    }
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
      if (shared.coalesceTimer != null) {
        clearTimeout(shared.coalesceTimer);
        shared.coalesceTimer = null;
      }
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
        // Re-open every shared sub that still has refs.
        for (const shared of this.subs.values()) {
          if (shared.refs.size === 0) continue;
          for (const ref of shared.refs) {
            this.send(ref.portId, { kind: 'status', subId: ref.portSubId, status: 'snapshotting' });
          }
          void this.openShared(shared, c).catch((err) => {
            for (const ref of shared.refs) {
              this.send(ref.portId, {
                kind: 'error',
                subId: ref.portSubId,
                message: err instanceof Error ? err.message : String(err),
              });
            }
          });
        }
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
    // Tear down every shared sub's upstream handle but keep the port refs.
    for (const shared of this.subs.values()) {
      shared.sub = null;
      shared.isLive = false;
      shared.rows.clear();
      shared.pendingAdd = [];
      shared.pendingUpdate = [];
      shared.pendingRemove = [];
      if (shared.coalesceTimer != null) {
        clearTimeout(shared.coalesceTimer);
        shared.coalesceTimer = null;
      }
      for (const ref of shared.refs) {
        this.send(ref.portId, { kind: 'status', subId: ref.portSubId, status: 'disconnected' });
      }
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

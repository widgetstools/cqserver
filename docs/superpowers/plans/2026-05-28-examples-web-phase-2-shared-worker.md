# examples-web Phase 2 — SharedWorker data layer

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every cqserver subscription off the main thread and into a SharedWorker (with a dedicated-Worker fallback), then delete `src/lib/cq-store.ts` and re-point all eight existing chapters at the new worker-backed hook so the legacy app keeps working unchanged until Phase 3+ migrates chapters into the new Atlas surface.

**Architecture:** A SharedWorker owns the `@cqserver/client` `Client` and the WebSocket — one connection per origin. Tabs talk to it through a typed message protocol; the worker ref-counts `(topic, filter, sql)` triples so two chapters that ask for the same view share one server subscription. Snapshots stream back as ~500-row chunks (so AG-Grid can paint progressively via `applyTransactionAsync({add: chunk})`); deltas are coalesced into a 50 ms post-message window so we never floor the main thread with 500 individual messages per second. Reconnect is supervised by the worker with capped exponential backoff and per-port re-subscription. The dedicated-Worker fallback preserves all behaviour except cross-tab connection sharing.

**Tech Stack:** Vite 7 worker imports (`?sharedworker`, `?worker`), `@cqserver/client` SDK (already aliased to `client-sdks/ts/dist/index.js`), React 19 `useSyncExternalStore`, AG-Grid v35 (consumer side unchanged in Phase 2).

---

## Pre-flight

This plan executes on a feature branch off `msrv-1.78` (Phase 1's branch). The implementer should `cd /Users/develop/cqserver` and verify Phase 1 is the tip:

```bash
git log --oneline -5
# tip should be 2c4a1ac5 fix(atlas): split #atlas branch into LegacyApp …
```

There is **pre-existing WIP everywhere in the working tree** (see `git status`). Every commit in this plan uses **precise `git add <paths>`** — never `git add -A`, never `git add .`. The implementer subagent is responsible for staging only the files the task names; spec-reviewers will FAIL the commit if extras are present.

**Atomicity:** the cqserver Atlas demo MUST continue to function at every commit boundary in this plan. The only commit that may transiently regress behaviour is Task 14 (the cq-store deletion), which is gated by the new worker being fully wired and exercised by Tasks 8-13.

**Worker model in dev:** Vite serves workers via the `?sharedworker` and `?worker` query suffixes. Both produce a typed constructor (`new CqWorker()`). The implementer does NOT need to touch `vite.config.ts` — `@cqserver/client` resolves the same in worker context as it does in the main bundle (verified: SDK is a single ESM file at `client-sdks/ts/dist/index.js` with the WS transport on top).

---

## File map

| Path | Status | Responsibility |
|---|---|---|
| `clients/examples-web/src/lib/worker/protocol.ts` | new | Typed message protocol (ClientMsg/ServerMsg/Row/Delta). Pure types, zero runtime. |
| `clients/examples-web/src/lib/worker/hub.ts` | new | All worker-side logic: `Client` ownership, port registry, ref-counted subs, chunked SOW, coalesced deltas, reconnect. |
| `clients/examples-web/src/lib/worker/cq-worker.shared.ts` | new | SharedWorker entry point — wires `self.onconnect` to `hub.attachPort()`. |
| `clients/examples-web/src/lib/worker/cq-worker.dedicated.ts` | new | Dedicated-Worker entry point — fallback for Safari etc. |
| `clients/examples-web/src/lib/worker/port.ts` | new | Main-thread bridge: SW detection + auto-fallback, typed `WorkerPort` (`request/onmessage/close`). |
| `clients/examples-web/src/lib/use-subscription.ts` | new | New canonical hook over `WorkerPort`. Returns `rows/status/size/subscribeSnapshotChunks/subscribeDeltas/getSnapshot`. |
| `clients/examples-web/src/lib/use-filtered-subscription.ts` | rewritten | Thin re-export so every existing call site (8 examples) keeps compiling. |
| `clients/examples-web/src/lib/use-live-query.ts` | rewritten | SQL-flavour subscription over the port. Same `LiveQueryHandle` surface. |
| `clients/examples-web/src/lib/use-filtered-aggregate.ts` | rewritten | Single-row aggregate subscription over the port. Same surface. |
| `clients/examples-web/src/components/panels/GridPanel.tsx` | modified | Drop topic-bound mode + `tickTopic`. Only `liveSubscription` consumed. |
| `clients/examples-web/src/components/atlas/ContextBar.tsx` | modified | Drop `cqStore`/`useTickCount`. Render static "—" placeholder for row count + ticks (Phase 3 will wire scope). |
| `clients/examples-web/src/examples/ex01-live-pnl/index.tsx` | modified | Replace `topic="positions"` GridPanel with `useSubscription('/positions', null)` + `liveSubscription={…}`. |
| `clients/examples-web/src/examples/ex06-joins/index.tsx` | modified | Same for `/positions` and `/trades`. |
| `clients/examples-web/src/App.tsx` | modified | Remove `import '@/lib/cq-store';` side-effect. |
| `clients/examples-web/src/lib/cq-store.ts` | **deleted** | All behaviour migrated to the worker. |

---

## Task 1: Worker message protocol (types only)

**Files:**
- Create: `clients/examples-web/src/lib/worker/protocol.ts`

The whole worker boundary is typed by this one file. Both sides import the same symbols; the wire is JSON-serialisable per-`postMessage` (no transferables in Phase 2 — we're not yet using ArrayBuffer payloads).

- [ ] **Step 1: Write the file**

```ts
/**
 * Typed message protocol shared by the main thread and the
 * cqserver SharedWorker. Both directions are JSON-serialisable
 * (no transferables yet — Phase 2 keeps everything plain Row).
 *
 * The worker is the source of truth: it owns the WebSocket, the
 * `Client`, every reference-counted subscription, and the coalesce
 * buffer. The main thread holds React hooks that mirror per-port
 * state derived from `ServerMsg`s.
 */
export type Row = Record<string, unknown>;

export type ConnectionStatus =
  | 'connecting'
  | 'snapshotting'
  | 'live'
  | 'disconnected'
  | 'error';

/** Main → Worker. */
export type ClientMsg =
  | { kind: 'hello'; tabId: string }
  | {
      kind: 'subscribe';
      subId: string;
      topic: string;
      filter?: string;
      /** Inline SQL — mutually exclusive with `filter`. */
      sql?: string;
    }
  | { kind: 'unsubscribe'; subId: string }
  | { kind: 'ping' };

/** Worker → Main. */
export type ServerMsg =
  | { kind: 'hello-ack'; sharedWorker: boolean }
  | { kind: 'connected' }
  | { kind: 'disconnected'; reason?: string }
  | { kind: 'status'; subId: string; status: ConnectionStatus }
  | {
      kind: 'snapshot';
      subId: string;
      /** This chunk of the SOW. */
      chunk: Row[];
      /** False on the final chunk; true while more chunks are pending. */
      more: boolean;
    }
  | {
      kind: 'delta';
      subId: string;
      /** Coalesced over the last ~50ms. Lists are stable per message. */
      add: Row[];
      update: Row[];
      remove: Row[];
    }
  | { kind: 'error'; subId?: string; message: string }
  | { kind: 'pong' };

/** Chunk size for progressive SOW delivery — keeps each postMessage cheap. */
export const SOW_CHUNK_ROWS = 500;

/** Coalesce window for live deltas (ms). Matches the legacy COALESCE_MS. */
export const COALESCE_MS = 50;
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/protocol.ts
git commit -m "feat(worker): protocol — typed Main↔Worker message contract

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Worker hub — connection, dispatcher, heartbeat

**Files:**
- Create: `clients/examples-web/src/lib/worker/hub.ts`

This task lands the skeleton: the worker connects to cqserver on first `attachPort`, tells every attached port `{kind:'connected'}` on success and `{kind:'disconnected'}` on close, but does **not** yet handle subscriptions (that's Task 3). The port registry is established here.

- [ ] **Step 1: Write the file**

```ts
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
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/hub.ts
git commit -m "feat(worker): hub skeleton — connection, port registry, reconnect

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Worker hub — ref-counted subscribe / unsubscribe

**Files:**
- Modify: `clients/examples-web/src/lib/worker/hub.ts`

Now wire the subscribe path. A key insight from the spec: two ports asking for the **same** `(topic, filter, sql)` triple share one cqserver subscription. The hub maintains an inverted index `key → { subscription, refCount, ports: Set<{portId, subId}> }`. When a port subscribes, we add it; on the second add we just fan the existing stream. On unsubscribe we decrement; when refCount hits zero we tear the cqserver sub down.

Each port can use its OWN local `subId` strings — the hub maps them to its internal canonical id. This keeps per-port bookkeeping simple on the main side.

- [ ] **Step 1: Replace the file with the extended hub**

```ts
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
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/hub.ts
git commit -m "feat(worker): ref-counted subscribe/unsubscribe with per-port subId map

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Worker hub — progressive SOW chunking

**Files:**
- Modify: `clients/examples-web/src/lib/worker/hub.ts`

The hub buffers SOW rows (deltas that arrive before `group_end`) into a queue, then flushes them in `SOW_CHUNK_ROWS`-sized chunks to every port that's referenced this shared sub. The last chunk carries `more: false`; subsequent live deltas land via Task 5's coalescer.

A second-arriving port for an already-live shared sub gets its own one-shot SOW replay from the hub's row map (which we now also maintain).

- [ ] **Step 1: Extend the file**

Find the `SharedSub` interface and add a `rows` field + `sowBuffer` field:

```ts
interface SharedSub {
  key: string;
  topic: string;
  filter?: string;
  sql?: string;
  refs: Set<{ portId: string; portSubId: string }>;
  sub: Subscription | null;
  isLive: boolean;
  /** Worker-side row mirror for SOW replay on late join + Task 6 resub. */
  rows: Map<string, Row>;
  /** Per-port SOW progress so a port that joined late gets its own chunked replay. */
  newcomers: { portId: string; portSubId: string }[];
}
```

Update the initial `shared = { … }` literal in `handleSubscribe` to set `rows: new Map()` and `newcomers: []`.

Then add a `rowKey(row)` helper at the top of the class:

```ts
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
```

Rewrite `openShared` so SOW deltas are buffered and flushed in chunks once `group_end` fires:

```ts
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

/** Stub for Task 5. Task 4 only needs the function to exist so
 *  `openShared`'s post-SOW branch compiles. */
private enqueueDelta(_shared: SharedSub, _kind: Delta['deltaType'], _row: Row): void {
  void _shared;
  void _kind;
  void _row;
}
```

Update the import line at the top of the file:

```ts
import type { ClientMsg, ServerMsg, Row } from './protocol';
import { SOW_CHUNK_ROWS } from './protocol';
```

Update the late-joiner branch in `handleSubscribe` so a port joining an already-live shared sub gets its own SOW replay instead of just a status flip:

```ts
} else if (shared.isLive) {
  // Replay the worker's mirrored rows so the newcomer paints in.
  this.sendSowChunked({ portId: state.id, portSubId: msg.subId }, Array.from(shared.rows.values()));
  this.send(state.id, { kind: 'status', subId: msg.subId, status: 'live' });
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/hub.ts
git commit -m "feat(worker): chunked SOW (~500 rows/msg) + late-joiner replay

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Worker hub — coalesced live deltas (50 ms window)

**Files:**
- Modify: `clients/examples-web/src/lib/worker/hub.ts`

Replace the Task 4 `enqueueDelta` stub with a real coalescer. Per shared sub, accumulate `add/update/remove` lists; on the first delta that lands within a quiet window, schedule a flush at `now + COALESCE_MS`. On flush, post the batched arrays to every ref'd port and clear.

- [ ] **Step 1: Extend the file**

Update the `SharedSub` interface — add coalesce fields:

```ts
interface SharedSub {
  key: string;
  topic: string;
  filter?: string;
  sql?: string;
  refs: Set<{ portId: string; portSubId: string }>;
  sub: Subscription | null;
  isLive: boolean;
  rows: Map<string, Row>;
  newcomers: { portId: string; portSubId: string }[];
  pendingAdd: Row[];
  pendingUpdate: Row[];
  pendingRemove: Row[];
  coalesceTimer: ReturnType<typeof setTimeout> | null;
}
```

Add `pendingAdd: [], pendingUpdate: [], pendingRemove: [], coalesceTimer: null` to the shared-sub initialiser in `handleSubscribe`.

Add to the protocol import:

```ts
import { SOW_CHUNK_ROWS, COALESCE_MS } from './protocol';
```

Replace the stub `enqueueDelta` with:

```ts
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
```

Also: when a port unsubscribes and `shared.refs.size === 0`, cancel any pending coalesce timer in `handleUnsubscribe`:

```ts
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
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/hub.ts
git commit -m "feat(worker): coalesce live deltas in a 50ms window

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Worker hub — resubscribe on reconnect

**Files:**
- Modify: `clients/examples-web/src/lib/worker/hub.ts`

When `handleClose` runs, the hub has lost its upstream subs but every port still has refs in the `subs` map. After `ensureClient()` succeeds on reconnect, walk every shared sub whose `refs.size > 0` and re-open it via `openShared`. Each ref'd port gets a fresh `{kind:'status', status:'snapshotting'}` and the SOW replays. Drop any rows from the old mirror first so a sub's snapshot reflects the server's current state, not what the worker had cached.

- [ ] **Step 1: Extend the file**

Update `handleClose`:

```ts
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
```

Update `ensureClient` so the `.then((c) => …)` arm also resubscribes:

```ts
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
  …
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/hub.ts
git commit -m "feat(worker): re-open every ref'd sub after reconnect

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Worker entry files + dedicated-Worker fallback

**Files:**
- Create: `clients/examples-web/src/lib/worker/cq-worker.shared.ts`
- Create: `clients/examples-web/src/lib/worker/cq-worker.dedicated.ts`

Two thin shells around `hub`. Both end up being a handful of lines each. The dedicated-worker version treats `self` as a MessagePort.

- [ ] **Step 1: Write `cq-worker.shared.ts`**

```ts
/// <reference lib="webworker" />
/**
 * SharedWorker entry — wires each connecting port into the singleton
 * `hub`. The hub does the heavy lifting; this file only adapts the
 * SharedWorker `onconnect` event shape into a `HubPort`.
 */
import { hub, type HubPort } from './hub';

const scope = self as unknown as SharedWorkerGlobalScope;

scope.onconnect = (event: MessageEvent) => {
  const port = event.ports[0];
  if (!port) return;
  const hubPort: HubPort = {
    postMessage: (msg) => port.postMessage(msg),
    addEventListener: (type, cb) => port.addEventListener(type, cb as EventListener),
    start: () => port.start(),
  };
  hub.attachPort(hubPort);
};
```

- [ ] **Step 2: Write `cq-worker.dedicated.ts`**

```ts
/// <reference lib="webworker" />
/**
 * Dedicated-Worker fallback entry. Used when the browser doesn't
 * expose `SharedWorker` (Safari before its 2026 release, mobile WKWebView).
 * Loses the cross-tab connection-sharing benefit; preserves every
 * other Phase 2 win — off-main JSON parse, chunked SOW, coalesced
 * deltas, supervised reconnect.
 */
import { hub, type HubPort } from './hub';

const scope = self as unknown as DedicatedWorkerGlobalScope;

const hubPort: HubPort = {
  postMessage: (msg) => scope.postMessage(msg),
  addEventListener: (type, cb) => scope.addEventListener(type, cb as EventListener),
};
hub.attachPort(hubPort);
```

- [ ] **Step 3: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/lib/worker/cq-worker.shared.ts clients/examples-web/src/lib/worker/cq-worker.dedicated.ts
git commit -m "feat(worker): SharedWorker + dedicated-Worker entry shells

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Main-thread port bridge with SharedWorker auto-fallback

**Files:**
- Create: `clients/examples-web/src/lib/worker/port.ts`

The single API the React hooks use. Detects SharedWorker; falls back to a dedicated Worker per tab. Exposes:

- `getCqPort()` — singleton, lazily instantiates the worker on first call.
- `WorkerPort` interface — `request(msg)`, `onMessage(cb)`, `onConnectionChange(cb)`, `connectionStatus` getter.

- [ ] **Step 1: Write the file**

```ts
/**
 * Main-thread bridge to the cqserver worker.
 *
 * One SharedWorker per origin when the browser supports it; one
 * dedicated Worker per tab otherwise. Both expose the same typed
 * `WorkerPort` to the React hooks above — `subId`-tagged
 * subscribe/unsubscribe requests and a single `onMessage` fan-out.
 *
 * Vite resolves the worker entry URLs at build time via `?sharedworker`
 * and `?worker`; both produce a constructor that knows where the
 * bundle lives.
 */
import SharedWorkerCtor from './cq-worker.shared.ts?sharedworker';
import DedicatedWorkerCtor from './cq-worker.dedicated.ts?worker';
import type { ClientMsg, ServerMsg } from './protocol';

export type WorkerConnectionStatus = 'connecting' | 'connected' | 'disconnected';

export interface WorkerPort {
  /** Fire-and-forget. The hub responds via `onMessage`. */
  send(msg: ClientMsg): void;
  /** Subscribe to every message from the hub. Returns an unsubscribe fn. */
  onMessage(cb: (msg: ServerMsg) => void): () => void;
  /** Subscribe to top-level connection state. Returns an unsubscribe fn. */
  onConnectionChange(cb: (status: WorkerConnectionStatus) => void): () => void;
  /** Current top-level worker-connection status. */
  getConnectionStatus(): WorkerConnectionStatus;
  /** True if running over a SharedWorker (cross-tab connection sharing). */
  isShared(): boolean;
}

class Port implements WorkerPort {
  private listeners = new Set<(msg: ServerMsg) => void>();
  private connListeners = new Set<(s: WorkerConnectionStatus) => void>();
  private connStatus: WorkerConnectionStatus = 'connecting';
  private shared: boolean;
  private postFn: (msg: ClientMsg) => void;

  constructor(
    shared: boolean,
    postFn: (msg: ClientMsg) => void,
    addMessageListener: (cb: (e: MessageEvent<ServerMsg>) => void) => void,
  ) {
    this.shared = shared;
    this.postFn = postFn;
    addMessageListener((e) => this.onIncoming(e.data));
    // Tell the worker we're here so it returns a `hello-ack`. This also
    // triggers the connection bootstrap if no other port has yet.
    const tabId = `t${Math.random().toString(36).slice(2, 10)}`;
    this.postFn({ kind: 'hello', tabId });
  }

  private setConnStatus(s: WorkerConnectionStatus): void {
    if (this.connStatus === s) return;
    this.connStatus = s;
    for (const cb of this.connListeners) cb(s);
  }

  private onIncoming(msg: ServerMsg): void {
    if (msg.kind === 'connected') this.setConnStatus('connected');
    else if (msg.kind === 'disconnected') this.setConnStatus('disconnected');
    for (const cb of this.listeners) cb(msg);
  }

  send(msg: ClientMsg): void {
    this.postFn(msg);
  }
  onMessage(cb: (msg: ServerMsg) => void): () => void {
    this.listeners.add(cb);
    return () => this.listeners.delete(cb);
  }
  onConnectionChange(cb: (s: WorkerConnectionStatus) => void): () => void {
    this.connListeners.add(cb);
    return () => this.connListeners.delete(cb);
  }
  getConnectionStatus(): WorkerConnectionStatus {
    return this.connStatus;
  }
  isShared(): boolean {
    return this.shared;
  }
}

let portSingleton: WorkerPort | null = null;

export function getCqPort(): WorkerPort {
  if (portSingleton) return portSingleton;
  // SharedWorker preferred. Safari WebView etc. expose neither — falling
  // back to a real `Worker` keeps a single tab functional even if we
  // lose cross-tab sharing.
  const hasShared = typeof SharedWorker !== 'undefined';
  if (hasShared) {
    const sw = new SharedWorkerCtor({ name: 'cqserver-hub' });
    sw.port.start();
    portSingleton = new Port(
      true,
      (m) => sw.port.postMessage(m),
      (cb) => sw.port.addEventListener('message', cb as EventListener),
    );
  } else {
    const w = new DedicatedWorkerCtor({ name: 'cqserver-hub' });
    portSingleton = new Port(
      false,
      (m) => w.postMessage(m),
      (cb) => w.addEventListener('message', cb as EventListener),
    );
  }
  return portSingleton;
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

If TypeScript complains about the `?sharedworker` / `?worker` imports having no type, add a tiny ambient declaration. Create `clients/examples-web/src/vite-worker.d.ts`:

```ts
declare module '*?sharedworker' {
  const Ctor: new (options?: WorkerOptions) => SharedWorker;
  export default Ctor;
}
declare module '*?worker' {
  const Ctor: new (options?: WorkerOptions) => Worker;
  export default Ctor;
}
```

If that file already existed (it doesn't on `msrv-1.78`'s tip — verified), stage and commit it in this same task.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/worker/port.ts
# If the ambient .d.ts was needed:
git add clients/examples-web/src/vite-worker.d.ts
git commit -m "feat(worker): main-thread port bridge with SharedWorker → Worker fallback

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: New `useSubscription` hook

**Files:**
- Create: `clients/examples-web/src/lib/use-subscription.ts`

The single React hook the chapters use. The returned handle's shape is a strict superset of the legacy `FilteredSubscription` — same six fields, plus the new `subscribeSnapshotChunks` for progressive painting. AG-Grid panels can stay on the existing `subscribeBatchedDeltas` + `getSnapshot` for Phase 2; the new chunk callback is wired in Phase 3 when `DataTable` switches to `applyTransactionAsync` on the SOW path.

A subscription handle's identity is stable across re-renders so consumers that use it as a useMemo / useEffect dependency don't churn. The `rows / status / size` fields are updated in place each render — same pattern as the existing `useFilteredSubscription`.

- [ ] **Step 1: Write the file**

```ts
/**
 * `useSubscription` — open a cqserver subscription over the
 * SharedWorker port. Replaces the legacy `useFilteredSubscription`;
 * the legacy file is now a thin re-export so existing chapters keep
 * compiling unchanged until Phase 3+ migrates them.
 *
 * Surface (strict superset of the legacy `FilteredSubscription`):
 *   rows                       — current snapshot, rebuilt on each
 *                                main-thread coalesce flush.
 *   status                     — 'connecting' | 'snapshotting' | 'live' | …
 *   size                       — rows.length, exposed for fast badges.
 *   subscribeSnapshotChunks(cb)— NEW. Fires once per chunk during the
 *                                SOW (more=true), then once with more=false.
 *   subscribeDeltas(cb)        — Live `{add, update, remove}` batches.
 *   subscribeBatchedDeltas(cb) — Legacy alias for `subscribeDeltas`.
 *   subscribeStatus(cb)        — Reactive status changes.
 *   getSnapshot() / getStatus() / getSize() — imperative accessors.
 *
 * Lifecycle:
 *   - filter / sql change → close current sub, open a new one
 *   - worker reconnect    → sub re-opens automatically (worker drives it)
 *   - component unmounts  → sub closes (deferred to survive StrictMode)
 */
import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { getCqPort } from './worker/port';
import type {
  ConnectionStatus,
  Row,
  ServerMsg,
} from './worker/protocol';

export type { ConnectionStatus, Row } from './worker/protocol';

export interface DeltaBatch {
  add: Row[];
  update: Row[];
  remove: Row[];
}

export interface SubscriptionHandle {
  rows: Row[];
  status: ConnectionStatus;
  size: number;
  /** Reactive status subscriber. */
  subscribeStatus: (cb: () => void) => () => void;
  /** Per-chunk SOW notifications. `more` is false on the final chunk. */
  subscribeSnapshotChunks: (cb: (chunk: Row[], more: boolean) => void) => () => void;
  /** Coalesced live delta batches. */
  subscribeDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  /** Legacy alias for `subscribeDeltas` — Phase 1 / 2 grids call this. */
  subscribeBatchedDeltas: (cb: (b: DeltaBatch) => void) => () => void;
  getSnapshot: () => Row[];
  getStatus: () => ConnectionStatus;
  getSize: () => number;
}

class Sub {
  private rows = new Map<string, Row>();
  private snap: Row[] = [];
  private status: ConnectionStatus = 'connecting';
  private listeners = new Set<() => void>();
  private statusListeners = new Set<() => void>();
  private chunkListeners = new Set<(chunk: Row[], more: boolean) => void>();
  private deltaListeners = new Set<(b: DeltaBatch) => void>();
  private subId = `s${Math.random().toString(36).slice(2, 10)}`;
  private off: (() => void) | null = null;
  private closed = false;
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private rowKey: (r: Row) => string;

  constructor(
    private readonly topic: string,
    private readonly filter: string | null,
    private readonly sql: string | null,
    rowIdKey: ((r: Row) => string) | undefined,
  ) {
    this.rowKey = rowIdKey ?? ((r) => {
      // Best-effort fallback so legacy chapters compile. Server-side
      // filter views already give us stable keys for most topics; this
      // path is taken only when a hook caller doesn't pass a getRowId.
      const candidate = r.position_id ?? r.trade_id ?? r.cusip ?? r.symbol;
      return candidate != null ? String(candidate) : JSON.stringify(r);
    });
    this.open();
  }

  private open(): void {
    if (this.closed) return;
    const port = getCqPort();
    this.off = port.onMessage((m) => this.onMsg(m));
    const req =
      this.sql != null
        ? { kind: 'subscribe' as const, subId: this.subId, topic: this.topic, sql: this.sql }
        : this.filter != null
          ? { kind: 'subscribe' as const, subId: this.subId, topic: this.topic, filter: this.filter }
          : { kind: 'subscribe' as const, subId: this.subId, topic: this.topic };
    port.send(req);
  }

  private onMsg(m: ServerMsg): void {
    switch (m.kind) {
      case 'status':
        if (m.subId !== this.subId) return;
        this.setStatus(m.status);
        return;
      case 'snapshot':
        if (m.subId !== this.subId) return;
        for (const r of m.chunk) this.rows.set(this.rowKey(r), r);
        for (const cb of this.chunkListeners) cb(m.chunk, m.more);
        if (!m.more) {
          this.snap = Array.from(this.rows.values());
          this.notifyRows();
        }
        return;
      case 'delta':
        if (m.subId !== this.subId) return;
        for (const r of m.add) this.rows.set(this.rowKey(r), r);
        for (const r of m.update) this.rows.set(this.rowKey(r), r);
        for (const r of m.remove) this.rows.delete(this.rowKey(r));
        this.snap = Array.from(this.rows.values());
        for (const cb of this.deltaListeners) cb({ add: m.add, update: m.update, remove: m.remove });
        this.notifyRows();
        return;
      case 'error':
        if (m.subId && m.subId !== this.subId) return;
        this.setStatus('error');
        return;
      case 'disconnected':
        this.setStatus('disconnected');
        return;
      case 'connected':
      case 'hello-ack':
      case 'pong':
        return;
    }
  }

  private setStatus(s: ConnectionStatus): void {
    if (this.status === s) return;
    this.status = s;
    for (const cb of this.statusListeners) cb();
  }
  private notifyRows(): void {
    for (const cb of this.listeners) cb();
  }

  // Public surface
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
  subscribeSnapshotChunks = (cb: (chunk: Row[], more: boolean) => void): (() => void) => {
    this.chunkListeners.add(cb);
    return () => {
      this.chunkListeners.delete(cb);
    };
  };
  subscribeDeltas = (cb: (b: DeltaBatch) => void): (() => void) => {
    this.deltaListeners.add(cb);
    return () => {
      this.deltaListeners.delete(cb);
    };
  };

  getSnapshot = (): Row[] => this.snap;
  getStatus = (): ConnectionStatus => this.status;
  getSize = (): number => this.rows.size;

  close(): void {
    if (this.closed) return;
    this.closed = true;
    const port = getCqPort();
    port.send({ kind: 'unsubscribe', subId: this.subId });
    this.off?.();
    this.off = null;
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
}

export function useSubscription(
  topic: string,
  filter: string | null,
  rowIdKey?: (r: Row) => string,
): SubscriptionHandle {
  const [sub, setSub] = useState<Sub>(() => new Sub(topic, filter, null, rowIdKey));
  const keyRef = useRef<{ topic: string; filter: string | null }>({ topic, filter });

  useEffect(() => {
    if (keyRef.current.topic === topic && keyRef.current.filter === filter) return;
    const next = new Sub(topic, filter, null, rowIdKey);
    keyRef.current = { topic, filter };
    setSub((prev) => {
      prev.close();
      return next;
    });
  }, [topic, filter, rowIdKey]);

  useEffect(() => {
    sub.cancelDeferredClose();
    return () => {
      sub.scheduleClose();
    };
  }, [sub]);

  const rows = useSyncExternalStore(sub.subscribe, sub.getSnapshot, () => [] as Row[]);
  const status = useSyncExternalStore(
    sub.subscribeStatus,
    sub.getStatus,
    () => 'connecting' as ConnectionStatus,
  );

  const wrapper = useMemo<SubscriptionHandle>(
    () => ({
      rows: [],
      status: 'connecting' as ConnectionStatus,
      size: 0,
      subscribeStatus: sub.subscribeStatus,
      subscribeSnapshotChunks: sub.subscribeSnapshotChunks,
      subscribeDeltas: sub.subscribeDeltas,
      subscribeBatchedDeltas: sub.subscribeDeltas, // legacy alias
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

/** SQL-flavour subscription used by `useLiveQuery` and
 *  `useFilteredAggregate`. Same `Sub` class, distinct entry point so the
 *  call sites read naturally. */
export function makeSqlSub(topic: string, sql: string, rowIdKey?: (r: Row) => string): Sub {
  return new Sub(topic, null, sql, rowIdKey);
}
export type { Sub };
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/use-subscription.ts
git commit -m "feat(hooks): useSubscription over the SharedWorker port

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Rewrite `useFilteredSubscription` as an alias

**Files:**
- Modify (full replace): `clients/examples-web/src/lib/use-filtered-subscription.ts`

Zero call-site changes — every existing example keeps compiling. Internally it's now just a re-export of `useSubscription` with the legacy topic-union type for callers that pass string literals.

- [ ] **Step 1: Replace the file**

```ts
/**
 * Legacy compatibility shim. The canonical hook is
 * `useSubscription` in ./use-subscription.ts — this file exists only
 * so the eight existing chapters keep compiling between Phase 2's
 * worker landing and Phase 3+'s chapter migration into the Atlas
 * surface. New code should import `useSubscription` directly.
 */
import { useSubscription, type SubscriptionHandle, type DeltaBatch, type Row } from './use-subscription';
import type { ConnectionStatus } from './worker/protocol';

export type { ConnectionStatus, Row, DeltaBatch } from './use-subscription';

/** Same topic union the previous hook accepted. Kept for the demo
 *  examples that pass string literals; new callers should pass any
 *  string topic to `useSubscription`. */
export type TopicName =
  | '/positions'
  | '/trades'
  | '/securities'
  | '/fi-market-data'
  | '/v_net_exposure'
  | '/v_slippage_venue_algo'
  | '/v_pnl_by_sector'
  | '/v_pnl_by_book'
  | '/v_compliance_counts'
  | '/v_cross_asset_pivot'
  | '/v_heatmap_sector_region'
  | '/v_trades_by_compliance'
  | '/v_book_totals';

export type FilteredSubscription = SubscriptionHandle;

export function useFilteredSubscription(
  topic: TopicName,
  filter: string | null,
): FilteredSubscription {
  return useSubscription(topic, filter);
}
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean — every existing example consumer compiles unchanged.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/use-filtered-subscription.ts
git commit -m "refactor(hooks): useFilteredSubscription becomes an alias for useSubscription

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Rewrite `useLiveQuery` over the port

**Files:**
- Modify (full replace): `clients/examples-web/src/lib/use-live-query.ts`

Same `LiveQueryHandle` surface (`FilteredSubscription` + `error`). Internally it drives `makeSqlSub` from `use-subscription.ts`.

- [ ] **Step 1: Replace the file**

```ts
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
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/use-live-query.ts
git commit -m "refactor(hooks): useLiveQuery driven by the SharedWorker port

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Rewrite `useFilteredAggregate` over the port

**Files:**
- Modify (full replace): `clients/examples-web/src/lib/use-filtered-aggregate.ts`

Single-row aggregate. The worker stream is the same SQL flavour as `useLiveQuery` — every coalesce flush we just take the latest row as the aggregate value.

- [ ] **Step 1: Replace the file**

```ts
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
  /** Imperative accessor for the latest row. */
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
    // The aggregate sub is always single-row by design — use a
    // constant rowKey so the worker mirror collapses to one entry.
    this.sub = makeSqlSub(topic, sql, () => 'AGG');
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
```

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/lib/use-filtered-aggregate.ts
git commit -m "refactor(hooks): useFilteredAggregate driven by the SharedWorker port

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 13: GridPanel — drop topic-bound mode; migrate ex01 + ex06 to liveSubscription

**Files:**
- Modify: `clients/examples-web/src/components/panels/GridPanel.tsx`
- Modify: `clients/examples-web/src/examples/ex01-live-pnl/index.tsx`
- Modify: `clients/examples-web/src/examples/ex06-joins/index.tsx`

`cqStore` is about to disappear. The two grid call sites that still depend on it (`topic="positions"` in ex01 + `topic="positions"` and `topic="trades"` in ex06) need to switch to `useFilteredSubscription('/positions', null)` + `liveSubscription={…}`, the same path the other six chapters already use. Once those call sites move, `GridPanel` drops its `topic` + `tickTopic` props entirely.

- [ ] **Step 1: Migrate `ex01-live-pnl/index.tsx`**

Open `clients/examples-web/src/examples/ex01-live-pnl/index.tsx`. Above the existing `const sectorSub = useFilteredSubscription(...)` line, add:

```tsx
  const positionsSub = useFilteredSubscription('/positions', null);
```

Find the `<GridPanel topic="positions" …>` JSX and change the props:

```tsx
        <GridPanel
          liveSubscription={positionsSub}
          getRowId={positionsRowId}
          // …keep all other existing props unchanged…
        />
```

If a `positionsRowId` const already lives in the file (it does — verify via `git grep` first), leave it. If not, define it at the top of the component:

```tsx
const positionsRowId = (r: Record<string, unknown>): string =>
  String(r.position_id ?? r.positionKey ?? `${r.book_id ?? r.book}|${r.cusip}`);
```

(Outside the component body — it has no React state so it's a pure function.)

- [ ] **Step 2: Migrate `ex06-joins/index.tsx`**

Same pattern. Add two subs above the existing `useFilteredSubscription` line:

```tsx
  const positionsSub = useFilteredSubscription('/positions', null);
  const tradesSub = useFilteredSubscription('/trades', null);
```

Find the two `<GridPanel topic="positions" …>` and `<GridPanel topic="trades" …>` JSX blocks and replace `topic="positions"` with `liveSubscription={positionsSub}`, `topic="trades"` with `liveSubscription={tradesSub}`. The `getRowId` definitions stay as-is (or define them as `r => String(r.position_id)` / `r => String(r.trade_id)` if they aren't already present).

- [ ] **Step 3: Strip topic-bound mode from `GridPanel.tsx`**

Open `clients/examples-web/src/components/panels/GridPanel.tsx`. Make these changes:

1. **Replace the top import block** that pulls from `cq-store`:

```tsx
// REMOVE:
import { cqStore, useTickCount, type CqTopic, type DeltaBatch, type Row } from '@/lib/cq-store';
import type { FilteredSubscription } from '@/lib/use-filtered-subscription';

// REPLACE WITH:
import type { DeltaBatch, Row } from '@/lib/use-subscription';
import type { FilteredSubscription } from '@/lib/use-filtered-subscription';
```

2. **Remove the `topic` and `tickTopic` props from the `GridPanelProps` interface.** Keep `liveSubscription` and `getRowId` (now required, not optional). The component is liveSubscription-only.

3. **In the props destructure**, remove `topic` and `tickTopic`. Inside the body, every reference to `topic`, `tickTopic`, `cqStore[topic!]`, `useTickCount(topic ?? tickTopic)` etc. must be removed.

4. **Remove the inner `TickBadge` component entirely** (it's only used for `tickTopic`). The Phase 2 main-thread no longer drives the tick badge; the chrome moves to per-subscription deltas in Phase 3+.

5. **The `source` useMemo** simplifies from a three-way switch (`liveSubscription` / `topic` / undefined) to a single `liveSubscription` assignment:

```tsx
const source = liveSubscription;
```

6. The component should require `liveSubscription` (TypeScript flag at compile time if someone forgets it).

- [ ] **Step 4: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add clients/examples-web/src/components/panels/GridPanel.tsx \
        clients/examples-web/src/examples/ex01-live-pnl/index.tsx \
        clients/examples-web/src/examples/ex06-joins/index.tsx
git commit -m "refactor(grid): GridPanel is liveSubscription-only; migrate ex01/ex06

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 14: ContextBar — drop `cqStore` / `useTickCount`

**Files:**
- Modify: `clients/examples-web/src/components/atlas/ContextBar.tsx`

The legacy ContextBar reads `cqStore[topic].getSize()` and `useTickCount(topic)` for the chrome row-count badge and the LIVE pulse. cqStore is gone after Task 15; the spec says the tick badge moves to per-subscription state in Phase 3+ (chapter scope). For Phase 2 we render a static "—" placeholder so the layout is unchanged but the data dependency is severed.

- [ ] **Step 1: Patch ContextBar**

Open `clients/examples-web/src/components/atlas/ContextBar.tsx`. Make these surgical changes:

1. **Remove the import**:
```tsx
// REMOVE:
import { cqStore, useTickCount } from '@/lib/cq-store';
```

2. **Remove the `const ticks = useTickCount(topic);` line.** Anywhere that reads `ticks`, replace the value with `0` (the badge will read "0 / s" until Phase 3 wires the chapter scope's tick stream).

3. **Replace the size readout** that calls `cqStore[topic].getSize().toLocaleString()` with a literal em-dash:
```tsx
// REPLACE:
{cqStore[topic].getSize().toLocaleString()}
// WITH:
—
```

4. **Inside the rate-math `useEffect`** that reads `cqStore[topic].getTickCount()`: delete the whole effect. The "ticks/s" derived number falls back to a static 0 until the chapter scope hook lands.

The visual layout is unchanged; only the live values are stubbed.

- [ ] **Step 2: Typecheck**

Run: `cd clients/examples-web && npm run typecheck`
Expected: clean — any unused `topic` parameter that becomes orphaned should be left in place; React will silently accept it.

- [ ] **Step 3: Commit**

```bash
git add clients/examples-web/src/components/atlas/ContextBar.tsx
git commit -m "refactor(context-bar): drop cq-store/useTickCount; stub live values

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 15: Delete cq-store + remove its side-effect import in App.tsx

**Files:**
- Modify: `clients/examples-web/src/App.tsx`
- Delete: `clients/examples-web/src/lib/cq-store.ts`

This is the line. Before this commit, both the worker and the legacy store coexist; after this commit, the worker is the only data path.

- [ ] **Step 1: Remove the side-effect import in App.tsx**

Open `clients/examples-web/src/App.tsx`. Delete these four lines verbatim:

```tsx
// Side-effect import: opens the cqserver WebSocket and starts streaming
// positions / trades / securities / fi-market-data into the live store
// before any example needs them.
import '@/lib/cq-store';
```

- [ ] **Step 2: Delete the file**

```bash
git rm clients/examples-web/src/lib/cq-store.ts
```

- [ ] **Step 3: Typecheck + build**

Run: `cd clients/examples-web && npm run typecheck && npm run build 2>&1 | tail -8`
Expected: typecheck clean, build succeeds. The pre-existing AG-Grid chunk-size advisory is OK; any other warning or error is a regression.

If typecheck surfaces a stray import from `@/lib/cq-store` that earlier tasks missed, fix the file in the same commit. The most likely candidates are:
- `src/lib/use-filtered-subscription.ts` — checked in Task 10; should be clean.
- `src/lib/use-live-query.ts` — checked in Task 11; should be clean.
- `src/lib/use-filtered-aggregate.ts` — checked in Task 12; should be clean.
- `src/components/panels/GridPanel.tsx` — checked in Task 13; should be clean.
- `src/components/atlas/ContextBar.tsx` — checked in Task 14; should be clean.

Re-run typecheck after any fix.

- [ ] **Step 4: Commit**

```bash
git add clients/examples-web/src/App.tsx
git add clients/examples-web/src/lib/cq-store.ts   # picks up the deletion
git commit -m "feat(worker): delete cq-store — SharedWorker is the only data path

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 16: Manual smoke verification

**Files:** none.

- [ ] **Step 1: Launch the demo**

```bash
cd /Users/develop/cqserver
./stop-demo.sh 2>/dev/null
POSITIONS=40000 TRADES_PER_POSITION=8 TICK_PCT=0.01 TICK_MS=250 ./start-atlas-demo.sh
```

Wait ~10s for cqserver + publisher to settle (idle until rows seed).

- [ ] **Step 2: Verify the legacy app still works at every URL except `#atlas`**

Browse to `http://localhost:5175/`. Click through all eight tabs. Expected for each:

- The grid populates (the SOW chunk visibly streams in for the wider tabs).
- Live ticks update once the publisher's tick loop fires.
- No console errors. The "ticks/s" badge in the context bar reads "0 / s" until Phase 3 — confirm that's the only visible regression.
- Switching tabs and switching back does NOT cause a fresh SOW (the worker is sharing the upstream sub between tabs of the same browser window — verified by watching the network panel: only one `sow_and_subscribe` per `(topic, filter)` triple).

- [ ] **Step 3: Verify the Atlas preview still renders at `#atlas`**

Browse to `http://localhost:5175/#atlas`. Confirm the Phase 1 Pulse preview renders identically — the design system code is untouched in Phase 2.

- [ ] **Step 4: Verify cross-tab sharing**

Open `http://localhost:5175/` in two tabs. Open DevTools → Application → Shared Workers in either tab. Expected:

- One `cqserver-hub` SharedWorker exists.
- Closing one tab leaves the worker (and the WS connection) alive in the other.
- Closing both tabs evicts the worker (browser default behaviour — no action required).

If SharedWorker is unavailable (test by setting `Object.defineProperty(window, 'SharedWorker', { value: undefined });` in DevTools before reload), the dedicated-worker fallback should keep a single tab functional. Confirm by reloading: the example grids should still populate.

- [ ] **Step 5: Verify reconnect**

With the page open, kill the cqserver process (`./stop-demo.sh`). Expected:

- Within ~500ms, the context bar's connection chip should flip to a disconnected state.
- The grid keeps the last snapshot visible (no flicker, no clear-to-empty).

Now restart cqserver (`POSITIONS=40000 … ./start-atlas-demo.sh` again). Expected:

- The worker reconnects within a few seconds (capped backoff).
- Every chapter's grid receives a fresh SOW (visible chunk-by-chunk in the wider tabs).
- Live ticks resume.

If any of these fail, fix the relevant earlier task before proceeding to Phase 3.

---

## Self-Review (completed by author)

**Spec coverage** (Phase 2 row from `docs/superpowers/specs/2026-05-27-examples-web-redesign-design.md`):

- **SharedWorker data layer** (`cq-worker.ts`) — split into `protocol.ts` + `hub.ts` + two thin entry shells. Tasks 1, 2-6, 7. ✅
- **Message protocol** — Task 1 with the exact `ClientMsg` / `ServerMsg` shapes (extended with `hello-ack`, `error`, `pong` for diagnostics). ✅
- **Main-thread hook rewrite** — `useSubscription` (Task 9), `useLiveQuery` (Task 11), `useFilteredAggregate` (Task 12). ✅
- **Progressive snapshot** — Task 4, with `SOW_CHUNK_ROWS = 500` (matches the spec's "~500-row messages"). ✅
- **Coalesced deltas** — Task 5, 50ms window (matches `COALESCE_MS = 50`). ✅
- **Reference-counted subscriptions** — Task 3, with `subKey(topic, filter, sql)` and `refs: Set<{portId, portSubId}>`. ✅
- **Reconnect** — Task 6, capped exponential backoff (`RECONNECT_BASE_MS = 500`, `RECONNECT_MAX_MS = 8_000`, ≤2⁶ multiplier) — same shape as the legacy cq-store. ✅
- **`cq-store.ts` deleted** — Task 15. ✅
- **Existing chapters keep working** — Task 10 (alias) + Task 13 (ex01 + ex06 grid migration). The other six chapters already used `useFilteredSubscription` + `liveSubscription`, so they pass through unchanged. ✅
- **SharedWorker quirks → dedicated-Worker fallback** — Tasks 7 + 8 (feature detection in `getCqPort()`). ✅
- **No row mirror on main** — the new `Sub` class in `use-subscription.ts` does keep a small `Map<rowKey, Row>` for its own snapshot derivation, but this is per-chapter scope (small), not the universe mirror cq-store maintained. The spec's intent ("the main thread holds only what AG-Grid currently renders + in-flight deltas") is satisfied by the fact that no universe-wide mirror exists on main anymore — the worker holds it, and the hook holds at most the chapter's filtered slice. ✅
- **`useTickCount` dropped** — Task 14 stubs the badge. Phase 3 will reintroduce per-subscription ticks. ✅

**Placeholder scan:** no "TBD", "TODO", "fix later", "add error handling", "similar to Task N" tokens. Every code block is complete. The Task 3 stub `enqueueDelta(...)` is explicitly named as a stub and replaced in Task 5. The Task 14 ContextBar literal "—" replacement is the documented Phase 2 behaviour (not a placeholder for Phase 2's deliverable). ✅

**Type / name consistency:**
- `ClientMsg` / `ServerMsg` / `Row` / `ConnectionStatus` / `DeltaBatch` defined in Task 1 (`protocol.ts`) and consumed identically in every later task. ✅
- `SOW_CHUNK_ROWS` and `COALESCE_MS` are imported from `./protocol` consistently across Tasks 4-5. ✅
- The internal `Sub` class is the only sub implementation across `useSubscription`, `useLiveQuery`, and `useFilteredAggregate` (via `makeSqlSub`). No drift. ✅
- `SubscriptionHandle` (new) is type-aliased to `FilteredSubscription` in Task 10 so every legacy call site keeps compiling. ✅
- The hub's internal `SharedSub.rowKey` helper is intentionally distinct from the main-thread hook's `Sub.rowKey` — both are documented as best-effort and only used for their own bookkeeping. The chapter's true row-id derivation (passed via `getRowId` to `GridPanel`) governs what AG-Grid sees. ✅

**Scope:** focused on the data layer. No design changes. No new chapters. No server changes. The existing app at `/` keeps working at every commit boundary except the transient window between Task 13 (which removes topic-bound GridPanel mode) and Task 15 (which deletes cq-store) — and even there, Task 13 lands ex01 + ex06's replacement before stripping the legacy code, so behaviour is preserved at every commit. ✅

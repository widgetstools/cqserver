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
/**
 * Grace period before an idle (zero-subscriber) shared sub is actually
 * torn down. While lingering, the upstream cqserver sub stays open and
 * keeps the worker's row mirror up to date, so a tab that re-subscribes
 * to the same `(topic, filter, sql)` — switching chapters, or a backgrounded
 * tab coming forward — replays from the warm cache instead of firing a
 * fresh SOW at the server. Bounded so genuinely abandoned views (e.g.
 * one-off ad-hoc queries) eventually release their server sub.
 */
const SUB_LINGER_MS = 5 * 60_000;

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
  /** Bumped on every `openShared`. The async-iterator drain loop and the
   *  snapshot-complete callback carry the generation they were started
   *  with and bail the moment it changes, so a stale upstream sub left
   *  over from a reconnect can't push rows into the replaced state. */
  generation: number;
  /** Set after `whenSnapshotComplete()`. */
  isLive: boolean;
  /** Keys a row the way the main thread does — built from the subscribe
   *  message's `keyCols` (or the legacy heuristic when absent). The mirror
   *  and the conflation buffer both go through this so a re-emitted
   *  aggregate group collapses to one slot instead of leaking one per tick. */
  keyer: (row: Row) => string;
  /** Worker-side row mirror for SOW replay on late join + Task 6 resub. */
  rows: Map<string, Row>;
  /** Per-key conflation buffer for the coalesce window. Last write per key
   *  wins; an add cancelled by a remove in the same window drops entirely. */
  pending: Map<string, { kind: 'add' | 'update' | 'remove'; row: Row }>;
  coalesceTimer: ReturnType<typeof setTimeout> | null;
  /** Set when refs hit zero; tears the sub down after SUB_LINGER_MS unless
   *  a new subscriber arrives first and cancels it (warm-cache reuse). */
  lingerTimer: ReturnType<typeof setTimeout> | null;
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

  /** Build a shared sub's row keyer from the subscribe message's
   *  `keyCols`. See the protocol's `keyCols` doc for the three cases. */
  private buildKeyer(keyCols: string[] | undefined): (row: Row) => string {
    if (keyCols) {
      // `[]` → every row collapses to one slot (continuous aggregates).
      if (keyCols.length === 0) return () => '\x00agg';
      return (row: Row) =>
        keyCols.map((c) => (row[c] == null ? '' : String(row[c]))).join('\x00');
    }
    // No key named — keep the legacy id-column heuristic.
    return (row: Row) => this.rowKey(row);
  }

  private broadcast(msg: ServerMsg): void {
    // Collect dead ports first; removePort mutates this.ports.
    let dead: string[] | null = null;
    for (const p of this.ports.values()) {
      if (!this.tryPost(p, msg)) (dead ??= []).push(p.id);
    }
    if (dead) for (const id of dead) this.removePort(id);
  }

  private send(portId: string, msg: ServerMsg): void {
    const p = this.ports.get(portId);
    if (p && !this.tryPost(p, msg)) this.removePort(portId);
  }

  /** postMessage that survives a closed MessagePort. A SharedWorker
   *  never gets a reliable close event when a tab goes away, so the
   *  first failed post is how we learn a port is gone. */
  private tryPost(p: PortState, msg: ServerMsg): boolean {
    try {
      p.port.postMessage(msg);
      return true;
    } catch {
      return false;
    }
  }

  /** Drop a dead port and release every subscription ref it held, so
   *  `subs`, the per-sub `refs` sets, and upstream cqserver subs don't
   *  accumulate orphans across tab churn. */
  private removePort(portId: string): void {
    const state = this.ports.get(portId);
    if (!state) return;
    this.ports.delete(portId);
    // Snapshot ids — handleUnsubscribe mutates state.subs as it goes.
    for (const portSubId of Array.from(state.subs.keys())) {
      this.handleUnsubscribe(state, portSubId);
    }
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
      case 'sow':
        void this.handleSow(state, msg);
        return;
    }
  }

  /**
   * One-shot SQL fetch. Runs the server's one-shot SOW path (which
   * evaluates JOINs / derived tables that the live evaluator can't) and
   * replies with `snapshot` chunks tagged with the request's `reqId`,
   * then a terminating `more:false`. Never registers a live sub.
   */
  private async handleSow(
    state: PortState,
    msg: Extract<ClientMsg, { kind: 'sow' }>,
  ): Promise<void> {
    try {
      const client = await this.ensureClient();
      const rows = (await client.sow(msg.topic, { sql: msg.sql })) as Row[];
      // Reuse the chunked snapshot delivery so a large JOIN result stays
      // within cheap postMessage payloads. `reqId` doubles as the subId
      // the main-thread one-shot listener matches on.
      this.sendSowChunked({ portId: state.id, portSubId: msg.reqId }, rows);
    } catch (err) {
      this.send(state.id, {
        kind: 'error',
        subId: msg.reqId,
        message: err instanceof Error ? err.message : String(err),
      });
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
        generation: 0,
        isLive: false,
        keyer: this.buildKeyer(msg.keyCols),
        rows: new Map(),
        pending: new Map(),
        coalesceTimer: null,
        lingerTimer: null,
      };
      this.subs.set(key, shared);
    }
    // A subscriber arrived — cancel any pending idle teardown so the warm,
    // still-updating cache is reused instead of re-fetched.
    if (shared.lingerTimer != null) {
      clearTimeout(shared.lingerTimer);
      shared.lingerTimer = null;
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
    // Claim this open. Any iterator/callback from a prior open carries an
    // older generation and self-cancels below.
    const gen = ++shared.generation;
    const sub = await client.sowAndSubscribe(shared.topic, {
      filter: shared.filter,
      sql: shared.sql,
    });
    // A newer open (or a teardown) raced us during the await — drop this
    // sub so we don't overwrite the live state with a now-stale handle.
    if (shared.generation !== gen) {
      void client.unsubscribe(sub.subId).catch(() => {});
      return;
    }
    shared.sub = sub;
    const sowBuffer: Row[] = [];
    let sowDone = false;
    void sub.whenSnapshotComplete().then(() => {
      if (shared.generation !== gen) return;
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
          // Stale generation → a reconnect/teardown replaced this sub.
          // Stop touching `shared` and let the loop unwind.
          if (shared.generation !== gen) break;
          const row = d.data as Row;
          const key = shared.keyer(row);
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
        if (shared.generation !== gen) return;
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
    // No viewers (lingering): `shared.rows` was already updated by the drain
    // loop, so the cache stays warm — skip the per-port coalesce machinery.
    if (shared.refs.size === 0) return;
    // Conflate per key so N updates to the same row in one window collapse
    // to the latest value, matching AMPS conflation. The key is unstable
    // only on the JSON.stringify fallback, where distinct values key
    // distinctly and this degrades to the old append behaviour — never to
    // anything wrong.
    const key = shared.keyer(row);
    const prev = shared.pending.get(key);
    if (kind === 'remove' || kind === 'oof') {
      // add → remove within the window cancels: the main thread never saw
      // the row (its add hasn't been flushed yet), so emit neither.
      if (prev && prev.kind === 'add') shared.pending.delete(key);
      else shared.pending.set(key, { kind: 'remove', row });
    } else {
      // Preserve a still-pending 'add' so a newly-entered row stays an add
      // even after later updates in the same window.
      const nextKind = prev && prev.kind === 'add' ? 'add' : kind === 'add' ? 'add' : 'update';
      shared.pending.set(key, { kind: nextKind, row });
    }
    if (shared.coalesceTimer == null) {
      shared.coalesceTimer = setTimeout(() => this.flushPending(shared), COALESCE_MS);
    }
  }

  private flushPending(shared: SharedSub): void {
    shared.coalesceTimer = null;
    if (shared.pending.size === 0) return;
    const add: Row[] = [];
    const update: Row[] = [];
    const remove: Row[] = [];
    for (const { kind, row } of shared.pending.values()) {
      if (kind === 'add') add.push(row);
      else if (kind === 'remove') remove.push(row);
      else update.push(row);
    }
    shared.pending = new Map();
    if (add.length === 0 && update.length === 0 && remove.length === 0) return;
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
    if (shared.refs.size === 0 && shared.lingerTimer == null) {
      // Don't tear down immediately — keep the upstream sub and its row
      // mirror warm for SUB_LINGER_MS so a quick re-subscribe (chapter
      // switch / tab refocus) replays from cache instead of re-SOWing.
      // Stop the coalesce flush (no viewers to send to); the drain loop
      // still updates `shared.rows` so the cache stays current.
      if (shared.coalesceTimer != null) {
        clearTimeout(shared.coalesceTimer);
        shared.coalesceTimer = null;
      }
      shared.pending = new Map();
      shared.lingerTimer = setTimeout(() => this.teardownShared(key), SUB_LINGER_MS);
    }
  }

  /** Actually release an idle shared sub once its linger window expires.
   *  No-ops if a subscriber rejoined in the meantime (refs > 0). */
  private teardownShared(key: string): void {
    const shared = this.subs.get(key);
    if (!shared) return;
    if (shared.lingerTimer != null) {
      clearTimeout(shared.lingerTimer);
      shared.lingerTimer = null;
    }
    if (shared.refs.size > 0) return; // re-subscribed during linger — keep it.
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
        // Re-open every shared sub that still has refs. Guard with
        // `openingPromise` exactly like `handleSubscribe` so a port that
        // subscribes to the same key during reconnect awaits this open
        // instead of racing it into a duplicate upstream sub.
        for (const shared of this.subs.values()) {
          if (shared.refs.size === 0) continue;
          for (const ref of shared.refs) {
            this.send(ref.portId, { kind: 'status', subId: ref.portSubId, status: 'snapshotting' });
          }
          if (shared.openingPromise != null) continue;
          shared.openingPromise = this.openShared(shared, c)
            .catch((err) => {
              for (const ref of shared.refs) {
                this.send(ref.portId, {
                  kind: 'error',
                  subId: ref.portSubId,
                  message: err instanceof Error ? err.message : String(err),
                });
              }
            })
            .finally(() => {
              shared.openingPromise = null;
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
    for (const [key, shared] of this.subs) {
      if (shared.lingerTimer != null) {
        clearTimeout(shared.lingerTimer);
        shared.lingerTimer = null;
      }
      // An idle sub that was only lingering has no viewers and the socket
      // just dropped — don't keep it around to re-open on reconnect.
      if (shared.refs.size === 0) {
        if (shared.coalesceTimer != null) {
          clearTimeout(shared.coalesceTimer);
          shared.coalesceTimer = null;
        }
        this.subs.delete(key);
        continue;
      }
      shared.sub = null;
      shared.isLive = false;
      shared.rows.clear();
      shared.pending = new Map();
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

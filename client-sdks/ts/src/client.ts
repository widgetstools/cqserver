import { ClientError, CqMessage, Delta, DeltaKind } from './types.js';
import { Transport, connectWs } from './transport.js';

/**
 * P15 — Fisher-Yates shuffle of `[0, n)` for `connectAny`'s URL
 * ordering. Uses Math.random — not cryptographically random, but
 * more than sufficient to spread N clients across M followers.
 */
function shuffledIndices(n: number): number[] {
  const out = Array.from({ length: n }, (_, i) => i);
  for (let i = n - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

interface Pending {
  resolve: (msg: CqMessage) => void;
  reject: (err: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export interface ClientOptions {
  ackTimeoutMs?: number;
  /**
   * If > 0, the client sends a `heartbeat` frame this often to keep
   * the server-side idle timer alive. cqserver disconnects peers that
   * stay silent past `idle_timeout` (default 65s), so subscriber-only
   * clients MUST heartbeat or they get kicked roughly every minute.
   * Default 25_000 (25s) — well inside the server window.
   */
  heartbeatIntervalMs?: number;
}

export class Subscription {
  private buffer: Delta[] = [];
  private waiters: Array<(d: Delta | null) => void> = [];
  private done = false;
  private snapshotComplete = false;
  private snapshotWaiters: Array<() => void> = [];
  public lastSequence = 0;

  constructor(public readonly subId: string) {}

  /** Push a delta into the subscription. Called by the driver. */
  push(d: Delta) {
    if (d.sequence !== undefined && d.sequence > this.lastSequence) {
      this.lastSequence = d.sequence;
    }
    const waiter = this.waiters.shift();
    if (waiter) waiter(d);
    else this.buffer.push(d);
  }

  /** Mark the subscription ended (server closed / unsubscribed). */
  end() {
    this.done = true;
    for (const w of this.waiters) w(null);
    this.waiters = [];
    // If we end before the snapshot was acknowledged complete, wake any
    // waiters anyway so they don't hang forever.
    if (!this.snapshotComplete) this.markSnapshotComplete();
  }

  /**
   * Called by the driver when `group_end` arrives — separates the SOW
   * phase from the live-delta phase for consumers that want to defer
   * UI work until the initial state has landed.
   */
  markSnapshotComplete() {
    if (this.snapshotComplete) return;
    this.snapshotComplete = true;
    for (const w of this.snapshotWaiters) w();
    this.snapshotWaiters = [];
  }

  /** True once the server has signalled `group_end` for this sub. */
  get isSnapshotComplete(): boolean {
    return this.snapshotComplete;
  }

  /** Resolves when the SOW phase ends. Resolves immediately afterward. */
  whenSnapshotComplete(): Promise<void> {
    if (this.snapshotComplete) return Promise.resolve();
    return new Promise<void>((resolve) => this.snapshotWaiters.push(resolve));
  }

  /** Promise that resolves with the next delta, or null when ended. */
  nextDelta(): Promise<Delta | null> {
    const queued = this.buffer.shift();
    if (queued) return Promise.resolve(queued);
    if (this.done) return Promise.resolve(null);
    return new Promise<Delta | null>((resolve) => this.waiters.push(resolve));
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Delta> {
    while (true) {
      const d = await this.nextDelta();
      if (d === null) return;
      yield d;
    }
  }
}

export class Client {
  private cid = 0;
  private pending = new Map<string, Pending>();
  private subs = new Map<string, Subscription>();
  private snapshotBuffers = new Map<string, Record<string, unknown>[]>();
  private snapshotCompletions = new Map<string, (rows: Record<string, unknown>[]) => void>();
  private closed = false;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private closeListeners = new Set<() => void>();

  constructor(
    private transport: Transport,
    private opts: ClientOptions = {},
  ) {
    transport.onFrame((m) => this.dispatch(m));
    transport.onClose(() => this.handleTransportClose());
    const hb = opts.heartbeatIntervalMs ?? 25_000;
    if (hb > 0) {
      this.heartbeatTimer = setInterval(() => {
        if (this.closed) return;
        // Fire and forget — heartbeat() is RPC-style so a bad response
        // would surface as a rejected promise. We swallow because the
        // subsequent onClose will surface the real disconnect anyway.
        this.heartbeat().catch(() => {});
      }, hb);
    }
  }

  /**
   * Register a callback for connection termination. Lets the store
   * layer schedule a reconnect when the WS drops or the server idle-
   * disconnects. The callback fires at most once per Client instance.
   */
  onClose(cb: () => void): void {
    if (this.closed) {
      cb();
      return;
    }
    this.closeListeners.add(cb);
  }

  private handleTransportClose(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    for (const p of this.pending.values()) {
      p.reject(new ClientError('connection closed'));
      if (p.timer) clearTimeout(p.timer);
    }
    this.pending.clear();
    for (const s of this.subs.values()) s.end();
    this.subs.clear();
    for (const cb of this.closeListeners) cb();
    this.closeListeners.clear();
  }

  static async connect(url: string, opts: ClientOptions = {}): Promise<Client> {
    if (url.startsWith('tcp://')) {
      const rest = url.slice('tcp://'.length);
      const [host, portStr] = rest.split(':');
      const port = parseInt(portStr, 10);
      if (!host || !Number.isFinite(port)) throw new ClientError(`bad tcp url: ${url}`);
      // Dynamic import keeps the TCP/Node code out of browser bundles.
      // Bundlers respect the `node:`-prefixed nested import and won't
      // try to resolve it for browser targets.
      const { connectTcp } = await import('./transport-node.js');
      const t = await connectTcp(host, port);
      const c = new Client(t, opts);
      c._activeUrl = url;
      return c;
    }
    if (url.startsWith('ws://') || url.startsWith('wss://')) {
      const t = await connectWs(url);
      const c = new Client(t, opts);
      c._activeUrl = url;
      return c;
    }
    throw new ClientError(`unsupported url scheme: ${url}`);
  }

  /**
   * P15 — HA failover. Attempt each URL in a randomised order; the
   * first successful connection wins. If they all fail, throws the
   * last error.
   *
   * Mirrors the Rust SDK's `connect_any`. Randomisation matters when
   * many clients start simultaneously: without it every client would
   * hammer the first URL and only fail over when it dies, defeating
   * the load-spread purpose.
   *
   * This is *initial-connect* failover only. Live reconnect after a
   * mid-stream disconnect is a separate concern (deferred follow-up).
   *
   * After the call returns, `client.activeUrl` exposes which URL the
   * client actually connected to.
   */
  static async connectAny(urls: string[], opts: ClientOptions = {}): Promise<Client> {
    if (urls.length === 0) {
      throw new ClientError('connectAny: empty url list');
    }
    const order = shuffledIndices(urls.length);
    let lastErr: unknown;
    for (const i of order) {
      try {
        return await Client.connect(urls[i], opts);
      } catch (e) {
        lastErr = e;
      }
    }
    throw lastErr instanceof Error
      ? lastErr
      : new ClientError(`connectAny: all ${urls.length} URLs failed`);
  }

  /**
   * P15 — the URL this client actually connected to (set by
   * `connect` / `connectAny`). Useful for tests and observability.
   */
  get activeUrl(): string | undefined {
    return this._activeUrl;
  }
  private _activeUrl: string | undefined;

  async close(): Promise<void> {
    if (this.closed) {
      await this.transport.close();
      return;
    }
    this.handleTransportClose();
    await this.transport.close();
  }

  private nextCid(): string {
    this.cid += 1;
    return `c-${this.cid}`;
  }

  private rpc(msg: CqMessage): Promise<CqMessage> {
    const cid = this.nextCid();
    msg.cid = cid;
    return new Promise<CqMessage>((resolve, reject) => {
      const ackTimeoutMs = this.opts.ackTimeoutMs ?? 30_000;
      const timer = setTimeout(() => {
        this.pending.delete(cid);
        reject(new ClientError(`ack timeout (cid=${cid})`));
      }, ackTimeoutMs);
      this.pending.set(cid, { resolve, reject, timer });
      this.transport.send(msg).catch((err) => {
        clearTimeout(timer);
        this.pending.delete(cid);
        reject(err);
      });
    }).then((resp) => {
      if (resp.s === 'error') throw new ClientError(resp.r ?? 'server error');
      return resp;
    });
  }

  // ----- Public API -----

  async logon(user: string, password: string): Promise<void> {
    await this.rpc({ c: 'logon', d: { user, password } });
  }

  async publish(topic: string, data: Record<string, unknown>): Promise<number> {
    const r = await this.rpc({ c: 'publish', t: topic, d: data });
    return r.seq ?? 0;
  }

  /**
   * Q2 — atomic batched publish. Sends every message in `msgs` to
   * `topic` in a single wire frame; the server commits them under
   * one upsert burst and returns a single Ack carrying the per-row
   * sequences in input order.
   *
   * Saves N-1 round-trips compared to N sequential `publish()`
   * calls. AMPS-equivalent shape: one frame, one ack, one txlog
   * append. (P16's earlier implementation used SDK-level
   * `Promise.all`; Q2 replaces it with the wire-level batch frame
   * that landed in `Command::PublishBatch`.)
   *
   * Throws on the first per-row failure; the surviving prefix is
   * already durable on the txlog (matches the AMPS contract — a
   * batch is not atomic across failures, per-row durability is
   * preserved).
   */
  async publishBatch(
    topic: string,
    msgs: Record<string, unknown>[],
  ): Promise<number[]> {
    if (msgs.length === 0) return [];
    const r = await this.rpc({ c: 'publish_batch', t: topic, bat: msgs });
    return r.seqs ?? [];
  }

  /**
   * Fire-and-forget publish. Sends the frame and resolves once the
   * transport has accepted it for delivery — does NOT wait for the
   * server ack and never tracks a pending RPC. Use this for
   * high-throughput publisher loops where individual ack delivery
   * isn't load-bearing on correctness; the ack frames still flow
   * but the SDK discards them by virtue of the missing `cid`.
   *
   * Internally we attach a `cid` so the server's logging stays
   * happy, but we DON'T register a pending entry. The server's ack
   * will arrive with that `cid` and be dropped silently.
   */
  async publishUnacked(topic: string, data: Record<string, unknown>): Promise<void> {
    const cid = this.nextCid();
    await this.transport.send({ c: 'publish', cid, t: topic, d: data });
  }

  /**
   * One-shot SOW. Server replies with `sow_batch`/`sow_complete` frames
   * carrying the rows produced by evaluating the query against the
   * topic's current SOW.
   *
   * - `filter` — server-side WHERE predicate. Composed into a
   *   `SELECT * FROM t WHERE …` query, so only row-shape projection.
   * - `sql` — verbatim SELECT. The server rewrites the FROM clause to
   *   reach the resolved topic, so the FROM table can be any
   *   identifier (e.g. `positions`). Use this for GROUP BY / aggregate
   *   / projection queries.
   */
  async sow(
    topic: string,
    opts: { filter?: string; sql?: string } = {},
  ): Promise<Record<string, unknown>[]> {
    const cid = this.nextCid();
    this.snapshotBuffers.set(cid, []);
    return new Promise<Record<string, unknown>[]>((resolve, reject) => {
      const ackTimeoutMs = this.opts.ackTimeoutMs ?? 30_000;
      const timer = setTimeout(() => {
        this.snapshotBuffers.delete(cid);
        this.snapshotCompletions.delete(cid);
        reject(new ClientError(`sow timeout (cid=${cid})`));
      }, ackTimeoutMs);
      this.snapshotCompletions.set(cid, (rows) => {
        clearTimeout(timer);
        resolve(rows);
      });
      const msg: CqMessage = { c: 'sow', cid, t: topic };
      if (opts.filter) msg.f = opts.filter;
      if (opts.sql) msg.sql = opts.sql;
      this.transport.send(msg).catch((err) => {
        clearTimeout(timer);
        this.snapshotBuffers.delete(cid);
        this.snapshotCompletions.delete(cid);
        reject(err);
      });
    });
  }

  async subscribe(topic: string, opts: { filter?: string } = {}): Promise<Subscription> {
    return this.subscribeInternal('subscribe', topic, opts.filter, undefined);
  }

  async sowAndSubscribe(
    topic: string,
    opts: { filter?: string; bookmark?: number } = {},
  ): Promise<Subscription> {
    return this.subscribeInternal('sow_and_subscribe', topic, opts.filter, opts.bookmark);
  }

  async deltaSubscribe(topic: string, opts: { filter?: string } = {}): Promise<Subscription> {
    return this.subscribeInternal('delta_subscribe', topic, opts.filter, undefined);
  }

  private async subscribeInternal(
    command: string,
    topic: string,
    filter: string | undefined,
    bookmark: number | undefined,
  ): Promise<Subscription> {
    const msg: CqMessage = { c: command, t: topic };
    if (filter !== undefined) msg.f = filter;
    if (bookmark !== undefined) msg.bm = bookmark;
    const ack = await this.rpc(msg);
    const subId = ack.sid;
    if (!subId) throw new ClientError('server did not return a sub_id');
    const sub = new Subscription(subId);
    if (bookmark !== undefined) sub.lastSequence = bookmark;
    this.subs.set(subId, sub);
    return sub;
  }

  async unsubscribe(subId: string): Promise<void> {
    await this.rpc({ c: 'unsubscribe', sid: subId });
    const sub = this.subs.get(subId);
    if (sub) {
      sub.end();
      this.subs.delete(subId);
    }
  }

  async sowDelete(topic: string, key: string): Promise<number> {
    const r = await this.rpc({ c: 'sow_delete', t: topic, d: { key } });
    return r.seq ?? 0;
  }

  async heartbeat(): Promise<void> {
    await this.rpc({ c: 'heartbeat' });
  }

  // ----- Driver -----

  private dispatch(msg: CqMessage) {
    if (this.closed) return;
    const cmd = msg.c;
    const cid = msg.cid;
    const sid = msg.sid;

    if (cmd === 'ack' && cid) {
      const p = this.pending.get(cid);
      if (p) {
        if (p.timer) clearTimeout(p.timer);
        this.pending.delete(cid);
        p.resolve(msg);
      }
      return;
    }
    if (cmd === 'sow' && sid) {
      // SOW frames arrive in two situations:
      //   (a) one-shot `sow()` — completion is keyed by cid (registered
      //       below when sow() was called) and we accumulate into the
      //       buffer until group_end fires the completion.
      //   (b) `sow_and_subscribe()` / `subscribe()` — there's an active
      //       Subscription keyed by sid that should receive each row as
      //       a Delta(add) on its async iterator, so consumers don't
      //       need to wait for group_end to start rendering.
      if (typeof msg.d === 'object' && msg.d !== null) {
        const buf = this.snapshotBuffers.get(sid);
        if (buf) buf.push(msg.d as Record<string, unknown>);
        const sub = this.subs.get(sid);
        if (sub) {
          sub.push({
            deltaType: 'add',
            subId: sid,
            data: msg.d as Record<string, unknown>,
          });
        }
      }
      return;
    }
    if (cmd === 'sow_batch' && sid) {
      // Chunked SOW frame — `d` is an array of row objects.
      if (Array.isArray(msg.d)) {
        const buf = this.snapshotBuffers.get(sid);
        const sub = this.subs.get(sid);
        for (const row of msg.d as Record<string, unknown>[]) {
          if (!row || typeof row !== 'object') continue;
          if (buf) buf.push(row);
          if (sub) sub.push({ deltaType: 'add', subId: sid, data: row });
        }
      }
      return;
    }
    if (cmd === 'group_end' && sid) {
      const done = this.snapshotCompletions.get(sid);
      if (done) {
        const rows = this.snapshotBuffers.get(sid) ?? [];
        this.snapshotBuffers.delete(sid);
        this.snapshotCompletions.delete(sid);
        done(rows);
      }
      const sub = this.subs.get(sid);
      if (sub) sub.markSnapshotComplete();
      return;
    }
    if (cmd === 'group_begin') return;
    if (cmd === 'publish' && sid) {
      const sub = this.subs.get(sid);
      if (sub) {
        const d: Delta = {
          deltaType: (msg.dt as DeltaKind) ?? 'update',
          subId: sid,
          sequence: msg.seq,
          data: (msg.d as Record<string, unknown>) ?? {},
        };
        sub.push(d);
      }
      return;
    }
    // Heartbeat from server, unknown commands — ignore.
  }
}

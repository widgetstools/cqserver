import { ClientError, CqMessage, Delta, DeltaKind } from './types.js';
import { Transport, connectTcp, connectWs } from './transport.js';

interface Pending {
  resolve: (msg: CqMessage) => void;
  reject: (err: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export interface ClientOptions {
  ackTimeoutMs?: number;
}

export class Subscription {
  private buffer: Delta[] = [];
  private waiters: Array<(d: Delta | null) => void> = [];
  private done = false;
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

  constructor(
    private transport: Transport,
    private opts: ClientOptions = {},
  ) {
    transport.onFrame((m) => this.dispatch(m));
  }

  static async connect(url: string, opts: ClientOptions = {}): Promise<Client> {
    if (url.startsWith('tcp://')) {
      const rest = url.slice('tcp://'.length);
      const [host, portStr] = rest.split(':');
      const port = parseInt(portStr, 10);
      if (!host || !Number.isFinite(port)) throw new ClientError(`bad tcp url: ${url}`);
      const t = await connectTcp(host, port);
      return new Client(t, opts);
    }
    if (url.startsWith('ws://') || url.startsWith('wss://')) {
      const t = await connectWs(url);
      return new Client(t, opts);
    }
    throw new ClientError(`unsupported url scheme: ${url}`);
  }

  async close(): Promise<void> {
    this.closed = true;
    for (const p of this.pending.values()) {
      p.reject(new ClientError('connection closed'));
      if (p.timer) clearTimeout(p.timer);
    }
    this.pending.clear();
    for (const s of this.subs.values()) s.end();
    this.subs.clear();
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

  async sow(topic: string, opts: { filter?: string } = {}): Promise<Record<string, unknown>[]> {
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
      const buf = this.snapshotBuffers.get(sid);
      if (buf && typeof msg.d === 'object' && msg.d !== null) {
        buf.push(msg.d as Record<string, unknown>);
      }
      return;
    }
    if (cmd === 'sow_batch' && sid) {
      // Chunked SOW frame — `d` is an array of row objects.
      const buf = this.snapshotBuffers.get(sid);
      if (buf && Array.isArray(msg.d)) {
        for (const row of msg.d as Record<string, unknown>[]) {
          if (row && typeof row === 'object') buf.push(row);
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

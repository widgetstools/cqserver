// CQServer client over TCP (length-prefixed JSON frames).
//
// Wire framing: each frame is `[u32 big-endian length][JSON body]`. The top
// bit of the length (0x80000000) flags a zstd-compressed body. This SDK
// advertises no compression, so the server never sets it; an inbound frame
// with the flag set is therefore a protocol error.

import net from 'node:net';

const COMPRESSED_FLAG = 0x80000000;
const MAX_FRAME = 16 * 1024 * 1024;

// Wire protocol versions this SDK understands (kept conservative).
export const SUPPORTED_VERSIONS = [1];

export class CqError extends Error {
  constructor(message) {
    super(message);
    this.name = 'CqError';
  }
}

/**
 * A single subscription update (snapshot row or live change).
 * @typedef {Object} Delta
 * @property {string} deltaType  "sow" (snapshot), "add", "update", "remove", ...
 * @property {string} subId
 * @property {Object} data
 * @property {number|null} sequence
 */

/** A pending RPC waiter resolved by an Ack with a matching cid. */
class Pending {
  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
    this.settled = false;
  }

  fulfil(value) {
    if (this.settled) return;
    this.settled = true;
    this.resolve(value);
  }

  fail(err) {
    if (this.settled) return;
    this.settled = true;
    this.reject(err);
  }
}

/** Delivers snapshot rows then live deltas via an async queue. */
export class Subscription {
  constructor(subId, cid) {
    this.subId = subId;
    this.cid = cid;
    /** @type {Delta[]} */
    this._queue = [];
    /** @type {Array<(d: Delta|null) => void>} */
    this._waiters = [];
    this._closed = false;
  }

  /** @param {Delta} delta */
  _push(delta) {
    const waiter = this._waiters.shift();
    if (waiter) {
      waiter(delta);
    } else {
      this._queue.push(delta);
    }
  }

  _close() {
    this._closed = true;
    // Wake any pending iterator waiters so they don't hang forever.
    while (this._waiters.length) {
      this._waiters.shift()(null);
    }
  }

  /**
   * Resolve to the next delta. Resolves to null on timeout.
   * @param {number} [timeoutMs]
   * @returns {Promise<Delta|null>}
   */
  nextDelta(timeoutMs) {
    if (this._queue.length) {
      return Promise.resolve(this._queue.shift());
    }
    if (this._closed) {
      return Promise.resolve(null);
    }
    return new Promise((resolve) => {
      let timer = null;
      const deliver = (d) => {
        if (timer) clearTimeout(timer);
        resolve(d);
      };
      this._waiters.push(deliver);
      if (timeoutMs != null) {
        timer = setTimeout(() => {
          const idx = this._waiters.indexOf(deliver);
          if (idx !== -1) this._waiters.splice(idx, 1);
          resolve(null);
        }, timeoutMs);
      }
    });
  }

  /** Async iterator: `for await (const d of sub) { ... }`. */
  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const d = await this.nextDelta();
        if (d == null) return { value: undefined, done: true };
        return { value: d, done: false };
      },
    };
  }
}

export class Client {
  /** @param {net.Socket} sock */
  constructor(sock) {
    this._sock = sock;
    this._cidCounter = 0;
    this._protocolVersion = 1;
    this._closed = false;

    // cid -> Pending (RPC waiters: publish, logon, etc.)
    this._pending = new Map();
    // cid -> Subscription, before the server assigns a sid (promoted on ack)
    this._pendingSubs = new Map();
    // sid -> Subscription (live routing)
    this._subs = new Map();
    // cid -> array of snapshot rows (one-shot sow)
    this._snapshotBuffers = new Map();
    // cid/sid -> Pending; resolves null on success, rejects with error text
    this._snapshotDone = new Map();

    // Incoming-frame parser state: accumulate partial TCP reads.
    this._inbound = Buffer.alloc(0);

    sock.on('data', (chunk) => this._onData(chunk));
    sock.on('error', () => this._failAllWaiters(new CqError('connection error')));
    sock.on('close', () => this._failAllWaiters(new CqError('connection closed')));
  }

  // ---- connection -------------------------------------------------

  /**
   * @param {string} host
   * @param {number} port
   * @param {number} [timeoutMs]
   * @returns {Promise<Client>}
   */
  static connect(host, port, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host, port });
      sock.setNoDelay(true);
      const onError = (err) => {
        sock.removeListener('connect', onConnect);
        reject(new CqError(`connect failed: ${err.message}`));
      };
      const onConnect = () => {
        sock.removeListener('error', onError);
        sock.setTimeout(0);
        resolve(new Client(sock));
      };
      sock.setTimeout(timeoutMs, () => {
        sock.destroy();
        reject(new CqError('connect timed out'));
      });
      sock.once('error', onError);
      sock.once('connect', onConnect);
    });
  }

  /** Accepts `tcp://host:port`. */
  static connectUrl(url, timeoutMs = 5000) {
    if (!url.startsWith('tcp://')) {
      throw new CqError(`unsupported url (tcp:// only): ${url}`);
    }
    const hostport = url.slice('tcp://'.length);
    const idx = hostport.lastIndexOf(':');
    const host = hostport.slice(0, idx);
    const port = parseInt(hostport.slice(idx + 1), 10);
    return Client.connect(host, port, timeoutMs);
  }

  close() {
    this._closed = true;
    try {
      this._sock.destroy();
    } catch {
      // ignore
    }
  }

  // ---- framing ----------------------------------------------------

  /** @param {Object} msg */
  _send(msg) {
    const body = Buffer.from(JSON.stringify(msg), 'utf-8');
    if (body.length > MAX_FRAME) {
      throw new CqError(`frame too large: ${body.length} bytes`);
    }
    const header = Buffer.allocUnsafe(4);
    header.writeUInt32BE(body.length, 0);
    this._sock.write(Buffer.concat([header, body]));
  }

  /** @param {Buffer} chunk */
  _onData(chunk) {
    this._inbound = this._inbound.length ? Buffer.concat([this._inbound, chunk]) : chunk;
    try {
      // Peek the 4-byte length, then wait for the full frame before parsing.
      while (this._inbound.length >= 4) {
        const raw = this._inbound.readUInt32BE(0);
        if (raw & COMPRESSED_FLAG) {
          throw new CqError('received compressed frame but no codec negotiated');
        }
        const length = raw & ~COMPRESSED_FLAG;
        if (length > MAX_FRAME) {
          throw new CqError(`frame too large: ${length}`);
        }
        if (this._inbound.length < 4 + length) {
          break; // partial frame; wait for more bytes
        }
        const payload = this._inbound.subarray(4, 4 + length);
        const msg = JSON.parse(payload.toString('utf-8'));
        this._inbound = this._inbound.subarray(4 + length);
        this._dispatch(msg);
      }
    } catch (err) {
      this._failAllWaiters(err instanceof CqError ? err : new CqError(String(err)));
      this.close();
    }
  }

  _failAllWaiters(err) {
    for (const p of this._pending.values()) p.fail(err);
    this._pending.clear();
    for (const p of this._snapshotDone.values()) p.fail(err);
    this._snapshotDone.clear();
    for (const sub of this._pendingSubs.values()) sub._close();
    this._pendingSubs.clear();
    for (const sub of this._subs.values()) sub._close();
    this._subs.clear();
  }

  // ---- dispatch ---------------------------------------------------

  // A single reader processes frames in arrival order. On an Ack we promote
  // pendingSubs[cid] -> subs[sid] BEFORE handling later frames, so snapshot
  // rows in the same batch route correctly.
  _dispatch(msg) {
    switch (msg.c) {
      case 'ack':
        this._onAck(msg);
        break;
      case 'sow':
        this._onSowRow(msg);
        break;
      case 'sow_batch':
        this._onSowBatch(msg);
        break;
      case 'group_end':
        this._onGroupEnd(msg);
        break;
      case 'publish':
        this._onDelta(msg);
        break;
      // group_begin / heartbeat / schema_change: nothing to do here.
      default:
        break;
    }
  }

  _onAck(msg) {
    const cid = msg.cid;
    const sid = msg.sid;
    // Promote a pre-registered subscription into the live routing map.
    if (cid != null && sid != null) {
      const sub = this._pendingSubs.get(cid);
      if (sub) {
        this._pendingSubs.delete(cid);
        sub.subId = sid;
        this._subs.set(sid, sub);
      }
    }
    if (cid != null) {
      // Error ack for an in-flight SOW: no group_end will arrive.
      if (msg.s === 'error') {
        const p = this._snapshotDone.get(cid);
        if (p) {
          this._snapshotDone.delete(cid);
          p.fail(new CqError(msg.r || 'server error'));
        }
      }
      const p = this._pending.get(cid);
      if (p) {
        this._pending.delete(cid);
        p.fulfil(msg);
      }
    }
  }

  _onSowRow(msg) {
    const sid = msg.sid;
    const data = msg.d;
    if (sid == null || typeof data !== 'object' || data === null || Array.isArray(data)) {
      return;
    }
    const buf = this._snapshotBuffers.get(sid);
    if (buf) buf.push(data);
    const sub = this._subs.get(sid);
    if (sub) sub._push({ deltaType: 'sow', subId: sid, data, sequence: msg.seq ?? null });
  }

  _onSowBatch(msg) {
    const sid = msg.sid;
    const rows = msg.d;
    if (sid == null || !Array.isArray(rows)) return;
    const buf = this._snapshotBuffers.get(sid);
    const sub = this._subs.get(sid);
    for (const row of rows) {
      if (typeof row !== 'object' || row === null || Array.isArray(row)) continue;
      if (buf) buf.push(row);
      if (sub) sub._push({ deltaType: 'sow', subId: sid, data: row, sequence: null });
    }
  }

  _onGroupEnd(msg) {
    const sid = msg.sid;
    if (sid == null) return;
    const p = this._snapshotDone.get(sid);
    if (p) {
      this._snapshotDone.delete(sid);
      p.fulfil(null); // success
    }
  }

  _onDelta(msg) {
    const sid = msg.sid;
    if (sid == null) return;
    const sub = this._subs.get(sid);
    if (!sub) return;
    const data = msg.d;
    sub._push({
      deltaType: msg.dt || 'update',
      subId: sid,
      data: typeof data === 'object' && data !== null && !Array.isArray(data) ? data : {},
      sequence: msg.seq ?? null,
    });
  }

  // ---- helpers ----------------------------------------------------

  _nextCid() {
    this._cidCounter += 1;
    return String(this._cidCounter);
  }

  /**
   * @param {Object} msg
   * @param {number} [timeoutMs]
   * @returns {Promise<Object>}
   */
  async _rpc(msg, timeoutMs = 5000) {
    const cid = msg.cid || this._nextCid();
    msg.cid = cid;
    const p = new Pending();
    this._pending.set(cid, p);

    let timer = null;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        this._pending.delete(cid);
        reject(new CqError('timed out waiting for ack'));
      }, timeoutMs);
    });

    try {
      this._send(msg);
    } catch (err) {
      clearTimeout(timer);
      this._pending.delete(cid);
      throw err;
    }

    const resp = await Promise.race([p.promise, timeout]);
    clearTimeout(timer);
    if (resp.s === 'error') {
      throw new CqError(resp.r || 'server error');
    }
    return resp;
  }

  // ---- API --------------------------------------------------------

  get protocolVersion() {
    return this._protocolVersion;
  }

  /**
   * Authenticate and negotiate the protocol version.
   *
   * With no credentials this is an anonymous handshake (works when the server
   * has auth disabled). Returns the negotiated protocol version.
   *
   * @param {Object} [opts]
   * @param {string} [opts.user]
   * @param {string} [opts.password]
   * @param {string} [opts.token]
   * @param {string} [opts.clientName]
   * @returns {Promise<number>}
   */
  async logon({ user, password, token, clientName } = {}) {
    const msg = { c: 'logon', pv: SUPPORTED_VERSIONS };
    const creds = {};
    if (token != null) {
      creds.token = token;
    } else if (user != null) {
      creds.user = user;
      creds.password = password || '';
    }
    if (Object.keys(creds).length) msg.d = creds;
    if (clientName != null) msg.client = clientName;
    const resp = await this._rpc(msg);
    const pv = resp.pv;
    if (Array.isArray(pv) && pv.length) {
      this._protocolVersion = pv[0];
    }
    return this._protocolVersion;
  }

  /**
   * Publish/upsert a record. Resolves to the assigned sequence.
   * @returns {Promise<number>}
   */
  async publish(topic, data, timeoutMs = 5000) {
    const resp = await this._rpc({ c: 'publish', t: topic, d: data }, timeoutMs);
    return Number(resp.seq || 0);
  }

  /**
   * Sparse update: merge `data` (key + changed fields) into the row.
   * @returns {Promise<number>}
   */
  async deltaPublish(topic, data, timeoutMs = 5000) {
    const resp = await this._rpc({ c: 'delta_publish', t: topic, d: data }, timeoutMs);
    return Number(resp.seq || 0);
  }

  /**
   * One-shot SOW query. Resolves to the snapshot rows.
   * @returns {Promise<Object[]>}
   */
  async sow(topic, filter = null, timeoutMs = 10000) {
    const msg = { c: 'sow', t: topic };
    if (filter != null) msg.f = filter;
    return this._runSow(msg, timeoutMs);
  }

  /**
   * Run an arbitrary SOW SQL query (GROUP BY / aggregates).
   * @returns {Promise<Object[]>}
   */
  async sowSql(topic, sql, timeoutMs = 10000) {
    return this._runSow({ c: 'sow', t: topic, sql }, timeoutMs);
  }

  async _runSow(msg, timeoutMs) {
    const cid = this._nextCid();
    msg.cid = cid;
    const done = new Pending();
    this._snapshotBuffers.set(cid, []);
    this._snapshotDone.set(cid, done);

    let timer = null;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => {
        this._snapshotBuffers.delete(cid);
        this._snapshotDone.delete(cid);
        reject(new CqError('timed out waiting for SOW group_end'));
      }, timeoutMs);
    });

    try {
      this._send(msg);
    } catch (err) {
      clearTimeout(timer);
      this._snapshotBuffers.delete(cid);
      this._snapshotDone.delete(cid);
      throw err;
    }

    try {
      await Promise.race([done.promise, timeout]);
    } finally {
      clearTimeout(timer);
    }
    const rows = this._snapshotBuffers.get(cid) || [];
    this._snapshotBuffers.delete(cid);
    return rows;
  }

  /**
   * Snapshot + continuous deltas. Snapshot rows arrive as deltas with
   * deltaType == "sow"; live changes as "add"/"update"/"remove".
   * @returns {Subscription}
   */
  sowAndSubscribe(topic, filter = null, bookmark = null) {
    const cid = this._nextCid();
    const sub = new Subscription('', cid);
    this._pendingSubs.set(cid, sub);
    const msg = { c: 'sow_and_subscribe', cid, t: topic };
    if (filter != null) msg.f = filter;
    if (bookmark != null) msg.bm = bookmark;
    this._send(msg);
    return sub;
  }

  /**
   * Live deltas only (no initial snapshot).
   * @returns {Subscription}
   */
  subscribe(topic, filter = null) {
    const cid = this._nextCid();
    const sub = new Subscription('', cid);
    this._pendingSubs.set(cid, sub);
    const msg = { c: 'subscribe', cid, t: topic };
    if (filter != null) msg.f = filter;
    this._send(msg);
    return sub;
  }

  /** @param {Subscription} sub */
  unsubscribe(sub) {
    if (sub.subId) {
      this._send({ c: 'unsubscribe', sid: sub.subId });
      this._subs.delete(sub.subId);
      sub._close();
    }
  }
}

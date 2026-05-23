/**
 * Thin cqserver WebSocket client tailored to streaming a single topic
 * into AG Grid via `applyTransactionAsync`.
 *
 * Protocol shape we care about:
 *   client → server:
 *     { c: 'sow_and_subscribe', cid, t: '/positions' }
 *   server → client:
 *     { c: 'ack',  cid, sid, s: 'ok' }
 *     { c: 'group_begin', sid, n }     // sow snapshot start (n rows incoming)
 *     { c: 'sow', sid, d: {...row} }   // each snapshot row
 *     { c: 'group_end', sid }          // snapshot complete
 *     { c: 'publish', sid, dt, d: {...row} }  // live delta
 *     { c: 'heartbeat' }
 */

export type Row = Record<string, unknown>;

export interface SubscriberCallbacks {
  /** Called once when the SOW phase begins (only in `sow_and_subscribe` mode). */
  onSnapshotStart?: (expectedRows: number) => void;
  /**
   * Called once with the full snapshot rows when group_end arrives.
   * In `deltasOnly` mode the snapshot phase is skipped entirely and
   * this callback is never invoked — make it optional in that case.
   */
  onSnapshot?: (rows: Row[]) => void;
  /** Called for each live publish after the snapshot. */
  onUpdate: (row: Row) => void;
  /** Connection lifecycle. */
  onStatusChange?: (status: ConnectionStatus) => void;
}

export interface SubscribeOptions {
  /**
   * Subscribe to live deltas only — no SOW. Use this when the consumer
   * doesn't need historical state (e.g., a sliding-window aggregation).
   * Avoids paying the snapshot cost for topics with millions of rows.
   */
  deltasOnly?: boolean;
}

export type ConnectionStatus =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'snapshotting'
  | 'live'
  | 'disconnected';

interface SubState {
  topic: string;
  cid: string;
  sid?: string;
  cb: SubscriberCallbacks;
  buffer: Row[];
  inSnapshot: boolean;
  deltasOnly: boolean;
}

export class CqClient {
  private ws: WebSocket | null = null;
  private subs = new Map<string, SubState>(); // keyed by cid, then by sid once ack arrives
  private bySid = new Map<string, SubState>();
  private url: string;
  private reconnectTimer: number | null = null;
  private nextCid = 1;
  private statusListeners = new Set<(s: ConnectionStatus) => void>();
  private status: ConnectionStatus = 'idle';

  constructor(url: string) {
    this.url = url;
  }

  onStatus(cb: (s: ConnectionStatus) => void): () => void {
    this.statusListeners.add(cb);
    cb(this.status);
    return () => {
      this.statusListeners.delete(cb);
    };
  }

  private setStatus(s: ConnectionStatus) {
    this.status = s;
    for (const cb of this.statusListeners) cb(s);
  }

  connect() {
    if (this.ws && this.ws.readyState <= WebSocket.OPEN) return;
    this.setStatus('connecting');
    const ws = new WebSocket(this.url);
    this.ws = ws;
    ws.onopen = () => {
      this.setStatus('connected');
      // (Re)issue any existing subscriptions.
      for (const sub of this.subs.values()) {
        this.sendSubscribe(sub);
      }
    };
    ws.onclose = () => {
      this.setStatus('disconnected');
      this.ws = null;
      this.bySid.clear();
      this.reconnectTimer = window.setTimeout(() => this.connect(), 2000);
    };
    ws.onerror = () => {
      // The close handler runs after this and triggers reconnect.
    };
    ws.onmessage = (evt) => {
      let m: Record<string, unknown>;
      try {
        m = JSON.parse(evt.data as string);
      } catch {
        return;
      }
      this.dispatch(m);
    };
  }

  close() {
    if (this.reconnectTimer != null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.subs.clear();
    this.bySid.clear();
  }

  subscribe(
    topic: string,
    cb: SubscriberCallbacks,
    opts: SubscribeOptions = {},
  ): () => void {
    const cid = `s${this.nextCid++}`;
    const state: SubState = {
      topic,
      cid,
      cb,
      buffer: [],
      inSnapshot: false,
      deltasOnly: !!opts.deltasOnly,
    };
    this.subs.set(cid, state);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.sendSubscribe(state);
    } else {
      this.connect();
    }
    return () => {
      this.subs.delete(cid);
      if (state.sid) this.bySid.delete(state.sid);
      if (this.ws && this.ws.readyState === WebSocket.OPEN && state.sid) {
        this.ws.send(JSON.stringify({ c: 'unsubscribe', sid: state.sid }));
      }
    };
  }

  private sendSubscribe(sub: SubState) {
    if (!this.ws) return;
    // `subscribe` returns live deltas only (no snapshot); `sow_and_subscribe`
    // also delivers the initial state via group_begin/sow.../group_end.
    const cmd = sub.deltasOnly ? 'subscribe' : 'sow_and_subscribe';
    this.ws.send(JSON.stringify({ c: cmd, cid: sub.cid, t: sub.topic }));
  }

  private dispatch(m: Record<string, unknown>) {
    const c = m.c as string | undefined;
    if (c === 'ack') {
      const cid = m.cid as string | undefined;
      const sid = m.sid as string | undefined;
      if (!cid || !sid) return;
      const sub = this.subs.get(cid);
      if (!sub) return;
      sub.sid = sid;
      this.bySid.set(sid, sub);
      return;
    }
    const sid = m.sid as string | undefined;
    if (!sid) return;
    const sub = this.bySid.get(sid);
    if (!sub) return;

    if (c === 'group_begin') {
      sub.inSnapshot = true;
      sub.buffer = [];
      const n = Number(m.n ?? 0) || 0;
      sub.cb.onSnapshotStart?.(n);
      this.setStatus('snapshotting');
      return;
    }
    if (c === 'sow') {
      const d = m.d as Row | undefined;
      if (d) sub.buffer.push(d);
      return;
    }
    if (c === 'sow_batch') {
      // Chunked SOW frame — `d` is an array of row objects.
      const arr = m.d as Row[] | undefined;
      if (Array.isArray(arr)) {
        for (const row of arr) sub.buffer.push(row);
      }
      return;
    }
    if (c === 'group_end') {
      if (sub.inSnapshot) {
        sub.inSnapshot = false;
        sub.cb.onSnapshot?.(sub.buffer);
        sub.buffer = [];
        this.setStatus('live');
      }
      return;
    }
    if (c === 'publish') {
      const d = m.d as Row | undefined;
      if (d) sub.cb.onUpdate(d);
      return;
    }
  }
}

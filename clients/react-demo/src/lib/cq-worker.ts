/**
 * cqserver SharedWorker.
 *
 * Why: every browser tab that opens the React demo would otherwise
 * spin up its own WebSocket and its own snapshot of every topic.
 * Two tabs = double the server subscriptions, double the JSON parse
 * work on the main thread, double the firehose.
 *
 * This worker centralizes that:
 *   - One WebSocket per browser origin, regardless of tab count.
 *   - One server subscription per (topic, deltasOnly) pair, regardless
 *     of how many tabs are watching.
 *   - JSON.parse runs here, off the main thread.
 *   - On second-tab join the worker fans out a cached snapshot from
 *     its in-memory mirror — no extra server roundtrip.
 *
 * Wire protocol with each tab (over the SharedWorker MessagePort):
 *
 *   tab → worker:
 *     { type: 'connect',     url }                       // one-shot, idempotent
 *     { type: 'subscribe',   subId, topic, deltasOnly? } // per react subscription
 *     { type: 'unsubscribe', subId }
 *
 *   worker → tab:
 *     { type: 'status', status }                         // 'connecting' | 'connected' | 'snapshotting' | 'live' | 'disconnected'
 *     { type: 'snapshot', subId, rows }                  // single fire-and-forget array
 *     { type: 'update',  subId, row }                    // live delta
 */

/// <reference lib="webworker" />
// Workers run in their own global; this `declare` keeps TS happy
// since the project's main tsconfig deliberately uses DOM types.
declare const self: SharedWorkerGlobalScope;

type Row = Record<string, unknown>;

type Status =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'snapshotting'
  | 'live'
  | 'disconnected';

interface TabSub {
  port: MessagePort;
  deltasOnly: boolean;
}

interface TopicState {
  /** Topic name, e.g. "/trades". */
  topic: string;
  /** Client-side cid we sent on `sow_and_subscribe`. */
  cid: string;
  /** Server-assigned sid (resolved on `ack`). */
  sid?: string;
  /** Key field used to dedupe rows in the mirror.
   *  Required for the cached-snapshot fanout to work; we discover it
   *  from the TOPIC_KEY_FIELD table below, falling back to scanning
   *  the first row for a plausible *Id / *Key field. */
  keyField: string;
  /** keyValue → latest row. */
  mirror: Map<string, Row>;
  /** True after the first group_end from the server. */
  snapshotComplete: boolean;
  /** subId → { port, deltasOnly }. */
  subs: Map<string, TabSub>;
  /** When the last sub unsubscribed, we linger for a short window
   *  before tearing down the server subscription — this absorbs the
   *  rapid unsubscribe/resubscribe pattern that React's effect
   *  cleanup-and-remount produces (HMR, dev double-mount, panel
   *  closes that trigger sibling re-renders). A non-null timer means
   *  "topic is closing; cancel me if a new sub joins". */
  closeTimer: ReturnType<typeof setTimeout> | null;
}

/** Grace period before a topic with zero subs is torn down. Bigger
 *  than typical React effect cleanup → re-mount intervals (~ a few
 *  ms in HMR, double-digit ms for dock-manager re-layout). Short
 *  enough that real "last subscriber went away" cases still get
 *  cleaned up quickly server-side. */
const TOPIC_LINGER_MS = 1500;

// Demo-specific key field hints. The React app subscribes to a known
// set of topics; hardcoding these avoids round-tripping `/admin/topics`
// on every cold boot.  Falls back to row-shape inspection if missing.
const TOPIC_KEY_FIELD: Record<string, string> = {
  '/trades': 'tradeId',
  '/positions': 'positionKey',
  '/securities': 'cusip',
  '/fi-market-data': 'cusip',
  '/market-data': 'symbol',
  '/orders': 'orderId',
  '/risk': 'positionKey',
};

let ws: WebSocket | null = null;
let wsUrl: string | null = null;
let status: Status = 'idle';
let nextCid = 1;

const topics = new Map<string, TopicState>();
// Each entry tracks the last `ping` we received from that port.
// Ports that miss enough pings are pruned and treated as dead tabs.
const ports = new Map<MessagePort, { lastPingAt: number }>();
const PORT_IDLE_DROP_MS = 25_000; // ~2.5 missed pings at 10 s cadence
let presenceCheckTimer: ReturnType<typeof setInterval> | null = null;
/** Fast reverse lookup: subId → topic name. */
const subToTopic = new Map<string, string>();

// ── Per-subscription delta batching ─────────────────────────────
//
// At /trades publish rates (hundreds–thousands/sec under load), a
// per-row postMessage is wasteful: every cross-thread message pays
// the structuredClone tax and wakes the main thread. We accumulate
// updates per subId for one animation frame (~16 ms) and flush them
// as a single `{ type: 'updates', subId, rows }` payload. AG Grid's
// applyTransactionAsync layer handles the rest.
//
// Snapshots still go out immediately — they're already one big array
// and the consuming grids need them to seed before live deltas can
// be applied.

const BATCH_FLUSH_MS = 16;

interface PendingBatch {
  port: MessagePort;
  subId: string;
  rows: Row[];
}

const pendingBatches = new Map<string, PendingBatch>(); // subId → batch
let flushTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleFlush() {
  if (flushTimer != null) return;
  flushTimer = setTimeout(flushBatches, BATCH_FLUSH_MS);
}

function flushBatches() {
  flushTimer = null;
  if (pendingBatches.size === 0) return;
  for (const batch of pendingBatches.values()) {
    if (batch.rows.length === 0) continue;
    try {
      batch.port.postMessage({
        type: 'updates',
        subId: batch.subId,
        rows: batch.rows,
      });
    } catch {
      /* port gone */
    }
  }
  pendingBatches.clear();
}

function enqueueUpdate(port: MessagePort, subId: string, row: Row) {
  let batch = pendingBatches.get(subId);
  if (!batch) {
    batch = { port, subId, rows: [] };
    pendingBatches.set(subId, batch);
  }
  batch.rows.push(row);
  scheduleFlush();
}

function setStatus(s: Status) {
  status = s;
  broadcast({ type: 'status', status: s });
}

function broadcast(msg: unknown) {
  for (const p of ports.keys()) {
    try {
      p.postMessage(msg);
    } catch {
      // Port closed — clean up on the next interaction.
      ports.delete(p);
    }
  }
}

function reapDeadPorts() {
  const now = Date.now();
  for (const [p, info] of ports) {
    if (now - info.lastPingAt > PORT_IDLE_DROP_MS) {
      ports.delete(p);
    }
  }
  // If nobody's home, tear down the WebSocket. The worker itself may
  // stay loaded for a bit longer (the browser GC's it), but the
  // server-side subscriptions stop immediately.
  if (ports.size === 0 && ws && ws.readyState <= WebSocket.OPEN) {
    try {
      ws.close();
    } catch {
      /* ignore */
    }
    ws = null;
    topics.clear();
  }
}

function ensurePresenceCheck() {
  if (presenceCheckTimer != null) return;
  presenceCheckTimer = setInterval(reapDeadPorts, 5_000);
}

function getOrCreateTopic(topic: string): TopicState {
  let t = topics.get(topic);
  if (!t) {
    t = {
      topic,
      cid: `c${nextCid++}`,
      keyField: TOPIC_KEY_FIELD[topic] ?? '__inferred__',
      mirror: new Map(),
      snapshotComplete: false,
      subs: new Map(),
      closeTimer: null,
    };
    topics.set(topic, t);
  } else if (t.closeTimer != null) {
    // The topic was about to be torn down — cancel: the new sub
    // arrived inside the linger window, so we keep the server
    // subscription and the cached mirror exactly as they are.
    clearTimeout(t.closeTimer);
    t.closeTimer = null;
  }
  return t;
}

function inferKeyField(row: Row): string | null {
  // Look for the first key ending in 'Id' or 'Key' that has a
  // primitive value. Common cases in the demo: tradeId, positionKey,
  // orderId. Returns null if nothing plausible.
  for (const k of Object.keys(row)) {
    if (/(Id|Key)$/.test(k)) {
      const v = row[k];
      if (
        typeof v === 'string' ||
        typeof v === 'number' ||
        typeof v === 'boolean'
      ) {
        return k;
      }
    }
  }
  return null;
}

function rowKey(t: TopicState, row: Row): string | null {
  if (t.keyField === '__inferred__') {
    const inferred = inferKeyField(row);
    if (!inferred) return null;
    t.keyField = inferred;
  }
  const v = row[t.keyField];
  if (v === undefined || v === null) return null;
  return String(v);
}

function ensureWs(url: string) {
  if (ws && ws.readyState <= WebSocket.OPEN) return;
  wsUrl = url;
  setStatus('connecting');
  const sock = new WebSocket(url);
  ws = sock;
  sock.onopen = () => {
    setStatus('connected');
    // (Re)issue every existing server subscription.
    for (const t of topics.values()) {
      sock.send(JSON.stringify({ c: 'sow_and_subscribe', cid: t.cid, t: t.topic }));
    }
  };
  sock.onclose = () => {
    setStatus('disconnected');
    ws = null;
    // Reconnect 2s later if we still have tabs.
    if (ports.size > 0 && wsUrl) {
      setTimeout(() => ensureWs(wsUrl!), 2000);
    }
  };
  sock.onerror = () => {
    /* close runs after — handles the retry */
  };
  sock.onmessage = (ev) => {
    let m: Record<string, unknown>;
    try {
      m = JSON.parse(ev.data as string);
    } catch {
      return;
    }
    dispatch(m);
  };
}

function dispatch(m: Record<string, unknown>) {
  const c = m.c as string | undefined;

  if (c === 'ack') {
    const cid = m.cid as string | undefined;
    const sid = m.sid as string | undefined;
    if (!cid || !sid) return;
    for (const t of topics.values()) {
      if (t.cid === cid) {
        t.sid = sid;
        break;
      }
    }
    return;
  }

  const sid = m.sid as string | undefined;
  if (!sid) return;
  let target: TopicState | undefined;
  for (const t of topics.values()) {
    if (t.sid === sid) {
      target = t;
      break;
    }
  }
  if (!target) return;

  if (c === 'group_begin') {
    target.mirror.clear();
    target.snapshotComplete = false;
    setStatus('snapshotting');
    return;
  }

  if (c === 'sow' || c === 'sow_batch') {
    const rows: Row[] =
      c === 'sow_batch'
        ? ((m.d as Row[] | undefined) ?? [])
        : m.d
          ? [m.d as Row]
          : [];
    for (const row of rows) {
      const k = rowKey(target, row);
      if (k != null) target.mirror.set(k, row);
    }
    return;
  }

  if (c === 'group_end') {
    target.snapshotComplete = true;
    setStatus('live');
    // Fan out a single snapshot array to every subscriber that asked
    // for it (i.e. !deltasOnly). New tabs that arrive later get the
    // same snapshot from the mirror without another server roundtrip.
    const rows = Array.from(target.mirror.values());
    for (const [subId, sub] of target.subs) {
      if (!sub.deltasOnly) {
        try {
          sub.port.postMessage({ type: 'snapshot', subId, rows });
        } catch {
          /* port gone */
        }
      }
    }
    return;
  }

  if (c === 'publish') {
    const row = m.d as Row | undefined;
    if (!row) return;
    const k = rowKey(target, row);
    if (k != null) target.mirror.set(k, row);
    // Fan out via the batcher — one postMessage per subscription per
    // frame instead of one per WS publish. Cuts cross-thread overhead
    // dramatically on high-rate topics like /trades.
    for (const [subId, sub] of target.subs) {
      enqueueUpdate(sub.port, subId, row);
    }
    return;
  }
}

function handleTabSubscribe(
  port: MessagePort,
  subId: string,
  topic: string,
  deltasOnly: boolean,
) {
  const t = getOrCreateTopic(topic);
  const isFirstSub = t.subs.size === 0;
  t.subs.set(subId, { port, deltasOnly });
  subToTopic.set(subId, topic);

  // If the server subscription doesn't exist yet (cold first sub),
  // request it now. Otherwise reuse the existing mirror.
  if (isFirstSub) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(
        JSON.stringify({ c: 'sow_and_subscribe', cid: t.cid, t: t.topic }),
      );
    }
    // On (re)connect onopen will replay every topic, so no else-branch.
  } else if (t.snapshotComplete && !deltasOnly) {
    // Second-tab join: deliver the cached snapshot right away — no
    // server roundtrip needed.
    try {
      port.postMessage({
        type: 'snapshot',
        subId,
        rows: Array.from(t.mirror.values()),
      });
    } catch {
      /* port gone */
    }
  }
}

function handleTabUnsubscribe(subId: string) {
  const topic = subToTopic.get(subId);
  if (!topic) return;
  subToTopic.delete(subId);
  pendingBatches.delete(subId);
  const t = topics.get(topic);
  if (!t) return;
  t.subs.delete(subId);
  if (t.subs.size > 0) return;

  // No more subscribers — start the linger timer. Don't unsubscribe
  // from the server yet: React effect cleanup followed immediately
  // by remount (HMR, dock-manager re-layout) would otherwise produce
  // an unsubscribe / re-subscribe pair to the server with the same
  // topic, and the in-flight ack from the second subscribe would race
  // any old publishes from the first sub. Lingering keeps the server
  // sub alive across the gap.
  if (t.closeTimer != null) clearTimeout(t.closeTimer);
  t.closeTimer = setTimeout(() => {
    // Re-check inside the timer — a new sub may have arrived right
    // before we fire, which getOrCreateTopic would have caught and
    // cancelled. If it didn't, no one's listening — go ahead and
    // tear down the server sub.
    if (t.subs.size > 0) return;
    if (ws && ws.readyState === WebSocket.OPEN && t.sid) {
      ws.send(JSON.stringify({ c: 'unsubscribe', sid: t.sid }));
    }
    topics.delete(topic);
  }, TOPIC_LINGER_MS);
}

function handleTabMessage(port: MessagePort, ev: MessageEvent) {
  const m = ev.data as Record<string, unknown>;
  if (!m || typeof m !== 'object') return;
  const type = m.type as string;
  // Refresh presence on every inbound message — any kind of tab
  // activity counts as "this port is alive". The dedicated `ping`
  // type is just the fallback heartbeat for tabs that aren't
  // currently subscribing or otherwise talking to the worker.
  const portInfo = ports.get(port);
  if (portInfo) portInfo.lastPingAt = Date.now();
  if (type === 'ping') return;
  if (type === 'connect') {
    const url = m.url as string;
    if (url) ensureWs(url);
    // Send current status so the new tab can hydrate its UI.
    try {
      port.postMessage({ type: 'status', status });
    } catch {
      /* port gone */
    }
    return;
  }
  if (type === 'subscribe') {
    handleTabSubscribe(
      port,
      m.subId as string,
      m.topic as string,
      !!m.deltasOnly,
    );
    return;
  }
  if (type === 'unsubscribe') {
    handleTabUnsubscribe(m.subId as string);
    return;
  }
}

// SharedWorker entry: one `connect` event per tab. Each connection
// brings its own MessagePort.
self.onconnect = (ev: MessageEvent) => {
  const port = ev.ports[0];
  if (!port) return;
  ports.set(port, { lastPingAt: Date.now() });
  port.onmessage = (msg) => handleTabMessage(port, msg);
  port.onmessageerror = () => ports.delete(port);
  port.start();
  ensurePresenceCheck();
};

export {};

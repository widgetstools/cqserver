/**
 * Typed wrappers around the cqserver admin HTTP API.
 *
 * Endpoints (see crates/cq-server/src/admin.rs):
 *   GET    /healthz
 *   GET    /stats
 *   GET    /topics
 *   GET    /subscriptions
 *   DELETE /subscriptions/:sub_id
 *   GET    /metrics                (Prometheus text format)
 *   POST   /admin/rotate-journal/:topic
 *   POST   /admin/shrink-store/:topic
 *   POST   /admin/shrink-store-all
 *   GET    /admin/replication
 *   GET    /admin/shard-for/:topic
 */

const DEFAULT_BASE = (import.meta.env.VITE_ADMIN_URL as string | undefined) ?? '/admin-api';

export const adminBase = DEFAULT_BASE.replace(/\/+$/, '');

async function get<T>(path: string): Promise<T> {
  const r = await fetch(`${adminBase}${path}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} on ${path}`);
  return (await r.json()) as T;
}

async function getText(path: string): Promise<string> {
  const r = await fetch(`${adminBase}${path}`);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} on ${path}`);
  return await r.text();
}

async function post(path: string): Promise<unknown> {
  const r = await fetch(`${adminBase}${path}`, { method: 'POST' });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} on ${path}`);
  return r.headers.get('content-type')?.includes('application/json')
    ? await r.json()
    : await r.text();
}

async function del(path: string): Promise<unknown> {
  const r = await fetch(`${adminBase}${path}`, { method: 'DELETE' });
  if (!r.ok) throw new Error(`${r.status} ${r.statusText} on ${path}`);
  return r.headers.get('content-type')?.includes('application/json')
    ? await r.json()
    : await r.text();
}

// ─── Response shapes ──────────────────────────────────────────────────

export interface ServerStats {
  topics: number;
  totalRows: number;
  totalSubscriptions: number;
  activeRoutes: number;
  processRssBytes: number;
  processVirtualBytes: number;
}

export interface TopicInfo {
  name: string;
  rowCount: number;
  columnCount: number;
  keyFields: string[];
  subscriptions: number;
  globalVersion: number;
  capacity: number;
  schemaDiscovered: boolean;
}

export interface SubscriptionInfo {
  subId: string;
  topic: string;
  sessionId: string;
  queueDepth: number;
  queueCapacity: number;
  fillRatio: number;
  dropped: number;
  ageMs: number;
  conflated: boolean;
  conflationIntervalMs: number;
}

export interface QueueInfo {
  name: string;
  kind: 'queue';
  buffered: number;
  consumers: number;
  sequence: number;
}

export interface ReplicationStatus {
  role: 'standalone' | 'primary' | 'standby';
  peer?: string | null;
  listen?: string | null;
  topics?: Array<{
    topic: string;
    current_sequence?: number;
  }>;
}

export interface ViewInfo {
  name: string;
  source: string;
  sql: string;
  initial_capacity: number;
  tap_capacity: number;
}

export interface ShardForResponse {
  topic: string;
  instance_url: string;
  matched_prefix: string | null;
  self: boolean;
}

// ─── API ──────────────────────────────────────────────────────────────

export const adminApi = {
  healthz: () => getText('/healthz'),
  stats: () => get<ServerStats>('/stats'),
  topics: () => get<TopicInfo[]>('/topics'),
  subscriptions: () => get<SubscriptionInfo[]>('/subscriptions'),
  queues: () => get<QueueInfo[]>('/queues'),
  views: () => get<ViewInfo[]>('/admin/views'),
  configToml: () => getText('/admin/config'),
  metricsText: () => getText('/metrics'),
  replication: () => get<ReplicationStatus>('/admin/replication'),
  shardFor: (topic: string) =>
    get<ShardForResponse>(`/admin/shard-for/${encodeURIComponent(topic)}`),
  rotateJournal: (topic: string) =>
    post(`/admin/rotate-journal/${encodeURIComponent(topic)}`),
  shrinkStore: (topic: string) =>
    post(`/admin/shrink-store/${encodeURIComponent(topic)}`),
  shrinkStoreAll: () => post('/admin/shrink-store-all'),
  dropSubscription: (subId: string) =>
    del(`/subscriptions/${encodeURIComponent(subId)}`),
};

// ─── Prometheus parser (just enough for the dashboard) ────────────────

export interface ParsedMetric {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export function parsePrometheus(text: string): ParsedMetric[] {
  const out: ParsedMetric[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    // metric{label="v",...} value
    // OR metric value
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN|Inf|-Inf)$/);
    if (!m) continue;
    const name = m[1];
    const labels: Record<string, string> = {};
    if (m[2]) {
      // Strip { }
      const inside = m[2].slice(1, -1);
      // Cheap label parser — adequate for cqserver-emitted lines (no
      // escaped quotes inside label values in our output).
      for (const pair of inside.split(',')) {
        const eq = pair.indexOf('=');
        if (eq < 0) continue;
        const k = pair.slice(0, eq).trim();
        const v = pair.slice(eq + 1).trim().replace(/^"|"$/g, '');
        if (k) labels[k] = v;
      }
    }
    const v = Number(m[3]);
    out.push({ name, labels, value: v });
  }
  return out;
}

/** Sum all series for a metric name (collapses labels). */
export function metricSum(metrics: ParsedMetric[], name: string): number {
  let s = 0;
  for (const m of metrics) if (m.name === name) s += m.value;
  return Number.isFinite(s) ? s : 0;
}

/** First value matching a metric+label predicate. */
export function metricValue(
  metrics: ParsedMetric[],
  name: string,
  labels?: Record<string, string>,
): number | undefined {
  for (const m of metrics) {
    if (m.name !== name) continue;
    if (labels) {
      let ok = true;
      for (const [k, v] of Object.entries(labels)) {
        if (m.labels[k] !== v) { ok = false; break; }
      }
      if (!ok) continue;
    }
    return m.value;
  }
  return undefined;
}

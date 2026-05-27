/**
 * Schema catalog client. Fetches cqserver's `GET /admin/catalog` —
 * every topic + materialized view with its column list and types —
 * for the Query Builder's authoring panel.
 *
 * In dev, Vite proxies `/cq-admin` → the admin HTTP port (see
 * vite.config.ts), so this fetch is same-origin (no CORS). A
 * standalone deployment can set `window.CQ_ADMIN_URL` to a reachable
 * admin origin (which must then return CORS headers).
 */

/** One column of a topic/view schema. `type` is a lowercase scalar
 *  type string: "double" | "long" | "int" | "string" | "bool" |
 *  "timestamp" | "bytes". */
export interface CatalogColumn {
  name: string;
  type: string;
}

/** A topic or materialized view, with its columns. `kind` lets the UI
 *  group views separately from raw topics. */
export interface CatalogEntry {
  name: string;
  kind: 'topic' | 'view';
  columns: CatalogColumn[];
}

function adminBase(): string {
  if (typeof window === 'undefined') return '/cq-admin';
  return (window as unknown as { CQ_ADMIN_URL?: string }).CQ_ADMIN_URL ?? '/cq-admin';
}

/** Fetch the full catalog. Throws on a non-2xx response or network
 *  failure; callers surface that as an error state. */
export async function fetchCatalog(signal?: AbortSignal): Promise<CatalogEntry[]> {
  const res = await fetch(`${adminBase()}/admin/catalog`, { signal });
  if (!res.ok) {
    throw new Error(`catalog fetch failed: HTTP ${res.status}`);
  }
  return (await res.json()) as CatalogEntry[];
}

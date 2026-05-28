/**
 * Query (Chapter 08 — Ad-Hoc SQL) scope.
 *
 * The chapter renders a pre-built catalog on the left, an editable SQL
 * editor on the top right, and a result grid below. cqserver's
 * sub-time JOIN evaluator is only on the one-shot SOW path, so the
 * runner forks by mode:
 *   - 'live'   — single-topic SELECT / WHERE / GROUP BY. Live
 *                sowAndSubscribe via useLiveQuery; the grid ticks.
 *   - 'static' — multi-topic JOIN queries. One-shot runOneShotSql
 *                against the left topic; grid renders frozen.
 *
 * Mode is auto-detected per query by inspecting the SQL for JOIN
 * keywords (matches the legacy ex08 heuristic).
 */
import type { QueryFeature } from '@/lib/queries/library';

export { QUERIES, type QueryEntry, type QueryFeature } from '@/lib/queries/library';

export const FEATURE_LABEL: Record<QueryFeature, string> = {
  join: 'Joins',
  filter: 'Filters',
  agg: 'Aggregations',
  pivot: 'Pivots',
  view: 'Views',
  window: 'Window Functions',
};

export const FEATURE_ORDER: QueryFeature[] = [
  'join', 'filter', 'agg', 'pivot', 'view', 'window',
];

/** Heuristic — JOIN keyword anywhere outside a string literal forces
 *  static mode. Lifted from legacy ex08-query-builder; the pattern is
 *  `\bJOIN\b` case-insensitive applied after a crude comment-strip. */
export function detectRunMode(sql: string): 'live' | 'static' {
  const stripped = sql
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
  return /\bJOIN\b/i.test(stripped) ? 'static' : 'live';
}

/** Pick the first topic referenced after `FROM`. The query runner uses
 *  this as the subscription topic for live mode and the SOW target for
 *  static mode. */
export function detectFromTopic(sql: string): string {
  const m = sql.match(/\bFROM\s+\/?([a-zA-Z_][\w.]*)/i);
  return m ? `/${m[1].replace(/^\//, '')}` : '/positions';
}

export function fmtMs(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  if (n < 1) return `<1 ms`;
  if (n < 1000) return `${Math.round(n)} ms`;
  return `${(n / 1000).toFixed(2)} s`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

/**
 * Strip `<alias>.` prefixes from column references so cqserver's
 * parser (which doesn't carry an alias-resolution table) sees the
 * bare column names from the combined JOIN schema. Ported from legacy
 * ex08; half the catalog queries use `p`/`t` aliases and would
 * otherwise trip "Unknown column" on both the live and static paths.
 */
export function stripAliases(sql: string): string {
  const aliasRe = /(?:from|join)\s+(\w+)\s+(\w+)\b/gi;
  const aliases: string[] = [];
  let m: RegExpExecArray | null;
  while ((m = aliasRe.exec(sql)) !== null) {
    const alias = m[2]!;
    const lower = alias.toLowerCase();
    // SQL clause-starting keywords that occasionally land in m[2] —
    // not real aliases.
    if ([
      'using', 'on', 'where', 'inner', 'left', 'right', 'full',
      'outer', 'cross', 'group', 'order', 'limit', 'having',
    ].includes(lower)) continue;
    aliases.push(alias);
  }
  if (aliases.length === 0) return sql;
  let out = sql;
  for (const a of aliases) {
    const escA = a.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    out = out.replace(new RegExp(`\\b${escA}\\.`, 'g'), '');
  }
  // Drop the alias tokens from FROM/JOIN clauses too.
  const clauseGuard = new Set([
    'where','group','order','limit','having','using','on','join',
    'inner','left','right','full','outer','cross',
  ]);
  out = out.replace(/(\bfrom\s+\w+)(\s+\w+)/gi, (full, head, tail) => {
    return clauseGuard.has(tail.trim().toLowerCase()) ? full : head;
  });
  out = out.replace(/(\bjoin\s+\w+)(\s+\w+)/gi, (full, head, tail) => {
    return clauseGuard.has(tail.trim().toLowerCase()) ? full : head;
  });
  return out;
}

// Examples registry — one declaration per example. The tab strip and
// the dock surface both read from this so adding a new example means
// writing one entry + one builder.

import type { ExampleId } from './shared';

export type FeatureTag = 'join' | 'view' | 'filter' | 'agg' | 'pivot' | 'stream' | 'window';

export interface ExampleEntry {
  id: ExampleId;
  serial: string;       // shown as "EX.01" or just "01"
  ord: number;          // 1-based ordinal
  /** Terse tab label — mixed case, ≤16 chars. */
  shortLabel: string;
  /** Full display title shown in the context band. Mixed case. */
  title: string;
  /** One-liner, kept for the markdown docs but no longer shown in chrome. */
  synopsis: string;
  features: FeatureTag[];
  category: 'live' | 'analytics' | 'reference' | 'lab';
}

export const EXAMPLES: ExampleEntry[] = [
  {
    id: 'live-pnl',
    serial: '01',
    ord: 1,
    shortLabel: 'Live PnL',
    title: 'Live Positions PnL Dashboard',
    synopsis: 'Join positions × trades; aggregate live PnL by book, trader and sector. KPIs flash on tick.',
    features: ['join', 'agg', 'stream', 'filter'],
    category: 'live',
  },
  {
    id: 'trade-blotter',
    serial: '02',
    ord: 2,
    shortLabel: 'Trade Blotter',
    title: 'Trade Blotter with Rich Filters',
    synopsis: 'A 200-column trade tape with multi-column predicate filters and slippage analytics.',
    features: ['filter', 'stream', 'window'],
    category: 'live',
  },
  {
    id: 'cross-asset-pivot',
    serial: '03',
    ord: 3,
    shortLabel: 'Cross-Asset Pivot',
    title: 'Cross-Asset Pivot',
    synopsis: 'Pivot positions by asset class × currency. Side panel surfaces drill-through detail.',
    features: ['pivot', 'agg', 'filter'],
    category: 'analytics',
  },
  {
    id: 'ticking-heatmap',
    serial: '04',
    ord: 4,
    shortLabel: 'Heatmap',
    title: 'Ticking Heatmap — Sector × Region',
    synopsis: 'A continuous heatmap of intraday returns driven by a cqserver pivot view that updates on tick.',
    features: ['view', 'pivot', 'agg', 'stream'],
    category: 'analytics',
  },
  {
    id: 'materialized-view',
    serial: '05',
    ord: 5,
    shortLabel: 'Materialized View',
    title: 'Materialized View — Net Exposure',
    synopsis: 'Define a server-side view; demonstrate sub-second refresh on upstream change.',
    features: ['view', 'agg'],
    category: 'reference',
  },
  {
    id: 'joins',
    serial: '06',
    ord: 6,
    shortLabel: 'Joins',
    title: 'Joins — Positions × Trades × Securities',
    synopsis: 'Walk through the three relational dimensions of cqserver: by key, by foreign key, and broadcast.',
    features: ['join', 'filter'],
    category: 'reference',
  },
  {
    id: 'slippage-agg',
    serial: '07',
    ord: 7,
    shortLabel: 'Slippage',
    title: 'Slippage Aggregation',
    synopsis: 'Group trades by venue + algorithm; surface arrival/VWAP slippage stats with rolling windows.',
    features: ['agg', 'window', 'filter'],
    category: 'analytics',
  },
  {
    id: 'query-builder',
    serial: '08',
    ord: 8,
    shortLabel: 'Query Builder',
    title: 'Query Builder — Pattern Library',
    synopsis: 'Run any of 40+ pre-built cqserver patterns against the dataset; edit and re-run live.',
    features: ['join', 'view', 'filter', 'agg', 'pivot', 'window'],
    category: 'lab',
  },
];

export function exampleById(id: ExampleId): ExampleEntry {
  const e = EXAMPLES.find((x) => x.id === id);
  if (!e) throw new Error(`Unknown example: ${id}`);
  return e;
}

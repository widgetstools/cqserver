// Examples registry — one declaration per example. The Atlas index
// and the dock surface both read from this so adding a new example
// means writing one entry + one builder.

import type { LucideIcon } from 'lucide-react';
import {
  Activity,
  Layers,
  Flame,
  Network,
  GitMerge,
  Filter,
  ListTree,
  TerminalSquare,
} from 'lucide-react';
import type { ExampleId } from './shared';

export type FeatureTag = 'join' | 'view' | 'filter' | 'agg' | 'pivot' | 'stream' | 'window';

export interface ExampleEntry {
  id: ExampleId;
  serial: string;         // shown as "EX.01"
  ord: number;            // 1-based ordinal
  title: string;          // display title (UPPER tracked)
  synopsis: string;       // one-liner shown under title
  features: FeatureTag[]; // capsule tags
  icon: LucideIcon;
  category: 'live' | 'analytics' | 'reference' | 'lab';
}

export const EXAMPLES: ExampleEntry[] = [
  {
    id: 'live-pnl',
    serial: 'EX.01',
    ord: 1,
    title: 'LIVE POSITIONS PNL DASHBOARD',
    synopsis: 'Join positions × trades; aggregate live PnL by book, trader and sector. KPIs flash on tick.',
    features: ['join', 'agg', 'stream', 'filter'],
    icon: Activity,
    category: 'live',
  },
  {
    id: 'trade-blotter',
    serial: 'EX.02',
    ord: 2,
    title: 'TRADE BLOTTER WITH RICH FILTERS',
    synopsis: 'A 200-column trade tape with multi-column predicate filters and slippage analytics.',
    features: ['filter', 'stream', 'window'],
    icon: ListTree,
    category: 'live',
  },
  {
    id: 'cross-asset-pivot',
    serial: 'EX.03',
    ord: 3,
    title: 'CROSS-ASSET PIVOT',
    synopsis: 'Pivot positions by asset class × currency. Side panel surfaces drill-through detail.',
    features: ['pivot', 'agg', 'filter'],
    icon: Layers,
    category: 'analytics',
  },
  {
    id: 'ticking-heatmap',
    serial: 'EX.04',
    ord: 4,
    title: 'TICKING HEATMAP — SECTOR × REGION',
    synopsis: 'A continuous heatmap of intraday returns driven by a cqserver pivot view that updates on tick.',
    features: ['view', 'pivot', 'agg', 'stream'],
    icon: Flame,
    category: 'analytics',
  },
  {
    id: 'materialized-view',
    serial: 'EX.05',
    ord: 5,
    title: 'MATERIALIZED VIEW — NET EXPOSURE',
    synopsis: 'Define a server-side view; demonstrate sub-second refresh on upstream change.',
    features: ['view', 'agg'],
    icon: Network,
    category: 'reference',
  },
  {
    id: 'joins',
    serial: 'EX.06',
    ord: 6,
    title: 'JOIN POSITIONS × TRADES × SECURITIES',
    synopsis: 'Walk through the three relational dimensions of cqserver: by key, by foreign key, and broadcast.',
    features: ['join', 'filter'],
    icon: GitMerge,
    category: 'reference',
  },
  {
    id: 'slippage-agg',
    serial: 'EX.07',
    ord: 7,
    title: 'SLIPPAGE AGGREGATION',
    synopsis: 'Group trades by venue + algorithm; surface arrival/VWAP slippage stats with rolling windows.',
    features: ['agg', 'window', 'filter'],
    icon: Filter,
    category: 'analytics',
  },
  {
    id: 'query-builder',
    serial: 'EX.08',
    ord: 8,
    title: 'QUERY BUILDER — PATTERN LIBRARY',
    synopsis: 'Run any of 40+ pre-built cqserver patterns against the dataset; edit and re-run live.',
    features: ['join', 'view', 'filter', 'agg', 'pivot', 'window'],
    icon: TerminalSquare,
    category: 'lab',
  },
];

export function exampleById(id: ExampleId): ExampleEntry {
  const e = EXAMPLES.find((x) => x.id === id);
  if (!e) throw new Error(`Unknown example: ${id}`);
  return e;
}

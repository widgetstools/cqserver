/**
 * Feature taxonomy — single source of truth for every cqserver primitive
 * referenced in the Atlas examples. The tab strip, the context bar, the
 * notes panels and (eventually) the query-builder filter chips all read
 * the same record here, so a new feature is added once.
 *
 * Each entry pairs a short letter glyph (rendered in a colored chip
 * inside the tab) with a name + sentence + the SQL clause it most
 * closely maps to. Colors are picked from the Stockflux palette so they
 * read consistently against teal / amber / slate / indigo / grey
 * themes without needing per-theme overrides.
 */

import type { FeatureTag } from '@/examples/registry';

export interface FeatureMeta {
  /** Same identifier used in `ExampleEntry.features`. */
  id: FeatureTag;
  /** Single-letter glyph rendered inside the tab feature chip. */
  glyph: string;
  /** Display name — used in chips and tooltips. */
  name: string;
  /** Mini SQL clause this primitive maps to in the cqserver query model. */
  clause: string;
  /** One-liner shown in the context bar on hover / under the chip. */
  blurb: string;
  /**
   * CSS variable that supplies the chip color. Kept as a `var()` token
   * so theme palette swaps adjust feature chrome automatically.
   */
  colorVar: string;
}

export const FEATURE_META: Record<FeatureTag, FeatureMeta> = {
  join: {
    id: 'join',
    glyph: 'J',
    name: 'Join',
    clause: 'JOIN ON',
    blurb: 'Relational joins across positions, trades, securities, risk',
    colorVar: 'var(--feature-join)',
  },
  view: {
    id: 'view',
    glyph: 'V',
    name: 'View',
    clause: 'CREATE VIEW',
    blurb: 'Server-side materialized view, refreshed on upstream change',
    colorVar: 'var(--feature-view)',
  },
  filter: {
    id: 'filter',
    glyph: 'F',
    name: 'Filter',
    clause: 'WHERE',
    blurb: 'Streaming content filter — predicate runs on every delta',
    colorVar: 'var(--feature-filter)',
  },
  agg: {
    id: 'agg',
    glyph: 'A',
    name: 'Aggregate',
    clause: 'GROUP BY',
    blurb: 'Incremental aggregation kept current as rows tick',
    colorVar: 'var(--feature-agg)',
  },
  pivot: {
    id: 'pivot',
    glyph: 'P',
    name: 'Pivot',
    clause: 'PIVOT',
    blurb: 'Rows × cols pivot table, recomputed on tick',
    colorVar: 'var(--feature-pivot)',
  },
  stream: {
    id: 'stream',
    glyph: 'S',
    name: 'Stream',
    clause: 'SOW + DELTAS',
    blurb: 'SOW snapshot + live delta feed, conflated server-side',
    colorVar: 'var(--feature-stream)',
  },
  window: {
    id: 'window',
    glyph: 'W',
    name: 'Window',
    clause: 'OVER (PARTITION BY)',
    blurb: 'Rolling window function over the live tape',
    colorVar: 'var(--feature-window)',
  },
};

/** Preferred display order — primary features first, refinements last. */
const ORDER: FeatureTag[] = ['stream', 'join', 'view', 'pivot', 'agg', 'filter', 'window'];

export function orderFeatures(tags: readonly FeatureTag[]): FeatureTag[] {
  return [...tags].sort((a, b) => ORDER.indexOf(a) - ORDER.indexOf(b));
}

/** Topical "headline" feature — the one we lead the example with. */
export function headlineFeature(tags: readonly FeatureTag[]): FeatureTag {
  return orderFeatures(tags)[0] ?? 'stream';
}

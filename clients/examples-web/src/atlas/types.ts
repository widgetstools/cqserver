/**
 * Atlas shared types. The data-layer hooks (Phase 2) and chapter
 * components (Phase 1+) both consume these.
 */

export type ChapterId =
  | 'pulse'
  | 'tape'
  | 'lens'
  | 'heat'
  | 'view'
  | 'join'
  | 'slip'
  | 'query';

export interface ChapterMeta {
  id: ChapterId;
  num: string;       // '01' .. '08' — typeset in the stations rail
  name: string;      // 'PULSE' — uppercase mono label
  kicker: string;    // 'LIVE BOOK' — eyebrow text on the chapter head
}

/** One chip in a chapter's filter rail. Phase 1 uses these for the
 *  visual chip rail; Phase 2 wires them to subscription-driven values. */
export interface ChipSpec {
  key: string;                  // 'BOOK', 'SECTOR' — the chip label
  column: string;               // 'book_name' — the source column
  source?: string;              // '/v_pnl_by_book' — view that supplies values (Phase 2)
  default?: string;             // first-paint scope (e.g. 'RATES-US')
}

export interface ChapterScope {
  primary: {
    topic: string;              // '/positions'
    rowIdKey: string;           // 'position_id'
    filter?: (s: Record<string, string>) => string | null;
  };
  views?: string[];             // '/v_book_totals' etc., subscribed for KPIs
  chips: ChipSpec[];
}

export interface KpiSpec {
  label: string;                // 'NET MV'
  format: 'ccy' | 'signed-ccy' | 'count' | 'pct';
  source: string;               // '/v_book_totals' — the view this reads from
  column: string;               // 'market_value'
  caption?: string;             // 'market_value · sum'
}

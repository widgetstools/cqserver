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

/**
 * One chip in a chapter's filter rail. Plan A's `useChapterScope` reads
 * `key`/`column`/`default`; chapter components subscribe to `source` to
 * derive the option list.
 */
export interface ChipSpec {
  key: string;                  // 'BOOK', 'SECTOR' — the chip label
  column: string;               // 'book_name' — the source column
  source?: string;              // '/v_pnl_by_book' — view that supplies values
  default?: string;             // first-paint scope (e.g. 'RATES-US')
}

// Per-chapter KPI/scope sketches were absorbed into Plan A:
//   - chip-and-WHERE state lives in `hooks/useChapterScope.ts`
//   - per-chapter KPI mapping lives in `scopes/<chapter>.ts` (e.g.
//     `PulseKpiDef` + `PULSE_KPIS` in `scopes/pulse.ts`)
// New chapters should follow the Plan A pattern, not import from here.

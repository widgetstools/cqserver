/**
 * The eight chapters of the Atlas. Order matters — drives the
 * stations rail layout and the `1`–`8` keyboard shortcuts.
 */
import type { ChapterMeta } from './types';

export const CHAPTERS: readonly ChapterMeta[] = [
  { id: 'pulse', num: '01', name: 'PULSE', kicker: 'LIVE BOOK' },
  { id: 'tape',  num: '02', name: 'TAPE',  kicker: 'FILTERED TRADE STREAM' },
  { id: 'lens',  num: '03', name: 'LENS',  kicker: 'CROSS-ASSET PIVOT' },
  { id: 'heat',  num: '04', name: 'HEAT',  kicker: 'SECTOR × REGION' },
  { id: 'view',  num: '05', name: 'VIEW',  kicker: 'MATERIALIZED VIEW' },
  { id: 'join',  num: '06', name: 'JOIN',  kicker: 'TRADES × POSITIONS' },
  { id: 'slip',  num: '07', name: 'SLIP',  kicker: 'SLIPPAGE AGGREGATION' },
  { id: 'query', num: '08', name: 'QUERY', kicker: 'AD-HOC SQL' },
] as const;

export function chapterById(id: string): ChapterMeta | undefined {
  return CHAPTERS.find((c) => c.id === id);
}

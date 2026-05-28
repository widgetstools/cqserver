/**
 * Chapter scope — single source of truth for a chapter's chip
 * selections, the resulting SQL WHERE expression, and a human-readable
 * summary. Every Atlas chapter consumes this hook so the filter
 * rail / subscription wiring looks identical across chapters.
 *
 * Initial state comes from each chip's `default` field (Phase 1's
 * `ChipSpec`). `setChip(key, value)` is the per-chip setter; `setState`
 * accepts FilterRail's full-record `onChange` callback.
 */
import { useCallback, useMemo, useState } from 'react';
import type { ChipSpec } from '../types';

export interface ChapterScopeHandle {
  /** chip.key → current selection. undefined or 'All' = no constraint. */
  state: Record<string, string | undefined>;
  /** Composed SQL WHERE expression, or null if every chip is unconstrained. */
  filterExpression: string | null;
  /** Human-readable summary: `book_name = 'RATES-US' · issuer_sector = 'Tech'`. */
  summary: string;
  /** Update one chip's value. Pass undefined to clear it. */
  setChip: (key: string, value: string | undefined) => void;
  /** Replace the whole state record — wired to FilterRail's onChange. */
  setState: (next: Record<string, string | undefined>) => void;
}

export function useChapterScope(
  chips: readonly ChipSpec[],
): ChapterScopeHandle {
  const initial = useMemo<Record<string, string | undefined>>(() => {
    const out: Record<string, string | undefined> = {};
    for (const c of chips) {
      if (c.default) out[c.key] = c.default;
    }
    return out;
  }, [chips]);

  const [state, setRawState] = useState<Record<string, string | undefined>>(initial);

  const setChip = useCallback((key: string, value: string | undefined) => {
    setRawState((prev) => ({ ...prev, [key]: value }));
  }, []);

  const { filterExpression, summary } = useMemo(() => {
    const parts: string[] = [];
    for (const c of chips) {
      const v = state[c.key];
      if (v == null || v === '' || v === 'All') continue;
      // Single-quote escape: SQL single-quote → '' (matches cqserver's parser).
      const escaped = v.replace(/'/g, "''");
      parts.push(`${c.column} = '${escaped}'`);
    }
    return {
      filterExpression: parts.length === 0 ? null : parts.join(' AND '),
      summary: parts.length === 0 ? '(unfiltered)' : parts.join(' · '),
    };
  }, [chips, state]);

  return { state, filterExpression, summary, setChip, setState: setRawState };
}

/**
 * Extract sorted distinct string values from a column across a snapshot
 * of view rows. Used by chapter components to derive chip option lists
 * from their bound view subscriptions.
 */
export function distinctValues(
  rows: readonly Record<string, unknown>[],
  column: string,
): string[] {
  const set = new Set<string>();
  for (const r of rows) {
    const v = r[column];
    if (v != null && v !== '') set.add(String(v));
  }
  return Array.from(set).sort();
}

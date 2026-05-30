/**
 * Join (Chapter 06 — JOIN) scope.
 *
 * The chapter renders three side-by-side grids:
 *   - LHS — /positions   (one side of the join, filtered by chip)
 *   - MID — /trades      (other side of the join, filtered by chip + FILLED)
 *   - RHS — /v_trades_by_compliance (server-side joined aggregate)
 *
 * The BOOK chip filters the two raw sides — it does NOT filter the
 * joined view, because /v_trades_by_compliance groups by
 * compliance_status only (no book_name column). The narrative is
 * intentional: "filter what you see locally; cqserver's join still
 * runs across the whole book and always returns the full aggregate."
 *
 * Trades are additionally filtered to status='FILLED' so the right
 * pane stays small and tickable (matches the same trick TapeChapter
 * uses to keep /trades manageable).
 */
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const JOIN_CHIPS: readonly ChipSpec[] = [
  // Default to a real seeded book name (see refdata.ts BOOKS list).
  { key: 'BOOK', column: 'book_name', source: '/positions', default: 'Rates Curve' },
];

/** Hardcoded book options — match refdata.ts BOOKS, no extra sub needed. */
export const JOIN_BOOK_OPTIONS = [
  'All',
  'Global Macro',
  'Equity Long-Short',
  'Credit Relative Val',
  'Vol Arbitrage',
  'Index Replication',
  'Rates Curve',
  'EM Sovereign',
  'High-Yield Carry',
];

/** Positions side — identifier + the column the join hangs off
 *  (position_id) + compliance/sector context. */
export const JOIN_POSITIONS_COL_DEFS: ColDef[] = [
  { field: 'position_id', headerName: 'position_id', width: 110, cellStyle: { color: 'var(--atlas-amber)' } },
  { field: 'book_name', headerName: 'book', width: 110 },
  { field: 'symbol', headerName: 'symbol', width: 80 },
  { field: 'issuer_sector', headerName: 'sector', width: 110 },
  {
    field: 'compliance_status',
    headerName: 'compliance',
    width: 120,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.08em' }
        : { color: 'var(--atlas-amber)', letterSpacing: '.08em' },
  },
  {
    field: 'market_value_usd',
    headerName: 'market_value',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number),
  },
];

/** Trades side — identifier + the join key (position_id) + execution. */
export const JOIN_TRADES_COL_DEFS: ColDef[] = [
  { field: 'trade_id', headerName: 'trade_id', width: 120, cellStyle: { color: 'var(--atlas-amber)' } },
  { field: 'position_id', headerName: 'position_id', width: 110 },
  { field: 'symbol', headerName: 'symbol', width: 80 },
  {
    field: 'side',
    headerName: 'side',
    width: 70,
    cellStyle: (p) =>
      p.value === 'BUY'
        ? { color: '#7ec96a' }
        : p.value === 'SELL'
          ? { color: '#ff6062' }
          : null,
  },
  {
    field: 'quantity',
    headerName: 'qty',
    width: 100,
    type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—',
  },
  {
    field: 'notional_usd',
    headerName: 'notional',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number),
  },
];

/** Joined view — the cqserver-aggregated rollup of trades-grouped-by
 *  position-compliance-status. Same shape it has always been. */
export const JOIN_RESULT_COL_DEFS: ColDef[] = [
  {
    field: 'compliance_status',
    headerName: 'compliance',
    width: 130,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.1em' }
        : { color: 'var(--atlas-amber)', letterSpacing: '.1em' },
  },
  {
    field: 'n_trades',
    headerName: 'n_trades',
    width: 100,
    type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—',
  },
  {
    field: 'total_fees',
    headerName: 'total_fees',
    width: 120,
    type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number),
  },
  {
    field: 'avg_slip_arr',
    headerName: 'avg_slip_arr',
    width: 110,
    type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number),
  },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(2)}M`;
}

export function fmtBps(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(1)} bps`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

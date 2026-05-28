/**
 * Pulse (Chapter 01 — Live Book) scope.
 *
 * Every datum the chapter component needs to render against real
 * cqserver data: chip definitions for the filter rail, KPI mapping
 * for the strip, column subset for the positions table.
 */
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

/** Chips render the FilterRail and drive the WHERE expression sent to
 *  /positions. Each chip's options come from the `source` view. */
export const PULSE_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', source: '/v_pnl_by_book', default: 'RATES-US' },
  { key: 'SECTOR', column: 'issuer_sector', source: '/v_pnl_by_sector' },
  { key: 'COMPLIANCE', column: 'compliance_status', source: '/v_compliance_counts' },
];

/**
 * Map of column names on `/v_book_totals` → display label / formatter
 * for the KPI strip. The chapter component reads the single aggregate
 * row of /v_book_totals and produces a `Kpi[]` from this mapping.
 *
 * `/v_compliance_counts` provides the BREACH count separately because it
 * lives on a different view (one row per status bucket).
 */
export interface PulseKpiDef {
  label: string;
  caption?: string;
  /** Field on /v_book_totals to read, or '__breaches__' for the synthetic
   *  breach count derived from /v_compliance_counts. */
  source: string;
  /** Display formatter. */
  format: 'currency-m' | 'currency-m-signed' | 'count';
  /** Apply amber colour to the value. */
  emphasis?: boolean;
}

export const PULSE_KPIS: readonly PulseKpiDef[] = [
  { label: 'NET MV', source: 'market_value', format: 'currency-m', caption: 'market_value · sum', emphasis: true },
  { label: 'EXPOSURE', source: 'exposure_gross', format: 'currency-m', caption: 'gross · sum' },
  { label: 'DAY PnL', source: 'day_pnl', format: 'currency-m-signed', caption: 'today', emphasis: true },
  { label: 'YTD PnL', source: 'ytd_pnl', format: 'currency-m-signed', caption: 'inception', emphasis: true },
  { label: 'VaR (1d)', source: 'var_95', format: 'currency-m', caption: '95% confidence' },
  { label: 'BREACHES', source: '__breaches__', format: 'count', caption: 'compliance' },
];

/** Column subset shown in the Pulse positions table. The full /positions
 *  topic has 206 columns; we show the eight that matter for a live read. */
export const PULSE_COL_DEFS: ColDef[] = [
  { field: 'position_id', headerName: 'position_id', width: 110, cellStyle: { color: '#f4a52b' } },
  { field: 'book_name', headerName: 'book', width: 110 },
  { field: 'symbol', headerName: 'symbol', width: 80 },
  { field: 'issuer_sector', headerName: 'sector', width: 120 },
  { field: 'asset_class', headerName: 'asset_class', width: 110 },
  {
    field: 'market_value_usd',
    headerName: 'market_value',
    width: 140,
    type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number),
    cellClass: 'ag-right-aligned-cell',
  },
  {
    field: 'day_pnl',
    headerName: 'day_pnl',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0
        ? { color: '#ff6062' }
        : typeof p.value === 'number'
          ? { color: '#f4a52b' }
          : null,
  },
  {
    field: 'compliance_status',
    headerName: 'status',
    width: 100,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.1em' }
        : { color: '#f4a52b', letterSpacing: '.1em' },
  },
];

/** Format a raw USD amount as `+$1.21M` / `-$0.04M`. */
export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

/** Format a raw USD amount as `$82.1M`. */
export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(1)}M`;
}

/** Format an integer count as `4,827`. */
export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

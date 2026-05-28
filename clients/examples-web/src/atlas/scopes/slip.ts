import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const SLIP_CHIPS: readonly ChipSpec[] = [
  { key: 'VENUE', column: 'execution_venue', source: '/v_slippage_venue_algo' },
  { key: 'ALGO', column: 'execution_algo', source: '/v_slippage_venue_algo' },
];

export const SLIP_COL_DEFS: ColDef[] = [
  { field: 'execution_venue', headerName: 'venue', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'execution_algo', headerName: 'algo', width: 140 },
  { field: 'n_trades', headerName: 'n_trades', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'avg_slip_arr', headerName: 'avg_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value > 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
  { field: 'avg_slip_vwap', headerName: 'avg_slip_vwap', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'max_slip_arr', headerName: 'max_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'min_slip_arr', headerName: 'min_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
  { field: 'total_fees', headerName: 'total_fees', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
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

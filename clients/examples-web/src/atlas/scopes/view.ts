import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const VIEW_CHIPS: readonly ChipSpec[] = [
  { key: 'BOOK', column: 'book_name', source: '/v_net_exposure' },
  { key: 'ASSET', column: 'asset_class', source: '/v_net_exposure' },
  { key: 'CCY', column: 'currency', source: '/v_net_exposure' },
];

export const VIEW_COL_DEFS: ColDef[] = [
  { field: 'book_name', headerName: 'book', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'asset_class', headerName: 'asset', width: 120 },
  { field: 'currency', headerName: 'ccy', width: 70 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'net_mv_usd', headerName: 'net_mv', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
  { field: 'gross_exposure', headerName: 'gross', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'net_dv01', headerName: 'dv01', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtSigned(p.value as number) },
  { field: 'sum_var', headerName: 'var', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'worst_util_pct', headerName: 'worst_util', width: 130, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toFixed(1) ?? '—' },
];

export function fmtMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `$${(n / 1_000_000).toFixed(1)}M`;
}

export function fmtSignedMillions(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n) / 1_000_000;
  const sign = n >= 0 ? '+' : '−';
  return `${sign}$${abs.toFixed(2)}M`;
}

export function fmtSigned(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n >= 0 ? `+${n.toFixed(0)}` : `−${Math.abs(n).toFixed(0)}`;
}

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

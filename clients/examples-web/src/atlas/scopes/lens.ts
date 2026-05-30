import type { ColDef } from 'ag-grid-community';

export const LENS_COL_DEFS: ColDef[] = [
  { field: 'asset_class', headerName: 'asset_class', width: 130, cellStyle: { color: 'var(--atlas-amber)' } },
  { field: 'currency', headerName: 'ccy', width: 80 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'market_value_usd', headerName: 'market_value', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'unrealized_pnl_usd', headerName: 'unrealized_pnl', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: 'var(--atlas-amber)' } : null },
  { field: 'exposure_gross', headerName: 'gross', width: 130, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'var_1d_95', headerName: 'var_1d', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
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

export function fmtCount(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

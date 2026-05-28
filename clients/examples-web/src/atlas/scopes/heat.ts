import type { ColDef } from 'ag-grid-community';

export const HEAT_COL_DEFS: ColDef[] = [
  { field: 'issuer_sector', headerName: 'sector', width: 160, cellStyle: { color: '#f4a52b' } },
  { field: 'issuer_region', headerName: 'region', width: 140 },
  { field: 'n_positions', headerName: 'n_positions', width: 120, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'weight', headerName: 'weight', width: 110, type: 'numericColumn',
    valueFormatter: (p) => fmtPct(p.value as number) },
  { field: 'weighted_sum', headerName: 'weighted_sum', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtSignedMillions(p.value as number),
    cellStyle: (p) =>
      typeof p.value === 'number' && p.value < 0 ? { color: '#ff6062' } :
      typeof p.value === 'number' ? { color: '#f4a52b' } : null },
];

export function fmtPct(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${(n * 100).toFixed(2)}%`;
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

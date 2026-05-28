import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

export const JOIN_CHIPS: readonly ChipSpec[] = [
  { key: 'COMPLIANCE', column: 'compliance_status', source: '/v_trades_by_compliance' },
];

export const JOIN_COL_DEFS: ColDef[] = [
  { field: 'compliance_status', headerName: 'compliance_status', width: 220,
    cellStyle: (p) =>
      p.value === 'BREACH'
        ? { color: '#ff6062', letterSpacing: '.1em' }
        : { color: '#f4a52b', letterSpacing: '.1em' } },
  { field: 'n_trades', headerName: 'n_trades', width: 130, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'total_fees', headerName: 'total_fees', width: 150, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'avg_slip_arr', headerName: 'avg_slip_arr', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtBps(p.value as number) },
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

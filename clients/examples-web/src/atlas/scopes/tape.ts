/**
 * Tape (Chapter 02 — Trade Blotter) scope.
 * Live trade tape filtered server-side; KPI strip from a continuous SQL aggregate.
 */
import type { ColDef } from 'ag-grid-community';
import type { ChipSpec } from '../types';

/**
 * Tape chips default STATUS to 'FILLED' so the initial subscription
 * loads ~1/7 of the trade universe instead of all 320k+ rows. SIDE/STATUS
 * options are hardcoded from the publisher's known enum (refdata.ts
 * SIDES + TRADE_STATUSES) rather than derived from a secondary unfiltered
 * /trades sub — the secondary sub doubled the network cost and was the
 * main reason Tape felt slow.
 */
export const TAPE_CHIPS: readonly ChipSpec[] = [
  { key: 'SIDE', column: 'side', source: '/trades' },
  { key: 'STATUS', column: 'status', source: '/trades', default: 'FILLED' },
];

/** Static chip option lists — match the publisher's refdata.ts. */
export const TAPE_SIDE_OPTIONS = ['All', 'BUY', 'SELL', 'SHORT', 'COVER'];
export const TAPE_STATUS_OPTIONS = [
  'All',
  'NEW',
  'PARTIALLY_FILLED',
  'FILLED',
  'CANCELED',
  'REJECTED',
  'EXPIRED',
  'PENDING_REVIEW',
];

export const TAPE_COL_DEFS: ColDef[] = [
  { field: 'trade_id', headerName: 'trade_id', width: 130, cellStyle: { color: 'var(--atlas-amber)' } },
  { field: 'position_id', headerName: 'position_id', width: 130 },
  { field: 'symbol', headerName: 'symbol', width: 90 },
  { field: 'side', headerName: 'side', width: 70,
    cellStyle: (p) =>
      p.value === 'BUY' ? { color: '#7ec96a' } : p.value === 'SELL' ? { color: '#ff6062' } : null },
  { field: 'quantity', headerName: 'qty', width: 100, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toLocaleString('en-US') ?? '—' },
  { field: 'price', headerName: 'price', width: 100, type: 'numericColumn',
    valueFormatter: (p) => (p.value as number)?.toFixed(4) ?? '—' },
  { field: 'notional_usd', headerName: 'notional_usd', width: 140, type: 'numericColumn',
    valueFormatter: (p) => fmtMillions(p.value as number) },
  { field: 'status', headerName: 'status', width: 110,
    cellStyle: { color: 'var(--atlas-amber)', letterSpacing: '.1em' } },
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

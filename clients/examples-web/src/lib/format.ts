// Numeric formatters used across grids, KPIs, panels. Each accepts
// possibly null/undefined and returns an em-dash for non-finite values.

export function fmtInt(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return Math.round(n).toLocaleString('en-US');
}

export function fmtDec(n: number | null | undefined, digits = 2): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function fmtCcy(
  n: number | null | undefined,
  ccy = 'USD',
  digits = 0,
): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n);
  if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B ${ccy}`;
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M ${ccy}`;
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}K ${ccy}`;
  return `${n.toFixed(digits)} ${ccy}`;
}

export function fmtPct(n: number | null | undefined, digits = 2): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(digits)}%`;
}

export function fmtBps(n: number | null | undefined, digits = 1): string {
  if (n == null || !Number.isFinite(n)) return '—';
  return `${n.toFixed(digits)} bps`;
}

export function fmtSigned(n: number | null | undefined, digits = 0): string {
  if (n == null || !Number.isFinite(n)) return '—';
  const abs = Math.abs(n);
  const sign = n >= 0 ? '+' : '−';
  if (abs >= 1_000_000_000) return `${sign}${(abs / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1)}K`;
  return `${sign}${abs.toFixed(digits)}`;
}

export function fmtTime(ts: number | string | null | undefined): string {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts) : new Date(ts);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleTimeString('en-US', { hour12: false });
}

export function fmtDate(ts: number | string | null | undefined): string {
  if (ts == null) return '—';
  const d = typeof ts === 'number' ? new Date(ts) : new Date(ts);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toISOString().slice(0, 10);
}

// Returns the numeric sign-class for grid cells: 'num-pos', 'num-neg', or ''.
export function signClass(n: number | null | undefined): string {
  if (n == null || !Number.isFinite(n) || n === 0) return '';
  return n > 0 ? 'num-pos' : 'num-neg';
}

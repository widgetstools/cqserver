// clients/examples-web/src/atlas/preview/placeholderData.ts
import type { ColDef } from 'ag-grid-community';
import type { Kpi } from '../components/KpiStrip';

export interface PulseRow {
  position_id: string;
  issuer: string;
  market_value: number;
  day_pnl: number;
  var_1d: number;
  util_pct: number;
  status: 'OK' | 'BREACH';
  [key: string]: unknown;
}

const ISSUERS = [
  'US Treasury 10Y', 'FNMA 30Y MBS', 'JPMC Sr Unsec', 'Apple 2031', 'Hertz HY 6.25',
  'Ford Mtr Co', 'UST Bill 3M', 'Microsoft 2029', 'Verizon 5.0', 'Bank of America Sub',
  'Caterpillar 2027', 'Comcast Cable 6.0', 'Goldman 5.5', 'Pfizer 2030', 'Intel 2032',
];

export function makePulseRows(n = 80): PulseRow[] {
  const rows: PulseRow[] = [];
  for (let i = 0; i < n; i++) {
    const mv = 0.5 + ((i * 173) % 1000) / 60;
    const pnl = (((i * 211) % 200) - 100) * 500;
    const util = 20 + ((i * 41) % 90);
    rows.push({
      position_id: `P-${String(481 + i).padStart(5, '0')}`,
      issuer: ISSUERS[i % ISSUERS.length]!,
      market_value: mv,
      day_pnl: pnl,
      var_1d: 1000 + ((i * 79) % 18000),
      util_pct: Math.round(util),
      status: util > 100 ? 'BREACH' : 'OK',
    });
  }
  return rows;
}

const fmtCcy = (n: number) =>
  n.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 });
const fmtSigned = (n: number) => (n >= 0 ? '+' : '−') + fmtCcy(Math.abs(n));
const fmtK = (n: number) => `${(n / 1000).toFixed(1)}k`;

export const PULSE_COL_DEFS: ColDef<PulseRow>[] = [
  {
    field: 'position_id',
    headerName: 'position_id',
    width: 110,
    cellStyle: { color: '#f4a52b' },
  },
  { field: 'issuer', headerName: 'issuer', flex: 1 },
  {
    field: 'market_value',
    headerName: 'market_value',
    width: 130,
    type: 'numericColumn',
    valueFormatter: (p) => `${(p.value as number).toFixed(2)}M`,
    cellClass: 'ag-right-aligned-cell',
  },
  {
    field: 'day_pnl',
    headerName: 'day_pnl',
    width: 120,
    type: 'numericColumn',
    valueFormatter: (p) => fmtSigned(p.value as number),
    cellClassRules: {
      'ag-pnl-pos': (p) => (p.value as number) >= 0,
      'ag-pnl-neg': (p) => (p.value as number) < 0,
    },
  },
  {
    field: 'var_1d',
    headerName: 'var_1d',
    width: 100,
    type: 'numericColumn',
    valueFormatter: (p) => fmtK(p.value as number),
  },
  {
    field: 'util_pct',
    headerName: 'util_%',
    width: 90,
    type: 'numericColumn',
    valueFormatter: (p) => `${p.value}`,
  },
  {
    field: 'status',
    headerName: 'status',
    width: 100,
    cellStyle: (p) => ({
      color: p.value === 'BREACH' ? '#ff6062' : '#f4a52b',
      letterSpacing: '.1em',
    }),
  },
];

export const PULSE_KPIS: readonly Kpi[] = [
  { label: 'NET MV', value: '$82.1M', caption: 'market_value · sum', emphasis: true },
  { label: 'EXPOSURE', value: '$248.6M', caption: 'gross · sum' },
  { label: 'DAY PnL', value: '+$0.41M', caption: 'today', emphasis: true },
  { label: 'YTD PnL', value: '+$8.92M', caption: 'inception', emphasis: true },
  { label: 'VaR (1d)', value: '$0.96M', caption: '95% confidence' },
  { label: 'POSITIONS', value: '4,827', caption: 'in scope' },
];

export const BOOK_OPTIONS = ['RATES-US', 'CREDIT-IG', 'EQTY-VOL', 'FX-MACRO'];
export const SECTOR_OPTIONS = ['All', 'Technology', 'Financials', 'Energy', 'Industrials'];
export const COMPLIANCE_OPTIONS = ['All', 'OK', 'BREACH'];

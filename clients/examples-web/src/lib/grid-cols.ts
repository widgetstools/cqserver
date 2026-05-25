// Build AG-Grid ColDef arrays from a column-schema definition.
// One file for both positions + trades since the col types map 1:1.

import type { ColDef, ValueFormatterParams, CellClassParams } from 'ag-grid-community';
import type { PositionColumn } from './schema/positions';
import type { TradeColumn } from './schema/trades';
import { fmtDec, fmtInt, fmtCcy, fmtPct, fmtBps, fmtSigned, fmtTime } from './format';

type AnyCol = PositionColumn | TradeColumn;

function formatterFor(type: AnyCol['type'], signed: boolean | undefined) {
  return (p: ValueFormatterParams) => {
    const v = p.value;
    if (v == null || v === '') return '—';
    switch (type) {
      case 'int':       return fmtInt(v as number);
      case 'qty':       return signed ? fmtSigned(v as number, 0) : fmtInt(v as number);
      case 'price':     return fmtDec(v as number, 4);
      case 'ccy':       return signed ? fmtSigned(v as number) : fmtCcy(v as number, '', 0);
      case 'pct':       return fmtPct(v as number, 2);
      case 'bps':       return fmtBps(v as number, 1);
      case 'rate':      return fmtDec(v as number, 4);
      case 'bool':      return v ? '●' : '○';
      case 'datetime':  return fmtTime(v as string);
      case 'date':      return v as string;
      default:          return String(v);
    }
  };
}

function cellClassFor(type: AnyCol['type'], signed: boolean | undefined): (p: CellClassParams) => string {
  return (p: CellClassParams) => {
    const isNumeric = type === 'qty' || type === 'price' || type === 'ccy' || type === 'pct' || type === 'bps' || type === 'rate' || type === 'int';
    const classes: string[] = [];
    if (isNumeric) classes.push('tabular-cell text-right');
    if (signed && typeof p.value === 'number') {
      if (p.value > 0) classes.push('num-pos');
      else if (p.value < 0) classes.push('num-neg');
    }
    return classes.join(' ');
  };
}

function widthFor(col: AnyCol): number {
  switch (col.type) {
    case 'string':   return col.field.includes('id') || col.field.includes('name') ? 130 : 110;
    case 'enum':     return 110;
    case 'date':     return 100;
    case 'datetime': return 95;
    case 'int':      return 80;
    case 'qty':      return 110;
    case 'price':    return 100;
    case 'ccy':      return 130;
    case 'pct':      return 85;
    case 'bps':      return 105;
    case 'rate':     return 90;
    case 'bool':     return 60;
    default:         return 100;
  }
}

export function buildColDefs<C extends AnyCol>(cols: C[]): ColDef[] {
  return cols.map((c) => {
    const isNumeric = ['int', 'qty', 'price', 'ccy', 'pct', 'bps', 'rate'].includes(c.type);
    return {
      field: c.field,
      headerName: c.label,
      width: widthFor(c),
      valueFormatter: formatterFor(c.type, c.signed),
      cellClass: cellClassFor(c.type, c.signed),
      filter: isNumeric ? 'agNumberColumnFilter' : 'agTextColumnFilter',
      sortable: true,
      resizable: true,
    } satisfies ColDef;
  });
}

/** A trimmed set of columns suited to default views — picks the most
 *  useful columns from each group so a fresh grid is readable without
 *  scrolling 200 fields. Callers can override. */
export function defaultPositionView(): string[] {
  return [
    'position_id', 'book_name', 'trader_name', 'symbol', 'security_name',
    'asset_class', 'issuer_sector', 'issuer_region', 'currency',
    'quantity', 'last_price', 'market_value_usd', 'cost_basis_usd',
    'unrealized_pnl_usd', 'realized_pnl_usd', 'day_pnl', 'mtd_pnl', 'ytd_pnl',
    'dv01_usd', 'var_1d_95', 'delta_dollar',
    'compliance_status', 'risk_limit_utilization_pct',
  ];
}

export function defaultTradeView(): string[] {
  return [
    'trade_id', 'trade_ts', 'position_id', 'book_name', 'trader_name',
    'symbol', 'side', 'quantity', 'price', 'notional_usd',
    'execution_venue', 'execution_algo',
    'slippage_arrival_bps', 'slippage_vwap_bps',
    'total_fees_usd', 'commission_bps',
    'status', 'lifecycle_stage', 'settlement_status',
    'counterparty', 'broker',
  ];
}

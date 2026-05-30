/**
 * Parse AMPS PIVOT SQL and reshape wide server output for AG Grid pivot mode.
 */
import type { PivotDisplayConfig } from '@/lib/queries/library';
import type { Row } from '@/lib/use-subscription';

export type { PivotDisplayConfig };

export interface ParsedPivotSpec {
  pivotField: string;
  pivotValues: string[];
  dynamic: boolean;
  measures: Array<{ alias: string; shortLabel: string }>;
  multiMeasure: boolean;
}

export function parsePivotSql(sql: string): ParsedPivotSpec | null {
  const stripped = sql
    .replace(/--[^\n]*/g, ' ')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
  const m = stripped.match(
    /\bPIVOT\s*\(\s*([\s\S]+?)\s+FOR\s+(\w+)\s+IN\s+(?:\(\s*([^)]*?)\s*\)|(ANY))\s*\)/i,
  );
  if (!m) return null;

  const aggPart = m[1]!.trim();
  const pivotField = m[2]!;
  const inRaw = m[3];
  const dynamic = inRaw == null;
  const pivotValues = inRaw ? parseInList(inRaw) : [];
  const aggStrings = splitAggregates(aggPart);
  const measures = aggStrings.map((a) => ({
    alias: a.replace(/\s+AS\s+\w+$/i, '').trim(),
    shortLabel: measureShortLabel(a),
  }));

  return {
    pivotField,
    pivotValues,
    dynamic,
    measures,
    multiMeasure: measures.length > 1,
  };
}

export function inferPivotDisplay(
  spec: ParsedPivotSpec,
): PivotDisplayConfig {
  const byField: Record<string, string[]> = {
    currency: ['asset_class'],
    issuer_region: ['issuer_sector'],
    asset_class: ['book_name'],
  };
  return { rowGroupFields: byField[spec.pivotField] ?? ['book_name'] };
}

/** Wide AMPS pivot rows → long rows for AG Grid pivotMode. */
export function unpivotWideRows(
  wideRows: Row[],
  spec: ParsedPivotSpec,
  rowGroupFields: string[],
): Row[] {
  if (wideRows.length === 0) return [];

  const pivotValueCols = resolvePivotValueColumns(wideRows, spec, rowGroupFields);
  const long: Row[] = [];

  for (const wide of wideRows) {
    const anchor: Row = {};
    for (const f of rowGroupFields) {
      anchor[f] = wide[f] ?? null;
    }
    const wideId = rowGroupFields.map((f) => String(wide[f] ?? '')).join('\0');

    if (!spec.multiMeasure) {
      for (const pv of pivotValueCols) {
        long.push({
          ...anchor,
          __pivotKey: pv,
          __value: wide[pv] ?? null,
          __rowId: `${wideId}|${pv}`,
        });
      }
      continue;
    }

    for (const pv of spec.pivotValues.length > 0 ? spec.pivotValues : pivotValueCols) {
      for (const measure of spec.measures) {
        const col = `${pv}_${measure.alias}`;
        long.push({
          ...anchor,
          __pivotKey: `${pv} · ${measure.shortLabel}`,
          __value: wide[col] ?? null,
          __rowId: `${wideId}|${pv}|${measure.alias}`,
        });
      }
    }
  }

  return long;
}

export function unpivotRowId(row: Row): string {
  return String(row.__rowId ?? JSON.stringify(row));
}

function resolvePivotValueColumns(
  wideRows: Row[],
  spec: ParsedPivotSpec,
  rowGroupFields: string[],
): string[] {
  if (spec.pivotValues.length > 0 && !spec.multiMeasure) {
    return spec.pivotValues;
  }

  const sample = wideRows[0]!;
  const deny = new Set(rowGroupFields);
  if (spec.multiMeasure) {
    return spec.pivotValues;
  }

  return Object.keys(sample)
    .filter((k) => !deny.has(k) && !k.startsWith('__'))
    .filter((k) => {
      const v = sample[k];
      return typeof v === 'number' || v === null;
    })
    .sort();
}

function parseInList(raw: string): string[] {
  const out: string[] = [];
  const re = /'([^']*)'|"([^"]*)"/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(raw)) !== null) {
    out.push(m[1] ?? m[2] ?? '');
  }
  return out;
}

function splitAggregates(part: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let cur = '';
  for (const ch of part) {
    if (ch === '(') depth += 1;
    if (ch === ')') depth -= 1;
    if (ch === ',' && depth === 0) {
      if (cur.trim()) out.push(cur.trim());
      cur = '';
    } else {
      cur += ch;
    }
  }
  if (cur.trim()) out.push(cur.trim());
  return out;
}

function measureShortLabel(agg: string): string {
  const inner = agg.match(/SUM\s*\(\s*(\w+)\s*\)/i)?.[1]?.toLowerCase() ?? '';
  if (inner.includes('market_value') || inner.endsWith('_mv')) return 'MV';
  if (inner.includes('var')) return 'VaR';
  if (inner.includes('pnl')) return 'PnL';
  if (inner.includes('notional')) return 'Notional';
  return agg.replace(/\s+/g, ' ').slice(0, 12);
}

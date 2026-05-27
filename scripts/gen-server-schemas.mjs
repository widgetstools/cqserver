#!/usr/bin/env node
/**
 * Regenerate cqserver JSON schemas for /positions and /trades from the
 * single source of truth in clients/examples-web/src/lib/schema/.
 *
 * The TS files declare 200+ columns each with logical types (`price`,
 * `bps`, `qty`, etc.) we map to cqserver's primitive types here.
 *
 * Run after editing the TS schemas:
 *   node scripts/gen-server-schemas.mjs
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

// Logical type → cqserver primitive.
//
// Native types in use:
//   - Phase 11: native `bool` (Atlas filter chips like `mifid_flag = true`)
//   - Phase 12: native `timestamp` for `date` / `datetime` logical types.
//     Wire form: RFC 3339 string (`"2026-05-25T22:48:43.201Z"`); the
//     server keeps i64 μs since UNIX epoch internally and renders
//     consistently. Filters can write `trade_ts > '2026-05-25'`.
const TYPE_MAP = {
  string: 'string',
  enum: 'string',
  date: 'timestamp',
  datetime: 'timestamp',
  tag: 'string',
  int: 'long',
  qty: 'long',
  price: 'double',
  ccy: 'double',
  pct: 'double',
  bps: 'double',
  rate: 'double',
  bool: 'bool',
};

function parseSchemaTs(path) {
  const src = readFileSync(path, 'utf8');
  // Match the body of `export const X_COLUMNS: Y[] = [ ... ]`. Anchor on
  // the `= [` so we don't accidentally lock onto the `[]` of the type.
  const startRe = /export const \w+_COLUMNS\s*:\s*\w+\[\]\s*=\s*\[/;
  const m = startRe.exec(src);
  if (!m) throw new Error(`Could not find COLUMNS export in ${path}`);
  const bodyStart = m.index + m[0].length - 1; // index of the `[`
  // naive brace matcher — the schemas are flat object arrays
  let depth = 0;
  let i = bodyStart;
  for (; i < src.length; i++) {
    const c = src[i];
    if (c === '[') depth++;
    else if (c === ']') {
      depth--;
      if (depth === 0) {
        i++;
        break;
      }
    }
  }
  const body = src.slice(bodyStart, i);

  // Extract each `{ field: '...', type: '...', ... }` object.
  // The fields we need (field, type) appear at known positions.
  const cols = [];
  const re = /field:\s*'([^']+)'[^}]*?type:\s*'([^']+)'/g;
  let hit;
  while ((hit = re.exec(body)) !== null) {
    const [, field, type] = hit;
    const mapped = TYPE_MAP[type];
    if (!mapped) throw new Error(`Unknown logical type "${type}" for field "${field}"`);
    cols.push([field, mapped]);
  }
  return cols;
}

function buildSchema(cols, extraFirst = {}) {
  // The schema JSON is a flat object {field: type}. cqserver preserves
  // declaration order which becomes physical column order in the SOW
  // store — put primary key + identifier columns first.
  const out = { ...extraFirst };
  for (const [field, type] of cols) {
    if (field in out) continue;
    out[field] = type;
  }
  return out;
}

const posCols = parseSchemaTs(join(ROOT, 'clients/examples-web/src/lib/schema/positions.ts'));
const trdCols = parseSchemaTs(join(ROOT, 'clients/examples-web/src/lib/schema/trades.ts'));

// Positions: primary key is position_id (already first in TS list, but
// be explicit for safety). Drop the legacy positionKey path here.
const posSchema = buildSchema(posCols);

// Trades: primary key is trade_id.
const trdSchema = buildSchema(trdCols);

writeFileSync(
  join(ROOT, 'config/schemas/positions.json'),
  JSON.stringify(posSchema, null, 2) + '\n',
);
writeFileSync(
  join(ROOT, 'config/schemas/trades.json'),
  JSON.stringify(trdSchema, null, 2) + '\n',
);

console.log(`Wrote config/schemas/positions.json (${Object.keys(posSchema).length} columns)`);
console.log(`Wrote config/schemas/trades.json   (${Object.keys(trdSchema).length} columns)`);

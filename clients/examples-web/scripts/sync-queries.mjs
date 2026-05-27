// Mirror src/lib/queries/library.ts into scripts/queries.mjs so the
// standalone audit harness sees the same SQL the UI ships.
import { readFile, writeFile } from 'node:fs/promises';
const src = await readFile('../src/lib/queries/library.ts', 'utf8');
const m = src.match(/export const QUERIES: QueryEntry\[\] = (\[[\s\S]*?\n\];)/);
if (!m) { console.error('could not locate QUERIES array'); process.exit(1); }
// Cheap TS → JS strip: replace TS-only `as const`, trailing semicolons keep.
// Then evaluate via Function to get the array, then JSON-serialize.
// Have to pre-substitute template literal numeric refs (ONE_DAY_US etc).
let body = m[1]
  .replace(/\$\{ONE_DAY_US\}/g, '86400000000')
  .replace(/\$\{ONE_HOUR_US\}/g, '3600000000');
const arr = Function(`return ${body}`)();
const out = `export const QUERIES = ${JSON.stringify(arr.map(q => ({
  id: q.id, feature: q.feature, sql: q.sql, title: q.title,
})), null, 2)};\n`;
await writeFile('queries.mjs', out);
console.log(`wrote ${arr.length} queries`);

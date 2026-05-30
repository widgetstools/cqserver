// Exercise every query in the Query Library against the running
// cqserver and report which ones succeed.
//
// Runs each query twice:
//   1. Static one-shot SOW (`sow(topic, {sql})`) — the JOIN path.
//   2. Live subscription (`sowAndSubscribe(topic, {sql})`) — the
//      live-tick path used by the demo when the query has no JOIN.
//
// Both modes are reported so we can tell which queries work where.
// Usage:
//   node scripts/test-query-library.mjs [ws://host:port]

import { Client } from '../../../client-sdks/ts/dist/index.js';
import {
  detectTopic,
  isStaticQuery,
  stripAliases,
} from './query-router-shared.mjs';
import { QUERIES } from './queries.mjs';

const WS_URL = process.argv[2] ?? 'ws://127.0.0.1:9008/cq/json';
const ROW_LIMIT_PREVIEW = 3;
// Per-query budget. JOINs against the full trades topic (~3000 rows)
// and any wide live snapshots may need more than the 5s the harness
// originally used; 15s leaves headroom while still surfacing real
// hangs (anything beyond is a server-side stall worth investigating).
const PER_QUERY_TIMEOUT_MS = 15000;

const COLORS = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
};

function truncate(s, n) {
  const t = String(s ?? '');
  return t.length > n ? `${t.slice(0, n - 1)}…` : t;
}

function withTimeout(promise, ms, label) {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve({ ok: false, err: `timeout after ${ms}ms (${label})` }), ms);
    promise.then(
      (val) => { clearTimeout(t); resolve({ ok: true, val }); },
      (err) => { clearTimeout(t); resolve({ ok: false, err: String(err?.message ?? err) }); },
    );
  });
}

async function runStatic(client, q) {
  const topic = detectTopic(q.sql);
  if (!topic) return { ok: false, err: 'unknown topic in FROM' };
  const wireSql = stripAliases(q.sql);
  const r = await withTimeout(client.sow(topic, { sql: wireSql }), PER_QUERY_TIMEOUT_MS, 'sow');
  if (!r.ok) return r;
  return { ok: true, rows: r.val.length, preview: r.val.slice(0, ROW_LIMIT_PREVIEW) };
}

async function runLive(client, q) {
  const topic = detectTopic(q.sql);
  if (!topic) return { ok: false, err: 'unknown topic in FROM' };
  const wireSql = stripAliases(q.sql);
  const subP = client.sowAndSubscribe(topic, { sql: wireSql });
  const subR = await withTimeout(subP, PER_QUERY_TIMEOUT_MS, 'sowAndSubscribe');
  if (!subR.ok) return subR;
  const sub = subR.val;
  // Drain the snapshot.
  const snapDone = withTimeout(sub.whenSnapshotComplete(), PER_QUERY_TIMEOUT_MS, 'snapshot');
  // Concurrently collect deltas until snapshot signals end.
  let rows = 0;
  const collector = (async () => {
    for await (const delta of sub) {
      if (sub.isSnapshotComplete) {
        // Snapshot ended; stop after a brief drain to count any
        // trailing in-flight rows.
        break;
      }
      if (delta.deltaType !== 'remove' && delta.deltaType !== 'oof') {
        rows++;
      }
    }
  })();
  const sR = await snapDone;
  // Give the for-await loop a moment to exit on isSnapshotComplete.
  await Promise.race([collector, new Promise((r) => setTimeout(r, 50))]);
  await client.unsubscribe(sub.subId).catch(() => {});
  if (!sR.ok) return sR;
  return { ok: true, rows };
}

const main = async () => {
  console.log(`Connecting to ${WS_URL}…`);
  const client = await Client.connect(WS_URL);
  console.log('Connected. Running query library…\n');

  const results = [];
  for (const q of QUERIES) {
    const staticish = isStaticQuery(q.sql);
    // The query builder routes JOIN and derived-table queries to
    // static; everyone else to live. Test each the way the builder
    // would, so a PASS here means the demo grid actually shows rows.
    const r = staticish
      ? await runStatic(client, q)
      : await runLive(client, q);
    results.push({ q, mode: staticish ? 'static' : 'live', r });

    const tag = r.ok ? `${COLORS.green}PASS${COLORS.reset}` : `${COLORS.red}FAIL${COLORS.reset}`;
    const detail = r.ok
      ? `${COLORS.dim}${r.rows} rows${COLORS.reset}`
      : `${COLORS.yellow}${truncate(r.err, 80)}${COLORS.reset}`;
    console.log(`  [${tag}] ${q.id.padEnd(6)} ${q.feature.padEnd(7)} ${(staticish ? 'static' : 'live').padEnd(6)} ${q.title.padEnd(40)} ${detail}`);
  }

  console.log('\n══════════════════════════════════════════════════════════════════════');
  console.log('SUMMARY by feature:');
  const byFeature = {};
  for (const { q, r } of results) {
    if (!byFeature[q.feature]) byFeature[q.feature] = { pass: 0, fail: 0 };
    byFeature[q.feature][r.ok ? 'pass' : 'fail']++;
  }
  for (const [f, c] of Object.entries(byFeature)) {
    const pct = ((c.pass / (c.pass + c.fail)) * 100).toFixed(0);
    console.log(`  ${f.padEnd(8)} ${c.pass} pass, ${c.fail} fail  (${pct}%)`);
  }
  const totalPass = results.filter((x) => x.r.ok).length;
  console.log(`\n  TOTAL    ${totalPass} / ${results.length} pass`);

  console.log('\n══════════════════════════════════════════════════════════════════════');
  console.log('FAILURES grouped by cause:');
  const causes = new Map();
  for (const { q, r } of results.filter((x) => !x.r.ok)) {
    const key = (r.err || '').replace(/[0-9]+/g, 'N').replace(/'[^']+'/g, "'…'").slice(0, 60);
    if (!causes.has(key)) causes.set(key, []);
    causes.get(key).push(q.id);
  }
  for (const [cause, ids] of [...causes.entries()].sort((a, b) => b[1].length - a[1].length)) {
    console.log(`  ${COLORS.yellow}${cause}${COLORS.reset}`);
    console.log(`    ${COLORS.dim}${ids.join(', ')}${COLORS.reset}`);
  }

  await new Promise((r) => setTimeout(r, 200));
  process.exit(0);
};

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});

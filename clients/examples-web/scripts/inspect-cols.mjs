import { Client } from '../../../client-sdks/ts/dist/index.js';
const c = await Client.connect('ws://127.0.0.1:9008/cq/json');
// SELECT * to see what columns exist in /securities
try {
  const sub = await c.sowAndSubscribe('/securities', { sql: 'SELECT * FROM securities' });
  await sub.whenSnapshotComplete();
  console.log('securities snapshot complete; subId:', sub.subId);
  // Try to list cols via a SOW too
  const r = await c.sow('/securities', { sql: 'SELECT * FROM securities LIMIT 1' });
  console.log('securities sow rows:', r.length, 'cols:', r[0] ? Object.keys(r[0]).join(',') : 'none');
  await c.unsubscribe(sub.subId);
} catch (e) { console.log('ERR:', e.message); }

// Now try a JOIN against just one securities column we know exists
try {
  const r = await c.sow('/trades', { sql: 'SELECT trade_id, cusip FROM trades JOIN securities USING (cusip)' });
  console.log(`jn-3 minimal: ${r.length} rows`);
} catch (e) { console.log('jn-3 minimal ERR:', e.message); }

process.exit(0);

// CQServer Node.js SDK quickstart.
//
// Run a server first, e.g.:
//     target/release/cqserver --config clients/sdk-smoke.toml
//
// Then:
//     node client-sdks/js/examples/quickstart.js [host] [port]

import { Client } from '../src/index.js';

const HOST = process.argv[2] || '127.0.0.1';
const PORT = process.argv[3] ? parseInt(process.argv[3], 10) : 9099;
const TOPIC = '/sdk-test';

async function main() {
  const c = await Client.connect(HOST, PORT);
  try {
    const version = await c.logon(); // anonymous handshake
    console.log(`connected, negotiated protocol version ${version}`);

    let seq = await c.publish(TOPIC, { k: 'AAA', v: 1 });
    console.log(`published AAA -> seq ${seq}`);

    // Open a live subscription (snapshot + deltas) before the next write.
    const sub = c.sowAndSubscribe(TOPIC);

    // Drain the snapshot (one row so far).
    let snap = 0;
    while (true) {
      const d = await sub.nextDelta(500);
      if (d === null) break;
      if (d.deltaType === 'sow') {
        snap += 1;
        console.log(`  snapshot row: ${JSON.stringify(d.data)}`);
      } else {
        break;
      }
    }
    console.log(`snapshot delivered ${snap} row(s)`);

    // A live publish should arrive on the subscription as an add/update.
    await c.publish(TOPIC, { k: 'BBB', v: 2 });
    const d = await sub.nextDelta(2000);
    console.log(
      `live delta: type=${d ? d.deltaType : null} data=${d ? JSON.stringify(d.data) : null}`,
    );

    // One-shot SOW shows both rows.
    const rows = await c.sow(TOPIC);
    const keys = rows.map((r) => r.k).sort();
    console.log(`sow returned ${rows.length} rows: ${JSON.stringify(keys)}`);

    c.unsubscribe(sub);
  } finally {
    c.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

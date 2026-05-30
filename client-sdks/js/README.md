# cqserver-client (Node.js)

A dependency-free Node.js client SDK for **CQServer**, a pub/sub database.

It speaks the native wire protocol over plain TCP: each frame is
`[u32 big-endian length][JSON body]` (top bit of the length flags zstd
compression, which this SDK never negotiates). Messages are JSON envelopes with
short keys (`c` command, `cid`, `t` topic, `sid` subscription id, `d` data, ...).

## Usage

```js
import { Client } from 'cqserver-client';

const c = await Client.connect('127.0.0.1', 9099);
await c.logon();                                  // anonymous handshake
await c.publish('/sdk-test', { k: 'AAA', v: 1 }); // upsert
const sub = c.sowAndSubscribe('/sdk-test');       // snapshot + live deltas
for await (const d of sub) {
  console.log(d.deltaType, d.data); // "sow" rows first, then live changes
}
const rows = await c.sow('/sdk-test');            // one-shot snapshot
```

`Subscription` exposes both `await sub.nextDelta(timeoutMs)` (returns `null` on
timeout) and async iteration (`for await (const d of sub)`). Each delta has the
shape `{ deltaType, subId, data, sequence }`.

## Run the example

Start a server first:

```sh
target/release/cqserver --config clients/sdk-smoke.toml
```

Then run the quickstart (requires Node >= 18):

```sh
node examples/quickstart.js          # defaults to 127.0.0.1:9099
node examples/quickstart.js HOST PORT
```

(or `npm run example` from this directory)

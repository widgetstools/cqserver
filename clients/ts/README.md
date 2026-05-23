# @cqserver/client

Async TypeScript/JavaScript client SDK for cqserver. Mirrors the Rust + Python SDKs.

## Quickstart (Node TCP)

```ts
import { Client } from '@cqserver/client';

const c = await Client.connect('tcp://127.0.0.1:9007');
await c.logon('alice', 's3cret');

const seq = await c.publish('/market-data', { symbol: 'AAPL', price: 150 });

const sub = await c.sowAndSubscribe('/market-data', { filter: 'price > 100' });
for await (const delta of sub) {
  console.log(delta.deltaType, delta.data);
}
```

## Browser (WebSocket)

```ts
const c = await Client.connect('ws://localhost:9008/cq/json');
```

TCP requires Node (uses `node:net`); WebSocket works in both Node 22+ (native `WebSocket`) and any browser.

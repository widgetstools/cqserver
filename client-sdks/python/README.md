# cqclient (Python)

Async client SDK for cqserver. Mirrors the API of `cq-client` (Rust).

## Quickstart

```python
import asyncio
from cqclient import Client

async def main():
    client = await Client.connect("tcp://127.0.0.1:9007")
    await client.logon("alice", "s3cret")

    seq = await client.publish("/market-data", {"symbol": "AAPL", "price": 150.0})
    print(f"published at seq={seq}")

    sub = await client.sow_and_subscribe("/market-data", filter="price > 100")
    async for delta in sub:
        print(delta.delta_type, delta.data)

asyncio.run(main())
```

## Sync wrapper

```python
from cqclient import SyncClient

c = SyncClient.connect("tcp://127.0.0.1:9007")
c.publish("/market-data", {"symbol": "AAPL", "price": 150.0})
```

"""Synchronous wrapper around the async ``Client``.

Each call drives the underlying event loop until completion. Convenient
for scripts and tests; not recommended for high-throughput production.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from .client import Client, Delta, Subscription


class SyncClient:
    def __init__(self, async_client: Client, loop: asyncio.AbstractEventLoop):
        self._c = async_client
        self._loop = loop

    @classmethod
    def connect(cls, url: str) -> "SyncClient":
        loop = asyncio.new_event_loop()
        async_client = loop.run_until_complete(Client.connect(url))
        return cls(async_client, loop)

    def close(self) -> None:
        self._loop.run_until_complete(self._c.close())
        self._loop.close()

    def logon(self, user: str, password: str) -> None:
        self._loop.run_until_complete(self._c.logon(user, password))

    def publish(self, topic: str, data: Dict[str, Any]) -> int:
        return self._loop.run_until_complete(self._c.publish(topic, data))

    def sow(self, topic: str, filter: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._loop.run_until_complete(self._c.sow(topic, filter))

    def sow_delete(self, topic: str, key: str) -> int:
        return self._loop.run_until_complete(self._c.sow_delete(topic, key))

    def heartbeat(self) -> None:
        self._loop.run_until_complete(self._c.heartbeat())

    def subscribe(self, topic: str, filter: Optional[str] = None) -> "SyncSubscription":
        sub = self._loop.run_until_complete(self._c.subscribe(topic, filter))
        return SyncSubscription(sub, self._loop)

    def sow_and_subscribe(
        self,
        topic: str,
        filter: Optional[str] = None,
        bookmark: Optional[int] = None,
    ) -> "SyncSubscription":
        sub = self._loop.run_until_complete(
            self._c.sow_and_subscribe(topic, filter, bookmark)
        )
        return SyncSubscription(sub, self._loop)


class SyncSubscription:
    def __init__(self, sub: Subscription, loop: asyncio.AbstractEventLoop):
        self._sub = sub
        self._loop = loop

    @property
    def sub_id(self) -> str:
        return self._sub.sub_id

    def next_delta(self, timeout: Optional[float] = None) -> Optional[Delta]:
        if timeout is None:
            return self._loop.run_until_complete(self._sub.next_delta())
        return self._loop.run_until_complete(
            asyncio.wait_for(self._sub.next_delta(), timeout=timeout)
        )

    def last_sequence(self) -> int:
        return self._sub.last_sequence()

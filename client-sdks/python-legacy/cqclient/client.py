"""Async TCP client for cqserver.

Wire protocol: length-prefixed (u32 big-endian) JSON frames matching
the `CqMessage` shape used by the Rust server. Schemes supported:
- ``tcp://host:port``
- ``ws://host:port/path``  (requires the ``websockets`` package)
"""

from __future__ import annotations

import asyncio
import dataclasses
import itertools
import json
import struct
import urllib.parse
from typing import Any, AsyncIterator, Dict, List, Optional


class ClientError(Exception):
    """Raised when the server returns an error ack or the connection drops."""


class DeltaKind:
    ADD = "add"
    UPDATE = "update"
    REMOVE = "remove"
    OOF = "oof"


@dataclasses.dataclass
class Delta:
    delta_type: str
    sub_id: str
    sequence: Optional[int]
    data: Dict[str, Any]


class Subscription:
    """Iterable handle that yields deltas as they arrive."""

    def __init__(self, sub_id: str, queue: "asyncio.Queue[Optional[Delta]]"):
        self.sub_id = sub_id
        self._queue = queue
        self._last_seq = 0

    async def next_delta(self) -> Optional[Delta]:
        d = await self._queue.get()
        if d is None:
            return None
        if d.sequence is not None and d.sequence > self._last_seq:
            self._last_seq = d.sequence
        return d

    def last_sequence(self) -> int:
        return self._last_seq

    def __aiter__(self) -> AsyncIterator[Delta]:
        return self

    async def __anext__(self) -> Delta:
        d = await self.next_delta()
        if d is None:
            raise StopAsyncIteration
        return d


class Client:
    """Async cqserver client.

    Use ``Client.connect(url)`` (an awaitable factory) instead of the
    constructor directly.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self._reader = reader
        self._writer = writer
        self._cid = itertools.count(1)
        self._pending: Dict[str, asyncio.Future] = {}
        self._subs: Dict[str, asyncio.Queue] = {}
        self._snapshot_buffers: Dict[str, List[Dict[str, Any]]] = {}
        self._snapshot_completions: Dict[str, asyncio.Future] = {}
        self._closed = False
        self._reader_task = asyncio.create_task(self._read_loop())

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    @classmethod
    async def connect(cls, url: str) -> "Client":
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme == "tcp":
            reader, writer = await asyncio.open_connection(
                parsed.hostname, parsed.port
            )
            return cls(reader, writer)
        if parsed.scheme in ("ws", "wss"):
            raise NotImplementedError(
                "WebSocket transport requires the optional 'websockets' "
                "package; use tcp:// for now"
            )
        raise ClientError(f"unsupported scheme: {parsed.scheme!r}")

    async def close(self) -> None:
        self._closed = True
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except Exception:
            pass
        self._reader_task.cancel()
        # Wake every pending future.
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(ClientError("connection closed"))
        for q in list(self._subs.values()):
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def logon(self, user: str, password: str) -> None:
        await self._rpc(
            {"c": "logon", "d": {"user": user, "password": password}}
        )

    async def publish(self, topic: str, data: Dict[str, Any]) -> int:
        resp = await self._rpc({"c": "publish", "t": topic, "d": data})
        return int(resp.get("seq", 0))

    async def sow(
        self, topic: str, filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        cid = self._next_cid()
        self._snapshot_buffers[cid] = []
        loop = asyncio.get_running_loop()
        done = loop.create_future()
        self._snapshot_completions[cid] = done

        msg = {"c": "sow", "cid": cid, "t": topic}
        if filter is not None:
            msg["f"] = filter
        await self._send(msg)

        try:
            await done
        finally:
            rows = self._snapshot_buffers.pop(cid, [])
            self._snapshot_completions.pop(cid, None)
        return rows

    async def subscribe(
        self, topic: str, filter: Optional[str] = None
    ) -> Subscription:
        return await self._subscribe("subscribe", topic, filter, None)

    async def sow_and_subscribe(
        self,
        topic: str,
        filter: Optional[str] = None,
        bookmark: Optional[int] = None,
    ) -> Subscription:
        return await self._subscribe("sow_and_subscribe", topic, filter, bookmark)

    async def delta_subscribe(
        self, topic: str, filter: Optional[str] = None
    ) -> Subscription:
        return await self._subscribe("delta_subscribe", topic, filter, None)

    async def _subscribe(
        self,
        command: str,
        topic: str,
        filter: Optional[str],
        bookmark: Optional[int],
    ) -> Subscription:
        msg: Dict[str, Any] = {"c": command, "t": topic}
        if filter is not None:
            msg["f"] = filter
        if bookmark is not None:
            msg["bm"] = bookmark
        resp = await self._rpc(msg)
        sub_id = resp.get("sid")
        if not sub_id:
            raise ClientError("server did not assign a sub_id")
        q: asyncio.Queue = asyncio.Queue()
        self._subs[sub_id] = q
        return Subscription(sub_id, q)

    async def unsubscribe(self, sub_id: str) -> None:
        await self._rpc({"c": "unsubscribe", "sid": sub_id})
        self._subs.pop(sub_id, None)

    async def sow_delete(self, topic: str, key: str) -> int:
        resp = await self._rpc(
            {"c": "sow_delete", "t": topic, "d": {"key": key}}
        )
        return int(resp.get("seq", 0))

    async def heartbeat(self) -> None:
        await self._rpc({"c": "heartbeat"})

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _next_cid(self) -> str:
        return f"c-{next(self._cid)}"

    async def _rpc(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        cid = self._next_cid()
        msg["cid"] = cid
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[cid] = fut
        await self._send(msg)
        try:
            resp = await fut
        finally:
            self._pending.pop(cid, None)
        if resp.get("s") == "error":
            raise ClientError(resp.get("r", "server error"))
        return resp

    async def _send(self, msg: Dict[str, Any]) -> None:
        body = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        self._writer.write(struct.pack(">I", len(body)) + body)
        await self._writer.drain()

    async def _read_loop(self) -> None:
        try:
            while not self._closed:
                hdr = await self._reader.readexactly(4)
                (length,) = struct.unpack(">I", hdr)
                body = await self._reader.readexactly(length)
                msg = json.loads(body)
                self._dispatch(msg)
        except asyncio.IncompleteReadError:
            pass
        except asyncio.CancelledError:
            pass
        finally:
            self._closed = True

    def _dispatch(self, msg: Dict[str, Any]) -> None:
        cmd = msg.get("c")
        cid = msg.get("cid")
        sid = msg.get("sid")
        if cmd == "ack":
            if cid and cid in self._pending:
                fut = self._pending.get(cid)
                if fut and not fut.done():
                    fut.set_result(msg)
            return
        if cmd == "sow" and sid and sid in self._snapshot_buffers:
            data = msg.get("d") or {}
            if isinstance(data, dict):
                self._snapshot_buffers[sid].append(data)
            return
        if cmd == "group_end" and sid and sid in self._snapshot_completions:
            fut = self._snapshot_completions[sid]
            if not fut.done():
                fut.set_result(None)
            return
        if cmd == "group_begin":
            return
        if cmd == "publish" and sid:
            q = self._subs.get(sid)
            if q is not None:
                q.put_nowait(
                    Delta(
                        delta_type=msg.get("dt", "update"),
                        sub_id=sid,
                        sequence=msg.get("seq"),
                        data=msg.get("d") or {},
                    )
                )
            return
        if cmd == "heartbeat":
            return
        # Anything else is informational — silently drop.

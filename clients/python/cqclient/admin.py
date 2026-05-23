"""Async client for the cqserver admin HTTP API."""
from __future__ import annotations

import asyncio
import json
import urllib.parse
from typing import Any


class AdminClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @classmethod
    def from_url(cls, url: str) -> "AdminClient":
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", ""):
            raise ValueError(f"unsupported scheme {parsed.scheme!r}")
        return cls(parsed.hostname or "127.0.0.1", parsed.port or 8085)

    async def healthz(self) -> str:
        return (await self._get("/healthz")).decode("utf-8")

    async def stats(self) -> dict:
        return json.loads(await self._get("/stats"))

    async def topics(self) -> Any:
        return json.loads(await self._get("/topics"))

    async def metrics(self) -> str:
        return (await self._get("/metrics")).decode("utf-8")

    async def _get(self, path: str) -> bytes:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("ascii")
        writer.write(req)
        await writer.drain()
        raw = await reader.read()
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        head_end = raw.find(b"\r\n\r\n")
        if head_end < 0:
            raise RuntimeError("malformed HTTP response")
        first_line = raw[: raw.find(b"\r\n")].decode("ascii", errors="replace")
        if " 200 " not in first_line:
            raise RuntimeError(f"admin GET {path}: {first_line}")
        return raw[head_end + 4 :]

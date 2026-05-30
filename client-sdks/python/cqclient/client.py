"""CQServer client over TCP (length-prefixed JSON frames)."""

from __future__ import annotations

import json
import queue
import socket
import struct
import threading
import itertools
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Top bit of the u32 length prefix flags a zstd-compressed body. This SDK
# advertises no compression, so the server never sets it; an inbound frame
# with the flag set is therefore a protocol error.
_COMPRESSED_FLAG = 0x80000000
_MAX_FRAME = 16 * 1024 * 1024

# Wire protocol versions this SDK understands (mirrors
# cq_protocol::version::SUPPORTED_VERSIONS — kept conservative).
SUPPORTED_VERSIONS = [1]


class CqError(Exception):
    """Raised for server-reported errors and protocol violations."""


@dataclass
class Delta:
    """A single subscription update (snapshot row or live change)."""

    delta_type: str  # "sow" (snapshot), "add", "update", "remove", ...
    sub_id: str
    data: Dict[str, Any]
    sequence: Optional[int] = None


@dataclass
class _Pending:
    event: threading.Event = field(default_factory=threading.Event)
    response: Optional[Dict[str, Any]] = None


class Subscription:
    """Delivers snapshot rows then live deltas via a thread-safe queue."""

    def __init__(self, sub_id: str, cid: str):
        self.sub_id = sub_id
        self.cid = cid
        self._q: "queue.Queue[Delta]" = queue.Queue()

    def _push(self, delta: Delta) -> None:
        self._q.put(delta)

    def next_delta(self, timeout: Optional[float] = None) -> Optional[Delta]:
        """Block for the next delta. Returns None on timeout."""
        try:
            return self._q.get(timeout=timeout)
        except queue.Empty:
            return None

    def __iter__(self):
        while True:
            yield self._q.get()


class Client:
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._send_lock = threading.Lock()
        self._cids = itertools.count(1)
        self._protocol_version = 1

        # cid -> _Pending (RPC waiters: publish, logon, sow setup)
        self._pending: Dict[str, _Pending] = {}
        # cid -> Subscription, before the server assigns a sid (promoted on ack)
        self._pending_subs: Dict[str, Subscription] = {}
        # sid -> Subscription (live routing)
        self._subs: Dict[str, Subscription] = {}
        # cid -> list of snapshot rows (one-shot sow)
        self._snapshot_buffers: Dict[str, List[Dict[str, Any]]] = {}
        # cid -> _Pending whose .response carries an optional error string
        self._snapshot_done: Dict[str, _Pending] = {}
        self._lock = threading.Lock()

        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()

    # ---- connection -------------------------------------------------

    @classmethod
    def connect(cls, host: str, port: int, timeout: float = 5.0) -> "Client":
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.settimeout(None)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(sock)

    @classmethod
    def connect_url(cls, url: str, timeout: float = 5.0) -> "Client":
        """Accepts `tcp://host:port`."""
        if not url.startswith("tcp://"):
            raise CqError(f"unsupported url (tcp:// only): {url}")
        hostport = url[len("tcp://"):]
        host, _, port = hostport.rpartition(":")
        return cls.connect(host, int(port), timeout)

    def close(self) -> None:
        self._closed = True
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- framing ----------------------------------------------------

    def _send(self, msg: Dict[str, Any]) -> None:
        body = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        if len(body) > _MAX_FRAME:
            raise CqError(f"frame too large: {len(body)} bytes")
        header = struct.pack(">I", len(body))
        with self._send_lock:
            self._sock.sendall(header + body)

    def _recv_exact(self, n: int) -> Optional[bytes]:
        chunks = []
        got = 0
        while got < n:
            b = self._sock.recv(n - got)
            if not b:
                return None
            chunks.append(b)
            got += len(b)
        return b"".join(chunks)

    def _read_loop(self) -> None:
        try:
            while True:
                header = self._recv_exact(4)
                if header is None:
                    break
                raw = struct.unpack(">I", header)[0]
                if raw & _COMPRESSED_FLAG:
                    raise CqError("received compressed frame but no codec negotiated")
                length = raw & ~_COMPRESSED_FLAG
                if length > _MAX_FRAME:
                    raise CqError(f"frame too large: {length}")
                payload = self._recv_exact(length)
                if payload is None:
                    break
                msg = json.loads(payload.decode("utf-8"))
                self._dispatch(msg)
        except (OSError, CqError):
            pass
        finally:
            # Wake any blocked waiters so they don't hang forever.
            self._fail_all_waiters()

    def _fail_all_waiters(self) -> None:
        with self._lock:
            for p in self._pending.values():
                p.event.set()
            for p in self._snapshot_done.values():
                p.response = "connection closed"
                p.event.set()

    # ---- dispatch ---------------------------------------------------

    def _dispatch(self, msg: Dict[str, Any]) -> None:
        cmd = msg.get("c")
        if cmd == "ack":
            self._on_ack(msg)
        elif cmd == "sow":
            self._on_sow_row(msg)
        elif cmd == "sow_batch":
            self._on_sow_batch(msg)
        elif cmd == "group_end":
            self._on_group_end(msg)
        elif cmd == "publish":
            self._on_delta(msg)
        # group_begin / heartbeat / schema_change: nothing to do here.

    def _on_ack(self, msg: Dict[str, Any]) -> None:
        cid = msg.get("cid")
        sid = msg.get("sid")
        # Promote a pre-registered subscription into the live routing map.
        if cid is not None and sid is not None:
            with self._lock:
                sub = self._pending_subs.pop(cid, None)
                if sub is not None:
                    sub.sub_id = sid
                    self._subs[sid] = sub
        if cid is not None:
            # Error ack for an in-flight SOW: no group_end will arrive.
            if msg.get("s") == "error":
                with self._lock:
                    p = self._snapshot_done.pop(cid, None)
                if p is not None:
                    p.response = msg.get("r") or "server error"
                    p.event.set()
            with self._lock:
                p = self._pending.pop(cid, None)
            if p is not None:
                p.response = msg
                p.event.set()

    def _on_sow_row(self, msg: Dict[str, Any]) -> None:
        sid = msg.get("sid")
        data = msg.get("d")
        if sid is None or not isinstance(data, dict):
            return
        with self._lock:
            buf = self._snapshot_buffers.get(sid)
            sub = self._subs.get(sid)
        if buf is not None:
            buf.append(data)
        if sub is not None:
            sub._push(Delta("sow", sid, data, msg.get("seq")))

    def _on_sow_batch(self, msg: Dict[str, Any]) -> None:
        sid = msg.get("sid")
        rows = msg.get("d")
        if sid is None or not isinstance(rows, list):
            return
        with self._lock:
            buf = self._snapshot_buffers.get(sid)
            sub = self._subs.get(sid)
        for row in rows:
            if not isinstance(row, dict):
                continue
            if buf is not None:
                buf.append(row)
            if sub is not None:
                sub._push(Delta("sow", sid, row, None))

    def _on_group_end(self, msg: Dict[str, Any]) -> None:
        sid = msg.get("sid")
        if sid is None:
            return
        with self._lock:
            p = self._snapshot_done.pop(sid, None)
        if p is not None:
            p.response = None  # success
            p.event.set()

    def _on_delta(self, msg: Dict[str, Any]) -> None:
        sid = msg.get("sid")
        if sid is None:
            return
        with self._lock:
            sub = self._subs.get(sid)
        if sub is None:
            return
        data = msg.get("d")
        sub._push(
            Delta(
                delta_type=msg.get("dt") or "update",
                sub_id=sid,
                data=data if isinstance(data, dict) else {},
                sequence=msg.get("seq"),
            )
        )

    # ---- helpers ----------------------------------------------------

    def _next_cid(self) -> str:
        return str(next(self._cids))

    def _rpc(self, msg: Dict[str, Any], timeout: float = 5.0) -> Dict[str, Any]:
        cid = msg.get("cid") or self._next_cid()
        msg["cid"] = cid
        p = _Pending()
        with self._lock:
            self._pending[cid] = p
        self._send(msg)
        if not p.event.wait(timeout):
            with self._lock:
                self._pending.pop(cid, None)
            raise CqError("timed out waiting for ack")
        resp = p.response
        if resp is None:
            raise CqError("connection closed")
        if resp.get("s") == "error":
            raise CqError(resp.get("r") or "server error")
        return resp

    # ---- API --------------------------------------------------------

    @property
    def protocol_version(self) -> int:
        return self._protocol_version

    def logon(
        self,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        client_name: Optional[str] = None,
    ) -> int:
        """Authenticate and negotiate the protocol version.

        With no credentials this is an anonymous handshake (works when the
        server has auth disabled). Returns the negotiated protocol version.
        """
        msg: Dict[str, Any] = {"c": "logon", "pv": SUPPORTED_VERSIONS}
        creds: Dict[str, Any] = {}
        if token is not None:
            creds["token"] = token
        elif user is not None:
            creds["user"] = user
            creds["password"] = password or ""
        if creds:
            msg["d"] = creds
        if client_name is not None:
            msg["client"] = client_name
        resp = self._rpc(msg)
        pv = resp.get("pv")
        if isinstance(pv, list) and pv:
            self._protocol_version = pv[0]
        return self._protocol_version

    def publish(self, topic: str, data: Dict[str, Any], timeout: float = 5.0) -> int:
        """Publish/upsert a record. Returns the assigned sequence."""
        resp = self._rpc({"c": "publish", "t": topic, "d": data}, timeout)
        return int(resp.get("seq") or 0)

    def delta_publish(self, topic: str, data: Dict[str, Any], timeout: float = 5.0) -> int:
        """Sparse update: merge `data` (key + changed fields) into the row."""
        resp = self._rpc({"c": "delta_publish", "t": topic, "d": data}, timeout)
        return int(resp.get("seq") or 0)

    def sow(
        self,
        topic: str,
        filter: Optional[str] = None,
        timeout: float = 10.0,
    ) -> List[Dict[str, Any]]:
        """One-shot SOW query. Returns the snapshot rows."""
        cid = self._next_cid()
        done = _Pending()
        with self._lock:
            self._snapshot_buffers[cid] = []
            self._snapshot_done[cid] = done
        msg: Dict[str, Any] = {"c": "sow", "cid": cid, "t": topic}
        if filter is not None:
            msg["f"] = filter
        self._send(msg)
        if not done.event.wait(timeout):
            with self._lock:
                self._snapshot_buffers.pop(cid, None)
                self._snapshot_done.pop(cid, None)
            raise CqError("timed out waiting for SOW group_end")
        with self._lock:
            rows = self._snapshot_buffers.pop(cid, [])
        if done.response is not None:
            raise CqError(done.response)
        return rows

    def sow_sql(self, topic: str, sql: str, timeout: float = 10.0) -> List[Dict[str, Any]]:
        """Run an arbitrary SOW SQL query (GROUP BY / aggregates)."""
        cid = self._next_cid()
        done = _Pending()
        with self._lock:
            self._snapshot_buffers[cid] = []
            self._snapshot_done[cid] = done
        self._send({"c": "sow", "cid": cid, "t": topic, "sql": sql})
        if not done.event.wait(timeout):
            with self._lock:
                self._snapshot_buffers.pop(cid, None)
                self._snapshot_done.pop(cid, None)
            raise CqError("timed out waiting for SOW group_end")
        with self._lock:
            rows = self._snapshot_buffers.pop(cid, [])
        if done.response is not None:
            raise CqError(done.response)
        return rows

    def sow_and_subscribe(
        self,
        topic: str,
        filter: Optional[str] = None,
        bookmark: Optional[int] = None,
    ) -> Subscription:
        """Snapshot + continuous deltas. Snapshot rows arrive as deltas with
        delta_type == "sow"; live changes as "add"/"update"/"remove"."""
        cid = self._next_cid()
        sub = Subscription(sub_id="", cid=cid)
        with self._lock:
            self._pending_subs[cid] = sub
        msg: Dict[str, Any] = {"c": "sow_and_subscribe", "cid": cid, "t": topic}
        if filter is not None:
            msg["f"] = filter
        if bookmark is not None:
            msg["bm"] = bookmark
        self._send(msg)
        return sub

    def subscribe(
        self,
        topic: str,
        filter: Optional[str] = None,
    ) -> Subscription:
        """Live deltas only (no initial snapshot)."""
        cid = self._next_cid()
        sub = Subscription(sub_id="", cid=cid)
        with self._lock:
            self._pending_subs[cid] = sub
        msg: Dict[str, Any] = {"c": "subscribe", "cid": cid, "t": topic}
        if filter is not None:
            msg["f"] = filter
        self._send(msg)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        if sub.sub_id:
            self._send({"c": "unsubscribe", "sid": sub.sub_id})
            with self._lock:
                self._subs.pop(sub.sub_id, None)

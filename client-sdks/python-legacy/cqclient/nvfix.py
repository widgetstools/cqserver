"""NVFIX (name=value\\x01...) payload helpers.

Mirror of ``cq-protocol::nvfix`` for Python clients that want to use the
flat tag=value wire shape as the payload of a publish.
"""
from __future__ import annotations

from typing import Any, Dict

SOH = b"\x01"


class NvFixError(ValueError):
    pass


def encode(d: Dict[str, Any]) -> bytes:
    out = bytearray()
    for k, v in d.items():
        if not isinstance(k, str) or "=" in k or "\x01" in k or not k:
            raise NvFixError(f"illegal field name: {k!r}")
        if isinstance(v, (dict, list)):
            raise NvFixError("nested values not allowed in NVFIX")
        if v is None:
            sval = ""
        elif isinstance(v, bool):
            sval = "true" if v else "false"
        elif isinstance(v, (int, float)):
            sval = str(v)
        elif isinstance(v, str):
            if "\x01" in v:
                raise NvFixError("value contains SOH")
            sval = v
        else:
            sval = str(v)
        out.extend(k.encode("utf-8"))
        out.append(ord("="))
        out.extend(sval.encode("utf-8"))
        out.extend(SOH)
    return bytes(out)


def decode(data: bytes) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for field in data.split(SOH):
        if not field:
            continue
        eq = field.find(b"=")
        if eq < 0:
            raise NvFixError("field without `=`")
        name = field[:eq].decode("utf-8")
        value = field[eq + 1 :].decode("utf-8")
        out[name] = value
    return out


def decode_typed(data: bytes) -> Dict[str, Any]:
    raw = decode(data)
    out: Dict[str, Any] = {}
    for k, s in raw.items():
        try:
            out[k] = int(s)
            continue
        except ValueError:
            pass
        try:
            out[k] = float(s)
            continue
        except ValueError:
            pass
        if s == "true":
            out[k] = True
        elif s == "false":
            out[k] = False
        else:
            out[k] = s
    return out

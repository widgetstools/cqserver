"""CQServer Python SDK.

A minimal, dependency-free client speaking the CQServer wire protocol over
TCP: length-prefixed JSON frames, anonymous/credentialed logon with
protocol-version negotiation, publish, one-shot SOW, and
sow-and-subscribe (snapshot + live deltas).

Wire format mirrors the Rust reference client (`crates/cq-client`):
  frame  = [u32 BE length][JSON body]
  (the top length bit is a zstd-compressed flag; this SDK never sets it
   and refuses inbound compressed frames since it advertises no codec)
"""

from .client import Client, Subscription, Delta, CqError

__all__ = ["Client", "Subscription", "Delta", "CqError"]

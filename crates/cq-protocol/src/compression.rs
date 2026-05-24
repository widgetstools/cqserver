//! S27 — wire-level compression negotiation.
//!
//! Each connection negotiates one compression algorithm at Logon time
//! (same handshake shape as protocol-version negotiation, see
//! [`crate::version`]):
//!
//!   - Client sends `compressions = ["none", "zstd"]` listing every
//!     algorithm it can speak, in preference order (most-preferred
//!     first).
//!   - Server picks the first entry that's also in its own support
//!     list — preferring the *client's* order so the client can
//!     express a preference (e.g., "zstd if you can, else none").
//!   - The ack echoes the chosen value; both sides store it on the
//!     session.
//!
//! The frame codec consults the session's compression mode at
//! encode + decode time. When compression is active AND the payload
//! is at least [`MIN_COMPRESSED_PAYLOAD_BYTES`], the body is zstd-
//! compressed and the high bit of the length prefix is set.
//!
//! ### Backwards compatibility
//!
//! Older clients that don't negotiate get `Compression::None` and
//! the wire format is unchanged — no length-prefix-bit twiddling.
//! Only sessions that explicitly negotiate a non-None compression
//! see the new bit pattern.

use serde::{Deserialize, Serialize};

/// Single wire-compression algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// No compression — the legacy default.
    None,
    /// zstd. Per-frame independent compression at the default level.
    Zstd,
}

impl Compression {
    /// Stable wire token. Used by the negotiation field on
    /// `CqMessage` so old + new code can round-trip the value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Zstd => "zstd",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Compression::None),
            "zstd" => Some(Compression::Zstd),
            _ => None,
        }
    }
}

/// Compression algorithms this build can speak. Listed in negotiation
/// preference order — when a client offers `["zstd", "none"]` the
/// server picks `zstd` if it's in this list; if not, falls through
/// to `none`.
pub const SUPPORTED_COMPRESSIONS: &[Compression] =
    &[Compression::Zstd, Compression::None];

/// Default compression assumed when a Logon arrives without an
/// explicit `compressions` field — preserves compatibility with
/// pre-S27 clients.
pub const DEFAULT_LEGACY_COMPRESSION: Compression = Compression::None;

/// Minimum payload size, in bytes, at which the codec compresses.
/// Below this threshold the overhead (zstd header + small-input
/// inefficiency) outweighs any size win, so the frame stays raw.
pub const MIN_COMPRESSED_PAYLOAD_BYTES: usize = 256;

/// Top bit of the wire length-prefix. Set by the encoder when a frame
/// is compressed so the decoder knows to decompress before parsing.
/// `MAX_FRAME_SIZE` is 16 MiB << 2^31, so this bit is always free.
pub const COMPRESSED_FLAG: u32 = 1 << 31;

/// Result of a Logon-time compression negotiation. Mirrors the version
/// module's `NegotiationOutcome` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationOutcome {
    /// Both sides share at least one algorithm; the client's most-
    /// preferred mutual algorithm is selected.
    Negotiated(Compression),
    /// Disjoint sets. This can only happen if both sides explicitly
    /// rule out `none` — otherwise the legacy default applies. Treated
    /// as an error so the client fails fast.
    NoOverlap,
}

/// Negotiate the active compression algorithm given the client's
/// advertised list (in preference order) and the server's supported
/// list. Returns the first client choice the server can speak.
///
/// An empty `client_compressions` is treated as legacy
/// (`Compression::None`).
pub fn negotiate(
    client_compressions: &[Compression],
    server_compressions: &[Compression],
) -> NegotiationOutcome {
    if client_compressions.is_empty() {
        if server_compressions.contains(&DEFAULT_LEGACY_COMPRESSION) {
            return NegotiationOutcome::Negotiated(DEFAULT_LEGACY_COMPRESSION);
        }
        return NegotiationOutcome::NoOverlap;
    }
    for &c in client_compressions {
        if server_compressions.contains(&c) {
            return NegotiationOutcome::Negotiated(c);
        }
    }
    NegotiationOutcome::NoOverlap
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_client_preferred_when_server_supports_it() {
        // Client prefers zstd; server supports both.
        let client = vec![Compression::Zstd, Compression::None];
        assert_eq!(
            negotiate(client.as_slice(), SUPPORTED_COMPRESSIONS),
            NegotiationOutcome::Negotiated(Compression::Zstd)
        );
    }

    #[test]
    fn picks_none_when_client_prefers_none() {
        let client = vec![Compression::None, Compression::Zstd];
        assert_eq!(
            negotiate(client.as_slice(), SUPPORTED_COMPRESSIONS),
            NegotiationOutcome::Negotiated(Compression::None)
        );
    }

    #[test]
    fn empty_client_list_implies_legacy_none() {
        assert_eq!(
            negotiate(&[], SUPPORTED_COMPRESSIONS),
            NegotiationOutcome::Negotiated(Compression::None)
        );
    }

    #[test]
    fn from_str_round_trips() {
        assert_eq!(Compression::from_str("zstd"), Some(Compression::Zstd));
        assert_eq!(Compression::from_str("none"), Some(Compression::None));
        assert_eq!(Compression::from_str("snappy"), None);
        for c in [Compression::None, Compression::Zstd] {
            assert_eq!(Compression::from_str(c.as_str()), Some(c));
        }
    }
}

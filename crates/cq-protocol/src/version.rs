//! S28 — wire-protocol version negotiation.
//!
//! The CQ wire protocol evolves over time (new frame fields, new
//! commands). To let clients of different vintages coexist, every
//! connection negotiates a single active protocol version on `Logon`:
//!
//!   - Client sends `protocol_versions = [V_c1, V_c2, ...]` listing
//!     every version it can speak (typically a contiguous range up
//!     to the client's compile-time max).
//!   - Server intersects with [`SUPPORTED_VERSIONS`] and picks
//!     the highest mutually supported version.
//!   - The ack carries `protocol_versions = [Vn]` so the client knows
//!     which version actually went live.
//!   - Both sides store `Vn` on the session for the lifetime of the
//!     connection and use it to gate version-specific features.
//!
//! Older clients/servers that don't send `protocol_versions` default
//! to version 1 — preserving compatibility with anything that
//! predates this field.

/// Every wire-protocol version the server can speak. Listed lowest
/// first; `negotiate` walks from the high end of the intersection.
///
/// Version semantics:
///   - **V1**: original frame format (everything before S28 / this
///     module). All optional fields default unset; no version
///     gating.
///   - **V2**: this release adds the explicit `protocol_versions`
///     field. No frame-shape changes vs V1 beyond that.
///
/// New versions get appended here as the wire format evolves.
pub const SUPPORTED_VERSIONS: &[u32] = &[1, 2];

/// Highest version this build can speak. Computed at compile time
/// from the last entry of [`SUPPORTED_VERSIONS`].
pub const MAX_PROTOCOL_VERSION: u32 = SUPPORTED_VERSIONS[SUPPORTED_VERSIONS.len() - 1];

/// Default protocol version assumed when a Logon arrives without an
/// explicit `protocol_versions` field — preserves backward
/// compatibility with pre-S28 clients.
pub const DEFAULT_LEGACY_VERSION: u32 = 1;

/// Result of a Logon-time version negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NegotiationOutcome {
    /// Both sides share at least one version; the highest is selected.
    Negotiated(u32),
    /// Disjoint sets — the server cannot speak any version the client
    /// advertised. The Logon handler turns this into a wire error so
    /// the client can fail fast rather than try to talk past us.
    NoOverlap,
}

/// Negotiate the active protocol version given the client's
/// advertised list and the server's supported list. Picks the highest
/// value present in both. If `client_versions` is empty, the result
/// is `Negotiated(DEFAULT_LEGACY_VERSION)` — matches pre-S28 clients
/// that don't advertise.
pub fn negotiate(
    client_versions: &[u32],
    server_versions: &[u32],
) -> NegotiationOutcome {
    if client_versions.is_empty() {
        // Legacy / unaware client. Treat as if they advertised
        // exactly the legacy version; servers always include it.
        if server_versions.contains(&DEFAULT_LEGACY_VERSION) {
            return NegotiationOutcome::Negotiated(DEFAULT_LEGACY_VERSION);
        }
        return NegotiationOutcome::NoOverlap;
    }
    let mut best: Option<u32> = None;
    for &v in client_versions {
        if server_versions.contains(&v) {
            best = Some(match best {
                Some(prev) if prev >= v => prev,
                _ => v,
            });
        }
    }
    match best {
        Some(v) => NegotiationOutcome::Negotiated(v),
        None => NegotiationOutcome::NoOverlap,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_highest_mutually_supported_version() {
        let client = vec![1, 2, 3];
        let server = vec![1, 2];
        assert_eq!(
            negotiate(&client, &server),
            NegotiationOutcome::Negotiated(2)
        );
    }

    #[test]
    fn client_only_supports_legacy_negotiates_legacy() {
        let client = vec![1];
        let server = SUPPORTED_VERSIONS;
        assert_eq!(negotiate(&client, server), NegotiationOutcome::Negotiated(1));
    }

    #[test]
    fn missing_versions_field_implies_legacy() {
        let server = SUPPORTED_VERSIONS;
        assert_eq!(negotiate(&[], server), NegotiationOutcome::Negotiated(1));
    }

    #[test]
    fn disjoint_sets_yield_no_overlap() {
        let client = vec![99, 100];
        let server = vec![1, 2];
        assert_eq!(negotiate(&client, &server), NegotiationOutcome::NoOverlap);
    }

    #[test]
    fn client_advertises_newer_than_server_falls_back_to_highest_shared() {
        // Client supports [1, 2, 5, 7]; server supports [1, 2, 3].
        // Should pick 2.
        let client = vec![1, 2, 5, 7];
        let server = vec![1, 2, 3];
        assert_eq!(
            negotiate(&client, &server),
            NegotiationOutcome::Negotiated(2)
        );
    }

    #[test]
    fn out_of_order_client_list_still_picks_max() {
        // Client list isn't required to be sorted.
        let client = vec![3, 1, 2];
        let server = vec![1, 2];
        assert_eq!(
            negotiate(&client, &server),
            NegotiationOutcome::Negotiated(2)
        );
    }
}

//! Standby-side log receiver.
//!
//! Listens for a single primary connection. On accept, it sends a
//! `Hello { highwater }` summarizing the standby's per-topic max
//! sequence, then loops reading `ReplFrame::Entry` frames and applies
//! each via `Topic::replay_upsert_map` / `replay_delete`. Reconnections
//! from a new primary are handled by dropping the previous loop and
//! accepting the new connection.

use crate::shipper::{read_frame, write_frame};
use crate::{ReplError, ReplFrame};
use cq_core::topic::SharedTopic;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Max time a freshly accepted primary has to present its `Auth` frame
/// before the standby drops the connection. Keeps an unauthenticated or
/// stalled peer from holding the single inline session slot forever.
const AUTH_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    pub listen_addr: String,
    /// Shared secret the connecting primary must present. When `Some`,
    /// the receiver requires a matching `ReplFrame::Auth` as the first
    /// frame (validated with a constant-time compare) before sending
    /// Hello or applying any entries. `None` (default) accepts any peer
    /// — only safe on a trusted network.
    pub token: Option<String>,
    /// S20 — this standby's AMPS-style instance id, reported to the primary
    /// in the `Hello` so the primary can avoid reflecting the standby's own
    /// entries back to it. Empty = unnamed (legacy single-node behaviour).
    pub instance_name: String,
    /// S21 — accept multiple concurrent inbound sessions. Active-passive
    /// (`standby`) keeps the legacy one-at-a-time behaviour (`false`): a new
    /// primary cleanly displaces the previous loop on failover. Active-active
    /// needs every peer connected simultaneously, so each accepted connection
    /// is spawned onto its own task (`true`).
    pub concurrent: bool,
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        ReceiverConfig {
            listen_addr: "0.0.0.0:9010".into(),
            token: None,
            instance_name: String::new(),
            concurrent: false,
        }
    }
}

/// Length-independent byte comparison so a rejected token can't be
/// recovered via response-timing analysis.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Listen forever and accept primaries one at a time. Returns only on
/// fatal bind error.
pub async fn run(
    cfg: ReceiverConfig,
    topics: Arc<DashMap<String, SharedTopic>>,
) -> Result<(), ReplError> {
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(addr = %cfg.listen_addr, "Replication receiver listening");

    let token = Arc::new(cfg.token.clone());
    let instance = Arc::new(cfg.instance_name.clone());
    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!(peer = %peer, "Replication peer connected");
        metrics::counter!("cq_repl_accept_total").increment(1);
        let topics = topics.clone();
        if cfg.concurrent {
            // S21 active-active: every peer stays connected at once, so
            // each session runs on its own task. Per-`(origin, sequence)`
            // dedup keeps concurrent applies convergent.
            let token = token.clone();
            let instance = instance.clone();
            tokio::spawn(async move {
                if let Err(e) = run_session(stream, topics, token.as_deref(), &instance).await {
                    tracing::warn!(error = %e, "Replication session ended");
                    metrics::counter!("cq_repl_session_error_total").increment(1);
                }
            });
        } else {
            // Active-passive: one primary at a time, run inline so a new
            // primary connection re-establishes state cleanly on failover.
            if let Err(e) = run_session(stream, topics, token.as_deref(), &instance).await {
                tracing::warn!(error = %e, "Replication session ended");
                metrics::counter!("cq_repl_session_error_total").increment(1);
            }
        }
    }
}

async fn run_session(
    mut stream: tokio::net::TcpStream,
    topics: Arc<DashMap<String, SharedTopic>>,
    token: Option<&str>,
    instance: &str,
) -> Result<(), ReplError> {
    // 0. Authenticate the primary before revealing any state. A peer
    //    that connects but never sends a valid Auth in time is dropped.
    if let Some(expected) = token {
        let frame = match tokio::time::timeout(AUTH_HANDSHAKE_TIMEOUT, read_frame(&mut stream)).await
        {
            Ok(f) => f?,
            Err(_) => {
                metrics::counter!("cq_repl_auth_failure_total", "reason" => "timeout").increment(1);
                tracing::warn!("Replication peer did not authenticate in time — closing");
                return Err(ReplError::Unauthorized("handshake timeout"));
            }
        };
        match frame {
            ReplFrame::Auth { token: presented }
                if constant_time_eq(presented.as_bytes(), expected.as_bytes()) =>
            {
                metrics::counter!("cq_repl_auth_success_total").increment(1);
            }
            ReplFrame::Auth { .. } => {
                metrics::counter!("cq_repl_auth_failure_total", "reason" => "bad_token")
                    .increment(1);
                tracing::warn!("Replication peer presented an invalid token — closing");
                return Err(ReplError::Unauthorized("token mismatch"));
            }
            _ => {
                metrics::counter!("cq_repl_auth_failure_total", "reason" => "no_auth").increment(1);
                tracing::warn!("Replication peer's first frame was not Auth — closing");
                return Err(ReplError::Unauthorized("missing auth frame"));
            }
        }
    }

    // 1. Send Hello with the standby's per-topic, per-origin high-water
    //    marks. For each topic we report what we've applied from every
    //    foreign origin (so the primary resumes each origin stream past
    //    what we hold) plus our own legacy/local high-water under the ""
    //    origin key (preserves resume for unnamed instances).
    let mut highwater: HashMap<String, HashMap<String, u64>> = HashMap::new();
    for entry in topics.iter() {
        let mut per_origin = entry.value().replicated_highwater_snapshot();
        per_origin.insert(String::new(), entry.value().current_sequence());
        highwater.insert(entry.key().clone(), per_origin);
    }
    write_frame(
        &mut stream,
        &ReplFrame::Hello {
            instance: instance.to_string(),
            highwater,
        },
    )
    .await?;

    // 2. Apply incoming entries forever. After each apply, emit an
    //    Ack back to the primary so the shipper's Ack reader can
    //    bump the per-topic `last_replicated_sequence` and release
    //    the publish path in S11 sync mode.
    loop {
        let frame = read_frame(&mut stream).await?;
        match frame {
            ReplFrame::Entry {
                sequence,
                topic,
                key,
                origin,
                is_tombstone,
                payload,
                is_delta,
            } => {
                apply_entry(
                    &topics,
                    sequence,
                    &topic,
                    &key,
                    &origin,
                    is_tombstone,
                    is_delta,
                    &payload,
                );
                let ack = ReplFrame::Ack {
                    topic: topic.clone(),
                    sequence,
                };
                if let Err(e) = write_frame(&mut stream, &ack).await {
                    tracing::warn!(error = %e, "Failed to write Ack — ending session");
                    return Err(e);
                }
            }
            ReplFrame::Hello { .. } => {
                tracing::warn!("Primary sent unexpected Hello — ignoring");
            }
            ReplFrame::Auth { .. } => {
                tracing::warn!("Primary sent unexpected post-handshake Auth — ignoring");
            }
            ReplFrame::Ack { .. } => {
                // Receivers ignore acks (only primaries care).
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_entry(
    topics: &Arc<DashMap<String, SharedTopic>>,
    sequence: u64,
    topic: &str,
    key: &str,
    origin: &str,
    is_tombstone: bool,
    is_delta: bool,
    payload: &[u8],
) {
    let Some(t) = topics.get(topic) else {
        tracing::debug!(topic, "Replicated entry for unknown topic — skipping");
        return;
    };
    if is_tombstone {
        t.replay_delete_origin(origin, sequence, key);
    } else {
        match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(serde_json::Value::Object(map)) => {
                if is_delta {
                    // Sparse `{key + changed fields}` payload — merge into
                    // the existing row instead of replacing it (a plain
                    // upsert would null every absent column).
                    t.replay_delta_map_origin(origin, sequence, &map);
                } else {
                    t.replay_upsert_map_origin(origin, sequence, &map);
                }
            }
            Ok(_) => {
                tracing::warn!(topic, "Replicated entry payload was not a JSON object");
            }
            Err(e) => {
                tracing::warn!(topic, error = %e, "Replicated entry payload not JSON");
            }
        }
    }
    metrics::counter!(
        "cq_repl_applied_entries_total",
        "topic" => topic.to_string()
    )
    .increment(1);
    metrics::gauge!(
        "cq_repl_applied_max_sequence",
        "topic" => topic.to_string()
    )
    .set(sequence as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shipper::{read_frame, write_frame};
    use crate::ReplFrame;
    use std::collections::HashMap;
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn constant_time_eq_matches_only_identical_bytes() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"Secret"));
        assert!(!constant_time_eq(b"secret", b"secre")); // length differs
        assert!(!constant_time_eq(b"", b"x"));
        assert!(constant_time_eq(b"", b""));
    }

    // Drives a real run_session against a connected client socket so the
    // handshake exercises the actual frame wire path.
    async fn connected_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (server, client)
    }

    fn empty_topics() -> Arc<DashMap<String, SharedTopic>> {
        Arc::new(DashMap::new())
    }

    #[tokio::test]
    async fn correct_token_passes_and_yields_hello() {
        let (server, mut client) = connected_pair().await;
        let session = tokio::spawn(async move {
            run_session(server, empty_topics(), Some("s3cret"), "standby").await
        });
        write_frame(
            &mut client,
            &ReplFrame::Auth {
                token: "s3cret".into(),
            },
        )
        .await
        .unwrap();
        // Server should now send Hello.
        let hello = read_frame(&mut client).await.unwrap();
        assert!(matches!(hello, ReplFrame::Hello { .. }));
        // Drop the client so the session loop ends.
        drop(client);
        let _ = session.await.unwrap();
    }

    #[tokio::test]
    async fn wrong_token_is_rejected_before_hello() {
        let (server, mut client) = connected_pair().await;
        let session = tokio::spawn(async move {
            run_session(server, empty_topics(), Some("s3cret"), "standby").await
        });
        write_frame(
            &mut client,
            &ReplFrame::Auth {
                token: "nope".into(),
            },
        )
        .await
        .unwrap();
        let res = session.await.unwrap();
        assert!(matches!(res, Err(ReplError::Unauthorized(_))));
        // No Hello should have been sent before rejection.
        assert!(read_frame(&mut client).await.is_err());
    }

    #[tokio::test]
    async fn missing_auth_frame_is_rejected() {
        let (server, mut client) = connected_pair().await;
        let session = tokio::spawn(async move {
            run_session(server, empty_topics(), Some("s3cret"), "standby").await
        });
        // Send a non-Auth frame first.
        write_frame(
            &mut client,
            &ReplFrame::Hello {
                instance: "primary".into(),
                highwater: HashMap::new(),
            },
        )
        .await
        .unwrap();
        let res = session.await.unwrap();
        assert!(matches!(res, Err(ReplError::Unauthorized(_))));
    }

    #[tokio::test]
    async fn concurrent_receiver_accepts_multiple_peers() {
        // S21: with `concurrent = true`, a single receiver keeps several
        // inbound peers connected at once (the active-active mesh shape).
        // The legacy inline path would hold only the first peer and block
        // the listener until that session ended.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let topics = empty_topics();
        let cfg = ReceiverConfig {
            listen_addr: addr.to_string(),
            token: None,
            instance_name: "node-self".into(),
            concurrent: true,
        };
        let handle = tokio::spawn(async move {
            let _ = run(cfg, topics).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Two peers connect at the same time; each must independently get a
        // Hello back, proving both sessions run concurrently.
        let mut a = TcpStream::connect(addr).await.unwrap();
        let mut b = TcpStream::connect(addr).await.unwrap();
        let hello_a = read_frame(&mut a).await.unwrap();
        let hello_b = read_frame(&mut b).await.unwrap();
        assert!(matches!(hello_a, ReplFrame::Hello { .. }));
        assert!(matches!(hello_b, ReplFrame::Hello { .. }));

        drop(a);
        drop(b);
        handle.abort();
    }

    #[tokio::test]
    async fn no_token_configured_skips_auth() {
        let (server, mut client) = connected_pair().await;
        let session =
            tokio::spawn(async move { run_session(server, empty_topics(), None, "standby").await });
        // With no token, the server sends Hello immediately without auth.
        let hello = read_frame(&mut client).await.unwrap();
        assert!(matches!(hello, ReplFrame::Hello { .. }));
        drop(client);
        let _ = session.await.unwrap();
    }
}

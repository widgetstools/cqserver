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

#[derive(Debug, Clone)]
pub struct ReceiverConfig {
    pub listen_addr: String,
}

impl Default for ReceiverConfig {
    fn default() -> Self {
        ReceiverConfig {
            listen_addr: "0.0.0.0:9010".into(),
        }
    }
}

/// Listen forever and accept primaries one at a time. Returns only on
/// fatal bind error.
pub async fn run(
    cfg: ReceiverConfig,
    topics: Arc<DashMap<String, SharedTopic>>,
) -> Result<(), ReplError> {
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    tracing::info!(addr = %cfg.listen_addr, "Replication receiver listening");

    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::info!(peer = %peer, "Replication primary connected");
        metrics::counter!("cq_repl_accept_total").increment(1);
        let topics = topics.clone();
        // One primary at a time: we run the session inline so a new
        // primary connection can re-establish state cleanly after a
        // failover.
        if let Err(e) = run_session(stream, topics).await {
            tracing::warn!(error = %e, "Replication session ended");
            metrics::counter!("cq_repl_session_error_total").increment(1);
        }
    }
}

async fn run_session(
    mut stream: tokio::net::TcpStream,
    topics: Arc<DashMap<String, SharedTopic>>,
) -> Result<(), ReplError> {
    // 1. Send Hello with the standby's per-topic high-water marks.
    let mut highwater: HashMap<String, u64> = HashMap::new();
    for entry in topics.iter() {
        highwater.insert(entry.key().clone(), entry.value().current_sequence());
    }
    write_frame(&mut stream, &ReplFrame::Hello { highwater }).await?;

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
                is_tombstone,
                payload,
            } => {
                apply_entry(&topics, sequence, &topic, &key, is_tombstone, &payload);
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
            ReplFrame::Ack { .. } => {
                // Receivers ignore acks (only primaries care).
            }
        }
    }
}

fn apply_entry(
    topics: &Arc<DashMap<String, SharedTopic>>,
    sequence: u64,
    topic: &str,
    key: &str,
    is_tombstone: bool,
    payload: &[u8],
) {
    let Some(t) = topics.get(topic) else {
        tracing::debug!(topic, "Replicated entry for unknown topic — skipping");
        return;
    };
    if is_tombstone {
        t.replay_delete(sequence, key);
    } else {
        match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(serde_json::Value::Object(map)) => {
                t.replay_upsert_map(sequence, &map);
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

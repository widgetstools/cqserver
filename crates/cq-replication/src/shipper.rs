//! Primary-side log shipper.
//!
//! For each persistent topic, the shipper holds a `TxLogReader` over the
//! topic's log directory. On connect to the standby it waits for a
//! `Hello { highwater }` frame, then streams every entry whose sequence
//! is strictly greater than the standby's high-water as a
//! `ReplFrame::Entry`. When the reader hits EOF on the current segment
//! the shipper re-opens it on the next poll tick — the segmented reader
//! will pick up any newly rolled segments automatically.

use crate::filter::{apply_filter, apply_transform, FilterSpec, TransformSpec};
use crate::{ReplError, ReplFrame, MAX_FRAME_SIZE};
use cq_core::topic::SharedTopic;
use cq_txlog::reader::TxLogReader;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct ShipperConfig {
    pub peer: String,
    pub topics: Vec<(String, PathBuf)>,
    pub poll_interval: Duration,
    pub reconnect_backoff: Duration,
    /// S12 — optional per-destination filter. When set, the shipper
    /// drops every entry whose JSON payload doesn't match
    /// `column = "value"`. Tombstones always pass through (the
    /// standby's SOW must observe deletes).
    pub filter: Option<FilterSpec>,
    /// S12 — optional per-destination transform. When set, the
    /// listed fields are stripped from the JSON payload before
    /// shipping. Useful for redacting columns that a less-trusted
    /// downstream destination shouldn't see.
    pub transform: Option<TransformSpec>,
    /// S11 — per-topic SharedTopic refs the shipper uses to bump
    /// `last_replicated_sequence` on every received Ack. Empty map
    /// = no live refs and the Ack reader still runs (for metrics)
    /// but doesn't update any barrier.
    pub topic_refs: HashMap<String, SharedTopic>,
}

impl Default for ShipperConfig {
    fn default() -> Self {
        ShipperConfig {
            peer: "127.0.0.1:9010".into(),
            topics: Vec::new(),
            poll_interval: Duration::from_millis(50),
            reconnect_backoff: Duration::from_secs(2),
            filter: None,
            transform: None,
            topic_refs: HashMap::new(),
        }
    }
}

/// Run forever: connect, ship, reconnect on error.
pub async fn run(cfg: ShipperConfig) -> Result<(), ReplError> {
    if cfg.topics.is_empty() {
        tracing::info!("Replication shipper has no topics — exiting");
        return Ok(());
    }
    loop {
        match ship_once(&cfg).await {
            Ok(()) => return Ok(()), // shouldn't happen in steady state
            Err(e) => {
                tracing::warn!(error = %e, "Replication shipper disconnected; reconnecting");
                metrics::counter!("cq_repl_reconnect_total").increment(1);
                tokio::time::sleep(cfg.reconnect_backoff).await;
            }
        }
    }
}

async fn ship_once(cfg: &ShipperConfig) -> Result<(), ReplError> {
    let conn = TcpStream::connect(&cfg.peer).await?;
    tracing::info!(peer = %cfg.peer, "Replication shipper connected");
    metrics::counter!("cq_repl_connect_total").increment(1);

    let (mut read_half, mut write_half) = tokio::io::split(conn);

    // 1. Receive Hello on the read half.
    let hello = read_frame_half(&mut read_half).await?;
    let highwater = match hello {
        ReplFrame::Hello { highwater } => highwater,
        other => {
            tracing::warn!(?other, "Standby didn't open with Hello; assuming empty");
            HashMap::new()
        }
    };

    // 2. Spawn the Ack reader. It runs concurrently with the ship loop
    //    and bumps `last_replicated_sequence` on each Ack so the
    //    publish path's S11 barrier can release. The reader exits when
    //    the connection drops; the ship loop's next write then errors
    //    and bubbles up to the reconnect-with-backoff path.
    let topic_refs = Arc::new(cfg.topic_refs.clone());
    let ack_task = {
        let topic_refs = topic_refs.clone();
        tokio::spawn(async move {
            loop {
                match read_frame_half(&mut read_half).await {
                    Ok(ReplFrame::Ack { topic, sequence }) => {
                        if let Some(t) = topic_refs.get(&topic) {
                            t.mark_replicated(sequence);
                        }
                        metrics::counter!(
                            "cq_repl_acks_received_total",
                            "topic" => topic.clone()
                        )
                        .increment(1);
                        metrics::gauge!(
                            "cq_repl_acked_max_sequence",
                            "topic" => topic
                        )
                        .set(sequence as f64);
                    }
                    Ok(other) => {
                        tracing::debug!(?other, "Shipper Ack reader saw non-Ack frame; ignoring");
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "Shipper Ack reader ended");
                        return;
                    }
                }
            }
        })
    };

    // 3. Per-topic state.
    let mut states: Vec<TopicShipper> = cfg
        .topics
        .iter()
        .map(|(name, dir)| TopicShipper {
            topic: name.clone(),
            dir: dir.clone(),
            last_shipped: highwater.get(name).copied().unwrap_or(0),
            filter: cfg.filter.clone(),
            transform: cfg.transform.clone(),
        })
        .collect();

    // 4. Stream loop. On any IO error we abort the Ack task and
    //    return the error to the reconnect-with-backoff outer loop.
    let result: Result<(), ReplError> = loop {
        let mut shipped_anything = false;
        let mut err: Option<ReplError> = None;
        for s in states.iter_mut() {
            match s.ship_pending_half(&mut write_half).await {
                Ok(any) => shipped_anything |= any,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = err {
            break Err(e);
        }
        if !shipped_anything {
            tokio::time::sleep(cfg.poll_interval).await;
        }
    };

    ack_task.abort();
    result
}

struct TopicShipper {
    topic: String,
    dir: PathBuf,
    last_shipped: u64,
    filter: Option<FilterSpec>,
    transform: Option<TransformSpec>,
}

impl TopicShipper {
    /// Open a fresh reader, skip past `last_shipped`, ship everything
    /// after it. Returns whether anything was sent. Applies the
    /// configured per-destination filter + transform on the
    /// payload-bearing path; tombstones always ship unchanged so the
    /// standby's SOW never drifts from the primary on deletes.
    ///
    /// Operates on the write half of the split TCP stream so the
    /// shipper's Ack reader can run concurrently on the read half.
    async fn ship_pending_half<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        conn: &mut W,
    ) -> Result<bool, ReplError> {
        let mut reader = TxLogReader::open(&self.dir)?;
        let mut any = false;
        while let Some(entry) = reader.read_next()? {
            if entry.sequence <= self.last_shipped {
                continue;
            }
            // S12 filter: drop non-matching entries (but never drop a
            // tombstone — `apply_filter` ships those unconditionally).
            if !apply_filter(&entry.payload, self.filter.as_ref()) {
                self.last_shipped = entry.sequence;
                metrics::counter!(
                    "cq_repl_filtered_entries_total",
                    "topic" => self.topic.clone()
                )
                .increment(1);
                continue;
            }
            // S12 transform: strip listed JSON fields. No-op on
            // tombstones.
            let payload = apply_transform(entry.payload, self.transform.as_ref());
            let is_tombstone = payload.is_empty();
            let frame = ReplFrame::Entry {
                sequence: entry.sequence,
                topic: entry.topic,
                key: entry.key,
                is_tombstone,
                payload,
            };
            write_frame_half(conn, &frame).await?;
            self.last_shipped = entry.sequence;
            metrics::counter!(
                "cq_repl_shipped_entries_total",
                "topic" => self.topic.clone()
            )
            .increment(1);
            metrics::gauge!(
                "cq_repl_shipped_max_sequence",
                "topic" => self.topic.clone()
            )
            .set(self.last_shipped as f64);
            any = true;
        }
        Ok(any)
    }
}

/// Half-stream variant of `read_frame` so the shipper can keep a
/// `tokio::io::ReadHalf` borrowed exclusively in the Ack reader task
/// while the ship loop holds the `WriteHalf`.
pub(crate) async fn read_frame_half<R: tokio::io::AsyncRead + Unpin>(
    conn: &mut R,
) -> Result<ReplFrame, ReplError> {
    let mut len_buf = [0u8; 4];
    conn.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(ReplError::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len];
    conn.read_exact(&mut body).await?;
    Ok(rmp_serde::from_slice(&body)?)
}

/// Half-stream variant of `write_frame`.
pub(crate) async fn write_frame_half<W: tokio::io::AsyncWrite + Unpin>(
    conn: &mut W,
    f: &ReplFrame,
) -> Result<(), ReplError> {
    let body = rmp_serde::to_vec_named(f)?;
    if body.len() > MAX_FRAME_SIZE {
        return Err(ReplError::FrameTooLarge(body.len()));
    }
    let len = (body.len() as u32).to_be_bytes();
    conn.write_all(&len).await?;
    conn.write_all(&body).await?;
    Ok(())
}

pub(crate) async fn read_frame(conn: &mut TcpStream) -> Result<ReplFrame, ReplError> {
    let mut len_buf = [0u8; 4];
    conn.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_SIZE {
        return Err(ReplError::FrameTooLarge(len));
    }
    let mut body = vec![0u8; len];
    conn.read_exact(&mut body).await?;
    Ok(rmp_serde::from_slice(&body)?)
}

pub(crate) async fn write_frame(conn: &mut TcpStream, f: &ReplFrame) -> Result<(), ReplError> {
    let body = rmp_serde::to_vec_named(f)?;
    if body.len() > MAX_FRAME_SIZE {
        return Err(ReplError::FrameTooLarge(body.len()));
    }
    let len = (body.len() as u32).to_be_bytes();
    conn.write_all(&len).await?;
    conn.write_all(&body).await?;
    Ok(())
}

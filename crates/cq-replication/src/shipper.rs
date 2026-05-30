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
    /// Shared secret presented to the standby on connect. When `Some`,
    /// the shipper sends a `ReplFrame::Auth` as its first frame; the
    /// standby must be configured with the same token or it closes the
    /// connection. `None` (default) sends no auth frame — only safe when
    /// the replication port is reachable solely over a trusted network.
    pub token: Option<String>,
    /// S20 — this instance's AMPS-style id. Informational on the shipper
    /// side (entries carry their own per-write `origin` from the txlog);
    /// kept for symmetry and future active-active handshakes. Empty =
    /// unnamed (legacy single-node behaviour).
    pub instance_name: String,
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
            token: None,
            instance_name: String::new(),
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

    // 0. Authenticate before anything else when a token is configured.
    //    The standby validates this before sending Hello, so we must
    //    write it on the write half prior to reading.
    if let Some(token) = &cfg.token {
        write_frame_half(
            &mut write_half,
            &ReplFrame::Auth {
                token: token.clone(),
            },
        )
        .await?;
    }

    // 1. Receive Hello on the read half.
    let hello = read_frame_half(&mut read_half).await?;
    let (peer_instance, highwater) = match hello {
        ReplFrame::Hello {
            instance,
            highwater,
        } => (instance, highwater),
        other => {
            tracing::warn!(?other, "Standby didn't open with Hello; assuming empty");
            (String::new(), HashMap::new())
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
            last_shipped: highwater.get(name).cloned().unwrap_or_default(),
            peer_instance: peer_instance.clone(),
            filter: cfg.filter.clone(),
            transform: cfg.transform.clone(),
            resume_segment: 0,
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
    /// Per-origin high-water of sequences already on the peer (seeded from
    /// the receiver's `Hello`) or shipped this session. Each origin has an
    /// independent sequence space, so a single scalar would let one origin's
    /// high sequences wrongly suppress another's low ones.
    last_shipped: HashMap<String, u64>,
    /// Peer's instance id, for loop avoidance — never reflect an entry back
    /// to the instance that produced it. Empty disables the guard.
    peer_instance: String,
    filter: Option<FilterSpec>,
    transform: Option<TransformSpec>,
    /// Journal cursor: the segment id we'd already shipped through as of
    /// the last poll cycle. Each cycle fast-forwards the fresh reader past
    /// every sealed segment below this id instead of rescanning the whole
    /// log. Seeded at 0 (full initial catch-up scan, gated by the peer's
    /// `Hello` high-water) and only ever advances within a session; a
    /// reconnect builds a new `TopicShipper`, resetting it to 0 so the
    /// re-sync honours the destination's possibly-lower durable position.
    ///
    /// Per-entry dedup is still governed by `last_shipped`, so the cursor
    /// is a pure optimization: starting a cycle anywhere at or before the
    /// first unshipped entry yields identical output.
    resume_segment: u64,
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
        // Fast-forward past sealed segments we've already shipped through.
        // Safe because everything below `resume_segment` was processed in
        // a prior cycle (shipped, filtered, or loop-avoided) and `reader`
        // re-lists the directory on open, so any segments rolled or
        // reclaimed since are reflected here.
        reader.skip_to_segment(self.resume_segment);
        let mut any = false;
        let mut scanned: u64 = 0;
        while let Some(entry) = reader.read_next()? {
            // Iterations that ship an entry await on `write_frame_half`
            // below (a natural yield point), but runs of skipped entries
            // (already-shipped, filtered, loop-avoided) have none — so a
            // long catch-up scan could monopolize the runtime worker.
            // Yield periodically to keep other tasks on this worker live.
            scanned = scanned.wrapping_add(1);
            if scanned % 256 == 0 {
                tokio::task::yield_now().await;
            }
            // Loop avoidance: never reflect an entry back to the instance
            // that first produced it. Only meaningful when both ids are
            // named (legacy unnamed entries have origin "").
            if !entry.origin.is_empty() && entry.origin == self.peer_instance {
                continue;
            }
            let hw = self.last_shipped.get(&entry.origin).copied().unwrap_or(0);
            if entry.sequence <= hw {
                continue;
            }
            // S12 filter: drop non-matching entries (but never drop a
            // tombstone — `apply_filter` ships those unconditionally).
            if !apply_filter(&entry.payload, self.filter.as_ref()) {
                self.last_shipped
                    .insert(entry.origin.clone(), entry.sequence);
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
            let origin = entry.origin.clone();
            let sequence = entry.sequence;
            let frame = ReplFrame::Entry {
                sequence,
                topic: entry.topic,
                key: entry.key,
                origin: entry.origin,
                is_tombstone,
                payload,
            };
            write_frame_half(conn, &frame).await?;
            self.last_shipped.insert(origin, sequence);
            metrics::counter!(
                "cq_repl_shipped_entries_total",
                "topic" => self.topic.clone()
            )
            .increment(1);
            metrics::gauge!(
                "cq_repl_shipped_max_sequence",
                "topic" => self.topic.clone()
            )
            .set(sequence as f64);
            any = true;
        }
        // Advance the cursor to the segment we drained to (the active
        // segment once the reader hits EOF). New entries only ever land in
        // that segment or a later one, so next cycle can skip everything
        // before it. We never advance past a segment with unshipped
        // entries: the loop above drains to EOF, and `last_shipped` still
        // gates any entries re-read within the resume segment.
        if let Some(seg) = reader.current_segment_id() {
            self.resume_segment = seg;
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
    // Single buffer (len prefix + body) → one write_all. With TCP_NODELAY
    // on, two separate writes flush as two packets; coalescing avoids the
    // extra kernel round-trip on every shipped frame.
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    conn.write_all(&buf).await?;
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
    // Single gathered write (len prefix + body) — see note in `write_frame`.
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
    buf.extend_from_slice(&body);
    conn.write_all(&buf).await?;
    Ok(())
}

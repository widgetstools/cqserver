//! Primary-side log shipper.
//!
//! For each persistent topic, the shipper holds a `TxLogReader` over the
//! topic's log directory. On connect to the standby it waits for a
//! `Hello { highwater }` frame, then streams every entry whose sequence
//! is strictly greater than the standby's high-water as a
//! `ReplFrame::Entry`. When the reader hits EOF on the current segment
//! the shipper re-opens it on the next poll tick — the segmented reader
//! will pick up any newly rolled segments automatically.

use crate::{ReplError, ReplFrame, MAX_FRAME_SIZE};
use cq_txlog::reader::TxLogReader;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct ShipperConfig {
    pub peer: String,
    pub topics: Vec<(String, PathBuf)>,
    pub poll_interval: Duration,
    pub reconnect_backoff: Duration,
}

impl Default for ShipperConfig {
    fn default() -> Self {
        ShipperConfig {
            peer: "127.0.0.1:9010".into(),
            topics: Vec::new(),
            poll_interval: Duration::from_millis(50),
            reconnect_backoff: Duration::from_secs(2),
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
    let mut conn = TcpStream::connect(&cfg.peer).await?;
    tracing::info!(peer = %cfg.peer, "Replication shipper connected");
    metrics::counter!("cq_repl_connect_total").increment(1);

    // 1. Receive Hello.
    let hello = read_frame(&mut conn).await?;
    let highwater = match hello {
        ReplFrame::Hello { highwater } => highwater,
        other => {
            tracing::warn!(?other, "Standby didn't open with Hello; assuming empty");
            HashMap::new()
        }
    };

    // 2. Per-topic state.
    let mut states: Vec<TopicShipper> = cfg
        .topics
        .iter()
        .map(|(name, dir)| TopicShipper {
            topic: name.clone(),
            dir: dir.clone(),
            last_shipped: highwater.get(name).copied().unwrap_or(0),
        })
        .collect();

    // 3. Stream loop. Each pass opens a fresh reader per topic (cheap —
    //    these are small directories) and skips entries we've already
    //    shipped. After a quiet pass we sleep `poll_interval`.
    loop {
        let mut shipped_anything = false;
        for s in states.iter_mut() {
            shipped_anything |= s.ship_pending(&mut conn).await?;
        }
        if !shipped_anything {
            tokio::time::sleep(cfg.poll_interval).await;
        }
    }
}

struct TopicShipper {
    topic: String,
    dir: PathBuf,
    last_shipped: u64,
}

impl TopicShipper {
    /// Open a fresh reader, skip past `last_shipped`, ship everything
    /// after it. Returns whether anything was sent.
    async fn ship_pending(&mut self, conn: &mut TcpStream) -> Result<bool, ReplError> {
        let mut reader = TxLogReader::open(&self.dir)?;
        let mut any = false;
        while let Some(entry) = reader.read_next()? {
            if entry.sequence <= self.last_shipped {
                continue;
            }
            let frame = ReplFrame::Entry {
                sequence: entry.sequence,
                topic: entry.topic,
                key: entry.key,
                is_tombstone: entry.payload.is_empty(),
                payload: entry.payload,
            };
            write_frame(conn, &frame).await?;
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

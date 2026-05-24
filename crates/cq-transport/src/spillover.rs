//! S21 — slow-client offlining to disk.
//!
//! A `Spillover` is a per-subscription overflow log: when the outbound
//! queue is full and the delivery code would otherwise drop a frame,
//! we append the frame to a file instead. A background drain task
//! pulls frames back out of the file and `try_send`s them into the
//! queue as soon as it has room, so the subscriber eventually receives
//! every frame the server tried to deliver — just out of phase with
//! live publishes.
//!
//! Ordering is preserved: once a route has any spilled frames, new
//! incoming frames also write to spillover (rather than to the queue)
//! so that the consumer sees frames in the same order the server
//! produced them. The drain task reads in append order.
//!
//! ### On-disk format
//!
//! The file is append-only. Each frame is written as:
//!
//! ```text
//! [u8: type tag]    // 0 = Text, 1 = Binary
//! [u32 BE: length]  // payload size in bytes
//! [bytes ...]       // payload
//! ```
//!
//! The reader maintains an in-memory `read_offset` cursor and advances
//! it after every successful `try_send`. The file's tail (bytes
//! between `read_offset` and EOF) holds the pending backlog.
//!
//! ### Bounded size
//!
//! Each spillover has a `max_bytes` cap. When the file would exceed it,
//! the new frame is dropped and `cq_spillover_drop_total` ticks — the
//! sub is so far behind that even disk can't keep up. Operators can
//! couple this with `auto_disconnect` so chronically-bad consumers are
//! evicted rather than indefinitely growing the spool.
//!
//! ### Cleanup
//!
//! On `close()`, the underlying file is deleted. Callers (sessions /
//! routes) invoke this when the subscription terminates. The file's
//! lifetime is intended to span the subscription only; it is not a
//! durability primitive.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::session::OutboundFrame;

/// Errors that may surface from the spillover layer.
#[derive(Debug, thiserror::Error)]
pub enum SpilloverError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("spillover exceeded {cap} bytes (route is hopelessly behind)")]
    OverCap { cap: u64 },
    #[error("malformed frame on read")]
    Malformed,
}

const TAG_TEXT: u8 = 0;
const TAG_BINARY: u8 = 1;

/// Per-route on-disk overflow file. Shared via `Arc`. Reader + writer
/// share one mutex over the inner state to keep ordering trivially
/// correct; spillover is a slow-path data structure so the lock
/// contention is acceptable.
pub struct Spillover {
    path: PathBuf,
    inner: Mutex<SpilloverInner>,
    /// Pending frames counter, updated under the lock. Exposed as an
    /// atomic so callers can cheaply check "is there backlog?" without
    /// acquiring the mutex (which the drain task and the delivery
    /// path both hold).
    pending: Arc<AtomicU64>,
    max_bytes: u64,
}

struct SpilloverInner {
    /// Buffered writer at the file's tail.
    writer: BufWriter<File>,
    /// Buffered reader at `read_offset`. Re-opened lazily on demand
    /// since the writer holds the only handle that's safe to extend
    /// the file.
    read_offset: u64,
    write_offset: u64,
}

impl Spillover {
    /// Open (creating if needed) a spillover file at `path`. If the
    /// file already exists, its tail becomes the initial backlog —
    /// useful for recovering on restart, though current production
    /// callers always create fresh files per subscription.
    pub fn open(path: PathBuf, max_bytes: u64) -> Result<Self, SpilloverError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        let write_offset = file.metadata()?.len();
        Ok(Spillover {
            path,
            inner: Mutex::new(SpilloverInner {
                writer: BufWriter::new(file),
                read_offset: 0,
                write_offset,
            }),
            pending: Arc::new(AtomicU64::new(0)),
            max_bytes,
        })
    }

    /// Append one frame to the spillover. Fails with `OverCap` when the
    /// new frame's size would exceed the configured budget; the caller
    /// can then choose to drop the frame (and tick a metric) instead of
    /// growing the spool indefinitely.
    pub fn write_frame(&self, frame: &OutboundFrame) -> Result<(), SpilloverError> {
        let bytes = frame.as_bytes();
        let tag = match frame {
            OutboundFrame::Text(_) => TAG_TEXT,
            OutboundFrame::Binary(_) => TAG_BINARY,
        };
        let frame_size = 1 + 4 + bytes.len() as u64;

        let mut inner = self.inner.lock();
        let new_size = inner.write_offset - inner.read_offset + frame_size;
        if new_size > self.max_bytes {
            return Err(SpilloverError::OverCap {
                cap: self.max_bytes,
            });
        }
        inner.writer.write_all(&[tag])?;
        inner.writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
        inner.writer.write_all(bytes)?;
        inner.writer.flush()?;
        inner.write_offset += frame_size;
        drop(inner);
        self.pending.fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Read the next pending frame in append order. Returns `Ok(None)`
    /// when the spillover is empty. Advances the read cursor on
    /// success so a subsequent call returns the *next* frame.
    pub fn read_next_frame(&self) -> Result<Option<OutboundFrame>, SpilloverError> {
        let mut inner = self.inner.lock();
        if inner.read_offset >= inner.write_offset {
            return Ok(None);
        }
        let path = self.path.clone();
        // Re-open a fresh reader at the current offset. Cheap: append
        // path is the slow path, and the same file is already open
        // for write — POSIX permits the second descriptor.
        let mut reader = BufReader::new(File::open(&path)?);
        reader.seek(SeekFrom::Start(inner.read_offset))?;
        let mut tag_buf = [0u8; 1];
        if let Err(e) = reader.read_exact(&mut tag_buf) {
            if e.kind() == ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(e.into());
        }
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        inner.read_offset += 1 + 4 + len as u64;
        drop(inner);
        self.pending.fetch_sub(1, Ordering::Release);
        let frame = match tag_buf[0] {
            TAG_TEXT => {
                let s = String::from_utf8(payload).map_err(|_| SpilloverError::Malformed)?;
                OutboundFrame::Text(s)
            }
            TAG_BINARY => OutboundFrame::Binary(payload),
            _ => return Err(SpilloverError::Malformed),
        };
        Ok(Some(frame))
    }

    /// Quick check: is anything pending? Cheap atomic load, no lock.
    pub fn is_empty(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0
    }

    /// Number of frames currently waiting to be drained.
    pub fn pending_count(&self) -> u64 {
        self.pending.load(Ordering::Acquire)
    }

    /// Bytes occupied by pending frames (writer offset minus reader offset).
    pub fn pending_bytes(&self) -> u64 {
        let inner = self.inner.lock();
        inner.write_offset.saturating_sub(inner.read_offset)
    }

    /// Best-effort cleanup: deletes the spillover file. Safe to call
    /// even if the file doesn't exist. Intended for subscription
    /// teardown — the file is not a durability primitive.
    pub fn close(&self) {
        let _ = std::fs::remove_file(&self.path);
    }

    /// Cheap clone of the shared pending counter — useful for the
    /// drain task to wake on changes without acquiring the mutex.
    pub fn pending_handle(&self) -> Arc<AtomicU64> {
        self.pending.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn new_spillover(dir: &std::path::Path) -> Spillover {
        Spillover::open(dir.join("spill.log"), 1024 * 1024).expect("open")
    }

    #[test]
    fn write_then_read_returns_frames_in_append_order() {
        let dir = tempdir().unwrap();
        let sp = new_spillover(dir.path());
        sp.write_frame(&OutboundFrame::Text("alpha".into())).unwrap();
        sp.write_frame(&OutboundFrame::Binary(vec![1, 2, 3, 4]))
            .unwrap();
        sp.write_frame(&OutboundFrame::Text("gamma".into())).unwrap();
        assert_eq!(sp.pending_count(), 3);

        let f1 = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f1, OutboundFrame::Text(ref s) if s == "alpha"));
        let f2 = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f2, OutboundFrame::Binary(ref b) if b == &[1, 2, 3, 4]));
        let f3 = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f3, OutboundFrame::Text(ref s) if s == "gamma"));
        assert!(sp.read_next_frame().unwrap().is_none());
        assert_eq!(sp.pending_count(), 0);
    }

    #[test]
    fn write_after_drain_continues_in_order() {
        let dir = tempdir().unwrap();
        let sp = new_spillover(dir.path());
        sp.write_frame(&OutboundFrame::Text("one".into())).unwrap();
        let _ = sp.read_next_frame().unwrap();
        sp.write_frame(&OutboundFrame::Text("two".into())).unwrap();
        let f = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f, OutboundFrame::Text(ref s) if s == "two"));
    }

    #[test]
    fn over_cap_returns_error_without_corrupting_state() {
        let dir = tempdir().unwrap();
        // Tiny budget: 32 bytes total. One small frame fits; the next blows the cap.
        let sp = Spillover::open(dir.path().join("spill.log"), 32).unwrap();
        sp.write_frame(&OutboundFrame::Text("hello".into()))
            .expect("first fits"); // 1+4+5 = 10 bytes
        let huge = OutboundFrame::Text("x".repeat(64));
        let err = sp.write_frame(&huge).unwrap_err();
        assert!(matches!(err, SpilloverError::OverCap { .. }));
        // First frame should still be readable.
        let f = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f, OutboundFrame::Text(ref s) if s == "hello"));
    }

    #[test]
    fn reopening_existing_file_recovers_pending_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("spill.log");
        {
            let sp = Spillover::open(path.clone(), 1024).unwrap();
            sp.write_frame(&OutboundFrame::Text("survives".into())).unwrap();
            // Drop sp without reading.
        }
        // Re-open at the same path — pending tail is preserved on disk
        // even if our in-memory pending counter resets. The read path
        // walks the file from offset 0 so the frame is recovered.
        let sp = Spillover::open(path, 1024).unwrap();
        let f = sp.read_next_frame().unwrap().unwrap();
        assert!(matches!(f, OutboundFrame::Text(ref s) if s == "survives"));
    }
}

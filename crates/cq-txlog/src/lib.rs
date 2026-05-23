//! Append-only transaction log for SOW durability.
//!
//! # Entry format
//!
//! Every entry is a length-prefixed, checksummed record:
//!
//! ```text
//! ┌──────────┬──────────┬──────────────┬───────────────────────────────┐
//! │ frame_len│ crc32    │ body                                          │
//! │ u32 BE   │ u32 BE   │ (frame_len bytes)                             │
//! └──────────┴──────────┴──────────────┴───────────────────────────────┘
//!                       │ body layout:
//!                       │   sequence     : u64 BE  (monotonic, per topic)
//!                       │   timestamp_ms : u64 BE
//!                       │   topic_len    : u16 BE  (≤ 1024)
//!                       │   topic        : bytes
//!                       │   key_len      : u16 BE
//!                       │   key          : bytes   (may be empty)
//!                       │   payload      : bytes   (rest of body)
//! ```
//!
//! The `sequence` is the bookmark surface: clients track the highest
//! sequence they've successfully processed and pass it on reconnect to
//! replay anything they missed.
//!
//! Empty `payload` is interpreted as a tombstone (key deletion).
//!
//! `crc32` is computed over the body. A mismatch on read aborts the replay
//! at that offset. A truncated tail (less than `frame_len` bytes available)
//! is treated as a clean truncation — the partial entry is discarded and
//! the reader stops, mimicking unclean-shutdown recovery in real systems.

pub mod reader;
pub mod segment;
pub mod writer;

use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_ENTRY_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_TOPIC_LEN: usize = 1024;

/// Default segment size (256 MB), matching the ARCHITECTURE.md spec.
pub const DEFAULT_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct TxEntry {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub topic: String,
    pub key: String,
    pub payload: Vec<u8>,
}

impl TxEntry {
    pub fn is_tombstone(&self) -> bool {
        self.payload.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Never fsync; rely on OS page cache flushes. Fastest, least durable.
    None,
    /// fsync after every appended entry. Slowest, strongest durability.
    EveryWrite,
}

impl Default for FsyncPolicy {
    fn default() -> Self {
        FsyncPolicy::None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TxLogError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("checksum mismatch at offset {offset}")]
    ChecksumMismatch { offset: u64 },
    #[error("entry too large at offset {offset}: {len} bytes (max {max})")]
    EntryTooLarge {
        offset: u64,
        len: usize,
        max: usize,
    },
    #[error("topic too long: {len} bytes (max {max})")]
    TopicTooLong { len: usize, max: usize },
    #[error("invalid utf8 in entry at offset {offset}")]
    Utf8 { offset: u64 },
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

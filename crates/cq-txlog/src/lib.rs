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
//! ## V2 (origin-tagged) bodies
//!
//! A V2 body is prefixed with a single `0x01` marker byte, followed by the
//! same fields plus an `origin` string inserted between `key` and `payload`:
//!
//! ```text
//!   marker      : u8 = 0x01
//!   sequence    : u64 BE
//!   timestamp_ms: u64 BE
//!   topic_len   : u16 BE  + topic
//!   key_len     : u16 BE  + key
//!   origin_len  : u16 BE  + origin   (≤ 256, may be empty)
//!   payload     : bytes
//! ```
//!
//! Legacy V1 bodies begin with the high byte of `sequence`, which is 0x00 in
//! practice, so the marker disambiguates the two formats on read. The
//! `origin` is the AMPS-style publisher id used for idempotent, convergent
//! cross-instance replication dedup.
//!
//! Empty `payload` is interpreted as a tombstone (key deletion).
//!
//! `crc32` is computed over the body. A mismatch on read aborts the replay
//! at that offset. A truncated tail (less than `frame_len` bytes available)
//! is treated as a clean truncation — the partial entry is discarded and
//! the reader stops, mimicking unclean-shutdown recovery in real systems.

pub mod reader;
pub mod segment;
pub mod snapshot;
pub mod writer;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const MAX_ENTRY_SIZE: usize = 16 * 1024 * 1024;
pub const MAX_TOPIC_LEN: usize = 1024;
pub const MAX_ORIGIN_LEN: usize = 256;

/// Body-format marker for V2 entries (origin-tagged). The first byte of a V2
/// body is this sentinel. Legacy V1 bodies begin with the high byte of the
/// `sequence` u64, which is effectively always 0x00 in practice (sequences fit
/// well under 2^56), so the marker is unambiguous on read.
pub const TXLOG_FORMAT_V2: u8 = 0x01;

/// Default segment size (256 MB), matching the ARCHITECTURE.md spec.
pub const DEFAULT_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct TxEntry {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub topic: String,
    pub key: String,
    /// Originating instance identity (AMPS-style publisher id). Empty for
    /// legacy V1 entries and for local writes that predate instance naming.
    pub origin: String,
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
    /// Group commit: every append is written straight through to the OS
    /// page cache (so it is immediately visible to readers tailing the
    /// segment — e.g. the replication shipper), but the expensive
    /// `fsync` is issued at most once per `Duration`. This is the
    /// fast-and-durable middle ground (AMPS-style group commit): it
    /// bounds data-loss-on-power-failure to roughly one interval's worth
    /// of writes while amortizing the costly fsync syscall across every
    /// append in that window. The first append after an interval lapses
    /// triggers the fsync, so a steady write stream fsyncs ~once per
    /// `Duration`; a clean shutdown (`sync`) always fsyncs regardless.
    Interval(Duration),
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
    #[error("origin too long: {len} bytes (max {max})")]
    OriginTooLong { len: usize, max: usize },
    #[error("invalid utf8 in entry at offset {offset}")]
    Utf8 { offset: u64 },
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

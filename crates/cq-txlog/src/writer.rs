//! Append-only segmented transaction log writer.
//!
//! Each topic owns a directory. The writer appends to the
//! highest-numbered segment until it would exceed `segment_size`, then
//! rolls over to a fresh `{n+1}.log`. Closed segments are immutable —
//! the natural unit of replication.

use crate::reader::TxLogReader;
use crate::segment::{list_segments, segment_path};
use crate::{
    now_ms, FsyncPolicy, TxLogError, DEFAULT_SEGMENT_SIZE, MAX_ENTRY_SIZE, MAX_ORIGIN_LEN,
    MAX_TOPIC_LEN, TXLOG_FORMAT_V2,
};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct TxLogWriter {
    dir: PathBuf,
    fsync: FsyncPolicy,
    segment_size: u64,
    current_id: u64,
    current_path: PathBuf,
    current_file: File,
    current_bytes: u64,
    entries_written: u64,
    max_sequence: u64,
    /// Wall-clock instant of the last `fsync`. Only meaningful under
    /// `FsyncPolicy::Interval`, where it gates group-commit: an append
    /// fsyncs only once `interval` has elapsed since this instant.
    last_fsync: Instant,
    /// When `true`, the active segment's blocks are reserved up front
    /// (`fallocate`/`F_PREALLOCATE`) so steady-state appends don't pay
    /// per-write extent-allocation cost. Best-effort — unsupported
    /// filesystems silently fall back to on-demand allocation. The file
    /// stays in append mode and its *logical* length still tracks the
    /// bytes actually written, so readers and recovery are unaffected.
    preallocate: bool,
    /// Physical byte offset up to which blocks are currently reserved for
    /// the active segment. `0` when nothing is reserved. Reset on
    /// rotation and after a tail-trim in `sync`.
    prealloc_end: u64,
    /// Optional archive directory. When set, sealed segments are
    /// moved here on rotation, keeping the live `dir` small. The
    /// archive directory is created on first rotation if missing.
    archive_dir: Option<PathBuf>,
    /// When `true`, sealed segments routed to `archive_dir` are
    /// zstd-compressed (renamed `.log.zst`). The reader transparently
    /// decompresses on read.
    archive_compress: bool,
}

impl TxLogWriter {
    /// Open (or create) a log directory with the default segment size.
    pub fn open(dir: impl AsRef<Path>, fsync: FsyncPolicy) -> Result<Self, TxLogError> {
        Self::open_with_segment_size(dir, fsync, DEFAULT_SEGMENT_SIZE)
    }

    /// Open with an explicit segment size. Tests use a small size so
    /// rotation paths can be exercised without writing hundreds of MB.
    /// Open with an optional archive directory: sealed segments are
    /// moved here on rotation (the live `dir` only ever holds the
    /// active segment + any segments not yet rotated past).
    pub fn open_with_archive(
        dir: impl AsRef<Path>,
        fsync: FsyncPolicy,
        segment_size: u64,
        archive_dir: Option<impl AsRef<Path>>,
    ) -> Result<Self, TxLogError> {
        let mut w = Self::open_with_segment_size(dir, fsync, segment_size)?;
        w.archive_dir = archive_dir.map(|p| p.as_ref().to_path_buf());
        Ok(w)
    }

    /// Same as `open_with_archive` but also compresses sealed
    /// segments with zstd. The active segment stays uncompressed
    /// (append-only writes); compression happens only at the
    /// rotation boundary. Cuts archive storage by 5–10× on
    /// repetitive payloads.
    pub fn open_with_archive_compressed(
        dir: impl AsRef<Path>,
        fsync: FsyncPolicy,
        segment_size: u64,
        archive_dir: impl AsRef<Path>,
    ) -> Result<Self, TxLogError> {
        let mut w = Self::open_with_segment_size(dir, fsync, segment_size)?;
        w.archive_dir = Some(archive_dir.as_ref().to_path_buf());
        w.archive_compress = true;
        Ok(w)
    }

    pub fn open_with_segment_size(
        dir: impl AsRef<Path>,
        fsync: FsyncPolicy,
        segment_size: u64,
    ) -> Result<Self, TxLogError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Discover existing segments to figure out where to resume.
        let existing = list_segments(&dir)?;
        let (current_id, max_sequence) = if let Some((last_id, _)) = existing.last() {
            // The local txlog only ever records *this* node's own writes,
            // appended in strictly increasing sequence order — replicated
            // foreign-origin entries are applied to the store but are never
            // written here. So the highest sequence lives in the
            // newest-numbered segment that holds any entry. Scan backward
            // from the last segment and stop at the first non-empty one,
            // rather than reading the entire (possibly multi-GB) log just
            // to seed the counter. This keeps startup O(one segment)
            // regardless of total log size — the difference between a
            // multi-minute and a near-instant restart on a large log.
            let mut max_seq = 0u64;
            for (_, path) in existing.iter().rev() {
                let mut reader = TxLogReader::open(path)?;
                let mut found = false;
                while let Some(entry) = reader.read_next()? {
                    found = true;
                    if entry.sequence > max_seq {
                        max_seq = entry.sequence;
                    }
                }
                if found {
                    break;
                }
            }
            (*last_id, max_seq)
        } else {
            (1, 0)
        };

        let current_path = segment_path(&dir, current_id);
        let current_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&current_path)?;
        let current_bytes = current_file.metadata()?.len();

        Ok(TxLogWriter {
            dir,
            fsync,
            segment_size,
            current_id,
            current_path,
            current_file,
            current_bytes,
            entries_written: 0,
            max_sequence,
            last_fsync: Instant::now(),
            preallocate: false,
            prealloc_end: 0,
            archive_dir: None,
            archive_compress: false,
        })
    }

    /// Enable (or disable) block preallocation for the active and all
    /// future segments. When enabling, the active segment's tail is
    /// reserved immediately. Preallocation is a throughput optimization
    /// only: a failure to reserve (unsupported FS, etc.) is logged and
    /// ignored, never surfaced as an error, because correctness does not
    /// depend on it. Returns `self` for chaining after an `open_*` call.
    pub fn with_preallocate(mut self, enabled: bool) -> Self {
        self.preallocate = enabled;
        if enabled {
            self.ensure_prealloc();
        }
        self
    }

    /// Reserve blocks for the active segment up to `segment_size` when
    /// preallocation is enabled and the current reservation doesn't
    /// already cover the whole segment. Cheap no-op once reserved.
    fn ensure_prealloc(&mut self) {
        if !self.preallocate || self.prealloc_end >= self.segment_size {
            return;
        }
        let len = self.segment_size.saturating_sub(self.current_bytes);
        if len == 0 {
            self.prealloc_end = self.segment_size;
            return;
        }
        match reserve_blocks(&self.current_file, self.current_bytes, len) {
            Ok(()) => self.prealloc_end = self.segment_size,
            Err(e) => {
                tracing::warn!(
                    path = %self.current_path.display(),
                    error = %e,
                    "txlog preallocation failed — falling back to on-demand allocation"
                );
                // Don't retry every append on a FS that doesn't support
                // it; treat the segment as "reserved" to suppress churn.
                self.prealloc_end = self.segment_size;
            }
        }
    }

    /// Release any reserved-but-unwritten blocks past the logical end of
    /// the active segment. Called when sealing a segment (rotation) and
    /// on clean shutdown so a sealed/closed file occupies only the space
    /// its real content needs. In append mode the logical length already
    /// equals `current_bytes`, so truncating to it frees only the
    /// preallocated tail.
    fn trim_prealloc_tail(&mut self) {
        if !self.preallocate || self.prealloc_end <= self.current_bytes {
            return;
        }
        if let Err(e) = self.current_file.set_len(self.current_bytes) {
            tracing::warn!(
                path = %self.current_path.display(),
                error = %e,
                "txlog preallocation tail-trim failed"
            );
        }
        self.prealloc_end = self.current_bytes;
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Compatibility shim — returns the path of the active segment.
    /// Useful for older callers that referred to "the log file"; new
    /// callers should use `dir()` and iterate via the reader.
    pub fn path(&self) -> &Path {
        &self.current_path
    }

    pub fn current_segment(&self) -> u64 {
        self.current_id
    }

    pub fn entries_written(&self) -> u64 {
        self.entries_written
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    /// Force a segment rotation now, regardless of current segment
    /// size. Used by the admin endpoint so operators can cut a fresh
    /// segment on demand (e.g., before backup or to bound replay
    /// latency on a hot topic).
    pub fn force_rotate(&mut self) -> Result<(), TxLogError> {
        self.rotate()
    }

    fn rotate(&mut self) -> Result<(), TxLogError> {
        // Flush current segment so a crash leaves it consistent.
        self.current_file.flush()?;
        // Release any reserved-but-unwritten tail so the sealed segment
        // occupies only its real content (and never carries trailing
        // zero padding that a strict reader would reject on a non-active
        // segment). Must happen before the sync/seal below.
        self.trim_prealloc_tail();
        if self.fsync != FsyncPolicy::None {
            self.current_file.sync_all()?;
        }
        // Capture the path of the segment we're about to seal before
        // we overwrite `current_path` with the new one.
        let sealed_id = self.current_id;
        let sealed_path = self.current_path.clone();

        let next_id = self.current_id + 1;
        let next_path = segment_path(&self.dir, next_id);
        let next_file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .read(true)
            .open(&next_path)?;
        self.current_id = next_id;
        self.current_path = next_path;
        self.current_file = next_file;
        self.current_bytes = 0;
        // Fresh segment owns no reservation yet; reserve its blocks now
        // so the first appends into it are already cheap.
        self.prealloc_end = 0;
        self.ensure_prealloc();

        // Move the just-sealed segment to the archive directory if
        // one is configured. We do this *after* opening the new
        // segment so a recovery scan during rotation always sees a
        // complete chain.
        if let Some(archive) = &self.archive_dir {
            std::fs::create_dir_all(archive)?;
            if self.archive_compress {
                // Stream-compress with zstd. Level 3 is a good
                // size/cpu balance for log-shaped data.
                let dest = crate::segment::segment_zstd_path(archive, sealed_id);
                let src = std::fs::File::open(&sealed_path)?;
                let dst = std::fs::File::create(&dest)?;
                let mut encoder = zstd::stream::write::Encoder::new(dst, 3)?;
                let mut reader = std::io::BufReader::new(src);
                std::io::copy(&mut reader, &mut encoder)?;
                encoder.finish()?;
                std::fs::remove_file(&sealed_path)?;
                tracing::info!(
                    from = %sealed_path.display(),
                    to = %dest.display(),
                    "Archived + compressed sealed segment"
                );
            } else {
                let dest = segment_path(archive, sealed_id);
                if let Err(e) = std::fs::rename(&sealed_path, &dest) {
                    // EXDEV == 18 on Linux and macOS: cross-device
                    // link. Fall back to copy+delete.
                    if e.raw_os_error() == Some(18) {
                        std::fs::copy(&sealed_path, &dest)?;
                        std::fs::remove_file(&sealed_path)?;
                    } else {
                        return Err(e.into());
                    }
                }
                tracing::info!(
                    from = %sealed_path.display(),
                    to = %dest.display(),
                    "Archived sealed segment"
                );
            }
        }

        tracing::info!(
            dir = %self.dir.display(),
            segment = next_id,
            "Rotated to new segment"
        );
        Ok(())
    }

    /// Append a single entry. `payload` may be empty to record a
    /// tombstone. `sequence` must be strictly greater than every
    /// previously appended sequence for this writer.
    ///
    /// Writes a legacy V1 body (no origin). Equivalent to
    /// `append_with_origin(sequence, topic, key, "", payload)`.
    pub fn append(
        &mut self,
        sequence: u64,
        topic: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<(), TxLogError> {
        self.append_with_origin(sequence, topic, key, "", payload)
    }

    /// Append a single entry stamped with an originating instance id.
    ///
    /// When `origin` is empty, a legacy V1 body is written for backward
    /// compatibility (so older readers and existing tests stay valid). When
    /// `origin` is non-empty, a V2 (origin-tagged) body is written.
    pub fn append_with_origin(
        &mut self,
        sequence: u64,
        topic: &str,
        key: &str,
        origin: &str,
        payload: &[u8],
    ) -> Result<(), TxLogError> {
        self.append_with_origin_format(
            sequence,
            topic,
            key,
            origin,
            payload,
            crate::PayloadFormat::Json,
        )
    }

    /// As [`Self::append_with_origin`], but stamps the payload encoding. A
    /// `JsonDelta` payload (sparse `{key + changed fields}`) is written with
    /// the `0x03` marker and always uses the V2 body layout so the marker is
    /// present for the reader to detect.
    pub fn append_with_origin_format(
        &mut self,
        sequence: u64,
        topic: &str,
        key: &str,
        origin: &str,
        payload: &[u8],
        format: crate::PayloadFormat,
    ) -> Result<(), TxLogError> {
        let delta = format == crate::PayloadFormat::JsonDelta;
        if topic.len() > MAX_TOPIC_LEN {
            return Err(TxLogError::TopicTooLong {
                len: topic.len(),
                max: MAX_TOPIC_LEN,
            });
        }
        if origin.len() > MAX_ORIGIN_LEN {
            return Err(TxLogError::OriginTooLong {
                len: origin.len(),
                max: MAX_ORIGIN_LEN,
            });
        }
        let topic_bytes = topic.as_bytes();
        let key_bytes = key.as_bytes();
        let origin_bytes = origin.as_bytes();
        // Delta payloads always use the V2 layout so their `0x03` marker is
        // present for the reader.
        let v2 = delta || !origin.is_empty();

        let body_len = if v2 {
            1 + 8 + 8 + 2 + topic_bytes.len() + 2 + key_bytes.len() + 2 + origin_bytes.len()
                + payload.len()
        } else {
            8 + 8 + 2 + topic_bytes.len() + 2 + key_bytes.len() + payload.len()
        };
        if body_len > MAX_ENTRY_SIZE {
            return Err(TxLogError::EntryTooLarge {
                offset: self.current_bytes,
                len: body_len,
                max: MAX_ENTRY_SIZE,
            });
        }

        let frame_len = (4 + 4 + body_len) as u64;

        // Rotate before writing if appending would push us past the limit.
        // Allow the very first entry of a fresh segment regardless of size
        // so individual oversized-but-≤MAX_ENTRY_SIZE messages still fit.
        if self.current_bytes > 0 && self.current_bytes + frame_len > self.segment_size {
            self.rotate()?;
        }

        // Ensure the active segment's blocks are reserved. No-op unless
        // preallocation is enabled and the reservation was dropped (e.g.
        // after a mid-stream `sync` tail-trim).
        self.ensure_prealloc();

        // Build the framed bytes in a single allocation: reserve the
        // 8-byte header (len + crc), append the body in place, then patch
        // the header once the body length and CRC are known. Avoids the
        // previous body→frame copy on every append.
        let mut frame = Vec::with_capacity(4 + 4 + body_len);
        frame.extend_from_slice(&[0u8; 8]); // header placeholder: len(4) + crc(4)
        if v2 {
            frame.push(if delta {
                crate::TXLOG_FORMAT_V2_DELTA
            } else {
                TXLOG_FORMAT_V2
            });
        }
        frame.extend_from_slice(&sequence.to_be_bytes());
        frame.extend_from_slice(&now_ms().to_be_bytes());
        frame.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        frame.extend_from_slice(topic_bytes);
        frame.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        frame.extend_from_slice(key_bytes);
        if v2 {
            frame.extend_from_slice(&(origin_bytes.len() as u16).to_be_bytes());
            frame.extend_from_slice(origin_bytes);
        }
        frame.extend_from_slice(payload);

        let body_actual_len = frame.len() - 8;
        let crc = crc32fast::hash(&frame[8..]);
        frame[0..4].copy_from_slice(&(body_actual_len as u32).to_be_bytes());
        frame[4..8].copy_from_slice(&crc.to_be_bytes());

        // The write goes straight through to the OS page cache — no
        // userspace buffering — so the entry is immediately visible to
        // any reader tailing this segment (the replication shipper opens
        // a fresh reader every poll tick and expects to see the latest
        // bytes). Durability is governed separately by `fsync`.
        self.current_file.write_all(&frame)?;
        match self.fsync {
            FsyncPolicy::EveryWrite => {
                self.current_file.sync_data()?;
            }
            FsyncPolicy::Interval(interval) => {
                // Group commit: fsync at most once per interval. The
                // first append after the interval lapses pays the cost
                // on behalf of every append buffered in the OS since the
                // last fsync.
                if self.last_fsync.elapsed() >= interval {
                    self.current_file.sync_data()?;
                    self.last_fsync = Instant::now();
                }
            }
            FsyncPolicy::None => {}
        }

        self.current_bytes += frame.len() as u64;
        self.entries_written += 1;
        if sequence > self.max_sequence {
            self.max_sequence = sequence;
        }
        Ok(())
    }

    /// Delete sealed segments whose id is strictly below `cutoff`,
    /// clamped so the active segment is never deleted. Returns the
    /// number of segment files removed.
    ///
    /// This is the reclaim primitive behind `snapshot_reclaim`: it is
    /// only safe to call with a `cutoff` that a *crash-durable*
    /// artifact (the `snapshot.bin` checkpoint) fully covers —
    /// recovery must never need a pruned segment. Archived copies
    /// (when `archive_dir` is configured) are not touched: rotation
    /// already moved sealed segments there, and the archive is the
    /// long-term retention tier.
    pub fn prune_segments_below(&mut self, cutoff: u64) -> Result<u64, TxLogError> {
        let cutoff = cutoff.min(self.current_id);
        let mut deleted = 0u64;
        for (id, path) in list_segments(&self.dir)? {
            if id >= cutoff {
                continue;
            }
            // Best-effort per file: a segment we can't delete (perms,
            // stale NFS handle) is logged and retried on the next
            // reclaim rather than aborting the remaining deletions.
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    deleted += 1;
                    tracing::info!(
                        path = %path.display(),
                        segment = id,
                        cutoff,
                        "Pruned sealed txlog segment"
                    );
                }
                Err(e) => tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "Failed to prune sealed txlog segment; will retry on next reclaim"
                ),
            }
        }
        Ok(deleted)
    }

    /// Flush + fsync the current segment. Useful before clean shutdown
    /// regardless of the configured policy.
    pub fn sync(&mut self) -> Result<(), TxLogError> {
        self.current_file.flush()?;
        // Drop any reserved-but-unwritten tail so a clean shutdown leaves
        // the file sized to its real content (no trailing reserved
        // blocks). If the writer keeps running afterwards, the next
        // append re-reserves via `ensure_prealloc`.
        self.trim_prealloc_tail();
        self.current_file.sync_all()?;
        self.last_fsync = Instant::now();
        Ok(())
    }
}

/// Reserve disk blocks for `file` covering `[offset, offset + len)`
/// without changing the file's logical length, so subsequent appends
/// into that range don't trigger per-write extent allocation.
///
/// Implemented with `fallocate(FALLOC_FL_KEEP_SIZE)` on Linux and
/// `fcntl(F_PREALLOCATE)` on macOS. On platforms (or filesystems)
/// without support this is a no-op returning `Ok(())` — preallocation
/// is purely a throughput optimization, never a correctness requirement.
#[cfg(target_os = "linux")]
fn reserve_blocks(file: &File, offset: u64, len: u64) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    let ret = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if ret == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    // EOPNOTSUPP / ENOSYS: the filesystem doesn't support fallocate.
    // Treat as a benign no-op — writes will allocate on demand.
    match err.raw_os_error() {
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) => Ok(()),
        _ => Err(err),
    }
}

#[cfg(target_os = "macos")]
fn reserve_blocks(file: &File, _offset: u64, len: u64) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;
    if len == 0 {
        return Ok(());
    }
    let fd = file.as_raw_fd();
    // F_PEOFPOSMODE allocates `fst_length` bytes starting at the physical
    // end of the file (so `offset` is implied by the current EOF — which,
    // in append mode, is exactly `current_bytes`). Try a contiguous
    // allocation first, then fall back to a possibly-fragmented one.
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATECONTIG,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len as libc::off_t,
        fst_bytesalloc: 0,
    };
    let mut ret = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
    if ret == -1 {
        store.fst_flags = libc::F_ALLOCATEALL;
        ret = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &mut store) };
    }
    if ret == -1 {
        let err = std::io::Error::last_os_error();
        // ENOTSUP: filesystem can't preallocate — benign no-op.
        return match err.raw_os_error() {
            Some(libc::ENOTSUP) => Ok(()),
            _ => Err(err),
        };
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn reserve_blocks(_file: &File, _offset: u64, _len: u64) -> std::io::Result<()> {
    // No portable preallocation primitive — on-demand allocation only.
    Ok(())
}

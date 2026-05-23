//! Append-only segmented transaction log writer.
//!
//! Each topic owns a directory. The writer appends to the
//! highest-numbered segment until it would exceed `segment_size`, then
//! rolls over to a fresh `{n+1}.log`. Closed segments are immutable —
//! the natural unit of replication.

use crate::reader::TxLogReader;
use crate::segment::{list_segments, segment_path};
use crate::{now_ms, FsyncPolicy, TxLogError, DEFAULT_SEGMENT_SIZE, MAX_ENTRY_SIZE, MAX_TOPIC_LEN};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

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
            let mut max_seq = 0u64;
            // Scan the whole directory for the highest sequence — the
            // current segment may have been only partially written, or
            // earlier segments may carry higher sequences due to
            // out-of-order writes (the writer guarantees in-order so this
            // is conservative).
            let mut reader = TxLogReader::open(&dir)?;
            while let Some(entry) = reader.read_next()? {
                if entry.sequence > max_seq {
                    max_seq = entry.sequence;
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
            archive_dir: None,
            archive_compress: false,
        })
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
        if self.fsync == FsyncPolicy::EveryWrite {
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
    pub fn append(
        &mut self,
        sequence: u64,
        topic: &str,
        key: &str,
        payload: &[u8],
    ) -> Result<(), TxLogError> {
        if topic.len() > MAX_TOPIC_LEN {
            return Err(TxLogError::TopicTooLong {
                len: topic.len(),
                max: MAX_TOPIC_LEN,
            });
        }
        let topic_bytes = topic.as_bytes();
        let key_bytes = key.as_bytes();

        let body_len = 8 + 8 + 2 + topic_bytes.len() + 2 + key_bytes.len() + payload.len();
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

        let mut body = Vec::with_capacity(body_len);
        body.extend_from_slice(&sequence.to_be_bytes());
        body.extend_from_slice(&now_ms().to_be_bytes());
        body.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(topic_bytes);
        body.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        body.extend_from_slice(key_bytes);
        body.extend_from_slice(payload);

        let crc = crc32fast::hash(&body);

        let mut frame = Vec::with_capacity(4 + 4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&crc.to_be_bytes());
        frame.extend_from_slice(&body);

        self.current_file.write_all(&frame)?;
        if self.fsync == FsyncPolicy::EveryWrite {
            self.current_file.sync_data()?;
        }

        self.current_bytes += frame.len() as u64;
        self.entries_written += 1;
        if sequence > self.max_sequence {
            self.max_sequence = sequence;
        }
        Ok(())
    }

    /// Flush + fsync the current segment. Useful before clean shutdown
    /// regardless of the configured policy.
    pub fn sync(&mut self) -> Result<(), TxLogError> {
        self.current_file.flush()?;
        self.current_file.sync_all()?;
        Ok(())
    }
}

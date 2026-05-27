//! Sequential transaction log reader over a directory of segments.
//!
//! Opens every segment in `dir` ordered by id and reads them
//! end-to-end. Each segment is independently corruption-tolerant on the
//! tail: a partial entry from a crashed write at the *active* segment
//! is treated as clean truncation. Mid-stream checksum mismatches abort
//! the replay.

use crate::segment::list_segments;
use crate::{TxEntry, TxLogError, MAX_ENTRY_SIZE};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

/// Iterates entries across every segment in a directory, in segment-id
/// order.
pub struct TxLogReader {
    segments: Vec<(u64, PathBuf)>,
    cursor: usize,
    current: Option<SegmentReader>,
    /// Total bytes read so far across all segments. Stable address for
    /// error reporting.
    global_offset: u64,
}

impl TxLogReader {
    /// Open a reader over the log directory `dir`. Single-file callers
    /// can keep passing a file path: if `dir` is a regular file with
    /// the `.log` extension, it's treated as a one-segment log.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, TxLogError> {
        Self::open_with_archive(dir, Option::<&Path>::None)
    }

    /// Open with an optional archive directory: segments from
    /// `archive_dir` are read first (older), then segments from
    /// `dir`. Deduped by segment id so a segment that's mid-archive
    /// (present in both during a rotation) isn't read twice.
    pub fn open_with_archive(
        dir: impl AsRef<Path>,
        archive_dir: Option<impl AsRef<Path>>,
    ) -> Result<Self, TxLogError> {
        let p = dir.as_ref();
        let live = if p.is_file() {
            vec![(1u64, p.to_path_buf())]
        } else {
            list_segments(p)?
        };
        let archived = match archive_dir {
            Some(a) => {
                let ap = a.as_ref();
                if ap.is_dir() {
                    list_segments(ap)?
                } else {
                    Vec::new()
                }
            }
            None => Vec::new(),
        };
        // Merge: dedup by segment id (live wins — the active segment
        // can transiently exist in both during a rotation race).
        use std::collections::BTreeMap;
        let mut by_id: BTreeMap<u64, PathBuf> = BTreeMap::new();
        for (id, path) in archived {
            by_id.insert(id, path);
        }
        for (id, path) in live {
            by_id.insert(id, path);
        }
        let segments: Vec<(u64, PathBuf)> = by_id.into_iter().collect();
        Ok(TxLogReader {
            segments,
            cursor: 0,
            current: None,
            global_offset: 0,
        })
    }

    /// Skip every segment whose first entry's id is `< min_segment_id`.
    /// Cheap fast-forward used by the replication shipper.
    pub fn skip_to_segment(&mut self, min_segment_id: u64) {
        while self.cursor < self.segments.len()
            && self.segments[self.cursor].0 < min_segment_id
        {
            self.cursor += 1;
        }
        self.current = None;
    }

    /// Read every entry from the start of the directory. Stops at the
    /// end of the last segment or a tail-truncation in the last segment.
    pub fn read_all(&mut self) -> Result<Vec<TxEntry>, TxLogError> {
        let mut entries = Vec::new();
        while let Some(e) = self.read_next()? {
            entries.push(e);
        }
        Ok(entries)
    }

    /// Read the next entry, advancing across segment boundaries.
    pub fn read_next(&mut self) -> Result<Option<TxEntry>, TxLogError> {
        loop {
            if self.current.is_none() {
                if self.cursor >= self.segments.len() {
                    return Ok(None);
                }
                let (_, ref path) = self.segments[self.cursor];
                self.current = Some(SegmentReader::open(path)?);
            }
            let is_last_segment = self.cursor + 1 >= self.segments.len();
            let reader = self.current.as_mut().expect("just set");
            match reader.read_next(is_last_segment)? {
                Some(entry) => {
                    self.global_offset += entry.frame_bytes;
                    return Ok(Some(entry.entry));
                }
                None => {
                    // End of this segment. Advance.
                    self.current = None;
                    self.cursor += 1;
                }
            }
        }
    }

    pub fn offset(&self) -> u64 {
        self.global_offset
    }
}

struct SegmentReader {
    /// `Box<dyn Read>` so the same code path handles plain `.log`
    /// files and zstd-compressed `.log.zst` files. The trade-off is
    /// a single virtual call per read, which is negligible alongside
    /// the syscall + checksum cost.
    inner: Box<dyn Read + Send>,
    offset: u64,
    path: PathBuf,
}

struct RawEntry {
    entry: TxEntry,
    frame_bytes: u64,
}

impl SegmentReader {
    fn open(path: &Path) -> Result<Self, TxLogError> {
        let file = File::open(path)?;
        let inner: Box<dyn Read + Send> = if crate::segment::is_compressed_segment(path) {
            // zstd::Decoder consumes the file as a stream. The
            // internal buffering inside `zstd::stream::read::Decoder`
            // is sized appropriately for log replay.
            Box::new(zstd::stream::read::Decoder::new(file)?)
        } else {
            Box::new(BufReader::new(file))
        };
        Ok(SegmentReader {
            inner,
            offset: 0,
            path: path.to_path_buf(),
        })
    }

    /// Read the next entry from this segment. Returns `Ok(None)` on
    /// clean EOF. On a truncated tail the behavior depends on
    /// `is_active_segment`: the active (last) segment can legitimately
    /// have a truncated tail from an interrupted write; an inactive
    /// segment cannot, so we surface that as a checksum-like error.
    fn read_next(
        &mut self,
        is_active_segment: bool,
    ) -> Result<Option<RawEntry>, TxLogError> {
        let mut header = [0u8; 8];
        match read_exact_or_eof(&mut self.inner, &mut header)? {
            ReadResult::Eof => return Ok(None),
            ReadResult::Partial => {
                if is_active_segment {
                    tracing::warn!(
                        path = %self.path.display(),
                        offset = self.offset,
                        "Truncated entry header in active segment — treating as EOF"
                    );
                    return Ok(None);
                }
                return Err(TxLogError::ChecksumMismatch { offset: self.offset });
            }
            ReadResult::Full => {}
        }

        let frame_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let expected_crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

        if frame_len > MAX_ENTRY_SIZE {
            // Trailing garbage in the active segment (e.g. filesystem
            // padding after a power-loss, or a torn write that
            // happened to land on bytes that decode to an impossibly
            // large length) can fail this check. Treating it as
            // torn-tail EOF mirrors the "Partial" arm above and lets
            // recovery surface every COMPLETE record before the
            // garbage. Sealed (non-active) segments cannot legitimately
            // contain bad lengths — surface the error to the caller.
            if is_active_segment {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    frame_len,
                    max = MAX_ENTRY_SIZE,
                    "Oversized frame length in active segment — treating as torn-tail EOF"
                );
                return Ok(None);
            }
            return Err(TxLogError::EntryTooLarge {
                offset: self.offset,
                len: frame_len,
                max: MAX_ENTRY_SIZE,
            });
        }

        // A real entry always has a positive body (header carries
        // sequence + topic length + key length + payload length all
        // > 0 each). A `frame_len = 0` decoded from the wire is
        // unambiguously garbage (e.g. zero-padded sectors after a
        // power-loss truncated the live segment). Treat as torn-tail
        // EOF on the active segment.
        if frame_len == 0 {
            if is_active_segment {
                tracing::warn!(
                    path = %self.path.display(),
                    offset = self.offset,
                    "Zero-length frame in active segment — treating as torn-tail EOF"
                );
                return Ok(None);
            }
            return Err(TxLogError::ChecksumMismatch { offset: self.offset });
        }

        let mut body = vec![0u8; frame_len];
        match read_exact_or_eof(&mut self.inner, &mut body)? {
            ReadResult::Full => {}
            _ => {
                if is_active_segment {
                    return Ok(None);
                }
                return Err(TxLogError::ChecksumMismatch { offset: self.offset });
            }
        }

        let actual_crc = crc32fast::hash(&body);
        if actual_crc != expected_crc {
            // CRC mismatch on a plausibly-sized frame is ambiguous:
            // it could be torn-tail garbage OR mid-log data
            // corruption. We surface as an error rather than swallow,
            // because the latter is a real data-integrity violation
            // and the former is caught by the oversized + zero-length
            // guards above (which match the actual shapes trailing
            // garbage takes on a torn write).
            return Err(TxLogError::ChecksumMismatch {
                offset: self.offset,
            });
        }

        let entry = parse_body(&body, self.offset)?;
        let frame_bytes = 8 + frame_len as u64;
        self.offset += frame_bytes;
        Ok(Some(RawEntry { entry, frame_bytes }))
    }
}

enum ReadResult {
    Full,
    Partial,
    Eof,
}

fn read_exact_or_eof(r: &mut impl Read, buf: &mut [u8]) -> Result<ReadResult, TxLogError> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..])? {
            0 => {
                return Ok(if read == 0 {
                    ReadResult::Eof
                } else {
                    ReadResult::Partial
                });
            }
            n => read += n,
        }
    }
    Ok(ReadResult::Full)
}

fn parse_body(body: &[u8], offset: u64) -> Result<TxEntry, TxLogError> {
    let mut cur = 0usize;
    let take = |cur: &mut usize, n: usize| -> Result<&[u8], TxLogError> {
        if *cur + n > body.len() {
            return Err(TxLogError::ChecksumMismatch { offset });
        }
        let slice = &body[*cur..*cur + n];
        *cur += n;
        Ok(slice)
    };

    let seq_bytes = take(&mut cur, 8)?;
    let sequence = u64::from_be_bytes([
        seq_bytes[0], seq_bytes[1], seq_bytes[2], seq_bytes[3], seq_bytes[4], seq_bytes[5],
        seq_bytes[6], seq_bytes[7],
    ]);

    let ts_bytes = take(&mut cur, 8)?;
    let timestamp_ms = u64::from_be_bytes([
        ts_bytes[0], ts_bytes[1], ts_bytes[2], ts_bytes[3], ts_bytes[4], ts_bytes[5], ts_bytes[6],
        ts_bytes[7],
    ]);

    let topic_len_bytes = take(&mut cur, 2)?;
    let topic_len = u16::from_be_bytes([topic_len_bytes[0], topic_len_bytes[1]]) as usize;
    let topic_bytes = take(&mut cur, topic_len)?;
    let topic = std::str::from_utf8(topic_bytes)
        .map_err(|_| TxLogError::Utf8 { offset })?
        .to_string();

    let key_len_bytes = take(&mut cur, 2)?;
    let key_len = u16::from_be_bytes([key_len_bytes[0], key_len_bytes[1]]) as usize;
    let key_bytes = take(&mut cur, key_len)?;
    let key = std::str::from_utf8(key_bytes)
        .map_err(|_| TxLogError::Utf8 { offset })?
        .to_string();

    let payload = body[cur..].to_vec();

    Ok(TxEntry {
        sequence,
        timestamp_ms,
        topic,
        key,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::segment_path;
    use crate::writer::TxLogWriter;
    use crate::FsyncPolicy;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_single_entry() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            w.append(1, "/trades", "AAPL", b"{\"price\":150}").unwrap();
            w.sync().unwrap();
        }

        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[0].topic, "/trades");
        assert_eq!(entries[0].key, "AAPL");
        assert_eq!(entries[0].payload, b"{\"price\":150}");
        assert!(!entries[0].is_tombstone());
    }

    #[test]
    fn roundtrip_many_entries_preserves_order() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            for i in 0..50u64 {
                let payload = format!("{{\"id\":{}}}", i);
                w.append(i + 1, "/orders", &format!("O{:03}", i), payload.as_bytes())
                    .unwrap();
            }
            w.sync().unwrap();
        }

        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 50);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.key, format!("O{:03}", i));
            assert_eq!(e.sequence, (i as u64) + 1);
        }
    }

    #[test]
    fn tombstone_roundtrips() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            w.append(1, "/trades", "AAPL", b"{\"p\":1}").unwrap();
            w.append(2, "/trades", "AAPL", &[]).unwrap(); // tombstone
            w.sync().unwrap();
        }

        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(!entries[0].is_tombstone());
        assert!(entries[1].is_tombstone());
        assert_eq!(entries[1].sequence, 2);
    }

    #[test]
    fn truncated_tail_treated_as_eof() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            w.append(1, "/trades", "AAPL", b"{\"p\":1}").unwrap();
            w.append(2, "/trades", "MSFT", b"{\"p\":2}").unwrap();
            w.sync().unwrap();
        }

        // Truncate the active segment's last 5 bytes.
        let active = segment_path(dir.path(), 1);
        let mut all = std::fs::read(&active).unwrap();
        all.truncate(all.len() - 5);
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&active)
            .unwrap();
        f.write_all(&all).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        // Active segment's truncated tail is treated as EOF.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key, "AAPL");
    }

    #[test]
    fn corrupted_checksum_errors() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            w.append(1, "/trades", "AAPL", b"{\"p\":1}").unwrap();
            w.sync().unwrap();
        }

        // Flip a byte inside the body — checksum will fail on read.
        let active = segment_path(dir.path(), 1);
        let mut all = std::fs::read(&active).unwrap();
        let last = all.len() - 1;
        all[last] ^= 0xff;
        std::fs::write(&active, &all).unwrap();

        let err = TxLogReader::open(dir.path()).unwrap().read_all().unwrap_err();
        match err {
            TxLogError::ChecksumMismatch { .. } => {}
            other => panic!("unexpected error: {:?}", other),
        }
    }

    #[test]
    fn reopen_appends_after_existing_content() {
        let dir = tempdir().unwrap();

        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            w.append(1, "/trades", "AAPL", b"{\"p\":1}").unwrap();
            w.sync().unwrap();
        }
        {
            let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
            assert_eq!(w.max_sequence(), 1, "max_sequence recovered on open");
            w.append(2, "/trades", "MSFT", b"{\"p\":2}").unwrap();
            w.sync().unwrap();
        }

        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, "AAPL");
        assert_eq!(entries[0].sequence, 1);
        assert_eq!(entries[1].key, "MSFT");
        assert_eq!(entries[1].sequence, 2);
    }

    #[test]
    fn rotates_when_segment_size_exceeded() {
        let dir = tempdir().unwrap();

        // Tiny segment size so each entry fills its own file.
        let mut w =
            TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, 64).unwrap();
        for i in 1..=5u64 {
            // Each entry is well over 64 bytes once headers + body are included.
            w.append(i, "/t", &format!("k{}", i), b"sufficient payload bytes")
                .unwrap();
        }
        w.sync().unwrap();

        // Expect multiple segment files.
        let segs = crate::segment::list_segments(dir.path()).unwrap();
        assert!(segs.len() >= 3, "expected rotation, got {} segment(s)", segs.len());

        // Reader iterates across them in order.
        let entries = TxLogReader::open(dir.path()).unwrap().read_all().unwrap();
        assert_eq!(entries.len(), 5);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.sequence, (i as u64) + 1);
            assert_eq!(e.key, format!("k{}", i + 1));
        }
    }

    #[test]
    fn compressed_archive_round_trips() {
        let root = tempdir().unwrap();
        let live = root.path().join("live");
        let archive = root.path().join("archive");
        std::fs::create_dir_all(&live).unwrap();

        let mut w = TxLogWriter::open_with_archive_compressed(
            &live,
            FsyncPolicy::None,
            64,
            &archive,
        )
        .unwrap();
        for i in 1..=10u64 {
            w.append(i, "/t", &format!("k{i}"), b"sufficient payload bytes")
                .unwrap();
        }
        w.sync().unwrap();

        // Archive contents should be `.log.zst` files, not `.log`.
        let archive_entries: Vec<_> = std::fs::read_dir(&archive)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().to_string()))
            .collect();
        assert!(
            archive_entries.iter().any(|n| n.ends_with(".log.zst")),
            "expected at least one .log.zst in archive, got {archive_entries:?}"
        );
        assert!(
            !archive_entries.iter().any(|n| n.ends_with(".log") && !n.ends_with(".log.zst")),
            "compressed archive should not contain raw .log files"
        );

        // Reader walks both dirs and transparently decompresses.
        let entries = TxLogReader::open_with_archive(&live, Some(&archive))
            .unwrap()
            .read_all()
            .unwrap();
        assert_eq!(entries.len(), 10);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.sequence, (i as u64) + 1);
            assert_eq!(e.key, format!("k{}", i + 1));
        }
    }

    #[test]
    fn archive_dir_holds_sealed_segments_and_reader_finds_them() {
        // Live dir + archive dir. Write enough to cause rotation;
        // sealed segments should end up in archive, the active one
        // stays in live. A reader opened with both finds every
        // entry in order.
        let root = tempdir().unwrap();
        let live = root.path().join("live");
        let archive = root.path().join("archive");
        std::fs::create_dir_all(&live).unwrap();

        let mut w = TxLogWriter::open_with_archive(
            &live,
            FsyncPolicy::None,
            64,
            Some(&archive),
        )
        .unwrap();
        for i in 1..=8u64 {
            w.append(i, "/t", &format!("k{}", i), b"sufficient payload bytes")
                .unwrap();
        }
        w.sync().unwrap();

        // After rotation, live should hold only the active segment;
        // archive holds the rest.
        let live_segs = crate::segment::list_segments(&live).unwrap();
        let archive_segs = crate::segment::list_segments(&archive).unwrap();
        assert_eq!(live_segs.len(), 1, "expected one live segment, got {live_segs:?}");
        assert!(
            archive_segs.len() >= 1,
            "expected at least one archived segment, got {archive_segs:?}"
        );

        // Reader walks both dirs in order; replay must produce all 8.
        let entries = TxLogReader::open_with_archive(&live, Some(&archive))
            .unwrap()
            .read_all()
            .unwrap();
        assert_eq!(entries.len(), 8);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.sequence, (i as u64) + 1);
        }
    }

    #[test]
    fn skip_to_segment_fast_forwards() {
        let dir = tempdir().unwrap();
        let mut w =
            TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, 64).unwrap();
        for i in 1..=10u64 {
            w.append(i, "/t", &format!("k{}", i), b"sufficient payload bytes")
                .unwrap();
        }
        w.sync().unwrap();
        let segs = crate::segment::list_segments(dir.path()).unwrap();
        let last_id = segs.last().unwrap().0;
        assert!(last_id >= 3);

        // Skip past every segment except the last.
        let mut r = TxLogReader::open(dir.path()).unwrap();
        r.skip_to_segment(last_id);
        let tail: Vec<TxEntry> = r.read_all().unwrap();
        // The last segment's entries should still be at the end of the stream.
        assert!(!tail.is_empty());
        let max_seq_seen = tail.iter().map(|e| e.sequence).max().unwrap();
        assert_eq!(max_seq_seen, 10);
    }
}

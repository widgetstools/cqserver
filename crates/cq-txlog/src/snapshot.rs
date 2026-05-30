//! SOW snapshot file — a point-in-time checkpoint of a topic's live store.
//!
//! # Why
//!
//! The transaction log is pure append-only with no compaction: a clean
//! restart replays *every* entry ever written, which on a hot topic with a
//! multi-gigabyte log turns startup into a multi-minute stall. A snapshot
//! is the AMPS-style fix — periodically (and on graceful shutdown) we write
//! the *current* live SOW to a single file together with the highest
//! sequence it reflects. On restart we load the snapshot to rebuild the
//! store in one pass, then replay only the txlog **tail** (entries with
//! `sequence > snapshot.sequence`). Superseded segments can then be
//! reclaimed.
//!
//! # File format
//!
//! ```text
//! ┌────────────┬─────────┬──────────┬─────────┬──────────────────────────┐
//! │ magic (8)  │ ver u8  │ body_len │ crc32   │ body (body_len bytes)    │
//! │ "CQSNAP\1\0"│         │ u64 BE   │ u32 BE  │ (crc is over body only)  │
//! └────────────┴─────────┴──────────┴─────────┴──────────────────────────┘
//!
//! body:
//!   sequence      : u64 BE   -- topic's last_applied_sequence at snapshot
//!   segment_id    : u64 BE   -- active txlog segment at snapshot time
//!   origin_count  : u32 BE
//!     repeated:  origin_len u16 BE + origin bytes + highwater u64 BE
//!   row_count     : u64 BE
//!     repeated:  payload_len u32 BE + payload bytes  (JSON object)
//! ```
//!
//! `segment_id` is the txlog segment the writer was appending to when the
//! snapshot was taken. No entry with `sequence > snapshot.sequence` can live
//! in an *earlier* segment, so recovery fast-forwards the reader to this
//! segment and replays only from there — the older (fully-covered) segments
//! are skipped entirely instead of being read and deduped frame by frame.
//!
//! Each row payload is the *same* self-describing JSON-object byte string
//! the txlog stores for an upsert, so applying a snapshot reuses the topic's
//! existing JSON→columns replay machinery — no separate columnar codec to
//! keep in lockstep with the store layout.
//!
//! # Durability & recovery posture
//!
//! Writes are atomic: body goes to `snapshot.bin.tmp`, is fsynced, then
//! renamed over `snapshot.bin` (rename is atomic on POSIX), and the
//! directory is fsynced so the rename itself survives a crash. A snapshot
//! that fails any integrity check on load (bad magic / version / CRC / short
//! read) is treated as **absent** — recovery falls back to a full txlog
//! replay, which is slower but always correct. A snapshot can therefore
//! never *lose* committed data; at worst it fails to accelerate startup.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// 8-byte file magic. The trailing `\x01\x00` is decorative; the real
/// version gate is the explicit `version` byte that follows.
const MAGIC: &[u8; 8] = b"CQSNAP\x01\x00";

/// On-disk format version. Bump when the body layout changes; an older
/// reader rejects a newer file (falls back to replay) rather than
/// misparsing it.
const SNAPSHOT_VERSION: u8 = 1;

/// Filename for a topic's snapshot, stored in the topic's txlog directory
/// alongside its `.log` segments. Chosen not to match the `*.log` glob so
/// segment enumeration never mistakes it for a log segment.
pub const SNAPSHOT_FILENAME: &str = "snapshot.bin";

/// Temp filename used for the write-then-rename atomic swap.
const SNAPSHOT_TMP_FILENAME: &str = "snapshot.bin.tmp";

/// A decoded SOW snapshot.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The topic's own `last_applied_sequence` at the moment the snapshot
    /// was taken. Tail replay applies only txlog entries whose sequence is
    /// strictly greater than this.
    pub sequence: u64,
    /// The active txlog segment id at snapshot time. Recovery skips every
    /// segment with a lower id (all fully covered by the snapshot) before
    /// replaying the tail. `0` means "replay from the first segment".
    pub segment_id: u64,
    /// Per-foreign-origin high-water marks at snapshot time, so replication
    /// dedup state survives restart exactly as the live store does.
    pub origin_highwater: HashMap<String, u64>,
    /// Live-row payloads, each a self-describing JSON object (the same
    /// bytes the txlog stores for an upsert). Tombstoned rows are excluded.
    pub rows: Vec<Vec<u8>>,
}

impl Snapshot {
    /// Encode the body (everything the CRC covers).
    fn encode_body(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            16 + self.rows.iter().map(|r| r.len() + 4).sum::<usize>(),
        );
        out.extend_from_slice(&self.sequence.to_be_bytes());
        out.extend_from_slice(&self.segment_id.to_be_bytes());

        out.extend_from_slice(&(self.origin_highwater.len() as u32).to_be_bytes());
        for (origin, hw) in &self.origin_highwater {
            let ob = origin.as_bytes();
            out.extend_from_slice(&(ob.len() as u16).to_be_bytes());
            out.extend_from_slice(ob);
            out.extend_from_slice(&hw.to_be_bytes());
        }

        out.extend_from_slice(&(self.rows.len() as u64).to_be_bytes());
        for row in &self.rows {
            out.extend_from_slice(&(row.len() as u32).to_be_bytes());
            out.extend_from_slice(row);
        }
        out
    }

    /// Decode a body produced by [`Snapshot::encode_body`]. Returns `None`
    /// on any structural inconsistency (truncated / overlong fields) so the
    /// caller can fall back to a full replay.
    fn decode_body(body: &[u8]) -> Option<Snapshot> {
        let mut cur = Cursor { buf: body, pos: 0 };
        let sequence = cur.u64()?;
        let segment_id = cur.u64()?;

        let origin_count = cur.u32()? as usize;
        // Guard against a corrupt length claiming millions of origins.
        if origin_count > 1_000_000 {
            return None;
        }
        let mut origin_highwater = HashMap::with_capacity(origin_count);
        for _ in 0..origin_count {
            let olen = cur.u16()? as usize;
            let origin = cur.bytes(olen)?;
            let hw = cur.u64()?;
            let origin = String::from_utf8(origin.to_vec()).ok()?;
            origin_highwater.insert(origin, hw);
        }

        let row_count = cur.u64()? as usize;
        let mut rows = Vec::with_capacity(row_count.min(1 << 20));
        for _ in 0..row_count {
            let plen = cur.u32()? as usize;
            let payload = cur.bytes(plen)?;
            rows.push(payload.to_vec());
        }

        // Reject trailing garbage — a clean snapshot consumes its whole body.
        if cur.pos != body.len() {
            return None;
        }
        Some(Snapshot {
            sequence,
            segment_id,
            origin_highwater,
            rows,
        })
    }

    /// Atomically write this snapshot into `dir` as `snapshot.bin`.
    /// Body is staged in a temp file, fsynced, then renamed into place;
    /// the directory is fsynced so the rename survives a crash.
    pub fn write_atomic(&self, dir: &Path) -> io::Result<()> {
        let body = self.encode_body();
        let crc = crc32fast::hash(&body);

        let tmp_path = dir.join(SNAPSHOT_TMP_FILENAME);
        let final_path = dir.join(SNAPSHOT_FILENAME);

        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            f.write_all(MAGIC)?;
            f.write_all(&[SNAPSHOT_VERSION])?;
            f.write_all(&(body.len() as u64).to_be_bytes())?;
            f.write_all(&crc.to_be_bytes())?;
            f.write_all(&body)?;
            f.sync_all()?;
        }

        fs::rename(&tmp_path, &final_path)?;

        // Best-effort directory fsync so the rename is durable. Not all
        // platforms allow opening a directory for fsync; ignore failures.
        if let Ok(dir_file) = File::open(dir) {
            let _ = dir_file.sync_all();
        }
        Ok(())
    }

    /// Load the snapshot from `dir`, if a valid one exists.
    ///
    /// Returns:
    /// - `Ok(Some(snapshot))` — a structurally valid, CRC-verified snapshot.
    /// - `Ok(None)` — no snapshot file, or one that failed any integrity
    ///   check (bad magic/version/CRC/short read). A warning is logged for
    ///   the corruption cases; the caller should fall back to full replay.
    /// - `Err(_)` — an unexpected IO error opening/reading the file.
    pub fn load(dir: &Path) -> io::Result<Option<Snapshot>> {
        let path = dir.join(SNAPSHOT_FILENAME);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };

        let mut header = [0u8; 8 + 1 + 8 + 4];
        if let Err(e) = f.read_exact(&mut header) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(?path, "snapshot header truncated; ignoring");
                return Ok(None);
            }
            return Err(e);
        }

        if &header[0..8] != MAGIC {
            tracing::warn!(?path, "snapshot magic mismatch; ignoring");
            return Ok(None);
        }
        let version = header[8];
        if version != SNAPSHOT_VERSION {
            tracing::warn!(?path, version, "snapshot version unsupported; ignoring");
            return Ok(None);
        }
        let body_len = u64::from_be_bytes(header[9..17].try_into().unwrap()) as usize;
        let crc_expected = u32::from_be_bytes(header[17..21].try_into().unwrap());

        // Sanity-cap the declared body length so a corrupt header can't
        // make us attempt a multi-terabyte allocation.
        const MAX_BODY: usize = 64 * 1024 * 1024 * 1024; // 64 GiB
        if body_len > MAX_BODY {
            tracing::warn!(?path, body_len, "snapshot body length implausible; ignoring");
            return Ok(None);
        }

        let mut body = vec![0u8; body_len];
        if let Err(e) = f.read_exact(&mut body) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                tracing::warn!(?path, "snapshot body truncated; ignoring");
                return Ok(None);
            }
            return Err(e);
        }

        if crc32fast::hash(&body) != crc_expected {
            tracing::warn!(?path, "snapshot CRC mismatch; ignoring");
            return Ok(None);
        }

        match Snapshot::decode_body(&body) {
            Some(s) => Ok(Some(s)),
            None => {
                tracing::warn!(?path, "snapshot body malformed; ignoring");
                Ok(None)
            }
        }
    }
}

/// Delete a topic's snapshot file, if present. Best-effort.
pub fn remove(dir: &Path) -> io::Result<()> {
    let path = dir.join(SNAPSHOT_FILENAME);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Path to the snapshot file within `dir` (whether or not it exists).
pub fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_FILENAME)
}

/// Minimal big-endian cursor over a byte slice. Every accessor returns
/// `None` rather than panicking on a short read, so a truncated/corrupt
/// body is rejected cleanly.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.buf.len() {
            return None;
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Some(s)
    }
    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample() -> Snapshot {
        let mut origin_highwater = HashMap::new();
        origin_highwater.insert("peer-a".to_string(), 42);
        origin_highwater.insert("peer-b".to_string(), 7);
        Snapshot {
            sequence: 1000,
            segment_id: 7,
            origin_highwater,
            rows: vec![
                br#"{"key":"k1","v":1}"#.to_vec(),
                br#"{"key":"k2","v":2}"#.to_vec(),
                br#"{"key":"k3","v":3}"#.to_vec(),
            ],
        }
    }

    #[test]
    fn round_trip_preserves_all_fields() {
        let dir = tempdir().unwrap();
        let snap = sample();
        snap.write_atomic(dir.path()).unwrap();

        let loaded = Snapshot::load(dir.path()).unwrap().expect("snapshot present");
        assert_eq!(loaded.sequence, snap.sequence);
        assert_eq!(loaded.segment_id, snap.segment_id);
        assert_eq!(loaded.origin_highwater, snap.origin_highwater);
        assert_eq!(loaded.rows, snap.rows);
    }

    #[test]
    fn empty_snapshot_round_trips() {
        let dir = tempdir().unwrap();
        let snap = Snapshot {
            sequence: 0,
            segment_id: 0,
            origin_highwater: HashMap::new(),
            rows: Vec::new(),
        };
        snap.write_atomic(dir.path()).unwrap();
        let loaded = Snapshot::load(dir.path()).unwrap().expect("present");
        assert_eq!(loaded.sequence, 0);
        assert!(loaded.origin_highwater.is_empty());
        assert!(loaded.rows.is_empty());
    }

    #[test]
    fn missing_file_is_none_not_error() {
        let dir = tempdir().unwrap();
        assert!(Snapshot::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn corrupt_crc_is_ignored() {
        let dir = tempdir().unwrap();
        sample().write_atomic(dir.path()).unwrap();
        // Flip a byte deep in the body (past the 21-byte header).
        let path = snapshot_path(dir.path());
        let mut bytes = fs::read(&path).unwrap();
        let idx = bytes.len() - 3;
        bytes[idx] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert!(
            Snapshot::load(dir.path()).unwrap().is_none(),
            "CRC corruption must be treated as no-snapshot"
        );
    }

    #[test]
    fn bad_magic_is_ignored() {
        let dir = tempdir().unwrap();
        let path = snapshot_path(dir.path());
        fs::write(&path, b"NOTASNAPSHOTfileatall").unwrap();
        assert!(Snapshot::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn truncated_body_is_ignored() {
        let dir = tempdir().unwrap();
        sample().write_atomic(dir.path()).unwrap();
        let path = snapshot_path(dir.path());
        let bytes = fs::read(&path).unwrap();
        // Keep the header but lop off most of the body.
        fs::write(&path, &bytes[..25]).unwrap();
        assert!(Snapshot::load(dir.path()).unwrap().is_none());
    }

    #[test]
    fn remove_is_idempotent() {
        let dir = tempdir().unwrap();
        // Removing a non-existent snapshot is fine.
        remove(dir.path()).unwrap();
        sample().write_atomic(dir.path()).unwrap();
        assert!(snapshot_path(dir.path()).exists());
        remove(dir.path()).unwrap();
        assert!(!snapshot_path(dir.path()).exists());
    }
}

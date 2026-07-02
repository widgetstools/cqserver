//! `TxLogWriter::force_rotate` durability tests.
//!
//! `force_rotate` backs the admin `POST /admin/rotate-journal/:topic`
//! endpoint, which `backup-cqserver.sh` calls before copying a topic's
//! on-disk directory into a backup archive. Regular size-triggered
//! rotation on the hot append path only fsyncs the sealed segment when
//! the writer's configured `FsyncPolicy` calls for it — under the
//! server default (`FsyncPolicy::None`) it does *not* fsync. That's
//! fine for steady-state throughput, but it means a naive force-rotate
//! reusing the same code path would let an operator believe a backup
//! taken right after "force-rotate" is durable when the sealed bytes
//! might still be sitting in the OS page cache.
//!
//! `force_rotate` must therefore unconditionally `sync_all` the sealed
//! segment regardless of `FsyncPolicy`, since it's an explicit,
//! infrequent, operator-triggered durability action — never on the hot
//! append path.
//!
//! Scope caveat: these tests exercise the completeness/archiving/
//! compression behavior of the `force_rotate` path (a torn or missing
//! sealed segment fails them), but a black-box reader cannot observe
//! whether `sync_all` was actually *called* — reverting `force_rotate`
//! to policy-gated fsync leaves these tests green. The unconditional-
//! fsync guarantee itself is enforced by the code (`rotate(true)`) and
//! its single admin-only caller, verified by review; a true regression
//! guard on the syscall would need fault injection / syscall spying.

use cq_txlog::reader::TxLogReader;
use cq_txlog::segment::{list_segments, segment_path, segment_zstd_path};
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use tempfile::tempdir;

/// Under the server default `FsyncPolicy::None`, `force_rotate` still
/// seals a complete, readable segment (the durability guarantee itself
/// — an unconditional `sync_all` — isn't directly observable from
/// outside the writer without fault injection, but a torn/incomplete
/// write would show up here as a read failure or missing entries, and
/// this exercises exactly the code path backup-cqserver.sh depends on).
#[test]
fn force_rotate_seals_a_complete_segment_under_fsync_none() {
    let dir = tempdir().unwrap();
    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();

    for i in 0..50u64 {
        w.append(i + 1, "/t", &format!("k{i}"), format!("payload-{i}").as_bytes())
            .expect("append");
    }
    let sealed_segment_id = w.current_segment();

    w.force_rotate().expect("force_rotate");

    // A fresh segment is now active.
    assert_eq!(w.current_segment(), sealed_segment_id + 1);

    // The sealed segment is a complete, replayable file — not a
    // torn/partial write left behind by a rotation that skipped fsync.
    let sealed_path = segment_path(dir.path(), sealed_segment_id);
    assert!(sealed_path.exists(), "sealed segment file exists on disk");

    let mut reader = TxLogReader::open(dir.path()).expect("open reader");
    let mut count = 0u64;
    let mut max_seq = 0u64;
    while let Some(entry) = reader.read_next().expect("read_next") {
        count += 1;
        max_seq = max_seq.max(entry.sequence);
    }
    assert_eq!(count, 50, "all 50 appended entries survive force_rotate");
    assert_eq!(max_seq, 50, "sequence numbers are intact");
}

/// `force_rotate` must fsync the segment's *final* resting place, which
/// is the archive directory when archiving is configured (rotation
/// moves — or copies — the sealed segment there after opening the new
/// active segment).
#[test]
fn force_rotate_seals_and_archives_completely_under_fsync_none() {
    let live_dir = tempdir().unwrap();
    let archive_dir = tempdir().unwrap();
    let mut w = TxLogWriter::open_with_archive(
        live_dir.path(),
        FsyncPolicy::None,
        1024 * 1024, // segment_size large enough that only force_rotate triggers rotation
        Some(archive_dir.path()),
    )
    .unwrap();

    for i in 0..20u64 {
        w.append(i + 1, "/t", &format!("k{i}"), format!("payload-{i}").as_bytes())
            .expect("append");
    }
    let sealed_segment_id = w.current_segment();

    w.force_rotate().expect("force_rotate");

    // Sealed segment moved out of the live dir into the archive dir.
    let live_sealed = segment_path(live_dir.path(), sealed_segment_id);
    assert!(!live_sealed.exists(), "sealed segment no longer in live dir");
    let archived_sealed = segment_path(archive_dir.path(), sealed_segment_id);
    assert!(archived_sealed.exists(), "sealed segment landed in archive dir");

    // The archived copy is complete and readable.
    let mut reader = TxLogReader::open(archive_dir.path()).expect("open archive reader");
    let mut count = 0u64;
    while reader.read_next().expect("read_next").is_some() {
        count += 1;
    }
    assert_eq!(count, 20, "all entries present in the archived segment");

    // Live dir now holds only the fresh active segment.
    let live_segments: Vec<u64> = list_segments(live_dir.path())
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(live_segments, vec![sealed_segment_id + 1]);
}

/// Same as above but with archive compression enabled — force_rotate
/// must fsync the `.log.zst` file, not the (now-deleted) uncompressed
/// intermediate.
#[test]
fn force_rotate_seals_and_archives_compressed_completely_under_fsync_none() {
    let live_dir = tempdir().unwrap();
    let archive_dir = tempdir().unwrap();
    let mut w = TxLogWriter::open_with_archive_compressed(
        live_dir.path(),
        FsyncPolicy::None,
        1024 * 1024,
        archive_dir.path(),
    )
    .unwrap();

    for i in 0..20u64 {
        w.append(i + 1, "/t", &format!("k{i}"), format!("payload-{i}").as_bytes())
            .expect("append");
    }
    let sealed_segment_id = w.current_segment();

    w.force_rotate().expect("force_rotate");

    let archived_zst = segment_zstd_path(archive_dir.path(), sealed_segment_id);
    assert!(archived_zst.exists(), "compressed sealed segment landed in archive dir");

    let mut reader = TxLogReader::open(archive_dir.path()).expect("open archive reader");
    let mut count = 0u64;
    while reader.read_next().expect("read_next").is_some() {
        count += 1;
    }
    assert_eq!(count, 20, "all entries present in the compressed archived segment");
}

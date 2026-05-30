//! Crash-recovery tests for the cq-txlog (worklog S41, review C10).
//!
//! These are **in-process** tests — they write a log, manually
//! corrupt the on-disk bytes to simulate the failure mode, then
//! reopen via `TxLogReader` and assert the post-corruption recovery
//! behavior. A separate `C10.2` process-spawn variant for
//! `fsync=every_write` durability claims is deferred — see the
//! S41 Progress note in `AMPS_WORKLOG.md`.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Duration;

use cq_txlog::reader::TxLogReader;
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use tempfile::tempdir;

fn write_n_entries(writer: &mut TxLogWriter, n: usize) {
    for i in 0..n {
        let key = format!("k{i:04}");
        let payload = format!("v{i:04}").into_bytes();
        writer
            .append((i as u64) + 1, "/t", &key, &payload)
            .expect("append");
    }
    writer.sync().expect("sync");
}

/// C10.1 — torn write at the tail: the last few bytes of the active
/// segment are garbage. Recovery must truncate at the corruption
/// boundary and return every entry that came before, without
/// surfacing the partial trailing frame.
#[test]
fn torn_write_at_tail_truncates_and_recovers_prior_entries() {
    let dir = tempdir().unwrap();
    let n = 50;

    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).expect("open");
    write_n_entries(&mut w, n);
    let segment_path = w.path().to_path_buf();
    drop(w);

    // Append 5 garbage bytes to the tail. A real torn write would
    // leave a partial frame whose length-prefix is invalid OR whose
    // CRC doesn't match; tail-junk is the simplest model of either.
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .expect("open append");
        f.write_all(&[0xff, 0xff, 0xff, 0xff, 0xff]).expect("trash");
    }

    let mut r = TxLogReader::open(dir.path()).expect("reopen");
    let entries = r.read_all().expect("read_all (recovery should not error on tail junk)");

    // Recovery must return every fully-written entry; trailing
    // garbage is silently truncated. We're tolerant of losing the
    // last entry only if its frame ends in the garbage region, but
    // since we appended AFTER the last sync, all N entries' frames
    // should be intact.
    assert_eq!(
        entries.len(),
        n,
        "torn tail lost {} pre-corruption entries",
        n - entries.len()
    );
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, (i as u64) + 1);
        assert_eq!(e.key, format!("k{i:04}"));
    }
}

/// Reopening a multi-segment log seeds `max_sequence` from the newest
/// non-empty segment's highest sequence — without re-reading the whole log.
/// This is the fast-restart invariant: the writer's resume scan must be
/// O(one segment), and it must still return the true global max even when
/// the data spans many rotated segments.
#[test]
fn reopen_seeds_max_sequence_from_tail_segment() {
    let dir = tempdir().unwrap();
    let segment_size = 256u64; // force frequent rotation
    let n = 60usize;

    {
        let mut w =
            TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, segment_size)
                .expect("open");
        for i in 0..n {
            w.append(
                (i as u64) + 1,
                "/t",
                &format!("k{i:03}"),
                b"payload bytes large enough to rotate often",
            )
            .expect("append");
        }
        w.sync().expect("sync");
    }

    // Confirm we rotated, so the tail-scan path (not a single segment) runs.
    let segs = cq_txlog::segment::list_segments(dir.path()).expect("list");
    assert!(segs.len() >= 2, "expected rotation, got {}", segs.len());

    // Reopen: the resume scan must recover the true max sequence (== n).
    let w2 = TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, segment_size)
        .expect("reopen");
    assert_eq!(
        w2.max_sequence(),
        n as u64,
        "reopen must seed max_sequence from the newest non-empty segment"
    );
}

/// C10.3 — CRC corruption mid-log. A byte is flipped inside a
/// completed entry's BODY (not in a header). The reader must
/// surface a CRC error rather than silently producing garbage data
/// or skipping past the bad frame.
///
/// NB: this test specifically targets a body byte (offset 12 — past
/// the first entry's 8-byte header). Header-byte corruption can
/// decode to an oversized frame_len, which the reader now treats as
/// torn-tail garbage on the active segment (see
/// `restart_after_garbage_appended_to_txlog_does_not_crash` in the
/// e2e suite). The CRC path remains strict — that's what this test
/// pins.
#[test]
fn crc_corruption_mid_log_surfaces_error() {
    let dir = tempdir().unwrap();
    let n = 20;

    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).expect("open");
    write_n_entries(&mut w, n);
    let segment_path = w.path().to_path_buf();
    drop(w);

    let file_size = std::fs::metadata(&segment_path).expect("meta").len();
    assert!(file_size > 400, "log too small to corrupt mid-stream");
    // Offset 12 = 4 bytes into the first record's body (skip the
    // 8-byte header). Flipping here always trips CRC, never the
    // length-decoding path.
    let corrupt_at = 12;

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&segment_path)
        .expect("rw open");
    f.seek(SeekFrom::Start(corrupt_at)).expect("seek");
    let mut byte = [0u8; 1];
    f.read_exact(&mut byte).expect("read byte");
    byte[0] ^= 0xff;
    f.seek(SeekFrom::Start(corrupt_at)).expect("seek back");
    f.write_all(&byte).expect("write flipped byte");
    drop(f);

    let mut r = TxLogReader::open(dir.path()).expect("reopen");
    let mut hit_error = false;
    let mut entries_read = 0usize;
    loop {
        match r.read_next() {
            Ok(Some(_)) => entries_read += 1,
            Ok(None) => break,
            Err(_) => {
                hit_error = true;
                break;
            }
        }
    }
    assert!(
        hit_error,
        "mid-log CRC corruption silently swallowed (read {entries_read} entries without error)"
    );
}

/// C10.5 — `FsyncPolicy::Interval` (group commit) preserves the same
/// crash-recovery shape as the other policies. Interval mode writes each
/// frame straight through to the OS page cache (only the fsync is
/// batched), so a torn write at the tail must still truncate cleanly at
/// the last complete frame and recover every prior entry.
#[test]
fn interval_group_commit_recovers_after_torn_tail() {
    let dir = tempdir().unwrap();
    let n = 50;

    let mut w =
        TxLogWriter::open(dir.path(), FsyncPolicy::Interval(Duration::from_millis(5)))
            .expect("open");
    write_n_entries(&mut w, n);
    let segment_path = w.path().to_path_buf();
    drop(w);

    // Simulate a torn trailing frame from a crash mid-interval.
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .expect("open append");
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x00]).expect("trash");
    }

    let entries = TxLogReader::open(dir.path())
        .expect("reopen")
        .read_all()
        .expect("read_all should tolerate torn tail");
    assert_eq!(entries.len(), n, "interval-mode recovery lost entries");
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, (i as u64) + 1);
        assert_eq!(e.key, format!("k{i:04}"));
    }
}

/// C10.6 — block preallocation is transparent to replay. With
/// preallocation enabled and a small segment size forcing rotation,
/// every entry must read back in order. This specifically exercises the
/// sealed-segment path: if a rotated segment carried preallocated
/// trailing padding, the strict (non-active) reader would reject it with
/// a checksum/zero-frame error — so a clean read of all entries proves
/// the reserved tail is released on rotation and that the logical file
/// length always tracks real content.
#[test]
fn preallocated_segments_replay_cleanly_across_rotation() {
    let dir = tempdir().unwrap();
    let segment_size = 256u64; // small => frequent rotation
    let n = 40usize;

    let mut w =
        TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, segment_size)
            .expect("open")
            .with_preallocate(true);
    for i in 0..n {
        w.append(
            (i as u64) + 1,
            "/t",
            &format!("k{i:03}"),
            b"sufficient payload bytes to force rotation",
        )
        .expect("append");
    }
    w.sync().expect("sync");

    // Confirm we actually rotated (otherwise the sealed-segment path
    // isn't exercised).
    let segs = cq_txlog::segment::list_segments(dir.path()).expect("list");
    assert!(segs.len() >= 2, "expected rotation, got {} segment(s)", segs.len());

    let entries = TxLogReader::open(dir.path())
        .expect("reopen")
        .read_all()
        .expect("preallocated log must replay without error");
    assert_eq!(entries.len(), n);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, (i as u64) + 1);
        assert_eq!(e.key, format!("k{i:03}"));
    }
}

/// C10.7 — a torn tail in a *preallocated* active segment recovers like
/// any other. Because preallocation reserves blocks without growing the
/// logical file size, the active segment never exposes the reserved
/// (zero) tail to a reader; a crash mid-write therefore truncates at the
/// last good frame exactly as in the non-preallocated case.
#[test]
fn preallocated_active_segment_torn_tail_recovers() {
    let dir = tempdir().unwrap();
    let n = 30;

    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None)
        .expect("open")
        .with_preallocate(true);
    write_n_entries(&mut w, n);
    let segment_path = w.path().to_path_buf();
    drop(w);

    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(&segment_path)
            .expect("open append");
        f.write_all(&[0xff, 0xff, 0xff]).expect("trash");
    }

    let entries = TxLogReader::open(dir.path())
        .expect("reopen")
        .read_all()
        .expect("read_all should tolerate torn tail");
    assert_eq!(entries.len(), n);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence, (i as u64) + 1);
    }
}

/// C10.4 — replay equivalence across mixed compressed + uncompressed
/// segments. A reader fed an archive dir with both `.log` and
/// `.log.zst` segments must return the same sequence of entries as
/// reading the same data from all-uncompressed segments.
#[test]
fn mixed_compressed_and_uncompressed_segments_replay_identically() {
    let n = 60;
    let segment_size = 256u64; // small so writes force rotation
    // First pass: uncompressed archive.
    let dir_a = tempdir().unwrap();
    let archive_a = dir_a.path().join("archive");
    std::fs::create_dir_all(&archive_a).unwrap();
    let live_a = dir_a.path().join("live");
    std::fs::create_dir_all(&live_a).unwrap();
    {
        let mut w = TxLogWriter::open_with_archive(
            &live_a,
            FsyncPolicy::None,
            segment_size,
            Some(archive_a.clone()),
        )
        .expect("open uncompressed");
        write_n_entries(&mut w, n);
    }
    let entries_a: Vec<_> = TxLogReader::open_with_archive(&live_a, Some(&archive_a))
        .expect("reopen a")
        .read_all()
        .expect("read all a");

    // Second pass: compressed archive (same data shape).
    let dir_b = tempdir().unwrap();
    let archive_b = dir_b.path().join("archive");
    std::fs::create_dir_all(&archive_b).unwrap();
    let live_b = dir_b.path().join("live");
    std::fs::create_dir_all(&live_b).unwrap();
    {
        let mut w = TxLogWriter::open_with_archive_compressed(
            &live_b,
            FsyncPolicy::None,
            segment_size,
            archive_b.clone(),
        )
        .expect("open compressed");
        write_n_entries(&mut w, n);
    }
    let entries_b: Vec<_> = TxLogReader::open_with_archive(&live_b, Some(&archive_b))
        .expect("reopen b")
        .read_all()
        .expect("read all b");

    assert_eq!(entries_a.len(), n);
    assert_eq!(entries_b.len(), n);
    for (i, (a, b)) in entries_a.iter().zip(entries_b.iter()).enumerate() {
        assert_eq!(a.sequence, b.sequence, "seq mismatch at {i}");
        assert_eq!(a.key, b.key, "key mismatch at {i}");
        assert_eq!(a.payload, b.payload, "payload mismatch at {i}");
        assert_eq!(a.topic, b.topic, "topic mismatch at {i}");
    }

    // Sanity: confirm we actually exercised both modes by looking
    // at the on-disk extensions.
    let names_b: Vec<String> = std::fs::read_dir(&archive_b)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    assert!(
        names_b.iter().any(|n| n.ends_with(".log.zst")),
        "compressed archive contained no .log.zst files: {names_b:?}"
    );
}

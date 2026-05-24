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

/// C10.3 — CRC corruption mid-log. A byte is flipped inside a
/// completed entry's body (NOT at the tail). The reader must
/// surface a CRC error rather than silently producing garbage data
/// or skipping past the bad frame.
#[test]
fn crc_corruption_mid_log_surfaces_error() {
    let dir = tempdir().unwrap();
    let n = 20;

    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).expect("open");
    write_n_entries(&mut w, n);
    let segment_path = w.path().to_path_buf();
    drop(w);

    // Open the file, find a byte well inside it (offset 200 — past
    // the first entry's header & well before the tail), and flip it.
    let file_size = std::fs::metadata(&segment_path).expect("meta").len();
    assert!(file_size > 400, "log too small to corrupt mid-stream");
    let corrupt_at = 200;

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

    // Read until we hit the corruption. We're testing that an error
    // surfaces — not whether N-correctly-decoded entries appeared
    // first. Either:
    //   (a) read_all returns Err — fine.
    //   (b) read_next eventually returns Err mid-stream — fine.
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

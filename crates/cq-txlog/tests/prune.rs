//! Segment pruning tests (txlog retention — snapshot-anchored reclaim).
//!
//! `TxLogWriter::prune_segments_below(cutoff)` deletes sealed segments
//! whose id is strictly below `cutoff`, never touching the active
//! segment. It is the reclaim primitive behind `snapshot_reclaim`:
//! once a crash-durable snapshot covers everything up to segment K,
//! segments 1..K are redundant for recovery and can be deleted.

use cq_txlog::reader::TxLogReader;
use cq_txlog::segment::list_segments;
use cq_txlog::writer::TxLogWriter;
use cq_txlog::FsyncPolicy;
use tempfile::tempdir;

/// Tiny segments so a handful of appends spans several files.
const SEG_SIZE: u64 = 256;

/// Append entries until the writer's active segment id reaches
/// `target_segment`. Returns the total number of entries written.
fn fill_to_segment(w: &mut TxLogWriter, start_seq: u64, target_segment: u64) -> u64 {
    let mut seq = start_seq;
    while w.current_segment() < target_segment {
        seq += 1;
        w.append(seq, "/t", &format!("k{seq}"), format!("payload-{seq}").as_bytes())
            .expect("append");
    }
    w.sync().expect("sync");
    seq
}

#[test]
fn prune_deletes_sealed_segments_below_cutoff() {
    let dir = tempdir().unwrap();
    let mut w =
        TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, SEG_SIZE).unwrap();
    let last_seq = fill_to_segment(&mut w, 0, 4);

    let before: Vec<u64> = list_segments(dir.path())
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(before, vec![1, 2, 3, 4], "precondition: four segments on disk");

    let deleted = w.prune_segments_below(3).expect("prune");
    assert_eq!(deleted, 2, "segments 1 and 2 deleted");

    let after: Vec<u64> = list_segments(dir.path())
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(after, vec![3, 4], "segments 3 (cutoff) and 4 (active) remain");

    // The surviving tail must still read cleanly from the cutoff onward.
    let mut r = TxLogReader::open(dir.path()).expect("reader");
    let mut max_seq = 0;
    let mut count = 0u64;
    while let Some(e) = r.read_next().expect("read") {
        max_seq = max_seq.max(e.sequence);
        count += 1;
    }
    assert!(count > 0, "tail entries still readable");
    assert_eq!(max_seq, last_seq, "newest entry survives pruning");
}

#[test]
fn prune_never_deletes_the_active_segment() {
    let dir = tempdir().unwrap();
    let mut w =
        TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, SEG_SIZE).unwrap();
    let last_seq = fill_to_segment(&mut w, 0, 3);
    let active = w.current_segment();

    // Cutoff far beyond the active segment: the writer must clamp.
    let deleted = w.prune_segments_below(999).expect("prune");
    assert_eq!(deleted, active - 1, "everything but the active segment deleted");

    let after: Vec<u64> = list_segments(dir.path())
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(after, vec![active], "only the active segment remains");

    // Writer still appends into the surviving segment; reader sees old
    // tail + new entry.
    w.append(last_seq + 1, "/t", "knew", b"after-prune").expect("append");
    w.sync().expect("sync");
    let mut r = TxLogReader::open(dir.path()).expect("reader");
    let mut seqs = Vec::new();
    while let Some(e) = r.read_next().expect("read") {
        seqs.push(e.sequence);
    }
    assert_eq!(*seqs.last().unwrap(), last_seq + 1, "append after prune works");
}

#[test]
fn prune_is_a_noop_when_nothing_is_below_cutoff() {
    let dir = tempdir().unwrap();
    let mut w =
        TxLogWriter::open_with_segment_size(dir.path(), FsyncPolicy::None, SEG_SIZE).unwrap();
    fill_to_segment(&mut w, 0, 2);

    let deleted = w.prune_segments_below(1).expect("prune");
    assert_eq!(deleted, 0);
    let ids: Vec<u64> = list_segments(dir.path())
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, vec![1, 2], "no segments deleted");
}

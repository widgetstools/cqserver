//! JsonDelta payload-format round-trip (sparse delta journaling).
//!
//! A `JsonDelta` entry's payload is the sparse `{key + changed fields}`
//! JSON map *as published* — not the fully-merged row. The `0x03` body
//! marker distinguishes it on read so recovery can merge (rather than
//! replace) and mixed logs replay fine.

use cq_txlog::reader::TxLogReader;
use cq_txlog::writer::TxLogWriter;
use cq_txlog::{FsyncPolicy, PayloadFormat};
use tempfile::tempdir;

#[test]
fn json_delta_entries_round_trip_with_format_marker() {
    let dir = tempdir().unwrap();
    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();

    // Full row (plain JSON), then a sparse delta touching two fields.
    let full = br#"{"id":"k1","a":1,"b":2,"c":3}"#;
    let sparse = br#"{"id":"k1","b":9}"#;
    w.append_with_origin_format(1, "/t", "k1", "", full, PayloadFormat::Json)
        .unwrap();
    w.append_with_origin_format(2, "/t", "k1", "node-a", sparse, PayloadFormat::JsonDelta)
        .unwrap();
    w.sync().unwrap();

    let mut r = TxLogReader::open(dir.path()).unwrap();

    let e1 = r.read_next().unwrap().expect("entry 1");
    assert_eq!(e1.payload_format, PayloadFormat::Json);
    assert_eq!(e1.payload, full);

    let e2 = r.read_next().unwrap().expect("entry 2");
    assert_eq!(e2.payload_format, PayloadFormat::JsonDelta);
    assert_eq!(e2.payload, sparse, "delta payload stored as published");
    assert_eq!(e2.origin, "node-a", "V2 origin survives on delta entries");
    assert_eq!(e2.sequence, 2);

    assert!(r.read_next().unwrap().is_none());
}

#[test]
fn json_delta_without_origin_still_uses_v2_marker_layout() {
    // An empty origin must not degrade a delta entry to the V1 layout —
    // the marker byte is the only thing that identifies it as a delta.
    let dir = tempdir().unwrap();
    let mut w = TxLogWriter::open(dir.path(), FsyncPolicy::None).unwrap();
    w.append_with_origin_format(1, "/t", "k1", "", br#"{"id":"k1","x":5}"#, PayloadFormat::JsonDelta)
        .unwrap();
    w.sync().unwrap();

    let mut r = TxLogReader::open(dir.path()).unwrap();
    let e = r.read_next().unwrap().expect("entry");
    assert_eq!(e.payload_format, PayloadFormat::JsonDelta);
    assert_eq!(e.origin, "");
}

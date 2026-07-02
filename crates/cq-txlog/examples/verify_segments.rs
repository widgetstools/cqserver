//! `verify_segments` — scan every segment file under a directory with the
//! real `TxLogReader` and report entry count / max sequence / tombstone
//! count, or a non-zero exit + error message on the first checksum or
//! framing failure.
//!
//! This is the verification primitive `scripts/backup-cqserver.sh` shells
//! out to (via `cargo run --release -p cq-txlog --example verify_segments`)
//! to prove a backed-up segment directory actually parses end-to-end with
//! the same reader the server uses for crash recovery / replication — a
//! stronger check than "files are non-empty," since it catches truncation,
//! bit-rot, and framing corruption introduced by the copy step.
//!
//! Usage:
//!   verify_segments <dir> [--archive-dir <dir>]
//!
//! Output (single line of JSON on success, to stdout):
//!   {"dir":"...","entries":1234,"tombstones":3,"maxSequence":1234,"bytesRead":56789}
//!
//! On any read/checksum error, prints a human-readable error to stderr and
//! exits 1.

use cq_txlog::reader::TxLogReader;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(dir) = args.next() else {
        eprintln!("usage: verify_segments <dir> [--archive-dir <dir>]");
        return ExitCode::from(2);
    };
    let mut archive_dir: Option<PathBuf> = None;
    while let Some(a) = args.next() {
        if a == "--archive-dir" {
            archive_dir = args.next().map(PathBuf::from);
        }
    }

    let dir_path = PathBuf::from(&dir);
    let mut reader = match TxLogReader::open_with_archive(&dir_path, archive_dir.as_ref()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("verify_segments: failed to open {dir}: {e}");
            return ExitCode::from(1);
        }
    };

    let mut entries: u64 = 0;
    let mut tombstones: u64 = 0;
    let mut max_sequence: u64 = 0;
    loop {
        match reader.read_next() {
            Ok(Some(entry)) => {
                entries += 1;
                if entry.is_tombstone() {
                    tombstones += 1;
                }
                if entry.sequence > max_sequence {
                    max_sequence = entry.sequence;
                }
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!(
                    "verify_segments: corruption at byte offset {} in {dir}: {e}",
                    reader.offset()
                );
                return ExitCode::from(1);
            }
        }
    }

    println!(
        "{{\"dir\":\"{}\",\"entries\":{},\"tombstones\":{},\"maxSequence\":{},\"bytesRead\":{}}}",
        dir.replace('\\', "\\\\").replace('"', "\\\""),
        entries,
        tombstones,
        max_sequence,
        reader.offset()
    );
    ExitCode::SUCCESS
}

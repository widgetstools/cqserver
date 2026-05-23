//! Segment file naming + discovery.
//!
//! A topic's transaction log lives in a directory. Each segment file is
//! named `{08-digit segment id}.log` (e.g. `00000001.log`). The id is a
//! monotonic 1-based counter; `00000001.log` is the oldest segment, the
//! highest-numbered file is the active one for new writes.
//!
//! Segments are sized-bounded: when an append would push the active
//! segment past `segment_size`, the writer closes it and opens the next.
//! This makes segments the natural unit of replication (a closed segment
//! is immutable and can be shipped verbatim).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub const SEGMENT_EXT: &str = "log";
pub const SEGMENT_ZSTD_EXT: &str = "log.zst";

/// Build the path for segment `id` under `dir`.
pub fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:08}.{}", id, SEGMENT_EXT))
}

/// Build the compressed-segment path for `id`.
pub fn segment_zstd_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{:08}.{}", id, SEGMENT_ZSTD_EXT))
}

/// Parse a segment id from a file name (without parent components). Returns
/// `None` if the file isn't a valid segment file. Accepts both
/// `00000001.log` and `00000001.log.zst`.
pub fn parse_segment_id(name: &OsStr) -> Option<u64> {
    let s = name.to_str()?;
    // Strip optional .zst suffix first.
    let stripped = s.strip_suffix(".zst").unwrap_or(s);
    let (stem, ext) = stripped.rsplit_once('.')?;
    if ext != SEGMENT_EXT {
        return None;
    }
    if stem.is_empty() || !stem.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    stem.parse::<u64>().ok()
}

/// Returns `true` when `path`'s filename ends in `.log.zst`.
pub fn is_compressed_segment(path: &Path) -> bool {
    path.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.ends_with(".log.zst"))
        .unwrap_or(false)
}

/// List every segment file in `dir`, sorted ascending by id.
/// Non-segment files (anything not matching `{digits}.log`) are ignored.
pub fn list_segments(dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(id) = parse_segment_id(&entry.file_name()) {
            out.push((id, entry.path()));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn segment_path_pads_to_8_digits() {
        let dir = Path::new("/tmp");
        assert_eq!(segment_path(dir, 1), Path::new("/tmp/00000001.log"));
        assert_eq!(segment_path(dir, 42), Path::new("/tmp/00000042.log"));
        assert_eq!(segment_path(dir, 12345678), Path::new("/tmp/12345678.log"));
    }

    #[test]
    fn parse_segment_id_accepts_valid_names() {
        assert_eq!(parse_segment_id(OsStr::new("00000001.log")), Some(1));
        assert_eq!(parse_segment_id(OsStr::new("00000042.log")), Some(42));
        assert_eq!(parse_segment_id(OsStr::new("12345678.log")), Some(12345678));
    }

    #[test]
    fn parse_segment_id_rejects_invalid_names() {
        assert_eq!(parse_segment_id(OsStr::new("notalog.txt")), None);
        assert_eq!(parse_segment_id(OsStr::new("abc.log")), None);
        assert_eq!(parse_segment_id(OsStr::new(".log")), None);
        assert_eq!(parse_segment_id(OsStr::new("00000001.log.bak")), None);
    }

    #[test]
    fn list_segments_sorts_ascending() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("00000003.log"), b"").unwrap();
        std::fs::write(dir.path().join("00000001.log"), b"").unwrap();
        std::fs::write(dir.path().join("00000002.log"), b"").unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"").unwrap();
        let segs = list_segments(dir.path()).unwrap();
        let ids: Vec<u64> = segs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn list_segments_returns_empty_for_missing_dir() {
        let segs = list_segments(Path::new("/definitely/not/a/path")).unwrap();
        assert!(segs.is_empty());
    }
}

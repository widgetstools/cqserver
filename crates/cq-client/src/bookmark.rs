//! S18 — client-side persistent bookmark store.
//!
//! Records the highest delta sequence each subscription has
//! successfully processed, keyed by topic name. On reconnect, the
//! client SDK reads the stored bookmark and passes it back as the
//! `bookmark` field on `sow_and_subscribe`, so the server replays
//! from that point instead of restarting the snapshot stream from
//! scratch.
//!
//! The store is a plain JSON file: `{ "<topic>": <max_seq>, ... }`.
//! It's safe to share across multiple Clients in the same process
//! (everything goes through an inner mutex). Writes are flushed via
//! atomic-rename so a half-written file can't corrupt the store on
//! crash.
//!
//! Use case: a UI dashboard that subscribes to a few SOW topics
//! restarts (process exit → relaunch). With a `LocalBookmarkStore`
//! attached, the relaunched client doesn't re-process the entire
//! snapshot — it picks up from wherever it left off.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum LocalBookmarkError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
}

/// Process-wide bookmark store keyed by topic name. Cheap to clone
/// (shares an `Arc<Mutex<_>>` internally); pass into
/// [`Client::with_bookmark_store`] at SDK construction time.
#[derive(Clone)]
pub struct LocalBookmarkStore {
    path: PathBuf,
    map: Arc<Mutex<HashMap<String, u64>>>,
}

impl LocalBookmarkStore {
    /// Open the store at `path`. If the file exists, its contents
    /// seed the in-memory map (malformed JSON is treated as "empty"
    /// rather than fatal — the store re-establishes from live deltas).
    /// If the path doesn't exist, returns an empty store that will be
    /// created on first persist.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, LocalBookmarkError> {
        let path = path.into();
        let map = if path.exists() {
            match std::fs::read(&path) {
                Ok(bytes) => serde_json::from_slice::<HashMap<String, u64>>(&bytes)
                    .unwrap_or_default(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
                Err(e) => return Err(e.into()),
            }
        } else {
            HashMap::new()
        };
        Ok(Self {
            path,
            map: Arc::new(Mutex::new(map)),
        })
    }

    /// Read the recorded bookmark for `topic`. `None` means no
    /// previous session has reported a sequence for this topic.
    pub fn get(&self, topic: &str) -> Option<u64> {
        self.map.lock().get(topic).copied()
    }

    /// Monotonic record: update the bookmark for `topic` to
    /// `max(existing, seq)`. Subsequent persists carry the new value.
    /// Out-of-order deltas can't lower an already-recorded bookmark.
    pub fn record(&self, topic: &str, seq: u64) {
        if seq == 0 {
            return;
        }
        let mut m = self.map.lock();
        let entry = m.entry(topic.to_string()).or_insert(0);
        if *entry < seq {
            *entry = seq;
        }
    }

    /// Snapshot the in-memory map and write it to disk via atomic
    /// rename. Safe to call concurrently — the lock is held only for
    /// the snapshot itself; the disk write happens outside the lock
    /// so a long-running fsync doesn't block recorders.
    pub fn persist(&self) -> Result<(), LocalBookmarkError> {
        let snapshot = self.map.lock().clone();
        let bytes = serde_json::to_vec(&snapshot)?;
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let tmp = tmp_path_for(&self.path);
        std::fs::write(&tmp, &bytes)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    /// Convenience for tests / debugging.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_through_persist() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bm.json");
        {
            let store = LocalBookmarkStore::load(&path).expect("load");
            store.record("/topic-a", 42);
            store.record("/topic-b", 7);
            store.record("/topic-a", 41); // should not lower
            store.persist().expect("persist");
        }
        let reloaded = LocalBookmarkStore::load(&path).expect("reload");
        assert_eq!(reloaded.get("/topic-a"), Some(42));
        assert_eq!(reloaded.get("/topic-b"), Some(7));
        assert_eq!(reloaded.get("/topic-missing"), None);
    }

    #[test]
    fn record_is_monotonic() {
        let dir = tempdir().unwrap();
        let store = LocalBookmarkStore::load(dir.path().join("bm.json")).unwrap();
        store.record("/x", 10);
        store.record("/x", 5);
        store.record("/x", 12);
        assert_eq!(store.get("/x"), Some(12));
    }

    #[test]
    fn record_zero_is_noop() {
        let dir = tempdir().unwrap();
        let store = LocalBookmarkStore::load(dir.path().join("bm.json")).unwrap();
        store.record("/x", 5);
        store.record("/x", 0); // sequence 0 is the "unset" sentinel
        assert_eq!(store.get("/x"), Some(5));
    }

    #[test]
    fn loading_nonexistent_file_returns_empty_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.json");
        let store = LocalBookmarkStore::load(&path).unwrap();
        assert_eq!(store.get("/anything"), None);
        store.record("/a", 1);
        store.persist().expect("persist creates the file");
        assert!(path.exists());
    }

    #[test]
    fn malformed_existing_file_is_treated_as_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bm.json");
        std::fs::write(&path, b"this is not json").unwrap();
        let store = LocalBookmarkStore::load(&path).unwrap();
        assert_eq!(store.get("/x"), None);
        // And we can still write fresh state.
        store.record("/x", 99);
        store.persist().unwrap();
        assert_eq!(
            LocalBookmarkStore::load(&path).unwrap().get("/x"),
            Some(99)
        );
    }
}

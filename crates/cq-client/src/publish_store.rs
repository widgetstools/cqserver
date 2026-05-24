//! S17 — client-side persistent publish buffer.
//!
//! Records every publish the application initiates BEFORE the SDK
//! forwards it on the wire. When the server acks, the entry is
//! removed. On a clean disconnect (or crash), surviving entries
//! represent publishes the server may not have durably received — on
//! reconnect, [`LocalPublishStore::pending`] drives a replay loop
//! that resends each one. The application sees at-least-once
//! semantics; duplicate deliveries are absorbed by the server's
//! `replay_dedup` gate (see S14).
//!
//! ### On-disk format
//!
//! The file is a JSON object `{ "id": <PendingPublish>, ... }` where
//! `id` is a client-assigned monotonic identifier. Writes go through
//! atomic-rename so a crash mid-write can't corrupt the file.
//!
//! ### Concurrency
//!
//! All state lives behind an inner `parking_lot::Mutex` so concurrent
//! `record` / `complete` / `persist` calls are safe. Cloning the
//! store is cheap (`Arc<Mutex<_>>` internally) — pass the same
//! instance into multiple `Client` instances if needed.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum LocalPublishError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
}

/// One queued publish, persisted to disk until the server acks it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingPublish {
    pub topic: String,
    pub data: serde_json::Value,
    /// Client-assigned monotonic id; also the key in the on-disk
    /// `BTreeMap`. Surfaced via `pending()` so the SDK can record
    /// it on the wire as a `cid` and reconcile acks back to the
    /// store entry.
    pub id: u64,
}

/// Process-wide publish buffer keyed by client-assigned id.
#[derive(Clone)]
pub struct LocalPublishStore {
    path: PathBuf,
    inner: Arc<Mutex<PublishStoreInner>>,
    next_id: Arc<AtomicU64>,
}

struct PublishStoreInner {
    pending: BTreeMap<u64, PendingPublish>,
}

impl LocalPublishStore {
    /// Open the store at `path`, seeding from any existing on-disk
    /// state. A malformed file is treated as "empty" rather than
    /// fatal — the SDK starts fresh in that case.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, LocalPublishError> {
        let path = path.into();
        let pending = if path.exists() {
            match std::fs::read(&path) {
                Ok(bytes) => serde_json::from_slice::<BTreeMap<u64, PendingPublish>>(&bytes)
                    .unwrap_or_default(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
                Err(e) => return Err(e.into()),
            }
        } else {
            BTreeMap::new()
        };
        // Seed `next_id` past any existing entry so replays don't
        // reuse ids.
        let next_id_seed = pending.keys().last().copied().unwrap_or(0) + 1;
        Ok(Self {
            path,
            inner: Arc::new(Mutex::new(PublishStoreInner { pending })),
            next_id: Arc::new(AtomicU64::new(next_id_seed)),
        })
    }

    /// Record a publish in the buffer and return its assigned id.
    /// The SDK then sends the publish on the wire keyed by this id;
    /// when the ack arrives, the SDK calls `complete(id)` to drop
    /// it from the buffer.
    pub fn record(&self, topic: &str, data: serde_json::Value) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        inner.pending.insert(
            id,
            PendingPublish {
                topic: topic.to_string(),
                data,
                id,
            },
        );
        id
    }

    /// Mark the publish with `id` as completed (server acked) — drop
    /// it from the buffer. Subsequent persists will not carry it.
    pub fn complete(&self, id: u64) {
        let mut inner = self.inner.lock();
        inner.pending.remove(&id);
    }

    /// Snapshot of every pending entry in id order. Used by the
    /// reconnect-replay loop.
    pub fn pending(&self) -> Vec<PendingPublish> {
        self.inner.lock().pending.values().cloned().collect()
    }

    /// Count of pending entries — handy for tests/observability.
    pub fn pending_count(&self) -> usize {
        self.inner.lock().pending.len()
    }

    /// Snapshot the in-memory map and write it to disk via atomic
    /// rename.
    pub fn persist(&self) -> Result<(), LocalPublishError> {
        let snapshot = self.inner.lock().pending.clone();
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
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn record_persist_reload_returns_same_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pub.json");
        let store = LocalPublishStore::load(&path).unwrap();
        let id1 = store.record("/topic-a", json!({ "k": 1 }));
        let id2 = store.record("/topic-b", json!({ "k": 2 }));
        assert_eq!(id1 + 1, id2);
        store.persist().unwrap();

        let reloaded = LocalPublishStore::load(&path).unwrap();
        let pending = reloaded.pending();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].topic, "/topic-a");
        assert_eq!(pending[1].topic, "/topic-b");
    }

    #[test]
    fn complete_removes_entries() {
        let dir = tempdir().unwrap();
        let store = LocalPublishStore::load(dir.path().join("pub.json")).unwrap();
        let id = store.record("/t", json!({}));
        assert_eq!(store.pending_count(), 1);
        store.complete(id);
        assert_eq!(store.pending_count(), 0);
    }

    #[test]
    fn ids_continue_past_existing_entries_after_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pub.json");
        let store = LocalPublishStore::load(&path).unwrap();
        let _ = store.record("/t", json!({}));
        let id2 = store.record("/t", json!({}));
        store.persist().unwrap();

        let reloaded = LocalPublishStore::load(&path).unwrap();
        // Next id assigned should be strictly greater than the
        // highest existing one.
        let id3 = reloaded.record("/t", json!({}));
        assert!(id3 > id2, "id3={}, id2={}", id3, id2);
    }

    #[test]
    fn loading_nonexistent_file_returns_empty_store() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nope.json");
        let store = LocalPublishStore::load(&path).unwrap();
        assert_eq!(store.pending_count(), 0);
        store.record("/x", json!({}));
        store.persist().unwrap();
        assert!(path.exists());
    }

    #[test]
    fn malformed_existing_file_is_treated_as_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pub.json");
        std::fs::write(&path, b"not json").unwrap();
        let store = LocalPublishStore::load(&path).unwrap();
        assert_eq!(store.pending_count(), 0);
    }
}

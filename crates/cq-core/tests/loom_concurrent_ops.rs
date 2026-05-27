//! Loom models for the remaining concurrency-critical paths.
//!
//! Models (each modelled as a miniature mirror of cqserver's
//! production code path, just small enough that loom can exhaust
//! every interleaving):
//!
//!   1. `delete` racing against `snapshot`. A subscriber's snapshot
//!      must either include the row (delete hadn't taken effect yet)
//!      or exclude it (delete won) — never see a half-deleted state
//!      where `key_to_row` removed the entry but the underlying store
//!      row is still walkable.
//!
//!   2. Two `publish` threads writing the same key concurrently. The
//!      final state must reflect exactly ONE of the publishes (the
//!      one that won the RwLock last). Never a mix.
//!
//!   3. `publish` racing against `read_row` (the path an evaluator
//!      uses to render a delta). With a seqlock-style read, the
//!      reader must either see the OLD complete row or the NEW
//!      complete row — never a torn mix.
//!
//! Run: `RUSTFLAGS="--cfg loom" cargo test -p cq-core --test
//! loom_concurrent_ops`.

#![cfg(loom)]

use cq_core::sync::atomic::{AtomicU64, Ordering};
use cq_core::sync::{thread, Arc, RwLock};
use std::collections::HashMap;

/// Loom-friendly miniature of `StoreState`: a column count, a vector
/// of rows (each row is N values), and a key→row index. Mirrors the
/// production state's shape closely enough that the protocol's
/// concurrency invariants are reproduced.
#[derive(Clone)]
struct State {
    cols: usize,
    rows: Vec<Vec<i64>>,
    /// key → row index in `rows`. A delete removes the entry here AND
    /// nulls out the row (sentinel) so the row index stays stable
    /// (matches cqserver's tombstone-on-delete pattern).
    key_to_row: HashMap<String, usize>,
}

const TOMBSTONE: i64 = i64::MIN;

struct ProtoTopic {
    state: RwLock<State>,
    next_seq: AtomicU64,
}

impl ProtoTopic {
    fn new(cols: usize) -> Self {
        Self {
            state: RwLock::new(State {
                cols,
                rows: Vec::new(),
                key_to_row: HashMap::new(),
            }),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Upsert a key. If the key exists, overwrite in place; otherwise
    /// append a new row and record the index. Returns a sequence.
    /// The entire operation is under `state.write()` — matches
    /// `Topic::commit_values_locked`.
    fn publish(&self, key: &str, values: &[i64]) -> u64 {
        let mut state = self.state.write().unwrap();
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(values.len(), state.cols, "values width must match schema");
        if let Some(&idx) = state.key_to_row.get(key) {
            state.rows[idx] = values.to_vec();
        } else {
            let idx = state.rows.len();
            state.rows.push(values.to_vec());
            state.key_to_row.insert(key.to_string(), idx);
        }
        seq
    }

    /// Delete a key. Removes from `key_to_row` AND tombstones the
    /// row in `rows` (so iterators don't see stale data). Matches
    /// cqserver's delete contract: row index stays valid (for
    /// secondary-index tombstone tracking), but the row's values are
    /// zeroed.
    fn delete(&self, key: &str) {
        let mut state = self.state.write().unwrap();
        if let Some(idx) = state.key_to_row.remove(key) {
            // Tombstone: every column → sentinel.
            let cols = state.cols;
            let row = &mut state.rows[idx];
            for cell in row.iter_mut() {
                *cell = TOMBSTONE;
            }
            // Suppress the unused `cols` warning when loom inlines aggressively.
            let _ = cols;
        }
    }

    /// Take a "live snapshot": for every live key, return its row.
    /// Tombstoned rows are filtered out by the `key_to_row` lookup.
    fn snapshot_live(&self) -> Vec<(String, Vec<i64>)> {
        let state = self.state.read().unwrap();
        let mut out = Vec::with_capacity(state.key_to_row.len());
        for (k, &idx) in state.key_to_row.iter() {
            out.push((k.clone(), state.rows[idx].clone()));
        }
        out
    }

    /// Read a specific row by key. Returns None if the key was
    /// deleted. This models what an evaluator's `evaluate_row` does
    /// when servicing a live delta.
    fn read_row_by_key(&self, key: &str) -> Option<Vec<i64>> {
        let state = self.state.read().unwrap();
        let idx = state.key_to_row.get(key).copied()?;
        Some(state.rows[idx].clone())
    }
}

// ───── Model 1: delete vs snapshot ───────────────────────────────

/// A subscriber's snapshot taken concurrently with a delete must
/// either include the row fully (delete lost the race) or exclude it
/// entirely (delete won) — never include a half-state.
#[test]
fn loom_delete_during_snapshot_is_all_or_nothing() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new(2));
        // Seed one row.
        topic.publish("K", &[10, 20]);

        let topic_d = topic.clone();
        let deleter = thread::spawn(move || topic_d.delete("K"));

        let topic_s = topic.clone();
        let snapshotter = thread::spawn(move || topic_s.snapshot_live());

        deleter.join().unwrap();
        let snap = snapshotter.join().unwrap();

        // Snap is either empty (delete won) or exactly [K → [10,20]].
        match snap.len() {
            0 => { /* delete won — fine */ }
            1 => {
                let (k, row) = &snap[0];
                assert_eq!(k, "K");
                assert_eq!(row, &vec![10, 20], "must see ORIGINAL values, not tombstone");
            }
            n => panic!("unexpected row count: {n}"),
        }
    });
}

/// Same race but with `read_row_by_key` instead of full snapshot.
/// Either Some(original) or None — never Some(tombstone).
#[test]
fn loom_delete_during_per_key_read_is_all_or_nothing() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new(2));
        topic.publish("K", &[100, 200]);

        let topic_d = topic.clone();
        let deleter = thread::spawn(move || topic_d.delete("K"));

        let topic_r = topic.clone();
        let reader = thread::spawn(move || topic_r.read_row_by_key("K"));

        deleter.join().unwrap();
        let got = reader.join().unwrap();
        match got {
            None => { /* delete won */ }
            Some(row) => {
                assert_eq!(row, vec![100, 200], "must see original values, not tombstone");
                // Tombstone marker must not leak through.
                assert!(
                    row.iter().all(|&v| v != TOMBSTONE),
                    "leaked tombstone sentinel"
                );
            }
        }
    });
}

// ───── Model 2: same-key concurrent publish ─────────────────────

/// Two publishers writing the same key concurrently. The final state
/// must reflect exactly ONE of the writes (the one whose write lock
/// won the lock race) — never a mix of fields from both.
#[test]
fn loom_concurrent_publish_same_key_is_last_writer_wins() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new(2));
        topic.publish("K", &[0, 0]); // initial

        let topic_a = topic.clone();
        let a = thread::spawn(move || topic_a.publish("K", &[1, 10]));

        let topic_b = topic.clone();
        let b = thread::spawn(move || topic_b.publish("K", &[2, 20]));

        let seq_a = a.join().unwrap();
        let seq_b = b.join().unwrap();
        assert_ne!(seq_a, seq_b, "sequences must be unique");

        let row = topic.read_row_by_key("K").expect("row must exist");
        // Must be EITHER [1, 10] (A won last) OR [2, 20] (B won last) —
        // never a torn [1, 20] or [2, 10].
        assert!(
            row == vec![1, 10] || row == vec![2, 20],
            "torn row from concurrent publishes: {row:?}"
        );
    });
}

/// Two publishers + one snapshotter on the same key. The snapshot
/// must observe one of the three legal states: original [0,0], A's
/// [1,10], or B's [2,20] — never a mix.
#[test]
fn loom_concurrent_publish_with_observer() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new(2));
        topic.publish("K", &[0, 0]);

        let topic_a = topic.clone();
        let a = thread::spawn(move || topic_a.publish("K", &[1, 10]));

        let topic_b = topic.clone();
        let b = thread::spawn(move || topic_b.publish("K", &[2, 20]));

        let topic_o = topic.clone();
        let obs = thread::spawn(move || topic_o.read_row_by_key("K"));

        a.join().unwrap();
        b.join().unwrap();
        let observed = obs.join().unwrap().expect("row exists");

        let legal = [vec![0, 0], vec![1, 10], vec![2, 20]];
        assert!(
            legal.contains(&observed),
            "observer saw torn row: {observed:?}; legal states: {legal:?}"
        );
    });
}

// ───── Model 3: publish vs concurrent reader (no-tear on update) ──

/// One writer races a reader on the SAME row. The reader must
/// observe a complete pre-update row OR a complete post-update row
/// — never a half-updated row.
///
/// Production: `ColumnStore::update_row` uses a seqlock + fence
/// pattern so a parallel `get` either sees v (old, complete) or v+2
/// (new, complete). Modelling that exactly here would require
/// duplicating the seqlock; instead we rely on the `RwLock` envelope
/// (every publish/read acquires the lock). The model proves the
/// envelope ALONE is sufficient.
#[test]
fn loom_writer_does_not_tear_concurrent_reader() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new(3));
        topic.publish("K", &[1, 2, 3]);

        let topic_w = topic.clone();
        let writer = thread::spawn(move || topic_w.publish("K", &[100, 200, 300]));

        let topic_r = topic.clone();
        let reader = thread::spawn(move || topic_r.read_row_by_key("K"));

        writer.join().unwrap();
        let r = reader.join().unwrap().expect("row exists");

        // Legal observations: pre-update OR post-update. Anything else
        // is a torn read.
        assert!(
            r == vec![1, 2, 3] || r == vec![100, 200, 300],
            "torn read: {r:?}"
        );
    });
}

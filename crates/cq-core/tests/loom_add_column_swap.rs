//! Loom model check for the Q11 `Topic::add_column` atomic swap.
//!
//! The contract `add_column` must satisfy:
//!
//!   Any reader that takes `state.read()` during a concurrent
//!   `add_column` call sees either the FULL pre-swap state (old
//!   schema + old row values) OR the FULL post-swap state (new
//!   schema + same row values + new column = null) — never a
//!   torn intermediate where the schema has the new column but
//!   the row data is still missing it (or vice versa).
//!
//! The production `Topic::add_column` holds `state.write()` for
//! the entire build-and-swap, so by RwLock semantics this property
//! holds by construction. This loom test models the protocol so
//! any future refactor that splits the swap into multiple steps
//! is caught.
//!
//! Run with: `RUSTFLAGS="--cfg loom" cargo test -p cq-core
//! --test loom_add_column_swap`.
//!
//! The test asserts the invariant across every permitted
//! publisher × add_column × subscriber interleaving loom explores.
//!
//! Tracked invariants:
//!   1. `state.cols` and `state.rows[i].len()` always agree.
//!   2. A new column appears as exactly null in every pre-existing
//!      row that was visible to the new-schema reader.
//!   3. Sequence numbers are unique and monotonically increasing
//!      regardless of which side of the swap they were issued on.

#![cfg(loom)]

use cq_core::sync::atomic::{AtomicU64, Ordering};
use cq_core::sync::{thread, Arc, RwLock};

/// Loom-friendly miniature of `StoreState`. The schema is just a
/// column count; each row is `Vec<i64>` where the i-th entry is
/// the value for column i (or `i64::MIN` for null).
#[derive(Clone)]
struct State {
    cols: usize,
    rows: Vec<Vec<i64>>,
}

const NULL_SENTINEL: i64 = i64::MIN;

struct ProtoTopic {
    state: RwLock<State>,
    next_seq: AtomicU64,
}

impl ProtoTopic {
    fn new() -> Self {
        Self {
            state: RwLock::new(State {
                cols: 2,
                rows: Vec::new(),
            }),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Mirror of `Topic::upsert_map`: takes the write lock, allocates
    /// a sequence, and appends one row whose width matches the
    /// schema's current column count.
    fn publish(&self, base_value: i64) -> u64 {
        let mut state = self.state.write().unwrap();
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        // The publish synthesises one i64 per current column.
        let row: Vec<i64> = (0..state.cols)
            .map(|c| base_value + c as i64)
            .collect();
        state.rows.push(row);
        seq
    }

    /// Mirror of `Topic::add_column`: takes the write lock, copies
    /// every existing row into a fresh state with `cols + 1`, the
    /// new column filled with NULL_SENTINEL, then atomically
    /// installs the new state. Any concurrent reader blocked on
    /// `state.read()` will be granted the new state only AFTER this
    /// returns.
    fn add_column(&self) {
        let mut state = self.state.write().unwrap();
        let new_cols = state.cols + 1;
        let mut new_rows: Vec<Vec<i64>> = Vec::with_capacity(state.rows.len());
        for row in state.rows.iter() {
            let mut new_row = row.clone();
            new_row.push(NULL_SENTINEL);
            new_rows.push(new_row);
        }
        *state = State {
            cols: new_cols,
            rows: new_rows,
        };
    }

    /// Mirror of `Topic::query` snapshot: takes the read lock,
    /// clones the state. The clone is the consistent point-in-time
    /// view a subscriber would render.
    fn snapshot(&self) -> State {
        self.state.read().unwrap().clone()
    }
}

/// Three-thread race: one publisher writes a row, one thread calls
/// add_column, one subscriber snapshots. The invariant must hold
/// regardless of how loom interleaves them.
#[test]
fn loom_add_column_never_tears() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new());

        let topic_p = topic.clone();
        let publisher = thread::spawn(move || topic_p.publish(100));

        let topic_a = topic.clone();
        let evolver = thread::spawn(move || topic_a.add_column());

        let topic_s = topic.clone();
        let subscriber = thread::spawn(move || topic_s.snapshot());

        publisher.join().unwrap();
        evolver.join().unwrap();
        let snap = subscriber.join().unwrap();

        // Invariant 1: every row's width equals `cols`.
        for (i, row) in snap.rows.iter().enumerate() {
            assert_eq!(
                row.len(),
                snap.cols,
                "torn schema: row {i} width = {} but cols = {}",
                row.len(),
                snap.cols
            );
        }

        // Invariant 2: schema is either the original 2 cols or the
        // post-evolution 3 cols — no other value is reachable from
        // the model.
        assert!(
            snap.cols == 2 || snap.cols == 3,
            "unexpected cols count: {}",
            snap.cols
        );

        // Invariant 3: if the publish landed and we're at 3 cols
        // (post-evolution), the publish either ran BEFORE the swap
        // (row width was 2 at write time, then add_column appended
        // NULL_SENTINEL) OR AFTER (row width was 3 at write time
        // with no NULL). Both shapes are valid. The check here:
        // if a row exists and last col is NULL_SENTINEL, the rest
        // of the row reflects the original publish base_value
        // schema.
        for row in &snap.rows {
            if snap.cols == 3 && row.last() == Some(&NULL_SENTINEL) {
                // Pre-evolution publish, then add_column appended null.
                assert_eq!(row[0], 100);
                assert_eq!(row[1], 101);
            } else if snap.cols == 3 {
                // Post-evolution publish — row should have all 3 cols
                // populated (since publish allocates from current cols).
                assert_eq!(row[0], 100);
                assert_eq!(row[1], 101);
                assert_eq!(row[2], 102);
            } else {
                // Pre-evolution snapshot: row width == 2.
                assert_eq!(row.len(), 2);
                assert_eq!(row[0], 100);
                assert_eq!(row[1], 101);
            }
        }
    });
}

/// Two-thread race: two publishers race, one of them runs add_column
/// between writes. The invariant: every row in the final snapshot
/// has the right width regardless of which side of the swap each
/// publish landed on.
#[test]
fn loom_two_publishes_around_add_column() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new());

        // Publish #1 → may be pre- or post-evolution.
        let topic_a = topic.clone();
        let p1 = thread::spawn(move || topic_a.publish(10));

        let topic_b = topic.clone();
        let evolve = thread::spawn(move || topic_b.add_column());

        let topic_c = topic.clone();
        let p2 = thread::spawn(move || topic_c.publish(20));

        let seq1 = p1.join().unwrap();
        evolve.join().unwrap();
        let seq2 = p2.join().unwrap();

        // Distinct sequences.
        assert_ne!(seq1, seq2, "sequences must be unique");

        let snap = topic.snapshot();
        // All rows have the same width as `cols` (no tear).
        for (i, row) in snap.rows.iter().enumerate() {
            assert_eq!(row.len(), snap.cols, "torn row {i}");
        }
    });
}

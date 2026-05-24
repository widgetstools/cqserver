//! Loom model check for the `sow_and_subscribe` atomicity contract
//! (review concern C1 / worklog session S32, test C1.3).
//!
//! Loom can't drive `Topic` directly — production code uses
//! `parking_lot::RwLock`, `crossbeam-channel`, and assorted non-loom
//! primitives. What loom CAN do is exhaustively model-check the
//! *protocol* that `subscribe_inner` and `write_store` follow:
//!
//!   - Writers increment `next_sequence` UNDER `state.write()` and
//!     commit the mutation in the same critical section.
//!   - Subscribers load `next_sequence` UNDER `state.read()`, treating
//!     the loaded value as a high-water mark for "every mutation with
//!     `seq ≤ captured` is in my snapshot."
//!
//! The contract holds iff one of these is always true at the end of
//! any interleaving of one publisher and one subscriber:
//!   (a) snapshot contains the published row AND captured ≥ pub_seq
//!       (live event will be suppressed → no duplicate), or
//!   (b) snapshot does NOT contain the row AND captured < pub_seq
//!       (live_start = captured + 1 ≤ pub_seq, so the event passes the
//!       evaluator's `seq < live_start` gate and is delivered live).
//!
//! Loom explores every permitted thread interleaving (subject to the
//! memory model) and asserts the contract on each.

#![cfg(loom)]

use cq_core::sync::atomic::{AtomicU64, Ordering};
use cq_core::sync::{thread, Arc, RwLock};

struct State {
    rows: Vec<u64>,
}

struct ProtoTopic {
    state: RwLock<State>,
    next_seq: AtomicU64,
}

impl ProtoTopic {
    fn new() -> Self {
        Self {
            state: RwLock::new(State { rows: Vec::new() }),
            next_seq: AtomicU64::new(0),
        }
    }

    /// Mirror of `Topic::write_store`: allocate the sequence and mutate
    /// the store inside the same write-lock critical section, so a
    /// concurrent subscriber that observes `next_seq = N` after their
    /// read-lock is granted necessarily sees every mutation up to N
    /// in the snapshot they just took.
    fn publish(&self, value: u64) -> u64 {
        let mut state = self.state.write().unwrap();
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst) + 1;
        state.rows.push(value);
        seq
    }

    /// Mirror of `Topic::subscribe_inner`: snapshot the store and
    /// capture the sequence high-water, both under the same read-lock
    /// critical section.
    fn subscribe(&self) -> (Vec<u64>, u64) {
        let state = self.state.read().unwrap();
        let snapshot = state.rows.clone();
        let captured = self.next_seq.load(Ordering::SeqCst);
        (snapshot, captured)
    }
}

/// Two-thread race: one publisher commits one row, one subscriber
/// snapshots concurrently. The contract must hold in every permitted
/// interleaving.
#[test]
fn loom_subscribe_publish_race_satisfies_contract() {
    loom::model(|| {
        let topic = Arc::new(ProtoTopic::new());

        let topic_p = topic.clone();
        let publisher = thread::spawn(move || topic_p.publish(42));

        let topic_s = topic.clone();
        let subscriber = thread::spawn(move || topic_s.subscribe());

        let pub_seq = publisher.join().unwrap();
        let (snapshot, captured) = subscriber.join().unwrap();
        let saw_row = snapshot.contains(&42);

        if saw_row {
            // Subscriber's snapshot includes the row → its captured
            // high-water must cover the publish, so live_start =
            // captured + 1 > pub_seq, and the evaluator suppresses
            // the queued live event for `pub_seq`.
            assert!(
                captured >= pub_seq,
                "duplicate delta race: snapshot saw row 42 (seq {pub_seq}) but \
                 subscriber captured only {captured} → evaluator will redeliver \
                 the live event"
            );
        } else {
            // Subscriber's snapshot missed the row → live_start =
            // captured + 1 must be ≤ pub_seq so the live event
            // passes the gate and is delivered.
            assert!(
                captured < pub_seq,
                "missed delta race: snapshot did not see row 42 (seq {pub_seq}) but \
                 subscriber captured {captured} → live_start = {} would suppress \
                 the live event",
                captured + 1
            );
        }
    });
}

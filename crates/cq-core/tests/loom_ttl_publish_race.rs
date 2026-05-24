//! Loom model check for the TTL-sweep × publish race (worklog S40,
//! review C9 test C9.1).
//!
//! Models the protocol `Topic::delete_if_still_expired` uses: a
//! sweeper observes `last_touched` in a read pass, then re-checks
//! `last_touched` **under the same write lock** that publishers
//! mutate before deciding to delete. Under that protocol, any
//! interleaving of publish + sweep ends in a state where the
//! publish's mutation is preserved — even when the sweep "wins"
//! the lock first.

#![cfg(loom)]

use cq_core::sync::{thread, Arc, Mutex};

/// Minimal model of one row's TTL-relevant state: a logical
/// timestamp (also serves as the "present" indicator — 0 = absent,
/// >0 = present at that logical time).
#[derive(Default)]
struct Row {
    last_touched: u64,
}

#[test]
fn loom_publish_during_sweep_is_never_lost() {
    loom::model(|| {
        // Initial state: row exists at logical time 1.
        let row = Arc::new(Mutex::new(Row { last_touched: 1 }));
        let sweep_observed_at = 1u64;
        let publish_new_ts = 2u64;

        let row_p = row.clone();
        let publisher = thread::spawn(move || {
            let mut r = row_p.lock().unwrap();
            r.last_touched = publish_new_ts;
        });

        let row_s = row.clone();
        let sweeper = thread::spawn(move || {
            let mut r = row_s.lock().unwrap();
            // Re-check under the same lock the publisher uses.
            if r.last_touched == sweep_observed_at {
                // Still expired (publisher hasn't refreshed it yet);
                // delete. Use 0 as the "absent" sentinel.
                r.last_touched = 0;
            }
            // else: publisher already refreshed → skip the delete.
        });

        publisher.join().unwrap();
        sweeper.join().unwrap();

        // Invariant: the publisher always ran (sync join). The
        // publisher's mutation (last_touched = 2) must be the FINAL
        // observed state — either because publisher ran after
        // sweeper (sweep saw 1, deleted to 0, publisher wrote 2),
        // OR because publisher ran first (last_touched = 2, sweep
        // observed 2 != 1, skipped). Sweep MUST NOT win the race
        // such that final state is 0.
        let r = row.lock().unwrap();
        assert_eq!(
            r.last_touched, publish_new_ts,
            "TTL sweep lost a published row: final last_touched = {}",
            r.last_touched
        );
    });
}

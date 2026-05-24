//! Loom model check for the per-row seqlock protocol that guards
//! against column tear (review concern C2 / worklog session S33, test
//! C2.1).
//!
//! The model: a single "row" with three logical columns `(q, p, n)`,
//! plus a `row_version` atomic that follows the same odd-during-write /
//! even-after-write convention as `ColumnStore::row_versions`. The
//! writer maintains the invariant `n == q * p` across every write.
//! The reader applies the seqlock retry-loop and verifies the
//! invariant on every observation.
//!
//! Loom exhaustively interleaves the writer's three column stores
//! with the reader's three column loads. Without the seqlock, the
//! reader can observe `q` from one write and `n` from another → the
//! invariant breaks. With the seqlock, every observation comes from
//! a consistent (start, end) version pair, so the invariant always
//! holds.

#![cfg(loom)]

use cq_core::sync::atomic::{fence, AtomicU64, Ordering};
use cq_core::sync::{thread, Arc};

struct Row {
    /// Per-row seqlock version. Even = consistent, odd = mid-write.
    version: AtomicU64,
    q: AtomicU64,
    p: AtomicU64,
    n: AtomicU64,
}

impl Row {
    fn new(q: u64, p: u64) -> Self {
        Self {
            version: AtomicU64::new(0),
            q: AtomicU64::new(q),
            p: AtomicU64::new(p),
            n: AtomicU64::new(q * p),
        }
    }

    /// Writer-side: mirror of `ColumnStore::update_row`. Flip version to
    /// odd, fence, write all columns, fence, flip to even + 2.
    fn write(&self, q: u64, p: u64) {
        let v = self.version.load(Ordering::Relaxed);
        assert!(v % 2 == 0, "concurrent writer detected");
        self.version.store(v + 1, Ordering::Release);
        fence(Ordering::Release);
        self.q.store(q, Ordering::Relaxed);
        self.p.store(p, Ordering::Relaxed);
        self.n.store(q * p, Ordering::Relaxed);
        fence(Ordering::Release);
        self.version.store(v + 2, Ordering::Release);
    }

    /// Reader-side: mirror of `ColumnStore::read_row_consistent`.
    /// Bounded to a finite retry budget so loom's exhaustive interleave
    /// terminates; one writer + a few retries is enough to cover every
    /// interesting schedule. In production code the retry loop is
    /// unbounded (the writer is guaranteed to release the seqlock in
    /// finite time).
    fn read_consistent(&self) -> Option<(u64, u64, u64)> {
        for _ in 0..8 {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 % 2 != 0 {
                loom::thread::yield_now();
                continue;
            }
            fence(Ordering::Acquire);
            let q = self.q.load(Ordering::Relaxed);
            let p = self.p.load(Ordering::Relaxed);
            let n = self.n.load(Ordering::Relaxed);
            fence(Ordering::Acquire);
            let v2 = self.version.load(Ordering::Acquire);
            if v1 == v2 {
                return Some((q, p, n));
            }
            loom::thread::yield_now();
        }
        None
    }
}

#[test]
fn loom_seqlock_prevents_column_tear() {
    loom::model(|| {
        let row = Arc::new(Row::new(2, 3));

        let row_w = row.clone();
        let writer = thread::spawn(move || {
            // Maintain `n = q * p` across the write.
            row_w.write(5, 7);
        });

        let row_r = row.clone();
        let reader = thread::spawn(move || {
            if let Some((q, p, n)) = row_r.read_consistent() {
                // The seqlock protocol must deliver a self-consistent
                // observation: either the initial (2, 3, 6) or the
                // post-write (5, 7, 35). Any blend (e.g., q=5, p=3,
                // n=15) would be a column tear.
                assert_eq!(
                    n,
                    q * p,
                    "column tear: observed q={q}, p={p}, n={n} — but n != q*p"
                );
            }
            // Hitting the retry budget without a consistent read is
            // fine for the loom model — what matters is that whenever
            // we DO return a result, it satisfies the invariant.
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

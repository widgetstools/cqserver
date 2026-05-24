//! Real-thread stress test for the per-row seqlock (review concern C2 /
//! worklog session S33, test C2.2).
//!
//! One writer thread continuously updates `(q, p, n=q*p)` for a single
//! row, advancing `q` and `p` each iteration. 16 reader threads
//! continuously read the row via the seqlock protocol and check the
//! invariant `n == q * p`. The test fails on the first observed
//! violation.
//!
//! The model uses an interior-mutable struct that mirrors
//! `ColumnStore::row_versions` semantics directly. The production
//! `ColumnStore` requires `&mut self` for writes (writes are
//! serialized by `state.write()` in the parent `StoreState`), so the
//! seqlock there is forward-prep for lock-free reader paths rather
//! than today's hot path. This test exercises the protocol in
//! isolation, on real OS threads, with 16-way reader contention —
//! catching any reorder or fence bug the loom model would miss only
//! because loom can't replay enough write iterations to fuzz the
//! schedule.

use std::sync::atomic::{fence, AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Three-column row with an embedded seqlock. Mirrors the layout
/// `ColumnStore` uses for one row × three numeric columns.
struct Row {
    version: AtomicU64,
    q: AtomicU64,
    p: AtomicU64,
    n: AtomicU64,
}

impl Row {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            q: AtomicU64::new(1),
            p: AtomicU64::new(1),
            n: AtomicU64::new(1),
        }
    }

    fn write(&self, q: u64, p: u64) {
        let v = self.version.load(Ordering::Relaxed);
        debug_assert!(v % 2 == 0);
        self.version.store(v + 1, Ordering::Release);
        fence(Ordering::Release);
        self.q.store(q, Ordering::Relaxed);
        self.p.store(p, Ordering::Relaxed);
        self.n.store(q.wrapping_mul(p), Ordering::Relaxed);
        fence(Ordering::Release);
        self.version.store(v + 2, Ordering::Release);
    }

    fn read_consistent(&self) -> (u64, u64, u64) {
        loop {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }
            fence(Ordering::Acquire);
            let q = self.q.load(Ordering::Relaxed);
            let p = self.p.load(Ordering::Relaxed);
            let n = self.n.load(Ordering::Relaxed);
            fence(Ordering::Acquire);
            let v2 = self.version.load(Ordering::Acquire);
            if v1 == v2 {
                return (q, p, n);
            }
        }
    }
}

/// 16 readers × 1 writer × 1s. Zero invariant violations expected.
/// The test panics on the FIRST violation so we get an easily
/// diagnosable failure rather than a count.
#[test]
fn stress_seqlock_holds_under_16_readers_one_writer() {
    let row = Arc::new(Row::new());
    let stop = Arc::new(AtomicBool::new(false));
    let violations = Arc::new(AtomicU64::new(0));

    let row_w = row.clone();
    let stop_w = stop.clone();
    let writer = thread::spawn(move || {
        let mut iter: u64 = 1;
        while !stop_w.load(Ordering::Relaxed) {
            let q = iter;
            let p = iter.wrapping_mul(2);
            row_w.write(q, p);
            iter = iter.wrapping_add(1);
        }
        iter
    });

    let mut readers = Vec::new();
    for _ in 0..16 {
        let row_r = row.clone();
        let stop_r = stop.clone();
        let violations_r = violations.clone();
        readers.push(thread::spawn(move || {
            let mut reads: u64 = 0;
            while !stop_r.load(Ordering::Relaxed) {
                let (q, p, n) = row_r.read_consistent();
                if n != q.wrapping_mul(p) {
                    violations_r.fetch_add(1, Ordering::Relaxed);
                }
                reads = reads.wrapping_add(1);
            }
            reads
        }));
    }

    let started = Instant::now();
    let duration = Duration::from_secs(1);
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);

    let writes = writer.join().expect("writer");
    let total_reads: u64 = readers
        .into_iter()
        .map(|h| h.join().expect("reader"))
        .sum();
    let elapsed_ms = started.elapsed().as_millis();

    let violations = violations.load(Ordering::Relaxed);
    eprintln!(
        "stress run: {writes} writes, {total_reads} reads over {elapsed_ms}ms — \
         {violations} invariant violations"
    );

    assert_eq!(
        violations, 0,
        "seqlock failed: {violations} column-tear observations across \
         {total_reads} reads (writer completed {writes} writes)"
    );
    // Confidence guard: if the writer ran but nobody actually
    // observed anything, the test is meaningless — flag that.
    assert!(
        writes >= 1000 && total_reads >= 1000,
        "stress test under-exercised: writes={writes}, reads={total_reads}"
    );
}

//! Smoke test proving the `--cfg loom` build path compiles and executes.
//!
//! Run with:
//!   RUSTFLAGS="--cfg loom" cargo test --release --test loom_smoke
//!
//! Under standard `cargo test` the file compiles to an empty binary
//! (`#![cfg(loom)]`), which keeps `cargo test --workspace` clean while
//! still letting loom-only tests live alongside the rest of the suite.

#![cfg(loom)]

use cq_core::sync::atomic::{AtomicUsize, Ordering};
use cq_core::sync::{thread, Arc};

#[test]
fn two_writers_one_atomic_observe_final_value() {
    loom::model(|| {
        let counter = Arc::new(AtomicUsize::new(0));

        let c1 = counter.clone();
        let t1 = thread::spawn(move || {
            c1.fetch_add(1, Ordering::Relaxed);
        });

        let c2 = counter.clone();
        let t2 = thread::spawn(move || {
            c2.fetch_add(1, Ordering::Relaxed);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    });
}

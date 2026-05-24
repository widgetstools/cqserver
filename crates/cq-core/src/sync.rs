//! Concurrency-primitive shim that swaps between `std::sync` / `std::thread`
//! and `loom::sync` / `loom::thread` based on the `--cfg loom` flag.
//!
//! Why: under `RUSTFLAGS="--cfg loom"`, loom's primitives drive an
//! exhaustive model checker that explores all permitted thread interleavings
//! and surfaces races deterministically. Production code that uses these
//! re-exports compiles unchanged under both configurations.
//!
//! Use `cq_core::sync::Arc`, `cq_core::sync::atomic::*`, and
//! `cq_core::sync::thread::*` in any code that participates in loom tests.

#[cfg(loom)]
pub use loom::sync::{Arc, Mutex, RwLock};
#[cfg(loom)]
pub mod atomic {
    pub use loom::sync::atomic::{
        fence, AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU32, AtomicU64, AtomicUsize,
        Ordering,
    };
}
#[cfg(loom)]
pub use loom::thread;

#[cfg(not(loom))]
pub use std::sync::{Arc, Mutex, RwLock};
#[cfg(not(loom))]
pub mod atomic {
    pub use std::sync::atomic::{
        fence, AtomicBool, AtomicI32, AtomicI64, AtomicIsize, AtomicU32, AtomicU64, AtomicUsize,
        Ordering,
    };
}
#[cfg(not(loom))]
pub use std::thread;

//! S21 stress: a slow draining consumer plus a hot writer interleave
//! correctly — spillover order is preserved end to end, and the
//! consumer ultimately receives every frame the writer produced.
//!
//! Scenario: a writer thread pushes N frames at line rate through
//! `Spillover::write_frame` while a reader thread pops them via
//! `Spillover::read_next_frame`. The reader keeps pace with a small
//! random jitter so writes occasionally outrun reads (and the
//! pending backlog grows). At the end, every frame must come out in
//! the same order it went in.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use cq_transport::session::OutboundFrame;
use cq_transport::spillover::Spillover;
use tempfile::tempdir;

#[test]
fn spillover_preserves_order_under_concurrent_write_read() {
    let dir = tempdir().unwrap();
    // 64 MiB cap — comfortably exceeds the 10K frames we write.
    let sp = Arc::new(
        Spillover::open(dir.path().join("stress.spill"), 64 * 1024 * 1024)
            .expect("open"),
    );
    let n_frames = 10_000usize;

    // Writer thread.
    let sp_w = sp.clone();
    let writer = thread::spawn(move || {
        for i in 0..n_frames {
            let payload = format!("frame-{:06}", i);
            sp_w.write_frame(&OutboundFrame::Text(payload))
                .expect("write");
            // Tiny natural jitter so the reader sometimes drains
            // faster than we write.
            if i % 64 == 0 {
                thread::sleep(Duration::from_micros(50));
            }
        }
    });

    // Reader thread: poll for frames until we've drained every one.
    let sp_r = sp.clone();
    let reader = thread::spawn(move || {
        let mut received: Vec<String> = Vec::with_capacity(n_frames);
        while received.len() < n_frames {
            match sp_r.read_next_frame() {
                Ok(Some(OutboundFrame::Text(s))) => received.push(s),
                Ok(Some(OutboundFrame::Binary(_))) => {
                    panic!("unexpected binary frame")
                }
                Ok(None) => thread::sleep(Duration::from_micros(100)),
                Err(e) => panic!("read failed: {}", e),
            }
        }
        received
    });

    writer.join().expect("writer");
    let received = reader.join().expect("reader");
    assert_eq!(received.len(), n_frames);
    for (i, frame) in received.iter().enumerate() {
        let expected = format!("frame-{:06}", i);
        assert_eq!(frame, &expected, "out-of-order at index {i}");
    }
}

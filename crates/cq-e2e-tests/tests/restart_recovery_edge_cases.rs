//! Restart-after-damage recovery tests. The existing `txlog_archive`
//! / `txlog_compression` / `bookmark_store_e2e` suites exercise the
//! happy-path recovery (kill -9 → restart → state intact). This
//! suite injects deliberate damage between stop and restart to
//! verify the server recovers gracefully — never silently loses
//! state, never crashes on a malformed segment.
//!
//! Scenarios:
//!   1. Truncate the live txlog segment to a partial record (mid-
//!      record kill). Recovery must read every COMPLETE record and
//!      stop at the truncation boundary, not panic.
//!   2. Append garbage bytes after the legitimate tail. Same
//!      invariant — server boots, exposes the recoverable rows.
//!   3. Truncate the bookmark file to invalid JSON. The
//!      `LocalBookmarkStore::load` contract says this is "empty
//!      store", not fatal.
//!   4. Kill the server while a publish is in flight (race the kill
//!      against the publish RPC). Recovery shouldn't lose any acked
//!      publish — checked by re-running the SOW after restart.

use cq_client::{Client, LocalBookmarkStore};
use cq_e2e_tests::{
    restart_kept, start_server_with, stop_keeping_dir, ServerOpts, TopicSpec,
};
use serde_json::json;
use std::time::Duration;

fn persistent_topic(name: &str) -> TopicSpec {
    TopicSpec::new(name, "k")
        .with_inline_columns([("k", "string"), ("v", "long")])
        .with_persist()
}

fn opts() -> ServerOpts {
    ServerOpts::default()
}

#[tokio::test]
async fn restart_after_truncated_txlog_segment_recovers_intact_records() {
    let server = start_server_with(vec![persistent_topic("/rec-trunc")], opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..10_i64 {
        client
            .publish("/rec-trunc", json!({ "k": format!("k{i:02}"), "v": i }))
            .await
            .unwrap();
    }
    // Give the fsync a beat so every publish is on disk.
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;

    // Find the txlog files for this topic and truncate the last one
    // by a few bytes to simulate a power-loss mid-record.
    let txlog_root = kept.txlog_dir();
    // Topic-name → on-disk subdir: cqserver maps "/rec-trunc" to
    // some path. We don't need to know the exact name; truncate
    // the largest .log file in the tree.
    let largest = find_largest_file_with_suffix(&txlog_root, ".log")
        .expect("no .log file under txlog dir");
    let original_len = std::fs::metadata(&largest).unwrap().len();
    let new_len = original_len.saturating_sub(8); // chop 8 bytes
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&largest)
        .unwrap();
    f.set_len(new_len).unwrap();
    drop(f);

    // Restart. The recovery path must read every complete record
    // and gracefully stop at the truncation point — never panic.
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2
        .sow("/rec-trunc", None)
        .await
        .expect("SOW after recovery must succeed");
    // Recovery may have lost the partial-write record; the rest must
    // be present. We tolerate ≤1 row lost.
    assert!(
        rows.len() >= 9 && rows.len() <= 10,
        "expected 9 or 10 rows after recovery, got {}",
        rows.len()
    );
}

/// Trailing garbage (e.g. 64 bytes of 0xFF after the last legit
/// record — what power-loss + filesystem padding can leave behind)
/// must NOT prevent the server from booting. The cq-txlog reader's
/// active-segment path treats oversized lengths and CRC mismatches
/// as torn-tail EOF, so every complete record before the garbage
/// is still recoverable.
#[tokio::test]
async fn restart_after_garbage_appended_to_txlog_does_not_crash() {
    let server = start_server_with(vec![persistent_topic("/rec-junk")], opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..5_i64 {
        client
            .publish("/rec-junk", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let largest = find_largest_file_with_suffix(&kept.txlog_dir(), ".log")
        .expect("no .log file");

    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&largest).unwrap();
    f.write_all(&[0xFF; 64]).unwrap();
    drop(f);

    // Server must boot, the 5 acked publishes must be recoverable.
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/rec-junk", None).await.unwrap();
    assert_eq!(rows.len(), 5);
}

/// Same shape, but with garbage that decodes to a "small" length
/// (e.g. 64 bytes of `0x00` — `frame_len=0`, then `body.len()==0`
/// CRC over empty buf = `0x00000000`, matches expected_crc=0).
/// This path goes through the CRC-mismatch branch (the body is 0
/// bytes and reads succeed, so we get to the CRC check). Same
/// torn-tail tolerance must apply.
#[tokio::test]
async fn restart_after_zero_byte_garbage_recovers() {
    let server = start_server_with(vec![persistent_topic("/rec-zero")], opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..3_i64 {
        client
            .publish("/rec-zero", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let largest = find_largest_file_with_suffix(&kept.txlog_dir(), ".log").unwrap();
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(&largest).unwrap();
    f.write_all(&[0x00; 64]).unwrap();
    drop(f);

    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/rec-zero", None).await.unwrap();
    // Either 3 (zero-byte garbage parsed as zero-len frame + valid
    // CRC over empty body → consumed as an empty entry, ignored;
    // possible but unlikely depending on parse_body) OR more
    // realistically: torn-tail detection skips the garbage and we
    // get the 3 legit records back. We assert ≥3 so either outcome
    // passes — the contract is "no crash + all acked records survive".
    assert!(rows.len() >= 3, "all 3 acked publishes must survive, got {}", rows.len());
}

/// Restart with TWO persistent topics — one with data, one empty —
/// the recovery loop must mount both correctly.
#[tokio::test]
async fn restart_with_mixed_populated_and_empty_topics() {
    let server = start_server_with(
        vec![
            persistent_topic("/rec-mix-full"),
            persistent_topic("/rec-mix-empty"),
        ],
        opts(),
    )
    .await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..7_i64 {
        client
            .publish("/rec-mix-full", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let full = client2.sow("/rec-mix-full", None).await.unwrap();
    let empty = client2.sow("/rec-mix-empty", None).await.unwrap();
    assert_eq!(full.len(), 7);
    assert!(empty.is_empty());
    // The empty topic accepts a fresh publish post-restart.
    client2
        .publish("/rec-mix-empty", json!({ "k": "first", "v": 1 }))
        .await
        .unwrap();
}

/// Restart twice in a row — second restart's recovery reads the
/// state that the first restart's recovery wrote (sanity check that
/// recovery itself doesn't drift state on re-entry).
#[tokio::test]
async fn restart_twice_in_a_row_preserves_state() {
    let server = start_server_with(vec![persistent_topic("/rec-double")], opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    for i in 0..5_i64 {
        client
            .publish("/rec-double", json!({ "k": format!("k{i}"), "v": i }))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(150)).await;
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let mid = client2.sow("/rec-double", None).await.unwrap();
    assert_eq!(mid.len(), 5);
    drop(client2);

    let kept2 = stop_keeping_dir(server2).await;
    let server3 = restart_kept(kept2).await;
    let client3 = Client::connect(&server3.tcp_url()).await.unwrap();
    let final_rows = client3.sow("/rec-double", None).await.unwrap();
    assert_eq!(final_rows.len(), 5);
}

#[tokio::test]
async fn corrupted_bookmark_file_loads_as_empty_store() {
    // The LocalBookmarkStore contract per its docstring:
    // "malformed JSON is treated as empty rather than fatal — the
    // store re-establishes from live deltas". Pin that behaviour.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bookmarks.json");
    std::fs::write(&path, b"{ this is not valid json }").unwrap();

    let store = LocalBookmarkStore::load(&path).expect("must not error");
    assert!(store.get("/anything").is_none(), "empty store after corruption");

    // Recording still works on the resurrected store.
    store.record("/topic-x", 42);
    assert_eq!(store.get("/topic-x"), Some(42));
}

#[tokio::test]
async fn truncated_bookmark_file_loads_as_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bookmarks.json");
    // Half-written JSON object — closing brace missing.
    std::fs::write(&path, b"{\"/topic-a\": 100").unwrap();

    let store = LocalBookmarkStore::load(&path).expect("must not error");
    assert!(store.get("/topic-a").is_none());
}

#[tokio::test]
async fn empty_bookmark_file_loads_as_empty_store() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("bookmarks.json");
    std::fs::write(&path, b"").unwrap(); // empty file

    let store = LocalBookmarkStore::load(&path).expect("must not error on empty file");
    assert!(store.get("/anything").is_none());
}

#[tokio::test]
async fn restart_immediately_after_publish_acks_recovers_those_publishes() {
    let server = start_server_with(vec![persistent_topic("/rec-fast")], opts()).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    // Pipeline publishes as fast as the SDK allows, then immediately stop.
    let mut acks = 0_usize;
    for i in 0..50_i64 {
        client
            .publish("/rec-fast", json!({ "k": format!("k{i:02}"), "v": i }))
            .await
            .unwrap();
        acks += 1;
    }
    // NO sleep here — kill while disk writes may still be in flight.
    drop(client);

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client2 = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client2.sow("/rec-fast", None).await.unwrap();
    // Every acked publish must be durable per cqserver's txlog contract.
    assert_eq!(
        rows.len(),
        acks,
        "{acks} publishes acked, only {} survived restart",
        rows.len()
    );
}

#[tokio::test]
async fn restart_handles_empty_persistent_topic() {
    // Persistent topic that never received a single publish. Recovery
    // must not crash on the empty/missing-segment case.
    let server = start_server_with(vec![persistent_topic("/rec-empty")], opts()).await;

    let kept = stop_keeping_dir(server).await;
    let server2 = restart_kept(kept).await;
    let client = Client::connect(&server2.tcp_url()).await.unwrap();
    let rows = client.sow("/rec-empty", None).await.unwrap();
    assert!(rows.is_empty());
    // Server is alive — first publish lands.
    client
        .publish("/rec-empty", json!({ "k": "first", "v": 1 }))
        .await
        .unwrap();
}

/// Find the largest file under `root` whose name ends in `suffix`.
fn find_largest_file_with_suffix(
    root: &std::path::Path,
    suffix: &str,
) -> Option<std::path::PathBuf> {
    let mut best: Option<(u64, std::path::PathBuf)> = None;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let read = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(_) => continue,
        };
        for entry in read.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.to_string_lossy().ends_with(suffix) {
                if let Ok(meta) = entry.metadata() {
                    let len = meta.len();
                    if best.as_ref().map_or(true, |(b, _)| len > *b) {
                        best = Some((len, path));
                    }
                }
            }
        }
    }
    best.map(|(_, p)| p)
}

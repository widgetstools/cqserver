//! e2e: chunked SOW wire shape.
//!
//! Talks directly to the cqserver TCP transport without the Rust SDK,
//! decoding length-prefixed JSON frames, so we can assert the actual
//! frame shape: many `sow_batch` frames (each holding N rows), zero
//! per-row `sow` frames, exactly one `group_begin` and one `group_end`.

use cq_client::Client;
use cq_e2e_tests::{start_server_with, ServerOpts, TopicSpec};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn write_framed(s: &mut TcpStream, msg: &Value) {
    let payload = serde_json::to_vec(msg).unwrap();
    let len = (payload.len() as u32).to_be_bytes();
    s.write_all(&len).await.unwrap();
    s.write_all(&payload).await.unwrap();
}

async fn read_framed(s: &mut TcpStream) -> Option<Value> {
    let mut len_buf = [0u8; 4];
    if s.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let n = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; n];
    if s.read_exact(&mut payload).await.is_err() {
        return None;
    }
    serde_json::from_slice(&payload).ok()
}

#[tokio::test]
async fn sow_arrives_as_chunked_batch_frames() {
    let schema = json!({ "k": "string", "v": "double" });
    let topic = TopicSpec::new("/batched", "k").with_schema(schema);

    // Small explicit batch size so we can count multiple frames.
    let opts = ServerOpts {
        outbound_queue_capacity: 16384,
        slow_consumer: None,
        tls: None,
        queues: Vec::new(),
        auth: None,
        txlog_archive: None,
        views: Vec::new(),
        spillover: None,
        logging_sinks: Vec::new(),
        replication: None,
        hard_max_sow_result_rows: None,
        admin_token: None,
        admin_tls: None,
        transport_limits: None,
        audit: None,
        ..ServerOpts::default()
    };
    let server = start_server_with(vec![topic], opts).await;

    // Pre-seed via the SDK.
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let n = 437; // deliberately not a multiple of any standard batch size
    for i in 0..n {
        client
            .publish("/batched", json!({ "k": format!("k{i:04}"), "v": i as f64 }))
            .await
            .expect("publish");
    }
    drop(client);

    // Open a raw TCP connection and issue a one-shot SOW manually.
    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .expect("tcp connect");
    write_framed(
        &mut s,
        &json!({ "c": "sow", "cid": "raw-probe", "t": "/batched" }),
    )
    .await;

    let mut group_begins = 0usize;
    let mut sow_singles = 0usize;
    let mut sow_batches = 0usize;
    let mut batch_sizes: Vec<usize> = Vec::new();
    let mut rows_in_batches = 0usize;
    let mut got_group_end = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);

    while !got_group_end && std::time::Instant::now() < deadline {
        let frame = match tokio::time::timeout(Duration::from_millis(800), read_framed(&mut s))
            .await
        {
            Ok(Some(v)) => v,
            _ => break,
        };
        let c = frame.get("c").and_then(|v| v.as_str()).unwrap_or("");
        match c {
            "group_begin" => group_begins += 1,
            "sow" => sow_singles += 1,
            "sow_batch" => {
                sow_batches += 1;
                if let Some(arr) = frame.get("d").and_then(|v| v.as_array()) {
                    batch_sizes.push(arr.len());
                    rows_in_batches += arr.len();
                }
            }
            "group_end" => got_group_end = true,
            _ => {}
        }
    }

    assert!(got_group_end, "did not receive group_end before deadline");
    assert_eq!(group_begins, 1, "expected exactly one group_begin");
    assert_eq!(
        sow_singles, 0,
        "streaming path should never emit per-row sow frames"
    );
    assert!(
        sow_batches >= 2,
        "expected several sow_batch frames, got {sow_batches}"
    );
    assert_eq!(rows_in_batches, n, "total rows in batches != published count");

    // All but the last batch should be at the configured batch size
    // (default = 200). The last is the remainder.
    let default_batch_size = 200;
    let expected_full_batches = n / default_batch_size;
    let expected_remainder = n % default_batch_size;
    let full_batches = batch_sizes.iter().filter(|&&n| n == default_batch_size).count();
    assert_eq!(
        full_batches, expected_full_batches,
        "expected {expected_full_batches} full batches of {default_batch_size}, got sizes {batch_sizes:?}"
    );
    if expected_remainder > 0 {
        assert!(
            batch_sizes.iter().any(|&n| n == expected_remainder),
            "missing remainder batch of {expected_remainder}, sizes={batch_sizes:?}"
        );
    }
}

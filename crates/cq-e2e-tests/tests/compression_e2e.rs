//! S27 — wire-compression negotiation end-to-end.
//!
//! Spins a real cqserver child, exercises the Logon handshake to
//! negotiate zstd, then publishes a large/repetitive payload through
//! the SDK. Verifies the client SDK's `compression()` reports the
//! negotiated algorithm. Separately, a raw socket measures wire bytes
//! with and without compression to confirm the on-the-wire size
//! shrinks.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use cq_protocol::command::Command;
use cq_protocol::compression::{Compression, COMPRESSED_FLAG};
use cq_protocol::message::CqMessage;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn sdk_negotiates_zstd_on_handshake() {
    let topic = TopicSpec::new("/comp-sdk", "k").with_inline_columns([
        ("k", "string"),
        ("body", "string"),
    ]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Pre-handshake: legacy default.
    assert_eq!(client.compression(), Compression::None);

    // Handshake — server should pick zstd (top of its support list).
    let _v = client.handshake_protocol().await.expect("handshake");
    assert_eq!(client.compression(), Compression::Zstd);

    // The handshake's effect on subsequent traffic: a publish with a
    // big repeating body should still round-trip correctly.
    let big_body: String = "x".repeat(2048);
    let seq = client
        .publish("/comp-sdk", json!({ "k": "k1", "body": big_body }))
        .await
        .expect("publish");
    assert!(seq > 0);

    // Pull the row back to verify roundtrip semantics survived
    // compression on at least one side of the wire.
    let rows = client
        .sow("/comp-sdk", None)
        .await
        .expect("sow");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("k").and_then(Value::as_str),
        Some("k1")
    );
    assert_eq!(
        rows[0].get("body").and_then(Value::as_str).unwrap_or(""),
        big_body
    );
}

/// Open a raw TCP socket, send a Logon advertising the supplied
/// compressions, then send `frames_to_send` framed messages. Read the
/// ack + any responses, returning the total bytes read from the wire
/// before EOF. Used to compare compressed vs uncompressed wire
/// volumes for the same logical payload.
async fn measure_wire_bytes(
    port: u16,
    compressions: Vec<Compression>,
    publish_payload: &str,
    publish_count: usize,
) -> u64 {
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("connect");

    // 1. Logon with the requested compression set.
    let mut logon = CqMessage::new(Command::Logon);
    logon.command_id = Some("comp-cid".into());
    logon.compressions = Some(compressions.clone());
    logon.protocol_versions = Some(cq_protocol::version::SUPPORTED_VERSIONS.to_vec());
    let body = serde_json::to_vec(&logon).unwrap();
    sock.write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    sock.write_all(&body).await.unwrap();
    // Read ack (might be compressed if server negotiated zstd).
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let raw_len = u32::from_be_bytes(len_buf);
    let length = (raw_len & !COMPRESSED_FLAG) as usize;
    let mut ack = vec![0u8; length];
    sock.read_exact(&mut ack).await.unwrap();
    let mut bytes_read = (4 + ack.len()) as u64;

    // 2. Subscribe so the server emits deltas back to us.
    let mut sub = CqMessage::new(Command::SowAndSubscribe);
    sub.command_id = Some("sub-cid".into());
    sub.topic = Some("/comp-wire".into());
    let body = serde_json::to_vec(&sub).unwrap();
    sock.write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    sock.write_all(&body).await.unwrap();
    // We don't need to read the subscribe ack synchronously — the
    // measurement loop below will absorb every server-emitted byte.

    // 3. Spawn a publisher SDK on the side that drives traffic to
    //    the topic. The SDK uses its own connection — what matters
    //    for the measurement is the OUTBOUND server→raw-socket
    //    traffic.
    let server_url = format!("tcp://127.0.0.1:{port}");
    let publisher = Client::connect(&server_url).await.expect("publisher");
    for i in 0..publish_count {
        let _ = publisher
            .publish(
                "/comp-wire",
                json!({ "k": format!("k{i}"), "body": publish_payload }),
            )
            .await;
    }

    // 4. Drain bytes from the raw socket for up to a small window —
    //    enough for every delta to arrive but not so long the
    //    measurement varies wildly.
    let deadline = std::time::Instant::now() + Duration::from_millis(1500);
    while std::time::Instant::now() < deadline {
        let mut buf = [0u8; 16 * 1024];
        match tokio::time::timeout(Duration::from_millis(200), sock.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => bytes_read += n as u64,
            _ => break,
        }
    }
    bytes_read
}

#[tokio::test]
async fn zstd_shrinks_wire_volume_vs_none() {
    let topic = TopicSpec::new("/comp-wire", "k").with_inline_columns([
        ("k", "string"),
        ("body", "string"),
    ]);
    let server = start_server(vec![topic]).await;

    // A repeating payload — zstd should compress this hard.
    let payload = "lorem ipsum dolor sit amet, ".repeat(64);
    let count = 50;

    let uncompressed_bytes =
        measure_wire_bytes(server.tcp_port, vec![Compression::None], &payload, count).await;
    let compressed_bytes =
        measure_wire_bytes(server.tcp_port, vec![Compression::Zstd], &payload, count).await;

    // We expect a substantial win on the wire — order of magnitude
    // for repeating text. Assert that compressed bytes < 80% of
    // uncompressed; in practice it's far less, but 80% leaves
    // headroom for the small uncompressed frames (logon ack, etc.)
    // that don't benefit.
    assert!(
        compressed_bytes < uncompressed_bytes,
        "expected compressed wire < uncompressed; got {compressed_bytes} vs {uncompressed_bytes}"
    );
    let ratio = compressed_bytes as f64 / uncompressed_bytes as f64;
    assert!(
        ratio < 0.8,
        "expected ratio < 0.8 (got {:.3}, compressed={}, uncompressed={})",
        ratio,
        compressed_bytes,
        uncompressed_bytes
    );
}

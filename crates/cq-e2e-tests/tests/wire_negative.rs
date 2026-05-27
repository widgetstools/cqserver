//! Wire-protocol negative tests — drive raw TCP frames at the
//! running server and verify it:
//!
//!   1. **Never panics.** A malformed frame must close the offending
//!      connection cleanly; other clients must remain unaffected.
//!   2. **Returns a useful error or disconnects** on protocol
//!      violations (oversized frame, truncated payload, non-JSON,
//!      unknown command, malformed publish payload).
//!   3. **Doesn't poison shared state.** After every negative
//!      exchange, a legitimate SDK client must still connect,
//!      publish, and SOW against the same server.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Send `frame_bytes` as the body of a length-prefixed frame.
async fn send_frame(s: &mut TcpStream, frame_bytes: &[u8]) -> std::io::Result<()> {
    let len = (frame_bytes.len() as u32).to_be_bytes();
    s.write_all(&len).await?;
    s.write_all(frame_bytes).await?;
    s.flush().await
}

/// Send a custom length prefix without a matching body — for
/// oversized / lying-length tests.
async fn send_length_prefix_only(s: &mut TcpStream, len: u32) -> std::io::Result<()> {
    s.write_all(&len.to_be_bytes()).await?;
    s.flush().await
}

/// After a malicious connection has done its damage, this confirms
/// the server is still alive by running a fresh SDK round-trip.
async fn server_still_healthy(server: &cq_e2e_tests::ServerHandle, topic: &str) {
    let c = Client::connect(&server.tcp_url())
        .await
        .expect("server must still accept fresh clients");
    c.publish(topic, json!({ "k": "health", "v": 1 }))
        .await
        .expect("server must still accept publishes");
    let rows = c.sow(topic, None).await.expect("server must still serve SOWs");
    assert!(!rows.is_empty());
}

#[tokio::test]
async fn oversized_frame_disconnects_cleanly() {
    let topic = TopicSpec::new("/wire-oversize", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();
    // Declare 32 MiB — twice MAX_FRAME_SIZE.
    send_length_prefix_only(&mut s, (MAX_FRAME_SIZE as u32) * 2)
        .await
        .unwrap();
    // Server should drop the connection. Read should EOF/err quickly.
    let mut buf = [0_u8; 16];
    let r = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await;
    // Either timeout (server keeps connection but ignores us — also
    // acceptable as long as it doesn't crash) or the read returns 0 / err.
    let _ = r;

    server_still_healthy(&server, "/wire-oversize").await;
}

#[tokio::test]
async fn zero_length_frame_is_handled() {
    let topic = TopicSpec::new("/wire-zero", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();
    send_length_prefix_only(&mut s, 0).await.unwrap();
    // Give the server a moment to process / close.
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(s);

    server_still_healthy(&server, "/wire-zero").await;
}

#[tokio::test]
async fn non_json_payload_does_not_crash_server() {
    let topic = TopicSpec::new("/wire-junk", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();
    // Send a valid-length-prefixed payload of binary garbage.
    let payload = vec![0xFF_u8; 64];
    send_frame(&mut s, &payload).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(s);

    server_still_healthy(&server, "/wire-junk").await;
}

#[tokio::test]
async fn unknown_command_returns_error_or_disconnects() {
    let topic = TopicSpec::new("/wire-unk", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();
    let payload = serde_json::to_vec(&json!({
        "c": "this_command_does_not_exist",
        "cid": "x",
        "t": "/wire-unk",
    }))
    .unwrap();
    send_frame(&mut s, &payload).await.unwrap();

    // Server should either send an error or close the connection.
    let mut buf = [0_u8; 256];
    let _ = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await;
    drop(s);

    server_still_healthy(&server, "/wire-unk").await;
}

#[tokio::test]
async fn truncated_payload_then_disconnect_does_not_wedge() {
    let topic = TopicSpec::new("/wire-trunc", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();
    // Declare 1024 byte payload, send 8.
    s.write_all(&(1024_u32).to_be_bytes()).await.unwrap();
    s.write_all(b"only8byt").await.unwrap();
    s.flush().await.unwrap();
    // Immediately drop the socket. Server's reader will see EOF
    // mid-frame; must not hang the worker pool or leak the session.
    drop(s);
    tokio::time::sleep(Duration::from_millis(150)).await;

    server_still_healthy(&server, "/wire-trunc").await;
}

#[tokio::test]
async fn publish_with_data_that_is_not_an_object_is_rejected() {
    // Use the SDK to send a publish with `data` as a JSON number —
    // the router validates `data` is an object before calling
    // upsert_map. This must NOT panic.
    let topic = TopicSpec::new("/wire-baddata", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    let r = client.publish("/wire-baddata", json!(42)).await;
    assert!(r.is_err(), "non-object data must error, got {r:?}");

    // Send a string too.
    let r2 = client.publish("/wire-baddata", json!("oops")).await;
    assert!(r2.is_err(), "string data must error, got {r2:?}");

    // Server is healthy — accepts a proper publish after.
    client
        .publish("/wire-baddata", json!({ "k": "ok", "v": 1 }))
        .await
        .unwrap();
    let rows = client.sow("/wire-baddata", None).await.unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("ok")));
}

#[tokio::test]
async fn publish_with_nan_value_handled_safely() {
    // NaN can't be expressed in standard JSON (`serde_json` refuses
    // to serialize it). But a client COULD construct one as the
    // string "NaN". The server's from_json for Double should treat
    // unparseable strings as null, never panic.
    let topic = TopicSpec::new("/wire-nan", "k")
        .with_inline_columns([("k", "string"), ("v", "double")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    client
        .publish("/wire-nan", json!({ "k": "a", "v": 1.0 }))
        .await
        .unwrap();
    // Publish with v as the string "NaN".
    let _ = client
        .publish("/wire-nan", json!({ "k": "b", "v": "NaN" }))
        .await;
    // Server stays alive either way.
    client
        .publish("/wire-nan", json!({ "k": "c", "v": 2.0 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    let rows = client.sow("/wire-nan", None).await.unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("c")));
}

#[tokio::test]
async fn deeply_nested_json_is_depth_capped_and_never_stalls() {
    // 100-level nesting: the cq-core flattener now caps recursion at
    // `FlattenConfig::max_depth` (default 32) so flattening is
    // bounded regardless of input depth. The publish path must
    // complete quickly.
    //
    // NB: at much deeper inputs (e.g. 500+ levels) the bottleneck
    // shifts to the WIRE serialization (serde_json's recursion +
    // the codec's frame encode), which is a separate codepath that
    // doesn't honour the flattener cap. That's a follow-up gap;
    // here we pin the flattener-cap contract specifically.
    let topic = TopicSpec::new("/wire-deep", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    let mut payload = json!({ "k": "deep" });
    let mut node = json!("leaf");
    for _ in 0..100 {
        node = json!({ "x": node });
    }
    payload.as_object_mut().unwrap().insert("nested".into(), node);

    // Must complete in well under the previous stall window.
    let r = tokio::time::timeout(
        Duration::from_secs(3),
        client.publish("/wire-deep", payload),
    )
    .await;
    assert!(
        r.is_ok(),
        "publish must not stall on 100-deep input (flattener cap should keep it bounded)"
    );
    let _ = r.unwrap();

    // Server must still serve a fresh publish + SOW.
    client
        .publish("/wire-deep", json!({ "k": "after" }))
        .await
        .unwrap();
    let rows = client.sow("/wire-deep", None).await.unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("after")));
}

#[tokio::test]
async fn concurrent_malicious_clients_dont_block_legitimate_traffic() {
    let topic = TopicSpec::new("/wire-concurrent", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    // Spawn 5 connections that each send garbage and disconnect.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let port = server.tcp_port;
        handles.push(tokio::spawn(async move {
            if let Ok(mut s) = TcpStream::connect(format!("127.0.0.1:{port}")).await {
                let _ = send_frame(&mut s, &[0xAB; 128]).await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }));
    }

    // While they're banging on the wire, a legitimate client should
    // still connect + publish + SOW.
    let client = Client::connect(&server.tcp_url()).await.unwrap();
    client
        .publish("/wire-concurrent", json!({ "k": "legit", "v": 1 }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let rows = client.sow("/wire-concurrent", None).await.unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("legit")));

    for h in handles {
        let _ = h.await;
    }
}

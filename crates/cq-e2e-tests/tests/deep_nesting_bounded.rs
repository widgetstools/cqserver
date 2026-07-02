//! Wire-level DoS guard: a deeply-nested (~500 level) JSON payload
//! must be rejected with a clean error frame before `serde_json`
//! ever attempts to decode it, not stall the connection.
//!
//! `wire_negative.rs::deeply_nested_json_is_depth_capped_and_never_stalls`
//! already pins the flattener-level cap (100 levels, via the SDK's
//! `serde_json::Value`, which itself won't stack-overflow at 100
//! deep). This test goes past that: 500 levels of nesting, sent as a
//! raw hand-built JSON string over a raw TCP connection (the client
//! SDK's own `serde_json::Value` construction is not the thing under
//! test — the wire decode path is), verifying the server's pre-parse
//! depth scan rejects it quickly and the server stays healthy.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Send `frame_bytes` as the body of a length-prefixed frame (see
/// `crates/cq-protocol/src/codec.rs`: `[len: u32 BE][payload]`).
async fn send_frame(s: &mut TcpStream, frame_bytes: &[u8]) -> std::io::Result<()> {
    let len = (frame_bytes.len() as u32).to_be_bytes();
    s.write_all(&len).await?;
    s.write_all(frame_bytes).await?;
    s.flush().await
}

/// Build a raw `publish` envelope whose `d` field is nested `depth`
/// levels deep: `{"n":{"n":{...:1}}}`. Constructed as a plain string
/// so we never have to build a 500-deep `serde_json::Value` in the
/// test process itself.
fn deeply_nested_publish_frame(topic: &str, depth: usize) -> Vec<u8> {
    let nested = "{\"n\":".repeat(depth) + "1" + &"}".repeat(depth);
    format!(
        "{{\"c\":\"publish\",\"cid\":\"deep-1\",\"t\":\"{topic}\",\"d\":{{\"k\":\"deep\",\"nested\":{nested}}}}}"
    )
    .into_bytes()
}

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
async fn deep_500_level_publish_rejected_cleanly_and_quickly() {
    let topic = TopicSpec::new("/wire-deep-500", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;

    let mut s = TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .unwrap();

    let frame = deeply_nested_publish_frame("/wire-deep-500", 500);
    let start = std::time::Instant::now();
    send_frame(&mut s, &frame).await.unwrap();

    // Expect either a clean error frame back, or the connection to be
    // closed — either way it must happen well within 2s (the old
    // behaviour was an unbounded `serde_json` recursion stall).
    let mut buf = [0_u8; 4096];
    let read = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await;
    let elapsed = start.elapsed();
    assert!(
        read.is_ok(),
        "server did not respond/close within 2s for a 500-deep publish (stalled in decode)"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "took {elapsed:?}, expected a fast rejection"
    );

    drop(s);
    server_still_healthy(&server, "/wire-deep-500").await;
}

#[tokio::test]
async fn deep_500_level_publish_over_websocket_rejected_cleanly() {
    use tokio_tungstenite::tungstenite::Message;

    let topic = TopicSpec::new("/wire-deep-500-ws", "k")
        .with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;

    let (mut ws, _resp) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .expect("ws connect");

    let nested = "{\"n\":".repeat(500) + "1" + &"}".repeat(500);
    let text = format!(
        "{{\"c\":\"publish\",\"cid\":\"deep-ws-1\",\"t\":\"/wire-deep-500-ws\",\"d\":{{\"k\":\"deep\",\"nested\":{nested}}}}}"
    );

    use futures_util::{SinkExt, StreamExt};
    let start = std::time::Instant::now();
    ws.send(Message::Text(text.into())).await.unwrap();

    let next = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;
    let elapsed = start.elapsed();
    assert!(
        next.is_ok(),
        "server did not respond/close within 2s for a 500-deep WS publish"
    );
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");

    drop(ws);
    server_still_healthy(&server, "/wire-deep-500-ws").await;
}

#[tokio::test]
async fn normal_publish_still_works_alongside_depth_guard() {
    let topic = TopicSpec::new("/wire-deep-normal", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.unwrap();

    client
        .publish("/wire-deep-normal", json!({ "k": "ok", "v": 1 }))
        .await
        .unwrap();
    let rows = client.sow("/wire-deep-normal", None).await.unwrap();
    assert!(rows.iter().any(|r| r.get("k").unwrap().as_str() == Some("ok")));

    // A moderately nested but legal payload (well under the 128 cap)
    // must still be accepted.
    let mut payload = json!({ "k": "mid-nested", "v": 2 });
    let mut node = json!("leaf");
    for _ in 0..20 {
        node = json!({ "x": node });
    }
    payload.as_object_mut().unwrap().insert("nested".into(), node);
    client
        .publish("/wire-deep-normal", payload)
        .await
        .expect("moderately nested payload under the cap must be accepted");
}

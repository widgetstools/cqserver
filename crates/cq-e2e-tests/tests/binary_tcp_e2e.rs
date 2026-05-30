//! #30 — binary-on-TCP via Logon-time codec negotiation, end-to-end.
//!
//! A pre-S30 TCP connection always spoke JSON. S30 lets the client
//! advertise a codec preference list on `Logon`; the server picks the
//! first it supports, echoes it on the ack, and both sides switch to
//! that codec for every post-ack frame. The Logon frame itself is
//! always JSON (the codec the server decodes Logons with), so the
//! handshake is race-free: the ack goes out in the pre-switch codec and
//! the switch only affects later frames.
//!
//! These tests spin a real cqserver child and verify (1) an SDK
//! configured for MessagePack negotiates it and round-trips publishes +
//! live deltas over the binary wire, (2) a default SDK stays on JSON,
//! and (3) a raw socket sees the ack advertise the negotiated codec and
//! the server then accepts a MessagePack-encoded frame.

use cq_client::{Client, ClientConfig, Codec, DeltaKind};
use cq_e2e_tests::{start_server, TopicSpec};
use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use cq_protocol::serialization::Codec as WireCodec;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn sdk_negotiates_msgpack_and_round_trips_binary() {
    let topic = TopicSpec::new("/bin-sdk", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let client = Client::connect_with(
        &server.tcp_url(),
        ClientConfig {
            codec: Codec::MessagePack,
            ..Default::default()
        },
    )
    .await
    .expect("connect");

    // Pre-handshake the Logon frame is JSON, so the active codec is the
    // legacy default until negotiation completes.
    assert_eq!(client.codec(), Codec::Json);

    // Anonymous handshake (server has auth disabled by default) drives
    // the codec negotiation. Server supports MessagePack, so it wins.
    let _v = client.handshake_protocol().await.expect("handshake");
    assert_eq!(client.codec(), Codec::MessagePack);

    // client → server over MessagePack: a publish must decode server-side.
    let seq = client
        .publish("/bin-sdk", json!({ "k": "a", "v": 1 }))
        .await
        .expect("publish");
    assert!(seq > 0);

    // server → client over MessagePack: subscribe, drain the snapshot,
    // then a live publish must arrive as a binary-encoded delta.
    let mut sub = client
        .sow_and_subscribe("/bin-sdk", None, None)
        .await
        .expect("subscribe");

    let mut snap = 0;
    loop {
        match tokio::time::timeout(Duration::from_millis(500), sub.next_delta()).await {
            Ok(Some(d)) if d.delta_type == DeltaKind::SowSnapshot => snap += 1,
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    assert_eq!(snap, 1, "expected the one existing row in the snapshot");

    client
        .publish("/bin-sdk", json!({ "k": "b", "v": 2 }))
        .await
        .expect("publish live");

    let mut got_live = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !got_live && std::time::Instant::now() < deadline {
        if let Ok(Some(d)) =
            tokio::time::timeout(Duration::from_millis(200), sub.next_delta()).await
        {
            if matches!(d.delta_type, DeltaKind::Add | DeltaKind::Update) {
                got_live = true;
            }
        }
    }
    assert!(got_live, "live delta never arrived over the MessagePack wire");

    // Final consistency check via a one-shot SOW (also MessagePack now).
    let rows = client.sow("/bin-sdk", None).await.expect("sow");
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn default_sdk_stays_json() {
    let topic = TopicSpec::new("/bin-json", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    // Default config advertises only JSON, so negotiation is a no-op.
    let client = Client::connect(&server.tcp_url()).await.expect("connect");
    let _v = client.handshake_protocol().await.expect("handshake");
    assert_eq!(client.codec(), Codec::Json);

    let seq = client
        .publish("/bin-json", json!({ "k": "a", "v": 1 }))
        .await
        .expect("publish");
    assert!(seq > 0);
}

/// Raw socket: send a JSON Logon advertising `[msgpack, json]`, confirm
/// the ack echoes `msgpack`, then send a MessagePack-encoded Publish and
/// confirm (via a side SDK) the row landed — proving the server switched
/// its decode codec after the ack.
#[tokio::test]
async fn raw_msgpack_frame_accepted_after_handshake() {
    let topic = TopicSpec::new("/bin-raw", "k")
        .with_inline_columns([("k", "string"), ("v", "long")]);
    let server = start_server(vec![topic]).await;

    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .expect("connect");

    // 1. JSON Logon advertising a MessagePack-first preference list.
    let mut logon = CqMessage::new(Command::Logon);
    logon.command_id = Some("raw-logon".into());
    logon.codecs = Some(vec![WireCodec::MessagePack, WireCodec::Json]);
    logon.protocol_versions = Some(cq_protocol::version::SUPPORTED_VERSIONS.to_vec());
    let body = serde_json::to_vec(&logon).unwrap();
    sock.write_all(&(body.len() as u32).to_be_bytes()).await.unwrap();
    sock.write_all(&body).await.unwrap();

    // 2. Read the ack (always JSON, the pre-switch codec) and assert it
    //    advertises the negotiated MessagePack codec.
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let length = u32::from_be_bytes(len_buf) as usize;
    let mut ack_bytes = vec![0u8; length];
    sock.read_exact(&mut ack_bytes).await.unwrap();
    let ack: CqMessage = serde_json::from_slice(&ack_bytes).expect("ack is JSON");
    assert_eq!(
        ack.codecs.as_deref(),
        Some([WireCodec::MessagePack].as_slice()),
        "ack must echo the negotiated codec"
    );

    // 3. Send a MessagePack-encoded Publish. If the server didn't switch
    //    its decode codec it would reject this as invalid JSON.
    let mut pubmsg = CqMessage::new(Command::Publish);
    pubmsg.command_id = Some("raw-pub".into());
    pubmsg.topic = Some("/bin-raw".into());
    pubmsg.data = Some(json!({ "k": "z", "v": 42 }));
    let mp = WireCodec::MessagePack.encode(&pubmsg).expect("encode msgpack");
    sock.write_all(&(mp.len() as u32).to_be_bytes()).await.unwrap();
    sock.write_all(&mp).await.unwrap();

    // 4. The publish ack comes back MessagePack-encoded now.
    sock.read_exact(&mut len_buf).await.unwrap();
    let length = u32::from_be_bytes(len_buf) as usize;
    let mut pub_ack = vec![0u8; length];
    sock.read_exact(&mut pub_ack).await.unwrap();
    let decoded = WireCodec::MessagePack
        .decode(&pub_ack)
        .expect("publish ack must be MessagePack");
    assert_eq!(decoded.command, Command::Ack);

    // 5. Independently confirm the row landed via a normal SDK SOW.
    let client = Client::connect(&server.tcp_url()).await.expect("sdk connect");
    let rows = client.sow("/bin-raw", None).await.expect("sow");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("k").and_then(Value::as_str), Some("z"));
    assert_eq!(rows[0].get("v").and_then(Value::as_i64), Some(42));
}

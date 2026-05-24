//! S28 — wire-protocol version negotiation end-to-end.
//!
//! Verifies the handshake-on-Logon path against a real cqserver child:
//!   - Without an explicit `protocol_versions` field on the wire, the
//!     server falls back to the legacy version (compat with pre-S28
//!     clients) and the client's view reflects that.
//!   - When the client advertises a list that intersects the server's
//!     support set, the server picks the highest mutually supported
//!     version and echoes it on the ack — the client stores it.
//!   - When the client's advertised set is disjoint from the server's,
//!     Logon errors out cleanly.

use cq_client::Client;
use cq_e2e_tests::{start_server, TopicSpec};
use cq_protocol::command::Command;
use cq_protocol::message::CqMessage;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn handshake_negotiates_protocol_version_via_client_sdk() {
    let topic = TopicSpec::new("/proto-v", "k").with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let client = Client::connect(&server.tcp_url()).await.expect("connect");

    // Pre-handshake: client reports the legacy default.
    assert_eq!(
        client.protocol_version(),
        cq_protocol::version::DEFAULT_LEGACY_VERSION
    );

    // Run the no-credentials handshake. With auth.required=false (the
    // e2e harness default) the server accepts the empty logon and
    // echoes the negotiated version.
    let negotiated = client
        .handshake_protocol()
        .await
        .expect("handshake");
    assert_eq!(negotiated, cq_protocol::version::MAX_PROTOCOL_VERSION);
    assert_eq!(client.protocol_version(), negotiated);
}

/// Send a raw Logon frame over TCP with caller-supplied `protocol_versions`,
/// read the next frame, return the parsed ack. Bypasses the SDK so we
/// can construct mismatched-versions scenarios that the SDK can't.
async fn raw_logon(
    server: &cq_e2e_tests::ServerHandle,
    protocol_versions: Option<Vec<u32>>,
) -> CqMessage {
    let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", server.tcp_port))
        .await
        .expect("tcp connect");
    let mut logon = CqMessage::new(Command::Logon);
    logon.command_id = Some("v-cid".into());
    logon.protocol_versions = protocol_versions;
    let payload = serde_json::to_vec(&logon).expect("encode logon");
    let len = (payload.len() as u32).to_be_bytes();
    sock.write_all(&len).await.unwrap();
    sock.write_all(&payload).await.unwrap();

    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.expect("read len");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    sock.read_exact(&mut buf).await.expect("read body");
    serde_json::from_slice::<CqMessage>(&buf).expect("decode ack")
}

#[tokio::test]
async fn client_v2_server_v3_picks_v2() {
    // Server here supports [1, 2] (this build's SUPPORTED_VERSIONS).
    // Client claims [2, 7] — pretending to be a vintage that knows
    // V2 plus some future version. Intersection = {2}; pick 2.
    let topic = TopicSpec::new("/v2v3", "k").with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let ack = raw_logon(&server, Some(vec![2, 7])).await;
    assert!(
        ack.status.is_some(),
        "expected an ack with status set, got: {ack:?}"
    );
    let versions = ack
        .protocol_versions
        .as_ref()
        .expect("ack must echo negotiated version");
    assert_eq!(versions, &vec![2]);
}

#[tokio::test]
async fn missing_protocol_versions_falls_back_to_legacy() {
    let topic = TopicSpec::new("/proto-legacy", "k").with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    let ack = raw_logon(&server, None).await;
    let versions = ack
        .protocol_versions
        .as_ref()
        .expect("ack must include negotiated version");
    assert_eq!(versions, &vec![cq_protocol::version::DEFAULT_LEGACY_VERSION]);
}

#[tokio::test]
async fn disjoint_version_set_errors_cleanly() {
    let topic = TopicSpec::new("/proto-x", "k").with_inline_columns([("k", "string")]);
    let server = start_server(vec![topic]).await;
    // Client only speaks v99 — server has nothing in common.
    let ack = raw_logon(&server, Some(vec![99])).await;
    let status = ack
        .status
        .clone()
        .expect("ack should carry a status");
    // The error path emits Status::Error + a reason explaining
    // there was no overlap.
    assert!(
        format!("{:?}", status).to_ascii_lowercase().contains("error"),
        "expected Error status, got {:?}",
        status
    );
    let reason = ack.reason.clone().unwrap_or_default();
    assert!(
        reason.to_ascii_lowercase().contains("protocol"),
        "expected reason to mention protocol; got {reason:?}"
    );
    // ack.data is None on errors.
    assert!(ack.data.is_none(), "data: {:?}", ack.data);
    let _ = Value::Null;
}

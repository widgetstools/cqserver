//! Wire codec for `CqMessage`. Supports JSON (default, human-readable)
//! and MessagePack (compact, language-portable binary).
//!
//! The codec is chosen per-connection at the transport layer:
//! - WebSocket: text frames imply JSON; binary frames imply MessagePack.
//! - TCP: JSON by default; a client may advertise a `codecs` preference
//!   list on `Logon` (S30) and the server echoes the negotiated codec on
//!   the ack. Both sides then switch to it for every post-ack frame —
//!   the Logon itself is always JSON, so the handshake is race-free.
//!   See [`negotiate_codec`] / [`SUPPORTED_CODECS`].
//!
//! Both codecs go through `serde::{Serialize, Deserialize}` on
//! `CqMessage`, so the field renames (`c`, `cid`, `sid`, etc.) are
//! preserved across both wire shapes.

use crate::message::CqMessage;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, Hash,
)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    #[default]
    Json,
    #[serde(rename = "msgpack")]
    MessagePack,
    Bson,
    /// S23 — FIX 4.x SOH-delimited tag=value. The envelope codec
    /// maps `CqMessage` fields onto a fixed set of FIX tags (see
    /// `crate::fix` for details); nested `data` is stringified as
    /// JSON under tag 5002. Out of scope: FIX 5.x repeating groups
    /// and DataDictionary-driven field typing.
    Fix,
}

impl Codec {
    /// Stable wire token. Used by the `codecs` negotiation field on
    /// `CqMessage` so old + new code can round-trip the value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Codec::Json => "json",
            Codec::MessagePack => "msgpack",
            Codec::Bson => "bson",
            Codec::Fix => "fix",
        }
    }

    pub fn from_wire(s: &str) -> Option<Self> {
        match s {
            "json" => Some(Codec::Json),
            "msgpack" => Some(Codec::MessagePack),
            "bson" => Some(Codec::Bson),
            "fix" => Some(Codec::Fix),
            _ => None,
        }
    }
}

/// Codecs this build can speak, in negotiation preference order. When a
/// client offers `["msgpack", "json"]` the server picks `msgpack` if
/// it's in this list; otherwise it falls through to the next mutual
/// choice (ultimately `json`, the legacy default).
pub const SUPPORTED_CODECS: &[Codec] =
    &[Codec::MessagePack, Codec::Bson, Codec::Fix, Codec::Json];

/// Default codec assumed when a Logon arrives without an explicit
/// `codecs` field — preserves compatibility with pre-S30 clients that
/// only ever speak JSON on the wire.
pub const DEFAULT_LEGACY_CODEC: Codec = Codec::Json;

/// Result of a Logon-time codec negotiation. Mirrors the compression
/// module's `NegotiationOutcome` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecNegotiationOutcome {
    /// Both sides share at least one codec; the client's most-preferred
    /// mutual codec is selected.
    Negotiated(Codec),
    /// Disjoint sets. Can only happen if both sides explicitly rule out
    /// `json` — otherwise the legacy default applies. Treated as an
    /// error so the client fails fast.
    NoOverlap,
}

/// Negotiate the active codec given the client's advertised list (in
/// preference order) and the server's supported list. Returns the first
/// client choice the server can speak.
///
/// An empty `client_codecs` is treated as legacy (`Codec::Json`).
pub fn negotiate_codec(
    client_codecs: &[Codec],
    server_codecs: &[Codec],
) -> CodecNegotiationOutcome {
    if client_codecs.is_empty() {
        if server_codecs.contains(&DEFAULT_LEGACY_CODEC) {
            return CodecNegotiationOutcome::Negotiated(DEFAULT_LEGACY_CODEC);
        }
        return CodecNegotiationOutcome::NoOverlap;
    }
    for &c in client_codecs {
        if server_codecs.contains(&c) {
            return CodecNegotiationOutcome::Negotiated(c);
        }
    }
    CodecNegotiationOutcome::NoOverlap
}

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("msgpack encode: {0}")]
    MsgpackEncode(#[from] rmp_serde::encode::Error),
    #[error("msgpack decode: {0}")]
    MsgpackDecode(#[from] rmp_serde::decode::Error),
    #[error("bson encode: {0}")]
    BsonEncode(String),
    #[error("bson decode: {0}")]
    BsonDecode(String),
    #[error("fix encode: {0}")]
    FixEncode(String),
    #[error("fix decode: {0}")]
    FixDecode(String),
    /// Wire-layer DoS guard (A8/E12): the raw payload's `{`/`[`
    /// nesting depth exceeds `crate::depth_guard::MAX_WIRE_JSON_DEPTH`
    /// before we even attempt to parse it. See `crate::depth_guard`.
    #[error("nesting depth exceeds {0}")]
    NestingTooDeep(usize),
}

impl Codec {
    pub fn encode(self, msg: &CqMessage) -> Result<Vec<u8>, CodecError> {
        match self {
            Codec::Json => Ok(serde_json::to_vec(msg)?),
            Codec::MessagePack => Ok(rmp_serde::to_vec_named(msg)?),
            Codec::Bson => {
                // BSON requires a document (i.e., an object). `CqMessage`
                // serializes to one via its serde-derived shape; the field
                // renames (`c`, `cid`, ...) carry over verbatim.
                let doc = bson::to_document(msg)
                    .map_err(|e| CodecError::BsonEncode(e.to_string()))?;
                let mut buf = Vec::new();
                doc.to_writer(&mut buf)
                    .map_err(|e| CodecError::BsonEncode(e.to_string()))?;
                Ok(buf)
            }
            Codec::Fix => encode_fix_envelope(msg)
                .map_err(|e| CodecError::FixEncode(e.to_string())),
        }
    }

    pub fn decode(self, bytes: &[u8]) -> Result<CqMessage, CodecError> {
        match self {
            Codec::Json => {
                // A8/E12 — bound JSON nesting BEFORE handing bytes to
                // serde_json. This is a cheap linear byte scan (no
                // parsing, no allocation) so a deeply-nested payload
                // never reaches decoder recursion. See
                // `crate::depth_guard`.
                if crate::depth_guard::json_depth_exceeds_default(bytes) {
                    return Err(CodecError::NestingTooDeep(
                        crate::depth_guard::MAX_WIRE_JSON_DEPTH,
                    ));
                }
                Ok(serde_json::from_slice(bytes)?)
            }
            Codec::MessagePack => Ok(rmp_serde::from_slice(bytes)?),
            Codec::Bson => {
                let doc = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
                    .map_err(|e| CodecError::BsonDecode(e.to_string()))?;
                bson::from_document(doc)
                    .map_err(|e| CodecError::BsonDecode(e.to_string()))
            }
            Codec::Fix => decode_fix_envelope(bytes)
                .map_err(|e| CodecError::FixDecode(e.to_string())),
        }
    }
}

/// Map `CqMessage` to a FIX tag-keyed map then encode via the FIX
/// codec. Tag assignments:
///
/// | Tag  | Field              |
/// |------|--------------------|
/// | 35   | command (string)   |
/// | 11   | command_id         |
/// | 55   | topic              |
/// | 5000 | sub_id             |
/// | 200  | filter             |
/// | 34   | sequence           |
/// | 58   | reason             |
/// | 5001 | status             |
/// | 5002 | data (JSON string) |
fn encode_fix_envelope(msg: &CqMessage) -> Result<Vec<u8>, String> {
    use serde_json::{Map, Value};
    let mut map: Map<String, Value> = Map::new();
    map.insert(
        "35".into(),
        Value::String(
            serde_json::to_value(msg.command)
                .map_err(|e| e.to_string())?
                .as_str()
                .unwrap_or("")
                .to_string(),
        ),
    );
    if let Some(v) = &msg.command_id {
        map.insert("11".into(), Value::String(v.clone()));
    }
    if let Some(v) = &msg.topic {
        map.insert("55".into(), Value::String(v.clone()));
    }
    if let Some(v) = &msg.sub_id {
        map.insert("5000".into(), Value::String(v.clone()));
    }
    if let Some(v) = &msg.filter {
        map.insert("200".into(), Value::String(v.clone()));
    }
    if let Some(v) = msg.sequence {
        map.insert("34".into(), Value::Number(v.into()));
    }
    if let Some(v) = &msg.reason {
        map.insert("58".into(), Value::String(v.clone()));
    }
    if let Some(v) = &msg.status {
        let s = serde_json::to_value(v)
            .map_err(|e| e.to_string())?
            .as_str()
            .unwrap_or("")
            .to_string();
        map.insert("5001".into(), Value::String(s));
    }
    if let Some(d) = &msg.data {
        let s = serde_json::to_string(d).map_err(|e| e.to_string())?;
        map.insert("5002".into(), Value::String(s));
    }
    crate::fix::encode(&map).map_err(|e| e.to_string())
}

fn decode_fix_envelope(bytes: &[u8]) -> Result<CqMessage, String> {
    use crate::command::{Command, Status};
    let map = crate::fix::decode(bytes).map_err(|e| e.to_string())?;
    let command_str = map
        .get("35")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing tag 35 (command)".to_string())?;
    let command_value = serde_json::Value::String(command_str.to_string());
    let command: Command =
        serde_json::from_value(command_value).map_err(|e| e.to_string())?;
    let mut out = CqMessage::new(command);
    out.command_id = map.get("11").and_then(|v| v.as_str()).map(String::from);
    out.topic = map.get("55").and_then(|v| v.as_str()).map(String::from);
    out.sub_id = map.get("5000").and_then(|v| v.as_str()).map(String::from);
    out.filter = map.get("200").and_then(|v| v.as_str()).map(String::from);
    out.sequence = map
        .get("34")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok());
    out.reason = map.get("58").and_then(|v| v.as_str()).map(String::from);
    if let Some(s) = map.get("5001").and_then(|v| v.as_str()) {
        let v = serde_json::Value::String(s.to_string());
        out.status = serde_json::from_value::<Status>(v).ok();
    }
    if let Some(s) = map.get("5002").and_then(|v| v.as_str()) {
        // A8/E12 — same wire-nesting guard as the JSON codec; tag
        // 5002 carries a JSON-stringified `data` payload, so it's
        // subject to the same decode-recursion risk.
        if !crate::depth_guard::json_depth_exceeds_default(s.as_bytes()) {
            out.data = serde_json::from_str(s).ok();
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;

    fn sample() -> CqMessage {
        let mut m = CqMessage::new(Command::SowAndSubscribe);
        m.command_id = Some("c-1".into());
        m.topic = Some("/market-data".into());
        m.filter = Some("price > 100".into());
        m.sequence = Some(42);
        m
    }

    #[test]
    fn json_roundtrip() {
        let m = sample();
        let bytes = Codec::Json.encode(&m).unwrap();
        let back = Codec::Json.decode(&bytes).unwrap();
        assert_eq!(back.command, m.command);
        assert_eq!(back.topic, m.topic);
        assert_eq!(back.sequence, m.sequence);
    }

    #[test]
    fn messagepack_roundtrip() {
        let m = sample();
        let bytes = Codec::MessagePack.encode(&m).unwrap();
        let back = Codec::MessagePack.decode(&bytes).unwrap();
        assert_eq!(back.command, m.command);
        assert_eq!(back.topic, m.topic);
        assert_eq!(back.sequence, m.sequence);
    }

    #[test]
    fn bson_roundtrip() {
        let m = sample();
        let bytes = Codec::Bson.encode(&m).unwrap();
        let back = Codec::Bson.decode(&bytes).unwrap();
        assert_eq!(back.command, m.command);
        assert_eq!(back.topic, m.topic);
        assert_eq!(back.filter, m.filter);
        assert_eq!(back.sequence, m.sequence);
    }

    #[test]
    fn bson_decode_rejects_json_bytes() {
        let m = sample();
        let j = Codec::Json.encode(&m).unwrap();
        assert!(
            Codec::Bson.decode(&j).is_err(),
            "BSON decoder must reject JSON-encoded bytes"
        );
    }

    #[test]
    fn messagepack_is_smaller_than_json_for_typical_payload() {
        let m = sample();
        let j = Codec::Json.encode(&m).unwrap();
        let mp = Codec::MessagePack.encode(&m).unwrap();
        // Not a strict guarantee for every payload, but typical CqMessages
        // with short keys (`c`, `cid`...) come out noticeably smaller.
        assert!(mp.len() <= j.len(), "msgpack={} json={}", mp.len(), j.len());
    }

    #[test]
    fn cross_codec_does_not_match() {
        // Sanity: bytes are not interchangeable across codecs.
        let m = sample();
        let j = Codec::Json.encode(&m).unwrap();
        assert!(Codec::MessagePack.decode(&j).is_err());
    }

    #[test]
    fn fix_envelope_roundtrip_preserves_supported_fields() {
        let mut m = CqMessage::new(Command::SowAndSubscribe);
        m.command_id = Some("c-1".into());
        m.topic = Some("/market-data".into());
        m.filter = Some("price > 100".into());
        m.sequence = Some(42);
        m.sub_id = Some("sess-1:sub-1".into());
        m.data = Some(serde_json::json!({ "symbol": "AAPL", "px": 150.25 }));
        let bytes = Codec::Fix.encode(&m).unwrap();
        let back = Codec::Fix.decode(&bytes).unwrap();
        assert_eq!(back.command, m.command);
        assert_eq!(back.command_id, m.command_id);
        assert_eq!(back.topic, m.topic);
        assert_eq!(back.filter, m.filter);
        assert_eq!(back.sequence, m.sequence);
        assert_eq!(back.sub_id, m.sub_id);
        // Data is round-tripped via JSON-string in tag 5002.
        let expected = m.data.unwrap();
        let got = back.data.unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn fix_decode_rejects_json_bytes() {
        let m = sample();
        let j = Codec::Json.encode(&m).unwrap();
        assert!(
            Codec::Fix.decode(&j).is_err(),
            "FIX decoder must reject JSON-encoded bytes"
        );
    }

    #[test]
    fn codec_wire_token_round_trips() {
        for c in [Codec::Json, Codec::MessagePack, Codec::Bson, Codec::Fix] {
            assert_eq!(Codec::from_wire(c.as_str()), Some(c));
        }
        assert_eq!(Codec::from_wire("protobuf"), None);
    }

    #[test]
    fn negotiate_picks_client_preferred_when_supported() {
        let client = vec![Codec::MessagePack, Codec::Json];
        assert_eq!(
            negotiate_codec(&client, SUPPORTED_CODECS),
            CodecNegotiationOutcome::Negotiated(Codec::MessagePack)
        );
    }

    #[test]
    fn negotiate_empty_client_implies_legacy_json() {
        assert_eq!(
            negotiate_codec(&[], SUPPORTED_CODECS),
            CodecNegotiationOutcome::Negotiated(Codec::Json)
        );
    }

    #[test]
    fn negotiate_no_overlap_when_json_excluded_both_sides() {
        let client = vec![Codec::Fix];
        let server = vec![Codec::MessagePack, Codec::Bson];
        assert_eq!(
            negotiate_codec(&client, &server),
            CodecNegotiationOutcome::NoOverlap
        );
    }

    #[test]
    fn codec_serde_uses_wire_tokens() {
        // The `codecs` field on the wire must use the lowercase tokens
        // (with msgpack renamed) so old + new peers agree.
        let json = serde_json::to_string(&Codec::MessagePack).unwrap();
        assert_eq!(json, "\"msgpack\"");
        let back: Codec = serde_json::from_str("\"bson\"").unwrap();
        assert_eq!(back, Codec::Bson);
    }
}

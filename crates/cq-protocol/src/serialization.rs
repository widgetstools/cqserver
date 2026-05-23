//! Wire codec for `CqMessage`. Supports JSON (default, human-readable)
//! and MessagePack (compact, language-portable binary).
//!
//! The codec is chosen per-connection at the transport layer:
//! - WebSocket: text frames imply JSON; binary frames imply MessagePack.
//! - TCP: JSON-only in the current build (binary TCP needs a magic-byte
//!   handshake that's a future addition).
//!
//! Both codecs go through `serde::{Serialize, Deserialize}` on
//! `CqMessage`, so the field renames (`c`, `cid`, `sid`, etc.) are
//! preserved across both wire shapes.

use crate::message::CqMessage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    #[default]
    Json,
    MessagePack,
    Bson,
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
        }
    }

    pub fn decode(self, bytes: &[u8]) -> Result<CqMessage, CodecError> {
        match self {
            Codec::Json => Ok(serde_json::from_slice(bytes)?),
            Codec::MessagePack => Ok(rmp_serde::from_slice(bytes)?),
            Codec::Bson => {
                let doc = bson::Document::from_reader(&mut std::io::Cursor::new(bytes))
                    .map_err(|e| CodecError::BsonDecode(e.to_string()))?;
                bson::from_document(doc)
                    .map_err(|e| CodecError::BsonDecode(e.to_string()))
            }
        }
    }
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
}

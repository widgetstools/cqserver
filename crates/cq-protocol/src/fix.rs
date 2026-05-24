//! S23 — FIX 4.x SOH-delimited tag=value codec.
//!
//! Wire shape: `35=D\x0149=SENDER\x0156=TARGET\x01...` — each field is
//! a numeric tag, an `=`, the value, terminated by SOH (0x01). The
//! "checksum field" (10=NNN) at the end is required by the FIX
//! standard but conventional implementations tolerate its absence
//! when in tightly-coupled point-to-point transports; we follow the
//! "permissive read, strict write" convention.
//!
//! ### Module surface
//!
//! - `encode(map)` / `decode(bytes)` — pure tag-keyed map ↔ bytes.
//! - `extract_tag(bytes, tag)` — fast scan to pull a single tag value
//!   out of a frame without constructing the whole map. Used by the
//!   "perfect-hash tag index" / `/35`-style path extraction the
//!   server uses when routing.
//!
//! ### Envelope codec
//!
//! `Codec::Fix` (in `crate::serialization`) maps the standard
//! `CqMessage` fields onto a small fixed set of FIX tags: command →
//! 35, command_id → 11, topic → 55, filter → 200, sequence → 34,
//! sub_id → 5000, ack_type → 39, status → 5001, reason → 58,
//! data → 5002 (JSON-serialized string). Re-decoding round-trips
//! every supported field.
//!
//! Out of scope for this revision: deeply-nested FIX 5.x repeating
//! groups, DataDictionary-driven field typing. Tags outside the
//! known set are preserved as string fields under their numeric
//! name (so a third-party FIX dialect can still flow through).

use serde_json::{Map, Value};

/// SOH = Start-of-Heading. The canonical FIX field delimiter.
pub const SOH: u8 = 0x01;

#[derive(Debug, thiserror::Error)]
pub enum FixError {
    #[error("missing `=` in field at byte {0}")]
    MissingEquals(usize),
    #[error("invalid utf8 at byte {0}")]
    Utf8(usize),
    #[error("tag '{0}' is not numeric")]
    NonNumericTag(String),
    #[error("value for tag '{0}' contains illegal SOH")]
    IllegalValue(String),
}

/// Encode a flat map keyed by numeric-string tags (e.g. `"35"`,
/// `"49"`) into SOH-delimited FIX bytes. Tags MUST parse as
/// non-negative integers; values are stringified (numbers, bools)
/// or passed through verbatim. SOH bytes inside values are rejected.
pub fn encode(map: &Map<String, Value>) -> Result<Vec<u8>, FixError> {
    let mut out = Vec::with_capacity(map.len() * 16);
    // Emit tags in numeric order — the FIX spec doesn't strictly
    // require this for application-level fields, but ordering by
    // tag is the convention every tooling we've seen produces.
    let mut keys: Vec<(u32, &String)> = Vec::with_capacity(map.len());
    for k in map.keys() {
        let tag: u32 = k.parse().map_err(|_| FixError::NonNumericTag(k.clone()))?;
        keys.push((tag, k));
    }
    keys.sort_by_key(|(t, _)| *t);
    for (_, k) in keys {
        let v = &map[k];
        let value_str = match v {
            Value::String(s) => {
                if s.as_bytes().contains(&SOH) {
                    return Err(FixError::IllegalValue(k.clone()));
                }
                s.clone()
            }
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            Value::Array(_) | Value::Object(_) => {
                // FIX is flat; the envelope codec stringifies nested
                // objects (CqMessage.data) into a JSON-string under a
                // sentinel tag before reaching this encoder.
                return Err(FixError::IllegalValue(k.clone()));
            }
        };
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(value_str.as_bytes());
        out.push(SOH);
    }
    Ok(out)
}

/// Decode SOH-delimited FIX bytes into a tag-keyed map. Tags are
/// kept as their original numeric-string representation (so the
/// caller's tag → field mapping is unambiguous).
pub fn decode(bytes: &[u8]) -> Result<Map<String, Value>, FixError> {
    let mut out = Map::new();
    let mut cursor = 0usize;
    for field in bytes.split(|b| *b == SOH) {
        if field.is_empty() {
            cursor += 1;
            continue;
        }
        let eq = field
            .iter()
            .position(|b| *b == b'=')
            .ok_or(FixError::MissingEquals(cursor))?;
        let (name, value) = field.split_at(eq);
        let value = &value[1..]; // skip '='
        let name = std::str::from_utf8(name).map_err(|_| FixError::Utf8(cursor))?;
        let value = std::str::from_utf8(value).map_err(|_| FixError::Utf8(cursor))?;
        // Tag must be numeric — protects callers that route on
        // numeric tags from being fed garbage.
        if name.parse::<u32>().is_err() {
            return Err(FixError::NonNumericTag(name.to_string()));
        }
        out.insert(name.to_string(), Value::String(value.to_string()));
        cursor += field.len() + 1;
    }
    Ok(out)
}

/// Fast path-extract: pull the value of a single tag from a FIX
/// frame without building the whole map. Returns `None` if the tag
/// isn't present. Used by the server's content-routing path
/// (`/35=D` matches "new order single", etc.).
pub fn extract_tag(bytes: &[u8], tag: u32) -> Option<&str> {
    let needle: Vec<u8> = format!("{}=", tag).into_bytes();
    let mut start = 0usize;
    while start < bytes.len() {
        let field_end = bytes[start..]
            .iter()
            .position(|b| *b == SOH)
            .map(|p| start + p)
            .unwrap_or(bytes.len());
        let field = &bytes[start..field_end];
        if field.len() >= needle.len() && field[..needle.len()] == needle[..] {
            let value = &field[needle.len()..];
            return std::str::from_utf8(value).ok();
        }
        start = field_end + 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn new_order_single_bytes() -> Vec<u8> {
        // Minimal NewOrderSingle (35=D). Tags are not exhaustive but
        // cover the canonical fields the "extract" test cares about.
        let body = "8=FIX.4.4\x019=130\x0135=D\x0149=SENDER\x0156=TARGET\x0111=ORDER-1\x0155=AAPL\x0154=1\x0138=100\x0140=2\x0144=150.25\x0110=000\x01";
        body.as_bytes().to_vec()
    }

    #[test]
    fn encode_then_decode_round_trips() {
        let mut m = Map::new();
        m.insert("35".into(), json!("D"));
        m.insert("11".into(), json!("ORDER-1"));
        m.insert("55".into(), json!("AAPL"));
        m.insert("38".into(), json!(100));
        let bytes = encode(&m).expect("encode");
        let back = decode(&bytes).expect("decode");
        // Numbers come back as strings — that's the FIX wire model.
        assert_eq!(back.get("35").unwrap().as_str(), Some("D"));
        assert_eq!(back.get("11").unwrap().as_str(), Some("ORDER-1"));
        assert_eq!(back.get("38").unwrap().as_str(), Some("100"));
    }

    #[test]
    fn extract_tag_pulls_msg_type_from_new_order_single() {
        let frame = new_order_single_bytes();
        assert_eq!(extract_tag(&frame, 35), Some("D"));
        assert_eq!(extract_tag(&frame, 55), Some("AAPL"));
        assert_eq!(extract_tag(&frame, 38), Some("100"));
        // Missing tag yields None.
        assert_eq!(extract_tag(&frame, 99999), None);
    }

    #[test]
    fn extract_tag_finds_tag_at_first_position() {
        let frame = new_order_single_bytes();
        // Tag 8 sits at the very start.
        assert_eq!(extract_tag(&frame, 8), Some("FIX.4.4"));
    }

    #[test]
    fn decode_rejects_non_numeric_tag() {
        let frame = b"foo=bar\x01";
        let r = decode(frame);
        assert!(r.is_err(), "non-numeric tag must be rejected");
    }

    #[test]
    fn encode_rejects_soh_in_value() {
        let mut m = Map::new();
        m.insert("35".into(), json!("D\x01injected"));
        let r = encode(&m);
        assert!(r.is_err(), "embedded SOH must be rejected");
    }

    #[test]
    fn fields_emit_in_tag_numeric_order() {
        let mut m = Map::new();
        m.insert("55".into(), json!("AAPL"));
        m.insert("35".into(), json!("D"));
        m.insert("8".into(), json!("FIX.4.4"));
        let bytes = encode(&m).expect("encode");
        let s = std::str::from_utf8(&bytes).expect("utf8");
        // 8 must come before 35, 35 before 55.
        let p8 = s.find("8=FIX.4.4").expect("8 present");
        let p35 = s.find("35=D").expect("35 present");
        let p55 = s.find("55=AAPL").expect("55 present");
        assert!(p8 < p35, "8 should precede 35: {} vs {}", p8, p35);
        assert!(p35 < p55, "35 should precede 55: {} vs {}", p35, p55);
    }
}

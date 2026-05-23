//! NVFIX (Named-Value FIX) — a flat `name=value` payload codec.
//!
//! Wire shape: `name1=value1\x01name2=value2\x01...` (each field
//! terminated by the SOH byte `0x01`, FIX-style). Compared to numeric
//! FIX it skips the per-application tag-number registry, so an existing
//! JSON-shaped record maps one-to-one onto NVFIX fields.
//!
//! Intended use: payload-level alternative to JSON for the `data` field
//! of a `CqMessage`. The CqMessage envelope itself is still encoded by
//! the session codec (JSON or MessagePack).
//!
//! Limitations
//! -----------
//! - Values are unquoted strings. Numbers round-trip as strings; the
//!   caller is responsible for re-parsing them if typed values are
//!   needed. A `decode_typed` helper does best-effort numeric coercion
//!   when JSON parses cleanly.
//! - Field names and values may not contain SOH (`0x01`) or `=`.
//!   Encoding fails on either.

use serde_json::{Map, Value};

pub const SOH: u8 = 0x01;

#[derive(Debug, thiserror::Error)]
pub enum NvFixError {
    #[error("field name '{0}' contains illegal char (SOH or `=`)")]
    IllegalName(String),
    #[error("field value contains illegal SOH")]
    IllegalValue,
    #[error("missing `=` in field at byte {0}")]
    MissingEquals(usize),
    #[error("invalid utf8")]
    Utf8,
}

/// Render a flat JSON object as NVFIX bytes. Each field becomes
/// `name=value\x01`. Non-string scalar values (numbers, bools) are
/// stringified; nested objects/arrays are not supported (NVFIX is flat).
pub fn encode(map: &Map<String, Value>) -> Result<Vec<u8>, NvFixError> {
    let mut out = Vec::with_capacity(map.len() * 16);
    for (k, v) in map {
        if k.is_empty() || k.as_bytes().iter().any(|&b| b == SOH || b == b'=') {
            return Err(NvFixError::IllegalName(k.clone()));
        }
        let value_str = match v {
            Value::String(s) => {
                if s.as_bytes().contains(&SOH) {
                    return Err(NvFixError::IllegalValue);
                }
                s.clone()
            }
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Null => String::new(),
            Value::Array(_) | Value::Object(_) => {
                // Refuse nested structures rather than silently dropping
                // information. Caller should flatten first.
                return Err(NvFixError::IllegalValue);
            }
        };
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(value_str.as_bytes());
        out.push(SOH);
    }
    Ok(out)
}

/// Parse NVFIX bytes into a flat `name -> string` map. Values are kept
/// as strings; see `decode_typed` for best-effort number/bool coercion.
pub fn decode(bytes: &[u8]) -> Result<Map<String, Value>, NvFixError> {
    let mut out = Map::new();
    let mut cursor = 0usize;
    for field in bytes.split(|b| *b == SOH) {
        if field.is_empty() {
            cursor += 1;
            continue;
        }
        let eq = field.iter().position(|b| *b == b'=').ok_or(NvFixError::MissingEquals(cursor))?;
        let (name, value) = field.split_at(eq);
        let value = &value[1..]; // skip '='
        let name = std::str::from_utf8(name).map_err(|_| NvFixError::Utf8)?.to_string();
        let value = std::str::from_utf8(value).map_err(|_| NvFixError::Utf8)?.to_string();
        out.insert(name, Value::String(value));
        cursor += field.len() + 1;
    }
    Ok(out)
}

/// Parse NVFIX bytes, attempting to recover typed values: anything that
/// parses cleanly as an integer or float becomes `Value::Number`; `true`
/// / `false` become `Value::Bool`. Everything else stays as a string.
pub fn decode_typed(bytes: &[u8]) -> Result<Map<String, Value>, NvFixError> {
    let raw = decode(bytes)?;
    let mut out = Map::with_capacity(raw.len());
    for (k, v) in raw {
        let s = v.as_str().unwrap_or("");
        let coerced = if let Ok(n) = s.parse::<i64>() {
            Value::Number(n.into())
        } else if let Ok(f) = s.parse::<f64>() {
            serde_json::Number::from_f64(f).map(Value::Number).unwrap_or(v)
        } else if s == "true" {
            Value::Bool(true)
        } else if s == "false" {
            Value::Bool(false)
        } else {
            v
        };
        out.insert(k, coerced);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_simple_map() {
        let mut m = Map::new();
        m.insert("symbol".into(), Value::String("AAPL".into()));
        m.insert("price".into(), Value::Number(serde_json::Number::from_f64(150.5).unwrap()));
        let bytes = encode(&m).unwrap();
        let s = String::from_utf8(bytes).unwrap();
        // Either field order is acceptable since serde_json::Map preserves
        // insertion order when the `preserve_order` feature isn't on, but
        // we just check that both fields appear with their delimiters.
        assert!(s.contains("symbol=AAPL\u{1}"));
        assert!(s.contains("price=150.5\u{1}"));
    }

    #[test]
    fn roundtrip_string_fields() {
        let mut m = Map::new();
        m.insert("a".into(), Value::String("hello".into()));
        m.insert("b".into(), Value::String("world".into()));
        let bytes = encode(&m).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.get("a").unwrap(), "hello");
        assert_eq!(back.get("b").unwrap(), "world");
    }

    #[test]
    fn decode_typed_recovers_numbers_and_bools() {
        let bytes = b"qty=100\x01price=99.5\x01active=true\x01name=Alice\x01";
        let m = decode_typed(bytes).unwrap();
        assert_eq!(m.get("qty").unwrap(), 100);
        assert_eq!(m.get("price").unwrap(), 99.5);
        assert_eq!(m.get("active").unwrap(), &Value::Bool(true));
        assert_eq!(m.get("name").unwrap(), "Alice");
    }

    #[test]
    fn rejects_illegal_field_name() {
        let mut m = Map::new();
        m.insert("bad=name".into(), Value::String("x".into()));
        assert!(matches!(encode(&m), Err(NvFixError::IllegalName(_))));
    }

    #[test]
    fn rejects_nested_object() {
        let mut m = Map::new();
        let inner = Value::Object(Map::new());
        m.insert("nested".into(), inner);
        assert!(matches!(encode(&m), Err(NvFixError::IllegalValue)));
    }

    #[test]
    fn empty_input_decodes_to_empty_map() {
        let m = decode(b"").unwrap();
        assert!(m.is_empty());
    }

    #[test]
    fn trailing_soh_is_tolerated() {
        let bytes = b"k=v\x01\x01";
        let m = decode(bytes).unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m.get("k").unwrap(), "v");
    }

    #[test]
    fn null_encodes_as_empty_value() {
        let mut m = Map::new();
        m.insert("absent".into(), Value::Null);
        let bytes = encode(&m).unwrap();
        assert_eq!(bytes, b"absent=\x01");
        let back = decode(&bytes).unwrap();
        assert_eq!(back.get("absent").unwrap(), "");
    }
}

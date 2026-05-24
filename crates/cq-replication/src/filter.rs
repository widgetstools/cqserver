//! S12 — per-destination replication filter + transform.
//!
//! The primary's shipper can optionally apply two passes to each log
//! entry before forwarding it to a downstream destination:
//!
//!   1. **Filter** — a simple `column = "value"` equality match on the
//!      entry's JSON payload. Non-matching entries are dropped (the
//!      destination never sees them); matching entries proceed.
//!   2. **Transform** — drop a list of named fields from the JSON
//!      payload before shipping. Useful for stripping restricted
//!      columns from less-trusted downstream targets.
//!
//! The intent is "good enough for routing-and-redaction" without
//! pulling the full SQL predicate engine into the replication crate.
//! Operators that need richer filtering should wire that at the
//! application layer (publish to topic A or topic B, replicate
//! independently).
//!
//! Tombstones (zero-length payload) always ship through unchanged —
//! filter on the data, but never drop a delete or you'd diverge the
//! standby's SOW from the primary's.

use serde::Deserialize;
use serde_json::{Map, Value};

/// Single equality predicate. `column = value` is matched against the
/// JSON payload's named field. Numeric values are compared after
/// JSON deserialization, so `"42"` matches an integer field equal to
/// 42 — the simplest way to keep config files plain-text.
#[derive(Debug, Clone, Deserialize)]
pub struct FilterSpec {
    pub column: String,
    pub value: String,
}

/// Field-strip transform. Each listed field is removed from the JSON
/// payload before shipping. Dotted-path names are interpreted
/// literally (the column store is flat — see the wider repo's
/// flatten convention), not as nested paths.
#[derive(Debug, Clone, Deserialize)]
pub struct TransformSpec {
    #[serde(default)]
    pub strip_fields: Vec<String>,
}

/// Apply `filter` to a serialized JSON payload. Returns `true` when
/// the entry should ship, `false` when it should be dropped.
///
/// Semantics:
///   - `None` filter → always ship.
///   - Tombstones (empty payload) → always ship; the standby needs
///     the delete or its SOW will diverge.
///   - Malformed JSON payload → ship (defensive — replicating an
///     unparseable byte stream is better than silently dropping it).
pub fn apply_filter(payload: &[u8], filter: Option<&FilterSpec>) -> bool {
    let Some(f) = filter else { return true };
    if payload.is_empty() {
        return true; // tombstone — always ship
    }
    let parsed: Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return true, // defensive — ship unparseable bytes
    };
    let map = match parsed.as_object() {
        Some(m) => m,
        None => return true,
    };
    match map.get(&f.column) {
        Some(Value::String(s)) => s == &f.value,
        Some(Value::Number(n)) => n.to_string() == f.value,
        Some(Value::Bool(b)) => b.to_string() == f.value,
        _ => false,
    }
}

/// Apply `transform` to a payload. Returns the rewritten bytes (or
/// the original bytes if there's nothing to strip or the payload
/// isn't JSON). Tombstones are returned unchanged.
pub fn apply_transform(
    payload: Vec<u8>,
    transform: Option<&TransformSpec>,
) -> Vec<u8> {
    let Some(t) = transform else { return payload };
    if t.strip_fields.is_empty() || payload.is_empty() {
        return payload;
    }
    let mut parsed: Value = match serde_json::from_slice(&payload) {
        Ok(v) => v,
        Err(_) => return payload,
    };
    if let Some(map) = parsed.as_object_mut() {
        let mut stripped = false;
        for field in &t.strip_fields {
            if map.remove(field).is_some() {
                stripped = true;
            }
        }
        if !stripped {
            return payload;
        }
        // Preserve insertion order of remaining fields.
        let new_map: Map<String, Value> = map.clone();
        serde_json::to_vec(&Value::Object(new_map)).unwrap_or(payload)
    } else {
        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload_for(v: Value) -> Vec<u8> {
        serde_json::to_vec(&v).unwrap()
    }

    #[test]
    fn no_filter_ships_everything() {
        let p = payload_for(json!({ "k": "x", "desk": "FX" }));
        assert!(apply_filter(&p, None));
    }

    #[test]
    fn matching_string_field_ships() {
        let p = payload_for(json!({ "k": "x", "desk": "RATES" }));
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        assert!(apply_filter(&p, Some(&f)));
    }

    #[test]
    fn non_matching_string_field_drops() {
        let p = payload_for(json!({ "k": "x", "desk": "FX" }));
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        assert!(!apply_filter(&p, Some(&f)));
    }

    #[test]
    fn missing_field_drops() {
        let p = payload_for(json!({ "k": "x" }));
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        assert!(!apply_filter(&p, Some(&f)));
    }

    #[test]
    fn numeric_field_compares_stringified() {
        let p = payload_for(json!({ "qty": 100 }));
        let f = FilterSpec {
            column: "qty".into(),
            value: "100".into(),
        };
        assert!(apply_filter(&p, Some(&f)));
    }

    #[test]
    fn tombstone_always_ships_regardless_of_filter() {
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        assert!(apply_filter(&[], Some(&f)));
    }

    #[test]
    fn malformed_payload_ships_defensively() {
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        assert!(apply_filter(b"not json", Some(&f)));
    }

    #[test]
    fn no_transform_returns_payload_unchanged() {
        let p = payload_for(json!({ "k": "x", "secret": "redact-me" }));
        let out = apply_transform(p.clone(), None);
        assert_eq!(out, p);
    }

    #[test]
    fn transform_strips_listed_fields() {
        let p = payload_for(json!({ "k": "x", "desk": "RATES", "secret": "redact" }));
        let t = TransformSpec {
            strip_fields: vec!["secret".into()],
        };
        let out = apply_transform(p, Some(&t));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v.get("k").and_then(|x| x.as_str()), Some("x"));
        assert_eq!(v.get("desk").and_then(|x| x.as_str()), Some("RATES"));
        assert!(v.get("secret").is_none(), "secret should be stripped");
    }

    #[test]
    fn transform_no_op_when_field_absent() {
        let p = payload_for(json!({ "k": "x" }));
        let t = TransformSpec {
            strip_fields: vec!["nope".into()],
        };
        let out = apply_transform(p.clone(), Some(&t));
        // Returns original bytes (no re-serialize on no-op).
        assert_eq!(out, p);
    }

    #[test]
    fn transform_passes_tombstone_unchanged() {
        let t = TransformSpec {
            strip_fields: vec!["secret".into()],
        };
        let out = apply_transform(Vec::new(), Some(&t));
        assert!(out.is_empty());
    }

    #[test]
    fn filter_then_transform_pipeline() {
        // Realistic flow: filter matches → transform redacts → ship.
        let p = payload_for(json!({ "k": "x", "desk": "RATES", "secret": "redact" }));
        let f = FilterSpec {
            column: "desk".into(),
            value: "RATES".into(),
        };
        let t = TransformSpec {
            strip_fields: vec!["secret".into()],
        };
        assert!(apply_filter(&p, Some(&f)));
        let out = apply_transform(p, Some(&t));
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(v.get("secret").is_none());
        assert_eq!(v.get("desk").and_then(|x| x.as_str()), Some("RATES"));
    }
}

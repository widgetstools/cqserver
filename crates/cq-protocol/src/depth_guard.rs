//! Wire-layer JSON nesting guard.
//!
//! The flattener (`cq-core`) caps recursion at `FlattenConfig::max_depth`
//! (default 32), but that cap only kicks in *after* the payload has
//! already been decoded into a `serde_json::Value`. A sufficiently
//! deep payload (hundreds of levels of `{"n": ...}`) can cost real
//! time/stack pressure in `serde_json::from_slice` itself, before the
//! flattener ever sees it — see `AMPS_PARITY_WORKLOG.md` "Deep-JSON
//! publish stalls — wire-codec layer".
//!
//! [`json_depth_exceeds`] is a dependency-free, linear pre-scan over
//! the raw bytes that counts the maximum `{`/`[` nesting depth,
//! correctly skipping over the contents of JSON strings (so braces
//! inside string values never count towards depth). Call it BEFORE
//! handing bytes to `serde_json`/`rmp_serde`/etc. so oversized
//! nesting is rejected with a cheap, allocation-free O(n) scan
//! instead of paying decode cost first.

/// Default maximum nesting depth allowed on the wire. Chosen well
/// above any realistic payload shape (the flattener's own default is
/// 32) but far below depths that stress decoder recursion.
pub const MAX_WIRE_JSON_DEPTH: usize = 128;

/// Scan `bytes` (assumed to be a JSON document, not yet parsed) and
/// return `true` if the maximum nesting depth of `{`/`[` structures
/// exceeds `limit`.
///
/// This is a byte-level scan, not a full JSON parser: it does not
/// validate that the document is well-formed JSON, only tracks brace/
/// bracket balance while correctly skipping over string contents
/// (respecting `"` string boundaries and `\` escapes, including an
/// escaped backslash immediately before a closing quote). Malformed
/// JSON that isn't actually over the depth limit will still be
/// rejected later by the real decoder; this function's only job is
/// to bound worst-case decode cost.
///
/// Returns `false` for empty input.
pub fn json_depth_exceeds(bytes: &[u8], limit: usize) -> bool {
    let mut depth: usize = 0;
    let mut max_depth: usize = 0;
    let mut in_string = false;
    let mut escaped = false;

    for &b in bytes {
        if in_string {
            if escaped {
                // This byte is escaped (e.g. the `n` in `\n`, or the
                // second `\` in `\\`); it can't start a new escape or
                // end the string.
                escaped = false;
                continue;
            }
            match b {
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max_depth {
                    max_depth = depth;
                    if max_depth > limit {
                        return true;
                    }
                }
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }

    max_depth > limit
}

/// Convenience wrapper using [`MAX_WIRE_JSON_DEPTH`].
pub fn json_depth_exceeds_default(bytes: &[u8]) -> bool {
    json_depth_exceeds(bytes, MAX_WIRE_JSON_DEPTH)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested(depth: usize) -> Vec<u8> {
        let mut s = "{\"n\":".repeat(depth);
        s.push('1');
        s.push_str(&"}".repeat(depth));
        s.into_bytes()
    }

    #[test]
    fn shallow_payload_is_within_limit() {
        let bytes = br#"{"a": 1, "b": {"c": 2}}"#;
        assert!(!json_depth_exceeds(bytes, 128));
    }

    #[test]
    fn empty_input_is_not_over_limit() {
        assert!(!json_depth_exceeds(b"", 128));
    }

    #[test]
    fn exactly_at_limit_is_not_exceeded() {
        // `nested(128)` reaches max depth 128 (the object braces) —
        // at the limit, not over it.
        let bytes = nested(128);
        assert!(!json_depth_exceeds(&bytes, 128));
    }

    #[test]
    fn one_past_limit_is_exceeded() {
        let bytes = nested(129);
        assert!(json_depth_exceeds(&bytes, 128));
    }

    #[test]
    fn five_hundred_levels_exceeds_default_limit() {
        let bytes = nested(500);
        assert!(json_depth_exceeds_default(&bytes));
    }

    #[test]
    fn deep_array_nesting_also_counts() {
        let mut s = "[".repeat(200);
        s.push('1');
        s.push_str(&"]".repeat(200));
        assert!(json_depth_exceeds(s.as_bytes(), 128));
    }

    #[test]
    fn mixed_object_array_nesting_counts_combined_depth() {
        // Alternate {"n":[ ... ]} — depth increases by 2 per level.
        let levels = 70; // 140 total depth > 128
        let mut s = String::new();
        for _ in 0..levels {
            s.push_str("{\"n\":[");
        }
        s.push('1');
        for _ in 0..levels {
            s.push_str("]}");
        }
        assert!(json_depth_exceeds(s.as_bytes(), 128));
    }

    #[test]
    fn braces_inside_strings_do_not_count_towards_depth() {
        // A shallow object whose *string value* is full of brace
        // characters — must NOT be treated as deep nesting.
        let payload = format!(
            r#"{{"a": "{}"}}"#,
            "}}}}{{{{".repeat(50)
        );
        assert!(
            !json_depth_exceeds(payload.as_bytes(), 128),
            "braces inside a JSON string must not count as structural nesting"
        );
    }

    #[test]
    fn escaped_quote_inside_string_does_not_end_string_early() {
        // `{"a": "he said \"{{{{\" and meant it"}` — the escaped quote
        // must not be treated as the string terminator, or the braces
        // after it would be miscounted as structural.
        let payload = br#"{"a": "he said \"{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{\" and meant it"}"#;
        assert!(!json_depth_exceeds(payload, 128));
    }

    #[test]
    fn escaped_backslash_before_quote_correctly_ends_string() {
        // `"a\\"` — the `\\` is an escaped backslash, so the following
        // `"` DOES end the string (not escaped). Depth after this
        // point comes from real structure, so this must be counted
        // correctly rather than treating the string as still open.
        let mut s = String::from(r#"{"a": "trailing backslash \\"}"#);
        // Sanity: this is shallow.
        assert!(!json_depth_exceeds(s.as_bytes(), 128));
        // Now nest real structure *after* the escaped-backslash
        // string closes, past the limit — this must be detected.
        s = format!(
            r#"{{"a": "\\", "b": {}}}"#,
            String::from_utf8(nested(200)).unwrap()
        );
        assert!(json_depth_exceeds(s.as_bytes(), 128));
    }

    #[test]
    fn depth_scan_ignores_malformed_but_shallow_input() {
        // Not valid JSON, but shallow — must not be flagged. The real
        // decoder is responsible for rejecting malformed input; this
        // scanner only bounds nesting depth.
        assert!(!json_depth_exceeds(b"not even json {", 128));
    }
}

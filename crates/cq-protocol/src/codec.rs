//! Frame codec for TCP transport.
//!
//! Wire format: `[length: u32 BE][payload: bytes]`
//! Payload is a JSON-serialized CqMessage.
//!
//! When per-connection compression is negotiated (S27), the top bit of
//! the length prefix is set to indicate a zstd-compressed payload.
//! Sessions that don't negotiate compression never set the bit, so
//! pre-S27 peers are completely unaffected.

use crate::compression::{
    Compression, COMPRESSED_FLAG, MIN_COMPRESSED_PAYLOAD_BYTES,
};
use bytes::{Buf, BufMut, BytesMut};
use std::io;

/// Maximum decoded frame size (16 MiB). Compressed frames may be
/// smaller on the wire but the decoded payload must still fit.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Encode a message into a length-prefixed frame without compression.
/// Kept for callers that pre-date S27 (tests, simple paths) and for
/// connections that negotiated `Compression::None`.
pub fn encode_frame(payload: &[u8], dst: &mut BytesMut) {
    encode_frame_with(payload, dst, Compression::None);
}

/// Encode a message into a length-prefixed frame, optionally
/// compressing the body when the connection negotiated a non-None
/// algorithm AND the payload is large enough to benefit (see
/// `MIN_COMPRESSED_PAYLOAD_BYTES`).
pub fn encode_frame_with(payload: &[u8], dst: &mut BytesMut, compression: Compression) {
    let body: std::borrow::Cow<'_, [u8]> =
        if matches!(compression, Compression::Zstd) && payload.len() >= MIN_COMPRESSED_PAYLOAD_BYTES
        {
            match zstd::stream::encode_all(payload, 0) {
                Ok(out) if out.len() < payload.len() => std::borrow::Cow::Owned(out),
                _ => std::borrow::Cow::Borrowed(payload), // compression didn't help — send raw
            }
        } else {
            std::borrow::Cow::Borrowed(payload)
        };
    let compressed = matches!(&body, std::borrow::Cow::Owned(_));
    let mut len_word = body.len() as u32;
    if compressed {
        len_word |= COMPRESSED_FLAG;
    }
    dst.reserve(4 + body.len());
    dst.put_u32(len_word);
    dst.put_slice(&body);
}

/// Try to decode a length-prefixed frame from the buffer.
/// Returns `Ok(Some(payload))` if a complete frame is available,
/// `Ok(None)` if more data is needed, or `Err` on protocol violation.
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
    if src.len() < 4 {
        return Ok(None); // Need more data for length prefix
    }

    let raw = u32::from_be_bytes([src[0], src[1], src[2], src[3]]);
    let compressed = (raw & COMPRESSED_FLAG) != 0;
    let length = (raw & !COMPRESSED_FLAG) as usize;

    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes (max {})", length, MAX_FRAME_SIZE),
        ));
    }

    if src.len() < 4 + length {
        src.reserve(4 + length - src.len());
        return Ok(None);
    }

    src.advance(4);
    let payload = src.split_to(length);
    if compressed {
        let decompressed = zstd::stream::decode_all(&payload[..]).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("zstd decode failed: {}", e),
            )
        })?;
        let mut out = BytesMut::with_capacity(decompressed.len());
        out.put_slice(&decompressed);
        Ok(Some(out))
    } else {
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let payload = b"hello world";
        let mut buf = BytesMut::new();
        encode_frame(payload, &mut buf);

        assert_eq!(buf.len(), 4 + payload.len());

        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded[..], payload);
    }

    #[test]
    fn test_partial_frame() {
        let mut buf = BytesMut::new();
        buf.put_u32(100); // Says 100 bytes of payload
        buf.put_slice(b"short"); // But only 5 bytes

        let result = decode_frame(&mut buf).unwrap();
        assert!(result.is_none()); // Need more data
    }

    #[test]
    fn test_oversized_frame() {
        let mut buf = BytesMut::new();
        // High bit clear → length field interpreted literally.
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);

        let result = decode_frame(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn zstd_compresses_large_payload_and_round_trips() {
        // A repeating string compresses very well — perfect for the
        // "did we actually compress?" assertion.
        let payload: Vec<u8> = "abcd1234".repeat(1024).into_bytes();
        let mut buf = BytesMut::new();
        encode_frame_with(&payload, &mut buf, Compression::Zstd);
        // Wire size must be smaller than `4 + payload.len()`.
        assert!(
            buf.len() < 4 + payload.len(),
            "expected zstd to shrink payload; wire len = {}, raw len = {}",
            buf.len(),
            payload.len()
        );
        // The high bit of the length word must be set.
        let raw_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert!(raw_len & COMPRESSED_FLAG != 0, "compressed flag should be set");
        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded[..], &payload[..]);
    }

    #[test]
    fn small_payload_stays_uncompressed_even_with_zstd_negotiated() {
        // Below MIN_COMPRESSED_PAYLOAD_BYTES, we don't bother.
        let payload = b"short payload";
        let mut buf = BytesMut::new();
        encode_frame_with(payload, &mut buf, Compression::Zstd);
        let raw_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(
            raw_len & COMPRESSED_FLAG,
            0,
            "compressed flag must NOT be set for small payloads"
        );
        let decoded = decode_frame(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded[..], payload);
    }
}

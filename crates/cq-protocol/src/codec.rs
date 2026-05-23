//! Frame codec for TCP transport.
//!
//! Wire format: `[length: u32 BE][payload: bytes]`
//! Payload is a JSON-serialized CqMessage.

use bytes::{Buf, BufMut, BytesMut};
use std::io;

/// Maximum frame size (16 MB).
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// Encode a message into a length-prefixed frame.
pub fn encode_frame(payload: &[u8], dst: &mut BytesMut) {
    dst.reserve(4 + payload.len());
    dst.put_u32(payload.len() as u32);
    dst.put_slice(payload);
}

/// Try to decode a length-prefixed frame from the buffer.
/// Returns `Ok(Some(payload))` if a complete frame is available,
/// `Ok(None)` if more data is needed, or `Err` on protocol violation.
pub fn decode_frame(src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
    if src.len() < 4 {
        return Ok(None); // Need more data for length prefix
    }

    let length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

    if length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Frame too large: {} bytes (max {})", length, MAX_FRAME_SIZE),
        ));
    }

    if src.len() < 4 + length {
        // Reserve space for the rest of the frame
        src.reserve(4 + length - src.len());
        return Ok(None); // Need more data
    }

    // Consume the length prefix
    src.advance(4);
    // Split off the payload
    let payload = src.split_to(length);
    Ok(Some(payload))
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
        buf.put_u32((MAX_FRAME_SIZE + 1) as u32);

        let result = decode_frame(&mut buf);
        assert!(result.is_err());
    }
}

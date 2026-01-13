//! Kafka wire protocol codec.
//!
//! Handles framing and request/response header parsing for the Kafka protocol.
//!
//! # Wire Format
//!
//! ```text
//! Request:
//! ┌─────────────────┬────────────────────────────────────────────────┐
//! │  Length (4B)    │                  Payload                       │
//! │   big-endian    │  RequestHeader + RequestBody                   │
//! └─────────────────┴────────────────────────────────────────────────┘
//!
//! Response:
//! ┌─────────────────┬────────────────────────────────────────────────┐
//! │  Length (4B)    │                  Payload                       │
//! │   big-endian    │  ResponseHeader + ResponseBody                 │
//! └─────────────────┴────────────────────────────────────────────────┘
//! ```

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::{
    messages::{RequestHeader, ResponseHeader},
    protocol::{Decodable, Encodable},
};

use crate::error::{KafkaCompatError, KafkaCompatResult};

/// Get request header version for a given API key and version.
///
/// Header version determines the request header format:
/// - v1: includes `client_id`
/// - v2: adds tagged fields (flexible versions)
///
/// Note: kafka-protocol crate only supports header versions 1-2.
#[must_use]
pub const fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    // Most APIs use header v1 for older versions and v2 for flexible versions.
    // ApiVersions v3+ uses flexible encoding (header v2).
    match api_key {
        18 => { // ApiVersions
            if api_version >= 3 { 2 } else { 1 }
        }
        _ => {
            // Use v1 for most cases which includes client_id.
            // Could upgrade to v2 for flexible versions if needed.
            1
        }
    }
}

/// Get response header version for a given API key and version.
#[must_use]
pub const fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    // Response headers: v0 has just correlation_id, v1 adds tagged fields.
    match api_key {
        // ApiVersions: v3+ uses flexible encoding (header v1).
        18 if api_version >= 3 => 1,
        _ => 0, // Use v0 for most responses (just correlation_id).
    }
}

/// Maximum message size (100 MB, same as Kafka default).
pub const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// Minimum frame size (4 bytes for length prefix).
pub const FRAME_HEADER_SIZE: usize = 4;

/// Read a length-prefixed frame from the buffer.
///
/// Returns `None` if not enough data is available yet.
/// Returns `Some(bytes)` with the frame payload (excluding length prefix).
///
/// # Errors
///
/// Returns an error if the message exceeds `MAX_MESSAGE_SIZE`.
pub fn read_frame(buf: &mut BytesMut) -> KafkaCompatResult<Option<Bytes>> {
    // Need at least 4 bytes for the length prefix.
    if buf.len() < FRAME_HEADER_SIZE {
        return Ok(None);
    }

    // Peek at the length (don't consume yet).
    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    // Validate length.
    if length > MAX_MESSAGE_SIZE {
        return Err(KafkaCompatError::Decode {
            message: format!("Message too large: {length} bytes (max {MAX_MESSAGE_SIZE})"),
        });
    }

    // Check if we have the full message.
    let total_size = FRAME_HEADER_SIZE + length;
    if buf.len() < total_size {
        return Ok(None);
    }

    // Consume the length prefix and extract the payload.
    buf.advance(FRAME_HEADER_SIZE);
    let payload = buf.split_to(length).freeze();

    Ok(Some(payload))
}

/// Write a length-prefixed frame to the buffer.
pub fn write_frame(buf: &mut BytesMut, payload: &[u8]) {
    // Safety: MAX_MESSAGE_SIZE is 100MB, well under u32::MAX (4GB).
    #[allow(clippy::cast_possible_truncation)]
    let length = payload.len() as u32;
    buf.put_u32(length);
    buf.put_slice(payload);
}

/// Decoded request with header and body bytes.
#[derive(Debug)]
pub struct DecodedRequest {
    /// The request header.
    pub header: RequestHeader,
    /// API key from the header.
    pub api_key: i16,
    /// API version from the header.
    pub api_version: i16,
    /// Correlation ID for matching responses.
    pub correlation_id: i32,
    /// Remaining body bytes after header.
    pub body: Bytes,
}

/// Decode a request header from the frame payload.
///
/// Returns the header and the remaining body bytes.
///
/// # Errors
///
/// Returns an error if the payload is too short or header decoding fails.
pub fn decode_request_header(mut payload: Bytes) -> KafkaCompatResult<DecodedRequest> {
    // First, we need to peek at api_key and api_version to know the header version.
    if payload.len() < 4 {
        return Err(KafkaCompatError::Decode {
            message: "Request too short for header".to_string(),
        });
    }

    let api_key = i16::from_be_bytes([payload[0], payload[1]]);
    let api_version = i16::from_be_bytes([payload[2], payload[3]]);

    // Get the header version for this API.
    let header_version = request_header_version(api_key, api_version);

    // Decode the full header.
    let header = RequestHeader::decode(&mut payload, header_version)
        .map_err(KafkaCompatError::decode)?;

    let correlation_id = header.correlation_id;

    Ok(DecodedRequest {
        header,
        api_key,
        api_version,
        correlation_id,
        body: payload,
    })
}

/// Encode a response with header and body.
///
/// # Errors
///
/// Returns an error if header or body encoding fails.
pub fn encode_response<R: Encodable>(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    response: &R,
) -> KafkaCompatResult<BytesMut> {
    let header_version = response_header_version(api_key, api_version);

    // Build response header.
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;

    // Encode header and body.
    let mut buf = BytesMut::new();
    header
        .encode(&mut buf, header_version)
        .map_err(KafkaCompatError::encode)?;
    response
        .encode(&mut buf, api_version)
        .map_err(KafkaCompatError::encode)?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_frame_incomplete() {
        let mut buf = BytesMut::from(&[0, 0, 0, 10][..]); // Length = 10, but no payload.
        let result = read_frame(&mut buf).unwrap();
        assert!(result.is_none());
        assert_eq!(buf.len(), 4); // Buffer unchanged.
    }

    #[test]
    fn test_read_frame_complete() {
        let mut buf = BytesMut::new();
        buf.put_u32(5); // Length = 5.
        buf.put_slice(b"hello");

        let result = read_frame(&mut buf).unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"hello")));
        assert!(buf.is_empty()); // Buffer consumed.
    }

    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn test_read_frame_too_large() {
        let mut buf = BytesMut::new();
        buf.put_u32(MAX_MESSAGE_SIZE as u32 + 1);

        let result = read_frame(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_frame() {
        let mut buf = BytesMut::new();
        write_frame(&mut buf, b"hello");

        assert_eq!(buf.len(), 9); // 4 (length) + 5 (payload).
        assert_eq!(&buf[0..4], &[0, 0, 0, 5]); // Length = 5.
        assert_eq!(&buf[4..9], b"hello");
    }
}

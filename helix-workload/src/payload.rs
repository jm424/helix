//! Verifiable payload encoding and decoding.
//!
//! Each message payload is self-describing to enable verification without external state.
//! The payload includes operation ID, sequence number, timestamp, and a checksum.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use rand::Rng;
use thiserror::Error;

/// Header size in bytes (`operation_id` + sequence + timestamp + checksum + `payload_len`).
const HEADER_SIZE: usize = 8 + 8 + 8 + 8 + 8;

/// Minimum payload size (header only).
pub const PAYLOAD_SIZE_MIN: usize = HEADER_SIZE;

/// Maximum payload size (64KB).
pub const PAYLOAD_SIZE_MAX: usize = 64 * 1024;

/// Errors that can occur when parsing payloads.
#[derive(Debug, Error)]
pub enum PayloadError {
    /// Payload is too short to contain header.
    #[error("payload too short: {0} bytes, need at least {HEADER_SIZE}")]
    TooShort(usize),

    /// Payload length field doesn't match actual length.
    #[error("length mismatch: header says {expected}, actual {actual}")]
    LengthMismatch {
        /// Expected length from header.
        expected: usize,
        /// Actual payload length.
        actual: usize,
    },

    /// Checksum verification failed.
    #[error("checksum mismatch: expected {expected:016x}, computed {computed:016x}")]
    ChecksumMismatch {
        /// Checksum from header.
        expected: u64,
        /// Computed checksum.
        computed: u64,
    },
}

/// A self-describing, verifiable message payload.
///
/// Layout (40 bytes header + variable data):
/// ```text
/// [0..8]   operation_id: u64    - Unique ID for correlation
/// [8..16]  sequence: u64        - Per-producer sequence number
/// [16..24] timestamp: u64       - Virtual/wall timestamp (microseconds)
/// [24..32] checksum: u64        - CRC64 of header (excluding checksum) + data
/// [32..40] data_len: u64        - Length of variable data
/// [40..]   data: [u8]           - Deterministic content based on seed
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiablePayload {
    /// Unique operation ID for correlation.
    pub operation_id: u64,
    /// Per-producer sequence number.
    pub sequence: u64,
    /// Timestamp in microseconds.
    pub timestamp: u64,
    /// CRC64 checksum.
    pub checksum: u64,
    /// Variable-length data.
    pub data: Bytes,
}

impl VerifiablePayload {
    /// Creates a new payload with deterministic content.
    ///
    /// # Arguments
    /// * `operation_id` - Unique operation ID.
    /// * `sequence` - Per-producer sequence number.
    /// * `timestamp` - Timestamp in microseconds.
    /// * `data_size` - Size of variable data to generate.
    /// * `rng` - Random number generator for deterministic data.
    #[must_use]
    pub fn new<R: Rng>(
        operation_id: u64,
        sequence: u64,
        timestamp: u64,
        data_size: usize,
        rng: &mut R,
    ) -> Self {
        // Generate deterministic data.
        let mut data = vec![0u8; data_size];
        rng.fill(&mut data[..]);
        let data = Bytes::from(data);

        // Compute checksum over all fields except checksum itself.
        let checksum = Self::compute_checksum(operation_id, sequence, timestamp, &data);

        Self {
            operation_id,
            sequence,
            timestamp,
            checksum,
            data,
        }
    }

    /// Creates a payload with specific data (for testing).
    #[must_use]
    pub fn with_data(operation_id: u64, sequence: u64, timestamp: u64, data: Bytes) -> Self {
        let checksum = Self::compute_checksum(operation_id, sequence, timestamp, &data);
        Self {
            operation_id,
            sequence,
            timestamp,
            checksum,
            data,
        }
    }

    /// Parses a payload from bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload is malformed or checksum fails.
    #[allow(clippy::cast_possible_truncation)] // data_len is validated against actual length.
    pub fn parse(bytes: &Bytes) -> Result<Self, PayloadError> {
        if bytes.len() < HEADER_SIZE {
            return Err(PayloadError::TooShort(bytes.len()));
        }

        let mut cursor = bytes.clone();

        let operation_id = cursor.get_u64();
        let sequence = cursor.get_u64();
        let timestamp = cursor.get_u64();
        let checksum = cursor.get_u64();
        let data_len = cursor.get_u64() as usize;

        // Verify length.
        let expected_len = HEADER_SIZE + data_len;
        if bytes.len() != expected_len {
            return Err(PayloadError::LengthMismatch {
                expected: expected_len,
                actual: bytes.len(),
            });
        }

        // Extract data.
        let data = bytes.slice(HEADER_SIZE..);

        // Verify checksum.
        let computed = Self::compute_checksum(operation_id, sequence, timestamp, &data);
        if checksum != computed {
            return Err(PayloadError::ChecksumMismatch {
                expected: checksum,
                computed,
            });
        }

        Ok(Self {
            operation_id,
            sequence,
            timestamp,
            checksum,
            data,
        })
    }

    /// Encodes the payload to bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + self.data.len());

        buf.put_u64(self.operation_id);
        buf.put_u64(self.sequence);
        buf.put_u64(self.timestamp);
        buf.put_u64(self.checksum);
        buf.put_u64(self.data.len() as u64);
        buf.put_slice(&self.data);

        buf.freeze()
    }

    /// Returns the total size of the encoded payload.
    #[must_use]
    pub const fn encoded_size(&self) -> usize {
        HEADER_SIZE + self.data.len()
    }

    /// Verifies the checksum is correct.
    #[must_use]
    pub fn verify_checksum(&self) -> bool {
        let computed = Self::compute_checksum(
            self.operation_id,
            self.sequence,
            self.timestamp,
            &self.data,
        );
        self.checksum == computed
    }

    /// Computes CRC64 checksum over the payload fields.
    fn compute_checksum(operation_id: u64, sequence: u64, timestamp: u64, data: &Bytes) -> u64 {
        use crc::{Crc, CRC_64_ECMA_182};

        const CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_ECMA_182);

        let mut digest = CRC64.digest();
        digest.update(&operation_id.to_be_bytes());
        digest.update(&sequence.to_be_bytes());
        digest.update(&timestamp.to_be_bytes());
        digest.update(data);
        digest.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    #[test]
    fn test_payload_roundtrip() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let payload = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng);

        let encoded = payload.encode();
        let decoded = VerifiablePayload::parse(&encoded).unwrap();

        assert_eq!(payload, decoded);
        assert!(decoded.verify_checksum());
    }

    #[test]
    fn test_payload_deterministic() {
        let mut rng1 = ChaCha8Rng::seed_from_u64(42);
        let mut rng2 = ChaCha8Rng::seed_from_u64(42);

        let payload1 = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng1);
        let payload2 = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng2);

        assert_eq!(payload1, payload2);
    }

    #[test]
    fn test_payload_different_seeds() {
        let mut rng1 = ChaCha8Rng::seed_from_u64(42);
        let mut rng2 = ChaCha8Rng::seed_from_u64(43);

        let payload1 = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng1);
        let payload2 = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng2);

        // Same metadata, different data.
        assert_eq!(payload1.operation_id, payload2.operation_id);
        assert_ne!(payload1.data, payload2.data);
        assert_ne!(payload1.checksum, payload2.checksum);
    }

    #[test]
    fn test_payload_too_short() {
        let bytes = Bytes::from_static(&[0; 10]);
        let result = VerifiablePayload::parse(&bytes);
        assert!(matches!(result, Err(PayloadError::TooShort(10))));
    }

    #[test]
    fn test_payload_length_mismatch() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let payload = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng);
        let mut encoded = payload.encode();

        // Truncate the data.
        let truncated = encoded.split_to(HEADER_SIZE + 32);
        let result = VerifiablePayload::parse(&truncated);
        assert!(matches!(result, Err(PayloadError::LengthMismatch { .. })));
    }

    #[test]
    fn test_payload_checksum_mismatch() {
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let payload = VerifiablePayload::new(1, 100, 1_000_000, 64, &mut rng);
        let encoded = payload.encode();

        // Corrupt a byte in the data section.
        let mut corrupted = BytesMut::from(&encoded[..]);
        corrupted[HEADER_SIZE + 10] ^= 0xFF;
        let corrupted = corrupted.freeze();

        let result = VerifiablePayload::parse(&corrupted);
        assert!(matches!(result, Err(PayloadError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_payload_empty_data() {
        let payload = VerifiablePayload::with_data(1, 100, 1_000_000, Bytes::new());
        assert_eq!(payload.encoded_size(), HEADER_SIZE);

        let encoded = payload.encode();
        let decoded = VerifiablePayload::parse(&encoded).unwrap();
        assert_eq!(payload, decoded);
    }
}

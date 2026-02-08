//! Producer info extraction from Kafka `RecordBatch` bytes.
//!
//! This module is separated from the main handler to make it available
//! under `MadSim` (where the TCP-based Kafka handler is excluded).

use bytes::Bytes;

/// Producer info from a Kafka `RecordBatch` header.
///
/// Kafka `RecordBatch` layout (relevant offsets):
/// - offset 43: `producerId` (i64)
/// - offset 51: `producerEpoch` (i16)
/// - offset 53: `baseSequence` (i32)
#[derive(Debug, Clone, Copy)]
pub struct ProducerInfo {
    /// Producer ID (-1 if non-idempotent).
    pub producer_id: i64,
    /// Producer epoch.
    pub epoch: i16,
    /// Base sequence number for this batch.
    pub base_sequence: i32,
}

impl ProducerInfo {
    /// Returns true if this is an idempotent produce (`producer_id` >= 0).
    #[must_use]
    pub const fn is_idempotent(&self) -> bool {
        self.producer_id >= 0
    }
}

/// Extract producer info from a Kafka `RecordBatch`.
///
/// Returns `None` if the batch is too short to contain producer info.
///
/// This function is public to allow the output processor to extract producer
/// state from committed entries (including `PREVIOUS_TERM` entries on new leaders).
#[must_use]
pub fn extract_producer_info(bytes: &Bytes) -> Option<ProducerInfo> {
    // Minimum size to contain all fields through baseSequence.
    const MIN_SIZE: usize = 57; // baseSequence ends at offset 57

    if bytes.len() < MIN_SIZE {
        return None;
    }

    // producerId at offset 43 (8 bytes, big-endian i64).
    let producer_id = i64::from_be_bytes([
        bytes[43], bytes[44], bytes[45], bytes[46], bytes[47], bytes[48], bytes[49], bytes[50],
    ]);

    // producerEpoch at offset 51 (2 bytes, big-endian i16).
    let epoch = i16::from_be_bytes([bytes[51], bytes[52]]);

    // baseSequence at offset 53 (4 bytes, big-endian i32).
    let base_sequence = i32::from_be_bytes([bytes[53], bytes[54], bytes[55], bytes[56]]);

    Some(ProducerInfo {
        producer_id,
        epoch,
        base_sequence,
    })
}

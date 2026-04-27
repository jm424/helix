//! Standalone helper functions for E2E tests.
//!
//! These have zero dependency on `E2ECluster` and are extracted first.

use bytes::{BufMut, Bytes, BytesMut};

/// Creates a minimal valid Kafka RecordBatch for testing.
///
/// This creates a properly formatted RecordBatch with the given payload as
/// a single record. The format must be valid for `count_records_in_batch`
/// to correctly parse the record count.
#[must_use]
pub fn create_test_record_batch(payload: &[u8]) -> Bytes {
    // Kafka RecordBatch v2 header format:
    // - baseOffset: 8 bytes (i64)
    // - batchLength: 4 bytes (i32) - length of everything after this field
    // - partitionLeaderEpoch: 4 bytes (i32)
    // - magic: 1 byte (2 for v2)
    // - crc: 4 bytes (i32) - we'll use 0 for testing
    // - attributes: 2 bytes (i16)
    // - lastOffsetDelta: 4 bytes (i32)
    // - firstTimestamp: 8 bytes (i64)
    // - maxTimestamp: 8 bytes (i64)
    // - producerId: 8 bytes (i64)
    // - producerEpoch: 2 bytes (i16)
    // - baseSequence: 4 bytes (i32)
    // - recordCount: 4 bytes (i32) - at offset 57

    let mut buf = BytesMut::with_capacity(128 + payload.len());

    // baseOffset: 0
    buf.put_i64(0);
    // batchLength: will be calculated (placeholder)
    let batch_length_pos = buf.len();
    buf.put_i32(0);
    // partitionLeaderEpoch: -1
    buf.put_i32(-1);
    // magic: 2
    buf.put_u8(2);
    // crc: 0 (not validated in our code)
    buf.put_i32(0);
    // attributes: 0
    buf.put_i16(0);
    // lastOffsetDelta: 0 (single record)
    buf.put_i32(0);
    // firstTimestamp: 0
    buf.put_i64(0);
    // maxTimestamp: 0
    buf.put_i64(0);
    // producerId: -1 (non-idempotent)
    buf.put_i64(-1);
    // producerEpoch: -1
    buf.put_i16(-1);
    // baseSequence: -1
    buf.put_i32(-1);
    // recordCount: 1
    buf.put_i32(1);

    // Record format (simplified):
    // - length: varint
    // - attributes: 1 byte
    // - timestampDelta: varint
    // - offsetDelta: varint
    // - keyLength: varint (-1 for null)
    // - key: bytes (if keyLength >= 0)
    // - valueLength: varint
    // - value: bytes
    // - headersCount: varint (0)

    // For simplicity, we'll just append the payload as a minimal record.
    // The record parsing isn't strictly validated in our tests.
    let record_start = buf.len();
    buf.put_u8(0); // attributes
    buf.put_u8(0); // timestampDelta varint (0)
    buf.put_u8(0); // offsetDelta varint (0)
    buf.put_u8(0xff); // keyLength varint (-1 = null key, using 0xff as signed varint)
                      // Safe cast: payload length is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    {
        buf.put_u8(payload.len() as u8); // valueLength varint
    }
    buf.put_slice(payload);
    buf.put_u8(0); // headersCount varint (0)

    // Calculate record length and prepend it.
    let record_len = buf.len() - record_start;
    let mut final_buf = BytesMut::with_capacity(buf.len() + 1);
    final_buf.put_slice(&buf[..record_start]);
    // Safe cast: record_len is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    final_buf.put_u8(record_len as u8); // record length varint
    final_buf.put_slice(&buf[record_start..]);

    // Update batchLength (everything after the batchLength field).
    // Safe cast: batch length is expected to be small for tests.
    #[allow(clippy::cast_possible_truncation)]
    let batch_length = (final_buf.len() - batch_length_pos - 4) as i32;
    final_buf[batch_length_pos..batch_length_pos + 4].copy_from_slice(&batch_length.to_be_bytes());

    final_buf.freeze()
}

/// Splits concatenated RecordBatches into individual batches.
///
/// Kafka RecordBatches are concatenated in fetch responses. This function
/// parses the batch headers to split them apart.
pub(crate) fn split_record_batches(data: &Bytes) -> Vec<Bytes> {
    let mut batches = Vec::new();
    let mut offset = 0;

    while offset + 12 <= data.len() {
        // Read batchLength at offset 8 (after 8-byte baseOffset).
        let batch_length_bytes: [u8; 4] = data[offset + 8..offset + 12]
            .try_into()
            .expect("slice should be 4 bytes");
        let batch_length = i32::from_be_bytes(batch_length_bytes);

        if batch_length <= 0 {
            break;
        }

        // Total batch size = 8 (baseOffset) + 4 (batchLength) + batchLength.
        // Safe cast: batch_length is positive.
        #[allow(clippy::cast_sign_loss)]
        let total_size = 8 + 4 + batch_length as usize;

        if offset + total_size > data.len() {
            // Incomplete batch at end - shouldn't happen in valid data.
            break;
        }

        batches.push(data.slice(offset..offset + total_size));
        offset += total_size;
    }

    batches
}


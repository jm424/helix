//! Read handlers for the Helix service.

use bytes::Bytes;
use helix_core::{Offset, PartitionId, Record};
use tracing::debug;

use crate::error::{ServerError, ServerResult};
use crate::generated::{ErrorCode, ReadRequest, ReadResponse, Record as ProtoRecord, RecordWithOffset};

use super::super::{HelixService, MAX_BYTES_PER_READ};

impl HelixService {
    /// Converts a core Record to a proto Record.
    pub(crate) fn record_to_proto(record: &Record) -> ProtoRecord {
        ProtoRecord {
            key: record.key.as_ref().map(|k| k.to_vec()),
            value: record.value.to_vec(),
            timestamp_ms: Some(record.timestamp.as_millis()),
            headers: record
                .headers
                .iter()
                .map(|h| (String::from_utf8_lossy(&h.key).to_string(), h.value.to_vec()))
                .collect(),
        }
    }

    /// Internal read implementation.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn read_internal(&self, request: ReadRequest) -> ServerResult<ReadResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let ps_lock = {
            let storage = self.partition_storage.read().await;
            storage.get(&group_id).cloned().ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            })?
        };
        let ps = ps_lock.read().await;

        // Check offset bounds.
        let log_start = ps.log_start_offset();
        let log_end = ps.log_end_offset();

        if request.offset < log_start.get() || request.offset > log_end.get() {
            return Err(ServerError::OffsetOutOfRange {
                offset: request.offset,
                start: log_start.get(),
                end: log_end.get(),
            });
        }

        // Read records.
        let max_records = if request.max_records == 0 {
            100
        } else {
            request.max_records
        };
        let max_bytes = if request.max_bytes == 0 {
            MAX_BYTES_PER_READ
        } else {
            request.max_bytes.min(MAX_BYTES_PER_READ)
        };

        let start_offset = Offset::new(request.offset);
        let records = ps.read(start_offset, max_records)?;

        // Convert to proto records with offsets.
        let mut proto_records = Vec::new();
        let mut bytes_read: u32 = 0;
        let mut current_offset = request.offset;

        for record in &records {
            // Estimate record size.
            // Safe cast: record value/key sizes are bounded by limits which fit in u32.
            #[allow(clippy::cast_possible_truncation)]
            let value_size = record.value.len() as u32;
            #[allow(clippy::cast_possible_truncation)]
            let key_size = record.key.as_ref().map_or(0, |k: &Bytes| k.len() as u32);
            let record_size = value_size + key_size + 16; // Overhead.

            if bytes_read + record_size > max_bytes && !proto_records.is_empty() {
                break;
            }

            proto_records.push(RecordWithOffset {
                offset: current_offset,
                record: Some(Self::record_to_proto(record)),
            });
            bytes_read += record_size;
            current_offset += 1;
        }

        let high_watermark = ps.high_watermark();

        debug!(
            topic = %request.topic,
            partition = request.partition,
            offset = request.offset,
            count = proto_records.len(),
            "Read records"
        );

        Ok(ReadResponse {
            records: proto_records,
            high_watermark: high_watermark.get(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }
}

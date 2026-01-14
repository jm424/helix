//! Consumer group handlers for the Helix service.

use bytes::Bytes;
use helix_core::{ConsumerGroupId, ConsumerId, LeaseId, Offset, PartitionId};
use helix_progress::{AckMode, PartitionKey};
use tracing::{debug, warn};

use crate::error::{ServerError, ServerResult};
use crate::generated::{
    AckMode as ProtoAckMode, AckRequest, AckResponse, CreateConsumerGroupRequest,
    CreateConsumerGroupResponse, ErrorCode, GetCommittedOffsetRequest, GetCommittedOffsetResponse,
    PullRequest, PullResponse, RecordWithOffset,
};

use super::super::{HelixService, MAX_BYTES_PER_READ};

impl HelixService {
    /// Creates a consumer group.
    pub(crate) async fn create_consumer_group_internal(
        &self,
        request: CreateConsumerGroupRequest,
    ) -> ServerResult<CreateConsumerGroupResponse> {
        let group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));

        // Convert proto ack mode to internal ack mode.
        let ack_mode = match ProtoAckMode::try_from(request.ack_mode).unwrap_or(ProtoAckMode::Cumulative) {
            ProtoAckMode::Cumulative => AckMode::Cumulative,
            ProtoAckMode::Individual => AckMode::Individual,
        };

        let current_time_us = Self::current_time_us();

        // Try to get or create the group with the specified ack mode.
        match self
            .progress_manager
            .get_or_create_group_with_mode(group_id, current_time_us, ack_mode)
            .await
        {
            Ok(_state) => Ok(CreateConsumerGroupResponse {
                created: true,
                error_code: ErrorCode::None.into(),
                error_message: None,
            }),
            Err(e) => Err(ServerError::Progress(e)),
        }
    }

    /// Pulls records for a consumer with lease-based delivery.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn pull_internal(&self, request: PullRequest) -> ServerResult<PullResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");
        assert!(
            !request.consumer_group_id.is_empty(),
            "consumer_group_id cannot be empty"
        );
        assert!(
            !request.consumer_id.is_empty(),
            "consumer_id cannot be empty"
        );

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
        let raft_group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));
        let consumer_id = ConsumerId::new(Self::hash_string(&request.consumer_id));

        let current_time_us = Self::current_time_us();

        // Register consumer if not already registered.
        self.progress_manager
            .register_consumer(consumer_group_id, consumer_id, current_time_us)
            .await
            .map_err(ServerError::Progress)?;

        // Determine the starting offset for the lease.
        // Query the committed offset to start from the next uncommitted offset.
        let from_offset = self
            .progress_manager
            .fetch_committed(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?
            .map_or(Offset::new(0), |o| Offset::new(o.get()));

        // Check how many records are available from storage.
        let log_end = {
            let storage = self.partition_storage.read().await;
            let ps = storage.get(&raft_group_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?;
            ps.log_end_offset()
        };

        // Limit max_records to what's actually available.
        let available_records = log_end.get().saturating_sub(from_offset.get());
        // Safe cast: available_records is bounded by partition size which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let available_records_u32 = available_records.min(u64::from(u32::MAX)) as u32;

        let max_records = if request.max_records == 0 {
            100.min(available_records_u32)
        } else {
            request.max_records.min(available_records_u32)
        };

        // Return empty if no records available.
        if max_records == 0 {
            return Ok(PullResponse {
                records: vec![],
                lease_id: 0,
                from_offset: from_offset.get(),
                to_offset: 0,
                lease_expires_at_us: 0,
                has_more: false,
                error_code: ErrorCode::None.into(),
                error_message: None,
            });
        }

        let lease_duration_us = if request.lease_duration_ms == 0 {
            60_000_000 // Default: 1 minute.
        } else {
            u64::from(request.lease_duration_ms) * 1000
        };

        // Lease offsets.
        let lease = self
            .progress_manager
            .lease_offsets(
                consumer_group_id,
                topic_meta.topic_id,
                partition_id,
                consumer_id,
                from_offset,
                max_records,
                lease_duration_us,
                current_time_us,
            )
            .await
            .map_err(ServerError::Progress)?;

        // If no lease was granted (no offsets available), return empty response.
        let Some(lease) = lease else {
            return Ok(PullResponse {
                records: vec![],
                lease_id: 0,
                from_offset: 0,
                to_offset: 0,
                lease_expires_at_us: 0,
                has_more: false,
                error_code: ErrorCode::None.into(),
                error_message: None,
            });
        };

        // Read the actual records from storage.
        let storage = self.partition_storage.read().await;
        let ps = storage.get(&raft_group_id).ok_or_else(|| {
            ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            }
        })?;

        let log_end = ps.log_end_offset();
        let lease_to_offset = lease.to_offset.get().min(log_end.get().saturating_sub(1));

        // Calculate how many records we can actually read.
        let record_count = lease_to_offset.saturating_sub(lease.from_offset.get()) + 1;
        // Safe cast: record_count fits in u32 because it's bounded by max_records.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u32 = record_count.min(u64::from(max_records)) as u32;

        let records = ps.read(lease.from_offset, record_count_u32)?;

        // Convert to proto records with offsets.
        let max_bytes = if request.max_bytes == 0 {
            MAX_BYTES_PER_READ
        } else {
            request.max_bytes.min(MAX_BYTES_PER_READ)
        };

        let mut proto_records = Vec::new();
        let mut bytes_read: u32 = 0;
        let mut current_offset = lease.from_offset.get();

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

        let has_more = current_offset <= lease_to_offset || log_end.get() > lease_to_offset + 1;

        debug!(
            topic = %request.topic,
            partition = request.partition,
            consumer_group = %request.consumer_group_id,
            lease_id = lease.lease_id.get(),
            count = proto_records.len(),
            "Pull records"
        );

        Ok(PullResponse {
            records: proto_records,
            lease_id: lease.lease_id.get(),
            from_offset: lease.from_offset.get(),
            to_offset: lease_to_offset,
            lease_expires_at_us: lease.expires_at_us,
            has_more,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Acknowledges, nacks, or extends leased records.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn ack_internal(&self, request: AckRequest) -> ServerResult<AckResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");
        assert!(
            !request.consumer_group_id.is_empty(),
            "consumer_group_id cannot be empty"
        );
        assert!(
            !request.consumer_id.is_empty(),
            "consumer_id cannot be empty"
        );

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

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));
        let consumer_id = ConsumerId::new(Self::hash_string(&request.consumer_id));

        let current_time_us = Self::current_time_us();

        let mut acked_count: u32 = 0;
        let mut nacked_count: u32 = 0;
        let mut extended_count: u32 = 0;

        // Process acks: commit the lease's offsets.
        for lease_id in &request.ack_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .commit_lease(
                    consumer_group_id,
                    topic_meta.topic_id,
                    partition_id,
                    consumer_id,
                    lease_id,
                    current_time_us,
                )
                .await
            {
                Ok(()) => acked_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to commit lease");
                }
            }
        }

        // Process nacks: release the lease for redelivery.
        let partition_key = PartitionKey::new(topic_meta.topic_id, partition_id);
        for lease_id in &request.nack_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .release_lease(consumer_group_id, partition_key, consumer_id, lease_id)
                .await
            {
                Ok(()) => nacked_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to release lease");
                }
            }
        }

        // Process extends: renew the lease duration.
        let extend_duration_us = if request.extend_duration_ms == 0 {
            60_000_000 // Default: 1 minute.
        } else {
            u64::from(request.extend_duration_ms) * 1000
        };

        for lease_id in &request.extend_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .extend_lease(
                    consumer_group_id,
                    partition_key,
                    consumer_id,
                    lease_id,
                    extend_duration_us,
                    current_time_us,
                )
                .await
            {
                Ok(_) => extended_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to extend lease");
                }
            }
        }

        // Get the current low watermark.
        let low_watermark = self
            .progress_manager
            .get_low_watermark(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?
            .map_or(0, Offset::get);

        debug!(
            topic = %request.topic,
            partition = request.partition,
            consumer_group = %request.consumer_group_id,
            acked = acked_count,
            nacked = nacked_count,
            extended = extended_count,
            low_watermark,
            "Ack processed"
        );

        Ok(AckResponse {
            acked_count,
            nacked_count,
            extended_count,
            low_watermark,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Gets the committed offset for a consumer group.
    pub(crate) async fn get_committed_offset_internal(
        &self,
        request: GetCommittedOffsetRequest,
    ) -> ServerResult<GetCommittedOffsetResponse> {
        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition < 0 || request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));

        let low_watermark = self
            .progress_manager
            .get_low_watermark(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?;

        Ok(GetCommittedOffsetResponse {
            low_watermark: low_watermark.map_or(0, Offset::get),
            has_committed: low_watermark.is_some(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }
}

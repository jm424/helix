//! Partition storage wrapper.
//!
//! This module provides a unified interface for partition storage,
//! supporting both in-memory (for testing) and durable WAL-backed storage.

use std::path::PathBuf;

use bytes::Bytes;
use helix_core::{LogIndex, Offset, PartitionId, ProducerEpoch, ProducerId, Record, SequenceNum, TopicId};
use helix_wal::{SharedEntry, SharedWalHandle, Storage, TokioStorage};
use tracing::{debug, warn};

use crate::error::{ServerError, ServerResult};
use crate::producer_state::{PartitionProducerState, SequenceCheckResult};
use crate::storage::{
    BlobFormat, DurablePartition, DurablePartitionConfig, DurablePartitionError, Partition,
    PartitionCommand, PartitionConfig, patch_kafka_base_offset,
};
use helix_tier::TieringConfig;
#[cfg(feature = "s3")]
use helix_tier::S3Config;

/// Inner storage type for a partition.
///
/// # Type Parameters
///
/// * `S` - Storage backend (e.g., `TokioStorage` for production, `SimulatedStorage` for DST)
pub enum PartitionStorageInner<S: Storage + Clone + Send + Sync + 'static> {
    /// In-memory storage (for testing).
    InMemory(Partition),
    /// Durable WAL-backed storage (for production).
    /// Boxed to reduce enum size difference between variants.
    Durable(Box<DurablePartition<S>>),
}

/// Storage for a single partition.
///
/// # Type Parameters
///
/// * `S` - Storage backend (e.g., `TokioStorage` for production, `SimulatedStorage` for DST)
pub struct PartitionStorage<S: Storage + Clone + Send + Sync + 'static> {
    /// Topic ID.
    #[allow(dead_code)]
    topic_id: TopicId,
    /// Partition ID.
    #[allow(dead_code)]
    partition_id: PartitionId,
    /// The underlying partition storage.
    pub(crate) inner: PartitionStorageInner<S>,
    /// Last applied Raft log index.
    last_applied: LogIndex,
    /// Producer state for idempotent deduplication.
    #[allow(dead_code)] // Used in append_blob integration (coming soon).
    producer_state: PartitionProducerState,
}

/// Type alias for production partition storage using Tokio filesystem.
pub type ServerPartitionStorage = PartitionStorage<TokioStorage>;

impl<S: Storage + Clone + Send + Sync + 'static> PartitionStorage<S> {
    /// Creates new in-memory partition storage.
    #[must_use] 
    pub fn new_in_memory(topic_id: TopicId, partition_id: PartitionId) -> Self {
        let config = PartitionConfig::new(topic_id, partition_id);
        Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::InMemory(Partition::new(config)),
            last_applied: LogIndex::new(0),
            producer_state: PartitionProducerState::new(),
        }
    }

    /// Creates new durable partition storage with the given storage backend.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend to use for WAL operations
    /// * `data_dir` - Base directory for partition data
    /// * `object_storage_dir` - Optional directory for object storage (tiering)
    /// * `s3_config` - Optional S3 configuration for tiering (requires `s3` feature)
    /// * `tiering_config` - Optional tiering configuration (enables tiering when set)
    /// * `topic_id` - Topic identifier
    /// * `partition_id` - Partition identifier
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened.
    #[cfg(feature = "s3")]
    pub async fn new_durable(
        storage: S,
        data_dir: &PathBuf,
        object_storage_dir: Option<&PathBuf>,
        s3_config: Option<&S3Config>,
        tiering_config: Option<&TieringConfig>,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Result<Self, DurablePartitionError> {
        let mut config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        if let Some(s3_cfg) = s3_config {
            config = config.with_s3_config(s3_cfg.clone());
        } else if let Some(object_dir) = object_storage_dir {
            config = config.with_object_storage_dir(object_dir);
        }
        // Enable tiering when config is provided.
        if let Some(tier_cfg) = tiering_config {
            config = config.with_tiering(tier_cfg.clone());
        }
        let durable = DurablePartition::open(storage, config).await?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Creates new durable partition storage with the given storage backend.
    ///
    /// # Arguments
    ///
    /// * `storage` - Storage backend to use for WAL operations
    /// * `data_dir` - Base directory for partition data
    /// * `object_storage_dir` - Optional directory for object storage (tiering)
    /// * `tiering_config` - Optional tiering configuration (enables tiering when set)
    /// * `topic_id` - Topic identifier
    /// * `partition_id` - Partition identifier
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened.
    #[cfg(not(feature = "s3"))]
    pub async fn new_durable(
        storage: S,
        data_dir: &PathBuf,
        object_storage_dir: Option<&PathBuf>,
        tiering_config: Option<&TieringConfig>,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Result<Self, DurablePartitionError> {
        let mut config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        if let Some(object_dir) = object_storage_dir {
            config = config.with_object_storage_dir(object_dir);
        }
        // Enable tiering when config is provided.
        if let Some(tier_cfg) = tiering_config {
            config = config.with_tiering(tier_cfg.clone());
        }
        let durable = DurablePartition::open(storage, config).await?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Creates new durable partition storage with a shared WAL handle.
    ///
    /// This is used when partitions share a WAL pool for fsync amortization.
    ///
    /// # Arguments
    ///
    /// * `data_dir` - Base directory for partition data
    /// * `topic_id` - Topic identifier
    /// * `partition_id` - Partition identifier
    /// * `wal_handle` - Shared WAL handle from the pool
    /// * `recovered_entries` - Entries recovered from the shared WAL for this partition
    /// * `object_storage_dir` - Optional directory for object storage (tiering)
    /// * `s3_config` - Optional S3 configuration for tiering (requires `s3` feature)
    /// * `tiering_config` - Optional tiering configuration
    ///
    /// # Errors
    /// Returns an error if the partition cannot be opened.
    #[cfg(feature = "s3")]
    #[allow(clippy::too_many_arguments)]
    pub async fn new_durable_with_shared_wal(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
        wal_handle: SharedWalHandle<S>,
        recovered_entries: Vec<SharedEntry>,
        object_storage_dir: Option<&PathBuf>,
        s3_config: Option<&S3Config>,
        tiering_config: Option<&TieringConfig>,
    ) -> Result<Self, DurablePartitionError> {
        let mut config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        if let Some(s3_cfg) = s3_config {
            config = config.with_s3_config(s3_cfg.clone());
        } else if let Some(object_dir) = object_storage_dir {
            config = config.with_object_storage_dir(object_dir);
        }
        if let Some(tier_cfg) = tiering_config {
            config = config.with_tiering(tier_cfg.clone());
        }
        let durable = DurablePartition::open_with_shared_wal(config, wal_handle, recovered_entries).await?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Creates new durable partition storage with a shared WAL handle.
    ///
    /// This is used when partitions share a WAL pool for fsync amortization.
    ///
    /// # Errors
    /// Returns an error if the partition cannot be opened.
    #[cfg(not(feature = "s3"))]
    #[allow(clippy::too_many_arguments)]
    pub async fn new_durable_with_shared_wal(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
        wal_handle: SharedWalHandle<S>,
        recovered_entries: Vec<SharedEntry>,
        object_storage_dir: Option<&PathBuf>,
        tiering_config: Option<&TieringConfig>,
    ) -> Result<Self, DurablePartitionError> {
        let mut config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        if let Some(object_dir) = object_storage_dir {
            config = config.with_object_storage_dir(object_dir);
        }
        if let Some(tier_cfg) = tiering_config {
            config = config.with_tiering(tier_cfg.clone());
        }
        let durable = DurablePartition::open_with_shared_wal(config, wal_handle, recovered_entries).await?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Returns the log start offset.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    #[must_use] 
    pub fn log_start_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_start_offset(),
            PartitionStorageInner::Durable(p) => p.log_start_offset(),
        }
    }

    /// Returns the log end offset.
    #[must_use] 
    pub fn log_end_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_end_offset(),
            PartitionStorageInner::Durable(p) => p.log_end_offset(),
        }
    }

    /// Returns the high watermark.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    #[must_use] 
    pub fn high_watermark(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.high_watermark(),
            PartitionStorageInner::Durable(p) => p.high_watermark(),
        }
    }

    /// Returns the blob log end offset (for Kafka batch storage).
    #[must_use] 
    pub fn blob_log_end_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.blob_log_end_offset(),
            PartitionStorageInner::Durable(p) => p.blob_log_end_offset(),
        }
    }

    /// Returns the last applied Raft log index.
    #[allow(dead_code)]
    #[must_use] 
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Checks if a produce request is a duplicate.
    ///
    /// This should be called BEFORE proposing to Raft to prevent duplicate log entries.
    ///
    /// # Returns
    ///
    /// - `Valid` if this is a new request that should be proposed
    /// - `Duplicate { cached_offset }` if this is a retry of an already-committed batch
    /// - `OutOfSequence` if there's a gap in sequence numbers
    /// - `ProducerFenced` if the epoch is stale
    #[allow(dead_code)] // Will be used in append_blob integration.
    #[must_use] 
    pub fn check_producer_sequence(
        &self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        sequence: SequenceNum,
    ) -> SequenceCheckResult {
        self.producer_state.check_sequence(producer_id, epoch, sequence)
    }

    /// Records a successful produce after Raft commit.
    ///
    /// Updates the producer state with the new sequence number and offset.
    /// This should be called after the batch has been committed to Raft.
    #[allow(dead_code)] // Will be used in append_blob integration.
    pub fn record_producer_sequence(
        &mut self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        sequence: SequenceNum,
        base_offset: Offset,
    ) {
        // Use current timestamp (microseconds since start would be better,
        // but for simplicity we use a monotonic counter based on offset).
        let timestamp_us = base_offset.get();
        self.producer_state.record_produce(
            producer_id,
            epoch,
            sequence,
            base_offset,
            timestamp_us,
        );
    }

    /// Reads records from the partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails (e.g., internal storage error).
    pub fn read(&self, start_offset: Offset, max_records: u32) -> ServerResult<Vec<Record>> {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.read(start_offset, max_records).map_err(|e| {
                ServerError::Internal {
                    message: format!("failed to read: {e}"),
                }
            }),
            PartitionStorageInner::Durable(p) => p.read(start_offset, max_records).map_err(|e| {
                ServerError::Internal {
                    message: format!("failed to read: {e}"),
                }
            }),
        }
    }

    /// Applies a committed entry to the partition (sync version for in-memory).
    ///
    /// # Errors
    ///
    /// Returns an error if the entry cannot be decoded or applied.
    #[allow(dead_code)] // Kept for potential future use; async version used everywhere.
    #[allow(clippy::too_many_lines)]
    pub fn apply_entry_sync(&mut self, index: LogIndex, data: &Bytes) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                last_applied = %self.last_applied,
                "skipping already-applied entry"
            );
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                "skipping empty entry (no-op)"
            );
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records, .. } => {
                    let _record_count = records.len();
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    // Update high watermark since entry is committed.
                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format, .. } => {
                    // Get the offset that will be assigned before appending.
                    let base_offset = partition.blob_log_end_offset();

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    let offset = partition.append_blob(blob_to_store, record_count).map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append blob: {e}"),
                        }
                    })?;

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        record_count = record_count,
                        base_offset = %offset,
                        new_hwm = %new_hwm,
                        format = ?format,
                        "applied blob"
                    );
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        from_offset = %from_offset,
                        "applied truncate"
                    );
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        high_watermark = %high_watermark,
                        "applied high watermark update"
                    );
                    partition.set_high_watermark(high_watermark);
                    None
                }
                PartitionCommand::AppendBlobBatch { blobs } => {
                    // Get the first offset that will be assigned.
                    let first_base_offset = partition.blob_log_end_offset();

                    // Apply each blob in sequence.
                    for batched in blobs {
                        let current_offset = partition.blob_log_end_offset();
                        let blob_to_store = match batched.format {
                            BlobFormat::Raw => batched.blob,
                            BlobFormat::KafkaRecordBatch => {
                                patch_kafka_base_offset(batched.blob, current_offset)
                            }
                        };
                        partition.append_blob(blob_to_store, batched.record_count).map_err(|e| {
                            ServerError::Internal {
                                message: format!("failed to append blob in batch: {e}"),
                            }
                        })?;
                    }

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        first_base_offset = %first_base_offset,
                        new_hwm = %new_hwm,
                        "applied blob batch (sync)"
                    );
                    Some(first_base_offset)
                }
            },
            PartitionStorageInner::Durable(_) => {
                // Durable storage should use apply_entry_async.
                return Err(ServerError::Internal {
                    message: "durable storage requires async apply".to_string(),
                });
            }
        };

        self.last_applied = index;
        Ok(base_offset)
    }

    /// Applies a committed entry to the partition (async version for durable).
    ///
    /// # Errors
    ///
    /// Returns an error if the entry cannot be decoded or applied.
    #[tracing::instrument(skip_all, name = "apply_entry", fields(index = index.get()))]
    #[allow(clippy::too_many_lines)]
    pub async fn apply_entry_async(
        &mut self,
        index: LogIndex,
        data: &Bytes,
    ) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                last_applied = %self.last_applied,
                "skipping already-applied entry"
            );
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                "skipping empty entry (no-op)"
            );
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records } => {
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format } => {
                    let base_offset = partition.blob_log_end_offset();

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    let offset = partition.append_blob(blob_to_store, record_count).map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append blob: {e}"),
                        }
                    })?;

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        record_count = record_count,
                        base_offset = %offset,
                        new_hwm = %new_hwm,
                        format = ?format,
                        "applied blob"
                    );
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        from_offset = %from_offset,
                        "applied truncate"
                    );
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        high_watermark = %high_watermark,
                        "applied high watermark update"
                    );
                    partition.set_high_watermark(high_watermark);
                    None
                }
                PartitionCommand::AppendBlobBatch { blobs } => {
                    // Get the first offset that will be assigned.
                    let first_base_offset = partition.blob_log_end_offset();

                    // Apply each blob in sequence.
                    for batched in blobs {
                        let current_offset = partition.blob_log_end_offset();
                        let blob_to_store = match batched.format {
                            BlobFormat::Raw => batched.blob,
                            BlobFormat::KafkaRecordBatch => {
                                patch_kafka_base_offset(batched.blob, current_offset)
                            }
                        };
                        partition.append_blob(blob_to_store, batched.record_count).map_err(|e| {
                            ServerError::Internal {
                                message: format!("failed to append blob in batch: {e}"),
                            }
                        })?;
                    }

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        first_base_offset = %first_base_offset,
                        new_hwm = %new_hwm,
                        "applied blob batch"
                    );
                    Some(first_base_offset)
                }
            },
            PartitionStorageInner::Durable(partition) => match command {
                PartitionCommand::Append { records, .. } => {
                    // Use append_at_index with the Raft index to ensure proper
                    // idempotency tracking after crash recovery.
                    let offset = partition.append_at_index(index.get(), records).await.map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append: {e}"),
                        }
                    })?;
                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format, .. } => {
                    // Get the offset that will be assigned before appending.
                    let base_offset = partition.blob_log_end_offset();

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    // Use append_blob_at_index with the Raft index.
                    let offset = partition.append_blob_at_index(index.get(), blob_to_store, record_count).await.map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append blob: {e}"),
                        }
                    })?;
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset: _ } => {
                    // TODO: Implement truncate for durable partition.
                    warn!("Truncate not yet implemented for durable partition");
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    partition.set_high_watermark(high_watermark);
                    None
                }
                PartitionCommand::AppendBlobBatch { blobs } => {
                    // Get the first offset that will be assigned.
                    let first_base_offset = partition.blob_log_end_offset();

                    // Note: For blob batches, we use the single index for all blobs.
                    // This is semantically correct since they're all part of the same
                    // Raft entry and will all be recovered together.
                    for batched in blobs {
                        let current_offset = partition.blob_log_end_offset();
                        let blob_to_store = match batched.format {
                            BlobFormat::Raw => batched.blob,
                            BlobFormat::KafkaRecordBatch => {
                                patch_kafka_base_offset(batched.blob, current_offset)
                            }
                        };
                        partition.append_blob_at_index(index.get(), blob_to_store, batched.record_count).await.map_err(|e| {
                            ServerError::Internal {
                                message: format!("failed to append blob in batch: {e}"),
                            }
                        })?;
                    }

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        first_base_offset = %first_base_offset,
                        new_log_end = %partition.blob_log_end_offset(),
                        "applied blob batch (durable)"
                    );
                    Some(first_base_offset)
                }
            },
        };

        self.last_applied = index;
        Ok(base_offset)
    }

    // -------------------------------------------------------------------------
    // Tiering Methods
    // -------------------------------------------------------------------------

    /// Returns whether tiering is enabled for this partition.
    #[must_use]
    pub fn has_tiering(&self) -> bool {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => false,
            PartitionStorageInner::Durable(p) => p.has_tiering(),
        }
    }

    /// Checks for newly sealed segments and registers them with the tiering manager.
    ///
    /// This should be called periodically or after writes that may have caused
    /// segment rotation.
    ///
    /// # Errors
    ///
    /// Returns an error if registration with the tiering manager fails.
    pub async fn check_and_register_sealed_segments(&mut self) -> Result<u32, ServerError> {
        match &mut self.inner {
            PartitionStorageInner::InMemory(_) => Ok(0),
            PartitionStorageInner::Durable(p) => {
                p.check_and_register_sealed_segments()
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("tiering registration failed: {e}"),
                    })
            }
        }
    }

    /// Marks segments as committed when entries are committed via Raft.
    ///
    /// Segments containing only committed entries become eligible for tiering.
    ///
    /// # Arguments
    ///
    /// * `committed_index` - The Raft log index that has been committed
    ///
    /// # Errors
    ///
    /// Returns an error if marking segments as committed fails.
    pub async fn on_entries_committed(&self, committed_index: u64) -> Result<u32, ServerError> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => Ok(0),
            PartitionStorageInner::Durable(p) => {
                p.on_entries_committed(committed_index)
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("tiering commit notification failed: {e}"),
                    })
            }
        }
    }

    /// Uploads eligible segments to object storage.
    ///
    /// This should be called periodically to tier segments that meet the
    /// eligibility criteria (committed, sealed, and past minimum age).
    ///
    /// # Errors
    ///
    /// Returns an error if the tiering operation fails.
    pub async fn tier_eligible_segments(&self) -> Result<u32, ServerError> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => Ok(0),
            PartitionStorageInner::Durable(p) => {
                p.tier_eligible_segments()
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("tiering upload failed: {e}"),
                    })
            }
        }
    }
}

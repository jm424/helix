//! Partition storage wrapper.
//!
//! This module provides a unified interface for partition storage,
//! supporting both in-memory (for testing) and durable WAL-backed storage.

use std::path::PathBuf;

use bytes::Bytes;
use helix_core::{
    LogIndex, Offset, PartitionId, ProducerEpoch, ProducerId, Record, SequenceNum, TopicId,
};
use helix_wal::{SharedEntry, SharedWalHandle, Storage, TokioStorage};
use tracing::{debug, info};

/// Extract a preview of the payload from a Kafka `RecordBatch` for debugging.
/// Returns the first ~30 bytes of the record value as a string.
fn extract_payload_preview(blob: &Bytes) -> String {
    // Kafka RecordBatch structure:
    // - 8 bytes: base offset
    // - 4 bytes: batch length
    // - ... headers ...
    // - Records start after ~57 bytes from start
    // Each record has: length (varint), attributes, timestamp delta, offset delta, key, value
    // This is a rough extraction - just grab ASCII bytes from the end of the blob.
    if blob.len() < 60 {
        return format!("<blob len={}>", blob.len());
    }

    // Scan backwards from end looking for ASCII text (the payload).
    let mut preview = Vec::new();
    for &b in blob.iter().rev().take(50) {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            preview.push(b);
        } else if !preview.is_empty() {
            break;
        }
    }
    preview.reverse();

    if preview.is_empty() {
        format!("<no ascii, len={}>", blob.len())
    } else {
        String::from_utf8_lossy(&preview).to_string()
    }
}

use crate::error::{ServerError, ServerResult};
use crate::producer_state::{PartitionProducerState, SequenceCheckResult};
use crate::storage::{
    patch_kafka_base_offset, BlobFormat, DurablePartition,
    DurablePartitionConfig, DurablePartitionError, Partition, PartitionCommand, PartitionConfig,
    PartitionRecoveryState,
};
#[cfg(feature = "s3")]
use helix_tier::S3Config;
use helix_tier::TieringConfig;

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
    /// Term of the last applied Raft log entry.
    last_applied_term: helix_core::TermId,
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
            last_applied_term: helix_core::TermId::new(0),
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
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
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
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
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
    pub fn new_durable_with_shared_wal(
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
        let durable =
            DurablePartition::open_with_shared_wal(config, wal_handle, recovered_entries)?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
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
    pub fn new_durable_with_shared_wal(
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
        let durable =
            DurablePartition::open_with_shared_wal(config, wal_handle, recovered_entries)?;
        // Initialize last_applied from recovered WAL state for proper idempotency.
        let last_applied = LogIndex::new(durable.last_applied_index());
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Creates new durable partition storage from pre-built streaming recovery state.
    ///
    /// This is the memory-efficient path: instead of accumulating all recovered
    /// `SharedEntry` objects, the caller built `state` incrementally via the
    /// streaming recovery callback. O(one-segment) peak memory during recovery.
    ///
    /// # Errors
    /// Returns an error if the partition cannot be opened.
    #[cfg(feature = "s3")]
    #[allow(clippy::too_many_arguments)]
    pub fn new_durable_with_shared_wal_state(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
        wal_handle: SharedWalHandle<S>,
        state: PartitionRecoveryState,
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
        let durable = DurablePartition::open_with_recovery_state(config, wal_handle, state)?;
        let last_applied = LogIndex::new(durable.last_applied_index());
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
            producer_state: PartitionProducerState::new(),
        })
    }

    /// Creates new durable partition storage from pre-built streaming recovery state.
    ///
    /// This is the memory-efficient path: instead of accumulating all recovered
    /// `SharedEntry` objects, the caller built `state` incrementally via the
    /// streaming recovery callback. O(one-segment) peak memory during recovery.
    ///
    /// # Errors
    /// Returns an error if the partition cannot be opened.
    #[cfg(not(feature = "s3"))]
    pub fn new_durable_with_shared_wal_state(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
        wal_handle: SharedWalHandle<S>,
        state: PartitionRecoveryState,
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
        let durable = DurablePartition::open_with_recovery_state(config, wal_handle, state)?;
        let last_applied = LogIndex::new(durable.last_applied_index());
        let last_applied_term = helix_core::TermId::new(durable.last_applied_term());
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied,
            last_applied_term,
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
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Returns the term of the last applied Raft log entry.
    #[must_use]
    pub const fn last_applied_term(&self) -> helix_core::TermId {
        self.last_applied_term
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
        &mut self,
        producer_id: ProducerId,
        epoch: ProducerEpoch,
        sequence: SequenceNum,
    ) -> SequenceCheckResult {
        self.producer_state
            .check_sequence(producer_id, epoch, sequence)
    }

    /// Records a successful produce after Raft commit.
    ///
    /// Updates the producer state with the new sequence number and offset.
    /// This should be called after the batch has been committed to Raft.
    ///
    /// This is called from:
    /// - blob.rs handlers after batch commit (leader path)
    /// - `output_processor.rs` when applying committed entries (`follower/PREVIOUS_TERM` path)
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
        self.producer_state
            .record_produce(producer_id, epoch, sequence, base_offset, timestamp_us);
    }

    /// Reads records from the partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails (e.g., internal storage error).
    pub fn read(&self, start_offset: Offset, max_records: u32) -> ServerResult<Vec<Record>> {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => {
                p.read(start_offset, max_records)
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to read: {e}"),
                    })
            }
            PartitionStorageInner::Durable(p) => {
                p.read(start_offset, max_records)
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to read: {e}"),
                    })
            }
        }
    }

    /// Applies a committed entry to the partition (sync version for in-memory).
    ///
    /// # Errors
    ///
    /// Returns an error if the entry cannot be decoded or applied.
    #[allow(dead_code)] // Kept for potential future use; async version used everywhere.
    #[allow(clippy::too_many_lines)]
    pub fn apply_entry_sync(
        &mut self,
        index: LogIndex,
        term: helix_core::TermId,
        metadata: &Bytes,
        payload: &Bytes,
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

        // Skip empty entries (e.g., Raft no-op entries).
        if metadata.is_empty() {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                "skipping empty entry (no-op)"
            );
            self.last_applied = index;
            return Ok(None);
        }

        let command =
            PartitionCommand::decode_split(metadata.clone(), payload.clone())
                .ok_or_else(|| ServerError::Internal {
                    message: "failed to decode partition command".to_string(),
                })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records, .. } => {
                    let _record_count = records.len();
                    let offset = partition
                        .append(records)
                        .map_err(|e| ServerError::Internal {
                            message: format!("failed to append: {e}"),
                        })?;

                    // Update high watermark since entry is committed.
                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::AppendBlob {
                    blob,
                    record_count,
                    format,
                    base_offset,
                } => {
                    // Use base_offset from command - assigned by leader at propose time.
                    // This ensures all replicas use the same offset for consistency.

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    partition
                        .append_blob(blob_to_store, record_count)
                        .map_err(|e| ServerError::Internal {
                            message: format!("failed to append blob: {e}"),
                        })?;

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        record_count = record_count,
                        base_offset = %base_offset,
                        new_hwm = %new_hwm,
                        format = ?format,
                        "applied blob"
                    );
                    Some(base_offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        from_offset = %from_offset,
                        "applied truncate"
                    );
                    partition
                        .truncate(from_offset)
                        .map_err(|e| ServerError::Internal {
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
                PartitionCommand::AppendBlobBatch { blobs, base_offset } => {
                    // Use base_offset from command - assigned by leader at propose time.
                    // This ensures all replicas use the same offset for consistency.

                    // Apply each blob in sequence, computing current offset from base.
                    let mut current_offset = base_offset;
                    for batched in blobs {
                        let blob_to_store = match batched.format {
                            BlobFormat::Raw => batched.blob,
                            BlobFormat::KafkaRecordBatch => {
                                patch_kafka_base_offset(batched.blob, current_offset)
                            }
                        };
                        partition
                            .append_blob(blob_to_store, batched.record_count)
                            .map_err(|e| ServerError::Internal {
                                message: format!("failed to append blob in batch: {e}"),
                            })?;
                        // Advance offset for next blob in batch.
                        current_offset =
                            Offset::new(current_offset.get() + u64::from(batched.record_count));
                    }

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        base_offset = %base_offset,
                        new_hwm = %new_hwm,
                        "applied blob batch (sync)"
                    );
                    Some(base_offset)
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
        self.last_applied_term = term;
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
        term: helix_core::TermId,
        metadata: &Bytes,
        payload: &Bytes,
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

        // Skip empty entries (e.g., Raft no-op entries).
        if metadata.is_empty() {
            debug!(
                topic = %self.topic_id,
                partition = %self.partition_id,
                index = %index,
                "skipping empty entry (no-op)"
            );
            self.last_applied = index;
            return Ok(None);
        }

        let command =
            PartitionCommand::decode_split(metadata.clone(), payload.clone())
                .ok_or_else(|| ServerError::Internal {
                    message: "failed to decode partition command".to_string(),
            })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records, .. } => {
                    let offset = partition
                        .append(records)
                        .map_err(|e| ServerError::Internal {
                            message: format!("failed to append: {e}"),
                        })?;

                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::AppendBlob {
                    blob,
                    record_count,
                    format,
                    base_offset,
                } => {
                    // Use base_offset from command - assigned by leader at propose time.

                    // Extract payload preview for debugging offset bugs.
                    let payload_preview = extract_payload_preview(&blob);

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    // Use append_blob_at_offset to store at the leader-assigned offset.
                    // This ensures all replicas use the same offset, even with concurrent
                    // PREVIOUS_TERM entry processing.
                    partition
                        .append_blob_at_offset(blob_to_store, record_count, base_offset)
                        .map_err(|e| ServerError::Internal {
                            message: format!("failed to append blob: {e}"),
                        })?;

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        record_count = record_count,
                        base_offset = %base_offset,
                        new_hwm = %new_hwm,
                        payload_preview = %payload_preview,
                        "APPLY_BLOB: stored at offset"
                    );
                    Some(base_offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        from_offset = %from_offset,
                        "applied truncate"
                    );
                    partition
                        .truncate(from_offset)
                        .map_err(|e| ServerError::Internal {
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
                PartitionCommand::AppendBlobBatch { blobs, base_offset } => {
                    // Use base_offset from command - assigned by leader at propose time.
                    // This ensures all replicas use the same offset for consistency.

                    // Apply each blob in sequence starting from leader-assigned offset.
                    // Iterate by value to avoid cloning blob data.
                    let mut current_offset = base_offset;
                    for batched in blobs {
                        let payload_preview =
                            extract_payload_preview(&batched.blob);
                        info!(
                            topic = %self.topic_id,
                            partition = %self.partition_id,
                            index = %index,
                            offset = %current_offset,
                            payload = %payload_preview,
                            "APPLY_BATCH: storing blob"
                        );
                        // Move blob directly for Raw; patch_kafka takes
                        // ownership and may reuse the buffer in-place.
                        let blob_to_store = match batched.format {
                            BlobFormat::Raw => batched.blob,
                            BlobFormat::KafkaRecordBatch => {
                                patch_kafka_base_offset(
                                    batched.blob,
                                    current_offset,
                                )
                            }
                        };
                        // Use append_blob_at_offset to store at the leader-assigned offset.
                        partition
                            .append_blob_at_offset(
                                blob_to_store,
                                batched.record_count,
                                current_offset,
                            )
                            .map_err(|e| ServerError::Internal {
                                message: format!(
                                    "failed to append blob in batch: {e}"
                                ),
                            })?;
                        current_offset = Offset::new(
                            current_offset.get()
                                + u64::from(batched.record_count),
                        );
                    }

                    let new_hwm = partition.blob_log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    debug!(
                        topic = %self.topic_id,
                        partition = %self.partition_id,
                        index = %index,
                        base_offset = %base_offset,
                        new_hwm = %new_hwm,
                        "applied blob batch"
                    );
                    Some(base_offset)
                }
            },
            PartitionStorageInner::Durable(partition) => {
                // Unified WAL write path: one Raft commit = one WAL entry.
                // When payload is empty, metadata IS the encode() bytes (non-blob or
                // single-path commands).
                // When payload is non-empty we must re-encode from the decoded command
                // rather than concatenating metadata||payload. The encode_split() format
                // for AppendBlobBatch with N>1 blobs puts all blob headers first then
                // all blob bytes; encode() interleaves each blob's header with its data.
                // metadata||payload and encode() are only identical for N==1. Re-encoding
                // from the already-decoded `command` produces the correct WAL format so
                // that WAL recovery (decode_owned) and apply_command_to_cache both work.
                let wal_data = if payload.is_empty() {
                    metadata.clone()
                } else {
                    command.encode()
                };

                partition
                    .write_wal_entry(index.get(), term.get(), wal_data.clone())
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to write WAL entry: {e}"),
                    })?;

                // Decode and apply to cache only (no additional WAL writes).
                // Pass wal_index for BlobIndex population. For shared WALs, this
                // is the auto-counter (not the Raft index), since read_entry()
                // looks up by auto-counter.
                let wal_index = partition.last_applied_wal_index();
                partition
                    .apply_command_to_cache(wal_index, &wal_data)
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to apply to cache: {e}"),
                    })?
            }
        };

        self.last_applied = index;
        self.last_applied_term = term;

        // Notify tiering system that entries up to `index` are committed.
        // For dedicated WAL: marks per-segment committed tracking in metadata store.
        // For shared WAL: updates the coordinator's committed-WAL-index so the
        // coordinator can determine when whole shared segments are safe to tier.
        if let PartitionStorageInner::Durable(partition) = &self.inner {
            // Fire-and-forget: tiering is best-effort. Ignore errors here;
            // `process_tiering()` will retry on the next tick.
            let _ = partition.on_entries_committed(index.get()).await;
        }

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
            PartitionStorageInner::Durable(p) => p
                .check_and_register_sealed_segments()
                .await
                .map_err(|e| ServerError::Internal {
                    message: format!("tiering registration failed: {e}"),
                }),
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
            PartitionStorageInner::Durable(p) => p
                .on_entries_committed(committed_index)
                .await
                .map_err(|e| ServerError::Internal {
                    message: format!("tiering commit notification failed: {e}"),
                }),
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

    // -------------------------------------------------------------------------
    // Segment Retention
    // -------------------------------------------------------------------------

    /// Runs segment retention for the underlying durable partition.
    ///
    /// Deletes sealed WAL segments that are older than `local_retention_ms`
    /// and whose entries have all been replicated.
    ///
    /// Returns 0 for in-memory storage (no WAL to retain).
    ///
    /// # Errors
    ///
    /// Returns an error if a segment file cannot be removed.
    pub async fn run_retention(
        &mut self,
        min_replicated_index: u64,
        local_retention_ms: u64,
    ) -> Result<u32, ServerError> {
        match &mut self.inner {
            PartitionStorageInner::InMemory(_) => Ok(0),
            PartitionStorageInner::Durable(p) => {
                p.run_retention(min_replicated_index, local_retention_ms)
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("retention failed: {e}"),
                    })
            }
        }
    }

    /// Trims `BlobIndex` entries referencing WAL indices below `min_wal_index`.
    ///
    /// Called after a shared WAL segment is deleted to free memory from stale
    /// `blob_index` entries. No-op for in-memory storage.
    pub fn trim_blob_index(&mut self, min_wal_index: u64) {
        match &mut self.inner {
            PartitionStorageInner::InMemory(_) => {}
            PartitionStorageInner::Durable(p) => p.trim_blob_index(min_wal_index),
        }
    }

    // -------------------------------------------------------------------------
    // WAL Read Methods (for WAL-backed AppendEntries)
    // -------------------------------------------------------------------------

    /// Reads WAL entries sequentially for serving via `AppendEntries`.
    ///
    /// Returns `None` for in-memory storage (no WAL).
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL read fails.
    pub async fn read_wal_entries(
        &self,
        start: u64,
        max_bytes: u64,
    ) -> ServerResult<Option<Vec<helix_wal::Entry>>> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => Ok(None),
            PartitionStorageInner::Durable(p) => {
                let entries = p
                    .read_wal_entries(start, max_bytes)
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("WAL read failed: {e}"),
                    })?;
                Ok(Some(entries))
            }
        }
    }

    /// Reads a single WAL entry by Raft index.
    ///
    /// Returns `None` for in-memory storage (no WAL).
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL read fails.
    pub async fn read_wal_entry(
        &self,
        index: u64,
    ) -> ServerResult<Option<helix_wal::Entry>> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => Ok(None),
            PartitionStorageInner::Durable(p) => p
                .read_wal_entry(index)
                .await
                .map_err(|e| ServerError::Internal {
                    message: format!("WAL read failed: {e}"),
                }),
        }
    }

    /// Reads a single WAL entry by Raft log index.
    ///
    /// For shared WAL this searches by the `raft_index` field in the entry
    /// header, which differs from the WAL auto-counter. Use this when you have
    /// a Raft log index (e.g. `prev_log_index` during follower catch-up).
    ///
    /// Returns `None` for in-memory storage (no WAL).
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL read fails.
    pub async fn read_wal_entry_by_raft_index(
        &self,
        raft_index: u64,
    ) -> ServerResult<Option<helix_wal::Entry>> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => Ok(None),
            PartitionStorageInner::Durable(p) => p
                .read_wal_entry_by_raft_index(raft_index)
                .await
                .map_err(|e| ServerError::Internal {
                    message: format!("WAL read failed: {e}"),
                }),
        }
    }

    /// Returns the compact state of the WAL: (index, term) of the last entry
    /// deleted by retention. Returns `None` if no retention has run yet.
    pub async fn wal_compact_state(&self) -> Option<(u64, u64)> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => None,
            PartitionStorageInner::Durable(p) => p.wal_compact_state().await,
        }
    }

    /// Returns the WAL floor index (first available index after retention).
    ///
    /// Returns `None` for in-memory storage or an empty WAL.
    pub async fn wal_floor(&self) -> Option<u64> {
        match &self.inner {
            PartitionStorageInner::InMemory(_) => None,
            PartitionStorageInner::Durable(p) => p.wal_floor().await,
        }
    }
}

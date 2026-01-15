//! Partition storage wrapper.
//!
//! This module provides a unified interface for partition storage,
//! supporting both in-memory (for testing) and durable WAL-backed storage.

use std::path::PathBuf;

use bytes::Bytes;
use helix_core::{LogIndex, Offset, PartitionId, ProducerEpoch, ProducerId, Record, SequenceNum, TopicId};
use helix_wal::{Storage, TokioStorage};
use tracing::warn;

use crate::error::{ServerError, ServerResult};
use crate::producer_state::{PartitionProducerState, SequenceCheckResult};
use crate::storage::{
    BlobFormat, DurablePartition, DurablePartitionConfig, DurablePartitionError, Partition,
    PartitionCommand, PartitionConfig, patch_kafka_base_offset,
};

/// Inner storage type for a partition.
///
/// # Type Parameters
///
/// * `S` - Storage backend (e.g., `TokioStorage` for production, `SimulatedStorage` for DST)
pub enum PartitionStorageInner<S: Storage + 'static> {
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
pub struct PartitionStorage<S: Storage + 'static> {
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
pub type ProductionPartitionStorage = PartitionStorage<TokioStorage>;

impl<S: Storage + 'static> PartitionStorage<S> {
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
    /// * `topic_id` - Topic identifier
    /// * `partition_id` - Partition identifier
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened.
    pub async fn new_durable(
        storage: S,
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Result<Self, DurablePartitionError> {
        let config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        let durable = DurablePartition::open(storage, config).await?;
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied: LogIndex::new(0),
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
    #[allow(dead_code)] // Kept for potential future use; async version used everywhere.
    pub fn apply_entry_sync(&mut self, index: LogIndex, data: &Bytes) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            eprintln!("[APPLY_SKIP] topic={} partition={} index={} (already applied, last={})",
                self.topic_id, self.partition_id, index, self.last_applied);
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            eprintln!("[APPLY_NOOP] topic={} partition={} index={} empty entry",
                self.topic_id, self.partition_id, index);
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records } => {
                    let record_count = records.len();
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    // Update high watermark since entry is committed.
                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    eprintln!("[APPLY_APPEND] topic={} partition={} index={} records={} base_offset={} new_hwm={}",
                        self.topic_id, self.partition_id, index, record_count, offset, new_hwm);
                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format } => {
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

                    eprintln!("[APPLY_BLOB] topic={} partition={} index={} records={} base_offset={} new_hwm={} format={:?}",
                        self.topic_id, self.partition_id, index, record_count, offset, new_hwm, format);
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    eprintln!("[APPLY_TRUNCATE] topic={} partition={} index={} from_offset={}",
                        self.topic_id, self.partition_id, index, from_offset);
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    eprintln!("[APPLY_HWM] topic={} partition={} index={} hwm={}",
                        self.topic_id, self.partition_id, index, high_watermark);
                    partition.set_high_watermark(high_watermark);
                    None
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
    pub async fn apply_entry_async(
        &mut self,
        index: LogIndex,
        data: &Bytes,
    ) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            eprintln!("[APPLY_SKIP] topic={} partition={} index={} (already applied, last={})",
                self.topic_id, self.partition_id, index, self.last_applied);
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            eprintln!("[APPLY_NOOP] topic={} partition={} index={} empty entry",
                self.topic_id, self.partition_id, index);
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records } => {
                    let record_count = records.len();
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    eprintln!("[APPLY_APPEND] topic={} partition={} index={} records={} base_offset={} new_hwm={}",
                        self.topic_id, self.partition_id, index, record_count, offset, new_hwm);
                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format } => {
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

                    eprintln!("[APPLY_BLOB] topic={} partition={} index={} records={} base_offset={} new_hwm={} format={:?}",
                        self.topic_id, self.partition_id, index, record_count, offset, new_hwm, format);
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    eprintln!("[APPLY_TRUNCATE] topic={} partition={} index={} from_offset={}",
                        self.topic_id, self.partition_id, index, from_offset);
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    eprintln!("[APPLY_HWM] topic={} partition={} index={} hwm={}",
                        self.topic_id, self.partition_id, index, high_watermark);
                    partition.set_high_watermark(high_watermark);
                    None
                }
            },
            PartitionStorageInner::Durable(partition) => match command {
                PartitionCommand::Append { records } => {
                    let offset = partition.append(records).await.map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append: {e}"),
                        }
                    })?;
                    Some(offset)
                }
                PartitionCommand::AppendBlob { blob, record_count, format } => {
                    // Get the offset that will be assigned before appending.
                    let base_offset = partition.blob_log_end_offset();

                    // Apply protocol-specific patching if needed.
                    let blob_to_store = match format {
                        BlobFormat::Raw => blob,
                        BlobFormat::KafkaRecordBatch => patch_kafka_base_offset(blob, base_offset),
                    };

                    let offset = partition.append_blob(blob_to_store, record_count).await.map_err(|e| {
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
            },
        };

        self.last_applied = index;
        Ok(base_offset)
    }
}

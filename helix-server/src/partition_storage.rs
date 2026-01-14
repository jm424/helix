//! Partition storage wrapper.
//!
//! This module provides a unified interface for partition storage,
//! supporting both in-memory (for testing) and durable WAL-backed storage.

use std::path::PathBuf;

use bytes::Bytes;
use helix_core::{LogIndex, Offset, PartitionId, Record, TopicId};
use tracing::warn;

use crate::error::{ServerError, ServerResult};
use crate::storage::{
    BlobFormat, DurablePartition, DurablePartitionConfig, DurablePartitionError, Partition,
    PartitionCommand, PartitionConfig, patch_kafka_base_offset,
};

/// Inner storage type for a partition.
pub enum PartitionStorageInner {
    /// In-memory storage (for testing).
    InMemory(Partition),
    /// Durable WAL-backed storage (for production).
    /// Boxed to reduce enum size difference between variants.
    Durable(Box<DurablePartition>),
}

/// Storage for a single partition.
pub struct PartitionStorage {
    /// Topic ID.
    #[allow(dead_code)]
    topic_id: TopicId,
    /// Partition ID.
    #[allow(dead_code)]
    partition_id: PartitionId,
    /// The underlying partition storage.
    pub(crate) inner: PartitionStorageInner,
    /// Last applied Raft log index.
    last_applied: LogIndex,
}

impl PartitionStorage {
    /// Creates new in-memory partition storage.
    pub const fn new_in_memory(topic_id: TopicId, partition_id: PartitionId) -> Self {
        let config = PartitionConfig::new(topic_id, partition_id);
        Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::InMemory(Partition::new(config)),
            last_applied: LogIndex::new(0),
        }
    }

    /// Creates new durable partition storage.
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened.
    pub async fn new_durable(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Result<Self, DurablePartitionError> {
        let config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        let durable = DurablePartition::open(config).await?;
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied: LogIndex::new(0),
        })
    }

    /// Returns the log start offset.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    pub fn log_start_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_start_offset(),
            PartitionStorageInner::Durable(p) => p.log_start_offset(),
        }
    }

    /// Returns the log end offset.
    pub fn log_end_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_end_offset(),
            PartitionStorageInner::Durable(p) => p.log_end_offset(),
        }
    }

    /// Returns the high watermark.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    pub fn high_watermark(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.high_watermark(),
            PartitionStorageInner::Durable(p) => p.high_watermark(),
        }
    }

    /// Returns the blob log end offset (for Kafka batch storage).
    pub fn blob_log_end_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.blob_log_end_offset(),
            PartitionStorageInner::Durable(p) => p.blob_log_end_offset(),
        }
    }

    /// Returns the last applied Raft log index.
    #[allow(dead_code)]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
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
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
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

                    // Update high watermark since entry is committed.
                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

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

                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
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
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
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

                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
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

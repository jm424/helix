//! Partition storage for Helix server.
//!
//! This module provides both in-memory and durable partition storage.
//!
//! # Architecture
//!
//! Per the RFC design, data is stored in a tiered architecture:
//! - **Tier 1 (Hot)**: In-memory + WAL on NVMe/EBS
//! - **Tier 2 (Warm)**: S3 Standard (not yet implemented)
//! - **Tier 3 (Cold)**: S3 Glacier (not yet implemented)
//!
//! This module implements Tier 1 with:
//! - [`Partition`]: In-memory storage (cache for fast reads)
//! - [`DurablePartition`]: WAL-backed storage with in-memory cache

use std::path::PathBuf;

use bytes::{BufMut, Bytes, BytesMut};
use helix_core::{Offset, PartitionId, Record, TopicId};
use helix_wal::{Entry, TokioStorage, Wal, WalConfig};
use tracing::{debug, info, warn};

/// Commands that can be applied to a partition.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    /// Append records to the partition.
    Append {
        /// Records to append.
        records: Vec<Record>,
    },
    /// Truncate the partition from a given offset.
    Truncate {
        /// Offset to truncate from (exclusive).
        from_offset: Offset,
    },
    /// Update the high watermark.
    UpdateHighWatermark {
        /// New high watermark value.
        high_watermark: Offset,
    },
}

impl PartitionCommand {
    /// Encodes the command to bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::Append { records } => {
                buf.put_u8(0); // Command type.
                // Safe cast: record count bounded by MAX_RECORDS_PER_WRITE.
                #[allow(clippy::cast_possible_truncation)]
                let count = records.len() as u32;
                buf.put_u32_le(count);
                for record in records {
                    record.encode(&mut buf);
                }
            }
            Self::Truncate { from_offset } => {
                buf.put_u8(1);
                buf.put_u64_le(from_offset.get());
            }
            Self::UpdateHighWatermark { high_watermark } => {
                buf.put_u8(2);
                buf.put_u64_le(high_watermark.get());
            }
        }
        buf.freeze()
    }

    /// Decodes a command from bytes.
    #[must_use]
    pub fn decode(data: &Bytes) -> Option<Self> {
        use bytes::Buf;

        if data.is_empty() {
            return None;
        }

        let mut buf = data.clone();
        let cmd_type = buf.get_u8();

        match cmd_type {
            0 => {
                if buf.remaining() < 4 {
                    return None;
                }
                // Safe cast: record count bounded by MAX_RECORDS_PER_WRITE (1000).
                #[allow(clippy::cast_possible_truncation)]
                let count = buf.get_u32_le() as usize;
                let mut records = Vec::with_capacity(count);
                for _ in 0..count {
                    let record = Record::decode(&mut buf)?;
                    records.push(record);
                }
                Some(Self::Append { records })
            }
            1 => {
                if buf.remaining() < 8 {
                    return None;
                }
                let from_offset = Offset::new(buf.get_u64_le());
                Some(Self::Truncate { from_offset })
            }
            2 => {
                if buf.remaining() < 8 {
                    return None;
                }
                let high_watermark = Offset::new(buf.get_u64_le());
                Some(Self::UpdateHighWatermark { high_watermark })
            }
            _ => None,
        }
    }
}

/// Configuration for a partition.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Topic ID.
    #[allow(dead_code)]
    pub topic_id: TopicId,
    /// Partition ID within the topic.
    #[allow(dead_code)]
    pub partition_id: PartitionId,
}

impl PartitionConfig {
    /// Creates a new partition configuration.
    #[must_use]
    pub const fn new(topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            topic_id,
            partition_id,
        }
    }
}

/// Error type for partition operations.
#[derive(Debug, Clone)]
pub enum PartitionError {
    /// Offset is out of range.
    OffsetOutOfRange {
        /// Requested offset.
        offset: Offset,
        /// First available offset.
        first: Offset,
        /// Last available offset.
        last: Offset,
    },
    /// Partition is closed.
    Closed,
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OffsetOutOfRange {
                offset,
                first,
                last,
            } => {
                write!(
                    f,
                    "offset {} out of range [{}, {}]",
                    offset.get(),
                    first.get(),
                    last.get()
                )
            }
            Self::Closed => write!(f, "partition is closed"),
        }
    }
}

impl std::error::Error for PartitionError {}

/// Result type for partition operations.
pub type PartitionResult<T> = Result<T, PartitionError>;

/// In-memory partition storage.
///
/// This is a temporary implementation. Per the RFC design, storage should
/// use `helix-wal` for durability with in-memory caching for performance.
#[derive(Debug)]
pub struct Partition {
    /// Configuration.
    #[allow(dead_code)]
    config: PartitionConfig,
    /// Records stored in this partition.
    records: Vec<Record>,
    /// Log start offset.
    log_start_offset: Offset,
    /// High watermark.
    high_watermark: Offset,
    /// Whether the partition is closed.
    closed: bool,
}

impl Partition {
    /// Creates a new partition.
    #[must_use]
    pub const fn new(config: PartitionConfig) -> Self {
        Self {
            config,
            records: Vec::new(),
            log_start_offset: Offset::new(0),
            high_watermark: Offset::new(0),
            closed: false,
        }
    }

    /// Returns the log start offset.
    #[must_use]
    pub const fn log_start_offset(&self) -> Offset {
        self.log_start_offset
    }

    /// Returns the log end offset (next offset to be assigned).
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        Offset::new(self.log_start_offset.get() + self.records.len() as u64)
    }

    /// Returns the high watermark.
    #[must_use]
    pub const fn high_watermark(&self) -> Offset {
        self.high_watermark
    }

    /// Sets the high watermark.
    pub fn set_high_watermark(&mut self, hwm: Offset) {
        self.high_watermark = hwm;
    }

    /// Appends records to the partition.
    ///
    /// # Returns
    /// The base offset of the first appended record.
    ///
    /// # Errors
    /// Returns an error if the partition is closed.
    pub fn append(&mut self, records: Vec<Record>) -> PartitionResult<Offset> {
        if self.closed {
            return Err(PartitionError::Closed);
        }

        let base_offset = self.log_end_offset();

        // Assign offsets to records.
        let mut offset = base_offset;
        for mut record in records {
            record.offset = offset;
            self.records.push(record);
            offset = offset.next();
        }

        Ok(base_offset)
    }

    /// Reads records starting at the given offset.
    ///
    /// # Errors
    /// Returns an error if the offset is out of range.
    pub fn read(&self, start_offset: Offset, max_records: u32) -> PartitionResult<Vec<Record>> {
        let log_start = self.log_start_offset;
        let log_end = self.log_end_offset();

        if start_offset < log_start || start_offset > log_end {
            return Err(PartitionError::OffsetOutOfRange {
                offset: start_offset,
                first: log_start,
                last: if log_end.get() > 0 {
                    Offset::new(log_end.get() - 1)
                } else {
                    log_end
                },
            });
        }

        // Calculate the index into our records vector.
        // Safe cast: index is bounded by records.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let start_idx = (start_offset.get() - log_start.get()) as usize;
        #[allow(clippy::cast_possible_truncation)]
        let end_idx = (start_idx + max_records as usize).min(self.records.len());

        Ok(self.records[start_idx..end_idx].to_vec())
    }

    /// Truncates the partition from the given offset.
    ///
    /// # Errors
    /// Returns an error if the partition is closed.
    pub fn truncate(&mut self, from_offset: Offset) -> PartitionResult<()> {
        if self.closed {
            return Err(PartitionError::Closed);
        }

        let log_start = self.log_start_offset;

        if from_offset <= log_start {
            self.records.clear();
            return Ok(());
        }

        // Safe cast: keep_count is bounded by records.len() which fits in usize.
        #[allow(clippy::cast_possible_truncation)]
        let keep_count = (from_offset.get() - log_start.get()) as usize;
        if keep_count < self.records.len() {
            self.records.truncate(keep_count);
        }

        // Adjust high watermark if needed.
        if self.high_watermark > from_offset {
            self.high_watermark = from_offset;
        }

        Ok(())
    }
}

/// Configuration for durable partition storage.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
pub struct DurablePartitionConfig {
    /// Base directory for WAL files.
    pub data_dir: PathBuf,
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID.
    pub partition_id: PartitionId,
    /// Whether to sync after every write.
    pub sync_on_write: bool,
}

#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
impl DurablePartitionConfig {
    /// Creates a new durable partition configuration.
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>, topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            data_dir: data_dir.into(),
            topic_id,
            partition_id,
            sync_on_write: false, // Default to batched syncs for performance.
        }
    }

    /// Enables sync after every write.
    #[must_use]
    pub const fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }

    /// Returns the WAL directory path for this partition.
    fn wal_dir(&self) -> PathBuf {
        self.data_dir
            .join(format!("topic-{}", self.topic_id.get()))
            .join(format!("partition-{}", self.partition_id.get()))
            .join("wal")
    }
}

/// Durable partition storage backed by WAL.
///
/// This implementation provides crash-safe storage by writing to the WAL
/// before updating the in-memory cache. On recovery, entries are replayed
/// from the WAL to rebuild the cache.
///
/// # Design
///
/// - **Write path**: WAL append → sync (if configured) → update cache
/// - **Read path**: Read from in-memory cache (fast)
/// - **Recovery**: Replay WAL entries to rebuild cache
#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
pub struct DurablePartition {
    /// Configuration.
    config: DurablePartitionConfig,
    /// WAL for durable storage.
    wal: Wal<TokioStorage>,
    /// In-memory cache for fast reads.
    cache: Partition,
    /// Last applied WAL index.
    last_applied_index: u64,
}

#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
impl DurablePartition {
    /// Opens or creates a durable partition.
    ///
    /// If the WAL exists, entries are recovered and replayed to rebuild
    /// the in-memory cache.
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened or recovery fails.
    pub async fn open(config: DurablePartitionConfig) -> Result<Self, DurablePartitionError> {
        let wal_dir = config.wal_dir();
        let wal_config = WalConfig::new(&wal_dir).with_sync_on_write(config.sync_on_write);

        info!(
            topic = config.topic_id.get(),
            partition = config.partition_id.get(),
            dir = ?wal_dir,
            "Opening durable partition"
        );

        let wal = Wal::open(TokioStorage::new(), wal_config)
            .await
            .map_err(|e| DurablePartitionError::WalOpen {
                path: wal_dir.clone(),
                message: e.to_string(),
            })?;

        let partition_config = PartitionConfig::new(config.topic_id, config.partition_id);
        let mut cache = Partition::new(partition_config);
        let mut last_applied_index = 0u64;

        // Recover entries from WAL.
        if let Some(last_index) = wal.last_index() {
            let first_index = wal.first_index();
            info!(
                first_index,
                last_index,
                "Recovering entries from WAL"
            );

            for index in first_index..=last_index {
                match wal.read(index) {
                    Ok(entry) => {
                        if let Err(e) = Self::apply_entry_to_cache(&mut cache, entry) {
                            warn!(index, error = %e, "Failed to apply entry during recovery");
                        }
                        last_applied_index = index;
                    }
                    Err(e) => {
                        warn!(index, error = %e, "Failed to read entry during recovery");
                    }
                }
            }

            info!(
                entries = last_index - first_index + 1,
                "Recovery complete"
            );
        }

        Ok(Self {
            config,
            wal,
            cache,
            last_applied_index,
        })
    }

    /// Returns the log start offset.
    #[must_use]
    pub const fn log_start_offset(&self) -> Offset {
        self.cache.log_start_offset()
    }

    /// Returns the log end offset.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        self.cache.log_end_offset()
    }

    /// Returns the high watermark.
    #[must_use]
    pub const fn high_watermark(&self) -> Offset {
        self.cache.high_watermark()
    }

    /// Sets the high watermark.
    pub fn set_high_watermark(&mut self, hwm: Offset) {
        self.cache.set_high_watermark(hwm);
    }

    /// Appends records to the partition.
    ///
    /// The records are first written to the WAL for durability,
    /// then applied to the in-memory cache.
    ///
    /// # Returns
    /// The base offset of the first appended record.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn append(&mut self, records: Vec<Record>) -> Result<Offset, DurablePartitionError> {
        let base_offset = self.cache.log_end_offset();

        // Create WAL entry with command.
        let command = PartitionCommand::Append {
            records: records.clone(),
        };
        let data = command.encode();

        let next_index = self.last_applied_index + 1;
        let entry = Entry::new(0, next_index, data).map_err(|e| DurablePartitionError::WalWrite {
            message: format!("failed to create WAL entry: {e}"),
        })?;

        // Write to WAL first (durability).
        self.wal.append(entry).await.map_err(|e| {
            DurablePartitionError::WalWrite {
                message: e.to_string(),
            }
        })?;

        // Update in-memory cache.
        self.cache.append(records).map_err(|e| {
            DurablePartitionError::CacheUpdate {
                message: e.to_string(),
            }
        })?;

        self.last_applied_index = next_index;

        // Update high watermark (entry is durable).
        let new_hwm = self.cache.log_end_offset();
        self.cache.set_high_watermark(new_hwm);

        debug!(
            topic = self.config.topic_id.get(),
            partition = self.config.partition_id.get(),
            base_offset = base_offset.get(),
            wal_index = next_index,
            "Appended records to durable partition"
        );

        Ok(base_offset)
    }

    /// Reads records starting at the given offset.
    ///
    /// Reads from the in-memory cache for performance.
    ///
    /// # Errors
    /// Returns an error if the offset is out of range.
    pub fn read(&self, start_offset: Offset, max_records: u32) -> Result<Vec<Record>, PartitionError> {
        self.cache.read(start_offset, max_records)
    }

    /// Syncs the WAL to disk.
    ///
    /// Call this periodically for group commit, or rely on `sync_on_write`
    /// for per-entry durability.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub async fn sync(&mut self) -> Result<(), DurablePartitionError> {
        self.wal.sync().await.map_err(|e| DurablePartitionError::WalSync {
            message: e.to_string(),
        })
    }

    /// Applies a WAL entry to the cache during recovery.
    fn apply_entry_to_cache(cache: &mut Partition, entry: &Entry) -> Result<(), PartitionError> {
        let data = Bytes::copy_from_slice(&entry.payload);

        if data.is_empty() {
            return Ok(());
        }

        let command = PartitionCommand::decode(&data).ok_or_else(|| PartitionError::OffsetOutOfRange {
            offset: Offset::new(0),
            first: Offset::new(0),
            last: Offset::new(0),
        })?;

        match command {
            PartitionCommand::Append { records } => {
                cache.append(records)?;
            }
            PartitionCommand::Truncate { from_offset } => {
                cache.truncate(from_offset)?;
            }
            PartitionCommand::UpdateHighWatermark { high_watermark } => {
                cache.set_high_watermark(high_watermark);
            }
        }

        Ok(())
    }
}

/// Error type for durable partition operations.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
pub enum DurablePartitionError {
    /// Failed to open WAL.
    WalOpen {
        /// Path that failed to open.
        path: PathBuf,
        /// Error message.
        message: String,
    },
    /// Failed to write to WAL.
    WalWrite {
        /// Error message.
        message: String,
    },
    /// Failed to sync WAL.
    WalSync {
        /// Error message.
        message: String,
    },
    /// Failed to update cache.
    CacheUpdate {
        /// Error message.
        message: String,
    },
    /// Partition is closed.
    Closed,
}

impl std::fmt::Display for DurablePartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WalOpen { path, message } => {
                write!(f, "failed to open WAL at {}: {message}", path.display())
            }
            Self::WalWrite { message } => {
                write!(f, "failed to write to WAL: {message}")
            }
            Self::WalSync { message } => {
                write!(f, "failed to sync WAL: {message}")
            }
            Self::CacheUpdate { message } => {
                write!(f, "failed to update cache: {message}")
            }
            Self::Closed => write!(f, "partition is closed"),
        }
    }
}

impl std::error::Error for DurablePartitionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_append_and_read() {
        let config = PartitionConfig::new(TopicId::new(1), PartitionId::new(0));
        let mut partition = Partition::new(config);

        let records = vec![
            Record::new(Bytes::from("value1")),
            Record::new(Bytes::from("value2")),
        ];

        let base_offset = partition.append(records).unwrap();
        assert_eq!(base_offset, Offset::new(0));
        assert_eq!(partition.log_end_offset(), Offset::new(2));

        let read_records = partition.read(Offset::new(0), 10).unwrap();
        assert_eq!(read_records.len(), 2);
    }

    #[test]
    fn test_partition_command_roundtrip() {
        let records = vec![Record::new(Bytes::from("test"))];
        let cmd = PartitionCommand::Append { records };
        let encoded = cmd.encode();
        let decoded = PartitionCommand::decode(&encoded).unwrap();

        match decoded {
            PartitionCommand::Append { records } => {
                assert_eq!(records.len(), 1);
            }
            _ => panic!("wrong command type"),
        }
    }

    #[tokio::test]
    async fn test_durable_partition_append_and_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        );

        let mut partition = DurablePartition::open(config).await.unwrap();

        let records = vec![
            Record::new(Bytes::from("value1")),
            Record::new(Bytes::from("value2")),
        ];

        let base_offset = partition.append(records).await.unwrap();
        assert_eq!(base_offset, Offset::new(0));
        assert_eq!(partition.log_end_offset(), Offset::new(2));
        assert_eq!(partition.high_watermark(), Offset::new(2));

        let read_records = partition.read(Offset::new(0), 10).unwrap();
        assert_eq!(read_records.len(), 2);
    }

    #[tokio::test]
    async fn test_durable_partition_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Write some records.
        {
            let config = DurablePartitionConfig::new(
                temp_dir.path(),
                TopicId::new(1),
                PartitionId::new(0),
            );

            let mut partition = DurablePartition::open(config).await.unwrap();

            for i in 0..5 {
                let records = vec![Record::new(Bytes::from(format!("value-{i}")))];
                partition.append(records).await.unwrap();
            }

            partition.sync().await.unwrap();
        }

        // Reopen and verify recovery.
        {
            let config = DurablePartitionConfig::new(
                temp_dir.path(),
                TopicId::new(1),
                PartitionId::new(0),
            );

            let partition = DurablePartition::open(config).await.unwrap();

            assert_eq!(partition.log_end_offset(), Offset::new(5));

            let read_records = partition.read(Offset::new(0), 10).unwrap();
            assert_eq!(read_records.len(), 5);
        }
    }
}

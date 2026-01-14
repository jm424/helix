//! Partition storage for Helix server.
//!
//! This module provides both in-memory and durable partition storage.
//!
//! # Architecture
//!
//! Per the RFC design, data is stored in a tiered architecture:
//! - **Tier 1 (Hot)**: In-memory + WAL on NVMe/EBS
//! - **Tier 2 (Warm)**: S3 Standard (via `helix-tier`)
//! - **Tier 3 (Cold)**: S3 Glacier (not yet implemented)
//!
//! This module implements Tier 1 with:
//! - [`Partition`]: In-memory storage (cache for fast reads)
//! - [`DurablePartition`]: WAL-backed storage with in-memory cache
//!
//! # Tiered Storage Integration
//!
//! When tiering is enabled, sealed WAL segments can be uploaded to S3 via
//! the [`helix_tier::IntegratedTieringManager`]. The `DurablePartition`
//! tracks segment lifecycle and notifies the tiering manager when segments
//! are sealed or committed.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::{ConsumerGroupId, ConsumerId, Offset, PartitionId, Record, TopicId};

// -----------------------------------------------------------------------------
// StoredBlob
// -----------------------------------------------------------------------------

/// A stored blob with metadata for protocol-agnostic storage.
///
/// Blobs are opaque byte sequences (e.g., Kafka `RecordBatch`) stored without
/// parsing. Metadata enables offset tracking and efficient retrieval.
#[derive(Debug, Clone)]
pub struct StoredBlob {
    /// Base offset of records in this blob.
    pub base_offset: Offset,
    /// Number of records in this blob.
    pub record_count: u32,
    /// Raw blob data (e.g., Kafka `RecordBatch` bytes).
    pub data: Bytes,
}

impl StoredBlob {
    /// Creates a new stored blob.
    #[must_use]
    pub const fn new(base_offset: Offset, record_count: u32, data: Bytes) -> Self {
        Self {
            base_offset,
            record_count,
            data,
        }
    }

    /// Returns the last offset in this blob (inclusive).
    #[must_use]
    pub fn last_offset(&self) -> Offset {
        if self.record_count == 0 {
            self.base_offset
        } else {
            Offset::new(self.base_offset.get() + u64::from(self.record_count) - 1)
        }
    }

    /// Returns the next offset after this blob.
    #[must_use]
    pub fn next_offset(&self) -> Offset {
        Offset::new(self.base_offset.get() + u64::from(self.record_count))
    }
}
use helix_progress::{
    Lease, ProgressConfig, ProgressError, ProgressManager, SimulatedProgressStore,
};
use helix_tier::{
    InMemoryMetadataStore, IntegratedTieringManager, SegmentMetadata, SegmentReader,
    SimulatedObjectStorage, TierError, TierResult, TieringConfig,
};
use helix_wal::{Entry, SegmentId, TokioStorage, Wal, WalConfig};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// -----------------------------------------------------------------------------
// WalSegmentReader
// -----------------------------------------------------------------------------

/// Wrapper that implements `SegmentReader` for a WAL reference.
///
/// This allows the `IntegratedTieringManager` to read segment bytes directly
/// from the WAL when tiering to S3.
///
/// Uses `RwLock` to allow concurrent reads while writes are exclusive.
pub struct WalSegmentReader {
    /// Shared reference to the WAL (behind `RwLock` for concurrent access).
    wal: Arc<RwLock<Wal<TokioStorage>>>,
}

impl WalSegmentReader {
    /// Creates a new segment reader for the given WAL.
    #[must_use]
    pub const fn new(wal: Arc<RwLock<Wal<TokioStorage>>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl SegmentReader for WalSegmentReader {
    async fn read_segment_bytes(&self, segment_id: SegmentId) -> TierResult<Bytes> {
        let wal = self.wal.read().await;

        // TigerStyle: Assert precondition.
        assert!(
            wal.segment_info(segment_id).is_some(),
            "segment must exist before reading"
        );

        wal.read_segment_bytes(segment_id).map_err(|e| TierError::Io {
            operation: "read_segment_bytes",
            message: e.to_string(),
        })
    }

    fn is_segment_sealed(&self, segment_id: SegmentId) -> bool {
        // Use try_read to avoid blocking; if we can't get the lock, assume not sealed.
        // This is safe because is_segment_sealed is only used as a precondition check.
        self.wal
            .try_read()
            .map(|wal| wal.segment_info(segment_id).is_some_and(|info| info.is_sealed))
            .unwrap_or(false)
    }
}

// -----------------------------------------------------------------------------
// Tiering Type Aliases
// -----------------------------------------------------------------------------

/// Type alias for the tiering manager with simulated storage (for testing).
pub type SimulatedTieringManager =
    IntegratedTieringManager<SimulatedObjectStorage, InMemoryMetadataStore, WalSegmentReader>;

/// Type alias for the progress manager with simulated storage (for testing).
pub type SimulatedProgressManager = ProgressManager<SimulatedProgressStore>;

// -----------------------------------------------------------------------------
// BlobFormat
// -----------------------------------------------------------------------------

/// Format of blob data, used to determine protocol-specific transformations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlobFormat {
    /// Raw blob data, no transformation needed.
    #[default]
    Raw,
    /// Kafka `RecordBatch` format - requires baseOffset patching at apply time.
    KafkaRecordBatch,
}

// -----------------------------------------------------------------------------
// PartitionCommand
// -----------------------------------------------------------------------------

/// Commands that can be applied to a partition.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    /// Append records to the partition (typed records).
    Append {
        /// Records to append.
        records: Vec<Record>,
    },
    /// Append a blob to the partition (protocol-agnostic storage).
    ///
    /// Protocol-specific transformations (like Kafka baseOffset patching) are
    /// applied at command application time, not at storage time. This ensures
    /// data written to the log is correct while keeping storage protocol-agnostic.
    AppendBlob {
        /// Raw blob data.
        blob: Bytes,
        /// Number of records in the blob.
        record_count: u32,
        /// Format of the blob data.
        format: BlobFormat,
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
            Self::AppendBlob { blob, record_count, format } => {
                buf.put_u8(3); // Command type for blob.
                buf.put_u32_le(*record_count);
                // Encode format: 0 = Raw, 1 = KafkaRecordBatch.
                let format_byte = match format {
                    BlobFormat::Raw => 0u8,
                    BlobFormat::KafkaRecordBatch => 1u8,
                };
                buf.put_u8(format_byte);
                // Safe cast: blob size bounded by protocol limits.
                #[allow(clippy::cast_possible_truncation)]
                let blob_len = blob.len() as u32;
                buf.put_u32_le(blob_len);
                buf.put_slice(blob);
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
            3 => {
                // AppendBlob: record_count (u32) + format (u8) + blob_len (u32) + blob bytes.
                if buf.remaining() < 9 {
                    return None;
                }
                let record_count = buf.get_u32_le();
                let format_byte = buf.get_u8();
                let format = match format_byte {
                    0 => BlobFormat::Raw,
                    1 => BlobFormat::KafkaRecordBatch,
                    _ => return None, // Unknown format.
                };
                let blob_len = buf.get_u32_le() as usize;
                if buf.remaining() < blob_len {
                    return None;
                }
                let blob = buf.copy_to_bytes(blob_len);
                Some(Self::AppendBlob { blob, record_count, format })
            }
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// Kafka Format Helpers
// -----------------------------------------------------------------------------

/// Patches the baseOffset field in a Kafka `RecordBatch`.
///
/// In Kafka's `RecordBatch` format, the first 8 bytes contain the baseOffset.
/// Clients send this as 0 since they don't know the actual offset. The broker
/// patches this field when the batch is committed to the log.
///
/// This function returns a new `Bytes` with the patched offset, leaving the
/// original data unchanged.
#[must_use]
pub fn patch_kafka_base_offset(blob: Bytes, base_offset: Offset) -> Bytes {
    const KAFKA_BASE_OFFSET_SIZE: usize = 8;

    if blob.len() < KAFKA_BASE_OFFSET_SIZE {
        // Blob too small to be a valid RecordBatch, return as-is.
        tracing::warn!(
            blob_len = blob.len(),
            "Blob too small for Kafka patching"
        );
        return blob;
    }

    // Read original baseOffset for debugging.
    let original_base_offset = i64::from_be_bytes([
        blob[0], blob[1], blob[2], blob[3], blob[4], blob[5], blob[6], blob[7],
    ]);

    let mut patched = BytesMut::with_capacity(blob.len());
    patched.put_i64(base_offset.get() as i64); // baseOffset is big-endian i64.
    patched.put_slice(&blob[KAFKA_BASE_OFFSET_SIZE..]);
    let result = patched.freeze();

    tracing::info!(
        blob_len = blob.len(),
        original_base_offset,
        new_base_offset = base_offset.get(),
        "Patched Kafka RecordBatch baseOffset"
    );

    result
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
/// Supports two storage modes:
/// - **Typed records**: Individual `Record` structs (used by gRPC API).
/// - **Blobs**: Opaque byte sequences (used by Kafka-compat for zero-copy).
///
/// Both modes can coexist but typically a partition uses one or the other.
#[derive(Debug)]
pub struct Partition {
    /// Configuration.
    #[allow(dead_code)]
    config: PartitionConfig,
    /// Records stored in this partition (typed mode).
    records: Vec<Record>,
    /// Blobs stored in this partition (protocol-agnostic mode).
    blobs: Vec<StoredBlob>,
    /// Log start offset.
    log_start_offset: Offset,
    /// Log end offset for blob storage (next offset to assign).
    blob_log_end_offset: Offset,
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
            blobs: Vec::new(),
            log_start_offset: Offset::new(0),
            blob_log_end_offset: Offset::new(0),
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
    pub const fn set_high_watermark(&mut self, hwm: Offset) {
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

    // -------------------------------------------------------------------------
    // Blob Storage Methods (Protocol-Agnostic)
    // -------------------------------------------------------------------------

    /// Returns the log end offset for blob storage.
    #[must_use]
    pub const fn blob_log_end_offset(&self) -> Offset {
        self.blob_log_end_offset
    }

    /// Returns the log start offset for blob storage (earliest available offset).
    #[must_use]
    pub fn blob_log_start_offset(&self) -> Offset {
        self.blobs
            .first()
            .map_or(Offset::new(0), |b| b.base_offset)
    }

    /// Appends a blob to the partition.
    ///
    /// The blob is stored as-is without parsing. The `record_count` is used
    /// for offset allocation.
    ///
    /// # Returns
    /// The base offset assigned to this blob.
    ///
    /// # Errors
    /// Returns an error if the partition is closed.
    pub fn append_blob(&mut self, blob: Bytes, record_count: u32) -> PartitionResult<Offset> {
        if self.closed {
            return Err(PartitionError::Closed);
        }

        let base_offset = self.blob_log_end_offset;

        // Store the blob as-is with its metadata.
        // Protocol-specific transformations (like Kafka baseOffset patching) happen at read time.
        self.blobs.push(StoredBlob::new(base_offset, record_count, blob));

        // Advance the log end offset.
        self.blob_log_end_offset = Offset::new(base_offset.get() + u64::from(record_count));

        Ok(base_offset)
    }

    /// Reads blobs starting at the given offset.
    ///
    /// Returns blobs that overlap with the requested offset range, up to
    /// `max_bytes` total size. Always returns at least one blob if any
    /// overlaps with the start offset.
    ///
    /// # Arguments
    /// * `start_offset` - First offset to fetch.
    /// * `max_bytes` - Maximum total bytes to return.
    ///
    /// # Returns
    /// Vector of blobs overlapping with the requested range.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    pub fn read_blobs(&self, start_offset: Offset, max_bytes: u32) -> Vec<StoredBlob> {
        let mut result = Vec::new();
        let mut total_bytes = 0u32;

        for blob in &self.blobs {
            // Check if this blob overlaps with the requested offset.
            let blob_end_offset = blob.next_offset();
            if blob_end_offset.get() <= start_offset.get() {
                // Blob is entirely before the requested offset.
                continue;
            }

            // Check if we'd exceed max_bytes (but always include at least one blob).
            let blob_size = blob.data.len() as u32;
            if !result.is_empty() && total_bytes + blob_size > max_bytes {
                break;
            }

            result.push(blob.clone());
            total_bytes += blob_size;
        }

        result
    }

    /// Checks if a blob partition has any data at the given offset.
    #[must_use]
    pub fn blob_partition_exists(&self, start_offset: Offset) -> bool {
        // If we have blobs and the offset is within range.
        !self.blobs.is_empty() && start_offset.get() < self.blob_log_end_offset.get()
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
    /// Optional tiering configuration. If `Some`, tiering is enabled.
    pub tiering: Option<TieringConfig>,
    /// Optional progress tracking configuration. If `Some`, consumer progress is tracked.
    pub progress: Option<ProgressConfig>,
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
            tiering: None,
            progress: None,
        }
    }

    /// Enables sync after every write.
    #[must_use]
    pub const fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }

    /// Enables tiered storage with the given configuration.
    #[must_use]
    pub const fn with_tiering(mut self, config: TieringConfig) -> Self {
        self.tiering = Some(config);
        self
    }

    /// Enables consumer progress tracking with the given configuration.
    #[must_use]
    pub const fn with_progress(mut self, config: ProgressConfig) -> Self {
        self.progress = Some(config);
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
///
/// # Tiering Integration
///
/// When tiering is enabled, sealed segments can be uploaded to S3. The
/// partition tracks the last committed index to determine when segments
/// are safe to tier.
#[allow(dead_code)] // Used in tests and will be used in service.rs integration.
pub struct DurablePartition {
    /// Configuration.
    config: DurablePartitionConfig,
    /// WAL for durable storage (shared with tiering manager via `RwLock`).
    wal: Arc<RwLock<Wal<TokioStorage>>>,
    /// In-memory cache for fast reads.
    cache: Partition,
    /// Last applied WAL index.
    last_applied_index: u64,
    /// Tiering manager for S3 uploads (optional).
    tiering: Option<SimulatedTieringManager>,
    /// Set of segment IDs that have been registered with tiering.
    tiered_segments: std::collections::HashSet<u64>,
    /// Progress manager for consumer offset tracking (optional).
    progress: Option<SimulatedProgressManager>,
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
            tiering_enabled = config.tiering.is_some(),
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

        // Wrap WAL in Arc<RwLock> for shared access with tiering.
        let wal = Arc::new(RwLock::new(wal));

        // Initialize tiering if configured.
        let tiering = config.tiering.as_ref().map(|tiering_config| {
            let segment_reader = WalSegmentReader::new(wal.clone());
            // Use simulated storage for now - production would use S3.
            let object_storage = SimulatedObjectStorage::new(42);
            let metadata_store = InMemoryMetadataStore::new();

            info!(
                min_age_secs = tiering_config.min_age_secs,
                max_concurrent = tiering_config.max_concurrent_uploads,
                "Tiering enabled"
            );

            IntegratedTieringManager::new(
                object_storage,
                metadata_store,
                segment_reader,
                tiering_config.clone(),
            )
        });

        // Initialize progress tracking if configured.
        let progress = config.progress.as_ref().map(|progress_config| {
            // Use simulated storage for now - production would use Raft-backed storage.
            let store = SimulatedProgressStore::new(42);

            info!(
                max_lease_duration_us = progress_config.max_lease_duration_us,
                max_consumers = progress_config.max_consumers_per_group,
                "Progress tracking enabled"
            );

            ProgressManager::new(store, progress_config.clone())
        });

        Ok(Self {
            config,
            wal,
            cache,
            last_applied_index,
            tiering,
            tiered_segments: std::collections::HashSet::new(),
            progress,
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
    pub const fn set_high_watermark(&mut self, hwm: Offset) {
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
        self.wal
            .write()
            .await
            .append(entry)
            .await
            .map_err(|e| DurablePartitionError::WalWrite {
                message: e.to_string(),
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

    // -------------------------------------------------------------------------
    // Blob Storage Methods (Protocol-Agnostic)
    // -------------------------------------------------------------------------

    /// Returns the log end offset for blob storage.
    #[must_use]
    pub const fn blob_log_end_offset(&self) -> Offset {
        self.cache.blob_log_end_offset()
    }

    /// Returns the log start offset for blob storage (earliest available offset).
    #[must_use]
    pub fn blob_log_start_offset(&self) -> Offset {
        self.cache.blob_log_start_offset()
    }

    /// Appends a blob to the partition.
    ///
    /// The blob is written to WAL for durability, then applied to the
    /// in-memory cache. No parsing or conversion is performed.
    ///
    /// # Arguments
    /// * `blob` - Raw blob data (e.g., Kafka `RecordBatch` bytes).
    /// * `record_count` - Number of records in the blob (for offset allocation).
    ///
    /// # Returns
    /// The base offset assigned to this blob.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn append_blob(
        &mut self,
        blob: Bytes,
        record_count: u32,
    ) -> Result<Offset, DurablePartitionError> {
        let base_offset = self.cache.blob_log_end_offset();
        let blob_size = blob.len();

        // Create WAL entry with blob command.
        // Use Raw format since any protocol-specific patching already happened
        // at the apply layer before this method is called.
        let command = PartitionCommand::AppendBlob {
            blob: blob.clone(),
            record_count,
            format: BlobFormat::Raw,
        };
        let data = command.encode();

        let next_index = self.last_applied_index + 1;
        let entry = Entry::new(0, next_index, data).map_err(|e| DurablePartitionError::WalWrite {
            message: format!("failed to create WAL entry: {e}"),
        })?;

        // Write to WAL first (durability).
        self.wal
            .write()
            .await
            .append(entry)
            .await
            .map_err(|e| DurablePartitionError::WalWrite {
                message: e.to_string(),
            })?;

        // Update in-memory cache.
        self.cache.append_blob(blob, record_count).map_err(|e| {
            DurablePartitionError::CacheUpdate {
                message: e.to_string(),
            }
        })?;

        self.last_applied_index = next_index;

        // Update high watermark (entry is durable).
        let new_hwm = self.cache.blob_log_end_offset();
        self.cache.set_high_watermark(new_hwm);

        debug!(
            topic = self.config.topic_id.get(),
            partition = self.config.partition_id.get(),
            base_offset = base_offset.get(),
            record_count,
            blob_size,
            wal_index = next_index,
            "Appended blob to durable partition"
        );

        Ok(base_offset)
    }

    /// Reads blobs starting at the given offset.
    ///
    /// Returns raw blobs from the in-memory cache without parsing.
    ///
    /// # Arguments
    /// * `start_offset` - First offset to fetch.
    /// * `max_bytes` - Maximum total bytes to return.
    ///
    /// # Returns
    /// Vector of stored blobs overlapping with the requested range.
    #[must_use]
    pub fn read_blobs(&self, start_offset: Offset, max_bytes: u32) -> Vec<StoredBlob> {
        self.cache.read_blobs(start_offset, max_bytes)
    }

    /// Checks if the blob partition has data.
    #[must_use]
    pub const fn blob_partition_exists(&self) -> bool {
        self.cache.blob_log_end_offset().get() > 0
    }

    /// Syncs the WAL to disk.
    ///
    /// Call this periodically for group commit, or rely on `sync_on_write`
    /// for per-entry durability.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub async fn sync(&self) -> Result<(), DurablePartitionError> {
        let mut wal = self.wal.write().await;
        wal.sync().await.map_err(|e| DurablePartitionError::WalSync {
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
            PartitionCommand::AppendBlob { blob, record_count, format: _ } => {
                // During WAL recovery, blobs are already in the correct format
                // (patching happened at original apply time). Store as-is.
                cache.append_blob(blob, record_count)?;
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

    // -------------------------------------------------------------------------
    // Tiering Hooks
    // -------------------------------------------------------------------------

    /// Checks for newly sealed segments and registers them with the tiering manager.
    ///
    /// This should be called periodically or after writes that may have caused
    /// segment rotation.
    ///
    /// # Errors
    ///
    /// Returns an error if registration with the tiering manager fails.
    ///
    /// # Panics
    ///
    /// Panics if more than 100 segments are registered in a single call (`TigerStyle` bound).
    pub async fn check_and_register_sealed_segments(&mut self) -> Result<u32, DurablePartitionError> {
        let Some(tiering) = &self.tiering else {
            return Ok(0);
        };

        // Read WAL state under lock.
        let wal = self.wal.read().await;
        let sealed_ids = wal.sealed_segment_ids();

        // Collect segment info while holding the lock.
        let mut segments_to_register = Vec::new();
        for segment_id in sealed_ids.iter().take(100) {
            let segment_id_raw = segment_id.get();

            // Skip if already registered.
            if self.tiered_segments.contains(&segment_id_raw) {
                continue;
            }

            // Get segment info for metadata.
            if let Some(info) = wal.segment_info(*segment_id) {
                segments_to_register.push((
                    *segment_id,
                    segment_id_raw,
                    info.first_index,
                    info.last_index,
                ));
            } else {
                warn!(segment_id = segment_id_raw, "Segment info not found");
            }
        }
        drop(wal); // Release lock before async operations.

        let mut registered_count = 0u32;

        // Register segments without holding the lock.
        for (segment_id, segment_id_raw, first_index, last_index) in segments_to_register {
            // Create metadata with offset range for eviction coordination.
            let mut metadata = SegmentMetadata::new(
                segment_id,
                self.config.topic_id,
                self.config.partition_id,
                first_index,
            );

            // Set offset range if we know it (required for progress-aware eviction).
            if let Some(last) = last_index {
                metadata.set_offset_range(Offset::new(first_index), Offset::new(last));
            }

            if let Err(e) = tiering.register_segment(metadata).await {
                warn!(
                    segment_id = segment_id_raw,
                    error = %e,
                    "Failed to register segment with tiering"
                );
                continue;
            }

            // Mark as sealed in tiering.
            if let Err(e) = tiering.mark_sealed(segment_id).await {
                warn!(
                    segment_id = segment_id_raw,
                    error = %e,
                    "Failed to mark segment as sealed"
                );
                continue;
            }

            self.tiered_segments.insert(segment_id_raw);
            registered_count += 1;

            debug!(
                segment_id = segment_id_raw,
                first_index,
                "Registered sealed segment with tiering"
            );
        }

        // TigerStyle: Assert postcondition.
        assert!(registered_count <= 100, "bounded registration count");

        Ok(registered_count)
    }

    /// Marks segments as committed when their entries are committed through Raft.
    ///
    /// # Arguments
    ///
    /// * `committed_index` - The highest committed WAL index.
    ///
    /// # Errors
    ///
    /// Returns an error if marking segments as committed fails.
    pub async fn on_entries_committed(&self, committed_index: u64) -> Result<u32, DurablePartitionError> {
        let Some(tiering) = &self.tiering else {
            return Ok(0);
        };

        // Read WAL state under lock.
        let wal = self.wal.read().await;
        let sealed_ids = wal.sealed_segment_ids();

        // Collect segments to commit while holding the lock.
        let mut segments_to_commit = Vec::new();
        for segment_id in sealed_ids.iter().take(100) {
            if let Some(info) = wal.segment_info(*segment_id) {
                // Check if all entries in this segment are committed.
                let segment_last_index = info.last_index.unwrap_or(info.first_index);
                if segment_last_index <= committed_index {
                    segments_to_commit.push((*segment_id, segment_last_index));
                }
            }
        }
        drop(wal); // Release lock before async operations.

        let mut committed_count = 0u32;

        // Mark segments as committed without holding the lock.
        for (segment_id, segment_last_index) in segments_to_commit {
            if let Err(e) = tiering.mark_committed(segment_id).await {
                warn!(
                    segment_id = segment_id.get(),
                    error = %e,
                    "Failed to mark segment as committed"
                );
                continue;
            }

            committed_count += 1;

            debug!(
                segment_id = segment_id.get(),
                segment_last_index,
                committed_index,
                "Segment marked as committed for tiering"
            );
        }

        Ok(committed_count)
    }

    /// Tiers eligible segments to S3.
    ///
    /// Finds and uploads segments that are sealed, committed, and meet the
    /// age requirement.
    ///
    /// # Errors
    ///
    /// Returns an error if tiering fails.
    pub async fn tier_eligible_segments(&self) -> Result<u32, DurablePartitionError> {
        let Some(tiering) = &self.tiering else {
            return Ok(0);
        };

        let eligible = tiering
            .find_eligible_segments()
            .await
            .map_err(|e| DurablePartitionError::WalWrite {
                message: format!("failed to find eligible segments: {e}"),
            })?;

        let mut tiered_count = 0u32;

        // `TigerStyle`: bounded iteration.
        for metadata in eligible.iter().take(10) {
            if let Err(e) = tiering.tier_segment(metadata.segment_id).await {
                warn!(
                    segment_id = metadata.segment_id.get(),
                    error = %e,
                    "Failed to tier segment"
                );
                continue;
            }

            tiered_count += 1;

            info!(
                segment_id = metadata.segment_id.get(),
                "Segment tiered to S3"
            );
        }

        Ok(tiered_count)
    }

    /// Returns whether tiering is enabled.
    #[must_use]
    pub const fn has_tiering(&self) -> bool {
        self.tiering.is_some()
    }

    /// Returns the tiering manager for testing purposes.
    #[must_use]
    pub const fn tiering_manager(&self) -> Option<&SimulatedTieringManager> {
        self.tiering.as_ref()
    }

    // -------------------------------------------------------------------------
    // Consumer Progress Methods
    // -------------------------------------------------------------------------

    /// Returns true if consumer progress tracking is enabled.
    #[must_use]
    pub const fn has_progress(&self) -> bool {
        self.progress.is_some()
    }

    /// Returns a reference to the progress manager for testing.
    #[must_use]
    pub const fn progress_manager(&self) -> Option<&SimulatedProgressManager> {
        self.progress.as_ref()
    }

    /// Leases offsets for a consumer to process.
    ///
    /// The consumer must first be registered with the group via
    /// `register_consumer`. The lease grants exclusive access to a range
    /// of offsets for the specified duration.
    ///
    /// # Errors
    ///
    /// Returns error if progress tracking is not enabled, consumer is not
    /// registered, or storage operation fails.
    pub async fn lease_for_consumer(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        max_count: u32,
        duration_us: u64,
        current_time_us: u64,
    ) -> Result<Option<Lease>, DurablePartitionError> {
        let progress = self.progress.as_ref().ok_or(DurablePartitionError::ProgressNotEnabled)?;

        // Start leasing from the current log start offset.
        let from_offset = self.cache.log_start_offset();

        progress
            .lease_offsets(
                group_id,
                self.config.topic_id,
                self.config.partition_id,
                consumer_id,
                from_offset,
                max_count,
                duration_us,
                current_time_us,
            )
            .await
            .map_err(|e| DurablePartitionError::Progress { source: e })
    }

    /// Commits an offset for a consumer.
    ///
    /// The offset must be covered by an active lease held by this consumer.
    /// In cumulative mode, committing offset N means all offsets up to N
    /// are considered processed.
    ///
    /// # Errors
    ///
    /// Returns error if progress tracking is not enabled, offset is not
    /// leased by this consumer, or storage operation fails.
    pub async fn commit_consumer_offset(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        offset: Offset,
        current_time_us: u64,
    ) -> Result<(), DurablePartitionError> {
        let progress = self.progress.as_ref().ok_or(DurablePartitionError::ProgressNotEnabled)?;

        progress
            .commit_offset(
                group_id,
                self.config.topic_id,
                self.config.partition_id,
                consumer_id,
                offset,
                current_time_us,
            )
            .await
            .map_err(|e| DurablePartitionError::Progress { source: e })
    }

    /// Registers a consumer with a consumer group.
    ///
    /// Must be called before leasing offsets or committing.
    ///
    /// # Errors
    ///
    /// Returns error if progress tracking is not enabled or storage fails.
    pub async fn register_consumer(
        &self,
        group_id: ConsumerGroupId,
        consumer_id: ConsumerId,
        current_time_us: u64,
    ) -> Result<(), DurablePartitionError> {
        let progress = self.progress.as_ref().ok_or(DurablePartitionError::ProgressNotEnabled)?;

        progress
            .register_consumer(group_id, consumer_id, current_time_us)
            .await
            .map_err(|e| DurablePartitionError::Progress { source: e })
    }

    /// Gets the safe eviction offset for this partition.
    ///
    /// Returns the minimum low watermark across all consumer groups tracking
    /// this partition. Segments with end offset below this value can be
    /// safely evicted from local storage.
    ///
    /// Returns `None` if no consumer groups are tracking this partition.
    ///
    /// # Errors
    ///
    /// Returns error if progress tracking is not enabled or storage fails.
    pub async fn get_safe_eviction_offset(&self) -> Result<Option<Offset>, DurablePartitionError> {
        // If progress tracking is not enabled, return None (no restriction on eviction).
        let Some(progress) = self.progress.as_ref() else {
            return Ok(None);
        };

        progress
            .get_safe_eviction_offset(self.config.topic_id, self.config.partition_id)
            .await
            .map_err(|e| DurablePartitionError::Progress { source: e })
    }

    /// Evicts eligible segments from local storage, respecting consumer progress.
    ///
    /// This method coordinates tiering and progress tracking:
    /// 1. Gets the safe eviction offset from progress (if enabled)
    /// 2. Only evicts segments whose `end_offset` is below the safe offset
    ///
    /// If progress tracking is not enabled, eviction proceeds based solely
    /// on tiering eligibility (segment must be in both local and remote storage).
    ///
    /// # Returns
    ///
    /// The number of segments evicted, or 0 if tiering is not enabled.
    ///
    /// # Errors
    ///
    /// Returns error if eviction operation fails.
    pub async fn evict_eligible_segments(&self) -> Result<u32, DurablePartitionError> {
        let Some(tiering) = &self.tiering else {
            return Ok(0);
        };

        // Get safe offset from progress (if enabled).
        let safe_offset = if let Some(progress) = &self.progress {
            progress
                .get_safe_eviction_offset(self.config.topic_id, self.config.partition_id)
                .await
                .map_err(|e| DurablePartitionError::Progress { source: e })?
        } else {
            // No progress tracking - allow eviction of any eligible segment.
            // Use max offset to indicate "no consumer constraints".
            Some(Offset::new(u64::MAX))
        };

        tiering
            .evict_with_progress(safe_offset)
            .await
            .map_err(|e| DurablePartitionError::Tiering {
                message: e.to_string(),
            })
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
    /// Progress tracking is not enabled.
    ProgressNotEnabled,
    /// Progress operation failed.
    Progress {
        /// The underlying progress error.
        source: ProgressError,
    },
    /// Tiering operation failed.
    Tiering {
        /// Error message.
        message: String,
    },
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
            Self::ProgressNotEnabled => write!(f, "progress tracking is not enabled"),
            Self::Progress { source } => write!(f, "progress error: {source}"),
            Self::Tiering { message } => write!(f, "tiering error: {message}"),
        }
    }
}

impl std::error::Error for DurablePartitionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_progress::ProgressStore;

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

    #[tokio::test]
    async fn test_durable_partition_with_tiering() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create partition with tiering enabled.
        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        )
        .with_tiering(TieringConfig::for_testing());

        let mut partition = DurablePartition::open(config).await.unwrap();

        // Verify tiering is enabled.
        assert!(partition.tiering_manager().is_some());

        // Write some records.
        let records = vec![
            Record::new(Bytes::from("value1")),
            Record::new(Bytes::from("value2")),
        ];
        partition.append(records).await.unwrap();

        // Call tiering hooks (no sealed segments yet, so should return 0).
        let registered = partition.check_and_register_sealed_segments().await.unwrap();
        assert_eq!(registered, 0);

        let committed = partition.on_entries_committed(10).await.unwrap();
        assert_eq!(committed, 0);

        let tiered = partition.tier_eligible_segments().await.unwrap();
        assert_eq!(tiered, 0);
    }

    #[tokio::test]
    async fn test_durable_partition_with_progress() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create partition with progress tracking enabled.
        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        )
        .with_progress(ProgressConfig::for_testing());

        let partition = DurablePartition::open(config).await.unwrap();

        // Verify progress is enabled.
        assert!(partition.has_progress());
        assert!(partition.progress_manager().is_some());
    }

    #[tokio::test]
    async fn test_durable_partition_consumer_registration() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        )
        .with_progress(ProgressConfig::for_testing());

        let partition = DurablePartition::open(config).await.unwrap();

        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(100);
        let current_time = 1000;

        // Register a consumer.
        partition
            .register_consumer(group_id, consumer_id, current_time)
            .await
            .unwrap();

        // Verify the group exists via progress manager store.
        let pm = partition.progress_manager().unwrap();
        let group = pm.store().get_group(group_id).await.unwrap();
        assert!(group.is_some());
        assert!(group.unwrap().consumers.contains_key(&consumer_id));
    }

    #[tokio::test]
    async fn test_durable_partition_lease_and_commit() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        )
        .with_progress(ProgressConfig::for_testing());

        let mut partition = DurablePartition::open(config).await.unwrap();

        // Write some records first.
        let records = vec![
            Record::new(Bytes::from("value1")),
            Record::new(Bytes::from("value2")),
            Record::new(Bytes::from("value3")),
        ];
        partition.append(records).await.unwrap();

        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(100);
        let current_time = 1000;
        let lease_duration = 60_000_000; // 60 seconds in microseconds.

        // Register consumer.
        partition
            .register_consumer(group_id, consumer_id, current_time)
            .await
            .unwrap();

        // Lease offsets (max_count=3 to match our 3 records).
        let lease = partition
            .lease_for_consumer(group_id, consumer_id, 3, lease_duration, current_time)
            .await
            .unwrap();

        assert!(lease.is_some());
        let lease = lease.unwrap();
        assert_eq!(lease.from_offset, Offset::new(0));
        assert_eq!(lease.to_offset, Offset::new(2)); // 3 records: 0, 1, 2

        // Commit the first offset.
        partition
            .commit_consumer_offset(group_id, consumer_id, Offset::new(0), current_time)
            .await
            .unwrap();

        // Verify progress.
        let pm = partition.progress_manager().unwrap();
        let partition_key = helix_progress::PartitionKey::new(TopicId::new(1), PartitionId::new(0));
        let progress = pm
            .store()
            .get_partition_progress(group_id, partition_key)
            .await
            .unwrap();
        assert!(progress.is_some());
        assert_eq!(progress.unwrap().low_watermark, Offset::new(1));
    }

    #[tokio::test]
    async fn test_durable_partition_safe_eviction_offset() {
        let temp_dir = tempfile::tempdir().unwrap();

        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        )
        .with_progress(ProgressConfig::for_testing());

        let mut partition = DurablePartition::open(config).await.unwrap();

        // Write records.
        let records = vec![
            Record::new(Bytes::from("value1")),
            Record::new(Bytes::from("value2")),
            Record::new(Bytes::from("value3")),
            Record::new(Bytes::from("value4")),
            Record::new(Bytes::from("value5")),
        ];
        partition.append(records).await.unwrap();

        let group1 = ConsumerGroupId::new(1);
        let group2 = ConsumerGroupId::new(2);
        let consumer1 = ConsumerId::new(100);
        let consumer2 = ConsumerId::new(200);
        let current_time = 1000;
        let lease_duration = 60_000_000; // 60 seconds in microseconds.

        // Register two consumer groups.
        partition
            .register_consumer(group1, consumer1, current_time)
            .await
            .unwrap();
        partition
            .register_consumer(group2, consumer2, current_time)
            .await
            .unwrap();

        // Group 1 leases and commits up to offset 3.
        let _lease1 = partition
            .lease_for_consumer(group1, consumer1, 10, lease_duration, current_time)
            .await
            .unwrap();
        for offset in 0..=3 {
            partition
                .commit_consumer_offset(group1, consumer1, Offset::new(offset), current_time)
                .await
                .unwrap();
        }

        // Group 2 leases and commits only up to offset 1.
        let _lease2 = partition
            .lease_for_consumer(group2, consumer2, 10, lease_duration, current_time)
            .await
            .unwrap();
        for offset in 0..=1 {
            partition
                .commit_consumer_offset(group2, consumer2, Offset::new(offset), current_time)
                .await
                .unwrap();
        }

        // Safe eviction offset should be min(group1_watermark, group2_watermark) = 2.
        let safe_offset = partition.get_safe_eviction_offset().await.unwrap();
        assert_eq!(safe_offset, Some(Offset::new(2)));
    }

    #[tokio::test]
    async fn test_durable_partition_progress_not_enabled_error() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create partition WITHOUT progress tracking.
        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        );

        let partition = DurablePartition::open(config).await.unwrap();

        // Attempting to use progress methods should fail.
        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(100);

        let result = partition
            .register_consumer(group_id, consumer_id, 1000)
            .await;
        assert!(matches!(
            result,
            Err(DurablePartitionError::ProgressNotEnabled)
        ));

        let result = partition
            .lease_for_consumer(group_id, consumer_id, 10, 60_000_000, 1000)
            .await;
        assert!(matches!(
            result,
            Err(DurablePartitionError::ProgressNotEnabled)
        ));

        let result = partition
            .commit_consumer_offset(group_id, consumer_id, Offset::new(0), 1000)
            .await;
        assert!(matches!(
            result,
            Err(DurablePartitionError::ProgressNotEnabled)
        ));

        // get_safe_eviction_offset returns None when progress is not enabled.
        let safe_offset = partition.get_safe_eviction_offset().await.unwrap();
        assert_eq!(safe_offset, None);
    }

    // -------------------------------------------------------------------------
    // Blob Storage Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_partition_blob_append_and_read() {
        let config = PartitionConfig::new(TopicId::new(1), PartitionId::new(0));
        let mut partition = Partition::new(config);

        // Append first blob (3 records).
        let blob1 = Bytes::from("kafka-batch-1-data");
        let base_offset1 = partition.append_blob(blob1.clone(), 3).unwrap();
        assert_eq!(base_offset1, Offset::new(0));
        assert_eq!(partition.blob_log_end_offset(), Offset::new(3));

        // Append second blob (2 records).
        let blob2 = Bytes::from("kafka-batch-2-data");
        let base_offset2 = partition.append_blob(blob2.clone(), 2).unwrap();
        assert_eq!(base_offset2, Offset::new(3));
        assert_eq!(partition.blob_log_end_offset(), Offset::new(5));

        // Read blobs from offset 0.
        let read_blobs = partition.read_blobs(Offset::new(0), 1000);
        assert_eq!(read_blobs.len(), 2);
        assert_eq!(read_blobs[0].data, blob1);
        assert_eq!(read_blobs[0].base_offset, Offset::new(0));
        assert_eq!(read_blobs[0].record_count, 3);
        assert_eq!(read_blobs[1].data, blob2);
        assert_eq!(read_blobs[1].base_offset, Offset::new(3));
        assert_eq!(read_blobs[1].record_count, 2);

        // Read blobs from offset 3 (should get second blob only).
        let read_blobs = partition.read_blobs(Offset::new(3), 1000);
        assert_eq!(read_blobs.len(), 1);
        assert_eq!(read_blobs[0].data, blob2);
    }

    #[test]
    fn test_partition_blob_max_bytes_limit() {
        let config = PartitionConfig::new(TopicId::new(1), PartitionId::new(0));
        let mut partition = Partition::new(config);

        // Append several blobs.
        for i in 0..5 {
            let blob = Bytes::from(format!("blob-{i}-with-some-data"));
            partition.append_blob(blob, 1).unwrap();
        }

        // Read with small max_bytes - should get at least one blob.
        let read_blobs = partition.read_blobs(Offset::new(0), 10);
        assert_eq!(read_blobs.len(), 1); // First blob is always included.

        // Read with larger max_bytes - should get more.
        let read_blobs = partition.read_blobs(Offset::new(0), 100);
        assert!(read_blobs.len() > 1);
    }

    #[test]
    fn test_partition_command_blob_roundtrip() {
        let blob = Bytes::from("test-kafka-batch-data");
        let cmd = PartitionCommand::AppendBlob {
            blob: blob.clone(),
            record_count: 5,
            format: BlobFormat::KafkaRecordBatch,
        };
        let encoded = cmd.encode();
        let decoded = PartitionCommand::decode(&encoded).unwrap();

        match decoded {
            PartitionCommand::AppendBlob {
                blob: decoded_blob,
                record_count,
                format,
            } => {
                assert_eq!(decoded_blob, blob);
                assert_eq!(record_count, 5);
                assert_eq!(format, BlobFormat::KafkaRecordBatch);
            }
            _ => panic!("wrong command type"),
        }
    }

    #[tokio::test]
    async fn test_durable_partition_blob_append_and_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = DurablePartitionConfig::new(
            temp_dir.path(),
            TopicId::new(1),
            PartitionId::new(0),
        );

        let mut partition = DurablePartition::open(config).await.unwrap();

        // Append blobs.
        let blob1 = Bytes::from("kafka-batch-1");
        let blob2 = Bytes::from("kafka-batch-2");

        let base_offset1 = partition.append_blob(blob1.clone(), 3).await.unwrap();
        assert_eq!(base_offset1, Offset::new(0));

        let base_offset2 = partition.append_blob(blob2.clone(), 2).await.unwrap();
        assert_eq!(base_offset2, Offset::new(3));

        assert_eq!(partition.blob_log_end_offset(), Offset::new(5));

        // Read blobs.
        let read_blobs = partition.read_blobs(Offset::new(0), 1000);
        assert_eq!(read_blobs.len(), 2);
        assert_eq!(read_blobs[0].data, blob1);
        assert_eq!(read_blobs[1].data, blob2);
    }

    #[tokio::test]
    async fn test_durable_partition_blob_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();

        // Write some blobs.
        {
            let config = DurablePartitionConfig::new(
                temp_dir.path(),
                TopicId::new(1),
                PartitionId::new(0),
            );

            let mut partition = DurablePartition::open(config).await.unwrap();

            for i in 0..5 {
                let blob = Bytes::from(format!("kafka-batch-{i}"));
                partition.append_blob(blob, 2).await.unwrap();
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

            // 5 blobs * 2 records each = 10 total offset advancement.
            assert_eq!(partition.blob_log_end_offset(), Offset::new(10));

            let read_blobs = partition.read_blobs(Offset::new(0), 10000);
            assert_eq!(read_blobs.len(), 5);

            // Verify blob contents.
            for (i, blob) in read_blobs.iter().enumerate() {
                assert_eq!(blob.data, Bytes::from(format!("kafka-batch-{i}")));
                assert_eq!(blob.record_count, 2);
            }
        }
    }

    #[test]
    fn test_stored_blob_offsets() {
        let blob = StoredBlob::new(Offset::new(10), 5, Bytes::from("data"));
        assert_eq!(blob.base_offset, Offset::new(10));
        assert_eq!(blob.last_offset(), Offset::new(14)); // 10 + 5 - 1
        assert_eq!(blob.next_offset(), Offset::new(15)); // 10 + 5

        // Empty blob edge case.
        let empty_blob = StoredBlob::new(Offset::new(10), 0, Bytes::new());
        assert_eq!(empty_blob.last_offset(), Offset::new(10));
        assert_eq!(empty_blob.next_offset(), Offset::new(10));
    }
}

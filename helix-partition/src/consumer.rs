//! Consumer API for reading records from partitions.
//!
//! The Consumer handles:
//! - Offset tracking (where to resume reading)
//! - Fetching records in batches
//! - Consumer group coordination (eventually)
//!
//! # Offset Management
//!
//! Consumers track their position in each partition. Offsets can be:
//! - Committed: Saved to storage, survives restarts
//! - Uncommitted: In-memory only, lost on restart
//!
//! # Auto-Offset Reset
//!
//! When no committed offset exists:
//! - `Earliest`: Start from beginning of partition
//! - `Latest`: Start from end of partition (only new records)

use std::collections::HashMap;

use helix_core::{ConsumerGroupId, ConsumerId, Offset, PartitionId, Record};

use crate::error::{PartitionError, PartitionResult};
use crate::partition::Partition;

/// Auto-offset reset policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoOffsetReset {
    /// Start from the earliest available offset.
    #[default]
    Earliest,
    /// Start from the latest offset (only new records).
    Latest,
    /// Fail if no offset is committed.
    None,
}

/// Configuration for the consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group ID.
    pub group_id: Option<ConsumerGroupId>,
    /// Consumer ID within the group.
    pub consumer_id: ConsumerId,
    /// Auto-offset reset policy.
    pub auto_offset_reset: AutoOffsetReset,
    /// Maximum records to fetch per poll.
    pub max_poll_records: u32,
    /// Maximum bytes to fetch per poll.
    pub max_poll_bytes: usize,
    /// Enable auto-commit of offsets.
    pub enable_auto_commit: bool,
    /// Auto-commit interval in milliseconds.
    pub auto_commit_interval_ms: u64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: None,
            consumer_id: ConsumerId::new(0),
            auto_offset_reset: AutoOffsetReset::Earliest,
            max_poll_records: 500,
            max_poll_bytes: 1024 * 1024, // 1 MB
            enable_auto_commit: true,
            auto_commit_interval_ms: 5000,
        }
    }
}

impl ConsumerConfig {
    /// Creates a config for a standalone consumer (no group).
    #[must_use]
    pub fn standalone(consumer_id: ConsumerId) -> Self {
        Self {
            consumer_id,
            group_id: None,
            ..Default::default()
        }
    }

    /// Creates a config for a consumer in a group.
    #[must_use]
    pub fn with_group(consumer_id: ConsumerId, group_id: ConsumerGroupId) -> Self {
        Self {
            consumer_id,
            group_id: Some(group_id),
            ..Default::default()
        }
    }
}

/// Result of a fetch/poll operation.
#[derive(Debug, Clone)]
pub struct FetchResult {
    /// Partition the records came from.
    pub partition_id: PartitionId,
    /// Records fetched.
    pub records: Vec<Record>,
    /// The high watermark of the partition.
    pub high_watermark: Offset,
    /// The log end offset of the partition.
    pub log_end_offset: Offset,
}

/// Tracks committed offsets.
#[derive(Debug, Default)]
pub struct OffsetStore {
    /// Committed offsets by partition.
    committed: HashMap<PartitionId, Offset>,
}

impl OffsetStore {
    /// Creates a new offset store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            committed: HashMap::new(),
        }
    }

    /// Gets the committed offset for a partition.
    #[must_use]
    pub fn get(&self, partition_id: PartitionId) -> Option<Offset> {
        self.committed.get(&partition_id).copied()
    }

    /// Commits an offset for a partition.
    pub fn commit(&mut self, partition_id: PartitionId, offset: Offset) {
        self.committed.insert(partition_id, offset);
    }

    /// Commits multiple offsets.
    pub fn commit_all(&mut self, offsets: &[(PartitionId, Offset)]) {
        for &(partition_id, offset) in offsets {
            self.committed.insert(partition_id, offset);
        }
    }

    /// Returns all committed offsets.
    #[must_use]
    pub const fn all(&self) -> &HashMap<PartitionId, Offset> {
        &self.committed
    }
}

/// Consumer for reading records from partitions.
pub struct Consumer {
    /// Configuration.
    config: ConsumerConfig,
    /// Assigned partitions.
    assignments: Vec<PartitionId>,
    /// Current position in each partition (next offset to fetch).
    positions: HashMap<PartitionId, Offset>,
    /// Committed offset store.
    offset_store: OffsetStore,
    /// Whether the consumer is paused.
    paused: HashMap<PartitionId, bool>,
}

impl Consumer {
    /// Creates a new consumer with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(ConsumerConfig::default())
    }

    /// Creates a new consumer with custom configuration.
    #[must_use]
    pub fn with_config(config: ConsumerConfig) -> Self {
        Self {
            config,
            assignments: Vec::new(),
            positions: HashMap::new(),
            offset_store: OffsetStore::new(),
            paused: HashMap::new(),
        }
    }

    /// Returns the consumer configuration.
    #[must_use]
    pub const fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    /// Assigns partitions to this consumer.
    pub fn assign(&mut self, partitions: Vec<PartitionId>) {
        self.assignments = partitions;
    }

    /// Returns the assigned partitions.
    #[must_use]
    pub fn assignments(&self) -> &[PartitionId] {
        &self.assignments
    }

    /// Seeks to a specific offset in a partition.
    pub fn seek(&mut self, partition_id: PartitionId, offset: Offset) {
        self.positions.insert(partition_id, offset);
    }

    /// Seeks to the beginning of a partition.
    pub fn seek_to_beginning(&mut self, partition_id: PartitionId, partition: &Partition) {
        self.positions
            .insert(partition_id, partition.log_start_offset());
    }

    /// Seeks to the end of a partition.
    pub fn seek_to_end(&mut self, partition_id: PartitionId, partition: &Partition) {
        self.positions
            .insert(partition_id, partition.log_end_offset());
    }

    /// Gets the current position for a partition.
    #[must_use]
    pub fn position(&self, partition_id: PartitionId) -> Option<Offset> {
        self.positions.get(&partition_id).copied()
    }

    /// Gets the committed offset for a partition.
    #[must_use]
    pub fn committed(&self, partition_id: PartitionId) -> Option<Offset> {
        self.offset_store.get(partition_id)
    }

    /// Commits the current position for a partition.
    pub fn commit(&mut self, partition_id: PartitionId) {
        if let Some(&offset) = self.positions.get(&partition_id) {
            self.offset_store.commit(partition_id, offset);
        }
    }

    /// Commits all current positions.
    pub fn commit_all(&mut self) {
        let offsets: Vec<_> = self.positions.iter().map(|(&p, &o)| (p, o)).collect();
        self.offset_store.commit_all(&offsets);
    }

    /// Commits a specific offset for a partition.
    pub fn commit_offset(&mut self, partition_id: PartitionId, offset: Offset) {
        self.offset_store.commit(partition_id, offset);
    }

    /// Pauses consumption from a partition.
    pub fn pause(&mut self, partition_id: PartitionId) {
        self.paused.insert(partition_id, true);
    }

    /// Resumes consumption from a partition.
    pub fn resume(&mut self, partition_id: PartitionId) {
        self.paused.remove(&partition_id);
    }

    /// Returns true if a partition is paused.
    #[must_use]
    pub fn is_paused(&self, partition_id: PartitionId) -> bool {
        self.paused.get(&partition_id).copied().unwrap_or(false)
    }

    /// Polls for records from assigned partitions.
    ///
    /// Returns records from all assigned (non-paused) partitions.
    pub fn poll(
        &mut self,
        partitions: &HashMap<PartitionId, Partition>,
    ) -> Vec<PartitionResult<FetchResult>> {
        let mut results = Vec::new();

        for &partition_id in &self.assignments.clone() {
            if self.is_paused(partition_id) {
                continue;
            }

            if let Some(partition) = partitions.get(&partition_id) {
                match self.fetch_from(partition_id, partition) {
                    Ok(result) => {
                        if !result.records.is_empty() {
                            results.push(Ok(result));
                        }
                    }
                    Err(e) => results.push(Err(e)),
                }
            }
        }

        results
    }

    /// Fetches records from a specific partition.
    ///
    /// # Errors
    /// Returns an error if offset is out of range or the partition is closed.
    pub fn fetch(
        &mut self,
        partition_id: PartitionId,
        partition: &Partition,
    ) -> PartitionResult<FetchResult> {
        self.fetch_from(partition_id, partition)
    }

    /// Internal fetch implementation.
    fn fetch_from(
        &mut self,
        partition_id: PartitionId,
        partition: &Partition,
    ) -> PartitionResult<FetchResult> {
        // Determine starting offset.
        let start_offset = match self.positions.get(&partition_id) {
            Some(&offset) => offset,
            None => {
                // No position, check committed offset or use auto-reset.
                match self.offset_store.get(partition_id) {
                    Some(offset) => offset,
                    None => match self.config.auto_offset_reset {
                        AutoOffsetReset::Earliest => partition.log_start_offset(),
                        AutoOffsetReset::Latest => partition.log_end_offset(),
                        AutoOffsetReset::None => {
                            return Err(PartitionError::OffsetOutOfRange {
                                offset: Offset::new(0),
                                first: partition.log_start_offset(),
                                last: partition.log_end_offset(),
                            });
                        }
                    },
                }
            }
        };

        // Check if we're at the end.
        if start_offset >= partition.log_end_offset() {
            return Ok(FetchResult {
                partition_id,
                records: Vec::new(),
                high_watermark: partition.high_watermark(),
                log_end_offset: partition.log_end_offset(),
            });
        }

        // Fetch records.
        let records = partition.read(start_offset, self.config.max_poll_records)?;

        // Update position.
        if let Some(last) = records.last() {
            self.positions.insert(partition_id, last.offset.next());
        }

        Ok(FetchResult {
            partition_id,
            records,
            high_watermark: partition.high_watermark(),
            log_end_offset: partition.log_end_offset(),
        })
    }

    /// Returns the lag (records behind) for a partition.
    ///
    /// Returns 0 if the consumer has no position set for this partition.
    #[must_use]
    pub fn lag(&self, partition_id: PartitionId, partition: &Partition) -> u64 {
        match self.positions.get(&partition_id) {
            Some(&position) => {
                let end = partition.log_end_offset();
                if end.get() > position.get() {
                    end.get() - position.get()
                } else {
                    0
                }
            }
            None => 0, // No position set, lag is unknown/not applicable
        }
    }
}

impl Default for Consumer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::partition::PartitionConfig;
    use helix_core::TopicId;

    fn make_partition_with_records(count: usize) -> Partition {
        let config = PartitionConfig::new(TopicId::new(1), PartitionId::new(0));
        let mut partition = Partition::new(config);

        let records: Vec<Record> = (0..count)
            .map(|i| Record::new(Bytes::from(format!("msg-{i}"))))
            .collect();
        partition.append(records).unwrap();

        partition
    }

    #[test]
    fn test_consumer_creation() {
        let consumer = Consumer::new();
        assert!(consumer.assignments().is_empty());
    }

    #[test]
    fn test_consumer_assign() {
        let mut consumer = Consumer::new();
        consumer.assign(vec![PartitionId::new(0), PartitionId::new(1)]);

        assert_eq!(consumer.assignments().len(), 2);
    }

    #[test]
    fn test_consumer_seek() {
        let mut consumer = Consumer::new();
        let partition_id = PartitionId::new(0);

        consumer.seek(partition_id, Offset::new(100));
        assert_eq!(consumer.position(partition_id), Some(Offset::new(100)));
    }

    #[test]
    fn test_consumer_fetch() {
        let mut consumer = Consumer::new();
        let partition_id = PartitionId::new(0);
        let partition = make_partition_with_records(5);

        let result = consumer.fetch(partition_id, &partition).unwrap();

        assert_eq!(result.records.len(), 5);
        assert_eq!(result.records[0].offset, Offset::new(0));
        assert_eq!(consumer.position(partition_id), Some(Offset::new(5)));
    }

    #[test]
    fn test_consumer_fetch_incremental() {
        let mut consumer = Consumer::with_config(ConsumerConfig {
            max_poll_records: 2,
            ..Default::default()
        });
        let partition_id = PartitionId::new(0);
        let partition = make_partition_with_records(5);

        // First fetch.
        let result1 = consumer.fetch(partition_id, &partition).unwrap();
        assert_eq!(result1.records.len(), 2);
        assert_eq!(consumer.position(partition_id), Some(Offset::new(2)));

        // Second fetch.
        let result2 = consumer.fetch(partition_id, &partition).unwrap();
        assert_eq!(result2.records.len(), 2);
        assert_eq!(consumer.position(partition_id), Some(Offset::new(4)));

        // Third fetch.
        let result3 = consumer.fetch(partition_id, &partition).unwrap();
        assert_eq!(result3.records.len(), 1);
        assert_eq!(consumer.position(partition_id), Some(Offset::new(5)));
    }

    #[test]
    fn test_consumer_commit() {
        let mut consumer = Consumer::new();
        let partition_id = PartitionId::new(0);

        consumer.seek(partition_id, Offset::new(10));
        consumer.commit(partition_id);

        assert_eq!(consumer.committed(partition_id), Some(Offset::new(10)));
    }

    #[test]
    fn test_consumer_pause_resume() {
        let mut consumer = Consumer::new();
        let partition_id = PartitionId::new(0);

        assert!(!consumer.is_paused(partition_id));

        consumer.pause(partition_id);
        assert!(consumer.is_paused(partition_id));

        consumer.resume(partition_id);
        assert!(!consumer.is_paused(partition_id));
    }

    #[test]
    fn test_consumer_lag() {
        let mut consumer = Consumer::new();
        let partition_id = PartitionId::new(0);
        let partition = make_partition_with_records(10);

        // No position, lag is 0.
        assert_eq!(consumer.lag(partition_id, &partition), 0);

        consumer.seek(partition_id, Offset::new(3));
        assert_eq!(consumer.lag(partition_id, &partition), 7);

        consumer.seek(partition_id, Offset::new(10));
        assert_eq!(consumer.lag(partition_id, &partition), 0);
    }

    #[test]
    fn test_auto_offset_reset_earliest() {
        let mut consumer = Consumer::with_config(ConsumerConfig {
            auto_offset_reset: AutoOffsetReset::Earliest,
            ..Default::default()
        });
        let partition_id = PartitionId::new(0);
        let partition = make_partition_with_records(5);

        // No position or committed offset, should start from beginning.
        let result = consumer.fetch(partition_id, &partition).unwrap();
        assert_eq!(result.records[0].offset, Offset::new(0));
    }

    #[test]
    fn test_auto_offset_reset_latest() {
        let mut consumer = Consumer::with_config(ConsumerConfig {
            auto_offset_reset: AutoOffsetReset::Latest,
            ..Default::default()
        });
        let partition_id = PartitionId::new(0);
        let partition = make_partition_with_records(5);

        // No position or committed offset, should start from end (no records).
        let result = consumer.fetch(partition_id, &partition).unwrap();
        assert!(result.records.is_empty());
    }
}

//! Producer API for appending records to partitions.
//!
//! The Producer handles:
//! - Batching records for efficient writes
//! - Partitioning (selecting which partition to write to)
//! - Acknowledgment tracking
//!
//! # Acknowledgment Modes
//!
//! - `Acks::None`: Fire and forget (fastest, least durable)
//! - `Acks::Leader`: Wait for leader to write (balanced)
//! - `Acks::All`: Wait for all replicas (slowest, most durable)

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use helix_core::{Offset, PartitionId, Record, RecordBatch, TopicId};

use crate::error::{PartitionError, PartitionResult};
use crate::partition::Partition;

/// Acknowledgment level for produce requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// No acknowledgment (fire and forget).
    None,
    /// Wait for leader to acknowledge.
    #[default]
    Leader,
    /// Wait for all in-sync replicas.
    All,
}

/// Result of a produce request.
#[derive(Debug, Clone)]
pub struct ProduceResult {
    /// Topic the records were written to.
    pub topic_id: TopicId,
    /// Partition the records were written to.
    pub partition_id: PartitionId,
    /// Offset of the first record.
    pub base_offset: Offset,
    /// Number of records written.
    pub record_count: u32,
    /// Timestamp assigned by the broker (if applicable).
    pub timestamp: Option<i64>,
}

/// Partitioner strategy for selecting partitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Partitioner {
    /// Round-robin across partitions.
    #[default]
    RoundRobin,
    /// Hash the key to select partition.
    KeyHash,
    /// Sticky partitioner (batch to same partition).
    Sticky,
    /// Random partition selection.
    Random,
}

/// Configuration for the producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Acknowledgment level.
    pub acks: Acks,
    /// Partitioner strategy.
    pub partitioner: Partitioner,
    /// Maximum batch size in bytes.
    pub max_batch_bytes: usize,
    /// Maximum number of records per batch.
    pub max_batch_records: usize,
    /// Linger time in milliseconds (wait before sending).
    pub linger_ms: u64,
    /// Request timeout in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: Acks::Leader,
            partitioner: Partitioner::RoundRobin,
            max_batch_bytes: 16 * 1024, // 16 KB
            max_batch_records: 1000,
            linger_ms: 5,
            request_timeout_ms: 30_000,
        }
    }
}

/// A pending batch being accumulated.
#[derive(Debug)]
struct PendingBatch {
    /// The batch being built.
    batch: RecordBatch,
    /// Current size in bytes.
    size_bytes: usize,
}

impl PendingBatch {
    fn new(partition_id: PartitionId) -> Self {
        Self {
            batch: RecordBatch::new(partition_id),
            size_bytes: 0,
        }
    }

    fn can_add(&self, record_size: usize, config: &ProducerConfig) -> bool {
        self.batch.len() < config.max_batch_records
            && self.size_bytes + record_size <= config.max_batch_bytes
    }

    fn add(&mut self, record: Record) {
        self.size_bytes += record.size();
        self.batch.push(record);
    }
}

/// Producer for writing records to partitions.
///
/// The producer accumulates records into batches and writes them to
/// the appropriate partitions.
pub struct Producer {
    /// Configuration.
    config: ProducerConfig,
    /// Pending batches by partition.
    pending: HashMap<PartitionId, PendingBatch>,
    /// Round-robin counter.
    round_robin_counter: usize,
    /// Sticky partition (for sticky partitioner).
    sticky_partition: Option<PartitionId>,
}

impl Producer {
    /// Creates a new producer with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(ProducerConfig::default())
    }

    /// Creates a new producer with custom configuration.
    #[must_use]
    pub fn with_config(config: ProducerConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
            round_robin_counter: 0,
            sticky_partition: None,
        }
    }

    /// Returns the producer configuration.
    #[must_use]
    pub fn config(&self) -> &ProducerConfig {
        &self.config
    }

    /// Sends a record to a partition.
    ///
    /// The record is added to a pending batch. Call `flush` to write
    /// all pending batches to their partitions.
    ///
    /// # Arguments
    /// * `partition` - The partition to write to
    /// * `record` - The record to write
    ///
    /// # Returns
    /// `true` if the record was added to a batch, `false` if the batch is full.
    pub fn send(&mut self, partition_id: PartitionId, record: Record) -> bool {
        let record_size = record.size();

        // Get or create pending batch for this partition.
        let pending = self
            .pending
            .entry(partition_id)
            .or_insert_with(|| PendingBatch::new(partition_id));

        if pending.can_add(record_size, &self.config) {
            pending.add(record);
            true
        } else {
            false
        }
    }

    /// Sends a record, automatically selecting a partition.
    ///
    /// Uses the configured partitioner to select the target partition.
    pub fn send_auto(
        &mut self,
        partition_count: u32,
        record: Record,
    ) -> PartitionId {
        let partition_id = self.select_partition(&record, partition_count);
        self.send(partition_id, record);
        partition_id
    }

    /// Selects a partition for a record based on the partitioner strategy.
    fn select_partition(&mut self, record: &Record, partition_count: u32) -> PartitionId {
        if partition_count == 0 {
            return PartitionId::new(0);
        }

        match self.config.partitioner {
            Partitioner::RoundRobin => {
                let partition = self.round_robin_counter % partition_count as usize;
                self.round_robin_counter = self.round_robin_counter.wrapping_add(1);
                PartitionId::new(partition as u64)
            }
            Partitioner::KeyHash => {
                if let Some(ref key) = record.key {
                    let mut hasher = std::collections::hash_map::DefaultHasher::new();
                    key.hash(&mut hasher);
                    let hash = hasher.finish();
                    PartitionId::new(hash % partition_count as u64)
                } else {
                    // No key, fall back to round-robin.
                    let partition = self.round_robin_counter % partition_count as usize;
                    self.round_robin_counter = self.round_robin_counter.wrapping_add(1);
                    PartitionId::new(partition as u64)
                }
            }
            Partitioner::Sticky => {
                // Use sticky partition until batch is full.
                if let Some(partition) = self.sticky_partition {
                    partition
                } else {
                    let partition = PartitionId::new(
                        (self.round_robin_counter % partition_count as usize) as u64,
                    );
                    self.sticky_partition = Some(partition);
                    partition
                }
            }
            Partitioner::Random => {
                // Simple pseudo-random (not cryptographically secure).
                let partition = (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .subsec_nanos() as u64)
                    % partition_count as u64;
                PartitionId::new(partition)
            }
        }
    }

    /// Flushes all pending batches to the given partitions.
    ///
    /// # Returns
    /// Results for each flushed batch.
    pub fn flush(
        &mut self,
        partitions: &mut HashMap<PartitionId, Partition>,
    ) -> Vec<PartitionResult<ProduceResult>> {
        let mut results = Vec::new();

        // Drain all pending batches.
        let batches: Vec<_> = self.pending.drain().collect();

        for (partition_id, pending) in batches {
            if pending.batch.is_empty() {
                continue;
            }

            let record_count = pending.batch.len() as u32;

            if let Some(partition) = partitions.get_mut(&partition_id) {
                match partition.append_batch(pending.batch) {
                    Ok(base_offset) => {
                        results.push(Ok(ProduceResult {
                            topic_id: partition.topic_id(),
                            partition_id,
                            base_offset,
                            record_count,
                            timestamp: None,
                        }));
                    }
                    Err(e) => {
                        results.push(Err(e));
                    }
                }
            } else {
                results.push(Err(PartitionError::InvalidConfig {
                    message: format!("partition {} not found", partition_id.get()),
                }));
            }
        }

        // Reset sticky partition after flush.
        self.sticky_partition = None;

        results
    }

    /// Returns the number of pending batches.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns the total number of pending records.
    #[must_use]
    pub fn pending_records(&self) -> usize {
        self.pending.values().map(|p| p.batch.len()).sum()
    }
}

impl Default for Producer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::partition::PartitionConfig;

    fn make_record(value: &str) -> Record {
        Record::new(Bytes::from(value.to_string()))
    }

    fn make_keyed_record(key: &str, value: &str) -> Record {
        Record::with_key(Bytes::from(key.to_string()), Bytes::from(value.to_string()))
    }

    #[test]
    fn test_producer_send() {
        let mut producer = Producer::new();
        let partition = PartitionId::new(0);

        assert!(producer.send(partition, make_record("test1")));
        assert!(producer.send(partition, make_record("test2")));

        assert_eq!(producer.pending_count(), 1);
        assert_eq!(producer.pending_records(), 2);
    }

    #[test]
    fn test_producer_flush() {
        let mut producer = Producer::new();
        let partition_id = PartitionId::new(0);

        producer.send(partition_id, make_record("msg1"));
        producer.send(partition_id, make_record("msg2"));

        let config = PartitionConfig::new(TopicId::new(1), partition_id);
        let mut partitions = HashMap::new();
        partitions.insert(partition_id, Partition::new(config));

        let results = producer.flush(&mut partitions);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        let result = results[0].as_ref().unwrap();
        assert_eq!(result.partition_id, partition_id);
        assert_eq!(result.record_count, 2);
        assert_eq!(result.base_offset, Offset::new(0));
    }

    #[test]
    fn test_round_robin_partitioner() {
        let mut producer = Producer::with_config(ProducerConfig {
            partitioner: Partitioner::RoundRobin,
            ..Default::default()
        });

        let p1 = producer.send_auto(3, make_record("a"));
        let p2 = producer.send_auto(3, make_record("b"));
        let p3 = producer.send_auto(3, make_record("c"));
        let p4 = producer.send_auto(3, make_record("d"));

        assert_eq!(p1.get(), 0);
        assert_eq!(p2.get(), 1);
        assert_eq!(p3.get(), 2);
        assert_eq!(p4.get(), 0);
    }

    #[test]
    fn test_key_hash_partitioner() {
        let mut producer = Producer::with_config(ProducerConfig {
            partitioner: Partitioner::KeyHash,
            ..Default::default()
        });

        // Same key should always go to same partition.
        let p1 = producer.send_auto(10, make_keyed_record("user-123", "a"));
        let p2 = producer.send_auto(10, make_keyed_record("user-123", "b"));
        let _p3 = producer.send_auto(10, make_keyed_record("user-456", "c"));

        assert_eq!(p1, p2); // Same key
        // _p3 might be different (depends on hash)
    }

    #[test]
    fn test_sticky_partitioner() {
        let mut producer = Producer::with_config(ProducerConfig {
            partitioner: Partitioner::Sticky,
            ..Default::default()
        });

        // All records should go to same partition until flush.
        let p1 = producer.send_auto(3, make_record("a"));
        let p2 = producer.send_auto(3, make_record("b"));
        let p3 = producer.send_auto(3, make_record("c"));

        assert_eq!(p1, p2);
        assert_eq!(p2, p3);
    }
}

//! Partition management.
//!
//! A partition is a totally ordered, immutable sequence of records.
//! Each partition is backed by one or more log segments.

use helix_core::{Offset, PartitionId, Record, RecordBatch, TopicId};

use crate::error::{PartitionError, PartitionResult};
use crate::log::{LogSegment, SegmentConfig};

/// Configuration for a partition.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID within the topic.
    pub partition_id: PartitionId,
    /// Segment configuration.
    pub segment_config: SegmentConfig,
    /// Maximum number of segments to keep.
    pub max_segments: u32,
}

impl PartitionConfig {
    /// Creates a new partition configuration.
    #[must_use]
    pub fn new(topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            topic_id,
            partition_id,
            segment_config: SegmentConfig::default(),
            max_segments: 100,
        }
    }

    /// Sets the segment configuration.
    #[must_use]
    pub const fn with_segment_config(mut self, config: SegmentConfig) -> Self {
        self.segment_config = config;
        self
    }
}

/// Partition metadata.
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID.
    pub partition_id: PartitionId,
    /// First offset in the partition (log start).
    pub log_start_offset: Offset,
    /// Last offset in the partition + 1 (log end).
    pub log_end_offset: Offset,
    /// High watermark (last committed offset).
    pub high_watermark: Offset,
    /// Leader epoch for replication.
    pub leader_epoch: u32,
    /// Number of segments.
    pub segment_count: u32,
    /// Total size in bytes.
    pub size_bytes: u64,
}

/// A partition containing records.
///
/// The partition manages multiple log segments and provides a unified
/// view of the record stream.
#[derive(Debug)]
pub struct Partition {
    /// Configuration.
    config: PartitionConfig,
    /// Log segments (ordered by base offset).
    segments: Vec<LogSegment>,
    /// Active segment for writing.
    active_segment_idx: usize,
    /// High watermark (committed offset).
    high_watermark: Offset,
    /// Leader epoch.
    leader_epoch: u32,
    /// Whether the partition is closed.
    closed: bool,
}

impl Partition {
    /// Creates a new partition.
    #[must_use]
    pub fn new(config: PartitionConfig) -> Self {
        // Create initial segment starting at offset 0.
        let segment = LogSegment::new(Offset::new(0), config.segment_config);

        Self {
            config,
            segments: vec![segment],
            active_segment_idx: 0,
            high_watermark: Offset::new(0),
            leader_epoch: 0,
            closed: false,
        }
    }

    /// Returns the partition configuration.
    #[must_use]
    pub const fn config(&self) -> &PartitionConfig {
        &self.config
    }

    /// Returns the partition ID.
    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.config.partition_id
    }

    /// Returns the topic ID.
    #[must_use]
    pub const fn topic_id(&self) -> TopicId {
        self.config.topic_id
    }

    /// Returns the log start offset (first available).
    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        self.segments
            .first()
            .map_or(Offset::new(0), LogSegment::base_offset)
    }

    /// Returns the log end offset (next offset to be assigned).
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        self.segments
            .last()
            .map_or(Offset::new(0), LogSegment::next_offset)
    }

    /// Returns the high watermark (last committed offset).
    #[must_use]
    pub const fn high_watermark(&self) -> Offset {
        self.high_watermark
    }

    /// Sets the high watermark.
    pub fn set_high_watermark(&mut self, hwm: Offset) {
        self.high_watermark = hwm;
    }

    /// Returns the leader epoch.
    #[must_use]
    pub const fn leader_epoch(&self) -> u32 {
        self.leader_epoch
    }

    /// Increments the leader epoch.
    pub fn increment_epoch(&mut self) {
        self.leader_epoch += 1;
    }

    /// Returns partition metadata.
    #[must_use]
    pub fn metadata(&self) -> PartitionMetadata {
        let size_bytes: u64 = self.segments.iter().map(LogSegment::size_bytes).sum();

        // Safe cast: segment count is bounded by config.max_segments which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let segment_count = self.segments.len() as u32;

        PartitionMetadata {
            topic_id: self.config.topic_id,
            partition_id: self.config.partition_id,
            log_start_offset: self.log_start_offset(),
            log_end_offset: self.log_end_offset(),
            high_watermark: self.high_watermark,
            leader_epoch: self.leader_epoch,
            segment_count,
            size_bytes,
        }
    }

    /// Appends records to the partition.
    ///
    /// # Returns
    /// The offset of the first record in the batch.
    ///
    /// # Errors
    /// Returns an error if the partition is closed or the append fails.
    pub fn append(&mut self, records: Vec<Record>) -> PartitionResult<Offset> {
        if self.closed {
            return Err(PartitionError::Closed);
        }

        if records.is_empty() {
            return Ok(self.log_end_offset());
        }

        let prev_end_offset = self.log_end_offset();
        let record_count = records.len();

        // Create a batch.
        let mut batch = RecordBatch::new(self.config.partition_id);
        for record in records {
            batch.push(record);
        }

        let result = self.append_batch(batch)?;

        // Postcondition: log end offset advanced by record count.
        debug_assert!(self.log_end_offset().get() == prev_end_offset.get() + record_count as u64);

        Ok(result)
    }

    /// Appends a record batch to the partition.
    ///
    /// # Returns
    /// The offset of the first record in the batch.
    ///
    /// # Errors
    /// Returns an error if the partition is closed or the append fails.
    pub fn append_batch(&mut self, mut batch: RecordBatch) -> PartitionResult<Offset> {
        // Precondition: partition must be open.
        debug_assert!(!self.closed);

        if self.closed {
            return Err(PartitionError::Closed);
        }

        // Precondition: batch must not be empty.
        debug_assert!(!batch.is_empty());

        let batch_size = batch.size();
        let record_count = batch.len() as u64;

        // Check if active segment has space.
        let active = &self.segments[self.active_segment_idx];
        if !active.has_space_for(batch_size, record_count) {
            // Roll to a new segment.
            self.roll_segment();
        }

        // Invariant: active segment index is valid.
        debug_assert!(self.active_segment_idx < self.segments.len());

        // Append to active segment.
        let active = &mut self.segments[self.active_segment_idx];
        active.append(&mut batch)
    }

    /// Rolls to a new segment.
    fn roll_segment(&mut self) {
        // Seal current segment.
        self.segments[self.active_segment_idx].seal();

        // Postcondition: current segment is sealed.
        debug_assert!(self.segments[self.active_segment_idx].is_sealed());

        // Create new segment.
        let next_offset = self.log_end_offset();
        let segment = LogSegment::new(next_offset, self.config.segment_config);

        self.segments.push(segment);
        self.active_segment_idx = self.segments.len() - 1;

        // Clean up old segments if we have too many.
        self.cleanup_old_segments();

        // Postcondition: we have at least as many segments (may have cleaned up).
        debug_assert!(!self.segments.is_empty());
        debug_assert!(self.active_segment_idx < self.segments.len());
    }

    /// Removes old segments if we exceed the limit.
    fn cleanup_old_segments(&mut self) {
        // Bounded loop: at most (segments.len() - max_segments) iterations.
        while self.segments.len() > self.config.max_segments as usize {
            // Remove the oldest segment.
            self.segments.remove(0);
            self.active_segment_idx = self.active_segment_idx.saturating_sub(1);
        }

        // Postcondition: segment count within limit.
        debug_assert!(self.segments.len() <= self.config.max_segments as usize);
    }

    /// Reads records starting at the given offset.
    ///
    /// # Errors
    /// Returns an error if the offset is out of range.
    pub fn read(&self, start_offset: Offset, max_records: u32) -> PartitionResult<Vec<Record>> {
        // Precondition: max_records should be positive for useful reads.
        debug_assert!(max_records > 0);

        if self.closed {
            return Err(PartitionError::Closed);
        }

        let log_start = self.log_start_offset();
        let log_end = self.log_end_offset();

        if start_offset < log_start || start_offset >= log_end {
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

        // Find the segment containing start_offset.
        let segment_idx = self.find_segment(start_offset);

        // Invariant: segment_idx must be valid.
        debug_assert!(segment_idx < self.segments.len());

        let mut records = Vec::new();
        let mut remaining = max_records;
        let mut current_offset = start_offset;

        // Bounded loop: iterates over segments starting from segment_idx.
        for segment in &self.segments[segment_idx..] {
            if remaining == 0 {
                break;
            }

            let segment_records = segment.read(current_offset, remaining)?;
            // Safe cast: segment_records.len() <= remaining which is u32.
            #[allow(clippy::cast_possible_truncation)]
            let records_len = segment_records.len() as u32;
            remaining -= records_len;

            if let Some(last) = segment_records.last() {
                current_offset = last.offset.next();
            }

            records.extend(segment_records);
        }

        // Postcondition: returned at most max_records.
        debug_assert!(records.len() <= max_records as usize);

        Ok(records)
    }

    /// Finds the segment containing the given offset.
    fn find_segment(&self, offset: Offset) -> usize {
        // Precondition: segments must not be empty.
        debug_assert!(!self.segments.is_empty());

        // Binary search for the segment.
        let result = self.segments
            .partition_point(|s| s.next_offset() <= offset)
            .saturating_sub(1)
            .min(self.segments.len() - 1);

        // Postcondition: result is a valid index.
        debug_assert!(result < self.segments.len());

        result
    }

    /// Truncates the partition to the given offset (exclusive).
    ///
    /// Removes all records with offset >= `from_offset`.
    ///
    /// # Errors
    /// Returns an error if truncation fails.
    pub fn truncate(&mut self, from_offset: Offset) -> PartitionResult<()> {
        if self.closed {
            return Err(PartitionError::Closed);
        }

        // Find the segment containing from_offset.
        let segment_idx = self.find_segment(from_offset);

        // Remove all segments after this one.
        while self.segments.len() > segment_idx + 1 {
            self.segments.pop();
        }

        // Truncate the segment.
        self.segments[segment_idx].truncate(from_offset)?;

        // Update active segment.
        self.active_segment_idx = segment_idx;

        // Update high watermark if necessary.
        if self.high_watermark > from_offset {
            self.high_watermark = from_offset;
        }

        Ok(())
    }

    /// Closes the partition.
    pub fn close(&mut self) {
        self.closed = true;
        // Seal all segments.
        for segment in &mut self.segments {
            segment.seal();
        }
    }

    /// Returns true if the partition is closed.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_config() -> PartitionConfig {
        PartitionConfig::new(TopicId::new(1), PartitionId::new(0))
    }

    fn make_records(count: usize) -> Vec<Record> {
        (0..count)
            .map(|i| Record::new(Bytes::from(format!("message-{i}"))))
            .collect()
    }

    #[test]
    fn test_partition_creation() {
        let partition = Partition::new(make_config());
        assert_eq!(partition.log_start_offset(), Offset::new(0));
        assert_eq!(partition.log_end_offset(), Offset::new(0));
        assert_eq!(partition.high_watermark(), Offset::new(0));
    }

    #[test]
    fn test_partition_append() {
        let mut partition = Partition::new(make_config());

        let offset = partition.append(make_records(3)).unwrap();
        assert_eq!(offset, Offset::new(0));
        assert_eq!(partition.log_end_offset(), Offset::new(3));
    }

    #[test]
    fn test_partition_append_multiple() {
        let mut partition = Partition::new(make_config());

        partition.append(make_records(2)).unwrap();
        let offset = partition.append(make_records(3)).unwrap();

        assert_eq!(offset, Offset::new(2));
        assert_eq!(partition.log_end_offset(), Offset::new(5));
    }

    #[test]
    fn test_partition_read() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(5)).unwrap();

        let records = partition.read(Offset::new(0), 10).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, Offset::new(0));
        assert_eq!(records[4].offset, Offset::new(4));
    }

    #[test]
    fn test_partition_read_partial() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(10)).unwrap();

        let records = partition.read(Offset::new(3), 4).unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].offset, Offset::new(3));
        assert_eq!(records[3].offset, Offset::new(6));
    }

    #[test]
    fn test_partition_read_out_of_range() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(5)).unwrap();

        // Before log start.
        // (Can't test easily since we start at 0)

        // After log end.
        let result = partition.read(Offset::new(10), 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_metadata() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(5)).unwrap();
        partition.set_high_watermark(Offset::new(3));

        let meta = partition.metadata();
        assert_eq!(meta.log_start_offset, Offset::new(0));
        assert_eq!(meta.log_end_offset, Offset::new(5));
        assert_eq!(meta.high_watermark, Offset::new(3));
        assert_eq!(meta.segment_count, 1);
    }

    #[test]
    fn test_partition_truncate() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(10)).unwrap();
        partition.set_high_watermark(Offset::new(8));

        partition.truncate(Offset::new(5)).unwrap();

        assert_eq!(partition.log_end_offset(), Offset::new(5));
        assert_eq!(partition.high_watermark(), Offset::new(5));
    }

    #[test]
    fn test_partition_close() {
        let mut partition = Partition::new(make_config());
        partition.append(make_records(3)).unwrap();
        partition.close();

        assert!(partition.is_closed());

        let result = partition.append(make_records(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_partition_leader_epoch() {
        let mut partition = Partition::new(make_config());
        assert_eq!(partition.leader_epoch(), 0);

        partition.increment_epoch();
        assert_eq!(partition.leader_epoch(), 1);
    }
}

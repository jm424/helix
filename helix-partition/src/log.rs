//! Log segment management.
//!
//! A log segment is a contiguous portion of the partition log,
//! stored as a single WAL segment. Segments are immutable once
//! rolled (closed for writes).

use bytes::{BufMut, BytesMut};
use helix_core::{Offset, Record, RecordBatch, Timestamp};

use crate::error::{PartitionError, PartitionResult};

/// Configuration for log segments.
#[derive(Debug, Clone, Copy)]
pub struct SegmentConfig {
    /// Maximum size of a segment in bytes.
    pub max_size_bytes: u64,
    /// Maximum number of records in a segment.
    pub max_records: u64,
    /// Maximum age of a segment before rolling (in milliseconds).
    pub max_age_ms: u64,
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 1024 * 1024 * 1024, // 1 GB
            max_records: 10_000_000,            // 10M records
            max_age_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
        }
    }
}

/// Index entry for fast offset lookups.
#[derive(Debug, Clone, Copy)]
struct IndexEntry {
    /// Offset of this entry.
    offset: Offset,
    /// Position in the segment data.
    position: u64,
}

/// A single log segment containing records.
#[derive(Debug)]
pub struct LogSegment {
    /// Base offset (first offset in this segment).
    base_offset: Offset,
    /// Next offset to be assigned.
    next_offset: Offset,
    /// Configuration.
    config: SegmentConfig,
    /// Segment data (encoded records).
    data: BytesMut,
    /// Sparse index for offset lookups.
    index: Vec<IndexEntry>,
    /// Index interval (create index entry every N records).
    index_interval: u32,
    /// Records since last index entry.
    records_since_index: u32,
    /// Creation timestamp (for segment aging).
    #[allow(dead_code)]
    created_at: Timestamp,
    /// Whether the segment is sealed (read-only).
    sealed: bool,
}

impl LogSegment {
    /// Creates a new log segment starting at the given offset.
    #[must_use]
    pub fn new(base_offset: Offset, config: SegmentConfig) -> Self {
        Self {
            base_offset,
            next_offset: base_offset,
            config,
            data: BytesMut::new(),
            index: Vec::new(),
            index_interval: 4096, // Index every 4K records
            records_since_index: 0,
            created_at: Timestamp::now(),
            sealed: false,
        }
    }

    /// Returns the base offset of this segment.
    #[must_use]
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Returns the next offset that will be assigned.
    #[must_use]
    pub fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// Returns the last offset in this segment, or None if empty.
    #[must_use]
    pub fn last_offset(&self) -> Option<Offset> {
        if self.next_offset.get() > self.base_offset.get() {
            Some(Offset::new(self.next_offset.get() - 1))
        } else {
            None
        }
    }

    /// Returns the number of records in this segment.
    #[must_use]
    pub fn record_count(&self) -> u64 {
        self.next_offset.get() - self.base_offset.get()
    }

    /// Returns the size of the segment data in bytes.
    #[must_use]
    pub fn size_bytes(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns true if the segment is sealed (read-only).
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Returns true if the segment has space for the given batch.
    #[must_use]
    pub fn has_space_for(&self, batch_size: usize, record_count: u64) -> bool {
        if self.sealed {
            return false;
        }

        let new_size = self.data.len() as u64 + batch_size as u64;
        let new_count = self.record_count() + record_count;

        new_size <= self.config.max_size_bytes && new_count <= self.config.max_records
    }

    /// Appends a batch of records to the segment.
    ///
    /// # Errors
    /// Returns an error if the segment is full or sealed.
    pub fn append(&mut self, batch: &mut RecordBatch) -> PartitionResult<Offset> {
        if self.sealed {
            return Err(PartitionError::SegmentFull {
                reason: "segment is sealed",
            });
        }

        let batch_size = batch.size();
        let record_count = batch.len() as u64;

        if !self.has_space_for(batch_size, record_count) {
            return Err(PartitionError::SegmentFull {
                reason: "size or record limit reached",
            });
        }

        // Assign offsets to the batch.
        batch.assign_offsets(self.next_offset);
        let first_offset = self.next_offset;

        // Add index entry if needed.
        if self.records_since_index >= self.index_interval {
            self.index.push(IndexEntry {
                offset: first_offset,
                position: self.data.len() as u64,
            });
            self.records_since_index = 0;
        }

        // Encode and append the batch.
        self.encode_batch(batch);

        // Update state.
        self.next_offset = Offset::new(self.next_offset.get() + record_count);
        self.records_since_index += record_count as u32;

        Ok(first_offset)
    }

    /// Encodes a batch to the segment data.
    fn encode_batch(&mut self, batch: &RecordBatch) {
        // Batch header.
        self.data.put_u64_le(batch.base_offset.get());
        self.data.put_i64_le(batch.base_timestamp.as_millis());
        self.data.put_i64_le(batch.max_timestamp.as_millis());
        self.data.put_u8(batch.compression as u8);
        self.data.put_u32_le(batch.records.len() as u32);

        // CRC placeholder (we'll compute it over the records).
        let crc_pos = self.data.len();
        self.data.put_u32_le(0);

        // Encode records and compute CRC.
        let records_start = self.data.len();
        for record in &batch.records {
            record.encode(&mut self.data);
        }

        // Compute and write CRC.
        let crc = crc32fast::hash(&self.data[records_start..]);
        let crc_bytes = crc.to_le_bytes();
        self.data[crc_pos..crc_pos + 4].copy_from_slice(&crc_bytes);
    }

    /// Reads records starting at the given offset.
    ///
    /// # Errors
    /// Returns an error if the offset is out of range.
    pub fn read(&self, start_offset: Offset, max_records: u32) -> PartitionResult<Vec<Record>> {
        if start_offset < self.base_offset {
            return Err(PartitionError::OffsetOutOfRange {
                offset: start_offset,
                first: self.base_offset,
                last: self.last_offset().unwrap_or(self.base_offset),
            });
        }

        if let Some(last) = self.last_offset() {
            if start_offset > last {
                return Err(PartitionError::OffsetOutOfRange {
                    offset: start_offset,
                    first: self.base_offset,
                    last,
                });
            }
        } else {
            // Empty segment.
            return Ok(Vec::new());
        }

        // Find starting position using index.
        let position = self.find_position(start_offset);

        // Read records from position.
        let mut reader = SegmentReader::new(&self.data[position as usize..]);
        let mut records = Vec::new();
        let mut count = 0u32;

        while count < max_records {
            match reader.read_batch() {
                Ok(batch) => {
                    for record in batch.records {
                        if record.offset >= start_offset {
                            records.push(record);
                            count += 1;
                            if count >= max_records {
                                break;
                            }
                        }
                    }
                }
                Err(_) => break, // End of data or error.
            }
        }

        Ok(records)
    }

    /// Finds the data position for a given offset using the index.
    fn find_position(&self, offset: Offset) -> u64 {
        // Binary search the index for the closest entry <= offset.
        let idx = self.index.partition_point(|e| e.offset <= offset);
        if idx == 0 {
            0
        } else {
            self.index[idx - 1].position
        }
    }

    /// Seals the segment, preventing further writes.
    pub fn seal(&mut self) {
        self.sealed = true;
    }

    /// Truncates the segment to the given offset (exclusive).
    ///
    /// Removes all records with offset >= `from_offset`.
    ///
    /// # Errors
    /// Returns an error if the segment is sealed.
    pub fn truncate(&mut self, from_offset: Offset) -> PartitionResult<()> {
        if self.sealed {
            return Err(PartitionError::SegmentFull {
                reason: "cannot truncate sealed segment",
            });
        }

        if from_offset <= self.base_offset {
            // Truncate everything.
            self.data.clear();
            self.index.clear();
            self.next_offset = self.base_offset;
            self.records_since_index = 0;
            return Ok(());
        }

        if from_offset >= self.next_offset {
            // Nothing to truncate.
            return Ok(());
        }

        // Find the position to truncate at.
        // This is a simplified implementation - just truncate at the index position.
        let position = self.find_position(from_offset);

        // Truncate data and index.
        self.data.truncate(position as usize);
        self.index.retain(|e| e.offset < from_offset);
        self.next_offset = from_offset;
        self.records_since_index = 0;

        Ok(())
    }

    /// Returns the raw segment data.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Reader for parsing segment data.
pub struct SegmentReader<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> SegmentReader<'a> {
    /// Creates a new segment reader.
    #[must_use]
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Reads the next batch from the segment.
    ///
    /// # Errors
    /// Returns an error if the data is invalid.
    pub fn read_batch(&mut self) -> PartitionResult<RecordBatch> {
        use bytes::Buf;

        // Batch header: u64 + i64 + i64 + u8 + u32 + u32 = 33 bytes
        const BATCH_HEADER_SIZE: usize = 8 + 8 + 8 + 1 + 4 + 4;

        let remaining = &self.data[self.position..];
        if remaining.len() < BATCH_HEADER_SIZE {
            return Err(PartitionError::Corruption {
                message: "incomplete batch header".to_string(),
            });
        }

        let mut buf = remaining;

        // Read header.
        let base_offset = Offset::new(buf.get_u64_le());
        let base_timestamp = Timestamp::from_millis(buf.get_i64_le());
        let max_timestamp = Timestamp::from_millis(buf.get_i64_le());
        let compression_byte = buf.get_u8();
        let record_count = buf.get_u32_le() as usize;
        let expected_crc = buf.get_u32_le();

        let _compression = helix_core::Compression::from_byte(compression_byte)
            .ok_or_else(|| PartitionError::Corruption {
                message: format!("invalid compression byte: {}", compression_byte),
            })?;

        // Header size for CRC verification (same as BATCH_HEADER_SIZE).
        let header_size = 33;

        // Read records.
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let record = Record::decode(&mut buf).ok_or_else(|| PartitionError::Corruption {
                message: "failed to decode record".to_string(),
            })?;
            records.push(record);
        }

        // Verify CRC.
        let records_end = remaining.len() - buf.remaining();
        let actual_crc = crc32fast::hash(&remaining[header_size..records_end]);
        if actual_crc != expected_crc {
            return Err(PartitionError::Corruption {
                message: format!(
                    "CRC mismatch: expected {:08x}, got {:08x}",
                    expected_crc, actual_crc
                ),
            });
        }

        // Update position.
        self.position += records_end;

        Ok(RecordBatch {
            partition_id: helix_core::PartitionId::new(0), // Will be set by caller
            base_offset,
            base_timestamp,
            max_timestamp,
            compression: helix_core::Compression::None,
            crc: expected_crc,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use helix_core::PartitionId;

    fn make_batch(offset: u64, count: usize) -> RecordBatch {
        let mut batch = RecordBatch::new(PartitionId::new(0));
        for i in 0..count {
            let record = Record::new(Bytes::from(format!("value-{}", offset as usize + i)));
            batch.push(record);
        }
        batch
    }

    #[test]
    fn test_segment_creation() {
        let segment = LogSegment::new(Offset::new(0), SegmentConfig::default());
        assert_eq!(segment.base_offset(), Offset::new(0));
        assert_eq!(segment.next_offset(), Offset::new(0));
        assert!(segment.last_offset().is_none());
        assert_eq!(segment.record_count(), 0);
    }

    #[test]
    fn test_segment_append() {
        let mut segment = LogSegment::new(Offset::new(0), SegmentConfig::default());

        let mut batch = make_batch(0, 3);
        let offset = segment.append(&mut batch).unwrap();

        assert_eq!(offset, Offset::new(0));
        assert_eq!(segment.next_offset(), Offset::new(3));
        assert_eq!(segment.last_offset(), Some(Offset::new(2)));
        assert_eq!(segment.record_count(), 3);
    }

    #[test]
    fn test_segment_append_multiple() {
        let mut segment = LogSegment::new(Offset::new(100), SegmentConfig::default());

        let mut batch1 = make_batch(100, 2);
        segment.append(&mut batch1).unwrap();

        let mut batch2 = make_batch(102, 3);
        segment.append(&mut batch2).unwrap();

        assert_eq!(segment.next_offset(), Offset::new(105));
        assert_eq!(segment.record_count(), 5);
    }

    #[test]
    fn test_segment_sealed_rejects_append() {
        let mut segment = LogSegment::new(Offset::new(0), SegmentConfig::default());
        segment.seal();

        let mut batch = make_batch(0, 1);
        let result = segment.append(&mut batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_segment_read() {
        let mut segment = LogSegment::new(Offset::new(0), SegmentConfig::default());

        let mut batch = make_batch(0, 5);
        segment.append(&mut batch).unwrap();

        let records = segment.read(Offset::new(0), 10).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, Offset::new(0));
        assert_eq!(records[4].offset, Offset::new(4));
    }

    #[test]
    fn test_segment_read_partial() {
        let mut segment = LogSegment::new(Offset::new(0), SegmentConfig::default());

        let mut batch = make_batch(0, 10);
        segment.append(&mut batch).unwrap();

        let records = segment.read(Offset::new(3), 3).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset, Offset::new(3));
        assert_eq!(records[2].offset, Offset::new(5));
    }

    #[test]
    fn test_segment_read_out_of_range() {
        let mut segment = LogSegment::new(Offset::new(10), SegmentConfig::default());

        let mut batch = make_batch(10, 5);
        segment.append(&mut batch).unwrap();

        // Before segment.
        let result = segment.read(Offset::new(5), 10);
        assert!(result.is_err());

        // After segment.
        let result = segment.read(Offset::new(20), 10);
        assert!(result.is_err());
    }
}

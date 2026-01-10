//! Kafka-style record types for Helix.
//!
//! Records are the fundamental unit of data in Helix. They follow the Kafka
//! record format with key, value, headers, and timestamp.
//!
//! # Record Format
//!
//! Each record contains:
//! - **Offset**: Position in the partition log (assigned by broker)
//! - **Timestamp**: When the record was created (client or broker time)
//! - **Key**: Optional key for partitioning and compaction
//! - **Value**: The actual message payload
//! - **Headers**: Optional key-value metadata
//!
//! # Record Batches
//!
//! Records are grouped into batches for efficient network transmission
//! and storage. A batch contains:
//! - Base offset and timestamp
//! - Compression codec
//! - CRC for integrity
//! - Multiple records

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::limits::Limits;
use crate::PartitionId;

/// A record header (key-value metadata).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// Header key.
    pub key: Bytes,
    /// Header value.
    pub value: Bytes,
}

impl Header {
    /// Creates a new header.
    #[must_use]
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the encoded size of this header.
    #[must_use]
    pub const fn encoded_size(&self) -> usize {
        4 + self.key.len() + 4 + self.value.len()
    }

    /// Encodes the header to bytes.
    #[allow(clippy::cast_possible_truncation)] // Sizes bounded by limits.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.key.len() as u32);
        buf.put_slice(&self.key);
        buf.put_u32_le(self.value.len() as u32);
        buf.put_slice(&self.value);
    }

    /// Decodes a header from bytes.
    ///
    /// # Errors
    /// Returns `None` if the buffer is too small.
    pub fn decode(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 4 {
            return None;
        }
        let key_len = buf.get_u32_le() as usize;
        if buf.remaining() < key_len + 4 {
            return None;
        }
        let key = buf.copy_to_bytes(key_len);

        let value_len = buf.get_u32_le() as usize;
        if buf.remaining() < value_len {
            return None;
        }
        let value = buf.copy_to_bytes(value_len);

        Some(Self { key, value })
    }
}

/// Timestamp type for records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Creates a timestamp from milliseconds since Unix epoch.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Returns the timestamp as milliseconds since Unix epoch.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// Returns the current time as a timestamp.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Timestamps won't overflow i64 for centuries.
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_millis() as i64)
    }

    /// Creates a timestamp representing "no timestamp".
    #[must_use]
    pub const fn none() -> Self {
        Self(-1)
    }

    /// Returns true if this represents "no timestamp".
    #[must_use]
    pub const fn is_none(self) -> bool {
        self.0 < 0
    }
}

impl Default for Timestamp {
    fn default() -> Self {
        Self::none()
    }
}

/// Offset in a partition log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Offset(u64);

impl Offset {
    /// Creates an offset from a raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw offset value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next offset.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the offset for "earliest" (beginning of log).
    #[must_use]
    pub const fn earliest() -> Self {
        Self(0)
    }

    /// Returns the offset for "latest" (end of log).
    #[must_use]
    pub const fn latest() -> Self {
        Self(u64::MAX)
    }
}

impl std::fmt::Display for Offset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A single record in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Offset in the partition (assigned by broker, 0 if not yet assigned).
    pub offset: Offset,
    /// Timestamp of the record.
    pub timestamp: Timestamp,
    /// Optional key for partitioning and compaction.
    pub key: Option<Bytes>,
    /// The record value/payload.
    pub value: Bytes,
    /// Optional headers.
    pub headers: Vec<Header>,
}

impl Record {
    /// Creates a new record with just a value.
    #[must_use]
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            offset: Offset::default(),
            timestamp: Timestamp::now(),
            key: None,
            value: value.into(),
            headers: Vec::new(),
        }
    }

    /// Creates a new record with key and value.
    #[must_use]
    pub fn with_key(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            offset: Offset::default(),
            timestamp: Timestamp::now(),
            key: Some(key.into()),
            value: value.into(),
            headers: Vec::new(),
        }
    }

    /// Sets the timestamp.
    #[must_use]
    pub const fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Adds a header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    /// Sets the offset (typically done by the broker).
    pub fn set_offset(&mut self, offset: Offset) {
        self.offset = offset;
    }

    /// Returns the approximate size of the record in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        let key_size = self.key.as_ref().map_or(0, Bytes::len);
        let headers_size: usize = self.headers.iter().map(Header::encoded_size).sum();
        8 + 8 + 4 + key_size + 4 + self.value.len() + 4 + headers_size
    }

    /// Validates the record against limits.
    ///
    /// # Errors
    /// Returns an error if the record exceeds any limits.
    pub fn validate(&self, limits: &Limits) -> Result<(), RecordError> {
        if self.value.len() > limits.record_value_size_bytes_max as usize {
            return Err(RecordError::ValueTooLarge {
                size: self.value.len(),
                max: limits.record_value_size_bytes_max as usize,
            });
        }

        if let Some(ref key) = self.key {
            if key.len() > limits.record_key_size_bytes_max as usize {
                return Err(RecordError::KeyTooLarge {
                    size: key.len(),
                    max: limits.record_key_size_bytes_max as usize,
                });
            }
        }

        if self.headers.len() > limits.record_headers_count_max as usize {
            return Err(RecordError::TooManyHeaders {
                count: self.headers.len(),
                max: limits.record_headers_count_max as usize,
            });
        }

        Ok(())
    }

    /// Encodes the record to bytes.
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)] // Sizes bounded by limits.
    pub fn encode(&self, buf: &mut BytesMut) {
        // Offset.
        buf.put_u64_le(self.offset.get());
        // Timestamp.
        buf.put_i64_le(self.timestamp.as_millis());
        // Key (length-prefixed, -1 for null).
        match &self.key {
            Some(k) => {
                buf.put_i32_le(k.len() as i32);
                buf.put_slice(k);
            }
            None => {
                buf.put_i32_le(-1);
            }
        }
        // Value (length-prefixed).
        buf.put_u32_le(self.value.len() as u32);
        buf.put_slice(&self.value);
        // Headers.
        buf.put_u32_le(self.headers.len() as u32);
        for header in &self.headers {
            header.encode(buf);
        }
    }

    /// Decodes a record from bytes.
    ///
    /// # Errors
    /// Returns `None` if the buffer is invalid.
    #[allow(clippy::cast_sign_loss)] // key_len is checked to be non-negative before cast.
    pub fn decode(buf: &mut impl Buf) -> Option<Self> {
        if buf.remaining() < 8 + 8 + 4 {
            return None;
        }

        let offset = Offset::new(buf.get_u64_le());
        let timestamp = Timestamp::from_millis(buf.get_i64_le());

        let key_len = buf.get_i32_le();
        let key = if key_len < 0 {
            None
        } else {
            if buf.remaining() < key_len as usize {
                return None;
            }
            Some(buf.copy_to_bytes(key_len as usize))
        };

        if buf.remaining() < 4 {
            return None;
        }
        let value_len = buf.get_u32_le() as usize;
        if buf.remaining() < value_len {
            return None;
        }
        let value = buf.copy_to_bytes(value_len);

        if buf.remaining() < 4 {
            return None;
        }
        let header_count = buf.get_u32_le() as usize;
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            headers.push(Header::decode(buf)?);
        }

        Some(Self {
            offset,
            timestamp,
            key,
            value,
            headers,
        })
    }
}

/// Compression codec for record batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Compression {
    /// No compression.
    #[default]
    None = 0,
    /// Gzip compression.
    Gzip = 1,
    /// Snappy compression.
    Snappy = 2,
    /// LZ4 compression.
    Lz4 = 3,
    /// Zstd compression.
    Zstd = 4,
}

impl Compression {
    /// Creates a compression from a raw byte.
    #[must_use]
    pub const fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// A batch of records for efficient storage and transmission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    /// Partition this batch belongs to.
    pub partition_id: PartitionId,
    /// First offset in this batch.
    pub base_offset: Offset,
    /// Timestamp of the batch (min timestamp of records).
    pub base_timestamp: Timestamp,
    /// Maximum timestamp in the batch.
    pub max_timestamp: Timestamp,
    /// Compression codec used.
    pub compression: Compression,
    /// CRC32 of the batch data.
    pub crc: u32,
    /// Records in this batch.
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// Creates a new record batch.
    #[must_use]
    pub fn new(partition_id: PartitionId) -> Self {
        Self {
            partition_id,
            base_offset: Offset::default(),
            base_timestamp: Timestamp::none(),
            max_timestamp: Timestamp::none(),
            compression: Compression::None,
            crc: 0,
            records: Vec::new(),
        }
    }

    /// Adds a record to the batch.
    pub fn push(&mut self, record: Record) {
        // Update timestamps.
        if self.base_timestamp.is_none() || record.timestamp < self.base_timestamp {
            self.base_timestamp = record.timestamp;
        }
        if self.max_timestamp.is_none() || record.timestamp > self.max_timestamp {
            self.max_timestamp = record.timestamp;
        }
        self.records.push(record);
    }

    /// Returns the number of records in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns true if the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns the last offset in the batch.
    #[must_use]
    pub fn last_offset(&self) -> Offset {
        if self.records.is_empty() {
            self.base_offset
        } else {
            Offset::new(self.base_offset.get() + self.records.len() as u64 - 1)
        }
    }

    /// Assigns offsets to all records starting from `base_offset`.
    pub fn assign_offsets(&mut self, base: Offset) {
        self.base_offset = base;
        let mut offset = base.get();
        for record in &mut self.records {
            record.offset = Offset::new(offset);
            offset += 1;
        }
    }

    /// Returns the approximate size of the batch in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        // Header: partition_id + base_offset + timestamps + compression + crc + record_count
        let header_size = 8 + 8 + 8 + 8 + 1 + 4 + 4;
        let records_size: usize = self.records.iter().map(Record::size).sum();
        header_size + records_size
    }

    /// Validates the batch against limits.
    ///
    /// # Errors
    /// Returns an error if the batch exceeds any limits.
    pub fn validate(&self, limits: &Limits) -> Result<(), RecordError> {
        if self.records.len() > limits.batch_records_count_max as usize {
            return Err(RecordError::BatchTooLarge {
                count: self.records.len(),
                max: limits.batch_records_count_max as usize,
            });
        }

        for record in &self.records {
            record.validate(limits)?;
        }

        Ok(())
    }
}

/// Record-related errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// Record value exceeds maximum size.
    ValueTooLarge {
        /// Actual size.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
    /// Record key exceeds maximum size.
    KeyTooLarge {
        /// Actual size.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
    /// Too many headers on a record.
    TooManyHeaders {
        /// Actual count.
        count: usize,
        /// Maximum allowed count.
        max: usize,
    },
    /// Batch has too many records.
    BatchTooLarge {
        /// Actual count.
        count: usize,
        /// Maximum allowed count.
        max: usize,
    },
    /// Record encoding/decoding error.
    Codec {
        /// Error description.
        message: String,
    },
}

impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValueTooLarge { size, max } => {
                write!(f, "record value too large: {size} > {max}")
            }
            Self::KeyTooLarge { size, max } => {
                write!(f, "record key too large: {size} > {max}")
            }
            Self::TooManyHeaders { count, max } => {
                write!(f, "too many headers: {count} > {max}")
            }
            Self::BatchTooLarge { count, max } => {
                write!(f, "batch too large: {count} records > {max}")
            }
            Self::Codec { message } => {
                write!(f, "codec error: {message}")
            }
        }
    }
}

impl std::error::Error for RecordError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_new() {
        let record = Record::new("hello");
        assert!(record.key.is_none());
        assert_eq!(record.value, Bytes::from("hello"));
        assert!(record.headers.is_empty());
    }

    #[test]
    fn test_record_with_key() {
        let record = Record::with_key("user-123", "data");
        assert_eq!(record.key, Some(Bytes::from("user-123")));
        assert_eq!(record.value, Bytes::from("data"));
    }

    #[test]
    fn test_record_with_headers() {
        let record = Record::new("value")
            .with_header("content-type", "application/json")
            .with_header("trace-id", "abc123");
        assert_eq!(record.headers.len(), 2);
    }

    #[test]
    fn test_record_roundtrip() {
        let original = Record::with_key("key", "value")
            .with_timestamp(Timestamp::from_millis(1_234_567_890))
            .with_header("h1", "v1");

        let mut buf = BytesMut::new();
        original.encode(&mut buf);

        let decoded = Record::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.value, original.value);
        assert_eq!(decoded.timestamp, original.timestamp);
        assert_eq!(decoded.headers.len(), 1);
    }

    #[test]
    fn test_record_null_key_roundtrip() {
        let original = Record::new("value");

        let mut buf = BytesMut::new();
        original.encode(&mut buf);

        let decoded = Record::decode(&mut buf.freeze()).unwrap();
        assert!(decoded.key.is_none());
        assert_eq!(decoded.value, original.value);
    }

    #[test]
    fn test_batch_operations() {
        let mut batch = RecordBatch::new(PartitionId::new(0));
        assert!(batch.is_empty());

        batch.push(Record::new("first"));
        batch.push(Record::new("second"));
        assert_eq!(batch.len(), 2);

        batch.assign_offsets(Offset::new(100));
        assert_eq!(batch.base_offset, Offset::new(100));
        assert_eq!(batch.records[0].offset, Offset::new(100));
        assert_eq!(batch.records[1].offset, Offset::new(101));
        assert_eq!(batch.last_offset(), Offset::new(101));
    }

    #[test]
    fn test_timestamp() {
        let ts = Timestamp::from_millis(1000);
        assert_eq!(ts.as_millis(), 1000);
        assert!(!ts.is_none());

        let none = Timestamp::none();
        assert!(none.is_none());
    }

    #[test]
    fn test_offset() {
        let offset = Offset::new(42);
        assert_eq!(offset.get(), 42);
        assert_eq!(offset.next().get(), 43);
        assert_eq!(format!("{offset}"), "42");
    }

    #[test]
    fn test_header_roundtrip() {
        let header = Header::new("key", "value");

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        let decoded = Header::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.key, header.key);
        assert_eq!(decoded.value, header.value);
    }
}

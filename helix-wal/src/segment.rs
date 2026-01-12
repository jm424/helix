//! WAL segment management.
//!
//! A segment is a single file in the WAL containing a sequence of entries.
//! Segments are append-only and rotated when they reach size limits.
//!
//! # Segment File Format
//!
//! ```text
//! +-------------------+
//! | Segment Header    |  (32 bytes)
//! +-------------------+
//! | Entry 1           |
//! +-------------------+
//! | Entry 2           |
//! +-------------------+
//! | ...               |
//! +-------------------+
//! ```
//!
//! Segment header:
//! - Magic (8 bytes): "HELIXWAL"
//! - Version (4 bytes): Format version
//! - Segment ID (8 bytes): Unique segment identifier
//! - First Index (8 bytes): First log index in this segment
//! - Reserved (4 bytes): For future use

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::entry::{Entry, ENTRY_HEADER_SIZE};
use crate::error::{WalError, WalResult};
use crate::limits::{ENTRIES_PER_SEGMENT_MAX, SEGMENT_SIZE_BYTES_MAX, SEGMENT_SIZE_BYTES_MIN};

/// Segment header size in bytes.
pub const SEGMENT_HEADER_SIZE: usize = 32;

/// Magic bytes identifying a Helix WAL segment.
const SEGMENT_MAGIC: &[u8; 8] = b"HELIXWAL";

/// Current segment format version.
const SEGMENT_VERSION: u32 = 1;

/// Unique identifier for a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u64);

impl SegmentId {
    /// Creates a new segment ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw ID value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the next segment ID.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl std::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seg-{:016x}", self.0)
    }
}

/// Configuration for a segment.
#[derive(Debug, Clone, Copy)]
pub struct SegmentConfig {
    /// Maximum size of the segment in bytes.
    pub max_size_bytes: u64,
    /// Maximum number of entries in the segment.
    pub max_entries: u64,
}

impl SegmentConfig {
    /// Creates a new segment configuration with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_size_bytes: SEGMENT_SIZE_BYTES_MAX,
            max_entries: ENTRIES_PER_SEGMENT_MAX,
        }
    }

    /// Sets the maximum size in bytes.
    ///
    /// # Panics
    /// Panics if size is outside valid range.
    #[must_use]
    pub const fn with_max_size(mut self, size: u64) -> Self {
        assert!(
            size >= SEGMENT_SIZE_BYTES_MIN && size <= SEGMENT_SIZE_BYTES_MAX,
            "segment size out of range"
        );
        self.max_size_bytes = size;
        self
    }

    /// Sets the maximum number of entries per segment.
    ///
    /// This is useful for testing segment rotation with small entry counts.
    ///
    /// # Panics
    /// Panics if entries is 0 or exceeds `ENTRIES_PER_SEGMENT_MAX`.
    #[must_use]
    pub const fn with_max_entries(mut self, entries: u64) -> Self {
        assert!(entries > 0, "max_entries must be positive");
        assert!(
            entries <= ENTRIES_PER_SEGMENT_MAX,
            "max_entries exceeds limit"
        );
        self.max_entries = entries;
        self
    }

    /// Validates the configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> WalResult<()> {
        if self.max_size_bytes < SEGMENT_SIZE_BYTES_MIN {
            return Err(WalError::Io {
                operation: "config",
                message: format!(
                    "max_size_bytes {} below minimum {}",
                    self.max_size_bytes, SEGMENT_SIZE_BYTES_MIN
                ),
            });
        }
        if self.max_size_bytes > SEGMENT_SIZE_BYTES_MAX {
            return Err(WalError::Io {
                operation: "config",
                message: format!(
                    "max_size_bytes {} above maximum {}",
                    self.max_size_bytes, SEGMENT_SIZE_BYTES_MAX
                ),
            });
        }
        Ok(())
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Segment header stored at the beginning of each segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Format version.
    pub version: u32,
    /// Unique segment identifier.
    pub segment_id: SegmentId,
    /// First log index in this segment.
    pub first_index: u64,
}

impl SegmentHeader {
    /// Creates a new segment header.
    #[must_use]
    pub const fn new(segment_id: SegmentId, first_index: u64) -> Self {
        Self {
            version: SEGMENT_VERSION,
            segment_id,
            first_index,
        }
    }

    /// Encodes the header to bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(SEGMENT_MAGIC);
        buf.put_u32_le(self.version);
        buf.put_u64_le(self.segment_id.get());
        buf.put_u64_le(self.first_index);
        buf.put_u32_le(0); // Reserved
    }

    /// Decodes a header from bytes.
    ///
    /// # Errors
    /// Returns an error if the data is invalid.
    pub fn decode(buf: &mut impl Buf) -> WalResult<Self> {
        if buf.remaining() < SEGMENT_HEADER_SIZE {
            return Err(WalError::InvalidHeader {
                offset: 0,
                reason: "segment header too small",
            });
        }

        // Check magic.
        let mut magic = [0u8; 8];
        buf.copy_to_slice(&mut magic);
        if &magic != SEGMENT_MAGIC {
            return Err(WalError::InvalidHeader {
                offset: 0,
                reason: "invalid segment magic",
            });
        }

        let version = buf.get_u32_le();
        if version != SEGMENT_VERSION {
            return Err(WalError::InvalidHeader {
                offset: 0,
                reason: "unsupported segment version",
            });
        }

        let segment_id = SegmentId::new(buf.get_u64_le());
        let first_index = buf.get_u64_le();
        let _reserved = buf.get_u32_le();

        Ok(Self {
            version,
            segment_id,
            first_index,
        })
    }
}

/// An in-memory WAL segment.
///
/// This is designed to work with Bloodhound's simulated storage for
/// deterministic testing.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Segment header.
    header: SegmentHeader,
    /// Configuration.
    config: SegmentConfig,
    /// Entries in this segment.
    entries: Vec<Entry>,
    /// Current size in bytes (header + entries).
    size_bytes: u64,
    /// Whether the segment is sealed (no more writes).
    sealed: bool,
}

impl Segment {
    /// Creates a new empty segment.
    #[must_use]
    pub const fn new(segment_id: SegmentId, first_index: u64, config: SegmentConfig) -> Self {
        Self {
            header: SegmentHeader::new(segment_id, first_index),
            config,
            entries: Vec::new(),
            size_bytes: SEGMENT_HEADER_SIZE as u64,
            sealed: false,
        }
    }

    /// Returns the segment ID.
    #[must_use]
    pub const fn id(&self) -> SegmentId {
        self.header.segment_id
    }

    /// Returns the first index in this segment.
    #[must_use]
    pub const fn first_index(&self) -> u64 {
        self.header.first_index
    }

    /// Returns the last index in this segment, or None if empty.
    #[must_use]
    pub fn last_index(&self) -> Option<u64> {
        self.entries.last().map(Entry::index)
    }

    /// Returns the number of entries in this segment.
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Returns the current size in bytes.
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns true if the segment is sealed (no more writes allowed).
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Returns true if the segment has room for an entry of the given size.
    #[must_use]
    pub fn has_space_for(&self, payload_size: u32) -> bool {
        if self.sealed {
            return false;
        }

        let entry_size = ENTRY_HEADER_SIZE as u64 + u64::from(payload_size);
        let new_size = self.size_bytes + entry_size;
        let new_count = self.entries.len() as u64 + 1;

        new_size <= self.config.max_size_bytes && new_count <= self.config.max_entries
    }

    /// Appends an entry to the segment.
    ///
    /// # Panics
    /// Panics if the segment is sealed or if the entry index is not sequential.
    ///
    /// # Errors
    /// Returns an error if the segment is full.
    pub fn append(&mut self, entry: Entry) -> WalResult<()> {
        // TigerStyle: Check preconditions.
        assert!(!self.sealed, "cannot append to sealed segment");

        let entry_size = entry.total_size();

        if self.size_bytes + entry_size > self.config.max_size_bytes {
            return Err(WalError::SegmentFull {
                reason: "size limit reached",
            });
        }

        if self.entries.len() as u64 >= self.config.max_entries {
            return Err(WalError::SegmentFull {
                reason: "entry count limit reached",
            });
        }

        // Verify index is sequential.
        let expected_index = self
            .entries
            .last()
            .map_or(self.header.first_index, |e| e.index() + 1);

        assert_eq!(
            entry.index(),
            expected_index,
            "entry index must be sequential"
        );

        self.size_bytes += entry_size;
        self.entries.push(entry);

        Ok(())
    }

    /// Reads an entry by index.
    ///
    /// # Errors
    /// Returns an error if the index is out of bounds.
    pub fn read(&self, index: u64) -> WalResult<&Entry> {
        if self.entries.is_empty() {
            return Err(WalError::IndexOutOfBounds {
                index,
                first: self.header.first_index,
                last: self.header.first_index,
            });
        }

        let first = self.header.first_index;
        let last = self.entries.last().map_or(first, Entry::index);

        if index < first || index > last {
            return Err(WalError::IndexOutOfBounds { index, first, last });
        }

        #[allow(clippy::cast_possible_truncation)] // Entry count bounded by config.
        let offset = (index - first) as usize;
        Ok(&self.entries[offset])
    }

    /// Truncates entries after the given index.
    ///
    /// Keeps entries up to and including `last_index_to_keep`.
    ///
    /// # Panics
    /// Panics if the segment is sealed.
    ///
    /// # Errors
    /// Currently infallible, but returns `Result` for future extensibility.
    pub fn truncate_after(&mut self, last_index_to_keep: u64) -> WalResult<()> {
        assert!(!self.sealed, "cannot truncate sealed segment");
        self.truncate_entries_after(last_index_to_keep)
    }

    /// Truncates in-memory entries after the given index.
    ///
    /// This works on both sealed and unsealed segments. For sealed segments,
    /// this only affects the in-memory representation - the on-disk file
    /// is not modified. This is used during WAL truncation to remove stale
    /// entries from sealed segments.
    ///
    /// # Errors
    /// Currently infallible, but returns `Result` for future extensibility.
    pub fn truncate_entries_after(&mut self, last_index_to_keep: u64) -> WalResult<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        let first = self.header.first_index;

        // If truncating before our first index, remove all entries.
        if last_index_to_keep < first {
            let removed_size: u64 = self.entries.iter().map(Entry::total_size).sum();
            self.size_bytes -= removed_size;
            self.entries.clear();
            return Ok(());
        }

        #[allow(clippy::cast_possible_truncation)] // Entry count bounded by config.
        let keep_count = (last_index_to_keep - first + 1) as usize;
        if keep_count >= self.entries.len() {
            return Ok(()); // Nothing to truncate.
        }

        // Calculate size of removed entries.
        let removed_size: u64 = self.entries[keep_count..].iter().map(Entry::total_size).sum();

        self.entries.truncate(keep_count);
        self.size_bytes -= removed_size;

        Ok(())
    }

    /// Seals the segment, preventing further writes.
    pub const fn seal(&mut self) {
        self.sealed = true;
    }

    /// Encodes the entire segment to bytes.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Size bounded by config.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.size_bytes as usize);

        // Header.
        self.header.encode(&mut buf);

        // Entries.
        for entry in &self.entries {
            entry.encode(&mut buf);
        }

        buf.freeze()
    }

    /// Decodes a segment from bytes.
    ///
    /// # Errors
    /// Returns an error if the data is invalid or corrupted.
    pub fn decode(mut data: Bytes, config: SegmentConfig) -> WalResult<Self> {
        let header = SegmentHeader::decode(&mut data)?;

        let mut entries = Vec::new();
        let mut offset = SEGMENT_HEADER_SIZE as u64;

        while data.has_remaining() {
            // Check if we have at least a header's worth of data.
            if data.remaining() < ENTRY_HEADER_SIZE {
                // Partial header at end - treat as truncated write.
                break;
            }

            match Entry::decode(&mut data, offset) {
                Ok(entry) => {
                    offset += entry.total_size();
                    entries.push(entry);
                }
                Err(WalError::TruncatedEntry { .. }) => {
                    // Truncated write at end of file - stop reading.
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        let size_bytes =
            SEGMENT_HEADER_SIZE as u64 + entries.iter().map(Entry::total_size).sum::<u64>();

        Ok(Self {
            header,
            config,
            entries,
            size_bytes,
            sealed: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_header_roundtrip() {
        let header = SegmentHeader::new(SegmentId::new(42), 100);

        let mut buf = BytesMut::new();
        header.encode(&mut buf);

        assert_eq!(buf.len(), SEGMENT_HEADER_SIZE);

        let decoded = SegmentHeader::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_segment_append_and_read() {
        let config = SegmentConfig::new();
        let mut segment = Segment::new(SegmentId::new(1), 1, config);

        // Append some entries.
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
            segment.append(entry).unwrap();
        }

        assert_eq!(segment.entry_count(), 5);
        assert_eq!(segment.first_index(), 1);
        assert_eq!(segment.last_index(), Some(5));

        // Read them back.
        for i in 1..=5 {
            let entry = segment.read(i).unwrap();
            assert_eq!(entry.index(), i);
            assert_eq!(entry.term(), 1);
        }
    }

    #[test]
    fn test_segment_roundtrip() {
        let config = SegmentConfig::new();
        let mut segment = Segment::new(SegmentId::new(1), 1, config);

        for i in 1..=3 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            segment.append(entry).unwrap();
        }

        let encoded = segment.encode();
        let decoded = Segment::decode(encoded, config).unwrap();

        assert_eq!(decoded.id(), segment.id());
        assert_eq!(decoded.first_index(), segment.first_index());
        assert_eq!(decoded.entry_count(), segment.entry_count());

        for i in 1..=3 {
            let orig = segment.read(i).unwrap();
            let dec = decoded.read(i).unwrap();
            assert_eq!(orig, dec);
        }
    }

    #[test]
    fn test_segment_truncate() {
        let config = SegmentConfig::new();
        let mut segment = Segment::new(SegmentId::new(1), 1, config);

        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from("x")).unwrap();
            segment.append(entry).unwrap();
        }

        segment.truncate_after(3).unwrap();

        assert_eq!(segment.entry_count(), 3);
        assert_eq!(segment.last_index(), Some(3));
        assert!(segment.read(4).is_err());
    }

    #[test]
    fn test_segment_full() {
        let config = SegmentConfig {
            max_entries: 3,
            max_size_bytes: SEGMENT_SIZE_BYTES_MAX,
        };
        let mut segment = Segment::new(SegmentId::new(1), 1, config);

        for i in 1..=3 {
            let entry = Entry::new(1, i, Bytes::from("x")).unwrap();
            segment.append(entry).unwrap();
        }

        let entry = Entry::new(1, 4, Bytes::from("x")).unwrap();
        let result = segment.append(entry);
        assert!(matches!(result, Err(WalError::SegmentFull { .. })));
    }

    #[test]
    fn test_segment_id_display() {
        let id = SegmentId::new(255);
        assert_eq!(format!("{id}"), "seg-00000000000000ff");
    }
}

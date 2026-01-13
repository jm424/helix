//! Shared WAL entry format.
//!
//! Each entry in a shared WAL has the following binary format:
//!
//! ```text
//! +----------+----------+----------+----------+----------+----------+
//! |  CRC32   |  Length  |  PID     |   Term   |  Index   | Payload  |
//! | (4 bytes)| (4 bytes)| (8 bytes)| (8 bytes)| (8 bytes)| (N bytes)|
//! +----------+----------+----------+----------+----------+----------+
//! ```
//!
//! - CRC32: Checksum of Length + `PartitionId` + Term + Index + Payload
//! - Length: Payload length in bytes (not including header)
//! - PID: Partition ID that owns this entry
//! - Term: Raft term when entry was created
//! - Index: Log index of this entry (partition-local)
//! - Payload: Application data
//!
//! All integers are stored in little-endian format.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::PartitionId;

use crate::entry::WalEntry;
use crate::error::{WalError, WalResult};
use crate::limits::ENTRY_PAYLOAD_SIZE_BYTES_MAX;

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

/// Size of the shared entry header in bytes.
pub const SHARED_ENTRY_HEADER_SIZE: usize = 32; // 4 + 4 + 8 + 8 + 8

/// Maximum payload length.
const ENTRY_LENGTH_MAX: u32 = ENTRY_PAYLOAD_SIZE_BYTES_MAX;

// ----------------------------------------------------------------------------
// SharedEntryHeader
// ----------------------------------------------------------------------------

/// Header for shared WAL entries.
///
/// Contains metadata identifying the partition and entry position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedEntryHeader {
    /// CRC32 checksum of the rest of the entry.
    pub crc: u32,
    /// Length of the payload in bytes.
    pub length: u32,
    /// Partition ID that owns this entry.
    pub partition_id: PartitionId,
    /// Raft term when this entry was created.
    pub term: u64,
    /// Log index of this entry (partition-local).
    pub index: u64,
}

impl SharedEntryHeader {
    /// Creates a new shared entry header.
    ///
    /// The CRC is computed from the other fields and the payload.
    ///
    /// # Errors
    /// Returns an error if the payload is too large.
    pub fn new(
        partition_id: PartitionId,
        term: u64,
        index: u64,
        payload: &[u8],
    ) -> WalResult<Self> {
        let length = payload.len();

        // TigerStyle: Check limits explicitly.
        if length > ENTRY_LENGTH_MAX as usize {
            #[allow(clippy::cast_possible_truncation)] // Truncated value is fine for error reporting.
            return Err(WalError::EntryTooLarge {
                size: length as u32,
                max: ENTRY_LENGTH_MAX,
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let length = length as u32;

        // Compute CRC over length + partition_id + term + index + payload.
        let crc = Self::compute_crc(length, partition_id, term, index, payload);

        Ok(Self {
            crc,
            length,
            partition_id,
            term,
            index,
        })
    }

    /// Computes the CRC32 checksum for an entry.
    fn compute_crc(
        length: u32,
        partition_id: PartitionId,
        term: u64,
        index: u64,
        payload: &[u8],
    ) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&length.to_le_bytes());
        hasher.update(&partition_id.get().to_le_bytes());
        hasher.update(&term.to_le_bytes());
        hasher.update(&index.to_le_bytes());
        hasher.update(payload);
        hasher.finalize()
    }

    /// Verifies the CRC matches the payload.
    ///
    /// # Errors
    /// Returns `ChecksumMismatch` if the CRC doesn't match.
    pub fn verify(&self, payload: &[u8], offset: u64) -> WalResult<()> {
        let expected =
            Self::compute_crc(self.length, self.partition_id, self.term, self.index, payload);
        if expected != self.crc {
            return Err(WalError::ChecksumMismatch {
                offset,
                expected,
                actual: self.crc,
            });
        }
        Ok(())
    }

    /// Encodes the header to bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32_le(self.crc);
        buf.put_u32_le(self.length);
        buf.put_u64_le(self.partition_id.get());
        buf.put_u64_le(self.term);
        buf.put_u64_le(self.index);
    }

    /// Decodes a header from bytes.
    ///
    /// # Errors
    /// Returns an error if the buffer is too small or the header is invalid.
    pub fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self> {
        if buf.remaining() < SHARED_ENTRY_HEADER_SIZE {
            return Err(WalError::InvalidHeader {
                offset,
                reason: "buffer too small for shared entry header",
            });
        }

        let crc = buf.get_u32_le();
        let length = buf.get_u32_le();
        let partition_id = PartitionId::new(buf.get_u64_le());
        let term = buf.get_u64_le();
        let index = buf.get_u64_le();

        // Validate length is reasonable.
        if length > ENTRY_LENGTH_MAX {
            return Err(WalError::InvalidHeader {
                offset,
                reason: "length exceeds maximum",
            });
        }

        Ok(Self {
            crc,
            length,
            partition_id,
            term,
            index,
        })
    }

    /// Returns the total size of the entry (header + payload).
    #[must_use]
    pub const fn total_size(&self) -> u64 {
        SHARED_ENTRY_HEADER_SIZE as u64 + self.length as u64
    }
}

// ----------------------------------------------------------------------------
// SharedEntry
// ----------------------------------------------------------------------------

/// A complete shared WAL entry (header + payload).
///
/// This entry type is used in shared WALs where multiple partitions
/// write to the same WAL. Each entry is tagged with a partition ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedEntry {
    /// Entry header with metadata.
    pub header: SharedEntryHeader,
    /// Entry payload (application data).
    pub payload: Bytes,
}

impl SharedEntry {
    /// Creates a new shared entry.
    ///
    /// # Errors
    /// Returns an error if the payload is too large.
    pub fn new(
        partition_id: PartitionId,
        term: u64,
        index: u64,
        payload: Bytes,
    ) -> WalResult<Self> {
        let header = SharedEntryHeader::new(partition_id, term, index, &payload)?;
        Ok(Self { header, payload })
    }

    /// Returns the partition ID.
    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.header.partition_id
    }

    /// Returns the Raft term.
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.header.term
    }

    /// Returns the log index.
    #[must_use]
    pub const fn index(&self) -> u64 {
        self.header.index
    }

    /// Returns the payload length.
    #[must_use]
    pub const fn payload_len(&self) -> u32 {
        self.header.length
    }

    /// Returns the total size (header + payload).
    #[must_use]
    pub const fn total_size(&self) -> u64 {
        self.header.total_size()
    }

    /// Encodes the entire entry to bytes.
    pub fn encode(&self, buf: &mut BytesMut) {
        self.header.encode(buf);
        buf.put_slice(&self.payload);
    }

    /// Decodes an entry from bytes.
    ///
    /// # Errors
    /// Returns an error if the data is invalid or corrupted.
    pub fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self> {
        let header = SharedEntryHeader::decode(buf, offset)?;

        if buf.remaining() < header.length as usize {
            return Err(WalError::TruncatedEntry {
                offset,
                expected: header.length,
                #[allow(clippy::cast_possible_truncation)]
                found: buf.remaining() as u32,
            });
        }

        let payload = buf.copy_to_bytes(header.length as usize);

        // Verify checksum.
        header.verify(&payload, offset)?;

        Ok(Self { header, payload })
    }
}

// ----------------------------------------------------------------------------
// WalEntry Implementation for SharedEntry
// ----------------------------------------------------------------------------

impl WalEntry for SharedEntry {
    const HEADER_SIZE: usize = SHARED_ENTRY_HEADER_SIZE;

    fn encode(&self, buf: &mut BytesMut) {
        // Delegate to inherent method.
        Self::encode(self, buf);
    }

    fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self> {
        // Delegate to inherent method.
        Self::decode(buf, offset)
    }

    fn total_size(&self) -> u64 {
        // Delegate to inherent method.
        Self::total_size(self)
    }

    fn index(&self) -> u64 {
        // Delegate to inherent method.
        Self::index(self)
    }

    fn payload_len(&self) -> u32 {
        // Delegate to inherent method.
        Self::payload_len(self)
    }

    /// `SharedEntry` uses partition-local indices, not WAL-global.
    ///
    /// Sequentiality is enforced per-partition by the `SharedWal` coordination
    /// layer, not by the segment.
    fn uses_global_index() -> bool {
        false
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_entry_roundtrip() {
        let partition_id = PartitionId::new(42);
        let payload = Bytes::from("hello, shared world!");
        let entry = SharedEntry::new(partition_id, 1, 100, payload.clone()).unwrap();

        assert_eq!(entry.partition_id(), partition_id);
        assert_eq!(entry.term(), 1);
        assert_eq!(entry.index(), 100);
        assert_eq!(entry.payload, payload);

        // Encode.
        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Verify encoded size.
        assert_eq!(buf.len(), SHARED_ENTRY_HEADER_SIZE + payload.len());

        // Decode.
        let decoded = SharedEntry::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_shared_entry_checksum_detects_corruption() {
        let entry =
            SharedEntry::new(PartitionId::new(1), 1, 1, Bytes::from("test")).unwrap();

        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Corrupt a byte in the payload.
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        // Should fail with checksum mismatch.
        let result = SharedEntry::decode(&mut buf.freeze(), 0);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_shared_entry_too_large() {
        let payload = Bytes::from(vec![0u8; ENTRY_PAYLOAD_SIZE_BYTES_MAX as usize + 1]);
        let result = SharedEntry::new(PartitionId::new(1), 1, 1, payload);
        assert!(matches!(result, Err(WalError::EntryTooLarge { .. })));
    }

    #[test]
    fn test_shared_entry_header_size() {
        // Verify our constant matches actual encoded size.
        let header =
            SharedEntryHeader::new(PartitionId::new(1), 1, 1, &[]).unwrap();
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), SHARED_ENTRY_HEADER_SIZE);
    }

    #[test]
    fn test_truncated_shared_entry() {
        let entry =
            SharedEntry::new(PartitionId::new(1), 1, 1, Bytes::from("hello")).unwrap();
        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Truncate the buffer.
        buf.truncate(SHARED_ENTRY_HEADER_SIZE + 2);

        let result = SharedEntry::decode(&mut buf.freeze(), 0);
        assert!(matches!(result, Err(WalError::TruncatedEntry { .. })));
    }

    #[test]
    fn test_shared_entry_walentry_trait() {
        // Verify SharedEntry implements WalEntry correctly.
        let entry =
            SharedEntry::new(PartitionId::new(99), 5, 42, Bytes::from("data")).unwrap();

        // Check trait constants and methods.
        assert_eq!(SharedEntry::HEADER_SIZE, SHARED_ENTRY_HEADER_SIZE);
        assert_eq!(WalEntry::index(&entry), 42);
        assert_eq!(WalEntry::payload_len(&entry), 4);
        assert_eq!(
            WalEntry::total_size(&entry),
            SHARED_ENTRY_HEADER_SIZE as u64 + 4
        );

        // Encode via trait.
        let mut buf = BytesMut::new();
        WalEntry::encode(&entry, &mut buf);

        // Decode via trait.
        let decoded = <SharedEntry as WalEntry>::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_shared_entry_multiple_partitions() {
        // Test entries from different partitions can be distinguished.
        let entry1 =
            SharedEntry::new(PartitionId::new(1), 1, 1, Bytes::from("p1")).unwrap();
        let entry2 =
            SharedEntry::new(PartitionId::new(2), 1, 1, Bytes::from("p2")).unwrap();
        let entry3 =
            SharedEntry::new(PartitionId::new(1), 1, 2, Bytes::from("p1-2")).unwrap();

        assert_ne!(entry1.partition_id(), entry2.partition_id());
        assert_eq!(entry1.partition_id(), entry3.partition_id());
        assert_eq!(entry1.index(), entry2.index());
        assert_ne!(entry1.index(), entry3.index());
    }
}

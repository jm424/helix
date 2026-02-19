//! WAL entry format.
//!
//! Each entry in the WAL has the following binary format:
//!
//! ```text
//! +----------+----------+----------+----------+----------+
//! |  CRC32   |  Length  |   Term   |  Index   | Payload  |
//! | (4 bytes)| (4 bytes)| (8 bytes)| (8 bytes)| (N bytes)|
//! +----------+----------+----------+----------+----------+
//! ```
//!
//! - CRC32: Checksum of Length + Term + Index + Payload (NOT including CRC itself)
//! - Length: Payload length in bytes (not including header)
//! - Term: Raft term when entry was created
//! - Index: Log index of this entry
//! - Payload: Application data
//!
//! All integers are stored in little-endian format.
//!
//! # Generic Entry Support
//!
//! The [`WalEntry`] trait allows the WAL to work with different entry formats.
//! The default [`Entry`] type is suitable for single-partition WALs. For shared
//! WALs that serve multiple partitions, use [`SharedEntry`](crate::SharedEntry)
//! which includes a partition ID.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{WalError, WalResult};
use crate::limits::ENTRY_PAYLOAD_SIZE_BYTES_MAX;

// ----------------------------------------------------------------------------
// WalEntry Trait
// ----------------------------------------------------------------------------

/// Trait for WAL entry serialization and deserialization.
///
/// This trait allows the WAL to be generic over entry format while reusing
/// all segment management, rotation, and recovery logic. The default [`Entry`]
/// type works for single-partition WALs; [`SharedEntry`](crate::SharedEntry)
/// adds a partition ID for multi-partition shared WALs.
///
/// # Implementors
///
/// - [`Entry`] - Standard entry with term, index, and payload (24-byte header)
/// - [`SharedEntry`](crate::SharedEntry) - Entry with partition ID (40-byte header)
///
/// # Example
///
/// ```ignore
/// use helix_wal::{WalEntry, Entry};
///
/// // Entry implements WalEntry
/// let entry = Entry::new(1, 1, Bytes::from("data"))?;
/// assert_eq!(entry.index(), 1);
/// assert_eq!(Entry::HEADER_SIZE, 24);
/// ```
pub trait WalEntry: Sized + Clone + Send + Sync + std::fmt::Debug {
    /// Size of the fixed header in bytes (before variable-length payload).
    const HEADER_SIZE: usize;

    /// Encodes the entry to bytes.
    fn encode(&self, buf: &mut BytesMut);

    /// Decodes an entry from bytes.
    ///
    /// The `offset` parameter is the byte offset in the file, used for error reporting.
    ///
    /// # Errors
    /// Returns an error if the data is invalid or corrupted.
    fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self>;

    /// Returns the total size in bytes (header + payload).
    fn total_size(&self) -> u64;

    /// Returns the log index of this entry.
    fn index(&self) -> u64;

    /// Returns the payload length in bytes.
    fn payload_len(&self) -> u32;

    /// Whether this entry type uses WAL-global sequential indices.
    ///
    /// Returns `true` for single-partition entries ([`Entry`]) where `index()`
    /// returns the WAL position and must be globally sequential.
    ///
    /// Returns `false` for shared entries ([`SharedEntry`](crate::SharedEntry))
    /// where `index()` returns partition-local indices. In this case, the WAL
    /// stores entries in append order and sequentiality is enforced per-partition
    /// by the coordination layer.
    #[must_use]
    fn uses_global_index() -> bool {
        true
    }

    /// Extracts `(index, payload_len)` from raw header bytes without full decode.
    ///
    /// Used during WAL open to scan segment metadata without allocating entry payloads,
    /// preventing OOM when recovering large WALs with many sealed segments.
    ///
    /// `header_bytes` must have at least `HEADER_SIZE` bytes.
    ///
    /// Entry types with `uses_global_index() == false` (e.g. `SharedEntry`) must
    /// override this method. The default panics.
    ///
    /// # Panics
    /// Panics if not overridden by an entry type that returns `false` from
    /// `uses_global_index()`.
    #[must_use]
    fn scan_header_info(header_bytes: &[u8]) -> (u64, u32) {
        let _ = header_bytes;
        panic!("scan_header_info not implemented for this entry type")
    }
}

// ----------------------------------------------------------------------------
// Entry Type (Standard Single-Partition Entry)
// ----------------------------------------------------------------------------

/// Size of the entry header in bytes.
pub const ENTRY_HEADER_SIZE: usize = 24; // 4 + 4 + 8 + 8

/// Magic bytes to identify valid entries (not stored, used for validation).
const ENTRY_LENGTH_MAX: u32 = ENTRY_PAYLOAD_SIZE_BYTES_MAX;

/// Entry header containing metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryHeader {
    /// CRC32 checksum of the rest of the entry.
    pub crc: u32,
    /// Length of the payload in bytes.
    pub length: u32,
    /// Raft term when this entry was created.
    pub term: u64,
    /// Log index of this entry.
    pub index: u64,
}

impl EntryHeader {
    /// Creates a new entry header.
    ///
    /// The CRC is computed from the other fields and the payload.
    ///
    /// # Errors
    /// Returns an error if the payload is too large.
    pub fn new(term: u64, index: u64, payload: &[u8]) -> WalResult<Self> {
        let length = payload.len();

        // TigerStyle: Check limits explicitly.
        if length > ENTRY_LENGTH_MAX as usize {
            #[allow(clippy::cast_possible_truncation)]
            // Truncated value is fine for error reporting.
            return Err(WalError::EntryTooLarge {
                size: length as u32,
                max: ENTRY_LENGTH_MAX,
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let length = length as u32;

        // Compute CRC over length + term + index + payload.
        let crc = Self::compute_crc(length, term, index, payload);

        Ok(Self {
            crc,
            length,
            term,
            index,
        })
    }

    /// Computes the CRC32 checksum for an entry.
    fn compute_crc(length: u32, term: u64, index: u64, payload: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&length.to_le_bytes());
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
        let expected = Self::compute_crc(self.length, self.term, self.index, payload);
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
        buf.put_u64_le(self.term);
        buf.put_u64_le(self.index);
    }

    /// Decodes a header from bytes.
    ///
    /// # Errors
    /// Returns an error if the buffer is too small or the header is invalid.
    pub fn decode(buf: &mut impl Buf, offset: u64) -> WalResult<Self> {
        if buf.remaining() < ENTRY_HEADER_SIZE {
            return Err(WalError::InvalidHeader {
                offset,
                reason: "buffer too small for header",
            });
        }

        let crc = buf.get_u32_le();
        let length = buf.get_u32_le();
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
            term,
            index,
        })
    }

    /// Returns the total size of the entry (header + payload).
    #[must_use]
    pub const fn total_size(&self) -> u64 {
        ENTRY_HEADER_SIZE as u64 + self.length as u64
    }
}

/// A complete WAL entry (header + payload).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Entry header with metadata.
    pub header: EntryHeader,
    /// Entry payload (application data).
    pub payload: Bytes,
}

impl Entry {
    /// Creates a new entry.
    ///
    /// # Errors
    /// Returns an error if the payload is too large.
    pub fn new(term: u64, index: u64, payload: Bytes) -> WalResult<Self> {
        let header = EntryHeader::new(term, index, &payload)?;
        Ok(Self { header, payload })
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
        let header = EntryHeader::decode(buf, offset)?;

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
// WalEntry Implementation for Entry
// ----------------------------------------------------------------------------

impl WalEntry for Entry {
    const HEADER_SIZE: usize = ENTRY_HEADER_SIZE;

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

    fn scan_header_info(header_bytes: &[u8]) -> (u64, u32) {
        debug_assert!(header_bytes.len() >= ENTRY_HEADER_SIZE);
        // Entry layout: [crc:u32(4), length:u32(4), term:u64(8), index:u64(8)]
        let payload_len =
            u32::from_le_bytes(header_bytes[4..8].try_into().expect("4 bytes"));
        let index =
            u64::from_le_bytes(header_bytes[16..24].try_into().expect("8 bytes"));
        (index, payload_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_roundtrip() {
        let payload = Bytes::from("hello, world!");
        let entry = Entry::new(1, 42, payload.clone()).unwrap();

        assert_eq!(entry.term(), 1);
        assert_eq!(entry.index(), 42);
        assert_eq!(entry.payload, payload);

        // Encode.
        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Decode.
        let decoded = Entry::decode(&mut buf.freeze(), 0).unwrap();
        assert_eq!(decoded, entry);
    }

    #[test]
    fn test_entry_checksum_detects_corruption() {
        let entry = Entry::new(1, 1, Bytes::from("test")).unwrap();

        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Corrupt a byte in the payload.
        let len = buf.len();
        buf[len - 1] ^= 0xFF;

        // Should fail with checksum mismatch.
        let result = Entry::decode(&mut buf.freeze(), 0);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn test_entry_too_large() {
        let payload = Bytes::from(vec![0u8; ENTRY_PAYLOAD_SIZE_BYTES_MAX as usize + 1]);
        let result = Entry::new(1, 1, payload);
        assert!(matches!(result, Err(WalError::EntryTooLarge { .. })));
    }

    #[test]
    fn test_entry_header_size() {
        // Verify our constant matches actual encoded size.
        let header = EntryHeader::new(1, 1, &[]).unwrap();
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), ENTRY_HEADER_SIZE);
    }

    #[test]
    fn test_truncated_entry() {
        let entry = Entry::new(1, 1, Bytes::from("hello")).unwrap();
        let mut buf = BytesMut::new();
        entry.encode(&mut buf);

        // Truncate the buffer.
        buf.truncate(ENTRY_HEADER_SIZE + 2);

        let result = Entry::decode(&mut buf.freeze(), 0);
        assert!(matches!(result, Err(WalError::TruncatedEntry { .. })));
    }
}

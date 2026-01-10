//! WAL error types.
//!
//! All errors are explicit and typed. No string errors.

use thiserror::Error;

/// Result type for WAL operations.
pub type WalResult<T> = Result<T, WalError>;

/// Errors that can occur during WAL operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// Entry payload exceeds maximum size.
    #[error("entry too large: {size} bytes exceeds max {max} bytes")]
    EntryTooLarge {
        /// Actual size in bytes.
        size: u32,
        /// Maximum allowed size.
        max: u32,
    },

    /// Segment is full (size or entry count limit reached).
    #[error("segment full: {reason}")]
    SegmentFull {
        /// Why the segment is full.
        reason: &'static str,
    },

    /// CRC checksum mismatch indicates corruption.
    #[error("checksum mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// Byte offset where corruption was detected.
        offset: u64,
        /// Expected CRC32 value.
        expected: u32,
        /// Actual CRC32 value computed.
        actual: u32,
    },

    /// Entry header is invalid or corrupted.
    #[error("invalid entry header at offset {offset}: {reason}")]
    InvalidHeader {
        /// Byte offset of the header.
        offset: u64,
        /// Why the header is invalid.
        reason: &'static str,
    },

    /// Attempted to read past end of segment.
    #[error("read past end of segment: offset {offset}, segment size {segment_size}")]
    ReadPastEnd {
        /// Requested offset.
        offset: u64,
        /// Actual segment size.
        segment_size: u64,
    },

    /// Index out of bounds.
    #[error("index {index} out of bounds (valid range: {first}..{last})")]
    IndexOutOfBounds {
        /// Requested index.
        index: u64,
        /// First valid index.
        first: u64,
        /// Last valid index (inclusive).
        last: u64,
    },

    /// I/O error from underlying storage.
    #[error("I/O error: {operation}: {message}")]
    Io {
        /// What operation was being performed.
        operation: &'static str,
        /// Error message.
        message: String,
    },

    /// Segment not found.
    #[error("segment {segment_id} not found")]
    SegmentNotFound {
        /// The segment ID that was not found.
        segment_id: u64,
    },

    /// Recovery detected truncated write (torn write).
    #[error("truncated entry at offset {offset}: expected {expected} bytes, found {found} bytes")]
    TruncatedEntry {
        /// Offset of the truncated entry.
        offset: u64,
        /// Expected entry size.
        expected: u32,
        /// Actual bytes found.
        found: u32,
    },
}

impl WalError {
    /// Creates an I/O error.
    pub fn io(operation: &'static str, err: impl std::fmt::Display) -> Self {
        Self::Io {
            operation,
            message: err.to_string(),
        }
    }

    /// Returns true if this error indicates data corruption.
    #[must_use]
    pub const fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::ChecksumMismatch { .. } | Self::InvalidHeader { .. }
        )
    }

    /// Returns true if this error is recoverable (e.g., torn write).
    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(self, Self::TruncatedEntry { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = WalError::ChecksumMismatch {
            offset: 1024,
            expected: 0xDEAD_BEEF,
            actual: 0xCAFE_BABE,
        };
        let msg = format!("{err}");
        assert!(msg.contains("1024"));
        assert!(msg.contains("0xdeadbeef"));
        assert!(msg.contains("0xcafebabe"));
    }

    #[test]
    fn test_is_corruption() {
        assert!(WalError::ChecksumMismatch {
            offset: 0,
            expected: 0,
            actual: 1
        }
        .is_corruption());

        assert!(!WalError::SegmentFull { reason: "test" }.is_corruption());
    }
}

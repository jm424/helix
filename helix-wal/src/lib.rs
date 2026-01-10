//! Helix WAL - Write-Ahead Log implementation.
//!
//! This crate provides a high-performance, crash-safe write-ahead log
//! for durably storing Raft entries and partition data.
//!
//! # Design Principles (`TigerStyle`)
//!
//! - **Crash safety**: All committed data survives crashes
//! - **Checksums**: CRC32 on every entry to detect corruption
//! - **Explicit limits**: Bounded segment sizes, entry counts
//! - **No unsafe code**: Safety > Performance

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod entry;
mod error;
mod segment;

pub use entry::{Entry, EntryHeader, ENTRY_HEADER_SIZE};
pub use error::{WalError, WalResult};
pub use segment::{Segment, SegmentConfig, SegmentId};

/// WAL configuration limits.
pub mod limits {
    /// Maximum size of a single entry payload in bytes (1 MB).
    pub const ENTRY_PAYLOAD_SIZE_BYTES_MAX: u32 = 1024 * 1024;

    /// Maximum size of a segment in bytes (1 GB).
    pub const SEGMENT_SIZE_BYTES_MAX: u64 = 1024 * 1024 * 1024;

    /// Minimum size of a segment in bytes (1 MB).
    pub const SEGMENT_SIZE_BYTES_MIN: u64 = 1024 * 1024;

    /// Maximum number of entries per segment.
    pub const ENTRIES_PER_SEGMENT_MAX: u64 = 10_000_000;
}

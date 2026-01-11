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
//!
//! # Example
//!
//! ```ignore
//! use helix_wal::{Wal, WalConfig, TokioStorage, Entry};
//! use bytes::Bytes;
//!
//! let config = WalConfig::new("/tmp/wal");
//! let mut wal = Wal::open(TokioStorage::new(), config).await?;
//!
//! // Append entries.
//! let entry = Entry::new(term, index, Bytes::from("data"))?;
//! wal.append(entry).await?;
//!
//! // Sync for durability.
//! wal.sync().await?;
//!
//! // Read entries.
//! let entry = wal.read(index)?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod buffered;
mod entry;
mod error;
mod segment;
mod storage;
mod wal;

pub use buffered::{BufferedWal, BufferedWalConfig};
pub use entry::{Entry, EntryHeader, ENTRY_HEADER_SIZE};
pub use error::{WalError, WalResult};
pub use segment::{Segment, SegmentConfig, SegmentHeader, SegmentId, SEGMENT_HEADER_SIZE};
pub use storage::{FaultConfig, SimulatedStorage, Storage, StorageFile, TokioStorage};
pub use wal::{Wal, WalConfig};

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

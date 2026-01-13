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
mod shared_entry;
mod shared_wal;
mod storage;
mod wal;

pub use buffered::{BufferedWal, BufferedWalConfig};
pub use entry::{Entry, EntryHeader, WalEntry, ENTRY_HEADER_SIZE};
pub use error::{WalError, WalResult};
pub use segment::{Segment, SegmentConfig, SegmentHeader, SegmentId, SEGMENT_HEADER_SIZE};
pub use shared_entry::{SharedEntry, SharedEntryHeader, SHARED_ENTRY_HEADER_SIZE};
pub use shared_wal::{
    CoordinatorConfig, DurableAck, PoolConfig, SharedWal, SharedWalConfig, SharedWalCoordinator,
    SharedWalHandle, SharedWalPool, POOL_WAL_COUNT_MAX,
};
pub use storage::{create_storage, FaultConfig, SimulatedStorage, Storage, StorageFile, TokioStorage};

// Low-level io_uring types (for direct use within tokio_uring context).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use storage::{IoUringFile, IoUringStorage};

// Worker-based io_uring types (Send + Sync, for use with standard tokio).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use storage::{IoUringInitError, IoUringWorkerFile, IoUringWorkerStorage};

pub use wal::{SegmentInfo, Wal, WalConfig};

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

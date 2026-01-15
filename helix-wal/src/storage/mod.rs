//! Storage abstraction for WAL.
//!
//! This module provides a trait-based storage abstraction allowing different
//! backends (`tokio::fs`, `io_uring`, in-memory for testing).
//!
//! # Design
//!
//! The storage trait is intentionally simple - it handles raw bytes at offsets.
//! Higher-level concerns (segments, entries, checksums) are handled by the WAL.
//!
//! # Available Backends
//!
//! - [`TokioStorage`]: Production storage using `tokio::fs` (all platforms)
//! - [`SimulatedStorage`]: In-memory storage for deterministic simulation testing
//! - [`IoUringWorkerStorage`]: High-performance Linux storage using `io_uring` (Linux only, feature-gated)
//!
//! # Selecting a Backend
//!
//! Use [`create_storage`] to automatically select the best backend for the platform:
//!
//! ```ignore
//! use helix_wal::storage::create_storage;
//!
//! let storage = create_storage();
//! ```
//!
//! On Linux with the `io-uring` feature enabled, this returns [`IoUringWorkerStorage`].
//! Otherwise, it returns [`TokioStorage`].

mod simulated;
mod tokio_storage;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod io_uring_storage;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod io_uring_worker;

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::WalResult;

// Re-export storage implementations.
pub use simulated::{FaultConfig, FaultStats, SimulatedFile, SimulatedStorage};
pub use tokio_storage::TokioStorage;

// Re-export low-level io_uring types (for direct use within tokio_uring context).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_storage::{IoUringFile, IoUringStorage};

// Re-export worker-based io_uring types (Send + Sync, for use with standard tokio).
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_worker::{IoUringInitError, IoUringWorkerFile, IoUringWorkerStorage};

/// Creates the appropriate storage backend for the platform.
///
/// On Linux with the `io-uring` feature enabled, tries to create an
/// [`IoUringWorkerStorage`]. If initialization fails, falls back to
/// [`TokioStorage`].
///
/// On other platforms (or without the feature), returns [`TokioStorage`].
///
/// # Example
///
/// ```ignore
/// use helix_wal::storage::create_storage;
///
/// let storage = create_storage();
/// let file = storage.open(Path::new("/tmp/test.wal")).await?;
/// ```
pub fn create_storage() -> Box<dyn Storage> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        match IoUringWorkerStorage::try_new() {
            Ok(storage) => {
                tracing::info!("using io_uring storage backend");
                return Box::new(storage);
            }
            Err(e) => {
                tracing::warn!("io_uring init failed ({e}), falling back to tokio::fs");
            }
        }
    }

    tracing::info!("using tokio::fs storage backend");
    Box::new(TokioStorage::new())
}

/// Storage backend trait for WAL segment files.
///
/// Implementations must be `Send + Sync` for use across async tasks.
/// All operations are fallible and return [`WalResult`].
#[async_trait]
pub trait Storage: Send + Sync {
    /// Opens or creates a file at the given path.
    ///
    /// If the file exists, it is opened for read/write.
    /// If the file does not exist, it is created.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or created.
    async fn open(&self, path: &Path) -> WalResult<Box<dyn StorageFile>>;

    /// Checks if a file exists at the given path.
    async fn exists(&self, path: &Path) -> WalResult<bool>;

    /// Lists files in a directory matching a pattern.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read.
    async fn list_files(&self, dir: &Path, extension: &str) -> WalResult<Vec<std::path::PathBuf>>;

    /// Removes a file at the given path.
    ///
    /// # Errors
    /// Returns an error if the file cannot be removed.
    async fn remove(&self, path: &Path) -> WalResult<()>;

    /// Creates a directory and all parent directories.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created.
    async fn create_dir_all(&self, path: &Path) -> WalResult<()>;
}

/// A handle to an open file for reading and writing.
#[async_trait]
pub trait StorageFile: Send + Sync {
    /// Writes data at the specified offset.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()>;

    /// Reads data from the specified offset.
    ///
    /// Returns the bytes read. May return fewer bytes than requested if EOF.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes>;

    /// Reads the entire file contents.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    async fn read_all(&self) -> WalResult<Bytes>;

    /// Syncs all buffered data to disk (fsync).
    ///
    /// This ensures durability - data written before `sync()` will survive crashes.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    async fn sync(&self) -> WalResult<()>;

    /// Returns the current file size in bytes.
    ///
    /// # Errors
    /// Returns an error if the size cannot be determined.
    async fn size(&self) -> WalResult<u64>;

    /// Truncates the file to the specified length.
    ///
    /// # Errors
    /// Returns an error if the truncation fails.
    async fn truncate(&self, len: u64) -> WalResult<()>;

    /// Writes data at the specified offset and syncs to disk.
    ///
    /// This is an optimization for storage backends that can combine write+sync
    /// more efficiently than separate calls (e.g., `io_uring` with linked SQEs).
    ///
    /// The default implementation just calls `write_at` followed by `sync`.
    ///
    /// # Errors
    /// Returns an error if the write or sync fails.
    async fn write_at_and_sync(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        self.write_at(offset, data).await?;
        self.sync().await
    }
}

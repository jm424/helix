//! io_uring-based storage implementation for Linux.
//!
//! This module provides a high-performance storage backend using Linux's
//! io_uring interface for true async I/O without thread pool overhead.
//!
//! # Requirements
//!
//! - Linux kernel 5.1+ (5.6+ recommended for best performance)
//! - The `io-uring` feature must be enabled
//!
//! # Runtime
//!
//! This storage backend requires the `tokio_uring` runtime. It will NOT work
//! with the standard tokio runtime. Use `tokio_uring::start()` to run code
//! that uses this storage.
//!
//! # Design Note
//!
//! tokio-uring is fundamentally single-threaded (uses Rc internally), so these
//! types do NOT implement the `Storage`/`StorageFile` traits which require
//! `Send + Sync`. Instead, they provide a similar API for direct use within
//! a `tokio_uring::start()` context.
//!
//! For production integration with the standard tokio runtime, use the worker
//! thread abstraction in `io_uring_worker.rs` (Phase 3).
//!
//! # Example
//!
//! ```ignore
//! use helix_wal::IoUringStorage;
//! use std::path::Path;
//!
//! tokio_uring::start(async {
//!     let storage = IoUringStorage::new();
//!     let file = storage.open(Path::new("/tmp/test.wal")).await?;
//!     file.write_at(0, b"hello").await?;
//!     file.sync().await?;
//!     Ok::<_, helix_wal::WalError>(())
//! });
//! ```

use std::path::{Path, PathBuf};
use std::rc::Rc;

use bytes::Bytes;
use tokio_uring::fs::{File, OpenOptions};

use crate::error::{WalError, WalResult};

/// io_uring-based storage implementation.
///
/// Provides true async I/O on Linux without thread pool overhead. Each file
/// operation is submitted directly to the kernel via io_uring.
///
/// # Performance
///
/// io_uring can provide 2-3x throughput improvement over `tokio::fs` for
/// write-heavy workloads like WAL, especially when combined with fsync.
///
/// # Thread Safety
///
/// This type is NOT `Send` or `Sync`. It must be used within a single
/// `tokio_uring::start()` context on a single thread.
#[derive(Debug, Clone, Default)]
pub struct IoUringStorage;

impl IoUringStorage {
    /// Creates a new io_uring storage instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Opens or creates a file at the given path.
    ///
    /// If the file exists, it is opened for read/write.
    /// If the file does not exist, it is created.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or created.
    pub async fn open(&self, path: &Path) -> WalResult<IoUringFile> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await
            .map_err(|e| WalError::io("open", e))?;

        Ok(IoUringFile {
            file: Rc::new(file),
            path: path.to_path_buf(),
        })
    }

    /// Checks if a file exists at the given path.
    pub fn exists(&self, path: &Path) -> WalResult<bool> {
        // tokio_uring doesn't have try_exists, use std::fs.
        // This is a quick metadata check, not worth io_uring overhead.
        Ok(path.exists())
    }

    /// Lists files in a directory matching a pattern.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read.
    pub fn list_files(&self, dir: &Path, extension: &str) -> WalResult<Vec<PathBuf>> {
        // Directory listing is not performance-critical, use std::fs.
        let entries = std::fs::read_dir(dir).map_err(|e| WalError::io("read_dir", e))?;

        let mut files = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| WalError::io("read_dir_entry", e))?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == extension) {
                files.push(path);
            }
        }

        // Sort by filename for deterministic ordering.
        files.sort();
        Ok(files)
    }

    /// Removes a file at the given path.
    ///
    /// # Errors
    /// Returns an error if the file cannot be removed.
    pub fn remove(&self, path: &Path) -> WalResult<()> {
        // tokio_uring doesn't have remove_file, use std::fs.
        std::fs::remove_file(path).map_err(|e| WalError::io("remove", e))
    }

    /// Creates a directory and all parent directories.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created.
    pub fn create_dir_all(&self, path: &Path) -> WalResult<()> {
        // Directory creation is not performance-critical, use std::fs.
        std::fs::create_dir_all(path).map_err(|e| WalError::io("create_dir_all", e))
    }
}

/// io_uring file handle.
///
/// Note: tokio_uring requires buffers to be owned during operations, so we
/// copy data internally. This is still faster than thread pool overhead for
/// most WAL workloads.
///
/// # Thread Safety
///
/// This type is NOT `Send` or `Sync`. It must be used within a single
/// `tokio_uring::start()` context on a single thread.
#[derive(Clone)]
pub struct IoUringFile {
    file: Rc<File>,
    path: PathBuf,
}

impl IoUringFile {
    /// Writes data at the specified offset.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        // tokio_uring requires owned buffers, so we need to copy.
        let buf = data.to_vec();
        // write_at returns UnsubmittedWrite, need to call submit() before await.
        let (result, _buf) = self.file.write_at(buf, offset).submit().await;
        result.map_err(|e| WalError::io("write", e))?;
        Ok(())
    }

    /// Reads data from the specified offset.
    ///
    /// Returns the bytes read. May return fewer bytes than requested if EOF.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    pub async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        let buf = vec![0u8; len];
        let (result, buf) = self.file.read_at(buf, offset).await;
        let n = result.map_err(|e| WalError::io("read", e))?;
        Ok(Bytes::from(buf[..n].to_vec()))
    }

    /// Reads the entire file contents.
    ///
    /// # Errors
    /// Returns an error if the read fails.
    pub async fn read_all(&self) -> WalResult<Bytes> {
        // Get file size first.
        let metadata = std::fs::metadata(&self.path).map_err(|e| WalError::io("metadata", e))?;
        #[allow(clippy::cast_possible_truncation)]
        let len = metadata.len() as usize;

        if len == 0 {
            return Ok(Bytes::new());
        }

        let buf = vec![0u8; len];
        let (result, buf) = self.file.read_at(buf, 0).await;
        let n = result.map_err(|e| WalError::io("read", e))?;
        Ok(Bytes::from(buf[..n].to_vec()))
    }

    /// Syncs all buffered data to disk (fsync).
    ///
    /// This ensures durability - data written before `sync()` will survive crashes.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub async fn sync(&self) -> WalResult<()> {
        self.file
            .sync_all()
            .await
            .map_err(|e| WalError::io("sync", e))
    }

    /// Returns the current file size in bytes.
    ///
    /// # Errors
    /// Returns an error if the size cannot be determined.
    pub fn size(&self) -> WalResult<u64> {
        // Use std::fs for metadata - not worth io_uring overhead.
        let metadata = std::fs::metadata(&self.path).map_err(|e| WalError::io("metadata", e))?;
        Ok(metadata.len())
    }

    /// Truncates the file to the specified length.
    ///
    /// # Errors
    /// Returns an error if the truncation fails.
    pub fn truncate(&self, len: u64) -> WalResult<()> {
        // tokio_uring doesn't have set_len directly, but we can use std::fs.
        // This requires reopening the file briefly.
        let std_file =
            std::fs::OpenOptions::new()
                .write(true)
                .open(&self.path)
                .map_err(|e| WalError::io("open_for_truncate", e))?;
        std_file
            .set_len(len)
            .map_err(|e| WalError::io("truncate", e))
    }

    /// Returns the path to the file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require tokio_uring runtime.
    // Run with: cargo test --features io-uring

    #[test]
    fn test_io_uring_storage_write_read() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.wal");

            let storage = IoUringStorage::new();
            let file = storage.open(&path).await.unwrap();

            // Write some data.
            let data = b"hello, world!";
            file.write_at(0, data).await.unwrap();
            file.sync().await.unwrap();

            // Read it back.
            let read_data = file.read_at(0, data.len()).await.unwrap();
            assert_eq!(&read_data[..], data);

            // Check size.
            let size = file.size().unwrap();
            assert_eq!(size, data.len() as u64);
        });
    }

    #[test]
    fn test_io_uring_storage_read_all() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.wal");

            let storage = IoUringStorage::new();
            let file = storage.open(&path).await.unwrap();

            let data = b"some test data for reading";
            file.write_at(0, data).await.unwrap();
            file.sync().await.unwrap();

            let all_data = file.read_all().await.unwrap();
            assert_eq!(&all_data[..], data);
        });
    }

    #[test]
    fn test_io_uring_storage_list_files() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();

            let storage = IoUringStorage::new();

            // Create some files.
            for i in 0..3 {
                let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
                let _ = storage.open(&path).await.unwrap();
            }

            // Also create a non-.wal file.
            let other_path = temp_dir.path().join("other.txt");
            let _ = storage.open(&other_path).await.unwrap();

            // List .wal files.
            let files = storage.list_files(temp_dir.path(), "wal").unwrap();
            assert_eq!(files.len(), 3);
        });
    }

    #[test]
    fn test_io_uring_storage_exists() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.wal");

            let storage = IoUringStorage::new();

            assert!(!storage.exists(&path).unwrap());

            let _ = storage.open(&path).await.unwrap();
            assert!(storage.exists(&path).unwrap());
        });
    }

    #[test]
    fn test_io_uring_storage_truncate() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.wal");

            let storage = IoUringStorage::new();
            let file = storage.open(&path).await.unwrap();

            // Write some data.
            let data = b"hello, world!";
            file.write_at(0, data).await.unwrap();
            file.sync().await.unwrap();

            // Truncate to 5 bytes.
            file.truncate(5).unwrap();

            // Check size.
            let size = file.size().unwrap();
            assert_eq!(size, 5);

            // Read back.
            let read_data = file.read_at(0, 5).await.unwrap();
            assert_eq!(&read_data[..], b"hello");
        });
    }

    #[test]
    fn test_io_uring_storage_write_at_offset() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();
            let path = temp_dir.path().join("test.wal");

            let storage = IoUringStorage::new();
            let file = storage.open(&path).await.unwrap();

            // Write at offset 0.
            file.write_at(0, b"hello").await.unwrap();
            // Write at offset 5.
            file.write_at(5, b" world").await.unwrap();
            file.sync().await.unwrap();

            // Read all.
            let data = file.read_all().await.unwrap();
            assert_eq!(&data[..], b"hello world");
        });
    }

    #[test]
    fn test_io_uring_storage_multiple_files() {
        tokio_uring::start(async {
            let temp_dir = tempfile::tempdir().unwrap();

            let storage = IoUringStorage::new();

            // Create and write to multiple files.
            for i in 0..5 {
                let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
                let file = storage.open(&path).await.unwrap();
                let data = format!("data for segment {i}");
                file.write_at(0, data.as_bytes()).await.unwrap();
                file.sync().await.unwrap();
            }

            // Verify each file.
            for i in 0..5 {
                let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
                let file = storage.open(&path).await.unwrap();
                let data = file.read_all().await.unwrap();
                let expected = format!("data for segment {i}");
                assert_eq!(&data[..], expected.as_bytes());
            }
        });
    }
}

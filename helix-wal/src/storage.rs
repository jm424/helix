//! Storage abstraction for WAL.
//!
//! This module provides a trait-based storage abstraction allowing different
//! backends (`tokio::fs`, `io_uring`, in-memory for testing).
//!
//! # Design
//!
//! The storage trait is intentionally simple - it handles raw bytes at offsets.
//! Higher-level concerns (segments, entries, checksums) are handled by the WAL.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::{WalError, WalResult};

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
}

/// Tokio-based file storage implementation.
///
/// Uses `tokio::fs` for async file operations. Note that `tokio::fs` uses
/// a thread pool under the hood (`spawn_blocking`), so it's not true async I/O,
/// but it provides a good async API and works on all platforms.
#[derive(Debug, Clone)]
pub struct TokioStorage;

impl TokioStorage {
    /// Creates a new Tokio storage instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for TokioStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for TokioStorage {
    async fn open(&self, path: &Path) -> WalResult<Box<dyn StorageFile>> {
        use tokio::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await
            .map_err(|e| WalError::io("open", e))?;

        Ok(Box::new(TokioFile {
            file: tokio::sync::Mutex::new(file),
        }))
    }

    async fn exists(&self, path: &Path) -> WalResult<bool> {
        Ok(tokio::fs::try_exists(path)
            .await
            .map_err(|e| WalError::io("exists", e))?)
    }

    async fn list_files(&self, dir: &Path, extension: &str) -> WalResult<Vec<std::path::PathBuf>> {
        let mut entries = tokio::fs::read_dir(dir)
            .await
            .map_err(|e| WalError::io("read_dir", e))?;

        let mut files = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| WalError::io("read_dir_entry", e))?
        {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == extension) {
                files.push(path);
            }
        }

        // Sort by filename for deterministic ordering.
        files.sort();
        Ok(files)
    }

    async fn remove(&self, path: &Path) -> WalResult<()> {
        tokio::fs::remove_file(path)
            .await
            .map_err(|e| WalError::io("remove", e))
    }

    async fn create_dir_all(&self, path: &Path) -> WalResult<()> {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| WalError::io("create_dir_all", e))
    }
}

/// A file handle using `tokio::fs`.
struct TokioFile {
    file: tokio::sync::Mutex<tokio::fs::File>,
}

// Allow significant_drop_tightening - holding the lock for the full operation is correct here.
#[allow(clippy::significant_drop_tightening)]

#[async_trait]
impl StorageFile for TokioFile {
    async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        use tokio::io::{AsyncSeekExt, AsyncWriteExt};

        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| WalError::io("seek", e))?;
        file.write_all(data)
            .await
            .map_err(|e| WalError::io("write", e))?;
        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| WalError::io("seek", e))?;

        let mut buf = vec![0u8; len];
        let n = file
            .read(&mut buf)
            .await
            .map_err(|e| WalError::io("read", e))?;
        buf.truncate(n);
        Ok(Bytes::from(buf))
    }

    async fn read_all(&self) -> WalResult<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = self.file.lock().await;
        file.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| WalError::io("seek", e))?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)
            .await
            .map_err(|e| WalError::io("read", e))?;
        Ok(Bytes::from(buf))
    }

    async fn sync(&self) -> WalResult<()> {
        let file = self.file.lock().await;
        file.sync_all()
            .await
            .map_err(|e| WalError::io("sync", e))
    }

    async fn size(&self) -> WalResult<u64> {
        let file = self.file.lock().await;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| WalError::io("metadata", e))?;
        Ok(metadata.len())
    }

    async fn truncate(&self, len: u64) -> WalResult<()> {
        let file = self.file.lock().await;
        file.set_len(len)
            .await
            .map_err(|e| WalError::io("truncate", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tokio_storage_write_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = TokioStorage::new();
        let file = storage.open(&path).await.unwrap();

        // Write some data.
        let data = b"hello, world!";
        file.write_at(0, data).await.unwrap();
        file.sync().await.unwrap();

        // Read it back.
        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(&read_data[..], data);

        // Check size.
        let size = file.size().await.unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_tokio_storage_read_all() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = TokioStorage::new();
        let file = storage.open(&path).await.unwrap();

        let data = b"some test data for reading";
        file.write_at(0, data).await.unwrap();
        file.sync().await.unwrap();

        let all_data = file.read_all().await.unwrap();
        assert_eq!(&all_data[..], data);
    }

    #[tokio::test]
    async fn test_tokio_storage_list_files() {
        let temp_dir = tempfile::tempdir().unwrap();

        let storage = TokioStorage::new();

        // Create some files.
        for i in 0..3 {
            let path = temp_dir.path().join(format!("segment-{i:04}.wal"));
            let _ = storage.open(&path).await.unwrap();
        }

        // Also create a non-.wal file.
        let other_path = temp_dir.path().join("other.txt");
        let _ = storage.open(&other_path).await.unwrap();

        // List .wal files.
        let files = storage.list_files(temp_dir.path(), "wal").await.unwrap();
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_tokio_storage_exists() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.wal");

        let storage = TokioStorage::new();

        assert!(!storage.exists(&path).await.unwrap());

        let _ = storage.open(&path).await.unwrap();
        assert!(storage.exists(&path).await.unwrap());
    }
}

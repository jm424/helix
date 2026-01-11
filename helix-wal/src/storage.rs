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

// ----------------------------------------------------------------------------
// Simulated Storage for Deterministic Simulation Testing (DST)
// ----------------------------------------------------------------------------

/// Configuration for fault injection in simulated storage.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability of a write being torn (partial write). Range: 0.0 - 1.0.
    pub torn_write_rate: f64,
    /// Probability of fsync failing. Range: 0.0 - 1.0.
    pub fsync_fail_rate: f64,
    /// Probability of read returning corrupted data. Range: 0.0 - 1.0.
    pub read_corruption_rate: f64,
    /// If true, next write will be torn at the configured byte offset.
    pub force_torn_write_at: Option<usize>,
    /// If true, next fsync will fail.
    pub force_fsync_fail: bool,
    /// If true, next write will fail with disk full.
    pub force_disk_full: bool,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            torn_write_rate: 0.0,
            fsync_fail_rate: 0.0,
            read_corruption_rate: 0.0,
            force_torn_write_at: None,
            force_fsync_fail: false,
            force_disk_full: false,
        }
    }
}

impl FaultConfig {
    /// Creates a fault config with no faults (for basic testing).
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Creates a fault config that simulates a flaky disk.
    #[must_use]
    pub fn flaky() -> Self {
        Self {
            torn_write_rate: 0.01,
            fsync_fail_rate: 0.005,
            read_corruption_rate: 0.001,
            ..Default::default()
        }
    }

    /// Sets the torn write rate.
    #[must_use]
    pub const fn with_torn_write_rate(mut self, rate: f64) -> Self {
        self.torn_write_rate = rate;
        self
    }

    /// Sets the fsync fail rate.
    #[must_use]
    pub const fn with_fsync_fail_rate(mut self, rate: f64) -> Self {
        self.fsync_fail_rate = rate;
        self
    }

    /// Forces the next write to be torn at the specified byte offset.
    #[must_use]
    pub const fn with_force_torn_write_at(mut self, offset: usize) -> Self {
        self.force_torn_write_at = Some(offset);
        self
    }

    /// Forces the next fsync to fail.
    #[must_use]
    pub const fn with_force_fsync_fail(mut self) -> Self {
        self.force_fsync_fail = true;
        self
    }

    /// Forces the next write to fail with disk full error.
    #[must_use]
    pub const fn with_force_disk_full(mut self) -> Self {
        self.force_disk_full = true;
        self
    }
}

/// In-memory simulated storage for deterministic testing.
///
/// This storage implementation keeps all data in memory and supports
/// configurable fault injection for testing crash recovery scenarios.
///
/// # Panics
///
/// Methods may panic if internal mutex locks are poisoned, which indicates
/// a previous panic in a critical section. This is acceptable for test code.
#[derive(Debug)]
#[allow(clippy::significant_drop_tightening, clippy::missing_panics_doc)]
pub struct SimulatedStorage {
    /// In-memory file contents. Uses interior mutability for trait compliance.
    files: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Vec<u8>>>>,
    /// Fault injection configuration.
    fault_config: std::sync::Arc<std::sync::Mutex<FaultConfig>>,
    /// RNG seed for deterministic fault injection.
    seed: u64,
}

#[allow(clippy::missing_panics_doc)]
impl SimulatedStorage {
    /// Creates a new simulated storage with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            files: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            fault_config: std::sync::Arc::new(std::sync::Mutex::new(FaultConfig::default())),
            seed,
        }
    }

    /// Creates a new simulated storage with fault injection enabled.
    #[must_use]
    pub fn with_faults(seed: u64, config: FaultConfig) -> Self {
        Self {
            files: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            fault_config: std::sync::Arc::new(std::sync::Mutex::new(config)),
            seed,
        }
    }

    /// Returns a reference to the fault configuration for modification.
    pub fn fault_config(&self) -> std::sync::MutexGuard<'_, FaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Gets the raw file content for inspection in tests.
    #[must_use]
    pub fn get_raw_content(&self, path: &Path) -> Option<Vec<u8>> {
        let files = self.files.lock().expect("files lock poisoned");
        files.get(path).cloned()
    }

    /// Sets raw file content directly (for simulating corruption).
    pub fn set_raw_content(&self, path: &Path, content: Vec<u8>) {
        let mut files = self.files.lock().expect("files lock poisoned");
        files.insert(path.to_path_buf(), content);
    }

    /// Corrupts bytes at the specified offset in a file.
    pub fn corrupt_bytes(&self, path: &Path, offset: usize, len: usize) {
        let mut files = self.files.lock().expect("files lock poisoned");
        if let Some(content) = files.get_mut(path) {
            for i in offset..std::cmp::min(offset + len, content.len()) {
                content[i] ^= 0xFF;
            }
        }
    }

    /// Truncates a file to simulate a torn write or crash.
    pub fn truncate_file(&self, path: &Path, len: usize) {
        let mut files = self.files.lock().expect("files lock poisoned");
        if let Some(content) = files.get_mut(path) {
            content.truncate(len);
        }
    }
}

impl Clone for SimulatedStorage {
    fn clone(&self) -> Self {
        Self {
            files: self.files.clone(),
            fault_config: self.fault_config.clone(),
            seed: self.seed,
        }
    }
}

#[async_trait]
#[allow(clippy::significant_drop_tightening)]
impl Storage for SimulatedStorage {
    async fn open(&self, path: &Path) -> WalResult<Box<dyn StorageFile>> {
        let mut files = self.files.lock().expect("files lock poisoned");
        files.entry(path.to_path_buf()).or_default();
        drop(files);

        Ok(Box::new(SimulatedFile {
            path: path.to_path_buf(),
            files: self.files.clone(),
            fault_config: self.fault_config.clone(),
            seed: self.seed,
            write_counter: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    async fn exists(&self, path: &Path) -> WalResult<bool> {
        let files = self.files.lock().expect("files lock poisoned");
        Ok(files.contains_key(path))
    }

    async fn list_files(&self, dir: &Path, extension: &str) -> WalResult<Vec<std::path::PathBuf>> {
        let files = self.files.lock().expect("files lock poisoned");
        let mut result: Vec<_> = files
            .keys()
            .filter(|p| {
                p.parent() == Some(dir) && p.extension().is_some_and(|ext| ext == extension)
            })
            .cloned()
            .collect();
        result.sort();
        Ok(result)
    }

    async fn remove(&self, path: &Path) -> WalResult<()> {
        let mut files = self.files.lock().expect("files lock poisoned");
        files.remove(path);
        Ok(())
    }

    async fn create_dir_all(&self, _path: &Path) -> WalResult<()> {
        // Directories are implicit in simulated storage.
        Ok(())
    }
}

/// A simulated file handle with fault injection.
#[allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
struct SimulatedFile {
    path: std::path::PathBuf,
    files: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Vec<u8>>>>,
    fault_config: std::sync::Arc<std::sync::Mutex<FaultConfig>>,
    seed: u64,
    write_counter: std::sync::atomic::AtomicU64,
}

#[allow(clippy::cast_precision_loss)]
impl SimulatedFile {
    /// Simple deterministic RNG based on seed and counter.
    fn should_inject_fault(&self, rate: f64, counter: u64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0x5851_f42d_4c95_7f2d);
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }
}

#[async_trait]
#[allow(clippy::significant_drop_tightening, clippy::cast_possible_truncation)]
impl StorageFile for SimulatedFile {
    async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        let counter = self
            .write_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let config = self.fault_config.lock().expect("fault config lock poisoned");

        // Check for disk full.
        if config.force_disk_full {
            drop(config);
            let mut config = self.fault_config.lock().expect("fault config lock poisoned");
            config.force_disk_full = false;
            return Err(WalError::Io {
                operation: "write",
                message: "disk full (simulated)".to_string(),
            });
        }

        // Determine if this write should be torn.
        let torn_at = if let Some(torn_offset) = config.force_torn_write_at {
            drop(config);
            let mut config = self.fault_config.lock().expect("fault config lock poisoned");
            config.force_torn_write_at = None;
            Some(torn_offset)
        } else if self.should_inject_fault(config.torn_write_rate, counter) {
            // Random torn write position within the data.
            let hash = self.seed.wrapping_add(counter).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            drop(config);
            Some(hash as usize % data.len().max(1))
        } else {
            drop(config);
            None
        };

        let mut files = self.files.lock().expect("files lock poisoned");
        let content = files.entry(self.path.clone()).or_default();

        // Ensure file is large enough.
        let end_offset = offset as usize + data.len();
        if content.len() < end_offset {
            content.resize(end_offset, 0);
        }

        // Write data (possibly torn).
        let write_len = torn_at.unwrap_or(data.len());
        let actual_write_len = write_len.min(data.len());
        content[offset as usize..offset as usize + actual_write_len]
            .copy_from_slice(&data[..actual_write_len]);

        // If torn, truncate file to simulate crash during write.
        if torn_at.is_some() {
            content.truncate(offset as usize + actual_write_len);
        }

        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        let files = self.files.lock().expect("files lock poisoned");
        let content = files.get(&self.path).ok_or_else(|| WalError::Io {
            operation: "read",
            message: "file not found".to_string(),
        })?;

        let start = offset as usize;
        if start >= content.len() {
            return Ok(Bytes::new());
        }

        let end = std::cmp::min(start + len, content.len());
        let mut data = content[start..end].to_vec();

        // Check for read corruption.
        let config = self.fault_config.lock().expect("fault config lock poisoned");
        let counter = self
            .write_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self.should_inject_fault(config.read_corruption_rate, counter) && !data.is_empty() {
            // Corrupt a random byte.
            let hash = self.seed.wrapping_add(counter).wrapping_mul(0xc6a4_a793_5bd1_e995);
            let corrupt_idx = hash as usize % data.len();
            data[corrupt_idx] ^= 0xFF;
        }

        Ok(Bytes::from(data))
    }

    async fn read_all(&self) -> WalResult<Bytes> {
        let files = self.files.lock().expect("files lock poisoned");
        let content = files.get(&self.path).ok_or_else(|| WalError::Io {
            operation: "read",
            message: "file not found".to_string(),
        })?;

        // Note: read_all doesn't inject corruption to allow clean recovery testing.
        Ok(Bytes::from(content.clone()))
    }

    async fn sync(&self) -> WalResult<()> {
        let config = self.fault_config.lock().expect("fault config lock poisoned");
        let counter = self
            .write_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        if config.force_fsync_fail || self.should_inject_fault(config.fsync_fail_rate, counter) {
            drop(config);
            let mut config = self.fault_config.lock().expect("fault config lock poisoned");
            config.force_fsync_fail = false;
            return Err(WalError::Io {
                operation: "sync",
                message: "fsync failed (simulated)".to_string(),
            });
        }

        // In simulated storage, sync is a no-op (data is already "durable").
        Ok(())
    }

    async fn size(&self) -> WalResult<u64> {
        let files = self.files.lock().expect("files lock poisoned");
        let content = files.get(&self.path).ok_or_else(|| WalError::Io {
            operation: "size",
            message: "file not found".to_string(),
        })?;
        Ok(content.len() as u64)
    }

    async fn truncate(&self, len: u64) -> WalResult<()> {
        let mut files = self.files.lock().expect("files lock poisoned");
        let content = files.entry(self.path.clone()).or_default();
        content.truncate(len as usize);
        Ok(())
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

    // -------------------------------------------------------------------------
    // SimulatedStorage Tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn test_simulated_storage_basic_operations() {
        let storage = SimulatedStorage::new(42);
        let path = Path::new("/test/file.wal");

        // File doesn't exist initially.
        assert!(!storage.exists(path).await.unwrap());

        // Open creates the file.
        let file = storage.open(path).await.unwrap();
        assert!(storage.exists(path).await.unwrap());

        // Write and read.
        let data = b"hello, world!";
        file.write_at(0, data).await.unwrap();

        let read_data = file.read_at(0, data.len()).await.unwrap();
        assert_eq!(&read_data[..], data);

        // Size.
        assert_eq!(file.size().await.unwrap(), data.len() as u64);
    }

    #[tokio::test]
    async fn test_simulated_storage_torn_write() {
        let config = FaultConfig::none().with_force_torn_write_at(5);
        let storage = SimulatedStorage::with_faults(42, config);
        let path = Path::new("/test/torn.wal");

        let file = storage.open(path).await.unwrap();

        // Write 10 bytes, but it will be torn at byte 5.
        let data = b"0123456789";
        file.write_at(0, data).await.unwrap();

        // File should only have 5 bytes.
        let content = storage.get_raw_content(path).unwrap();
        assert_eq!(content.len(), 5);
        assert_eq!(&content[..], b"01234");
    }

    #[tokio::test]
    async fn test_simulated_storage_fsync_failure() {
        let config = FaultConfig::none().with_force_fsync_fail();
        let storage = SimulatedStorage::with_faults(42, config);
        let path = Path::new("/test/sync.wal");

        let file = storage.open(path).await.unwrap();
        file.write_at(0, b"data").await.unwrap();

        // First sync should fail.
        let result = file.sync().await;
        assert!(result.is_err());

        // Second sync should succeed (force flag cleared).
        let result = file.sync().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simulated_storage_disk_full() {
        let config = FaultConfig::none().with_force_disk_full();
        let storage = SimulatedStorage::with_faults(42, config);
        let path = Path::new("/test/full.wal");

        let file = storage.open(path).await.unwrap();

        // First write should fail.
        let result = file.write_at(0, b"data").await;
        assert!(result.is_err());

        // Second write should succeed.
        let result = file.write_at(0, b"data").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simulated_storage_corrupt_bytes() {
        let storage = SimulatedStorage::new(42);
        let path = Path::new("/test/corrupt.wal");

        let file = storage.open(path).await.unwrap();
        file.write_at(0, b"hello").await.unwrap();

        // Corrupt byte at offset 2.
        storage.corrupt_bytes(path, 2, 1);

        let content = storage.get_raw_content(path).unwrap();
        assert_ne!(content[2], b'l'); // 'l' ^ 0xFF
        assert_eq!(content[0], b'h');
        assert_eq!(content[1], b'e');
    }

    #[tokio::test]
    async fn test_simulated_storage_truncate_file() {
        let storage = SimulatedStorage::new(42);
        let path = Path::new("/test/truncate.wal");

        let file = storage.open(path).await.unwrap();
        file.write_at(0, b"hello world").await.unwrap();

        // Truncate to 5 bytes.
        storage.truncate_file(path, 5);

        let content = storage.get_raw_content(path).unwrap();
        assert_eq!(content, b"hello");
    }

    #[tokio::test]
    async fn test_simulated_storage_list_files() {
        let storage = SimulatedStorage::new(42);
        let dir = Path::new("/wal");

        // Create some files.
        for i in 0..3 {
            let path = dir.join(format!("segment-{i:04}.wal"));
            let _ = storage.open(&path).await.unwrap();
        }

        // Create a non-.wal file.
        let _ = storage.open(&dir.join("other.txt")).await.unwrap();

        let files = storage.list_files(dir, "wal").await.unwrap();
        assert_eq!(files.len(), 3);
    }

    #[tokio::test]
    async fn test_simulated_storage_clone_shares_state() {
        let storage = SimulatedStorage::new(42);
        let path = Path::new("/test/shared.wal");

        let file = storage.open(path).await.unwrap();
        file.write_at(0, b"original").await.unwrap();

        // Clone the storage.
        let storage2 = storage.clone();

        // Both should see the same content.
        assert_eq!(
            storage.get_raw_content(path),
            storage2.get_raw_content(path)
        );

        // Modify through clone.
        storage2.corrupt_bytes(path, 0, 1);

        // Original should see the change.
        let content = storage.get_raw_content(path).unwrap();
        assert_ne!(content[0], b'o');
    }
}

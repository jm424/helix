//! Simulated Storage for Deterministic Simulation Testing (DST).
//!
//! This module provides an in-memory storage implementation with configurable
//! fault injection for testing crash recovery scenarios.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;

use super::{Storage, StorageFile};
use crate::error::{WalError, WalResult};

/// Configuration for fault injection in simulated storage.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability of a write being torn (partial write). Range: 0.0 - 1.0.
    pub torn_write_rate: f64,
    /// Probability of fsync failing. Range: 0.0 - 1.0.
    pub fsync_fail_rate: f64,
    /// Probability of read returning corrupted data. Range: 0.0 - 1.0.
    pub read_corruption_rate: f64,
    /// Probability of read operation failing completely. Range: 0.0 - 1.0.
    pub read_fail_rate: f64,
    /// Probability of write operation failing. Range: 0.0 - 1.0.
    pub write_fail_rate: f64,
    /// Probability of exists check failing. Range: 0.0 - 1.0.
    pub exists_fail_rate: f64,
    /// Probability of `list_files` failing. Range: 0.0 - 1.0.
    pub list_files_fail_rate: f64,
    /// Probability of file open failing. Range: 0.0 - 1.0.
    pub open_fail_rate: f64,
    /// Probability of file remove failing. Range: 0.0 - 1.0.
    pub remove_fail_rate: f64,
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
            read_fail_rate: 0.0,
            write_fail_rate: 0.0,
            exists_fail_rate: 0.0,
            list_files_fail_rate: 0.0,
            open_fail_rate: 0.0,
            remove_fail_rate: 0.0,
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
            read_fail_rate: 0.005,
            write_fail_rate: 0.005,
            exists_fail_rate: 0.002,
            list_files_fail_rate: 0.002,
            open_fail_rate: 0.002,
            remove_fail_rate: 0.002,
            ..Default::default()
        }
    }

    /// Sets the read fail rate.
    #[must_use]
    pub const fn with_read_fail_rate(mut self, rate: f64) -> Self {
        self.read_fail_rate = rate;
        self
    }

    /// Sets the write fail rate.
    #[must_use]
    pub const fn with_write_fail_rate(mut self, rate: f64) -> Self {
        self.write_fail_rate = rate;
        self
    }

    /// Sets the exists check fail rate.
    #[must_use]
    pub const fn with_exists_fail_rate(mut self, rate: f64) -> Self {
        self.exists_fail_rate = rate;
        self
    }

    /// Sets the `list_files` fail rate.
    #[must_use]
    pub const fn with_list_files_fail_rate(mut self, rate: f64) -> Self {
        self.list_files_fail_rate = rate;
        self
    }

    /// Sets the open fail rate.
    #[must_use]
    pub const fn with_open_fail_rate(mut self, rate: f64) -> Self {
        self.open_fail_rate = rate;
        self
    }

    /// Sets the remove fail rate.
    #[must_use]
    pub const fn with_remove_fail_rate(mut self, rate: f64) -> Self {
        self.remove_fail_rate = rate;
        self
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

/// Type of fault being injected.
#[derive(Debug, Clone, Copy)]
enum FaultType {
    TornWrite,
    FsyncFail,
    ReadCorruption,
    ReadFail,
    WriteFail,
    ExistsFail,
    ListFilesFail,
    OpenFail,
    RemoveFail,
}

/// Statistics tracking for injected faults.
#[derive(Debug, Default, Clone)]
pub struct FaultStats {
    /// Number of torn writes injected.
    pub torn_writes: u64,
    /// Number of fsync failures injected.
    pub fsync_failures: u64,
    /// Number of read corruptions injected.
    pub read_corruptions: u64,
    /// Number of read failures injected.
    pub read_failures: u64,
    /// Number of write failures injected.
    pub write_failures: u64,
    /// Number of exists check failures injected.
    pub exists_failures: u64,
    /// Number of `list_files` failures injected.
    pub list_files_failures: u64,
    /// Number of open failures injected.
    pub open_failures: u64,
    /// Number of remove failures injected.
    pub remove_failures: u64,
    /// Total storage operations attempted.
    pub total_ops: u64,
}

impl FaultStats {
    /// Returns the total number of faults injected.
    #[must_use]
    pub const fn total_faults(&self) -> u64 {
        self.torn_writes
            + self.fsync_failures
            + self.read_corruptions
            + self.read_failures
            + self.write_failures
            + self.exists_failures
            + self.list_files_failures
            + self.open_failures
            + self.remove_failures
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
    /// In-memory file contents (dirty - may not be synced).
    files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    /// Synced file contents (durable - survives crash).
    synced_files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    /// Fault injection configuration.
    fault_config: Arc<Mutex<FaultConfig>>,
    /// RNG seed for deterministic fault injection.
    seed: u64,
    /// Counter for deterministic fault injection on storage-level operations.
    op_counter: Arc<AtomicU64>,
    /// Statistics tracking for injected faults.
    fault_stats: Arc<Mutex<FaultStats>>,
}

#[allow(clippy::missing_panics_doc, clippy::significant_drop_tightening)]
impl SimulatedStorage {
    /// Creates a new simulated storage with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
            synced_files: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(FaultConfig::default())),
            seed,
            op_counter: Arc::new(AtomicU64::new(0)),
            fault_stats: Arc::new(Mutex::new(FaultStats::default())),
        }
    }

    /// Creates a new simulated storage with fault injection enabled.
    #[must_use]
    pub fn with_faults(seed: u64, config: FaultConfig) -> Self {
        Self {
            files: Arc::new(Mutex::new(HashMap::new())),
            synced_files: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(config)),
            seed,
            op_counter: Arc::new(AtomicU64::new(0)),
            fault_stats: Arc::new(Mutex::new(FaultStats::default())),
        }
    }

    /// Simulates a crash by reverting all files to their last synced state.
    /// Un-synced data is lost.
    pub fn simulate_crash(&self) {
        let synced = self.synced_files.lock().expect("synced_files lock poisoned");
        let mut files = self.files.lock().expect("files lock poisoned");
        files.clone_from(&synced);
    }

    /// Simple deterministic RNG for fault injection at storage level.
    fn should_inject_fault(&self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let counter = self.op_counter.fetch_add(1, Ordering::Relaxed);
        let hash = self
            .seed
            .wrapping_add(counter)
            .wrapping_mul(0x5851_f42d_4c95_7f2d);
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }

    /// Returns a reference to the fault configuration for modification.
    pub fn fault_config(&self) -> MutexGuard<'_, FaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Returns a copy of the current fault statistics.
    #[must_use]
    pub fn fault_stats(&self) -> FaultStats {
        self.fault_stats.lock().expect("fault stats lock poisoned").clone()
    }

    /// Increments a fault counter.
    fn record_fault(&self, fault_type: FaultType) {
        let mut stats = self.fault_stats.lock().expect("fault stats lock poisoned");
        match fault_type {
            FaultType::TornWrite => stats.torn_writes += 1,
            FaultType::FsyncFail => stats.fsync_failures += 1,
            FaultType::ReadCorruption => stats.read_corruptions += 1,
            FaultType::ReadFail => stats.read_failures += 1,
            FaultType::WriteFail => stats.write_failures += 1,
            FaultType::ExistsFail => stats.exists_failures += 1,
            FaultType::ListFilesFail => stats.list_files_failures += 1,
            FaultType::OpenFail => stats.open_failures += 1,
            FaultType::RemoveFail => stats.remove_failures += 1,
        }
    }

    /// Increments total operation counter.
    fn record_op(&self) {
        let mut stats = self.fault_stats.lock().expect("fault stats lock poisoned");
        stats.total_ops += 1;
    }

    /// Gets the raw file content for inspection in tests.
    #[must_use]
    pub fn get_raw_content(&self, path: &Path) -> Option<Vec<u8>> {
        let files = self.files.lock().expect("files lock poisoned");
        files.get(path).cloned()
    }

    /// Gets the synced (durable) file content for inspection in tests.
    #[must_use]
    pub fn get_synced_content(&self, path: &Path) -> Option<Vec<u8>> {
        let synced = self.synced_files.lock().expect("synced_files lock poisoned");
        synced.get(path).cloned()
    }

    /// Returns all synced file paths for inspection in tests.
    #[must_use]
    pub fn synced_file_paths(&self) -> Vec<PathBuf> {
        let synced = self.synced_files.lock().expect("synced_files lock poisoned");
        synced.keys().cloned().collect()
    }

    /// Returns a snapshot of all synced files with their contents for debugging.
    #[must_use]
    pub fn synced_snapshot(&self) -> HashMap<PathBuf, Vec<u8>> {
        let synced = self.synced_files.lock().expect("synced_files lock poisoned");
        synced.clone()
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

    // ========================================================================
    // Synchronous API for DST (Deterministic Simulation Testing)
    // ========================================================================
    //
    // These methods provide synchronous access to storage operations.
    // They are identical to the async trait methods but don't require a runtime.
    // Use these with DurablePartition via futures::executor::block_on for DST.

    /// Opens or creates a file at the given path (sync version).
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or created.
    pub fn open_sync(&self, path: &Path) -> WalResult<SimulatedFile> {
        self.record_op();
        // Check for open failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.open_fail_rate) {
                self.record_fault(FaultType::OpenFail);
                return Err(WalError::Io {
                    operation: "open",
                    message: "open failed (simulated)".to_string(),
                });
            }
        }

        let mut files = self.files.lock().expect("files lock poisoned");
        files.entry(path.to_path_buf()).or_default();
        drop(files);

        Ok(SimulatedFile {
            path: path.to_path_buf(),
            files: self.files.clone(),
            synced_files: self.synced_files.clone(),
            fault_config: self.fault_config.clone(),
            fault_stats: self.fault_stats.clone(),
            seed: self.seed,
            write_counter: AtomicU64::new(0),
            sync_counter: AtomicU64::new(0),
            read_counter: AtomicU64::new(0),
        })
    }

    /// Checks if a file exists at the given path (sync version).
    ///
    /// # Errors
    /// Returns an error if the exists check fails due to simulated fault.
    pub fn exists_sync(&self, path: &Path) -> WalResult<bool> {
        self.record_op();
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.exists_fail_rate) {
                self.record_fault(FaultType::ExistsFail);
                return Err(WalError::Io {
                    operation: "exists",
                    message: "exists check failed (simulated)".to_string(),
                });
            }
        }

        let files = self.files.lock().expect("files lock poisoned");
        Ok(files.contains_key(path))
    }

    /// Lists files in a directory matching an extension (sync version).
    ///
    /// # Errors
    /// Returns an error if the directory cannot be read.
    pub fn list_files_sync(&self, dir: &Path, extension: &str) -> WalResult<Vec<PathBuf>> {
        self.record_op();
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.list_files_fail_rate) {
                self.record_fault(FaultType::ListFilesFail);
                return Err(WalError::Io {
                    operation: "list_files",
                    message: "list_files failed (simulated)".to_string(),
                });
            }
        }

        let files = self.files.lock().expect("files lock poisoned");
        let dir_str = dir.to_string_lossy();
        let matching: Vec<PathBuf> = files
            .keys()
            .filter(|p| {
                let p_str = p.to_string_lossy();
                p_str.starts_with(dir_str.as_ref())
                    && (extension.is_empty() || p_str.ends_with(extension))
            })
            .cloned()
            .collect();
        Ok(matching)
    }

    /// Removes a file at the given path (sync version).
    ///
    /// # Errors
    /// Returns an error if the file cannot be removed.
    pub fn remove_sync(&self, path: &Path) -> WalResult<()> {
        self.record_op();
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.remove_fail_rate) {
                self.record_fault(FaultType::RemoveFail);
                return Err(WalError::Io {
                    operation: "remove",
                    message: "remove failed (simulated)".to_string(),
                });
            }
        }

        let mut files = self.files.lock().expect("files lock poisoned");
        files.remove(path);
        let mut synced = self.synced_files.lock().expect("synced_files lock poisoned");
        synced.remove(path);
        Ok(())
    }

    /// Creates a directory and all parent directories (sync version).
    /// No-op for in-memory storage.
    ///
    /// # Errors
    /// This method always succeeds for simulated storage.
    pub fn create_dir_all_sync(&self, _path: &Path) -> WalResult<()> {
        self.record_op();
        Ok(())
    }
}

impl Clone for SimulatedStorage {
    fn clone(&self) -> Self {
        Self {
            files: self.files.clone(),
            synced_files: self.synced_files.clone(),
            fault_config: self.fault_config.clone(),
            seed: self.seed,
            op_counter: self.op_counter.clone(),
            fault_stats: self.fault_stats.clone(),
        }
    }
}

#[async_trait]
#[allow(clippy::significant_drop_tightening)]
impl Storage for SimulatedStorage {
    async fn open(&self, path: &Path) -> WalResult<Box<dyn StorageFile>> {
        self.record_op();
        // Check for open failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.open_fail_rate) {
                self.record_fault(FaultType::OpenFail);
                return Err(WalError::Io {
                    operation: "open",
                    message: "open failed (simulated)".to_string(),
                });
            }
        }

        let mut files = self.files.lock().expect("files lock poisoned");
        files.entry(path.to_path_buf()).or_default();
        drop(files);

        Ok(Box::new(SimulatedFile {
            path: path.to_path_buf(),
            files: self.files.clone(),
            synced_files: self.synced_files.clone(),
            fault_config: self.fault_config.clone(),
            fault_stats: self.fault_stats.clone(),
            seed: self.seed,
            write_counter: AtomicU64::new(0),
            sync_counter: AtomicU64::new(0),
            read_counter: AtomicU64::new(0),
        }))
    }

    async fn exists(&self, path: &Path) -> WalResult<bool> {
        self.record_op();
        // Check for exists failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.exists_fail_rate) {
                self.record_fault(FaultType::ExistsFail);
                return Err(WalError::Io {
                    operation: "exists",
                    message: "exists check failed (simulated)".to_string(),
                });
            }
        }

        let files = self.files.lock().expect("files lock poisoned");
        Ok(files.contains_key(path))
    }

    async fn list_files(&self, dir: &Path, extension: &str) -> WalResult<Vec<PathBuf>> {
        self.record_op();
        // Check for list_files failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.list_files_fail_rate) {
                self.record_fault(FaultType::ListFilesFail);
                return Err(WalError::Io {
                    operation: "list_files",
                    message: "list_files failed (simulated)".to_string(),
                });
            }
        }

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
        self.record_op();
        // Check for remove failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            if self.should_inject_fault(config.remove_fail_rate) {
                self.record_fault(FaultType::RemoveFail);
                return Err(WalError::Io {
                    operation: "remove",
                    message: "remove failed (simulated)".to_string(),
                });
            }
        }

        // Remove from both dirty and synced state (removal is atomic).
        let mut files = self.files.lock().expect("files lock poisoned");
        files.remove(path);
        drop(files);

        let mut synced = self.synced_files.lock().expect("synced_files lock poisoned");
        synced.remove(path);

        Ok(())
    }

    async fn create_dir_all(&self, _path: &Path) -> WalResult<()> {
        // Directories are implicit in simulated storage.
        Ok(())
    }
}

/// A simulated file handle with fault injection.
///
/// Uses separate counters for different fault types to ensure independence.
/// This is critical for DST: enabling torn writes shouldn't affect fsync failure rate.
#[allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss
)]
/// A handle to an open simulated file.
///
/// Provides both async (via `StorageFile` trait) and sync methods for DST.
pub struct SimulatedFile {
    path: PathBuf,
    files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    synced_files: Arc<Mutex<HashMap<PathBuf, Vec<u8>>>>,
    fault_config: Arc<Mutex<FaultConfig>>,
    fault_stats: Arc<Mutex<FaultStats>>,
    seed: u64,
    /// Counter for write/torn write fault injection.
    write_counter: AtomicU64,
    /// Separate counter for fsync fault injection.
    sync_counter: AtomicU64,
    /// Separate counter for read fault injection.
    read_counter: AtomicU64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::missing_panics_doc,
    clippy::significant_drop_tightening
)]
impl SimulatedFile {
    /// Simple deterministic RNG based on seed and counter.
    fn should_inject_fault(&self, rate: f64, counter: u64) -> bool {
        Self::should_inject_fault_with_salt(self.seed, rate, counter, 0)
    }

    /// Deterministic RNG with additional salt to decorrelate fault types.
    fn should_inject_fault_with_salt(seed: u64, rate: f64, counter: u64, salt: u64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        // Use different multipliers for different salts to decorrelate.
        let multiplier = match salt {
            0 => 0x5851_f42d_4c95_7f2d, // writes
            1 => 0x9e37_79b9_7f4a_7c15, // syncs
            2 => 0xc6a4_a793_5bd1_e995, // reads
            _ => 0x5851_f42d_4c95_7f2d_u64.wrapping_add(salt),
        };
        let hash = seed.wrapping_add(counter).wrapping_mul(multiplier);
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }

    /// Records a fault injection in the statistics.
    fn record_fault(&self, fault_type: FaultType) {
        let mut stats = self.fault_stats.lock().expect("fault stats lock poisoned");
        match fault_type {
            FaultType::TornWrite => stats.torn_writes += 1,
            FaultType::FsyncFail => stats.fsync_failures += 1,
            FaultType::ReadCorruption => stats.read_corruptions += 1,
            FaultType::ReadFail => stats.read_failures += 1,
            FaultType::WriteFail => stats.write_failures += 1,
            FaultType::ExistsFail => stats.exists_failures += 1,
            FaultType::ListFilesFail => stats.list_files_failures += 1,
            FaultType::OpenFail => stats.open_failures += 1,
            FaultType::RemoveFail => stats.remove_failures += 1,
        }
    }

    // ========================================================================
    // Synchronous API for DST (Deterministic Simulation Testing)
    // ========================================================================

    /// Writes data at the specified offset (sync version).
    ///
    /// # Errors
    /// Returns an error if the write fails or is torn.
    #[allow(clippy::cast_possible_truncation)]
    pub fn write_at_sync(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        let counter = self.write_counter.fetch_add(1, Ordering::Relaxed);

        let config = self.fault_config.lock().expect("fault config lock poisoned");

        // Check for write failure.
        if self.should_inject_fault(config.write_fail_rate, counter) {
            self.record_fault(FaultType::WriteFail);
            return Err(WalError::Io {
                operation: "write",
                message: "write failed (simulated)".to_string(),
            });
        }

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
            let hash = self
                .seed
                .wrapping_add(counter)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15);
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
        if let Some(torn_offset) = torn_at {
            let actual_write_len = torn_offset.min(data.len());
            content[offset as usize..offset as usize + actual_write_len]
                .copy_from_slice(&data[..actual_write_len]);
            content.truncate(offset as usize + actual_write_len);

            self.record_fault(FaultType::TornWrite);
            return Err(WalError::Io {
                operation: "write",
                message: "torn write (simulated crash during write)".to_string(),
            });
        }

        // Normal write.
        content[offset as usize..offset as usize + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Reads data from the specified offset (sync version).
    ///
    /// # Errors
    /// Returns an error if the read fails.
    #[allow(clippy::cast_possible_truncation)]
    pub fn read_at_sync(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        // Check for read failure.
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            let counter = self.read_counter.fetch_add(1, Ordering::Relaxed);
            if Self::should_inject_fault_with_salt(self.seed, config.read_fail_rate, counter, 2) {
                self.record_fault(FaultType::ReadFail);
                return Err(WalError::Io {
                    operation: "read",
                    message: "read failed (simulated)".to_string(),
                });
            }
        }

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
        let counter = self.read_counter.fetch_add(1, Ordering::Relaxed);
        if Self::should_inject_fault_with_salt(self.seed, config.read_corruption_rate, counter, 2)
            && !data.is_empty()
        {
            let hash = self
                .seed
                .wrapping_add(counter)
                .wrapping_mul(0xc6a4_a793_5bd1_e995);
            let corrupt_idx = hash as usize % data.len();
            data[corrupt_idx] ^= 0xFF;
            self.record_fault(FaultType::ReadCorruption);
        }

        Ok(Bytes::from(data))
    }

    /// Reads the entire file contents (sync version).
    ///
    /// # Errors
    /// Returns an error if the read fails.
    pub fn read_all_sync(&self) -> WalResult<Bytes> {
        let files = self.files.lock().expect("files lock poisoned");
        let content = files.get(&self.path).ok_or_else(|| WalError::Io {
            operation: "read",
            message: "file not found".to_string(),
        })?;
        Ok(Bytes::from(content.clone()))
    }

    /// Syncs all buffered data to disk (sync version).
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub fn sync_data(&self) -> WalResult<()> {
        let config = self.fault_config.lock().expect("fault config lock poisoned");
        let counter = self.sync_counter.fetch_add(1, Ordering::Relaxed);
        let should_fail = config.force_fsync_fail
            || Self::should_inject_fault_with_salt(self.seed, config.fsync_fail_rate, counter, 1);

        if should_fail {
            drop(config);
            let mut config = self.fault_config.lock().expect("fault config lock poisoned");
            config.force_fsync_fail = false;
            self.record_fault(FaultType::FsyncFail);
            return Err(WalError::Io {
                operation: "sync",
                message: "fsync failed (simulated)".to_string(),
            });
        }
        drop(config);

        // Copy current file content to synced state (makes it durable).
        let files = self.files.lock().expect("files lock poisoned");
        if let Some(content) = files.get(&self.path) {
            let mut synced = self.synced_files.lock().expect("synced_files lock poisoned");
            synced.insert(self.path.clone(), content.clone());
        }

        Ok(())
    }

    /// Returns the current file size in bytes (sync version).
    ///
    /// # Errors
    /// Returns an error if the file is not found.
    pub fn size_sync(&self) -> WalResult<u64> {
        let files = self.files.lock().expect("files lock poisoned");
        let content = files.get(&self.path).ok_or_else(|| WalError::Io {
            operation: "size",
            message: "file not found".to_string(),
        })?;
        #[allow(clippy::cast_possible_truncation)]
        Ok(content.len() as u64)
    }

    /// Truncates the file to the specified length (sync version).
    ///
    /// # Errors
    /// Returns an error if the file is not found.
    pub fn truncate_sync(&self, len: u64) -> WalResult<()> {
        let mut files = self.files.lock().expect("files lock poisoned");
        let content = files.get_mut(&self.path).ok_or_else(|| WalError::Io {
            operation: "truncate",
            message: "file not found".to_string(),
        })?;
        // Truncation is bounded by simulated storage limits.
        #[allow(clippy::cast_possible_truncation)]
        content.truncate(len as usize);
        Ok(())
    }
}

#[async_trait]
#[allow(clippy::significant_drop_tightening, clippy::cast_possible_truncation)]
impl StorageFile for SimulatedFile {
    async fn write_at(&self, offset: u64, data: &[u8]) -> WalResult<()> {
        let counter = self.write_counter.fetch_add(1, Ordering::Relaxed);

        let config = self.fault_config.lock().expect("fault config lock poisoned");

        // Check for write failure.
        if self.should_inject_fault(config.write_fail_rate, counter) {
            self.record_fault(FaultType::WriteFail);
            return Err(WalError::Io {
                operation: "write",
                message: "write failed (simulated)".to_string(),
            });
        }

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
            let hash = self
                .seed
                .wrapping_add(counter)
                .wrapping_mul(0x9e37_79b9_7f4a_7c15);
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
        if let Some(torn_offset) = torn_at {
            // Torn write: simulate crash during write.
            // Write partial data, then return error (process crashed).
            let actual_write_len = torn_offset.min(data.len());
            content[offset as usize..offset as usize + actual_write_len]
                .copy_from_slice(&data[..actual_write_len]);
            content.truncate(offset as usize + actual_write_len);

            self.record_fault(FaultType::TornWrite);
            // Return error - the calling process "crashed" during this write.
            // This is more realistic than silently corrupting and continuing.
            return Err(WalError::Io {
                operation: "write",
                message: "torn write (simulated crash during write)".to_string(),
            });
        }

        // Normal write.
        content[offset as usize..offset as usize + data.len()].copy_from_slice(data);

        Ok(())
    }

    async fn read_at(&self, offset: u64, len: usize) -> WalResult<Bytes> {
        // Check for read failure first (use separate read_counter AND salt=2).
        {
            let config = self.fault_config.lock().expect("fault config lock poisoned");
            let counter = self.read_counter.fetch_add(1, Ordering::Relaxed);
            if Self::should_inject_fault_with_salt(self.seed, config.read_fail_rate, counter, 2) {
                self.record_fault(FaultType::ReadFail);
                return Err(WalError::Io {
                    operation: "read",
                    message: "read failed (simulated)".to_string(),
                });
            }
        }

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

        // Check for read corruption (use separate read_counter AND salt=2).
        let config = self.fault_config.lock().expect("fault config lock poisoned");
        let counter = self.read_counter.fetch_add(1, Ordering::Relaxed);
        if Self::should_inject_fault_with_salt(self.seed, config.read_corruption_rate, counter, 2)
            && !data.is_empty()
        {
            // Corrupt a random byte.
            let hash = self
                .seed
                .wrapping_add(counter)
                .wrapping_mul(0xc6a4_a793_5bd1_e995);
            let corrupt_idx = hash as usize % data.len();
            data[corrupt_idx] ^= 0xFF;
            self.record_fault(FaultType::ReadCorruption);
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
        // Use separate sync_counter AND salt=1 for independence from write faults.
        let counter = self.sync_counter.fetch_add(1, Ordering::Relaxed);
        let should_fail = config.force_fsync_fail
            || Self::should_inject_fault_with_salt(self.seed, config.fsync_fail_rate, counter, 1);

        if should_fail {
            drop(config);
            let mut config = self.fault_config.lock().expect("fault config lock poisoned");
            config.force_fsync_fail = false;
            self.record_fault(FaultType::FsyncFail);
            return Err(WalError::Io {
                operation: "sync",
                message: "fsync failed (simulated)".to_string(),
            });
        }
        drop(config);

        // Copy current file content to synced state (makes it durable).
        let files = self.files.lock().expect("files lock poisoned");
        if let Some(content) = files.get(&self.path) {
            let mut synced = self.synced_files.lock().expect("synced_files lock poisoned");
            synced.insert(self.path.clone(), content.clone());
        }

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
#[allow(clippy::items_after_statements)] // Const near usage is clearer.
mod tests {
    use super::*;

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
        // The write should fail (simulating crash during write).
        let data = b"0123456789";
        let result = file.write_at(0, data).await;
        assert!(result.is_err());

        // File should only have 5 bytes (partial write before "crash").
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

    #[tokio::test]
    async fn test_simulated_storage_fsync_rate_failures() {
        // Test that rate-based fsync failures work correctly.
        // With 100% fsync_fail_rate, all syncs should fail.
        let config = FaultConfig::none().with_fsync_fail_rate(1.0);
        let storage = SimulatedStorage::with_faults(42, config);
        let path = Path::new("/test/rate.wal");

        let file = storage.open(path).await.unwrap();
        file.write_at(0, b"data").await.unwrap();

        // With 100% rate, sync should always fail.
        let result = file.sync().await;
        assert!(result.is_err(), "100% fsync_fail_rate should cause failure");

        // Try again - should still fail.
        let result = file.sync().await;
        assert!(result.is_err(), "100% fsync_fail_rate should consistently fail");
    }

    #[tokio::test]
    async fn test_simulated_storage_fsync_rate_across_seeds() {
        // Verify that fsync failures happen at expected rate across seeds.
        let mut failures = 0;
        const SEED_COUNT: u64 = 1000;

        for seed in 0..SEED_COUNT {
            let config = FaultConfig::none().with_fsync_fail_rate(0.10);
            let storage = SimulatedStorage::with_faults(seed, config);
            let path = Path::new("/test/rate.wal");

            let file = storage.open(path).await.unwrap();
            file.write_at(0, b"data").await.unwrap();

            if file.sync().await.is_err() {
                failures += 1;
            }
        }

        // With 10% rate and 1000 seeds, expect ~100 failures (±30 for variance).
        assert!(
            failures > 50 && failures < 150,
            "Expected ~100 fsync failures, got {failures}"
        );
    }
}

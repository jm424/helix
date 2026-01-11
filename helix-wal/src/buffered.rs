//! Buffered WAL with periodic flushing.
//!
//! Wraps the core [`Wal`] with write buffering for high-throughput scenarios.
//! Trades durability guarantees for performance by batching writes.
//!
//! # Durability Modes
//!
//! - `flush_interval = 0` → Flush every write (max durability, lower throughput)
//! - `flush_interval = 10ms` → High throughput (≤10ms data at risk on crash)
//! - `flush_interval = 100ms` → Maximum throughput for non-critical data
//!
//! # `io_uring` Compatibility
//!
//! This design is optimized for future `io_uring` integration:
//! - Batches multiple writes into single submissions
//! - Can chain write operations with fsync
//! - Minimizes syscall overhead

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::entry::Entry;
use crate::error::{WalError, WalResult};
use crate::storage::Storage;
use crate::wal::{Wal, WalConfig};

/// Configuration for buffered WAL.
#[derive(Debug, Clone)]
pub struct BufferedWalConfig {
    /// Base WAL configuration.
    pub wal_config: WalConfig,
    /// Flush interval. Set to `Duration::ZERO` for synchronous writes.
    pub flush_interval: Duration,
    /// Maximum entries to buffer before forcing a flush.
    pub max_buffer_entries: usize,
    /// Maximum bytes to buffer before forcing a flush.
    pub max_buffer_bytes: usize,
}

impl BufferedWalConfig {
    /// Creates a new buffered WAL configuration.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(wal_config: WalConfig) -> Self {
        Self {
            wal_config,
            flush_interval: Duration::from_millis(10), // Default: 10ms batching
            max_buffer_entries: 10_000,
            max_buffer_bytes: 64 * 1024 * 1024, // 64 MB
        }
    }

    /// Sets the flush interval.
    #[must_use]
    pub const fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Sets the maximum buffer entries.
    #[must_use]
    pub const fn with_max_buffer_entries(mut self, max: usize) -> Self {
        self.max_buffer_entries = max;
        self
    }

    /// Sets the maximum buffer bytes.
    #[must_use]
    pub const fn with_max_buffer_bytes(mut self, max: usize) -> Self {
        self.max_buffer_bytes = max;
        self
    }

    /// Returns true if synchronous mode is enabled (`flush_interval` = 0).
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn is_sync_mode(&self) -> bool {
        self.flush_interval.is_zero()
    }
}

/// Internal state shared between `BufferedWal` and flush task.
struct BufferedWalInner<S: Storage> {
    /// Underlying WAL.
    wal: Wal<S>,
    /// Pending entries waiting to be flushed, sorted by index.
    buffer: VecDeque<Entry>,
    /// Total bytes in buffer.
    buffer_bytes: usize,
    /// Last flush time.
    last_flush: Instant,
    /// Configuration.
    config: BufferedWalConfig,
    /// Next expected index for sequential writes.
    next_index: u64,
}

/// Buffered WAL with periodic flushing.
///
/// Provides `Arc`-based concurrent access with internal synchronization.
/// Multiple tasks can append concurrently; writes are batched and flushed
/// periodically for high throughput.
#[allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::option_if_let_else
)]
pub struct BufferedWal<S: Storage + Send + Sync + 'static> {
    /// Shared state protected by mutex.
    inner: Arc<Mutex<BufferedWalInner<S>>>,
    /// Notification for flush task.
    flush_notify: Arc<Notify>,
    /// Whether the flush task is running.
    flush_task_running: Arc<std::sync::atomic::AtomicBool>,
}

#[allow(
    clippy::significant_drop_tightening,
    clippy::cast_possible_truncation,
    clippy::map_unwrap_or
)]
impl<S: Storage + Send + Sync + 'static> BufferedWal<S> {
    /// Opens or creates a buffered WAL.
    ///
    /// Starts a background flush task if `flush_interval > 0`.
    ///
    /// # Errors
    /// Returns an error if the underlying WAL cannot be opened.
    pub async fn open(storage: S, config: BufferedWalConfig) -> WalResult<Self> {
        let wal = Wal::open(storage, config.wal_config.clone()).await?;

        // Next index is one past the last written entry.
        let next_index = wal.last_index().map_or(1, |idx| idx + 1);

        let inner = Arc::new(Mutex::new(BufferedWalInner {
            wal,
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            last_flush: Instant::now(),
            config: config.clone(),
            next_index,
        }));

        let flush_notify = Arc::new(Notify::new());
        let flush_task_running =
            Arc::new(std::sync::atomic::AtomicBool::new(false));

        let buffered_wal = Self {
            inner,
            flush_notify,
            flush_task_running,
        };

        // Start background flush task if not in sync mode.
        if !config.is_sync_mode() {
            buffered_wal.start_flush_task();
        }

        info!(
            flush_interval_ms = config.flush_interval.as_millis(),
            max_buffer_entries = config.max_buffer_entries,
            "Opened buffered WAL"
        );

        Ok(buffered_wal)
    }

    /// Starts the background flush task.
    fn start_flush_task(&self) {
        use std::sync::atomic::Ordering;

        if self
            .flush_task_running
            .swap(true, Ordering::SeqCst)
        {
            return; // Already running.
        }

        let inner = self.inner.clone();
        let notify = self.flush_notify.clone();
        let running = self.flush_task_running.clone();

        tokio::spawn(async move {
            loop {
                let flush_interval = {
                    let guard = inner.lock().await;
                    guard.config.flush_interval
                };

                // Wait for flush interval or notification.
                tokio::select! {
                    () = tokio::time::sleep(flush_interval) => {}
                    () = notify.notified() => {}
                }

                // Flush pending writes.
                let mut guard = inner.lock().await;
                if let Err(e) = Self::flush_buffer(&mut guard).await {
                    warn!(error = %e, "Flush failed");
                }

                // Check if we should stop.
                if !running.load(Ordering::SeqCst) {
                    break;
                }
            }
        });
    }

    /// Appends an entry to the WAL.
    ///
    /// In buffered mode, the entry is added to the buffer and will be
    /// flushed asynchronously. The entry is immediately visible to reads.
    ///
    /// In sync mode (`flush_interval = 0`), the entry is written and
    /// synced immediately.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn append(&self, entry: Entry) -> WalResult<u64> {
        let mut guard = self.inner.lock().await;
        let index = entry.index();
        let entry_bytes = entry.total_size() as usize;

        // Add to buffer (even in sync mode, to handle concurrent access).
        guard.buffer.push_back(entry);
        guard.buffer_bytes += entry_bytes;

        // In sync mode, flush immediately after each append.
        if guard.config.is_sync_mode() {
            Self::flush_buffer(&mut guard).await?;
            return Ok(index);
        }

        // Check if we need to flush.
        let should_flush = guard.buffer.len() >= guard.config.max_buffer_entries
            || guard.buffer_bytes >= guard.config.max_buffer_bytes;

        if should_flush {
            Self::flush_buffer(&mut guard).await?;
        }

        Ok(index)
    }

    /// Appends multiple entries in a batch.
    ///
    /// More efficient than multiple individual appends as it acquires
    /// the lock once for all entries.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    pub async fn append_batch(&self, entries: Vec<Entry>) -> WalResult<u64> {
        if entries.is_empty() {
            return Err(WalError::InvalidEntry {
                reason: "empty batch",
            });
        }

        let mut guard = self.inner.lock().await;
        let first_index = entries.first().map(Entry::index).unwrap_or(0);

        // Add all to buffer (even in sync mode, to handle concurrent access).
        let total_bytes: usize = entries.iter().map(|e| e.total_size() as usize).sum();
        guard.buffer.extend(entries);
        guard.buffer_bytes += total_bytes;

        // In sync mode, flush immediately.
        if guard.config.is_sync_mode() {
            Self::flush_buffer(&mut guard).await?;
            return Ok(first_index);
        }

        // Check if we need to flush.
        let should_flush = guard.buffer.len() >= guard.config.max_buffer_entries
            || guard.buffer_bytes >= guard.config.max_buffer_bytes;

        if should_flush {
            Self::flush_buffer(&mut guard).await?;
        }

        Ok(first_index)
    }

    /// Flushes all buffered entries to disk.
    ///
    /// # Errors
    /// Returns an error if the flush fails.
    pub async fn flush(&self) -> WalResult<()> {
        let mut guard = self.inner.lock().await;
        Self::flush_buffer(&mut guard).await
    }

    /// Internal flush implementation.
    ///
    /// Only flushes contiguous entries starting from `next_index`.
    /// Out-of-order entries remain in the buffer until their predecessors arrive.
    async fn flush_buffer(inner: &mut BufferedWalInner<S>) -> WalResult<()> {
        if inner.buffer.is_empty() {
            return Ok(());
        }

        // Sort entries by index first.
        let mut entries: Vec<Entry> = inner.buffer.drain(..).collect();
        entries.sort_by_key(Entry::index);

        // Find contiguous sequence starting from next_index.
        let mut to_write = Vec::new();
        let mut remaining = Vec::new();
        let mut expected = inner.next_index;

        for entry in entries {
            if entry.index() == expected {
                to_write.push(entry);
                expected += 1;
            } else if entry.index() > expected {
                // Gap - keep in buffer for later.
                remaining.push(entry);
            }
            // Skip entries with index < expected (duplicates or already written).
        }

        if to_write.is_empty() {
            // No contiguous entries to write, put everything back.
            inner.buffer.extend(remaining);
            return Ok(());
        }

        let count = to_write.len();
        let bytes: usize = to_write.iter().map(|e| e.total_size() as usize).sum();

        // Write contiguous entries.
        // Note: This is where io_uring could batch all writes + fsync.
        for entry in to_write {
            inner.wal.append(entry).await?;
        }

        // Sync to disk.
        inner.wal.sync().await?;

        // Update state.
        inner.next_index = expected;
        inner.buffer.extend(remaining);
        inner.buffer_bytes = inner
            .buffer
            .iter()
            .map(|e| e.total_size() as usize)
            .sum();
        inner.last_flush = Instant::now();

        debug!(entries = count, bytes, next_index = expected, "Flushed buffer");

        Ok(())
    }

    /// Reads an entry by index.
    ///
    /// Reads from both the buffer (unflushed) and the underlying WAL.
    ///
    /// # Errors
    /// Returns an error if the index is out of bounds.
    pub async fn read(&self, index: u64) -> WalResult<Entry> {
        let guard = self.inner.lock().await;

        // Check buffer first (entries not yet flushed).
        for entry in &guard.buffer {
            if entry.index() == index {
                return Ok(entry.clone());
            }
        }

        // Fall back to WAL.
        guard.wal.read(index).cloned()
    }

    /// Returns the first index in the WAL.
    pub async fn first_index(&self) -> u64 {
        let guard = self.inner.lock().await;
        guard.wal.first_index()
    }

    /// Returns the last index in the WAL (including buffered entries).
    pub async fn last_index(&self) -> Option<u64> {
        let guard = self.inner.lock().await;

        // Check buffer for latest entry.
        if let Some(entry) = guard.buffer.back() {
            return Some(entry.index());
        }

        guard.wal.last_index()
    }

    /// Returns true if the WAL is empty.
    pub async fn is_empty(&self) -> bool {
        let guard = self.inner.lock().await;
        guard.buffer.is_empty() && guard.wal.is_empty()
    }

    /// Returns the number of entries currently buffered (not yet flushed).
    pub async fn buffered_count(&self) -> usize {
        let guard = self.inner.lock().await;
        guard.buffer.len()
    }

    /// Stops the background flush task and flushes remaining entries.
    ///
    /// # Errors
    /// Returns an error if the final flush fails.
    pub async fn close(&self) -> WalResult<()> {
        use std::sync::atomic::Ordering;

        // Stop flush task.
        self.flush_task_running.store(false, Ordering::SeqCst);
        self.flush_notify.notify_one();

        // Final flush.
        self.flush().await
    }
}

impl<S: Storage + Send + Sync + 'static> Clone for BufferedWal<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            flush_notify: self.flush_notify.clone(),
            flush_task_running: self.flush_task_running.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TokioStorage;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_buffered_wal_sync_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::ZERO); // Sync mode

        let wal = BufferedWal::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Append entries.
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            let idx = wal.append(entry).await.unwrap();
            assert_eq!(idx, i);
        }

        // Should be no buffered entries in sync mode.
        assert_eq!(wal.buffered_count().await, 0);

        // Read entries.
        for i in 1..=5 {
            let entry = wal.read(i).await.unwrap();
            assert_eq!(entry.index(), i);
        }
    }

    #[tokio::test]
    async fn test_buffered_wal_buffered_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_secs(60)) // Long interval
            .with_max_buffer_entries(100);

        let wal = BufferedWal::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Append entries.
        for i in 1..=10 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            wal.append(entry).await.unwrap();
        }

        // Should be buffered.
        assert_eq!(wal.buffered_count().await, 10);

        // Can still read from buffer.
        for i in 1..=10 {
            let entry = wal.read(i).await.unwrap();
            assert_eq!(entry.index(), i);
        }

        // Manual flush.
        wal.flush().await.unwrap();
        assert_eq!(wal.buffered_count().await, 0);

        // Can still read after flush.
        for i in 1..=10 {
            let entry = wal.read(i).await.unwrap();
            assert_eq!(entry.index(), i);
        }
    }

    #[tokio::test]
    async fn test_buffered_wal_auto_flush_on_size() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_secs(60))
            .with_max_buffer_entries(5); // Small buffer

        let wal = BufferedWal::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Append 4 entries - should stay buffered.
        for i in 1..=4 {
            let entry = Entry::new(1, i, Bytes::from("data")).unwrap();
            wal.append(entry).await.unwrap();
        }
        assert_eq!(wal.buffered_count().await, 4);

        // Append 5th entry - should trigger flush.
        let entry = Entry::new(1, 5, Bytes::from("data")).unwrap();
        wal.append(entry).await.unwrap();
        assert_eq!(wal.buffered_count().await, 0);
    }

    #[tokio::test]
    async fn test_buffered_wal_batch_append() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_secs(60));

        let wal = BufferedWal::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Batch append.
        let entries: Vec<Entry> = (1..=100)
            .map(|i| Entry::new(1, i, Bytes::from("data")).unwrap())
            .collect();

        let first_idx = wal.append_batch(entries).await.unwrap();
        assert_eq!(first_idx, 1);
        assert_eq!(wal.buffered_count().await, 100);

        // Flush and verify.
        wal.flush().await.unwrap();
        assert_eq!(wal.last_index().await, Some(100));
    }

    /// NOTE: Uses multi-threaded runtime to expose race conditions that would
    /// be hidden by single-threaded Tokio's serialization of spawned tasks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_buffered_wal_concurrent_access() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_millis(10));

        let wal = BufferedWal::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Spawn multiple tasks.
        let mut handles = vec![];
        for task_id in 0u64..4 {
            let wal_clone = wal.clone();
            handles.push(tokio::spawn(async move {
                for i in 0u64..25 {
                    let index = task_id * 25 + i + 1;
                    let entry = Entry::new(1, index, Bytes::from("data")).unwrap();
                    wal_clone.append(entry).await.unwrap();
                }
            }));
        }

        // Wait for all tasks.
        for handle in handles {
            handle.await.unwrap();
        }

        // Flush and verify count.
        wal.flush().await.unwrap();
        assert_eq!(wal.last_index().await, Some(100));
    }

    /// NOTE: Uses multi-threaded runtime to expose race conditions that would
    /// be hidden by single-threaded Tokio's serialization of spawned tasks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_buffered_wal_concurrent_shared_counter_sync() {
        // Mimics the benchmark pattern: shared atomic counter for indices.
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::ZERO); // Sync mode

        let wal = Arc::new(
            BufferedWal::open(TokioStorage::new(), config)
                .await
                .unwrap(),
        );
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(1));

        // Spawn tasks with shared counter (64 tasks like benchmark).
        let mut handles = vec![];
        for _ in 0..64 {
            let wal_clone = wal.clone();
            let counter_clone = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..8 {
                    let idx =
                        counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = Entry::new(1, idx, Bytes::from("data")).unwrap();
                    wal_clone.append(entry).await.unwrap();
                }
            }));
        }

        // Wait for all tasks.
        for handle in handles {
            handle.await.unwrap();
        }

        // Flush and verify.
        wal.flush().await.unwrap();
        assert_eq!(wal.last_index().await, Some(512)); // 64 tasks * 8 entries
    }

    /// NOTE: Uses multi-threaded runtime to expose race conditions that would
    /// be hidden by single-threaded Tokio's serialization of spawned tasks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_buffered_wal_concurrent_shared_counter_buffered() {
        // Mimics the benchmark pattern with buffered mode.
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_config = WalConfig::new(temp_dir.path());
        let config = BufferedWalConfig::new(wal_config)
            .with_flush_interval(Duration::from_millis(10));

        let wal = Arc::new(
            BufferedWal::open(TokioStorage::new(), config)
                .await
                .unwrap(),
        );
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(1));

        // Spawn tasks with shared counter.
        let mut handles = vec![];
        for _ in 0..64 {
            let wal_clone = wal.clone();
            let counter_clone = counter.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..8 {
                    let idx =
                        counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let entry = Entry::new(1, idx, Bytes::from("data")).unwrap();
                    wal_clone.append(entry).await.unwrap();
                }
            }));
        }

        // Wait for all tasks.
        for handle in handles {
            handle.await.unwrap();
        }

        // Flush and verify.
        wal.flush().await.unwrap();
        assert_eq!(wal.last_index().await, Some(512));
    }
}

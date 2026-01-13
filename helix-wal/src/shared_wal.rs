//! Shared WAL for multiple partitions.
//!
//! A shared WAL allows multiple partitions to share a single WAL, amortizing
//! fsync cost across partitions. Instead of N fsyncs for N partitions, a single
//! fsync makes all buffered entries durable.
//!
//! # Design
//!
//! - Entries are tagged with partition ID via [`SharedEntry`]
//! - Partition-local indices are used (not WAL-global)
//! - Recovery scans sequentially and groups entries by partition
//! - No in-memory index; sequential access via `entries()` iterator
//!
//! # Example
//!
//! ```ignore
//! use helix_wal::{SharedWal, SharedWalConfig, TokioStorage};
//! use helix_core::PartitionId;
//! use bytes::Bytes;
//!
//! let config = SharedWalConfig::new("/tmp/shared-wal");
//! let mut wal = SharedWal::open(TokioStorage::new(), config).await?;
//!
//! // Append entries from different partitions.
//! let p1 = PartitionId::new(1);
//! let p2 = PartitionId::new(2);
//!
//! wal.append(p1, 1, 1, Bytes::from("p1-data")).await?;
//! wal.append(p2, 1, 1, Bytes::from("p2-data")).await?;
//!
//! // Single sync makes both entries durable.
//! wal.sync().await?;
//!
//! // Recovery groups entries by partition.
//! let by_partition = wal.recover()?;
//! ```

use std::collections::HashMap;
use std::path::PathBuf;

use bytes::Bytes;
use helix_core::PartitionId;

use crate::error::WalResult;
use crate::shared_entry::SharedEntry;
use crate::storage::Storage;
use crate::wal::{Wal, WalConfig};

// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

/// Configuration for a shared WAL.
#[derive(Debug, Clone)]
pub struct SharedWalConfig {
    /// Base WAL configuration (directory, segment size, etc.).
    wal_config: WalConfig,
}

impl SharedWalConfig {
    /// Creates a new shared WAL configuration.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            wal_config: WalConfig::new(dir),
        }
    }

    /// Returns a reference to the underlying WAL config.
    #[must_use]
    pub const fn wal_config(&self) -> &WalConfig {
        &self.wal_config
    }

    /// Sets the segment configuration.
    #[must_use]
    pub fn with_segment_config(mut self, config: crate::SegmentConfig) -> Self {
        self.wal_config = self.wal_config.with_segment_config(config);
        self
    }
}

// ----------------------------------------------------------------------------
// SharedWal
// ----------------------------------------------------------------------------

/// A shared WAL for multiple partitions.
///
/// Multiple partitions can append entries to the same WAL, amortizing fsync
/// cost. A single `sync()` call makes all pending entries durable.
///
/// # Recovery
///
/// On startup, call [`recover`](Self::recover) to get entries grouped by partition.
/// Each partition then rebuilds its in-memory Raft log from its entries.
///
/// # Thread Safety
///
/// `SharedWal` requires `&mut self` for append operations. For concurrent access
/// from multiple partitions, wrap in appropriate synchronization (e.g., `Mutex`).
/// A future iteration may add partition handles with internal synchronization.
pub struct SharedWal<S: Storage> {
    /// Underlying WAL with `SharedEntry` format.
    wal: Wal<S, SharedEntry>,
    /// Per-partition tracking for sequential index assertion.
    partition_state: HashMap<PartitionId, PartitionState>,
    /// Per-partition durable index (last synced index per partition).
    partition_durable: HashMap<PartitionId, u64>,
    /// Per-partition truncation point (entries with index > this are logically deleted).
    /// None means no truncation has occurred for that partition.
    partition_truncated_after: HashMap<PartitionId, u64>,
}

/// Per-partition state for assertion checking.
#[derive(Debug, Clone, Copy)]
struct PartitionState {
    /// Last appended index for this partition.
    last_index: u64,
    /// Last appended term for this partition.
    last_term: u64,
}

impl<S: Storage> SharedWal<S> {
    /// Opens or creates a shared WAL.
    ///
    /// If the directory contains existing WAL data, it will be recovered.
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened or recovery fails.
    pub async fn open(storage: S, config: SharedWalConfig) -> WalResult<Self> {
        let wal = Wal::open(storage, config.wal_config).await?;
        Ok(Self {
            wal,
            partition_state: HashMap::new(),
            partition_durable: HashMap::new(),
            partition_truncated_after: HashMap::new(),
        })
    }

    /// Returns true if the WAL is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.wal.is_empty()
    }

    /// Returns the total number of entries across all partitions.
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.wal.entry_count()
    }

    /// Appends an entry for a partition.
    ///
    /// # Panics
    ///
    /// Panics if `TigerStyle` assertions fail:
    /// - Partition-local indices must be sequential (1, 2, 3...)
    /// - Term must be monotonically non-decreasing per partition
    ///
    /// # Errors
    /// Returns an error if the append fails.
    pub async fn append(
        &mut self,
        partition_id: PartitionId,
        term: u64,
        index: u64,
        payload: Bytes,
    ) -> WalResult<()> {
        // TigerStyle: Assert per-partition sequential indices.
        if let Some(state) = self.partition_state.get(&partition_id) {
            assert_eq!(
                index,
                state.last_index + 1,
                "partition {} index must be sequential: expected {}, got {}",
                partition_id,
                state.last_index + 1,
                index
            );
            assert!(
                term >= state.last_term,
                "partition {} term must be non-decreasing: last {}, got {}",
                partition_id,
                state.last_term,
                term
            );
        } else {
            // First entry for this partition - index should be 1 or explicitly set.
            // We allow any starting index to support recovery scenarios.
        }

        let entry = SharedEntry::new(partition_id, term, index, payload)?;
        self.wal.append(entry).await?;

        // Update partition state.
        self.partition_state.insert(
            partition_id,
            PartitionState {
                last_index: index,
                last_term: term,
            },
        );

        Ok(())
    }

    /// Syncs all pending entries to disk.
    ///
    /// After this call returns, all previously appended entries are durable.
    /// Updates per-partition durable indices on success.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub async fn sync(&mut self) -> WalResult<()> {
        self.wal.sync().await?;

        // Update per-partition durable indices after successful sync.
        for (partition_id, state) in &self.partition_state {
            self.partition_durable.insert(*partition_id, state.last_index);
        }

        Ok(())
    }

    /// Returns the durable index (last synced entry's index).
    ///
    /// Note: For shared WAL, this is the WAL-internal index, not partition-local.
    #[must_use]
    pub const fn durable_index(&self) -> Option<u64> {
        self.wal.durable_index()
    }

    /// Returns the durable index for a specific partition.
    ///
    /// Returns `None` if no entries have been synced for this partition.
    #[must_use]
    pub fn partition_durable_index(&self, partition_id: PartitionId) -> Option<u64> {
        self.partition_durable.get(&partition_id).copied()
    }

    /// Truncates entries after the given index for a partition.
    ///
    /// After truncation, entries with `index > after_index` are considered logically
    /// deleted. Subsequent appends must start from `after_index + 1`.
    ///
    /// # Logical Truncation
    ///
    /// This is a logical truncation - entries remain in the WAL but are filtered out
    /// during reads and recovery. They will be cleaned up when their segment ages out.
    ///
    /// When entries are later appended with indices that were truncated, the new entries
    /// "shadow" the old ones. Recovery uses last-write-wins semantics to handle this.
    ///
    /// # Panics
    ///
    /// Panics if `after_index` is greater than the last appended index for this partition
    /// (cannot truncate entries that don't exist).
    pub fn truncate_after(&mut self, partition_id: PartitionId, after_index: u64) {
        if let Some(state) = self.partition_state.get(&partition_id) {
            assert!(
                after_index <= state.last_index,
                "cannot truncate partition {} after index {} (last appended: {})",
                partition_id,
                after_index,
                state.last_index
            );
        }
        // else: no entries for this partition yet, truncation is a no-op

        // Record truncation point for filtering during reads.
        self.partition_truncated_after.insert(partition_id, after_index);

        // Update partition state so next append expects after_index + 1.
        // We preserve the term from the entry at after_index (or allow any term for new entries).
        if let Some(state) = self.partition_state.get_mut(&partition_id) {
            state.last_index = after_index;
            // Note: We don't reset last_term because the new leader may use a higher term.
            // The term monotonicity check will still work correctly.
        }

        // Update durable index if it was beyond the truncation point.
        if let Some(durable) = self.partition_durable.get_mut(&partition_id) {
            if *durable > after_index {
                *durable = after_index;
            }
        }
    }

    /// Returns the truncation point for a partition, if any.
    ///
    /// Returns `None` if no truncation has occurred for this partition.
    #[must_use]
    pub fn partition_truncated_after(&self, partition_id: PartitionId) -> Option<u64> {
        self.partition_truncated_after.get(&partition_id).copied()
    }

    /// Returns an iterator over all entries in the WAL.
    ///
    /// Entries are returned in append order (interleaved across partitions).
    pub fn entries(&self) -> impl Iterator<Item = &SharedEntry> {
        self.wal.entries()
    }

    /// Recovers entries from the WAL, grouped by partition.
    ///
    /// This is typically called during node startup to restore partition state.
    /// Each partition then rebuilds its in-memory Raft log from its entries.
    ///
    /// # Last-Write-Wins Semantics
    ///
    /// If multiple entries exist with the same partition and index (due to truncation
    /// and re-append), the last entry in WAL order wins. This ensures that after
    /// truncation and new appends, recovery returns the correct (new) entries.
    ///
    /// Also rebuilds internal partition state and durable tracking for assertion checking.
    ///
    /// # Errors
    /// Currently infallible, but returns `Result` for future extensibility.
    pub fn recover(&mut self) -> WalResult<HashMap<PartitionId, Vec<SharedEntry>>> {
        // Use HashMap to deduplicate: last-write-wins for same (partition, index).
        let mut by_partition_map: HashMap<PartitionId, HashMap<u64, SharedEntry>> = HashMap::new();

        for entry in self.wal.entries() {
            let partition_id = entry.partition_id();
            let index = entry.index();

            // Last-write-wins: later entries with same index overwrite earlier ones.
            by_partition_map
                .entry(partition_id)
                .or_default()
                .insert(index, entry.clone());
        }

        // Convert to sorted Vec and update partition state.
        let mut by_partition: HashMap<PartitionId, Vec<SharedEntry>> = HashMap::new();

        for (partition_id, index_map) in by_partition_map {
            // Sort by index.
            let mut entries: Vec<SharedEntry> = index_map.into_values().collect();
            entries.sort_by_key(SharedEntry::index);

            // Update partition state from the last (highest index) entry.
            if let Some(last_entry) = entries.last() {
                self.partition_state.insert(
                    partition_id,
                    PartitionState {
                        last_index: last_entry.index(),
                        last_term: last_entry.term(),
                    },
                );
            }

            by_partition.insert(partition_id, entries);
        }

        // Recovered entries are durable - update per-partition durable indices.
        for (partition_id, state) in &self.partition_state {
            self.partition_durable.insert(*partition_id, state.last_index);
        }

        // Clear truncation tracking since we've recovered to a consistent state.
        self.partition_truncated_after.clear();

        Ok(by_partition)
    }

    /// Returns entries for a specific partition.
    ///
    /// This scans the entire WAL and filters by partition ID.
    /// For recovery, prefer [`recover`](Self::recover) which scans once.
    ///
    /// # Last-Write-Wins and Truncation
    ///
    /// - Uses last-write-wins for duplicate indices (after truncation + re-append).
    /// - Respects truncation point: entries beyond the truncation point are excluded
    ///   unless newer entries have been appended.
    #[must_use]
    pub fn entries_for_partition(&self, partition_id: PartitionId) -> Vec<SharedEntry> {
        // Use HashMap for last-write-wins deduplication.
        let mut index_map: HashMap<u64, SharedEntry> = HashMap::new();

        for entry in self.wal.entries() {
            if entry.partition_id() == partition_id {
                // Last-write-wins: later entries overwrite earlier ones.
                index_map.insert(entry.index(), entry.clone());
            }
        }

        // Filter by truncation point if set.
        let max_valid_index = self
            .partition_state
            .get(&partition_id)
            .map(|s| s.last_index);

        // Sort by index and filter.
        let mut entries: Vec<SharedEntry> = index_map
            .into_values()
            .filter(|e| {
                // Only include entries up to the current last_index.
                max_valid_index.is_none_or(|max| e.index() <= max)
            })
            .collect();

        entries.sort_by_key(SharedEntry::index);
        entries
    }

    /// Reads a specific entry by partition ID and index.
    ///
    /// Returns `None` if no entry exists for the given partition and index,
    /// or if the index is beyond the current valid range (after truncation).
    ///
    /// Uses last-write-wins: if multiple entries exist with the same index
    /// (due to truncation and re-append), returns the last one.
    #[must_use]
    pub fn read(&self, partition_id: PartitionId, index: u64) -> Option<SharedEntry> {
        // Check if index is beyond current valid range.
        if let Some(state) = self.partition_state.get(&partition_id) {
            if index > state.last_index {
                return None;
            }
        }

        // Find last occurrence (last-write-wins).
        self.wal
            .entries()
            .filter(|e| e.partition_id() == partition_id && e.index() == index)
            .last()
            .cloned()
    }

    /// Returns the set of partition IDs that have entries in this WAL.
    pub fn partition_ids(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.partition_state.keys().copied()
    }
}

// ============================================================================
// Coordination Layer (Phase 3)
//
// Provides concurrent access to SharedWal via handles, automatic batching,
// and background flushing for fsync amortization.
// ============================================================================

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{oneshot, Mutex, Notify, RwLock};

/// Configuration for the coordinated shared WAL.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Base shared WAL configuration.
    pub wal_config: SharedWalConfig,
    /// Maximum time to wait before flushing buffered entries.
    pub flush_interval: Duration,
    /// Maximum entries to buffer before forcing a flush.
    pub max_buffer_entries: usize,
    /// Maximum bytes to buffer before forcing a flush.
    pub max_buffer_bytes: usize,
}

impl CoordinatorConfig {
    /// Creates a new coordinator configuration with defaults.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            wal_config: SharedWalConfig::new(dir),
            flush_interval: Duration::from_millis(1),
            max_buffer_entries: 1000,
            max_buffer_bytes: 16 * 1024 * 1024, // 16 MB
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

    /// Sets the segment configuration.
    #[must_use]
    pub fn with_segment_config(mut self, config: crate::SegmentConfig) -> Self {
        self.wal_config = self.wal_config.with_segment_config(config);
        self
    }
}

/// Acknowledgment that an entry is durable.
#[derive(Debug, Clone, Copy)]
pub struct DurableAck {
    /// The partition ID.
    pub partition_id: PartitionId,
    /// The partition's local index that was made durable.
    pub index: u64,
    /// The term of the entry.
    pub term: u64,
}

/// A partition's handle to a coordinated shared WAL.
///
/// This handle is `Clone + Send + Sync` and can be used from multiple tasks.
/// Writes are buffered internally and flushed by a background task.
#[derive(Clone)]
pub struct SharedWalHandle<S: Storage> {
    partition_id: PartitionId,
    inner: Arc<CoordinatorInner<S>>,
}

impl<S: Storage + Clone + Send + Sync + 'static> SharedWalHandle<S> {
    /// Appends an entry for this partition.
    ///
    /// Returns when the entry is durable (fsync'd to disk).
    ///
    /// # Panics
    ///
    /// The background flush task will panic if indices are not sequential
    /// or terms are not monotonically non-decreasing.
    ///
    /// # Errors
    ///
    /// Returns an error if the append or sync fails.
    pub async fn append(&self, term: u64, index: u64, payload: Bytes) -> WalResult<DurableAck> {
        let rx = self.append_async(term, index, payload).await?;
        rx.await.map_err(|_| crate::WalError::Shutdown)?
    }

    /// Appends an entry without waiting for durability.
    ///
    /// Returns a receiver that will be notified when the entry is durable.
    /// This allows batching multiple appends before waiting.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator is shut down.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn append_async(
        &self,
        term: u64,
        index: u64,
        payload: Bytes,
    ) -> WalResult<oneshot::Receiver<WalResult<DurableAck>>> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(crate::WalError::Shutdown);
        }

        let entry = SharedEntry::new(self.partition_id, term, index, payload.clone())?;
        let (tx, rx) = oneshot::channel();

        let pending = PendingWrite {
            entry,
            durable_tx: tx,
        };

        // Add to buffer.
        {
            let mut buffer = self.inner.buffer.lock().await;
            let entry_bytes = payload.len() + crate::SHARED_ENTRY_HEADER_SIZE;
            buffer.entries.push_back(pending);
            buffer.bytes += entry_bytes;

            // Check if we should trigger immediate flush.
            if buffer.entries.len() >= self.inner.config.max_buffer_entries
                || buffer.bytes >= self.inner.config.max_buffer_bytes
            {
                self.inner.flush_notify.notify_one();
            }
        }

        Ok(rx)
    }

    /// Returns the partition ID for this handle.
    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

/// A pending write waiting to be flushed.
struct PendingWrite {
    entry: SharedEntry,
    /// Notified when entry is durable.
    durable_tx: oneshot::Sender<WalResult<DurableAck>>,
}

/// Buffer of pending writes.
struct WriteBuffer {
    entries: VecDeque<PendingWrite>,
    bytes: usize,
}

impl WriteBuffer {
    #[allow(clippy::missing_const_for_fn)] // VecDeque::new() is not const
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
        }
    }

    fn drain(&mut self) -> Vec<PendingWrite> {
        self.bytes = 0;
        std::mem::take(&mut self.entries).into()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Internal state for the coordinator.
struct CoordinatorInner<S: Storage> {
    /// The underlying shared WAL (protected by mutex).
    wal: Mutex<SharedWal<S>>,
    /// Pending writes waiting to be flushed.
    buffer: Mutex<WriteBuffer>,
    /// Notifies `flush_loop` when buffer needs flushing.
    flush_notify: Notify,
    /// Configuration.
    config: CoordinatorConfig,
    /// Shutdown flag.
    shutdown: AtomicBool,
    /// Per-partition last index (for assertion without locking WAL).
    partition_last_index: RwLock<HashMap<PartitionId, u64>>,
}

/// Coordinated shared WAL with concurrent handles and automatic batching.
///
/// This is the production-ready interface for `SharedWal`. It provides:
/// - Concurrent access via `Clone + Send + Sync` handles
/// - Automatic write batching and fsync coalescing
/// - Background flush task for consistent latency
///
/// # Example
///
/// ```ignore
/// use helix_wal::{SharedWalCoordinator, CoordinatorConfig, TokioStorage};
/// use helix_core::PartitionId;
/// use bytes::Bytes;
///
/// let config = CoordinatorConfig::new("/tmp/shared-wal");
/// let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config).await?;
///
/// // Get handles for partitions.
/// let h1 = coordinator.handle(PartitionId::new(1));
/// let h2 = coordinator.handle(PartitionId::new(2));
///
/// // Concurrent writes - each returns when durable.
/// let (r1, r2) = tokio::join!(
///     h1.append(1, 1, Bytes::from("data1")),
///     h2.append(1, 1, Bytes::from("data2")),
/// );
///
/// // Shutdown gracefully.
/// coordinator.shutdown().await?;
/// ```
pub struct SharedWalCoordinator<S: Storage> {
    inner: Arc<CoordinatorInner<S>>,
    /// Handle to the flush task (for shutdown).
    flush_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl<S: Storage + Clone + Send + Sync + 'static> SharedWalCoordinator<S> {
    /// Opens or creates a coordinated shared WAL.
    ///
    /// This spawns a background flush task that batches writes and syncs.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying WAL cannot be opened.
    pub async fn open(storage: S, config: CoordinatorConfig) -> WalResult<Self> {
        let wal = SharedWal::open(storage, config.wal_config.clone()).await?;

        let inner = Arc::new(CoordinatorInner {
            wal: Mutex::new(wal),
            buffer: Mutex::new(WriteBuffer::new()),
            flush_notify: Notify::new(),
            config,
            shutdown: AtomicBool::new(false),
            partition_last_index: RwLock::new(HashMap::new()),
        });

        // Spawn flush task.
        let flush_inner = inner.clone();
        let flush_handle = tokio::spawn(async move {
            flush_loop(flush_inner).await;
        });

        Ok(Self {
            inner,
            flush_handle: Mutex::new(Some(flush_handle)),
        })
    }

    /// Gets a handle for a partition.
    ///
    /// Each partition should call this once and clone the handle as needed.
    /// The handle is `Clone + Send + Sync`.
    #[must_use]
    pub fn handle(&self, partition_id: PartitionId) -> SharedWalHandle<S> {
        SharedWalHandle {
            partition_id,
            inner: self.inner.clone(),
        }
    }

    /// Forces a flush of all buffered entries.
    ///
    /// This is normally not needed as the background task handles flushing,
    /// but can be useful for testing or explicit sync points.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    pub async fn flush(&self) -> WalResult<()> {
        // Notify flush task and wait for buffer to drain.
        self.inner.flush_notify.notify_one();

        // Wait for buffer to empty.
        loop {
            {
                let buffer = self.inner.buffer.lock().await;
                if buffer.is_empty() {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    /// Recovers entries from the WAL, grouped by partition.
    ///
    /// This should be called during startup before issuing new writes.
    /// It also rebuilds internal state for assertion checking.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub async fn recover(&self) -> WalResult<HashMap<PartitionId, Vec<SharedEntry>>> {
        let result = {
            let mut wal = self.inner.wal.lock().await;
            wal.recover()?
        };

        // Update partition_last_index from recovered state.
        {
            let mut last_index = self.inner.partition_last_index.write().await;
            for (partition_id, entries) in &result {
                if let Some(last) = entries.last() {
                    last_index.insert(*partition_id, last.index());
                }
            }
        }

        Ok(result)
    }

    /// Returns the durable index for a specific partition.
    ///
    /// Returns `None` if no entries have been synced for this partition.
    pub async fn partition_durable_index(&self, partition_id: PartitionId) -> Option<u64> {
        let wal = self.inner.wal.lock().await;
        wal.partition_durable_index(partition_id)
    }

    /// Shuts down the coordinator gracefully.
    ///
    /// Flushes any remaining buffered entries and stops the flush task.
    ///
    /// # Errors
    ///
    /// Returns an error if the final flush fails.
    pub async fn shutdown(&self) -> WalResult<()> {
        // Signal shutdown.
        self.inner.shutdown.store(true, Ordering::Release);

        // Wake flush task to process final entries.
        self.inner.flush_notify.notify_one();

        // Wait for flush task to complete.
        let handle = self.flush_handle.lock().await.take();
        if let Some(h) = handle {
            let _ = h.await;
        }

        Ok(())
    }

    /// Returns true if the WAL is empty.
    pub async fn is_empty(&self) -> bool {
        let wal = self.inner.wal.lock().await;
        wal.is_empty()
    }

    /// Returns the total number of entries across all partitions.
    pub async fn entry_count(&self) -> u64 {
        let wal = self.inner.wal.lock().await;
        wal.entry_count()
    }
}

/// Background task that batches writes and syncs.
#[allow(clippy::significant_drop_tightening)] // WAL lock held during write+sync is intentional
async fn flush_loop<S: Storage + Clone + Send + Sync + 'static>(inner: Arc<CoordinatorInner<S>>) {
    loop {
        // Wait for flush interval or notification.
        tokio::select! {
            () = tokio::time::sleep(inner.config.flush_interval) => {}
            () = inner.flush_notify.notified() => {}
        }

        // Check for shutdown.
        let shutting_down = inner.shutdown.load(Ordering::Acquire);

        // Drain buffer atomically.
        let pending: Vec<PendingWrite> = {
            let mut buffer = inner.buffer.lock().await;
            if buffer.is_empty() {
                if shutting_down {
                    break;
                }
                continue;
            }
            buffer.drain()
        };

        // Write all entries to WAL.
        let mut wal = inner.wal.lock().await;
        let mut results: Vec<(oneshot::Sender<WalResult<DurableAck>>, WalResult<DurableAck>)> =
            Vec::with_capacity(pending.len());

        for pw in pending {
            let partition_id = pw.entry.partition_id();
            let index = pw.entry.index();
            let term = pw.entry.term();

            match wal
                .append(partition_id, term, index, pw.entry.payload.clone())
                .await
            {
                Ok(()) => {
                    results.push((
                        pw.durable_tx,
                        Ok(DurableAck {
                            partition_id,
                            index,
                            term,
                        }),
                    ));
                }
                Err(e) => {
                    results.push((pw.durable_tx, Err(e)));
                }
            }
        }

        // Single fsync for entire batch.
        let sync_result = wal.sync().await;

        // Notify all waiters.
        match sync_result {
            Ok(()) => {
                // Update partition_last_index for successful writes.
                {
                    let mut last_index = inner.partition_last_index.write().await;
                    for (_, result) in &results {
                        if let Ok(ack) = result {
                            last_index.insert(ack.partition_id, ack.index);
                        }
                    }
                }

                // Send success notifications.
                for (tx, result) in results {
                    let _ = tx.send(result);
                }
            }
            Err(e) => {
                // Send failure notifications to all.
                for (tx, _) in results {
                    let _ = tx.send(Err(e.clone()));
                }
            }
        }

        if shutting_down {
            break;
        }
    }
}

// ----------------------------------------------------------------------------
// SharedWalPool - Multiple Shared WALs
// ----------------------------------------------------------------------------

/// Maximum number of WALs in a pool.
///
/// # Why 16?
///
/// 1. **NVMe fsync parallelism**: Modern NVMe SSDs handle ~4-8 parallel fsyncs
///    efficiently. Beyond that, the device's internal queue depth and flash
///    translation layer become the bottleneck, not the number of parallel ops.
///
/// 2. **NUMA topology**: Typical servers have 2-8 NUMA nodes. The optimal setup
///    is 1 WAL per NUMA node for memory locality. 16 covers large multi-socket
///    systems.
///
/// 3. **Diminishing returns**: With 100 partitions, 1 WAL gives 100x fsync
///    reduction, 4 WALs gives 25x, 16 WALs gives 6x. Beyond 16, you're
///    approaching per-partition WALs with little amortization benefit.
///
/// 4. **TigerStyle**: "Put a limit on everything." This catches configuration
///    bugs early (e.g., `wal_count = 1000` is almost certainly a mistake).
pub const POOL_WAL_COUNT_MAX: u32 = 16;

/// Configuration for a pool of shared WALs.
///
/// Use a pool when you need more than one fsync stream, typically:
/// - 1 WAL per NUMA node for memory locality
/// - Multiple WALs to parallelize fsyncs on high-core systems
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Base directory for all WALs. Each WAL gets a subdirectory.
    pub base_dir: PathBuf,
    /// Number of WALs in the pool.
    pub wal_count: u32,
    /// Configuration for each WAL's coordinator.
    pub coordinator_config: CoordinatorConfig,
}

impl PoolConfig {
    /// Creates a new pool configuration.
    ///
    /// # Panics
    ///
    /// Panics if `wal_count` is 0 or exceeds `POOL_WAL_COUNT_MAX`.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>, wal_count: u32) -> Self {
        // TigerStyle: Assert preconditions.
        assert!(wal_count > 0, "wal_count must be at least 1");
        assert!(
            wal_count <= POOL_WAL_COUNT_MAX,
            "wal_count {wal_count} exceeds max {POOL_WAL_COUNT_MAX}"
        );

        let base_dir = base_dir.into();
        // Create a default coordinator config; the WAL directory will be set per-WAL.
        let coordinator_config = CoordinatorConfig::new(&base_dir);

        Self {
            base_dir,
            wal_count,
            coordinator_config,
        }
    }

    /// Sets the flush interval for all WALs.
    #[must_use]
    pub const fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.coordinator_config.flush_interval = interval;
        self
    }

    /// Sets the maximum buffer entries for all WALs.
    #[must_use]
    pub const fn with_max_buffer_entries(mut self, max: usize) -> Self {
        self.coordinator_config.max_buffer_entries = max;
        self
    }
}

/// A pool of shared WALs for distributing partitions across multiple fsync streams.
///
/// # Why Use a Pool?
///
/// A single shared WAL amortizes fsync cost (N partitions → 1 fsync), but serializes
/// all writes through one file. A pool distributes partitions across K WALs, enabling:
/// - K parallel fsyncs (better `NVMe` utilization)
/// - K parallel write streams (reduced lock contention)
/// - NUMA-aware placement (1 WAL per NUMA node)
///
/// # Partition Assignment
///
/// Partitions are assigned to WALs by hashing: `partition_id % wal_count`. This ensures:
/// - Deterministic assignment (same partition always goes to same WAL)
/// - Even distribution (assuming partition IDs are well-distributed)
/// - No coordination needed (each partition knows its WAL)
pub struct SharedWalPool<S: Storage> {
    /// The WALs in this pool, indexed by `partition_id % wal_count`.
    coordinators: Vec<SharedWalCoordinator<S>>,
    /// Number of WALs (stored separately to avoid Vec length lookup in hot path).
    wal_count: u32,
}

impl<S: Storage + Clone + Send + Sync + 'static> SharedWalPool<S> {
    /// Opens or creates a pool of shared WALs.
    ///
    /// Creates `config.wal_count` subdirectories under `config.base_dir`, each
    /// containing one shared WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if any WAL fails to open.
    ///
    /// # Panics
    ///
    /// Panics if `config.wal_count` is 0 or exceeds `POOL_WAL_COUNT_MAX`.
    pub async fn open(storage: S, config: PoolConfig) -> WalResult<Self> {
        // TigerStyle: Assert preconditions (also checked in PoolConfig::new).
        assert!(config.wal_count > 0);
        assert!(config.wal_count <= POOL_WAL_COUNT_MAX);

        let mut coordinators = Vec::with_capacity(config.wal_count as usize);

        for i in 0..config.wal_count {
            // Each WAL gets its own subdirectory: base_dir/wal-00, wal-01, etc.
            let wal_dir = config.base_dir.join(format!("wal-{i:02}"));
            let mut wal_config = config.coordinator_config.clone();
            wal_config.wal_config = SharedWalConfig::new(&wal_dir);

            let coordinator = SharedWalCoordinator::open(storage.clone(), wal_config).await?;
            coordinators.push(coordinator);
        }

        // TigerStyle: Assert postcondition.
        assert_eq!(coordinators.len(), config.wal_count as usize);

        Ok(Self {
            coordinators,
            wal_count: config.wal_count,
        })
    }

    /// Gets a handle for a partition.
    ///
    /// The partition is assigned to a WAL by hashing: `partition_id % wal_count`.
    /// This assignment is deterministic and stable.
    ///
    /// # Panics
    ///
    /// Panics if the internal state is inconsistent (should never happen).
    #[must_use]
    pub fn handle(&self, partition_id: PartitionId) -> SharedWalHandle<S> {
        // TigerStyle: Explicit cast with bounds check via assert.
        // Safe: wal_count <= POOL_WAL_COUNT_MAX (16), so result fits in usize on any platform.
        #[allow(clippy::cast_possible_truncation)]
        let wal_index = (partition_id.get() % u64::from(self.wal_count)) as usize;
        assert!(wal_index < self.coordinators.len());

        self.coordinators[wal_index].handle(partition_id)
    }

    /// Returns which WAL index a partition is assigned to.
    ///
    /// Useful for debugging and metrics.
    ///
    /// # Panics
    ///
    /// Panics if the internal state is inconsistent (should never happen).
    #[must_use]
    pub fn wal_index_for_partition(&self, partition_id: PartitionId) -> u32 {
        #[allow(clippy::cast_possible_truncation)] // Bounded by wal_count.
        let index = (partition_id.get() % u64::from(self.wal_count)) as u32;
        assert!(index < self.wal_count);
        index
    }

    /// Returns the number of WALs in the pool.
    #[must_use]
    pub const fn wal_count(&self) -> u32 {
        self.wal_count
    }

    /// Recovers entries from all WALs, grouped by partition.
    ///
    /// Merges recovery results from all WALs in the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if any WAL fails to recover.
    ///
    /// # Panics
    ///
    /// Panics if recovered entries are not sorted by index (indicates WAL corruption).
    pub async fn recover(&self) -> WalResult<HashMap<PartitionId, Vec<SharedEntry>>> {
        let mut all_entries: HashMap<PartitionId, Vec<SharedEntry>> = HashMap::new();

        for coordinator in &self.coordinators {
            let wal_entries = coordinator.recover().await?;
            for (partition_id, entries) in wal_entries {
                all_entries.entry(partition_id).or_default().extend(entries);
            }
        }

        // TigerStyle: Assert postcondition - entries should be sorted per partition.
        for entries in all_entries.values() {
            for window in entries.windows(2) {
                assert!(
                    window[0].index() < window[1].index(),
                    "recovered entries must be sorted by index"
                );
            }
        }

        Ok(all_entries)
    }

    /// Shuts down all WALs in the pool.
    ///
    /// # Errors
    ///
    /// Returns an error if any WAL fails to shut down.
    pub async fn shutdown(&self) -> WalResult<()> {
        for coordinator in &self.coordinators {
            coordinator.shutdown().await?;
        }
        Ok(())
    }

    /// Returns true if all WALs in the pool are empty.
    pub async fn is_empty(&self) -> bool {
        for coordinator in &self.coordinators {
            if !coordinator.is_empty().await {
                return false;
            }
        }
        true
    }

    /// Returns the total entry count across all WALs.
    pub async fn entry_count(&self) -> u64 {
        let mut total = 0u64;
        for coordinator in &self.coordinators {
            total += coordinator.entry_count().await;
        }
        total
    }
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::TokioStorage;

    #[tokio::test]
    async fn test_shared_wal_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();
        assert!(wal.is_empty());

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        // Append entries from two partitions.
        wal.append(p1, 1, 1, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, Bytes::from("p1-2")).await.unwrap();
        wal.append(p2, 1, 2, Bytes::from("p2-2")).await.unwrap();

        wal.sync().await.unwrap();

        assert_eq!(wal.entry_count(), 4);
    }

    #[tokio::test]
    async fn test_shared_wal_recover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);
        let p3 = PartitionId::new(3);

        // Write entries.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            wal.append(p1, 1, 1, Bytes::from("p1-1")).await.unwrap();
            wal.append(p2, 1, 1, Bytes::from("p2-1")).await.unwrap();
            wal.append(p1, 1, 2, Bytes::from("p1-2")).await.unwrap();
            wal.append(p3, 1, 1, Bytes::from("p3-1")).await.unwrap();
            wal.append(p2, 1, 2, Bytes::from("p2-2")).await.unwrap();

            wal.sync().await.unwrap();
        }

        // Reopen and recover.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let by_partition = wal.recover().unwrap();

            // Verify partition 1.
            let p1_entries = by_partition.get(&p1).unwrap();
            assert_eq!(p1_entries.len(), 2);
            assert_eq!(p1_entries[0].index(), 1);
            assert_eq!(p1_entries[1].index(), 2);

            // Verify partition 2.
            let p2_entries = by_partition.get(&p2).unwrap();
            assert_eq!(p2_entries.len(), 2);
            assert_eq!(p2_entries[0].index(), 1);
            assert_eq!(p2_entries[1].index(), 2);

            // Verify partition 3.
            let p3_entries = by_partition.get(&p3).unwrap();
            assert_eq!(p3_entries.len(), 1);
            assert_eq!(p3_entries[0].index(), 1);

            // Can continue appending after recovery.
            wal.append(p1, 1, 3, Bytes::from("p1-3")).await.unwrap();
            wal.append(p3, 1, 2, Bytes::from("p3-2")).await.unwrap();
            wal.sync().await.unwrap();

            assert_eq!(wal.entry_count(), 7);
        }
    }

    #[tokio::test]
    async fn test_shared_wal_entries_for_partition() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        wal.append(p1, 1, 1, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, Bytes::from("p1-2")).await.unwrap();
        wal.append(p1, 1, 3, Bytes::from("p1-3")).await.unwrap();
        wal.sync().await.unwrap();

        let p1_entries = wal.entries_for_partition(p1);
        assert_eq!(p1_entries.len(), 3);

        let p2_entries = wal.entries_for_partition(p2);
        assert_eq!(p2_entries.len(), 1);

        let p3_entries = wal.entries_for_partition(PartitionId::new(3));
        assert_eq!(p3_entries.len(), 0);
    }

    #[tokio::test]
    #[should_panic(expected = "index must be sequential")]
    async fn test_shared_wal_asserts_sequential_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);

        wal.append(p1, 1, 1, Bytes::from("p1-1")).await.unwrap();
        // Skip index 2 - should panic.
        wal.append(p1, 1, 3, Bytes::from("p1-3")).await.unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "term must be non-decreasing")]
    async fn test_shared_wal_asserts_term_monotonic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);

        wal.append(p1, 5, 1, Bytes::from("p1-1")).await.unwrap();
        // Term goes backwards - should panic.
        wal.append(p1, 3, 2, Bytes::from("p1-2")).await.unwrap();
    }

    #[tokio::test]
    async fn test_shared_wal_independent_partition_indices() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        // Both partitions can have index 1, 2, 3...
        wal.append(p1, 1, 1, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, Bytes::from("p1-2")).await.unwrap();
        wal.append(p2, 1, 2, Bytes::from("p2-2")).await.unwrap();

        wal.sync().await.unwrap();

        // Verify both partitions have their own index sequences.
        let p1_entries = wal.entries_for_partition(p1);
        assert_eq!(p1_entries[0].index(), 1);
        assert_eq!(p1_entries[1].index(), 2);

        let p2_entries = wal.entries_for_partition(p2);
        assert_eq!(p2_entries[0].index(), 1);
        assert_eq!(p2_entries[1].index(), 2);
    }

    #[tokio::test]
    async fn test_shared_wal_truncate_after_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);

        // Append entries 1-5.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Truncate after index 3.
        wal.truncate_after(p1, 3);

        // Entries 4, 5 should no longer be visible.
        let entries = wal.entries_for_partition(p1);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.last().unwrap().index(), 3);

        // read() should return None for truncated indices.
        assert!(wal.read(p1, 3).is_some());
        assert!(wal.read(p1, 4).is_none());
        assert!(wal.read(p1, 5).is_none());

        // Can append new entries starting from 4.
        wal.append(p1, 2, 4, Bytes::from("p1-4-new"))
            .await
            .unwrap();
        wal.append(p1, 2, 5, Bytes::from("p1-5-new"))
            .await
            .unwrap();
        wal.sync().await.unwrap();

        // Now we should have 5 entries: 1, 2, 3, 4-new, 5-new.
        let entries = wal.entries_for_partition(p1);
        assert_eq!(entries.len(), 5);

        // The new entries should have term 2.
        assert_eq!(entries[3].term(), 2);
        assert_eq!(entries[4].term(), 2);
    }

    #[tokio::test]
    async fn test_shared_wal_truncate_and_recover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = PartitionId::new(1);

        // Phase 1: Write, truncate, write new entries.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            // Write entries 1-5 at term 1.
            for i in 1..=5u64 {
                wal.append(p1, 1, i, Bytes::from(format!("old-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();

            // Truncate after 3.
            wal.truncate_after(p1, 3);

            // Write new entries 4-6 at term 2.
            for i in 4..=6u64 {
                wal.append(p1, 2, i, Bytes::from(format!("new-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Phase 2: Recover and verify last-write-wins.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let by_partition = wal.recover().unwrap();
            let entries = by_partition.get(&p1).unwrap();

            // Should have 6 entries: 1, 2, 3 (old), 4, 5, 6 (new).
            assert_eq!(entries.len(), 6);

            // Entries 1-3 should be from term 1 (old).
            assert_eq!(entries[0].term(), 1);
            assert_eq!(entries[1].term(), 1);
            assert_eq!(entries[2].term(), 1);

            // Entries 4-6 should be from term 2 (new, shadowing old 4-5).
            assert_eq!(entries[3].term(), 2);
            assert_eq!(entries[4].term(), 2);
            assert_eq!(entries[5].term(), 2);

            // Verify payload shows new entries shadowed old ones.
            assert_eq!(entries[3].payload.as_ref(), b"new-4");
            assert_eq!(entries[4].payload.as_ref(), b"new-5");
        }
    }

    #[tokio::test]
    #[should_panic(expected = "cannot truncate partition")]
    async fn test_shared_wal_truncate_beyond_last_panics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);

        // Append entries 1-3.
        for i in 1..=3u64 {
            wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
        }

        // Truncate beyond last index - should panic.
        wal.truncate_after(p1, 5);
    }

    #[tokio::test]
    async fn test_shared_wal_truncate_multiple_partitions() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        // Append entries for both partitions.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
            wal.append(p2, 1, i, Bytes::from(format!("p2-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Truncate p1 after 2, leave p2 intact.
        wal.truncate_after(p1, 2);

        // p1 should have 2 entries, p2 should have 5.
        assert_eq!(wal.entries_for_partition(p1).len(), 2);
        assert_eq!(wal.entries_for_partition(p2).len(), 5);

        // Append new entries to p1.
        wal.append(p1, 2, 3, Bytes::from("p1-3-new"))
            .await
            .unwrap();
        wal.sync().await.unwrap();

        assert_eq!(wal.entries_for_partition(p1).len(), 3);
        assert_eq!(wal.entries_for_partition(p2).len(), 5);
    }
}

// DST (Deterministic Simulation Testing) tests have been moved to helix-tests/src/shared_wal_dst.rs
// for consistent test organization. See helix-tests/src/lib.rs for test organization guidelines.
// ============================================================================
// Coordinator Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::redundant_closure_for_method_calls)]
#[allow(clippy::unnecessary_cast)]
#[allow(clippy::cast_precision_loss)]
#[allow(clippy::too_many_lines)]
mod coordinator_tests {
    use super::*;
    use crate::storage::TokioStorage;

    #[tokio::test]
    async fn test_coordinator_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(10));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        let h1 = coordinator.handle(p1);
        let h2 = coordinator.handle(p2);

        // Concurrent writes from two partitions.
        let (r1, r2) = tokio::join!(
            h1.append(1, 1, Bytes::from("p1-1")),
            h2.append(1, 1, Bytes::from("p2-1")),
        );

        assert!(r1.is_ok());
        assert!(r2.is_ok());

        let ack1 = r1.unwrap();
        let ack2 = r2.unwrap();

        assert_eq!(ack1.partition_id, p1);
        assert_eq!(ack1.index, 1);
        assert_eq!(ack2.partition_id, p2);
        assert_eq!(ack2.index, 1);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_sequential_writes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = PartitionId::new(1);
        let h1 = coordinator.handle(p1);

        // Sequential writes.
        for i in 1..=10u64 {
            let ack = h1.append(1, i, Bytes::from(format!("entry-{i}"))).await.unwrap();
            assert_eq!(ack.index, i);
        }

        assert_eq!(coordinator.entry_count().await, 10);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_multi_partition_concurrent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let partition_count = 10;
        let entries_per_partition = 5;

        // Create handles for all partitions.
        let handles: Vec<_> = (1..=partition_count)
            .map(|p| coordinator.handle(PartitionId::new(p)))
            .collect();

        // Spawn concurrent writers.
        let mut tasks = Vec::new();
        for (p_idx, handle) in handles.into_iter().enumerate() {
            let task = tokio::spawn(async move {
                for i in 1..=entries_per_partition as u64 {
                    let ack = handle
                        .append(1, i, Bytes::from(format!("p{}-{}", p_idx + 1, i)))
                        .await
                        .unwrap();
                    assert_eq!(ack.index, i);
                }
            });
            tasks.push(task);
        }

        // Wait for all writers.
        for task in tasks {
            task.await.unwrap();
        }

        let total_entries = partition_count * entries_per_partition;
        assert_eq!(coordinator.entry_count().await, total_entries as u64);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);

        // Phase 1: Write entries.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            let h1 = coordinator.handle(p1);
            let h2 = coordinator.handle(p2);

            for i in 1..=5u64 {
                h1.append(1, i, Bytes::from(format!("p1-{i}"))).await.unwrap();
                h2.append(1, i, Bytes::from(format!("p2-{i}"))).await.unwrap();
            }

            coordinator.shutdown().await.unwrap();
        }

        // Phase 2: Recover.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let recovered = coordinator.recover().await.unwrap();

            // Verify both partitions recovered.
            assert_eq!(recovered.get(&p1).map(|v| v.len()), Some(5));
            assert_eq!(recovered.get(&p2).map(|v| v.len()), Some(5));

            // Can continue appending.
            let h1 = coordinator.handle(p1);
            h1.append(1, 6, Bytes::from("p1-6")).await.unwrap();

            assert_eq!(coordinator.entry_count().await, 11);

            coordinator.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_coordinator_handle_clone() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = PartitionId::new(1);
        let h1 = coordinator.handle(p1);
        let h1_clone = h1.clone();

        // Both handles can write.
        h1.append(1, 1, Bytes::from("from-original")).await.unwrap();
        h1_clone.append(1, 2, Bytes::from("from-clone")).await.unwrap();

        assert_eq!(coordinator.entry_count().await, 2);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_flush() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_secs(60)); // Long interval

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = PartitionId::new(1);
        let h1 = coordinator.handle(p1);

        // Write without waiting for automatic flush.
        let rx = h1.append_async(1, 1, Bytes::from("entry")).await.unwrap();

        // Force flush.
        coordinator.flush().await.unwrap();

        // Now the entry should be durable.
        let ack = rx.await.unwrap().unwrap();
        assert_eq!(ack.index, 1);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_shutdown_flushes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_secs(60)); // Long interval

        let p1 = PartitionId::new(1);

        // Phase 1: Write using append_async and shutdown (should flush).
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            let h1 = coordinator.handle(p1);
            // Use append_async so we don't wait for the 60s flush interval.
            let _rx = h1.append_async(1, 1, Bytes::from("entry-1")).await.unwrap();

            // Shutdown should flush any pending entries.
            coordinator.shutdown().await.unwrap();
        }

        // Phase 2: Verify entry was persisted.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let recovered = coordinator.recover().await.unwrap();
            assert_eq!(recovered.get(&p1).map(|v| v.len()), Some(1));

            coordinator.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_coordinator_durable_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = PartitionId::new(1);
        let h1 = coordinator.handle(p1);

        // Initially no durable index.
        assert_eq!(coordinator.partition_durable_index(p1).await, None);

        // Write and wait for durability.
        h1.append(1, 1, Bytes::from("entry-1")).await.unwrap();
        h1.append(1, 2, Bytes::from("entry-2")).await.unwrap();

        // Now durable index should be 2.
        assert_eq!(coordinator.partition_durable_index(p1).await, Some(2));

        coordinator.shutdown().await.unwrap();
    }
}

// ----------------------------------------------------------------------------
// Pool Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod pool_tests {
    use super::*;
    use crate::storage::TokioStorage;

    #[tokio::test]
    async fn test_pool_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PoolConfig::new(temp_dir.path(), 2)
            .with_flush_interval(Duration::from_millis(10));

        let pool = SharedWalPool::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Verify pool has correct count.
        assert_eq!(pool.wal_count(), 2);
        assert!(pool.is_empty().await);

        // Partitions should be distributed across WALs.
        let p1 = PartitionId::new(1);
        let p2 = PartitionId::new(2);
        let p3 = PartitionId::new(3);

        // With 2 WALs: p1 -> wal 1, p2 -> wal 0, p3 -> wal 1.
        assert_eq!(pool.wal_index_for_partition(p1), 1);
        assert_eq!(pool.wal_index_for_partition(p2), 0);
        assert_eq!(pool.wal_index_for_partition(p3), 1);

        // Get handles and write.
        let h1 = pool.handle(p1);
        let h2 = pool.handle(p2);

        h1.append(1, 1, Bytes::from("p1-entry-1")).await.unwrap();
        h2.append(1, 1, Bytes::from("p2-entry-1")).await.unwrap();

        // Verify entry count.
        assert_eq!(pool.entry_count().await, 2);

        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_pool_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let p1 = PartitionId::new(10);
        let p2 = PartitionId::new(11);

        // Write entries.
        {
            let config = PoolConfig::new(&base_path, 4)
                .with_flush_interval(Duration::from_millis(5));

            let pool = SharedWalPool::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let h1 = pool.handle(p1);
            let h2 = pool.handle(p2);

            h1.append(1, 1, Bytes::from("p1-1")).await.unwrap();
            h1.append(1, 2, Bytes::from("p1-2")).await.unwrap();
            h2.append(1, 1, Bytes::from("p2-1")).await.unwrap();

            pool.shutdown().await.unwrap();
        }

        // Recover entries.
        {
            let config = PoolConfig::new(&base_path, 4)
                .with_flush_interval(Duration::from_millis(5));

            let pool = SharedWalPool::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let recovered = pool.recover().await.unwrap();

            // Verify p1 entries.
            let p1_entries = recovered.get(&p1).unwrap();
            assert_eq!(p1_entries.len(), 2);
            assert_eq!(p1_entries[0].index(), 1);
            assert_eq!(p1_entries[1].index(), 2);

            // Verify p2 entries.
            let p2_entries = recovered.get(&p2).unwrap();
            assert_eq!(p2_entries.len(), 1);
            assert_eq!(p2_entries[0].index(), 1);

            pool.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_pool_partition_distribution() {
        // Test that partitions are evenly distributed.
        let temp_dir = tempfile::tempdir().unwrap();
        let config = PoolConfig::new(temp_dir.path(), 4);

        let pool = SharedWalPool::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Track which WAL each partition goes to.
        let mut wal_counts = [0u32; 4];
        for i in 0..100 {
            let partition = PartitionId::new(i);
            let wal_idx = pool.wal_index_for_partition(partition);
            wal_counts[wal_idx as usize] += 1;
        }

        // With sequential partition IDs mod 4, distribution should be exact.
        assert_eq!(wal_counts[0], 25);
        assert_eq!(wal_counts[1], 25);
        assert_eq!(wal_counts[2], 25);
        assert_eq!(wal_counts[3], 25);

        pool.shutdown().await.unwrap();
    }

    #[test]
    #[should_panic(expected = "wal_count must be at least 1")]
    fn test_pool_config_zero_wals() {
        let _config = PoolConfig::new("/tmp/test", 0);
    }

    #[test]
    #[should_panic(expected = "exceeds max")]
    fn test_pool_config_too_many_wals() {
        let _config = PoolConfig::new("/tmp/test", POOL_WAL_COUNT_MAX + 1);
    }
}

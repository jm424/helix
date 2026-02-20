//! Shared WAL for multiple partitions.
//!
//! A shared WAL allows multiple partitions to share a single WAL, amortizing
//! fsync cost across partitions. Instead of N fsyncs for N partitions, a single
//! fsync makes all buffered entries durable.
//!
//! # Design
//!
//! - Entries are tagged with group ID via [`SharedEntry`]
//! - Group-local indices are used (not WAL-global)
//! - Recovery scans sequentially and groups entries by group
//! - Per-group `BTreeMap` index for O(log n) entry lookups
//!
//! # Example
//!
//! ```ignore
//! use helix_wal::{SharedWal, SharedWalConfig, TokioStorage};
//! use helix_core::GroupId;
//! use bytes::Bytes;
//!
//! let config = SharedWalConfig::new("/tmp/shared-wal");
//! let mut wal = SharedWal::open(TokioStorage::new(), config).await?;
//!
//! // Append entries from different groups.
//! let g1 = GroupId::new(1);
//! let g2 = GroupId::new(2);
//!
//! wal.append(g1, 1, 1, Bytes::from("g1-data")).await?;
//! wal.append(g2, 1, 1, Bytes::from("g2-data")).await?;
//!
//! // Single sync makes both entries durable.
//! wal.sync().await?;
//!
//! // Recovery groups entries by group (loads segments one at a time to avoid OOM).
//! let by_group = wal.recover().await?;
//! ```

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use bytes::Bytes;
use helix_core::{GroupId, WriteDurability};

use tracing::warn;

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

    /// Controls whether to fsync when rotating segments.
    #[must_use]
    pub const fn with_sync_on_rotation(mut self, sync: bool) -> Self {
        self.wal_config.sync_on_rotation = sync;
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
    /// Per-group tracking for sequential index assertion.
    group_state: HashMap<GroupId, GroupState>,
    /// Per-group durable index (last synced index per group).
    group_durable: HashMap<GroupId, u64>,
    /// Per-group truncation point (entries with index > this are logically deleted).
    /// None means no truncation has occurred for that group.
    group_truncated_after: HashMap<GroupId, u64>,
    /// Per-group index for O(log n) entry lookups.
    ///
    /// Maps `(group_id, raft_index) → SharedEntry`. Maintained in sync with
    /// append/truncate/recover operations. `SharedEntry` payload is `Bytes`
    /// (Arc-backed), so clones share data cheaply (~56 bytes of header + pointer).
    group_index: HashMap<GroupId, BTreeMap<u64, SharedEntry>>,
    /// Lightweight index for evicted entries.
    ///
    /// Maps `group_id → (wal_counter → (segment_id, raft_index))`. Used to
    /// locate entries on disk after their `SharedEntry` has been evicted from
    /// `group_index`. The stored `raft_index` enables the retention check to
    /// compare against Raft match indices without loading entries from disk.
    /// Each entry costs ~24 bytes (vs ~56 bytes for the full `SharedEntry`).
    evicted_index: HashMap<GroupId, BTreeMap<u64, (crate::SegmentId, u64)>>,
}

/// Per-group recovery metadata returned by [`SharedWal::recover_streaming`].
///
/// Contains the minimum state needed to initialize a Raft node after recovery:
/// the last WAL index (auto-counter), the Raft index of that entry (for
/// `compacted_index`), and its term. Payloads are never stored here.
#[derive(Debug, Clone, Copy, Default)]
#[allow(clippy::struct_field_names)]
pub struct GroupRecoveryState {
    /// WAL auto-counter index of the last entry for this group.
    pub last_wal_index: u64,
    /// Raft term of the last entry.
    pub last_term: u64,
    /// Raft log index of the last entry (used as `compacted_index`).
    pub last_raft_index: u64,
}

/// Per-group state for assertion checking.
#[derive(Debug, Clone, Copy)]
struct GroupState {
    /// Last appended index for this group.
    last_index: u64,
    /// Last appended term for this group.
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
            group_state: HashMap::new(),
            group_durable: HashMap::new(),
            group_truncated_after: HashMap::new(),
            group_index: HashMap::new(),
            evicted_index: HashMap::new(),
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

    /// Returns the last index for a specific group from internal state.
    ///
    /// This reflects entries that have been processed by `append_batch`,
    /// even if they haven't been synced yet.
    #[must_use]
    pub fn group_last_index(&self, group_id: GroupId) -> Option<u64> {
        self.group_state
            .get(&group_id)
            .map(|s| s.last_index)
    }

    /// Appends an entry for a group.
    ///
    /// # Panics
    ///
    /// Panics if `TigerStyle` assertions fail:
    /// - Group-local indices must be strictly increasing per group
    ///
    /// Note: Raft term is NOT required to be monotonically non-decreasing.
    /// A new leader may commit `PREVIOUS_TERM` entries (entries proposed by
    /// old leaders that were never committed), which can have lower terms than
    /// the last committed entry. The data WAL stores committed entry data in
    /// Raft index order, so only the auto-counter index is required to increase.
    ///
    /// # Errors
    /// Returns an error if the append fails.
    pub async fn append(
        &mut self,
        group_id: GroupId,
        term: u64,
        index: u64,
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<()> {
        // TigerStyle: Assert per-group indices are strictly increasing.
        // Gaps are allowed to support cases where NOOP entries are skipped.
        if let Some(state) = self.group_state.get(&group_id) {
            assert!(
                index > state.last_index,
                "group {} index must be greater than last: last {}, got {}",
                group_id,
                state.last_index,
                index
            );
            // Note: term is NOT asserted monotone here. A new Raft leader may
            // commit PREVIOUS_TERM entries with lower terms than the last
            // committed entry. See SharedWal doc comment for details.
        } else {
            // First entry for this group - index should be 1 or explicitly set.
            // We allow any starting index to support recovery scenarios.
        }

        let entry = SharedEntry::new(group_id, term, index, raft_index, payload)?;
        self.wal.append(entry.clone()).await?;

        // Update group state.
        self.group_state.insert(
            group_id,
            GroupState {
                last_index: index,
                last_term: term,
            },
        );

        // Update group index.
        self.group_index
            .entry(group_id)
            .or_default()
            .insert(index, entry);

        Ok(())
    }

    /// Appends multiple entries in a single batched I/O operation.
    ///
    /// This is significantly faster than calling `append` in a loop because
    /// all entries are written in a single syscall.
    ///
    /// Each entry must satisfy the same `TigerStyle` invariants as `append`:
    /// - Per-group indices must be strictly increasing (auto-counter order)
    ///
    /// Raft term is NOT required to be monotonically non-decreasing. See
    /// `append` for details on why `PREVIOUS_TERM` commits allow lower terms.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch write fails.
    ///
    /// # Panics
    ///
    /// Panics if any entry violates sequentiality or term constraints.
    pub async fn append_batch(&mut self, entries: &[SharedEntry]) -> WalResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // Validate all entries before writing.
        for entry in entries {
            let group_id = entry.group_id();
            let index = entry.index();
            let term = entry.term();

            if let Some(state) = self.group_state.get(&group_id) {
                assert!(
                    index > state.last_index,
                    "group {} index must be greater than last: last {}, got {}",
                    group_id,
                    state.last_index,
                    index
                );
                // Note: term is NOT asserted monotone here. See append() doc.
            }

            // Update group state for subsequent entries in this batch.
            self.group_state.insert(
                group_id,
                GroupState {
                    last_index: index,
                    last_term: term,
                },
            );
        }

        // Single batched write.
        self.wal.append_batch(entries).await?;

        // Update group index.
        for entry in entries {
            self.group_index
                .entry(entry.group_id())
                .or_default()
                .insert(entry.index(), entry.clone());
        }

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
        self.update_group_durable_indices();
        Ok(())
    }

    /// Marks all written entries as durable without fsync.
    ///
    /// This is used in `ReplicationOnly` mode where durability is provided
    /// by Raft replication rather than local fsync. Once entries are written
    /// to the WAL file (in page cache), they're considered "locally durable"
    /// for the purposes of replication tracking.
    ///
    /// In `Fsync` mode, use `sync()` instead which both fsyncs and updates
    /// the durable indices.
    pub fn update_group_durable_indices(&mut self) {
        for (group_id, state) in &self.group_state {
            self.group_durable
                .insert(*group_id, state.last_index);
        }
    }

    /// Returns the durable index (last synced entry's index).
    ///
    /// Note: For shared WAL, this is the WAL-internal index, not partition-local.
    #[must_use]
    pub const fn durable_index(&self) -> Option<u64> {
        self.wal.durable_index()
    }

    /// Returns the durable index for a specific group.
    ///
    /// Returns `None` if no entries have been synced for this group.
    #[must_use]
    pub fn group_durable_index(&self, group_id: GroupId) -> Option<u64> {
        self.group_durable.get(&group_id).copied()
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
    pub fn truncate_after(&mut self, group_id: GroupId, after_index: u64) {
        if let Some(state) = self.group_state.get(&group_id) {
            assert!(
                after_index <= state.last_index,
                "cannot truncate group {} after index {} (last appended: {})",
                group_id,
                after_index,
                state.last_index
            );
        }
        // else: no entries for this group yet, truncation is a no-op

        // Record truncation point for filtering during reads.
        self.group_truncated_after
            .insert(group_id, after_index);

        // Look up the term at the truncation point before mutating group_state.
        // After truncation the leader may send entries at a term lower than the
        // stale entries that were just removed, so last_term must be reset.
        let term_at_truncation = self
            .group_index
            .get(&group_id)
            .and_then(|btree| btree.get(&after_index))
            .map_or(0, SharedEntry::term);

        // Update group state so next append expects after_index + 1 and the
        // term monotonicity check uses the term at the truncation point.
        if let Some(state) = self.group_state.get_mut(&group_id) {
            state.last_index = after_index;
            // Reset last_term to the term at the truncation point. Keeping the
            // stale (higher) last_term would cause append_batch to panic when the
            // leader sends log entries at a lower term to replace the truncated ones.
            state.last_term = term_at_truncation;
        }

        // Update durable index if it was beyond the truncation point.
        if let Some(durable) = self.group_durable.get_mut(&group_id) {
            if *durable > after_index {
                *durable = after_index;
            }
        }

        // Trim group index: remove entries with index > after_index.
        if let Some(btree) = self.group_index.get_mut(&group_id) {
            // split_off returns entries >= key, so split at after_index + 1.
            let _ = btree.split_off(&(after_index + 1));
        }
    }

    /// Returns the truncation point for a group, if any.
    ///
    /// Returns `None` if no truncation has occurred for this group.
    #[must_use]
    pub fn group_truncated_after(&self, group_id: GroupId) -> Option<u64> {
        self.group_truncated_after.get(&group_id).copied()
    }

    /// Returns an iterator over all entries in the WAL.
    ///
    /// Entries are returned in append order (interleaved across partitions).
    pub fn entries(&self) -> impl Iterator<Item = &SharedEntry> {
        self.wal.entries()
    }

    /// Recovers entries from the WAL, grouped by group.
    ///
    /// This is typically called during node startup to restore group state.
    /// Each group then rebuilds its in-memory Raft log from its entries.
    ///
    /// # Last-Write-Wins Semantics
    ///
    /// If multiple entries exist with the same group and index (due to truncation
    /// and re-append), the last entry in WAL order wins. This ensures that after
    /// truncation and new appends, recovery returns the correct (new) entries.
    ///
    /// # Memory Efficiency
    ///
    /// Sealed segments are loaded from disk one at a time and freed after
    /// their entries are extracted. Peak memory during recovery is bounded
    /// to one segment at a time plus the deduplicated entry set, rather than
    /// all historical segment data simultaneously.
    ///
    /// Also rebuilds internal group state and durable tracking for assertion checking.
    ///
    /// # Errors
    /// Returns an error if any sealed segment file cannot be read or decoded.
    pub async fn recover(&mut self) -> WalResult<HashMap<GroupId, Vec<SharedEntry>>> {
        // Use HashMap to deduplicate: last-write-wins for same (group, index).
        let mut by_group_map: HashMap<GroupId, HashMap<u64, SharedEntry>> = HashMap::new();

        // Process sealed segments one at a time (load → extract → drop).
        // This bounds peak memory to one segment worth of raw data at a time.
        let sealed_ids = self.wal.sealed_segment_ids();
        for segment_id in sealed_ids {
            let segment = self.wal.load_sealed_segment_for_recovery(segment_id).await?;
            for entry in segment.entries() {
                // Last-write-wins: later segments overwrite earlier entries.
                by_group_map
                    .entry(entry.group_id())
                    .or_default()
                    .insert(entry.index(), entry.clone());
            }
            // segment is dropped here, freeing raw payload bytes.
        }

        // Process entries in the active segment (always resident in memory).
        for entry in self.wal.active_entries() {
            by_group_map
                .entry(entry.group_id())
                .or_default()
                .insert(entry.index(), entry.clone());
        }

        // Convert to sorted Vec and update group state.
        let mut by_group: HashMap<GroupId, Vec<SharedEntry>> = HashMap::new();

        for (group_id, index_map) in by_group_map {
            // Sort by index.
            let mut entries: Vec<SharedEntry> = index_map.into_values().collect();
            entries.sort_by_key(SharedEntry::index);

            // Update group state from the last (highest index) entry.
            if let Some(last_entry) = entries.last() {
                self.group_state.insert(
                    group_id,
                    GroupState {
                        last_index: last_entry.index(),
                        last_term: last_entry.term(),
                    },
                );
            }

            by_group.insert(group_id, entries);
        }

        // Recovered entries are durable - update per-group durable indices.
        for (group_id, state) in &self.group_state {
            self.group_durable.insert(*group_id, state.last_index);
        }

        // Populate group index from recovered entries.
        self.group_index.clear();
        for (group_id, entries) in &by_group {
            let btree: BTreeMap<u64, SharedEntry> =
                entries.iter().map(|e| (e.index(), e.clone())).collect();
            self.group_index.insert(*group_id, btree);
        }

        // Clear truncation tracking since we've recovered to a consistent state.
        self.group_truncated_after.clear();

        Ok(by_group)
    }

    /// Recovers state from the WAL using two-pass streaming to minimize peak memory.
    ///
    /// Unlike [`recover`](Self::recover) which accumulates all entries in memory,
    /// this method processes one sealed segment at a time and calls `apply_fn`
    /// for each winning entry without retaining any entry after the call returns.
    ///
    /// # Algorithm
    ///
    /// **Pass 1** (header-only, newest→oldest): Reads each segment file scanning
    /// only 40-byte entry headers. Builds a `winner` map: for each
    /// `(group_id, entry_index)` pair, records which segment is the latest write.
    /// Memory cost: `O(N_unique_entries` × ~32 bytes).
    ///
    /// **Pass 2** (streaming, oldest→newest): Loads one segment at a time,
    /// calls `apply_fn` only for winning entries, records them in `evicted_index`,
    /// then drops the segment. Memory cost: `O(one_segment_at_a_time)`.
    ///
    /// After recovery, `group_index` contains only active-segment entries.
    /// Historical entries are in `evicted_index`; reads fall back to disk via
    /// [`read_entries_range_or_load`](Self::read_entries_range_or_load).
    ///
    /// # Errors
    /// Returns an error if any segment file cannot be read or decoded.
    ///
    /// # Panics
    ///
    /// Panics if a segment header cannot be parsed during the scan pass,
    /// or if entry bytes are malformed (indicates storage corruption).
    pub async fn recover_streaming<F>(
        &mut self,
        apply_fn: &mut F,
    ) -> WalResult<HashMap<GroupId, GroupRecoveryState>>
    where
        F: FnMut(GroupId, &SharedEntry),
    {
        let sealed_ids = self.wal.sealed_segment_ids();

        // Pass 1: Header-only scan, newest-to-oldest.
        // winner[(group_id, entry_index)] = segment_id of the most recent write.
        let mut winner: HashMap<(GroupId, u64), crate::SegmentId> = HashMap::new();
        for &segment_id in sealed_ids.iter().rev() {
            self.wal
                .scan_sealed_segment_entry_headers(segment_id, |header_bytes| {
                    // SharedEntry header layout (40 bytes total):
                    // [crc:4][length:4][group_id:8][term:8][index:8][raft_index:8]
                    debug_assert!(
                        header_bytes.len()
                            >= crate::shared_entry::SHARED_ENTRY_HEADER_SIZE
                    );
                    let group_id_raw = u64::from_le_bytes(
                        header_bytes[8..16].try_into().expect("8 bytes"),
                    );
                    let index = u64::from_le_bytes(
                        header_bytes[24..32].try_into().expect("8 bytes"),
                    );
                    let group_id = GroupId::new(group_id_raw);
                    // First occurrence = newest segment (processing newest first).
                    winner.entry((group_id, index)).or_insert(segment_id);
                })
                .await?;
        }

        // Pass 2: Stream segments oldest-to-newest, applying only winners.
        let mut group_states: HashMap<GroupId, GroupRecoveryState> = HashMap::new();
        for &segment_id in &sealed_ids {
            let segment = self.wal.load_sealed_segment_for_recovery(segment_id).await?;
            for entry in segment.entries() {
                let key = (entry.group_id(), entry.index());
                if winner.get(&key) != Some(&segment_id) {
                    continue; // A later segment supersedes this entry.
                }
                // Winning entry: apply via callback (no clone stored).
                apply_fn(entry.group_id(), entry);

                // Record in evicted_index so reads fall back to disk.
                // Store raft_index alongside segment_id for retention checks.
                self.evicted_index
                    .entry(entry.group_id())
                    .or_default()
                    .insert(entry.index(), (segment_id, entry.raft_index()));

                // Track per-group recovery metadata.
                let state = group_states.entry(entry.group_id()).or_default();
                if entry.index() >= state.last_wal_index {
                    state.last_wal_index = entry.index();
                    state.last_term = entry.term();
                    state.last_raft_index = entry.raft_index();
                }
            }
            // segment is dropped here: payload Bytes lose their Arc ref
            // to the loaded buffer, freeing the segment's raw data.
        }

        // Active segment: entries are already in memory; add to group_index.
        for entry in self.wal.active_entries() {
            apply_fn(entry.group_id(), entry);

            self.group_index
                .entry(entry.group_id())
                .or_default()
                .insert(entry.index(), entry.clone());

            let state = group_states.entry(entry.group_id()).or_default();
            if entry.index() >= state.last_wal_index {
                state.last_wal_index = entry.index();
                state.last_term = entry.term();
                state.last_raft_index = entry.raft_index();
            }
        }

        // Update group_state and group_durable from recovered metadata.
        for (group_id, state) in &group_states {
            self.group_state.insert(
                *group_id,
                GroupState {
                    last_index: state.last_wal_index,
                    last_term: state.last_term,
                },
            );
            self.group_durable.insert(*group_id, state.last_wal_index);
        }

        // Clear truncation tracking — recovery represents the consistent state.
        self.group_truncated_after.clear();

        Ok(group_states)
    }

    /// Returns entries for a specific group.
    ///
    /// Uses the per-group `BTreeMap` index, which returns entries in sorted
    /// order. The index is kept in sync with truncation, so no filtering needed.
    #[must_use]
    pub fn entries_for_group(&self, group_id: GroupId) -> Vec<SharedEntry> {
        self.group_index
            .get(&group_id)
            .map(|btree| btree.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Reads a specific entry by group ID and index.
    ///
    /// Returns `None` if no entry exists for the given group and index,
    /// or if the index is beyond the current valid range (after truncation).
    ///
    /// Uses the per-group `BTreeMap` index for O(log n) lookup.
    #[must_use]
    pub fn read(&self, group_id: GroupId, index: u64) -> Option<SharedEntry> {
        // Check if index is beyond current valid range.
        if let Some(state) = self.group_state.get(&group_id) {
            if index > state.last_index {
                return None;
            }
        }

        self.group_index
            .get(&group_id)
            .and_then(|btree| btree.get(&index).cloned())
    }

    /// Reads a range of entries for a group, bounded by byte size.
    ///
    /// Returns entries from `start_index` to `end_index` (inclusive), stopping
    /// when adding the next entry would exceed `max_bytes`. Always includes at
    /// least one entry if any exist in the range.
    #[must_use]
    pub fn read_entries_range(
        &self,
        group_id: GroupId,
        start_index: u64,
        end_index: u64,
        max_bytes: u64,
    ) -> Vec<SharedEntry> {
        let Some(btree) = self.group_index.get(&group_id) else {
            return Vec::new();
        };

        let mut result = Vec::new();
        let mut total_bytes = 0u64;
        for (_index, entry) in btree.range(start_index..=end_index) {
            let entry_size = entry.payload.len() as u64;
            // Always include at least one entry.
            if !result.is_empty() && total_bytes + entry_size > max_bytes {
                break;
            }
            total_bytes += entry_size;
            result.push(entry.clone());
        }
        result
    }

    /// Returns the set of group IDs that have entries in this WAL.
    pub fn group_ids(&self) -> impl Iterator<Item = GroupId> + '_ {
        self.group_state.keys().copied()
    }

    /// Returns the list of sealed segment IDs.
    ///
    /// Sealed segments are immutable and can be safely uploaded to tiering storage.
    #[must_use]
    pub fn sealed_segment_ids(&self) -> Vec<crate::SegmentId> {
        self.wal.sealed_segment_ids()
    }

    /// Reads the raw bytes of a sealed segment.
    ///
    /// This is used by the tiering manager to upload segments to S3.
    /// Reads from memory if resident, or from disk if evicted.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment does not exist or cannot be read.
    pub async fn read_segment_bytes(
        &self,
        segment_id: crate::SegmentId,
    ) -> WalResult<Bytes> {
        self.wal.read_segment_bytes(segment_id).await
    }

    /// Returns information about a sealed segment.
    ///
    /// Returns `None` if the segment doesn't exist or is the active segment.
    #[must_use]
    pub fn segment_info(&self, segment_id: crate::SegmentId) -> Option<crate::wal::SegmentInfo> {
        self.wal.segment_info(segment_id)
    }

    // -------------------------------------------------------------------------
    // Segment Eviction
    // -------------------------------------------------------------------------

    /// Evicts a sealed segment from memory.
    ///
    /// This performs dual eviction:
    /// 1. Removes `SharedEntry` values from `group_index` (frees payload data)
    /// 2. Records `(group_id, index) → segment_id` in `evicted_index`
    /// 3. Drops the in-memory `Segment` in the underlying WAL
    ///
    /// After eviction, reads fall back to `read_or_load()` which reads from disk.
    ///
    /// Returns `true` if the segment was evicted.
    pub fn evict_sealed_segment(&mut self, segment_id: crate::SegmentId) -> bool {
        // Step 1: Read entries from the WAL segment before eviction.
        // Collect (group_id, wal_counter, raft_index) for evicted_index.
        let entry_mappings: Vec<(GroupId, u64, u64)> =
            if let Some(entries) = self.wal.sealed_segment_entries(segment_id) {
                entries
                    .map(|e| (e.group_id(), e.index(), e.raft_index()))
                    .collect()
            } else {
                return false; // Segment not found or already evicted.
            };

        // Step 2: Move entries from group_index to evicted_index.
        // Store raft_index alongside segment_id so the retention safety check
        // can compare against Raft match indices without a disk read.
        for &(group_id, index, raft_index) in &entry_mappings {
            // Remove from group_index.
            if let Some(btree) = self.group_index.get_mut(&group_id) {
                btree.remove(&index);
            }
            // Add to evicted_index with (segment_id, raft_index).
            self.evicted_index
                .entry(group_id)
                .or_default()
                .insert(index, (segment_id, raft_index));
        }

        // Step 3: Evict the underlying WAL segment.
        self.wal.evict_sealed_segment(segment_id)
    }

    /// Evicts all sealed segments from memory.
    ///
    /// Returns the number of segments evicted.
    pub fn evict_all_sealed_segments(&mut self) -> u32 {
        let segment_ids = self.wal.sealed_segment_ids();
        let mut count = 0u32;
        for segment_id in segment_ids {
            if self.evict_sealed_segment(segment_id) {
                count += 1;
            }
        }
        count
    }

    // -------------------------------------------------------------------------
    // Segment Retention (Deletion)
    // -------------------------------------------------------------------------

    /// Returns metadata for all sealed segments, ordered by segment ID.
    ///
    /// Used by the retention system to enumerate deletion candidates.
    #[must_use]
    pub fn sealed_segment_infos(&self) -> Vec<crate::wal::SegmentInfo> {
        self.wal.sealed_segment_infos()
    }

    /// Returns `(group_id, max_raft_index, max_wal_counter)` for all groups
    /// with entries in a segment.
    ///
    /// Scans both `group_index` (in-memory) and `evicted_index` to find which
    /// groups have entries in the given segment and their maximum indices.
    ///
    /// - `max_raft_index`: used by the retention system to verify all entries
    ///   have been replicated (compare against Raft `match_index`).
    /// - `max_wal_counter`: used to advance `BlobIndex` floor after deletion.
    ///
    /// Using `max_raft_index` for the replication check is essential because
    /// WAL auto-counters and Raft log indices are different index spaces.
    /// Comparing WAL counter against Raft `match_index` can allow premature
    /// segment deletion when Raft no-ops advance `raft_index` beyond the WAL
    /// counter, making it appear safe to delete segments that followers still
    /// need for catch-up.
    #[must_use]
    pub fn groups_in_segment(
        &self,
        segment_id: crate::SegmentId,
    ) -> Vec<(GroupId, u64, u64)> {
        // Track (max_raft_index, max_wal_counter) per group.
        let mut group_max: HashMap<GroupId, (u64, u64)> = HashMap::new();

        if self.wal.segment_info(segment_id).is_none() {
            return Vec::new();
        }

        // In-memory path: entries still resident in sealed_segments.
        // Use the actual raft_index() for the replication safety check.
        if let Some(entries) = self.wal.sealed_segment_entries(segment_id) {
            for entry in entries {
                let gid = entry.group_id();
                let wal_counter = entry.index();
                let raft_index = entry.raft_index();
                let cur = group_max.entry(gid).or_insert((0, 0));
                if wal_counter > cur.1 {
                    cur.0 = raft_index;
                    cur.1 = wal_counter;
                }
            }
        }

        // Evicted path: entries moved to evicted_index after memory eviction.
        // Stored raft_index avoids a disk read just for the replication check.
        for (gid, btree) in &self.evicted_index {
            for (&wal_counter, &(sid, raft_index)) in btree {
                if sid != segment_id {
                    continue;
                }
                let cur = group_max.entry(*gid).or_insert((0, 0));
                if wal_counter > cur.1 {
                    cur.0 = raft_index;
                    cur.1 = wal_counter;
                }
            }
        }

        group_max
            .into_iter()
            .map(|(gid, (max_raft, max_wal))| (gid, max_raft, max_wal))
            .collect()
    }

    /// Deletes a sealed segment from memory and disk.
    ///
    /// Also cleans up `group_index` and `evicted_index` entries that
    /// reference the deleted segment. Updates `group_state` for groups
    /// whose entries are entirely within deleted segments.
    ///
    /// # Errors
    /// Returns an error if the disk file cannot be removed.
    pub async fn delete_sealed_segment(
        &mut self,
        segment_id: crate::SegmentId,
    ) -> WalResult<()> {
        // Clean up evicted_index entries referencing this segment.
        for btree in self.evicted_index.values_mut() {
            btree.retain(|_, (sid, _raft_idx)| *sid != segment_id);
        }
        // Remove empty group entries.
        self.evicted_index.retain(|_, btree| !btree.is_empty());

        // Clean up group_index entries that are in this segment.
        // We do this by checking if entries were in this segment (via
        // the WAL's sealed_segment_entries, if still resident).
        if let Some(entries) = self.wal.sealed_segment_entries(segment_id) {
            let pairs: Vec<(GroupId, u64)> = entries
                .map(|e| (e.group_id(), e.index()))
                .collect();
            for (gid, idx) in pairs {
                if let Some(btree) = self.group_index.get_mut(&gid) {
                    btree.remove(&idx);
                }
            }
        }

        // Delete from the underlying WAL.
        self.wal.delete_sealed_segment(segment_id).await
    }

    /// Reads an entry, falling back to disk if evicted.
    ///
    /// Checks `partition_index` first (O(log n) in-memory). On miss, checks
    /// `evicted_index` to find the segment file, reads it from disk, decodes,
    /// and returns the entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the disk read fails.
    pub async fn read_or_load(
        &mut self,
        group_id: GroupId,
        index: u64,
    ) -> WalResult<Option<SharedEntry>> {
        // Fast path: check group_index (in-memory) first. Entries are
        // pre-registered here on append, before flush_loop writes them
        // to disk. Checking this before group_state ensures buffered
        // entries are readable immediately.
        if let Some(entry) = self
            .group_index
            .get(&group_id)
            .and_then(|btree| btree.get(&index).cloned())
        {
            return Ok(Some(entry));
        }

        // Check if index is beyond current valid range (flushed entries).
        if let Some(state) = self.group_state.get(&group_id) {
            if index > state.last_index {
                return Ok(None);
            }
        }

        // Slow path: check evicted_index → scan segment from disk.
        // SharedEntry indices are group-local, not WAL-global, so we
        // can't use Wal::read_entry_from_disk (which does positional
        // lookup). Instead, decode the segment and scan for the entry
        // matching both group_id and index.
        if let Some((segment_id, _raft_index)) = self
            .evicted_index
            .get(&group_id)
            .and_then(|btree| btree.get(&index).copied())
        {
            if let Some(entry) = self
                .wal
                .scan_segment_for_entry(segment_id, |e| {
                    e.group_id() == group_id && e.index() == index
                })
                .await?
            {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    /// Reads a range of entries, falling back to disk for evicted entries.
    ///
    /// Like `read_entries_range()` but handles evicted segments transparently.
    ///
    /// # Errors
    ///
    /// Returns an error if a disk read fails.
    pub async fn read_entries_range_or_load(
        &mut self,
        group_id: GroupId,
        start_index: u64,
        end_index: u64,
        max_bytes: u64,
    ) -> WalResult<Vec<SharedEntry>> {
        let mut result = Vec::new();
        let mut total_bytes = 0u64;

        for index in start_index..=end_index {
            if let Some(entry) = self.read_or_load(group_id, index).await? {
                let entry_size = entry.payload.len() as u64;
                // Always include at least one entry.
                if !result.is_empty() && total_bytes + entry_size > max_bytes {
                    break;
                }
                total_bytes += entry_size;
                result.push(entry);
            }
            // Skip missing entries (gaps from NOOP skipping).
        }

        Ok(result)
    }

    /// Reads entries for a group with `raft_index` in the given range.
    ///
    /// Unlike `read_entries_range` which iterates by WAL auto-counter, this
    /// filters entries by their `raft_index` field. Entries within a group are
    /// stored in WAL-counter order, which also reflects `raft_index` order, so
    /// the scan terminates early once `raft_index > end_raft_index`.
    ///
    /// Only checks in-memory entries. For evicted-segment fallback, use
    /// `read_entries_by_raft_index_or_load`.
    #[must_use]
    pub fn read_entries_by_raft_index(
        &self,
        group_id: GroupId,
        start_raft_index: u64,
        end_raft_index: u64,
        max_bytes: u64,
    ) -> Vec<SharedEntry> {
        let Some(btree) = self.group_index.get(&group_id) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut total_bytes = 0u64;
        // Entries are in WAL-counter order. Since entries for a group are
        // always appended in raft_index order, raft_indices are also
        // monotonically increasing. Skip entries below start and stop at
        // the first entry above end.
        for entry in btree.values() {
            let ri = entry.raft_index();
            if ri < start_raft_index {
                continue;
            }
            if ri > end_raft_index {
                break;
            }
            let entry_size = entry.payload.len() as u64;
            // Always include at least one entry.
            if !result.is_empty() && total_bytes + entry_size > max_bytes {
                break;
            }
            total_bytes += entry_size;
            result.push(entry.clone());
        }
        result
    }

    /// Reads entries by `raft_index` range, falling back to disk for evicted
    /// segments.
    ///
    /// # Errors
    ///
    /// Returns an error if a disk read fails.
    pub async fn read_entries_by_raft_index_or_load(
        &mut self,
        group_id: GroupId,
        start_raft_index: u64,
        end_raft_index: u64,
        max_bytes: u64,
    ) -> WalResult<Vec<SharedEntry>> {
        let mut result = Vec::new();
        let mut total_bytes = 0u64;

        // Collect (wal_counter, raft_index) for this group in WAL-counter order,
        // which also reflects raft_index order. Stored raft_index avoids loading
        // entries from disk just to filter by raft_index range.
        let evicted: Vec<(u64, u64)> = self
            .evicted_index
            .get(&group_id)
            .map(|btree| {
                btree
                    .iter()
                    .map(|(&wc, &(_, ri))| (wc, ri))
                    .collect()
            })
            .unwrap_or_default();

        for (wal_counter, raft_index) in evicted {
            if raft_index < start_raft_index {
                continue;
            }
            if raft_index > end_raft_index {
                // Entries are in raft_index order; no more in range.
                break;
            }
            let Some(entry) = self.read_or_load(group_id, wal_counter).await? else {
                continue;
            };
            let entry_size = entry.payload.len() as u64;
            // Always include at least one entry.
            if !result.is_empty() && total_bytes + entry_size > max_bytes {
                break;
            }
            total_bytes += entry_size;
            result.push(entry);
        }

        Ok(result)
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
    /// Write durability mode.
    ///
    /// - `Fsync`: Wait for fsync on each flush (safe for single-node, slower)
    /// - `ReplicationOnly`: Skip fsync, rely on Raft replication (fast, requires multi-node)
    ///
    /// Default is `ReplicationOnly` for maximum throughput in replicated deployments.
    pub durability: WriteDurability,
}

impl CoordinatorConfig {
    /// Creates a new coordinator configuration with defaults.
    ///
    /// Default durability is `ReplicationOnly` for maximum throughput.
    /// Use `with_durability(WriteDurability::Fsync)` for single-node safety.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            wal_config: SharedWalConfig::new(dir),
            flush_interval: Duration::from_millis(1),
            max_buffer_entries: 1000,
            max_buffer_bytes: 16 * 1024 * 1024,     // 16 MB
            durability: WriteDurability::default(), // ReplicationOnly for throughput
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

    /// Sets the write durability mode.
    ///
    /// - `WriteDurability::Fsync`: Wait for fsync on each flush (safe for single-node)
    /// - `WriteDurability::ReplicationOnly`: Skip fsync, rely on Raft replication (fast)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // For single-node deployments (safe but slower):
    /// let config = CoordinatorConfig::new(dir)
    ///     .with_durability(WriteDurability::Fsync);
    ///
    /// // For multi-node deployments (fast, relies on replication):
    /// let config = CoordinatorConfig::new(dir)
    ///     .with_durability(WriteDurability::ReplicationOnly);
    /// ```
    #[must_use]
    pub const fn with_durability(mut self, durability: WriteDurability) -> Self {
        self.durability = durability;
        self
    }
}

/// Acknowledgment that an entry is durable.
#[derive(Debug, Clone, Copy)]
pub struct DurableAck {
    /// The group ID.
    pub group_id: GroupId,
    /// The group's local index that was made durable.
    pub index: u64,
    /// The term of the entry.
    pub term: u64,
    /// The actual Raft log index.
    pub raft_index: u64,
}

/// A group's handle to a coordinated shared WAL.
///
/// This handle is `Clone + Send + Sync` and can be used from multiple tasks.
/// Writes are buffered internally and flushed by a background task.
#[derive(Clone)]
pub struct SharedWalHandle<S: Storage> {
    group_id: GroupId,
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
    pub async fn append(
        &self,
        term: u64,
        index: u64,
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<DurableAck> {
        let rx = self.append_async(term, index, raft_index, payload).await?;
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
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<oneshot::Receiver<WalResult<DurableAck>>> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(crate::WalError::Shutdown);
        }

        let entry = SharedEntry::new(self.group_id, term, index, raft_index, payload.clone())?;
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

    /// Appends an entry with auto-assigned index.
    ///
    /// The index is assigned atomically while holding the buffer lock, eliminating
    /// TOCTOU races between computing the next index and adding to the buffer.
    ///
    /// Returns when the entry is durable (fsync'd to disk).
    ///
    /// # Errors
    ///
    /// Returns an error if the append or sync fails.
    pub async fn append_auto(
        &self,
        term: u64,
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<DurableAck> {
        let (_index, rx) = self.append_auto_async(term, raft_index, payload).await?;
        rx.await.map_err(|_| crate::WalError::Shutdown)?
    }

    /// Appends an entry with auto-assigned index without waiting for durability.
    ///
    /// The index is assigned atomically while holding the buffer lock, eliminating
    /// TOCTOU races. This is the recommended method for new code.
    ///
    /// Returns a tuple of (`assigned_index`, `receiver`). The receiver will be notified
    /// when the entry is durable. The index is available immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator is shut down.
    pub async fn append_auto_async(
        &self,
        term: u64,
        raft_index: u64,
        payload: Bytes,
    ) -> WalResult<(u64, oneshot::Receiver<WalResult<DurableAck>>)> {
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(crate::WalError::Shutdown);
        }

        let (tx, rx) = oneshot::channel();

        // Add to buffer with atomically assigned index.
        let (next_index, entry_for_index) = {
            let mut buffer = self.inner.buffer.lock().await;

            // Determine the next index atomically while holding buffer lock.
            let last_index = if let Some(idx) = buffer.last_index_for(self.group_id) {
                idx
            } else {
                // First append for this partition - check WAL state for initialization.
                // This only happens once per partition per coordinator lifetime.
                let wal = self.inner.wal.lock().await;
                let wal_last = wal.group_last_index(self.group_id).unwrap_or(0);
                drop(wal);
                // Initialize buffer tracking.
                buffer.update_last_index(self.group_id, wal_last);
                wal_last
            };

            let next_index = last_index + 1;

            // Diagnostic: detect unexpected jumps in per-group counter.
            if last_index > 0 {
                let from_buffer = buffer.last_index_for(self.group_id).is_some();
                if !from_buffer {
                    tracing::warn!(
                        group_id = %self.group_id,
                        last_index,
                        next_index,
                        "append_auto_async: initialized from WAL state (not buffer)"
                    );
                }
            }

            // Create entry with assigned index.
            let entry = SharedEntry::new(
                self.group_id,
                term,
                next_index,
                raft_index,
                payload.clone(),
            )?;

            // Clone for group_index pre-registration (O(1), Bytes refcount).
            let entry_for_index = entry.clone();

            // Update buffer tracking BEFORE adding entry (atomic with lock).
            buffer.update_last_index(self.group_id, next_index);

            let pending = PendingWrite {
                entry,
                durable_tx: tx,
            };

            let entry_bytes = payload.len() + crate::SHARED_ENTRY_HEADER_SIZE;
            buffer.entries.push_back(pending);
            buffer.bytes += entry_bytes;

            // Check if we should trigger immediate flush.
            let should_flush = buffer.entries.len() >= self.inner.config.max_buffer_entries
                || buffer.bytes >= self.inner.config.max_buffer_bytes;

            // Drop buffer lock before notifying to avoid holding lock during notification.
            drop(buffer);

            if should_flush {
                self.inner.flush_notify.notify_one();
            }

            (next_index, entry_for_index)
        };

        // Pre-register entry in group_index so reads find it immediately,
        // before flush_loop writes it to disk. Without this, read_or_load()
        // returns None for buffered-but-unflushed entries, causing
        // "BlobIndex references missing WAL entry" on the read path.
        {
            let mut wal = self.inner.wal.lock().await;
            wal.group_index
                .entry(self.group_id)
                .or_default()
                .insert(next_index, entry_for_index);
        }

        Ok((next_index, rx))
    }

    /// Appends an entry with auto-assigned index, returning immediately.
    ///
    /// This method does NOT wait for durability. In a replicated system,
    /// durability is provided by Raft replication across nodes, so waiting
    /// for local fsync is unnecessary and hurts throughput.
    ///
    /// The entry is buffered and will be fsynced within `flush_interval` (typically 1ms).
    /// If the node crashes before fsync, recovery rebuilds state from Raft peers.
    ///
    /// Returns the assigned index immediately.
    ///
    /// # Errors
    ///
    /// Returns an error if the coordinator is shut down.
    pub async fn append_nowait(&self, term: u64, raft_index: u64, payload: Bytes) -> WalResult<u64> {
        let (index, _rx) = self.append_auto_async(term, raft_index, payload).await?;
        Ok(index)
    }

    /// Reads a specific entry by Raft index for this partition.
    ///
    /// Uses the per-partition `BTreeMap` index for O(log n) lookup.
    /// Falls back to disk read if the entry's segment has been evicted.
    /// Returns `None` if no entry exists at the given index.
    ///
    /// # Errors
    ///
    /// Returns an error if the disk fallback read fails (I/O or decode).
    pub async fn read_entry(&self, index: u64) -> Option<SharedEntry> {
        let mut wal = self.inner.wal.lock().await;
        // Use read_or_load for transparent disk fallback.
        match wal.read_or_load(self.group_id, index).await {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    group_id = %self.group_id,
                    index,
                    error = %e,
                    "Failed to read entry from disk"
                );
                None
            }
        }
    }

    /// Reads a range of entries for this partition, bounded by byte size.
    ///
    /// Returns entries from `start_index` to `end_index` (inclusive), stopping
    /// when adding the next entry would exceed `max_bytes`. Always includes at
    /// least one entry if any exist in the range.
    ///
    /// Falls back to disk reads for evicted segments.
    pub async fn read_entries_range(
        &self,
        start_index: u64,
        end_index: u64,
        max_bytes: u64,
    ) -> Vec<SharedEntry> {
        let mut wal = self.inner.wal.lock().await;
        // Try in-memory first; if all entries are resident this is fast.
        let result = wal.read_entries_range(
            self.group_id,
            start_index,
            end_index,
            max_bytes,
        );
        if !result.is_empty() {
            return result;
        }

        // In-memory returned nothing — try disk fallback.
        match wal
            .read_entries_range_or_load(
                self.group_id,
                start_index,
                end_index,
                max_bytes,
            )
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    group_id = %self.group_id,
                    start_index,
                    end_index,
                    error = %e,
                    "Failed to read entry range from disk"
                );
                Vec::new()
            }
        }
    }

    /// Reads a single entry for this partition by Raft log index.
    ///
    /// Unlike `read_entry` which looks up by WAL auto-counter, this searches
    /// by the `raft_index` field stored in each entry header.
    ///
    /// Falls back to disk reads for evicted segments.
    pub async fn read_entry_by_raft_index(&self, raft_index: u64) -> Option<SharedEntry> {
        let mut wal = self.inner.wal.lock().await;
        // Fast path: scan in-memory entries for this group.
        let entry = wal
            .group_index
            .get(&self.group_id)
            .and_then(|btree| btree.values().find(|e| e.raft_index() == raft_index))
            .cloned();
        if entry.is_some() {
            return entry;
        }
        // In-memory miss — scan evicted WAL counters in order, using stored
        // raft_index to skip and early-terminate without disk reads.
        let evicted: Vec<(u64, u64)> = wal
            .evicted_index
            .get(&self.group_id)
            .map(|btree| {
                btree
                    .iter()
                    .map(|(&wc, &(_, ri))| (wc, ri))
                    .collect()
            })
            .unwrap_or_default();
        for (wal_counter, stored_raft_index) in evicted {
            if stored_raft_index < raft_index {
                continue;
            }
            if stored_raft_index > raft_index {
                break; // Past target; entry not present.
            }
            // stored_raft_index == raft_index: load from disk.
            match wal.read_or_load(self.group_id, wal_counter).await {
                Ok(Some(e)) => return Some(e),
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        group_id = %self.group_id,
                        raft_index,
                        error = %err,
                        "Failed to read entry by raft_index from disk"
                    );
                    break;
                }
            }
        }
        None
    }

    /// Reads entries for this partition with `raft_index` in the given range.
    ///
    /// Unlike `read_entries_range` which iterates by WAL auto-counter, this
    /// returns entries whose `raft_index` falls within `[start_raft_index,
    /// end_raft_index]`. Always includes at least one entry if any exist.
    ///
    /// Falls back to disk reads for evicted segments.
    pub async fn read_entries_by_raft_index(
        &self,
        start_raft_index: u64,
        end_raft_index: u64,
        max_bytes: u64,
    ) -> Vec<SharedEntry> {
        let mut wal = self.inner.wal.lock().await;
        let result = wal.read_entries_by_raft_index(
            self.group_id,
            start_raft_index,
            end_raft_index,
            max_bytes,
        );
        if !result.is_empty() {
            return result;
        }
        // In-memory returned nothing — try disk fallback.
        match wal
            .read_entries_by_raft_index_or_load(
                self.group_id,
                start_raft_index,
                end_raft_index,
                max_bytes,
            )
            .await
        {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    group_id = %self.group_id,
                    start_raft_index,
                    end_raft_index,
                    error = %e,
                    "Failed to read entries by raft_index from disk"
                );
                Vec::new()
            }
        }
    }

    /// Returns the group ID for this handle.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Returns the list of sealed segment IDs.
    ///
    /// Sealed segments are immutable and can be safely uploaded to tiering storage.
    /// This method is provided for compatibility with tiering infrastructure that
    /// needs to query segment state.
    pub async fn sealed_segment_ids(&self) -> Vec<crate::SegmentId> {
        let wal = self.inner.wal.lock().await;
        wal.sealed_segment_ids()
    }

    /// Reads the raw bytes of a sealed segment.
    ///
    /// This is used by the tiering manager to upload segments to S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment does not exist or cannot be read.
    pub async fn read_segment_bytes(&self, segment_id: crate::SegmentId) -> WalResult<Bytes> {
        let wal = self.inner.wal.lock().await;
        wal.read_segment_bytes(segment_id).await
    }

    /// Returns information about a sealed segment.
    ///
    /// Returns `None` if the segment doesn't exist or is the active segment.
    pub async fn segment_info(
        &self,
        segment_id: crate::SegmentId,
    ) -> Option<crate::wal::SegmentInfo> {
        let wal = self.inner.wal.lock().await;
        wal.segment_info(segment_id)
    }

    /// Returns the last known index for this partition.
    ///
    /// This checks all sources of index information:
    /// 1. The WAL's `partition_state` (updated during batch processing)
    /// 2. The durable index (entries that have been synced to disk)
    /// 3. The buffer (entries that are pending flush)
    ///
    /// Returns `None` if no entries have been written for this partition.
    /// This is useful for initializing a `DurablePartition` to continue
    /// from the correct index after recovery.
    pub async fn last_index(&self) -> Option<u64> {
        // Check WAL's group_state (source of truth for in-progress batches).
        let wal_state_index = {
            let wal = self.inner.wal.lock().await;
            wal.group_last_index(self.group_id)
        };

        // Check durable index (synced entries).
        let durable_index = {
            let last_index = self.inner.group_last_index.read().await;
            last_index.get(&self.group_id).copied()
        };

        // Check buffer for pending entries.
        let buffer_index = {
            let buffer = self.inner.buffer.lock().await;
            buffer
                .entries
                .iter()
                .filter(|pw| pw.entry.group_id() == self.group_id)
                .map(|pw| pw.entry.index())
                .max()
        };

        // Return the maximum of all sources.
        [wal_state_index, durable_index, buffer_index]
            .into_iter()
            .flatten()
            .max()
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
    /// Per-group last buffered index (for tracking expected next index).
    /// This is updated atomically with adding entries to prevent TOCTOU races.
    group_last_buffered: HashMap<GroupId, u64>,
}

impl WriteBuffer {
    #[allow(clippy::missing_const_for_fn)] // VecDeque::new() is not const
    fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            bytes: 0,
            group_last_buffered: HashMap::new(),
        }
    }

    fn drain(&mut self) -> Vec<PendingWrite> {
        self.bytes = 0;
        // Don't clear group_last_buffered - it tracks cumulative state.
        std::mem::take(&mut self.entries).into()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the last buffered index for a group.
    fn last_index_for(&self, group_id: GroupId) -> Option<u64> {
        self.group_last_buffered.get(&group_id).copied()
    }

    /// Updates the last buffered index for a group.
    fn update_last_index(&mut self, group_id: GroupId, index: u64) {
        self.group_last_buffered.insert(group_id, index);
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
    /// Per-group last index (for assertion without locking WAL).
    group_last_index: RwLock<HashMap<GroupId, u64>>,
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
/// use helix_core::GroupId;
/// use bytes::Bytes;
///
/// let config = CoordinatorConfig::new("/tmp/shared-wal");
/// let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config).await?;
///
/// // Get handles for groups.
/// let h1 = coordinator.handle(GroupId::new(1));
/// let h2 = coordinator.handle(GroupId::new(2));
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
        // Propagate durability mode to the WAL layer so it knows
        // whether to fsync on segment rotation.
        let wal_config = config
            .wal_config
            .clone()
            .with_sync_on_rotation(config.durability.requires_fsync());
        let wal = SharedWal::open(storage, wal_config).await?;

        let inner = Arc::new(CoordinatorInner {
            wal: Mutex::new(wal),
            buffer: Mutex::new(WriteBuffer::new()),
            flush_notify: Notify::new(),
            config,
            shutdown: AtomicBool::new(false),
            group_last_index: RwLock::new(HashMap::new()),
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

    /// Gets a handle for a group.
    ///
    /// Each group should call this once and clone the handle as needed.
    /// The handle is `Clone + Send + Sync`.
    #[must_use]
    pub fn handle(&self, group_id: GroupId) -> SharedWalHandle<S> {
        SharedWalHandle {
            group_id,
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
    pub async fn recover(&self) -> WalResult<HashMap<GroupId, Vec<SharedEntry>>> {
        let result = {
            let mut wal = self.inner.wal.lock().await;
            wal.recover().await?
        };

        // Update group_last_index from recovered state.
        {
            let mut last_index = self.inner.group_last_index.write().await;
            for (group_id, entries) in &result {
                if let Some(last) = entries.last() {
                    last_index.insert(*group_id, last.index());
                }
            }
        }

        Ok(result)
    }

    /// Recovers entries using streaming to bound peak memory usage.
    ///
    /// Calls `apply_fn` for each winning entry (last-write-wins across segments)
    /// without accumulating entries in memory. See [`SharedWal::recover_streaming`]
    /// for the full algorithm description.
    ///
    /// # Errors
    /// Returns an error if recovery fails.
    pub async fn recover_streaming<F>(
        &self,
        apply_fn: &mut F,
    ) -> WalResult<HashMap<GroupId, GroupRecoveryState>>
    where
        F: FnMut(GroupId, &SharedEntry),
    {
        let result = {
            let mut wal = self.inner.wal.lock().await;
            wal.recover_streaming(apply_fn).await?
        };

        // Update group_last_index from recovered state.
        {
            let mut last_index = self.inner.group_last_index.write().await;
            for (group_id, state) in &result {
                last_index.insert(*group_id, state.last_wal_index);
            }
        }

        Ok(result)
    }

    /// Returns the durable index for a specific group.
    ///
    /// Returns `None` if no entries have been synced for this group.
    pub async fn group_durable_index(&self, group_id: GroupId) -> Option<u64> {
        let wal = self.inner.wal.lock().await;
        wal.group_durable_index(group_id)
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

    /// Returns the list of sealed segment IDs.
    ///
    /// Sealed segments are immutable and can be safely uploaded to tiering storage.
    pub async fn sealed_segment_ids(&self) -> Vec<crate::SegmentId> {
        let wal = self.inner.wal.lock().await;
        wal.sealed_segment_ids()
    }

    /// Reads the raw bytes of a sealed segment.
    ///
    /// This is used by the tiering manager to upload segments to S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment does not exist or cannot be read.
    pub async fn read_segment_bytes(&self, segment_id: crate::SegmentId) -> WalResult<Bytes> {
        let wal = self.inner.wal.lock().await;
        wal.read_segment_bytes(segment_id).await
    }

    /// Returns information about a sealed segment.
    ///
    /// Returns `None` if the segment doesn't exist or is the active segment.
    pub async fn segment_info(
        &self,
        segment_id: crate::SegmentId,
    ) -> Option<crate::wal::SegmentInfo> {
        let wal = self.inner.wal.lock().await;
        wal.segment_info(segment_id)
    }

    // -------------------------------------------------------------------------
    // Segment Retention (Deletion)
    // -------------------------------------------------------------------------

    /// Returns metadata for all sealed segments.
    pub async fn sealed_segment_infos(&self) -> Vec<crate::wal::SegmentInfo> {
        let wal = self.inner.wal.lock().await;
        wal.sealed_segment_infos()
    }

    /// Returns `(group_id, max_raft_index, max_wal_counter)` for all groups
    /// with entries in a segment.
    pub async fn groups_in_segment(
        &self,
        segment_id: crate::SegmentId,
    ) -> Vec<(GroupId, u64, u64)> {
        let wal = self.inner.wal.lock().await;
        wal.groups_in_segment(segment_id)
    }

    /// Deletes a sealed segment from memory and disk.
    ///
    /// # Errors
    /// Returns an error if the disk file cannot be removed.
    pub async fn delete_sealed_segment(
        &self,
        segment_id: crate::SegmentId,
    ) -> WalResult<()> {
        let mut wal = self.inner.wal.lock().await;
        wal.delete_sealed_segment(segment_id).await
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

        // Extract entries and response channels.
        let entries: Vec<SharedEntry> = pending.iter().map(|pw| pw.entry.clone()).collect();
        let channels: Vec<(oneshot::Sender<WalResult<DurableAck>>, DurableAck)> = pending
            .into_iter()
            .map(|pw| {
                let ack = DurableAck {
                    group_id: pw.entry.group_id(),
                    index: pw.entry.index(),
                    term: pw.entry.term(),
                    raft_index: pw.entry.raft_index(),
                };
                (pw.durable_tx, ack)
            })
            .collect();

        // Single batched write for all entries.
        let mut wal = inner.wal.lock().await;
        let write_result = wal.append_batch(&entries).await;

        if let Err(e) = write_result {
            // Batch write failed - notify all waiters with error.
            for (tx, _) in channels {
                let _ = tx.send(Err(e.clone()));
            }
            if shutting_down {
                break;
            }
            continue;
        }

        // Conditionally fsync based on durability mode.
        //
        // - Fsync mode: Wait for fsync to ensure data is on disk (safe, slow)
        // - ReplicationOnly mode: Skip fsync, rely on Raft replication for durability (fast)
        //
        // In ReplicationOnly mode, data goes to the OS page cache and will be
        // flushed to disk by the OS in the background. If the node crashes before
        // the OS flush, data is recovered from Raft peers.
        let sync_result = if inner.config.durability.requires_fsync() {
            // Fsync mode: sync to disk (also updates partition_durable indices).
            wal.sync().await
        } else {
            // ReplicationOnly: skip fsync, but still update partition_durable indices
            // since entries are now written and "durable enough" for replication tracking.
            wal.update_group_durable_indices();
            Ok(())
        };

        // Evict sealed segments after successful write + sync.
        // Segments just sealed during append_batch are durable on disk.
        // Evicting their in-memory entries frees ~4 MB per segment.
        if sync_result.is_ok() {
            wal.evict_all_sealed_segments();
        }

        // Notify all waiters.
        match sync_result {
            Ok(()) => {
                // Update group_last_index for successful writes.
                {
                    let mut last_index = inner.group_last_index.write().await;
                    for (_, ack) in &channels {
                        last_index.insert(ack.group_id, ack.index);
                    }
                }

                // Send success notifications.
                for (tx, ack) in channels {
                    let _ = tx.send(Ok(ack));
                }
            }
            Err(e) => {
                // Sync failed - notify all waiters with error.
                for (tx, _) in channels {
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
/// 1. **`NVMe` fsync parallelism**: Modern `NVMe` SSDs handle ~4-8 parallel fsyncs
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
/// 4. **`TigerStyle`**: "Put a limit on everything." This catches configuration
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

    /// Sets the write durability mode for all WALs.
    ///
    /// - `WriteDurability::Fsync`: Wait for fsync on each flush (safe for single-node)
    /// - `WriteDurability::ReplicationOnly`: Skip fsync, rely on Raft replication (fast)
    #[must_use]
    pub const fn with_durability(mut self, durability: WriteDurability) -> Self {
        self.coordinator_config.durability = durability;
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
/// # Group Assignment
///
/// Groups are assigned to WALs by hashing: `group_id % wal_count`. This ensures:
/// - Deterministic assignment (same group always goes to same WAL)
/// - Even distribution (assuming group IDs are well-distributed)
/// - No coordination needed (each group knows its WAL)
pub struct SharedWalPool<S: Storage> {
    /// The WALs in this pool, indexed by `group_id % wal_count`.
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

    /// Gets a handle for a group.
    ///
    /// The group is assigned to a WAL by hashing: `group_id % wal_count`.
    /// This assignment is deterministic and stable.
    ///
    /// # Panics
    ///
    /// Panics if the internal state is inconsistent (should never happen).
    #[must_use]
    pub fn handle(&self, group_id: GroupId) -> SharedWalHandle<S> {
        // TigerStyle: Explicit cast with bounds check via assert.
        // Safe: wal_count <= POOL_WAL_COUNT_MAX (16), so result fits in usize on any platform.
        #[allow(clippy::cast_possible_truncation)]
        let wal_index = (group_id.get() % u64::from(self.wal_count)) as usize;
        assert!(wal_index < self.coordinators.len());

        self.coordinators[wal_index].handle(group_id)
    }

    /// Returns which WAL index a group is assigned to.
    ///
    /// Useful for debugging and metrics.
    ///
    /// # Panics
    ///
    /// Panics if the internal state is inconsistent (should never happen).
    #[must_use]
    pub fn wal_index_for_group(&self, group_id: GroupId) -> u32 {
        #[allow(clippy::cast_possible_truncation)] // Bounded by wal_count.
        let index = (group_id.get() % u64::from(self.wal_count)) as u32;
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
    pub async fn recover(&self) -> WalResult<HashMap<GroupId, Vec<SharedEntry>>> {
        let mut all_entries: HashMap<GroupId, Vec<SharedEntry>> = HashMap::new();

        for coordinator in &self.coordinators {
            let wal_entries = coordinator.recover().await?;
            for (group_id, entries) in wal_entries {
                all_entries.entry(group_id).or_default().extend(entries);
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

    /// Recovers state using streaming to bound peak memory.
    ///
    /// Calls `apply_fn` for each winning entry across all WALs in the pool.
    /// Unlike [`recover`](Self::recover), entries are never accumulated in memory:
    /// each winning entry is delivered to `apply_fn` and then discarded.
    ///
    /// # Errors
    ///
    /// Returns an error if any WAL fails to recover.
    pub async fn recover_streaming<F>(
        &self,
        apply_fn: &mut F,
    ) -> WalResult<HashMap<GroupId, GroupRecoveryState>>
    where
        F: FnMut(GroupId, &SharedEntry),
    {
        let mut all_states: HashMap<GroupId, GroupRecoveryState> = HashMap::new();
        for coordinator in &self.coordinators {
            let states = coordinator.recover_streaming(apply_fn).await?;
            // Merge: keep the state with higher last_wal_index per group.
            // (Each group is assigned to exactly one WAL, so merging is a no-op
            // in practice — but we handle it defensively.)
            for (group_id, state) in states {
                let entry = all_states.entry(group_id).or_default();
                if state.last_wal_index > entry.last_wal_index {
                    *entry = state;
                }
            }
        }
        Ok(all_states)
    }

    /// Flushes all pending writes across all WALs in the pool.
    ///
    /// This triggers an immediate fsync on each WAL, making all buffered
    /// entries durable. Use this when you need to ensure durability without
    /// waiting for the automatic flush interval.
    ///
    /// # Errors
    ///
    /// Returns an error if any WAL fails to flush.
    pub async fn flush(&self) -> WalResult<()> {
        for coordinator in &self.coordinators {
            coordinator.flush().await?;
        }
        Ok(())
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

    /// Returns a reference to the coordinators for retention processing.
    ///
    /// The retention task iterates over each coordinator to check segment
    /// age and replication safety. This avoids duplicating pool-level
    /// iteration logic in the server.
    #[must_use]
    pub fn coordinators(&self) -> &[SharedWalCoordinator<S>] {
        &self.coordinators
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

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Append entries from two partitions.
        wal.append(p1, 1, 1, 0, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, 0, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, 0, Bytes::from("p1-2")).await.unwrap();
        wal.append(p2, 1, 2, 0, Bytes::from("p2-2")).await.unwrap();

        wal.sync().await.unwrap();

        assert_eq!(wal.entry_count(), 4);
    }

    #[tokio::test]
    async fn test_shared_wal_recover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);
        let p3 = GroupId::new(3);

        // Write entries.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            wal.append(p1, 1, 1, 0, Bytes::from("p1-1")).await.unwrap();
            wal.append(p2, 1, 1, 0, Bytes::from("p2-1")).await.unwrap();
            wal.append(p1, 1, 2, 0, Bytes::from("p1-2")).await.unwrap();
            wal.append(p3, 1, 1, 0, Bytes::from("p3-1")).await.unwrap();
            wal.append(p2, 1, 2, 0, Bytes::from("p2-2")).await.unwrap();

            wal.sync().await.unwrap();
        }

        // Reopen and recover.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

            let by_partition = wal.recover().await.unwrap();

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
            wal.append(p1, 1, 3, 0, Bytes::from("p1-3")).await.unwrap();
            wal.append(p3, 1, 2, 0, Bytes::from("p3-2")).await.unwrap();
            wal.sync().await.unwrap();

            assert_eq!(wal.entry_count(), 7);
        }
    }

    #[tokio::test]
    async fn test_shared_wal_entries_for_group() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        wal.append(p1, 1, 1, 0, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, 0, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, 0, Bytes::from("p1-2")).await.unwrap();
        wal.append(p1, 1, 3, 0, Bytes::from("p1-3")).await.unwrap();
        wal.sync().await.unwrap();

        let p1_entries = wal.entries_for_group(p1);
        assert_eq!(p1_entries.len(), 3);

        let p2_entries = wal.entries_for_group(p2);
        assert_eq!(p2_entries.len(), 1);

        let p3_entries = wal.entries_for_group(GroupId::new(3));
        assert_eq!(p3_entries.len(), 0);
    }

    #[tokio::test]
    async fn test_shared_wal_allows_index_gaps() {
        // Gaps are allowed to support explicit index mode where NOOP entries
        // may be skipped (e.g., Raft PREVIOUS_TERM entries).
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        wal.append(p1, 1, 1, 0, Bytes::from("p1-1")).await.unwrap();
        // Skip index 2 - gaps are allowed.
        wal.append(p1, 1, 3, 0, Bytes::from("p1-3")).await.unwrap();
        wal.sync().await.unwrap();

        let entries = wal.entries_for_group(p1);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index(), 1);
        assert_eq!(entries[1].index(), 3);
    }

    #[tokio::test]
    #[should_panic(expected = "index must be greater than last")]
    async fn test_shared_wal_rejects_out_of_order_index() {
        // Out-of-order (going backwards) is still rejected.
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        wal.append(p1, 1, 5, 0, Bytes::from("p1-5")).await.unwrap();
        // Going backwards - should panic.
        wal.append(p1, 1, 3, 0, Bytes::from("p1-3")).await.unwrap();
    }

    /// Verifies that term decreases are accepted (PREVIOUS_TERM commit scenario).
    ///
    /// Previously this was a `should_panic` test asserting term monotonicity.
    /// That assertion was wrong — Raft PREVIOUS_TERM commits legitimately produce
    /// committed entries with lower terms than previously written entries.
    #[tokio::test]
    async fn test_shared_wal_allows_lower_term() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        wal.append(p1, 5, 1, 0, Bytes::from("p1-1")).await.unwrap();
        // Term goes backwards (PREVIOUS_TERM commit) - must not panic.
        wal.append(p1, 3, 2, 0, Bytes::from("p1-2"))
            .await
            .expect("lower term must be accepted");
        // Higher term entry after the lower one works too.
        wal.append(p1, 6, 3, 0, Bytes::from("p1-3"))
            .await
            .expect("higher term after lower must be accepted");
    }

    #[tokio::test]
    async fn test_shared_wal_independent_partition_indices() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Both partitions can have index 1, 2, 3...
        wal.append(p1, 1, 1, 0, Bytes::from("p1-1")).await.unwrap();
        wal.append(p2, 1, 1, 0, Bytes::from("p2-1")).await.unwrap();
        wal.append(p1, 1, 2, 0, Bytes::from("p1-2")).await.unwrap();
        wal.append(p2, 1, 2, 0, Bytes::from("p2-2")).await.unwrap();

        wal.sync().await.unwrap();

        // Verify both partitions have their own index sequences.
        let p1_entries = wal.entries_for_group(p1);
        assert_eq!(p1_entries[0].index(), 1);
        assert_eq!(p1_entries[1].index(), 2);

        let p2_entries = wal.entries_for_group(p2);
        assert_eq!(p2_entries[0].index(), 1);
        assert_eq!(p2_entries[1].index(), 2);
    }

    #[tokio::test]
    async fn test_shared_wal_truncate_after_basic() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        // Append entries 1-5.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, 0, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Truncate after index 3.
        wal.truncate_after(p1, 3);

        // Entries 4, 5 should no longer be visible.
        let entries = wal.entries_for_group(p1);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.last().unwrap().index(), 3);

        // read() should return None for truncated indices.
        assert!(wal.read(p1, 3).is_some());
        assert!(wal.read(p1, 4).is_none());
        assert!(wal.read(p1, 5).is_none());

        // Can append new entries starting from 4.
        wal.append(p1, 2, 4, 0, Bytes::from("p1-4-new")).await.unwrap();
        wal.append(p1, 2, 5, 0, Bytes::from("p1-5-new")).await.unwrap();
        wal.sync().await.unwrap();

        // Now we should have 5 entries: 1, 2, 3, 4-new, 5-new.
        let entries = wal.entries_for_group(p1);
        assert_eq!(entries.len(), 5);

        // The new entries should have term 2.
        assert_eq!(entries[3].term(), 2);
        assert_eq!(entries[4].term(), 2);
    }

    #[tokio::test]
    async fn test_shared_wal_truncate_and_recover() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = GroupId::new(1);

        // Phase 1: Write, truncate, write new entries.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            // Write entries 1-5 at term 1.
            for i in 1..=5u64 {
                wal.append(p1, 1, i, 0, Bytes::from(format!("old-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();

            // Truncate after 3.
            wal.truncate_after(p1, 3);

            // Write new entries 4-6 at term 2.
            for i in 4..=6u64 {
                wal.append(p1, 2, i, 0, Bytes::from(format!("new-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Phase 2: Recover and verify last-write-wins.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

            let by_partition = wal.recover().await.unwrap();
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

    /// Regression test: PREVIOUS_TERM commit after higher-term commit must not panic.
    ///
    /// This reproduces the production staging crash:
    ///   "group group-94 term must be non-decreasing: last 340, got 233"
    ///
    /// Scenario: partition group has committed entries up to term 340. After a
    /// restart, a new leader in term 341 commits entries that were PROPOSED in
    /// term 233 but never committed (PREVIOUS_TERM entries from an old leader
    /// that survived in the new leader's log). These entries have term=233, which
    /// is lower than the last-written term=340. The WAL must accept them.
    #[tokio::test]
    async fn test_shared_wal_previous_term_commit_lower_term() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());
        let group = GroupId::new(94);

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        // Simulate a sequence of commits that includes terms going up to 340,
        // matching the staging scenario (last committed entry in term 340).
        wal.append(group, 100, 1, 50, Bytes::from("entry-1"))
            .await
            .unwrap();
        wal.append(group, 233, 2, 51, Bytes::from("entry-2"))
            .await
            .unwrap();
        wal.append(group, 340, 3, 100, Bytes::from("entry-3"))
            .await
            .unwrap();
        // group_state: last_index=3, last_term=340

        // After restart, new leader in term 341 commits a PREVIOUS_TERM entry
        // (proposed in term 233, never committed). This entry has Raft index 101
        // but term 233 — lower than the last committed term 340. Must not panic.
        wal.append(group, 233, 4, 101, Bytes::from("previous-term-entry"))
            .await
            .expect("PREVIOUS_TERM commit with lower term must not panic");

        // New leader's own entry at term 341 follows.
        wal.append(group, 341, 5, 102, Bytes::from("new-leader-entry"))
            .await
            .expect("new leader entry at term 341 must succeed");
    }

    /// Regression test: truncating stale high-term entries then appending at a lower
    /// term must not panic.  This is the Raft log-conflict-resolution path where a
    /// follower has uncommitted entries at term T and a new leader rewrites them at
    /// term T' < T.
    #[tokio::test]
    async fn test_shared_wal_truncate_then_append_lower_term() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());
        let p1 = GroupId::new(1);

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        // Write entries 1-5 escalating from term 1 to term 5 (simulating a
        // follower that received entries from several successive leaders).
        for i in 1..=5u64 {
            wal.append(p1, i, i, 0, Bytes::from(format!("stale-{i}")))
                .await
                .unwrap();
        }
        // group-state: last_index=5, last_term=5

        // New leader at term 3 wins, it truncates entries 4-5 (stale) and
        // rewrites from index 4 at term 3 — which is LOWER than term 5.
        wal.truncate_after(p1, 3);

        // Must not panic.
        wal.append(p1, 3, 4, 0, Bytes::from("leader-4"))
            .await
            .expect("append at lower term after truncation must succeed");
        wal.append(p1, 3, 5, 0, Bytes::from("leader-5"))
            .await
            .expect("append at same term must succeed");
        wal.append(p1, 4, 6, 0, Bytes::from("leader-6"))
            .await
            .expect("append at higher term must succeed");
    }

    #[tokio::test]
    #[should_panic(expected = "cannot truncate group")]
    async fn test_shared_wal_truncate_beyond_last_panics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        // Append entries 1-3.
        for i in 1..=3u64 {
            wal.append(p1, 1, i, 0, Bytes::from(format!("p1-{i}")))
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

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Append entries for both partitions.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, 0, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
            wal.append(p2, 1, i, 0, Bytes::from(format!("p2-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Truncate p1 after 2, leave p2 intact.
        wal.truncate_after(p1, 2);

        // p1 should have 2 entries, p2 should have 5.
        assert_eq!(wal.entries_for_group(p1).len(), 2);
        assert_eq!(wal.entries_for_group(p2).len(), 5);

        // Append new entries to p1.
        wal.append(p1, 2, 3, 0, Bytes::from("p1-3-new")).await.unwrap();
        wal.sync().await.unwrap();

        assert_eq!(wal.entries_for_group(p1).len(), 3);
        assert_eq!(wal.entries_for_group(p2).len(), 5);
    }

    #[tokio::test]
    async fn test_shared_wal_evict_and_read_from_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Append entries.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, 0, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
            wal.append(p2, 1, i, 0, Bytes::from(format!("p2-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Force rotation by appending enough to create a new segment.
        // First, we need entries in a sealed segment, so append more to trigger rotation.
        wal.append(p1, 1, 6, 0, Bytes::from("p1-6")).await.unwrap();
        wal.sync().await.unwrap();

        let sealed_ids = wal.sealed_segment_ids();
        if sealed_ids.is_empty() {
            // No sealed segments - can't test eviction.
            return;
        }

        // In-memory read should work.
        assert!(wal.read(p1, 3).is_some());

        // Evict all sealed segments.
        let count = wal.evict_all_sealed_segments();
        assert!(count > 0);

        // partition_index should be empty for evicted entries.
        // read() returns None for evicted entries (it only checks partition_index).
        assert!(wal.read(p1, 3).is_none());

        // read_or_load() should read from disk.
        let entry = wal.read_or_load(p1, 3).await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.group_id(), p1);
        assert_eq!(entry.index(), 3);
        assert_eq!(entry.payload.as_ref(), b"p1-3");

        // read_or_load() for p2 should also work.
        let entry = wal.read_or_load(p2, 2).await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.group_id(), p2);
        assert_eq!(entry.index(), 2);

        // read_entries_range_or_load should work.
        let entries = wal
            .read_entries_range_or_load(p1, 1, 5, u64::MAX)
            .await
            .unwrap();
        assert_eq!(entries.len(), 5);
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.index(), (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_shared_wal_evict_preserves_metadata() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();

        let p1 = GroupId::new(1);

        for i in 1..=3u64 {
            wal.append(p1, 1, i, 0, Bytes::from(format!("data-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Append more to trigger rotation.
        wal.append(p1, 1, 4, 0, Bytes::from("data-4")).await.unwrap();

        let sealed_ids = wal.sealed_segment_ids();
        if sealed_ids.is_empty() {
            return;
        }

        // Check segment info before eviction.
        let info_before = wal.segment_info(sealed_ids[0]);
        assert!(info_before.is_some());

        // Evict.
        wal.evict_all_sealed_segments();

        // segment_info should still work (uses stored info).
        let info_after = wal.segment_info(sealed_ids[0]);
        assert!(info_after.is_some());
        assert_eq!(info_before.unwrap().entry_count, info_after.unwrap().entry_count);

        // entry_count still correct.
        assert!(wal.entry_count() > 0);

        // partition_last_index still correct.
        assert_eq!(wal.group_last_index(p1), Some(4));
    }

    /// Verifies that raft_index survives WAL round-trip through recovery.
    ///
    /// This is the core property: after write → crash → recover, the
    /// `raft_index` field in each entry must reflect the actual Raft index
    /// (not the auto-counter), so `compacted_index` is set correctly.
    #[tokio::test]
    async fn test_raft_index_preserved_through_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = GroupId::new(1);

        // Simulate entries where raft_index diverges from auto-counter index.
        // This happens when leader elections produce no-op entries that
        // increment the Raft index but are never written to the data WAL.
        //
        // auto-counter: 1, 2, 3, 4, 5
        // raft_index:  10, 11, 15, 16, 20  (gaps from elections/no-ops)
        let raft_indices = [10u64, 11, 15, 16, 20];

        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            for (i, &raft_idx) in raft_indices.iter().enumerate() {
                let auto_idx = (i + 1) as u64;
                wal.append(
                    p1,
                    1,
                    auto_idx,
                    raft_idx,
                    Bytes::from(format!("data-{auto_idx}")),
                )
                .await
                .unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Reopen and recover — simulates crash + restart.
        {
            let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();
            let by_partition = wal.recover().await.unwrap();
            let entries = by_partition.get(&p1).unwrap();

            assert_eq!(entries.len(), 5);

            for (i, entry) in entries.iter().enumerate() {
                let expected_auto = (i + 1) as u64;
                let expected_raft = raft_indices[i];

                // Auto-counter (index) must be preserved.
                assert_eq!(
                    entry.index(),
                    expected_auto,
                    "auto-counter mismatch at position {i}"
                );
                // Raft index must be preserved — this is the critical property.
                assert_eq!(
                    entry.raft_index(),
                    expected_raft,
                    "raft_index mismatch at position {i}: \
                     expected {expected_raft}, got {}",
                    entry.raft_index()
                );
            }

            // The last recovered entry's raft_index is what would become
            // compacted_index. It must be the actual Raft index (20),
            // NOT the auto-counter (5).
            let last = entries.last().unwrap();
            assert_eq!(last.raft_index(), 20);
            assert_eq!(last.index(), 5);
        }
    }

    /// Verifies that multiple partitions can have independently diverging
    /// raft_index values, and recovery preserves them correctly.
    #[tokio::test]
    async fn test_raft_index_independent_per_partition() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = SharedWalConfig::new(temp_dir.path());

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        {
            let mut wal = SharedWal::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            // p1: auto=1 raft=100, auto=2 raft=105 (5 elections happened)
            wal.append(p1, 1, 1, 100, Bytes::from("p1-1")).await.unwrap();
            wal.append(p1, 1, 2, 105, Bytes::from("p1-2")).await.unwrap();

            // p2: auto=1 raft=200, auto=2 raft=201 (no elections)
            wal.append(p2, 1, 1, 200, Bytes::from("p2-1")).await.unwrap();
            wal.append(p2, 1, 2, 201, Bytes::from("p2-2")).await.unwrap();

            wal.sync().await.unwrap();
        }

        {
            let mut wal = SharedWal::open(TokioStorage::new(), config).await.unwrap();
            let by_partition = wal.recover().await.unwrap();

            let p1_entries = by_partition.get(&p1).unwrap();
            assert_eq!(p1_entries[0].raft_index(), 100);
            assert_eq!(p1_entries[1].raft_index(), 105);

            let p2_entries = by_partition.get(&p2).unwrap();
            assert_eq!(p2_entries[0].raft_index(), 200);
            assert_eq!(p2_entries[1].raft_index(), 201);
        }
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
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(10));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        let h1 = coordinator.handle(p1);
        let h2 = coordinator.handle(p2);

        // Concurrent writes from two partitions.
        let (r1, r2) = tokio::join!(
            h1.append(1, 1, 0, Bytes::from("p1-1")),
            h2.append(1, 1, 0, Bytes::from("p2-1")),
        );

        assert!(r1.is_ok());
        assert!(r2.is_ok());

        let ack1 = r1.unwrap();
        let ack2 = r2.unwrap();

        assert_eq!(ack1.group_id, p1);
        assert_eq!(ack1.index, 1);
        assert_eq!(ack2.group_id, p2);
        assert_eq!(ack2.index, 1);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_sequential_writes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);

        // Sequential writes.
        for i in 1..=10u64 {
            let ack = h1
                .append(1, i, 0, Bytes::from(format!("entry-{i}")))
                .await
                .unwrap();
            assert_eq!(ack.index, i);
        }

        assert_eq!(coordinator.entry_count().await, 10);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_multi_partition_concurrent() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let partition_count = 10;
        let entries_per_partition = 5;

        // Create handles for all partitions.
        let handles: Vec<_> = (1..=partition_count)
            .map(|p| coordinator.handle(GroupId::new(p)))
            .collect();

        // Spawn concurrent writers.
        let mut tasks = Vec::new();
        for (p_idx, handle) in handles.into_iter().enumerate() {
            let task = tokio::spawn(async move {
                for i in 1..=entries_per_partition as u64 {
                    let ack = handle
                        .append(1, i, 0, Bytes::from(format!("p{}-{}", p_idx + 1, i)))
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
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(5));

        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);

        // Phase 1: Write entries.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            let h1 = coordinator.handle(p1);
            let h2 = coordinator.handle(p2);

            for i in 1..=5u64 {
                h1.append(1, i, 0, Bytes::from(format!("p1-{i}")))
                    .await
                    .unwrap();
                h2.append(1, i, 0, Bytes::from(format!("p2-{i}")))
                    .await
                    .unwrap();
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
            h1.append(1, 6, 0, Bytes::from("p1-6")).await.unwrap();

            assert_eq!(coordinator.entry_count().await, 11);

            coordinator.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_coordinator_handle_clone() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);
        let h1_clone = h1.clone();

        // Both handles can write.
        h1.append(1, 1, 0, Bytes::from("from-original")).await.unwrap();
        h1_clone
            .append(1, 2, 0, Bytes::from("from-clone"))
            .await
            .unwrap();

        assert_eq!(coordinator.entry_count().await, 2);

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_flush() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_secs(60)); // Long interval

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);

        // Write without waiting for automatic flush.
        let rx = h1.append_async(1, 1, 0, Bytes::from("entry")).await.unwrap();

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
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_secs(60)); // Long interval

        let p1 = GroupId::new(1);

        // Phase 1: Write using append_async and shutdown (should flush).
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            let h1 = coordinator.handle(p1);
            // Use append_async so we don't wait for the 60s flush interval.
            let _rx = h1.append_async(1, 1, 0, Bytes::from("entry-1")).await.unwrap();

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
        let config =
            CoordinatorConfig::new(temp_dir.path()).with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);

        // Initially no durable index.
        assert_eq!(coordinator.group_durable_index(p1).await, None);

        // Write and wait for durability.
        h1.append(1, 1, 0, Bytes::from("entry-1")).await.unwrap();
        h1.append(1, 2, 0, Bytes::from("entry-2")).await.unwrap();

        // Now durable index should be 2.
        assert_eq!(coordinator.group_durable_index(p1).await, Some(2));

        coordinator.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_coordinator_read_after_eviction() {
        // Coordinator's flush_loop evicts sealed segments automatically.
        // Verify that reads still work through disk fallback.
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5))
            .with_segment_config(
                crate::SegmentConfig::new().with_max_size(1024 * 1024),
            );

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);

        // Write enough entries to trigger segment rotation.
        for i in 1..=20u64 {
            h1.append(1, i, 0, Bytes::from(format!("entry-{i}")))
                .await
                .unwrap();
        }

        // Wait for flush_loop to process (includes eviction).
        coordinator.flush().await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Reads should still work (through disk fallback if evicted).
        let entry = h1.read_entry(5).await;
        assert!(entry.is_some(), "entry 5 should be readable after eviction");
        assert_eq!(entry.unwrap().index(), 5);

        // Range read should work.
        let entries = h1.read_entries_range(1, 10, u64::MAX).await;
        assert!(!entries.is_empty(), "range read should return entries");

        coordinator.shutdown().await.unwrap();
    }

    /// Verifies raft_index survives the coordinator path (append_nowait → flush
    /// → recovery). This is the production write path for shared WAL partitions.
    #[tokio::test]
    async fn test_coordinator_raft_index_through_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let p1 = GroupId::new(1);

        // Write entries with diverging raft_index through the coordinator path.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config.clone())
                .await
                .unwrap();

            let h1 = coordinator.handle(p1);

            // append_nowait(term, raft_index, payload) → auto-counter assigned internally.
            // auto=1 raft=100, auto=2 raft=105, auto=3 raft=110
            h1.append_nowait(1, 100, Bytes::from("entry-1")).await.unwrap();
            h1.append_nowait(1, 105, Bytes::from("entry-2")).await.unwrap();
            h1.append_nowait(1, 110, Bytes::from("entry-3")).await.unwrap();

            // Ensure everything is flushed to disk.
            coordinator.flush().await.unwrap();
            coordinator.shutdown().await.unwrap();
        }

        // Recover and verify raft_index is preserved.
        {
            let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let recovered = coordinator.recover().await.unwrap();
            let entries = recovered.get(&p1).unwrap();

            assert_eq!(entries.len(), 3);

            // Auto-counter (index) must be sequential: 1, 2, 3.
            assert_eq!(entries[0].index(), 1);
            assert_eq!(entries[1].index(), 2);
            assert_eq!(entries[2].index(), 3);

            // Raft index must match what was passed: 100, 105, 110.
            assert_eq!(entries[0].raft_index(), 100);
            assert_eq!(entries[1].raft_index(), 105);
            assert_eq!(entries[2].raft_index(), 110);

            coordinator.shutdown().await.unwrap();
        }
    }

    /// Verifies the DurableAck returned from append contains the correct raft_index.
    #[tokio::test]
    async fn test_coordinator_durable_ack_has_raft_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = CoordinatorConfig::new(temp_dir.path())
            .with_flush_interval(Duration::from_millis(5));

        let coordinator = SharedWalCoordinator::open(TokioStorage::new(), config)
            .await
            .unwrap();

        let p1 = GroupId::new(1);
        let h1 = coordinator.handle(p1);

        // append() waits for durability and returns DurableAck.
        let ack = h1.append(1, 1, 7254, Bytes::from("data")).await.unwrap();
        assert_eq!(ack.index, 1); // auto-counter
        assert_eq!(ack.raft_index, 7254); // actual Raft index

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
        let config =
            PoolConfig::new(temp_dir.path(), 2).with_flush_interval(Duration::from_millis(10));

        let pool = SharedWalPool::open(TokioStorage::new(), config)
            .await
            .unwrap();

        // Verify pool has correct count.
        assert_eq!(pool.wal_count(), 2);
        assert!(pool.is_empty().await);

        // Partitions should be distributed across WALs.
        let p1 = GroupId::new(1);
        let p2 = GroupId::new(2);
        let p3 = GroupId::new(3);

        // With 2 WALs: p1 -> wal 1, p2 -> wal 0, p3 -> wal 1.
        assert_eq!(pool.wal_index_for_group(p1), 1);
        assert_eq!(pool.wal_index_for_group(p2), 0);
        assert_eq!(pool.wal_index_for_group(p3), 1);

        // Get handles and write.
        let h1 = pool.handle(p1);
        let h2 = pool.handle(p2);

        h1.append(1, 1, 0, Bytes::from("p1-entry-1")).await.unwrap();
        h2.append(1, 1, 0, Bytes::from("p2-entry-1")).await.unwrap();

        // Verify entry count.
        assert_eq!(pool.entry_count().await, 2);

        pool.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_pool_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_path = temp_dir.path().to_path_buf();

        let p1 = GroupId::new(10);
        let p2 = GroupId::new(11);

        // Write entries.
        {
            let config =
                PoolConfig::new(&base_path, 4).with_flush_interval(Duration::from_millis(5));

            let pool = SharedWalPool::open(TokioStorage::new(), config)
                .await
                .unwrap();

            let h1 = pool.handle(p1);
            let h2 = pool.handle(p2);

            h1.append(1, 1, 0, Bytes::from("p1-1")).await.unwrap();
            h1.append(1, 2, 0, Bytes::from("p1-2")).await.unwrap();
            h2.append(1, 1, 0, Bytes::from("p2-1")).await.unwrap();

            pool.shutdown().await.unwrap();
        }

        // Recover entries.
        {
            let config =
                PoolConfig::new(&base_path, 4).with_flush_interval(Duration::from_millis(5));

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
            let partition = GroupId::new(i);
            let wal_idx = pool.wal_index_for_group(partition);
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

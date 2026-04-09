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

use tracing::{info, warn};

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

        let evicted_groups = self.evicted_index.len();
        let evicted_entries: usize = self.evicted_index.values().map(std::collections::BTreeMap::len).sum();
        info!(
            segments_scanned = sealed_ids.len(),
            evicted_groups,
            evicted_entries,
            active_groups = self.group_index.len(),
            "SharedWal recovery complete"
        );

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

    /// Sets group state from an external source (e.g., snapshot recovery).
    ///
    /// On a fresh node recovering from a remote snapshot, the WAL has no
    /// local entries for the group, but `read_or_load` needs `group_state`
    /// to avoid early-returning `None` for indices that exist in S3.
    /// Calling this with the snapshot's last WAL auto-counter and term
    /// ensures the bounds check passes and the S3 fallback path activates.
    ///
    /// No-op if the group already has state (from WAL recovery).
    pub fn set_group_state_if_absent(
        &mut self,
        group_id: GroupId,
        last_index: u64,
        last_term: u64,
    ) {
        self.group_state.entry(group_id).or_insert(GroupState {
            last_index,
            last_term,
        });
    }

    /// Returns the list of sealed segment IDs.
    ///
    /// Sealed segments are immutable and can be safely uploaded to tiering storage.
    #[must_use]
    pub fn sealed_segment_ids(&self) -> Vec<crate::SegmentId> {
        self.wal.sealed_segment_ids()
    }

    /// Ensures segment ID counter is above `min_id` to prevent collisions
    /// with remote segments that were not downloaded locally.
    pub fn ensure_next_segment_id_above(&mut self, min_id: crate::SegmentId) {
        self.wal.ensure_next_segment_id_above(min_id);
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

    /// Writes raw segment bytes to the WAL directory and registers the segment.
    ///
    /// Used during startup recovery to restore segments downloaded from the
    /// object store. After this call the segment is visible to `recover()`.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails or the bytes are invalid.
    pub async fn restore_segment_from_bytes(
        &mut self,
        segment_id: u64,
        bytes: Bytes,
    ) -> WalResult<()> {
        self.wal.restore_segment_from_bytes(segment_id, bytes).await
    }

    /// Registers a segment whose bytes have already been written to disk.
    ///
    /// Unlike [`restore_segment_from_bytes`], this performs no disk I/O — it
    /// only updates the in-memory segment index. Use this when the disk write
    /// was performed outside the WAL lock to keep expensive I/O off the lock path.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment header cannot be decoded from `bytes`.
    pub fn register_restored_segment(
        &mut self,
        segment_id: u64,
        path: std::path::PathBuf,
        bytes: Bytes,
    ) -> WalResult<()> {
        self.wal.register_restored_segment(segment_id, path, bytes)
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

    /// Deletes a sealed segment from disk without cleaning `evicted_index` or
    /// `group_index`.
    ///
    /// After deletion, `read_or_load()` will find the entry in `evicted_index`
    /// but the segment file is missing, returning `WalError::SegmentNotFound`.
    /// The coordinator's read methods intercept this error to trigger an S3
    /// fallback download.
    ///
    /// # Errors
    ///
    /// Returns an error if the disk file cannot be removed.
    pub async fn delete_segment_file_only(
        &mut self,
        segment_id: crate::SegmentId,
    ) -> WalResult<()> {
        // Delegate to the underlying WAL: removes from sealed_segments, deletes
        // the file on disk, updates compact_state.
        // Intentionally skip the evicted_index and group_index cleanup that
        // delete_sealed_segment() performs, so S3 fallback reads can locate
        // the entry's segment_id via evicted_index.
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
                warn!(
                    group_id = group_id.get(),
                    index,
                    last_index = state.last_index,
                    "read_or_load: index beyond last_index (bounds check)"
                );
                return Ok(None);
            }
        } else {
            warn!(
                group_id = group_id.get(),
                index,
                "read_or_load: no group_state for group"
            );
            return Ok(None);
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
            let result = self
                .wal
                .scan_segment_for_entry(segment_id, |e| {
                    e.group_id() == group_id && e.index() == index
                })
                .await?;
            if result.is_none() {
                warn!(
                    group_id = group_id.get(),
                    index,
                    segment_id = segment_id.get(),
                    "read_or_load: evicted_index hit but scan returned None (segment present but entry missing)"
                );
            }
            return Ok(result);
        }

        // Log diagnostic info: are we missing the whole group or just this index?
        let (evicted_has_group, evicted_min, evicted_max, evicted_count) =
            self.evicted_index.get(&group_id).map_or(
                (false, 0, 0, 0),
                |btree| {
                    let min = btree.keys().next().copied().unwrap_or(0);
                    let max = btree.keys().next_back().copied().unwrap_or(0);
                    (true, min, max, btree.len())
                },
            );
        warn!(
            group_id = group_id.get(),
            index,
            evicted_has_group,
            evicted_min,
            evicted_max,
            evicted_count,
            group_index_has_group = self.group_index.contains_key(&group_id),
            "read_or_load: not in group_index or evicted_index, returning None"
        );
        Ok(None)
    }

    /// Scans all sealed segments on disk for an entry matching (group, index).
    ///
    /// Used as a fallback when the entry is within `group_state` range but
    /// not in `group_index` or `evicted_index`. This happens on fresh nodes
    /// that loaded a remote snapshot and restored S3 segments to disk but
    /// have not yet built the index mappings for those segments.
    ///
    /// Returns `Ok(Some(entry))` if found, `Ok(None)` if no segment has it.
    /// Returns `Err(SegmentNotFound)` if a segment is registered but the
    /// file is missing (trigger for another S3 restore attempt).
    ///
    /// # Errors
    ///
    /// Returns an error if segment decode fails.
    pub async fn scan_sealed_segments_for_entry(
        &mut self,
        group_id: GroupId,
        index: u64,
    ) -> WalResult<Option<SharedEntry>> {
        let segment_ids = self.wal.sealed_segment_ids();
        for sid in segment_ids {
            match self
                .wal
                .scan_segment_for_entry(sid, |e| {
                    e.group_id() == group_id && e.index() == index
                })
                .await
            {
                Ok(Some(entry)) => {
                    // Populate evicted_index so future lookups are O(1).
                    self.evicted_index
                        .entry(group_id)
                        .or_default()
                        .insert(index, (sid, entry.raft_index()));
                    return Ok(Some(entry));
                }
                Ok(None) => {}
                Err(e) => return Err(e),
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

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use tokio::sync::{oneshot, watch, Mutex, Notify, RwLock};
use tracing::error;

use crate::segment_store::WalSegmentStore;

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

    /// Downloads a segment from S3 and restores it to the local WAL directory.
    ///
    /// Returns `true` if the segment was successfully restored, `false` if
    /// tiering is not configured, the segment is not confirmed in S3, or the
    /// download fails.
    ///
    /// Coordinator-level deduplication ensures at most one concurrent download
    /// per `segment_id`, preventing the `O_TRUNC` race that causes
    /// "segment header too small" errors when multiple partition actors in the
    /// same WAL pool simultaneously request the same segment.
    #[allow(clippy::too_many_lines)]
    async fn restore_from_s3(&self, segment_id: u64) -> bool {
        // Only attempt if the segment is confirmed in S3.
        let is_tiered = self
            .inner
            .tiered_segments
            .read()
            .expect("tiered_segments poisoned")
            .contains(&segment_id);
        if !is_tiered {
            warn!(
                segment_id,
                "restore_from_s3: segment not in tiered_segments, S3 fallback unavailable"
            );
            return false;
        }
        let store = {
            let guard = self.inner.segment_store.lock().await;
            guard.as_ref().cloned()
        };
        let Some(store) = store else {
            warn!(segment_id, "restore_from_s3: no segment store configured");
            return false;
        };
        let prefix = self
            .inner
            .store_prefix
            .read()
            .expect("store_prefix poisoned")
            .clone();
        // Key format must match tier_eligible_sealed_segments().
        let key = format!("{prefix}{segment_id:08x}.wal");

        // Phase 0: Coordinator-level deduplication.
        //
        // Multiple partition actors in the same WAL pool share physical WAL
        // segments.  Without this guard, each actor independently calls
        // restore_from_s3 for the same segment_id and all race to
        // tokio::fs::write, which uses O_CREAT|O_WRONLY|O_TRUNC.
        // The second writer truncates the file to 0 bytes mid-read by the
        // first, causing "segment header too small".
        //
        // Solution: the first caller to arrive registers a watch channel in
        // restore_in_progress and does the actual download.  Subsequent callers
        // for the same segment_id find the entry and wait on the channel.
        // watch::Receiver::wait_for checks the current value on arrival, so
        // late subscribers that arrive after send(Some(_)) return immediately.
        let tx = {
            let mut in_progress = self.inner.restore_in_progress.lock().await;
            if let Some(tx) = in_progress.get(&segment_id) {
                // Another handle is already restoring this segment; wait.
                let mut rx = tx.subscribe();
                drop(in_progress);
                // wait_for returns immediately if the value already matches.
                let _ = rx.wait_for(Option::is_some).await;
                return *rx.borrow() == Some(true);
            }
            // We are the designated restorer for this segment.
            let (tx, _rx) = watch::channel(None::<bool>);
            let tx = Arc::new(tx);
            in_progress.insert(segment_id, Arc::clone(&tx));
            tx
        };

        // Phase 1: S3 download — no WAL lock held.
        let bytes = match store.get(&key).await {
            Ok(b) => b,
            Err(e) => {
                warn!(segment_id, error = %e, "S3 fallback download failed");
                self.inner.finish_restore(&tx, segment_id, false).await;
                return false;
            }
        };

        // Validate: a header-only (32-byte) or empty file in S3 is corrupt.
        // Registering it would cause "segment header too small" errors on every
        // subsequent read of that segment.  Reject early so the caller falls
        // through to returning None, which surfaces as a BlobIndex miss rather
        // than a confusing WAL decode error.
        if bytes.len() <= crate::segment::SEGMENT_HEADER_SIZE {
            warn!(
                segment_id,
                bytes = bytes.len(),
                key = %key,
                "S3 fallback: refusing corrupt/empty segment (header-only or truncated)"
            );
            self.inner.finish_restore(&tx, segment_id, false).await;
            return false;
        }

        // Belt-and-suspenders: if the segment was registered by a very late
        // concurrent caller that arrived after our map insertion (impossible
        // under the current deduplication logic, but kept for safety), skip
        // the write to avoid a redundant O_TRUNC.
        {
            let wal = self.inner.wal.lock().await;
            if wal.segment_info(crate::SegmentId::new(segment_id)).is_some() {
                self.inner.finish_restore(&tx, segment_id, true).await;
                return true;
            }
        }

        // Phase 2: Write segment file to disk WITHOUT holding the WAL lock.
        //
        // A naive implementation would acquire the WAL lock before writing —
        // that holds the lock for the full disk write (100-200ms for a 64 MB
        // segment), blocking every concurrent WAL append (Raft log writes).
        // Instead, we write the file first and only acquire the lock for the
        // fast in-memory index update that follows.
        //
        // Uses the Storage trait instead of raw tokio::fs so the write works
        // under deterministic simulation (MadSim) where tokio::fs is unavailable.
        let seg_path = self
            .inner
            .config
            .wal_config
            .wal_config()
            .dir
            .join(format!("segment-{segment_id:08x}.wal"));
        let write_result = async {
            let file = self.inner.storage.open(&seg_path).await?;
            file.truncate(0).await?;
            file.write_at(0, &bytes[..]).await?;
            file.sync().await?;
            Ok::<(), crate::WalError>(())
        }
        .await;
        if let Err(e) = write_result {
            warn!(
                segment_id,
                error = %e,
                "S3 fallback: failed to write segment file to disk"
            );
            self.inner.finish_restore(&tx, segment_id, false).await;
            return false;
        }

        // Phase 3: Register segment in WAL index under WAL lock.
        // This is now a fast in-memory-only operation (header decode + HashMap insert).
        let mut wal = self.inner.wal.lock().await;
        let succeeded = match wal.register_restored_segment(segment_id, seg_path, bytes) {
            Ok(()) => {
                info!(segment_id, key = %key, "S3 fallback: segment restored from object store");
                true
            }
            Err(e) => {
                warn!(
                    segment_id,
                    error = %e,
                    "S3 fallback: failed to register restored segment in WAL index"
                );
                false
            }
        };
        drop(wal);

        // Phase 4: Notify waiters and remove from in-flight map.
        self.inner.finish_restore(&tx, segment_id, succeeded).await;
        succeeded
    }

    /// Reads a specific entry by WAL auto-counter for this partition.
    ///
    /// Uses the per-partition `BTreeMap` index for O(log n) lookup.
    /// Falls back to disk read if the entry's segment has been evicted.
    /// If the segment file has been deleted by tiering, transparently downloads
    /// it from S3 and retries the read. If the entry is not in any local
    /// index (fresh node with remote snapshot), scans sealed segments and
    /// restores from S3 as needed.
    /// Returns `None` if no entry exists at the given index.
    pub async fn read_entry(&self, index: u64) -> Option<SharedEntry> {
        // First attempt: try local disk (with evicted_index fallback).
        // Lock is scoped to this block so it is released before S3 download.
        let first_result = {
            let mut wal = self.inner.wal.lock().await;
            wal.read_or_load(self.group_id, index).await
        };
        match first_result {
            Ok(Some(entry)) => return Some(entry),
            Ok(None) => {
                // Entry not in group_index or evicted_index.
                // On a fresh node with snapshot recovery, segments
                // may exist locally (downloaded) or only in S3.
                // Fall through to S3 scan below.
            }
            Err(crate::WalError::SegmentNotFound { segment_id }) => {
                // Segment file was deleted by tiering; attempt S3 fallback.
                if self.restore_from_s3(segment_id).await {
                    let mut wal = self.inner.wal.lock().await;
                    match wal.read_or_load(self.group_id, index).await {
                        Ok(Some(entry)) => return Some(entry),
                        Ok(None) => {}
                        Err(e) => {
                            warn!(
                                group_id = %self.group_id,
                                index,
                                error = %e,
                                "Read failed after S3 segment restore"
                            );
                            return None;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    group_id = %self.group_id,
                    index,
                    error = %e,
                    "Failed to read entry from disk"
                );
                return None;
            }
        }

        // Fallback: scan local sealed segments (handles segments restored
        // from S3 that aren't in evicted_index yet).
        {
            let mut wal = self.inner.wal.lock().await;
            match wal
                .scan_sealed_segments_for_entry(self.group_id, index)
                .await
            {
                Ok(Some(entry)) => return Some(entry),
                Err(crate::WalError::SegmentNotFound { segment_id }) => {
                    // A registered segment's file is missing; try S3.
                    drop(wal);
                    if self.restore_from_s3(segment_id).await {
                        let mut wal2 = self.inner.wal.lock().await;
                        if let Ok(Some(entry)) = wal2
                            .scan_sealed_segments_for_entry(self.group_id, index)
                            .await
                        {
                            return Some(entry);
                        }
                    }
                }
                Ok(None) | Err(_) => {}
            }
        }

        // Last resort: restore tiered segments from S3 and scan each.
        // This is the cold path for fresh nodes where the entry's segment
        // was never downloaded (below snapshot floor, only in S3).
        self.restore_and_scan_tiered(index).await
    }

    /// Restores tiered segments from S3 one at a time, scanning each for
    /// a specific entry. Stops as soon as the entry is found.
    ///
    /// Segments are restored in ascending order (oldest first) since
    /// `BlobIndex` entries for earlier offsets are in older segments.
    /// Each restored segment is cached locally, amortizing the cost
    /// across subsequent reads.
    async fn restore_and_scan_tiered(&self, index: u64) -> Option<SharedEntry> {
        let tiered: Vec<u64> = {
            let guard = self
                .inner
                .tiered_segments
                .read()
                .expect("tiered_segments poisoned");
            let ids: Vec<u64> = guard.iter().copied().collect();
            drop(guard);
            let mut sorted = ids;
            sorted.sort_unstable();
            sorted
        };

        if tiered.is_empty() {
            return None;
        }

        for segment_id in tiered {
            // Skip if already local (was scanned above).
            {
                let wal = self.inner.wal.lock().await;
                if wal.wal.segment_info(crate::SegmentId::new(segment_id)).is_some() {
                    continue;
                }
            }

            if !self.restore_from_s3(segment_id).await {
                continue;
            }

            let mut wal = self.inner.wal.lock().await;
            if let Ok(Some(entry)) = wal
                .scan_sealed_segments_for_entry(self.group_id, index)
                .await
            {
                return Some(entry);
            }
        }

        warn!(
            group_id = %self.group_id,
            index,
            "read_entry: entry not found in any local or tiered segment"
        );
        None
    }

    /// Reads a range of entries for this partition, bounded by byte size.
    ///
    /// Returns entries from `start_index` to `end_index` (inclusive), stopping
    /// when adding the next entry would exceed `max_bytes`. Always includes at
    /// least one entry if any exist in the range.
    ///
    /// Falls back to disk reads for evicted segments. If the segment file has
    /// been deleted by tiering, transparently downloads it from S3 and retries
    /// (up to 3 times, for ranges spanning multiple missing segments).
    pub async fn read_entries_range(
        &self,
        start_index: u64,
        end_index: u64,
        max_bytes: u64,
    ) -> Vec<SharedEntry> {
        // Bounded retry loop: at most 3 S3 restores for ranges that span
        // multiple tiering-deleted segments.
        let mut restores_remaining = 3u32;
        loop {
            // Scope the WAL lock so it is released before any S3 download.
            let (entries, missing_sid) = {
                let mut wal = self.inner.wal.lock().await;
                let result = wal.read_entries_range(
                    self.group_id,
                    start_index,
                    end_index,
                    max_bytes,
                );
                if !result.is_empty() {
                    return result;
                }
                match wal
                    .read_entries_range_or_load(
                        self.group_id,
                        start_index,
                        end_index,
                        max_bytes,
                    )
                    .await
                {
                    Ok(e) => (e, None),
                    Err(crate::WalError::SegmentNotFound { segment_id }) => {
                        (Vec::new(), Some(segment_id))
                    }
                    Err(e) => {
                        warn!(
                            group_id = %self.group_id,
                            start_index,
                            end_index,
                            error = %e,
                            "Failed to read entry range from disk"
                        );
                        return Vec::new();
                    }
                }
            };
            if !entries.is_empty() {
                return entries;
            }
            if let Some(sid) = missing_sid {
                if restores_remaining > 0 && self.restore_from_s3(sid).await {
                    restores_remaining -= 1;
                    continue; // Retry after S3 restore.
                }
                warn!(
                    group_id = %self.group_id,
                    start_index,
                    end_index,
                    segment_id = sid,
                    "Segment not found locally or in S3 for range read"
                );
            }
            return entries;
        }
    }

    /// Reads a single entry for this partition by Raft log index.
    ///
    /// Unlike `read_entry` which looks up by WAL auto-counter, this searches
    /// by the `raft_index` field stored in each entry header.
    ///
    /// Falls back to disk reads for evicted segments.
    ///
    /// # Errors
    ///
    /// Returns `Err(WalError::SegmentNotFound)` if the segment was locally
    /// deleted by tiering. Callers on the Raft hot path should use snapshot
    /// installation instead of S3 fallback reads when this error is returned.
    pub async fn read_entry_by_raft_index(
        &self,
        raft_index: u64,
    ) -> WalResult<Option<SharedEntry>> {
        let mut wal = self.inner.wal.lock().await;
        // Fast path: scan in-memory entries for this group.
        let mem = wal
            .group_index
            .get(&self.group_id)
            .and_then(|btree| btree.values().find(|e| e.raft_index() == raft_index))
            .cloned();
        if mem.is_some() {
            return Ok(mem);
        }
        // Evicted path: scan by stored raft_index to find matching entry.
        let evicted: Vec<(u64, u64)> = wal
            .evicted_index
            .get(&self.group_id)
            .map(|btree| btree.iter().map(|(&wc, &(_, ri))| (wc, ri)).collect())
            .unwrap_or_default();
        for (wal_counter, stored_raft_index) in evicted {
            if stored_raft_index < raft_index {
                continue;
            }
            if stored_raft_index > raft_index {
                break; // Past target; entry not present.
            }
            match wal.read_or_load(self.group_id, wal_counter).await {
                Ok(Some(e)) => return Ok(Some(e)),
                Ok(None) => {}
                Err(crate::WalError::SegmentNotFound { segment_id }) => {
                    // Segment was locally deleted by tiering. Do NOT attempt
                    // S3 fallback on the Raft hot path — callers must use
                    // snapshot installation for followers that need these entries.
                    eprintln!(
                        "[DST-PROBE] SEGMENT_NOT_FOUND: group={} raft_index={} segment={}",
                        self.group_id,
                        raft_index,
                        segment_id,
                    );
                    warn!(
                        group_id = %self.group_id,
                        raft_index,
                        segment_id,
                        "read_entry_by_raft_index: segment locally deleted by tiering; \
                         caller should use snapshot installation"
                    );
                    return Err(crate::WalError::SegmentNotFound { segment_id });
                }
                Err(err) => {
                    warn!(
                        group_id = %self.group_id,
                        raft_index,
                        error = %err,
                        "Failed to read entry by raft_index from disk"
                    );
                    return Err(err);
                }
            }
        }
        Ok(None)
    }

    /// Reads entries for this partition with `raft_index` in the given range.
    ///
    /// Unlike `read_entries_range` which iterates by WAL auto-counter, this
    /// returns entries whose `raft_index` falls within `[start_raft_index,
    /// end_raft_index]`. Always includes at least one entry if any exist.
    ///
    /// # Errors
    ///
    /// Returns `Err(WalError::SegmentNotFound)` if the required segment was
    /// locally deleted by tiering. Callers on the Raft hot path must use
    /// snapshot installation instead of S3 fallback reads when this occurs.
    pub async fn read_entries_by_raft_index(
        &self,
        start_raft_index: u64,
        end_raft_index: u64,
        max_bytes: u64,
    ) -> WalResult<Vec<SharedEntry>> {
        let mut wal = self.inner.wal.lock().await;
        let result = wal.read_entries_by_raft_index(
            self.group_id,
            start_raft_index,
            end_raft_index,
            max_bytes,
        );
        if !result.is_empty() {
            return Ok(result);
        }
        match wal
            .read_entries_by_raft_index_or_load(
                self.group_id,
                start_raft_index,
                end_raft_index,
                max_bytes,
            )
            .await
        {
            Ok(e) => Ok(e),
            Err(crate::WalError::SegmentNotFound { segment_id }) => {
                // Segment was locally deleted by tiering. Do NOT attempt
                // S3 fallback on the Raft hot path — callers must use
                // snapshot installation for followers that need these entries.
                warn!(
                    group_id = %self.group_id,
                    start_raft_index,
                    end_raft_index,
                    segment_id,
                    "read_entries_by_raft_index: segment locally deleted by tiering; \
                     caller should use snapshot installation"
                );
                Err(crate::WalError::SegmentNotFound { segment_id })
            }
            Err(e) => {
                warn!(
                    group_id = %self.group_id,
                    start_raft_index,
                    end_raft_index,
                    error = %e,
                    "Failed to read entries by raft_index from disk"
                );
                Err(e)
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

    // -------------------------------------------------------------------------
    // Tiering
    // -------------------------------------------------------------------------

    /// Reports the latest WAL auto-counter index that this group has committed
    /// through Raft consensus.
    ///
    /// Called by `DurablePartition::on_entries_committed()` so the coordinator
    /// can determine when all groups in a segment have committed past it, making
    /// the segment eligible for tiering.
    ///
    /// Thread-safe and non-async: uses a `std::sync::RwLock` internally.
    pub fn update_committed_wal_index(&self, wal_index: u64) {
        self.inner
            .update_committed_wal_index(self.group_id, wal_index);
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
    /// Storage backend for writing restored segment files.
    ///
    /// Used by `restore_from_s3` to write downloaded segment data through the
    /// `Storage` trait instead of raw `tokio::fs`, which is not available under
    /// deterministic simulation (`MadSim`).
    storage: S,
    // -------------------------------------------------------------------------
    // Tiering state (populated by configure_tiering; None = tiering disabled).
    // -------------------------------------------------------------------------
    /// Object store for segment tiering. `None` = tiering disabled.
    segment_store: Mutex<Option<Arc<dyn WalSegmentStore>>>,
    /// Per-group last WAL auto-counter index that has been committed through Raft.
    ///
    /// A segment is eligible for tiering once every group that wrote entries
    /// into it has reported a committed WAL index past the last entry of that
    /// group in the segment (see `groups_in_segment()`).
    committed_wal_index: std::sync::RwLock<HashMap<GroupId, u64>>,
    /// Segments confirmed uploaded to the object store.
    /// Once in this set, the segment may be deleted from local disk (subject to
    /// the `delete_tiered_segments` logic).
    tiered_segments: std::sync::RwLock<HashSet<u64>>,
    /// Fully-qualified S3/store key prefix for this coordinator's segments.
    /// Format: `{pod_prefix}shared/{pool_index}/`
    store_prefix: std::sync::RwLock<String>,
    /// In-flight S3 segment restores, keyed by `segment_id`.
    ///
    /// Ensures at most one download+write happens per segment at a time.
    /// Other handles that need the same segment wait for the in-progress
    /// restore to complete rather than racing to write the same file.
    restore_in_progress: Mutex<HashMap<u64, Arc<watch::Sender<Option<bool>>>>>,
    // -------------------------------------------------------------------------
    // Snapshot floor tracking (safety guard for local segment deletion).
    // -------------------------------------------------------------------------
    /// Per-group WAL index of the last snapshot taken.
    ///
    /// A segment can only be deleted once all registered groups have taken a
    /// snapshot that covers every entry in the segment. This prevents deleting
    /// segments that are still needed for crash recovery on a node that has no
    /// snapshot yet.
    snapshot_floors: std::sync::RwLock<HashMap<GroupId, u64>>,
    /// Groups that have been registered as users of this coordinator.
    ///
    /// All groups in this set must have a `snapshot_floors` entry before any
    /// local deletion is allowed. Groups that have never taken a snapshot
    /// contribute a floor of 0, blocking all deletion.
    registered_groups: std::sync::RwLock<std::collections::HashSet<GroupId>>,
}

impl<S: Storage> CoordinatorInner<S> {
    /// Records that `group_id` has committed all WAL entries up through `wal_index`.
    ///
    /// # Panics
    ///
    /// Panics if the `committed_wal_index` lock is poisoned.
    #[allow(clippy::significant_drop_tightening)] // Guard must outlive the mutable entry.
    fn update_committed_wal_index(&self, group_id: GroupId, wal_index: u64) {
        let mut map = self
            .committed_wal_index
            .write()
            .expect("committed_wal_index poisoned");
        let entry = map.entry(group_id).or_insert(0);
        if wal_index > *entry {
            *entry = wal_index;
        }
    }

    /// Removes `segment_id` from the in-flight restore map and notifies all
    /// waiters of the outcome.
    ///
    /// Called by the designated restorer on every return path from
    /// `restore_from_s3`.  The `watch` channel ensures subscribers that arrive
    /// after this call return immediately rather than hanging.
    async fn finish_restore(
        &self,
        tx: &watch::Sender<Option<bool>>,
        segment_id: u64,
        succeeded: bool,
    ) {
        {
            let mut in_progress = self.restore_in_progress.lock().await;
            in_progress.remove(&segment_id);
        }
        // Wakes all current and future subscribers.
        let _ = tx.send(Some(succeeded));
    }
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
        let storage_clone = storage.clone();
        let wal = SharedWal::open(storage, wal_config).await?;

        let inner = Arc::new(CoordinatorInner {
            wal: Mutex::new(wal),
            buffer: Mutex::new(WriteBuffer::new()),
            flush_notify: Notify::new(),
            config,
            shutdown: AtomicBool::new(false),
            group_last_index: RwLock::new(HashMap::new()),
            storage: storage_clone,
            segment_store: Mutex::new(None),
            committed_wal_index: std::sync::RwLock::new(HashMap::new()),
            tiered_segments: std::sync::RwLock::new(HashSet::new()),
            store_prefix: std::sync::RwLock::new(String::new()),
            restore_in_progress: Mutex::new(HashMap::new()),
            snapshot_floors: std::sync::RwLock::new(HashMap::new()),
            registered_groups: std::sync::RwLock::new(std::collections::HashSet::new()),
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

    /// Sets group state from snapshot metadata (if no local WAL state).
    ///
    /// On a fresh node with a remote snapshot, the WAL has no entries for
    /// snapshot-recovered groups. This seeds `group_state` so that
    /// `read_or_load` knows the valid index range and can trigger S3
    /// fallback instead of returning `None` immediately.
    pub async fn set_group_state_if_absent(
        &self,
        group_id: GroupId,
        last_index: u64,
        last_term: u64,
    ) {
        let mut wal = self.inner.wal.lock().await;
        wal.set_group_state_if_absent(group_id, last_index, last_term);
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

    /// Deletes only the segment file (and removes from `sealed_segments`),
    /// without cleaning `evicted_index` or `group_index`.
    ///
    /// Used by `delete_tiered_local_segments()` so that `read_or_load()` still
    /// finds entries in `evicted_index` and returns `WalError::SegmentNotFound`,
    /// which the handle's read methods intercept to trigger an S3 download.
    ///
    /// # Errors
    ///
    /// Returns an error if the disk file cannot be removed.
    async fn delete_tiered_segment_file_only(
        &self,
        segment_id: crate::SegmentId,
    ) -> WalResult<()> {
        let mut wal = self.inner.wal.lock().await;
        wal.delete_segment_file_only(segment_id).await
    }

    // -------------------------------------------------------------------------
    // Tiering API
    // -------------------------------------------------------------------------

    /// Configures object-store tiering for this coordinator.
    ///
    /// Must be called before the first `process_tiering()` tick. Calling it
    /// multiple times replaces the store (useful for re-configuration in tests).
    ///
    /// `store_prefix` should end with `/`. Example:
    /// `"helix/helix-0/shared/2/"` for pool-index 2 on pod `helix-0`.
    ///
    /// # Panics
    ///
    /// Panics if `store_prefix` does not end with `/`, or if an internal lock is poisoned.
    pub async fn configure_tiering(
        &self,
        store: Arc<dyn WalSegmentStore>,
        store_prefix: String,
    ) {
        assert!(
            store_prefix.ends_with('/'),
            "store_prefix must end with '/'"
        );
        *self.inner.segment_store.lock().await = Some(store);
        *self.inner.store_prefix.write().expect("store_prefix poisoned") = store_prefix;
    }

    /// Returns `true` if object-store tiering has been configured for this coordinator.
    pub async fn has_tiering(&self) -> bool {
        self.inner.segment_store.lock().await.is_some()
    }

    /// Reports that `group_id` has committed all WAL entries up to and
    /// including `wal_index` (the auto-counter index, not the Raft index).
    ///
    /// Called by `DurablePartition` from `on_entries_committed()`.
    /// Thread-safe, non-async.
    pub fn update_committed_wal_index(&self, group_id: GroupId, wal_index: u64) {
        self.inner.update_committed_wal_index(group_id, wal_index);
    }

    /// Returns whether a sealed segment is eligible for tiering.
    ///
    /// Eligible means: every group that has entries in the segment has
    /// committed its WAL index past the last entry of that group in the segment.
    async fn is_segment_tierable(&self, segment_id: crate::SegmentId) -> bool {
        let groups = self.groups_in_segment(segment_id).await;
        if groups.is_empty() {
            // No groups found means either the segment has 0 entries (corrupt /
            // not yet recovered into evicted_index) or all its groups' entries
            // have already been cleaned from group_index/evicted_index after
            // deletion.  In either case we must NOT tier: if the segment is
            // truly empty we should never upload it; if group state is
            // temporarily unavailable we should wait until it is.
            return false;
        }
        // Snapshot committed indices so the guard is dropped before the loop.
        let committed: HashMap<GroupId, u64> = self
            .inner
            .committed_wal_index
            .read()
            .expect("committed_wal_index poisoned")
            .clone();
        for (group_id, _max_raft_idx, max_wal_counter) in &groups {
            let group_committed = committed.get(group_id).copied().unwrap_or(0);
            if group_committed < *max_wal_counter {
                return false;
            }
        }
        true
    }

    /// Uploads all eligible sealed segments to the object store.
    ///
    /// A segment is eligible when: it is sealed AND all groups that wrote
    /// entries to it have committed past those entries (via
    /// `update_committed_wal_index`).
    ///
    /// Returns the number of segments successfully uploaded.
    ///
    /// Failures are logged as warnings and retried on the next tick.
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned.
    pub async fn tier_eligible_sealed_segments(&self) -> u32 {
        let store_guard = self.inner.segment_store.lock().await;
        let store = match store_guard.as_ref() {
            Some(s) => s.clone(),
            None => return 0,
        };
        drop(store_guard); // Release lock before I/O.

        let prefix = self
            .inner
            .store_prefix
            .read()
            .expect("store_prefix poisoned")
            .clone();

        let sealed_ids = self.sealed_segment_ids().await;
        let mut uploaded: u32 = 0;

        for segment_id in sealed_ids {
            // Skip already-tiered segments.
            {
                let tiered = self
                    .inner
                    .tiered_segments
                    .read()
                    .expect("tiered_segments poisoned");
                if tiered.contains(&segment_id.get()) {
                    continue;
                }
            }

            let tierable = self.is_segment_tierable(segment_id).await;
            if !tierable {
                continue;
            }

            // Read segment bytes (holds WAL lock only briefly).
            let bytes = match self.read_segment_bytes(segment_id).await {
                Ok(b) => b,
                Err(e) => {
                    warn!(
                        segment_id = segment_id.get(),
                        error = %e,
                        "Failed to read segment bytes for tiering"
                    );
                    continue;
                }
            };

            // Guard: never upload empty or corrupt data to S3.
            //
            // A valid segment must have:
            //   1. More than the 32-byte segment header (i.e. at least one entry).
            //   2. entry_count > 0 according to segment metadata.
            //
            // The `bytes.len() <= SEGMENT_HEADER_SIZE` check catches:
            //   - Truncated files shorter than 32 bytes (disk corruption).
            //   - Files that are exactly 32 bytes (header only, 0 entries).
            //     Previously the guard used `<` which let exactly-32-byte
            //     segments through (32 < 32 = false) — fixed here to `<=`.
            //
            // The entry_count check adds a second layer: even if bytes look
            // large enough, skip segments the WAL metadata says have 0 entries.
            let entry_count = self
                .segment_info(segment_id)
                .await
                .map_or(0, |info| info.entry_count);
            if bytes.len() <= crate::segment::SEGMENT_HEADER_SIZE || entry_count == 0 {
                warn!(
                    segment_id = segment_id.get(),
                    bytes = bytes.len(),
                    entry_count,
                    "Refusing to tier empty or undersized segment (possible file corruption)"
                );
                continue;
            }

            let key = format!("{}{:08x}.wal", prefix, segment_id.get());
            match store.put(&key, bytes).await {
                Ok(()) => {
                    eprintln!(
                        "[DST-PROBE] TIERING_UPLOAD: segment={} key={}",
                        segment_id.get(),
                        key,
                    );
                    info!(
                        segment_id = segment_id.get(),
                        key = %key,
                        "Segment tiered to object store"
                    );
                    self.inner
                        .tiered_segments
                        .write()
                        .expect("tiered_segments poisoned")
                        .insert(segment_id.get());
                    uploaded += 1;
                }
                Err(e) => {
                    warn!(
                        segment_id = segment_id.get(),
                        key = %key,
                        error = %e,
                        "Failed to upload segment; will retry on next tick"
                    );
                }
            }
        }

        uploaded
    }

    /// Deletes locally-tiered segments from disk.
    ///
    /// Only deletes segments that are:
    /// 1. Already confirmed in the object store (`tiered_segments`), AND
    /// 2. Older than `min_local_age_secs` since sealing (respects local retention).
    ///
    /// Without the age gate, tiering would delete segments seconds after upload,
    /// making every consumer read older than the active segment hit S3 — even
    /// when local retention is configured to keep data for hours.
    ///
    /// Returns the number of segments deleted.
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned.
    pub async fn delete_tiered_local_segments(&self, min_local_age_secs: Option<u64>) -> u32 {
        let sealed_infos = self.sealed_segment_infos().await;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let mut deleted: u32 = 0;

        for info in sealed_infos {
            let segment_id = info.segment_id;

            let is_tiered = self
                .inner
                .tiered_segments
                .read()
                .expect("tiered_segments poisoned")
                .contains(&segment_id.get());

            if !is_tiered {
                continue;
            }

            // The segment is confirmed in S3 — the local copy is redundant.
            // A restarting node calls `download_missing_segments` which fetches
            // any needed segments from S3, and on-demand consumer reads fall
            // back to S3 transparently.  The only gate is local retention age.

            // Respect local retention: don't delete until the segment is old enough.
            // If the seal time is unknown, skip deletion to be safe.
            if let Some(min_age_secs) = min_local_age_secs {
                let age_secs = info
                    .sealed_at_secs
                    .map_or(0, |t| now_secs.saturating_sub(t));
                if age_secs < min_age_secs {
                    continue;
                }
            }

            // Use file-only deletion to preserve evicted_index so that
            // subsequent reads can transparently fall back to S3.
            match self.delete_tiered_segment_file_only(segment_id).await {
                Ok(()) => {
                    eprintln!(
                        "[DST-PROBE] LOCAL_DELETION: segment={}",
                        segment_id.get(),
                    );
                    info!(
                        segment_id = segment_id.get(),
                        "Deleted locally-tiered segment (S3 fallback read preserved)"
                    );
                    deleted += 1;
                }
                Err(e) => {
                    error!(
                        segment_id = segment_id.get(),
                        error = %e,
                        "Failed to delete tiered segment from disk"
                    );
                }
            }
        }

        deleted
    }

    /// Returns `true` if a segment has been confirmed uploaded to the object store.
    ///
    /// # Panics
    ///
    /// Panics if the `tiered_segments` lock is poisoned.
    #[must_use]
    pub fn is_tiered(&self, segment_id: u64) -> bool {
        self.inner
            .tiered_segments
            .read()
            .expect("tiered_segments poisoned")
            .contains(&segment_id)
    }

    /// Marks a segment as already tiered (used during startup recovery when
    /// re-downloading segments from the object store).
    ///
    /// # Panics
    ///
    /// Panics if the `tiered_segments` lock is poisoned.
    pub fn mark_segment_tiered(&self, segment_id: u64) {
        self.inner
            .tiered_segments
            .write()
            .expect("tiered_segments poisoned")
            .insert(segment_id);
    }

    // -------------------------------------------------------------------------
    // Snapshot floor API
    // -------------------------------------------------------------------------

    /// Registers a group as a user of this coordinator.
    ///
    /// Once registered, the group must take a snapshot before any segment that
    /// contains its entries can be deleted from local disk. Call this when a
    /// partition actor is created.
    ///
    /// # Panics
    ///
    /// Panics if the `registered_groups` lock is poisoned.
    pub fn register_group(&self, group_id: GroupId) {
        self.inner
            .registered_groups
            .write()
            .expect("registered_groups poisoned")
            .insert(group_id);
    }

    /// Deregisters a group (e.g., partition deleted or reassigned away).
    ///
    /// Removes the group from both the registered set and the snapshot floor
    /// map so it no longer participates in the `eviction_floor()` calculation.
    ///
    /// # Panics
    ///
    /// Panics if either lock is poisoned.
    pub fn deregister_group(&self, group_id: GroupId) {
        self.inner
            .registered_groups
            .write()
            .expect("registered_groups poisoned")
            .remove(&group_id);
        self.inner
            .snapshot_floors
            .write()
            .expect("snapshot_floors poisoned")
            .remove(&group_id);
    }

    /// Records the WAL index through which `group_id`'s state is captured by
    /// a durably saved snapshot.
    ///
    /// Call this after a snapshot has been saved to local disk (and optionally
    /// S3). Once all registered groups have reported a floor, segments whose
    /// `max_wal_index < eviction_floor()` become eligible for local deletion.
    ///
    /// # Panics
    ///
    /// Panics if the `snapshot_floors` lock is poisoned.
    pub fn set_snapshot_floor(&self, group_id: GroupId, wal_index: u64) {
        let mut floors = self
            .inner
            .snapshot_floors
            .write()
            .expect("snapshot_floors poisoned");
        let entry = floors.entry(group_id).or_insert(0);
        if wal_index > *entry {
            *entry = wal_index;
        }
        drop(floors);
    }

    /// Returns the minimum WAL index that must be retained across all
    /// registered groups for full crash recovery without snapshot fallback.
    ///
    /// Returns 0 if no registered group has taken a snapshot yet (nothing
    /// is safe to delete).  Returns `u64::MAX` if no groups are registered
    /// (vacuously safe — nothing to protect).
    ///
    /// Only groups that have reported a snapshot floor participate in the
    /// minimum calculation.  Groups that are registered but have never
    /// snapshotted (e.g. follower replicas that only receive entries via
    /// Raft, or brand-new partitions awaiting their first eager snapshot)
    /// are excluded so they do not pin the floor at 0 globally.  Their
    /// entries are protected by Raft replication and can be recovered from
    /// the leader or from S3.
    ///
    /// A segment can be safely deleted only when
    /// `segment.last_wal_index < eviction_floor()`.
    ///
    /// # Panics
    ///
    /// Panics if either lock is poisoned.
    #[must_use]
    pub fn eviction_floor(&self) -> u64 {
        let registered = self
            .inner
            .registered_groups
            .read()
            .expect("registered_groups poisoned");
        if registered.is_empty() {
            return u64::MAX;
        }
        let floors = self
            .inner
            .snapshot_floors
            .read()
            .expect("snapshot_floors poisoned");
        // Only consider groups that have actually taken a snapshot.
        // Groups without a floor entry (followers, newly created) are
        // excluded — their entries are replicated via Raft and stored
        // in S3, so local deletion is safe.
        registered
            .iter()
            .filter_map(|g| floors.get(g).copied())
            .min()
            .unwrap_or(0)
    }

    /// Downloads all segments from the object store that are not present locally.
    ///
    /// Called during startup before `recover()` to reconstruct WAL state on a
    /// new node that lost local disk. Each downloaded segment is placed in the
    /// WAL's local directory and marked as tiered.
    ///
    /// Returns the number of segments downloaded.
    ///
    /// # Errors
    ///
    /// Returns an error if listing the object store fails (hard error — startup
    /// cannot proceed without knowing what segments exist).
    ///
    /// # Panics
    ///
    /// Panics if an internal lock is poisoned.
    /// Downloads segments from S3 that are missing locally.
    ///
    /// When `min_raft_index > 0`, only segments whose first entry's Raft
    /// index is at or above the floor are downloaded. The floor is a Raft
    /// log index (globally consistent across nodes), not a WAL auto-counter
    /// index (node-local). This is critical because snapshots may come from
    /// a different node, and Raft indices are the only portable reference.
    ///
    /// To find the first entry's Raft index, we read 72 bytes per segment:
    /// 32-byte segment header + 40-byte `SharedEntryHeader`, where bytes
    /// 64–71 contain `raft_index`.
    ///
    /// Segments below the floor are still marked as tiered (for lazy S3
    /// reads) but not fetched.
    ///
    /// # Errors
    ///
    /// Returns an error if listing the object store fails.
    #[allow(clippy::too_many_lines)]
    pub async fn download_missing_segments(
        &self,
        min_raft_index: u64,
    ) -> WalResult<u32> {
        // Declared first to satisfy items_after_statements lint.
        const DOWNLOAD_CONCURRENCY: usize = 16;

        let store_guard = self.inner.segment_store.lock().await;
        let store = match store_guard.as_ref() {
            Some(s) => s.clone(),
            None => return Ok(0),
        };
        drop(store_guard);

        let prefix = self
            .inner
            .store_prefix
            .read()
            .expect("store_prefix poisoned")
            .clone();

        let remote_keys = store.list(&prefix).await?;

        // Parse segment IDs from remote keys.
        let mut remote_ids: Vec<u64> = remote_keys
            .iter()
            .filter_map(|k| crate::segment_store::parse_segment_id_from_key(k))
            .collect();

        if remote_ids.is_empty() {
            info!(
                prefix = %prefix,
                remote_key_count = remote_keys.len(),
                min_raft_index,
                "No remote segment IDs found in object store"
            );
            return Ok(0);
        }

        // Sort ascending so we can binary-search for the floor cutoff.
        remote_ids.sort_unstable();

        let local_ids: HashSet<u64> = {
            let wal = self.inner.wal.lock().await;
            wal.sealed_segment_ids()
                .into_iter()
                .map(super::segment::SegmentId::get)
                .collect()
        };

        // Mark ALL remote segments as tiered before downloading. Segments
        // below the floor are not downloaded but must still be marked so
        // that lazy S3 reads work for old data.
        for &segment_id in &remote_ids {
            self.mark_segment_tiered(segment_id);
        }

        // Bump the WAL's segment ID counter above the max remote segment ID.
        // Without this, a fresh node (wiped disk) would allocate new segments
        // starting from ID 1, colliding with old S3 segments that were not
        // downloaded (below the snapshot floor). The collision causes
        // restore_from_s3 to skip S3 segments it thinks are already local.
        if let Some(&max_remote) = remote_ids.last() {
            let mut wal = self.inner.wal.lock().await;
            wal.ensure_next_segment_id_above(
                crate::SegmentId::new(max_remote),
            );
        }

        // Determine which segments to download. When min_raft_index > 0,
        // read segment + entry headers (72 bytes each) to find the cutoff.
        // We read the 32-byte segment header + 40-byte SharedEntry header
        // to extract the first entry's raft_index (bytes 64–71). Raft
        // indices are globally consistent, unlike WAL auto-counter indices
        // which are node-local.
        //
        // Segment IDs are monotonically increasing, so once we find a
        // segment whose first entry's raft_index >= min_raft_index,
        // download it and everything after. We also download the segment
        // immediately before the cutoff to catch straddling entries.
        let download_ids: Vec<u64> = if min_raft_index == 0 {
            remote_ids.clone()
        } else {
            // 32-byte segment header + 40-byte SharedEntry header.
            let probe_size: u64 = (crate::segment::SEGMENT_HEADER_SIZE
                + crate::shared_entry::SHARED_ENTRY_HEADER_SIZE)
                as u64;
            let mut cutoff_idx: Option<usize> = None;

            for (i, &seg_id) in remote_ids.iter().enumerate() {
                // Skip header reads for segments already local.
                if local_ids.contains(&seg_id) {
                    continue;
                }

                let key = format!("{prefix}{seg_id:08x}.wal");
                match store.get_range(&key, 0, probe_size).await {
                    Ok(header_bytes) => {
                        let mut buf = &header_bytes[..];
                        // Decode segment header (32 bytes).
                        if let Err(e) =
                            crate::segment::SegmentHeader::decode(&mut buf)
                        {
                            warn!(
                                seg_id,
                                error = %e,
                                "Failed to decode S3 segment header; \
                                 including in download set"
                            );
                            cutoff_idx = Some(i);
                            break;
                        }
                        // Decode first SharedEntry header to get raft_index.
                        match crate::shared_entry::SharedEntryHeader::decode(
                            &mut buf, 0,
                        ) {
                            Ok(entry_hdr) => {
                                if entry_hdr.raft_index >= min_raft_index {
                                    cutoff_idx = Some(i);
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    seg_id,
                                    error = %e,
                                    "Failed to decode S3 entry header; \
                                     including in download set"
                                );
                                cutoff_idx = Some(i);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            seg_id,
                            error = %e,
                            "Failed to read S3 segment header; \
                             including in download set"
                        );
                        cutoff_idx = Some(i);
                        break;
                    }
                }
            }

            // Include one segment before the cutoff to catch entries
            // that straddle the boundary. If all segments are below the
            // floor, nothing to download.
            cutoff_idx.map_or_else(Vec::new, |idx| {
                let start = idx.saturating_sub(1);
                remote_ids[start..].to_vec()
            })
        };

        let skipped = remote_ids.len().saturating_sub(download_ids.len());

        // Download qualifying segments concurrently.
        let inner = Arc::clone(&self.inner);
        let local_ids = Arc::new(local_ids);

        // Each element: (downloaded: bool, already_local: bool).
        let results: Vec<(bool, bool)> =
            futures::stream::iter(download_ids.iter().copied())
                .map(|segment_id| {
                    let store = Arc::clone(&store);
                    let prefix = prefix.clone();
                    let local_ids = Arc::clone(&local_ids);
                    let inner = Arc::clone(&inner);
                    async move {
                        if local_ids.contains(&segment_id) {
                            return (false, true);
                        }

                        let key =
                            format!("{prefix}{segment_id:08x}.wal");
                        let bytes = match store.get(&key).await {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(
                                    segment_id,
                                    key = %key,
                                    error = %e,
                                    "Failed to download segment from \
                                     object store during startup"
                                );
                                return (false, false);
                            }
                        };

                        if bytes.len()
                            <= crate::segment::SEGMENT_HEADER_SIZE
                        {
                            warn!(
                                segment_id,
                                key = %key,
                                bytes = bytes.len(),
                                "Skipping corrupt/empty segment from \
                                 S3 (header-only or truncated)"
                            );
                            return (false, false);
                        }

                        let mut wal = inner.wal.lock().await;
                        match wal
                            .restore_segment_from_bytes(segment_id, bytes)
                            .await
                        {
                            Ok(()) => {
                                info!(
                                    segment_id,
                                    "Downloaded segment from object store"
                                );
                                (true, false)
                            }
                            Err(e) => {
                                warn!(
                                    segment_id,
                                    error = %e,
                                    "Failed to restore downloaded segment"
                                );
                                (false, false)
                            }
                        }
                    }
                })
                .buffer_unordered(DOWNLOAD_CONCURRENCY)
                .collect()
                .await;

        // Safe: segment count is bounded by max_segments_per_wal.
        #[allow(clippy::cast_possible_truncation)]
        let downloaded: u32 =
            results.iter().filter(|(d, _)| *d).count() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let already_local: u32 =
            results.iter().filter(|(_, a)| *a).count() as u32;

        info!(
            remote_count = remote_ids.len(),
            skipped_below_floor = skipped,
            already_local,
            downloaded,
            min_raft_index,
            "Downloaded missing shared WAL segments"
        );

        Ok(downloaded)
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

    /// Sets the segment configuration for all WALs.
    #[must_use]
    pub fn with_segment_config(mut self, config: crate::SegmentConfig) -> Self {
        self.coordinator_config = self.coordinator_config.with_segment_config(config);
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
            // Preserve any custom segment config (e.g., for testing) when
            // setting the per-WAL directory. Without this, SharedWalConfig::new()
            // would overwrite the segment config with defaults.
            let segment_config = config.coordinator_config.wal_config.wal_config().segment_config;
            wal_config.wal_config = SharedWalConfig::new(&wal_dir)
                .with_segment_config(segment_config);

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

    /// Seeds group state from snapshot metadata for lazy S3 reads.
    ///
    /// Routes to the coordinator owning `group_id` and sets its
    /// `group_state` if no local WAL state exists. This must be
    /// called after loading snapshot anchors and before any reads
    /// so that `read_or_load` can validate index bounds and trigger
    /// the S3 fallback path on fresh nodes.
    ///
    /// # Panics
    ///
    /// Panics if the computed coordinator index exceeds the pool size.
    pub async fn set_group_state_if_absent(
        &self,
        group_id: GroupId,
        last_index: u64,
        last_term: u64,
    ) {
        #[allow(clippy::cast_possible_truncation)]
        let wal_index = (group_id.get() % u64::from(self.wal_count)) as usize;
        assert!(wal_index < self.coordinators.len());
        self.coordinators[wal_index]
            .set_group_state_if_absent(group_id, last_index, last_term)
            .await;
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

    // -------------------------------------------------------------------------
    // Snapshot floor API (delegates to the owning coordinator per group)
    // -------------------------------------------------------------------------

    /// Registers a group as a user of its assigned coordinator.
    ///
    /// # Panics
    ///
    /// Panics if the group maps to an out-of-bounds coordinator index
    /// (cannot happen given valid `wal_count`).
    pub fn register_group(&self, group_id: GroupId) {
        #[allow(clippy::cast_possible_truncation)]
        let idx = (group_id.get() % u64::from(self.wal_count)) as usize;
        assert!(idx < self.coordinators.len());
        self.coordinators[idx].register_group(group_id);
    }

    /// Deregisters a group from its assigned coordinator.
    ///
    /// # Panics
    ///
    /// Panics if the group maps to an out-of-bounds coordinator index.
    pub fn deregister_group(&self, group_id: GroupId) {
        #[allow(clippy::cast_possible_truncation)]
        let idx = (group_id.get() % u64::from(self.wal_count)) as usize;
        assert!(idx < self.coordinators.len());
        self.coordinators[idx].deregister_group(group_id);
    }

    /// Advances the snapshot floor for `group_id` to `wal_index`.
    ///
    /// # Panics
    ///
    /// Panics if the group maps to an out-of-bounds coordinator index.
    pub fn set_snapshot_floor(&self, group_id: GroupId, wal_index: u64) {
        #[allow(clippy::cast_possible_truncation)]
        let idx = (group_id.get() % u64::from(self.wal_count)) as usize;
        assert!(idx < self.coordinators.len());
        self.coordinators[idx].set_snapshot_floor(group_id, wal_index);
    }

    // -------------------------------------------------------------------------
    // Tiering
    // -------------------------------------------------------------------------

    /// Configures object-store tiering for every coordinator in the pool.
    ///
    /// `base_prefix` is the pod-scoped prefix (e.g., `"helix/helix-0/"`).
    /// Each coordinator's full prefix will be:
    /// `"{base_prefix}shared/{pool_index}/"`.
    ///
    /// Must be called before the first `process_tiering()` tick.
    pub async fn configure_tiering(
        &self,
        store: Arc<dyn WalSegmentStore>,
        base_prefix: String,
    ) {
        for (i, coordinator) in self.coordinators.iter().enumerate() {
            let coordinator_prefix = format!("{base_prefix}shared/{i}/");
            coordinator
                .configure_tiering(store.clone(), coordinator_prefix)
                .await;
        }
    }

    /// Runs one tiering tick across all coordinators.
    ///
    /// For each coordinator:
    /// 1. Uploads eligible sealed segments to the object store.
    /// 2. Deletes locally-tiered segments that have also aged past
    ///    `local_retention_ms` (if set). Deletion preserves `evicted_index`
    ///    so reads can transparently fall back to S3 via `restore_from_s3()`.
    ///
    /// Call this periodically (e.g., every 5 seconds) from the server's
    /// background tick task.
    pub async fn process_tiering(&self, local_retention_ms: Option<u64>) {
        let min_local_age_secs = local_retention_ms.map(|ms| ms / 1000);
        for coordinator in &self.coordinators {
            coordinator.tier_eligible_sealed_segments().await;
            coordinator
                .delete_tiered_local_segments(min_local_age_secs)
                .await;
        }
    }

    /// Downloads all missing segments from the object store to local disk.
    ///
    /// Call this during startup (before `recover()`) to restore state on a
    /// new node. No-op if tiering has not been configured.
    ///
    /// # Errors
    ///
    /// Returns an error if listing the object store fails.
    /// Downloads segments from S3 that are missing locally.
    ///
    /// When `min_raft_index > 0`, only segments at or above the floor are
    /// downloaded. Segments below the floor are still marked as tiered for
    /// lazy S3 reads.
    ///
    /// # Errors
    ///
    /// Returns an error if listing the object store fails.
    pub async fn download_missing_segments(
        &self,
        min_raft_index: u64,
    ) -> WalResult<u32> {
        // Run all coordinator downloads concurrently. Each coordinator
        // manages an independent WAL directory.
        let futures = self
            .coordinators
            .iter()
            .map(|c| c.download_missing_segments(min_raft_index));
        let counts = futures::future::try_join_all(futures).await?;
        Ok(counts.iter().sum())
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

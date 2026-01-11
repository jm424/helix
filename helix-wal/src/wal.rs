//! Write-Ahead Log management.
//!
//! The [`Wal`] struct manages multiple segments, handling:
//! - Segment rotation when size limits are reached
//! - Recovery from crashes (detecting and handling torn writes)
//! - Group commit for batching fsync calls
//!
//! # File Layout
//!
//! ```text
//! /wal-dir/
//!   segment-00000001.wal   # Sealed segment
//!   segment-00000002.wal   # Sealed segment
//!   segment-00000003.wal   # Active segment (current writes)
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::entry::Entry;
use crate::error::{WalError, WalResult};
use crate::segment::{Segment, SegmentConfig, SegmentHeader, SegmentId, SEGMENT_HEADER_SIZE};
use crate::storage::{Storage, StorageFile};

/// Information about a segment for tiering decisions.
///
/// This provides metadata needed by `TieringManager` to determine
/// whether a segment is eligible for tiering to S3.
#[derive(Debug, Clone, Copy)]
pub struct SegmentInfo {
    /// Segment identifier.
    pub segment_id: SegmentId,
    /// First log index in this segment.
    pub first_index: u64,
    /// Last log index in this segment (None if empty).
    pub last_index: Option<u64>,
    /// Total size in bytes.
    pub size_bytes: u64,
    /// Number of entries.
    pub entry_count: u64,
    /// Whether the segment is sealed (no more writes).
    pub is_sealed: bool,
}

/// WAL configuration.
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL segment files.
    pub dir: PathBuf,
    /// Configuration for individual segments.
    pub segment_config: SegmentConfig,
    /// Whether to sync after every write (vs batch syncs).
    /// When false, call `sync()` explicitly for durability.
    pub sync_on_write: bool,
}

impl WalConfig {
    /// Creates a new WAL configuration.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            segment_config: SegmentConfig::new(),
            sync_on_write: false, // Default to batched syncs for performance.
        }
    }

    /// Sets the segment configuration.
    #[must_use]
    pub const fn with_segment_config(mut self, config: SegmentConfig) -> Self {
        self.segment_config = config;
        self
    }

    /// Enables sync after every write.
    #[must_use]
    pub const fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }
}

/// Write-Ahead Log.
///
/// Generic over storage backend `S` for flexibility (`tokio::fs`, `io_uring`, in-memory).
pub struct Wal<S: Storage> {
    /// Storage backend.
    storage: Arc<S>,
    /// Configuration.
    config: WalConfig,
    /// Sealed segments (`segment_id` -> segment).
    sealed_segments: BTreeMap<SegmentId, SealedSegment>,
    /// Active segment for writes.
    active_segment: Option<ActiveSegment>,
    /// Next segment ID to use.
    next_segment_id: SegmentId,
    /// First index in the WAL (for bounds checking).
    first_index: u64,
    /// Last index written (may not be durable yet).
    last_index: Option<u64>,
    /// Last index guaranteed durable (survives crash).
    /// After `sync()` succeeds, `durable_index == last_index`.
    durable_index: Option<u64>,
    /// Bytes written since last sync.
    bytes_since_sync: u64,
    /// Sealed segments that were modified by truncation but not yet synced.
    /// When non-empty, `sync()` cannot update `durable_index` because the sealed
    /// segments' synced state may have more entries than in-memory state.
    sealed_segments_pending_sync: Vec<SegmentId>,
}

/// A sealed (read-only) segment.
struct SealedSegment {
    /// Segment metadata.
    segment: Segment,
    /// Path to segment file.
    #[allow(dead_code)]
    path: PathBuf,
}

/// The active segment being written to.
struct ActiveSegment {
    /// In-memory segment data.
    segment: Segment,
    /// File handle for writes.
    file: Box<dyn StorageFile>,
    /// Path to segment file.
    path: PathBuf,
    /// Current write offset in file.
    write_offset: u64,
}

impl<S: Storage> Wal<S> {
    /// Opens or creates a WAL in the given directory.
    ///
    /// If the directory contains existing segments, they are recovered.
    /// Torn writes at the end of the last segment are detected and truncated.
    ///
    /// # Errors
    /// Returns an error if recovery fails or the directory cannot be created.
    #[allow(clippy::too_many_lines)]
    pub async fn open(storage: S, config: WalConfig) -> WalResult<Self> {
        let storage = Arc::new(storage);

        // Create directory if it doesn't exist.
        storage.create_dir_all(&config.dir).await?;

        // List existing segment files.
        let mut segment_files = storage.list_files(&config.dir, "wal").await?;
        segment_files.sort();

        let mut sealed_segments = BTreeMap::new();
        let mut next_segment_id = SegmentId::new(1);
        let mut first_index = 1u64;
        let mut last_index = None;

        // Recover existing segments.
        for path in &segment_files {
            let file = storage.open(path).await?;
            let data = file.read_all().await?;

            if data.len() < SEGMENT_HEADER_SIZE {
                warn!(?path, "Skipping empty or corrupt segment file");
                continue;
            }

            match Segment::decode(data.clone(), config.segment_config) {
                Ok(segment) => {
                    let segment_id = segment.id();
                    debug!(
                        segment_id = segment_id.get(),
                        first_index = segment.first_index(),
                        last_index = ?segment.last_index(),
                        entries = segment.entry_count(),
                        "Recovered segment"
                    );

                    // Update tracking.
                    if segment_id >= next_segment_id {
                        next_segment_id = segment_id.next();
                    }

                    if sealed_segments.is_empty() {
                        first_index = segment.first_index();
                    }

                    if let Some(idx) = segment.last_index() {
                        last_index = Some(idx);
                    }

                    sealed_segments.insert(
                        segment_id,
                        SealedSegment {
                            segment,
                            path: path.clone(),
                        },
                    );
                }
                Err(e) => {
                    warn!(?path, error = %e, "Failed to recover segment, skipping");
                }
            }
        }

        // Remove empty segments - they have a header but no entries (un-synced new segment).
        let empty_segments: Vec<SegmentId> = sealed_segments
            .iter()
            .filter(|(_, sealed)| sealed.segment.last_index().is_none())
            .map(|(id, _)| *id)
            .collect();
        for id in empty_segments {
            if let Some(sealed) = sealed_segments.remove(&id) {
                let _ = storage.remove(&sealed.path).await;
                debug!(segment_id = id.get(), "Removed empty segment");
            }
        }

        // Validate segment contiguity, fix gaps, and handle overlaps.
        // Gaps can occur after a failed truncation leaves some segment files deleted.
        // Overlaps can occur after a failed truncation leaves old segment data on disk.
        // We resolve gaps by keeping only the first contiguous run of entries.
        // We resolve overlaps by truncating older segments to not overlap with newer ones.
        let mut valid_last_index = None;
        let mut expected_next = first_index;
        let mut first_gap_id = None;
        let mut overlaps_to_fix: Vec<(SegmentId, u64)> = Vec::new(); // (segment_id, truncate_to)

        for (id, sealed) in &sealed_segments {
            let seg_first = sealed.segment.first_index();
            let seg_last = sealed.segment.last_index();

            if seg_first > expected_next {
                // Gap detected! This segment starts after a gap.
                // All segments from here on will be removed.
                if first_gap_id.is_none() {
                    warn!(
                        segment_id = id.get(),
                        expected = expected_next,
                        actual = seg_first,
                        "Gap detected in WAL - removing segment and all following"
                    );
                    first_gap_id = Some(*id);
                }
                // Don't update valid_last_index for segments after the gap
            } else if seg_first < expected_next && first_gap_id.is_none() {
                // Overlap detected! This newer segment starts before the previous one ended.
                // This happens when truncation failed to rewrite the older segment.
                // The newer segment has correct data, so we'll truncate older segments.
                warn!(
                    segment_id = id.get(),
                    seg_first,
                    expected_next,
                    "Overlap detected - newer segment overlaps with older"
                );
                // Mark all older segments that overlap for truncation.
                // They should be truncated to (seg_first - 1).
                let truncate_to = seg_first.saturating_sub(1);
                for (old_id, old_sealed) in &sealed_segments {
                    if *old_id < *id {
                        if let Some(old_last) = old_sealed.segment.last_index() {
                            if old_last >= seg_first {
                                overlaps_to_fix.push((*old_id, truncate_to));
                            }
                        }
                    }
                }
                // Update tracking with this segment's data.
                if let Some(last) = seg_last {
                    valid_last_index = Some(last);
                    expected_next = last + 1;
                }
            } else if first_gap_id.is_none() {
                // Normal case: segment continues from expected_next
                if let Some(last) = seg_last {
                    valid_last_index = Some(last);
                    expected_next = last + 1;
                }
            }
        }

        // Fix overlapping segments by truncating them.
        // We truncate both in-memory AND on-disk to ensure recovery is idempotent.
        let had_overlaps = !overlaps_to_fix.is_empty();
        for (seg_id, truncate_to) in overlaps_to_fix {
            if let Some(sealed) = sealed_segments.get_mut(&seg_id) {
                let _ = sealed.segment.truncate_entries_after(truncate_to);

                // IMPORTANT: Also rewrite the segment file on disk.
                // Otherwise, the synced file still has the old (overlapping) data,
                // and future recovery could see inconsistent state.
                let data = sealed.segment.encode();
                if let Ok(file) = storage.open(&sealed.path).await {
                    let _ = file.truncate(0).await;
                    if file.write_at(0, &data).await.is_ok() {
                        let _ = file.sync().await;
                    }
                }

                info!(
                    segment_id = seg_id.get(),
                    truncate_to,
                    "Truncated overlapping segment"
                );
            }
        }

        // Update last_index if we fixed overlaps (valid_last_index tracks correct value).
        if had_overlaps && first_gap_id.is_none() {
            last_index = valid_last_index;
        }

        // If we found gaps, remove the discontinuous segments.
        if let Some(gap_id) = first_gap_id {
            // Remove all segments with ID >= the first gap ID.
            let all_to_remove: Vec<SegmentId> = sealed_segments
                .keys()
                .filter(|id| **id >= gap_id)
                .copied()
                .collect();

            for id in all_to_remove {
                if let Some(sealed) = sealed_segments.remove(&id) {
                    // Best-effort file cleanup
                    let _ = storage.remove(&sealed.path).await;
                }
            }

            last_index = valid_last_index;
            info!(
                ?last_index,
                "Truncated WAL to last contiguous entry after gap detection"
            );
        }

        info!(
            segments = sealed_segments.len(),
            ?first_index,
            ?last_index,
            "WAL recovery complete"
        );

        Ok(Self {
            storage,
            config,
            sealed_segments,
            active_segment: None,
            next_segment_id,
            first_index,
            last_index,
            durable_index: last_index, // Recovered state is durable (survived crash)
            bytes_since_sync: 0,
            sealed_segments_pending_sync: Vec::new(),
        })
    }

    /// Returns the first index in the WAL.
    #[must_use]
    pub const fn first_index(&self) -> u64 {
        self.first_index
    }

    /// Returns the last index written to the WAL, or None if empty.
    ///
    /// This index may not be durable yet. Use `durable_index()` to get the
    /// last index guaranteed to survive a crash.
    #[must_use]
    pub const fn last_index(&self) -> Option<u64> {
        self.last_index
    }

    /// Returns the last index guaranteed to be durable (survives crash).
    ///
    /// After `sync()` succeeds, `durable_index() == last_index()`.
    /// Entries in the range `(durable_index, last_index]` may be lost on crash.
    ///
    /// After recovery, `durable_index() == last_index()` because only
    /// durable data survives.
    #[must_use]
    pub const fn durable_index(&self) -> Option<u64> {
        self.durable_index
    }

    /// Returns true if the WAL is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.last_index.is_none()
    }

    /// Appends an entry to the WAL.
    ///
    /// The entry is written to the active segment. If `sync_on_write` is enabled,
    /// the entry is synced to disk immediately. Otherwise, call `sync()` to
    /// ensure durability.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    ///
    /// # Panics
    /// Panics if `ensure_active_segment` fails to create an active segment
    /// (should not happen in normal operation).
    pub async fn append(&mut self, entry: Entry) -> WalResult<u64> {
        let index = entry.index();
        let entry_size = entry.total_size();

        // Ensure we have an active segment with space.
        self.ensure_active_segment(entry.payload_len()).await?;

        let active = self
            .active_segment
            .as_mut()
            .expect("active segment should exist after ensure_active_segment");

        // Encode entry to bytes.
        // Safe cast: entry size bounded by limits::ENTRY_PAYLOAD_SIZE_BYTES_MAX.
        #[allow(clippy::cast_possible_truncation)]
        let mut buf = bytes::BytesMut::with_capacity(entry_size as usize);
        entry.encode(&mut buf);
        let data = buf.freeze();

        // Write to file.
        active.file.write_at(active.write_offset, &data).await?;
        active.write_offset += entry_size;

        // Update in-memory segment.
        active.segment.append(entry)?;

        // Update WAL state.
        self.last_index = Some(index);
        self.bytes_since_sync += entry_size;

        // Sync if configured.
        if self.config.sync_on_write {
            self.sync().await?;
        }

        debug!(index, bytes = entry_size, "Appended entry");
        Ok(index)
    }

    /// Syncs all buffered writes to disk.
    ///
    /// Call this periodically for group commit, or rely on `sync_on_write`
    /// for per-entry durability.
    ///
    /// # Errors
    /// Returns an error if the sync fails.
    pub async fn sync(&mut self) -> WalResult<()> {
        // First, try to sync any sealed segments that were modified by truncation.
        // Track the maximum index in still-pending segments for durable_index calculation.
        let mut still_pending = Vec::new();
        #[allow(unused_variables)]
        let mut max_pending_synced_index: Option<u64> = None;

        for segment_id in std::mem::take(&mut self.sealed_segments_pending_sync) {
            if let Some(sealed) = self.sealed_segments.get(&segment_id) {
                let data = sealed.segment.encode();
                let mut sync_ok = false;
                if let Ok(file) = self.storage.open(&sealed.path).await {
                    // IMPORTANT: Only sync if write succeeded, otherwise we'd sync a 0-byte file.
                    let _ = file.truncate(0).await;
                    if file.write_at(0, &data).await.is_ok() && file.sync().await.is_ok() {
                        sync_ok = true;
                    }
                }
                if !sync_ok {
                    still_pending.push(segment_id);
                    // This segment's synced_files might have MORE entries than in-memory.
                    // We don't know exactly how many, but we know the segment existed before
                    // truncation, so its synced content could be up to any previous state.
                    // For safety, we can't claim any index beyond this segment's current
                    // in-memory last_index is durable, because on crash we might recover
                    // extra entries from this segment's synced state.
                    if let Some(seg_last) = sealed.segment.last_index() {
                        max_pending_synced_index = Some(
                            max_pending_synced_index.map_or(seg_last, |m| m.max(seg_last)),
                        );
                    }
                }
            }
            // If segment was removed, it's no longer pending.
        }
        self.sealed_segments_pending_sync = still_pending;

        // Sync the active segment.
        if let Some(active) = &self.active_segment {
            let result = active.file.sync().await;
            debug!(
                bytes = self.bytes_since_sync,
                success = result.is_ok(),
                "Syncing active segment"
            );
            result?;
        }
        self.bytes_since_sync = 0;

        // Update durable_index based on what we can guarantee:
        // - If no pending sealed segments, we can claim last_index is durable.
        // - If there are pending sealed segments, their synced state might have
        //   extra entries that would be recovered on crash. In this case, we
        //   can't safely lower durable_index below what it was, but we CAN
        //   raise it if we successfully synced the active segment with newer entries.
        //
        // The invariant we maintain: recovered_last_index <= durable_index
        // On crash, recovery might see MORE entries from pending sealed segments'
        // synced state. So durable_index should be at least as high as whatever
        // the pending segments' synced state might contain.
        //
        // Since we don't know exactly what's in synced_files for pending segments,
        // we keep durable_index at max(old_value, last_index) to be safe.
        if self.sealed_segments_pending_sync.is_empty() {
            self.durable_index = self.last_index;
        } else if let Some(last) = self.last_index {
            // There are pending segments, but we synced the active segment.
            // We can raise durable_index to last_index if it's higher than current.
            self.durable_index = Some(self.durable_index.map_or(last, |d| d.max(last)));
        }
        Ok(())
    }

    /// Reads an entry by index.
    ///
    /// # Errors
    /// Returns an error if the index is out of bounds or the read fails.
    pub fn read(&self, index: u64) -> WalResult<&Entry> {
        // Check bounds.
        let last = self.last_index.ok_or(WalError::IndexOutOfBounds {
            index,
            first: self.first_index,
            last: self.first_index,
        })?;

        if index < self.first_index || index > last {
            return Err(WalError::IndexOutOfBounds {
                index,
                first: self.first_index,
                last,
            });
        }

        // Check active segment first.
        if let Some(active) = &self.active_segment {
            if index >= active.segment.first_index() {
                return active.segment.read(index);
            }
        }

        // Search sealed segments.
        for sealed in self.sealed_segments.values().rev() {
            if index >= sealed.segment.first_index() {
                if let Ok(entry) = sealed.segment.read(index) {
                    return Ok(entry);
                }
            }
        }

        Err(WalError::IndexOutOfBounds {
            index,
            first: self.first_index,
            last,
        })
    }

    /// Truncates all entries after the given index.
    ///
    /// Entries with index > `last_index_to_keep` are removed. This handles
    /// truncation across segment boundaries by:
    /// 1. Removing sealed segments entirely after the truncation point
    /// 2. Removing the active segment if it starts after the truncation point
    /// 3. Truncating entries within the active segment if it contains the point
    ///
    /// # Errors
    /// Returns an error if the truncation fails. Note: partial truncation may leave
    /// gaps that are resolved on next recovery.
    pub async fn truncate_after(&mut self, last_index_to_keep: u64) -> WalResult<()> {
        // Update last_index FIRST to ensure consistency.
        // Even if file operations fail, the in-memory state will be correct.
        self.last_index = if last_index_to_keep >= self.first_index {
            Some(last_index_to_keep)
        } else {
            None
        };

        // Track whether truncation was synced to disk.
        // We only update durable_index if all syncs succeed, because if sync fails,
        // the old (pre-truncation) data may still be in synced_files and would be
        // recovered on crash.
        let mut truncation_synced = true;

        // Identify what needs to be done.
        let remove_active = self
            .active_segment
            .as_ref()
            .is_some_and(|active| last_index_to_keep < active.segment.first_index());

        let truncate_active = !remove_active && self.active_segment.is_some();

        // Handle active segment (most likely to have recent data).
        if remove_active {
            if let Some(active) = self.active_segment.take() {
                // Remove file, ignore errors (best-effort cleanup).
                let _ = self.storage.remove(&active.path).await;
                debug!(
                    segment_first = active.segment.first_index(),
                    "Removed active segment (truncation before its start)"
                );
            }
        } else if truncate_active {
            if let Some(active) = &mut self.active_segment {
                active.segment.truncate_after(last_index_to_keep)?;

                // Re-encode and rewrite the segment file.
                // Best-effort: file operations may fail but in-memory state is correct.
                // IMPORTANT: Only sync if write succeeded, otherwise we'd sync a 0-byte file.
                let data = active.segment.encode();
                let _ = active.file.truncate(0).await;
                if active.file.write_at(0, &data).await.is_ok() {
                    active.write_offset = data.len() as u64;
                    if active.file.sync().await.is_err() {
                        truncation_synced = false;
                    }
                } else {
                    truncation_synced = false;
                }
            }
        }

        // Collect segments to remove (after handling active to maintain consistency).
        let segments_to_remove: Vec<SegmentId> = self
            .sealed_segments
            .iter()
            .filter(|(_, seg)| seg.segment.first_index() > last_index_to_keep)
            .map(|(id, _)| *id)
            .collect();

        // Remove sealed segments. For each, remove both in-memory and file.
        // Ignore file removal errors (best-effort cleanup).
        for segment_id in segments_to_remove {
            if let Some(sealed) = self.sealed_segments.remove(&segment_id) {
                let _ = self.storage.remove(&sealed.path).await;
                debug!(
                    segment_id = segment_id.get(),
                    first_index = sealed.segment.first_index(),
                    "Removed sealed segment (truncation before its start)"
                );
            }
        }

        // Handle sealed segments that contain entries beyond the truncation point.
        // We need to truncate them both in-memory AND on disk, otherwise recovery
        // will reload the stale entries.
        // First, collect segment IDs that need truncation to avoid borrow issues.
        let segments_to_truncate: Vec<SegmentId> = self
            .sealed_segments
            .iter()
            .filter(|(_, seg)| seg.segment.last_index().is_some_and(|l| l > last_index_to_keep))
            .map(|(id, _)| *id)
            .collect();

        for segment_id in segments_to_truncate {
            if let Some(sealed) = self.sealed_segments.get_mut(&segment_id) {
                // Clone segment and truncate the clone to generate truncated data.
                // We DON'T modify the original yet - only after sync succeeds.
                // This ensures in-memory state stays consistent with on-disk state.
                let mut truncated_segment = sealed.segment.clone();
                let _ = truncated_segment.truncate_entries_after(last_index_to_keep);
                let data = truncated_segment.encode();

                // Best-effort: try to rewrite the segment file with truncated data.
                // If this fails, add to pending list so sync() will retry.
                // IMPORTANT: Only sync if write succeeded, otherwise we'd sync a 0-byte file.
                // IMPORTANT: Check truncate result - if truncate fails, write_at might
                // append data instead of replacing, leaving stale trailing data.
                let mut sync_succeeded = false;
                if let Ok(file) = self.storage.open(&sealed.path).await {
                    if file.truncate(0).await.is_ok()
                        && file.write_at(0, &data).await.is_ok()
                        && file.sync().await.is_ok()
                    {
                        sync_succeeded = true;
                    }
                }

                // Only modify in-memory state if sync succeeded.
                // This ensures segment stays in segments_to_truncate for retry.
                if sync_succeeded {
                    let _ = sealed.segment.truncate_entries_after(last_index_to_keep);
                } else {
                    truncation_synced = false;
                    self.sealed_segments_pending_sync.push(segment_id);
                }

                debug!(
                    segment_id = segment_id.get(),
                    last_index_to_keep,
                    "Truncated sealed segment (in-memory and on disk)"
                );
            }
        }

        // Only update durable_index if truncation was synced to disk.
        // If sync failed, the old (pre-truncation) data may still be in synced_files
        // and could be recovered on crash. Keeping the old durable_index is safe:
        // the invariant "recovered <= durable_before" will still hold.
        if truncation_synced {
            self.durable_index = self.last_index;
        }

        debug!(last_index_to_keep, "Truncated WAL");
        Ok(())
    }

    /// Ensures there's an active segment with space for the given payload.
    async fn ensure_active_segment(&mut self, payload_size: u32) -> WalResult<()> {
        // Check if current active segment has space.
        if let Some(active) = &self.active_segment {
            if active.segment.has_space_for(payload_size) {
                return Ok(());
            }

            // Seal current segment and rotate.
            self.rotate_segment().await?;
        }

        // Create new active segment.
        let segment_id = self.next_segment_id;
        self.next_segment_id = segment_id.next();

        let first_index = self.last_index.map_or(self.first_index, |i| i + 1);
        let path = self.segment_path(segment_id);

        let file = self.storage.open(&path).await?;

        // Truncate the file in case it already exists with old data.
        // This is critical for crash safety: if we're creating a new segment after
        // recovery found nothing (or skipped a corrupt segment), the file might still
        // have old data that would be recovered on the next crash.
        file.truncate(0).await?;

        // Create segment and write header.
        let segment = Segment::new(segment_id, first_index, self.config.segment_config);
        let header_data = {
            let mut buf = bytes::BytesMut::with_capacity(SEGMENT_HEADER_SIZE);
            SegmentHeader::new(segment_id, first_index).encode(&mut buf);
            buf.freeze()
        };

        file.write_at(0, &header_data).await?;

        self.active_segment = Some(ActiveSegment {
            segment,
            file,
            path,
            write_offset: SEGMENT_HEADER_SIZE as u64,
        });

        info!(segment_id = segment_id.get(), first_index, "Created new segment");
        Ok(())
    }

    /// Rotates the current active segment to sealed.
    async fn rotate_segment(&mut self) -> WalResult<()> {
        if let Some(mut active) = self.active_segment.take() {
            // Sync before sealing - this makes the segment's contents durable.
            active.file.sync().await?;

            // Update durable_index since we just synced.
            self.durable_index = self.last_index;

            active.segment.seal();

            let segment_id = active.segment.id();
            info!(
                segment_id = segment_id.get(),
                entries = active.segment.entry_count(),
                "Sealed segment"
            );

            self.sealed_segments.insert(
                segment_id,
                SealedSegment {
                    segment: active.segment,
                    path: active.path,
                },
            );
        }
        Ok(())
    }

    /// Returns the path for a segment file.
    fn segment_path(&self, segment_id: SegmentId) -> PathBuf {
        self.config
            .dir
            .join(format!("segment-{:08x}.wal", segment_id.get()))
    }

    // -------------------------------------------------------------------------
    // Tiering Support
    // -------------------------------------------------------------------------
    // These methods provide segment-level access for the TieringManager to
    // upload sealed segments to S3.

    /// Returns the IDs of all sealed segments.
    ///
    /// Sealed segments are immutable and eligible for tiering to S3.
    /// The active segment (if any) is not included.
    #[must_use]
    pub fn sealed_segment_ids(&self) -> Vec<SegmentId> {
        self.sealed_segments.keys().copied().collect()
    }

    /// Returns the number of sealed segments.
    #[must_use]
    pub fn sealed_segment_count(&self) -> u32 {
        // TigerStyle: Use u32, not usize.
        #[allow(clippy::cast_possible_truncation)]
        let count = self.sealed_segments.len() as u32;
        count
    }

    /// Returns information about a sealed segment.
    ///
    /// Returns `None` if the segment doesn't exist or is the active segment.
    #[must_use]
    pub fn segment_info(&self, segment_id: SegmentId) -> Option<SegmentInfo> {
        self.sealed_segments.get(&segment_id).map(|sealed| {
            let segment = &sealed.segment;
            SegmentInfo {
                segment_id,
                first_index: segment.first_index(),
                last_index: segment.last_index(),
                size_bytes: segment.size_bytes(),
                entry_count: segment.entry_count(),
                is_sealed: segment.is_sealed(),
            }
        })
    }

    /// Reads the raw bytes of a sealed segment for tiering to S3.
    ///
    /// This encodes the entire segment (header + entries) as bytes that can
    /// be uploaded to object storage and later decoded with `Segment::decode`.
    ///
    /// # Errors
    ///
    /// Returns `WalError::SegmentNotFound` if the segment doesn't exist.
    ///
    /// # Panics
    ///
    /// Panics if the segment exists but is not sealed (invariant violation).
    pub fn read_segment_bytes(&self, segment_id: SegmentId) -> WalResult<bytes::Bytes> {
        let sealed = self.sealed_segments.get(&segment_id).ok_or_else(|| {
            WalError::SegmentNotFound {
                segment_id: segment_id.get(),
            }
        })?;

        // TigerStyle: Assert preconditions.
        assert!(
            sealed.segment.is_sealed(),
            "segment must be sealed for tiering"
        );

        Ok(sealed.segment.encode())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::storage::TokioStorage;

    #[tokio::test]
    async fn test_wal_append_and_read() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig::new(temp_dir.path());

        let mut wal = Wal::open(TokioStorage::new(), config).await.unwrap();
        assert!(wal.is_empty());

        // Append entries.
        for i in 1..=5 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            let idx = wal.append(entry).await.unwrap();
            assert_eq!(idx, i);
        }

        // Sync to ensure durability.
        wal.sync().await.unwrap();

        // Read entries.
        for i in 1..=5 {
            let entry = wal.read(i).unwrap();
            assert_eq!(entry.index(), i);
            assert_eq!(entry.term(), 1);
        }

        assert_eq!(wal.first_index(), 1);
        assert_eq!(wal.last_index(), Some(5));
    }

    #[tokio::test]
    async fn test_wal_recovery() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig::new(temp_dir.path());

        // Write some entries.
        {
            let mut wal = Wal::open(TokioStorage::new(), config.clone()).await.unwrap();
            for i in 1..=10 {
                let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
                wal.append(entry).await.unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Reopen and verify recovery.
        {
            let wal = Wal::open(TokioStorage::new(), config).await.unwrap();
            assert_eq!(wal.first_index(), 1);
            // Note: After recovery, entries are in sealed segments.
            // The last_index is tracked from recovery.
            assert_eq!(wal.last_index(), Some(10));
        }
    }

    #[tokio::test]
    async fn test_wal_truncate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig::new(temp_dir.path());

        let mut wal = Wal::open(TokioStorage::new(), config).await.unwrap();

        // Append entries.
        for i in 1..=10 {
            let entry = Entry::new(1, i, Bytes::from("x")).unwrap();
            wal.append(entry).await.unwrap();
        }

        // Truncate after index 5.
        wal.truncate_after(5).await.unwrap();

        assert_eq!(wal.last_index(), Some(5));

        // Reading index 6 should fail.
        assert!(wal.read(6).is_err());

        // Reading index 5 should succeed.
        assert!(wal.read(5).is_ok());
    }

    #[tokio::test]
    async fn test_wal_durable_index() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = WalConfig::new(temp_dir.path());

        let mut wal = Wal::open(TokioStorage::new(), config.clone()).await.unwrap();

        // Initially empty - both indices are None
        assert_eq!(wal.last_index(), None);
        assert_eq!(wal.durable_index(), None);

        // After append, last_index advances but durable_index stays behind
        let entry1 = Entry::new(1, 1, Bytes::from("a")).unwrap();
        wal.append(entry1).await.unwrap();
        assert_eq!(wal.last_index(), Some(1));
        assert_eq!(wal.durable_index(), None); // Not synced yet

        let entry2 = Entry::new(1, 2, Bytes::from("b")).unwrap();
        wal.append(entry2).await.unwrap();
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(wal.durable_index(), None); // Still not synced

        // After sync, durable_index catches up
        wal.sync().await.unwrap();
        assert_eq!(wal.last_index(), Some(2));
        assert_eq!(wal.durable_index(), Some(2)); // Now durable

        // More writes without sync
        let entry3 = Entry::new(1, 3, Bytes::from("c")).unwrap();
        wal.append(entry3).await.unwrap();
        assert_eq!(wal.last_index(), Some(3));
        assert_eq!(wal.durable_index(), Some(2)); // Lags behind

        // Truncate below durable_index - durable_index updated to truncation point
        wal.truncate_after(1).await.unwrap();
        assert_eq!(wal.last_index(), Some(1));
        assert_eq!(wal.durable_index(), Some(1)); // Updated by truncation

        // After recovery, durable_index == last_index
        drop(wal);
        let wal = Wal::open(TokioStorage::new(), config).await.unwrap();
        assert_eq!(wal.last_index(), wal.durable_index());
    }
}

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
    /// Last index in the WAL.
    last_index: Option<u64>,
    /// Bytes written since last sync.
    bytes_since_sync: u64,
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

            match Segment::decode(data, config.segment_config) {
                Ok(segment) => {
                    let segment_id = segment.id();
                    info!(
                        segment_id = segment_id.get(),
                        first_index = segment.first_index(),
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
            bytes_since_sync: 0,
        })
    }

    /// Returns the first index in the WAL.
    #[must_use]
    pub const fn first_index(&self) -> u64 {
        self.first_index
    }

    /// Returns the last index in the WAL, or None if empty.
    #[must_use]
    pub const fn last_index(&self) -> Option<u64> {
        self.last_index
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
        if let Some(active) = &self.active_segment {
            active.file.sync().await?;
            debug!(bytes = self.bytes_since_sync, "Synced WAL");
        }
        self.bytes_since_sync = 0;
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
    /// Entries with index > `last_index_to_keep` are removed.
    ///
    /// # Errors
    /// Returns an error if the truncation fails.
    pub async fn truncate_after(&mut self, last_index_to_keep: u64) -> WalResult<()> {
        // Truncate active segment.
        if let Some(active) = &mut self.active_segment {
            active.segment.truncate_after(last_index_to_keep)?;

            // Re-encode and rewrite the segment file.
            let data = active.segment.encode();
            active.file.truncate(0).await?;
            active.file.write_at(0, &data).await?;
            active.write_offset = data.len() as u64;
            active.file.sync().await?;
        }

        // Update last_index.
        self.last_index = if last_index_to_keep >= self.first_index {
            Some(last_index_to_keep)
        } else {
            None
        };

        // TODO: Handle truncation across segment boundaries.
        // For now, we assume truncation only affects the active segment.

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
            // Sync before sealing.
            active.file.sync().await?;
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
}

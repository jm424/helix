//! Tiering manager for coordinating segment upload/download.
//!
//! The `TieringManager` orchestrates movement of WAL segments between local
//! storage and S3, tracking metadata and enforcing eligibility policies.

use std::sync::Arc;

use bloodhound::buggify;
use bytes::Bytes;
use helix_core::Offset;
use helix_wal::SegmentId;
use tracing::{debug, info, warn};

use crate::error::{TierError, TierResult};
use crate::metadata::{MetadataStore, SegmentLocation, SegmentMetadata};
use crate::storage::{ObjectKey, ObjectStorage, SegmentReader};

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

/// Maximum number of segments to tier in a single batch.
/// `TigerStyle`: explicit limit on batch size.
const MAX_TIER_BATCH_SIZE: u32 = 100;

/// Configuration for the tiering manager.
#[derive(Debug, Clone)]
pub struct TieringConfig {
    /// Minimum age in seconds before a segment can be tiered.
    pub min_age_secs: u64,
    /// Maximum number of concurrent uploads.
    pub max_concurrent_uploads: u32,
    /// Whether to verify CRC after download.
    pub verify_on_download: bool,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            min_age_secs: 3600, // 1 hour.
            max_concurrent_uploads: 4,
            verify_on_download: true,
        }
    }
}

impl TieringConfig {
    /// Creates a config with no minimum age (for testing).
    #[must_use]
    pub const fn for_testing() -> Self {
        Self {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        }
    }
}

// -----------------------------------------------------------------------------
// TieringManager
// -----------------------------------------------------------------------------

/// Tiering manager orchestrates segment movement between local and S3.
///
/// # Design
///
/// The manager maintains metadata about segment locations and enforces
/// eligibility policies before upload or eviction. It does not perform
/// background work autonomously - the caller must invoke methods explicitly.
pub struct TieringManager<S: ObjectStorage, M: MetadataStore> {
    /// Object storage backend.
    storage: Arc<S>,
    /// Metadata store.
    metadata: Arc<M>,
    /// Configuration.
    config: TieringConfig,
}

impl<S: ObjectStorage, M: MetadataStore> TieringManager<S, M> {
    /// Creates a new tiering manager.
    #[must_use]
    pub fn new(storage: S, metadata: M, config: TieringConfig) -> Self {
        Self {
            storage: Arc::new(storage),
            metadata: Arc::new(metadata),
            config,
        }
    }

    /// Returns a reference to the storage backend.
    #[must_use]
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns a reference to the metadata store.
    #[must_use]
    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    /// Registers a segment with the metadata store.
    ///
    /// Called when a new segment is created.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be persisted.
    ///
    /// # Panics
    ///
    /// Panics if the segment location is not `Local`.
    pub async fn register_segment(&self, metadata: SegmentMetadata) -> TierResult<()> {
        // Precondition: segment should be local and not sealed.
        assert!(
            metadata.location == SegmentLocation::Local,
            "new segment must be local"
        );

        debug!(
            segment_id = metadata.segment_id.get(),
            topic_id = metadata.topic_id.get(),
            partition_id = metadata.partition_id.get(),
            "Registering segment"
        );

        self.metadata.set(metadata).await
    }

    /// Marks a segment as sealed.
    ///
    /// Called when the WAL rotates a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found or metadata update fails.
    pub async fn mark_sealed(&self, segment_id: SegmentId) -> TierResult<()> {
        let mut metadata = self.get_metadata_or_error(segment_id).await?;

        // Precondition: segment must not already be sealed.
        if metadata.sealed {
            warn!(segment_id = segment_id.get(), "Segment already sealed");
            return Ok(());
        }

        metadata.mark_sealed();
        self.metadata.set(metadata).await?;

        debug!(segment_id = segment_id.get(), "Segment marked as sealed");
        Ok(())
    }

    /// Marks a segment as committed.
    ///
    /// Called when all entries in the segment are committed through Raft.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found or metadata update fails.
    pub async fn mark_committed(&self, segment_id: SegmentId) -> TierResult<()> {
        let mut metadata = self.get_metadata_or_error(segment_id).await?;

        // Precondition: segment must not already be committed.
        if metadata.committed {
            warn!(segment_id = segment_id.get(), "Segment already committed");
            return Ok(());
        }

        // Precondition: segment must be sealed before it can be committed.
        if !metadata.sealed {
            return Err(TierError::NotEligible {
                segment_id: segment_id.get(),
                reason: "segment must be sealed before committing",
            });
        }

        metadata.mark_committed();
        self.metadata.set(metadata).await?;

        debug!(segment_id = segment_id.get(), "Segment marked as committed");
        Ok(())
    }

    /// Uploads a segment to S3.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to upload.
    /// * `data` - The segment data (from WAL).
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not eligible or upload fails.
    ///
    /// # Panics
    ///
    /// Panics if `data` is empty.
    pub async fn upload_segment(&self, segment_id: SegmentId, data: Bytes) -> TierResult<()> {
        // Precondition: data must not be empty.
        assert!(!data.is_empty(), "segment data must not be empty");

        // Get metadata to determine the key (before claiming).
        let metadata = self.get_metadata_or_error(segment_id).await?;

        let key = ObjectKey::from_segment(
            metadata.topic_id.get(),
            metadata.partition_id.get(),
            segment_id.get(),
        );

        // Atomically claim the segment for upload.
        // This prevents race conditions with concurrent uploads.
        let claimed = self.metadata.try_claim_for_upload(segment_id).await?;
        if !claimed {
            return Err(TierError::NotEligible {
                segment_id: segment_id.get(),
                reason: "segment already uploaded",
            });
        }

        info!(
            segment_id = segment_id.get(),
            key = %key,
            size_bytes = data.len(),
            "Uploading segment to S3"
        );

        // BUGGIFY: Simulate crash after claiming but before upload.
        // This leaves the segment stuck in Uploading state - a real bug that
        // DST should find. When this triggers, we return an error WITHOUT
        // calling abort_upload, simulating a process crash.
        if buggify!("tiering_crash_after_claim", 0.1) {
            warn!(
                segment_id = segment_id.get(),
                "BUGGIFY: Simulating crash after claim, segment left in Uploading state"
            );
            return Err(TierError::Io {
                operation: "upload_segment",
                message: "BUGGIFY: simulated crash after claim".into(),
            });
        }

        // Upload to S3.
        let upload_result = self.storage.put(&key, data).await;

        // Handle upload result.
        match upload_result {
            Ok(()) => {
                // BUGGIFY: Simulate crash after upload succeeded but before metadata update.
                // Data is in S3 but metadata still says Uploading.
                if buggify!("tiering_crash_after_upload_before_complete", 0.1) {
                    warn!(
                        segment_id = segment_id.get(),
                        "BUGGIFY: Crash after upload, before complete_upload"
                    );
                    return Err(TierError::Io {
                        operation: "upload_segment",
                        message: "BUGGIFY: simulated crash after upload".into(),
                    });
                }

                // Success: complete the upload.
                self.metadata.complete_upload(segment_id, key).await?;
                info!(segment_id = segment_id.get(), "Segment uploaded successfully");
                Ok(())
            }
            Err(e) => {
                // BUGGIFY: Simulate crash during error handling, before abort_upload.
                // Upload failed but we don't get to revert the state.
                if buggify!("tiering_crash_before_abort", 0.1) {
                    warn!(
                        segment_id = segment_id.get(),
                        "BUGGIFY: Crash before abort_upload"
                    );
                    return Err(TierError::Io {
                        operation: "upload_segment",
                        message: "BUGGIFY: simulated crash before abort".into(),
                    });
                }

                // Failure: abort the upload (revert to Local).
                warn!(
                    segment_id = segment_id.get(),
                    error = %e,
                    "Upload failed, reverting claim"
                );
                self.metadata.abort_upload(segment_id).await?;
                Err(e)
            }
        }
    }

    /// Downloads a segment from S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not in S3 or download fails.
    ///
    /// # Panics
    ///
    /// Panics if the downloaded data is empty.
    pub async fn download_segment(&self, segment_id: SegmentId) -> TierResult<Bytes> {
        let metadata = self.get_metadata_or_error(segment_id).await?;

        // Precondition: segment must have a remote key.
        let key = metadata.remote_key.as_ref().ok_or_else(|| TierError::NotFound {
            key: format!("segment-{segment_id} has no remote key"),
        })?;

        debug!(
            segment_id = segment_id.get(),
            key = %key,
            "Downloading segment from S3"
        );

        let data = self.storage.get(key).await?;

        // Postcondition: downloaded data must not be empty.
        assert!(!data.is_empty(), "downloaded segment must not be empty");

        debug!(
            segment_id = segment_id.get(),
            size_bytes = data.len(),
            "Segment downloaded"
        );

        Ok(data)
    }

    /// Gets the location of a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read.
    pub async fn get_location(&self, segment_id: SegmentId) -> TierResult<Option<SegmentLocation>> {
        Ok(self.metadata.get(segment_id).await?.map(|m| m.location))
    }

    /// Checks if a segment exists in S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails.
    pub async fn exists_in_s3(&self, segment_id: SegmentId) -> TierResult<bool> {
        let Some(metadata) = self.metadata.get(segment_id).await? else {
            return Ok(false);
        };

        match &metadata.remote_key {
            Some(key) => self.storage.exists(key).await,
            None => Ok(false),
        }
    }

    /// Recovers segments stuck in `Uploading` state after a crash.
    ///
    /// This method should be called on startup to recover from incomplete uploads.
    /// For each stuck segment:
    /// - If data exists in S3 → complete the upload (Both)
    /// - If data doesn't exist in S3 → abort the upload (Local)
    ///
    /// Returns the number of segments recovered.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub async fn recover_stuck_uploads(&self) -> TierResult<u32> {
        let stuck = self.metadata.find_stuck_uploads().await?;
        let mut recovered_count = 0u32;

        for metadata in stuck {
            let segment_id = metadata.segment_id;
            let key = ObjectKey::from_segment(
                metadata.topic_id.get(),
                metadata.partition_id.get(),
                segment_id.get(),
            );

            // Check if data actually made it to S3.
            // IMPORTANT: If exists() fails, we skip this segment rather than
            // assuming data is not in S3. Incorrectly aborting would create
            // orphaned data in S3.
            let exists_in_s3 = match self.storage.exists(&key).await {
                Ok(exists) => exists,
                Err(e) => {
                    warn!(
                        segment_id = segment_id.get(),
                        error = %e,
                        "Cannot determine if segment data exists in S3, skipping recovery"
                    );
                    continue; // Skip this segment, try again on next recovery
                }
            };

            if exists_in_s3 {
                // Data is in S3 - complete the upload.
                info!(
                    segment_id = segment_id.get(),
                    "Recovering stuck upload: data exists in S3, completing"
                );
                self.metadata.complete_upload(segment_id, key).await?;
            } else {
                // Data is not in S3 - abort and allow retry.
                info!(
                    segment_id = segment_id.get(),
                    "Recovering stuck upload: data not in S3, aborting"
                );
                self.metadata.abort_upload(segment_id).await?;
            }

            recovered_count += 1;
        }

        if recovered_count > 0 {
            info!(recovered_count, "Recovered stuck uploads");
        }

        Ok(recovered_count)
    }

    /// Finds and returns segments eligible for tiering.
    ///
    /// Segments are eligible if:
    /// - Sealed (no more writes)
    /// - Committed (all entries durable via Raft)
    /// - Local only (not yet uploaded)
    /// - Older than `min_age_secs`
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read.
    ///
    /// # Panics
    ///
    /// Panics if postcondition fails (result exceeds batch limit).
    pub async fn find_eligible_segments(&self) -> TierResult<Vec<SegmentMetadata>> {
        let candidates = self.metadata.find_eligible_for_tiering().await?;
        let current_time = current_timestamp_secs();

        // `TigerStyle`: explicit limit on iteration.
        let mut eligible = Vec::with_capacity(MAX_TIER_BATCH_SIZE as usize);

        for (count, metadata) in candidates.into_iter().enumerate() {
            // Bounded loop: stop at batch limit.
            if count >= MAX_TIER_BATCH_SIZE as usize {
                break;
            }

            // Check age requirement.
            let age_secs = metadata
                .sealed_at_secs
                .map_or(0, |sealed| current_time.saturating_sub(sealed));

            if age_secs >= self.config.min_age_secs {
                eligible.push(metadata);
            }
        }

        // Postcondition: result size is bounded.
        assert!(eligible.len() <= MAX_TIER_BATCH_SIZE as usize);

        Ok(eligible)
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// Gets metadata or returns a `NotFound` error.
    async fn get_metadata_or_error(&self, segment_id: SegmentId) -> TierResult<SegmentMetadata> {
        self.metadata
            .get(segment_id)
            .await?
            .ok_or_else(|| TierError::NotFound {
                key: format!("segment-{segment_id}"),
            })
    }
}

// -----------------------------------------------------------------------------
// IntegratedTieringManager (with WAL)
// -----------------------------------------------------------------------------

/// Tiering manager with integrated WAL access.
///
/// This wraps `TieringManager` and adds a `SegmentReader` reference, enabling
/// the `tier_segment` method which reads segment bytes from the WAL internally.
///
/// Per the main implementation plan, this is the production interface:
/// ```ignore
/// pub struct TieringManager {
///     wal: Wal,
///     s3_client: S3Client,
///     ...
/// }
/// ```
pub struct IntegratedTieringManager<S: ObjectStorage, M: MetadataStore, R: SegmentReader> {
    /// The base tiering manager.
    inner: TieringManager<S, M>,
    /// Segment reader (WAL reference).
    segment_reader: Arc<R>,
}

impl<S: ObjectStorage, M: MetadataStore, R: SegmentReader> IntegratedTieringManager<S, M, R> {
    /// Creates a new integrated tiering manager with WAL access.
    #[must_use]
    pub fn new(storage: S, metadata: M, segment_reader: R, config: TieringConfig) -> Self {
        Self {
            inner: TieringManager::new(storage, metadata, config),
            segment_reader: Arc::new(segment_reader),
        }
    }

    /// Tiers a segment to S3 by reading from the WAL.
    ///
    /// This is the primary method per the implementation plan. It:
    /// 1. Checks eligibility (sealed + committed + local)
    /// 2. Reads segment bytes from WAL
    /// 3. Uploads to S3
    /// 4. Updates metadata
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Segment is not eligible for tiering
    /// - WAL read fails
    /// - S3 upload fails
    ///
    /// # Panics
    ///
    /// Panics if the segment is not sealed in the WAL (invariant violation).
    pub async fn tier_segment(&self, segment_id: SegmentId) -> TierResult<()> {
        // TigerStyle: Assert precondition.
        assert!(
            self.segment_reader.is_segment_sealed(segment_id),
            "segment must be sealed before tiering"
        );

        // Read segment bytes from WAL.
        let data = self.segment_reader.read_segment_bytes(segment_id).await?;

        // TigerStyle: Assert postcondition of read.
        assert!(!data.is_empty(), "WAL returned empty segment data");

        // Delegate to inner manager.
        self.inner.upload_segment(segment_id, data).await
    }

    /// Registers a segment with the metadata store.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be persisted.
    pub async fn register_segment(&self, metadata: SegmentMetadata) -> TierResult<()> {
        self.inner.register_segment(metadata).await
    }

    /// Marks a segment as sealed.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found or metadata update fails.
    pub async fn mark_sealed(&self, segment_id: SegmentId) -> TierResult<()> {
        self.inner.mark_sealed(segment_id).await
    }

    /// Marks a segment as committed.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not found or metadata update fails.
    pub async fn mark_committed(&self, segment_id: SegmentId) -> TierResult<()> {
        self.inner.mark_committed(segment_id).await
    }

    /// Downloads a segment from S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is not in S3 or download fails.
    pub async fn download_segment(&self, segment_id: SegmentId) -> TierResult<Bytes> {
        self.inner.download_segment(segment_id).await
    }

    /// Gets the location of a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read.
    pub async fn get_location(&self, segment_id: SegmentId) -> TierResult<Option<SegmentLocation>> {
        self.inner.get_location(segment_id).await
    }

    /// Finds and returns segments eligible for tiering.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata cannot be read.
    pub async fn find_eligible_segments(&self) -> TierResult<Vec<SegmentMetadata>> {
        self.inner.find_eligible_segments().await
    }

    /// Recovers segments stuck in `Uploading` state after a crash.
    ///
    /// This method should be called on startup to recover from incomplete uploads.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub async fn recover_stuck_uploads(&self) -> TierResult<u32> {
        self.inner.recover_stuck_uploads().await
    }

    /// Returns a reference to the storage backend.
    #[must_use]
    pub fn storage(&self) -> &S {
        self.inner.storage()
    }

    /// Returns a reference to the metadata store.
    #[must_use]
    pub fn metadata(&self) -> &M {
        self.inner.metadata()
    }

    /// Evicts eligible segments from local storage, respecting consumer progress.
    ///
    /// Only evicts segments where:
    /// 1. The segment is in both local and remote storage
    /// 2. The segment's `end_offset` is at or below the `safe_offset` (all records consumed)
    ///
    /// If `safe_offset` is `None`, no eviction is performed (consumers haven't
    /// started tracking this partition yet).
    ///
    /// # Returns
    ///
    /// The number of segments evicted.
    ///
    /// # Errors
    ///
    /// Returns an error if metadata operations fail.
    pub async fn evict_with_progress(
        &self,
        safe_offset: Option<Offset>,
    ) -> TierResult<u32> {
        let Some(safe) = safe_offset else {
            // No consumer tracking yet - don't evict anything.
            return Ok(0);
        };

        let candidates = self.inner.metadata.find_eligible_for_eviction().await?;
        let mut evicted = 0u32;

        for metadata in candidates {
            // Check if segment can be evicted with progress constraint.
            if !metadata.can_evict_with_progress(safe) {
                continue;
            }

            // Mark as evicted and persist.
            let mut updated = metadata;
            updated.mark_local_evicted();
            self.inner.metadata.set(updated).await?;
            evicted += 1;
        }

        Ok(evicted)
    }
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

/// Returns the current Unix timestamp in seconds.
fn current_timestamp_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::InMemoryMetadataStore;
    use crate::storage::{SegmentReader, SimulatedObjectStorage};
    use helix_core::{PartitionId, TopicId};

    fn test_segment_id(id: u32) -> SegmentId {
        SegmentId::new(u64::from(id))
    }

    fn test_metadata(segment_id: u32) -> SegmentMetadata {
        SegmentMetadata::new(
            test_segment_id(segment_id),
            TopicId::new(1),
            PartitionId::new(0),
            0,
        )
    }

    /// Stub segment reader for tests that don't need actual segment data.
    struct StubSegmentReader;

    #[async_trait::async_trait]
    impl SegmentReader for StubSegmentReader {
        async fn read_segment_bytes(&self, segment_id: SegmentId) -> TierResult<Bytes> {
            Ok(Bytes::from(format!("segment-{}", segment_id.get())))
        }

        fn is_segment_sealed(&self, _segment_id: SegmentId) -> bool {
            true // All segments are sealed in stub.
        }
    }

    #[tokio::test]
    async fn test_manager_register_and_mark() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let manager = TieringManager::new(storage, metadata_store, TieringConfig::for_testing());

        // Register a segment.
        let meta = test_metadata(1);
        manager.register_segment(meta).await.unwrap();

        // Verify location.
        let location = manager.get_location(test_segment_id(1)).await.unwrap();
        assert_eq!(location, Some(SegmentLocation::Local));

        // Mark sealed and committed.
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap();

        // Verify eligibility.
        let eligible = manager.find_eligible_segments().await.unwrap();
        assert_eq!(eligible.len(), 1);
    }

    #[tokio::test]
    async fn test_manager_upload_download() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let manager = TieringManager::new(storage, metadata_store, TieringConfig::for_testing());

        // Register and prepare segment.
        let meta = test_metadata(1);
        manager.register_segment(meta).await.unwrap();
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap();

        // Upload.
        let data = Bytes::from("segment data");
        manager
            .upload_segment(test_segment_id(1), data.clone())
            .await
            .unwrap();

        // Verify location changed.
        let location = manager.get_location(test_segment_id(1)).await.unwrap();
        assert_eq!(location, Some(SegmentLocation::Both));

        // Verify exists in S3.
        assert!(manager.exists_in_s3(test_segment_id(1)).await.unwrap());

        // Download.
        let downloaded = manager.download_segment(test_segment_id(1)).await.unwrap();
        assert_eq!(downloaded, data);
    }

    #[tokio::test]
    async fn test_manager_upload_not_eligible() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let manager = TieringManager::new(storage, metadata_store, TieringConfig::for_testing());

        // Register but don't seal.
        let meta = test_metadata(1);
        manager.register_segment(meta).await.unwrap();

        // Upload should fail.
        let result = manager
            .upload_segment(test_segment_id(1), Bytes::from("data"))
            .await;
        assert!(matches!(result, Err(TierError::NotEligible { .. })));
    }

    #[tokio::test]
    async fn test_manager_min_age_filter() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();

        // Config with 1 hour min age.
        let config = TieringConfig {
            min_age_secs: 3600,
            ..TieringConfig::default()
        };
        let manager = TieringManager::new(storage, metadata_store, config);

        // Register and prepare segment.
        let meta = test_metadata(1);
        manager.register_segment(meta).await.unwrap();
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap();

        // Should not be eligible (too young).
        let eligible = manager.find_eligible_segments().await.unwrap();
        assert!(eligible.is_empty());
    }

    #[tokio::test]
    async fn test_manager_idempotent_mark() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let manager = TieringManager::new(storage, metadata_store, TieringConfig::for_testing());

        // Register a segment.
        let meta = test_metadata(1);
        manager.register_segment(meta).await.unwrap();

        // Mark sealed twice - should be idempotent.
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_sealed(test_segment_id(1)).await.unwrap(); // No panic.

        // Mark committed twice - should be idempotent.
        manager.mark_committed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap(); // No panic.
    }

    fn test_metadata_with_offsets(segment_id: u32, start: u64, end: u64) -> SegmentMetadata {
        let mut meta = SegmentMetadata::new(
            test_segment_id(segment_id),
            TopicId::new(1),
            PartitionId::new(0),
            start,
        );
        meta.set_offset_range(Offset::new(start), Offset::new(end));
        meta
    }

    #[tokio::test]
    async fn test_integrated_manager_evict_with_progress_respects_safe_offset() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let segment_reader = StubSegmentReader;
        let manager = IntegratedTieringManager::new(
            storage,
            metadata_store,
            segment_reader,
            TieringConfig::for_testing(),
        );

        // Create three segments with different offset ranges:
        // Segment 1: offsets 0-99
        // Segment 2: offsets 100-199
        // Segment 3: offsets 200-299
        let meta1 = test_metadata_with_offsets(1, 0, 99);
        let meta2 = test_metadata_with_offsets(2, 100, 199);
        let meta3 = test_metadata_with_offsets(3, 200, 299);

        // Register and make eligible for eviction (local + remote).
        for meta in [meta1, meta2, meta3] {
            let seg_id = meta.segment_id;
            manager.register_segment(meta).await.unwrap();
            manager.mark_sealed(seg_id).await.unwrap();
            manager.mark_committed(seg_id).await.unwrap();
            // Tier the segment (reads from stub reader, uploads to storage).
            manager.tier_segment(seg_id).await.unwrap();
        }

        // After tiering, segments are in Both location and eviction-eligible.
        // We verify eviction behavior below.

        // Consumer has only consumed up to offset 150.
        // Safe eviction offset = 150 means segments with end_offset < 150 can be evicted.
        let safe_offset = Offset::new(150);

        let evicted = manager.evict_with_progress(Some(safe_offset)).await.unwrap();

        // Only segment 1 (end_offset=99 < 150) should be evicted.
        // Segment 2 (end_offset=199 >= 150) should NOT be evicted.
        // Segment 3 (end_offset=299 >= 150) should NOT be evicted.
        assert_eq!(evicted, 1, "Only segment 1 should be evicted");

        // Verify segment 1 is evicted, segments 2 and 3 are not.
        let loc1 = manager.get_location(test_segment_id(1)).await.unwrap();
        let loc2 = manager.get_location(test_segment_id(2)).await.unwrap();
        let loc3 = manager.get_location(test_segment_id(3)).await.unwrap();

        assert_eq!(loc1, Some(SegmentLocation::Remote), "Segment 1 should be remote-only");
        assert_eq!(loc2, Some(SegmentLocation::Both), "Segment 2 should still be local+remote");
        assert_eq!(loc3, Some(SegmentLocation::Both), "Segment 3 should still be local+remote");
    }

    #[tokio::test]
    async fn test_integrated_manager_evict_with_progress_none_evicts_nothing() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let segment_reader = StubSegmentReader;
        let manager = IntegratedTieringManager::new(
            storage,
            metadata_store,
            segment_reader,
            TieringConfig::for_testing(),
        );

        // Create a segment eligible for eviction.
        let meta = test_metadata_with_offsets(1, 0, 99);
        manager.register_segment(meta).await.unwrap();
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap();
        manager.tier_segment(test_segment_id(1)).await.unwrap();

        // No safe offset (no consumers) - should not evict anything.
        let evicted = manager.evict_with_progress(None).await.unwrap();
        assert_eq!(evicted, 0, "Should not evict when safe_offset is None");

        // Verify segment is still local.
        let loc = manager.get_location(test_segment_id(1)).await.unwrap();
        assert_eq!(loc, Some(SegmentLocation::Both));
    }

    #[tokio::test]
    async fn test_integrated_manager_evict_with_progress_no_offset_range() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let segment_reader = StubSegmentReader;
        let manager = IntegratedTieringManager::new(
            storage,
            metadata_store,
            segment_reader,
            TieringConfig::for_testing(),
        );

        // Create a segment WITHOUT offset range set.
        let meta = test_metadata(1); // No offset range!
        manager.register_segment(meta).await.unwrap();
        manager.mark_sealed(test_segment_id(1)).await.unwrap();
        manager.mark_committed(test_segment_id(1)).await.unwrap();
        manager.tier_segment(test_segment_id(1)).await.unwrap();

        // Try to evict with safe_offset = high value.
        let evicted = manager
            .evict_with_progress(Some(Offset::new(1000)))
            .await
            .unwrap();

        // Should NOT evict because end_offset is None (can't determine safety).
        assert_eq!(evicted, 0, "Should not evict segments without offset range");

        // Verify segment is still local.
        let loc = manager.get_location(test_segment_id(1)).await.unwrap();
        assert_eq!(loc, Some(SegmentLocation::Both));
    }

    #[tokio::test]
    async fn test_integrated_manager_evict_with_progress_high_safe_offset() {
        let storage = SimulatedObjectStorage::new(42);
        let metadata_store = InMemoryMetadataStore::new();
        let segment_reader = StubSegmentReader;
        let manager = IntegratedTieringManager::new(
            storage,
            metadata_store,
            segment_reader,
            TieringConfig::for_testing(),
        );

        // Create segments.
        let meta1 = test_metadata_with_offsets(1, 0, 99);
        let meta2 = test_metadata_with_offsets(2, 100, 199);

        for meta in [meta1, meta2] {
            let seg_id = meta.segment_id;
            manager.register_segment(meta).await.unwrap();
            manager.mark_sealed(seg_id).await.unwrap();
            manager.mark_committed(seg_id).await.unwrap();
            manager.tier_segment(seg_id).await.unwrap();
        }

        // Consumer has consumed everything (safe_offset = 1000).
        let evicted = manager
            .evict_with_progress(Some(Offset::new(1000)))
            .await
            .unwrap();

        // Both segments should be evicted.
        assert_eq!(evicted, 2, "Both segments should be evicted");

        // Verify both are remote-only.
        let loc1 = manager.get_location(test_segment_id(1)).await.unwrap();
        let loc2 = manager.get_location(test_segment_id(2)).await.unwrap();
        assert_eq!(loc1, Some(SegmentLocation::Remote));
        assert_eq!(loc2, Some(SegmentLocation::Remote));
    }
}

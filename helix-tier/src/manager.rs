//! Tiering manager for coordinating segment upload/download.
//!
//! The `TieringManager` orchestrates movement of WAL segments between local
//! storage and S3, tracking metadata and enforcing eligibility policies.

use std::sync::Arc;

use bytes::Bytes;
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

        let metadata = self.get_metadata_or_error(segment_id).await?;

        // Check eligibility.
        self.check_upload_eligibility(&metadata)?;

        let key = ObjectKey::from_segment(
            metadata.topic_id.get(),
            metadata.partition_id.get(),
            segment_id.get(),
        );

        info!(
            segment_id = segment_id.get(),
            key = %key,
            size_bytes = data.len(),
            "Uploading segment to S3"
        );

        // Upload to S3.
        self.storage.put(&key, data).await?;

        // Update metadata.
        let mut updated = metadata;
        updated.mark_uploaded(key);
        self.metadata.set(updated).await?;

        info!(segment_id = segment_id.get(), "Segment uploaded successfully");
        Ok(())
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

    /// Checks if a segment is eligible for upload.
    #[allow(clippy::unused_self)]
    fn check_upload_eligibility(&self, metadata: &SegmentMetadata) -> TierResult<()> {
        if !metadata.sealed {
            return Err(TierError::NotEligible {
                segment_id: metadata.segment_id.get(),
                reason: "segment not sealed",
            });
        }

        if !metadata.committed {
            return Err(TierError::NotEligible {
                segment_id: metadata.segment_id.get(),
                reason: "segment not committed",
            });
        }

        if metadata.location != SegmentLocation::Local {
            return Err(TierError::NotEligible {
                segment_id: metadata.segment_id.get(),
                reason: "segment already uploaded",
            });
        }

        Ok(())
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
    use crate::storage::SimulatedObjectStorage;
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
}

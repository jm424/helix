//! Segment metadata tracking for tiered storage.
//!
//! This module tracks which segments are stored locally, remotely (S3), or both,
//! enabling the tiering manager to make decisions about uploads and evictions.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use helix_core::{Offset, PartitionId, TopicId};
use helix_wal::SegmentId;

use crate::error::TierResult;
use crate::storage::ObjectKey;

// -----------------------------------------------------------------------------
// SegmentLocation
// -----------------------------------------------------------------------------

/// Location of a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentLocation {
    /// Segment is stored locally only.
    Local,
    /// Segment is being uploaded to remote storage.
    /// This is a transient state that prevents concurrent uploads.
    Uploading,
    /// Segment is stored in remote storage (S3) only.
    Remote,
    /// Segment is stored both locally and remotely.
    Both,
}

// -----------------------------------------------------------------------------
// SegmentMetadata
// -----------------------------------------------------------------------------

/// Metadata for a single segment.
///
/// Tracks the segment's identity, index range, and current storage location.
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    /// Segment ID.
    pub segment_id: SegmentId,
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID.
    pub partition_id: PartitionId,
    /// First log index in the segment.
    pub first_index: u64,
    /// Last log index in the segment.
    pub last_index: u64,
    /// Segment size in bytes.
    pub size_bytes: u64,
    /// Whether the segment is sealed (no more writes).
    pub sealed: bool,
    /// Whether all entries in the segment are committed through Raft.
    pub committed: bool,
    /// Current location of the segment.
    pub location: SegmentLocation,
    /// Object key in remote storage (if uploaded).
    pub remote_key: Option<ObjectKey>,
    /// Unix timestamp (seconds) when the segment was created.
    pub created_at_secs: u64,
    /// Unix timestamp (seconds) when the segment was sealed.
    pub sealed_at_secs: Option<u64>,
    /// Unix timestamp (seconds) when the segment was uploaded.
    pub uploaded_at_secs: Option<u64>,
    /// First partition offset in this segment (if tracked).
    /// Used for coordinating eviction with consumer progress.
    pub start_offset: Option<Offset>,
    /// End partition offset (exclusive) for this segment (if tracked).
    /// All records in this segment have offsets in `[start_offset, end_offset)`.
    pub end_offset: Option<Offset>,
}

impl SegmentMetadata {
    /// Creates new metadata for a segment.
    #[must_use]
    pub fn new(
        segment_id: SegmentId,
        topic_id: TopicId,
        partition_id: PartitionId,
        first_index: u64,
    ) -> Self {
        Self {
            segment_id,
            topic_id,
            partition_id,
            first_index,
            last_index: first_index,
            size_bytes: 0,
            sealed: false,
            committed: false,
            location: SegmentLocation::Local,
            remote_key: None,
            created_at_secs: current_timestamp_secs(),
            sealed_at_secs: None,
            uploaded_at_secs: None,
            start_offset: None,
            end_offset: None,
        }
    }

    /// Sets the partition offset range for this segment.
    ///
    /// This is used to coordinate eviction with consumer progress tracking.
    /// Only segments with `end_offset` below the safe eviction offset should
    /// be evicted from local storage.
    pub const fn set_offset_range(&mut self, start: Offset, end: Offset) {
        self.start_offset = Some(start);
        self.end_offset = Some(end);
    }

    /// Returns true if this segment can be safely evicted given the safe offset.
    ///
    /// A segment can be evicted if:
    /// 1. It's in both local and remote storage (`can_evict_local`)
    /// 2. Its `end_offset` is at or below the `safe_offset` (all records consumed)
    ///
    /// If `end_offset` is None, returns false because we can't verify consumer
    /// safety without knowing the offset range.
    #[must_use]
    pub const fn can_evict_with_progress(&self, safe_offset: Offset) -> bool {
        if !self.can_evict_local() {
            return false;
        }
        // Can't evict if we don't know the offset range - safety cannot be verified.
        match self.end_offset {
            Some(end) => end.get() < safe_offset.get(),
            None => false,
        }
    }

    /// Returns true if the segment is eligible for tiering to S3.
    ///
    /// Requirements:
    /// - Segment must be sealed (no more writes)
    /// - All entries must be committed through Raft
    /// - Segment must still be local (not yet uploaded)
    #[must_use]
    pub const fn is_eligible_for_tiering(&self) -> bool {
        self.sealed && self.committed && matches!(self.location, SegmentLocation::Local)
    }

    /// Returns true if the local copy can be evicted.
    ///
    /// Requirements:
    /// - Segment must be uploaded to remote storage
    #[must_use]
    pub const fn can_evict_local(&self) -> bool {
        matches!(self.location, SegmentLocation::Both)
    }

    /// Returns true if the segment is only in remote storage.
    #[must_use]
    pub const fn is_remote_only(&self) -> bool {
        matches!(self.location, SegmentLocation::Remote)
    }

    /// Marks the segment as sealed.
    ///
    /// # Panics
    ///
    /// Panics if the segment is already sealed.
    pub fn mark_sealed(&mut self) {
        // Precondition: segment must not already be sealed.
        assert!(!self.sealed, "segment already sealed");

        self.sealed = true;
        self.sealed_at_secs = Some(current_timestamp_secs());

        // Postcondition: segment is now sealed with timestamp.
        assert!(self.sealed);
        assert!(self.sealed_at_secs.is_some());
    }

    /// Marks the segment as committed.
    ///
    /// # Panics
    ///
    /// Panics if the segment is already committed.
    pub fn mark_committed(&mut self) {
        // Precondition: segment must not already be committed.
        assert!(!self.committed, "segment already committed");

        self.committed = true;

        // Postcondition: segment is now committed.
        assert!(self.committed);
    }

    /// Marks the segment as uploaded.
    ///
    /// # Panics
    ///
    /// Panics if the segment is not local or already has a remote key.
    pub fn mark_uploaded(&mut self, remote_key: ObjectKey) {
        // Precondition: segment must be local-only.
        assert!(
            self.location == SegmentLocation::Local,
            "segment must be local to upload"
        );
        assert!(self.remote_key.is_none(), "segment already has remote key");

        self.location = SegmentLocation::Both;
        self.remote_key = Some(remote_key);
        self.uploaded_at_secs = Some(current_timestamp_secs());

        // Postcondition: segment is now in both locations.
        assert!(self.location == SegmentLocation::Both);
        assert!(self.remote_key.is_some());
        assert!(self.uploaded_at_secs.is_some());
    }

    /// Marks the local copy as evicted.
    ///
    /// # Panics
    ///
    /// Panics if the segment is not in both locations.
    pub fn mark_local_evicted(&mut self) {
        // Precondition: segment must be in both locations.
        assert!(
            self.location == SegmentLocation::Both,
            "segment must be in both locations to evict local"
        );
        assert!(self.remote_key.is_some(), "segment must have remote key");

        self.location = SegmentLocation::Remote;

        // Postcondition: segment is now remote-only.
        assert!(self.location == SegmentLocation::Remote);
    }
}

// -----------------------------------------------------------------------------
// MetadataStore Trait
// -----------------------------------------------------------------------------

/// Metadata store trait for persistence.
///
/// Implementations track segment metadata and provide queries for tiering decisions.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    /// Gets metadata for a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be accessed.
    async fn get(&self, segment_id: SegmentId) -> TierResult<Option<SegmentMetadata>>;

    /// Sets metadata for a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata cannot be persisted.
    async fn set(&self, metadata: SegmentMetadata) -> TierResult<()>;

    /// Removes metadata for a segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata cannot be removed.
    async fn remove(&self, segment_id: SegmentId) -> TierResult<()>;

    /// Lists all segments for a partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be accessed.
    async fn list_partition(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> TierResult<Vec<SegmentMetadata>>;

    /// Finds segments eligible for tiering to S3.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be accessed.
    async fn find_eligible_for_tiering(&self) -> TierResult<Vec<SegmentMetadata>>;

    /// Finds segments eligible for local eviction.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be accessed.
    async fn find_eligible_for_eviction(&self) -> TierResult<Vec<SegmentMetadata>>;

    /// Syncs metadata to persistent storage (if applicable).
    ///
    /// # Errors
    ///
    /// Returns an error if the sync fails.
    async fn sync(&self) -> TierResult<()>;

    /// Atomically claims a segment for upload if it's eligible.
    ///
    /// This method atomically checks if the segment is:
    /// - Sealed
    /// - Committed
    /// - Location is Local
    ///
    /// If all conditions are met, it marks the segment as `Uploading` and
    /// returns `Ok(true)`. The caller must then call either `complete_upload`
    /// on success or `abort_upload` on failure.
    ///
    /// If the segment is not eligible (already uploaded, being uploaded, etc.),
    /// returns `Ok(false)` without modifying the segment.
    ///
    /// This method is essential for preventing race conditions when multiple
    /// threads try to upload the same segment concurrently.
    ///
    /// # Arguments
    ///
    /// * `segment_id` - The segment to claim
    ///
    /// # Errors
    ///
    /// Returns an error if the segment doesn't exist or metadata cannot be accessed.
    async fn try_claim_for_upload(&self, segment_id: SegmentId) -> TierResult<bool>;

    /// Completes a successful upload by marking the segment as `Both`.
    ///
    /// Must only be called after a successful S3 upload for a segment that
    /// was previously claimed via `try_claim_for_upload`.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment doesn't exist or is not in `Uploading` state.
    async fn complete_upload(&self, segment_id: SegmentId, remote_key: ObjectKey)
        -> TierResult<()>;

    /// Aborts a failed upload by reverting the segment back to `Local`.
    ///
    /// Must be called if the S3 upload fails for a segment that was previously
    /// claimed via `try_claim_for_upload`.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment doesn't exist or is not in `Uploading` state.
    async fn abort_upload(&self, segment_id: SegmentId) -> TierResult<()>;

    /// Finds segments stuck in `Uploading` state.
    ///
    /// These segments were being uploaded when a crash occurred. They need
    /// recovery: either complete the upload (if data is in S3) or abort
    /// (if data is not in S3).
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata store cannot be accessed.
    async fn find_stuck_uploads(&self) -> TierResult<Vec<SegmentMetadata>>;
}

// -----------------------------------------------------------------------------
// MetadataStoreFaultConfig
// -----------------------------------------------------------------------------

/// Configuration for fault injection in simulated metadata store.
///
/// Used for deterministic simulation testing (DST) of tiering scenarios.
#[derive(Debug, Clone, Default)]
pub struct MetadataStoreFaultConfig {
    /// Probability of `complete_upload` failing. Range: 0.0 - 1.0.
    pub complete_upload_fail_rate: f64,
    /// Probability of `abort_upload` failing. Range: 0.0 - 1.0.
    pub abort_upload_fail_rate: f64,
    /// Probability of set (write) failing. Range: 0.0 - 1.0.
    pub set_fail_rate: f64,
    /// Probability of get (read) failing. Range: 0.0 - 1.0.
    pub get_fail_rate: f64,
    /// Probability of `find_stuck_uploads` failing. Range: 0.0 - 1.0.
    pub find_stuck_fail_rate: f64,
    /// Probability of `try_claim_for_upload` failing. Range: 0.0 - 1.0.
    pub try_claim_fail_rate: f64,
}

impl MetadataStoreFaultConfig {
    /// Creates a fault config with no faults.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Creates a fault config that simulates flaky metadata store.
    #[must_use]
    pub const fn flaky() -> Self {
        Self {
            complete_upload_fail_rate: 0.05,
            abort_upload_fail_rate: 0.05,
            set_fail_rate: 0.02,
            get_fail_rate: 0.02,
            find_stuck_fail_rate: 0.05,
            try_claim_fail_rate: 0.05,
        }
    }

    /// Sets the `complete_upload` failure rate.
    #[must_use]
    pub const fn with_complete_upload_fail_rate(mut self, rate: f64) -> Self {
        self.complete_upload_fail_rate = rate;
        self
    }

    /// Sets the `abort_upload` failure rate.
    #[must_use]
    pub const fn with_abort_upload_fail_rate(mut self, rate: f64) -> Self {
        self.abort_upload_fail_rate = rate;
        self
    }

    /// Sets the set failure rate.
    #[must_use]
    pub const fn with_set_fail_rate(mut self, rate: f64) -> Self {
        self.set_fail_rate = rate;
        self
    }

    /// Sets the get failure rate.
    #[must_use]
    pub const fn with_get_fail_rate(mut self, rate: f64) -> Self {
        self.get_fail_rate = rate;
        self
    }

    /// Sets the `find_stuck_uploads` failure rate.
    #[must_use]
    pub const fn with_find_stuck_fail_rate(mut self, rate: f64) -> Self {
        self.find_stuck_fail_rate = rate;
        self
    }

    /// Sets the `try_claim_for_upload` failure rate.
    #[must_use]
    pub const fn with_try_claim_fail_rate(mut self, rate: f64) -> Self {
        self.try_claim_fail_rate = rate;
        self
    }
}

// -----------------------------------------------------------------------------
// InMemoryMetadataStore
// -----------------------------------------------------------------------------

/// In-memory metadata store for testing.
///
/// All data is kept in memory and lost when the store is dropped.
/// Clones share the same underlying data (via `Arc`).
/// Supports fault injection for DST.
#[derive(Debug, Clone)]
pub struct InMemoryMetadataStore {
    segments: Arc<Mutex<HashMap<SegmentId, SegmentMetadata>>>,
    fault_config: Arc<Mutex<MetadataStoreFaultConfig>>,
    seed: u64,
    counter: Arc<std::sync::atomic::AtomicU64>,
}

impl Default for InMemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryMetadataStore {
    /// Creates a new in-memory metadata store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            segments: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(MetadataStoreFaultConfig::default())),
            seed: 0,
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Creates a new metadata store with fault injection enabled.
    #[must_use]
    pub fn with_faults(seed: u64, config: MetadataStoreFaultConfig) -> Self {
        Self {
            segments: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(config)),
            seed,
            counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Returns a reference to the fault configuration for modification.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn fault_config(&self) -> std::sync::MutexGuard<'_, MetadataStoreFaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Deterministic RNG based on seed and counter.
    fn should_inject_fault(&self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let counter = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0x517c_c1b7_2722_0a95);
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }

    /// Returns the number of segments tracked.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        let segments = self.segments.lock().expect("segments lock poisoned");
        segments.len()
    }

    /// Clears all segments.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn clear(&self) {
        let mut segments = self.segments.lock().expect("segments lock poisoned");
        segments.clear();
    }

    /// Returns all segments as a vector of (id, metadata) pairs.
    ///
    /// This bypasses fault injection for use in invariant checking.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn all_segments(&self) -> Vec<(SegmentId, SegmentMetadata)> {
        let segments = self.segments.lock().expect("segments lock poisoned");
        segments.iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

// Allow significant_drop_tightening since this is test-only simulation code.
#[allow(clippy::significant_drop_tightening)]
#[async_trait]
impl MetadataStore for InMemoryMetadataStore {
    async fn get(&self, segment_id: SegmentId) -> TierResult<Option<SegmentMetadata>> {
        // Check for fault injection.
        let get_fail_rate = self.fault_config.lock().expect("lock").get_fail_rate;
        if self.should_inject_fault(get_fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "metadata_get",
                message: "simulated metadata get failure".to_string(),
            });
        }

        let segments = self.segments.lock().expect("segments lock poisoned");
        Ok(segments.get(&segment_id).cloned())
    }

    async fn set(&self, metadata: SegmentMetadata) -> TierResult<()> {
        // Check for fault injection.
        let set_fail_rate = self.fault_config.lock().expect("lock").set_fail_rate;
        if self.should_inject_fault(set_fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "metadata_set",
                message: "simulated metadata set failure".to_string(),
            });
        }

        let mut segments = self.segments.lock().expect("segments lock poisoned");
        segments.insert(metadata.segment_id, metadata);
        Ok(())
    }

    async fn remove(&self, segment_id: SegmentId) -> TierResult<()> {
        let mut segments = self.segments.lock().expect("segments lock poisoned");
        segments.remove(&segment_id);
        Ok(())
    }

    async fn list_partition(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> TierResult<Vec<SegmentMetadata>> {
        let segments = self.segments.lock().expect("segments lock poisoned");
        let result: Vec<_> = segments
            .values()
            .filter(|m| m.topic_id == topic_id && m.partition_id == partition_id)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn find_eligible_for_tiering(&self) -> TierResult<Vec<SegmentMetadata>> {
        let segments = self.segments.lock().expect("segments lock poisoned");
        let result: Vec<_> = segments
            .values()
            .filter(|m| m.is_eligible_for_tiering())
            .cloned()
            .collect();
        Ok(result)
    }

    async fn find_eligible_for_eviction(&self) -> TierResult<Vec<SegmentMetadata>> {
        let segments = self.segments.lock().expect("segments lock poisoned");
        let result: Vec<_> = segments
            .values()
            .filter(|m| m.can_evict_local())
            .cloned()
            .collect();
        Ok(result)
    }

    async fn sync(&self) -> TierResult<()> {
        // No-op for in-memory store.
        Ok(())
    }

    async fn try_claim_for_upload(&self, segment_id: SegmentId) -> TierResult<bool> {
        // Check for fault injection.
        let fail_rate = self.fault_config.lock().expect("lock").try_claim_fail_rate;
        if self.should_inject_fault(fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "try_claim_for_upload",
                message: "simulated try_claim failure".to_string(),
            });
        }

        let mut segments = self.segments.lock().expect("segments lock poisoned");

        let Some(metadata) = segments.get_mut(&segment_id) else {
            return Err(crate::error::TierError::NotFound {
                key: format!("segment-{}", segment_id.get()),
            });
        };

        // Check eligibility atomically.
        if !metadata.is_eligible_for_tiering() {
            return Ok(false);
        }

        // Claim the segment by marking it as Uploading.
        metadata.location = SegmentLocation::Uploading;

        Ok(true)
    }

    async fn complete_upload(
        &self,
        segment_id: SegmentId,
        remote_key: ObjectKey,
    ) -> TierResult<()> {
        // Check for fault injection.
        let fail_rate = self.fault_config.lock().expect("lock").complete_upload_fail_rate;
        if self.should_inject_fault(fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "complete_upload",
                message: "simulated complete_upload failure".to_string(),
            });
        }

        let mut segments = self.segments.lock().expect("segments lock poisoned");

        let Some(metadata) = segments.get_mut(&segment_id) else {
            return Err(crate::error::TierError::NotFound {
                key: format!("segment-{}", segment_id.get()),
            });
        };

        // Verify segment is in Uploading state.
        assert!(
            metadata.location == SegmentLocation::Uploading,
            "complete_upload called on segment not in Uploading state: {:?}",
            metadata.location
        );

        // Mark as successfully uploaded.
        metadata.location = SegmentLocation::Both;
        metadata.remote_key = Some(remote_key);
        metadata.uploaded_at_secs = Some(current_timestamp_secs());

        Ok(())
    }

    async fn abort_upload(&self, segment_id: SegmentId) -> TierResult<()> {
        // Check for fault injection.
        let fail_rate = self.fault_config.lock().expect("lock").abort_upload_fail_rate;
        if self.should_inject_fault(fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "abort_upload",
                message: "simulated abort_upload failure".to_string(),
            });
        }

        let mut segments = self.segments.lock().expect("segments lock poisoned");

        let Some(metadata) = segments.get_mut(&segment_id) else {
            return Err(crate::error::TierError::NotFound {
                key: format!("segment-{}", segment_id.get()),
            });
        };

        // Verify segment is in Uploading state.
        assert!(
            metadata.location == SegmentLocation::Uploading,
            "abort_upload called on segment not in Uploading state: {:?}",
            metadata.location
        );

        // Revert to Local.
        metadata.location = SegmentLocation::Local;

        Ok(())
    }

    async fn find_stuck_uploads(&self) -> TierResult<Vec<SegmentMetadata>> {
        // Check for fault injection.
        let fail_rate = self.fault_config.lock().expect("lock").find_stuck_fail_rate;
        if self.should_inject_fault(fail_rate) {
            return Err(crate::error::TierError::Io {
                operation: "find_stuck_uploads",
                message: "simulated find_stuck_uploads failure".to_string(),
            });
        }

        let segments = self.segments.lock().expect("segments lock poisoned");
        let result: Vec<_> = segments
            .values()
            .filter(|m| matches!(m.location, SegmentLocation::Uploading))
            .cloned()
            .collect();
        Ok(result)
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

    fn test_segment_id(id: u64) -> SegmentId {
        SegmentId::new(id)
    }

    fn test_topic_id(id: u64) -> TopicId {
        TopicId::new(id)
    }

    fn test_partition_id(id: u64) -> PartitionId {
        PartitionId::new(id)
    }

    #[test]
    fn test_segment_metadata_tiering_eligibility() {
        let mut meta = SegmentMetadata::new(
            test_segment_id(1),
            test_topic_id(1),
            test_partition_id(0),
            0,
        );

        // Not eligible: not sealed, not committed.
        assert!(!meta.is_eligible_for_tiering());

        // Seal it.
        meta.mark_sealed();
        assert!(!meta.is_eligible_for_tiering());

        // Commit it.
        meta.mark_committed();
        assert!(meta.is_eligible_for_tiering());

        // Upload it.
        let key = ObjectKey::from_segment(1, 0, 1);
        meta.mark_uploaded(key);
        assert!(!meta.is_eligible_for_tiering()); // Already uploaded.
        assert!(meta.can_evict_local());

        // Evict local.
        meta.mark_local_evicted();
        assert!(!meta.can_evict_local());
        assert!(meta.is_remote_only());
    }

    #[tokio::test]
    async fn test_in_memory_metadata_store_basic() {
        let store = InMemoryMetadataStore::new();
        let segment_id = test_segment_id(1);

        // Initially empty.
        assert!(store.get(segment_id).await.unwrap().is_none());
        assert_eq!(store.segment_count(), 0);

        // Set metadata.
        let meta = SegmentMetadata::new(
            segment_id,
            test_topic_id(1),
            test_partition_id(0),
            0,
        );
        store.set(meta.clone()).await.unwrap();

        // Get metadata.
        let retrieved = store.get(segment_id).await.unwrap().unwrap();
        assert_eq!(retrieved.segment_id, segment_id);
        assert_eq!(store.segment_count(), 1);

        // Remove metadata.
        store.remove(segment_id).await.unwrap();
        assert!(store.get(segment_id).await.unwrap().is_none());
        assert_eq!(store.segment_count(), 0);
    }

    #[tokio::test]
    async fn test_in_memory_metadata_store_list_partition() {
        let store = InMemoryMetadataStore::new();

        // Add segments to different partitions.
        store
            .set(SegmentMetadata::new(
                test_segment_id(1),
                test_topic_id(1),
                test_partition_id(0),
                0,
            ))
            .await
            .unwrap();
        store
            .set(SegmentMetadata::new(
                test_segment_id(2),
                test_topic_id(1),
                test_partition_id(0),
                100,
            ))
            .await
            .unwrap();
        store
            .set(SegmentMetadata::new(
                test_segment_id(3),
                test_topic_id(1),
                test_partition_id(1),
                0,
            ))
            .await
            .unwrap();

        // List partition 0.
        let segments = store
            .list_partition(test_topic_id(1), test_partition_id(0))
            .await
            .unwrap();
        assert_eq!(segments.len(), 2);

        // List partition 1.
        let segments = store
            .list_partition(test_topic_id(1), test_partition_id(1))
            .await
            .unwrap();
        assert_eq!(segments.len(), 1);

        // List non-existent partition.
        let segments = store
            .list_partition(test_topic_id(1), test_partition_id(99))
            .await
            .unwrap();
        assert!(segments.is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_metadata_store_find_eligible() {
        let store = InMemoryMetadataStore::new();

        // Segment 1: sealed + committed = eligible.
        let mut meta1 = SegmentMetadata::new(
            test_segment_id(1),
            test_topic_id(1),
            test_partition_id(0),
            0,
        );
        meta1.mark_sealed();
        meta1.mark_committed();
        store.set(meta1).await.unwrap();

        // Segment 2: only sealed = not eligible.
        let mut meta2 = SegmentMetadata::new(
            test_segment_id(2),
            test_topic_id(1),
            test_partition_id(0),
            100,
        );
        meta2.mark_sealed();
        store.set(meta2).await.unwrap();

        // Segment 3: uploaded = can evict.
        let mut meta3 = SegmentMetadata::new(
            test_segment_id(3),
            test_topic_id(1),
            test_partition_id(0),
            200,
        );
        meta3.mark_sealed();
        meta3.mark_committed();
        meta3.mark_uploaded(ObjectKey::from_segment(1, 0, 3));
        store.set(meta3).await.unwrap();

        // Find tiering eligible.
        let eligible = store.find_eligible_for_tiering().await.unwrap();
        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].segment_id, test_segment_id(1));

        // Find eviction eligible.
        let evictable = store.find_eligible_for_eviction().await.unwrap();
        assert_eq!(evictable.len(), 1);
        assert_eq!(evictable[0].segment_id, test_segment_id(3));
    }

    #[tokio::test]
    async fn test_in_memory_metadata_store_clone_shares_state() {
        let store = InMemoryMetadataStore::new();
        let store2 = store.clone();

        let segment_id = test_segment_id(1);
        let meta = SegmentMetadata::new(
            segment_id,
            test_topic_id(1),
            test_partition_id(0),
            0,
        );

        // Set via store.
        store.set(meta).await.unwrap();

        // Visible via store2.
        assert!(store2.get(segment_id).await.unwrap().is_some());

        // Remove via store2.
        store2.remove(segment_id).await.unwrap();

        // Removed from store.
        assert!(store.get(segment_id).await.unwrap().is_none());
    }
}

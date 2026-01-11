//! Object storage abstraction for tiered storage.
//!
//! This module provides a trait-based abstraction allowing different
//! backends (S3, simulated in-memory for DST).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use helix_wal::SegmentId;

use crate::error::{TierError, TierResult};

// -----------------------------------------------------------------------------
// SegmentReader Trait
// -----------------------------------------------------------------------------

/// Trait for reading segment bytes from a WAL.
///
/// This abstraction allows `TieringManager` to read segment data directly
/// from the WAL when tiering, without the caller having to provide the bytes.
///
/// # Implementors
///
/// - `helix_wal::Wal` implements this for production use
/// - Test implementations can provide canned segment data
#[async_trait]
pub trait SegmentReader: Send + Sync {
    /// Reads the raw bytes of a sealed segment.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment doesn't exist or is not sealed.
    async fn read_segment_bytes(&self, segment_id: SegmentId) -> TierResult<Bytes>;

    /// Checks if a segment is sealed and eligible for tiering.
    fn is_segment_sealed(&self, segment_id: SegmentId) -> bool;
}

// -----------------------------------------------------------------------------
// ObjectKey
// -----------------------------------------------------------------------------

/// Object key in the storage backend.
///
/// Keys follow the format: `{topic_id}/{partition_id}/segment-{segment_id:08x}.wal`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates a new object key from a string.
    #[must_use]
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Creates a new object key from segment identifiers.
    ///
    /// Format: `{topic_id}/{partition_id}/segment-{segment_id:08x}.wal`
    #[must_use]
    pub fn from_segment(topic_id: u64, partition_id: u64, segment_id: u64) -> Self {
        Self(format!(
            "{topic_id}/{partition_id}/segment-{segment_id:08x}.wal"
        ))
    }

    /// Returns the key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// -----------------------------------------------------------------------------
// ObjectStorage Trait
// -----------------------------------------------------------------------------

/// Object storage backend trait.
///
/// Implementations must be `Send + Sync` for use across async tasks.
#[async_trait]
pub trait ObjectStorage: Send + Sync {
    /// Uploads an object to storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload fails.
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()>;

    /// Downloads an object from storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the object doesn't exist or download fails.
    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes>;

    /// Deletes an object from storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the deletion fails.
    async fn delete(&self, key: &ObjectKey) -> TierResult<()>;

    /// Lists objects with the given prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails.
    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>>;

    /// Checks if an object exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails.
    async fn exists(&self, key: &ObjectKey) -> TierResult<bool>;
}

// -----------------------------------------------------------------------------
// Fault Configuration
// -----------------------------------------------------------------------------

/// Configuration for fault injection in simulated object storage.
///
/// Used for deterministic simulation testing (DST) of tiering scenarios.
#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct ObjectStorageFaultConfig {
    /// Probability of put (upload) failing. Range: 0.0 - 1.0.
    pub put_fail_rate: f64,
    /// Probability of get (download) failing. Range: 0.0 - 1.0.
    pub get_fail_rate: f64,
    /// Probability of get returning corrupted data. Range: 0.0 - 1.0.
    pub get_corruption_rate: f64,
    /// Probability of delete failing. Range: 0.0 - 1.0.
    pub delete_fail_rate: f64,
    /// Probability of exists check failing. Range: 0.0 - 1.0.
    pub exists_fail_rate: f64,
    /// If true, next put will fail (one-shot).
    pub force_put_fail: bool,
    /// If true, next get will fail (one-shot).
    pub force_get_fail: bool,
    /// If true, next get will return corrupted data (one-shot).
    pub force_get_corruption: bool,
    /// If true, next delete will fail (one-shot).
    pub force_delete_fail: bool,
    /// If true, next exists will fail (one-shot).
    pub force_exists_fail: bool,
}

impl Default for ObjectStorageFaultConfig {
    fn default() -> Self {
        Self {
            put_fail_rate: 0.0,
            get_fail_rate: 0.0,
            get_corruption_rate: 0.0,
            delete_fail_rate: 0.0,
            exists_fail_rate: 0.0,
            force_put_fail: false,
            force_get_fail: false,
            force_get_corruption: false,
            force_delete_fail: false,
            force_exists_fail: false,
        }
    }
}

impl ObjectStorageFaultConfig {
    /// Creates a fault config with no faults (for basic testing).
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Creates a fault config that simulates flaky S3.
    #[must_use]
    pub fn flaky() -> Self {
        Self {
            put_fail_rate: 0.01,
            get_fail_rate: 0.01,
            get_corruption_rate: 0.001,
            delete_fail_rate: 0.001,
            exists_fail_rate: 0.01,
            ..Default::default()
        }
    }

    /// Sets the exists failure rate.
    ///
    /// # Panics
    ///
    /// Panics if rate is not in range 0.0..=1.0.
    #[must_use]
    pub fn with_exists_fail_rate(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "rate must be in 0.0..=1.0");
        self.exists_fail_rate = rate;
        self
    }

    /// Sets the put failure rate.
    ///
    /// # Panics
    ///
    /// Panics if rate is not in range 0.0..=1.0.
    #[must_use]
    pub fn with_put_fail_rate(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "rate must be in 0.0..=1.0");
        self.put_fail_rate = rate;
        self
    }

    /// Sets the get failure rate.
    ///
    /// # Panics
    ///
    /// Panics if rate is not in range 0.0..=1.0.
    #[must_use]
    pub fn with_get_fail_rate(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "rate must be in 0.0..=1.0");
        self.get_fail_rate = rate;
        self
    }

    /// Sets the get corruption rate.
    ///
    /// # Panics
    ///
    /// Panics if rate is not in range 0.0..=1.0.
    #[must_use]
    pub fn with_get_corruption_rate(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "rate must be in 0.0..=1.0");
        self.get_corruption_rate = rate;
        self
    }

    /// Sets the delete failure rate.
    ///
    /// # Panics
    ///
    /// Panics if rate is not in range 0.0..=1.0.
    #[must_use]
    pub fn with_delete_fail_rate(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "rate must be in 0.0..=1.0");
        self.delete_fail_rate = rate;
        self
    }

    /// Forces the next put to fail (one-shot).
    #[must_use]
    pub const fn with_force_put_fail(mut self) -> Self {
        self.force_put_fail = true;
        self
    }

    /// Forces the next get to fail (one-shot).
    #[must_use]
    pub const fn with_force_get_fail(mut self) -> Self {
        self.force_get_fail = true;
        self
    }

    /// Forces the next get to return corrupted data (one-shot).
    #[must_use]
    pub const fn with_force_get_corruption(mut self) -> Self {
        self.force_get_corruption = true;
        self
    }

    /// Forces the next delete to fail (one-shot).
    #[must_use]
    pub const fn with_force_delete_fail(mut self) -> Self {
        self.force_delete_fail = true;
        self
    }
}

// -----------------------------------------------------------------------------
// SimulatedObjectStorage
// -----------------------------------------------------------------------------

/// In-memory simulated object storage for deterministic testing.
///
/// This storage implementation keeps all data in memory and supports
/// configurable fault injection for testing tiering scenarios.
///
/// # Determinism
///
/// All fault injection uses a seeded RNG based on a hash of the seed and
/// operation counter, ensuring reproducible behavior across test runs.
///
/// # Cloning
///
/// Clones share the same underlying object store (via `Arc`), allowing
/// multiple handles to observe and modify the same state.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct SimulatedObjectStorage {
    /// In-memory objects using interior mutability.
    objects: Arc<Mutex<HashMap<ObjectKey, Vec<u8>>>>,
    /// Fault injection configuration.
    fault_config: Arc<Mutex<ObjectStorageFaultConfig>>,
    /// RNG seed for deterministic fault injection.
    seed: u64,
    /// Operation counter for deterministic RNG.
    counter: Arc<AtomicU64>,
}

impl SimulatedObjectStorage {
    /// Creates a new simulated storage with the given seed.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(ObjectStorageFaultConfig::default())),
            seed,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a new simulated storage with fault injection enabled.
    #[must_use]
    pub fn with_faults(seed: u64, config: ObjectStorageFaultConfig) -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            fault_config: Arc::new(Mutex::new(config)),
            seed,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns a reference to the fault configuration for modification.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn fault_config(&self) -> std::sync::MutexGuard<'_, ObjectStorageFaultConfig> {
        self.fault_config.lock().expect("fault config lock poisoned")
    }

    /// Gets the raw object content for inspection in tests.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn get_raw_content(&self, key: &ObjectKey) -> Option<Vec<u8>> {
        let objects = self.objects.lock().expect("objects lock poisoned");
        objects.get(key).cloned()
    }

    /// Sets raw object content directly (for simulating pre-existing data).
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn set_raw_content(&self, key: &ObjectKey, content: Vec<u8>) {
        let mut objects = self.objects.lock().expect("objects lock poisoned");
        objects.insert(key.clone(), content);
    }

    /// Corrupts bytes at the specified offset in an object.
    ///
    /// Bytes are XOR'd with `0xFF` to flip all bits.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn corrupt_bytes(&self, key: &ObjectKey, offset: usize, len: usize) {
        let mut objects = self.objects.lock().expect("objects lock poisoned");
        if let Some(content) = objects.get_mut(key) {
            let end = std::cmp::min(offset + len, content.len());
            for byte in &mut content[offset..end] {
                *byte ^= 0xFF;
            }
        }
    }

    /// Returns the number of objects stored.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    #[must_use]
    pub fn object_count(&self) -> usize {
        let objects = self.objects.lock().expect("objects lock poisoned");
        objects.len()
    }

    /// Clears all objects from storage.
    ///
    /// # Panics
    ///
    /// Panics if the mutex is poisoned.
    pub fn clear(&self) {
        let mut objects = self.objects.lock().expect("objects lock poisoned");
        objects.clear();
    }

    /// Deterministic RNG based on seed and counter.
    ///
    /// Uses the same hash-based approach as `helix-wal::SimulatedStorage`.
    fn should_inject_fault(&self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        if rate >= 1.0 {
            return true;
        }
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        // Hash seed+counter together for proper pseudo-random distribution.
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0x5851_f42d_4c95_7f2d);
        // Safety: precision loss is acceptable for probability calculation.
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < rate
    }

    /// Returns a deterministic byte index for corruption.
    fn corruption_index(&self, len: usize) -> usize {
        if len == 0 {
            return 0;
        }
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        // Different multiplier for independent randomness from fault injection.
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0xc6a4_a793_5bd1_e995);
        // Safety: truncation is acceptable since we immediately take modulo len.
        #[allow(clippy::cast_possible_truncation)]
        let idx = hash as usize;
        idx % len
    }
}

impl Clone for SimulatedObjectStorage {
    fn clone(&self) -> Self {
        Self {
            objects: self.objects.clone(),
            fault_config: self.fault_config.clone(),
            seed: self.seed,
            counter: self.counter.clone(),
        }
    }
}

// Allow significant_drop_tightening since this is test-only simulation code.
#[allow(clippy::significant_drop_tightening)]
#[async_trait]
impl ObjectStorage for SimulatedObjectStorage {
    async fn put(&self, key: &ObjectKey, data: Bytes) -> TierResult<()> {
        // TigerStyle: Assert preconditions.
        assert!(!key.as_str().is_empty(), "object key must not be empty");
        assert!(!data.is_empty(), "data must not be empty");

        // Check for forced failure first.
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_put_fail {
            config.force_put_fail = false;
            drop(config);
            return Err(TierError::UploadFailed {
                key: key.as_str().to_string(),
                message: "simulated upload failure (forced)".to_string(),
            });
        }
        let put_fail_rate = config.put_fail_rate;
        drop(config);

        // Check for probabilistic failure.
        if self.should_inject_fault(put_fail_rate) {
            return Err(TierError::UploadFailed {
                key: key.as_str().to_string(),
                message: "simulated upload failure (random)".to_string(),
            });
        }

        // Store the object.
        let mut objects = self.objects.lock().expect("objects lock poisoned");
        let data_len = data.len();
        objects.insert(key.clone(), data.to_vec());

        // TigerStyle: Assert postcondition - data was stored.
        assert!(
            objects.get(key).is_some_and(|v| v.len() == data_len),
            "data should be stored after put"
        );
        Ok(())
    }

    async fn get(&self, key: &ObjectKey) -> TierResult<Bytes> {
        // TigerStyle: Assert precondition.
        assert!(!key.as_str().is_empty(), "object key must not be empty");

        // Check for forced failure first.
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_get_fail {
            config.force_get_fail = false;
            drop(config);
            return Err(TierError::DownloadFailed {
                key: key.as_str().to_string(),
                message: "simulated download failure (forced)".to_string(),
            });
        }

        let should_corrupt = if config.force_get_corruption {
            config.force_get_corruption = false;
            true
        } else {
            false
        };
        let get_fail_rate = config.get_fail_rate;
        let get_corruption_rate = config.get_corruption_rate;
        drop(config);

        // Check for probabilistic failure.
        if self.should_inject_fault(get_fail_rate) {
            return Err(TierError::DownloadFailed {
                key: key.as_str().to_string(),
                message: "simulated download failure (random)".to_string(),
            });
        }

        // Retrieve the object.
        let objects = self.objects.lock().expect("objects lock poisoned");
        let data = objects.get(key).ok_or_else(|| TierError::NotFound {
            key: key.as_str().to_string(),
        })?;

        let mut result = data.clone();

        // Apply corruption if needed.
        let should_corrupt = should_corrupt || self.should_inject_fault(get_corruption_rate);
        if should_corrupt && !result.is_empty() {
            let corrupt_idx = self.corruption_index(result.len());
            result[corrupt_idx] ^= 0xFF;
        }

        Ok(Bytes::from(result))
    }

    async fn delete(&self, key: &ObjectKey) -> TierResult<()> {
        // TigerStyle: Assert precondition.
        assert!(!key.as_str().is_empty(), "object key must not be empty");

        // Check for forced failure first.
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_delete_fail {
            config.force_delete_fail = false;
            drop(config);
            return Err(TierError::Io {
                operation: "delete",
                message: "simulated delete failure (forced)".to_string(),
            });
        }
        let delete_fail_rate = config.delete_fail_rate;
        drop(config);

        // Check for probabilistic failure.
        if self.should_inject_fault(delete_fail_rate) {
            return Err(TierError::Io {
                operation: "delete",
                message: "simulated delete failure (random)".to_string(),
            });
        }

        // Delete the object.
        let mut objects = self.objects.lock().expect("objects lock poisoned");
        objects.remove(key);

        // TigerStyle: Assert postcondition - key no longer exists.
        assert!(!objects.contains_key(key), "key should not exist after delete");
        Ok(())
    }

    async fn list(&self, prefix: &str) -> TierResult<Vec<ObjectKey>> {
        let objects = self.objects.lock().expect("objects lock poisoned");
        let mut result: Vec<_> = objects
            .keys()
            .filter(|k| k.as_str().starts_with(prefix))
            .cloned()
            .collect();
        result.sort_by(|a, b| a.as_str().cmp(b.as_str()));

        // TigerStyle: Assert postcondition - all results match prefix.
        assert!(
            result.iter().all(|k| k.as_str().starts_with(prefix)),
            "all listed keys should start with prefix"
        );
        Ok(result)
    }

    async fn exists(&self, key: &ObjectKey) -> TierResult<bool> {
        // TigerStyle: Assert precondition.
        assert!(!key.as_str().is_empty(), "object key must not be empty");

        // Check for forced failure first.
        let mut config = self.fault_config.lock().expect("fault config lock poisoned");
        if config.force_exists_fail {
            config.force_exists_fail = false;
            drop(config);
            return Err(TierError::Io {
                operation: "exists",
                message: "simulated exists failure (forced)".to_string(),
            });
        }
        let exists_fail_rate = config.exists_fail_rate;
        drop(config);

        // Check for probabilistic failure.
        if self.should_inject_fault(exists_fail_rate) {
            return Err(TierError::Io {
                operation: "exists",
                message: "simulated exists failure (random)".to_string(),
            });
        }

        let objects = self.objects.lock().expect("objects lock poisoned");
        Ok(objects.contains_key(key))
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key_from_segment() {
        let key = ObjectKey::from_segment(1, 2, 3);
        assert_eq!(key.as_str(), "1/2/segment-00000003.wal");

        let key = ObjectKey::from_segment(100, 200, 0x1234_5678);
        assert_eq!(key.as_str(), "100/200/segment-12345678.wal");
    }

    #[tokio::test]
    async fn test_simulated_storage_basic_operations() {
        let storage = SimulatedObjectStorage::new(42);
        let key = ObjectKey::from_segment(1, 0, 1);

        // Initially not exists.
        assert!(!storage.exists(&key).await.unwrap());
        assert_eq!(storage.object_count(), 0);

        // Put.
        let data = Bytes::from("test data");
        storage.put(&key, data.clone()).await.unwrap();

        // Exists.
        assert!(storage.exists(&key).await.unwrap());
        assert_eq!(storage.object_count(), 1);

        // Get.
        let retrieved = storage.get(&key).await.unwrap();
        assert_eq!(retrieved, data);

        // Delete.
        storage.delete(&key).await.unwrap();
        assert!(!storage.exists(&key).await.unwrap());
        assert_eq!(storage.object_count(), 0);
    }

    #[tokio::test]
    async fn test_simulated_storage_not_found() {
        let storage = SimulatedObjectStorage::new(42);
        let key = ObjectKey::from_segment(1, 0, 999);

        let result = storage.get(&key).await;
        assert!(matches!(result, Err(TierError::NotFound { .. })));
    }

    #[tokio::test]
    async fn test_simulated_storage_put_failure() {
        let config = ObjectStorageFaultConfig::none().with_force_put_fail();
        let storage = SimulatedObjectStorage::with_faults(42, config);
        let key = ObjectKey::from_segment(1, 0, 1);

        // First put should fail.
        let result = storage.put(&key, Bytes::from("data")).await;
        assert!(matches!(result, Err(TierError::UploadFailed { .. })));

        // Second put should succeed (one-shot cleared).
        let result = storage.put(&key, Bytes::from("data")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simulated_storage_get_failure() {
        let storage = SimulatedObjectStorage::new(42);
        let key = ObjectKey::from_segment(1, 0, 1);

        // First put data.
        storage.put(&key, Bytes::from("data")).await.unwrap();

        // Force get failure.
        storage.fault_config().force_get_fail = true;

        // First get should fail.
        let result = storage.get(&key).await;
        assert!(matches!(result, Err(TierError::DownloadFailed { .. })));

        // Second get should succeed.
        let result = storage.get(&key).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_simulated_storage_get_corruption() {
        let config = ObjectStorageFaultConfig::none().with_force_get_corruption();
        let storage = SimulatedObjectStorage::with_faults(42, config);
        let key = ObjectKey::from_segment(1, 0, 1);

        let original = Bytes::from("original data");
        storage.put(&key, original.clone()).await.unwrap();

        // First get should be corrupted.
        let corrupted = storage.get(&key).await.unwrap();
        assert_ne!(corrupted, original);

        // Second get should be clean.
        let clean = storage.get(&key).await.unwrap();
        assert_eq!(clean, original);
    }

    #[tokio::test]
    async fn test_simulated_storage_list() {
        let storage = SimulatedObjectStorage::new(42);

        // Add objects with different prefixes.
        storage
            .put(&ObjectKey::from_segment(1, 0, 1), Bytes::from("a"))
            .await
            .unwrap();
        storage
            .put(&ObjectKey::from_segment(1, 0, 2), Bytes::from("b"))
            .await
            .unwrap();
        storage
            .put(&ObjectKey::from_segment(2, 0, 1), Bytes::from("c"))
            .await
            .unwrap();

        // List topic 1.
        let keys = storage.list("1/").await.unwrap();
        assert_eq!(keys.len(), 2);

        // List topic 2.
        let keys = storage.list("2/").await.unwrap();
        assert_eq!(keys.len(), 1);

        // List all.
        let keys = storage.list("").await.unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[tokio::test]
    async fn test_simulated_storage_clone_shares_state() {
        let storage = SimulatedObjectStorage::new(42);
        let key = ObjectKey::from_segment(1, 0, 1);

        storage.put(&key, Bytes::from("data")).await.unwrap();

        let storage2 = storage.clone();
        assert!(storage2.exists(&key).await.unwrap());

        // Delete through clone.
        storage2.delete(&key).await.unwrap();

        // Original sees the deletion.
        assert!(!storage.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn test_simulated_storage_raw_content_manipulation() {
        let storage = SimulatedObjectStorage::new(42);
        let key = ObjectKey::from_segment(1, 0, 1);

        // Set raw content directly.
        storage.set_raw_content(&key, b"raw data".to_vec());

        // Read via get.
        let data = storage.get(&key).await.unwrap();
        assert_eq!(&data[..], b"raw data");

        // Get raw content.
        let raw = storage.get_raw_content(&key).unwrap();
        assert_eq!(raw, b"raw data");

        // Corrupt bytes.
        storage.corrupt_bytes(&key, 0, 3);
        let corrupted = storage.get_raw_content(&key).unwrap();
        assert_ne!(&corrupted[0..3], &b"raw"[..]);
    }

    #[tokio::test]
    async fn test_simulated_storage_deterministic_faults() {
        // Same seed should produce same fault pattern.
        let config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.5);
        let storage1 = SimulatedObjectStorage::with_faults(12345, config.clone());
        let storage2 = SimulatedObjectStorage::with_faults(12345, config);

        let key = ObjectKey::from_segment(1, 0, 1);
        let data = Bytes::from("test");

        // Run 10 puts on each storage.
        let mut results1 = Vec::new();
        let mut results2 = Vec::new();

        for _ in 0..10 {
            results1.push(storage1.put(&key, data.clone()).await.is_ok());
            results2.push(storage2.put(&key, data.clone()).await.is_ok());
        }

        // Results should be identical.
        assert_eq!(results1, results2);
    }

    #[tokio::test]
    async fn test_simulated_storage_different_seeds_different_faults() {
        let config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.5);
        let storage1 = SimulatedObjectStorage::with_faults(12345, config.clone());
        let storage2 = SimulatedObjectStorage::with_faults(54321, config);

        let key = ObjectKey::from_segment(1, 0, 1);
        let data = Bytes::from("test");

        // Run 20 puts on each storage.
        let mut results1 = Vec::new();
        let mut results2 = Vec::new();

        for _ in 0..20 {
            results1.push(storage1.put(&key, data.clone()).await.is_ok());
            results2.push(storage2.put(&key, data.clone()).await.is_ok());
        }

        // Results should differ (with very high probability for 20 trials at 50%).
        assert_ne!(results1, results2);
    }
}

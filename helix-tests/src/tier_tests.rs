//! Tiered storage simulation tests.
//!
//! These tests verify tiering correctness under fault conditions using
//! `SimulatedObjectStorage` with deterministic fault injection.
//!
//! # Test Categories
//!
//! 1. **Upload Failures**: Verify retry logic and state consistency on upload failure
//! 2. **Download Failures**: Verify fetch retries and corruption detection
//! 3. **Metadata Consistency**: Segment state transitions remain consistent
//! 4. **Multi-Seed Stress**: Same scenario with different seeds for coverage

use bytes::Bytes;
use helix_core::{PartitionId, TopicId};
use helix_tier::{
    InMemoryMetadataStore, ObjectStorage, ObjectStorageFaultConfig, SegmentLocation,
    SegmentMetadata, SimulatedObjectStorage, TierError, TieringConfig, TieringManager,
};
use helix_wal::SegmentId;

// ============================================================================
// Helper Functions
// ============================================================================

fn test_segment_id(id: u32) -> SegmentId {
    SegmentId::new(u64::from(id))
}

fn test_metadata(segment_id: u32, topic_id: u64, partition_id: u64) -> SegmentMetadata {
    SegmentMetadata::new(
        test_segment_id(segment_id),
        TopicId::new(topic_id),
        PartitionId::new(partition_id),
        0,
    )
}

/// Creates a tiering manager with the given seed and fault config.
fn create_manager(
    seed: u64,
    fault_config: ObjectStorageFaultConfig,
) -> TieringManager<SimulatedObjectStorage, InMemoryMetadataStore> {
    let storage = SimulatedObjectStorage::with_faults(seed, fault_config);
    let metadata_store = InMemoryMetadataStore::new();
    TieringManager::new(storage, metadata_store, TieringConfig::for_testing())
}

// ============================================================================
// DST Test 1: Upload with Forced Failure
// ============================================================================

/// Tests that a forced upload failure returns an error and leaves state unchanged.
#[tokio::test]
async fn test_upload_forced_failure_leaves_state_unchanged() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register and prepare segment.
    let meta = test_metadata(1, 1, 0);
    manager.register_segment(meta).await.unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();
    manager.mark_committed(test_segment_id(1)).await.unwrap();

    // Force next upload to fail.
    manager.storage().fault_config().force_put_fail = true;

    // Attempt upload - should fail.
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("segment data"))
        .await;
    assert!(matches!(result, Err(TierError::UploadFailed { .. })));

    // State should still be Local.
    let location = manager.get_location(test_segment_id(1)).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Local));

    // S3 should not have the segment.
    assert!(!manager.exists_in_s3(test_segment_id(1)).await.unwrap());
}

/// Tests successful retry after a forced failure.
#[tokio::test]
async fn test_upload_retry_after_failure_succeeds() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register and prepare segment.
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();
    manager.mark_committed(test_segment_id(1)).await.unwrap();

    // Force first upload to fail.
    manager.storage().fault_config().force_put_fail = true;
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("segment data"))
        .await;
    assert!(result.is_err());

    // Retry - should succeed.
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("segment data"))
        .await;
    assert!(result.is_ok());

    // State should be Both.
    let location = manager.get_location(test_segment_id(1)).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Both));
}

// ============================================================================
// DST Test 2: Download with Forced Failure
// ============================================================================

/// Tests that a forced download failure returns an error.
#[tokio::test]
async fn test_download_forced_failure() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register, seal, commit, and upload.
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();
    manager.mark_committed(test_segment_id(1)).await.unwrap();
    manager
        .upload_segment(test_segment_id(1), Bytes::from("segment data"))
        .await
        .unwrap();

    // Force next download to fail.
    manager.storage().fault_config().force_get_fail = true;

    // Attempt download - should fail.
    let result = manager.download_segment(test_segment_id(1)).await;
    assert!(matches!(result, Err(TierError::DownloadFailed { .. })));

    // Retry - should succeed.
    let data = manager.download_segment(test_segment_id(1)).await.unwrap();
    assert_eq!(data, Bytes::from("segment data"));
}

// ============================================================================
// DST Test 3: Download with Corruption Detection
// ============================================================================

/// Tests that forced corruption is detected during download.
/// Note: The actual corruption detection requires CRC verification at a higher level,
/// but this test verifies the corruption injection mechanism works.
#[tokio::test]
async fn test_download_corruption_injection() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register, seal, commit, and upload.
    let original_data = Bytes::from("segment data for corruption test");
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();
    manager.mark_committed(test_segment_id(1)).await.unwrap();
    manager
        .upload_segment(test_segment_id(1), original_data.clone())
        .await
        .unwrap();

    // Force next download to return corrupted data.
    manager.storage().fault_config().force_get_corruption = true;

    // Download - should return corrupted data (different from original).
    let corrupted_data = manager.download_segment(test_segment_id(1)).await.unwrap();
    assert_ne!(corrupted_data, original_data, "Data should be corrupted");

    // Next download should be clean (one-shot corruption).
    let clean_data = manager.download_segment(test_segment_id(1)).await.unwrap();
    assert_eq!(clean_data, original_data, "Data should be clean after one-shot");
}

// ============================================================================
// DST Test 4: Probabilistic Upload Failures
// ============================================================================

/// Tests behavior with probabilistic upload failures.
/// Aggregates results across multiple seeds to ensure statistical validity.
#[tokio::test]
async fn test_probabilistic_upload_failures() {
    // Test across multiple seeds to ensure the fault injection works.
    let seeds = [42, 123, 456, 789, 1111];
    let mut total_successes = 0;
    let mut total_failures = 0;

    for seed in seeds {
        let config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.3);
        let manager = create_manager(seed, config);

        // Try 20 uploads with different segments per seed.
        for i in 1..=20 {
            let seg_id = i;
            manager
                .register_segment(test_metadata(seg_id, 1, 0))
                .await
                .unwrap();
            manager.mark_sealed(test_segment_id(seg_id)).await.unwrap();
            manager.mark_committed(test_segment_id(seg_id)).await.unwrap();

            let result = manager
                .upload_segment(test_segment_id(seg_id), Bytes::from(format!("data-{i}")))
                .await;

            if result.is_ok() {
                total_successes += 1;
            } else {
                total_failures += 1;
            }
        }
    }

    // With 30% rate across 100 attempts (5 seeds * 20 attempts), expect mix.
    // Expected: ~70 successes, ~30 failures.
    assert!(
        total_successes >= 30,
        "Expected at least 30 successes with 30% failure rate across 100 attempts, got {total_successes}"
    );
    assert!(
        total_failures >= 10,
        "Expected at least 10 failures with 30% failure rate across 100 attempts, got {total_failures}"
    );
}

// ============================================================================
// DST Test 5: Deterministic Fault Injection
// ============================================================================

/// Tests that the same seed produces the same sequence of faults.
#[tokio::test]
async fn test_deterministic_faults_same_seed() {
    let config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.3);

    let mut results1 = Vec::new();
    let mut results2 = Vec::new();

    // Run with seed 123.
    let manager = create_manager(123, config.clone());
    for i in 1..=10 {
        manager
            .register_segment(test_metadata(i, 1, 0))
            .await
            .unwrap();
        manager.mark_sealed(test_segment_id(i)).await.unwrap();
        manager.mark_committed(test_segment_id(i)).await.unwrap();
        let result = manager
            .upload_segment(test_segment_id(i), Bytes::from("data"))
            .await;
        results1.push(result.is_ok());
    }

    // Run with same seed 123 again.
    let manager = create_manager(123, config);
    for i in 1..=10 {
        manager
            .register_segment(test_metadata(i, 1, 0))
            .await
            .unwrap();
        manager.mark_sealed(test_segment_id(i)).await.unwrap();
        manager.mark_committed(test_segment_id(i)).await.unwrap();
        let result = manager
            .upload_segment(test_segment_id(i), Bytes::from("data"))
            .await;
        results2.push(result.is_ok());
    }

    // Results should be identical.
    assert_eq!(results1, results2, "Same seed should produce same results");
}

/// Tests that different seeds produce different fault sequences.
#[tokio::test]
async fn test_deterministic_faults_different_seeds() {
    let config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.5);

    let mut results_seed1 = Vec::new();
    let mut results_seed2 = Vec::new();

    // Run with seed 42.
    let manager = create_manager(42, config.clone());
    for i in 1..=20 {
        manager
            .register_segment(test_metadata(i, 1, 0))
            .await
            .unwrap();
        manager.mark_sealed(test_segment_id(i)).await.unwrap();
        manager.mark_committed(test_segment_id(i)).await.unwrap();
        let result = manager
            .upload_segment(test_segment_id(i), Bytes::from("data"))
            .await;
        results_seed1.push(result.is_ok());
    }

    // Run with seed 999.
    let manager = create_manager(999, config);
    for i in 1..=20 {
        manager
            .register_segment(test_metadata(i, 1, 0))
            .await
            .unwrap();
        manager.mark_sealed(test_segment_id(i)).await.unwrap();
        manager.mark_committed(test_segment_id(i)).await.unwrap();
        let result = manager
            .upload_segment(test_segment_id(i), Bytes::from("data"))
            .await;
        results_seed2.push(result.is_ok());
    }

    // Results should differ (with very high probability).
    assert_ne!(
        results_seed1, results_seed2,
        "Different seeds should produce different results"
    );
}

// ============================================================================
// DST Test 6: Multi-Partition Upload
// ============================================================================

/// Tests uploading segments from multiple partitions.
#[tokio::test]
async fn test_multi_partition_upload() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Upload segments from different partitions.
    for partition in 0..3 {
        for segment in 1..=3 {
            let segment_id = partition * 10 + segment;
            manager
                .register_segment(test_metadata(segment_id, 1, u64::from(partition)))
                .await
                .unwrap();
            manager.mark_sealed(test_segment_id(segment_id)).await.unwrap();
            manager
                .mark_committed(test_segment_id(segment_id))
                .await
                .unwrap();
            manager
                .upload_segment(
                    test_segment_id(segment_id),
                    Bytes::from(format!("p{partition}-s{segment}")),
                )
                .await
                .unwrap();
        }
    }

    // Verify all 9 segments are uploaded.
    for partition in 0..3 {
        for segment in 1..=3 {
            let segment_id = partition * 10 + segment;
            assert!(manager.exists_in_s3(test_segment_id(segment_id)).await.unwrap());

            // Verify correct data.
            let data = manager.download_segment(test_segment_id(segment_id)).await.unwrap();
            assert_eq!(data, Bytes::from(format!("p{partition}-s{segment}")));
        }
    }
}

// ============================================================================
// DST Test 7: Eligibility Enforcement
// ============================================================================

/// Tests that upload fails if segment is not sealed.
#[tokio::test]
async fn test_upload_requires_sealed() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register but don't seal.
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();

    // Upload should fail.
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("data"))
        .await;
    assert!(matches!(result, Err(TierError::NotEligible { .. })));
}

/// Tests that upload fails if segment is not committed.
#[tokio::test]
async fn test_upload_requires_committed() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register and seal but don't commit.
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();

    // Upload should fail.
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("data"))
        .await;
    assert!(matches!(result, Err(TierError::NotEligible { .. })));
}

/// Tests that upload fails if segment is already uploaded.
#[tokio::test]
async fn test_upload_fails_if_already_uploaded() {
    let config = ObjectStorageFaultConfig::none();
    let manager = create_manager(42, config);

    // Register, seal, commit, and upload.
    manager
        .register_segment(test_metadata(1, 1, 0))
        .await
        .unwrap();
    manager.mark_sealed(test_segment_id(1)).await.unwrap();
    manager.mark_committed(test_segment_id(1)).await.unwrap();
    manager
        .upload_segment(test_segment_id(1), Bytes::from("data"))
        .await
        .unwrap();

    // Second upload should fail.
    let result = manager
        .upload_segment(test_segment_id(1), Bytes::from("data"))
        .await;
    assert!(matches!(result, Err(TierError::NotEligible { .. })));
}

// ============================================================================
// DST Test 8: Multi-Seed Stress Test
// ============================================================================

/// Stress test running the same scenario with multiple seeds.
#[tokio::test]
async fn test_multi_seed_stress() {
    let seeds = [1, 42, 123, 999, 12345, 0xDEAD_BEEF, 0xCAFE_BABE];

    for seed in seeds {
        let config = ObjectStorageFaultConfig::flaky();
        let manager = create_manager(seed, config);

        // Run a typical tiering workflow.
        for i in 1..=10 {
            manager
                .register_segment(test_metadata(i, 1, 0))
                .await
                .unwrap();
            manager.mark_sealed(test_segment_id(i)).await.unwrap();
            manager.mark_committed(test_segment_id(i)).await.unwrap();

            // Retry upload up to 5 times.
            let mut uploaded = false;
            for _attempt in 0..5 {
                if manager
                    .upload_segment(test_segment_id(i), Bytes::from(format!("data-{i}")))
                    .await
                    .is_ok()
                {
                    uploaded = true;
                    break;
                }
            }

            // With 1% failure rate and 5 retries, should succeed.
            if !uploaded {
                // This is very unlikely but could happen - just skip verification.
                continue;
            }

            // Verify download works (may need retry).
            let mut downloaded = false;
            for _attempt in 0..5 {
                if let Ok(data) = manager.download_segment(test_segment_id(i)).await {
                    assert_eq!(data, Bytes::from(format!("data-{i}")));
                    downloaded = true;
                    break;
                }
            }
            assert!(downloaded, "Failed to download after 5 retries with seed {seed}");
        }
    }
}

// ============================================================================
// DST Test 9: Clone Shares State
// ============================================================================

/// Tests that cloned storage shares state with the original.
#[tokio::test]
async fn test_storage_clone_shares_state() {
    let storage = SimulatedObjectStorage::new(42);
    let storage_clone = storage.clone();

    // Put via original.
    let key = helix_tier::ObjectKey::new("test/key");
    storage
        .put(&key, Bytes::from("original data"))
        .await
        .unwrap();

    // Get via clone - should see the data.
    let data = storage_clone.get(&key).await.unwrap();
    assert_eq!(data, Bytes::from("original data"));

    // Update via clone.
    storage_clone
        .put(&key, Bytes::from("updated data"))
        .await
        .unwrap();

    // Get via original - should see update.
    let data = storage.get(&key).await.unwrap();
    assert_eq!(data, Bytes::from("updated data"));
}

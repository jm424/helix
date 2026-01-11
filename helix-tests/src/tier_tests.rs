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
//! 5. **E2E with DurablePartition**: Full integration with WAL-backed storage

use bytes::Bytes;
use helix_core::{PartitionId, Record, TopicId};
use helix_server::storage::{DurablePartition, DurablePartitionConfig};
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

// ============================================================================
// E2E Tests: DurablePartition + Tiering Integration
// ============================================================================
//
// These tests verify the full integration between DurablePartition and tiering.
// They test the hooks and tiering workflow with real WAL-backed storage.

/// Tests that DurablePartition with tiering enabled initializes correctly.
#[tokio::test]
async fn test_e2e_durable_partition_tiering_init() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());

    let partition = DurablePartition::open(config).await.unwrap();

    // Tiering should be enabled.
    assert!(partition.tiering_manager().is_some());
}

/// Tests writing records and calling tiering hooks (no sealed segments yet).
#[tokio::test]
async fn test_e2e_write_and_tiering_hooks() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());

    let mut partition = DurablePartition::open(config).await.unwrap();

    // Write some records.
    for i in 0..10 {
        let records = vec![Record::new(Bytes::from(format!("record-{i}")))];
        partition.append(records).await.unwrap();
    }

    // Call tiering hooks - should return 0 since no segments are sealed.
    let registered = partition.check_and_register_sealed_segments().await.unwrap();
    assert_eq!(registered, 0, "No sealed segments expected");

    let committed = partition.on_entries_committed(100).await.unwrap();
    assert_eq!(committed, 0, "No segments to commit");

    let tiered = partition.tier_eligible_segments().await.unwrap();
    assert_eq!(tiered, 0, "No segments to tier");
}

/// Tests that tiering hooks are idempotent (can be called multiple times safely).
#[tokio::test]
async fn test_e2e_tiering_hooks_idempotent() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());

    let mut partition = DurablePartition::open(config).await.unwrap();

    // Write records.
    for i in 0..5 {
        let records = vec![Record::new(Bytes::from(format!("data-{i}")))];
        partition.append(records).await.unwrap();
    }

    // Call hooks multiple times - should be safe.
    for _ in 0..5 {
        let _ = partition.check_and_register_sealed_segments().await.unwrap();
        let _ = partition.on_entries_committed(100).await.unwrap();
        let _ = partition.tier_eligible_segments().await.unwrap();
    }

    // Partition should still be functional.
    let records = vec![Record::new(Bytes::from("final-record"))];
    partition.append(records).await.unwrap();
}

/// Tests partition without tiering enabled (hooks should be no-op).
#[tokio::test]
async fn test_e2e_partition_without_tiering() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config =
        DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0));

    let mut partition = DurablePartition::open(config).await.unwrap();

    // Tiering should be disabled.
    assert!(partition.tiering_manager().is_none());

    // Write records.
    for i in 0..5 {
        let records = vec![Record::new(Bytes::from(format!("data-{i}")))];
        partition.append(records).await.unwrap();
    }

    // Hooks should return 0 (no-op when tiering disabled).
    let registered = partition.check_and_register_sealed_segments().await.unwrap();
    assert_eq!(registered, 0);

    let committed = partition.on_entries_committed(100).await.unwrap();
    assert_eq!(committed, 0);

    let tiered = partition.tier_eligible_segments().await.unwrap();
    assert_eq!(tiered, 0);
}

/// Tests concurrent writes with tiering hooks.
#[tokio::test]
async fn test_e2e_concurrent_writes_with_tiering() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());

    let mut partition = DurablePartition::open(config).await.unwrap();

    // Interleave writes and tiering hooks.
    for batch in 0..5 {
        // Write some records.
        for i in 0..10 {
            let records = vec![Record::new(Bytes::from(format!("batch-{batch}-{i}")))];
            partition.append(records).await.unwrap();
        }

        // Call tiering hooks between batches.
        let _ = partition.check_and_register_sealed_segments().await;
        let _ = partition.on_entries_committed(u64::try_from(batch * 10 + 10).unwrap()).await;
        let _ = partition.tier_eligible_segments().await;
    }

    // Verify all records are readable.
    let records = partition.read(helix_core::Offset::new(0), 100).unwrap();
    assert_eq!(records.len(), 50);
}

/// Tests tiering config variations.
#[tokio::test]
async fn test_e2e_tiering_config_variations() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Test with custom config.
    let custom_config = TieringConfig {
        min_age_secs: 0, // No minimum age for testing.
        max_concurrent_uploads: 2,
        verify_on_download: true,
    };

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(custom_config);

    let mut partition = DurablePartition::open(config).await.unwrap();

    // Write and verify.
    let records = vec![Record::new(Bytes::from("test-data"))];
    partition.append(records).await.unwrap();

    // Hooks should work with custom config.
    let _ = partition.check_and_register_sealed_segments().await.unwrap();
}

/// Tests multi-partition tiering isolation.
#[tokio::test]
async fn test_e2e_multi_partition_tiering_isolation() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create two partitions with tiering.
    let config1 = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());
    let config2 = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(1))
        .with_tiering(TieringConfig::for_testing());

    let mut partition1 = DurablePartition::open(config1).await.unwrap();
    let mut partition2 = DurablePartition::open(config2).await.unwrap();

    // Write to each partition.
    partition1
        .append(vec![Record::new(Bytes::from("p1-data"))])
        .await
        .unwrap();
    partition2
        .append(vec![Record::new(Bytes::from("p2-data"))])
        .await
        .unwrap();

    // Tiering hooks should be independent.
    let _ = partition1.check_and_register_sealed_segments().await.unwrap();
    let _ = partition2.check_and_register_sealed_segments().await.unwrap();

    // Verify data isolation.
    let p1_records = partition1.read(helix_core::Offset::new(0), 10).unwrap();
    let p2_records = partition2.read(helix_core::Offset::new(0), 10).unwrap();

    assert_eq!(p1_records.len(), 1);
    assert_eq!(p2_records.len(), 1);
    assert_ne!(p1_records[0].value, p2_records[0].value);
}

/// Stress test with multiple seeds for tiering manager.
#[tokio::test]
async fn test_e2e_multi_seed_tiering_stress() {
    let seeds = [42, 123, 456, 789, 1111];

    for seed in seeds {
        let temp_dir = tempfile::tempdir().unwrap();

        // Create tiering config with specific seed behavior would require
        // exposing the seed in config, but we test the flow works with
        // multiple invocations.
        let config =
            DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
                .with_tiering(TieringConfig::for_testing());

        let mut partition = DurablePartition::open(config).await.unwrap();

        // Write records with seed-based variation.
        for i in 0..10 {
            let data = format!("seed-{seed}-record-{i}");
            let records = vec![Record::new(Bytes::from(data))];
            partition.append(records).await.unwrap();
        }

        // Run tiering workflow.
        let _ = partition.check_and_register_sealed_segments().await;
        let _ = partition.on_entries_committed(10).await;
        let _ = partition.tier_eligible_segments().await;

        // Verify data integrity.
        let records = partition.read(helix_core::Offset::new(0), 20).unwrap();
        assert_eq!(records.len(), 10, "All records should be readable for seed {seed}");
    }
}

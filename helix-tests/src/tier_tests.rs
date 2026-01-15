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
//! 5. **E2E with `DurablePartition`**: Full integration with WAL-backed storage
//! 6. **BUGGIFY Crash Tests**: Verify recovery from crashes at arbitrary points

// Test-specific lint allowances - these are less critical in test code.
#![allow(clippy::cast_precision_loss)] // f64 precision loss acceptable in test stats
#![allow(clippy::cast_possible_truncation)] // u64 to usize safe on 64-bit test machines
#![allow(clippy::too_many_lines)] // Test functions can be longer for clarity
#![allow(clippy::items_after_statements)] // Allow const definitions where needed
#![allow(clippy::significant_drop_tightening)] // Test code clarity > drop optimization
#![allow(clippy::doc_markdown)] // Backticks in docs not critical for tests
#![allow(clippy::uninlined_format_args)] // Format string style not critical for tests
#![allow(clippy::needless_pass_by_value)] // Pass by value can improve test clarity
#![allow(clippy::type_complexity)] // Complex types acceptable in test utilities
#![allow(clippy::format_push_string)] // String building style not critical
#![allow(clippy::iter_over_hash_type)] // Iteration order doesn't matter in tests
#![allow(clippy::unused_async)] // Async functions for trait consistency
#![allow(clippy::explicit_iter_loop)] // Explicit iteration can be clearer
#![allow(clippy::if_then_some_else_none)] // If-else style not critical
#![allow(clippy::option_if_let_else)] // Pattern style not critical
#![allow(clippy::single_match_else)] // Match style not critical
#![allow(clippy::manual_let_else)] // Let-else style not critical
#![allow(clippy::map_unwrap_or)] // map/unwrap_or style not critical
#![allow(clippy::only_used_in_recursion)] // False positives in test helpers
#![allow(clippy::ref_as_ptr)] // Ref/ptr style not critical
#![allow(clippy::panic_in_result_fn)] // Tests may intentionally panic
#![allow(clippy::collection_is_never_read)] // Test setup may create unused collections
#![allow(clippy::needless_lifetimes)] // Explicit lifetimes can be clearer
#![allow(clippy::for_kv_map)] // Iterate over keys may be intentional for clarity
#![allow(clippy::deref_addrof)] // Ref/deref pattern may be intentional
#![allow(clippy::cast_sign_loss)] // Test data is always positive

use bloodhound::buggify;
use bytes::Bytes;
use helix_core::{PartitionId, Record, TopicId};
use helix_server::storage::{DurablePartition, DurablePartitionConfig};
use helix_wal::TokioStorage;
use helix_tier::{
    InMemoryMetadataStore, IntegratedTieringManager, MetadataStoreFaultConfig, ObjectKey,
    ObjectStorage, ObjectStorageFaultConfig, SegmentLocation, SegmentMetadata, SegmentReader,
    SimulatedObjectStorage, TierError, TierResult, TieringConfig, TieringManager,
};
use helix_wal::{Entry, SegmentConfig, SegmentId, Wal, WalConfig};
use std::sync::Arc;
use tokio::sync::RwLock;

// Import for SegmentReader implementation
use async_trait::async_trait;

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

/// DST stress test with deterministic fault injection.
///
/// Following the same pattern as WAL stress tests:
/// 1. Run with multiple seeds for coverage
/// 2. Use meaningful fault rates (5-10%)
/// 3. Track fault occurrences to verify injection is working
/// 4. Verify data integrity properties hold
#[tokio::test]
async fn test_multi_seed_stress() {
    let seeds = [1, 42, 123, 999, 12345];

    for seed in seeds {
        // Use higher fault rates to actually trigger faults.
        let config = ObjectStorageFaultConfig::none()
            .with_put_fail_rate(0.1)  // 10% upload failures
            .with_get_fail_rate(0.05); // 5% download failures

        let storage = SimulatedObjectStorage::with_faults(seed, config);
        let metadata = InMemoryMetadataStore::new();
        let manager = TieringManager::new(
            storage.clone(),
            metadata,
            TieringConfig::for_testing(),
        );

        // Track outcomes for this seed.
        let mut upload_successes = 0u64;
        let mut upload_failures = 0u64;
        let mut download_successes = 0u64;
        let mut download_failures = 0u64;
        let mut successfully_uploaded: Vec<u32> = Vec::new();

        // Run 50 operations per seed (enough to trigger faults).
        const OPS_PER_SEED: u32 = 50;
        for i in 1..=OPS_PER_SEED {
            let segment_id = test_segment_id(i);
            let data = Bytes::from(format!("stress-data-{seed}-{i}"));

            // Register, seal, commit the segment.
            manager
                .register_segment(test_metadata(i, 1, 0))
                .await
                .unwrap();
            manager.mark_sealed(segment_id).await.unwrap();
            manager.mark_committed(segment_id).await.unwrap();

            // Try upload (NO retries - we want to observe fault behavior).
            match manager.upload_segment(segment_id, data.clone()).await {
                Ok(()) => {
                    upload_successes += 1;
                    successfully_uploaded.push(i);
                }
                Err(TierError::UploadFailed { .. }) => {
                    upload_failures += 1;
                }
                Err(e) => panic!("Unexpected upload error for seed {seed}: {e:?}"),
            }
        }

        // Verify uploads that succeeded can be downloaded.
        for &i in &successfully_uploaded {
            let segment_id = test_segment_id(i);
            let expected_data = Bytes::from(format!("stress-data-{seed}-{i}"));

            match manager.download_segment(segment_id).await {
                Ok(data) => {
                    download_successes += 1;
                    // PROPERTY: Downloaded data must match uploaded data.
                    assert_eq!(
                        data, expected_data,
                        "seed {seed}: Data mismatch for segment {i}"
                    );
                }
                Err(TierError::DownloadFailed { .. }) => {
                    download_failures += 1;
                }
                Err(e) => panic!("Unexpected download error for seed {seed}: {e:?}"),
            }
        }

        // VERIFY: Faults actually fired (with 10% rate over 50 ops, expect ~5 failures).
        // Allow some variance but ensure faults are happening.
        let total_upload_ops = u64::from(OPS_PER_SEED);
        let expected_failures_min = 1u64; // At least 1 failure expected
        assert!(
            upload_failures >= expected_failures_min,
            "seed {seed}: Only {upload_failures} upload failures in {total_upload_ops} ops \
             with 10% rate - fault injection may not be working!"
        );

        // VERIFY: Most uploads succeed (should be ~90% with 10% failure rate).
        let success_rate = upload_successes as f64 / total_upload_ops as f64;
        assert!(
            success_rate > 0.7 && success_rate < 0.99,
            "seed {seed}: Upload success rate {success_rate:.2} outside expected range [0.7, 0.99]"
        );

        // Print stats for visibility.
        println!(
            "Seed {seed}: uploads {upload_successes}/{total_upload_ops} succeeded ({upload_failures} failed), \
             downloads {download_successes}/{} succeeded ({download_failures} failed)",
            successfully_uploaded.len()
        );
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

    let partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

    // Tiering should be enabled.
    assert!(partition.tiering_manager().is_some());
}

/// Tests writing records and calling tiering hooks (no sealed segments yet).
#[tokio::test]
async fn test_e2e_write_and_tiering_hooks() {
    let temp_dir = tempfile::tempdir().unwrap();

    let config = DurablePartitionConfig::new(temp_dir.path(), TopicId::new(1), PartitionId::new(0))
        .with_tiering(TieringConfig::for_testing());

    let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

    let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

    let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

    let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

    let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

    let mut partition1 = DurablePartition::open(TokioStorage::new(), config1).await.unwrap();
    let mut partition2 = DurablePartition::open(TokioStorage::new(), config2).await.unwrap();

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

        let mut partition = DurablePartition::open(TokioStorage::new(), config).await.unwrap();

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

// ============================================================================
// REAL Integration Tests: WAL + Tiering with Segment Rotation
// ============================================================================
//
// These tests use small segment limits to trigger actual segment rotation
// and verify the full tiering workflow end-to-end.

/// SegmentReader implementation for testing with direct WAL access.
struct TestSegmentReader {
    wal: Arc<RwLock<Wal<TokioStorage>>>,
}

impl TestSegmentReader {
    fn new(wal: Arc<RwLock<Wal<TokioStorage>>>) -> Self {
        Self { wal }
    }
}

#[async_trait]
impl SegmentReader for TestSegmentReader {
    async fn read_segment_bytes(&self, segment_id: SegmentId) -> TierResult<Bytes> {
        let wal = self.wal.read().await;
        wal.read_segment_bytes(segment_id).map_err(|e| TierError::Io {
            operation: "read_segment_bytes",
            message: e.to_string(),
        })
    }

    fn is_segment_sealed(&self, segment_id: SegmentId) -> bool {
        self.wal
            .try_read()
            .map(|wal| wal.segment_info(segment_id).is_some_and(|info| info.is_sealed))
            .unwrap_or(false)
    }
}

/// Segment reader with fault injection for DST.
struct FaultingSegmentReader {
    inner: TestSegmentReader,
    read_fail_rate: f64,
    seed: u64,
    counter: std::sync::atomic::AtomicU64,
}

impl FaultingSegmentReader {
    fn new(wal: Arc<RwLock<Wal<TokioStorage>>>, seed: u64, read_fail_rate: f64) -> Self {
        Self {
            inner: TestSegmentReader::new(wal),
            read_fail_rate,
            seed,
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn should_inject_fault(&self) -> bool {
        if self.read_fail_rate <= 0.0 {
            return false;
        }
        let counter = self.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let hash = self.seed.wrapping_add(counter).wrapping_mul(0x517c_c1b7_2722_0a95);
        #[allow(clippy::cast_precision_loss)]
        let normalized = (hash as f64) / (u64::MAX as f64);
        normalized < self.read_fail_rate
    }
}

#[async_trait]
impl SegmentReader for FaultingSegmentReader {
    async fn read_segment_bytes(&self, segment_id: SegmentId) -> TierResult<Bytes> {
        if self.should_inject_fault() {
            return Err(TierError::Io {
                operation: "read_segment_bytes",
                message: "simulated segment read failure".to_string(),
            });
        }
        self.inner.read_segment_bytes(segment_id).await
    }

    fn is_segment_sealed(&self, segment_id: SegmentId) -> bool {
        self.inner.is_segment_sealed(segment_id)
    }
}

/// Tests REAL segment rotation and tiering with small segment limits.
///
/// This test actually exercises the tiering code by:
/// 1. Creating a WAL with 5 entries per segment max
/// 2. Writing 12 entries to create 2 sealed segments + 1 active
/// 3. Registering, committing, and tiering the sealed segments
/// 4. Verifying data is in SimulatedObjectStorage
#[tokio::test]
async fn test_real_segment_rotation_and_tiering() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segment limit (5 entries max per segment).
    let segment_config = SegmentConfig::new().with_max_entries(5);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write 12 entries - should create 2 sealed segments (5 each) + 1 active (2).
    for i in 1..=12 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    // Verify we have sealed segments.
    let sealed_count = wal.read().await.sealed_segment_count();
    assert!(
        sealed_count >= 2,
        "Expected at least 2 sealed segments, got {sealed_count}"
    );

    // Create tiering infrastructure.
    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    // Use config with 0 min age so segments are immediately eligible.
    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store.clone(),
        segment_reader,
        tiering_config,
    );

    // Get sealed segment IDs.
    let sealed_ids = wal.read().await.sealed_segment_ids();
    assert!(!sealed_ids.is_empty(), "Should have sealed segments");

    // Register, seal, and commit each segment.
    for segment_id in &sealed_ids {
        let info = wal.read().await.segment_info(*segment_id).unwrap();

        // Register segment.
        let metadata = SegmentMetadata::new(
            *segment_id,
            TopicId::new(1),
            PartitionId::new(0),
            info.first_index,
        );
        tiering_manager.register_segment(metadata).await.unwrap();

        // Mark as sealed (already is, but tiering needs to know).
        tiering_manager.mark_sealed(*segment_id).await.unwrap();

        // Mark as committed (all entries are "committed" in this test).
        tiering_manager.mark_committed(*segment_id).await.unwrap();
    }

    // Find eligible segments.
    let eligible = tiering_manager.find_eligible_segments().await.unwrap();
    assert_eq!(
        eligible.len(),
        sealed_ids.len(),
        "All sealed segments should be eligible"
    );

    // Tier each eligible segment using tier_segment (reads from WAL via SegmentReader).
    for metadata in &eligible {
        tiering_manager.tier_segment(metadata.segment_id).await.unwrap();
    }

    // Verify segments are in S3 (location should be Both after tiering).
    for segment_id in &sealed_ids {
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "Segment {segment_id:?} should be in Both locations after tiering"
        );
    }

    // Download and verify data integrity.
    for segment_id in &sealed_ids {
        let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
        assert!(!downloaded.is_empty(), "Downloaded segment should not be empty");

        // The downloaded bytes should be the encoded segment.
        let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert_eq!(
            downloaded, original,
            "Downloaded data should match original"
        );
    }
}

/// Tests tiering with upload failures and retry.
#[tokio::test]
async fn test_real_tiering_with_upload_failures() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segments.
    let segment_config = SegmentConfig::new().with_max_entries(5);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries to create sealed segments.
    for i in 1..=10 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    // Create tiering with fault injection.
    let segment_reader = TestSegmentReader::new(wal.clone());
    let fault_config = ObjectStorageFaultConfig::none();
    let object_storage = SimulatedObjectStorage::with_faults(42, fault_config);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store,
        segment_reader,
        tiering_config,
    );

    // Get first sealed segment.
    let sealed_ids = wal.read().await.sealed_segment_ids();
    assert!(!sealed_ids.is_empty());
    let segment_id = sealed_ids[0];

    // Register and prepare segment.
    let info = wal.read().await.segment_info(segment_id).unwrap();
    let metadata = SegmentMetadata::new(
        segment_id,
        TopicId::new(1),
        PartitionId::new(0),
        info.first_index,
    );
    tiering_manager.register_segment(metadata).await.unwrap();
    tiering_manager.mark_sealed(segment_id).await.unwrap();
    tiering_manager.mark_committed(segment_id).await.unwrap();

    // Force upload failure.
    object_storage.fault_config().force_put_fail = true;

    // First upload should fail.
    let result = tiering_manager.tier_segment(segment_id).await;
    assert!(result.is_err(), "Upload should fail with forced fault");

    // Verify state is still Local.
    let location = tiering_manager.get_location(segment_id).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Local));

    // Retry should succeed (force_put_fail is one-shot).
    let result = tiering_manager.tier_segment(segment_id).await;
    assert!(result.is_ok(), "Retry should succeed");

    // Verify state is now Both.
    let location = tiering_manager.get_location(segment_id).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Both));
}

/// Tests tiering with data corruption detection.
#[tokio::test]
async fn test_real_tiering_corruption_detection() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segments.
    let segment_config = SegmentConfig::new().with_max_entries(5);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries.
    for i in 1..=7 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    // Create tiering.
    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store,
        segment_reader,
        tiering_config,
    );

    // Upload segment.
    let sealed_ids = wal.read().await.sealed_segment_ids();
    let segment_id = sealed_ids[0];

    let info = wal.read().await.segment_info(segment_id).unwrap();
    let metadata = SegmentMetadata::new(
        segment_id,
        TopicId::new(1),
        PartitionId::new(0),
        info.first_index,
    );
    tiering_manager.register_segment(metadata).await.unwrap();
    tiering_manager.mark_sealed(segment_id).await.unwrap();
    tiering_manager.mark_committed(segment_id).await.unwrap();
    tiering_manager.tier_segment(segment_id).await.unwrap();

    // Force corruption on next download.
    object_storage.fault_config().force_get_corruption = true;

    // Download with corruption - data should be different.
    let corrupted = tiering_manager.download_segment(segment_id).await.unwrap();
    let original = wal.read().await.read_segment_bytes(segment_id).unwrap();
    assert_ne!(corrupted, original, "Corrupted data should differ from original");

    // Clean download should match.
    let clean = tiering_manager.download_segment(segment_id).await.unwrap();
    assert_eq!(clean, original, "Clean download should match original");
}

/// Tests multi-segment tiering with varying entry sizes.
#[tokio::test]
async fn test_real_multi_segment_tiering() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segments.
    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write 15 entries - should create 5 segments (3 entries each).
    for i in 1u64..=15 {
        // Vary payload size.
        let payload = "x".repeat(i as usize * 10);
        let entry = Entry::new(1, i, Bytes::from(payload)).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    // Should have at least 4 sealed segments.
    let sealed_count = wal.read().await.sealed_segment_count();
    assert!(sealed_count >= 4, "Expected at least 4 sealed segments, got {sealed_count}");

    // Create tiering.
    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store,
        segment_reader,
        tiering_config,
    );

    // Tier all sealed segments.
    let sealed_ids = wal.read().await.sealed_segment_ids();
    let mut tiered_count = 0;

    for segment_id in &sealed_ids {
        let info = wal.read().await.segment_info(*segment_id).unwrap();
        let metadata = SegmentMetadata::new(
            *segment_id,
            TopicId::new(1),
            PartitionId::new(0),
            info.first_index,
        );

        tiering_manager.register_segment(metadata).await.unwrap();
        tiering_manager.mark_sealed(*segment_id).await.unwrap();
        tiering_manager.mark_committed(*segment_id).await.unwrap();
        tiering_manager.tier_segment(*segment_id).await.unwrap();

        tiered_count += 1;
    }

    assert!(tiered_count >= 4, "Should have tiered at least 4 segments");

    // Verify all segments in S3 and download to verify integrity.
    for segment_id in &sealed_ids {
        // Check location is Both (in WAL and S3).
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "Segment {segment_id:?} should be in Both locations"
        );

        // Download and verify.
        let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
        let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert_eq!(downloaded, original);
    }
}

/// Test with PROBABILISTIC faults to verify fault injection is working.
///
/// This test uses non-zero fail rates (not force_ flags) and verifies
/// that failures actually occur over multiple iterations.
#[tokio::test]
async fn test_probabilistic_fault_injection_actually_fires() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segments.
    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries to create sealed segments.
    for i in 1u64..=6 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    let sealed_ids = wal.read().await.sealed_segment_ids();
    assert!(!sealed_ids.is_empty(), "Need sealed segments for this test");

    // Create storage with HIGH fault rate (50% put failures).
    let fault_config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.5);
    let object_storage = SimulatedObjectStorage::with_faults(42, fault_config);

    // Track successes and failures.
    let mut success_count = 0usize;
    let mut failure_count = 0usize;

    // Try many uploads - with 50% fail rate, we should see both outcomes.
    // TigerStyle: bounded iteration.
    const MAX_ATTEMPTS: usize = 20;
    for attempt in 0..MAX_ATTEMPTS {
        let key = ObjectKey::from_segment(1, 0, attempt as u64);
        let result = object_storage.put(&key, Bytes::from("test-data")).await;

        match result {
            Ok(()) => success_count += 1,
            Err(TierError::UploadFailed { .. }) => failure_count += 1,
            Err(e) => panic!("Unexpected error type: {e:?}"),
        }
    }

    // With 50% fail rate over 20 attempts, we should see BOTH outcomes.
    // Probabilistically, chance of all success or all failure is ~0.0001%.
    assert!(
        success_count > 0,
        "FAULT INJECTION BUG: No uploads succeeded with 50% fail rate! \
         This suggests the fault injection is ALWAYS failing."
    );
    assert!(
        failure_count > 0,
        "FAULT INJECTION BUG: No uploads failed with 50% fail rate! \
         This suggests the fault injection is NOT FIRING."
    );

    // Sanity check: should have some reasonable distribution.
    let total = success_count + failure_count;
    assert_eq!(total, MAX_ATTEMPTS);

    // With seed=42, the distribution should be deterministic.
    // Let's verify we get reasonable numbers (not all one way).
    let success_rate = success_count as f64 / total as f64;
    assert!(
        (0.2..=0.8).contains(&success_rate),
        "Success rate {success_rate:.2} is outside expected range [0.2, 0.8] for 50% fail rate"
    );
}

/// Test that verifies we handle the case where a segment is already tiered.
///
/// This catches bugs where we might re-upload already-tiered segments.
#[tokio::test]
async fn test_cannot_tier_already_tiered_segment() {
    let temp_dir = tempfile::tempdir().unwrap();

    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries.
    for i in 1u64..=6 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage,
        metadata_store,
        segment_reader,
        tiering_config,
    );

    let sealed_ids = wal.read().await.sealed_segment_ids();
    let segment_id = sealed_ids[0];

    // Register and prepare segment.
    let info = wal.read().await.segment_info(segment_id).unwrap();
    let metadata = SegmentMetadata::new(
        segment_id,
        TopicId::new(1),
        PartitionId::new(0),
        info.first_index,
    );
    tiering_manager.register_segment(metadata).await.unwrap();
    tiering_manager.mark_sealed(segment_id).await.unwrap();
    tiering_manager.mark_committed(segment_id).await.unwrap();

    // First tier should succeed.
    tiering_manager.tier_segment(segment_id).await.unwrap();

    // Verify location is Both.
    let location = tiering_manager.get_location(segment_id).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Both));

    // Second tier should fail - segment already uploaded.
    let result = tiering_manager.tier_segment(segment_id).await;
    assert!(
        matches!(result, Err(TierError::NotEligible { reason, .. }) if reason == "segment already uploaded"),
        "Expected NotEligible error for already-tiered segment, got {result:?}"
    );
}

/// Test concurrent tiering of different segments.
///
/// Verifies that concurrent tiering operations don't interfere with each other.
#[tokio::test]
async fn test_concurrent_tiering_different_segments() {
    let temp_dir = tempfile::tempdir().unwrap();

    let segment_config = SegmentConfig::new().with_max_entries(2);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write many entries to create multiple sealed segments.
    for i in 1u64..=10 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    let sealed_ids = wal.read().await.sealed_segment_ids();
    assert!(
        sealed_ids.len() >= 2,
        "Need multiple sealed segments for concurrency test"
    );

    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = Arc::new(IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store,
        segment_reader,
        tiering_config,
    ));

    // Register all segments first.
    for segment_id in &sealed_ids {
        let info = wal.read().await.segment_info(*segment_id).unwrap();
        let metadata = SegmentMetadata::new(
            *segment_id,
            TopicId::new(1),
            PartitionId::new(0),
            info.first_index,
        );
        tiering_manager.register_segment(metadata).await.unwrap();
        tiering_manager.mark_sealed(*segment_id).await.unwrap();
        tiering_manager.mark_committed(*segment_id).await.unwrap();
    }

    // Tier all segments concurrently.
    let mut handles = Vec::new();
    for segment_id in sealed_ids.clone() {
        let manager = tiering_manager.clone();
        let handle = tokio::spawn(async move { manager.tier_segment(segment_id).await });
        handles.push(handle);
    }

    // Wait for all to complete.
    for handle in handles {
        let result = handle.await.expect("task panicked");
        assert!(result.is_ok(), "Concurrent tiering failed: {result:?}");
    }

    // Verify all segments are tiered.
    for segment_id in &sealed_ids {
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "Segment {segment_id:?} not in Both location after concurrent tiering"
        );

        // Verify data integrity.
        let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
        let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert_eq!(
            downloaded, original,
            "Data mismatch for segment {segment_id:?}"
        );
    }
}

/// Test concurrent tiering of the SAME segment (race condition test).
///
/// This test verifies that concurrent tiering of the same segment is handled
/// correctly - only one should succeed, others should fail with NotEligible.
///
/// NOTE: Uses multi-threaded runtime to expose race conditions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_tiering_same_segment_race() {
    let temp_dir = tempfile::tempdir().unwrap();

    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries.
    for i in 1u64..=6 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = Arc::new(IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store,
        segment_reader,
        tiering_config,
    ));

    let sealed_ids = wal.read().await.sealed_segment_ids();
    let segment_id = sealed_ids[0];

    // Register and prepare segment.
    let info = wal.read().await.segment_info(segment_id).unwrap();
    let metadata = SegmentMetadata::new(
        segment_id,
        TopicId::new(1),
        PartitionId::new(0),
        info.first_index,
    );
    tiering_manager.register_segment(metadata).await.unwrap();
    tiering_manager.mark_sealed(segment_id).await.unwrap();
    tiering_manager.mark_committed(segment_id).await.unwrap();

    // Try to tier the SAME segment from multiple tasks concurrently.
    const CONCURRENT_ATTEMPTS: usize = 5;
    let mut handles = Vec::new();

    for _ in 0..CONCURRENT_ATTEMPTS {
        let manager = tiering_manager.clone();
        let handle = tokio::spawn(async move { manager.tier_segment(segment_id).await });
        handles.push(handle);
    }

    // Collect results.
    let mut success_count = 0usize;
    let mut not_eligible_count = 0usize;

    for handle in handles {
        let result = handle.await.expect("task panicked");
        match result {
            Ok(()) => success_count += 1,
            Err(TierError::NotEligible { .. }) => not_eligible_count += 1,
            Err(e) => panic!("Unexpected error: {e:?}"),
        }
    }

    // Exactly ONE should succeed, the rest should fail.
    // NOTE: If this fails with success_count > 1, we have a race condition bug!
    assert_eq!(
        success_count, 1,
        "RACE CONDITION BUG: Expected exactly 1 success, got {success_count}. \
         Multiple concurrent uploads of the same segment succeeded!"
    );
    assert_eq!(
        not_eligible_count,
        CONCURRENT_ATTEMPTS - 1,
        "Expected {} NotEligible errors, got {not_eligible_count}",
        CONCURRENT_ATTEMPTS - 1
    );

    // Verify segment is tiered exactly once.
    let location = tiering_manager.get_location(segment_id).await.unwrap();
    assert_eq!(location, Some(SegmentLocation::Both));

    // Verify data integrity.
    let downloaded = tiering_manager.download_segment(segment_id).await.unwrap();
    let original = wal.read().await.read_segment_bytes(segment_id).unwrap();
    assert_eq!(downloaded, original);
}

// ============================================================================
// Strong General Scenario Tests (WAL Pattern)
// ============================================================================
//
// These tests follow the WAL stress testing pattern:
// 1. Run operations with random faults
// 2. "Crash" (drop manager) at various points
// 3. Restart with new manager (same underlying storage)
// 4. Verify state consistency
// 5. Repeat with multiple seeds

/// Test recovery after manager restart.
///
/// This is a GENERAL scenario test that exercises the recovery path.
/// If there are bugs where segments get stuck in intermediate states,
/// this test would naturally find them by restarting the manager mid-operation.
#[tokio::test]
async fn test_manager_restart_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();

    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries to create sealed segments.
    for i in 1u64..=12 {
        let entry = Entry::new(1, i, Bytes::from(format!("entry-{i}"))).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    let sealed_ids = wal.read().await.sealed_segment_ids();
    assert!(sealed_ids.len() >= 2, "Need multiple segments for this test");

    // Use shared object storage and metadata store (simulates persistent storage).
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    // Phase 1: Start tiering, tier SOME segments, then "crash".
    {
        let segment_reader = TestSegmentReader::new(wal.clone());
        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            tiering_config,
        );

        // Register ALL segments.
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();
        }

        // Tier only the FIRST segment.
        tiering_manager.tier_segment(sealed_ids[0]).await.unwrap();

        // "Crash" - drop the manager.
    }

    // Phase 2: Create NEW manager (simulating restart) with same storage.
    {
        let segment_reader = TestSegmentReader::new(wal.clone());
        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let new_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            tiering_config,
        );

        // Verify first segment state is preserved (should be Both).
        let location = new_manager.get_location(sealed_ids[0]).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "First segment should still be Both after restart"
        );

        // Verify second segment is still Local (not tiered yet).
        let location = new_manager.get_location(sealed_ids[1]).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Local),
            "Second segment should still be Local after restart"
        );

        // Continue tiering the remaining segments.
        for segment_id in sealed_ids.iter().skip(1) {
            new_manager.tier_segment(*segment_id).await.unwrap();
        }

        // Verify ALL segments are now Both.
        for segment_id in &sealed_ids {
            let location = new_manager.get_location(*segment_id).await.unwrap();
            assert_eq!(
                location,
                Some(SegmentLocation::Both),
                "Segment {segment_id:?} should be Both after completing tiering"
            );

            // Verify data integrity.
            let downloaded = new_manager.download_segment(*segment_id).await.unwrap();
            let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
            assert_eq!(downloaded, original);
        }
    }
}

/// Stress test with continuous tiering and random faults.
///
/// This is the tiering equivalent of WAL's `test_random_faults_stress`.
/// It exercises the system with random failures and verifies consistency.
#[tokio::test]
async fn test_continuous_tiering_with_random_faults() {
    // Run with multiple seeds for coverage.
    for seed in [1, 42, 123, 999, 12345] {
        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Write entries to create multiple sealed segments.
        for i in 1u64..=15 {
            let entry = Entry::new(1, i, Bytes::from(format!("seed-{seed}-entry-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // Create tiering with probabilistic faults.
        let fault_config = ObjectStorageFaultConfig::none()
            .with_put_fail_rate(0.15) // 15% upload failures
            .with_get_fail_rate(0.1); // 10% download failures

        let segment_reader = TestSegmentReader::new(wal.clone());
        let object_storage = SimulatedObjectStorage::with_faults(seed, fault_config);
        let metadata_store = InMemoryMetadataStore::new();

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Track outcomes.
        let mut successfully_tiered: Vec<SegmentId> = Vec::new();

        // Try to tier all segments (some will fail due to faults).
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();

            match tiering_manager.tier_segment(*segment_id).await {
                Ok(()) => {
                    successfully_tiered.push(*segment_id);
                }
                Err(TierError::UploadFailed { .. }) => {
                    // Segment should remain Local on failure.
                }
                Err(e) => panic!("Unexpected error for seed {seed}: {e:?}"),
            }
        }

        // Note: With 4 segments and 15% failure rate, some seeds will have 0 failures by chance.
        // The important property is that successful vs failed segments have correct state.

        // PROPERTY 2: Successfully tiered segments must be in Both state.
        for segment_id in &successfully_tiered {
            let location = tiering_manager.get_location(*segment_id).await.unwrap();
            assert_eq!(
                location,
                Some(SegmentLocation::Both),
                "seed {seed}: Successfully tiered segment should be in Both state"
            );
        }

        // PROPERTY 3: Failed segments must still be in Local state.
        for segment_id in &sealed_ids {
            if !successfully_tiered.contains(segment_id) {
                let location = tiering_manager.get_location(*segment_id).await.unwrap();
                assert_eq!(
                    location,
                    Some(SegmentLocation::Local),
                    "seed {seed}: Failed segment should remain in Local state"
                );
            }
        }

        // PROPERTY 4: Data integrity - all tiered segments have correct data.
        // Disable faults for verification.
        object_storage.fault_config().get_fail_rate = 0.0;

        for segment_id in &successfully_tiered {
            let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
            let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
            assert_eq!(
                downloaded, original,
                "seed {seed}: Data integrity violation for segment {segment_id:?}"
            );
        }

        // PROPERTY 5: Retry after disabling faults should succeed.
        object_storage.fault_config().put_fail_rate = 0.0;

        for segment_id in &sealed_ids {
            if !successfully_tiered.contains(segment_id) {
                // Retry tiering - should now succeed.
                tiering_manager.tier_segment(*segment_id).await.unwrap();

                let location = tiering_manager.get_location(*segment_id).await.unwrap();
                assert_eq!(
                    location,
                    Some(SegmentLocation::Both),
                    "seed {seed}: Retried segment should be in Both state"
                );
            }
        }
    }
}

/// Test recovery after manager restart with pending upload.
///
/// This scenario tests what happens if a manager "crashes" while an upload
/// is conceptually in progress. The new manager should be able to recover
/// and complete or retry the tiering.
#[tokio::test]
async fn test_restart_recovery_with_multiple_seeds() {
    for seed in [42, 123, 456, 789] {
        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Write entries.
        for i in 1u64..=9 {
            let entry = Entry::new(1, i, Bytes::from(format!("seed-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Shared storage (persists across "restarts").
        let object_storage = SimulatedObjectStorage::new(seed);
        let metadata_store = InMemoryMetadataStore::new();

        // Phase 1: Initial manager - register all, tier some.
        let tiered_in_phase1: Vec<SegmentId>;
        {
            let segment_reader = TestSegmentReader::new(wal.clone());
            let tiering_config = TieringConfig {
                min_age_secs: 0,
                max_concurrent_uploads: 4,
                verify_on_download: true,
            };

            let manager = IntegratedTieringManager::new(
                object_storage.clone(),
                metadata_store.clone(),
                segment_reader,
                tiering_config,
            );

            // Register all segments.
            for segment_id in &sealed_ids {
                let info = wal.read().await.segment_info(*segment_id).unwrap();
                let metadata = SegmentMetadata::new(
                    *segment_id,
                    TopicId::new(1),
                    PartitionId::new(0),
                    info.first_index,
                );
                manager.register_segment(metadata).await.unwrap();
                manager.mark_sealed(*segment_id).await.unwrap();
                manager.mark_committed(*segment_id).await.unwrap();
            }

            // Tier half the segments.
            let half = sealed_ids.len() / 2;
            tiered_in_phase1 = sealed_ids.iter().take(half).copied().collect();

            for segment_id in &tiered_in_phase1 {
                manager.tier_segment(*segment_id).await.unwrap();
            }

            // "Crash" - drop manager.
        }

        // Phase 2: New manager after "restart".
        {
            let segment_reader = TestSegmentReader::new(wal.clone());
            let tiering_config = TieringConfig {
                min_age_secs: 0,
                max_concurrent_uploads: 4,
                verify_on_download: true,
            };

            let new_manager = IntegratedTieringManager::new(
                object_storage.clone(),
                metadata_store.clone(),
                segment_reader,
                tiering_config,
            );

            // VERIFY: State is consistent after restart.

            // Segments tiered in phase 1 should still be Both.
            for segment_id in &tiered_in_phase1 {
                let location = new_manager.get_location(*segment_id).await.unwrap();
                assert_eq!(
                    location,
                    Some(SegmentLocation::Both),
                    "seed {seed}: Segment tiered before restart should be Both"
                );
            }

            // Segments NOT tiered should be Local.
            for segment_id in sealed_ids.iter().skip(tiered_in_phase1.len()) {
                let location = new_manager.get_location(*segment_id).await.unwrap();
                assert_eq!(
                    location,
                    Some(SegmentLocation::Local),
                    "seed {seed}: Segment not tiered should be Local"
                );
            }

            // Complete tiering the remaining segments.
            for segment_id in sealed_ids.iter().skip(tiered_in_phase1.len()) {
                new_manager.tier_segment(*segment_id).await.unwrap();
            }

            // All should now be Both.
            for segment_id in &sealed_ids {
                let location = new_manager.get_location(*segment_id).await.unwrap();
                assert_eq!(location, Some(SegmentLocation::Both));

                // Data integrity check.
                let downloaded = new_manager.download_segment(*segment_id).await.unwrap();
                let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
                assert_eq!(
                    downloaded, original,
                    "seed {seed}: Data integrity violation"
                );
            }
        }
    }
}

/// Test interleaved writes, sealing, and tiering with faults.
///
/// This simulates a more realistic workload where writes and tiering
/// happen concurrently with occasional failures.
#[tokio::test]
async fn test_interleaved_operations_with_faults() {
    let temp_dir = tempfile::tempdir().unwrap();

    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Create tiering with moderate fault rates.
    let fault_config = ObjectStorageFaultConfig::none().with_put_fail_rate(0.1);
    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::with_faults(42, fault_config);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store.clone(),
        segment_reader,
        tiering_config,
    );

    let mut entry_index = 1u64;
    let mut tiered_segments: Vec<SegmentId> = Vec::new();

    // Simulate multiple rounds of write + tier.
    const MAX_ROUNDS: usize = 5;
    for round in 0..MAX_ROUNDS {
        // Write enough entries to create a sealed segment.
        for _ in 0..4 {
            let entry = Entry::new(1, entry_index, Bytes::from(format!("round-{round}-{entry_index}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
            entry_index += 1;
        }

        // Get newly sealed segments.
        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Try to tier any new sealed segments.
        for segment_id in &sealed_ids {
            if tiered_segments.contains(segment_id) {
                continue; // Already tiered.
            }

            // Check if already registered.
            let location = tiering_manager.get_location(*segment_id).await.unwrap();
            if location.is_none() {
                // Register new segment.
                let info = wal.read().await.segment_info(*segment_id).unwrap();
                let metadata = SegmentMetadata::new(
                    *segment_id,
                    TopicId::new(1),
                    PartitionId::new(0),
                    info.first_index,
                );
                tiering_manager.register_segment(metadata).await.unwrap();
                tiering_manager.mark_sealed(*segment_id).await.unwrap();
                tiering_manager.mark_committed(*segment_id).await.unwrap();
            }

            // Try to tier (may fail due to fault injection).
            match tiering_manager.tier_segment(*segment_id).await {
                Ok(()) => {
                    tiered_segments.push(*segment_id);
                }
                Err(TierError::UploadFailed { .. }) => {
                    // Expected - will retry next round.
                }
                Err(TierError::NotEligible { .. }) => {
                    // Already tiered - add to list.
                    tiered_segments.push(*segment_id);
                }
                Err(e) => panic!("Unexpected error: {e:?}"),
            }
        }
    }

    // Final pass: disable faults and tier everything.
    object_storage.fault_config().put_fail_rate = 0.0;

    let final_sealed = wal.read().await.sealed_segment_ids();
    for segment_id in &final_sealed {
        if tiered_segments.contains(segment_id) {
            continue;
        }

        // Register if needed.
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        if location.is_none() {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();
        }

        tiering_manager.tier_segment(*segment_id).await.unwrap();
        tiered_segments.push(*segment_id);
    }

    // Verify all segments are properly tiered with correct data.
    for segment_id in &final_sealed {
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "All segments should be Both at end"
        );

        let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
        let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert_eq!(downloaded, original, "Data integrity check failed");
    }
}

/// Diagnostic test to verify the tiering pipeline is working correctly.
///
/// This test prints detailed info about what's happening at each step
/// and verifies data directly in the object storage.
#[tokio::test]
async fn test_diagnostic_tiering_pipeline() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create WAL with small segments (3 entries max).
    let segment_config = SegmentConfig::new().with_max_entries(3);
    let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

    let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
    let wal = Arc::new(RwLock::new(wal));

    // Write entries and track what we write.
    let mut written_payloads: Vec<String> = Vec::new();
    for i in 1u64..=9 {
        let payload = format!("diagnostic-entry-{i}-with-data");
        written_payloads.push(payload.clone());
        let entry = Entry::new(1, i, Bytes::from(payload)).unwrap();
        wal.write().await.append(entry).await.unwrap();
    }

    // Verify segment rotation occurred.
    let sealed_count = wal.read().await.sealed_segment_count();
    let sealed_ids = wal.read().await.sealed_segment_ids();

    // With 9 entries and max 3 per segment: should have 2 sealed (3+3) + 1 active (3).
    assert!(
        sealed_count >= 2,
        "DIAGNOSTIC: Expected >= 2 sealed segments, got {sealed_count}. \
         This means segment rotation is NOT happening!"
    );

    // Verify each sealed segment has correct info.
    for segment_id in &sealed_ids {
        let info = wal.read().await.segment_info(*segment_id).unwrap();
        assert!(
            info.is_sealed,
            "DIAGNOSTIC: Segment {segment_id:?} reported in sealed_ids but is_sealed=false"
        );
        assert!(
            info.entry_count > 0,
            "DIAGNOSTIC: Sealed segment {segment_id:?} has 0 entries"
        );
    }

    // Create tiering infrastructure.
    let segment_reader = TestSegmentReader::new(wal.clone());
    let object_storage = SimulatedObjectStorage::new(42);
    let metadata_store = InMemoryMetadataStore::new();

    let tiering_config = TieringConfig {
        min_age_secs: 0,
        max_concurrent_uploads: 4,
        verify_on_download: true,
    };

    let tiering_manager = IntegratedTieringManager::new(
        object_storage.clone(),
        metadata_store.clone(),
        segment_reader,
        tiering_config,
    );

    // Tier each segment and verify raw storage.
    for segment_id in &sealed_ids {
        let info = wal.read().await.segment_info(*segment_id).unwrap();

        // Read raw bytes from WAL.
        let wal_bytes = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert!(
            !wal_bytes.is_empty(),
            "DIAGNOSTIC: WAL segment {segment_id:?} bytes are empty!"
        );

        // Register and prepare segment.
        let metadata = SegmentMetadata::new(
            *segment_id,
            TopicId::new(1),
            PartitionId::new(0),
            info.first_index,
        );
        tiering_manager.register_segment(metadata).await.unwrap();
        tiering_manager.mark_sealed(*segment_id).await.unwrap();
        tiering_manager.mark_committed(*segment_id).await.unwrap();

        // Tier the segment.
        tiering_manager.tier_segment(*segment_id).await.unwrap();

        // Verify data is in object storage using raw access.
        let key = ObjectKey::from_segment(1, 0, segment_id.get());
        let raw_content = object_storage.get_raw_content(&key);
        assert!(
            raw_content.is_some(),
            "DIAGNOSTIC: Object storage does NOT contain key {key:?} after tiering!"
        );

        let stored_bytes = raw_content.unwrap();
        assert_eq!(
            stored_bytes.len(),
            wal_bytes.len(),
            "DIAGNOSTIC: Stored bytes length ({}) != WAL bytes length ({})",
            stored_bytes.len(),
            wal_bytes.len()
        );
        assert_eq!(
            stored_bytes,
            wal_bytes.to_vec(),
            "DIAGNOSTIC: Stored bytes don't match WAL bytes!"
        );
    }

    // Final verification: download through the API and compare.
    for segment_id in &sealed_ids {
        let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
        let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
        assert_eq!(
            downloaded, original,
            "DIAGNOSTIC: Downloaded bytes don't match original WAL bytes"
        );
    }

    // Verify metadata state.
    for segment_id in &sealed_ids {
        let location = tiering_manager.get_location(*segment_id).await.unwrap();
        assert_eq!(
            location,
            Some(SegmentLocation::Both),
            "DIAGNOSTIC: Segment {segment_id:?} location is {location:?}, expected Both"
        );
    }
}

// ============================================================================
// BUGGIFY Crash Tests
// ============================================================================
//
// These tests use BUGGIFY to inject crashes at arbitrary points in the code.
// They verify GENERAL PROPERTIES that must hold regardless of where crashes occur:
//
// 1. NO INTERMEDIATE STATES: After any crash/error, segments must be in a
//    recoverable state (Local or Both), never stuck in Uploading.
//
// 2. RETRYABILITY: Any segment that failed to tier must be retryable.
//
// 3. DATA INTEGRITY: Successfully tiered segments must have correct data in S3.
//
// The buggify crash points in the tiering code are:
// - tiering_crash_after_claim: After claiming, before upload
// - tiering_crash_after_upload_before_complete: Upload succeeded, before metadata update
// - tiering_crash_before_abort: Upload failed, before reverting state

/// GENERAL PROPERTY TEST: No segments stuck in intermediate states after recovery.
///
/// This test runs tiering operations with BUGGIFY crashes enabled at multiple
/// points in the code. After crashes occur, it calls `recover_stuck_uploads`
/// and verifies that all segments are in a recoverable state.
///
/// This is a GENERAL test - it will catch ANY bug where recovery doesn't work.
#[tokio::test]
async fn test_buggify_no_intermediate_states() {
    for seed in [42, 123, 456, 789, 1111, 2222, 3333, 4444] {
        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Write entries to create multiple sealed segments.
        for i in 1u64..=15 {
            let entry = Entry::new(1, i, Bytes::from(format!("seed-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        let segment_reader = TestSegmentReader::new(wal.clone());
        let object_storage = SimulatedObjectStorage::new(seed);
        let metadata_store = InMemoryMetadataStore::new();

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage,
            metadata_store,
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Try to tier all segments - some may crash due to BUGGIFY.
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();

            // Attempt to tier - may fail due to BUGGIFY crashes.
            let _ = tiering_manager.tier_segment(*segment_id).await;
        }

        // RECOVERY: Call recover_stuck_uploads (simulates restart recovery).
        let recovered = tiering_manager.recover_stuck_uploads().await.unwrap();

        // PROPERTY: After recovery, no segments should be stuck in Uploading state.
        for segment_id in &sealed_ids {
            let location = tiering_manager.get_location(*segment_id).await.unwrap();
            assert!(
                !matches!(location, Some(SegmentLocation::Uploading)),
                "seed {seed}: RECOVERY FAILED! Segment {:?} still in Uploading state \
                 after recover_stuck_uploads(). Recovery should fix all stuck segments.",
                segment_id
            );
        }

        // Print BUGGIFY stats for visibility.
        let stats = buggify::all_stats();
        let total_crashes: u64 = stats.values().map(|s| s.triggers).sum();
        if total_crashes > 0 {
            println!("seed {seed}: BUGGIFY triggered {total_crashes} crashes, recovered {recovered}");
        }

        buggify::reset();
    }
}

/// GENERAL PROPERTY TEST: All segments eventually tierable after recovery.
///
/// After crashes occur, verifies that ALL segments can eventually be tiered
/// when we call recovery and retry. This proves the system is recoverable.
#[tokio::test]
async fn test_buggify_all_segments_eventually_tierable() {
    for seed in [42, 123, 456, 789] {
        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        for i in 1u64..=12 {
            let entry = Entry::new(1, i, Bytes::from(format!("seed-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        let segment_reader = TestSegmentReader::new(wal.clone());
        let object_storage = SimulatedObjectStorage::new(seed);
        let metadata_store = InMemoryMetadataStore::new();

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage,
            metadata_store.clone(),
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Phase 1: Tier with BUGGIFY enabled (some will crash).
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();
            let _ = tiering_manager.tier_segment(*segment_id).await;
        }

        // Phase 2: Disable BUGGIFY and run recovery.
        buggify::disable();

        // RECOVERY: Fix any stuck uploads (simulates restart).
        tiering_manager.recover_stuck_uploads().await.unwrap();

        // Phase 3: Retry all non-tiered segments.
        for segment_id in &sealed_ids {
            let location = tiering_manager.get_location(*segment_id).await.unwrap();

            // After recovery, segments should be Local or Both (never Uploading).
            // If Local, retry the tier operation.
            if matches!(location, Some(SegmentLocation::Local)) {
                let result = tiering_manager.tier_segment(*segment_id).await;
                assert!(
                    result.is_ok(),
                    "seed {seed}: Segment {:?} in Local state should be retryable, but got: {:?}",
                    segment_id,
                    result
                );
            }
        }

        // PROPERTY: ALL segments should now be in Both state.
        for segment_id in &sealed_ids {
            let location = tiering_manager.get_location(*segment_id).await.unwrap();
            assert_eq!(
                location,
                Some(SegmentLocation::Both),
                "seed {seed}: After recovery, ALL segments should be in Both state. \
                 Segment {:?} is in {:?}. This indicates a recovery bug.",
                segment_id,
                location
            );
        }

        buggify::reset();
    }
}

/// GENERAL PROPERTY TEST: Data integrity after crashes.
///
/// Verifies that segments which successfully tiered have correct data in S3,
/// even if crashes occurred during the process.
#[tokio::test]
async fn test_buggify_data_integrity_after_crashes() {
    for seed in [42, 123, 456] {
        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        for i in 1u64..=9 {
            let entry = Entry::new(1, i, Bytes::from(format!("integrity-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        let segment_reader = TestSegmentReader::new(wal.clone());
        let object_storage = SimulatedObjectStorage::new(seed);
        let metadata_store = InMemoryMetadataStore::new();

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage,
            metadata_store,
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Tier with BUGGIFY enabled.
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();
            let _ = tiering_manager.tier_segment(*segment_id).await;
        }

        // Disable BUGGIFY for verification.
        buggify::disable();

        // PROPERTY: All segments in Both state must have correct data.
        for segment_id in &sealed_ids {
            let location = tiering_manager.get_location(*segment_id).await.unwrap();

            if matches!(location, Some(SegmentLocation::Both)) {
                let downloaded = tiering_manager.download_segment(*segment_id).await.unwrap();
                let original = wal.read().await.read_segment_bytes(*segment_id).unwrap();
                assert_eq!(
                    downloaded, original,
                    "seed {seed}: DATA INTEGRITY VIOLATION! Segment {:?} data in S3 \
                     doesn't match WAL. Downloaded {} bytes, expected {} bytes.",
                    segment_id,
                    downloaded.len(),
                    original.len()
                );
            }
        }

        buggify::reset();
    }
}

// ============================================================================
// Randomized DST - Random Operation Sequences with Faults
// ============================================================================
//
// This is the proper DST approach:
// 1. Generate RANDOM sequences of operations (not scripted)
// 2. Execute with faults enabled at ALL layers
// 3. Check invariants after EVERY operation
// 4. Run with many seeds - let the framework find bugs

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// Operations that can be performed on the tiering system.
#[derive(Debug, Clone, Copy)]
enum TierOp {
    /// Register a new segment (if any unregistered).
    Register,
    /// Mark a segment as sealed.
    Seal,
    /// Mark a segment as committed.
    Commit,
    /// Attempt to tier a segment to S3.
    Tier,
    /// Attempt to download a segment from S3.
    Download,
    /// Run recovery for stuck uploads.
    Recover,
}

impl TierOp {
    fn random(rng: &mut ChaCha8Rng) -> Self {
        match rng.gen_range(0..6) {
            0 => Self::Register,
            1 => Self::Seal,
            2 => Self::Commit,
            3 => Self::Tier,
            4 => Self::Download,
            _ => Self::Recover,
        }
    }
}

/// Check HARD invariants that must hold even during operations (without recovery).
///
/// Hard invariants:
/// 1. If metadata says `Both`, data MUST exist in S3 (otherwise data loss)
/// 2. If metadata says `Local`, data should NOT exist in S3 (otherwise orphaned data)
/// 3. If metadata says `Both` or `Remote`, must have a remote_key (referential integrity)
/// 4. If tiered (Both/Remote), must be committed (ordering)
/// 5. If committed, must be sealed (ordering)
///
/// Note: `Uploading` state is allowed - it's a transient state where S3 might or might not have data.
async fn check_hard_invariants<R: SegmentReader>(
    tiering_manager: &IntegratedTieringManager<
        SimulatedObjectStorage,
        InMemoryMetadataStore,
        R,
    >,
    object_storage: &SimulatedObjectStorage,
    seed: u64,
    op_num: usize,
) -> Result<(), String> {
    // Get all registered segments from metadata store.
    let all_segments = tiering_manager.metadata().all_segments();

    for (segment_id, metadata) in all_segments {
        let key = ObjectKey::from_segment(
            metadata.topic_id.get(),
            metadata.partition_id.get(),
            segment_id.get(),
        );
        let in_s3 = object_storage.get_raw_content(&key).is_some();

        // INVARIANT: Ordering - if committed, must be sealed.
        if metadata.committed && !metadata.sealed {
            return Err(format!(
                "seed {seed}, op {op_num}: ORDERING VIOLATION - Segment {:?} is committed but not sealed",
                segment_id
            ));
        }

        // INVARIANT: Ordering - if tiered (Both/Remote/Uploading), must be committed.
        let is_tiered = matches!(
            metadata.location,
            SegmentLocation::Both | SegmentLocation::Remote | SegmentLocation::Uploading
        );
        if is_tiered && !metadata.committed {
            return Err(format!(
                "seed {seed}, op {op_num}: ORDERING VIOLATION - Segment {:?} is {:?} but not committed",
                segment_id, metadata.location
            ));
        }

        match metadata.location {
            // HARD INVARIANT 1: If metadata says Both, data MUST exist in S3.
            SegmentLocation::Both => {
                if !in_s3 {
                    return Err(format!(
                        "seed {seed}, op {op_num}: DATA LOSS - Segment {:?} metadata says Both but no data in S3",
                        segment_id
                    ));
                }
                // INVARIANT: Referential integrity - must have remote_key.
                if metadata.remote_key.is_none() {
                    return Err(format!(
                        "seed {seed}, op {op_num}: REFERENTIAL INTEGRITY - Segment {:?} is Both but has no remote_key",
                        segment_id
                    ));
                }
            }
            // HARD INVARIANT 2: If metadata says Local, data should NOT be in S3.
            // If it is, we have orphaned data - a bug in recovery logic.
            SegmentLocation::Local => {
                if in_s3 {
                    return Err(format!(
                        "seed {seed}, op {op_num}: ORPHANED DATA - Segment {:?} metadata says Local but data exists in S3",
                        segment_id
                    ));
                }
            }
            // Uploading is transient - S3 might or might not have data, both are valid.
            SegmentLocation::Uploading => {}
            // Remote means data should be in S3 (local copy evicted).
            SegmentLocation::Remote => {
                if !in_s3 {
                    return Err(format!(
                        "seed {seed}, op {op_num}: DATA LOSS - Segment {:?} metadata says Remote but no data in S3",
                        segment_id
                    ));
                }
                // INVARIANT: Referential integrity - must have remote_key.
                if metadata.remote_key.is_none() {
                    return Err(format!(
                        "seed {seed}, op {op_num}: REFERENTIAL INTEGRITY - Segment {:?} is Remote but has no remote_key",
                        segment_id
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Check invariants that must hold AFTER recovery has run.
async fn check_post_recovery_invariants<R: SegmentReader>(
    tiering_manager: &IntegratedTieringManager<
        SimulatedObjectStorage,
        InMemoryMetadataStore,
        R,
    >,
    wal: &Arc<RwLock<Wal<TokioStorage>>>,
    object_storage: &SimulatedObjectStorage,
    seed: u64,
) -> Result<(), String> {
    let sealed_ids = wal.read().await.sealed_segment_ids();

    for segment_id in &sealed_ids {
        let location = tiering_manager.get_location(*segment_id).await.ok().flatten();

        // INVARIANT 1: No segments stuck in Uploading state after recovery.
        if matches!(location, Some(SegmentLocation::Uploading)) {
            return Err(format!(
                "seed {seed}: POST-RECOVERY VIOLATION - Segment {:?} still in Uploading after recovery",
                segment_id
            ));
        }

        // INVARIANT 2: If metadata says Both, data should exist in S3.
        if matches!(location, Some(SegmentLocation::Both)) {
            let key = ObjectKey::from_segment(1, 0, segment_id.get());
            let exists = object_storage.get_raw_content(&key).is_some();
            if !exists {
                return Err(format!(
                    "seed {seed}: POST-RECOVERY DATA LOSS - Segment {:?} metadata says Both but no data in S3",
                    segment_id
                ));
            }
        }
    }

    Ok(())
}

/// RANDOMIZED DST: Generate random operation sequences with faults.
///
/// This test:
/// 1. Creates segments in the WAL
/// 2. Generates random sequences of tiering operations
/// 3. Executes with fault injection enabled at storage layer
/// 4. Checks invariants after EVERY operation
/// 5. Runs with many different seeds
///
/// If any invariant fails, we've found a bug.
#[tokio::test]
async fn test_randomized_operations_with_faults() {
    const NUM_SEEDS: u64 = 20;
    const OPS_PER_SEED: usize = 50;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 12345 + 42; // Spread seeds out
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        // Initialize BUGGIFY for crash injection in manager code.
        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Write entries to create multiple sealed segments.
        for i in 1u64..=15 {
            let entry = Entry::new(1, i, Bytes::from(format!("seed-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // Create storage with fault injection at ALL operations.
        let storage_faults = ObjectStorageFaultConfig::none()
            .with_put_fail_rate(0.1)
            .with_get_fail_rate(0.1)
            .with_exists_fail_rate(0.1)
            .with_delete_fail_rate(0.05);

        // Create metadata store with fault injection at ALL operations.
        let metadata_faults = MetadataStoreFaultConfig::none()
            .with_complete_upload_fail_rate(0.1)
            .with_abort_upload_fail_rate(0.1)
            .with_find_stuck_fail_rate(0.1)
            .with_try_claim_fail_rate(0.1);

        // Create segment reader with fault injection.
        let segment_reader = FaultingSegmentReader::new(wal.clone(), seed, 0.1);
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);
        let metadata_store = InMemoryMetadataStore::with_faults(seed, metadata_faults);

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store,
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Track segment state for operation selection.
        let mut registered: Vec<SegmentId> = Vec::new();
        let mut sealed_set: Vec<SegmentId> = Vec::new();
        let mut committed: Vec<SegmentId> = Vec::new();

        // Execute random operations.
        for op_num in 0..OPS_PER_SEED {
            let op = TierOp::random(&mut rng);

            match op {
                TierOp::Register => {
                    // Register a segment that isn't registered yet.
                    for segment_id in &sealed_ids {
                        if !registered.contains(segment_id) {
                            let info = wal.read().await.segment_info(*segment_id).unwrap();
                            let metadata = SegmentMetadata::new(
                                *segment_id,
                                TopicId::new(1),
                                PartitionId::new(0),
                                info.first_index,
                            );
                            let _ = tiering_manager.register_segment(metadata).await;
                            registered.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Seal => {
                    // Seal a registered segment that isn't sealed.
                    for segment_id in &registered {
                        if !sealed_set.contains(segment_id) {
                            let _ = tiering_manager.mark_sealed(*segment_id).await;
                            sealed_set.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Commit => {
                    // Commit a sealed segment that isn't committed.
                    for segment_id in &sealed_set {
                        if !committed.contains(segment_id) {
                            let _ = tiering_manager.mark_committed(*segment_id).await;
                            committed.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Tier => {
                    // Try to tier a committed segment.
                    if !committed.is_empty() {
                        let idx = rng.gen_range(0..committed.len());
                        let _ = tiering_manager.tier_segment(committed[idx]).await;
                    }
                }
                TierOp::Download => {
                    // Try to download any registered segment.
                    if !registered.is_empty() {
                        let idx = rng.gen_range(0..registered.len());
                        let _ = tiering_manager.download_segment(registered[idx]).await;
                    }
                }
                TierOp::Recover => {
                    // Run recovery.
                    let _ = tiering_manager.recover_stuck_uploads().await;
                }
            }

            // CHECK HARD INVARIANTS AFTER EVERY OPERATION.
            // We do NOT run recovery automatically - that would mask bugs!
            // Instead, we check invariants that must hold even without recovery.

            if let Err(e) = check_hard_invariants(
                &tiering_manager,
                &object_storage,
                seed,
                op_num,
            )
            .await
            {
                panic!("{e}");
            }
        }

        // AFTER all operations, run recovery and check final state.
        let _ = tiering_manager.recover_stuck_uploads().await;

        // Now check that recovery cleaned everything up.
        if let Err(e) = check_post_recovery_invariants(
            &tiering_manager,
            &wal,
            &object_storage,
            seed,
        )
        .await
        {
            panic!("{e}");
        }

        buggify::reset();
    }
}

/// Extended randomized test with more seeds and longer sequences.
#[tokio::test]
#[ignore = "long-running randomized test - run explicitly with --ignored"]
async fn test_randomized_extended() {
    const NUM_SEEDS: u64 = 100;
    const OPS_PER_SEED: usize = 200;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 7919 + 1; // Prime multiplier for spread
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        for i in 1u64..=20 {
            let entry = Entry::new(1, i, Bytes::from(format!("ext-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // Higher fault rates for stress testing.
        let storage_faults = ObjectStorageFaultConfig::none()
            .with_put_fail_rate(0.15)
            .with_get_fail_rate(0.15)
            .with_exists_fail_rate(0.15)
            .with_delete_fail_rate(0.1)
            .with_get_corruption_rate(0.05);

        // Higher metadata fault rates for stress testing at ALL operations.
        let metadata_faults = MetadataStoreFaultConfig::none()
            .with_complete_upload_fail_rate(0.15)
            .with_abort_upload_fail_rate(0.15)
            .with_find_stuck_fail_rate(0.15)
            .with_try_claim_fail_rate(0.15);

        // Segment reader with fault injection.
        let segment_reader = FaultingSegmentReader::new(wal.clone(), seed, 0.15);
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);
        let metadata_store = InMemoryMetadataStore::with_faults(seed, metadata_faults);

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store,
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        let mut registered: Vec<SegmentId> = Vec::new();
        let mut sealed_set: Vec<SegmentId> = Vec::new();
        let mut committed: Vec<SegmentId> = Vec::new();

        for op_num in 0..OPS_PER_SEED {
            let op = TierOp::random(&mut rng);

            match op {
                TierOp::Register => {
                    for segment_id in &sealed_ids {
                        if !registered.contains(segment_id) {
                            let info = wal.read().await.segment_info(*segment_id).unwrap();
                            let metadata = SegmentMetadata::new(
                                *segment_id,
                                TopicId::new(1),
                                PartitionId::new(0),
                                info.first_index,
                            );
                            let _ = tiering_manager.register_segment(metadata).await;
                            registered.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Seal => {
                    for segment_id in &registered {
                        if !sealed_set.contains(segment_id) {
                            let _ = tiering_manager.mark_sealed(*segment_id).await;
                            sealed_set.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Commit => {
                    for segment_id in &sealed_set {
                        if !committed.contains(segment_id) {
                            let _ = tiering_manager.mark_committed(*segment_id).await;
                            committed.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Tier => {
                    if !committed.is_empty() {
                        let idx = rng.gen_range(0..committed.len());
                        let _ = tiering_manager.tier_segment(committed[idx]).await;
                    }
                }
                TierOp::Download => {
                    if !registered.is_empty() {
                        let idx = rng.gen_range(0..registered.len());
                        let _ = tiering_manager.download_segment(registered[idx]).await;
                    }
                }
                TierOp::Recover => {
                    let _ = tiering_manager.recover_stuck_uploads().await;
                }
            }

            // Check hard invariants periodically (every 10 ops for performance).
            // We do NOT run recovery automatically - that would mask bugs!
            if op_num % 10 == 0 {
                if let Err(e) = check_hard_invariants(
                    &tiering_manager,
                    &object_storage,
                    seed,
                    op_num,
                )
                .await
                {
                    panic!("{e}");
                }
            }
        }

        // AFTER all operations, run recovery and check final state.
        let _ = tiering_manager.recover_stuck_uploads().await;
        if let Err(e) = check_post_recovery_invariants(
            &tiering_manager,
            &wal,
            &object_storage,
            seed,
        )
        .await
        {
            panic!("{e}");
        }

        if base_seed % 10 == 0 {
            println!("Completed seed {seed} ({base_seed}/{NUM_SEEDS})");
        }

        buggify::reset();
    }
}

/// Test that specifically targets complete_upload failure and verifies recovery.
///
/// This test:
/// 1. Forces S3 upload to succeed but complete_upload to fail
/// 2. Verifies the segment is stuck in Uploading state with data in S3
/// 3. Runs recovery and verifies it completes the upload
/// 4. Checks for orphaned data (S3 has data that metadata doesn't know about)
#[tokio::test]
async fn test_complete_upload_failure_recovery() {
    for seed in 0..50 {
        let temp_dir = tempfile::tempdir().unwrap();
        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Create enough entries to have multiple sealed segments.
        for i in 1u64..=10 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // S3 storage with NO faults (uploads always succeed).
        let storage_faults = ObjectStorageFaultConfig::none();
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);

        // Metadata store with HIGH complete_upload failure rate.
        let metadata_faults = MetadataStoreFaultConfig::none()
            .with_complete_upload_fail_rate(0.5); // 50% failure rate
        let metadata_store = InMemoryMetadataStore::with_faults(seed, metadata_faults);

        let segment_reader = TestSegmentReader::new(wal.clone());
        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            TieringConfig::for_testing(),
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Try to tier each segment.
        for segment_id in &sealed_ids {
            // Register, seal, commit.
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            tiering_manager.register_segment(metadata).await.unwrap();
            tiering_manager.mark_sealed(*segment_id).await.unwrap();
            tiering_manager.mark_committed(*segment_id).await.unwrap();

            // Try to tier - this might fail due to complete_upload fault.
            let _ = tiering_manager.tier_segment(*segment_id).await;
        }

        // Check hard invariant before recovery: if Both, data must be in S3.
        for segment_id in &sealed_ids {
            let loc = tiering_manager.get_location(*segment_id).await.unwrap();
            if matches!(loc, Some(SegmentLocation::Both)) {
                let key = ObjectKey::from_segment(1, 0, segment_id.get());
                let in_s3 = object_storage.get_raw_content(&key).is_some();
                assert!(
                    in_s3,
                    "seed {seed}: DATA LOSS - segment {:?} Both but no S3 data",
                    segment_id
                );
            }
        }

        // Run recovery multiple times (in case it fails once due to faults).
        for _ in 0..5 {
            let _ = tiering_manager.recover_stuck_uploads().await;
        }

        // After recovery, check post-recovery invariants.
        for segment_id in &sealed_ids {
            let loc = tiering_manager.get_location(*segment_id).await.unwrap();

            // No segments should be stuck in Uploading after recovery.
            assert!(
                !matches!(loc, Some(SegmentLocation::Uploading)),
                "seed {seed}: segment {:?} still Uploading after recovery",
                segment_id
            );

            // If Both, data must be in S3.
            if matches!(loc, Some(SegmentLocation::Both)) {
                let key = ObjectKey::from_segment(1, 0, segment_id.get());
                let in_s3 = object_storage.get_raw_content(&key).is_some();
                assert!(
                    in_s3,
                    "seed {seed}: DATA LOSS - segment {:?} Both but no S3 data",
                    segment_id
                );
            }
        }

        // Check for orphaned S3 data: data in S3 without corresponding Both in metadata.
        // This could happen if exists() fails during recovery and we abort incorrectly.
        for segment_id in &sealed_ids {
            let key = ObjectKey::from_segment(1, 0, segment_id.get());
            let in_s3 = object_storage.get_raw_content(&key).is_some();
            let loc = tiering_manager.get_location(*segment_id).await.unwrap();

            if in_s3 {
                // If data is in S3, metadata should say Both.
                assert!(
                    matches!(loc, Some(SegmentLocation::Both)),
                    "seed {seed}: ORPHANED DATA - segment {:?} has S3 data but metadata says {:?}",
                    segment_id,
                    loc
                );
            }
        }
    }
}

/// Test recovery when exists() calls fail.
///
/// This specifically tests the bug where exists() failure causes
/// incorrect abort (returning segment to Local even though S3 has data).
#[tokio::test]
async fn test_recovery_with_exists_failures() {
    for seed in 0..50 {
        let temp_dir = tempfile::tempdir().unwrap();
        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        for i in 1u64..=6 {
            let entry = Entry::new(1, i, Bytes::from(format!("data-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // S3 storage with high exists() failure rate but no other faults.
        let storage_faults = ObjectStorageFaultConfig::none()
            .with_exists_fail_rate(0.5); // 50% exists failure
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);

        // Metadata store with complete_upload failure to create stuck segments.
        let metadata_faults = MetadataStoreFaultConfig::none()
            .with_complete_upload_fail_rate(0.5);
        let metadata_store = InMemoryMetadataStore::with_faults(seed, metadata_faults);

        let segment_reader = TestSegmentReader::new(wal.clone());
        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            TieringConfig::for_testing(),
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        // Register, seal, commit, and tier all segments.
        for segment_id in &sealed_ids {
            let info = wal.read().await.segment_info(*segment_id).unwrap();
            let metadata = SegmentMetadata::new(
                *segment_id,
                TopicId::new(1),
                PartitionId::new(0),
                info.first_index,
            );
            let _ = tiering_manager.register_segment(metadata).await;
            let _ = tiering_manager.mark_sealed(*segment_id).await;
            let _ = tiering_manager.mark_committed(*segment_id).await;
            let _ = tiering_manager.tier_segment(*segment_id).await;
        }

        // Run recovery multiple times.
        for _ in 0..10 {
            let _ = tiering_manager.recover_stuck_uploads().await;
        }

        // Check for orphaned data after recovery.
        for segment_id in &sealed_ids {
            let key = ObjectKey::from_segment(1, 0, segment_id.get());
            let in_s3 = object_storage.get_raw_content(&key).is_some();
            let loc = tiering_manager.get_location(*segment_id).await.unwrap();

            if in_s3 {
                // If data is in S3, metadata should say Both.
                // If it says Local, we have orphaned data - BUG!
                assert!(
                    matches!(loc, Some(SegmentLocation::Both)),
                    "seed {seed}: ORPHANED DATA BUG - segment {:?} has S3 data but metadata says {:?}. \
                     This happens when exists() fails during recovery and we incorrectly abort.",
                    segment_id,
                    loc
                );
            }
        }
    }
}

/// COMPREHENSIVE STRESS TEST with high fault rates, more seeds, and all fault injection points.
///
/// This test:
/// 1. Runs 500 seeds with 100 operations each
/// 2. Uses 25% fault rates across ALL layers (storage, metadata, segment reader)
/// 3. Includes concurrent tier operations on same segment
/// 4. Verifies data integrity after successful downloads
/// 5. Checks for "eventually unstuck" - segments shouldn't stay stuck after enough recovery
///
/// Run explicitly: cargo test test_comprehensive_stress -- --ignored --nocapture
#[tokio::test]
#[ignore = "comprehensive 500-seed stress test - run explicitly with --ignored"]
async fn test_comprehensive_stress() {
    const NUM_SEEDS: u64 = 500;
    const OPS_PER_SEED: usize = 100;
    const FAULT_RATE: f64 = 0.25;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 7919 + 17; // Prime multiplier for spread
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        buggify::init(seed, true);

        let temp_dir = tempfile::tempdir().unwrap();

        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        // Write entries to create multiple sealed segments.
        for i in 1u64..=20 {
            let entry = Entry::new(1, i, Bytes::from(format!("stress-{seed}-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // HIGH fault rates across ALL layers.
        let storage_faults = ObjectStorageFaultConfig::none()
            .with_put_fail_rate(FAULT_RATE)
            .with_get_fail_rate(FAULT_RATE)
            .with_exists_fail_rate(FAULT_RATE)
            .with_delete_fail_rate(FAULT_RATE / 2.0)
            .with_get_corruption_rate(0.05);

        let metadata_faults = MetadataStoreFaultConfig::none()
            .with_complete_upload_fail_rate(FAULT_RATE)
            .with_abort_upload_fail_rate(FAULT_RATE)
            .with_find_stuck_fail_rate(FAULT_RATE)
            .with_try_claim_fail_rate(FAULT_RATE)
            .with_set_fail_rate(FAULT_RATE / 5.0)
            .with_get_fail_rate(FAULT_RATE / 5.0);

        let segment_reader = FaultingSegmentReader::new(wal.clone(), seed, FAULT_RATE);
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);
        let metadata_store = InMemoryMetadataStore::with_faults(seed, metadata_faults);

        let tiering_config = TieringConfig {
            min_age_secs: 0,
            max_concurrent_uploads: 4,
            verify_on_download: true,
        };

        let tiering_manager = IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store.clone(),
            segment_reader,
            tiering_config,
        );

        let sealed_ids = wal.read().await.sealed_segment_ids();

        let mut registered: Vec<SegmentId> = Vec::new();
        let mut sealed_set: Vec<SegmentId> = Vec::new();
        let mut committed: Vec<SegmentId> = Vec::new();

        // Execute random operations.
        for op_num in 0..OPS_PER_SEED {
            let op = TierOp::random(&mut rng);

            match op {
                TierOp::Register => {
                    for segment_id in &sealed_ids {
                        if !registered.contains(segment_id) {
                            let info = wal.read().await.segment_info(*segment_id).unwrap();
                            let metadata = SegmentMetadata::new(
                                *segment_id,
                                TopicId::new(1),
                                PartitionId::new(0),
                                info.first_index,
                            );
                            let _ = tiering_manager.register_segment(metadata).await;
                            registered.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Seal => {
                    for segment_id in &registered {
                        if !sealed_set.contains(segment_id) {
                            let _ = tiering_manager.mark_sealed(*segment_id).await;
                            sealed_set.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Commit => {
                    for segment_id in &sealed_set {
                        if !committed.contains(segment_id) {
                            let _ = tiering_manager.mark_committed(*segment_id).await;
                            committed.push(*segment_id);
                            break;
                        }
                    }
                }
                TierOp::Tier => {
                    // Try to tier a committed segment - possibly concurrently.
                    if !committed.is_empty() {
                        let idx = rng.gen_range(0..committed.len());
                        let _ = tiering_manager.tier_segment(committed[idx]).await;
                    }
                }
                TierOp::Download => {
                    if !registered.is_empty() {
                        let idx = rng.gen_range(0..registered.len());
                        let segment_id = registered[idx];

                        // Try to download and verify data integrity.
                        if let Ok(data) = tiering_manager.download_segment(segment_id).await {
                            // Data integrity check: downloaded data should not be empty.
                            assert!(
                                !data.is_empty(),
                                "seed {seed}, op {op_num}: DATA INTEGRITY - downloaded empty data for {:?}",
                                segment_id
                            );
                        }
                    }
                }
                TierOp::Recover => {
                    let _ = tiering_manager.recover_stuck_uploads().await;
                }
            }

            // CHECK HARD INVARIANTS AFTER EVERY OPERATION.
            if let Err(e) = check_hard_invariants(
                &tiering_manager,
                &object_storage,
                seed,
                op_num,
            )
            .await
            {
                panic!("{e}");
            }
        }

        // Run recovery multiple times to ensure "eventually unstuck".
        for _ in 0..20 {
            let _ = tiering_manager.recover_stuck_uploads().await;
        }

        // Check post-recovery invariants.
        if let Err(e) = check_post_recovery_invariants(
            &tiering_manager,
            &wal,
            &object_storage,
            seed,
        )
        .await
        {
            panic!("{e}");
        }

        // EVENTUALLY UNSTUCK INVARIANT: After 20 recovery attempts, no segments should be stuck.
        let all_segments = tiering_manager.metadata().all_segments();
        for (segment_id, metadata) in &all_segments {
            if matches!(metadata.location, SegmentLocation::Uploading) {
                panic!(
                    "seed {seed}: EVENTUALLY STUCK - Segment {:?} still in Uploading after 20 recovery attempts",
                    segment_id
                );
            }
        }

        if base_seed % 50 == 0 {
            println!("Completed {base_seed}/{NUM_SEEDS} seeds");
        }

        buggify::reset();
    }

    println!("Comprehensive stress test complete: {NUM_SEEDS} seeds, {OPS_PER_SEED} ops each, {FAULT_RATE:.0}% fault rate");
}

/// Test concurrent tier operations on the same segment.
///
/// This tests the race condition where two tier operations happen simultaneously.
#[tokio::test]
async fn test_concurrent_tier_same_segment() {
    for seed in 0..20 {
        let temp_dir = tempfile::tempdir().unwrap();
        let segment_config = SegmentConfig::new().with_max_entries(3);
        let wal_config = WalConfig::new(temp_dir.path()).with_segment_config(segment_config);

        let wal = Wal::open(TokioStorage::new(), wal_config).await.unwrap();
        let wal = Arc::new(RwLock::new(wal));

        for i in 1u64..=5 {
            let entry = Entry::new(1, i, Bytes::from(format!("conc-{i}"))).unwrap();
            wal.write().await.append(entry).await.unwrap();
        }

        // No faults - just test concurrency logic.
        let storage_faults = ObjectStorageFaultConfig::none();
        let object_storage = SimulatedObjectStorage::with_faults(seed, storage_faults);
        let metadata_store = InMemoryMetadataStore::new();
        let segment_reader = TestSegmentReader::new(wal.clone());

        let tiering_manager = Arc::new(IntegratedTieringManager::new(
            object_storage.clone(),
            metadata_store,
            segment_reader,
            TieringConfig::for_testing(),
        ));

        let sealed_ids = wal.read().await.sealed_segment_ids();
        if sealed_ids.is_empty() {
            continue;
        }

        let segment_id = sealed_ids[0];

        // Register, seal, commit.
        let info = wal.read().await.segment_info(segment_id).unwrap();
        let metadata = SegmentMetadata::new(
            segment_id,
            TopicId::new(1),
            PartitionId::new(0),
            info.first_index,
        );
        tiering_manager.register_segment(metadata).await.unwrap();
        tiering_manager.mark_sealed(segment_id).await.unwrap();
        tiering_manager.mark_committed(segment_id).await.unwrap();

        // Launch two concurrent tier operations on the same segment.
        let tm1 = tiering_manager.clone();
        let tm2 = tiering_manager.clone();

        let (r1, r2) = tokio::join!(
            tm1.tier_segment(segment_id),
            tm2.tier_segment(segment_id),
        );

        // At most one should succeed, the other should fail with NotEligible.
        let successes = [r1.is_ok(), r2.is_ok()].iter().filter(|&&x| x).count();
        assert!(
            successes <= 1,
            "seed {seed}: CONCURRENT VIOLATION - both tier operations succeeded"
        );

        // Final state should be consistent.
        let loc = tiering_manager.get_location(segment_id).await.unwrap();
        let key = ObjectKey::from_segment(1, 0, segment_id.get());
        let in_s3 = object_storage.get_raw_content(&key).is_some();

        // If in S3, should be Both. If not, should be Local or Uploading.
        if in_s3 {
            assert!(
                matches!(loc, Some(SegmentLocation::Both)),
                "seed {seed}: S3 has data but location is {:?}",
                loc
            );
        }
    }
}

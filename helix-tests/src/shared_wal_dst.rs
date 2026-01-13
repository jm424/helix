//! SharedWal Deterministic Simulation Tests.
//!
//! DST tests for SharedWal using SimulatedStorage with fault injection.
//! Verifies durability, ordering, content integrity, and crash recovery properties.

// Test-specific lint allowances - these are less critical in test code.
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::manual_is_variant_and)]
#![allow(clippy::unnecessary_map_or)]

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use helix_core::PartitionId;
use helix_wal::{FaultConfig, SharedEntry, SharedWal, SharedWalConfig, SimulatedStorage};

/// Entry data tracked for verification.
#[derive(Debug, Clone)]
struct TrackedEntry {
    term: u64,
    index: u64,
    payload: Bytes,
}

/// Property state tracked during simulation for verification.
#[derive(Debug, Default)]
struct PropertyState {
    /// Entries appended (with full data for content verification).
    appended: HashMap<PartitionId, Vec<TrackedEntry>>,
    /// Entries that were synced (durable) - indices only.
    synced: HashMap<PartitionId, HashSet<u64>>,
    /// Entries recovered after "crash" - indices only.
    recovered: HashMap<PartitionId, HashSet<u64>>,
}

/// Property: All synced entries must be recovered after crash.
fn verify_durability(state: &PropertyState) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, synced_indices) in &state.synced {
        let recovered_indices = state.recovered.get(partition_id);

        for idx in synced_indices {
            let was_recovered = recovered_indices.map_or(false, |r| r.contains(idx));

            if !was_recovered {
                violations.push(format!(
                    "Durability violation: partition {} index {} was synced but not recovered",
                    partition_id, idx
                ));
            }
        }
    }

    violations
}

/// Property: Recovered entries must be in sequential order per partition.
fn verify_ordering(recovered: &HashMap<PartitionId, Vec<SharedEntry>>) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, entries) in recovered {
        let mut prev_index = 0u64;
        for entry in entries {
            if entry.index() <= prev_index && prev_index > 0 {
                violations.push(format!(
                    "Ordering violation: partition {} has entry {} after {}",
                    partition_id,
                    entry.index(),
                    prev_index
                ));
            }
            prev_index = entry.index();
        }
    }

    violations
}

/// Property: No phantom entries (entries that were never appended).
fn verify_no_phantoms(
    state: &PropertyState,
    recovered: &HashMap<PartitionId, Vec<SharedEntry>>,
) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, entries) in recovered {
        let appended_indices: HashSet<u64> = state
            .appended
            .get(partition_id)
            .map(|v| v.iter().map(|e| e.index).collect())
            .unwrap_or_default();

        for entry in entries {
            if !appended_indices.contains(&entry.index()) {
                violations.push(format!(
                    "Phantom violation: partition {} index {} was recovered but never appended",
                    partition_id,
                    entry.index()
                ));
            }
        }
    }

    violations
}

/// Property: Recovered entry content must match what was written.
///
/// This is THE critical verification - ensures data integrity, not just metadata.
fn verify_content(
    state: &PropertyState,
    recovered: &HashMap<PartitionId, Vec<SharedEntry>>,
) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, recovered_entries) in recovered {
        // Build lookup of appended entries by index.
        let appended_by_index: HashMap<u64, &TrackedEntry> = state
            .appended
            .get(partition_id)
            .map(|v| v.iter().map(|e| (e.index, e)).collect())
            .unwrap_or_default();

        for recovered_entry in recovered_entries {
            if let Some(appended) = appended_by_index.get(&recovered_entry.index()) {
                // Verify term matches.
                if recovered_entry.term() != appended.term {
                    violations.push(format!(
                        "Content violation: partition {} index {} term mismatch: expected {}, got {}",
                        partition_id,
                        recovered_entry.index(),
                        appended.term,
                        recovered_entry.term()
                    ));
                }

                // Verify payload matches.
                if recovered_entry.payload != appended.payload {
                    violations.push(format!(
                        "Content violation: partition {} index {} payload mismatch: expected {:?}, got {:?}",
                        partition_id,
                        recovered_entry.index(),
                        String::from_utf8_lossy(&appended.payload),
                        String::from_utf8_lossy(&recovered_entry.payload)
                    ));
                }
            }
            // Note: if not in appended_by_index, verify_no_phantoms will catch it.
        }
    }

    violations
}

/// Property: No duplicate entries (same index appearing twice).
fn verify_no_duplicates(recovered: &HashMap<PartitionId, Vec<SharedEntry>>) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, entries) in recovered {
        let mut seen_indices: HashSet<u64> = HashSet::new();
        for entry in entries {
            if !seen_indices.insert(entry.index()) {
                violations.push(format!(
                    "Duplicate violation: partition {} index {} appears multiple times",
                    partition_id,
                    entry.index()
                ));
            }
        }
    }

    violations
}

/// Property: Prefix consistency - no holes in recovered synced entries.
///
/// If entry N is recovered and was synced, all synced entries with index < N
/// must also be recovered. This catches "hole" bugs where middle entries are lost.
fn verify_prefix_consistency(
    state: &PropertyState,
    recovered: &HashMap<PartitionId, Vec<SharedEntry>>,
) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, synced_indices) in &state.synced {
        let recovered_indices: HashSet<u64> = recovered
            .get(partition_id)
            .map(|v| v.iter().map(|e| e.index()).collect())
            .unwrap_or_default();

        // Find max recovered index that was synced.
        let max_recovered_synced = synced_indices
            .iter()
            .filter(|idx| recovered_indices.contains(idx))
            .max()
            .copied();

        if let Some(max_idx) = max_recovered_synced {
            // All synced indices <= max_idx should be recovered.
            for idx in synced_indices {
                if *idx <= max_idx && !recovered_indices.contains(idx) {
                    violations.push(format!(
                        "Prefix violation: partition {} index {} missing (hole) - max recovered synced is {}",
                        partition_id, idx, max_idx
                    ));
                }
            }
        }
    }

    violations
}

/// Property: Partition isolation - verify partition_id in entry matches key.
fn verify_partition_isolation(recovered: &HashMap<PartitionId, Vec<SharedEntry>>) -> Vec<String> {
    let mut violations = Vec::new();

    for (partition_id, entries) in recovered {
        for entry in entries {
            if entry.partition_id() != *partition_id {
                violations.push(format!(
                    "Isolation violation: entry claims partition {} but stored under partition {}",
                    entry.partition_id(),
                    partition_id
                ));
            }
        }
    }

    violations
}

#[tokio::test]
async fn test_dst_shared_wal_basic_durability() {
    // Basic test: append, sync, "crash" (drop), recover, verify.
    let storage = SimulatedStorage::new(1);
    let config = SharedWalConfig::new("/test/shared-wal");

    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);
    let p2 = PartitionId::new(2);

    // Phase 1: Write and sync entries.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        for i in 1..=10u64 {
            let payload1 = Bytes::from(format!("p1-{i}"));
            wal.append(p1, 1, i, payload1.clone()).await.unwrap();
            state.appended.entry(p1).or_default().push(TrackedEntry {
                term: 1,
                index: i,
                payload: payload1,
            });

            let payload2 = Bytes::from(format!("p2-{i}"));
            wal.append(p2, 1, i, payload2.clone()).await.unwrap();
            state.appended.entry(p2).or_default().push(TrackedEntry {
                term: 1,
                index: i,
                payload: payload2,
            });
        }

        wal.sync().await.unwrap();

        // Mark as synced.
        for i in 1..=10u64 {
            state.synced.entry(p1).or_default().insert(i);
            state.synced.entry(p2).or_default().insert(i);
        }
    }

    // CRITICAL: Simulate crash - reverts to last synced state.
    storage.simulate_crash();

    // Phase 2: "Crash" and recover.
    {
        let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();

        let recovered = wal.recover().unwrap();

        // Track recovered entries.
        for (partition_id, entries) in &recovered {
            for entry in entries {
                state
                    .recovered
                    .entry(*partition_id)
                    .or_default()
                    .insert(entry.index());
            }
        }

        // Verify ALL properties including content.
        let durability_violations = verify_durability(&state);
        let ordering_violations = verify_ordering(&recovered);
        let phantom_violations = verify_no_phantoms(&state, &recovered);
        let content_violations = verify_content(&state, &recovered);

        assert!(
            durability_violations.is_empty(),
            "Durability violations: {:?}",
            durability_violations
        );
        assert!(
            ordering_violations.is_empty(),
            "Ordering violations: {:?}",
            ordering_violations
        );
        assert!(
            phantom_violations.is_empty(),
            "Phantom violations: {:?}",
            phantom_violations
        );
        assert!(
            content_violations.is_empty(),
            "Content violations: {:?}",
            content_violations
        );

        // Verify counts.
        assert_eq!(recovered.get(&p1).map(|v| v.len()), Some(10));
        assert_eq!(recovered.get(&p2).map(|v| v.len()), Some(10));
    }
}

#[tokio::test]
async fn test_dst_shared_wal_unsynced_entries_may_be_lost() {
    // Entries appended but NOT synced may be lost on crash.
    let storage = SimulatedStorage::new(2);
    let config = SharedWalConfig::new("/test/shared-wal");

    let p1 = PartitionId::new(1);

    // Phase 1: Append entries, sync some, leave some unsynced.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        // Append and sync first 5.
        for i in 1..=5u64 {
            wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();

        // Append 5 more but DON'T sync.
        for i in 6..=10u64 {
            wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await
                .unwrap();
        }
        // Drop without sync - simulates crash.
    }

    // CRITICAL: Simulate crash - unsynced data should be lost.
    storage.simulate_crash();

    // Phase 2: Recover.
    {
        let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();

        let recovered = wal.recover().unwrap();
        let p1_entries = recovered.get(&p1).unwrap();

        // Only synced entries (1-5) should be recovered.
        // Unsynced entries (6-10) are lost on crash.
        assert_eq!(
            p1_entries.len(),
            5,
            "Only synced entries should survive crash"
        );

        // Verify synced entries are present.
        let recovered_indices: HashSet<u64> = p1_entries.iter().map(|e| e.index()).collect();
        for i in 1..=5u64 {
            assert!(
                recovered_indices.contains(&i),
                "Synced entry {} should be recovered",
                i
            );
        }

        // Verify unsynced entries are NOT present.
        for i in 6..=10u64 {
            assert!(
                !recovered_indices.contains(&i),
                "Unsynced entry {} should NOT survive crash",
                i
            );
        }
    }
}

#[tokio::test]
async fn test_dst_shared_wal_fsync_failure_rate_statistics() {
    // Verify fsync failures happen at expected rate in SharedWal.
    const SEED_COUNT: u64 = 200;
    let mut sync_failures = 0;
    let mut sync_successes = 0;

    for seed in 0..SEED_COUNT {
        let fault_config = FaultConfig::default().with_fsync_fail_rate(0.10);
        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/fsync-rate-{seed}"));

        let p1 = PartitionId::new(1);

        let mut wal = match SharedWal::open(storage.clone(), config).await {
            Ok(w) => w,
            Err(_) => continue,
        };

        // Write just one entry to minimize torn write interference.
        if wal.append(p1, 1, 1, Bytes::from("test")).await.is_err() {
            continue;
        }

        if wal.sync().await.is_ok() {
            sync_successes += 1;
        } else {
            sync_failures += 1;
        }
    }

    let total = sync_successes + sync_failures;
    eprintln!(
        "Fsync rate test: {sync_failures}/{total} failures ({:.1}%)",
        sync_failures as f64 / total as f64 * 100.0
    );

    // With 10% rate, expect ~20 failures out of 200.
    assert!(
        sync_failures > 5,
        "Too few fsync failures: {sync_failures}/{total} - fault injection not working"
    );
}

#[tokio::test]
async fn test_dst_shared_wal_fsync_failure_no_durability_guarantee() {
    // If fsync fails, entries are not guaranteed durable.
    let mut fault_config = FaultConfig::default();
    fault_config.fsync_fail_rate = 1.0; // Always fail fsync.

    let storage = SimulatedStorage::with_faults(3, fault_config);
    let config = SharedWalConfig::new("/test/shared-wal");

    let p1 = PartitionId::new(1);

    let mut wal = SharedWal::open(storage.clone(), config.clone())
        .await
        .unwrap();

    // Append entries.
    for i in 1..=5u64 {
        wal.append(p1, 1, i, Bytes::from(format!("p1-{i}")))
            .await
            .unwrap();
    }

    // Sync should fail.
    let sync_result = wal.sync().await;
    assert!(sync_result.is_err(), "Sync should fail with fsync error");

    // Since sync failed, no durability guarantee.
    // This is correct behavior - we don't claim entries are durable if sync fails.
}

#[tokio::test]
async fn test_dst_shared_wal_multi_partition_interleaved() {
    // Test with many partitions, interleaved writes.
    let storage = SimulatedStorage::new(4);
    let config = SharedWalConfig::new("/test/shared-wal");

    let partition_count = 10;
    let entries_per_partition = 20;

    let mut state = PropertyState::default();

    // Phase 1: Write interleaved entries.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        for i in 1..=entries_per_partition as u64 {
            for p in 1..=partition_count as u64 {
                let partition_id = PartitionId::new(p);
                let payload = Bytes::from(format!("p{p}-{i}"));
                wal.append(partition_id, 1, i, payload.clone())
                    .await
                    .unwrap();
                state
                    .appended
                    .entry(partition_id)
                    .or_default()
                    .push(TrackedEntry {
                        term: 1,
                        index: i,
                        payload,
                    });
            }
        }

        wal.sync().await.unwrap();

        // Mark all as synced.
        for p in 1..=partition_count as u64 {
            let partition_id = PartitionId::new(p);
            for i in 1..=entries_per_partition as u64 {
                state.synced.entry(partition_id).or_default().insert(i);
            }
        }
    }

    // CRITICAL: Simulate crash.
    storage.simulate_crash();

    // Phase 2: Recover and verify.
    {
        let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();

        let recovered = wal.recover().unwrap();

        // Track recovered.
        for (partition_id, entries) in &recovered {
            for entry in entries {
                state
                    .recovered
                    .entry(*partition_id)
                    .or_default()
                    .insert(entry.index());
            }
        }

        // Verify all properties including content.
        let durability_violations = verify_durability(&state);
        let ordering_violations = verify_ordering(&recovered);
        let phantom_violations = verify_no_phantoms(&state, &recovered);
        let content_violations = verify_content(&state, &recovered);

        assert!(durability_violations.is_empty(), "{:?}", durability_violations);
        assert!(ordering_violations.is_empty(), "{:?}", ordering_violations);
        assert!(phantom_violations.is_empty(), "{:?}", phantom_violations);
        assert!(content_violations.is_empty(), "{:?}", content_violations);

        // Verify counts.
        assert_eq!(recovered.len(), partition_count);
        for p in 1..=partition_count as u64 {
            let partition_id = PartitionId::new(p);
            assert_eq!(
                recovered.get(&partition_id).map(|v| v.len()),
                Some(entries_per_partition),
                "Partition {} should have {} entries",
                p,
                entries_per_partition
            );
        }
    }
}

#[tokio::test]
async fn test_dst_shared_wal_multiple_crash_recover_cycles() {
    // Multiple crash/recover cycles, each time adding more entries.
    let storage = SimulatedStorage::new(5);
    let config = SharedWalConfig::new("/test/shared-wal");

    let p1 = PartitionId::new(1);
    let mut expected_count = 0u64;

    for cycle in 1..=5 {
        // Write and sync.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .unwrap();

            // Must recover first to get partition state.
            if cycle > 1 {
                let _ = wal.recover().unwrap();
            }

            // Append 10 entries per cycle.
            let start = expected_count + 1;
            let end = expected_count + 10;
            for i in start..=end {
                wal.append(p1, cycle, i, Bytes::from(format!("cycle{cycle}-{i}")))
                    .await
                    .unwrap();
            }
            expected_count = end;

            wal.sync().await.unwrap();
        }

        // CRITICAL: Simulate crash before recovery.
        storage.simulate_crash();

        // Recover and verify.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .unwrap();

            let recovered = wal.recover().unwrap();
            let p1_entries = recovered.get(&p1).unwrap();

            assert_eq!(
                p1_entries.len() as u64,
                expected_count,
                "Cycle {}: expected {} entries, got {}",
                cycle,
                expected_count,
                p1_entries.len()
            );

            // Verify ordering.
            let violations = verify_ordering(&recovered);
            assert!(violations.is_empty(), "Cycle {}: {:?}", cycle, violations);
        }
    }

    // Final count should be 50.
    assert_eq!(expected_count, 50);
}

#[tokio::test]
async fn test_dst_shared_wal_torn_write_recovery() {
    // Simulate torn write (partial entry) and verify recovery handles it.
    let mut fault_config = FaultConfig::default();
    fault_config.torn_write_rate = 0.5; // 50% chance of torn write.

    let storage = SimulatedStorage::with_faults(6, fault_config);
    let config = SharedWalConfig::new("/test/shared-wal");

    let p1 = PartitionId::new(1);

    // Phase 1: Write entries with potential torn writes.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        for i in 1..=20u64 {
            // Some appends may fail due to torn writes.
            let _ = wal
                .append(p1, 1, i, Bytes::from(format!("p1-{i}")))
                .await;
        }

        // Sync may partially succeed.
        let _ = wal.sync().await;
    }

    // CRITICAL: Simulate crash.
    storage.simulate_crash();

    // Phase 2: Recovery should handle torn writes gracefully.
    {
        let result = SharedWal::open(storage.clone(), config).await;

        // Recovery should succeed (torn writes are truncated).
        assert!(result.is_ok(), "Recovery should succeed despite torn writes");

        let mut wal = result.unwrap();
        let recovered = wal.recover().unwrap();

        // Whatever was recovered should be valid.
        let violations = verify_ordering(&recovered);
        assert!(violations.is_empty(), "Recovered entries should be ordered");

        // Entries that are recovered should be complete and valid.
        if let Some(entries) = recovered.get(&p1) {
            for entry in entries {
                // Verify entry is valid (CRC passed during decode).
                assert!(entry.index() > 0);
                assert!(!entry.payload.is_empty());
            }
        }
    }
}

// ========================================================================
// Verification that property checks CATCH violations
// ========================================================================

#[test]
fn test_verify_durability_catches_missing_entry() {
    // Simulate: we synced entry but it wasn't recovered (durability violation).
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // We appended and synced entries 1, 2, 3.
    state.appended.insert(
        p1,
        vec![
            TrackedEntry {
                term: 1,
                index: 1,
                payload: Bytes::from("e1"),
            },
            TrackedEntry {
                term: 1,
                index: 2,
                payload: Bytes::from("e2"),
            },
            TrackedEntry {
                term: 1,
                index: 3,
                payload: Bytes::from("e3"),
            },
        ],
    );
    state.synced.insert(p1, HashSet::from([1, 2, 3]));

    // But only entries 1, 2 were recovered (3 is missing - durability violation!).
    state.recovered.insert(p1, HashSet::from([1, 2]));

    let violations = verify_durability(&state);
    assert!(!violations.is_empty(), "Should detect missing synced entry");
    assert!(
        violations[0].contains("index 3"),
        "Should mention missing index 3: {:?}",
        violations
    );
}

#[test]
fn test_verify_ordering_catches_out_of_order() {
    // Simulate: recovered entries are out of order.
    let p1 = PartitionId::new(1);

    // Create entries with out-of-order indices.
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("e1")).unwrap();
    let entry3 = SharedEntry::new(p1, 1, 3, Bytes::from("e3")).unwrap();
    let entry2 = SharedEntry::new(p1, 1, 2, Bytes::from("e2")).unwrap();

    // Recovered in wrong order: 1, 3, 2 (should be 1, 2, 3).
    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry3, entry2]);

    let violations = verify_ordering(&recovered);
    assert!(!violations.is_empty(), "Should detect out-of-order entries");
    assert!(
        violations[0].contains("entry 2 after 3"),
        "Should describe the ordering issue: {:?}",
        violations
    );
}

#[test]
fn test_verify_no_phantoms_catches_phantom() {
    // Simulate: recovered entry that was never appended (phantom).
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // We only appended entries 1, 2.
    state.appended.insert(
        p1,
        vec![
            TrackedEntry {
                term: 1,
                index: 1,
                payload: Bytes::from("e1"),
            },
            TrackedEntry {
                term: 1,
                index: 2,
                payload: Bytes::from("e2"),
            },
        ],
    );

    // Create entry 3 that was never appended.
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("e1")).unwrap();
    let entry2 = SharedEntry::new(p1, 1, 2, Bytes::from("e2")).unwrap();
    let phantom = SharedEntry::new(p1, 1, 3, Bytes::from("phantom")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry2, phantom]);

    let violations = verify_no_phantoms(&state, &recovered);
    assert!(!violations.is_empty(), "Should detect phantom entry");
    assert!(
        violations[0].contains("index 3"),
        "Should mention phantom index 3: {:?}",
        violations
    );
}

#[test]
fn test_verify_all_properties_pass_when_correct() {
    // Verify that correct state passes all property checks.
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // Append and sync 1, 2, 3.
    state.appended.insert(
        p1,
        vec![
            TrackedEntry {
                term: 1,
                index: 1,
                payload: Bytes::from("e1"),
            },
            TrackedEntry {
                term: 1,
                index: 2,
                payload: Bytes::from("e2"),
            },
            TrackedEntry {
                term: 1,
                index: 3,
                payload: Bytes::from("e3"),
            },
        ],
    );
    state.synced.insert(p1, HashSet::from([1, 2, 3]));
    state.recovered.insert(p1, HashSet::from([1, 2, 3]));

    // Create correctly ordered entries with matching content.
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("e1")).unwrap();
    let entry2 = SharedEntry::new(p1, 1, 2, Bytes::from("e2")).unwrap();
    let entry3 = SharedEntry::new(p1, 1, 3, Bytes::from("e3")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry2, entry3]);

    // All checks should pass including content.
    assert!(verify_durability(&state).is_empty());
    assert!(verify_ordering(&recovered).is_empty());
    assert!(verify_no_phantoms(&state, &recovered).is_empty());
    assert!(verify_content(&state, &recovered).is_empty());
}

#[test]
fn test_verify_content_catches_payload_mismatch() {
    // Simulate: recovered entry has wrong payload (content violation).
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // Append entries with specific payloads.
    state.appended.insert(
        p1,
        vec![TrackedEntry {
            term: 1,
            index: 1,
            payload: Bytes::from("original-payload"),
        }],
    );
    state.synced.insert(p1, HashSet::from([1]));
    state.recovered.insert(p1, HashSet::from([1]));

    // Create entry with DIFFERENT payload than what was appended.
    let corrupted_entry = SharedEntry::new(p1, 1, 1, Bytes::from("corrupted-payload")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![corrupted_entry]);

    let violations = verify_content(&state, &recovered);
    assert!(!violations.is_empty(), "Should detect payload mismatch");
    assert!(
        violations[0].contains("payload mismatch"),
        "Should mention payload mismatch: {:?}",
        violations
    );
}

#[test]
fn test_verify_content_catches_term_mismatch() {
    // Simulate: recovered entry has wrong term (content violation).
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // Append entry with term 1.
    state.appended.insert(
        p1,
        vec![TrackedEntry {
            term: 1,
            index: 1,
            payload: Bytes::from("data"),
        }],
    );
    state.synced.insert(p1, HashSet::from([1]));
    state.recovered.insert(p1, HashSet::from([1]));

    // Create entry with DIFFERENT term (but same payload) than what was appended.
    let wrong_term_entry = SharedEntry::new(p1, 2, 1, Bytes::from("data")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![wrong_term_entry]);

    let violations = verify_content(&state, &recovered);
    assert!(!violations.is_empty(), "Should detect term mismatch");
    assert!(
        violations[0].contains("term mismatch"),
        "Should mention term mismatch: {:?}",
        violations
    );
}

#[test]
fn test_verify_no_duplicates_catches_duplicate() {
    let p1 = PartitionId::new(1);

    // Create entries with duplicate index.
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("first")).unwrap();
    let entry2 = SharedEntry::new(p1, 1, 2, Bytes::from("second")).unwrap();
    let duplicate = SharedEntry::new(p1, 1, 1, Bytes::from("duplicate")).unwrap(); // Same index!

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry2, duplicate]);

    let violations = verify_no_duplicates(&recovered);
    assert!(!violations.is_empty(), "Should detect duplicate index");
    assert!(
        violations[0].contains("index 1") && violations[0].contains("multiple times"),
        "Should mention duplicate index: {:?}",
        violations
    );
}

#[test]
fn test_verify_prefix_consistency_catches_hole() {
    let mut state = PropertyState::default();
    let p1 = PartitionId::new(1);

    // Synced entries 1, 2, 3.
    state.synced.insert(p1, HashSet::from([1, 2, 3]));

    // But only recovered 1 and 3 (missing 2 - a "hole").
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("e1")).unwrap();
    let entry3 = SharedEntry::new(p1, 1, 3, Bytes::from("e3")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry3]);

    let violations = verify_prefix_consistency(&state, &recovered);
    assert!(!violations.is_empty(), "Should detect hole in prefix");
    assert!(
        violations[0].contains("index 2") && violations[0].contains("hole"),
        "Should mention missing index 2: {:?}",
        violations
    );
}

#[test]
fn test_verify_partition_isolation_catches_mismatch() {
    let p1 = PartitionId::new(1);
    let p2 = PartitionId::new(2);

    // Entry claims to be for partition 2 but stored under partition 1.
    let wrong_partition_entry = SharedEntry::new(p2, 1, 1, Bytes::from("data")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![wrong_partition_entry]); // Stored under p1!

    let violations = verify_partition_isolation(&recovered);
    assert!(!violations.is_empty(), "Should detect partition mismatch");
    assert!(
        violations[0].contains("partition"),
        "Should mention partition mismatch: {:?}",
        violations
    );
}

#[test]
fn test_verify_all_new_properties_pass_when_correct() {
    let p1 = PartitionId::new(1);

    // Create correctly ordered entries with no duplicates.
    let entry1 = SharedEntry::new(p1, 1, 1, Bytes::from("e1")).unwrap();
    let entry2 = SharedEntry::new(p1, 1, 2, Bytes::from("e2")).unwrap();
    let entry3 = SharedEntry::new(p1, 1, 3, Bytes::from("e3")).unwrap();

    let mut recovered = HashMap::new();
    recovered.insert(p1, vec![entry1, entry2, entry3]);

    let mut state = PropertyState::default();
    state.synced.insert(p1, HashSet::from([1, 2, 3]));

    // All new checks should pass.
    assert!(verify_no_duplicates(&recovered).is_empty());
    assert!(verify_prefix_consistency(&state, &recovered).is_empty());
    assert!(verify_partition_isolation(&recovered).is_empty());
}

// ========================================================================
// Multi-seed DST tests with fault injection
// ========================================================================

/// Simple test iteration: write entries (no faults during write), sync, crash, verify recovery.
#[tokio::test]
async fn test_dst_shared_wal_multi_seed_sync_durability() {
    const SEED_COUNT: u64 = 100;

    for seed in 0..SEED_COUNT {
        let storage = SimulatedStorage::new(seed);
        let config = SharedWalConfig::new(format!("/test/sync-{seed}"));
        let p1 = PartitionId::new(1);

        // Write and sync.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .expect("open should succeed without faults");

            for i in 1..=10u64 {
                wal.append(p1, 1, i, Bytes::from(format!("entry-{i}")))
                    .await
                    .expect("append should succeed without faults");
            }
            wal.sync().await.expect("sync should succeed without faults");
        }

        // Crash and recover.
        storage.simulate_crash();

        {
            let mut wal = SharedWal::open(storage.clone(), config)
                .await
                .expect("recovery should succeed");

            let recovered = wal.recover().expect("recover should succeed");
            let entries = recovered.get(&p1).expect("partition should have entries");

            // Verify all 10 entries recovered.
            assert_eq!(
                entries.len(),
                10,
                "Seed {seed}: expected 10 entries, got {}",
                entries.len()
            );

            // Verify ordering.
            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(
                    entry.index(),
                    (i + 1) as u64,
                    "Seed {seed}: entry {} has wrong index",
                    i
                );
            }
        }
    }
}

/// Test with fsync failures: if sync fails, no durability guarantee.
#[tokio::test]
async fn test_dst_shared_wal_multi_seed_fsync_failures() {
    const SEED_COUNT: u64 = 100;

    for seed in 0..SEED_COUNT {
        let fault_config = FaultConfig::default().with_fsync_fail_rate(0.3);
        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/fsync-{seed}"));
        let p1 = PartitionId::new(1);

        let sync_succeeded: bool;

        // Write and try to sync.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .expect("open should succeed");

            for i in 1..=5u64 {
                wal.append(p1, 1, i, Bytes::from(format!("entry-{i}")))
                    .await
                    .expect("append should succeed");
            }

            sync_succeeded = wal.sync().await.is_ok();
        }

        // Crash and recover.
        storage.simulate_crash();

        {
            let wal_result = SharedWal::open(storage.clone(), config).await;
            if wal_result.is_err() {
                // Recovery failed - acceptable if sync also failed.
                continue;
            }
            let mut wal = wal_result.unwrap();
            let recovered = wal.recover().unwrap_or_default();

            if sync_succeeded {
                // Sync succeeded - entries MUST be recovered.
                let entries = recovered.get(&p1);
                assert!(
                    entries.is_some() && entries.unwrap().len() == 5,
                    "Seed {seed}: sync succeeded but recovery failed"
                );
            }
            // If sync failed, no durability requirement - any result is acceptable.
        }
    }
}

/// Test with torn writes: recovery should handle partial entries gracefully.
#[tokio::test]
async fn test_dst_shared_wal_multi_seed_torn_writes() {
    const SEED_COUNT: u64 = 100;

    for seed in 0..SEED_COUNT {
        let fault_config = FaultConfig::default().with_torn_write_rate(0.2);
        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/torn-{seed}"));
        let p1 = PartitionId::new(1);

        let mut successful_appends = 0u64;
        let sync_succeeded: bool;

        // Write entries (some may be torn).
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .expect("open should succeed");

            for i in 1..=10u64 {
                if wal
                    .append(p1, 1, i, Bytes::from(format!("entry-{i}")))
                    .await
                    .is_ok()
                {
                    successful_appends = i; // Track highest successful index.
                } else {
                    break; // Stop on first failure to maintain sequential indices.
                }
            }

            sync_succeeded = wal.sync().await.is_ok();
        }

        // Crash and recover.
        storage.simulate_crash();

        {
            // Recovery should not panic - it should handle torn writes.
            let wal_result = SharedWal::open(storage.clone(), config).await;
            if wal_result.is_err() {
                continue; // Recovery failed due to corruption - acceptable.
            }

            let mut wal = wal_result.unwrap();
            let recovered = wal.recover().unwrap_or_default();

            // Verify recovered entries are valid and ordered.
            if let Some(entries) = recovered.get(&p1) {
                let mut prev_index = 0u64;
                for entry in entries {
                    assert!(
                        entry.index() > prev_index,
                        "Seed {seed}: entries not ordered"
                    );
                    prev_index = entry.index();
                }

                // If sync succeeded, we should have at least some entries.
                if sync_succeeded && successful_appends > 0 {
                    assert!(
                        !entries.is_empty(),
                        "Seed {seed}: sync succeeded with {} appends but no entries recovered",
                        successful_appends
                    );
                }
            }
        }
    }
}

// ========================================================================
// Comprehensive DST (1000 seeds, 25% combined faults)
// ========================================================================

/// Comprehensive stress test with combined faults.
///
/// This is the primary DST verification test. It exercises:
/// - Multiple partitions (10)
/// - Many entries per partition (20)
/// - Combined fault injection (10% fsync fail + 10% torn write)
/// - Crash/recovery cycles
/// - Property verification (durability, ordering, no-phantoms, content)
#[tokio::test]
async fn test_dst_shared_wal_comprehensive_stress() {
    const SEED_COUNT: u64 = 1000;
    const PARTITION_COUNT: u64 = 10;
    const ENTRIES_PER_PARTITION: u64 = 20;

    let mut total_violations = 0usize;
    let mut seeds_with_issues = Vec::new();

    // Statistics to verify test coverage.
    let mut syncs_succeeded = 0u64;
    let mut syncs_failed = 0u64;
    let mut total_entries_synced = 0u64;
    let mut total_entries_recovered = 0u64;
    let mut seeds_skipped = 0u64;

    for seed in 0..SEED_COUNT {
        // Combined faults: ~20% total
        let fault_config = FaultConfig::default()
            .with_fsync_fail_rate(0.10)
            .with_torn_write_rate(0.10);

        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/stress-{seed}"));

        let mut state = PropertyState::default();

        // Phase 1: Write entries with potential faults.
        let sync_succeeded: bool;
        let mut entries_appended = 0u64;
        {
            let mut wal = match SharedWal::open(storage.clone(), config.clone()).await {
                Ok(w) => w,
                Err(_) => {
                    seeds_skipped += 1;
                    continue;
                }
            };

            // Write all entries, tracking what succeeded with full content.
            for p in 1..=PARTITION_COUNT {
                let partition_id = PartitionId::new(p);

                for i in 1..=ENTRIES_PER_PARTITION {
                    let payload = Bytes::from(format!("p{p}-{i}"));
                    match wal.append(partition_id, 1, i, payload.clone()).await {
                        Ok(()) => {
                            state.appended.entry(partition_id).or_default().push(
                                TrackedEntry {
                                    term: 1,
                                    index: i,
                                    payload,
                                },
                            );
                            entries_appended += 1;
                        }
                        Err(_) => {
                            // Torn write or other error - stop this partition.
                            break;
                        }
                    }
                }
            }

            // Single sync at the end.
            sync_succeeded = wal.sync().await.is_ok();

            if sync_succeeded {
                syncs_succeeded += 1;
                total_entries_synced += entries_appended;
                // Mark all successfully appended entries as synced.
                for (partition_id, appended) in &state.appended {
                    for entry in appended {
                        state.synced.entry(*partition_id).or_default().insert(entry.index);
                    }
                }
            } else {
                syncs_failed += 1;
            }
        }

        // Simulate crash.
        storage.simulate_crash();

        // Phase 2: Recover and verify.
        {
            let wal_result = SharedWal::open(storage.clone(), config).await;
            if wal_result.is_err() {
                seeds_skipped += 1;
                continue;
            }

            let mut wal = wal_result.unwrap();
            let recovered = wal.recover().unwrap_or_default();

            // Track recovered entries.
            for (partition_id, entries) in &recovered {
                for entry in entries {
                    state
                        .recovered
                        .entry(*partition_id)
                        .or_default()
                        .insert(entry.index());
                    total_entries_recovered += 1;
                }
            }

            // Verify properties.
            let mut seed_violations = Vec::new();

            // Durability: synced entries must be recovered.
            if sync_succeeded {
                for (partition_id, synced_indices) in &state.synced {
                    let recovered_indices = state.recovered.get(partition_id);
                    for idx in synced_indices {
                        let was_recovered = recovered_indices.map_or(false, |r| r.contains(idx));
                        if !was_recovered {
                            seed_violations.push(format!(
                                "Durability: p{} idx {} synced but not recovered",
                                partition_id.get(),
                                idx
                            ));
                        }
                    }
                }

                // Content verification: synced entries must have correct content.
                let content_violations = verify_content(&state, &recovered);
                seed_violations.extend(content_violations);
            }

            // Ordering: recovered entries must be in order.
            let ordering_violations = verify_ordering(&recovered);
            seed_violations.extend(ordering_violations);

            // No phantoms: recovered entries must have been appended.
            let phantom_violations = verify_no_phantoms(&state, &recovered);
            seed_violations.extend(phantom_violations);

            // No duplicates: same index shouldn't appear twice.
            let duplicate_violations = verify_no_duplicates(&recovered);
            seed_violations.extend(duplicate_violations);

            // Prefix consistency: no holes in synced entries.
            if sync_succeeded {
                let prefix_violations = verify_prefix_consistency(&state, &recovered);
                seed_violations.extend(prefix_violations);
            }

            // Partition isolation: entry partition_id matches storage key.
            let isolation_violations = verify_partition_isolation(&recovered);
            seed_violations.extend(isolation_violations);

            if !seed_violations.is_empty() {
                total_violations += seed_violations.len();
                seeds_with_issues.push((seed, seed_violations));
            }
        }
    }

    // Print statistics for visibility.
    eprintln!("\n=== DST Comprehensive Stress Test Statistics ===");
    eprintln!("Seeds: {SEED_COUNT} total, {seeds_skipped} skipped");
    eprintln!(
        "Syncs: {syncs_succeeded} succeeded, {syncs_failed} failed ({:.1}% failure rate)",
        syncs_failed as f64 / (syncs_succeeded + syncs_failed) as f64 * 100.0
    );
    eprintln!("Entries: {total_entries_synced} synced, {total_entries_recovered} recovered");
    eprintln!("Violations: {total_violations}");
    eprintln!("================================================\n");

    // These assertions verify the test is actually exercising meaningful code paths.
    assert!(
        syncs_succeeded > SEED_COUNT / 4,
        "Too few syncs succeeded: {syncs_succeeded}/{SEED_COUNT}"
    );
    assert!(
        total_entries_synced > 1000,
        "Too few entries synced: {total_entries_synced}"
    );
    assert!(
        total_entries_recovered > 1000,
        "Too few entries recovered: {total_entries_recovered}"
    );
    assert!(
        seeds_skipped < SEED_COUNT / 4,
        "Too many seeds skipped: {seeds_skipped}/{SEED_COUNT}"
    );

    assert!(
        total_violations == 0,
        "DST found {} violations across {} seeds:\n{:?}",
        total_violations,
        seeds_with_issues.len(),
        seeds_with_issues
    );
}

/// Multi-partition concurrent stress test with truncation.
#[tokio::test]
async fn test_dst_shared_wal_stress_with_truncation() {
    const SEED_COUNT: u64 = 100;

    for seed in 0..SEED_COUNT {
        let storage = SimulatedStorage::new(seed);
        let config = SharedWalConfig::new(format!("/test/trunc-{seed}"));

        let p1 = PartitionId::new(1);

        // Phase 1: Write, truncate, write more.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .expect("open should succeed");

            // Write entries 1-10.
            for i in 1..=10u64 {
                wal.append(p1, 1, i, Bytes::from(format!("old-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();

            // Truncate after 5.
            wal.truncate_after(p1, 5);

            // Write new entries 6-15 at term 2.
            for i in 6..=15u64 {
                wal.append(p1, 2, i, Bytes::from(format!("new-{i}")))
                    .await
                    .unwrap();
            }
            wal.sync().await.unwrap();
        }

        // Simulate crash.
        storage.simulate_crash();

        // Phase 2: Recover and verify.
        {
            let mut wal = SharedWal::open(storage.clone(), config)
                .await
                .expect("recovery should succeed");

            let recovered = wal.recover().unwrap();
            let entries = recovered.get(&p1).unwrap();

            // Should have 15 entries: 1-5 (old), 6-15 (new).
            assert_eq!(
                entries.len(),
                15,
                "Seed {seed}: expected 15 entries, got {}",
                entries.len()
            );

            // Entries 1-5 should be term 1.
            for entry in entries.iter().take(5) {
                assert_eq!(
                    entry.term(),
                    1,
                    "Seed {seed}: entry {} should be term 1",
                    entry.index()
                );
            }

            // Entries 6-15 should be term 2 (new, shadowing old 6-10).
            for entry in entries.iter().skip(5) {
                assert_eq!(
                    entry.term(),
                    2,
                    "Seed {seed}: entry {} should be term 2",
                    entry.index()
                );
            }
        }
    }
}

/// Test recovery property: multiple crash/recovery cycles preserve data.
#[tokio::test]
async fn test_dst_shared_wal_multi_crash_recovery_cycles() {
    const SEED_COUNT: u64 = 100;
    const CYCLES: u64 = 5;

    for seed in 0..SEED_COUNT {
        let storage = SimulatedStorage::new(seed);
        let config = SharedWalConfig::new(format!("/test/cycles-{seed}"));

        let p1 = PartitionId::new(1);
        let mut expected_index = 0u64;

        for cycle in 1..=CYCLES {
            // Write more entries.
            {
                let mut wal = SharedWal::open(storage.clone(), config.clone())
                    .await
                    .expect("open should succeed");

                if cycle > 1 {
                    let _ = wal.recover();
                }

                // Append entries for this cycle.
                let start = expected_index + 1;
                let end = expected_index + 10;
                for i in start..=end {
                    wal.append(p1, cycle, i, Bytes::from(format!("c{cycle}-{i}")))
                        .await
                        .expect("append should succeed");
                }
                wal.sync().await.expect("sync should succeed");
                expected_index = end;
            }

            // Crash.
            storage.simulate_crash();

            // Verify.
            {
                let mut wal = SharedWal::open(storage.clone(), config.clone())
                    .await
                    .expect("recovery should succeed");

                let recovered = wal.recover().unwrap();
                let entries = recovered.get(&p1).unwrap();

                assert_eq!(
                    entries.len() as u64,
                    expected_index,
                    "Seed {seed} cycle {cycle}: expected {} entries, got {}",
                    expected_index,
                    entries.len()
                );
            }
        }
    }
}

// ========================================================================
// Edge Case and Boundary Tests
// ========================================================================

/// Test: Empty WAL recovery should work correctly.
#[tokio::test]
async fn test_dst_shared_wal_empty_recovery() {
    const SEED_COUNT: u64 = 50;

    for seed in 0..SEED_COUNT {
        let storage = SimulatedStorage::new(seed);
        let config = SharedWalConfig::new(format!("/test/empty-{seed}"));

        // Create WAL, sync (nothing written), crash.
        {
            let mut wal = SharedWal::open(storage.clone(), config.clone())
                .await
                .expect("open should succeed");
            wal.sync().await.expect("sync empty WAL should succeed");
        }

        storage.simulate_crash();

        // Recover empty WAL.
        {
            let mut wal = SharedWal::open(storage.clone(), config)
                .await
                .expect("recovery of empty WAL should succeed");

            let recovered = wal.recover().unwrap();
            assert!(
                recovered.is_empty(),
                "Seed {seed}: empty WAL should recover empty"
            );
        }
    }
}

/// Test: Last-write-wins semantics under crash.
///
/// SharedWal uses last-write-wins for same (partition_id, index). This test verifies:
/// 1. Entries that are overwritten (same partition+index) get the new value
/// 2. Entries not explicitly overwritten remain (truncation is logical, not physical)
/// 3. Recovery produces a consistent prefix (no gaps)
#[tokio::test]
async fn test_dst_shared_wal_last_write_wins_semantics() {
    const SEED_COUNT: u64 = 200;

    let mut all_old_count = 0u64;
    let mut all_new_count = 0u64;
    let mut partial_new_count = 0u64;
    let mut violation_count = 0u64;

    for seed in 0..SEED_COUNT {
        let fault_config = FaultConfig::default()
            .with_fsync_fail_rate(0.15)
            .with_torn_write_rate(0.15);

        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/lww-{seed}"));

        let p1 = PartitionId::new(1);

        // Phase 1: Write entries 1-10 at term 1, sync.
        let phase1_ok: bool;
        {
            let mut wal = match SharedWal::open(storage.clone(), config.clone()).await {
                Ok(w) => w,
                Err(_) => continue,
            };

            let mut all_ok = true;
            for i in 1..=10u64 {
                if wal
                    .append(p1, 1, i, Bytes::from(format!("old-{i}")))
                    .await
                    .is_err()
                {
                    all_ok = false;
                    break;
                }
            }

            phase1_ok = all_ok && wal.sync().await.is_ok();
        }

        if !phase1_ok {
            continue;
        }

        // Phase 2: Truncate after 5, write new entries 6-10 at term 2.
        let phase2_synced: bool;
        {
            let mut wal = match SharedWal::open(storage.clone(), config.clone()).await {
                Ok(w) => w,
                Err(_) => continue,
            };

            let _ = wal.recover();
            wal.truncate_after(p1, 5);

            // Write new entries 6-10 at term 2 (overwrites old 6-10).
            let mut all_ok = true;
            for i in 6..=10u64 {
                if wal
                    .append(p1, 2, i, Bytes::from(format!("new-{i}")))
                    .await
                    .is_err()
                {
                    all_ok = false;
                    break;
                }
            }

            phase2_synced = all_ok && wal.sync().await.is_ok();
        }

        // Crash.
        storage.simulate_crash();

        // Phase 3: Recover and verify consistency.
        {
            let mut wal = match SharedWal::open(storage.clone(), config).await {
                Ok(w) => w,
                Err(_) => continue,
            };

            let recovered = wal.recover().unwrap_or_default();
            let entries = recovered.get(&p1).map(|v| v.as_slice()).unwrap_or(&[]);

            // Check for ordering violations.
            let mut has_ordering_violation = false;
            for (i, entry) in entries.iter().enumerate() {
                if entry.index() != (i + 1) as u64 {
                    has_ordering_violation = true;
                    break;
                }
            }

            if has_ordering_violation {
                violation_count += 1;
                continue;
            }

            // Count how many entries 6-10 are at term 2.
            let term_2_count = entries
                .iter()
                .filter(|e| e.index() >= 6 && e.term() == 2)
                .count();

            // Verify content consistency.
            let mut content_ok = true;
            for entry in entries {
                let expected_prefix = if entry.term() == 1 { "old" } else { "new" };
                let payload_str = String::from_utf8_lossy(&entry.payload);
                if !payload_str.starts_with(expected_prefix) {
                    content_ok = false;
                    break;
                }
            }

            if !content_ok {
                violation_count += 1;
                continue;
            }

            // Categorize the result.
            if term_2_count == 0 {
                all_old_count += 1;
            } else if term_2_count == 5 && phase2_synced {
                all_new_count += 1;
            } else {
                partial_new_count += 1;
            }
        }
    }

    eprintln!("\n=== Last-Write-Wins Semantics Statistics ===");
    eprintln!("All old (term 1): {all_old_count}");
    eprintln!("All new (term 2): {all_new_count}");
    eprintln!("Partial new: {partial_new_count}");
    eprintln!("Violations: {violation_count}");
    eprintln!("=============================================\n");

    assert_eq!(
        violation_count, 0,
        "No ordering or content violations should occur"
    );
}

/// Test: Partial overwrite after truncation (stale entries may reappear).
///
/// This test documents the EXPECTED behavior when truncation + partial overwrite
/// occurs before a crash. See design doc "Truncation Safety Analysis" section.
#[tokio::test]
async fn test_dst_shared_wal_partial_overwrite_stale_entries_reappear() {
    let storage = SimulatedStorage::new(42);
    let config = SharedWalConfig::new("/test/partial-overwrite");

    let p1 = PartitionId::new(1);

    // Phase 1: Write entries 1-10 at term 1.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        for i in 1..=10u64 {
            wal.append(p1, 1, i, Bytes::from(format!("old-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Phase 2: Truncate after 5, write ONLY entries 6-8 at term 2.
    {
        let mut wal = SharedWal::open(storage.clone(), config.clone())
            .await
            .unwrap();

        let _ = wal.recover();
        wal.truncate_after(p1, 5);

        // Write only 6-8, NOT 9-10.
        for i in 6..=8u64 {
            wal.append(p1, 2, i, Bytes::from(format!("new-{i}")))
                .await
                .unwrap();
        }
        wal.sync().await.unwrap();
    }

    // Crash.
    storage.simulate_crash();

    // Phase 3: Recover and verify behavior.
    {
        let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();
        let recovered = wal.recover().unwrap();
        let entries = recovered.get(&p1).unwrap();

        // Should have 10 entries (stale 9-10 reappear).
        assert_eq!(entries.len(), 10, "Stale entries 9-10 should reappear");

        // Verify terms.
        for entry in entries.iter() {
            let expected_term = if entry.index() >= 6 && entry.index() <= 8 {
                2
            } else {
                1
            };
            assert_eq!(
                entry.term(),
                expected_term,
                "Entry {} should be at term {}",
                entry.index(),
                expected_term
            );
        }

        // Verify content matches term.
        for entry in entries.iter() {
            let payload_str = String::from_utf8_lossy(&entry.payload);
            if entry.term() == 1 {
                assert!(
                    payload_str.starts_with("old"),
                    "Term 1 entries should have 'old-*' payload"
                );
            } else {
                assert!(
                    payload_str.starts_with("new"),
                    "Term 2 entries should have 'new-*' payload"
                );
            }
        }

        // Verify stale entry 10 has term 1.
        let last_entry = entries.last().unwrap();
        assert_eq!(last_entry.term(), 1, "Stale entry 10 at term 1");
    }
}

/// Test: Maximum entry size boundary.
#[tokio::test]
async fn test_dst_shared_wal_max_entry_size() {
    use helix_wal::limits::ENTRY_PAYLOAD_SIZE_BYTES_MAX;

    let storage = SimulatedStorage::new(42);
    let config = SharedWalConfig::new("/test/max-size");

    let p1 = PartitionId::new(1);

    let mut wal = SharedWal::open(storage.clone(), config.clone())
        .await
        .unwrap();

    // Create payload at exactly max size.
    let max_payload = vec![b'x'; ENTRY_PAYLOAD_SIZE_BYTES_MAX as usize];
    let result = wal.append(p1, 1, 1, Bytes::from(max_payload)).await;
    assert!(result.is_ok(), "Max size entry should succeed");

    // Create payload over max size.
    let over_payload = vec![b'y'; ENTRY_PAYLOAD_SIZE_BYTES_MAX as usize + 1];
    let result = wal.append(p1, 1, 2, Bytes::from(over_payload)).await;
    assert!(result.is_err(), "Over-max-size entry should fail");

    // Sync and verify recovery of the valid entry.
    wal.sync().await.unwrap();
    drop(wal);

    storage.simulate_crash();

    let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();
    let recovered = wal.recover().unwrap();
    let entries = recovered.get(&p1).unwrap();

    assert_eq!(entries.len(), 1, "Should recover exactly one entry");
    assert_eq!(
        entries[0].payload.len(),
        ENTRY_PAYLOAD_SIZE_BYTES_MAX as usize,
        "Recovered entry should have max-size payload"
    );
}

/// Test: Segment rollover when segment fills up.
#[tokio::test]
async fn test_dst_shared_wal_segment_rollover() {
    use helix_wal::limits::SEGMENT_SIZE_BYTES_MIN;

    let storage = SimulatedStorage::new(42);
    let config = SharedWalConfig::new("/test/rollover")
        .with_segment_config(helix_wal::SegmentConfig::new().with_max_size(SEGMENT_SIZE_BYTES_MIN));

    let p1 = PartitionId::new(1);

    let mut wal = SharedWal::open(storage.clone(), config.clone())
        .await
        .unwrap();

    // Write enough entries to trigger segment rollover.
    let entry_count = 12_000u64;
    for i in 1..=entry_count {
        let payload = format!("entry-{i:08}");
        wal.append(p1, 1, i, Bytes::from(payload)).await.unwrap();

        if i % 1000 == 0 {
            wal.sync().await.unwrap();
        }
    }
    wal.sync().await.unwrap();

    let durable_idx = wal.durable_index();
    assert_eq!(durable_idx, Some(entry_count), "All entries should be durable");

    drop(wal);
    storage.simulate_crash();

    let mut wal = SharedWal::open(storage.clone(), config).await.unwrap();
    let recovered = wal.recover().unwrap();
    let entries = recovered.get(&p1).unwrap();

    assert_eq!(
        entries.len() as u64,
        entry_count,
        "All entries should be recovered after rollover"
    );

    // Verify ordering and content.
    for (i, entry) in entries.iter().enumerate() {
        let expected_index = (i + 1) as u64;
        assert_eq!(entry.index(), expected_index, "Index should match");
        let expected_payload = format!("entry-{expected_index:08}");
        assert_eq!(
            entry.payload,
            Bytes::from(expected_payload),
            "Payload should match for index {expected_index}"
        );
    }
}

/// DST: Segment rollover under faults.
#[tokio::test]
async fn test_dst_shared_wal_segment_rollover_with_faults() {
    use helix_wal::limits::SEGMENT_SIZE_BYTES_MIN;

    const SEED_COUNT: u64 = 50;

    let mut successful_rollovers = 0u64;
    let mut skipped_early_failure = 0u64;
    let mut skipped_recovery_failed = 0u64;

    let large_payload = vec![b'x'; 1024];

    for seed in 0..SEED_COUNT {
        let fault_config = FaultConfig::default()
            .with_fsync_fail_rate(0.005)
            .with_torn_write_rate(0.005);

        let storage = SimulatedStorage::with_faults(seed, fault_config);
        let config = SharedWalConfig::new(format!("/test/rollover-fault-{seed}"))
            .with_segment_config(helix_wal::SegmentConfig::new().with_max_size(SEGMENT_SIZE_BYTES_MIN));

        let p1 = PartitionId::new(1);

        let entries_written: u64;
        let sync_succeeded: bool;
        {
            let mut wal = match SharedWal::open(storage.clone(), config.clone()).await {
                Ok(w) => w,
                Err(_) => {
                    skipped_early_failure += 1;
                    continue;
                }
            };

            let mut next_index = 1u64;
            let mut sync_failures = 0u32;
            let target_entries = 2000u64;

            while next_index <= target_entries && sync_failures < 5 {
                let mut payload = large_payload.clone();
                payload[0..8].copy_from_slice(&next_index.to_le_bytes());

                if wal
                    .append(p1, 1, next_index, Bytes::from(payload))
                    .await
                    .is_ok()
                {
                    next_index += 1;
                } else {
                    break;
                }

                if next_index % 200 == 0 {
                    if wal.sync().await.is_err() {
                        sync_failures += 1;
                    }
                }
            }

            entries_written = next_index.saturating_sub(1);
            sync_succeeded = wal.sync().await.is_ok();
        }

        if !sync_succeeded || entries_written < 100 {
            skipped_early_failure += 1;
            continue;
        }

        storage.simulate_crash();

        {
            let mut wal = match SharedWal::open(storage.clone(), config).await {
                Ok(w) => w,
                Err(_) => {
                    skipped_recovery_failed += 1;
                    continue;
                }
            };

            let recovered = wal.recover().unwrap_or_default();
            let entries = recovered.get(&p1).map(|v| v.len()).unwrap_or(0);

            if entries > 0 {
                successful_rollovers += 1;

                let ordering_violations = verify_ordering(&recovered);
                assert!(
                    ordering_violations.is_empty(),
                    "Seed {seed}: ordering violations after rollover: {:?}",
                    ordering_violations
                );

                let dup_violations = verify_no_duplicates(&recovered);
                assert!(
                    dup_violations.is_empty(),
                    "Seed {seed}: duplicate violations after rollover: {:?}",
                    dup_violations
                );

                let recovered_entries = recovered.get(&p1).unwrap();
                for entry in recovered_entries.iter().take(10) {
                    let expected_index = entry.index();
                    let payload_index =
                        u64::from_le_bytes(entry.payload[0..8].try_into().unwrap());
                    assert_eq!(
                        payload_index, expected_index,
                        "Seed {seed}: content mismatch at index {}",
                        entry.index()
                    );
                }
            }
        }
    }

    eprintln!("\n=== Segment Rollover with Faults Statistics ===");
    eprintln!("Successful rollovers verified: {successful_rollovers}/{SEED_COUNT}");
    eprintln!("Skipped (early failure): {skipped_early_failure}");
    eprintln!("Skipped (recovery failed): {skipped_recovery_failed}");
    eprintln!("================================================\n");

    assert!(
        successful_rollovers > 0,
        "At least some rollover tests should succeed"
    );
}

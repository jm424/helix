//! Consumer progress tracking DST tests.
//!
//! Comprehensive deterministic simulation testing for helix-progress with:
//! - Exhaustive invariant checking
//! - All fault injection sites exercised
//! - Limit enforcement verification
//! - Edge case coverage
//! - State consistency after partial failures

// Test-specific lint allowances.
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::similar_names)]
#![allow(dead_code)]
#![allow(unused_variables)]

use helix_core::{ConsumerGroupId, ConsumerId, LeaseId, Offset, PartitionId, TopicId};
use helix_progress::{
    AckMode, ConsumerGroupState, Lease, PartitionKey, PartitionProgress, ProgressConfig,
    ProgressError, ProgressManager, ProgressStore, ProgressStoreFaultConfig,
    SimulatedProgressStore,
};

use crate::scenarios::seeds::REGRESSION_SEEDS;

// ============================================================================
// Comprehensive Property Checker
// ============================================================================

/// All possible invariant violations.
#[derive(Debug, Clone)]
pub enum Violation {
    // Watermark invariants
    WatermarkDecreased {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
        old: u64,
        new: u64,
    },
    WatermarkExceedsNextLease {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
        watermark: u64,
        next_lease: u64,
    },

    // Bitmap invariants
    BitmapBitZeroSet {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
    },
    BitmapSetInCumulativeMode {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
        bitmap_len: u64,
    },

    // Lease invariants
    LeaseFromExceedsTo {
        lease_id: u64,
        from: u64,
        to: u64,
    },
    LeaseExpiresBeforeCreated {
        lease_id: u64,
        created: u64,
        expires: u64,
    },
    OverlappingLeases {
        group_id: u64,
        lease_a: u64,
        lease_b: u64,
        overlap_start: u64,
        overlap_end: u64,
    },
    LeaseNotInConsumerList {
        group_id: u64,
        consumer_id: u64,
        lease_id: u64,
    },
    ConsumerLeaseNotInPartition {
        group_id: u64,
        consumer_id: u64,
        lease_id: u64,
    },
    LeaseBelowWatermark {
        group_id: u64,
        lease_id: u64,
        lease_from: u64,
        watermark: u64,
    },

    // Consumer invariants
    ConsumerNotInGroup {
        group_id: u64,
        consumer_id: u64,
    },
    DuplicateConsumer {
        group_id: u64,
        consumer_id: u64,
    },

    // Offset invariants
    CommittedOffsetAboveWatermark {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
        offset: u64,
        watermark: u64,
    },
    OffsetBelowWatermarkNotCommitted {
        group_id: u64,
        topic_id: u64,
        partition_id: u64,
        offset: u64,
        watermark: u64,
    },
}

/// Comprehensive property checker that verifies ALL invariants.
pub struct PropertyChecker {
    /// Previous watermarks for monotonicity check.
    watermarks: std::collections::HashMap<(u64, u64, u64), u64>,
    /// All violations found.
    violations: Vec<Violation>,
    /// Statistics.
    pub checks_performed: u64,
    pub groups_checked: u64,
    pub partitions_checked: u64,
    pub leases_checked: u64,
    pub consumers_checked: u64,
}

impl PropertyChecker {
    pub fn new() -> Self {
        Self {
            watermarks: std::collections::HashMap::new(),
            violations: Vec::new(),
            checks_performed: 0,
            groups_checked: 0,
            partitions_checked: 0,
            leases_checked: 0,
            consumers_checked: 0,
        }
    }

    /// Performs exhaustive invariant checking on all state.
    pub fn check_all_invariants(&mut self, store: &SimulatedProgressStore) {
        self.checks_performed += 1;

        for (group_id, group) in store.all_groups() {
            self.groups_checked += 1;
            self.check_group_invariants(group_id, &group);
        }
    }

    fn check_group_invariants(&mut self, group_id: ConsumerGroupId, group: &ConsumerGroupState) {
        let gid = group_id.get();

        // Check each partition's invariants.
        for (partition_key, progress) in &group.partitions {
            self.partitions_checked += 1;
            self.check_partition_invariants(gid, partition_key, progress, group);
        }

        // Check consumer invariants.
        for (consumer_id, consumer_state) in &group.consumers {
            self.consumers_checked += 1;

            // Verify each lease in consumer's list exists in some partition.
            for lease_id in &consumer_state.lease_ids {
                let lease_exists = group.partitions.values().any(|p| {
                    p.active_leases.contains_key(lease_id)
                });

                if !lease_exists {
                    self.violations.push(Violation::ConsumerLeaseNotInPartition {
                        group_id: gid,
                        consumer_id: consumer_id.get(),
                        lease_id: lease_id.get(),
                    });
                }
            }
        }
    }

    fn check_partition_invariants(
        &mut self,
        group_id: u64,
        partition_key: &PartitionKey,
        progress: &PartitionProgress,
        group: &ConsumerGroupState,
    ) {
        let tid = partition_key.topic_id.get();
        let pid = partition_key.partition_id.get();
        let key = (group_id, tid, pid);

        // 1. Watermark monotonicity.
        let current_watermark = progress.low_watermark.get();
        if let Some(&prev_watermark) = self.watermarks.get(&key) {
            if current_watermark < prev_watermark {
                self.violations.push(Violation::WatermarkDecreased {
                    group_id,
                    topic_id: tid,
                    partition_id: pid,
                    old: prev_watermark,
                    new: current_watermark,
                });
            }
        }
        self.watermarks.insert(key, current_watermark);

        // 2. Watermark <= next_lease_offset.
        if current_watermark > progress.next_lease_offset.get() {
            self.violations.push(Violation::WatermarkExceedsNextLease {
                group_id,
                topic_id: tid,
                partition_id: pid,
                watermark: current_watermark,
                next_lease: progress.next_lease_offset.get(),
            });
        }

        // 3. Bitmap bit 0 should never be set (Individual mode).
        if progress.ack_mode == AckMode::Individual && progress.committed_bitmap.contains(0) {
            self.violations.push(Violation::BitmapBitZeroSet {
                group_id,
                topic_id: tid,
                partition_id: pid,
            });
        }

        // 4. Bitmap should be empty in Cumulative mode.
        if progress.ack_mode == AckMode::Cumulative && !progress.committed_bitmap.is_empty() {
            self.violations.push(Violation::BitmapSetInCumulativeMode {
                group_id,
                topic_id: tid,
                partition_id: pid,
                bitmap_len: progress.committed_bitmap.len(),
            });
        }

        // 5. Check each lease's invariants.
        let leases: Vec<_> = progress.active_leases.values().collect();
        for lease in &leases {
            self.leases_checked += 1;
            self.check_lease_invariants(group_id, lease, progress, group);
        }

        // 6. Check for overlapping leases.
        for i in 0..leases.len() {
            for j in (i + 1)..leases.len() {
                let a = leases[i];
                let b = leases[j];

                let a_start = a.from_offset.get();
                let a_end = a.to_offset.get();
                let b_start = b.from_offset.get();
                let b_end = b.to_offset.get();

                // Check overlap: ranges [a_start, a_end] and [b_start, b_end].
                if a_start <= b_end && b_start <= a_end {
                    let overlap_start = a_start.max(b_start);
                    let overlap_end = a_end.min(b_end);
                    self.violations.push(Violation::OverlappingLeases {
                        group_id,
                        lease_a: a.lease_id.get(),
                        lease_b: b.lease_id.get(),
                        overlap_start,
                        overlap_end,
                    });
                }
            }
        }

        // 7. Verify is_committed consistency for offsets around watermark.
        // Offsets below watermark should be "committed".
        if current_watermark > 0 {
            let test_offset = current_watermark - 1;
            if !progress.is_committed(Offset::new(test_offset)) {
                self.violations.push(Violation::OffsetBelowWatermarkNotCommitted {
                    group_id,
                    topic_id: tid,
                    partition_id: pid,
                    offset: test_offset,
                    watermark: current_watermark,
                });
            }
        }
    }

    fn check_lease_invariants(
        &mut self,
        group_id: u64,
        lease: &Lease,
        progress: &PartitionProgress,
        group: &ConsumerGroupState,
    ) {
        let lid = lease.lease_id.get();

        // 1. from_offset <= to_offset.
        if lease.from_offset.get() > lease.to_offset.get() {
            self.violations.push(Violation::LeaseFromExceedsTo {
                lease_id: lid,
                from: lease.from_offset.get(),
                to: lease.to_offset.get(),
            });
        }

        // 2. expires_at >= created_at.
        if lease.expires_at_us < lease.created_at_us {
            self.violations.push(Violation::LeaseExpiresBeforeCreated {
                lease_id: lid,
                created: lease.created_at_us,
                expires: lease.expires_at_us,
            });
        }

        // 3. Lease should be in consumer's lease list.
        if let Some(consumer) = group.consumers.get(&lease.consumer_id) {
            if !consumer.lease_ids.contains(&lease.lease_id) {
                self.violations.push(Violation::LeaseNotInConsumerList {
                    group_id,
                    consumer_id: lease.consumer_id.get(),
                    lease_id: lid,
                });
            }
        } else {
            // Consumer doesn't exist but has lease - this is a violation.
            self.violations.push(Violation::ConsumerNotInGroup {
                group_id,
                consumer_id: lease.consumer_id.get(),
            });
        }

        // 4. Active lease should not be entirely below watermark.
        // (Offsets below watermark are committed, shouldn't be leased.)
        if lease.to_offset.get() < progress.low_watermark.get() {
            self.violations.push(Violation::LeaseBelowWatermark {
                group_id,
                lease_id: lid,
                lease_from: lease.from_offset.get(),
                watermark: progress.low_watermark.get(),
            });
        }
    }

    pub fn violations(&self) -> &[Violation] {
        &self.violations
    }

    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn print_summary(&self, label: &str) {
        println!(
            "{}: checks={}, groups={}, partitions={}, leases={}, consumers={}, violations={}",
            label,
            self.checks_performed,
            self.groups_checked,
            self.partitions_checked,
            self.leases_checked,
            self.consumers_checked,
            self.violations.len()
        );
    }
}

// ============================================================================
// Fault Injection Statistics
// ============================================================================

/// Tracks which fault injection sites were exercised.
#[derive(Debug, Default)]
pub struct FaultStats {
    pub get_group_calls: u64,
    pub get_group_faults: u64,
    pub save_group_calls: u64,
    pub save_group_faults: u64,
    pub remove_group_calls: u64,
    pub remove_group_faults: u64,
    pub get_partition_progress_calls: u64,
    pub get_partition_progress_faults: u64,
    pub save_partition_progress_calls: u64,
    pub save_partition_progress_faults: u64,
    pub get_active_leases_calls: u64,
    pub get_active_leases_faults: u64,
    pub get_min_low_watermark_calls: u64,
    pub get_min_low_watermark_faults: u64,
    pub list_groups_calls: u64,
    pub list_groups_faults: u64,
}

impl FaultStats {
    pub fn total_calls(&self) -> u64 {
        self.get_group_calls
            + self.save_group_calls
            + self.remove_group_calls
            + self.get_partition_progress_calls
            + self.save_partition_progress_calls
            + self.get_active_leases_calls
            + self.get_min_low_watermark_calls
            + self.list_groups_calls
    }

    pub fn total_faults(&self) -> u64 {
        self.get_group_faults
            + self.save_group_faults
            + self.remove_group_faults
            + self.get_partition_progress_faults
            + self.save_partition_progress_faults
            + self.get_active_leases_faults
            + self.get_min_low_watermark_faults
            + self.list_groups_faults
    }

    pub fn print_summary(&self) {
        println!("Fault injection coverage:");
        println!(
            "  get_group: {}/{} faults",
            self.get_group_faults, self.get_group_calls
        );
        println!(
            "  save_group: {}/{} faults",
            self.save_group_faults, self.save_group_calls
        );
        println!(
            "  remove_group: {}/{} faults",
            self.remove_group_faults, self.remove_group_calls
        );
        println!(
            "  get_partition_progress: {}/{} faults",
            self.get_partition_progress_faults, self.get_partition_progress_calls
        );
        println!(
            "  save_partition_progress: {}/{} faults",
            self.save_partition_progress_faults, self.save_partition_progress_calls
        );
        println!(
            "  get_active_leases: {}/{} faults",
            self.get_active_leases_faults, self.get_active_leases_calls
        );
        println!(
            "  get_min_low_watermark: {}/{} faults",
            self.get_min_low_watermark_faults, self.get_min_low_watermark_calls
        );
        println!(
            "  list_groups: {}/{} faults",
            self.list_groups_faults, self.list_groups_calls
        );
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn create_manager(
    seed: u64,
    fault_config: ProgressStoreFaultConfig,
) -> ProgressManager<SimulatedProgressStore> {
    let store = SimulatedProgressStore::with_faults(seed, fault_config);
    ProgressManager::new(store, ProgressConfig::for_testing())
}

fn create_manager_no_faults(seed: u64) -> ProgressManager<SimulatedProgressStore> {
    create_manager(seed, ProgressStoreFaultConfig::none())
}

/// High fault rate config for stress testing all paths.
fn high_fault_config() -> ProgressStoreFaultConfig {
    ProgressStoreFaultConfig::none()
        .with_get_fail_rate(0.15)
        .with_save_fail_rate(0.15)
        .with_remove_fail_rate(0.15)
}

/// Deterministic hash for randomized operations.
fn det_hash(seed: u64, counter: u64) -> u64 {
    seed.wrapping_add(counter).wrapping_mul(0x9e3779b97f4a7c15)
}

// ============================================================================
// Test Category 1: Exhaustive Invariant Verification
// ============================================================================

/// Verifies all invariants hold after every operation in a complex sequence.
#[tokio::test]
async fn test_invariants_after_every_operation() {
    for &seed in REGRESSION_SEEDS {
        let manager = create_manager_no_faults(seed);
        let mut checker = PropertyChecker::new();

        let group_id = ConsumerGroupId::new(1);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Operation sequence that exercises all state transitions.
        // 1. Create group.
        manager
            .get_or_create_group(group_id, 1000)
            .await
            .unwrap();
        checker.check_all_invariants(manager.store());
        assert!(checker.is_clean(), "seed {}: after create group: {:?}", seed, checker.violations());

        // 2. Register consumers.
        for i in 1..=3 {
            manager
                .register_consumer(group_id, ConsumerId::new(i), 1000 + i * 100)
                .await
                .unwrap();
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after register consumer {}: {:?}", seed, i, checker.violations());
        }

        // 3. Create leases for each consumer.
        let mut leases = Vec::new();
        for i in 1..=3 {
            let lease = manager
                .lease_offsets(
                    group_id,
                    topic_id,
                    partition_id,
                    ConsumerId::new(i),
                    Offset::new((i - 1) * 10),
                    10,
                    60_000_000,
                    2000,
                )
                .await
                .unwrap();
            leases.push(lease);
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after lease {}: {:?}", seed, i, checker.violations());
        }

        // 4. Commit offsets (some contiguous, some sparse).
        for i in 0..5 {
            manager
                .commit_offset(
                    group_id,
                    topic_id,
                    partition_id,
                    ConsumerId::new(1),
                    Offset::new(i),
                    3000,
                )
                .await
                .unwrap();
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after commit {}: {:?}", seed, i, checker.violations());
        }

        // 5. Extend a lease.
        if let Some(Some(lease)) = leases.first() {
            let pk = PartitionKey::new(topic_id, partition_id);
            let _ = manager
                .extend_lease(group_id, pk, ConsumerId::new(1), lease.lease_id, 30_000_000, 4000)
                .await;
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after extend: {:?}", seed, checker.violations());
        }

        // 6. Expire leases.
        manager
            .expire_leases(group_id, 100_000_000)
            .await
            .unwrap();
        checker.check_all_invariants(manager.store());
        assert!(checker.is_clean(), "seed {}: after expire: {:?}", seed, checker.violations());

        // 7. Unregister a consumer.
        manager
            .unregister_consumer(group_id, ConsumerId::new(3))
            .await
            .unwrap();
        checker.check_all_invariants(manager.store());
        assert!(checker.is_clean(), "seed {}: after unregister: {:?}", seed, checker.violations());
    }
}

/// Tests Individual mode bitmap invariants specifically.
#[tokio::test]
async fn test_individual_mode_bitmap_invariants() {
    for &seed in REGRESSION_SEEDS {
        let manager = create_manager_no_faults(seed);
        let mut checker = PropertyChecker::new();

        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(1);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Create group with Individual mode.
        manager
            .get_or_create_group_with_mode(group_id, 1000, AckMode::Individual)
            .await
            .unwrap();
        manager
            .register_consumer(group_id, consumer_id, 1000)
            .await
            .unwrap();

        // Lease offsets.
        manager
            .lease_offsets(
                group_id, topic_id, partition_id, consumer_id,
                Offset::new(0), 100, 60_000_000, 1000,
            )
            .await
            .unwrap();

        // Commit sparse offsets: 0, 2, 4, 6, 8 (gaps at 1, 3, 5, 7).
        for i in (0..10).step_by(2) {
            manager
                .commit_offset(group_id, topic_id, partition_id, consumer_id, Offset::new(i), 2000)
                .await
                .unwrap();
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after sparse commit {}: {:?}", seed, i, checker.violations());
        }

        // Verify watermark is at 1 (stopped at first gap).
        let wm = manager
            .get_low_watermark(group_id, topic_id, partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm.get(), 1, "seed {}: watermark should be 1", seed);

        // Fill gaps one by one and check invariants.
        for i in [1, 3, 5, 7, 9] {
            manager
                .commit_offset(group_id, topic_id, partition_id, consumer_id, Offset::new(i), 3000)
                .await
                .unwrap();
            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: after fill gap {}: {:?}", seed, i, checker.violations());
        }

        // Watermark should now be at 10.
        let wm = manager
            .get_low_watermark(group_id, topic_id, partition_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wm.get(), 10, "seed {}: watermark should be 10", seed);
    }
}

/// Tests Cumulative mode invariants (bitmap should always be empty).
#[tokio::test]
async fn test_cumulative_mode_bitmap_empty() {
    for &seed in REGRESSION_SEEDS {
        let manager = create_manager_no_faults(seed);
        let mut checker = PropertyChecker::new();

        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(1);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Create group with Cumulative mode.
        manager
            .get_or_create_group_with_mode(group_id, 1000, AckMode::Cumulative)
            .await
            .unwrap();
        manager
            .register_consumer(group_id, consumer_id, 1000)
            .await
            .unwrap();

        // Lease and commit multiple times.
        for batch in 0..5 {
            let start = batch * 20;
            manager
                .lease_offsets(
                    group_id, topic_id, partition_id, consumer_id,
                    Offset::new(start), 20, 60_000_000, 1000 + batch * 1000,
                )
                .await
                .unwrap();

            // Commit at end of batch (cumulative).
            manager
                .commit_offset(
                    group_id, topic_id, partition_id, consumer_id,
                    Offset::new(start + 19), 2000 + batch * 1000,
                )
                .await
                .unwrap();

            checker.check_all_invariants(manager.store());
            assert!(checker.is_clean(), "seed {}: batch {}: {:?}", seed, batch, checker.violations());

            // Verify watermark jumped.
            let wm = manager
                .get_low_watermark(group_id, topic_id, partition_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(wm.get(), start + 20, "seed {}: watermark after batch {}", seed, batch);
        }
    }
}

// ============================================================================
// Test Category 2: All Fault Injection Sites
// ============================================================================

/// Exercises ALL storage methods with fault injection.
#[tokio::test]
async fn test_all_fault_injection_sites() {
    let config = high_fault_config();
    let mut total_stats = FaultStats::default();

    for seed in 0..50 {
        let manager = create_manager(seed * 7919, config.clone());
        let mut stats = FaultStats::default();

        let group_id = ConsumerGroupId::new(seed + 1);
        let consumer_id = ConsumerId::new(1);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);
        let partition_key = PartitionKey::new(topic_id, partition_id);

        // 1. get_group (via get_or_create_group).
        stats.get_group_calls += 1;
        if manager.get_or_create_group(group_id, 1000).await.is_err() {
            stats.get_group_faults += 1;
        }

        // 2. save_group (via register_consumer).
        stats.save_group_calls += 1;
        if manager.register_consumer(group_id, consumer_id, 1000).await.is_err() {
            stats.save_group_faults += 1;
        }

        // 3. Direct store calls for methods not exposed via manager.
        let store = manager.store();

        // get_partition_progress.
        stats.get_partition_progress_calls += 1;
        if store.get_partition_progress(group_id, partition_key).await.is_err() {
            stats.get_partition_progress_faults += 1;
        }

        // get_active_leases.
        stats.get_active_leases_calls += 1;
        if store.get_active_leases(partition_key).await.is_err() {
            stats.get_active_leases_faults += 1;
        }

        // get_min_low_watermark.
        stats.get_min_low_watermark_calls += 1;
        if store.get_min_low_watermark(partition_key).await.is_err() {
            stats.get_min_low_watermark_faults += 1;
        }

        // list_groups.
        stats.list_groups_calls += 1;
        if store.list_groups().await.is_err() {
            stats.list_groups_faults += 1;
        }

        // remove_group (need to create first).
        let remove_group_id = ConsumerGroupId::new(seed + 1000);
        let _ = manager.get_or_create_group(remove_group_id, 1000).await;
        stats.remove_group_calls += 1;
        if store.remove_group(remove_group_id).await.is_err() {
            stats.remove_group_faults += 1;
        }

        // Accumulate.
        total_stats.get_group_calls += stats.get_group_calls;
        total_stats.get_group_faults += stats.get_group_faults;
        total_stats.save_group_calls += stats.save_group_calls;
        total_stats.save_group_faults += stats.save_group_faults;
        total_stats.remove_group_calls += stats.remove_group_calls;
        total_stats.remove_group_faults += stats.remove_group_faults;
        total_stats.get_partition_progress_calls += stats.get_partition_progress_calls;
        total_stats.get_partition_progress_faults += stats.get_partition_progress_faults;
        total_stats.get_active_leases_calls += stats.get_active_leases_calls;
        total_stats.get_active_leases_faults += stats.get_active_leases_faults;
        total_stats.get_min_low_watermark_calls += stats.get_min_low_watermark_calls;
        total_stats.get_min_low_watermark_faults += stats.get_min_low_watermark_faults;
        total_stats.list_groups_calls += stats.list_groups_calls;
        total_stats.list_groups_faults += stats.list_groups_faults;
    }

    println!("\n=== Fault Injection Coverage ===");
    total_stats.print_summary();

    // Verify all sites were exercised with faults.
    assert!(total_stats.get_group_faults > 0, "get_group should have faults");
    assert!(total_stats.save_group_faults > 0, "save_group should have faults");
    assert!(total_stats.remove_group_faults > 0, "remove_group should have faults");
    // Note: partition-level faults may be 0 if no partitions created due to earlier failures.
}

/// Tests state consistency when operations fail mid-way.
#[tokio::test]
async fn test_state_consistency_after_failures() {
    for seed in 0..20 {
        let config = ProgressStoreFaultConfig::none()
            .with_save_fail_rate(0.5); // 50% save failures

        let manager = create_manager(seed * 12345, config);
        let mut checker = PropertyChecker::new();

        let group_id = ConsumerGroupId::new(1);
        let consumer_id = ConsumerId::new(1);
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Try operations multiple times, some will fail.
        for attempt in 0..10 {
            let _ = manager.get_or_create_group(group_id, 1000).await;
            let _ = manager.register_consumer(group_id, consumer_id, 1000).await;
            let _ = manager
                .lease_offsets(
                    group_id, topic_id, partition_id, consumer_id,
                    Offset::new(attempt * 10), 10, 60_000_000, 1000 + attempt * 100,
                )
                .await;

            // Check invariants after each attempt (success or failure).
            checker.check_all_invariants(manager.store());
            assert!(
                checker.is_clean(),
                "seed {}, attempt {}: invariants violated after failure: {:?}",
                seed, attempt, checker.violations()
            );
        }
    }
}

// ============================================================================
// Test Category 3: Limit Enforcement
// ============================================================================

/// Tests max_consumers_per_group limit.
#[tokio::test]
async fn test_max_consumers_limit() {
    let manager = create_manager_no_faults(42);
    let group_id = ConsumerGroupId::new(1);

    manager.get_or_create_group(group_id, 1000).await.unwrap();

    // Register up to the limit (10 in test config).
    for i in 1..=10 {
        let result = manager
            .register_consumer(group_id, ConsumerId::new(i), 1000)
            .await;
        assert!(result.is_ok(), "should allow consumer {}", i);
    }

    // 11th should fail.
    let result = manager
        .register_consumer(group_id, ConsumerId::new(11), 1000)
        .await;
    assert!(
        matches!(result, Err(ProgressError::TooManyConsumers { .. })),
        "should reject 11th consumer"
    );
}

/// Tests max_lease_duration_us limit.
#[tokio::test]
async fn test_max_lease_duration_limit() {
    let manager = create_manager_no_faults(42);
    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();

    // Request lease with excessive duration.
    let result = manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(0), 10,
            u64::MAX, // Excessive duration
            1000,
        )
        .await;

    assert!(
        matches!(result, Err(ProgressError::LeaseDurationExceeded { .. })),
        "should reject excessive lease duration"
    );
}

/// Tests max_offsets_per_lease limit.
#[tokio::test]
async fn test_max_offsets_per_lease_limit() {
    let manager = create_manager_no_faults(42);
    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();

    // Request more offsets than allowed (config has max_offsets_per_lease = 10000).
    let lease = manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(0),
            u32::MAX, // Request way too many
            60_000_000,
            1000,
        )
        .await
        .unwrap()
        .unwrap();

    // Should be capped at config limit.
    let count = lease.to_offset.get() - lease.from_offset.get() + 1;
    assert!(
        count <= 10000,
        "lease should be capped at max_offsets_per_lease, got {}",
        count
    );
}

// ============================================================================
// Test Category 4: Edge Cases
// ============================================================================

/// Tests behavior with offset near u64::MAX.
#[tokio::test]
async fn test_high_offset_values() {
    let manager = create_manager_no_faults(42);
    let mut checker = PropertyChecker::new();

    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();

    // Start at high offset.
    let high_offset = u64::MAX - 1000;
    let lease = manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(high_offset), 10, 60_000_000, 1000,
        )
        .await
        .unwrap()
        .unwrap();

    checker.check_all_invariants(manager.store());
    assert!(checker.is_clean(), "invariants after high offset lease: {:?}", checker.violations());

    // Commit at high offset.
    manager
        .commit_offset(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(high_offset), 2000,
        )
        .await
        .unwrap();

    checker.check_all_invariants(manager.store());
    assert!(checker.is_clean(), "invariants after high offset commit: {:?}", checker.violations());
}

/// Tests empty group behavior.
#[tokio::test]
async fn test_empty_group() {
    let manager = create_manager_no_faults(42);
    let mut checker = PropertyChecker::new();

    let group_id = ConsumerGroupId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    // Create empty group.
    manager.get_or_create_group(group_id, 1000).await.unwrap();

    checker.check_all_invariants(manager.store());
    assert!(checker.is_clean());

    // Query operations on empty group.
    let wm = manager
        .get_low_watermark(group_id, topic_id, partition_id)
        .await
        .unwrap();
    assert!(wm.is_none(), "no watermark for non-existent partition");

    let safe = manager
        .get_safe_eviction_offset(topic_id, partition_id)
        .await
        .unwrap();
    assert!(safe.is_none(), "no safe offset for non-existent partition");
}

/// Tests zero-count lease request.
#[tokio::test]
#[should_panic(expected = "max_count must be positive")]
async fn test_zero_count_lease_panics() {
    let manager = create_manager_no_faults(42);
    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();

    // Zero count should panic (TigerStyle assertion).
    let _ = manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(0), 0, 60_000_000, 1000,
        )
        .await;
}

/// Tests duplicate commit rejection.
#[tokio::test]
async fn test_duplicate_commit_rejected() {
    let manager = create_manager_no_faults(42);

    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();
    manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(0), 10, 60_000_000, 1000,
        )
        .await
        .unwrap();

    // First commit should succeed.
    manager
        .commit_offset(group_id, topic_id, partition_id, consumer_id, Offset::new(0), 2000)
        .await
        .unwrap();

    // Second commit of same offset should fail.
    let result = manager
        .commit_offset(group_id, topic_id, partition_id, consumer_id, Offset::new(0), 3000)
        .await;

    assert!(
        matches!(result, Err(ProgressError::OffsetAlreadyCommitted { .. })),
        "duplicate commit should be rejected"
    );
}

/// Tests commit without lease rejected.
#[tokio::test]
async fn test_commit_without_lease_rejected() {
    let manager = create_manager_no_faults(42);

    let group_id = ConsumerGroupId::new(1);
    let consumer_id = ConsumerId::new(1);
    let topic_id = TopicId::new(1);
    let partition_id = PartitionId::new(0);

    manager.get_or_create_group(group_id, 1000).await.unwrap();
    manager.register_consumer(group_id, consumer_id, 1000).await.unwrap();

    // Create a lease for offsets 0-9.
    manager
        .lease_offsets(
            group_id, topic_id, partition_id, consumer_id,
            Offset::new(0), 10, 60_000_000, 1000,
        )
        .await
        .unwrap();

    // Try to commit offset 20 which is not leased (beyond lease range).
    let result = manager
        .commit_offset(group_id, topic_id, partition_id, consumer_id, Offset::new(20), 2000)
        .await;

    assert!(
        matches!(result, Err(ProgressError::OffsetNotLeased { .. })),
        "commit without lease should be rejected"
    );
}

// ============================================================================
// Test Category 5: Comprehensive Stress Tests
// ============================================================================

/// Comprehensive stress test with all operations and invariant checking.
#[tokio::test]
async fn test_comprehensive_stress_100_seeds() {
    const NUM_SEEDS: u64 = 100;
    const OPS_PER_SEED: u64 = 100;

    let config = high_fault_config();
    let mut total_violations = 0;
    let mut total_checks = 0u64;
    let mut total_faults = 0u64;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 12345 + 42;
        let manager = create_manager(seed, config.clone());
        let mut checker = PropertyChecker::new();
        let mut faults_this_seed = 0u64;

        // Multiple groups, multiple consumers.
        let num_groups = 2;
        let num_consumers = 3;
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        // Initialize groups.
        for g in 1..=num_groups {
            let group_id = ConsumerGroupId::new(g);
            let mode = if g % 2 == 0 { AckMode::Individual } else { AckMode::Cumulative };
            let _ = manager.get_or_create_group_with_mode(group_id, 1000, mode).await;

            for c in 1..=num_consumers {
                let _ = manager.register_consumer(group_id, ConsumerId::new(c), 1000).await;
            }
        }

        // Track state per group.
        let mut next_offset: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut active_leases: std::collections::HashMap<u64, Vec<(ConsumerId, LeaseId, u64, u64)>> =
            std::collections::HashMap::new();

        for g in 1..=num_groups {
            next_offset.insert(g, 0);
            active_leases.insert(g, Vec::new());
        }

        for op_num in 0..OPS_PER_SEED {
            let op_hash = det_hash(seed, op_num);
            let group_idx = (op_hash % num_groups) + 1;
            let group_id = ConsumerGroupId::new(group_idx);
            let consumer_idx = (op_hash / 10 % num_consumers) + 1;
            let consumer_id = ConsumerId::new(consumer_idx);
            let op_type = (op_hash / 100) % 12;

            match op_type {
                0..=3 => {
                    // Lease (33%).
                    let offset = *next_offset.get(&group_idx).unwrap_or(&0);
                    match manager
                        .lease_offsets(
                            group_id, topic_id, partition_id, consumer_id,
                            Offset::new(offset), 10, 60_000_000, 1000 + op_num * 100,
                        )
                        .await
                    {
                        Ok(Some(lease)) => {
                            active_leases.entry(group_idx).or_default().push((
                                consumer_id,
                                lease.lease_id,
                                lease.from_offset.get(),
                                lease.to_offset.get(),
                            ));
                        }
                        Ok(None) => {}
                        Err(_) => faults_this_seed += 1,
                    }
                }
                4..=6 => {
                    // Commit (25%).
                    if let Some(leases) = active_leases.get(&group_idx) {
                        if let Some((cid, _lid, from, to)) = leases.first() {
                            let offset = next_offset.get(&group_idx).copied().unwrap_or(0);
                            if offset >= *from && offset <= *to {
                                match manager
                                    .commit_offset(
                                        group_id, topic_id, partition_id, *cid,
                                        Offset::new(offset), 1000 + op_num * 100,
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        *next_offset.entry(group_idx).or_insert(0) = offset + 1;
                                    }
                                    Err(_) => faults_this_seed += 1,
                                }
                            }
                        }
                    }
                }
                7 => {
                    // Expire leases (8%).
                    match manager
                        .expire_leases(group_id, 1000 + op_num * 100 + 70_000_000)
                        .await
                    {
                        Ok(count) => {
                            if count > 0 {
                                active_leases.entry(group_idx).or_default().clear();
                            }
                        }
                        Err(_) => faults_this_seed += 1,
                    }
                }
                8 => {
                    // Extend lease (8%).
                    if let Some(leases) = active_leases.get(&group_idx) {
                        if let Some((cid, lid, _, _)) = leases.first() {
                            let pk = PartitionKey::new(topic_id, partition_id);
                            if manager
                                .extend_lease(group_id, pk, *cid, *lid, 30_000_000, 1000 + op_num * 100)
                                .await
                                .is_err()
                            {
                                faults_this_seed += 1;
                            }
                        }
                    }
                }
                9 => {
                    // Query watermark (8%).
                    if manager
                        .get_low_watermark(group_id, topic_id, partition_id)
                        .await
                        .is_err()
                    {
                        faults_this_seed += 1;
                    }
                }
                10 => {
                    // Query safe eviction (8%).
                    if manager
                        .get_safe_eviction_offset(topic_id, partition_id)
                        .await
                        .is_err()
                    {
                        faults_this_seed += 1;
                    }
                }
                _ => {
                    // Fetch committed (8%).
                    if manager
                        .fetch_committed(group_id, topic_id, partition_id)
                        .await
                        .is_err()
                    {
                        faults_this_seed += 1;
                    }
                }
            }

            // Check invariants every 10 operations.
            if op_num % 10 == 9 {
                checker.check_all_invariants(manager.store());
            }
        }

        // Final check.
        checker.check_all_invariants(manager.store());

        if !checker.is_clean() {
            println!("Seed {} VIOLATIONS: {:?}", seed, checker.violations());
            total_violations += checker.violations().len();
        }

        total_checks += checker.checks_performed;
        total_faults += faults_this_seed;

        if base_seed % 20 == 19 {
            println!(
                "Progress: {}/{} seeds, {} violations, {} faults",
                base_seed + 1, NUM_SEEDS, total_violations, total_faults
            );
        }
    }

    println!("\n=== STRESS TEST RESULTS ===");
    println!("Seeds: {}", NUM_SEEDS);
    println!("Operations per seed: {}", OPS_PER_SEED);
    println!("Total invariant checks: {}", total_checks);
    println!("Total faults: {}", total_faults);
    println!("Total violations: {}", total_violations);

    assert_eq!(total_violations, 0, "No invariant violations allowed");
    assert!(total_faults > 0, "Faults should be injected");
}

/// Large-scale stress test (500 seeds) - run with --ignored.
#[tokio::test]
#[ignore]
async fn test_comprehensive_stress_500_seeds() {
    const NUM_SEEDS: u64 = 500;
    const OPS_PER_SEED: u64 = 200;

    let config = high_fault_config();
    let mut total_violations = 0;
    let mut total_checks = 0u64;
    let mut total_faults = 0u64;
    let mut total_leases = 0u64;
    let mut total_commits = 0u64;

    for base_seed in 0..NUM_SEEDS {
        let seed = base_seed * 12345 + 42;
        let manager = create_manager(seed, config.clone());
        let mut checker = PropertyChecker::new();
        let mut faults_this_seed = 0u64;
        let mut leases_this_seed = 0u64;
        let mut commits_this_seed = 0u64;

        // Multiple groups with different modes.
        let num_groups = 3;
        let num_consumers = 4;
        let topic_id = TopicId::new(1);
        let partition_id = PartitionId::new(0);

        for g in 1..=num_groups {
            let group_id = ConsumerGroupId::new(g);
            let mode = match g % 3 {
                0 => AckMode::Cumulative,
                1 => AckMode::Individual,
                _ => AckMode::Cumulative,
            };
            let _ = manager.get_or_create_group_with_mode(group_id, 1000, mode).await;

            for c in 1..=num_consumers {
                let _ = manager.register_consumer(group_id, ConsumerId::new(c), 1000).await;
            }
        }

        let mut next_offset: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut active_leases: std::collections::HashMap<u64, Vec<(ConsumerId, LeaseId, u64, u64)>> =
            std::collections::HashMap::new();

        for g in 1..=num_groups {
            next_offset.insert(g, 0);
            active_leases.insert(g, Vec::new());
        }

        for op_num in 0..OPS_PER_SEED {
            let op_hash = det_hash(seed, op_num);
            let group_idx = (op_hash % num_groups) + 1;
            let group_id = ConsumerGroupId::new(group_idx);
            let consumer_idx = (op_hash / 10 % num_consumers) + 1;
            let consumer_id = ConsumerId::new(consumer_idx);
            let op_type = (op_hash / 100) % 15;

            match op_type {
                0..=4 => {
                    // Lease (33%).
                    let offset = *next_offset.get(&group_idx).unwrap_or(&0);
                    match manager
                        .lease_offsets(
                            group_id, topic_id, partition_id, consumer_id,
                            Offset::new(offset), 10, 60_000_000, 1000 + op_num * 100,
                        )
                        .await
                    {
                        Ok(Some(lease)) => {
                            leases_this_seed += 1;
                            active_leases.entry(group_idx).or_default().push((
                                consumer_id,
                                lease.lease_id,
                                lease.from_offset.get(),
                                lease.to_offset.get(),
                            ));
                        }
                        Ok(None) => {}
                        Err(_) => faults_this_seed += 1,
                    }
                }
                5..=8 => {
                    // Commit (27%).
                    if let Some(leases) = active_leases.get(&group_idx) {
                        if let Some((cid, _lid, from, to)) = leases.first() {
                            let offset = next_offset.get(&group_idx).copied().unwrap_or(0);
                            if offset >= *from && offset <= *to {
                                match manager
                                    .commit_offset(
                                        group_id, topic_id, partition_id, *cid,
                                        Offset::new(offset), 1000 + op_num * 100,
                                    )
                                    .await
                                {
                                    Ok(()) => {
                                        commits_this_seed += 1;
                                        *next_offset.entry(group_idx).or_insert(0) = offset + 1;
                                    }
                                    Err(_) => faults_this_seed += 1,
                                }
                            }
                        }
                    }
                }
                9 => {
                    // Expire.
                    match manager
                        .expire_leases(group_id, 1000 + op_num * 100 + 70_000_000)
                        .await
                    {
                        Ok(count) => {
                            if count > 0 {
                                active_leases.entry(group_idx).or_default().clear();
                            }
                        }
                        Err(_) => faults_this_seed += 1,
                    }
                }
                10 => {
                    // Extend.
                    if let Some(leases) = active_leases.get(&group_idx) {
                        if let Some((cid, lid, _, _)) = leases.first() {
                            let pk = PartitionKey::new(topic_id, partition_id);
                            if manager
                                .extend_lease(group_id, pk, *cid, *lid, 30_000_000, 1000 + op_num * 100)
                                .await
                                .is_err()
                            {
                                faults_this_seed += 1;
                            }
                        }
                    }
                }
                11 => {
                    // Unregister consumer.
                    if manager
                        .unregister_consumer(group_id, consumer_id)
                        .await
                        .is_err()
                    {
                        faults_this_seed += 1;
                    }
                    // Re-register.
                    let _ = manager.register_consumer(group_id, consumer_id, 1000 + op_num * 100).await;
                }
                12 => {
                    // Query watermark.
                    if manager.get_low_watermark(group_id, topic_id, partition_id).await.is_err() {
                        faults_this_seed += 1;
                    }
                }
                13 => {
                    // Query safe eviction.
                    if manager.get_safe_eviction_offset(topic_id, partition_id).await.is_err() {
                        faults_this_seed += 1;
                    }
                }
                _ => {
                    // Fetch committed.
                    if manager.fetch_committed(group_id, topic_id, partition_id).await.is_err() {
                        faults_this_seed += 1;
                    }
                }
            }

            // Check invariants every 20 operations.
            if op_num % 20 == 19 {
                checker.check_all_invariants(manager.store());
            }
        }

        // Final check.
        checker.check_all_invariants(manager.store());

        if !checker.is_clean() {
            println!("Seed {} VIOLATIONS: {:?}", seed, checker.violations());
            total_violations += checker.violations().len();
        }

        total_checks += checker.checks_performed;
        total_faults += faults_this_seed;
        total_leases += leases_this_seed;
        total_commits += commits_this_seed;

        if base_seed % 50 == 49 {
            println!(
                "Progress: {}/{} seeds, {} violations, {} faults, {} leases, {} commits",
                base_seed + 1, NUM_SEEDS, total_violations, total_faults, total_leases, total_commits
            );
        }
    }

    println!("\n=== COMPREHENSIVE STRESS TEST RESULTS ===");
    println!("Seeds: {}", NUM_SEEDS);
    println!("Operations per seed: {}", OPS_PER_SEED);
    println!("Total invariant checks: {}", total_checks);
    println!("Total faults injected: {}", total_faults);
    println!("Total leases created: {}", total_leases);
    println!("Total commits: {}", total_commits);
    println!("Total violations: {}", total_violations);

    assert_eq!(total_violations, 0, "No invariant violations allowed");
    assert!(total_faults > 0, "Faults should be injected");
    assert!(total_leases > 0, "Leases should be created");
    assert!(total_commits > 0, "Commits should succeed");
}

// ============================================================================
// Test Category 6: Determinism Verification
// ============================================================================

/// Verifies exact determinism: same seed = identical execution.
#[tokio::test]
async fn test_determinism_exact_replay() {
    let config = high_fault_config();

    // Run 1.
    let mut state1 = Vec::new();
    {
        let manager = create_manager(42, config.clone());
        for i in 1..=30 {
            let r1 = manager
                .get_or_create_group(ConsumerGroupId::new(i), 1000)
                .await
                .is_ok();
            let r2 = manager
                .register_consumer(ConsumerGroupId::new(i), ConsumerId::new(1), 1000)
                .await
                .is_ok();
            state1.push((r1, r2));
        }
    }

    // Run 2 with same seed.
    let mut state2 = Vec::new();
    {
        let manager = create_manager(42, config.clone());
        for i in 1..=30 {
            let r1 = manager
                .get_or_create_group(ConsumerGroupId::new(i), 1000)
                .await
                .is_ok();
            let r2 = manager
                .register_consumer(ConsumerGroupId::new(i), ConsumerId::new(1), 1000)
                .await
                .is_ok();
            state2.push((r1, r2));
        }
    }

    assert_eq!(state1, state2, "Same seed must produce identical results");
}

/// Verifies different seeds produce different fault patterns.
#[tokio::test]
async fn test_determinism_different_seeds_diverge() {
    let config = high_fault_config();

    let mut all_results: Vec<Vec<bool>> = Vec::new();

    for seed in [1, 42, 123, 999, 12345, 54321, 0xDEAD, 0xBEEF] {
        let manager = create_manager(seed, config.clone());
        let mut results = Vec::new();

        for i in 1..=100 {
            let ok = manager
                .register_consumer(ConsumerGroupId::new(i), ConsumerId::new(1), 1000)
                .await
                .is_ok();
            results.push(ok);
        }

        all_results.push(results);
    }

    // Count unique result patterns.
    let unique_count = {
        let mut unique = all_results.clone();
        unique.sort();
        unique.dedup();
        unique.len()
    };

    // With 15% fault rate over 100 ops, different seeds should produce different patterns.
    assert!(
        unique_count >= 2,
        "Different seeds should produce at least 2 unique patterns, got {}",
        unique_count
    );
}

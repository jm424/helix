//! E2E Deterministic Simulation Tests for Helix Service.
//!
//! These tests verify the full Helix service stack under fault injection:
//! - Raft consensus via `MultiRaft`
//! - Controller partition for metadata coordination
//! - Topic creation and partition assignment
//! - Crash recovery and network partitions
//!
//! # Property Checks
//!
//! - `SingleLeaderPerTerm`: At most one controller leader per term
//! - `LeaderCompleteness`: Committed entries survive leader changes
//! - `Durability`: Synced data survives crash/recovery
//!
//! # Test Scenarios
//!
//! 1. Basic cluster startup and leader election
//! 2. Leader crash during operation
//! 3. Network partition (minority isolation)
//! 4. Stress test with multiple seeds

// Test-specific lint allowances.
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::significant_drop_tightening)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::type_complexity)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bloodhound::simulation::discrete::actors::SimulatedActor;
use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use helix_wal::FaultConfig;

use crate::helix_service_actor::{create_helix_cluster, WalMode};
use crate::properties::{
    assert_no_helix_violations, check_helix_properties, HelixPropertyCheckResult,
    HelixPropertyState, SharedHelixPropertyState,
};
use crate::raft_actor::SharedNetworkState;

// ============================================================================
// Test Configuration
// ============================================================================

/// Configuration for Helix E2E DST tests.
#[allow(clippy::struct_excessive_bools)]
pub struct HelixTestConfig {
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// Random seed for determinism.
    pub seed: u64,
    /// Maximum simulation time in seconds.
    pub max_time_secs: u64,
    /// Whether to inject crashes.
    pub inject_crashes: bool,
    /// Time to inject first crash (ms).
    pub crash_time_ms: u64,
    /// Time to inject recovery (ms).
    pub recover_time_ms: u64,
    /// Node to crash (0-indexed).
    pub crash_node_index: usize,
    /// Whether to inject network partition.
    pub inject_partition: bool,
    /// Time to create partition (ms).
    pub partition_time_ms: u64,
    /// Time to heal partition (ms).
    #[allow(dead_code)] // Reserved for future partition healing tests.
    pub heal_time_ms: u64,
    /// Whether to inject storage faults (torn writes, fsync failures).
    pub inject_storage_faults: bool,
    /// Storage fault configuration (used when inject_storage_faults is true).
    pub storage_fault_config: Option<FaultConfig>,
    /// Whether to generate client traffic (produce operations).
    pub inject_client_traffic: bool,
    /// Interval between produce operations (ms).
    pub produce_interval_ms: u64,
    /// Number of produce operations to schedule.
    pub produce_count: u32,
    /// WAL mode: per-partition or shared.
    pub wal_mode: WalMode,
    /// Number of shared WALs (only used when wal_mode is Shared).
    pub shared_wal_count: u32,
    /// Actor mode: track batch pending per-group instead of shared map.
    pub actor_mode: bool,
}

impl Default for HelixTestConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            seed: 42,
            max_time_secs: 30, // Increased from 10s
            inject_crashes: false,
            crash_time_ms: 5000,
            recover_time_ms: 15000,
            crash_node_index: 0,
            inject_partition: false,
            partition_time_ms: 5000,
            heal_time_ms: 15000,
            inject_storage_faults: false,
            storage_fault_config: None,
            inject_client_traffic: false,
            produce_interval_ms: 100, // Faster produces
            produce_count: 200, // Many more produces
            wal_mode: WalMode::InMemory, // Default to per-partition for backwards compatibility
            shared_wal_count: 4, // Default K=4 shared WALs when using shared mode
            actor_mode: false, // Default to standard mode
        }
    }
}

// ============================================================================
// Simulation Setup
// ============================================================================

/// Creates a Helix E2E simulation with property tracking.
pub fn create_helix_simulation(
    config: &HelixTestConfig,
) -> (
    DiscreteSimulationEngine,
    Vec<ActorId>,
    SharedHelixPropertyState,
    SharedNetworkState,
) {
    let engine_config = EngineConfig::new(config.seed)
        .with_max_time(Duration::from_secs(config.max_time_secs))
        .with_stats(true)
        .with_recording(false);

    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    // Create property state first so it can be shared with actors.
    let property_state = Arc::new(Mutex::new(HelixPropertyState::new()));

    // Build fault config based on test configuration.
    let fault_config = if config.inject_storage_faults {
        config
            .storage_fault_config
            .clone()
            .unwrap_or_else(FaultConfig::flaky)
    } else {
        FaultConfig::default()
    };

    // Create cluster using the factory function, passing property state and fault config.
    let (actors, network_state) = create_helix_cluster(
        config.node_count,
        config.seed,
        Arc::clone(&property_state),
        fault_config,
        config.wal_mode,
        config.shared_wal_count,
        config.actor_mode,
    );

    // Collect actor IDs.
    let actor_ids: Vec<ActorId> = actors.iter().map(SimulatedActor::id).collect();

    // Register actors with the engine.
    for actor in actors {
        engine.register_actor(Box::new(actor));
    }

    (engine, actor_ids, property_state, network_state)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helix_service_actor::custom_events;

    /// Result of running a simulation with property checking.
    struct SimulationResult {
        /// Whether the engine completed successfully.
        success: bool,
        /// Number of events processed.
        events: u64,
        /// Property check result.
        property_result: HelixPropertyCheckResult,
    }

    /// Runs a basic simulation and verifies safety properties.
    fn run_basic_simulation(config: &HelixTestConfig) -> SimulationResult {
        let (mut engine, actor_ids, property_state, _network_state) =
            create_helix_simulation(config);

        // Inject faults if configured.
        if config.inject_crashes && config.crash_node_index < actor_ids.len() {
            let crash_actor = actor_ids[config.crash_node_index];

            engine.schedule_after(
                Duration::from_millis(config.crash_time_ms),
                EventKind::ProcessCrash { actor: crash_actor },
            );

            engine.schedule_after(
                Duration::from_millis(config.recover_time_ms),
                EventKind::ProcessRecover { actor: crash_actor },
            );
        }

        // Inject network partition if configured (now uses dynamic events).
        if config.inject_partition && config.node_count >= 3 {
            let isolated = actor_ids[0];

            // Schedule partition event to be handled by the actor.
            engine.schedule_after(
                Duration::from_millis(config.partition_time_ms),
                EventKind::Custom {
                    actor: isolated,
                    name: custom_events::APPLY_PARTITION.to_string(),
                    data: Vec::new(), // Empty = isolate from all
                },
            );

            // Schedule heal event.
            engine.schedule_after(
                Duration::from_millis(config.heal_time_ms),
                EventKind::Custom {
                    actor: isolated,
                    name: custom_events::HEAL_PARTITION.to_string(),
                    data: Vec::new(), // Empty = heal all
                },
            );
        }

        // Inject client traffic if configured.
        if config.inject_client_traffic {
            // First, schedule topic creation at 500ms (after leader election).
            // Send to ALL nodes - only the controller leader will process it.
            // Data format: name_len (4 bytes) + name + partition_count (4 bytes) + replication_factor (4 bytes)
            let topic_name = b"test-topic";
            let mut create_data = Vec::with_capacity(4 + topic_name.len() + 8);
            create_data.extend_from_slice(&(topic_name.len() as u32).to_le_bytes());
            create_data.extend_from_slice(topic_name);
            create_data.extend_from_slice(&3u32.to_le_bytes()); // 3 partitions
            create_data.extend_from_slice(&(config.node_count as u32).to_le_bytes()); // RF = node count

            // Send topic creation to ALL nodes (only the leader will succeed).
            // Start at 1500ms to ensure leader election has completed (election timeout ~150-300ms).
            for (i, &actor_id) in actor_ids.iter().enumerate() {
                engine.schedule_after(
                    Duration::from_millis(1500 + (i as u64) * 50), // Stagger slightly
                    EventKind::Custom {
                        actor: actor_id,
                        name: custom_events::CREATE_TOPIC.to_string(),
                        data: create_data.clone(),
                    },
                );
            }

            // Schedule produce events starting at 2500ms (after topic creation).
            // Produce data format: topic_id (8 bytes) + partition_id (8 bytes) + payload
            // We use topic_id=1 (first user topic after controller).
            // Partition is round-robin across all 3 partitions to actually test multi-partition.
            const PARTITION_COUNT: u64 = 3;
            for i in 0..config.produce_count {
                let time_ms = 2500 + u64::from(i) * config.produce_interval_ms;

                // Round-robin across partitions to test multi-partition scenarios.
                let partition_id = u64::from(i) % PARTITION_COUNT;

                // Create produce payload with sequence number for verification.
                let mut produce_data = Vec::with_capacity(24);
                produce_data.extend_from_slice(&1u64.to_le_bytes()); // topic_id = 1
                produce_data.extend_from_slice(&partition_id.to_le_bytes()); // partition_id = round-robin
                produce_data.extend_from_slice(&u64::from(i).to_le_bytes()); // payload = sequence number

                // Round-robin across nodes (might hit non-leaders, which is realistic).
                let target_actor = actor_ids[i as usize % actor_ids.len()];

                engine.schedule_after(
                    Duration::from_millis(time_ms),
                    EventKind::Custom {
                        actor: target_actor,
                        name: custom_events::PRODUCE.to_string(),
                        data: produce_data,
                    },
                );
            }
        }

        // Schedule data integrity verification near the end of simulation.
        // We verify on ALL nodes to ensure replicated data is consistent.
        // Schedule at 90% of max_time to allow simulation to settle.
        let verify_time_ms = (config.max_time_secs * 1000 * 90) / 100;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(verify_time_ms),
                EventKind::Custom {
                    actor: actor_id,
                    name: custom_events::VERIFY_INTEGRITY.to_string(),
                    data: Vec::new(),
                },
            );
        }

        // Schedule consumer verification after integrity verification.
        // This verifies all client-ack'd data is consumable from any surviving node.
        let consumer_verify_time_ms = verify_time_ms + 1000;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(consumer_verify_time_ms),
                EventKind::Custom {
                    actor: actor_id,
                    name: custom_events::VERIFY_CONSUMER.to_string(),
                    data: Vec::new(),
                },
            );
        }

        // Run simulation.
        let result = engine.run();

        // Finalize consumer verification - check which acks no node could verify.
        if let Ok(mut state) = property_state.lock() {
            state.finalize_consumer_verification();
        }

        // Check properties.
        let property_result = check_helix_properties(&property_state)
            .expect("property state lock should not be poisoned");

        // Format per-partition ack counts for display.
        let per_partition_str: String = property_result
            .per_partition_ack_counts
            .iter()
            .map(|((t, p), count)| format!("t{}p{}={}", t, p, count))
            .collect::<Vec<_>>()
            .join(", ");

        println!(
            "Seed {}: {} events in {}ms, {} property events, leaders_by_term: {:?}, \
             partitions: {}, produces: {}, commits: {}, client_acks: {}, \
             per_partition_acks: [{}], \
             integrity_verified: {}, integrity_violations: {}, consumer_verified: {}, consumer_violations: {}",
            config.seed,
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000,
            property_result.events_processed,
            property_result.leader_summary,
            property_result.total_data_partitions,
            property_result.total_produce_success,
            property_result.total_committed_entries,
            property_result.total_client_acks,
            per_partition_str,
            property_result.integrity_verified,
            property_result.data_integrity_violations.len(),
            property_result.consumer_verified,
            property_result.consumer_violations.len(),
        );

        SimulationResult {
            success: result.success,
            events: result.stats.events_processed,
            property_result,
        }
    }

    /// Asserts simulation completed and properties hold.
    fn assert_simulation_ok(result: &SimulationResult, test_name: &str, min_events: u64) {
        assert!(
            result.success,
            "{}: Simulation should complete successfully",
            test_name
        );
        assert!(
            result.events >= min_events,
            "{}: Should process at least {} events (got {})",
            test_name,
            min_events,
            result.events
        );
        assert!(
            result.property_result.events_processed > 0,
            "{}: Property state should have recorded events",
            test_name
        );
        assert_no_helix_violations(&result.property_result, test_name);
    }

    // ------------------------------------------------------------------------
    // Basic Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_basic_startup() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 42,
            max_time_secs: 30,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "basic_startup", 500);
    }

    #[test]
    fn test_helix_service_single_node() {
        let config = HelixTestConfig {
            node_count: 1,
            seed: 123,
            max_time_secs: 30,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "single_node", 50);
    }

    #[test]
    fn test_helix_service_five_node_cluster() {
        let config = HelixTestConfig {
            node_count: 5,
            seed: 456,
            max_time_secs: 45,
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "five_node_cluster", 1000);
    }

    // ------------------------------------------------------------------------
    // Crash Recovery Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_leader_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 789,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 8000,
            recover_time_ms: 20000,
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "leader_crash_recovery", 800);
    }

    #[test]
    fn test_helix_service_follower_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1011,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 8000,
            recover_time_ms: 20000,
            crash_node_index: 2, // Crash follower (likely).
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "follower_crash_recovery", 800);
    }

    #[test]
    fn test_helix_service_rapid_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1213,
            max_time_secs: 30,
            inject_crashes: true,
            crash_time_ms: 5000,
            recover_time_ms: 10000, // Moderate recovery time.
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "rapid_crash_recovery", 500);
    }

    // ------------------------------------------------------------------------
    // Network Partition Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_minority_isolation() {
        // Create a 3-node cluster and partition node 0 from the majority.
        // This uses dynamic partition injection - partition applied at 5s, healed at 20s.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1415,
            max_time_secs: 45,
            inject_partition: true, // Use dynamic partition injection.
            partition_time_ms: 5000,
            heal_time_ms: 20000,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "minority_isolation", 800);

        // Verify we saw leader elections (partition should cause re-election).
        assert!(
            !result.property_result.leader_summary.is_empty(),
            "Should have recorded leader elections"
        );
    }

    #[test]
    fn test_helix_service_partition_with_crash() {
        // Test partition combined with crash - more aggressive fault injection.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1617,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 30000,
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 15000,
            heal_time_ms: 35000,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 400,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "partition_with_crash", 1000);
    }

    // ------------------------------------------------------------------------
    // Storage Fault Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_storage_faults_basic() {
        // Test with basic storage faults (flaky config).
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1819,
            max_time_secs: 45,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "storage_faults_basic", 800);
    }

    #[test]
    fn test_helix_service_storage_faults_with_crash() {
        // Combine storage faults with process crash - tests recovery under faulty storage.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 2021,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 25000,
            crash_node_index: 0,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "storage_faults_with_crash", 800);
    }

    #[test]
    fn test_helix_service_storage_faults_all_fault_types() {
        // Test with all fault types enabled - aggressive storage testing.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 2223,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 12000,
            recover_time_ms: 30000,
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 8000,
            heal_time_ms: 25000,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 400,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "storage_all_fault_types", 1000);
    }

    // ------------------------------------------------------------------------
    // Client Traffic Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_client_traffic_basic() {
        // Basic client traffic: topic creation + produce operations.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 2425,
            max_time_secs: 45,
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 400,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "client_traffic_basic", 1000);
    }

    #[test]
    fn test_helix_service_client_traffic_with_crash() {
        // Client traffic during crash/recovery - tests durability.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 2627,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 30000,
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "client_traffic_with_crash", 1200);
    }

    #[test]
    fn test_helix_service_client_traffic_with_partition() {
        // Client traffic during network partition.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 2829,
            max_time_secs: 60,
            inject_partition: true,
            partition_time_ms: 8000,
            heal_time_ms: 30000,
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "client_traffic_with_partition", 1200);
    }

    #[test]
    fn test_helix_service_client_traffic_all_faults() {
        // Client traffic with all fault types - comprehensive durability test.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 3031,
            max_time_secs: 90,
            inject_crashes: true,
            crash_time_ms: 15000,
            recover_time_ms: 45000,
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 10000,
            heal_time_ms: 40000,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 800,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "client_traffic_all_faults", 2000);
    }

    // ------------------------------------------------------------------------
    // Stress Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_stress_10_seeds() {
        let mut total_property_events = 0u64;
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 30,
                inject_client_traffic: true,  // Always inject client traffic.
                produce_interval_ms: 80,
                produce_count: 250,
                inject_storage_faults: seed % 3 == 0, // Storage faults every 3rd seed.
                storage_fault_config: if seed % 3 == 0 { Some(FaultConfig::flaky()) } else { None },
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("stress_seed_{}", seed), 500);

            total_property_events += result.property_result.events_processed;
            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across 10 seeds");
        println!(
            "Stress test: 10 seeds completed, {} property events, {} produces, {} commits, 0 violations",
            total_property_events, total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_stress_30_seeds() {
        // 30-seed stress test for InMemory mode (aligned with other WAL modes).
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..30 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 30,
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 250,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 8000,
                recover_time_ms: 18000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 4 == 0,
                storage_fault_config: if seed % 4 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("inmemory_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across InMemory 30-seed stress");
        println!(
            "InMemory stress: 30 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_stress_50_seeds() {
        let mut total_events = 0u64;
        let mut total_property_events = 0u64;
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..50 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 30,
                inject_client_traffic: true,  // Always inject client traffic.
                produce_interval_ms: 100,
                produce_count: 200,
                inject_crashes: seed % 5 == 0, // Crash every 5th seed.
                crash_time_ms: 8000,
                recover_time_ms: 18000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 4 == 0, // Storage faults every 4th seed.
                storage_fault_config: if seed % 4 == 0 { Some(FaultConfig::flaky()) } else { None },
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            assert!(
                result.success,
                "Seed {} should complete successfully",
                seed
            );
            assert_no_helix_violations(&result.property_result, &format!("stress_50_seed_{}", seed));

            total_events += result.events;
            total_property_events += result.property_result.events_processed;
            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across 50 seeds");
        println!(
            "Stress test: 50 seeds completed, {} events, {} property events, {} produces, {} commits",
            total_events, total_property_events, total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_stress_crash_recovery_10_seeds() {
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: true,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_client_traffic: true,  // Always inject client traffic during crashes.
                produce_interval_ms: 80,
                produce_count: 400,
                inject_storage_faults: true, // Always inject storage faults during crash recovery.
                storage_fault_config: Some(FaultConfig::flaky()),
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("crash_stress_seed_{}", seed), 800);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across crash recovery seeds");
        println!(
            "Crash recovery stress: 10 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    // ------------------------------------------------------------------------
    // Long-Running Tests (commented out for CI, uncomment for thorough testing)
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_stress_100_seeds() {
        let mut total_events = 0u64;
        let mut total_property_events = 0u64;
        let mut total_violations = 0usize;
        let mut total_integrity_violations = 0usize;
        let mut total_consumer_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;
        let mut total_client_acks = 0usize;
        let mut failures = Vec::new();

        for seed in 0..100 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 60,  // 60 seconds per seed.
                inject_crashes: seed % 3 == 0,           // Crash every 3rd seed.
                crash_time_ms: 15000,
                recover_time_ms: 35000,
                crash_node_index: (seed as usize) % 3,
                inject_partition: seed % 5 == 0,         // Partition every 5th seed.
                partition_time_ms: 10000,
                heal_time_ms: 30000,
                inject_storage_faults: seed % 4 == 0,    // Storage faults every 4th seed.
                storage_fault_config: if seed % 4 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,             // Always inject client traffic.
                produce_interval_ms: 80,
                produce_count: 600,                      // 600 produces per seed (fills 60s at 80ms interval).
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            if !result.success || !result.property_result.is_ok() {
                failures.push(seed);
            }
            total_events += result.events;
            total_property_events += result.property_result.events_processed;
            total_violations += result.property_result.violations.len();
            total_integrity_violations += result.property_result.data_integrity_violations.len();
            total_consumer_violations += result.property_result.consumer_violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
            total_client_acks += result.property_result.total_client_acks;
        }

        assert!(
            failures.is_empty(),
            "Seeds failed: {:?}",
            failures
        );
        assert_eq!(total_violations, 0, "No consensus violations across 100 seeds");
        assert_eq!(total_integrity_violations, 0, "No data integrity violations across 100 seeds");
        assert_eq!(total_consumer_violations, 0, "No consumer violations (data loss) across 100 seeds");

        println!(
            "Stress test: 100 seeds completed, {} events, {} property events, {} produces, {} commits, \
             {} client_acks, 0 consensus violations, 0 integrity violations, 0 consumer violations",
            total_events, total_property_events, total_produces, total_commits, total_client_acks
        );
    }

    #[test]
    #[ignore] // Run manually with: cargo test inmemory_stress_500 -- --ignored
    fn test_helix_service_stress_500_seeds() {
        // Comprehensive 500-seed stress test for InMemory mode.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..500 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            if seed % 50 == 0 {
                println!("InMemory seed {}: produces={}, commits={}, violations={}",
                    seed,
                    result.property_result.total_produce_success,
                    result.property_result.total_committed_entries,
                    result.property_result.violations.len()
                );
            }

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;

            assert_eq!(
                result.property_result.violations.len(), 0,
                "InMemory seed {} had violations", seed
            );
        }

        println!(
            "InMemory stress: 500 seeds completed, {} produces, {} commits, {} violations",
            total_produces, total_commits, total_violations
        );
        assert_eq!(total_violations, 0, "No violations across 500 seeds");
    }

    // ------------------------------------------------------------------------
    // SharedWAL E2E Tests
    // ------------------------------------------------------------------------
    // These tests verify that the SharedWAL code path works correctly under
    // fault injection, providing coverage for the production WAL architecture.

    #[test]
    fn test_helix_service_shared_wal_basic() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 42,
            max_time_secs: 30,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            wal_mode: WalMode::Shared,
            shared_wal_count: 4,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "shared_wal_basic", 500);
    }

    #[test]
    fn test_helix_service_shared_wal_with_faults() {
        // Test SharedWAL with storage faults (torn writes, fsync failures).
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1234,
            max_time_secs: 45,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            wal_mode: WalMode::Shared,
            shared_wal_count: 4,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "shared_wal_with_faults", 800);
    }

    #[test]
    fn test_helix_service_shared_wal_crash_recovery() {
        // Test SharedWAL with crash/recovery cycles.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 5678,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 25000,
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            wal_mode: WalMode::Shared,
            shared_wal_count: 4,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "shared_wal_crash_recovery", 800);
    }

    #[test]
    fn test_helix_service_shared_wal_all_faults() {
        // Comprehensive SharedWAL test with all fault types.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 9999,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 15000,
            recover_time_ms: 35000,
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 10000,
            heal_time_ms: 30000,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,
            wal_mode: WalMode::Shared,
            shared_wal_count: 4,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "shared_wal_all_faults", 1200);
    }

    #[test]
    fn test_helix_service_shared_wal_stress_10_seeds() {
        // Multi-seed stress test for SharedWAL.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0, // Storage faults every other seed.
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::Shared,
                shared_wal_count: 4,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("shared_wal_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across SharedWAL stress seeds");
        println!(
            "SharedWAL stress: 10 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_shared_wal_stress_30_seeds() {
        // 30-seed stress test for SharedWAL (aligned with other WAL modes).
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..30 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::Shared,
                shared_wal_count: 4,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("shared_wal_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across SharedWAL 30-seed stress");
        println!(
            "SharedWAL stress: 30 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    #[ignore] // Run manually with: cargo test shared_wal_stress_500 -- --ignored
    fn test_helix_service_shared_wal_stress_500_seeds() {
        // Comprehensive 500-seed stress test for SharedWAL.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..500 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::Shared,
                shared_wal_count: 4,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            if seed % 50 == 0 {
                println!("SharedWAL seed {}: produces={}, commits={}, violations={}",
                    seed,
                    result.property_result.total_produce_success,
                    result.property_result.total_committed_entries,
                    result.property_result.violations.len()
                );
            }

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;

            assert_eq!(
                result.property_result.violations.len(), 0,
                "SharedWAL seed {} had violations", seed
            );
        }

        println!(
            "SharedWAL stress: 500 seeds completed, {} produces, {} commits, {} violations",
            total_produces, total_commits, total_violations
        );
        assert_eq!(total_violations, 0, "No violations across 500 seeds");
    }

    #[test]
    fn test_helix_service_shared_wal_different_pool_sizes() {
        // Test different SharedWAL pool sizes (K=1, 2, 4, 8).
        for wal_count in [1, 2, 4, 8] {
            let config = HelixTestConfig {
                node_count: 3,
                seed: u64::from(wal_count) * 1000,
                max_time_secs: 30,
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 200,
                wal_mode: WalMode::Shared,
                shared_wal_count: wal_count,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(
                &result,
                &format!("shared_wal_pool_size_{}", wal_count),
                500,
            );
        }
    }

    // =========================================================================
    // Multi-Partition Leader Failure Tests
    // =========================================================================
    // These tests verify that partition leaders can fail over correctly when
    // the node hosting them crashes, and that no data is lost.

    #[test]
    fn test_helix_service_multi_partition_leader_failure() {
        // 3-node cluster with client traffic to multiple partitions.
        // Crash node 0 (likely leader for some partitions) mid-test.
        // Verify partitions remain accessible after failover.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 8888,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 15000,    // Crash at 15s
            recover_time_ms: 35000,  // Recover at 35s
            crash_node_index: 0,     // Crash node 0
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,      // Many produces across partitions
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "multi_partition_leader_failure", 1000);

        // Verify we had substantial client acks (data was actually written).
        // With 500 produce operations, we expect a significant number of acks.
        let min_expected_acks = result.property_result.total_committed_entries / 2;
        assert!(
            result.property_result.total_client_acks >= min_expected_acks as usize,
            "Should have substantial client acks: got {}, expected at least {} (commits={})",
            result.property_result.total_client_acks,
            min_expected_acks,
            result.property_result.total_committed_entries
        );

        // Verify data went to ALL 3 partitions (not just partition 0).
        // Topic ID 1 is the first user topic; partitions are 0, 1, 2.
        let acks = &result.property_result.per_partition_ack_counts;
        assert!(
            acks.get(&(1, 0)).map_or(0, |&c| c) > 0,
            "Should have acks for partition 0, got: {:?}",
            acks
        );
        assert!(
            acks.get(&(1, 1)).map_or(0, |&c| c) > 0,
            "Should have acks for partition 1, got: {:?}",
            acks
        );
        assert!(
            acks.get(&(1, 2)).map_or(0, |&c| c) > 0,
            "Should have acks for partition 2, got: {:?}",
            acks
        );

        // Verify no data integrity violations.
        assert!(
            result.property_result.data_integrity_violations.is_empty(),
            "Should have no data integrity violations"
        );

        // Verify no consumer violations (acknowledged data is consumable).
        assert!(
            result.property_result.consumer_violations.is_empty(),
            "Should have no consumer violations (data loss)"
        );
    }

    #[test]
    fn test_helix_service_multi_partition_leader_failure_stress() {
        // Multi-seed stress test for partition leader failure.
        let mut total_violations = 0usize;
        let mut total_client_acks = 0usize;
        let mut total_data_integrity_violations = 0usize;
        let mut total_consumer_violations = 0usize;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 60,
                inject_crashes: true,
                crash_time_ms: 12000 + (seed % 5) * 2000,  // Vary crash time
                recover_time_ms: 35000,
                crash_node_index: (seed as usize) % 3,     // Rotate crash node
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 400,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(
                &result,
                &format!("multi_partition_leader_failure_seed_{}", seed),
                800,
            );

            total_violations += result.property_result.violations.len();
            total_client_acks += result.property_result.total_client_acks;
            total_data_integrity_violations += result.property_result.data_integrity_violations.len();
            total_consumer_violations += result.property_result.consumer_violations.len();
        }

        assert_eq!(
            total_violations, 0,
            "No consensus violations across multi-partition leader failure stress"
        );
        assert_eq!(
            total_data_integrity_violations, 0,
            "No data integrity violations across stress test"
        );
        assert_eq!(
            total_consumer_violations, 0,
            "No consumer violations (data loss) across stress test"
        );
        println!(
            "Multi-partition leader failure stress: 10 seeds completed, {} client acks, 0 violations",
            total_client_acks
        );
    }

    #[test]
    fn test_helix_service_multi_partition_cascading_failure() {
        // Test cascading failure: crash one node, then partition another.
        // This exercises more complex failure scenarios.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 9999,
            max_time_secs: 90,
            inject_crashes: true,
            crash_time_ms: 15000,    // Crash node at 15s
            recover_time_ms: 50000,  // Recover at 50s
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 25000, // Partition at 25s (while node is crashed)
            heal_time_ms: 45000,      // Heal partition at 45s
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 600,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "multi_partition_cascading_failure", 1500);

        // Verify safety properties held under cascading failures.
        assert!(
            result.property_result.violations.is_empty(),
            "No consensus violations during cascading failure"
        );
        assert!(
            result.property_result.data_integrity_violations.is_empty(),
            "No data integrity violations during cascading failure"
        );
    }

    // =========================================================================
    // Per-Partition Durable WAL Tests
    // =========================================================================

    #[test]
    fn test_helix_service_per_partition_basic() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 42,
            max_time_secs: 30,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            wal_mode: WalMode::PerPartition,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "per_partition_basic", 500);
    }

    #[test]
    fn test_helix_service_per_partition_with_faults() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1234,
            max_time_secs: 45,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            wal_mode: WalMode::PerPartition,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "per_partition_with_faults", 500);
    }

    #[test]
    fn test_helix_service_per_partition_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 5678,
            max_time_secs: 45,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 25000,
            crash_node_index: 0,
            wal_mode: WalMode::PerPartition,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "per_partition_crash_recovery", 500);
    }

    #[test]
    fn test_helix_service_per_partition_all_faults() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 9999,
            max_time_secs: 60,
            inject_client_traffic: true,
            produce_interval_ms: 150,
            produce_count: 250,
            inject_crashes: true,
            crash_time_ms: 15000,
            recover_time_ms: 40000,
            crash_node_index: 1,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            wal_mode: WalMode::PerPartition,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "per_partition_all_faults", 500);
    }

    #[test]
    fn test_helix_service_per_partition_stress_10_seeds() {
        // Multi-seed stress test for PerPartition WAL.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::PerPartition,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("per_partition_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across durable per-partition stress seeds");
        println!(
            "PerPartition stress: 10 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_per_partition_stress_30_seeds() {
        // 30-seed stress test for PerPartition WAL (aligned with other WAL modes).
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..30 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::PerPartition,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("per_partition_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
        }

        assert_eq!(total_violations, 0, "No violations across durable per-partition 30-seed stress");
        println!(
            "PerPartition stress: 30 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    #[ignore] // Run manually with: cargo test per_partition_stress_500 -- --ignored
    fn test_helix_service_per_partition_stress_500_seeds() {
        // Comprehensive 500-seed stress test for PerPartition WAL.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;

        for seed in 0..500 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                wal_mode: WalMode::PerPartition,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            if seed % 50 == 0 {
                println!("PerPartition seed {}: produces={}, commits={}, violations={}",
                    seed,
                    result.property_result.total_produce_success,
                    result.property_result.total_committed_entries,
                    result.property_result.violations.len()
                );
            }

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;

            assert_eq!(
                result.property_result.violations.len(), 0,
                "PerPartition seed {} had violations", seed
            );
        }

        println!(
            "PerPartition stress: 500 seeds completed, {} produces, {} commits, {} violations",
            total_produces, total_commits, total_violations
        );
        assert_eq!(total_violations, 0, "No violations across 500 seeds");
    }

    // =========================================================================
    // Actor Mode DST Tests
    // =========================================================================
    //
    // These tests use actor_mode: true to test the REAL production partition
    // actor code path (PartitionActor, WalActor) with fine-grained fault injection.
    //
    // Actor mode tests the actual production implementation with:
    // - SimulatedStorage for disk fault injection (torn writes, fsync failures)
    // - SimulatedTransport for network fault injection (partitions, delays)
    // - Deterministic timing via Bloodhound
    // - Property-based validation (data integrity, consumer verification)

    #[test]
    fn test_helix_service_actor_mode_basic() {
        // Basic actor mode test - verifies the production partition actor works.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 42,
            max_time_secs: 30,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 200,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_basic", 500);
    }

    #[test]
    fn test_helix_service_actor_mode_with_crashes() {
        // Actor mode with process crash/recovery.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1234,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 10000,
            recover_time_ms: 25000,
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_with_crashes", 800);
    }

    #[test]
    fn test_helix_service_actor_mode_with_storage_faults() {
        // Actor mode with storage faults (torn writes, fsync failures).
        let config = HelixTestConfig {
            node_count: 3,
            seed: 5678,
            max_time_secs: 45,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_with_storage_faults", 800);
    }

    #[test]
    fn test_helix_service_actor_mode_with_partition() {
        // Actor mode with network partition.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 9012,
            max_time_secs: 45,
            inject_partition: true,
            partition_time_ms: 8000,
            heal_time_ms: 25000,
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_with_partition", 800);
    }

    #[test]
    fn test_helix_service_actor_mode_all_faults() {
        // Actor mode with all fault types - comprehensive test.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 3456,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 15000,
            recover_time_ms: 35000,
            crash_node_index: 1,
            inject_partition: true,
            partition_time_ms: 10000,
            heal_time_ms: 30000,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_all_faults", 1200);
    }

    #[test]
    fn test_helix_service_actor_mode_stress_10_seeds() {
        // Multi-seed stress test for actor mode.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;
        let mut total_client_acks = 0usize;

        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                actor_mode: true,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("actor_mode_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
            total_client_acks += result.property_result.total_client_acks;
        }

        assert_eq!(total_violations, 0, "No violations across actor mode stress seeds");
        println!(
            "Actor mode stress: 10 seeds completed, {} produces, {} commits, {} client_acks, 0 violations",
            total_produces, total_commits, total_client_acks
        );
    }

    #[test]
    fn test_helix_service_actor_mode_stress_30_seeds() {
        // 30-seed stress test for actor mode (aligned with other modes).
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;
        let mut total_integrity_violations = 0usize;
        let mut total_consumer_violations = 0usize;

        for seed in 0..30 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 45,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 10000,
                recover_time_ms: 25000,
                crash_node_index: (seed as usize) % 3,
                inject_partition: seed % 5 == 0,
                partition_time_ms: 8000,
                heal_time_ms: 22000,
                inject_storage_faults: seed % 2 == 0,
                storage_fault_config: if seed % 2 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 100,
                produce_count: 300,
                actor_mode: true,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);
            assert_simulation_ok(&result, &format!("actor_mode_stress_seed_{}", seed), 500);

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
            total_integrity_violations += result.property_result.data_integrity_violations.len();
            total_consumer_violations += result.property_result.consumer_violations.len();
        }

        assert_eq!(total_violations, 0, "No consensus violations across actor mode 30-seed stress");
        assert_eq!(total_integrity_violations, 0, "No data integrity violations across actor mode stress");
        assert_eq!(total_consumer_violations, 0, "No consumer violations (data loss) across actor mode stress");
        println!(
            "Actor mode stress: 30 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    #[ignore] // Run manually with: cargo test actor_mode_stress_100 -- --ignored
    fn test_helix_service_actor_mode_stress_100_seeds() {
        // Comprehensive 100-seed stress test for actor mode.
        let mut total_violations = 0usize;
        let mut total_produces = 0u64;
        let mut total_commits = 0u64;
        let mut total_integrity_violations = 0usize;
        let mut total_consumer_violations = 0usize;
        let mut failures = Vec::new();

        for seed in 0..100 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 60,
                inject_crashes: seed % 3 == 0,
                crash_time_ms: 15000,
                recover_time_ms: 35000,
                crash_node_index: (seed as usize) % 3,
                inject_partition: seed % 5 == 0,
                partition_time_ms: 10000,
                heal_time_ms: 30000,
                inject_storage_faults: seed % 4 == 0,
                storage_fault_config: if seed % 4 == 0 {
                    Some(FaultConfig::flaky())
                } else {
                    None
                },
                inject_client_traffic: true,
                produce_interval_ms: 80,
                produce_count: 600,
                actor_mode: true,
                ..Default::default()
            };

            let result = run_basic_simulation(&config);

            if seed % 10 == 0 {
                println!(
                    "Actor mode seed {}: produces={}, commits={}, violations={}, integrity={}, consumer={}",
                    seed,
                    result.property_result.total_produce_success,
                    result.property_result.total_committed_entries,
                    result.property_result.violations.len(),
                    result.property_result.data_integrity_violations.len(),
                    result.property_result.consumer_violations.len()
                );
            }

            if !result.success || !result.property_result.is_ok() {
                failures.push(seed);
            }

            total_violations += result.property_result.violations.len();
            total_produces += result.property_result.total_produce_success;
            total_commits += result.property_result.total_committed_entries;
            total_integrity_violations += result.property_result.data_integrity_violations.len();
            total_consumer_violations += result.property_result.consumer_violations.len();
        }

        assert!(failures.is_empty(), "Actor mode seeds failed: {:?}", failures);
        assert_eq!(total_violations, 0, "No consensus violations across actor mode 100-seed stress");
        assert_eq!(total_integrity_violations, 0, "No data integrity violations");
        assert_eq!(total_consumer_violations, 0, "No consumer violations (data loss)");

        println!(
            "Actor mode stress: 100 seeds completed, {} produces, {} commits, 0 violations",
            total_produces, total_commits
        );
    }

    #[test]
    fn test_helix_service_actor_mode_with_shared_wal() {
        // Actor mode with SharedWAL - tests production WAL architecture.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 7890,
            max_time_secs: 45,
            inject_crashes: true,
            crash_time_ms: 12000,
            recover_time_ms: 28000,
            crash_node_index: 0,
            inject_storage_faults: true,
            storage_fault_config: Some(FaultConfig::flaky()),
            inject_client_traffic: true,
            produce_interval_ms: 100,
            produce_count: 300,
            actor_mode: true,
            wal_mode: WalMode::Shared,
            shared_wal_count: 4,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_with_shared_wal", 800);
    }

    #[test]
    fn test_helix_service_actor_mode_leader_failure() {
        // Actor mode with leader crash - verifies partition failover.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 8888,
            max_time_secs: 60,
            inject_crashes: true,
            crash_time_ms: 15000,
            recover_time_ms: 35000,
            crash_node_index: 0,
            inject_client_traffic: true,
            produce_interval_ms: 80,
            produce_count: 500,
            actor_mode: true,
            ..Default::default()
        };

        let result = run_basic_simulation(&config);
        assert_simulation_ok(&result, "actor_mode_leader_failure", 1000);

        // Verify we had substantial client acks (data was actually written).
        // With 500 produce operations, we expect a significant number of acks.
        // Client acks should be close to commits since all commits from our
        // produces should have acks when we were the proposing leader.
        let min_expected_acks = result.property_result.total_committed_entries / 2;
        assert!(
            result.property_result.total_client_acks >= min_expected_acks as usize,
            "Should have substantial client acks: got {}, expected at least {} (commits={})",
            result.property_result.total_client_acks,
            min_expected_acks,
            result.property_result.total_committed_entries
        );

        // Verify no data integrity violations.
        assert!(
            result.property_result.data_integrity_violations.is_empty(),
            "Should have no data integrity violations"
        );

        // Verify no consumer violations (acknowledged data is consumable).
        assert!(
            result.property_result.consumer_violations.is_empty(),
            "Should have no consumer violations (data loss)"
        );
    }

}

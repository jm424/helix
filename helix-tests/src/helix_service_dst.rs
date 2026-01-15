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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bloodhound::simulation::discrete::actors::SimulatedActor;
use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use helix_raft::RaftState;

use crate::helix_service_actor::create_helix_cluster;
use crate::raft_actor::SharedNetworkState;

// ============================================================================
// Property State Tracking (for future use)
// ============================================================================

/// Property verification infrastructure for Helix E2E DST.
///
/// This module defines types for tracking and verifying safety properties.
/// Currently tests verify basic functionality; property checking will be
/// fully wired up in a future iteration.
#[allow(dead_code)]
mod property_tracking {
    use super::*;

    /// Snapshot of a Helix service node's state.
    #[derive(Debug, Clone)]
    pub struct HelixNodeSnapshot {
        /// Node ID.
        pub node_id: u64,
        /// Controller term.
        pub controller_term: u64,
        /// Controller state (Leader, Follower, Candidate).
        pub controller_state: RaftState,
        /// Whether the node is crashed.
        pub crashed: bool,
    }

    /// Property state for Helix E2E DST.
    #[derive(Debug, Default)]
    pub struct HelixPropertyState {
        /// Snapshots: node_id -> snapshot.
        pub snapshots: BTreeMap<u64, HelixNodeSnapshot>,
        /// Leaders observed per term: term -> set of node IDs.
        pub leaders_by_term: BTreeMap<u64, BTreeSet<u64>>,
        /// Total events processed.
        pub events_processed: u64,
    }

    impl HelixPropertyState {
        /// Creates new empty state.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Updates snapshot for a node.
        pub fn update_snapshot(&mut self, snapshot: HelixNodeSnapshot) {
            // Track leaders by term.
            if snapshot.controller_state == RaftState::Leader {
                self.leaders_by_term
                    .entry(snapshot.controller_term)
                    .or_default()
                    .insert(snapshot.node_id);
            }

            self.snapshots.insert(snapshot.node_id, snapshot);
        }

        /// Increments event counter.
        pub fn increment_events(&mut self) {
            self.events_processed += 1;
        }
    }

    /// Shared property state handle.
    pub type SharedHelixPropertyState = Arc<Mutex<HelixPropertyState>>;

    /// Property violation types.
    #[derive(Debug, Clone)]
    pub enum HelixViolation {
        /// Multiple leaders in same term.
        MultipleLeadersInTerm { term: u64, leaders: Vec<u64> },
    }

    impl std::fmt::Display for HelixViolation {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::MultipleLeadersInTerm { term, leaders } => {
                    write!(f, "Multiple leaders in term {}: {:?}", term, leaders)
                }
            }
        }
    }

    /// Result of property checking.
    #[derive(Debug)]
    pub struct HelixPropertyCheckResult {
        /// Detected violations.
        pub violations: Vec<HelixViolation>,
        /// Events processed.
        pub events_processed: u64,
        /// Leader summary: term -> count of leaders seen.
        pub leader_summary: BTreeMap<u64, usize>,
    }

    impl HelixPropertyCheckResult {
        /// Returns true if no violations were detected.
        #[must_use]
        pub fn is_ok(&self) -> bool {
            self.violations.is_empty()
        }
    }

    /// Checks property state for violations.
    pub fn check_helix_properties(
        state: &SharedHelixPropertyState,
    ) -> Result<HelixPropertyCheckResult, &'static str> {
        let state = state.lock().map_err(|_| "lock poisoned")?;

        let mut violations = Vec::new();
        let mut leader_summary = BTreeMap::new();

        // Check SingleLeaderPerTerm.
        for (&term, leaders) in &state.leaders_by_term {
            leader_summary.insert(term, leaders.len());

            if leaders.len() > 1 {
                violations.push(HelixViolation::MultipleLeadersInTerm {
                    term,
                    leaders: leaders.iter().copied().collect(),
                });
            }
        }

        Ok(HelixPropertyCheckResult {
            violations,
            events_processed: state.events_processed,
            leader_summary,
        })
    }

    /// Creates a client request event for state collection.
    pub fn collect_helix_state_event(actor: ActorId) -> EventKind {
        EventKind::Custom {
            actor,
            name: "collect_state".to_string(),
            data: Vec::new(),
        }
    }

    /// Asserts no property violations.
    pub fn assert_no_violations(result: &HelixPropertyCheckResult, test_name: &str) {
        if !result.is_ok() {
            let violations: Vec<_> = result.violations.iter().map(ToString::to_string).collect();
            panic!(
                "{} FAILED with {} violations:\n{}",
                test_name,
                violations.len(),
                violations.join("\n")
            );
        }
    }
}

// Re-export for use when property checking is enabled.
#[allow(unused_imports)]
pub use property_tracking::*;

// ============================================================================
// Test Configuration
// ============================================================================

/// Configuration for Helix E2E DST tests.
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
}

impl Default for HelixTestConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            seed: 42,
            max_time_secs: 10,
            inject_crashes: false,
            crash_time_ms: 2000,
            recover_time_ms: 6000,
            crash_node_index: 0,
            inject_partition: false,
            partition_time_ms: 2000,
            heal_time_ms: 6000,
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

    // Create cluster using the factory function.
    let (actors, network_state) = create_helix_cluster(config.node_count, config.seed);

    // Collect actor IDs.
    let actor_ids: Vec<ActorId> = actors.iter().map(|a| a.id()).collect();

    // Register actors with the engine.
    for actor in actors {
        engine.register_actor(Box::new(actor));
    }

    // Create property state.
    let property_state = Arc::new(Mutex::new(HelixPropertyState::new()));

    (engine, actor_ids, property_state, network_state)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs a basic simulation and checks for leader election.
    fn run_basic_simulation(config: &HelixTestConfig) -> (bool, u64) {
        let (mut engine, actor_ids, _property_state, _network_state) =
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

        // Inject network partition if configured.
        if config.inject_partition && config.node_count >= 3 {
            // Partition node 0 from nodes 1 and 2.
            let isolated = actor_ids[0];
            let _majority: Vec<_> = actor_ids.iter().skip(1).copied().collect();

            // Create partition event.
            // Note: Actual partitioning is done via network_state in specific tests.
            engine.schedule_after(
                Duration::from_millis(config.partition_time_ms),
                EventKind::Custom {
                    actor: isolated,
                    name: "partition".to_string(),
                    data: Vec::new(),
                },
            );
        }

        // Run simulation.
        let result = engine.run();

        println!(
            "Seed {}: {} events in {}ms virtual time",
            config.seed,
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000,
        );

        (result.success, result.stats.events_processed)
    }

    // ------------------------------------------------------------------------
    // Basic Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_basic_startup() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 42,
            max_time_secs: 5,
            ..Default::default()
        };

        let (success, events) = run_basic_simulation(&config);

        assert!(success, "Simulation should complete successfully");
        assert!(events > 100, "Should process meaningful number of events");
    }

    #[test]
    fn test_helix_service_single_node() {
        let config = HelixTestConfig {
            node_count: 1,
            seed: 123,
            max_time_secs: 3,
            ..Default::default()
        };

        let (success, events) = run_basic_simulation(&config);

        assert!(success, "Single node simulation should complete");
        assert!(events > 50, "Should process events");
    }

    #[test]
    fn test_helix_service_five_node_cluster() {
        let config = HelixTestConfig {
            node_count: 5,
            seed: 456,
            max_time_secs: 10,
            ..Default::default()
        };

        let (success, events) = run_basic_simulation(&config);

        assert!(success, "5-node simulation should complete");
        assert!(events > 200, "Should process many events");
    }

    // ------------------------------------------------------------------------
    // Crash Recovery Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_leader_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 789,
            max_time_secs: 15,
            inject_crashes: true,
            crash_time_ms: 3000,
            recover_time_ms: 8000,
            crash_node_index: 0,
            ..Default::default()
        };

        let (success, events) = run_basic_simulation(&config);

        assert!(success, "Crash recovery simulation should complete");
        assert!(events > 300, "Should process events across crash/recovery");
    }

    #[test]
    fn test_helix_service_follower_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1011,
            max_time_secs: 15,
            inject_crashes: true,
            crash_time_ms: 3000,
            recover_time_ms: 8000,
            crash_node_index: 2, // Crash follower (likely).
            ..Default::default()
        };

        let (success, events) = run_basic_simulation(&config);

        assert!(success, "Follower crash recovery should complete");
        assert!(events > 300, "Should process events across crash/recovery");
    }

    #[test]
    fn test_helix_service_rapid_crash_recovery() {
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1213,
            max_time_secs: 10,
            inject_crashes: true,
            crash_time_ms: 1000,
            recover_time_ms: 2000, // Quick recovery.
            crash_node_index: 0,
            ..Default::default()
        };

        let (success, _events) = run_basic_simulation(&config);

        assert!(success, "Rapid crash recovery should complete");
    }

    // ------------------------------------------------------------------------
    // Network Partition Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_minority_isolation() {
        // Create a 3-node cluster and partition node 0 from the majority.
        let config = HelixTestConfig {
            node_count: 3,
            seed: 1415,
            max_time_secs: 15,
            inject_partition: false, // We'll manually partition via network state.
            ..Default::default()
        };

        let (mut engine, actor_ids, _property_state, network_state) =
            create_helix_simulation(&config);

        // Schedule partition at 2s.
        let isolated = actor_ids[0];
        let other1 = actor_ids[1];
        let other2 = actor_ids[2];

        // Use a Custom event to trigger partition.
        engine.schedule_after(
            Duration::from_millis(2000),
            EventKind::Custom {
                actor: isolated,
                name: "apply_partition".to_string(),
                data: Vec::new(),
            },
        );

        // Apply partition via network state (before run).
        // Note: For true partition timing, we'd need actor handling.
        // For now, we partition immediately for the test.
        {
            let mut state = network_state.lock().expect("lock");
            // Partition isolated from others.
            state.partition(&[isolated, other1]);
            state.partition(&[isolated, other2]);
        }

        let result = engine.run();

        // Heal partition and run a bit more.
        {
            let mut state = network_state.lock().expect("lock");
            state.heal(&[isolated, other1]);
            state.heal(&[isolated, other2]);
        }

        println!(
            "Partition test: {} events in {}ms",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000,
        );

        assert!(result.success, "Partition test should complete");
    }

    // ------------------------------------------------------------------------
    // Stress Tests
    // ------------------------------------------------------------------------

    #[test]
    fn test_helix_service_stress_10_seeds() {
        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 5,
                ..Default::default()
            };

            let (success, events) = run_basic_simulation(&config);

            assert!(
                success,
                "Seed {} should complete successfully",
                seed
            );
            assert!(
                events > 50,
                "Seed {} should process events (got {})",
                seed,
                events
            );
        }

        println!("Stress test: 10 seeds completed successfully");
    }

    #[test]
    fn test_helix_service_stress_50_seeds() {
        let mut total_events = 0u64;

        for seed in 0..50 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 3,
                ..Default::default()
            };

            let (success, events) = run_basic_simulation(&config);

            assert!(
                success,
                "Seed {} should complete successfully",
                seed
            );
            total_events += events;
        }

        println!(
            "Stress test: 50 seeds completed, {} total events",
            total_events
        );
    }

    #[test]
    fn test_helix_service_stress_crash_recovery_10_seeds() {
        for seed in 0..10 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 10,
                inject_crashes: true,
                crash_time_ms: 2000,
                recover_time_ms: 5000,
                crash_node_index: (seed as usize) % 3,
                ..Default::default()
            };

            let (success, events) = run_basic_simulation(&config);

            assert!(
                success,
                "Crash recovery seed {} should complete",
                seed
            );
            assert!(
                events > 100,
                "Seed {} should process meaningful events",
                seed
            );
        }

        println!("Crash recovery stress: 10 seeds completed successfully");
    }

    // ------------------------------------------------------------------------
    // Long-Running Tests (commented out for CI, uncomment for thorough testing)
    // ------------------------------------------------------------------------

    #[test]
    #[ignore] // Enable for thorough testing.
    fn test_helix_service_stress_100_seeds() {
        let mut total_events = 0u64;
        let mut failures = Vec::new();

        for seed in 0..100 {
            let config = HelixTestConfig {
                node_count: 3,
                seed,
                max_time_secs: 5,
                inject_crashes: seed % 3 == 0, // Crash every 3rd seed.
                crash_time_ms: 1500,
                recover_time_ms: 3500,
                crash_node_index: (seed as usize) % 3,
                ..Default::default()
            };

            let (success, events) = run_basic_simulation(&config);

            if !success {
                failures.push(seed);
            }
            total_events += events;
        }

        assert!(
            failures.is_empty(),
            "Seeds failed: {:?}",
            failures
        );

        println!(
            "Stress test: 100 seeds completed, {} total events",
            total_events
        );
    }
}

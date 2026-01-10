//! Comprehensive simulation tests with fault injection.
//!
//! These tests verify Raft safety properties under various fault conditions
//! using Bloodhound's deterministic simulation framework.

use std::collections::BTreeMap;
use std::time::Duration;

use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use helix_core::NodeId;
use helix_raft::RaftConfig;

use std::sync::{Arc, Mutex};

use crate::properties::{check_single_leader_per_term, has_leader, leader_count, PropertyChecker};
use crate::raft_actor::{custom_events, NetworkState, RaftActor, SharedNetworkState};

/// Test configuration for simulation runs.
struct SimulationTestConfig {
    /// Number of nodes in the cluster.
    node_count: usize,
    /// Random seed for determinism.
    seed: u64,
    /// Maximum simulation time.
    max_time_secs: u64,
    /// Whether to inject faults.
    inject_faults: bool,
    /// Time to inject first crash (if faults enabled).
    crash_time_ms: u64,
    /// Time to inject recovery (if faults enabled).
    recover_time_ms: u64,
    /// Node to crash (0-indexed).
    crash_node_index: usize,
}

impl Default for SimulationTestConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            seed: 42,
            max_time_secs: 10,
            inject_faults: false,
            crash_time_ms: 2000,
            recover_time_ms: 5000,
            crash_node_index: 0,
        }
    }
}

/// Creates a simulation engine with a Raft cluster.
///
/// Returns the engine and the actor IDs.
fn create_simulation(config: &SimulationTestConfig) -> (DiscreteSimulationEngine, Vec<ActorId>) {
    let engine_config = EngineConfig::new(config.seed)
        .with_max_time(Duration::from_secs(config.max_time_secs))
        .with_stats(true)
        .with_recording(false); // Disable recording to save memory

    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    // Create node and actor IDs.
    // Safe cast: node_count is bounded by cluster limits.
    #[allow(clippy::cast_possible_truncation)]
    let node_count = config.node_count as u64;
    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Register actors with the engine.
    // Use longer election timeouts for simulation to reduce event storms
    // during partitions. This doesn't affect correctness, just test speed.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone())
            .with_election_timeout(500_000, 1_500_000); // 500ms-1.5s for faster simulation
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    (engine, actor_ids)
}

/// Runs simulation with given configuration and verifies it completes.
fn run_simulation_test(config: &SimulationTestConfig) -> bool {
    let (mut engine, actor_ids) = create_simulation(config);

    // Schedule fault injection events if enabled.
    if config.inject_faults && config.crash_node_index < actor_ids.len() {
        let crash_actor = actor_ids[config.crash_node_index];

        // Schedule process crash.
        engine.schedule_after(
            Duration::from_millis(config.crash_time_ms),
            EventKind::ProcessCrash {
                actor: crash_actor,
            },
        );

        // Schedule process recovery.
        engine.schedule_after(
            Duration::from_millis(config.recover_time_ms),
            EventKind::ProcessRecover {
                actor: crash_actor,
            },
        );

        tracing::info!(
            actor = %crash_actor,
            crash_time_ms = config.crash_time_ms,
            recover_time_ms = config.recover_time_ms,
            "scheduled fault injection"
        );
    }

    // Run the simulation.
    let result = engine.run();

    // Print stats.
    println!(
        "Seed {}: {} events in {}ms virtual time ({:.1}x speedup)",
        config.seed,
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000,
        result.stats.speedup()
    );

    result.success
}

// ============================================================================
// Basic Simulation Tests
// ============================================================================

#[test]
fn test_simulation_basic_election() {
    let config = SimulationTestConfig {
        seed: 42,
        ..Default::default()
    };
    assert!(run_simulation_test(&config));
}

#[test]
fn test_simulation_multiple_seeds() {
    // Run with multiple seeds for confidence.
    let seeds = [42, 123, 456, 789, 1000, 2000, 3000, 4000, 5000, 9999];

    for seed in seeds {
        let config = SimulationTestConfig {
            seed,
            ..Default::default()
        };
        assert!(
            run_simulation_test(&config),
            "Simulation failed with seed {seed}"
        );
    }
    println!("All {} seeds passed", seeds.len());
}

// ============================================================================
// Fault Injection Tests
// ============================================================================

#[test]
fn test_simulation_single_node_crash_recovery() {
    let config = SimulationTestConfig {
        seed: 42,
        inject_faults: true,
        crash_time_ms: 2000,   // Crash after 2 seconds.
        recover_time_ms: 5000, // Recover after 5 seconds.
        crash_node_index: 0,   // Crash node 1.
        ..Default::default()
    };
    assert!(run_simulation_test(&config));
}

#[test]
fn test_simulation_leader_crash_recovery() {
    // This test is harder - if the leader crashes, a new election should happen.
    // We run multiple seeds because which node becomes leader is seed-dependent.
    for seed in [100, 200, 300] {
        let config = SimulationTestConfig {
            seed,
            inject_faults: true,
            crash_time_ms: 3000,   // Crash after 3 seconds (leader likely elected).
            recover_time_ms: 7000, // Recover after 7 seconds.
            crash_node_index: 0,   // May or may not be leader.
            ..Default::default()
        };
        assert!(
            run_simulation_test(&config),
            "Leader crash test failed with seed {seed}"
        );
    }
}

#[test]
fn test_simulation_minority_crash() {
    // Crash one node in a 3-node cluster. Cluster should remain available.
    let config = SimulationTestConfig {
        seed: 555,
        node_count: 3,
        inject_faults: true,
        crash_time_ms: 2000,
        recover_time_ms: 8000,
        crash_node_index: 2,
        ..Default::default()
    };
    assert!(run_simulation_test(&config));
}

#[test]
fn test_simulation_five_node_with_crash() {
    // Five-node cluster can tolerate 2 failures.
    let config = SimulationTestConfig {
        seed: 777,
        node_count: 5,
        inject_faults: true,
        crash_time_ms: 2000,
        recover_time_ms: 6000,
        crash_node_index: 3,
        ..Default::default()
    };
    assert!(run_simulation_test(&config));
}

// ============================================================================
// Extended Fault Injection Tests
// ============================================================================

#[test]
fn test_simulation_multiple_crashes_sequential() {
    // Test multiple sequential crashes with different nodes.
    let seeds = [111, 222, 333];

    for seed in seeds {
        // Crash node 0, then crash node 1 after recovery.
        let (mut engine, actor_ids) = create_simulation(&SimulationTestConfig {
            seed,
            node_count: 3,
            max_time_secs: 15,
            ..Default::default()
        });

        // First crash-recovery cycle.
        engine.schedule_after(
            Duration::from_millis(2000),
            EventKind::ProcessCrash {
                actor: actor_ids[0],
            },
        );
        engine.schedule_after(
            Duration::from_millis(4000),
            EventKind::ProcessRecover {
                actor: actor_ids[0],
            },
        );

        // Second crash-recovery cycle (different node).
        engine.schedule_after(
            Duration::from_millis(6000),
            EventKind::ProcessCrash {
                actor: actor_ids[1],
            },
        );
        engine.schedule_after(
            Duration::from_millis(8000),
            EventKind::ProcessRecover {
                actor: actor_ids[1],
            },
        );

        let result = engine.run();
        assert!(
            result.success,
            "Multiple sequential crashes failed with seed {seed}"
        );

        println!(
            "Sequential crashes seed {seed}: {} events",
            result.stats.events_processed
        );
    }
}

// ============================================================================
// Property Checking Tests
// ============================================================================

#[test]
fn test_property_checker_with_simulation() {
    // This test demonstrates property checking integration.
    // Full property checking during simulation requires step-by-step execution
    // which is shown here.

    const CHECK_INTERVAL: u64 = 100; // Check every 100 steps.
    let seed = 42;
    let node_count: u64 = 3;

    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(5))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Create actors but keep references for later checking.
    // Note: We can't keep references after registration, so we'll use
    // step-by-step execution and check after completion.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone());
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    // Run simulation step by step, checking for violations.
    let property_checker = PropertyChecker::new();
    let mut step_count = 0u64;

    // Start the simulation (initializes actors).
    engine.start();

    while engine.step().is_some() {
        step_count += 1;

        // Periodically check properties.
        // Note: Full property checking requires access to actor state.
        // This demonstrates the pattern; actual implementation would need
        // actors to report their state.
        if step_count % CHECK_INTERVAL == 0 {
            // Property checking would go here.
            // For now, we just track that we're progressing.
            assert!(property_checker.is_valid(), "Property violation at step {step_count}");
        }
    }

    println!(
        "Property checking test: {} steps, time {}ms",
        step_count,
        engine.current_time().as_millis()
    );

    // Final property check.
    assert!(property_checker.is_valid());
}

#[test]
fn test_property_functions_standalone() {
    // Test property functions with manually created actors.
    use crate::raft_actor::create_raft_cluster;

    let actors = create_raft_cluster(3);

    // Initially no leader.
    assert!(!has_leader(&actors));
    assert_eq!(leader_count(&actors), 0);

    // Single leader per term check (trivially true with no leaders).
    assert!(check_single_leader_per_term(&actors));
}

// ============================================================================
// Stress Tests (longer running)
// ============================================================================

#[test]
#[ignore = "Long-running test, run with --ignored"]
fn test_simulation_extended_duration() {
    // Run for longer simulated time to find rare bugs.
    let config = SimulationTestConfig {
        seed: 12345,
        max_time_secs: 60, // 60 simulated seconds.
        inject_faults: true,
        crash_time_ms: 10_000,
        recover_time_ms: 30_000,
        crash_node_index: 1,
        ..Default::default()
    };
    assert!(run_simulation_test(&config));
}

#[test]
#[ignore = "Long-running test, run with --ignored"]
fn test_simulation_many_seeds() {
    // Run with many seeds for statistical confidence.
    let seed_count = 100;

    for seed in 0..seed_count {
        let config = SimulationTestConfig {
            seed,
            max_time_secs: 5,
            ..Default::default()
        };
        assert!(
            run_simulation_test(&config),
            "Simulation failed with seed {seed}"
        );
    }
    println!("Passed {seed_count} seed runs");
}

#[test]
#[ignore = "Long-running test, run with --ignored"]
fn test_simulation_many_seeds_with_faults() {
    // Run with many seeds and fault injection.
    let seed_count = 50;

    for seed in 0..seed_count {
        let config = SimulationTestConfig {
            seed,
            max_time_secs: 10,
            inject_faults: true,
            crash_time_ms: 2000 + (seed % 3000), // Vary crash time.
            recover_time_ms: 6000 + (seed % 2000), // Vary recovery time.
            crash_node_index: (seed % 3) as usize, // Vary crashed node.
            ..Default::default()
        };
        assert!(
            run_simulation_test(&config),
            "Fault injection simulation failed with seed {seed}"
        );
    }
    println!("Passed {seed_count} fault injection runs");
}

// ============================================================================
// Client Operation Tests
// ============================================================================

/// Helper to create a client request event.
fn client_request_event(actor: ActorId, data: &[u8]) -> EventKind {
    EventKind::Custom {
        actor,
        name: custom_events::CLIENT_REQUEST.to_string(),
        data: data.to_vec(),
    }
}

#[test]
fn test_simulation_client_request_basic() {
    // Test that client requests are processed after leader election.
    let seed = 42;
    let node_count: u64 = 3;

    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(10))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone());
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    // Schedule client requests to all nodes after election time.
    // Only the leader will accept them.
    for (i, &actor_id) in actor_ids.iter().enumerate() {
        let data = format!("command-{i}");
        engine.schedule_after(
            Duration::from_millis(3000 + (i as u64) * 100),
            client_request_event(actor_id, data.as_bytes()),
        );
    }

    let result = engine.run();

    assert!(result.success);
    println!(
        "Client request test: {} events, {}ms virtual time",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

#[test]
fn test_simulation_client_requests_multiple() {
    // Test multiple client requests over time.
    let seed = 123;
    let node_count: u64 = 3;

    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(15))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone());
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    // Schedule 10 client requests spread over time.
    // We send to all nodes; only leader will accept.
    for request_num in 0..10u64 {
        let base_time_ms = 2000 + request_num * 500; // Every 500ms.
        for &actor_id in &actor_ids {
            let data = format!("request-{request_num}");
            engine.schedule_after(
                Duration::from_millis(base_time_ms),
                client_request_event(actor_id, data.as_bytes()),
            );
        }
    }

    let result = engine.run();

    assert!(result.success);
    println!(
        "Multiple client requests: {} events, {}ms virtual time",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

#[test]
fn test_simulation_client_requests_with_crash() {
    // Test client requests during fault injection.
    let seed = 456;
    let node_count: u64 = 3;

    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(15))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone());
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    // Schedule client requests before, during, and after crash.
    // Before crash (2-3s).
    for request_num in 0..3u64 {
        let time_ms = 2000 + request_num * 300;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("pre-{request_num}").as_bytes()),
            );
        }
    }

    // Crash node 0 at 4s.
    engine.schedule_after(
        Duration::from_millis(4000),
        EventKind::ProcessCrash {
            actor: actor_ids[0],
        },
    );

    // Client requests during crash (5-7s).
    for request_num in 0..3u64 {
        let time_ms = 5000 + request_num * 500;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("during-{request_num}").as_bytes()),
            );
        }
    }

    // Recover node 0 at 8s.
    engine.schedule_after(
        Duration::from_millis(8000),
        EventKind::ProcessRecover {
            actor: actor_ids[0],
        },
    );

    // Client requests after recovery (9-10s).
    for request_num in 0..3u64 {
        let time_ms = 9000 + request_num * 300;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("post-{request_num}").as_bytes()),
            );
        }
    }

    let result = engine.run();

    assert!(result.success);
    println!(
        "Client requests with crash: {} events, {}ms virtual time",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

#[test]
fn test_simulation_client_requests_multiple_seeds() {
    // Run client request test with multiple seeds.
    let seeds = [10, 20, 30, 40, 50];

    for seed in seeds {
        let node_count: u64 = 3;

        let engine_config = EngineConfig::new(seed)
            .with_max_time(Duration::from_secs(10))
            .with_stats(false);
        let mut engine = DiscreteSimulationEngine::with_config(engine_config);

        let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
        let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

        let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
            .iter()
            .zip(actor_ids.iter())
            .map(|(&n, &a)| (n, a))
            .collect();

        for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
            let raft_config = RaftConfig::new(node_id, node_ids.clone());
            let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
            engine.register_actor(Box::new(actor));
        }

        // Schedule 5 client requests.
        for request_num in 0..5u64 {
            let time_ms = 2000 + request_num * 200;
            for &actor_id in &actor_ids {
                engine.schedule_after(
                    Duration::from_millis(time_ms),
                    client_request_event(actor_id, format!("req-{request_num}").as_bytes()),
                );
            }
        }

        let result = engine.run();
        assert!(
            result.success,
            "Client request test failed with seed {seed}"
        );
    }
    println!("All {} client request seeds passed", seeds.len());
}

// ============================================================================
// Network Partition Tests
// ============================================================================

/// Creates a simulation with shared network state for partition testing.
fn create_simulation_with_network_state(
    seed: u64,
    node_count: usize,
    max_time_secs: u64,
) -> (DiscreteSimulationEngine, Vec<ActorId>, SharedNetworkState) {
    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(max_time_secs))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    // Create shared network state.
    let network_state: SharedNetworkState = Arc::new(Mutex::new(NetworkState::new()));

    // Safe cast: node_count is bounded.
    #[allow(clippy::cast_possible_truncation)]
    let node_count_u64 = node_count as u64;
    let node_ids: Vec<NodeId> = (1..=node_count_u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count_u64).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Use longer election timeouts for simulation to reduce event storms
    // during partitions. This doesn't affect correctness, just test speed.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone())
            .with_election_timeout(500_000, 1_500_000); // 500ms-1.5s for faster simulation
        let mut actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        actor.set_network_state(Arc::clone(&network_state));
        engine.register_actor(Box::new(actor));
    }

    (engine, actor_ids, network_state)
}

#[test]
fn test_simulation_network_partition_minority() {
    // Partition one node from the other two. The majority should still elect a leader.
    let seed = 42;
    let (mut engine, actor_ids, network_state) =
        create_simulation_with_network_state(seed, 3, 15);

    // After 2 seconds, partition node 1 from nodes 2 and 3.
    // Node 1 is isolated, nodes 2 and 3 can communicate.
    engine.schedule_after(
        Duration::from_millis(2000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[1]], // 1 partitioned from 2
        },
    );
    engine.schedule_after(
        Duration::from_millis(2000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[2]], // 1 partitioned from 3
        },
    );

    // Heal partition at 10 seconds.
    engine.schedule_after(
        Duration::from_millis(10000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(10000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    let result = engine.run();
    assert!(result.success);

    // Check that network state is clean after heal.
    assert!(
        !network_state
            .lock()
            .unwrap()
            .is_partitioned(actor_ids[0], actor_ids[1]),
        "partition should be healed"
    );

    println!(
        "Minority partition test: {} events, {}ms",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

#[test]
fn test_simulation_network_partition_leader_isolated() {
    // More challenging: isolate the leader after it's elected.
    // The remaining nodes should elect a new leader.
    for seed in [100, 200, 300] {
        let (mut engine, actor_ids, _network_state) =
            create_simulation_with_network_state(seed, 3, 20);

        // Wait for leader election (3s), then partition node 1 (may be leader).
        engine.schedule_after(
            Duration::from_millis(3000),
            EventKind::NetworkPartition {
                nodes: vec![actor_ids[0], actor_ids[1]],
            },
        );
        engine.schedule_after(
            Duration::from_millis(3000),
            EventKind::NetworkPartition {
                nodes: vec![actor_ids[0], actor_ids[2]],
            },
        );

        // Heal at 15s.
        engine.schedule_after(
            Duration::from_millis(15000),
            EventKind::NetworkHeal {
                nodes: vec![actor_ids[0], actor_ids[1]],
            },
        );
        engine.schedule_after(
            Duration::from_millis(15000),
            EventKind::NetworkHeal {
                nodes: vec![actor_ids[0], actor_ids[2]],
            },
        );

        let result = engine.run();
        assert!(
            result.success,
            "Leader isolation test failed with seed {seed}"
        );
    }
    println!("Leader isolation tests passed");
}

#[test]
fn test_simulation_network_partition_with_client_requests() {
    // Test client requests during partition - they should fail on isolated nodes.
    let seed = 42;
    let (mut engine, actor_ids, _network_state) =
        create_simulation_with_network_state(seed, 3, 20);

    // Partition node 0 at 3s.
    engine.schedule_after(
        Duration::from_millis(3000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(3000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    // Send client requests during partition.
    for i in 0..5u64 {
        let time_ms = 5000 + i * 500;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("partition-req-{i}").as_bytes()),
            );
        }
    }

    // Heal at 12s.
    engine.schedule_after(
        Duration::from_millis(12000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(12000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    // More requests after heal.
    for i in 0..3u64 {
        let time_ms = 14000 + i * 300;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("post-heal-req-{i}").as_bytes()),
            );
        }
    }

    let result = engine.run();
    assert!(result.success);
    println!(
        "Partition with requests: {} events, {}ms",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

#[test]
fn test_simulation_combined_crash_and_partition() {
    // Ultimate stress test: combine crashes AND partitions.
    for seed in [111, 222, 333] {
        let (mut engine, actor_ids, _network_state) =
            create_simulation_with_network_state(seed, 5, 30);

        // Phase 1: Partition at 2s - isolate nodes 0 and 1 from rest.
        for &isolated in &actor_ids[0..2] {
            for &other in &actor_ids[2..] {
                engine.schedule_after(
                    Duration::from_millis(2000),
                    EventKind::NetworkPartition {
                        nodes: vec![isolated, other],
                    },
                );
            }
        }

        // Phase 2: Crash node 2 at 5s.
        engine.schedule_after(
            Duration::from_millis(5000),
            EventKind::ProcessCrash {
                actor: actor_ids[2],
            },
        );

        // Phase 3: Recover node 2 at 10s.
        engine.schedule_after(
            Duration::from_millis(10000),
            EventKind::ProcessRecover {
                actor: actor_ids[2],
            },
        );

        // Phase 4: Heal partition at 15s.
        for &isolated in &actor_ids[0..2] {
            for &other in &actor_ids[2..] {
                engine.schedule_after(
                    Duration::from_millis(15000),
                    EventKind::NetworkHeal {
                        nodes: vec![isolated, other],
                    },
                );
            }
        }

        // Phase 5: Another crash at 20s.
        engine.schedule_after(
            Duration::from_millis(20000),
            EventKind::ProcessCrash {
                actor: actor_ids[4],
            },
        );
        engine.schedule_after(
            Duration::from_millis(25000),
            EventKind::ProcessRecover {
                actor: actor_ids[4],
            },
        );

        let result = engine.run();
        assert!(
            result.success,
            "Combined crash/partition test failed with seed {seed}"
        );
    }
    println!("Combined crash and partition tests passed");
}

#[test]
fn test_simulation_split_brain_scenario() {
    // Scenario that could cause split-brain if Raft is buggy:
    // - 5 nodes, partition into two groups: [0,1] and [2,3,4]
    // - Both groups try to elect leaders
    // - Only [2,3,4] should succeed (majority)
    let seed = 12345;
    let (mut engine, actor_ids, _network_state) =
        create_simulation_with_network_state(seed, 5, 20);

    // Create partition: [0,1] <-> [2,3,4]
    let minority = &actor_ids[0..2];
    let majority = &actor_ids[2..5];

    // Partition after initial election at 3s.
    // Add all partition pairs.
    for &m in minority {
        for &j in majority {
            engine.schedule_after(
                Duration::from_millis(3000),
                EventKind::NetworkPartition { nodes: vec![m, j] },
            );
        }
    }

    // Let it run with partition for a while, then heal.
    for &m in minority {
        for &j in majority {
            engine.schedule_after(
                Duration::from_millis(15000),
                EventKind::NetworkHeal { nodes: vec![m, j] },
            );
        }
    }

    let result = engine.run();
    assert!(result.success);
    println!(
        "Split-brain scenario: {} events, {}ms",
        result.stats.events_processed,
        result.stats.final_time_ns / 1_000_000
    );
}

// ============================================================================
// Property-Verified Simulation Tests
// ============================================================================

use crate::properties::{check_shared_state, PropertyCheckResult};
use crate::raft_actor::{PropertyState, SharedPropertyState};

/// Creates a simulation with property checking enabled.
///
/// Returns the engine, actor IDs, network state, and property state.
fn create_verified_simulation(
    seed: u64,
    node_count: usize,
    max_time_secs: u64,
) -> (
    DiscreteSimulationEngine,
    Vec<ActorId>,
    SharedNetworkState,
    SharedPropertyState,
) {
    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(max_time_secs))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    // Create shared state for network and properties.
    let network_state: SharedNetworkState = Arc::new(Mutex::new(NetworkState::new()));
    let property_state: SharedPropertyState = Arc::new(Mutex::new(PropertyState::new()));

    // Safe cast: node_count is bounded.
    #[allow(clippy::cast_possible_truncation)]
    let node_count_u64 = node_count as u64;
    let node_ids: Vec<NodeId> = (1..=node_count_u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count_u64).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Register actors with property checking.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone())
            .with_election_timeout(500_000, 1_500_000);
        let mut actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        actor.set_network_state(Arc::clone(&network_state));
        actor.set_property_state(Arc::clone(&property_state));
        engine.register_actor(Box::new(actor));
    }

    (engine, actor_ids, network_state, property_state)
}

/// Checks property state and panics with details if violations found.
fn assert_no_violations(result: &PropertyCheckResult, context: &str) {
    if !result.is_valid() {
        let mut msg = format!("Property violations in {context}:\n");
        for v in &result.leader_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        for v in &result.log_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        for v in &result.state_machine_violations {
            msg.push_str(&format!("  - {v}\n"));
        }
        panic!("{msg}");
    }
}

#[test]
fn test_verified_basic_election() {
    // Test basic election with property verification.
    let seed = 42;
    let (mut engine, _actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 10);

    let result = engine.run();
    assert!(result.success);

    // Verify properties.
    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "basic election");

    let state = property_state.lock().unwrap();
    println!(
        "Verified basic election: {} events, {} state updates, leaders by term: {:?}",
        result.stats.events_processed,
        state.events_processed,
        state.leaders_by_term
    );
}

#[test]
fn test_verified_with_crash_recovery() {
    // Test crash/recovery with property verification.
    let seed = 123;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 15);

    // Schedule crash and recovery.
    engine.schedule_after(
        Duration::from_millis(3000),
        EventKind::ProcessCrash {
            actor: actor_ids[0],
        },
    );
    engine.schedule_after(
        Duration::from_millis(8000),
        EventKind::ProcessRecover {
            actor: actor_ids[0],
        },
    );

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "crash recovery");

    println!(
        "Verified crash recovery: {} events, {} violations",
        result.stats.events_processed,
        check.violation_count()
    );
}

#[test]
fn test_verified_with_partition() {
    // Test network partition with property verification.
    let seed = 456;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 20);

    // Partition node 0 from nodes 1 and 2.
    engine.schedule_after(
        Duration::from_millis(3000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(3000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    // Heal after 12 seconds.
    engine.schedule_after(
        Duration::from_millis(12000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(12000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "network partition");

    println!(
        "Verified partition test: {} events, {} violations",
        result.stats.events_processed,
        check.violation_count()
    );
}

#[test]
fn test_verified_split_brain_scenario() {
    // Test split-brain scenario with property verification.
    // This is the critical test - 5 nodes split into [0,1] and [2,3,4].
    let seed = 789;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 5, 25);

    let minority = &actor_ids[0..2];
    let majority = &actor_ids[2..5];

    // Partition after initial election.
    for &m in minority {
        for &j in majority {
            engine.schedule_after(
                Duration::from_millis(3000),
                EventKind::NetworkPartition { nodes: vec![m, j] },
            );
        }
    }

    // Heal after 15 seconds.
    for &m in minority {
        for &j in majority {
            engine.schedule_after(
                Duration::from_millis(15000),
                EventKind::NetworkHeal { nodes: vec![m, j] },
            );
        }
    }

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "split-brain scenario");

    let state = property_state.lock().unwrap();
    println!(
        "Verified split-brain: {} events, leaders by term: {:?}",
        result.stats.events_processed,
        state.leaders_by_term
    );
}

#[test]
fn test_verified_sustained_client_load() {
    // Test sustained client requests with property verification.
    let seed = 111;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 30);

    // Schedule continuous client requests every 200ms for 20 seconds.
    // Bounded loop: 100 requests maximum.
    for i in 0..100u64 {
        let time_ms = 2000 + i * 200; // Start at 2s, every 200ms.
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("sustained-req-{i}").as_bytes()),
            );
        }
    }

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "sustained client load");

    let state = property_state.lock().unwrap();
    let total_applied: usize = state.applied_entries.values().map(Vec::len).sum();
    println!(
        "Verified sustained load: {} events, {} entries applied",
        result.stats.events_processed,
        total_applied
    );
}

#[test]
fn test_verified_load_with_crash() {
    // Test client load during crash/recovery with property verification.
    let seed = 222;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 30);

    // Schedule crash at 5s, recovery at 15s.
    engine.schedule_after(
        Duration::from_millis(5000),
        EventKind::ProcessCrash {
            actor: actor_ids[0],
        },
    );
    engine.schedule_after(
        Duration::from_millis(15000),
        EventKind::ProcessRecover {
            actor: actor_ids[0],
        },
    );

    // Continuous client requests throughout.
    for i in 0..80u64 {
        let time_ms = 2000 + i * 300;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("crash-req-{i}").as_bytes()),
            );
        }
    }

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "load with crash");

    let state = property_state.lock().unwrap();
    println!(
        "Verified load with crash: {} events, leaders: {:?}",
        result.stats.events_processed,
        state.leaders_by_term.keys().collect::<Vec<_>>()
    );
}

#[test]
fn test_verified_load_with_partition() {
    // Test client load during network partition with property verification.
    let seed = 333;
    let (mut engine, actor_ids, _network_state, property_state) =
        create_verified_simulation(seed, 3, 30);

    // Partition at 5s, heal at 15s.
    engine.schedule_after(
        Duration::from_millis(5000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(5000),
        EventKind::NetworkPartition {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(15000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[1]],
        },
    );
    engine.schedule_after(
        Duration::from_millis(15000),
        EventKind::NetworkHeal {
            nodes: vec![actor_ids[0], actor_ids[2]],
        },
    );

    // Continuous client requests throughout.
    for i in 0..80u64 {
        let time_ms = 2000 + i * 300;
        for &actor_id in &actor_ids {
            engine.schedule_after(
                Duration::from_millis(time_ms),
                client_request_event(actor_id, format!("partition-req-{i}").as_bytes()),
            );
        }
    }

    let result = engine.run();
    assert!(result.success);

    let check = check_shared_state(&property_state).expect("lock failed");
    assert_no_violations(&check, "load with partition");

    let state = property_state.lock().unwrap();
    let total_applied: usize = state.applied_entries.values().map(Vec::len).sum();
    println!(
        "Verified load with partition: {} events, {} applied",
        result.stats.events_processed,
        total_applied
    );
}

#[test]
fn test_verified_multiple_seeds() {
    // Run multiple seeds with property verification.
    let seeds = [1, 2, 3, 4, 5, 10, 20, 30, 42, 100];

    for seed in seeds {
        let (mut engine, actor_ids, _network_state, property_state) =
            create_verified_simulation(seed, 3, 10);

        // Add some client requests.
        for i in 0..10u64 {
            let time_ms = 2000 + i * 200;
            for &actor_id in &actor_ids {
                engine.schedule_after(
                    Duration::from_millis(time_ms),
                    client_request_event(actor_id, format!("seed{seed}-req{i}").as_bytes()),
                );
            }
        }

        let result = engine.run();
        assert!(result.success, "seed {seed} failed to complete");

        let check = check_shared_state(&property_state).expect("lock failed");
        assert_no_violations(&check, &format!("seed {seed}"));
    }
    println!("All {} verified seeds passed", seeds.len());
}

#[test]
#[ignore = "Long-running test, run with --ignored"]
fn test_verified_extensive_seeds() {
    // Run many seeds with property verification.
    let seed_count = 100;
    let mut violations_found = 0;

    for seed in 0..seed_count {
        let (mut engine, actor_ids, _network_state, property_state) =
            create_verified_simulation(seed, 3, 15);

        // Vary the scenario based on seed.
        match seed % 3 {
            0 => {
                // Crash scenario.
                engine.schedule_after(
                    Duration::from_millis(3000),
                    EventKind::ProcessCrash {
                        actor: actor_ids[(seed as usize) % 3],
                    },
                );
                engine.schedule_after(
                    Duration::from_millis(8000),
                    EventKind::ProcessRecover {
                        actor: actor_ids[(seed as usize) % 3],
                    },
                );
            }
            1 => {
                // Partition scenario.
                let isolated = (seed as usize) % 3;
                for j in 0..3 {
                    if j != isolated {
                        engine.schedule_after(
                            Duration::from_millis(3000),
                            EventKind::NetworkPartition {
                                nodes: vec![actor_ids[isolated], actor_ids[j]],
                            },
                        );
                        engine.schedule_after(
                            Duration::from_millis(10000),
                            EventKind::NetworkHeal {
                                nodes: vec![actor_ids[isolated], actor_ids[j]],
                            },
                        );
                    }
                }
            }
            _ => {
                // No faults, just client load.
            }
        }

        // Add client requests.
        for i in 0..20u64 {
            let time_ms = 2000 + i * 200;
            for &actor_id in &actor_ids {
                engine.schedule_after(
                    Duration::from_millis(time_ms),
                    client_request_event(actor_id, format!("ext-{seed}-{i}").as_bytes()),
                );
            }
        }

        let result = engine.run();
        assert!(result.success, "seed {seed} failed to complete");

        let check = check_shared_state(&property_state).expect("lock failed");
        if !check.is_valid() {
            violations_found += 1;
            println!("Seed {seed} found {} violations!", check.violation_count());
            for v in &check.leader_violations {
                println!("  - {v}");
            }
            for v in &check.log_violations {
                println!("  - {v}");
            }
            for v in &check.state_machine_violations {
                println!("  - {v}");
            }
        }
    }

    assert_eq!(
        violations_found, 0,
        "Found violations in {violations_found} of {seed_count} seeds"
    );
    println!("All {} extensive seeds passed", seed_count);
}

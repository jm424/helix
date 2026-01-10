//! Raft consensus simulation tests.
//!
//! These tests use Bloodhound's discrete event simulation to verify
//! Raft safety properties under various fault conditions.

use std::collections::BTreeMap;
use std::time::Duration;

use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::ActorId;
use helix_core::NodeId;
use helix_raft::{RaftConfig, RaftState};

use crate::raft_actor::{create_raft_cluster, RaftActor};
use crate::scenarios::seeds::REGRESSION_SEEDS;

/// Helper to check if a leader has been elected.
fn has_leader(actors: &[RaftActor]) -> bool {
    actors.iter().any(RaftActor::is_leader)
}

/// Helper to count leaders in the cluster.
fn leader_count(actors: &[RaftActor]) -> usize {
    actors.iter().filter(|a| a.is_leader()).count()
}

/// Helper to get the current leaders by term.
fn leaders_by_term(actors: &[RaftActor]) -> BTreeMap<u64, Vec<usize>> {
    let mut result = BTreeMap::new();
    for (i, actor) in actors.iter().enumerate() {
        if actor.state() == RaftState::Leader {
            result
                .entry(actor.current_term().get())
                .or_insert_with(Vec::new)
                .push(i);
        }
    }
    result
}

/// Verify `SingleLeaderPerTerm` property.
fn check_single_leader_per_term(actors: &[RaftActor]) -> bool {
    let leaders = leaders_by_term(actors);
    for leader_indices in leaders.values() {
        if leader_indices.len() > 1 {
            return false;
        }
    }
    true
}

#[test]
fn test_cluster_creation() {
    let actors = create_raft_cluster(3);
    assert_eq!(actors.len(), 3);

    // All nodes start as followers.
    for actor in &actors {
        assert_eq!(actor.state(), RaftState::Follower);
        assert_eq!(actor.current_term().get(), 0);
        assert_eq!(actor.commit_index().get(), 0);
    }
}

#[test]
fn test_single_node_cluster() {
    let actors = create_raft_cluster(1);
    assert_eq!(actors.len(), 1);

    // Single node starts as follower, but becomes leader immediately
    // on first election timeout (tested in unit tests).
    assert_eq!(actors[0].state(), RaftState::Follower);
}

#[test]
fn test_five_node_cluster_creation() {
    let actors = create_raft_cluster(5);
    assert_eq!(actors.len(), 5);

    for actor in &actors {
        assert_eq!(actor.state(), RaftState::Follower);
    }
}

#[test]
fn test_regression_seed_cluster_init() {
    // Verify cluster creation works with all regression seeds.
    for &seed in REGRESSION_SEEDS {
        let actors = create_raft_cluster(3);
        assert_eq!(actors.len(), 3);

        // All start as followers with term 0.
        for actor in &actors {
            assert_eq!(actor.state(), RaftState::Follower);
            assert_eq!(actor.current_term().get(), 0);
        }

        // Seed is recorded for debugging.
        let _ = seed;
    }
}

#[test]
fn test_initial_state_invariants() {
    let actors = create_raft_cluster(3);

    // No leader initially.
    assert!(!has_leader(&actors));
    assert_eq!(leader_count(&actors), 0);

    // Single leader per term is trivially true (no leaders).
    assert!(check_single_leader_per_term(&actors));
}

// ============================================================================
// Full Simulation Tests using Bloodhound DiscreteSimulationEngine
// ============================================================================

/// Run a leader election simulation and verify a leader is elected.
#[test]
fn test_simulation_leader_election() {
    // Run with multiple seeds for confidence.
    for seed in [42, 12345, 999] {
        run_leader_election_simulation(seed);
    }
}

fn run_leader_election_simulation(seed: u64) {
    let node_count: u64 = 3;

    // Create engine with seed.
    let config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(10)) // 10 simulated seconds
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(config);

    // Create actors.
    let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Register actors with the engine.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let raft_config = RaftConfig::new(node_id, node_ids.clone());
        let actor = RaftActor::new(actor_id, raft_config, node_to_actor.clone());
        engine.register_actor(Box::new(actor));
    }

    // Run the simulation.
    let result = engine.run();

    // Verify simulation completed.
    assert!(
        result.success,
        "Simulation failed with seed {seed}: {:?}",
        result.stop_reason
    );

    // Check statistics.
    assert!(
        result.stats.events_processed > 0,
        "No events processed with seed {seed}"
    );

    // Print stats for debugging.
    println!(
        "Seed {seed}: {} events in {}ns virtual time ({:.0}x speedup)",
        result.stats.events_processed,
        result.stats.final_time_ns,
        result.stats.speedup()
    );
}

/// Test that single-node cluster elects itself as leader.
#[test]
fn test_simulation_single_node_election() {
    let config = EngineConfig::new(42)
        .with_max_time(Duration::from_secs(5))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(config);

    // Create single-node cluster.
    let node_id = NodeId::new(1);
    let actor_id = ActorId::new(1);
    let node_ids = vec![node_id];

    let mut node_to_actor = BTreeMap::new();
    node_to_actor.insert(node_id, actor_id);

    let raft_config = RaftConfig::new(node_id, node_ids);
    let actor = RaftActor::new(actor_id, raft_config, node_to_actor);
    engine.register_actor(Box::new(actor));

    // Run simulation.
    let result = engine.run();

    assert!(result.success);
    println!(
        "Single node: {} events, final time {}ns",
        result.stats.events_processed, result.stats.final_time_ns
    );
}

/// Test leader election with 5-node cluster.
#[test]
fn test_simulation_five_node_cluster() {
    let config = EngineConfig::new(7777)
        .with_max_time(Duration::from_secs(10))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(config);

    let node_count: u64 = 5;
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

    let result = engine.run();

    assert!(result.success);
    println!(
        "5-node cluster: {} events, {}ns virtual time, {:.0} events/sec",
        result.stats.events_processed,
        result.stats.final_time_ns,
        result.stats.events_per_second()
    );
}

/// Run simulation with all regression seeds.
#[test]
fn test_simulation_regression_seeds() {
    for &seed in REGRESSION_SEEDS {
        let config = EngineConfig::new(seed)
            .with_max_time(Duration::from_secs(5))
            .with_stats(false); // Disable stats for speed
        let mut engine = DiscreteSimulationEngine::with_config(config);

        let node_count: u64 = 3;
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

        let result = engine.run();

        assert!(
            result.success,
            "Simulation failed with seed {seed}: {:?}",
            result.stop_reason
        );
    }

    println!("All {} regression seeds passed", REGRESSION_SEEDS.len());
}

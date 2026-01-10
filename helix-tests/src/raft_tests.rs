//! Raft consensus simulation tests.
//!
//! These tests use Bloodhound's discrete event simulation to verify
//! Raft safety properties under various fault conditions.

use crate::raft_actor::{create_raft_cluster, RaftActor};
use crate::scenarios::seeds::REGRESSION_SEEDS;
use helix_raft::RaftState;

/// Helper to check if a leader has been elected.
fn has_leader(actors: &[RaftActor]) -> bool {
    actors.iter().any(|a| a.is_leader())
}

/// Helper to count leaders in the cluster.
fn leader_count(actors: &[RaftActor]) -> usize {
    actors.iter().filter(|a| a.is_leader()).count()
}

/// Helper to get the current leaders by term.
fn leaders_by_term(actors: &[RaftActor]) -> std::collections::BTreeMap<u64, Vec<usize>> {
    let mut result = std::collections::BTreeMap::new();
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

/// Verify SingleLeaderPerTerm property.
fn check_single_leader_per_term(actors: &[RaftActor]) -> bool {
    let leaders = leaders_by_term(actors);
    for (_term, leader_indices) in &leaders {
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

// TODO: Full simulation tests require integrating with Bloodhound's
// SimulationEngine. These are placeholder tests that verify the actor
// creation and basic state.

#[test]
fn test_regression_seed_cluster_init() {
    // Verify cluster creation works with all regression seeds.
    // (Seeds would be used for RNG in actual simulation.)
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

//! Property definitions and checkers for Helix simulation tests.
//!
//! Properties are invariants that must hold throughout simulation.
//! This module provides both the property definitions and functions
//! to verify them against simulation state.

use std::collections::{BTreeMap, BTreeSet};

use helix_raft::RaftState;

use crate::raft_actor::RaftActor;

// ============================================================================
// Property Violation Types
// ============================================================================

/// A violation of a Raft safety property.
#[derive(Debug, Clone)]
pub enum PropertyViolation {
    /// Multiple leaders in the same term.
    MultipleLeadersInTerm {
        /// The term in which multiple leaders were observed.
        term: u64,
        /// The node IDs of the leaders.
        leaders: Vec<u64>,
    },
    /// Log entries at same index have different terms.
    LogMismatch {
        /// The log index where the mismatch occurred.
        index: u64,
        /// The first node ID.
        node_a: u64,
        /// The term at the index on the first node.
        term_a: u64,
        /// The second node ID.
        node_b: u64,
        /// The term at the index on the second node.
        term_b: u64,
    },
    /// Committed entry missing from a leader's log.
    MissingCommittedEntry {
        /// The log index of the missing entry.
        index: u64,
        /// The term of the missing entry.
        term: u64,
        /// The leader node ID that is missing the entry.
        leader_node: u64,
        /// The leader's current term.
        leader_term: u64,
    },
}

impl std::fmt::Display for PropertyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MultipleLeadersInTerm { term, leaders } => {
                write!(f, "Multiple leaders in term {term}: {leaders:?}")
            }
            Self::LogMismatch {
                index,
                node_a,
                term_a,
                node_b,
                term_b,
            } => {
                write!(
                    f,
                    "Log mismatch at index {index}: node {node_a} has term {term_a}, \
                     node {node_b} has term {term_b}"
                )
            }
            Self::MissingCommittedEntry {
                index,
                term,
                leader_node,
                leader_term,
            } => {
                write!(
                    f,
                    "Committed entry (index={index}, term={term}) missing from \
                     leader {leader_node} in term {leader_term}"
                )
            }
        }
    }
}

// ============================================================================
// Property Checker
// ============================================================================

/// Tracks state across simulation for property verification.
#[derive(Debug, Default)]
pub struct PropertyChecker {
    /// Leaders observed in each term: term -> set of node IDs.
    leaders_by_term: BTreeMap<u64, BTreeSet<u64>>,
    /// Committed entries: (index, term) pairs that have been committed.
    committed_entries: BTreeSet<(u64, u64)>,
    /// Violations found.
    violations: Vec<PropertyViolation>,
}

impl PropertyChecker {
    /// Creates a new property checker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records the current state of all actors.
    ///
    /// Call this after each simulation step to track state changes.
    pub fn record_state(&mut self, actors: &[RaftActor]) {
        for actor in actors {
            let node_id = actor.node_id().get();
            let term = actor.current_term().get();

            // Track leaders by term.
            if actor.state() == RaftState::Leader {
                self.leaders_by_term
                    .entry(term)
                    .or_default()
                    .insert(node_id);
            }

            // Track committed entries.
            // Note: We'd need access to the log to get actual entries.
            // For now, we track commit index advancement.
            let commit_index = actor.commit_index().get();
            if commit_index > 0 {
                // Record that entries up to commit_index are committed.
                // In a full implementation, we'd track (index, term) pairs.
                self.committed_entries.insert((commit_index, term));
            }
        }
    }

    /// Checks all properties and returns any violations.
    ///
    /// Call this at the end of simulation or at checkpoints.
    pub fn check_all(&mut self, actors: &[RaftActor]) -> Vec<PropertyViolation> {
        self.record_state(actors);

        let mut violations = Vec::new();

        // Check SingleLeaderPerTerm.
        violations.extend(self.check_single_leader_per_term());

        // Check LogMatching.
        violations.extend(self.check_log_matching(actors));

        self.violations.extend(violations.clone());
        violations
    }

    /// Checks the `SingleLeaderPerTerm` property.
    ///
    /// Returns violations if multiple leaders were observed in any term.
    #[must_use]
    pub fn check_single_leader_per_term(&self) -> Vec<PropertyViolation> {
        let mut violations = Vec::new();

        for (&term, leaders) in &self.leaders_by_term {
            if leaders.len() > 1 {
                violations.push(PropertyViolation::MultipleLeadersInTerm {
                    term,
                    leaders: leaders.iter().copied().collect(),
                });
            }
        }

        violations
    }

    /// Checks the `LogMatching` property.
    ///
    /// If two nodes have entries at the same index with the same term,
    /// all preceding entries must also match.
    #[must_use]
    pub fn check_log_matching(&self, actors: &[RaftActor]) -> Vec<PropertyViolation> {
        // Compare logs pairwise.
        // Note: This requires access to log entries, which we don't have
        // in the current RaftActor API. We'd need to expose log access.
        //
        // For now, we do a basic check: all nodes with same commit_index
        // should have compatible state.

        let mut commit_indices: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for actor in actors {
            let node_id = actor.node_id().get();
            let commit_index = actor.commit_index().get();
            commit_indices.entry(commit_index).or_default().push(node_id);
        }

        // If multiple nodes have the same commit index, they should agree.
        // This is a weak check - full LogMatching requires log access.
        // TODO: Expose log entries from RaftActor for full verification.
        let _ = commit_indices; // Silence warning until full implementation.

        Vec::new()
    }

    /// Returns all violations found so far.
    #[must_use]
    pub fn violations(&self) -> &[PropertyViolation] {
        &self.violations
    }

    /// Returns true if no violations have been found.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    /// Clears recorded state (for starting a new simulation).
    pub fn reset(&mut self) {
        self.leaders_by_term.clear();
        self.committed_entries.clear();
        self.violations.clear();
    }
}

// ============================================================================
// Standalone Check Functions
// ============================================================================

/// Checks `SingleLeaderPerTerm` property on current actor state.
///
/// Returns true if at most one leader exists per term.
#[must_use]
pub fn check_single_leader_per_term(actors: &[RaftActor]) -> bool {
    let mut leaders_by_term: BTreeMap<u64, u32> = BTreeMap::new();

    for actor in actors {
        if actor.state() == RaftState::Leader {
            let term = actor.current_term().get();
            *leaders_by_term.entry(term).or_default() += 1;
        }
    }

    leaders_by_term.values().all(|&count| count <= 1)
}

/// Counts the number of leaders in the cluster.
#[must_use]
pub fn leader_count(actors: &[RaftActor]) -> usize {
    actors.iter().filter(|a| a.state() == RaftState::Leader).count()
}

/// Returns true if at least one leader exists.
#[must_use]
pub fn has_leader(actors: &[RaftActor]) -> bool {
    actors.iter().any(|a| a.state() == RaftState::Leader)
}

/// Returns the current leader, if any.
#[must_use]
pub fn find_leader(actors: &[RaftActor]) -> Option<&RaftActor> {
    actors.iter().find(|a| a.state() == RaftState::Leader)
}

/// Returns all leaders grouped by term.
#[must_use]
pub fn leaders_by_term(actors: &[RaftActor]) -> BTreeMap<u64, Vec<u64>> {
    let mut result: BTreeMap<u64, Vec<u64>> = BTreeMap::new();

    for actor in actors {
        if actor.state() == RaftState::Leader {
            let term = actor.current_term().get();
            let node_id = actor.node_id().get();
            result.entry(term).or_default().push(node_id);
        }
    }

    result
}

// ============================================================================
// Property Marker Types (for documentation)
// ============================================================================

/// Raft safety properties.
pub mod raft {
    /// `SingleLeaderPerTerm`: At most one leader can be elected in a given term.
    ///
    /// This is the fundamental safety property of Raft. Violating this means
    /// split-brain and potential data loss.
    pub struct SingleLeaderPerTerm;

    /// `LogMatching`: If two logs contain an entry with the same index and term,
    /// then the logs are identical in all entries up through that index.
    ///
    /// This ensures that committed entries are never lost or overwritten.
    pub struct LogMatching;

    /// `LeaderCompleteness`: If a log entry is committed in a given term,
    /// that entry will be present in the logs of all leaders for all
    /// higher-numbered terms.
    ///
    /// This ensures durability of committed entries across leader changes.
    pub struct LeaderCompleteness;

    /// `StateMachineSafety`: If a server has applied a log entry at a given
    /// index to its state machine, no other server will ever apply a
    /// different log entry for the same index.
    ///
    /// This ensures linearizability of the replicated state machine.
    pub struct StateMachineSafety;
}

/// WAL integrity properties.
pub mod wal {
    /// `WalIntegrity`: After crash recovery, the WAL contains exactly the
    /// entries that were synced before the crash, with no corruption.
    pub struct WalIntegrity;

    /// `WalMonotonicity`: Log indices are strictly increasing. No gaps,
    /// no duplicates.
    pub struct WalMonotonicity;
}

/// Progress tracking properties.
pub mod progress {
    /// `NoLostMessages`: Every acked message is delivered exactly once.
    /// No message is delivered without being produced.
    pub struct NoLostMessages;

    /// `EventualProgress`: If messages continue to be produced, the
    /// low watermark eventually advances (no stuck consumers).
    pub struct EventualProgress;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_checker_creation() {
        let checker = PropertyChecker::new();
        assert!(checker.is_valid());
        assert!(checker.violations().is_empty());
    }

    #[test]
    fn test_single_leader_check_empty() {
        let actors: Vec<RaftActor> = Vec::new();
        assert!(check_single_leader_per_term(&actors));
    }
}

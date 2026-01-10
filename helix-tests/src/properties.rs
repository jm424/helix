//! Property definitions and checkers for Helix simulation tests.
//!
//! Properties are invariants that must hold throughout simulation.
//! This module provides both the property definitions and functions
//! to verify them against simulation state.

use std::collections::{BTreeMap, BTreeSet};

use helix_core::LogIndex;
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
    /// Committed entries: index -> term of entry.
    /// Once an entry is committed, it must appear in all future leaders.
    committed_entries: BTreeMap<u64, u64>,
    /// Highest commit index seen across all nodes.
    max_commit_index_seen: u64,
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
            // Skip crashed nodes.
            if actor.is_crashed() {
                continue;
            }

            let node_id = actor.node_id().get();
            let term = actor.current_term().get();

            // Track leaders by term.
            if actor.state() == RaftState::Leader {
                self.leaders_by_term
                    .entry(term)
                    .or_default()
                    .insert(node_id);
            }

            // Track committed entries with their actual entry terms.
            let commit_index = actor.commit_index().get();
            if commit_index > self.max_commit_index_seen {
                // Record new committed entries.
                for idx in (self.max_commit_index_seen + 1)..=commit_index {
                    let entry_term = actor.term_at(LogIndex::new(idx));
                    if entry_term.get() > 0 {
                        self.committed_entries.insert(idx, entry_term.get());
                    }
                }
                self.max_commit_index_seen = commit_index;
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

        // Check LeaderCompleteness.
        violations.extend(self.check_leader_completeness(actors));

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
        let mut violations = Vec::new();

        // Compare logs pairwise (skip crashed nodes).
        let active_actors: Vec<_> = actors.iter().filter(|a| !a.is_crashed()).collect();

        for (i, actor_a) in active_actors.iter().enumerate() {
            for actor_b in active_actors.iter().skip(i + 1) {
                violations.extend(Self::compare_logs(actor_a, actor_b));
            }
        }

        violations
    }

    /// Compares logs of two actors for the `LogMatching` property.
    fn compare_logs(actor_a: &RaftActor, actor_b: &RaftActor) -> Vec<PropertyViolation> {
        let mut violations = Vec::new();

        // Find the overlapping index range.
        let first_a = actor_a.first_log_index().get();
        let first_b = actor_b.first_log_index().get();
        let last_a = actor_a.last_log_index().get();
        let last_b = actor_b.last_log_index().get();

        // No overlap means nothing to compare.
        if first_a == 0 || first_b == 0 {
            return violations;
        }

        let start = first_a.max(first_b);
        let end = last_a.min(last_b);

        if start > end {
            return violations;
        }

        // Check each index in the overlapping range.
        // LogMatching: if entries at same index have same term, all preceding
        // entries must also have same term.
        for idx in start..=end {
            let index = LogIndex::new(idx);
            let term_a = actor_a.term_at(index);
            let term_b = actor_b.term_at(index);

            if term_a.get() != term_b.get() && term_a.get() != 0 && term_b.get() != 0 {
                violations.push(PropertyViolation::LogMismatch {
                    index: idx,
                    node_a: actor_a.node_id().get(),
                    term_a: term_a.get(),
                    node_b: actor_b.node_id().get(),
                    term_b: term_b.get(),
                });
            }
        }

        violations
    }

    /// Checks the `LeaderCompleteness` property.
    ///
    /// If a log entry is committed in a given term, that entry will be present
    /// in the logs of all leaders for all higher-numbered terms.
    #[must_use]
    pub fn check_leader_completeness(&self, actors: &[RaftActor]) -> Vec<PropertyViolation> {
        let mut violations = Vec::new();

        // Find current leaders (non-crashed).
        for actor in actors {
            if actor.is_crashed() || actor.state() != RaftState::Leader {
                continue;
            }

            let leader_term = actor.current_term().get();
            let leader_node = actor.node_id().get();

            // Check that this leader has all committed entries.
            for (&index, &entry_term) in &self.committed_entries {
                let leader_entry_term = actor.term_at(LogIndex::new(index));

                // Leader must have this entry with the same term.
                if leader_entry_term.get() != entry_term {
                    violations.push(PropertyViolation::MissingCommittedEntry {
                        index,
                        term: entry_term,
                        leader_node,
                        leader_term,
                    });
                }
            }
        }

        violations
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
        self.max_commit_index_seen = 0;
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
// Real-Time Property Checking (from SharedPropertyState)
// ============================================================================

use crate::raft_actor::{AppliedEntry, PropertyState, SharedPropertyState};

/// Checks `SingleLeaderPerTerm` from shared property state.
///
/// Returns violations if multiple leaders were observed in any term.
#[must_use]
pub fn check_single_leader_from_state(state: &PropertyState) -> Vec<PropertyViolation> {
    let mut violations = Vec::new();

    for (&term, leaders) in &state.leaders_by_term {
        if leaders.len() > 1 {
            violations.push(PropertyViolation::MultipleLeadersInTerm {
                term,
                leaders: leaders.iter().copied().collect(),
            });
        }
    }

    violations
}

/// Checks `LogMatching` from shared property state.
///
/// Only checks COMMITTED entries - uncommitted entries can legitimately differ
/// during partitions. For each node pair, we check that committed entries
/// (index <= min of both commit indices) have matching terms.
#[must_use]
pub fn check_log_matching_from_state(state: &PropertyState) -> Vec<PropertyViolation> {
    let mut violations = Vec::new();

    // Get active (non-crashed) nodes.
    let active_nodes: Vec<_> = state
        .nodes
        .values()
        .filter(|n| !n.crashed)
        .collect();

    // Compare logs pairwise - only for committed entries.
    for (i, node_a) in active_nodes.iter().enumerate() {
        for node_b in active_nodes.iter().skip(i + 1) {
            // Only check entries that are committed on BOTH nodes.
            let common_commit = node_a.commit_index.min(node_b.commit_index);

            // Skip if neither has committed anything.
            if common_commit == 0 {
                continue;
            }

            // Check entries from 1 to common_commit.
            for idx in 1..=common_commit {
                let term_a = node_a.log_terms.get(&idx).copied().unwrap_or(0);
                let term_b = node_b.log_terms.get(&idx).copied().unwrap_or(0);

                // Only flag if both have the entry (non-zero term) and terms differ.
                if term_a != term_b && term_a != 0 && term_b != 0 {
                    violations.push(PropertyViolation::LogMismatch {
                        index: idx,
                        node_a: node_a.node_id,
                        term_a,
                        node_b: node_b.node_id,
                        term_b,
                    });
                }
            }
        }
    }

    violations
}

/// A `StateMachineSafety` violation.
#[derive(Debug, Clone)]
pub struct StateMachineSafetyViolation {
    /// The log index where different entries were applied.
    pub index: u64,
    /// First node and its applied entry.
    pub node_a: u64,
    /// Term of entry applied by first node.
    pub term_a: u64,
    /// Data hash of entry applied by first node.
    pub hash_a: u64,
    /// Second node and its applied entry.
    pub node_b: u64,
    /// Term of entry applied by second node.
    pub term_b: u64,
    /// Data hash of entry applied by second node.
    pub hash_b: u64,
}

impl std::fmt::Display for StateMachineSafetyViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StateMachineSafety violated at index {}: node {} applied (term={}, hash={:x}), \
             node {} applied (term={}, hash={:x})",
            self.index, self.node_a, self.term_a, self.hash_a,
            self.node_b, self.term_b, self.hash_b
        )
    }
}

/// Checks `StateMachineSafety` from shared property state.
///
/// If a server has applied a log entry at a given index, no other server
/// will ever apply a different log entry for the same index.
#[must_use]
pub fn check_state_machine_safety(state: &PropertyState) -> Vec<StateMachineSafetyViolation> {
    let mut violations = Vec::new();

    // Build map of applied entries: index -> (node_id, entry).
    let mut applied_by_index: BTreeMap<u64, Vec<(u64, &AppliedEntry)>> = BTreeMap::new();

    for (&node_id, entries) in &state.applied_entries {
        for entry in entries {
            applied_by_index
                .entry(entry.index)
                .or_default()
                .push((node_id, entry));
        }
    }

    // Check each index for consistency.
    for (&index, entries) in &applied_by_index {
        if entries.len() < 2 {
            continue;
        }

        // All entries at this index should have same term and data hash.
        let (first_node, first_entry) = entries[0];
        for &(other_node, other_entry) in entries.iter().skip(1) {
            if first_entry.term != other_entry.term
                || first_entry.data_hash != other_entry.data_hash
            {
                violations.push(StateMachineSafetyViolation {
                    index,
                    node_a: first_node,
                    term_a: first_entry.term,
                    hash_a: first_entry.data_hash,
                    node_b: other_node,
                    term_b: other_entry.term,
                    hash_b: other_entry.data_hash,
                });
            }
        }
    }

    violations
}

/// Result of checking all properties.
#[derive(Debug, Default)]
pub struct PropertyCheckResult {
    /// `SingleLeaderPerTerm` violations.
    pub leader_violations: Vec<PropertyViolation>,
    /// `LogMatching` violations.
    pub log_violations: Vec<PropertyViolation>,
    /// `StateMachineSafety` violations.
    pub state_machine_violations: Vec<StateMachineSafetyViolation>,
}

impl PropertyCheckResult {
    /// Returns true if no violations were found.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.leader_violations.is_empty()
            && self.log_violations.is_empty()
            && self.state_machine_violations.is_empty()
    }

    /// Returns the total number of violations.
    #[must_use]
    pub fn violation_count(&self) -> usize {
        self.leader_violations.len()
            + self.log_violations.len()
            + self.state_machine_violations.len()
    }
}

/// Checks all properties from shared property state.
#[must_use]
pub fn check_all_from_state(state: &PropertyState) -> PropertyCheckResult {
    PropertyCheckResult {
        leader_violations: check_single_leader_from_state(state),
        log_violations: check_log_matching_from_state(state),
        state_machine_violations: check_state_machine_safety(state),
    }
}

/// Checks properties from a shared property state handle.
///
/// Returns None if the lock cannot be acquired.
#[must_use]
pub fn check_shared_state(state: &SharedPropertyState) -> Option<PropertyCheckResult> {
    state.lock().ok().map(|s| check_all_from_state(&s))
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

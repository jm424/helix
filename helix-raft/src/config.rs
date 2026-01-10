//! Raft configuration.

use helix_core::NodeId;

use crate::limits::{
    CLUSTER_SIZE_MAX, ELECTION_TICK_DEFAULT, ELECTION_TICK_MAX, ELECTION_TICK_MIN,
    HEARTBEAT_TICK_DEFAULT,
};

/// Configuration for a Raft node.
///
/// # Tick-Based Timing
///
/// Timing is abstracted into "ticks".
/// The application calls `tick()` at regular intervals (e.g., every 100ms),
/// and the library internally tracks elapsed ticks to determine when to
/// trigger elections and heartbeats.
///
/// - `election_tick`: Number of ticks before a follower starts an election.
///   The actual timeout is randomized in `[election_tick, 2*election_tick)`.
/// - `heartbeat_tick`: Number of ticks between leader heartbeats.
///
/// This design enables deterministic simulation testing - just call `tick()`
/// the desired number of times.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID.
    pub node_id: NodeId,

    /// IDs of all nodes in the cluster (including this one).
    pub cluster: Vec<NodeId>,

    /// Number of ticks before election timeout.
    /// Actual timeout is randomized in `[election_tick, 2*election_tick)`.
    /// Must be greater than `heartbeat_tick`.
    pub election_tick: u32,

    /// Number of ticks between leader heartbeats.
    /// Should be significantly smaller than `election_tick`.
    /// Recommendation: `election_tick >= 10 * heartbeat_tick`.
    pub heartbeat_tick: u32,

    /// Random seed for this node (used for election timeout randomization).
    /// Each node should have a different seed.
    pub random_seed: u64,
}

impl RaftConfig {
    /// Creates a new Raft configuration with default tick settings.
    ///
    /// # Panics
    /// Panics if the cluster is empty or too large, or if this node is not in the cluster.
    #[must_use]
    pub fn new(node_id: NodeId, cluster: Vec<NodeId>) -> Self {
        assert!(!cluster.is_empty(), "cluster cannot be empty");
        assert!(
            cluster.len() <= CLUSTER_SIZE_MAX,
            "cluster size {} exceeds maximum {}",
            cluster.len(),
            CLUSTER_SIZE_MAX
        );
        assert!(cluster.contains(&node_id), "node_id must be in cluster");

        Self {
            node_id,
            cluster,
            election_tick: ELECTION_TICK_DEFAULT,
            heartbeat_tick: HEARTBEAT_TICK_DEFAULT,
            // Use node_id as seed by default for determinism.
            random_seed: node_id.get(),
        }
    }

    /// Returns the number of nodes in the cluster.
    #[must_use]
    pub fn cluster_size(&self) -> usize {
        self.cluster.len()
    }

    /// Returns the quorum size (majority).
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.cluster.len() / 2 + 1
    }

    /// Returns the other nodes in the cluster (excluding this node).
    #[must_use]
    pub fn peers(&self) -> Vec<NodeId> {
        self.cluster
            .iter()
            .copied()
            .filter(|&id| id != self.node_id)
            .collect()
    }

    /// Sets custom election and heartbeat tick values.
    ///
    /// # Panics
    /// Panics if `election_tick <= heartbeat_tick` or values are out of range.
    #[must_use]
    pub fn with_tick_config(mut self, election_tick: u32, heartbeat_tick: u32) -> Self {
        assert!(
            election_tick > heartbeat_tick,
            "election_tick must be > heartbeat_tick"
        );
        assert!(
            election_tick >= ELECTION_TICK_MIN,
            "election_tick below minimum"
        );
        assert!(
            election_tick <= ELECTION_TICK_MAX,
            "election_tick above maximum"
        );
        assert!(heartbeat_tick > 0, "heartbeat_tick must be > 0");

        self.election_tick = election_tick;
        self.heartbeat_tick = heartbeat_tick;
        self
    }

    /// Sets the random seed for election timeout randomization.
    #[must_use]
    pub const fn with_random_seed(mut self, seed: u64) -> Self {
        self.random_seed = seed;
        self
    }

    /// Validates the configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.cluster.is_empty() {
            return Err("cluster cannot be empty");
        }
        if self.cluster.len() > CLUSTER_SIZE_MAX {
            return Err("cluster too large");
        }
        if !self.cluster.contains(&self.node_id) {
            return Err("node_id not in cluster");
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err("election_tick must be > heartbeat_tick");
        }
        if self.heartbeat_tick == 0 {
            return Err("heartbeat_tick must be > 0");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_basics() {
        let cluster = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let config = RaftConfig::new(NodeId::new(1), cluster);

        assert_eq!(config.cluster_size(), 3);
        assert_eq!(config.quorum_size(), 2);
        assert_eq!(config.peers().len(), 2);
        assert_eq!(config.election_tick, ELECTION_TICK_DEFAULT);
        assert_eq!(config.heartbeat_tick, HEARTBEAT_TICK_DEFAULT);
    }

    #[test]
    #[should_panic(expected = "cluster cannot be empty")]
    fn test_empty_cluster_panics() {
        let _ = RaftConfig::new(NodeId::new(1), vec![]);
    }

    #[test]
    #[should_panic(expected = "node_id must be in cluster")]
    fn test_node_not_in_cluster_panics() {
        let cluster = vec![NodeId::new(2), NodeId::new(3)];
        let _ = RaftConfig::new(NodeId::new(1), cluster);
    }

    #[test]
    fn test_quorum_sizes() {
        // 1 node: quorum = 1
        let c1 = RaftConfig::new(NodeId::new(1), vec![NodeId::new(1)]);
        assert_eq!(c1.quorum_size(), 1);

        // 3 nodes: quorum = 2
        let c3 = RaftConfig::new(
            NodeId::new(1),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        );
        assert_eq!(c3.quorum_size(), 2);

        // 5 nodes: quorum = 3
        let c5 = RaftConfig::new(
            NodeId::new(1),
            vec![
                NodeId::new(1),
                NodeId::new(2),
                NodeId::new(3),
                NodeId::new(4),
                NodeId::new(5),
            ],
        );
        assert_eq!(c5.quorum_size(), 3);
    }

    #[test]
    fn test_custom_tick_config() {
        let cluster = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let config = RaftConfig::new(NodeId::new(1), cluster).with_tick_config(20, 2);

        assert_eq!(config.election_tick, 20);
        assert_eq!(config.heartbeat_tick, 2);
    }

    #[test]
    #[should_panic(expected = "election_tick must be > heartbeat_tick")]
    fn test_invalid_tick_config_panics() {
        let cluster = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let _ = RaftConfig::new(NodeId::new(1), cluster).with_tick_config(5, 5);
    }
}

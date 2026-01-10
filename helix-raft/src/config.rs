//! Raft configuration.

use helix_core::NodeId;

use crate::limits::{
    CLUSTER_SIZE_MAX, ELECTION_TIMEOUT_US_MAX, ELECTION_TIMEOUT_US_MIN, HEARTBEAT_INTERVAL_US,
};

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's ID.
    pub node_id: NodeId,

    /// IDs of all nodes in the cluster (including this one).
    pub cluster: Vec<NodeId>,

    /// Minimum election timeout in microseconds.
    pub election_timeout_min_us: u64,

    /// Maximum election timeout in microseconds.
    pub election_timeout_max_us: u64,

    /// Heartbeat interval in microseconds.
    pub heartbeat_interval_us: u64,
}

impl RaftConfig {
    /// Creates a new Raft configuration.
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
        assert!(
            cluster.contains(&node_id),
            "node_id must be in cluster"
        );

        Self {
            node_id,
            cluster,
            election_timeout_min_us: ELECTION_TIMEOUT_US_MIN,
            election_timeout_max_us: ELECTION_TIMEOUT_US_MAX,
            heartbeat_interval_us: HEARTBEAT_INTERVAL_US,
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

    /// Sets custom election timeout range.
    ///
    /// # Panics
    /// Panics if min > max or values are out of valid range.
    #[must_use]
    pub fn with_election_timeout(mut self, min_us: u64, max_us: u64) -> Self {
        assert!(min_us <= max_us, "min must be <= max");
        assert!(
            min_us >= ELECTION_TIMEOUT_US_MIN,
            "min_us below minimum allowed"
        );
        assert!(
            max_us <= ELECTION_TIMEOUT_US_MAX,
            "max_us above maximum allowed"
        );

        self.election_timeout_min_us = min_us;
        self.election_timeout_max_us = max_us;
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
        if self.election_timeout_min_us > self.election_timeout_max_us {
            return Err("election timeout min > max");
        }
        if self.heartbeat_interval_us >= self.election_timeout_min_us {
            return Err("heartbeat interval must be less than election timeout");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_basics() {
        let cluster = vec![
            NodeId::new(1),
            NodeId::new(2),
            NodeId::new(3),
        ];
        let config = RaftConfig::new(NodeId::new(1), cluster);

        assert_eq!(config.cluster_size(), 3);
        assert_eq!(config.quorum_size(), 2);
        assert_eq!(config.peers().len(), 2);
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
}

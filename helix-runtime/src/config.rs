//! Runtime configuration.
//!
//! Configuration for the production Raft server.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use helix_core::NodeId;

/// Configuration for a Raft server node.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// This node's unique identifier.
    pub node_id: NodeId,
    /// Address to bind for peer connections.
    pub peer_addr: SocketAddr,
    /// Address to bind for client connections.
    pub client_addr: SocketAddr,
    /// List of peer addresses in the cluster.
    pub peers: Vec<PeerConfig>,
    /// Data directory for WAL and state.
    pub data_dir: PathBuf,
    /// Timing configuration.
    pub timing: TimingConfig,
}

impl ServerConfig {
    /// Creates a new server configuration.
    #[must_use]
    pub fn new(node_id: NodeId, peer_addr: SocketAddr, client_addr: SocketAddr) -> Self {
        Self {
            node_id,
            peer_addr,
            client_addr,
            peers: Vec::new(),
            data_dir: PathBuf::from("data"),
            timing: TimingConfig::default(),
        }
    }

    /// Sets the peer list.
    #[must_use]
    pub fn with_peers(mut self, peers: Vec<PeerConfig>) -> Self {
        self.peers = peers;
        self
    }

    /// Sets the data directory.
    #[must_use]
    pub fn with_data_dir(mut self, dir: PathBuf) -> Self {
        self.data_dir = dir;
        self
    }

    /// Sets the timing configuration.
    #[must_use]
    pub const fn with_timing(mut self, timing: TimingConfig) -> Self {
        self.timing = timing;
        self
    }

    /// Returns all cluster node IDs including self.
    #[must_use]
    pub fn cluster_nodes(&self) -> Vec<NodeId> {
        let mut nodes = vec![self.node_id];
        for peer in &self.peers {
            nodes.push(peer.node_id);
        }
        nodes.sort_by_key(|n| n.get());
        nodes
    }
}

/// Configuration for a peer node.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Peer's node ID.
    pub node_id: NodeId,
    /// Peer's address for connections.
    pub addr: SocketAddr,
}

impl PeerConfig {
    /// Creates a new peer configuration.
    #[must_use]
    pub const fn new(node_id: NodeId, addr: SocketAddr) -> Self {
        Self { node_id, addr }
    }
}

/// Timing configuration for Raft.
///
/// Timing is tick-based:
/// - The runtime calls `tick()` at regular intervals (`tick_interval`)
/// - The Raft library internally tracks elapsed ticks
/// - Elections and heartbeats fire based on tick counts (configured in `RaftConfig`)
#[derive(Debug, Clone)]
pub struct TimingConfig {
    /// Interval at which to call `tick()` on the Raft state machine.
    /// This drives both election timeouts and heartbeats.
    /// Recommended: 100ms (gives ~1-2s election timeout with default tick settings).
    pub tick_interval: Duration,
    /// Request timeout for client operations.
    pub request_timeout: Duration,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(100),
            request_timeout: Duration::from_secs(5),
        }
    }
}

impl TimingConfig {
    /// Creates timing config suitable for testing (faster ticks).
    #[must_use]
    pub const fn fast_for_testing() -> Self {
        Self {
            tick_interval: Duration::from_millis(20),
            request_timeout: Duration::from_secs(1),
        }
    }

    /// Validates the timing configuration.
    ///
    /// # Errors
    /// Returns an error if the configuration is invalid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Tick interval must be > 0.
        if self.tick_interval.is_zero() {
            return Err(ConfigError::InvalidTiming {
                message: "tick_interval must be > 0".to_string(),
            });
        }

        Ok(())
    }
}

/// Configuration errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Invalid timing configuration.
    InvalidTiming {
        /// Error description.
        message: String,
    },
    /// Invalid peer configuration.
    InvalidPeer {
        /// Error description.
        message: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTiming { message } => write!(f, "invalid timing: {message}"),
            Self::InvalidPeer { message } => write!(f, "invalid peer: {message}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_timing_is_valid() {
        let timing = TimingConfig::default();
        assert!(timing.validate().is_ok());
    }

    #[test]
    fn test_fast_timing_is_valid() {
        let timing = TimingConfig::fast_for_testing();
        assert!(timing.validate().is_ok());
    }

    #[test]
    fn test_invalid_zero_tick_interval() {
        let timing = TimingConfig {
            tick_interval: Duration::ZERO,
            ..Default::default()
        };
        assert!(timing.validate().is_err());
    }

    #[test]
    fn test_cluster_nodes() {
        let config = ServerConfig::new(
            NodeId::new(1),
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
        )
        .with_peers(vec![
            PeerConfig::new(NodeId::new(2), "127.0.0.1:8002".parse().unwrap()),
            PeerConfig::new(NodeId::new(3), "127.0.0.1:8003".parse().unwrap()),
        ]);

        let nodes = config.cluster_nodes();
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0], NodeId::new(1));
        assert_eq!(nodes[1], NodeId::new(2));
        assert_eq!(nodes[2], NodeId::new(3));
    }
}

//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Multi-Raft for consensus across multiple
//! partition groups, with configurable partition storage.

/// Handler implementations for the Helix service.
pub mod handlers;
mod tick;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use helix_core::{GroupId, NodeId, Offset, TopicId};
use helix_progress::{ProgressConfig, ProgressManager, SimulatedProgressStore};
use helix_raft::multi::MultiRaft;
use helix_runtime::{PeerInfo, TransportConfig, TransportError, TransportHandle};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{error, info};

use crate::controller::{ControllerState, BROKER_HEARTBEAT_TIMEOUT_MS, CONTROLLER_GROUP_ID};
use crate::group_map::GroupMap;
use crate::partition_storage::ProductionPartitionStorage;

/// Maximum records per write request.
pub const MAX_RECORDS_PER_WRITE: usize = 1000;

/// Maximum bytes per read response.
pub const MAX_BYTES_PER_READ: u32 = 1024 * 1024;

/// Tick interval in milliseconds.
pub const TICK_INTERVAL_MS: u64 = 50;

/// Heartbeat interval in milliseconds (re-exported for DST use).
#[allow(unused_imports)]
pub use tick::HEARTBEAT_INTERVAL_MS;

/// A pending proposal waiting for Raft commit.
///
/// When a client proposes an entry, we track it here. When the entry is
/// committed and applied, we send the result (offset) through the channel.
pub struct PendingProposal {
    /// The Raft log index of the proposed entry.
    pub log_index: helix_core::LogIndex,
    /// Channel to send the result (offset) when the entry is applied.
    pub result_tx: oneshot::Sender<crate::error::ServerResult<Offset>>,
}

/// A pending controller proposal waiting for Raft commit.
///
/// Used for controller commands like `CreateTopic`, `DeleteTopic`, etc.
pub struct PendingControllerProposal {
    /// The Raft log index of the proposed entry.
    pub log_index: helix_core::LogIndex,
    /// Channel to send notification when the entry is committed.
    pub result_tx: oneshot::Sender<crate::error::ServerResult<()>>,
}

/// Topic metadata.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Number of partitions.
    pub partition_count: i32,
}

/// The Helix gRPC service backed by Multi-Raft.
///
/// This provides a Raft-replicated implementation using the Multi-Raft
/// engine for efficient management of many partition groups.
pub struct HelixService {
    /// Cluster ID.
    pub(crate) cluster_id: String,
    /// This node's ID.
    pub(crate) node_id: NodeId,
    /// Multi-Raft engine for consensus.
    pub(crate) multi_raft: Arc<RwLock<MultiRaft>>,
    /// Partition storage indexed by `GroupId`.
    pub(crate) partition_storage: Arc<RwLock<HashMap<GroupId, ProductionPartitionStorage>>>,
    /// Group ID mapping.
    pub(crate) group_map: Arc<RwLock<GroupMap>>,
    /// Topic name to metadata mapping.
    pub(crate) topics: Arc<RwLock<HashMap<String, TopicMetadata>>>,
    /// Next topic ID.
    pub(crate) next_topic_id: Arc<RwLock<u64>>,
    /// All nodes in the cluster.
    pub(crate) cluster_nodes: Vec<NodeId>,
    /// Peer addresses indexed by `NodeId` (for metadata responses).
    pub(crate) peer_addrs: HashMap<NodeId, String>,
    /// Shutdown signal sender.
    pub(crate) _shutdown_tx: mpsc::Sender<()>,
    /// Data directory for durable storage (None = in-memory only).
    pub(crate) data_dir: Option<PathBuf>,
    /// Object storage directory for tiering (None = simulated storage).
    pub(crate) object_storage_dir: Option<PathBuf>,
    /// Transport handle for sending Raft messages (multi-node only).
    #[allow(dead_code)]
    pub(crate) transport_handle: Option<TransportHandle>,
    /// Progress manager for consumer group tracking.
    pub(crate) progress_manager: Arc<ProgressManager<SimulatedProgressStore>>,
    /// Controller state machine (cluster metadata).
    pub(crate) controller_state: Arc<RwLock<ControllerState>>,
    /// Pending proposals waiting for Raft commit (multi-node mode only).
    pub(crate) pending_proposals: Arc<RwLock<HashMap<GroupId, Vec<PendingProposal>>>>,
    /// Pending controller proposals waiting for Raft commit.
    pub(crate) pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    /// Local broker heartbeat timestamps (soft state, not Raft-replicated).
    ///
    /// Following Kafka `KRaft` pattern, heartbeats are maintained as soft state
    /// on each node. Each broker sends heartbeats via transport to all peers,
    /// and each node maintains its own view of broker liveness.
    pub(crate) local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
}

impl HelixService {
    /// Creates a new Helix service with in-memory storage (for testing).
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    #[must_use]
    pub fn new(cluster_id: String, node_id: u64) -> Self {
        Self::new_internal(cluster_id, node_id, None, None)
    }

    /// Creates a new Helix service with durable WAL-backed storage.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    /// Partition data is persisted to the specified directory.
    #[must_use]
    pub fn with_data_dir(cluster_id: String, node_id: u64, data_dir: PathBuf) -> Self {
        Self::new_internal(cluster_id, node_id, Some(data_dir), None)
    }

    /// Creates a new Helix service with durable storage and object storage for tiering.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    /// Partition data is persisted to `data_dir`, and tiered segments are stored
    /// in `object_storage_dir`.
    #[must_use]
    pub fn with_data_and_object_storage(
        cluster_id: String,
        node_id: u64,
        data_dir: PathBuf,
        object_storage_dir: PathBuf,
    ) -> Self {
        Self::new_internal(cluster_id, node_id, Some(data_dir), Some(object_storage_dir))
    }

    /// Internal constructor.
    fn new_internal(
        cluster_id: String,
        node_id: u64,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
    ) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let pending_proposals = Arc::new(RwLock::new(HashMap::new()));

        // Create progress manager with simulated store.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task.
        tokio::spawn(tick::tick_task(
            Arc::clone(&multi_raft),
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
            Arc::clone(&pending_proposals),
            shutdown_rx,
        ));

        Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs: HashMap::new(),
            _shutdown_tx: shutdown_tx,
            data_dir,
            object_storage_dir,
            transport_handle: None,
            progress_manager,
            controller_state: Arc::new(RwLock::new(ControllerState::new())),
            pending_proposals,
            pending_controller_proposals: Arc::new(RwLock::new(Vec::new())),
            local_broker_heartbeats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Creates a new Helix service with multi-node networking.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_multi_node(
        cluster_id: String,
        node_id: u64,
        listen_addr: SocketAddr,
        peers: Vec<PeerInfo>,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        kafka_addr: String,
        kafka_peer_addrs: HashMap<NodeId, String>,
    ) -> Result<Self, TransportError> {
        let node_id = NodeId::new(node_id);

        // Build cluster nodes list (self + peers).
        let mut cluster_nodes = vec![node_id];
        cluster_nodes.extend(peers.iter().map(|p| p.node_id));

        // Create and start transport.
        let mut transport_config = TransportConfig::new(node_id, listen_addr);
        for peer in &peers {
            transport_config = transport_config.with_peer(peer.node_id, peer.addr.clone());
        }

        let (transport, incoming_rx) = helix_runtime::Transport::new(transport_config);
        let transport_handle = transport.start().await?;

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let controller_state = Arc::new(RwLock::new(ControllerState::new()));
        let pending_proposals = Arc::new(RwLock::new(HashMap::new()));
        let pending_controller_proposals = Arc::new(RwLock::new(Vec::new()));
        let local_broker_heartbeats = Arc::new(RwLock::new(HashMap::new()));

        // Create controller partition (group 0) with all cluster nodes.
        {
            let mut mr = multi_raft.write().await;
            if let Err(e) = mr.create_group(CONTROLLER_GROUP_ID, cluster_nodes.clone()) {
                error!(error = %e, "Failed to create controller partition");
            } else {
                info!(
                    group_id = CONTROLLER_GROUP_ID.get(),
                    nodes = ?cluster_nodes.iter().map(|n| n.get()).collect::<Vec<_>>(),
                    "Created controller partition"
                );
            }
        }

        // Create progress manager.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task with transport.
        tokio::spawn(tick::tick_task_multi_node(
            Arc::clone(&multi_raft),
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
            Arc::clone(&controller_state),
            Arc::clone(&pending_proposals),
            Arc::clone(&pending_controller_proposals),
            Arc::clone(&local_broker_heartbeats),
            cluster_nodes.clone(),
            transport_handle.clone(),
            data_dir.clone(),
            object_storage_dir.clone(),
            incoming_rx,
            shutdown_rx,
        ));

        info!(
            node_id = node_id.get(),
            listen_addr = %listen_addr,
            peer_count = peers.len(),
            "Started multi-node Helix service"
        );

        // Build Kafka peer addresses map (includes self).
        let mut peer_addrs = kafka_peer_addrs;
        peer_addrs.insert(node_id, kafka_addr);

        Ok(Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs,
            _shutdown_tx: shutdown_tx,
            data_dir,
            object_storage_dir,
            transport_handle: Some(transport_handle),
            progress_manager,
            controller_state,
            pending_proposals,
            pending_controller_proposals,
            local_broker_heartbeats,
        })
    }

    /// Returns the cluster nodes.
    #[must_use]
    pub fn cluster_nodes(&self) -> &[NodeId] {
        &self.cluster_nodes
    }

    /// Returns live brokers (those with recent heartbeats).
    ///
    /// In multi-node mode, filters out brokers that have missed heartbeats.
    /// In single-node mode, returns all cluster nodes (no heartbeat filtering).
    ///
    /// # Kafka `KRaft` Pattern
    ///
    /// Unlike other controller state, heartbeats are **soft state** (not Raft-replicated).
    /// Each broker sends heartbeats via transport to all peers, and each node maintains
    /// its own local view of broker liveness based on received heartbeats.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn live_brokers(&self) -> Vec<NodeId> {
        // In single-node mode, all brokers are "live".
        if !self.is_multi_node() {
            return self.cluster_nodes.clone();
        }

        // Get current time in milliseconds.
        // Safe truncation: milliseconds won't overflow u64 for ~584 million years.
        #[allow(clippy::cast_possible_truncation)]
        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        // Use local heartbeat soft state (not Raft-replicated controller state).
        let heartbeats = self.local_broker_heartbeats.read().await;

        // If no heartbeats have been recorded yet, assume all brokers are live.
        // This handles initial startup before heartbeats are established.
        if heartbeats.is_empty() {
            return self.cluster_nodes.clone();
        }

        self.cluster_nodes
            .iter()
            .filter(|&node_id| {
                heartbeats
                    .get(node_id)
                    .is_some_and(|&last_heartbeat| {
                        current_time_ms.saturating_sub(last_heartbeat) < BROKER_HEARTBEAT_TIMEOUT_MS
                    })
            })
            .copied()
            .collect()
    }

    /// Returns the cluster ID.
    #[must_use] 
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Returns whether this is a multi-node cluster.
    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.cluster_nodes.len() > 1
    }

    /// Returns the Kafka address for a node.
    pub fn get_node_address(&self, node_id: NodeId) -> Option<&str> {
        self.peer_addrs.get(&node_id).map(String::as_str)
    }

    /// Waits for the controller partition to have a leader.
    ///
    /// This should be called before attempting controller operations like
    /// topic creation in multi-node mode. The method polls until the
    /// controller Raft group has an elected leader or the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout expires before a leader is elected.
    pub async fn wait_for_controller_ready(
        &self,
        timeout: std::time::Duration,
    ) -> crate::error::ServerResult<()> {
        use crate::error::ServerError;

        if !self.is_multi_node() {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = std::time::Duration::from_millis(100);

        loop {
            // Check if controller group has a leader.
            {
                let mr = self.multi_raft.read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    if state.leader_id.is_some() {
                        info!("Controller ready, leader elected");
                        return Ok(());
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(ServerError::Internal {
                    message: "timeout waiting for controller leader election".to_string(),
                });
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

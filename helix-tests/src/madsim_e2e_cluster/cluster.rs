//! E2ECluster struct, configuration, types, startup, and client API.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use helix_core::{NodeId, Offset};
use helix_server::kafka::KafkaHandler;
use helix_server::service::router::PartitionRouter;
use helix_server::service::HelixService;
use helix_wal::{FaultConfig, SimulatedStorage};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::madsim_transport::{
    create_cluster_mailboxes, MadSimNetworkState, MadSimTransport, SharedMadSimNetworkState,
    SharedNodeMailboxes,
};
use crate::properties::{
    check_helix_properties, HelixPropertyCheckResult, SharedHelixPropertyState,
    HelixPropertyState,
};

use super::helpers::{create_test_record_batch, message_bridge_task, split_record_batches};

// ============================================================================
// E2E Cluster Configuration
// ============================================================================

/// Configuration for E2E cluster.
#[derive(Debug, Clone)]
pub struct E2EClusterConfig {
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// Base seed for deterministic execution.
    pub base_seed: u64,
    /// Default storage fault configuration.
    pub storage_faults: FaultConfig,
    /// Mailbox capacity for transport messages.
    pub mailbox_capacity: usize,
    /// Enable durable storage with SharedWalPool (production-like config).
    /// When true, uses SharedWalPool + BufferedWal instead of in-memory storage.
    pub use_durable_storage: bool,
    /// Number of shared WALs in the pool (passed to `HelixService`).
    /// When `None`, uses the server default.
    pub shared_wal_count: Option<u32>,
}

impl Default for E2EClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            base_seed: 42,
            storage_faults: FaultConfig::default(),
            mailbox_capacity: 10000,
            use_durable_storage: true, // Default to production-like config
            shared_wal_count: Some(4), // Default to shared WAL pool with 4 WALs
        }
    }
}

impl E2EClusterConfig {
    /// Creates a config for a cluster with the given number of nodes.
    #[must_use]
    pub fn with_nodes(node_count: usize) -> Self {
        Self {
            node_count,
            ..Default::default()
        }
    }

    /// Sets the base seed.
    #[must_use]
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.base_seed = seed;
        self
    }

    /// Disables durable storage (uses in-memory partition storage instead).
    #[must_use]
    pub fn without_durable_storage(mut self) -> Self {
        self.use_durable_storage = false;
        self
    }

    /// Sets the number of shared WALs in the pool.
    #[must_use]
    pub fn with_shared_wal_count(mut self, count: u32) -> Self {
        self.shared_wal_count = Some(count);
        self
    }

    /// Uses per-partition dedicated WAL instead of shared WAL pool.
    /// Each partition gets its own WAL file — simpler, no pool coordination.
    #[must_use]
    pub fn with_per_partition_wal(mut self) -> Self {
        self.shared_wal_count = None;
        self
    }
}

// ============================================================================
// E2E Node
// ============================================================================

/// Type alias for the service used in E2E tests.
pub type E2EHelixService = HelixService<SimulatedStorage, MadSimTransport>;

/// Type alias for the Kafka handler used in E2E tests.
pub type E2EKafkaHandler = KafkaHandler<SimulatedStorage, MadSimTransport>;

/// A single node in the E2E cluster with its service and infrastructure.
pub struct E2ENode {
    /// The node's ID.
    pub node_id: NodeId,
    /// The real HelixService instance (shared with handler).
    pub service: Arc<E2EHelixService>,
    /// Kafka protocol handler for this node.
    pub handler: Arc<E2EKafkaHandler>,
    /// Storage for fault injection.
    pub storage: SimulatedStorage,
    /// Transport for sending messages.
    pub transport: MadSimTransport,
    /// Shutdown sender to stop background tasks.
    pub(crate) shutdown_tx: mpsc::Sender<()>,
    /// Data directory for durable storage (if enabled).
    #[allow(dead_code)]
    pub(crate) data_dir: Option<std::path::PathBuf>,
    /// Partition router for actor mode (retrieved from service).
    #[allow(dead_code)]
    pub(crate) actor_router: Option<Arc<PartitionRouter>>,
}

impl E2ENode {
    /// Shuts down the node's background tasks.
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(()).await;
    }
}

// ============================================================================
// E2E Cluster
// ============================================================================

/// E2E cluster running REAL `HelixService` instances with background tasks.
///
/// This harness provides:
/// - Real service instances with tick_task, batcher_task running
/// - Client API methods (create_topic, produce, consume)
/// - Fault injection (partitions, crashes, storage faults)
/// - Deterministic execution under MadSim
/// - Property tracking for DST verification (SingleLeaderPerTerm, data integrity)
///
/// # Architecture
///
/// The cluster uses `PartitionActor` instances with `PartitionRouter` for lock-free operation.
pub struct E2ECluster {
    /// Configuration.
    pub(crate) config: E2EClusterConfig,
    /// All nodes in the cluster.
    pub nodes: BTreeMap<NodeId, E2ENode>,
    /// Shared network state for partition simulation.
    pub(crate) network_state: SharedMadSimNetworkState,
    /// Shared mailboxes for inter-node communication (supports restart_node).
    pub(crate) shared_mailboxes: Arc<SharedNodeMailboxes>,
    /// Base data directory for durable storage.
    pub(crate) base_data_dir: Option<std::path::PathBuf>,
    /// All node IDs for convenience.
    pub(crate) node_ids: Vec<NodeId>,
    /// Shared property state for DST verification.
    pub(crate) property_state: SharedHelixPropertyState,
}

impl E2ECluster {
    /// Starts an E2E cluster with the given number of nodes.
    ///
    /// This creates real `HelixService` instances and spawns their background
    /// tasks (tick_task, batcher_task) under MadSim.
    pub async fn start(node_count: usize) -> Self {
        Self::start_with_config(E2EClusterConfig::with_nodes(node_count)).await
    }

    /// Starts an E2E cluster with custom configuration.
    ///
    /// Uses the production `new_multi_node_with_transport` constructor to ensure
    /// E2E tests exercise the exact same code paths as production.
    pub async fn start_with_config(config: E2EClusterConfig) -> Self {
        use helix_core::WriteDurability;

        let node_ids: Vec<NodeId> = (1..=config.node_count)
            .map(|i| {
                #[allow(clippy::cast_possible_truncation)]
                NodeId::new(i as u64)
            })
            .collect();

        // Create shared network state.
        let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

        // Create mailboxes for inter-node communication.
        let (shared_mailboxes, mut receivers) =
            create_cluster_mailboxes(&node_ids, config.mailbox_capacity);

        // Create base data directory for durable storage (if enabled).
        // Use thread ID to avoid collisions when cargo test runs multiple
        // test functions in parallel with the same base_seed.
        let base_data_dir = if config.use_durable_storage {
            let thread_id = format!("{:?}", std::thread::current().id());
            let dir = std::path::PathBuf::from(format!(
                "/tmp/helix-e2e-{}-{}",
                config.base_seed, thread_id
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("Failed to create base data directory");
            Some(dir)
        } else {
            None
        };

        // Create nodes using the production constructor with injected transport.
        let mut nodes = BTreeMap::new();

        for &node_id in &node_ids {
            // Create storage with per-node seed for fault injection.
            let storage_seed = config.base_seed.wrapping_add(node_id.get());
            let storage =
                SimulatedStorage::with_faults(storage_seed, config.storage_faults.clone());

            // Create transport with shared network state and shared mailboxes.
            let transport =
                MadSimTransport::new(node_id, network_state.clone(), shared_mailboxes.clone());

            // Get incoming message receiver for this node.
            let madsim_rx = receivers.remove(&node_id).expect("receiver should exist");

            // Create per-node data directory (if durable storage enabled).
            let node_data_dir = base_data_dir.as_ref().map(|base| {
                let dir = base.join(format!("node-{}", node_id.get()));
                std::fs::create_dir_all(&dir).expect("Failed to create node data directory");
                dir
            });

            // Bridge MadSim messages to helix_runtime format.
            let (incoming_tx, incoming_rx) = mpsc::channel(config.mailbox_capacity);
            tokio::spawn(message_bridge_task(madsim_rx, incoming_tx));

            // Use the production constructor with injected transport.
            // This exercises the exact same wiring as production code.
            let service = HelixService::new_multi_node_with_transport(
                "e2e-cluster".to_string(),
                node_id.get(),
                node_ids.clone(),
                transport.clone(),
                incoming_rx,
                node_data_dir.clone(),
                config.shared_wal_count,
                WriteDurability::Fsync,
                storage.clone(),
            )
            .await;

            // Get actor router from service (if actor mode enabled).
            let actor_router = service.actor_router().cloned();

            // Create shutdown channel for graceful shutdown.
            let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

            // Create Kafka handler for this node.
            let service = Arc::new(service);
            let handler = Arc::new(KafkaHandler::new(
                Arc::clone(&service),
                "127.0.0.1".to_string(),
                9092,
                true, // auto_create_topics
                1,    // auto_create_partitions
            ));

            nodes.insert(
                node_id,
                E2ENode {
                    node_id,
                    service,
                    handler,
                    storage,
                    transport,
                    shutdown_tx,
                    data_dir: node_data_dir,
                    actor_router,
                },
            );
        }

        let property_state = Arc::new(Mutex::new(HelixPropertyState::new()));

        info!(
            node_count = config.node_count,
            base_seed = config.base_seed,
            durable_storage = config.use_durable_storage,
            "E2E cluster started using production constructor"
        );

        Self {
            config,
            nodes,
            network_state,
            shared_mailboxes,
            base_data_dir,
            node_ids,
            property_state,
        }
    }

    // ========================================================================
    // Client API
    // ========================================================================

    /// Creates a topic on the cluster using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (CreateTopicsRequest/CreateTopicsResponse).
    /// For multi-node clusters, the handler coordinates through the controller.
    /// For single-node, it uses direct topic creation.
    ///
    /// The replication factor is automatically set to the cluster size.
    ///
    /// Retries on different nodes up to 5 times.
    pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<(), String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partitions_i32 = partitions as i32;

        // Use cluster size as replication factor.
        // Safe cast: cluster size is expected to be small.
        #[allow(clippy::cast_possible_truncation)]
        let replication_factor = self.node_ids.len() as i16;

        // Get available nodes.
        let available_nodes = self.get_available_nodes();
        let mut current_node_id = available_nodes
            .first()
            .or_else(|| self.node_ids.first())
            .copied();

        let mut last_error = String::new();

        // Retry on different nodes up to 5 times.
        for _attempt in 0..5 {
            let Some(node_id) = current_node_id else {
                return Err("No node available".to_string());
            };

            let Some(node) = self.nodes.get(&node_id) else {
                return Err(format!("Node {} not found", node_id.get()));
            };

            // Use the Kafka handler's create_topic method (Kafka protocol path).
            match node
                .handler
                .create_topic_via_kafka(name, partitions_i32, replication_factor)
                .await
            {
                Ok(()) => return Ok(()),
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) => {
                    // Error code 36 = TOPIC_ALREADY_EXISTS (which is fine).
                    if error_code == 36 {
                        return Ok(());
                    }
                    // Try another node.
                    debug!(
                        node = node_id.get(),
                        error_code,
                        message = %message,
                        "CreateTopics failed, trying another node"
                    );
                    let idx = self
                        .node_ids
                        .iter()
                        .position(|&n| n == node_id)
                        .unwrap_or(0);
                    current_node_id = self.node_ids.get((idx + 1) % self.node_ids.len()).copied();
                    last_error = format!("CreateTopics error: {message}");
                }
                Err(e) => {
                    last_error = format!("create_topic failed: {e}");
                    // Try another node.
                    let idx = self
                        .node_ids
                        .iter()
                        .position(|&n| n == node_id)
                        .unwrap_or(0);
                    current_node_id = self.node_ids.get((idx + 1) % self.node_ids.len()).copied();
                }
            }
        }

        Err(format!(
            "Failed to create topic after retries: {last_error}"
        ))
    }

    /// Produces data to a topic partition using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (ProduceRequest/ProduceResponse):
    /// - Starts with an available (non-partitioned) node
    /// - On NOT_LEADER error, tries the next available node
    /// - Retries up to 5 times trying different nodes
    ///
    /// Note: The data is automatically wrapped in a valid Kafka RecordBatch format.
    pub async fn produce(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
    ) -> Result<Offset, String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        // Convert data to a valid Kafka RecordBatch format.
        // This is required because the Kafka handler parses record_count from the batch header.
        let data_bytes: Bytes = data.into();
        let record_batch = create_test_record_batch(&data_bytes);

        // Get available (non-partitioned, non-crashed) nodes to try first.
        let available_nodes = self.get_available_nodes();
        let starting_node = available_nodes
            .first()
            .or_else(|| self.node_ids.first())
            .copied();

        let mut current_node_id = starting_node;
        let mut tried_nodes = std::collections::BTreeSet::new();
        let mut last_error = String::new();

        // Retry on different nodes up to 5 times.
        for _attempt in 0..5 {
            let Some(node_id) = current_node_id else {
                return Err("No node available".to_string());
            };

            let Some(node) = self.nodes.get(&node_id) else {
                return Err(format!("Node {} not found", node_id.get()));
            };

            tried_nodes.insert(node_id);

            // Use the Kafka handler's produce method (Kafka protocol path).
            match node
                .handler
                .produce(topic, partition_i32, record_batch.clone())
                .await
            {
                Ok(offset) => return Ok(Offset::new(offset)),
                Err(KafkaError::Protocol { error_code, .. }) if error_code == 6 => {
                    // NOT_LEADER_OR_FOLLOWER (error code 6) - try another node.
                    debug!(node = node_id.get(), "Not leader, trying another node");
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied()
                        .or_else(|| {
                            // All available nodes tried, cycle to next.
                            let idx = self
                                .node_ids
                                .iter()
                                .position(|&n| n == node_id)
                                .unwrap_or(0);
                            self.node_ids.get((idx + 1) % self.node_ids.len()).copied()
                        });
                    last_error = "NotLeader".to_string();
                }
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) if error_code == 9 => {
                    // BROKER_NOT_AVAILABLE (error code 9) - server overloaded, try another node.
                    debug!(
                        node = node_id.get(),
                        "Broker not available (overloaded), trying another node"
                    );
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied();
                    last_error = format!("broker_not_available: {message}");
                }
                Err(KafkaError::Protocol {
                    error_code,
                    ref message,
                }) if error_code == 3 || error_code == 5 || error_code == -1 => {
                    // UNKNOWN_TOPIC_OR_PARTITION (3), LEADER_NOT_AVAILABLE (5), or UNKNOWN (-1)
                    // These are retriable - the controller may not have replicated to this node yet,
                    // or there's a transient issue during partition setup.
                    debug!(
                        node = node_id.get(),
                        error_code,
                        "Partition not ready on this node, trying another"
                    );
                    current_node_id = available_nodes
                        .iter()
                        .find(|&&n| !tried_nodes.contains(&n))
                        .copied()
                        .or_else(|| {
                            // All available nodes tried, cycle to next.
                            let idx = self
                                .node_ids
                                .iter()
                                .position(|&n| n == node_id)
                                .unwrap_or(0);
                            self.node_ids.get((idx + 1) % self.node_ids.len()).copied()
                        });
                    last_error = format!("partition_not_ready: {message}");
                }
                Err(e) => {
                    last_error = format!("produce failed: {e}");
                    return Err(last_error);
                }
            }
        }

        Err(format!("Failed to find leader after retries: {last_error}"))
    }

    /// Returns a list of available (non-partitioned, non-crashed) node IDs.
    pub(crate) fn get_available_nodes(&self) -> Vec<NodeId> {
        let state = self.network_state.lock().expect("lock poisoned");
        self.node_ids
            .iter()
            .filter(|&&node_id| !state.is_crashed(node_id))
            .copied()
            .collect()
    }

    /// Checks if a specific node is available (not partitioned from all others, not crashed).
    pub(crate) fn is_node_available(&self, node_id: NodeId) -> bool {
        let state = self.network_state.lock().expect("lock poisoned");
        if state.is_crashed(node_id) {
            return false;
        }
        // Check if this node can reach at least one other node.
        self.node_ids
            .iter()
            .filter(|&&n| n != node_id)
            .any(|&other| !state.is_partitioned(node_id, other))
    }

    /// Consumes data from a topic partition using the Kafka protocol handler.
    ///
    /// This uses the REAL Kafka protocol API (FetchRequest/FetchResponse):
    /// - Reads from the partition leader for consistent data (Kafka default)
    /// - On leader errors, retries after "refreshing" leader selection
    /// - Only falls back to non-leader nodes when no leader is known, mirroring
    ///   how clients probe brokers to refresh metadata
    ///
    /// Returns a `Vec<Bytes>` where each entry is a single RecordBatch.
    pub async fn consume(
        &self,
        topic: &str,
        partition: u32,
        start_offset: u64,
    ) -> Result<Vec<Bytes>, String> {
        use helix_server::kafka::KafkaError;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_possible_wrap)]
        let partition_i32 = partition as i32;

        // Try available (non-crashed) nodes via the Kafka fetch path.
        // The Kafka handler routes through the actor router for data partitions,
        // returning NOT_LEADER if this node isn't the leader. We cycle through
        // nodes just like a real Kafka client's metadata refresh.
        let available = self.get_available_nodes();
        let mut tried = std::collections::BTreeSet::new();

        const MAX_ATTEMPTS: u32 = 10;
        for _attempt in 0..MAX_ATTEMPTS {
            // Pick an untried available node, or cycle through all.
            let node_id = available
                .iter()
                .find(|&&n| !tried.contains(&n))
                .or_else(|| available.first())
                .copied();

            let Some(node_id) = node_id else {
                return Err("No available nodes".to_string());
            };
            tried.insert(node_id);

            let Some(node) = self.nodes.get(&node_id) else {
                continue;
            };

            match node
                .handler
                .fetch(topic, partition_i32, start_offset, 1024 * 1024)
                .await
            {
                Ok(response) => {
                    let mut all_batches = Vec::new();
                    for data in response {
                        all_batches.extend(split_record_batches(&data));
                    }
                    return Ok(all_batches);
                }
                Err(KafkaError::Protocol { error_code, .. })
                    if error_code == 6
                        || error_code == 5
                        || error_code == 3
                        || error_code == 9 =>
                {
                    // Retriable: NOT_LEADER (6), LEADER_NOT_AVAILABLE (5),
                    // UNKNOWN_TOPIC (3), BROKER_NOT_AVAILABLE (9).
                    debug!(
                        node = node_id.get(),
                        error_code,
                        "fetch: retriable error, trying next node"
                    );
                    continue;
                }
                Err(e) => {
                    debug!(node = node_id.get(), error = %e, "fetch failed");
                    continue;
                }
            }
        }

        Err("consume failed after retries on all available nodes".to_string())
    }

    /// Finds the leader node for a topic partition via `MultiRaft` state.
    ///
    /// Note: for actor-mode data partitions, leadership lives in partition actors,
    /// not `MultiRaft`. This method works for controller partitions.
    /// Returns `None` if no leader is known.
    #[allow(dead_code)] // Used in debug logging.
    pub(crate) async fn find_partition_leader(
        &self,
        topic: &str,
        partition: u32,
    ) -> Option<NodeId> {
        use helix_core::PartitionId;
        use helix_raft::RaftState;

        // Safe cast: partition is expected to be small.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));

        // First pass: prefer nodes that report themselves as leader to avoid stale leader hints.
        for node in self.nodes.values() {
            // Skip crashed nodes - they may have stale state.
            if !self.is_node_available(node.node_id) {
                continue;
            }

            // Get topic ID from controller state (multi-node mode) or local topics map.
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                continue;
            };

            // Get the group ID for this partition.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                continue;
            };

            // Check if this node believes it is the leader.
            let mr = node.service.multi_raft().read().await;
            if let Some(state) = mr.group_state(group_id) {
                if state.state == RaftState::Leader {
                    return Some(node.node_id);
                }
            }
        }

        // Second pass: fall back to leader hints if no leader is known locally.
        for node in self.nodes.values() {
            // Skip crashed nodes - they may have stale state.
            if !self.is_node_available(node.node_id) {
                continue;
            }

            // Get topic ID from controller state (multi-node mode) or local topics map.
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                continue;
            };

            // Get the group ID for this partition.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                continue;
            };

            // Check if this node knows the leader.
            let mr = node.service.multi_raft().read().await;
            if let Some(state) = mr.group_state(group_id) {
                // If this node knows who the leader is (and that leader is available), return it.
                if let Some(leader_id) = state.leader_id {
                    if self.is_node_available(leader_id) {
                        return Some(leader_id);
                    }
                }
            }
        }

        None
    }

    // ========================================================================
    // Cluster State
    // ========================================================================

    /// Returns all node IDs.
    #[must_use]
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// Returns a node by ID.
    #[must_use]
    pub fn node(&self, node_id: NodeId) -> Option<&E2ENode> {
        self.nodes.get(&node_id)
    }

    /// Waits for simulated time to pass.
    pub async fn sleep(&self, duration: Duration) {
        madsim::time::sleep(duration).await;
    }

    // ========================================================================
    // Property Tracking for DST Verification
    // ========================================================================

    /// Returns a reference to the shared property state.
    #[must_use]
    pub fn property_state(&self) -> &SharedHelixPropertyState {
        &self.property_state
    }

    /// Produces data with retries until leader is available.
    ///
    /// This is the proper E2E approach - just use the API and retry on transient errors.
    pub async fn produce_with_retry(
        &self,
        topic: &str,
        partition: u32,
        data: impl Into<Bytes> + Clone,
        max_retries: u32,
    ) -> Result<Offset, String> {
        let mut last_error = String::new();
        for attempt in 0..max_retries {
            match self.produce(topic, partition, data.clone()).await {
                Ok(offset) => return Ok(offset),
                Err(e) => {
                    last_error = e;
                    // Short wait for leader election / replication (10ms simulated).
                    self.sleep(Duration::from_millis(10)).await;
                    if attempt % 50 == 0 && attempt > 0 {
                        debug!(attempt, "Produce retry, waiting for leader");
                    }
                }
            }
        }
        Err(format!(
            "produce failed after {max_retries} retries: {last_error}"
        ))
    }

    /// Debug helper: prints leadership and data state for a topic partition.
    #[allow(dead_code)]
    pub async fn debug_partition_state(&self, topic: &str, partition: u32) {
        use helix_core::PartitionId;
        use helix_raft::RaftState;
        use helix_server::controller::CONTROLLER_GROUP_ID;

        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition));

        debug!(topic, partition, "Debug partition state");

        for node in self.nodes.values() {
            let node_id = node.node_id;

            // Get topic metadata from controller state (multi-node mode).
            let topic_id = {
                let state = node.service.controller_state().read().await;
                state.get_topic(topic).map(|info| info.topic_id)
            };

            let Some(topic_id) = topic_id else {
                debug!(
                    node_id = node_id.get(),
                    "Topic not found in controller state"
                );
                continue;
            };

            // Get group ID.
            let group_id = {
                let gm = node.service.group_map().read().await;
                gm.get(topic_id, partition_id)
            };

            let Some(group_id) = group_id else {
                debug!(
                    node_id = node_id.get(),
                    "Partition not found in group_map"
                );
                continue;
            };

            // Get Raft state.
            let (state_str, term, leader_id, commit_idx, log_len) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(group_id) {
                    let state_str = match state.state {
                        RaftState::Follower => "Follower",
                        RaftState::PreCandidate => "PreCandidate",
                        RaftState::Candidate => "Candidate",
                        RaftState::Leader => "LEADER",
                    };
                    (
                        state_str,
                        state.current_term.get(),
                        state.leader_id.map(|n| n.get()),
                        state.commit_index.get(),
                        state.last_log_index.get(),
                    )
                } else {
                    ("NoGroup", 0, None, 0, 0)
                }
            };

            // Get blob count from storage.
            let blob_count = {
                let ps_map = node.service.partition_storage_map().read().await;
                if let Some(ps_lock) = ps_map.get(&group_id) {
                    let ps = ps_lock.read().await;
                    ps.blob_log_end_offset().get() as usize
                } else {
                    0
                }
            };

            // Get replication state from leader's perspective (if this is the leader).
            let repl_info = if state_str == "LEADER" {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(group_id) {
                    // Can't access replication_state directly, but we have commit and log indices.
                    format!(
                        " repl=(commit={}, log={})",
                        state.commit_index.get(),
                        state.last_log_index.get()
                    )
                } else {
                    String::new()
                }
            } else {
                String::new()
            };

            debug!(
                node_id = node_id.get(),
                state = state_str,
                term,
                ?leader_id,
                commit_idx,
                log_len,
                blob_count,
                repl_info,
                "Partition node state"
            );
        }

        // Suppress unused import warning — CONTROLLER_GROUP_ID is used in debug_controller_state
        // but included here for consistency.
        let _ = CONTROLLER_GROUP_ID;
    }

    /// Debug helper: prints controller group state (group 0) for all nodes.
    #[allow(dead_code)]
    pub async fn debug_controller_state(&self) {
        use helix_raft::RaftState;
        use helix_server::controller::CONTROLLER_GROUP_ID;

        debug!("Controller (group 0) state");

        for node in self.nodes.values() {
            let node_id = node.node_id;

            // Get controller group state.
            let (state_str, term, leader_id, commit_idx, log_len) = {
                let mr = node.service.multi_raft().read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    let state_str = match state.state {
                        RaftState::Follower => "Follower",
                        RaftState::PreCandidate => "PreCandidate",
                        RaftState::Candidate => "Candidate",
                        RaftState::Leader => "LEADER",
                    };
                    (
                        state_str,
                        state.current_term.get(),
                        state.leader_id.map(|n| n.get()),
                        state.commit_index.get(),
                        state.last_log_index.get(),
                    )
                } else {
                    ("NoGroup", 0, None, 0, 0)
                }
            };

            debug!(
                node_id = node_id.get(),
                state = state_str,
                term,
                ?leader_id,
                commit_idx,
                log_len,
                "Controller node state"
            );
        }
    }

    /// Debug: prints router partition counts for all nodes.
    #[allow(dead_code)]
    pub async fn debug_print_router_state(&self) {
        for (node_id, node) in &self.nodes {
            if let Some(router) = &node.actor_router {
                let count = router.partition_count().await;
                let group_ids: Vec<u64> =
                    router.group_ids().await.iter().map(|g| g.get()).collect();
                debug!(
                    node_id = node_id.get(),
                    partition_count = count,
                    ?group_ids,
                    "Router state"
                );

                // Query leader state for each partition.
                for group_id in router.group_ids().await {
                    if let Ok(handle) = router.partition(group_id).await {
                        let is_leader = handle.is_leader().await.unwrap_or(false);
                        let leader_id = handle.leader_id().await.unwrap_or(None);
                        debug!(
                            group_id = group_id.get(),
                            is_leader,
                            leader_id = ?leader_id.map(|n| n.get()),
                            "Group state"
                        );
                    }
                }

                // Also check if the service's actor_router has the same partitions.
                let service_has_router = node.service.actor_router().is_some();
                debug!(service_has_router, "Service actor_router");
                if let Some(service_router) = node.service.actor_router() {
                    let service_count = service_router.partition_count().await;
                    let service_groups: Vec<u64> = service_router
                        .group_ids()
                        .await
                        .iter()
                        .map(|g| g.get())
                        .collect();
                    debug!(
                        partition_count = service_count,
                        ?service_groups,
                        "Service router partitions"
                    );
                }
            } else {
                debug!(node_id = node_id.get(), "No router");
            }
        }
    }

    /// Finalizes verification and returns the property check result.
    ///
    /// Call this after `consume_and_verify` has been called for all partitions.
    /// This checks that ALL client-acked offsets were successfully verified.
    #[must_use]
    pub fn finalize_verification(&self) -> HelixPropertyCheckResult {
        // Finalize consumer verification in property state.
        if let Ok(mut state) = self.property_state.lock() {
            state.finalize_consumer_verification();
        }

        // Return the full check result.
        check_helix_properties(&self.property_state).expect("lock should not be poisoned")
    }
}

//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Multi-Raft for consensus across multiple
//! partition groups, with in-memory partition storage.
//!
//! NOTE: This uses temporary in-memory storage. Per the RFC design,
//! storage should use `helix-wal` for durability. See storage.rs.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use helix_core::{ConsumerGroupId, ConsumerId, GroupId, LeaseId, LogIndex, NodeId, Offset, PartitionId, Record, TopicId};
use helix_progress::{
    AckMode, PartitionKey, ProgressConfig, ProgressManager, SimulatedProgressStore,
};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use helix_runtime::{IncomingMessage, PeerInfo, TransportConfig, TransportError, TransportHandle};
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::storage::{
    DurablePartition, DurablePartitionConfig, DurablePartitionError, Partition, PartitionCommand,
    PartitionConfig,
};

use crate::error::{ServerError, ServerResult};
use crate::generated::{
    helix_server::Helix, AckMode as ProtoAckMode, AckRequest, AckResponse, BrokerInfo,
    CreateConsumerGroupRequest, CreateConsumerGroupResponse, ErrorCode, GetCommittedOffsetRequest,
    GetCommittedOffsetResponse, GetMetadataRequest, GetMetadataResponse, GetPartitionInfoRequest,
    GetPartitionInfoResponse, PartitionInfo, PullRequest, PullResponse, ReadRequest, ReadResponse,
    Record as ProtoRecord, RecordWithOffset, TopicInfo, WriteRequest, WriteResponse,
};

/// Maximum records per write request.
const MAX_RECORDS_PER_WRITE: usize = 1000;

/// Maximum bytes per read response.
const MAX_BYTES_PER_READ: u32 = 1024 * 1024;

/// Tick interval in milliseconds.
/// A single tick drives both elections and heartbeats for all groups.
const TICK_INTERVAL_MS: u64 = 50;

/// Topic metadata.
#[derive(Debug, Clone)]
struct TopicMetadata {
    /// Topic ID.
    topic_id: TopicId,
    /// Number of partitions.
    partition_count: i32,
}

/// Inner storage type for a partition.
enum PartitionStorageInner {
    /// In-memory storage (for testing).
    InMemory(Partition),
    /// Durable WAL-backed storage (for production).
    /// Boxed to reduce enum size difference between variants.
    Durable(Box<DurablePartition>),
}

/// Storage for a single partition.
struct PartitionStorage {
    /// Topic ID.
    #[allow(dead_code)]
    topic_id: TopicId,
    /// Partition ID.
    #[allow(dead_code)]
    partition_id: PartitionId,
    /// The underlying partition storage.
    inner: PartitionStorageInner,
    /// Last applied Raft log index.
    last_applied: LogIndex,
}

impl PartitionStorage {
    /// Creates new in-memory partition storage.
    const fn new_in_memory(topic_id: TopicId, partition_id: PartitionId) -> Self {
        let config = PartitionConfig::new(topic_id, partition_id);
        Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::InMemory(Partition::new(config)),
            last_applied: LogIndex::new(0),
        }
    }

    /// Creates new durable partition storage.
    ///
    /// # Errors
    /// Returns an error if the WAL cannot be opened.
    async fn new_durable(
        data_dir: &PathBuf,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Result<Self, DurablePartitionError> {
        let config = DurablePartitionConfig::new(data_dir, topic_id, partition_id);
        let durable = DurablePartition::open(config).await?;
        Ok(Self {
            topic_id,
            partition_id,
            inner: PartitionStorageInner::Durable(Box::new(durable)),
            last_applied: LogIndex::new(0),
        })
    }

    /// Returns the log start offset.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    fn log_start_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_start_offset(),
            PartitionStorageInner::Durable(p) => p.log_start_offset(),
        }
    }

    /// Returns the log end offset.
    fn log_end_offset(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.log_end_offset(),
            PartitionStorageInner::Durable(p) => p.log_end_offset(),
        }
    }

    /// Returns the high watermark.
    #[allow(clippy::missing_const_for_fn)] // Const match not stable yet.
    fn high_watermark(&self) -> Offset {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.high_watermark(),
            PartitionStorageInner::Durable(p) => p.high_watermark(),
        }
    }

    /// Reads records from the partition.
    fn read(&self, start_offset: Offset, max_records: u32) -> ServerResult<Vec<Record>> {
        match &self.inner {
            PartitionStorageInner::InMemory(p) => p.read(start_offset, max_records).map_err(|e| {
                ServerError::Internal {
                    message: format!("failed to read: {e}"),
                }
            }),
            PartitionStorageInner::Durable(p) => p.read(start_offset, max_records).map_err(|e| {
                ServerError::Internal {
                    message: format!("failed to read: {e}"),
                }
            }),
        }
    }

    /// Applies a committed entry to the partition (sync version for in-memory).
    #[allow(dead_code)] // Kept for potential future use; async version used everywhere.
    fn apply_entry_sync(&mut self, index: LogIndex, data: &Bytes) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records } => {
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    // Update high watermark since entry is committed.
                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    partition.set_high_watermark(high_watermark);
                    None
                }
            },
            PartitionStorageInner::Durable(_) => {
                // Durable storage should use apply_entry_async.
                return Err(ServerError::Internal {
                    message: "durable storage requires async apply".to_string(),
                });
            }
        };

        self.last_applied = index;
        Ok(base_offset)
    }

    /// Applies a committed entry to the partition (async version for durable).
    async fn apply_entry_async(
        &mut self,
        index: LogIndex,
        data: &Bytes,
    ) -> ServerResult<Option<Offset>> {
        // Skip if already applied.
        if index <= self.last_applied {
            return Ok(None);
        }

        // Skip empty entries (e.g., no-op entries).
        if data.is_empty() {
            self.last_applied = index;
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| ServerError::Internal {
            message: "failed to decode partition command".to_string(),
        })?;

        let base_offset = match &mut self.inner {
            PartitionStorageInner::InMemory(partition) => match command {
                PartitionCommand::Append { records } => {
                    let offset = partition.append(records).map_err(|e| ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    })?;

                    let new_hwm = partition.log_end_offset();
                    partition.set_high_watermark(new_hwm);

                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset } => {
                    partition.truncate(from_offset).map_err(|e| ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    })?;
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    partition.set_high_watermark(high_watermark);
                    None
                }
            },
            PartitionStorageInner::Durable(partition) => match command {
                PartitionCommand::Append { records } => {
                    let offset = partition.append(records).await.map_err(|e| {
                        ServerError::Internal {
                            message: format!("failed to append: {e}"),
                        }
                    })?;
                    Some(offset)
                }
                PartitionCommand::Truncate { from_offset: _ } => {
                    // TODO: Implement truncate for durable partition.
                    warn!("Truncate not yet implemented for durable partition");
                    None
                }
                PartitionCommand::UpdateHighWatermark { high_watermark } => {
                    partition.set_high_watermark(high_watermark);
                    None
                }
            },
        };

        self.last_applied = index;
        Ok(base_offset)
    }
}

/// Maps between (`TopicId`, `PartitionId`) and `GroupId`.
struct GroupMap {
    /// Forward mapping: (`TopicId`, `PartitionId`) -> `GroupId`.
    by_key: HashMap<(TopicId, PartitionId), GroupId>,
    /// Reverse mapping: `GroupId` -> (`TopicId`, `PartitionId`).
    by_group: HashMap<GroupId, (TopicId, PartitionId)>,
    /// Next group ID to assign.
    next_group_id: u64,
}

impl GroupMap {
    /// Creates a new empty group map.
    fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            by_group: HashMap::new(),
            next_group_id: 1,
        }
    }

    /// Allocates a new group ID for a topic/partition pair.
    fn allocate(&mut self, topic_id: TopicId, partition_id: PartitionId) -> GroupId {
        let key = (topic_id, partition_id);

        // Return existing if already allocated.
        if let Some(&group_id) = self.by_key.get(&key) {
            return group_id;
        }

        // Allocate new group ID.
        let group_id = GroupId::new(self.next_group_id);
        self.next_group_id += 1;

        self.by_key.insert(key, group_id);
        self.by_group.insert(group_id, key);

        group_id
    }

    /// Gets the group ID for a topic/partition pair.
    fn get(&self, topic_id: TopicId, partition_id: PartitionId) -> Option<GroupId> {
        self.by_key.get(&(topic_id, partition_id)).copied()
    }

    /// Gets the topic/partition pair for a group ID.
    fn get_key(&self, group_id: GroupId) -> Option<(TopicId, PartitionId)> {
        self.by_group.get(&group_id).copied()
    }
}

/// The Helix gRPC service backed by Multi-Raft.
///
/// This provides a Raft-replicated implementation using the Multi-Raft
/// engine for efficient management of many partition groups.
pub struct HelixService {
    /// Cluster ID.
    cluster_id: String,
    /// This node's ID.
    node_id: NodeId,
    /// Multi-Raft engine for consensus.
    multi_raft: Arc<RwLock<MultiRaft>>,
    /// Partition storage indexed by `GroupId`.
    partition_storage: Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
    /// Group ID mapping.
    group_map: Arc<RwLock<GroupMap>>,
    /// Topic name to metadata mapping.
    topics: Arc<RwLock<HashMap<String, TopicMetadata>>>,
    /// Next topic ID.
    next_topic_id: Arc<RwLock<u64>>,
    /// All nodes in the cluster.
    cluster_nodes: Vec<NodeId>,
    /// Shutdown signal sender.
    _shutdown_tx: mpsc::Sender<()>,
    /// Data directory for durable storage (None = in-memory only).
    data_dir: Option<PathBuf>,
    /// Transport handle for sending Raft messages (multi-node only).
    /// Kept to prevent the transport from being dropped (handle is cloned to tick task).
    #[allow(dead_code)]
    transport_handle: Option<TransportHandle>,
    /// Progress manager for consumer group tracking.
    progress_manager: Arc<ProgressManager<SimulatedProgressStore>>,
}

impl HelixService {
    /// Creates a new Helix service with in-memory storage (for testing).
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    #[must_use]
    pub fn new(cluster_id: String, node_id: u64) -> Self {
        Self::new_internal(cluster_id, node_id, None)
    }

    /// Creates a new Helix service with durable WAL-backed storage.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    /// Partition data is persisted to the specified directory.
    #[must_use]
    pub fn with_data_dir(cluster_id: String, node_id: u64, data_dir: PathBuf) -> Self {
        Self::new_internal(cluster_id, node_id, Some(data_dir))
    }

    /// Internal constructor.
    fn new_internal(cluster_id: String, node_id: u64, data_dir: Option<PathBuf>) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));

        // Create progress manager with simulated store (use node_id as seed for determinism).
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task.
        tokio::spawn(Self::tick_task(
            Arc::clone(&multi_raft),
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
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
            _shutdown_tx: shutdown_tx,
            data_dir,
            transport_handle: None,
            progress_manager,
        }
    }

    /// Creates a new Helix service with multi-node networking.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `cluster_id` - Cluster identifier
    /// * `node_id` - This node's numeric ID
    /// * `listen_addr` - Address for Raft peer connections
    /// * `peers` - List of other nodes in the cluster
    /// * `data_dir` - Directory for persistent storage (optional for testing)
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    pub async fn new_multi_node(
        cluster_id: String,
        node_id: u64,
        listen_addr: SocketAddr,
        peers: Vec<PeerInfo>,
        data_dir: Option<PathBuf>,
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

        // Create progress manager with simulated store (use node_id as seed for determinism).
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task with transport.
        tokio::spawn(Self::tick_task_multi_node(
            Arc::clone(&multi_raft),
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
            transport_handle.clone(),
            incoming_rx,
            shutdown_rx,
        ));

        info!(
            node_id = node_id.get(),
            listen_addr = %listen_addr,
            peer_count = peers.len(),
            "Started multi-node Helix service"
        );

        Ok(Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            _shutdown_tx: shutdown_tx,
            data_dir,
            transport_handle: Some(transport_handle),
            progress_manager,
        })
    }

    /// Background task to handle Raft ticks for all groups.
    ///
    /// A single tick timer drives elections and heartbeats for all groups.
    /// Each group internally tracks elapsed ticks and triggers actions
    /// when thresholds are reached.
    #[allow(clippy::significant_drop_tightening)]
    async fn tick_task(
        multi_raft: Arc<RwLock<MultiRaft>>,
        partition_storage: Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
        group_map: Arc<RwLock<GroupMap>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut tick_interval =
            tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    debug!("Tick task shutting down");
                    break;
                }
                _ = tick_interval.tick() => {
                    let outputs = {
                        let mut mr = multi_raft.write().await;
                        mr.tick()
                    };
                    Self::process_outputs(
                        &outputs,
                        &partition_storage,
                        &group_map,
                    ).await;
                }
            }
        }
    }

    /// Background task for multi-node operation.
    ///
    /// This handles both incoming Raft messages from peers and periodic ticks.
    /// Messages are sent via the transport handle.
    #[allow(clippy::significant_drop_tightening)]
    async fn tick_task_multi_node(
        multi_raft: Arc<RwLock<MultiRaft>>,
        partition_storage: Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
        group_map: Arc<RwLock<GroupMap>>,
        transport_handle: TransportHandle,
        mut incoming_rx: mpsc::Receiver<IncomingMessage>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut tick_interval =
            tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    debug!("Multi-node tick task shutting down");
                    break;
                }
                _ = tick_interval.tick() => {
                    let outputs = {
                        let mut mr = multi_raft.write().await;
                        mr.tick()
                    };
                    Self::process_outputs_multi_node(
                        &outputs,
                        &partition_storage,
                        &group_map,
                        &transport_handle,
                    ).await;
                }
                Some(incoming) = incoming_rx.recv() => {
                    // Handle incoming messages from peers.
                    let outputs = match incoming {
                        IncomingMessage::Single(_message) => {
                            // Single messages not expected in multi-node mode,
                            // but handle them for backwards compatibility.
                            warn!("Received single message in multi-node mode, expected batch");
                            vec![]
                        }
                        IncomingMessage::Batch(group_messages) => {
                            let mut mr = multi_raft.write().await;
                            let mut all_outputs = Vec::new();
                            for group_msg in group_messages {
                                let outputs = mr.handle_message(
                                    group_msg.group_id,
                                    group_msg.message,
                                );
                                all_outputs.extend(outputs);
                            }
                            all_outputs
                        }
                    };
                    Self::process_outputs_multi_node(
                        &outputs,
                        &partition_storage,
                        &group_map,
                        &transport_handle,
                    ).await;
                }
            }
        }
    }

    /// Processes Multi-Raft outputs with transport for sending messages.
    async fn process_outputs_multi_node(
        outputs: &[MultiRaftOutput],
        partition_storage: &Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
        group_map: &Arc<RwLock<GroupMap>>,
        transport_handle: &TransportHandle,
    ) {
        for output in outputs {
            match output {
                MultiRaftOutput::CommitEntry {
                    group_id,
                    index,
                    data,
                } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };

                    if let Some((topic_id, partition_id)) = key {
                        let mut storage = partition_storage.write().await;
                        if let Some(ps) = storage.get_mut(group_id) {
                            if let Err(e) = ps.apply_entry_async(*index, data).await {
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    error = %e,
                                    "Failed to apply committed entry"
                                );
                            }
                        }
                    }
                }
                MultiRaftOutput::BecameLeader { group_id } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };
                    if let Some((topic_id, partition_id)) = key {
                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = group_id.get(),
                            "Became leader"
                        );
                    }
                }
                MultiRaftOutput::SteppedDown { group_id } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };
                    if let Some((topic_id, partition_id)) = key {
                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = group_id.get(),
                            "Stepped down from leader"
                        );
                    }
                }
                MultiRaftOutput::SendMessages { to, messages } => {
                    // Send messages to peer via transport.
                    if let Err(e) = transport_handle.send_batch(*to, messages.clone()).await {
                        error!(
                            to = to.get(),
                            count = messages.len(),
                            error = %e,
                            "Failed to send messages to peer"
                        );
                    } else {
                        debug!(
                            to = to.get(),
                            count = messages.len(),
                            "Sent messages to peer"
                        );
                    }
                }
            }
        }
    }

    /// Processes Multi-Raft outputs.
    async fn process_outputs(
        outputs: &[MultiRaftOutput],
        partition_storage: &Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
        group_map: &Arc<RwLock<GroupMap>>,
    ) {
        for output in outputs {
            match output {
                MultiRaftOutput::CommitEntry {
                    group_id,
                    index,
                    data,
                } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };

                    if let Some((topic_id, partition_id)) = key {
                        let mut storage = partition_storage.write().await;
                        if let Some(ps) = storage.get_mut(group_id) {
                            if let Err(e) = ps.apply_entry_async(*index, data).await {
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    error = %e,
                                    "Failed to apply committed entry"
                                );
                            }
                        }
                    }
                }
                MultiRaftOutput::BecameLeader { group_id } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };
                    if let Some((topic_id, partition_id)) = key {
                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = group_id.get(),
                            "Became leader"
                        );
                    }
                }
                MultiRaftOutput::SteppedDown { group_id } => {
                    let key = {
                        let gm = group_map.read().await;
                        gm.get_key(*group_id)
                    };
                    if let Some((topic_id, partition_id)) = key {
                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = group_id.get(),
                            "Stepped down from leader"
                        );
                    }
                }
                MultiRaftOutput::SendMessages { to, messages } => {
                    // For multi-node clusters, this is where we'd send
                    // messages over the network. For now (single-node),
                    // we can ignore these.
                    debug!(
                        to = to.get(),
                        count = messages.len(),
                        "Would send messages (single-node, ignoring)"
                    );
                }
            }
        }
    }

    /// Creates a topic with the specified number of partitions.
    ///
    /// # Errors
    /// Returns an error if the topic already exists.
    ///
    /// # Panics
    /// Panics if `partition_count` is not in the range (0, 256].
    #[allow(clippy::significant_drop_tightening)]
    pub async fn create_topic(&self, name: String, partition_count: i32) -> ServerResult<()> {
        assert!(partition_count > 0, "partition_count must be positive");
        assert!(partition_count <= 256, "partition_count exceeds limit");

        let mut topics = self.topics.write().await;
        if topics.contains_key(&name) {
            return Err(ServerError::Internal {
                message: format!("topic already exists: {name}"),
            });
        }

        let mut next_id = self.next_topic_id.write().await;
        let topic_id = TopicId::new(*next_id);
        *next_id += 1;

        // Create partition groups.
        let mut multi_raft = self.multi_raft.write().await;
        let mut group_map = self.group_map.write().await;
        let mut partition_storage = self.partition_storage.write().await;

        for i in 0..partition_count {
            // Safe cast: partition_count is bounded by 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);

            // Allocate group ID for this partition.
            let group_id = group_map.allocate(topic_id, partition_id);

            // Create Raft group (single-node cluster).
            multi_raft
                .create_group(group_id, self.cluster_nodes.clone())
                .map_err(|e| ServerError::Internal {
                    message: format!("failed to create Raft group: {e}"),
                })?;

            // Create partition storage (durable or in-memory based on config).
            let ps = if let Some(data_dir) = &self.data_dir {
                PartitionStorage::new_durable(data_dir, topic_id, partition_id)
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to create durable partition: {e}"),
                    })?
            } else {
                PartitionStorage::new_in_memory(topic_id, partition_id)
            };
            partition_storage.insert(group_id, ps);

            // For single-node cluster, tick until the node becomes leader.
            // With default election_tick=10 and randomized timeout in [10, 20),
            // we need up to 20 ticks to guarantee an election.
            if self.cluster_nodes.len() == 1 {
                for _ in 0..25 {
                    let outputs = multi_raft.tick();

                    // Apply any committed entries.
                    for output in &outputs {
                        if let MultiRaftOutput::CommitEntry {
                            group_id: gid,
                            index,
                            data,
                        } = output
                        {
                            if *gid == group_id {
                                if let Some(ps) = partition_storage.get_mut(&group_id) {
                                    let _ = ps.apply_entry_async(*index, data).await;
                                }
                            }
                        }
                    }

                    // Check if became leader.
                    let is_leader = multi_raft
                        .group_state(group_id)
                        .is_some_and(|s| s.state == RaftState::Leader);

                    if is_leader {
                        break;
                    }
                }
            }
        }

        topics.insert(
            name.clone(),
            TopicMetadata {
                topic_id,
                partition_count,
            },
        );

        info!(topic = %name, partitions = partition_count, "Created topic");
        Ok(())
    }

    /// Gets topic metadata by name.
    async fn get_topic(&self, name: &str) -> Option<TopicMetadata> {
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    /// Converts a proto Record to a core Record.
    #[allow(clippy::option_if_let_else)]
    fn proto_to_record(proto: &ProtoRecord) -> Record {
        let mut record = if let Some(key) = &proto.key {
            Record::with_key(
                Bytes::from(key.clone()),
                Bytes::from(proto.value.clone()),
            )
        } else {
            Record::new(Bytes::from(proto.value.clone()))
        };

        if let Some(ts) = proto.timestamp_ms {
            record = record.with_timestamp(helix_core::Timestamp::from_millis(ts));
        }
        for (k, v) in &proto.headers {
            record = record.with_header(k.clone(), Bytes::from(v.clone()));
        }
        record
    }

    /// Converts a core Record to a proto Record.
    fn record_to_proto(record: &Record) -> ProtoRecord {
        ProtoRecord {
            key: record.key.as_ref().map(|k| k.to_vec()),
            value: record.value.to_vec(),
            timestamp_ms: Some(record.timestamp.as_millis()),
            headers: record
                .headers
                .iter()
                .map(|h| (String::from_utf8_lossy(&h.key).to_string(), h.value.to_vec()))
                .collect(),
        }
    }

    /// Internal write implementation using Multi-Raft.
    #[allow(clippy::significant_drop_tightening)]
    #[allow(clippy::too_many_lines)] // Complex flow with validation, Raft, and storage.
    async fn write_internal(&self, request: WriteRequest) -> ServerResult<WriteResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(
            request.records.len() <= MAX_RECORDS_PER_WRITE,
            "too many records: {} > {}",
            request.records.len(),
            MAX_RECORDS_PER_WRITE
        );

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        let partition_idx = if request.partition < 0 {
            0 // Auto-partition: use round-robin or hash-based assignment.
        } else {
            request.partition
        };

        if partition_idx >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            });
        }

        // Safe cast: partition_idx is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition_idx as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: partition_idx,
                }
            })?
        };

        // Convert proto records to core records.
        let records: Vec<Record> = request.records.iter().map(Self::proto_to_record).collect();

        let record_count = records.len();

        // Get base offset before proposing.
        let base_offset = {
            let storage = self.partition_storage.read().await;
            let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            })?;
            ps.log_end_offset()
        };

        // Check if we're the leader.
        let (is_leader, leader_hint) = {
            let mr = self.multi_raft.read().await;
            let state = mr.group_state(group_id);
            let is_leader = state.as_ref().is_some_and(|s| s.state == RaftState::Leader);
            let leader = state.and_then(|s| s.leader_id);
            (is_leader, leader)
        };

        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: request.topic.clone(),
                partition: partition_idx,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // Encode command and propose to Raft.
        let command = PartitionCommand::Append { records };
        let data = command.encode();

        let outputs = {
            let mut mr = self.multi_raft.write().await;
            mr.propose(group_id, data)
        };

        // Apply any committed entries (single-node: immediate commit).
        if let Some(outputs) = outputs {
            for output in &outputs {
                if let MultiRaftOutput::CommitEntry {
                    group_id: gid,
                    index,
                    data,
                } = output
                {
                    if *gid == group_id {
                        let mut storage = self.partition_storage.write().await;
                        if let Some(ps) = storage.get_mut(&group_id) {
                            ps.apply_entry_async(*index, data)
                                .await
                                .map_err(|e| ServerError::Internal {
                                    message: format!("failed to apply: {e}"),
                                })?;
                        }
                    }
                }
            }
        }

        debug!(
            topic = %request.topic,
            partition = partition_idx,
            base_offset = base_offset.get(),
            count = record_count,
            "Wrote records"
        );

        // Safe cast: record_count is bounded by MAX_RECORDS_PER_WRITE which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u32 = record_count as u32;

        Ok(WriteResponse {
            base_offset: base_offset.get(),
            record_count: record_count_u32,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Internal read implementation.
    #[allow(clippy::significant_drop_tightening)]
    async fn read_internal(&self, request: ReadRequest) -> ServerResult<ReadResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let storage = self.partition_storage.read().await;
        let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
            topic: request.topic.clone(),
            partition: request.partition,
        })?;

        // Check offset bounds.
        let log_start = ps.log_start_offset();
        let log_end = ps.log_end_offset();

        if request.offset < log_start.get() || request.offset > log_end.get() {
            return Err(ServerError::OffsetOutOfRange {
                offset: request.offset,
                start: log_start.get(),
                end: log_end.get(),
            });
        }

        // Read records.
        let max_records = if request.max_records == 0 {
            100
        } else {
            request.max_records
        };
        let max_bytes = if request.max_bytes == 0 {
            MAX_BYTES_PER_READ
        } else {
            request.max_bytes.min(MAX_BYTES_PER_READ)
        };

        let start_offset = Offset::new(request.offset);
        let records = ps.read(start_offset, max_records)?;

        // Convert to proto records with offsets.
        let mut proto_records = Vec::new();
        let mut bytes_read: u32 = 0;
        let mut current_offset = request.offset;

        for record in &records {
            // Estimate record size.
            // Safe cast: record value/key sizes are bounded by limits which fit in u32.
            #[allow(clippy::cast_possible_truncation)]
            let value_size = record.value.len() as u32;
            #[allow(clippy::cast_possible_truncation)]
            let key_size = record.key.as_ref().map_or(0, |k: &Bytes| k.len() as u32);
            let record_size = value_size + key_size + 16; // Overhead.

            if bytes_read + record_size > max_bytes && !proto_records.is_empty() {
                break;
            }

            proto_records.push(RecordWithOffset {
                offset: current_offset,
                record: Some(Self::record_to_proto(record)),
            });
            bytes_read += record_size;
            current_offset += 1;
        }

        let high_watermark = ps.high_watermark();

        debug!(
            topic = %request.topic,
            partition = request.partition,
            offset = request.offset,
            count = proto_records.len(),
            "Read records"
        );

        Ok(ReadResponse {
            records: proto_records,
            high_watermark: high_watermark.get(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Internal metadata implementation.
    async fn get_metadata_internal(
        &self,
        request: GetMetadataRequest,
    ) -> ServerResult<GetMetadataResponse> {
        let topics = self.topics.read().await;
        let multi_raft = self.multi_raft.read().await;
        let group_map = self.group_map.read().await;

        // Filter topics if specific ones requested.
        let topic_infos: Vec<TopicInfo> = if request.topics.is_empty() {
            // Return all topics.
            topics
                .iter()
                .map(|(name, meta)| self.make_topic_info(name, meta, &multi_raft, &group_map))
                .collect()
        } else {
            // Return only requested topics.
            request
                .topics
                .iter()
                .filter_map(|name| {
                    topics
                        .get(name)
                        .map(|meta| self.make_topic_info(name, meta, &multi_raft, &group_map))
                })
                .collect()
        };

        Ok(GetMetadataResponse {
            cluster_id: self.cluster_id.clone(),
            controller_id: self.node_id.get(),
            brokers: vec![BrokerInfo {
                node_id: self.node_id.get(),
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: topic_infos,
        })
    }

    /// Creates topic info from metadata.
    fn make_topic_info(
        &self,
        name: &str,
        meta: &TopicMetadata,
        multi_raft: &MultiRaft,
        group_map: &GroupMap,
    ) -> TopicInfo {
        let mut partitions = Vec::new();

        for i in 0..meta.partition_count {
            // Safe cast: i is in range [0, partition_count) and partition_count <= 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);

            let leader = group_map
                .get(meta.topic_id, partition_id)
                .and_then(|gid| multi_raft.group_state(gid))
                .and_then(|state| state.leader_id)
                .map_or_else(|| self.node_id.get(), NodeId::get);

            partitions.push(PartitionInfo {
                partition: i,
                leader,
                replicas: self.cluster_nodes.iter().map(|n| n.get()).collect(),
                isr: vec![leader], // In-sync replicas.
            });
        }

        TopicInfo {
            name: name.to_string(),
            topic_id: meta.topic_id.get(),
            partitions,
        }
    }

    /// Internal partition info implementation.
    #[allow(clippy::significant_drop_tightening)]
    async fn get_partition_info_internal(
        &self,
        request: GetPartitionInfoRequest,
    ) -> ServerResult<GetPartitionInfoResponse> {
        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition < 0 || request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let multi_raft = self.multi_raft.read().await;
        let storage = self.partition_storage.read().await;

        let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
            topic: request.topic.clone(),
            partition: request.partition,
        })?;

        let leader = multi_raft
            .group_state(group_id)
            .and_then(|state| state.leader_id)
            .map_or_else(|| self.node_id.get(), NodeId::get);

        Ok(GetPartitionInfoResponse {
            partition: Some(PartitionInfo {
                partition: request.partition,
                leader,
                replicas: self.cluster_nodes.iter().map(|n| n.get()).collect(),
                isr: vec![leader],
            }),
            log_start_offset: ps.log_start_offset().get(),
            log_end_offset: ps.log_end_offset().get(),
            high_watermark: ps.high_watermark().get(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    // =========================================================================
    // Consumer Group Internal Methods
    // =========================================================================

    /// Creates a consumer group.
    async fn create_consumer_group_internal(
        &self,
        request: CreateConsumerGroupRequest,
    ) -> ServerResult<CreateConsumerGroupResponse> {
        let group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));

        // Convert proto ack mode to internal ack mode.
        let ack_mode = match ProtoAckMode::try_from(request.ack_mode).unwrap_or(ProtoAckMode::Cumulative) {
            ProtoAckMode::Cumulative => AckMode::Cumulative,
            ProtoAckMode::Individual => AckMode::Individual,
        };

        let current_time_us = Self::current_time_us();

        // Try to get or create the group with the specified ack mode.
        match self
            .progress_manager
            .get_or_create_group_with_mode(group_id, current_time_us, ack_mode)
            .await
        {
            Ok(_state) => Ok(CreateConsumerGroupResponse {
                created: true,
                error_code: ErrorCode::None.into(),
                error_message: None,
            }),
            Err(e) => Err(ServerError::Progress(e)),
        }
    }

    /// Pulls records for a consumer with lease-based delivery.
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::significant_drop_tightening)]
    async fn pull_internal(&self, request: PullRequest) -> ServerResult<PullResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");
        assert!(
            !request.consumer_group_id.is_empty(),
            "consumer_group_id cannot be empty"
        );
        assert!(
            !request.consumer_id.is_empty(),
            "consumer_id cannot be empty"
        );

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        // Get the group ID for this partition.
        let raft_group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));
        let consumer_id = ConsumerId::new(Self::hash_string(&request.consumer_id));

        let current_time_us = Self::current_time_us();

        // Register consumer if not already registered.
        self.progress_manager
            .register_consumer(consumer_group_id, consumer_id, current_time_us)
            .await
            .map_err(ServerError::Progress)?;

        // Determine the starting offset for the lease.
        // Query the committed offset to start from the next uncommitted offset.
        let from_offset = self
            .progress_manager
            .fetch_committed(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?
            .map_or(Offset::new(0), |o| Offset::new(o.get()));

        // Check how many records are available from storage.
        let log_end = {
            let storage = self.partition_storage.read().await;
            let ps = storage.get(&raft_group_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?;
            ps.log_end_offset()
        };

        // Limit max_records to what's actually available.
        let available_records = log_end.get().saturating_sub(from_offset.get());
        // Safe cast: available_records is bounded by partition size which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let available_records_u32 = available_records.min(u64::from(u32::MAX)) as u32;

        let max_records = if request.max_records == 0 {
            100.min(available_records_u32)
        } else {
            request.max_records.min(available_records_u32)
        };

        // Return empty if no records available.
        if max_records == 0 {
            return Ok(PullResponse {
                records: vec![],
                lease_id: 0,
                from_offset: from_offset.get(),
                to_offset: 0,
                lease_expires_at_us: 0,
                has_more: false,
                error_code: ErrorCode::None.into(),
                error_message: None,
            });
        }

        let lease_duration_us = if request.lease_duration_ms == 0 {
            60_000_000 // Default: 1 minute.
        } else {
            u64::from(request.lease_duration_ms) * 1000
        };

        // Lease offsets.
        let lease = self
            .progress_manager
            .lease_offsets(
                consumer_group_id,
                topic_meta.topic_id,
                partition_id,
                consumer_id,
                from_offset,
                max_records,
                lease_duration_us,
                current_time_us,
            )
            .await
            .map_err(ServerError::Progress)?;

        // If no lease was granted (no offsets available), return empty response.
        let Some(lease) = lease else {
            return Ok(PullResponse {
                records: vec![],
                lease_id: 0,
                from_offset: 0,
                to_offset: 0,
                lease_expires_at_us: 0,
                has_more: false,
                error_code: ErrorCode::None.into(),
                error_message: None,
            });
        };

        // Read the actual records from storage.
        let storage = self.partition_storage.read().await;
        let ps = storage.get(&raft_group_id).ok_or_else(|| {
            ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            }
        })?;

        let log_end = ps.log_end_offset();
        let lease_to_offset = lease.to_offset.get().min(log_end.get().saturating_sub(1));

        // Calculate how many records we can actually read.
        let record_count = lease_to_offset.saturating_sub(lease.from_offset.get()) + 1;
        // Safe cast: record_count fits in u32 because it's bounded by max_records.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u32 = record_count.min(u64::from(max_records)) as u32;

        let records = ps.read(lease.from_offset, record_count_u32)?;

        // Convert to proto records with offsets.
        let max_bytes = if request.max_bytes == 0 {
            MAX_BYTES_PER_READ
        } else {
            request.max_bytes.min(MAX_BYTES_PER_READ)
        };

        let mut proto_records = Vec::new();
        let mut bytes_read: u32 = 0;
        let mut current_offset = lease.from_offset.get();

        for record in &records {
            // Estimate record size.
            // Safe cast: record value/key sizes are bounded by limits which fit in u32.
            #[allow(clippy::cast_possible_truncation)]
            let value_size = record.value.len() as u32;
            #[allow(clippy::cast_possible_truncation)]
            let key_size = record.key.as_ref().map_or(0, |k: &Bytes| k.len() as u32);
            let record_size = value_size + key_size + 16; // Overhead.

            if bytes_read + record_size > max_bytes && !proto_records.is_empty() {
                break;
            }

            proto_records.push(RecordWithOffset {
                offset: current_offset,
                record: Some(Self::record_to_proto(record)),
            });
            bytes_read += record_size;
            current_offset += 1;
        }

        let has_more = current_offset <= lease_to_offset || log_end.get() > lease_to_offset + 1;

        debug!(
            topic = %request.topic,
            partition = request.partition,
            consumer_group = %request.consumer_group_id,
            lease_id = lease.lease_id.get(),
            count = proto_records.len(),
            "Pull records"
        );

        Ok(PullResponse {
            records: proto_records,
            lease_id: lease.lease_id.get(),
            from_offset: lease.from_offset.get(),
            to_offset: lease_to_offset,
            lease_expires_at_us: lease.expires_at_us,
            has_more,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Acknowledges, nacks, or extends leased records.
    #[allow(clippy::too_many_lines)]
    async fn ack_internal(&self, request: AckRequest) -> ServerResult<AckResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(request.partition >= 0, "partition must be non-negative");
        assert!(
            !request.consumer_group_id.is_empty(),
            "consumer_group_id cannot be empty"
        );
        assert!(
            !request.consumer_id.is_empty(),
            "consumer_id cannot be empty"
        );

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));
        let consumer_id = ConsumerId::new(Self::hash_string(&request.consumer_id));

        let current_time_us = Self::current_time_us();

        let mut acked_count: u32 = 0;
        let mut nacked_count: u32 = 0;
        let mut extended_count: u32 = 0;

        // Process acks: commit the lease's offsets.
        for lease_id in &request.ack_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .commit_lease(
                    consumer_group_id,
                    topic_meta.topic_id,
                    partition_id,
                    consumer_id,
                    lease_id,
                    current_time_us,
                )
                .await
            {
                Ok(()) => acked_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to commit lease");
                }
            }
        }

        // Process nacks: release the lease for redelivery.
        let partition_key = PartitionKey::new(topic_meta.topic_id, partition_id);
        for lease_id in &request.nack_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .release_lease(consumer_group_id, partition_key, consumer_id, lease_id)
                .await
            {
                Ok(()) => nacked_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to release lease");
                }
            }
        }

        // Process extends: renew the lease duration.
        let extend_duration_us = if request.extend_duration_ms == 0 {
            60_000_000 // Default: 1 minute.
        } else {
            u64::from(request.extend_duration_ms) * 1000
        };

        for lease_id in &request.extend_lease_ids {
            let lease_id = LeaseId::new(*lease_id);
            match self
                .progress_manager
                .extend_lease(
                    consumer_group_id,
                    partition_key,
                    consumer_id,
                    lease_id,
                    extend_duration_us,
                    current_time_us,
                )
                .await
            {
                Ok(_) => extended_count += 1,
                Err(e) => {
                    warn!(lease_id = lease_id.get(), error = %e, "Failed to extend lease");
                }
            }
        }

        // Get the current low watermark.
        let low_watermark = self
            .progress_manager
            .get_low_watermark(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?
            .map_or(0, Offset::get);

        debug!(
            topic = %request.topic,
            partition = request.partition,
            consumer_group = %request.consumer_group_id,
            acked = acked_count,
            nacked = nacked_count,
            extended = extended_count,
            low_watermark,
            "Ack processed"
        );

        Ok(AckResponse {
            acked_count,
            nacked_count,
            extended_count,
            low_watermark,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Gets the committed offset for a consumer group.
    async fn get_committed_offset_internal(
        &self,
        request: GetCommittedOffsetRequest,
    ) -> ServerResult<GetCommittedOffsetResponse> {
        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition < 0 || request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        let consumer_group_id = ConsumerGroupId::new(Self::hash_string(&request.consumer_group_id));

        let low_watermark = self
            .progress_manager
            .get_low_watermark(consumer_group_id, topic_meta.topic_id, partition_id)
            .await
            .map_err(ServerError::Progress)?;

        Ok(GetCommittedOffsetResponse {
            low_watermark: low_watermark.map_or(0, Offset::get),
            has_committed: low_watermark.is_some(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }

    /// Hashes a string to produce a deterministic u64 ID.
    fn hash_string(s: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    /// Returns the current time in microseconds.
    fn current_time_us() -> u64 {
        // Safe cast: u64 microseconds can represent ~584,942 years from UNIX epoch.
        // We won't overflow for any reasonable time value.
        #[allow(clippy::cast_possible_truncation)]
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() as u64)
    }
}

#[tonic::async_trait]
impl Helix for HelixService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let inner = request.into_inner();
        debug!(topic = %inner.topic, records = inner.records.len(), "Write request");

        match self.write_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Write failed");
                Ok(Response::new(WriteResponse {
                    base_offset: 0,
                    record_count: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            offset = inner.offset,
            "Read request"
        );

        match self.read_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Read failed");
                Ok(Response::new(ReadResponse {
                    records: vec![],
                    high_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let inner = request.into_inner();
        debug!(topics = ?inner.topics, "GetMetadata request");

        match self.get_metadata_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetMetadata failed");
                Err(Status::internal(e.message()))
            }
        }
    }

    async fn get_partition_info(
        &self,
        request: Request<GetPartitionInfoRequest>,
    ) -> Result<Response<GetPartitionInfoResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            "GetPartitionInfo request"
        );

        match self.get_partition_info_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetPartitionInfo failed");
                Ok(Response::new(GetPartitionInfoResponse {
                    partition: None,
                    log_start_offset: 0,
                    log_end_offset: 0,
                    high_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            consumer = %inner.consumer_id,
            "Pull request"
        );

        match self.pull_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Pull failed");
                Ok(Response::new(PullResponse {
                    records: vec![],
                    lease_id: 0,
                    from_offset: 0,
                    to_offset: 0,
                    lease_expires_at_us: 0,
                    has_more: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            ack_count = inner.ack_lease_ids.len(),
            nack_count = inner.nack_lease_ids.len(),
            extend_count = inner.extend_lease_ids.len(),
            "Ack request"
        );

        match self.ack_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Ack failed");
                Ok(Response::new(AckResponse {
                    acked_count: 0,
                    nacked_count: 0,
                    extended_count: 0,
                    low_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn create_consumer_group(
        &self,
        request: Request<CreateConsumerGroupRequest>,
    ) -> Result<Response<CreateConsumerGroupResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            consumer_group = %inner.consumer_group_id,
            ack_mode = ?inner.ack_mode,
            topics = ?inner.topics,
            "CreateConsumerGroup request"
        );

        match self.create_consumer_group_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "CreateConsumerGroup failed");
                Ok(Response::new(CreateConsumerGroupResponse {
                    created: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn get_committed_offset(
        &self,
        request: Request<GetCommittedOffsetRequest>,
    ) -> Result<Response<GetCommittedOffsetResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            "GetCommittedOffset request"
        );

        match self.get_committed_offset_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetCommittedOffset failed");
                Ok(Response::new(GetCommittedOffsetResponse {
                    low_watermark: 0,
                    has_committed: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_topic() {
        let service = HelixService::new("test-cluster".to_string(), 1);

        service
            .create_topic("test-topic".to_string(), 3)
            .await
            .unwrap();

        let metadata = service
            .get_metadata_internal(GetMetadataRequest { topics: vec![] })
            .await
            .unwrap();

        assert_eq!(metadata.topics.len(), 1);
        assert_eq!(metadata.topics[0].name, "test-topic");
        assert_eq!(metadata.topics[0].partitions.len(), 3);
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let service = HelixService::new("test-cluster".to_string(), 1);
        service
            .create_topic("test-topic".to_string(), 1)
            .await
            .unwrap();

        // Write records.
        let write_response = service
            .write_internal(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![
                    ProtoRecord {
                        key: Some(b"key1".to_vec()),
                        value: b"value1".to_vec(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1000),
                    },
                    ProtoRecord {
                        key: Some(b"key2".to_vec()),
                        value: b"value2".to_vec(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1001),
                    },
                ],
                required_acks: 1,
            })
            .await
            .unwrap();

        assert_eq!(write_response.base_offset, 0);
        assert_eq!(write_response.record_count, 2);
        assert_eq!(write_response.error_code, i32::from(ErrorCode::None));

        // Read records.
        let read_response = service
            .read_internal(ReadRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 0,
                max_records: 10,
                max_bytes: 1024,
            })
            .await
            .unwrap();

        assert_eq!(read_response.records.len(), 2);
        assert_eq!(read_response.high_watermark, 2);
        assert_eq!(read_response.records[0].offset, 0);
        assert_eq!(read_response.records[1].offset, 1);
    }

    #[tokio::test]
    async fn test_read_nonexistent_topic() {
        let service = HelixService::new("test-cluster".to_string(), 1);

        let result = service
            .read_internal(ReadRequest {
                topic: "nonexistent".to_string(),
                partition: 0,
                offset: 0,
                max_records: 10,
                max_bytes: 1024,
            })
            .await;

        assert!(matches!(result, Err(ServerError::TopicNotFound { .. })));
    }

    #[tokio::test]
    async fn test_offset_out_of_range() {
        let service = HelixService::new("test-cluster".to_string(), 1);
        service
            .create_topic("test-topic".to_string(), 1)
            .await
            .unwrap();

        let result = service
            .read_internal(ReadRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 100, // Beyond high watermark.
                max_records: 10,
                max_bytes: 1024,
            })
            .await;

        assert!(matches!(result, Err(ServerError::OffsetOutOfRange { .. })));
    }

    #[tokio::test]
    async fn test_multi_partition_topic() {
        let service = HelixService::new("test-cluster".to_string(), 1);
        service
            .create_topic("multi-topic".to_string(), 4)
            .await
            .unwrap();

        // Write to different partitions.
        for partition in 0..4 {
            let write_response = service
                .write_internal(WriteRequest {
                    topic: "multi-topic".to_string(),
                    partition,
                    records: vec![ProtoRecord {
                        key: Some(format!("key-{partition}").into_bytes()),
                        value: format!("value-{partition}").into_bytes(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1000),
                    }],
                    required_acks: 1,
                })
                .await
                .unwrap();

            assert_eq!(write_response.base_offset, 0);
            assert_eq!(write_response.record_count, 1);
        }

        // Read from each partition.
        for partition in 0..4 {
            let read_response = service
                .read_internal(ReadRequest {
                    topic: "multi-topic".to_string(),
                    partition,
                    offset: 0,
                    max_records: 10,
                    max_bytes: 1024,
                })
                .await
                .unwrap();

            assert_eq!(read_response.records.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_create_consumer_group() {
        let service = HelixService::new("test-cluster".to_string(), 1);

        // Create consumer group.
        let response = service
            .create_consumer_group_internal(CreateConsumerGroupRequest {
                consumer_group_id: "test-group".to_string(),
                ack_mode: i32::from(ProtoAckMode::Cumulative),
                topics: vec!["test-topic".to_string()],
            })
            .await
            .unwrap();

        assert!(response.created);
        assert_eq!(response.error_code, i32::from(ErrorCode::None));
    }

    #[tokio::test]
    async fn test_pull_and_ack() {
        let service = HelixService::new("test-cluster".to_string(), 1);

        // Create topic and write some records.
        service
            .create_topic("test-topic".to_string(), 1)
            .await
            .unwrap();

        service
            .write_internal(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![
                    ProtoRecord {
                        key: Some(b"key1".to_vec()),
                        value: b"value1".to_vec(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1000),
                    },
                    ProtoRecord {
                        key: Some(b"key2".to_vec()),
                        value: b"value2".to_vec(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1001),
                    },
                ],
                required_acks: 1,
            })
            .await
            .unwrap();

        // Create consumer group.
        service
            .create_consumer_group_internal(CreateConsumerGroupRequest {
                consumer_group_id: "test-group".to_string(),
                ack_mode: i32::from(ProtoAckMode::Cumulative),
                topics: vec!["test-topic".to_string()],
            })
            .await
            .unwrap();

        // Pull records.
        let pull_response = service
            .pull_internal(PullRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                consumer_group_id: "test-group".to_string(),
                consumer_id: "consumer-1".to_string(),
                max_records: 10,
                max_bytes: 1024,
                lease_duration_ms: 60000,
            })
            .await
            .unwrap();

        assert_eq!(pull_response.records.len(), 2);
        assert!(pull_response.lease_id > 0);
        assert_eq!(pull_response.from_offset, 0);
        assert_eq!(pull_response.to_offset, 1);

        // Ack the lease.
        let ack_response = service
            .ack_internal(AckRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                consumer_group_id: "test-group".to_string(),
                consumer_id: "consumer-1".to_string(),
                ack_lease_ids: vec![pull_response.lease_id],
                nack_lease_ids: vec![],
                extend_lease_ids: vec![],
                extend_duration_ms: 0,
            })
            .await
            .unwrap();

        assert_eq!(ack_response.acked_count, 1);
        // After ack, low watermark should advance to 2 (past both offsets 0 and 1).
        assert_eq!(ack_response.low_watermark, 2);

        // Pull again - should get no records since all are committed.
        let pull_response2 = service
            .pull_internal(PullRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                consumer_group_id: "test-group".to_string(),
                consumer_id: "consumer-1".to_string(),
                max_records: 10,
                max_bytes: 1024,
                lease_duration_ms: 60000,
            })
            .await
            .unwrap();

        // Should have no new records to deliver.
        assert_eq!(pull_response2.records.len(), 0);
    }

    #[tokio::test]
    async fn test_get_committed_offset() {
        let service = HelixService::new("test-cluster".to_string(), 1);

        // Create topic and consumer group.
        service
            .create_topic("test-topic".to_string(), 1)
            .await
            .unwrap();

        service
            .create_consumer_group_internal(CreateConsumerGroupRequest {
                consumer_group_id: "test-group".to_string(),
                ack_mode: i32::from(ProtoAckMode::Cumulative),
                topics: vec!["test-topic".to_string()],
            })
            .await
            .unwrap();

        // Get committed offset before any commits.
        let response = service
            .get_committed_offset_internal(GetCommittedOffsetRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                consumer_group_id: "test-group".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(response.low_watermark, 0);
        // Note: has_committed may be false until consumer subscribes.
    }

    #[tokio::test]
    async fn test_group_map() {
        let mut gm = GroupMap::new();

        let g1 = gm.allocate(TopicId::new(1), PartitionId::new(0));
        let g2 = gm.allocate(TopicId::new(1), PartitionId::new(1));
        let g3 = gm.allocate(TopicId::new(2), PartitionId::new(0));

        // Each should get unique group IDs.
        assert_ne!(g1, g2);
        assert_ne!(g2, g3);
        assert_ne!(g1, g3);

        // Same allocation should return same ID.
        let g1_again = gm.allocate(TopicId::new(1), PartitionId::new(0));
        assert_eq!(g1, g1_again);

        // Lookups should work.
        assert_eq!(gm.get(TopicId::new(1), PartitionId::new(0)), Some(g1));
        assert_eq!(gm.get_key(g1), Some((TopicId::new(1), PartitionId::new(0))));
    }
}

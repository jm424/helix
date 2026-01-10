//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Multi-Raft for consensus across multiple
//! partition groups, with separate partition storage for durability.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, Offset, PartitionId, Record, TopicId};
use helix_partition::{Partition, PartitionConfig, PartitionCommand};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::error::{ServerError, ServerResult};
use crate::generated::{
    helix_server::Helix, BrokerInfo, ErrorCode, GetMetadataRequest, GetMetadataResponse,
    GetPartitionInfoRequest, GetPartitionInfoResponse, PartitionInfo, ReadRequest, ReadResponse,
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

/// Storage for a single partition.
struct PartitionStorage {
    /// Topic ID.
    #[allow(dead_code)]
    topic_id: TopicId,
    /// Partition ID.
    #[allow(dead_code)]
    partition_id: PartitionId,
    /// The underlying partition storage.
    partition: Partition,
    /// Last applied Raft log index.
    last_applied: LogIndex,
}

impl PartitionStorage {
    /// Creates new partition storage.
    fn new(topic_id: TopicId, partition_id: PartitionId) -> Self {
        let config = PartitionConfig::new(topic_id, partition_id);
        Self {
            topic_id,
            partition_id,
            partition: Partition::new(config),
            last_applied: LogIndex::new(0),
        }
    }

    /// Applies a committed entry to the partition.
    fn apply_entry(&mut self, index: LogIndex, data: &Bytes) -> ServerResult<Option<Offset>> {
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

        let base_offset = match command {
            PartitionCommand::Append { records } => {
                let offset = self.partition.append(records).map_err(|e| {
                    ServerError::Internal {
                        message: format!("failed to append: {e}"),
                    }
                })?;

                // Update high watermark since entry is committed.
                let new_hwm = self.partition.log_end_offset();
                self.partition.set_high_watermark(new_hwm);

                Some(offset)
            }
            PartitionCommand::Truncate { from_offset } => {
                self.partition.truncate(from_offset).map_err(|e| {
                    ServerError::Internal {
                        message: format!("failed to truncate: {e}"),
                    }
                })?;
                None
            }
            PartitionCommand::UpdateHighWatermark { high_watermark } => {
                self.partition.set_high_watermark(high_watermark);
                None
            }
        };

        self.last_applied = index;
        Ok(base_offset)
    }
}

/// Maps between (TopicId, PartitionId) and GroupId.
struct GroupMap {
    /// Forward mapping: (TopicId, PartitionId) -> GroupId.
    by_key: HashMap<(TopicId, PartitionId), GroupId>,
    /// Reverse mapping: GroupId -> (TopicId, PartitionId).
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
    /// Partition storage indexed by GroupId.
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
}

impl HelixService {
    /// Creates a new Helix service backed by Multi-Raft.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    #[must_use]
    pub fn new(cluster_id: String, node_id: u64) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));

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
        }
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
                            if let Err(e) = ps.apply_entry(*index, data) {
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

            // Create partition storage.
            partition_storage.insert(group_id, PartitionStorage::new(topic_id, partition_id));

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
                                    let _ = ps.apply_entry(*index, data);
                                }
                            }
                        }
                    }

                    // Check if became leader.
                    let is_leader = multi_raft
                        .group_state(group_id)
                        .map_or(false, |s| s.state == RaftState::Leader);

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
            ps.partition.log_end_offset()
        };

        // Check if we're the leader.
        let (is_leader, leader_hint) = {
            let mr = self.multi_raft.read().await;
            let state = mr.group_state(group_id);
            let is_leader = state.as_ref().map_or(false, |s| s.state == RaftState::Leader);
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
                            ps.apply_entry(*index, data).map_err(|e| ServerError::Internal {
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
        let log_start = ps.partition.log_start_offset();
        let log_end = ps.partition.log_end_offset();

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
        let records = ps.partition.read(start_offset, max_records).map_err(|e| {
            ServerError::Internal {
                message: format!("failed to read: {e}"),
            }
        })?;

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

        let high_watermark = ps.partition.high_watermark();

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
            log_start_offset: ps.partition.log_start_offset().get(),
            log_end_offset: ps.partition.log_end_offset().get(),
            high_watermark: ps.partition.high_watermark().get(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
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

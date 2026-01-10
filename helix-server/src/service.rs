//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Raft-replicated partitions for durability
//! and consistency.

use std::collections::HashMap;
use std::sync::Arc;

use helix_core::{NodeId, Offset, PartitionId, Record, TopicId};
use helix_partition::{ReplicatedPartitionConfig, ReplicationManager, ReplicationState};
use helix_raft::RaftOutput;
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

/// Election timeout interval in milliseconds.
const ELECTION_TICK_MS: u64 = 150;

/// Heartbeat interval in milliseconds.
const HEARTBEAT_TICK_MS: u64 = 50;

/// Topic metadata.
#[derive(Debug, Clone)]
struct TopicMetadata {
    /// Topic ID.
    topic_id: TopicId,
    /// Number of partitions.
    partition_count: i32,
}

/// The Helix gRPC service.
///
/// This provides a Raft-replicated implementation for production use.
pub struct HelixService {
    /// Cluster ID.
    cluster_id: String,
    /// This node's ID.
    node_id: NodeId,
    /// Replication manager for all partitions.
    replication: Arc<RwLock<ReplicationManager>>,
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
    /// Creates a new Helix service.
    ///
    /// This starts a background task to handle Raft ticks.
    #[must_use]
    pub fn new(cluster_id: String, node_id: u64) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let replication = Arc::new(RwLock::new(ReplicationManager::new(node_id)));
        let replication_clone = Arc::clone(&replication);

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task.
        tokio::spawn(Self::tick_task(replication_clone, shutdown_rx));

        Self {
            cluster_id,
            node_id,
            replication,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            _shutdown_tx: shutdown_tx,
        }
    }

    /// Background task to handle Raft election and heartbeat ticks.
    #[allow(clippy::significant_drop_tightening)]
    async fn tick_task(
        replication: Arc<RwLock<ReplicationManager>>,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        let mut election_interval = tokio::time::interval(
            tokio::time::Duration::from_millis(ELECTION_TICK_MS)
        );
        let mut heartbeat_interval = tokio::time::interval(
            tokio::time::Duration::from_millis(HEARTBEAT_TICK_MS)
        );

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    debug!("Tick task shutting down");
                    break;
                }
                _ = election_interval.tick() => {
                    let mut repl = replication.write().await;
                    let outputs = repl.handle_election_timeouts();
                    for ((topic_id, partition_id), partition_outputs) in outputs {
                        Self::process_outputs(&mut repl, topic_id, partition_id, &partition_outputs);
                    }
                }
                _ = heartbeat_interval.tick() => {
                    let mut repl = replication.write().await;
                    let outputs = repl.handle_heartbeat_timeouts();
                    for ((topic_id, partition_id), partition_outputs) in outputs {
                        Self::process_outputs(&mut repl, topic_id, partition_id, &partition_outputs);
                    }
                }
            }
        }
    }

    /// Processes Raft outputs for a partition.
    fn process_outputs(
        replication: &mut ReplicationManager,
        topic_id: TopicId,
        partition_id: PartitionId,
        outputs: &[RaftOutput],
    ) {
        if outputs.is_empty() {
            return;
        }

        // Apply committed entries.
        if let Some(partition) = replication.get_mut(topic_id, partition_id) {
            if let Err(e) = partition.apply_outputs(outputs) {
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to apply outputs"
                );
            }
        }

        // Log state changes.
        for output in outputs {
            match output {
                RaftOutput::BecameLeader => {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        "Became leader"
                    );
                }
                RaftOutput::SteppedDown => {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        "Stepped down from leader"
                    );
                }
                _ => {}
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
    pub async fn create_topic(
        &self,
        name: String,
        partition_count: i32,
    ) -> ServerResult<()> {
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

        // Create partitions.
        let mut replication = self.replication.write().await;
        for i in 0..partition_count {
            // Safe cast: partition_count is bounded by 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);
            let config = ReplicatedPartitionConfig::new(
                topic_id,
                partition_id,
                self.node_id,
                self.cluster_nodes.clone(),
            );
            replication.add_partition(config);

            // Trigger election for single-node cluster.
            if let Some(partition) = replication.get_mut(topic_id, partition_id) {
                let outputs = partition.handle_election_timeout();
                Self::process_outputs(&mut replication, topic_id, partition_id, &outputs);
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
                bytes::Bytes::from(key.clone()),
                bytes::Bytes::from(proto.value.clone()),
            )
        } else {
            Record::new(bytes::Bytes::from(proto.value.clone()))
        };

        if let Some(ts) = proto.timestamp_ms {
            record = record.with_timestamp(helix_core::Timestamp::from_millis(ts));
        }
        for (k, v) in &proto.headers {
            record = record.with_header(k.clone(), bytes::Bytes::from(v.clone()));
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

    /// Internal write implementation.
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

        // Convert proto records to core records.
        let records: Vec<Record> = request
            .records
            .iter()
            .map(Self::proto_to_record)
            .collect();

        let record_count = records.len();

        // Propose to Raft.
        let mut replication = self.replication.write().await;
        let partition = replication
            .get_mut(topic_meta.topic_id, partition_id)
            .ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            })?;

        // Check if we're the leader.
        if partition.state() != ReplicationState::Leader {
            return Err(ServerError::NotLeader {
                topic: request.topic.clone(),
                partition: partition_idx,
                leader_hint: partition.leader().map(helix_core::NodeId::get),
            });
        }

        // Get the base offset before appending.
        let base_offset = partition.log_end_offset();

        // Propose and wait for commit.
        let outputs = partition
            .propose_append(records)
            .map_err(|e| ServerError::Internal {
                message: format!("failed to propose: {e}"),
            })?;

        // Apply the outputs (single-node: immediate commit).
        if let Some(outputs) = outputs {
            partition.apply_outputs(&outputs).map_err(|e| ServerError::Internal {
                message: format!("failed to apply: {e}"),
            })?;
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

        let replication = self.replication.read().await;
        let partition = replication
            .get(topic_meta.topic_id, partition_id)
            .ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            })?;

        // Check offset bounds.
        let log_start = partition.log_start_offset();
        let log_end = partition.log_end_offset();

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
        let records = partition.read(start_offset, max_records).map_err(|e| {
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
            let key_size = record.key.as_ref().map_or(0, |k| k.len() as u32);
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

        debug!(
            topic = %request.topic,
            partition = request.partition,
            offset = request.offset,
            count = proto_records.len(),
            "Read records"
        );

        Ok(ReadResponse {
            records: proto_records,
            high_watermark: partition.high_watermark().get(),
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
        let replication = self.replication.read().await;

        // Filter topics if specific ones requested.
        let topic_infos: Vec<TopicInfo> = if request.topics.is_empty() {
            // Return all topics.
            topics
                .iter()
                .map(|(name, meta)| self.make_topic_info(name, meta, &replication))
                .collect()
        } else {
            // Return only requested topics.
            request
                .topics
                .iter()
                .filter_map(|name| {
                    topics
                        .get(name)
                        .map(|meta| self.make_topic_info(name, meta, &replication))
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
        replication: &ReplicationManager,
    ) -> TopicInfo {
        let mut partitions = Vec::new();

        for i in 0..meta.partition_count {
            // Safe cast: i is in range [0, partition_count) and partition_count <= 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);
            let leader = replication
                .get(meta.topic_id, partition_id)
                .and_then(helix_partition::ReplicatedPartition::leader)
                .map_or_else(|| self.node_id.get(), helix_core::NodeId::get);

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

        let replication = self.replication.read().await;
        let partition = replication
            .get(topic_meta.topic_id, partition_id)
            .ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            })?;

        let leader = partition
            .leader()
            .map_or_else(|| self.node_id.get(), helix_core::NodeId::get);

        Ok(GetPartitionInfoResponse {
            partition: Some(PartitionInfo {
                partition: request.partition,
                leader,
                replicas: self.cluster_nodes.iter().map(|n| n.get()).collect(),
                isr: vec![leader],
            }),
            log_start_offset: partition.log_start_offset().get(),
            log_end_offset: partition.log_end_offset().get(),
            high_watermark: partition.high_watermark().get(),
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
}

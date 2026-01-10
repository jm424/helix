//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::error::{ServerError, ServerResult};
use crate::generated::{
    helix_server::Helix, BrokerInfo, ErrorCode, GetMetadataRequest, GetMetadataResponse,
    GetPartitionInfoRequest, GetPartitionInfoResponse, PartitionInfo, ReadRequest, ReadResponse,
    Record, RecordWithOffset, TopicInfo, WriteRequest, WriteResponse,
};

/// Maximum records per write request.
const MAX_RECORDS_PER_WRITE: usize = 1000;

/// Maximum bytes per read response.
const MAX_BYTES_PER_READ: u32 = 1024 * 1024;

/// A partition's state for testing/development.
#[derive(Debug)]
struct PartitionState {
    /// Records stored in this partition.
    records: Vec<StoredRecord>,
    /// High watermark (last committed offset + 1).
    high_watermark: u64,
}

/// A stored record with metadata.
#[derive(Debug, Clone)]
struct StoredRecord {
    /// The record data.
    record: Record,
    /// The offset of this record.
    offset: u64,
}

impl PartitionState {
    const fn new() -> Self {
        Self {
            records: Vec::new(),
            high_watermark: 0,
        }
    }
}

/// Topic state for testing/development.
#[derive(Debug)]
struct TopicState {
    /// Topic ID.
    topic_id: u64,
    /// Partitions in this topic.
    partitions: HashMap<i32, PartitionState>,
}

impl TopicState {
    fn new(topic_id: u64, partition_count: i32) -> Self {
        let mut partitions = HashMap::new();
        for i in 0..partition_count {
            partitions.insert(i, PartitionState::new());
        }
        Self {
            topic_id,
            partitions,
        }
    }
}

/// The Helix gRPC service.
///
/// This provides a simple in-memory implementation for testing and development.
/// Production use will integrate with the Raft-based partition manager.
pub struct HelixService {
    /// Cluster ID.
    cluster_id: String,
    /// This node's ID.
    node_id: u64,
    /// Topics and their partitions.
    topics: Arc<RwLock<HashMap<String, TopicState>>>,
    /// Next topic ID.
    next_topic_id: Arc<RwLock<u64>>,
}

impl HelixService {
    /// Creates a new Helix service.
    #[must_use]
    pub fn new(cluster_id: String, node_id: u64) -> Self {
        Self {
            cluster_id,
            node_id,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
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
        let topic_id = *next_id;
        *next_id += 1;

        topics.insert(name.clone(), TopicState::new(topic_id, partition_count));
        info!(topic = %name, partitions = partition_count, "Created topic");

        Ok(())
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

        let mut topics = self.topics.write().await;

        // Get or create topic.
        let topic = topics.get_mut(&request.topic).ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        // Get partition.
        let partition_idx = if request.partition < 0 {
            // Auto-partition: use round-robin or hash-based assignment.
            0
        } else {
            request.partition
        };

        let partition = topic.partitions.get_mut(&partition_idx).ok_or_else(|| {
            ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            }
        })?;

        // Write records.
        let base_offset = partition.high_watermark;
        let record_count = request.records.len();

        for (i, record) in request.records.into_iter().enumerate() {
            // Safe cast: i is bounded by MAX_RECORDS_PER_WRITE which fits in u64.
            #[allow(clippy::cast_possible_truncation)]
            let offset = base_offset + i as u64;
            partition.records.push(StoredRecord { record, offset });
        }

        // Safe cast: record_count is bounded by MAX_RECORDS_PER_WRITE which fits in u64.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u64 = record_count as u64;
        partition.high_watermark = base_offset + record_count_u64;

        debug!(
            topic = %request.topic,
            partition = partition_idx,
            base_offset = base_offset,
            count = record_count,
            "Wrote records"
        );

        // Safe cast: record_count is bounded by MAX_RECORDS_PER_WRITE which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u32 = record_count as u32;
        Ok(WriteResponse {
            base_offset,
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

        let topics = self.topics.read().await;

        let topic = topics.get(&request.topic).ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        let partition = topic.partitions.get(&request.partition).ok_or_else(|| {
            ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            }
        })?;

        // Check offset bounds.
        let log_start = if partition.records.is_empty() {
            0
        } else {
            partition.records[0].offset
        };
        let log_end = partition.high_watermark;

        if request.offset < log_start || request.offset > log_end {
            return Err(ServerError::OffsetOutOfRange {
                offset: request.offset,
                start: log_start,
                end: log_end,
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

        let mut records = Vec::new();
        let mut bytes_read: u32 = 0;

        for stored in &partition.records {
            if stored.offset < request.offset {
                continue;
            }
            if records.len() >= max_records as usize {
                break;
            }

            // Estimate record size.
            // Safe cast: record value/key sizes are bounded by message limits which fit in u32.
            #[allow(clippy::cast_possible_truncation)]
            let value_size = stored.record.value.len() as u32;
            #[allow(clippy::cast_possible_truncation)]
            let key_size = stored.record.key.as_ref().map_or(0, |k| k.len() as u32);
            let record_size = value_size + key_size + 16; // Overhead.

            if bytes_read + record_size > max_bytes && !records.is_empty() {
                break;
            }

            records.push(RecordWithOffset {
                offset: stored.offset,
                record: Some(stored.record.clone()),
            });
            bytes_read += record_size;
        }

        debug!(
            topic = %request.topic,
            partition = request.partition,
            offset = request.offset,
            count = records.len(),
            "Read records"
        );

        Ok(ReadResponse {
            records,
            high_watermark: partition.high_watermark,
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

        // Filter topics if specific ones requested.
        let topic_infos: Vec<TopicInfo> = if request.topics.is_empty() {
            // Return all topics.
            topics
                .iter()
                .map(|(name, state)| self.make_topic_info(name, state))
                .collect()
        } else {
            // Return only requested topics.
            request
                .topics
                .iter()
                .filter_map(|name| {
                    topics
                        .get(name)
                        .map(|state| self.make_topic_info(name, state))
                })
                .collect()
        };

        Ok(GetMetadataResponse {
            cluster_id: self.cluster_id.clone(),
            controller_id: self.node_id,
            brokers: vec![BrokerInfo {
                node_id: self.node_id,
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: topic_infos,
        })
    }

    /// Creates topic info from state.
    fn make_topic_info(&self, name: &str, state: &TopicState) -> TopicInfo {
        let partitions: Vec<PartitionInfo> = state
            .partitions
            .keys()
            .map(|&partition_idx| PartitionInfo {
                partition: partition_idx,
                leader: self.node_id,
                replicas: vec![self.node_id],
                isr: vec![self.node_id],
            })
            .collect();

        TopicInfo {
            name: name.to_string(),
            topic_id: state.topic_id,
            partitions,
        }
    }

    /// Internal partition info implementation.
    #[allow(clippy::significant_drop_tightening)]
    async fn get_partition_info_internal(
        &self,
        request: GetPartitionInfoRequest,
    ) -> ServerResult<GetPartitionInfoResponse> {
        let topics = self.topics.read().await;

        let topic = topics.get(&request.topic).ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        let partition = topic.partitions.get(&request.partition).ok_or_else(|| {
            ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            }
        })?;

        let log_start = if partition.records.is_empty() {
            0
        } else {
            partition.records[0].offset
        };

        Ok(GetPartitionInfoResponse {
            partition: Some(PartitionInfo {
                partition: request.partition,
                leader: self.node_id,
                replicas: vec![self.node_id],
                isr: vec![self.node_id],
            }),
            log_start_offset: log_start,
            log_end_offset: partition.high_watermark,
            high_watermark: partition.high_watermark,
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

        service.create_topic("test-topic".to_string(), 3).await.unwrap();

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
        service.create_topic("test-topic".to_string(), 1).await.unwrap();

        // Write records.
        let write_response = service
            .write_internal(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![
                    Record {
                        key: Some(b"key1".to_vec()),
                        value: b"value1".to_vec(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1000),
                    },
                    Record {
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
        service.create_topic("test-topic".to_string(), 1).await.unwrap();

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

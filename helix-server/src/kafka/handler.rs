//! Kafka protocol request handler.
//!
//! Dispatches incoming Kafka requests to the appropriate API handler and maps
//! operations to the underlying `HelixService` storage via `MultiRaft`.
//!
//! # Storage Architecture
//!
//! Uses protocol-agnostic blob storage via `HelixService`.
//! Kafka `RecordBatch` bytes are stored as-is without parsing, enabling
//! zero-copy on the fetch path. Data is replicated through Raft.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    messages::{
        api_versions_response::ApiVersion, fetch_response::FetchableTopicResponse,
        fetch_response::PartitionData, find_coordinator_response::Coordinator,
        list_offsets_response::ListOffsetsPartitionResponse,
        list_offsets_response::ListOffsetsTopicResponse,
        metadata_response::MetadataResponseBroker, ApiKey, ApiVersionsResponse, BrokerId,
        FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
        ListOffsetsRequest, ListOffsetsResponse, MetadataRequest, MetadataResponse,
        ProduceRequest, ProduceResponse,
    },
    protocol::{Decodable, StrBytes},
};
use tracing::{debug, info, warn};

use super::codec::{self, DecodedRequest};
use super::error::{KafkaError, KafkaResult};
use crate::HelixService;

/// Supported API versions.
///
/// Each tuple is (`api_key`, `min_version`, `max_version`).
const SUPPORTED_APIS: &[(i16, i16, i16)] = &[
    (ApiKey::ApiVersions as i16, 0, 3),
    (ApiKey::Metadata as i16, 0, 12),
    (ApiKey::Produce as i16, 0, 9),
    (ApiKey::Fetch as i16, 4, 12),
    (ApiKey::ListOffsets as i16, 0, 7),
    (ApiKey::FindCoordinator as i16, 0, 4),
];

/// Kafka protocol handler backed by `HelixService`.
///
/// Handles Kafka wire protocol requests by delegating storage operations
/// to `HelixService` which provides `MultiRaft`-replicated storage.
pub struct KafkaHandler {
    /// The underlying Helix service for storage.
    service: Arc<HelixService>,
    /// Hostname for this node.
    host: String,
    /// Kafka port.
    port: i32,
    /// Auto-create topics on first produce/fetch.
    auto_create_topics: bool,
}

impl KafkaHandler {
    /// Creates a new Kafka handler.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Arc fields prevent const.
    pub fn new(service: Arc<HelixService>, host: String, port: i32, auto_create_topics: bool) -> Self {
        Self {
            service,
            host,
            port,
            auto_create_topics,
        }
    }

    /// Returns a reference to the underlying Helix service.
    #[must_use]
    pub const fn service(&self) -> &Arc<HelixService> {
        &self.service
    }

    /// Handle a decoded request and return the encoded response.
    ///
    /// # Errors
    ///
    /// Returns an error if the API is unsupported or encoding/decoding fails.
    pub async fn handle_request(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        debug!(
            api_key = request.api_key,
            api_version = request.api_version,
            correlation_id = request.correlation_id,
            "Handling Kafka request"
        );

        match request.api_key {
            x if x == ApiKey::ApiVersions as i16 => self.handle_api_versions(request),
            x if x == ApiKey::Metadata as i16 => self.handle_metadata(request).await,
            x if x == ApiKey::Produce as i16 => self.handle_produce(request).await,
            x if x == ApiKey::Fetch as i16 => self.handle_fetch(request).await,
            x if x == ApiKey::ListOffsets as i16 => self.handle_list_offsets(request).await,
            x if x == ApiKey::FindCoordinator as i16 => self.handle_find_coordinator(request),
            _ => {
                warn!(api_key = request.api_key, "Unsupported Kafka API");
                Err(KafkaError::UnsupportedApi {
                    api_key: request.api_key,
                    api_version: request.api_version,
                })
            }
        }
    }

    /// Handle `ApiVersions` request.
    #[allow(clippy::unused_self)] // Part of consistent handler API.
    fn handle_api_versions(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        let our_max_version = SUPPORTED_APIS
            .iter()
            .find(|(key, _, _)| *key == ApiKey::ApiVersions as i16)
            .map_or(0, |(_, _, max)| *max);

        let response_version = request.api_version.min(our_max_version);

        let mut response = ApiVersionsResponse::default();

        for &(api_key, min_version, max_version) in SUPPORTED_APIS {
            let mut api_version = ApiVersion::default();
            api_version.api_key = api_key;
            api_version.min_version = min_version;
            api_version.max_version = max_version;
            response.api_keys.push(api_version);
        }

        response.error_code = 0;
        response.throttle_time_ms = 0;

        codec::encode_response(
            request.api_key,
            response_version,
            request.correlation_id,
            &response,
        )
    }

    /// Handle Metadata request.
    #[allow(clippy::too_many_lines)]
    async fn handle_metadata(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
        use kafka_protocol::messages::TopicName;

        let mut body = request.body.clone();
        let metadata_request =
            MetadataRequest::decode(&mut body, request.api_version).map_err(KafkaError::decode)?;

        let mut response = MetadataResponse::default();

        // Add all brokers in the cluster.
        // Safe cast: NodeId (u64) fits in i32 for reasonable cluster sizes.
        #[allow(clippy::cast_possible_truncation)]
        for node_id in self.service.cluster_nodes() {
            let mut broker = MetadataResponseBroker::default();
            broker.node_id = BrokerId(node_id.get() as i32);

            // Get the Kafka address for this node from the service.
            // In single-node mode, fall back to this node's address.
            if let Some(addr) = self.service.get_node_address(*node_id) {
                // Parse host:port from stored Kafka address.
                if let Some((host, port)) = addr.rsplit_once(':') {
                    broker.host = StrBytes::from_string(host.to_string());
                    broker.port = port.parse().unwrap_or(self.port);
                } else {
                    broker.host = StrBytes::from_string(addr.to_string());
                    broker.port = self.port;
                }
            } else {
                // Single-node mode: use this node's Kafka address.
                broker.host = StrBytes::from_string(self.host.clone());
                broker.port = self.port;
            }

            response.brokers.push(broker);
        }

        // Set cluster ID.
        response.cluster_id = Some(StrBytes::from_string(self.service.cluster_id().to_string()));

        // Set controller ID.
        // Safe cast: NodeId (u64) fits in i32 for reasonable cluster sizes.
        #[allow(clippy::cast_possible_truncation)]
        {
            response.controller_id = BrokerId(self.service.node_id().get() as i32);
        }

        // Get all topics.
        let all_topics = self.service.get_all_topics().await;

        // Filter topics if specific ones requested.
        let requested_topics: Option<Vec<String>> = metadata_request.topics.as_ref().map(|topics| {
            topics
                .iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
                .collect()
        });

        for (topic_name, partition_count) in &all_topics {
            if let Some(ref requested) = requested_topics {
                if !requested.contains(topic_name) {
                    continue;
                }
            }

            let mut topic_response = MetadataResponseTopic::default();
            topic_response.error_code = 0;
            topic_response.name = Some(TopicName(StrBytes::from_string(topic_name.clone())));
            topic_response.is_internal = false;

            for partition_id in 0..*partition_count {
                let partition_response = self
                    .build_partition_metadata(topic_name, partition_id)
                    .await;
                topic_response.partitions.push(partition_response);
            }

            response.topics.push(topic_response);
        }

        // Handle topics that were requested but don't exist.
        if let Some(ref requested) = requested_topics {
            let existing_names: Vec<_> = all_topics.iter().map(|(n, _)| n.clone()).collect();
            for topic_name in requested {
                if existing_names.contains(topic_name) {
                    continue;
                }

                if self.auto_create_topics {
                    // Auto-create topic with 1 partition.
                    if let Err(e) = self.service.create_topic(topic_name.clone(), 1).await {
                        warn!(topic = %topic_name, error = %e, "Failed to auto-create topic");
                    } else {
                        info!(topic = %topic_name, "Auto-created topic");

                        let mut topic_response = MetadataResponseTopic::default();
                        topic_response.error_code = 0;
                        topic_response.name =
                            Some(TopicName(StrBytes::from_string(topic_name.clone())));
                        topic_response.is_internal = false;

                        let partition_response = self.build_partition_metadata(topic_name, 0).await;
                        topic_response.partitions.push(partition_response);
                        response.topics.push(topic_response);
                    }
                } else {
                    // Return error for non-existent topic.
                    let mut topic_response = MetadataResponseTopic::default();
                    topic_response.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    topic_response.name =
                        Some(TopicName(StrBytes::from_string(topic_name.clone())));
                    topic_response.is_internal = false;
                    topic_response.partitions = vec![];
                    response.topics.push(topic_response);
                }
            }
        }

        codec::encode_response(
            request.api_key,
            request.api_version,
            request.correlation_id,
            &response,
        )
    }

    /// Build partition metadata response.
    async fn build_partition_metadata(
        &self,
        topic_name: &str,
        partition_id: i32,
    ) -> kafka_protocol::messages::metadata_response::MetadataResponsePartition {
        use kafka_protocol::messages::metadata_response::MetadataResponsePartition;

        let mut partition_response = MetadataResponsePartition::default();
        partition_response.error_code = 0;
        partition_response.partition_index = partition_id;

        // Get leader from MultiRaft.
        let leader_id = self
            .service
            .get_leader(topic_name, partition_id)
            .await
            .unwrap_or_else(|| self.service.node_id());

        // Safe cast: NodeId (u64) fits in i32 for reasonable cluster sizes.
        #[allow(clippy::cast_possible_truncation)]
        {
            partition_response.leader_id = BrokerId(leader_id.get() as i32);
            partition_response.replica_nodes = self
                .service
                .cluster_nodes()
                .iter()
                .map(|n| BrokerId(n.get() as i32))
                .collect();
            partition_response.isr_nodes = vec![BrokerId(leader_id.get() as i32)];
        }
        partition_response.leader_epoch = 0;
        partition_response.offline_replicas = vec![];

        partition_response
    }

    /// Handle Produce request.
    async fn handle_produce(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        let mut body = request.body.clone();
        let produce_request =
            ProduceRequest::decode(&mut body, request.api_version).map_err(KafkaError::decode)?;

        debug!(
            acks = produce_request.acks,
            timeout_ms = produce_request.timeout_ms,
            num_topics = produce_request.topic_data.len(),
            "Processing produce request"
        );

        let mut response = ProduceResponse::default();

        for topic_data in &produce_request.topic_data {
            let topic_name = topic_data.name.to_string();

            let mut topic_response =
                kafka_protocol::messages::produce_response::TopicProduceResponse::default();
            topic_response.name = topic_data.name.clone();

            for partition_data in &topic_data.partition_data {
                let partition_id = partition_data.index;
                let records_bytes = partition_data.records.as_ref();

                let record_count = records_bytes.map_or(0, count_records_in_batch);

                let mut partition_response =
                    kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
                partition_response.index = partition_id;

                // Safe cast: record_count is u64 but bounded by batch size.
                #[allow(clippy::cast_possible_truncation)]
                let (base_offset, error_code) = if record_count > 0 {
                    if let Some(data) = records_bytes {
                        self.produce_with_auto_create(
                            &topic_name,
                            partition_id,
                            record_count as u32,
                            data.clone(),
                        )
                        .await
                    } else {
                        (self.get_log_end_offset(&topic_name, partition_id).await, 0)
                    }
                } else {
                    (self.get_log_end_offset(&topic_name, partition_id).await, 0)
                };

                partition_response.error_code = error_code;
                // Safe cast: offset fits in i64.
                #[allow(clippy::cast_possible_wrap)]
                {
                    partition_response.base_offset = base_offset as i64;
                }
                partition_response.log_append_time_ms = -1;
                partition_response.log_start_offset = 0;

                topic_response.partition_responses.push(partition_response);
            }

            response.responses.push(topic_response);
        }

        codec::encode_response(
            request.api_key,
            request.api_version,
            request.correlation_id,
            &response,
        )
    }

    /// Produce records with auto-create topic support.
    async fn produce_with_auto_create(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
    ) -> (u64, i16) {
        // Ensure topic exists (auto-create if enabled).
        if !self.service.topic_exists(topic).await {
            if self.auto_create_topics {
                if let Err(e) = self.service.create_topic(topic.to_string(), 1).await {
                    warn!(topic = %topic, error = %e, "Failed to auto-create topic");
                    return (0, 3); // UNKNOWN_TOPIC_OR_PARTITION
                }
            } else {
                return (0, 3); // UNKNOWN_TOPIC_OR_PARTITION
            }
        }

        self.append_and_get_offset(topic, partition, record_count, data)
            .await
    }

    /// Append blob and get offset, returning (offset, `error_code`).
    async fn append_and_get_offset(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
    ) -> (u64, i16) {
        match self
            .service
            .append_blob(topic, partition, record_count, data)
            .await
        {
            Ok(offset) => {
                info!(
                    topic = %topic,
                    partition,
                    base_offset = offset,
                    record_count,
                    "Produced records"
                );
                (offset, 0)
            }
            Err(crate::ServerError::NotLeader { leader_hint, .. }) => {
                debug!(
                    topic = %topic,
                    partition,
                    leader_hint = ?leader_hint,
                    "Not leader for partition"
                );
                (0, 6) // NOT_LEADER_OR_FOLLOWER
            }
            Err(
                crate::ServerError::TopicNotFound { .. }
                | crate::ServerError::PartitionNotFound { .. },
            ) => (0, 3), // UNKNOWN_TOPIC_OR_PARTITION
            Err(e) => {
                warn!(topic = %topic, partition, error = %e, "Produce failed");
                (0, 1) // UNKNOWN_SERVER_ERROR
            }
        }
    }

    /// Get log end offset, returning 0 if not found.
    async fn get_log_end_offset(&self, topic: &str, partition: i32) -> u64 {
        self.service
            .blob_log_end_offset(topic, partition)
            .await
            .unwrap_or(0)
    }

    /// Handle Fetch request.
    #[allow(clippy::too_many_lines)]
    async fn handle_fetch(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        let mut body = request.body.clone();
        let fetch_request =
            FetchRequest::decode(&mut body, request.api_version).map_err(KafkaError::decode)?;

        debug!(
            max_wait_ms = fetch_request.max_wait_ms,
            min_bytes = fetch_request.min_bytes,
            max_bytes = fetch_request.max_bytes,
            num_topics = fetch_request.topics.len(),
            "Processing fetch request"
        );

        let mut response = FetchResponse::default();

        for topic_data in &fetch_request.topics {
            let topic_name = topic_data.topic.to_string();

            let mut topic_response = FetchableTopicResponse::default();
            topic_response.topic = topic_data.topic.clone();

            for partition_data in &topic_data.partitions {
                let partition_id = partition_data.partition;

                // Safe cast: fetch_offset is i64 but we clamp to 0.
                #[allow(clippy::cast_sign_loss)]
                let fetch_offset = partition_data.fetch_offset.max(0) as u64;

                let mut partition_response = PartitionData::default();
                partition_response.partition_index = partition_id;

                // Check if partition exists.
                if !self.service.blob_partition_exists(&topic_name, partition_id).await {
                    partition_response.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                    partition_response.high_watermark = -1;
                    partition_response.last_stable_offset = -1;
                    partition_response.log_start_offset = -1;
                    partition_response.records = Some(Bytes::new());

                    topic_response.partitions.push(partition_response);
                    continue;
                }

                // Get high watermark.
                let high_watermark = self.get_log_end_offset(&topic_name, partition_id).await;

                // Fetch blobs.
                // Safe cast: partition_max_bytes is i32 but we clamp negative to MAX.
                #[allow(clippy::cast_sign_loss)]
                let max_bytes = if partition_data.partition_max_bytes < 0 {
                    u32::MAX
                } else {
                    partition_data.partition_max_bytes as u32
                };

                match self
                    .service
                    .read_blobs(&topic_name, partition_id, fetch_offset, max_bytes)
                    .await
                {
                    Ok(blobs) => {
                        if blobs.is_empty() && fetch_offset >= high_watermark {
                            // No data available yet.
                            partition_response.error_code = 0;
                            // Safe cast: offset fits in i64.
                            #[allow(clippy::cast_possible_wrap)]
                            {
                                partition_response.high_watermark = high_watermark as i64;
                                partition_response.last_stable_offset = high_watermark as i64;
                                partition_response.log_start_offset = 0;
                            }
                            partition_response.records = Some(Bytes::new());
                        } else {
                            // Concatenate blobs.
                            let mut records_data = BytesMut::new();
                            for blob in &blobs {
                                records_data.extend_from_slice(blob);
                            }
                            let total_bytes = records_data.len();

                            partition_response.error_code = 0;
                            // Safe cast: offset fits in i64.
                            #[allow(clippy::cast_possible_wrap)]
                            {
                                partition_response.high_watermark = high_watermark as i64;
                                partition_response.last_stable_offset = high_watermark as i64;
                                partition_response.log_start_offset = 0;
                            }
                            partition_response.records = Some(records_data.freeze());

                            info!(
                                topic = %topic_name,
                                partition = partition_id,
                                fetch_offset,
                                blobs_count = blobs.len(),
                                total_bytes,
                                "Fetched records"
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            topic = %topic_name,
                            partition = partition_id,
                            error = %e,
                            "Fetch failed"
                        );
                        partition_response.error_code = 1; // UNKNOWN_SERVER_ERROR
                        partition_response.high_watermark = -1;
                        partition_response.last_stable_offset = -1;
                        partition_response.log_start_offset = -1;
                        partition_response.records = Some(Bytes::new());
                    }
                }

                topic_response.partitions.push(partition_response);
            }

            response.responses.push(topic_response);
        }

        codec::encode_response(
            request.api_key,
            request.api_version,
            request.correlation_id,
            &response,
        )
    }

    /// Handle `ListOffsets` request.
    async fn handle_list_offsets(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        let mut body = request.body.clone();
        let list_offsets_request = ListOffsetsRequest::decode(&mut body, request.api_version)
            .map_err(KafkaError::decode)?;

        let mut response = ListOffsetsResponse::default();

        for topic_data in &list_offsets_request.topics {
            let topic_name = topic_data.name.to_string();

            let mut topic_response = ListOffsetsTopicResponse::default();
            topic_response.name = topic_data.name.clone();

            for partition_data in &topic_data.partitions {
                let partition_id = partition_data.partition_index;
                let timestamp = partition_data.timestamp;

                let mut partition_response = ListOffsetsPartitionResponse::default();
                partition_response.partition_index = partition_id;

                let log_end_offset = self.get_log_end_offset(&topic_name, partition_id).await;

                // -2 = EARLIEST, -1 = LATEST
                let offset = match timestamp {
                    -2 => 0, // EARLIEST
                    _ => log_end_offset,
                };

                partition_response.error_code = 0;
                // Safe cast: offset fits in i64.
                #[allow(clippy::cast_possible_wrap)]
                {
                    partition_response.offset = offset as i64;
                }
                partition_response.timestamp = -1;

                topic_response.partitions.push(partition_response);
            }

            response.topics.push(topic_response);
        }

        codec::encode_response(
            request.api_key,
            request.api_version,
            request.correlation_id,
            &response,
        )
    }

    /// Handle `FindCoordinator` request.
    fn handle_find_coordinator(&self, request: &DecodedRequest) -> KafkaResult<BytesMut> {
        let mut body = request.body.clone();
        let find_coordinator_request =
            FindCoordinatorRequest::decode(&mut body, request.api_version)
                .map_err(KafkaError::decode)?;

        let key = find_coordinator_request.key.to_string();
        debug!(key = %key, "Finding coordinator");

        let mut response = FindCoordinatorResponse::default();

        // Safe cast: NodeId (u64) fits in i32 for reasonable cluster sizes.
        #[allow(clippy::cast_possible_truncation)]
        let node_id = self.service.node_id().get() as i32;

        response.error_code = 0;
        response.node_id = BrokerId(node_id);
        response.host = StrBytes::from_string(self.host.clone());
        response.port = self.port;

        if request.api_version >= 3 {
            let mut coordinator = Coordinator::default();
            coordinator.key = find_coordinator_request.key;
            coordinator.node_id = BrokerId(node_id);
            coordinator.host = StrBytes::from_string(self.host.clone());
            coordinator.port = self.port;
            coordinator.error_code = 0;
            response.coordinators.push(coordinator);
        }

        codec::encode_response(
            request.api_key,
            request.api_version,
            request.correlation_id,
            &response,
        )
    }
}

/// Count records in a Kafka `RecordBatch`.
fn count_records_in_batch(bytes: &Bytes) -> u64 {
    const RECORD_COUNT_OFFSET: usize = 57;

    if bytes.len() < RECORD_COUNT_OFFSET + 4 {
        return 0;
    }

    let count = i32::from_be_bytes([
        bytes[RECORD_COUNT_OFFSET],
        bytes[RECORD_COUNT_OFFSET + 1],
        bytes[RECORD_COUNT_OFFSET + 2],
        bytes[RECORD_COUNT_OFFSET + 3],
    ]);

    count.try_into().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_records_empty() {
        let bytes = Bytes::new();
        assert_eq!(count_records_in_batch(&bytes), 0);
    }

    #[test]
    fn test_count_records_too_short() {
        let bytes = Bytes::from_static(&[0; 60]);
        assert_eq!(count_records_in_batch(&bytes), 0);
    }

    #[test]
    fn test_count_records_valid() {
        let mut bytes = vec![0u8; 61];
        // Set record count at offset 57.
        bytes[57] = 0;
        bytes[58] = 0;
        bytes[59] = 0;
        bytes[60] = 5; // 5 records
        assert_eq!(count_records_in_batch(&Bytes::from(bytes)), 5);
    }
}

//! Request handler for Kafka protocol messages.
//!
//! Dispatches incoming requests to the appropriate API handler and maps
//! operations to the underlying Helix storage.
//!
//! # Storage Architecture
//!
//! Uses protocol-agnostic blob storage via `helix_server::storage::DurablePartition`.
//! Kafka `RecordBatch` bytes are stored as-is without parsing, enabling
//! zero-copy on the fetch path. Data is persisted to WAL for durability.

use std::collections::HashMap;
use std::path::PathBuf;

use bytes::{Bytes, BytesMut};
use helix_core::{PartitionId, TopicId};
use helix_server::storage::{DurablePartition, DurablePartitionConfig};
use tokio::sync::RwLock;
use kafka_protocol::{
    messages::{
        ApiKey, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
        FindCoordinatorRequest, FindCoordinatorResponse,
        ListOffsetsRequest, ListOffsetsResponse,
        MetadataRequest, MetadataResponse,
        OffsetCommitRequest, OffsetCommitResponse,
        OffsetFetchRequest, OffsetFetchResponse,
        ProduceRequest, ProduceResponse,
        api_versions_response::ApiVersion,
        fetch_response::{FetchableTopicResponse, PartitionData},
        find_coordinator_response::Coordinator,
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::MetadataResponseBroker,
        offset_commit_response::{OffsetCommitResponsePartition, OffsetCommitResponseTopic},
        offset_fetch_response::{OffsetFetchResponsePartition, OffsetFetchResponseTopic},
    },
    protocol::{Decodable, StrBytes},
};
use tracing::{debug, info, warn};

use crate::{
    codec::{self, DecodedRequest},
    error::{KafkaCompatError, KafkaCompatResult},
};

/// Supported API versions.
///
/// Each tuple is (`api_key`, `min_version`, `max_version`).
const SUPPORTED_APIS: &[(i16, i16, i16)] = &[
    (ApiKey::ApiVersions as i16, 0, 3),
    (ApiKey::Metadata as i16, 0, 12),
    (ApiKey::Produce as i16, 0, 9),
    (ApiKey::Fetch as i16, 0, 12),
    (ApiKey::ListOffsets as i16, 0, 7),
    (ApiKey::OffsetCommit as i16, 0, 8),
    (ApiKey::OffsetFetch as i16, 0, 8),
    (ApiKey::FindCoordinator as i16, 0, 4),
];

/// Key for topic/partition lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic: String,
    partition: i32,
}

/// Key for consumer group committed offset tracking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ConsumerGroupPartitionKey {
    group_id: String,
    topic: String,
    partition: i32,
}

/// Topic ID counter for generating unique topic IDs.
static NEXT_TOPIC_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Handler context containing Helix storage and configuration.
///
/// Uses protocol-agnostic blob storage for Kafka `RecordBatch` data.
/// Data is persisted to WAL via `DurablePartition`.
pub struct HandlerContext {
    /// Node ID of this Helix node.
    pub node_id: i32,
    /// Hostname for this node.
    pub host: String,
    /// Kafka-compat port.
    pub port: i32,
    /// Cluster ID.
    pub cluster_id: String,
    /// Data directory for WAL storage.
    data_dir: PathBuf,
    /// Topic name to ID mapping.
    topic_ids: std::sync::RwLock<HashMap<String, TopicId>>,
    /// Partition storage (topic+partition -> `DurablePartition` with blob storage).
    partitions: RwLock<HashMap<PartitionKey, DurablePartition>>,
    /// Consumer group committed offsets (group+topic+partition -> offset).
    committed_offsets: std::sync::RwLock<HashMap<ConsumerGroupPartitionKey, i64>>,
}

impl HandlerContext {
    /// Create a new handler context.
    #[must_use]
    pub fn new(node_id: i32, host: String, port: i32, data_dir: PathBuf) -> Self {
        Self {
            node_id,
            host,
            port,
            cluster_id: "helix-cluster".to_string(),
            data_dir,
            topic_ids: std::sync::RwLock::new(HashMap::new()),
            partitions: RwLock::new(HashMap::new()),
            committed_offsets: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Gets or creates a topic ID for the given topic name.
    fn get_or_create_topic_id(&self, topic: &str) -> TopicId {
        // Try read lock first.
        {
            let topic_ids = self.topic_ids.read().unwrap();
            if let Some(&id) = topic_ids.get(topic) {
                return id;
            }
        }

        // Need write lock to create.
        let mut topic_ids = self.topic_ids.write().unwrap();
        *topic_ids.entry(topic.to_string()).or_insert_with(|| {
            TopicId::new(NEXT_TOPIC_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
        })
    }

    /// Gets or creates a partition for the given topic and partition ID.
    ///
    /// Creates the underlying Helix `DurablePartition` with WAL-backed blob storage.
    async fn ensure_partition(&self, topic: &str, partition: i32) {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        // Check if partition exists.
        {
            let partitions = self.partitions.read().await;
            if partitions.contains_key(&key) {
                return;
            }
        }

        // Create new partition with WAL-backed blob storage.
        let topic_id = self.get_or_create_topic_id(topic);
        // Safe cast: partition ID fits in u32.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(u64::from(partition as u32));

        // Create partition directory: data_dir/topic_id/partition_id
        let partition_dir = self
            .data_dir
            .join(format!("topic-{}", topic_id.get()))
            .join(format!("partition-{}", partition_id.get()));

        let config = DurablePartitionConfig::new(partition_dir, topic_id, partition_id);

        // Open the durable partition (creates WAL if needed).
        let new_partition = DurablePartition::open(config)
            .await
            .expect("failed to open durable partition");

        let mut partitions = self.partitions.write().await;
        partitions.entry(key).or_insert(new_partition);
    }

    /// Store a blob (Kafka `RecordBatch`) in the partition.
    ///
    /// Returns the base offset assigned to this batch.
    #[allow(clippy::significant_drop_tightening)]
    async fn store_blob(&self, topic: &str, partition: i32, record_count: u32, data: Bytes) -> u64 {
        self.ensure_partition(topic, partition).await;

        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let mut partitions = self.partitions.write().await;
        let p = partitions
            .get_mut(&key)
            .expect("partition should exist after ensure");

        // append_blob returns the base offset.
        p.append_blob(data, record_count)
            .await
            .expect("append_blob should not fail")
            .get()
    }

    /// Get the current log end offset for a partition.
    async fn log_end_offset(&self, topic: &str, partition: i32) -> u64 {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let partitions = self.partitions.read().await;
        partitions
            .get(&key)
            .map_or(0, |p| p.blob_log_end_offset().get())
    }

    /// Fetch blobs starting from the given offset.
    ///
    /// Returns raw Kafka `RecordBatch` bytes that overlap with the requested range.
    #[allow(clippy::cast_sign_loss, clippy::significant_drop_tightening)]
    async fn fetch_blobs(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_bytes: i32,
    ) -> Vec<Bytes> {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let partitions = self.partitions.read().await;
        let Some(p) = partitions.get(&key) else {
            return vec![];
        };

        let max_bytes_u32 = if max_bytes < 0 {
            u32::MAX
        } else {
            max_bytes as u32
        };

        p.read_blobs(helix_core::Offset::new(start_offset), max_bytes_u32)
            .into_iter()
            .map(|blob| blob.data)
            .collect()
    }

    /// Commit an offset for a consumer group.
    fn commit_offset(&self, group_id: &str, topic: &str, partition: i32, offset: i64) {
        let key = ConsumerGroupPartitionKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };

        let mut offsets = self.committed_offsets.write().unwrap();
        offsets.insert(key, offset);
    }

    /// Fetch the committed offset for a consumer group.
    ///
    /// Returns -1 if no offset has been committed.
    fn fetch_committed_offset(&self, group_id: &str, topic: &str, partition: i32) -> i64 {
        let key = ConsumerGroupPartitionKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };

        let offsets = self.committed_offsets.read().unwrap();
        offsets.get(&key).copied().unwrap_or(-1)
    }

    /// Check if a partition exists (has been written to).
    async fn partition_exists(&self, topic: &str, partition: i32) -> bool {
        let key = PartitionKey {
            topic: topic.to_string(),
            partition,
        };

        let partitions = self.partitions.read().await;
        partitions
            .get(&key)
            .is_some_and(DurablePartition::blob_partition_exists)
    }

    /// Get all known topics and their partitions.
    #[allow(clippy::significant_drop_tightening)]
    async fn get_known_topics(&self) -> HashMap<String, Vec<i32>> {
        let partitions = self.partitions.read().await;
        let mut topics: HashMap<String, Vec<i32>> = HashMap::new();

        for key in partitions.keys() {
            topics
                .entry(key.topic.clone())
                .or_default()
                .push(key.partition);
        }

        // Sort partitions for consistent output.
        for partitions in topics.values_mut() {
            partitions.sort_unstable();
        }

        topics
    }
}

/// Handle a decoded request and return the encoded response.
///
/// # Errors
///
/// Returns an error if the API is unsupported or encoding/decoding fails.
pub async fn handle_request(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    debug!(
        api_key = request.api_key,
        api_version = request.api_version,
        correlation_id = request.correlation_id,
        "Handling request"
    );

    match request.api_key {
        x if x == ApiKey::ApiVersions as i16 => handle_api_versions(ctx, request),
        x if x == ApiKey::Metadata as i16 => handle_metadata(ctx, request).await,
        x if x == ApiKey::Produce as i16 => handle_produce(ctx, request).await,
        x if x == ApiKey::Fetch as i16 => handle_fetch(ctx, request).await,
        x if x == ApiKey::ListOffsets as i16 => handle_list_offsets(ctx, request).await,
        x if x == ApiKey::FindCoordinator as i16 => handle_find_coordinator(ctx, request),
        x if x == ApiKey::OffsetCommit as i16 => handle_offset_commit(ctx, request),
        x if x == ApiKey::OffsetFetch as i16 => handle_offset_fetch(ctx, request),
        _ => {
            warn!(api_key = request.api_key, "Unsupported API");
            Err(KafkaCompatError::UnsupportedApi {
                api_key: request.api_key,
            })
        }
    }
}

/// Handle `ApiVersions` request.
///
/// This is typically the first request a client sends to discover supported APIs.
fn handle_api_versions(
    _ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    let mut response = ApiVersionsResponse::default();

    // Populate supported API versions.
    for &(api_key, min_version, max_version) in SUPPORTED_APIS {
        let mut api_version = ApiVersion::default();
        api_version.api_key = api_key;
        api_version.min_version = min_version;
        api_version.max_version = max_version;
        response.api_keys.push(api_version);
    }

    response.error_code = 0; // No error.

    codec::encode_response(
        request.api_key,
        request.api_version,
        request.correlation_id,
        &response,
    )
}

/// Handle Metadata request.
///
/// Returns broker and topic/partition information.
async fn handle_metadata(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    use kafka_protocol::messages::metadata_response::{
        MetadataResponsePartition, MetadataResponseTopic,
    };
    use kafka_protocol::messages::TopicName;

    // Decode the metadata request body.
    let mut body = request.body.clone();
    let metadata_request = MetadataRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    let mut response = MetadataResponse::default();

    // Add this node as a broker.
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = BrokerId(ctx.node_id);
    broker.host = StrBytes::from_string(ctx.host.clone());
    broker.port = ctx.port;
    response.brokers.push(broker);

    // Set cluster ID (version 2+).
    response.cluster_id = Some(StrBytes::from_string(ctx.cluster_id.clone()));

    // Set controller ID.
    response.controller_id = BrokerId(ctx.node_id);

    // Get all known topics and partitions.
    let known_topics = ctx.get_known_topics().await;

    // Build topic metadata based on request.
    // If topics is None or empty, return all known topics.
    // If topics is specified, return only those topics.
    let requested_topics: Option<Vec<String>> = metadata_request.topics.as_ref().map(|topics| {
        topics
            .iter()
            .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
            .collect()
    });

    for (topic_name, partitions) in &known_topics {
        // If specific topics requested, filter to those.
        if let Some(ref requested) = requested_topics {
            if !requested.contains(topic_name) {
                continue;
            }
        }

        let mut topic_response = MetadataResponseTopic::default();
        topic_response.error_code = 0;
        topic_response.name = Some(TopicName(StrBytes::from_string(topic_name.clone())));
        topic_response.is_internal = false;

        // Add partition metadata.
        for &partition_id in partitions {
            let mut partition_response = MetadataResponsePartition::default();
            partition_response.error_code = 0;
            partition_response.partition_index = partition_id;
            partition_response.leader_id = BrokerId(ctx.node_id);
            partition_response.leader_epoch = 0;
            partition_response.replica_nodes = vec![BrokerId(ctx.node_id)];
            partition_response.isr_nodes = vec![BrokerId(ctx.node_id)];
            partition_response.offline_replicas = vec![];

            topic_response.partitions.push(partition_response);
        }

        response.topics.push(topic_response);
    }

    // If specific topics were requested but not found, return them with error code.
    if let Some(ref requested) = requested_topics {
        for topic_name in requested {
            if !known_topics.contains_key(topic_name) {
                let mut topic_response = MetadataResponseTopic::default();
                topic_response.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION
                topic_response.name = Some(TopicName(StrBytes::from_string(topic_name.clone())));
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

/// Handle Produce request.
///
/// Writes records to Helix partitions.
async fn handle_produce(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the produce request body.
    let mut body = request.body.clone();
    let produce_request = ProduceRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    debug!(
        acks = produce_request.acks,
        timeout_ms = produce_request.timeout_ms,
        num_topics = produce_request.topic_data.len(),
        "Processing produce request"
    );

    let mut response = ProduceResponse::default();

    // Process each topic.
    for topic_data in &produce_request.topic_data {
        let topic_name = topic_data.name.to_string();
        debug!(topic = %topic_name, "Processing topic");

        let mut topic_response =
            kafka_protocol::messages::produce_response::TopicProduceResponse::default();
        topic_response.name = topic_data.name.clone();

        // Process each partition.
        for partition_data in &topic_data.partition_data {
            let partition_id = partition_data.index;
            let records_bytes = partition_data.records.as_ref();

            // Count records in the batch.
            let record_count = records_bytes.map_or(0, count_records_in_batch);

            debug!(
                topic = %topic_name,
                partition = partition_id,
                record_count,
                records_size = records_bytes.map_or(0, bytes::Bytes::len),
                "Processing partition"
            );

            let mut partition_response =
                kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
            partition_response.index = partition_id;

            // Store blob and get base offset (or get current end offset if empty).
            // Safe cast: record_count is u64 from counting, but limited by batch size.
            #[allow(clippy::cast_possible_truncation)]
            let base_offset = if record_count > 0 {
                if let Some(data) = records_bytes {
                    // Store raw Kafka `RecordBatch` bytes as blob.
                    let offset = ctx
                        .store_blob(&topic_name, partition_id, record_count as u32, data.clone())
                        .await;

                    info!(
                        topic = %topic_name,
                        partition = partition_id,
                        base_offset = offset,
                        record_count,
                        "Produced records"
                    );

                    offset
                } else {
                    ctx.log_end_offset(&topic_name, partition_id).await
                }
            } else {
                // Empty batch - return current end offset without advancing.
                ctx.log_end_offset(&topic_name, partition_id).await
            };

            partition_response.error_code = 0;
            // Safe cast: base_offset is u64, Kafka protocol uses i64.
            #[allow(clippy::cast_possible_wrap)]
            {
                partition_response.base_offset = base_offset as i64;
            }
            partition_response.log_append_time_ms = -1; // Not using log append time.
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

/// Count records in a Kafka `RecordBatch`.
///
/// `RecordBatch` format (relevant fields):
/// - Offset 57: `recordCount` (4 bytes, big-endian)
///
/// Returns 0 if the batch is too short to parse.
fn count_records_in_batch(bytes: &bytes::Bytes) -> u64 {
    // RecordBatch header is at least 61 bytes before records start.
    // The recordCount field is at offset 57.
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

    // Kafka uses signed i32 but count should never be negative.
    // Use try_into to safely convert, returning 0 if negative.
    count.try_into().unwrap_or(0)
}

/// Handle Fetch request.
///
/// Reads records from Helix partitions.
#[allow(clippy::too_many_lines)]
async fn handle_fetch(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the fetch request body.
    let mut body = request.body.clone();
    let fetch_request = FetchRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    debug!(
        max_wait_ms = fetch_request.max_wait_ms,
        min_bytes = fetch_request.min_bytes,
        max_bytes = fetch_request.max_bytes,
        num_topics = fetch_request.topics.len(),
        "Processing fetch request"
    );

    let mut response = FetchResponse::default();

    // Process each topic.
    for topic_data in &fetch_request.topics {
        let topic_name = topic_data.topic.to_string();
        debug!(topic = %topic_name, "Fetching topic");

        let mut topic_response = FetchableTopicResponse::default();
        topic_response.topic = topic_data.topic.clone();

        // Process each partition.
        for partition_data in &topic_data.partitions {
            let partition_id = partition_data.partition;

            // Safe cast: fetch_offset is i64 but should be non-negative.
            #[allow(clippy::cast_sign_loss)]
            let fetch_offset = partition_data.fetch_offset.max(0) as u64;

            debug!(
                topic = %topic_name,
                partition = partition_id,
                fetch_offset,
                partition_max_bytes = partition_data.partition_max_bytes,
                "Fetching partition"
            );

            let mut partition_response = PartitionData::default();
            partition_response.partition_index = partition_id;

            // Check if partition exists.
            if !ctx.partition_exists(&topic_name, partition_id).await {
                // Return UNKNOWN_TOPIC_OR_PARTITION error.
                partition_response.error_code = 3;
                partition_response.high_watermark = -1;
                partition_response.last_stable_offset = -1;
                partition_response.log_start_offset = -1;
                partition_response.records = None;

                debug!(
                    topic = %topic_name,
                    partition = partition_id,
                    "Unknown topic or partition"
                );

                topic_response.partitions.push(partition_response);
                continue;
            }

            // Get the high watermark (log end offset).
            let high_watermark = ctx.log_end_offset(&topic_name, partition_id).await;

            // Fetch blobs from storage.
            let blobs = ctx
                .fetch_blobs(
                    &topic_name,
                    partition_id,
                    fetch_offset,
                    partition_data.partition_max_bytes,
                )
                .await;

            if blobs.is_empty() && fetch_offset >= high_watermark {
                // No data available yet - return empty response.
                partition_response.error_code = 0;
                #[allow(clippy::cast_possible_wrap)]
                {
                    partition_response.high_watermark = high_watermark as i64;
                    partition_response.last_stable_offset = high_watermark as i64;
                    partition_response.log_start_offset = 0;
                }
                partition_response.records = None;

                debug!(
                    topic = %topic_name,
                    partition = partition_id,
                    high_watermark,
                    "No records available"
                );
            } else {
                // Concatenate all blobs into a single records blob.
                let mut records_data = BytesMut::new();
                for blob in &blobs {
                    records_data.extend_from_slice(blob);
                }
                let total_bytes = records_data.len();

                partition_response.error_code = 0;
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
///
/// Returns the earliest or latest offset for partitions.
/// Timestamp-based lookup returns the latest offset (simplified).
async fn handle_list_offsets(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the list offsets request body.
    let mut body = request.body.clone();
    let list_offsets_request = ListOffsetsRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    debug!(
        num_topics = list_offsets_request.topics.len(),
        "Processing list offsets request"
    );

    let mut response = ListOffsetsResponse::default();

    // Process each topic.
    for topic_data in &list_offsets_request.topics {
        let topic_name = topic_data.name.to_string();
        debug!(topic = %topic_name, "Listing offsets for topic");

        let mut topic_response = ListOffsetsTopicResponse::default();
        topic_response.name = topic_data.name.clone();

        // Process each partition.
        for partition_data in &topic_data.partitions {
            let partition_id = partition_data.partition_index;
            let timestamp = partition_data.timestamp;

            debug!(
                topic = %topic_name,
                partition = partition_id,
                timestamp,
                "Listing offsets for partition"
            );

            let mut partition_response = ListOffsetsPartitionResponse::default();
            partition_response.partition_index = partition_id;

            // Get the current log end offset (high watermark).
            let log_end_offset = ctx.log_end_offset(&topic_name, partition_id).await;

            // Kafka timestamp conventions:
            // -1 = LATEST (high watermark)
            // -2 = EARLIEST (log start offset)
            // >= 0 = lookup by timestamp (we return LATEST as simplification)
            let offset = match timestamp {
                -2 => 0, // EARLIEST - log start offset is always 0 in our implementation
                _ => log_end_offset, // LATEST or timestamp lookup - return high watermark
            };

            partition_response.error_code = 0;
            #[allow(clippy::cast_possible_wrap)]
            {
                partition_response.offset = offset as i64;
            }
            partition_response.timestamp = -1; // Not using timestamps

            debug!(
                topic = %topic_name,
                partition = partition_id,
                offset,
                "Returning offset"
            );

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
///
/// Returns this node as the coordinator for all consumer groups.
fn handle_find_coordinator(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the find coordinator request body.
    let mut body = request.body.clone();
    let find_coordinator_request = FindCoordinatorRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    let key = find_coordinator_request.key.to_string();
    debug!(key = %key, "Finding coordinator");

    let mut response = FindCoordinatorResponse::default();

    // This node is the coordinator for all groups (single-node cluster).
    response.error_code = 0;
    response.node_id = BrokerId(ctx.node_id);
    response.host = StrBytes::from_string(ctx.host.clone());
    response.port = ctx.port;

    // For v3+ responses, we also need to set coordinators array.
    if request.api_version >= 3 {
        let mut coordinator = Coordinator::default();
        coordinator.key = find_coordinator_request.key;
        coordinator.node_id = BrokerId(ctx.node_id);
        coordinator.host = StrBytes::from_string(ctx.host.clone());
        coordinator.port = ctx.port;
        coordinator.error_code = 0;
        response.coordinators.push(coordinator);
    }

    debug!(
        key = %key,
        node_id = ctx.node_id,
        host = %ctx.host,
        port = ctx.port,
        "Returning coordinator"
    );

    codec::encode_response(
        request.api_key,
        request.api_version,
        request.correlation_id,
        &response,
    )
}

/// Handle `OffsetCommit` request.
///
/// Commits consumer group offsets.
fn handle_offset_commit(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the offset commit request body.
    let mut body = request.body.clone();
    let offset_commit_request = OffsetCommitRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    let group_id = offset_commit_request.group_id.to_string();
    debug!(
        group_id = %group_id,
        num_topics = offset_commit_request.topics.len(),
        "Processing offset commit"
    );

    let mut response = OffsetCommitResponse::default();

    // Process each topic.
    for topic_data in &offset_commit_request.topics {
        let topic_name = topic_data.name.to_string();

        let mut topic_response = OffsetCommitResponseTopic::default();
        topic_response.name = topic_data.name.clone();

        // Process each partition.
        for partition_data in &topic_data.partitions {
            let partition_id = partition_data.partition_index;
            let offset = partition_data.committed_offset;

            // Commit the offset.
            ctx.commit_offset(&group_id, &topic_name, partition_id, offset);

            info!(
                group_id = %group_id,
                topic = %topic_name,
                partition = partition_id,
                offset,
                "Committed offset"
            );

            let mut partition_response = OffsetCommitResponsePartition::default();
            partition_response.partition_index = partition_id;
            partition_response.error_code = 0;

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

/// Handle `OffsetFetch` request.
///
/// Fetches committed consumer group offsets.
fn handle_offset_fetch(
    ctx: &HandlerContext,
    request: &DecodedRequest,
) -> KafkaCompatResult<BytesMut> {
    // Decode the offset fetch request body.
    let mut body = request.body.clone();
    let offset_fetch_request = OffsetFetchRequest::decode(&mut body, request.api_version)
        .map_err(KafkaCompatError::decode)?;

    let group_id = offset_fetch_request.group_id.to_string();
    debug!(
        group_id = %group_id,
        num_topics = offset_fetch_request.topics.as_ref().map_or(0, Vec::len),
        "Processing offset fetch"
    );

    let mut response = OffsetFetchResponse::default();
    response.error_code = 0;

    // Process each topic (if specified).
    if let Some(topics) = &offset_fetch_request.topics {
        for topic_data in topics {
            let topic_name = topic_data.name.to_string();

            let mut topic_response = OffsetFetchResponseTopic::default();
            topic_response.name = topic_data.name.clone();

            // Process each partition.
            for partition_data in &topic_data.partition_indexes {
                let partition_id = *partition_data;

                // Fetch the committed offset.
                let offset = ctx.fetch_committed_offset(&group_id, &topic_name, partition_id);

                debug!(
                    group_id = %group_id,
                    topic = %topic_name,
                    partition = partition_id,
                    offset,
                    "Fetched committed offset"
                );

                let mut partition_response = OffsetFetchResponsePartition::default();
                partition_response.partition_index = partition_id;
                partition_response.committed_offset = offset;
                partition_response.error_code = 0;
                partition_response.metadata = Some(StrBytes::from_string(String::new()));

                topic_response.partitions.push(partition_response);
            }

            response.topics.push(topic_response);
        }
    }

    codec::encode_response(
        request.api_key,
        request.api_version,
        request.correlation_id,
        &response,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiVersionsRequest;
    use kafka_protocol::protocol::Encodable;

    fn test_context() -> HandlerContext {
        let temp_dir = std::env::temp_dir().join(format!(
            "helix-kafka-test-{}",
            std::process::id()
        ));
        HandlerContext::new(1, "localhost".to_string(), 9092, temp_dir)
    }

    #[tokio::test]
    async fn test_api_versions() {
        let ctx = test_context();

        // Build ApiVersions request.
        let mut request_body = BytesMut::new();
        let api_versions_req = ApiVersionsRequest::default();
        api_versions_req.encode(&mut request_body, 0).unwrap();

        // Build full request with header.
        // kafka-protocol crate only supports header versions 1-2.
        let mut header = kafka_protocol::messages::RequestHeader::default();
        header.request_api_key = ApiKey::ApiVersions as i16;
        header.request_api_version = 0;
        header.correlation_id = 123;

        let mut full_request = BytesMut::new();
        header.encode(&mut full_request, 1).unwrap();
        full_request.extend_from_slice(&request_body);

        // Decode and handle.
        let decoded = codec::decode_request_header(full_request.freeze()).unwrap();
        let response = handle_request(&ctx, &decoded).await.unwrap();

        // Verify response is not empty.
        assert!(!response.is_empty());
    }

    #[tokio::test]
    async fn test_metadata() {
        let ctx = test_context();

        // Build Metadata request.
        let mut request_body = BytesMut::new();
        let metadata_req = MetadataRequest::default();
        metadata_req.encode(&mut request_body, 0).unwrap();

        // Build full request with header.
        let mut header = kafka_protocol::messages::RequestHeader::default();
        header.request_api_key = ApiKey::Metadata as i16;
        header.request_api_version = 0;
        header.correlation_id = 456;

        let mut full_request = BytesMut::new();
        header.encode(&mut full_request, 1).unwrap();
        full_request.extend_from_slice(&request_body);

        // Decode and handle.
        let decoded = codec::decode_request_header(full_request.freeze()).unwrap();
        let response = handle_request(&ctx, &decoded).await.unwrap();

        // Verify response is not empty.
        assert!(!response.is_empty());
    }
}

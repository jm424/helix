//! Integration tests for Kafka-compat server.
//!
//! These tests verify the full TCP stack by connecting real TCP clients
//! to the server and exchanging Kafka protocol messages.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, BrokerId, FetchRequest, FindCoordinatorRequest,
    ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest,
    RequestHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use helix_kafka_compat::server::KafkaServerConfig;

/// Counter for unique test directory names.
static TEST_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Creates a unique temp directory for test data.
fn test_data_dir() -> std::path::PathBuf {
    let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "helix-kafka-integration-{}-{}",
        std::process::id(),
        counter
    ))
}

/// Builds a Kafka request with length-prefixed framing.
fn build_request(api_key: ApiKey, api_version: i16, correlation_id: i32, body: &[u8]) -> BytesMut {
    // Build header.
    let mut header = RequestHeader::default();
    header.request_api_key = api_key as i16;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;

    // Encode header + body.
    let mut payload = BytesMut::new();
    header.encode(&mut payload, 1).unwrap();
    payload.extend_from_slice(body);

    // Add length prefix.
    let mut frame = BytesMut::new();
    #[allow(clippy::cast_possible_truncation)]
    let length = payload.len() as u32;
    frame.put_u32(length);
    frame.extend_from_slice(&payload);

    frame
}

/// Reads a Kafka response with length-prefixed framing.
async fn read_response(stream: &mut TcpStream) -> BytesMut {
    // Read length prefix.
    let mut length_buf = [0u8; 4];
    stream.read_exact(&mut length_buf).await.unwrap();
    let length = u32::from_be_bytes(length_buf) as usize;

    // Read payload.
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await.unwrap();

    BytesMut::from(&payload[..])
}

#[tokio::test]
async fn test_api_versions_over_tcp() {
    // Start server on a random port.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = KafkaServerConfig::new(addr, 1, test_data_dir());

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    // Spawn server task.
    let server_handle = tokio::spawn(async move {
        loop {
            let (stream, _peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let ctx = Arc::new(helix_kafka_compat::handler::HandlerContext::new(
                1,
                "localhost".to_string(),
                actual_addr.port() as i32,
                test_data_dir(),
            ));

            tokio::spawn(async move {
                let _ = handle_test_connection(stream, ctx).await;
            });
        }
    });

    // Give server time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client.
    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Build ApiVersions request.
    let api_versions_req = ApiVersionsRequest::default();
    let mut body = BytesMut::new();
    api_versions_req.encode(&mut body, 0).unwrap();

    let request = build_request(ApiKey::ApiVersions, 0, 123, &body);

    // Send request.
    client.write_all(&request).await.unwrap();

    // Read response.
    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("response timeout");

    // Decode response header.
    let response_bytes = response.freeze();
    let correlation_id = i32::from_be_bytes([
        response_bytes[0],
        response_bytes[1],
        response_bytes[2],
        response_bytes[3],
    ]);

    assert_eq!(correlation_id, 123, "correlation ID should match");
    assert!(response_bytes.len() > 4, "response should have body");

    // Clean up.
    drop(client);
    server_handle.abort();
}

#[tokio::test]
async fn test_metadata_over_tcp() {
    // Start server on a random port.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = KafkaServerConfig::new(addr, 1, test_data_dir());

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    // Spawn server task.
    let server_handle = tokio::spawn(async move {
        loop {
            let (stream, _peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let ctx = Arc::new(helix_kafka_compat::handler::HandlerContext::new(
                1,
                "localhost".to_string(),
                actual_addr.port() as i32,
                test_data_dir(),
            ));

            tokio::spawn(async move {
                let _ = handle_test_connection(stream, ctx).await;
            });
        }
    });

    // Give server time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client.
    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Build Metadata request.
    let metadata_req = MetadataRequest::default();
    let mut body = BytesMut::new();
    metadata_req.encode(&mut body, 0).unwrap();

    let request = build_request(ApiKey::Metadata, 0, 456, &body);

    // Send request.
    client.write_all(&request).await.unwrap();

    // Read response.
    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("response timeout");

    // Decode response header.
    let correlation_id = i32::from_be_bytes([
        response[0],
        response[1],
        response[2],
        response[3],
    ]);

    assert_eq!(correlation_id, 456, "correlation ID should match");
    assert!(response.len() > 4, "response should have body");

    // Clean up.
    drop(client);
    server_handle.abort();
}

/// Simplified connection handler for tests.
async fn handle_test_connection(
    mut stream: tokio::net::TcpStream,
    ctx: Arc<helix_kafka_compat::handler::HandlerContext>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);

    loop {
        let bytes_read = stream.read_buf(&mut read_buf).await?;
        if bytes_read == 0 {
            break;
        }

        // Process frames.
        while let Some(payload) = helix_kafka_compat::codec::read_frame(&mut read_buf)? {
            let request = helix_kafka_compat::codec::decode_request_header(payload)?;
            let response_body = helix_kafka_compat::handler::handle_request(&ctx, &request).await?;

            // Write response with length prefix.
            let mut response_frame = BytesMut::new();
            helix_kafka_compat::codec::write_frame(&mut response_frame, &response_body);
            stream.write_all(&response_frame).await?;
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_produce_and_fetch_over_tcp() {
    // Start server on a random port.
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = KafkaServerConfig::new(addr, 1, test_data_dir());

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    // Create a shared context for the server.
    let ctx = Arc::new(helix_kafka_compat::handler::HandlerContext::new(
        1,
        "localhost".to_string(),
        actual_addr.port() as i32,
        test_data_dir(),
    ));
    let server_ctx = ctx.clone();

    // Spawn server task.
    let server_handle = tokio::spawn(async move {
        loop {
            let (stream, _peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let ctx = server_ctx.clone();
            tokio::spawn(async move {
                let _ = handle_test_connection(stream, ctx).await;
            });
        }
    });

    // Give server time to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client.
    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Build a minimal Produce request with a simple RecordBatch.
    let topic_name = "test-topic";
    let partition = 0i32;

    // Create a minimal RecordBatch (Kafka 2.x format).
    // This is a simplified version - real clients would use proper encoding.
    let record_batch = build_minimal_record_batch(3); // 3 records

    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data = kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(record_batch.clone().into());
    topic_data.partition_data.push(partition_data);

    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap(); // Use version 3

    let request = build_request(ApiKey::Produce, 3, 789, &body);

    // Send produce request.
    client.write_all(&request).await.unwrap();

    // Read produce response.
    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("produce response timeout");

    // Verify produce response.
    let correlation_id = i32::from_be_bytes([
        response[0], response[1], response[2], response[3],
    ]);
    assert_eq!(correlation_id, 789, "produce correlation ID should match");

    // Decode produce response to check base_offset.
    let mut response_body = response.freeze();
    response_body.advance(4); // Skip correlation_id
    let produce_response = kafka_protocol::messages::ProduceResponse::decode(&mut response_body, 3)
        .expect("should decode produce response");

    assert_eq!(produce_response.responses.len(), 1, "should have one topic response");
    let topic_response = &produce_response.responses[0];
    assert_eq!(topic_response.partition_responses.len(), 1, "should have one partition response");
    let partition_response = &topic_response.partition_responses[0];
    assert_eq!(partition_response.error_code, 0, "produce should succeed");
    assert_eq!(partition_response.base_offset, 0, "first produce should get offset 0");

    // Now send a Fetch request.
    let mut fetch_req = FetchRequest::default();
    fetch_req.max_wait_ms = 100;
    fetch_req.min_bytes = 1;
    fetch_req.max_bytes = 1024 * 1024;

    let mut fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut fetch_partition = kafka_protocol::messages::fetch_request::FetchPartition::default();
    fetch_partition.partition = partition;
    fetch_partition.fetch_offset = 0;
    fetch_partition.partition_max_bytes = 1024 * 1024;
    fetch_topic.partitions.push(fetch_partition);

    fetch_req.topics.push(fetch_topic);

    let mut fetch_body = BytesMut::new();
    fetch_req.encode(&mut fetch_body, 4).unwrap(); // Use version 4

    let fetch_request = build_request(ApiKey::Fetch, 4, 790, &fetch_body);

    // Send fetch request.
    client.write_all(&fetch_request).await.unwrap();

    // Read fetch response.
    let fetch_response_bytes = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("fetch response timeout");

    // Verify fetch response.
    let fetch_correlation_id = i32::from_be_bytes([
        fetch_response_bytes[0], fetch_response_bytes[1],
        fetch_response_bytes[2], fetch_response_bytes[3],
    ]);
    assert_eq!(fetch_correlation_id, 790, "fetch correlation ID should match");

    // Decode fetch response.
    let mut fetch_response_body = fetch_response_bytes.freeze();
    fetch_response_body.advance(4); // Skip correlation_id
    let fetch_response = kafka_protocol::messages::FetchResponse::decode(&mut fetch_response_body, 4)
        .expect("should decode fetch response");

    assert_eq!(fetch_response.responses.len(), 1, "should have one topic in fetch response");
    let fetch_topic_response = &fetch_response.responses[0];
    assert_eq!(fetch_topic_response.partitions.len(), 1, "should have one partition");
    let fetch_partition_response = &fetch_topic_response.partitions[0];
    assert_eq!(fetch_partition_response.error_code, 0, "fetch should succeed");
    assert_eq!(fetch_partition_response.high_watermark, 3, "high watermark should be 3");

    // Records should be present.
    assert!(fetch_partition_response.records.is_some(), "should have records");

    // Clean up.
    drop(client);
    server_handle.abort();
}

/// Build a minimal Kafka RecordBatch for testing.
///
/// This creates a valid RecordBatch header with the specified number of records.
/// The actual record content is minimal but structurally valid.
fn build_minimal_record_batch(record_count: i32) -> BytesMut {
    let mut batch = BytesMut::new();

    // RecordBatch format (Kafka 2.x, magic=2):
    // baseOffset: i64 (8 bytes)
    batch.put_i64(0);

    // batchLength: i32 (4 bytes) - will be updated at the end
    let length_pos = batch.len();
    batch.put_i32(0); // Placeholder

    // partitionLeaderEpoch: i32 (4 bytes)
    batch.put_i32(0);

    // magic: i8 (1 byte) - must be 2 for new format
    batch.put_i8(2);

    // crc: i32 (4 bytes) - we'll use 0 for testing (not validated by our server)
    batch.put_i32(0);

    // attributes: i16 (2 bytes)
    batch.put_i16(0);

    // lastOffsetDelta: i32 (4 bytes)
    batch.put_i32(record_count - 1);

    // baseTimestamp: i64 (8 bytes)
    batch.put_i64(0);

    // maxTimestamp: i64 (8 bytes)
    batch.put_i64(0);

    // producerId: i64 (8 bytes)
    batch.put_i64(-1);

    // producerEpoch: i16 (2 bytes)
    batch.put_i16(-1);

    // baseSequence: i32 (4 bytes)
    batch.put_i32(-1);

    // recordCount: i32 (4 bytes) - THIS IS AT OFFSET 57
    batch.put_i32(record_count);

    // Now add minimal records (varint encoded).
    for i in 0..record_count {
        // length (varint) - minimal record is ~10 bytes
        batch.put_u8(10);
        // attributes (i8)
        batch.put_i8(0);
        // timestampDelta (varint)
        batch.put_u8(0);
        // offsetDelta (varint)
        batch.put_u8(i as u8);
        // keyLength (varint) - -1 means null
        batch.put_u8(0x01); // -1 in varint
        // valueLength (varint)
        batch.put_u8(4);
        // value
        batch.put_slice(b"test");
        // headers count (varint)
        batch.put_u8(0);
    }

    // Update batchLength.
    let batch_length = (batch.len() - length_pos - 4) as i32;
    let length_bytes = batch_length.to_be_bytes();
    batch[length_pos] = length_bytes[0];
    batch[length_pos + 1] = length_bytes[1];
    batch[length_pos + 2] = length_bytes[2];
    batch[length_pos + 3] = length_bytes[3];

    batch
}

/// Helper to create a test server and return the context and address.
async fn setup_test_server() -> (
    Arc<helix_kafka_compat::handler::HandlerContext>,
    SocketAddr,
    tokio::task::JoinHandle<()>,
) {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
    let config = KafkaServerConfig::new(addr, 1, test_data_dir());

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    let ctx = Arc::new(helix_kafka_compat::handler::HandlerContext::new(
        1,
        "localhost".to_string(),
        actual_addr.port() as i32,
        test_data_dir(),
    ));
    let server_ctx = ctx.clone();

    let server_handle = tokio::spawn(async move {
        loop {
            let (stream, _peer_addr) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };

            let ctx = server_ctx.clone();
            tokio::spawn(async move {
                let _ = handle_test_connection(stream, ctx).await;
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    (ctx, actual_addr, server_handle)
}

#[tokio::test]
async fn test_list_offsets_over_tcp() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    // First produce some records to have meaningful offsets.
    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "list-offsets-topic";
    let partition = 0i32;

    // Produce 5 records.
    let record_batch = build_minimal_record_batch(5);
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = partition;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap();
    let request = build_request(ApiKey::Produce, 3, 1000, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // Test ListOffsets - EARLIEST (-2).
    let mut list_offsets_req = ListOffsetsRequest::default();
    list_offsets_req.replica_id = BrokerId(-1);

    let mut topic = kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part =
        kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default();
    part.partition_index = partition;
    part.timestamp = -2; // EARLIEST
    topic.partitions.push(part);
    list_offsets_req.topics.push(topic);

    let mut body = BytesMut::new();
    list_offsets_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::ListOffsets, 1, 1001, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("list offsets response timeout");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 1001, "correlation ID should match");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let list_offsets_response =
        kafka_protocol::messages::ListOffsetsResponse::decode(&mut response_body, 1)
            .expect("should decode list offsets response");

    assert_eq!(list_offsets_response.topics.len(), 1);
    let topic_resp = &list_offsets_response.topics[0];
    assert_eq!(topic_resp.partitions.len(), 1);
    let part_resp = &topic_resp.partitions[0];
    assert_eq!(part_resp.error_code, 0, "ListOffsets should succeed");
    assert_eq!(part_resp.offset, 0, "earliest offset should be 0");

    // Test ListOffsets - LATEST (-1).
    let mut list_offsets_req = ListOffsetsRequest::default();
    list_offsets_req.replica_id = BrokerId(-1);

    let mut topic = kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part =
        kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default();
    part.partition_index = partition;
    part.timestamp = -1; // LATEST
    topic.partitions.push(part);
    list_offsets_req.topics.push(topic);

    let mut body = BytesMut::new();
    list_offsets_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::ListOffsets, 1, 1002, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("list offsets response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let list_offsets_response =
        kafka_protocol::messages::ListOffsetsResponse::decode(&mut response_body, 1)
            .expect("should decode list offsets response");

    let part_resp = &list_offsets_response.topics[0].partitions[0];
    assert_eq!(part_resp.error_code, 0, "ListOffsets should succeed");
    assert_eq!(part_resp.offset, 5, "latest offset should be 5 (next offset)");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

#[tokio::test]
async fn test_find_coordinator_over_tcp() {
    let (_ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Build FindCoordinator request.
    let mut find_coord_req = FindCoordinatorRequest::default();
    find_coord_req.key = StrBytes::from_string("test-consumer-group".to_string());
    find_coord_req.key_type = 0; // GROUP

    let mut body = BytesMut::new();
    find_coord_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::FindCoordinator, 1, 2000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("find coordinator response timeout");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 2000, "correlation ID should match");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let find_coord_response =
        kafka_protocol::messages::FindCoordinatorResponse::decode(&mut response_body, 1)
            .expect("should decode find coordinator response");

    assert_eq!(
        find_coord_response.error_code, 0,
        "FindCoordinator should succeed"
    );
    assert_eq!(find_coord_response.node_id, 1, "should return our node id");
    assert_eq!(
        find_coord_response.host.as_str(),
        "localhost",
        "should return our host"
    );
    assert_eq!(
        find_coord_response.port,
        actual_addr.port() as i32,
        "should return our port"
    );

    drop(client);
    server_handle.abort();
}

#[tokio::test]
async fn test_offset_commit_and_fetch_over_tcp() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let group_id = "test-consumer-group";
    let topic_name = "offset-test-topic";
    let partition = 0i32;
    let committed_offset = 42i64;

    // First, commit an offset.
    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = StrBytes::from_string(group_id.to_string()).into();
    commit_req.generation_id_or_member_epoch = -1; // Not using group coordination
    commit_req.member_id = StrBytes::from_string(String::new());

    let mut topic = kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part = kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default();
    part.partition_index = partition;
    part.committed_offset = committed_offset;
    part.committed_leader_epoch = 0;
    part.committed_metadata = Some(StrBytes::from_string("test-metadata".to_string()));
    topic.partitions.push(part);
    commit_req.topics.push(topic);

    let mut body = BytesMut::new();
    commit_req.encode(&mut body, 2).unwrap();
    let request = build_request(ApiKey::OffsetCommit, 2, 3000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("offset commit response timeout");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 3000, "correlation ID should match");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let commit_response =
        kafka_protocol::messages::OffsetCommitResponse::decode(&mut response_body, 2)
            .expect("should decode offset commit response");

    assert_eq!(commit_response.topics.len(), 1);
    let topic_resp = &commit_response.topics[0];
    assert_eq!(topic_resp.partitions.len(), 1);
    let part_resp = &topic_resp.partitions[0];
    assert_eq!(part_resp.error_code, 0, "OffsetCommit should succeed");

    // Now fetch the committed offset.
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = StrBytes::from_string(group_id.to_string()).into();

    let mut topic = kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));
    topic.partition_indexes = vec![partition];
    fetch_req.topics = Some(vec![topic]);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::OffsetFetch, 1, 3001, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("offset fetch response timeout");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 3001, "correlation ID should match");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response =
        kafka_protocol::messages::OffsetFetchResponse::decode(&mut response_body, 1)
            .expect("should decode offset fetch response");

    assert_eq!(fetch_response.topics.len(), 1);
    let topic_resp = &fetch_response.topics[0];
    assert_eq!(topic_resp.partitions.len(), 1);
    let part_resp = &topic_resp.partitions[0];
    assert_eq!(part_resp.error_code, 0, "OffsetFetch should succeed");
    assert_eq!(
        part_resp.committed_offset, committed_offset,
        "should return committed offset"
    );

    drop(client);
    drop(ctx);
    server_handle.abort();
}

#[tokio::test]
async fn test_metadata_response_structure() {
    let (_ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // First produce to create a topic.
    let topic_name = "metadata-test-topic";
    let record_batch = build_minimal_record_batch(1);
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap();
    let request = build_request(ApiKey::Produce, 3, 4000, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // Now request metadata for the topic.
    let mut metadata_req = MetadataRequest::default();
    let mut topic = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
    topic.name = Some(TopicName(StrBytes::from_string(topic_name.to_string())));
    metadata_req.topics = Some(vec![topic]);

    let mut body = BytesMut::new();
    metadata_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::Metadata, 1, 4001, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("metadata response timeout");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 4001, "correlation ID should match");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let metadata_response =
        kafka_protocol::messages::MetadataResponse::decode(&mut response_body, 1)
            .expect("should decode metadata response");

    // Verify broker info.
    assert!(!metadata_response.brokers.is_empty(), "should have brokers");
    let broker = &metadata_response.brokers[0];
    assert_eq!(broker.node_id, 1, "broker node_id should be 1");
    assert_eq!(broker.host.as_str(), "localhost", "broker host should match");
    assert_eq!(
        broker.port,
        actual_addr.port() as i32,
        "broker port should match"
    );

    // Verify topic info.
    assert_eq!(metadata_response.topics.len(), 1, "should have one topic");
    let topic_resp = &metadata_response.topics[0];
    assert_eq!(topic_resp.error_code, 0, "topic error_code should be 0");
    assert_eq!(
        topic_resp.name.as_ref().map(|n| n.as_str()),
        Some(topic_name),
        "topic name should match"
    );

    // Verify partition info.
    assert_eq!(topic_resp.partitions.len(), 1, "should have one partition");
    let partition_resp = &topic_resp.partitions[0];
    assert_eq!(
        partition_resp.error_code, 0,
        "partition error_code should be 0"
    );
    assert_eq!(
        partition_resp.partition_index, 0,
        "partition index should be 0"
    );
    assert_eq!(partition_resp.leader_id, 1, "leader should be node 1");
    assert!(!partition_resp.replica_nodes.is_empty(), "should have replicas");
    assert!(!partition_resp.isr_nodes.is_empty(), "should have ISR");

    drop(client);
    server_handle.abort();
}

#[tokio::test]
async fn test_fetch_unknown_topic_returns_error() {
    let (_ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Fetch from a topic that doesn't exist.
    let mut fetch_req = FetchRequest::default();
    fetch_req.max_wait_ms = 100;
    fetch_req.min_bytes = 1;
    fetch_req.max_bytes = 1024 * 1024;

    let mut fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_string("nonexistent-topic".to_string()));

    let mut fetch_partition = kafka_protocol::messages::fetch_request::FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.fetch_offset = 0;
    fetch_partition.partition_max_bytes = 1024 * 1024;
    fetch_topic.partitions.push(fetch_partition);
    fetch_req.topics.push(fetch_topic);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 4).unwrap();
    let request = build_request(ApiKey::Fetch, 4, 5000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("fetch response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response = kafka_protocol::messages::FetchResponse::decode(&mut response_body, 4)
        .expect("should decode fetch response");

    assert_eq!(fetch_response.responses.len(), 1);
    let topic_resp = &fetch_response.responses[0];
    assert_eq!(topic_resp.partitions.len(), 1);
    let part_resp = &topic_resp.partitions[0];

    // Should return UNKNOWN_TOPIC_OR_PARTITION (3).
    assert_eq!(
        part_resp.error_code, 3,
        "should return UNKNOWN_TOPIC_OR_PARTITION error"
    );

    drop(client);
    server_handle.abort();
}

#[tokio::test]
async fn test_offset_fetch_uncommitted_returns_invalid() {
    let (_ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // Fetch offset for a group/topic/partition that has no commits.
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = StrBytes::from_string("uncommitted-group".to_string()).into();

    let mut topic = kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string("uncommitted-topic".to_string()));
    topic.partition_indexes = vec![0];
    fetch_req.topics = Some(vec![topic]);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 1).unwrap();
    let request = build_request(ApiKey::OffsetFetch, 1, 6000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("offset fetch response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response =
        kafka_protocol::messages::OffsetFetchResponse::decode(&mut response_body, 1)
            .expect("should decode offset fetch response");

    assert_eq!(fetch_response.topics.len(), 1);
    let topic_resp = &fetch_response.topics[0];
    assert_eq!(topic_resp.partitions.len(), 1);
    let part_resp = &topic_resp.partitions[0];

    // Should return -1 for uncommitted offset (no error, just invalid offset).
    assert_eq!(part_resp.error_code, 0, "should not be an error");
    assert_eq!(
        part_resp.committed_offset, -1,
        "uncommitted should return -1"
    );

    drop(client);
    server_handle.abort();
}

#[tokio::test]
async fn test_multiple_partitions_in_single_request() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "multi-partition-topic";

    // Produce to 3 different partitions.
    for partition in 0..3 {
        let record_batch = build_minimal_record_batch(2);
        let mut produce_req = ProduceRequest::default();
        produce_req.acks = 1;
        produce_req.timeout_ms = 30000;

        let mut topic_data =
            kafka_protocol::messages::produce_request::TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

        let mut partition_data =
            kafka_protocol::messages::produce_request::PartitionProduceData::default();
        partition_data.index = partition;
        partition_data.records = Some(record_batch.into());
        topic_data.partition_data.push(partition_data);
        produce_req.topic_data.push(topic_data);

        let mut body = BytesMut::new();
        produce_req.encode(&mut body, 3).unwrap();
        let request = build_request(ApiKey::Produce, 3, 7000 + partition, &body);
        client.write_all(&request).await.unwrap();
        let _ = read_response(&mut client).await;
    }

    // Fetch from all 3 partitions in a single request.
    let mut fetch_req = FetchRequest::default();
    fetch_req.max_wait_ms = 100;
    fetch_req.min_bytes = 1;
    fetch_req.max_bytes = 1024 * 1024;

    let mut fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_string(topic_name.to_string()));

    for partition in 0..3 {
        let mut fetch_partition =
            kafka_protocol::messages::fetch_request::FetchPartition::default();
        fetch_partition.partition = partition;
        fetch_partition.fetch_offset = 0;
        fetch_partition.partition_max_bytes = 1024 * 1024;
        fetch_topic.partitions.push(fetch_partition);
    }
    fetch_req.topics.push(fetch_topic);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 4).unwrap();
    let request = build_request(ApiKey::Fetch, 4, 7100, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("fetch response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response = kafka_protocol::messages::FetchResponse::decode(&mut response_body, 4)
        .expect("should decode fetch response");

    assert_eq!(fetch_response.responses.len(), 1, "should have one topic");
    let topic_resp = &fetch_response.responses[0];
    assert_eq!(
        topic_resp.partitions.len(),
        3,
        "should have 3 partition responses"
    );

    for (i, part_resp) in topic_resp.partitions.iter().enumerate() {
        assert_eq!(part_resp.error_code, 0, "partition {} should succeed", i);
        assert_eq!(
            part_resp.high_watermark, 2,
            "partition {} high watermark should be 2",
            i
        );
        assert!(
            part_resp.records.is_some(),
            "partition {} should have records",
            i
        );
    }

    drop(client);
    drop(ctx);
    server_handle.abort();
}

// =============================================================================
// Tests for higher protocol versions
// =============================================================================

/// Test Produce with version 8.
#[tokio::test]
async fn test_produce_v8_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "produce-v8-topic";
    let record_batch = build_minimal_record_batch(2);

    let mut produce_req = ProduceRequest::default();
    produce_req.acks = -1; // All replicas
    produce_req.timeout_ms = 15000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 8).unwrap(); // v8

    let request = build_request(ApiKey::Produce, 8, 8000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("produce response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let produce_response = kafka_protocol::messages::ProduceResponse::decode(&mut response_body, 8)
        .expect("should decode produce v8 response");

    assert_eq!(produce_response.responses.len(), 1);
    let part_resp = &produce_response.responses[0].partition_responses[0];
    assert_eq!(part_resp.error_code, 0, "produce v8 should succeed");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test Fetch with version 11 and large max_bytes (55MiB).
#[tokio::test]
async fn test_fetch_v11_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "fetch-v11-topic";

    // First produce some data.
    let record_batch = build_minimal_record_batch(3);
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap();
    let request = build_request(ApiKey::Produce, 3, 11000, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // Now fetch with v11.
    let mut fetch_req = FetchRequest::default();
    fetch_req.max_wait_ms = 100;
    fetch_req.min_bytes = 1;
    fetch_req.max_bytes = 57_671_680; // 55MiB

    let mut fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut fetch_partition = kafka_protocol::messages::fetch_request::FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.fetch_offset = 0;
    fetch_partition.partition_max_bytes = 57_671_680; // 55MiB
    fetch_topic.partitions.push(fetch_partition);
    fetch_req.topics.push(fetch_topic);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 11).unwrap(); // v11

    let request = build_request(ApiKey::Fetch, 11, 11001, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("fetch response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response = kafka_protocol::messages::FetchResponse::decode(&mut response_body, 11)
        .expect("should decode fetch v11 response");

    assert_eq!(fetch_response.responses.len(), 1);
    let part_resp = &fetch_response.responses[0].partitions[0];
    assert_eq!(part_resp.error_code, 0, "fetch v11 should succeed");
    assert!(part_resp.records.is_some(), "should have records");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test Metadata with version 8.
#[tokio::test]
async fn test_metadata_v8_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    // First create a topic by producing.
    let topic_name = "metadata-v8-topic";
    let record_batch = build_minimal_record_batch(1);
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap();
    let request = build_request(ApiKey::Produce, 3, 8100, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // Request metadata v8.
    let mut metadata_req = MetadataRequest::default();
    let mut topic = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
    topic.name = Some(TopicName(StrBytes::from_string(topic_name.to_string())));
    metadata_req.topics = Some(vec![topic]);

    let mut body = BytesMut::new();
    metadata_req.encode(&mut body, 8).unwrap(); // v8

    let request = build_request(ApiKey::Metadata, 8, 8101, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("metadata response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let metadata_response =
        kafka_protocol::messages::MetadataResponse::decode(&mut response_body, 8)
            .expect("should decode metadata v8 response");

    assert!(!metadata_response.brokers.is_empty(), "should have brokers");
    assert_eq!(metadata_response.topics.len(), 1, "should have one topic");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test OffsetCommit with version 7.
#[tokio::test]
async fn test_offset_commit_v7_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let group_id = "test-group";
    let topic_name = "offset-commit-v7-topic";

    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = StrBytes::from_string(group_id.to_string()).into();
    commit_req.generation_id_or_member_epoch = -1;
    commit_req.member_id = StrBytes::from_string(String::new());

    let mut topic =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default();
    part.partition_index = 0;
    part.committed_offset = 100;
    part.committed_leader_epoch = 0; // v7 includes leader epoch
    part.committed_metadata = Some(StrBytes::from_string("test-metadata".to_string()));
    topic.partitions.push(part);
    commit_req.topics.push(topic);

    let mut body = BytesMut::new();
    commit_req.encode(&mut body, 7).unwrap(); // v7

    let request = build_request(ApiKey::OffsetCommit, 7, 7000, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("offset commit response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let commit_response =
        kafka_protocol::messages::OffsetCommitResponse::decode(&mut response_body, 7)
            .expect("should decode offset commit v7 response");

    assert_eq!(commit_response.topics.len(), 1);
    let part_resp = &commit_response.topics[0].partitions[0];
    assert_eq!(part_resp.error_code, 0, "offset commit v7 should succeed");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test OffsetFetch with version 5.
#[tokio::test]
async fn test_offset_fetch_v5_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let group_id = "fetch-v5-group";
    let topic_name = "offset-fetch-v5-topic";

    // First commit an offset.
    let mut commit_req = OffsetCommitRequest::default();
    commit_req.group_id = StrBytes::from_string(group_id.to_string()).into();
    commit_req.generation_id_or_member_epoch = -1;
    commit_req.member_id = StrBytes::from_string(String::new());

    let mut topic =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part =
        kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default();
    part.partition_index = 0;
    part.committed_offset = 200;
    topic.partitions.push(part);
    commit_req.topics.push(topic);

    let mut body = BytesMut::new();
    commit_req.encode(&mut body, 2).unwrap();
    let request = build_request(ApiKey::OffsetCommit, 2, 5100, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // Now fetch with v5.
    let mut fetch_req = OffsetFetchRequest::default();
    fetch_req.group_id = StrBytes::from_string(group_id.to_string()).into();

    let mut topic =
        kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));
    topic.partition_indexes = vec![0];
    fetch_req.topics = Some(vec![topic]);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 5).unwrap(); // v5

    let request = build_request(ApiKey::OffsetFetch, 5, 5101, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("offset fetch response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response =
        kafka_protocol::messages::OffsetFetchResponse::decode(&mut response_body, 5)
            .expect("should decode offset fetch v5 response");

    assert_eq!(fetch_response.topics.len(), 1);
    let part_resp = &fetch_response.topics[0].partitions[0];
    assert_eq!(part_resp.error_code, 0, "offset fetch v5 should succeed");
    assert_eq!(part_resp.committed_offset, 200, "should return committed offset");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test ListOffsets with version 5.
#[tokio::test]
async fn test_list_offsets_v5_high_version() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "list-offsets-v5-topic";

    // First produce some data.
    let record_batch = build_minimal_record_batch(10);
    let mut produce_req = ProduceRequest::default();
    produce_req.acks = 1;
    produce_req.timeout_ms = 30000;

    let mut topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default();
    topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut partition_data =
        kafka_protocol::messages::produce_request::PartitionProduceData::default();
    partition_data.index = 0;
    partition_data.records = Some(record_batch.into());
    topic_data.partition_data.push(partition_data);
    produce_req.topic_data.push(topic_data);

    let mut body = BytesMut::new();
    produce_req.encode(&mut body, 3).unwrap();
    let request = build_request(ApiKey::Produce, 3, 5200, &body);
    client.write_all(&request).await.unwrap();
    let _ = read_response(&mut client).await;

    // List offsets with v5.
    let mut list_offsets_req = ListOffsetsRequest::default();
    list_offsets_req.replica_id = BrokerId(-1);

    let mut topic = kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default();
    topic.name = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut part =
        kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default();
    part.partition_index = 0;
    part.timestamp = -1; // LATEST
    part.current_leader_epoch = 0; // v5 includes leader epoch
    topic.partitions.push(part);
    list_offsets_req.topics.push(topic);

    let mut body = BytesMut::new();
    list_offsets_req.encode(&mut body, 5).unwrap();

    let request = build_request(ApiKey::ListOffsets, 5, 5201, &body);
    client.write_all(&request).await.unwrap();

    let response = timeout(Duration::from_secs(5), read_response(&mut client))
        .await
        .expect("list offsets response timeout");

    let mut response_body = response.freeze();
    response_body.advance(4);
    let list_offsets_response =
        kafka_protocol::messages::ListOffsetsResponse::decode(&mut response_body, 5)
            .expect("should decode list offsets v5 response");

    assert_eq!(list_offsets_response.topics.len(), 1);
    let part_resp = &list_offsets_response.topics[0].partitions[0];
    assert_eq!(part_resp.error_code, 0, "list offsets v5 should succeed");
    assert_eq!(part_resp.offset, 10, "latest offset should be 10");

    drop(client);
    drop(ctx);
    server_handle.abort();
}

/// Test connection reuse - multiple requests on same connection.
#[tokio::test]
async fn test_connection_reuse_multiple_requests() {
    let (ctx, actual_addr, server_handle) = setup_test_server().await;

    let mut client = timeout(Duration::from_secs(5), TcpStream::connect(actual_addr))
        .await
        .expect("connection timeout")
        .expect("failed to connect");

    let topic_name = "connection-reuse-topic";

    // Send multiple produce requests on the same connection.
    for i in 0..5 {
        let record_batch = build_minimal_record_batch(2);
        let mut produce_req = ProduceRequest::default();
        produce_req.acks = 1;
        produce_req.timeout_ms = 30000;

        let mut topic_data =
            kafka_protocol::messages::produce_request::TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(topic_name.to_string()));

        let mut partition_data =
            kafka_protocol::messages::produce_request::PartitionProduceData::default();
        partition_data.index = 0;
        partition_data.records = Some(record_batch.into());
        topic_data.partition_data.push(partition_data);
        produce_req.topic_data.push(topic_data);

        let mut body = BytesMut::new();
        produce_req.encode(&mut body, 3).unwrap();
        let request = build_request(ApiKey::Produce, 3, 9000 + i, &body);
        client.write_all(&request).await.unwrap();

        let response = read_response(&mut client).await;
        let correlation_id =
            i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(
            correlation_id,
            9000 + i,
            "correlation ID should match for request {}",
            i
        );
    }

    // Verify all data was written - fetch should return 10 records.
    let mut fetch_req = FetchRequest::default();
    fetch_req.max_wait_ms = 100;
    fetch_req.min_bytes = 1;
    fetch_req.max_bytes = 1024 * 1024;

    let mut fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
    fetch_topic.topic = TopicName(StrBytes::from_string(topic_name.to_string()));

    let mut fetch_partition = kafka_protocol::messages::fetch_request::FetchPartition::default();
    fetch_partition.partition = 0;
    fetch_partition.fetch_offset = 0;
    fetch_partition.partition_max_bytes = 1024 * 1024;
    fetch_topic.partitions.push(fetch_partition);
    fetch_req.topics.push(fetch_topic);

    let mut body = BytesMut::new();
    fetch_req.encode(&mut body, 4).unwrap();
    let request = build_request(ApiKey::Fetch, 4, 9100, &body);
    client.write_all(&request).await.unwrap();

    let response = read_response(&mut client).await;
    let mut response_body = response.freeze();
    response_body.advance(4);
    let fetch_response = kafka_protocol::messages::FetchResponse::decode(&mut response_body, 4)
        .expect("should decode fetch response");

    let part_resp = &fetch_response.responses[0].partitions[0];
    assert_eq!(part_resp.error_code, 0);
    assert_eq!(
        part_resp.high_watermark, 10,
        "should have 10 records from 5 batches of 2"
    );

    drop(client);
    drop(ctx);
    server_handle.abort();
}

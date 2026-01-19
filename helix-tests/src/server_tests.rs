//! gRPC server integration tests.
//!
//! These tests verify the Helix gRPC API works correctly, including:
//! - Topic creation and metadata retrieval
//! - Write and read operations
//! - Error handling for invalid requests
//! - Multi-client concurrent access

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use helix_server::generated::helix_client::HelixClient;
use helix_server::generated::helix_server::HelixServer;
use helix_server::generated::{
    ErrorCode, GetMetadataRequest, GetPartitionInfoRequest, ReadRequest, Record, WriteRequest,
};
use helix_server::HelixService;
use tokio::time::timeout;
use tonic::transport::Server;

/// Maximum time to wait for server operations.
const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Starts a test server on an available port and returns the client and address.
async fn start_test_server() -> (HelixClient<tonic::transport::Channel>, SocketAddr) {
    // Use port 0 to get an available port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = HelixService::new("test-cluster".to_string(), 1).await;

    // Create a default topic.
    service
        .create_topic("test-topic".to_string(), 4)
        .await
        .unwrap();

    // Spawn the server.
    tokio::spawn(async move {
        Server::builder()
            .add_service(HelixServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give the server a moment to start.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect the client.
    let client = HelixClient::connect(format!("http://{addr}"))
        .await
        .unwrap();

    (client, addr)
}

#[tokio::test]
async fn test_get_metadata() {
    let (mut client, _addr) = start_test_server().await;

    let response = timeout(
        TEST_TIMEOUT,
        client.get_metadata(GetMetadataRequest { topics: vec![] }),
    )
    .await
    .unwrap()
    .unwrap();

    let metadata = response.into_inner();
    assert_eq!(metadata.cluster_id, "test-cluster");
    assert_eq!(metadata.controller_id, 1);
    assert_eq!(metadata.brokers.len(), 1);
    assert_eq!(metadata.topics.len(), 1);
    assert_eq!(metadata.topics[0].name, "test-topic");
    assert_eq!(metadata.topics[0].partitions.len(), 4);
}

#[tokio::test]
async fn test_get_metadata_specific_topic() {
    let (mut client, _addr) = start_test_server().await;

    // Request only the test topic.
    let response = timeout(
        TEST_TIMEOUT,
        client.get_metadata(GetMetadataRequest {
            topics: vec!["test-topic".to_string()],
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let metadata = response.into_inner();
    assert_eq!(metadata.topics.len(), 1);
    assert_eq!(metadata.topics[0].name, "test-topic");

    // Request a non-existent topic.
    let response = timeout(
        TEST_TIMEOUT,
        client.get_metadata(GetMetadataRequest {
            topics: vec!["nonexistent".to_string()],
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let metadata = response.into_inner();
    assert_eq!(metadata.topics.len(), 0);
}

#[tokio::test]
async fn test_write_and_read() {
    let (mut client, _addr) = start_test_server().await;

    // Write some records.
    let write_response = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
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
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let write_result = write_response.into_inner();
    assert_eq!(write_result.base_offset, 0);
    assert_eq!(write_result.record_count, 2);
    assert_eq!(write_result.error_code, i32::from(ErrorCode::None));

    // Read them back.
    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(read_result.records.len(), 2);
    assert_eq!(read_result.high_watermark, 2);
    assert_eq!(read_result.error_code, i32::from(ErrorCode::None));

    // Verify record contents.
    assert_eq!(read_result.records[0].offset, 0);
    assert_eq!(
        read_result.records[0].record.as_ref().unwrap().value,
        b"value1"
    );
    assert_eq!(read_result.records[1].offset, 1);
    assert_eq!(
        read_result.records[1].record.as_ref().unwrap().value,
        b"value2"
    );
}

#[tokio::test]
async fn test_write_multiple_batches() {
    let (mut client, _addr) = start_test_server().await;

    // Write first batch.
    let write1 = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![Record {
                key: None,
                value: b"batch1-record1".to_vec(),
                headers: HashMap::new(),
                timestamp_ms: None,
            }],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(write1.into_inner().base_offset, 0);

    // Write second batch.
    let write2 = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![
                Record {
                    key: None,
                    value: b"batch2-record1".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                },
                Record {
                    key: None,
                    value: b"batch2-record2".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                },
            ],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(write2.into_inner().base_offset, 1);

    // Read all records.
    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_records: 10,
            max_bytes: 4096,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(read_result.records.len(), 3);
    assert_eq!(read_result.high_watermark, 3);
}

#[tokio::test]
async fn test_read_from_offset() {
    let (mut client, _addr) = start_test_server().await;

    // Write 5 records.
    for i in 0..5 {
        timeout(
            TEST_TIMEOUT,
            client.write(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![Record {
                    key: None,
                    value: format!("record-{i}").into_bytes(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                }],
                required_acks: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap();
    }

    // Read from offset 2.
    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 2,
            max_records: 10,
            max_bytes: 4096,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(read_result.records.len(), 3);
    assert_eq!(read_result.records[0].offset, 2);
    assert_eq!(
        read_result.records[0].record.as_ref().unwrap().value,
        b"record-2"
    );
}

#[tokio::test]
async fn test_read_nonexistent_topic() {
    let (mut client, _addr) = start_test_server().await;

    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "nonexistent".to_string(),
            partition: 0,
            offset: 0,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(read_result.error_code, i32::from(ErrorCode::InvalidTopic));
    assert!(read_result.error_message.is_some());
}

#[tokio::test]
async fn test_read_invalid_partition() {
    let (mut client, _addr) = start_test_server().await;

    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 999, // Invalid partition.
            offset: 0,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(
        read_result.error_code,
        i32::from(ErrorCode::InvalidPartition)
    );
}

#[tokio::test]
async fn test_read_offset_out_of_range() {
    let (mut client, _addr) = start_test_server().await;

    // Write one record.
    timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![Record {
                key: None,
                value: b"test".to_vec(),
                headers: HashMap::new(),
                timestamp_ms: None,
            }],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    // Try to read from offset 100 (out of range).
    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(
        read_result.error_code,
        i32::from(ErrorCode::OffsetOutOfRange)
    );
}

#[tokio::test]
async fn test_get_partition_info() {
    let (mut client, _addr) = start_test_server().await;

    // Write some records first.
    timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![
                Record {
                    key: None,
                    value: b"record1".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                },
                Record {
                    key: None,
                    value: b"record2".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                },
            ],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    // Get partition info.
    let response = timeout(
        TEST_TIMEOUT,
        client.get_partition_info(GetPartitionInfoRequest {
            topic: "test-topic".to_string(),
            partition: 0,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let info = response.into_inner();
    assert_eq!(info.error_code, i32::from(ErrorCode::None));
    assert!(info.partition.is_some());
    assert_eq!(info.partition.as_ref().unwrap().partition, 0);
    assert_eq!(info.log_start_offset, 0);
    assert_eq!(info.log_end_offset, 2);
    assert_eq!(info.high_watermark, 2);
}

#[tokio::test]
async fn test_write_to_different_partitions() {
    let (mut client, _addr) = start_test_server().await;

    // Write to partition 0.
    let write0 = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![Record {
                key: None,
                value: b"partition-0".to_vec(),
                headers: HashMap::new(),
                timestamp_ms: None,
            }],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(write0.into_inner().base_offset, 0);

    // Write to partition 1.
    let write1 = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 1,
            records: vec![Record {
                key: None,
                value: b"partition-1".to_vec(),
                headers: HashMap::new(),
                timestamp_ms: None,
            }],
            required_acks: 1,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    // Should also be offset 0 since it's a different partition.
    assert_eq!(write1.into_inner().base_offset, 0);

    // Verify each partition has its own data.
    let read0 = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        read0.into_inner().records[0]
            .record
            .as_ref()
            .unwrap()
            .value,
        b"partition-0"
    );

    let read1 = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 1,
            offset: 0,
            max_records: 10,
            max_bytes: 1024,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(
        read1.into_inner().records[0]
            .record
            .as_ref()
            .unwrap()
            .value,
        b"partition-1"
    );
}

/// NOTE: Uses multi-threaded runtime to expose race conditions that would
/// be hidden by single-threaded Tokio's serialization of spawned tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writes() {
    let (mut client, _addr) = start_test_server().await;

    // Spawn multiple concurrent writes.
    let mut handles = Vec::new();
    for i in 0..10 {
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            client_clone
                .write(WriteRequest {
                    topic: "test-topic".to_string(),
                    partition: 0,
                    records: vec![Record {
                        key: None,
                        value: format!("concurrent-{i}").into_bytes(),
                        headers: HashMap::new(),
                        timestamp_ms: None,
                    }],
                    required_acks: 1,
                })
                .await
        }));
    }

    // Wait for all writes to complete.
    for handle in handles {
        let result = handle.await.unwrap().unwrap();
        assert_eq!(result.into_inner().error_code, i32::from(ErrorCode::None));
    }

    // Verify all records were written.
    let read_response = timeout(
        TEST_TIMEOUT,
        client.read(ReadRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 0,
            max_records: 20,
            max_bytes: 4096,
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let read_result = read_response.into_inner();
    assert_eq!(read_result.records.len(), 10);
    assert_eq!(read_result.high_watermark, 10);
}

/// Concurrent writes to multiple partitions simultaneously.
/// Verifies per-partition offset isolation and ordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_writes_multi_partition() {
    let (mut client, _addr) = start_test_server().await;
    let partitions = 4;

    // Spawn concurrent writes to each partition.
    let mut handles = Vec::new();
    for partition in 0..partitions {
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            let mut offsets = Vec::new();
            for i in 0..10 {
                let result = client_clone
                    .write(WriteRequest {
                        topic: "test-topic".to_string(),
                        partition,
                        records: vec![Record {
                            key: None,
                            value: format!("p{partition}-msg{i}").into_bytes(),
                            headers: HashMap::new(),
                            timestamp_ms: None,
                        }],
                        required_acks: 1,
                    })
                    .await
                    .unwrap();
                offsets.push(result.into_inner().base_offset);
            }
            (partition, offsets)
        }));
    }

    // Collect results.
    let mut partition_offsets: HashMap<i32, Vec<u64>> = HashMap::new();
    for handle in handles {
        let (partition, offsets) = handle.await.unwrap();
        partition_offsets.insert(partition, offsets);
    }

    // Verify per-partition offset isolation: each partition starts at 0.
    for partition in 0..partitions {
        let offsets = partition_offsets.get(&partition).unwrap();
        assert_eq!(
            offsets[0], 0,
            "Partition {partition} should start at offset 0"
        );

        // Verify offsets are monotonically increasing within partition.
        for i in 1..offsets.len() {
            assert!(
                offsets[i] > offsets[i - 1],
                "Partition {partition}: offset[{i}]={} should be > offset[{}]={}",
                offsets[i],
                i - 1,
                offsets[i - 1]
            );
        }
    }

    // Verify all data is readable and matches what was written.
    for partition in 0..partitions {
        let read_response = timeout(
            TEST_TIMEOUT,
            client.read(ReadRequest {
                topic: "test-topic".to_string(),
                partition,
                offset: 0,
                max_records: 20,
                max_bytes: 4096,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let read_result = read_response.into_inner();
        assert_eq!(
            read_result.records.len(),
            10,
            "Partition {partition} should have 10 records"
        );
        assert_eq!(read_result.high_watermark, 10);

        // Verify content matches expected pattern.
        for (i, record_with_offset) in read_result.records.iter().enumerate() {
            let expected = format!("p{partition}-msg{i}");
            let actual = record_with_offset.record.as_ref().unwrap().value.clone();
            assert_eq!(
                actual,
                expected.as_bytes(),
                "Partition {partition}, record {i} content mismatch"
            );
        }
    }
}

/// Interleaved writes across partitions from a single client.
/// Verifies no cross-partition interference.
#[tokio::test]
async fn test_multi_partition_interleaved_writes() {
    let (mut client, _addr) = start_test_server().await;
    let partitions = 4;
    let writes_per_partition = 5;

    // Round-robin writes across partitions.
    for round in 0..writes_per_partition {
        for partition in 0..partitions {
            let write_response = timeout(
                TEST_TIMEOUT,
                client.write(WriteRequest {
                    topic: "test-topic".to_string(),
                    partition,
                    records: vec![Record {
                        key: None,
                        value: format!("round{round}-p{partition}").into_bytes(),
                        headers: HashMap::new(),
                        timestamp_ms: None,
                    }],
                    required_acks: 1,
                }),
            )
            .await
            .unwrap()
            .unwrap();

            let result = write_response.into_inner();
            assert_eq!(result.error_code, i32::from(ErrorCode::None));
            // Each partition should have offset = round.
            #[allow(clippy::cast_sign_loss)] // round is non-negative
            let expected_offset = round as u64;
            assert_eq!(
                result.base_offset, expected_offset,
                "Partition {partition}, round {round}: expected offset {round}"
            );
        }
    }

    // Verify all data is correct.
    for partition in 0..partitions {
        let read_response = timeout(
            TEST_TIMEOUT,
            client.read(ReadRequest {
                topic: "test-topic".to_string(),
                partition,
                offset: 0,
                max_records: 20,
                max_bytes: 4096,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let read_result = read_response.into_inner();
        assert_eq!(
            read_result.records.len() as i32,
            writes_per_partition,
            "Partition {partition} should have {writes_per_partition} records"
        );

        // Verify content integrity.
        for (round, record_with_offset) in read_result.records.iter().enumerate() {
            let expected = format!("round{round}-p{partition}");
            let actual = record_with_offset.record.as_ref().unwrap().value.clone();
            assert_eq!(actual, expected.as_bytes());
        }
    }
}

/// High volume test with many partitions and concurrent writers.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_multi_partition_high_volume() {
    let (mut client, _addr) = start_test_server().await;

    // Use 4 partitions (the test topic already has 4).
    let partitions = 4;
    let writes_per_partition = 100;
    let concurrent_writers_per_partition = 2;

    // Spawn multiple writers per partition.
    let mut handles = Vec::new();
    for partition in 0..partitions {
        for writer_id in 0..concurrent_writers_per_partition {
            let mut client_clone = client.clone();
            let writes_per_writer = writes_per_partition / concurrent_writers_per_partition;
            handles.push(tokio::spawn(async move {
                let mut success_count = 0;
                for i in 0..writes_per_writer {
                    let result = client_clone
                        .write(WriteRequest {
                            topic: "test-topic".to_string(),
                            partition,
                            records: vec![Record {
                                key: None,
                                value: format!("p{partition}-w{writer_id}-{i}").into_bytes(),
                                headers: HashMap::new(),
                                timestamp_ms: None,
                            }],
                            required_acks: 1,
                        })
                        .await;
                    if let Ok(resp) = result {
                        if resp.into_inner().error_code == i32::from(ErrorCode::None) {
                            success_count += 1;
                        }
                    }
                }
                (partition, success_count)
            }));
        }
    }

    // Wait for all writers and collect results.
    let mut partition_success: HashMap<i32, i32> = HashMap::new();
    for handle in handles {
        let (partition, count) = handle.await.unwrap();
        *partition_success.entry(partition).or_insert(0) += count;
    }

    // Verify each partition received all its writes.
    for partition in 0..partitions {
        let success = partition_success.get(&partition).copied().unwrap_or(0);
        assert_eq!(
            success, writes_per_partition,
            "Partition {partition}: expected {writes_per_partition} successful writes, got {success}"
        );
    }

    // Verify we can read all records back.
    for partition in 0..partitions {
        let read_response = timeout(
            Duration::from_secs(10),
            client.read(ReadRequest {
                topic: "test-topic".to_string(),
                partition,
                offset: 0,
                max_records: 200,
                max_bytes: 65536,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let read_result = read_response.into_inner();
        assert_eq!(
            read_result.records.len() as i32,
            writes_per_partition,
            "Partition {partition}: expected {writes_per_partition} records, got {}",
            read_result.records.len()
        );
        #[allow(clippy::cast_sign_loss)] // writes_per_partition is positive
        let expected_hwm = writes_per_partition as u64;
        assert_eq!(read_result.high_watermark, expected_hwm);

        // Verify offsets are sequential.
        for (i, record) in read_result.records.iter().enumerate() {
            assert_eq!(
                record.offset, i as u64,
                "Partition {partition}: record {i} has wrong offset {}",
                record.offset
            );
        }
    }

    // Total operations across all partitions.
    let total_writes: i32 = partition_success.values().sum();
    assert_eq!(
        total_writes,
        partitions * writes_per_partition,
        "Total writes mismatch"
    );
}

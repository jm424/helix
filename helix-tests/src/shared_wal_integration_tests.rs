//! SharedWAL integration tests for helix-server.
//!
//! These tests verify the SharedWalPool integration in HelixService:
//! - Basic write and read with shared WAL
//! - Multi-partition concurrent writes
//! - Recovery after restart
//! - Different WAL count configurations

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use helix_server::generated::helix_client::HelixClient;
use helix_server::generated::helix_server::HelixServer;
use helix_server::generated::{ErrorCode, ReadRequest, Record, WriteRequest};
use helix_server::HelixService;
use tokio::time::timeout;
use tonic::transport::Server;

/// Maximum time to wait for server operations.
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts a test server with SharedWAL on an available port.
async fn start_test_server_with_shared_wal(
    data_dir: std::path::PathBuf,
    shared_wal_count: Option<u32>,
) -> (HelixClient<tonic::transport::Channel>, SocketAddr) {
    // Use port 0 to get an available port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let service = HelixService::with_data_dir(
        "test-cluster".to_string(),
        1,
        data_dir,
        shared_wal_count,
        helix_core::WriteDurability::ReplicationOnly,
    )
    .await;

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

/// Test basic write and read operations with SharedWAL.
#[tokio::test]
async fn test_shared_wal_basic_write_read() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut client, _addr) =
        start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(4)).await;

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

/// Test writes across multiple partitions using shared WAL.
/// With K=4 WALs, partitions 0-3 each use a different WAL.
#[tokio::test]
async fn test_shared_wal_multiple_partitions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut client, _addr) =
        start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(4)).await;

    // Write to all 4 partitions.
    for partition in 0..4 {
        let write_response = timeout(
            TEST_TIMEOUT,
            client.write(WriteRequest {
                topic: "test-topic".to_string(),
                partition,
                records: vec![Record {
                    key: Some(format!("key-p{partition}").into_bytes()),
                    value: format!("value-partition-{partition}").into_bytes(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                }],
                required_acks: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let write_result = write_response.into_inner();
        assert_eq!(write_result.base_offset, 0, "partition {partition}");
        assert_eq!(write_result.error_code, i32::from(ErrorCode::None));
    }

    // Read back from all partitions.
    for partition in 0..4 {
        let read_response = timeout(
            TEST_TIMEOUT,
            client.read(ReadRequest {
                topic: "test-topic".to_string(),
                partition,
                offset: 0,
                max_records: 10,
                max_bytes: 1024,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let read_result = read_response.into_inner();
        assert_eq!(read_result.records.len(), 1, "partition {partition}");
        assert_eq!(
            read_result.records[0].record.as_ref().unwrap().value,
            format!("value-partition-{partition}").into_bytes(),
            "partition {partition}"
        );
    }
}

/// Test concurrent writes to multiple partitions.
/// This exercises the SharedWalPool's fsync amortization.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shared_wal_concurrent_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (client, _addr) =
        start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(4)).await;

    // Spawn concurrent writes to all 4 partitions.
    let mut handles = Vec::new();
    for partition in 0..4 {
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            // Write 10 records to this partition.
            for i in 0..10 {
                let result = client_clone
                    .write(WriteRequest {
                        topic: "test-topic".to_string(),
                        partition,
                        records: vec![Record {
                            key: None,
                            value: format!("p{partition}-record-{i}").into_bytes(),
                            headers: HashMap::new(),
                            timestamp_ms: None,
                        }],
                        required_acks: 1,
                    })
                    .await
                    .unwrap();

                assert_eq!(
                    result.into_inner().error_code,
                    i32::from(ErrorCode::None)
                );
            }
            partition
        }));
    }

    // Wait for all writes to complete.
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all records were written to each partition.
    let mut client = client;
    for partition in 0..4 {
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
            "partition {partition} should have 10 records"
        );
        assert_eq!(read_result.high_watermark, 10);
    }
}

/// Test different WAL count configurations.
#[tokio::test]
async fn test_shared_wal_different_wal_counts() {
    // Test with K=1 (single WAL for all partitions).
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut client, _addr) =
            start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(1)).await;

        // Write to partition 0.
        let write_response = timeout(
            TEST_TIMEOUT,
            client.write(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![Record {
                    key: None,
                    value: b"k1-test".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                }],
                required_acks: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            write_response.into_inner().error_code,
            i32::from(ErrorCode::None)
        );
    }

    // Test with K=8 (more WALs than partitions).
    {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut client, _addr) =
            start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(8)).await;

        // Write to partition 0.
        let write_response = timeout(
            TEST_TIMEOUT,
            client.write(WriteRequest {
                topic: "test-topic".to_string(),
                partition: 0,
                records: vec![Record {
                    key: None,
                    value: b"k8-test".to_vec(),
                    headers: HashMap::new(),
                    timestamp_ms: None,
                }],
                required_acks: 1,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            write_response.into_inner().error_code,
            i32::from(ErrorCode::None)
        );
    }
}

/// Test that multiple batches accumulate correctly in SharedWAL.
#[tokio::test]
async fn test_shared_wal_multiple_batches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut client, _addr) =
        start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(4)).await;

    // Write first batch.
    let write1 = timeout(
        TEST_TIMEOUT,
        client.write(WriteRequest {
            topic: "test-topic".to_string(),
            partition: 0,
            records: vec![Record {
                key: None,
                value: b"batch1-record".to_vec(),
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

/// Test recovery after service restart using SharedWAL.
/// This tests that data written to the SharedWAL is correctly recovered
/// when a new service instance is created with the same data directory.
///
/// Note: This test uses two sequential gRPC server instances on different ports.
/// The first instance writes data, then the second instance verifies recovery.
#[tokio::test]
async fn test_shared_wal_recovery_after_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Phase 1: Start first server, write data.
    // We use a oneshot channel to signal when to stop.
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let data_path_clone = data_path.clone();

    let server_handle = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let service = HelixService::with_data_dir(
            "test-cluster".to_string(),
            1,
            data_path_clone,
            Some(4),
            helix_core::WriteDurability::ReplicationOnly,
        )
        .await;

        // Create topic.
        service
            .create_topic("recovery-test".to_string(), 4)
            .await
            .unwrap();

        // Start server with graceful shutdown.
        let server = Server::builder()
            .add_service(HelixServer::new(service))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    stop_rx.await.ok();
                },
            );

        // Return the address so client can connect.
        (addr, server)
    });

    // Wait a moment for server to start binding.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get the server address and start it.
    let (addr, server_future) = server_handle.await.unwrap();
    let server_task = tokio::spawn(server_future);

    // Give server time to actually start listening.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect client and write data.
    {
        let mut client = HelixClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

        // Write to all 4 partitions.
        for partition in 0..4 {
            let write_response = timeout(
                TEST_TIMEOUT,
                client.write(WriteRequest {
                    topic: "recovery-test".to_string(),
                    partition,
                    records: vec![Record {
                        key: Some(format!("key-p{partition}").into_bytes()),
                        value: format!("recovered-value-{partition}").into_bytes(),
                        headers: HashMap::new(),
                        timestamp_ms: Some(1000),
                    }],
                    required_acks: 1,
                }),
            )
            .await
            .unwrap()
            .unwrap();

            assert_eq!(
                write_response.into_inner().error_code,
                i32::from(ErrorCode::None)
            );
        }
    }

    // Signal server to shutdown gracefully.
    stop_tx.send(()).ok();
    let _ = server_task.await;

    // Small delay to ensure WAL is flushed.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Phase 2: Start new server with same data dir, verify recovery.
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();

    let service2 = HelixService::with_data_dir(
        "test-cluster".to_string(),
        1,
        data_path,
        Some(4),
        helix_core::WriteDurability::ReplicationOnly,
    )
    .await;

    // Re-create topic (metadata not persisted in single-node mode).
    // This will pick up recovered entries from the WAL.
    service2
        .create_topic("recovery-test".to_string(), 4)
        .await
        .unwrap();

    // Start second server.
    tokio::spawn(async move {
        Server::builder()
            .add_service(HelixServer::new(service2))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener2))
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect and verify data was recovered.
    let mut client2 = HelixClient::connect(format!("http://{addr2}"))
        .await
        .unwrap();

    for partition in 0..4 {
        let read_response = timeout(
            TEST_TIMEOUT,
            client2.read(ReadRequest {
                topic: "recovery-test".to_string(),
                partition,
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
            i32::from(ErrorCode::None),
            "partition {partition} read should succeed"
        );
        assert_eq!(
            read_result.records.len(),
            1,
            "partition {partition} should have 1 recovered record"
        );
        assert_eq!(
            read_result.records[0].record.as_ref().unwrap().value,
            format!("recovered-value-{partition}").into_bytes(),
            "partition {partition} value mismatch"
        );
    }
}

/// Test high-volume writes to stress the SharedWAL batching.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shared_wal_high_volume() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (client, _addr) =
        start_test_server_with_shared_wal(temp_dir.path().to_path_buf(), Some(4)).await;

    const RECORDS_PER_PARTITION: usize = 100;
    const PARTITIONS: i32 = 4;

    // Spawn concurrent high-volume writes.
    let mut handles = Vec::new();
    for partition in 0..PARTITIONS {
        let mut client_clone = client.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..RECORDS_PER_PARTITION {
                let result = client_clone
                    .write(WriteRequest {
                        topic: "test-topic".to_string(),
                        partition,
                        records: vec![Record {
                            key: Some(format!("key-{i}").into_bytes()),
                            value: format!("partition-{partition}-value-{i}").into_bytes(),
                            headers: HashMap::new(),
                            timestamp_ms: None,
                        }],
                        required_acks: 1,
                    })
                    .await
                    .unwrap();

                assert_eq!(
                    result.into_inner().error_code,
                    i32::from(ErrorCode::None),
                    "partition {partition}, record {i}"
                );
            }
        }));
    }

    // Wait for all writes.
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify counts.
    let mut client = client;
    for partition in 0..PARTITIONS {
        let read_response = timeout(
            TEST_TIMEOUT,
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
            read_result.records.len(),
            RECORDS_PER_PARTITION,
            "partition {partition}"
        );
        assert_eq!(
            read_result.high_watermark as usize, RECORDS_PER_PARTITION,
            "partition {partition}"
        );
    }
}

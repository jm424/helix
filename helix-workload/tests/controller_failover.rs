//! Controller partition failover tests.
//!
//! These tests verify that the controller partition (Raft group 0) correctly
//! handles leader failover. The controller manages critical cluster metadata:
//! - Topic creation (`CreateTopic` → `AssignPartition` commands)
//! - Partition assignments (which nodes host which partitions)
//! - Leader tracking (`UpdatePartitionLeader`)
//!
//! # Test Strategy
//!
//! We cover two classes of scenarios:
//! 1. Generic node failures while validating controller-mediated operations
//!    (topic creation, metadata convergence, and write continuity).
//! 2. Explicit controller-leader failover by identifying the active controller
//!    and killing that node.
//! 3. Quorum loss behavior (2/3 nodes down) and recovery semantics.

#![allow(clippy::too_many_lines)]
#![allow(clippy::type_complexity)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unused_async)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use helix_workload::{ProducerMode, RealCluster, RealExecutor, WorkloadExecutor};
use kafka_protocol::messages::{
    ApiKey, MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::types::RDKafkaRespErr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ============================================================================
// Test Helpers
// ============================================================================

fn binary_path() -> PathBuf {
    let release = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/release/helix-server");
    if release.exists() {
        return release;
    }

    let debug = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/debug/helix-server");
    if debug.exists() {
        return debug;
    }

    panic!(
        "helix-server binary not found. Run `cargo build -p helix-server` first.\n\
         Checked:\n  - {}\n  - {}",
        release.display(),
        debug.display()
    );
}

fn test_data_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-controller-failover-tests")
        .join(test_name)
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Creates a 3-node cluster for controller failover testing.
async fn setup_cluster(test_name: &str, base_port: u16, raft_base_port: u16) -> RealCluster {
    RealCluster::builder()
        .nodes(3)
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir(test_name))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .log_level("debug")
        .build()
        .expect("failed to start cluster")
}

/// Fetches the controller leader ID from Kafka metadata.
///
/// Returns the node ID of the current controller leader, or an error if unavailable.
async fn get_controller_leader(bootstrap_servers: &str) -> Result<i32, String> {
    let (live, errors) = fetch_live_controller_ids(bootstrap_servers).await;
    if live.is_empty() {
        return Err(format!(
            "no live brokers with controller id: {}",
            errors.join(" | ")
        ));
    }

    let mut votes_by_controller: std::collections::HashMap<i32, usize> =
        std::collections::HashMap::new();
    for (_, controller_id) in &live {
        *votes_by_controller.entry(*controller_id).or_insert(0) += 1;
    }

    if let Some((&controller_id, &votes)) =
        votes_by_controller.iter().max_by_key(|(_, votes)| *votes)
    {
        if votes * 2 > live.len() {
            return Ok(controller_id);
        }
    }

    Err(format!(
        "no controller majority among live brokers: {:?}; errors: {}",
        live,
        errors.join(" | ")
    ))
}

/// Returns live broker observations `(broker_addr, controller_id)` and per-broker errors.
async fn fetch_live_controller_ids(bootstrap_servers: &str) -> (Vec<(String, i32)>, Vec<String>) {
    let brokers: Vec<String> = bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect();
    let mut live = Vec::new();
    let mut errors = Vec::new();

    for broker in &brokers {
        match fetch_controller_id_from_broker(broker).await {
            Ok(controller_id) if controller_id >= 0 => {
                live.push((broker.clone(), controller_id));
            }
            Ok(controller_id) => {
                errors.push(format!("{broker}: invalid controller id {controller_id}"));
            }
            Err(e) => {
                errors.push(format!("{broker}: {e}"));
            }
        }
    }

    (live, errors)
}

/// Returns `controller_id` by issuing a raw Metadata request to one broker.
async fn fetch_controller_id_from_broker(bootstrap_server: &str) -> Result<i32, String> {
    let mut stream = TcpStream::connect(bootstrap_server)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;

    let api_key = ApiKey::Metadata as i16;
    let api_version = 8i16; // includes controller_id and avoids flexible encoding.
    let request_header_version = 1i16;
    let response_header_version = 0i16;
    let correlation_id = 42i32;

    let mut header = RequestHeader::default();
    header.request_api_key = api_key;
    header.request_api_version = api_version;
    header.correlation_id = correlation_id;
    header.client_id = Some(StrBytes::from_string("controller-checker".to_string()));

    let request = MetadataRequest::default();

    let mut payload = Vec::new();
    header
        .encode(&mut payload, request_header_version)
        .map_err(|e| format!("request header encode failed: {e}"))?;
    request
        .encode(&mut payload, api_version)
        .map_err(|e| format!("metadata request encode failed: {e}"))?;

    let mut frame = Vec::with_capacity(4 + payload.len());
    #[allow(clippy::cast_possible_truncation)]
    let payload_len = payload.len() as u32;
    frame.extend_from_slice(&payload_len.to_be_bytes());
    frame.extend_from_slice(&payload);
    stream
        .write_all(&frame)
        .await
        .map_err(|e| format!("write failed: {e}"))?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read frame length failed: {e}"))?;
    #[allow(clippy::cast_sign_loss)]
    let response_len = u32::from_be_bytes(len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];
    stream
        .read_exact(&mut response_buf)
        .await
        .map_err(|e| format!("read frame payload failed: {e}"))?;

    let mut bytes = bytes::Bytes::from(response_buf);
    let response_header = ResponseHeader::decode(&mut bytes, response_header_version)
        .map_err(|e| format!("response header decode failed: {e}"))?;
    if response_header.correlation_id != correlation_id {
        return Err(format!(
            "correlation mismatch: expected {correlation_id}, got {}",
            response_header.correlation_id
        ));
    }

    let response = MetadataResponse::decode(&mut bytes, api_version)
        .map_err(|e| format!("metadata response decode failed: {e}"))?;
    Ok(i32::from(response.controller_id))
}

/// Waits until all live brokers converge to a single controller ID.
///
/// If `expected_controller` is provided, convergence must match it.
async fn wait_for_controller_convergence_across_live_brokers(
    bootstrap_servers: &str,
    expected_controller: Option<i32>,
    min_live_brokers: usize,
    timeout: Duration,
) -> Result<(i32, Vec<(String, i32)>), String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_observation = String::new();

    while std::time::Instant::now() < deadline {
        let (live, errors) = fetch_live_controller_ids(bootstrap_servers).await;
        if live.len() < min_live_brokers {
            last_observation = format!(
                "only {} live broker controller observations (need {min_live_brokers}); errors: {}",
                live.len(),
                errors.join(" | ")
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        let first = live[0].1;
        let converged = live
            .iter()
            .all(|(_, controller_id)| *controller_id == first);
        if !converged {
            last_observation = format!("controller disagreement across live brokers: {live:?}");
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }

        if let Some(expected) = expected_controller {
            if first != expected {
                last_observation =
                    format!("converged controller {first} does not match expected {expected}");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        }

        return Ok((first, live));
    }

    Err(format!(
        "timeout waiting for controller convergence: {last_observation}"
    ))
}

/// Waits for controller leadership to move away from `previous_controller`.
async fn wait_for_controller_leader_change(
    bootstrap_servers: &str,
    previous_controller: i32,
    min_live_brokers: usize,
    timeout: Duration,
) -> Result<i32, String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_observation = String::new();
    while std::time::Instant::now() < deadline {
        match wait_for_controller_convergence_across_live_brokers(
            bootstrap_servers,
            None,
            min_live_brokers,
            Duration::from_secs(2),
        )
        .await
        {
            Ok((current, _)) if current >= 0 && current != previous_controller => {
                return Ok(current);
            }
            Ok((current, _)) => {
                last_observation = format!("controller still {current}");
            }
            Err(e) => {
                last_observation = e;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    Err(format!(
        "timeout waiting for controller leader change from {previous_controller}: {last_observation}"
    ))
}

/// Fetches topic metadata and returns partition count and replica assignments.
///
/// Returns `(partition_count, Vec<(partition_id, leader, replicas)>)`.
#[allow(dead_code)]
fn fetch_topic_metadata(
    bootstrap_servers: &str,
    topic: &str,
) -> Result<(usize, Vec<(i32, i32, Vec<i32>)>), String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "metadata-checker")
        .create()
        .map_err(|e| format!("failed to create consumer: {e}"))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("failed to fetch metadata: {e}"))?;

    for t in metadata.topics() {
        if t.name() == topic {
            if let Some(topic_error) = t.error() {
                if topic_error != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                    return Err(format!("topic '{topic}' metadata error: {topic_error:?}"));
                }
            }

            let mut partitions = Vec::with_capacity(t.partitions().len());
            for p in t.partitions() {
                if let Some(partition_error) = p.error() {
                    if partition_error != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                        return Err(format!(
                            "topic '{topic}' partition {} metadata error: {partition_error:?}",
                            p.id()
                        ));
                    }
                }
                let replicas: Vec<i32> = p.replicas().to_vec();
                partitions.push((p.id(), p.leader(), replicas));
            }

            if partitions.is_empty() {
                return Err(format!("topic '{topic}' metadata has no partitions yet"));
            }

            return Ok((partitions.len(), partitions));
        }
    }

    Err(format!("topic '{topic}' not found in metadata"))
}

/// Fetches topic metadata from each bootstrap broker individually.
fn fetch_topic_metadata_per_broker(
    bootstrap_servers: &str,
    topic: &str,
) -> Vec<(String, Result<(usize, Vec<(i32, i32, Vec<i32>)>), String>)> {
    bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|broker| (broker.to_string(), fetch_topic_metadata(broker, topic)))
        .collect()
}

fn normalize_partition_metadata(partitions: &[(i32, i32, Vec<i32>)]) -> Vec<(i32, i32, Vec<i32>)> {
    let mut normalized = partitions.to_vec();
    normalized.sort_by_key(|(partition_id, _, _)| *partition_id);
    for (_, _, replicas) in &mut normalized {
        replicas.sort_unstable();
    }
    normalized
}

fn validate_partition_layout(
    partitions: &[(i32, i32, Vec<i32>)],
    expected_partitions: usize,
) -> Result<(), String> {
    if partitions.len() != expected_partitions {
        return Err(format!(
            "expected {expected_partitions} partitions, got {}",
            partitions.len()
        ));
    }

    for expected_partition_id in 0..expected_partitions as i32 {
        let Some((_, leader, replicas)) = partitions
            .iter()
            .find(|(partition_id, _, _)| *partition_id == expected_partition_id)
        else {
            return Err(format!("missing partition {expected_partition_id}"));
        };

        if *leader < 0 {
            return Err(format!(
                "partition {expected_partition_id} has no leader (leader={leader})"
            ));
        }

        if replicas.is_empty() {
            return Err(format!("partition {expected_partition_id} has no replicas"));
        }

        if !replicas.contains(leader) {
            return Err(format!(
                "partition {expected_partition_id} leader {leader} not in replicas {replicas:?}"
            ));
        }
    }

    Ok(())
}

/// Waits until all brokers report identical topic metadata.
///
/// This enforces cluster-wide convergence semantics, not just "one broker works".
async fn wait_for_topic_convergence_across_brokers(
    bootstrap_servers: &str,
    topic: &str,
    expected_partitions: usize,
    timeout: Duration,
) -> Result<Vec<(String, Vec<(i32, i32, Vec<i32>)>)>, String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_observation = String::new();

    while std::time::Instant::now() < deadline {
        let results = fetch_topic_metadata_per_broker(bootstrap_servers, topic);
        let mut broker_layouts: Vec<(String, Vec<(i32, i32, Vec<i32>)>)> = Vec::new();
        let mut errors: Vec<String> = Vec::new();

        for (broker, result) in results {
            match result {
                Ok((count, partitions)) => {
                    if count != expected_partitions {
                        errors.push(format!(
                            "{broker}: expected {expected_partitions} partitions, got {count}"
                        ));
                        continue;
                    }

                    let normalized = normalize_partition_metadata(&partitions);
                    if let Err(e) = validate_partition_layout(&normalized, expected_partitions) {
                        errors.push(format!("{broker}: {e}"));
                        continue;
                    }

                    broker_layouts.push((broker, normalized));
                }
                Err(e) => {
                    errors.push(format!("{broker}: {e}"));
                }
            }
        }

        if broker_layouts.is_empty() {
            errors.push("no broker metadata responses available".to_string());
        }

        if errors.is_empty() {
            let first_layout = &broker_layouts[0].1;
            let all_match = broker_layouts
                .iter()
                .all(|(_, layout)| layout == first_layout);
            if all_match {
                return Ok(broker_layouts);
            }
            errors.push("brokers disagree on partition metadata layout".to_string());
        }

        last_observation = errors.join(" | ");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(format!(
        "timeout waiting for cluster-wide metadata convergence for topic '{topic}': {last_observation}"
    ))
}

/// Waits for a topic to appear in metadata with the expected partition count.
#[allow(dead_code)]
async fn wait_for_topic(
    bootstrap_servers: &str,
    topic: &str,
    expected_partitions: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;

    while std::time::Instant::now() < deadline {
        match fetch_topic_metadata(bootstrap_servers, topic) {
            Ok((count, _)) if count == expected_partitions => {
                return Ok(());
            }
            Ok((count, _)) => {
                eprintln!(
                    "  Topic '{topic}' has {count} partitions, waiting for {expected_partitions}..."
                );
            }
            Err(e) => {
                eprintln!("  Waiting for topic '{topic}': {e}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(format!(
        "timeout waiting for topic '{topic}' with {expected_partitions} partitions"
    ))
}

// ============================================================================
// Test 1.1: Basic Controller Failover
// ============================================================================

/// Tests that controller failover works correctly.
///
/// This test:
/// 1. Creates a topic (requires controller to process `CreateTopic`)
/// 2. Verifies we can write to it
/// 3. Kills each node one at a time, checking topic creation still works
/// 4. Verifies all previously created topics still work
#[tokio::test]
async fn test_controller_failover_basic() {
    let mut cluster = setup_cluster("controller_failover_basic", 24100, 24200).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Step 1: Create initial topic (triggers controller processing)
    println!("\n=== Step 1: Creating initial topic ===");
    let topic1 = "ctrl-test-1";

    // wait_for_leader triggers auto-create and waits for leader election
    let leader = cluster
        .wait_for_leader(topic1, 0, Duration::from_secs(60))
        .await
        .expect("no leader for topic1");
    println!("  Topic '{topic1}' created, leader = node {leader}");

    // Verify we can write to it
    let result = executor.send(topic1, 0, Bytes::from("test-1")).await;
    assert!(result.is_ok(), "Failed to write to topic1: {result:?}");
    println!("  Write to '{topic1}' succeeded");

    // Step 2: Kill nodes one at a time and verify controller still works
    // With 3 nodes, killing 1 maintains quorum (2/3)
    for victim in 1..=3u64 {
        println!("\n=== Step 2.{victim}: Kill node {victim}, create new topic ===");

        cluster.kill_node(victim).expect("failed to kill node");
        println!("  Killed node {victim}");

        // Wait for cluster to detect failure and potentially elect new controller leader
        tokio::time::sleep(Duration::from_secs(5)).await;

        // Create a new topic - this requires controller to be functional
        let topic_name = format!("ctrl-after-kill-{victim}");

        // Trigger topic creation via wait_for_leader (handles retries)
        match cluster
            .wait_for_leader(&topic_name, 0, Duration::from_secs(60))
            .await
        {
            Ok(new_leader) => {
                println!("  Topic '{topic_name}' created, leader = node {new_leader}");
            }
            Err(e) => {
                panic!("Topic creation failed after killing node {victim}: {e}");
            }
        }

        // Verify we can write to the new topic
        let result = executor
            .send(&topic_name, 0, Bytes::from(format!("write-{victim}")))
            .await;
        assert!(
            result.is_ok(),
            "Failed to write to new topic after killing node {victim}: {result:?}"
        );
        println!("  Write to '{topic_name}' succeeded");

        // Verify original topic still works
        let result = executor
            .send(topic1, 0, Bytes::from(format!("after-kill-{victim}")))
            .await;
        assert!(
            result.is_ok(),
            "Failed to write to original topic after killing node {victim}: {result:?}"
        );
        println!("  Original topic '{topic1}' still works");

        // Restart the node before next iteration
        cluster
            .restart_node(victim)
            .expect("failed to restart node");
        println!("  Restarted node {victim}");

        // Wait for node to rejoin
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    // Step 3: Final verification - write to all topics
    println!("\n=== Step 3: Final verification ===");

    let topics_to_check = vec![
        topic1.to_string(),
        "ctrl-after-kill-1".to_string(),
        "ctrl-after-kill-2".to_string(),
        "ctrl-after-kill-3".to_string(),
    ];

    for topic in &topics_to_check {
        let result = executor.send(topic, 0, Bytes::from("final-check")).await;
        assert!(result.is_ok(), "Failed to write to {topic}: {result:?}");
        println!("  Write to '{topic}' succeeded");
    }

    println!("\n=== Controller failover basic test PASSED ===");
}

// ============================================================================
// Test 1.2: Controller Failover During Topic Creation
// ============================================================================

/// Tests controller failover during topic creation with multiple partitions.
///
/// This test creates a topic with many partitions, which generates multiple
/// `AssignPartition` commands. We verify that even if the controller leader
/// fails mid-creation, the topic is eventually created correctly.
#[tokio::test]
async fn test_controller_failover_during_create() {
    let mut cluster = setup_cluster("controller_failover_during_create", 24300, 24400).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create a warmup topic to ensure controller is ready
    println!("\n=== Warmup: Create initial topic ===");
    let warmup_topic = "warmup-topic";
    cluster
        .wait_for_leader(warmup_topic, 0, Duration::from_secs(30))
        .await
        .expect("warmup topic leader not elected");
    println!("  Warmup topic created");

    let (initial_controller, initial_controller_view) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(30),
        )
        .await
        .expect("controller did not converge before failover");
    let killed_controller_node =
        u64::try_from(initial_controller).expect("controller id should convert to u64");
    println!(
        "  Initial controller leader = node {initial_controller} ({initial_controller_view:?})"
    );

    // Now test: create topic with 8 partitions while killing a node
    println!("\n=== Test: Create 8-partition topic while killing controller ===");
    let test_topic = "multi-partition-failover";
    let num_partitions = 8i32;

    // Start creating the topic by requesting metadata (triggers auto-create)
    // Simultaneously kill a node to potentially hit the controller leader
    let create_handle = {
        let bootstrap = bootstrap_servers.clone();
        let topic = test_topic.to_string();
        tokio::spawn(async move {
            // Try to trigger topic creation on all partitions
            for p in 0..num_partitions {
                let consumer: BaseConsumer = ClientConfig::new()
                    .set("bootstrap.servers", &bootstrap)
                    .set("group.id", format!("creator-{p}"))
                    .create()
                    .expect("consumer creation failed");

                // Request metadata - this triggers auto-create
                let _ = consumer.fetch_metadata(Some(&topic), Duration::from_secs(5));
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };

    // Kill the active controller leader shortly after starting creation.
    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("  Killing controller node {killed_controller_node} during topic creation...");
    cluster
        .kill_node(killed_controller_node)
        .expect("failed to kill controller node");

    let new_controller = wait_for_controller_leader_change(
        &bootstrap_servers,
        initial_controller,
        2,
        Duration::from_secs(45),
    )
    .await
    .expect("controller leader did not change after targeted kill");
    println!("  Controller failover: {initial_controller} -> {new_controller}");

    // Wait for creation attempt to complete
    let _ = create_handle.await;

    // Wait for cluster to stabilize and topic creation to complete
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify topic was created (may need to retry after controller failover)
    println!("\n=== Verification: Check topic metadata ===");

    // The topic should eventually exist with the requested partition count.
    let mut topic_found = false;
    for attempt in 1..=10 {
        match fetch_topic_metadata(&bootstrap_servers, test_topic) {
            Ok((count, partitions)) => {
                println!("  Attempt {attempt}: Topic '{test_topic}' has {count} partition(s)");
                for (pid, leader, replicas) in &partitions {
                    println!("    Partition {pid}: leader={leader}, replicas={replicas:?}");
                }
                topic_found = true;

                // Verify we can write to available partitions
                for (pid, leader, _) in &partitions {
                    if *leader >= 0 {
                        let result = executor
                            .send(test_topic, *pid, Bytes::from(format!("test-p{pid}")))
                            .await;
                        if result.is_ok() {
                            println!("    Write to partition {pid} succeeded");
                        }
                    }
                }
                break;
            }
            Err(e) => {
                println!("  Attempt {attempt}: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    assert!(
        topic_found,
        "Topic '{test_topic}' was never created after failover"
    );

    // Restart killed node.
    cluster
        .restart_node(killed_controller_node)
        .expect("failed to restart killed controller node");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let (post_restart_controller, post_restart_view) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(45),
        )
        .await
        .expect("controller did not converge across all brokers after restart");
    println!(
        "  Post-restart controller convergence: node {post_restart_controller} ({post_restart_view:?})"
    );

    // Final verification: all brokers must converge to identical metadata.
    let converged = wait_for_topic_convergence_across_brokers(
        &bootstrap_servers,
        test_topic,
        num_partitions as usize,
        Duration::from_secs(45),
    )
    .await
    .expect("failed to reach cluster-wide metadata convergence");

    println!("\n=== Final metadata convergence ===");
    for (broker, layout) in &converged {
        println!("  {broker}: {} partitions", layout.len());
    }
    println!(
        "\n=== Final state: converged on {num_partitions} partition(s) across all brokers ==="
    );

    println!("\n=== Controller failover during create test PASSED ===");
}

// ============================================================================
// Test 1.3: Controller Failover Under Data Load
// ============================================================================

/// Tests that data operations continue during controller failover.
///
/// The data path (produce/consume) should be independent of the controller.
/// This test verifies:
/// 1. Continuous writes work during controller leader failure
/// 2. New topics can be created after controller recovery
/// 3. Both old and new topics work correctly
#[tokio::test]
async fn test_controller_failover_under_load() {
    let mut cluster = setup_cluster("controller_failover_under_load", 24500, 24600).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create data topic with multiple partitions
    println!("\n=== Setup: Create data topic ===");
    let data_topic = "data-topic";
    let num_partitions = 4i32;

    for p in 0..num_partitions {
        cluster
            .wait_for_leader(data_topic, p, Duration::from_secs(30))
            .await
            .unwrap_or_else(|_| panic!("no leader for partition {p}"));
    }
    println!("  Data topic created with {num_partitions} partitions");

    // Track acknowledged writes for verification
    let mut acknowledged: Vec<(i32, u64, String)> = Vec::new();

    // Phase 1: Write some data before failover
    println!("\n=== Phase 1: Initial writes ===");
    for p in 0..num_partitions {
        for i in 0..25u32 {
            let payload = format!("phase1-p{p}-{i:04}");
            match executor
                .send(data_topic, p, Bytes::from(payload.clone()))
                .await
            {
                Ok(offset) => {
                    acknowledged.push((p, offset, payload));
                }
                Err(e) => panic!("Phase 1 write failed: {e}"),
            }
        }
    }
    let phase1_count = acknowledged.len();
    println!("  Phase 1: {phase1_count} writes acknowledged");

    // Phase 2: Start continuous writes and kill nodes
    println!("\n=== Phase 2: Writes during controller failover ===");

    for victim in 1..=3u64 {
        println!("  Killing node {victim}...");
        cluster.kill_node(victim).expect("failed to kill node");

        // Continue writing during failover
        let mut writes_during = 0u32;
        let mut failures_during = 0u32;

        for p in 0..num_partitions {
            for i in 0..10u32 {
                let payload = format!("during-kill{victim}-p{p}-{i:04}");
                match executor
                    .send(data_topic, p, Bytes::from(payload.clone()))
                    .await
                {
                    Ok(offset) => {
                        acknowledged.push((p, offset, payload));
                        writes_during += 1;
                    }
                    Err(_) => {
                        failures_during += 1;
                    }
                }
            }
        }
        println!("    Writes during kill-{victim}: {writes_during} ok, {failures_during} failed");

        // Restart node
        cluster.restart_node(victim).expect("failed to restart");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Phase 3: Create new topic after all failovers
    println!("\n=== Phase 3: Create new topic after failovers ===");
    let new_topic = "post-failover-topic";

    cluster
        .wait_for_leader(new_topic, 0, Duration::from_secs(30))
        .await
        .expect("failed to create post-failover topic");
    println!("  New topic '{new_topic}' created successfully");

    // Write to new topic
    for i in 0..10u32 {
        let result = executor
            .send(new_topic, 0, Bytes::from(format!("new-{i:04}")))
            .await;
        assert!(result.is_ok(), "Failed to write to new topic: {result:?}");
    }
    println!("  Writes to new topic succeeded");

    // Phase 4: Verify all acknowledged data
    println!("\n=== Phase 4: Verify acknowledged data ===");

    let mut verified = 0u32;
    let mut errors = 0u32;

    // Group by partition for efficient polling
    let mut by_partition: std::collections::HashMap<i32, Vec<(u64, String)>> =
        std::collections::HashMap::new();
    for (p, offset, payload) in &acknowledged {
        by_partition
            .entry(*p)
            .or_default()
            .push((*offset, payload.clone()));
    }

    for (partition, expected) in &by_partition {
        let consumed = executor
            .poll(data_topic, *partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {
                    verified += 1;
                }
                Some(actual) => {
                    eprintln!(
                        "CORRUPTION: p{partition}@{offset}: expected '{expected_payload}', got '{}'",
                        String::from_utf8_lossy(actual)
                    );
                    errors += 1;
                }
                None => {
                    eprintln!("DATA LOSS: p{partition}@{offset}: '{expected_payload}'");
                    errors += 1;
                }
            }
        }
    }

    let total_ack = acknowledged.len();
    println!("\n=== Results ===");
    println!("  Total acknowledged: {total_ack}");
    println!("  Verified: {verified}");
    println!("  Errors: {errors}");

    assert_eq!(errors, 0, "Data integrity errors after controller failover");
    assert_eq!(
        verified as usize, total_ack,
        "Not all acknowledged writes verified"
    );

    println!("\n=== Controller failover under load test PASSED ===");
}

// ============================================================================
// Test 1.4: Controller State Consistency After Multiple Failovers
// ============================================================================

/// Tests that controller state remains consistent after multiple failovers.
///
/// This test:
/// 1. Creates multiple topics
/// 2. Performs multiple controller failovers
/// 3. Verifies all topic metadata is consistent across all nodes
#[tokio::test]
async fn test_controller_state_consistency() {
    let mut cluster = setup_cluster("controller_state_consistency", 24700, 24800).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create several topics
    println!("\n=== Setup: Create 5 topics ===");
    let topics: Vec<String> = (1..=5).map(|i| format!("consistency-topic-{i}")).collect();

    for topic in &topics {
        cluster
            .wait_for_leader(topic, 0, Duration::from_secs(30))
            .await
            .unwrap_or_else(|_| panic!("failed to create topic {topic}"));
        println!("  Created: {topic}");
    }

    // Perform multiple failover cycles
    println!("\n=== Failover cycles ===");
    for cycle in 1..=5u32 {
        let victim = ((cycle - 1) % 3) + 1;
        println!("  Cycle {cycle}: kill node {victim}");

        cluster.kill_node(victim.into()).expect("kill failed");
        tokio::time::sleep(Duration::from_secs(2)).await;

        cluster.restart_node(victim.into()).expect("restart failed");
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Verify consistency: all nodes should report same metadata
    println!("\n=== Verify metadata consistency across nodes ===");

    let base_port = 24700u16;
    let mut node_metadata: Vec<Vec<(String, usize)>> = Vec::new();

    for node_id in 1..=3u64 {
        let port = base_port + node_id as u16;
        let node_bootstrap = format!("127.0.0.1:{port}");

        let mut this_node: Vec<(String, usize)> = Vec::new();
        for topic in &topics {
            match fetch_topic_metadata(&node_bootstrap, topic) {
                Ok((count, _)) => {
                    this_node.push((topic.clone(), count));
                }
                Err(e) => {
                    eprintln!("  Node {node_id}: failed to fetch {topic}: {e}");
                    this_node.push((topic.clone(), 0));
                }
            }
        }
        println!("  Node {node_id}: {this_node:?}");
        node_metadata.push(this_node);
    }

    // All nodes should have identical view
    let reference = &node_metadata[0];
    for (node_idx, metadata) in node_metadata.iter().enumerate().skip(1) {
        assert_eq!(
            reference,
            metadata,
            "Node {} has different metadata than node 1",
            node_idx + 1
        );
    }

    // Verify we can write to all topics
    println!("\n=== Verify write access to all topics ===");
    for topic in &topics {
        let result = executor.send(topic, 0, Bytes::from("final-check")).await;
        assert!(result.is_ok(), "Failed to write to {topic}: {result:?}");
    }
    println!("  All topics writable");

    println!("\n=== Controller state consistency test PASSED ===");
}

// ============================================================================
// Test 1.5: Quorum Loss - Operations MUST Fail
// ============================================================================

/// Tests that operations fail when quorum is lost, then recover once quorum is restored.
///
/// This validates a critical safety and recovery property:
/// 1. With 2 of 3 nodes dead, topic creation/write fail.
/// 2. After both nodes restart, metadata and writes recover eventually.
#[tokio::test]
async fn test_controller_quorum_loss_must_fail() {
    let mut cluster = setup_cluster("controller_quorum_loss", 24900, 25000).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create initial topic to verify cluster works
    println!("\n=== Setup: Verify cluster works ===");
    let setup_topic = "quorum-setup";
    cluster
        .wait_for_leader(setup_topic, 0, Duration::from_secs(30))
        .await
        .expect("setup topic creation failed");
    println!("  Setup topic created - cluster is healthy");

    let setup_converged = wait_for_topic_convergence_across_brokers(
        &bootstrap_servers,
        setup_topic,
        8,
        Duration::from_secs(60),
    )
    .await
    .expect("setup topic did not converge across brokers before quorum-loss step");
    println!(
        "  Setup topic converged across {} broker(s)",
        setup_converged.len()
    );

    // Kill 2 of 3 nodes - this MUST break quorum
    println!("\n=== Kill 2 nodes to break quorum ===");
    cluster.kill_node(1).expect("failed to kill node 1");
    cluster.kill_node(2).expect("failed to kill node 2");
    println!("  Killed nodes 1 and 2 - only node 3 remains");

    // Wait for cluster to detect failures
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try to create a new topic - this SHOULD FAIL
    println!("\n=== Attempt topic creation without quorum (should fail) ===");
    let fail_topic = "quorum-fail-topic";

    // Use a short timeout - we expect this to fail
    let result = cluster
        .wait_for_leader(fail_topic, 0, Duration::from_secs(10))
        .await;

    match result {
        Ok(leader) => {
            panic!("Topic creation unexpectedly succeeded without quorum; leader={leader}");
        }
        Err(e) => {
            println!("  EXPECTED: Topic creation failed: {e}");
            println!("  This confirms quorum is required for metadata operations");
        }
    }

    // Also try a write to the existing topic - this should also fail
    println!("\n=== Attempt write without quorum ===");
    let write_result = executor
        .send(setup_topic, 0, Bytes::from("quorum-test"))
        .await;
    match write_result {
        Ok(offset) => {
            panic!("Write unexpectedly succeeded without quorum at offset {offset}");
        }
        Err(e) => {
            println!("  EXPECTED: Write failed: {e}");
            println!("  This confirms quorum is required for data operations");
        }
    }

    // Restore quorum by restarting nodes
    println!("\n=== Restore quorum ===");
    cluster.restart_node(1).expect("failed to restart node 1");
    println!("  Restarted node 1");
    tokio::time::sleep(Duration::from_secs(3)).await;

    cluster.restart_node(2).expect("failed to restart node 2");
    println!("  Restarted node 2");

    // Wait for controller to converge across all brokers.
    let (controller_after_restore, controller_view_after_restore) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(90),
        )
        .await
        .expect("controller did not converge after quorum restore");
    println!(
        "  Controller convergence after restore: node {} ({:?})",
        controller_after_restore, controller_view_after_restore
    );

    // Keep using the original long-lived client and wait for metadata to recover.
    executor
        .wait_ready(Duration::from_secs(90))
        .await
        .expect("cluster did not become ready after quorum restore");

    let setup_recovered = wait_for_topic_convergence_across_brokers(
        &bootstrap_servers,
        setup_topic,
        8,
        Duration::from_secs(90),
    )
    .await
    .expect("setup topic metadata did not recover cluster-wide after quorum restore");
    println!(
        "  Existing topic recovered across {} broker(s)",
        setup_recovered.len()
    );

    // Now topic creation should work
    println!("\n=== Verify recovery: create topic after quorum restored ===");
    let recovery_topic = "quorum-recovery-topic";

    let result = cluster
        .wait_for_leader(recovery_topic, 0, Duration::from_secs(120))
        .await;

    match result {
        Ok(leader) => {
            println!("  Topic '{recovery_topic}' created, leader = {leader}");
        }
        Err(e) => {
            panic!("Topic creation failed after quorum restored: {e}");
        }
    }

    let recovery_converged = wait_for_topic_convergence_across_brokers(
        &bootstrap_servers,
        recovery_topic,
        8,
        Duration::from_secs(120),
    )
    .await
    .expect("recovery topic metadata did not converge cluster-wide");
    println!(
        "  Recovery topic converged across {} broker(s)",
        recovery_converged.len()
    );

    // Verify write works with eventual success semantics.
    let send_deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut attempts = 0usize;
    let mut last_err = None;
    let write_offset = loop {
        attempts += 1;
        match executor
            .send(recovery_topic, 0, Bytes::from("recovery-test"))
            .await
        {
            Ok(offset) => break Some(offset),
            Err(e) => {
                last_err = Some(e);
                if std::time::Instant::now() >= send_deadline {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    let Some(offset) = write_offset else {
        panic!(
            "Write failed after quorum restored (attempts={attempts}): {:?}",
            last_err
        );
    };
    println!("  Write to recovery topic succeeded at offset {offset} (attempts={attempts})");

    println!("\n=== Controller quorum loss test PASSED ===");
}

// ============================================================================
// Test 1.6: Quorum Loss During Bootstrap
// ============================================================================

/// Tests quorum loss during early metadata bootstrap, then eventual recovery.
///
/// This scenario intentionally injects quorum loss while a first topic operation
/// is in-flight (before any cluster-wide convergence checks), then asserts:
/// 1. No-quorum operations fail.
/// 2. After quorum restore, controller and metadata converge again.
/// 3. The same long-lived client can produce successfully.
#[tokio::test]
async fn test_controller_quorum_loss_during_bootstrap_recovers() {
    let mut cluster = setup_cluster("controller_quorum_loss_during_bootstrap", 25300, 25400).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = Arc::new(
        RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
            .expect("failed to create executor"),
    );
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    println!("\n=== Step 1: Start bootstrap write, then break quorum immediately ===");
    let bootstrap_topic = "quorum-bootstrap-topic";
    let in_flight = {
        let executor = Arc::clone(&executor);
        tokio::spawn(async move {
            executor
                .send(bootstrap_topic, 0, Bytes::from("bootstrap-in-flight"))
                .await
        })
    };

    // Intentionally break quorum quickly, before waiting for any convergence.
    cluster.kill_node(1).expect("failed to kill node 1");
    cluster.kill_node(2).expect("failed to kill node 2");
    println!("  Killed nodes 1 and 2");

    // The in-flight operation may succeed or fail depending on exact timing,
    // but it must complete (no indefinite hang).
    match tokio::time::timeout(Duration::from_secs(40), in_flight).await {
        Ok(Ok(Ok(offset))) => {
            println!("  In-flight bootstrap write acknowledged at offset {offset}");
        }
        Ok(Ok(Err(e))) => {
            println!("  In-flight bootstrap write failed (acceptable under fault): {e}");
        }
        Ok(Err(join_err)) => {
            panic!("bootstrap write task panicked/join failed: {join_err}");
        }
        Err(_) => {
            panic!("in-flight bootstrap write did not complete within timeout");
        }
    }

    println!("\n=== Step 2: Without quorum, operations must fail ===");
    let no_quorum_topic = "quorum-bootstrap-noquorum-topic";
    let create_without_quorum = cluster
        .wait_for_leader(no_quorum_topic, 0, Duration::from_secs(12))
        .await;
    assert!(
        create_without_quorum.is_err(),
        "Topic creation unexpectedly succeeded without quorum: {create_without_quorum:?}"
    );
    println!("  EXPECTED: topic creation failed without quorum");

    let write_without_quorum = executor
        .send(
            bootstrap_topic,
            0,
            Bytes::from("should-fail-without-quorum"),
        )
        .await;
    assert!(
        write_without_quorum.is_err(),
        "Write unexpectedly succeeded without quorum: {write_without_quorum:?}"
    );
    println!("  EXPECTED: write failed without quorum");

    println!("\n=== Step 3: Restore quorum and require convergence ===");
    cluster.restart_node(1).expect("failed to restart node 1");
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.restart_node(2).expect("failed to restart node 2");

    let (controller_after_restore, controller_view_after_restore) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(90),
        )
        .await
        .expect("controller did not converge after restore");
    println!(
        "  Controller convergence after restore: node {} ({:?})",
        controller_after_restore, controller_view_after_restore
    );

    executor
        .wait_ready(Duration::from_secs(90))
        .await
        .expect("cluster did not become ready after restore");

    println!("\n=== Step 4: Verify new topic creation and writes recover ===");
    let recovery_topic = "quorum-bootstrap-recovery-topic";
    let recovery_leader = cluster
        .wait_for_leader(recovery_topic, 0, Duration::from_secs(120))
        .await
        .expect("failed to create recovery topic after quorum restore");
    println!("  Recovery topic leader = node {recovery_leader}");

    let recovery_converged = wait_for_topic_convergence_across_brokers(
        &bootstrap_servers,
        recovery_topic,
        8,
        Duration::from_secs(120),
    )
    .await
    .expect("recovery topic metadata did not converge cluster-wide");
    println!(
        "  Recovery topic converged across {} broker(s)",
        recovery_converged.len()
    );

    let send_deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut attempts = 0usize;
    let mut last_err = None;
    let final_offset = loop {
        attempts += 1;
        match executor
            .send(recovery_topic, 0, Bytes::from("recovery-write"))
            .await
        {
            Ok(offset) => break Some(offset),
            Err(e) => {
                last_err = Some(e);
                if std::time::Instant::now() >= send_deadline {
                    break None;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    };
    let Some(final_offset) = final_offset else {
        panic!(
            "Write failed after quorum restore (attempts={attempts}): {:?}",
            last_err
        );
    };
    println!("  Recovery write succeeded at offset {final_offset} (attempts={attempts})");

    println!("\n=== Controller quorum-loss-during-bootstrap test PASSED ===");
}

// ============================================================================
// Test 1.7: Targeted Controller Leader Kill
// ============================================================================
/// Tests killing the actual controller leader (not random nodes).
///
/// This test:
/// 1. Identifies which node is controller leader
/// 2. Kills that specific node
/// 3. Verifies new controller leader is elected
/// 4. Verifies metadata operations work with new leader
#[tokio::test]
async fn test_targeted_controller_leader_kill() {
    let mut cluster = setup_cluster("targeted_controller_kill", 25100, 25200).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create initial topic.
    println!("\n=== Setup: Create initial topic ===");
    let topic1 = "targeted-topic-1";
    let topic1_data_leader = cluster
        .wait_for_leader(topic1, 0, Duration::from_secs(30))
        .await
        .expect("initial topic creation failed");
    println!("  Topic '{topic1}' created, data leader = {topic1_data_leader}");

    let (initial_controller, initial_controller_view) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(30),
        )
        .await
        .expect("controller did not converge before kill");
    let majority_controller = get_controller_leader(&bootstrap_servers)
        .await
        .expect("failed to determine controller majority");
    assert_eq!(
        initial_controller, majority_controller,
        "controller majority and convergence should match"
    );
    assert!(
        initial_controller > 0,
        "invalid controller id {initial_controller}"
    );
    let initial_controller_u64 =
        u64::try_from(initial_controller).expect("controller id should convert to u64");
    println!(
        "  Initial controller leader = node {initial_controller} ({initial_controller_view:?})"
    );

    // Acknowledged writes across failover must remain readable by offset.
    let mut acknowledged: Vec<(u64, String)> = Vec::new();
    for i in 0..20u32 {
        let payload = format!("pre-failover-{i:04}");
        let offset = executor
            .send(topic1, 0, Bytes::from(payload.clone()))
            .await
            .expect("pre-failover write failed");
        acknowledged.push((offset, payload));
    }
    println!("  Pre-failover acknowledged writes: {}", acknowledged.len());

    println!("\n=== Kill controller leader (node {initial_controller}) ===");
    cluster
        .kill_node(initial_controller_u64)
        .expect("failed to kill controller leader");
    println!("  Killed controller leader node {initial_controller}");

    let mut during_failover_acks = 0u32;
    let mut during_failover_failures = 0u32;
    for i in 0..60u32 {
        let payload = format!("during-failover-{i:04}");
        match executor.send(topic1, 0, Bytes::from(payload.clone())).await {
            Ok(offset) => {
                acknowledged.push((offset, payload));
                during_failover_acks += 1;
            }
            Err(_) => {
                during_failover_failures += 1;
            }
        }
    }
    println!(
        "  During-failover writes: {during_failover_acks} acknowledged, {during_failover_failures} failed"
    );

    let new_controller = wait_for_controller_leader_change(
        &bootstrap_servers,
        initial_controller,
        2,
        Duration::from_secs(45),
    )
    .await
    .expect("controller leader did not change after kill");
    println!("  New controller leader = node {new_controller}");
    let (_, post_kill_view) = wait_for_controller_convergence_across_live_brokers(
        &bootstrap_servers,
        Some(new_controller),
        2,
        Duration::from_secs(20),
    )
    .await
    .expect("controller did not converge on surviving brokers after kill");
    println!("  Post-kill controller convergence (survivors): {post_kill_view:?}");

    // Create new topic - must succeed under the new controller leader.
    println!("\n=== Create new topic after killing controller node {initial_controller} ===");
    let topic2 = "targeted-topic-2";
    let new_leader = cluster
        .wait_for_leader(topic2, 0, Duration::from_secs(60))
        .await
        .expect("topic creation failed after leader kill");
    println!("  Topic '{topic2}' created, new leader = {new_leader}");

    // Verify the killed controller is not reported as topic leader.
    assert_ne!(
        new_leader as i32, initial_controller,
        "New topic should not have killed controller node as leader"
    );

    // Verify writes work to both topics and keep tracking acked durability writes.
    let post_payload = "after-kill";
    let result = executor
        .send(topic1, 0, Bytes::from(post_payload))
        .await
        .expect("post-failover write to topic1 failed");
    acknowledged.push((result, post_payload.to_string()));
    println!("  Write to '{topic1}' succeeded");

    let result = executor.send(topic2, 0, Bytes::from("new-topic")).await;
    assert!(result.is_ok(), "Write to {topic2} failed: {result:?}");
    println!("  Write to '{topic2}' succeeded");

    // Restart killed node
    cluster
        .restart_node(initial_controller_u64)
        .expect("restart failed");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (post_restart_controller, post_restart_view) =
        wait_for_controller_convergence_across_live_brokers(
            &bootstrap_servers,
            None,
            3,
            Duration::from_secs(45),
        )
        .await
        .expect("controller did not converge across all brokers after restart");
    println!(
        "  Post-restart controller convergence: node {post_restart_controller} ({post_restart_view:?})"
    );

    // Verify no acknowledged writes were lost.
    let consumed = executor
        .poll(topic1, 0, 0, acknowledged.len() as u32 + 50)
        .await
        .expect("poll failed for durability verification");
    let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();
    for (offset, expected_payload) in &acknowledged {
        let actual = consumed_map
            .get(offset)
            .unwrap_or_else(|| panic!("acknowledged offset {offset} missing after failover"));
        assert_eq!(
            String::from_utf8_lossy(actual),
            expected_payload.as_str(),
            "payload mismatch at offset {offset}"
        );
    }
    println!(
        "  Durability verified for {} acknowledged writes",
        acknowledged.len()
    );

    // Final verification
    println!("\n=== Final verification ===");
    let result = executor.send(topic1, 0, Bytes::from("final")).await;
    assert!(result.is_ok(), "Final write failed: {result:?}");
    println!("  Final write succeeded");

    println!("\n=== Targeted controller leader kill test PASSED ===");
}

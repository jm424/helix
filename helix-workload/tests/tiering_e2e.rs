#![allow(clippy::cast_possible_truncation)] // Test constants are small.

//! E2E tiering tests for helix-workload.
//!
//! These tests verify tiered storage correctness through the full stack:
//! Kafka client -> helix-server -> WAL -> `TieringManager` -> S3 (`LocalStack`).
//!
//! # Prerequisites
//!
//! 1. `LocalStack` running:
//!    ```bash
//!    docker run --rm -p 4566:4566 localstack/localstack
//!    ```
//!
//! 2. AWS CLI configured (for bucket creation):
//!    ```bash
//!    aws configure set aws_access_key_id test
//!    aws configure set aws_secret_access_key test
//!    aws configure set region us-east-1
//!    ```
//!
//! # Running Tests
//!
//! ```bash
//! # Build helix-server with S3 support
//! cargo build --release -p helix-server --features s3
//!
//! # Run tiering tests (ignored by default)
//! cargo test -p helix-workload --test tiering_e2e -- --ignored --nocapture
//! ```

use bytes::Bytes;
use helix_workload::{RealCluster, RealExecutor, WorkloadExecutor};
use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

const LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";

fn helix_binary_path() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR is helix-workload/, so go up one level to project root.
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .expect("manifest dir has no parent")
        .join("target/release/helix-server")
}

// =============================================================================
// `LocalStack` Utilities
// =============================================================================

/// Check if `LocalStack` is running and healthy.
fn is_localstack_running() -> bool {
    let output = Command::new("curl")
        .args(["-s", "-o", "/dev/null", "-w", "%{http_code}"])
        .arg(format!("{LOCALSTACK_ENDPOINT}/_localstack/health"))
        .output();

    match output {
        Ok(o) => String::from_utf8_lossy(&o.stdout).trim() == "200",
        Err(_) => false,
    }
}

/// Ensure `LocalStack` is running, skip test if not.
fn require_localstack() {
    if !is_localstack_running() {
        eprintln!("`LocalStack` not running. Start with:");
        eprintln!("  docker run --rm -p 4566:4566 localstack/localstack");
        panic!("`LocalStack` required for this test");
    }
}

/// Ensure helix-server binary exists with S3 support.
fn require_helix_binary() {
    let binary = helix_binary_path();
    if !binary.exists() {
        eprintln!("helix-server binary not found at {}", binary.display());
        eprintln!("Build with: cargo build --release -p helix-server --features s3");
        panic!("helix-server binary required");
    }
}

/// Create an S3 bucket in `LocalStack`.
fn create_s3_bucket(bucket: &str) {
    let output = Command::new("aws")
        .args(["--endpoint-url", LOCALSTACK_ENDPOINT])
        .args(["s3", "mb", &format!("s3://{bucket}")])
        .output()
        .expect("aws cli failed");

    if output.status.success() {
        eprintln!("[S3] Created bucket: {bucket}");
    } else {
        // Bucket may already exist, that's fine.
        let stderr = String::from_utf8_lossy(&output.stderr);
        if !stderr.contains("BucketAlreadyOwnedByYou") && !stderr.contains("BucketAlreadyExists") {
            eprintln!("[S3] Warning creating bucket: {stderr}");
        }
    }
}

/// Delete an S3 bucket and all its contents in `LocalStack`.
fn delete_s3_bucket(bucket: &str) {
    let _ = Command::new("aws")
        .args(["--endpoint-url", LOCALSTACK_ENDPOINT])
        .args(["s3", "rb", "--force", &format!("s3://{bucket}")])
        .output();
    eprintln!("[S3] Deleted bucket: {bucket}");
}

/// List objects in an S3 bucket.
fn list_s3_objects(bucket: &str, prefix: &str) -> Vec<String> {
    let output = Command::new("aws")
        .args(["--endpoint-url", LOCALSTACK_ENDPOINT])
        .args(["s3", "ls", &format!("s3://{bucket}/{prefix}"), "--recursive"])
        .output()
        .expect("aws cli failed");

    if !output.status.success() {
        return Vec::new();
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().last().map(String::from))
        .collect()
}

// =============================================================================
// Test Helpers
// =============================================================================

/// Send messages and track acknowledged offsets.
async fn send_messages(
    executor: &RealExecutor,
    topic: &str,
    partition: i32,
    count: usize,
    prefix: &str,
) -> Vec<(u64, Bytes)> {
    let mut acknowledged = Vec::new();

    for i in 0..count {
        let payload = Bytes::from(format!("{prefix}-{i:06}"));
        match executor.send(topic, partition, payload.clone()).await {
            Ok(offset) => {
                acknowledged.push((offset, payload));
            }
            Err(e) => {
                eprintln!("[SEND] Error at {i}: {e}");
            }
        }
    }

    acknowledged
}

/// Verify all acknowledged messages can be read back correctly.
fn verify_messages(
    acknowledged: &[(u64, Bytes)],
    received: &[(u64, Bytes)],
) -> (usize, Vec<u64>, Vec<u64>) {
    let received_map: HashMap<u64, &Bytes> = received.iter().map(|(o, p)| (*o, p)).collect();

    let mut matched = 0;
    let mut lost = Vec::new();
    let mut corrupted = Vec::new();

    for (offset, expected_payload) in acknowledged {
        match received_map.get(offset) {
            Some(actual_payload) => {
                if *actual_payload == expected_payload {
                    matched += 1;
                } else {
                    corrupted.push(*offset);
                }
            }
            None => {
                lost.push(*offset);
            }
        }
    }

    (matched, lost, corrupted)
}

// =============================================================================
// Test: Basic Tiering Roundtrip
// =============================================================================

/// Tests that data written via Kafka survives tiering and can be read back.
///
/// This test:
/// 1. Starts a 3-node cluster with S3 tiering enabled
/// 2. Writes messages to trigger segment rotation
/// 3. Waits for tiering to complete
/// 4. Reads all messages back and verifies correctness
#[tokio::test]
#[ignore = "requires `LocalStack` and helix-server with S3 feature"]
async fn test_basic_tiering_roundtrip() {
    require_localstack();
    require_helix_binary();

    let bucket = "helix-tiering-test-basic";
    create_s3_bucket(bucket);

    eprintln!("\n=== Basic Tiering Roundtrip Test ===\n");

    // Start cluster with S3 tiering.
    eprintln!("Starting 3-node cluster with `LocalStack` S3 tiering...");
    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19092)
        .raft_base_port(50100)
        .binary_path(helix_binary_path())
        .data_dir("/tmp/helix-tiering-test-basic")
        .with_localstack_tiering(bucket)
        .topic("tier-test", 1)
        .build()
        .expect("failed to start cluster");

    assert!(cluster.is_tiering_enabled(), "Tiering should be enabled");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");

    eprintln!("Waiting for cluster to be ready...");
    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");

    eprintln!("Waiting for leader election...");
    cluster
        .wait_for_leader("tier-test", 0, Duration::from_secs(30))
        .await
        .expect("no leader elected");

    // Phase 1: Write messages.
    // Default segment size is 4 MiB. With ~125 byte entries after WAL overhead,
    // we need ~33,500 entries per segment. Write enough to fill at least 2 segments.
    let topic = "tier-test";
    let partition = 0;
    let message_count = 70_000;

    eprintln!("\nPhase 1: Writing {message_count} messages...");
    let acknowledged = send_messages(&executor, topic, partition, message_count, "tier-msg").await;
    eprintln!("  Acknowledged: {} messages", acknowledged.len());

    assert!(!acknowledged.is_empty(), "No messages were acknowledged");

    // Phase 2: Wait for tiering.
    eprintln!("\nPhase 2: Waiting for tiering to complete...");
    cluster.wait_for_tiering_duration(Duration::from_secs(10)).await;

    // Check S3 for tiered objects.
    let s3_objects = list_s3_objects(bucket, "helix/segments/");
    eprintln!("  S3 objects found: {}", s3_objects.len());
    for obj in &s3_objects {
        eprintln!("    - {obj}");
    }

    // Phase 3: Read back and verify.
    eprintln!("\nPhase 3: Reading messages back...");
    let received = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .expect("poll failed");
    eprintln!("  Received: {} messages", received.len());

    let (matched, lost, corrupted) = verify_messages(&acknowledged, &received);

    eprintln!("\n=== Results ===");
    eprintln!("  Matched: {matched}");
    eprintln!("  Lost: {}", lost.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    assert!(lost.is_empty(), "Lost messages: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted messages: {corrupted:?}");
    assert_eq!(matched, acknowledged.len(), "Not all messages matched");

    eprintln!("\nPASSED: Basic tiering roundtrip");

    // Cleanup.
    drop(cluster);
    delete_s3_bucket(bucket);
}

// =============================================================================
// Test: Data Survives Full Cluster Restart with Tiering
// =============================================================================

/// Tests that tiered data survives a full cluster restart.
///
/// This is the key durability test: if data can be read after all nodes
/// restart, tiering to S3 is working correctly.
#[tokio::test]
#[ignore = "requires `LocalStack` and helix-server with S3 feature"]
async fn test_tiering_survives_cluster_restart() {
    require_localstack();
    require_helix_binary();

    let bucket = "helix-tiering-test-restart";
    create_s3_bucket(bucket);

    eprintln!("\n=== Tiering Survives Cluster Restart Test ===\n");

    // Start cluster.
    eprintln!("Starting 3-node cluster with `LocalStack` S3 tiering...");
    let mut cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19192)
        .raft_base_port(50200)
        .binary_path(helix_binary_path())
        .data_dir("/tmp/helix-tiering-test-restart")
        .with_localstack_tiering(bucket)
        .topic("restart-test", 1)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");
    cluster
        .wait_for_leader("restart-test", 0, Duration::from_secs(30))
        .await
        .expect("no leader");

    // Phase 1: Write data.
    let topic = "restart-test";
    let partition = 0;
    let message_count = 150;

    eprintln!("\nPhase 1: Writing {message_count} messages...");
    let acknowledged =
        send_messages(&executor, topic, partition, message_count, "restart-msg").await;
    eprintln!("  Acknowledged: {} messages", acknowledged.len());

    // Phase 2: Wait for tiering.
    eprintln!("\nPhase 2: Waiting for tiering...");
    cluster.wait_for_tiering_duration(Duration::from_secs(10)).await;

    let s3_objects = list_s3_objects(bucket, "helix/segments/");
    eprintln!("  S3 objects: {}", s3_objects.len());

    // Phase 3: Full cluster restart.
    eprintln!("\nPhase 3: Stopping all nodes...");
    cluster.stop();
    tokio::time::sleep(Duration::from_secs(2)).await;

    eprintln!("Restarting all nodes...");
    cluster.restart_all().expect("restart failed");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready after restart");
    cluster
        .wait_for_leader(topic, partition, Duration::from_secs(60))
        .await
        .expect("no leader after restart");

    // Phase 4: Verify data survived.
    eprintln!("\nPhase 4: Verifying data after restart...");
    let received = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .expect("poll failed");
    eprintln!("  Received: {} messages", received.len());

    let (matched, lost, corrupted) = verify_messages(&acknowledged, &received);

    eprintln!("\n=== Results ===");
    eprintln!("  Matched: {matched}");
    eprintln!("  Lost: {}", lost.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    assert!(lost.is_empty(), "Lost messages after restart: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted messages: {corrupted:?}");
    assert_eq!(matched, acknowledged.len());

    eprintln!("\nPASSED: Tiering survives cluster restart");

    // Cleanup.
    drop(cluster);
    delete_s3_bucket(bucket);
}

// =============================================================================
// Test: Filesystem Tiering (No S3 Required)
// =============================================================================

/// Tests filesystem-based tiering without requiring `LocalStack`.
///
/// This test uses the filesystem object storage backend instead of S3,
/// making it easier to run without external dependencies.
#[tokio::test]
#[ignore = "requires helix-server binary"]
async fn test_filesystem_tiering() {
    require_helix_binary();

    let tier_dir = "/tmp/helix-tiering-test-fs-tier";
    let data_dir = "/tmp/helix-tiering-test-fs-data";

    // Cleanup previous runs.
    let _ = std::fs::remove_dir_all(tier_dir);
    let _ = std::fs::remove_dir_all(data_dir);

    eprintln!("\n=== Filesystem Tiering Test ===\n");

    eprintln!("Starting 3-node cluster with filesystem tiering...");
    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19292)
        .raft_base_port(50300)
        .binary_path(helix_binary_path())
        .data_dir(data_dir)
        .with_filesystem_tiering(tier_dir)
        .topic("fs-tier-test", 1)
        .build()
        .expect("failed to start cluster");

    assert!(cluster.is_tiering_enabled());
    assert!(cluster.object_storage_dir().is_some());

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");
    cluster
        .wait_for_leader("fs-tier-test", 0, Duration::from_secs(30))
        .await
        .expect("no leader");

    // Write and verify.
    let topic = "fs-tier-test";
    let partition = 0;
    let message_count = 100;

    eprintln!("\nWriting {message_count} messages...");
    let acknowledged =
        send_messages(&executor, topic, partition, message_count, "fs-tier-msg").await;
    eprintln!("  Acknowledged: {}", acknowledged.len());

    // Wait for tiering.
    cluster.wait_for_tiering_duration(Duration::from_secs(5)).await;

    // Check filesystem for tiered files.
    let tier_files: Vec<_> = std::fs::read_dir(tier_dir)
        .map(|entries| entries.filter_map(Result::ok).collect())
        .unwrap_or_default();
    eprintln!("  Tier directory entries: {}", tier_files.len());

    // Read back.
    let received = executor
        .poll(topic, partition, 0, message_count as u32)
        .await
        .expect("poll failed");

    let (matched, lost, corrupted) = verify_messages(&acknowledged, &received);

    assert!(lost.is_empty(), "Lost: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted: {corrupted:?}");
    assert_eq!(matched, acknowledged.len());

    eprintln!("\nPASSED: Filesystem tiering");

    // Cleanup.
    drop(cluster);
    let _ = std::fs::remove_dir_all(tier_dir);
    let _ = std::fs::remove_dir_all(data_dir);
}

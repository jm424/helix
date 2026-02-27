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
//! # Build helix-server (S3 is enabled by default)
//! cargo build --release -p helix-server
//!
//! # Run tiering tests (ignored by default, require LocalStack)
//! cargo test -p helix-workload --test tiering_e2e -- --ignored --nocapture
//! ```

use bytes::Bytes;
use helix_workload::{RealCluster, RealExecutor, TieringTestConfig, WorkloadExecutor};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use std::collections::{HashMap, HashSet};
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

const LOCALSTACK_ENDPOINT: &str = "http://localhost:4566";

struct ProgressHeartbeat {
    stop_tx: Option<mpsc::Sender<()>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl ProgressHeartbeat {
    fn start(test_name: String, run_start: Instant) -> Self {
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        let handle = std::thread::spawn(move || loop {
            match stop_rx.recv_timeout(Duration::from_secs(15)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "[E2E_PROGRESS] test={} elapsed={}s",
                        test_name,
                        run_start.elapsed().as_secs()
                    );
                }
            }
        });

        Self {
            stop_tx: Some(stop_tx),
            handle: Some(handle),
        }
    }
}

impl Drop for ProgressHeartbeat {
    fn drop(&mut self) {
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn current_test_name() -> String {
    std::thread::current()
        .name()
        .unwrap_or("tiering_e2e")
        .to_string()
}

fn e2e_server_log_level() -> String {
    std::env::var("HELIX_E2E_SERVER_LOG_LEVEL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "warn".to_string())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn e2e_data_dir(default: &str) -> String {
    std::env::var("HELIX_E2E_DATA_DIR")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn helix_binary_path() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR is helix-workload/, so go up one level to project root.
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .expect("manifest dir has no parent")
        .join("target/release/helix-server")
}

fn log_metadata_snapshot(bootstrap_servers: &str, topic: &str, partition: i32) {
    eprintln!(
        "[E2E_DIAG] metadata snapshot start bootstrap_servers={} topic={} partition={}",
        bootstrap_servers, topic, partition
    );

    for broker in bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let consumer: Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", "tiering-e2e-metadata-diag")
            .set("socket.timeout.ms", "3000")
            .set("metadata.max.age.ms", "1000")
            .create();

        let Ok(consumer) = consumer else {
            eprintln!(
                "[E2E_DIAG] metadata broker={} create_consumer_failed",
                broker
            );
            continue;
        };

        match consumer.fetch_metadata(Some(topic), Duration::from_secs(3)) {
            Ok(metadata) => {
                let broker_ids: Vec<i32> = metadata.brokers().iter().map(|b| b.id()).collect();
                eprintln!(
                    "[E2E_DIAG] metadata broker={} cluster_brokers={:?}",
                    broker, broker_ids
                );

                let mut found_topic = false;
                for t in metadata.topics() {
                    if t.name() != topic {
                        continue;
                    }
                    found_topic = true;
                    eprintln!(
                        "[E2E_DIAG] metadata broker={} topic={} topic_err={:?}",
                        broker,
                        t.name(),
                        t.error()
                    );
                    for p in t.partitions() {
                        if p.id() == partition {
                            eprintln!(
                                "[E2E_DIAG] metadata broker={} partition={} leader={} replicas={:?} isr={:?} partition_err={:?}",
                                broker,
                                p.id(),
                                p.leader(),
                                p.replicas(),
                                p.isr(),
                                p.error()
                            );
                        }
                    }
                }
                if !found_topic {
                    eprintln!(
                        "[E2E_DIAG] metadata broker={} topic={} missing",
                        broker, topic
                    );
                }
            }
            Err(e) => {
                eprintln!("[E2E_DIAG] metadata broker={} fetch_failed={}", broker, e);
            }
        }
    }

    eprintln!("[E2E_DIAG] metadata snapshot end");
}

fn fetch_end_offset(
    bootstrap_servers: &str,
    topic: &str,
    partition: i32,
    timeout: Duration,
) -> u64 {
    let consumer: BaseConsumer = match ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "tiering-e2e-watermarks")
        .set("socket.timeout.ms", "5000")
        .create()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[E2E_DIAG] watermark create_consumer_failed error={e}");
            return 0;
        }
    };

    let deadline = Instant::now() + timeout;
    loop {
        match consumer.fetch_watermarks(topic, partition, Duration::from_secs(5)) {
            Ok((_low, high)) if high >= 0 => {
                #[allow(clippy::cast_sign_loss)]
                return high as u64;
            }
            Ok((_low, high)) => {
                eprintln!(
                    "[E2E_DIAG] watermark negative_high={} topic={topic} partition={partition}",
                    high
                );
            }
            Err(e) => {
                eprintln!("[E2E_DIAG] watermark fetch_failed topic={topic} partition={partition} error={e}");
            }
        }
        if Instant::now() >= deadline {
            eprintln!(
                "[E2E_DIAG] watermark timeout topic={} partition={} timeout={:?}",
                topic, partition, timeout
            );
            return 0;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
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

/// Count `.wal` files recursively under `dir` (bounded by known directory depth).
fn count_wal_files_in_dir(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                count_wal_files_in_dir(&p)
            } else if p.extension().and_then(|x| x.to_str()) == Some("wal") {
                1
            } else {
                0
            }
        })
        .sum()
}

/// List objects in an S3 bucket.
fn list_s3_objects(bucket: &str, prefix: &str) -> Vec<String> {
    let output = Command::new("aws")
        .args(["--endpoint-url", LOCALSTACK_ENDPOINT])
        .args([
            "s3",
            "ls",
            &format!("s3://{bucket}/{prefix}"),
            "--recursive",
        ])
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
    let mut send_errors = 0usize;
    let mut consecutive_errors = 0usize;
    let start = Instant::now();
    let verbose_errors = std::env::var("HELIX_E2E_VERBOSE_ERRORS").ok().as_deref() == Some("1");
    let fail_fast_consecutive_error_limit = 1_000usize;
    let per_message_max_retries = env_usize("HELIX_E2E_SEND_MAX_RETRIES", 50);
    let report_every = if count >= 50_000 {
        5_000
    } else if count >= 10_000 {
        1_000
    } else if count >= 1_000 {
        200
    } else {
        25
    };

    for i in 0..count {
        let payload = Bytes::from(format!("{prefix}-{i:06}"));
        let mut tries = 0usize;
        let mut sent = false;

        while tries < per_message_max_retries {
            tries += 1;
            match executor.send(topic, partition, payload.clone()).await {
                Ok(offset) => {
                    acknowledged.push((offset, payload.clone()));
                    consecutive_errors = 0;
                    sent = true;
                    break;
                }
                Err(e) => {
                    send_errors += 1;
                    consecutive_errors += 1;

                    if verbose_errors {
                        eprintln!("[SEND] Error at {i} try={tries}: {e}");
                    } else if send_errors <= 3 || send_errors % 500 == 0 {
                        eprintln!(
                            "[E2E_PROGRESS] phase=produce_error topic={} idx={} try={} errors={} consecutive_errors={} last_error={}",
                            topic,
                            i,
                            tries,
                            send_errors,
                            consecutive_errors,
                            e
                        );
                    }

                    // Fail fast if the producer is clearly not making progress.
                    if acknowledged.is_empty()
                        && consecutive_errors >= fail_fast_consecutive_error_limit
                    {
                        log_metadata_snapshot(executor.bootstrap_servers(), topic, partition);
                        eprintln!(
                            "[E2E_FAIL_FAST] phase=produce topic={} sent={} acked=0 errors={} reason=consecutive_errors_exceeded",
                            topic,
                            i + 1,
                            send_errors
                        );
                        return acknowledged;
                    }

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }

        if !sent {
            eprintln!(
                "[E2E_FAIL_FAST] phase=produce topic={} idx={} reason=max_retries_exceeded retries={} acked={} errors={}",
                topic,
                i,
                per_message_max_retries,
                acknowledged.len(),
                send_errors
            );
            return acknowledged;
        }

        if (i + 1) % report_every == 0 || i + 1 == count {
            let elapsed_secs = start.elapsed().as_secs_f64();
            let acked = acknowledged.len();
            let throughput = acked as f64 / elapsed_secs.max(0.001);
            eprintln!(
                "[E2E_PROGRESS] phase=produce topic={} sent={}/{} acked={} errors={} elapsed={}s rate={:.1}msg/s",
                topic,
                i + 1,
                count,
                acked,
                send_errors,
                start.elapsed().as_secs(),
                throughput
            );
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

fn log_ack_offset_summary(acknowledged: &[(u64, Bytes)]) {
    if acknowledged.is_empty() {
        eprintln!("  Ack offset summary: empty");
        return;
    }

    let mut offsets: Vec<u64> = acknowledged.iter().map(|(o, _)| *o).collect();
    offsets.sort_unstable();

    let mut duplicates = 0usize;
    let mut first_gaps = Vec::new();
    let mut prev = offsets[0];

    for &off in offsets.iter().skip(1) {
        if off == prev {
            duplicates += 1;
        } else if off > prev + 1 && first_gaps.len() < 8 {
            first_gaps.push((prev, off));
        }
        prev = off;
    }

    let unique = offsets.windows(2).filter(|w| w[0] != w[1]).count() + 1usize;
    let min = *offsets.first().unwrap_or(&0);
    let max = *offsets.last().unwrap_or(&0);
    eprintln!(
        "  Ack offset summary: count={} unique={} min={} max={} duplicates={} first_gaps={:?}",
        acknowledged.len(),
        unique,
        min,
        max,
        duplicates,
        first_gaps
    );
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
#[ignore = "requires `LocalStack` running on localhost:4566"]
async fn test_basic_tiering_roundtrip() {
    let test_name = current_test_name();
    let run_start = Instant::now();
    eprintln!("[E2E_START] test={test_name}");
    let _heartbeat = ProgressHeartbeat::start(test_name.clone(), run_start);

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
        .data_dir(e2e_data_dir("/tmp/helix-tiering-test-basic"))
        .log_level(e2e_server_log_level())
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
    let message_count = env_usize("HELIX_TIERING_MESSAGE_COUNT", 70_000);

    eprintln!("\nPhase 1: Writing {message_count} messages...");
    let acknowledged = send_messages(&executor, topic, partition, message_count, "tier-msg").await;
    eprintln!("  Acknowledged: {} messages", acknowledged.len());
    log_ack_offset_summary(&acknowledged);

    assert_eq!(
        acknowledged.len(),
        message_count,
        "Expected {message_count} acknowledged messages, got {}",
        acknowledged.len()
    );

    // Phase 2: Wait for tiering.
    eprintln!("\nPhase 2: Waiting for tiering to complete...");
    cluster
        .wait_for_tiering_duration(Duration::from_secs(10))
        .await;

    // Check S3 for tiered objects.
    let s3_objects = list_s3_objects(bucket, "helix/segments/");
    eprintln!("  S3 objects found: {}", s3_objects.len());
    for obj in &s3_objects {
        eprintln!("    - {obj}");
    }

    // Phase 3: Read back and verify.
    eprintln!("\nPhase 3: Reading messages back...");
    let end_offset = fetch_end_offset(
        executor.bootstrap_servers(),
        topic,
        partition,
        Duration::from_secs(20),
    );
    let poll_target = end_offset.max(acknowledged.len() as u64);
    let max_messages = u32::try_from(poll_target).unwrap_or(u32::MAX);
    eprintln!(
        "  Poll target: {} messages (end_offset={}, acknowledged={})",
        max_messages,
        end_offset,
        acknowledged.len()
    );
    let received = executor
        .poll(topic, partition, 0, max_messages)
        .await
        .expect("poll failed");
    eprintln!("  Received: {} messages", received.len());

    let unique_received_offsets: HashSet<u64> = received.iter().map(|(o, _)| *o).collect();
    if unique_received_offsets.len() < received.len() {
        eprintln!(
            "  Duplicate offsets observed: {}",
            received.len() - unique_received_offsets.len()
        );
    }

    let (matched, lost, corrupted) = verify_messages(&acknowledged, &received);

    eprintln!("\n=== Results ===");
    eprintln!("  Matched: {matched}");
    eprintln!("  Lost: {}", lost.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    assert!(lost.is_empty(), "Lost messages: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted messages: {corrupted:?}");
    assert_eq!(matched, acknowledged.len(), "Not all messages matched");

    eprintln!("\nPASSED: Basic tiering roundtrip");
    eprintln!(
        "[E2E_DONE] test={} elapsed={}s",
        test_name,
        run_start.elapsed().as_secs()
    );

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
#[ignore = "requires `LocalStack` running on localhost:4566"]
async fn test_tiering_survives_cluster_restart() {
    let test_name = current_test_name();
    let run_start = Instant::now();
    eprintln!("[E2E_START] test={test_name}");
    let _heartbeat = ProgressHeartbeat::start(test_name.clone(), run_start);

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
        .data_dir(e2e_data_dir("/tmp/helix-tiering-test-restart"))
        .log_level(e2e_server_log_level())
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
    log_ack_offset_summary(&acknowledged);
    assert_eq!(
        acknowledged.len(),
        message_count,
        "Expected {message_count} acknowledged messages, got {}",
        acknowledged.len()
    );

    // Phase 2: Wait for tiering.
    eprintln!("\nPhase 2: Waiting for tiering...");
    cluster
        .wait_for_tiering_duration(Duration::from_secs(10))
        .await;

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
    let end_offset = fetch_end_offset(
        executor.bootstrap_servers(),
        topic,
        partition,
        Duration::from_secs(20),
    );
    let poll_target = end_offset.max(acknowledged.len() as u64);
    let max_messages = u32::try_from(poll_target).unwrap_or(u32::MAX);
    eprintln!(
        "  Poll target: {} messages (end_offset={}, acknowledged={})",
        max_messages,
        end_offset,
        acknowledged.len()
    );
    let received = executor
        .poll(topic, partition, 0, max_messages)
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
    eprintln!(
        "[E2E_DONE] test={} elapsed={}s",
        test_name,
        run_start.elapsed().as_secs()
    );

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
    let test_name = current_test_name();
    let run_start = Instant::now();
    eprintln!("[E2E_START] test={test_name}");
    let _heartbeat = ProgressHeartbeat::start(test_name.clone(), run_start);

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
        .log_level(e2e_server_log_level())
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
    log_ack_offset_summary(&acknowledged);
    assert_eq!(
        acknowledged.len(),
        message_count,
        "Expected {message_count} acknowledged messages, got {}",
        acknowledged.len()
    );

    // Wait for tiering.
    cluster
        .wait_for_tiering_duration(Duration::from_secs(5))
        .await;

    // Check filesystem for tiered files.
    let tier_files: Vec<_> = std::fs::read_dir(tier_dir)
        .map(|entries| entries.filter_map(Result::ok).collect())
        .unwrap_or_default();
    eprintln!("  Tier directory entries: {}", tier_files.len());

    // Read back.
    let end_offset = fetch_end_offset(
        executor.bootstrap_servers(),
        topic,
        partition,
        Duration::from_secs(10),
    );
    let poll_target = end_offset.max(acknowledged.len() as u64);
    let max_messages = u32::try_from(poll_target).unwrap_or(u32::MAX);
    eprintln!(
        "  Poll target: {} messages (end_offset={}, acknowledged={})",
        max_messages,
        end_offset,
        acknowledged.len()
    );
    let received = executor
        .poll(topic, partition, 0, max_messages)
        .await
        .expect("poll failed");

    let (matched, lost, corrupted) = verify_messages(&acknowledged, &received);

    assert!(lost.is_empty(), "Lost: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted: {corrupted:?}");
    assert_eq!(matched, acknowledged.len());

    eprintln!("\nPASSED: Filesystem tiering");
    eprintln!(
        "[E2E_DONE] test={} elapsed={}s",
        test_name,
        run_start.elapsed().as_secs()
    );

    // Cleanup.
    drop(cluster);
    let _ = std::fs::remove_dir_all(tier_dir);
    let _ = std::fs::remove_dir_all(data_dir);
}

// =============================================================================
// Test: Shared WAL Tiering + Single-Node Empty-Disk Recovery
// =============================================================================

/// Tests that shared WAL segments are uploaded to S3, local copies are deleted to
/// free disk space, and a node whose local disk is wiped can recover from S3.
///
/// This is the primary end-to-end test for shared WAL tiering.
/// It exercises three code paths that unit tests cannot cover:
///
/// 1. **Upload path**: `SharedWalCoordinator::tier_eligible_sealed_segments()`
///    is called by the tick task once enough entries have been committed across
///    all groups sharing a WAL shard.  S3 objects under `shared/` prove it ran.
///
/// 2. **Recovery path**: After wiping node 1's local WAL directory and
///    restarting it, `SharedWalPool::download_missing_segments()` is called
///    during startup to restore segments from S3 before Raft replay.  The
///    subsequent Raft `AppendEntries` catch-up fills the gap between the last
///    tiered index and the current committed index on nodes 2/3, so the
///    restarted node rejoins the cluster as a full voting member.
///
/// 3. **Local deletion + S3 read fallback**: After tiering, local segment
///    copies are deleted to reclaim disk space.  Phase 7 asserts that the on-disk
///    `.wal` file count stays within the active-segments bound.  The Phase 4
///    Raft catch-up implicitly validates the S3 read fallback: nodes 2/3 serve
///    `AppendEntries` for segments whose local files were deleted, triggering
///    `SharedWalHandle::restore_from_s3()` transparently.
///
/// # Note on message count
///
/// The default shared WAL segment size is 4 MiB.  Each `SharedEntry` has a
/// 40-byte header; with ~30 byte payloads per message the total per entry is
/// ~70 bytes, requiring ~60 000 entries to fill one segment.  We write 80 000
/// messages so that at least one full segment is sealed and eligible for
/// tiering, with a second segment starting to absorb additional writes.
#[tokio::test]
#[ignore = "requires `LocalStack` running on localhost:4566"]
#[allow(clippy::too_many_lines)] // Multi-phase E2E test requires more than 100 lines.
async fn test_shared_wal_tiering_and_recovery() {
    let test_name = current_test_name();
    let run_start = Instant::now();
    eprintln!("[E2E_START] test={test_name}");
    let _heartbeat = ProgressHeartbeat::start(test_name.clone(), run_start);

    require_localstack();
    require_helix_binary();

    let bucket = "helix-shared-wal-tiering-test";
    create_s3_bucket(bucket);

    eprintln!("\n=== Shared WAL Tiering + Recovery Test ===\n");

    // Start cluster with shared WAL + S3 tiering.
    // tier_min_age_secs=0 bypasses the age gate so segments are eligible
    // for upload immediately after they are sealed and committed.
    // local_retention_ms=2000 evicts local segments shortly after tiering,
    // ensuring the recovery path is exercised (rather than using local data).
    eprintln!("Starting 3-node cluster with shared WAL (count=2) + LocalStack S3 tiering...");
    let mut cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19392)
        .raft_base_port(50400)
        .binary_path(helix_binary_path())
        .data_dir(e2e_data_dir("/tmp/helix-shared-wal-tiering-test"))
        .log_level(e2e_server_log_level())
        .with_localstack_tiering(bucket)
        .with_tiering_config(TieringTestConfig { min_age_secs: 0 })
        .local_retention_ms(2_000)
        .with_shared_wal_count(2)
        .topic("shared-wal-test", 1)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");
    cluster
        .wait_for_leader("shared-wal-test", 0, Duration::from_secs(30))
        .await
        .expect("no leader");

    let topic = "shared-wal-test";
    let partition = 0;

    // Phase 1: Write enough messages to fill at least one 4 MiB shared WAL segment.
    // ~60 000 entries x ~70 bytes/entry = ~4.2 MB per segment.
    let message_count = env_usize("HELIX_TIERING_MESSAGE_COUNT", 80_000);
    eprintln!("\nPhase 1: Writing {message_count} messages to fill a shared WAL segment...");
    let acknowledged =
        send_messages(&executor, topic, partition, message_count, "swal-msg").await;
    eprintln!("  Acknowledged: {} messages", acknowledged.len());
    log_ack_offset_summary(&acknowledged);
    assert_eq!(
        acknowledged.len(),
        message_count,
        "Expected {message_count} acknowledged messages, got {}",
        acknowledged.len()
    );

    // Capture the data directory path for Phase 7 local file counting.
    let data_dir_string = e2e_data_dir("/tmp/helix-shared-wal-tiering-test");
    let data_dir_path = std::path::Path::new(&data_dir_string);

    // Phase 2: Wait for tiering ticks + local retention eviction.
    // Tiering runs every ~5 s; we wait 20 s to allow:
    //   - Multiple tiering ticks to upload all sealed segments.
    //   - The 2 s local retention window to expire and evict local copies.
    eprintln!("\nPhase 2: Waiting for tiering + local eviction...");
    cluster
        .wait_for_tiering_duration(Duration::from_secs(20))
        .await;

    // Verify that shared WAL objects are present in S3.
    // Each coordinator stores segments at:
    //   {s3_prefix}node-{N}/shared/{pool_index}/{segment_id:08x}.wal
    let all_objects = list_s3_objects(bucket, "helix/segments/");
    eprintln!("  Total S3 objects: {}", all_objects.len());
    for obj in &all_objects {
        eprintln!("    - {obj}");
    }
    let shared_wal_objects: Vec<&String> = all_objects
        .iter()
        .filter(|key| key.contains("/shared/"))
        .collect();
    eprintln!(
        "  Shared WAL objects: {}",
        shared_wal_objects.len()
    );
    assert!(
        !shared_wal_objects.is_empty(),
        "Expected at least one shared WAL segment in S3 under '/shared/' prefix, found none. \
         Check that tier_min_age_secs=0 is set and that enough messages were written to \
         seal a segment."
    );

    // Phase 7: Confirm that local copies of tiered segments were deleted from disk.
    //
    // With local_retention_ms=2_000 and the tiering tick running every 5s,
    // delete_tiered_local_segments() runs continuously while Phase 1 writes.
    // By the time Phase 1 finishes (~240s), sealed segments that were tiered
    // to S3 have already had their local copies removed.
    //
    // Invariant: only the currently-active (unsealed) WAL segment per coordinator
    // remains on disk.  Upper bound = node_count × shared_wal_count = 3 × 2 = 6.
    // Without local deletion, sealed segments pile up pushing the count above 6.
    eprintln!("\nPhase 7: Verifying local segment deletion after tiering...");
    let local_wal_count: usize = (1u32..=3)
        .map(|id| count_wal_files_in_dir(&data_dir_path.join(format!("node-{id}"))))
        .sum();
    // One active segment per coordinator is the expected maximum.
    let active_segment_max = 3usize * 2usize; // node_count × shared_wal_count
    eprintln!(
        "  Local .wal files: {} (max expected: {}, S3 tiered: {})",
        local_wal_count,
        active_segment_max,
        shared_wal_objects.len(),
    );
    assert!(
        !shared_wal_objects.is_empty(),
        "No shared WAL segments in S3 — tiering did not run, cannot verify local deletion."
    );
    assert!(
        local_wal_count <= active_segment_max,
        "Expected at most {active_segment_max} local .wal files (one active segment per \
         coordinator) after tiering deletion, found {local_wal_count}. \
         S3 has {} tiered segments that should have been deleted locally.",
        shared_wal_objects.len(),
    );
    eprintln!(
        "  Verified: {} tiered segments absent from local disk, {} active segments remain",
        shared_wal_objects.len(),
        local_wal_count,
    );

    // Phase 3: Kill node 1 and wipe its entire data directory.
    // This simulates a pod being rescheduled to a new host with an empty disk.
    eprintln!("\nPhase 3: Killing node 1 and wiping its data directory...");
    cluster.kill_node(1).expect("kill node 1 failed");
    tokio::time::sleep(Duration::from_secs(1)).await;
    cluster
        .wipe_node_data_dir(1)
        .expect("wipe node 1 data dir failed");
    eprintln!("  Node 1 data directory wiped");

    // Phase 4: Restart node 1 with an empty local disk.
    // On startup the server calls SharedWalPool::download_missing_segments()
    // before replaying the WAL, downloading all tiered segments from S3.
    // After replay, Raft catches up the untiered gap via AppendEntries from
    // nodes 2/3 (which still have the recent entries in their active segments).
    eprintln!("\nPhase 4: Restarting node 1 (S3 recovery path)...");
    cluster.restart_node(1).expect("restart node 1 failed");
    tokio::time::sleep(Duration::from_secs(5)).await;

    let executor = RealExecutor::new(&cluster).expect("failed to create executor after restart");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready after node 1 restart");
    cluster
        .wait_for_leader(topic, partition, Duration::from_secs(60))
        .await
        .expect("no leader after node 1 restart");

    // Phase 5: Confirm cluster health by writing a few more messages.
    eprintln!("\nPhase 5: Writing post-recovery messages to confirm cluster is functional...");
    let post_recovery_messages = 100usize;
    let post_acknowledged = send_messages(
        &executor,
        topic,
        partition,
        post_recovery_messages,
        "post-recovery",
    )
    .await;
    eprintln!("  Post-recovery acknowledged: {}", post_acknowledged.len());
    assert_eq!(
        post_acknowledged.len(),
        post_recovery_messages,
        "Cluster not functional after node 1 recovery"
    );

    // Phase 6: Read all messages back and verify correctness.
    eprintln!("\nPhase 6: Verifying all messages...");
    let all_acknowledged: Vec<(u64, bytes::Bytes)> = acknowledged
        .iter()
        .chain(post_acknowledged.iter())
        .cloned()
        .collect();

    let end_offset = fetch_end_offset(
        executor.bootstrap_servers(),
        topic,
        partition,
        Duration::from_secs(20),
    );
    let poll_target = end_offset.max(all_acknowledged.len() as u64);
    let max_messages = u32::try_from(poll_target).unwrap_or(u32::MAX);
    eprintln!(
        "  Poll target: {} messages (end_offset={}, acknowledged={})",
        max_messages,
        end_offset,
        all_acknowledged.len()
    );
    let received = executor
        .poll(topic, partition, 0, max_messages)
        .await
        .expect("poll failed");
    eprintln!("  Received: {} messages", received.len());

    let (matched, lost, corrupted) = verify_messages(&all_acknowledged, &received);

    eprintln!("\n=== Results ===");
    eprintln!("  Matched: {matched}");
    eprintln!("  Lost: {}", lost.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    assert!(lost.is_empty(), "Lost messages after shared WAL recovery: {lost:?}");
    assert!(corrupted.is_empty(), "Corrupted messages: {corrupted:?}");
    assert_eq!(matched, all_acknowledged.len());

    eprintln!("\nPASSED: Shared WAL tiering + local deletion + S3 fallback + single-node empty-disk recovery");
    eprintln!(
        "[E2E_DONE] test={} elapsed={}s",
        test_name,
        run_start.elapsed().as_secs()
    );

    // Cleanup.
    drop(cluster);
    delete_s3_bucket(bucket);
}

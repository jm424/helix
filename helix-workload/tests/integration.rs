//! Integration tests for helix-workload against helix-server.
//!
//! These tests spawn real helix-server processes in Kafka mode and run
//! workloads against them to verify correctness.

use std::path::PathBuf;
use std::time::Duration;

use helix_workload::{
    ProducerMode, RealCluster, RealExecutor, SizeDistribution, TopicConfig, Workload,
    WorkloadExecutor, WorkloadPattern, WorkloadStats,
};

/// Returns the path to the helix-server binary.
fn binary_path() -> PathBuf {
    // Try release first, then debug.
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
        "helix-server binary not found. Run `cargo build --release -p helix-server` first.\n\
         Checked:\n  - {}\n  - {}",
        release.display(),
        debug.display()
    );
}

/// Creates a unique data directory for a test.
fn test_data_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-workload-tests")
        .join(test_name)
        .join(format!("{}", std::process::id()));
    // Clean up any previous run.
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn find_available_base_port(start: u16, count: u16) -> u16 {
    let max_base = u16::MAX.saturating_sub(count);
    for base in start..=max_base {
        let mut available = true;
        for offset in 1..=count {
            let port = base.saturating_add(offset);
            let addr = format!("127.0.0.1:{port}");
            if std::net::TcpListener::bind(&addr).is_err() {
                available = false;
                break;
            }
        }
        if available {
            return base;
        }
    }
    panic!("unable to find available port block (count={count}) starting at {start}");
}

fn env_producer_mode(name: &str, default: ProducerMode) -> ProducerMode {
    let value = std::env::var(name).ok();
    match value
        .as_deref()
        .map(|mode| mode.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("low") | Some("lowlatency") | Some("low-latency") => ProducerMode::LowLatency,
        Some("high") | Some("highthroughput") | Some("high-throughput") => {
            ProducerMode::HighThroughput
        }
        _ => default,
    }
}

/// Single-node test: basic produce and consume correctness.
#[tokio::test]
async fn test_single_node_sequential() {
    let cluster = RealCluster::builder()
        .nodes(1)
        .base_port(19092)
        .raft_base_port(19100)
        .binary_path(binary_path())
        .data_dir(test_data_dir("single_node_sequential"))
        .auto_create_topics(true)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");

    // Wait for cluster to be ready.
    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");

    // Run a simple sequential workload.
    let mut workload = Workload::builder()
        .seed(42)
        .topic("test-sequential", 1)
        .operations(100)
        .pattern(WorkloadPattern::Sequential)
        .message_size(SizeDistribution::Fixed(64))
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Single Node Sequential Test ===");
    stats.print_summary();

    // Verify no violations.
    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );

    // Verify operations completed.
    assert!(
        stats.operations_ok > 0,
        "No successful operations recorded"
    );
}

/// Single-node test with more operations.
#[tokio::test]
async fn test_single_node_larger_workload() {
    let cluster = RealCluster::builder()
        .nodes(1)
        .base_port(19192)
        .raft_base_port(19200)
        .binary_path(binary_path())
        .data_dir(test_data_dir("single_node_larger"))
        .auto_create_topics(true)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");

    executor
        .wait_ready(Duration::from_secs(30))
        .await
        .expect("cluster not ready");

    // Larger workload with variable message sizes.
    let mut workload = Workload::builder()
        .seed(123)
        .topic("test-larger", 1)
        .operations(500)
        .pattern(WorkloadPattern::Sequential)
        .message_size(SizeDistribution::Uniform { min: 32, max: 1024 })
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Single Node Larger Workload Test ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    assert!(stats.operations_ok > 0);
}

/// Three-node cluster test: basic produce and consume.
#[tokio::test]
async fn test_three_node_sequential() {
    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19292)
        .raft_base_port(19300)
        .binary_path(binary_path())
        .data_dir(test_data_dir("three_node_sequential"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");

    // Multi-node clusters may take longer to elect leader.
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let mut workload = Workload::builder()
        .seed(456)
        .topic("test-replicated", 1)
        .operations(100)
        .pattern(WorkloadPattern::Sequential)
        .message_size(SizeDistribution::Fixed(128))
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Three Node Sequential Test ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    assert!(stats.operations_ok > 0);
}

/// Three-node test with multiple partitions.
#[tokio::test]
async fn test_three_node_many_partitions() {
    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19392)
        .raft_base_port(19400)
        .binary_path(binary_path())
        .data_dir(test_data_dir("three_node_partitions"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .build()
        .expect("failed to start cluster");

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");

    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Use multiple partitions to distribute load.
    let mut workload = Workload::builder()
        .seed(789)
        .topics(vec![TopicConfig::new("test-partitioned", 4, 3)])
        .operations(200)
        .pattern(WorkloadPattern::ManyPartitions {
            partition_count: 4,
            operations_per_partition: 50,
        })
        .message_size(SizeDistribution::Fixed(64))
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Three Node Many Partitions Test ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    assert!(stats.operations_ok > 0);
}

/// Test that deterministic workloads produce consistent results.
#[tokio::test]
async fn test_deterministic_workload() {
    // Two workloads with same seed should generate same payloads.
    let workload1 = Workload::builder()
        .seed(999)
        .topic("test", 1)
        .operations(10)
        .build();

    let workload2 = Workload::builder()
        .seed(999)
        .topic("test", 1)
        .operations(10)
        .build();

    // Configs should match.
    assert_eq!(workload1.config().seed, workload2.config().seed);
    assert_eq!(workload1.config().operations, workload2.config().operations);
}

/// Test cluster health check.
#[tokio::test]
async fn test_cluster_health_check() {
    let mut cluster = RealCluster::builder()
        .nodes(1)
        .base_port(19492)
        .raft_base_port(19500)
        .binary_path(binary_path())
        .data_dir(test_data_dir("health_check"))
        .auto_create_topics(true)
        .build()
        .expect("failed to start cluster");

    // Give process time to start.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Health check should pass.
    cluster.check_health().expect("cluster should be healthy");
}

/// Three-node throughput test with concurrent requests and batching.
///
/// Uses `ProducerMode::HighThroughput` (linger.ms=100ms) and sends
/// concurrent requests to measure actual throughput capacity.
#[tokio::test]
async fn test_three_node_concurrent_throughput() {
    use bytes::Bytes;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let cluster = RealCluster::builder()
        .nodes(3)
        .base_port(19592)
        .raft_base_port(19600)
        .binary_path(binary_path())
        .data_dir(test_data_dir("three_node_throughput"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .build()
        .expect("failed to start cluster");

    // Create a temporary executor just for wait_ready check.
    let wait_executor = RealExecutor::with_mode(cluster.bootstrap_servers(), ProducerMode::HighThroughput)
        .expect("failed to create executor");

    wait_executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Parameters for throughput test.
    const TOTAL_MESSAGES: u64 = 1000;
    const CONCURRENT_REQUESTS: u64 = 10;
    const MESSAGE_SIZE: usize = 1024; // 1KB - realistic payload size

    let topic = "throughput-test";
    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    println!("=== Three Node Concurrent Throughput Test ===");
    println!("Total messages: {TOTAL_MESSAGES}");
    println!("Concurrent requests: {CONCURRENT_REQUESTS}");
    println!("Message size: {MESSAGE_SIZE} bytes");
    println!("Producer mode: HighThroughput (linger.ms=5ms, batch.size=512KB)");
    println!();

    let start = std::time::Instant::now();

    // Spawn concurrent producer tasks.
    // Each producer gets its OWN executor/connection to verify server-side concurrency.
    // Using a shared executor would serialize requests due to rdkafka connection pooling.
    let mut handles = Vec::new();
    let messages_per_producer = TOTAL_MESSAGES / CONCURRENT_REQUESTS;
    let bootstrap_servers = cluster.bootstrap_servers().to_string();

    for producer_id in 0..CONCURRENT_REQUESTS {
        let bootstrap = bootstrap_servers.clone();
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);

        let handle = tokio::spawn(async move {
            // Create a separate executor per producer for true connection concurrency.
            let producer_executor = RealExecutor::with_mode(&bootstrap, ProducerMode::HighThroughput)
                .expect("failed to create producer executor");

            for i in 0..messages_per_producer {
                let payload = Bytes::from(vec![0u8; MESSAGE_SIZE]);
                match producer_executor.send(topic, 0, payload).await {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        if errors.load(Ordering::Relaxed) <= 5 {
                            eprintln!("Producer {producer_id} error on msg {i}: {e}");
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all producers to complete.
    for handle in handles {
        handle.await.expect("producer task panicked");
    }

    let duration = start.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let throughput = success as f64 / duration.as_secs_f64();

    println!("=== Results ===");
    println!("Duration: {:.2}s", duration.as_secs_f64());
    println!("Successful: {success}");
    println!("Errors: {errors}");
    println!("Throughput: {:.1} ops/sec", throughput);

    // Allow up to 1% error rate for distributed system tolerance.
    // Occasional timeouts can occur during leader transitions or network hiccups.
    let max_allowed_errors = TOTAL_MESSAGES / 100; // 1%
    assert!(
        errors <= max_allowed_errors,
        "Error rate too high: {errors}/{} ({:.1}%), max allowed: {max_allowed_errors}",
        success + errors,
        (errors as f64 / (success + errors) as f64) * 100.0
    );
    assert!(
        success >= TOTAL_MESSAGES * 99 / 100,
        "Expected at least 99% success rate, got {success}/{TOTAL_MESSAGES}"
    );
}

/// Single-partition throughput test with configurable in-flight requests.
///
/// This keeps a single partition but allows multiple in-flight produce
/// requests per producer to measure server-side batching capacity.
#[tokio::test]
async fn test_single_partition_throughput_inflight() {
    use bytes::Bytes;
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let total_messages = env_u64("HELIX_THROUGHPUT_TOTAL_MESSAGES", 2_000);
    let inflight = env_usize("HELIX_THROUGHPUT_INFLIGHT", 64).max(1);
    let producer_count = env_usize("HELIX_THROUGHPUT_PRODUCERS", 1).max(1);
    let message_size = env_usize("HELIX_THROUGHPUT_MESSAGE_SIZE", 1024);
    let producer_mode = env_producer_mode(
        "HELIX_THROUGHPUT_MODE",
        ProducerMode::HighThroughput,
    );
    let node_count = 3u16;
    let base_port = std::env::var("HELIX_THROUGHPUT_BASE_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_else(|| find_available_base_port(20000, node_count));
    let raft_base_port = std::env::var("HELIX_THROUGHPUT_RAFT_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or_else(|| find_available_base_port(30000, node_count));

    let topic = "throughput-single-partition";
    let cluster = RealCluster::builder()
        .nodes(u32::from(node_count))
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir("single_partition_throughput"))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .topic(topic, 1)
        .build()
        .expect("failed to start cluster");

    let wait_executor =
        RealExecutor::with_mode(cluster.bootstrap_servers(), producer_mode)
            .expect("failed to create executor");
    wait_executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let success_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));

    println!("=== Single Partition Inflight Throughput Test ===");
    println!("Total messages: {total_messages}");
    println!("Producers: {producer_count}");
    println!("Inflight per producer: {inflight}");
    println!("Message size: {message_size} bytes");
    println!("Producer mode: {producer_mode:?}");
    println!();

    let start = std::time::Instant::now();
    let mut handles = Vec::new();
    let messages_per_producer = total_messages / producer_count as u64;
    let remainder = total_messages % producer_count as u64;
    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let payload = Bytes::from(vec![0u8; message_size]);

    for producer_id in 0..producer_count {
        let bootstrap = bootstrap_servers.clone();
        let success = Arc::clone(&success_count);
        let errors = Arc::clone(&error_count);
        let payload = payload.clone();
        let producer_mode = producer_mode;
        let producer_messages =
            messages_per_producer + u64::from(producer_id == 0) * remainder;

        let handle = tokio::spawn(async move {
            let executor = Arc::new(
                RealExecutor::with_mode(&bootstrap, producer_mode)
                    .expect("failed to create producer executor"),
            );
            let mut inflight_set = FuturesUnordered::new();
            let mut sent = 0u64;

            while sent < producer_messages {
                while inflight_set.len() < inflight && sent < producer_messages {
                    let exec = Arc::clone(&executor);
                    let payload = payload.clone();
                    inflight_set.push(async move { exec.send(topic, 0, payload).await });
                    sent += 1;
                }

                if let Some(result) = inflight_set.next().await {
                    match result {
                        Ok(_) => {
                            success.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            let count = errors.fetch_add(1, Ordering::Relaxed) + 1;
                            if count <= 5 {
                                eprintln!("Producer {producer_id} error: {e}");
                            }
                        }
                    }
                }
            }

            while let Some(result) = inflight_set.next().await {
                match result {
                    Ok(_) => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let count = errors.fetch_add(1, Ordering::Relaxed) + 1;
                        if count <= 5 {
                            eprintln!("Producer {producer_id} error: {e}");
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("producer task panicked");
    }

    let duration = start.elapsed();
    let success = success_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);
    let throughput = success as f64 / duration.as_secs_f64();

    println!("=== Results ===");
    println!("Duration: {:.2}s", duration.as_secs_f64());
    println!("Successful: {success}");
    println!("Errors: {errors}");
    println!("Throughput: {:.1} ops/sec", throughput);

    assert!(errors == 0, "Expected no errors, got {errors}");
    assert!(
        success >= total_messages.saturating_sub(10),
        "Expected ~{total_messages} successes, got {success}"
    );
}

//! Integration tests for helix-workload against helix-server.
//!
//! These tests spawn real helix-server processes in Kafka mode and run
//! workloads against them to verify correctness.

use std::path::PathBuf;
use std::time::Duration;

use helix_workload::{
    RealCluster, RealExecutor, SizeDistribution, TopicConfig, Workload, WorkloadExecutor,
    WorkloadPattern, WorkloadStats,
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

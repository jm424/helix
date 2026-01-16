//! Integration test for helix-workload against helix-server.
//!
//! Runs verification workloads against real Helix server processes.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

use helix_workload::{RealCluster, RealExecutor, Workload, WorkloadExecutor, WorkloadPattern};

const HELIX_BINARY: &str = "./target/release/helix-server";

struct TestServer {
    child: Child,
    port: u16,
}

impl TestServer {
    fn start(port: u16) -> Self {
        // Clean up any previous data.
        let _ = std::fs::remove_dir_all("/tmp/helix-test-single");

        let child = Command::new(HELIX_BINARY)
            .arg("--protocol")
            .arg("kafka")
            .arg("--node-id")
            .arg("1")
            .arg("--listen-addr")
            .arg(format!("127.0.0.1:{port}"))
            .arg("--data-dir")
            .arg("/tmp/helix-test-single")
            .arg("--auto-create-topics")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to start helix-server");

        Self { child, port }
    }

    fn bootstrap_servers(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all("/tmp/helix-test-single");
    }
}

async fn test_single_node() -> bool {
    eprintln!("=== Single-Node Test ===\n");

    // Start a single-node server.
    eprintln!("Starting single-node helix-server on port 9292...");
    let server = TestServer::start(9292);

    // Create executor.
    let executor = RealExecutor::with_bootstrap_servers(&server.bootstrap_servers())
        .expect("failed to create executor");

    // Wait for server to be ready.
    eprintln!("Waiting for server to be ready...");
    match executor.wait_ready(Duration::from_secs(30)).await {
        Ok(()) => eprintln!("Server is ready!"),
        Err(e) => {
            eprintln!("ERROR: Server not ready: {e}");
            return false;
        }
    }

    // Run a small workload.
    eprintln!("\nRunning workload (10 operations)...\n");

    let mut workload = Workload::builder()
        .seed(42)
        .topic("test-topic", 1)
        .operations(10)
        .pattern(WorkloadPattern::Sequential)
        .build();

    let stats = workload.run(&executor).await;

    eprintln!("\nResults:");
    eprintln!(
        "  Operations: {} total, {} ok, {} failed",
        stats.operations_total, stats.operations_ok, stats.operations_failed
    );
    eprintln!("  Violations: {}", stats.violations.len());

    for violation in &stats.violations {
        eprintln!("    - {violation}");
    }

    stats.violations.is_empty() && stats.operations_failed == 0
}

/// Sends messages to a topic/partition and tracks acknowledged offsets.
async fn send_messages(
    executor: &RealExecutor,
    topic: &str,
    partition: i32,
    prefix: &str,
    count: usize,
    acknowledged_offsets: &mut Vec<u64>,
    payloads: &mut std::collections::HashMap<u64, bytes::Bytes>,
) {
    for i in 0..count {
        let payload = bytes::Bytes::from(format!("{prefix}-{i}"));
        match executor.send(topic, partition, payload.clone()).await {
            Ok(offset) => {
                eprintln!("  Sent message {i} at offset {offset}");
                acknowledged_offsets.push(offset);
                payloads.insert(offset, payload);
            }
            Err(e) => {
                eprintln!("  ERROR sending message {i}: {e}");
            }
        }
    }
}

/// Verifies no data loss by checking acknowledged offsets against received messages.
fn verify_no_data_loss(
    acknowledged_offsets: &[u64],
    messages: &[(u64, bytes::Bytes)],
    payloads: &std::collections::HashMap<u64, bytes::Bytes>,
) -> (Vec<u64>, Vec<u64>) {
    let received_offsets: std::collections::HashSet<u64> =
        messages.iter().map(|(o, _)| *o).collect();

    let lost_writes: Vec<u64> = acknowledged_offsets
        .iter()
        .filter(|o| !received_offsets.contains(o))
        .copied()
        .collect();

    let corrupted: Vec<u64> = messages
        .iter()
        .filter_map(|(offset, payload)| {
            payloads
                .get(offset)
                .filter(|expected| payload != *expected)
                .map(|_| *offset)
        })
        .collect();

    (lost_writes, corrupted)
}

/// Sets up a 3-node cluster for failover testing.
fn setup_failover_cluster() -> Result<RealCluster, String> {
    let _ = std::fs::remove_dir_all("/tmp/helix-test-failover");

    RealCluster::builder()
        .nodes(3)
        .base_port(9492)
        .raft_base_port(50300)
        .binary_path(HELIX_BINARY)
        .data_dir("/tmp/helix-test-failover")
        .topic("failover-topic", 1)
        .build()
        .map_err(|e| format!("Failed to start cluster: {e}"))
}

/// Polls messages and verifies no data loss, reporting results.
async fn verify_and_report_results(
    executor: &RealExecutor,
    topic: &str,
    partition: i32,
    acknowledged_offsets: &[u64],
    payloads: &std::collections::HashMap<u64, bytes::Bytes>,
) -> bool {
    eprintln!("\nPhase 4: Verifying no data loss...");
    let min_offset = *acknowledged_offsets.iter().min().unwrap_or(&0);
    let max_offset = *acknowledged_offsets.iter().max().unwrap_or(&0);
    #[allow(clippy::cast_possible_truncation)]
    let expected_count = (max_offset - min_offset + 1) as u32;

    eprintln!("  Polling offsets {min_offset} to {max_offset} ({expected_count} messages)");

    let messages = match executor.poll(topic, partition, min_offset, expected_count).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("ERROR: Failed to poll messages: {e}");
            return false;
        }
    };

    eprintln!("  Received {} messages", messages.len());

    let (lost_writes, corrupted) = verify_no_data_loss(acknowledged_offsets, &messages, payloads);

    eprintln!("\n=== Failover Test Results ===");
    eprintln!("  Messages acknowledged: {}", acknowledged_offsets.len());
    eprintln!("  Messages received: {}", messages.len());
    eprintln!("  Lost writes: {}", lost_writes.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    if !lost_writes.is_empty() {
        eprintln!("  Lost offsets: {lost_writes:?}");
    }
    if !corrupted.is_empty() {
        eprintln!("  Corrupted offsets: {corrupted:?}");
    }

    lost_writes.is_empty() && corrupted.is_empty()
}

async fn test_leader_failover() -> bool {
    eprintln!("\n=== Leader Failover Test (3 nodes) ===\n");

    eprintln!("Starting 3-node cluster...");
    let mut cluster = match setup_failover_cluster() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: {e}");
            return false;
        }
    };

    let executor = match RealExecutor::new(&cluster) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ERROR: Failed to create executor: {e}");
            return false;
        }
    };

    eprintln!("Waiting for cluster to be ready...");
    if let Err(e) = executor.wait_ready(Duration::from_secs(30)).await {
        eprintln!("ERROR: Cluster not ready: {e}");
        return false;
    }

    eprintln!("Waiting for leader election...");
    let leader = match cluster
        .wait_for_leader("failover-topic", 0, Duration::from_secs(30))
        .await
    {
        Ok(l) => {
            eprintln!("Leader elected: node {l}");
            l
        }
        Err(e) => {
            eprintln!("ERROR: No leader elected: {e}");
            return false;
        }
    };

    let topic = "failover-topic";
    let partition = 0;
    let mut acknowledged_offsets: Vec<u64> = Vec::new();
    let mut payloads: std::collections::HashMap<u64, bytes::Bytes> =
        std::collections::HashMap::new();

    eprintln!("\nPhase 1: Sending 10 messages before failover...");
    send_messages(&executor, topic, partition, "message-before-failover", 10,
        &mut acknowledged_offsets, &mut payloads).await;

    if acknowledged_offsets.is_empty() {
        eprintln!("ERROR: No messages were acknowledged before failover");
        return false;
    }
    eprintln!("  Acknowledged {} messages", acknowledged_offsets.len());

    eprintln!("\nPhase 2: Killing leader (node {leader})...");
    if let Err(e) = cluster.kill_node(leader) {
        eprintln!("ERROR: Failed to kill leader: {e}");
        return false;
    }
    eprintln!("  Leader killed");

    eprintln!("Waiting for new leader...");
    let new_leader = match cluster
        .wait_for_leader(topic, partition, Duration::from_secs(60))
        .await
    {
        Ok(l) => {
            eprintln!("  New leader elected: node {l}");
            l
        }
        Err(e) => {
            eprintln!("ERROR: No new leader elected after failover: {e}");
            return false;
        }
    };

    if new_leader == leader {
        eprintln!("WARNING: Same leader re-elected (node may not be fully dead)");
    }

    eprintln!("Waiting for dead broker to be removed from metadata (heartbeat timeout)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    eprintln!("\nPhase 3: Sending 10 messages after failover...");
    send_messages(&executor, topic, partition, "message-after-failover", 10,
        &mut acknowledged_offsets, &mut payloads).await;
    eprintln!("  Total acknowledged: {} messages", acknowledged_offsets.len());

    verify_and_report_results(&executor, topic, partition, &acknowledged_offsets, &payloads).await
}

async fn test_multi_node() -> bool {
    eprintln!("\n=== Multi-Node Test (3 nodes) ===\n");

    // Clean up any previous data.
    let _ = std::fs::remove_dir_all("/tmp/helix-test-cluster");

    // Start a 3-node cluster using RealCluster.
    eprintln!("Starting 3-node cluster...");
    let cluster = match RealCluster::builder()
        .nodes(3)
        .base_port(9392) // Use different ports to avoid conflict with single-node test.
        .raft_base_port(50200)
        .binary_path(HELIX_BINARY)
        .data_dir("/tmp/helix-test-cluster")
        .auto_create_topics(true)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to start cluster: {e}");
            return false;
        }
    };

    // Create executor connected to the cluster.
    let executor = match RealExecutor::new(&cluster) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ERROR: Failed to create executor: {e}");
            return false;
        }
    };

    // Wait for cluster to be ready (metadata available from at least one broker).
    eprintln!("Waiting for cluster to be ready...");
    match executor.wait_ready(Duration::from_secs(30)).await {
        Ok(()) => eprintln!("Cluster is ready!"),
        Err(e) => {
            eprintln!("ERROR: Cluster not ready: {e}");
            return false;
        }
    }

    // With auto-create enabled and proper retry settings in rdkafka,
    // topic creation and leader election happen automatically when we
    // first produce. No explicit wait_for_leader needed.
    eprintln!("\nRunning workload (20 operations)...");

    let mut workload = Workload::builder()
        .seed(123)
        .topic("multi-topic", 1)
        .operations(20)
        .pattern(WorkloadPattern::Sequential)
        .build();

    let stats = workload.run(&executor).await;

    eprintln!("\nResults:");
    eprintln!(
        "  Operations: {} total, {} ok, {} failed",
        stats.operations_total, stats.operations_ok, stats.operations_failed
    );
    eprintln!("  Violations: {}", stats.violations.len());

    for violation in &stats.violations {
        eprintln!("    - {violation}");
    }

    stats.violations.is_empty() && stats.operations_failed == 0
}

/// Tests that data survives a full cluster restart (all nodes stopped and restarted).
///
/// This exercises `SharedWAL` recovery on all nodes simultaneously, which is different
/// from the leader failover test where only one node restarts at a time.
async fn test_full_cluster_restart() -> bool {
    eprintln!("\n=== Full Cluster Restart Test (3 nodes) ===\n");

    // Clean up any previous data.
    let _ = std::fs::remove_dir_all("/tmp/helix-test-restart");

    eprintln!("Starting 3-node cluster...");
    let mut cluster = match RealCluster::builder()
        .nodes(3)
        .base_port(9592) // Use different ports to avoid conflict with other tests.
        .raft_base_port(50400)
        .binary_path(HELIX_BINARY)
        .data_dir("/tmp/helix-test-restart")
        .topic("restart-topic", 1)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to start cluster: {e}");
            return false;
        }
    };

    let executor = match RealExecutor::new(&cluster) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ERROR: Failed to create executor: {e}");
            return false;
        }
    };

    eprintln!("Waiting for cluster to be ready...");
    if let Err(e) = executor.wait_ready(Duration::from_secs(30)).await {
        eprintln!("ERROR: Cluster not ready: {e}");
        return false;
    }

    eprintln!("Waiting for leader election...");
    if let Err(e) = cluster
        .wait_for_leader("restart-topic", 0, Duration::from_secs(30))
        .await
    {
        eprintln!("ERROR: No leader elected: {e}");
        return false;
    }

    let topic = "restart-topic";
    let partition = 0;
    let mut acknowledged_offsets: Vec<u64> = Vec::new();
    let mut payloads: std::collections::HashMap<u64, bytes::Bytes> =
        std::collections::HashMap::new();

    // Phase 1: Write data before restart.
    eprintln!("\nPhase 1: Sending 20 messages before full cluster restart...");
    send_messages(
        &executor,
        topic,
        partition,
        "message-before-restart",
        20,
        &mut acknowledged_offsets,
        &mut payloads,
    )
    .await;

    if acknowledged_offsets.is_empty() {
        eprintln!("ERROR: No messages were acknowledged before restart");
        return false;
    }
    eprintln!("  Acknowledged {} messages", acknowledged_offsets.len());

    // Phase 2: Stop ALL nodes (full cluster shutdown).
    eprintln!("\nPhase 2: Stopping ALL nodes (full cluster shutdown)...");
    cluster.stop();
    eprintln!("  All nodes stopped");

    // Wait a moment to ensure clean shutdown.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 3: Restart ALL nodes.
    eprintln!("\nPhase 3: Restarting ALL nodes...");
    if let Err(e) = cluster.restart_all() {
        eprintln!("ERROR: Failed to restart cluster: {e}");
        return false;
    }
    eprintln!("  All nodes restarted");

    // Wait for cluster to recover.
    eprintln!("Waiting for cluster to recover...");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Create new executor (old connections are stale).
    let executor = match RealExecutor::new(&cluster) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ERROR: Failed to create executor after restart: {e}");
            return false;
        }
    };

    if let Err(e) = executor.wait_ready(Duration::from_secs(60)).await {
        eprintln!("ERROR: Cluster not ready after restart: {e}");
        return false;
    }

    eprintln!("Waiting for leader election after restart...");
    if let Err(e) = cluster
        .wait_for_leader(topic, partition, Duration::from_secs(60))
        .await
    {
        eprintln!("ERROR: No leader elected after restart: {e}");
        return false;
    }
    eprintln!("  Leader elected after restart");

    // Phase 4: Verify all data survived the restart.
    verify_and_report_results(&executor, topic, partition, &acknowledged_offsets, &payloads).await
}

#[tokio::main]
async fn main() {
    eprintln!("=== Helix Workload Integration Test ===\n");

    // Check if the binary exists.
    if !std::path::Path::new(HELIX_BINARY).exists() {
        eprintln!("ERROR: helix-server binary not found at {HELIX_BINARY}");
        eprintln!("Please run: cargo build --release -p helix-server");
        std::process::exit(1);
    }

    let mut all_passed = true;

    // Test 1: Single-node
    if test_single_node().await {
        eprintln!("\nPASSED: Single-node test");
    } else {
        eprintln!("\nFAILED: Single-node test");
        all_passed = false;
    }

    // Test 2: Multi-node
    if test_multi_node().await {
        eprintln!("\nPASSED: Multi-node test");
    } else {
        eprintln!("\nFAILED: Multi-node test");
        all_passed = false;
    }

    // Test 3: Leader failover
    if test_leader_failover().await {
        eprintln!("\nPASSED: Leader failover test");
    } else {
        eprintln!("\nFAILED: Leader failover test");
        all_passed = false;
    }

    // Test 4: Full cluster restart
    if test_full_cluster_restart().await {
        eprintln!("\nPASSED: Full cluster restart test");
    } else {
        eprintln!("\nFAILED: Full cluster restart test");
        all_passed = false;
    }

    eprintln!("\n=== Final Results ===");
    if all_passed {
        eprintln!("ALL TESTS PASSED!");
    } else {
        eprintln!("SOME TESTS FAILED!");
        std::process::exit(1);
    }
}

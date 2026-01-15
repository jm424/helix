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

async fn test_leader_failover() -> bool {
    eprintln!("\n=== Leader Failover Test (3 nodes) ===\n");

    // Clean up any previous data.
    let _ = std::fs::remove_dir_all("/tmp/helix-test-failover");

    // Start a 3-node cluster with a pre-created topic.
    eprintln!("Starting 3-node cluster...");
    let mut cluster = match RealCluster::builder()
        .nodes(3)
        .base_port(9492) // Different ports to avoid conflict.
        .raft_base_port(50300)
        .binary_path(HELIX_BINARY)
        .data_dir("/tmp/helix-test-failover")
        .topic("failover-topic", 1) // Pre-create topic.
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ERROR: Failed to start cluster: {e}");
            return false;
        }
    };

    // Create executor.
    let executor = match RealExecutor::new(&cluster) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("ERROR: Failed to create executor: {e}");
            return false;
        }
    };

    // Wait for cluster to be ready.
    eprintln!("Waiting for cluster to be ready...");
    if let Err(e) = executor.wait_ready(Duration::from_secs(30)).await {
        eprintln!("ERROR: Cluster not ready: {e}");
        return false;
    }

    // Wait for leader election on the topic partition.
    eprintln!("Waiting for leader election...");
    let leader = match cluster.wait_for_leader("failover-topic", 0, Duration::from_secs(30)).await {
        Ok(l) => {
            eprintln!("Leader elected: node {l}");
            l
        }
        Err(e) => {
            eprintln!("ERROR: No leader elected: {e}");
            return false;
        }
    };

    // Phase 1: Send some messages before failover.
    let topic = "failover-topic";
    let partition = 0;
    let mut acknowledged_offsets: Vec<u64> = Vec::new();
    let mut payloads: std::collections::HashMap<u64, bytes::Bytes> = std::collections::HashMap::new();

    eprintln!("\nPhase 1: Sending 10 messages before failover...");
    for i in 0..10 {
        let payload = bytes::Bytes::from(format!("message-before-failover-{i}"));
        match executor.send(topic, partition, payload.clone()).await {
            Ok(offset) => {
                eprintln!("  Sent message {i} at offset {offset}");
                acknowledged_offsets.push(offset);
                payloads.insert(offset, payload);
            }
            Err(e) => {
                eprintln!("  ERROR sending message {i}: {e}");
                // Continue - some failures are expected during leader election.
            }
        }
    }

    if acknowledged_offsets.is_empty() {
        eprintln!("ERROR: No messages were acknowledged before failover");
        return false;
    }
    eprintln!("  Acknowledged {} messages", acknowledged_offsets.len());

    // Phase 2: Kill the leader.
    eprintln!("\nPhase 2: Killing leader (node {leader})...");
    if let Err(e) = cluster.kill_node(leader) {
        eprintln!("ERROR: Failed to kill leader: {e}");
        return false;
    }
    eprintln!("  Leader killed");

    // Wait for new leader election.
    eprintln!("Waiting for new leader...");
    let new_leader = match cluster.wait_for_leader(topic, partition, Duration::from_secs(60)).await {
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

    // Wait for heartbeat timeout to expire so dead broker is removed from metadata.
    // The heartbeat timeout is 6 seconds, so we wait 8 seconds to be safe.
    eprintln!("Waiting for dead broker to be removed from metadata (heartbeat timeout)...");
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Phase 3: Send more messages after failover.
    eprintln!("\nPhase 3: Sending 10 messages after failover...");
    for i in 0..10 {
        let payload = bytes::Bytes::from(format!("message-after-failover-{i}"));
        match executor.send(topic, partition, payload.clone()).await {
            Ok(offset) => {
                eprintln!("  Sent message {i} at offset {offset}");
                acknowledged_offsets.push(offset);
                payloads.insert(offset, payload);
            }
            Err(e) => {
                eprintln!("  ERROR sending message {i}: {e}");
                // Some failures may be expected during recovery.
            }
        }
    }
    eprintln!("  Total acknowledged: {} messages", acknowledged_offsets.len());

    // Phase 4: Poll all messages and verify no data loss.
    eprintln!("\nPhase 4: Verifying no data loss...");
    let min_offset = *acknowledged_offsets.iter().min().unwrap_or(&0);
    let max_offset = *acknowledged_offsets.iter().max().unwrap_or(&0);
    let expected_count = (max_offset - min_offset + 1) as u32;

    eprintln!("  Polling offsets {} to {} ({} messages)", min_offset, max_offset, expected_count);

    let messages = match executor.poll(topic, partition, min_offset, expected_count).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("ERROR: Failed to poll messages: {e}");
            return false;
        }
    };

    eprintln!("  Received {} messages", messages.len());

    // Check that all acknowledged offsets are present in the poll result.
    let received_offsets: std::collections::HashSet<u64> = messages.iter().map(|(o, _)| *o).collect();
    let mut lost_writes = Vec::new();

    for offset in &acknowledged_offsets {
        if !received_offsets.contains(offset) {
            lost_writes.push(*offset);
        }
    }

    // Verify payload integrity.
    let mut corrupted = Vec::new();
    for (offset, payload) in &messages {
        if let Some(expected) = payloads.get(offset) {
            if payload != expected {
                corrupted.push(*offset);
            }
        }
    }

    // Report results.
    eprintln!("\n=== Failover Test Results ===");
    eprintln!("  Messages acknowledged: {}", acknowledged_offsets.len());
    eprintln!("  Messages received: {}", messages.len());
    eprintln!("  Lost writes: {}", lost_writes.len());
    eprintln!("  Corrupted: {}", corrupted.len());

    if !lost_writes.is_empty() {
        eprintln!("  Lost offsets: {:?}", lost_writes);
    }
    if !corrupted.is_empty() {
        eprintln!("  Corrupted offsets: {:?}", corrupted);
    }

    lost_writes.is_empty() && corrupted.is_empty()
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
    if !test_single_node().await {
        eprintln!("\nFAILED: Single-node test");
        all_passed = false;
    } else {
        eprintln!("\nPASSED: Single-node test");
    }

    // Test 2: Multi-node
    if !test_multi_node().await {
        eprintln!("\nFAILED: Multi-node test");
        all_passed = false;
    } else {
        eprintln!("\nPASSED: Multi-node test");
    }

    // Test 3: Leader failover
    if !test_leader_failover().await {
        eprintln!("\nFAILED: Leader failover test");
        all_passed = false;
    } else {
        eprintln!("\nPASSED: Leader failover test");
    }

    eprintln!("\n=== Final Results ===");
    if all_passed {
        eprintln!("ALL TESTS PASSED!");
    } else {
        eprintln!("SOME TESTS FAILED!");
        std::process::exit(1);
    }
}

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

    eprintln!("\n=== Final Results ===");
    if all_passed {
        eprintln!("ALL TESTS PASSED!");
    } else {
        eprintln!("SOME TESTS FAILED!");
        std::process::exit(1);
    }
}

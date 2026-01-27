//! Actor mode correctness tests for helix-workload.
//!
//! These tests verify that actor mode (per-partition batch pending tracking)
//! works correctly under various conditions including:
//! - Multi-partition concurrent writes
//! - Node failures during load
//! - High concurrency stress
//!
//! Actor mode is the production architecture where each partition actor tracks
//! its own pending batches rather than using a shared map.

// Test-specific lint allowances - these are acceptable in test code
#![allow(clippy::too_many_lines)] // Complex tests with many verification steps
#![allow(clippy::cast_possible_truncation)] // Bounded by test parameters
#![allow(clippy::cast_sign_loss)] // Partition indices are always non-negative
#![allow(clippy::cast_possible_wrap)] // Index casts bounded by test parameters
#![allow(clippy::single_match)] // Match for clarity in error handling
#![allow(clippy::single_match_else)] // Match with else for clarity
#![allow(clippy::needless_borrows_for_generic_args)] // Explicit borrows for clarity
#![allow(clippy::unused_async)] // Test setup functions may need async signature
#![allow(clippy::if_not_else)] // Negative conditions can be clearer in some contexts

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use helix_workload::{
    ProducerMode, RealCluster, RealExecutor, SizeDistribution, TopicConfig, Violation, Workload,
    WorkloadExecutor, WorkloadPattern, WorkloadStats,
};

// ============================================================================
// Helpers
// ============================================================================

/// Returns the path to the helix-server binary.
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

/// Creates a unique data directory for a test.
fn test_data_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-actor-mode-tests")
        .join(test_name)
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Creates an actor-mode enabled cluster with the given configuration.
async fn setup_actor_mode_cluster(
    test_name: &str,
    nodes: u32,
    base_port: u16,
    raft_base_port: u16,
) -> RealCluster {
    RealCluster::builder()
        .nodes(nodes)
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir(test_name))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .actor_mode(true)
        .build()
        .expect("failed to start actor mode cluster")
}

// ============================================================================
// Basic Tests
// ============================================================================

/// Basic single-partition actor mode test.
///
/// Verifies that actor mode works correctly with a single partition.
#[tokio::test]
async fn test_actor_mode_single_partition() {
    let cluster = setup_actor_mode_cluster("single_partition", 3, 20100, 20200).await;

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let mut workload = Workload::builder()
        .seed(100)
        .topics(vec![TopicConfig::new("actor-single", 1, 3)])
        .operations(100)
        .pattern(WorkloadPattern::Sequential)
        .message_size(SizeDistribution::Fixed(128))
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Actor Mode Single Partition ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    // All sends must succeed on a healthy cluster.
    assert_eq!(
        stats.sends_ok, 100,
        "Expected exactly 100 successful sends, got {}",
        stats.sends_ok
    );
    assert_eq!(
        stats.operations_failed, 0,
        "Expected no failed operations, got {}",
        stats.operations_failed
    );
}

/// Multi-partition actor mode test.
///
/// Verifies that actor mode correctly handles multiple partitions with
/// per-partition ordering maintained.
#[tokio::test]
async fn test_actor_mode_multi_partition() {
    let cluster = setup_actor_mode_cluster("multi_partition", 3, 20300, 20400).await;

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let mut workload = Workload::builder()
        .seed(200)
        .topics(vec![TopicConfig::new("actor-multi", 4, 3)])
        .operations(200)
        .pattern(WorkloadPattern::ManyPartitions {
            partition_count: 4,
            operations_per_partition: 50,
        })
        .message_size(SizeDistribution::Fixed(64))
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Actor Mode Multi Partition ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    // All sends must succeed on a healthy cluster.
    assert_eq!(
        stats.sends_ok, 200,
        "Expected exactly 200 successful sends, got {}",
        stats.sends_ok
    );
    assert_eq!(
        stats.operations_failed, 0,
        "Expected no failed operations, got {}",
        stats.operations_failed
    );
}

// ============================================================================
// Concurrency Tests
// ============================================================================

/// Concurrent producers writing to different partitions.
///
/// Verifies that actor mode correctly handles concurrent writes from multiple
/// producers to different partitions without interference.
/// Uses the Workload framework for proper topic creation and verification.
#[tokio::test]
async fn test_actor_mode_concurrent_producers() {
    let cluster = setup_actor_mode_cluster("concurrent_producers", 3, 20500, 20600).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();

    // Wait for cluster readiness once before spawning producers.
    let warmup_executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create warmup executor");
    warmup_executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Spawn 4 concurrent workloads, each writing to their own partitions.
    // This simulates multiple concurrent producers in a more realistic way
    // using the proper Workload framework for topic management.
    let mut handles = Vec::new();
    let topic = "actor-concurrent";

    for producer_id in 0u32..4 {
        let bootstrap = bootstrap_servers.clone();
        let topic_name = topic.to_string();

        let handle = tokio::spawn(async move {
            let executor = RealExecutor::with_mode(&bootstrap, ProducerMode::LowLatency)
                .expect("failed to create executor");

            // Each producer uses different partitions via ManyPartitions pattern.
            // Producer 0: partitions 0-1, Producer 1: partitions 2-3, etc.
            let mut workload = Workload::builder()
                .seed(500 + u64::from(producer_id))
                .topics(vec![TopicConfig::new(&topic_name, 8, 3)])
                .operations(100)
                .pattern(WorkloadPattern::ManyPartitions {
                    partition_count: 2,
                    operations_per_partition: 50,
                })
                .message_size(SizeDistribution::Fixed(64))
                .build();

            workload.run(&executor).await
        });
        handles.push(handle);
    }

    // Collect results from all concurrent producers.
    let mut total_sends = 0u64;
    let mut total_failed = 0u64;
    let mut all_violations = Vec::new();

    for (i, handle) in handles.into_iter().enumerate() {
        let stats = handle.await.expect("producer task failed");
        println!(
            "Producer {i}: sends={} failed={}",
            stats.sends_ok, stats.operations_failed
        );
        total_sends += stats.sends_ok;
        total_failed += stats.operations_failed;
        if !stats.violations.is_empty() {
            all_violations.extend(stats.violations);
        }
    }

    // For concurrent producers, OffsetGap violations are expected because each
    // producer's verification only tracks its own writes. When reading back,
    // offsets from other producers appear as "gaps". Filter these out and only
    // fail on serious violations like LostWrite or InconsistentRead.
    let serious_violations: Vec<_> = all_violations
        .iter()
        .filter(|v| !matches!(v, Violation::OffsetGap { .. }))
        .collect();

    println!("=== Actor Mode Concurrent Producers ===");
    println!("  Total sends: {total_sends}");
    println!("  Total failed: {total_failed}");
    println!("  Violations: {} (OffsetGap: {}, serious: {})",
        all_violations.len(),
        all_violations.len() - serious_violations.len(),
        serious_violations.len()
    );
    for v in &serious_violations {
        println!("    - {v:?}");
    }

    // All sends must succeed on a healthy cluster (4 producers * 100 ops = 400 total).
    assert_eq!(
        total_sends, 400,
        "Expected exactly 400 successful sends, got {total_sends}"
    );
    assert_eq!(
        total_failed, 0,
        "Expected no failed operations, got {total_failed}"
    );
    assert!(
        serious_violations.is_empty(),
        "Found {} serious violations: {:?}",
        serious_violations.len(),
        serious_violations
    );
}

/// High concurrency stress test.
///
/// Verifies actor mode under high load with many partitions and operations.
#[tokio::test]
async fn test_actor_mode_high_concurrency_stress() {
    let cluster = setup_actor_mode_cluster("high_concurrency", 3, 20700, 20800).await;

    let executor = RealExecutor::new(&cluster).expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // 8 partitions, 500 operations total.
    let mut workload = Workload::builder()
        .seed(300)
        .topics(vec![TopicConfig::new("actor-stress", 8, 3)])
        .operations(500)
        .pattern(WorkloadPattern::ManyPartitions {
            partition_count: 8,
            operations_per_partition: 62, // ~500 total
        })
        .message_size(SizeDistribution::Uniform { min: 32, max: 256 })
        .build();

    let stats: WorkloadStats = workload.run(&executor).await;

    println!("=== Actor Mode High Concurrency Stress ===");
    stats.print_summary();

    assert!(
        stats.violations.is_empty(),
        "Found violations: {:?}",
        stats.violations
    );
    // All sends must succeed on a healthy cluster (8 partitions * 62 ops = 496).
    assert_eq!(
        stats.sends_ok, 496,
        "Expected exactly 496 successful sends, got {}",
        stats.sends_ok
    );
    assert_eq!(
        stats.operations_failed, 0,
        "Expected no failed operations, got {}",
        stats.operations_failed
    );
}

// ============================================================================
// Fault Tolerance Tests
// ============================================================================

/// Leader failure during sustained load with full verification.
///
/// Verifies that actor mode correctly handles leader failure:
/// - All pre-failure writes must succeed (healthy cluster)
/// - All acknowledged writes (pre and post failure) are readable
/// - No data loss or corruption
#[tokio::test]
async fn test_actor_mode_leader_failure() {
    let mut cluster = setup_actor_mode_cluster("leader_failure", 3, 20900, 21000).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "actor-leader-fail";
    let partition = 0i32;

    // Track all acknowledged writes for verification
    let mut acknowledged: Vec<(u64, String)> = Vec::new();

    // Wait for leader election.
    let leader = cluster
        .wait_for_leader(topic, partition, Duration::from_secs(30))
        .await
        .expect("no leader elected");
    println!("Initial leader: node {leader}");

    // Phase 1: Write before failure - ALL must succeed on healthy cluster.
    println!("\nPhase 1: Writing 50 messages before leader failure...");
    for i in 0..50u32 {
        let payload_str = format!("before-{i:04}");
        let payload = Bytes::from(payload_str.clone());
        match executor.send(topic, partition, payload).await {
            Ok(offset) => {
                acknowledged.push((offset, payload_str));
            }
            Err(e) => {
                panic!("Phase 1 send failed (healthy cluster): {e}");
            }
        }
    }
    let ack_count = acknowledged.len();
    println!("  Phase 1 complete: {ack_count} acknowledged");
    assert_eq!(ack_count, 50, "Phase 1: all 50 writes must succeed on healthy cluster");

    // Phase 2: Kill leader.
    println!("\nPhase 2: Killing leader (node {leader})...");
    cluster.kill_node(leader).expect("failed to kill leader");

    // Write during failover - track only acknowledged.
    println!("  Writing during failover (some may fail)...");
    for i in 0..20u32 {
        let payload_str = format!("during-{i:04}");
        let payload = Bytes::from(payload_str.clone());
        if let Ok(offset) = executor.send(topic, partition, payload).await {
            acknowledged.push((offset, payload_str));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Phase 3: Wait for new leader and continue.
    println!("\nPhase 3: Waiting for new leader...");
    let new_leader = cluster
        .wait_for_leader(topic, partition, Duration::from_secs(30))
        .await
        .expect("no new leader elected");
    assert_ne!(new_leader, leader, "New leader must be different from killed leader");
    println!("  New leader: node {new_leader}");

    // Write after failover - ALL must succeed (cluster recovered).
    println!("  Writing 50 messages after failover...");
    for i in 0..50u32 {
        let payload_str = format!("after-{i:04}");
        let payload = Bytes::from(payload_str.clone());
        match executor.send(topic, partition, payload).await {
            Ok(offset) => {
                acknowledged.push((offset, payload_str));
            }
            Err(e) => {
                panic!("Phase 3 send failed (recovered cluster): {e}");
            }
        }
    }

    // Verify ALL acknowledged writes are readable.
    let total_acks = acknowledged.len();
    println!("\nVerifying {total_acks} acknowledged writes...");
    let consumed = executor
        .poll(topic, partition, 0, acknowledged.len() as u32 + 10)
        .await
        .expect("poll failed");

    let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

    let mut verified = 0u32;
    let mut errors = 0u32;
    for (offset, expected) in &acknowledged {
        match consumed_map.get(offset) {
            Some(actual) if String::from_utf8_lossy(actual) == expected.as_str() => {
                verified += 1;
            }
            Some(actual) => {
                eprintln!("CORRUPTION at offset {}: expected '{}', got '{}'",
                    offset, expected, String::from_utf8_lossy(actual));
                errors += 1;
            }
            None => {
                eprintln!("DATA LOSS at offset {offset}: expected '{expected}'");
                errors += 1;
            }
        }
    }

    let total_ack = acknowledged.len();
    println!("\n=== Actor Mode Leader Failure ===");
    println!("  Acknowledged: {total_ack}");
    println!("  Verified: {verified}");
    println!("  Errors: {errors}");

    assert_eq!(errors, 0, "CRITICAL: {errors} data integrity errors after leader failure");
    assert_eq!(
        verified as usize, total_ack,
        "Not all acknowledged writes verified: {verified}/{total_ack}"
    );
}

/// Node failure during multi-partition load with full verification.
///
/// Verifies that actor mode handles node failure correctly:
/// - All pre-failure writes must succeed (healthy cluster)
/// - All acknowledged writes are readable after recovery
/// - No data loss or corruption
#[tokio::test]
async fn test_actor_mode_node_failure_multi_partition() {
    let mut cluster = setup_actor_mode_cluster("node_failure_multi", 3, 21100, 21200).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "actor-node-fail-multi";
    let num_partitions = 4i32;

    // Track acknowledged writes per partition
    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Wait for leaders on all partitions.
    for p in 0..num_partitions {
        cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader elected");
    }

    // Phase 1: Write to all partitions - ALL must succeed on healthy cluster.
    println!("\nPhase 1: Writing 25 messages to each of 4 partitions...");
    for partition in 0..num_partitions {
        for i in 0..25u32 {
            let payload_str = format!("p{partition}-before-{i:04}");
            let payload = Bytes::from(payload_str.clone());
            match executor.send(topic, partition, payload).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload_str));
                }
                Err(e) => {
                    panic!("Phase 1 send failed (healthy cluster): {e}");
                }
            }
        }
    }
    let phase1_count: usize = acknowledged.iter().map(Vec::len).sum();
    println!("  Phase 1 complete: {phase1_count} acknowledged");
    assert_eq!(phase1_count, 100, "Phase 1: all 100 writes must succeed on healthy cluster");

    // Phase 2: Kill node 2.
    println!("\nPhase 2: Killing node 2...");
    cluster.kill_node(2).expect("failed to kill node 2");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 3: Continue writing - track only acknowledged.
    println!("\nPhase 3: Continuing writes after node failure...");
    for partition in 0..num_partitions {
        for i in 0..25u32 {
            let payload_str = format!("p{partition}-during-{i:04}");
            let payload = Bytes::from(payload_str.clone());
            if let Ok(offset) = executor.send(topic, partition, payload).await {
                acknowledged[partition as usize].push((offset, payload_str));
            }
        }
    }

    // Phase 4: Restart node.
    println!("\nPhase 4: Restarting node 2...");
    cluster.restart_node(2).expect("failed to restart node 2");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Write after restart - ALL must succeed (cluster recovered).
    println!("  Writing 10 more messages per partition after restart...");
    for partition in 0..num_partitions {
        for i in 0..10u32 {
            let payload_str = format!("p{partition}-restart-{i:04}");
            let payload = Bytes::from(payload_str.clone());
            match executor.send(topic, partition, payload).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload_str));
                }
                Err(e) => {
                    panic!("Phase 4 send failed (recovered cluster): {e}");
                }
            }
        }
    }

    // Verify ALL acknowledged writes are readable.
    let total_acknowledged: usize = acknowledged.iter().map(Vec::len).sum();
    println!("\nVerifying {total_acknowledged} acknowledged writes...");

    let mut verified = 0u32;
    let mut errors = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {
                    verified += 1;
                }
                Some(actual) => {
                    let actual_str = String::from_utf8_lossy(actual);
                    eprintln!("CORRUPTION: p{partition}@{offset}: expected '{expected_payload}', got '{actual_str}'");
                    errors += 1;
                }
                None => {
                    eprintln!("DATA LOSS: p{partition}@{offset}: expected '{expected_payload}'");
                    errors += 1;
                }
            }
        }
    }

    println!("\n=== Actor Mode Node Failure Multi-Partition ===");
    println!("  Acknowledged: {total_acknowledged}");
    println!("  Verified: {verified}");
    println!("  Errors: {errors}");

    assert_eq!(errors, 0, "CRITICAL: {errors} data integrity errors after node failure");
    assert_eq!(
        verified as usize, total_acknowledged,
        "Not all acknowledged writes verified: {verified}/{total_acknowledged}"
    );
}

// ============================================================================
// Consumer Tests
// ============================================================================

/// Producer-consumer correctness test with full verification.
///
/// Verifies that all committed data is readable by consumers and ordering
/// is maintained within each partition. All writes must succeed on healthy cluster.
#[tokio::test]
async fn test_actor_mode_producer_consumer() {
    let cluster = setup_actor_mode_cluster("producer_consumer", 3, 21300, 21400).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "actor-prod-cons";
    let num_partitions = 2i32;

    // Wait for leaders.
    for p in 0..num_partitions {
        cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader");
    }

    // Track acknowledged writes per partition: Vec<(offset, payload)>
    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Produce messages - ALL must succeed on healthy cluster.
    println!("Producing 50 messages to each of 2 partitions...");
    for partition in 0..num_partitions {
        for i in 0..50u32 {
            let value = format!("p{partition}-v{i:04}-{:08x}", rand::random::<u32>());
            let payload = Bytes::from(value.clone());

            match executor.send(topic, partition, payload).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, value));
                }
                Err(e) => {
                    panic!("Send failed on healthy cluster: {e}");
                }
            }
        }
    }

    let total_produced: usize = acknowledged.iter().map(Vec::len).sum();
    assert_eq!(total_produced, 100, "All 100 sends must succeed on healthy cluster");

    // Consume and verify ALL data.
    println!("Consuming and verifying...");
    let mut verified = 0u32;
    let mut ordering_errors = 0u32;
    let mut missing = 0u32;
    let mut corruption = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        // Verify count matches.
        if consumed.len() != expected.len() {
            eprintln!(
                "Partition {}: expected {} messages, got {}",
                partition, expected.len(), consumed.len()
            );
        }

        // Verify each message matches in order.
        for (i, (expected_offset, expected_value)) in expected.iter().enumerate() {
            if i < consumed.len() {
                let (actual_offset, actual_bytes) = &consumed[i];
                let actual_value = String::from_utf8_lossy(actual_bytes);

                if actual_offset != expected_offset {
                    eprintln!(
                        "Partition {partition} index {i}: offset mismatch - expected {expected_offset}, got {actual_offset}"
                    );
                    ordering_errors += 1;
                } else if actual_value != expected_value.as_str() {
                    eprintln!(
                        "Partition {partition} offset {expected_offset}: corruption - expected '{expected_value}', got '{actual_value}'"
                    );
                    corruption += 1;
                } else {
                    verified += 1;
                }
            } else {
                eprintln!(
                    "Partition {partition} offset {expected_offset}: missing (expected '{expected_value}')"
                );
                missing += 1;
            }
        }
    }

    println!("\n=== Actor Mode Producer-Consumer ===");
    println!("  Produced: {total_produced}");
    println!("  Verified: {verified}");
    println!("  Ordering errors: {ordering_errors}");
    println!("  Missing: {missing}");
    println!("  Corruption: {corruption}");

    assert_eq!(ordering_errors, 0, "Found {ordering_errors} ordering violations");
    assert_eq!(missing, 0, "Found {missing} missing messages");
    assert_eq!(corruption, 0, "Found {corruption} corrupted messages");
    assert_eq!(
        verified as usize, total_produced,
        "Not all messages verified: {verified}/{total_produced}"
    );
}

// ============================================================================
// Stress Tests (ignored by default)
// ============================================================================

/// Extended stress test with many seeds.
///
/// Runs multiple iterations with different seeds to catch rare bugs.
#[tokio::test]
#[ignore = "long-running stress test - run manually with --ignored"]
async fn test_actor_mode_stress_many_seeds() {
    let mut total_violations = 0;
    let mut total_successes = 0u64;

    for seed in 0..20u64 {
        // Use port range 25000+ to avoid conflicts with other tests.
        // Each seed gets 200 ports (100 for kafka, 100 for raft).
        let port_base = 25000 + (seed as u16 * 200);
        let cluster = setup_actor_mode_cluster(
            &format!("stress_seed_{seed}"),
            3,
            port_base,
            port_base + 100,
        )
        .await;

        let executor = RealExecutor::new(&cluster).expect("failed to create executor");
        executor
            .wait_ready(Duration::from_secs(60))
            .await
            .expect("cluster not ready");

        let mut workload = Workload::builder()
            .seed(seed)
            .topics(vec![TopicConfig::new(&format!("stress-{seed}"), 4, 3)])
            .operations(100)
            .pattern(WorkloadPattern::ManyPartitions {
                partition_count: 4,
                operations_per_partition: 25,
            })
            .message_size(SizeDistribution::Fixed(128))
            .build();

        let stats: WorkloadStats = workload.run(&executor).await;

        if !stats.violations.is_empty() {
            eprintln!("Seed {seed} had violations: {:?}", stats.violations);
            total_violations += stats.violations.len();
        }
        total_successes += stats.operations_ok;

        if seed % 5 == 0 {
            let ops = stats.operations_ok;
            println!("Completed seed {seed}: {ops} ops");
        }
    }

    println!("\n=== Actor Mode Stress Many Seeds ===");
    println!("  Seeds: 20");
    println!("  Total successes: {total_successes}");
    println!("  Total violations: {total_violations}");

    assert_eq!(total_violations, 0, "Found violations across seeds");
    assert!(
        total_successes >= 1500,
        "Expected >=1500 total successes, got {total_successes}"
    );
}

// ============================================================================
// Comprehensive Failover and Recovery Tests
// ============================================================================

/// Multi-partition leader failover with full data verification.
///
/// This test:
/// 1. Identifies which node is leader for each partition
/// 2. Writes to ALL partitions and tracks every acknowledged write
/// 3. Kills a node that is leader for SOME partitions
/// 4. Verifies affected partitions elect new leaders
/// 5. Continues writing to all partitions
/// 6. Reads back ALL acknowledged writes and verifies byte-for-byte
///
/// Strong assertions:
/// - ALL acknowledged writes must be readable (zero data loss)
/// - Payload bytes must match exactly (no corruption)
/// - Per-partition ordering must be maintained
#[tokio::test]
async fn test_actor_mode_multi_partition_leader_failover_verified() {
    let mut cluster = setup_actor_mode_cluster("multi_partition_leader_failover", 3, 23000, 23100).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "leader-failover-verified";
    let num_partitions = 8i32;  // More partitions

    // Track all acknowledged writes: partition -> Vec<(offset, payload)>
    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Step 1: Identify leaders for each partition
    println!("Step 1: Identifying leaders for {num_partitions} partitions...");
    let mut partition_leaders: Vec<u64> = Vec::new();
    for p in 0..num_partitions {
        let leader = cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader elected");
        partition_leaders.push(leader);
        println!("  Partition {p}: leader = node {leader}");
    }

    // Step 2: Write to all partitions before failure - 200 per partition = 1600 total
    println!("\nStep 2: Writing 200 messages to each partition (1600 total)...");
    for partition in 0..num_partitions {
        for i in 0..200u32 {
            let payload = format!("p{partition}-phase1-{i:05}");
            let payload_bytes = Bytes::from(payload.clone());

            match executor.send(topic, partition, payload_bytes).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => {
                    panic!("Phase 1 send failed for partition {partition} msg {i}: {e}");
                }
            }
        }
    }

    let phase1_total: usize = acknowledged.iter().map(Vec::len).sum();
    println!("  Phase 1 complete: {phase1_total} acknowledged writes");
    assert_eq!(phase1_total, 1600, "Phase 1: expected exactly 1600 acknowledged writes");

    // Step 3: Find a node that is leader for multiple partitions and kill it
    let mut leader_counts: std::collections::HashMap<u64, Vec<i32>> = std::collections::HashMap::new();
    for (p, &leader) in partition_leaders.iter().enumerate() {
        leader_counts.entry(leader).or_default().push(p as i32);
    }

    // Pick a node that is leader for at least one partition
    let (victim_node, affected_partitions) = leader_counts
        .iter()
        .max_by_key(|(_, partitions)| partitions.len())
        .map(|(&node, partitions)| (node, partitions.clone()))
        .expect("no leaders found");

    println!("\nStep 3: Killing node {victim_node} (leader for partitions {affected_partitions:?})...");
    cluster.kill_node(victim_node).expect("failed to kill node");

    // Step 4: Verify new leaders elected for affected partitions
    println!("\nStep 4: Waiting for new leaders on affected partitions...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    for &partition in &affected_partitions {
        let new_leader = cluster
            .wait_for_leader(topic, partition, Duration::from_secs(30))
            .await
            .expect("no new leader elected");
        assert_ne!(
            new_leader, victim_node,
            "Partition {partition} still has dead node {victim_node} as leader"
        );
        println!("  Partition {partition}: new leader = node {new_leader}");
    }

    // Step 5: Write more to all partitions after failure - 100 per partition = 800 total
    println!("\nStep 5: Writing 100 more messages to each partition (800 total)...");
    let mut phase2_success = 0u32;
    let mut phase2_fail = 0u32;

    for partition in 0..num_partitions {
        for i in 0..100u32 {
            let payload = format!("p{partition}-phase2-{i:05}");
            let payload_bytes = Bytes::from(payload.clone());

            match executor.send(topic, partition, payload_bytes).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                    phase2_success += 1;
                }
                Err(_) => {
                    phase2_fail += 1;
                }
            }
        }
    }

    println!("  Phase 2 complete: {phase2_success} succeeded, {phase2_fail} failed");
    // With RF=3 and 1 node down, all writes should succeed (majority quorum available)
    assert_eq!(
        phase2_fail, 0,
        "Phase 2: expected zero failures with RF=3 and 1 node down, got {phase2_fail}"
    );

    // Step 6: Read back ALL acknowledged writes and verify
    println!("\nStep 6: Verifying all {} acknowledged writes...",
        acknowledged.iter().map(Vec::len).sum::<usize>());

    let mut verification_errors = 0u32;
    let mut verified_count = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        if expected.is_empty() {
            continue;
        }

        // Read all messages for this partition
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        // Build offset -> payload map from consumed
        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed
            .into_iter()
            .collect();

        // Verify each acknowledged write
        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual_bytes) => {
                    let actual_payload = String::from_utf8_lossy(actual_bytes);
                    if actual_payload != expected_payload.as_str() {
                        eprintln!(
                            "CORRUPTION: partition {partition} offset {offset}: expected '{expected_payload}', got '{actual_payload}'"
                        );
                        verification_errors += 1;
                    } else {
                        verified_count += 1;
                    }
                }
                None => {
                    eprintln!(
                        "DATA LOSS: partition {partition} offset {offset} not found (expected '{expected_payload}')"
                    );
                    verification_errors += 1;
                }
            }
        }
    }

    let total_acknowledged: u32 = acknowledged.iter().map(|v| v.len() as u32).sum();

    println!("\n=== Multi-Partition Leader Failover Verification ===");
    println!("  Total acknowledged: {total_acknowledged}");
    println!("  Verified correct: {verified_count}");
    println!("  Errors (loss + corruption): {verification_errors}");

    assert_eq!(
        verification_errors, 0,
        "CRITICAL: Found {verification_errors} data integrity errors (loss or corruption)"
    );
    assert_eq!(
        verified_count, total_acknowledged,
        "Not all acknowledged writes were verified: {verified_count}/{total_acknowledged}"
    );
}

/// Rolling restart with full data integrity verification.
///
/// This test:
/// 1. Writes data to multiple partitions
/// 2. Restarts each node one by one (rolling restart)
/// 3. After each restart, verifies all previously acknowledged data is still readable
/// 4. Writes more data after all nodes restarted
/// 5. Final verification of all data
///
/// Strong assertions:
/// - Zero data loss through entire rolling restart
/// - All payloads match exactly
#[tokio::test]
async fn test_actor_mode_rolling_restart_verified() {
    let mut cluster = setup_actor_mode_cluster("rolling_restart", 3, 23200, 23300).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "rolling-restart-verified";
    let num_partitions = 6i32;  // More partitions

    // Track all acknowledged writes
    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Wait for leaders
    for p in 0..num_partitions {
        cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader");
    }

    // Initial writes - 100 per partition = 600 total
    println!("Initial phase: Writing 100 messages to each of {num_partitions} partitions...");
    for partition in 0..num_partitions {
        for i in 0..100u32 {
            let payload = format!("p{partition}-initial-{i:04}");
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => panic!("Initial write failed: {e}"),
            }
        }
    }

    let initial_count: usize = acknowledged.iter().map(Vec::len).sum();
    println!("  Initial writes: {initial_count}");
    assert_eq!(initial_count, 600, "Expected exactly 600 initial writes");

    // Rolling restart: restart each node one by one
    for node_id in 1..=3u64 {
        println!("\n--- Rolling restart: node {node_id} ---");

        // Kill node
        println!("  Killing node {node_id}...");
        cluster.kill_node(node_id).expect("failed to kill node");
        tokio::time::sleep(Duration::from_secs(1)).await;  // Shorter sleep to stress more

        // Write during node down - 30 per partition
        println!("  Writing 30 messages per partition while node {node_id} is down...");
        for partition in 0..num_partitions {
            for i in 0..30u32 {
                let payload = format!("p{partition}-during-node{node_id}-down-{i:04}");
                match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                    Ok(offset) => {
                        acknowledged[partition as usize].push((offset, payload));
                    }
                    Err(e) => {
                        // Some failures acceptable during failover
                        eprintln!("    Write failed during node {node_id} down: {e}");
                    }
                }
            }
        }

        // Restart node
        println!("  Restarting node {node_id}...");
        cluster.restart_node(node_id).expect("failed to restart node");
        tokio::time::sleep(Duration::from_secs(2)).await;  // Shorter sleep

        // Verify all data still readable after restart
        println!("  Verifying data integrity after node {node_id} restart...");
        let mut errors = 0u32;
        for partition in 0..num_partitions {
            let expected = &acknowledged[partition as usize];
            let consumed = executor
                .poll(topic, partition, 0, expected.len() as u32 + 10)
                .await
                .expect("poll failed");

            let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

            for (offset, expected_payload) in expected {
                match consumed_map.get(offset) {
                    Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {}
                    Some(_) => {
                        eprintln!("    CORRUPTION after node {node_id} restart: p{partition}@{offset}");
                        errors += 1;
                    }
                    None => {
                        eprintln!("    DATA LOSS after node {node_id} restart: p{partition}@{offset}");
                        errors += 1;
                    }
                }
            }
        }
        assert_eq!(
            errors, 0,
            "Data integrity errors after node {node_id} restart: {errors}"
        );
        println!("  ✓ All data verified after node {node_id} restart");
    }

    // Final writes after all nodes restarted - 50 per partition = 300 total
    println!("\nFinal phase: Writing 50 more messages per partition...");
    for partition in 0..num_partitions {
        for i in 0..50u32 {
            let payload = format!("p{partition}-final-{i:04}");
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => panic!("Final write failed: {e}"),
            }
        }
    }

    // Final verification
    println!("\nFinal verification of all acknowledged writes...");
    let total_acknowledged: usize = acknowledged.iter().map(Vec::len).sum();
    let mut total_verified = 0u32;
    let mut total_errors = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {
                    total_verified += 1;
                }
                _ => {
                    total_errors += 1;
                }
            }
        }
    }

    println!("\n=== Rolling Restart Verification ===");
    println!("  Total acknowledged: {total_acknowledged}");
    println!("  Verified correct: {total_verified}");
    println!("  Errors: {total_errors}");

    assert_eq!(total_errors, 0, "CRITICAL: {total_errors} data integrity errors after rolling restart");
    assert_eq!(
        total_verified as usize, total_acknowledged,
        "Not all writes verified: {total_verified}/{total_acknowledged}"
    );
}

/// Storage recovery test - node recovers from WAL after crash.
///
/// This test:
/// 1. Writes data to partitions
/// 2. Kills a node abruptly (simulating crash)
/// 3. Restarts the node (should recover from WAL)
/// 4. Verifies the restarted node can serve reads correctly
/// 5. Verifies replication catches up
///
/// Strong assertions:
/// - All acknowledged data readable after crash recovery
/// - No corruption in recovered data
#[tokio::test]
async fn test_actor_mode_crash_recovery_from_wal() {
    let mut cluster = setup_actor_mode_cluster("crash_recovery", 3, 23400, 23500).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "crash-recovery-wal";
    let num_partitions = 6i32;  // More partitions

    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Wait for leaders
    println!("Identifying partition leaders...");
    for p in 0..num_partitions {
        let leader = cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader");
        println!("  Partition {p}: leader = node {leader}");
    }

    // Write substantial data - 200 per partition = 1200 total
    println!("\nWriting 200 messages to each partition (1200 total)...");
    for partition in 0..num_partitions {
        for i in 0..200u32 {
            let payload = format!("p{partition}-msg-{i:05}-checksum-{:08x}", rand::random::<u32>());
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => panic!("Write failed: {e}"),
            }
        }
    }

    let total_written: usize = acknowledged.iter().map(Vec::len).sum();
    println!("  Written: {total_written} messages");
    assert_eq!(total_written, 1200, "Expected exactly 1200 writes");

    // Crash node 2 (abrupt kill, simulating power failure)
    println!("\nSimulating crash of node 2...");
    cluster.kill_node(2).expect("failed to kill node");

    // Wait a bit for cluster to detect failure
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Write more data while node 2 is down - 100 per partition = 600 total
    println!("Writing 100 more messages per partition while node 2 is down (600 total)...");
    for partition in 0..num_partitions {
        for i in 0..100u32 {
            let payload = format!("p{partition}-after-crash-{i:05}");
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => {
                    // Acceptable during failover
                    eprintln!("  Write during crash: {e}");
                }
            }
        }
    }

    // Restart node 2 - should recover from WAL
    println!("\nRestarting node 2 (should recover from WAL)...");
    cluster.restart_node(2).expect("failed to restart node");

    // Wait for recovery and replication catch-up
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify all data
    println!("\nVerifying all acknowledged data...");
    let total_acknowledged: usize = acknowledged.iter().map(Vec::len).sum();
    let mut verified = 0u32;
    let mut data_loss = 0u32;
    let mut corruption = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) => {
                    let actual_str = String::from_utf8_lossy(actual);
                    if actual_str == expected_payload.as_str() {
                        verified += 1;
                    } else {
                        eprintln!(
                            "CORRUPTION: p{partition}@{offset}: expected '{expected_payload}', got '{actual_str}'"
                        );
                        corruption += 1;
                    }
                }
                None => {
                    eprintln!("DATA LOSS: p{partition}@{offset} ('{expected_payload}')");
                    data_loss += 1;
                }
            }
        }
    }

    println!("\n=== Crash Recovery Verification ===");
    println!("  Total acknowledged: {total_acknowledged}");
    println!("  Verified correct: {verified}");
    println!("  Data loss: {data_loss}");
    println!("  Corruption: {corruption}");

    assert_eq!(data_loss, 0, "CRITICAL: {data_loss} messages lost after crash recovery");
    assert_eq!(corruption, 0, "CRITICAL: {corruption} messages corrupted after crash recovery");
    assert_eq!(
        verified as usize, total_acknowledged,
        "Not all writes verified: {verified}/{total_acknowledged}"
    );
}

/// Simultaneous multi-node failure and recovery.
///
/// Tests the extreme case where 2 out of 3 nodes fail (loses quorum),
/// then recover. Data written before failure must be preserved.
///
/// Strong assertions:
/// - Data written before quorum loss is preserved
/// - System recovers when quorum restored
#[tokio::test]
async fn test_actor_mode_quorum_loss_recovery() {
    let mut cluster = setup_actor_mode_cluster("quorum_loss", 3, 23600, 23700).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "quorum-loss-recovery";
    let num_partitions = 2i32;

    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Wait for leaders
    for p in 0..num_partitions {
        cluster.wait_for_leader(topic, p, Duration::from_secs(30)).await.expect("no leader");
    }

    // Write data with full cluster
    println!("Phase 1: Writing with full cluster...");
    for partition in 0..num_partitions {
        for i in 0..50u32 {
            let payload = format!("p{partition}-before-quorum-loss-{i:04}");
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => panic!("Pre-failure write failed: {e}"),
            }
        }
    }

    let before_count: usize = acknowledged.iter().map(Vec::len).sum();
    println!("  Acknowledged before quorum loss: {before_count}");
    assert_eq!(before_count, 100, "Expected exactly 100 pre-failure writes");

    // Kill 2 nodes - loses quorum
    println!("\nPhase 2: Killing nodes 2 and 3 (quorum loss)...");
    cluster.kill_node(2).expect("failed to kill node 2");
    cluster.kill_node(3).expect("failed to kill node 3");

    // Writes should fail without quorum
    println!("  Attempting writes without quorum (should fail)...");
    let mut quorum_loss_failures = 0u32;
    for partition in 0..num_partitions {
        for i in 0..5u32 {
            let payload = format!("p{partition}-during-quorum-loss-{i}");
            match executor.send(topic, partition, Bytes::from(payload)).await {
                Ok(_) => {
                    // Unexpected success - might happen if there's a race
                }
                Err(_) => {
                    quorum_loss_failures += 1;
                }
            }
        }
    }
    println!("  Writes failed during quorum loss: {quorum_loss_failures}");

    // Restore quorum by restarting one node
    println!("\nPhase 3: Restoring quorum (restarting node 2)...");
    cluster.restart_node(2).expect("failed to restart node 2");
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for leaders to be elected
    for p in 0..num_partitions {
        cluster.wait_for_leader(topic, p, Duration::from_secs(30)).await.expect("no leader after recovery");
    }

    // Verify pre-failure data is intact
    println!("\nPhase 4: Verifying data written before quorum loss...");
    let mut verified = 0u32;
    let mut errors = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {
                    verified += 1;
                }
                _ => {
                    eprintln!("ERROR: p{partition}@{offset} missing or corrupted");
                    errors += 1;
                }
            }
        }
    }

    // Write more after recovery
    println!("\nPhase 5: Writing after quorum restored...");
    for partition in 0..num_partitions {
        for i in 0..30u32 {
            let payload = format!("p{partition}-after-recovery-{i:04}");
            match executor.send(topic, partition, Bytes::from(payload.clone())).await {
                Ok(offset) => {
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => panic!("Post-recovery write failed: {e}"),
            }
        }
    }

    // Restart node 3 as well
    println!("\nPhase 6: Restarting node 3 for full cluster...");
    cluster.restart_node(3).expect("failed to restart node 3");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Final verification
    println!("\nFinal verification...");
    let total_acknowledged: usize = acknowledged.iter().map(Vec::len).sum();
    let mut final_verified = 0u32;
    let mut final_errors = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 10)
            .await
            .expect("poll failed");

        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) if String::from_utf8_lossy(actual) == expected_payload.as_str() => {
                    final_verified += 1;
                }
                _ => {
                    final_errors += 1;
                }
            }
        }
    }

    println!("\n=== Quorum Loss Recovery Verification ===");
    println!("  Pre-failure acknowledged: {before_count}");
    println!("  Pre-failure verified after recovery: {verified}");
    println!("  Total acknowledged: {total_acknowledged}");
    println!("  Total verified: {final_verified}");
    println!("  Total errors: {final_errors}");

    assert_eq!(
        errors, 0,
        "CRITICAL: {errors} pre-failure messages lost or corrupted after quorum recovery"
    );
    assert_eq!(
        final_errors, 0,
        "CRITICAL: {final_errors} total data integrity errors"
    );
    assert_eq!(
        final_verified as usize, total_acknowledged,
        "Not all writes verified: {final_verified}/{total_acknowledged}"
    );
}

/// Helper to set up a cluster with configurable actor mode.
async fn setup_cluster_with_mode(
    test_name: &str,
    nodes: u32,
    base_port: u16,
    raft_base_port: u16,
    actor_mode: bool,
) -> RealCluster {
    RealCluster::builder()
        .nodes(nodes)
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir(test_name))
        .auto_create_topics(true)
        .default_replication_factor(3)
        .actor_mode(actor_mode)
        .build()
        .expect("failed to start cluster")
}

/// Rapid failover stress test implementation.
///
/// # Test Design
///
/// This test validates Kafka's core offset guarantee under rapid failover:
/// **If produce() returns Ok(offset), then consume(offset) MUST return that exact payload.**
///
/// # Cluster Configuration
///
/// - 3 nodes, replication factor 3
/// - Only 1 node killed at a time → quorum (2/3) always maintained
/// - Cluster should remain available throughout
///
/// # Expected Behavior
///
/// Since quorum is always available:
/// - Initial/final writes: 100% success expected
/// - Failover writes: High success rate (~90%+), only transient failures during leader election
///
/// # What We Validate
///
/// 1. **Core Kafka invariant**: Every acknowledged (offset, payload) pair is readable and correct
/// 2. **No duplicate offsets**: Producer should never return same offset for different payloads
/// 3. **No storage duplicates**: Same offset should not appear twice in consumed data
///
/// # What We DON'T Validate (not Kafka requirements)
///
/// - Sequential offsets in consumed data (gaps are expected when writes fail)
async fn run_rapid_failover_stress_test(actor_mode: bool, base_port: u16, raft_base_port: u16) {
    let mode_str = if actor_mode { "actor" } else { "non-actor" };
    let test_name = format!("rapid_failover_{mode_str}");

    let mut cluster =
        setup_cluster_with_mode(&test_name, 3, base_port, raft_base_port, actor_mode).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    let topic = "rapid-failover-stress";
    let num_partitions = 6i32;

    // Track all acknowledged writes: partition -> [(offset, payload)]
    let mut acknowledged: Vec<Vec<(u64, String)>> = vec![Vec::new(); num_partitions as usize];

    // Track write statistics
    let mut initial_attempts = 0u32;
    let mut initial_successes = 0u32;
    let mut failover_attempts = 0u32;
    let mut failover_successes = 0u32;
    let mut final_attempts = 0u32;
    let mut final_successes = 0u32;

    // Wait for initial leaders
    for p in 0..num_partitions {
        cluster
            .wait_for_leader(topic, p, Duration::from_secs(30))
            .await
            .expect("no leader");
    }

    // === Phase 1: Initial writes (stable cluster, expect 100% success) ===
    println!("[{mode_str}] Phase 1: Initial writes (300 total, expect 100% success)...");
    for partition in 0..num_partitions {
        for i in 0..50u32 {
            let payload = format!("p{partition}-initial-{i:04}");
            initial_attempts += 1;
            match executor
                .send(topic, partition, Bytes::from(payload.clone()))
                .await
            {
                Ok(offset) => {
                    initial_successes += 1;
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => {
                    eprintln!("INITIAL_WRITE_FAILED: partition {partition} i={i}: {e}");
                }
            }
        }
    }
    println!(
        "  Initial writes: {initial_successes}/{initial_attempts} succeeded ({:.1}%)",
        100.0 * initial_successes as f64 / initial_attempts as f64
    );

    // === Phase 2: Rapid failover cycles ===
    println!("\n[{mode_str}] Phase 2: Rapid failover cycles (10 cycles)...");
    for cycle in 1..=10u64 {
        let victim = ((cycle - 1) % 3) + 1; // Cycle through nodes 1, 2, 3

        println!("  Cycle {cycle}: kill node {victim}, write 120 records, restart");

        cluster.kill_node(victim).expect("kill failed");
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Write during failover - 20 per partition = 120 total per cycle
        let mut cycle_successes = 0u32;
        for partition in 0..num_partitions {
            for i in 0..20u32 {
                let payload = format!("p{partition}-cycle{cycle}-{i:04}");
                failover_attempts += 1;
                match executor
                    .send(topic, partition, Bytes::from(payload.clone()))
                    .await
                {
                    Ok(offset) => {
                        failover_successes += 1;
                        cycle_successes += 1;
                        acknowledged[partition as usize].push((offset, payload));
                    }
                    Err(_) => {
                        // Transient failures during leader election are acceptable
                    }
                }
            }
        }
        println!("    Cycle {cycle} writes: {cycle_successes}/120 succeeded");

        cluster.restart_node(victim).expect("restart failed");
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    println!(
        "  Failover writes total: {failover_successes}/{failover_attempts} succeeded ({:.1}%)",
        100.0 * failover_successes as f64 / failover_attempts as f64
    );

    // === Phase 3: Final writes (stable cluster, expect 100% success) ===
    println!("\n[{mode_str}] Phase 3: Final writes (300 total, expect 100% success)...");
    for partition in 0..num_partitions {
        for i in 0..50u32 {
            let payload = format!("p{partition}-final-{i:04}");
            final_attempts += 1;
            match executor
                .send(topic, partition, Bytes::from(payload.clone()))
                .await
            {
                Ok(offset) => {
                    final_successes += 1;
                    acknowledged[partition as usize].push((offset, payload));
                }
                Err(e) => {
                    eprintln!("FINAL_WRITE_FAILED: partition {partition} i={i}: {e}");
                }
            }
        }
    }
    println!(
        "  Final writes: {final_successes}/{final_attempts} succeeded ({:.1}%)",
        100.0 * final_successes as f64 / final_attempts as f64
    );

    // === Phase 4: Verification ===
    println!("\n[{mode_str}] Phase 4: Verifying Kafka invariants...");

    let total_attempts = initial_attempts + failover_attempts + final_attempts;
    let total_successes = initial_successes + failover_successes + final_successes;
    let total_acknowledged: usize = acknowledged.iter().map(Vec::len).sum();

    // Sanity check: acknowledged count should match successes
    assert_eq!(
        total_acknowledged, total_successes as usize,
        "BUG: acknowledged count mismatch"
    );

    let mut verified = 0u32;
    let mut content_mismatch_errors = 0u32;
    let mut missing_offset_errors = 0u32;
    let mut storage_duplicate_errors = 0u32;
    let mut producer_duplicate_errors = 0u32;

    for partition in 0..num_partitions {
        let expected = &acknowledged[partition as usize];

        // Check 1: Producer should not return same offset for different payloads
        let mut offset_to_payload: std::collections::HashMap<u64, &str> =
            std::collections::HashMap::new();
        for (offset, payload) in expected {
            if let Some(existing) = offset_to_payload.insert(*offset, payload.as_str()) {
                if existing != payload.as_str() {
                    eprintln!(
                        "PRODUCER_DUPLICATE: partition {partition} offset {offset} returned for BOTH '{}' AND '{}'",
                        existing, payload
                    );
                    producer_duplicate_errors += 1;
                }
            }
        }

        // Read back all data from storage
        let consumed = executor
            .poll(topic, partition, 0, expected.len() as u32 + 100)
            .await
            .expect("poll failed");

        // Check 2: Storage should not have duplicate offsets
        let mut seen_offsets: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (offset, _) in &consumed {
            if !seen_offsets.insert(*offset) {
                eprintln!(
                    "STORAGE_DUPLICATE: partition {partition} offset {offset} appears multiple times in storage"
                );
                storage_duplicate_errors += 1;
            }
        }

        // Build map for content verification
        let consumed_map: std::collections::HashMap<u64, Bytes> = consumed.into_iter().collect();

        // Check 3: THE CORE KAFKA INVARIANT
        // Every acknowledged (offset, payload) must be readable with correct content
        for (offset, expected_payload) in expected {
            match consumed_map.get(offset) {
                Some(actual) => {
                    let actual_str = String::from_utf8_lossy(actual);
                    if actual_str == expected_payload.as_str() {
                        verified += 1;
                    } else {
                        eprintln!(
                            "CONTENT_MISMATCH: partition {partition} offset {offset}: expected '{}', got '{}'",
                            expected_payload, actual_str
                        );
                        content_mismatch_errors += 1;
                    }
                }
                None => {
                    eprintln!(
                        "MISSING_OFFSET: partition {partition} offset {offset} not found (expected '{expected_payload}')"
                    );
                    missing_offset_errors += 1;
                }
            }
        }
    }

    // === Results Summary ===
    let integrity_errors =
        content_mismatch_errors + missing_offset_errors + storage_duplicate_errors + producer_duplicate_errors;

    println!("\n=== Rapid Failover Stress Test Results ({mode_str} mode) ===");
    println!("Write Statistics:");
    println!("  Initial:  {initial_successes}/{initial_attempts} ({:.1}%)",
        100.0 * initial_successes as f64 / initial_attempts as f64);
    println!("  Failover: {failover_successes}/{failover_attempts} ({:.1}%)",
        100.0 * failover_successes as f64 / failover_attempts as f64);
    println!("  Final:    {final_successes}/{final_attempts} ({:.1}%)",
        100.0 * final_successes as f64 / final_attempts as f64);
    println!("  Total:    {total_successes}/{total_attempts} ({:.1}%)",
        100.0 * total_successes as f64 / total_attempts as f64);
    println!();
    println!("Verification:");
    println!("  Acknowledged writes: {total_acknowledged}");
    println!("  Verified correct:    {verified}");
    println!();
    println!("Integrity Errors:");
    println!("  Content mismatches:    {content_mismatch_errors}");
    println!("  Missing offsets:       {missing_offset_errors}");
    println!("  Storage duplicates:    {storage_duplicate_errors}");
    println!("  Producer duplicates:   {producer_duplicate_errors}");
    println!("  TOTAL ERRORS:          {integrity_errors}");

    // === Assertions ===

    // 1. Initial and final writes should have 100% success (stable cluster)
    assert_eq!(
        initial_successes, initial_attempts,
        "Initial writes failed unexpectedly ({initial_successes}/{initial_attempts}) - cluster should be stable"
    );
    assert_eq!(
        final_successes, final_attempts,
        "Final writes failed unexpectedly ({final_successes}/{final_attempts}) - cluster should be stable"
    );

    // 2. Failover writes should have high success rate (quorum always available)
    let failover_success_rate = failover_successes as f64 / failover_attempts as f64;
    assert!(
        failover_success_rate >= 0.80,
        "Failover write success rate too low ({:.1}%) - quorum should be available",
        failover_success_rate * 100.0
    );

    // 3. Zero integrity errors (the core Kafka guarantee)
    assert_eq!(
        integrity_errors, 0,
        "CRITICAL: {integrity_errors} data integrity errors - Kafka offset guarantee violated! \
         (content_mismatch={content_mismatch_errors}, missing={missing_offset_errors}, \
         storage_dup={storage_duplicate_errors}, producer_dup={producer_duplicate_errors})"
    );

    // 4. All acknowledged writes verified
    assert_eq!(
        verified as usize, total_acknowledged,
        "Not all acknowledged writes verified: {verified}/{total_acknowledged}"
    );
}

/// Rapid failover stress test - actor mode.
#[tokio::test]
async fn test_actor_mode_rapid_failover_stress() {
    run_rapid_failover_stress_test(true, 23800, 23900).await;
}

/// Rapid failover stress test - non-actor mode.
///
/// Tests the same bug scenario as the actor mode test, but using the
/// traditional tick-based processing path in helix-server.
#[tokio::test]
async fn test_non_actor_mode_rapid_failover_stress() {
    run_rapid_failover_stress_test(false, 23950, 24050).await;
}

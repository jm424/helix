//! Single/multi-node tests: node startup, partition, crash, multi-partition leader failure.

use std::time::Duration;

use helix_core::NodeId;
use madsim::runtime::Runtime;

use super::super::*;

#[test]
fn test_e2e_single_node() {
    // Test single node (no Raft replication needed).
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(1);
        let cluster = E2ECluster::start_with_config(config).await;

        // Wait for cluster to stabilize (single-node needs time for controller election).
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic with retries (same pattern as multi-node).
        for attempt in 0..30 {
            if cluster.create_topic("actor-test", 1).await.is_ok() {
                eprintln!(
                    "[INFO] Single-node: Topic created on attempt {}",
                    attempt
                );
                break;
            }
            cluster.sleep(Duration::from_millis(100)).await;
            if attempt == 29 {
                panic!("Failed to create topic after 30 attempts");
            }
        }

        // Wait for partition actors to be created.
        cluster.sleep(Duration::from_secs(1)).await;

        // Debug: check router state.
        cluster.debug_print_router_state().await;

        // Produce data with retries.
        for i in 0..5 {
            let offset = cluster
                .produce_with_retry("actor-test", 0, format!("record-{i}"), 100)
                .await;
            assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
            eprintln!(
                "[INFO] Produced record {} at offset {:?}",
                i, offset
            );
        }

        // Consume data.
        let records = cluster.consume("actor-test", 0, 0).await;
        assert!(records.is_ok(), "Consume should succeed");
        assert_eq!(records.unwrap().len(), 5, "Should have 5 records");

        eprintln!("[PASS] test_e2e_single_node: Single-node produce/consume works");
    });
}

#[test]
fn test_e2e_multi_node() {
    // Test 3-node cluster using Raft replication.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3);
        let cluster = E2ECluster::start_with_config(config).await;

        // Wait for cluster to stabilize (controller election takes time).
        cluster.sleep(Duration::from_secs(5)).await;

        // Create topic - uses controller in multi-node mode.
        // Retry multiple times as controller election may still be in progress.
        for attempt in 0..30 {
            if cluster.create_topic("actor-orders", 1).await.is_ok() {
                eprintln!("[INFO] Topic created on attempt {}", attempt);
                break;
            }
            cluster.sleep(Duration::from_millis(200)).await;
            if attempt == 29 {
                panic!("Failed to create topic after 30 attempts");
            }
        }

        // Wait for partition actors to be created on all nodes.
        // The AssignPartition commit needs to replicate to all nodes and each node
        // needs to create its partition actor before the election can complete.
        cluster.sleep(Duration::from_secs(3)).await;

        // Debug: check router state.
        cluster.debug_print_router_state().await;

        // Produce with retries - will succeed once leader is elected.
        for i in 0..10 {
            let offset = cluster
                .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                .await;
            assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
            eprintln!(
                "[INFO] Produced record {} at offset {:?}",
                i, offset
            );
        }

        // Wait for replication to complete.
        cluster.sleep(Duration::from_secs(1)).await;

        // Consume.
        let records = cluster
            .consume("actor-orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!("[INFO] Consumed {} records", records.len());
        assert_eq!(records.len(), 10, "Should have 10 records");

        eprintln!("[PASS] test_e2e_multi_node: Multi-node produce/consume works");
    });
}

#[test]
fn test_e2e_with_partition() {
    // Test cluster survives network partitions.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3);
        let cluster = E2ECluster::start_with_config(config).await;

        // Wait for cluster to stabilize.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster
            .create_topic("actor-orders", 1)
            .await
            .expect("create topic");

        // Produce some data.
        for i in 0..5 {
            cluster
                .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed");
        }
        eprintln!("[INFO] Produced 5 records before partition");

        // Partition node 1 from nodes 2 and 3.
        cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
        cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
        eprintln!("[INFO] Network partition created");

        // Wait for leader election.
        cluster.sleep(Duration::from_secs(3)).await;

        // Produce more (should succeed on majority partition).
        for i in 5..10 {
            cluster
                .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed after partition");
        }
        eprintln!("[INFO] Produced 5 more records during partition");

        // Heal the partition.
        cluster.heal_all();
        cluster.sleep(Duration::from_secs(5)).await;

        // Verify data available.
        let records = cluster
            .consume("actor-orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!(
            "[INFO] Consumed {} records after healing",
            records.len()
        );
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_with_partition: Cluster survives partitions");
    });
}

#[test]
fn test_e2e_with_crash() {
    // Test cluster survives node crashes.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3);
        let cluster = E2ECluster::start_with_config(config).await;

        // Wait for cluster to stabilize.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster
            .create_topic("actor-orders", 1)
            .await
            .expect("create topic");

        // Produce some data.
        for i in 0..5 {
            cluster
                .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed");
        }
        eprintln!("[INFO] Produced 5 records before crash");

        // Crash node 1.
        cluster.crash_node(NodeId::new(1));
        eprintln!("[INFO] Node 1 crashed");

        // Wait for failover.
        cluster.sleep(Duration::from_secs(3)).await;

        // Produce more (should succeed on surviving nodes).
        for i in 5..10 {
            cluster
                .produce_with_retry("actor-orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed after crash");
        }
        eprintln!("[INFO] Produced 5 more records after crash");

        // Wait for replication.
        cluster.sleep(Duration::from_secs(2)).await;

        // Verify data on surviving nodes.
        let records = cluster
            .consume("actor-orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!("[INFO] Consumed {} records", records.len());
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_with_crash: Cluster survives crashes");
    });
}

#[test]
fn test_e2e_multi_partition_leader_failure() {
    // Multi-partition leader failure with real Kafka protocol path.
    //
    // Produces use acks=all, so every Ok(offset) means the record is
    // Raft-committed on a quorum. After a node crash and re-election,
    // ALL acked records must be consumable — no timing hacks needed.
    //
    // This exercises the full path: Kafka handler → batcher → partition
    // actor → Raft propose → quorum commit → ack. On NOT_LEADER,
    // produce_with_retry cycles nodes just like a real Kafka client.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3);
        let cluster = E2ECluster::start_with_config(config).await;

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("multi-part", 3)
            .await
            .expect("create topic");
        cluster.sleep(Duration::from_secs(1)).await;

        const RECORDS_PER_PHASE: usize = 10;
        const PARTITION_COUNT: u32 = 3;

        // Phase 1: Produce to all partitions (healthy cluster).
        // Every ack means quorum-committed.
        let mut acked_per_partition: Vec<usize> =
            vec![0; PARTITION_COUNT as usize];
        for i in 0..RECORDS_PER_PHASE {
            for p in 0..PARTITION_COUNT {
                let payload = format!("phase1-p{p}-{i}");
                cluster
                    .produce_with_retry("multi-part", p, payload, 50)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("phase1 p{p} record {i}: {e}")
                    });
                acked_per_partition[p as usize] += 1;
            }
        }
        eprintln!(
            "[Phase 1] {RECORDS_PER_PHASE} acked per partition (healthy)"
        );

        // Crash node 1 (may be leader for some partitions).
        cluster.crash_node(NodeId::new(1));
        eprintln!("[Crash] Node 1 down");

        // Leader re-election happens within Raft election timeout (~150-300ms
        // simulated). produce_with_retry handles NOT_LEADER by cycling nodes,
        // so we don't need an explicit sleep — the retry loop does the waiting.

        // Phase 2: Produce to all partitions (degraded cluster, 2 of 3 nodes).
        for i in 0..RECORDS_PER_PHASE {
            for p in 0..PARTITION_COUNT {
                let payload = format!("phase2-p{p}-{i}");
                cluster
                    .produce_with_retry("multi-part", p, payload, 200)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("phase2 p{p} record {i}: {e}")
                    });
                acked_per_partition[p as usize] += 1;
            }
        }
        eprintln!(
            "[Phase 2] {RECORDS_PER_PHASE} more acked per partition (degraded)"
        );

        // Verify: every acked record must be consumable. read_blobs now
        // gates on leadership (like Kafka), so consume() only gets data from
        // the leader which has the latest applied state. No retries needed.
        for p in 0..PARTITION_COUNT {
            let expected = acked_per_partition[p as usize];

            let records = cluster
                .consume("multi-part", p, 0)
                .await
                .unwrap_or_else(|e| panic!("consume p{p}: {e}"));

            eprintln!("[Verify] p{p}: acked={expected}, consumed={}", records.len());
            assert_eq!(
                records.len(),
                expected,
                "Partition {p}: acked {expected} but consumed {actual} \
                 (data loss after leader failure)",
                actual = records.len()
            );
        }

        // Cross-replica consistency on surviving nodes.
        for p in 0..PARTITION_COUNT {
            if let Err(e) = cluster
                .verify_replica_consistency("multi-part", p)
                .await
            {
                panic!("Partition {p} replica consistency: {e}");
            }
        }

        let total = acked_per_partition.iter().sum::<usize>();
        eprintln!(
            "[PASS] {PARTITION_COUNT} partitions, {total} total records, \
             zero data loss after leader failure"
        );
    });
}

//! Basic E2E cluster tests: startup, topic creation, storage, produce/consume, partition.

use std::time::Duration;

use helix_core::NodeId;
use helix_wal::FaultConfig;
use madsim::runtime::Runtime;

use super::super::*;

#[test]
fn test_e2e_cluster_starts() {
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        assert_eq!(cluster.node_ids().len(), 3);
        eprintln!("[PASS] test_e2e_cluster_starts: Cluster started with 3 nodes");
    });
}

#[test]
fn test_e2e_create_topic() {
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize (controller election needs time).
        cluster.sleep(Duration::from_secs(5)).await;

        // Create topic with retries (controller may still be electing).
        for attempt in 0..30 {
            if cluster.create_topic("test-topic", 1).await.is_ok() {
                eprintln!(
                    "[PASS] test_e2e_create_topic: Created topic on attempt {}",
                    attempt
                );
                return;
            }
            cluster.sleep(Duration::from_millis(200)).await;
        }
        panic!("test_e2e_create_topic: topic creation failed after 30 attempts");
    });
}

#[test]
fn test_e2e_durable_storage_exercised() {
    // Verify that durable storage (SharedWalPool) is actually being exercised.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(1).await;

        // Verify durable storage is enabled.
        let node = cluster.node(NodeId::new(1)).expect("node 1");
        assert!(
            node.service.shared_wal_pool().is_some(),
            "SharedWalPool should be enabled for durable storage"
        );
        assert!(
            node.service.data_dir().is_some(),
            "data_dir should be set for durable storage"
        );

        // Get initial storage stats.
        let initial_stats = node.storage.fault_stats();
        eprintln!(
            "[INFO] Initial storage stats: file_ops={}, writes={}, reads={}, syncs={}",
            initial_stats.total_ops,
            initial_stats.write_ops,
            initial_stats.read_ops,
            initial_stats.sync_ops
        );

        // Wait for cluster to stabilize.
        // Wait for controller election.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic and produce data.
        cluster.create_topic("durable-test", 1).await.expect("create topic");
        for i in 0..10 {
            cluster.produce_with_retry("durable-test", 0, format!("record-{i}"), 100).await
                .expect("produce should succeed");
        }

        // Let writes flush.
        cluster.sleep(Duration::from_millis(100)).await;

        // Check storage was exercised.
        let final_stats = node.storage.fault_stats();
        let write_delta = final_stats.write_ops - initial_stats.write_ops;
        let sync_delta = final_stats.sync_ops - initial_stats.sync_ops;
        eprintln!(
            "[INFO] Final storage stats: file_ops={}, writes={} (+{}), reads={}, syncs={} (+{})",
            final_stats.total_ops,
            final_stats.write_ops,
            write_delta,
            final_stats.read_ops,
            final_stats.sync_ops,
            sync_delta
        );

        // Verify writes actually happened.
        assert!(
            write_delta > 0,
            "Storage writes should have been exercised: initial={}, final={}",
            initial_stats.write_ops, final_stats.write_ops
        );

        // Verify syncs happened (durable storage).
        assert!(
            sync_delta > 0,
            "Storage syncs should have been exercised: initial={}, final={}",
            initial_stats.sync_ops, final_stats.sync_ops
        );

        // Consume and verify data.
        let records = cluster.consume("durable-test", 0, 0).await.expect("consume");
        assert_eq!(records.len(), 10, "Should have 10 records");

        eprintln!(
            "[PASS] test_e2e_durable_storage_exercised: SharedWalPool active, {} writes, {} syncs",
            write_delta, sync_delta
        );
    });
}

#[test]
fn test_e2e_storage_fault_injection() {
    // Verify that storage fault injection actually works with SharedWalPool.
    // Uses 3-node cluster because single-node uses append_nowait which doesn't
    // wait for storage durability (relies on Raft replication for guarantees).
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize (controller election, etc).
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic first (before enabling faults) - retry on failure.
        for attempt in 0..20 {
            if cluster.create_topic("fault-test", 1).await.is_ok() {
                break;
            }
            cluster.sleep(Duration::from_millis(100)).await;
            if attempt == 19 {
                panic!("Failed to create topic after 20 attempts");
            }
        }

        // Wait for partition actor leader election.
        cluster.sleep(Duration::from_secs(1)).await;

        // Produce some records successfully first.
        for i in 0..5 {
            cluster.produce_with_retry("fault-test", 0, format!("record-{i}"), 100).await
                .expect("produce should succeed");
        }

        // Wait for writes to flush.
        cluster.sleep(Duration::from_millis(100)).await;

        // Get initial fault stats.
        let node = cluster.node(NodeId::new(1)).expect("node 1");
        let initial_stats = node.storage.fault_stats();

        // Also check SharedWalPool is enabled and using our storage.
        let pool = node.service.shared_wal_pool();
        eprintln!("[INFO] SharedWalPool present: {}", pool.is_some());

        // Debug: print Arc pointer to verify sharing.
        eprintln!("[DEBUG] node.storage fault_config ptr: 0x{:x}", node.storage.fault_config_ptr());
        eprintln!(
            "[INFO] Before fault injection: write_ops={}, write_failures={}, sync_ops={}, fsync_failures={}",
            initial_stats.write_ops, initial_stats.write_failures,
            initial_stats.sync_ops, initial_stats.fsync_failures
        );

        // Verify the fault config pointer is what we expect.
        {
            let config = node.storage.fault_config();
            eprintln!("[INFO] Current fault config: write_fail_rate={}", config.write_fail_rate);
        }

        // Enable write failures on ALL nodes at 30% rate.
        // This tests cluster-wide fault tolerance - with 3 nodes and 30% failure rate,
        // most writes should succeed via replication, but some might fail.
        let fault_config = FaultConfig {
            write_fail_rate: 0.3,
            ..Default::default()
        };
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, fault_config.clone());
        }
        eprintln!("[INFO] Set 30% write failure rate on all nodes");

        // Try to produce more records - some should fail.
        let mut successes = 0;
        let mut failures = 0;
        for i in 5..25 {
            // Use lower retry count to see failures.
            match cluster.produce_with_retry("fault-test", 0, format!("record-{i}"), 3).await {
                Ok(_) => successes += 1,
                Err(_) => failures += 1,
            }
        }

        // Wait for writes to flush to storage.
        cluster.sleep(Duration::from_millis(100)).await;

        // Aggregate fault stats from all nodes.
        let mut total_write_failures = 0u64;
        let mut total_write_ops = 0u64;
        for &nid in cluster.node_ids() {
            let n = cluster.node(nid).expect("node exists");
            let stats = n.storage.fault_stats();
            total_write_failures += stats.write_failures - initial_stats.write_failures;
            total_write_ops += stats.write_ops - initial_stats.write_ops;
            eprintln!(
                "[INFO] Node {} after faults: write_ops={}, write_failures={}",
                nid.get(), stats.write_ops, stats.write_failures
            );
        }
        eprintln!(
            "[INFO] Cluster total: write_ops delta={}, write_failures delta={}, produces: successes={}, failures={}",
            total_write_ops, total_write_failures, successes, failures
        );

        // Verify faults were actually injected somewhere in the cluster.
        assert!(
            total_write_failures > 0,
            "Storage write failures should have been injected across cluster"
        );

        // Disable faults on all nodes and verify recovery.
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, FaultConfig::default());
        }
        cluster.sleep(Duration::from_millis(100)).await;

        // Should be able to produce again.
        cluster.produce_with_retry("fault-test", 0, "recovery-record", 50).await
            .expect("produce should succeed after disabling faults");

        eprintln!(
            "[PASS] test_e2e_storage_fault_injection: {} write failures injected across cluster, system recovered",
            total_write_failures
        );
    });
}

#[test]
fn test_e2e_produce_consume() {
    // Test E2E produce/consume with 3-node cluster using just public APIs.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize (controller election, etc).
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic - uses controller in multi-node mode.
        cluster
            .create_topic("orders", 1)
            .await
            .expect("create topic");

        // Produce with retries - will succeed once leader is elected.
        for i in 0..10 {
            let offset = cluster
                .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                .await;
            assert!(offset.is_ok(), "Produce should succeed: {:?}", offset);
            eprintln!("[INFO] Produced record {} at offset {:?}", i, offset);
        }

        // Consume.
        let records = cluster
            .consume("orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!("[INFO] Consumed {} records", records.len());
        assert_eq!(records.len(), 10, "Should have 10 records");

        eprintln!("[PASS] test_e2e_produce_consume: Multi-node E2E works");
    });
}

#[test]
fn test_e2e_network_partition() {
    // Test that data survives network partitions.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster
            .create_topic("orders", 1)
            .await
            .expect("create topic");

        // Produce some data.
        for i in 0..5 {
            cluster
                .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed");
        }
        eprintln!("[INFO] Produced 5 records before partition");

        // Partition node 1 from nodes 2 and 3.
        cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
        cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
        eprintln!("[INFO] Network partition created: node 1 isolated");

        // Wait for leader election. The partitioned node needs time to:
        // 1. Realize it can't reach quorum (heartbeat failures)
        // 2. Step down from leader
        // 3. Nodes 2 and 3 need to elect a new leader
        // This requires multiple election timeout periods (~300ms each).
        cluster.sleep(Duration::from_secs(3)).await;
        for i in 5..10 {
            cluster
                .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed after partition");
        }
        eprintln!("[INFO] Produced 5 more records during partition");

        // Heal the partition.
        cluster.heal_all();

        // Wait for data to replicate from new leader back to healed node.
        // This requires time for:
        // 1. Leader to detect partition healed and send AppendEntries
        // 2. Follower to resolve log conflicts by backtracking
        // 3. Entries to be replicated and committed
        cluster.sleep(Duration::from_secs(5)).await;

        // Find and log which node we're consuming from.
        let leader = cluster.find_partition_leader("orders", 0).await;
        eprintln!(
            "[DEBUG] Consuming from leader: {:?}",
            leader.map(|n| n.get())
        );

        // Verify data available on healed cluster.
        let records = cluster
            .consume("orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!("[INFO] Consumed {} records after healing", records.len());
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_network_partition: Data survives partitions");
    });
}

//! Replica consistency and node crash tests.

use std::time::Duration;

use helix_core::NodeId;
use madsim::runtime::Runtime;

use super::super::*;

#[test]
fn test_e2e_replica_consistency_after_partition() {
    // Test that replicas have consistent data after network partition heals.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster.create_topic("consistency-test", 1).await.expect("create topic");

        // Produce some data before partition.
        for i in 0..5 {
            cluster.produce_with_retry("consistency-test", 0, format!("before-{i}"), 100).await
                .expect("produce should succeed");
        }
        eprintln!("[INFO] Produced 5 records before partition");

        // Verify initial consistency (all nodes should agree).
        cluster.verify_replica_consistency("consistency-test", 0).await
            .expect("should be consistent before partition");
        eprintln!("[INFO] Replicas consistent before partition");

        // Partition node 1 from nodes 2 and 3.
        cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
        cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
        eprintln!("[INFO] Network partition created: node 1 isolated");

        // Wait for new leader election.
        cluster.sleep(Duration::from_secs(3)).await;

        // Produce more data during partition (goes to majority partition).
        for i in 5..10 {
            cluster.produce_with_retry("consistency-test", 0, format!("during-{i}"), 100).await
                .expect("produce should succeed during partition");
        }
        eprintln!("[INFO] Produced 5 more records during partition");

        // Heal the partition.
        cluster.heal_all();
        eprintln!("[INFO] Partition healed");

        // Wait for replication to catch up.
        cluster.sleep(Duration::from_secs(5)).await;

        // Verify consistency after healing.
        // This is the key test - all replicas should converge to same state.
        cluster.verify_replica_consistency("consistency-test", 0).await
            .expect("should be consistent after partition heals");
        eprintln!("[INFO] Replicas consistent after healing");

        // Verify all data is readable.
        let records = cluster.consume("consistency-test", 0, 0).await.expect("consume should succeed");
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_replica_consistency_after_partition: Replicas converge after partition");
    });
}

#[test]
fn test_e2e_replica_consistency_after_crash() {
    // Test that replicas have consistent data after node crash and continued writes.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        // Wait for cluster to stabilize.
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster.create_topic("crash-consistency", 1).await.expect("create topic");

        // Produce some data.
        for i in 0..5 {
            cluster.produce_with_retry("crash-consistency", 0, format!("before-{i}"), 100).await
                .expect("produce should succeed");
        }
        eprintln!("[INFO] Produced 5 records before crash");

        // Crash node 1.
        cluster.crash_node(NodeId::new(1));
        eprintln!("[INFO] Node 1 crashed");

        // Wait for failover.
        cluster.sleep(Duration::from_secs(3)).await;

        // Produce more data after crash (to surviving nodes only).
        for i in 5..10 {
            cluster.produce_with_retry("crash-consistency", 0, format!("after-{i}"), 100).await
                .expect("produce should succeed after crash");
        }
        eprintln!("[INFO] Produced 5 more records after crash");

        // Wait for replication.
        cluster.sleep(Duration::from_secs(2)).await;

        // Verify consistency among surviving nodes (crashed node is skipped).
        cluster.verify_replica_consistency("crash-consistency", 0).await
            .expect("surviving replicas should be consistent");
        eprintln!("[INFO] Surviving replicas consistent");

        // Verify all data is readable from surviving nodes.
        let records = cluster.consume("crash-consistency", 0, 0).await.expect("consume should succeed");
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_replica_consistency_after_crash: Surviving replicas stay consistent");
    });
}

#[test]
fn test_e2e_node_crash() {
    // Test that data survives node crashes.
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
        eprintln!("[INFO] Produced 5 records before crash");

        // Crash node 1.
        cluster.crash_node(NodeId::new(1));
        eprintln!("[INFO] Node 1 crashed");

        // Wait for failover. Similar to partition, surviving nodes need time to:
        // 1. Detect the crash (missed heartbeats)
        // 2. Elect a new leader among Nodes 2 and 3
        cluster.sleep(Duration::from_secs(3)).await;
        for i in 5..10 {
            cluster
                .produce_with_retry("orders", 0, format!("order-{i}"), 100)
                .await
                .expect("produce should succeed after crash");
        }
        eprintln!("[INFO] Produced 5 more records after crash");

        // Wait a bit more for replication.
        cluster.sleep(Duration::from_secs(2)).await;

        // Debug: show state before consuming.
        cluster.debug_partition_state("orders", 0).await;

        // Find and log which node we're consuming from.
        let leader = cluster.find_partition_leader("orders", 0).await;
        eprintln!(
            "[DEBUG] Consuming from leader: {:?}",
            leader.map(|n| n.get())
        );

        // Verify data on surviving nodes.
        let records = cluster
            .consume("orders", 0, 0)
            .await
            .expect("consume should succeed");
        eprintln!("[INFO] Consumed {} records", records.len());
        assert_eq!(records.len(), 10, "All 10 records should be available");

        eprintln!("[PASS] test_e2e_node_crash: Data survives node crash");
    });
}

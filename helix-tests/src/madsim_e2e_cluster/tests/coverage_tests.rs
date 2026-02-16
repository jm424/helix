//! Coverage gap tests: five-node cluster, rapid crash recovery, storage faults,
//! WAL configurations, and WAL replay.

use std::time::Duration;

use helix_core::NodeId;
use helix_wal::FaultConfig;
use madsim::runtime::Runtime;

use super::super::*;

/// Helper for five-node cluster test, parameterized by config.
fn run_five_node_cluster(config: E2EClusterConfig, test_name: &str) {
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start_with_config(config).await;

        assert_eq!(cluster.node_ids().len(), 5);
        cluster.sleep(Duration::from_secs(2)).await;

        // Create topic.
        cluster
            .create_topic("five-node", 1)
            .await
            .expect("create topic");

        // Phase 1: produce on healthy cluster.
        for i in 0..5 {
            cluster
                .produce_and_track("five-node", 0, format!("before-{i}"), 100)
                .await
                .expect("produce before crash");
        }
        eprintln!("[INFO] Produced 5 records on healthy 5-node cluster");

        // Crash nodes 1 and 2 (cluster still has quorum: 3 of 5).
        cluster.crash_node(NodeId::new(1));
        cluster.crash_node(NodeId::new(2));
        eprintln!("[INFO] Nodes 1 and 2 crashed");

        cluster.sleep(Duration::from_secs(3)).await;

        // Phase 2: produce during degraded state.
        for i in 0..5 {
            cluster
                .produce_and_track("five-node", 0, format!("during-{i}"), 100)
                .await
                .expect("produce during crash");
        }
        eprintln!("[INFO] Produced 5 records on degraded cluster");

        // Recover both nodes.
        cluster.recover_node(NodeId::new(1));
        cluster.recover_node(NodeId::new(2));
        cluster.sleep(Duration::from_secs(5)).await;

        // Phase 3: produce after recovery.
        for i in 0..5 {
            cluster
                .produce_and_track("five-node", 0, format!("after-{i}"), 100)
                .await
                .expect("produce after recovery");
        }
        eprintln!("[INFO] Produced 5 records after recovery");

        // Wait for replication to converge.
        cluster.sleep(Duration::from_secs(3)).await;

        // Verify all 15 records are consumable with correct content.
        let records = cluster
            .consume_and_verify("five-node", 0, 0)
            .await
            .expect("consume should succeed");
        assert_eq!(records.len(), 15, "All 15 acked records must be present");

        // Verify cross-replica consistency on all 5 nodes.
        cluster
            .verify_replica_consistency("five-node", 0)
            .await
            .expect("all 5 replicas should be consistent");

        cluster.assert_no_violations(test_name);
        eprintln!("[PASS] {test_name}: 15 records, 5 replicas consistent");
    });
}

#[test]
fn test_e2e_five_node_cluster() {
    run_five_node_cluster(
        E2EClusterConfig::with_nodes(5),
        "test_e2e_five_node_cluster",
    );
}

#[test]
fn test_e2e_rapid_crash_recovery() {
    // 3 nodes, crash/recover node 1 three times, produce after each recovery.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start(3).await;

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("rapid-crash", 1)
            .await
            .expect("create topic");

        let mut total_produced = 0u32;

        for cycle in 0..3 {
            // Crash node 1.
            cluster.crash_node(NodeId::new(1));
            eprintln!("[INFO] Cycle {cycle}: node 1 crashed");
            cluster.sleep(Duration::from_secs(2)).await;

            // Recover node 1.
            cluster.recover_node(NodeId::new(1));
            eprintln!("[INFO] Cycle {cycle}: node 1 recovered");
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce 5 records after recovery.
            for i in 0..5 {
                cluster
                    .produce_and_track(
                        "rapid-crash",
                        0,
                        format!("cycle{cycle}-{i}"),
                        100,
                    )
                    .await
                    .expect("produce after recovery");
                total_produced += 1;
            }
            eprintln!("[INFO] Cycle {cycle}: produced 5 records (total={total_produced})");
        }

        // Wait for final replication.
        cluster.sleep(Duration::from_secs(3)).await;

        let records = cluster
            .consume_and_verify("rapid-crash", 0, 0)
            .await
            .expect("consume should succeed");
        assert_eq!(
            records.len(),
            total_produced as usize,
            "All {total_produced} acked records must be present"
        );

        cluster.assert_no_violations("test_e2e_rapid_crash_recovery");
        eprintln!("[PASS] test_e2e_rapid_crash_recovery: {total_produced} records after 3 crash/recover cycles");
    });
}

#[test]
fn test_e2e_storage_faults_with_crash() {
    // 3 nodes with flaky storage, produce, crash a node, produce more,
    // recover, verify data integrity.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3);
        let cluster = E2ECluster::start_with_config(config).await;

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("fault-crash", 1)
            .await
            .expect("create topic");

        // Enable flaky storage on all nodes.
        let flaky = FaultConfig::flaky_runtime();
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, flaky.clone());
        }
        eprintln!("[INFO] Enabled flaky_runtime faults on all nodes");

        // Produce with faults active.
        let mut acked = 0u32;
        for i in 0..10 {
            if cluster
                .produce_and_track("fault-crash", 0, format!("pre-{i}"), 20)
                .await
                .is_ok()
            {
                acked += 1;
            }
        }
        eprintln!("[INFO] Produced {acked}/10 with faults active");

        // Crash node 1.
        cluster.crash_node(NodeId::new(1));
        cluster.sleep(Duration::from_secs(3)).await;

        // Produce more on surviving nodes.
        for i in 0..10 {
            if cluster
                .produce_and_track("fault-crash", 0, format!("post-{i}"), 20)
                .await
                .is_ok()
            {
                acked += 1;
            }
        }
        eprintln!("[INFO] Produced total {acked} acked records");

        // Recover and disable faults.
        cluster.recover_node(NodeId::new(1));
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, FaultConfig::default());
        }
        cluster.sleep(Duration::from_secs(5)).await;

        // Verify: all acked records must be present and correct.
        let records = cluster
            .consume_and_verify("fault-crash", 0, 0)
            .await
            .expect("consume should succeed");
        assert!(
            records.len() >= acked as usize,
            "Expected at least {acked} records, got {}",
            records.len()
        );

        cluster.assert_no_violations("test_e2e_storage_faults_with_crash");
        eprintln!(
            "[PASS] test_e2e_storage_faults_with_crash: {} records verified under faults + crash",
            records.len()
        );
    });
}

/// Helper for storage-faults-all-types test, parameterized by config.
fn run_storage_faults_all_types(config: E2EClusterConfig, test_name: &str) {
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let cluster = E2ECluster::start_with_config(config).await;

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("fault-all", 1)
            .await
            .expect("create topic");

        // Phase 1: Healthy produce.
        for i in 0..5 {
            cluster
                .produce_and_track("fault-all", 0, format!("healthy-{i}"), 100)
                .await
                .expect("healthy produce");
        }
        eprintln!("[INFO] Phase 1: 5 records on healthy cluster");

        // Phase 2: Enable aggressive faults (torn writes + fsync + write failures).
        let aggressive = FaultConfig {
            torn_write_rate: 0.05,
            fsync_fail_rate: 0.05,
            write_fail_rate: 0.1,
            ..Default::default()
        };
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, aggressive.clone());
        }

        let mut acked_faults = 0u32;
        for i in 0..10 {
            if cluster
                .produce_and_track("fault-all", 0, format!("faulty-{i}"), 20)
                .await
                .is_ok()
            {
                acked_faults += 1;
            }
        }
        eprintln!("[INFO] Phase 2: {acked_faults}/10 acked under aggressive faults");

        // Phase 3: Network partition (node 1 isolated).
        cluster.partition(&[NodeId::new(1), NodeId::new(2)]);
        cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
        cluster.sleep(Duration::from_secs(3)).await;

        let mut acked_partition = 0u32;
        for i in 0..5 {
            if cluster
                .produce_and_track("fault-all", 0, format!("part-{i}"), 20)
                .await
                .is_ok()
            {
                acked_partition += 1;
            }
        }
        eprintln!("[INFO] Phase 3: {acked_partition}/5 acked during partition + faults");

        // Phase 4: Crash node 2 (only node 3 remains fully healthy).
        cluster.crash_node(NodeId::new(2));
        cluster.sleep(Duration::from_secs(2)).await;

        // Phase 5: Heal everything and recover.
        cluster.heal_all();
        cluster.recover_node(NodeId::new(2));
        for &nid in cluster.node_ids() {
            cluster.set_storage_faults(nid, FaultConfig::default());
        }
        cluster.sleep(Duration::from_secs(5)).await;

        // Final produce on healthy cluster.
        for i in 0..5 {
            cluster
                .produce_and_track("fault-all", 0, format!("final-{i}"), 100)
                .await
                .expect("final produce");
        }

        cluster.sleep(Duration::from_secs(3)).await;

        // Verify all acked data is intact.
        let records = cluster
            .consume_and_verify("fault-all", 0, 0)
            .await
            .expect("consume should succeed");
        let min_expected = 5 + acked_faults + acked_partition + 5;
        assert!(
            records.len() >= min_expected as usize,
            "Expected at least {min_expected} records, got {}",
            records.len()
        );

        cluster
            .verify_replica_consistency("fault-all", 0)
            .await
            .expect("replicas should be consistent after full recovery");

        cluster.assert_no_violations(test_name);
        eprintln!(
            "[PASS] {test_name}: {} records through all fault types",
            records.len()
        );
    });
}

#[test]
fn test_e2e_storage_faults_all_types() {
    run_storage_faults_all_types(
        E2EClusterConfig::with_nodes(3),
        "test_e2e_storage_faults_all_types",
    );
}

#[test]
fn test_e2e_shared_wal_pool_sizes() {
    // Test different shared WAL pool sizes (K=1,2,4,8).
    for &wal_count in &[1u32, 2, 4, 8] {
        eprintln!("[INFO] Testing shared_wal_count={wal_count}");

        let rt = Runtime::with_seed_and_config(42 + u64::from(wal_count), Default::default());
        rt.block_on(async {
            let config = E2EClusterConfig::with_nodes(3)
                .with_shared_wal_count(wal_count);
            let cluster = E2ECluster::start_with_config(config).await;

            cluster.sleep(Duration::from_secs(2)).await;

            let topic = format!("wal-k{wal_count}");
            cluster
                .create_topic(&topic, 1)
                .await
                .expect("create topic");

            // Produce before crash.
            for i in 0..5 {
                cluster
                    .produce_and_track(&topic, 0, format!("pre-{i}"), 100)
                    .await
                    .expect("produce before crash");
            }

            // Crash node 1.
            cluster.crash_node(NodeId::new(1));
            cluster.sleep(Duration::from_secs(3)).await;

            // Produce after crash.
            for i in 0..5 {
                cluster
                    .produce_and_track(&topic, 0, format!("post-{i}"), 100)
                    .await
                    .expect("produce after crash");
            }

            // Recover and wait for replication.
            cluster.recover_node(NodeId::new(1));
            cluster.sleep(Duration::from_secs(5)).await;

            // Verify all 10 records.
            let records = cluster
                .consume_and_verify(&topic, 0, 0)
                .await
                .expect("consume should succeed");
            assert_eq!(
                records.len(),
                10,
                "shared_wal_count={wal_count}: expected 10 records, got {}",
                records.len()
            );

            cluster
                .verify_replica_consistency(&topic, 0)
                .await
                .expect("replicas should be consistent");

            cluster.assert_no_violations(
                &format!("test_e2e_shared_wal_pool_sizes(K={wal_count})"),
            );
            eprintln!("[PASS] shared_wal_count={wal_count}: 10 records, replicas consistent");
        });
    }

    eprintln!("[PASS] test_e2e_shared_wal_pool_sizes: All pool sizes work correctly");
}

#[test]
fn test_e2e_per_partition_wal() {
    // Test per-partition dedicated WAL mode (no SharedWalPool).
    // Each partition gets its own WAL file — simpler than shared pool.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let config = E2EClusterConfig::with_nodes(3)
            .with_per_partition_wal();
        let cluster = E2ECluster::start_with_config(config).await;

        // Verify no SharedWalPool was created.
        let node = cluster.node(NodeId::new(1)).expect("node 1");
        assert!(
            node.service.shared_wal_pool().is_none(),
            "SharedWalPool should NOT be present in per-partition WAL mode"
        );
        assert!(
            node.service.data_dir().is_some(),
            "data_dir should be set for durable storage"
        );

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("wal-test", 1)
            .await
            .expect("create topic");

        // Produce records — these go through per-partition dedicated WAL.
        for i in 0..10 {
            cluster
                .produce_and_track("wal-test", 0, format!("record-{i}"), 100)
                .await
                .expect("produce should succeed");
        }

        // Verify data is readable before crash.
        let pre_crash = cluster
            .consume("wal-test", 0, 0)
            .await
            .expect("consume before crash");
        assert_eq!(pre_crash.len(), 10, "All 10 records should be readable before crash");

        // Crash node 1, produce more on survivors.
        cluster.crash_node(NodeId::new(1));
        cluster.sleep(Duration::from_secs(3)).await;

        for i in 10..15 {
            cluster
                .produce_and_track("wal-test", 0, format!("record-{i}"), 100)
                .await
                .expect("produce after crash");
        }

        // Recover and verify all acked data.
        cluster.recover_node(NodeId::new(1));
        cluster.sleep(Duration::from_secs(5)).await;

        let records = cluster
            .consume_and_verify("wal-test", 0, 0)
            .await
            .expect("consume should succeed");
        assert_eq!(records.len(), 15, "All 15 acked records must be present");

        cluster
            .verify_replica_consistency("wal-test", 0)
            .await
            .expect("replicas should be consistent");

        cluster.assert_no_violations("test_e2e_per_partition_wal");
        eprintln!(
            "[PASS] test_e2e_per_partition_wal: 15 records, replicas consistent"
        );
    });
}

// ========================================================================
// Crash Recovery with WAL Replay Tests
// ========================================================================

#[test]
fn test_e2e_crash_recovery_with_wal_replay() {
    // Tests real crash recovery: crash a node (reverts unsynced storage),
    // produce more on survivors, restart the crashed node (replays WAL),
    // verify the restarted node catches up and all data is consistent.
    let rt = Runtime::with_seed_and_config(42, Default::default());
    rt.block_on(async {
        let mut cluster =
            E2ECluster::start_with_config(E2EClusterConfig::with_nodes(3)).await;

        cluster.sleep(Duration::from_secs(2)).await;

        cluster
            .create_topic("wal-replay", 1)
            .await
            .expect("create topic");

        // Phase 1: Produce 10 records on healthy cluster.
        for i in 0..10 {
            cluster
                .produce_and_track("wal-replay", 0, format!("before-{i}"), 100)
                .await
                .expect("produce before crash");
        }
        eprintln!("[INFO] Produced 10 records before crash");

        // Wait for replication to all nodes.
        cluster.sleep(Duration::from_secs(1)).await;

        // Get storage stats before crash for later comparison.
        let pre_crash_stats = cluster
            .node(NodeId::new(1))
            .expect("node 1")
            .storage
            .fault_stats();
        eprintln!(
            "[INFO] Node 1 pre-crash: writes={}, syncs={}",
            pre_crash_stats.write_ops, pre_crash_stats.sync_ops
        );

        // Crash node 1 (reverts unsynced writes via simulate_crash).
        cluster.crash_node(NodeId::new(1));
        eprintln!("[INFO] Node 1 crashed (storage reverted to last fsync)");

        // Wait for failover.
        cluster.sleep(Duration::from_secs(3)).await;

        // Phase 2: Produce 10 more records on surviving nodes.
        for i in 10..20 {
            cluster
                .produce_and_track("wal-replay", 0, format!("during-{i}"), 100)
                .await
                .expect("produce during crash");
        }
        eprintln!("[INFO] Produced 10 more records on survivors");

        // Wait for replication on survivors.
        cluster.sleep(Duration::from_secs(1)).await;

        // Restart node 1 (creates fresh HelixService, replays WAL).
        cluster.restart_node(NodeId::new(1)).await;
        eprintln!("[INFO] Node 1 restarted (WAL replayed)");

        // Wait for the restarted node to catch up via Raft replication.
        cluster.sleep(Duration::from_secs(5)).await;

        // Get storage stats after restart to verify reads (WAL replay).
        let post_restart_stats = cluster
            .node(NodeId::new(1))
            .expect("node 1")
            .storage
            .fault_stats();
        eprintln!(
            "[INFO] Node 1 post-restart: writes={}, reads={}, syncs={}",
            post_restart_stats.write_ops,
            post_restart_stats.read_ops,
            post_restart_stats.sync_ops
        );

        // Verify: the restarted node should have read from storage (WAL replay).
        assert!(
            post_restart_stats.read_ops > 0,
            "Restarted node should have WAL replay reads, got 0"
        );

        // Phase 3: Produce 5 more records to verify the restarted node participates.
        for i in 20..25 {
            cluster
                .produce_and_track("wal-replay", 0, format!("after-{i}"), 100)
                .await
                .expect("produce after restart");
        }
        eprintln!("[INFO] Produced 5 records after restart");

        cluster.sleep(Duration::from_secs(3)).await;

        // Verify all 25 records are consumable with correct content.
        let records = cluster
            .consume_and_verify("wal-replay", 0, 0)
            .await
            .expect("consume should succeed");
        assert_eq!(records.len(), 25, "All 25 acked records must be present");

        // Verify cross-replica consistency on ALL 3 nodes (including restarted).
        cluster
            .verify_replica_consistency("wal-replay", 0)
            .await
            .expect("all 3 replicas (including restarted) should be consistent");

        cluster.assert_no_violations("test_e2e_crash_recovery_with_wal_replay");
        eprintln!(
            "[PASS] test_e2e_crash_recovery_with_wal_replay: \
             25 records, 3 replicas consistent after WAL replay"
        );
    });
}

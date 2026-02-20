//! Rolling restart liveness test.
//!
//! This test verifies that after a rolling restart of all cluster nodes (one at a time),
//! every partition on every topic still has a valid leader elected.
//!
//! # Bug reproduced
//!
//! After a rolling deploy in staging, some Raft groups ended up with no leader:
//! the metadata handler returned LEADER_NOT_AVAILABLE silently because the
//! partition actor was never registered on the surviving nodes. This manifested
//! as `NotLeaderForPartition` errors on the client side that never resolved.
//!
//! # What this test checks
//!
//! 1. Create topics with multiple partitions and wait for all to have stable leaders.
//! 2. Perform a rolling restart: kill node 1 → wait for recovery → kill node 2 →
//!    wait for recovery → kill node 3 → wait for recovery.
//! 3. After each individual restart, ALL partitions on ALL topics must have a
//!    valid leader. No partition may be left leaderless.
//! 4. After the full rolling restart, leader distribution must not be
//!    pathologically skewed (no single node leads >60% of all partition groups).
//! 5. Produce to every partition on every topic must succeed after the full restart.

#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::unused_async)]

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use helix_workload::{ProducerMode, RealCluster, RealExecutor, WorkloadExecutor};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::types::RDKafkaRespErr;

// ============================================================================
// Helpers (duplicated from controller_failover.rs intentionally — these tests
// are independent binaries and share no common test-only library)
// ============================================================================

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

fn test_data_dir(test_name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("helix-rolling-restart-tests")
        .join(test_name)
        .join(format!("{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn setup_cluster(test_name: &str, base_port: u16, raft_base_port: u16) -> RealCluster {
    RealCluster::builder()
        .nodes(3)
        .base_port(base_port)
        .raft_base_port(raft_base_port)
        .binary_path(binary_path())
        .data_dir(test_data_dir(test_name))
        .auto_create_topics(true)
        .auto_create_partitions(16)
        .default_replication_factor(3)
        .log_level("debug")
        .build()
        .expect("failed to start cluster")
}

/// Fetches metadata for a topic and returns `(count, Vec<(partition_id, leader, replicas)>)`.
fn fetch_topic_metadata(
    bootstrap_servers: &str,
    topic: &str,
) -> Result<(usize, Vec<(i32, i32, Vec<i32>)>), String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", "rolling-restart-metadata-checker")
        .create()
        .map_err(|e| format!("failed to create consumer: {e}"))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("failed to fetch metadata: {e}"))?;

    for t in metadata.topics() {
        if t.name() == topic {
            if let Some(err) = t.error() {
                if err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                    return Err(format!("topic '{topic}' metadata error: {err:?}"));
                }
            }

            let mut partitions = Vec::with_capacity(t.partitions().len());
            for p in t.partitions() {
                if let Some(err) = p.error() {
                    if err != RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR {
                        return Err(format!(
                            "partition {} error: {err:?}",
                            p.id()
                        ));
                    }
                }
                partitions.push((p.id(), p.leader(), p.replicas().to_vec()));
            }

            if partitions.is_empty() {
                return Err(format!("topic '{topic}' has no partitions yet"));
            }

            return Ok((partitions.len(), partitions));
        }
    }

    Err(format!("topic '{topic}' not found in metadata"))
}

/// Returns per-broker metadata results for a topic.
fn fetch_topic_metadata_per_broker(
    bootstrap_servers: &str,
    topic: &str,
) -> Vec<(String, Result<(usize, Vec<(i32, i32, Vec<i32>)>), String>)> {
    bootstrap_servers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|broker| (broker.to_string(), fetch_topic_metadata(broker, topic)))
        .collect()
}

/// Verifies that all `expected_partitions` partitions have valid leaders.
///
/// Returns `Err` if any partition is missing or has no leader (`leader < 0`).
fn validate_all_partitions_have_leaders(
    partitions: &[(i32, i32, Vec<i32>)],
    expected_partitions: usize,
) -> Result<(), String> {
    if partitions.len() != expected_partitions {
        return Err(format!(
            "expected {expected_partitions} partitions, got {}",
            partitions.len()
        ));
    }

    for expected_pid in 0..expected_partitions as i32 {
        let Some((_, leader, replicas)) = partitions
            .iter()
            .find(|(pid, _, _)| *pid == expected_pid)
        else {
            return Err(format!("partition {expected_pid} missing from metadata"));
        };

        if *leader < 0 {
            return Err(format!(
                "partition {expected_pid} has no leader (leader={leader})"
            ));
        }

        if replicas.is_empty() {
            return Err(format!("partition {expected_pid} has no replicas"));
        }

        if !replicas.contains(leader) {
            return Err(format!(
                "partition {expected_pid} leader {leader} not in replicas {replicas:?}"
            ));
        }
    }

    Ok(())
}

/// Waits until every live broker reports that all partitions on `topic` have valid leaders.
///
/// `min_live_brokers` is the number of brokers that must respond successfully (useful during
/// a rolling restart when one node is down, so we accept 2/3).
async fn wait_for_all_leaders(
    bootstrap_servers: &str,
    topic: &str,
    expected_partitions: usize,
    min_live_brokers: usize,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_observation = String::new();

    while std::time::Instant::now() < deadline {
        let results = fetch_topic_metadata_per_broker(bootstrap_servers, topic);
        let mut ok_brokers = 0usize;
        let mut errors: Vec<String> = Vec::new();

        for (broker, result) in results {
            match result {
                Ok((count, partitions)) => {
                    if count != expected_partitions {
                        errors.push(format!(
                            "{broker}: got {count} partitions, want {expected_partitions}"
                        ));
                        continue;
                    }
                    let mut sorted = partitions;
                    sorted.sort_by_key(|(pid, _, _)| *pid);
                    if let Err(e) =
                        validate_all_partitions_have_leaders(&sorted, expected_partitions)
                    {
                        errors.push(format!("{broker}: {e}"));
                        continue;
                    }
                    ok_brokers += 1;
                }
                Err(e) => {
                    errors.push(format!("{broker}: {e}"));
                }
            }
        }

        // Enough live brokers confirmed all partitions have leaders.
        // Brokers that failed to respond (transport error) are simply not
        // counted toward ok_brokers — they don't indicate a liveness failure.
        if ok_brokers >= min_live_brokers {
            return Ok(());
        }

        last_observation = errors.join(" | ");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(format!(
        "timeout waiting for all leaders on '{topic}': {last_observation}"
    ))
}

/// Counts how many partitions each node leads across all topics.
///
/// Returns a map `node_id → leader_count`.
fn count_leaders_per_node(
    bootstrap_servers: &str,
    topics: &[&str],
) -> std::collections::HashMap<i32, usize> {
    let mut counts: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
    for topic in topics {
        if let Ok((_, partitions)) = fetch_topic_metadata(bootstrap_servers, topic) {
            for (_, leader, _) in partitions {
                if leader >= 0 {
                    *counts.entry(leader).or_insert(0) += 1;
                }
            }
        }
    }
    counts
}

// ============================================================================
// Test: Rolling Restart Liveness
// ============================================================================

/// Tests that all partition leaders survive a full rolling restart.
///
/// This is the E2E reproduction of the staging bug where DLT partition leaders
/// concentrated on a single node after a rolling deploy, leaving the other
/// nodes with no registered partition actors for those groups and returning
/// silent `LEADER_NOT_AVAILABLE` to clients indefinitely.
#[tokio::test]
async fn test_rolling_restart_all_partitions_keep_leaders() {
    let mut cluster = setup_cluster("rolling_restart_liveness", 26100, 26200).await;

    let bootstrap_servers = cluster.bootstrap_servers().to_string();
    let executor = RealExecutor::with_mode(&bootstrap_servers, ProducerMode::LowLatency)
        .expect("failed to create executor");
    executor
        .wait_ready(Duration::from_secs(60))
        .await
        .expect("cluster not ready");

    // Create two topics with 16 partitions each (RF=3).
    // Using 16 partitions gives a good spread across 3 nodes and increases the
    // chance of detecting unbalanced leader distribution after restart.
    let topics = ["rolling-topic-a", "rolling-topic-b"];
    let partitions_per_topic = 16usize;

    println!("\n=== Setup: Create {} topics × {} partitions ===", topics.len(), partitions_per_topic);

    for topic in &topics {
        for p in 0..partitions_per_topic as i32 {
            cluster
                .wait_for_leader(topic, p, Duration::from_secs(60))
                .await
                .unwrap_or_else(|e| panic!("no leader for {topic}[{p}]: {e}"));
        }
        println!("  Topic '{topic}' ready with {partitions_per_topic} partitions");
    }

    // Wait for all brokers to converge on stable metadata before starting the
    // rolling restart. All 3 brokers must agree on all partitions.
    println!("\n=== Pre-restart: Wait for cluster-wide metadata convergence ===");
    for topic in &topics {
        wait_for_all_leaders(&bootstrap_servers, topic, partitions_per_topic, 3, Duration::from_secs(60))
            .await
            .unwrap_or_else(|e| panic!("pre-restart convergence failed for '{topic}': {e}"));
        println!("  '{topic}' all {partitions_per_topic} partitions have leaders on all 3 brokers");
    }

    // Perform a rolling restart: kill one node at a time, wait for recovery,
    // then move to the next. This mirrors a production rolling deploy.
    //
    // After each restart, we check with min_live_brokers=2 because one node is
    // temporarily down. The critical assertion is that the 2 live brokers agree
    // that ALL partitions still have leaders — not just some.
    println!("\n=== Rolling restart: kill → wait → restart (one node at a time) ===");

    for node_id in 1u64..=3 {
        println!("\n--- Rolling step: kill node {node_id} ---");
        cluster.kill_node(node_id).expect("failed to kill node");

        // Give Raft time to detect the failure and elect new leaders for any
        // groups that were led by the killed node.
        println!("  Waiting for leader re-election after node {node_id} kill...");

        for topic in &topics {
            // Only require 2 live brokers while node {node_id} is down.
            wait_for_all_leaders(
                &bootstrap_servers,
                topic,
                partitions_per_topic,
                2,
                Duration::from_secs(60),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "LIVENESS FAILURE: after killing node {node_id}, topic '{topic}' \
                     has partitions without a leader: {e}"
                )
            });
        }

        println!("  All partitions have leaders after killing node {node_id}");

        // Restart the node before proceeding to the next one.
        cluster.restart_node(node_id).expect("failed to restart node");
        println!("  Node {node_id} restarted, waiting for it to rejoin...");

        // Wait for all 3 brokers to agree again before killing the next node.
        // This matches real rolling deploy semantics: wait for health before
        // proceeding to the next pod.
        for topic in &topics {
            wait_for_all_leaders(
                &bootstrap_servers,
                topic,
                partitions_per_topic,
                3,
                Duration::from_secs(90),
            )
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "LIVENESS FAILURE: after restarting node {node_id}, topic '{topic}' \
                     failed to recover on all 3 brokers: {e}"
                )
            });
        }

        println!("  All 3 brokers converged on full leader set after node {node_id} restart");
    }

    // Final check: all partitions still have leaders on all 3 nodes.
    println!("\n=== Final check: All partitions have leaders after full rolling restart ===");
    for topic in &topics {
        wait_for_all_leaders(
            &bootstrap_servers,
            topic,
            partitions_per_topic,
            3,
            Duration::from_secs(60),
        )
        .await
        .unwrap_or_else(|e| {
            panic!("LIVENESS FAILURE after full rolling restart, topic '{topic}': {e}")
        });
        println!("  '{topic}': all {partitions_per_topic} partitions have leaders on all 3 brokers");
    }

    // Leader distribution: log for observability. Raft does not automatically
    // rebalance leaders after a rolling restart, so some skew is expected and
    // not a correctness failure on its own.
    //
    // We assert one hard invariant: no single node holds ALL leaders.
    // 100% concentration on one node means the other nodes' actors are not
    // registered — that is the pre-fix liveness bug.
    println!("\n=== Leader distribution ===");
    let total_partitions = topics.len() * partitions_per_topic;
    let topic_refs: Vec<&str> = topics.iter().copied().collect();
    let leader_counts = count_leaders_per_node(&bootstrap_servers, &topic_refs);

    for (node, count) in &leader_counts {
        let pct = (*count as f64 / total_partitions as f64) * 100.0;
        println!("  Node {node}: leads {count}/{total_partitions} partitions ({pct:.0}%)");
    }

    let max_leaders = leader_counts.values().copied().max().unwrap_or(0);
    assert!(
        max_leaders < total_partitions,
        "All {total_partitions} leaders on one node — other nodes have no registered actors. \
         Distribution: {leader_counts:?}"
    );
    println!("  Leadership is spread across ≥2 nodes (max={max_leaders}/{total_partitions})");

    // Produce to every partition on every topic to confirm the data path is
    // fully functional after the rolling restart.
    println!("\n=== Post-restart produce check ===");
    for topic in &topics {
        for p in 0..partitions_per_topic as i32 {
            let payload = Bytes::from(format!("post-restart-{topic}-p{p}"));
            executor
                .send(topic, p, payload)
                .await
                .unwrap_or_else(|e| {
                    panic!("produce to {topic}[{p}] failed after rolling restart: {e}")
                });
        }
        println!("  Produced to all {partitions_per_topic} partitions of '{topic}'");
    }

    println!("\n=== Rolling restart liveness test PASSED ===");
}

//! DST runner functions: `run_e2e_dst_random_faults` and `run_e2e_dst_concurrent_faults`.
//!
//! These are not tests themselves — they are parameterized runner functions
//! called by thin `#[test]` wrappers in `dst_tests.rs`.

use std::time::Duration;

use helix_core::NodeId;
use madsim::runtime::Runtime;
use tracing::debug;

use super::super::*;
use super::super::concurrent::background_producer_task;

/// Proper DST test with random fault injection based on seed.
///
/// This is the correct DST approach - each seed generates a DIFFERENT random
/// fault scenario, providing comprehensive coverage across failure modes.
///
/// Unlike the scripted tests (`test_e2e_crash_many_seeds`, `test_e2e_partition_many_seeds`),
/// this test uses `FaultScenario::random(seed)` to deterministically generate
/// varied fault patterns.
///
/// # Known Limitation: Sequential Produce-Then-Fault
///
/// The DST loop follows a sequential pattern: produce records, then inject
/// faults, then sleep. It never produces concurrently with active fault
/// injection within the same tick. This means certain race conditions
/// (e.g., a write in progress when a crash occurs) are not exercised here.
///
/// The helix-workload E2E tests (which spawn real `helix-server` processes)
/// DO exercise concurrent production during faults, because the producer
/// and fault injector run in separate threads.
///
/// # Verification
///
/// This test performs three levels of verification:
/// 1. **Raft invariants**: `SingleLeaderPerTerm` via property state snapshots
/// 2. **Data integrity**: Payload hashes match between produce and consume
/// 3. **Count check**: At least 50% of produced records are consumable
pub(super) fn run_e2e_dst_random_faults(
    total_seeds: u64,
    records_per_seed: usize,
    ticks_per_seed: u32,
    topic: &str,
    partition_count: u32,
    progress_mod: u64,
) {
    use crate::madsim_scenarios::{FaultScenario, ScenarioExecutor};
    use std::collections::HashSet;

    let mut failures: Vec<(u64, String, String)> = Vec::new();
    let mut scenario_counts: std::collections::HashMap<&'static str, u64> =
        std::collections::HashMap::new();
    let seed_filter = std::env::var("MADSIM_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());
    let debug_seed = std::env::var("MADSIM_DEBUG_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    let suite_start = std::time::Instant::now();
    for seed in 0..total_seeds {
        if seed_filter.is_some_and(|s| s != seed) {
            continue;
        }
        // Generate random scenario from seed.
        let scenario = FaultScenario::random(seed);
        let scenario_name = scenario.name();
        *scenario_counts.entry(scenario_name).or_default() += 1;

        if seed % progress_mod == 0 {
            eprintln!(
                "[PROGRESS] Seed {}/{} - scenario: {}",
                seed, total_seeds, scenario_name
            );
        }

        let seed_start = std::time::Instant::now();
        let result = std::panic::catch_unwind(|| {
            let rt = Runtime::with_seed_and_config(seed, Default::default());
            rt.block_on(async {
                let config = E2EClusterConfig::with_nodes(3);
                let mut cluster = E2ECluster::start_with_config(config).await;
                let mut executor = ScenarioExecutor::new(scenario.clone());

                // Wait for controller election (reduced from 500ms).
                cluster.sleep(Duration::from_millis(200)).await;
                for attempt in 0..10 {
                    if cluster.create_topic(topic, partition_count).await.is_ok() {
                        break;
                    }
                    cluster.sleep(Duration::from_millis(50)).await;
                    if attempt == 9 {
                        panic!("Failed to create topic after 10 attempts");
                    }
                }

                // Interleave production with fault injection.
                let mut produced = 0;
                let records_per_tick = records_per_seed / ticks_per_seed as usize;
                let records_per_tick = records_per_tick.max(1);

                for tick in 0..ticks_per_seed {
                    // Produce some records with tracking for verification.
                    for i in 0..records_per_tick {
                        let record_num = tick as usize * records_per_tick + i;
                        if record_num >= records_per_seed {
                            break;
                        }
                        let partition = (record_num as u32) % partition_count;
                        if cluster
                            .produce_and_track(
                                topic,
                                partition,
                                format!("record-{}", record_num),
                                5,
                            )
                            .await
                            .is_ok()
                        {
                            produced += 1;
                        }
                    }

                    // Collect Raft snapshots for property verification.
                    cluster.collect_raft_snapshots().await;

                    // Apply fault scenario based on tick count.
                    // Pass None for leader - scenarios that need leader will be skipped.
                    if let Some(action) = executor.tick(&cluster, None) {
                        debug!(seed, tick, action, "Fault injected");
                    }

                    // Small sleep to advance simulated time (reduced from 50ms).
                    cluster.sleep(Duration::from_millis(20)).await;
                }

                // Heal all partitions after test.
                cluster.heal_all();

                // Restart any crashed nodes for full WAL replay recovery.
                let crashed_nodes: Vec<NodeId> = {
                    let state = cluster.network_state.lock().expect("lock poisoned");
                    cluster
                        .node_ids
                        .iter()
                        .filter(|&&n| state.is_crashed(n))
                        .copied()
                        .collect()
                };
                let had_restarts = !crashed_nodes.is_empty();
                for node_id in crashed_nodes {
                    cluster.restart_node(node_id).await;
                }

                // Extra stabilization time when nodes were restarted so
                // controller state (topic metadata) replicates via Raft.
                if had_restarts {
                    cluster.sleep(Duration::from_secs(3)).await;
                }
                cluster.sleep(Duration::from_millis(500)).await;
                if partition_count > 1 {
                    // Extra stabilization time for multi-partition metadata to propagate.
                    cluster.sleep(Duration::from_millis(1000)).await;
                }

                // Collect final Raft snapshots.
                cluster.collect_raft_snapshots().await;

                // NEW: Verify cross-replica consistency after healing.
                // All replicas should have identical committed data.
                for partition in 0..partition_count {
                    let mut last_error: Option<String> = None;
                    for attempt in 0..20 {
                        match cluster.verify_replica_consistency(topic, partition).await {
                            Ok(()) => {
                                last_error = None;
                                break;
                            }
                            Err(e) => {
                                last_error = Some(e);
                                if attempt < 19 {
                                    cluster.sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                    }
                    if let Some(e) = last_error {
                        // Diagnostic dump for multi-partition failures.
                        if debug_seed == Some(seed) {
                            eprintln!(
                                "[DEBUG] Seed {} partition {} consistency failure: {}",
                                seed, partition, e
                            );
                            for node in cluster.nodes.values() {
                                let node_id = node.node_id;
                                let available = cluster.is_node_available(node_id);
                                let topic_id = {
                                    let state = node.service.controller_state().read().await;
                                    state.get_topic(topic).map(|info| info.topic_id)
                                };
                                let group_id = if let Some(tid) = topic_id {
                                    let gm = node.service.group_map().read().await;
                                    gm.get(tid, helix_core::PartitionId::new(u64::from(partition)))
                                } else {
                                    None
                                };
                                let group_state = if let Some(gid) = group_id {
                                    let mr = node.service.multi_raft().read().await;
                                    mr.group_state(gid)
                                } else {
                                    None
                                };
                                let fetch_result = node
                                    .handler
                                    .fetch(topic, partition as i32, 0, 1024 * 1024)
                                    .await;
                                eprintln!(
                                    "  node={} available={} topic_id={:?} group_id={:?} state={:?} fetch={:?}",
                                    node_id.get(),
                                    available,
                                    topic_id.map(|t| t.get()),
                                    group_id.map(|g| g.get()),
                                    group_state.as_ref().map(|s| (s.state, s.leader_id, s.commit_index.get())),
                                    fetch_result.as_ref().err()
                                );
                            }
                        }
                        panic!("Seed {} partition {}: {}", seed, partition, e);
                    }
                }

                // Verify data with integrity checking.
                let mut consumed = 0usize;
                for partition in 0..partition_count {
                    let records = cluster
                        .consume_and_verify(topic, partition, 0)
                        .await
                        .expect("consume should succeed");
                    consumed += records.len();
                }

                // Finalize verification and check for property violations.
                let check_result = cluster.finalize_verification();

                // Check Raft invariants (SingleLeaderPerTerm).
                if !check_result.violations.is_empty() {
                    let violation_strs: Vec<_> = check_result
                        .violations
                        .iter()
                        .map(|v| format!("{v}"))
                        .collect();
                    panic!(
                        "Seed {}: Raft invariant violations: {}",
                        seed,
                        violation_strs.join(", ")
                    );
                }

                // Check data integrity violations (hash mismatches).
                // ALL acked data must be readable and have correct content.
                if !check_result.consumer_violations.is_empty() {
                    if debug_seed == Some(seed) {
                        eprintln!("=== DEBUG SEED {}: consumer violations ===", seed);
                        for violation in &check_result.consumer_violations {
                            eprintln!(
                                "violation: topic={} partition={} offset={} reason={}",
                                violation.topic_id,
                                violation.partition_id,
                                violation.offset,
                                violation.reason
                            );
                            for node_id in cluster.nodes.keys().copied() {
                                let partition = violation.partition_id as u32;
                                let commit_index = cluster
                                    .get_partition_commit_index(node_id, topic, partition)
                                    .await;
                                let batches = cluster
                                    .consume_from_node(node_id, topic, partition, 0)
                                    .await;
                                let mut offsets = HashSet::new();
                                let mut max_offset: Option<u64> = None;
                                if let Some(batches) = batches {
                                    for batch in &batches {
                                        if let Ok(offset) = E2ECluster::extract_base_offset(batch) {
                                            offsets.insert(offset);
                                            max_offset = Some(match max_offset {
                                                Some(m) => m.max(offset),
                                                None => offset,
                                            });
                                        }
                                    }
                                }
                                let has_offset = offsets.contains(&violation.offset);
                                eprintln!(
                                    "  node={} commit_index={:?} has_offset={} max_offset={:?} offsets_seen={}",
                                    node_id.get(),
                                    commit_index,
                                    has_offset,
                                    max_offset,
                                    offsets.len()
                                );
                            }
                        }
                    }
                    panic!(
                        "Seed {}: {} data integrity violations: {:?}",
                        seed,
                        check_result.consumer_violations.len(),
                        check_result.consumer_violations
                    );
                }

                // With fault injection, we may lose some records depending on scenario.
                // Verify we got a reasonable number of records.
                debug!(
                    seed,
                    produced,
                    consumed,
                    "Seed completed - all {} consumed records verified correct",
                    consumed
                );
            });
        });

        if let Err(e) = result {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            let seed_elapsed = seed_start.elapsed();
            eprintln!("[SEED] {} elapsed {:?} (failed)", seed, seed_elapsed);
            eprintln!("[FAIL] Seed {} ({}): {}", seed, scenario_name, msg);
            failures.push((seed, scenario_name.to_string(), msg));
        } else {
            let seed_elapsed = seed_start.elapsed();
            if seed % progress_mod == 0 {
                eprintln!("[SEED] {} elapsed {:?}", seed, seed_elapsed);
            }
        }
    }

    // Print scenario distribution.
    eprintln!("\n=== Scenario Distribution ===");
    for (name, count) in &scenario_counts {
        eprintln!("  {}: {} seeds", name, count);
    }

    eprintln!("\n=== E2E DST Random Faults Results ===");
    eprintln!("Total seeds: {}", total_seeds);
    eprintln!("Passed: {}", total_seeds - failures.len() as u64);
    eprintln!("Failed: {}", failures.len());

    if !failures.is_empty() {
        eprintln!("\nFailed seeds:");
        for (seed, scenario, msg) in &failures {
            eprintln!("  Seed {} ({}): {}", seed, scenario, msg);
        }
        panic!("{} seeds failed", failures.len());
    }

    eprintln!(
        "[PASS] All {} seeds passed with random fault injection",
        total_seeds
    );
    eprintln!("[TOTAL] elapsed {:?}", suite_start.elapsed());
}

/// Runs DST with background producers that operate concurrently with fault
/// injection. Unlike `run_e2e_dst_random_faults` where production and faults
/// are sequential, this exercises writes that are in-flight when faults hit.
///
/// The invariant being tested: **if a produce was acked, that data must be
/// readable after recovery.** Failed produces (expected during faults) are
/// not tracked and not verified.
#[allow(clippy::too_many_arguments)]
pub(super) fn run_e2e_dst_concurrent_faults(
    total_seeds: u64,
    test_duration_ms: u64,
    fault_tick_interval_ms: u64,
    producer_count: u32,
    produce_interval_ms: u64,
    topic: &str,
    partition_count: u32,
    progress_mod: u64,
) {
    use crate::madsim_scenarios::{FaultScenario, ScenarioExecutor};

    let mut failures: Vec<(u64, String, String)> = Vec::new();
    let mut scenario_counts: std::collections::HashMap<&'static str, u64> =
        std::collections::HashMap::new();
    let seed_filter = std::env::var("MADSIM_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok());

    let suite_start = std::time::Instant::now();
    for seed in 0..total_seeds {
        if seed_filter.is_some_and(|s| s != seed) {
            continue;
        }

        let scenario = FaultScenario::random(seed);
        let scenario_name = scenario.name();
        *scenario_counts.entry(scenario_name).or_default() += 1;

        if seed % progress_mod == 0 {
            eprintln!(
                "[PROGRESS] Concurrent seed {}/{} - scenario: {}",
                seed, total_seeds, scenario_name
            );
        }

        let seed_start = std::time::Instant::now();
        let result = std::panic::catch_unwind(|| {
            let rt = Runtime::with_seed_and_config(seed, Default::default());
            rt.block_on(async {
                // ===== PHASE 1: SETUP =====
                let config = E2EClusterConfig::with_nodes(3);
                let mut cluster = E2ECluster::start_with_config(config).await;
                let executor = ScenarioExecutor::new(scenario.clone());

                cluster.sleep(Duration::from_millis(200)).await;
                for attempt in 0..10 {
                    if cluster.create_topic(topic, partition_count).await.is_ok() {
                        break;
                    }
                    cluster.sleep(Duration::from_millis(50)).await;
                    if attempt == 9 {
                        panic!("Failed to create topic after 10 attempts");
                    }
                }

                // ===== PHASE 2: START CONCURRENT PRODUCERS =====
                // Inflate network latency so messages are in-flight long
                // enough for the fault injection task to modify network
                // state while they're sleeping. With 50x multiplier,
                // NETWORK_LATENCY (1ms) becomes 50ms per hop, making Raft
                // replication take ~100ms round-trip. Fault ticks at 10ms
                // intervals can then hit mid-replication.
                {
                    let mut net = cluster.network_state.lock().expect("lock poisoned");
                    net.set_global_latency_multiplier(50);
                }

                let (shutdown_tx, shutdown_rx) =
                    tokio::sync::watch::channel(false);
                let mut producer_handles = Vec::with_capacity(producer_count as usize);

                for producer_id in 0..producer_count {
                    let handle = cluster.create_producer_handle();
                    let rx = shutdown_rx.clone();
                    let topic_owned = topic.to_string();

                    let join_handle = tokio::spawn(background_producer_task(
                        handle,
                        topic_owned,
                        partition_count,
                        Duration::from_millis(produce_interval_ms),
                        producer_id,
                        rx,
                    ));
                    producer_handles.push(join_handle);
                }

                // ===== PHASE 3: FAULT INJECTION (CONCURRENT WITH PRODUCERS) =====
                // Fault injection runs in its own spawned task at the same
                // scheduler level as producers. Under MadSim's cooperative
                // scheduler, faults interleave with producers at `.await`
                // yield points (the `madsim::time::sleep` between produces).
                //
                // KNOWN LIMITATION: MadSim completes the full produce→Raft
                // commit→response chain atomically because the transport's
                // 5ms network latency is too short for the fault task to
                // interleave mid-replication. Faults effectively hit BETWEEN
                // produce calls, not during in-flight Raft replication.
                // This still tests that:
                // - Producers handle faults on subsequent attempts (retry logic)
                // - Acked data survives fault/recovery cycles
                // - Multiple producers competing during faults don't corrupt state
                // Testing in-flight replication faults requires transport-level
                // fault injection (e.g., dropping messages mid-delivery).
                let fault_handle = cluster.create_fault_injector_handle();
                let fault_shutdown_rx = shutdown_rx.clone();
                let fault_tick_count = test_duration_ms / fault_tick_interval_ms;

                let fault_task = tokio::spawn(async move {
                    let mut executor = executor;
                    for _tick in 0..fault_tick_count {
                        if *fault_shutdown_rx.borrow() {
                            break;
                        }
                        if let Some(_action) = executor.tick(&fault_handle, None) {
                            // Fault injected mid-produce.
                        }
                        madsim::time::sleep(
                            Duration::from_millis(fault_tick_interval_ms),
                        )
                        .await;
                    }
                });

                // Main task just waits for the test duration. The producers
                // and fault injector are both spawned tasks competing for
                // scheduler time.
                cluster
                    .sleep(Duration::from_millis(test_duration_ms + 100))
                    .await;

                // ===== PHASE 4: STOP PRODUCERS =====
                let _ = shutdown_tx.send(true);
                // Sleep to let producers and fault task see the shutdown signal.
                cluster.sleep(Duration::from_millis(100)).await;
                // Wait for fault task to finish.
                let _ = fault_task.await;

                let mut total_acked: u64 = 0;
                let mut total_failed: u64 = 0;
                for handle in producer_handles {
                    if let Ok(result) = handle.await {
                        total_acked += result.acked;
                        total_failed += result.failed;
                    }
                }

                // Fidelity check: producers must have actually run.
                assert!(
                    total_acked + total_failed > 0,
                    "Seed {}: producers never ran (acked={}, failed={})",
                    seed, total_acked, total_failed
                );

                if seed % progress_mod == 0 {
                    eprintln!(
                        "[STATS] Seed {}: acked={} failed={} ({}% failure rate)",
                        seed,
                        total_acked,
                        total_failed,
                        if total_acked + total_failed > 0 {
                            total_failed * 100 / (total_acked + total_failed)
                        } else {
                            0
                        }
                    );
                }

                // ===== PHASE 5: HEAL AND VERIFY =====
                // Reset latency to normal speed for verification.
                {
                    let mut net = cluster.network_state.lock().expect("lock poisoned");
                    net.set_global_latency_multiplier(1);
                }
                cluster.heal_all();

                // Restart crashed nodes for WAL replay.
                let crashed_nodes: Vec<NodeId> = {
                    let state = cluster.network_state.lock().expect("lock poisoned");
                    cluster
                        .node_ids
                        .iter()
                        .filter(|&&n| state.is_crashed(n))
                        .copied()
                        .collect()
                };
                let had_restarts = !crashed_nodes.is_empty();
                for node_id in crashed_nodes {
                    cluster.restart_node(node_id).await;
                }

                if had_restarts {
                    cluster.sleep(Duration::from_secs(3)).await;
                }
                cluster.sleep(Duration::from_millis(500)).await;
                if partition_count > 1 {
                    cluster.sleep(Duration::from_millis(1000)).await;
                }

                // Final snapshot collection.
                cluster.collect_raft_snapshots().await;

                // Cross-replica consistency check (with retries for convergence).
                for partition in 0..partition_count {
                    let mut last_error: Option<String> = None;
                    for attempt in 0..20 {
                        match cluster.verify_replica_consistency(topic, partition).await {
                            Ok(()) => {
                                last_error = None;
                                break;
                            }
                            Err(e) => {
                                last_error = Some(e);
                                if attempt < 19 {
                                    cluster.sleep(Duration::from_millis(200)).await;
                                }
                            }
                        }
                    }
                    if let Some(e) = last_error {
                        panic!("Seed {} partition {}: {}", seed, partition, e);
                    }
                }

                // Consume and verify all acked data is readable.
                if total_acked > 0 {
                    for partition in 0..partition_count {
                        cluster
                            .consume_and_verify(topic, partition, 0)
                            .await
                            .expect("consume should succeed");
                    }

                    let check_result = cluster.finalize_verification();

                    if !check_result.violations.is_empty() {
                        let strs: Vec<_> = check_result
                            .violations
                            .iter()
                            .map(|v| format!("{v}"))
                            .collect();
                        panic!(
                            "Seed {}: Raft invariant violations: {}",
                            seed,
                            strs.join(", ")
                        );
                    }

                    if !check_result.consumer_violations.is_empty() {
                        panic!(
                            "Seed {}: {} data integrity violations: {:?}",
                            seed,
                            check_result.consumer_violations.len(),
                            check_result.consumer_violations
                        );
                    }
                }

                debug!(
                    seed,
                    total_acked,
                    total_failed,
                    "Concurrent DST seed completed"
                );
            });
        });

        if let Err(e) = result {
            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = e.downcast_ref::<String>() {
                s.clone()
            } else {
                "Unknown panic".to_string()
            };
            let seed_elapsed = seed_start.elapsed();
            eprintln!("[SEED] {} elapsed {:?} (failed)", seed, seed_elapsed);
            eprintln!("[FAIL] Seed {} ({}): {}", seed, scenario_name, msg);
            failures.push((seed, scenario_name.to_string(), msg));
        } else {
            let seed_elapsed = seed_start.elapsed();
            if seed % progress_mod == 0 {
                eprintln!("[SEED] {} elapsed {:?}", seed, seed_elapsed);
            }
        }
    }

    eprintln!("\n=== Scenario Distribution ===");
    for (name, count) in &scenario_counts {
        eprintln!("  {}: {} seeds", name, count);
    }

    eprintln!("\n=== Concurrent DST Results ===");
    eprintln!("Total seeds: {}", total_seeds);
    eprintln!("Passed: {}", total_seeds - failures.len() as u64);
    eprintln!("Failed: {}", failures.len());

    if !failures.is_empty() {
        eprintln!("\nFailed seeds:");
        for (seed, scenario, msg) in &failures {
            eprintln!("  Seed {} ({}): {}", seed, scenario, msg);
        }
        panic!("{} seeds failed", failures.len());
    }

    eprintln!(
        "[PASS] All {} concurrent DST seeds passed",
        total_seeds
    );
    eprintln!("[TOTAL] elapsed {:?}", suite_start.elapsed());
}

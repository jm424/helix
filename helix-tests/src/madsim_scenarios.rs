//! MadSim Fault Scenarios for DST.
//!
//! This module defines reusable fault scenarios for deterministic simulation testing.
//! Each scenario represents a specific failure mode that can occur in distributed systems.
//!
//! # Usage
//!
//! ```ignore
//! let scenario = FaultScenario::LeaderCrash { after_ticks: 10 };
//! let mut executor = ScenarioExecutor::new(scenario);
//! executor.tick(&mut cluster, leader);
//! ```
//!
//! # Generic Cluster Support
//!
//! The `ScenarioExecutor` works with any cluster implementing `FaultInjectable`,
//! such as `E2ECluster` for full service testing under MadSim.

#![cfg(feature = "madsim")]

use helix_core::NodeId;
use helix_wal::FaultConfig;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;

// ============================================================================
// Fault Injectable Trait
// ============================================================================

/// Trait for cluster types that support fault injection.
///
/// This trait abstracts the fault injection interface so `ScenarioExecutor`
/// can work with any cluster implementation (e.g., `E2ECluster`).
pub trait FaultInjectable {
    /// Returns the node IDs in this cluster.
    fn node_ids(&self) -> &[NodeId];

    /// Partitions the given nodes from each other.
    fn partition(&self, nodes: &[NodeId]);

    /// Heals the partition between the given nodes.
    fn heal(&self, nodes: &[NodeId]);

    /// Crashes a node (stops it from receiving/sending messages).
    fn crash_node(&self, node_id: NodeId);

    /// Recovers a crashed node.
    fn recover_node(&self, node_id: NodeId);

    /// Sets storage fault configuration on a node.
    fn set_storage_faults(&self, node_id: NodeId, config: FaultConfig);

    /// Sets the network latency multiplier for a node.
    ///
    /// Messages TO this node will have their delivery latency multiplied
    /// by this factor. A value of 1 means normal latency.
    /// Default implementation is a no-op for backward compatibility.
    fn set_node_latency(&self, _node_id: NodeId, _multiplier: u32) {}
}

// ============================================================================
// Fault Scenarios
// ============================================================================

/// A fault scenario for DST testing.
///
/// Each variant represents a specific failure mode that can occur in
/// distributed systems. Scenarios can be applied to a cluster to inject
/// the corresponding faults.
#[derive(Clone, Debug)]
pub enum FaultScenario {
    /// No faults - baseline test for correctness without failures.
    NoFaults,

    /// Leader crashes after a specified number of ticks.
    ///
    /// Tests: Leader failover, election safety, log consistency.
    LeaderCrash {
        /// Number of ticks before crash.
        after_ticks: u32,
    },

    /// Follower crashes after a specified number of ticks.
    ///
    /// Tests: Replication continues with remaining nodes, recovery.
    FollowerCrash {
        /// Which follower to crash (1-indexed within followers).
        follower_index: usize,
        /// Number of ticks before crash.
        after_ticks: u32,
    },

    /// Minority partition - one node isolated from the others.
    ///
    /// Tests: Cluster continues operating, isolated node cannot elect itself.
    MinorityPartition {
        /// The node to isolate.
        isolated_node: NodeId,
        /// Duration in ticks before healing.
        duration_ticks: u32,
    },

    /// Majority partition - cluster splits such that no partition has quorum.
    ///
    /// In a 3-node cluster, this partitions node 1 from nodes 2 and 3,
    /// AND partitions node 2 from node 3. No node can get majority.
    ///
    /// Tests: Cluster correctly stalls, no split-brain.
    MajorityPartition {
        /// Duration in ticks before healing.
        duration_ticks: u32,
    },

    /// Asymmetric partition - A can reach B, B can reach C, but A cannot reach C.
    ///
    /// Tests: Message routing, potential leader confusion.
    AsymmetricPartition {
        /// Duration in ticks before healing.
        duration_ticks: u32,
    },

    /// Network partition that heals and re-partitions multiple times.
    ///
    /// Tests: Repeated partition/heal cycles, state consistency.
    FlappingPartition {
        /// Nodes involved in the partition.
        nodes: Vec<NodeId>,
        /// Duration of each partition in ticks.
        partition_duration: u32,
        /// Duration of each heal in ticks.
        heal_duration: u32,
        /// Number of flap cycles.
        cycles: u32,
    },

    /// Storage fault on a specific node (e.g., fsync failures).
    ///
    /// Tests: Handling of storage errors, recovery.
    StorageFault {
        /// The node with storage faults.
        node: NodeId,
        /// The fault configuration.
        fault_config: FaultConfig,
    },

    /// Storage fault specifically on the leader.
    ///
    /// Tests: Leader demotion on storage failure, safe failover.
    LeaderStorageFault {
        /// The fault configuration.
        fault_config: FaultConfig,
    },

    /// Slow follower with delayed message delivery.
    ///
    /// Tests: Progress with slow nodes, leader doesn't wait indefinitely.
    SlowFollower {
        /// The slow node.
        slow_node: NodeId,
        /// Message delay multiplier (e.g., 10 = 10x normal latency).
        delay_multiplier: u32,
    },

    /// Random faults generated from seed for reproducibility.
    ///
    /// Tests: General robustness under chaotic conditions.
    RandomFaults {
        /// Probability of fault per tick (0.0 - 1.0).
        fault_rate: f64,
        /// Random seed for reproducibility.
        seed: u64,
    },

    /// Combined scenario: leader crash followed by partition.
    ///
    /// Tests: Complex failure sequences.
    LeaderCrashThenPartition {
        /// Ticks before leader crash.
        crash_after_ticks: u32,
        /// Ticks after crash before partition.
        partition_after_crash_ticks: u32,
        /// Duration of partition.
        partition_duration_ticks: u32,
    },

    /// Node recovery after crash.
    ///
    /// Tests: State recovery, log catch-up.
    CrashAndRecover {
        /// Node to crash.
        node: NodeId,
        /// Ticks before crash.
        crash_after_ticks: u32,
        /// Ticks after crash before recovery.
        recover_after_ticks: u32,
    },
}

impl FaultScenario {
    /// Generates a random scenario from a seed.
    ///
    /// The same seed always produces the same scenario for reproducibility.
    #[must_use]
    pub fn random(seed: u64) -> Self {
        let mut rng = ChaChaRng::seed_from_u64(seed);

        match rng.gen_range(0..13) {
            0 => Self::NoFaults,
            1 => Self::LeaderCrash {
                after_ticks: rng.gen_range(5..50),
            },
            2 => Self::FollowerCrash {
                follower_index: rng.gen_range(0..2),
                after_ticks: rng.gen_range(5..50),
            },
            3 => Self::MinorityPartition {
                isolated_node: NodeId::new(rng.gen_range(1..=3)),
                duration_ticks: rng.gen_range(10..50),
            },
            4 => Self::MajorityPartition {
                duration_ticks: rng.gen_range(10..30),
            },
            5 => Self::AsymmetricPartition {
                duration_ticks: rng.gen_range(10..30),
            },
            6 => Self::FlappingPartition {
                nodes: vec![NodeId::new(1), NodeId::new(2)],
                partition_duration: rng.gen_range(5..15),
                heal_duration: rng.gen_range(5..15),
                cycles: rng.gen_range(2..5),
            },
            // Storage fault with random fault type selection.
            7 => {
                let fault_type = rng.gen_range(0..4);
                let fault_config = match fault_type {
                    0 => FaultConfig {
                        fsync_fail_rate: rng.gen_range(0.1..0.5),
                        ..Default::default()
                    },
                    1 => FaultConfig {
                        write_fail_rate: rng.gen_range(0.1..0.3),
                        ..Default::default()
                    },
                    2 => FaultConfig {
                        read_fail_rate: rng.gen_range(0.1..0.3),
                        ..Default::default()
                    },
                    _ => FaultConfig {
                        torn_write_rate: rng.gen_range(0.1..0.3),
                        ..Default::default()
                    },
                };
                Self::StorageFault {
                    node: NodeId::new(rng.gen_range(1..=3)),
                    fault_config,
                }
            }
            // Leader storage fault - targets current leader.
            8 => {
                let fault_type = rng.gen_range(0..4);
                let fault_config = match fault_type {
                    0 => FaultConfig {
                        fsync_fail_rate: rng.gen_range(0.2..0.6),
                        ..Default::default()
                    },
                    1 => FaultConfig {
                        write_fail_rate: rng.gen_range(0.2..0.5),
                        ..Default::default()
                    },
                    2 => FaultConfig {
                        read_fail_rate: rng.gen_range(0.1..0.3),
                        ..Default::default()
                    },
                    _ => FaultConfig {
                        torn_write_rate: rng.gen_range(0.1..0.3),
                        ..Default::default()
                    },
                };
                Self::LeaderStorageFault { fault_config }
            }
            9 => Self::SlowFollower {
                slow_node: NodeId::new(rng.gen_range(1..=3)),
                delay_multiplier: rng.gen_range(5..20),
            },
            10 => Self::RandomFaults {
                fault_rate: rng.gen_range(0.01..0.1),
                seed,
            },
            11 => Self::CrashAndRecover {
                node: NodeId::new(rng.gen_range(1..=3)),
                crash_after_ticks: rng.gen_range(10..30),
                recover_after_ticks: rng.gen_range(10..30),
            },
            _ => Self::LeaderCrashThenPartition {
                crash_after_ticks: rng.gen_range(10..30),
                partition_after_crash_ticks: rng.gen_range(5..15),
                partition_duration_ticks: rng.gen_range(10..30),
            },
        }
    }

    /// Returns a human-readable name for the scenario.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::NoFaults => "no_faults",
            Self::LeaderCrash { .. } => "leader_crash",
            Self::FollowerCrash { .. } => "follower_crash",
            Self::MinorityPartition { .. } => "minority_partition",
            Self::MajorityPartition { .. } => "majority_partition",
            Self::AsymmetricPartition { .. } => "asymmetric_partition",
            Self::FlappingPartition { .. } => "flapping_partition",
            Self::StorageFault { .. } => "storage_fault",
            Self::LeaderStorageFault { .. } => "leader_storage_fault",
            Self::SlowFollower { .. } => "slow_follower",
            Self::RandomFaults { .. } => "random_faults",
            Self::LeaderCrashThenPartition { .. } => "leader_crash_then_partition",
            Self::CrashAndRecover { .. } => "crash_and_recover",
        }
    }
}

// ============================================================================
// Scenario Executor
// ============================================================================

/// Tracks the state of a running scenario.
pub struct ScenarioExecutor {
    /// The scenario being executed.
    scenario: FaultScenario,
    /// Current tick count.
    tick_count: u32,
    /// Whether the initial fault has been applied.
    fault_applied: bool,
    /// Whether the fault has been healed (for partition scenarios).
    fault_healed: bool,
    /// Additional state for complex scenarios.
    phase: u32,
    /// RNG for random fault scenarios.
    rng: Option<ChaChaRng>,
}

impl ScenarioExecutor {
    /// Creates a new executor for the given scenario.
    #[must_use]
    pub fn new(scenario: FaultScenario) -> Self {
        let rng = if let FaultScenario::RandomFaults { seed, .. } = &scenario {
            Some(ChaChaRng::seed_from_u64(*seed))
        } else {
            None
        };

        Self {
            scenario,
            tick_count: 0,
            fault_applied: false,
            fault_healed: false,
            phase: 0,
            rng,
        }
    }

    /// Advances the scenario by one tick, applying faults as needed.
    ///
    /// Works with any cluster implementing `FaultInjectable`.
    ///
    /// Returns a description of any action taken, or None if no action.
    pub fn tick<C: FaultInjectable>(
        &mut self,
        cluster: &C,
        leader: Option<NodeId>,
    ) -> Option<String> {
        self.tick_count += 1;

        match &self.scenario {
            FaultScenario::NoFaults => None,

            FaultScenario::LeaderCrash { after_ticks } => {
                if !self.fault_applied && self.tick_count >= *after_ticks {
                    if let Some(leader_id) = leader {
                        cluster.crash_node(leader_id);
                        self.fault_applied = true;
                        return Some(format!("Crashed leader node {}", leader_id.get()));
                    }
                }
                None
            }

            FaultScenario::FollowerCrash {
                follower_index,
                after_ticks,
            } => {
                if !self.fault_applied && self.tick_count >= *after_ticks {
                    // Get non-leader nodes.
                    let followers: Vec<NodeId> = cluster
                        .node_ids()
                        .iter()
                        .copied()
                        .filter(|&n| Some(n) != leader)
                        .collect();

                    if let Some(&follower) = followers.get(*follower_index) {
                        cluster.crash_node(follower);
                        self.fault_applied = true;
                        return Some(format!("Crashed follower node {}", follower.get()));
                    }
                }
                None
            }

            FaultScenario::MinorityPartition {
                isolated_node,
                duration_ticks,
            } => {
                if !self.fault_applied {
                    // Partition the isolated node from all others.
                    let others: Vec<NodeId> = cluster
                        .node_ids()
                        .iter()
                        .copied()
                        .filter(|&n| n != *isolated_node)
                        .collect();

                    for other in &others {
                        cluster.partition(&[*isolated_node, *other]);
                    }
                    self.fault_applied = true;
                    return Some(format!(
                        "Isolated node {} from cluster",
                        isolated_node.get()
                    ));
                }

                if self.fault_applied && !self.fault_healed && self.tick_count >= *duration_ticks {
                    // Heal the partition.
                    let others: Vec<NodeId> = cluster
                        .node_ids()
                        .iter()
                        .copied()
                        .filter(|&n| n != *isolated_node)
                        .collect();

                    for other in &others {
                        cluster.heal(&[*isolated_node, *other]);
                    }
                    self.fault_healed = true;
                    return Some(format!("Healed partition for node {}", isolated_node.get()));
                }
                None
            }

            FaultScenario::MajorityPartition { duration_ticks } => {
                if !self.fault_applied {
                    // Partition all nodes from each other (no quorum possible).
                    let nodes = cluster.node_ids().to_vec();
                    for i in 0..nodes.len() {
                        for j in (i + 1)..nodes.len() {
                            cluster.partition(&[nodes[i], nodes[j]]);
                        }
                    }
                    self.fault_applied = true;
                    return Some("Created majority partition (no quorum)".to_string());
                }

                if self.fault_applied && !self.fault_healed && self.tick_count >= *duration_ticks {
                    // Heal all partitions.
                    let nodes = cluster.node_ids().to_vec();
                    for i in 0..nodes.len() {
                        for j in (i + 1)..nodes.len() {
                            cluster.heal(&[nodes[i], nodes[j]]);
                        }
                    }
                    self.fault_healed = true;
                    return Some("Healed majority partition".to_string());
                }
                None
            }

            FaultScenario::AsymmetricPartition { duration_ticks } => {
                if !self.fault_applied {
                    // A-B connected, B-C connected, A-C partitioned.
                    // In a 3-node cluster: 1-2 ok, 2-3 ok, 1-3 partitioned.
                    cluster.partition(&[NodeId::new(1), NodeId::new(3)]);
                    self.fault_applied = true;
                    return Some("Created asymmetric partition (1-3)".to_string());
                }

                if self.fault_applied && !self.fault_healed && self.tick_count >= *duration_ticks {
                    cluster.heal(&[NodeId::new(1), NodeId::new(3)]);
                    self.fault_healed = true;
                    return Some("Healed asymmetric partition".to_string());
                }
                None
            }

            FaultScenario::FlappingPartition {
                nodes,
                partition_duration,
                heal_duration,
                cycles,
            } => {
                let cycle_length = partition_duration + heal_duration;
                let current_cycle = self.tick_count / cycle_length;
                let position_in_cycle = self.tick_count % cycle_length;

                if current_cycle >= *cycles {
                    return None; // Done with all cycles.
                }

                let is_partitioned = position_in_cycle < *partition_duration;
                let was_partitioned = self.phase == 1;

                if is_partitioned && !was_partitioned {
                    cluster.partition(nodes);
                    self.phase = 1;
                    return Some(format!("Flap cycle {}: partition", current_cycle + 1));
                }

                if !is_partitioned && was_partitioned {
                    cluster.heal(nodes);
                    self.phase = 0;
                    return Some(format!("Flap cycle {}: heal", current_cycle + 1));
                }

                None
            }

            FaultScenario::StorageFault { node, fault_config } => {
                if !self.fault_applied {
                    cluster.set_storage_faults(*node, fault_config.clone());
                    self.fault_applied = true;
                    return Some(format!("Applied storage faults to node {}", node.get()));
                }
                None
            }

            FaultScenario::LeaderStorageFault { fault_config } => {
                if !self.fault_applied {
                    if let Some(leader_id) = leader {
                        cluster.set_storage_faults(leader_id, fault_config.clone());
                        self.fault_applied = true;
                        return Some(format!(
                            "Applied storage faults to leader {}",
                            leader_id.get()
                        ));
                    }
                }
                None
            }

            FaultScenario::SlowFollower {
                slow_node,
                delay_multiplier,
            } => {
                if !self.fault_applied {
                    cluster.set_node_latency(*slow_node, *delay_multiplier);
                    self.fault_applied = true;
                    return Some(format!(
                        "SlowFollower: node {} latency {}x",
                        slow_node.get(),
                        delay_multiplier
                    ));
                }
                None
            }

            FaultScenario::RandomFaults { fault_rate, .. } => {
                if let Some(ref mut rng) = self.rng {
                    let roll: f64 = rng.gen();
                    if roll < *fault_rate {
                        // Random fault: 50% partition, 50% crash.
                        let nodes = cluster.node_ids().to_vec();
                        let node = nodes[rng.gen_range(0..nodes.len())];

                        if rng.gen_bool(0.5) {
                            // Partition.
                            let other = nodes[rng.gen_range(0..nodes.len())];
                            if node != other {
                                cluster.partition(&[node, other]);
                                return Some(format!(
                                    "Random partition: {}-{}",
                                    node.get(),
                                    other.get()
                                ));
                            }
                        } else {
                            // Brief crash (immediate recovery to avoid deadlock).
                            cluster.crash_node(node);
                            cluster.recover_node(node);
                            return Some(format!("Random crash/recover: {}", node.get()));
                        }
                    }
                }
                None
            }

            FaultScenario::CrashAndRecover {
                node,
                crash_after_ticks,
                ..
            } => {
                if !self.fault_applied && self.tick_count >= *crash_after_ticks {
                    cluster.crash_node(*node);
                    self.fault_applied = true;
                    return Some(format!("Crashed node {}", node.get()));
                }

                // Node stays crashed — the test harness detects crashed nodes
                // and calls restart_node() (full WAL replay) during the recovery
                // phase. recover_node() (network-only) is wrong here: it leaves
                // the old HelixService running with an in-memory Raft log that
                // references entries reverted from storage by simulate_crash().
                None
            }

            FaultScenario::LeaderCrashThenPartition {
                crash_after_ticks,
                partition_after_crash_ticks,
                partition_duration_ticks,
            } => {
                // Phase 0: Wait for crash.
                if self.phase == 0 && self.tick_count >= *crash_after_ticks {
                    if let Some(leader_id) = leader {
                        cluster.crash_node(leader_id);
                        self.phase = 1;
                        return Some(format!("Phase 1: Crashed leader {}", leader_id.get()));
                    }
                }

                // Phase 1: Wait for partition.
                if self.phase == 1
                    && self.tick_count >= crash_after_ticks + partition_after_crash_ticks
                {
                    // Partition remaining nodes.
                    let nodes = cluster.node_ids().to_vec();
                    cluster.partition(&nodes[..2.min(nodes.len())]);
                    self.phase = 2;
                    return Some("Phase 2: Created partition".to_string());
                }

                // Phase 2: Wait to heal.
                if self.phase == 2
                    && self.tick_count
                        >= crash_after_ticks
                            + partition_after_crash_ticks
                            + partition_duration_ticks
                {
                    let nodes = cluster.node_ids().to_vec();
                    cluster.heal(&nodes[..2.min(nodes.len())]);
                    self.phase = 3;
                    return Some("Phase 3: Healed partition".to_string());
                }

                None
            }
        }
    }

    /// Returns whether the scenario has completed all its fault injections.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        match &self.scenario {
            FaultScenario::NoFaults => true,
            FaultScenario::LeaderCrash { .. } => self.fault_applied,
            FaultScenario::FollowerCrash { .. } => self.fault_applied,
            FaultScenario::MinorityPartition { .. } => self.fault_healed,
            FaultScenario::MajorityPartition { .. } => self.fault_healed,
            FaultScenario::AsymmetricPartition { .. } => self.fault_healed,
            FaultScenario::FlappingPartition {
                cycles,
                partition_duration,
                heal_duration,
                ..
            } => self.tick_count >= cycles * (partition_duration + heal_duration),
            FaultScenario::StorageFault { .. } => self.fault_applied,
            FaultScenario::LeaderStorageFault { .. } => self.fault_applied,
            FaultScenario::SlowFollower { .. } => self.fault_applied,
            FaultScenario::RandomFaults { .. } => false, // Never "complete" - runs indefinitely.
            FaultScenario::CrashAndRecover { .. } => self.fault_applied,
            FaultScenario::LeaderCrashThenPartition { .. } => self.phase >= 3,
        }
    }

    /// Returns the current tick count.
    #[must_use]
    pub fn tick_count(&self) -> u32 {
        self.tick_count
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, madsim))]
mod tests {
    use super::*;
    use crate::madsim_e2e_cluster::E2ECluster;
    use madsim::runtime::Runtime;

    #[test]
    fn test_scenario_random_determinism() {
        // Same seed should produce same scenario.
        let s1 = FaultScenario::random(42);
        let s2 = FaultScenario::random(42);
        assert_eq!(s1.name(), s2.name());
    }

    #[test]
    fn test_scenario_random_variety() {
        // Different seeds should produce variety.
        let mut names = std::collections::HashSet::new();
        for seed in 0..100 {
            let scenario = FaultScenario::random(seed);
            names.insert(scenario.name());
        }
        // Should have at least 5 different scenario types.
        assert!(names.len() >= 5, "Expected variety in random scenarios");
    }

    #[test]
    fn test_executor_no_faults() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;
            let mut executor = ScenarioExecutor::new(FaultScenario::NoFaults);

            // Should always be complete with no actions.
            assert!(executor.is_complete());
            let action = executor.tick(&cluster, None);
            assert!(action.is_none());
        });
    }

    #[test]
    fn test_executor_minority_partition() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;
            let mut executor = ScenarioExecutor::new(FaultScenario::MinorityPartition {
                isolated_node: NodeId::new(1),
                duration_ticks: 5,
            });

            // First tick should create partition.
            let action = executor.tick(&cluster, None);
            assert!(action.is_some());
            assert!(action.unwrap().contains("Isolated"));

            // Tick until heal.
            for _ in 0..5 {
                executor.tick(&cluster, None);
            }

            // Should be healed now.
            assert!(executor.is_complete());
        });
    }

    #[test]
    fn test_executor_leader_crash() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let cluster = E2ECluster::start(3).await;
            let mut executor = ScenarioExecutor::new(FaultScenario::LeaderCrash { after_ticks: 3 });

            let leader = NodeId::new(1);

            // Tick without action until threshold.
            for _ in 0..2 {
                let action = executor.tick(&cluster, Some(leader));
                assert!(action.is_none());
            }

            // Third tick should crash leader.
            let action = executor.tick(&cluster, Some(leader));
            assert!(action.is_some());
            assert!(action.unwrap().contains("Crashed leader"));

            assert!(executor.is_complete());
        });
    }

    #[test]
    fn test_scenario_names() {
        assert_eq!(FaultScenario::NoFaults.name(), "no_faults");
        assert_eq!(
            FaultScenario::LeaderCrash { after_ticks: 10 }.name(),
            "leader_crash"
        );
        assert_eq!(
            FaultScenario::MinorityPartition {
                isolated_node: NodeId::new(1),
                duration_ticks: 10
            }
            .name(),
            "minority_partition"
        );
    }
}

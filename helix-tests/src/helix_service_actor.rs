//! Helix Service Actor for E2E Deterministic Simulation Testing.
//!
//! This module wraps the real Helix service logic as a Bloodhound `SimulatedActor`,
//! enabling comprehensive fault injection testing with:
//! - Simulated storage (torn writes, fsync failures)
//! - Simulated network (partitions, latency)
//! - Deterministic timing (Bloodhound timer events)
//!
//! # Architecture
//!
//! The actor mirrors the production tick task logic but operates synchronously
//! within Bloodhound's event loop:
//!
//! ```text
//! Production (tick.rs)              Simulation (HelixServiceActor)
//! ─────────────────────             ────────────────────────────────
//! tokio::time::interval()    →      TimerFired events via ctx.set_timer()
//! TransportHandle (TCP)      →      SimulatedTransport (event queue)
//! TokioStorage (filesystem)  →      SimulatedStorage (in-memory + faults)
//! ```

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bloodhound::simulation::discrete::actors::{SimulatedActor, SimulationContext};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, Offset, PartitionId, Record, TopicId};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::{RaftConfig, RaftNode, RaftState};
use tokio::sync::mpsc;
use helix_server::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use helix_server::group_map::GroupMap;
use helix_server::partition_storage::PartitionStorage;
use helix_server::storage::PartitionCommand;
use helix_server::service::{HEARTBEAT_INTERVAL_MS, TICK_INTERVAL_MS};
use helix_server::service::partition_actor::{
    spawn_partition_actor_shared, BatchProposalInfo, GroupedOutput, PartitionActorConfig,
    PartitionActorHandle, PartitionOutput,
};
use helix_wal::{
    Entry, FaultConfig, FaultStats, PoolConfig, SharedEntry, SharedWalPool, SimulatedStorage, Wal,
    WalConfig,
};
use tracing::{debug, error, info, warn};

use crate::properties::{DataIntegrityViolationRecord, HelixNodeSnapshot, SharedHelixPropertyState};
use crate::raft_actor::SharedNetworkState;
use crate::simulated_transport::SimulatedTransport;

// ============================================================================
// DST WAL Wrapper (Sync access to async Wal)
// ============================================================================

/// Synchronous wrapper around `Wal` for DST.
///
/// Uses `futures::executor::block_on` to call async methods synchronously.
/// Safe because `SimulatedStorage` is actually synchronous (in-memory `HashMap`).
pub struct DstWal {
    wal: Wal<SimulatedStorage>,
}

impl DstWal {
    /// Opens or creates a WAL at the given directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL cannot be opened or created.
    pub fn open(storage: SimulatedStorage, dir: &std::path::Path) -> Result<Self, String> {
        let config = WalConfig::new(dir);
        let wal = futures::executor::block_on(Wal::open(storage, config))
            .map_err(|e| format!("WAL open failed: {e}"))?;
        Ok(Self { wal })
    }

    /// Appends an entry to the WAL. Returns the index of the appended entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the append operation fails.
    pub fn append(&mut self, entry: Entry) -> Result<u64, String> {
        futures::executor::block_on(self.wal.append(entry))
            .map_err(|e| format!("WAL append failed: {e}"))
    }

    /// Syncs the WAL to durable storage.
    ///
    /// # Errors
    ///
    /// Returns an error if the sync operation fails.
    pub fn sync(&mut self) -> Result<(), String> {
        futures::executor::block_on(self.wal.sync())
            .map_err(|e| format!("WAL sync failed: {e}"))
    }

    /// Returns the first index in the WAL.
    #[must_use] 
    pub const fn first_index(&self) -> u64 {
        self.wal.first_index()
    }

    /// Returns the last index in the WAL (if any).
    #[must_use] 
    pub const fn last_index(&self) -> Option<u64> {
        self.wal.last_index()
    }

    /// Reads an entry at the given index.
    ///
    /// # Errors
    ///
    /// Returns an error if the entry cannot be read.
    pub fn read(&self, index: u64) -> Result<Entry, String> {
        self.wal.read(index).cloned().map_err(|e| format!("WAL read failed: {e}"))
    }
}

// ============================================================================
// WAL Mode Configuration
// ============================================================================

/// WAL mode for E2E DST tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalMode {
    /// In-memory storage only (no WAL, for fast consensus-only testing).
    #[default]
    InMemory,
    /// Each partition has its own dedicated WAL with `SimulatedStorage` (fault injection).
    PerPartition,
    /// Multiple partitions share a pool of WALs (production `SharedWAL`).
    Shared,
}

// ============================================================================
// DST SharedWalPool Wrapper (Sync access to async SharedWalPool)
// ============================================================================

/// Synchronous wrapper around `SharedWalPool` for DST.
///
/// Uses a dedicated Tokio runtime to call async methods synchronously.
/// This is necessary because `SharedWalCoordinator` spawns background tasks
/// that require a Tokio runtime.
pub struct DstSharedWalPool {
    pool: SharedWalPool<SimulatedStorage>,
    /// Tokio runtime for async operations.
    runtime: tokio::runtime::Runtime,
}

impl DstSharedWalPool {
    /// Opens or creates a pool of shared WALs.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool cannot be opened or created.
    pub fn open(storage: SimulatedStorage, config: PoolConfig) -> Result<Self, String> {
        // Create a multi-threaded runtime for the SharedWalCoordinator background tasks.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create Tokio runtime: {e}"))?;

        let pool = runtime
            .block_on(SharedWalPool::open(storage, config))
            .map_err(|e| format!("SharedWalPool open failed: {e}"))?;

        Ok(Self { pool, runtime })
    }

    /// Gets a handle for a partition.
    ///
    /// The returned handle can be passed to `PartitionStorage::new_durable_with_shared_wal`.
    #[must_use]
    pub fn handle(&self, partition_id: PartitionId) -> helix_wal::SharedWalHandle<SimulatedStorage> {
        self.pool.handle(partition_id)
    }

    /// Recovers entries from all WALs, grouped by partition.
    ///
    /// # Errors
    ///
    /// Returns an error if recovery fails.
    pub fn recover(&self) -> Result<std::collections::HashMap<PartitionId, Vec<SharedEntry>>, String> {
        self.runtime
            .block_on(self.pool.recover())
            .map_err(|e| format!("SharedWal recover failed: {e}"))
    }

    /// Returns the number of WALs in the pool.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Can't be const: calls non-const pool.wal_count().
    pub fn wal_count(&self) -> u32 {
        self.pool.wal_count()
    }

    /// Returns a reference to the Tokio runtime.
    ///
    /// Used for running async operations synchronously in DST context.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)] // Runtime doesn't support const access.
    pub fn runtime(&self) -> &tokio::runtime::Runtime {
        &self.runtime
    }
}

// ============================================================================
// Data Integrity Verification
// ============================================================================

/// A data integrity violation - committed data that is missing or corrupted.
#[derive(Debug, Clone)]
pub struct DataIntegrityViolation {
    /// Node where the violation was detected.
    pub node_id: u64,
    /// Topic ID.
    pub topic_id: u64,
    /// Partition ID.
    pub partition_id: u64,
    /// Offset that was committed.
    pub offset: u64,
    /// Expected hash of the data.
    pub expected_hash: u64,
    /// Actual hash read back (None if data missing).
    pub actual_hash: Option<u64>,
    /// Reason for violation.
    pub reason: String,
}

impl std::fmt::Display for DataIntegrityViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.actual_hash {
            Some(actual) => write!(
                f,
                "Data integrity violation on node {}: topic={}, partition={}, offset={} - \
                 expected hash {:x}, got {:x} ({})",
                self.node_id, self.topic_id, self.partition_id, self.offset,
                self.expected_hash, actual, self.reason
            ),
            None => write!(
                f,
                "Data integrity violation on node {}: topic={}, partition={}, offset={} - \
                 data missing ({})",
                self.node_id, self.topic_id, self.partition_id, self.offset, self.reason
            ),
        }
    }
}

/// Timer IDs for service events.
mod timer_ids {
    /// Tick timer - drives Raft consensus.
    pub const TICK: u64 = 1;
    /// Heartbeat timer - drives broker liveness.
    pub const HEARTBEAT: u64 = 2;
}

/// Custom event names for client operations.
pub mod custom_events {
    /// Produce request: data is serialized `ProduceRequest`.
    pub const PRODUCE: &str = "produce";
    /// Fetch request: data is serialized `FetchRequest`.
    pub const FETCH: &str = "fetch";
    /// Create topic request.
    pub const CREATE_TOPIC: &str = "create_topic";
    /// Collect state for property checking.
    pub const COLLECT_STATE: &str = "collect_state";
    /// Apply network partition.
    pub const APPLY_PARTITION: &str = "apply_partition";
    /// Heal network partition.
    pub const HEAL_PARTITION: &str = "heal_partition";
    /// Verify data integrity - reads back all committed data and verifies hashes.
    pub const VERIFY_INTEGRITY: &str = "verify_integrity";
    /// Verify consumer - reads back all client-ack'd data from any surviving node.
    pub const VERIFY_CONSUMER: &str = "verify_consumer";
}

/// Type alias for partition storage with simulated backend.
pub type SimulatedPartitionStorage = PartitionStorage<SimulatedStorage>;

/// Helix service wrapped as a Bloodhound `SimulatedActor`.
///
/// This actor runs the real Helix service code path against simulated
/// infrastructure for deterministic fault injection testing.
pub struct HelixServiceActor {
    /// Bloodhound actor ID.
    actor_id: ActorId,
    /// Human-readable name.
    name: String,
    /// Helix node ID.
    node_id: NodeId,
    /// All cluster node IDs.
    cluster_nodes: Vec<NodeId>,
    /// Multi-Raft engine for consensus.
    multi_raft: MultiRaft,
    /// Partition storage indexed by group ID.
    partition_storage: BTreeMap<GroupId, SimulatedPartitionStorage>,
    /// Group ID mapping (topic, partition) -> `GroupId`.
    group_map: GroupMap,
    /// Controller state machine.
    controller_state: ControllerState,
    /// Simulated storage backend (shared for crash simulation).
    storage: SimulatedStorage,
    /// Simulated transport for message delivery.
    transport: SimulatedTransport,
    /// Network state for partition simulation.
    network_state: SharedNetworkState,
    /// Whether this node is crashed.
    crashed: bool,
    /// Mapping from `NodeId` to `ActorId`.
    node_to_actor: BTreeMap<NodeId, ActorId>,
    /// Seed for deterministic behavior.
    #[allow(dead_code)] // Used for future random fault injection.
    seed: u64,
    /// Tick counter for time tracking (increments each tick).
    tick_count: u64,
    /// Shared property state for verification.
    property_state: SharedHelixPropertyState,
    /// Committed data entries for durability verification.
    /// Maps (`topic_id`, `partition_id`) -> list of (offset, `data_hash`) pairs.
    committed_data: BTreeMap<(TopicId, PartitionId), Vec<(Offset, u64)>>,
    /// Total produce operations attempted.
    #[allow(dead_code)] // Tracked but not yet exposed.
    produce_count: u64,
    /// Total successful produce operations.
    produce_success_count: u64,
    /// Storage fault configuration.
    #[allow(dead_code)] // Used to create storage, retained for recovery.
    fault_config: FaultConfig,
    /// WAL mode: per-partition or shared.
    wal_mode: WalMode,
    /// `SharedWAL` pool (only used when `wal_mode` is `Shared`).
    /// Each node has its own pool, mirroring production architecture.
    shared_wal_pool: Option<DstSharedWalPool>,
    /// Number of shared WALs (for recreating pool on recovery).
    shared_wal_count: u32,
    /// Tokio runtime for per-partition WAL mode.
    ///
    /// `BufferedWal` spawns background tasks that require a tokio runtime.
    /// This is only used when `wal_mode` is `PerPartition`.
    per_partition_runtime: Option<tokio::runtime::Runtime>,
    /// Actor mode: use real production partition actors instead of `MultiRaft`.
    /// This tests the actual production implementation with zero code duplication.
    actor_mode: bool,
    /// Real partition actor handles (only used when `actor_mode` is true).
    /// Each partition has its own real `PartitionActorShared` with its own `RaftNode`.
    partition_actor_handles: BTreeMap<GroupId, PartitionActorHandle>,
    /// Shared output channel sender for partition actors.
    /// Passed to each `spawn_partition_actor_shared()` call.
    partition_output_tx: Option<mpsc::Sender<GroupedOutput>>,
    /// Shared output channel receiver for partition actors.
    /// Outputs from ALL partition actors come through this channel.
    partition_output_rx: Option<mpsc::Receiver<GroupedOutput>>,
    /// Tokio runtime for actor mode (drives async partition actors).
    actor_runtime: Option<tokio::runtime::Runtime>,
    /// Actor mode commits with notification (for verification).
    /// Counts commits where `batch_notify.is_some()` - we were the proposing leader.
    actor_mode_commits_with_notify: u64,
    /// Actor mode commits without notification (replicated entries).
    /// Counts commits where `batch_notify.is_none()` - we're a follower.
    actor_mode_commits_without_notify: u64,
}

impl HelixServiceActor {
    /// Creates a new Helix service actor.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - Bloodhound actor ID
    /// * `node_id` - Helix node ID
    /// * `cluster_nodes` - All node IDs in the cluster
    /// * `node_to_actor` - Mapping from `NodeId` to `ActorId`
    /// * `network_state` - Shared network state for partitions
    /// * `property_state` - Shared property state for verification
    /// * `fault_config` - Storage fault injection configuration
    /// * `seed` - Random seed for deterministic behavior
    /// * `wal_mode` - WAL mode: per-partition or shared
    /// * `shared_wal_count` - Number of shared WALs (only used when `wal_mode` is `Shared`)
    /// * `actor_mode` - Whether to use actor mode batch tracking
    #[must_use]
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn new(
        actor_id: ActorId,
        node_id: NodeId,
        cluster_nodes: Vec<NodeId>,
        node_to_actor: BTreeMap<NodeId, ActorId>,
        network_state: SharedNetworkState,
        property_state: SharedHelixPropertyState,
        fault_config: FaultConfig,
        seed: u64,
        wal_mode: WalMode,
        shared_wal_count: u32,
        actor_mode: bool,
    ) -> Self {
        let name = format!("helix-service-{}", node_id.get());

        // Create simulated storage with fault injection.
        let storage = SimulatedStorage::with_faults(seed, fault_config.clone());

        // Create simulated transport.
        let transport = SimulatedTransport::new(
            node_id,
            actor_id,
            node_to_actor.clone(),
            Arc::clone(&network_state),
        );

        // Create Multi-Raft with controller group.
        let mut multi_raft = MultiRaft::new(node_id);

        // Note: We do NOT set default_peers for auto-creating groups here.
        // Data partition groups must only be created via the controller's
        // AssignPartition command, which ensures partition_storage is also
        // created. Auto-creating Raft groups on message receipt would cause
        // commits to be dropped because partition_storage doesn't exist yet.

        // Create controller group.
        if let Err(e) = multi_raft.create_group(CONTROLLER_GROUP_ID, cluster_nodes.clone()) {
            error!(error = %e, "Failed to create controller group");
        }

        // Create shared WAL pool if in shared mode.
        // Use storage.clone() so SharedWAL shares the same simulated storage as the actor.
        // This ensures simulate_crash() reverts SharedWAL data along with other storage.
        let shared_wal_pool = if wal_mode == WalMode::Shared && shared_wal_count > 0 {
            let pool_dir = format!("/node-{}/shared-wal", node_id.get());
            let pool_config = PoolConfig::new(&pool_dir, shared_wal_count);
            match DstSharedWalPool::open(storage.clone(), pool_config) {
                Ok(pool) => Some(pool),
                Err(e) => {
                    error!(error = %e, "Failed to create SharedWalPool, falling back to per-partition");
                    None
                }
            }
        } else {
            None
        };

        // Create tokio runtime for per-partition WAL mode.
        // BufferedWal spawns background tasks that require a tokio runtime.
        let per_partition_runtime = if wal_mode == WalMode::PerPartition {
            match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => Some(rt),
                Err(e) => {
                    error!(error = %e, "Failed to create tokio runtime for per-partition mode");
                    None
                }
            }
        } else {
            None
        };

        // Determine effective WAL mode (may fall back if pool creation failed).
        let effective_wal_mode = match wal_mode {
            WalMode::Shared if shared_wal_pool.is_some() => WalMode::Shared,
            WalMode::PerPartition if per_partition_runtime.is_some() => WalMode::PerPartition,
            // Fallback to InMemory if pool/runtime creation failed.
            WalMode::Shared | WalMode::InMemory | WalMode::PerPartition => WalMode::InMemory,
        };

        // Create actor mode infrastructure if enabled.
        // This creates a shared output channel and tokio runtime for REAL partition actors.
        let (partition_output_tx, partition_output_rx, actor_runtime) = if actor_mode {
            // Create shared output channel - all partition actors send outputs here.
            let (tx, rx) = mpsc::channel(1000);

            // Create tokio runtime for async partition actor operations.
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
            {
                Ok(rt) => Some(rt),
                Err(e) => {
                    error!(error = %e, "Failed to create tokio runtime for actor mode");
                    None
                }
            };

            if runtime.is_some() {
                eprintln!("[ACTOR-MODE] Node {} enabling actor mode with real partition actors", node_id.get());
                info!(node = node_id.get(), "Actor mode enabled with real partition actors");
                (Some(tx), Some(rx), runtime)
            } else {
                warn!(node = node_id.get(), "Actor mode disabled - runtime creation failed");
                (None, None, None)
            }
        } else {
            (None, None, None)
        };

        // Effective actor mode - only enabled if runtime was created successfully.
        let effective_actor_mode = actor_mode && actor_runtime.is_some();

        Self {
            actor_id,
            name,
            node_id,
            cluster_nodes,
            multi_raft,
            partition_storage: BTreeMap::new(),
            group_map: GroupMap::new(),
            controller_state: ControllerState::new(),
            storage,
            transport,
            network_state,
            crashed: false,
            node_to_actor,
            seed,
            tick_count: 0,
            property_state,
            committed_data: BTreeMap::new(),
            produce_count: 0,
            produce_success_count: 0,
            fault_config,
            wal_mode: effective_wal_mode,
            shared_wal_pool,
            shared_wal_count,
            per_partition_runtime,
            actor_mode: effective_actor_mode,
            partition_actor_handles: BTreeMap::new(),
            partition_output_tx,
            partition_output_rx,
            actor_runtime,
            actor_mode_commits_with_notify: 0,
            actor_mode_commits_without_notify: 0,
        }
    }

    /// Returns the node ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns whether this node is crashed.
    #[must_use]
    pub const fn is_crashed(&self) -> bool {
        self.crashed
    }

    /// Returns whether this node is the controller leader.
    #[must_use]
    pub fn is_controller_leader(&self) -> bool {
        self.multi_raft
            .group_state(CONTROLLER_GROUP_ID)
            .is_some_and(|s| s.state == RaftState::Leader)
    }

    /// Returns the current controller term.
    #[must_use]
    pub fn controller_term(&self) -> u64 {
        self.multi_raft
            .group_state(CONTROLLER_GROUP_ID)
            .map_or(0, |s| s.current_term.get())
    }

    /// Returns the current controller state.
    #[must_use]
    pub fn controller_state(&self) -> RaftState {
        self.multi_raft
            .group_state(CONTROLLER_GROUP_ID)
            .map_or(RaftState::Follower, |s| s.state)
    }

    /// Creates a snapshot of the current node state for property verification.
    #[must_use]
    pub fn snapshot(&self) -> HelixNodeSnapshot {
        HelixNodeSnapshot {
            node_id: self.node_id.get(),
            controller_term: self.controller_term(),
            controller_state: self.controller_state(),
            crashed: self.crashed,
        }
    }

    /// Returns the number of successful produce operations.
    #[must_use]
    pub const fn produce_success_count(&self) -> u64 {
        self.produce_success_count
    }

    /// Returns the number of data partitions created.
    #[must_use]
    pub fn data_partition_count(&self) -> usize {
        // Count partition_storage entries (excludes controller partition).
        self.partition_storage.len()
    }

    /// Returns the total committed entry count across all partitions.
    #[must_use]
    pub fn committed_entry_count(&self) -> usize {
        self.committed_data.values().map(Vec::len).sum()
    }

    /// Returns the storage fault statistics.
    #[must_use]
    pub fn fault_stats(&self) -> FaultStats {
        self.storage.fault_stats()
    }

    /// Returns the committed data for verification.
    /// Maps (`topic_id`, `partition_id`) -> list of (offset, `data_hash`).
    #[must_use]
    pub const fn committed_data(&self) -> &BTreeMap<(TopicId, PartitionId), Vec<(Offset, u64)>> {
        &self.committed_data
    }

    /// Verifies data integrity: all committed data is readable with correct hash.
    ///
    /// Returns a list of integrity violations (offset, `expected_hash`, `actual_hash` or None if missing).
    #[must_use]
    pub fn verify_data_integrity(&self) -> Vec<DataIntegrityViolation> {
        let mut violations = Vec::new();

        for (&(topic_id, partition_id), committed) in &self.committed_data {
            // Find the partition storage.
            let Some(group_id) = self.group_map.get(topic_id, partition_id) else {
                // Group not found - this is a violation if we have committed data.
                for &(offset, expected_hash) in committed {
                    violations.push(DataIntegrityViolation {
                        node_id: self.node_id.get(),
                        topic_id: topic_id.get(),
                        partition_id: partition_id.get(),
                        offset: offset.get(),
                        expected_hash,
                        actual_hash: None,
                        reason: "partition storage not found".to_string(),
                    });
                }
                continue;
            };

            let Some(ps) = self.partition_storage.get(&group_id) else {
                // Storage not found - violation.
                for &(offset, expected_hash) in committed {
                    violations.push(DataIntegrityViolation {
                        node_id: self.node_id.get(),
                        topic_id: topic_id.get(),
                        partition_id: partition_id.get(),
                        offset: offset.get(),
                        expected_hash,
                        actual_hash: None,
                        reason: "partition storage not initialized".to_string(),
                    });
                }
                continue;
            };

            // Verify each committed entry is readable with correct hash.
            for &(offset, expected_hash) in committed {
                // Read 1 record at the committed offset.
                match ps.read(offset, 1) {
                    Ok(records) if !records.is_empty() => {
                        // For single record, use direct hash comparison.
                        let actual_hash = Self::simple_hash(&records[0].value);
                        if actual_hash != expected_hash {
                            violations.push(DataIntegrityViolation {
                                node_id: self.node_id.get(),
                                topic_id: topic_id.get(),
                                partition_id: partition_id.get(),
                                offset: offset.get(),
                                expected_hash,
                                actual_hash: Some(actual_hash),
                                reason: "hash mismatch".to_string(),
                            });
                        }
                    }
                    Ok(_) => {
                        // Empty records returned.
                        violations.push(DataIntegrityViolation {
                            node_id: self.node_id.get(),
                            topic_id: topic_id.get(),
                            partition_id: partition_id.get(),
                            offset: offset.get(),
                            expected_hash,
                            actual_hash: None,
                            reason: "empty records at offset".to_string(),
                        });
                    }
                    Err(e) => {
                        violations.push(DataIntegrityViolation {
                            node_id: self.node_id.get(),
                            topic_id: topic_id.get(),
                            partition_id: partition_id.get(),
                            offset: offset.get(),
                            expected_hash,
                            actual_hash: None,
                            reason: format!("read error: {e}"),
                        });
                    }
                }
            }
        }

        violations
    }

    /// Updates the shared property state with this node's current state.
    fn update_property_state(&self) {
        let snapshot = self.snapshot();
        if let Ok(mut state) = self.property_state.lock() {
            state.update_snapshot(snapshot);
            state.increment_events();
            // Update stats (only include this node's contribution).
            state.update_stats(
                self.produce_success_count,
                self.data_partition_count() as u64,
                self.committed_entry_count() as u64,
            );
        }
    }

    /// Handles the `collect_state` event - updates property state.
    fn handle_collect_state(&self) {
        self.update_property_state();
    }

    /// Handles the `verify_integrity` event - verifies all committed data is readable.
    fn handle_verify_integrity(&self) {
        info!(actor = %self.name, "Verifying data integrity");

        let violations = self.verify_data_integrity();

        // Record violations in shared property state.
        if let Ok(mut state) = self.property_state.lock() {
            for v in violations {
                state.record_integrity_violation(DataIntegrityViolationRecord {
                    node_id: v.node_id,
                    topic_id: v.topic_id,
                    partition_id: v.partition_id,
                    offset: v.offset,
                    expected_hash: v.expected_hash,
                    actual_hash: v.actual_hash,
                    reason: v.reason,
                });
            }
            state.mark_integrity_verified();
        }

        info!(
            actor = %self.name,
            committed_entries = self.committed_entry_count(),
            "Data integrity verification complete"
        );
    }

    /// Handles consumer verification - records which client-ack'd offsets this node can read.
    ///
    /// Each node reports which offsets it can successfully read and verify.
    /// After all nodes report, `finalize_consumer_verification` determines which
    /// offsets NO node could verify (those are true data loss violations).
    fn handle_verify_consumer(&self) {
        info!(actor = %self.name, "Verifying consumer data (all client acks)");

        // Get all client acks from shared state.
        let client_acks: std::collections::BTreeMap<(u64, u64), Vec<(u64, u64)>> = {
            if let Ok(state) = self.property_state.lock() { state.client_acked_produces.clone() } else {
                warn!(actor = %self.name, "Failed to lock property state for consumer verification");
                return;
            }
        };

        let mut verified_offsets = Vec::new();
        let mut verified_count = 0;

        // For each partition with acks, try to read and verify from our storage.
        for ((topic_id, partition_id), acks) in &client_acks {
            let topic_id_typed = TopicId::new(*topic_id);
            let partition_id_typed = PartitionId::new(*partition_id);

            // Find partition storage for this topic/partition.
            let Some(group_id) = self.group_map.get(topic_id_typed, partition_id_typed) else {
                // This node doesn't have this partition - skip (other nodes will verify).
                continue;
            };

            let Some(ps) = self.partition_storage.get(&group_id) else {
                // Storage not initialized - can't verify from this node.
                continue;
            };

            // Try to verify each ack'd offset.
            for &(offset, expected_hash) in acks {
                match ps.read(Offset::new(offset), 1) {
                    Ok(records) if !records.is_empty() => {
                        let actual_hash = Self::simple_hash(&records[0].value);
                        if actual_hash == expected_hash {
                            // This node can successfully verify this offset.
                            verified_offsets.push((*topic_id, *partition_id, offset));
                            verified_count += 1;
                            // TRACE: Log successful verification for high offsets.
                            if offset >= 40 && *topic_id == 1 && *partition_id == 0 {
                                eprintln!(
                                    "[TRACE-VERIFY] {} offset={} VERIFIED (hash={})",
                                    self.name, offset, actual_hash
                                );
                            }
                        } else {
                            // TRACE: Log hash mismatch for high offsets.
                            if offset >= 40 && *topic_id == 1 && *partition_id == 0 {
                                eprintln!(
                                    "[TRACE-VERIFY] {} offset={} HASH MISMATCH expected={} actual={}",
                                    self.name, offset, expected_hash, actual_hash
                                );
                            }
                        }
                    }
                    _ => {
                        // Can't read - don't mark as verified, other nodes might have it.
                        // TRACE: Log read failure for high offsets.
                        if offset >= 40 && *topic_id == 1 && *partition_id == 0 {
                            eprintln!(
                                "[TRACE-VERIFY] {} offset={} CANNOT READ (log_end={}, expected_hash={})",
                                self.name, offset, ps.log_end_offset().get(), expected_hash
                            );
                        }
                    }
                }
            }
        }

        // Record verified offsets in shared property state.
        if let Ok(mut state) = self.property_state.lock() {
            for (topic_id, partition_id, offset) in verified_offsets {
                state.record_verified_offset(topic_id, partition_id, offset);
            }
        }

        info!(
            actor = %self.name,
            verified = verified_count,
            total_acks = client_acks.values().map(Vec::len).sum::<usize>(),
            "Consumer verification complete"
        );
    }

    /// Handles applying a network partition from event data.
    /// Data format: count (1 byte) + `actor_ids` (8 bytes each)
    #[allow(clippy::significant_drop_tightening)] // Lock must be held during loop iteration.
    fn handle_apply_partition(&self, data: &[u8]) {
        if data.is_empty() {
            // Default: partition this node from all others.
            let mut state = self.network_state.lock().expect("lock");
            for &other_actor in self.node_to_actor.values() {
                if other_actor != self.actor_id {
                    state.partition(&[self.actor_id, other_actor]);
                }
            }
            info!(actor = %self.name, "Applied partition (isolated from all)");
            return;
        }

        // Parse partition data.
        let count = data[0] as usize;
        if data.len() < 1 + count * 8 {
            warn!(actor = %self.name, "Invalid partition data");
            return;
        }

        let mut actors = Vec::with_capacity(count);
        for i in 0..count {
            let start = 1 + i * 8;
            let actor_id = u64::from_le_bytes(data[start..start + 8].try_into().unwrap());
            actors.push(ActorId::new(actor_id));
        }

        let mut state = self.network_state.lock().expect("lock");
        for pair in actors.windows(2) {
            state.partition(&[pair[0], pair[1]]);
        }
        info!(actor = %self.name, actors = ?actors, "Applied partition");
    }

    /// Handles healing a network partition.
    #[allow(clippy::significant_drop_tightening)] // Lock must be held during loop iteration.
    fn handle_heal_partition(&self, data: &[u8]) {
        if data.is_empty() {
            // Default: heal all partitions involving this node.
            let mut state = self.network_state.lock().expect("lock");
            for &other_actor in self.node_to_actor.values() {
                if other_actor != self.actor_id {
                    state.heal(&[self.actor_id, other_actor]);
                }
            }
            info!(actor = %self.name, "Healed all partitions");
            return;
        }

        // Parse heal data (same format as partition).
        let count = data[0] as usize;
        if data.len() < 1 + count * 8 {
            warn!(actor = %self.name, "Invalid heal data");
            return;
        }

        let mut actors = Vec::with_capacity(count);
        for i in 0..count {
            let start = 1 + i * 8;
            let actor_id = u64::from_le_bytes(data[start..start + 8].try_into().unwrap());
            actors.push(ActorId::new(actor_id));
        }

        let mut state = self.network_state.lock().expect("lock");
        for pair in actors.windows(2) {
            state.heal(&[pair[0], pair[1]]);
        }
        info!(actor = %self.name, actors = ?actors, "Healed partition");
    }

    /// Schedules the next tick timer.
    #[allow(clippy::unused_self)] // Kept as method for consistency with other handlers.
    fn schedule_tick(&self, ctx: &mut SimulationContext) {
        ctx.set_timer(
            Duration::from_millis(TICK_INTERVAL_MS),
            timer_ids::TICK,
        );
    }

    /// Schedules the next heartbeat timer.
    #[allow(clippy::unused_self)] // Kept as method for consistency with other handlers.
    fn schedule_heartbeat(&self, ctx: &mut SimulationContext) {
        ctx.set_timer(
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            timer_ids::HEARTBEAT,
        );
    }

    /// Handles a tick event - mirrors `tick_task_multi_node` logic.
    fn handle_tick(&mut self, ctx: &mut SimulationContext) {
        // Increment tick counter for time tracking.
        self.tick_count += 1;

        if self.actor_mode {
            // Actor mode: tick real partition actors and controller via MultiRaft.
            self.handle_tick_actor_mode(ctx);
        } else {
            // Legacy mode: tick all groups via MultiRaft.
            let outputs = self.multi_raft.tick();
            self.process_outputs(&outputs, ctx);
        }

        // Drain transport queue and schedule message deliveries.
        self.transport.drain_and_schedule(ctx);

        // Update property state every 10 ticks (500ms at 50ms tick interval).
        // This captures leader elections and term changes.
        if self.tick_count % 10 == 0 {
            self.update_property_state();
        }

        // Schedule next tick.
        self.schedule_tick(ctx);
    }

    /// Handles tick in actor mode - ticks real partition actors.
    fn handle_tick_actor_mode(&mut self, ctx: &mut SimulationContext) {
        // Tick controller partition via MultiRaft (controller uses legacy path).
        let controller_outputs = self.multi_raft.tick();
        self.process_outputs(&controller_outputs, ctx);

        // Tick each real partition actor via its handle.
        let Some(runtime) = self.actor_runtime.as_ref() else {
            return;
        };

        // Send tick to each partition actor (REAL production code!).
        for handle in self.partition_actor_handles.values() {
            runtime.block_on(async {
                let _ = handle.tick().await;
            });
        }

        // Give the runtime time to process commands on worker threads.
        // The tick() calls above just send commands to channels and return immediately.
        // The actual processing happens asynchronously on worker threads, so we need
        // to yield to let them complete before draining outputs.
        runtime.block_on(async {
            // Small sleep to let worker threads process. This ensures the actors
            // have time to handle the tick commands and generate outputs.
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        });

        // Drain outputs from all partition actors (shared channel).
        self.process_partition_actor_outputs(ctx);
    }

    /// Processes outputs from real partition actors (actor mode).
    fn process_partition_actor_outputs(&mut self, ctx: &mut SimulationContext) {
        // Drain all available outputs into a Vec first to avoid borrow issues.
        // We need to release the mutable borrow of partition_output_rx before
        // calling handle_commit which needs mutable access to self.
        let outputs: Vec<GroupedOutput> = {
            let Some(output_rx) = self.partition_output_rx.as_mut() else {
                return;
            };

            let mut collected = Vec::new();
            while let Ok(grouped) = output_rx.try_recv() {
                collected.push(grouped);
            }
            collected
        };

        // Now process all collected outputs.
        if !outputs.is_empty() {
            eprintln!(
                "[ACTOR-MODE] {} processing {} outputs from partition actors",
                self.name, outputs.len()
            );
        }
        for grouped in outputs {
            let group_id = grouped.group_id;

            match grouped.output {
                PartitionOutput::SendMessages { to, messages } => {
                    eprintln!(
                        "[SEND] {} group={} sending {} messages to {:?}",
                        self.name, group_id.get(), messages.len(), to.get()
                    );
                    // Route through simulated network instead of real TCP.
                    self.transport.queue_batch(to, &messages);
                }
                PartitionOutput::EntryCommitted { index, data, batch_notify } => {
                    let was_proposing_leader = batch_notify.is_some();
                    eprintln!(
                        "[ACTOR-MODE] {} committed entry: group={} index={} has_notify={}",
                        self.name, group_id.get(), index.get(), was_proposing_leader
                    );
                    // Track notification status for verification.
                    if was_proposing_leader {
                        self.actor_mode_commits_with_notify += 1;
                    } else {
                        self.actor_mode_commits_without_notify += 1;
                    }

                    // Apply to storage using the existing commit handling.
                    // Pass was_proposing_leader so client_acks are recorded correctly.
                    self.handle_commit(group_id, index, &data, Some(was_proposing_leader), ctx);
                }
                PartitionOutput::BecameLeader => {
                    eprintln!(
                        "[ACTOR-MODE] {} became leader of group {}",
                        self.name, group_id.get()
                    );
                    info!(
                        actor = %self.name,
                        group = group_id.get(),
                        "became leader (actor mode)"
                    );
                }
                PartitionOutput::SteppedDown => {
                    info!(
                        actor = %self.name,
                        group = group_id.get(),
                        "stepped down (actor mode)"
                    );
                }
                PartitionOutput::VoteStateChanged { term, voted_for } => {
                    debug!(
                        actor = %self.name,
                        group = group_id.get(),
                        term,
                        voted_for = ?voted_for.map(NodeId::get),
                        "vote state changed (actor mode)"
                    );
                }
            }
        }
    }

    /// Handles a heartbeat timer event.
    fn handle_heartbeat(&self, ctx: &mut SimulationContext) {
        // Use tick count as timestamp (deterministic).
        // Each tick is TICK_INTERVAL_MS, so we multiply.
        let timestamp_ms = self.tick_count * TICK_INTERVAL_MS;

        let heartbeat = helix_runtime::BrokerHeartbeat {
            node_id: self.node_id,
            timestamp_ms,
        };

        // Send heartbeat to all peers via transport.
        // Use the sync queue_heartbeat method instead of async.
        for peer_id in &self.cluster_nodes {
            if *peer_id != self.node_id {
                self.transport.queue_heartbeat(*peer_id, &heartbeat);
            }
        }

        // Drain and schedule the heartbeat messages.
        self.transport.drain_and_schedule(ctx);

        // Schedule next heartbeat.
        self.schedule_heartbeat(ctx);
    }

    /// Processes Multi-Raft outputs.
    fn process_outputs(&mut self, outputs: &[MultiRaftOutput], ctx: &mut SimulationContext) {
        for output in outputs {
            match output {
                MultiRaftOutput::CommitEntry { group_id, index, data } => {
                    // Pass None - non-actor mode uses multi_raft.group_state() for leadership check.
                    self.handle_commit(*group_id, *index, data, None, ctx);
                }
                MultiRaftOutput::SendMessages { to, messages } => {
                    // Queue messages for delivery via transport (sync).
                    self.transport.queue_batch(*to, messages);
                }
                MultiRaftOutput::BecameLeader { group_id } => {
                    // TRACE-LEADER: Log leader election with Raft state
                    if *group_id == GroupId::new(1) {
                        let state = self.multi_raft.group_state(*group_id);
                        let (commit_idx, last_applied) = state.map_or((0, 0), |s| {
                            (s.commit_index.get(), s.last_applied.get())
                        });
                        let log_end = self.partition_storage.get(group_id)
                            .map_or(0, |ps| ps.log_end_offset().get());
                        eprintln!(
                            "[TRACE-LEADER] {} BECAME LEADER: group={} raft_commit_idx={} raft_last_applied={} partition_log_end={}",
                            self.name, group_id.get(), commit_idx, last_applied, log_end
                        );
                    }
                    info!(
                        actor = %self.name,
                        group = group_id.get(),
                        "became leader"
                    );
                }
                MultiRaftOutput::SteppedDown { group_id } => {
                    info!(
                        actor = %self.name,
                        group = group_id.get(),
                        "stepped down"
                    );
                }
                MultiRaftOutput::VoteStateChanged { group_id, term, voted_for } => {
                    // In DST, vote state persistence is handled by the simulated
                    // VoteStorage. For now, just track it for debugging.
                    debug!(
                        actor = %self.name,
                        group = group_id.get(),
                        term = term.get(),
                        voted_for = ?voted_for.map(NodeId::get),
                        "vote state changed"
                    );
                }
            }
        }
    }

    /// Handles a committed entry.
    ///
    /// # Arguments
    ///
    /// * `was_proposing_leader` - In actor mode, `Some(true)` if we were the proposing leader
    ///   (batch_notify was present), `Some(false)` if we were a follower. In non-actor mode,
    ///   `None` and the leadership is determined from `multi_raft.group_state()`.
    #[allow(clippy::too_many_lines)]
    fn handle_commit(
        &mut self,
        group_id: GroupId,
        index: LogIndex,
        data: &Bytes,
        was_proposing_leader: Option<bool>,
        ctx: &mut SimulationContext,
    ) {
        // TRACE-COMMIT: Log commits for high indices on partition 0
        if group_id == GroupId::new(1) && index.get() >= 45 {
            eprintln!(
                "[TRACE-COMMIT] {} committing: group={} raft_index={}",
                self.name, group_id.get(), index.get()
            );
        }

        // Check if this is a controller partition commit.
        if group_id == CONTROLLER_GROUP_ID {
            self.handle_controller_commit(index, data);
            return;
        }

        // In actor mode, batch notification tracking is done in process_partition_actor_outputs
        // via the real batch_notify field from production PartitionOutput::EntryCommitted.
        // No fake tracking needed here - we just apply to storage.

        // Regular data partition commit - apply to storage.
        if let Some((topic_id, partition_id)) = self.group_map.get_key(group_id) {
            if let Some(ps) = self.partition_storage.get_mut(&group_id) {
                eprintln!(
                    "[ACTOR-MODE-COMMIT] {} handling commit: group={} topic={} partition={} index={}",
                    self.name, group_id.get(), topic_id.get(), partition_id.get(), index.get()
                );
                // Decode the command to get the actual record payloads for tracking.
                let record_payloads: Vec<Vec<u8>> = match PartitionCommand::decode(data) {
                    Some(PartitionCommand::Append { ref records, .. }) => {
                        eprintln!(
                            "[DECODE] {} decoded Append with {} records, data len={}",
                            self.name, records.len(), data.len()
                        );
                        records.iter().map(|r| r.value.to_vec()).collect()
                    }
                    other => {
                        eprintln!(
                            "[DECODE] {} command is {:?}, data len={}",
                            self.name, other, data.len()
                        );
                        Vec::new()
                    }
                };

                // TRACE: Log state before apply for debugging offset 21.
                if topic_id.get() == 1 && partition_id.get() == 0 && ps.log_end_offset().get() <= 22 {
                    eprintln!(
                        "[TRACE] {} BEFORE apply: topic={} partition={} raft_index={} log_end={} ps_last_applied={}",
                        self.name, topic_id.get(), partition_id.get(), index.get(), ps.log_end_offset().get(), ps.last_applied().get()
                    );
                }

                // Apply entry - uses sync for InMemory, async via block_on for Durable.
                let apply_result = match self.wal_mode {
                    WalMode::InMemory => {
                        // In-memory storage: use sync apply.
                        ps.apply_entry_sync(index, data)
                    }
                    WalMode::PerPartition => {
                        // Durable per-partition WAL: use async apply via block_on.
                        futures::executor::block_on(ps.apply_entry_async(index, data))
                    }
                    WalMode::Shared => {
                        // Durable storage with SharedWAL: use async apply via runtime.
                        if let Some(ref pool) = self.shared_wal_pool {
                            pool.runtime().block_on(ps.apply_entry_async(index, data))
                        } else {
                            // Fallback to sync if no pool (shouldn't happen).
                            ps.apply_entry_sync(index, data)
                        }
                    }
                };

                eprintln!(
                    "[APPLY] {} applying index={} record_payloads_len={} wal_mode={:?}",
                    self.name, index.get(), record_payloads.len(), self.wal_mode
                );
                match apply_result {
                    Ok(offset_opt) => {
                        eprintln!(
                            "[APPLY-OK] {} applied index={} offset_opt={:?}",
                            self.name, index.get(), offset_opt.map(helix_core::Offset::get)
                        );
                        // Track committed data for durability verification.
                        let was_skipped = offset_opt.is_none();
                        let base_offset = offset_opt.unwrap_or_else(|| ps.log_end_offset());

                        // TRACE: Log entry applications for high offsets (>= 40) on ALL nodes.
                        if topic_id.get() == 1 && partition_id.get() == 0 && base_offset.get() >= 40 {
                            eprintln!(
                                "[TRACE-APPLY] {} applied: topic={} partition={} raft_index={} base_offset={} records={} skipped={} log_end={}",
                                self.name, topic_id.get(), partition_id.get(), index.get(), base_offset.get(), record_payloads.len(), was_skipped, ps.log_end_offset().get()
                            );
                        }

                        // Collect records for tracking.
                        let records_to_track: Vec<(Offset, u64)> = record_payloads
                            .iter()
                            .enumerate()
                            .map(|(i, payload)| {
                                #[allow(clippy::cast_possible_truncation)]
                                let offset = Offset::new(base_offset.get() + i as u64);
                                let data_hash = Self::simple_hash(payload);
                                (offset, data_hash)
                            })
                            .collect();

                        // Update committed_data tracking.
                        let committed = self.committed_data
                            .entry((topic_id, partition_id))
                            .or_default();
                        for (offset, data_hash) in &records_to_track {
                            committed.push((*offset, *data_hash));
                        }

                        // Record client acknowledgments in shared state.
                        // Only record if we're the leader - the leader is the one that
                        // sends acks to clients in a real system.
                        //
                        // In actor mode, `was_proposing_leader` tells us if we were the proposer.
                        // In non-actor mode, we check multi_raft.group_state().
                        let is_leader = was_proposing_leader.unwrap_or_else(|| {
                            self.multi_raft
                                .group_state(group_id)
                                .is_some_and(|s| s.state == helix_raft::RaftState::Leader)
                        });

                        if is_leader {
                            if let Ok(mut state) = self.property_state.lock() {
                                for (offset, data_hash) in &records_to_track {
                                    // TRACE: Log ALL client acks for offsets >= 40 to catch the failing one.
                                    if offset.get() >= 40 && topic_id.get() == 1 && partition_id.get() == 0 {
                                        eprintln!(
                                            "[TRACE-ACK] {} recording client ack: topic={} partition={} offset={} raft_index={} hash={}",
                                            self.name, topic_id.get(), partition_id.get(), offset.get(), index.get(), data_hash
                                        );
                                    }
                                    state.record_client_ack(
                                        topic_id.get(),
                                        partition_id.get(),
                                        offset.get(),
                                        *data_hash,
                                    );
                                }
                            }
                        }

                        self.produce_success_count += 1;
                        eprintln!(
                            "[ACTOR-MODE-STAT] {} produce_success_count={} committed_data_len={} after commit index={}",
                            self.name, self.produce_success_count, self.committed_data.values().map(Vec::len).sum::<usize>(), index.get()
                        );
                    }
                    Err(e) => {
                        let error_msg = e.to_string();

                        // A "torn write" error means the simulated process crashed during
                        // the write. Only crash if it won't violate quorum (majority survives).
                        if error_msg.contains("torn write") {
                            let can_crash = self.network_state
                                .lock()
                                .map(|ns| ns.can_crash_safely(self.actor_id))
                                .unwrap_or(true);

                            if can_crash {
                                info!(
                                    actor = %self.name,
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    index = index.get(),
                                    "Torn write detected - crashing (quorum preserved)"
                                );
                                self.handle_crash();
                                // Schedule recovery after a short delay.
                                ctx.schedule_after(
                                    Duration::from_millis(50),
                                    EventKind::ProcessRecover { actor: self.actor_id },
                                );
                                return;
                            }
                            // Can't crash without violating quorum - just log and continue.
                            // The write failed but node stays up to preserve quorum.
                            warn!(
                                actor = %self.name,
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                index = index.get(),
                                "Torn write detected but skipping crash to preserve quorum"
                            );
                            return;
                        }

                        warn!(
                            actor = %self.name,
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            error = %e,
                            "Failed to apply entry"
                        );
                    }
                }
            }
        }
    }

    /// Loads all persisted records for a partition from `SimulatedStorage`.
    ///
    /// Returns a map of offset -> record data.
    fn load_persisted_records(&self, topic_id: TopicId, partition_id: PartitionId) -> BTreeMap<Offset, Vec<u8>> {
        let mut records = BTreeMap::new();
        let dir_path = std::path::PathBuf::from(format!(
            "/partitions/{}/{}",
            topic_id.get(),
            partition_id.get()
        ));

        // List files in the directory using sync API.
        if let Ok(files) = self.storage.list_files_sync(&dir_path, "") {
            for file_path in files {
                if let Some(file_name) = file_path.file_name().and_then(|n| n.to_str()) {
                    if let Some(offset_str) = file_name.strip_prefix("offset_") {
                        if let Ok(offset_num) = offset_str.parse::<u64>() {
                            if let Ok(file) = self.storage.open_sync(&file_path) {
                                if let Ok(data) = file.read_all_sync() {
                                    records.insert(Offset::new(offset_num), data.to_vec());
                                }
                            }
                        }
                    }
                }
            }
        }

        records
    }

    /// Simple hash for data comparison.
    fn simple_hash(data: &[u8]) -> u64 {
        // FNV-1a hash for simplicity.
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in data {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        hash
    }

    /// Handles a controller partition commit.
    fn handle_controller_commit(&mut self, index: LogIndex, data: &Bytes) {
        let Some(cmd) = ControllerCommand::decode(data) else {
            warn!(
                actor = %self.name,
                index = index.get(),
                "Failed to decode controller command"
            );
            return;
        };

        debug!(
            actor = %self.name,
            index = index.get(),
            command = ?cmd,
            "Applying controller command"
        );

        let follow_ups = self.controller_state.apply(&cmd, &self.cluster_nodes);

        // Propose follow-up commands if we're the leader.
        if !follow_ups.is_empty() && self.is_controller_leader() {
            for follow_up in follow_ups {
                let encoded = follow_up.encode();
                if self.multi_raft.propose(CONTROLLER_GROUP_ID, encoded).is_none() {
                    warn!(
                        actor = %self.name,
                        command = ?follow_up,
                        "Failed to propose follow-up controller command"
                    );
                }
            }
        }

        // Handle AssignPartition by creating data Raft group.
        if let ControllerCommand::AssignPartition {
            topic_id,
            partition_id,
            group_id: data_group_id,
            ref replicas,
        } = cmd
        {
            if replicas.contains(&self.node_id) {
                self.create_data_partition(topic_id, partition_id, data_group_id, replicas);
            }
        }
    }

    /// Creates a data partition from controller assignment.
    #[allow(clippy::too_many_lines)]
    fn create_data_partition(
        &mut self,
        topic_id: TopicId,
        partition_id: PartitionId,
        group_id: GroupId,
        replicas: &[NodeId],
    ) {
        info!(
            actor = %self.name,
            topic = topic_id.get(),
            partition = partition_id.get(),
            group = group_id.get(),
            wal_mode = ?self.wal_mode,
            actor_mode = self.actor_mode,
            "Creating data partition"
        );

        // Update group map first.
        self.group_map.insert(topic_id, partition_id, group_id);

        if self.actor_mode {
            // Actor mode: spawn a REAL production partition actor.
            self.create_data_partition_actor_mode(topic_id, partition_id, group_id, replicas);
        } else {
            // Legacy mode: use MultiRaft.
            let existing_groups = self.multi_raft.group_ids();
            if !existing_groups.contains(&group_id) {
                if let Err(e) = self.multi_raft.create_group(group_id, replicas.to_vec()) {
                    warn!(
                        actor = %self.name,
                        error = %e,
                        group = group_id.get(),
                        "Failed to create data Raft group"
                    );
                    return;
                }
            }
        }

        // Create partition storage based on WAL mode.
        if self.partition_storage.contains_key(&group_id) {
            return; // Already created.
        }

        // TRACE: Log partition storage creation.
        if topic_id.get() == 1 && partition_id.get() == 0 {
            eprintln!(
                "[TRACE] {} CREATING NEW partition_storage for topic=1 partition=0 group={}",
                self.name, group_id.get()
            );
        }

        let storage = match self.wal_mode {
            WalMode::InMemory => {
                // In-memory storage (no WAL).
                SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
            }
            WalMode::PerPartition => {
                // Durable storage with per-partition WAL using SimulatedStorage.
                // Use the tokio runtime for BufferedWal's background tasks.
                if let Some(ref runtime) = self.per_partition_runtime {
                    let data_dir = std::path::PathBuf::from(format!(
                        "/node-{}/partitions/{}/{}",
                        self.node_id.get(),
                        topic_id.get(),
                        partition_id.get()
                    ));

                    match runtime.block_on(SimulatedPartitionStorage::new_durable(
                        self.storage.clone(),
                        &data_dir,
                        None, // No object storage for DST
                        None, // No tiering config for DST
                        topic_id,
                        partition_id,
                    )) {
                        Ok(ps) => ps,
                        Err(e) => {
                            warn!(
                                actor = %self.name,
                                error = %e,
                                "Failed to create durable per-partition storage, falling back to in-memory"
                            );
                            SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                        }
                    }
                } else {
                    warn!(
                        actor = %self.name,
                        "No tokio runtime for per-partition mode, falling back to in-memory"
                    );
                    SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                }
            }
            WalMode::Shared => {
                // Durable storage with SharedWAL.
                if let Some(ref pool) = self.shared_wal_pool {
                    let handle = pool.handle(partition_id);
                    // No recovered entries for new partitions - the async constructor
                    // will query the SharedWal for any pending/durable entries.
                    let recovered_entries = Vec::new();
                    let data_dir = std::path::PathBuf::from("/data");

                    // Use block_on since we're in a sync context but calling async API.
                    match pool.runtime().block_on(PartitionStorage::new_durable_with_shared_wal(
                        &data_dir,
                        topic_id,
                        partition_id,
                        handle,
                        recovered_entries,
                        None, // No object storage for DST
                        None, // No tiering config for DST
                    )) {
                        Ok(ps) => ps,
                        Err(e) => {
                            warn!(
                                actor = %self.name,
                                error = %e,
                                "Failed to create durable partition, falling back to in-memory"
                            );
                            SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                        }
                    }
                } else {
                    // Fallback to in-memory if no pool.
                    SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                }
            }
        };

        self.partition_storage.insert(group_id, storage);
    }

    /// Creates a data partition in actor mode by spawning a REAL production partition actor.
    ///
    /// This is the key function that enables testing the actual production implementation.
    /// It spawns a real `PartitionActorShared` via `spawn_partition_actor_shared()`.
    fn create_data_partition_actor_mode(
        &mut self,
        topic_id: TopicId,
        partition_id: PartitionId,
        group_id: GroupId,
        replicas: &[NodeId],
    ) {
        // Check if we already have this partition actor.
        if self.partition_actor_handles.contains_key(&group_id) {
            return;
        }

        let Some(runtime) = self.actor_runtime.as_ref() else {
            error!(
                actor = %self.name,
                group = group_id.get(),
                "No tokio runtime for actor mode"
            );
            return;
        };

        let Some(output_tx) = self.partition_output_tx.clone() else {
            error!(
                actor = %self.name,
                group = group_id.get(),
                "No output channel for actor mode"
            );
            return;
        };

        // Create a REAL RaftNode for this partition (same as production).
        let config = RaftConfig::new(self.node_id, replicas.to_vec())
            .with_tick_config(10, 2)
            .with_random_seed(self.seed.wrapping_add(group_id.get()));
        let raft_node = RaftNode::new(config);

        // Spawn the REAL production partition actor!
        // This calls the exact same function that production uses.
        let actor_config = PartitionActorConfig {
            channel_buffer_size: 100,
        };

        let handle = runtime.block_on(async {
            spawn_partition_actor_shared(group_id, raft_node, actor_config, output_tx)
        });

        eprintln!(
            "[ACTOR-MODE] {} spawned REAL production partition actor: group={} topic={} partition={}",
            self.name, group_id.get(), topic_id.get(), partition_id.get()
        );
        info!(
            actor = %self.name,
            group = group_id.get(),
            topic = topic_id.get(),
            partition = partition_id.get(),
            "Spawned REAL production partition actor"
        );

        self.partition_actor_handles.insert(group_id, handle);
    }

    /// Handles incoming message from another node.
    fn handle_message(&mut self, payload: &[u8], ctx: &mut SimulationContext) {
        use crate::simulated_transport::{decode_simulated_message, is_group_batch, is_heartbeat};
        use helix_runtime::{decode_broker_heartbeat, decode_group_batch};

        let Some((tag, data)) = decode_simulated_message(payload) else {
            warn!(actor = %self.name, "Failed to decode message tag");
            return;
        };

        if is_group_batch(tag) {
            // Decode and process group messages.
            match decode_group_batch(data) {
                Ok((messages, _consumed)) => {
                    for group_msg in messages {
                        if self.actor_mode && group_msg.group_id != CONTROLLER_GROUP_ID {
                            // Actor mode: route data partition messages to real partition actors.
                            self.handle_raft_message_actor_mode(
                                group_msg.group_id,
                                group_msg.message,
                                ctx,
                            );
                        } else {
                            // Legacy mode or controller messages: use MultiRaft.
                            let outputs = self.multi_raft.handle_message(
                                group_msg.group_id,
                                group_msg.message,
                            );
                            self.process_outputs(&outputs, ctx);
                        }
                    }
                }
                Err(e) => {
                    warn!(actor = %self.name, error = %e, "Failed to decode group batch");
                }
            }
        } else if is_heartbeat(tag) {
            // Decode and record heartbeat.
            match decode_broker_heartbeat(data) {
                Ok((heartbeat, _consumed)) => {
                    debug!(
                        actor = %self.name,
                        from = heartbeat.node_id.get(),
                        timestamp = heartbeat.timestamp_ms,
                        "Received heartbeat"
                    );
                    // Update controller state with heartbeat.
                    self.controller_state.record_heartbeat(heartbeat.node_id, heartbeat.timestamp_ms);
                }
                Err(e) => {
                    warn!(actor = %self.name, error = %e, "Failed to decode heartbeat");
                }
            }
        }

        // Drain any response messages.
        self.transport.drain_and_schedule(ctx);
    }

    /// Routes a Raft message to a real partition actor (actor mode).
    fn handle_raft_message_actor_mode(
        &mut self,
        group_id: GroupId,
        message: helix_raft::Message,
        ctx: &mut SimulationContext,
    ) {
        let Some(runtime) = self.actor_runtime.as_ref() else {
            return;
        };

        // Find the partition actor handle.
        if let Some(handle) = self.partition_actor_handles.get(&group_id) {
            // Send message to real partition actor.
            let from = message.from();
            eprintln!(
                "[RECV] {} received raft message for group {} from node {}",
                self.name, group_id.get(), from.get()
            );
            runtime.block_on(async {
                let _ = handle.send_raft_message(from, message).await;
            });

            // Give the runtime time to process the message on worker threads.
            runtime.block_on(async {
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
            });

            // Drain outputs from all partition actors.
            self.process_partition_actor_outputs(ctx);
        } else {
            debug!(
                actor = %self.name,
                group = group_id.get(),
                "No partition actor for group (may not be created yet)"
            );
        }
    }

    /// Handles crash event.
    fn handle_crash(&mut self) {
        if self.crashed {
            return;
        }

        self.crashed = true;

        // Mark as crashed in shared state for quorum tracking.
        if let Ok(mut ns) = self.network_state.lock() {
            ns.mark_crashed(self.actor_id);
        }

        // Simulate storage crash - revert to last synced state.
        self.storage.simulate_crash();

        // Clear pending transport messages.
        self.transport.clear_pending();

        // Drop the shared WAL pool (this shuts down its Tokio runtime).
        // It will be recreated on recovery.
        self.shared_wal_pool = None;

        // Shutdown all partition actors (actor mode).
        // We clear the handles which drops the senders, causing actors to shutdown.
        if self.actor_mode {
            // Shutdown each partition actor gracefully.
            if let Some(ref runtime) = self.actor_runtime {
                for handle in self.partition_actor_handles.values() {
                    runtime.block_on(async {
                        let _ = handle.shutdown().await;
                    });
                }
            }
            self.partition_actor_handles.clear();

            // Drop and recreate output channel for fresh recovery.
            self.partition_output_tx = None;
            self.partition_output_rx = None;
        }

        info!(
            actor = %self.name,
            "CRASHED - volatile state lost, storage reverted to last sync"
        );
    }

    /// Handles recovery event.
    fn handle_recover(&mut self, ctx: &mut SimulationContext) {
        if !self.crashed {
            return;
        }

        info!(actor = %self.name, "RECOVERING - reinitializing state");

        // Recreate Multi-Raft.
        self.multi_raft = MultiRaft::new(self.node_id);

        // Note: We do NOT set default_peers for auto-creating groups here.
        // Data partition groups will be created when the controller log is
        // replayed (via AssignPartition commands), which ensures partition_storage
        // is also created. See comment in HelixServiceActor::new for details.

        // Recreate controller group.
        if let Err(e) = self.multi_raft.create_group(CONTROLLER_GROUP_ID, self.cluster_nodes.clone()) {
            error!(actor = %self.name, error = %e, "Failed to recreate controller group");
        }

        // Recreate SharedWAL pool if in Shared mode.
        // Use self.storage.clone() so we share the same storage that was reverted by simulate_crash().
        // This allows SharedWAL to recover data that was synced before the crash.
        if self.wal_mode == WalMode::Shared && self.shared_wal_count > 0 {
            let pool_dir = format!("/node-{}/shared-wal", self.node_id.get());
            let pool_config = PoolConfig::new(&pool_dir, self.shared_wal_count);
            match DstSharedWalPool::open(self.storage.clone(), pool_config) {
                Ok(pool) => {
                    self.shared_wal_pool = Some(pool);
                }
                Err(e) => {
                    error!(error = %e, "Failed to recreate SharedWalPool on recovery");
                    self.shared_wal_pool = None;
                    // Fall back to in-memory mode.
                    self.wal_mode = WalMode::InMemory;
                }
            }
        }

        // Recreate actor mode output channel if needed.
        // Partition actors will be recreated when controller replays AssignPartition commands.
        if self.actor_mode && self.partition_output_tx.is_none() {
            let (tx, rx) = mpsc::channel(1000);
            self.partition_output_tx = Some(tx);
            self.partition_output_rx = Some(rx);
            info!(actor = %self.name, "Recreated actor mode output channel on recovery");
        }

        // Clear in-memory state.
        self.partition_storage.clear();
        self.group_map = GroupMap::new();
        self.controller_state = ControllerState::new();
        self.tick_count = 0;
        self.committed_data.clear();

        // Recover partition data based on WAL mode.
        match self.wal_mode {
            WalMode::InMemory => {
                // Recover from SimulatedStorage files (written during verification).
                self.recover_partitions_from_storage();
            }
            WalMode::PerPartition => {
                // Recover from per-partition WALs.
                self.recover_partitions_from_per_partition_wal();
            }
            WalMode::Shared => {
                // Recover from SharedWAL pool.
                self.recover_partitions_from_shared_wal();
            }
        }

        self.crashed = false;

        // Mark as recovered in shared state for quorum tracking.
        if let Ok(mut ns) = self.network_state.lock() {
            ns.mark_recovered(self.actor_id);
        }

        // Restart timers.
        self.schedule_tick(ctx);
        self.schedule_heartbeat(ctx);

        info!(
            actor = %self.name,
            recovered_partitions = self.partition_storage.len(),
            recovered_records = self.committed_data.values().map(Vec::len).sum::<usize>(),
            "RECOVERED - restarted as follower with durable data"
        );
    }

    /// Recovers partition data from `SimulatedStorage` after crash.
    fn recover_partitions_from_storage(&mut self) {
        // Scan SimulatedStorage for partition data files.
        // Path format: /partitions/{topic_id}/{partition_id}/offset_{offset}
        // Use list_files_sync with empty extension to get all files.
        let partitions_dir = std::path::PathBuf::from("/partitions");

        // Get all files under /partitions (recursive via path prefix matching).
        let Ok(all_files) = self.storage.list_files_sync(&partitions_dir, "") else {
            return;
        };

        // Group files by (topic_id, partition_id).
        let mut partition_files: BTreeMap<(TopicId, PartitionId), Vec<std::path::PathBuf>> = BTreeMap::new();

        for file_path in all_files {
            // Parse path: /partitions/{topic_id}/{partition_id}/offset_{offset}
            let path_str = file_path.to_string_lossy();
            let parts: Vec<&str> = path_str.split('/').collect();

            // Expected: ["", "partitions", "{topic_id}", "{partition_id}", "offset_{offset}"]
            if parts.len() >= 5 && parts[1] == "partitions" {
                if let (Ok(topic_id), Ok(partition_id)) = (
                    parts[2].parse::<u64>(),
                    parts[3].parse::<u64>(),
                ) {
                    let key = (TopicId::new(topic_id), PartitionId::new(partition_id));
                    partition_files.entry(key).or_default().push(file_path);
                }
            }
        }

        // Recover each partition.
        for ((topic_id, partition_id), _files) in partition_files {
            // Load persisted records for this partition.
            let persisted = self.load_persisted_records(topic_id, partition_id);

            if persisted.is_empty() {
                continue;
            }

            // Recreate the partition storage.
            let group_id = GroupId::new(topic_id.get() * 1000 + partition_id.get());
            self.group_map.insert(topic_id, partition_id, group_id);

            let mut ps = SimulatedPartitionStorage::new_in_memory(topic_id, partition_id);

            // Rebuild committed_data tracking and partition storage.
            let committed = self.committed_data
                .entry((topic_id, partition_id))
                .or_default();

            for (offset, data) in persisted {
                let data_hash = Self::simple_hash(&data);
                committed.push((offset, data_hash));

                // Re-append to in-memory partition storage via apply_entry_sync.
                let record = Record::new(Bytes::from(data));
                let command = PartitionCommand::Append {
                    records: vec![record],
                };
                let encoded = command.encode();
                // Use a synthetic log index based on offset.
                let log_index = LogIndex::new(offset.get() + 1);
                let _ = ps.apply_entry_sync(log_index, &encoded);
            }

            // Recreate Raft group for this partition.
            if let Err(e) = self.multi_raft.create_group(group_id, self.cluster_nodes.clone()) {
                warn!(actor = %self.name, group = group_id.get(), error = %e, "Failed to recreate partition group");
            }

            self.partition_storage.insert(group_id, ps);
        }
    }

    /// Recovers partition data from per-partition WALs after crash.
    ///
    /// Scans for WAL directories and recreates `DurablePartition` instances
    /// that will recover their state from the WAL.
    fn recover_partitions_from_per_partition_wal(&mut self) {
        // Scan for partition WAL directories.
        // Path format: /node-{id}/partitions/{topic_id}/{partition_id}/
        let base_dir = std::path::PathBuf::from(format!("/node-{}/partitions", self.node_id.get()));

        // Get all files under the partitions directory.
        let Ok(all_files) = self.storage.list_files_sync(&base_dir, "") else {
            info!(actor = %self.name, "No partition WAL directories found for recovery");
            return;
        };

        // Extract unique (topic_id, partition_id) pairs from paths.
        let mut partitions: std::collections::BTreeSet<(TopicId, PartitionId)> = std::collections::BTreeSet::new();

        for file_path in all_files {
            // Parse path: /node-{id}/partitions/{topic_id}/{partition_id}/...
            let path_str = file_path.to_string_lossy();
            let parts: Vec<&str> = path_str.split('/').collect();

            // Expected: ["", "node-{id}", "partitions", "{topic_id}", "{partition_id}", ...]
            if parts.len() >= 5 && parts[2] == "partitions" {
                if let (Ok(topic_id), Ok(partition_id)) = (
                    parts[3].parse::<u64>(),
                    parts[4].parse::<u64>(),
                ) {
                    partitions.insert((TopicId::new(topic_id), PartitionId::new(partition_id)));
                }
            }
        }

        info!(
            actor = %self.name,
            partition_count = partitions.len(),
            "Found partitions to recover from per-partition WAL"
        );

        // Recover each partition by creating a DurablePartition (which recovers from WAL).
        for (topic_id, partition_id) in partitions {
            let group_id = GroupId::new(topic_id.get() * 1000 + partition_id.get());

            // Update group map.
            self.group_map.insert(topic_id, partition_id, group_id);

            // Create DurablePartition with the same path as used in initialize_partition_storage.
            // Use the tokio runtime for BufferedWal's background tasks.
            let storage = if let Some(ref runtime) = self.per_partition_runtime {
                let data_dir = std::path::PathBuf::from(format!(
                    "/node-{}/partitions/{}/{}",
                    self.node_id.get(),
                    topic_id.get(),
                    partition_id.get()
                ));

                match runtime.block_on(SimulatedPartitionStorage::new_durable(
                    self.storage.clone(),
                    &data_dir,
                    None, // No object storage for DST
                    None, // No tiering config for DST
                    topic_id,
                    partition_id,
                )) {
                    Ok(ps) => ps,
                    Err(e) => {
                        warn!(
                            actor = %self.name,
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            error = %e,
                            "Failed to recover partition from WAL, using in-memory"
                        );
                        SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                    }
                }
            } else {
                warn!(
                    actor = %self.name,
                    "No tokio runtime for per-partition recovery, using in-memory"
                );
                SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
            };

            // Recreate Raft group for this partition.
            if let Err(e) = self.multi_raft.create_group(group_id, self.cluster_nodes.clone()) {
                warn!(
                    actor = %self.name,
                    group = group_id.get(),
                    error = %e,
                    "Failed to recreate partition group during per-partition WAL recovery"
                );
            }

            self.partition_storage.insert(group_id, storage);

            info!(
                actor = %self.name,
                topic = topic_id.get(),
                partition = partition_id.get(),
                "Recovered partition from per-partition WAL"
            );
        }
    }

    /// Recovers partition data from `SharedWAL` after crash.
    ///
    /// This is the `SharedWAL`-aware recovery path that properly recovers entries
    /// from the `SharedWAL` pool and creates `DurablePartition` instances.
    fn recover_partitions_from_shared_wal(&mut self) {
        let Some(ref pool) = self.shared_wal_pool else {
            warn!(actor = %self.name, "No SharedWAL pool available for recovery");
            return;
        };

        // Recover all entries from the SharedWAL pool.
        let recovered_by_partition = match pool.recover() {
            Ok(entries) => entries,
            Err(e) => {
                warn!(actor = %self.name, error = %e, "Failed to recover from SharedWAL");
                return;
            }
        };

        info!(
            actor = %self.name,
            partitions = recovered_by_partition.len(),
            total_entries = recovered_by_partition.values().map(Vec::len).sum::<usize>(),
            "Recovered entries from SharedWAL"
        );

        // Create partition storage for each recovered partition.
        for (partition_id, entries) in recovered_by_partition {
            if entries.is_empty() {
                continue;
            }

            // Infer topic_id from stored entries (entries contain partition data).
            // For DST, we use a simple mapping: topic_id = 1 (single topic per test).
            // TODO: Store topic_id in SharedEntry if needed for multi-topic support.
            let topic_id = TopicId::new(1);

            // Create group ID using same formula as initialize_partition_storage.
            let group_id = GroupId::new(topic_id.get() * 1000 + partition_id.get());

            // Update group map.
            self.group_map.insert(topic_id, partition_id, group_id);

            // Create DurablePartition with recovered entries.
            let handle = pool.handle(partition_id);
            let data_dir = std::path::PathBuf::from("/data");

            let storage = match pool.runtime().block_on(
                SimulatedPartitionStorage::new_durable_with_shared_wal(
                    &data_dir,
                    topic_id,
                    partition_id,
                    handle,
                    entries.clone(),
                    None, // No object storage for DST
                    None, // No tiering config for DST
                )
            ) {
                Ok(ps) => ps,
                Err(e) => {
                    warn!(
                        actor = %self.name,
                        partition = partition_id.get(),
                        error = %e,
                        "Failed to create DurablePartition during recovery, using in-memory"
                    );
                    SimulatedPartitionStorage::new_in_memory(topic_id, partition_id)
                }
            };

            // Note: We don't rebuild committed_data here. After recovery, the node
            // hasn't committed anything yet - Raft will replay committed entries as needed.
            // The DurablePartition cache is populated from SharedWAL recovery.

            // Recreate Raft group for this partition.
            if let Err(e) = self.multi_raft.create_group(group_id, self.cluster_nodes.clone()) {
                warn!(
                    actor = %self.name,
                    group = group_id.get(),
                    error = %e,
                    "Failed to recreate partition group during SharedWAL recovery"
                );
            }

            self.partition_storage.insert(group_id, storage);

            info!(
                actor = %self.name,
                topic = topic_id.get(),
                partition = partition_id.get(),
                entries = entries.len(),
                "Recovered partition from SharedWAL"
            );
        }
    }

    /// Handles a client produce request.
    fn handle_produce(&mut self, data: &[u8], ctx: &mut SimulationContext) {
        // Simple produce: data format is topic_id (8 bytes) + partition_id (8 bytes) + payload
        if data.len() < 16 {
            warn!(actor = %self.name, "Invalid produce request: too short");
            return;
        }

        let topic_id = TopicId::new(u64::from_le_bytes(data[0..8].try_into().unwrap()));
        let partition_id = PartitionId::new(u64::from_le_bytes(data[8..16].try_into().unwrap()));
        let payload = Bytes::copy_from_slice(&data[16..]);

        // Look up group ID.
        let Some(group_id) = self.group_map.get(topic_id, partition_id) else {
            warn!(
                actor = %self.name,
                topic = topic_id.get(),
                partition = partition_id.get(),
                "Partition not found"
            );
            return;
        };

        // Create a proper Record from the payload.
        let record = Record::new(payload);

        // Create a PartitionCommand::Append with the record.
        let command = PartitionCommand::Append {
            records: vec![record],
        };

        // Encode the command for Raft.
        let encoded = command.encode();

        self.produce_count += 1;

        if self.actor_mode {
            // Actor mode: use REAL production partition actor's propose_batch.
            self.handle_produce_actor_mode(group_id, encoded, ctx);
        } else {
            // Standard mode: just propose without tracking.
            if self.multi_raft.propose(group_id, encoded).is_none() {
                debug!(
                    actor = %self.name,
                    group = group_id.get(),
                    "Not leader, cannot propose"
                );
            }

            // Process any immediate outputs.
            let outputs = self.multi_raft.tick();
            self.process_outputs(&outputs, ctx);
            self.transport.drain_and_schedule(ctx);
        }
    }

    /// Handles produce in actor mode using real production partition actors.
    fn handle_produce_actor_mode(
        &mut self,
        group_id: GroupId,
        data: Bytes,
        ctx: &mut SimulationContext,
    ) {
        let Some(runtime) = self.actor_runtime.as_ref() else {
            warn!(actor = %self.name, "No tokio runtime for actor mode produce");
            return;
        };

        let Some(handle) = self.partition_actor_handles.get(&group_id) else {
            eprintln!(
                "[ACTOR-MODE] {} ERROR: No partition actor for group {} (have {} handles)",
                self.name, group_id.get(), self.partition_actor_handles.len()
            );
            debug!(
                actor = %self.name,
                group = group_id.get(),
                "No partition actor for group (partition may not exist)"
            );
            return;
        };

        // Check if we're the leader before proposing.
        // In production, clients would get NotLeader and redirect. In DST, we just skip.
        let is_leader = {
            let handle_clone = handle.clone();
            runtime.block_on(async { handle_clone.is_leader().await.unwrap_or(false) })
        };

        if !is_leader {
            // Not leader - skip this produce (just like non-actor mode does).
            debug!(
                actor = %self.name,
                group = group_id.get(),
                "Not leader, cannot propose (actor mode)"
            );
            return;
        }

        // Create batch info (same as production batcher does).
        // DST doesn't need notification channels since we track via batch_notify output.
        // Safety: data.len() is bounded by test input sizes (< 4KB typically).
        #[allow(clippy::cast_possible_truncation)]
        let batch_info = BatchProposalInfo {
            first_request_at: std::time::Instant::now(),
            batch_size: 1,
            batch_bytes: data.len() as u32,
            total_records: 1,
            record_counts: vec![1],
            result_txs: vec![], // DST doesn't use notification channels
        };

        // Call the REAL production propose_batch!
        // This is the actual production code path being tested.
        let handle_clone = handle.clone();
        runtime.block_on(async {
            let _ = handle_clone.propose_batch(data, batch_info).await;
        });

        // Give the runtime time to process the command on worker threads.
        runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_micros(100)).await;
        });

        // Drain outputs from all partition actors.
        self.process_partition_actor_outputs(ctx);

        // Drain transport queue.
        self.transport.drain_and_schedule(ctx);
    }

    /// Handles a create topic request.
    fn handle_create_topic(&mut self, data: &[u8], ctx: &mut SimulationContext) {
        // Data format: name_len (4 bytes) + name + partition_count (4 bytes) + replication_factor (4 bytes)
        if data.len() < 12 {
            warn!(actor = %self.name, "Invalid create topic request");
            return;
        }

        let name_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + name_len + 8 {
            warn!(actor = %self.name, "Invalid create topic request: name too long");
            return;
        }

        let Ok(name) = String::from_utf8(data[4..4 + name_len].to_vec()) else {
            warn!(actor = %self.name, "Invalid create topic request: invalid UTF-8 name");
            return;
        };
        let partition_count = u32::from_le_bytes(data[4 + name_len..4 + name_len + 4].try_into().unwrap());
        let replication_factor = u32::from_le_bytes(data[4 + name_len + 4..4 + name_len + 8].try_into().unwrap());

        // Create controller command.
        let cmd = ControllerCommand::CreateTopic {
            name,
            partition_count,
            replication_factor,
        };

        // Propose to controller group.
        let encoded = cmd.encode();
        if self.multi_raft.propose(CONTROLLER_GROUP_ID, encoded).is_none() {
            debug!(
                actor = %self.name,
                "Not controller leader, cannot create topic"
            );
        }

        // Process outputs.
        let outputs = self.multi_raft.tick();
        self.process_outputs(&outputs, ctx);
        self.transport.drain_and_schedule(ctx);
    }
}

impl SimulatedActor for HelixServiceActor {
    fn handle(&mut self, event: EventKind, ctx: &mut SimulationContext) {
        match event {
            // Crash/Recovery - always handle regardless of crash state.
            EventKind::ProcessCrash { .. } => self.handle_crash(),
            EventKind::ProcessRecover { .. } => self.handle_recover(ctx),

            // Skip all other events if crashed.
            _ if self.crashed => {
                tracing::trace!(actor = %self.name, "ignoring event while crashed");
            }

            EventKind::ActorStart { .. } => {
                info!(actor = %self.name, "starting");
                self.schedule_tick(ctx);
                self.schedule_heartbeat(ctx);
            }

            EventKind::TimerFired { timer_id, .. } => {
                match timer_id {
                    timer_ids::TICK => self.handle_tick(ctx),
                    timer_ids::HEARTBEAT => self.handle_heartbeat(ctx),
                    _ => {}
                }
            }

            EventKind::PacketDelivery { payload, from, .. } => {
                // Check for partition before processing.
                let is_partitioned = {
                    let state = self.network_state.lock().expect("lock poisoned");
                    state.is_partitioned(from, self.actor_id)
                };

                if is_partitioned {
                    tracing::trace!(
                        actor = %self.name,
                        %from,
                        "dropping partitioned packet"
                    );
                    return;
                }

                self.handle_message(&payload, ctx);
            }

            EventKind::Custom { name, data, .. } => {
                match name.as_str() {
                    custom_events::PRODUCE => self.handle_produce(&data, ctx),
                    custom_events::CREATE_TOPIC => self.handle_create_topic(&data, ctx),
                    custom_events::COLLECT_STATE => self.handle_collect_state(),
                    custom_events::APPLY_PARTITION => self.handle_apply_partition(&data),
                    custom_events::HEAL_PARTITION => self.handle_heal_partition(&data),
                    custom_events::VERIFY_INTEGRITY => self.handle_verify_integrity(),
                    custom_events::VERIFY_CONSUMER => self.handle_verify_consumer(),
                    _ => {
                        debug!(actor = %self.name, event = %name, "unknown custom event");
                    }
                }
            }

            _ => {}
        }
    }

    fn id(&self) -> ActorId {
        self.actor_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn checkpoint(&self) -> Box<dyn Any + Send> {
        // Checkpoint is complex for full service state.
        // For basic testing, we rely on crash/recover simulation.
        Box::new(())
    }

    fn restore(&mut self, _state: Box<dyn Any + Send>) {
        tracing::warn!(actor = %self.name, "checkpoint restore not implemented");
    }

    fn on_start(&mut self, ctx: &mut SimulationContext) {
        ctx.schedule_after(
            Duration::from_nanos(0),
            EventKind::ActorStart {
                actor: self.actor_id,
            },
        );
    }
}

/// Creates a cluster of Helix service actors for simulation.
///
/// # Arguments
///
/// * `node_count` - Number of nodes (must be 1-7)
/// * `base_seed` - Base seed for deterministic behavior
/// * `property_state` - Shared property state for verification
/// * `fault_config` - Storage fault injection configuration (shared by all nodes)
/// * `wal_mode` - WAL mode: per-partition or shared
/// * `shared_wal_count` - Number of shared WALs (only used when `wal_mode` is `Shared`)
///
/// # Returns
///
/// A tuple of (actors, `network_state`) for the simulation.
///
/// # Panics
///
/// Panics if `node_count` is 0 or greater than 7.
#[must_use]
#[allow(clippy::needless_pass_by_value)] // Arc is cheap to clone, FaultConfig is cloned anyway.
pub fn create_helix_cluster(
    node_count: usize,
    base_seed: u64,
    property_state: SharedHelixPropertyState,
    fault_config: FaultConfig,
    wal_mode: WalMode,
    shared_wal_count: u32,
    actor_mode: bool,
) -> (Vec<HelixServiceActor>, SharedNetworkState) {
    assert!(node_count > 0, "cluster must have at least one node");
    assert!(node_count <= 7, "cluster size exceeds maximum");

    // Create node IDs and actor IDs.
    #[allow(clippy::cast_possible_truncation)]
    let node_count_u64 = node_count as u64;
    let node_ids: Vec<NodeId> = (1..=node_count_u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count_u64).map(ActorId::new).collect();

    // Create mapping.
    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Create shared network state with cluster size for quorum tracking.
    let mut ns = crate::raft_actor::NetworkState::new();
    ns.set_cluster_size(node_count);
    let network_state = Arc::new(Mutex::new(ns));

    // Create actors - each actor creates its OWN shared WAL pool if needed.
    // This mirrors production where each broker has independent storage.
    let actors = node_ids
        .iter()
        .zip(actor_ids.iter())
        .enumerate()
        .map(|(i, (&node_id, &actor_id))| {
            #[allow(clippy::cast_possible_truncation)]
            let seed = base_seed.wrapping_add(i as u64);
            HelixServiceActor::new(
                actor_id,
                node_id,
                node_ids.clone(),
                node_to_actor.clone(),
                Arc::clone(&network_state),
                Arc::clone(&property_state),
                fault_config.clone(),
                seed,
                wal_mode,
                shared_wal_count,
                actor_mode,
            )
        })
        .collect();

    (actors, network_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::properties::HelixPropertyState;

    fn create_test_property_state() -> SharedHelixPropertyState {
        Arc::new(Mutex::new(HelixPropertyState::new()))
    }

    #[test]
    fn test_create_helix_cluster_per_partition_wal() {
        let property_state = create_test_property_state();
        let (actors, _network_state) = create_helix_cluster(
            3,
            42,
            property_state,
            FaultConfig::default(),
            WalMode::InMemory,
            4,
            false, // actor_mode
        );
        assert_eq!(actors.len(), 3);

        for (i, actor) in actors.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let expected_node_id = (i + 1) as u64;
            assert_eq!(actor.node_id().get(), expected_node_id);
            assert!(!actor.is_crashed());
        }
    }

    #[test]
    fn test_create_helix_cluster_shared_wal() {
        let property_state = create_test_property_state();
        let (actors, _network_state) = create_helix_cluster(
            3,
            42,
            property_state,
            FaultConfig::default(),
            WalMode::Shared,
            4,
            false, // actor_mode
        );
        assert_eq!(actors.len(), 3);

        for (i, actor) in actors.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            let expected_node_id = (i + 1) as u64;
            assert_eq!(actor.node_id().get(), expected_node_id);
            assert!(!actor.is_crashed());
        }
    }

    #[test]
    fn test_actor_crash_and_recover() {
        let property_state = create_test_property_state();
        let (mut actors, _network_state) = create_helix_cluster(
            1,
            42,
            property_state,
            FaultConfig::default(),
            WalMode::InMemory,
            4,
            false, // actor_mode
        );
        let actor = &mut actors[0];

        assert!(!actor.is_crashed());
        actor.handle_crash();
        assert!(actor.is_crashed());

        // Can't recover without context, but we can check the flag.
    }

    #[test]
    fn test_create_cluster_with_faults_per_partition() {
        let property_state = create_test_property_state();
        let (actors, _network_state) = create_helix_cluster(
            3,
            42,
            property_state,
            FaultConfig::flaky(),
            WalMode::InMemory,
            4,
            false, // actor_mode
        );
        assert_eq!(actors.len(), 3);
    }

    #[test]
    fn test_create_cluster_with_faults_shared_wal() {
        let property_state = create_test_property_state();
        let (actors, _network_state) = create_helix_cluster(
            3,
            42,
            property_state,
            FaultConfig::flaky(),
            WalMode::Shared,
            4,
            false, // actor_mode
        );
        assert_eq!(actors.len(), 3);
    }
}

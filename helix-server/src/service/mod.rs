//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Multi-Raft for consensus across multiple
//! partition groups, with configurable partition storage.

/// Handler implementations for the Helix service.
pub mod handlers;
/// Request batching for improved throughput.
pub mod batcher;
/// Partition actor for lock-free multi-partition scalability.
pub mod partition_actor;
/// WAL actor for command-channel based WAL access.
pub mod wal_actor;
/// Partition router for lock-free request dispatch.
pub mod router;
/// Output processor for actor-based multi-partition coordination.
pub mod output_processor;
/// Actor-based service setup for lock-free multi-partition coordination.
pub mod actor_setup;
mod tick;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use helix_core::{GroupId, LogIndex, NodeId, Offset, PartitionId, TopicId, WriteDurability};
use helix_progress::{ProgressConfig, ProgressManager, SimulatedProgressStore};
use helix_raft::multi::MultiRaft;
use helix_runtime::{PeerInfo, TransportConfig, TransportError, TransportHandle};
use helix_wal::{PoolConfig, SharedEntry, SharedWalPool, TokioStorage};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{error, info};

use crate::controller::{ControllerState, BROKER_HEARTBEAT_TIMEOUT_MS, CONTROLLER_GROUP_ID};
use crate::group_map::GroupMap;
use crate::partition_storage::ServerPartitionStorage;

use self::router::PartitionRouter;

/// Maximum records per write request.
pub const MAX_RECORDS_PER_WRITE: usize = 1000;

/// Maximum bytes per read response.
pub const MAX_BYTES_PER_READ: u32 = 1024 * 1024;

/// Tick interval in milliseconds.
pub const TICK_INTERVAL_MS: u64 = 50;

/// Heartbeat interval in milliseconds (re-exported for DST use).
#[allow(unused_imports)]
pub use tick::HEARTBEAT_INTERVAL_MS;

/// Actor-based tick task (re-exported for service wiring).
#[allow(unused_imports)]
pub use tick::tick_task_actor;

/// A pending proposal waiting for Raft commit.
///
/// When a client proposes an entry, we track it here. When the entry is
/// committed and applied, we send the result (offset) through the channel.
pub struct PendingProposal {
    /// The Raft log index of the proposed entry.
    pub log_index: helix_core::LogIndex,
    /// Channel to send the result (offset) when the entry is applied.
    pub result_tx: oneshot::Sender<crate::error::ServerResult<Offset>>,
}

/// A pending controller proposal waiting for Raft commit.
///
/// Used for controller commands like `CreateTopic`, `DeleteTopic`, etc.
pub struct PendingControllerProposal {
    /// The Raft log index of the proposed entry.
    pub log_index: helix_core::LogIndex,
    /// Channel to send notification when the entry is committed.
    pub result_tx: oneshot::Sender<crate::error::ServerResult<()>>,
}

/// A pending batched proposal waiting for Raft commit.
///
/// When multiple producer requests are batched into a single `AppendBlobBatch`
/// entry, this struct tracks all the waiters. On commit, the tick task
/// calculates per-request offsets and notifies each waiter.
pub struct BatchPendingProposal {
    /// The Raft log index of the proposed entry.
    pub log_index: helix_core::LogIndex,
    /// Timestamp when the first request entered the batch.
    pub first_request_at: std::time::Instant,
    /// Timestamp when the batch was proposed to Raft.
    pub proposed_at: std::time::Instant,
    /// Number of requests in this batch.
    pub batch_size: u32,
    /// Total bytes in this batch.
    pub batch_bytes: u32,
    /// Total records across all requests in this batch.
    pub total_records: u64,
    /// Record counts for each request in the batch (for offset calculation).
    pub record_counts: Vec<u32>,
    /// Channels to notify each waiter with their assigned offset.
    pub result_txs: Vec<oneshot::Sender<crate::error::ServerResult<Offset>>>,
}

/// Aggregated batcher performance stats for reporting.
#[derive(Default)]
pub struct BatcherStats {
    /// Total number of batch flushes.
    flush_count: std::sync::atomic::AtomicU64,
    /// Flushes triggered by linger timeout.
    flush_linger_count: std::sync::atomic::AtomicU64,
    /// Flushes triggered by batch size limits.
    flush_size_count: std::sync::atomic::AtomicU64,
    /// Flushes triggered by shutdown.
    flush_shutdown_count: std::sync::atomic::AtomicU64,
    /// Total requests observed across all batches.
    total_batch_requests: std::sync::atomic::AtomicU64,
    /// Total bytes observed across all batches.
    total_batch_bytes: std::sync::atomic::AtomicU64,
    /// Total records observed across all batches.
    total_batch_records: std::sync::atomic::AtomicU64,
    /// Total batch age at flush time (microseconds).
    total_batch_age_us: std::sync::atomic::AtomicU64,
    /// Total number of committed batches.
    commit_count: std::sync::atomic::AtomicU64,
    /// Total commit latency after proposal (microseconds).
    total_commit_latency_us: std::sync::atomic::AtomicU64,
    /// Total batch wait time before proposal (microseconds).
    total_batch_wait_us: std::sync::atomic::AtomicU64,
    /// Total time from first request to commit (microseconds).
    total_total_age_us: std::sync::atomic::AtomicU64,
    /// Number of batch flushes rejected due to not being leader.
    not_leader_count: std::sync::atomic::AtomicU64,
    /// Number of batch apply errors on commit.
    apply_error_count: std::sync::atomic::AtomicU64,
}

impl BatcherStats {
    /// Records a batch flush observation.
    pub fn record_flush(
        &self,
        reason: &str,
        batch_requests: u64,
        batch_bytes: u64,
        batch_records: u64,
        batch_age_us: u64,
    ) {
        self.flush_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match reason {
            "linger" => {
                self.flush_linger_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            "size" => {
                self.flush_size_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            "shutdown" => {
                self.flush_shutdown_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {}
        }
        self.total_batch_requests
            .fetch_add(batch_requests, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_bytes
            .fetch_add(batch_bytes, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_records
            .fetch_add(batch_records, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_age_us
            .fetch_add(batch_age_us, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records a batch commit observation.
    pub fn record_commit(
        &self,
        batch_requests: u64,
        batch_bytes: u64,
        batch_records: u64,
        batch_wait_us: u64,
        commit_latency_us: u64,
        total_age_us: u64,
    ) {
        self.commit_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_requests
            .fetch_add(batch_requests, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_bytes
            .fetch_add(batch_bytes, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_records
            .fetch_add(batch_records, std::sync::atomic::Ordering::Relaxed);
        self.total_batch_wait_us
            .fetch_add(batch_wait_us, std::sync::atomic::Ordering::Relaxed);
        self.total_commit_latency_us
            .fetch_add(commit_latency_us, std::sync::atomic::Ordering::Relaxed);
        self.total_total_age_us
            .fetch_add(total_age_us, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records a batch flush rejection due to not being leader.
    pub fn record_not_leader(&self) {
        self.not_leader_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records a batch apply error on commit.
    pub fn record_apply_error(&self) {
        self.apply_error_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Takes a snapshot of the current counters.
    pub fn snapshot(&self) -> BatcherStatsSnapshot {
        BatcherStatsSnapshot {
            flush_count: self.flush_count.load(std::sync::atomic::Ordering::Relaxed),
            flush_linger_count: self
                .flush_linger_count
                .load(std::sync::atomic::Ordering::Relaxed),
            flush_size_count: self
                .flush_size_count
                .load(std::sync::atomic::Ordering::Relaxed),
            flush_shutdown_count: self
                .flush_shutdown_count
                .load(std::sync::atomic::Ordering::Relaxed),
            total_batch_requests: self
                .total_batch_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            total_batch_bytes: self
                .total_batch_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            total_batch_records: self
                .total_batch_records
                .load(std::sync::atomic::Ordering::Relaxed),
            total_batch_age_us: self
                .total_batch_age_us
                .load(std::sync::atomic::Ordering::Relaxed),
            commit_count: self.commit_count.load(std::sync::atomic::Ordering::Relaxed),
            total_commit_latency_us: self
                .total_commit_latency_us
                .load(std::sync::atomic::Ordering::Relaxed),
            total_batch_wait_us: self
                .total_batch_wait_us
                .load(std::sync::atomic::Ordering::Relaxed),
            total_total_age_us: self
                .total_total_age_us
                .load(std::sync::atomic::Ordering::Relaxed),
            not_leader_count: self
                .not_leader_count
                .load(std::sync::atomic::Ordering::Relaxed),
            apply_error_count: self
                .apply_error_count
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Snapshot of batcher stats for reporting.
pub struct BatcherStatsSnapshot {
    /// Total number of batch flushes.
    pub flush_count: u64,
    /// Flushes triggered by linger timeout.
    pub flush_linger_count: u64,
    /// Flushes triggered by batch size limits.
    pub flush_size_count: u64,
    /// Flushes triggered by shutdown.
    pub flush_shutdown_count: u64,
    /// Total requests observed across all batches.
    pub total_batch_requests: u64,
    /// Total bytes observed across all batches.
    pub total_batch_bytes: u64,
    /// Total records observed across all batches.
    pub total_batch_records: u64,
    /// Total batch age at flush time (microseconds).
    pub total_batch_age_us: u64,
    /// Total number of committed batches.
    pub commit_count: u64,
    /// Total commit latency after proposal (microseconds).
    pub total_commit_latency_us: u64,
    /// Total batch wait time before proposal (microseconds).
    pub total_batch_wait_us: u64,
    /// Total time from first request to commit (microseconds).
    pub total_total_age_us: u64,
    /// Number of batch flushes rejected due to not being leader.
    pub not_leader_count: u64,
    /// Number of batch apply errors on commit.
    pub apply_error_count: u64,
}

impl BatcherStatsSnapshot {
    /// Formats the snapshot as a JSON string for reporting.
    #[must_use] 
    pub fn to_json(&self, node_id: u64, timestamp_ms: u64) -> String {
        format!(
            "{{\"node_id\":{node_id},\"timestamp_ms\":{timestamp_ms},\"flush_count\":{flush_count},\"flush_linger_count\":{flush_linger_count},\"flush_size_count\":{flush_size_count},\"flush_shutdown_count\":{flush_shutdown_count},\"commit_count\":{commit_count},\"total_batch_requests\":{total_batch_requests},\"total_batch_bytes\":{total_batch_bytes},\"total_batch_records\":{total_batch_records},\"total_batch_age_us\":{total_batch_age_us},\"total_batch_wait_us\":{total_batch_wait_us},\"total_commit_latency_us\":{total_commit_latency_us},\"total_total_age_us\":{total_total_age_us},\"not_leader_count\":{not_leader_count},\"apply_error_count\":{apply_error_count}}}",
            node_id = node_id,
            timestamp_ms = timestamp_ms,
            flush_count = self.flush_count,
            flush_linger_count = self.flush_linger_count,
            flush_size_count = self.flush_size_count,
            flush_shutdown_count = self.flush_shutdown_count,
            commit_count = self.commit_count,
            total_batch_requests = self.total_batch_requests,
            total_batch_bytes = self.total_batch_bytes,
            total_batch_records = self.total_batch_records,
            total_batch_age_us = self.total_batch_age_us,
            total_batch_wait_us = self.total_batch_wait_us,
            total_commit_latency_us = self.total_commit_latency_us,
            total_total_age_us = self.total_total_age_us,
            not_leader_count = self.not_leader_count,
            apply_error_count = self.apply_error_count,
        )
    }
}

/// Topic metadata.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Number of partitions.
    pub partition_count: i32,
}

/// The Helix gRPC service backed by Multi-Raft.
///
/// This provides a Raft-replicated implementation using the Multi-Raft
/// engine for efficient management of many partition groups.
pub struct HelixService {
    /// Cluster ID.
    pub(crate) cluster_id: String,
    /// This node's ID.
    pub(crate) node_id: NodeId,
    /// Multi-Raft engine for consensus.
    pub(crate) multi_raft: Arc<RwLock<MultiRaft>>,
    /// Partition storage indexed by `GroupId`.
    pub(crate) partition_storage: Arc<RwLock<HashMap<GroupId, ServerPartitionStorage>>>,
    /// Group ID mapping.
    pub(crate) group_map: Arc<RwLock<GroupMap>>,
    /// Topic name to metadata mapping.
    pub(crate) topics: Arc<RwLock<HashMap<String, TopicMetadata>>>,
    /// Next topic ID.
    pub(crate) next_topic_id: Arc<RwLock<u64>>,
    /// All nodes in the cluster.
    pub(crate) cluster_nodes: Vec<NodeId>,
    /// Peer addresses indexed by `NodeId` (for metadata responses).
    pub(crate) peer_addrs: HashMap<NodeId, String>,
    /// Shutdown signal sender.
    pub(crate) _shutdown_tx: mpsc::Sender<()>,
    /// Data directory for durable storage (None = in-memory only).
    pub(crate) data_dir: Option<PathBuf>,
    /// Object storage directory for tiering (None = simulated storage).
    pub(crate) object_storage_dir: Option<PathBuf>,
    /// S3 configuration for tiering (None = use filesystem or simulated).
    #[cfg(feature = "s3")]
    pub(crate) s3_config: Option<helix_tier::S3Config>,
    /// Tiering configuration (None = tiering disabled).
    pub(crate) tiering_config: Option<helix_tier::TieringConfig>,
    /// Transport handle for sending Raft messages (multi-node only).
    #[allow(dead_code)]
    pub(crate) transport_handle: Option<TransportHandle>,
    /// Progress manager for consumer group tracking.
    pub(crate) progress_manager: Arc<ProgressManager<SimulatedProgressStore>>,
    /// Controller state machine (cluster metadata).
    pub(crate) controller_state: Arc<RwLock<ControllerState>>,
    /// Pending proposals waiting for Raft commit (multi-node mode only).
    /// Indexed by (`GroupId`, `LogIndex`) for O(1) lookup on commit.
    pub(crate) pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, PendingProposal>>>>,
    /// Pending controller proposals waiting for Raft commit.
    pub(crate) pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    /// Local broker heartbeat timestamps (soft state, not Raft-replicated).
    ///
    /// Following Kafka `KRaft` pattern, heartbeats are maintained as soft state
    /// on each node. Each broker sends heartbeats via transport to all peers,
    /// and each node maintains its own view of broker liveness.
    pub(crate) local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    /// Shared WAL pool for fsync amortization. Present when `data_dir` is set.
    pub(crate) shared_wal_pool: Option<Arc<SharedWalPool<TokioStorage>>>,
    /// Recovered entries from shared WAL, indexed by `PartitionId`.
    /// Used during partition creation to restore state (Phase 3).
    #[allow(dead_code)] // Used in Phase 3 of SharedWAL integration.
    pub(crate) recovered_entries: Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>,
    /// Pending batched proposals waiting for Raft commit (multi-node mode only).
    /// Indexed by (`GroupId`, `LogIndex`) for O(1) lookup on commit.
    /// Note: This field is shared via Arc with the tick task, not read directly here.
    #[allow(dead_code)]
    pub(crate) batch_pending_proposals: Arc<RwLock<HashMap<GroupId, HashMap<LogIndex, BatchPendingProposal>>>>,
    /// Handle to submit requests to the batcher (multi-node mode only).
    pub(crate) batcher_handle: Option<batcher::BatcherHandle>,
    /// Aggregated batcher performance stats (multi-node mode only).
    pub(crate) batcher_stats: Option<Arc<BatcherStats>>,
    /// Whether actor mode is enabled (lock-free multi-partition).
    #[allow(dead_code)] // Used by handlers in follow-up integration.
    pub(crate) actor_mode: bool,
    /// Partition router for actor mode (lock-free request dispatch).
    #[allow(dead_code)] // Used by handlers in follow-up integration.
    pub(crate) actor_router: Option<Arc<PartitionRouter>>,
    /// Shutdown sender for actor tick task (data partitions).
    #[allow(dead_code)] // Used for graceful shutdown in follow-up.
    pub(crate) actor_shutdown_tx: Option<mpsc::Sender<()>>,
    /// Shutdown sender for controller tick task (actor mode only).
    #[allow(dead_code)] // Used for graceful shutdown in follow-up.
    pub(crate) controller_shutdown_tx: Option<mpsc::Sender<()>>,
    /// Backpressure state for actor mode.
    #[allow(dead_code)] // Used by handlers in follow-up integration.
    pub(crate) actor_backpressure: Option<Arc<batcher::BackpressureState>>,
}

impl HelixService {
    /// Creates a new Helix service with in-memory storage (for testing).
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    pub async fn new(cluster_id: String, node_id: u64) -> Self {
        // In-memory mode doesn't use WAL, but pass Fsync for safety if WAL is added later.
        Self::new_internal(cluster_id, node_id, None, None, None, WriteDurability::Fsync).await
    }

    /// Creates a new Helix service with durable WAL-backed storage.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    /// Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `cluster_id` - Unique cluster identifier
    /// * `node_id` - This node's ID
    /// * `data_dir` - Directory for durable storage
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    pub async fn with_data_dir(
        cluster_id: String,
        node_id: u64,
        data_dir: PathBuf,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
    ) -> Self {
        Self::new_internal(cluster_id, node_id, Some(data_dir), None, shared_wal_count, write_durability).await
    }

    /// Creates a new Helix service with durable storage and object storage for tiering.
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    /// Partition data is persisted to `data_dir`, and tiered segments are stored
    /// in `object_storage_dir`.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    pub async fn with_data_and_object_storage(
        cluster_id: String,
        node_id: u64,
        data_dir: PathBuf,
        object_storage_dir: PathBuf,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
    ) -> Self {
        Self::new_internal(
            cluster_id,
            node_id,
            Some(data_dir),
            Some(object_storage_dir),
            shared_wal_count,
            write_durability,
        )
        .await
    }

    /// Internal constructor.
    async fn new_internal(
        cluster_id: String,
        node_id: u64,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
    ) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let pending_proposals = Arc::new(RwLock::new(HashMap::new()));

        // Create progress manager with simulated store.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        // Initialize SharedWalPool if data_dir is set.
        let (shared_wal_pool, recovered_entries) = if let Some(ref dir) = data_dir {
            // Determine WAL count (default 4, or user override).
            let wal_count = shared_wal_count.unwrap_or(4);
            assert!(
                (1..=16).contains(&wal_count),
                "shared_wal_count must be in range [1, 16]"
            );

            info!(
                wal_count,
                data_dir = ?dir,
                "Initializing SharedWalPool"
            );

            // Create pool config with durability setting.
            let pool_config = PoolConfig::new(dir.join("shared-wal"), wal_count)
                .with_flush_interval(std::time::Duration::from_millis(1))
                .with_max_buffer_entries(1000)
                .with_durability(write_durability);

            info!(
                wal_count,
                durability = %write_durability,
                "SharedWalPool durability mode"
            );

            // Open pool.
            let pool = SharedWalPool::open(TokioStorage::new(), pool_config)
                .await
                .expect("Failed to open SharedWalPool");

            // Recover all partitions.
            let recovered = pool.recover().await.expect("Failed to recover from SharedWalPool");

            info!(
                partitions = recovered.len(),
                "SharedWalPool recovery complete"
            );

            (Some(Arc::new(pool)), Arc::new(RwLock::new(recovered)))
        } else {
            // In-memory mode.
            (None, Arc::new(RwLock::new(HashMap::new())))
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Start background tick task.
        tokio::spawn(tick::tick_task(
            Arc::clone(&multi_raft),
            Arc::clone(&partition_storage),
            Arc::clone(&group_map),
            Arc::clone(&pending_proposals),
            shutdown_rx,
        ));

        Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs: HashMap::new(),
            _shutdown_tx: shutdown_tx,
            data_dir,
            object_storage_dir,
            #[cfg(feature = "s3")]
            s3_config: None,
            tiering_config: None,
            transport_handle: None,
            progress_manager,
            controller_state: Arc::new(RwLock::new(ControllerState::new())),
            pending_proposals,
            pending_controller_proposals: Arc::new(RwLock::new(Vec::new())),
            local_broker_heartbeats: Arc::new(RwLock::new(HashMap::new())),
            shared_wal_pool,
            recovered_entries,
            batch_pending_proposals: Arc::new(RwLock::new(HashMap::new())),
            batcher_handle: None, // No batching in single-node mode.
            batcher_stats: None,
            actor_mode: false, // Single-node uses lock-based approach.
            actor_router: None,
            actor_shutdown_tx: None,
            controller_shutdown_tx: None,
            actor_backpressure: None,
        }
    }

    /// Creates a new Helix service with multi-node networking.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `enable_actor_mode` - If true, uses lock-free actor-based architecture
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    ///
    /// # Panics
    /// Panics if `shared_wal_count` is not in range [1, 16], or if the `SharedWalPool`
    /// fails to open or recover (indicates filesystem or corruption issues).
    #[allow(clippy::too_many_arguments)] // Constructor naturally needs many parameters.
    #[allow(clippy::too_many_lines)] // Constructor with initialization logic.
    #[cfg(feature = "s3")]
    pub async fn new_multi_node(
        cluster_id: String,
        node_id: u64,
        listen_addr: SocketAddr,
        peers: Vec<PeerInfo>,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        s3_config: Option<helix_tier::S3Config>,
        tiering_config: Option<helix_tier::TieringConfig>,
        kafka_addr: String,
        kafka_peer_addrs: HashMap<NodeId, String>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        enable_actor_mode: bool,
    ) -> Result<Self, TransportError> {
        Self::new_multi_node_internal(
            cluster_id,
            node_id,
            listen_addr,
            peers,
            data_dir,
            object_storage_dir,
            s3_config,
            tiering_config,
            kafka_addr,
            kafka_peer_addrs,
            shared_wal_count,
            write_durability,
            enable_actor_mode,
        )
        .await
    }

    /// Creates a new Helix service with multi-node networking.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `enable_actor_mode` - If true, uses lock-free actor-based architecture
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    ///
    /// # Panics
    /// Panics if `shared_wal_count` is not in range [1, 16], or if the `SharedWalPool`
    /// fails to open or recover (indicates filesystem or corruption issues).
    #[allow(clippy::too_many_arguments)]
    #[cfg(not(feature = "s3"))]
    pub async fn new_multi_node(
        cluster_id: String,
        node_id: u64,
        listen_addr: SocketAddr,
        peers: Vec<PeerInfo>,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        tiering_config: Option<helix_tier::TieringConfig>,
        kafka_addr: String,
        kafka_peer_addrs: HashMap<NodeId, String>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        enable_actor_mode: bool,
    ) -> Result<Self, TransportError> {
        Self::new_multi_node_internal(
            cluster_id,
            node_id,
            listen_addr,
            peers,
            data_dir,
            object_storage_dir,
            tiering_config,
            kafka_addr,
            kafka_peer_addrs,
            shared_wal_count,
            write_durability,
            enable_actor_mode,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn new_multi_node_internal(
        cluster_id: String,
        node_id: u64,
        listen_addr: SocketAddr,
        peers: Vec<PeerInfo>,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        #[cfg(feature = "s3")] s3_config: Option<helix_tier::S3Config>,
        tiering_config: Option<helix_tier::TieringConfig>,
        kafka_addr: String,
        kafka_peer_addrs: HashMap<NodeId, String>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        enable_actor_mode: bool,
    ) -> Result<Self, TransportError> {
        let node_id = NodeId::new(node_id);

        // Build cluster nodes list (self + peers).
        let mut cluster_nodes = vec![node_id];
        cluster_nodes.extend(peers.iter().map(|p| p.node_id));

        // Create and start transport.
        let mut transport_config = TransportConfig::new(node_id, listen_addr);
        for peer in &peers {
            transport_config = transport_config.with_peer(peer.node_id, peer.addr.clone());
        }

        let (transport, incoming_rx) = helix_runtime::Transport::new(transport_config);
        let transport_handle = transport.start().await?;

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage = Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let controller_state = Arc::new(RwLock::new(ControllerState::new()));
        let pending_proposals = Arc::new(RwLock::new(HashMap::new()));
        let pending_controller_proposals = Arc::new(RwLock::new(Vec::new()));
        let local_broker_heartbeats = Arc::new(RwLock::new(HashMap::new()));

        // Note: We do NOT set default_peers for auto-creating groups.
        // Data partition groups must only be created via the controller's
        // AssignPartition command, which ensures partition_storage is also
        // created. Auto-creating Raft groups on message receipt would cause
        // commits to be dropped because partition_storage doesn't exist yet.

        // Create controller partition (group 0) with all cluster nodes.
        {
            let mut mr = multi_raft.write().await;
            if let Err(e) = mr.create_group(CONTROLLER_GROUP_ID, cluster_nodes.clone()) {
                error!(error = %e, "Failed to create controller partition");
            } else {
                info!(
                    group_id = CONTROLLER_GROUP_ID.get(),
                    nodes = ?cluster_nodes.iter().map(|n| n.get()).collect::<Vec<_>>(),
                    "Created controller partition"
                );
            }
        }

        // Create progress manager.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        // Initialize SharedWalPool if data_dir is set.
        let (shared_wal_pool, recovered_entries) = if let Some(ref dir) = data_dir {
            // Determine WAL count (default 4, or user override).
            let wal_count = shared_wal_count.unwrap_or(4);
            assert!(
                (1..=16).contains(&wal_count),
                "shared_wal_count must be in range [1, 16]"
            );

            info!(
                wal_count,
                data_dir = ?dir,
                "Initializing SharedWalPool for multi-node"
            );

            // Create pool config with durability setting.
            let pool_config = PoolConfig::new(dir.join("shared-wal"), wal_count)
                .with_flush_interval(std::time::Duration::from_millis(1))
                .with_max_buffer_entries(1000)
                .with_durability(write_durability);

            info!(
                wal_count,
                durability = %write_durability,
                "SharedWalPool durability mode (multi-node)"
            );

            // Open pool.
            let pool = SharedWalPool::open(TokioStorage::new(), pool_config)
                .await
                .expect("Failed to open SharedWalPool");

            // Recover all partitions.
            let recovered = pool.recover().await.expect("Failed to recover from SharedWalPool");

            info!(
                partitions = recovered.len(),
                "SharedWalPool recovery complete for multi-node"
            );

            (Some(Arc::new(pool)), Arc::new(RwLock::new(recovered)))
        } else {
            // In-memory mode.
            (None, Arc::new(RwLock::new(HashMap::new())))
        };

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        // Create batch pending proposals map.
        let batch_pending_proposals = Arc::new(RwLock::new(HashMap::new()));

        // Branch based on actor mode.
        let (batcher_handle, batcher_stats, actor_router, actor_shutdown_tx, controller_shutdown_tx, actor_backpressure) =
            if enable_actor_mode {
                // Actor mode: use lock-free actor-based architecture.
                info!("Initializing actor mode for multi-node service");

                // Create actor-based setup with empty initial groups.
                // Data partitions will be created dynamically via AssignPartition.
                let actor_handles = actor_setup::setup_multi_partition(
                    node_id,
                    cluster_nodes.clone(),
                    HashMap::new(), // Empty initial groups - created via controller.
                    Arc::clone(&partition_storage),
                    Arc::clone(&group_map),
                    Arc::clone(&controller_state),
                    Arc::clone(&pending_proposals),
                    Arc::clone(&pending_controller_proposals),
                    Arc::clone(&batch_pending_proposals),
                    Arc::clone(&local_broker_heartbeats),
                    Arc::clone(&multi_raft),
                    transport_handle.clone(),
                    incoming_rx,
                    actor_setup::ActorSetupConfig::default(),
                )
                .await;

                // Spawn controller tick task (handles controller partition via MultiRaft).
                let controller_shutdown_rx = {
                    let (tx, rx) = mpsc::channel(1);
                    // Store tx in actor_shutdown_tx to signal shutdown.
                    // For now, we'll use the actor_handles.shutdown_tx for the actor tick,
                    // and spawn a separate controller tick.
                    tokio::spawn(tick::tick_task_controller(
                        Arc::clone(&multi_raft),
                        Arc::clone(&partition_storage),
                        Arc::clone(&group_map),
                        Arc::clone(&controller_state),
                        Arc::clone(&pending_proposals),
                        Arc::clone(&pending_controller_proposals),
                        cluster_nodes.clone(),
                        transport_handle.clone(),
                        Arc::clone(&actor_handles.router),
                        actor_handles.output_tx.clone(),
                        rx,
                    ));
                    tx
                };

                info!(
                    node_id = node_id.get(),
                    listen_addr = %listen_addr,
                    peer_count = peers.len(),
                    "Started multi-node Helix service (actor mode)"
                );

                // Return actor handles - keep both shutdown channels alive.
                (
                    Some(actor_handles.batcher_handle),
                    Some(actor_handles.batcher_stats),
                    Some(actor_handles.router),
                    Some(actor_handles.shutdown_tx), // Actor tick task (data partitions)
                    Some(controller_shutdown_rx),    // Controller tick task
                    Some(actor_handles.backpressure),
                )
            } else {
                // Lock-based mode: use existing MultiRaft-based architecture.

                // Create and spawn the batcher task.
                let batcher_stats = Arc::new(BatcherStats::default());
                let (batcher_handle, batcher_rx, backpressure_state) = batcher::create_batcher();
                tokio::spawn(batcher::batcher_task(
                    batcher_rx,
                    Arc::clone(&multi_raft),
                    Arc::clone(&batch_pending_proposals),
                    Some(transport_handle.clone()),
                    Arc::clone(&batcher_stats),
                    batcher::BatcherConfig::default(),
                    Arc::clone(&backpressure_state),
                ));

                info!("Started request batcher for multi-node");

                // Start background tick task with transport.
                #[cfg(feature = "s3")]
                tokio::spawn(tick::tick_task_multi_node(
                    Arc::clone(&multi_raft),
                    Arc::clone(&partition_storage),
                    Arc::clone(&group_map),
                    Arc::clone(&controller_state),
                    Arc::clone(&pending_proposals),
                    Arc::clone(&pending_controller_proposals),
                    Arc::clone(&batch_pending_proposals),
                    Arc::clone(&local_broker_heartbeats),
                    cluster_nodes.clone(),
                    transport_handle.clone(),
                    data_dir.clone(),
                    object_storage_dir.clone(),
                    s3_config.clone(),
                    tiering_config.clone(),
                    shared_wal_pool.clone(),
                    Arc::clone(&recovered_entries),
                    Some(Arc::clone(&batcher_stats)),
                    Some(backpressure_state.clone()),
                    incoming_rx,
                    shutdown_rx,
                ));
                #[cfg(not(feature = "s3"))]
                tokio::spawn(tick::tick_task_multi_node(
                    Arc::clone(&multi_raft),
                    Arc::clone(&partition_storage),
                    Arc::clone(&group_map),
                    Arc::clone(&controller_state),
                    Arc::clone(&pending_proposals),
                    Arc::clone(&pending_controller_proposals),
                    Arc::clone(&batch_pending_proposals),
                    Arc::clone(&local_broker_heartbeats),
                    cluster_nodes.clone(),
                    transport_handle.clone(),
                    data_dir.clone(),
                    object_storage_dir.clone(),
                    tiering_config.clone(),
                    shared_wal_pool.clone(),
                    Arc::clone(&recovered_entries),
                    Some(Arc::clone(&batcher_stats)),
                    Some(backpressure_state.clone()),
                    incoming_rx,
                    shutdown_rx,
                ));

                info!(
                    node_id = node_id.get(),
                    listen_addr = %listen_addr,
                    peer_count = peers.len(),
                    "Started multi-node Helix service"
                );

                (
                    Some(batcher_handle),
                    Some(batcher_stats),
                    None,  // No actor router in lock-based mode
                    None,  // No actor shutdown in lock-based mode
                    None,  // No controller shutdown in lock-based mode
                    Some(backpressure_state),
                )
            };

        // Build Kafka peer addresses map (includes self).
        let mut peer_addrs = kafka_peer_addrs;
        peer_addrs.insert(node_id, kafka_addr);

        Ok(Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs,
            _shutdown_tx: shutdown_tx,
            data_dir,
            object_storage_dir,
            #[cfg(feature = "s3")]
            s3_config,
            tiering_config,
            transport_handle: Some(transport_handle),
            progress_manager,
            controller_state,
            pending_proposals,
            pending_controller_proposals,
            local_broker_heartbeats,
            shared_wal_pool,
            recovered_entries,
            batch_pending_proposals,
            batcher_handle,
            batcher_stats,
            actor_mode: enable_actor_mode,
            actor_router,
            actor_shutdown_tx,
            controller_shutdown_tx,
            actor_backpressure,
        })
    }

    /// Sets the S3 configuration for tiered storage.
    ///
    /// This should be called after creating the service if S3 tiering is desired.
    #[cfg(feature = "s3")]
    pub fn set_s3_config(&mut self, config: helix_tier::S3Config) {
        self.s3_config = Some(config);
    }

    /// Shuts down the service gracefully.
    ///
    /// Flushes and closes the `SharedWalPool` if present.
    ///
    /// # Errors
    /// Returns an error if the pool shutdown fails.
    pub async fn shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(pool) = self.shared_wal_pool {
            info!("Shutting down SharedWalPool");
            pool.shutdown().await?;
        }
        Ok(())
    }

    /// Returns the cluster nodes.
    #[must_use]
    pub fn cluster_nodes(&self) -> &[NodeId] {
        &self.cluster_nodes
    }

    /// Returns live brokers (those with recent heartbeats).
    ///
    /// In multi-node mode, filters out brokers that have missed heartbeats.
    /// In single-node mode, returns all cluster nodes (no heartbeat filtering).
    ///
    /// # Kafka `KRaft` Pattern
    ///
    /// Unlike other controller state, heartbeats are **soft state** (not Raft-replicated).
    /// Each broker sends heartbeats via transport to all peers, and each node maintains
    /// its own local view of broker liveness based on received heartbeats.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn live_brokers(&self) -> Vec<NodeId> {
        // In single-node mode, all brokers are "live".
        if !self.is_multi_node() {
            return self.cluster_nodes.clone();
        }

        // Get current time in milliseconds.
        // Safe truncation: milliseconds won't overflow u64 for ~584 million years.
        #[allow(clippy::cast_possible_truncation)]
        let current_time_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64);

        // Use local heartbeat soft state (not Raft-replicated controller state).
        let heartbeats = self.local_broker_heartbeats.read().await;

        // If no heartbeats have been recorded yet, assume all brokers are live.
        // This handles initial startup before heartbeats are established.
        if heartbeats.is_empty() {
            return self.cluster_nodes.clone();
        }

        self.cluster_nodes
            .iter()
            .filter(|&node_id| {
                heartbeats
                    .get(node_id)
                    .is_some_and(|&last_heartbeat| {
                        current_time_ms.saturating_sub(last_heartbeat) < BROKER_HEARTBEAT_TIMEOUT_MS
                    })
            })
            .copied()
            .collect()
    }

    /// Returns the cluster ID.
    #[must_use] 
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Returns whether this is a multi-node cluster.
    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        self.cluster_nodes.len() > 1
    }

    /// Returns the Kafka address for a node.
    pub fn get_node_address(&self, node_id: NodeId) -> Option<&str> {
        self.peer_addrs.get(&node_id).map(String::as_str)
    }

    /// Sends Raft messages via the transport handle.
    ///
    /// This is used to immediately send messages after proposing entries,
    /// rather than waiting for the next tick. This significantly reduces
    /// latency for acks=all workloads.
    ///
    /// Messages are sent in parallel to all destination nodes.
    pub(crate) async fn send_raft_messages(
        &self,
        outputs: &[helix_raft::multi::MultiRaftOutput],
    ) {
        let Some(ref transport) = self.transport_handle else {
            return;
        };

        for output in outputs {
            if let helix_raft::multi::MultiRaftOutput::SendMessages { to, messages } = output {
                if let Err(e) = transport.send_batch(*to, messages.clone()).await {
                    tracing::debug!(
                        to = to.get(),
                        count = messages.len(),
                        error = %e,
                        "Failed to send Raft messages (will retry on tick)"
                    );
                }
            }
        }
    }

    /// Waits for the controller partition to have a leader.
    ///
    /// This should be called before attempting controller operations like
    /// topic creation in multi-node mode. The method polls until the
    /// controller Raft group has an elected leader or the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns an error if the timeout expires before a leader is elected.
    pub async fn wait_for_controller_ready(
        &self,
        timeout: std::time::Duration,
    ) -> crate::error::ServerResult<()> {
        use crate::error::ServerError;

        if !self.is_multi_node() {
            return Ok(());
        }

        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = std::time::Duration::from_millis(100);

        loop {
            // Check if controller group has a leader.
            {
                let mr = self.multi_raft.read().await;
                if let Some(state) = mr.group_state(CONTROLLER_GROUP_ID) {
                    if state.leader_id.is_some() {
                        info!("Controller ready, leader elected");
                        return Ok(());
                    }
                }
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(ServerError::Internal {
                    message: "timeout waiting for controller leader election".to_string(),
                });
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Starts a periodic batcher stats reporter that writes JSON to a file.
    pub fn start_bench_reporter(&self, report_path: PathBuf, interval_ms: u64) {
        let Some(stats) = self.batcher_stats.clone() else {
            return;
        };

        let node_id = self.node_id.get();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(interval_ms));
            loop {
                interval.tick().await;
                // Safety: duration since UNIX_EPOCH will never exceed u64::MAX milliseconds
                // (would require running for hundreds of millions of years).
                #[allow(clippy::cast_possible_truncation)]
                let timestamp_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_millis() as u64);
                let snapshot = stats.snapshot();
                let json = snapshot.to_json(node_id, timestamp_ms);
                if let Err(e) = tokio::fs::write(&report_path, json).await {
                    tracing::warn!(
                        path = %report_path.display(),
                        error = %e,
                        "Failed to write bench report"
                    );
                }
            }
        });
    }
}

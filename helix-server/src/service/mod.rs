//! Helix gRPC service implementation.
//!
//! This module implements the Helix gRPC service, handling Write, Read,
//! and Metadata requests from clients.
//!
//! The service is backed by Multi-Raft for consensus across multiple
//! partition groups, with configurable partition storage.

/// Actor-based service setup for lock-free multi-partition coordination.
pub mod actor_setup;
/// Request batching for improved throughput.
pub mod batcher;
/// Handler implementations for the Helix service.
pub mod handlers;
/// Output processor for actor-based multi-partition coordination.
pub mod output_processor;
/// Partition actor for lock-free multi-partition scalability.
pub mod partition_actor;
/// Partition router for lock-free request dispatch.
pub mod router;
/// Tick task for Raft consensus.
pub mod tick;
/// WAL actor for command-channel based WAL access.
pub mod wal_actor;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use helix_core::{
    GroupId, LogIndex, NodeId, Offset, TermId, TopicId, WriteDurability,
};
use helix_progress::{ProgressConfig, ProgressManager, SimulatedProgressStore};
use helix_raft::multi::MultiRaft;
use helix_runtime::{PeerInfo, TransportConfig, TransportError, TransportHandle, TransportService};
use helix_tier::SimulatedObjectStorage;
use helix_wal::{PoolConfig, SharedEntry, SharedWalPool, Storage, TokioStorage};
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, info, warn};

use crate::controller::{ControllerState, BROKER_HEARTBEAT_TIMEOUT_MS, CONTROLLER_GROUP_ID};
use crate::group_map::GroupMap;
use crate::partition_storage::PartitionStorage;
use crate::vote_store::{LocalFileVoteStorage, VoteState, VoteStore};

use self::router::PartitionRouter;

// =============================================================================
// Type Aliases for Complex Nested Types
// =============================================================================

/// Shared map of pending proposals, keyed by group ID, then by log index.
/// Uses per-partition locks to enable parallel processing of different partitions.
#[allow(clippy::type_complexity)]
pub type PendingProposalMap =
    Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>;

/// Shared map of batch pending proposals, keyed by group ID, then by log index.
/// Uses per-partition locks to enable parallel processing of different partitions.
#[allow(clippy::type_complexity)]
pub type BatchPendingProposalMap =
    Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>;

/// Generic shared map of partition storage, keyed by group ID.
/// Uses per-partition locks to enable parallel access to different partitions.
///
/// # Type Parameters
///
/// * `S` - Storage backend (e.g., `TokioStorage` for production, `SimulatedStorage` for DST)
#[allow(clippy::type_complexity)]
pub type GenericPartitionStorageMap<S> =
    Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>;

/// Shared map of partition storage using production `TokioStorage`.
#[allow(clippy::type_complexity)]
pub type PartitionStorageMap = GenericPartitionStorageMap<TokioStorage>;

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

    // --- Batcher loop instrumentation ---

    /// Number of select! loop iterations.
    pub loop_iterations: std::sync::atomic::AtomicU64,
    /// Microseconds spent waiting in recv/select (idle time).
    pub loop_idle_us: std::sync::atomic::AtomicU64,
    /// Microseconds spent in `handle_submit_actor` (recv arm).
    pub loop_submit_us: std::sync::atomic::AtomicU64,
    /// Number of submit messages received.
    pub loop_submit_count: std::sync::atomic::AtomicU64,
    /// Microseconds spent in linger flush arm (all flushes).
    pub loop_linger_flush_us: std::sync::atomic::AtomicU64,
    /// Number of linger flush cycles (each may flush N groups).
    pub loop_linger_flush_cycles: std::sync::atomic::AtomicU64,
    /// Total groups flushed via linger.
    pub loop_linger_flush_groups: std::sync::atomic::AtomicU64,
    /// Microseconds in flush: `router.partition()` lookup.
    pub flush_router_us: std::sync::atomic::AtomicU64,
    /// Microseconds in flush: `partition_storage` read locks.
    pub flush_storage_lock_us: std::sync::atomic::AtomicU64,
    /// Microseconds in flush: `encode_split`.
    pub flush_encode_us: std::sync::atomic::AtomicU64,
    /// Microseconds in flush: `propose_batch` channel send.
    pub flush_propose_us: std::sync::atomic::AtomicU64,
    /// Number of inline flushes (triggered by size limit in submit).
    pub inline_flush_count: std::sync::atomic::AtomicU64,
    /// Microseconds spent in inline flushes.
    pub inline_flush_us: std::sync::atomic::AtomicU64,
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
        self.flush_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        use std::sync::atomic::Ordering::Relaxed;
        BatcherStatsSnapshot {
            flush_count: self.flush_count.load(Relaxed),
            flush_linger_count: self.flush_linger_count.load(Relaxed),
            flush_size_count: self.flush_size_count.load(Relaxed),
            flush_shutdown_count: self.flush_shutdown_count.load(Relaxed),
            total_batch_requests: self.total_batch_requests.load(Relaxed),
            total_batch_bytes: self.total_batch_bytes.load(Relaxed),
            total_batch_records: self.total_batch_records.load(Relaxed),
            total_batch_age_us: self.total_batch_age_us.load(Relaxed),
            commit_count: self.commit_count.load(Relaxed),
            total_commit_latency_us: self.total_commit_latency_us.load(Relaxed),
            total_batch_wait_us: self.total_batch_wait_us.load(Relaxed),
            total_total_age_us: self.total_total_age_us.load(Relaxed),
            not_leader_count: self.not_leader_count.load(Relaxed),
            apply_error_count: self.apply_error_count.load(Relaxed),
            loop_iterations: self.loop_iterations.load(Relaxed),
            loop_idle_us: self.loop_idle_us.load(Relaxed),
            loop_submit_us: self.loop_submit_us.load(Relaxed),
            loop_submit_count: self.loop_submit_count.load(Relaxed),
            loop_linger_flush_us: self.loop_linger_flush_us.load(Relaxed),
            loop_linger_flush_cycles: self
                .loop_linger_flush_cycles.load(Relaxed),
            loop_linger_flush_groups: self
                .loop_linger_flush_groups.load(Relaxed),
            flush_router_us: self.flush_router_us.load(Relaxed),
            flush_storage_lock_us: self.flush_storage_lock_us.load(Relaxed),
            flush_encode_us: self.flush_encode_us.load(Relaxed),
            flush_propose_us: self.flush_propose_us.load(Relaxed),
            inline_flush_count: self.inline_flush_count.load(Relaxed),
            inline_flush_us: self.inline_flush_us.load(Relaxed),
        }
    }
}

/// Snapshot of batcher stats for reporting.
#[allow(missing_docs)]
pub struct BatcherStatsSnapshot {
    pub flush_count: u64,
    pub flush_linger_count: u64,
    pub flush_size_count: u64,
    pub flush_shutdown_count: u64,
    pub total_batch_requests: u64,
    pub total_batch_bytes: u64,
    pub total_batch_records: u64,
    pub total_batch_age_us: u64,
    pub commit_count: u64,
    pub total_commit_latency_us: u64,
    pub total_batch_wait_us: u64,
    pub total_total_age_us: u64,
    pub not_leader_count: u64,
    pub apply_error_count: u64,
    // Batcher loop instrumentation.
    pub loop_iterations: u64,
    pub loop_idle_us: u64,
    pub loop_submit_us: u64,
    pub loop_submit_count: u64,
    pub loop_linger_flush_us: u64,
    pub loop_linger_flush_cycles: u64,
    pub loop_linger_flush_groups: u64,
    pub flush_router_us: u64,
    pub flush_storage_lock_us: u64,
    pub flush_encode_us: u64,
    pub flush_propose_us: u64,
    pub inline_flush_count: u64,
    pub inline_flush_us: u64,
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

/// Output processor performance stats for bottleneck analysis.
///
/// Tracks per-message-type processing times and idle time to determine
/// whether the output processor is the throughput bottleneck.
#[derive(Default)]
pub struct OutputProcessorStats {
    /// Number of `SendMessages` processed.
    pub send_messages_count: std::sync::atomic::AtomicU64,
    /// Cumulative microseconds spent on `SendMessages` (encode + transport send).
    pub send_messages_us: std::sync::atomic::AtomicU64,
    /// Number of `EntryCommitted` processed.
    pub entry_committed_count: std::sync::atomic::AtomicU64,
    /// Cumulative microseconds spent on `EntryCommitted` (decode + apply + notify).
    pub entry_committed_us: std::sync::atomic::AtomicU64,
    /// Number of other outputs processed (`BecameLeader`, `SteppedDown`, etc.).
    pub other_count: std::sync::atomic::AtomicU64,
    /// Cumulative microseconds spent on other outputs.
    pub other_us: std::sync::atomic::AtomicU64,
    /// Cumulative microseconds spent idle (waiting in `recv_many()`).
    pub recv_idle_us: std::sync::atomic::AtomicU64,
    /// Number of `recv_many` batches processed.
    pub batch_count: std::sync::atomic::AtomicU64,
    /// Total messages processed across all batches.
    pub total_messages: std::sync::atomic::AtomicU64,
}

impl OutputProcessorStats {
    /// Takes a snapshot for reporting.
    pub fn snapshot(&self) -> OutputProcessorStatsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        OutputProcessorStatsSnapshot {
            send_messages_count: self.send_messages_count.load(Relaxed),
            send_messages_us: self.send_messages_us.load(Relaxed),
            entry_committed_count: self.entry_committed_count.load(Relaxed),
            entry_committed_us: self.entry_committed_us.load(Relaxed),
            other_count: self.other_count.load(Relaxed),
            other_us: self.other_us.load(Relaxed),
            recv_idle_us: self.recv_idle_us.load(Relaxed),
            batch_count: self.batch_count.load(Relaxed),
            total_messages: self.total_messages.load(Relaxed),
        }
    }
}

/// Snapshot of output processor stats for reporting.
#[allow(missing_docs)]
pub struct OutputProcessorStatsSnapshot {
    pub send_messages_count: u64,
    pub send_messages_us: u64,
    pub entry_committed_count: u64,
    pub entry_committed_us: u64,
    pub other_count: u64,
    pub other_us: u64,
    pub recv_idle_us: u64,
    pub batch_count: u64,
    pub total_messages: u64,
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
///
/// # Type Parameters
///
/// * `S` - Storage backend (e.g., `TokioStorage` for production, `SimulatedStorage` for DST)
/// * `T` - Transport backend (e.g., `TransportHandle` for production, `MadSimTransport` for DST)
pub struct HelixService<
    S: Storage + Clone + Send + Sync + 'static,
    T: TransportService = TransportHandle,
> {
    /// Cluster ID.
    pub(crate) cluster_id: String,
    /// This node's ID.
    pub(crate) node_id: NodeId,
    /// Multi-Raft engine for consensus.
    pub(crate) multi_raft: Arc<RwLock<MultiRaft>>,
    /// Partition storage indexed by `GroupId`.
    /// Uses per-partition locks to enable parallel processing of different partitions.
    pub(crate) partition_storage: GenericPartitionStorageMap<S>,
    /// Storage backend instance for creating new partitions.
    pub(crate) storage: S,
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
    pub(crate) transport_handle: Option<T>,
    /// Progress manager for consumer group tracking.
    pub(crate) progress_manager: Arc<ProgressManager<SimulatedProgressStore>>,
    /// Controller state machine (cluster metadata).
    pub(crate) controller_state: Arc<RwLock<ControllerState>>,
    /// Pending proposals waiting for Raft commit (multi-node mode only).
    /// Indexed by (`GroupId`, `LogIndex`) for O(1) lookup on commit.
    /// Uses per-partition locks to enable parallel processing of different partitions.
    pub(crate) pending_proposals: PendingProposalMap,
    /// Pending controller proposals waiting for Raft commit.
    pub(crate) pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    /// Local broker heartbeat timestamps (soft state, not Raft-replicated).
    ///
    /// Following Kafka `KRaft` pattern, heartbeats are maintained as soft state
    /// on each node. Each broker sends heartbeats via transport to all peers,
    /// and each node maintains its own view of broker liveness.
    pub(crate) local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    /// Shared WAL pool for fsync amortization. Present when `data_dir` is set.
    pub(crate) shared_wal_pool: Option<Arc<SharedWalPool<S>>>,
    /// Recovered entries from shared WAL, indexed by `GroupId`.
    /// Used during partition creation to restore state (Phase 3).
    #[allow(dead_code)] // Used in Phase 3 of SharedWAL integration.
    pub(crate) recovered_entries: Arc<RwLock<HashMap<GroupId, Vec<SharedEntry>>>>,
    /// Pending batched proposals waiting for Raft commit (multi-node mode only).
    /// Indexed by (`GroupId`, `LogIndex`) for O(1) lookup on commit.
    /// Note: This field is shared via Arc with the tick task, not read directly here.
    /// Uses per-partition locks to enable parallel processing of different partitions.
    #[allow(dead_code)]
    pub(crate) batch_pending_proposals: BatchPendingProposalMap,
    /// Handle to submit requests to the batcher (multi-node mode only).
    pub(crate) batcher_handle: Option<batcher::BatcherHandle>,
    /// Aggregated batcher performance stats (multi-node mode only).
    pub(crate) batcher_stats: Option<Arc<BatcherStats>>,
    /// Partition router for actor-based lock-free request dispatch (multi-node only).
    pub(crate) actor_router: Option<Arc<PartitionRouter>>,
    /// Output channel for partition actors.
    /// Used to process propose outputs for single-node clusters.
    pub(crate) actor_output_tx: Option<mpsc::Sender<partition_actor::GroupedOutput>>,
    /// Shutdown sender for actor tick task (data partitions).
    /// Kept alive to prevent premature task shutdown via channel close.
    #[allow(dead_code)]
    pub(crate) actor_shutdown_tx: Option<mpsc::Sender<()>>,
    /// Shutdown sender for controller tick task.
    /// Kept alive to prevent premature task shutdown via channel close.
    #[allow(dead_code)]
    pub(crate) controller_shutdown_tx: Option<mpsc::Sender<()>>,
    /// Backpressure state for actor-based flow control.
    #[allow(dead_code)]
    pub(crate) actor_backpressure: Option<Arc<batcher::BackpressureState>>,
    /// Vote store for persisting Raft vote state (multi-node only).
    /// Uses Arc<Mutex> for thread-safe access from tick tasks.
    #[allow(dead_code)] // Used by tick tasks for vote persistence.
    pub(crate) vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    /// Local disk retention in milliseconds.
    /// `None` means retention is disabled (segments kept forever).
    pub(crate) local_retention_ms: Option<u64>,
}

/// Type alias for production Helix service using `TokioStorage` and `TransportHandle`.
///
/// This is the default type used in production and most tests. For DST
/// (Deterministic Simulation Testing), use `HelixService<SimulatedStorage, MadSimTransport>`.
pub type ProductionHelixService = HelixService<TokioStorage, TransportHandle>;

impl<S: Storage + Clone + Send + Sync + 'static> HelixService<S> {
    /// Creates a new Helix service with in-memory storage (for testing/DST).
    ///
    /// **Note**: This constructor does NOT start a background tick task.
    /// For DST, the test harness handles ticking manually.
    /// For production use, use `ProductionHelixService::new()` instead.
    ///
    /// # Arguments
    /// * `cluster_id` - Unique cluster identifier
    /// * `node_id` - This node's ID
    /// * `storage` - Storage backend for WAL operations
    pub async fn with_storage(cluster_id: String, node_id: u64, storage: S) -> Self {
        // In-memory mode doesn't use WAL, but pass Fsync for safety if WAL is added later.
        let (service, _shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            None,
            None,
            None,
            WriteDurability::Fsync,
            storage,
        )
        .await;
        service
    }

    /// Creates a new Helix service with durable WAL-backed storage (for testing/DST).
    ///
    /// **Note**: This constructor does NOT start a background tick task.
    /// For DST, the test harness handles ticking manually.
    /// For production use, use `ProductionHelixService::with_data_dir()` instead.
    ///
    /// # Arguments
    /// * `cluster_id` - Unique cluster identifier
    /// * `node_id` - This node's ID
    /// * `data_dir` - Directory for durable storage
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `storage` - Storage backend for WAL operations
    pub async fn with_data_dir_and_storage(
        cluster_id: String,
        node_id: u64,
        data_dir: PathBuf,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        storage: S,
    ) -> Self {
        let (service, _shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            Some(data_dir),
            None,
            shared_wal_count,
            write_durability,
            storage,
        )
        .await;
        service
    }

    /// Creates a new Helix service with durable storage and object storage for tiering (for testing/DST).
    ///
    /// **Note**: This constructor does NOT start a background tick task.
    /// For DST, the test harness handles ticking manually.
    /// For production use, use `ProductionHelixService::with_data_and_object_storage()` instead.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `storage` - Storage backend for WAL operations
    pub async fn with_data_and_object_storage_and_storage(
        cluster_id: String,
        node_id: u64,
        data_dir: PathBuf,
        object_storage_dir: PathBuf,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        storage: S,
    ) -> Self {
        let (service, _shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            Some(data_dir),
            Some(object_storage_dir),
            shared_wal_count,
            write_durability,
            storage,
        )
        .await;
        service
    }

    /// Internal constructor that returns the service and shutdown receiver.
    ///
    /// The caller is responsible for spawning the tick task if needed.
    /// Production constructors spawn the tick task; DST constructors don't.
    async fn new_internal(
        cluster_id: String,
        node_id: u64,
        data_dir: Option<PathBuf>,
        object_storage_dir: Option<PathBuf>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        storage: S,
    ) -> (Self, mpsc::Receiver<()>) {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id]; // Single node for now.

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage: GenericPartitionStorageMap<S> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let pending_proposals: PendingProposalMap = Arc::new(RwLock::new(HashMap::new()));

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

            // Open pool with the provided storage backend.
            let pool = SharedWalPool::open(storage.clone(), pool_config)
                .await
                .expect("Failed to open SharedWalPool");

            // Recover all partitions.
            let recovered = pool
                .recover()
                .await
                .expect("Failed to recover from SharedWalPool");

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

        // Note: Tick task is NOT spawned here. Caller must decide whether to
        // spawn the tick task based on the use case:
        // - Production: spawn tick task using the returned shutdown_rx
        // - DST: don't spawn, test harness handles ticking manually

        let service = Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            storage,
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
            actor_router: None,
            actor_output_tx: None,
            actor_shutdown_tx: None,
            controller_shutdown_tx: None,
            actor_backpressure: None,
            vote_store: None, // Single-node mode doesn't persist vote state.
            local_retention_ms: None,
        };

        (service, shutdown_rx)
    }

    /// Creates a new Helix service with multi-node networking and custom storage.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `storage` - Storage backend for WAL operations
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
    pub async fn new_multi_node_with_storage(
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
        storage: S,
        local_retention_ms: Option<u64>,
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
            storage,
            local_retention_ms,
        )
        .await
    }

    /// Creates a new Helix service with multi-node networking and custom storage.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `storage` - Storage backend for WAL operations
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    ///
    /// # Panics
    /// Panics if `shared_wal_count` is not in range [1, 16], or if the `SharedWalPool`
    /// fails to open or recover (indicates filesystem or corruption issues).
    #[allow(clippy::too_many_arguments)]
    #[cfg(not(feature = "s3"))]
    pub async fn new_multi_node_with_storage(
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
        storage: S,
        local_retention_ms: Option<u64>,
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
            storage,
            local_retention_ms,
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
        storage: S,
        local_retention_ms: Option<u64>,
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
        let partition_storage: GenericPartitionStorageMap<S> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let controller_state = Arc::new(RwLock::new(ControllerState::new()));
        let pending_proposals: PendingProposalMap = Arc::new(RwLock::new(HashMap::new()));
        let pending_controller_proposals = Arc::new(RwLock::new(Vec::new()));
        let local_broker_heartbeats = Arc::new(RwLock::new(HashMap::new()));

        // Note: We do NOT set default_peers for auto-creating groups.
        // Data partition groups must only be created via the controller's
        // AssignPartition command, which ensures partition_storage is also
        // created. Auto-creating Raft groups on message receipt would cause
        // commits to be dropped because partition_storage doesn't exist yet.

        // Initialize VoteStore for Raft vote state persistence.
        // Uses local file for fast recovery + S3 backup for disk loss scenarios.
        // Must be done before creating Raft groups so we can restore vote state.
        let (vote_store, initial_vote_state, recovered_from_remote) =
            if let Some(ref dir) = data_dir {
                let vote_file_path = dir.join("vote-state.bin");
                let local_storage = Arc::new(LocalFileVoteStorage::new(vote_file_path));

                // Use simulated object storage for S3 backup (can be upgraded to real S3 later).
                let remote_storage = Arc::new(SimulatedObjectStorage::new(node_id.get()));

                // Load existing vote state or start fresh.
                let load_result = VoteStore::<LocalFileVoteStorage>::load(
                    node_id,
                    local_storage.as_ref(),
                    remote_storage.as_ref(),
                )
                .await;

                let (initial_state, recovered_from_remote) = match load_result {
                    Ok(result) => {
                        info!(
                            node_id = node_id.get(),
                            sequence = result.state.sequence,
                            groups = result.state.group_count(),
                            recovered_from_remote = result.recovered_from_remote,
                            "Loaded vote state"
                        );
                        (result.state, result.recovered_from_remote)
                    }
                    Err(e) => {
                        warn!(
                            node_id = node_id.get(),
                            error = %e,
                            "Failed to load vote state, starting fresh"
                        );
                        (VoteState::new(), false)
                    }
                };

                // Create VoteStore and spawn background S3 worker.
                let (store, handle) = VoteStore::new(
                    node_id,
                    local_storage,
                    remote_storage,
                    initial_state.clone(),
                );

                // Spawn background S3 upload worker.
                tokio::spawn(handle.run());

                (
                    Some(Arc::new(Mutex::new(store))),
                    initial_state,
                    recovered_from_remote,
                )
            } else {
                // In-memory mode - no vote persistence.
                (None, VoteState::new(), false)
            };

        // Create controller partition (group 0) with all cluster nodes.
        // Use persisted vote state if available to restore term/voted_for.
        let controller_vote = initial_vote_state.get_group(CONTROLLER_GROUP_ID);
        let (term, voted_for) =
            controller_vote.map_or((TermId::new(0), None), |v| (v.term, v.voted_for));

        let create_result = multi_raft.write().await.create_group_with_state(
            CONTROLLER_GROUP_ID,
            cluster_nodes.clone(),
            term,
            voted_for,
            recovered_from_remote, // observation mode if recovered from S3
        );

        match create_result {
            Ok(()) => {
                info!(
                    group_id = CONTROLLER_GROUP_ID.get(),
                    term = term.get(),
                    voted_for = voted_for.map(NodeId::get),
                    observation_mode = recovered_from_remote,
                    nodes = ?cluster_nodes.iter().map(|n| n.get()).collect::<Vec<_>>(),
                    "Created controller partition with restored vote state"
                );
            }
            Err(e) => {
                error!(error = %e, "Failed to create controller partition");
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

            // Open pool with the provided storage backend.
            let pool = SharedWalPool::open(storage.clone(), pool_config)
                .await
                .expect("Failed to open SharedWalPool");

            // Recover all partitions.
            let recovered = pool
                .recover()
                .await
                .expect("Failed to recover from SharedWalPool");

            info!(
                partitions = recovered.len(),
                "SharedWalPool recovery complete for multi-node"
            );

            (Some(Arc::new(pool)), Arc::new(RwLock::new(recovered)))
        } else {
            // In-memory mode.
            (None, Arc::new(RwLock::new(HashMap::new())))
        };

        // Replay controller entries from SharedWAL into ControllerState.
        // This restores topics and partition assignments after a pod restart.
        #[allow(clippy::significant_drop_tightening)]
        {
            let mut recovered = recovered_entries.write().await;
            if let Some(controller_entries) = recovered.remove(&CONTROLLER_GROUP_ID) {
                let mut state = controller_state.write().await;
                let mut count = 0u64;
                let mut max_raft_index = 0u64;
                for entry in &controller_entries {
                    if entry.header.raft_index > max_raft_index {
                        max_raft_index = entry.header.raft_index;
                    }
                    if let Some(cmd) =
                        crate::controller::ControllerCommand::decode(&entry.payload)
                    {
                        // Discard follow-ups — they were already committed
                        // and persisted as separate WAL entries.
                        let _ = state.apply(&cmd, &cluster_nodes);
                        count += 1;
                    } else {
                        warn!(
                            raft_index = entry.header.raft_index,
                            "Failed to decode controller entry during recovery"
                        );
                    }
                }
                drop(state);
                // Tell tick tasks to skip WAL writes for entries at or
                // below this index — they're already persisted.
                tick::CONTROLLER_LAST_PERSISTED_INDEX
                    .store(max_raft_index, std::sync::atomic::Ordering::Relaxed);
                if count > 0 {
                    info!(
                        replayed = count,
                        max_raft_index,
                        "Replayed controller state from SharedWAL"
                    );
                }
            }
        }

        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        // Create batch pending proposals map.
        let batch_pending_proposals: BatchPendingProposalMap =
            Arc::new(RwLock::new(HashMap::new()));

        // Actor-based architecture: lock-free per-partition actors with message-passing.
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
            vote_store.clone(),
            shared_wal_pool.clone(),
            data_dir.clone(),
            Arc::clone(&recovered_entries),
            storage.clone(),
            local_retention_ms,
        )
        .await;

        // Spawn controller tick task (handles controller partition via MultiRaft).
        let controller_shutdown_tx = {
            let (tx, rx) = mpsc::channel(1);
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
                vote_store.clone(),
                shared_wal_pool.clone(),
                data_dir.clone(),
                Arc::clone(&recovered_entries),
                storage.clone(),
                Arc::clone(&local_broker_heartbeats),
                rx,
            ));
            tx
        };

        info!(
            node_id = node_id.get(),
            listen_addr = %listen_addr,
            peer_count = peers.len(),
            "Started multi-node Helix service"
        );

        // Build Kafka peer addresses map (includes self).
        let mut peer_addrs = kafka_peer_addrs;
        peer_addrs.insert(node_id, kafka_addr);

        Ok(Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            storage,
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
            batcher_handle: Some(actor_handles.batcher_handle),
            batcher_stats: Some(actor_handles.batcher_stats),
            actor_router: Some(actor_handles.router),
            actor_output_tx: Some(actor_handles.output_tx.clone()),
            actor_shutdown_tx: Some(actor_handles.shutdown_tx),
            controller_shutdown_tx: Some(controller_shutdown_tx),
            actor_backpressure: Some(actor_handles.backpressure),
            vote_store,
            local_retention_ms,
        })
    }

    /// Sets the S3 configuration for tiered storage.
    ///
    /// This should be called after creating the service if S3 tiering is desired.
    #[cfg(feature = "s3")]
    pub fn set_s3_config(&mut self, config: helix_tier::S3Config) {
        self.s3_config = Some(config);
    }

    /// Sets the local disk retention in milliseconds.
    ///
    /// When set, sealed WAL segments older than this value are deleted
    /// from local disk, provided all entries have been replicated.
    /// Set to 0 to disable retention.
    pub const fn set_local_retention_ms(&mut self, ms: u64) {
        self.local_retention_ms = if ms == 0 { None } else { Some(ms) };
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
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(interval_ms));
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
                    warn!(
                        path = %report_path.display(),
                        error = %e,
                        "Failed to write bench report"
                    );
                }
            }
        });
    }
}

// =============================================================================
// DST Helper Methods (Generic over Transport)
// =============================================================================

/// DST methods for `HelixService<S, T>` with any transport type.
///
/// These methods enable Deterministic Simulation Testing (DST) by allowing
/// the test harness to drive the service manually:
/// - Inject transport and configure cluster nodes
/// - Drive ticks and process incoming messages
/// - Access internal state for property checking
impl<S: Storage + Clone + Send + Sync + 'static, T: TransportService> HelixService<S, T> {
    /// Creates a new Helix service for DST with explicit transport type.
    ///
    /// This constructor allows creating a service with any transport type T,
    /// not just the default `TransportHandle`. The service is created without
    /// a background tick task - the test harness drives ticking manually.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let service: HelixService<SimulatedStorage, MadSimTransport> =
    ///     HelixService::new_for_test(cluster_id, node_id, storage);
    /// ```
    #[allow(clippy::unused_async)]
    pub async fn new_for_test(cluster_id: String, node_id: u64, storage: S) -> Self {
        let node_id = NodeId::new(node_id);
        let cluster_nodes = vec![node_id];

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage: GenericPartitionStorageMap<S> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let pending_proposals: PendingProposalMap = Arc::new(RwLock::new(HashMap::new()));

        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs: HashMap::new(),
            _shutdown_tx: shutdown_tx,
            data_dir: None,
            object_storage_dir: None,
            #[cfg(feature = "s3")]
            s3_config: None,
            tiering_config: None,
            transport_handle: None,
            progress_manager,
            controller_state: Arc::new(RwLock::new(ControllerState::new())),
            pending_proposals,
            pending_controller_proposals: Arc::new(RwLock::new(Vec::new())),
            local_broker_heartbeats: Arc::new(RwLock::new(HashMap::new())),
            shared_wal_pool: None,
            recovered_entries: Arc::new(RwLock::new(HashMap::new())),
            batch_pending_proposals: Arc::new(RwLock::new(HashMap::new())),
            batcher_handle: None,
            batcher_stats: None,
            actor_router: None,
            actor_output_tx: None,
            actor_shutdown_tx: None,
            controller_shutdown_tx: None,
            actor_backpressure: None,
            vote_store: None,
            local_retention_ms: None,
        }
    }

    /// Returns the shared WAL pool (if durable storage is enabled).
    #[must_use]
    pub fn shared_wal_pool(&self) -> Option<Arc<SharedWalPool<S>>> {
        self.shared_wal_pool.clone()
    }

    /// Returns the data directory (if durable storage is enabled).
    #[must_use]
    pub const fn data_dir(&self) -> Option<&PathBuf> {
        self.data_dir.as_ref()
    }

    /// Sets the transport for DST.
    ///
    /// This allows injecting a simulated transport (e.g., `MadSimTransport`)
    /// after creating the service.
    pub fn set_transport(&mut self, transport: T) {
        self.transport_handle = Some(transport);
    }

    /// Sets the cluster nodes for multi-node DST.
    ///
    /// This configures the service for multi-node operation without starting
    /// a background tick task. The test harness drives ticking manually.
    pub fn set_cluster_nodes(&mut self, nodes: Vec<NodeId>) {
        self.cluster_nodes = nodes;
    }

    /// Sets the batcher handle for multi-node DST.
    ///
    /// This enables the batcher for concurrent produce requests, which
    /// serializes offset assignment to prevent race conditions.
    pub fn set_batcher_handle(&mut self, handle: batcher::BatcherHandle) {
        self.batcher_handle = Some(handle);
    }

    /// Sets the actor router for actor mode DST.
    ///
    /// This enables actor mode request routing for multi-node DST, allowing
    /// the service to query partition actors for leadership and routing.
    pub fn set_actor_router(&mut self, router: Arc<router::PartitionRouter>) {
        self.actor_router = Some(router);
    }

    /// Returns a reference to the actor router, if set.
    ///
    /// This is used for debugging and testing to verify the router is properly configured.
    #[must_use]
    pub const fn actor_router(&self) -> Option<&Arc<router::PartitionRouter>> {
        self.actor_router.as_ref()
    }

    /// Ticks all Raft groups and returns outputs.
    ///
    /// This is the DST equivalent of the periodic tick in `tick_task_actor`.
    /// The test harness should call this periodically to advance Raft state.
    ///
    /// Returns a list of `MultiRaftOutput` that need processing.
    pub async fn tick(&self) -> Vec<helix_raft::multi::MultiRaftOutput> {
        let mut multi_raft = self.multi_raft.write().await;
        multi_raft.tick()
    }

    /// Processes incoming Raft messages from other nodes.
    ///
    /// This is the DST equivalent of handling `IncomingMessage::Batch` in
    /// `tick_task_actor`. The test harness calls this when messages
    /// arrive from the simulated transport.
    ///
    /// Returns a list of `MultiRaftOutput` that need processing.
    pub async fn process_incoming_raft_batch(
        &self,
        messages: Vec<helix_raft::multi::GroupMessage>,
    ) -> Vec<helix_raft::multi::MultiRaftOutput> {
        let mut multi_raft = self.multi_raft.write().await;
        let mut all_outputs = Vec::new();
        for group_msg in messages {
            let outputs = multi_raft.handle_message(group_msg.group_id, group_msg.message);
            all_outputs.extend(outputs);
        }
        // Flush any pending outbound messages immediately.
        all_outputs.extend(multi_raft.flush());
        all_outputs
    }

    /// Processes an incoming broker heartbeat.
    ///
    /// This is the DST equivalent of handling `IncomingMessage::Heartbeat` in
    /// `tick_task_actor`. Updates local heartbeat soft state.
    pub async fn process_broker_heartbeat(&self, heartbeat: helix_runtime::BrokerHeartbeat) {
        let mut heartbeats = self.local_broker_heartbeats.write().await;
        heartbeats.insert(heartbeat.node_id, heartbeat.timestamp_ms);
    }

    /// Returns access to the `MultiRaft` engine for advanced DST scenarios.
    ///
    /// This provides direct access to the consensus engine for property checking
    /// and state inspection during DST.
    #[must_use]
    pub const fn multi_raft(&self) -> &Arc<RwLock<MultiRaft>> {
        &self.multi_raft
    }

    /// Returns access to the partition storage map for DST.
    #[must_use]
    pub const fn partition_storage_map(&self) -> &GenericPartitionStorageMap<S> {
        &self.partition_storage
    }

    /// Returns access to the group map for DST.
    #[must_use]
    pub const fn group_map(&self) -> &Arc<RwLock<GroupMap>> {
        &self.group_map
    }

    /// Returns access to pending proposals for DST.
    #[must_use]
    pub const fn pending_proposals(&self) -> &PendingProposalMap {
        &self.pending_proposals
    }

    /// Returns access to pending controller proposals for DST.
    #[must_use]
    pub const fn pending_controller_proposals(
        &self,
    ) -> &Arc<RwLock<Vec<PendingControllerProposal>>> {
        &self.pending_controller_proposals
    }

    /// Returns access to batch pending proposals for DST.
    #[must_use]
    pub const fn batch_pending_proposals(&self) -> &BatchPendingProposalMap {
        &self.batch_pending_proposals
    }

    /// Returns access to local broker heartbeats for DST.
    #[must_use]
    pub const fn local_broker_heartbeats(&self) -> &Arc<RwLock<HashMap<NodeId, u64>>> {
        &self.local_broker_heartbeats
    }

    /// Returns access to controller state for DST.
    #[must_use]
    pub const fn controller_state(&self) -> &Arc<RwLock<ControllerState>> {
        &self.controller_state
    }

    /// Returns access to recovered entries for DST.
    #[must_use]
    pub const fn recovered_entries(&self) -> &Arc<RwLock<HashMap<GroupId, Vec<SharedEntry>>>> {
        &self.recovered_entries
    }

    /// Returns access to the storage backend.
    #[must_use]
    pub const fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns the cluster nodes (for generic transport types).
    #[must_use]
    pub fn cluster_nodes_generic(&self) -> &[NodeId] {
        &self.cluster_nodes
    }

    /// Returns topic metadata by name (for E2E testing).
    pub async fn get_topic_generic(&self, name: &str) -> Option<TopicMetadata> {
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    /// Returns whether this service uses the controller partition for topic
    /// and partition management. After the removal of non-actor mode, all
    /// nodes — including single-node clusters — use the controller path.
    #[must_use]
    pub fn is_multi_node(&self) -> bool {
        // All nodes now use the controller partition (Raft group 0) for topic
        // and partition management. The name is historical; a single-node
        // cluster still runs the controller Raft group with quorum of 1.
        !self.cluster_nodes.is_empty()
    }

    /// Alias for backwards compatibility.
    #[must_use]
    pub fn is_multi_node_generic(&self) -> bool {
        self.is_multi_node()
    }

    /// Returns the cluster ID.
    #[must_use]
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Returns the cluster nodes.
    #[must_use]
    pub fn cluster_nodes(&self) -> &[NodeId] {
        &self.cluster_nodes
    }

    /// Returns the Kafka address for a node.
    #[must_use]
    pub fn get_node_address(&self, node_id: NodeId) -> Option<&str> {
        self.peer_addrs.get(&node_id).map(String::as_str)
    }

    /// Returns live brokers based on heartbeat status.
    ///
    /// Returns only nodes with recent heartbeats. If no heartbeats have
    /// been recorded yet (initial startup), assumes all brokers are live.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn live_brokers(&self) -> Vec<NodeId> {
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
                heartbeats.get(node_id).is_some_and(|&last_heartbeat| {
                    current_time_ms.saturating_sub(last_heartbeat) < BROKER_HEARTBEAT_TIMEOUT_MS
                })
            })
            .copied()
            .collect()
    }

    /// Sends Raft messages via the transport handle (generic version).
    ///
    /// This is used to immediately send messages after proposing entries,
    /// rather than waiting for the next tick. This significantly reduces
    /// latency for acks=all workloads.
    ///
    /// Messages are sent in parallel to all destination nodes.
    pub(crate) async fn send_raft_messages(&self, outputs: &[helix_raft::multi::MultiRaftOutput]) {
        let Some(ref transport) = self.transport_handle else {
            return;
        };

        for output in outputs {
            if let helix_raft::multi::MultiRaftOutput::SendMessages { to, messages } = output {
                if let Err(e) = transport.send_batch(*to, messages.clone()).await {
                    debug!(
                        to = to.get(),
                        count = messages.len(),
                        error = %e,
                        "Failed to send Raft messages (will retry on tick)"
                    );
                }
            }
        }
    }

    /// Creates a new Helix service for multi-node DST with an injected transport.
    ///
    /// This constructor uses the full production wiring (actor mode or tick-based)
    /// but accepts an injected transport instead of creating a TCP transport.
    /// This enables Deterministic Simulation Testing with `MadSimTransport` while
    /// exercising the exact same code paths as production.
    ///
    /// # Arguments
    ///
    /// * `cluster_id` - Unique cluster identifier
    /// * `node_id` - This node's ID (as u64)
    /// * `cluster_nodes` - All node IDs in the cluster (including self)
    /// * `transport` - Injected transport implementing `TransportService`
    /// * `incoming_rx` - Receiver for incoming Raft messages from the transport
    /// * `data_dir` - Optional directory for durable storage (`SharedWalPool`)
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    /// * `storage` - Storage backend (e.g., `SimulatedStorage` for DST)
    ///
    /// # Example
    ///
    /// ```ignore
    /// let transport = MadSimTransport::new(node_id, network_state, mailboxes);
    /// let service: HelixService<SimulatedStorage, MadSimTransport> =
    ///     HelixService::new_multi_node_with_transport(
    ///         "cluster".to_string(),
    ///         1,
    ///         vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    ///         transport,
    ///         incoming_rx,
    ///         Some(data_dir),
    ///         None,
    ///         WriteDurability::Fsync,
    ///         storage,
    ///     ).await;
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `shared_wal_count` is outside `[1, 16]` or if shared WAL
    /// open/recovery fails while `data_dir` is enabled.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn new_multi_node_with_transport(
        cluster_id: String,
        node_id: u64,
        cluster_nodes: Vec<NodeId>,
        transport: T,
        incoming_rx: mpsc::Receiver<helix_runtime::IncomingMessage>,
        data_dir: Option<PathBuf>,
        shared_wal_count: Option<u32>,
        write_durability: WriteDurability,
        storage: S,
    ) -> Self {
        let node_id = NodeId::new(node_id);

        let multi_raft = Arc::new(RwLock::new(MultiRaft::new(node_id)));
        let partition_storage: GenericPartitionStorageMap<S> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let controller_state = Arc::new(RwLock::new(ControllerState::new()));
        let pending_proposals: PendingProposalMap = Arc::new(RwLock::new(HashMap::new()));
        let pending_controller_proposals = Arc::new(RwLock::new(Vec::new()));
        let local_broker_heartbeats = Arc::new(RwLock::new(HashMap::new()));

        // For DST, we skip vote persistence (simpler, deterministic).
        // Production multi-node uses VoteStore for crash recovery.
        let vote_store = None;

        // Create controller partition (group 0) with all cluster nodes.
        let create_result = multi_raft
            .write()
            .await
            .create_group(CONTROLLER_GROUP_ID, cluster_nodes.clone());

        match create_result {
            Ok(()) => {
                info!(
                    group_id = CONTROLLER_GROUP_ID.get(),
                    nodes = ?cluster_nodes.iter().map(|n| n.get()).collect::<Vec<_>>(),
                    "Created controller partition for DST"
                );
            }
            Err(e) => {
                error!(error = %e, "Failed to create controller partition");
            }
        }

        // Create progress manager.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        // Initialize SharedWalPool if data_dir and shared_wal_count are set.
        // When shared_wal_count is None, per-partition WAL mode is used instead.
        let (shared_wal_pool, recovered_entries) =
            if let (Some(ref dir), Some(wal_count)) = (&data_dir, shared_wal_count) {
                assert!(
                    (1..=16).contains(&wal_count),
                    "shared_wal_count must be in range [1, 16]"
                );

                info!(
                    wal_count,
                    data_dir = ?dir,
                    "Initializing SharedWalPool for DST multi-node"
                );

                let pool_config = PoolConfig::new(dir.join("shared-wal"), wal_count)
                    .with_flush_interval(std::time::Duration::from_millis(1))
                    .with_max_buffer_entries(1000)
                    .with_durability(write_durability);

                let pool = SharedWalPool::open(storage.clone(), pool_config)
                    .await
                    .expect("Failed to open SharedWalPool");

                let recovered = pool
                    .recover()
                    .await
                    .expect("Failed to recover from SharedWalPool");

                info!(
                    partitions = recovered.len(),
                    "SharedWalPool recovery complete for DST multi-node"
                );

                (Some(Arc::new(pool)), Arc::new(RwLock::new(recovered)))
            } else {
                if data_dir.is_some() {
                    info!("Using per-partition WAL mode (no SharedWalPool)");
                }
                (None, Arc::new(RwLock::new(HashMap::new())))
            };

        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);
        let batch_pending_proposals: BatchPendingProposalMap =
            Arc::new(RwLock::new(HashMap::new()));

        // Actor-based architecture: lock-free per-partition actors.
        info!("Initializing actor mode for DST multi-node service");

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
            transport.clone(),
            incoming_rx,
            actor_setup::ActorSetupConfig::default(),
            vote_store.clone(),
            shared_wal_pool.clone(),
            data_dir.clone(),
            Arc::clone(&recovered_entries),
            storage.clone(),
            None, // local_retention_ms — set after service creation.
        )
        .await;

        // Spawn controller tick task.
        let controller_shutdown_tx = {
            let (tx, rx) = mpsc::channel(1);
            tokio::spawn(tick::tick_task_controller(
                Arc::clone(&multi_raft),
                Arc::clone(&partition_storage),
                Arc::clone(&group_map),
                Arc::clone(&controller_state),
                Arc::clone(&pending_proposals),
                Arc::clone(&pending_controller_proposals),
                cluster_nodes.clone(),
                transport.clone(),
                Arc::clone(&actor_handles.router),
                actor_handles.output_tx.clone(),
                vote_store.clone(),
                shared_wal_pool.clone(),
                data_dir.clone(),
                Arc::clone(&recovered_entries),
                storage.clone(),
                Arc::clone(&local_broker_heartbeats),
                rx,
            ));
            tx
        };

        info!(
            node_id = node_id.get(),
            cluster_size = cluster_nodes.len(),
            "Started DST multi-node Helix service"
        );

        // Build peer addresses map - use placeholder addresses for DST.
        let peer_addrs: HashMap<NodeId, String> = cluster_nodes
            .iter()
            .map(|&n| (n, format!("127.0.0.1:{}", 9092 + n.get())))
            .collect();

        Self {
            cluster_id,
            node_id,
            multi_raft,
            partition_storage,
            storage,
            group_map,
            topics: Arc::new(RwLock::new(HashMap::new())),
            next_topic_id: Arc::new(RwLock::new(1)),
            cluster_nodes,
            peer_addrs,
            _shutdown_tx: shutdown_tx,
            data_dir,
            object_storage_dir: None,
            #[cfg(feature = "s3")]
            s3_config: None,
            tiering_config: None,
            transport_handle: Some(transport),
            progress_manager,
            controller_state,
            pending_proposals,
            pending_controller_proposals,
            local_broker_heartbeats,
            shared_wal_pool,
            recovered_entries,
            batch_pending_proposals,
            batcher_handle: Some(actor_handles.batcher_handle),
            batcher_stats: Some(actor_handles.batcher_stats),
            actor_router: Some(actor_handles.router),
            actor_output_tx: Some(actor_handles.output_tx.clone()),
            actor_shutdown_tx: Some(actor_handles.shutdown_tx),
            controller_shutdown_tx: Some(controller_shutdown_tx),
            actor_backpressure: Some(actor_handles.backpressure),
            vote_store,
            local_retention_ms: None,
        }
    }
}

// =============================================================================
// Backwards-Compatible Constructors for ProductionHelixService
// =============================================================================

/// Backwards-compatible constructors using `TokioStorage`.
///
/// These constructors automatically use `TokioStorage::new()` as the storage backend
/// and spawn background tick tasks for automatic Raft tick processing.
/// For DST or custom storage, use the generic constructors that accept a storage parameter.
impl ProductionHelixService {
    /// Creates a new Helix service with in-memory storage (for testing).
    ///
    /// This starts a background task to handle Raft ticks for all groups.
    pub async fn new(cluster_id: String, node_id: u64) -> Self {
        let (service, shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            None,
            None,
            None,
            WriteDurability::Fsync,
            TokioStorage::new(),
        )
        .await;

        // Spawn tick task for production use.
        tokio::spawn(tick::tick_task(
            Arc::clone(&service.multi_raft),
            Arc::clone(&service.partition_storage),
            Arc::clone(&service.group_map),
            Arc::clone(&service.pending_proposals),
            shutdown_rx,
        ));

        service
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
        let (service, shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            Some(data_dir),
            None,
            shared_wal_count,
            write_durability,
            TokioStorage::new(),
        )
        .await;

        // Spawn tick task for production use.
        tokio::spawn(tick::tick_task(
            Arc::clone(&service.multi_raft),
            Arc::clone(&service.partition_storage),
            Arc::clone(&service.group_map),
            Arc::clone(&service.pending_proposals),
            shutdown_rx,
        ));

        service
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
        let (service, shutdown_rx) = Self::new_internal(
            cluster_id,
            node_id,
            Some(data_dir),
            Some(object_storage_dir),
            shared_wal_count,
            write_durability,
            TokioStorage::new(),
        )
        .await;

        // Spawn tick task for production use.
        tokio::spawn(tick::tick_task(
            Arc::clone(&service.multi_raft),
            Arc::clone(&service.partition_storage),
            Arc::clone(&service.group_map),
            Arc::clone(&service.pending_proposals),
            shutdown_rx,
        ));

        service
    }

    /// Creates a new Helix service with multi-node networking.
    ///
    /// This starts both the Raft tick task and the transport for peer
    /// communication. Partition data is persisted to the specified directory.
    ///
    /// # Arguments
    /// * `shared_wal_count` - Number of shared WALs in pool (default: 4)
    /// * `write_durability` - Durability mode for writes
    ///
    /// # Errors
    /// Returns an error if the transport cannot be started.
    ///
    /// # Panics
    /// Panics if `shared_wal_count` is not in range [1, 16], or if the `SharedWalPool`
    /// fails to open or recover (indicates filesystem or corruption issues).
    #[allow(clippy::too_many_arguments)]
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
        local_retention_ms: Option<u64>,
    ) -> Result<Self, TransportError> {
        Self::new_multi_node_with_storage(
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
            TokioStorage::new(),
            local_retention_ms,
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
        local_retention_ms: Option<u64>,
    ) -> Result<Self, TransportError> {
        Self::new_multi_node_with_storage(
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
            TokioStorage::new(),
            local_retention_ms,
        )
        .await
    }
}

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
use helix_tier::{ObjectStorage, SimulatedObjectStorage, WalSegmentStoreAdapter};
use helix_wal::{PoolConfig, SharedEntry, SharedWalPool, Storage, TokioStorage};
use crate::storage::PartitionRecoveryState;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::{debug, error, info, warn};

use crate::controller::{ControllerState, BROKER_HEARTBEAT_TIMEOUT_MS, CONTROLLER_GROUP_ID};
use crate::group_map::GroupMap;
use crate::offset_group::{
    offset_group_slot, OFFSET_GROUP_COUNT, OFFSET_GROUP_COUNT_USIZE,
    OFFSET_GROUP_ID_BASE,
};
use crate::partition_storage::PartitionStorage;
use crate::snapshot::Snapshottable;
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

/// A pending offset-group proposal waiting for Raft commit.
///
/// Used for `OffsetGroupCommand::CommitOffset` proposals routed through
/// one of the N=3 dedicated offset Raft groups.
pub struct PendingOffsetProposal {
    /// The offset group that received this proposal.
    pub offset_group_id: GroupId,
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

/// Configures coordinator-level tiering on a `SharedWalPool`.
///
/// Selects the same object storage backend as per-partition tiering (S3 >
/// filesystem > simulated), wraps it in a `WalSegmentStoreAdapter`, and
/// calls `pool.configure_tiering()`. This enables the pool to upload and
/// download shared WAL segments independent of per-partition tiering.
///
/// The `base_prefix` for the pool is derived from the S3 key prefix (or
/// left empty for filesystem/simulated backends).
async fn configure_pool_tiering<S: helix_wal::Storage + Clone + Send + Sync + 'static>(
    pool: &SharedWalPool<S>,
    node_id: u64,
    object_storage_dir: Option<&std::path::PathBuf>,
    #[cfg(feature = "s3")] s3_config: Option<&helix_tier::S3Config>,
) {
    // Each node must use its own S3 prefix so that segment files from different
    // nodes (which share the same segment IDs but have different WAL content)
    // do not overwrite each other in object storage.
    let base_prefix = format!("node-{node_id}/");

    #[cfg(feature = "s3")]
    if let Some(cfg) = s3_config {
        use helix_tier::S3ObjectStorage;
        let store = S3ObjectStorage::new(cfg.clone())
            .await
            .expect("failed to create S3 object storage for pool tiering");
        // S3ObjectStorage prepends key_prefix internally. Each coordinator's
        // final S3 key will be:
        //   key_prefix + "{pod_name}/shared/{pool_index}/{segment_id:08x}.wal"
        let adapter: Arc<dyn helix_wal::WalSegmentStore> =
            Arc::new(WalSegmentStoreAdapter::new(store));
        pool.configure_tiering(adapter, base_prefix).await;
        info!("SharedWalPool tiering configured with S3 backend");
        return;
    }

    if let Some(dir) = object_storage_dir {
        use helix_tier::{FilesystemConfig, FilesystemObjectStorage};
        let fs_cfg = FilesystemConfig {
            base_path: dir.clone(),
            sync_on_write: false,
            create_if_missing: true,
        };
        let store = FilesystemObjectStorage::new(fs_cfg)
            .await
            .expect("failed to create filesystem object storage for pool tiering");
        let adapter: Arc<dyn helix_wal::WalSegmentStore> =
            Arc::new(WalSegmentStoreAdapter::new(store));
        pool.configure_tiering(adapter, base_prefix).await;
        info!(
            object_storage_dir = ?dir,
            "SharedWalPool tiering configured with filesystem backend"
        );
        return;
    }

    // No real backend configured; use simulated (no-op uploads).
    let store = SimulatedObjectStorage::new(0);
    let adapter: Arc<dyn helix_wal::WalSegmentStore> =
        Arc::new(WalSegmentStoreAdapter::new(store));
    pool.configure_tiering(adapter, base_prefix).await;
    info!("SharedWalPool tiering configured with simulated backend");
}

// =============================================================================
// Shared WAL + Snapshot Recovery
// =============================================================================

/// Result of WAL + snapshot recovery, used to initialize Raft groups.
///
/// Returned by [`recover_from_wal_and_snapshots`] and consumed by both
/// the production and DST constructors to create Raft groups with the
/// correct committed log position after restart.
struct WalRecoveryResult {
    /// Controller (commit_index, commit_term) from WAL replay.
    controller_commit: (u64, u64),
    /// Per-offset-group (commit_index, commit_term) from WAL replay.
    offset_commits: [(u64, u64); OFFSET_GROUP_COUNT_USIZE],
    /// Recovered data partition states (snapshot + WAL entries).
    data_partition_states: HashMap<GroupId, PartitionRecoveryState>,
}

/// Recovers controller, offset group, and data partition state from
/// snapshots and WAL replay.
///
/// This function is the single source of truth for startup recovery logic,
/// called by both the production (`new_multi_node_internal`) and DST
/// (`new_multi_node_with_transport`) constructors. It:
///
/// 1. Loads controller + offset group snapshots (if `snapshot_store` is set)
/// 2. Replays WAL entries via `pool.recover_streaming()`
/// 3. Rebuilds controller and offset group state machines
/// 4. Loads data partition snapshots and replays remaining WAL entries
///
/// The caller must open the pool and configure tiering/S3 downloads
/// before calling this function.
#[allow(clippy::too_many_lines)]
async fn recover_from_wal_and_snapshots<S: Storage + Clone + Send + Sync + 'static>(
    pool: &SharedWalPool<S>,
    snapshot_store: Option<&Arc<crate::snapshot::SnapshotStore>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    offset_group_states: &[Arc<RwLock<crate::offset_group::OffsetGroupState>>],
    cluster_nodes: &[NodeId],
    node_id: NodeId,
) -> WalRecoveryResult {
    // -----------------------------------------------------------------
    // 1. Load controller snapshot (if available) before WAL replay.
    // -----------------------------------------------------------------
    let controller_snapshot_index: u64 = if let Some(store) = snapshot_store {
        match store.load(CONTROLLER_GROUP_ID).await {
            Some((meta, body, from_remote)) => {
                let apply_result = controller_state
                    .write()
                    .await
                    .apply_snapshot(&meta, body.as_ref());
                match apply_result {
                    Ok(()) => {
                        info!(
                            snap_index = meta.last_included_index,
                            snap_term = meta.last_included_term,
                            from_remote,
                            "Loaded controller snapshot; WAL entries at or below \
                             index will be skipped"
                        );
                        meta.last_included_index
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "Failed to apply controller snapshot; replaying WAL"
                        );
                        0
                    }
                }
            }
            None => 0,
        }
    } else {
        0
    };

    // -----------------------------------------------------------------
    // 2. Load offset group snapshots (if available).
    // -----------------------------------------------------------------
    let mut offset_snapshot_indices = [0u64; OFFSET_GROUP_COUNT_USIZE];
    if let Some(store) = snapshot_store {
        for slot in 0..OFFSET_GROUP_COUNT_USIZE {
            #[allow(clippy::cast_possible_truncation)]
            let group_id = GroupId::new(OFFSET_GROUP_ID_BASE.get() + slot as u64);
            if let Some((meta, body, from_remote)) = store.load(group_id).await {
                let apply_result = offset_group_states[slot]
                    .write()
                    .await
                    .apply_snapshot(&meta, body.as_ref());
                match apply_result {
                    Ok(()) => {
                        info!(
                            slot,
                            snap_index = meta.last_included_index,
                            snap_term = meta.last_included_term,
                            from_remote,
                            "Loaded offset group snapshot; WAL entries at or \
                             below index will be skipped"
                        );
                        offset_snapshot_indices[slot] = meta.last_included_index;
                    }
                    Err(e) => {
                        warn!(
                            slot,
                            error = %e,
                            "Failed to apply offset group snapshot; replaying WAL"
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // 3. Streaming recovery: collect all entries by category.
    // -----------------------------------------------------------------
    let mut controller_entries_raw: Vec<SharedEntry> = Vec::new();
    let mut offset_entries_raw: Vec<Vec<SharedEntry>> =
        vec![Vec::new(); OFFSET_GROUP_COUNT_USIZE];
    let mut data_entries_raw: HashMap<GroupId, Vec<SharedEntry>> = HashMap::new();
    pool.recover_streaming(&mut |group_id, entry| {
        if group_id == CONTROLLER_GROUP_ID {
            controller_entries_raw.push(entry.clone());
        } else if let Some(slot) = offset_group_slot(group_id) {
            offset_entries_raw[slot].push(entry.clone());
        } else {
            data_entries_raw
                .entry(group_id)
                .or_default()
                .push(entry.clone());
        }
    })
    .await
    .expect("Failed to recover from SharedWalPool");

    info!(
        data_groups = data_entries_raw.len(),
        controller_entries = controller_entries_raw.len(),
        "SharedWalPool recovery complete"
    );

    // -----------------------------------------------------------------
    // 4. Replay controller entries into ControllerState.
    //
    // We use max_raft_index (not max_contiguous) for both the Raft
    // commit_index and the WAL write guard. The state machine is built
    // from ALL entries found in the WAL during startup replay (not just
    // up to max_contiguous), so the Raft commit_index must match to
    // prevent re-delivery of already-applied entries.
    // -----------------------------------------------------------------
    let (ctl_commit_index, ctl_commit_term) = {
        let (max_contiguous_index, max_contiguous_term) = {
            let mut sorted: Vec<u64> = controller_entries_raw
                .iter()
                .map(|e| e.header.raft_index)
                .collect();
            sorted.sort_unstable();
            sorted.dedup();
            if sorted.is_empty() {
                (0u64, 0u64)
            } else {
                let min_ri = sorted[0];
                let mut last_contiguous = min_ri.saturating_sub(1);
                let terms: std::collections::HashMap<u64, u64> =
                    controller_entries_raw
                        .iter()
                        .map(|e| (e.header.raft_index, e.header.term))
                        .collect();
                for idx in sorted {
                    if idx == last_contiguous + 1 {
                        last_contiguous = idx;
                    } else {
                        break;
                    }
                }
                let term =
                    terms.get(&last_contiguous).copied().unwrap_or(0);
                (last_contiguous, term)
            }
        };

        let mut state = controller_state.write().await;
        let mut count = 0u64;
        let (max_raft_index, max_raft_term) = controller_entries_raw
            .iter()
            .max_by_key(|e| e.header.raft_index)
            .map_or((0, 0), |e| (e.header.raft_index, e.header.term));
        for entry in &controller_entries_raw {
            if entry.header.raft_index <= controller_snapshot_index {
                continue;
            }
            if let Some(cmd) =
                crate::controller::ControllerCommand::decode(&entry.payload)
            {
                let _ = state.apply(&cmd, cluster_nodes);
                state.last_applied_index = entry.header.raft_index;
                state.last_applied_term = entry.header.term;
                state.last_applied_wal_index = entry.header.index;
                count += 1;
            } else {
                warn!(
                    raft_index = entry.header.raft_index,
                    "Failed to decode controller entry during recovery"
                );
            }
        }
        drop(state);

        let effective_max = max_raft_index.max(controller_snapshot_index);
        tick::CONTROLLER_LAST_PERSISTED_INDEX
            .store(effective_max, std::sync::atomic::Ordering::Relaxed);
        if count > 0 || controller_snapshot_index > 0 {
            info!(
                replayed = count,
                snapshot_index = controller_snapshot_index,
                max_contiguous_index,
                max_contiguous_term,
                max_raft_index,
                wal_holes =
                    max_raft_index.saturating_sub(max_contiguous_index),
                "Replayed controller state from SharedWAL"
            );
        }
        (
            effective_max,
            if max_raft_index > 0 { max_raft_term } else { 0 },
        )
    };

    // -----------------------------------------------------------------
    // 5. Replay offset group entries.
    // -----------------------------------------------------------------
    let mut offset_commit_indices =
        [(0u64, 0u64); OFFSET_GROUP_COUNT_USIZE];
    for (slot, entries) in offset_entries_raw.into_iter().enumerate() {
        let (max_contiguous_index, max_contiguous_term) = {
            let mut sorted: Vec<u64> =
                entries.iter().map(|e| e.header.raft_index).collect();
            sorted.sort_unstable();
            sorted.dedup();
            if sorted.is_empty() {
                (0u64, 0u64)
            } else {
                let min_ri = sorted[0];
                let mut last_contiguous = min_ri.saturating_sub(1);
                let terms: std::collections::HashMap<u64, u64> = entries
                    .iter()
                    .map(|e| (e.header.raft_index, e.header.term))
                    .collect();
                for idx in sorted {
                    if idx == last_contiguous + 1 {
                        last_contiguous = idx;
                    } else {
                        break;
                    }
                }
                let term =
                    terms.get(&last_contiguous).copied().unwrap_or(0);
                (last_contiguous, term)
            }
        };

        let (max_raft_index, max_raft_term) = entries
            .iter()
            .max_by_key(|e| e.header.raft_index)
            .map_or((0, 0), |e| (e.header.raft_index, e.header.term));
        let snap_index = offset_snapshot_indices[slot];
        let mut count = 0u64;
        let mut state = offset_group_states[slot].write().await;
        // Lazy registration: only register offset groups that have WAL
        // entries or a snapshot. Groups with no entries must not pin the
        // eviction floor at 0.
        #[allow(clippy::cast_possible_truncation)]
        let og_group_id = GroupId::new(OFFSET_GROUP_ID_BASE.get() + slot as u64);
        if !entries.is_empty() || snap_index > 0 {
            pool.register_group(og_group_id);
        }
        for entry in entries {
            if entry.header.raft_index <= snap_index {
                continue;
            }
            if let Some(cmd) =
                crate::offset_group::OffsetGroupCommand::decode(
                    &entry.payload,
                )
            {
                state.apply(
                    &cmd,
                    entry.header.raft_index,
                    entry.header.term,
                );
                state.last_applied_wal_index = entry.header.index;
                count += 1;
            } else {
                warn!(
                    slot,
                    raft_index = entry.header.raft_index,
                    "Failed to decode offset group entry during recovery"
                );
            }
        }
        drop(state);

        let effective_max = max_raft_index.max(snap_index);
        offset_commit_indices[slot] = (
            effective_max,
            if max_raft_index > 0 { max_raft_term } else { 0 },
        );
        tick::OFFSET_GROUP_LAST_PERSISTED[slot]
            .store(effective_max, std::sync::atomic::Ordering::Relaxed);
        if count > 0 || snap_index > 0 {
            info!(
                slot,
                replayed = count,
                snapshot_index = snap_index,
                max_contiguous_index,
                max_contiguous_term,
                max_raft_index,
                wal_holes =
                    max_raft_index.saturating_sub(max_contiguous_index),
                "Replayed offset group state from SharedWAL"
            );
        }
    }

    // -----------------------------------------------------------------
    // 6. Load data partition snapshots (after controller is rebuilt).
    // -----------------------------------------------------------------
    let mut partition_snapshot_anchors: HashMap<GroupId, u64> =
        HashMap::new();
    let mut data_partition_states: HashMap<GroupId, PartitionRecoveryState> =
        HashMap::new();
    if let Some(store) = snapshot_store {
        let assigned_group_ids: Vec<GroupId> = {
            let state = controller_state.read().await;
            state
                .all_assignments()
                .filter(|(_, a)| a.replicas.contains(&node_id))
                .map(|(_, a)| a.group_id)
                .collect()
        };
        for group_id in assigned_group_ids {
            if let Some((meta, body, from_remote)) =
                store.load(group_id).await
            {
                match PartitionRecoveryState::from_snapshot(&meta, &body) {
                    Ok(recovery_state) => {
                        info!(
                            group_id = group_id.get(),
                            snap_index = meta.last_included_index,
                            snap_wal_index =
                                meta.last_included_wal_index,
                            from_remote,
                            "Loaded partition snapshot; WAL entries \
                             at or below wal_index will be skipped"
                        );
                        partition_snapshot_anchors.insert(
                            group_id,
                            meta.last_included_wal_index,
                        );
                        data_partition_states
                            .insert(group_id, recovery_state);
                    }
                    Err(e) => {
                        warn!(
                            group_id = group_id.get(),
                            error = %e,
                            "Failed to decode partition snapshot; \
                             replaying WAL fully"
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // 7. Replay data partition WAL entries.
    // -----------------------------------------------------------------
    for (group_id, entries) in &data_entries_raw {
        let state = data_partition_states.entry(*group_id).or_default();
        let anchor = partition_snapshot_anchors.get(group_id).copied();
        for entry in entries {
            if let Some(a) = anchor {
                if entry.index() <= a {
                    continue;
                }
            }
            state.apply_entry(entry);
        }
    }

    if !data_partition_states.is_empty() {
        info!(
            partitions = data_partition_states.len(),
            snapshots = partition_snapshot_anchors.len(),
            "Data partition recovery complete"
        );
    }

    WalRecoveryResult {
        controller_commit: (ctl_commit_index, ctl_commit_term),
        offset_commits: offset_commit_indices,
        data_partition_states,
    }
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
    /// Pre-built recovery states from streaming WAL recovery, indexed by `GroupId`.
    /// Used during partition creation to restore state without re-reading WAL entries.
    pub(crate) recovered_entries: Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>,
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
    pub(crate) actor_backpressure: Arc<batcher::BackpressureState>,
    /// Output processor performance stats (multi-node actor mode only).
    pub(crate) output_processor_stats: Option<Arc<OutputProcessorStats>>,
    /// Vote store for persisting Raft vote state (multi-node only).
    /// Uses Arc<Mutex> for thread-safe access from tick tasks.
    #[allow(dead_code)] // Used by tick tasks for vote persistence.
    pub(crate) vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    /// Per-offset-group in-memory state (len = `OFFSET_GROUP_COUNT`).
    pub(crate) offset_group_states: Vec<Arc<RwLock<crate::offset_group::OffsetGroupState>>>,
    /// Pending proposals across all offset groups.
    pub(crate) pending_offset_proposals: Arc<RwLock<Vec<PendingOffsetProposal>>>,
    /// Unified snapshot store for all Raft group types (None in test/DST mode).
    #[allow(dead_code)]
    pub(crate) snapshot_store: Option<Arc<crate::snapshot::SnapshotStore>>,
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
    #[allow(clippy::too_many_lines)] // Constructor with initialization logic.
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

        // Create a local snapshot store for disk mode (uses SimulatedObjectStorage
        // as a no-op S3 backend so the upload channel compiles but never uploads).
        // Snapshots are persisted to local disk; S3 upload is skipped.
        let snapshot_store_internal: Option<Arc<crate::snapshot::SnapshotStore>> =
            data_dir.as_ref().map(|dir| {
                let remote: Arc<dyn ObjectStorage> =
                    Arc::new(SimulatedObjectStorage::new(0));
                Arc::new(crate::snapshot::SnapshotStore::new(
                    dir.clone(),
                    format!("node-{}/", node_id.get()),
                    remote,
                ))
            });

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

            // Configure coordinator-level tiering for filesystem backend if available.
            if object_storage_dir.is_some() {
                configure_pool_tiering(&pool, node_id.get(), object_storage_dir.as_ref(), #[cfg(feature = "s3")] None).await;

                // Download any segments present in object storage but missing from local
                // disk before replay. This recovers a pod that restarted on a new node with
                // an empty local directory.
                match pool.download_missing_segments().await {
                    Ok(n) if n > 0 => info!(
                        downloaded = n,
                        "Downloaded missing shared WAL segments from object storage before replay"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(
                        error = %e,
                        "Failed to download missing shared WAL segments from object storage"
                    ),
                }
            }

            // Load partition snapshots (local disk only) before WAL replay.
            let mut partition_snapshot_anchors_internal: HashMap<GroupId, u64> =
                HashMap::new();
            let mut data_partition_states_init_internal: HashMap<GroupId, PartitionRecoveryState> =
                HashMap::new();
            if let Some(ref store) = snapshot_store_internal {
                for group_id in store.list_local_group_ids() {
                    if group_id == CONTROLLER_GROUP_ID {
                        continue;
                    }
                    if let Some((meta, body, _)) = store.load(group_id).await {
                        match PartitionRecoveryState::from_snapshot(&meta, &body) {
                            Ok(state) => {
                                info!(
                                    group_id = group_id.get(),
                                    snap_wal_index = meta.last_included_wal_index,
                                    "Loaded partition snapshot (single-node); WAL \
                                     entries at or below wal_index will be skipped"
                                );
                                partition_snapshot_anchors_internal
                                    .insert(group_id, meta.last_included_wal_index);
                                data_partition_states_init_internal
                                    .insert(group_id, state);
                            }
                            Err(e) => {
                                warn!(
                                    group_id = group_id.get(),
                                    error = %e,
                                    "Failed to decode partition snapshot; replaying WAL"
                                );
                            }
                        }
                    }
                }
            }

            // Streaming recovery: build pre-built states per group without
            // accumulating all entries in memory (fixes OOM on large WALs).
            let mut data_partition_states = data_partition_states_init_internal;
            pool.recover_streaming(&mut |group_id, entry| {
                if group_id != CONTROLLER_GROUP_ID {
                    // Skip entries already covered by a partition snapshot.
                    if let Some(&anchor) =
                        partition_snapshot_anchors_internal.get(&group_id)
                    {
                        if entry.index() <= anchor {
                            return;
                        }
                    }
                    data_partition_states
                        .entry(group_id)
                        .or_default()
                        .apply_entry(entry);
                }
                // Controller entries in single-node mode are not used — skip.
            })
            .await
            .expect("Failed to recover from SharedWalPool");

            info!(
                partitions = data_partition_states.len(),
                "SharedWalPool recovery complete"
            );

            (
                Some(Arc::new(pool)),
                Arc::new(RwLock::new(data_partition_states)),
            )
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
            actor_backpressure: batcher::BackpressureState::noop(),
            output_processor_stats: None,
            vote_store: None, // Single-node mode doesn't persist vote state.
            offset_group_states: (0..OFFSET_GROUP_COUNT_USIZE)
                .map(|slot| Arc::new(RwLock::new(crate::offset_group::OffsetGroupState::new_for_slot(slot))))
                .collect(),
            pending_offset_proposals: Arc::new(RwLock::new(Vec::new())),
            snapshot_store: snapshot_store_internal,
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

        // Create transport but do NOT start it yet (don't open the TCP port).
        // Starting the listener before S3 recovery and WAL replay completes causes
        // peers to connect and queue up Raft messages that go unread for several
        // minutes, triggering TCP_USER_TIMEOUT reconnect loops on every rolling deploy.
        // The port opens after recovery is complete, just before the service loop starts.
        let (transport, incoming_rx) = helix_runtime::Transport::new(transport_config);

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
        // Also retains remote_storage so snapshot stores can share the same backend.
        let (vote_store, initial_vote_state, recovered_from_remote, remote_object_storage) =
            if let Some(ref dir) = data_dir {
                let vote_file_path = dir.join("vote-state.bin");
                let local_storage = Arc::new(LocalFileVoteStorage::new(vote_file_path));

                // Build remote storage shared by vote state, snapshot stores, and tiering.
                // Uses S3 in production; falls back to simulated storage otherwise.
                #[cfg(feature = "s3")]
                let remote_storage: Arc<dyn ObjectStorage> =
                    if let Some(ref cfg) = s3_config {
                        Arc::new(
                            helix_tier::S3ObjectStorage::new(cfg.clone())
                                .await
                                .expect("failed to create S3 object storage for vote state"),
                        )
                    } else {
                        Arc::new(SimulatedObjectStorage::new(node_id.get()))
                    };
                #[cfg(not(feature = "s3"))]
                let remote_storage: Arc<dyn ObjectStorage> =
                    Arc::new(SimulatedObjectStorage::new(node_id.get()));

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

                // Clone remote_storage before moving into VoteStore so snapshot
                // stores can share the same object storage backend.
                let remote_for_snapshots = Arc::clone(&remote_storage);

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
                    Some(remote_for_snapshots),
                )
            } else {
                // In-memory mode - no vote persistence.
                (None, VoteState::new(), false, None)
            };

        // Create the unified snapshot store.
        // Only created when a data_dir is set (multi-node production/test).
        // Loaded before WAL replay so we can skip already-snapshotted entries.
        let snapshot_store_opt: Option<Arc<crate::snapshot::SnapshotStore>> =
            data_dir.as_ref().map(|dir| {
                // Use the real S3 backend when configured, otherwise fall back to
                // a no-op SimulatedObjectStorage so that local-only snapshots
                // (disk save/load) still work without S3.
                let remote: Arc<dyn ObjectStorage> =
                    if let Some(ref real) = remote_object_storage {
                        Arc::clone(real)
                    } else {
                        Arc::new(SimulatedObjectStorage::new(node_id.get()))
                    };
                let s3_prefix = format!("node-{}/", node_id.get());
                Arc::new(crate::snapshot::SnapshotStore::new(
                    dir.clone(),
                    s3_prefix,
                    remote,
                ))
            });

        // Controller partition (group 0) is created after WAL recovery so that
        // the committed log position from the WAL can be passed to the Raft
        // initialiser.  This prevents a new leader from overwriting already-
        // committed controller entries on nodes whose in-memory Raft state was
        // reset to commit_index=0 after restart.

        // Create progress manager.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        // Initialize per-offset-group state (populated by WAL recovery below).
        let offset_group_states_vec: Vec<Arc<RwLock<crate::offset_group::OffsetGroupState>>> =
            (0..OFFSET_GROUP_COUNT_USIZE)
                .map(|slot| Arc::new(RwLock::new(crate::offset_group::OffsetGroupState::new_for_slot(slot))))
                .collect();
        let pending_offset_proposals: Arc<RwLock<Vec<PendingOffsetProposal>>> =
            Arc::new(RwLock::new(Vec::new()));

        // Initialize SharedWalPool if data_dir is set.
        // WAL + snapshot recovery is handled by recover_from_wal_and_snapshots().
        #[allow(clippy::type_complexity)]
        let (shared_wal_pool, recovered_entries, controller_wal_commit_index, controller_wal_commit_term, offset_commit_indices) :
            (Option<Arc<SharedWalPool<S>>>, Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>, u64, u64, [(u64, u64); OFFSET_GROUP_COUNT_USIZE]) =
            if let Some(ref dir) = data_dir {
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

            // Configure coordinator-level tiering on the pool if tiering is enabled.
            // Uses the same backend selection logic as per-partition tiering.
            if tiering_config.is_some() {
                configure_pool_tiering(&pool, node_id.get(), object_storage_dir.as_ref(), #[cfg(feature = "s3")] s3_config.as_ref()).await;

                // Download any segments present in object storage but missing from local
                // disk before replay. This recovers a pod that restarted on a new node with
                // an empty local directory.
                match pool.download_missing_segments().await {
                    Ok(n) if n > 0 => info!(
                        downloaded = n,
                        "Downloaded missing shared WAL segments from object storage before replay (multi-node)"
                    ),
                    Ok(_) => {}
                    Err(e) => warn!(
                        error = %e,
                        "Failed to download missing shared WAL segments from object storage (multi-node)"
                    ),
                }
            }

            // Run shared WAL + snapshot recovery.
            let recovery = recover_from_wal_and_snapshots(
                &pool,
                snapshot_store_opt.as_ref(),
                &controller_state,
                &offset_group_states_vec,
                &cluster_nodes,
                node_id,
            )
            .await;

            (
                Some(Arc::new(pool)),
                Arc::new(RwLock::new(recovery.data_partition_states)),
                recovery.controller_commit.0,
                recovery.controller_commit.1,
                recovery.offset_commits,
            )
        } else {
            // In-memory mode — no persisted state, start from zero.
            (None, Arc::new(RwLock::new(HashMap::new())), 0u64, 0u64, [(0u64, 0u64); OFFSET_GROUP_COUNT_USIZE])
        };

        // Register controller with WAL pool for snapshot floor tracking.
        // Offset groups are registered lazily on first WAL append — groups
        // that never process entries (no consumer groups) must not pin the
        // eviction floor at 0.
        if let Some(ref pool) = shared_wal_pool {
            pool.register_group(CONTROLLER_GROUP_ID);
        }

        // Create controller partition (group 0) with all cluster nodes.
        // Uses WAL-recovered commit state so the Raft log's compacted position
        // reflects entries that are already durable.  This prevents a new leader
        // from overwriting committed entries at indices that a recovering follower
        // has already marked as applied.
        {
            let controller_vote = initial_vote_state.get_group(CONTROLLER_GROUP_ID);
            let (term, voted_for) =
                controller_vote.map_or((TermId::new(0), None), |v| (v.term, v.voted_for));
            let commit_index = LogIndex::new(controller_wal_commit_index);
            let commit_term = TermId::new(controller_wal_commit_term);

            let create_result = multi_raft.write().await.create_group_with_recovery_state(
                CONTROLLER_GROUP_ID,
                cluster_nodes.clone(),
                term,
                voted_for,
                recovered_from_remote, // observation mode if recovered from S3
                commit_index,
                commit_term,
            );

            match create_result {
                Ok(()) => {
                    info!(
                        group_id = CONTROLLER_GROUP_ID.get(),
                        term = term.get(),
                        voted_for = voted_for.map(NodeId::get),
                        observation_mode = recovered_from_remote,
                        commit_index = commit_index.get(),
                        commit_term = commit_term.get(),
                        nodes = ?cluster_nodes.iter().map(|n| n.get()).collect::<Vec<_>>(),
                        "Created controller partition with restored vote and WAL commit state"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Failed to create controller partition");
                }
            }
        }

        // Create offset groups for consumer group offset storage (IDs 1_000_000–1_000_002).
        // Each group covers a hash bucket: offset_group_id(consumer_group) routes to one of these.
        for i in 0..OFFSET_GROUP_COUNT {
            let gid = GroupId::new(OFFSET_GROUP_ID_BASE.get() + i);
            // Safe: i is in [0, OFFSET_GROUP_COUNT), always fits in usize.
            #[allow(clippy::cast_possible_truncation)]
            let slot = i as usize;
            let offset_vote = initial_vote_state.get_group(gid);
            let (term, voted_for) =
                offset_vote.map_or((TermId::new(0), None), |v| (v.term, v.voted_for));
            let (commit_index_val, commit_term_val) = offset_commit_indices[slot];
            let commit_index = LogIndex::new(commit_index_val);
            let commit_term = TermId::new(commit_term_val);

            let create_result = multi_raft.write().await.create_group_with_recovery_state(
                gid,
                cluster_nodes.clone(),
                term,
                voted_for,
                recovered_from_remote,
                commit_index,
                commit_term,
            );

            match create_result {
                Ok(()) => {
                    info!(
                        group_id = gid.get(),
                        term = term.get(),
                        commit_index = commit_index.get(),
                        commit_term = commit_term.get(),
                        "Created offset partition with restored state"
                    );
                }
                Err(e) => {
                    error!(error = %e, group_id = gid.get(), "Failed to create offset partition");
                }
            }
        }

        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        // Create batch pending proposals map.
        let batch_pending_proposals: BatchPendingProposalMap =
            Arc::new(RwLock::new(HashMap::new()));

        // S3 recovery and WAL replay are complete. Open the transport port now so
        // peers can connect and send Raft messages to a node that is ready to process them.
        let transport_handle = transport.start().await?;

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
            actor_setup::ActorSetupConfig::default(), // production: no trailing override
            vote_store.clone(),
            shared_wal_pool.clone(),
            data_dir.clone(),
            Arc::clone(&recovered_entries),
            storage.clone(),
            local_retention_ms,
            offset_group_states_vec.clone(),
            Arc::clone(&pending_offset_proposals),
            snapshot_store_opt.clone(),
        )
        .await;

        // Spawn controller tick task (handles controller + offset partitions via MultiRaft).
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
                offset_group_states_vec.clone(),
                Arc::clone(&pending_offset_proposals),
                Arc::clone(&actor_handles.backpressure),
                snapshot_store_opt.clone(),
                None, // log_trailing_entries: use default in production
                actor_handles.leader_update_rx,
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
            actor_backpressure: actor_handles.backpressure,
            output_processor_stats: Some(actor_handles.output_processor_stats),
            vote_store,
            offset_group_states: offset_group_states_vec,
            pending_offset_proposals,
            snapshot_store: snapshot_store_opt,
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
            actor_backpressure: batcher::BackpressureState::noop(),
            output_processor_stats: None,
            vote_store: None,
            offset_group_states: (0..OFFSET_GROUP_COUNT_USIZE)
                .map(|slot| Arc::new(RwLock::new(crate::offset_group::OffsetGroupState::new_for_slot(slot))))
                .collect(),
            pending_offset_proposals: Arc::new(RwLock::new(Vec::new())),
            snapshot_store: None,
            local_retention_ms: None,
        }
    }

    /// Returns the shared WAL pool (if durable storage is enabled).
    #[must_use]
    pub fn shared_wal_pool(&self) -> Option<Arc<SharedWalPool<S>>> {
        self.shared_wal_pool.clone()
    }

    /// Returns the snapshot store (if snapshot persistence is enabled).
    #[must_use]
    pub fn snapshot_store(&self) -> Option<Arc<crate::snapshot::SnapshotStore>> {
        self.snapshot_store.clone()
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

    /// Returns the output processor stats, if running in actor mode.
    #[must_use]
    pub const fn output_processor_stats(&self) -> Option<&Arc<OutputProcessorStats>> {
        self.output_processor_stats.as_ref()
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

    /// Returns access to recovered partition states for DST.
    #[must_use]
    pub const fn recovered_entries(
        &self,
    ) -> &Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>> {
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

        let mut mr = MultiRaft::new(node_id);
        // Use a small trailing window in DST so the in-memory Raft log
        // compacts aggressively. This forces NeedEntries when a restarted
        // follower needs entries that are no longer in memory, exercising
        // the WAL read → SegmentNotFound → snapshot transfer chain.
        mr.set_log_trailing_entries_override(10);
        let multi_raft = Arc::new(RwLock::new(mr));
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

        // Create progress manager.
        let progress_store = SimulatedProgressStore::new(node_id.get());
        let progress_config = ProgressConfig::for_testing();
        let progress_manager = Arc::new(ProgressManager::new(progress_store, progress_config));

        // Initialize per-offset-group state (populated by WAL recovery below).
        let offset_group_states_vec: Vec<Arc<RwLock<crate::offset_group::OffsetGroupState>>> =
            (0..OFFSET_GROUP_COUNT_USIZE)
                .map(|slot| Arc::new(RwLock::new(crate::offset_group::OffsetGroupState::new_for_slot(slot))))
                .collect();
        let pending_offset_proposals_dst: Arc<RwLock<Vec<PendingOffsetProposal>>> =
            Arc::new(RwLock::new(Vec::new()));

        // Create snapshot store so DST exercises the same snapshot code paths
        // as production. Uses SimulatedObjectStorage (no real S3).
        let snapshot_store_opt: Option<Arc<crate::snapshot::SnapshotStore>> =
            data_dir.as_ref().map(|dir| {
                let remote: Arc<dyn ObjectStorage> =
                    Arc::new(SimulatedObjectStorage::new(node_id.get()));
                Arc::new(crate::snapshot::SnapshotStore::new(
                    dir.clone(),
                    format!("node-{}/", node_id.get()),
                    remote,
                ))
            });

        // Initialize SharedWalPool if data_dir and shared_wal_count are set.
        // Uses the same shared recovery function as production so DST
        // exercises controller/offset snapshot + WAL replay paths.
        let (shared_wal_pool, recovered_entries, ctl_commit_index, ctl_commit_term, offset_commit_indices) =
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
                    .with_durability(write_durability)
                    .with_segment_config(
                        helix_wal::SegmentConfig::new().with_max_entries(10),
                    );

                let pool = SharedWalPool::open(storage.clone(), pool_config)
                    .await
                    .expect("Failed to open SharedWalPool");

                // Configure simulated tiering so that retention deletes local
                // segment files (file-only) while keeping data accessible via
                // the S3 fallback read path. This exercises the full chain:
                // segment seals → tiering uploads → local file deleted →
                // consumer reads fall back to S3 → Raft NeedEntries gets
                // SegmentNotFound → snapshot transfer.
                configure_pool_tiering(
                    &pool,
                    node_id.get(),
                    None,
                    #[cfg(feature = "s3")]
                    None,
                )
                .await;

                // Run shared WAL + snapshot recovery (same path as production).
                let recovery = recover_from_wal_and_snapshots(
                    &pool,
                    snapshot_store_opt.as_ref(),
                    &controller_state,
                    &offset_group_states_vec,
                    &cluster_nodes,
                    node_id,
                )
                .await;

                (
                    Some(Arc::new(pool)),
                    Arc::new(RwLock::new(recovery.data_partition_states)),
                    recovery.controller_commit.0,
                    recovery.controller_commit.1,
                    recovery.offset_commits,
                )
            } else {
                if data_dir.is_some() {
                    info!("Using per-partition WAL mode (no SharedWalPool)");
                }
                (None, Arc::new(RwLock::new(HashMap::new())), 0u64, 0u64, [(0u64, 0u64); OFFSET_GROUP_COUNT_USIZE])
            };

        // Create controller partition (group 0) AFTER WAL recovery so the
        // commit_index from recovery can be passed to the Raft initialiser.
        {
            let commit_index = LogIndex::new(ctl_commit_index);
            let commit_term = TermId::new(ctl_commit_term);
            let create_result = multi_raft.write().await.create_group_with_recovery_state(
                CONTROLLER_GROUP_ID,
                cluster_nodes.clone(),
                TermId::new(0),
                None,
                false, // no observation mode in DST
                commit_index,
                commit_term,
            );
            match create_result {
                Ok(()) => {
                    info!(
                        group_id = CONTROLLER_GROUP_ID.get(),
                        commit_index = commit_index.get(),
                        commit_term = commit_term.get(),
                        "Created controller partition for DST with recovery state"
                    );
                }
                Err(e) => {
                    error!(error = %e, "Failed to create controller partition");
                }
            }
        }

        // Create offset groups (IDs 1–3) with recovery state.
        for i in 0..OFFSET_GROUP_COUNT {
            let gid = GroupId::new(OFFSET_GROUP_ID_BASE.get() + i);
            #[allow(clippy::cast_possible_truncation)]
            let slot = i as usize;
            let (ci_val, ct_val) = offset_commit_indices[slot];
            let commit_index = LogIndex::new(ci_val);
            let commit_term = TermId::new(ct_val);
            let result = multi_raft.write().await.create_group_with_recovery_state(
                gid,
                cluster_nodes.clone(),
                TermId::new(0),
                None,
                false,
                commit_index,
                commit_term,
            );
            match result {
                Ok(()) => info!(
                    group_id = gid.get(),
                    commit_index = commit_index.get(),
                    "Created offset partition for DST with recovery state"
                ),
                Err(e) => error!(
                    error = %e,
                    group_id = gid.get(),
                    "Failed to create offset partition"
                ),
            }
        }

        // Register controller with WAL pool for snapshot floor tracking.
        // Offset groups are registered lazily on first WAL append — groups
        // that never process entries (no consumer groups) must not pin the
        // eviction floor at 0.
        if let Some(ref pool) = shared_wal_pool {
            pool.register_group(CONTROLLER_GROUP_ID);
        }

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
            actor_setup::ActorSetupConfig {
                log_trailing_entries: Some(10),
                ..Default::default()
            },
            vote_store.clone(),
            shared_wal_pool.clone(),
            data_dir.clone(),
            Arc::clone(&recovered_entries),
            storage.clone(),
            // Enable retention only when shared WAL is active (snapshot
            // infrastructure provides the fallback for evicted entries).
            if shared_wal_pool.is_some() { Some(500) } else { None },
            offset_group_states_vec.clone(),
            Arc::clone(&pending_offset_proposals_dst),
            snapshot_store_opt.clone(),
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
                offset_group_states_vec.clone(),
                Arc::clone(&pending_offset_proposals_dst),
                Arc::clone(&actor_handles.backpressure),
                snapshot_store_opt.clone(),
                Some(10), // log_trailing_entries: compact aggressively in DST
                actor_handles.leader_update_rx,
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
            // Enable retention only when shared WAL is active (snapshot
            // infrastructure provides the fallback for evicted entries).
            local_retention_ms: if shared_wal_pool.is_some() { Some(500) } else { None },
            shared_wal_pool,
            recovered_entries,
            batch_pending_proposals,
            batcher_handle: Some(actor_handles.batcher_handle),
            batcher_stats: Some(actor_handles.batcher_stats),
            actor_router: Some(actor_handles.router),
            actor_output_tx: Some(actor_handles.output_tx.clone()),
            actor_shutdown_tx: Some(actor_handles.shutdown_tx),
            controller_shutdown_tx: Some(controller_shutdown_tx),
            actor_backpressure: actor_handles.backpressure,
            output_processor_stats: Some(actor_handles.output_processor_stats),
            vote_store,
            offset_group_states: offset_group_states_vec,
            pending_offset_proposals: pending_offset_proposals_dst,
            snapshot_store: snapshot_store_opt,
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

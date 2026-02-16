//! Partition Actor for lock-free multi-partition scalability.
//!
//! Each partition runs as an independent actor with its own Raft state machine.
//! This eliminates lock contention between partitions, enabling full parallelism.
//!
//! # Architecture
//!
//! ```text
//! Client Request
//!       |
//!       v
//! +------------------+
//! | PartitionActor   |------> Transport (send to peers)
//! |   - RaftNode     |
//! |   - Pending      |<------ Transport (receive from peers)
//! +------------------+
//!       |
//!       v
//!   WalActor (batched writes)
//! ```
//!
//! # Message Flow
//!
//! 1. `Propose`: Client submits entry, actor proposes to Raft
//! 2. `Tick`: Periodic tick for election timeouts and heartbeats
//! 3. `RaftMessage`: Inbound message from peer
//! 4. `ProduceRequest`: Direct produce for per-partition batching

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use helix_core::{GroupId, LogIndex, NodeId, Offset};
use helix_raft::multi::GroupMessage;
use helix_raft::{ClientRequest, Message, RaftNode};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, instrument, warn};

use crate::error::{ServerError, ServerResult};
use crate::storage::{
    BatchedBlob, BlobFormat,
    PartitionCommand as StoragePartitionCommand,
};

// =============================================================================
// Commands
// =============================================================================

/// Commands sent to a partition actor.
#[derive(Debug)]
pub enum PartitionCommand {
    /// Propose a new entry to the Raft log.
    Propose {
        /// The data to propose.
        data: Bytes,
        /// Channel to receive the result (offset on success).
        reply: oneshot::Sender<Result<ProposeResult, PartitionError>>,
    },

    /// Propose a batch of records with reply channels for commit notification.
    ProposeBatch {
        /// Command header (type, offsets, counts, per-blob headers).
        metadata: Bytes,
        /// Blob data (zero-copy reference from producer).
        payload: Bytes,
        /// Information needed to notify clients on commit.
        batch_info: BatchProposalInfo,
    },

    /// Periodic tick for Raft state machine (elections, heartbeats).
    Tick,

    /// Inbound Raft message from a peer.
    RaftMessage {
        /// The peer that sent the message.
        from: NodeId,
        /// The Raft message.
        message: Message,
    },

    /// Check if this node is the leader for this partition.
    IsLeader {
        /// Channel to receive the result.
        reply: oneshot::Sender<bool>,
    },

    /// Get the current leader ID for this partition.
    LeaderId {
        /// Channel to receive the result.
        reply: oneshot::Sender<Option<NodeId>>,
    },

    /// A produce request to be batched by the partition actor.
    ///
    /// Instead of routing through the centralized batcher, produce
    /// requests go directly to the partition actor for batching.
    ProduceRequest {
        /// Raw blob data (e.g., Kafka `RecordBatch` bytes).
        blob: Bytes,
        /// Number of records in the blob.
        record_count: u32,
        /// Format of the blob data.
        format: BlobFormat,
        /// Channel to send the result (offset) when committed.
        result_tx: oneshot::Sender<ServerResult<Offset>>,
        /// Size of the blob in bytes (for backpressure tracking).
        blob_size_bytes: u32,
    },

    /// Seed the actor's offset tracking after leadership change.
    SeedOffset {
        /// The current storage end offset to seed from.
        offset: Offset,
    },

    /// Provide entries read from the data WAL for a follower.
    ///
    /// Sent by the output processor after reading WAL entries in response
    /// to a `NeedWalEntries` output. The actor calls
    /// `raft_node.provide_entries()` with the supplied entries.
    ProvideEntries {
        /// The follower that needs entries.
        follower_id: NodeId,
        /// Index before the first provided entry.
        prev_log_index: LogIndex,
        /// Term at `prev_log_index`.
        prev_log_term: helix_core::TermId,
        /// Entries read from the WAL.
        entries: Vec<helix_raft::LogEntry>,
    },

    /// Graceful shutdown.
    Shutdown,
}

// =============================================================================
// Result / Error Types
// =============================================================================

/// Result of a successful propose operation.
#[derive(Debug, Clone)]
pub struct ProposeResult {
    /// The log index assigned to the entry.
    pub log_index: LogIndex,
}

/// Errors from partition actor operations.
#[derive(Debug, Clone)]
pub enum PartitionError {
    /// This node is not the leader for this partition.
    NotLeader {
        /// The known leader, if any.
        leader_id: Option<NodeId>,
    },
    /// The actor has shut down.
    ActorShutdown,
    /// The proposal was rejected.
    ProposalRejected(String),
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_id } => {
                if let Some(id) = leader_id {
                    write!(f, "not leader, leader is node {}", id.get())
                } else {
                    write!(f, "not leader, leader unknown")
                }
            }
            Self::ActorShutdown => {
                write!(f, "partition actor has shut down")
            }
            Self::ProposalRejected(reason) => {
                write!(f, "proposal rejected: {reason}")
            }
        }
    }
}

impl std::error::Error for PartitionError {}

// =============================================================================
// Batch Info Types
// =============================================================================

/// Information needed to notify batch clients on commit.
pub struct BatchProposalInfo {
    /// Timestamp when the first request entered the batch.
    pub first_request_at: std::time::Instant,
    /// Number of requests in this batch.
    pub batch_size: u32,
    /// Total bytes in this batch.
    pub batch_bytes: u32,
    /// Total records across all requests in this batch.
    pub total_records: u64,
    /// Record counts for each request in the batch.
    pub record_counts: Vec<u32>,
    /// Channels to notify each waiter with their assigned offset.
    pub result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
}

impl std::fmt::Debug for BatchProposalInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchProposalInfo")
            .field("batch_size", &self.batch_size)
            .field("batch_bytes", &self.batch_bytes)
            .field("total_records", &self.total_records)
            .field("record_counts", &self.record_counts)
            .field("result_txs_len", &self.result_txs.len())
            .finish_non_exhaustive()
    }
}

/// Internal state for a pending batch proposal.
struct BatchPendingInfo {
    first_request_at: std::time::Instant,
    proposed_at: std::time::Instant,
    batch_size: u32,
    batch_bytes: u32,
    total_records: u64,
    record_counts: Vec<u32>,
    result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
}

/// Information needed by output processor to notify batch clients.
pub struct BatchNotifyInfo {
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
    /// Record counts for each request in the batch.
    pub record_counts: Vec<u32>,
    /// Channels to notify each waiter with their assigned offset.
    pub result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
}

impl std::fmt::Debug for BatchNotifyInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchNotifyInfo")
            .field("batch_size", &self.batch_size)
            .field("batch_bytes", &self.batch_bytes)
            .field("total_records", &self.total_records)
            .field("record_counts", &self.record_counts)
            .field("result_txs_len", &self.result_txs.len())
            .finish_non_exhaustive()
    }
}

// =============================================================================
// Handle
// =============================================================================

/// Handle for sending commands to a partition actor.
#[derive(Clone)]
pub struct PartitionActorHandle {
    tx: mpsc::Sender<PartitionCommand>,
    group_id: GroupId,
    /// Cached leadership state updated by the actor.
    is_leader_cache: Arc<AtomicBool>,
    /// Cached blob end offset updated by the output processor on
    /// commit. Allows the batcher to read the offset without
    /// acquiring `partition_storage` locks.
    blob_end_offset_cache: Arc<AtomicU64>,
}

impl PartitionActorHandle {
    /// Creates a new handle.
    #[must_use]
    pub fn new(
        tx: mpsc::Sender<PartitionCommand>,
        group_id: GroupId,
    ) -> Self {
        Self {
            tx,
            group_id,
            is_leader_cache: Arc::new(AtomicBool::new(false)),
            blob_end_offset_cache: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the shared leadership cache for the actor to update.
    #[must_use]
    pub fn leader_cache(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_leader_cache)
    }

    /// Returns a best-effort cached leadership check (~1 ns).
    #[must_use]
    pub fn is_leader_cached(&self) -> bool {
        self.is_leader_cache.load(Ordering::Relaxed)
    }

    /// Returns the cached blob end offset (~1 ns, no lock).
    #[must_use]
    pub fn blob_end_offset_cached(&self) -> Offset {
        Offset::new(
            self.blob_end_offset_cache.load(Ordering::Acquire),
        )
    }

    /// Returns the shared blob end offset cache for the output
    /// processor to update after applying committed entries.
    #[must_use]
    pub fn blob_end_offset_arc(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.blob_end_offset_cache)
    }

    /// Returns the group ID for this partition.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Proposes a new entry to the Raft log.
    ///
    /// # Errors
    ///
    /// Returns an error if not leader, shut down, or rejected.
    pub async fn propose(
        &self,
        data: Bytes,
    ) -> Result<ProposeResult, PartitionError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PartitionCommand::Propose {
                data,
                reply: reply_tx,
            })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;
        reply_rx.await.map_err(|_| PartitionError::ActorShutdown)?
    }

    /// Proposes a batch with reply channels for commit notification.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn propose_batch(
        &self,
        metadata: Bytes,
        payload: Bytes,
        batch_info: BatchProposalInfo,
    ) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::ProposeBatch { metadata, payload, batch_info })
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }

    /// Submits a produce request for per-partition batching.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    ///
    /// # Panics
    ///
    /// Panics if the blob exceeds the 16 MB safety limit.
    pub async fn submit_produce(
        &self,
        blob: Bytes,
        record_count: u32,
        format: BlobFormat,
    ) -> Result<oneshot::Receiver<ServerResult<Offset>>, PartitionError>
    {
        #[allow(clippy::cast_possible_truncation)]
        let blob_size_bytes = blob.len() as u32;
        assert!(
            blob_size_bytes <= 16 * 1024 * 1024,
            "blob exceeds 16 MB safety limit"
        );
        let (result_tx, result_rx) = oneshot::channel();
        self.tx
            .send(PartitionCommand::ProduceRequest {
                blob,
                record_count,
                format,
                result_tx,
                blob_size_bytes,
            })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;
        Ok(result_rx)
    }

    /// Seeds the actor's offset tracking after leadership change.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn seed_offset(
        &self,
        offset: Offset,
    ) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::SeedOffset { offset })
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }

    /// Sends a tick to the partition actor.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn tick(&self) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::Tick)
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }

    /// Sends a Raft message to the partition actor.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn send_raft_message(
        &self,
        from: NodeId,
        message: Message,
    ) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::RaftMessage { from, message })
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }

    /// Checks if this node is the leader for this partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn is_leader(&self) -> Result<bool, PartitionError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PartitionCommand::IsLeader { reply: reply_tx })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;
        reply_rx.await.map_err(|_| PartitionError::ActorShutdown)
    }

    /// Gets the current leader ID for this partition.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn leader_id(
        &self,
    ) -> Result<Option<NodeId>, PartitionError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(PartitionCommand::LeaderId { reply: reply_tx })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;
        reply_rx.await.map_err(|_| PartitionError::ActorShutdown)
    }

    /// Provides WAL entries to the Raft node for serving to a follower.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down.
    pub async fn provide_entries(
        &self,
        follower_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: helix_core::TermId,
        entries: Vec<helix_raft::LogEntry>,
    ) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::ProvideEntries {
                follower_id,
                prev_log_index,
                prev_log_term,
                entries,
            })
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }

    /// Shuts down the partition actor.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has already shut down.
    pub async fn shutdown(&self) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::Shutdown)
            .await
            .map_err(|_| PartitionError::ActorShutdown)
    }
}

impl std::fmt::Debug for PartitionActorHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionActorHandle")
            .field("group_id", &self.group_id)
            .finish_non_exhaustive()
    }
}

// =============================================================================
// Output Types
// =============================================================================

/// Output from processing a partition command.
#[derive(Debug)]
pub enum PartitionOutput {
    /// Send messages to peers via transport.
    SendMessages {
        /// Destination node.
        to: NodeId,
        /// Messages to send.
        messages: Vec<GroupMessage>,
    },
    /// An entry was committed and needs to be applied.
    EntryCommitted {
        /// Log index of the committed entry.
        index: LogIndex,
        /// Raft term of the committed entry.
        term: helix_core::TermId,
        /// Command header.
        metadata: Bytes,
        /// Blob payload (empty for non-blob commands).
        payload: Bytes,
        /// Optional batch notification info.
        batch_notify: Option<BatchNotifyInfo>,
    },
    /// This node became leader.
    BecameLeader,
    /// This node stepped down from leader.
    SteppedDown,
    /// Vote state changed (needs persistence).
    VoteStateChanged {
        /// New term.
        term: u64,
        /// Who we voted for.
        voted_for: Option<NodeId>,
    },
    /// Leader needs entries from the data WAL for a follower.
    ///
    /// The output processor reads entries from the WAL and sends
    /// a `ProvideEntries` command back to the actor.
    NeedWalEntries {
        /// The follower that needs entries.
        follower_id: NodeId,
        /// First entry index to read.
        start_index: LogIndex,
        /// Index before `start_index` (for `prev_log_term` lookup).
        prev_log_index: LogIndex,
        /// Maximum bytes to read.
        max_bytes: u64,
    },
}

// =============================================================================
// Config
// =============================================================================

/// Configuration for a partition actor.
#[derive(Debug, Clone, Copy)]
pub struct PartitionActorConfig {
    /// Command channel buffer size.
    pub channel_buffer_size: usize,
}

impl Default for PartitionActorConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 1000,
        }
    }
}

/// Configuration for per-partition batching.
#[derive(Debug, Clone, Copy)]
pub struct PartitionBatchConfig {
    /// Maximum time to wait before flushing (milliseconds).
    pub linger_ms: u64,
    /// Maximum total bytes in a batch before forcing flush.
    pub max_batch_bytes: u32,
    /// Maximum number of requests in a batch.
    pub max_batch_requests: u32,
    /// Maximum pending bytes per partition for backpressure.
    pub max_pending_bytes: u32,
    /// Maximum pending requests per partition for backpressure.
    pub max_pending_requests: u32,
}

impl Default for PartitionBatchConfig {
    fn default() -> Self {
        Self {
            linger_ms: 1,
            max_batch_bytes: 4 * 1024 * 1024,
            max_batch_requests: 1000,
            max_pending_bytes: 25 * 1024 * 1024,
            max_pending_requests: 500,
        }
    }
}

// =============================================================================
// Spawn Functions
// =============================================================================

/// Creates a new partition actor and returns a handle.
#[must_use]
pub fn spawn_partition_actor(
    group_id: GroupId,
    raft_node: RaftNode,
    config: PartitionActorConfig,
) -> (PartitionActorHandle, mpsc::Receiver<PartitionOutput>) {
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_buffer_size);
    let (output_tx, output_rx) =
        mpsc::channel(config.channel_buffer_size);

    let actor = PartitionActor {
        group_id,
        raft_node,
        cmd_rx,
        output_tx,
        pending_proposals: HashMap::new(),
    };
    tokio::spawn(actor.run());

    let handle = PartitionActorHandle::new(cmd_tx, group_id);
    (handle, output_rx)
}

/// Output with group ID for shared channel aggregation.
#[derive(Debug)]
pub struct GroupedOutput {
    /// The group/partition that produced this output.
    pub group_id: GroupId,
    /// The output.
    pub output: PartitionOutput,
}

/// Spawns a partition actor with a shared output channel.
#[must_use]
pub fn spawn_partition_actor_shared(
    group_id: GroupId,
    raft_node: RaftNode,
    config: PartitionActorConfig,
    shared_output_tx: mpsc::Sender<GroupedOutput>,
) -> PartitionActorHandle {
    spawn_partition_actor_shared_with_batch_config(
        group_id,
        raft_node,
        config,
        shared_output_tx,
        PartitionBatchConfig::default(),
        None,
        None,
    )
}

/// Spawns a partition actor with batch config and global stats.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn spawn_partition_actor_shared_with_batch_config(
    group_id: GroupId,
    raft_node: RaftNode,
    config: PartitionActorConfig,
    shared_output_tx: mpsc::Sender<GroupedOutput>,
    batch_config: PartitionBatchConfig,
    batcher_stats: Option<Arc<super::BatcherStats>>,
    global_backpressure: Option<Arc<super::batcher::BackpressureState>>,
) -> PartitionActorHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_buffer_size);
    let handle = PartitionActorHandle::new(cmd_tx, group_id);
    let is_leader_cache = handle.leader_cache();

    let backpressure = PartitionBackpressure::new(
        batch_config.max_pending_bytes,
        batch_config.max_pending_requests,
    );

    let actor = PartitionActorShared {
        group_id,
        raft_node,
        cmd_rx,
        output_tx: shared_output_tx,
        pending_proposals: HashMap::new(),
        batch_pending_proposals: HashMap::new(),
        pending_batch: AccumulatedBatch::new(),
        batch_config,
        backpressure,
        next_base_offset: Offset::new(0),
        offset_seeded: false,
        is_leader_cache,
        batcher_stats,
        global_backpressure,
    };
    tokio::spawn(actor.run());

    handle
}

// =============================================================================
// Per-Partition Batching Internal Types
// =============================================================================

#[derive(Debug, Clone, Copy)]
enum FlushReason {
    Linger,
    Size,
    Shutdown,
}

impl FlushReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Linger => "linger",
            Self::Size => "size",
            Self::Shutdown => "shutdown",
        }
    }
}

struct AccumulatedBatch {
    blobs: Vec<BatchedBlob>,
    record_counts: Vec<u32>,
    result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
    total_bytes: u32,
    first_request_time: Instant,
}

impl AccumulatedBatch {
    fn new() -> Self {
        Self {
            blobs: Vec::with_capacity(100),
            record_counts: Vec::with_capacity(100),
            result_txs: Vec::with_capacity(100),
            total_bytes: 0,
            first_request_time: Instant::now(),
        }
    }

    fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    fn request_count(&self) -> u32 {
        #[allow(clippy::cast_possible_truncation)]
        let count = self.blobs.len() as u32;
        count
    }
}

struct PartitionBackpressure {
    pending_bytes: u32,
    pending_requests: u32,
    max_pending_bytes: u32,
    max_pending_requests: u32,
}

impl PartitionBackpressure {
    const fn new(max_bytes: u32, max_requests: u32) -> Self {
        Self {
            pending_bytes: 0,
            pending_requests: 0,
            max_pending_bytes: max_bytes,
            max_pending_requests: max_requests,
        }
    }

    const fn should_reject(&self, blob_size_bytes: u32) -> bool {
        self.pending_requests >= self.max_pending_requests
            || self.pending_bytes + blob_size_bytes
                > self.max_pending_bytes
    }

    const fn add(&mut self, blob_size_bytes: u32) {
        self.pending_bytes =
            self.pending_bytes.saturating_add(blob_size_bytes);
        self.pending_requests = self.pending_requests.saturating_add(1);
    }

    const fn subtract_batch(&mut self, bytes: u32, count: u32) {
        self.pending_bytes = self.pending_bytes.saturating_sub(bytes);
        self.pending_requests =
            self.pending_requests.saturating_sub(count);
    }
}

// =============================================================================
// Shared Partition Actor (production path)
// =============================================================================

struct PartitionActorShared {
    group_id: GroupId,
    raft_node: RaftNode,
    cmd_rx: mpsc::Receiver<PartitionCommand>,
    output_tx: mpsc::Sender<GroupedOutput>,
    pending_proposals: HashMap<
        LogIndex,
        oneshot::Sender<Result<ProposeResult, PartitionError>>,
    >,
    batch_pending_proposals: HashMap<LogIndex, BatchPendingInfo>,
    pending_batch: AccumulatedBatch,
    batch_config: PartitionBatchConfig,
    backpressure: PartitionBackpressure,
    next_base_offset: Offset,
    offset_seeded: bool,
    is_leader_cache: Arc<AtomicBool>,
    batcher_stats: Option<Arc<super::BatcherStats>>,
    #[allow(dead_code)]
    global_backpressure:
        Option<Arc<super::batcher::BackpressureState>>,
}

impl PartitionActorShared {
    /// Batch drain limit for `recv_many()`.
    ///
    /// Caps how many commands are drained per `select!` iteration to
    /// bound latency before rechecking the linger timer. At ~200 ns per
    /// command, 1024 messages take ~200 μs — small vs the 5 ms linger.
    const DRAIN_BATCH_LIMIT: usize = 1024;

    #[instrument(skip(self), fields(group_id = self.group_id.get()))]
    #[allow(clippy::too_many_lines)]
    async fn run(mut self) {
        info!("Partition actor (shared output) started");
        let linger_dur =
            Duration::from_millis(self.batch_config.linger_ms);
        let mut cmd_buf =
            Vec::with_capacity(Self::DRAIN_BATCH_LIMIT);

        // Pin the linger sleep so it persists across select! iterations.
        // Without pinning, the sleep future is recreated each iteration
        // and never completes under load (recv_many returns immediately
        // when the channel has messages, so the loop spins faster than
        // the linger period).
        let linger_sleep =
            tokio::time::sleep(Duration::from_secs(86400));
        tokio::pin!(linger_sleep);
        let mut linger_active = false;

        loop {
            // Arm the linger timer when the first request arrives.
            if !linger_active && !self.pending_batch.is_empty() {
                linger_sleep.as_mut().reset(
                    tokio::time::Instant::now() + linger_dur,
                );
                linger_active = true;
            }

            tokio::select! {
                // Bias: check linger first so it fires even when
                // the command channel is saturated.
                biased;
                () = &mut linger_sleep, if linger_active => {
                    self.flush_pending_batch(FlushReason::Linger).await;
                    linger_active = false;
                }
                // Drain all available commands in one async op.
                // recv_many blocks until ≥1 message, then drains up
                // to DRAIN_BATCH_LIMIT without re-entering select!.
                n = self.cmd_rx.recv_many(
                    &mut cmd_buf,
                    Self::DRAIN_BATCH_LIMIT,
                ) => {
                    if n == 0 { break; } // channel closed
                    // drain(..) reuses the Vec allocation across
                    // iterations; into_iter() would deallocate.
                    #[allow(clippy::iter_with_drain)]
                    for cmd in cmd_buf.drain(..) {
                        self.handle_command(cmd).await;
                    }
                }
            }
        }

        self.shutdown_cleanup().await;
        self.is_leader_cache.store(false, Ordering::Relaxed);
        info!("Partition actor (shared output) stopped");
    }

    async fn handle_command(&mut self, cmd: PartitionCommand) {
        match cmd {
            PartitionCommand::Propose { data, reply } => {
                self.handle_propose(data, reply).await;
            }
            PartitionCommand::ProposeBatch { metadata, payload, batch_info } => {
                self.handle_propose_batch(metadata, payload, batch_info).await;
            }
            PartitionCommand::ProduceRequest {
                blob,
                record_count,
                format,
                result_tx,
                blob_size_bytes,
            } => {
                self.handle_produce_request(
                    blob,
                    record_count,
                    format,
                    result_tx,
                    blob_size_bytes,
                )
                .await;
            }
            PartitionCommand::SeedOffset { offset } => {
                self.handle_seed_offset(offset);
            }
            PartitionCommand::Tick => {
                self.handle_tick().await;
            }
            PartitionCommand::RaftMessage { from, message } => {
                self.handle_raft_message(from, message).await;
            }
            PartitionCommand::IsLeader { reply } => {
                let _ = reply.send(self.raft_node.is_leader());
            }
            PartitionCommand::LeaderId { reply } => {
                let _ = reply.send(self.raft_node.leader_id());
            }
            PartitionCommand::ProvideEntries {
                follower_id,
                prev_log_index,
                prev_log_term,
                entries,
            } => {
                let outputs = self.raft_node.provide_entries(
                    follower_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                );
                self.process_raft_outputs(outputs).await;
            }
            PartitionCommand::Shutdown => {
                info!("Partition actor (shared) shutting down");
                self.flush_pending_batch(FlushReason::Shutdown).await;
                // Caller breaks from loop after this returns.
                // We signal shutdown by dropping cmd_rx in cleanup.
            }
        }
    }

    #[allow(clippy::unused_async)]
    async fn shutdown_cleanup(&mut self) {
        for (_, reply) in self.pending_proposals.drain() {
            let _ = reply.send(Err(PartitionError::ActorShutdown));
        }
        let err = ServerError::Internal {
            message: "partition actor shut down".to_string(),
        };
        for (_, bi) in self.batch_pending_proposals.drain() {
            for result_tx in bi.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
        }
        if !self.pending_batch.is_empty() {
            let batch = std::mem::replace(
                &mut self.pending_batch,
                AccumulatedBatch::new(),
            );
            for result_tx in batch.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
        }
    }

    async fn handle_propose_batch(
        &mut self,
        metadata: Bytes,
        payload: Bytes,
        batch_info: BatchProposalInfo,
    ) {
        if !self.raft_node.is_leader() {
            let err = ServerError::NotLeader {
                topic: "unknown".to_string(),
                partition: 0,
                leader_hint: self
                    .raft_node
                    .leader_id()
                    .map(NodeId::get),
            };
            for result_tx in batch_info.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
            return;
        }

        // Patch base_offset in metadata to ensure monotonicity. The batcher
        // reads blob_log_end_offset (committed only), so concurrent
        // batches can get the same stale offset. The actor's
        // next_base_offset accounts for uncommitted proposals.
        let metadata = self.patch_batch_base_offset(metadata, &batch_info);

        let request = ClientRequest::new(metadata, payload);
        if let Some(outputs) =
            self.raft_node.handle_client_request(request)
        {
            let log_index =
                LogIndex::new(self.raft_node.log().last_index().get());
            let pending_info = BatchPendingInfo {
                first_request_at: batch_info.first_request_at,
                proposed_at: Instant::now(),
                batch_size: batch_info.batch_size,
                batch_bytes: batch_info.batch_bytes,
                total_records: batch_info.total_records,
                record_counts: batch_info.record_counts,
                result_txs: batch_info.result_txs,
            };
            self.batch_pending_proposals
                .insert(log_index, pending_info);
            info!(
                group_id = self.group_id.get(),
                log_index = log_index.get(),
                batch_size = batch_info.batch_size,
                "Batch proposed to Raft (actor mode)"
            );
            self.process_raft_outputs(outputs).await;
        } else {
            let err = ServerError::Internal {
                message: "Raft rejected proposal".to_string(),
            };
            for result_tx in batch_info.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
        }
    }

    /// Patches `base_offset` in an `AppendBlobBatch` command to
    /// ensure monotonically increasing offsets across concurrent
    /// proposals.
    ///
    /// The batcher reads `blob_log_end_offset` (committed only) as
    /// its suggested offset. When two batches flush before the first
    /// commits, both get the same stale offset. This method uses the
    /// actor's `next_base_offset` — which accounts for uncommitted
    /// proposals — to guarantee monotonicity.
    fn patch_batch_base_offset(
        &mut self,
        data: Bytes,
        batch_info: &BatchProposalInfo,
    ) -> Bytes {
        const APPEND_BLOB_BATCH_TYPE: u8 = 4;
        // type (1 byte) + base_offset (8 bytes).
        const HEADER_LEN: usize = 9;

        if data.len() < HEADER_LEN
            || data[0] != APPEND_BLOB_BATCH_TYPE
        {
            return data;
        }

        let batcher_offset = Offset::new(u64::from_le_bytes(
            data[1..9]
                .try_into()
                .expect("base_offset slice is 8 bytes"),
        ));

        // Seed from batcher on first propose after leadership change.
        // Use max to avoid overwriting a higher next_base_offset that
        // was advanced by committed PREVIOUS_TERM entries.
        if !self.offset_seeded {
            self.next_base_offset = std::cmp::max(
                self.next_base_offset,
                batcher_offset,
            );
            self.offset_seeded = true;
        }

        let actual_offset =
            std::cmp::max(self.next_base_offset, batcher_offset);

        // Advance tracker past this batch's records.
        self.next_base_offset =
            Offset::new(actual_offset.get() + batch_info.total_records);

        if actual_offset == batcher_offset {
            return data;
        }

        // Patch bytes 1..9 with the corrected offset.
        debug!(
            group_id = self.group_id.get(),
            batcher_offset = batcher_offset.get(),
            actual_offset = actual_offset.get(),
            "Patching stale base_offset in ProposeBatch"
        );
        let mut buf = BytesMut::from(&data[..]);
        buf[1..9]
            .copy_from_slice(&actual_offset.get().to_le_bytes());
        buf.freeze()
    }

    async fn handle_propose(
        &mut self,
        data: Bytes,
        reply: oneshot::Sender<Result<ProposeResult, PartitionError>>,
    ) {
        if !self.raft_node.is_leader() {
            let _ = reply.send(Err(PartitionError::NotLeader {
                leader_id: self.raft_node.leader_id(),
            }));
            return;
        }
        let request = ClientRequest::new(data, Bytes::new());
        match self.raft_node.handle_client_request(request) {
            Some(outputs) => {
                let log_index = LogIndex::new(
                    self.raft_node.log().last_index().get(),
                );
                self.pending_proposals.insert(log_index, reply);
                self.process_raft_outputs(outputs).await;
            }
            None => {
                let _ = reply.send(Err(
                    PartitionError::ProposalRejected(
                        "Raft rejected proposal".to_string(),
                    ),
                ));
            }
        }
    }

    async fn handle_tick(&mut self) {
        let outputs = self.raft_node.tick();
        self.process_raft_outputs(outputs).await;
    }

    async fn handle_raft_message(
        &mut self,
        _from: NodeId,
        message: Message,
    ) {
        let outputs = self.raft_node.handle_message(message);
        self.process_raft_outputs(outputs).await;
    }

    // Per-partition batching methods

    async fn handle_produce_request(
        &mut self,
        blob: Bytes,
        record_count: u32,
        format: BlobFormat,
        result_tx: oneshot::Sender<ServerResult<Offset>>,
        blob_size_bytes: u32,
    ) {
        if self.backpressure.should_reject(blob_size_bytes) {
            debug!(
                group_id = self.group_id.get(),
                "Rejecting produce: backpressure"
            );
            let _ = result_tx.send(Err(ServerError::Overloaded {
                pending_requests: u64::from(
                    self.backpressure.pending_requests,
                ),
                pending_bytes: u64::from(
                    self.backpressure.pending_bytes,
                ),
            }));
            return;
        }
        self.backpressure.add(blob_size_bytes);

        let would_exceed_bytes = self.pending_batch.total_bytes
            + blob_size_bytes
            > self.batch_config.max_batch_bytes;
        let would_exceed_count = self.pending_batch.request_count()
            >= self.batch_config.max_batch_requests;
        if !self.pending_batch.is_empty()
            && (would_exceed_bytes || would_exceed_count)
        {
            self.flush_pending_batch(FlushReason::Size).await;
        }

        if self.pending_batch.is_empty() {
            self.pending_batch.first_request_time = Instant::now();
        }
        self.pending_batch.blobs.push(BatchedBlob {
            blob,
            record_count,
            format,
        });
        self.pending_batch.record_counts.push(record_count);
        self.pending_batch.result_txs.push(result_tx);
        self.pending_batch.total_bytes += blob_size_bytes;
    }

    #[allow(clippy::too_many_lines)]
    async fn flush_pending_batch(&mut self, reason: FlushReason) {
        if self.pending_batch.is_empty() {
            return;
        }
        let batch = std::mem::replace(
            &mut self.pending_batch,
            AccumulatedBatch::new(),
        );
        let batch_size = batch.request_count();
        let batch_bytes = batch.total_bytes;
        let batch_records: u64 = batch
            .record_counts
            .iter()
            .map(|c| u64::from(*c))
            .sum();

        if let Some(ref stats) = self.batcher_stats {
            #[allow(clippy::cast_possible_truncation)]
            let age_us = batch
                .first_request_time
                .elapsed()
                .as_micros() as u64;
            stats.record_flush(
                reason.as_str(),
                u64::from(batch_size),
                u64::from(batch_bytes),
                batch_records,
                age_us,
            );
        }

        if !self.raft_node.is_leader() {
            let err = ServerError::NotLeader {
                topic: "unknown".to_string(),
                partition: 0,
                leader_hint: self
                    .raft_node
                    .leader_id()
                    .map(NodeId::get),
            };
            for result_tx in batch.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
            self.backpressure
                .subtract_batch(batch_bytes, batch_size);
            return;
        }

        if !self.offset_seeded {
            warn!(
                group_id = self.group_id.get(),
                "Rejecting batch: offset not yet seeded"
            );
            let err = ServerError::Internal {
                message: "offset not yet seeded".to_string(),
            };
            for result_tx in batch.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
            self.backpressure
                .subtract_batch(batch_bytes, batch_size);
            return;
        }

        let base_offset = self.next_base_offset;
        let command = StoragePartitionCommand::AppendBlobBatch {
            blobs: batch.blobs,
            base_offset,
        };
        let command_data = command.encode();
        let request = ClientRequest::new(command_data, Bytes::new());
        let Some(outputs) =
            self.raft_node.handle_client_request(request)
        else {
            let err = ServerError::Internal {
                message: "Raft rejected proposal".to_string(),
            };
            for result_tx in batch.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
            self.backpressure
                .subtract_batch(batch_bytes, batch_size);
            return;
        };

        let log_index =
            LogIndex::new(self.raft_node.log().last_index().get());
        self.next_base_offset =
            Offset::new(base_offset.get() + batch_records);

        let pending_info = BatchPendingInfo {
            first_request_at: batch.first_request_time,
            proposed_at: Instant::now(),
            batch_size,
            batch_bytes,
            total_records: batch_records,
            record_counts: batch.record_counts,
            result_txs: batch.result_txs,
        };
        self.batch_pending_proposals
            .insert(log_index, pending_info);

        debug!(
            group_id = self.group_id.get(),
            log_index = log_index.get(),
            batch_size,
            base_offset = base_offset.get(),
            batch_records,
            reason = reason.as_str(),
            "Batch proposed (per-partition batching)"
        );
        self.process_raft_outputs(outputs).await;
    }

    fn handle_seed_offset(&mut self, offset: Offset) {
        info!(
            group_id = self.group_id.get(),
            offset = offset.get(),
            previous = self.next_base_offset.get(),
            "Seeding next_base_offset"
        );
        self.next_base_offset = offset;
        self.offset_seeded = true;
    }

    /// Advances `next_base_offset` from a committed entry's data.
    ///
    /// After a leadership change, `PREVIOUS_TERM` entries are committed
    /// before any new proposals arrive. Without tracking their offsets,
    /// the first new proposal would reuse `base_offset=0`, violating
    /// `BlobIndex` monotonicity.
    fn advance_offset_from_committed(&mut self, data: &Bytes) {
        if data.is_empty() {
            return;
        }
        // Use the canonical decoder to handle all encode formats.
        let Some(cmd) = StoragePartitionCommand::decode(data) else {
            return;
        };
        let end_offset = match &cmd {
            StoragePartitionCommand::AppendBlob {
                base_offset,
                record_count,
                ..
            } => Offset::new(
                base_offset.get() + u64::from(*record_count),
            ),
            StoragePartitionCommand::AppendBlobBatch {
                blobs,
                base_offset,
            } => {
                let total: u64 = blobs
                    .iter()
                    .map(|b| u64::from(b.record_count))
                    .sum();
                Offset::new(base_offset.get() + total)
            }
            _ => return,
        };
        if end_offset > self.next_base_offset {
            self.next_base_offset = end_offset;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn process_raft_outputs(
        &mut self,
        outputs: Vec<helix_raft::RaftOutput>,
    ) {
        use helix_raft::RaftOutput;
        for output in outputs {
            match output {
                RaftOutput::SendMessage(message) => {
                    let to = message.to();
                    let gm = GroupMessage {
                        group_id: self.group_id,
                        message,
                    };
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::SendMessages {
                            to,
                            messages: vec![gm],
                        },
                    };
                    if self.output_tx.send(grouped).await.is_err() {
                        warn!("Failed to send message output");
                    }
                }
                RaftOutput::CommitEntry { index, term, metadata, payload } => {
                    // Advance next_base_offset from committed entries.
                    // Reconstitute full data for decode (encode_split
                    // metadata alone is incomplete).
                    {
                        let data = if payload.is_empty() {
                            metadata.clone()
                        } else {
                            let mut buf = BytesMut::with_capacity(
                                metadata.len() + payload.len(),
                            );
                            buf.extend_from_slice(&metadata);
                            buf.extend_from_slice(&payload);
                            buf.freeze()
                        };
                        self.advance_offset_from_committed(&data);
                    }

                    if let Some(reply) =
                        self.pending_proposals.remove(&index)
                    {
                        let _ = reply.send(Ok(ProposeResult {
                            log_index: index,
                        }));
                    }
                    let has_batch = self
                        .batch_pending_proposals
                        .contains_key(&index);
                    let batch_notify = self
                        .batch_pending_proposals
                        .remove(&index)
                        .map(|p| BatchNotifyInfo {
                            first_request_at: p.first_request_at,
                            proposed_at: p.proposed_at,
                            batch_size: p.batch_size,
                            batch_bytes: p.batch_bytes,
                            total_records: p.total_records,
                            record_counts: p.record_counts,
                            result_txs: p.result_txs,
                        });
                    info!(
                        group_id = self.group_id.get(),
                        index = index.get(),
                        has_batch,
                        batch_notify_some = batch_notify.is_some(),
                        "CommitEntry in partition actor"
                    );
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::EntryCommitted {
                            index,
                            term,
                            metadata,
                            payload,
                            batch_notify,
                        },
                    };
                    if self.output_tx.send(grouped).await.is_err() {
                        warn!("Failed to send committed entry");
                    }
                }
                RaftOutput::BecameLeader => {
                    info!(
                        group_id = self.group_id.get(),
                        "Became leader"
                    );
                    self.is_leader_cache
                        .store(true, Ordering::Relaxed);
                    self.offset_seeded = false;
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::BecameLeader,
                    };
                    let _ = self.output_tx.send(grouped).await;
                }
                RaftOutput::SteppedDown => {
                    info!(
                        group_id = self.group_id.get(),
                        "Stepped down from leader"
                    );
                    self.is_leader_cache
                        .store(false, Ordering::Relaxed);
                    self.offset_seeded = false;
                    for (_, reply) in self.pending_proposals.drain() {
                        let _ =
                            reply.send(Err(PartitionError::NotLeader {
                                leader_id: self
                                    .raft_node
                                    .leader_id(),
                            }));
                    }
                    let err = ServerError::NotLeader {
                        topic: "unknown".to_string(),
                        partition: 0,
                        leader_hint: self
                            .raft_node
                            .leader_id()
                            .map(NodeId::get),
                    };
                    for (_, bi) in
                        self.batch_pending_proposals.drain()
                    {
                        for result_tx in bi.result_txs {
                            let _ =
                                result_tx.send(Err(err.clone()));
                        }
                    }
                    if !self.pending_batch.is_empty() {
                        let batch = std::mem::replace(
                            &mut self.pending_batch,
                            AccumulatedBatch::new(),
                        );
                        let bb = batch.total_bytes;
                        let bc = batch.request_count();
                        for result_tx in batch.result_txs {
                            let _ =
                                result_tx.send(Err(err.clone()));
                        }
                        self.backpressure.subtract_batch(bb, bc);
                    }
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::SteppedDown,
                    };
                    let _ = self.output_tx.send(grouped).await;
                }
                RaftOutput::VoteStateChanged { term, voted_for } => {
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::VoteStateChanged {
                            term: term.get(),
                            voted_for,
                        },
                    };
                    let _ = self.output_tx.send(grouped).await;
                }
                RaftOutput::NeedEntries {
                    follower_id,
                    start_index,
                    prev_log_index,
                    max_bytes,
                } => {
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::NeedWalEntries {
                            follower_id,
                            start_index,
                            prev_log_index,
                            max_bytes,
                        },
                    };
                    if self.output_tx.send(grouped).await.is_err() {
                        warn!("Failed to send NeedWalEntries output");
                    }
                }
            }
        }

        // Compact the Raft log after processing outputs to bound memory.
        if self.raft_node.is_leader() {
            self.raft_node.compact_log();
        } else {
            self.raft_node.compact_log_follower();
        }
    }
}

// =============================================================================
// Non-Shared Partition Actor (testing/simple path)
// =============================================================================

struct PartitionActor {
    group_id: GroupId,
    raft_node: RaftNode,
    cmd_rx: mpsc::Receiver<PartitionCommand>,
    output_tx: mpsc::Sender<PartitionOutput>,
    pending_proposals: HashMap<
        LogIndex,
        oneshot::Sender<Result<ProposeResult, PartitionError>>,
    >,
}

impl PartitionActor {
    #[instrument(skip(self), fields(group_id = self.group_id.get()))]
    async fn run(mut self) {
        info!("Partition actor started");
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                PartitionCommand::Propose { data, reply } => {
                    self.handle_propose(data, reply).await;
                }
                PartitionCommand::ProposeBatch { batch_info, .. } => {
                    let err = ServerError::Internal {
                        message: "ProposeBatch not supported"
                            .to_string(),
                    };
                    for result_tx in batch_info.result_txs {
                        let _ = result_tx.send(Err(err.clone()));
                    }
                }
                PartitionCommand::ProduceRequest {
                    result_tx, ..
                } => {
                    let _ = result_tx.send(Err(ServerError::Internal {
                        message: "ProduceRequest not supported"
                            .to_string(),
                    }));
                }
                PartitionCommand::SeedOffset { .. } => {}
                PartitionCommand::Tick => {
                    self.handle_tick().await;
                }
                PartitionCommand::RaftMessage { from, message } => {
                    self.handle_raft_message(from, message).await;
                }
                PartitionCommand::IsLeader { reply } => {
                    let _ = reply.send(self.raft_node.is_leader());
                }
                PartitionCommand::LeaderId { reply } => {
                    let _ = reply.send(self.raft_node.leader_id());
                }
                PartitionCommand::ProvideEntries {
                    follower_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                } => {
                    let outputs = self.raft_node.provide_entries(
                        follower_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                    );
                    self.process_raft_outputs(outputs).await;
                }
                PartitionCommand::Shutdown => {
                    info!("Partition actor shutting down");
                    break;
                }
            }
        }
        for (_, reply) in self.pending_proposals.drain() {
            let _ = reply.send(Err(PartitionError::ActorShutdown));
        }
        info!("Partition actor stopped");
    }

    async fn handle_propose(
        &mut self,
        data: Bytes,
        reply: oneshot::Sender<Result<ProposeResult, PartitionError>>,
    ) {
        if !self.raft_node.is_leader() {
            let _ = reply.send(Err(PartitionError::NotLeader {
                leader_id: self.raft_node.leader_id(),
            }));
            return;
        }
        let request = ClientRequest::new(data, Bytes::new());
        match self.raft_node.handle_client_request(request) {
            Some(outputs) => {
                let log_index = LogIndex::new(
                    self.raft_node.log().last_index().get(),
                );
                self.pending_proposals.insert(log_index, reply);
                self.process_raft_outputs(outputs).await;
            }
            None => {
                let _ = reply.send(Err(
                    PartitionError::ProposalRejected(
                        "Raft rejected proposal".to_string(),
                    ),
                ));
            }
        }
    }

    async fn handle_tick(&mut self) {
        let outputs = self.raft_node.tick();
        self.process_raft_outputs(outputs).await;
    }

    async fn handle_raft_message(
        &mut self,
        _from: NodeId,
        message: Message,
    ) {
        let outputs = self.raft_node.handle_message(message);
        self.process_raft_outputs(outputs).await;
    }

    async fn process_raft_outputs(
        &mut self,
        outputs: Vec<helix_raft::RaftOutput>,
    ) {
        use helix_raft::RaftOutput;
        for output in outputs {
            match output {
                RaftOutput::SendMessage(message) => {
                    let to = message.to();
                    let gm = GroupMessage {
                        group_id: self.group_id,
                        message,
                    };
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::SendMessages {
                            to,
                            messages: vec![gm],
                        })
                        .await;
                }
                RaftOutput::CommitEntry { index, term, metadata, payload } => {
                    if let Some(reply) =
                        self.pending_proposals.remove(&index)
                    {
                        let _ = reply.send(Ok(ProposeResult {
                            log_index: index,
                        }));
                    }
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::EntryCommitted {
                            index,
                            term,
                            metadata,
                            payload,
                            batch_notify: None,
                        })
                        .await;
                }
                RaftOutput::BecameLeader => {
                    info!(
                        group_id = self.group_id.get(),
                        "Became leader"
                    );
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::BecameLeader)
                        .await;
                }
                RaftOutput::SteppedDown => {
                    info!(
                        group_id = self.group_id.get(),
                        "Stepped down from leader"
                    );
                    for (_, reply) in self.pending_proposals.drain() {
                        let _ =
                            reply.send(Err(PartitionError::NotLeader {
                                leader_id: self
                                    .raft_node
                                    .leader_id(),
                            }));
                    }
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::SteppedDown)
                        .await;
                }
                RaftOutput::VoteStateChanged { term, voted_for } => {
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::VoteStateChanged {
                            term: term.get(),
                            voted_for,
                        })
                        .await;
                }
                RaftOutput::NeedEntries {
                    follower_id,
                    start_index,
                    prev_log_index,
                    max_bytes,
                } => {
                    // Non-shared actor: forward as output for processing.
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::NeedWalEntries {
                            follower_id,
                            start_index,
                            prev_log_index,
                            max_bytes,
                        })
                        .await;
                }
            }
        }

        // Compact the Raft log after processing outputs to bound memory.
        if self.raft_node.is_leader() {
            self.raft_node.compact_log();
        } else {
            self.raft_node.compact_log_follower();
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use helix_core::NodeId;
    use helix_raft::RaftConfig;

    fn create_test_raft_node(
        node_id: u64,
        cluster: Vec<u64>,
    ) -> RaftNode {
        let config = RaftConfig::new(
            NodeId::new(node_id),
            cluster.into_iter().map(NodeId::new).collect(),
        );
        RaftNode::new(config)
    }

    #[tokio::test]
    async fn test_partition_actor_startup_shutdown() {
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, _output_rx) = spawn_partition_actor(
            GroupId::new(1),
            raft_node,
            PartitionActorConfig::default(),
        );
        assert!(handle.is_leader().await.is_ok());
        assert!(handle.shutdown().await.is_ok());

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(matches!(
            handle.is_leader().await,
            Err(PartitionError::ActorShutdown)
        ));
    }

    #[tokio::test]
    async fn test_partition_actor_not_leader() {
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, _output_rx) = spawn_partition_actor(
            GroupId::new(1),
            raft_node,
            PartitionActorConfig::default(),
        );
        let is_leader = handle.is_leader().await.unwrap();
        assert!(!is_leader);

        let result = handle.propose(Bytes::from("test")).await;
        assert!(matches!(
            result,
            Err(PartitionError::NotLeader { .. })
        ));
        handle.shutdown().await.ok();
    }

    #[tokio::test]
    async fn test_partition_actor_tick() {
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, mut output_rx) = spawn_partition_actor(
            GroupId::new(1),
            raft_node,
            PartitionActorConfig::default(),
        );
        for _ in 0..20 {
            handle.tick().await.unwrap();
        }
        let output = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            output_rx.recv(),
        )
        .await;
        drop(output);
        handle.shutdown().await.ok();
    }

    #[tokio::test]
    async fn test_partition_actor_handle_clone() {
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, _output_rx) = spawn_partition_actor(
            GroupId::new(1),
            raft_node,
            PartitionActorConfig::default(),
        );
        let handle2 = handle.clone();
        assert!(handle.is_leader().await.is_ok());
        assert!(handle2.is_leader().await.is_ok());
        handle.shutdown().await.ok();
    }

    #[tokio::test]
    async fn test_partition_actor_is_leader_cached() {
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, _output_rx) = spawn_partition_actor(
            GroupId::new(1),
            raft_node,
            PartitionActorConfig::default(),
        );
        assert!(!handle.is_leader_cached());
        handle.shutdown().await.ok();
    }

    /// Helper to build a `PartitionActorShared` for unit-testing
    /// `patch_batch_base_offset` without spawning a full actor loop.
    fn create_test_shared_actor(
        next_base_offset: Offset,
        offset_seeded: bool,
    ) -> (
        PartitionActorShared,
        mpsc::Receiver<GroupedOutput>,
        mpsc::Sender<PartitionCommand>,
    ) {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (output_tx, output_rx) = mpsc::channel(16);
        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let actor = PartitionActorShared {
            group_id: GroupId::new(1),
            raft_node,
            cmd_rx,
            output_tx,
            pending_proposals: HashMap::new(),
            batch_pending_proposals: HashMap::new(),
            pending_batch: AccumulatedBatch::new(),
            batch_config: PartitionBatchConfig::default(),
            backpressure: PartitionBackpressure::new(
                25 * 1024 * 1024,
                500,
            ),
            next_base_offset,
            offset_seeded,
            is_leader_cache: Arc::new(AtomicBool::new(false)),
            batcher_stats: None,
            global_backpressure: None,
        };
        (actor, output_rx, cmd_tx)
    }

    /// Encodes a minimal `AppendBlobBatch` with the given
    /// `base_offset` and `record_count` for one blob.
    fn encode_blob_batch(
        base_offset: Offset,
        record_count: u32,
    ) -> Bytes {
        StoragePartitionCommand::AppendBlobBatch {
            blobs: vec![BatchedBlob {
                blob: Bytes::from(vec![0u8; 32]),
                record_count,
                format: BlobFormat::Raw,
            }],
            base_offset,
        }
        .encode()
    }

    fn make_batch_info(total_records: u64) -> BatchProposalInfo {
        let (tx, _rx) = oneshot::channel();
        BatchProposalInfo {
            first_request_at: Instant::now(),
            batch_size: 1,
            batch_bytes: 32,
            total_records,
            record_counts: vec![
                #[allow(clippy::cast_possible_truncation)]
                {
                    total_records as u32
                },
            ],
            result_txs: vec![tx],
        }
    }

    #[test]
    fn test_patch_base_offset_seeds_on_first_propose() {
        let (mut actor, _output_rx, _cmd_tx) =
            create_test_shared_actor(Offset::new(0), false);

        assert!(!actor.offset_seeded);
        let data =
            encode_blob_batch(Offset::new(500), 100);
        let info = make_batch_info(100);
        let patched = actor.patch_batch_base_offset(data, &info);

        // First propose seeds from batcher offset.
        assert!(actor.offset_seeded);
        // No patch needed — batcher offset becomes the seed.
        let offset_bytes: [u8; 8] =
            patched[1..9].try_into().unwrap();
        assert_eq!(
            u64::from_le_bytes(offset_bytes),
            500,
            "first propose should use batcher's offset"
        );
        assert_eq!(
            actor.next_base_offset,
            Offset::new(600),
            "tracker should advance by total_records"
        );
    }

    #[test]
    fn test_patch_base_offset_corrects_stale_offset() {
        let (mut actor, _output_rx, _cmd_tx) =
            create_test_shared_actor(Offset::new(0), false);

        // First batch: seeds offset from batcher (603), 125 records.
        let data1 =
            encode_blob_batch(Offset::new(603), 125);
        let info1 = make_batch_info(125);
        let patched1 =
            actor.patch_batch_base_offset(data1, &info1);
        let o1: [u8; 8] = patched1[1..9].try_into().unwrap();
        assert_eq!(u64::from_le_bytes(o1), 603);
        assert_eq!(actor.next_base_offset, Offset::new(728));

        // Second batch: batcher still reads stale 603 (first
        // batch not yet committed).
        let data2 =
            encode_blob_batch(Offset::new(603), 100);
        let info2 = make_batch_info(100);
        let patched2 =
            actor.patch_batch_base_offset(data2, &info2);
        let o2: [u8; 8] = patched2[1..9].try_into().unwrap();
        assert_eq!(
            u64::from_le_bytes(o2),
            728,
            "stale offset 603 should be patched to 728"
        );
        assert_eq!(
            actor.next_base_offset,
            Offset::new(828),
            "tracker should be 728 + 100"
        );
    }

    #[test]
    fn test_patch_base_offset_no_patch_when_current() {
        // Actor already seeded at 1000.
        let (mut actor, _output_rx, _cmd_tx) =
            create_test_shared_actor(Offset::new(1000), true);

        // Batcher reads the same offset. No patch needed.
        let data =
            encode_blob_batch(Offset::new(1000), 50);
        let info = make_batch_info(50);
        let patched = actor.patch_batch_base_offset(data, &info);
        let o: [u8; 8] = patched[1..9].try_into().unwrap();
        assert_eq!(u64::from_le_bytes(o), 1000);
        assert_eq!(actor.next_base_offset, Offset::new(1050));
    }

    #[test]
    fn test_patch_base_offset_non_blob_batch_passthrough() {
        let (mut actor, _output_rx, _cmd_tx) =
            create_test_shared_actor(Offset::new(500), true);

        // A non-AppendBlobBatch command (type byte != 4).
        let data = Bytes::from(vec![0u8; 20]);
        let info = make_batch_info(10);
        let patched = actor.patch_batch_base_offset(data.clone(), &info);
        // Should be returned unmodified.
        assert_eq!(patched, data);
        // Tracker should NOT advance for non-blob commands.
        assert_eq!(actor.next_base_offset, Offset::new(500));
    }
}

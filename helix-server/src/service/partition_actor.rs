//! Partition Actor for lock-free multi-partition scalability.
//!
//! Each partition runs as an independent actor with its own Raft state machine.
//! This eliminates lock contention between partitions, enabling full parallelism.
//!
//! # Architecture
//!
//! ```text
//! Client Request
//!       │
//!       ▼
//! ┌─────────────────┐
//! │ PartitionActor  │──────► Transport (send to peers)
//! │   - RaftNode    │
//! │   - Pending     │◄────── Transport (receive from peers)
//! └─────────────────┘
//!       │
//!       ▼
//!   WalActor (batched writes)
//! ```
//!
//! # Message Flow
//!
//! 1. `Propose`: Client submits entry, actor proposes to Raft, replicates to peers
//! 2. `Tick`: Periodic tick for election timeouts and heartbeats
//! 3. `RaftMessage`: Inbound message from peer, handled by Raft state machine
//! 4. `CommitReady`: WAL write completed, notify waiting clients

use std::collections::HashMap;

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, Offset};
use helix_raft::multi::GroupMessage;
use helix_raft::{ClientRequest, Message, RaftNode};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, instrument, warn};

use crate::error::{ServerError, ServerResult};

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
    ///
    /// The partition actor owns the full lifecycle: it stores the batch info,
    /// proposes to Raft, and notifies the reply channels when committed.
    /// This avoids race conditions by keeping all state in one actor.
    ProposeBatch {
        /// The batch data to propose.
        data: Bytes,
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

    /// Graceful shutdown.
    Shutdown,
}

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
            Self::ActorShutdown => write!(f, "partition actor has shut down"),
            Self::ProposalRejected(reason) => write!(f, "proposal rejected: {reason}"),
        }
    }
}

impl std::error::Error for PartitionError {}

/// Information needed to notify batch clients on commit.
///
/// This is sent with `ProposeBatch` and stored by the partition actor until
/// the entry commits, at which point the actor notifies all reply channels.
pub struct BatchProposalInfo {
    /// Timestamp when the first request entered the batch.
    pub first_request_at: std::time::Instant,
    /// Number of requests in this batch.
    pub batch_size: u32,
    /// Total bytes in this batch.
    pub batch_bytes: u32,
    /// Total records across all requests in this batch.
    pub total_records: u64,
    /// Record counts for each request in the batch (for offset calculation).
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
/// Stored by the partition actor until commit, then used to notify clients.
struct BatchPendingInfo {
    /// Timestamp when the first request entered the batch.
    first_request_at: std::time::Instant,
    /// Timestamp when the batch was proposed to Raft.
    proposed_at: std::time::Instant,
    /// Number of requests in this batch.
    batch_size: u32,
    /// Total bytes in this batch.
    batch_bytes: u32,
    /// Total records across all requests in this batch.
    total_records: u64,
    /// Record counts for each request in the batch (for offset calculation).
    record_counts: Vec<u32>,
    /// Channels to notify each waiter with their assigned offset.
    result_txs: Vec<oneshot::Sender<ServerResult<Offset>>>,
}

/// Information needed by output processor to notify batch clients after storage apply.
///
/// The partition actor extracts this from `BatchPendingInfo` on commit and attaches
/// it to `EntryCommitted`. The output processor applies the entry to storage to get
/// the `base_offset`, then uses this info to notify each client with their offset.
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
    /// Record counts for each request in the batch (for offset calculation).
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

/// Handle for sending commands to a partition actor.
#[derive(Clone)]
pub struct PartitionActorHandle {
    /// Channel to send commands.
    tx: mpsc::Sender<PartitionCommand>,
    /// The group ID this handle is for.
    group_id: GroupId,
}

impl PartitionActorHandle {
    /// Creates a new handle.
    #[must_use]
    pub const fn new(tx: mpsc::Sender<PartitionCommand>, group_id: GroupId) -> Self {
        Self { tx, group_id }
    }

    /// Returns the group ID for this partition.
    #[must_use]
    pub const fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Proposes a new entry to the Raft log.
    ///
    /// Returns the log index assigned to the entry on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader
    /// - The actor has shut down
    /// - The proposal was rejected
    pub async fn propose(&self, data: Bytes) -> Result<ProposeResult, PartitionError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(PartitionCommand::Propose { data, reply: reply_tx })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;

        reply_rx.await.map_err(|_| PartitionError::ActorShutdown)?
    }

    /// Proposes a batch with reply channels for commit notification.
    ///
    /// The partition actor owns the full lifecycle: it stores the batch info,
    /// proposes to Raft, and notifies the reply channels when committed.
    /// This method returns immediately after sending the command.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor has shut down. Note that proposal failures
    /// (e.g., not leader) are communicated via the reply channels in `batch_info`.
    pub async fn propose_batch(
        &self,
        data: Bytes,
        batch_info: BatchProposalInfo,
    ) -> Result<(), PartitionError> {
        self.tx
            .send(PartitionCommand::ProposeBatch { data, batch_info })
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
    pub async fn leader_id(&self) -> Result<Option<NodeId>, PartitionError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.tx
            .send(PartitionCommand::LeaderId { reply: reply_tx })
            .await
            .map_err(|_| PartitionError::ActorShutdown)?;

        reply_rx.await.map_err(|_| PartitionError::ActorShutdown)
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

/// Output from processing a partition command.
///
/// These are actions the actor needs external help to complete.
#[derive(Debug)]
pub enum PartitionOutput {
    /// Send messages to peers via transport.
    SendMessages {
        /// Destination node.
        to: NodeId,
        /// Messages to send (may contain multiple groups if batched).
        messages: Vec<GroupMessage>,
    },
    /// An entry was committed and needs to be applied.
    EntryCommitted {
        /// Log index of the committed entry.
        index: LogIndex,
        /// Data payload.
        data: Bytes,
        /// Optional batch notification info (for batched proposals from this leader).
        /// If present, the output processor should notify clients after applying.
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
}

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

/// Creates a new partition actor and returns a handle to communicate with it.
///
/// The actor runs in its own tokio task and processes commands sequentially.
///
/// # Arguments
///
/// * `group_id` - The partition/group ID
/// * `raft_node` - The Raft state machine for this partition
/// * `config` - Actor configuration
///
/// # Returns
///
/// A tuple of (handle, `output_receiver`) where:
/// - `handle` is used to send commands to the actor
/// - `output_receiver` receives outputs that need external processing
#[must_use]
pub fn spawn_partition_actor(
    group_id: GroupId,
    raft_node: RaftNode,
    config: PartitionActorConfig,
) -> (PartitionActorHandle, mpsc::Receiver<PartitionOutput>) {
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_buffer_size);
    let (output_tx, output_rx) = mpsc::channel(config.channel_buffer_size);

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
///
/// When multiple partition actors share an output channel, this wrapper
/// identifies which partition produced the output.
#[derive(Debug)]
pub struct GroupedOutput {
    /// The group/partition that produced this output.
    pub group_id: GroupId,
    /// The output.
    pub output: PartitionOutput,
}

/// Spawns a partition actor with a shared output channel.
///
/// Unlike `spawn_partition_actor`, this variant sends outputs to a shared
/// channel (wrapped in `GroupedOutput`). This enables a single task to
/// process outputs from all partition actors.
///
/// # Arguments
///
/// * `group_id` - The partition/group ID
/// * `raft_node` - The Raft state machine for this partition
/// * `config` - Actor configuration
/// * `shared_output_tx` - Shared channel for all actor outputs
///
/// # Returns
///
/// A handle for sending commands to the actor.
#[must_use]
pub fn spawn_partition_actor_shared(
    group_id: GroupId,
    raft_node: RaftNode,
    config: PartitionActorConfig,
    shared_output_tx: mpsc::Sender<GroupedOutput>,
) -> PartitionActorHandle {
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_buffer_size);

    let actor = PartitionActorShared {
        group_id,
        raft_node,
        cmd_rx,
        output_tx: shared_output_tx,
        pending_proposals: HashMap::new(),
        batch_pending_proposals: HashMap::new(),
    };

    tokio::spawn(actor.run());

    PartitionActorHandle::new(cmd_tx, group_id)
}

/// Partition actor that sends to a shared output channel.
struct PartitionActorShared {
    group_id: GroupId,
    raft_node: RaftNode,
    cmd_rx: mpsc::Receiver<PartitionCommand>,
    output_tx: mpsc::Sender<GroupedOutput>,
    /// Pending single-entry proposals waiting for commit (for `Propose` command).
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<ProposeResult, PartitionError>>>,
    /// Pending batch proposals waiting for commit (for `ProposeBatch` command).
    /// The actor owns this state and notifies clients directly on commit.
    batch_pending_proposals: HashMap<LogIndex, BatchPendingInfo>,
}

impl PartitionActorShared {
    #[instrument(skip(self), fields(group_id = self.group_id.get()))]
    async fn run(mut self) {
        info!("Partition actor (shared output) started");

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                PartitionCommand::Propose { data, reply } => {
                    self.handle_propose(data, reply).await;
                }
                PartitionCommand::ProposeBatch { data, batch_info } => {
                    self.handle_propose_batch(data, batch_info).await;
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
                PartitionCommand::Shutdown => {
                    info!("Partition actor (shared output) shutting down");
                    break;
                }
            }
        }

        // Fail any pending single-entry proposals on shutdown.
        for (_, reply) in self.pending_proposals.drain() {
            let _ = reply.send(Err(PartitionError::ActorShutdown));
        }

        // Fail any pending batch proposals on shutdown.
        for (_, batch_info) in self.batch_pending_proposals.drain() {
            let err = ServerError::Internal {
                message: "partition actor shut down".to_string(),
            };
            for result_tx in batch_info.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
        }

        info!("Partition actor (shared output) stopped");
    }

    async fn handle_propose_batch(&mut self, data: Bytes, batch_info: BatchProposalInfo) {
        // Check if we're the leader.
        if !self.raft_node.is_leader() {
            let err = ServerError::NotLeader {
                topic: "unknown".to_string(),
                partition: 0,
                leader_hint: self.raft_node.leader_id().map(NodeId::get),
            };
            for result_tx in batch_info.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
            return;
        }

        // Propose to Raft.
        let request = ClientRequest::new(data);
        if let Some(outputs) = self.raft_node.handle_client_request(request) {
            // Get the log index assigned to this entry.
            let log_index = LogIndex::new(self.raft_node.log().last_index().get());

            // Store batch info for notification on commit.
            // This happens BEFORE processing outputs, so no race condition.
            let pending_info = BatchPendingInfo {
                first_request_at: batch_info.first_request_at,
                proposed_at: std::time::Instant::now(),
                batch_size: batch_info.batch_size,
                batch_bytes: batch_info.batch_bytes,
                total_records: batch_info.total_records,
                record_counts: batch_info.record_counts,
                result_txs: batch_info.result_txs,
            };
            self.batch_pending_proposals.insert(log_index, pending_info);

            info!(
                group_id = self.group_id.get(),
                log_index = log_index.get(),
                batch_size = batch_info.batch_size,
                pending_count = self.batch_pending_proposals.len(),
                "Batch proposed to Raft (actor mode)"
            );

            // Process outputs (may produce CommitEntry, which we'll handle).
            self.process_raft_outputs(outputs).await;
        } else {
            // Proposal rejected.
            let err = ServerError::Internal {
                message: "Raft rejected proposal".to_string(),
            };
            for result_tx in batch_info.result_txs {
                let _ = result_tx.send(Err(err.clone()));
            }
        }
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

        let request = ClientRequest::new(data);
        match self.raft_node.handle_client_request(request) {
            Some(outputs) => {
                let log_index = LogIndex::new(self.raft_node.log().last_index().get());
                self.pending_proposals.insert(log_index, reply);
                self.process_raft_outputs(outputs).await;
            }
            None => {
                let _ = reply.send(Err(PartitionError::ProposalRejected(
                    "Raft rejected proposal".to_string(),
                )));
            }
        }
    }

    async fn handle_tick(&mut self) {
        let outputs = self.raft_node.tick();
        self.process_raft_outputs(outputs).await;
    }

    async fn handle_raft_message(&mut self, _from: NodeId, message: Message) {
        let outputs = self.raft_node.handle_message(message);
        self.process_raft_outputs(outputs).await;
    }

    #[allow(clippy::too_many_lines)]
    async fn process_raft_outputs(&mut self, outputs: Vec<helix_raft::RaftOutput>) {
        use helix_raft::RaftOutput;

        for output in outputs {
            match output {
                RaftOutput::SendMessage(message) => {
                    let to = message.to();
                    let group_message = GroupMessage {
                        group_id: self.group_id,
                        message,
                    };
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::SendMessages {
                            to,
                            messages: vec![group_message],
                        },
                    };
                    if self.output_tx.send(grouped).await.is_err() {
                        warn!("Failed to send message output");
                    }
                }
                RaftOutput::CommitEntry { index, data } => {
                    // Notify single-entry pending proposal if any.
                    if let Some(reply) = self.pending_proposals.remove(&index) {
                        let _ = reply.send(Ok(ProposeResult { log_index: index }));
                    }

                    // Extract batch info if this was a batched proposal from this leader.
                    // Convert to BatchNotifyInfo to pass to output processor.
                    let has_batch = self.batch_pending_proposals.contains_key(&index);
                    let batch_notify =
                        self.batch_pending_proposals
                            .remove(&index)
                            .map(|pending| BatchNotifyInfo {
                                first_request_at: pending.first_request_at,
                                proposed_at: pending.proposed_at,
                                batch_size: pending.batch_size,
                                batch_bytes: pending.batch_bytes,
                                total_records: pending.total_records,
                                record_counts: pending.record_counts,
                                result_txs: pending.result_txs,
                            });

                    info!(
                        group_id = self.group_id.get(),
                        index = index.get(),
                        has_batch,
                        batch_notify_some = batch_notify.is_some(),
                        "CommitEntry in partition actor (actor mode)"
                    );

                    // Send EntryCommitted for storage application.
                    // Output processor will apply entry, get base_offset, then notify clients.
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::EntryCommitted {
                            index,
                            data,
                            batch_notify,
                        },
                    };
                    if self.output_tx.send(grouped).await.is_err() {
                        warn!("Failed to send committed entry output");
                    }
                }
                RaftOutput::BecameLeader => {
                    info!(group_id = self.group_id.get(), "Became leader");
                    let grouped = GroupedOutput {
                        group_id: self.group_id,
                        output: PartitionOutput::BecameLeader,
                    };
                    let _ = self.output_tx.send(grouped).await;
                }
                RaftOutput::SteppedDown => {
                    info!(group_id = self.group_id.get(), "Stepped down from leader");

                    // Fail single-entry pending proposals.
                    for (_, reply) in self.pending_proposals.drain() {
                        let _ = reply.send(Err(PartitionError::NotLeader {
                            leader_id: self.raft_node.leader_id(),
                        }));
                    }

                    // Fail batch pending proposals.
                    let err = ServerError::NotLeader {
                        topic: "unknown".to_string(),
                        partition: 0,
                        leader_hint: self.raft_node.leader_id().map(NodeId::get),
                    };
                    for (_, batch_info) in self.batch_pending_proposals.drain() {
                        for result_tx in batch_info.result_txs {
                            let _ = result_tx.send(Err(err.clone()));
                        }
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
            }
        }
    }

}

/// The partition actor state.
struct PartitionActor {
    /// The group/partition ID.
    group_id: GroupId,
    /// The Raft state machine.
    raft_node: RaftNode,
    /// Inbound command channel.
    cmd_rx: mpsc::Receiver<PartitionCommand>,
    /// Outbound output channel.
    output_tx: mpsc::Sender<PartitionOutput>,
    /// Pending proposals awaiting commit.
    /// Maps log index to reply channel.
    pending_proposals: HashMap<LogIndex, oneshot::Sender<Result<ProposeResult, PartitionError>>>,
}

impl PartitionActor {
    /// Runs the actor message loop.
    #[instrument(skip(self), fields(group_id = self.group_id.get()))]
    async fn run(mut self) {
        info!("Partition actor started");

        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                PartitionCommand::Propose { data, reply } => {
                    self.handle_propose(data, reply).await;
                }
                PartitionCommand::ProposeBatch { batch_info, .. } => {
                    // Non-shared actor doesn't support batched proposals.
                    // Fail all waiters immediately.
                    let err = ServerError::Internal {
                        message: "ProposeBatch not supported in non-shared actor".to_string(),
                    };
                    for result_tx in batch_info.result_txs {
                        let _ = result_tx.send(Err(err.clone()));
                    }
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
                PartitionCommand::Shutdown => {
                    info!("Partition actor shutting down");
                    break;
                }
            }
        }

        // Fail any pending proposals on shutdown.
        for (_, reply) in self.pending_proposals.drain() {
            let _ = reply.send(Err(PartitionError::ActorShutdown));
        }

        info!("Partition actor stopped");
    }

    /// Handles a propose command.
    async fn handle_propose(
        &mut self,
        data: Bytes,
        reply: oneshot::Sender<Result<ProposeResult, PartitionError>>,
    ) {
        // Check if we're the leader.
        if !self.raft_node.is_leader() {
            let _ = reply.send(Err(PartitionError::NotLeader {
                leader_id: self.raft_node.leader_id(),
            }));
            return;
        }

        // Propose to Raft.
        let request = ClientRequest::new(data);
        match self.raft_node.handle_client_request(request) {
            Some(outputs) => {
                // Get the log index that was assigned.
                let log_index = LogIndex::new(self.raft_node.log().last_index().get());

                // Store pending reply.
                self.pending_proposals.insert(log_index, reply);

                // Process Raft outputs.
                self.process_raft_outputs(outputs).await;
            }
            None => {
                // Proposal rejected (shouldn't happen if we checked is_leader).
                let _ = reply.send(Err(PartitionError::ProposalRejected(
                    "Raft rejected proposal".to_string(),
                )));
            }
        }
    }

    /// Handles a tick command.
    async fn handle_tick(&mut self) {
        let outputs = self.raft_node.tick();
        self.process_raft_outputs(outputs).await;
    }

    /// Handles an inbound Raft message.
    async fn handle_raft_message(&mut self, _from: NodeId, message: Message) {
        let outputs = self.raft_node.handle_message(message);
        self.process_raft_outputs(outputs).await;
    }

    /// Processes Raft outputs and sends them to the output channel.
    async fn process_raft_outputs(&mut self, outputs: Vec<helix_raft::RaftOutput>) {
        use helix_raft::RaftOutput;

        for output in outputs {
            match output {
                RaftOutput::SendMessage(message) => {
                    let to = message.to();
                    let group_message = GroupMessage {
                        group_id: self.group_id,
                        message,
                    };
                    let _ = self.output_tx.send(PartitionOutput::SendMessages {
                        to,
                        messages: vec![group_message],
                    }).await;
                }
                RaftOutput::CommitEntry { index, data } => {
                    // Notify pending proposal if any.
                    if let Some(reply) = self.pending_proposals.remove(&index) {
                        let _ = reply.send(Ok(ProposeResult { log_index: index }));
                    }

                    // Send commit notification for state machine application.
                    // Non-shared actor doesn't support batch proposals.
                    let _ = self
                        .output_tx
                        .send(PartitionOutput::EntryCommitted {
                            index,
                            data,
                            batch_notify: None,
                        })
                        .await;
                }
                RaftOutput::BecameLeader => {
                    info!(group_id = self.group_id.get(), "Became leader");
                    let _ = self.output_tx.send(PartitionOutput::BecameLeader).await;
                }
                RaftOutput::SteppedDown => {
                    info!(group_id = self.group_id.get(), "Stepped down from leader");

                    // Fail pending proposals - we're no longer leader.
                    for (_, reply) in self.pending_proposals.drain() {
                        let _ = reply.send(Err(PartitionError::NotLeader {
                            leader_id: self.raft_node.leader_id(),
                        }));
                    }

                    let _ = self.output_tx.send(PartitionOutput::SteppedDown).await;
                }
                RaftOutput::VoteStateChanged { term, voted_for } => {
                    let _ = self.output_tx.send(PartitionOutput::VoteStateChanged {
                        term: term.get(),
                        voted_for,
                    }).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_core::NodeId;
    use helix_raft::RaftConfig;

    fn create_test_raft_node(node_id: u64, cluster: Vec<u64>) -> RaftNode {
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

        // Actor should be running.
        assert!(handle.is_leader().await.is_ok());

        // Shutdown should succeed.
        assert!(handle.shutdown().await.is_ok());

        // Further commands should fail.
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

        // Initially not leader (haven't won election).
        let is_leader = handle.is_leader().await.unwrap();
        assert!(!is_leader);

        // Propose should fail with NotLeader.
        let result = handle.propose(Bytes::from("test")).await;
        assert!(matches!(result, Err(PartitionError::NotLeader { .. })));

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

        // Send ticks to trigger election.
        for _ in 0..20 {
            handle.tick().await.unwrap();
        }

        // Should receive some outputs (pre-vote messages, etc.).
        let output = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            output_rx.recv()
        ).await;

        // Expect either a message or timeout (both are valid depending on timing).
        // The important thing is the actor processed the ticks without panicking.
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

        // Clone the handle.
        let handle2 = handle.clone();

        // Both handles should work.
        assert!(handle.is_leader().await.is_ok());
        assert!(handle2.is_leader().await.is_ok());

        handle.shutdown().await.ok();
    }
}

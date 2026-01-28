//! Background tick task for Raft consensus.
//!
//! This module handles periodic ticks and message processing for Raft groups.

// Allow complex nested types for proposal maps - refactoring would require significant API changes.
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use helix_core::{GroupId, LogIndex, NodeId, Offset, TermId};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use helix_runtime::{BrokerHeartbeat, IncomingMessage, TransportHandle};
use helix_wal::TokioStorage;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::vote_store::{LocalFileVoteStorage, VoteStore};

use crate::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::partition_storage::ServerPartitionStorage;
use crate::storage::PartitionCommand;
use helix_tier::TieringConfig;
#[cfg(feature = "s3")]
use helix_tier::S3Config;

use super::{
    output_processor::extract_and_record_producer_state, BatchPendingProposal, BatcherStats,
    PendingControllerProposal, PendingProposal, TICK_INTERVAL_MS,
};

/// Interval for sending broker heartbeats to the controller (in milliseconds).
///
/// Set to 1 second for fast dead broker detection. Combined with a 5 second
/// timeout, this allows up to 5 missed heartbeats before a broker is fenced.
///
/// Used by both production tick tasks and DST simulation actors.
pub const HEARTBEAT_INTERVAL_MS: u64 = 1_000; // 1 second.

/// Interval for tiering background tasks (in milliseconds).
///
/// Set to 5 seconds as a balance between promptly tiering new segments
/// and avoiding excessive overhead. Tiering operations include:
/// - Registering newly sealed segments with the tiering manager
/// - Marking committed segments as eligible for tiering
/// - Uploading eligible segments to object storage (S3/filesystem)
pub const TIERING_INTERVAL_MS: u64 = 5_000; // 5 seconds.

/// Extract a payload preview from a `CommitEntry`'s data for diagnostic logging.
///
/// Decodes the `PartitionCommand` and extracts the first ~30 bytes of ASCII text
/// from the first blob (if present). This helps correlate `NOTIFY_CLIENT` logs
/// with the actual data being committed.
fn extract_commit_payload_preview(data: &bytes::Bytes) -> String {
    let Some(command) = PartitionCommand::decode(data) else {
        return "<decode failed>".to_string();
    };

    let blob = match &command {
        PartitionCommand::AppendBlob { blob, .. } => blob,
        PartitionCommand::AppendBlobBatch { blobs, .. } => {
            if blobs.is_empty() {
                return "<empty batch>".to_string();
            }
            &blobs[0].blob
        }
        _ => return "<non-blob command>".to_string(),
    };

    // Extract ASCII text from the end of the blob (where payload typically is).
    if blob.len() < 60 {
        return format!("<blob len={}>", blob.len());
    }

    let mut preview = Vec::new();
    for &b in blob.iter().rev().take(50) {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            preview.push(b);
        } else if !preview.is_empty() {
            break;
        }
    }
    preview.reverse();
    String::from_utf8(preview).unwrap_or_else(|_| "<non-utf8>".to_string())
}

/// Background task to handle Raft ticks for all groups (single-node).
#[allow(clippy::significant_drop_tightening)]
pub async fn tick_task(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut tiering_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TIERING_INTERVAL_MS));

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                let outputs = {
                    let mut mr = multi_raft.write().await;
                    mr.tick()
                };
                process_outputs(
                    &outputs,
                    &partition_storage,
                    &group_map,
                    &pending_proposals,
                ).await;
            }
            _ = tiering_interval.tick() => {
                process_tiering(&partition_storage).await;
            }
        }
    }
}

/// Background task for multi-node operation.
#[allow(clippy::significant_drop_tightening)]
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn tick_task_multi_node(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    cluster_nodes: Vec<NodeId>,
    transport_handle: TransportHandle,
    data_dir: Option<PathBuf>,
    object_storage_dir: Option<PathBuf>,
    #[cfg(feature = "s3")] s3_config: Option<S3Config>,
    tiering_config: Option<TieringConfig>,
    shared_wal_pool: Option<Arc<helix_wal::SharedWalPool<helix_wal::TokioStorage>>>,
    recovered_entries: Arc<RwLock<HashMap<helix_core::PartitionId, Vec<helix_wal::SharedEntry>>>>,
    batcher_stats: Option<Arc<BatcherStats>>,
    batcher_backpressure: Option<Arc<crate::service::batcher::BackpressureState>>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    mut incoming_rx: mpsc::Receiver<IncomingMessage>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut heartbeat_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    let mut tiering_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TIERING_INTERVAL_MS));

    // Get our node ID for heartbeats.
    let node_id = {
        let mr = multi_raft.read().await;
        mr.node_id()
    };

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Multi-node tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                let outputs = {
                    let mut mr = multi_raft.write().await;
                    mr.tick()
                };
                #[cfg(feature = "s3")]
                process_outputs_multi_node(
                    &outputs,
                    node_id,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &batch_pending_proposals,
                    &batcher_stats,
                    &batcher_backpressure,
                    &cluster_nodes,
                    &transport_handle,
                    &data_dir,
                    &object_storage_dir,
                    &s3_config,
                    &tiering_config,
                    &shared_wal_pool,
                    &recovered_entries,
                    vote_store.as_ref(),
                ).await;
                #[cfg(not(feature = "s3"))]
                process_outputs_multi_node(
                    &outputs,
                    node_id,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &batch_pending_proposals,
                    &batcher_stats,
                    &batcher_backpressure,
                    &cluster_nodes,
                    &transport_handle,
                    &data_dir,
                    &object_storage_dir,
                    &tiering_config,
                    &shared_wal_pool,
                    &recovered_entries,
                    vote_store.as_ref(),
                ).await;
            }
            _ = heartbeat_interval.tick() => {
                // Send broker heartbeat via transport to all peers (Kafka KRaft pattern).
                // Unlike Raft-replicated state, heartbeats are soft state maintained locally
                // on each node based on received heartbeat messages.
                send_broker_heartbeats_to_peers(
                    &local_broker_heartbeats,
                    node_id,
                    &cluster_nodes,
                    &transport_handle,
                ).await;
            }
            _ = tiering_interval.tick() => {
                // Process tiering for all durable partitions.
                process_tiering(&partition_storage).await;
            }
            Some(incoming) = incoming_rx.recv() => {
                let outputs = match incoming {
                    IncomingMessage::Single(_message) => {
                        warn!("Received single message in multi-node mode, expected batch");
                        vec![]
                    }
                    IncomingMessage::Batch(group_messages) => {
                        let mut mr = multi_raft.write().await;
                        let mut all_outputs = Vec::new();
                        for group_msg in group_messages {
                            let outputs = mr.handle_message(
                                group_msg.group_id,
                                group_msg.message,
                            );
                            all_outputs.extend(outputs);
                        }

                        // Flush any pending outbound messages immediately.
                        // This ensures responses and commit advancements are sent
                        // without waiting for the next tick (50ms), reducing latency.
                        all_outputs.extend(mr.flush());
                        all_outputs
                    }
                    IncomingMessage::Heartbeat(heartbeat) => {
                        // Update local heartbeat soft state.
                        let mut heartbeats = local_broker_heartbeats.write().await;
                        heartbeats.insert(heartbeat.node_id, heartbeat.timestamp_ms);
                        debug!(
                            from = heartbeat.node_id.get(),
                            timestamp_ms = heartbeat.timestamp_ms,
                            "Recorded heartbeat from peer"
                        );
                        vec![] // No Raft outputs for heartbeats.
                    }
                };
                #[cfg(feature = "s3")]
                process_outputs_multi_node(
                    &outputs,
                    node_id,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &batch_pending_proposals,
                    &batcher_stats,
                    &batcher_backpressure,
                    &cluster_nodes,
                    &transport_handle,
                    &data_dir,
                    &object_storage_dir,
                    &s3_config,
                    &tiering_config,
                    &shared_wal_pool,
                    &recovered_entries,
                    vote_store.as_ref(),
                ).await;
                #[cfg(not(feature = "s3"))]
                process_outputs_multi_node(
                    &outputs,
                    node_id,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &batch_pending_proposals,
                    &batcher_stats,
                    &batcher_backpressure,
                    &cluster_nodes,
                    &transport_handle,
                    &data_dir,
                    &object_storage_dir,
                    &tiering_config,
                    &shared_wal_pool,
                    &recovered_entries,
                    vote_store.as_ref(),
                ).await;
            }
        }
    }
}

/// Background tick task for actor-based multi-node operation.
///
/// This is the actor-based alternative to `tick_task_multi_node`. Instead of
/// using `Arc<RwLock<MultiRaft>>`, it routes ticks and messages to partition
/// actors via the `PartitionRouter`, eliminating lock contention.
///
/// # Responsibilities
///
/// 1. **Tick broadcast**: Periodically calls `router.tick_all()` to tick all
///    partition actors in parallel.
/// 2. **Message routing**: Routes incoming Raft messages from transport to
///    the appropriate partition actors via `router.route_messages()`.
/// 3. **Heartbeats**: Sends broker heartbeats to all peers (Kafka `KRaft` pattern).
/// 4. **Tiering**: Processes tiering for durable partitions.
///
/// # Key Differences from `tick_task_multi_node`
///
/// - No `multi_raft` lock - ticks are sent via channels to partition actors
/// - No `process_outputs_multi_node` - outputs go through the shared output
///   channel and are processed by the `OutputProcessor`
/// - Simpler control flow since output processing is decoupled
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn tick_task_actor(
    router: Arc<super::router::PartitionRouter>,
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: Vec<NodeId>,
    transport_handle: TransportHandle,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    mut incoming_rx: mpsc::Receiver<IncomingMessage>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut heartbeat_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    let mut tiering_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TIERING_INTERVAL_MS));

    let initial_partition_count = router.partition_count().await;
    info!(
        node_id = node_id.get(),
        partition_count = initial_partition_count,
        "Actor tick task started"
    );

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Actor tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                // Broadcast tick to all partition actors in parallel.
                // Each actor processes its tick independently without lock contention.
                router.tick_all().await;
            }
            _ = heartbeat_interval.tick() => {
                // Send broker heartbeats to all peers (Kafka KRaft pattern).
                // This is unchanged from the lock-based approach since heartbeats
                // are soft state that doesn't require Raft consensus.
                send_broker_heartbeats_to_peers(
                    &local_broker_heartbeats,
                    node_id,
                    &cluster_nodes,
                    &transport_handle,
                ).await;
            }
            _ = tiering_interval.tick() => {
                // Process tiering for all durable partitions.
                // This is unchanged from the lock-based approach.
                process_tiering(&partition_storage).await;
            }
            Some(incoming) = incoming_rx.recv() => {
                // Route incoming messages to partition actors or MultiRaft.
                match incoming {
                    IncomingMessage::Single(_message) => {
                        warn!("Received single message in actor mode, expected batch");
                    }
                    IncomingMessage::Batch(group_messages) => {
                        // Split messages: controller messages go to MultiRaft,
                        // data partition messages go to partition actors.
                        let (controller_msgs, data_msgs): (Vec<_>, Vec<_>) = group_messages
                            .into_iter()
                            .partition(|gm| gm.group_id == CONTROLLER_GROUP_ID);

                        // Step controller messages through MultiRaft and process ALL outputs.
                        // This includes BecameLeader, CommitEntry, etc. - not just SendMessages.
                        if !controller_msgs.is_empty() {
                            info!(
                                count = controller_msgs.len(),
                                "Received controller messages in tick_task_actor"
                            );
                            let outputs = {
                                let mut mr = multi_raft.write().await;
                                mr.handle_messages(controller_msgs)
                            };
                            info!(
                                output_count = outputs.len(),
                                "Processed controller messages, got outputs"
                            );

                            // Process all controller outputs including BecameLeader, CommitEntry.
                            process_controller_outputs(
                                &outputs,
                                &multi_raft,
                                &partition_storage,
                                &group_map,
                                &controller_state,
                                &pending_proposals,
                                &pending_controller_proposals,
                                &cluster_nodes,
                                &transport_handle,
                                &router,
                                &output_tx,
                                vote_store.as_ref(),
                            ).await;
                        }

                        // Route data partition messages to partition actors.
                        if !data_msgs.is_empty() {
                            let from = data_msgs
                                .first()
                                .map_or(NodeId::new(0), |m| m.message.from());
                            router.route_messages(data_msgs, from).await;
                        }
                    }
                    IncomingMessage::Heartbeat(heartbeat) => {
                        // Update local heartbeat soft state.
                        // This is unchanged from the lock-based approach.
                        local_broker_heartbeats
                            .write()
                            .await
                            .insert(heartbeat.node_id, heartbeat.timestamp_ms);
                        debug!(
                            from = heartbeat.node_id.get(),
                            timestamp_ms = heartbeat.timestamp_ms,
                            "Recorded heartbeat from peer (actor mode)"
                        );
                    }
                }
            }
        }
    }

    info!("Actor tick task stopped");
}

/// Background tick task for controller partition only (actor mode).
///
/// When actor mode is enabled for data partitions, the controller partition
/// still needs to be ticked via `MultiRaft`. This task handles:
///
/// 1. **Controller ticking**: Periodically ticks the controller Raft group.
/// 2. **Controller outputs**: Processes commits, leader changes, and message sending.
/// 3. **Follow-up commands**: Proposes follow-up controller commands.
/// 4. **Dynamic partition creation**: Creates partition actors on `AssignPartition`.
///
/// Data partition operations are handled by `tick_task_actor` via the router.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn tick_task_controller(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: Vec<NodeId>,
    transport_handle: TransportHandle,
    router: Arc<super::router::PartitionRouter>,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));

    let node_id = {
        let mr = multi_raft.read().await;
        mr.node_id()
    };

    info!(
        node_id = node_id.get(),
        "Controller tick task started (actor mode)"
    );

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Controller tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                // Tick only the controller partition via MultiRaft.
                let outputs = {
                    let mut mr = multi_raft.write().await;
                    mr.tick()
                };

                if !outputs.is_empty() {
                    for output in &outputs {
                        match output {
                            MultiRaftOutput::CommitEntry { group_id, index, .. } => {
                                info!(group = group_id.get(), index = index.get(), "Tick: CommitEntry");
                            }
                            MultiRaftOutput::BecameLeader { group_id } => {
                                info!(group = group_id.get(), "Tick: BecameLeader");
                            }
                            MultiRaftOutput::SendMessages { to, messages } => {
                                info!(to = to.get(), count = messages.len(), "Tick: SendMessages");
                            }
                            MultiRaftOutput::SteppedDown { group_id } => {
                                info!(group = group_id.get(), "Tick: SteppedDown");
                            }
                            MultiRaftOutput::VoteStateChanged { group_id, term, voted_for } => {
                                info!(group = group_id.get(), term = term.get(), "Tick: VoteStateChanged");
                                // Persist vote state change.
                                if let Some(ref vs) = vote_store {
                                    if let Ok(mut store) = vs.lock() {
                                        if let Err(e) = store.save(*group_id, *term, *voted_for) {
                                            error!(
                                                group = group_id.get(),
                                                error = %e,
                                                "Failed to persist vote state"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Process controller-related outputs only.
                process_controller_outputs(
                    &outputs,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &cluster_nodes,
                    &transport_handle,
                    &router,
                    &output_tx,
                    vote_store.as_ref(),
                ).await;
            }
        }
    }

    info!("Controller tick task stopped");
}

/// Processes outputs for controller partition only (actor mode).
///
/// This is a simplified version of `process_outputs_multi_node` that only
/// handles controller partition (group 0) outputs. Data partition outputs
/// are handled by the `OutputProcessor`.
#[allow(clippy::too_many_arguments, clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn process_controller_outputs(
    outputs: &[MultiRaftOutput],
    multi_raft: &Arc<RwLock<MultiRaft>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    _pending_proposals: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    pending_controller_proposals: &Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: &[NodeId],
    transport_handle: &TransportHandle,
    router: &Arc<super::router::PartitionRouter>,
    output_tx: &mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<&Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
) {
    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                data,
            } => {
                // Only process controller partition commits.
                if *group_id != CONTROLLER_GROUP_ID {
                    // Data partition commits are handled by OutputProcessor in actor mode.
                    continue;
                }

                let Some(cmd) = ControllerCommand::decode(data) else {
                    warn!(
                        index = index.get(),
                        "Failed to decode controller command"
                    );
                    continue;
                };

                info!(
                    index = index.get(),
                    command = ?cmd,
                    "Applying controller command (actor mode)"
                );
                let mut state = controller_state.write().await;
                let follow_ups = state.apply(&cmd, cluster_nodes);

                // Notify any pending controller proposals for this index.
                {
                    let mut proposals = pending_controller_proposals.write().await;
                    let pending_indexes: Vec<u64> = proposals.iter().map(|p| p.log_index.get()).collect();
                    info!(
                        commit_index = index.get(),
                        pending_count = proposals.len(),
                        pending_indexes = ?pending_indexes,
                        "Checking pending controller proposals (actor mode)"
                    );
                    if let Some(pos) = proposals.iter().position(|p| p.log_index == *index) {
                        let proposal = proposals.swap_remove(pos);
                        let send_result = proposal.result_tx.send(Ok(()));
                        info!(
                            index = index.get(),
                            send_success = send_result.is_ok(),
                            "Notified pending controller proposal (actor mode)"
                        );
                    } else {
                        info!(
                            index = index.get(),
                            "No pending controller proposal for this index (actor mode)"
                        );
                    }
                }

                // Propose follow-up commands if we're the leader.
                if !follow_ups.is_empty() {
                    let is_leader = {
                        let mr = multi_raft.read().await;
                        mr.group_state(CONTROLLER_GROUP_ID)
                            .is_some_and(|s| s.state == RaftState::Leader)
                    };

                    if is_leader {
                        let mut mr = multi_raft.write().await;
                        for follow_up in follow_ups {
                            let encoded = follow_up.encode();
                            if mr.propose(CONTROLLER_GROUP_ID, encoded).is_none() {
                                warn!(
                                    command = ?follow_up,
                                    "Failed to propose follow-up controller command (actor mode)"
                                );
                            }
                        }
                    }
                }

                // Handle AssignPartition by creating partition actor and storage.
                if let ControllerCommand::AssignPartition {
                    topic_id,
                    partition_id,
                    group_id: data_group_id,
                    ref replicas,
                } = cmd
                {
                    let node_id = {
                        let mr = multi_raft.read().await;
                        mr.node_id()
                    };

                    if replicas.contains(&node_id) {
                        // Look up any persisted vote state for this partition.
                        let (term, voted_for, observation_mode) = vote_store
                            .and_then(|vs| vs.lock().ok())
                            .and_then(|store| {
                                store.state().get_group(data_group_id).map(|v| (v.term, v.voted_for, false))
                            })
                            .unwrap_or((TermId::new(0), None, false));

                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = data_group_id.get(),
                            term = term.get(),
                            voted_for = voted_for.map(NodeId::get),
                            replicas = ?replicas.iter().map(|n| n.get()).collect::<Vec<_>>(),
                            "Creating data partition from controller assignment (actor mode)"
                        );

                        // Update group map.
                        {
                            let mut gm = group_map.write().await;
                            gm.insert(topic_id, partition_id, data_group_id);
                        }

                        // Create partition storage (in-memory for now).
                        {
                            let mut storage = partition_storage.write().await;
                            if let std::collections::hash_map::Entry::Vacant(e) = storage.entry(data_group_id) {
                                let ps = ServerPartitionStorage::new_in_memory(topic_id, partition_id);
                                e.insert(Arc::new(RwLock::new(ps)));
                            }
                        }

                        // Create the partition actor with restored vote state and add it to the router.
                        let partition_handle = super::actor_setup::create_partition_actor_with_state(
                            data_group_id,
                            node_id,
                            replicas.clone(),
                            term,
                            voted_for,
                            observation_mode,
                            output_tx.clone(),
                            super::partition_actor::PartitionActorConfig::default(),
                        );

                        if router.add_partition_dynamic(data_group_id, partition_handle).await {
                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Created partition actor (actor mode)"
                            );
                        } else {
                            warn!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Partition actor already exists"
                            );
                        }
                    }
                }
            }
            MultiRaftOutput::BecameLeader { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Became controller leader (actor mode)");
                }
                // Ignore data partition leader changes - handled by OutputProcessor.
            }
            MultiRaftOutput::SteppedDown { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Stepped down from controller leader (actor mode)");
                }
                // Ignore data partition step downs - handled by OutputProcessor.
            }
            MultiRaftOutput::SendMessages { to, messages } => {
                // Filter to only send controller partition messages.
                // Data partition messages are sent by OutputProcessor.
                let controller_messages: Vec<_> = messages
                    .iter()
                    .filter(|m| m.group_id == CONTROLLER_GROUP_ID)
                    .cloned()
                    .collect();

                if !controller_messages.is_empty() {
                    if let Err(e) = transport_handle.send_batch(*to, controller_messages.clone()).await {
                        error!(
                            to = to.get(),
                            count = controller_messages.len(),
                            error = %e,
                            "Failed to send controller messages to peer (actor mode)"
                        );
                    } else {
                        debug!(
                            to = to.get(),
                            count = controller_messages.len(),
                            "Sent controller messages to peer (actor mode)"
                        );
                    }
                }
            }
            MultiRaftOutput::VoteStateChanged { group_id, term, voted_for } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    debug!(
                        group = group_id.get(),
                        term = term.get(),
                        voted_for = ?voted_for.map(helix_core::NodeId::get),
                        "Controller vote state changed (actor mode)"
                    );
                    // Persist controller vote state.
                    if let Some(vs) = vote_store {
                        if let Ok(mut store) = vs.lock() {
                            if let Err(e) = store.save(*group_id, *term, *voted_for) {
                                error!(
                                    group = group_id.get(),
                                    error = %e,
                                    "Failed to persist controller vote state"
                                );
                            }
                        }
                    }
                }
                // Ignore data partition vote changes - handled by OutputProcessor.
            }
        }
    }
}

/// Sends broker heartbeats to all peers via transport (Kafka `KRaft` pattern).
///
/// Unlike Raft-replicated state, heartbeats are soft state. Each broker:
/// 1. Records its own heartbeat locally
/// 2. Sends heartbeats to all other nodes via transport
///
/// This ensures all nodes have a local view of broker liveness without requiring
/// Raft consensus, which would only work for the leader node.
async fn send_broker_heartbeats_to_peers(
    local_broker_heartbeats: &Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: &[NodeId],
    transport_handle: &TransportHandle,
) {
    // Get current time in milliseconds.
    // Safe truncation: milliseconds won't overflow u64 for ~584 million years.
    #[allow(clippy::cast_possible_truncation)]
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    // Record our own heartbeat locally.
    {
        let mut heartbeats = local_broker_heartbeats.write().await;
        heartbeats.insert(node_id, timestamp_ms);
    }

    // Create the heartbeat message.
    let heartbeat = BrokerHeartbeat::new(node_id, timestamp_ms);

    // Send to all other nodes.
    for peer_id in cluster_nodes {
        if *peer_id == node_id {
            continue; // Don't send to self.
        }

        if let Err(e) = transport_handle.send_heartbeat(*peer_id, &heartbeat).await {
            debug!(
                peer_id = peer_id.get(),
                error = %e,
                "Failed to send heartbeat to peer"
            );
            // Non-fatal - we'll retry on next interval.
        }
    }

    debug!(
        node_id = node_id.get(),
        timestamp_ms = timestamp_ms,
        peer_count = cluster_nodes.len() - 1,
        "Sent broker heartbeats"
    );
}

/// Processes Multi-Raft outputs (single-node).
#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn process_outputs(
    outputs: &[MultiRaftOutput],
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    pending_proposals: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
) {
    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                data,
            } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };

                // Get base_offset BEFORE apply for producer state recording.
                let base_offset = {
                    let ps_lock = {
                        let storage = partition_storage.read().await;
                        storage.get(group_id).cloned()
                    };
                    if let Some(ps_lock) = ps_lock {
                        let ps = ps_lock.read().await;
                        ps.blob_log_end_offset()
                    } else {
                        Offset::new(0)
                    }
                };

                let apply_result = if let Some((topic_id, partition_id)) = key {
                    let ps_lock = {
                        let storage = partition_storage.read().await;
                        storage.get(group_id).cloned()
                    };
                    if let Some(ps_lock) = ps_lock {
                        let mut ps = ps_lock.write().await;
                        match ps.apply_entry_async(*index, data).await {
                            Ok(offset) => Ok(offset),
                            Err(e) => {
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    error = %e,
                                    "Failed to apply committed entry"
                                );
                                Err(e)
                            }
                        }
                    } else {
                        Err(ServerError::Internal {
                            message: "partition storage not found".to_string(),
                        })
                    }
                } else {
                    Err(ServerError::Internal {
                        message: "group not found in group map".to_string(),
                    })
                };

                // Extract and record producer state from committed entry.
                // Critical for PREVIOUS_TERM entries on new leader.
                if apply_result.is_ok() {
                    extract_and_record_producer_state(
                        data,
                        base_offset,
                        *group_id,
                        partition_storage,
                    )
                    .await;
                }

                // Find and notify any pending proposal for this entry (O(1) lookup).
                let inner_lock = {
                    let proposals = pending_proposals.read().await;
                    proposals.get(group_id).cloned()
                };
                if let Some(inner_lock) = inner_lock {
                    let mut group_proposals = inner_lock.write().await;
                    if let Some(proposal) = group_proposals.remove(index) {
                        let result = match &apply_result {
                            Ok(Some(offset)) => Ok(*offset),
                            Ok(None) => {
                                // No offset returned (e.g., empty entry), use current log end.
                                let ps_lock = {
                                    let storage = partition_storage.read().await;
                                    storage.get(group_id).cloned()
                                };
                                let offset = if let Some(ps_lock) = ps_lock {
                                    let ps = ps_lock.read().await;
                                    ps.log_end_offset()
                                } else {
                                    Offset::new(0)
                                };
                                Ok(offset)
                            }
                            Err(e) => Err(ServerError::Internal {
                                message: format!("apply failed: {e}"),
                            }),
                        };
                        // Ignore send errors (receiver may have timed out).
                        let _ = proposal.result_tx.send(result);
                    }
                }
            }
            MultiRaftOutput::BecameLeader { group_id } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Became leader"
                    );
                }
            }
            MultiRaftOutput::SteppedDown { group_id } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Stepped down from leader"
                    );
                }
            }
            MultiRaftOutput::SendMessages { to, messages } => {
                debug!(
                    to = to.get(),
                    count = messages.len(),
                    "Would send messages (single-node, ignoring)"
                );
            }
            MultiRaftOutput::VoteStateChanged { group_id, term, voted_for } => {
                debug!(
                    group = group_id.get(),
                    term = term.get(),
                    voted_for = ?voted_for.map(helix_core::NodeId::get),
                    "Vote state changed (single-node, not persisting)"
                );
                // Single-node mode doesn't persist vote state.
                // Multi-node mode uses the VoteStore for persistence.
            }
        }
    }
}

/// Processes Multi-Raft outputs with transport for sending messages.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::ref_option,
    clippy::significant_drop_tightening
)]
#[tracing::instrument(skip_all, name = "process_outputs", fields(output_count = outputs.len(), node_id = node_id.get()))]
async fn process_outputs_multi_node(
    outputs: &[MultiRaftOutput],
    node_id: NodeId,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    pending_proposals: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>>,
    pending_controller_proposals: &Arc<RwLock<Vec<PendingControllerProposal>>>,
    batch_pending_proposals: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
    batcher_stats: &Option<Arc<BatcherStats>>,
    batcher_backpressure: &Option<Arc<crate::service::batcher::BackpressureState>>,
    cluster_nodes: &[NodeId],
    transport_handle: &TransportHandle,
    data_dir: &Option<PathBuf>,
    object_storage_dir: &Option<PathBuf>,
    #[cfg(feature = "s3")] s3_config: &Option<S3Config>,
    tiering_config: &Option<TieringConfig>,
    shared_wal_pool: &Option<Arc<helix_wal::SharedWalPool<helix_wal::TokioStorage>>>,
    recovered_entries: &Arc<RwLock<HashMap<helix_core::PartitionId, Vec<helix_wal::SharedEntry>>>>,
    vote_store: Option<&Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
) {
    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                data,
            } => {
                info!(
                    node = node_id.get(),
                    group = group_id.get(),
                    index = index.get(),
                    data_len = data.len(),
                    "COMMIT_ENTRY: received"
                );

                // Check if this is a controller partition commit.
                if *group_id == CONTROLLER_GROUP_ID {
                    info!(
                        index = index.get(),
                        data_len = data.len(),
                        pending_proposals_ptr = ?std::sync::Arc::as_ptr(pending_controller_proposals),
                        "CommitEntry received for controller group"
                    );

                    let Some(cmd) = ControllerCommand::decode(data) else {
                        warn!(
                            index = index.get(),
                            data_len = data.len(),
                            "Failed to decode controller command"
                        );
                        continue;
                    };

                    info!(
                        index = index.get(),
                        command = ?cmd,
                        "Applying controller command"
                    );
                    let mut state = controller_state.write().await;
                    let follow_ups = state.apply(&cmd, cluster_nodes);

                    // Notify any pending controller proposals for this index.
                    {
                        let proposals = pending_controller_proposals.read().await;
                        let pending_indexes: Vec<u64> = proposals.iter().map(|p| p.log_index.get()).collect();
                        info!(
                            index = index.get(),
                            pending_count = proposals.len(),
                            pending_indexes = ?pending_indexes,
                            "Checking for pending proposal to notify"
                        );
                        drop(proposals);

                        let mut proposals = pending_controller_proposals.write().await;
                        if let Some(pos) = proposals.iter().position(|p| p.log_index == *index) {
                            let proposal = proposals.swap_remove(pos);
                            let _ = proposal.result_tx.send(Ok(()));
                            info!(
                                index = index.get(),
                                "Notified pending controller proposal"
                            );
                        } else {
                            info!(
                                index = index.get(),
                                "No pending proposal found for this index"
                            );
                        }
                    }

                    // Propose follow-up commands if we're the leader.
                    if !follow_ups.is_empty() {
                        let is_leader = {
                            let mr = multi_raft.read().await;
                            mr.group_state(CONTROLLER_GROUP_ID)
                                .is_some_and(|s| s.state == RaftState::Leader)
                        };

                        if is_leader {
                            let mut mr = multi_raft.write().await;
                            for follow_up in follow_ups {
                                let encoded = follow_up.encode();
                                if mr.propose(CONTROLLER_GROUP_ID, encoded).is_none() {
                                    warn!(
                                        command = ?follow_up,
                                        "Failed to propose follow-up controller command"
                                    );
                                }
                            }
                        }
                    }

                    // Handle AssignPartition by creating the data Raft group.
                    if let ControllerCommand::AssignPartition {
                        topic_id,
                        partition_id,
                        group_id: data_group_id,
                        ref replicas,
                    } = cmd
                    {
                        let node_id = {
                            let mr = multi_raft.read().await;
                            mr.node_id()
                        };

                        if replicas.contains(&node_id) {
                            // Look up any persisted vote state for this partition.
                            let (term, voted_for, observation_mode) = vote_store
                                .and_then(|vs| vs.lock().ok())
                                .and_then(|store| {
                                    store.state().get_group(data_group_id).map(|v| (v.term, v.voted_for, false))
                                })
                                .unwrap_or((TermId::new(0), None, false));

                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                term = term.get(),
                                voted_for = voted_for.map(NodeId::get),
                                replicas = ?replicas.iter().map(|n| n.get()).collect::<Vec<_>>(),
                                "Creating data partition from controller assignment"
                            );

                            // Create the data Raft group with restored vote state if available.
                            let mut mr = multi_raft.write().await;
                            let existing_groups = mr.group_ids();
                            if !existing_groups.contains(&data_group_id) {
                                let create_result = mr.create_group_with_state(
                                    data_group_id,
                                    replicas.clone(),
                                    term,
                                    voted_for,
                                    observation_mode,
                                );
                                if let Err(e) = create_result {
                                    warn!(
                                        error = %e,
                                        group = data_group_id.get(),
                                        "Failed to create data Raft group"
                                    );
                                }
                            }
                            drop(mr);

                            // Update group map.
                            {
                                let mut gm = group_map.write().await;
                                gm.insert(topic_id, partition_id, data_group_id);
                            }

                            // Create partition storage (durable if data_dir is set).
                            {
                                let mut storage = partition_storage.write().await;
                                if let std::collections::hash_map::Entry::Vacant(e) = storage.entry(data_group_id) {
                                    let ps = if let Some(ref pool) = shared_wal_pool {
                                        // Shared WAL mode: get handle from pool and recovered entries.
                                        let dir = data_dir.as_ref().expect("data_dir must be set with shared_wal_pool");
                                        let wal_handle = pool.handle(partition_id);
                                        let recovered = recovered_entries
                                            .write()
                                            .await
                                            .remove(&partition_id)
                                            .unwrap_or_default();

                                        #[cfg(feature = "s3")]
                                        let ps_result = ServerPartitionStorage::new_durable_with_shared_wal(
                                            dir,
                                            topic_id,
                                            partition_id,
                                            wal_handle,
                                            recovered,
                                            object_storage_dir.as_ref(),
                                            s3_config.as_ref(),
                                            tiering_config.as_ref(),
                                        ).await;
                                        #[cfg(not(feature = "s3"))]
                                        let ps_result = ServerPartitionStorage::new_durable_with_shared_wal(
                                            dir,
                                            topic_id,
                                            partition_id,
                                            wal_handle,
                                            recovered,
                                            object_storage_dir.as_ref(),
                                            tiering_config.as_ref(),
                                        ).await;

                                        match ps_result {
                                            Ok(durable) => durable,
                                            Err(e) => {
                                                error!(
                                                    topic = topic_id.get(),
                                                    partition = partition_id.get(),
                                                    error = %e,
                                                    "Failed to create partition with shared WAL, falling back to in-memory"
                                                );
                                                ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                                            }
                                        }
                                    } else {
                                        // Dedicated WAL mode (used when shared WAL is not available).
                                        #[cfg(feature = "s3")]
                                        let ps_inner = if let Some(dir) = data_dir {
                                            match ServerPartitionStorage::new_durable(
                                                TokioStorage::new(),
                                                dir,
                                                object_storage_dir.as_ref(),
                                                s3_config.as_ref(),
                                                tiering_config.as_ref(),
                                                topic_id,
                                                partition_id,
                                            ).await {
                                                Ok(durable) => durable,
                                                Err(e) => {
                                                    error!(
                                                        topic = topic_id.get(),
                                                        partition = partition_id.get(),
                                                        error = %e,
                                                        "Failed to create durable partition, falling back to in-memory"
                                                    );
                                                    ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                                                }
                                            }
                                        } else {
                                            ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                                        };
                                        #[cfg(not(feature = "s3"))]
                                        let ps_inner = if let Some(dir) = data_dir {
                                            match ServerPartitionStorage::new_durable(
                                                TokioStorage::new(),
                                                dir,
                                                object_storage_dir.as_ref(),
                                                tiering_config.as_ref(),
                                                topic_id,
                                                partition_id,
                                            ).await {
                                                Ok(durable) => durable,
                                                Err(e) => {
                                                    error!(
                                                        topic = topic_id.get(),
                                                        partition = partition_id.get(),
                                                        error = %e,
                                                        "Failed to create durable partition, falling back to in-memory"
                                                    );
                                                    ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                                                }
                                            }
                                        } else {
                                            ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                                        };
                                        ps_inner
                                    };
                                    e.insert(Arc::new(RwLock::new(ps)));
                                }
                            }
                        }
                    }
                    continue;
                }

                // Regular data partition commit.
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };

                if let Some((topic_id, partition_id)) = key {
                    // Check for batch pending proposal first.
                    let batch_proposal = {
                        let inner_lock = {
                            let batch_proposals = batch_pending_proposals.read().await;
                            batch_proposals.get(group_id).cloned()
                        };
                        if let Some(inner_lock) = inner_lock {
                            let mut inner = inner_lock.write().await;
                            inner.remove(index)
                        } else {
                            None
                        }
                    };

                    // Also check for single pending proposal to determine entry type.
                    let has_single_proposal = {
                        let inner_lock = {
                            let proposals = pending_proposals.read().await;
                            proposals.get(group_id).cloned()
                        };
                        if let Some(inner_lock) = inner_lock {
                            let inner = inner_lock.read().await;
                            inner.contains_key(index)
                        } else {
                            false
                        }
                    };

                    // Log the offset BEFORE applying to detect interleaving.
                    let offset_before = {
                        let ps_lock = {
                            let storage = partition_storage.read().await;
                            storage.get(group_id).cloned()
                        };
                        if let Some(ps_lock) = ps_lock {
                            let ps = ps_lock.read().await;
                            ps.blob_log_end_offset().get()
                        } else {
                            0
                        }
                    };

                    // Determine entry type for logging
                    let entry_type = if batch_proposal.is_some() {
                        "BATCH_CLIENT"
                    } else if has_single_proposal {
                        "SINGLE_CLIENT"
                    } else {
                        "PREVIOUS_TERM"  // No pending proposal = replicated from old leader
                    };

                    info!(
                        node = node_id.get(),
                        group = group_id.get(),
                        index = index.get(),
                        entry_type = entry_type,
                        offset_before = offset_before,
                        has_batch_proposal = batch_proposal.is_some(),
                        has_single_proposal = has_single_proposal,
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        "ENTRY_TYPE: determined entry type before apply"
                    );

                    if let Some(batch_proposal) = batch_proposal {
                        // DEFENSIVE ASSERTION: Verify the batch_proposal's stored index matches
                        // the CommitEntry index. If these don't match, there's a bug in the lookup.
                        if batch_proposal.log_index != *index {
                            error!(
                                node = node_id.get(),
                                group = group_id.get(),
                                commit_index = index.get(),
                                stored_index = batch_proposal.log_index.get(),
                                "BUG: batch_proposal log_index mismatch! This indicates a lookup error."
                            );
                        }

                        // Batch proposal path: apply and use the returned base_offset.
                        // The apply_entry_async function returns the actual base_offset used,
                        // which is captured atomically during the apply operation.
                        let apply_result: Result<Option<Offset>, ServerError> = {
                            let ps_lock = {
                                let storage = partition_storage.read().await;
                                storage.get(group_id).cloned()
                            };
                            if let Some(ps_lock) = ps_lock {
                                let mut ps = ps_lock.write().await;
                                let result = ps.apply_entry_async(*index, data).await;
                                let offset_after = ps.blob_log_end_offset().get();
                                info!(
                                    node = node_id.get(),
                                    group = group_id.get(),
                                    index = index.get(),
                                    offset_before = offset_before,
                                    offset_after = offset_after,
                                    returned_offset = ?result.as_ref().ok().and_then(|o| o.map(helix_core::Offset::get)),
                                    "BATCH: apply_entry_async completed"
                                );
                                result
                            } else {
                                Err(ServerError::PartitionNotFound {
                                    topic: topic_id.get().to_string(),
                                    partition: i32::try_from(partition_id.get()).unwrap_or(0),
                                })
                            }
                        };

                        // Notify each waiter with the offset returned by apply.
                        let batch_size = batch_proposal.record_counts.len();
                        match apply_result {
                            Ok(Some(base_offset)) => {
                                // DEFENSIVE CHECK: The offset returned by apply should match
                                // what we observed before apply. If they differ, something else
                                // modified storage (indicates a race condition or bug).
                                if base_offset.get() != offset_before {
                                    // Safety: offsets are u64 but in practice will never exceed
                                    // i64::MAX (9 exabytes). Cast to i64 for signed delta display.
                                    #[allow(clippy::cast_possible_wrap)]
                                    let delta = base_offset.get() as i64 - offset_before as i64;
                                    error!(
                                        node = node_id.get(),
                                        group = group_id.get(),
                                        index = index.get(),
                                        offset_before = offset_before,
                                        base_offset = base_offset.get(),
                                        delta,
                                        "BUG: apply returned different offset than expected! \
                                         Storage was modified between offset capture and apply."
                                    );
                                }

                                // Extract and record producer state from committed entry.
                                // Critical for PREVIOUS_TERM entries on new leader.
                                extract_and_record_producer_state(
                                    data,
                                    base_offset,
                                    *group_id,
                                    partition_storage,
                                )
                                .await;

                                let commit_latency = batch_proposal.proposed_at.elapsed();
                                let total_age = batch_proposal.first_request_at.elapsed();
                                let batch_wait = batch_proposal
                                    .proposed_at
                                    .duration_since(batch_proposal.first_request_at);
                                if let Some(stats) = batcher_stats.as_ref() {
                                    // Safety: durations will never exceed u64::MAX microseconds
                                    // (would require running for millions of years).
                                    #[allow(clippy::cast_possible_truncation)]
                                    stats.record_commit(
                                        u64::from(batch_proposal.batch_size),
                                        u64::from(batch_proposal.batch_bytes),
                                        batch_proposal.total_records,
                                        batch_wait.as_micros() as u64,
                                        commit_latency.as_micros() as u64,
                                        total_age.as_micros() as u64,
                                    );
                                }
                                // Extract payload preview for diagnostic logging.
                                let commit_payload = extract_commit_payload_preview(data);

                                let mut cumulative = 0u64;
                                for (i, (record_count, result_tx)) in batch_proposal
                                    .record_counts
                                    .iter()
                                    .zip(batch_proposal.result_txs)
                                    .enumerate()
                                {
                                    let offset = Offset::new(base_offset.get() + cumulative);
                                    debug!(
                                        node = node_id.get(),
                                        group = group_id.get(),
                                        index = index.get(),
                                        waiter_index = i,
                                        base_offset = base_offset.get(),
                                        cumulative = cumulative,
                                        sent_offset = offset.get(),
                                        record_count = record_count,
                                        commit_payload = %commit_payload,
                                        "NOTIFY_CLIENT: sending offset to waiter"
                                    );
                                    let _ = result_tx.send(Ok(offset));
                                    cumulative += u64::from(*record_count);
                                }
                                // Decrement backpressure counters now that requests are complete.
                                if let Some(ref bp) = batcher_backpressure {
                                    bp.pending_requests.fetch_sub(
                                        u64::from(batch_proposal.batch_size),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    bp.pending_bytes.fetch_sub(
                                        u64::from(batch_proposal.batch_bytes),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                // Safety: durations will never exceed u64::MAX microseconds
                                // (would require running for millions of years).
                                #[allow(clippy::cast_possible_truncation)]
                                {
                                    info!(
                                        group = group_id.get(),
                                        index = index.get(),
                                        batch_size = batch_proposal.batch_size,
                                        batch_bytes = batch_proposal.batch_bytes,
                                        total_records = batch_proposal.total_records,
                                        batch_wait_us = batch_wait.as_micros() as u64,
                                        commit_latency_us = commit_latency.as_micros() as u64,
                                        total_age_us = total_age.as_micros() as u64,
                                        "Batch committed"
                                    );
                                }
                                debug!(
                                    group = group_id.get(),
                                    index = index.get(),
                                    batch_size,
                                    base_offset = base_offset.get(),
                                    "Notified batch pending proposal"
                                );
                            }
                            Ok(None) => {
                                // Entry was already applied or empty - shouldn't happen for batches.
                                // Decrement backpressure and notify waiters with error.
                                if let Some(ref bp) = batcher_backpressure {
                                    bp.pending_requests.fetch_sub(
                                        u64::from(batch_proposal.batch_size),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    bp.pending_bytes.fetch_sub(
                                        u64::from(batch_proposal.batch_bytes),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                let err = ServerError::Internal {
                                    message: "batch apply returned no offset (already applied or empty)".to_string(),
                                };
                                for result_tx in batch_proposal.result_txs {
                                    let _ = result_tx.send(Err(err.clone()));
                                }
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    group = group_id.get(),
                                    index = index.get(),
                                    "Batch apply returned None - entry was already applied or empty"
                                );
                            }
                            Err(ref e) => {
                                // Decrement backpressure counters now that requests are complete (failed).
                                if let Some(ref bp) = batcher_backpressure {
                                    bp.pending_requests.fetch_sub(
                                        u64::from(batch_proposal.batch_size),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    bp.pending_bytes.fetch_sub(
                                        u64::from(batch_proposal.batch_bytes),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                }
                                if let Some(stats) = batcher_stats.as_ref() {
                                    stats.record_apply_error();
                                }
                                let err = ServerError::Internal {
                                    message: format!("batch apply failed: {e}"),
                                };
                                for result_tx in batch_proposal.result_txs {
                                    let _ = result_tx.send(Err(err.clone()));
                                }
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    error = %e,
                                    "Failed to apply batch committed entry"
                                );
                            }
                        }
                    } else {
                        // Single proposal path (existing logic) or PREVIOUS_TERM entry.
                        // Apply first, then use the returned offset for everything.
                        // This eliminates the TOCTOU race where base_offset could be
                        // captured before concurrent PREVIOUS_TERM entries are applied.
                        let apply_result: Result<Option<Offset>, ServerError> = {
                            let ps_lock = {
                                let storage = partition_storage.read().await;
                                storage.get(group_id).cloned()
                            };
                            if let Some(ps_lock) = ps_lock {
                                let mut ps = ps_lock.write().await;
                                let result = ps.apply_entry_async(*index, data).await;
                                let offset_after = ps.blob_log_end_offset().get();
                                info!(
                                    node = node_id.get(),
                                    group = group_id.get(),
                                    index = index.get(),
                                    offset_before = offset_before,
                                    offset_after = offset_after,
                                    returned_offset = ?result.as_ref().ok().and_then(|o| o.map(helix_core::Offset::get)),
                                    "NON-BATCH: apply_entry_async completed"
                                );
                                result
                            } else {
                                Err(ServerError::PartitionNotFound {
                                    topic: topic_id.get().to_string(),
                                    partition: i32::try_from(partition_id.get()).unwrap_or(0),
                                })
                            }
                        };

                        // Extract and record producer state from committed entry.
                        // Critical for PREVIOUS_TERM entries on new leader.
                        // Use the offset returned by apply (not pre-captured) to avoid races.
                        if let Ok(Some(applied_offset)) = &apply_result {
                            extract_and_record_producer_state(
                                data,
                                *applied_offset,
                                *group_id,
                                partition_storage,
                            )
                            .await;
                        }

                        // Find and notify any pending proposal (O(1) lookup).
                        let inner_lock = {
                            let proposals = pending_proposals.read().await;
                            proposals.get(group_id).cloned()
                        };
                        let had_proposal = if let Some(ref inner_lock) = inner_lock {
                            let inner = inner_lock.read().await;
                            inner.contains_key(index)
                        } else {
                            false
                        };
                        if let Some(inner_lock) = inner_lock {
                            let mut group_proposals = inner_lock.write().await;
                            if let Some(proposal) = group_proposals.remove(index) {
                                let result = match &apply_result {
                                    Ok(Some(offset)) => {
                                        debug!(
                                            topic = topic_id.get(),
                                            partition = partition_id.get(),
                                            group = group_id.get(),
                                            index = index.get(),
                                            offset = offset.get(),
                                            "Notifying pending proposal"
                                        );
                                        Ok(*offset)
                                    }
                                    Ok(None) => {
                                        // Apply returned None - entry was empty or already applied.
                                        // This should NOT happen for client entries with pending proposals.
                                        // Log an error and return a failure instead of guessing offset.
                                        error!(
                                            topic = topic_id.get(),
                                            partition = partition_id.get(),
                                            group = group_id.get(),
                                            index = index.get(),
                                            "BUG: apply returned None for entry with pending proposal"
                                        );
                                        Err(ServerError::Internal {
                                            message: "apply returned None for client entry".to_string(),
                                        })
                                    }
                                    Err(e) => Err(ServerError::Internal {
                                        message: format!("apply failed: {e}"),
                                    }),
                                };
                                if proposal.result_tx.send(result).is_err() {
                                    debug!(
                                        group = group_id.get(),
                                        index = index.get(),
                                        "Pending proposal receiver dropped"
                                    );
                                }
                            }
                        }

                        // Log PREVIOUS_TERM entries that consumed offsets without client notification.
                        // These are critical because they "steal" offsets from subsequent client entries.
                        if !had_proposal {
                            if let Ok(Some(offset)) = &apply_result {
                                let payload = extract_commit_payload_preview(data);
                                debug!(
                                    node = node_id.get(),
                                    group = group_id.get(),
                                    index = index.get(),
                                    offset = offset.get(),
                                    payload = %payload,
                                    "PREVIOUS_TERM: applied entry without client notification"
                                );
                            }
                        }

                        if let Err(e) = apply_result {
                            warn!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                error = %e,
                                "Failed to apply committed entry"
                            );
                        }
                    }
                }
            }
            MultiRaftOutput::BecameLeader { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Became controller leader");
                    continue;
                }

                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Became leader"
                    );
                }
            }
            MultiRaftOutput::SteppedDown { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Stepped down from controller leader");
                    continue;
                }

                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Stepped down from leader"
                    );
                }
            }
            MultiRaftOutput::SendMessages { to, messages } => {
                if let Err(e) = transport_handle.send_batch(*to, messages.clone()).await {
                    error!(
                        to = to.get(),
                        count = messages.len(),
                        error = %e,
                        "Failed to send messages to peer"
                    );
                } else {
                    debug!(
                        to = to.get(),
                        count = messages.len(),
                        "Sent messages to peer"
                    );
                }
            }
            MultiRaftOutput::VoteStateChanged { group_id, term, voted_for } => {
                debug!(
                    group = group_id.get(),
                    term = term.get(),
                    voted_for = ?voted_for.map(helix_core::NodeId::get),
                    "Vote state changed (multi-node)"
                );
                // Persist vote state to local file + async S3 backup.
                if let Some(vs) = vote_store {
                    if let Ok(mut store) = vs.lock() {
                        if let Err(e) = store.save(*group_id, *term, *voted_for) {
                            error!(
                                group = group_id.get(),
                                error = %e,
                                "Failed to persist vote state"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Processes tiering for all durable partitions.
///
/// This function iterates over all partition storage and performs tiering
/// operations for those with tiering enabled:
/// 1. Registers newly sealed segments with the tiering manager
/// 2. Uploads eligible segments to object storage (S3/filesystem)
///
/// # Arguments
///
/// * `partition_storage` - Map of group ID to partition storage
#[allow(clippy::significant_drop_tightening)]
async fn process_tiering(
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
) {
    // Collect group IDs of partitions with tiering enabled.
    // Note: We hold the outer read lock while checking has_tiering() on each partition.
    // This is intentional to get a consistent snapshot of which partitions have tiering.
    let tiering_groups: Vec<(GroupId, Arc<RwLock<ServerPartitionStorage>>)> = {
        let storage = partition_storage.read().await;
        let mut groups = Vec::new();
        for (group_id, ps_lock) in storage.iter() {
            let ps = ps_lock.read().await;
            if ps.has_tiering() {
                groups.push((*group_id, ps_lock.clone()));
            }
        }
        groups
    };

    if tiering_groups.is_empty() {
        return;
    }

    debug!(
        partition_count = tiering_groups.len(),
        "Processing tiering for partitions"
    );

    // `TigerStyle`: bounded iteration.
    for (group_id, ps_lock) in tiering_groups.iter().take(100) {
        // Register newly sealed segments.
        {
            let mut ps = ps_lock.write().await;
            match ps.check_and_register_sealed_segments().await {
                Ok(count) if count > 0 => {
                    info!(
                        group = group_id.get(),
                        registered = count,
                        "Registered sealed segments for tiering"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        group = group_id.get(),
                        error = %e,
                        "Failed to register sealed segments"
                    );
                }
            }
        }

        // Upload eligible segments.
        {
            let ps = ps_lock.read().await;
            match ps.tier_eligible_segments().await {
                Ok(count) if count > 0 => {
                    info!(
                        group = group_id.get(),
                        tiered = count,
                        "Tiered segments to object storage"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        group = group_id.get(),
                        error = %e,
                        "Failed to tier eligible segments"
                    );
                }
            }
        }
    }
}

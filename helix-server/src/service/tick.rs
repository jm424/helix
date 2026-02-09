//! Background tick task for Raft consensus.
//!
//! This module handles periodic ticks and message processing for Raft groups.

// Allow complex nested types for proposal maps - refactoring would require significant API changes.
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use helix_core::{GroupId, LogIndex, NodeId, Offset, PartitionId, TermId};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use helix_runtime::{BrokerHeartbeat, IncomingMessage, TransportService};
use helix_wal::{SharedEntry, SharedWalPool, Storage};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::vote_store::{LocalFileVoteStorage, VoteStore};

use crate::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::partition_storage::PartitionStorage;
use super::{
    output_processor::extract_and_record_producer_state, PendingControllerProposal,
    PendingProposal, TICK_INTERVAL_MS,
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

/// Background task to handle Raft ticks for all groups (single-node).
#[allow(clippy::significant_drop_tightening, clippy::implicit_hasher)]
pub async fn tick_task<S: Storage + Clone + Send + Sync + 'static>(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
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

// Legacy non-actor tick tasks were removed in ADR-0006.
// All multi-node operation now uses tick_task_actor + tick_task_controller.

/// Background tick task for actor-based multi-node operation.
///
/// Routes ticks and messages to partition actors via the `PartitionRouter`,
/// eliminating lock contention. The controller partition is handled separately
/// by `tick_task_controller`.
///
/// # Responsibilities
///
/// 1. **Tick broadcast**: Periodically calls `router.tick_all()` to tick all
///    partition actors in parallel.
/// 2. **Message routing**: Routes incoming Raft messages from transport to
///    the appropriate partition actors via `router.route_messages()`.
/// 3. **Heartbeats**: Sends broker heartbeats to all peers (Kafka `KRaft` pattern).
/// 4. **Tiering**: Processes tiering for durable partitions.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn tick_task_actor<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    router: Arc<super::router::PartitionRouter>,
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: Vec<NodeId>,
    transport_handle: T,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<Arc<SharedWalPool<S>>>,
    data_dir: Option<PathBuf>,
    recovered_entries: Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>,
    storage: S,
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
                // Heartbeats are soft state that doesn't require Raft consensus.
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
                                shared_wal_pool.as_ref(),
                                data_dir.as_ref(),
                                Some(&recovered_entries),
                                Some(&storage),
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
pub async fn tick_task_controller<
    S: Storage + Clone + Send + Sync + 'static,
    T: TransportService,
>(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: Vec<NodeId>,
    transport_handle: T,
    router: Arc<super::router::PartitionRouter>,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<Arc<SharedWalPool<S>>>,
    data_dir: Option<PathBuf>,
    recovered_entries: Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>,
    storage: S,
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
        "Controller tick task started"
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
                    shared_wal_pool.as_ref(),
                    data_dir.as_ref(),
                    Some(&recovered_entries),
                    Some(&storage),
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
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::significant_drop_tightening,
    clippy::implicit_hasher
)]
pub async fn process_controller_outputs<
    S: Storage + Clone + Send + Sync + 'static,
    T: TransportService,
>(
    outputs: &[MultiRaftOutput],
    multi_raft: &Arc<RwLock<MultiRaft>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    _pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: &Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: &[NodeId],
    transport_handle: &T,
    router: &Arc<super::router::PartitionRouter>,
    output_tx: &mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<&Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    data_dir: Option<&PathBuf>,
    recovered_entries: Option<&Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>>,
    storage: Option<&S>,
) {
    // Collect follow-up outputs for single-node processing.
    // For single-node clusters, propose() returns CommitEntry immediately,
    // so we need to process those after the main loop.
    let mut all_follow_up_outputs: Vec<MultiRaftOutput> = Vec::new();

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
                    warn!(index = index.get(), "Failed to decode controller command");
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
                    let pending_indexes: Vec<u64> =
                        proposals.iter().map(|p| p.log_index.get()).collect();
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
                // For single-node clusters, these commits happen immediately during propose,
                // so we collect the outputs and process them after the loop.
                if !follow_ups.is_empty() {
                    let is_leader = {
                        let mr = multi_raft.read().await;
                        mr.group_state(CONTROLLER_GROUP_ID)
                            .is_some_and(|s| s.state == RaftState::Leader)
                    };

                    if is_leader {
                        let mut mr = multi_raft.write().await;
                        for follow_up in &follow_ups {
                            let encoded = follow_up.encode();
                            match mr.propose(CONTROLLER_GROUP_ID, encoded) {
                                None => {
                                    warn!(
                                        command = ?follow_up,
                                        "Failed to propose follow-up controller command (actor mode)"
                                    );
                                }
                                Some(outputs) => {
                                    // Collect outputs for single-node processing.
                                    all_follow_up_outputs.extend(outputs);
                                }
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
                                store
                                    .state()
                                    .get_group(data_group_id)
                                    .map(|v| (v.term, v.voted_for, false))
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

                        // Create partition storage (durable if SharedWalPool available).
                        {
                            let mut ps_map = partition_storage.write().await;
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                ps_map.entry(data_group_id)
                            {
                                let ps = create_partition_storage(
                                    topic_id,
                                    partition_id,
                                    shared_wal_pool,
                                    data_dir,
                                    recovered_entries,
                                    storage,
                                )
                                .await;
                                e.insert(Arc::new(RwLock::new(ps)));
                            }
                        }

                        // Create the partition actor with restored vote state and add it to the router.
                        let partition_handle =
                            super::actor_setup::create_partition_actor_with_state(
                                data_group_id,
                                node_id,
                                replicas.clone(),
                                term,
                                voted_for,
                                observation_mode,
                                output_tx.clone(),
                                super::partition_actor::PartitionActorConfig::default(),
                            );

                        if router
                            .add_partition_dynamic(data_group_id, partition_handle)
                            .await
                        {
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
                    if let Err(e) = transport_handle
                        .send_batch(*to, controller_messages.clone())
                        .await
                    {
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
            MultiRaftOutput::VoteStateChanged {
                group_id,
                term,
                voted_for,
            } => {
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

    // Process follow-up outputs (for single-node clusters).
    // These are commits that happened during propose of follow-up commands.
    if !all_follow_up_outputs.is_empty() {
        for output in &all_follow_up_outputs {
            if let MultiRaftOutput::CommitEntry {
                group_id,
                index,
                data,
            } = output
            {
                if *group_id != CONTROLLER_GROUP_ID {
                    continue;
                }

                let Some(cmd) = ControllerCommand::decode(data) else {
                    warn!(
                        index = index.get(),
                        "Failed to decode follow-up controller command"
                    );
                    continue;
                };

                // Apply to controller state (for AssignPartition, this is a no-op since already applied).
                {
                    let mut state = controller_state.write().await;
                    let _ = state.apply(&cmd, cluster_nodes);
                }

                // Handle AssignPartition by creating partition actor.
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
                        // Update group map.
                        {
                            let mut gm = group_map.write().await;
                            gm.insert(topic_id, partition_id, data_group_id);
                        }

                        // Create partition storage (durable if SharedWalPool available).
                        {
                            let mut ps_map = partition_storage.write().await;
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                ps_map.entry(data_group_id)
                            {
                                let ps = create_partition_storage(
                                    topic_id,
                                    partition_id,
                                    shared_wal_pool,
                                    data_dir,
                                    recovered_entries,
                                    storage,
                                )
                                .await;
                                e.insert(Arc::new(RwLock::new(ps)));
                            }
                        }

                        // Create the partition actor and add to router.
                        let partition_handle =
                            super::actor_setup::create_partition_actor_with_state(
                                data_group_id,
                                node_id,
                                replicas.clone(),
                                TermId::new(0),
                                None,
                                false,
                                output_tx.clone(),
                                super::partition_actor::PartitionActorConfig::default(),
                            );

                        if router
                            .add_partition_dynamic(data_group_id, partition_handle)
                            .await
                        {
                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Created partition actor from follow-up (single-node)"
                            );
                        } else {
                            warn!(
                                group = data_group_id.get(),
                                "Follow-up partition actor already exists"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Creates partition storage using the best available durability mode:
/// 1. Shared WAL pool (when `shared_wal_pool` is set) — fsync amortization
/// 2. Per-partition dedicated WAL (when `data_dir` is set) — simpler, one WAL per partition
/// 3. In-memory (fallback) — no durability
async fn create_partition_storage<S: Storage + Clone + Send + Sync + 'static>(
    topic_id: helix_core::TopicId,
    partition_id: PartitionId,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    data_dir: Option<&PathBuf>,
    recovered_entries: Option<&Arc<RwLock<HashMap<PartitionId, Vec<SharedEntry>>>>>,
    storage: Option<&S>,
) -> PartitionStorage<S> {
    // Mode 1: Shared WAL pool.
    if let (Some(pool), Some(dir)) = (shared_wal_pool, data_dir) {
        let wal_handle = pool.handle(partition_id);
        let recovered = if let Some(entries) = recovered_entries {
            entries
                .write()
                .await
                .remove(&partition_id)
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        #[cfg(feature = "s3")]
        let result = PartitionStorage::new_durable_with_shared_wal(
            dir,
            topic_id,
            partition_id,
            wal_handle,
            recovered,
            None, // object_storage_dir
            None, // s3_config
            None, // tiering_config
        )
        .await;
        #[cfg(not(feature = "s3"))]
        let result = PartitionStorage::new_durable_with_shared_wal(
            dir,
            topic_id,
            partition_id,
            wal_handle,
            recovered,
            None, // object_storage_dir
            None, // tiering_config
        )
        .await;

        return match result {
            Ok(ps) => {
                info!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    "Created durable partition storage via SharedWalPool"
                );
                ps
            }
            Err(e) => {
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to create shared WAL storage, falling back to in-memory"
                );
                PartitionStorage::new_in_memory(topic_id, partition_id)
            }
        };
    }

    // Mode 2: Per-partition dedicated WAL.
    if let (Some(dir), Some(storage)) = (data_dir, storage) {
        #[cfg(feature = "s3")]
        let result = PartitionStorage::new_durable(
            storage.clone(),
            dir,
            None, // object_storage_dir
            None, // s3_config
            None, // tiering_config
            topic_id,
            partition_id,
        )
        .await;
        #[cfg(not(feature = "s3"))]
        let result = PartitionStorage::new_durable(
            storage.clone(),
            dir,
            None, // object_storage_dir
            None, // tiering_config
            topic_id,
            partition_id,
        )
        .await;

        return match result {
            Ok(ps) => {
                info!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    "Created durable partition storage with per-partition WAL"
                );
                ps
            }
            Err(e) => {
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to create per-partition WAL storage, falling back to in-memory"
                );
                PartitionStorage::new_in_memory(topic_id, partition_id)
            }
        };
    }

    // Mode 3: In-memory.
    PartitionStorage::new_in_memory(topic_id, partition_id)
}

/// Sends broker heartbeats to all peers via transport (Kafka `KRaft` pattern).
///
/// Unlike Raft-replicated state, heartbeats are soft state. Each broker:
/// 1. Records its own heartbeat locally
/// 2. Sends heartbeats to all other nodes via transport
///
/// This ensures all nodes have a local view of broker liveness without requiring
/// Raft consensus, which would only work for the leader node.
async fn send_broker_heartbeats_to_peers<T: TransportService>(
    local_broker_heartbeats: &Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: &[NodeId],
    transport_handle: &T,
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
async fn process_outputs<S: Storage + Clone + Send + Sync + 'static>(
    outputs: &[MultiRaftOutput],
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
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
            MultiRaftOutput::VoteStateChanged {
                group_id,
                term,
                voted_for,
            } => {
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
async fn process_tiering<S: Storage + Clone + Send + Sync + 'static>(
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
) {
    // Collect group IDs of partitions with tiering enabled.
    // Note: We hold the outer read lock while checking has_tiering() on each partition.
    // This is intentional to get a consistent snapshot of which partitions have tiering.
    let tiering_groups: Vec<(GroupId, Arc<RwLock<PartitionStorage<S>>>)> = {
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

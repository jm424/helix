//! Background tick task for Raft consensus.
//!
//! This module handles periodic ticks and message processing for Raft groups.

use std::collections::HashMap;
use std::sync::Arc;

use helix_core::{GroupId, NodeId, Offset};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use helix_runtime::{BrokerHeartbeat, IncomingMessage, TransportHandle};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::partition_storage::PartitionStorage;

use super::{PendingControllerProposal, PendingProposal, TICK_INTERVAL_MS};

/// Interval for sending broker heartbeats to the controller (in milliseconds).
const HEARTBEAT_INTERVAL_MS: u64 = 3_000; // 3 seconds.

/// Background task to handle Raft ticks for all groups (single-node).
#[allow(clippy::significant_drop_tightening)]
pub async fn tick_task(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
    group_map: Arc<RwLock<GroupMap>>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));

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
                ).await;
            }
        }
    }
}

/// Background task for multi-node operation.
#[allow(clippy::significant_drop_tightening)]
#[allow(clippy::too_many_arguments)]
pub async fn tick_task_multi_node(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Vec<PendingProposal>>>>,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    cluster_nodes: Vec<NodeId>,
    transport_handle: TransportHandle,
    mut incoming_rx: mpsc::Receiver<IncomingMessage>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut heartbeat_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(HEARTBEAT_INTERVAL_MS));

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
                process_outputs_multi_node(
                    &outputs,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &cluster_nodes,
                    &transport_handle,
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
                process_outputs_multi_node(
                    &outputs,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &cluster_nodes,
                    &transport_handle,
                ).await;
            }
        }
    }
}

/// Sends broker heartbeats to all peers via transport (Kafka KRaft pattern).
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
async fn process_outputs(
    outputs: &[MultiRaftOutput],
    partition_storage: &Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
    group_map: &Arc<RwLock<GroupMap>>,
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

                if let Some((topic_id, partition_id)) = key {
                    let mut storage = partition_storage.write().await;
                    if let Some(ps) = storage.get_mut(group_id) {
                        if let Err(e) = ps.apply_entry_async(*index, data).await {
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
        }
    }
}

/// Processes Multi-Raft outputs with transport for sending messages.
#[allow(clippy::too_many_arguments)]
async fn process_outputs_multi_node(
    outputs: &[MultiRaftOutput],
    multi_raft: &Arc<RwLock<MultiRaft>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, PartitionStorage>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    pending_proposals: &Arc<RwLock<HashMap<GroupId, Vec<PendingProposal>>>>,
    pending_controller_proposals: &Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: &[NodeId],
    transport_handle: &TransportHandle,
) {
    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                data,
            } => {
                // Check if this is a controller partition commit.
                if *group_id == CONTROLLER_GROUP_ID {
                    let Some(cmd) = ControllerCommand::decode(data) else {
                        warn!(
                            index = index.get(),
                            "Failed to decode controller command"
                        );
                        continue;
                    };

                    debug!(
                        index = index.get(),
                        command = ?cmd,
                        "Applying controller command"
                    );
                    let mut state = controller_state.write().await;
                    let follow_ups = state.apply(&cmd, cluster_nodes);

                    // Notify any pending controller proposals for this index.
                    {
                        let mut proposals = pending_controller_proposals.write().await;
                        if let Some(pos) = proposals.iter().position(|p| p.log_index == *index) {
                            let proposal = proposals.swap_remove(pos);
                            let _ = proposal.result_tx.send(Ok(()));
                            debug!(
                                index = index.get(),
                                "Notified pending controller proposal"
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
                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                replicas = ?replicas.iter().map(|n| n.get()).collect::<Vec<_>>(),
                                "Creating data partition from controller assignment"
                            );

                            // Create the data Raft group if it doesn't exist.
                            let mut mr = multi_raft.write().await;
                            let existing_groups = mr.group_ids();
                            if !existing_groups.contains(&data_group_id) {
                                if let Err(e) = mr.create_group(data_group_id, replicas.clone()) {
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

                            // Create partition storage.
                            {
                                let mut storage = partition_storage.write().await;
                                storage.entry(data_group_id).or_insert_with(|| PartitionStorage::new_in_memory(topic_id, partition_id));
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
                    let apply_result = {
                        let mut storage = partition_storage.write().await;
                        if let Some(ps) = storage.get_mut(group_id) {
                            ps.apply_entry_async(*index, data).await
                        } else {
                            Err(ServerError::PartitionNotFound {
                                topic: topic_id.get().to_string(),
                                partition: i32::try_from(partition_id.get()).unwrap_or(0),
                            })
                        }
                    };

                    // Find and notify any pending proposal.
                    let mut proposals = pending_proposals.write().await;
                    if let Some(group_proposals) = proposals.get_mut(group_id) {
                        if let Some(pos) = group_proposals
                            .iter()
                            .position(|p| p.log_index == *index)
                        {
                            let proposal = group_proposals.swap_remove(pos);
                            let result = match &apply_result {
                                Ok(Some(offset)) => {
                                    eprintln!("[NOTIFY] topic={} partition={} group_id={} index={} apply_result=Some({}) -> sending offset={}",
                                        topic_id, partition_id, group_id, index, offset, offset);
                                    Ok(*offset)
                                }
                                Ok(None) => {
                                    let storage = partition_storage.read().await;
                                    let offset = storage
                                        .get(group_id)
                                        .map_or(Offset::new(0), super::super::partition_storage::PartitionStorage::blob_log_end_offset);
                                    eprintln!("[NOTIFY] topic={} partition={} group_id={} index={} apply_result=None -> sending blob_log_end_offset={}",
                                        topic_id, partition_id, group_id, index, offset);
                                    Ok(offset)
                                }
                                Err(e) => Err(ServerError::Internal {
                                    message: format!("apply failed: {e}"),
                                }),
                            };
                            let send_result = proposal.result_tx.send(result);
                            if send_result.is_err() {
                                eprintln!("[NOTIFY_FAILED] group_id={} index={} - receiver dropped",
                                    group_id, index);
                            }
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
        }
    }
}

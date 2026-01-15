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
use helix_core::{GroupId, LogIndex, NodeId, PartitionId, TopicId};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::RaftState;
use helix_server::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use helix_server::group_map::GroupMap;
use helix_server::partition_storage::PartitionStorage;
use helix_server::service::{HEARTBEAT_INTERVAL_MS, TICK_INTERVAL_MS};
use helix_wal::SimulatedStorage;
use tracing::{debug, error, info, warn};

use crate::raft_actor::SharedNetworkState;
use crate::simulated_transport::SimulatedTransport;

/// Timer IDs for service events.
mod timer_ids {
    /// Tick timer - drives Raft consensus.
    pub const TICK: u64 = 1;
    /// Heartbeat timer - drives broker liveness.
    pub const HEARTBEAT: u64 = 2;
}

/// Custom event names for client operations.
pub mod custom_events {
    /// Produce request: data is serialized ProduceRequest.
    pub const PRODUCE: &str = "produce";
    /// Fetch request: data is serialized FetchRequest.
    pub const FETCH: &str = "fetch";
    /// Create topic request.
    pub const CREATE_TOPIC: &str = "create_topic";
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
    /// Group ID mapping (topic, partition) -> GroupId.
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
    /// Mapping from NodeId to ActorId.
    #[allow(dead_code)] // Used by transport, kept for future state reconstruction.
    node_to_actor: BTreeMap<NodeId, ActorId>,
    /// Seed for deterministic behavior.
    #[allow(dead_code)] // Used for future random fault injection.
    seed: u64,
    /// Tick counter for time tracking (increments each tick).
    tick_count: u64,
}

impl HelixServiceActor {
    /// Creates a new Helix service actor.
    ///
    /// # Arguments
    ///
    /// * `actor_id` - Bloodhound actor ID
    /// * `node_id` - Helix node ID
    /// * `cluster_nodes` - All node IDs in the cluster
    /// * `node_to_actor` - Mapping from NodeId to ActorId
    /// * `network_state` - Shared network state for partitions
    /// * `seed` - Random seed for deterministic behavior
    #[must_use]
    pub fn new(
        actor_id: ActorId,
        node_id: NodeId,
        cluster_nodes: Vec<NodeId>,
        node_to_actor: BTreeMap<NodeId, ActorId>,
        network_state: SharedNetworkState,
        seed: u64,
    ) -> Self {
        let name = format!("helix-service-{}", node_id.get());

        // Create simulated storage with the seed.
        let storage = SimulatedStorage::new(seed);

        // Create simulated transport.
        let transport = SimulatedTransport::new(
            node_id,
            actor_id,
            node_to_actor.clone(),
            Arc::clone(&network_state),
        );

        // Create Multi-Raft with controller group.
        let mut multi_raft = MultiRaft::new(node_id);

        // Create controller group.
        if let Err(e) = multi_raft.create_group(CONTROLLER_GROUP_ID, cluster_nodes.clone()) {
            error!(error = %e, "Failed to create controller group");
        }

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

    /// Schedules the next tick timer.
    fn schedule_tick(&self, ctx: &mut SimulationContext) {
        ctx.set_timer(
            Duration::from_millis(TICK_INTERVAL_MS),
            timer_ids::TICK,
        );
    }

    /// Schedules the next heartbeat timer.
    fn schedule_heartbeat(&self, ctx: &mut SimulationContext) {
        ctx.set_timer(
            Duration::from_millis(HEARTBEAT_INTERVAL_MS),
            timer_ids::HEARTBEAT,
        );
    }

    /// Handles a tick event - mirrors tick_task_multi_node logic.
    fn handle_tick(&mut self, ctx: &mut SimulationContext) {
        // Increment tick counter for time tracking.
        self.tick_count += 1;

        // Tick the Multi-Raft engine.
        let outputs = self.multi_raft.tick();

        // Process outputs synchronously.
        self.process_outputs(&outputs, ctx);

        // Drain transport queue and schedule message deliveries.
        self.transport.drain_and_schedule(ctx);

        // Schedule next tick.
        self.schedule_tick(ctx);
    }

    /// Handles a heartbeat timer event.
    fn handle_heartbeat(&mut self, ctx: &mut SimulationContext) {
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
                    self.handle_commit(*group_id, *index, data, ctx);
                }
                MultiRaftOutput::SendMessages { to, messages } => {
                    // Queue messages for delivery via transport (sync).
                    self.transport.queue_batch(*to, messages.clone());
                }
                MultiRaftOutput::BecameLeader { group_id } => {
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
            }
        }
    }

    /// Handles a committed entry.
    fn handle_commit(
        &mut self,
        group_id: GroupId,
        index: LogIndex,
        data: &Bytes,
        _ctx: &mut SimulationContext,
    ) {
        // Check if this is a controller partition commit.
        if group_id == CONTROLLER_GROUP_ID {
            self.handle_controller_commit(index, data);
            return;
        }

        // Regular data partition commit - apply to storage.
        if let Some((topic_id, partition_id)) = self.group_map.get_key(group_id) {
            if let Some(ps) = self.partition_storage.get_mut(&group_id) {
                // Apply entry synchronously (DST uses in-memory storage).
                if let Err(e) = ps.apply_entry_sync(index, data) {
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
            "Creating data partition"
        );

        // Create Raft group if it doesn't exist.
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

        // Update group map.
        self.group_map.insert(topic_id, partition_id, group_id);

        // Create partition storage with simulated backend.
        if !self.partition_storage.contains_key(&group_id) {
            let ps = SimulatedPartitionStorage::new_in_memory(topic_id, partition_id);
            self.partition_storage.insert(group_id, ps);
        }
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
                        let outputs = self.multi_raft.handle_message(
                            group_msg.group_id,
                            group_msg.message,
                        );
                        self.process_outputs(&outputs, ctx);
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

    /// Handles crash event.
    fn handle_crash(&mut self) {
        if self.crashed {
            return;
        }

        self.crashed = true;

        // Simulate storage crash - revert to last synced state.
        self.storage.simulate_crash();

        // Clear pending transport messages.
        self.transport.clear_pending();

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

        // Recreate controller group.
        if let Err(e) = self.multi_raft.create_group(CONTROLLER_GROUP_ID, self.cluster_nodes.clone()) {
            error!(actor = %self.name, error = %e, "Failed to recreate controller group");
        }

        // Clear in-memory state (will be rebuilt from WAL on partition access).
        self.partition_storage.clear();
        self.group_map = GroupMap::new();
        self.controller_state = ControllerState::new();
        self.tick_count = 0;

        self.crashed = false;

        // Restart timers.
        self.schedule_tick(ctx);
        self.schedule_heartbeat(ctx);

        info!(actor = %self.name, "RECOVERED - restarted as follower");
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

        // Propose to Raft.
        if self.multi_raft.propose(group_id, payload).is_none() {
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

        let name = match String::from_utf8(data[4..4 + name_len].to_vec()) {
            Ok(n) => n,
            Err(_) => {
                warn!(actor = %self.name, "Invalid create topic request: invalid UTF-8 name");
                return;
            }
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
///
/// # Returns
///
/// A tuple of (actors, network_state) for the simulation.
///
/// # Panics
///
/// Panics if node_count is 0 or greater than 7.
#[must_use]
pub fn create_helix_cluster(
    node_count: usize,
    base_seed: u64,
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

    // Create shared network state.
    let network_state = Arc::new(Mutex::new(crate::raft_actor::NetworkState::new()));

    // Create actors.
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
                seed,
            )
        })
        .collect();

    (actors, network_state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_helix_cluster() {
        let (actors, _network_state) = create_helix_cluster(3, 42);
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
        let (mut actors, _network_state) = create_helix_cluster(1, 42);
        let actor = &mut actors[0];

        assert!(!actor.is_crashed());
        actor.handle_crash();
        assert!(actor.is_crashed());

        // Can't recover without context, but we can check the flag.
    }
}

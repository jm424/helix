//! Raft SimulatedActor for Bloodhound simulation.
//!
//! This module wraps the Raft state machine in a Bloodhound `SimulatedActor`
//! for deterministic simulation testing.

use std::any::Any;
use std::collections::BTreeMap;
use std::time::Duration;

use bloodhound::simulation::discrete::actors::{SimulatedActor, SimulationContext};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use bytes::Bytes;
use helix_core::{LogIndex, NodeId, TermId};
use helix_raft::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, Message, RaftConfig, RaftNode,
    RaftOutput, RaftState, RequestVoteRequest, RequestVoteResponse,
};

/// Timer IDs for Raft events.
mod timer_ids {
    /// Election timeout timer.
    pub const ELECTION_TIMEOUT: u64 = 1;
    /// Heartbeat timer (leader only).
    pub const HEARTBEAT: u64 = 2;
}

/// Network latency for message delivery (simulated).
const NETWORK_LATENCY_US: u64 = 1_000; // 1ms

/// Message type tags for serialization.
mod message_tags {
    pub const REQUEST_VOTE: u8 = 1;
    pub const REQUEST_VOTE_RESPONSE: u8 = 2;
    pub const APPEND_ENTRIES: u8 = 3;
    pub const APPEND_ENTRIES_RESPONSE: u8 = 4;
}

/// Serialize a Raft message to bytes for network transmission.
fn serialize_message(msg: &Message) -> Vec<u8> {
    let mut buf = Vec::new();

    match msg {
        Message::RequestVote(req) => {
            buf.push(message_tags::REQUEST_VOTE);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.candidate_id.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_index.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_term.get().to_le_bytes());
        }
        Message::RequestVoteResponse(resp) => {
            buf.push(message_tags::REQUEST_VOTE_RESPONSE);
            buf.extend_from_slice(&resp.term.get().to_le_bytes());
            buf.extend_from_slice(&resp.from.get().to_le_bytes());
            buf.extend_from_slice(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.vote_granted));
        }
        Message::AppendEntries(req) => {
            buf.push(message_tags::APPEND_ENTRIES);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.leader_id.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
            buf.extend_from_slice(&req.prev_log_index.get().to_le_bytes());
            buf.extend_from_slice(&req.prev_log_term.get().to_le_bytes());
            buf.extend_from_slice(&req.leader_commit.get().to_le_bytes());
            // Entries count.
            buf.extend_from_slice(&(req.entries.len() as u32).to_le_bytes());
            for entry in &req.entries {
                buf.extend_from_slice(&entry.term.get().to_le_bytes());
                buf.extend_from_slice(&entry.index.get().to_le_bytes());
                buf.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
                buf.extend_from_slice(&entry.data);
            }
        }
        Message::AppendEntriesResponse(resp) => {
            buf.push(message_tags::APPEND_ENTRIES_RESPONSE);
            buf.extend_from_slice(&resp.term.get().to_le_bytes());
            buf.extend_from_slice(&resp.from.get().to_le_bytes());
            buf.extend_from_slice(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.success));
            buf.extend_from_slice(&resp.match_index.get().to_le_bytes());
        }
    }

    buf
}

/// Deserialize a Raft message from bytes.
fn deserialize_message(data: &[u8]) -> Option<Message> {
    if data.is_empty() {
        return None;
    }

    let tag = data[0];
    let data = &data[1..];

    match tag {
        message_tags::REQUEST_VOTE => {
            if data.len() < 40 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(data[0..8].try_into().ok()?));
            let candidate_id = NodeId::new(u64::from_le_bytes(data[8..16].try_into().ok()?));
            let to = NodeId::new(u64::from_le_bytes(data[16..24].try_into().ok()?));
            let last_log_index = LogIndex::new(u64::from_le_bytes(data[24..32].try_into().ok()?));
            let last_log_term = TermId::new(u64::from_le_bytes(data[32..40].try_into().ok()?));
            Some(Message::RequestVote(RequestVoteRequest::new(
                term,
                candidate_id,
                to,
                last_log_index,
                last_log_term,
            )))
        }
        message_tags::REQUEST_VOTE_RESPONSE => {
            if data.len() < 25 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(data[0..8].try_into().ok()?));
            let from = NodeId::new(u64::from_le_bytes(data[8..16].try_into().ok()?));
            let to = NodeId::new(u64::from_le_bytes(data[16..24].try_into().ok()?));
            let vote_granted = data[24] != 0;
            Some(Message::RequestVoteResponse(RequestVoteResponse::new(
                term,
                from,
                to,
                vote_granted,
            )))
        }
        message_tags::APPEND_ENTRIES => {
            if data.len() < 52 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(data[0..8].try_into().ok()?));
            let leader_id = NodeId::new(u64::from_le_bytes(data[8..16].try_into().ok()?));
            let to = NodeId::new(u64::from_le_bytes(data[16..24].try_into().ok()?));
            let prev_log_index = LogIndex::new(u64::from_le_bytes(data[24..32].try_into().ok()?));
            let prev_log_term = TermId::new(u64::from_le_bytes(data[32..40].try_into().ok()?));
            let leader_commit = LogIndex::new(u64::from_le_bytes(data[40..48].try_into().ok()?));
            let entry_count = u32::from_le_bytes(data[48..52].try_into().ok()?) as usize;

            let mut offset = 52;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                if offset + 20 > data.len() {
                    return None;
                }
                let entry_term =
                    TermId::new(u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?));
                let entry_index = LogIndex::new(u64::from_le_bytes(
                    data[offset + 8..offset + 16].try_into().ok()?,
                ));
                let data_len =
                    u32::from_le_bytes(data[offset + 16..offset + 20].try_into().ok()?) as usize;
                offset += 20;
                if offset + data_len > data.len() {
                    return None;
                }
                let entry_data = Bytes::copy_from_slice(&data[offset..offset + data_len]);
                offset += data_len;
                entries.push(LogEntry::new(entry_term, entry_index, entry_data));
            }

            Some(Message::AppendEntries(AppendEntriesRequest::new(
                term,
                leader_id,
                to,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )))
        }
        message_tags::APPEND_ENTRIES_RESPONSE => {
            if data.len() < 33 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(data[0..8].try_into().ok()?));
            let from = NodeId::new(u64::from_le_bytes(data[8..16].try_into().ok()?));
            let to = NodeId::new(u64::from_le_bytes(data[16..24].try_into().ok()?));
            let success = data[24] != 0;
            let match_index = LogIndex::new(u64::from_le_bytes(data[25..33].try_into().ok()?));
            Some(Message::AppendEntriesResponse(AppendEntriesResponse::new(
                term,
                from,
                to,
                success,
                match_index,
            )))
        }
        _ => None,
    }
}

/// Checkpoint state for RaftActor.
///
/// Note: Fields are stored for future checkpoint/restore implementation.
#[derive(Clone)]
#[allow(dead_code)]
struct RaftActorCheckpoint {
    current_term: u64,
    voted_for: Option<u64>,
    log_entries: Vec<(u64, u64, Bytes)>, // (term, index, data)
    commit_index: u64,
    last_applied: u64,
    state: RaftState,
}

/// A Raft node wrapped as a Bloodhound SimulatedActor.
pub struct RaftActor {
    /// The actor's unique ID.
    actor_id: ActorId,
    /// Human-readable name.
    name: String,
    /// The Raft node state machine.
    node: RaftNode,
    /// Mapping from Helix NodeId to Bloodhound ActorId.
    node_to_actor: BTreeMap<NodeId, ActorId>,
    /// Mapping from Bloodhound ActorId to Helix NodeId.
    #[allow(dead_code)]
    actor_to_node: BTreeMap<ActorId, NodeId>,
    /// Configuration for election timeouts.
    election_timeout_min_us: u64,
    election_timeout_max_us: u64,
    /// Heartbeat interval.
    heartbeat_interval_us: u64,
}

impl RaftActor {
    /// Creates a new Raft actor.
    ///
    /// # Arguments
    /// * `actor_id` - The Bloodhound actor ID for this node.
    /// * `config` - The Raft configuration.
    /// * `node_actor_mapping` - Mapping from Helix NodeId to Bloodhound ActorId.
    #[must_use]
    pub fn new(
        actor_id: ActorId,
        config: RaftConfig,
        node_actor_mapping: BTreeMap<NodeId, ActorId>,
    ) -> Self {
        let name = format!("raft-{}", config.node_id);

        let actor_to_node: BTreeMap<ActorId, NodeId> = node_actor_mapping
            .iter()
            .map(|(node, actor)| (*actor, *node))
            .collect();

        let election_timeout_min_us = config.election_timeout_min_us;
        let election_timeout_max_us = config.election_timeout_max_us;
        let heartbeat_interval_us = config.heartbeat_interval_us;

        Self {
            actor_id,
            name,
            node: RaftNode::new(config),
            node_to_actor: node_actor_mapping,
            actor_to_node,
            election_timeout_min_us,
            election_timeout_max_us,
            heartbeat_interval_us,
        }
    }

    /// Returns true if this node is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    /// Returns the current term.
    #[must_use]
    pub fn current_term(&self) -> TermId {
        self.node.current_term()
    }

    /// Returns the current state.
    #[must_use]
    pub fn state(&self) -> RaftState {
        self.node.state()
    }

    /// Returns the commit index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.node.commit_index()
    }

    /// Processes outputs from the Raft state machine.
    fn process_outputs(&self, outputs: Vec<RaftOutput>, ctx: &mut SimulationContext) {
        for output in outputs {
            match output {
                RaftOutput::SendMessage(msg) => {
                    self.send_raft_message(&msg, ctx);
                }
                RaftOutput::ResetElectionTimer => {
                    self.schedule_election_timeout(ctx);
                }
                RaftOutput::ResetHeartbeatTimer => {
                    self.schedule_heartbeat(ctx);
                }
                RaftOutput::CommitEntry { index, data } => {
                    tracing::debug!(
                        actor = %self.name,
                        index = index.get(),
                        data_len = data.len(),
                        "committed entry"
                    );
                }
                RaftOutput::BecameLeader => {
                    tracing::info!(
                        actor = %self.name,
                        term = self.node.current_term().get(),
                        "became leader"
                    );
                }
                RaftOutput::SteppedDown => {
                    tracing::info!(
                        actor = %self.name,
                        term = self.node.current_term().get(),
                        "stepped down"
                    );
                }
            }
        }
    }

    /// Sends a Raft message to another actor using PacketDelivery.
    fn send_raft_message(&self, msg: &Message, ctx: &mut SimulationContext) {
        let to_node = msg.to();
        if let Some(&to_actor) = self.node_to_actor.get(&to_node) {
            let payload = serialize_message(msg);
            ctx.schedule_after(
                Duration::from_micros(NETWORK_LATENCY_US),
                EventKind::PacketDelivery {
                    from: self.actor_id,
                    to: to_actor,
                    payload,
                },
            );
        }
    }

    /// Schedules a random election timeout.
    fn schedule_election_timeout(&self, ctx: &mut SimulationContext) {
        let timeout_us =
            ctx.random_range(self.election_timeout_min_us..=self.election_timeout_max_us);
        ctx.set_timer(Duration::from_micros(timeout_us), timer_ids::ELECTION_TIMEOUT);
    }

    /// Schedules the next heartbeat.
    fn schedule_heartbeat(&self, ctx: &mut SimulationContext) {
        ctx.set_timer(
            Duration::from_micros(self.heartbeat_interval_us),
            timer_ids::HEARTBEAT,
        );
    }
}

impl SimulatedActor for RaftActor {
    fn handle(&mut self, event: EventKind, ctx: &mut SimulationContext) {
        match event {
            EventKind::ActorStart { .. } => {
                // Schedule initial election timeout.
                self.schedule_election_timeout(ctx);
                tracing::debug!(actor = %self.name, "started");
            }

            EventKind::TimerFired { timer_id, .. } => {
                let outputs = match timer_id {
                    timer_ids::ELECTION_TIMEOUT => {
                        tracing::debug!(actor = %self.name, "election timeout");
                        self.node.handle_election_timeout()
                    }
                    timer_ids::HEARTBEAT => {
                        tracing::trace!(actor = %self.name, "heartbeat timeout");
                        self.node.handle_heartbeat_timeout()
                    }
                    _ => Vec::new(),
                };
                self.process_outputs(outputs, ctx);
            }

            EventKind::PacketDelivery { payload, .. } => {
                // Deserialize and handle the Raft message.
                if let Some(msg) = deserialize_message(&payload) {
                    let outputs = self.node.handle_message(msg);
                    self.process_outputs(outputs, ctx);
                }
            }

            EventKind::ProcessCrash { .. } => {
                tracing::info!(actor = %self.name, "crashed");
                // In a full implementation, we'd clear volatile state here.
            }

            EventKind::ProcessRecover { .. } => {
                tracing::info!(actor = %self.name, "recovered");
                // In a full implementation, we'd reload from WAL here.
                self.schedule_election_timeout(ctx);
            }

            _ => {
                // Ignore other events.
            }
        }
    }

    fn id(&self) -> ActorId {
        self.actor_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn checkpoint(&self) -> Box<dyn Any + Send> {
        // For now, we can't easily checkpoint the RaftNode internals.
        // In a full implementation, we'd serialize all state.
        // For basic testing, we'll checkpoint what we can access.
        let checkpoint = RaftActorCheckpoint {
            current_term: self.node.current_term().get(),
            voted_for: None, // Not accessible from public API
            log_entries: Vec::new(), // Would need to iterate log
            commit_index: self.node.commit_index().get(),
            last_applied: self.node.last_applied().get(),
            state: self.node.state(),
        };
        Box::new(checkpoint)
    }

    fn restore(&mut self, _state: Box<dyn Any + Send>) {
        // Full restore would require recreating RaftNode.
        // For now, this is a limitation.
        tracing::warn!(actor = %self.name, "checkpoint restore not fully implemented");
    }

    fn on_start(&mut self, ctx: &mut SimulationContext) {
        // Schedule the ActorStart event to trigger initialization.
        ctx.schedule_after(
            Duration::from_nanos(0),
            EventKind::ActorStart {
                actor: self.actor_id,
            },
        );
    }
}

/// Creates a cluster of Raft actors for simulation.
///
/// # Arguments
/// * `node_count` - Number of nodes in the cluster.
///
/// # Returns
/// A vector of `RaftActor` instances configured as a cluster.
#[must_use]
pub fn create_raft_cluster(node_count: usize) -> Vec<RaftActor> {
    assert!(node_count > 0, "cluster must have at least one node");
    assert!(node_count <= 7, "cluster size exceeds maximum");

    // Create node IDs and actor IDs.
    let node_ids: Vec<NodeId> = (1..=node_count as u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count as u64).map(ActorId::new).collect();

    // Create mapping.
    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&n, &a)| (n, a))
        .collect();

    // Create actors.
    node_ids
        .iter()
        .zip(actor_ids.iter())
        .map(|(&node_id, &actor_id)| {
            let config = RaftConfig::new(node_id, node_ids.clone());
            RaftActor::new(actor_id, config, node_to_actor.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cluster() {
        let actors = create_raft_cluster(3);
        assert_eq!(actors.len(), 3);

        for (i, actor) in actors.iter().enumerate() {
            assert_eq!(actor.id(), ActorId::new((i + 1) as u64));
            assert!(!actor.is_leader());
            assert_eq!(actor.state(), RaftState::Follower);
        }
    }

    #[test]
    fn test_message_serialization_roundtrip() {
        // Test RequestVote.
        let req = Message::RequestVote(RequestVoteRequest::new(
            TermId::new(5),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(10),
            TermId::new(4),
        ));
        let serialized = serialize_message(&req);
        let deserialized = deserialize_message(&serialized).unwrap();
        assert_eq!(req, deserialized);

        // Test RequestVoteResponse.
        let resp = Message::RequestVoteResponse(RequestVoteResponse::new(
            TermId::new(5),
            NodeId::new(2),
            NodeId::new(1),
            true,
        ));
        let serialized = serialize_message(&resp);
        let deserialized = deserialize_message(&serialized).unwrap();
        assert_eq!(resp, deserialized);

        // Test AppendEntries with entries.
        let ae = Message::AppendEntries(AppendEntriesRequest::new(
            TermId::new(3),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(5),
            TermId::new(2),
            vec![
                LogEntry::new(TermId::new(3), LogIndex::new(6), Bytes::from("cmd1")),
                LogEntry::new(TermId::new(3), LogIndex::new(7), Bytes::from("cmd2")),
            ],
            LogIndex::new(4),
        ));
        let serialized = serialize_message(&ae);
        let deserialized = deserialize_message(&serialized).unwrap();
        assert_eq!(ae, deserialized);

        // Test AppendEntriesResponse.
        let ae_resp = Message::AppendEntriesResponse(AppendEntriesResponse::new(
            TermId::new(3),
            NodeId::new(2),
            NodeId::new(1),
            true,
            LogIndex::new(7),
        ));
        let serialized = serialize_message(&ae_resp);
        let deserialized = deserialize_message(&serialized).unwrap();
        assert_eq!(ae_resp, deserialized);
    }

    #[test]
    fn test_empty_append_entries() {
        // Heartbeat (empty entries).
        let hb = Message::AppendEntries(AppendEntriesRequest::heartbeat(
            TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(0),
            TermId::new(0),
            LogIndex::new(0),
        ));
        let serialized = serialize_message(&hb);
        let deserialized = deserialize_message(&serialized).unwrap();
        assert_eq!(hb, deserialized);
    }
}

//! Multi-Raft simulation tests.
//!
//! These tests verify the `MultiRaft` engine behavior under simulation,
//! including election staggering, message batching, and multiple groups.

// Test-specific lint allowances - these are less critical in test code.
#![allow(clippy::cast_precision_loss)] // f64 precision loss acceptable in test stats
#![allow(clippy::cast_possible_truncation)] // u64 to usize safe on 64-bit test machines
#![allow(clippy::too_many_lines)] // Test functions can be longer for clarity
#![allow(clippy::significant_drop_tightening)] // Test code clarity > drop optimization
#![allow(clippy::doc_markdown)] // Backticks in docs not critical for tests
#![allow(clippy::uninlined_format_args)] // Format string style not critical for tests
#![allow(clippy::needless_pass_by_value)] // Pass by value can improve test clarity
#![allow(clippy::items_after_test_module)] // Test module organization
#![allow(clippy::cast_sign_loss)] // Test data is always positive
#![allow(clippy::type_complexity)] // Complex types acceptable in test utilities
#![allow(clippy::iter_over_hash_type)] // Iteration order doesn't matter in tests
#![allow(clippy::explicit_iter_loop)] // Explicit iteration can be clearer
#![allow(clippy::format_push_string)] // String building style not critical
#![allow(clippy::for_kv_map)] // Iterate over keys may be intentional for clarity
#![allow(clippy::panic_in_result_fn)] // Tests may intentionally panic
#![allow(clippy::manual_assert)] // if + panic pattern is clear

use std::any::Any;
use std::collections::BTreeMap;
use std::time::Duration;

use bloodhound::simulation::discrete::actors::{SimulatedActor, SimulationContext};
use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, TermId};
use helix_raft::multi::{GroupMessage, MultiRaft, MultiRaftOutput};
use helix_raft::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry, Message, PreVoteRequest,
    PreVoteResponse, RaftState, RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};

/// Timer IDs for Multi-Raft events.
mod timer_ids {
    /// Tick timer (processes all groups).
    pub const TICK: u64 = 1;
}

/// Network latency for message delivery (simulated).
const NETWORK_LATENCY_US: u64 = 1_000; // 1ms

/// Tick interval for Multi-Raft processing.
/// Each tick advances the Raft internal counters. Elections and heartbeats
/// are triggered when thresholds are reached.
const TICK_INTERVAL_US: u64 = 10_000; // 10ms (faster for simulation)

/// Helper to get message type name for debugging.
fn message_type(msg: &Message) -> &'static str {
    match msg {
        Message::PreVote(_) => "PreVote",
        Message::PreVoteResponse(_) => "PreVoteResponse",
        Message::RequestVote(_) => "RequestVote",
        Message::RequestVoteResponse(_) => "RequestVoteResponse",
        Message::AppendEntries(_) => "AppendEntries",
        Message::AppendEntriesResponse(_) => "AppendEntriesResponse",
        Message::TimeoutNow(_) => "TimeoutNow",
        Message::InstallSnapshot(_) => "InstallSnapshot",
        Message::InstallSnapshotResponse(_) => "InstallSnapshotResponse",
    }
}

// ============================================================================
// Multi-Raft Actor
// ============================================================================

/// Multi-Raft actor for Bloodhound simulation.
///
/// This wraps the `MultiRaft` engine for deterministic simulation testing.
pub struct MultiRaftActor {
    /// Actor ID.
    actor_id: ActorId,
    /// Actor name for logging.
    name: String,
    /// The Multi-Raft engine.
    pub multi_raft: MultiRaft,
    /// Mapping from NodeId to ActorId for message delivery.
    node_to_actor: BTreeMap<NodeId, ActorId>,
    /// Whether this actor is currently crashed.
    crashed: bool,
    /// Groups managed by this Multi-Raft instance.
    pub groups: Vec<GroupId>,
    /// Election count per group for verification.
    election_count: u64,
}

#[allow(dead_code)]
impl MultiRaftActor {
    /// Creates a new Multi-Raft actor.
    #[must_use]
    pub fn new(
        actor_id: ActorId,
        node_id: NodeId,
        groups: Vec<GroupId>,
        all_nodes: Vec<NodeId>,
        node_to_actor: BTreeMap<NodeId, ActorId>,
    ) -> Self {
        let mut multi_raft = MultiRaft::new(node_id);

        // Create all groups.
        for &group_id in &groups {
            multi_raft
                .create_group(group_id, all_nodes.clone())
                .expect("failed to create group");
        }

        Self {
            actor_id,
            name: format!("multi-raft-{}", node_id.get()),
            multi_raft,
            node_to_actor,
            crashed: false,
            groups,
            election_count: 0,
        }
    }

    /// Processes outputs from Multi-Raft.
    fn process_outputs(&mut self, ctx: &mut SimulationContext, outputs: Vec<MultiRaftOutput>) {
        for output in outputs {
            match output {
                MultiRaftOutput::SendMessages { to, messages } => {
                    self.send_batch(ctx, to, messages);
                }
                MultiRaftOutput::BecameLeader { group_id: _ } => {
                    self.election_count += 1;
                }
                MultiRaftOutput::CommitEntry { .. } | MultiRaftOutput::SteppedDown { .. } => {
                    // Handled by application layer.
                }
            }
        }
    }

    /// Sends a batch of messages to a destination node.
    fn send_batch(&self, ctx: &mut SimulationContext, to: NodeId, messages: Vec<GroupMessage>) {
        if self.crashed {
            return;
        }

        if let Some(&target_actor) = self.node_to_actor.get(&to) {
            let payload = serialize_batch(&messages);
            ctx.schedule_after(
                Duration::from_micros(NETWORK_LATENCY_US),
                EventKind::PacketDelivery {
                    from: self.actor_id,
                    to: target_actor,
                    payload: payload.to_vec(),
                },
            );
        }
    }

    /// Handles a received batch of messages.
    fn handle_batch(&mut self, ctx: &mut SimulationContext, payload: &[u8]) {
        if self.crashed {
            return;
        }

        if let Some(messages) = deserialize_batch_slice(payload) {
            static MSG_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let count = MSG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if count < 10 {
                println!(
                    "Node {} received batch of {} messages",
                    self.multi_raft.node_id().get(),
                    messages.len()
                );
                for gm in &messages {
                    println!("  Group {}: {:?}", gm.group_id.get(), message_type(&gm.message));
                }
            }

            for gm in messages {
                let outputs = self.multi_raft.handle_message(gm.group_id, gm.message);
                self.process_outputs(ctx, outputs);
            }
            // CRITICAL: Flush any batched outbound messages after handling!
            let flushed = self.multi_raft.flush();
            self.process_outputs(ctx, flushed);
        }
    }

    /// Handles tick event - ticks all groups.
    ///
    /// A single tick drives both
    /// election timeouts and leader heartbeats. The Raft library internally
    /// tracks elapsed ticks and triggers actions when thresholds are reached.
    fn handle_tick(&mut self, ctx: &mut SimulationContext) {
        if self.crashed {
            return;
        }

        // Tick all groups - this internally handles elections and heartbeats.
        let outputs = self.multi_raft.tick();
        self.process_outputs(ctx, outputs);

        // Schedule next tick.
        ctx.schedule_after(
            Duration::from_micros(TICK_INTERVAL_US),
            EventKind::TimerFired {
                actor: self.actor_id,
                timer_id: timer_ids::TICK,
            },
        );
    }

    /// Handles crash event.
    fn handle_crash(&mut self) {
        self.crashed = true;
    }

    /// Handles recovery event.
    fn handle_recover(&mut self, ctx: &mut SimulationContext) {
        self.crashed = false;
        // Recreate the Multi-Raft engine (simulating restart).
        let node_id = self.multi_raft.node_id();
        let all_nodes: Vec<_> = self.node_to_actor.keys().copied().collect();
        self.multi_raft = MultiRaft::new(node_id);
        for &group_id in &self.groups {
            let _ = self.multi_raft.create_group(group_id, all_nodes.clone());
        }
        // Schedule tick to restart processing.
        ctx.schedule_after(
            Duration::from_micros(TICK_INTERVAL_US),
            EventKind::TimerFired {
                actor: self.actor_id,
                timer_id: timer_ids::TICK,
            },
        );
    }

    /// Returns the number of groups that have this node as leader.
    #[must_use]
    pub fn leader_group_count(&self) -> usize {
        self.groups
            .iter()
            .filter(|&&g| {
                self.multi_raft
                    .group_state(g)
                    .is_some_and(|info| info.state == RaftState::Leader)
            })
            .count()
    }

    /// Returns the total election count.
    #[must_use]
    pub const fn election_count(&self) -> u64 {
        self.election_count
    }
}

impl SimulatedActor for MultiRaftActor {
    fn handle(&mut self, event: EventKind, ctx: &mut SimulationContext) {
        match event {
            EventKind::ProcessCrash { .. } => self.handle_crash(),
            EventKind::ProcessRecover { .. } => self.handle_recover(ctx),

            _ if self.crashed => {}

            EventKind::ActorStart { .. } => {
                // Schedule initial tick.
                ctx.schedule_after(
                    Duration::from_micros(TICK_INTERVAL_US),
                    EventKind::TimerFired {
                        actor: self.actor_id,
                        timer_id: timer_ids::TICK,
                    },
                );
            }

            EventKind::TimerFired { timer_id, .. } => {
                if timer_id == timer_ids::TICK {
                    self.handle_tick(ctx);
                }
            }

            EventKind::PacketDelivery { ref payload, .. } => {
                self.handle_batch(ctx, payload);
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
        // Simplified checkpoint for testing.
        Box::new(self.election_count)
    }

    fn restore(&mut self, _state: Box<dyn Any + Send>) {
        // Simplified restore for testing.
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

// ============================================================================
// Message Serialization
// ============================================================================

/// Serializes a batch of group messages.
fn serialize_batch(messages: &[GroupMessage]) -> Bytes {
    let mut buf = Vec::new();
    // Write count.
    // Safe cast: batch size is limited.
    #[allow(clippy::cast_possible_truncation)]
    let count = messages.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());

    for gm in messages {
        buf.extend_from_slice(&gm.group_id.get().to_le_bytes());
        serialize_message_to(&mut buf, &gm.message);
    }

    Bytes::from(buf)
}

/// Deserializes a batch of group messages from a slice.
fn deserialize_batch_slice(payload: &[u8]) -> Option<Vec<GroupMessage>> {
    if payload.len() < 4 {
        return None;
    }

    let count = u32::from_le_bytes(payload[0..4].try_into().ok()?) as usize;
    let mut messages = Vec::with_capacity(count);
    let mut offset = 4;

    for _ in 0..count {
        if offset + 8 > payload.len() {
            return None;
        }
        let group_id = GroupId::new(u64::from_le_bytes(
            payload[offset..offset + 8].try_into().ok()?,
        ));
        offset += 8;

        let (msg, consumed) = deserialize_message_with_len(&payload[offset..])?;
        offset += consumed;
        messages.push(GroupMessage::new(group_id, msg));
    }

    Some(messages)
}

/// Serializes a Raft message to a buffer.
fn serialize_message_to(buf: &mut Vec<u8>, msg: &Message) {
    match msg {
        Message::PreVote(req) => {
            buf.push(1);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.candidate_id.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_index.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_term.get().to_le_bytes());
        }
        Message::PreVoteResponse(resp) => {
            buf.push(2);
            buf.extend_from_slice(&resp.term.get().to_le_bytes());
            buf.extend_from_slice(&resp.from.get().to_le_bytes());
            buf.extend_from_slice(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.vote_granted));
        }
        Message::RequestVote(req) => {
            buf.push(3);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.candidate_id.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_index.get().to_le_bytes());
            buf.extend_from_slice(&req.last_log_term.get().to_le_bytes());
        }
        Message::RequestVoteResponse(resp) => {
            buf.push(4);
            buf.extend_from_slice(&resp.term.get().to_le_bytes());
            buf.extend_from_slice(&resp.from.get().to_le_bytes());
            buf.extend_from_slice(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.vote_granted));
        }
        Message::AppendEntries(req) => {
            buf.push(5);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.leader_id.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
            buf.extend_from_slice(&req.prev_log_index.get().to_le_bytes());
            buf.extend_from_slice(&req.prev_log_term.get().to_le_bytes());
            buf.extend_from_slice(&req.leader_commit.get().to_le_bytes());
            // Safe cast: entry count is bounded.
            #[allow(clippy::cast_possible_truncation)]
            let entry_count = req.entries.len() as u32;
            buf.extend_from_slice(&entry_count.to_le_bytes());
            for entry in &req.entries {
                buf.extend_from_slice(&entry.term.get().to_le_bytes());
                buf.extend_from_slice(&entry.index.get().to_le_bytes());
                // Safe cast: data length is bounded.
                #[allow(clippy::cast_possible_truncation)]
                let data_len = entry.data.len() as u32;
                buf.extend_from_slice(&data_len.to_le_bytes());
                buf.extend_from_slice(&entry.data);
            }
        }
        Message::AppendEntriesResponse(resp) => {
            buf.push(6);
            buf.extend_from_slice(&resp.term.get().to_le_bytes());
            buf.extend_from_slice(&resp.from.get().to_le_bytes());
            buf.extend_from_slice(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.success));
            buf.extend_from_slice(&resp.match_index.get().to_le_bytes());
        }
        Message::TimeoutNow(req) => {
            buf.push(7);
            buf.extend_from_slice(&req.term.get().to_le_bytes());
            buf.extend_from_slice(&req.from.get().to_le_bytes());
            buf.extend_from_slice(&req.to.get().to_le_bytes());
        }
        // InstallSnapshot messages are not used in Multi-Raft simulation tests yet.
        Message::InstallSnapshot(_) | Message::InstallSnapshotResponse(_) => {
            panic!("InstallSnapshot messages not yet supported in simulation")
        }
    }
}

/// Deserializes a Raft message with its length.
fn deserialize_message_with_len(payload: &[u8]) -> Option<(Message, usize)> {
    if payload.is_empty() {
        return None;
    }
    let tag = payload[0];
    let mut offset = 1;

    match tag {
        1 => {
            // PreVote
            if payload.len() < offset + 40 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let candidate_id = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let last_log_index = LogIndex::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let last_log_term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            Some((
                Message::PreVote(PreVoteRequest::new(
                    term,
                    candidate_id,
                    to,
                    last_log_index,
                    last_log_term,
                )),
                offset,
            ))
        }
        2 => {
            // PreVoteResponse
            if payload.len() < offset + 25 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let from = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let vote_granted = payload[offset] != 0;
            offset += 1;
            Some((
                Message::PreVoteResponse(PreVoteResponse::new(term, from, to, vote_granted)),
                offset,
            ))
        }
        3 => {
            // RequestVote
            if payload.len() < offset + 40 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let candidate_id = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let last_log_index = LogIndex::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let last_log_term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            Some((
                Message::RequestVote(RequestVoteRequest::new(
                    term,
                    candidate_id,
                    to,
                    last_log_index,
                    last_log_term,
                )),
                offset,
            ))
        }
        4 => {
            // RequestVoteResponse
            if payload.len() < offset + 25 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let from = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let vote_granted = payload[offset] != 0;
            offset += 1;
            Some((
                Message::RequestVoteResponse(RequestVoteResponse::new(term, from, to, vote_granted)),
                offset,
            ))
        }
        5 => {
            // AppendEntries
            if payload.len() < offset + 52 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let leader_id = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let prev_log_index = LogIndex::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let prev_log_term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let leader_commit = LogIndex::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let entry_count =
                u32::from_le_bytes(payload[offset..offset + 4].try_into().ok()?) as usize;
            offset += 4;

            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                if payload.len() < offset + 20 {
                    return None;
                }
                let entry_term = TermId::new(u64::from_le_bytes(
                    payload[offset..offset + 8].try_into().ok()?,
                ));
                offset += 8;
                let entry_index = LogIndex::new(u64::from_le_bytes(
                    payload[offset..offset + 8].try_into().ok()?,
                ));
                offset += 8;
                let data_len =
                    u32::from_le_bytes(payload[offset..offset + 4].try_into().ok()?) as usize;
                offset += 4;
                if payload.len() < offset + data_len {
                    return None;
                }
                let data = Bytes::copy_from_slice(&payload[offset..offset + data_len]);
                offset += data_len;
                entries.push(LogEntry::new(entry_term, entry_index, data));
            }

            Some((
                Message::AppendEntries(AppendEntriesRequest::new(
                    term,
                    leader_id,
                    to,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )),
                offset,
            ))
        }
        6 => {
            // AppendEntriesResponse
            if payload.len() < offset + 33 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let from = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let success = payload[offset] != 0;
            offset += 1;
            let match_index = LogIndex::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            Some((
                Message::AppendEntriesResponse(AppendEntriesResponse::new(
                    term,
                    from,
                    to,
                    success,
                    match_index,
                )),
                offset,
            ))
        }
        7 => {
            // TimeoutNow
            if payload.len() < offset + 24 {
                return None;
            }
            let term = TermId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let from = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            let to = NodeId::new(u64::from_le_bytes(
                payload[offset..offset + 8].try_into().ok()?,
            ));
            offset += 8;
            Some((
                Message::TimeoutNow(TimeoutNowRequest::new(term, from, to)),
                offset,
            ))
        }
        _ => None,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Test configuration for Multi-Raft simulation.
    struct MultiRaftTestConfig {
        /// Number of nodes.
        node_count: usize,
        /// Number of groups per node.
        groups_per_node: usize,
        /// Random seed.
        seed: u64,
        /// Maximum simulation time.
        max_time_secs: u64,
    }

    impl Default for MultiRaftTestConfig {
        fn default() -> Self {
            Self {
                node_count: 3,
                groups_per_node: 5,
                seed: 42,
                max_time_secs: 10,
            }
        }
    }

    /// Creates a Multi-Raft simulation.
    fn create_multi_raft_simulation(
        config: &MultiRaftTestConfig,
    ) -> (DiscreteSimulationEngine, Vec<ActorId>) {
        let engine_config = EngineConfig::new(config.seed)
            .with_max_time(Duration::from_secs(config.max_time_secs))
            .with_stats(true)
            .with_recording(false);

        let mut engine = DiscreteSimulationEngine::with_config(engine_config);

        // Create node and actor IDs.
        #[allow(clippy::cast_possible_truncation)]
        let node_count = config.node_count as u64;
        let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
        let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

        let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
            .iter()
            .zip(actor_ids.iter())
            .map(|(&n, &a)| (n, a))
            .collect();

        // Create group IDs.
        #[allow(clippy::cast_possible_truncation)]
        let groups_count = config.groups_per_node as u64;
        let groups: Vec<GroupId> = (1..=groups_count).map(GroupId::new).collect();

        // Register actors.
        for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
            let actor = MultiRaftActor::new(
                actor_id,
                node_id,
                groups.clone(),
                node_ids.clone(),
                node_to_actor.clone(),
            );
            engine.register_actor(Box::new(actor));
        }

        (engine, actor_ids)
    }

    #[test]
    fn test_multi_raft_basic_election() {
        let config = MultiRaftTestConfig::default();
        let (mut engine, _actor_ids) = create_multi_raft_simulation(&config);

        let result = engine.run();

        println!(
            "Multi-Raft basic: {} events in {}ms virtual time",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000
        );

        assert!(result.success, "Simulation should complete successfully");

        // Verify elections happened: we should have processed many more events
        // now that messages are actually being exchanged.
        assert!(
            result.stats.events_processed > 100,
            "Expected significant message exchange, got only {} events",
            result.stats.events_processed
        );
    }

    #[test]
    fn test_multi_raft_multiple_groups() {
        let config = MultiRaftTestConfig {
            groups_per_node: 10,
            max_time_secs: 15,
            ..Default::default()
        };
        let (mut engine, _actor_ids) = create_multi_raft_simulation(&config);

        let result = engine.run();

        println!(
            "Multi-Raft 10 groups: {} events in {}ms virtual time",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000
        );

        assert!(result.success);
    }

    #[test]
    fn test_multi_raft_multiple_seeds() {
        let seeds = [42, 123, 456, 789, 1000];

        for seed in seeds {
            let config = MultiRaftTestConfig {
                seed,
                groups_per_node: 5,
                ..Default::default()
            };
            let (mut engine, _actor_ids) = create_multi_raft_simulation(&config);
            let result = engine.run();

            assert!(
                result.success,
                "Multi-Raft simulation failed with seed {seed}"
            );
        }
        println!("All {} seeds passed", seeds.len());
    }

    #[test]
    fn test_multi_raft_with_crash() {
        let config = MultiRaftTestConfig {
            groups_per_node: 3,
            max_time_secs: 15,
            ..Default::default()
        };
        let (mut engine, actor_ids) = create_multi_raft_simulation(&config);

        // Schedule crash and recovery.
        let crash_actor = actor_ids[0];
        engine.schedule_after(
            Duration::from_millis(3000),
            EventKind::ProcessCrash {
                actor: crash_actor,
            },
        );
        engine.schedule_after(
            Duration::from_millis(8000),
            EventKind::ProcessRecover {
                actor: crash_actor,
            },
        );

        let result = engine.run();

        println!(
            "Multi-Raft crash/recover: {} events in {}ms virtual time",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000
        );

        assert!(result.success);
    }

    #[test]
    fn test_multi_raft_many_groups_stress() {
        // Stress test with many groups.
        let config = MultiRaftTestConfig {
            node_count: 3,
            groups_per_node: 50,
            seed: 42,
            max_time_secs: 20,
        };
        let (mut engine, _actor_ids) = create_multi_raft_simulation(&config);

        let result = engine.run();

        println!(
            "Multi-Raft 50 groups stress: {} events in {}ms virtual time ({:.1}x speedup)",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000,
            result.stats.speedup()
        );

        assert!(result.success);
    }

    /// Test that verifies SingleLeaderPerTerm property.
    ///
    /// This is a critical safety property: for each group and term,
    /// at most one node should believe it's the leader.
    #[test]
    fn test_single_leader_per_term_property() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};

        // Track leaders per (group, term).
        let leaders: Arc<Mutex<HashMap<(u64, u64), Vec<u64>>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let config = MultiRaftTestConfig {
            node_count: 3,
            groups_per_node: 3,
            seed: 42,
            max_time_secs: 10,
        };

        let engine_config = EngineConfig::new(config.seed)
            .with_max_time(Duration::from_secs(config.max_time_secs))
            .with_stats(true)
            .with_recording(false);

        let mut engine = DiscreteSimulationEngine::with_config(engine_config);

        #[allow(clippy::cast_possible_truncation)]
        let node_count = config.node_count as u64;
        let node_ids: Vec<NodeId> = (1..=node_count).map(NodeId::new).collect();
        let actor_ids: Vec<ActorId> = (1..=node_count).map(ActorId::new).collect();

        let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids
            .iter()
            .zip(actor_ids.iter())
            .map(|(&n, &a)| (n, a))
            .collect();

        #[allow(clippy::cast_possible_truncation)]
        let groups_count = config.groups_per_node as u64;
        let groups: Vec<GroupId> = (1..=groups_count).map(GroupId::new).collect();

        // Create actors with leader tracking.
        for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
            let actor = MultiRaftActorWithTracking::new(
                actor_id,
                node_id,
                groups.clone(),
                node_ids.clone(),
                node_to_actor.clone(),
                Arc::clone(&leaders),
            );
            engine.register_actor(Box::new(actor));
        }

        let result = engine.run();
        assert!(result.success);

        println!(
            "Simulation completed: {} events in {}ms",
            result.stats.events_processed,
            result.stats.final_time_ns / 1_000_000
        );

        // Verify single leader per term property.
        let leaders = leaders.lock().unwrap();
        println!("Leaders map has {} entries", leaders.len());
        for ((group, term), nodes) in leaders.iter() {
            println!("  Group {group} Term {term}: {nodes:?}");
        }
        let mut violations = Vec::new();

        for ((group, term), leader_list) in leaders.iter() {
            if leader_list.len() > 1 {
                violations.push(format!(
                    "Group {group} Term {term}: multiple leaders {leader_list:?}"
                ));
            }
        }

        if !violations.is_empty() {
            panic!(
                "SingleLeaderPerTerm VIOLATED!\n{}",
                violations.join("\n")
            );
        }

        println!(
            "SingleLeaderPerTerm: verified {} (group, term) combinations, all valid",
            leaders.len()
        );
    }
}

// ============================================================================
// Actor with Leader Tracking
// ============================================================================

use std::sync::{Arc, Mutex};
use std::collections::HashMap as StdHashMap;

/// Multi-Raft actor with leader tracking for property verification.
struct MultiRaftActorWithTracking {
    inner: MultiRaftActor,
    /// Track leaders: (group_id, term) -> list of node_ids that became leader.
    leaders: Arc<Mutex<StdHashMap<(u64, u64), Vec<u64>>>>,
    /// Track previous leader state per group to detect transitions.
    was_leader: StdHashMap<u64, bool>,
    /// Track previous term per group to detect term changes.
    last_term: StdHashMap<u64, u64>,
}

impl MultiRaftActorWithTracking {
    fn new(
        actor_id: ActorId,
        node_id: NodeId,
        groups: Vec<GroupId>,
        all_nodes: Vec<NodeId>,
        node_to_actor: BTreeMap<NodeId, ActorId>,
        leaders: Arc<Mutex<StdHashMap<(u64, u64), Vec<u64>>>>,
    ) -> Self {
        Self {
            inner: MultiRaftActor::new(actor_id, node_id, groups, all_nodes, node_to_actor),
            leaders,
            was_leader: StdHashMap::new(),
            last_term: StdHashMap::new(),
        }
    }

    /// Records a leader election only on transition.
    fn check_and_record_leaders(&mut self) {
        static DEBUG_COUNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

        for &group_id in &self.inner.groups.clone() {
            if let Some(info) = self.inner.multi_raft.group_state(group_id) {
                let gid = group_id.get();
                let is_leader = info.state == RaftState::Leader;
                let current_term = info.current_term.get();
                let was_leader = self.was_leader.get(&gid).copied().unwrap_or(false);
                let last_term = self.last_term.get(&gid).copied().unwrap_or(0);

                // Debug: Print first few state observations.
                let count = DEBUG_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if count < 100 {
                    println!(
                        "Node {} Group {} Term {}: state={:?}, is_leader={}, was_leader={}",
                        self.inner.multi_raft.node_id().get(),
                        gid,
                        current_term,
                        info.state,
                        is_leader,
                        was_leader
                    );
                }

                // Record if we just became leader OR if term changed while leader.
                if is_leader && (!was_leader || current_term != last_term) {
                    println!(
                        "LEADER ELECTED: Node {} Group {} Term {}",
                        self.inner.multi_raft.node_id().get(),
                        gid,
                        current_term
                    );
                    if let Ok(mut map) = self.leaders.lock() {
                        map.entry((gid, current_term))
                            .or_default()
                            .push(self.inner.multi_raft.node_id().get());
                    }
                }

                self.was_leader.insert(gid, is_leader);
                self.last_term.insert(gid, current_term);
            }
        }
    }
}

impl SimulatedActor for MultiRaftActorWithTracking {
    fn handle(&mut self, event: EventKind, ctx: &mut SimulationContext) {
        // Handle the event with inner actor.
        self.inner.handle(event, ctx);

        // Check for leader transitions and record them.
        self.check_and_record_leaders();
    }

    fn id(&self) -> ActorId {
        self.inner.id()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn checkpoint(&self) -> Box<dyn Any + Send> {
        self.inner.checkpoint()
    }

    fn restore(&mut self, state: Box<dyn Any + Send>) {
        self.inner.restore(state);
    }

    fn on_start(&mut self, ctx: &mut SimulationContext) {
        self.inner.on_start(ctx);
    }
}

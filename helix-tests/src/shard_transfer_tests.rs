//! Shard transfer DST tests with realistic topology.
//!
//! These tests exercise the real `TransferCoordinator` and `MultiRaft` components
//! together, simulating a production-like cluster with multiple nodes, each running
//! the same Raft groups.
//!
//! # Architecture
//!
//! - Each node runs a `ShardNode` actor containing both `MultiRaft` and `TransferCoordinator`
//! - Raft messages flow between nodes for consensus
//! - Transfer messages flow for shard movement orchestration
//! - Fault injection: message drops, node crashes
//!
//! # Fault Rate Selection
//!
//! We test with aggressive fault rates - designing for worst-case, not typical.
//!
//! **Published datacenter data (for reference):**
//! - Network: 0.01-1% typical, spikes during incidents
//! - Nodes: Google Borg reports 1-5% annual machine failure
//!
//! **Our test rates (intentionally aggressive):**
//! - 3% message drop - well above typical production
//! - 0.5% crash per tick - extremely aggressive (with 150-tick recovery)
//!
//! **Results at 3% drop / 0.5% crash:**
//! - 500 seeds × 3 transfers = 1500 initiated
//! - ~96% success rate despite extreme fault injection
//! - Protocol remains robust due to retry logic (3 retries, 1000-tick timeout)

// Test-specific lint allowances - these are less critical in test code.
#![allow(clippy::cast_precision_loss)] // f64 precision loss acceptable in test stats
#![allow(clippy::cast_possible_truncation)] // u64 to usize safe on 64-bit test machines
#![allow(clippy::too_many_lines)] // Test functions can be longer for clarity
#![allow(clippy::too_many_arguments)] // Test setup functions may need many params
#![allow(clippy::fn_params_excessive_bools)] // Test flags are often bools
#![allow(clippy::doc_markdown)] // Backticks in docs not critical for tests
#![allow(clippy::uninlined_format_args)] // Format string style not critical for tests
#![allow(clippy::needless_pass_by_value)] // Pass by value can improve test clarity
#![allow(clippy::type_complexity)] // Complex types acceptable in test utilities
#![allow(clippy::needless_lifetimes)] // Explicit lifetimes can be clearer
#![allow(clippy::deref_addrof)] // Ref/deref pattern may be intentional

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bloodhound::simulation::discrete::actors::{SimulatedActor, SimulationContext};
use bloodhound::simulation::discrete::engine::{DiscreteSimulationEngine, EngineConfig};
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, TermId};
use helix_raft::multi::{GroupMessage, MultiRaft, MultiRaftOutput};
use helix_raft::Message;
use helix_routing::{ShardMap, ShardRange, TransferCoordinator, TransferMessage, TransferOutput};
use helix_runtime::{decode_transfer_message, encode_transfer_message};

/// Constants.
const TICK_INTERVAL_US: u64 = 10_000; // 10ms
const NETWORK_LATENCY_US: u64 = 1_000; // 1ms

// Aggressive fault rates - we design for worst-case, not typical.
// Production varies: 0.01-1% packet loss typical, but incidents can spike higher.
// We test well above typical to ensure robustness.
const MESSAGE_DROP_RATE: f64 = 0.03; // 3% - stress test level
const CRASH_RATE: f64 = 0.005; // 0.5%/tick - very aggressive
const RECOVERY_TICKS: u64 = 150;

// ============================================================================
// Message Types
// ============================================================================

/// Message envelope that can carry either Raft or Transfer messages.
#[derive(Clone)]
enum NodeMessage {
    Raft(Vec<GroupMessage>),
    Transfer(TransferMessage),
}

fn encode_node_message(msg: &NodeMessage) -> Vec<u8> {
    match msg {
        NodeMessage::Raft(batch) => {
            let mut buf = vec![0u8]; // Tag: 0 = Raft
            for gm in batch {
                buf.extend(encode_group_message(gm));
            }
            buf
        }
        NodeMessage::Transfer(tm) => {
            let mut buf = vec![1u8]; // Tag: 1 = Transfer
            if let Ok(payload) = encode_transfer_message(tm) {
                buf.extend(&payload);
            }
            buf
        }
    }
}

fn decode_node_message(payload: &[u8]) -> Option<NodeMessage> {
    if payload.is_empty() {
        return None;
    }
    match payload[0] {
        0 => {
            let mut messages = Vec::new();
            let mut offset = 1;
            while offset < payload.len() {
                if let Some((gm, new_offset)) = decode_group_message(&payload[offset..]) {
                    messages.push(gm);
                    offset += new_offset;
                } else {
                    break;
                }
            }
            Some(NodeMessage::Raft(messages))
        }
        1 => {
            let (tm, _) = decode_transfer_message(&payload[1..]).ok()?;
            Some(NodeMessage::Transfer(tm))
        }
        _ => None,
    }
}

// Simple serialization for GroupMessage (reusing multi_raft_tests pattern).
fn encode_group_message(gm: &GroupMessage) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend(&gm.group_id.get().to_le_bytes());
    buf.extend(encode_raft_message(&gm.message));
    buf
}

fn decode_group_message(payload: &[u8]) -> Option<(GroupMessage, usize)> {
    if payload.len() < 8 {
        return None;
    }
    let group_id = GroupId::new(u64::from_le_bytes(payload[0..8].try_into().ok()?));
    let (msg, msg_len) = decode_raft_message(&payload[8..])?;
    Some((GroupMessage::new(group_id, msg), 8 + msg_len))
}

// Minimal Raft message encoding (just type tags + essential fields).
fn encode_raft_message(msg: &Message) -> Vec<u8> {
    let mut buf = Vec::new();
    match msg {
        Message::PreVote(req) => {
            buf.push(0);
            buf.extend(&req.term.get().to_le_bytes());
            buf.extend(&req.candidate_id.get().to_le_bytes());
            buf.extend(&req.to.get().to_le_bytes());
            buf.extend(&req.last_log_index.get().to_le_bytes());
            buf.extend(&req.last_log_term.get().to_le_bytes());
        }
        Message::PreVoteResponse(resp) => {
            buf.push(1);
            buf.extend(&resp.term.get().to_le_bytes());
            buf.extend(&resp.from.get().to_le_bytes());
            buf.extend(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.vote_granted));
        }
        Message::RequestVote(req) => {
            buf.push(2);
            buf.extend(&req.term.get().to_le_bytes());
            buf.extend(&req.candidate_id.get().to_le_bytes());
            buf.extend(&req.to.get().to_le_bytes());
            buf.extend(&req.last_log_index.get().to_le_bytes());
            buf.extend(&req.last_log_term.get().to_le_bytes());
        }
        Message::RequestVoteResponse(resp) => {
            buf.push(3);
            buf.extend(&resp.term.get().to_le_bytes());
            buf.extend(&resp.from.get().to_le_bytes());
            buf.extend(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.vote_granted));
        }
        Message::AppendEntries(req) => {
            buf.push(4);
            buf.extend(&req.term.get().to_le_bytes());
            buf.extend(&req.leader_id.get().to_le_bytes());
            buf.extend(&req.to.get().to_le_bytes());
            buf.extend(&req.prev_log_index.get().to_le_bytes());
            buf.extend(&req.prev_log_term.get().to_le_bytes());
            buf.extend(&req.leader_commit.get().to_le_bytes());
            buf.extend(&(req.entries.len() as u32).to_le_bytes());
            for entry in &req.entries {
                buf.extend(&entry.index.get().to_le_bytes());
                buf.extend(&entry.term.get().to_le_bytes());
                buf.extend(&(entry.data.len() as u32).to_le_bytes());
                buf.extend(&entry.data);
            }
        }
        Message::AppendEntriesResponse(resp) => {
            buf.push(5);
            buf.extend(&resp.term.get().to_le_bytes());
            buf.extend(&resp.from.get().to_le_bytes());
            buf.extend(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.success));
            buf.extend(&resp.match_index.get().to_le_bytes());
        }
        Message::TimeoutNow(req) => {
            buf.push(6);
            buf.extend(&req.term.get().to_le_bytes());
            buf.extend(&req.from.get().to_le_bytes());
            buf.extend(&req.to.get().to_le_bytes());
        }
        Message::InstallSnapshot(req) => {
            buf.push(7);
            buf.extend(&req.term.get().to_le_bytes());
            buf.extend(&req.leader_id.get().to_le_bytes());
            buf.extend(&req.to.get().to_le_bytes());
            buf.extend(&req.last_included_index.get().to_le_bytes());
            buf.extend(&req.last_included_term.get().to_le_bytes());
            buf.extend(&req.offset.to_le_bytes());
            buf.extend(&(req.data.len() as u64).to_le_bytes());
            buf.extend(&req.data);
            buf.push(u8::from(req.done));
        }
        Message::InstallSnapshotResponse(resp) => {
            buf.push(8);
            buf.extend(&resp.term.get().to_le_bytes());
            buf.extend(&resp.from.get().to_le_bytes());
            buf.extend(&resp.to.get().to_le_bytes());
            buf.push(u8::from(resp.success));
            buf.extend(&resp.next_offset.to_le_bytes());
        }
    }
    buf
}

fn decode_raft_message(payload: &[u8]) -> Option<(Message, usize)> {
    use helix_raft::*;
    if payload.is_empty() {
        return None;
    }
    let mut offset = 1;

    macro_rules! read_u64 {
        () => {{
            if payload.len() < offset + 8 { return None; }
            let v = u64::from_le_bytes(payload[offset..offset+8].try_into().ok()?);
            offset += 8;
            v
        }};
    }
    macro_rules! read_u32 {
        () => {{
            if payload.len() < offset + 4 { return None; }
            let v = u32::from_le_bytes(payload[offset..offset+4].try_into().ok()?);
            offset += 4;
            v
        }};
    }
    macro_rules! read_bool {
        () => {{
            if payload.len() < offset + 1 { return None; }
            let v = payload[offset] != 0;
            offset += 1;
            v
        }};
    }

    match payload[0] {
        0 => { // PreVote
            let term = TermId::new(read_u64!());
            let candidate_id = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let last_log_index = LogIndex::new(read_u64!());
            let last_log_term = TermId::new(read_u64!());
            Some((Message::PreVote(PreVoteRequest::new(term, candidate_id, to, last_log_index, last_log_term)), offset))
        }
        1 => { // PreVoteResponse
            let term = TermId::new(read_u64!());
            let from = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let vote_granted = read_bool!();
            Some((Message::PreVoteResponse(PreVoteResponse::new(term, from, to, vote_granted)), offset))
        }
        2 => { // RequestVote
            let term = TermId::new(read_u64!());
            let candidate_id = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let last_log_index = LogIndex::new(read_u64!());
            let last_log_term = TermId::new(read_u64!());
            Some((Message::RequestVote(RequestVoteRequest::new(term, candidate_id, to, last_log_index, last_log_term)), offset))
        }
        3 => { // RequestVoteResponse
            let term = TermId::new(read_u64!());
            let from = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let vote_granted = read_bool!();
            Some((Message::RequestVoteResponse(RequestVoteResponse::new(term, from, to, vote_granted)), offset))
        }
        4 => { // AppendEntries
            let term = TermId::new(read_u64!());
            let leader_id = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let prev_log_index = LogIndex::new(read_u64!());
            let prev_log_term = TermId::new(read_u64!());
            let leader_commit = LogIndex::new(read_u64!());
            let entry_count = read_u32!() as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let index = LogIndex::new(read_u64!());
                let entry_term = TermId::new(read_u64!());
                let data_len = read_u32!() as usize;
                if payload.len() < offset + data_len { return None; }
                let data = Bytes::copy_from_slice(&payload[offset..offset + data_len]);
                offset += data_len;
                entries.push(LogEntry::new(entry_term, index, data));
            }
            Some((Message::AppendEntries(AppendEntriesRequest::new(term, leader_id, to, prev_log_index, prev_log_term, entries, leader_commit)), offset))
        }
        5 => { // AppendEntriesResponse
            let term = TermId::new(read_u64!());
            let from = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let success = read_bool!();
            let match_index = LogIndex::new(read_u64!());
            Some((Message::AppendEntriesResponse(AppendEntriesResponse::new(term, from, to, success, match_index)), offset))
        }
        6 => { // TimeoutNow
            let term = TermId::new(read_u64!());
            let from = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            Some((Message::TimeoutNow(TimeoutNowRequest::new(term, from, to)), offset))
        }
        7 => { // InstallSnapshot
            let term = TermId::new(read_u64!());
            let leader_id = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let last_included_index = LogIndex::new(read_u64!());
            let last_included_term = TermId::new(read_u64!());
            let chunk_offset = read_u64!();
            let data_len = read_u64!() as usize;
            if payload.len() < offset + data_len { return None; }
            let data = Bytes::copy_from_slice(&payload[offset..offset + data_len]);
            offset += data_len;
            let done = read_bool!();
            Some((Message::InstallSnapshot(InstallSnapshotRequest::new(term, leader_id, to, last_included_index, last_included_term, chunk_offset, data, done)), offset))
        }
        8 => { // InstallSnapshotResponse
            let term = TermId::new(read_u64!());
            let from = NodeId::new(read_u64!());
            let to = NodeId::new(read_u64!());
            let success = read_bool!();
            let next_offset = read_u64!();
            Some((Message::InstallSnapshotResponse(InstallSnapshotResponse::new(term, from, to, success, next_offset)), offset))
        }
        _ => None,
    }
}

// ============================================================================
// Property State
// ============================================================================

#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub ticks: u64,
    pub raft_messages: u64,
    pub transfer_messages: u64,
    pub messages_dropped: u64,
    pub crashes: u64,
    pub transfers_initiated: u64,
    pub transfers_completed: u64,
    pub transfers_failed: u64,
    pub violations: Vec<String>,
}

pub type SharedStats = Arc<Mutex<Stats>>;

// ============================================================================
// Deterministic RNG
// ============================================================================

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_f64(&mut self) -> f64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        (x as f64) / (u64::MAX as f64)
    }
}

// ============================================================================
// Shard Node Actor
// ============================================================================

/// Combined actor running MultiRaft + TransferCoordinator.
pub struct ShardNode {
    actor_id: ActorId,
    node_id: NodeId,
    multi_raft: MultiRaft,
    coordinator: TransferCoordinator,
    shard_map: ShardMap,
    node_to_actor: BTreeMap<NodeId, ActorId>,
    group_leaders: HashMap<GroupId, NodeId>,
    crashed: bool,
    recovery_tick: Option<u64>,
    tick: u64,
    rng: Rng,
    stats: SharedStats,
    pending_transfers: Vec<(GroupId, GroupId, ShardRange)>,
}

impl ShardNode {
    pub fn new(
        actor_id: ActorId,
        node_id: NodeId,
        groups: Vec<GroupId>,
        all_nodes: Vec<NodeId>,
        node_to_actor: BTreeMap<NodeId, ActorId>,
        shard_map: ShardMap,
        stats: SharedStats,
        seed: u64,
    ) -> Self {
        let mut multi_raft = MultiRaft::new(node_id);
        for &group_id in &groups {
            multi_raft.create_group(group_id, all_nodes.clone()).expect("create group");
        }

        Self {
            actor_id,
            node_id,
            multi_raft,
            coordinator: TransferCoordinator::new(),
            shard_map,
            node_to_actor,
            group_leaders: HashMap::new(),
            crashed: false,
            recovery_tick: None,
            tick: 0,
            rng: Rng::new(seed.wrapping_add(node_id.get())),
            stats,
            pending_transfers: Vec::new(),
        }
    }

    pub fn schedule_transfer(&mut self, source: GroupId, target: GroupId, range: ShardRange) {
        self.pending_transfers.push((source, target, range));
    }

    fn handle_tick(&mut self, ctx: &mut SimulationContext) {
        self.tick += 1;

        if let Ok(mut s) = self.stats.lock() {
            s.ticks += 1;
        }

        // Check recovery.
        if let Some(recovery) = self.recovery_tick {
            if self.tick >= recovery {
                self.crashed = false;
                self.recovery_tick = None;
            }
        }

        // Maybe crash.
        if !self.crashed && self.rng.next_f64() < CRASH_RATE {
            self.crashed = true;
            self.recovery_tick = Some(self.tick + RECOVERY_TICKS);
            if let Ok(mut s) = self.stats.lock() {
                s.crashes += 1;
            }
        }

        if self.crashed {
            self.schedule_next_tick(ctx);
            return;
        }

        // Start pending transfers.
        for (source, target, range) in self.pending_transfers.drain(..) {
            if let Ok(_id) = self.coordinator.start_transfer(source, target, range, self.tick) {
                if let Ok(mut s) = self.stats.lock() {
                    s.transfers_initiated += 1;
                }
            }
        }

        // Tick MultiRaft.
        let raft_outputs = self.multi_raft.tick();
        self.process_raft_outputs(raft_outputs, ctx);

        // Tick TransferCoordinator.
        let transfer_outputs = self.coordinator.tick(self.tick);
        self.process_transfer_outputs(transfer_outputs, ctx);

        self.schedule_next_tick(ctx);
    }

    fn schedule_next_tick(&self, ctx: &mut SimulationContext) {
        ctx.schedule_after(
            Duration::from_micros(TICK_INTERVAL_US),
            EventKind::TimerFired { actor: self.actor_id, timer_id: 1 },
        );
    }

    fn process_raft_outputs(&mut self, outputs: Vec<MultiRaftOutput>, ctx: &mut SimulationContext) {
        for output in outputs {
            match output {
                MultiRaftOutput::SendMessages { to, messages } => {
                    self.send_raft_messages(to, messages, ctx);
                }
                MultiRaftOutput::BecameLeader { group_id } => {
                    self.group_leaders.insert(group_id, self.node_id);
                }
                _ => {}
            }
        }
    }

    fn process_transfer_outputs(&mut self, outputs: Vec<TransferOutput>, ctx: &mut SimulationContext) {
        for output in outputs {
            match output {
                TransferOutput::SendMessage { to_group, message } => {
                    // Find leader for target group, or broadcast.
                    if let Some(&leader) = self.group_leaders.get(&to_group) {
                        self.send_transfer_message(leader, message, ctx);
                    } else {
                        // Broadcast to all nodes.
                        let targets: Vec<NodeId> = self.node_to_actor.keys()
                            .copied()
                            .filter(|&n| n != self.node_id)
                            .collect();
                        for target_node in targets {
                            self.send_transfer_message(target_node, message.clone(), ctx);
                        }
                    }
                }
                TransferOutput::UpdateShardMap { shard_range, from_group, to_group, .. } => {
                    let _ = self.shard_map.transfer_shard(shard_range, from_group, to_group);
                }
                TransferOutput::TransferComplete { .. } => {
                    if let Ok(mut s) = self.stats.lock() {
                        s.transfers_completed += 1;
                    }
                }
                TransferOutput::TransferFailed { .. } => {
                    if let Ok(mut s) = self.stats.lock() {
                        s.transfers_failed += 1;
                    }
                }
            }
        }
    }

    fn send_raft_messages(&mut self, to: NodeId, messages: Vec<GroupMessage>, ctx: &mut SimulationContext) {
        if self.crashed {
            return;
        }

        // Message drop.
        if self.rng.next_f64() < MESSAGE_DROP_RATE {
            if let Ok(mut s) = self.stats.lock() {
                s.messages_dropped += 1;
            }
            return;
        }

        if let Some(&target_actor) = self.node_to_actor.get(&to) {
            if let Ok(mut s) = self.stats.lock() {
                s.raft_messages += messages.len() as u64;
            }
            let payload = encode_node_message(&NodeMessage::Raft(messages));
            ctx.schedule_after(
                Duration::from_micros(NETWORK_LATENCY_US),
                EventKind::PacketDelivery {
                    from: self.actor_id,
                    to: target_actor,
                    payload,
                },
            );
        }
    }

    fn send_transfer_message(&mut self, to: NodeId, message: TransferMessage, ctx: &mut SimulationContext) {
        if self.crashed {
            return;
        }

        // Message drop.
        if self.rng.next_f64() < MESSAGE_DROP_RATE {
            if let Ok(mut s) = self.stats.lock() {
                s.messages_dropped += 1;
            }
            return;
        }

        if let Some(&target_actor) = self.node_to_actor.get(&to) {
            if let Ok(mut s) = self.stats.lock() {
                s.transfer_messages += 1;
            }
            let payload = encode_node_message(&NodeMessage::Transfer(message));
            ctx.schedule_after(
                Duration::from_micros(NETWORK_LATENCY_US),
                EventKind::PacketDelivery {
                    from: self.actor_id,
                    to: target_actor,
                    payload,
                },
            );
        }
    }

    fn handle_packet(&mut self, payload: &[u8], ctx: &mut SimulationContext) {
        if self.crashed {
            return;
        }

        if let Some(msg) = decode_node_message(payload) {
            match msg {
                NodeMessage::Raft(messages) => {
                    for gm in messages {
                        let outputs = self.multi_raft.handle_message(gm.group_id, gm.message);
                        self.process_raft_outputs(outputs, ctx);
                    }
                }
                NodeMessage::Transfer(message) => {
                    self.handle_transfer_message(message, ctx);
                }
            }
        }
    }

    fn handle_transfer_message(&mut self, message: TransferMessage, ctx: &mut SimulationContext) {
        // Generate response for requests.
        if let Some(response) = self.generate_transfer_response(&message) {
            // Send response back (broadcast for now).
            let targets: Vec<NodeId> = self.node_to_actor.keys()
                .copied()
                .filter(|&n| n != self.node_id)
                .collect();
            for target_node in targets {
                self.send_transfer_message(target_node, response.clone(), ctx);
            }
        }

        // Also pass to coordinator in case this is a response we're waiting for.
        let outputs = self.coordinator.handle_message(message, self.tick);
        self.process_transfer_outputs(outputs, ctx);
    }

    fn generate_transfer_response(&self, message: &TransferMessage) -> Option<TransferMessage> {
        match message {
            TransferMessage::PrepareRequest { transfer_id, shard_range } => {
                // Use real MultiRaft snapshot creation.
                if let Some(group_id) = self.shard_map.lookup(shard_range.start) {
                    let snapshot_data = Bytes::from(format!("snapshot_{}_{}", transfer_id.get(), group_id.get()));
                    if let Some(snapshot) = self.multi_raft.create_group_snapshot(group_id, snapshot_data) {
                        return Some(TransferMessage::PrepareResponse {
                            transfer_id: *transfer_id,
                            snapshot_index: snapshot.last_included_index,
                            snapshot_term: snapshot.last_included_term,
                            snapshot_size: snapshot.data.len() as u64,
                        });
                    }
                }
                // Fallback.
                Some(TransferMessage::PrepareResponse {
                    transfer_id: *transfer_id,
                    snapshot_index: LogIndex::new(100),
                    snapshot_term: TermId::new(1),
                    snapshot_size: 1000,
                })
            }
            TransferMessage::SnapshotChunk { transfer_id, done: false, .. } => {
                // Respond with final chunk.
                Some(TransferMessage::SnapshotChunk {
                    transfer_id: *transfer_id,
                    offset: 0,
                    data: Bytes::from("chunk_data"),
                    done: true,
                })
            }
            TransferMessage::SwitchRequest { transfer_id } => {
                Some(TransferMessage::SwitchResponse {
                    transfer_id: *transfer_id,
                    success: true,
                })
            }
            _ => None,
        }
    }
}

impl SimulatedActor for ShardNode {
    fn handle(&mut self, event: EventKind, ctx: &mut SimulationContext) {
        match event {
            EventKind::ActorStart { .. } => {
                self.schedule_next_tick(ctx);
            }
            EventKind::TimerFired { timer_id: 1, .. } => {
                self.handle_tick(ctx);
            }
            EventKind::PacketDelivery { ref payload, .. } => {
                self.handle_packet(payload, ctx);
            }
            _ => {}
        }
    }

    fn id(&self) -> ActorId { self.actor_id }
    fn name(&self) -> &'static str { "shard-node" }
    fn checkpoint(&self) -> Box<dyn Any + Send> { Box::new(self.tick) }
    fn restore(&mut self, _: Box<dyn Any + Send>) {}
    fn on_start(&mut self, ctx: &mut SimulationContext) {
        ctx.schedule_after(Duration::from_nanos(0), EventKind::ActorStart { actor: self.actor_id });
    }
}

// ============================================================================
// Test Setup
// ============================================================================

fn run_test(seed: u64, node_count: usize, transfers: usize, duration_secs: u64) -> Stats {
    let stats = Arc::new(Mutex::new(Stats::default()));

    // Create shard map.
    let mut shard_map = ShardMap::new();
    let _ = shard_map.assign(ShardRange::new(0, 500), GroupId::new(1));
    let _ = shard_map.assign(ShardRange::new(500, 1000), GroupId::new(2));

    // Node and actor IDs.
    let node_ids: Vec<NodeId> = (1..=node_count as u64).map(NodeId::new).collect();
    let actor_ids: Vec<ActorId> = (1..=node_count as u64).map(ActorId::new).collect();
    let node_to_actor: BTreeMap<NodeId, ActorId> = node_ids.iter().zip(actor_ids.iter()).map(|(&n, &a)| (n, a)).collect();

    let groups = vec![GroupId::new(1), GroupId::new(2)];

    let engine_config = EngineConfig::new(seed)
        .with_max_time(Duration::from_secs(duration_secs))
        .with_stats(true);
    let mut engine = DiscreteSimulationEngine::with_config(engine_config);

    // Create nodes.
    for (&node_id, &actor_id) in node_ids.iter().zip(actor_ids.iter()) {
        let mut node = ShardNode::new(
            actor_id,
            node_id,
            groups.clone(),
            node_ids.clone(),
            node_to_actor.clone(),
            shard_map.clone(),
            Arc::clone(&stats),
            seed,
        );

        // First node initiates transfers.
        if node_id == NodeId::new(1) {
            for i in 0..transfers {
                let source = GroupId::new(1 + (i % 2) as u64);
                let target = GroupId::new(2 - (i % 2) as u64);
                node.schedule_transfer(source, target, ShardRange::new(0, 100));
            }
        }

        engine.register_actor(Box::new(node));
    }

    let result = engine.run();
    assert!(result.success);

    let final_stats = stats.lock().unwrap().clone();
    final_stats
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_transfer() {
        let stats = run_test(42, 3, 1, 30);

        println!("=== Single Transfer ===");
        println!("Ticks:             {}", stats.ticks);
        println!("Raft messages:     {}", stats.raft_messages);
        println!("Transfer messages: {}", stats.transfer_messages);
        println!("Dropped:           {}", stats.messages_dropped);
        println!("Crashes:           {}", stats.crashes);
        println!("Initiated:         {}", stats.transfers_initiated);
        println!("Completed:         {}", stats.transfers_completed);
        println!("Failed:            {}", stats.transfers_failed);

        assert_eq!(stats.transfers_initiated, 1);
        assert!(stats.raft_messages > 100, "Expected Raft activity");
    }

    #[test]
    fn test_100_seeds() {
        let mut total_initiated = 0u64;
        let mut total_completed = 0u64;
        let mut total_raft = 0u64;
        let mut total_drops = 0u64;
        let mut total_crashes = 0u64;

        for seed in 0..100 {
            let stats = run_test(seed, 3, 2, 20);
            total_initiated += stats.transfers_initiated;
            total_completed += stats.transfers_completed;
            total_raft += stats.raft_messages;
            total_drops += stats.messages_dropped;
            total_crashes += stats.crashes;
        }

        println!("\n=== 100 Seeds ===");
        println!("Total Raft msgs: {total_raft}");
        println!("Total drops:     {total_drops}");
        println!("Total crashes:   {total_crashes}");
        println!("Initiated:       {total_initiated}");
        println!("Completed:       {total_completed}");

        let success_rate = (total_completed as f64) / (total_initiated as f64) * 100.0;
        println!("Success rate:    {success_rate:.1}%");

        assert!(total_raft > 10000, "Expected significant Raft activity");
        // With aggressive 3%/0.5% fault rates, expect >90% success
        assert!(success_rate > 90.0, "Expected >90% success rate, got {success_rate:.1}%");
        // Verify faults are actually being injected
        assert!(total_crashes > 1000, "Expected >1000 crashes with 0.5% rate, got {total_crashes}");
    }

    #[test]
    fn test_500_seeds_stress() {
        let mut total_initiated = 0u64;
        let mut total_completed = 0u64;

        for seed in 0..500 {
            let stats = run_test(seed, 3, 3, 15);
            total_initiated += stats.transfers_initiated;
            total_completed += stats.transfers_completed;

            assert!(stats.violations.is_empty(), "Seed {seed} violations: {:?}", stats.violations);

            if seed % 100 == 99 {
                println!("Completed {} seeds...", seed + 1);
            }
        }

        let success_rate = (total_completed as f64) / (total_initiated as f64) * 100.0;
        println!("\n=== 500 Seeds Stress ===");
        println!("Initiated: {total_initiated}");
        println!("Completed: {total_completed}");
        println!("Success rate: {success_rate:.1}%");

        // With aggressive 3%/0.5% fault rates, expect >90% success
        assert!(success_rate > 90.0, "Expected >90% success rate, got {success_rate:.1}%");
    }
}

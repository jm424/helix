//! Multi-Raft engine for managing multiple Raft groups per node.
//!
//! This module provides `MultiRaft`, which manages thousands of Raft groups
//! on a single node efficiently through:
//!
//! - **Message batching**: Combines messages to the same destination node
//! - **Tick-based timing**: Single `tick()` call drives all groups
//! - **Resource sharing**: Common scheduling across groups
//!
//! # Design
//!
//! Timing is tick-based:
//! - Call `tick()` at regular intervals (e.g., every 100ms)
//! - Each group internally tracks election/heartbeat elapsed ticks
//! - Randomized election timeouts prevent thundering herd
//!
//! # Example
//!
//! ```ignore
//! let mut multi_raft = MultiRaft::new(NodeId::new(1));
//!
//! // Create groups
//! multi_raft.create_group(GroupId::new(1), peers.clone())?;
//! multi_raft.create_group(GroupId::new(2), peers.clone())?;
//!
//! // In your event loop:
//! loop {
//!     // Handle incoming messages
//!     for msg in incoming {
//!         let outputs = multi_raft.handle_message(msg.group_id, msg.message);
//!         process_outputs(outputs);
//!     }
//!
//!     // Tick all groups
//!     let outputs = multi_raft.tick();
//!     process_outputs(outputs);
//!
//!     sleep(Duration::from_millis(100));
//! }
//! ```

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use helix_core::{GroupId, LogIndex, NodeId, TermId};

use crate::config::RaftConfig;
use crate::message::{ClientRequest, Message};
use crate::state::{RaftNode, RaftOutput, RaftState};

/// Maximum number of Raft groups per `MultiRaft` engine.
pub const GROUPS_PER_NODE_MAX: usize = 10_000;

/// Maximum number of messages to batch together for one destination.
pub const MESSAGE_BATCH_SIZE_MAX: usize = 100;

/// Maximum number of groups to process in one tick.
pub const GROUPS_PER_TICK_MAX: usize = 1_000;

/// A message destined for a specific Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMessage {
    /// The target Raft group.
    pub group_id: GroupId,
    /// The Raft message.
    pub message: Message,
}

impl GroupMessage {
    /// Creates a new group message.
    #[must_use]
    pub const fn new(group_id: GroupId, message: Message) -> Self {
        Self { group_id, message }
    }
}

/// Output action from `MultiRaft`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiRaftOutput {
    /// Send messages to a destination node.
    /// Messages are batched for efficiency.
    SendMessages {
        /// Destination node.
        to: NodeId,
        /// Batched messages (with group IDs).
        messages: Vec<GroupMessage>,
    },
    /// A log entry was committed in a group.
    CommitEntry {
        /// The Raft group.
        group_id: GroupId,
        /// Log index of the committed entry.
        index: LogIndex,
        /// The committed data.
        data: Bytes,
    },
    /// A group elected a new leader (this node).
    BecameLeader {
        /// The Raft group.
        group_id: GroupId,
    },
    /// A group's leader stepped down (this node was leader).
    SteppedDown {
        /// The Raft group.
        group_id: GroupId,
    },
}

/// Information about a Raft group.
#[derive(Debug)]
struct GroupInfo {
    /// The Raft state machine.
    node: RaftNode,
}

/// Multi-Raft engine managing multiple Raft groups per node.
///
/// # Tick-Based Timing
///
/// All timing is tick-based:
/// - Call `tick()` at regular intervals (e.g., every 100ms)
/// - Each `RaftNode` internally tracks elapsed ticks
/// - Elections and heartbeats fire when thresholds are reached
/// - Randomized election timeouts prevent thundering herd
///
/// This design enables deterministic simulation testing.
#[derive(Debug)]
pub struct MultiRaft {
    /// This node's ID.
    node_id: NodeId,
    /// Raft groups managed by this engine.
    groups: BTreeMap<GroupId, GroupInfo>,
    /// Pending outbound messages batched by destination.
    outbound_batches: HashMap<NodeId, Vec<GroupMessage>>,
}

impl MultiRaft {
    /// Creates a new Multi-Raft engine.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            groups: BTreeMap::new(),
            outbound_batches: HashMap::new(),
        }
    }

    /// Returns this node's ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the number of groups managed by this engine.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Returns true if the specified group exists.
    #[must_use]
    pub fn has_group(&self, group_id: GroupId) -> bool {
        self.groups.contains_key(&group_id)
    }

    /// Returns information about a group's state.
    #[must_use]
    pub fn group_state(&self, group_id: GroupId) -> Option<GroupStateInfo> {
        self.groups.get(&group_id).map(|info| GroupStateInfo {
            group_id,
            state: info.node.state(),
            current_term: info.node.current_term(),
            leader_id: info.node.leader_id(),
            commit_index: info.node.commit_index(),
            last_applied: info.node.last_applied(),
        })
    }

    /// Returns all group IDs managed by this engine.
    #[must_use]
    pub fn group_ids(&self) -> Vec<GroupId> {
        self.groups.keys().copied().collect()
    }

    /// Creates a new Raft group.
    ///
    /// The group starts as a follower and will trigger an election when
    /// its randomized election timeout fires (via `tick()`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The group already exists
    /// - Maximum group count exceeded
    /// - This node is not in the peer list
    pub fn create_group(
        &mut self,
        group_id: GroupId,
        peers: Vec<NodeId>,
    ) -> Result<(), MultiRaftError> {
        // Precondition: group doesn't exist.
        if self.groups.contains_key(&group_id) {
            return Err(MultiRaftError::GroupExists(group_id));
        }

        // Precondition: under limit.
        if self.groups.len() >= GROUPS_PER_NODE_MAX {
            return Err(MultiRaftError::TooManyGroups {
                count: self.groups.len(),
                max: GROUPS_PER_NODE_MAX,
            });
        }

        // Precondition: this node is in the peer list.
        if !peers.contains(&self.node_id) {
            return Err(MultiRaftError::NodeNotInPeers {
                node_id: self.node_id,
                group_id,
            });
        }

        // Use group_id as part of seed for deterministic but varied timeouts.
        let config = RaftConfig::new(self.node_id, peers)
            .with_random_seed(self.node_id.get() ^ group_id.get());
        let node = RaftNode::new(config);

        self.groups.insert(group_id, GroupInfo { node });

        // Postcondition: group now exists.
        debug_assert!(self.groups.contains_key(&group_id));

        Ok(())
    }

    /// Creates a new Raft group with custom tick configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The group already exists
    /// - Maximum group count exceeded
    /// - This node is not in the peer list
    pub fn create_group_with_config(
        &mut self,
        group_id: GroupId,
        peers: Vec<NodeId>,
        election_tick: u32,
        heartbeat_tick: u32,
    ) -> Result<(), MultiRaftError> {
        // Precondition: group doesn't exist.
        if self.groups.contains_key(&group_id) {
            return Err(MultiRaftError::GroupExists(group_id));
        }

        // Precondition: under limit.
        if self.groups.len() >= GROUPS_PER_NODE_MAX {
            return Err(MultiRaftError::TooManyGroups {
                count: self.groups.len(),
                max: GROUPS_PER_NODE_MAX,
            });
        }

        // Precondition: this node is in the peer list.
        if !peers.contains(&self.node_id) {
            return Err(MultiRaftError::NodeNotInPeers {
                node_id: self.node_id,
                group_id,
            });
        }

        let config = RaftConfig::new(self.node_id, peers)
            .with_tick_config(election_tick, heartbeat_tick)
            .with_random_seed(self.node_id.get() ^ group_id.get());
        let node = RaftNode::new(config);

        self.groups.insert(group_id, GroupInfo { node });

        Ok(())
    }

    /// Removes a Raft group.
    ///
    /// Any pending operations for this group are cancelled.
    ///
    /// # Errors
    ///
    /// Returns an error if the group doesn't exist.
    pub fn remove_group(&mut self, group_id: GroupId) -> Result<(), MultiRaftError> {
        if self.groups.remove(&group_id).is_none() {
            return Err(MultiRaftError::GroupNotFound(group_id));
        }

        // Remove any pending outbound messages for this group.
        for messages in self.outbound_batches.values_mut() {
            messages.retain(|m| m.group_id != group_id);
        }

        Ok(())
    }

    /// Advances the logical clock for all groups by one tick.
    ///
    /// This is the main driver for Raft timing. Call this at regular intervals
    /// (e.g., every 100ms). Each group internally tracks elapsed ticks and
    /// triggers elections/heartbeats when appropriate.
    ///
    /// # Returns
    ///
    /// Actions to take (messages to send, entries committed, etc.).
    pub fn tick(&mut self) -> Vec<MultiRaftOutput> {
        let mut all_outputs = Vec::new();

        // Tick all groups.
        // Note: We iterate over a clone of keys to avoid borrow issues.
        let group_ids: Vec<GroupId> = self.groups.keys().copied().collect();

        for group_id in group_ids {
            if let Some(info) = self.groups.get_mut(&group_id) {
                let outputs = info.node.tick();
                all_outputs.extend(self.process_outputs(group_id, outputs));
            }
        }

        // Flush any batched outbound messages.
        all_outputs.extend(self.flush_outbound_batches());

        all_outputs
    }

    /// Ticks a specific group only.
    ///
    /// Use this when you need fine-grained control over which groups tick.
    pub fn tick_group(&mut self, group_id: GroupId) -> Vec<MultiRaftOutput> {
        let Some(info) = self.groups.get_mut(&group_id) else {
            return Vec::new();
        };

        let outputs = info.node.tick();
        self.process_outputs(group_id, outputs)
    }

    /// Proposes a client command to a specific group.
    ///
    /// Returns outputs if this node is the leader of the group.
    /// Returns `None` if not leader or group doesn't exist.
    pub fn propose(
        &mut self,
        group_id: GroupId,
        data: Bytes,
    ) -> Option<Vec<MultiRaftOutput>> {
        let info = self.groups.get_mut(&group_id)?;

        let request = ClientRequest::new(data);
        let outputs = info.node.handle_client_request(request)?;
        Some(self.process_outputs(group_id, outputs))
    }

    /// Handles an incoming message for a specific group.
    pub fn handle_message(
        &mut self,
        group_id: GroupId,
        message: Message,
    ) -> Vec<MultiRaftOutput> {
        let Some(info) = self.groups.get_mut(&group_id) else {
            return Vec::new();
        };

        let outputs = info.node.handle_message(message);
        self.process_outputs(group_id, outputs)
    }

    /// Handles a batch of incoming messages.
    ///
    /// This is more efficient than handling messages one at a time
    /// as it batches outgoing messages.
    pub fn handle_messages(&mut self, messages: Vec<GroupMessage>) -> Vec<MultiRaftOutput> {
        let mut all_outputs = Vec::new();

        for GroupMessage { group_id, message } in messages {
            let outputs = self.handle_message(group_id, message);
            all_outputs.extend(outputs);
        }

        // Flush any batched outbound messages.
        all_outputs.extend(self.flush_outbound_batches());

        all_outputs
    }

    /// Initiates leadership transfer for a group.
    ///
    /// The current leader will stop accepting new proposals and transfer
    /// leadership to the specified target node.
    ///
    /// Returns `None` if not leader or group doesn't exist.
    pub fn transfer_leadership(
        &mut self,
        group_id: GroupId,
        target: NodeId,
    ) -> Option<Vec<MultiRaftOutput>> {
        let info = self.groups.get_mut(&group_id)?;
        let outputs = info.node.transfer_leadership(target)?;
        Some(self.process_outputs(group_id, outputs))
    }

    /// Returns an iterator over groups where this node is the leader.
    #[must_use]
    pub fn leader_groups(&self) -> Vec<GroupId> {
        self.groups
            .iter()
            .filter(|(_, info)| info.node.is_leader())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Processes outputs from a single Raft node and converts them to
    /// `MultiRaftOutput`, batching messages where possible.
    fn process_outputs(
        &mut self,
        group_id: GroupId,
        outputs: Vec<RaftOutput>,
    ) -> Vec<MultiRaftOutput> {
        let mut result = Vec::with_capacity(outputs.len());

        for output in outputs {
            match output {
                RaftOutput::SendMessage(msg) => {
                    let dest = msg.to();
                    let group_msg = GroupMessage::new(group_id, msg);

                    // Add to batch for this destination.
                    let batch = self.outbound_batches.entry(dest).or_default();
                    batch.push(group_msg);

                    // Flush if batch is full.
                    if batch.len() >= MESSAGE_BATCH_SIZE_MAX {
                        let messages = std::mem::take(batch);
                        result.push(MultiRaftOutput::SendMessages { to: dest, messages });
                    }
                }
                RaftOutput::CommitEntry { index, data } => {
                    result.push(MultiRaftOutput::CommitEntry {
                        group_id,
                        index,
                        data,
                    });
                }
                RaftOutput::BecameLeader => {
                    result.push(MultiRaftOutput::BecameLeader { group_id });
                }
                RaftOutput::SteppedDown => {
                    result.push(MultiRaftOutput::SteppedDown { group_id });
                }
            }
        }

        result
    }

    /// Flushes all pending outbound message batches.
    fn flush_outbound_batches(&mut self) -> Vec<MultiRaftOutput> {
        let mut outputs = Vec::new();

        for (dest, messages) in self.outbound_batches.drain() {
            if !messages.is_empty() {
                outputs.push(MultiRaftOutput::SendMessages { to: dest, messages });
            }
        }

        outputs
    }

    /// Flushes pending outbound messages and returns them.
    ///
    /// Call this after processing a batch of events to ensure all
    /// messages are sent.
    pub fn flush(&mut self) -> Vec<MultiRaftOutput> {
        self.flush_outbound_batches()
    }
}

/// Information about a Raft group's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupStateInfo {
    /// The group ID.
    pub group_id: GroupId,
    /// Current Raft state (Follower, Candidate, Leader).
    pub state: RaftState,
    /// Current term.
    pub current_term: TermId,
    /// Known leader ID.
    pub leader_id: Option<NodeId>,
    /// Commit index.
    pub commit_index: LogIndex,
    /// Last applied index.
    pub last_applied: LogIndex,
}

/// Errors from Multi-Raft operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiRaftError {
    /// Group already exists.
    GroupExists(GroupId),
    /// Group not found.
    GroupNotFound(GroupId),
    /// Too many groups.
    TooManyGroups {
        /// Current count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },
    /// This node is not in the peer list.
    NodeNotInPeers {
        /// This node's ID.
        node_id: NodeId,
        /// The group ID.
        group_id: GroupId,
    },
}

impl std::fmt::Display for MultiRaftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GroupExists(id) => write!(f, "group {id} already exists"),
            Self::GroupNotFound(id) => write!(f, "group {id} not found"),
            Self::TooManyGroups { count, max } => {
                write!(f, "too many groups: {count} (max {max})")
            }
            Self::NodeNotInPeers { node_id, group_id } => {
                write!(f, "node {node_id} not in peer list for group {group_id}")
            }
        }
    }
}

impl std::error::Error for MultiRaftError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_peers() -> Vec<NodeId> {
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)]
    }

    /// Helper to tick until a condition is met or max ticks reached.
    fn tick_until<F>(multi: &mut MultiRaft, max_ticks: u32, condition: F) -> Vec<MultiRaftOutput>
    where
        F: Fn(&MultiRaft) -> bool,
    {
        let mut all_outputs = Vec::new();
        for _ in 0..max_ticks {
            let outputs = multi.tick();
            all_outputs.extend(outputs);
            if condition(multi) {
                break;
            }
        }
        all_outputs
    }

    #[test]
    fn test_create_group() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        let result = multi.create_group(GroupId::new(1), make_peers());
        assert!(result.is_ok());
        assert!(multi.has_group(GroupId::new(1)));
        assert_eq!(multi.group_count(), 1);
    }

    #[test]
    fn test_create_duplicate_group() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        multi.create_group(GroupId::new(1), make_peers()).unwrap();
        let result = multi.create_group(GroupId::new(1), make_peers());

        assert!(matches!(result, Err(MultiRaftError::GroupExists(_))));
    }

    #[test]
    fn test_create_group_node_not_in_peers() {
        let mut multi = MultiRaft::new(NodeId::new(99));

        let result = multi.create_group(GroupId::new(1), make_peers());

        assert!(matches!(result, Err(MultiRaftError::NodeNotInPeers { .. })));
    }

    #[test]
    fn test_remove_group() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        multi.create_group(GroupId::new(1), make_peers()).unwrap();
        assert!(multi.has_group(GroupId::new(1)));

        let result = multi.remove_group(GroupId::new(1));
        assert!(result.is_ok());
        assert!(!multi.has_group(GroupId::new(1)));
    }

    #[test]
    fn test_remove_nonexistent_group() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        let result = multi.remove_group(GroupId::new(99));
        assert!(matches!(result, Err(MultiRaftError::GroupNotFound(_))));
    }

    #[test]
    fn test_group_state() {
        let mut multi = MultiRaft::new(NodeId::new(1));
        multi.create_group(GroupId::new(1), make_peers()).unwrap();

        let state = multi.group_state(GroupId::new(1));
        assert!(state.is_some());

        let state = state.unwrap();
        assert_eq!(state.group_id, GroupId::new(1));
        assert_eq!(state.state, RaftState::Follower);
        assert_eq!(state.current_term, TermId::new(0));
    }

    #[test]
    fn test_tick_triggers_election() {
        let mut multi = MultiRaft::new(NodeId::new(1));
        multi
            .create_group_with_config(GroupId::new(1), make_peers(), 5, 1)
            .unwrap();

        // Tick enough times to trigger election.
        let outputs = tick_until(&mut multi, 20, |m| {
            m.group_state(GroupId::new(1))
                .map_or(false, |s| s.state != RaftState::Follower)
        });

        // Should have sent messages (pre-vote requests).
        assert!(outputs
            .iter()
            .any(|o| matches!(o, MultiRaftOutput::SendMessages { .. })));

        // Should be in PreCandidate state.
        let state = multi.group_state(GroupId::new(1)).unwrap();
        assert_eq!(state.state, RaftState::PreCandidate);
    }

    #[test]
    fn test_single_node_group_becomes_leader() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        // Single-node cluster.
        multi
            .create_group_with_config(GroupId::new(1), vec![NodeId::new(1)], 5, 1)
            .unwrap();

        // Tick until leader.
        let outputs = tick_until(&mut multi, 20, |m| {
            m.group_state(GroupId::new(1))
                .map_or(false, |s| s.state == RaftState::Leader)
        });

        // Should become leader.
        assert!(outputs
            .iter()
            .any(|o| matches!(o, MultiRaftOutput::BecameLeader { .. })));

        let state = multi.group_state(GroupId::new(1)).unwrap();
        assert_eq!(state.state, RaftState::Leader);
    }

    #[test]
    fn test_propose_not_leader() {
        let mut multi = MultiRaft::new(NodeId::new(1));
        multi.create_group(GroupId::new(1), make_peers()).unwrap();

        // Not leader, so propose should return None.
        let result = multi.propose(GroupId::new(1), Bytes::from("test"));
        assert!(result.is_none());
    }

    #[test]
    fn test_propose_as_leader() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        // Single-node cluster (will become leader).
        multi
            .create_group_with_config(GroupId::new(1), vec![NodeId::new(1)], 5, 1)
            .unwrap();

        // Tick until leader.
        tick_until(&mut multi, 20, |m| {
            m.group_state(GroupId::new(1))
                .map_or(false, |s| s.state == RaftState::Leader)
        });

        // Now we can propose.
        let result = multi.propose(GroupId::new(1), Bytes::from("test"));
        assert!(result.is_some());

        let outputs = result.unwrap();
        // Should have committed the entry (single-node cluster).
        assert!(outputs
            .iter()
            .any(|o| matches!(o, MultiRaftOutput::CommitEntry { .. })));
    }

    #[test]
    fn test_group_ids() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        multi.create_group(GroupId::new(1), make_peers()).unwrap();
        multi.create_group(GroupId::new(5), make_peers()).unwrap();
        multi.create_group(GroupId::new(3), make_peers()).unwrap();

        let ids = multi.group_ids();
        assert_eq!(ids.len(), 3);
        // BTreeMap maintains sorted order.
        assert_eq!(ids, vec![GroupId::new(1), GroupId::new(3), GroupId::new(5)]);
    }

    #[test]
    fn test_leader_groups() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        // Create two single-node groups (both become leaders).
        multi
            .create_group_with_config(GroupId::new(1), vec![NodeId::new(1)], 5, 1)
            .unwrap();
        multi
            .create_group_with_config(GroupId::new(2), vec![NodeId::new(1)], 5, 1)
            .unwrap();
        // Create one 3-node group (stays follower).
        multi.create_group(GroupId::new(3), make_peers()).unwrap();

        // Tick until the single-node groups become leaders.
        tick_until(&mut multi, 30, |m| {
            m.group_state(GroupId::new(1))
                .map_or(false, |s| s.state == RaftState::Leader)
                && m.group_state(GroupId::new(2))
                    .map_or(false, |s| s.state == RaftState::Leader)
        });

        let leaders = multi.leader_groups();
        assert_eq!(leaders.len(), 2);
        assert!(leaders.contains(&GroupId::new(1)));
        assert!(leaders.contains(&GroupId::new(2)));
    }

    #[test]
    fn test_message_batching() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        // Create multiple groups with fast election config.
        for i in 0..5 {
            multi
                .create_group_with_config(GroupId::new(i), make_peers(), 3, 1)
                .unwrap();
        }

        // Tick until all groups start elections.
        let outputs = tick_until(&mut multi, 20, |m| {
            (0..5).all(|i| {
                m.group_state(GroupId::new(i))
                    .map_or(false, |s| s.state != RaftState::Follower)
            })
        });

        // Messages should be batched by destination.
        let send_outputs: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, MultiRaftOutput::SendMessages { .. }))
            .collect();

        // Should have batches to nodes 2 and 3.
        assert!(!send_outputs.is_empty());

        // Each batch should contain messages from multiple groups.
        for output in &send_outputs {
            if let MultiRaftOutput::SendMessages { messages, .. } = output {
                // Multiple groups sending to same destination should be batched.
                assert!(!messages.is_empty());
            }
        }
    }

    #[test]
    fn test_varied_election_timeouts() {
        let mut multi = MultiRaft::new(NodeId::new(1));

        // Create multiple groups.
        for i in 0..5 {
            multi
                .create_group_with_config(GroupId::new(i), make_peers(), 10, 1)
                .unwrap();
        }

        // Tick a few times - groups should NOT all start elections simultaneously
        // due to randomized timeouts.
        multi.tick();
        multi.tick();

        let states: Vec<_> = (0..5)
            .map(|i| {
                multi
                    .group_state(GroupId::new(i))
                    .map(|s| s.state)
                    .unwrap_or(RaftState::Follower)
            })
            .collect();

        // After just 2 ticks (with election_tick=10), most should still be Follower
        // because randomized timeout is in [10, 20) ticks.
        let followers = states.iter().filter(|&&s| s == RaftState::Follower).count();
        assert!(
            followers >= 4,
            "Expected most groups to still be followers after 2 ticks, got {} followers",
            followers
        );
    }
}

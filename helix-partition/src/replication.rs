//! Partition replication using Raft consensus.
//!
//! This module provides replicated partitions that use Raft to ensure
//! durability and consistency across multiple nodes.
//!
//! # Design
//!
//! Each replicated partition has:
//! - A Raft group for consensus
//! - A local partition for storage
//! - A state machine that applies committed entries
//!
//! # Write Path
//!
//! 1. Client sends records to the leader
//! 2. Leader appends to Raft log and replicates
//! 3. Once committed, leader applies to local partition
//! 4. Followers apply after learning commit

use std::collections::HashMap;

use bytes::{BufMut, Bytes, BytesMut};
use helix_core::{LogIndex, NodeId, Offset, PartitionId, Record, TermId, TopicId};
use helix_raft::{ClientRequest, Message, RaftConfig, RaftNode, RaftOutput, RaftState};

use crate::error::{PartitionError, PartitionResult};
use crate::partition::{Partition, PartitionConfig};

/// State of a replicated partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    /// This node is the leader for this partition.
    Leader,
    /// This node is a follower for this partition.
    Follower,
    /// This node is a candidate (election in progress).
    Candidate,
}

/// Configuration for a replicated partition.
#[derive(Debug, Clone)]
pub struct ReplicatedPartitionConfig {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID.
    pub partition_id: PartitionId,
    /// This node's ID.
    pub node_id: NodeId,
    /// All nodes in the replication group.
    pub nodes: Vec<NodeId>,
    /// Partition configuration.
    pub partition_config: PartitionConfig,
}

impl ReplicatedPartitionConfig {
    /// Creates a new replicated partition configuration.
    #[must_use]
    pub fn new(
        topic_id: TopicId,
        partition_id: PartitionId,
        node_id: NodeId,
        nodes: Vec<NodeId>,
    ) -> Self {
        Self {
            topic_id,
            partition_id,
            node_id,
            nodes,
            partition_config: PartitionConfig::new(topic_id, partition_id),
        }
    }
}

/// Command types for the partition state machine.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    /// Append records to the partition.
    Append {
        /// Records to append.
        records: Vec<Record>,
    },
    /// Truncate the partition to a given offset.
    Truncate {
        /// Offset to truncate to (exclusive).
        from_offset: Offset,
    },
    /// Update the high watermark.
    UpdateHighWatermark {
        /// New high watermark.
        high_watermark: Offset,
    },
}

impl PartitionCommand {
    /// Encodes the command to bytes.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        match self {
            Self::Append { records } => {
                buf.put_u8(0); // Command type
                // Safe cast: records.len() is bounded by partition limits which fit in u32.
                #[allow(clippy::cast_possible_truncation)]
                let count = records.len() as u32;
                buf.put_u32_le(count);
                for record in records {
                    record.encode(&mut buf);
                }
            }
            Self::Truncate { from_offset } => {
                buf.put_u8(1);
                buf.put_u64_le(from_offset.get());
            }
            Self::UpdateHighWatermark { high_watermark } => {
                buf.put_u8(2);
                buf.put_u64_le(high_watermark.get());
            }
        }
        buf.freeze()
    }

    /// Decodes a command from bytes.
    #[must_use]
    pub fn decode(data: &[u8]) -> Option<Self> {
        use bytes::Buf;

        if data.is_empty() {
            return None;
        }

        let mut buf = data;
        let cmd_type = buf.get_u8();

        match cmd_type {
            0 => {
                // Append
                if buf.remaining() < 4 {
                    return None;
                }
                let count = buf.get_u32_le() as usize;
                let mut records = Vec::with_capacity(count);
                for _ in 0..count {
                    let record = Record::decode(&mut buf)?;
                    records.push(record);
                }
                Some(Self::Append { records })
            }
            1 => {
                // Truncate
                if buf.remaining() < 8 {
                    return None;
                }
                Some(Self::Truncate {
                    from_offset: Offset::new(buf.get_u64_le()),
                })
            }
            2 => {
                // UpdateHighWatermark
                if buf.remaining() < 8 {
                    return None;
                }
                Some(Self::UpdateHighWatermark {
                    high_watermark: Offset::new(buf.get_u64_le()),
                })
            }
            _ => None,
        }
    }
}

/// Result of applying a command.
#[derive(Debug, Clone)]
pub struct ApplyResult {
    /// Index of the applied entry.
    pub index: LogIndex,
    /// Offset of the first record (for Append commands).
    pub base_offset: Option<Offset>,
}

/// A replicated partition backed by Raft consensus.
pub struct ReplicatedPartition {
    /// Configuration.
    config: ReplicatedPartitionConfig,
    /// Underlying Raft node.
    raft: RaftNode,
    /// Local partition storage.
    partition: Partition,
    /// Last applied Raft index.
    last_applied: LogIndex,
    /// Pending append results (for leader).
    #[allow(dead_code)]
    pending_appends: HashMap<LogIndex, Offset>,
}

impl ReplicatedPartition {
    /// Creates a new replicated partition.
    #[must_use]
    pub fn new(config: ReplicatedPartitionConfig) -> Self {
        let raft_config = RaftConfig::new(config.node_id, config.nodes.clone());
        let raft = RaftNode::new(raft_config);
        let partition = Partition::new(config.partition_config.clone());

        Self {
            config,
            raft,
            partition,
            last_applied: LogIndex::new(0),
            pending_appends: HashMap::new(),
        }
    }

    /// Returns the current replication state.
    #[must_use]
    pub const fn state(&self) -> ReplicationState {
        match self.raft.state() {
            RaftState::Leader => ReplicationState::Leader,
            RaftState::Follower => ReplicationState::Follower,
            RaftState::PreCandidate | RaftState::Candidate => ReplicationState::Candidate,
        }
    }

    /// Returns true if this node is the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.state() == ReplicationState::Leader
    }

    /// Returns the leader node ID if known.
    #[must_use]
    pub const fn leader(&self) -> Option<NodeId> {
        self.raft.leader_id()
    }

    /// Returns the current term.
    #[must_use]
    pub const fn term(&self) -> TermId {
        self.raft.current_term()
    }

    /// Returns the partition ID.
    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.config.partition_id
    }

    /// Returns the topic ID.
    #[must_use]
    pub const fn topic_id(&self) -> TopicId {
        self.config.topic_id
    }

    /// Returns the log start offset.
    #[must_use]
    pub fn log_start_offset(&self) -> Offset {
        self.partition.log_start_offset()
    }

    /// Returns the log end offset.
    #[must_use]
    pub fn log_end_offset(&self) -> Offset {
        self.partition.log_end_offset()
    }

    /// Returns the high watermark (committed offset).
    #[must_use]
    pub const fn high_watermark(&self) -> Offset {
        self.partition.high_watermark()
    }

    /// Returns the underlying Raft node for message handling.
    pub fn raft_mut(&mut self) -> &mut RaftNode {
        &mut self.raft
    }

    /// Returns the underlying Raft node.
    #[must_use]
    pub const fn raft(&self) -> &RaftNode {
        &self.raft
    }

    /// Proposes records to be appended (leader only).
    ///
    /// # Errors
    /// Returns an error if not the leader or proposal fails.
    pub fn propose_append(&mut self, records: Vec<Record>) -> PartitionResult<Option<Vec<RaftOutput>>> {
        if !self.is_leader() {
            return Err(PartitionError::InvalidConfig {
                message: "not the leader".to_string(),
            });
        }

        if records.is_empty() {
            return Ok(None);
        }

        let command = PartitionCommand::Append { records };
        let data = command.encode();

        let request = ClientRequest::new(data);
        let outputs = self.raft.handle_client_request(request);

        Ok(outputs)
    }

    /// Reads records from the partition.
    ///
    /// # Errors
    /// Returns an error if the offset is out of range.
    pub fn read(&self, start_offset: Offset, max_records: u32) -> PartitionResult<Vec<Record>> {
        self.partition.read(start_offset, max_records)
    }

    /// Applies committed entries from Raft outputs.
    ///
    /// # Errors
    /// Returns an error if applying an entry fails.
    pub fn apply_outputs(&mut self, outputs: &[RaftOutput]) -> PartitionResult<Vec<ApplyResult>> {
        let mut results = Vec::new();

        for output in outputs {
            if let RaftOutput::CommitEntry { index, data } = output {
                if let Some(result) = self.apply_entry(*index, data)? {
                    results.push(result);
                }
            }
        }

        Ok(results)
    }

    /// Applies a single log entry.
    fn apply_entry(
        &mut self,
        index: LogIndex,
        data: &Bytes,
    ) -> PartitionResult<Option<ApplyResult>> {
        // Skip entries with empty data (e.g., no-op entries).
        if data.is_empty() {
            return Ok(None);
        }

        let command = PartitionCommand::decode(data).ok_or_else(|| {
            PartitionError::Corruption {
                message: "failed to decode partition command".to_string(),
            }
        })?;

        match command {
            PartitionCommand::Append { records } => {
                let base_offset = self.partition.append(records)?;
                self.last_applied = index;

                // Update high watermark to log end offset.
                // For a replicated partition, the high watermark represents the offset
                // up to which all in-sync replicas have replicated. Since this entry
                // is now committed (by Raft consensus), it's safe to advance the
                // high watermark.
                let new_hwm = self.partition.log_end_offset();
                self.partition.set_high_watermark(new_hwm);

                Ok(Some(ApplyResult {
                    index,
                    base_offset: Some(base_offset),
                }))
            }
            PartitionCommand::Truncate { from_offset } => {
                self.partition.truncate(from_offset)?;
                self.last_applied = index;
                Ok(Some(ApplyResult {
                    index,
                    base_offset: None,
                }))
            }
            PartitionCommand::UpdateHighWatermark { high_watermark } => {
                self.partition.set_high_watermark(high_watermark);
                self.last_applied = index;
                Ok(Some(ApplyResult {
                    index,
                    base_offset: None,
                }))
            }
        }
    }

    /// Advances the Raft state machine by one tick.
    ///
    /// This is the main driver for Raft timing. Call this at regular intervals
    /// (e.g., every 100ms). Elections and heartbeats are triggered internally
    /// based on elapsed ticks.
    pub fn tick(&mut self) -> Vec<RaftOutput> {
        self.raft.tick()
    }

    /// Handles a Raft message from another node.
    pub fn handle_message(&mut self, message: Message) -> Vec<RaftOutput> {
        self.raft.handle_message(message)
    }

    /// Returns partition metadata.
    #[must_use]
    pub fn metadata(&self) -> crate::partition::PartitionMetadata {
        self.partition.metadata()
    }

    /// Returns the Raft commit index.
    #[must_use]
    pub const fn commit_index(&self) -> LogIndex {
        self.raft.commit_index()
    }

    /// Returns the last applied index.
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
}

/// Manager for multiple replicated partitions.
pub struct ReplicationManager {
    /// This node's ID.
    node_id: NodeId,
    /// Replicated partitions by (topic, partition) key.
    partitions: HashMap<(TopicId, PartitionId), ReplicatedPartition>,
}

impl ReplicationManager {
    /// Creates a new replication manager.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            partitions: HashMap::new(),
        }
    }

    /// Adds a replicated partition.
    pub fn add_partition(&mut self, config: ReplicatedPartitionConfig) {
        let key = (config.topic_id, config.partition_id);
        let partition = ReplicatedPartition::new(config);
        self.partitions.insert(key, partition);
    }

    /// Gets a partition by topic and partition ID.
    #[must_use]
    pub fn get(&self, topic_id: TopicId, partition_id: PartitionId) -> Option<&ReplicatedPartition> {
        self.partitions.get(&(topic_id, partition_id))
    }

    /// Gets a mutable partition by topic and partition ID.
    pub fn get_mut(
        &mut self,
        topic_id: TopicId,
        partition_id: PartitionId,
    ) -> Option<&mut ReplicatedPartition> {
        self.partitions.get_mut(&(topic_id, partition_id))
    }

    /// Returns all partitions as an iterator.
    pub fn partitions(&self) -> impl Iterator<Item = &ReplicatedPartition> {
        self.partitions.values()
    }

    /// Returns all partitions mutably.
    pub fn partitions_mut(&mut self) -> impl Iterator<Item = &mut ReplicatedPartition> {
        self.partitions.values_mut()
    }

    /// Ticks all partitions, advancing their Raft state machines.
    ///
    /// This is the main driver for Raft timing. Call this at regular intervals
    /// (e.g., every 100ms). Elections and heartbeats are triggered internally
    /// based on elapsed ticks.
    pub fn tick(&mut self) -> Vec<((TopicId, PartitionId), Vec<RaftOutput>)> {
        self.partitions
            .iter_mut()
            .map(|(&key, partition)| (key, partition.tick()))
            .collect()
    }

    /// Returns the node ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the number of partitions.
    #[must_use]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> ReplicatedPartitionConfig {
        ReplicatedPartitionConfig::new(
            TopicId::new(1),
            PartitionId::new(0),
            NodeId::new(1),
            vec![NodeId::new(1)], // Single node cluster
        )
    }

    fn make_record(value: &str) -> Record {
        Record::new(Bytes::from(value.to_string()))
    }

    #[test]
    fn test_replicated_partition_creation() {
        let partition = ReplicatedPartition::new(make_config());
        assert_eq!(partition.log_start_offset(), Offset::new(0));
        assert_eq!(partition.log_end_offset(), Offset::new(0));
    }

    #[test]
    fn test_command_encode_decode() {
        let records = vec![make_record("test1"), make_record("test2")];
        let cmd = PartitionCommand::Append { records };
        let encoded = cmd.encode();
        let decoded = PartitionCommand::decode(&encoded).unwrap();

        if let PartitionCommand::Append {
            records: decoded_records,
        } = decoded
        {
            assert_eq!(decoded_records.len(), 2);
        } else {
            panic!("expected Append command");
        }
    }

    #[test]
    fn test_truncate_command() {
        let cmd = PartitionCommand::Truncate {
            from_offset: Offset::new(100),
        };
        let encoded = cmd.encode();
        let decoded = PartitionCommand::decode(&encoded).unwrap();

        if let PartitionCommand::Truncate { from_offset } = decoded {
            assert_eq!(from_offset, Offset::new(100));
        } else {
            panic!("expected Truncate command");
        }
    }

    #[test]
    fn test_hwm_command() {
        let cmd = PartitionCommand::UpdateHighWatermark {
            high_watermark: Offset::new(50),
        };
        let encoded = cmd.encode();
        let decoded = PartitionCommand::decode(&encoded).unwrap();

        if let PartitionCommand::UpdateHighWatermark { high_watermark } = decoded {
            assert_eq!(high_watermark, Offset::new(50));
        } else {
            panic!("expected UpdateHighWatermark command");
        }
    }

    #[test]
    fn test_single_node_becomes_leader() {
        let mut partition = ReplicatedPartition::new(make_config());

        // Tick until election timeout triggers - single node cluster should become leader.
        // With default election_tick=10 and randomized timeout in [10, 20), we need up to 20 ticks.
        let mut became_leader = false;
        for _ in 0..25 {
            let outputs = partition.tick();
            if outputs.iter().any(|o| matches!(o, RaftOutput::BecameLeader)) {
                became_leader = true;
                break;
            }
        }

        assert!(became_leader);
        assert!(partition.is_leader());
    }

    #[test]
    fn test_single_node_propose_and_apply() {
        let mut partition = ReplicatedPartition::new(make_config());

        // Become leader by ticking until election triggers.
        for _ in 0..25 {
            let outputs = partition.tick();
            if outputs.iter().any(|o| matches!(o, RaftOutput::BecameLeader)) {
                break;
            }
        }
        assert!(partition.is_leader());

        // Propose records.
        let records = vec![make_record("msg1"), make_record("msg2")];
        let result = partition.propose_append(records);
        assert!(result.is_ok());

        let outputs = result.unwrap();
        assert!(outputs.is_some());

        // Apply committed entries.
        let apply_results = partition.apply_outputs(&outputs.unwrap()).unwrap();
        assert_eq!(apply_results.len(), 1);
        assert_eq!(apply_results[0].base_offset, Some(Offset::new(0)));

        // Verify records were written.
        let read_records = partition.read(Offset::new(0), 10).unwrap();
        assert_eq!(read_records.len(), 2);
    }

    #[test]
    fn test_replication_manager() {
        let mut manager = ReplicationManager::new(NodeId::new(1));

        let config1 = ReplicatedPartitionConfig::new(
            TopicId::new(1),
            PartitionId::new(0),
            NodeId::new(1),
            vec![NodeId::new(1)],
        );

        let config2 = ReplicatedPartitionConfig::new(
            TopicId::new(1),
            PartitionId::new(1),
            NodeId::new(1),
            vec![NodeId::new(1)],
        );

        manager.add_partition(config1);
        manager.add_partition(config2);

        assert_eq!(manager.partition_count(), 2);
        assert!(manager.get(TopicId::new(1), PartitionId::new(0)).is_some());
        assert!(manager.get(TopicId::new(1), PartitionId::new(1)).is_some());
        assert!(manager.get(TopicId::new(2), PartitionId::new(0)).is_none());
    }

    #[test]
    fn test_tick_triggers_election() {
        let mut manager = ReplicationManager::new(NodeId::new(1));

        let config = ReplicatedPartitionConfig::new(
            TopicId::new(1),
            PartitionId::new(0),
            NodeId::new(1),
            vec![NodeId::new(1)],
        );

        manager.add_partition(config);

        // Tick until election triggers.
        for _ in 0..25 {
            let results = manager.tick();
            if results.iter().any(|(_, outputs)| {
                outputs.iter().any(|o| matches!(o, RaftOutput::BecameLeader))
            }) {
                break;
            }
        }

        // Single node should become leader.
        let partition = manager.get(TopicId::new(1), PartitionId::new(0)).unwrap();
        assert!(partition.is_leader());
    }
}

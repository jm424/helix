//! Group ID mapping for topic/partition to Raft group.
//!
//! This module provides bidirectional mapping between (`TopicId`, `PartitionId`)
//! pairs and their corresponding Raft `GroupId`s.

use std::collections::HashMap;

use helix_core::{GroupId, PartitionId, TopicId};

/// Maps between (`TopicId`, `PartitionId`) and `GroupId`.
pub struct GroupMap {
    /// Forward mapping: (`TopicId`, `PartitionId`) -> `GroupId`.
    by_key: HashMap<(TopicId, PartitionId), GroupId>,
    /// Reverse mapping: `GroupId` -> (`TopicId`, `PartitionId`).
    by_group: HashMap<GroupId, (TopicId, PartitionId)>,
    /// Next group ID to assign.
    next_group_id: u64,
}

impl GroupMap {
    /// Creates a new empty group map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_key: HashMap::new(),
            by_group: HashMap::new(),
            next_group_id: 1,
        }
    }

    /// Allocates a new group ID for a topic/partition pair.
    pub fn allocate(&mut self, topic_id: TopicId, partition_id: PartitionId) -> GroupId {
        let key = (topic_id, partition_id);

        // Return existing if already allocated.
        if let Some(&group_id) = self.by_key.get(&key) {
            return group_id;
        }

        // Allocate new group ID.
        let group_id = GroupId::new(self.next_group_id);
        self.next_group_id += 1;

        self.by_key.insert(key, group_id);
        self.by_group.insert(group_id, key);

        group_id
    }

    /// Gets the group ID for a topic/partition pair.
    #[must_use]
    pub fn get(&self, topic_id: TopicId, partition_id: PartitionId) -> Option<GroupId> {
        self.by_key.get(&(topic_id, partition_id)).copied()
    }

    /// Gets the topic/partition pair for a group ID.
    #[must_use]
    pub fn get_key(&self, group_id: GroupId) -> Option<(TopicId, PartitionId)> {
        self.by_group.get(&group_id).copied()
    }

    /// Inserts a mapping with a specific group ID.
    ///
    /// This is used when the group ID is assigned externally (e.g., by the controller).
    pub fn insert(&mut self, topic_id: TopicId, partition_id: PartitionId, group_id: GroupId) {
        let key = (topic_id, partition_id);
        self.by_key.insert(key, group_id);
        self.by_group.insert(group_id, key);
        // Keep next_group_id ahead of any assigned group IDs.
        if group_id.get() >= self.next_group_id {
            self.next_group_id = group_id.get() + 1;
        }
    }
}

impl Default for GroupMap {
    fn default() -> Self {
        Self::new()
    }
}

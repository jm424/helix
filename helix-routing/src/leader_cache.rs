//! Leader cache - tracks known leaders for Raft groups.
//!
//! Caching leader information reduces the latency of finding the correct
//! node to send requests to.

use std::collections::HashMap;

use helix_core::{GroupId, NodeId};

/// Maximum number of leader cache entries.
pub const LEADER_CACHE_ENTRIES_MAX: usize = 100_000;

/// Default TTL for leader cache entries in microseconds (5 seconds).
pub const LEADER_CACHE_TTL_US_DEFAULT: u64 = 5_000_000;

/// Configuration for the leader cache.
#[derive(Debug, Clone, Copy)]
pub struct LeaderCacheConfig {
    /// Maximum number of entries in the cache.
    pub max_entries: usize,
    /// Time-to-live for entries in microseconds.
    pub ttl_us: u64,
}

impl Default for LeaderCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: LEADER_CACHE_ENTRIES_MAX,
            ttl_us: LEADER_CACHE_TTL_US_DEFAULT,
        }
    }
}

/// An entry in the leader cache.
#[derive(Debug, Clone, Copy)]
struct CacheEntry {
    /// The known leader node.
    leader: NodeId,
    /// Timestamp when this entry was last updated (microseconds).
    updated_at_us: u64,
}

/// Cache of known leader nodes for Raft groups.
///
/// The cache helps reduce leader discovery latency by remembering
/// which node was the leader for each group.
#[derive(Debug)]
pub struct LeaderCache {
    /// Configuration.
    config: LeaderCacheConfig,
    /// Cache entries by group ID.
    entries: HashMap<GroupId, CacheEntry>,
}

impl LeaderCache {
    /// Creates a new leader cache with the given configuration.
    #[must_use]
    pub fn new(config: LeaderCacheConfig) -> Self {
        Self {
            config,
            entries: HashMap::new(),
        }
    }

    /// Creates a new leader cache with default configuration.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(LeaderCacheConfig::default())
    }

    /// Returns the cached leader for a group, if known and not expired.
    #[must_use]
    pub fn get(&self, group_id: GroupId, current_time_us: u64) -> Option<NodeId> {
        self.entries.get(&group_id).and_then(|entry| {
            let age = current_time_us.saturating_sub(entry.updated_at_us);
            if age <= self.config.ttl_us {
                Some(entry.leader)
            } else {
                None
            }
        })
    }

    /// Updates the cached leader for a group.
    ///
    /// If the cache is full, the oldest entry is evicted.
    pub fn put(&mut self, group_id: GroupId, leader: NodeId, current_time_us: u64) {
        // Check if we need to evict.
        if self.entries.len() >= self.config.max_entries
            && !self.entries.contains_key(&group_id)
        {
            self.evict_oldest();
        }

        self.entries.insert(
            group_id,
            CacheEntry {
                leader,
                updated_at_us: current_time_us,
            },
        );
    }

    /// Invalidates the cached leader for a group.
    ///
    /// Call this when a request fails due to the cached leader being stale.
    pub fn invalidate(&mut self, group_id: GroupId) {
        self.entries.remove(&group_id);
    }

    /// Invalidates all entries for a specific leader node.
    ///
    /// Call this when a node is known to have failed.
    pub fn invalidate_node(&mut self, node_id: NodeId) {
        self.entries.retain(|_, entry| entry.leader != node_id);
    }

    /// Clears all entries in the cache.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns the number of entries in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Evicts the oldest entry from the cache.
    fn evict_oldest(&mut self) {
        if let Some((&oldest_id, _)) = self
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.updated_at_us)
        {
            self.entries.remove(&oldest_id);
        }
    }

    /// Removes all expired entries.
    pub fn evict_expired(&mut self, current_time_us: u64) {
        self.entries.retain(|_, entry| {
            let age = current_time_us.saturating_sub(entry.updated_at_us);
            age <= self.config.ttl_us
        });
    }
}

impl Default for LeaderCache {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut cache = LeaderCache::with_defaults();
        let group = GroupId::new(1);
        let leader = NodeId::new(42);
        let time = 1_000_000;

        cache.put(group, leader, time);

        assert_eq!(cache.get(group, time), Some(leader));
        assert_eq!(cache.get(group, time + 1_000_000), Some(leader));
    }

    #[test]
    fn test_expiration() {
        let config = LeaderCacheConfig {
            max_entries: 100,
            ttl_us: 1_000_000, // 1 second
        };
        let mut cache = LeaderCache::new(config);
        let group = GroupId::new(1);
        let leader = NodeId::new(42);
        let time = 1_000_000;

        cache.put(group, leader, time);

        // Should be valid immediately.
        assert_eq!(cache.get(group, time), Some(leader));

        // Should be valid at TTL boundary.
        assert_eq!(cache.get(group, time + 1_000_000), Some(leader));

        // Should expire after TTL.
        assert_eq!(cache.get(group, time + 1_000_001), None);
    }

    #[test]
    fn test_invalidate() {
        let mut cache = LeaderCache::with_defaults();
        let group = GroupId::new(1);
        let leader = NodeId::new(42);
        let time = 1_000_000;

        cache.put(group, leader, time);
        assert!(cache.get(group, time).is_some());

        cache.invalidate(group);
        assert!(cache.get(group, time).is_none());
    }

    #[test]
    fn test_invalidate_node() {
        let mut cache = LeaderCache::with_defaults();
        let leader = NodeId::new(42);
        let time = 1_000_000;

        // Add entries for multiple groups with the same leader.
        cache.put(GroupId::new(1), leader, time);
        cache.put(GroupId::new(2), leader, time);
        cache.put(GroupId::new(3), NodeId::new(99), time);

        assert_eq!(cache.len(), 3);

        cache.invalidate_node(leader);

        assert_eq!(cache.len(), 1);
        assert!(cache.get(GroupId::new(1), time).is_none());
        assert!(cache.get(GroupId::new(2), time).is_none());
        assert!(cache.get(GroupId::new(3), time).is_some());
    }

    #[test]
    fn test_eviction() {
        let config = LeaderCacheConfig {
            max_entries: 2,
            ttl_us: 1_000_000,
        };
        let mut cache = LeaderCache::new(config);

        // Fill the cache.
        cache.put(GroupId::new(1), NodeId::new(1), 100);
        cache.put(GroupId::new(2), NodeId::new(2), 200);
        assert_eq!(cache.len(), 2);

        // Adding a third entry should evict the oldest (group 1).
        cache.put(GroupId::new(3), NodeId::new(3), 300);
        assert_eq!(cache.len(), 2);

        // Group 1 should be evicted.
        assert!(cache.get(GroupId::new(1), 300).is_none());
        assert!(cache.get(GroupId::new(2), 300).is_some());
        assert!(cache.get(GroupId::new(3), 300).is_some());
    }

    #[test]
    fn test_evict_expired() {
        let config = LeaderCacheConfig {
            max_entries: 100,
            ttl_us: 1_000_000,
        };
        let mut cache = LeaderCache::new(config);

        cache.put(GroupId::new(1), NodeId::new(1), 100);
        cache.put(GroupId::new(2), NodeId::new(2), 500);
        cache.put(GroupId::new(3), NodeId::new(3), 900);

        assert_eq!(cache.len(), 3);

        // Evict entries older than time 600 + TTL = 1_600_100.
        cache.evict_expired(1_100_101);

        // Only group 3 should remain (it was added at 900, TTL is 1_000_000).
        // Wait, let me recalculate. At time 1_100_101:
        // - Group 1 age = 1_100_101 - 100 = 1_100_001 > 1_000_000, expired
        // - Group 2 age = 1_100_101 - 500 = 1_099_601 > 1_000_000, expired
        // - Group 3 age = 1_100_101 - 900 = 1_099_201 > 1_000_000, expired
        // All should be expired. Let me use a more reasonable time.
        let mut cache2 = LeaderCache::new(config);
        cache2.put(GroupId::new(1), NodeId::new(1), 0);
        cache2.put(GroupId::new(2), NodeId::new(2), 500_000);
        cache2.put(GroupId::new(3), NodeId::new(3), 900_000);

        cache2.evict_expired(1_100_000);
        // - Group 1 age = 1_100_000 - 0 = 1_100_000 > 1_000_000, expired
        // - Group 2 age = 1_100_000 - 500_000 = 600_000 <= 1_000_000, valid
        // - Group 3 age = 1_100_000 - 900_000 = 200_000 <= 1_000_000, valid
        assert_eq!(cache2.len(), 2);
    }

    #[test]
    fn test_update_existing() {
        let mut cache = LeaderCache::with_defaults();
        let group = GroupId::new(1);

        cache.put(group, NodeId::new(1), 100);
        cache.put(group, NodeId::new(2), 200);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(group, 200), Some(NodeId::new(2)));
    }
}

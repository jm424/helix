//! Shard router - routes keys to the correct node.
//!
//! The router combines the shard map (key → group) with the leader cache
//! (group → node) to provide fast key → node routing.

use helix_core::{GroupId, NodeId};
use xxhash_rust::xxh3::xxh3_64;

use crate::leader_cache::{LeaderCache, LeaderCacheConfig};
use crate::shard_map::ShardMap;

/// Maximum number of routing retries.
pub const ROUTING_RETRIES_MAX: u32 = 3;

/// Configuration for the shard router.
#[derive(Debug, Clone)]
pub struct ShardRouterConfig {
    /// Leader cache configuration.
    pub leader_cache: LeaderCacheConfig,
    /// Maximum routing retries.
    pub max_retries: u32,
}

impl Default for ShardRouterConfig {
    fn default() -> Self {
        Self {
            leader_cache: LeaderCacheConfig::default(),
            max_retries: ROUTING_RETRIES_MAX,
        }
    }
}

/// Result of a routing operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteResult {
    /// The target node for the request.
    pub node_id: NodeId,
    /// The Raft group responsible for the key.
    pub group_id: GroupId,
    /// Whether this was a cache hit.
    pub cache_hit: bool,
}

/// Router for directing requests to the correct shard and leader.
///
/// The router maintains:
/// - A shard map for key hash → Raft group mapping
/// - A leader cache for Raft group → leader node mapping
#[derive(Debug)]
pub struct ShardRouter {
    /// Configuration.
    config: ShardRouterConfig,
    /// The shard map.
    shard_map: ShardMap,
    /// Cache of known leaders.
    leader_cache: LeaderCache,
}

impl ShardRouter {
    /// Creates a new router with the given shard map.
    #[must_use]
    pub fn new(config: ShardRouterConfig, shard_map: ShardMap) -> Self {
        Self {
            leader_cache: LeaderCache::new(config.leader_cache),
            config,
            shard_map,
        }
    }

    /// Creates a router with default configuration.
    #[must_use]
    pub fn with_shard_map(shard_map: ShardMap) -> Self {
        Self::new(ShardRouterConfig::default(), shard_map)
    }

    /// Hashes a key to a 32-bit shard hash.
    #[must_use]
    pub fn hash_key(key: &[u8]) -> u32 {
        // Use xxHash for fast, high-quality hashing.
        // Truncate to u32 for shard lookup.
        let hash64 = xxh3_64(key);
        // Use upper bits (often better distributed).
        (hash64 >> 32) as u32
    }

    /// Routes a key to its responsible group.
    ///
    /// Returns the group ID responsible for the key, or None if not found.
    #[must_use]
    pub fn route_to_group(&self, key: &[u8]) -> Option<GroupId> {
        let hash = Self::hash_key(key);
        self.shard_map.lookup(hash)
    }

    /// Routes a key to its responsible node using the leader cache.
    ///
    /// If the leader is cached, returns immediately. Otherwise returns
    /// just the group ID so the caller can discover the leader.
    ///
    /// # Errors
    ///
    /// Returns `RoutingError::NoShardForKey` if no shard covers the key hash.
    /// Returns `RoutingError::LeaderUnknown` if the leader is not cached.
    pub fn route(&self, key: &[u8], current_time_us: u64) -> Result<RouteResult, RoutingError> {
        let hash = Self::hash_key(key);

        // Find the responsible group.
        let group_id = self.shard_map.lookup(hash).ok_or(RoutingError::NoShardForKey { hash })?;

        // Check the leader cache.
        if let Some(node_id) = self.leader_cache.get(group_id, current_time_us) {
            return Ok(RouteResult {
                node_id,
                group_id,
                cache_hit: true,
            });
        }

        Err(RoutingError::LeaderUnknown { group_id })
    }

    /// Routes directly using a pre-computed hash.
    ///
    /// # Errors
    ///
    /// Returns `RoutingError::NoShardForKey` if no shard covers the hash.
    /// Returns `RoutingError::LeaderUnknown` if the leader is not cached.
    pub fn route_hash(
        &self,
        hash: u32,
        current_time_us: u64,
    ) -> Result<RouteResult, RoutingError> {
        let group_id = self.shard_map.lookup(hash).ok_or(RoutingError::NoShardForKey { hash })?;

        if let Some(node_id) = self.leader_cache.get(group_id, current_time_us) {
            return Ok(RouteResult {
                node_id,
                group_id,
                cache_hit: true,
            });
        }

        Err(RoutingError::LeaderUnknown { group_id })
    }

    /// Updates the cached leader for a group.
    ///
    /// Call this when you discover or confirm a leader.
    pub fn update_leader(&mut self, group_id: GroupId, leader: NodeId, current_time_us: u64) {
        self.leader_cache.put(group_id, leader, current_time_us);
    }

    /// Marks a routing attempt as failed.
    ///
    /// This invalidates the cached leader so the next route will
    /// trigger leader discovery.
    pub fn mark_failed(&mut self, group_id: GroupId) {
        self.leader_cache.invalidate(group_id);
    }

    /// Marks a node as failed, invalidating all cached leaders for that node.
    pub fn mark_node_failed(&mut self, node_id: NodeId) {
        self.leader_cache.invalidate_node(node_id);
    }

    /// Updates the shard map.
    ///
    /// Call this when receiving an updated shard map from the control plane.
    pub fn update_shard_map(&mut self, shard_map: ShardMap) {
        self.shard_map = shard_map;
        // Clear the leader cache since group assignments may have changed.
        self.leader_cache.clear();
    }

    /// Returns the current shard map version.
    #[must_use]
    pub const fn shard_map_version(&self) -> u64 {
        self.shard_map.version()
    }

    /// Returns a reference to the shard map.
    #[must_use]
    pub const fn shard_map(&self) -> &ShardMap {
        &self.shard_map
    }

    /// Returns the number of cached leaders.
    #[must_use]
    pub fn cached_leader_count(&self) -> usize {
        self.leader_cache.len()
    }

    /// Evicts expired leader cache entries.
    pub fn evict_expired(&mut self, current_time_us: u64) {
        self.leader_cache.evict_expired(current_time_us);
    }

    /// Returns the configuration.
    #[must_use]
    pub const fn config(&self) -> &ShardRouterConfig {
        &self.config
    }
}

/// Errors from routing operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    /// No shard assignment for the key hash.
    NoShardForKey {
        /// The hash value that couldn't be routed.
        hash: u32,
    },
    /// Leader not known for the group.
    LeaderUnknown {
        /// The group needing leader discovery.
        group_id: GroupId,
    },
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoShardForKey { hash } => {
                write!(f, "no shard assignment for hash {hash:#x}")
            }
            Self::LeaderUnknown { group_id } => {
                write!(f, "leader unknown for group {}", group_id.get())
            }
        }
    }
}

impl std::error::Error for RoutingError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_router() -> ShardRouter {
        let groups = vec![GroupId::new(1), GroupId::new(2), GroupId::new(3)];
        let shard_map = ShardMap::uniform(&groups);
        ShardRouter::with_shard_map(shard_map)
    }

    #[test]
    fn test_hash_key() {
        let hash1 = ShardRouter::hash_key(b"key1");
        let hash2 = ShardRouter::hash_key(b"key2");
        let hash3 = ShardRouter::hash_key(b"key1");

        // Same key should produce same hash.
        assert_eq!(hash1, hash3);
        // Different keys should (usually) produce different hashes.
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_route_to_group() {
        let router = create_test_router();

        // Every key should route to some group.
        let keys: &[&[u8]] = &[b"key1", b"key2", b"another_key", b"test"];
        for key in keys {
            let group = router.route_to_group(key);
            assert!(group.is_some(), "key {:?} should route to a group", key);
        }
    }

    #[test]
    fn test_route_without_cached_leader() {
        let router = create_test_router();
        let time = 1_000_000;

        // Routing should fail with LeaderUnknown since cache is empty.
        let result = router.route(b"key1", time);
        assert!(matches!(result, Err(RoutingError::LeaderUnknown { .. })));
    }

    #[test]
    fn test_route_with_cached_leader() {
        let mut router = create_test_router();
        let time = 1_000_000;

        // Route to get the group.
        let group_id = router.route_to_group(b"key1").unwrap();
        let leader = NodeId::new(42);

        // Cache the leader.
        router.update_leader(group_id, leader, time);

        // Now routing should succeed.
        let result = router.route(b"key1", time).unwrap();
        assert_eq!(result.group_id, group_id);
        assert_eq!(result.node_id, leader);
        assert!(result.cache_hit);
    }

    #[test]
    fn test_mark_failed() {
        let mut router = create_test_router();
        let time = 1_000_000;

        let group_id = router.route_to_group(b"key1").unwrap();
        let leader = NodeId::new(42);

        router.update_leader(group_id, leader, time);
        assert!(router.route(b"key1", time).is_ok());

        // Mark as failed.
        router.mark_failed(group_id);

        // Should now return LeaderUnknown.
        let result = router.route(b"key1", time);
        assert!(matches!(result, Err(RoutingError::LeaderUnknown { .. })));
    }

    #[test]
    fn test_mark_node_failed() {
        let mut router = create_test_router();
        let time = 1_000_000;

        let leader = NodeId::new(42);

        // Cache the same leader for multiple groups.
        router.update_leader(GroupId::new(1), leader, time);
        router.update_leader(GroupId::new(2), leader, time);
        router.update_leader(GroupId::new(3), NodeId::new(99), time);

        assert_eq!(router.cached_leader_count(), 3);

        // Mark the node as failed.
        router.mark_node_failed(leader);

        // Only group 3's leader should remain cached.
        assert_eq!(router.cached_leader_count(), 1);
    }

    #[test]
    fn test_update_shard_map() {
        let mut router = create_test_router();
        let time = 1_000_000;

        // Cache some leaders.
        router.update_leader(GroupId::new(1), NodeId::new(42), time);
        assert_eq!(router.cached_leader_count(), 1);

        // Update the shard map.
        let new_groups = vec![GroupId::new(10), GroupId::new(20)];
        let new_map = ShardMap::uniform(&new_groups);
        router.update_shard_map(new_map);

        // Leader cache should be cleared.
        assert_eq!(router.cached_leader_count(), 0);
        assert_eq!(router.shard_map().shard_count(), 2);
    }

    #[test]
    fn test_consistent_routing() {
        let router = create_test_router();

        // Same key should always route to same group.
        let key = b"consistent_key";
        let group1 = router.route_to_group(key);
        let group2 = router.route_to_group(key);
        let group3 = router.route_to_group(key);

        assert_eq!(group1, group2);
        assert_eq!(group2, group3);
    }

    #[test]
    fn test_route_hash_direct() {
        let mut router = create_test_router();
        let time = 1_000_000;

        let hash = 12345_u32;
        let group_id = router.shard_map.lookup(hash).unwrap();
        let leader = NodeId::new(42);

        router.update_leader(group_id, leader, time);

        let result = router.route_hash(hash, time).unwrap();
        assert_eq!(result.node_id, leader);
        assert_eq!(result.group_id, group_id);
    }
}

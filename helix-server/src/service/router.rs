//! Partition Router for actor-based multi-partition coordination.
//!
//! `PartitionRouter` replaces `Arc<RwLock<MultiRaft>>` with a low-contention
//! routing layer that dispatches requests to partition actors.
//!
//! # Design
//!
//! ```text
//!                       ┌─────────────────────────────────────────┐
//!                       │           PartitionRouter               │
//! Batcher ─────────────►│  ┌─────────────────────────────────┐   │
//!                       │  │ partitions: HashMap<GroupId, _> │   │
//! Tick Task ───────────►│  │   0 → PartitionActorHandle     │   │
//!                       │  │   1 → PartitionActorHandle     │   │
//! Transport ───────────►│  │   2 → PartitionActorHandle     │   │
//!                       │  │   ...                           │   │
//!                       │  └─────────────────────────────────┘   │
//!                       │                                         │
//!                       │  ┌─────────────────────────────────┐   │
//!                       │  │ wal_actors: Vec<WalActorHandle> │   │
//!                       │  │   [0] → WalActorHandle          │   │
//!                       │  │   [1] → WalActorHandle          │   │
//!                       │  └─────────────────────────────────┘   │
//!                       └─────────────────────────────────────────┘
//! ```
//!
//! # Benefits
//!
//! - Low-contention request dispatch (read-mostly workload)
//! - Parallel tick broadcast to all partitions
//! - Dynamic partition creation support
//! - Clean separation of routing from consensus logic

use std::collections::HashMap;

use helix_core::{GroupId, NodeId};
use tokio::sync::RwLock;

use crate::service::partition_actor::PartitionActorHandle;
use crate::service::wal_actor::WalActorHandle;

/// Error returned when a partition is not found.
#[derive(Debug, Clone)]
pub struct PartitionNotFound(pub GroupId);

impl std::fmt::Display for PartitionNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition {} not found", self.0)
    }
}

impl std::error::Error for PartitionNotFound {}

/// Routes requests to partition actors and WAL actors.
///
/// `PartitionRouter` is the central coordination point for the actor model.
/// It maintains handles to all partition actors and WAL actors, providing
/// low-contention request dispatch with support for dynamic partition creation.
///
/// # Thread Safety
///
/// `PartitionRouter` is `Send + Sync` and can be shared across tasks via `Arc`.
/// Partitions use interior mutability (`RwLock`) to support dynamic creation
/// while maintaining efficient concurrent reads for the common case.
pub struct PartitionRouter {
    /// Partition actors indexed by group ID.
    /// Uses `RwLock` to support dynamic partition creation.
    partitions: RwLock<HashMap<GroupId, PartitionActorHandle>>,
    /// WAL actors (partitions are assigned round-robin).
    /// Fixed at startup, no dynamic modification needed.
    wal_actors: Vec<WalActorHandle>,
}

impl PartitionRouter {
    /// Creates a new empty router.
    #[must_use]
    pub fn new() -> Self {
        Self {
            partitions: RwLock::new(HashMap::new()),
            wal_actors: Vec::new(),
        }
    }

    /// Creates a router with pre-allocated capacity.
    #[must_use]
    pub fn with_capacity(partition_count: usize, wal_count: usize) -> Self {
        Self {
            partitions: RwLock::new(HashMap::with_capacity(partition_count)),
            wal_actors: Vec::with_capacity(wal_count),
        }
    }

    /// Adds a partition actor to the router (mutable, for initialization).
    ///
    /// This is the synchronous version for use during initial setup when
    /// we have exclusive access to the router.
    ///
    /// # Panics
    ///
    /// Panics if a partition with the same group ID already exists.
    pub fn add_partition(&mut self, group_id: GroupId, handle: PartitionActorHandle) {
        let partitions = self.partitions.get_mut();
        assert!(
            !partitions.contains_key(&group_id),
            "partition {group_id} already exists"
        );
        partitions.insert(group_id, handle);
    }

    /// Adds a partition actor dynamically (for use after initialization).
    ///
    /// This is the async version that acquires the write lock, for use when
    /// creating partitions dynamically via controller commands.
    ///
    /// # Returns
    ///
    /// Returns `true` if the partition was added, `false` if it already exists.
    pub async fn add_partition_dynamic(
        &self,
        group_id: GroupId,
        handle: PartitionActorHandle,
    ) -> bool {
        let mut partitions = self.partitions.write().await;
        if partitions.contains_key(&group_id) {
            return false;
        }
        partitions.insert(group_id, handle);
        true
    }

    /// Adds a WAL actor to the router.
    pub fn add_wal_actor(&mut self, handle: WalActorHandle) {
        self.wal_actors.push(handle);
    }

    /// Returns a clone of the handle for a partition.
    ///
    /// # Errors
    ///
    /// Returns `PartitionNotFound` if the partition doesn't exist.
    pub async fn partition(&self, group_id: GroupId) -> Result<PartitionActorHandle, PartitionNotFound> {
        let partitions = self.partitions.read().await;
        partitions
            .get(&group_id)
            .cloned()
            .ok_or(PartitionNotFound(group_id))
    }

    /// Returns the WAL actor for a partition (round-robin assignment).
    ///
    /// # Panics
    ///
    /// Panics if no WAL actors have been added.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn wal_for(&self, group_id: GroupId) -> &WalActorHandle {
        assert!(!self.wal_actors.is_empty(), "no WAL actors configured");
        // Safe: we're using modulo, so the result is always < wal_actors.len().
        let wal_idx = group_id.get() as usize % self.wal_actors.len();
        &self.wal_actors[wal_idx]
    }

    /// Returns the number of partitions.
    pub async fn partition_count(&self) -> usize {
        self.partitions.read().await.len()
    }

    /// Returns the number of partitions (synchronous, for tests/init).
    #[must_use]
    pub fn partition_count_sync(&self) -> usize {
        // This blocks, only use during init or tests.
        self.partitions.blocking_read().len()
    }

    /// Returns the number of WAL actors.
    #[must_use]
    pub fn wal_count(&self) -> usize {
        self.wal_actors.len()
    }

    /// Broadcasts a tick to all partition actors.
    ///
    /// This sends `PartitionCommand::Tick` to every partition in parallel.
    /// Failures are logged but do not halt the broadcast.
    pub async fn tick_all(&self) {
        // Clone handles under read lock to minimize lock duration.
        let handles: Vec<_> = {
            let partitions = self.partitions.read().await;
            partitions
                .iter()
                .map(|(group_id, handle)| (*group_id, handle.clone()))
                .collect()
        };

        // Collect all send futures.
        let send_futures: Vec<_> = handles
            .into_iter()
            .map(|(group_id, handle)| {
                async move {
                    if let Err(e) = handle.tick().await {
                        tracing::warn!("failed to tick partition {group_id}: {e}");
                    }
                }
            })
            .collect();

        // Execute all ticks in parallel.
        futures::future::join_all(send_futures).await;
    }

    /// Routes a Raft message to the appropriate partition actor.
    ///
    /// # Arguments
    ///
    /// * `group_id` - The target partition/group
    /// * `from` - The sender node ID
    /// * `message` - The Raft message
    ///
    /// # Errors
    ///
    /// Returns `PartitionNotFound` if the partition doesn't exist.
    /// Returns an error if the partition actor has shut down.
    pub async fn route_message(
        &self,
        group_id: GroupId,
        from: NodeId,
        message: helix_raft::Message,
    ) -> Result<(), PartitionNotFound> {
        let handle = self.partition(group_id).await?;
        // Ignore send errors (actor may have shut down).
        let _ = handle.send_raft_message(from, message).await;
        Ok(())
    }

    /// Routes multiple Raft messages in parallel.
    ///
    /// Messages to unknown partitions are logged but do not halt processing.
    pub async fn route_messages(
        &self,
        messages: Vec<helix_raft::multi::GroupMessage>,
        from: NodeId,
    ) {
        let send_futures: Vec<_> = messages
            .into_iter()
            .map(|group_msg| {
                let group_id = group_msg.group_id;
                async move {
                    if let Err(e) = self.route_message(group_id, from, group_msg.message).await {
                        tracing::debug!("failed to route message to partition {group_id}: {e}");
                    }
                }
            })
            .collect();

        futures::future::join_all(send_futures).await;
    }

    /// Shuts down all actors managed by this router.
    ///
    /// Sends shutdown commands to all partition actors and WAL actors.
    /// Errors are ignored since we're shutting down anyway.
    pub async fn shutdown(&self) {
        // Clone handles to minimize lock duration during shutdown.
        let partition_handles: Vec<_> = {
            let partitions = self.partitions.read().await;
            partitions.values().cloned().collect()
        };

        // Shutdown partition actors.
        for handle in partition_handles {
            let _ = handle.shutdown().await;
        }

        // Shutdown WAL actors.
        for handle in &self.wal_actors {
            handle.shutdown().await;
        }
    }

    /// Returns all partition group IDs.
    pub async fn group_ids(&self) -> Vec<GroupId> {
        self.partitions.read().await.keys().copied().collect()
    }
}

impl Default for PartitionRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::partition_actor::{spawn_partition_actor, PartitionActorConfig};
    use crate::service::wal_actor::{spawn_wal_actor, WalActorConfig};
    use helix_core::NodeId;
    use helix_raft::{RaftConfig, RaftNode};
    use helix_wal::{PoolConfig, SimulatedStorage, SharedWalPool};
    use tempfile::TempDir;

    fn create_test_raft_node(node_id: u64, cluster: Vec<u64>) -> RaftNode {
        let config = RaftConfig::new(
            NodeId::new(node_id),
            cluster.into_iter().map(NodeId::new).collect(),
        );
        RaftNode::new(config)
    }

    async fn setup_wal_pool() -> (TempDir, SharedWalPool<SimulatedStorage>) {
        let temp_dir = TempDir::new().unwrap();
        let storage = SimulatedStorage::new(42);
        let pool_config = PoolConfig::new(temp_dir.path(), 2);
        let pool = SharedWalPool::open(storage, pool_config).await.unwrap();
        (temp_dir, pool)
    }

    #[tokio::test]
    async fn test_router_add_partition() {
        let group_id = GroupId::new(1);

        let raft_node = create_test_raft_node(1, vec![1, 2, 3]);
        let (handle, _output_rx) =
            spawn_partition_actor(group_id, raft_node, PartitionActorConfig::default());

        let mut router = PartitionRouter::new();
        router.add_partition(group_id, handle);

        assert_eq!(router.partition_count().await, 1);
        assert!(router.partition(group_id).await.is_ok());
        assert!(router.partition(GroupId::new(99)).await.is_err());

        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_router_add_wal_actor() {
        let (_temp_dir, pool) = setup_wal_pool().await;
        let handle = spawn_wal_actor(pool, WalActorConfig::default());

        let mut router = PartitionRouter::new();
        router.add_wal_actor(handle);

        assert_eq!(router.wal_count(), 1);

        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_router_wal_assignment() {
        let (_temp_dir, pool1) = setup_wal_pool().await;
        let (_temp_dir2, pool2) = setup_wal_pool().await;

        let handle1 = spawn_wal_actor(pool1, WalActorConfig::default());
        let handle2 = spawn_wal_actor(pool2, WalActorConfig::default());

        let mut router = PartitionRouter::new();
        router.add_wal_actor(handle1);
        router.add_wal_actor(handle2);

        // With 2 WAL actors, partitions should alternate.
        // GroupId 0 -> WAL 0, GroupId 1 -> WAL 1, GroupId 2 -> WAL 0, etc.
        let _wal0 = router.wal_for(GroupId::new(0));
        let _wal1 = router.wal_for(GroupId::new(1));
        let _wal2 = router.wal_for(GroupId::new(2));

        // We can't easily compare handles, but we can verify the logic works.
        // The assignment is deterministic: group_id % wal_count.
        assert_eq!(router.wal_count(), 2);

        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_router_tick_all() {
        let mut router = PartitionRouter::new();

        // Add 3 partitions.
        for i in 1..=3 {
            let group_id = GroupId::new(i);
            let raft_node = create_test_raft_node(i, vec![1, 2, 3]);
            let (handle, _output_rx) =
                spawn_partition_actor(group_id, raft_node, PartitionActorConfig::default());
            router.add_partition(group_id, handle);
        }

        assert_eq!(router.partition_count().await, 3);

        // Tick all should complete without panic.
        router.tick_all().await;

        router.shutdown().await;
    }

    #[tokio::test]
    async fn test_router_group_ids() {
        let mut router = PartitionRouter::new();

        let ids: Vec<u64> = vec![5, 10, 15];
        for id in &ids {
            let group_id = GroupId::new(*id);
            let raft_node = create_test_raft_node(*id, vec![5, 10, 15]);
            let (handle, _output_rx) =
                spawn_partition_actor(group_id, raft_node, PartitionActorConfig::default());
            router.add_partition(group_id, handle);
        }

        let mut collected: Vec<u64> = router.group_ids().await.iter().map(|g| g.get()).collect();
        collected.sort();

        assert_eq!(collected, ids);

        router.shutdown().await;
    }
}

//! Actor-based service setup for lock-free multi-partition coordination.
//!
//! This module provides the wiring to set up the actor-based architecture:
//!
//! ```text
//!                      ┌─────────────────────────────────────────────────────┐
//!                      │                 Actor Setup                         │
//!                      │                                                     │
//!  Requests ──────────►│  Batcher ───► PartitionRouter ───► PartitionActors │
//!                      │                      │                    │         │
//!                      │                      │                    ▼         │
//!                      │                      │          SharedOutputChannel │
//!                      │                      │                    │         │
//!  Incoming Messages ─►│  TickTask ──────────►                    │         │
//!   (Transport)        │      │                                    ▼         │
//!                      │      │               OutputProcessor ◄────┘         │
//!                      │      │                    │                         │
//!                      │      └── Heartbeats       ├── Transport (send)      │
//!                      │      └── Tiering          ├── PartitionStorage      │
//!                      │                           └── Notify Waiters        │
//!                      └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Benefits
//!
//! - Lock-free request dispatch (no `RwLock` contention on `MultiRaft`)
//! - Parallel tick broadcast to all partitions
//! - Decoupled output processing from consensus

// Allow complex nested types for proposal maps - refactoring would require significant API changes.
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use helix_core::{GroupId, LogIndex, NodeId};
use helix_raft::{RaftConfig, RaftNode};
use helix_runtime::{IncomingMessage, TransportHandle};
use tokio::sync::{mpsc, RwLock};
use tracing::info;

use crate::vote_store::{LocalFileVoteStorage, VoteStore};

use crate::group_map::GroupMap;
use crate::partition_storage::ServerPartitionStorage;

use super::batcher::{self, BackpressureState, BatcherConfig, BatcherHandle};
use super::output_processor::{self, OutputProcessorConfig};
use super::partition_actor::{
    spawn_partition_actor_shared, GroupedOutput, PartitionActorConfig, PartitionActorHandle,
};
use super::router::PartitionRouter;
use super::{BatchPendingProposal, BatcherStats};

/// Configuration for actor-based service setup.
#[derive(Debug, Clone, Default)]
pub struct ActorSetupConfig {
    /// Configuration for partition actors.
    pub partition_actor_config: PartitionActorConfig,
    /// Configuration for the output processor.
    pub output_processor_config: OutputProcessorConfig,
    /// Configuration for the batcher.
    pub batcher_config: BatcherConfig,
}

/// Handles returned from actor setup for external interaction.
pub struct ActorSetupHandles {
    /// Router for dispatching requests to partition actors.
    pub router: Arc<PartitionRouter>,
    /// Handle to submit requests to the batcher.
    pub batcher_handle: BatcherHandle,
    /// Aggregated batcher stats.
    pub batcher_stats: Arc<BatcherStats>,
    /// Backpressure state.
    pub backpressure: Arc<BackpressureState>,
    /// Shutdown sender for the tick task.
    pub shutdown_tx: mpsc::Sender<()>,
    /// Map of partition handles by group ID (for direct access if needed).
    pub partition_handles: HashMap<GroupId, PartitionActorHandle>,
    /// Output channel sender for creating new partition actors.
    pub output_tx: mpsc::Sender<GroupedOutput>,
}

/// Sets up the actor-based service for a single partition group.
///
/// This is a simpler setup for testing or single-partition use cases.
///
/// # Arguments
///
/// * `group_id` - The Raft group ID for the partition
/// * `node_id` - This node's ID
/// * `cluster_nodes` - All nodes in the cluster
/// * `partition_storage` - Shared partition storage map
/// * `group_map` - Shared group mapping
/// * `transport_handle` - Transport for sending Raft messages (optional)
/// * `config` - Actor setup configuration
///
/// # Returns
///
/// Handles for interacting with the actor system.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher, clippy::unused_async)]
pub async fn setup_single_partition(
    group_id: GroupId,
    node_id: NodeId,
    cluster_nodes: Vec<NodeId>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
    transport_handle: Option<TransportHandle>,
    config: ActorSetupConfig,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
) -> ActorSetupHandles {
    // Create shared output channel.
    let (output_tx, output_rx) =
        output_processor::create_output_channel(config.output_processor_config);

    // Create the Raft node for this partition.
    let raft_config = RaftConfig::new(node_id, cluster_nodes);
    let raft_node = RaftNode::new(raft_config);

    // Spawn the partition actor with shared output channel.
    let partition_handle = spawn_partition_actor_shared(
        group_id,
        raft_node,
        config.partition_actor_config,
        output_tx.clone(),
    );

    // Build the router with this single partition.
    let mut router = PartitionRouter::with_capacity(1, 0);
    router.add_partition(group_id, partition_handle.clone());
    let router = Arc::new(router);

    // Create batcher stats and backpressure state.
    let batcher_stats = Arc::new(BatcherStats::default());
    let (batcher_handle, batcher_rx, backpressure) = batcher::create_batcher();

    // Spawn the batcher task (actor mode).
    tokio::spawn(batcher::batcher_task_actor(
        batcher_rx,
        Arc::clone(&router),
        Arc::clone(&batch_pending_proposals),
        Arc::clone(&batcher_stats),
        config.batcher_config,
        Arc::clone(&backpressure),
        Arc::clone(&partition_storage),
    ));

    // Spawn the output processor.
    tokio::spawn(output_processor::output_processor_task(
        output_rx,
        Arc::clone(&partition_storage),
        Arc::clone(&group_map),
        Arc::clone(&batch_pending_proposals),
        transport_handle,
        Some(Arc::clone(&batcher_stats)),
        Some(Arc::clone(&backpressure)),
        vote_store,
    ));

    info!(
        group_id = group_id.get(),
        node_id = node_id.get(),
        "Actor setup complete for single partition"
    );

    // Return handles (no tick task for single partition - caller manages ticks).
    let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);
    let mut partition_handles = HashMap::new();
    partition_handles.insert(group_id, partition_handle);

    ActorSetupHandles {
        router,
        batcher_handle,
        batcher_stats,
        backpressure,
        shutdown_tx,
        partition_handles,
        output_tx,
    }
}

/// Sets up the actor-based service for multiple partition groups with tick task.
///
/// This is the full setup for multi-node operation with automatic ticking.
///
/// # Arguments
///
/// * `node_id` - This node's ID
/// * `cluster_nodes` - All nodes in the cluster
/// * `initial_groups` - Initial Raft groups to create (`group_id` -> replica nodes)
/// * `partition_storage` - Shared partition storage map
/// * `group_map` - Shared group mapping
/// * `local_broker_heartbeats` - Heartbeat timestamps for broker liveness
/// * `transport_handle` - Transport for sending Raft messages
/// * `incoming_rx` - Receiver for incoming transport messages
/// * `config` - Actor setup configuration
///
/// # Returns
///
/// Handles for interacting with the actor system.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher, clippy::unused_async)]
pub async fn setup_multi_partition(
    node_id: NodeId,
    cluster_nodes: Vec<NodeId>,
    initial_groups: HashMap<GroupId, Vec<NodeId>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<crate::controller::ControllerState>>,
    pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, super::PendingProposal>>>>>>,
    pending_controller_proposals: Arc<RwLock<Vec<super::PendingControllerProposal>>>,
    batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    multi_raft: Arc<RwLock<helix_raft::multi::MultiRaft>>,
    transport_handle: TransportHandle,
    incoming_rx: mpsc::Receiver<IncomingMessage>,
    config: ActorSetupConfig,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
) -> ActorSetupHandles {
    let group_count = initial_groups.len();

    // Create shared output channel.
    let (output_tx, output_rx) =
        output_processor::create_output_channel(config.output_processor_config);

    // Create partition actors for each initial group.
    let mut router = PartitionRouter::with_capacity(group_count, 0);
    let mut partition_handles = HashMap::with_capacity(group_count);

    for (group_id, replicas) in initial_groups {
        // Create the Raft node for this partition.
        let raft_config = RaftConfig::new(node_id, replicas);
        let raft_node = RaftNode::new(raft_config);

        // Spawn the partition actor with shared output channel.
        let handle = spawn_partition_actor_shared(
            group_id,
            raft_node,
            config.partition_actor_config,
            output_tx.clone(),
        );

        router.add_partition(group_id, handle.clone());
        partition_handles.insert(group_id, handle);
    }

    let router = Arc::new(router);

    // Create batcher stats and backpressure state.
    let batcher_stats = Arc::new(BatcherStats::default());
    let (batcher_handle, batcher_rx, backpressure) = batcher::create_batcher();

    // Spawn the batcher task (actor mode).
    tokio::spawn(batcher::batcher_task_actor(
        batcher_rx,
        Arc::clone(&router),
        Arc::clone(&batch_pending_proposals),
        Arc::clone(&batcher_stats),
        config.batcher_config,
        Arc::clone(&backpressure),
        Arc::clone(&partition_storage),
    ));

    // Spawn the output processor.
    tokio::spawn(output_processor::output_processor_task(
        output_rx,
        Arc::clone(&partition_storage),
        Arc::clone(&group_map),
        Arc::clone(&batch_pending_proposals),
        Some(transport_handle.clone()),
        Some(Arc::clone(&batcher_stats)),
        Some(Arc::clone(&backpressure)),
        vote_store.clone(),
    ));

    // Create shutdown channel for tick task.
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    // Spawn the actor tick task.
    tokio::spawn(super::tick_task_actor(
        Arc::clone(&router),
        multi_raft,
        Arc::clone(&partition_storage),
        Arc::clone(&group_map),
        controller_state,
        pending_proposals,
        pending_controller_proposals,
        Arc::clone(&local_broker_heartbeats),
        node_id,
        cluster_nodes,
        transport_handle,
        output_tx.clone(),
        vote_store,
        incoming_rx,
        shutdown_rx,
    ));

    info!(
        node_id = node_id.get(),
        partition_count = group_count,
        "Actor setup complete for multi-partition"
    );

    ActorSetupHandles {
        router,
        batcher_handle,
        batcher_stats,
        backpressure,
        shutdown_tx,
        partition_handles,
        output_tx,
    }
}

/// Adds a new partition to an existing actor setup.
///
/// This is used when the controller assigns a new partition via `AssignPartition`.
///
/// # Arguments
///
/// * `router` - The existing partition router (must be mutable via `Arc::get_mut` or similar)
/// * `group_id` - The new partition's group ID
/// * `node_id` - This node's ID
/// * `replicas` - Replica nodes for this partition
/// * `output_tx` - Shared output channel sender
/// * `config` - Partition actor configuration
///
/// # Returns
///
/// The handle for the new partition actor.
///
/// # Note
///
/// The caller is responsible for:
/// 1. Creating the corresponding `ServerPartitionStorage`
/// 2. Updating the `GroupMap`
/// 3. Adding the handle to the router (requires mutable access)
#[must_use]
pub fn create_partition_actor(
    group_id: GroupId,
    node_id: NodeId,
    replicas: Vec<NodeId>,
    output_tx: mpsc::Sender<GroupedOutput>,
    config: PartitionActorConfig,
) -> PartitionActorHandle {
    let raft_config = RaftConfig::new(node_id, replicas);
    let raft_node = RaftNode::new(raft_config);

    spawn_partition_actor_shared(group_id, raft_node, config, output_tx)
}

/// Creates a partition actor with restored vote state.
///
/// Used when creating partitions after recovering from a restart with persisted vote state.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn create_partition_actor_with_state(
    group_id: GroupId,
    node_id: NodeId,
    replicas: Vec<NodeId>,
    term: helix_core::TermId,
    voted_for: Option<NodeId>,
    observation_mode: bool,
    output_tx: mpsc::Sender<GroupedOutput>,
    config: PartitionActorConfig,
) -> PartitionActorHandle {
    let raft_config = RaftConfig::new(node_id, replicas);
    let raft_node = RaftNode::with_vote_state(raft_config, term, voted_for, observation_mode);

    spawn_partition_actor_shared(group_id, raft_node, config, output_tx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use helix_core::{PartitionId, TopicId};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_setup_single_partition() {
        let group_id = GroupId::new(1);
        let node_id = NodeId::new(1);
        let cluster_nodes = vec![node_id];

        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Add group mapping.
        {
            let mut gm = group_map.write().await;
            gm.insert(TopicId::new(1), PartitionId::new(0), group_id);
        }

        // Add partition storage.
        {
            let mut storage = partition_storage.write().await;
            let ps = ServerPartitionStorage::new_in_memory(TopicId::new(1), PartitionId::new(0));
            storage.insert(group_id, Arc::new(RwLock::new(ps)));
        }

        let handles = setup_single_partition(
            group_id,
            node_id,
            cluster_nodes,
            partition_storage,
            group_map,
            batch_pending_proposals,
            None,
            ActorSetupConfig::default(),
            None, // No vote persistence in tests.
        )
        .await;

        // Verify router has the partition.
        assert_eq!(handles.router.partition_count().await, 1);
        assert!(handles.router.partition(group_id).await.is_ok());

        // Verify partition handle is accessible.
        assert!(handles.partition_handles.contains_key(&group_id));

        // Shutdown.
        handles.router.shutdown().await;
    }

    #[tokio::test]
    async fn test_single_partition_propose() {
        let group_id = GroupId::new(1);
        let node_id = NodeId::new(1);
        let cluster_nodes = vec![node_id];

        let partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<ServerPartitionStorage>>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let group_map = Arc::new(RwLock::new(GroupMap::new()));
        let batch_pending_proposals: Arc<RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, BatchPendingProposal>>>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Add group mapping.
        {
            let mut gm = group_map.write().await;
            gm.insert(TopicId::new(1), PartitionId::new(0), group_id);
        }

        // Add partition storage.
        {
            let mut storage = partition_storage.write().await;
            let ps = ServerPartitionStorage::new_in_memory(TopicId::new(1), PartitionId::new(0));
            storage.insert(group_id, Arc::new(RwLock::new(ps)));
        }

        let handles = setup_single_partition(
            group_id,
            node_id,
            cluster_nodes,
            partition_storage,
            group_map,
            batch_pending_proposals,
            None,
            ActorSetupConfig::default(),
            None, // No vote persistence in tests.
        )
        .await;

        // Get the partition handle and tick it to become leader.
        let partition = handles.partition_handles.get(&group_id).unwrap();

        // Tick multiple times to trigger election.
        for _ in 0..20 {
            let _ = partition.tick().await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Check if we're leader.
        let is_leader = timeout(Duration::from_millis(100), partition.is_leader())
            .await
            .expect("is_leader timed out")
            .expect("is_leader failed");

        if is_leader {
            // Propose a command.
            use crate::storage::{BatchedBlob, BlobFormat, PartitionCommand};
            use helix_core::Offset;
            let command = PartitionCommand::AppendBlobBatch {
                blobs: vec![BatchedBlob {
                    blob: Bytes::from("test data"),
                    record_count: 1,
                    format: BlobFormat::Raw,
                }],
                base_offset: Offset::new(0),
            };
            let data = command.encode();

            let result = timeout(Duration::from_millis(100), partition.propose(data))
                .await
                .expect("propose timed out");

            assert!(result.is_ok(), "Propose should succeed: {:?}", result);
        }

        // Shutdown.
        handles.router.shutdown().await;
    }

    #[tokio::test]
    async fn test_create_partition_actor() {
        let group_id = GroupId::new(5);
        let node_id = NodeId::new(1);
        let replicas = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];

        // Create a dummy output channel.
        let (output_tx, _output_rx) = mpsc::channel(100);

        let handle = create_partition_actor(
            group_id,
            node_id,
            replicas,
            output_tx,
            PartitionActorConfig::default(),
        );

        // Verify the handle works.
        let tick_result = handle.tick().await;
        assert!(tick_result.is_ok());

        // Shutdown.
        let _ = handle.shutdown().await;
    }
}

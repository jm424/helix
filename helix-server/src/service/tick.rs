//! Background tick task for Raft consensus.
//!
//! This module handles periodic ticks and message processing for Raft groups.

// Allow complex nested types for proposal maps - refactoring would require significant API changes.
#![allow(clippy::type_complexity)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use helix_core::{GroupId, LogIndex, NodeId, Offset, PartitionId, TermId};
use helix_raft::multi::{MultiRaft, MultiRaftOutput};
use helix_raft::{LogEntry, RaftState};
use helix_runtime::{BrokerHeartbeat, IncomingMessage, TransportService};
use helix_wal::{SharedWalPool, Storage};
use crate::storage::PartitionRecoveryState;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

use crate::vote_store::{LocalFileVoteStorage, VoteStore};

use crate::controller::{ControllerCommand, ControllerState, CONTROLLER_GROUP_ID};
use crate::offset_group::{
    is_offset_group, offset_group_slot, OffsetGroupCommand, OffsetGroupState,
    OFFSET_GROUP_COUNT_USIZE, OFFSET_GROUP_ID_BASE,
};
use crate::snapshot::{
    decode_envelope, encode_envelope, SnapshotConfig, SnapshotCoordinator, SnapshotStore,
    Snapshottable,
};

/// Last Raft index persisted for the controller partition (group 0) in the `SharedWAL`.
///
/// Set during WAL recovery in `new_multi_node_internal`; checked before each append in
/// `process_controller_outputs` to avoid re-persisting entries that were already recovered.
/// Without this guard, a follower that replays entries from the leader after restart can hit
/// the `SharedWAL`'s term-monotonicity assertion (term from an earlier Raft epoch < the term
/// of the last recovered entry).
pub static CONTROLLER_LAST_PERSISTED_INDEX: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Last Raft index persisted per offset group (slot 0 = group 1, slot 1 = group 2, slot 2 = group 3).
///
/// Mirrors `CONTROLLER_LAST_PERSISTED_INDEX` for the three offset Raft groups. Set during
/// WAL recovery; checked before each `SharedWAL` append to skip already-persisted entries.
pub static OFFSET_GROUP_LAST_PERSISTED: [std::sync::atomic::AtomicU64; 3] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

use crate::error::ServerError;
use crate::group_map::GroupMap;
use crate::partition_storage::PartitionStorage;
use super::{
    output_processor::extract_and_record_producer_state, PendingControllerProposal,
    PendingOffsetProposal, PendingProposal, TICK_INTERVAL_MS,
};

/// Interval for sending broker heartbeats to the controller (in milliseconds).
///
/// Set to 1 second for fast dead broker detection. Combined with a 5 second
/// timeout, this allows up to 5 missed heartbeats before a broker is fenced.
///
/// Used by both production tick tasks and DST simulation actors.
pub const HEARTBEAT_INTERVAL_MS: u64 = 1_000; // 1 second.

/// Interval for tiering background tasks (in milliseconds).
///
/// Set to 5 seconds as a balance between promptly tiering new segments
/// and avoiding excessive overhead. Tiering operations include:
/// - Registering newly sealed segments with the tiering manager
/// - Marking committed segments as eligible for tiering
/// - Uploading eligible segments to object storage (S3/filesystem)
pub const TIERING_INTERVAL_MS: u64 = 5_000; // 5 seconds.

/// Interval for leader rebalancing (in milliseconds).
///
/// The controller leader periodically checks whether partition leaders match
/// their preferred replicas (`replicas[0]`) and issues `TransferLeadership`
/// for any imbalanced partitions. Set to 10 seconds for fast convergence
/// after topic creation or node restarts.
pub const LEADER_REBALANCE_INTERVAL_MS: u64 = 10_000; // 10 seconds.

/// Maximum leadership transfers per rebalance cycle.
///
/// Rate-limits transfers to avoid disrupting in-flight requests. At 10s
/// intervals with 10 transfers per cycle, 1000 imbalanced partitions
/// converge in ~17 minutes. For typical workloads (128-512 partitions),
/// convergence is 1-2 cycles.
pub const MAX_TRANSFERS_PER_CYCLE: usize = 10;

/// Interval for segment retention checks (in milliseconds).
///
/// Set to 60 seconds — retention is not latency-sensitive. Checks all
/// shared and dedicated WAL segments for age + replication safety, then
/// deletes eligible segments.
pub const RETENTION_INTERVAL_MS: u64 = 60_000; // 60 seconds.

/// Background task to handle Raft ticks for all groups (single-node).
#[allow(clippy::significant_drop_tightening, clippy::implicit_hasher)]
pub async fn tick_task<S: Storage + Clone + Send + Sync + 'static>(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut tiering_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TIERING_INTERVAL_MS));

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                debug!("Tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                let outputs = {
                    let mut mr = multi_raft.write().await;
                    mr.tick()
                };
                process_outputs(
                    &outputs,
                    &partition_storage,
                    &group_map,
                    &pending_proposals,
                ).await;
            }
            _ = tiering_interval.tick() => {
                // Spawn tiering in a background task so S3 uploads never block
                // Raft ticks. Uploads can take seconds per segment; running them
                // inline would freeze the tick loop and trigger election timeouts.
                let ps = Arc::clone(&partition_storage);
                tokio::spawn(async move {
                    process_tiering(&ps, None::<&Arc<SharedWalPool<S>>>, None).await;
                });
            }
        }
    }
}

// Legacy non-actor tick tasks were removed in ADR-0006.
// All multi-node operation now uses tick_task_actor + tick_task_controller.

/// Background tick task for actor-based multi-node operation.
///
/// Routes ticks and messages to partition actors via the `PartitionRouter`,
/// eliminating lock contention. The controller partition is handled separately
/// by `tick_task_controller`.
///
/// # Responsibilities
///
/// 1. **Tick broadcast**: Periodically calls `router.tick_all()` to tick all
///    partition actors in parallel.
/// 2. **Message routing**: Routes incoming Raft messages from transport to
///    the appropriate partition actors via `router.route_messages()`.
/// 3. **Heartbeats**: Sends broker heartbeats to all peers (Kafka `KRaft` pattern).
/// 4. **Tiering**: Processes tiering for durable partitions.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher, clippy::too_many_lines)]
pub async fn tick_task_actor<S: Storage + Clone + Send + Sync + 'static, T: TransportService>(
    router: Arc<super::router::PartitionRouter>,
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: Vec<NodeId>,
    transport_handle: T,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<Arc<SharedWalPool<S>>>,
    data_dir: Option<PathBuf>,
    recovered_entries: Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>,
    storage: S,
    local_retention_ms: Option<u64>,
    offset_group_states: Vec<Arc<RwLock<OffsetGroupState>>>,
    pending_offset_proposals: Arc<RwLock<Vec<PendingOffsetProposal>>>,
    backpressure: Arc<super::batcher::BackpressureState>,
    snapshot_store: Option<Arc<SnapshotStore>>,
    log_trailing_entries: Option<u64>,
    mut incoming_rx: mpsc::Receiver<IncomingMessage>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut heartbeat_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(HEARTBEAT_INTERVAL_MS));
    // When retention is short (DST), run tiering at the same frequency so
    // uploads + local file deletion keep pace with segment rotation.
    let tiering_check_ms = local_retention_ms
        .map_or(TIERING_INTERVAL_MS, |r| r.min(TIERING_INTERVAL_MS));
    let mut tiering_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(tiering_check_ms));
    // Check retention at least as often as the retention threshold itself, so
    // DST (short simulated time) still exercises the deletion path.
    let retention_check_ms = local_retention_ms
        .map_or(RETENTION_INTERVAL_MS, |r| r.min(RETENTION_INTERVAL_MS));
    let mut retention_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(retention_check_ms));

    // Per-partition snapshot coordinators and WAL index tracking.
    // Coordinators start with `eager_first_snapshot` for new groups (no prior
    // snapshot) and switch to `for_data_partition` after the first snapshot.
    let mut data_snap_coords: HashMap<GroupId, SnapshotCoordinator> = HashMap::new();
    let mut data_snap_last_wal: HashMap<GroupId, u64> = HashMap::new();

    // Controller/offset snapshot coordinators for the incoming message path.
    //
    // In a multi-node cluster, most controller/offset CommitEntry outputs
    // arrive via `mr.handle_messages()` (when follower AppendEntries
    // responses achieve quorum), NOT via `mr.tick()`. The tick_task_controller
    // only sees entries from `mr.tick()`, which rarely includes commits.
    // Without coordinators here, the eager first snapshot would never fire.
    //
    // All groups start eager (threshold=1) then switch to their normal
    // config after the first snapshot fires.
    let mut actor_ctl_snap_coord =
        SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot());
    let mut actor_ctl_eager = true;
    let mut actor_og_snap_coords: Vec<SnapshotCoordinator> = (0
        ..crate::offset_group::OFFSET_GROUP_COUNT_USIZE)
        .map(|_| SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot()))
        .collect();
    let mut actor_og_eager: Vec<bool> =
        vec![true; crate::offset_group::OFFSET_GROUP_COUNT_USIZE];

    let initial_partition_count = router.partition_count().await;
    info!(
        node_id = node_id.get(),
        partition_count = initial_partition_count,
        "Actor tick task started"
    );

    // Recovery phase: create partition actors for all pre-existing assignments.
    //
    // After a restart, the controller state is replayed from the SharedWAL,
    // restoring all topics and partition assignments. However, partition actors
    // are NOT created during that replay because the tick task only creates them
    // when it processes NEW AssignPartition commit entries — entries already
    // committed before the restart are never re-emitted.
    //
    // Without this recovery step, every partition group this node is a replica
    // of starts with no actor registered. The actor router returns None for
    // those groups, Kafka metadata returns LEADER_NOT_AVAILABLE silently, and
    // clients can never make progress on those partitions.
    recover_partition_actors(
        node_id,
        &router,
        &controller_state,
        &partition_storage,
        &group_map,
        vote_store.as_ref(),
        shared_wal_pool.as_ref(),
        data_dir.as_ref(),
        &recovered_entries,
        &storage,
        &output_tx,
        &backpressure,
        log_trailing_entries,
    )
    .await;

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Actor tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                // Broadcast tick to all partition actors in parallel.
                // Each actor processes its tick independently without lock contention.
                router.tick_all().await;
            }
            _ = heartbeat_interval.tick() => {
                // Send broker heartbeats to all peers (Kafka KRaft pattern).
                // Heartbeats are soft state that doesn't require Raft consensus.
                send_broker_heartbeats_to_peers(
                    &local_broker_heartbeats,
                    node_id,
                    &cluster_nodes,
                    &transport_handle,
                ).await;
            }
            _ = tiering_interval.tick() => {
                // Spawn tiering in a background task so S3 uploads never block
                // Raft ticks, peer message processing, or heartbeats. Segment
                // uploads can take seconds and must not hold the tick loop.
                let ps = Arc::clone(&partition_storage);
                let pool = shared_wal_pool.clone();
                tokio::spawn(async move {
                    process_tiering(&ps, pool.as_ref(), local_retention_ms).await;
                });

                // Trigger data partition snapshots when coordinators signal.
                if let Some(ref store) = snapshot_store {
                    let leader_map = router.leader_cached_snapshot().await;
                    process_data_partition_snapshots(
                        &partition_storage,
                        &leader_map,
                        &mut data_snap_coords,
                        &mut data_snap_last_wal,
                        store,
                        shared_wal_pool.clone(),
                    )
                    .await;
                }
            }
            _ = retention_interval.tick() => {
                // Delete old WAL segments that have been replicated.
                if let Some(retention_ms) = local_retention_ms {
                    process_retention(
                        &partition_storage,
                        &router,
                        shared_wal_pool.as_ref(),
                        retention_ms,
                    ).await;
                }
            }
            Some(incoming) = incoming_rx.recv() => {
                // Route incoming messages to partition actors or MultiRaft.
                match incoming {
                    IncomingMessage::Single(_message) => {
                        warn!("Received single message in actor mode, expected batch");
                    }
                    IncomingMessage::Batch(group_messages) => {
                        // Split messages: controller + offset group messages go to MultiRaft,
                        // data partition messages go to partition actors.
                        let (controller_msgs, data_msgs): (Vec<_>, Vec<_>) = group_messages
                            .into_iter()
                            .partition(|gm| {
                                gm.group_id == CONTROLLER_GROUP_ID
                                    || is_offset_group(gm.group_id)
                            });

                        // Step controller/offset messages through MultiRaft and process outputs.
                        // This includes BecameLeader, CommitEntry, etc. - not just SendMessages.
                        if !controller_msgs.is_empty() {
                            trace!(
                                count = controller_msgs.len(),
                                "Received controller/offset messages in tick_task_actor"
                            );
                            let outputs = {
                                let mut mr = multi_raft.write().await;
                                mr.handle_messages(controller_msgs)
                            };
                            trace!(
                                output_count = outputs.len(),
                                "Processed controller/offset messages, got outputs"
                            );

                            // Process all controller and offset group outputs.
                            let (ctl_entries, og_entries) = process_controller_outputs(
                                &outputs,
                                &multi_raft,
                                &partition_storage,
                                &group_map,
                                &controller_state,
                                &pending_proposals,
                                &pending_controller_proposals,
                                &cluster_nodes,
                                &transport_handle,
                                &router,
                                &output_tx,
                                vote_store.as_ref(),
                                shared_wal_pool.as_ref(),
                                data_dir.as_ref(),
                                Some(&recovered_entries),
                                Some(&storage),
                                &offset_group_states,
                                &pending_offset_proposals,
                                &backpressure,
                                log_trailing_entries,
                            ).await;

                            // Feed committed entry counts into controller/offset
                            // snapshot coordinators and trigger snapshots when due.
                            for _ in 0..ctl_entries {
                                actor_ctl_snap_coord.record_entry();
                            }
                            maybe_snapshot(
                                &controller_state,
                                &mut actor_ctl_snap_coord,
                                snapshot_store.as_ref(),
                                CONTROLLER_GROUP_ID,
                                &multi_raft,
                                shared_wal_pool.clone(),
                            ).await;
                            // Switch from eager to normal after the first
                            // snapshot. Detect "snapshot fired this
                            // iteration" by: we recorded entries
                            // (ctl_entries > 0) but entries_since_snapshot
                            // is back to 0 (mark_completed was called).
                            if actor_ctl_eager
                                && ctl_entries > 0
                                && actor_ctl_snap_coord.entries_since_snapshot()
                                    == 0
                            {
                                actor_ctl_snap_coord = SnapshotCoordinator::new(
                                    SnapshotConfig::for_controller(),
                                );
                                actor_ctl_eager = false;
                            }

                            for (slot, count) in og_entries.iter().enumerate() {
                                for _ in 0..*count {
                                    actor_og_snap_coords[slot].record_entry();
                                }
                                let og_had =
                                    actor_og_snap_coords[slot].entries_since_snapshot() > 0;
                                #[allow(clippy::cast_possible_truncation)]
                                let group_id = GroupId::new(
                                    OFFSET_GROUP_ID_BASE.get() + slot as u64,
                                );
                                maybe_snapshot(
                                    &offset_group_states[slot],
                                    &mut actor_og_snap_coords[slot],
                                    snapshot_store.as_ref(),
                                    group_id,
                                    &multi_raft,
                                    shared_wal_pool.clone(),
                                ).await;
                                if actor_og_eager[slot]
                                    && og_had
                                    && actor_og_snap_coords[slot].entries_since_snapshot() == 0
                                {
                                    actor_og_snap_coords[slot] =
                                        SnapshotCoordinator::new(
                                            SnapshotConfig::for_offset_group(),
                                        );
                                    actor_og_eager[slot] = false;
                                }
                            }
                        }

                        // Route data partition messages to partition actors.
                        if !data_msgs.is_empty() {
                            let from = data_msgs
                                .first()
                                .map_or(NodeId::new(0), |m| m.message.from());
                            router.route_messages(data_msgs, from).await;
                        }
                    }
                    IncomingMessage::Heartbeat(heartbeat) => {
                        // Update local heartbeat soft state.
                        local_broker_heartbeats
                            .write()
                            .await
                            .insert(heartbeat.node_id, heartbeat.timestamp_ms);
                        debug!(
                            from = heartbeat.node_id.get(),
                            timestamp_ms = heartbeat.timestamp_ms,
                            "Recorded heartbeat from peer (actor mode)"
                        );
                    }
                }
            }
        }
    }

    info!("Actor tick task stopped");
}

/// Background tick task for controller partition only (actor mode).
///
/// When actor mode is enabled for data partitions, the controller partition
/// still needs to be ticked via `MultiRaft`. This task handles:
///
/// 1. **Controller ticking**: Periodically ticks the controller Raft group.
/// 2. **Controller outputs**: Processes commits, leader changes, and message sending.
/// 3. **Follow-up commands**: Proposes follow-up controller commands.
/// 4. **Dynamic partition creation**: Creates partition actors on `AssignPartition`.
///
/// Data partition operations are handled by `tick_task_actor` via the router.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher, clippy::too_many_lines)]
pub async fn tick_task_controller<
    S: Storage + Clone + Send + Sync + 'static,
    T: TransportService,
>(
    multi_raft: Arc<RwLock<MultiRaft>>,
    partition_storage: Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: Arc<RwLock<GroupMap>>,
    controller_state: Arc<RwLock<ControllerState>>,
    pending_proposals: Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: Vec<NodeId>,
    transport_handle: T,
    router: Arc<super::router::PartitionRouter>,
    output_tx: mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<Arc<SharedWalPool<S>>>,
    data_dir: Option<PathBuf>,
    recovered_entries: Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>,
    storage: S,
    local_broker_heartbeats: Arc<RwLock<HashMap<NodeId, u64>>>,
    offset_group_states: Vec<Arc<RwLock<OffsetGroupState>>>,
    pending_offset_proposals: Arc<RwLock<Vec<PendingOffsetProposal>>>,
    backpressure: Arc<super::batcher::BackpressureState>,
    snapshot_store: Option<Arc<crate::snapshot::SnapshotStore>>,
    log_trailing_entries: Option<u64>,
    mut leader_update_rx: mpsc::UnboundedReceiver<crate::controller::ControllerCommand>,
    mut shutdown_rx: mpsc::Receiver<()>,
) {
    let mut tick_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(TICK_INTERVAL_MS));
    let mut rebalance_interval =
        tokio::time::interval(tokio::time::Duration::from_millis(LEADER_REBALANCE_INTERVAL_MS));

    let node_id = {
        let mr = multi_raft.read().await;
        mr.node_id()
    };

    // Snapshot coordinators track when a snapshot is due for each Raft group.
    // The coordinator fires when both the entry count AND the time interval
    // thresholds defined in SnapshotConfig are met.
    //
    // All groups start with eager first snapshot (1 entry, 0s interval) so
    // the WAL eviction floor is advanced quickly. After the first snapshot
    // fires, each switches to its normal config. The controller snapshot
    // is an optimization (skip WAL entries during replay), not a
    // correctness requirement — data partition snapshot loading happens
    // after full controller WAL replay.
    let mut controller_snap_coord =
        SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot());
    let mut controller_eager = true;
    let mut offset_snap_coords: Vec<SnapshotCoordinator> = (0
        ..crate::offset_group::OFFSET_GROUP_COUNT_USIZE)
        .map(|_| SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot()))
        .collect();
    let mut offset_eager: Vec<bool> =
        vec![true; crate::offset_group::OFFSET_GROUP_COUNT_USIZE];

    info!(
        node_id = node_id.get(),
        "Controller tick task started"
    );

    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Controller tick task shutting down");
                break;
            }
            _ = tick_interval.tick() => {
                // Tick only the controller partition via MultiRaft.
                let outputs = {
                    let mut mr = multi_raft.write().await;
                    mr.tick()
                };

                // Process controller and offset group outputs.
                let (ctl_entries, og_entries) = process_controller_outputs(
                    &outputs,
                    &multi_raft,
                    &partition_storage,
                    &group_map,
                    &controller_state,
                    &pending_proposals,
                    &pending_controller_proposals,
                    &cluster_nodes,
                    &transport_handle,
                    &router,
                    &output_tx,
                    vote_store.as_ref(),
                    shared_wal_pool.as_ref(),
                    data_dir.as_ref(),
                    Some(&recovered_entries),
                    Some(&storage),
                    &offset_group_states,
                    &pending_offset_proposals,
                    &backpressure,
                    log_trailing_entries,
                ).await;

                // Feed applied entry counts into snapshot coordinators.
                for _ in 0..ctl_entries {
                    controller_snap_coord.record_entry();
                }
                for (slot, count) in og_entries.iter().enumerate() {
                    for _ in 0..*count {
                        offset_snap_coords[slot].record_entry();
                    }
                }

                // Snapshot trigger: controller (leader only).
                maybe_snapshot(
                    &controller_state,
                    &mut controller_snap_coord,
                    snapshot_store.as_ref(),
                    CONTROLLER_GROUP_ID,
                    &multi_raft,
                    shared_wal_pool.clone(),
                ).await;
                // Switch from eager to normal after the first snapshot.
                if controller_eager
                    && ctl_entries > 0
                    && controller_snap_coord.entries_since_snapshot() == 0
                {
                    controller_snap_coord = SnapshotCoordinator::new(
                        SnapshotConfig::for_controller(),
                    );
                    controller_eager = false;
                }

                // Snapshot trigger: offset groups (leader only per group).
                for slot in 0..OFFSET_GROUP_COUNT_USIZE {
                    #[allow(clippy::cast_possible_truncation)]
                    let group_id = GroupId::new(OFFSET_GROUP_ID_BASE.get() + slot as u64);
                    let og_had_entries =
                        offset_snap_coords[slot].entries_since_snapshot() > 0;
                    maybe_snapshot(
                        &offset_group_states[slot],
                        &mut offset_snap_coords[slot],
                        snapshot_store.as_ref(),
                        group_id,
                        &multi_raft,
                        shared_wal_pool.clone(),
                    ).await;
                    // Switch from eager to normal after the first snapshot.
                    if offset_eager[slot]
                        && og_had_entries
                        && offset_snap_coords[slot].entries_since_snapshot() == 0
                    {
                        offset_snap_coords[slot] =
                            SnapshotCoordinator::new(SnapshotConfig::for_offset_group());
                        offset_eager[slot] = false;
                    }
                }
            }
            _ = rebalance_interval.tick() => {
                // Leader rebalancing: move partition leaders to their preferred
                // replicas (replicas[0]). Decentralized — all nodes run this,
                // each acting only on partitions where it is the actual Raft leader.
                rebalance_partition_leaders(
                    node_id,
                    &controller_state,
                    &router,
                    &local_broker_heartbeats,
                ).await;
            }
            Some(cmd) = leader_update_rx.recv() => {
                // A data partition's leader changed. Propose UpdatePartitionLeader
                // to the controller so the rebalancer can see the current leader
                // and stop issuing redundant TransferLeadership commands.
                //
                // Only the controller leader proposes; followers see the committed
                // entry via normal Raft replication.
                let is_leader = {
                    let mr = multi_raft.read().await;
                    mr.group_state(CONTROLLER_GROUP_ID)
                        .is_some_and(|s| s.state == RaftState::Leader)
                };
                if is_leader {
                    let encoded = cmd.encode();
                    let mut mr = multi_raft.write().await;
                    if mr.propose(CONTROLLER_GROUP_ID, encoded).is_none() {
                        warn!("Failed to propose UpdatePartitionLeader to controller");
                    }
                }
            }
        }
    }

    info!("Controller tick task stopped");
}

/// Takes a snapshot for a Raft group if the coordinator signals one is due.
///
/// Only the Raft leader for `group_id` saves snapshots to avoid duplicate
/// writes. State is serialized under the read lock (fast, no I/O), then
/// persisted asynchronously so the tick loop is never blocked by disk or S3.
///
/// `coordinator.mark_completed()` is called synchronously after spawning the
/// save task. The coordinator is reset regardless of whether the save
/// ultimately succeeds — a failed save will cause the next snapshot to be
/// taken after the next threshold crossing rather than retrying immediately.
async fn maybe_snapshot<SM, S>(
    state_lock: &Arc<RwLock<SM>>,
    coordinator: &mut SnapshotCoordinator,
    store_opt: Option<&Arc<SnapshotStore>>,
    group_id: GroupId,
    multi_raft: &Arc<RwLock<MultiRaft>>,
    wal_pool: Option<Arc<SharedWalPool<S>>>,
)
where
    SM: Snapshottable + Send + Sync + 'static,
    S: helix_wal::Storage + Clone + Send + Sync + 'static,
{
    let Some(store) = store_opt else { return };

    if !coordinator.should_snapshot() {
        return;
    }

    // Only the Raft leader for this group saves snapshots.
    let is_leader = {
        let mr = multi_raft.read().await;
        mr.group_state(group_id)
            .is_some_and(|s| s.state == RaftState::Leader)
    };
    if !is_leader {
        return;
    }
    eprintln!(
        "[DST-PROBE] MAYBE_SNAPSHOT: group={} entries_since={}",
        group_id.get(),
        coordinator.entries_since_snapshot(),
    );

    // Serialize state under the read lock — fast, no I/O.
    let snapshot_result = {
        let state = state_lock.read().await;
        state.take_snapshot()
    };
    let (meta, body) = match snapshot_result {
        Ok(pair) => pair,
        Err(e) => {
            warn!(group = group_id.get(), error = %e, "Failed to serialize snapshot");
            return;
        }
    };

    // Save asynchronously so S3 upload does not block the tick loop.
    // After a successful save, advance the WAL eviction floor and update the
    // state machine's last_snapshot_wal_index so min_required_wal_index()
    // returns the correct value on the leader.
    let store = Arc::clone(store);
    let state_for_update = Arc::clone(state_lock);
    let wal_floor = meta.last_included_wal_index;
    eprintln!(
        "[DST-PROBE] SNAPSHOT_SAVE_SPAWNING: group={} wal_floor={}",
        group_id.get(),
        wal_floor,
    );
    tokio::spawn(async move {
        match store.save(&meta, &body).await {
            Ok(()) => {
                eprintln!(
                    "[DST-PROBE] SNAPSHOT_SAVED: group={} wal_floor={} index={}",
                    group_id.get(),
                    wal_floor,
                    meta.last_included_index,
                );
                info!(
                    group = group_id.get(),
                    last_index = meta.last_included_index,
                    wal_floor,
                    "Saved Raft group snapshot"
                );
                if let Some(pool) = wal_pool {
                    pool.set_snapshot_floor(group_id, wal_floor);
                }
                state_for_update.write().await.set_last_snapshot_wal_index(wal_floor);
            }
            Err(e) => {
                eprintln!(
                    "[DST-PROBE] SNAPSHOT_SAVE_FAILED: group={} error={}",
                    group_id.get(),
                    e,
                );
                warn!(group = group_id.get(), error = %e, "Failed to save snapshot");
            }
        }
    });

    coordinator.mark_completed();
}

/// Checks each data partition for snapshot eligibility and saves when due.
///
/// Iterates the `partition_storage` map, tracks committed entry counts via
/// WAL-index deltas, and feeds them into per-partition `SnapshotCoordinator`
/// instances. When a coordinator signals a snapshot is due and this node is
/// the leader, the snapshot is serialized under the read lock and saved
/// asynchronously.
///
/// New groups (no prior snapshot, `min_required_wal_index == None`) use the
/// `eager_first_snapshot` config so the WAL eviction floor is advanced after
/// just one applied entry. After the first snapshot fires the coordinator
/// switches to the normal `for_data_partition` config.
async fn process_data_partition_snapshots<S>(
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<crate::partition_storage::PartitionStorage<S>>>>>>,
    leader_map: &HashMap<GroupId, bool>,
    snap_coords: &mut HashMap<GroupId, SnapshotCoordinator>,
    snap_last_wal: &mut HashMap<GroupId, u64>,
    store: &Arc<SnapshotStore>,
    wal_pool: Option<Arc<SharedWalPool<S>>>,
)
where
    S: helix_wal::Storage + Clone + Send + Sync + 'static,
{
    let storage_guard = partition_storage.read().await;
    for (&group_id, storage_lock) in storage_guard.iter() {
        let ps = storage_lock.read().await;
        // Determine if this is the first snapshot for this group.
        let no_snapshot_yet = ps.min_required_wal_index().is_none();
        // Get-or-create coordinator: eager for new groups, normal otherwise.
        let coord = snap_coords.entry(group_id).or_insert_with(|| {
            if no_snapshot_yet {
                SnapshotCoordinator::new(SnapshotConfig::eager_first_snapshot())
            } else {
                SnapshotCoordinator::new(SnapshotConfig::for_data_partition())
            }
        });
        // Advance coordinator by delta WAL entries since last check.
        let current_wal = ps.last_applied_wal_index();
        let last_seen = snap_last_wal.entry(group_id).or_insert(current_wal);
        let delta = current_wal.saturating_sub(*last_seen);
        for _ in 0..delta {
            coord.record_entry();
        }
        *last_seen = current_wal;
        // Skip if coordinator says not yet or this node is not the leader.
        if !coord.should_snapshot() {
            continue;
        }
        if !leader_map.get(&group_id).copied().unwrap_or(false) {
            continue;
        }
        // Serialize under read lock (fast, no I/O).
        let snapshot_result = ps.take_snapshot();
        drop(ps);
        let (meta, body) = match snapshot_result {
            Ok(pair) => pair,
            Err(e) => {
                warn!(group = group_id.get(), error = %e, "Failed to serialize data partition snapshot");
                coord.mark_completed();
                continue;
            }
        };
        // Reset coordinator; switch new groups to normal frequency.
        coord.mark_completed();
        if no_snapshot_yet {
            *coord = SnapshotCoordinator::new(SnapshotConfig::for_data_partition());
        }
        // Save asynchronously so disk/S3 I/O does not block the tick loop.
        let store = Arc::clone(store);
        let pool = wal_pool.clone();
        let wal_floor = meta.last_included_wal_index;
        let ps_lock = Arc::clone(storage_lock);
        eprintln!(
            "[DST-PROBE] DATA_SNAPSHOT_SPAWNING: group={} wal_floor={}",
            group_id.get(),
            wal_floor,
        );
        tokio::spawn(async move {
            match store.save(&meta, &body).await {
                Ok(()) => {
                    eprintln!(
                        "[DST-PROBE] DATA_SNAPSHOT_SAVED: group={} wal_floor={}",
                        group_id.get(),
                        wal_floor,
                    );
                    info!(
                        group = group_id.get(),
                        last_index = meta.last_included_index,
                        wal_floor,
                        "Saved data partition snapshot"
                    );
                    if let Some(pool) = pool {
                        pool.set_snapshot_floor(group_id, wal_floor);
                    }
                    ps_lock.write().await.set_last_snapshot_wal_index(wal_floor);
                }
                Err(e) => {
                    warn!(group = group_id.get(), error = %e, "Failed to save data partition snapshot");
                }
            }
        });
    }
}

/// Rebalances partition leaders to their preferred replicas (decentralized).
///
/// Each node runs this independently. For each data partition where this node
/// is the **actual Raft leader** but not the preferred replica (`replicas[0]`),
/// it initiates a `TransferLeadership` to the preferred node. Rate-limited to
/// `MAX_TRANSFERS_PER_CYCLE` per invocation.
///
/// This decentralized approach avoids the two bugs of the old controller-leader-only
/// design: (1) stale `assignment.leader` cache on controller followers, and (2)
/// `router.partition()` returning only the local actor, making transfers a no-op
/// when the actual leader is on another node. Since Raft guarantees at most one
/// leader per term, only the actual leader initiates the transfer — no double-transfer
/// risk.
///
/// Nodes are muted after a failed leadership transfer for one rebalance interval
/// to avoid rapid retries. Same pattern as Redpanda's `leadership_transfer_backoff`.
static REBALANCE_MUTED: std::sync::LazyLock<
    tokio::sync::Mutex<HashMap<NodeId, u64>>,
> = std::sync::LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

async fn rebalance_partition_leaders(
    node_id: NodeId,
    controller_state: &Arc<RwLock<ControllerState>>,
    router: &Arc<super::router::PartitionRouter>,
    local_broker_heartbeats: &Arc<RwLock<HashMap<NodeId, u64>>>,
) {
    // Get current time for broker liveness checks.
    #[allow(clippy::cast_possible_truncation)]
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    // Check broker liveness from BOTH sources:
    // 1. local_broker_heartbeats: soft-state heartbeats exchanged via
    //    transport in production mode.
    // 2. controller_state.broker_heartbeats: Raft-replicated heartbeats
    //    used in DST (deterministic simulation testing).
    //
    // A broker is considered alive if EITHER source has a recent
    // heartbeat. This ensures the same rebalance logic works in both
    // production and DST.
    let local_live: std::collections::HashSet<NodeId> = {
        let hb = local_broker_heartbeats.read().await;
        hb.iter()
            .filter(|(_, &ts)| now_ms.saturating_sub(ts) < crate::controller::BROKER_HEARTBEAT_TIMEOUT_MS)
            .map(|(&id, _)| id)
            .collect()
    };

    // Expire old mutes and collect the currently muted set.
    let muted_nodes: std::collections::HashSet<NodeId> = {
        let mut muted = REBALANCE_MUTED.lock().await;
        muted.retain(|_, &mut until| until > now_ms);
        muted.keys().copied().collect()
    };

    // Phase 1 (sync, lock held briefly): collect candidates where this node is NOT
    // the preferred replica, the preferred is alive, and the preferred is not muted.
    // We rely on live Raft state (Phase 2) rather than stale controller-cached leader
    // to decide whether a transfer is needed.
    let candidates: Vec<(GroupId, NodeId)> = {
        let state = controller_state.read().await;
        state
            .all_assignments()
            .filter_map(|(_, assignment)| {
                let preferred = *assignment.replicas.first()?;
                // Skip if we are already the preferred replica — no transfer needed.
                if preferred == node_id {
                    return None;
                }
                // Skip if preferred leader is not alive (check both sources).
                let alive = local_live.contains(&preferred)
                    || state.is_broker_live(preferred, now_ms);
                if !alive {
                    return None;
                }
                // Skip if the target node is muted from a recent failed transfer attempt.
                if muted_nodes.contains(&preferred) {
                    return None;
                }
                // Skip the controller partition (group 0).
                if assignment.group_id == CONTROLLER_GROUP_ID {
                    return None;
                }
                Some((assignment.group_id, preferred))
            })
            .collect()
    };

    if candidates.is_empty() {
        return;
    }

    // Phase 2 (async, no lock held): keep only partitions where this node is the
    // actual Raft leader. Only the actual leader can initiate a transfer — this
    // eliminates the stale-cache problem where controller-side assignment.leader
    // is only updated on the controller leader and lags for ~2/3 of leader changes.
    let mut imbalanced: Vec<(GroupId, NodeId)> = Vec::new();
    for (group_id, preferred) in candidates {
        if imbalanced.len() >= MAX_TRANSFERS_PER_CYCLE {
            break;
        }
        if let Ok(handle) = router.partition(group_id).await {
            if matches!(handle.leader_id().await, Ok(Some(actual_leader)) if actual_leader == node_id) {
                // This node is the actual Raft leader and not the preferred
                // replica — initiate a transfer.
                imbalanced.push((group_id, preferred));
            }
            // Otherwise: not the leader (someone else is, or no leader yet).
            // That node's rebalancer will handle it.
        }
        // else: actor not yet started — skip for this cycle.
    }

    if imbalanced.is_empty() {
        return;
    }

    info!(
        count = imbalanced.len(),
        "Rebalancing partition leaders"
    );

    for (group_id, preferred) in &imbalanced {
        match router.partition(*group_id).await {
            Ok(handle) => {
                if let Err(e) = handle.transfer_leadership(*preferred).await {
                    warn!(
                        group = group_id.get(),
                        target = preferred.get(),
                        error = %e,
                        "Failed to send transfer leadership to partition actor"
                    );
                } else {
                    debug!(
                        group = group_id.get(),
                        target = preferred.get(),
                        "Initiated leadership transfer to preferred replica"
                    );
                    // Mute this target for one rebalance interval to avoid
                    // retrying if the transfer fails (target unreachable).
                    // The mute is cleared automatically when it expires.
                    let mute_until = now_ms + LEADER_REBALANCE_INTERVAL_MS;
                    REBALANCE_MUTED.lock().await.insert(*preferred, mute_until);
                }
            }
            Err(_) => {
                debug!(
                    group = group_id.get(),
                    "Partition actor not found for rebalance (may still be starting)"
                );
            }
        }
    }
}

/// Processes outputs for controller partition only (actor mode).
///
/// This is a simplified version of `process_outputs_multi_node` that only
/// handles controller partition (group 0) outputs. Data partition outputs
/// are handled by the `OutputProcessor`.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::significant_drop_tightening,
    clippy::implicit_hasher
)]
pub async fn process_controller_outputs<
    S: Storage + Clone + Send + Sync + 'static,
    T: TransportService,
>(
    outputs: &[MultiRaftOutput],
    multi_raft: &Arc<RwLock<MultiRaft>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    controller_state: &Arc<RwLock<ControllerState>>,
    _pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
    pending_controller_proposals: &Arc<RwLock<Vec<PendingControllerProposal>>>,
    cluster_nodes: &[NodeId],
    transport_handle: &T,
    router: &Arc<super::router::PartitionRouter>,
    output_tx: &mpsc::Sender<super::partition_actor::GroupedOutput>,
    vote_store: Option<&Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    data_dir: Option<&PathBuf>,
    recovered_entries: Option<&Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>>,
    storage: Option<&S>,
    offset_group_states: &[Arc<RwLock<OffsetGroupState>>],
    pending_offset_proposals: &Arc<RwLock<Vec<PendingOffsetProposal>>>,
    backpressure: &Arc<super::batcher::BackpressureState>,
    log_trailing_entries: Option<u64>,
) -> (u64, Vec<u64>) {
    // Entry counts returned to the caller so snapshot coordinators can be
    // updated without threading mutable coordinator refs through this function.
    let mut controller_entries_applied: u64 = 0;
    let mut offset_entries_applied: Vec<u64> =
        vec![0; crate::offset_group::OFFSET_GROUP_COUNT_USIZE];

    // Collect follow-up outputs for single-node processing.
    // For single-node clusters, propose() returns CommitEntry immediately,
    // so we need to process those after the main loop.
    let mut all_follow_up_outputs: Vec<MultiRaftOutput> = Vec::new();

    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                term,
                metadata,
                ..
            } => {
                if let Some(slot) = offset_group_slot(*group_id) {
                    // Offset group commit: decode, apply, persist to WAL, notify waiters.
                    // Empty payload = Raft no-op entry (e.g. leader election); skip silently.
                    if metadata.is_empty() {
                        continue;
                    }
                    let Some(cmd) = OffsetGroupCommand::decode(metadata) else {
                        warn!(
                            index = index.get(),
                            group_id = group_id.get(),
                            "Failed to decode offset group command"
                        );
                        continue;
                    };

                    let mut og_state = offset_group_states[slot].write().await;
                    og_state.apply(&cmd, index.get(), term.get());
                    offset_entries_applied[slot] = offset_entries_applied[slot].saturating_add(1);

                    // Persist to SharedWAL (skip already-recovered entries).
                    let last_persisted = OFFSET_GROUP_LAST_PERSISTED[slot]
                        .load(std::sync::atomic::Ordering::Relaxed);
                    if index.get() > last_persisted {
                        if let Some(pool) = shared_wal_pool {
                            // Lazy registration: register on first WAL append
                            // so groups with no entries don't pin eviction floor.
                            pool.register_group(*group_id);
                            let handle = pool.handle(*group_id);
                            match handle
                                .append_nowait(term.get(), index.get(), metadata.clone())
                                .await
                            {
                                Err(e) => error!(
                                    index = index.get(),
                                    slot,
                                    error = %e,
                                    "Failed to persist offset entry to SharedWAL"
                                ),
                                Ok(wal_index) => {
                                    OFFSET_GROUP_LAST_PERSISTED[slot].store(
                                        index.get(),
                                        std::sync::atomic::Ordering::Relaxed,
                                    );
                                    // Track the WAL counter so `take_snapshot()` includes
                                    // the correct `last_included_wal_index` for the
                                    // eviction floor.
                                    og_state.last_applied_wal_index = wal_index;
                                    handle.update_committed_wal_index(wal_index);
                                }
                            }
                        }
                    }
                    drop(og_state);

                    // Notify any pending offset proposals for this (group, index).
                    {
                        let mut proposals = pending_offset_proposals.write().await;
                        if let Some(pos) = proposals.iter().position(|p| {
                            p.offset_group_id == *group_id && p.log_index == *index
                        }) {
                            let proposal = proposals.swap_remove(pos);
                            let _ = proposal.result_tx.send(Ok(()));
                        }
                    }
                    continue;
                }

                // Only process controller partition commits.
                if *group_id != CONTROLLER_GROUP_ID {
                    // Data partition commits are handled by OutputProcessor in actor mode.
                    continue;
                }

                // Controller commands are non-blob; metadata is the full command.
                // Empty metadata = Raft no-op entry (leader election); skip silently.
                if metadata.is_empty() {
                    continue;
                }
                let Some(cmd) = ControllerCommand::decode(metadata) else {
                    warn!(
                        index = index.get(),
                        metadata_len = metadata.len(),
                        "Failed to decode controller command"
                    );
                    continue;
                };

                info!(
                    index = index.get(),
                    command = ?cmd,
                    "Applying controller command (actor mode)"
                );
                // Collect group IDs that will be removed by DeleteTopic so
                // we can deregister them from the WAL floor after apply.
                let deleted_groups: Vec<GroupId> =
                    if let ControllerCommand::DeleteTopic { ref name } = cmd {
                        let state = controller_state.read().await;
                        state
                            .get_topic(name)
                            .map(|t| {
                                state
                                    .topic_assignments(t.topic_id)
                                    .map(|(_, a)| a.group_id)
                                    .collect()
                            })
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };

                let mut state = controller_state.write().await;
                let follow_ups = state.apply(&cmd, cluster_nodes);
                state.last_applied_index = index.get();
                state.last_applied_term = term.get();
                controller_entries_applied = controller_entries_applied.saturating_add(1);

                // Deregister deleted groups from WAL floor coordination.
                if !deleted_groups.is_empty() {
                    if let Some(pool) = shared_wal_pool {
                        for gid in &deleted_groups {
                            pool.deregister_group(*gid);
                        }
                    }
                }

                // Persist controller entry to SharedWAL for crash recovery.
                // Skip entries already recovered from WAL to avoid the
                // term-monotonicity assertion (a follower replaying from
                // the leader may see entries with earlier terms).
                let last_persisted =
                    CONTROLLER_LAST_PERSISTED_INDEX.load(std::sync::atomic::Ordering::Relaxed);
                if index.get() > last_persisted {
                    if let Some(pool) = shared_wal_pool {
                        let handle = pool.handle(CONTROLLER_GROUP_ID);
                        match handle
                            .append_nowait(term.get(), index.get(), metadata.clone())
                            .await
                        {
                            Err(e) => {
                                error!(
                                    index = index.get(),
                                    error = %e,
                                    "Failed to persist controller entry to SharedWAL"
                                );
                            }
                            Ok(wal_index) => {
                                CONTROLLER_LAST_PERSISTED_INDEX
                                    .store(index.get(), std::sync::atomic::Ordering::Relaxed);
                                // Track the WAL counter so `take_snapshot()` includes
                                // the correct `last_included_wal_index` for the eviction
                                // floor. Without this, controller snapshots always report
                                // wal_floor=0, pinning the eviction floor and preventing
                                // local segment deletion.
                                state.last_applied_wal_index = wal_index;
                                // Mark this WAL entry committed so the coordinator knows
                                // this group has consumed through wal_index. Without this,
                                // controller segments never become eligible for S3 tiering
                                // and are lost on every pod restart (ephemeral disk wipe).
                                handle.update_committed_wal_index(wal_index);
                            }
                        }
                    }
                }

                // Notify any pending controller proposals for this index.
                {
                    let mut proposals = pending_controller_proposals.write().await;
                    let pending_indexes: Vec<u64> =
                        proposals.iter().map(|p| p.log_index.get()).collect();
                    info!(
                        commit_index = index.get(),
                        pending_count = proposals.len(),
                        pending_indexes = ?pending_indexes,
                        "Checking pending controller proposals (actor mode)"
                    );
                    if let Some(pos) = proposals.iter().position(|p| p.log_index == *index) {
                        let proposal = proposals.swap_remove(pos);
                        let send_result = proposal.result_tx.send(Ok(()));
                        info!(
                            index = index.get(),
                            send_success = send_result.is_ok(),
                            "Notified pending controller proposal (actor mode)"
                        );
                    } else {
                        info!(
                            index = index.get(),
                            "No pending controller proposal for this index (actor mode)"
                        );
                    }
                }

                // Propose follow-up commands if we're the leader.
                // For single-node clusters, these commits happen immediately during propose,
                // so we collect the outputs and process them after the loop.
                if !follow_ups.is_empty() {
                    let is_leader = {
                        let mr = multi_raft.read().await;
                        mr.group_state(CONTROLLER_GROUP_ID)
                            .is_some_and(|s| s.state == RaftState::Leader)
                    };

                    if is_leader {
                        let mut mr = multi_raft.write().await;
                        for follow_up in &follow_ups {
                            let encoded = follow_up.encode();
                            match mr.propose(CONTROLLER_GROUP_ID, encoded) {
                                None => {
                                    warn!(
                                        command = ?follow_up,
                                        "Failed to propose follow-up controller command (actor mode)"
                                    );
                                }
                                Some(outputs) => {
                                    // Collect outputs for single-node processing.
                                    all_follow_up_outputs.extend(outputs);
                                }
                            }
                        }
                    }
                }

                // Handle AssignPartition by creating partition actor and storage.
                if let ControllerCommand::AssignPartition {
                    topic_id,
                    partition_id,
                    group_id: data_group_id,
                    ref replicas,
                } = cmd
                {
                    let node_id = {
                        let mr = multi_raft.read().await;
                        mr.node_id()
                    };

                    if replicas.contains(&node_id) {
                        // Look up any persisted vote state for this partition.
                        let (term, voted_for, observation_mode) = vote_store
                            .and_then(|vs| vs.lock().ok())
                            .and_then(|store| {
                                store
                                    .state()
                                    .get_group(data_group_id)
                                    .map(|v| (v.term, v.voted_for, false))
                            })
                            .unwrap_or((TermId::new(0), None, false));

                        info!(
                            topic = topic_id.get(),
                            partition = partition_id.get(),
                            group = data_group_id.get(),
                            term = term.get(),
                            voted_for = voted_for.map(NodeId::get),
                            replicas = ?replicas.iter().map(|n| n.get()).collect::<Vec<_>>(),
                            "Creating data partition from controller assignment (actor mode)"
                        );

                        // Update group map.
                        {
                            let mut gm = group_map.write().await;
                            gm.insert(topic_id, partition_id, data_group_id);
                        }

                        // Create partition storage (durable if SharedWalPool available).
                        // Extract commit metadata for Raft recovery state.
                        let (commit_index, commit_term) = {
                            let mut ps_map = partition_storage.write().await;
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                ps_map.entry(data_group_id)
                            {
                                let ps = create_partition_storage(
                                    topic_id,
                                    partition_id,
                                    data_group_id,
                                    shared_wal_pool,
                                    data_dir,
                                    recovered_entries,
                                    storage,
                                )
                                .await;
                                let ci = ps.last_applied();
                                let ct = ps.last_applied_term();
                                e.insert(Arc::new(RwLock::new(ps)));
                                (ci, ct)
                            } else {
                                // Storage already exists (non-vacant entry), read from it.
                                if let Some(ps_lock) = ps_map.get(&data_group_id) {
                                    let ps = ps_lock.read().await;
                                    (ps.last_applied(), ps.last_applied_term())
                                } else {
                                    (LogIndex::new(0), TermId::new(0))
                                }
                            }
                        };

                        // Create the partition actor with restored vote state and add it to the router.
                        let partition_handle =
                            super::actor_setup::create_partition_actor_with_state(
                                data_group_id,
                                node_id,
                                replicas.clone(),
                                term,
                                voted_for,
                                observation_mode,
                                commit_index,
                                commit_term,
                                output_tx.clone(),
                                super::partition_actor::PartitionActorConfig::default(),
                                Arc::clone(backpressure),
                                log_trailing_entries,
                            );

                        // Register with WAL floor so eviction_floor() accounts for this group.
                        if let Some(pool) = shared_wal_pool {
                            pool.register_group(data_group_id);
                        }

                        if router
                            .add_partition_dynamic(data_group_id, partition_handle)
                            .await
                        {
                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Created partition actor (actor mode)"
                            );
                        } else {
                            warn!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Partition actor already exists"
                            );
                        }
                    }
                }
            }
            MultiRaftOutput::BecameLeader { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Became controller leader (actor mode)");
                } else if is_offset_group(*group_id) {
                    info!(group_id = group_id.get(), "Became offset group leader");
                }
                // Ignore data partition leader changes - handled by OutputProcessor.
            }
            MultiRaftOutput::SteppedDown { group_id } => {
                if *group_id == CONTROLLER_GROUP_ID {
                    info!("Stepped down from controller leader (actor mode)");
                } else if is_offset_group(*group_id) {
                    info!(group_id = group_id.get(), "Stepped down from offset group leader");
                }
                // Ignore data partition step downs - handled by OutputProcessor.
            }
            MultiRaftOutput::SendMessages { to, messages } => {
                // Send controller and offset group messages; data partition messages go via OutputProcessor.
                let managed_messages: Vec<_> = messages
                    .iter()
                    .filter(|m| {
                        m.group_id == CONTROLLER_GROUP_ID || is_offset_group(m.group_id)
                    })
                    .cloned()
                    .collect();

                if !managed_messages.is_empty() {
                    if let Err(e) = transport_handle
                        .send_batch(*to, managed_messages.clone())
                        .await
                    {
                        error!(
                            to = to.get(),
                            count = managed_messages.len(),
                            error = %e,
                            "Failed to send controller/offset messages to peer (actor mode)"
                        );
                    } else {
                        debug!(
                            to = to.get(),
                            count = managed_messages.len(),
                            "Sent controller/offset messages to peer (actor mode)"
                        );
                    }
                }
            }
            MultiRaftOutput::VoteStateChanged {
                group_id,
                term,
                voted_for,
            } => {
                if *group_id == CONTROLLER_GROUP_ID || is_offset_group(*group_id) {
                    debug!(
                        group = group_id.get(),
                        term = term.get(),
                        voted_for = ?voted_for.map(helix_core::NodeId::get),
                        "Controller/offset group vote state changed (actor mode)"
                    );
                    // Persist vote state.
                    if let Some(vs) = vote_store {
                        if let Ok(mut store) = vs.lock() {
                            if let Err(e) = store.save(*group_id, *term, *voted_for) {
                                error!(
                                    group = group_id.get(),
                                    error = %e,
                                    "Failed to persist vote state"
                                );
                            }
                        }
                    }
                }
                // Ignore data partition vote changes - handled by OutputProcessor.
            }
            MultiRaftOutput::NeedEntries {
                group_id,
                follower_id,
                start_index,
                prev_log_index,
                max_bytes,
            } => {
                // Only serve NeedEntries for controller and offset groups.
                // Data partition groups are handled by the OutputProcessor.
                if *group_id != CONTROLLER_GROUP_ID && !is_offset_group(*group_id) {
                    continue;
                }

                let Some(pool) = shared_wal_pool else {
                    warn!(
                        group = group_id.get(),
                        follower = follower_id.get(),
                        "NeedEntries for controller/offset group but no SharedWalPool"
                    );
                    continue;
                };

                // Determine the end of the readable range from the persisted-index guard.
                // This is always the highest contiguous index we have on disk.
                let end_index = if *group_id == CONTROLLER_GROUP_ID {
                    CONTROLLER_LAST_PERSISTED_INDEX
                        .load(std::sync::atomic::Ordering::Relaxed)
                } else {
                    let slot = offset_group_slot(*group_id).unwrap_or(0);
                    OFFSET_GROUP_LAST_PERSISTED[slot]
                        .load(std::sync::atomic::Ordering::Relaxed)
                };

                if end_index < start_index.get() {
                    // Nothing we can serve yet (leader just started, no entries
                    // committed since restart). The follower will retry on the
                    // next NeedEntries once we have entries to send.
                    debug!(
                        group = group_id.get(),
                        follower = follower_id.get(),
                        start = start_index.get(),
                        end_index,
                        "NeedEntries: no entries available yet"
                    );
                    continue;
                }

                let handle = pool.handle(*group_id);

                // Read WAL entries for the raft_index range [start_index, end_index].
                let entries_result = handle
                    .read_entries_by_raft_index(start_index.get(), end_index, *max_bytes)
                    .await;

                let shared_entries = match entries_result {
                    Ok(e) => e,
                    Err(e) => {
                        // WAL read failed — entries may be tiered or compacted.
                        // Fall back to sending a live snapshot so the follower
                        // can fast-forward past the unavailable range.
                        eprintln!(
                            "[DST-PROBE] NEED_ENTRIES_FALLBACK: group={} follower={} start={} error={}",
                            group_id.get(),
                            follower_id.get(),
                            start_index.get(),
                            e,
                        );
                        warn!(
                            group = group_id.get(),
                            follower = follower_id.get(),
                            start = start_index.get(),
                            error = %e,
                            "Failed to read WAL entries for NeedEntries; \
                             attempting snapshot fallback"
                        );
                        provide_controller_group_snapshot(
                            *group_id,
                            *follower_id,
                            controller_state,
                            offset_group_states,
                            multi_raft,
                            transport_handle,
                        )
                        .await;
                        continue;
                    }
                };

                // WAL read succeeded but returned no entries — the requested
                // range has been compacted or tiered. Fall back to a snapshot
                // so the follower can fast-forward past the gap.
                if shared_entries.is_empty() {
                    warn!(
                        group = group_id.get(),
                        follower = follower_id.get(),
                        start = start_index.get(),
                        "WAL read returned empty entries; falling back to snapshot"
                    );
                    provide_controller_group_snapshot(
                        *group_id,
                        *follower_id,
                        controller_state,
                        offset_group_states,
                        multi_raft,
                        transport_handle,
                    )
                    .await;
                    continue;
                }

                // Look up prev_log_term from the WAL entry at prev_log_index.
                let prev_log_term = if prev_log_index.get() == 0 {
                    TermId::new(0)
                } else {
                    match handle.read_entry_by_raft_index(prev_log_index.get()).await {
                        Ok(Some(e)) => TermId::new(e.header.term),
                        Ok(None) => TermId::new(0),
                        Err(e) => {
                            // prev_log entry is unavailable (segment deleted by
                            // tiering/retention). Fall back to snapshot so the
                            // follower can fast-forward past the gap.
                            eprintln!(
                                "[DST-PROBE] NEED_ENTRIES_FALLBACK: group={} \
                                 follower={} prev_log={} error={}",
                                group_id.get(),
                                follower_id.get(),
                                prev_log_index.get(),
                                e,
                            );
                            warn!(
                                group = group_id.get(),
                                follower = follower_id.get(),
                                prev_log_index = prev_log_index.get(),
                                error = %e,
                                "Failed to read prev_log entry for NeedEntries; \
                                 attempting snapshot fallback"
                            );
                            provide_controller_group_snapshot(
                                *group_id,
                                *follower_id,
                                controller_state,
                                offset_group_states,
                                multi_raft,
                                transport_handle,
                            )
                            .await;
                            continue;
                        }
                    }
                };

                // Convert SharedEntry → LogEntry.
                // For shared WAL the raft_index field is the Raft log index;
                // the WAL auto-counter is different and irrelevant here.
                let log_entries: Vec<LogEntry> = shared_entries
                    .into_iter()
                    .map(|e| LogEntry {
                        term: TermId::new(e.header.term),
                        index: LogIndex::new(e.header.raft_index),
                        metadata: bytes::Bytes::copy_from_slice(&e.payload),
                        payload: bytes::Bytes::new(),
                    })
                    .collect();

                // Feed entries back into the Raft node and collect outbound messages.
                let outbound = {
                    let mut mr = multi_raft.write().await;
                    mr.provide_entries(
                        *group_id,
                        *follower_id,
                        *prev_log_index,
                        prev_log_term,
                        log_entries,
                    )
                };

                // Send any resulting AppendEntries messages to the follower.
                for msg_output in outbound {
                    if let MultiRaftOutput::SendMessages { to, messages } = msg_output {
                        // Filter to only controller/offset messages for safety.
                        let managed: Vec<_> = messages
                            .into_iter()
                            .filter(|m| {
                                m.group_id == CONTROLLER_GROUP_ID
                                    || is_offset_group(m.group_id)
                            })
                            .collect();
                        if !managed.is_empty() {
                            if let Err(e) =
                                transport_handle.send_batch(to, managed).await
                            {
                                warn!(
                                    to = to.get(),
                                    error = %e,
                                    "Failed to send NeedEntries response to peer"
                                );
                            }
                        }
                    }
                }
            }
            MultiRaftOutput::ApplySnapshot { group_id, snapshot } => {
                // Follower received a complete snapshot for a controller or offset
                // group. Decode the envelope and apply to the in-memory state machine.
                if *group_id != CONTROLLER_GROUP_ID && !is_offset_group(*group_id) {
                    // Data partition snapshots are handled by the output processor.
                    continue;
                }
                apply_controller_group_snapshot(
                    *group_id,
                    snapshot,
                    controller_state,
                    offset_group_states,
                )
                .await;
            }
        }
    }

    // Process follow-up outputs (for single-node clusters).
    // These are commits that happened during propose of follow-up commands.
    if !all_follow_up_outputs.is_empty() {
        for output in &all_follow_up_outputs {
            if let MultiRaftOutput::CommitEntry {
                group_id,
                index,
                term,
                metadata,
                ..
            } = output
            {
                if *group_id != CONTROLLER_GROUP_ID {
                    continue;
                }

                // Controller commands are non-blob; metadata is the full command.
                let Some(cmd) = ControllerCommand::decode(metadata) else {
                    warn!(
                        index = index.get(),
                        "Failed to decode follow-up controller command"
                    );
                    continue;
                };

                // Capture groups to deregister before apply removes them.
                let deleted_groups: Vec<GroupId> =
                    if let ControllerCommand::DeleteTopic { ref name } = cmd {
                        let state = controller_state.read().await;
                        state
                            .get_topic(name)
                            .map(|t| {
                                state
                                    .topic_assignments(t.topic_id)
                                    .map(|(_, a)| a.group_id)
                                    .collect()
                            })
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    };

                {
                    let mut state = controller_state.write().await;
                    let _ = state.apply(&cmd, cluster_nodes);
                    state.last_applied_index = index.get();
                    state.last_applied_term = term.get();
                }
                controller_entries_applied = controller_entries_applied.saturating_add(1);

                if !deleted_groups.is_empty() {
                    if let Some(pool) = shared_wal_pool {
                        for gid in &deleted_groups {
                            pool.deregister_group(*gid);
                        }
                    }
                }

                // Persist follow-up controller entry to SharedWAL.
                let last_persisted =
                    CONTROLLER_LAST_PERSISTED_INDEX.load(std::sync::atomic::Ordering::Relaxed);
                if index.get() > last_persisted {
                    if let Some(pool) = shared_wal_pool {
                        let handle = pool.handle(CONTROLLER_GROUP_ID);
                        if let Err(e) = handle
                            .append_nowait(term.get(), index.get(), metadata.clone())
                            .await
                        {
                            error!(
                                index = index.get(),
                                error = %e,
                                "Failed to persist follow-up controller entry to SharedWAL"
                            );
                        } else {
                            CONTROLLER_LAST_PERSISTED_INDEX
                                .store(index.get(), std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }

                // Handle AssignPartition by creating partition actor.
                if let ControllerCommand::AssignPartition {
                    topic_id,
                    partition_id,
                    group_id: data_group_id,
                    ref replicas,
                } = cmd
                {
                    let node_id = {
                        let mr = multi_raft.read().await;
                        mr.node_id()
                    };

                    if replicas.contains(&node_id) {
                        // Update group map.
                        {
                            let mut gm = group_map.write().await;
                            gm.insert(topic_id, partition_id, data_group_id);
                        }

                        // Create partition storage (durable if SharedWalPool available).
                        // Extract commit metadata for Raft recovery state.
                        let (commit_index, commit_term) = {
                            let mut ps_map = partition_storage.write().await;
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                ps_map.entry(data_group_id)
                            {
                                let ps = create_partition_storage(
                                    topic_id,
                                    partition_id,
                                    data_group_id,
                                    shared_wal_pool,
                                    data_dir,
                                    recovered_entries,
                                    storage,
                                )
                                .await;
                                let ci = ps.last_applied();
                                let ct = ps.last_applied_term();
                                e.insert(Arc::new(RwLock::new(ps)));
                                (ci, ct)
                            } else {
                                // Storage already exists (non-vacant entry), read from it.
                                if let Some(ps_lock) = ps_map.get(&data_group_id) {
                                    let ps = ps_lock.read().await;
                                    (ps.last_applied(), ps.last_applied_term())
                                } else {
                                    (LogIndex::new(0), TermId::new(0))
                                }
                            }
                        };

                        // Create the partition actor and add to router.
                        let partition_handle =
                            super::actor_setup::create_partition_actor_with_state(
                                data_group_id,
                                node_id,
                                replicas.clone(),
                                TermId::new(0),
                                None,
                                false,
                                commit_index,
                                commit_term,
                                output_tx.clone(),
                                super::partition_actor::PartitionActorConfig::default(),
                                Arc::clone(backpressure),
                                log_trailing_entries,
                            );

                        // Register with WAL floor so eviction_floor() accounts for this group.
                        if let Some(pool) = shared_wal_pool {
                            pool.register_group(data_group_id);
                        }

                        if router
                            .add_partition_dynamic(data_group_id, partition_handle)
                            .await
                        {
                            info!(
                                topic = topic_id.get(),
                                partition = partition_id.get(),
                                group = data_group_id.get(),
                                "Created partition actor from follow-up (single-node)"
                            );
                        } else {
                            warn!(
                                group = data_group_id.get(),
                                "Follow-up partition actor already exists"
                            );
                        }
                    }
                }
            }
        }
    }
    (controller_entries_applied, offset_entries_applied)
}

/// Creates partition actors for all pre-existing assignments after a restart.
///
/// This is the recovery counterpart to the `AssignPartition` handling in
/// `process_controller_outputs`. On a fresh start those entries are committed
/// for the first time and the tick task creates actors as it processes them.
/// After a restart the entries are already in the WAL — they won't appear as
/// new commits, so actors would never get created without this recovery step.
///
/// This function is idempotent: it skips groups whose actor already exists in
/// the router (the `add_partition_dynamic` call returns `false` in that case).
#[allow(clippy::too_many_arguments)]
async fn recover_partition_actors<S: Storage + Clone + Send + Sync + 'static>(
    node_id: NodeId,
    router: &Arc<super::router::PartitionRouter>,
    controller_state: &Arc<RwLock<ControllerState>>,
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    vote_store: Option<&Arc<Mutex<VoteStore<LocalFileVoteStorage>>>>,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    data_dir: Option<&PathBuf>,
    recovered_entries: &Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>,
    storage: &S,
    output_tx: &mpsc::Sender<super::partition_actor::GroupedOutput>,
    backpressure: &Arc<super::batcher::BackpressureState>,
    log_trailing_entries: Option<u64>,
) {
    // Snapshot the assignments under a short read lock, then release it before
    // doing any async I/O below.
    let assignments: Vec<(helix_core::TopicId, PartitionId, GroupId, Vec<NodeId>)> = {
        let state = controller_state.read().await;
        state
            .partitions_for_node(node_id)
            .into_iter()
            .map(|(tid, pid, a)| (tid, pid, a.group_id, a.replicas.clone()))
            .collect()
    };

    if assignments.is_empty() {
        return;
    }

    info!(
        node_id = node_id.get(),
        count = assignments.len(),
        "Recovering partition actors from controller state after restart"
    );

    let mut recovered = 0u32;

    for (topic_id, partition_id, data_group_id, replicas) in assignments {
        // Skip if the actor was already created (e.g., on a fresh start where
        // AssignPartition commits arrive before this recovery path runs).
        if router.partition(data_group_id).await.is_ok() {
            continue;
        }

        // Look up persisted vote state so the Raft node resumes from the
        // correct term and knows who it voted for.
        let (term, voted_for, observation_mode) = vote_store
            .and_then(|vs| vs.lock().ok())
            .and_then(|store| {
                store
                    .state()
                    .get_group(data_group_id)
                    .map(|v| (v.term, v.voted_for, false))
            })
            .unwrap_or((helix_core::TermId::new(0), None, false));

        // Update group map so that Kafka handlers can resolve topic+partition → group.
        {
            let mut gm = group_map.write().await;
            gm.insert(topic_id, partition_id, data_group_id);
        }

        // Create (or reopen) durable partition storage.
        let (commit_index, commit_term) = {
            let mut ps_map = partition_storage.write().await;
            if let std::collections::hash_map::Entry::Vacant(e) = ps_map.entry(data_group_id) {
                let ps = create_partition_storage(
                    topic_id,
                    partition_id,
                    data_group_id,
                    shared_wal_pool,
                    data_dir,
                    Some(recovered_entries),
                    Some(storage),
                )
                .await;
                let ci = ps.last_applied();
                let ct = ps.last_applied_term();
                e.insert(Arc::new(RwLock::new(ps)));
                (ci, ct)
            } else if let Some(ps_lock) = ps_map.get(&data_group_id) {
                let ps = ps_lock.read().await;
                (ps.last_applied(), ps.last_applied_term())
            } else {
                (helix_core::LogIndex::new(0), helix_core::TermId::new(0))
            }
        };

        // Spawn the partition actor with fully restored Raft state.
        let partition_handle = super::actor_setup::create_partition_actor_with_state(
            data_group_id,
            node_id,
            replicas,
            term,
            voted_for,
            observation_mode,
            commit_index,
            commit_term,
            output_tx.clone(),
            super::partition_actor::PartitionActorConfig::default(),
            Arc::clone(backpressure),
            log_trailing_entries,
        );

        // Register with WAL floor so eviction_floor() accounts for this group.
        if let Some(pool) = shared_wal_pool {
            pool.register_group(data_group_id);
        }

        if router
            .add_partition_dynamic(data_group_id, partition_handle)
            .await
        {
            info!(
                topic = topic_id.get(),
                partition = partition_id.get(),
                group = data_group_id.get(),
                term = term.get(),
                commit_index = commit_index.get(),
                "Recovered partition actor after restart"
            );
            recovered += 1;
        }
    }

    info!(
        node_id = node_id.get(),
        recovered,
        "Partition actor recovery complete"
    );
}

/// Creates partition storage using the best available durability mode:
/// 1. Shared WAL pool (when `shared_wal_pool` is set) — fsync amortization
/// 2. Per-partition dedicated WAL (when `data_dir` is set) — simpler, one WAL per partition
/// 3. In-memory (fallback) — no durability
async fn create_partition_storage<S: Storage + Clone + Send + Sync + 'static>(
    topic_id: helix_core::TopicId,
    partition_id: PartitionId,
    data_group_id: GroupId,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    data_dir: Option<&PathBuf>,
    recovered_entries: Option<&Arc<RwLock<HashMap<GroupId, PartitionRecoveryState>>>>,
    storage: Option<&S>,
) -> PartitionStorage<S> {
    // Mode 1: Shared WAL pool.
    if let (Some(pool), Some(dir)) = (shared_wal_pool, data_dir) {
        let wal_handle = pool.handle(data_group_id);
        // Remove pre-built recovery state (Default = no prior history if absent).
        let state = if let Some(entries) = recovered_entries {
            entries
                .write()
                .await
                .remove(&data_group_id)
                .unwrap_or_default()
        } else {
            PartitionRecoveryState::default()
        };

        #[cfg(feature = "s3")]
        let result = PartitionStorage::new_durable_with_shared_wal_state(
            dir,
            topic_id,
            partition_id,
            data_group_id,
            wal_handle,
            state,
            None, // object_storage_dir
            None, // s3_config
            None, // tiering_config
        );
        #[cfg(not(feature = "s3"))]
        let result = PartitionStorage::new_durable_with_shared_wal_state(
            dir,
            topic_id,
            partition_id,
            data_group_id,
            wal_handle,
            state,
            None, // object_storage_dir
            None, // tiering_config
        );

        return match result {
            Ok(ps) => {
                info!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    "Created durable partition storage via SharedWalPool"
                );
                ps
            }
            Err(e) => {
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to create shared WAL storage, falling back to in-memory"
                );
                PartitionStorage::new_in_memory(topic_id, partition_id)
            }
        };
    }

    // Mode 2: Per-partition dedicated WAL.
    if let (Some(dir), Some(storage)) = (data_dir, storage) {
        #[cfg(feature = "s3")]
        let result = PartitionStorage::new_durable(
            storage.clone(),
            dir,
            None, // object_storage_dir
            None, // s3_config
            None, // tiering_config
            topic_id,
            partition_id,
            data_group_id,
        )
        .await;
        #[cfg(not(feature = "s3"))]
        let result = PartitionStorage::new_durable(
            storage.clone(),
            dir,
            None, // object_storage_dir
            None, // tiering_config
            topic_id,
            partition_id,
            data_group_id,
        )
        .await;

        return match result {
            Ok(ps) => {
                info!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    "Created durable partition storage with per-partition WAL"
                );
                ps
            }
            Err(e) => {
                warn!(
                    topic = topic_id.get(),
                    partition = partition_id.get(),
                    error = %e,
                    "Failed to create per-partition WAL storage, falling back to in-memory"
                );
                PartitionStorage::new_in_memory(topic_id, partition_id)
            }
        };
    }

    // Mode 3: In-memory.
    PartitionStorage::new_in_memory(topic_id, partition_id)
}

/// Sends broker heartbeats to all peers via transport (Kafka `KRaft` pattern).
///
/// Unlike Raft-replicated state, heartbeats are soft state. Each broker:
/// 1. Records its own heartbeat locally
/// 2. Sends heartbeats to all other nodes via transport
///
/// This ensures all nodes have a local view of broker liveness without requiring
/// Raft consensus, which would only work for the leader node.
async fn send_broker_heartbeats_to_peers<T: TransportService>(
    local_broker_heartbeats: &Arc<RwLock<HashMap<NodeId, u64>>>,
    node_id: NodeId,
    cluster_nodes: &[NodeId],
    transport_handle: &T,
) {
    // Get current time in milliseconds.
    // Safe truncation: milliseconds won't overflow u64 for ~584 million years.
    #[allow(clippy::cast_possible_truncation)]
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    // Record our own heartbeat locally.
    {
        let mut heartbeats = local_broker_heartbeats.write().await;
        heartbeats.insert(node_id, timestamp_ms);
    }

    // Create the heartbeat message.
    let heartbeat = BrokerHeartbeat::new(node_id, timestamp_ms);

    // Send to all other nodes.
    for peer_id in cluster_nodes {
        if *peer_id == node_id {
            continue; // Don't send to self.
        }

        if let Err(e) = transport_handle.send_heartbeat(*peer_id, &heartbeat).await {
            debug!(
                peer_id = peer_id.get(),
                error = %e,
                "Failed to send heartbeat to peer"
            );
            // Non-fatal - we'll retry on next interval.
        }
    }

    debug!(
        node_id = node_id.get(),
        timestamp_ms = timestamp_ms,
        peer_count = cluster_nodes.len() - 1,
        "Sent broker heartbeats"
    );
}

/// Processes Multi-Raft outputs (single-node).
#[allow(clippy::too_many_lines, clippy::significant_drop_tightening)]
async fn process_outputs<S: Storage + Clone + Send + Sync + 'static>(
    outputs: &[MultiRaftOutput],
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    group_map: &Arc<RwLock<GroupMap>>,
    pending_proposals: &Arc<
        RwLock<HashMap<GroupId, Arc<RwLock<HashMap<LogIndex, PendingProposal>>>>>,
    >,
) {
    for output in outputs {
        match output {
            MultiRaftOutput::CommitEntry {
                group_id,
                index,
                term,
                metadata,
                payload,
            } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };

                // Get base_offset BEFORE apply for producer state recording.
                let base_offset = {
                    let ps_lock = {
                        let storage = partition_storage.read().await;
                        storage.get(group_id).cloned()
                    };
                    if let Some(ps_lock) = ps_lock {
                        let ps = ps_lock.read().await;
                        ps.blob_log_end_offset()
                    } else {
                        Offset::new(0)
                    }
                };

                let apply_result = if let Some((topic_id, partition_id)) = key {
                    let ps_lock = {
                        let storage = partition_storage.read().await;
                        storage.get(group_id).cloned()
                    };
                    if let Some(ps_lock) = ps_lock {
                        let mut ps = ps_lock.write().await;
                        match ps.apply_entry_async(*index, *term, metadata, payload).await {
                            Ok(offset) => Ok(offset),
                            Err(e) => {
                                warn!(
                                    topic = topic_id.get(),
                                    partition = partition_id.get(),
                                    error = %e,
                                    "Failed to apply committed entry"
                                );
                                Err(e)
                            }
                        }
                    } else {
                        Err(ServerError::Internal {
                            message: "partition storage not found".to_string(),
                        })
                    }
                } else {
                    Err(ServerError::Internal {
                        message: "group not found in group map".to_string(),
                    })
                };

                // Extract and record producer state from committed entry.
                // Critical for PREVIOUS_TERM entries on new leader.
                if apply_result.is_ok() {
                    extract_and_record_producer_state(
                        metadata,
                        payload,
                        base_offset,
                        *group_id,
                        partition_storage,
                    )
                    .await;
                }

                // Find and notify any pending proposal for this entry (O(1) lookup).
                let inner_lock = {
                    let proposals = pending_proposals.read().await;
                    proposals.get(group_id).cloned()
                };
                if let Some(inner_lock) = inner_lock {
                    let mut group_proposals = inner_lock.write().await;
                    if let Some(proposal) = group_proposals.remove(index) {
                        let result = match &apply_result {
                            Ok(Some(offset)) => Ok(*offset),
                            Ok(None) => {
                                // No offset returned (e.g., empty entry), use current log end.
                                let ps_lock = {
                                    let storage = partition_storage.read().await;
                                    storage.get(group_id).cloned()
                                };
                                let offset = if let Some(ps_lock) = ps_lock {
                                    let ps = ps_lock.read().await;
                                    ps.log_end_offset()
                                } else {
                                    Offset::new(0)
                                };
                                Ok(offset)
                            }
                            Err(e) => Err(ServerError::Internal {
                                message: format!("apply failed: {e}"),
                            }),
                        };
                        // Ignore send errors (receiver may have timed out).
                        let _ = proposal.result_tx.send(result);
                    }
                }
            }
            MultiRaftOutput::BecameLeader { group_id } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Became leader"
                    );
                }
            }
            MultiRaftOutput::SteppedDown { group_id } => {
                let key = {
                    let gm = group_map.read().await;
                    gm.get_key(*group_id)
                };
                if let Some((topic_id, partition_id)) = key {
                    info!(
                        topic = topic_id.get(),
                        partition = partition_id.get(),
                        group = group_id.get(),
                        "Stepped down from leader"
                    );
                }
            }
            MultiRaftOutput::SendMessages { to, messages } => {
                debug!(
                    to = to.get(),
                    count = messages.len(),
                    "Would send messages (single-node, ignoring)"
                );
            }
            MultiRaftOutput::VoteStateChanged {
                group_id,
                term,
                voted_for,
            } => {
                debug!(
                    group = group_id.get(),
                    term = term.get(),
                    voted_for = ?voted_for.map(helix_core::NodeId::get),
                    "Vote state changed (single-node, not persisting)"
                );
                // Single-node mode doesn't persist vote state.
                // Multi-node mode uses the VoteStore for persistence.
            }
            MultiRaftOutput::NeedEntries { .. } | MultiRaftOutput::ApplySnapshot { .. } => {
                // Single-node mode never has followers: no AppendEntries or snapshots needed.
            }
        }
    }
}

/// Processes tiering for all durable partitions and the shared WAL pool.
///
/// This function:
/// 1. Registers newly sealed segments with the tiering manager for dedicated WAL partitions
/// 2. Uploads eligible dedicated WAL segments to object storage (S3/filesystem)
/// 3. Uploads eligible shared WAL segments via `SharedWalPool::process_tiering()`
///
/// # Arguments
///
/// * `partition_storage` - Map of group ID to partition storage
/// * `shared_wal_pool` - Optional shared WAL pool for coordinator-level tiering
#[allow(clippy::significant_drop_tightening)]
async fn process_tiering<S: Storage + Clone + Send + Sync + 'static>(
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    local_retention_ms: Option<u64>,
) {
    // Collect group IDs of partitions with tiering enabled.
    // Note: We hold the outer read lock while checking has_tiering() on each partition.
    // This is intentional to get a consistent snapshot of which partitions have tiering.
    let tiering_groups: Vec<(GroupId, Arc<RwLock<PartitionStorage<S>>>)> = {
        let storage = partition_storage.read().await;
        let mut groups = Vec::new();
        for (group_id, ps_lock) in storage.iter() {
            let ps = ps_lock.read().await;
            if ps.has_tiering() {
                groups.push((*group_id, ps_lock.clone()));
            }
        }
        groups
    };

    // Process shared WAL coordinator tiering (uploads + local deletion of tiered segments).
    // This runs independently of whether any dedicated-WAL partitions have tiering enabled.
    if let Some(pool) = shared_wal_pool {
        pool.process_tiering(local_retention_ms).await;
    }

    if tiering_groups.is_empty() {
        return;
    }

    debug!(
        partition_count = tiering_groups.len(),
        "Processing tiering for partitions"
    );

    // `TigerStyle`: bounded iteration.
    for (group_id, ps_lock) in tiering_groups.iter().take(100) {
        // Register newly sealed segments.
        {
            let mut ps = ps_lock.write().await;
            match ps.check_and_register_sealed_segments().await {
                Ok(count) if count > 0 => {
                    info!(
                        group = group_id.get(),
                        registered = count,
                        "Registered sealed segments for tiering"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        group = group_id.get(),
                        error = %e,
                        "Failed to register sealed segments"
                    );
                }
            }
        }

        // Upload eligible segments.
        {
            let ps = ps_lock.read().await;
            match ps.tier_eligible_segments().await {
                Ok(count) if count > 0 => {
                    info!(
                        group = group_id.get(),
                        tiered = count,
                        "Tiered segments to object storage"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        group = group_id.get(),
                        error = %e,
                        "Failed to tier eligible segments"
                    );
                }
            }
        }
    }
}

/// Runs segment retention for both dedicated and shared WAL partitions.
///
/// For **dedicated WAL** partitions: calls `run_retention(min_replicated_index)`
/// on each partition where this node is the leader.
///
/// For **shared WAL** pools: checks each coordinator's sealed segments, verifies
/// that ALL groups with entries in a segment are fully replicated, and deletes
/// eligible segments that are old enough.
async fn process_retention<S: Storage + Clone + Send + Sync + 'static>(
    partition_storage: &Arc<RwLock<HashMap<GroupId, Arc<RwLock<PartitionStorage<S>>>>>>,
    router: &Arc<super::router::PartitionRouter>,
    shared_wal_pool: Option<&Arc<SharedWalPool<S>>>,
    local_retention_ms: u64,
) {
    // Collect min_replicated_index from all partition actors.
    let replicated_indices: HashMap<GroupId, u64> = router
        .min_replicated_indices()
        .await
        .into_iter()
        .collect();

    // --- Dedicated WAL retention ---
    // Only process partitions where this node is leader (non-zero min_replicated_index).
    let dedicated_groups: Vec<(GroupId, Arc<RwLock<PartitionStorage<S>>>)> = {
        let storage = partition_storage.read().await;
        storage
            .iter()
            .filter_map(|(gid, ps_lock)| {
                let min_rep = replicated_indices.get(gid).copied().unwrap_or(0);
                if min_rep > 0 {
                    Some((*gid, ps_lock.clone()))
                } else {
                    None
                }
            })
            .collect()
    };

    // TigerStyle: bounded iteration.
    for (group_id, ps_lock) in dedicated_groups.iter().take(200) {
        let min_rep = replicated_indices.get(group_id).copied().unwrap_or(0);
        let mut ps = ps_lock.write().await;
        match ps.run_retention(min_rep, local_retention_ms).await {
            Ok(deleted) if deleted > 0 => {
                info!(
                    group = group_id.get(),
                    deleted,
                    "Retention: deleted dedicated WAL segments"
                );
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    group = group_id.get(),
                    error = %e,
                    "Failed to run retention"
                );
            }
        }
    }

    // --- Shared WAL retention ---
    if let Some(pool) = shared_wal_pool {
        for coordinator in pool.coordinators() {
            // When tiering is configured, the tiering path owns local deletion
            // (file-only, after local_retention_ms). The retention path only
            // handles the no-tiering case (full delete, no S3 fallback needed).
            if coordinator.has_tiering().await {
                continue;
            }

            let infos = coordinator.sealed_segment_infos().await;
            if infos.is_empty() {
                continue;
            }

            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .expect("system clock before UNIX epoch")
                .as_secs();

            // TigerStyle: bounded iteration.
            for info in infos.iter().take(200) {
                if !info.is_sealed {
                    continue;
                }

                let retention_secs = local_retention_ms / 1000;
                let age_ok = info
                    .sealed_at_secs
                    .is_some_and(|sealed_at| now_secs.saturating_sub(sealed_at) >= retention_secs);
                if !age_ok {
                    continue;
                }

                // Check that ALL groups in this segment are fully replicated.
                // groups_in_segment returns (group_id, max_raft_index, max_wal_counter).
                // Use max_raft_index for the replication safety check — WAL auto-counters
                // and Raft log indices are different spaces (no-ops advance raft_index but
                // are not stored in SharedWal), so comparing WAL counter against Raft
                // match_index can allow premature deletion.
                let groups = coordinator.groups_in_segment(info.segment_id).await;
                let all_replicated = groups.iter().all(|(gid, max_raft_idx, _max_wal_idx)| {
                    let min_rep = replicated_indices.get(gid).copied().unwrap_or(0);
                    // If min_rep is 0, this node is not leader for this group.
                    // We can't safely delete — skip this segment.
                    min_rep > 0 && *max_raft_idx <= min_rep
                });

                if !all_replicated {
                    continue;
                }

                match coordinator.delete_sealed_segment(info.segment_id).await {
                    Ok(()) => {
                        info!(
                            segment_id = info.segment_id.get(),
                            groups = groups.len(),
                            "Retention: deleted shared WAL segment"
                        );
                        // Trim each affected partition's BlobIndex to free memory.
                        // Use max_wal_counter (not raft_index) — BlobIndex is keyed by
                        // WAL auto-counter, so max_wal_counter+1 is the new floor.
                        let storage = partition_storage.read().await;
                        for (group_id, _max_raft_idx, max_wal_idx) in &groups {
                            if let Some(ps_lock) = storage.get(group_id) {
                                let mut ps = ps_lock.write().await;
                                ps.trim_blob_index(max_wal_idx + 1);
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            segment_id = info.segment_id.get(),
                            error = %e,
                            "Failed to delete shared WAL segment"
                        );
                    }
                }
            }
        }
    }
}

// ============================================================================
// Snapshot helpers for controller/offset groups
// ============================================================================

/// Takes a live snapshot from a controller or offset group and sends it to
/// the follower via the Raft `InstallSnapshot` path.
///
/// Called by the `NeedEntries` handler when the WAL read fails (entries are
/// tiered or compacted). The follower applies the snapshot and then requests
/// only the entries after `last_included_index`.
///
/// # Panics
///
/// Does not panic; all fallible operations are logged and silently ignored.
async fn provide_controller_group_snapshot<T: TransportService>(
    group_id: GroupId,
    follower_id: NodeId,
    controller_state: &Arc<RwLock<ControllerState>>,
    offset_group_states: &[Arc<RwLock<OffsetGroupState>>],
    multi_raft: &Arc<RwLock<MultiRaft>>,
    transport_handle: &T,
) {
    // Take a snapshot from the appropriate in-memory state machine.
    let snap_result = if group_id == CONTROLLER_GROUP_ID {
        controller_state.read().await.take_snapshot()
    } else if let Some(slot) = offset_group_slot(group_id) {
        offset_group_states[slot].read().await.take_snapshot()
    } else {
        return;
    };

    let (meta, body) = match snap_result {
        Ok(pair) if pair.0.last_included_index > 0 => {
            eprintln!(
                "[DST-PROBE] PROVIDE_SNAPSHOT: group={} follower={} index={}",
                group_id.get(),
                follower_id.get(),
                pair.0.last_included_index,
            );
            pair
        }
        Ok(_) => {
            // State machine has no entries applied yet — nothing to snapshot.
            debug!(
                group = group_id.get(),
                follower = follower_id.get(),
                "No snapshot available for NeedEntries fallback (index=0)"
            );
            return;
        }
        Err(e) => {
            warn!(
                group = group_id.get(),
                follower = follower_id.get(),
                error = %e,
                "take_snapshot failed during NeedEntries fallback"
            );
            return;
        }
    };

    // Encode snapshot into the common envelope format so the follower can
    // decode both the application body and the full SnapshotMeta.
    let encoded = encode_envelope(&meta, &body);
    let snapshot = helix_raft::snapshot::Snapshot::new(
        LogIndex::new(meta.last_included_index),
        TermId::new(meta.last_included_term),
        encoded,
    );

    // Feed through the Raft layer, which sends InstallSnapshotRequest chunks.
    let outbound = multi_raft
        .write()
        .await
        .provide_group_snapshot(group_id, follower_id, &snapshot);

    // Send resulting messages to the follower.
    for msg_output in outbound {
        if let MultiRaftOutput::SendMessages { to, messages } = msg_output {
            let managed: Vec<_> = messages
                .into_iter()
                .filter(|m| {
                    m.group_id == CONTROLLER_GROUP_ID || is_offset_group(m.group_id)
                })
                .collect();
            if !managed.is_empty() {
                if let Err(e) = transport_handle.send_batch(to, managed).await {
                    warn!(
                        to = to.get(),
                        error = %e,
                        "Failed to send InstallSnapshot to follower"
                    );
                }
            }
        }
    }
}

/// Applies a received `InstallSnapshot` to the controller or offset group
/// in-memory state machine.
///
/// Decodes the snapshot envelope, validates the group ID, and calls
/// `apply_snapshot` on the appropriate state machine under write lock.
///
/// # Panics
///
/// Does not panic; all fallible operations are logged and silently ignored.
async fn apply_controller_group_snapshot(
    group_id: GroupId,
    snapshot: &helix_raft::snapshot::Snapshot,
    controller_state: &Arc<RwLock<ControllerState>>,
    offset_group_states: &[Arc<RwLock<OffsetGroupState>>],
) {
    assert!(
        snapshot.last_included_index.get() > 0,
        "apply_controller_group_snapshot: snapshot must have last_included_index > 0"
    );

    let (meta, body) = match decode_envelope(&snapshot.data) {
        Ok(pair) => pair,
        Err(e) => {
            warn!(
                group = group_id.get(),
                index = snapshot.last_included_index.get(),
                error = %e,
                "Failed to decode snapshot envelope for controller/offset group"
            );
            return;
        }
    };

    if group_id == CONTROLLER_GROUP_ID {
        let mut state = controller_state.write().await;
        if let Err(e) = state.apply_snapshot(&meta, &body) {
            warn!(
                group = group_id.get(),
                index = meta.last_included_index,
                error = %e,
                "Failed to apply snapshot to ControllerState"
            );
        } else {
            eprintln!(
                "[DST-PROBE] APPLY_SNAPSHOT: group={} index={} type=controller",
                group_id.get(),
                meta.last_included_index,
            );
            info!(
                group = group_id.get(),
                index = meta.last_included_index,
                "Applied snapshot to ControllerState"
            );
        }
    } else if let Some(slot) = offset_group_slot(group_id) {
        let mut state = offset_group_states[slot].write().await;
        if let Err(e) = state.apply_snapshot(&meta, &body) {
            warn!(
                group = group_id.get(),
                slot,
                index = meta.last_included_index,
                error = %e,
                "Failed to apply snapshot to OffsetGroupState"
            );
        } else {
            eprintln!(
                "[DST-PROBE] APPLY_SNAPSHOT: group={} slot={} index={} type=offset_group",
                group_id.get(),
                slot,
                meta.last_included_index,
            );
            info!(
                group = group_id.get(),
                slot,
                index = meta.last_included_index,
                "Applied snapshot to OffsetGroupState"
            );
        }
    }
}

//! Topic management handlers for the Helix service.

use helix_core::PartitionId;
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use tokio::sync::oneshot;
use tracing::{debug, info};

use helix_runtime::TransportService;
use helix_wal::Storage;

use crate::controller::{ControllerCommand, CONTROLLER_GROUP_ID};
use crate::error::{ServerError, ServerResult};
use crate::partition_storage::PartitionStorage;

use super::super::{HelixService, PendingControllerProposal, TopicMetadata};

impl<S: Storage + Clone + Send + Sync + 'static, T: TransportService> HelixService<S, T> {
    /// Creates a topic with the specified number of partitions.
    ///
    /// # Errors
    /// Returns an error if the topic already exists.
    ///
    /// # Panics
    /// Panics if `partition_count` is not in the range (0, 256].
    #[allow(clippy::significant_drop_tightening, clippy::too_many_lines)]
    pub async fn create_topic(&self, name: String, partition_count: i32) -> ServerResult<()> {
        assert!(partition_count > 0, "partition_count must be positive");
        assert!(partition_count <= 256, "partition_count exceeds limit");

        let mut topics = self.topics.write().await;
        if topics.contains_key(&name) {
            return Err(ServerError::TopicAlreadyExists { topic: name });
        }

        let mut next_id = self.next_topic_id.write().await;
        let topic_id = helix_core::TopicId::new(*next_id);
        *next_id += 1;

        // Create partition groups.
        let mut multi_raft = self.multi_raft.write().await;
        let mut group_map = self.group_map.write().await;
        let mut partition_storage = self.partition_storage.write().await;

        for i in 0..partition_count {
            // Safe cast: partition_count is bounded by 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);

            // Allocate group ID for this partition.
            let group_id = group_map.allocate(topic_id, partition_id);

            // Create Raft group (single-node cluster).
            multi_raft
                .create_group(group_id, self.cluster_nodes.clone())
                .map_err(|e| ServerError::Internal {
                    message: format!("failed to create Raft group: {e}"),
                })?;

            // Create partition storage (durable or in-memory based on config).
            let ps = if let Some(ref pool) = self.shared_wal_pool {
                // Shared WAL mode: get handle from pool and recovered entries.
                let data_dir = self
                    .data_dir
                    .as_ref()
                    .expect("data_dir must be set with shared_wal_pool");
                let wal_handle = pool.handle(partition_id);
                let recovered = self
                    .recovered_entries
                    .write()
                    .await
                    .remove(&partition_id)
                    .unwrap_or_default();

                #[cfg(feature = "s3")]
                let ps_result = PartitionStorage::new_durable_with_shared_wal(
                    data_dir,
                    topic_id,
                    partition_id,
                    wal_handle,
                    recovered,
                    self.object_storage_dir.as_ref(),
                    self.s3_config.as_ref(),
                    self.tiering_config.as_ref(),
                )
                .await;
                #[cfg(not(feature = "s3"))]
                let ps_result = PartitionStorage::new_durable_with_shared_wal(
                    data_dir,
                    topic_id,
                    partition_id,
                    wal_handle,
                    recovered,
                    self.object_storage_dir.as_ref(),
                    self.tiering_config.as_ref(),
                )
                .await;
                ps_result.map_err(|e| ServerError::Internal {
                    message: format!("failed to create partition with shared WAL: {e}"),
                })?
            } else {
                // Dedicated WAL mode (used when shared WAL is not available).
                #[cfg(feature = "s3")]
                let ps_inner = if let Some(data_dir) = &self.data_dir {
                    PartitionStorage::new_durable(
                        self.storage.clone(),
                        data_dir,
                        self.object_storage_dir.as_ref(),
                        self.s3_config.as_ref(),
                        self.tiering_config.as_ref(),
                        topic_id,
                        partition_id,
                    )
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to create durable partition: {e}"),
                    })?
                } else {
                    PartitionStorage::new_in_memory(topic_id, partition_id)
                };
                #[cfg(not(feature = "s3"))]
                let ps_inner = if let Some(data_dir) = &self.data_dir {
                    PartitionStorage::new_durable(
                        self.storage.clone(),
                        data_dir,
                        self.object_storage_dir.as_ref(),
                        self.tiering_config.as_ref(),
                        topic_id,
                        partition_id,
                    )
                    .await
                    .map_err(|e| ServerError::Internal {
                        message: format!("failed to create durable partition: {e}"),
                    })?
                } else {
                    PartitionStorage::new_in_memory(topic_id, partition_id)
                };
                ps_inner
            };
            partition_storage.insert(group_id, std::sync::Arc::new(tokio::sync::RwLock::new(ps)));

            // For single-node cluster, tick until the node becomes leader.
            // With default election_tick=10 and randomized timeout in [10, 20),
            // we need up to 20 ticks to guarantee an election.
            if self.cluster_nodes.len() == 1 {
                for _ in 0..25 {
                    let outputs = multi_raft.tick();

                    // Apply any committed entries.
                    for output in &outputs {
                        if let MultiRaftOutput::CommitEntry {
                            group_id: gid,
                            index,
                            data,
                        } = output
                        {
                            if *gid == group_id {
                                if let Some(ps_lock) = partition_storage.get(&group_id) {
                                    let mut ps = ps_lock.write().await;
                                    let _ = ps.apply_entry_async(*index, data).await;
                                }
                            }
                        }
                    }

                    // Check if became leader.
                    let is_leader = multi_raft
                        .group_state(group_id)
                        .is_some_and(|s| s.state == RaftState::Leader);

                    if is_leader {
                        break;
                    }
                }
            }
        }

        topics.insert(
            name.clone(),
            TopicMetadata {
                topic_id,
                partition_count,
            },
        );

        info!(topic = %name, partitions = partition_count, "Created topic");
        Ok(())
    }

    /// Creates a topic through the controller partition (multi-node mode).
    ///
    /// This proposes a `CreateTopic` command to the controller Raft group and waits
    /// for the full topic creation flow to complete:
    /// 1. `CreateTopic` command commits
    /// 2. `AssignPartition` commands commit (generated by controller leader)
    /// 3. Data Raft groups elect leaders
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the controller leader, if the
    /// proposal fails, or if the topic creation times out.
    ///
    /// # Panics
    ///
    /// Panics in single-node actor mode if internal actor output channels are
    /// unexpectedly unavailable during immediate output processing.
    #[allow(clippy::too_many_lines)] // Synchronous topic creation is a single logical unit.
    pub async fn create_topic_via_controller(
        &self,
        name: String,
        partition_count: u32,
        replication_factor: u32,
    ) -> ServerResult<()> {
        const TOPIC_CREATION_TIMEOUT_MS: u64 = 30_000;
        const POLL_INTERVAL_MS: u64 = 50;

        // Check if topic already exists in controller state.
        {
            let state = self.controller_state.read().await;
            if state.topic_exists(&name) {
                return Err(ServerError::TopicAlreadyExists { topic: name });
            }
        }

        // Propose CreateTopic command to controller and get the log index.
        let command = ControllerCommand::CreateTopic {
            name: name.clone(),
            partition_count,
            replication_factor,
        };
        let encoded = command.encode();

        let (result_tx, result_rx) = oneshot::channel();

        // For single-node with actor infrastructure, we need to process propose outputs
        // immediately because commits happen during propose, not during tick.
        let is_single_node_actor =
            self.cluster_nodes.len() == 1 && self.actor_router.is_some();

        let (proposed_index, propose_outputs) = {
            let mut mr = self.multi_raft.write().await;
            let Some((outputs, index)) = mr.propose_with_index(CONTROLLER_GROUP_ID, encoded) else {
                // Get controller leader hint if available.
                let controller_hint = mr
                    .group_state(CONTROLLER_GROUP_ID)
                    .and_then(|s| s.leader_id)
                    .map(helix_core::NodeId::get);
                drop(mr); // Release lock early before returning.
                return Err(ServerError::NotController { controller_hint });
            };
            drop(mr); // Release lock early before awaiting result.
            (index, outputs)
        };

        // Register the pending controller proposal.
        let pending_count = {
            let mut proposals = self.pending_controller_proposals.write().await;
            proposals.push(PendingControllerProposal {
                log_index: proposed_index,
                result_tx,
            });
            proposals.len()
        };

        // For single-node actor mode, process propose outputs immediately.
        // This handles commits that happen during propose rather than tick.
        if is_single_node_actor && !propose_outputs.is_empty() {
            if let (Some(router), Some(output_tx)) = (&self.actor_router, &self.actor_output_tx) {
                debug!(
                    topic = %name,
                    outputs = propose_outputs.len(),
                    "Single-node actor: processing propose outputs"
                );
                crate::service::tick::process_controller_outputs(
                    &propose_outputs,
                    &self.multi_raft,
                    &self.partition_storage,
                    &self.group_map,
                    &self.controller_state,
                    &self.pending_proposals,
                    &self.pending_controller_proposals,
                    &self.cluster_nodes,
                    self.transport_handle
                        .as_ref()
                        .expect("transport must be set"),
                    router,
                    output_tx,
                    None, // vote_store not available here
                    self.shared_wal_pool.as_ref(),
                    self.data_dir.as_ref(),
                    Some(&self.recovered_entries),
                    Some(&self.storage),
                )
                .await;
            }
        }

        info!(
            topic = %name,
            partitions = partition_count,
            replication = replication_factor,
            log_index = proposed_index.get(),
            pending_count = pending_count,
            pending_proposals_ptr = ?std::sync::Arc::as_ptr(&self.pending_controller_proposals),
            "Proposed topic creation to controller, waiting for commit"
        );

        // Step 1: Wait for CreateTopic command to commit.
        tokio::time::timeout(
            std::time::Duration::from_millis(TOPIC_CREATION_TIMEOUT_MS),
            result_rx,
        )
        .await
        .map_err(|_| ServerError::Internal {
            message: format!("timeout waiting for CreateTopic commit for {name}"),
        })?
        .map_err(|_| ServerError::Internal {
            message: "controller proposal channel closed".to_string(),
        })??;

        debug!(topic = %name, "CreateTopic command committed");

        // Step 2: Wait for all AssignPartition commands to commit.
        // Poll controller_state until all partitions have assignments.
        let topic_id = {
            let state = self.controller_state.read().await;
            state
                .get_topic(&name)
                .map(|t| t.topic_id)
                .ok_or_else(|| ServerError::Internal {
                    message: format!("topic {name} not found in controller state after commit"),
                })?
        };

        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(TOPIC_CREATION_TIMEOUT_MS);

        loop {
            // Check if all partitions have assignments.
            let all_assigned = {
                let state = self.controller_state.read().await;
                (0..partition_count).all(|p| {
                    state
                        .get_assignment(topic_id, PartitionId::new(u64::from(p)))
                        .is_some()
                })
            };

            if all_assigned {
                debug!(topic = %name, partitions = partition_count, "All partitions assigned");
                break;
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(ServerError::Internal {
                    message: format!("timeout waiting for partition assignments for {name}"),
                });
            }

            tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        // Step 3: Data partitions are managed by partition actors, not MultiRaft.
        // Skip the leader check since the partition actors elect leaders independently.
        // Clients will get LEADER_NOT_AVAILABLE and retry if leaders aren't ready yet.
        debug!(topic = %name, "Skipping leader election check (actor mode)");

        info!(
            topic = %name,
            partitions = partition_count,
            replication = replication_factor,
            "Topic created successfully"
        );

        Ok(())
    }

    /// Gets topic metadata by name.
    #[allow(clippy::significant_drop_tightening)] // Lock dropped before fallback lookup.
    pub(crate) async fn get_topic(&self, name: &str) -> Option<TopicMetadata> {
        // Check controller state first — all topic metadata is managed
        // by the controller partition.
        let state = self.controller_state.read().await;
        if let Some(info) = state.get_topic(name) {
            // Safe cast: partition_count is bounded by 256.
            #[allow(clippy::cast_possible_wrap)]
            return Some(TopicMetadata {
                topic_id: info.topic_id,
                partition_count: info.partition_count as i32,
            });
        }
        drop(state);

        // Fallback to local topics map (legacy single-node path).
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    /// Gets all topics and their partition counts.
    ///
    /// Returns a list of (`topic_name`, `partition_count`) pairs.
    pub async fn get_all_topics(&self) -> Vec<(String, i32)> {
        // All topic metadata is managed by the controller partition.
        let state = self.controller_state.read().await;
        state
            .topics()
            .map(|info| {
                // Safe cast: partition_count is bounded by 256.
                #[allow(clippy::cast_possible_wrap)]
                (info.name.clone(), info.partition_count as i32)
            })
            .collect()
    }

    /// Checks if a topic exists.
    pub async fn topic_exists(&self, topic: &str) -> bool {
        let state = self.controller_state.read().await;
        state.topic_exists(topic)
    }
}

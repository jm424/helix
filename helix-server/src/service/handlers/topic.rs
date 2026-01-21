//! Topic management handlers for the Helix service.

use helix_core::PartitionId;
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use helix_wal::TokioStorage;

use crate::controller::{ControllerCommand, CONTROLLER_GROUP_ID};
use crate::error::{ServerError, ServerResult};
use crate::partition_storage::ServerPartitionStorage;

use super::super::{HelixService, PendingControllerProposal, TopicMetadata};

impl HelixService {
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
            return Err(ServerError::Internal {
                message: format!("topic already exists: {name}"),
            });
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
                let data_dir = self.data_dir.as_ref().expect("data_dir must be set with shared_wal_pool");
                let wal_handle = pool.handle(partition_id);
                let recovered = self
                    .recovered_entries
                    .write()
                    .await
                    .remove(&partition_id)
                    .unwrap_or_default();

                #[cfg(feature = "s3")]
                let ps_result = ServerPartitionStorage::new_durable_with_shared_wal(
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
                let ps_result = ServerPartitionStorage::new_durable_with_shared_wal(
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
                    ServerPartitionStorage::new_durable(
                        TokioStorage::new(),
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
                    ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                };
                #[cfg(not(feature = "s3"))]
                let ps_inner = if let Some(data_dir) = &self.data_dir {
                    ServerPartitionStorage::new_durable(
                        TokioStorage::new(),
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
                    ServerPartitionStorage::new_in_memory(topic_id, partition_id)
                };
                ps_inner
            };
            partition_storage.insert(group_id, ps);

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
                                if let Some(ps) = partition_storage.get_mut(&group_id) {
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
                return Err(ServerError::Internal {
                    message: format!("topic already exists: {name}"),
                });
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

        let proposed_index = {
            let mut mr = self.multi_raft.write().await;
            let Some((_, index)) = mr.propose_with_index(CONTROLLER_GROUP_ID, encoded) else {
                // Get controller leader hint if available.
                let controller_hint = mr
                    .group_state(CONTROLLER_GROUP_ID)
                    .and_then(|s| s.leader_id)
                    .map(helix_core::NodeId::get);
                drop(mr); // Release lock early before returning.
                return Err(ServerError::NotController { controller_hint });
            };
            drop(mr); // Release lock early before awaiting result.
            index
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
                    message: format!(
                        "timeout waiting for partition assignments for {name}"
                    ),
                });
            }

            tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
        }

        // Step 3: Wait for data Raft groups to elect leaders.
        // This ensures clients can immediately produce/consume after topic creation.
        if self.actor_mode {
            // In actor mode, data partitions are managed by partition actors, not MultiRaft.
            // Skip the leader check since the partition actors will elect leaders independently.
            // Clients will get LEADER_NOT_AVAILABLE and retry if leaders aren't ready yet.
            debug!(topic = %name, "Actor mode: skipping leader election check");
        } else {
            loop {
                let all_have_leaders = {
                    let state = self.controller_state.read().await;
                    let mr = self.multi_raft.read().await;

                    (0..partition_count).all(|p| {
                        let partition_id = PartitionId::new(u64::from(p));
                        state.get_assignment(topic_id, partition_id).is_some_and(|assignment| {
                            mr.group_state(assignment.group_id)
                                .is_some_and(|gs| gs.leader_id.is_some())
                        })
                    })
                };

                if all_have_leaders {
                    debug!(topic = %name, "All partitions have leaders");
                    break;
                }

                if tokio::time::Instant::now() >= deadline {
                    warn!(
                        topic = %name,
                        "Timeout waiting for leader election, returning success anyway"
                    );
                    // Don't fail here - partitions are created, just leaders not elected yet.
                    // Clients will get LEADER_NOT_AVAILABLE and retry.
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
        }

        info!(
            topic = %name,
            partitions = partition_count,
            replication = replication_factor,
            "Topic created successfully"
        );

        Ok(())
    }

    /// Gets topic metadata by name.
    pub(crate) async fn get_topic(&self, name: &str) -> Option<TopicMetadata> {
        // In multi-node mode, read from controller state.
        if self.is_multi_node() {
            let state = self.controller_state.read().await;
            return state.get_topic(name).map(|info| {
                // Safe cast: partition_count is bounded by 256.
                #[allow(clippy::cast_possible_wrap)]
                TopicMetadata {
                    topic_id: info.topic_id,
                    partition_count: info.partition_count as i32,
                }
            });
        }

        // Single-node mode uses local topics map.
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }

    /// Gets all topics and their partition counts.
    ///
    /// Returns a list of (`topic_name`, `partition_count`) pairs.
    pub async fn get_all_topics(&self) -> Vec<(String, i32)> {
        // In multi-node mode, read from controller state.
        if self.is_multi_node() {
            let state = self.controller_state.read().await;
            return state
                .topics()
                .map(|info| {
                    // Safe cast: partition_count is bounded by 256.
                    #[allow(clippy::cast_possible_wrap)]
                    (info.name.clone(), info.partition_count as i32)
                })
                .collect();
        }

        // Single-node mode uses local topics map.
        let topics = self.topics.read().await;
        topics
            .iter()
            .map(|(name, meta)| (name.clone(), meta.partition_count))
            .collect()
    }

    /// Checks if a topic exists.
    pub async fn topic_exists(&self, topic: &str) -> bool {
        // In multi-node mode, check controller state.
        if self.is_multi_node() {
            let state = self.controller_state.read().await;
            return state.topic_exists(topic);
        }

        // Single-node mode uses local topics map.
        let topics = self.topics.read().await;
        topics.contains_key(topic)
    }
}

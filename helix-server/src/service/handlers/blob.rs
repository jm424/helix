//! Blob storage handlers for the Helix service (Kafka zero-copy support).

use bytes::Bytes;
use helix_core::{NodeId, Offset, PartitionId};
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use helix_wal::Storage;
use tokio::sync::oneshot;
use tracing::{debug, info, warn, Instrument};

use helix_runtime::TransportService;

use crate::error::{ServerError, ServerResult};
use crate::partition_storage::PartitionStorageInner;
use crate::storage::{BlobFormat, PartitionCommand};

use super::super::{HelixService, PendingProposal};

impl<S: Storage + Clone + Send + Sync + 'static, T: TransportService> HelixService<S, T> {
    /// Checks if this node is the leader for the given group.
    ///
    /// Returns `(is_leader, leader_hint)`. In actor mode, queries the partition
    /// actor; otherwise queries `MultiRaft`.
    #[allow(clippy::significant_drop_tightening)] // Lock dropped after group_state returns owned value.
    async fn check_leadership(
        &self,
        group_id: helix_core::GroupId,
    ) -> (bool, Option<NodeId>) {
        if let Some(router) = &self.actor_router {
            match router.partition(group_id).await {
                Ok(handle) => {
                    let is_leader = handle.is_leader().await.unwrap_or(false);
                    let leader = handle.leader_id().await.ok().flatten();
                    (is_leader, leader)
                }
                Err(_) => (false, None),
            }
        } else {
            let state = self.multi_raft.read().await.group_state(group_id);
            let is_leader =
                state.as_ref().is_some_and(|s| s.state == RaftState::Leader);
            let leader = state.and_then(|s| s.leader_id);
            (is_leader, leader)
        }
    }

    /// Appends a blob (Kafka `RecordBatch`) to a partition through Raft.
    ///
    /// Returns the base offset assigned to this batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic/partition doesn't exist or this node is not leader.
    #[allow(clippy::significant_drop_tightening, clippy::too_many_lines)]
    pub async fn append_blob(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
    ) -> ServerResult<u64> {
        // Get topic metadata.
        let topic_meta = self
            .get_topic(topic)
            .await
            .ok_or_else(|| ServerError::TopicNotFound {
                topic: topic.to_string(),
            })?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            });
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                }
            })?
        };

        // Only the leader handles writes.
        let (is_leader, leader_hint) = self.check_leadership(group_id).await;
        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: topic.to_string(),
                partition,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // In multi-node mode, we need to wait for the commit to happen asynchronously.
        // In single-node mode, the commit happens synchronously.
        let is_multi_node = self.is_multi_node_generic();

        // Capture base_offset BEFORE proposing to ensure consistency across all replicas.
        // The offset is encoded in the command and used by all replicas during apply.
        let base_offset = {
            let ps_lock = {
                let storage = self.partition_storage.read().await;
                storage
                    .get(&group_id)
                    .cloned()
                    .ok_or_else(|| ServerError::PartitionNotFound {
                        topic: topic.to_string(),
                        partition,
                    })?
            };
            let ps = ps_lock.read().await;
            ps.blob_log_end_offset()
        };

        // Encode blob command with the leader-assigned base_offset.
        // Use KafkaRecordBatch format since this API is for Kafka protocol.
        let command = PartitionCommand::AppendBlob {
            blob: data,
            record_count,
            format: BlobFormat::KafkaRecordBatch,
            base_offset,
        };
        let (cmd_meta, cmd_payload) = command.encode_split();

        if is_multi_node {
            // Multi-node: propose, register pending proposal, wait for commit notification.
            // The tick task applies the entry and notifies us via the oneshot channel.
            let (result_tx, result_rx) = oneshot::channel();

            // Propose to Raft, flush pending messages, and get the log index.
            // IMPORTANT: We flush immediately after propose to avoid waiting for the
            // next tick (50ms). This reduces latency from ~50ms to ~RTT.
            let (proposed_index, messages_to_send) = {
                let mut mr = self.multi_raft.write().await;

                let (outputs, idx) =
                    mr.propose_with_index_split(group_id, cmd_meta, cmd_payload)
                        .ok_or_else(|| ServerError::NotLeader {
                            topic: topic.to_string(),
                            partition,
                            leader_hint: None,
                        })?;

                // Flush any pending outbound messages (including the AppendEntries
                // generated by this propose).
                let flush_outputs = mr.flush();

                // Combine outputs for sending.
                let all_outputs: Vec<_> = outputs.into_iter().chain(flush_outputs).collect();

                (idx, all_outputs)
            };

            // Send Raft messages immediately (outside the lock).
            self.send_raft_messages(&messages_to_send).await;

            // Register the pending proposal so the tick task can notify us.
            {
                let inner_lock = {
                    let mut proposals = self.pending_proposals.write().await;
                    proposals
                        .entry(group_id)
                        .or_insert_with(|| {
                            std::sync::Arc::new(tokio::sync::RwLock::new(
                                std::collections::HashMap::new(),
                            ))
                        })
                        .clone()
                };
                let mut inner = inner_lock.write().await;
                inner.insert(
                    proposed_index,
                    PendingProposal {
                        log_index: proposed_index,
                        result_tx,
                    },
                );
            }

            // Wait for commit with timeout.
            let offset = tokio::time::timeout(std::time::Duration::from_secs(30), result_rx)
                .await
                .map_err(|_| ServerError::Internal {
                    message: "timeout waiting for commit".to_string(),
                })?
                .map_err(|_| ServerError::Internal {
                    message: "commit notification channel closed".to_string(),
                })??;

            debug!(
                topic = %topic,
                partition,
                base_offset = offset.get(),
                record_count,
                "Appended blob"
            );

            Ok(offset.get())
        } else {
            // Single-node: commit happens synchronously.
            // base_offset already captured above (used in the command).

            let outputs = {
                let mut mr = self.multi_raft.write().await;
                mr.propose_split(group_id, cmd_meta, cmd_payload)
            };

            info!(
                topic = %topic,
                partition,
                group_id = group_id.get(),
                has_outputs = outputs.is_some(),
                output_count = outputs.as_ref().map_or(0, std::vec::Vec::len),
                "Single-node propose result"
            );

            // Apply any committed entries (single-node: immediate commit).
            if let Some(outputs) = outputs {
                for output in &outputs {
                    if let MultiRaftOutput::CommitEntry {
                        group_id: gid,
                        index,
                        term,
                        metadata,
                        payload,
                    } = output
                    {
                        if *gid == group_id {
                            let ps_lock = {
                                let storage = self.partition_storage.read().await;
                                storage.get(&group_id).cloned()
                            };
                            if let Some(ps_lock) = ps_lock {
                                let mut ps = ps_lock.write().await;
                                ps.apply_entry_async(*index, *term, metadata, payload)
                                    .await
                                    .map_err(|e| ServerError::Internal {
                                        message: format!("failed to apply: {e}"),
                                    })?;
                                let new_end = ps.blob_log_end_offset();
                                info!(
                                    group_id = group_id.get(),
                                    new_blob_log_end = new_end.get(),
                                    "Applied entry to storage"
                                );
                            } else {
                                warn!(group_id = group_id.get(), "Storage not found for group!");
                            }
                        }
                    }
                }
            }

            info!(
                topic = %topic,
                partition,
                base_offset = base_offset.get(),
                record_count,
                "Appended blob"
            );

            Ok(base_offset.get())
        }
    }

    /// Appends a blob with idempotent producer support.
    ///
    /// Idempotent sequence checks happen inside the partition actor (after
    /// the `offset_seeded` gate) to avoid stale state on leader changes.
    /// Producer state is recorded by the output processor on commit via
    /// `extract_and_record_producer_state`.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `partition` - Partition index
    /// * `record_count` - Number of records in the batch
    /// * `data` - Raw `RecordBatch` bytes
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Topic/partition doesn't exist
    /// - This node is not the leader
    /// - Producer is fenced (stale epoch)
    /// - Sequence is out of order
    #[allow(clippy::significant_drop_tightening, clippy::too_many_lines)]
    #[tracing::instrument(skip_all, name = "append_blob_idempotent", fields(topic = %topic, partition))]
    pub async fn append_blob_idempotent(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
    ) -> ServerResult<u64> {
        // Get topic metadata.
        let topic_meta = self
            .get_topic(topic)
            .await
            .ok_or_else(|| ServerError::TopicNotFound {
                topic: topic.to_string(),
            })?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            });
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                }
            })?
        };

        // Only the leader handles writes.
        let (is_leader, leader_hint) = self.check_leadership(group_id).await;
        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: topic.to_string(),
                partition,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // In multi-node mode, we need to wait for the commit to happen asynchronously.
        // In single-node mode, the commit happens synchronously.
        let is_multi_node = self.is_multi_node_generic();

        if is_multi_node {
            // Multi-node: route through batcher for improved throughput.
            if let Some(ref batcher) = self.batcher_handle {
                // Submit to batcher instead of proposing directly.
                let result_rx = batcher
                    .submit(group_id, data, record_count, BlobFormat::KafkaRecordBatch)
                    .instrument(tracing::info_span!("batcher_submit"))
                    .await?;

                // Wait for commit with timeout.
                let offset = tokio::time::timeout(std::time::Duration::from_secs(30), result_rx)
                    .instrument(tracing::info_span!("wait_commit"))
                    .await
                    .map_err(|_| ServerError::Internal {
                        message: "timeout waiting for batch commit".to_string(),
                    })?
                    .map_err(|_| ServerError::Internal {
                        message: "batch commit notification channel closed".to_string(),
                    })??;

                info!(
                    topic = %topic,
                    partition,
                    group_id = group_id.get(),
                    received_offset = offset.get(),
                    record_count,
                    "BLOB_HANDLER: received offset from commit notification"
                );

                debug!(
                    topic = %topic,
                    partition,
                    base_offset = offset.get(),
                    record_count,
                    "Appended blob via batcher"
                );

                return Ok(offset.get());
            }

            // Fallback: direct propose (if batcher not configured).
            // Capture base_offset BEFORE proposing to ensure consistency across all replicas.
            let base_offset = {
                let ps_lock = {
                    let storage = self.partition_storage.read().await;
                    storage.get(&group_id).cloned().ok_or_else(|| {
                        ServerError::PartitionNotFound {
                            topic: topic.to_string(),
                            partition,
                        }
                    })?
                };
                let ps = ps_lock.read().await;
                ps.blob_log_end_offset()
            };

            // Encode blob command with the leader-assigned base_offset.
            let command = PartitionCommand::AppendBlob {
                blob: data.clone(),
                record_count,
                format: BlobFormat::KafkaRecordBatch,
                base_offset,
            };
            let (cmd_meta, cmd_payload) = command.encode_split();

            let (result_tx, result_rx) = oneshot::channel();

            // Propose to Raft, flush pending messages, and get the log index.
            // IMPORTANT: We flush immediately after propose to avoid waiting for the
            // next tick (50ms). This reduces latency from ~50ms to ~RTT.
            let (proposed_index, messages_to_send) = {
                let mut mr = self.multi_raft.write().await;
                let (outputs, idx) =
                    mr.propose_with_index_split(group_id, cmd_meta, cmd_payload)
                        .ok_or_else(|| ServerError::NotLeader {
                            topic: topic.to_string(),
                            partition,
                            leader_hint: None,
                        })?;

                // Flush any pending outbound messages (including the AppendEntries
                // generated by this propose).
                let flush_outputs = mr.flush();

                // Combine outputs for sending.
                let all_outputs: Vec<_> = outputs.into_iter().chain(flush_outputs).collect();
                (idx, all_outputs)
            };

            // Send Raft messages immediately (outside the lock).
            self.send_raft_messages(&messages_to_send).await;

            // Register the pending proposal so the tick task can notify us.
            {
                let inner_lock = {
                    let mut proposals = self.pending_proposals.write().await;
                    proposals
                        .entry(group_id)
                        .or_insert_with(|| {
                            std::sync::Arc::new(tokio::sync::RwLock::new(
                                std::collections::HashMap::new(),
                            ))
                        })
                        .clone()
                };
                let mut inner = inner_lock.write().await;
                inner.insert(
                    proposed_index,
                    PendingProposal {
                        log_index: proposed_index,
                        result_tx,
                    },
                );
            }

            // Wait for commit with timeout.
            let offset = tokio::time::timeout(std::time::Duration::from_secs(30), result_rx)
                .await
                .map_err(|_| ServerError::Internal {
                    message: "timeout waiting for commit".to_string(),
                })?
                .map_err(|_| ServerError::Internal {
                    message: "commit notification channel closed".to_string(),
                })??;

            debug!(
                topic = %topic,
                partition,
                base_offset = offset.get(),
                record_count,
                "Appended blob (idempotent)"
            );

            Ok(offset.get())
        } else {
            // Single-node: commit happens synchronously.
            // Capture base_offset BEFORE proposing to ensure consistency.
            let base_offset = {
                let ps_lock = {
                    let storage = self.partition_storage.read().await;
                    storage.get(&group_id).cloned().ok_or_else(|| {
                        ServerError::PartitionNotFound {
                            topic: topic.to_string(),
                            partition,
                        }
                    })?
                };
                let ps = ps_lock.read().await;
                ps.blob_log_end_offset()
            };

            // Encode blob command with the leader-assigned base_offset.
            let command = PartitionCommand::AppendBlob {
                blob: data,
                record_count,
                format: BlobFormat::KafkaRecordBatch,
                base_offset,
            };
            let (cmd_meta, cmd_payload) = command.encode_split();

            let outputs = {
                let mut mr = self.multi_raft.write().await;
                mr.propose_split(group_id, cmd_meta, cmd_payload)
            };

            // Apply any committed entries (single-node: immediate commit).
            if let Some(outputs) = outputs {
                for output in &outputs {
                    if let MultiRaftOutput::CommitEntry {
                        group_id: gid,
                        index,
                        term,
                        metadata,
                        payload,
                    } = output
                    {
                        if *gid == group_id {
                            let ps_lock = {
                                let storage = self.partition_storage.read().await;
                                storage.get(&group_id).cloned()
                            };
                            if let Some(ps_lock) = ps_lock {
                                let mut ps = ps_lock.write().await;
                                ps.apply_entry_async(*index, *term, metadata, payload)
                                    .await
                                    .map_err(|e| ServerError::Internal {
                                        message: format!("failed to apply: {e}"),
                                    })?;
                            }
                        }
                    }
                }
            }

            debug!(
                topic = %topic,
                partition,
                base_offset = base_offset.get(),
                record_count,
                "Appended blob (single-node)"
            );

            Ok(base_offset.get())
        }
    }

    /// Reads blobs (Kafka `RecordBatches`) from a partition.
    ///
    /// Returns raw blob data that can be sent directly to Kafka clients.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic/partition doesn't exist or this node is not leader.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn read_blobs(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_bytes: u32,
    ) -> ServerResult<Vec<Bytes>> {
        // Get topic metadata.
        let topic_meta = self
            .get_topic(topic)
            .await
            .ok_or_else(|| ServerError::TopicNotFound {
                topic: topic.to_string(),
            })?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            });
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                }
            })?
        };

        // Only the leader serves reads (Kafka semantics). Followers may have
        // stale state if their output processor hasn't applied all committed
        // entries yet. Returning NotLeader forces the client to find the leader,
        // which always has the latest applied state.
        let (is_leader, leader_hint) = self.check_leadership(group_id).await;
        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: topic.to_string(),
                partition,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // Read blobs from storage.
        let ps_lock = {
            let storage = self.partition_storage.read().await;
            storage
                .get(&group_id)
                .cloned()
                .ok_or_else(|| ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?
        };
        let ps = ps_lock.read().await;

        let storage_end_offset = ps.blob_log_end_offset();
        info!(
            topic = %topic,
            partition,
            group_id = group_id.get(),
            start_offset,
            storage_end_offset = storage_end_offset.get(),
            "read_blobs: looking up storage"
        );

        let blobs: Vec<Bytes> = match &ps.inner {
            PartitionStorageInner::InMemory(p) => p
                .read_blobs(Offset::new(start_offset), max_bytes)
                .into_iter()
                .map(|b| b.data)
                .collect(),
            PartitionStorageInner::Durable(p) => p
                .read_blobs(Offset::new(start_offset), max_bytes)
                .await
                .map_err(|e| ServerError::Internal {
                    message: e.to_string(),
                })?
                .into_iter()
                .map(|b| b.data)
                .collect(),
        };

        // Log result.
        info!(
            topic = %topic,
            partition,
            group_id = group_id.get(),
            start_offset,
            blobs_count = blobs.len(),
            "read_blobs: returning"
        );

        Ok(blobs)
    }

    /// Gets the blob log end offset for a partition.
    ///
    /// This is the high watermark for blob storage (Kafka `RecordBatches`).
    #[allow(clippy::significant_drop_tightening)]
    pub async fn blob_log_end_offset(&self, topic: &str, partition: i32) -> Option<u64> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return None;
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id)?
        };

        // Get blob log end offset from storage.
        let ps_lock = {
            let storage = self.partition_storage.read().await;
            storage.get(&group_id).cloned()?
        };
        let ps = ps_lock.read().await;

        let offset = match &ps.inner {
            PartitionStorageInner::InMemory(p) => p.blob_log_end_offset(),
            PartitionStorageInner::Durable(p) => p.blob_log_end_offset(),
        };

        Some(offset.get())
    }

    /// Checks if a partition exists with blob storage.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn blob_partition_exists(&self, topic: &str, partition: i32) -> bool {
        // Get topic metadata.
        let Some(topic_meta) = self.get_topic(topic).await else {
            return false;
        };

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return false;
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            match gm.get(topic_meta.topic_id, partition_id) {
                Some(gid) => gid,
                None => return false,
            }
        };

        // Check storage.
        let ps_lock = {
            let storage = self.partition_storage.read().await;
            let Some(ps_lock) = storage.get(&group_id).cloned() else {
                return false;
            };
            ps_lock
        };
        let ps = ps_lock.read().await;

        match &ps.inner {
            // Check if any blobs exist (offset 0 always valid if partition has data).
            PartitionStorageInner::InMemory(p) => p.blob_partition_exists(Offset::new(0)),
            PartitionStorageInner::Durable(p) => p.blob_partition_exists(),
        }
    }

    /// Gets the blob high watermark (committed offset) for a partition.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn blob_high_watermark(&self, topic: &str, partition: i32) -> Option<u64> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await?;

        // Validate partition index.
        if partition < 0 || partition >= topic_meta.partition_count {
            return None;
        }

        // Safe cast: partition is validated to be non-negative.
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);

        // Get group ID.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id)?
        };

        // Get high watermark from storage.
        let ps_lock = {
            let storage = self.partition_storage.read().await;
            storage.get(&group_id).cloned()?
        };
        let ps = ps_lock.read().await;

        Some(ps.high_watermark().get())
    }

    /// Returns the log start offset for a partition (earliest available).
    ///
    /// After snapshot-only recovery from a remote node, the log start offset
    /// equals the high watermark: no historical data is available locally.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn get_log_start_offset(&self, topic: &str, partition: i32) -> u64 {
        let Some(topic_meta) = self.get_topic(topic).await else {
            return 0;
        };
        if partition < 0 || partition >= topic_meta.partition_count {
            return 0;
        }
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition as u64);
        let group_id = {
            let gm = self.group_map.read().await;
            match gm.get(topic_meta.topic_id, partition_id) {
                Some(g) => g,
                None => return 0,
            }
        };
        let ps_lock = {
            let storage = self.partition_storage.read().await;
            match storage.get(&group_id).cloned() {
                Some(ps) => ps,
                None => return 0,
            }
        };
        let ps = ps_lock.read().await;
        ps.log_start_offset().get()
    }
}

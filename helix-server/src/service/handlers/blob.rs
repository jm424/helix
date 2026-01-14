//! Blob storage handlers for the Helix service (Kafka zero-copy support).

use bytes::Bytes;
use helix_core::{NodeId, Offset, PartitionId, ProducerEpoch, ProducerId, SequenceNum};
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::error::{ServerError, ServerResult};
use crate::partition_storage::PartitionStorageInner;
use crate::producer_state::SequenceCheckResult;
use crate::storage::{BlobFormat, PartitionCommand};

use super::super::{HelixService, PendingProposal};

/// Producer info for idempotent produce requests.
#[derive(Debug, Clone, Copy)]
pub struct IdempotentProducerInfo {
    /// Producer ID (must be >= 0 for idempotent).
    pub producer_id: ProducerId,
    /// Producer epoch.
    pub epoch: ProducerEpoch,
    /// Base sequence number for this batch.
    pub base_sequence: SequenceNum,
}

impl HelixService {
    /// Appends a blob (Kafka `RecordBatch`) to a partition through Raft.
    ///
    /// Returns the base offset assigned to this batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic/partition doesn't exist or this node is not leader.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn append_blob(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
    ) -> ServerResult<u64> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await.ok_or_else(|| ServerError::TopicNotFound {
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
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?
        };

        // Check if we're the leader.
        let (is_leader, leader_hint) = {
            let mr = self.multi_raft.read().await;
            let state = mr.group_state(group_id);
            let is_leader = state.as_ref().is_some_and(|s| s.state == RaftState::Leader);
            let leader = state.and_then(|s| s.leader_id);
            (is_leader, leader)
        };

        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: topic.to_string(),
                partition,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // Encode blob command and propose to Raft.
        // Use KafkaRecordBatch format since this API is for Kafka protocol.
        let command = PartitionCommand::AppendBlob {
            blob: data,
            record_count,
            format: BlobFormat::KafkaRecordBatch,
        };
        let command_data = command.encode();

        // In multi-node mode, we need to wait for the commit to happen asynchronously.
        // In single-node mode, the commit happens synchronously.
        let is_multi_node = self.is_multi_node();

        if is_multi_node {
            // Multi-node: propose, register pending proposal, wait for commit notification.
            // The tick task applies the entry and notifies us via the oneshot channel.
            let (result_tx, result_rx) = oneshot::channel();

            // Propose to Raft and get the log index.
            let (_, proposed_index) = {
                let mut mr = self.multi_raft.write().await;
                mr.propose_with_index(group_id, command_data).ok_or_else(|| {
                    ServerError::NotLeader {
                        topic: topic.to_string(),
                        partition,
                        leader_hint: None,
                    }
                })?
            };

            // Register the pending proposal so the tick task can notify us.
            {
                eprintln!("[PROPOSE] topic={} partition={} group_id={} proposed_index={}",
                    topic, partition, group_id, proposed_index);
                let mut proposals = self.pending_proposals.write().await;
                proposals
                    .entry(group_id)
                    .or_insert_with(Vec::new)
                    .push(PendingProposal {
                        log_index: proposed_index,
                        result_tx,
                    });
            }

            // Wait for commit with timeout.
            let offset = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                result_rx,
            )
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
            // Get base offset before proposing.
            let base_offset = {
                let storage = self.partition_storage.read().await;
                let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;
                ps.blob_log_end_offset()
            };

            let outputs = {
                let mut mr = self.multi_raft.write().await;
                mr.propose(group_id, command_data)
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
                        data: entry_data,
                    } = output
                    {
                        info!(
                            commit_group_id = gid.get(),
                            expected_group_id = group_id.get(),
                            index = index.get(),
                            data_len = entry_data.len(),
                            "Processing CommitEntry"
                        );
                        if *gid == group_id {
                            let mut storage = self.partition_storage.write().await;
                            if let Some(ps) = storage.get_mut(&group_id) {
                                ps.apply_entry_async(*index, entry_data)
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
    /// This method checks for duplicate sequences before proposing to Raft,
    /// preventing duplicate log entries. If a duplicate is detected, the
    /// cached offset is returned without re-proposing.
    ///
    /// # Arguments
    ///
    /// * `topic` - Topic name
    /// * `partition` - Partition index
    /// * `record_count` - Number of records in the batch
    /// * `data` - Raw `RecordBatch` bytes
    /// * `producer_info` - Optional producer info for idempotent deduplication
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Topic/partition doesn't exist
    /// - This node is not the leader
    /// - Producer is fenced (stale epoch)
    /// - Sequence is out of order
    #[allow(clippy::significant_drop_tightening, clippy::too_many_lines)]
    pub async fn append_blob_idempotent(
        &self,
        topic: &str,
        partition: i32,
        record_count: u32,
        data: Bytes,
        producer_info: Option<IdempotentProducerInfo>,
    ) -> ServerResult<u64> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await.ok_or_else(|| ServerError::TopicNotFound {
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
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?
        };

        // Check if we're the leader.
        let (is_leader, leader_hint) = {
            let mr = self.multi_raft.read().await;
            let state = mr.group_state(group_id);
            let is_leader = state.as_ref().is_some_and(|s| s.state == RaftState::Leader);
            let leader = state.and_then(|s| s.leader_id);
            (is_leader, leader)
        };

        if !is_leader {
            return Err(ServerError::NotLeader {
                topic: topic.to_string(),
                partition,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // Check producer sequence for idempotent deduplication BEFORE proposing.
        if let Some(ref info) = producer_info {
            let storage = self.partition_storage.read().await;
            if let Some(ps) = storage.get(&group_id) {
                match ps.check_producer_sequence(info.producer_id, info.epoch, info.base_sequence) {
                    SequenceCheckResult::Valid => {
                        // Continue with the proposal.
                        debug!(
                            topic = %topic,
                            partition,
                            producer_id = info.producer_id.get(),
                            sequence = info.base_sequence.get(),
                            "Idempotent sequence check passed"
                        );
                    }
                    SequenceCheckResult::Duplicate { cached_offset } => {
                        // Return cached offset without re-proposing.
                        info!(
                            topic = %topic,
                            partition,
                            producer_id = info.producer_id.get(),
                            sequence = info.base_sequence.get(),
                            cached_offset = cached_offset.get(),
                            "Duplicate produce detected, returning cached offset"
                        );
                        return Ok(cached_offset.get());
                    }
                    SequenceCheckResult::OutOfSequence { expected, received } => {
                        warn!(
                            topic = %topic,
                            partition,
                            producer_id = info.producer_id.get(),
                            expected,
                            received,
                            "Out of sequence produce rejected"
                        );
                        return Err(ServerError::OutOfOrderSequence {
                            topic: topic.to_string(),
                            partition,
                            expected,
                            received,
                        });
                    }
                    SequenceCheckResult::ProducerFenced { current_epoch } => {
                        warn!(
                            topic = %topic,
                            partition,
                            producer_id = info.producer_id.get(),
                            stale_epoch = info.epoch.get(),
                            current_epoch = current_epoch.get(),
                            "Producer fenced"
                        );
                        return Err(ServerError::ProducerFenced {
                            topic: topic.to_string(),
                            partition,
                            producer_id: info.producer_id.get(),
                        });
                    }
                }
            }
        }

        // Encode blob command and propose to Raft.
        // Use KafkaRecordBatch format since this API is for Kafka protocol.
        let command = PartitionCommand::AppendBlob {
            blob: data,
            record_count,
            format: BlobFormat::KafkaRecordBatch,
        };
        let command_data = command.encode();

        // In multi-node mode, we need to wait for the commit to happen asynchronously.
        // In single-node mode, the commit happens synchronously.
        let is_multi_node = self.is_multi_node();

        if is_multi_node {
            // Multi-node: propose, register pending proposal, wait for commit notification.
            let (result_tx, result_rx) = oneshot::channel();

            // Propose to Raft and get the log index.
            let (_, proposed_index) = {
                let mut mr = self.multi_raft.write().await;
                mr.propose_with_index(group_id, command_data).ok_or_else(|| {
                    ServerError::NotLeader {
                        topic: topic.to_string(),
                        partition,
                        leader_hint: None,
                    }
                })?
            };

            // Register the pending proposal so the tick task can notify us.
            {
                eprintln!("[PROPOSE_IDEMP] topic={topic} partition={partition} group_id={group_id} proposed_index={proposed_index}");
                let mut proposals = self.pending_proposals.write().await;
                proposals
                    .entry(group_id)
                    .or_insert_with(Vec::new)
                    .push(PendingProposal {
                        log_index: proposed_index,
                        result_tx,
                    });
            }

            // Wait for commit with timeout.
            let offset = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                result_rx,
            )
            .await
            .map_err(|_| ServerError::Internal {
                message: "timeout waiting for commit".to_string(),
            })?
            .map_err(|_| ServerError::Internal {
                message: "commit notification channel closed".to_string(),
            })??;

            // Record producer sequence after successful commit.
            if let Some(ref info) = producer_info {
                let mut storage = self.partition_storage.write().await;
                if let Some(ps) = storage.get_mut(&group_id) {
                    ps.record_producer_sequence(
                        info.producer_id,
                        info.epoch,
                        info.base_sequence,
                        offset,
                    );
                }
            }

            debug!(
                topic = %topic,
                partition,
                base_offset = offset.get(),
                record_count,
                idempotent = producer_info.is_some(),
                "Appended blob (idempotent)"
            );

            Ok(offset.get())
        } else {
            // Single-node: commit happens synchronously.
            let base_offset = {
                let storage = self.partition_storage.read().await;
                let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })?;
                ps.blob_log_end_offset()
            };

            let outputs = {
                let mut mr = self.multi_raft.write().await;
                mr.propose(group_id, command_data)
            };

            // Apply any committed entries (single-node: immediate commit).
            if let Some(outputs) = outputs {
                for output in &outputs {
                    if let MultiRaftOutput::CommitEntry {
                        group_id: gid,
                        index,
                        data: entry_data,
                    } = output
                    {
                        if *gid == group_id {
                            let mut storage = self.partition_storage.write().await;
                            if let Some(ps) = storage.get_mut(&group_id) {
                                ps.apply_entry_async(*index, entry_data)
                                    .await
                                    .map_err(|e| ServerError::Internal {
                                        message: format!("failed to apply: {e}"),
                                    })?;

                                // Record producer sequence after successful commit.
                                if let Some(ref info) = producer_info {
                                    ps.record_producer_sequence(
                                        info.producer_id,
                                        info.epoch,
                                        info.base_sequence,
                                        base_offset,
                                    );
                                }
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
                idempotent = producer_info.is_some(),
                "Appended blob (idempotent)"
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
    /// Returns an error if the topic/partition doesn't exist.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn read_blobs(
        &self,
        topic: &str,
        partition: i32,
        start_offset: u64,
        max_bytes: u32,
    ) -> ServerResult<Vec<Bytes>> {
        // Get topic metadata.
        let topic_meta = self.get_topic(topic).await.ok_or_else(|| ServerError::TopicNotFound {
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
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| ServerError::PartitionNotFound {
                topic: topic.to_string(),
                partition,
            })?
        };

        // Read blobs from storage.
        let storage = self.partition_storage.read().await;
        let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
            topic: topic.to_string(),
            partition,
        })?;

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
            PartitionStorageInner::InMemory(p) => {
                p.read_blobs(Offset::new(start_offset), max_bytes)
                    .into_iter()
                    .map(|b| b.data)
                    .collect()
            }
            PartitionStorageInner::Durable(p) => {
                p.read_blobs(Offset::new(start_offset), max_bytes)
                    .into_iter()
                    .map(|b| b.data)
                    .collect()
            }
        };

        // Log result.
        if blobs.is_empty() {
            info!(
                topic = %topic,
                partition,
                group_id = group_id.get(),
                start_offset,
                "read_blobs: returning EMPTY"
            );
        } else {
            info!(
                topic = %topic,
                partition,
                group_id = group_id.get(),
                blobs_count = blobs.len(),
                "read_blobs: returning blobs"
            );
            // Debug: log the baseOffset of each blob being returned.
            for (i, blob) in blobs.iter().enumerate() {
                if blob.len() >= 8 {
                    let base_offset = i64::from_be_bytes([
                        blob[0], blob[1], blob[2], blob[3], blob[4], blob[5], blob[6], blob[7],
                    ]);
                    info!(
                        topic = %topic,
                        partition,
                        blob_index = i,
                        blob_len = blob.len(),
                        base_offset,
                        "Returning blob"
                    );
                }
            }
        }

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
        let storage = self.partition_storage.read().await;
        let ps = storage.get(&group_id)?;

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
        let storage = self.partition_storage.read().await;
        let Some(ps) = storage.get(&group_id) else {
            return false;
        };

        match &ps.inner {
            // Check if any blobs exist (offset 0 always valid if partition has data).
            PartitionStorageInner::InMemory(p) => p.blob_partition_exists(Offset::new(0)),
            PartitionStorageInner::Durable(p) => p.blob_partition_exists(),
        }
    }
}

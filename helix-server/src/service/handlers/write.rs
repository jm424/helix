//! Write handlers for the Helix service.

use bytes::Bytes;
use helix_core::{NodeId, PartitionId, Record};
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use tokio::sync::oneshot;
use tracing::debug;

use crate::error::{ServerError, ServerResult};
use crate::generated::{ErrorCode, Record as ProtoRecord, WriteRequest, WriteResponse};
use crate::storage::PartitionCommand;

use super::super::{HelixService, PendingProposal, MAX_RECORDS_PER_WRITE};

impl HelixService {
    /// Converts a proto Record to a core Record.
    #[allow(clippy::option_if_let_else)]
    pub(crate) fn proto_to_record(proto: &ProtoRecord) -> Record {
        let mut record = if let Some(key) = &proto.key {
            Record::with_key(
                Bytes::from(key.clone()),
                Bytes::from(proto.value.clone()),
            )
        } else {
            Record::new(Bytes::from(proto.value.clone()))
        };

        if let Some(ts) = proto.timestamp_ms {
            record = record.with_timestamp(helix_core::Timestamp::from_millis(ts));
        }
        for (k, v) in &proto.headers {
            record = record.with_header(k.clone(), Bytes::from(v.clone()));
        }
        record
    }

    /// Internal write implementation using Multi-Raft.
    ///
    /// Uses the `PendingProposal` pattern: propose to Raft, register a pending
    /// proposal, and wait for the tick task to apply and notify us with the
    /// resulting offset. This ensures entries are applied in Raft log order
    /// regardless of concurrent write timing.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn write_internal(&self, request: WriteRequest) -> ServerResult<WriteResponse> {
        // Validate request.
        assert!(!request.topic.is_empty(), "topic cannot be empty");
        assert!(
            request.records.len() <= MAX_RECORDS_PER_WRITE,
            "too many records: {} > {}",
            request.records.len(),
            MAX_RECORDS_PER_WRITE
        );

        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        let partition_idx = if request.partition < 0 {
            0 // Auto-partition: use round-robin or hash-based assignment.
        } else {
            request.partition
        };

        if partition_idx >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            });
        }

        // Safe cast: partition_idx is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(partition_idx as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: partition_idx,
                }
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
                topic: request.topic.clone(),
                partition: partition_idx,
                leader_hint: leader_hint.map(NodeId::get),
            });
        }

        // Convert proto records to core records.
        let records: Vec<Record> = request.records.iter().map(Self::proto_to_record).collect();
        let record_count = records.len();

        // Encode command.
        let command = PartitionCommand::Append { records };
        let data = command.encode();

        // Create channel for receiving the apply result.
        let (result_tx, result_rx) = oneshot::channel();

        // Propose to Raft and get the log index.
        let (outputs, proposed_index) = {
            let mut mr = self.multi_raft.write().await;
            mr.propose_with_index(group_id, data).ok_or_else(|| {
                ServerError::NotLeader {
                    topic: request.topic.clone(),
                    partition: partition_idx,
                    leader_hint: None,
                }
            })?
        };

        // In single-node mode, the commit happens synchronously and the CommitEntry
        // is returned in outputs. We can apply immediately IF our entry is next in
        // line (index == last_applied + 1). If there's a gap (another concurrent
        // write's entry needs to be applied first), we wait for the tick task.
        //
        // In multi-node mode, outputs won't contain CommitEntry (needs replication),
        // so we always register a pending proposal and wait for the tick task.
        let mut immediate_offset = None;
        for output in &outputs {
            if let MultiRaftOutput::CommitEntry {
                group_id: gid,
                index,
                data: entry_data,
            } = output
            {
                if *gid == group_id && *index == proposed_index {
                    let mut storage = self.partition_storage.write().await;
                    if let Some(ps) = storage.get_mut(&group_id) {
                        // Only apply if we're next in line (no gap).
                        // This ensures entries are applied in order even with concurrent writes.
                        let last_applied = ps.last_applied();
                        let expected_next = helix_core::LogIndex::new(last_applied.get() + 1);

                        if *index == expected_next {
                            match ps.apply_entry_async(*index, entry_data).await {
                                Ok(Some(offset)) => immediate_offset = Some(offset),
                                Ok(None) => immediate_offset = Some(ps.log_end_offset()),
                                Err(e) => {
                                    return Err(ServerError::Internal {
                                        message: format!("failed to apply: {e}"),
                                    });
                                }
                            }
                        }
                        // If index > expected_next, there's a gap - another write's entry
                        // needs to be applied first. Fall through to wait for tick task.
                    }
                }
            }
        }

        // If we got an immediate offset, we're done (single-node fast path).
        // Otherwise, register pending proposal and wait for tick task.
        let base_offset = if let Some(offset) = immediate_offset {
            offset
        } else {
            // Register the pending proposal so the tick task can notify us.
            {
                let mut proposals = self.pending_proposals.write().await;
                proposals
                    .entry(group_id)
                    .or_insert_with(Vec::new)
                    .push(PendingProposal {
                        log_index: proposed_index,
                        result_tx,
                    });
            }

            // Wait for commit and apply with timeout.
            tokio::time::timeout(std::time::Duration::from_secs(30), result_rx)
                .await
                .map_err(|_| ServerError::Internal {
                    message: "timeout waiting for commit".to_string(),
                })?
                .map_err(|_| ServerError::Internal {
                    message: "commit notification channel closed".to_string(),
                })??
        };

        debug!(
            topic = %request.topic,
            partition = partition_idx,
            base_offset = base_offset.get(),
            count = record_count,
            "Wrote records"
        );

        // Safe cast: record_count is bounded by MAX_RECORDS_PER_WRITE which fits in u32.
        #[allow(clippy::cast_possible_truncation)]
        let record_count_u32 = record_count as u32;

        Ok(WriteResponse {
            base_offset: base_offset.get(),
            record_count: record_count_u32,
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }
}

//! Write handlers for the Helix service.

use bytes::Bytes;
use helix_core::{NodeId, PartitionId, Record};
use helix_raft::multi::MultiRaftOutput;
use helix_raft::RaftState;
use tracing::debug;

use crate::error::{ServerError, ServerResult};
use crate::generated::{ErrorCode, Record as ProtoRecord, WriteRequest, WriteResponse};
use crate::storage::PartitionCommand;

use super::super::{HelixService, MAX_RECORDS_PER_WRITE};

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
    #[allow(clippy::significant_drop_tightening)]
    #[allow(clippy::too_many_lines)] // Complex flow with validation, Raft, and storage.
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

        // Convert proto records to core records.
        let records: Vec<Record> = request.records.iter().map(Self::proto_to_record).collect();

        let record_count = records.len();

        // Get base offset before proposing.
        let base_offset = {
            let storage = self.partition_storage.read().await;
            let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: partition_idx,
            })?;
            ps.log_end_offset()
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

        // Encode command and propose to Raft.
        let command = PartitionCommand::Append { records };
        let data = command.encode();

        let outputs = {
            let mut mr = self.multi_raft.write().await;
            mr.propose(group_id, data)
        };

        // Apply any committed entries (single-node: immediate commit).
        if let Some(outputs) = outputs {
            for output in &outputs {
                if let MultiRaftOutput::CommitEntry {
                    group_id: gid,
                    index,
                    data,
                } = output
                {
                    if *gid == group_id {
                        let mut storage = self.partition_storage.write().await;
                        if let Some(ps) = storage.get_mut(&group_id) {
                            ps.apply_entry_async(*index, data)
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

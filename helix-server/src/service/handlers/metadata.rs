//! Metadata handlers for the Helix service.

use helix_core::{NodeId, PartitionId};
use helix_raft::multi::MultiRaft;

use crate::error::{ServerError, ServerResult};
use crate::generated::{
    BrokerInfo, ErrorCode, GetMetadataRequest, GetMetadataResponse, GetPartitionInfoRequest,
    GetPartitionInfoResponse, PartitionInfo, TopicInfo,
};
use crate::group_map::GroupMap;

use super::super::{HelixService, TopicMetadata};

impl HelixService {
    /// Internal metadata implementation.
    pub(crate) async fn get_metadata_internal(
        &self,
        request: GetMetadataRequest,
    ) -> ServerResult<GetMetadataResponse> {
        let topics = self.topics.read().await;
        let multi_raft = self.multi_raft.read().await;
        let group_map = self.group_map.read().await;

        // Filter topics if specific ones requested.
        let topic_infos: Vec<TopicInfo> = if request.topics.is_empty() {
            // Return all topics.
            topics
                .iter()
                .map(|(name, meta)| self.make_topic_info(name, meta, &multi_raft, &group_map))
                .collect()
        } else {
            // Return only requested topics.
            request
                .topics
                .iter()
                .filter_map(|name| {
                    topics
                        .get(name)
                        .map(|meta| self.make_topic_info(name, meta, &multi_raft, &group_map))
                })
                .collect()
        };

        Ok(GetMetadataResponse {
            cluster_id: self.cluster_id.clone(),
            controller_id: self.node_id.get(),
            brokers: vec![BrokerInfo {
                node_id: self.node_id.get(),
                host: "localhost".to_string(),
                port: 9092,
            }],
            topics: topic_infos,
        })
    }

    /// Creates topic info from metadata.
    pub(crate) fn make_topic_info(
        &self,
        name: &str,
        meta: &TopicMetadata,
        multi_raft: &MultiRaft,
        group_map: &GroupMap,
    ) -> TopicInfo {
        let mut partitions = Vec::new();

        for i in 0..meta.partition_count {
            // Safe cast: i is in range [0, partition_count) and partition_count <= 256.
            #[allow(clippy::cast_sign_loss)]
            let partition_id = PartitionId::new(i as u64);

            let leader = group_map
                .get(meta.topic_id, partition_id)
                .and_then(|gid| multi_raft.group_state(gid))
                .and_then(|state| state.leader_id)
                .map_or_else(|| self.node_id.get(), NodeId::get);

            partitions.push(PartitionInfo {
                partition: i,
                leader,
                replicas: self.cluster_nodes.iter().map(|n| n.get()).collect(),
                isr: vec![leader], // In-sync replicas.
            });
        }

        TopicInfo {
            name: name.to_string(),
            topic_id: meta.topic_id.get(),
            partitions,
        }
    }

    /// Internal partition info implementation.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) async fn get_partition_info_internal(
        &self,
        request: GetPartitionInfoRequest,
    ) -> ServerResult<GetPartitionInfoResponse> {
        let topic_meta = self.get_topic(&request.topic).await.ok_or_else(|| {
            ServerError::TopicNotFound {
                topic: request.topic.clone(),
            }
        })?;

        if request.partition < 0 || request.partition >= topic_meta.partition_count {
            return Err(ServerError::PartitionNotFound {
                topic: request.topic.clone(),
                partition: request.partition,
            });
        }

        // Safe cast: request.partition is validated to be in range [0, partition_count).
        #[allow(clippy::cast_sign_loss)]
        let partition_id = PartitionId::new(request.partition as u64);

        // Get the group ID for this partition.
        let group_id = {
            let gm = self.group_map.read().await;
            gm.get(topic_meta.topic_id, partition_id).ok_or_else(|| {
                ServerError::PartitionNotFound {
                    topic: request.topic.clone(),
                    partition: request.partition,
                }
            })?
        };

        let multi_raft = self.multi_raft.read().await;
        let storage = self.partition_storage.read().await;

        let ps = storage.get(&group_id).ok_or_else(|| ServerError::PartitionNotFound {
            topic: request.topic.clone(),
            partition: request.partition,
        })?;

        let leader = multi_raft
            .group_state(group_id)
            .and_then(|state| state.leader_id)
            .map_or_else(|| self.node_id.get(), NodeId::get);

        Ok(GetPartitionInfoResponse {
            partition: Some(PartitionInfo {
                partition: request.partition,
                leader,
                replicas: self.cluster_nodes.iter().map(|n| n.get()).collect(),
                isr: vec![leader],
            }),
            log_start_offset: ps.log_start_offset().get(),
            log_end_offset: ps.log_end_offset().get(),
            high_watermark: ps.high_watermark().get(),
            error_code: ErrorCode::None.into(),
            error_message: None,
        })
    }
}

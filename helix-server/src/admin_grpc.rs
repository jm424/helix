//! Kafka admin gRPC service implementation (kafkaadmin.Resources).
//!
//! Implements the `kafkaadmin.Resources` gRPC trait for topic lifecycle
//! management (create, describe, delete topics).
//!
//! Write operations (create/delete topic) return UNAVAILABLE when this
//! node is not the controller leader. Callers retry with DNS round-robin.

use std::collections::HashMap;
use std::sync::Arc;

use helix_core::ConsumerGroupId;
use helix_progress::ProgressStore;
use helix_runtime::TransportHandle;
use helix_wal::TokioStorage;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::generated_admin::resources_server::Resources;
use crate::generated_admin::{
    CreateTopicRequest, CreateTopicResponse, DeleteConsumerGroupsRequest,
    DeleteConsumerGroupsResponse, DeleteTopicRequest, DeleteTopicResponse,
    DescribeTopicRequest, DescribeTopicResponse, DescribeTopicsRequest, DescribeTopicsResponse,
    GetEndOffsetsRequest, GetEndOffsetsResponse,
    GetHealthRequest, GetHealthResponse, GetTopicStateRequest, GetTopicStateResponse,
    GetTopicStatesRequest, GetTopicStatesResponse, ListTopicConfigsRequest,
    ListTopicConfigsResponse, ListTopicsRequest, ListTopicsResponse,
    ListUsableBrokerIdsRequest, ListUsableBrokerIdsResponse,
    TagTopicsRequest, TagTopicsResponse, TopicConfig, TopicDescription, TopicState,
    UpdateTopicConfigRequest, UpdateTopicConfigResponse,
};
use crate::service::HelixService;

/// Admin gRPC service wrapping `HelixService`.
pub struct AdminService {
    service: Arc<HelixService<TokioStorage, TransportHandle>>,
    /// Default retention in milliseconds (from `--retention-ms` flag).
    default_retention_ms: u64,
}

impl AdminService {
    /// Creates a new admin service.
    #[must_use]
    pub const fn new(
        service: Arc<HelixService<TokioStorage, TransportHandle>>,
        default_retention_ms: u64,
    ) -> Self {
        Self {
            service,
            default_retention_ms,
        }
    }
}

#[tonic::async_trait]
impl Resources for AdminService {
    async fn create_topic(
        &self,
        request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();
        let name = req.topic_name.clone();

        #[allow(clippy::cast_sign_loss)]
        let partition_count = req.partition_count as u32;
        #[allow(clippy::cast_sign_loss)]
        let mut replication_factor = req.replication_factor as u32;

        if replication_factor == 0 {
            #[allow(clippy::cast_possible_truncation)]
            {
                replication_factor = self.service.cluster_nodes().len().min(3) as u32;
            }
        }

        info!(topic = %name, partitions = partition_count, replication = replication_factor, "Admin CreateTopic");

        match self
            .service
            .create_topic_via_controller(name.clone(), partition_count, replication_factor)
            .await
        {
            Ok(()) => Ok(Response::new(CreateTopicResponse {})),
            Err(e) if e.message().contains("already exists") => {
                info!(topic = %name, "Topic already exists (idempotent create)");
                Ok(Response::new(CreateTopicResponse {}))
            }
            Err(e) if e.message().contains("not the controller") => {
                warn!(topic = %name, "Not controller leader, returning UNAVAILABLE");
                Err(Status::unavailable("not the controller leader, retry"))
            }
            Err(e) => {
                warn!(topic = %name, error = %e, "Admin CreateTopic failed");
                Err(Status::internal(e.message()))
            }
        }
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn describe_topic(
        &self,
        request: Request<DescribeTopicRequest>,
    ) -> Result<Response<DescribeTopicResponse>, Status> {
        let name = request.into_inner().topic_name;

        let state = self.service.controller_state().read().await;
        let Some(topic_info) = state.get_topic(&name) else {
            return Err(Status::not_found(format!("topic '{name}' not found")));
        };

        #[allow(clippy::cast_possible_wrap)]
        let response = DescribeTopicResponse {
            partition_count: topic_info.partition_count as i32,
            replication_factor: topic_info.replication_factor as i32,
            config: {
                let mut config = HashMap::new();
                config.insert(
                    "retention.ms".to_owned(),
                    self.default_retention_ms.to_string(),
                );
                config
            },
            tags: HashMap::new(),
        };
        drop(state);

        Ok(Response::new(response))
    }

    async fn tag_topics(
        &self,
        _request: Request<TagTopicsRequest>,
    ) -> Result<Response<TagTopicsResponse>, Status> {
        Ok(Response::new(TagTopicsResponse {}))
    }

    async fn delete_topic(
        &self,
        request: Request<DeleteTopicRequest>,
    ) -> Result<Response<DeleteTopicResponse>, Status> {
        let req = request.into_inner();
        let name = req.topic_name.clone();

        info!(topic = %name, "Admin DeleteTopic");

        match self.service.delete_topic_via_controller(name.clone()).await {
            Ok(()) => Ok(Response::new(DeleteTopicResponse {})),
            Err(e) if e.message().contains("not found") => {
                Err(Status::not_found(format!("topic '{name}' not found")))
            }
            Err(e) if e.message().contains("not the controller") => {
                warn!(topic = %name, "Not controller leader, returning UNAVAILABLE");
                Err(Status::unavailable("not the controller leader, retry"))
            }
            Err(e) => {
                warn!(topic = %name, error = %e, "Admin DeleteTopic failed");
                Err(Status::internal(e.message()))
            }
        }
    }

    async fn delete_consumer_groups(
        &self,
        request: Request<DeleteConsumerGroupsRequest>,
    ) -> Result<Response<DeleteConsumerGroupsResponse>, Status> {
        let names = request.into_inner().consumer_group_names;

        for name in &names {
            let group_id =
                ConsumerGroupId::new(HelixService::<TokioStorage, TransportHandle>::hash_string(
                    name,
                ));

            match self.service.progress_manager.store().remove_group(group_id).await {
                Ok(()) => info!(group = %name, "Deleted consumer group"),
                Err(e) => warn!(group = %name, error = %e, "Failed to delete consumer group"),
            }
        }

        Ok(Response::new(DeleteConsumerGroupsResponse {}))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let state = self.service.controller_state().read().await;
        let topic_names: Vec<String> =
            state.topics().map(|t| t.name.clone()).collect();
        drop(state);

        debug!(count = topic_names.len(), "Admin ListTopics");
        Ok(Response::new(ListTopicsResponse { topic_names }))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn describe_topics(
        &self,
        request: Request<DescribeTopicsRequest>,
    ) -> Result<Response<DescribeTopicsResponse>, Status> {
        let requested = request.into_inner().topic_names;
        let state = self.service.controller_state().read().await;

        let mut topics: HashMap<String, TopicDescription> = HashMap::new();

        if requested.is_empty() {
            // Return all topics when no names specified.
            for info in state.topics() {
                topics.insert(
                    info.name.clone(),
                    build_topic_description(info, self.default_retention_ms),
                );
            }
        } else {
            for name in &requested {
                if let Some(info) = state.get_topic(name) {
                    topics.insert(
                        name.clone(),
                        build_topic_description(info, self.default_retention_ms),
                    );
                }
            }
        }
        drop(state);

        debug!(count = topics.len(), "Admin DescribeTopics");
        Ok(Response::new(DescribeTopicsResponse { topics }))
    }

    async fn get_health(
        &self,
        _request: Request<GetHealthRequest>,
    ) -> Result<Response<GetHealthResponse>, Status> {
        let live = self.service.live_brokers().await;

        let total = self.service.cluster_nodes().len();
        let up = live.len();
        let is_healthy = up == total;

        let mut brokers_up: HashMap<String, bool> = HashMap::new();
        brokers_up.insert("default".to_owned(), is_healthy);

        debug!(is_healthy, total, up, "Admin GetHealth");
        Ok(Response::new(GetHealthResponse {
            is_healthy,
            brokers_up,
        }))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn get_topic_state(
        &self,
        request: Request<GetTopicStateRequest>,
    ) -> Result<Response<GetTopicStateResponse>, Status> {
        let name = request.into_inner().topic_name;

        let state = self.service.controller_state().read().await;
        let Some(info) = state.get_topic(&name) else {
            return Err(Status::not_found(format!("topic '{name}' not found")));
        };
        let topic_state = build_topic_state(info);
        drop(state);

        debug!(topic = %name, "Admin GetTopicState");
        Ok(Response::new(GetTopicStateResponse {
            topic_state: Some(topic_state),
        }))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn get_topic_states(
        &self,
        _request: Request<GetTopicStatesRequest>,
    ) -> Result<Response<GetTopicStatesResponse>, Status> {
        let state = self.service.controller_state().read().await;
        let mut states: HashMap<String, TopicState> = HashMap::new();

        for info in state.topics() {
            states.insert(info.name.clone(), build_topic_state(info));
        }
        drop(state);

        debug!(count = states.len(), "Admin GetTopicStates");
        Ok(Response::new(GetTopicStatesResponse { states }))
    }

    async fn list_usable_broker_ids(
        &self,
        _request: Request<ListUsableBrokerIdsRequest>,
    ) -> Result<Response<ListUsableBrokerIdsResponse>, Status> {
        let live = self.service.live_brokers().await;

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let broker_ids: Vec<i32> = live.iter().map(|n| n.get() as i32).collect();

        debug!(count = broker_ids.len(), "Admin ListUsableBrokerIds");
        Ok(Response::new(ListUsableBrokerIdsResponse { broker_ids }))
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn list_topic_configs(
        &self,
        _request: Request<ListTopicConfigsRequest>,
    ) -> Result<Response<ListTopicConfigsResponse>, Status> {
        let state = self.service.controller_state().read().await;
        let mut topic_configs: HashMap<String, TopicConfig> = HashMap::new();

        for info in state.topics() {
            topic_configs.insert(
                info.name.clone(),
                build_topic_config(info, self.default_retention_ms),
            );
        }
        drop(state);

        debug!(count = topic_configs.len(), "Admin ListTopicConfigs");
        Ok(Response::new(ListTopicConfigsResponse { topic_configs }))
    }

    async fn update_topic_config(
        &self,
        request: Request<UpdateTopicConfigRequest>,
    ) -> Result<Response<UpdateTopicConfigResponse>, Status> {
        let req = request.into_inner();
        info!(topic = %req.topic_name, "Admin UpdateTopicConfig (no-op stub)");
        Ok(Response::new(UpdateTopicConfigResponse {}))
    }

    async fn get_end_offsets(
        &self,
        request: Request<GetEndOffsetsRequest>,
    ) -> Result<Response<GetEndOffsetsResponse>, Status> {
        let topic_id = request.into_inner().topic_id;

        assert!(!topic_id.is_empty(), "topic_id must not be empty");

        debug!(topic = %topic_id, "Admin GetEndOffsets");

        // topic_id from streaming-admin IS the Kafka topic name in helix.
        let Some(offsets) = self.service.get_topic_end_offsets(&topic_id).await else {
            return Err(Status::not_found(format!("topic '{topic_id}' not found")));
        };

        assert!(offsets.len() <= 256, "partition count exceeds limit");

        Ok(Response::new(GetEndOffsetsResponse { offsets }))
    }
}

/// Builds a `TopicConfig` from controller `TopicInfo`.
#[allow(clippy::cast_possible_wrap)]
fn build_topic_config(
    info: &crate::controller::TopicInfo,
    default_retention_ms: u64,
) -> TopicConfig {
    TopicConfig {
        retention_ms: default_retention_ms,
        partitions: info.partition_count as i32,
        replication_factor: info.replication_factor as i32,
        config: HashMap::new(),
    }
}

/// Builds a `TopicDescription` from controller `TopicInfo`.
#[allow(clippy::cast_possible_wrap)]
fn build_topic_description(
    info: &crate::controller::TopicInfo,
    default_retention_ms: u64,
) -> TopicDescription {
    TopicDescription {
        topic_config: Some(build_topic_config(info, default_retention_ms)),
        tags: HashMap::new(),
    }
}

/// Builds a `TopicState` from controller `TopicInfo`.
///
/// Field layout matches the real resources.proto for wire compatibility.
const fn build_topic_state(
    _info: &crate::controller::TopicInfo,
) -> TopicState {
    TopicState {
        is_optimally_placed: true,
        spans_usable_brokers: true,
        uses_non_usable_brokers: false,
        broker_aligned_partition_count: 0,
    }
}

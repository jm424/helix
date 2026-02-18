//! Kafka admin gRPC service implementation (kafkaadmin.Resources).
//!
//! Implements the `kafkaadmin.Resources` gRPC trait for topic lifecycle
//! management (create, describe, delete topics).
//!
//! Write operations (create/delete topic) are tried locally first. If this
//! node is not the controller leader, the error includes a leader hint which
//! is used to forward the request to the correct node.

use std::collections::HashMap;
use std::sync::Arc;

use helix_core::ConsumerGroupId;
use helix_progress::ProgressStore;
use helix_runtime::TransportHandle;
use helix_wal::TokioStorage;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::generated_admin::resources_client::ResourcesClient;
use crate::generated_admin::resources_server::Resources;
use crate::generated_admin::{
    BrokersUp, CreateTopicRequest, CreateTopicResponse, DeleteConsumerGroupsRequest,
    DeleteConsumerGroupsResponse, DeleteTopicRequest, DeleteTopicResponse,
    DescribeTopicRequest, DescribeTopicResponse, DescribeTopicsRequest, DescribeTopicsResponse,
    GetHealthRequest, GetHealthResponse, GetTopicStateRequest, GetTopicStateResponse,
    GetTopicStatesRequest, GetTopicStatesResponse, ListTopicsRequest, ListTopicsResponse,
    TagTopicsRequest, TagTopicsResponse, TopicConfig, TopicDescription, TopicState,
};
use crate::service::HelixService;

/// Admin gRPC service wrapping `HelixService`.
pub struct AdminService {
    service: Arc<HelixService<TokioStorage, TransportHandle>>,
    admin_port: u16,
}

impl AdminService {
    /// Creates a new admin service.
    #[must_use]
    pub const fn new(
        service: Arc<HelixService<TokioStorage, TransportHandle>>,
        admin_port: u16,
    ) -> Self {
        Self {
            service,
            admin_port,
        }
    }

    /// Connects to a peer node's admin gRPC port given its node ID.
    async fn connect_to_node(
        &self,
        node_id: u64,
    ) -> Result<ResourcesClient<Channel>, Status> {
        let nid = helix_core::NodeId::new(node_id);
        let peer_addr = self
            .service
            .get_node_address(nid)
            .ok_or_else(|| Status::internal(format!("no address for node {node_id}")))?;

        let host = peer_addr
            .rfind(':')
            .map_or(peer_addr, |i| &peer_addr[..i]);

        let addr = format!("http://{host}:{}", self.admin_port);
        debug!(node_id, addr = %addr, "Forwarding to controller leader");

        ResourcesClient::connect(addr)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to node {node_id}: {e}")))
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
            Err(e) if e.controller_hint().is_some() => {
                let hint = e.controller_hint().expect("checked above");
                info!(topic = %name, leader = hint, "Forwarding CreateTopic to leader");
                let mut client = self.connect_to_node(hint).await?;
                client.create_topic(Request::new(req)).await
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
            config: std::collections::HashMap::new(),
            tags: std::collections::HashMap::new(),
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
            Err(e) if e.controller_hint().is_some() => {
                let hint = e.controller_hint().expect("checked above");
                info!(topic = %name, leader = hint, "Forwarding DeleteTopic to leader");
                let mut client = self.connect_to_node(hint).await?;
                client.delete_topic(Request::new(req)).await
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
                topics.insert(info.name.clone(), build_topic_description(info));
            }
        } else {
            for name in &requested {
                if let Some(info) = state.get_topic(name) {
                    topics.insert(name.clone(), build_topic_description(info));
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

        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let total = self.service.cluster_nodes().len() as i32;
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let up = live.len() as i32;

        let is_healthy = up == total;
        let mut brokers_up: HashMap<String, BrokersUp> = HashMap::new();
        brokers_up.insert("default".to_owned(), BrokersUp { total, up });

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
        let mut topic_states: HashMap<String, TopicState> = HashMap::new();

        for info in state.topics() {
            topic_states.insert(info.name.clone(), build_topic_state(info));
        }
        drop(state);

        debug!(count = topic_states.len(), "Admin GetTopicStates");
        Ok(Response::new(GetTopicStatesResponse { topic_states }))
    }
}

/// Builds a `TopicDescription` from controller `TopicInfo`.
#[allow(clippy::cast_possible_wrap)]
fn build_topic_description(
    info: &crate::controller::TopicInfo,
) -> TopicDescription {
    TopicDescription {
        topic_config: Some(TopicConfig {
            partition_count: info.partition_count as i32,
            replication_factor: info.replication_factor as i32,
            config: HashMap::new(),
        }),
        tags: HashMap::new(),
    }
}

/// Builds a `TopicState` from controller `TopicInfo`.
#[allow(clippy::cast_possible_wrap)]
const fn build_topic_state(
    info: &crate::controller::TopicInfo,
) -> TopicState {
    TopicState {
        partition_count: info.partition_count as i32,
        is_optimally_placed: true,
        spans_usable_brokers: true,
        has_out_of_sync_replicas: false,
        has_under_replicated_partitions: false,
    }
}

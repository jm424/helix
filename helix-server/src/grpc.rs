//! gRPC service trait implementation for Helix.
//!
//! This module implements the `Helix` tonic trait, translating gRPC requests
//! to internal service methods.

use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::generated::{
    helix_server::Helix, AckRequest, AckResponse, CreateConsumerGroupRequest,
    CreateConsumerGroupResponse, GetCommittedOffsetRequest, GetCommittedOffsetResponse,
    GetMetadataRequest, GetMetadataResponse, GetPartitionInfoRequest, GetPartitionInfoResponse,
    PullRequest, PullResponse, ReadRequest, ReadResponse, WriteRequest, WriteResponse,
};
use crate::service::HelixService;

#[tonic::async_trait]
impl Helix for HelixService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let inner = request.into_inner();
        debug!(topic = %inner.topic, records = inner.records.len(), "Write request");

        match self.write_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Write failed");
                Ok(Response::new(WriteResponse {
                    base_offset: 0,
                    record_count: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<ReadResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            offset = inner.offset,
            "Read request"
        );

        match self.read_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Read failed");
                Ok(Response::new(ReadResponse {
                    records: vec![],
                    high_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn get_metadata(
        &self,
        request: Request<GetMetadataRequest>,
    ) -> Result<Response<GetMetadataResponse>, Status> {
        let inner = request.into_inner();
        debug!(topics = ?inner.topics, "GetMetadata request");

        match self.get_metadata_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetMetadata failed");
                Err(Status::internal(e.message()))
            }
        }
    }

    async fn get_partition_info(
        &self,
        request: Request<GetPartitionInfoRequest>,
    ) -> Result<Response<GetPartitionInfoResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            "GetPartitionInfo request"
        );

        match self.get_partition_info_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetPartitionInfo failed");
                Ok(Response::new(GetPartitionInfoResponse {
                    partition: None,
                    log_start_offset: 0,
                    log_end_offset: 0,
                    high_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn pull(&self, request: Request<PullRequest>) -> Result<Response<PullResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            consumer = %inner.consumer_id,
            "Pull request"
        );

        match self.pull_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Pull failed");
                Ok(Response::new(PullResponse {
                    records: vec![],
                    lease_id: 0,
                    from_offset: 0,
                    to_offset: 0,
                    lease_expires_at_us: 0,
                    has_more: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            ack_count = inner.ack_lease_ids.len(),
            nack_count = inner.nack_lease_ids.len(),
            extend_count = inner.extend_lease_ids.len(),
            "Ack request"
        );

        match self.ack_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "Ack failed");
                Ok(Response::new(AckResponse {
                    acked_count: 0,
                    nacked_count: 0,
                    extended_count: 0,
                    low_watermark: 0,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn create_consumer_group(
        &self,
        request: Request<CreateConsumerGroupRequest>,
    ) -> Result<Response<CreateConsumerGroupResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            consumer_group = %inner.consumer_group_id,
            ack_mode = ?inner.ack_mode,
            topics = ?inner.topics,
            "CreateConsumerGroup request"
        );

        match self.create_consumer_group_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "CreateConsumerGroup failed");
                Ok(Response::new(CreateConsumerGroupResponse {
                    created: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }

    async fn get_committed_offset(
        &self,
        request: Request<GetCommittedOffsetRequest>,
    ) -> Result<Response<GetCommittedOffsetResponse>, Status> {
        let inner = request.into_inner();
        debug!(
            topic = %inner.topic,
            partition = inner.partition,
            consumer_group = %inner.consumer_group_id,
            "GetCommittedOffset request"
        );

        match self.get_committed_offset_internal(inner).await {
            Ok(response) => Ok(Response::new(response)),
            Err(e) => {
                warn!(error = %e, "GetCommittedOffset failed");
                Ok(Response::new(GetCommittedOffsetResponse {
                    low_watermark: 0,
                    has_committed: false,
                    error_code: e.to_error_code().into(),
                    error_message: Some(e.message()),
                }))
            }
        }
    }
}

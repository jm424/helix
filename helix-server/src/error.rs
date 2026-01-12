//! Server error types.

use crate::generated::ErrorCode;
use helix_progress::ProgressError;

/// Server error type.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Topic not found.
    #[error("topic not found: {topic}")]
    TopicNotFound {
        /// The topic name.
        topic: String,
    },

    /// Partition not found.
    #[error("partition {partition} not found in topic {topic}")]
    PartitionNotFound {
        /// The topic name.
        topic: String,
        /// The partition index.
        partition: i32,
    },

    /// Offset out of range.
    #[error("offset {offset} out of range [{start}, {end})")]
    OffsetOutOfRange {
        /// The requested offset.
        offset: u64,
        /// Start of valid range.
        start: u64,
        /// End of valid range.
        end: u64,
    },

    /// Not the leader for this partition.
    #[error("not leader for partition {partition} of topic {topic}")]
    NotLeader {
        /// The topic name.
        topic: String,
        /// The partition index.
        partition: i32,
        /// Hint about who the leader might be.
        leader_hint: Option<u64>,
    },

    /// Record batch too large.
    #[error("record batch too large: {size} bytes exceeds limit of {limit} bytes")]
    RecordBatchTooLarge {
        /// The actual size.
        size: usize,
        /// The maximum allowed size.
        limit: usize,
    },

    /// Consumer group not found.
    #[error("consumer group not found: {group_id}")]
    ConsumerGroupNotFound {
        /// The consumer group ID.
        group_id: String,
    },

    /// Consumer not found in group.
    #[error("consumer {consumer_id} not found in group {group_id}")]
    ConsumerNotFound {
        /// The consumer group ID.
        group_id: String,
        /// The consumer ID.
        consumer_id: String,
    },

    /// Lease not found.
    #[error("lease {lease_id} not found")]
    LeaseNotFound {
        /// The lease ID.
        lease_id: u64,
    },

    /// Lease expired.
    #[error("lease {lease_id} expired")]
    LeaseExpired {
        /// The lease ID.
        lease_id: u64,
    },

    /// Offset already committed.
    #[error("offset {offset} already committed for partition {partition}")]
    OffsetAlreadyCommitted {
        /// The partition.
        partition: i32,
        /// The offset.
        offset: u64,
    },

    /// Offset not leased.
    #[error("offset {offset} not leased by consumer {consumer_id}")]
    OffsetNotLeased {
        /// The consumer ID.
        consumer_id: String,
        /// The offset.
        offset: u64,
    },

    /// Too many consumer groups.
    #[error("too many consumer groups: {count} (max {max})")]
    TooManyGroups {
        /// Current count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// Too many consumers in group.
    #[error("too many consumers in group {group_id}: {count} (max {max})")]
    TooManyConsumers {
        /// The consumer group ID.
        group_id: String,
        /// Current count.
        count: usize,
        /// Maximum allowed.
        max: usize,
    },

    /// Progress tracking error.
    #[error("progress error: {0}")]
    Progress(#[from] ProgressError),

    /// Internal error.
    #[error("internal error: {message}")]
    Internal {
        /// Error message.
        message: String,
    },
}

impl ServerError {
    /// Converts the error to a protobuf error code.
    #[must_use]
    pub const fn to_error_code(&self) -> ErrorCode {
        match self {
            Self::TopicNotFound { .. } => ErrorCode::InvalidTopic,
            Self::PartitionNotFound { .. } => ErrorCode::InvalidPartition,
            Self::OffsetOutOfRange { .. } => ErrorCode::OffsetOutOfRange,
            Self::NotLeader { .. } => ErrorCode::NotLeader,
            Self::RecordBatchTooLarge { .. } => ErrorCode::RecordBatchTooLarge,
            Self::ConsumerGroupNotFound { .. } => ErrorCode::ConsumerGroupNotFound,
            Self::ConsumerNotFound { .. } => ErrorCode::ConsumerNotFound,
            Self::LeaseNotFound { .. } => ErrorCode::LeaseNotFound,
            Self::LeaseExpired { .. } => ErrorCode::LeaseExpired,
            Self::OffsetAlreadyCommitted { .. } => ErrorCode::OffsetAlreadyCommitted,
            Self::OffsetNotLeased { .. } => ErrorCode::OffsetNotLeased,
            Self::TooManyGroups { .. } => ErrorCode::TooManyGroups,
            Self::TooManyConsumers { .. } => ErrorCode::TooManyConsumers,
            Self::Progress(e) => progress_error_to_code(e),
            Self::Internal { .. } => ErrorCode::Unknown,
        }
    }

    /// Returns the error message.
    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}

/// Maps a `ProgressError` to an `ErrorCode`.
const fn progress_error_to_code(e: &ProgressError) -> ErrorCode {
    match e {
        ProgressError::GroupNotFound { .. } => ErrorCode::ConsumerGroupNotFound,
        ProgressError::ConsumerNotFound { .. } => ErrorCode::ConsumerNotFound,
        ProgressError::PartitionNotFound { .. } => ErrorCode::InvalidPartition,
        ProgressError::LeaseNotFound { .. } => ErrorCode::LeaseNotFound,
        ProgressError::LeaseExpired { .. } => ErrorCode::LeaseExpired,
        ProgressError::OffsetAlreadyCommitted { .. } => ErrorCode::OffsetAlreadyCommitted,
        ProgressError::OffsetNotLeased { .. } => ErrorCode::OffsetNotLeased,
        ProgressError::TooManyGroups { .. } => ErrorCode::TooManyGroups,
        ProgressError::TooManyConsumers { .. } => ErrorCode::TooManyConsumers,
        ProgressError::LeaseDurationExceeded { .. } | ProgressError::Io { .. } => ErrorCode::Unknown,
    }
}

/// Result type for server operations.
pub type ServerResult<T> = Result<T, ServerError>;

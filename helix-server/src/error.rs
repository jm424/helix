//! Server error types.

use crate::generated::ErrorCode;

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
            Self::Internal { .. } => ErrorCode::Unknown,
        }
    }

    /// Returns the error message.
    #[must_use]
    pub fn message(&self) -> String {
        self.to_string()
    }
}

/// Result type for server operations.
pub type ServerResult<T> = Result<T, ServerError>;

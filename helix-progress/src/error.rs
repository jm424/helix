//! Progress tracking error types.

use helix_core::{ConsumerGroupId, ConsumerId, LeaseId, Offset, PartitionId, TopicId};
use thiserror::Error;

/// Result type for progress operations.
pub type ProgressResult<T> = Result<T, ProgressError>;

/// Errors that can occur during progress operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProgressError {
    /// Consumer group not found.
    #[error("consumer group not found: {group_id}")]
    GroupNotFound {
        /// The group that was not found.
        group_id: ConsumerGroupId,
    },

    /// Consumer not found in group.
    #[error("consumer {consumer_id} not found in group {group_id}")]
    ConsumerNotFound {
        /// The consumer group.
        group_id: ConsumerGroupId,
        /// The consumer that was not found.
        consumer_id: ConsumerId,
    },

    /// Partition not found in group.
    #[error("partition {topic_id}/{partition_id} not found in group {group_id}")]
    PartitionNotFound {
        /// The consumer group.
        group_id: ConsumerGroupId,
        /// The topic.
        topic_id: TopicId,
        /// The partition.
        partition_id: PartitionId,
    },

    /// Lease not found or expired.
    #[error("lease {lease_id} not found")]
    LeaseNotFound {
        /// The lease that was not found.
        lease_id: LeaseId,
    },

    /// Lease expired.
    #[error("lease {lease_id} expired at {expired_at_us}us")]
    LeaseExpired {
        /// The expired lease.
        lease_id: LeaseId,
        /// When the lease expired (microseconds since epoch).
        expired_at_us: u64,
    },

    /// Offset already committed.
    #[error("offset {offset} already committed for {topic_id}/{partition_id}")]
    OffsetAlreadyCommitted {
        /// The topic.
        topic_id: TopicId,
        /// The partition.
        partition_id: PartitionId,
        /// The offset that was already committed.
        offset: Offset,
    },

    /// Offset not leased by this consumer.
    #[error("offset {offset} not leased by consumer {consumer_id}")]
    OffsetNotLeased {
        /// The consumer that doesn't hold the lease.
        consumer_id: ConsumerId,
        /// The offset that is not leased.
        offset: Offset,
    },

    /// Too many consumer groups for partition.
    #[error("too many consumer groups for partition {topic_id}/{partition_id}: {count} >= {max}")]
    TooManyGroups {
        /// The topic.
        topic_id: TopicId,
        /// The partition.
        partition_id: PartitionId,
        /// Current count.
        count: u32,
        /// Maximum allowed.
        max: u32,
    },

    /// Too many consumers in group.
    #[error("too many consumers in group {group_id}: {count} >= {max}")]
    TooManyConsumers {
        /// The consumer group.
        group_id: ConsumerGroupId,
        /// Current count.
        count: u32,
        /// Maximum allowed.
        max: u32,
    },

    /// Lease duration exceeds maximum.
    #[error("lease duration {requested_us}us exceeds max {max_us}us")]
    LeaseDurationExceeded {
        /// Requested duration in microseconds.
        requested_us: u64,
        /// Maximum allowed duration in microseconds.
        max_us: u64,
    },

    /// I/O error during storage operation.
    #[error("I/O error: {operation}: {message}")]
    Io {
        /// The operation that failed.
        operation: &'static str,
        /// Error message.
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ProgressError::GroupNotFound {
            group_id: ConsumerGroupId::new(42),
        };
        assert!(err.to_string().contains("42"));

        let err = ProgressError::LeaseExpired {
            lease_id: LeaseId::new(123),
            expired_at_us: 1_000_000,
        };
        assert!(err.to_string().contains("123"));
        assert!(err.to_string().contains("1000000"));
    }

    #[test]
    fn test_error_equality() {
        let err1 = ProgressError::GroupNotFound {
            group_id: ConsumerGroupId::new(1),
        };
        let err2 = ProgressError::GroupNotFound {
            group_id: ConsumerGroupId::new(1),
        };
        let err3 = ProgressError::GroupNotFound {
            group_id: ConsumerGroupId::new(2),
        };

        assert_eq!(err1, err2);
        assert_ne!(err1, err3);
    }
}

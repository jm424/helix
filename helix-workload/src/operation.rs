//! Operation model for workload execution.
//!
//! Defines the types of operations that can be performed against a Kafka-compatible
//! system and their results.

use bytes::Bytes;

/// Timestamp in microseconds since workload start.
pub type Timestamp = u64;

/// A single operation in the workload.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Unique operation ID for correlation.
    pub id: u64,
    /// Operation type and parameters.
    pub kind: OperationKind,
    /// When the operation was invoked (microseconds since start).
    pub invoked_at: Timestamp,
    /// When the operation completed (filled after execution).
    pub completed_at: Option<Timestamp>,
    /// Result of the operation (filled after execution).
    pub result: Option<OperationResult>,
}

impl Operation {
    /// Creates a new operation with the given ID and kind.
    #[must_use]
    pub const fn new(id: u64, kind: OperationKind, invoked_at: Timestamp) -> Self {
        Self {
            id,
            kind,
            invoked_at,
            completed_at: None,
            result: None,
        }
    }

    /// Records the completion of this operation.
    pub fn complete(&mut self, completed_at: Timestamp, result: OperationResult) {
        self.completed_at = Some(completed_at);
        self.result = Some(result);
    }

    /// Returns true if this operation completed successfully.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.result.as_ref().is_some_and(OperationResult::is_ok)
    }

    /// Returns the latency in microseconds, if completed.
    #[must_use]
    pub fn latency_us(&self) -> Option<u64> {
        self.completed_at.map(|c| c.saturating_sub(self.invoked_at))
    }
}

/// The type of operation to perform.
#[derive(Debug, Clone)]
pub enum OperationKind {
    /// Produce a message to a topic/partition.
    Send {
        /// Target topic name.
        topic: String,
        /// Target partition.
        partition: i32,
        /// Message payload (includes verification data).
        payload: Bytes,
    },

    /// Fetch messages from a topic/partition starting at offset.
    Poll {
        /// Source topic name.
        topic: String,
        /// Source partition.
        partition: i32,
        /// Starting offset for fetch.
        start_offset: u64,
        /// Maximum number of messages to fetch.
        max_messages: u32,
    },

    /// Commit consumer offset for a group.
    Commit {
        /// Consumer group ID.
        group: String,
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
        /// Offset to commit.
        offset: u64,
    },

    /// Fetch committed offset for a consumer group.
    FetchOffset {
        /// Consumer group ID.
        group: String,
        /// Topic name.
        topic: String,
        /// Partition.
        partition: i32,
    },

    /// Join a consumer group.
    JoinGroup {
        /// Consumer group ID.
        group: String,
        /// Topics to subscribe to.
        topics: Vec<String>,
        /// Member ID (empty for new member).
        member_id: String,
    },

    /// Send heartbeat to consumer group coordinator.
    Heartbeat {
        /// Consumer group ID.
        group: String,
        /// Generation ID from join.
        generation_id: i32,
        /// Member ID from join.
        member_id: String,
    },

    /// Leave a consumer group.
    LeaveGroup {
        /// Consumer group ID.
        group: String,
        /// Member ID.
        member_id: String,
    },
}

impl OperationKind {
    /// Returns the topic name if this operation involves a topic.
    #[must_use]
    pub fn topic(&self) -> Option<&str> {
        match self {
            Self::Send { topic, .. }
            | Self::Poll { topic, .. }
            | Self::Commit { topic, .. }
            | Self::FetchOffset { topic, .. } => Some(topic),
            Self::JoinGroup { .. } | Self::Heartbeat { .. } | Self::LeaveGroup { .. } => None,
        }
    }

    /// Returns the partition if this operation involves a partition.
    #[must_use]
    pub const fn partition(&self) -> Option<i32> {
        match self {
            Self::Send { partition, .. }
            | Self::Poll { partition, .. }
            | Self::Commit { partition, .. }
            | Self::FetchOffset { partition, .. } => Some(*partition),
            Self::JoinGroup { .. } | Self::Heartbeat { .. } | Self::LeaveGroup { .. } => None,
        }
    }

    /// Returns the consumer group if this operation involves a group.
    #[must_use]
    pub fn group(&self) -> Option<&str> {
        match self {
            Self::Commit { group, .. }
            | Self::FetchOffset { group, .. }
            | Self::JoinGroup { group, .. }
            | Self::Heartbeat { group, .. }
            | Self::LeaveGroup { group, .. } => Some(group),
            Self::Send { .. } | Self::Poll { .. } => None,
        }
    }
}

/// Result of an operation execution.
#[derive(Debug, Clone)]
pub enum OperationResult {
    /// Send completed successfully.
    SendOk {
        /// Assigned offset for the message.
        offset: u64,
    },
    /// Send failed.
    SendError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Poll completed successfully.
    PollOk {
        /// Messages returned: (offset, payload).
        messages: Vec<(u64, Bytes)>,
    },
    /// Poll failed.
    PollError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Commit completed successfully.
    CommitOk,
    /// Commit failed.
    CommitError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Fetch offset completed successfully.
    FetchOffsetOk {
        /// Committed offset, if any.
        offset: Option<u64>,
    },
    /// Fetch offset failed.
    FetchOffsetError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Join group completed successfully.
    JoinGroupOk {
        /// Assigned member ID.
        member_id: String,
        /// Generation ID for this membership.
        generation_id: i32,
        /// Whether this member is the leader.
        is_leader: bool,
        /// Assigned partitions (if leader performed assignment).
        assignments: Vec<(String, i32)>,
    },
    /// Join group failed.
    JoinGroupError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Heartbeat completed successfully.
    HeartbeatOk,
    /// Heartbeat failed (may indicate rebalance needed).
    HeartbeatError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },

    /// Leave group completed successfully.
    LeaveGroupOk,
    /// Leave group failed.
    LeaveGroupError {
        /// Kafka error code.
        code: i16,
        /// Error message.
        message: String,
    },
}

impl OperationResult {
    /// Returns true if this result indicates success.
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(
            self,
            Self::SendOk { .. }
                | Self::PollOk { .. }
                | Self::CommitOk
                | Self::FetchOffsetOk { .. }
                | Self::JoinGroupOk { .. }
                | Self::HeartbeatOk
                | Self::LeaveGroupOk
        )
    }

    /// Returns the error code if this is an error result.
    #[must_use]
    pub const fn error_code(&self) -> Option<i16> {
        match self {
            Self::SendError { code, .. }
            | Self::PollError { code, .. }
            | Self::CommitError { code, .. }
            | Self::FetchOffsetError { code, .. }
            | Self::JoinGroupError { code, .. }
            | Self::HeartbeatError { code, .. }
            | Self::LeaveGroupError { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns the assigned offset for a successful send.
    #[must_use]
    pub const fn send_offset(&self) -> Option<u64> {
        match self {
            Self::SendOk { offset } => Some(*offset),
            _ => None,
        }
    }

    /// Returns the messages for a successful poll.
    #[must_use]
    pub fn poll_messages(&self) -> Option<&[(u64, Bytes)]> {
        match self {
            Self::PollOk { messages } => Some(messages),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_completion() {
        let mut op = Operation::new(
            1,
            OperationKind::Send {
                topic: "test".to_string(),
                partition: 0,
                payload: Bytes::from_static(b"hello"),
            },
            1000,
        );

        assert!(!op.is_ok());
        assert!(op.latency_us().is_none());

        op.complete(1500, OperationResult::SendOk { offset: 42 });

        assert!(op.is_ok());
        assert_eq!(op.latency_us(), Some(500));
    }

    #[test]
    fn test_operation_kind_accessors() {
        let send = OperationKind::Send {
            topic: "my-topic".to_string(),
            partition: 3,
            payload: Bytes::new(),
        };
        assert_eq!(send.topic(), Some("my-topic"));
        assert_eq!(send.partition(), Some(3));
        assert!(send.group().is_none());

        let commit = OperationKind::Commit {
            group: "my-group".to_string(),
            topic: "my-topic".to_string(),
            partition: 0,
            offset: 100,
        };
        assert_eq!(commit.group(), Some("my-group"));
        assert_eq!(commit.topic(), Some("my-topic"));
    }

    #[test]
    fn test_operation_result_accessors() {
        let ok = OperationResult::SendOk { offset: 42 };
        assert!(ok.is_ok());
        assert_eq!(ok.send_offset(), Some(42));
        assert!(ok.error_code().is_none());

        let err = OperationResult::SendError {
            code: 6,
            message: "not leader".to_string(),
        };
        assert!(!err.is_ok());
        assert_eq!(err.error_code(), Some(6));
        assert!(err.send_offset().is_none());
    }
}

//! Error types for Kafka compatibility layer.

use kafka_protocol::protocol::StrBytes;
use thiserror::Error;

/// Result type for Kafka compatibility operations.
pub type KafkaCompatResult<T> = Result<T, KafkaCompatError>;

/// Errors that can occur in the Kafka compatibility layer.
#[derive(Debug, Error)]
pub enum KafkaCompatError {
    /// I/O error during network operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Error decoding Kafka protocol message.
    #[error("Protocol decode error: {message}")]
    Decode { message: String },

    /// Error encoding Kafka protocol message.
    #[error("Protocol encode error: {message}")]
    Encode { message: String },

    /// Unknown or unsupported API key.
    #[error("Unsupported API key: {api_key}")]
    UnsupportedApi { api_key: i16 },

    /// Unsupported API version.
    #[error("Unsupported API version: {api_key} v{version}")]
    UnsupportedVersion { api_key: i16, version: i16 },

    /// Topic not found.
    #[error("Topic not found: {topic}")]
    TopicNotFound { topic: String },

    /// Partition not found.
    #[error("Partition not found: {topic}/{partition}")]
    PartitionNotFound { topic: String, partition: i32 },

    /// Not the leader for this partition.
    #[error("Not leader for partition: {topic}/{partition}")]
    NotLeader { topic: String, partition: i32 },

    /// Consumer group not found.
    #[error("Consumer group not found: {group_id}")]
    GroupNotFound { group_id: String },

    /// Internal Helix error.
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Connection closed by client.
    #[error("Connection closed")]
    ConnectionClosed,
}

impl KafkaCompatError {
    /// Convert to Kafka error code.
    ///
    /// See: <https://kafka.apache.org/protocol#protocol_error_codes>
    #[must_use]
    pub const fn to_kafka_error_code(&self) -> i16 {
        match self {
            // UNKNOWN_SERVER_ERROR (-1)
            Self::Io(_)
            | Self::Internal { .. }
            | Self::Decode { .. }
            | Self::Encode { .. }
            | Self::ConnectionClosed => -1,

            // UNSUPPORTED_VERSION (35)
            Self::UnsupportedApi { .. } | Self::UnsupportedVersion { .. } => 35,

            // UNKNOWN_TOPIC_OR_PARTITION (3)
            Self::TopicNotFound { .. } | Self::PartitionNotFound { .. } => 3,

            // NOT_LEADER_OR_FOLLOWER (6)
            Self::NotLeader { .. } => 6,

            // GROUP_ID_NOT_FOUND (69)
            Self::GroupNotFound { .. } => 69,
        }
    }

    /// Create a decode error from any error type.
    pub fn decode<E: std::fmt::Display>(err: E) -> Self {
        Self::Decode {
            message: err.to_string(),
        }
    }

    /// Create an encode error from any error type.
    pub fn encode<E: std::fmt::Display>(err: E) -> Self {
        Self::Encode {
            message: err.to_string(),
        }
    }
}

/// Convert `StrBytes` to `String` for error messages.
pub fn str_bytes_to_string(s: &StrBytes) -> String {
    String::from_utf8_lossy(s.as_ref()).to_string()
}

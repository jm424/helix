//! Error types for the Kafka protocol module.

use std::io;

/// Errors that can occur in Kafka protocol handling.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    /// Failed to decode a Kafka protocol message.
    #[error("decode error: {message}")]
    Decode {
        /// Error details.
        message: String,
    },

    /// Failed to encode a Kafka protocol message.
    #[error("encode error: {message}")]
    Encode {
        /// Error details.
        message: String,
    },

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Unsupported API.
    #[error("unsupported API: key={api_key} version={api_version}")]
    UnsupportedApi {
        /// API key.
        api_key: i16,
        /// API version.
        api_version: i16,
    },

    /// Not leader for partition.
    #[error("not leader for {topic}/{partition}")]
    NotLeader {
        /// Topic name.
        topic: String,
        /// Partition ID.
        partition: i32,
        /// Hint for the actual leader, if known.
        leader_hint: Option<i32>,
    },

    /// Storage error from underlying Helix service.
    #[error("storage error: {message}")]
    Storage {
        /// Error details.
        message: String,
    },
}

impl KafkaError {
    /// Creates a decode error from any error type.
    pub fn decode<E: std::fmt::Display>(e: E) -> Self {
        Self::Decode {
            message: e.to_string(),
        }
    }

    /// Creates an encode error from any error type.
    pub fn encode<E: std::fmt::Display>(e: E) -> Self {
        Self::Encode {
            message: e.to_string(),
        }
    }
}

/// Result type for Kafka protocol operations.
pub type KafkaResult<T> = Result<T, KafkaError>;

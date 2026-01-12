//! Flow control error types.

/// Flow control error type.
#[derive(Debug, thiserror::Error)]
pub enum FlowError {
    /// Rate limit exceeded.
    #[error("rate limit exceeded for stream {stream_id}: {tokens_requested} tokens requested, {tokens_available} available")]
    RateLimitExceeded {
        /// The stream ID.
        stream_id: u64,
        /// Tokens requested.
        tokens_requested: u64,
        /// Tokens available.
        tokens_available: u64,
    },

    /// Queue is full.
    #[error("queue full: {size} items (max {max_size})")]
    QueueFull {
        /// Current queue size.
        size: usize,
        /// Maximum queue size.
        max_size: usize,
    },

    /// Stream not found.
    #[error("stream {stream_id} not found")]
    StreamNotFound {
        /// The stream ID.
        stream_id: u64,
    },

    /// Invalid configuration.
    #[error("invalid configuration: {message}")]
    InvalidConfig {
        /// Error message.
        message: String,
    },

    /// Operation would block.
    #[error("operation would block")]
    WouldBlock,

    /// Shutdown in progress.
    #[error("shutdown in progress")]
    Shutdown,
}

/// Result type for flow control operations.
pub type FlowResult<T> = Result<T, FlowError>;

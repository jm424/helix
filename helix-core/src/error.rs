//! Error types for Helix core operations.
//!
//! Following `TigerStyle`: all errors must be handled explicitly.
//! No silent failures, no ignored errors.

use std::fmt;

/// The result type for Helix operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in Helix operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A storage operation failed.
    Storage(StorageErrorKind),

    /// A network operation failed.
    Network(NetworkErrorKind),

    /// An operation timed out.
    Timeout {
        /// What operation timed out.
        operation: &'static str,
        /// How long we waited (in microseconds).
        waited_us: u64,
    },

    /// A resource limit was exceeded.
    LimitExceeded {
        /// Which limit was exceeded.
        limit: &'static str,
        /// The maximum allowed value.
        max: u64,
        /// The actual value that exceeded the limit.
        actual: u64,
    },

    /// An invalid argument was provided.
    InvalidArgument {
        /// The name of the argument.
        name: &'static str,
        /// Why it was invalid.
        reason: &'static str,
    },

    /// The requested resource was not found.
    NotFound {
        /// The type of resource.
        resource: &'static str,
        /// An identifier for the resource.
        id: u64,
    },

    /// The operation is not permitted in the current state.
    InvalidState {
        /// The current state.
        current: &'static str,
        /// The required state for this operation.
        required: &'static str,
    },
}

/// Kinds of storage errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageErrorKind {
    /// The storage device is full.
    DeviceFull,
    /// Data corruption was detected.
    Corruption,
    /// An I/O error occurred.
    Io,
    /// The file or directory does not exist.
    NotFound,
    /// Permission denied.
    PermissionDenied,
}

/// Kinds of network errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkErrorKind {
    /// Connection refused by the peer.
    ConnectionRefused,
    /// Connection reset by the peer.
    ConnectionReset,
    /// The connection timed out.
    TimedOut,
    /// The address is not reachable.
    Unreachable,
    /// The message was too large.
    MessageTooLarge,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(kind) => write!(f, "storage error: {kind:?}"),
            Self::Network(kind) => write!(f, "network error: {kind:?}"),
            Self::Timeout {
                operation,
                waited_us,
            } => {
                write!(f, "timeout: {operation} after {waited_us}us")
            }
            Self::LimitExceeded { limit, max, actual } => {
                write!(f, "limit exceeded: {limit} (max={max}, actual={actual})")
            }
            Self::InvalidArgument { name, reason } => {
                write!(f, "invalid argument '{name}': {reason}")
            }
            Self::NotFound { resource, id } => {
                write!(f, "{resource} not found: {id}")
            }
            Self::InvalidState { current, required } => {
                write!(f, "invalid state: in {current}, need {required}")
            }
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::Timeout {
            operation: "read",
            waited_us: 5_000_000,
        };
        assert_eq!(format!("{err}"), "timeout: read after 5000000us");
    }

    #[test]
    fn test_limit_exceeded_display() {
        let err = Error::LimitExceeded {
            limit: "batch_size",
            max: 1024,
            actual: 2048,
        };
        let msg = format!("{err}");
        assert!(msg.contains("batch_size"));
        assert!(msg.contains("1024"));
        assert!(msg.contains("2048"));
    }
}

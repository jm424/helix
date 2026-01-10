//! Partition error types.

use helix_core::Offset;

/// Result type for partition operations.
pub type PartitionResult<T> = Result<T, PartitionError>;

/// Partition operation errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionError {
    /// Offset is out of range.
    OffsetOutOfRange {
        /// Requested offset.
        offset: Offset,
        /// First available offset.
        first: Offset,
        /// Last available offset.
        last: Offset,
    },
    /// Segment is full.
    SegmentFull {
        /// Reason for being full.
        reason: &'static str,
    },
    /// Record batch is too large.
    BatchTooLarge {
        /// Actual size.
        size: usize,
        /// Maximum allowed size.
        max: usize,
    },
    /// Data corruption detected.
    Corruption {
        /// Description of corruption.
        message: String,
    },
    /// I/O error.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Error description.
        message: String,
    },
    /// Partition is closed.
    Closed,
    /// Invalid configuration.
    InvalidConfig {
        /// Configuration error description.
        message: String,
    },
}

impl std::fmt::Display for PartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OffsetOutOfRange { offset, first, last } => {
                write!(
                    f,
                    "offset {} out of range [{}, {}]",
                    offset.get(),
                    first.get(),
                    last.get()
                )
            }
            Self::SegmentFull { reason } => write!(f, "segment full: {}", reason),
            Self::BatchTooLarge { size, max } => {
                write!(f, "batch too large: {} > {} bytes", size, max)
            }
            Self::Corruption { message } => write!(f, "data corruption: {}", message),
            Self::Io { operation, message } => {
                write!(f, "I/O error during {}: {}", operation, message)
            }
            Self::Closed => write!(f, "partition is closed"),
            Self::InvalidConfig { message } => write!(f, "invalid config: {}", message),
        }
    }
}

impl std::error::Error for PartitionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = PartitionError::OffsetOutOfRange {
            offset: Offset::new(100),
            first: Offset::new(0),
            last: Offset::new(50),
        };
        assert!(format!("{err}").contains("100"));
        assert!(format!("{err}").contains("out of range"));
    }
}

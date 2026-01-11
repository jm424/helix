//! Tier error types.
//!
//! This module defines the error types used throughout the helix-tier crate
//! for tiered storage operations.

use thiserror::Error;

/// Result type for tier operations.
pub type TierResult<T> = Result<T, TierError>;

/// Errors that can occur during tier operations.
#[derive(Debug, Error)]
pub enum TierError {
    /// Object not found in storage.
    #[error("object not found: {key}")]
    NotFound {
        /// The object key that was not found.
        key: String,
    },

    /// Upload failed.
    #[error("upload failed for {key}: {message}")]
    UploadFailed {
        /// The object key.
        key: String,
        /// Error message.
        message: String,
    },

    /// Download failed.
    #[error("download failed for {key}: {message}")]
    DownloadFailed {
        /// The object key.
        key: String,
        /// Error message.
        message: String,
    },

    /// Data corruption detected.
    #[error("data corruption for {key}: expected CRC {expected:#010x}, got {actual:#010x}")]
    DataCorruption {
        /// The object key.
        key: String,
        /// Expected CRC32 value.
        expected: u32,
        /// Actual CRC32 value.
        actual: u32,
    },

    /// Metadata error.
    #[error("metadata error: {message}")]
    Metadata {
        /// Error message.
        message: String,
    },

    /// Segment not eligible for tiering.
    #[error("segment {segment_id} not eligible: {reason}")]
    NotEligible {
        /// Segment ID.
        segment_id: u64,
        /// Why it's not eligible.
        reason: &'static str,
    },

    /// I/O error.
    #[error("I/O error: {operation}: {message}")]
    Io {
        /// What operation was being performed.
        operation: &'static str,
        /// Error message.
        message: String,
    },
}

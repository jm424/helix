//! Helix Tier - S3 tiered storage for WAL segments.
//!
//! This crate provides tiered storage capabilities for moving sealed WAL segments
//! to S3-compatible object storage and fetching them back on demand.
//!
//! # Design Principles (`TigerStyle`)
//!
//! - **Deterministic testing**: `SimulatedObjectStorage` enables fault injection
//! - **Explicit limits**: Bounded batch sizes, configurable thresholds
//! - **Assertions**: Pre/post-conditions on all state transitions
//! - **No unsafe code**: Safety > Performance
//!
//! # Storage Backends
//!
//! - [`SimulatedObjectStorage`]: In-memory storage with fault injection for DST
//! - [`FilesystemObjectStorage`]: Local filesystem storage for development/testing
//! - `S3ObjectStorage`: Production S3 backend (behind `s3` feature flag)
//!
//! # Example
//!
//! ```ignore
//! use helix_tier::{
//!     TieringManager, TieringConfig,
//!     SimulatedObjectStorage, InMemoryMetadataStore,
//!     SegmentMetadata, SegmentLocation,
//! };
//! use helix_core::{TopicId, PartitionId};
//! use helix_wal::SegmentId;
//!
//! // Create simulated storage for testing.
//! let storage = SimulatedObjectStorage::new(42);
//! let metadata_store = InMemoryMetadataStore::new();
//! let config = TieringConfig::for_testing();
//!
//! let manager = TieringManager::new(storage, metadata_store, config);
//!
//! // Register a new segment.
//! let meta = SegmentMetadata::new(
//!     SegmentId::new(1).unwrap(),
//!     TopicId::new(1),
//!     PartitionId::new(0),
//!     0,
//! );
//! manager.register_segment(meta).await?;
//!
//! // Later, after sealing and committing...
//! manager.upload_segment(segment_id, data).await?;
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod filesystem;
mod manager;
mod metadata;
#[cfg(feature = "s3")]
mod s3;
mod storage;

pub use error::{TierError, TierResult};
pub use filesystem::{FilesystemConfig, FilesystemObjectStorage};
pub use manager::{IntegratedTieringManager, TieringConfig, TieringManager};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3ConfigError, S3ObjectStorage, S3StorageClass};
pub use metadata::{
    InMemoryMetadataStore, MetadataStore, MetadataStoreFaultConfig, SegmentLocation,
    SegmentMetadata,
};
pub use storage::{
    ObjectKey, ObjectStorage, ObjectStorageFaultConfig, SegmentReader, SimulatedObjectStorage,
};

/// Tiering configuration limits.
pub mod limits {
    /// Maximum number of segments to tier in a single batch.
    pub const TIER_BATCH_SIZE_MAX: u32 = 100;

    /// Default minimum age in seconds before a segment can be tiered.
    pub const SEGMENT_AGE_SECS_MIN_DEFAULT: u64 = 3600;

    /// Default maximum concurrent uploads.
    pub const CONCURRENT_UPLOADS_MAX_DEFAULT: u32 = 4;
}

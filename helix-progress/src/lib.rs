//! Consumer progress tracking for Helix.
//!
//! This crate provides consumer progress tracking with lease-based offset
//! management, supporting both Kafka-style cumulative and Pulsar-style
//! individual acknowledgment modes.
//!
//! # Overview
//!
//! Consumer progress tracking enables:
//! - **Lease management**: Consumers lease offsets before processing to prevent
//!   duplicate delivery during concurrent consumption.
//! - **Offset commits**: Consumers commit processed offsets to advance progress.
//! - **Watermark tracking**: The low watermark indicates the safe eviction
//!   boundary for tiered storage.
//!
//! # Acknowledgment Modes
//!
//! Two acknowledgment modes are supported:
//!
//! - **Cumulative** (Kafka-style): Committing offset N means "processed everything
//!   up to and including N". The watermark advances directly to N+1.
//!
//! - **Individual** (Pulsar-style): Commits are tracked individually using a
//!   bitmap. The watermark only advances when contiguous offsets are committed.
//!   This supports parallel processing where offsets may be committed out of order.
//!
//! # Example
//!
//! ```ignore
//! use helix_progress::{ProgressManager, ProgressConfig, SimulatedProgressStore};
//! use helix_core::{ConsumerGroupId, ConsumerId, TopicId, PartitionId, Offset};
//!
//! // Create manager with simulated store.
//! let store = SimulatedProgressStore::new(42);
//! let manager = ProgressManager::new(store, ProgressConfig::for_testing());
//!
//! // Register a consumer.
//! manager.register_consumer(
//!     ConsumerGroupId::new(1),
//!     ConsumerId::new(10),
//!     current_time_us,
//! ).await?;
//!
//! // Lease offsets for consumption.
//! let lease = manager.lease_offsets(
//!     ConsumerGroupId::new(1),
//!     TopicId::new(1),
//!     PartitionId::new(0),
//!     ConsumerId::new(10),
//!     Offset::new(0),    // from_offset
//!     100,               // max_count
//!     60_000_000,        // duration_us (1 minute)
//!     current_time_us,
//! ).await?;
//!
//! // Process records, then commit.
//! manager.commit_offset(
//!     ConsumerGroupId::new(1),
//!     TopicId::new(1),
//!     PartitionId::new(0),
//!     ConsumerId::new(10),
//!     Offset::new(99),   // Cumulative: processed up to 99
//!     current_time_us,
//! ).await?;
//!
//! // Query safe eviction boundary.
//! let safe_offset = manager.get_safe_eviction_offset(
//!     TopicId::new(1),
//!     PartitionId::new(0),
//! ).await?;
//! ```
//!
//! # Integration
//!
//! This crate integrates with:
//! - **helix-server**: Provides consumer APIs for the gRPC layer.
//! - **helix-tier**: Queries safe eviction boundaries before evicting segments.
//!
//! # Testing
//!
//! The [`SimulatedProgressStore`] provides deterministic fault injection for
//! DST testing:
//!
//! ```ignore
//! use helix_progress::{SimulatedProgressStore, ProgressStoreFaultConfig};
//!
//! // Create store with fault injection.
//! let config = ProgressStoreFaultConfig::flaky(); // 5% failure rate
//! let store = SimulatedProgressStore::with_faults(seed, config);
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
// Allow these for cleaner code in this crate.
#![allow(clippy::module_name_repetitions)]

mod bitmap;
mod error;
mod manager;
mod storage;
mod types;

// Re-export public API.
pub use bitmap::OffsetBitmap;
pub use error::{ProgressError, ProgressResult};
pub use manager::ProgressManager;
pub use storage::{ProgressStore, ProgressStoreFaultConfig, SimulatedProgressStore};
pub use types::{
    AckMode, ConsumerGroupState, ConsumerState, Lease, LeaseState, PartitionKey, PartitionProgress,
    ProgressConfig,
};

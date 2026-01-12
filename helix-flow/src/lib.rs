//! Flow control and I/O scheduling for Helix.
//!
//! This crate provides flow control mechanisms for managing throughput and fairness
//! in the Helix system. It includes:
//!
//! - **Token bucket rate limiting**: Per-stream and global rate limits.
//! - **Weighted fair queue**: I/O scheduling across different operation classes.
//! - **AIMD congestion control**: Adaptive rate adjustment based on latency/errors.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      FlowController                             │
//! │                                                                 │
//! │  ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐  │
//! │  │ Per-Stream   │    │ WeightedFairQueue │    │    AIMD      │  │
//! │  │ TokenBuckets │───▶│  (I/O Scheduler)  │───▶│  Controller  │  │
//! │  └──────────────┘    └──────────────────┘    └──────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # I/O Classes
//!
//! Operations are classified into four I/O classes with different scheduling weights:
//!
//! | Class | Default Weight | Description |
//! |-------|----------------|-------------|
//! | `Write` | 100 | Write operations (high priority) |
//! | `LiveRead` | 100 | Live read operations (high priority) |
//! | `BackfillRead` | 50 | Backfill reads (medium priority) |
//! | `Tiering` | 25 | Background tiering (low priority) |
//!
//! # DST Compatibility
//!
//! All time-dependent operations accept `current_time_us` as a parameter rather than
//! using system time. This allows deterministic simulation testing with controlled time.
//!
//! # Example
//!
//! ```rust
//! use helix_flow::{FlowController, FlowControllerConfig, IoClass};
//!
//! let config = FlowControllerConfig::for_testing();
//! let mut controller = FlowController::new(config, 0);
//!
//! // Submit an operation.
//! controller.submit(
//!     1,                    // stream_id
//!     IoClass::Write,       // I/O class
//!     1024,                 // size in bytes
//!     "my_operation",       // payload
//!     0,                    // current time (microseconds)
//! ).unwrap();
//!
//! // Get next operation to execute.
//! if let Some(op) = controller.next_operation() {
//!     // Execute the operation...
//!     let latency_us = 5000; // 5ms
//!     controller.record_completion(latency_us, true, 5000);
//! }
//! ```

#![warn(missing_docs)]

pub mod aimd;
pub mod controller;
pub mod error;
pub mod fair_queue;
pub mod token_bucket;

// Re-export main types for convenience.
pub use aimd::{AimdConfig, AimdController, AimdState, AimdStats, IntervalMetrics};
pub use controller::{FlowController, FlowControllerConfig, FlowControllerStats, PendingOperation};
pub use error::{FlowError, FlowResult};
pub use fair_queue::{
    FairQueueConfig, FairQueueStats, IoClass, IoOperation, WeightedFairQueue,
};
pub use token_bucket::{TokenBucket, TokenBucketConfig};

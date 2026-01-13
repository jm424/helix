//! Helix Workload: E2E Verifiable Load Testing
//!
//! A unified framework for end-to-end testing of Helix that serves three purposes:
//!
//! 1. **Correctness verification** - Detect lost writes, duplicates, ordering violations
//! 2. **Performance measurement** - Throughput, latency percentiles, bottleneck identification
//! 3. **Fault tolerance validation** - Verify behavior under crashes, partitions, disk failures
//!
//! # Architecture
//!
//! The same workload and verification logic runs in two modes:
//! - **Real mode**: Against actual Helix processes with real I/O
//! - **Simulated mode**: In Bloodhound DST with fault injection
//!
//! # Example
//!
//! ```ignore
//! use helix_workload::{Workload, WorkloadPattern, RealCluster, RealExecutor};
//!
//! let cluster = RealCluster::builder()
//!     .nodes(3)
//!     .build()
//!     .await?;
//!
//! let workload = Workload::builder()
//!     .seed(42)
//!     .pattern(WorkloadPattern::Sequential)
//!     .operations(1000)
//!     .build();
//!
//! let executor = RealExecutor::new(&cluster);
//! let stats = workload.run(&executor).await;
//!
//! assert!(stats.violations.is_empty());
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

mod history;
mod operation;
mod payload;
mod verification;
mod workload;

pub mod executor;

pub use history::History;
pub use operation::{Operation, OperationKind, OperationResult};
pub use payload::{PayloadError, VerifiablePayload, PAYLOAD_SIZE_MAX, PAYLOAD_SIZE_MIN};
pub use verification::{Verification, Violation};
pub use workload::{
    SizeDistribution, TopicConfig, Workload, WorkloadBuilder, WorkloadConfig, WorkloadPattern,
    WorkloadStats,
};

// Re-export executor types.
pub use executor::{
    ExecutorError, RealCluster, RealClusterBuilder, RealClusterConfig, RealExecutor,
    WorkloadExecutor,
};

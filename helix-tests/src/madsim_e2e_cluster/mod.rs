//! E2E Cluster for MadSim Deterministic Simulation Testing.
//!
//! This module provides a test harness that runs REAL `HelixService` instances
//! with their background tasks (tick_task, batcher_task, output_processor_task)
//! under MadSim deterministic simulation.
//!
//! # Producer/Consumer Semantics
//!
//! The test harness implements proper producer/consumer semantics:
//!
//! - **Producer** (`produce()`): Routes to the leader using `NotLeader` hints.
//!   When a node returns `NotLeader` with a `leader_hint`, the producer follows
//!   the hint to find the correct leader. This mirrors real Kafka client behavior.
//!
//! - **Consumer** (`consume()`): Reads from the partition leader for consistency.
//!   The consumer queries Raft state to find the leader and reads from it,
//!   ensuring up-to-date data.
//!
//! # NOTE: Uses KafkaHandler directly (not full TCP socket layer)
//!
//! Currently, `produce()` and `consume()` call the `KafkaHandler` directly,
//! bypassing the TCP socket layer. This tests the core service logic and
//! Kafka protocol handling correctly, but skips the network I/O layer.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                     MadSim Deterministic Runtime                             │
//! └─────────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!         ┌──────────────────────────┼──────────────────────────┐
//!         ▼                          ▼                          ▼
//! ┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
//! │   HelixService    │    │   HelixService    │    │   HelixService    │
//! │   (Node 1)        │    │   (Node 2)        │    │   (Node 3)        │
//! │                   │    │                   │    │                   │
//! │ tick_task      ───┼────┼─ RUNNING AS ──────┼────┼─── BACKGROUND ────│
//! │ batcher_task   ───┼────┼─ SPAWNED TASKS ───┼────┼─── (not manual) ──│
//! │ output_proc    ───┼────┼─ UNDER MADSIM ────┼────┼───────────────────│
//! └────────┬──────────┘    └────────┬──────────┘    └────────┬──────────┘
//!          │                        │                        │
//!          └────────────────────────┼────────────────────────┘
//!                                   ▼
//!               ┌─────────────────────────────────────────────┐
//!               │  SimulatedStorage    │   MadSimTransport    │
//!               │  (fault injection)   │   (partitions)       │
//!               └─────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! #[test]
//! fn test_e2e_produce_consume() {
//!     let rt = madsim::runtime::Runtime::with_seed_and_config(42, Default::default());
//!     rt.block_on(async {
//!         let cluster = E2ECluster::start(3).await;
//!
//!         // Real API calls
//!         cluster.create_topic("orders", 1).await.unwrap();
//!         cluster.produce("orders", 0, b"order-1").await.unwrap();
//!
//!         // Fault injection
//!         cluster.crash_node(NodeId::new(1));
//!
//!         // Verify data
//!         let records = cluster.consume("orders", 0, 0).await.unwrap();
//!         assert_eq!(records.len(), 1);
//!     });
//! }
//! ```

#![cfg(feature = "madsim")]

// Infrastructure modules.
pub mod helpers;
pub mod cluster;
mod verification;
mod fault_injection;
pub(crate) mod concurrent;

// Test modules.
#[cfg(all(test, madsim))]
mod tests;

// Re-exports for public API compatibility.
pub use cluster::{
    E2ECluster, E2EClusterConfig, E2EHelixService, E2EKafkaHandler, E2ENode,
};
pub use helpers::create_test_record_batch;

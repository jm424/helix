//! Helix Core - Strongly-typed identifiers and configuration for Helix.
//!
//! This crate provides Helix-specific types and configuration. It does NOT
//! provide Clock, Network, Storage, or Rng abstractions - those come from
//! the Bloodhound DST framework.
//!
//! # Design Principles (TigerStyle)
//!
//! - **Strongly-typed IDs**: Prevent mixing up NodeId with PartitionId
//! - **Explicit limits**: Every resource has a bounded maximum
//! - **Explicit types**: Use u32/u64, not usize
//! - **No unsafe code**: Safety > Performance

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod limits;
mod types;

pub use error::{Error, Result};
pub use limits::Limits;
pub use types::{
    BatchId, ConsumerGroupId, ConsumerId, GroupId, LeaseId, LogIndex, NodeId, PartitionId,
    ReplicaId, TermId, TopicId,
};

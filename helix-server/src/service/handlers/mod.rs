//! Handler implementations for the Helix service.
//!
//! This module contains all the handler methods for the `HelixService` struct,
//! organized by functionality:
//!
//! - `topic`: Topic creation and management
//! - `write`: Record writing via Raft consensus
//! - `read`: Record reading from partition storage
//! - `metadata`: Cluster and partition metadata queries
//! - `consumer`: Consumer group and lease management
//! - `blob`: Blob storage for Kafka protocol support
//! - `util`: Utility methods (hashing, time, cluster info)

pub mod blob;
mod consumer;
mod metadata;
mod read;
mod topic;
mod util;
mod write;

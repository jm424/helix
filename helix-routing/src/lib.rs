//! Helix Routing - Shard routing and management.
//!
//! This crate provides shard routing infrastructure for Helix, mapping keys
//! to Raft groups and tracking leader locations.
//!
//! # Design (`TigerStyle`)
//!
//! - **Consistent hashing**: Keys map deterministically to shards
//! - **Leader caching**: Reduces leader discovery latency
//! - **Explicit limits**: All resources are bounded
//! - **Transfer protocol**: State machine for shard movement

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod coordinator;
mod leader_cache;
mod router;
mod shard_map;
mod transfer;

pub use coordinator::TransferCoordinator;
pub use leader_cache::{LeaderCache, LeaderCacheConfig};
pub use router::{RouteResult, RoutingError, ShardRouter, ShardRouterConfig};
pub use shard_map::{ShardMap, ShardMapError, ShardRange};
pub use transfer::{
    ShardTransfer, TransferError, TransferMessage, TransferOutput, TransferState,
    MAX_CONCURRENT_TRANSFERS, TRANSFER_TIMEOUT_TICKS,
};

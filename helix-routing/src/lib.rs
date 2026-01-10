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

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod leader_cache;
mod shard_map;
mod router;

pub use leader_cache::{LeaderCache, LeaderCacheConfig};
pub use shard_map::{ShardMap, ShardRange, ShardMapError};
pub use router::{ShardRouter, ShardRouterConfig, RouteResult, RoutingError};

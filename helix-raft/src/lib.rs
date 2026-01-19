//! Helix Raft - Raft consensus implementation.
//!
//! This crate provides a correct, well-tested Raft consensus implementation
//! designed for deterministic simulation testing with Bloodhound.
//!
//! # Design Principles (`TigerStyle`)
//!
//! - **Correctness first**: Matches TLA+ specification exactly
//! - **Deterministic**: All randomness comes from seeded RNG
//! - **Testable**: Designed for Bloodhound simulation from day one
//! - **No unsafe code**: Safety > Performance

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod client;
mod config;
mod log;
mod message;
pub mod multi;
pub mod snapshot;
mod state;
mod storage;
mod wal_storage;

pub use client::{
    ClientCommand, ClientId, ClientQuery, ClientResult, ClientSessions, PendingRequests,
    RaftClient, ReadConsistency, RequestId,
};
pub use config::RaftConfig;
pub use log::{LogEntry, RaftLog};
pub use message::{
    AppendEntriesRequest, AppendEntriesResponse, ClientRequest, InstallSnapshotRequest,
    InstallSnapshotResponse, Message, PreVoteRequest, PreVoteResponse, RequestVoteRequest,
    RequestVoteResponse, TimeoutNowRequest,
};
pub use state::{RaftNode, RaftOutput, RaftState};
pub use snapshot::{Snapshot, SnapshotBuildError, SnapshotBuilder, SnapshotChunk, SnapshotMeta};
pub use storage::{MemoryStorage, PersistentState, RaftStorage, StorageError, StorageResult};
pub use wal_storage::WalStorage;

/// Raft configuration limits.
pub mod limits {
    /// Maximum number of entries in a single `AppendEntries` request.
    pub const APPEND_ENTRIES_BATCH_SIZE_MAX: u32 = 1000;

    /// Maximum number of in-flight `AppendEntries` per follower.
    ///
    /// Similar to Kafka's `max.in.flight.requests.per.connection`.
    /// Pipelining improves throughput by allowing the leader to send
    /// multiple `AppendEntries` without waiting for each response.
    pub const MAX_INFLIGHT_APPEND_ENTRIES: u32 = 5;

    /// Maximum number of nodes in a cluster.
    pub const CLUSTER_SIZE_MAX: usize = 7;

    /// Default election tick (number of ticks before election timeout).
    ///
    /// The actual timeout will be randomized in \[`election_tick`, 2 * `election_tick`).
    /// With a 100ms tick interval, this gives 1-2 second election timeout.
    pub const ELECTION_TICK_DEFAULT: u32 = 10;

    /// Default heartbeat tick (number of ticks between heartbeats).
    /// With a 100ms tick interval, this gives 100ms heartbeat.
    pub const HEARTBEAT_TICK_DEFAULT: u32 = 1;

    /// Minimum election tick (must be > heartbeat tick).
    pub const ELECTION_TICK_MIN: u32 = 2;

    /// Maximum election tick.
    pub const ELECTION_TICK_MAX: u32 = 100;
}

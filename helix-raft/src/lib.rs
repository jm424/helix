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
    AppendEntriesRequest, AppendEntriesResponse, ClientRequest, Message, PreVoteRequest,
    PreVoteResponse, RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};
pub use state::{RaftNode, RaftOutput, RaftState};
pub use storage::{MemoryStorage, PersistentState, RaftStorage, StorageError, StorageResult};
pub use wal_storage::WalStorage;

/// Raft configuration limits.
pub mod limits {
    /// Maximum number of entries in a single `AppendEntries` request.
    pub const APPEND_ENTRIES_BATCH_SIZE_MAX: u32 = 1000;

    /// Maximum time to wait for election timeout (microseconds).
    /// Note: Default is 500ms, but tests may use longer values for faster simulation.
    pub const ELECTION_TIMEOUT_US_MAX: u64 = 2_000_000; // 2s (allows longer for testing)

    /// Minimum time to wait for election timeout (microseconds).
    pub const ELECTION_TIMEOUT_US_MIN: u64 = 150_000; // 150ms

    /// Heartbeat interval (microseconds).
    pub const HEARTBEAT_INTERVAL_US: u64 = 50_000; // 50ms

    /// Maximum number of nodes in a cluster.
    pub const CLUSTER_SIZE_MAX: usize = 7;
}

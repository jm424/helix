//! Helix Tests - Bloodhound simulation tests for Helix.
//!
//! This crate contains deterministic simulation tests using the Bloodhound
//! framework. Tests are organized by component:
//!
//! - `raft_actor`: Raft SimulatedActor for Bloodhound simulation
//! - `raft_tests`: Raft consensus safety and liveness properties
//! - `wal_tests`: Write-ahead log crash recovery and integrity
//! - `properties`: Property definitions (SingleLeaderPerTerm, LogMatching, etc.)
//! - `scenarios`: Reusable test scenarios

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

pub mod properties;
pub mod raft_actor;
pub mod scenarios;

// Test modules (only compiled for tests).
#[cfg(test)]
mod raft_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod wal_tests;

//! Helix Tests - Deterministic Simulation Testing for Helix.
//!
//! This crate contains all DST (Deterministic Simulation Testing) and integration
//! tests for Helix. Tests are organized by component and type:
//!
//! ## Test Organization
//!
//! **DST Tests** (`*_dst.rs`): Deterministic simulation with fault injection
//! - `actor_mode_dst`: Actor-mode partition architecture DST
//! - `helix_service_dst`: Full Helix service E2E DST
//! - `raft_dst`: Raft consensus DST with Bloodhound simulation
//! - `wal_dst`: Per-partition WAL DST with `SimulatedStorage`
//! - `shared_wal_dst`: `SharedWal` DST with `SimulatedStorage`
//!
//! **Integration Tests** (`*_tests.rs`): Multi-component integration
//! - `raft_tests`: Raft consensus safety and liveness properties
//! - `multi_raft_tests`: Multi-Raft group tests
//! - `server_tests`: gRPC server integration tests
//! - `tier_tests`: Tiered storage tests
//! - `progress_tests`: Progress tracking tests
//!
//! **Support Modules**:
//! - `raft_actor`: Raft `SimulatedActor` for Bloodhound simulation
//! - `helix_service_actor`: Helix service `SimulatedActor` for E2E DST
//! - `properties`: Property definitions (`SingleLeaderPerTerm`, `LogMatching`, etc.)
//! - `scenarios`: Reusable test scenarios
//!
//! ## Naming Conventions
//!
//! - DST tests: `test_dst_<component>_<scenario>`
//! - Integration tests: `test_<component>_<scenario>`
//! - Unit tests: Inline in each crate under `#[cfg(test)]`

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::format_collect)]
#![allow(clippy::needless_update)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::too_many_lines)]

#[cfg(test)]
use std::sync::OnceLock;

#[cfg(test)]
/// Initialize tracing for tests (no-op if already initialized).
pub(crate) fn init_test_tracing() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .try_init();
    });
}

pub mod helix_service_actor;
pub mod properties;
pub mod raft_actor;
pub mod scenarios;
pub mod simulated_transport;

// MadSim modules (requires madsim feature).
#[cfg(feature = "madsim")]
pub mod madsim_e2e_cluster;
#[cfg(feature = "madsim")]
pub mod madsim_scenarios;
#[cfg(feature = "madsim")]
pub mod madsim_transport;

// DST test modules (deterministic simulation with fault injection).
#[cfg(test)]
mod helix_service_dst;
#[cfg(test)]
mod raft_dst;
#[cfg(test)]
mod shared_wal_dst;
#[cfg(test)]
mod wal_dst;

// Integration test modules (multi-component tests).
#[cfg(test)]
mod multi_raft_tests;
#[cfg(test)]
mod multi_raft_verified_tests;
#[cfg(test)]
mod progress_tests;
#[cfg(test)]
mod raft_tests;
#[cfg(test)]
mod server_tests;
#[cfg(test)]
mod shard_transfer_tests;
#[cfg(test)]
mod shared_wal_integration_tests;
#[cfg(test)]
mod tier_tests;

// MadSim E2E DST tests.
// Run with: RUSTFLAGS="--cfg madsim" cargo test -p helix-tests --features madsim madsim_e2e_cluster

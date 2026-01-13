//! Kafka wire protocol compatibility layer for Helix.
//!
//! This crate implements a subset of the Kafka wire protocol, allowing
//! existing Kafka clients to connect to Helix without code changes.
//!
//! # Supported APIs
//!
//! | API Key | Name | Status |
//! |---------|------|--------|
//! | 0 | `Produce` | Implemented |
//! | 1 | `Fetch` | Implemented |
//! | 2 | `ListOffsets` | Implemented |
//! | 3 | `Metadata` | Implemented |
//! | 8 | `OffsetCommit` | Implemented |
//! | 9 | `OffsetFetch` | Implemented |
//! | 10 | `FindCoordinator` | Implemented |
//! | 18 | `ApiVersions` | Implemented |
//!
//! # Architecture
//!
//! ```text
//! Kafka Client ──── Kafka Protocol ────► KafkaServer ────► HelixServer
//!                       :9092                │
//!                                            ▼
//!                                    DurablePartition
//!                                    ProgressManager
//! ```

#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

pub mod error;
pub mod server;
pub mod handler;
pub mod codec;

pub use error::{KafkaCompatError, KafkaCompatResult};
pub use server::{KafkaServer, KafkaServerConfig};

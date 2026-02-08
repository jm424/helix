//! Kafka wire protocol support for helix-server.
//!
//! This module provides Kafka protocol compatibility, allowing Kafka clients
//! to connect to Helix without code changes. Uses the same MultiRaft storage
//! as the gRPC API.
//!
//! # Architecture
//!
//! ```text
//! Kafka Client ──► KafkaServer ──► KafkaHandler ──► HelixService ──► MultiRaft
//! ```

// Codec and handler are pure protocol handling - available under madsim for DST.
// Only the TCP server requires real networking and is excluded under madsim.
pub mod codec;
mod error;
mod handler;
// Producer info extraction is always available (doesn't depend on TCP).
mod producer_info;
#[cfg(not(madsim))]
mod server;

pub use error::{KafkaError, KafkaResult};
pub use handler::KafkaHandler;
// ProducerInfo and extract_producer_info are available under madsim.
pub use producer_info::{extract_producer_info, ProducerInfo};
#[cfg(not(madsim))]
pub use server::{KafkaServer, KafkaServerConfig};

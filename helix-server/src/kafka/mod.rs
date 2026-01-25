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

mod codec;
mod error;
mod handler;
mod server;

pub use error::{KafkaError, KafkaResult};
pub use handler::{extract_producer_info, KafkaHandler, ProducerInfo};
pub use server::{KafkaServer, KafkaServerConfig};

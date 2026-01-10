//! Helix Partition - Kafka-style log partition management.
//!
//! This crate provides the partition abstraction for storing and retrieving
//! records in a Kafka-like manner. Each partition is an append-only log
//! backed by WAL segments.
//!
//! # Design
//!
//! A partition consists of:
//! - One or more log segments (WAL files)
//! - An index for fast offset lookups
//! - Metadata tracking (high watermark, leader epoch, etc.)
//!
//! # Operations
//!
//! - `append`: Add records to the end of the log
//! - `read`: Fetch records starting from an offset
//! - `truncate`: Remove records after a given offset (for replication)
//!
//! # `TigerStyle` Principles
//!
//! - Explicit limits on segment size and record count
//! - CRC checksums on all data
//! - No unsafe code

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod consumer;
mod error;
mod log;
mod partition;
mod producer;
mod replication;

pub use consumer::{AutoOffsetReset, Consumer, ConsumerConfig, FetchResult, OffsetStore};
pub use error::{PartitionError, PartitionResult};
pub use log::{LogSegment, SegmentConfig, SegmentReader};
pub use partition::{Partition, PartitionConfig, PartitionMetadata};
pub use producer::{Acks, Partitioner, ProduceResult, Producer, ProducerConfig};
pub use replication::{
    ApplyResult, PartitionCommand, ReplicatedPartition, ReplicatedPartitionConfig,
    ReplicationManager, ReplicationState,
};

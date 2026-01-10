//! Helix Runtime - Production runtime implementations.
//!
//! This crate provides production implementations for running Raft nodes
//! using real system resources (actual time, TCP sockets, file I/O).
//!
//! # Overview
//!
//! The runtime provides:
//! - `ServerConfig`: Configuration for a Raft server node
//! - `RaftServer`: The main server that runs a Raft node
//! - `ServerHandle`: Handle for interacting with a running server
//!
//! # Example
//!
//! ```ignore
//! use helix_runtime::{ServerConfig, RaftServer};
//! use helix_core::NodeId;
//!
//! let config = ServerConfig::new(
//!     NodeId::new(1),
//!     "127.0.0.1:8001".parse().unwrap(),
//!     "127.0.0.1:9001".parse().unwrap(),
//! );
//!
//! let (events_tx, events_rx) = tokio::sync::mpsc::channel(16);
//! let server = RaftServer::new(config, events_tx);
//! let (handle, server_future) = server.run();
//!
//! // Spawn the server and use handle to interact with it.
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod config;
mod server;

pub use config::{ConfigError, PeerConfig, ServerConfig, TimingConfig};
pub use server::{OutgoingMessage, RaftServer, ServerCommand, ServerError, ServerEvent, ServerHandle};

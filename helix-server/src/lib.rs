//! Helix distributed log server.
//!
//! This crate provides the gRPC server implementation for Helix,
//! exposing the Write, Read, and Metadata APIs to clients.

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod error;
mod service;

pub mod generated {
    //! Generated protobuf types.
    include!("generated/helix.v1.rs");
}

pub use error::{ServerError, ServerResult};
pub use service::HelixService;

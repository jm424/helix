//! Helix Runtime - Production runtime implementations.
//!
//! This crate provides production implementations of the core traits
//! using real system resources (actual time, TCP sockets, file I/O).

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

// TODO: Implement production Clock, Network, Storage, Rng

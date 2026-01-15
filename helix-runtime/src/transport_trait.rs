//! Transport trait abstraction for Raft peer communication.
//!
//! This module defines a trait for transport operations, allowing different
//! implementations for production (TCP) and simulation (Bloodhound DST).
//!
//! # Design
//!
//! The trait abstracts the core transport operations:
//! - Sending batches of Raft messages to peers
//! - Sending broker heartbeats for liveness tracking
//!
//! Production code uses [`TransportHandle`] (TCP-based).
//! DST uses `SimulatedTransport` (event-based via Bloodhound).

use async_trait::async_trait;
use helix_core::NodeId;
use helix_raft::multi::GroupMessage;

use crate::codec::BrokerHeartbeat;
use crate::transport::TransportResult;

/// Trait for transport operations between Raft nodes.
///
/// This trait enables abstraction over different transport implementations:
/// - [`TransportHandle`]: Production TCP transport
/// - `SimulatedTransport`: DST transport using Bloodhound events
///
/// # Implementation Notes
///
/// Implementations must be `Send + Sync` for use across async tasks.
/// The `Clone` bound enables sharing handles across multiple tasks.
#[async_trait]
pub trait TransportService: Send + Sync + Clone + 'static {
    /// Sends a batch of `GroupMessage`s to a peer.
    ///
    /// This is the primary send method for Multi-Raft, which batches messages
    /// by destination node for efficiency.
    ///
    /// # Arguments
    ///
    /// * `to` - The destination node ID
    /// * `messages` - The batch of group messages to send
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is unknown, the send queue is full, or
    /// the underlying transport fails.
    async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()>;

    /// Sends a broker heartbeat to a peer.
    ///
    /// Heartbeats are soft-state messages (not Raft-replicated) used to track
    /// broker liveness. Each node sends heartbeats to all peers.
    ///
    /// # Arguments
    ///
    /// * `to` - The destination node ID
    /// * `heartbeat` - The heartbeat message
    ///
    /// # Errors
    ///
    /// Returns an error if the peer is unknown, the send queue is full, or
    /// the underlying transport fails.
    async fn send_heartbeat(&self, to: NodeId, heartbeat: &BrokerHeartbeat) -> TransportResult<()>;

    /// Returns the node ID of this transport.
    fn node_id(&self) -> NodeId;
}

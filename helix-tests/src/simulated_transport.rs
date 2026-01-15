//! Simulated transport for E2E Deterministic Simulation Testing.
//!
//! This module provides a transport implementation that works with Bloodhound's
//! simulation framework, scheduling message delivery via `PacketDelivery` events.
//!
//! # Design
//!
//! Unlike the production `TransportHandle` which uses TCP sockets, this transport
//! queues messages for later scheduling by the owning `SimulatedActor`. The actor
//! drains the queue and schedules `PacketDelivery` events through the simulation
//! context.
//!
//! This design allows:
//! - Deterministic message ordering via Bloodhound's event queue
//! - Network partition simulation via `SharedNetworkState`
//! - Configurable latency for realistic timing behavior

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bloodhound::simulation::discrete::actors::SimulationContext;
use bloodhound::simulation::discrete::event::{ActorId, EventKind};
use helix_core::NodeId;
use helix_raft::multi::GroupMessage;
use helix_runtime::{
    encode_broker_heartbeat, encode_group_batch, BrokerHeartbeat, TransportResult, TransportService,
};

use crate::raft_actor::SharedNetworkState;

/// Network latency for simulated message delivery (in microseconds).
const NETWORK_LATENCY_US: u64 = 1_000; // 1ms

/// Message type tags for simulation.
mod message_tags {
    /// Tag for batched group messages.
    pub const GROUP_BATCH: u8 = 1;
    /// Tag for broker heartbeats.
    pub const HEARTBEAT: u8 = 2;
}

/// A queued message waiting to be scheduled.
#[derive(Debug, Clone)]
pub struct QueuedMessage {
    /// Destination actor ID.
    pub to_actor: ActorId,
    /// Serialized payload.
    pub payload: Vec<u8>,
}

/// Simulated transport for Bloodhound DST.
///
/// This transport queues messages instead of sending them over TCP.
/// The owning `SimulatedActor` drains the queue and schedules events.
#[derive(Clone)]
pub struct SimulatedTransport {
    /// This node's ID.
    node_id: NodeId,
    /// This node's actor ID.
    actor_id: ActorId,
    /// Mapping from `NodeId` to `ActorId`.
    node_to_actor: BTreeMap<NodeId, ActorId>,
    /// Queued messages waiting to be scheduled.
    pending_messages: Arc<Mutex<Vec<QueuedMessage>>>,
    /// Network state for partition simulation.
    network_state: SharedNetworkState,
}

impl SimulatedTransport {
    /// Creates a new simulated transport.
    ///
    /// # Arguments
    ///
    /// * `node_id` - This node's Helix node ID
    /// * `actor_id` - This node's Bloodhound actor ID
    /// * `node_to_actor` - Mapping from Helix node IDs to Bloodhound actor IDs
    /// * `network_state` - Shared network state for partition tracking
    #[must_use]
    pub fn new(
        node_id: NodeId,
        actor_id: ActorId,
        node_to_actor: BTreeMap<NodeId, ActorId>,
        network_state: SharedNetworkState,
    ) -> Self {
        Self {
            node_id,
            actor_id,
            node_to_actor,
            pending_messages: Arc::new(Mutex::new(Vec::new())),
            network_state,
        }
    }

    /// Drains the pending message queue and schedules events.
    ///
    /// This should be called by the owning `SimulatedActor` during event handling
    /// to actually schedule the message deliveries.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The simulation context for scheduling events
    pub fn drain_and_schedule(&self, ctx: &mut SimulationContext) {
        let messages = {
            let mut guard = self.pending_messages.lock().expect("lock poisoned");
            std::mem::take(&mut *guard)
        };

        for msg in messages {
            // Check for network partition before scheduling.
            let is_partitioned = {
                let state = self.network_state.lock().expect("lock poisoned");
                state.is_partitioned(self.actor_id, msg.to_actor)
            };

            if is_partitioned {
                tracing::trace!(
                    from = %self.actor_id,
                    to = %msg.to_actor,
                    "message dropped due to partition"
                );
                continue;
            }

            ctx.schedule_after(
                Duration::from_micros(NETWORK_LATENCY_US),
                EventKind::PacketDelivery {
                    from: self.actor_id,
                    to: msg.to_actor,
                    payload: msg.payload,
                },
            );
        }
    }

    /// Returns the number of pending messages.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending_messages.lock().expect("lock poisoned").len()
    }

    /// Clears all pending messages (useful after crash simulation).
    pub fn clear_pending(&self) {
        self.pending_messages.lock().expect("lock poisoned").clear();
    }

    /// Queues a batch of messages synchronously.
    ///
    /// This is a sync wrapper around the async `send_batch` for use in
    /// simulation contexts where blocking is acceptable.
    pub fn queue_batch(&self, to: NodeId, messages: Vec<GroupMessage>) {
        if messages.is_empty() {
            return;
        }

        // Look up the destination actor ID.
        let to_actor = match self.node_to_actor.get(&to) {
            Some(&actor) => actor,
            None => {
                tracing::warn!(to = to.get(), "unknown destination node");
                return;
            }
        };

        // Encode the batch.
        let Ok(encoded) = encode_group_batch(&messages) else {
            tracing::warn!("failed to encode group batch");
            return;
        };

        // Create tagged payload.
        let mut payload = Vec::with_capacity(1 + encoded.len());
        payload.push(message_tags::GROUP_BATCH);
        payload.extend_from_slice(&encoded);

        // Queue the message.
        self.pending_messages
            .lock()
            .expect("lock poisoned")
            .push(QueuedMessage { to_actor, payload });
    }

    /// Queues a heartbeat message synchronously.
    ///
    /// This is a sync wrapper for use in simulation contexts.
    pub fn queue_heartbeat(&self, to: NodeId, heartbeat: &BrokerHeartbeat) {
        // Look up the destination actor ID.
        let to_actor = match self.node_to_actor.get(&to) {
            Some(&actor) => actor,
            None => {
                tracing::warn!(to = to.get(), "unknown destination node for heartbeat");
                return;
            }
        };

        // Encode the heartbeat.
        let Ok(encoded) = encode_broker_heartbeat(heartbeat) else {
            tracing::warn!("failed to encode heartbeat");
            return;
        };

        // Create tagged payload.
        let mut payload = Vec::with_capacity(1 + encoded.len());
        payload.push(message_tags::HEARTBEAT);
        payload.extend_from_slice(&encoded);

        // Queue the message.
        self.pending_messages
            .lock()
            .expect("lock poisoned")
            .push(QueuedMessage { to_actor, payload });
    }
}

#[async_trait]
impl TransportService for SimulatedTransport {
    async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()> {
        if messages.is_empty() {
            return Ok(());
        }

        // Look up the destination actor ID.
        let to_actor = match self.node_to_actor.get(&to) {
            Some(&actor) => actor,
            None => {
                tracing::warn!(to = to.get(), "unknown destination node");
                return Ok(()); // Drop messages to unknown nodes (matches production behavior).
            }
        };

        // Encode the batch.
        let encoded = encode_group_batch(&messages)?;

        // Create tagged payload.
        let mut payload = Vec::with_capacity(1 + encoded.len());
        payload.push(message_tags::GROUP_BATCH);
        payload.extend_from_slice(&encoded);

        // Queue the message.
        self.pending_messages
            .lock()
            .expect("lock poisoned")
            .push(QueuedMessage { to_actor, payload });

        Ok(())
    }

    async fn send_heartbeat(&self, to: NodeId, heartbeat: &BrokerHeartbeat) -> TransportResult<()> {
        // Look up the destination actor ID.
        let to_actor = match self.node_to_actor.get(&to) {
            Some(&actor) => actor,
            None => {
                tracing::warn!(to = to.get(), "unknown destination node for heartbeat");
                return Ok(()); // Drop heartbeats to unknown nodes.
            }
        };

        // Encode the heartbeat.
        let encoded = encode_broker_heartbeat(heartbeat)?;

        // Create tagged payload.
        let mut payload = Vec::with_capacity(1 + encoded.len());
        payload.push(message_tags::HEARTBEAT);
        payload.extend_from_slice(&encoded);

        // Queue the message.
        self.pending_messages
            .lock()
            .expect("lock poisoned")
            .push(QueuedMessage { to_actor, payload });

        Ok(())
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Decodes a simulated transport message.
///
/// Returns the message tag and the payload without the tag byte.
#[must_use]
pub fn decode_simulated_message(data: &[u8]) -> Option<(u8, &[u8])> {
    if data.is_empty() {
        return None;
    }
    Some((data[0], &data[1..]))
}

/// Checks if the message is a group batch.
#[must_use]
pub fn is_group_batch(tag: u8) -> bool {
    tag == message_tags::GROUP_BATCH
}

/// Checks if the message is a heartbeat.
#[must_use]
pub fn is_heartbeat(tag: u8) -> bool {
    tag == message_tags::HEARTBEAT
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_actor::NetworkState;
    use helix_core::GroupId;
    use helix_raft::Message;

    fn create_test_transport() -> SimulatedTransport {
        let node_id = NodeId::new(1);
        let actor_id = ActorId::new(1);
        let mut node_to_actor = BTreeMap::new();
        node_to_actor.insert(NodeId::new(1), ActorId::new(1));
        node_to_actor.insert(NodeId::new(2), ActorId::new(2));
        node_to_actor.insert(NodeId::new(3), ActorId::new(3));
        let network_state = Arc::new(Mutex::new(NetworkState::new()));

        SimulatedTransport::new(node_id, actor_id, node_to_actor, network_state)
    }

    #[tokio::test]
    async fn test_send_batch_queues_message() {
        let transport = create_test_transport();

        // Create a simple message batch.
        let msg = Message::RequestVote(helix_raft::RequestVoteRequest::new(
            helix_core::TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            helix_core::LogIndex::new(0),
            helix_core::TermId::new(0),
        ));
        let batch = vec![GroupMessage::new(GroupId::new(1), msg)];

        // Send should queue the message.
        assert_eq!(transport.pending_count(), 0);
        transport.send_batch(NodeId::new(2), batch).await.unwrap();
        assert_eq!(transport.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_send_empty_batch_does_nothing() {
        let transport = create_test_transport();

        transport.send_batch(NodeId::new(2), vec![]).await.unwrap();
        assert_eq!(transport.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_send_to_unknown_node() {
        let transport = create_test_transport();

        // Send to unknown node should succeed but not queue.
        let msg = Message::RequestVote(helix_raft::RequestVoteRequest::new(
            helix_core::TermId::new(1),
            NodeId::new(1),
            NodeId::new(99), // Unknown node.
            helix_core::LogIndex::new(0),
            helix_core::TermId::new(0),
        ));
        let batch = vec![GroupMessage::new(GroupId::new(1), msg)];

        transport.send_batch(NodeId::new(99), batch).await.unwrap();
        assert_eq!(transport.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_clear_pending() {
        let transport = create_test_transport();

        let msg = Message::RequestVote(helix_raft::RequestVoteRequest::new(
            helix_core::TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            helix_core::LogIndex::new(0),
            helix_core::TermId::new(0),
        ));
        let batch = vec![GroupMessage::new(GroupId::new(1), msg)];

        transport.send_batch(NodeId::new(2), batch).await.unwrap();
        assert_eq!(transport.pending_count(), 1);

        transport.clear_pending();
        assert_eq!(transport.pending_count(), 0);
    }

    #[test]
    fn test_transport_is_clone() {
        let transport = create_test_transport();
        let _cloned = transport.clone();
    }

    #[test]
    fn test_node_id() {
        let transport = create_test_transport();
        assert_eq!(transport.node_id(), NodeId::new(1));
    }
}

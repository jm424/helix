//! MadSim-compatible transport implementing the `TransportService` trait.
//!
//! This module provides a deterministic transport for use with MadSim that
//! implements the production `TransportService` trait, enabling real
//! `HelixService` code to run under deterministic simulation.
//!
//! # Design
//!
//! The transport uses MadSim's task spawning and time simulation to deliver
//! messages with deterministic latency. Network partitions and node crashes
//! are simulated via `MadSimNetworkState`.
//!
//! # Usage
//!
//! ```ignore
//! let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));
//! let (mailboxes, receivers) = create_cluster_mailboxes(&node_ids, 1000);
//! let transport = MadSimTransport::new(node_id, network_state, mailboxes);
//!
//! // Use with HelixService<SimulatedStorage, MadSimTransport>
//! ```

#![cfg(feature = "madsim")]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use helix_core::NodeId;
use helix_raft::multi::GroupMessage;
use helix_runtime::{
    decode_broker_heartbeat, decode_group_batch, encode_broker_heartbeat, encode_group_batch,
    BrokerHeartbeat, TransportResult, TransportService,
};
use tokio::sync::mpsc;

// ============================================================================
// Network State
// ============================================================================

/// Network state for MadSim transport, tracking partitions between nodes.
///
/// This is adapted from the Bloodhound version but uses `NodeId` directly
/// instead of `ActorId`.
#[derive(Debug, Default)]
pub struct MadSimNetworkState {
    /// Set of partitioned node pairs. If (a, b) is in the set, messages
    /// from a to b are dropped. Partitions are bidirectional.
    partitioned_pairs: std::collections::BTreeSet<(NodeId, NodeId)>,
    /// Set of currently crashed nodes.
    crashed_nodes: std::collections::BTreeSet<NodeId>,
    /// Per-node latency multiplier. When set, messages TO this node have
    /// their delivery latency multiplied by this factor. Used by the
    /// `SlowFollower` fault scenario.
    node_latency_multiplier: BTreeMap<NodeId, u32>,
    /// Global latency multiplier applied to ALL messages. Used by the
    /// concurrent DST to inflate delivery time so faults injected while
    /// messages are in-flight (sleeping) are visible at delivery time.
    /// Default 1 (no effect).
    global_latency_multiplier: u32,
}

impl MadSimNetworkState {
    /// Creates a new network state with no partitions.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Partitions the given nodes from each other.
    ///
    /// Messages between any pair of nodes in the list will be dropped.
    pub fn partition(&mut self, nodes: &[NodeId]) {
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                self.partitioned_pairs.insert((nodes[i], nodes[j]));
                self.partitioned_pairs.insert((nodes[j], nodes[i]));
            }
        }
    }

    /// Heals a partition between the given nodes.
    pub fn heal(&mut self, nodes: &[NodeId]) {
        for i in 0..nodes.len() {
            for j in (i + 1)..nodes.len() {
                self.partitioned_pairs.remove(&(nodes[i], nodes[j]));
                self.partitioned_pairs.remove(&(nodes[j], nodes[i]));
            }
        }
    }

    /// Checks if two nodes are partitioned from each other.
    #[must_use]
    pub fn is_partitioned(&self, from: NodeId, to: NodeId) -> bool {
        self.partitioned_pairs.contains(&(from, to))
            || self.crashed_nodes.contains(&from)
            || self.crashed_nodes.contains(&to)
    }

    /// Marks a node as crashed.
    pub fn crash_node(&mut self, node: NodeId) {
        self.crashed_nodes.insert(node);
    }

    /// Marks a node as recovered.
    pub fn recover_node(&mut self, node: NodeId) {
        self.crashed_nodes.remove(&node);
    }

    /// Returns whether a node is crashed.
    #[must_use]
    pub fn is_crashed(&self, node: NodeId) -> bool {
        self.crashed_nodes.contains(&node)
    }

    /// Sets the latency multiplier for a specific node.
    ///
    /// Messages delivered TO this node will have their latency multiplied
    /// by this factor. A multiplier of 1 is normal latency.
    /// Pass 0 or 1 to clear the multiplier.
    pub fn set_node_latency(&mut self, node: NodeId, multiplier: u32) {
        if multiplier <= 1 {
            self.node_latency_multiplier.remove(&node);
        } else {
            self.node_latency_multiplier.insert(node, multiplier);
        }
    }

    /// Returns the latency multiplier for a destination node.
    ///
    /// Returns 1 if no multiplier is set (normal latency).
    #[must_use]
    pub fn get_latency_multiplier(&self, destination: NodeId) -> u32 {
        self.node_latency_multiplier
            .get(&destination)
            .copied()
            .unwrap_or(1)
    }

    /// Sets the global latency multiplier applied to ALL messages.
    ///
    /// This inflates `NETWORK_LATENCY` for every message, giving fault
    /// injection tasks time to modify network state while messages are
    /// in-flight (sleeping). The transport checks partition/crash state
    /// AFTER the sleep, so faults injected during the sleep window are
    /// visible at delivery time.
    ///
    /// Pass 0 or 1 to disable.
    pub fn set_global_latency_multiplier(&mut self, multiplier: u32) {
        self.global_latency_multiplier = multiplier;
    }

    /// Returns the effective latency for a message to a destination node.
    ///
    /// Combines the global multiplier with the per-node multiplier.
    #[must_use]
    pub fn effective_latency(&self, destination: NodeId) -> Duration {
        let node_mult = self.get_latency_multiplier(destination);
        let global_mult = self.global_latency_multiplier.max(1);
        NETWORK_LATENCY * node_mult * global_mult
    }
}

/// Shared network state handle for MadSim.
pub type SharedMadSimNetworkState = Arc<Mutex<MadSimNetworkState>>;

// ============================================================================
// Message Types
// ============================================================================

/// Network latency for simulated message delivery.
const NETWORK_LATENCY: Duration = Duration::from_millis(1);

/// A Raft message delivered through the MadSim transport.
#[derive(Debug, Clone)]
pub struct RaftMessage {
    /// Source node.
    pub from: NodeId,
    /// Batch of group messages.
    pub messages: Vec<GroupMessage>,
}

/// A heartbeat message delivered through the MadSim transport.
#[derive(Debug, Clone)]
pub struct HeartbeatMessage {
    /// The heartbeat payload.
    pub heartbeat: BrokerHeartbeat,
}

/// Incoming message type for nodes.
#[derive(Debug, Clone)]
pub enum IncomingMessage {
    /// Raft message batch.
    Raft(RaftMessage),
    /// Broker heartbeat.
    Heartbeat(HeartbeatMessage),
}

/// Mailbox for receiving messages at a node.
pub type NodeMailbox = mpsc::Sender<IncomingMessage>;
/// Receiver end of a node's mailbox.
pub type NodeMailboxReceiver = mpsc::Receiver<IncomingMessage>;

/// Collection of all node mailboxes in the cluster.
pub type NodeMailboxes = BTreeMap<NodeId, NodeMailbox>;

/// Shared mailbox collection that supports replacing individual node mailboxes.
///
/// When `restart_node()` creates a new mailbox for a restarted node, existing
/// transports on other nodes automatically route messages to the new mailbox
/// because they hold `Arc<SharedNodeMailboxes>` and look up the mailbox at
/// send time.
#[derive(Debug)]
pub struct SharedNodeMailboxes {
    inner: Mutex<BTreeMap<NodeId, NodeMailbox>>,
}

impl SharedNodeMailboxes {
    /// Creates a new shared mailbox collection from existing mailboxes.
    #[must_use]
    pub const fn new(mailboxes: BTreeMap<NodeId, NodeMailbox>) -> Self {
        Self {
            inner: Mutex::new(mailboxes),
        }
    }

    /// Gets a cloned sender for a specific node.
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    #[must_use]
    pub fn get(&self, node_id: &NodeId) -> Option<NodeMailbox> {
        self.inner
            .lock()
            .expect("lock poisoned")
            .get(node_id)
            .cloned()
    }

    /// Replaces the mailbox for a node (used during `restart_node`).
    ///
    /// # Panics
    ///
    /// Panics if the internal mutex is poisoned.
    pub fn replace(&self, node_id: NodeId, mailbox: NodeMailbox) {
        self.inner
            .lock()
            .expect("lock poisoned")
            .insert(node_id, mailbox);
    }
}

// ============================================================================
// MadSim Transport
// ============================================================================

/// MadSim-compatible transport that implements `TransportService`.
///
/// This transport uses MadSim's deterministic task spawning to deliver
/// messages with simulated network latency. Network partitions cause
/// messages to be dropped.
///
/// Uses `SharedNodeMailboxes` so that `restart_node()` can swap in a new
/// mailbox sender while existing transports on other nodes automatically
/// route to the new mailbox.
#[derive(Clone)]
pub struct MadSimTransport {
    /// This node's ID.
    node_id: NodeId,
    /// Network state for partition simulation.
    network_state: SharedMadSimNetworkState,
    /// Shared mailboxes for all nodes in the cluster.
    shared_mailboxes: Arc<SharedNodeMailboxes>,
}

impl MadSimTransport {
    /// Creates a new MadSim transport with shared mailboxes.
    #[must_use]
    pub const fn new(
        node_id: NodeId,
        network_state: SharedMadSimNetworkState,
        shared_mailboxes: Arc<SharedNodeMailboxes>,
    ) -> Self {
        Self {
            node_id,
            network_state,
            shared_mailboxes,
        }
    }
}

#[async_trait]
impl TransportService for MadSimTransport {
    async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()> {
        let from = self.node_id;
        let network_state = self.network_state.clone();
        let shared_mailboxes = self.shared_mailboxes.clone();

        // Serialize/deserialize round-trip to test the codec layer.
        // This ensures the MadSim transport exercises the same wire format as production.
        let encoded = match encode_group_batch(&messages) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    from = from.get(),
                    to = to.get(),
                    error = %e,
                    "Failed to encode batch, dropping message"
                );
                return Ok(()); // Drop the message (simulates corruption).
            }
        };

        let decoded_messages = match decode_group_batch(&encoded) {
            Ok((msgs, _consumed)) => msgs,
            Err(e) => {
                tracing::warn!(
                    from = from.get(),
                    to = to.get(),
                    error = %e,
                    "Failed to decode batch, dropping message"
                );
                return Ok(()); // Drop the message (simulates corruption).
            }
        };

        // Spawn an async task that delivers after network latency.
        madsim::task::spawn(async move {
            // Simulate network latency (global + per-node multipliers).
            // Faults injected while this sleep is active will be visible
            // at delivery time (the partition/crash check runs AFTER sleep).
            let latency = {
                let state = network_state.lock().expect("lock poisoned");
                state.effective_latency(to)
            };
            madsim::time::sleep(latency).await;

            // Check for partition at delivery time (not send time).
            let is_partitioned = {
                let state = network_state.lock().expect("lock poisoned");
                state.is_partitioned(from, to)
            };

            if is_partitioned {
                tracing::trace!(
                    from = from.get(),
                    to = to.get(),
                    "Raft batch dropped due to partition"
                );
                return;
            }

            // Deliver the decoded messages to target mailbox.
            if let Some(mailbox) = shared_mailboxes.get(&to) {
                let msg = IncomingMessage::Raft(RaftMessage {
                    from,
                    messages: decoded_messages,
                });
                // Ignore send errors (receiver might be dropped).
                let _ = mailbox.send(msg).await;
            }
        });

        Ok(())
    }

    async fn send_heartbeat(&self, to: NodeId, heartbeat: &BrokerHeartbeat) -> TransportResult<()> {
        let from = self.node_id;
        let network_state = self.network_state.clone();
        let shared_mailboxes = self.shared_mailboxes.clone();

        // Serialize/deserialize round-trip to test the codec layer.
        let encoded = match encode_broker_heartbeat(heartbeat) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(
                    from = from.get(),
                    to = to.get(),
                    error = %e,
                    "Failed to encode heartbeat, dropping message"
                );
                return Ok(()); // Drop the message (simulates corruption).
            }
        };

        let decoded_heartbeat = match decode_broker_heartbeat(&encoded) {
            Ok((hb, _consumed)) => hb,
            Err(e) => {
                tracing::warn!(
                    from = from.get(),
                    to = to.get(),
                    error = %e,
                    "Failed to decode heartbeat, dropping message"
                );
                return Ok(()); // Drop the message (simulates corruption).
            }
        };

        // Spawn an async task that delivers after network latency.
        madsim::task::spawn(async move {
            // Simulate network latency (global + per-node multipliers).
            // Faults injected while this sleep is active will be visible
            // at delivery time (the partition/crash check runs AFTER sleep).
            let latency = {
                let state = network_state.lock().expect("lock poisoned");
                state.effective_latency(to)
            };
            madsim::time::sleep(latency).await;

            // Check for partition at delivery time.
            let is_partitioned = {
                let state = network_state.lock().expect("lock poisoned");
                state.is_partitioned(from, to)
            };

            if is_partitioned {
                tracing::trace!(
                    from = from.get(),
                    to = to.get(),
                    "Heartbeat dropped due to partition"
                );
                return;
            }

            // Deliver the decoded heartbeat to target mailbox.
            if let Some(mailbox) = shared_mailboxes.get(&to) {
                let msg = IncomingMessage::Heartbeat(HeartbeatMessage {
                    heartbeat: decoded_heartbeat,
                });
                // Ignore send errors (receiver might be dropped).
                let _ = mailbox.send(msg).await;
            }
        });

        Ok(())
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Creates mailboxes for a cluster of nodes.
///
/// Returns shared mailboxes (for transport) and receivers (for nodes).
/// The `SharedNodeMailboxes` wrapper allows `restart_node()` to swap in
/// new mailbox senders without recreating all transports.
#[must_use]
pub fn create_cluster_mailboxes(
    node_ids: &[NodeId],
    capacity: usize,
) -> (Arc<SharedNodeMailboxes>, BTreeMap<NodeId, NodeMailboxReceiver>) {
    let mut senders = BTreeMap::new();
    let mut receivers = BTreeMap::new();

    for &node_id in node_ids {
        let (tx, rx) = mpsc::channel(capacity);
        senders.insert(node_id, tx);
        receivers.insert(node_id, rx);
    }

    (Arc::new(SharedNodeMailboxes::new(senders)), receivers)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(all(test, madsim))]
mod tests {
    use super::*;
    use madsim::runtime::Runtime;

    #[test]
    fn test_transport_service_send_batch() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let node_ids = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
            let (mailboxes, mut receivers) = create_cluster_mailboxes(&node_ids, 10);
            let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

            let transport = MadSimTransport::new(NodeId::new(1), network_state.clone(), mailboxes);

            // Create a test message.
            let msg = GroupMessage {
                group_id: helix_core::GroupId::new(1),
                message: helix_raft::Message::RequestVote(helix_raft::RequestVoteRequest::new(
                    helix_core::TermId::new(1),
                    NodeId::new(1),
                    NodeId::new(2),
                    helix_core::LogIndex::new(0),
                    helix_core::TermId::new(0),
                )),
            };

            // Send batch via TransportService trait.
            transport
                .send_batch(NodeId::new(2), vec![msg])
                .await
                .unwrap();

            // Wait for delivery.
            madsim::time::sleep(Duration::from_millis(5)).await;

            // Should receive.
            let received = receivers.get_mut(&NodeId::new(2)).unwrap().try_recv();
            assert!(received.is_ok(), "Should receive Raft batch");

            if let Ok(IncomingMessage::Raft(raft_msg)) = received {
                assert_eq!(raft_msg.from, NodeId::new(1));
                assert_eq!(raft_msg.messages.len(), 1);
            } else {
                panic!("Expected Raft message");
            }

            eprintln!("[PASS] test_transport_service_send_batch");
        });
    }

    #[test]
    fn test_transport_service_send_heartbeat() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let node_ids = [NodeId::new(1), NodeId::new(2)];
            let (mailboxes, mut receivers) = create_cluster_mailboxes(&node_ids, 10);
            let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

            let transport = MadSimTransport::new(NodeId::new(1), network_state.clone(), mailboxes);

            // Send heartbeat via TransportService trait.
            let heartbeat = BrokerHeartbeat::new(NodeId::new(1), 12345);
            transport
                .send_heartbeat(NodeId::new(2), &heartbeat)
                .await
                .unwrap();

            // Wait for delivery.
            madsim::time::sleep(Duration::from_millis(5)).await;

            // Should receive.
            let received = receivers.get_mut(&NodeId::new(2)).unwrap().try_recv();
            assert!(received.is_ok(), "Should receive heartbeat");

            if let Ok(IncomingMessage::Heartbeat(hb_msg)) = received {
                assert_eq!(hb_msg.heartbeat.node_id, NodeId::new(1));
                assert_eq!(hb_msg.heartbeat.timestamp_ms, 12345);
            } else {
                panic!("Expected Heartbeat message");
            }

            eprintln!("[PASS] test_transport_service_send_heartbeat");
        });
    }

    #[test]
    fn test_transport_service_partition_drops_messages() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let node_ids = [NodeId::new(1), NodeId::new(2)];
            let (mailboxes, mut receivers) = create_cluster_mailboxes(&node_ids, 10);
            let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

            // Partition nodes.
            {
                let mut state = network_state.lock().unwrap();
                state.partition(&[NodeId::new(1), NodeId::new(2)]);
            }

            let transport = MadSimTransport::new(NodeId::new(1), network_state.clone(), mailboxes);

            // Send batch - should be dropped.
            let msg = GroupMessage {
                group_id: helix_core::GroupId::new(1),
                message: helix_raft::Message::RequestVote(helix_raft::RequestVoteRequest::new(
                    helix_core::TermId::new(1),
                    NodeId::new(1),
                    NodeId::new(2),
                    helix_core::LogIndex::new(0),
                    helix_core::TermId::new(0),
                )),
            };
            transport
                .send_batch(NodeId::new(2), vec![msg])
                .await
                .unwrap();

            // Wait for delivery attempt.
            madsim::time::sleep(Duration::from_millis(5)).await;

            // Should NOT receive (partitioned).
            let received = receivers.get_mut(&NodeId::new(2)).unwrap().try_recv();
            assert!(received.is_err(), "Partitioned message should be dropped");

            eprintln!("[PASS] test_transport_service_partition_drops_messages");
        });
    }

    #[test]
    fn test_transport_node_id() {
        let node_ids = [NodeId::new(1), NodeId::new(2)];
        let (mailboxes, _receivers) = create_cluster_mailboxes(&node_ids, 10);
        let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

        let transport = MadSimTransport::new(NodeId::new(42), network_state, mailboxes);
        assert_eq!(transport.node_id(), NodeId::new(42));

        eprintln!("[PASS] test_transport_node_id");
    }

    /// Verifies that send_batch performs a codec round-trip that preserves message content.
    ///
    /// This test ensures the MadSim transport exercises the same wire format as production.
    #[test]
    fn test_transport_codec_round_trip_preserves_content() {
        use bytes::Bytes;
        use helix_core::{GroupId, LogIndex, TermId};
        use helix_raft::{AppendEntriesRequest, LogEntry, Message};

        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let node_ids = [NodeId::new(1), NodeId::new(2)];
            let (mailboxes, mut receivers) = create_cluster_mailboxes(&node_ids, 10);
            let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

            let transport = MadSimTransport::new(NodeId::new(1), network_state.clone(), mailboxes);

            // Create a batch with multiple message types including log entries with data.
            let entry1 = LogEntry::new(
                TermId::new(5),
                LogIndex::new(10),
                Bytes::from("hello world"),
            );
            let entry2 = LogEntry::new(
                TermId::new(5),
                LogIndex::new(11),
                Bytes::from("test data 123"),
            );
            let append_req = AppendEntriesRequest::new(
                TermId::new(5),
                NodeId::new(1),
                NodeId::new(2),
                LogIndex::new(9),
                TermId::new(4),
                vec![entry1, entry2],
                LogIndex::new(8),
            );

            let messages = vec![
                GroupMessage {
                    group_id: GroupId::new(42),
                    message: Message::AppendEntries(append_req.clone()),
                },
                GroupMessage {
                    group_id: GroupId::new(99),
                    message: helix_raft::Message::RequestVote(helix_raft::RequestVoteRequest::new(
                        TermId::new(10),
                        NodeId::new(1),
                        NodeId::new(2),
                        LogIndex::new(100),
                        TermId::new(9),
                    )),
                },
            ];

            // Send batch - this performs encode/decode round-trip internally.
            transport
                .send_batch(NodeId::new(2), messages.clone())
                .await
                .unwrap();

            // Wait for delivery.
            madsim::time::sleep(Duration::from_millis(5)).await;

            // Verify received messages match original.
            let received = receivers.get_mut(&NodeId::new(2)).unwrap().try_recv();
            assert!(received.is_ok(), "Should receive Raft batch");

            if let Ok(IncomingMessage::Raft(raft_msg)) = received {
                assert_eq!(raft_msg.from, NodeId::new(1));
                assert_eq!(raft_msg.messages.len(), 2, "Should have 2 messages");

                // Verify first message (AppendEntries with entries).
                assert_eq!(raft_msg.messages[0].group_id.get(), 42);
                if let Message::AppendEntries(req) = &raft_msg.messages[0].message {
                    assert_eq!(req.term.get(), 5);
                    assert_eq!(req.leader_id.get(), 1);
                    assert_eq!(req.entries.len(), 2);
                    assert_eq!(req.entries[0].data.as_ref(), b"hello world");
                    assert_eq!(req.entries[1].data.as_ref(), b"test data 123");
                } else {
                    panic!("Expected AppendEntries message");
                }

                // Verify second message (RequestVote).
                assert_eq!(raft_msg.messages[1].group_id.get(), 99);
                if let Message::RequestVote(req) = &raft_msg.messages[1].message {
                    assert_eq!(req.term.get(), 10);
                    assert_eq!(req.last_log_index.get(), 100);
                } else {
                    panic!("Expected RequestVote message");
                }
            } else {
                panic!("Expected Raft message");
            }

            eprintln!("[PASS] test_transport_codec_round_trip_preserves_content");
        });
    }

    /// Verifies that heartbeat send performs a codec round-trip.
    #[test]
    fn test_transport_heartbeat_codec_round_trip() {
        let rt = Runtime::with_seed_and_config(42, Default::default());
        rt.block_on(async {
            let node_ids = [NodeId::new(1), NodeId::new(2)];
            let (mailboxes, mut receivers) = create_cluster_mailboxes(&node_ids, 10);
            let network_state = Arc::new(Mutex::new(MadSimNetworkState::new()));

            let transport = MadSimTransport::new(NodeId::new(1), network_state.clone(), mailboxes);

            // Create a heartbeat with specific values.
            let heartbeat = BrokerHeartbeat::new(NodeId::new(42), 987_654_321);
            transport
                .send_heartbeat(NodeId::new(2), &heartbeat)
                .await
                .unwrap();

            // Wait for delivery.
            madsim::time::sleep(Duration::from_millis(5)).await;

            // Verify received heartbeat matches original.
            let received = receivers.get_mut(&NodeId::new(2)).unwrap().try_recv();
            assert!(received.is_ok(), "Should receive heartbeat");

            if let Ok(IncomingMessage::Heartbeat(hb_msg)) = received {
                assert_eq!(hb_msg.heartbeat.node_id.get(), 42);
                assert_eq!(hb_msg.heartbeat.timestamp_ms, 987_654_321);
            } else {
                panic!("Expected Heartbeat message");
            }

            eprintln!("[PASS] test_transport_heartbeat_codec_round_trip");
        });
    }
}

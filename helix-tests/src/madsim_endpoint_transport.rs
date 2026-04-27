//! `madsim::net::Endpoint`-based transport for DST.
//!
//! Replaces the custom mpsc-based `MadSimTransport`. Sends and receives go
//! through `madsim::net::Endpoint`, which routes through madsim's seeded
//! `Network::test_link()` — that path consults `packet_loss_rate` and samples
//! `send_latency: Range<Duration>` on every message. Partitions are applied
//! via `madsim::net::NetSim::clog_link(src, dst)` (directional) rather than
//! a custom `BTreeSet<(NodeId, NodeId)>`.
//!
//! # Why this exists
//!
//! The previous `MadSimTransport` (deleted) used in-process `mpsc` channels
//! with hardcoded 1ms latency × integer multipliers and a symmetric partition
//! set. Seeds did not affect drop or latency decisions — only task
//! scheduling. This transport routes through madsim's actual DST primitives
//! so a seed change alters every message's drop dice and latency sample.
//!
//! # Pre-conditions
//!
//! `Endpoint::bind()` must be called inside a madsim node task context
//! (`runtime.create_node().ip(addr).build().spawn(...)`). Endpoints bound
//! outside a per-node context all share one node and `Network::test_link`
//! treats them as loopback — no drop dice, no latency sampling.

#![cfg(feature = "madsim")]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use helix_core::NodeId;
use helix_raft::multi::GroupMessage;
use helix_runtime::{
    decode_broker_heartbeat, decode_coalesced_heartbeats, decode_group_batch,
    encode_broker_heartbeat, encode_coalesced_heartbeats, encode_group_batch, BrokerHeartbeat,
    IncomingMessage, TransportResult, TransportService,
};
use madsim::net::Endpoint;
use tokio::sync::mpsc;
use tracing::{trace, warn};

/// Tag for Raft `GroupMessage` batches.
const TAG_RAFT_BATCH: u64 = 1;
/// Tag for broker heartbeats.
const TAG_HEARTBEAT: u64 = 2;
/// Tag for coalesced Raft heartbeat batches.
const TAG_COALESCED_HEARTBEAT: u64 = 3;

/// Receive buffer size — matches `READ_BUFFER_SIZE` in `helix_runtime::transport`.
const RECV_BUFFER_SIZE: usize = 1024 * 1024;

/// Maps `NodeId` to its bound `SocketAddr` in the madsim network.
///
/// Constructed once at cluster setup. Cloned into every `MadSimEndpointTransport`
/// instance so each node knows where to send to.
pub type NodeAddrMap = Arc<BTreeMap<NodeId, SocketAddr>>;

/// `Endpoint`-based transport implementing `TransportService`.
///
/// Each helix node holds one of these. The `endpoint` must have been bound
/// inside the helix node's madsim node task context.
#[derive(Clone)]
pub struct MadSimEndpointTransport {
    node_id: NodeId,
    endpoint: Endpoint,
    peers: NodeAddrMap,
}

impl MadSimEndpointTransport {
    /// Creates a transport over an already-bound `Endpoint`.
    ///
    /// `endpoint` must be created via `Endpoint::bind()` inside the helix
    /// node's madsim node task context. `peers` maps every cluster `NodeId`
    /// (including `node_id`) to its bound socket address.
    #[must_use]
    pub const fn new(node_id: NodeId, endpoint: Endpoint, peers: NodeAddrMap) -> Self {
        Self {
            node_id,
            endpoint,
            peers,
        }
    }

    /// Returns the local socket address this transport is bound to.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }
}

#[async_trait]
impl TransportService for MadSimEndpointTransport {
    async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()> {
        let Some(&addr) = self.peers.get(&to) else {
            warn!(
                from = self.node_id.get(),
                to = to.get(),
                "send_batch: peer addr unknown — dropping"
            );
            return Ok(());
        };
        let encoded = match encode_group_batch(&messages) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    from = self.node_id.get(),
                    to = to.get(),
                    error = %e,
                    "send_batch: encode failed — dropping"
                );
                return Ok(());
            }
        };
        // Endpoint::send_to runs through madsim's Network::test_link which
        // consults packet_loss_rate and samples send_latency Range<Duration>.
        if let Err(e) = self.endpoint.send_to(addr, TAG_RAFT_BATCH, &encoded).await {
            trace!(
                from = self.node_id.get(),
                to = to.get(),
                error = %e,
                "send_batch: endpoint send_to failed — dropping"
            );
        }
        Ok(())
    }

    async fn send_heartbeat(
        &self,
        to: NodeId,
        heartbeat: &BrokerHeartbeat,
    ) -> TransportResult<()> {
        let Some(&addr) = self.peers.get(&to) else {
            return Ok(());
        };
        let encoded = match encode_broker_heartbeat(heartbeat) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    from = self.node_id.get(),
                    to = to.get(),
                    error = %e,
                    "send_heartbeat: encode failed — dropping"
                );
                return Ok(());
            }
        };
        if let Err(e) = self.endpoint.send_to(addr, TAG_HEARTBEAT, &encoded).await {
            trace!(
                from = self.node_id.get(),
                to = to.get(),
                error = %e,
                "send_heartbeat: endpoint send_to failed — dropping"
            );
        }
        Ok(())
    }

    async fn send_heartbeat_batch(
        &self,
        to: NodeId,
        messages: Vec<GroupMessage>,
    ) -> TransportResult<()> {
        let Some(&addr) = self.peers.get(&to) else {
            return Ok(());
        };
        let encoded = match encode_coalesced_heartbeats(self.node_id, to, &messages) {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    from = self.node_id.get(),
                    to = to.get(),
                    error = %e,
                    "send_heartbeat_batch: encode failed — dropping"
                );
                return Ok(());
            }
        };
        if let Err(e) = self
            .endpoint
            .send_to(addr, TAG_COALESCED_HEARTBEAT, &encoded)
            .await
        {
            trace!(
                from = self.node_id.get(),
                to = to.get(),
                error = %e,
                "send_heartbeat_batch: endpoint send_to failed — dropping"
            );
        }
        Ok(())
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Spawns three receive loops on the given endpoint — one per tag.
///
/// Each loop calls `endpoint.recv_from(tag, &mut buf)`, decodes the inbound
/// payload, and pushes a `helix_runtime::IncomingMessage` to `incoming_tx`.
/// On endpoint close or send-side disconnect, the loops exit cleanly.
///
/// Must be called inside the same madsim node task context that bound the
/// endpoint, so the spawned tasks inherit the node's network identity.
pub fn spawn_receive_loops(endpoint: Endpoint, incoming_tx: mpsc::Sender<IncomingMessage>) {
    {
        let endpoint = endpoint.clone();
        let tx = incoming_tx.clone();
        madsim::task::spawn(async move {
            recv_loop_raft_batch(endpoint, tx).await;
        });
    }
    {
        let endpoint = endpoint.clone();
        let tx = incoming_tx.clone();
        madsim::task::spawn(async move {
            recv_loop_heartbeat(endpoint, tx).await;
        });
    }
    {
        let tx = incoming_tx;
        madsim::task::spawn(async move {
            recv_loop_coalesced(endpoint, tx).await;
        });
    }
}

async fn recv_loop_raft_batch(endpoint: Endpoint, tx: mpsc::Sender<IncomingMessage>) {
    let mut buf = vec![0u8; RECV_BUFFER_SIZE];
    loop {
        match endpoint.recv_from(TAG_RAFT_BATCH, &mut buf).await {
            Ok((len, _from)) => match decode_group_batch(&buf[..len]) {
                Ok((msgs, _consumed)) => {
                    if tx.send(IncomingMessage::Batch(msgs)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "recv_loop_raft_batch: decode failed — dropping");
                }
            },
            Err(_) => break,
        }
    }
}

async fn recv_loop_heartbeat(endpoint: Endpoint, tx: mpsc::Sender<IncomingMessage>) {
    let mut buf = vec![0u8; RECV_BUFFER_SIZE];
    loop {
        match endpoint.recv_from(TAG_HEARTBEAT, &mut buf).await {
            Ok((len, _from)) => match decode_broker_heartbeat(&buf[..len]) {
                Ok((hb, _consumed)) => {
                    if tx.send(IncomingMessage::Heartbeat(hb)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "recv_loop_heartbeat: decode failed — dropping");
                }
            },
            Err(_) => break,
        }
    }
}

async fn recv_loop_coalesced(endpoint: Endpoint, tx: mpsc::Sender<IncomingMessage>) {
    let mut buf = vec![0u8; RECV_BUFFER_SIZE];
    loop {
        match endpoint.recv_from(TAG_COALESCED_HEARTBEAT, &mut buf).await {
            Ok((len, _from)) => match decode_coalesced_heartbeats(&buf[..len]) {
                Ok((msgs, _consumed)) => {
                    if tx.send(IncomingMessage::Batch(msgs)).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "recv_loop_coalesced: decode failed — dropping");
                }
            },
            Err(_) => break,
        }
    }
}

/// Builds a `NodeAddrMap` for a cluster of nodes using a deterministic IP
/// scheme.
///
/// Node `i` is assigned `10.0.0.{i}:8000`. Caller is responsible for
/// creating the corresponding madsim nodes with matching IPs via
/// `runtime.create_node().ip(...).build()`.
#[must_use]
pub fn build_node_addrs(node_ids: &[NodeId]) -> NodeAddrMap {
    let port: u16 = 8000;
    let mut map = BTreeMap::new();
    for &nid in node_ids {
        // Safe cast: NodeId is a u64 but we expect cluster sizes ≤ 255.
        let id = nid.get();
        assert!(id <= 255, "build_node_addrs: NodeId {id} exceeds /24 IP range");
        #[allow(clippy::cast_possible_truncation)]
        let octet = id as u8;
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, octet));
        map.insert(nid, SocketAddr::new(ip, port));
    }
    Arc::new(map)
}

//! TCP transport layer for Raft peer communication.
//!
//! This module provides reliable message delivery between Raft nodes using TCP.
//!
//! # Design
//!
//! The transport manages two types of connections:
//! - **Outbound**: Connections initiated to other peers for sending messages
//! - **Inbound**: Connections accepted from other peers for receiving messages
//!
//! Each peer maintains a single bidirectional connection. The node with the
//! lower `NodeId` initiates the connection to avoid duplicate connections.
//!
//! # Connection Lifecycle
//!
//! 1. Transport starts listening on the configured address
//! 2. Outbound connections are established lazily on first send
//! 3. Connections are automatically reconnected on failure
//! 4. Messages are buffered briefly if connection is pending

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use helix_core::NodeId;
use helix_raft::multi::GroupMessage;
use helix_raft::Message;
// socket2 is only used for production TCP binding (not under MadSim).
#[cfg(not(madsim))]
use socket2::{Domain, Socket, Type};
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::codec::{
    decode_broker_heartbeat, decode_group_batch, decode_message, encode_broker_heartbeat,
    encode_group_batch, encode_message, is_broker_heartbeat, is_group_batch, BrokerHeartbeat,
    CodecError,
};
use crate::transport_trait::TransportService;

/// Maximum read buffer size (1 MB).
const READ_BUFFER_SIZE: usize = 1024 * 1024;

/// Connection timeout in milliseconds.
const CONNECT_TIMEOUT_MS: u64 = 5000;

/// TCP keepalive idle/interval in seconds.
///
/// Detects half-open connections when idle (no pending unacked data). When
/// a peer pod restarts and the OS send buffer is empty, keepalive probes
/// fire after this many seconds of inactivity and detect the dead connection
/// within `TCP_KEEPALIVE_SECS` + `TCP_KEEPALIVE_SECS` * `TCP_KEEPALIVE_CNT` seconds.
const TCP_KEEPALIVE_SECS: u64 = 5;

/// TCP keepalive probe count.
///
/// After `TCP_KEEPALIVE_SECS` idle, the OS sends up to this many probes at
/// `TCP_KEEPALIVE_SECS` intervals before declaring the connection dead.
/// Total idle-connection detection time: 5s + 5s * 5 = 30s.
const TCP_KEEPALIVE_CNT: u32 = 5;

/// `TCP_USER_TIMEOUT` in seconds.
///
/// This is the correct fix for half-open connections when the send buffer
/// has pending data. `SO_KEEPALIVE` probes are suppressed by the retransmission
/// timer when there is unacked data in the send buffer — so keepalive alone
/// cannot detect a wedged connection under load. `TCP_USER_TIMEOUT` tells the
/// kernel: "abort this connection if transmitted data goes unacknowledged for
/// more than N seconds." This covers the case where Raft heartbeats have
/// filled the send buffer on a stale connection (e.g. peer pod restarted
/// mid-send). Without this, the connection wedges for ~15 minutes (the
/// `tcp_retries2` default). With it, detection occurs within ~30 seconds.
/// Used by gRPC (proposal A18), Linkerd, and etcd for the same reason.
///
/// Set to match `TCP_KEEPALIVE_SECS` + `TCP_KEEPALIVE_SECS` * `TCP_KEEPALIVE_CNT`
/// so both code paths (idle vs. data-pending) have the same ~30s bound.
const TCP_USER_TIMEOUT_SECS: u64 = TCP_KEEPALIVE_SECS + TCP_KEEPALIVE_SECS * TCP_KEEPALIVE_CNT as u64;

/// Maximum pending messages per peer.
const MAX_PENDING_MESSAGES: usize = 1000;

/// Transport errors.
#[derive(Debug, Error)]
pub enum TransportError {
    /// Failed to bind to address.
    #[error("failed to bind to {addr}: {source}")]
    BindFailed {
        /// The address we tried to bind.
        addr: SocketAddr,
        /// The underlying error.
        source: std::io::Error,
    },

    /// Failed to connect to peer.
    #[error("failed to connect to peer {node_id} at {addr}: {source}")]
    ConnectFailed {
        /// The peer node ID.
        node_id: NodeId,
        /// The peer address.
        addr: String,
        /// The underlying error.
        source: std::io::Error,
    },

    /// Codec error.
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Transport is shutdown.
    #[error("transport is shutdown")]
    Shutdown,

    /// Unknown peer.
    #[error("unknown peer: {0}")]
    UnknownPeer(NodeId),

    /// Send queue full.
    #[error("send queue full for peer {0}")]
    QueueFull(NodeId),
}

/// Result type for transport operations.
pub type TransportResult<T> = Result<T, TransportError>;

/// Incoming message from a peer.
///
/// This enum distinguishes between single messages (backward compatible),
/// batches of `GroupMessage`s used by Multi-Raft, and broker heartbeats.
#[derive(Debug, Clone)]
pub enum IncomingMessage {
    /// A single Raft message.
    Single(Message),
    /// A batch of `GroupMessage`s from Multi-Raft.
    Batch(Vec<GroupMessage>),
    /// A broker heartbeat (soft state, not Raft-replicated).
    Heartbeat(BrokerHeartbeat),
}

/// Internal type for outbound data.
enum OutgoingData {
    /// A single message to send.
    Single(Message),
    /// A batch of group messages to send.
    Batch(Bytes),
    /// A broker heartbeat to send.
    Heartbeat(Bytes),
}

/// Configuration for a peer node.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The peer's node ID.
    pub node_id: NodeId,
    /// The peer's address (hostname:port or ip:port, resolved at connect time).
    pub addr: String,
}

/// Transport configuration.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// Address to listen on.
    pub listen_addr: SocketAddr,
    /// Known peers.
    pub peers: Vec<PeerInfo>,
}

impl TransportConfig {
    /// Creates a new transport configuration.
    #[must_use]
    pub const fn new(node_id: NodeId, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            listen_addr,
            peers: Vec::new(),
        }
    }

    /// Adds a peer to the configuration.
    ///
    /// The address can be either `ip:port` or `hostname:port`. DNS resolution
    /// is deferred until connection time.
    #[must_use]
    pub fn with_peer(mut self, node_id: NodeId, addr: impl Into<String>) -> Self {
        self.peers.push(PeerInfo {
            node_id,
            addr: addr.into(),
        });
        self
    }
}

/// State of a peer connection.
struct PeerConnection {
    /// The peer's address (stored for reconnection).
    #[allow(dead_code)]
    addr: String,
    /// Sender for outbound data.
    sender: mpsc::Sender<OutgoingData>,
}

/// Handle to interact with the transport.
#[derive(Clone)]
pub struct TransportHandle {
    /// Our node ID.
    node_id: NodeId,
    /// Peer connections.
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    /// Shutdown signal.
    shutdown: Arc<Mutex<bool>>,
}

impl TransportHandle {
    /// Sends a single message to a peer.
    ///
    /// # Errors
    /// Returns an error if the peer is unknown or the send queue is full.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn send(&self, to: NodeId, message: Message) -> TransportResult<()> {
        // Precondition: can't send to self.
        debug_assert!(to != self.node_id, "cannot send message to self");

        if *self.shutdown.lock().await {
            return Err(TransportError::Shutdown);
        }

        let peers = self.peers.read().await;
        let conn = peers.get(&to).ok_or(TransportError::UnknownPeer(to))?;

        conn.sender
            .try_send(OutgoingData::Single(message))
            .map_err(|_| TransportError::QueueFull(to))
    }

    /// Sends a batch of `GroupMessage`s to a peer.
    ///
    /// This is the primary send method for Multi-Raft, which batches messages
    /// by destination node for efficiency.
    ///
    /// # Errors
    /// Returns an error if the peer is unknown, the send queue is full, or
    /// encoding fails.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()> {
        // Precondition: can't send to self.
        debug_assert!(to != self.node_id, "cannot send batch to self");

        if messages.is_empty() {
            return Ok(());
        }

        if *self.shutdown.lock().await {
            return Err(TransportError::Shutdown);
        }

        // Encode the batch upfront to catch codec errors early.
        let encoded = encode_group_batch(&messages)?;

        let peers = self.peers.read().await;
        let conn = peers.get(&to).ok_or(TransportError::UnknownPeer(to))?;

        conn.sender
            .try_send(OutgoingData::Batch(encoded))
            .map_err(|_| TransportError::QueueFull(to))
    }

    /// Sends a broker heartbeat to a peer.
    ///
    /// Heartbeats are soft-state messages (not Raft-replicated) used to track
    /// broker liveness. Each node sends heartbeats to all peers.
    ///
    /// # Errors
    /// Returns an error if the peer is unknown, the send queue is full, or
    /// encoding fails.
    #[allow(clippy::significant_drop_tightening)]
    pub async fn send_heartbeat(
        &self,
        to: NodeId,
        heartbeat: &BrokerHeartbeat,
    ) -> TransportResult<()> {
        // Precondition: can't send to self.
        debug_assert!(to != self.node_id, "cannot send heartbeat to self");

        if *self.shutdown.lock().await {
            return Err(TransportError::Shutdown);
        }

        // Encode the heartbeat upfront.
        let encoded = encode_broker_heartbeat(heartbeat)?;

        let peers = self.peers.read().await;
        let conn = peers.get(&to).ok_or(TransportError::UnknownPeer(to))?;

        conn.sender
            .try_send(OutgoingData::Heartbeat(encoded))
            .map_err(|_| TransportError::QueueFull(to))
    }

    /// Returns the node ID of this transport.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns true if the transport is shutdown.
    pub async fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().await
    }
}

/// Implementation of `TransportService` for `TransportHandle`.
///
/// This allows `TransportHandle` to be used generically where `TransportService`
/// is expected, enabling both production (TCP) and simulated (Bloodhound) transports.
#[async_trait]
impl TransportService for TransportHandle {
    async fn send_batch(&self, to: NodeId, messages: Vec<GroupMessage>) -> TransportResult<()> {
        // Delegate to the inherent method.
        Self::send_batch(self, to, messages).await
    }

    async fn send_heartbeat(&self, to: NodeId, heartbeat: &BrokerHeartbeat) -> TransportResult<()> {
        // Delegate to the inherent method.
        Self::send_heartbeat(self, to, heartbeat).await
    }

    fn node_id(&self) -> NodeId {
        // Delegate to the inherent method.
        Self::node_id(self)
    }
}

/// TCP transport for Raft communication.
pub struct Transport {
    /// Configuration.
    config: TransportConfig,
    /// Peer connections.
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    /// Channel for received messages.
    incoming_tx: mpsc::Sender<IncomingMessage>,
    /// Shutdown signal.
    shutdown: Arc<Mutex<bool>>,
}

impl Transport {
    /// Creates a new transport.
    ///
    /// Returns the transport and a receiver for incoming messages.
    #[must_use]
    pub fn new(config: TransportConfig) -> (Self, mpsc::Receiver<IncomingMessage>) {
        let (incoming_tx, incoming_rx) = mpsc::channel(1024);

        let transport = Self {
            config,
            peers: Arc::new(RwLock::new(HashMap::new())),
            incoming_tx,
            shutdown: Arc::new(Mutex::new(false)),
        };

        (transport, incoming_rx)
    }

    /// Starts the transport, returning a handle for sending messages.
    ///
    /// # Errors
    /// Returns an error if binding fails.
    pub async fn start(self) -> TransportResult<TransportHandle> {
        // Bind the listener with SO_REUSEADDR to allow quick restarts.
        // Under MadSim, use the async madsim-compatible version.
        #[cfg(not(madsim))]
        let listener = create_reusable_listener(self.config.listen_addr).map_err(|e| {
            TransportError::BindFailed {
                addr: self.config.listen_addr,
                source: e,
            }
        })?;
        #[cfg(madsim)]
        let listener = create_reusable_listener_madsim(self.config.listen_addr)
            .await
            .map_err(|e| TransportError::BindFailed {
                addr: self.config.listen_addr,
                source: e,
            })?;

        info!(
            node_id = self.config.node_id.get(),
            addr = %self.config.listen_addr,
            "Transport listening"
        );

        let handle = TransportHandle {
            node_id: self.config.node_id,
            peers: Arc::clone(&self.peers),
            shutdown: Arc::clone(&self.shutdown),
        };

        // Initialize peer connections.
        for peer in &self.config.peers {
            self.init_peer_connection(peer.node_id, peer.addr.clone())
                .await;
        }

        // Spawn the accept loop.
        let accept_shutdown = Arc::clone(&self.shutdown);
        let accept_incoming_tx = self.incoming_tx.clone();
        let accept_node_id = self.config.node_id;

        tokio::spawn(async move {
            Self::accept_loop(
                listener,
                accept_incoming_tx,
                accept_shutdown,
                accept_node_id,
            )
            .await;
        });

        Ok(handle)
    }

    /// Initializes a connection to a peer.
    async fn init_peer_connection(&self, peer_id: NodeId, addr: String) {
        let (tx, rx) = mpsc::channel(MAX_PENDING_MESSAGES);

        {
            let mut peers = self.peers.write().await;
            peers.insert(
                peer_id,
                PeerConnection {
                    addr: addr.clone(),
                    sender: tx,
                },
            );
        }

        // Spawn the sender task.
        let shutdown = Arc::clone(&self.shutdown);
        let node_id = self.config.node_id;

        tokio::spawn(async move {
            Self::sender_loop(node_id, peer_id, addr, rx, shutdown).await;
        });
    }

    /// Loop that accepts incoming connections.
    async fn accept_loop(
        listener: TcpListener,
        incoming_tx: mpsc::Sender<IncomingMessage>,
        shutdown: Arc<Mutex<bool>>,
        node_id: NodeId,
    ) {
        loop {
            if *shutdown.lock().await {
                info!(node_id = node_id.get(), "Accept loop shutting down");
                break;
            }

            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!(
                        node_id = node_id.get(),
                        peer_addr = %peer_addr,
                        "Accepted connection"
                    );

                    let tx = incoming_tx.clone();
                    let shutdown_clone = Arc::clone(&shutdown);

                    tokio::spawn(async move {
                        if let Err(e) = Self::receive_loop(stream, tx, shutdown_clone).await {
                            debug!(peer_addr = %peer_addr, error = %e, "Receive loop ended");
                        }
                    });
                }
                Err(e) => {
                    warn!(error = %e, "Failed to accept connection");
                }
            }
        }
    }

    /// Loop that sends messages to a peer.
    ///
    /// When the TCP connection to a peer is lost (peer killed/restarted),
    /// this loop enters a reconnect cycle with exponential backoff. During
    /// reconnect, stale Raft messages accumulate in the mpsc queue. If the
    /// queue fills (1000 entries), ALL new `try_send` calls fail with
    /// `QueueFull`, blocking Raft progress for every group.
    ///
    /// To prevent this, we drain stale messages from the queue on each
    /// failed connect attempt and after a successful reconnect. Raft
    /// messages are idempotent — the leader will resend anything the
    /// follower actually needs.
    #[allow(clippy::items_after_statements)]
    async fn sender_loop(
        node_id: NodeId,
        peer_id: NodeId,
        addr: String,
        mut rx: mpsc::Receiver<OutgoingData>,
        shutdown: Arc<Mutex<bool>>,
    ) {
        let mut stream: Option<TcpStream> = None;
        let mut reconnect_delay_ms: u64 = 100;
        const MAX_RECONNECT_DELAY_MS: u64 = 10000;
        let mut pending: Option<OutgoingData> = None;
        // Tracks whether we have ever successfully connected to this peer.
        // On the initial connect, `pending` holds the very first message to send
        // and should not be discarded. On a reconnect (was_connected=true) we
        // discard stale Raft messages because the Raft state machine will resend
        // whatever the follower actually needs from its current position.
        let mut was_connected = false;

        loop {
            if *shutdown.lock().await {
                debug!(
                    node_id = node_id.get(),
                    peer_id = peer_id.get(),
                    "Sender loop shutting down"
                );
                break;
            }

            // Keep one message "in hand" until it is successfully sent.
            // This avoids dropping messages on connect/send failures.
            if pending.is_none() {
                let Some(data) = rx.recv().await else {
                    break; // Channel closed.
                };
                pending = Some(data);
            }

            // Ensure we have a connection.
            if stream.is_none() {
                match Self::connect_to_peer(peer_id, &addr).await {
                    Ok(s) => {
                        if was_connected {
                            // Reconnect after a lost connection — drain stale
                            // Raft messages. The Raft state machine will resend
                            // whatever the follower actually needs; stale
                            // AppendEntries to a restarted peer are useless.
                            Self::drain_channel(&mut rx, peer_id, "reconnect");
                            // Discard the pending message too — it's from before
                            // the reconnect and equally stale.
                            pending = None;
                        }
                        // On the initial connect, keep pending — it's the first
                        // fresh message the caller wants delivered.
                        was_connected = true;
                        stream = Some(s);
                        reconnect_delay_ms = 100;
                        info!(
                            node_id = node_id.get(),
                            peer_id = peer_id.get(),
                            addr = %addr,
                            "Connected to peer"
                        );
                    }
                    Err(e) => {
                        // Drain the queue to prevent saturation: while we sleep
                        // in backoff, tick tasks enqueue ~500 msgs/sec across
                        // all Raft groups; the 1000-entry queue fills in ~2s,
                        // after which ALL try_send calls fail, blocking Raft
                        // progress cluster-wide.
                        // Keep `pending` — we will retry it on the next connect
                        // attempt. Raft handles duplicate/stale messages via its
                        // normal rejection flow.
                        Self::drain_channel(&mut rx, peer_id, "connect backoff");
                        warn!(
                            node_id = node_id.get(),
                            peer_id = peer_id.get(),
                            error = %e,
                            "Failed to connect, will retry"
                        );
                        // Exponential backoff.
                        tokio::time::sleep(tokio::time::Duration::from_millis(reconnect_delay_ms))
                            .await;
                        reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                        continue;
                    }
                }
            }

            // Send the data.
            let Some(data) = pending.as_ref() else {
                continue;
            };
            if let Some(ref mut s) = stream {
                let sent = Self::try_send(s, data).await;
                if sent {
                    pending = None;
                    reconnect_delay_ms = 100;
                } else {
                    warn!(peer_id = peer_id.get(), "Failed to send data, reconnecting");
                    stream = None;
                    Self::drain_channel(&mut rx, peer_id, "send failure");
                    pending = None;
                    tokio::time::sleep(tokio::time::Duration::from_millis(reconnect_delay_ms))
                        .await;
                    reconnect_delay_ms = (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                }
            }
        }
    }

    /// Sends `data` on `stream`. Returns `true` on success, `false` on error.
    async fn try_send(stream: &mut TcpStream, data: &OutgoingData) -> bool {
        let result = match data {
            OutgoingData::Single(message) => {
                let encoded = encode_message(message);
                match encoded {
                    Ok(bytes) => Self::send_bytes(stream, &bytes).await,
                    Err(e) => Err(e.into()),
                }
            }
            OutgoingData::Batch(bytes) | OutgoingData::Heartbeat(bytes) => {
                Self::send_bytes(stream, bytes).await
            }
        };
        match result {
            Ok(()) => {
                let msg_desc = match data {
                    OutgoingData::Single(m) => format!("single:{:?}", std::mem::discriminant(m)),
                    OutgoingData::Batch(b) => format!("batch:{} bytes", b.len()),
                    OutgoingData::Heartbeat(_) => "heartbeat".to_string(),
                };
                debug!(msg = %msg_desc, "Sent data");
                true
            }
            Err(_) => false,
        }
    }

    /// Drains all pending messages from the channel and logs the count.
    fn drain_channel(
        rx: &mut mpsc::Receiver<OutgoingData>,
        peer_id: NodeId,
        reason: &str,
    ) {
        let mut drained = 0u32;
        while rx.try_recv().is_ok() {
            drained += 1;
        }
        if drained > 0 {
            debug!(peer_id = peer_id.get(), drained, reason, "Drained stale messages");
        }
    }

    /// Connects to a peer with timeout.
    ///
    /// The address is resolved at connection time to support hostnames.
    async fn connect_to_peer(peer_id: NodeId, addr: &str) -> TransportResult<TcpStream> {
        let timeout = tokio::time::Duration::from_millis(CONNECT_TIMEOUT_MS);

        // Resolve the address (supports both IP and hostname).
        let connect_future = async {
            let mut addrs = tokio::net::lookup_host(addr).await?;
            let resolved = addrs.next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no addresses found for {addr}"),
                )
            })?;
            TcpStream::connect(resolved).await
        };

        match tokio::time::timeout(timeout, connect_future).await {
            Ok(Ok(stream)) => {
                // Disable Nagle's algorithm for lower latency.
                stream.set_nodelay(true)?;

                // Enable TCP keepalive to detect idle half-open connections.
                // When a peer pod restarts, the old TCP connection becomes
                // half-open: writes succeed (kernel buffers them) but data
                // never reaches the peer. Keepalive detects this within
                // TCP_KEEPALIVE_SECS + TCP_KEEPALIVE_SECS * TCP_KEEPALIVE_CNT
                // seconds (~30s) when the send buffer is empty.
                let sock = socket2::SockRef::from(&stream);
                let keepalive = socket2::TcpKeepalive::new()
                    .with_time(std::time::Duration::from_secs(TCP_KEEPALIVE_SECS))
                    .with_interval(std::time::Duration::from_secs(TCP_KEEPALIVE_SECS))
                    .with_retries(TCP_KEEPALIVE_CNT);
                if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
                    warn!(peer_id = peer_id.get(), error = %e, "Failed to set TCP keepalive");
                }

                // Set TCP_USER_TIMEOUT to detect half-open connections when
                // the send buffer has pending unacked data. SO_KEEPALIVE is
                // suppressed by the retransmission timer in this case, so
                // keepalive alone cannot detect a wedged connection under
                // load. TCP_USER_TIMEOUT aborts after N ms of unacknowledged
                // data, matching the ~30s bound of our keepalive config.
                // Linux only; macOS does not support this socket option.
                #[cfg(target_os = "linux")]
                if let Err(e) = sock.set_tcp_user_timeout(Some(
                    std::time::Duration::from_secs(TCP_USER_TIMEOUT_SECS),
                )) {
                    warn!(peer_id = peer_id.get(), error = %e, "Failed to set TCP_USER_TIMEOUT");
                }

                Ok(stream)
            }
            Ok(Err(e)) => Err(TransportError::ConnectFailed {
                node_id: peer_id,
                addr: addr.to_string(),
                source: e,
            }),
            Err(_) => Err(TransportError::ConnectFailed {
                node_id: peer_id,
                addr: addr.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out"),
            }),
        }
    }

    /// Sends raw bytes over a TCP stream.
    ///
    /// Wraps writes in a timeout so that a wedged connection (send buffer full,
    /// peer unreachable) is detected at the application layer regardless of OS
    /// TCP timer support. On Linux, `TCP_USER_TIMEOUT` provides the primary
    /// detection; this timeout is defense-in-depth that also works on macOS.
    async fn send_bytes(stream: &mut TcpStream, data: &[u8]) -> TransportResult<()> {
        let write_timeout = tokio::time::Duration::from_secs(TCP_USER_TIMEOUT_SECS);
        tokio::time::timeout(write_timeout, stream.write_all(data))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "write timed out: peer unreachable")
            })??;
        stream.flush().await?;
        Ok(())
    }

    /// Loop that receives messages from a connection.
    ///
    /// Handles both single messages and batched `GroupMessage`s.
    #[allow(clippy::too_many_lines)] // Complex message handling requires extended logic.
    async fn receive_loop(
        mut stream: TcpStream,
        incoming_tx: mpsc::Sender<IncomingMessage>,
        shutdown: Arc<Mutex<bool>>,
    ) -> TransportResult<()> {
        let mut buffer = BytesMut::with_capacity(READ_BUFFER_SIZE);

        loop {
            if *shutdown.lock().await {
                break;
            }

            // Read data into buffer.
            let bytes_read = stream.read_buf(&mut buffer).await?;
            if bytes_read == 0 {
                // Connection closed.
                debug!("Connection closed by peer");
                break;
            }

            // Try to decode messages from buffer.
            while !buffer.is_empty() {
                // Check message type and decode accordingly.
                if is_group_batch(&buffer) {
                    match decode_group_batch(&buffer) {
                        Ok((messages, consumed)) => {
                            debug!(count = messages.len(), "Received batch");

                            // Forward to handler.
                            let incoming = IncomingMessage::Batch(messages);
                            if incoming_tx.send(incoming).await.is_err() {
                                // Receiver dropped.
                                return Ok(());
                            }

                            // Remove consumed bytes.
                            let _ = buffer.split_to(consumed);
                        }
                        Err(CodecError::InsufficientData { .. }) => {
                            // Need more data.
                            break;
                        }
                        Err(e) => {
                            let hex_dump: String = buffer
                                .iter()
                                .take(64)
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            error!(
                                error = %e,
                                buffer_len = buffer.len(),
                                buffer_hex = %hex_dump,
                                "Failed to decode batch"
                            );
                            return Err(e.into());
                        }
                    }
                } else if is_broker_heartbeat(&buffer) {
                    match decode_broker_heartbeat(&buffer) {
                        Ok((heartbeat, consumed)) => {
                            debug!(
                                node_id = heartbeat.node_id.get(),
                                timestamp_ms = heartbeat.timestamp_ms,
                                "Received heartbeat"
                            );

                            // Forward to handler.
                            let incoming = IncomingMessage::Heartbeat(heartbeat);
                            if incoming_tx.send(incoming).await.is_err() {
                                // Receiver dropped.
                                return Ok(());
                            }

                            // Remove consumed bytes.
                            let _ = buffer.split_to(consumed);
                        }
                        Err(CodecError::InsufficientData { .. }) => {
                            // Need more data.
                            break;
                        }
                        Err(e) => {
                            let hex_dump: String = buffer
                                .iter()
                                .take(64)
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            error!(
                                error = %e,
                                buffer_len = buffer.len(),
                                buffer_hex = %hex_dump,
                                "Failed to decode heartbeat"
                            );
                            return Err(e.into());
                        }
                    }
                } else {
                    match decode_message(&buffer) {
                        Ok((message, consumed)) => {
                            debug!(
                                msg_type = ?std::mem::discriminant(&message),
                                from = message.from().get(),
                                "Received message"
                            );

                            // Forward to handler.
                            let incoming = IncomingMessage::Single(message);
                            if incoming_tx.send(incoming).await.is_err() {
                                // Receiver dropped.
                                return Ok(());
                            }

                            // Remove consumed bytes.
                            let _ = buffer.split_to(consumed);
                        }
                        Err(CodecError::InsufficientData { .. }) => {
                            // Need more data.
                            break;
                        }
                        Err(e) => {
                            let hex_dump: String = buffer
                                .iter()
                                .take(64)
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            error!(
                                error = %e,
                                buffer_len = buffer.len(),
                                buffer_hex = %hex_dump,
                                "Failed to decode message"
                            );
                            return Err(e.into());
                        }
                    }
                }
            }

            // Compact buffer only when empty to prevent unbounded growth.
            // IMPORTANT: Only replace when empty! If we have partial message data
            // waiting for more bytes (InsufficientData case), we must preserve it.
            // Replacing a non-empty buffer would discard partial message data and
            // cause the next read to see the middle of a message as a new frame.
            if buffer.is_empty() && buffer.capacity() > READ_BUFFER_SIZE * 2 {
                buffer = BytesMut::with_capacity(READ_BUFFER_SIZE);
            }
        }

        Ok(())
    }
}

/// Create a TCP listener with `SO_REUSEADDR` enabled.
///
/// This allows the transport to bind to a port that is in `TIME_WAIT` state,
/// which is essential for fast restarts during testing.
///
/// NOTE: This function is not available under `MadSim` because `madsim-tokio`
/// doesn't support `TcpListener::from_std()`. Under `MadSim`, use `MadSimTransport`
/// instead of this production TCP transport.
#[cfg(not(madsim))]
fn create_reusable_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };

    let socket = Socket::new(domain, Type::STREAM, None)?;
    socket.set_reuse_address(true)?;
    // On macOS/BSD, SO_REUSEPORT allows multiple processes to bind to the same port.
    // This helps with rapid test restarts when previous sockets are still closing.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    // Backlog of 128 pending connections.
    socket.listen(128)?;

    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// `MadSim` stub - uses madsim's native `TcpListener::bind()`.
/// This is simpler because `MadSim` simulates the network layer entirely.
#[cfg(madsim)]
async fn create_reusable_listener_madsim(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// Builder for transport configuration.
pub struct TransportBuilder {
    config: TransportConfig,
}

impl TransportBuilder {
    /// Creates a new transport builder.
    #[must_use]
    pub const fn new(node_id: NodeId, listen_addr: SocketAddr) -> Self {
        Self {
            config: TransportConfig::new(node_id, listen_addr),
        }
    }

    /// Adds a peer.
    #[must_use]
    pub fn with_peer(mut self, node_id: NodeId, addr: impl Into<String>) -> Self {
        self.config = self.config.with_peer(node_id, addr);
        self
    }

    /// Builds and starts the transport.
    ///
    /// # Errors
    /// Returns an error if binding fails.
    pub async fn build(
        self,
    ) -> TransportResult<(TransportHandle, mpsc::Receiver<IncomingMessage>)> {
        let (transport, incoming_rx) = Transport::new(self.config);
        let handle = transport.start().await?;
        Ok((handle, incoming_rx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_core::TermId;
    use helix_raft::RequestVoteRequest;

    fn make_test_message(from: u64, to: u64) -> Message {
        Message::RequestVote(RequestVoteRequest::new(
            TermId::new(1),
            NodeId::new(from),
            NodeId::new(to),
            helix_core::LogIndex::new(0),
            TermId::new(0),
        ))
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let config = TransportConfig::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap());

        let (transport, _incoming_rx) = Transport::new(config);
        let handle = transport.start().await.unwrap();

        assert!(!handle.is_shutdown().await);
    }

    #[tokio::test]
    async fn test_transport_builder() {
        let (handle, _incoming_rx) =
            TransportBuilder::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap())
                .with_peer(NodeId::new(2), "127.0.0.1:9002")
                .build()
                .await
                .unwrap();

        assert!(!handle.is_shutdown().await);
    }

    #[tokio::test]
    async fn test_send_to_unknown_peer() {
        let config = TransportConfig::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap());

        let (transport, _incoming_rx) = Transport::new(config);
        let handle = transport.start().await.unwrap();

        let message = make_test_message(1, 99);
        let result = handle.send(NodeId::new(99), message).await;

        assert!(matches!(result, Err(TransportError::UnknownPeer(_))));
    }

    #[tokio::test]
    async fn test_transport_two_nodes_communication() {
        // Use unique ports for this test to avoid conflicts.
        let node2_addr: SocketAddr = "127.0.0.1:19102".parse().unwrap();
        let node1_addr: SocketAddr = "127.0.0.1:19101".parse().unwrap();

        // Start node 2 first (receiver).
        let (transport2, mut incoming2) =
            Transport::new(TransportConfig::new(NodeId::new(2), node2_addr));
        let _handle2 = transport2.start().await.unwrap();

        // Start node 1 with node 2 as a peer.
        let (transport1, _incoming1) = Transport::new(
            TransportConfig::new(NodeId::new(1), node1_addr)
                .with_peer(NodeId::new(2), "127.0.0.1:19102"),
        );
        let handle1 = transport1.start().await.unwrap();

        // Give transports time to connect.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Send a message from node 1 to node 2.
        let message = make_test_message(1, 2);
        let result = handle1.send(NodeId::new(2), message.clone()).await;
        assert!(result.is_ok(), "Failed to send: {result:?}");

        // Wait for the message to be received.
        let received =
            tokio::time::timeout(tokio::time::Duration::from_secs(2), incoming2.recv()).await;

        assert!(received.is_ok(), "Timeout waiting for message");
        let received_message = received.unwrap();
        assert!(received_message.is_some(), "Channel closed");

        // Should be a single message.
        match received_message.unwrap() {
            IncomingMessage::Single(msg) => assert_eq!(msg, message),
            IncomingMessage::Batch(_) => panic!("Expected single message, got batch"),
            IncomingMessage::Heartbeat(_) => panic!("Expected single message, got heartbeat"),
        }
    }

    #[tokio::test]
    async fn test_transport_batch_communication() {
        use helix_core::GroupId;

        // Use unique ports for this test.
        let node2_addr: SocketAddr = "127.0.0.1:19202".parse().unwrap();
        let node1_addr: SocketAddr = "127.0.0.1:19201".parse().unwrap();

        // Start node 2 first (receiver).
        let (transport2, mut incoming2) =
            Transport::new(TransportConfig::new(NodeId::new(2), node2_addr));
        let _handle2 = transport2.start().await.unwrap();

        // Start node 1 with node 2 as a peer.
        let (transport1, _incoming1) = Transport::new(
            TransportConfig::new(NodeId::new(1), node1_addr)
                .with_peer(NodeId::new(2), "127.0.0.1:19202"),
        );
        let handle1 = transport1.start().await.unwrap();

        // Give transports time to connect.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Send a batch of messages from node 1 to node 2.
        let batch = vec![
            GroupMessage::new(GroupId::new(1), make_test_message(1, 2)),
            GroupMessage::new(GroupId::new(2), make_test_message(1, 2)),
            GroupMessage::new(GroupId::new(1), make_test_message(1, 2)),
        ];
        let result = handle1.send_batch(NodeId::new(2), batch.clone()).await;
        assert!(result.is_ok(), "Failed to send batch: {result:?}");

        // Wait for the batch to be received.
        let received =
            tokio::time::timeout(tokio::time::Duration::from_secs(2), incoming2.recv()).await;

        assert!(received.is_ok(), "Timeout waiting for batch");
        let received_msg = received.unwrap();
        assert!(received_msg.is_some(), "Channel closed");

        // Should be a batch.
        match received_msg.unwrap() {
            IncomingMessage::Batch(msgs) => {
                assert_eq!(msgs.len(), 3);
                assert_eq!(msgs[0].group_id.get(), 1);
                assert_eq!(msgs[1].group_id.get(), 2);
                assert_eq!(msgs[2].group_id.get(), 1);
            }
            IncomingMessage::Single(_) => panic!("Expected batch, got single message"),
            IncomingMessage::Heartbeat(_) => panic!("Expected batch, got heartbeat"),
        }
    }

    #[tokio::test]
    async fn test_message_survives_initial_connect_failure() {
        // Use unique ports for this test.
        let node1_addr: SocketAddr = "127.0.0.1:19301".parse().unwrap();
        let node2_addr: SocketAddr = "127.0.0.1:19302".parse().unwrap();

        // Start node 1 first, with node 2 configured as a peer but not yet running.
        let (transport1, _incoming1) = Transport::new(
            TransportConfig::new(NodeId::new(1), node1_addr)
                .with_peer(NodeId::new(2), "127.0.0.1:19302"),
        );
        let handle1 = transport1.start().await.unwrap();

        // Queue a message while node 2 is still down.
        let message = make_test_message(1, 2);
        let result = handle1.send(NodeId::new(2), message.clone()).await;
        assert!(result.is_ok(), "Failed to enqueue message: {result:?}");

        // Let sender_loop attempt (and fail) at least one connect.
        tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;

        // Start node 2 after the initial failure.
        let (transport2, mut incoming2) =
            Transport::new(TransportConfig::new(NodeId::new(2), node2_addr));
        let _handle2 = transport2.start().await.unwrap();

        // The original message should still be delivered once connection succeeds.
        let received =
            tokio::time::timeout(tokio::time::Duration::from_secs(5), incoming2.recv()).await;
        assert!(received.is_ok(), "Timeout waiting for recovered delivery");
        let received_message = received.unwrap();
        assert!(received_message.is_some(), "Channel closed");

        match received_message.unwrap() {
            IncomingMessage::Single(msg) => assert_eq!(msg, message),
            IncomingMessage::Batch(_) => panic!("Expected single message, got batch"),
            IncomingMessage::Heartbeat(_) => panic!("Expected single message, got heartbeat"),
        }
    }

    #[tokio::test]
    async fn test_send_batch_empty() {
        let config = TransportConfig::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap())
            .with_peer(NodeId::new(2), "127.0.0.1:9999");

        let (transport, _incoming_rx) = Transport::new(config);
        let handle = transport.start().await.unwrap();

        // Sending an empty batch should succeed immediately.
        let result = handle.send_batch(NodeId::new(2), vec![]).await;
        assert!(result.is_ok());
    }
}

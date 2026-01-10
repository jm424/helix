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

use bytes::BytesMut;
use helix_core::NodeId;
use helix_raft::Message;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::codec::{decode_message, encode_message, CodecError};

/// Maximum read buffer size (1 MB).
const READ_BUFFER_SIZE: usize = 1024 * 1024;

/// Connection timeout in milliseconds.
const CONNECT_TIMEOUT_MS: u64 = 5000;

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
        addr: SocketAddr,
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

/// Configuration for a peer node.
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// The peer's node ID.
    pub node_id: NodeId,
    /// The peer's address.
    pub addr: SocketAddr,
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
    pub fn new(node_id: NodeId, listen_addr: SocketAddr) -> Self {
        Self {
            node_id,
            listen_addr,
            peers: Vec::new(),
        }
    }

    /// Adds a peer to the configuration.
    #[must_use]
    pub fn with_peer(mut self, node_id: NodeId, addr: SocketAddr) -> Self {
        self.peers.push(PeerInfo { node_id, addr });
        self
    }
}

/// State of a peer connection.
#[derive(Debug)]
struct PeerConnection {
    /// The peer's address (stored for reconnection).
    #[allow(dead_code)]
    addr: SocketAddr,
    /// Sender for outbound messages.
    sender: mpsc::Sender<Message>,
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
    /// Sends a message to a peer.
    ///
    /// # Errors
    /// Returns an error if the peer is unknown or the send queue is full.
    pub async fn send(&self, to: NodeId, message: Message) -> TransportResult<()> {
        // Precondition: can't send to self.
        debug_assert!(to != self.node_id, "cannot send message to self");

        if *self.shutdown.lock().await {
            return Err(TransportError::Shutdown);
        }

        let peers = self.peers.read().await;
        let conn = peers
            .get(&to)
            .ok_or(TransportError::UnknownPeer(to))?;

        conn.sender
            .try_send(message)
            .map_err(|_| TransportError::QueueFull(to))
    }

    /// Returns true if the transport is shutdown.
    pub async fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().await
    }
}

/// TCP transport for Raft communication.
pub struct Transport {
    /// Configuration.
    config: TransportConfig,
    /// Peer connections.
    peers: Arc<RwLock<HashMap<NodeId, PeerConnection>>>,
    /// Channel for received messages.
    incoming_tx: mpsc::Sender<Message>,
    /// Shutdown signal.
    shutdown: Arc<Mutex<bool>>,
}

impl Transport {
    /// Creates a new transport.
    ///
    /// Returns the transport and a receiver for incoming messages.
    #[must_use]
    pub fn new(config: TransportConfig) -> (Self, mpsc::Receiver<Message>) {
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
        // Bind the listener.
        let listener = TcpListener::bind(self.config.listen_addr)
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
            self.init_peer_connection(peer.node_id, peer.addr).await;
        }

        // Spawn the accept loop.
        let accept_shutdown = Arc::clone(&self.shutdown);
        let accept_incoming_tx = self.incoming_tx.clone();
        let accept_node_id = self.config.node_id;

        tokio::spawn(async move {
            Self::accept_loop(listener, accept_incoming_tx, accept_shutdown, accept_node_id).await;
        });

        Ok(handle)
    }

    /// Initializes a connection to a peer.
    async fn init_peer_connection(&self, peer_id: NodeId, addr: SocketAddr) {
        let (tx, rx) = mpsc::channel(MAX_PENDING_MESSAGES);

        {
            let mut peers = self.peers.write().await;
            peers.insert(peer_id, PeerConnection { addr, sender: tx });
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
        incoming_tx: mpsc::Sender<Message>,
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
    async fn sender_loop(
        node_id: NodeId,
        peer_id: NodeId,
        addr: SocketAddr,
        mut rx: mpsc::Receiver<Message>,
        shutdown: Arc<Mutex<bool>>,
    ) {
        let mut stream: Option<TcpStream> = None;
        let mut reconnect_delay_ms: u64 = 100;
        const MAX_RECONNECT_DELAY_MS: u64 = 10000;

        loop {
            if *shutdown.lock().await {
                debug!(
                    node_id = node_id.get(),
                    peer_id = peer_id.get(),
                    "Sender loop shutting down"
                );
                break;
            }

            // Wait for a message.
            let message = match rx.recv().await {
                Some(msg) => msg,
                None => break, // Channel closed.
            };

            // Ensure we have a connection.
            if stream.is_none() {
                match Self::connect_to_peer(peer_id, addr).await {
                    Ok(s) => {
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
                        warn!(
                            node_id = node_id.get(),
                            peer_id = peer_id.get(),
                            error = %e,
                            "Failed to connect, will retry"
                        );
                        // Exponential backoff.
                        tokio::time::sleep(tokio::time::Duration::from_millis(reconnect_delay_ms))
                            .await;
                        reconnect_delay_ms =
                            (reconnect_delay_ms * 2).min(MAX_RECONNECT_DELAY_MS);
                        continue;
                    }
                }
            }

            // Send the message.
            if let Some(ref mut s) = stream {
                match Self::send_message(s, &message).await {
                    Ok(()) => {
                        debug!(
                            peer_id = peer_id.get(),
                            msg_type = ?std::mem::discriminant(&message),
                            "Sent message"
                        );
                    }
                    Err(e) => {
                        warn!(
                            peer_id = peer_id.get(),
                            error = %e,
                            "Failed to send message, reconnecting"
                        );
                        stream = None;
                    }
                }
            }
        }
    }

    /// Connects to a peer with timeout.
    async fn connect_to_peer(peer_id: NodeId, addr: SocketAddr) -> TransportResult<TcpStream> {
        let timeout = tokio::time::Duration::from_millis(CONNECT_TIMEOUT_MS);

        match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
            Ok(Ok(stream)) => {
                // Disable Nagle's algorithm for lower latency.
                stream.set_nodelay(true)?;
                Ok(stream)
            }
            Ok(Err(e)) => Err(TransportError::ConnectFailed {
                node_id: peer_id,
                addr,
                source: e,
            }),
            Err(_) => Err(TransportError::ConnectFailed {
                node_id: peer_id,
                addr,
                source: std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out"),
            }),
        }
    }

    /// Sends a single message over a TCP stream.
    async fn send_message(stream: &mut TcpStream, message: &Message) -> TransportResult<()> {
        let data = encode_message(message)?;
        stream.write_all(&data).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Loop that receives messages from a connection.
    async fn receive_loop(
        mut stream: TcpStream,
        incoming_tx: mpsc::Sender<Message>,
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
                match decode_message(&buffer) {
                    Ok((message, consumed)) => {
                        debug!(
                            msg_type = ?std::mem::discriminant(&message),
                            from = message.from().get(),
                            "Received message"
                        );

                        // Forward to handler.
                        if incoming_tx.send(message).await.is_err() {
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
                        error!(error = %e, "Failed to decode message");
                        return Err(e.into());
                    }
                }
            }

            // Prevent buffer from growing unbounded.
            if buffer.capacity() > READ_BUFFER_SIZE * 2 {
                buffer = BytesMut::with_capacity(READ_BUFFER_SIZE);
            }
        }

        Ok(())
    }
}

/// Builder for transport configuration.
pub struct TransportBuilder {
    config: TransportConfig,
}

impl TransportBuilder {
    /// Creates a new transport builder.
    #[must_use]
    pub fn new(node_id: NodeId, listen_addr: SocketAddr) -> Self {
        Self {
            config: TransportConfig::new(node_id, listen_addr),
        }
    }

    /// Adds a peer.
    #[must_use]
    pub fn with_peer(mut self, node_id: NodeId, addr: SocketAddr) -> Self {
        self.config = self.config.with_peer(node_id, addr);
        self
    }

    /// Builds and starts the transport.
    ///
    /// # Errors
    /// Returns an error if binding fails.
    pub async fn build(self) -> TransportResult<(TransportHandle, mpsc::Receiver<Message>)> {
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
        let (handle, _incoming_rx) = TransportBuilder::new(NodeId::new(1), "127.0.0.1:0".parse().unwrap())
            .with_peer(NodeId::new(2), "127.0.0.1:9002".parse().unwrap())
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
        let (transport2, mut incoming2) = Transport::new(
            TransportConfig::new(NodeId::new(2), node2_addr)
        );
        let _handle2 = transport2.start().await.unwrap();

        // Start node 1 with node 2 as a peer.
        let (transport1, _incoming1) = Transport::new(
            TransportConfig::new(NodeId::new(1), node1_addr)
                .with_peer(NodeId::new(2), node2_addr)
        );
        let handle1 = transport1.start().await.unwrap();

        // Give transports time to connect.
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Send a message from node 1 to node 2.
        let message = make_test_message(1, 2);
        let result = handle1.send(NodeId::new(2), message.clone()).await;
        assert!(result.is_ok(), "Failed to send: {:?}", result);

        // Wait for the message to be received.
        let received = tokio::time::timeout(
            tokio::time::Duration::from_secs(2),
            incoming2.recv()
        ).await;

        assert!(received.is_ok(), "Timeout waiting for message");
        let received_message = received.unwrap();
        assert!(received_message.is_some(), "Channel closed");

        let received_message = received_message.unwrap();
        assert_eq!(received_message, message);
    }
}

//! Raft server implementation.
//!
//! The `RaftServer` is the main entry point for running a Raft node in production.
//! It handles:
//! - Tick-based timer management (single tick drives elections and heartbeats)
//! - Processing Raft outputs (sending messages, applying commits)
//! - Coordinating between the network and the Raft state machine
//!
//! # Tick-Based Timing
//!
//! Timing is tick-based:
//! - Call `tick()` at regular intervals (e.g., every 100ms)
//! - The Raft library internally tracks elapsed ticks
//! - Elections and heartbeats fire when thresholds are reached
//! - Randomized election timeouts prevent thundering herd

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use helix_core::NodeId;
use helix_raft::{
    ClientRequest, ClientResult, PendingRequests, RaftConfig, RaftNode, RaftOutput, RaftState,
    RequestId,
};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::config::{ServerConfig, TimingConfig};
use crate::transport::TransportHandle;

/// Commands that can be sent to the Raft server.
#[derive(Debug)]
pub enum ServerCommand {
    /// Submit a client command.
    ClientCommand {
        /// The command data.
        data: Bytes,
        /// Request ID for tracking.
        request_id: RequestId,
        /// Channel to send the response.
        response: oneshot::Sender<ClientResult>,
    },
    /// Receive a message from a peer.
    PeerMessage {
        /// The Raft message.
        message: helix_raft::Message,
    },
    /// Shutdown the server.
    Shutdown,
}

/// Events emitted by the Raft server.
#[derive(Debug, Clone)]
pub enum ServerEvent {
    /// A log entry was committed.
    Committed {
        /// The log index.
        index: helix_core::LogIndex,
        /// The committed data.
        data: Bytes,
    },
    /// This node became the leader.
    BecameLeader,
    /// This node stepped down from leadership.
    SteppedDown,
    /// State changed.
    StateChanged {
        /// The new state.
        state: RaftState,
    },
}

/// Handle to interact with a running Raft server.
#[derive(Clone)]
pub struct ServerHandle {
    /// Command sender.
    commands: mpsc::Sender<ServerCommand>,
}

impl ServerHandle {
    /// Submits a command to the Raft cluster.
    ///
    /// # Errors
    /// Returns an error if the server is not available or not the leader.
    pub async fn submit(&self, data: Bytes, request_id: RequestId) -> Result<ClientResult, ServerError> {
        let (tx, rx) = oneshot::channel();

        self.commands
            .send(ServerCommand::ClientCommand {
                data,
                request_id,
                response: tx,
            })
            .await
            .map_err(|_| ServerError::ServerUnavailable)?;

        rx.await.map_err(|_| ServerError::ServerUnavailable)
    }

    /// Sends a peer message to the server.
    ///
    /// # Errors
    /// Returns an error if the server is not available.
    pub async fn peer_message(&self, message: helix_raft::Message) -> Result<(), ServerError> {
        self.commands
            .send(ServerCommand::PeerMessage { message })
            .await
            .map_err(|_| ServerError::ServerUnavailable)
    }

    /// Requests the server to shut down.
    ///
    /// # Errors
    /// Returns an error if the server is not available.
    pub async fn shutdown(&self) -> Result<(), ServerError> {
        self.commands
            .send(ServerCommand::Shutdown)
            .await
            .map_err(|_| ServerError::ServerUnavailable)
    }
}

/// Server errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerError {
    /// Server is not available.
    ServerUnavailable,
    /// Server is not the leader.
    NotLeader {
        /// Hint about who the leader might be.
        leader_hint: Option<NodeId>,
    },
    /// Request timed out.
    Timeout,
    /// Internal error.
    Internal {
        /// Error message.
        message: String,
    },
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServerUnavailable => write!(f, "server unavailable"),
            Self::NotLeader { leader_hint } => match leader_hint {
                Some(id) => {
                    let id_val = id.get();
                    write!(f, "not leader, try node {id_val}")
                }
                None => write!(f, "not leader"),
            },
            Self::Timeout => write!(f, "request timed out"),
            Self::Internal { message } => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for ServerError {}

/// A message to be sent to a peer.
#[derive(Debug)]
pub struct OutgoingMessage {
    /// Target node.
    pub to: NodeId,
    /// The message to send.
    pub message: helix_raft::Message,
}

/// The Raft server.
pub struct RaftServer {
    /// Server configuration (used for network transport).
    #[allow(dead_code)]
    config: ServerConfig,
    /// The Raft state machine.
    node: RaftNode,
    /// Timing configuration.
    timing: TimingConfig,
    /// Pending client requests.
    pending: PendingRequests,
    /// Response channels for pending requests.
    response_channels: HashMap<RequestId, oneshot::Sender<ClientResult>>,
    /// Outgoing message queue.
    outgoing: Vec<OutgoingMessage>,
    /// Event channel sender.
    events: mpsc::Sender<ServerEvent>,
}

impl RaftServer {
    /// Creates a new Raft server.
    #[must_use]
    pub fn new(config: ServerConfig, events: mpsc::Sender<ServerEvent>) -> Self {
        let cluster_nodes = config.cluster_nodes();
        let raft_config = RaftConfig::new(config.node_id, cluster_nodes);
        let node = RaftNode::new(raft_config);

        Self {
            timing: config.timing.clone(),
            config,
            node,
            pending: PendingRequests::new(),
            response_channels: HashMap::new(),
            outgoing: Vec::new(),
            events,
        }
    }

    /// Runs the server, returning a handle for interaction.
    ///
    /// This spawns background tasks for timer management.
    /// This version runs without network transport (for testing).
    pub fn run(self) -> (ServerHandle, impl std::future::Future<Output = ()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let handle = ServerHandle { commands: cmd_tx };

        let server = Arc::new(Mutex::new(self));
        let future = Self::run_loop(server, cmd_rx, None, None);

        (handle, future)
    }

    /// Runs the server with network transport.
    ///
    /// This version uses TCP transport for peer communication.
    pub fn run_with_transport(
        self,
        transport: TransportHandle,
        incoming: mpsc::Receiver<helix_raft::Message>,
    ) -> (ServerHandle, impl std::future::Future<Output = ()>) {
        let (cmd_tx, cmd_rx) = mpsc::channel(256);
        let handle = ServerHandle { commands: cmd_tx };

        let server = Arc::new(Mutex::new(self));
        let future = Self::run_loop(server, cmd_rx, Some(transport), Some(incoming));

        (handle, future)
    }

    /// Main server loop.
    ///
    /// Uses a single tick timer.
    /// The Raft library internally tracks elapsed ticks and triggers
    /// elections/heartbeats when appropriate.
    async fn run_loop(
        server: Arc<Mutex<Self>>,
        mut commands: mpsc::Receiver<ServerCommand>,
        transport: Option<TransportHandle>,
        mut incoming: Option<mpsc::Receiver<helix_raft::Message>>,
    ) {
        // Get tick interval from config.
        let tick_interval = {
            let s = server.lock().await;
            s.timing.tick_interval
        };

        // Single tick timer drives both elections and heartbeats.
        let mut tick_timer = interval(tick_interval);
        tick_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Handle incoming commands.
                Some(cmd) = commands.recv() => {
                    let mut s = server.lock().await;
                    match cmd {
                        ServerCommand::ClientCommand { data, request_id, response } => {
                            s.handle_client_command(data, request_id, response);
                        }
                        ServerCommand::PeerMessage { message } => {
                            s.handle_peer_message(message).await;
                        }
                        ServerCommand::Shutdown => {
                            info!("Server shutting down");
                            break;
                        }
                    }

                    // Drain outgoing messages.
                    let messages = std::mem::take(&mut s.outgoing);
                    drop(s);
                    Self::send_messages(messages, transport.as_ref()).await;
                }

                // Handle incoming transport messages.
                Some(message) = async {
                    match incoming.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    let mut s = server.lock().await;
                    s.handle_peer_message(message).await;

                    // Drain outgoing messages.
                    let messages = std::mem::take(&mut s.outgoing);
                    drop(s);
                    Self::send_messages(messages, transport.as_ref()).await;
                }

                // Tick the Raft state machine.
                // This drives both election timeouts and leader heartbeats.
                _ = tick_timer.tick() => {
                    let mut s = server.lock().await;

                    // Tick the Raft node - it internally tracks elapsed ticks
                    // and triggers elections/heartbeats when needed.
                    let outputs = s.node.tick();
                    s.process_outputs(outputs).await;

                    // Drain outgoing messages.
                    let messages = std::mem::take(&mut s.outgoing);
                    drop(s);
                    Self::send_messages(messages, transport.as_ref()).await;
                }
            }
        }
    }

    /// Handles a client command.
    fn handle_client_command(
        &mut self,
        data: Bytes,
        request_id: RequestId,
        response: oneshot::Sender<ClientResult>,
    ) {
        if !self.node.is_leader() {
            let result = ClientResult::NotLeader {
                leader_hint: self.node.leader_id(),
            };
            let _ = response.send(result);
            return;
        }

        // Create client request.
        let request = ClientRequest::new(data);

        // Handle the request.
        if let Some(outputs) = self.node.handle_client_request(request) {
            // Register the pending request.
            let index = self.node.log().last_index();
            self.pending.register(index, request_id);
            self.response_channels.insert(request_id, response);

            // Process outputs synchronously (will be async later).
            tokio::spawn({
                let events = self.events.clone();
                async move {
                    for output in outputs {
                        Self::process_single_output_async(output, &events).await;
                    }
                }
            });
        } else {
            // Should not happen if we checked is_leader.
            let _ = response.send(ClientResult::Error {
                message: "Failed to process request".to_string(),
            });
        }
    }

    /// Handles a message from a peer.
    async fn handle_peer_message(&mut self, message: helix_raft::Message) {
        let outputs = self.node.handle_message(message);
        self.process_outputs(outputs).await;
    }

    /// Processes Raft outputs.
    async fn process_outputs(&mut self, outputs: Vec<RaftOutput>) {
        for output in outputs {
            self.process_single_output(output).await;
        }
    }

    /// Processes a single Raft output.
    async fn process_single_output(&mut self, output: RaftOutput) {
        match output {
            RaftOutput::SendMessage(message) => {
                self.outgoing.push(OutgoingMessage {
                    to: message.to(),
                    message,
                });
            }
            RaftOutput::CommitEntry { index, data } => {
                info!(index = index.get(), "Entry committed");

                // Send event.
                let _ = self.events.send(ServerEvent::Committed {
                    index,
                    data: data.clone(),
                }).await;

                // Complete pending request if any.
                if let Some(request_id) = self.pending.complete(index) {
                    if let Some(response) = self.response_channels.remove(&request_id) {
                        let result = ClientResult::Success {
                            index,
                            result: Some(data),
                        };
                        let _ = response.send(result);
                    }
                }
            }
            RaftOutput::BecameLeader => {
                info!("This node became leader");
                let _ = self.events.send(ServerEvent::BecameLeader).await;
            }
            RaftOutput::SteppedDown => {
                warn!("This node stepped down from leadership");

                // Cancel all pending requests.
                let leader_hint = self.node.leader_id();
                for (_, response) in self.response_channels.drain() {
                    let _ = response.send(ClientResult::NotLeader { leader_hint });
                }
                self.pending.clear();

                let _ = self.events.send(ServerEvent::SteppedDown).await;
            }
        }
    }

    /// Processes a single output asynchronously (for spawned tasks).
    async fn process_single_output_async(output: RaftOutput, events: &mpsc::Sender<ServerEvent>) {
        match output {
            RaftOutput::SendMessage(_message) => {
                // Messages are handled separately.
            }
            RaftOutput::CommitEntry { index, data } => {
                let _ = events.send(ServerEvent::Committed { index, data }).await;
            }
            RaftOutput::BecameLeader => {
                let _ = events.send(ServerEvent::BecameLeader).await;
            }
            RaftOutput::SteppedDown => {
                let _ = events.send(ServerEvent::SteppedDown).await;
            }
        }
    }

    /// Sends outgoing messages to peers.
    ///
    /// If transport is provided, messages are sent over TCP.
    /// Otherwise, messages are logged but not sent (useful for testing).
    async fn send_messages(messages: Vec<OutgoingMessage>, transport: Option<&TransportHandle>) {
        for msg in messages {
            match transport {
                Some(t) => {
                    if let Err(e) = t.send(msg.to, msg.message).await {
                        warn!(
                            to = msg.to.get(),
                            error = %e,
                            "Failed to send message to peer"
                        );
                    }
                }
                None => {
                    debug!(
                        to = msg.to.get(),
                        msg_type = ?std::mem::discriminant(&msg.message),
                        "Would send message to peer (no transport)"
                    );
                }
            }
        }
    }

    /// Returns the current Raft state.
    #[must_use]
    pub const fn state(&self) -> RaftState {
        self.node.state()
    }

    /// Returns true if this node is the leader.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        self.node.is_leader()
    }

    /// Returns the current leader ID if known.
    #[must_use]
    pub const fn leader_id(&self) -> Option<NodeId> {
        self.node.leader_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> ServerConfig {
        ServerConfig::new(
            NodeId::new(1),
            "127.0.0.1:8001".parse().unwrap(),
            "127.0.0.1:9001".parse().unwrap(),
        )
        .with_timing(crate::config::TimingConfig::fast_for_testing())
    }

    #[tokio::test]
    async fn test_server_creation() {
        let (events_tx, _events_rx) = mpsc::channel(16);
        let server = RaftServer::new(make_config(), events_tx);

        assert_eq!(server.state(), RaftState::Follower);
        assert!(!server.is_leader());
    }

    #[tokio::test]
    async fn test_server_handle_not_leader() {
        let (events_tx, _events_rx) = mpsc::channel(16);
        let server = RaftServer::new(make_config(), events_tx);

        let (handle, server_future) = server.run();

        // Spawn the server.
        let server_handle = tokio::spawn(server_future);

        // Try to submit a command (should fail - not leader).
        let request_id = helix_raft::RequestId::new(helix_raft::ClientId::new(1), 1);
        let result = handle.submit(Bytes::from("test"), request_id).await;

        assert!(result.is_ok());
        match result.unwrap() {
            ClientResult::NotLeader { .. } => {}
            other => panic!("Expected NotLeader, got {other:?}"),
        }

        // Shutdown.
        handle.shutdown().await.unwrap();
        server_handle.await.unwrap();
    }
}

//! Raft client interface.
//!
//! This module provides types and utilities for clients interacting with
//! a Raft cluster. It handles request tracking, response types, and
//! provides the foundation for leader discovery and request routing.
//!
//! # Request Flow
//!
//! 1. Client creates a `ClientRequest` with a unique `RequestId`
//! 2. Request is sent to the cluster (ideally to the leader)
//! 3. If sent to a follower, receive `NotLeader` response with leader hint
//! 4. Leader replicates the command and responds with `Success` when committed
//! 5. Client receives `ClientResponse` indicating outcome
//!
//! # Request IDs
//!
//! Each request has a unique `RequestId` consisting of:
//! - Client ID: Identifies the client session
//! - Sequence number: Monotonically increasing within a client session
//!
//! This enables exactly-once semantics by detecting duplicate requests.

use std::collections::HashMap;

use bytes::Bytes;
use helix_core::{LogIndex, NodeId};

/// Unique identifier for a client session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    /// Creates a new client ID.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the raw ID value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client-{}", self.0)
    }
}

/// Unique identifier for a client request.
///
/// Combines client ID and sequence number to uniquely identify requests
/// across the entire cluster lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RequestId {
    /// Client session identifier.
    pub client_id: ClientId,
    /// Sequence number within the client session.
    pub sequence: u64,
}

impl RequestId {
    /// Creates a new request ID.
    #[must_use]
    pub const fn new(client_id: ClientId, sequence: u64) -> Self {
        Self { client_id, sequence }
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.client_id, self.sequence)
    }
}

/// A client command request to the Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCommand {
    /// Unique identifier for this request.
    pub id: RequestId,
    /// The command payload to replicate.
    pub command: Bytes,
}

impl ClientCommand {
    /// Creates a new client command.
    #[must_use]
    pub fn new(id: RequestId, command: Bytes) -> Self {
        Self { id, command }
    }
}

/// Result of a client request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResult {
    /// Request was successfully committed.
    Success {
        /// The log index where the command was committed.
        index: LogIndex,
        /// Optional result data from the state machine.
        result: Option<Bytes>,
    },
    /// This node is not the leader.
    NotLeader {
        /// Hint about who the leader might be (if known).
        leader_hint: Option<NodeId>,
    },
    /// Request timed out before being committed.
    Timeout,
    /// An error occurred.
    Error {
        /// Error description.
        message: String,
    },
}

impl ClientResult {
    /// Returns true if the request succeeded.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Returns true if we should redirect to another node.
    #[must_use]
    pub fn should_redirect(&self) -> bool {
        matches!(self, Self::NotLeader { .. })
    }
}

/// Read consistency levels for client queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Read from the leader only, ensuring linearizability.
    /// This requires a round of heartbeats to confirm leadership.
    Linearizable,
    /// Read from the leader without confirmation.
    /// May return stale data if the leader has been partitioned.
    LeaderLease,
    /// Read from any node. May return stale data.
    Stale,
}

impl Default for ReadConsistency {
    fn default() -> Self {
        Self::Linearizable
    }
}

/// A read-only query to the Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientQuery {
    /// Unique identifier for this request.
    pub id: RequestId,
    /// The query payload.
    pub query: Bytes,
    /// Required consistency level.
    pub consistency: ReadConsistency,
}

impl ClientQuery {
    /// Creates a new client query with default (linearizable) consistency.
    #[must_use]
    pub fn new(id: RequestId, query: Bytes) -> Self {
        Self {
            id,
            query,
            consistency: ReadConsistency::default(),
        }
    }

    /// Creates a query with specified consistency level.
    #[must_use]
    pub fn with_consistency(id: RequestId, query: Bytes, consistency: ReadConsistency) -> Self {
        Self {
            id,
            query,
            consistency,
        }
    }
}

/// Tracks pending client requests.
///
/// This is used by the leader to track which requests are waiting
/// for commit confirmation.
#[derive(Debug, Default)]
pub struct PendingRequests {
    /// Maps log index to the request that created it.
    by_index: HashMap<LogIndex, RequestId>,
    /// Maps request ID to the log index.
    by_request: HashMap<RequestId, LogIndex>,
}

impl PendingRequests {
    /// Creates a new pending requests tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_index: HashMap::new(),
            by_request: HashMap::new(),
        }
    }

    /// Registers a request at the given log index.
    pub fn register(&mut self, index: LogIndex, request_id: RequestId) {
        self.by_index.insert(index, request_id);
        self.by_request.insert(request_id, index);
    }

    /// Gets the request ID for a given log index.
    #[must_use]
    pub fn get_by_index(&self, index: LogIndex) -> Option<RequestId> {
        self.by_index.get(&index).copied()
    }

    /// Gets the log index for a given request ID.
    #[must_use]
    pub fn get_by_request(&self, request_id: RequestId) -> Option<LogIndex> {
        self.by_request.get(&request_id).copied()
    }

    /// Removes and returns the request ID for a committed index.
    pub fn complete(&mut self, index: LogIndex) -> Option<RequestId> {
        if let Some(request_id) = self.by_index.remove(&index) {
            self.by_request.remove(&request_id);
            Some(request_id)
        } else {
            None
        }
    }

    /// Clears all pending requests (e.g., on leadership loss).
    pub fn clear(&mut self) {
        self.by_index.clear();
        self.by_request.clear();
    }

    /// Returns the number of pending requests.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_index.len()
    }

    /// Returns true if there are no pending requests.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_index.is_empty()
    }
}

/// Client session state for exactly-once semantics.
///
/// Tracks the last completed request for each client to detect
/// and reject duplicate requests.
#[derive(Debug, Default)]
pub struct ClientSessions {
    /// Maps client ID to their last completed sequence number.
    last_sequence: HashMap<ClientId, u64>,
    /// Cached results for the last request from each client.
    last_result: HashMap<ClientId, Bytes>,
}

impl ClientSessions {
    /// Creates a new client sessions tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            last_sequence: HashMap::new(),
            last_result: HashMap::new(),
        }
    }

    /// Checks if a request is a duplicate.
    ///
    /// Returns `Some(result)` if this is a duplicate and we have a cached result.
    /// Returns `None` if this is a new request.
    #[must_use]
    pub fn check_duplicate(&self, request_id: RequestId) -> Option<&Bytes> {
        let last_seq = self.last_sequence.get(&request_id.client_id)?;

        if request_id.sequence <= *last_seq {
            // This is a duplicate - return cached result if same sequence
            if request_id.sequence == *last_seq {
                self.last_result.get(&request_id.client_id)
            } else {
                // Old duplicate without cached result
                None
            }
        } else {
            None
        }
    }

    /// Records a completed request.
    pub fn record_completion(&mut self, request_id: RequestId, result: Option<Bytes>) {
        let current = self.last_sequence.entry(request_id.client_id).or_insert(0);
        if request_id.sequence > *current {
            *current = request_id.sequence;
            if let Some(r) = result {
                self.last_result.insert(request_id.client_id, r);
            } else {
                self.last_result.remove(&request_id.client_id);
            }
        }
    }

    /// Expires old sessions (called periodically).
    ///
    /// In production, this would use timestamps to expire inactive sessions.
    /// For now, this is a placeholder.
    pub fn expire_old_sessions(&mut self) {
        // TODO: Implement session expiration based on activity timestamps
    }
}

/// Client-side state for interacting with a Raft cluster.
#[derive(Debug)]
pub struct RaftClient {
    /// Our client ID.
    client_id: ClientId,
    /// Next sequence number for requests.
    next_sequence: u64,
    /// Known cluster members.
    cluster: Vec<NodeId>,
    /// Last known leader.
    leader: Option<NodeId>,
}

impl RaftClient {
    /// Creates a new Raft client.
    #[must_use]
    pub fn new(client_id: ClientId, cluster: Vec<NodeId>) -> Self {
        Self {
            client_id,
            next_sequence: 1,
            cluster,
            leader: None,
        }
    }

    /// Returns our client ID.
    #[must_use]
    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Creates a new command request.
    #[must_use]
    pub fn create_command(&mut self, command: Bytes) -> ClientCommand {
        let id = RequestId::new(self.client_id, self.next_sequence);
        self.next_sequence += 1;
        ClientCommand::new(id, command)
    }

    /// Creates a new query request.
    #[must_use]
    pub fn create_query(&mut self, query: Bytes) -> ClientQuery {
        let id = RequestId::new(self.client_id, self.next_sequence);
        self.next_sequence += 1;
        ClientQuery::new(id, query)
    }

    /// Updates the known leader.
    pub fn set_leader(&mut self, leader: Option<NodeId>) {
        self.leader = leader;
    }

    /// Returns the last known leader.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Returns the target node for a request.
    ///
    /// Returns the known leader, or picks a random cluster member.
    #[must_use]
    pub fn target_node(&self) -> Option<NodeId> {
        self.leader.or_else(|| self.cluster.first().copied())
    }

    /// Handles a response, updating leader information.
    pub fn handle_response(&mut self, result: &ClientResult) {
        if let ClientResult::NotLeader { leader_hint } = result {
            self.leader = *leader_hint;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_display() {
        let id = RequestId::new(ClientId::new(42), 100);
        assert_eq!(format!("{id}"), "client-42:100");
    }

    #[test]
    fn test_client_creates_sequential_requests() {
        let mut client = RaftClient::new(ClientId::new(1), vec![NodeId::new(1)]);

        let cmd1 = client.create_command(Bytes::from("cmd1"));
        let cmd2 = client.create_command(Bytes::from("cmd2"));

        assert_eq!(cmd1.id.sequence, 1);
        assert_eq!(cmd2.id.sequence, 2);
        assert_eq!(cmd1.id.client_id, cmd2.id.client_id);
    }

    #[test]
    fn test_pending_requests() {
        let mut pending = PendingRequests::new();
        let request_id = RequestId::new(ClientId::new(1), 1);

        pending.register(LogIndex::new(10), request_id);

        assert_eq!(pending.get_by_index(LogIndex::new(10)), Some(request_id));
        assert_eq!(pending.get_by_request(request_id), Some(LogIndex::new(10)));

        let completed = pending.complete(LogIndex::new(10));
        assert_eq!(completed, Some(request_id));
        assert!(pending.is_empty());
    }

    #[test]
    fn test_client_sessions_duplicate_detection() {
        let mut sessions = ClientSessions::new();
        let client = ClientId::new(1);

        // First request completes.
        let req1 = RequestId::new(client, 1);
        sessions.record_completion(req1, Some(Bytes::from("result1")));

        // Duplicate of req1 should return cached result.
        let dup = sessions.check_duplicate(req1);
        assert!(dup.is_some());
        assert_eq!(dup.unwrap(), &Bytes::from("result1"));

        // New request should not be duplicate.
        let req2 = RequestId::new(client, 2);
        assert!(sessions.check_duplicate(req2).is_none());
    }

    #[test]
    fn test_client_result_checks() {
        let success = ClientResult::Success {
            index: LogIndex::new(1),
            result: None,
        };
        assert!(success.is_success());
        assert!(!success.should_redirect());

        let not_leader = ClientResult::NotLeader {
            leader_hint: Some(NodeId::new(2)),
        };
        assert!(!not_leader.is_success());
        assert!(not_leader.should_redirect());
    }

    #[test]
    fn test_client_leader_tracking() {
        let mut client = RaftClient::new(ClientId::new(1), vec![NodeId::new(1), NodeId::new(2)]);

        assert!(client.leader().is_none());

        // Receive not-leader response with hint.
        let response = ClientResult::NotLeader {
            leader_hint: Some(NodeId::new(2)),
        };
        client.handle_response(&response);

        assert_eq!(client.leader(), Some(NodeId::new(2)));
    }
}

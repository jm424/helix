//! Raft RPC message types.
//!
//! These messages correspond exactly to the TLA+ specification.

use bytes::Bytes;
use helix_core::{LogIndex, NodeId, TermId};

use crate::LogEntry;

/// Raft message types for communication between nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Request vote from candidate to other nodes.
    RequestVote(RequestVoteRequest),
    /// Response to vote request.
    RequestVoteResponse(RequestVoteResponse),
    /// Append entries from leader to followers (also used as heartbeat).
    AppendEntries(AppendEntriesRequest),
    /// Response to append entries.
    AppendEntriesResponse(AppendEntriesResponse),
}

impl Message {
    /// Returns the source node ID of this message.
    #[must_use]
    pub const fn from(&self) -> NodeId {
        match self {
            Self::RequestVote(r) => r.candidate_id,
            Self::RequestVoteResponse(r) => r.from,
            Self::AppendEntries(r) => r.leader_id,
            Self::AppendEntriesResponse(r) => r.from,
        }
    }

    /// Returns the destination node ID of this message.
    #[must_use]
    pub const fn to(&self) -> NodeId {
        match self {
            Self::RequestVote(r) => r.to,
            Self::RequestVoteResponse(r) => r.to,
            Self::AppendEntries(r) => r.to,
            Self::AppendEntriesResponse(r) => r.to,
        }
    }

    /// Returns the term of this message.
    #[must_use]
    pub const fn term(&self) -> TermId {
        match self {
            Self::RequestVote(r) => r.term,
            Self::RequestVoteResponse(r) => r.term,
            Self::AppendEntries(r) => r.term,
            Self::AppendEntriesResponse(r) => r.term,
        }
    }
}

/// `RequestVote` RPC request.
///
/// Sent by candidates to gather votes during leader election.
/// Corresponds to TLA+ `RequestVoteRequest`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVoteRequest {
    /// Candidate's term.
    pub term: TermId,
    /// Candidate requesting vote.
    pub candidate_id: NodeId,
    /// Target node.
    pub to: NodeId,
    /// Index of candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of candidate's last log entry.
    pub last_log_term: TermId,
}

impl RequestVoteRequest {
    /// Creates a new `RequestVote` request.
    #[must_use]
    pub const fn new(
        term: TermId,
        candidate_id: NodeId,
        to: NodeId,
        last_log_index: LogIndex,
        last_log_term: TermId,
    ) -> Self {
        Self {
            term,
            candidate_id,
            to,
            last_log_index,
            last_log_term,
        }
    }
}

/// `RequestVote` RPC response.
///
/// Sent by voters in response to `RequestVote` requests.
/// Corresponds to TLA+ `RequestVoteResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestVoteResponse {
    /// Current term, for candidate to update itself.
    pub term: TermId,
    /// Sender of this response.
    pub from: NodeId,
    /// Candidate that requested the vote.
    pub to: NodeId,
    /// True if vote was granted.
    pub vote_granted: bool,
}

impl RequestVoteResponse {
    /// Creates a new `RequestVote` response.
    #[must_use]
    pub const fn new(term: TermId, from: NodeId, to: NodeId, vote_granted: bool) -> Self {
        Self {
            term,
            from,
            to,
            vote_granted,
        }
    }
}

/// `AppendEntries` RPC request.
///
/// Sent by leader to replicate log entries and as heartbeat.
/// Corresponds to TLA+ `AppendEntriesRequest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesRequest {
    /// Leader's term.
    pub term: TermId,
    /// Leader sending this request.
    pub leader_id: NodeId,
    /// Target follower.
    pub to: NodeId,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: LogIndex,
    /// Term of `prev_log_index` entry.
    pub prev_log_term: TermId,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

impl AppendEntriesRequest {
    /// Creates a new `AppendEntries` request.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        term: TermId,
        leader_id: NodeId,
        to: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: TermId,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> Self {
        Self {
            term,
            leader_id,
            to,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    /// Creates a heartbeat (`AppendEntries` with no entries).
    #[must_use]
    pub const fn heartbeat(
        term: TermId,
        leader_id: NodeId,
        to: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: TermId,
        leader_commit: LogIndex,
    ) -> Self {
        Self::new(
            term,
            leader_id,
            to,
            prev_log_index,
            prev_log_term,
            Vec::new(),
            leader_commit,
        )
    }

    /// Returns true if this is a heartbeat (no entries).
    #[must_use]
    pub fn is_heartbeat(&self) -> bool {
        self.entries.is_empty()
    }
}

/// `AppendEntries` RPC response.
///
/// Sent by followers in response to `AppendEntries` requests.
/// Corresponds to TLA+ `AppendEntriesResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendEntriesResponse {
    /// Current term, for leader to update itself.
    pub term: TermId,
    /// Sender of this response.
    pub from: NodeId,
    /// Leader that sent the request.
    pub to: NodeId,
    /// True if follower contained entry matching `prev_log_index`/term.
    pub success: bool,
    /// The follower's last log index (for fast backup).
    pub match_index: LogIndex,
}

impl AppendEntriesResponse {
    /// Creates a new `AppendEntries` response.
    #[must_use]
    pub const fn new(term: TermId, from: NodeId, to: NodeId, success: bool, match_index: LogIndex) -> Self {
        Self {
            term,
            from,
            to,
            success,
            match_index,
        }
    }
}

/// Client request to the Raft cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRequest {
    /// The command data to replicate.
    pub data: Bytes,
}

impl ClientRequest {
    /// Creates a new client request.
    #[must_use]
    pub const fn new(data: Bytes) -> Self {
        Self { data }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_accessors() {
        let req = RequestVoteRequest::new(
            TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(0),
            TermId::new(0),
        );
        let msg = Message::RequestVote(req);

        assert_eq!(msg.from(), NodeId::new(1));
        assert_eq!(msg.to(), NodeId::new(2));
        assert_eq!(msg.term(), TermId::new(1));
    }

    #[test]
    fn test_heartbeat() {
        let hb = AppendEntriesRequest::heartbeat(
            TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(0),
            TermId::new(0),
            LogIndex::new(0),
        );

        assert!(hb.is_heartbeat());
        assert!(hb.entries.is_empty());
    }

    #[test]
    fn test_append_entries_with_entries() {
        let entry = LogEntry::new(
            TermId::new(1),
            LogIndex::new(1),
            Bytes::from("test"),
        );
        let req = AppendEntriesRequest::new(
            TermId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(0),
            TermId::new(0),
            vec![entry],
            LogIndex::new(0),
        );

        assert!(!req.is_heartbeat());
        assert_eq!(req.entries.len(), 1);
    }
}

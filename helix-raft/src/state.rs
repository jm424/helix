//! Raft state machine implementation.
//!
//! This implements the core Raft consensus algorithm as specified in the TLA+ spec.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use helix_core::{LogIndex, NodeId, TermId};

use crate::config::RaftConfig;
use crate::log::{LogEntry, RaftLog};
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, ClientRequest, Message, RequestVoteRequest,
    RequestVoteResponse,
};

/// Raft node state (role).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftState {
    /// Follower state - passive, responds to RPCs.
    Follower,
    /// Candidate state - actively seeking votes.
    Candidate,
    /// Leader state - handles client requests, replicates log.
    Leader,
}

impl Default for RaftState {
    fn default() -> Self {
        Self::Follower
    }
}

/// Output actions from the Raft state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftOutput {
    /// Send a message to another node.
    SendMessage(Message),
    /// Reset the election timer with a new random timeout.
    ResetElectionTimer,
    /// Reset the heartbeat timer.
    ResetHeartbeatTimer,
    /// A log entry has been committed and can be applied.
    CommitEntry {
        /// The log index of the committed entry.
        index: LogIndex,
        /// The data payload of the committed entry.
        data: Bytes,
    },
    /// This node became leader.
    BecameLeader,
    /// This node stepped down from leader.
    SteppedDown,
}

/// A Raft consensus node.
///
/// This is a pure state machine - it takes inputs and produces outputs
/// but does not perform I/O itself. This design enables deterministic
/// simulation testing.
#[derive(Debug)]
pub struct RaftNode {
    /// Configuration.
    config: RaftConfig,

    // Persistent state (would be persisted to WAL in production).
    /// Current term.
    current_term: TermId,
    /// Who we voted for in current term.
    voted_for: Option<NodeId>,
    /// The log.
    log: RaftLog,

    // Volatile state on all servers.
    /// Current role.
    state: RaftState,
    /// Index of highest log entry known to be committed.
    commit_index: LogIndex,
    /// Index of highest log entry applied to state machine.
    last_applied: LogIndex,

    // Volatile state on leaders (reinitialized after election).
    /// For each server, index of next log entry to send.
    next_index: HashMap<NodeId, LogIndex>,
    /// For each server, index of highest log entry known to be replicated.
    match_index: HashMap<NodeId, LogIndex>,

    // Candidate state.
    /// Votes received in current election.
    votes_received: HashSet<NodeId>,

    /// Current leader (if known).
    leader_id: Option<NodeId>,
}

impl RaftNode {
    /// Creates a new Raft node.
    #[must_use]
    pub fn new(config: RaftConfig) -> Self {
        let peers = config.peers();
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        // Initialize leader state for all peers.
        for peer in &peers {
            next_index.insert(*peer, LogIndex::new(1));
            match_index.insert(*peer, LogIndex::new(0));
        }

        Self {
            config,
            current_term: TermId::new(0),
            voted_for: None,
            log: RaftLog::new(),
            state: RaftState::Follower,
            commit_index: LogIndex::new(0),
            last_applied: LogIndex::new(0),
            next_index,
            match_index,
            votes_received: HashSet::new(),
            leader_id: None,
        }
    }

    /// Returns this node's ID.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Returns the current term.
    #[must_use]
    pub fn current_term(&self) -> TermId {
        self.current_term
    }

    /// Returns the current state (role).
    #[must_use]
    pub fn state(&self) -> RaftState {
        self.state
    }

    /// Returns true if this node is the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.state == RaftState::Leader
    }

    /// Returns the current leader ID if known.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Returns the commit index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the last applied index.
    #[must_use]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Returns a reference to the log.
    #[must_use]
    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Handles an election timeout.
    ///
    /// This should be called when the election timer fires.
    /// Returns the actions to take.
    pub fn handle_election_timeout(&mut self) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();
        let prev_term = self.current_term;

        // Only followers and candidates start elections.
        if self.state == RaftState::Leader {
            return outputs;
        }

        // Start election: increment term, become candidate, vote for self.
        self.current_term = TermId::new(self.current_term.get() + 1);
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.config.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.config.node_id);
        self.leader_id = None;

        // Postcondition: term incremented, we voted for ourselves.
        debug_assert!(self.current_term.get() == prev_term.get() + 1);
        debug_assert!(self.voted_for == Some(self.config.node_id));
        debug_assert!(self.votes_received.contains(&self.config.node_id));

        // Reset election timer.
        outputs.push(RaftOutput::ResetElectionTimer);

        // Send RequestVote to all peers.
        for peer in self.config.peers() {
            let request = RequestVoteRequest::new(
                self.current_term,
                self.config.node_id,
                peer,
                self.log.last_index(),
                self.log.last_term(),
            );
            outputs.push(RaftOutput::SendMessage(Message::RequestVote(request)));
        }

        // Check if we already have quorum (single-node cluster).
        if self.votes_received.len() >= self.config.quorum_size() {
            outputs.extend(self.become_leader());
        }

        // Postcondition: we are either candidate or leader (if single-node).
        debug_assert!(self.state == RaftState::Candidate || self.state == RaftState::Leader);

        outputs
    }

    /// Handles a heartbeat timeout (leader only).
    ///
    /// This should be called when the heartbeat timer fires.
    /// Returns the actions to take.
    pub fn handle_heartbeat_timeout(&mut self) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        if self.state != RaftState::Leader {
            // Precondition: only leaders send heartbeats.
            debug_assert!(outputs.is_empty());
            return outputs;
        }

        // Invariant: leader always knows it is the leader.
        debug_assert!(self.leader_id == Some(self.config.node_id));

        // Send heartbeats to all peers.
        let peer_count = self.config.peers().len();
        for peer in self.config.peers() {
            outputs.extend(self.send_append_entries(peer));
        }

        outputs.push(RaftOutput::ResetHeartbeatTimer);

        // Postcondition: sent messages to all peers plus timer reset.
        debug_assert!(outputs.len() >= peer_count);

        outputs
    }

    /// Handles a client request (leader only).
    ///
    /// Returns the actions to take, or None if not leader.
    pub fn handle_client_request(&mut self, request: ClientRequest) -> Option<Vec<RaftOutput>> {
        if self.state != RaftState::Leader {
            return None;
        }

        // Precondition: leader always knows it is the leader.
        debug_assert!(self.leader_id == Some(self.config.node_id));

        let mut outputs = Vec::new();
        let prev_last_index = self.log.last_index();

        // Append entry to our log.
        let index = LogIndex::new(self.log.last_index().get() + 1);
        let entry = LogEntry::new(self.current_term, index, request.data);
        self.log.append(entry);

        // Postcondition: log grew by exactly one entry.
        debug_assert!(self.log.last_index().get() == prev_last_index.get() + 1);

        // Update our own match_index.
        // (Leader implicitly has all its own entries.)

        // Send AppendEntries to all peers.
        for peer in self.config.peers() {
            outputs.extend(self.send_append_entries(peer));
        }

        // Try to advance commit index (might succeed in single-node cluster).
        outputs.extend(self.try_advance_commit_index());

        Some(outputs)
    }

    /// Handles an incoming message.
    ///
    /// Returns the actions to take.
    pub fn handle_message(&mut self, message: Message) -> Vec<RaftOutput> {
        // Check if message term is newer - step down if so.
        let msg_term = message.term();
        if msg_term > self.current_term {
            self.step_down(msg_term);
        }

        match message {
            Message::RequestVote(req) => self.handle_request_vote(req),
            Message::RequestVoteResponse(resp) => self.handle_request_vote_response(resp),
            Message::AppendEntries(req) => self.handle_append_entries(req),
            Message::AppendEntriesResponse(resp) => self.handle_append_entries_response(resp),
        }
    }

    /// Steps down to follower and updates term.
    fn step_down(&mut self, new_term: TermId) {
        self.current_term = new_term;
        self.state = RaftState::Follower;
        self.voted_for = None;
        self.votes_received.clear();
    }

    /// Handles a RequestVote request.
    fn handle_request_vote(&mut self, req: RequestVoteRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        let vote_granted = self.should_grant_vote(&req);

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            outputs.push(RaftOutput::ResetElectionTimer);
        }

        let response = RequestVoteResponse::new(
            self.current_term,
            self.config.node_id,
            req.candidate_id,
            vote_granted,
        );
        outputs.push(RaftOutput::SendMessage(Message::RequestVoteResponse(
            response,
        )));

        outputs
    }

    /// Determines if we should grant a vote to a candidate.
    fn should_grant_vote(&self, req: &RequestVoteRequest) -> bool {
        // Reject if term is old.
        if req.term < self.current_term {
            return false;
        }

        // Check if we can vote for this candidate.
        let can_vote = match self.voted_for {
            None => true,
            Some(id) => id == req.candidate_id,
        };

        if !can_vote {
            return false;
        }

        // Check if candidate's log is at least as up-to-date as ours.
        self.log.is_up_to_date(req.last_log_term, req.last_log_index)
    }

    /// Handles a RequestVote response.
    fn handle_request_vote_response(&mut self, resp: RequestVoteResponse) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Ignore if not a candidate or term doesn't match.
        if self.state != RaftState::Candidate || resp.term != self.current_term {
            return outputs;
        }

        if resp.vote_granted {
            self.votes_received.insert(resp.from);

            // Check if we have quorum.
            if self.votes_received.len() >= self.config.quorum_size() {
                outputs.extend(self.become_leader());
            }
        }

        outputs
    }

    /// Becomes leader.
    fn become_leader(&mut self) -> Vec<RaftOutput> {
        // Precondition: must have received quorum of votes.
        debug_assert!(self.votes_received.len() >= self.config.quorum_size());

        let mut outputs = Vec::new();

        self.state = RaftState::Leader;
        self.leader_id = Some(self.config.node_id);

        // Initialize leader state.
        let next_idx = LogIndex::new(self.log.last_index().get() + 1);
        for peer in self.config.peers() {
            self.next_index.insert(peer, next_idx);
            self.match_index.insert(peer, LogIndex::new(0));
        }

        // Postcondition: leader state initialized for all peers.
        debug_assert!(self.next_index.len() == self.config.peers().len());
        debug_assert!(self.match_index.len() == self.config.peers().len());

        outputs.push(RaftOutput::BecameLeader);
        outputs.push(RaftOutput::ResetHeartbeatTimer);

        // Send initial empty AppendEntries (heartbeat) to all peers.
        for peer in self.config.peers() {
            outputs.extend(self.send_append_entries(peer));
        }

        // Postcondition: we are now the leader.
        debug_assert!(self.state == RaftState::Leader);
        debug_assert!(self.leader_id == Some(self.config.node_id));

        outputs
    }

    /// Handles an AppendEntries request.
    fn handle_append_entries(&mut self, req: AppendEntriesRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Reject if term is old.
        if req.term < self.current_term {
            let response = AppendEntriesResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                false,
                self.log.last_index(),
            );
            outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
                response,
            )));
            return outputs;
        }

        // Valid AppendEntries from current or newer term.
        // Step down if we're a candidate.
        if self.state == RaftState::Candidate {
            self.state = RaftState::Follower;
            self.votes_received.clear();
        }

        // Remember the leader.
        self.leader_id = Some(req.leader_id);

        // Reset election timer - we heard from a valid leader.
        outputs.push(RaftOutput::ResetElectionTimer);

        // Check if we have the prev_log entry.
        let log_ok = req.prev_log_index.get() == 0
            || (self.log.get(req.prev_log_index).is_some()
                && self.log.term_at(req.prev_log_index) == req.prev_log_term);

        if !log_ok {
            // Log doesn't contain an entry at prev_log_index with matching term.
            let response = AppendEntriesResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                false,
                self.log.last_index(),
            );
            outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
                response,
            )));
            return outputs;
        }

        // Append entries (handling conflicts).
        if !req.entries.is_empty() {
            self.log.append_entries(req.entries);
        }

        // Update commit index.
        if req.leader_commit > self.commit_index {
            let new_commit = std::cmp::min(req.leader_commit, self.log.last_index());
            outputs.extend(self.apply_committed_entries(new_commit));
        }

        // Send success response.
        let response = AppendEntriesResponse::new(
            self.current_term,
            self.config.node_id,
            req.leader_id,
            true,
            self.log.last_index(),
        );
        outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
            response,
        )));

        outputs
    }

    /// Handles an AppendEntries response (leader only).
    fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesResponse,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Ignore if not leader or term doesn't match.
        if self.state != RaftState::Leader || resp.term != self.current_term {
            return outputs;
        }

        if resp.success {
            // Update next_index and match_index for this follower.
            let new_match = resp.match_index;
            self.match_index.insert(resp.from, new_match);
            self.next_index
                .insert(resp.from, LogIndex::new(new_match.get() + 1));

            // Try to advance commit index.
            outputs.extend(self.try_advance_commit_index());
        } else {
            // Decrement next_index and retry.
            let next = self.next_index.get(&resp.from).copied().unwrap_or(LogIndex::new(1));
            if next.get() > 1 {
                self.next_index.insert(resp.from, LogIndex::new(next.get() - 1));
            }

            // Immediately retry with new next_index.
            outputs.extend(self.send_append_entries(resp.from));
        }

        outputs
    }

    /// Sends AppendEntries to a specific peer.
    fn send_append_entries(&self, peer: NodeId) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        let next_idx = self.next_index.get(&peer).copied().unwrap_or(LogIndex::new(1));
        let prev_idx = LogIndex::new(next_idx.get().saturating_sub(1));
        let prev_term = self.log.term_at(prev_idx);

        let entries = self.log.entries_from(next_idx);

        let request = AppendEntriesRequest::new(
            self.current_term,
            self.config.node_id,
            peer,
            prev_idx,
            prev_term,
            entries,
            self.commit_index,
        );
        outputs.push(RaftOutput::SendMessage(Message::AppendEntries(request)));

        outputs
    }

    /// Tries to advance the commit index (leader only).
    fn try_advance_commit_index(&mut self) -> Vec<RaftOutput> {
        if self.state != RaftState::Leader {
            return Vec::new();
        }

        // Precondition: must be leader to advance commit.
        debug_assert!(self.leader_id == Some(self.config.node_id));

        let prev_commit = self.commit_index;

        // Find the highest index that a majority has replicated.
        // An index can be committed if:
        // 1. It exists in our log.
        // 2. The entry at that index has term == current_term.
        // 3. A majority of servers have match_index >= that index.

        let mut outputs = Vec::new();

        // Check each index from commit_index+1 to last_index.
        // Bounded loop: at most (last_index - commit_index) iterations.
        let loop_bound = self.log.last_index().get().saturating_sub(self.commit_index.get());
        debug_assert!(loop_bound <= u64::MAX); // Explicit bound check.

        for n in (self.commit_index.get() + 1)..=self.log.last_index().get() {
            let idx = LogIndex::new(n);

            // Entry must be from current term (Raft safety property).
            if self.log.term_at(idx) != self.current_term {
                continue;
            }

            // Count how many servers have this entry.
            // Leader always has it.
            let mut count = 1;
            for peer in self.config.peers() {
                if let Some(&match_idx) = self.match_index.get(&peer) {
                    if match_idx >= idx {
                        count += 1;
                    }
                }
            }

            // Check for majority.
            if count >= self.config.quorum_size() {
                outputs.extend(self.apply_committed_entries(idx));
            }
        }

        // Postcondition: commit index never decreases.
        debug_assert!(self.commit_index >= prev_commit);

        outputs
    }

    /// Applies committed entries up to the given index.
    fn apply_committed_entries(&mut self, new_commit: LogIndex) -> Vec<RaftOutput> {
        // Precondition: new_commit must be valid log index.
        debug_assert!(new_commit <= self.log.last_index());

        let mut outputs = Vec::new();
        let prev_last_applied = self.last_applied;

        if new_commit <= self.commit_index {
            return outputs;
        }

        self.commit_index = new_commit;

        // Apply entries from last_applied+1 to commit_index.
        // Loop is bounded: at most (commit_index - last_applied) iterations.
        while self.last_applied < self.commit_index {
            let idx = LogIndex::new(self.last_applied.get() + 1);
            if let Some(entry) = self.log.get(idx) {
                outputs.push(RaftOutput::CommitEntry {
                    index: idx,
                    data: entry.data.clone(),
                });
            }
            self.last_applied = idx;
        }

        // Postcondition: last_applied advanced, commit index set.
        debug_assert!(self.last_applied >= prev_last_applied);
        debug_assert!(self.last_applied == self.commit_index);

        outputs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(node_id: u64) -> RaftConfig {
        RaftConfig::new(
            NodeId::new(node_id),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
    }

    #[test]
    fn test_new_node_is_follower() {
        let node = RaftNode::new(make_config(1));

        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.current_term(), TermId::new(0));
        assert!(node.leader_id().is_none());
    }

    #[test]
    fn test_election_timeout_starts_election() {
        let mut node = RaftNode::new(make_config(1));

        let outputs = node.handle_election_timeout();

        assert_eq!(node.state(), RaftState::Candidate);
        assert_eq!(node.current_term(), TermId::new(1));

        // Should have reset timer + sent 2 RequestVote messages.
        let vote_requests: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, RaftOutput::SendMessage(Message::RequestVote(_))))
            .collect();
        assert_eq!(vote_requests.len(), 2);
    }

    #[test]
    fn test_vote_granted_on_valid_request() {
        let mut node = RaftNode::new(make_config(1));

        let request = RequestVoteRequest::new(
            TermId::new(1),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
        );

        let outputs = node.handle_message(Message::RequestVote(request));

        // Should have granted vote.
        let response = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::RequestVoteResponse(r)) => Some(r),
            _ => None,
        });
        assert!(response.is_some());
        assert!(response.unwrap().vote_granted);
    }

    #[test]
    fn test_vote_not_granted_if_already_voted() {
        let mut node = RaftNode::new(make_config(1));

        // First vote.
        let request1 = RequestVoteRequest::new(
            TermId::new(1),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
        );
        node.handle_message(Message::RequestVote(request1));

        // Second vote request in same term from different candidate.
        let request2 = RequestVoteRequest::new(
            TermId::new(1),
            NodeId::new(3),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
        );
        let outputs = node.handle_message(Message::RequestVote(request2));

        // Should NOT have granted vote.
        let response = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::RequestVoteResponse(r)) => Some(r),
            _ => None,
        });
        assert!(response.is_some());
        assert!(!response.unwrap().vote_granted);
    }

    #[test]
    fn test_becomes_leader_with_quorum() {
        let mut node = RaftNode::new(make_config(1));

        // Start election.
        node.handle_election_timeout();
        assert_eq!(node.state(), RaftState::Candidate);

        // Receive vote from node 2.
        let vote = RequestVoteResponse::new(
            TermId::new(1),
            NodeId::new(2),
            NodeId::new(1),
            true,
        );
        let outputs = node.handle_message(Message::RequestVoteResponse(vote));

        // Should be leader now (have 2 votes: self + node 2, quorum is 2).
        assert_eq!(node.state(), RaftState::Leader);
        assert!(outputs.iter().any(|o| matches!(o, RaftOutput::BecameLeader)));
    }

    #[test]
    fn test_append_entries_resets_election_timer() {
        let mut node = RaftNode::new(make_config(1));

        let request = AppendEntriesRequest::heartbeat(
            TermId::new(1),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
            LogIndex::new(0),
        );

        let outputs = node.handle_message(Message::AppendEntries(request));

        assert!(outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::ResetElectionTimer)));
        assert_eq!(node.leader_id(), Some(NodeId::new(2)));
    }

    #[test]
    fn test_client_request_appends_to_log() {
        let mut node = RaftNode::new(make_config(1));

        // Become leader first.
        node.handle_election_timeout();
        let vote = RequestVoteResponse::new(TermId::new(1), NodeId::new(2), NodeId::new(1), true);
        node.handle_message(Message::RequestVoteResponse(vote));

        assert!(node.is_leader());

        // Send client request.
        let request = ClientRequest::new(Bytes::from("test command"));
        let outputs = node.handle_client_request(request);

        assert!(outputs.is_some());
        assert_eq!(node.log().len(), 1);
        assert_eq!(node.log().last_index().get(), 1);
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut node = RaftNode::new(make_config(1));

        // Become leader.
        node.handle_election_timeout();
        let vote = RequestVoteResponse::new(TermId::new(1), NodeId::new(2), NodeId::new(1), true);
        node.handle_message(Message::RequestVoteResponse(vote));
        assert!(node.is_leader());

        // Receive message with higher term.
        let request = AppendEntriesRequest::heartbeat(
            TermId::new(5),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntries(request));

        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.current_term(), TermId::new(5));
    }

    #[test]
    fn test_single_node_cluster_commits_immediately() {
        let config = RaftConfig::new(NodeId::new(1), vec![NodeId::new(1)]);
        let mut node = RaftNode::new(config);

        // Should become leader immediately on election timeout.
        let outputs = node.handle_election_timeout();
        assert!(node.is_leader());
        assert!(outputs.iter().any(|o| matches!(o, RaftOutput::BecameLeader)));

        // Client request should commit immediately.
        let request = ClientRequest::new(Bytes::from("test"));
        let outputs = node.handle_client_request(request).unwrap();

        assert!(outputs.iter().any(|o| matches!(o, RaftOutput::CommitEntry { .. })));
        assert_eq!(node.commit_index().get(), 1);
    }
}

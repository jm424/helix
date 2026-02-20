//! Raft state machine implementation.
//!
//! This implements the core Raft consensus algorithm with tick-based timing.
//! The application calls `tick()` at regular intervals, and the library
//! internally tracks elapsed ticks to determine when to trigger elections
//! and heartbeats.
//!
//! # Design
//!
//! - **Tick-based timing**: No external timers needed - just call `tick()` periodically
//! - **Randomized election timeout**: Prevents split votes and thundering herd
//! - **Deterministic**: All randomness from seeded PRNG for simulation testing
//! - **Pre-vote extension**: Prevents disruption from partitioned nodes

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use helix_core::{LogIndex, NodeId, TermId, MAX_RAFT_MESSAGE_BYTES};

use tracing::{debug, info, warn};

use crate::config::RaftConfig;
use crate::limits;
use crate::log::{LogEntry, RaftLog};
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, ClientRequest, InstallSnapshotRequest,
    InstallSnapshotResponse, Message, PreVoteRequest, PreVoteResponse, RequestVoteRequest,
    RequestVoteResponse, TimeoutNowRequest,
};
use crate::snapshot::{Snapshot, SnapshotMeta};

/// Raft node state (role).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RaftState {
    /// Follower state - passive, responds to RPCs.
    #[default]
    Follower,
    /// Pre-candidate state - gathering pre-votes before starting election.
    /// This prevents disruption from partitioned nodes.
    PreCandidate,
    /// Candidate state - actively seeking votes.
    Candidate,
    /// Leader state - handles client requests, replicates log.
    Leader,
}

/// Replication state for a single follower (leader only).
///
/// Tracks the progress of replicating the log to a follower, including
/// pipelining state for in-flight `AppendEntries` requests.
#[derive(Debug, Clone)]
struct ReplicationState {
    /// Index of next log entry to send to this follower.
    next_index: LogIndex,
    /// Index of highest log entry known to be replicated on this follower.
    match_index: LogIndex,
    /// Number of `AppendEntries` currently in flight to this follower.
    /// Bounded by `MAX_INFLIGHT_APPEND_ENTRIES`.
    inflight_count: u32,
    /// Ticks since inflight requests were sent without response.
    /// Reset to 0 when a response is received. Used to timeout stale
    /// inflight state (e.g., after follower crash/recovery).
    inflight_stale_ticks: u32,
}

/// Number of ticks without response before resetting inflight state.
/// This handles the case where a follower crashes and recovers.
const INFLIGHT_TIMEOUT_TICKS: u32 = 20;

impl ReplicationState {
    /// Creates new replication state with the given `next_index`.
    const fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: LogIndex::new(0),
            inflight_count: 0,
            inflight_stale_ticks: 0,
        }
    }
}

/// Output actions from the Raft state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftOutput {
    /// Send a message to another node.
    SendMessage(Message),
    /// A log entry has been committed and can be applied.
    CommitEntry {
        /// The log index of the committed entry.
        index: LogIndex,
        /// The Raft term of the committed entry.
        term: TermId,
        /// Command header of the committed entry.
        metadata: Bytes,
        /// Blob payload of the committed entry (empty for non-blob commands).
        payload: Bytes,
    },
    /// This node became leader.
    BecameLeader,
    /// This node stepped down from leader.
    SteppedDown,
    /// Vote state changed (term or `voted_for`).
    ///
    /// This output is emitted whenever the node's `term` or `voted_for` changes.
    /// The caller should persist this state to prevent double-voting on restart.
    VoteStateChanged {
        /// The new term.
        term: TermId,
        /// Who we voted for in the new term (None if haven't voted).
        voted_for: Option<NodeId>,
    },
    /// Leader needs entries from the data WAL to serve a follower.
    ///
    /// Emitted when the leader's in-memory log doesn't have entries that
    /// exist in the data WAL (after restart with ephemeral log). The caller
    /// should read entries from the WAL and call `provide_entries()`.
    NeedEntries {
        /// The follower that needs entries.
        follower_id: NodeId,
        /// First entry index to read from the WAL.
        start_index: LogIndex,
        /// Index before `start_index` (for `prev_log_term` lookup).
        prev_log_index: LogIndex,
        /// Maximum bytes of entries to read.
        max_bytes: u64,
    },
}

/// Simple linear congruential generator for deterministic randomization.
///
/// Uses the same constants as glibc for reasonable distribution.
#[derive(Debug, Clone)]
struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    /// Creates a new RNG with the given seed.
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Generates the next random u32.
    const fn next_u32(&mut self) -> u32 {
        // LCG constants from Numerical Recipes.
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.state >> 32) as u32
    }

    /// Generates a random u32 in the range [min, max).
    fn range(&mut self, min: u32, max: u32) -> u32 {
        debug_assert!(min < max, "range requires min < max");
        let range = max - min;
        min + (self.next_u32() % range)
    }
}

/// A Raft consensus node.
///
/// This is a pure state machine with tick-based timing. Call `tick()` at
/// regular intervals (e.g., every 100ms) and the node will internally
/// track when to trigger elections and heartbeats.
///
/// # Example
///
/// ```ignore
/// let config = RaftConfig::new(node_id, peers);
/// let mut node = RaftNode::new(config);
///
/// // In your event loop:
/// loop {
///     // Process any incoming messages.
///     for msg in incoming_messages {
///         let outputs = node.handle_message(msg);
///         process_outputs(outputs);
///     }
///
///     // Tick at regular intervals (e.g., every 100ms).
///     let outputs = node.tick();
///     process_outputs(outputs);
///
///     sleep(Duration::from_millis(100));
/// }
/// ```
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
    /// Replication state for each follower (leader only).
    ///
    /// Tracks `next_index`, `match_index`, and pipelining inflight count.
    replication_state: HashMap<NodeId, ReplicationState>,

    // Pre-candidate state.
    /// Pre-votes received in current pre-election.
    pre_votes_received: HashSet<NodeId>,

    // Candidate state.
    /// Votes received in current election.
    votes_received: HashSet<NodeId>,

    /// Current leader (if known).
    leader_id: Option<NodeId>,

    // Leadership transfer state.
    /// Target node for leadership transfer (if in progress).
    transfer_target: Option<NodeId>,
    /// Ticks elapsed since the transfer started. Used to abort stale
    /// transfers (e.g., target node is dead). Aborts after one election
    /// timeout, same as etcd/Raft.
    transfer_elapsed: u32,

    // Snapshot state.
    /// Latest snapshot metadata (if any).
    snapshot_meta: Option<SnapshotMeta>,

    // Tick-based timing state.
    /// Ticks since last election timeout reset.
    /// Incremented on each tick for followers/candidates.
    election_elapsed: u32,

    /// Ticks since last heartbeat sent.
    /// Incremented on each tick for leaders.
    heartbeat_elapsed: u32,

    /// Randomized election timeout (in ticks).
    ///
    /// Chosen randomly in \[`election_tick`, 2 * `election_tick`) on each reset.
    randomized_election_timeout: u32,

    /// Random number generator for election timeout randomization.
    rng: SimpleRng,

    // Observation mode state (for safe S3 recovery).
    /// Whether observation mode is enabled.
    ///
    /// In observation mode, the node rejects vote requests but learns the
    /// current term from heartbeats. This is used when recovering from S3
    /// where the persisted state may be stale.
    observation_mode: bool,

    /// Ticks remaining in observation mode.
    ///
    /// Decremented each tick. When it reaches 0, observation mode ends.
    observation_ticks_remaining: u32,

    // Recovery state (for crash recovery with ephemeral log).
    /// Last committed index recovered from the data WAL.
    /// Used by Part B for fast-forward commit on followers.
    persisted_commit_index: LogIndex,

    /// Term of the last committed entry recovered from the data WAL.
    persisted_commit_term: TermId,

    /// Followers for which a WAL read is pending.
    ///
    /// While a WAL read is in-flight for a follower, we suppress sending
    /// additional `AppendEntries` to avoid duplicate reads.
    wal_read_pending: HashSet<NodeId>,
}

impl RaftNode {
    /// Creates a new Raft node.
    #[must_use]
    pub fn new(config: RaftConfig) -> Self {
        Self::with_vote_state(config, TermId::new(0), None, false)
    }

    /// Creates a Raft node with restored vote state.
    ///
    /// Use this when recovering from persisted storage to restore the node's
    /// term and `voted_for` state. This prevents double-voting after restarts.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration
    /// * `term` - Persisted term
    /// * `voted_for` - Who we voted for in the persisted term
    /// * `observation_mode` - Enable observation mode for safe S3 recovery
    ///
    /// # Observation Mode
    ///
    /// When recovering from S3 (vs local disk), the persisted state may be
    /// stale because S3 uploads are asynchronous. In observation mode:
    /// - The node rejects all vote requests
    /// - The node learns the current term from heartbeats
    /// - After one election timeout, observation mode ends
    ///
    /// This prevents double-voting when the persisted term is behind the actual
    /// cluster term.
    #[must_use]
    pub fn with_vote_state(
        config: RaftConfig,
        term: TermId,
        voted_for: Option<NodeId>,
        observation_mode: bool,
    ) -> Self {
        let mut rng = SimpleRng::new(config.random_seed);
        let election_tick = config.election_tick;
        let randomized_timeout = rng.range(election_tick, election_tick * 2);

        // In observation mode, wait one full election timeout before participating.
        let observation_ticks = if observation_mode {
            randomized_timeout
        } else {
            0
        };

        Self {
            config,
            current_term: term,
            voted_for,
            log: RaftLog::new(),
            state: RaftState::Follower,
            commit_index: LogIndex::new(0),
            last_applied: LogIndex::new(0),
            // Replication state is initialized when becoming leader.
            replication_state: HashMap::new(),
            pre_votes_received: HashSet::new(),
            votes_received: HashSet::new(),
            leader_id: None,
            transfer_target: None,
            transfer_elapsed: 0,
            snapshot_meta: None,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: randomized_timeout,
            rng,
            observation_mode,
            observation_ticks_remaining: observation_ticks,
            persisted_commit_index: LogIndex::new(0),
            persisted_commit_term: TermId::new(0),
            wal_read_pending: HashSet::new(),
        }
    }

    /// Creates a Raft node with recovery state from the data WAL.
    ///
    /// After restart with an ephemeral log, the data WAL tells us the last
    /// committed index and term. This state is set on the `RaftLog` as
    /// compacted state so `last_index()`, `last_term()`, and `is_up_to_date()`
    /// return correct values. Election safety is preserved automatically.
    ///
    /// # Arguments
    ///
    /// * `config` - Node configuration
    /// * `term` - Persisted vote term
    /// * `voted_for` - Who we voted for in the persisted term
    /// * `observation_mode` - Enable observation mode for safe S3 recovery
    /// * `commit_index` - Last committed Raft log index from data WAL
    /// * `commit_term` - Term of the last committed entry from data WAL
    #[must_use]
    pub fn with_recovery_state(
        config: RaftConfig,
        term: TermId,
        voted_for: Option<NodeId>,
        observation_mode: bool,
        commit_index: LogIndex,
        commit_term: TermId,
    ) -> Self {
        let mut rng = SimpleRng::new(config.random_seed);
        let election_tick = config.election_tick;
        let randomized_timeout = rng.range(election_tick, election_tick * 2);

        let observation_ticks = if observation_mode {
            randomized_timeout
        } else {
            0
        };

        Self {
            config,
            current_term: term,
            voted_for,
            log: RaftLog::with_compacted(commit_index.get(), commit_term.get()),
            state: RaftState::Follower,
            commit_index,
            last_applied: commit_index,
            replication_state: HashMap::new(),
            pre_votes_received: HashSet::new(),
            votes_received: HashSet::new(),
            leader_id: None,
            transfer_target: None,
            transfer_elapsed: 0,
            snapshot_meta: None,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            randomized_election_timeout: randomized_timeout,
            rng,
            observation_mode,
            observation_ticks_remaining: observation_ticks,
            persisted_commit_index: commit_index,
            persisted_commit_term: commit_term,
            wal_read_pending: HashSet::new(),
        }
    }

    /// Returns the who this node voted for in the current term.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns whether observation mode is active.
    #[must_use]
    pub const fn is_observation_mode(&self) -> bool {
        self.observation_mode
    }

    /// Returns this node's ID.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    /// Returns the current term.
    #[must_use]
    pub const fn current_term(&self) -> TermId {
        self.current_term
    }

    /// Returns the current state (role).
    #[must_use]
    pub const fn state(&self) -> RaftState {
        self.state
    }

    /// Returns true if this node is the leader.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        matches!(self.state, RaftState::Leader)
    }

    /// Returns the current leader ID if known.
    #[must_use]
    pub const fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// Returns the commit index.
    #[must_use]
    pub const fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the last applied index.
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Returns the persisted commit index recovered from the data WAL.
    #[must_use]
    pub const fn persisted_commit_index(&self) -> LogIndex {
        self.persisted_commit_index
    }

    /// Returns the persisted commit term recovered from the data WAL.
    #[must_use]
    pub const fn persisted_commit_term(&self) -> TermId {
        self.persisted_commit_term
    }

    /// Returns a reference to the log.
    #[must_use]
    pub const fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Returns the current election elapsed ticks.
    #[must_use]
    pub const fn election_elapsed(&self) -> u32 {
        self.election_elapsed
    }

    /// Returns the current randomized election timeout.
    #[must_use]
    pub const fn randomized_election_timeout(&self) -> u32 {
        self.randomized_election_timeout
    }

    /// Advances the internal logical clock by one tick.
    ///
    /// This is the main driver for Raft timing. Call this at regular intervals
    /// (e.g., every 100ms). The node internally tracks elapsed ticks and
    /// triggers elections/heartbeats when appropriate.
    ///
    /// # Returns
    ///
    /// Actions to take (messages to send, entries committed, etc.).
    pub fn tick(&mut self) -> Vec<RaftOutput> {
        match self.state {
            RaftState::Leader => self.tick_leader(),
            RaftState::Follower | RaftState::PreCandidate | RaftState::Candidate => {
                self.tick_election()
            }
        }
    }

    /// Tick processing for leaders.
    ///
    /// Sends heartbeats when `heartbeat_elapsed` >= `heartbeat_tick`.
    /// Also tracks inflight timeouts to handle follower crash/recovery.
    fn tick_leader(&mut self) -> Vec<RaftOutput> {
        self.heartbeat_elapsed += 1;

        // Abort stale leadership transfers. If the target node is dead or
        // unreachable, the transfer will never complete. Abort after one
        // election timeout to unblock proposals. Same policy as etcd/Raft.
        if self.transfer_target.is_some() {
            self.transfer_elapsed += 1;
            if self.transfer_elapsed >= self.config.election_tick {
                warn!(
                    node_id = self.config.node_id.get(),
                    target = ?self.transfer_target,
                    elapsed = self.transfer_elapsed,
                    "Aborting stale leadership transfer (target unreachable)"
                );
                self.transfer_target = None;
                self.transfer_elapsed = 0;
            }
        }

        // Track inflight timeouts for each follower.
        // If we've been waiting too long for responses, reset inflight state.
        // This handles the case where a follower crashes and recovers.
        for state in self.replication_state.values_mut() {
            if state.inflight_count > 0 {
                state.inflight_stale_ticks += 1;
                if state.inflight_stale_ticks >= INFLIGHT_TIMEOUT_TICKS {
                    // Assume inflight requests are lost. Reset state.
                    state.inflight_count = 0;
                    state.inflight_stale_ticks = 0;
                }
            }
        }

        if self.heartbeat_elapsed >= self.config.heartbeat_tick {
            self.heartbeat_elapsed = 0;
            self.send_heartbeats()
        } else {
            Vec::new()
        }
    }

    /// Tick processing for non-leaders.
    ///
    /// Starts election when `election_elapsed` >= `randomized_timeout`.
    /// In observation mode, decrements the observation timer instead of
    /// starting elections.
    fn tick_election(&mut self) -> Vec<RaftOutput> {
        // Handle observation mode countdown.
        if self.observation_mode {
            if self.observation_ticks_remaining > 0 {
                self.observation_ticks_remaining -= 1;
            }
            if self.observation_ticks_remaining == 0 {
                self.observation_mode = false;
            }
            // Don't start elections during observation mode.
            // Reset election elapsed to prevent immediate election after exiting.
            self.election_elapsed = 0;
            return Vec::new();
        }

        self.election_elapsed += 1;

        if self.election_elapsed >= self.randomized_election_timeout {
            self.election_elapsed = 0;
            self.reset_randomized_election_timeout();
            self.start_pre_election()
        } else {
            Vec::new()
        }
    }

    /// Resets the election timeout with a new random value.
    fn reset_randomized_election_timeout(&mut self) {
        let election_tick = self.config.election_tick;
        self.randomized_election_timeout = self.rng.range(election_tick, election_tick * 2);
    }

    /// Resets the election elapsed counter.
    ///
    /// Called when receiving valid messages from the leader.
    const fn reset_election_elapsed(&mut self) {
        self.election_elapsed = 0;
    }

    /// Sends heartbeats to all followers (leader only).
    fn send_heartbeats(&mut self) -> Vec<RaftOutput> {
        debug_assert!(self.state == RaftState::Leader);
        debug_assert!(self.leader_id == Some(self.config.node_id));

        let mut outputs = Vec::new();
        for peer in self.config.peers() {
            outputs.extend(self.send_append_entries(peer));
        }
        outputs
    }

    /// Starts a pre-election (pre-vote phase).
    ///
    /// With pre-vote enabled, we first gather pre-votes before incrementing
    /// term. This prevents disruption from partitioned nodes.
    fn start_pre_election(&mut self) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Leaders don't start elections.
        if self.state == RaftState::Leader {
            return outputs;
        }

        // Start pre-election: become pre-candidate, gather pre-votes.
        // Don't increment term yet - that happens only after pre-vote succeeds.
        self.state = RaftState::PreCandidate;
        self.pre_votes_received.clear();
        self.pre_votes_received.insert(self.config.node_id); // Vote for self.
        self.leader_id = None;

        // The term we would use if we win the pre-vote.
        let proposed_term = TermId::new(self.current_term.get() + 1);

        // Send PreVote to all peers.
        for peer in self.config.peers() {
            let request = PreVoteRequest::new(
                proposed_term,
                self.config.node_id,
                peer,
                self.log.last_index(),
                self.log.last_term(),
            );
            outputs.push(RaftOutput::SendMessage(Message::PreVote(request)));
        }

        // Check if we already have quorum (single-node cluster).
        if self.pre_votes_received.len() >= self.config.quorum_size() {
            outputs.extend(self.start_election());
        }

        // Postcondition: we are pre-candidate, candidate, or leader.
        debug_assert!(
            self.state == RaftState::PreCandidate
                || self.state == RaftState::Candidate
                || self.state == RaftState::Leader
        );

        outputs
    }

    /// Handles a client request (leader only).
    ///
    /// Returns the actions to take, or None if not leader or transfer in progress.
    pub fn handle_client_request(&mut self, request: ClientRequest) -> Option<Vec<RaftOutput>> {
        if self.state != RaftState::Leader {
            return None;
        }

        // Reject new requests during leadership transfer.
        if self.transfer_target.is_some() {
            return None;
        }

        // Precondition: leader always knows it is the leader.
        debug_assert!(self.leader_id == Some(self.config.node_id));

        let mut outputs = Vec::new();
        let prev_last_index = self.log.last_index();

        // Append entry to our log.
        let index = LogIndex::new(self.log.last_index().get() + 1);
        let entry = LogEntry::new(self.current_term, index, request.metadata, request.payload);
        self.log.append(entry);

        // Postcondition: log grew by exactly one entry.
        debug_assert!(self.log.last_index().get() == prev_last_index.get() + 1);

        // Update our own match_index.
        // (Leader implicitly has all its own entries.)

        // Send AppendEntries to all peers.
        let peers = self.config.peers();
        let peer_count = peers.len();

        // Log inflight state before sending.
        for &peer in &peers {
            if let Some(state) = self.replication_state.get(&peer) {
                debug!(
                    index = index.get(),
                    peer = peer.get(),
                    inflight = state.inflight_count,
                    next_index = state.next_index.get(),
                    match_index = state.match_index.get(),
                    "handle_client_request: peer state before send"
                );
            } else {
                warn!(
                    index = index.get(),
                    peer = peer.get(),
                    "handle_client_request: NO replication state for peer"
                );
            }
        }

        for peer in peers {
            outputs.extend(self.send_append_entries(peer));
        }

        // Count how many SendMessage outputs we generated.
        let msg_count = outputs
            .iter()
            .filter(|o| matches!(o, RaftOutput::SendMessage(_)))
            .count();
        debug!(
            index = index.get(),
            peer_count,
            msg_count,
            "handle_client_request: proposed entry"
        );

        // Try to advance commit index (might succeed in single-node cluster).
        outputs.extend(self.try_advance_commit_index());

        Some(outputs)
    }

    /// Handles an incoming message.
    ///
    /// Returns the actions to take.
    pub fn handle_message(&mut self, message: Message) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // For pre-vote messages, don't step down based on term.
        // Pre-vote terms are speculative and shouldn't cause state changes.
        let is_pre_vote = matches!(message, Message::PreVote(_) | Message::PreVoteResponse(_));

        // Check if message term is newer - step down if so.
        // Don't step down for pre-vote messages.
        let msg_term = message.term();
        if !is_pre_vote && msg_term > self.current_term {
            outputs.extend(self.step_down(msg_term));
        }

        outputs.extend(match message {
            Message::PreVote(req) => self.handle_pre_vote(req),
            Message::PreVoteResponse(resp) => self.handle_pre_vote_response(resp),
            Message::RequestVote(req) => self.handle_request_vote(req),
            Message::RequestVoteResponse(resp) => self.handle_request_vote_response(resp),
            Message::AppendEntries(req) => self.handle_append_entries(req),
            Message::AppendEntriesResponse(resp) => self.handle_append_entries_response(resp),
            Message::TimeoutNow(req) => self.handle_timeout_now(req),
            Message::InstallSnapshot(ref req) => self.handle_install_snapshot(req),
            Message::InstallSnapshotResponse(resp) => self.handle_install_snapshot_response(resp),
        });

        outputs
    }

    /// Steps down to follower and updates term.
    ///
    /// Returns a `VoteStateChanged` output that must be included in the response.
    fn step_down(&mut self, new_term: TermId) -> Vec<RaftOutput> {
        self.current_term = new_term;
        self.state = RaftState::Follower;
        self.voted_for = None;
        self.pre_votes_received.clear();
        self.votes_received.clear();
        self.transfer_target = None;
        self.wal_read_pending.clear();

        // Reset election timeout when stepping down.
        self.reset_election_elapsed();
        self.reset_randomized_election_timeout();

        // Emit vote state change (term updated, vote cleared).
        vec![RaftOutput::VoteStateChanged {
            term: self.current_term,
            voted_for: self.voted_for,
        }]
    }

    /// Handles a pre-vote request.
    ///
    /// Pre-vote doesn't update term or `voted_for` state - it's speculative.
    fn handle_pre_vote(&self, req: PreVoteRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Reject all pre-votes during observation mode.
        // In observation mode, we're recovering from S3 and don't know the
        // current cluster state yet.
        if self.observation_mode {
            let response =
                PreVoteResponse::new(req.term, self.config.node_id, req.candidate_id, false);
            outputs.push(RaftOutput::SendMessage(Message::PreVoteResponse(response)));
            return outputs;
        }

        // Grant pre-vote if:
        // 1. The candidate's proposed term is >= our term
        // 2. We don't have a current leader (or the proposed term is higher)
        // 3. The candidate's log is at least as up-to-date as ours
        let vote_granted = req.term >= self.current_term
            && (self.leader_id.is_none() || req.term > self.current_term)
            && self
                .log
                .is_up_to_date(req.last_log_term, req.last_log_index);

        let response = PreVoteResponse::new(
            req.term, // Echo back the proposed term.
            self.config.node_id,
            req.candidate_id,
            vote_granted,
        );
        outputs.push(RaftOutput::SendMessage(Message::PreVoteResponse(response)));

        outputs
    }

    /// Handles a pre-vote response.
    fn handle_pre_vote_response(&mut self, resp: PreVoteResponse) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Ignore if not a pre-candidate.
        if self.state != RaftState::PreCandidate {
            return outputs;
        }

        // Ignore if the response is for a different term than we're proposing.
        let proposed_term = self.current_term.get() + 1;
        if resp.term.get() != proposed_term {
            return outputs;
        }

        if resp.vote_granted {
            self.pre_votes_received.insert(resp.from);

            // Check if we have quorum - if so, start real election.
            if self.pre_votes_received.len() >= self.config.quorum_size() {
                outputs.extend(self.start_election());
            }
        }

        outputs
    }

    /// Starts a real election after pre-vote succeeds.
    ///
    /// This increments term and sends actual `RequestVote` messages.
    fn start_election(&mut self) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Precondition: we have quorum of pre-votes.
        debug_assert!(self.pre_votes_received.len() >= self.config.quorum_size());

        // Now increment term and become candidate.
        self.current_term = TermId::new(self.current_term.get() + 1);
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.config.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.config.node_id);

        // Emit vote state change (term incremented, voted for self).
        outputs.push(RaftOutput::VoteStateChanged {
            term: self.current_term,
            voted_for: self.voted_for,
        });

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

        outputs
    }

    /// Handles a `RequestVote` request.
    fn handle_request_vote(&mut self, req: RequestVoteRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        let vote_granted = self.should_grant_vote(&req);

        if vote_granted {
            self.voted_for = Some(req.candidate_id);
            // Reset election timeout when granting vote.
            self.reset_election_elapsed();

            // Emit vote state change (voted for a candidate).
            outputs.push(RaftOutput::VoteStateChanged {
                term: self.current_term,
                voted_for: self.voted_for,
            });
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
        // Reject all votes during observation mode.
        // In observation mode, we're recovering from S3 and don't know the
        // current cluster state yet. We need to wait and learn from heartbeats.
        if self.observation_mode {
            return false;
        }

        // Reject if term is old.
        if req.term < self.current_term {
            return false;
        }

        // Check if we can vote for this candidate.
        let can_vote = self.voted_for.is_none_or(|id| id == req.candidate_id);

        if !can_vote {
            return false;
        }

        // Check if candidate's log is at least as up-to-date as ours.
        self.log
            .is_up_to_date(req.last_log_term, req.last_log_index)
    }

    /// Handles a `RequestVote` response.
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
        self.heartbeat_elapsed = 0;
        self.wal_read_pending.clear();

        // Initialize leader state.
        // next_index starts at last_index + 1 (optimistic).
        // match_index starts at 0 (unknown).
        // inflight_count starts at 0 (no in-flight requests).
        let next_idx = LogIndex::new(self.log.last_index().get() + 1);
        for peer in self.config.peers() {
            self.replication_state
                .insert(peer, ReplicationState::new(next_idx));
        }

        // Postcondition: leader state initialized for all peers.
        debug_assert!(self.replication_state.len() == self.config.peers().len());

        outputs.push(RaftOutput::BecameLeader);

        // Append a no-op entry to commit entries from previous terms.
        //
        // Raft leaders cannot directly commit entries from previous terms (safety).
        // By appending an entry in the current term and committing it, all prior
        // entries are also committed. Without this, uncommitted entries from a
        // crashed leader could remain uncommitted indefinitely if no new client
        // requests arrive.
        //
        // This is a standard Raft optimization implemented by Kafka KRaft
        // (LeaderChangeMessage) and Redpanda (replicate_config_as_new_leader).
        let noop_index = LogIndex::new(self.log.last_index().get() + 1);
        let noop_entry = LogEntry::new(self.current_term, noop_index, Bytes::new(), Bytes::new());
        self.log.append(noop_entry);

        // Send AppendEntries (with no-op) to all peers.
        for peer in self.config.peers() {
            outputs.extend(self.send_append_entries(peer));
        }

        // Try to advance commit index (may succeed in single-node cluster).
        outputs.extend(self.try_advance_commit_index());

        // Postcondition: we are now the leader.
        debug_assert!(self.state == RaftState::Leader);
        debug_assert!(self.leader_id == Some(self.config.node_id));

        outputs
    }

    /// Handles an `AppendEntries` request.
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
                self.persisted_commit_index,
            );
            outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
                response,
            )));
            return outputs;
        }

        // Valid AppendEntries from current or newer term.
        // Step down if we're a candidate or pre-candidate.
        // A PreCandidate receiving AppendEntries from a valid leader should
        // also step down (the leader exists, no need to start an election).
        if self.state == RaftState::Candidate || self.state == RaftState::PreCandidate {
            self.state = RaftState::Follower;
            self.votes_received.clear();
            self.pre_votes_received.clear();
        }

        // Remember the leader.
        // Only update leader_id if we're not already the leader.
        // A Leader receiving AppendEntries from another leader in the same term
        // is a Raft violation (two leaders in same term). In this case, we keep
        // our own leader_id to avoid corrupting our state.
        if self.state != RaftState::Leader {
            self.leader_id = Some(req.leader_id);
        }

        // Reset election timeout - we heard from a valid leader.
        self.reset_election_elapsed();

        // Check if we have the prev_log entry.
        // Three cases:
        // 1. prev_log_index == 0: beginning of log, always ok.
        // 2. prev_log_index <= persisted_commit_index: the entry was committed
        //    and is in our data WAL. Committed entries are identical on all
        //    nodes, so accept without checking the (empty) in-memory log.
        // 3. Otherwise: check the in-memory log normally.
        let log_ok = if req.prev_log_index.get() == 0 {
            true
        } else if req.prev_log_index <= self.persisted_commit_index {
            // Committed entries are identical on all nodes.
            // Accept without checking the (empty) in-memory log.
            true
        } else {
            req.prev_log_index <= self.log.last_index()
                && self.log.term_at(req.prev_log_index) == req.prev_log_term
        };

        if !log_ok {
            // Log doesn't contain an entry at prev_log_index with matching term.
            let response = AppendEntriesResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                false,
                self.log.last_index(),
                self.persisted_commit_index,
            );
            outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
                response,
            )));
            return outputs;
        }

        // Append entries (handling conflicts).
        if !req.entries.is_empty() && !self.log.append_entries(req.entries) {
            // Entries contain a gap relative to our log tail. This can
            // happen when the leader has a stale next_index from pipelining
            // and sends a batch that skips indices we don't have yet.
            // Reject so the leader recalculates from our match_index.
            let response = AppendEntriesResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                false,
                self.log.last_index(),
                self.persisted_commit_index,
            );
            outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
                response,
            )));
            return outputs;
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
            self.persisted_commit_index,
        );
        outputs.push(RaftOutput::SendMessage(Message::AppendEntriesResponse(
            response,
        )));

        outputs
    }

    /// Handles an `AppendEntries` response (leader only).
    ///
    /// With pipelining, we may have multiple in-flight requests. On success,
    /// we update `match_index` and try to send more. On failure, we reset the
    /// pipeline and backtrack using the follower's `match_index` for fast backup.
    fn handle_append_entries_response(&mut self, resp: AppendEntriesResponse) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Ignore if not leader or term doesn't match.
        if self.state != RaftState::Leader || resp.term != self.current_term {
            return outputs;
        }

        let Some(state) = self.replication_state.get_mut(&resp.from) else {
            return outputs;
        };

        // Reset stale tick counter - we got a response from this follower.
        state.inflight_stale_ticks = 0;

        if resp.success {
            // Decrement inflight count - this response completes one request.
            state.inflight_count = state.inflight_count.saturating_sub(1);

            // Update match_index to the confirmed value from the follower.
            // Only advance if the new match is higher (responses may arrive
            // out of order with pipelining).
            let new_match = resp.match_index;
            if new_match > state.match_index {
                state.match_index = new_match;
            }

            // Copy next_index before dropping the borrow.
            let next_index = state.next_index;

            // Try to advance commit index with the new match_index.
            outputs.extend(self.try_advance_commit_index());

            // Only send more entries if there are actually entries to replicate.
            // Don't send empty heartbeats here - that happens on tick().
            // This prevents exponential message growth under message duplication.
            if next_index <= self.log.last_index() {
                outputs.extend(self.send_append_entries(resp.from));
            }

            // Check if leadership transfer can complete.
            let state = self.replication_state.get(&resp.from);
            if self.transfer_target == Some(resp.from) {
                if let Some(s) = state {
                    if s.match_index >= self.log.last_index() {
                        let timeout_now = TimeoutNowRequest::new(
                            self.current_term,
                            self.config.node_id,
                            resp.from,
                        );
                        outputs.push(RaftOutput::SendMessage(Message::TimeoutNow(timeout_now)));
                    }
                }
            }
        } else {
            // Rejection - need to backtrack. Clear all in-flight requests
            // since we don't know which ones will fail.
            state.inflight_count = 0;

            // Fast-forward using the follower's persisted_commit_index when
            // available. The follower has committed entries in its WAL that
            // may not be in its in-memory log after restart. Skip directly
            // to persisted_commit_index + 1 instead of backing up entry-by-entry.
            let fast_forward = resp.persisted_commit_index.get() + 1;
            let backup = resp.match_index.get() + 1;
            state.next_index = LogIndex::new(fast_forward.max(backup).max(1));

            // Immediately retry with corrected next_index.
            outputs.extend(self.send_append_entries(resp.from));
        }

        outputs
    }

    /// Handles a `TimeoutNow` request (leadership transfer target).
    ///
    /// When receiving this message, the node immediately starts an election
    /// without waiting for the election timeout. This bypasses pre-vote
    /// since the leader explicitly requested the transfer.
    fn handle_timeout_now(&mut self, req: TimeoutNowRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Only process if the term matches and we're a follower.
        if req.term != self.current_term || self.state != RaftState::Follower {
            return outputs;
        }

        // Start election immediately (bypass pre-vote).
        // The leader has explicitly requested this node become leader.
        self.current_term = TermId::new(self.current_term.get() + 1);
        self.state = RaftState::Candidate;
        self.voted_for = Some(self.config.node_id);
        self.votes_received.clear();
        self.votes_received.insert(self.config.node_id); // Vote for self.
        self.leader_id = None;

        // Emit vote state change (term incremented, voted for self).
        outputs.push(RaftOutput::VoteStateChanged {
            term: self.current_term,
            voted_for: self.voted_for,
        });

        // Reset election timing.
        self.reset_election_elapsed();
        self.reset_randomized_election_timeout();

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

        outputs
    }

    /// Handles an `InstallSnapshot` RPC from the leader.
    ///
    /// This is called when a follower is too far behind to catch up via
    /// `AppendEntries` and needs a snapshot to advance.
    ///
    /// Note: Full implementation requires snapshot storage integration.
    /// This is a placeholder that acknowledges the snapshot.
    fn handle_install_snapshot(&mut self, req: &InstallSnapshotRequest) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Check term.
        if req.term < self.current_term {
            // Stale term, reject.
            let response = InstallSnapshotResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                false,
                0,
            );
            outputs.push(RaftOutput::SendMessage(Message::InstallSnapshotResponse(
                response,
            )));
            return outputs;
        }

        // Valid leader, update state.
        // Only update leader_id if we're not already the leader.
        // A Leader receiving InstallSnapshot from another leader in the same term
        // is a Raft violation (two leaders in same term).
        if self.state != RaftState::Leader {
            self.leader_id = Some(req.leader_id);
        }
        self.reset_election_elapsed();

        let response = if req.done {
            // Final chunk: install the snapshot. Advance commit and log floor so that
            // subsequent AppendEntries consistency checks (prev_log_index <= commit_index)
            // and term-at-index lookups both return correct results.
            let snap_index = req.last_included_index;
            let snap_term = req.last_included_term;
            self.persisted_commit_index =
                LogIndex::new(self.persisted_commit_index.get().max(snap_index.get()));
            self.commit_index = LogIndex::new(self.commit_index.get().max(snap_index.get()));
            self.log.set_compacted_state(snap_index.get(), snap_term.get());
            InstallSnapshotResponse::complete(self.current_term, self.config.node_id, req.leader_id)
        } else {
            // Request next chunk.
            InstallSnapshotResponse::new(
                self.current_term,
                self.config.node_id,
                req.leader_id,
                true,
                req.offset + req.data.len() as u64,
            )
        };
        outputs.push(RaftOutput::SendMessage(Message::InstallSnapshotResponse(
            response,
        )));

        outputs
    }

    /// Handles an `InstallSnapshotResponse` from a follower.
    ///
    /// Decrements the inflight count on success so that the leader can
    /// pipeline the next `AppendEntries` request to catch the follower up.
    fn handle_install_snapshot_response(&mut self, resp: InstallSnapshotResponse) -> Vec<RaftOutput> {
        // Term check is done by handle_message before dispatching here.
        if self.state != RaftState::Leader || resp.term != self.current_term {
            return Vec::new();
        }

        if let Some(state) = self.replication_state.get_mut(&resp.from) {
            if resp.success {
                state.inflight_count = state.inflight_count.saturating_sub(1);
            }
        }

        Vec::new()
    }

    /// Initiates leadership transfer to the specified target node.
    ///
    /// Returns outputs to send, or None if transfer cannot be started.
    /// The leader will stop accepting new client requests and send
    /// `AppendEntries` to catch up the target, then send `TimeoutNow`.
    pub fn transfer_leadership(&mut self, target: NodeId) -> Option<Vec<RaftOutput>> {
        // Must be leader to transfer.
        if self.state != RaftState::Leader {
            return None;
        }

        // Cannot transfer to self.
        if target == self.config.node_id {
            return None;
        }

        // Target must be in the cluster.
        if !self.config.cluster.contains(&target) {
            return None;
        }

        // Cannot start a new transfer if one is in progress.
        if self.transfer_target.is_some() {
            return None;
        }

        let mut outputs = Vec::new();

        // Set the transfer target and reset the elapsed counter.
        self.transfer_target = Some(target);
        self.transfer_elapsed = 0;

        // Check if target is already caught up.
        let target_match = self
            .replication_state
            .get(&target)
            .map_or(LogIndex::new(0), |s| s.match_index);
        if target_match >= self.log.last_index() {
            // Target is already caught up, send TimeoutNow immediately.
            let timeout_now =
                TimeoutNowRequest::new(self.current_term, self.config.node_id, target);
            outputs.push(RaftOutput::SendMessage(Message::TimeoutNow(timeout_now)));
        } else {
            // Send AppendEntries to catch up the target.
            outputs.extend(self.send_append_entries(target));
        }

        Some(outputs)
    }

    /// Aborts an in-progress leadership transfer.
    pub const fn abort_transfer(&mut self) {
        self.transfer_target = None;
    }

    /// Returns the current transfer target, if any.
    #[must_use]
    pub const fn transfer_target(&self) -> Option<NodeId> {
        self.transfer_target
    }

    // ========================================================================
    // Snapshot Operations
    // ========================================================================

    /// Creates a snapshot of the current state up to the commit index.
    ///
    /// The `data` parameter contains the serialized state machine state that
    /// the application wants to include in the snapshot.
    ///
    /// # Returns
    ///
    /// Returns `None` if there's nothing to snapshot (`commit_index` is 0).
    #[must_use]
    pub fn create_snapshot(&self, data: Bytes) -> Option<Snapshot> {
        // Nothing to snapshot if nothing committed.
        if self.commit_index.get() == 0 {
            return None;
        }

        // Get the term at commit_index.
        let term = self.log.term_at(self.commit_index);

        let snapshot = Snapshot::new(self.commit_index, term, data);

        // Postcondition: snapshot covers committed entries.
        debug_assert!(snapshot.last_included_index == self.commit_index);

        Some(snapshot)
    }

    /// Installs a snapshot received from another node.
    ///
    /// This is called when a follower receives a complete snapshot from the
    /// leader (or during shard transfer). It:
    /// 1. Updates the snapshot metadata
    /// 2. Truncates the log to remove entries covered by the snapshot
    /// 3. Updates `commit_index` and `last_applied`
    ///
    /// The caller is responsible for updating the state machine with the
    /// snapshot data.
    ///
    /// # Returns
    ///
    /// Returns `true` if the snapshot was installed, `false` if it was stale.
    pub fn install_snapshot(&mut self, snapshot: &Snapshot) -> bool {
        // Reject stale snapshots.
        if let Some(ref meta) = self.snapshot_meta {
            if snapshot.last_included_index <= meta.last_included_index {
                return false;
            }
        }

        // Update snapshot metadata.
        self.snapshot_meta = Some(SnapshotMeta::from_snapshot(snapshot));

        // Truncate log - remove entries covered by snapshot.
        // Keep entries after the snapshot's last_included_index.
        self.log.truncate_prefix(snapshot.last_included_index);

        // Update indices.
        if snapshot.last_included_index > self.commit_index {
            self.commit_index = snapshot.last_included_index;
        }
        if snapshot.last_included_index > self.last_applied {
            self.last_applied = snapshot.last_included_index;
        }

        // Postcondition: indices are consistent with snapshot.
        debug_assert!(self.commit_index >= snapshot.last_included_index);
        debug_assert!(self.last_applied >= snapshot.last_included_index);

        true
    }

    /// Returns the current snapshot metadata, if any.
    #[must_use]
    pub const fn snapshot_meta(&self) -> Option<&SnapshotMeta> {
        self.snapshot_meta.as_ref()
    }

    /// Returns true if the given index is covered by a snapshot.
    #[must_use]
    pub fn is_index_compacted(&self, index: LogIndex) -> bool {
        self.snapshot_meta
            .as_ref()
            .is_some_and(|meta| index <= meta.last_included_index)
    }

    /// Sends `AppendEntries` to a specific peer.
    ///
    /// With pipelining, this function may send multiple requests without
    /// waiting for responses, up to `MAX_INFLIGHT_APPEND_ENTRIES`. The
    /// `next_index` is speculatively advanced after each send.
    ///
    /// If entries exist in the data WAL but not in the in-memory log
    /// (after restart), emits `NeedEntries` instead. The caller reads
    /// the WAL and calls `provide_entries()` to resume replication.
    fn send_append_entries(&mut self, peer: NodeId) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Suppress sends while a WAL read is pending for this follower.
        if self.wal_read_pending.contains(&peer) {
            return outputs;
        }

        let Some(state) = self.replication_state.get_mut(&peer) else {
            debug!(
                peer = peer.get(),
                "send_append_entries: no replication state for peer"
            );
            return outputs;
        };

        // Don't send if at inflight limit (pipelining bound).
        if state.inflight_count >= limits::MAX_INFLIGHT_APPEND_ENTRIES {
            debug!(
                peer = peer.get(),
                inflight = state.inflight_count,
                limit = limits::MAX_INFLIGHT_APPEND_ENTRIES,
                "send_append_entries: at inflight limit"
            );
            return outputs;
        }

        let next_idx = state.next_index;
        let prev_idx = LogIndex::new(next_idx.get().saturating_sub(1));
        let prev_term = self.log.term_at(prev_idx);

        // Limit entries to fit within transport message size.
        // Uses half of MAX_RAFT_MESSAGE_BYTES to leave room for AppendEntries headers.
        let max_entries_bytes = MAX_RAFT_MESSAGE_BYTES / 2;
        let entries = self.log.entries_from_limited(next_idx, max_entries_bytes);
        let entries_count = entries.len();

        // If the requested entries are below the in-memory log's range,
        // request a WAL read. This happens after restart (entries below
        // persisted_commit_index) or after runtime log compaction
        // (entries truncated from memory but present in WAL).
        let log_first = self.log.first_index();
        let need_wal = log_first.get() > 0 && next_idx < log_first;
        if need_wal {
            outputs.push(RaftOutput::NeedEntries {
                follower_id: peer,
                start_index: next_idx,
                prev_log_index: prev_idx,
                max_bytes: max_entries_bytes,
            });
            self.wal_read_pending.insert(peer);
            return outputs;
        }

        // Speculatively advance next_index to the end of entries being sent.
        // This allows subsequent sends to pipeline more entries.
        if entries_count > 0 {
            // Safe: entries_count is bounded by log size.
            #[allow(clippy::cast_possible_truncation)]
            let advance = entries_count as u64;
            state.next_index = LogIndex::new(next_idx.get() + advance);
        }

        // Increment inflight count - will be decremented on response.
        state.inflight_count += 1;

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

    /// Provides entries read from the data WAL for a follower.
    ///
    /// Called by the partition actor after reading entries from the WAL in
    /// response to a `NeedEntries` output. Constructs an `AppendEntries`
    /// request with the provided entries and sends it to the follower.
    ///
    /// # Arguments
    ///
    /// * `follower_id` - The follower that needs entries
    /// * `prev_log_index` - Index before the first provided entry
    /// * `prev_log_term` - Term at `prev_log_index` (from WAL)
    /// * `entries` - Entries read from the WAL
    pub fn provide_entries(
        &mut self,
        follower_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: TermId,
        entries: Vec<LogEntry>,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Clear the pending flag regardless of outcome.
        self.wal_read_pending.remove(&follower_id);

        // Only send if we're still leader.
        if self.state != RaftState::Leader {
            return outputs;
        }

        let Some(state) = self.replication_state.get_mut(&follower_id) else {
            return outputs;
        };

        // Advance next_index past the entries being sent.
        if !entries.is_empty() {
            let last_entry_index = entries[entries.len() - 1].index;
            state.next_index = LogIndex::new(last_entry_index.get() + 1);
        }

        state.inflight_count += 1;

        let request = AppendEntriesRequest::new(
            self.current_term,
            self.config.node_id,
            follower_id,
            prev_log_index,
            prev_log_term,
            entries,
            self.commit_index,
        );
        outputs.push(RaftOutput::SendMessage(Message::AppendEntries(request)));

        outputs
    }

    /// Called when the WAL floor has advanced past a follower's `next_index`.
    ///
    /// Sends `InstallSnapshot` to fast-forward the follower to the WAL floor.
    /// The snapshot carries no state data for helix WAL partitions — it only
    /// establishes the `(last_included_index, last_included_term)` boundary so
    /// that the subsequent `AppendEntries` consistency check can pass.
    ///
    /// # Why this is correct
    ///
    /// The follower's log is either empty (fresh node) or has entries that are
    /// all below the WAL floor (stale node). In both cases, those entries are
    /// outside the leader's retention window and can be discarded. After the
    /// snapshot installs the floor boundary, normal Raft replication from the
    /// WAL floor onward resumes.
    pub fn provide_snapshot(
        &mut self,
        follower_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: TermId,
    ) -> Vec<RaftOutput> {
        let mut outputs = Vec::new();

        // Clear pending flag regardless of outcome.
        self.wal_read_pending.remove(&follower_id);

        if self.state != RaftState::Leader {
            return outputs;
        }

        let Some(state) = self.replication_state.get_mut(&follower_id) else {
            return outputs;
        };

        // Advance next_index past the snapshot boundary.
        state.next_index = LogIndex::new(last_included_index.get() + 1);
        state.inflight_count += 1;

        outputs.push(RaftOutput::SendMessage(Message::InstallSnapshot(
            InstallSnapshotRequest::new(
                self.current_term,
                self.config.node_id,
                follower_id,
                last_included_index,
                last_included_term,
                0,
                bytes::Bytes::new(), // No state to install for WAL snapshots.
                true,                // Single chunk, done immediately.
            ),
        )));

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
        let _loop_bound = self
            .log
            .last_index()
            .get()
            .saturating_sub(self.commit_index.get());
        // Loop is bounded by the log length which is finite.

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
                if let Some(state) = self.replication_state.get(&peer) {
                    if state.match_index >= idx {
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

    /// Compacts the Raft log by truncating committed entries (leader).
    ///
    /// Keeps a trailing window of entries for fast in-memory follower
    /// catch-up. The window size is `config.log_trailing_entries`. A
    /// hard cap based on `commit_index` prevents a lagging follower from
    /// blocking compaction and causing OOM.
    ///
    /// Returns true if any entries were removed.
    pub fn compact_log(&mut self) -> bool {
        if self.state != RaftState::Leader {
            return false;
        }

        let trailing = self.config.log_trailing_entries;
        let log_len = self.log.len();

        // Nothing to compact if the log is small enough.
        if log_len <= trailing {
            return false;
        }

        // Find the minimum match_index across all followers so we don't
        // compact entries they still need from memory.
        let min_match = self
            .replication_state
            .values()
            .map(|s| s.match_index)
            .min()
            .unwrap_or(LogIndex::new(0));

        // Hard cap: never keep more than trailing_window entries behind
        // commit_index, even if a follower is lagging (WAL fallback).
        let hard_cap = self
            .commit_index
            .get()
            .saturating_sub(trailing);

        // Compact to the max of (min_match, hard_cap) — keep entries
        // that followers need, but enforce the hard cap.
        let compact_to = hard_cap.max(min_match.get());

        // Never compact past last_applied (entries not yet emitted as
        // CommitEntry must stay in the log).
        let compact_to = compact_to.min(self.last_applied.get());

        // Nothing to compact if compact_to is below the log's start.
        let log_first = self.log.first_index().get();
        if log_first == 0 || compact_to < log_first {
            return false;
        }

        self.log.truncate_prefix(LogIndex::new(compact_to));
        true
    }

    /// Compacts the Raft log by truncating committed entries (follower).
    ///
    /// Followers don't have `replication_state` so they compact based on
    /// `last_applied` minus the trailing window.
    ///
    /// Returns true if any entries were removed.
    pub fn compact_log_follower(&mut self) -> bool {
        let trailing = self.config.log_trailing_entries;
        let log_len = self.log.len();

        if log_len <= trailing {
            return false;
        }

        let compact_to = self
            .last_applied
            .get()
            .saturating_sub(trailing);

        let log_first = self.log.first_index().get();
        if log_first == 0 || compact_to < log_first {
            return false;
        }

        self.log.truncate_prefix(LogIndex::new(compact_to));
        true
    }

    /// Returns the minimum `match_index` across all followers.
    ///
    /// Only meaningful when this node is the leader. Returns `None` if
    /// the node is not a leader or has no followers (single-node cluster).
    ///
    /// Used by the retention system to determine the oldest WAL entry
    /// that may still be needed for Raft replication.
    #[must_use]
    pub fn min_match_index(&self) -> Option<LogIndex> {
        if self.state != RaftState::Leader {
            return None;
        }
        self.replication_state
            .values()
            .map(|s| s.match_index)
            .min()
    }

    /// Returns the `match_index` for a specific follower.
    ///
    /// Returns `None` if not leader or the follower is not in the
    /// replication state. Returns `Some(0)` if the follower exists but
    /// has never successfully replicated.
    ///
    /// Used by the application layer to check whether a follower is
    /// responsive before initiating leadership transfer.
    #[must_use]
    pub fn follower_match_index(&self, follower: NodeId) -> Option<LogIndex> {
        if self.state != RaftState::Leader {
            return None;
        }
        self.replication_state
            .get(&follower)
            .map(|s| s.match_index)
    }

    /// Applies committed entries up to the given index.
    fn apply_committed_entries(&mut self, new_commit: LogIndex) -> Vec<RaftOutput> {
        // Precondition: new_commit must be within the valid log range.
        // After recovery, entries may exist in the log since they were appended
        // before commit advanced, so we check against last_index which includes
        // both entries and compacted state.
        debug_assert!(
            new_commit <= self.log.last_index(),
            "new_commit {} > log.last_index() {}",
            new_commit.get(),
            self.log.last_index().get(),
        );

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
                info!(
                    node_id = self.config.node_id.get(),
                    index = idx.get(),
                    entry_term = entry.term.get(),
                    current_term = self.current_term.get(),
                    metadata_len = entry.metadata.len(),
                    payload_len = entry.payload.len(),
                    "RAFT_COMMIT_EMIT: emitting CommitEntry"
                );
                outputs.push(RaftOutput::CommitEntry {
                    index: idx,
                    term: entry.term,
                    metadata: entry.metadata.clone(),
                    payload: entry.payload.clone(),
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
        // Use faster tick config for tests.
        .with_tick_config(5, 1)
    }

    /// Helper to tick a node until election timeout fires.
    fn tick_until_election(node: &mut RaftNode) -> Vec<RaftOutput> {
        let mut all_outputs = Vec::new();
        // Tick enough times to trigger election (max 2x election_tick).
        for _ in 0..(node.config.election_tick * 2) {
            let outputs = node.tick();
            all_outputs.extend(outputs);
            if node.state() != RaftState::Follower {
                break;
            }
        }
        all_outputs
    }

    /// Helper to make a node become leader through the full pre-vote + vote phases.
    fn make_leader(node: &mut RaftNode) -> Vec<RaftOutput> {
        let mut all_outputs = Vec::new();

        // Phase 1: Tick until pre-vote starts.
        all_outputs.extend(tick_until_election(node));

        // Phase 2: Receive pre-vote response to reach quorum.
        let prevote = PreVoteResponse::new(
            TermId::new(node.current_term().get() + 1),
            NodeId::new(2),
            node.config.node_id,
            true,
        );
        all_outputs.extend(node.handle_message(Message::PreVoteResponse(prevote)));

        // Phase 3: Receive vote response to become leader.
        let vote = RequestVoteResponse::new(
            node.current_term(),
            NodeId::new(2),
            node.config.node_id,
            true,
        );
        all_outputs.extend(node.handle_message(Message::RequestVoteResponse(vote)));

        all_outputs
    }

    #[test]
    fn test_new_node_is_follower() {
        let node = RaftNode::new(make_config(1));

        assert_eq!(node.state(), RaftState::Follower);
        assert_eq!(node.current_term(), TermId::new(0));
        assert!(node.leader_id().is_none());
    }

    #[test]
    fn test_tick_increments_election_elapsed() {
        let mut node = RaftNode::new(make_config(1));

        assert_eq!(node.election_elapsed(), 0);

        node.tick();
        assert_eq!(node.election_elapsed(), 1);

        node.tick();
        assert_eq!(node.election_elapsed(), 2);
    }

    #[test]
    fn test_tick_triggers_election_after_timeout() {
        let mut node = RaftNode::new(make_config(1));

        // Tick until election fires.
        let outputs = tick_until_election(&mut node);

        // With pre-vote, node should become PreCandidate.
        assert_eq!(node.state(), RaftState::PreCandidate);
        // Term should NOT be incremented yet during pre-vote.
        assert_eq!(node.current_term(), TermId::new(0));

        // Should have sent PreVote messages.
        let prevote_requests: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, RaftOutput::SendMessage(Message::PreVote(_))))
            .collect();
        assert_eq!(prevote_requests.len(), 2);
    }

    #[test]
    fn test_leader_tick_sends_heartbeats() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);
        assert!(node.is_leader());

        // Reset heartbeat elapsed.
        node.heartbeat_elapsed = 0;

        // Tick until heartbeat fires.
        let mut heartbeat_sent = false;
        for _ in 0..5 {
            let outputs = node.tick();
            if outputs
                .iter()
                .any(|o| matches!(o, RaftOutput::SendMessage(Message::AppendEntries(_))))
            {
                heartbeat_sent = true;
                break;
            }
        }

        assert!(heartbeat_sent, "Leader should send heartbeats on tick");
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

        // Phase 1: Tick until pre-vote starts.
        tick_until_election(&mut node);
        assert_eq!(node.state(), RaftState::PreCandidate);
        assert_eq!(node.current_term(), TermId::new(0)); // No term increment yet.

        // Phase 2: Receive pre-vote from node 2 (quorum reached).
        let prevote = PreVoteResponse::new(
            TermId::new(1), // The term that would be used.
            NodeId::new(2),
            NodeId::new(1),
            true,
        );
        let outputs = node.handle_message(Message::PreVoteResponse(prevote));

        // Should transition to Candidate and send actual RequestVote messages.
        assert_eq!(node.state(), RaftState::Candidate);
        assert_eq!(node.current_term(), TermId::new(1)); // Term now incremented.
        let vote_requests: Vec<_> = outputs
            .iter()
            .filter(|o| matches!(o, RaftOutput::SendMessage(Message::RequestVote(_))))
            .collect();
        assert_eq!(vote_requests.len(), 2);

        // Phase 3: Receive vote from node 2.
        let vote = RequestVoteResponse::new(TermId::new(1), NodeId::new(2), NodeId::new(1), true);
        let outputs = node.handle_message(Message::RequestVoteResponse(vote));

        // Should be leader now (have 2 votes: self + node 2, quorum is 2).
        assert_eq!(node.state(), RaftState::Leader);
        assert!(outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::BecameLeader)));
    }

    #[test]
    fn test_append_entries_resets_election_elapsed() {
        let mut node = RaftNode::new(make_config(1));

        // Tick a few times.
        node.tick();
        node.tick();
        assert!(node.election_elapsed() > 0);

        let request = AppendEntriesRequest::heartbeat(
            TermId::new(1),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(0),
            TermId::new(0),
            LogIndex::new(0),
        );

        node.handle_message(Message::AppendEntries(request));

        // Election elapsed should be reset.
        assert_eq!(node.election_elapsed(), 0);
        assert_eq!(node.leader_id(), Some(NodeId::new(2)));
    }

    #[test]
    fn test_client_request_appends_to_log() {
        let mut node = RaftNode::new(make_config(1));

        // Become leader first (through pre-vote + vote phases).
        make_leader(&mut node);
        assert!(node.is_leader());

        // Send client request.
        // Note: log already has 1 entry (no-op from leader election).
        let request = ClientRequest::new(Bytes::from("test command"), Bytes::new());
        let outputs = node.handle_client_request(request);

        assert!(outputs.is_some());
        assert_eq!(node.log().len(), 2); // no-op + client request
        assert_eq!(node.log().last_index().get(), 2);
    }

    #[test]
    fn test_step_down_on_higher_term() {
        let mut node = RaftNode::new(make_config(1));

        // Become leader (through pre-vote + vote phases).
        make_leader(&mut node);
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
        let config = RaftConfig::new(NodeId::new(1), vec![NodeId::new(1)]).with_tick_config(5, 1);
        let mut node = RaftNode::new(config);

        // Tick until election triggers - should become leader immediately.
        let outputs = tick_until_election(&mut node);
        assert!(node.is_leader());
        assert!(outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::BecameLeader)));

        // Client request should commit immediately.
        // Note: no-op at index 1 is already committed, client request at index 2.
        let request = ClientRequest::new(Bytes::from("test"), Bytes::new());
        let outputs = node.handle_client_request(request).unwrap();

        assert!(outputs
            .iter()
            .any(|o| matches!(o, RaftOutput::CommitEntry { .. })));
        assert_eq!(node.commit_index().get(), 2); // no-op + client request both committed
    }

    #[test]
    fn test_randomized_election_timeout_varies() {
        // Create nodes with different seeds.
        let config1 = make_config(1).with_random_seed(1);
        let config2 = make_config(1).with_random_seed(2);
        let config3 = make_config(1).with_random_seed(3);

        let node1 = RaftNode::new(config1);
        let node2 = RaftNode::new(config2);
        let node3 = RaftNode::new(config3);

        // Randomized timeouts should differ.
        let timeouts = [
            node1.randomized_election_timeout(),
            node2.randomized_election_timeout(),
            node3.randomized_election_timeout(),
        ];

        // At least two should be different (with high probability).
        let unique: std::collections::HashSet<_> = timeouts.iter().collect();
        assert!(
            unique.len() >= 2,
            "Expected different timeouts, got {timeouts:?}"
        );
    }

    // ========================================================================
    // Pipelining Tests
    // ========================================================================

    #[test]
    fn test_pipelining_inflight_limit_respected() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);
        assert!(node.is_leader());

        // Add many entries to the log.
        for i in 0..10 {
            let request = ClientRequest::new(Bytes::from(format!("entry {i}")), Bytes::new());
            node.handle_client_request(request);
        }

        // Reset a peer's replication state to simulate catching up.
        let peer = NodeId::new(2);
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.next_index = LogIndex::new(1);
            state.match_index = LogIndex::new(0);
            state.inflight_count = 0;
        }

        // Send AppendEntries repeatedly - should be limited by MAX_INFLIGHT.
        let mut total_sends = 0_u32;
        for _ in 0..10 {
            let outputs = node.send_append_entries(peer);
            if outputs.is_empty() {
                break;
            }
            total_sends += 1;
        }

        // Should have sent exactly MAX_INFLIGHT_APPEND_ENTRIES requests.
        assert_eq!(total_sends, limits::MAX_INFLIGHT_APPEND_ENTRIES);

        // Verify inflight count is at limit.
        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(state.inflight_count, limits::MAX_INFLIGHT_APPEND_ENTRIES);
    }

    #[test]
    fn test_pipelining_success_frees_slot_for_retry() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);

        let peer = NodeId::new(2);

        // Set inflight to max so no new sends can happen.
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.inflight_count = limits::MAX_INFLIGHT_APPEND_ENTRIES;
            state.next_index = LogIndex::new(1);
        }

        // Try to send - should fail because at limit.
        let outputs = node.send_append_entries(peer);
        assert!(outputs.is_empty(), "Should not send when at inflight limit");

        // Simulate successful response - this decrements inflight.
        let response = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            true,
            LogIndex::new(0),
            LogIndex::new(0),
        );
        let outputs = node.handle_message(Message::AppendEntriesResponse(response));

        // The response handler should have sent a new AppendEntries
        // since we freed up an inflight slot.
        let sent_count = outputs
            .iter()
            .filter(|o| matches!(o, RaftOutput::SendMessage(Message::AppendEntries(_))))
            .count();
        assert_eq!(
            sent_count, 1,
            "Should send new AppendEntries after freeing slot"
        );
    }

    #[test]
    fn test_pipelining_rejection_resets_inflight() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);

        // Add entries.
        for i in 0..5 {
            let request = ClientRequest::new(Bytes::from(format!("entry {i}")), Bytes::new());
            node.handle_client_request(request);
        }

        let peer = NodeId::new(2);

        // Artificially set high inflight count.
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.inflight_count = 3;
        }

        // Simulate rejection with match_index indicating follower has entry 2.
        let response = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            false,
            LogIndex::new(2),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(response));

        // After rejection:
        // 1. inflight_count is reset to 0
        // 2. next_index is set to match_index + 1 = 3
        // 3. send_append_entries is called which:
        //    - sends entries from index 3 to end (3 entries: 3, 4, 5)
        //    - speculatively advances next_index to 6
        //    - increments inflight_count to 1
        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(state.inflight_count, 1);

        // next_index should be advanced past the log after the retry.
        assert_eq!(state.next_index.get(), node.log().last_index().get() + 1);
    }

    #[test]
    fn test_pipelining_speculative_next_index_advance() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);

        // Add entries.
        for i in 0..3 {
            let request = ClientRequest::new(Bytes::from(format!("entry {i}")), Bytes::new());
            node.handle_client_request(request);
        }

        let peer = NodeId::new(2);

        // Reset the peer's state to simulate starting fresh.
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.next_index = LogIndex::new(1);
            state.match_index = LogIndex::new(0);
            state.inflight_count = 0;
        }

        // Send AppendEntries.
        node.send_append_entries(peer);

        // next_index should have advanced speculatively to end of log + 1.
        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(state.next_index.get(), node.log().last_index().get() + 1);
        assert_eq!(state.inflight_count, 1);
    }

    #[test]
    fn test_pipelining_out_of_order_responses() {
        let mut node = RaftNode::new(make_config(1));
        make_leader(&mut node);

        // Add entries.
        for i in 0..5 {
            let request = ClientRequest::new(Bytes::from(format!("entry {i}")), Bytes::new());
            node.handle_client_request(request);
        }

        let peer = NodeId::new(2);

        // Set up as if we've sent multiple pipelined requests.
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.inflight_count = 3;
            state.match_index = LogIndex::new(0);
        }

        // Receive response for match_index 3 first (later batch).
        let response = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            true,
            LogIndex::new(3),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(response));

        assert_eq!(
            node.replication_state.get(&peer).unwrap().match_index.get(),
            3
        );

        // Receive response for match_index 2 (earlier batch, out of order).
        // match_index should NOT go backwards.
        let response = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            true,
            LogIndex::new(2),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(response));

        // match_index should still be 3 (not 2).
        assert_eq!(
            node.replication_state.get(&peer).unwrap().match_index.get(),
            3
        );
    }

    // ========================================================================
    // Recovery State Tests
    // ========================================================================

    #[test]
    fn test_recovery_election_rejects_stale_candidate() {
        // Node 1 has recovery state (committed up to index 10, term 3).
        let config1 = make_config(1);
        let node = RaftNode::with_recovery_state(
            config1,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        // A stale candidate (term 2, index 5) should be rejected.
        let req = RequestVoteRequest::new(
            TermId::new(4),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(5),
            TermId::new(2),
        );
        assert!(
            !node.should_grant_vote(&req),
            "Should reject candidate with stale log"
        );

        // A candidate with matching state should be accepted.
        let req = RequestVoteRequest::new(
            TermId::new(4),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(10),
            TermId::new(3),
        );
        assert!(
            node.should_grant_vote(&req),
            "Should accept candidate with matching log"
        );

        // A candidate with higher term should be accepted.
        let req = RequestVoteRequest::new(
            TermId::new(4),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(1),
            TermId::new(4),
        );
        assert!(
            node.should_grant_vote(&req),
            "Should accept candidate with higher term"
        );
    }

    #[test]
    fn test_recovery_become_leader_noop_index() {
        // Single-node cluster with recovery state.
        let config = RaftConfig::new(NodeId::new(1), vec![NodeId::new(1)])
            .with_tick_config(5, 1);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        // Tick until election.
        let _outputs = tick_until_election(&mut node);
        assert!(node.is_leader());

        // No-op should be placed at compacted_index + 1.
        assert_eq!(node.log().last_index().get(), 11);
    }

    #[test]
    fn test_recovery_append_entries_at_commit_boundary() {
        // Follower with recovery state.
        let config = make_config(1);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        // Leader sends AppendEntries with prev_log_index = 10 (compacted).
        let entries = vec![LogEntry::new(
            TermId::new(4),
            LogIndex::new(11),
            Bytes::from("new-entry"),
            Bytes::new(),
        )];
        let req = AppendEntriesRequest::new(
            TermId::new(4),
            NodeId::new(2),
            NodeId::new(1),
            LogIndex::new(10), // prev_log_index = compacted_index
            TermId::new(3),    // prev_log_term = compacted_term
            entries,
            LogIndex::new(11),
        );
        let outputs = node.handle_message(Message::AppendEntries(req));

        // Should succeed (not reject).
        let response = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::AppendEntriesResponse(r)) => Some(r),
            _ => None,
        });
        assert!(response.is_some());
        assert!(
            response.unwrap().success,
            "Should accept AppendEntries at compacted boundary"
        );
        assert_eq!(node.log().last_index().get(), 11);
    }

    #[test]
    fn test_recovery_prevote_uses_compacted_state() {
        // Node with recovery state starts pre-election.
        let config = make_config(1);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        // Tick until pre-election.
        let outputs = tick_until_election(&mut node);
        assert_eq!(node.state(), RaftState::PreCandidate);

        // Verify PreVote messages include compacted state.
        let prevote = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::PreVote(r)) => Some(r),
            _ => None,
        });
        assert!(prevote.is_some());
        let pv = prevote.unwrap();
        assert_eq!(pv.last_log_index.get(), 10);
        assert_eq!(pv.last_log_term.get(), 3);
    }

    // =========================================================================
    // Part B: WAL-Backed AppendEntries Tests
    // =========================================================================

    /// Helper: create a recovered leader (recovery state + become leader).
    ///
    /// The leader has `persisted_commit_index = commit_index` and an empty
    /// in-memory log (except the no-op appended on `become_leader`).
    fn make_recovered_leader(commit_index: u64, commit_term: u64) -> RaftNode {
        let config = make_config(1);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(commit_term),
            None,
            false,
            LogIndex::new(commit_index),
            TermId::new(commit_term),
        );
        make_leader(&mut node);
        assert!(node.is_leader());
        node
    }

    #[test]
    fn test_need_entries_emitted_when_log_missing() {
        // Leader recovered with commit_index=10 but empty in-memory log.
        // When a follower needs entries starting from index 5,
        // the leader should emit NeedEntries (entries are in WAL, not memory).
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Set the follower's next_index to 5 (below persisted_commit_index).
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.next_index = LogIndex::new(5);
        }

        let outputs = node.send_append_entries(peer);

        // Should emit NeedEntries, not a regular AppendEntries.
        let need = outputs.iter().find(|o| matches!(o, RaftOutput::NeedEntries { .. }));
        assert!(need.is_some(), "Should emit NeedEntries for WAL-backed entries");

        if let RaftOutput::NeedEntries {
            follower_id,
            start_index,
            prev_log_index,
            ..
        } = need.unwrap()
        {
            assert_eq!(*follower_id, peer);
            assert_eq!(start_index.get(), 5);
            assert_eq!(prev_log_index.get(), 4);
        }
    }

    #[test]
    fn test_provide_entries_sends_append_entries() {
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Mark as pending (simulating that NeedEntries was emitted).
        node.wal_read_pending.insert(peer);

        // Provide entries as if read from WAL.
        let entries = vec![
            LogEntry::new(TermId::new(3), LogIndex::new(5), Bytes::from("cmd-5"), Bytes::new()),
            LogEntry::new(TermId::new(3), LogIndex::new(6), Bytes::from("cmd-6"), Bytes::new()),
        ];

        let outputs = node.provide_entries(
            peer,
            LogIndex::new(4),
            TermId::new(3),
            entries,
        );

        // Should produce an AppendEntries message.
        let ae = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::AppendEntries(req)) => Some(req),
            _ => None,
        });
        assert!(ae.is_some(), "Should send AppendEntries after provide_entries");

        let req = ae.unwrap();
        assert_eq!(req.prev_log_index.get(), 4);
        assert_eq!(req.prev_log_term.get(), 3);
        assert_eq!(req.entries.len(), 2);
        assert_eq!(req.entries[0].index.get(), 5);
        assert_eq!(req.entries[1].index.get(), 6);
    }

    #[test]
    fn test_wal_read_pending_suppresses_sends() {
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Set next_index below persisted_commit_index to trigger NeedEntries.
        if let Some(state) = node.replication_state.get_mut(&peer) {
            state.next_index = LogIndex::new(5);
        }

        // First call should emit NeedEntries and set pending.
        let outputs = node.send_append_entries(peer);
        assert!(
            outputs.iter().any(|o| matches!(o, RaftOutput::NeedEntries { .. })),
            "First call should emit NeedEntries"
        );

        // Second call should be suppressed (WAL read is pending).
        let outputs = node.send_append_entries(peer);
        assert!(
            outputs.is_empty(),
            "Should suppress sends while WAL read is pending"
        );
    }

    #[test]
    fn test_wal_read_pending_cleared_on_provide() {
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Mark as pending.
        node.wal_read_pending.insert(peer);
        assert!(node.wal_read_pending.contains(&peer));

        // Provide entries clears the pending flag.
        let _ = node.provide_entries(
            peer,
            LogIndex::new(4),
            TermId::new(3),
            vec![LogEntry::new(TermId::new(3), LogIndex::new(5), Bytes::from("cmd"), Bytes::new())],
        );

        assert!(
            !node.wal_read_pending.contains(&peer),
            "provide_entries should clear wal_read_pending"
        );
    }

    #[test]
    fn test_wal_read_pending_cleared_on_step_down() {
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Mark as pending.
        node.wal_read_pending.insert(peer);
        assert!(node.wal_read_pending.contains(&peer));

        // Step down clears pending.
        node.step_down(TermId::new(5));

        assert!(
            node.wal_read_pending.is_empty(),
            "step_down should clear wal_read_pending"
        );
    }

    #[test]
    fn test_follower_accepts_prev_at_persisted_commit() {
        // Follower recovered with commit_index=10 (in-memory log is empty).
        // Leader sends AppendEntries with prev_log_index=10. The follower
        // should accept because index 10 is within persisted_commit_index.
        let config = make_config(2);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        let req = AppendEntriesRequest::new(
            TermId::new(4),
            NodeId::new(1), // leader
            NodeId::new(2), // us
            LogIndex::new(10),
            TermId::new(3),
            vec![LogEntry::new(TermId::new(4), LogIndex::new(11), Bytes::from("data"), Bytes::new())],
            LogIndex::new(10),
        );

        let outputs = node.handle_message(Message::AppendEntries(req));

        let resp = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::AppendEntriesResponse(r)) => Some(r),
            _ => None,
        });
        assert!(resp.is_some());
        assert!(
            resp.unwrap().success,
            "Should accept when prev_log_index <= persisted_commit_index"
        );
    }

    #[test]
    fn test_follower_rejects_prev_above_persisted_commit() {
        // Follower recovered with commit_index=10 (in-memory log is empty).
        // Leader sends AppendEntries with prev_log_index=15. The follower
        // should REJECT because index 15 is above persisted_commit_index
        // and also not in the in-memory log.
        let config = make_config(2);
        let mut node = RaftNode::with_recovery_state(
            config,
            TermId::new(3),
            None,
            false,
            LogIndex::new(10),
            TermId::new(3),
        );

        let req = AppendEntriesRequest::new(
            TermId::new(4),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(15), // Above persisted commit and not in memory.
            TermId::new(3),
            vec![],
            LogIndex::new(10),
        );

        let outputs = node.handle_message(Message::AppendEntries(req));

        let resp = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::AppendEntriesResponse(r)) => Some(r),
            _ => None,
        });
        assert!(resp.is_some());
        assert!(
            !resp.unwrap().success,
            "Should reject when prev_log_index > persisted_commit_index and not in log"
        );
    }

    #[test]
    fn test_leader_fast_forward_on_rejection() {
        // Leader receives rejection with persisted_commit_index=8, match_index=0.
        // Fast-forward should set next_index = max(8+1, 0+1) = 9.
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        let resp = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            false,
            LogIndex::new(0),  // match_index
            LogIndex::new(8),  // persisted_commit_index
        );

        node.handle_message(Message::AppendEntriesResponse(resp));

        let state = node.replication_state.get(&peer).unwrap();
        // next_index should be max(8+1, 0+1) = 9.
        assert_eq!(
            state.next_index.get(),
            9,
            "Should fast-forward to persisted_commit_index + 1"
        );
    }

    // =========================================================================
    // Log compaction tests
    // =========================================================================

    /// Helper: creates a leader with `n` committed entries and a small
    /// trailing window for compaction tests.
    fn make_leader_with_committed(
        entry_count: u64,
        trailing: u64,
    ) -> RaftNode {
        let config = make_config(1).with_log_trailing_entries(trailing);
        let mut node = RaftNode::new(config);
        make_leader(&mut node);

        let peer = NodeId::new(2);

        // Propose entries.
        for i in 0..entry_count {
            let data = Bytes::from(format!("entry-{i}"));
            node.handle_client_request(ClientRequest::new(data, Bytes::new()));
        }

        // Ack from peer to commit (quorum = 2 in a 3-node cluster).
        // last_index includes the leader's no-op entry at index 1,
        // so entries are at indices 2..=entry_count+1.
        let match_idx = node.log.last_index();
        let resp = AppendEntriesResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            true,
            match_idx,
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp));

        // Postcondition: all entries committed and applied.
        assert_eq!(node.commit_index().get(), match_idx.get());
        assert_eq!(node.last_applied().get(), match_idx.get());
        node
    }

    #[test]
    fn test_compact_log_truncates_committed_entries() {
        let mut node = make_leader_with_committed(20, 5);

        let log_len_before = node.log.len();
        assert!(log_len_before > 5, "should have >5 entries before compact");

        let compacted = node.compact_log();
        assert!(compacted, "compact_log should have removed entries");

        // Log should be trimmed — at most trailing + a few entries.
        assert!(
            node.log.len() <= 10,
            "log should be trimmed, got {}",
            node.log.len()
        );
    }

    #[test]
    fn test_compact_log_respects_trailing_window() {
        let mut node = make_leader_with_committed(30, 10);

        node.compact_log();

        // Should retain at least trailing_window entries.
        assert!(
            node.log.len() >= 10,
            "should retain at least trailing entries, got {}",
            node.log.len()
        );

        // The last entry should still be accessible.
        let last = node.log.last_index();
        assert!(node.log.get(last).is_some(), "last entry should exist");
    }

    #[test]
    fn test_compact_log_respects_min_match_index() {
        let config = make_config(1).with_log_trailing_entries(5);
        let mut node = RaftNode::new(config);
        make_leader(&mut node);

        let peer2 = NodeId::new(2);
        let peer3 = NodeId::new(3);

        // Propose 20 entries.
        for i in 0..20 {
            let data = Bytes::from(format!("entry-{i}"));
            node.handle_client_request(ClientRequest::new(data, Bytes::new()));
        }

        // Peer2 has caught up to index 21, peer3 only to index 10.
        let resp2 = AppendEntriesResponse::new(
            node.current_term(),
            peer2,
            node.node_id(),
            true,
            node.log.last_index(),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp2));

        let resp3 = AppendEntriesResponse::new(
            node.current_term(),
            peer3,
            node.node_id(),
            true,
            LogIndex::new(10),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp3));

        // min(match_index) = 10. With trailing=5 and commit_index=21,
        // hard_cap = 21-5 = 16. compact_to = max(10, 16) = 16.
        // So entries up to 16 get removed.
        node.compact_log();

        let first = node.log.first_index();
        assert!(
            first.get() >= 10,
            "first_index should be >= min_match (10), got {}",
            first.get()
        );
    }

    #[test]
    fn test_compact_log_with_lagging_follower_enforces_cap() {
        let config = make_config(1).with_log_trailing_entries(5);
        let mut node = RaftNode::new(config);
        make_leader(&mut node);

        let peer2 = NodeId::new(2);
        let peer3 = NodeId::new(3);

        // Propose 30 entries.
        for i in 0..30 {
            let data = Bytes::from(format!("entry-{i}"));
            node.handle_client_request(ClientRequest::new(data, Bytes::new()));
        }

        // Peer2 fully caught up, peer3 stuck at index 2 (very lagging).
        let resp2 = AppendEntriesResponse::new(
            node.current_term(),
            peer2,
            node.node_id(),
            true,
            node.log.last_index(),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp2));

        let resp3 = AppendEntriesResponse::new(
            node.current_term(),
            peer3,
            node.node_id(),
            true,
            LogIndex::new(2),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp3));

        // commit_index = 31, trailing = 5, hard_cap = 26.
        // min_match = 2. compact_to = max(2, 26) = 26.
        // Hard cap overrides the lagging follower.
        node.compact_log();

        let first = node.log.first_index();
        assert!(
            first.get() > 2,
            "hard cap should override lagging follower, got first={}",
            first.get()
        );
    }

    #[test]
    fn test_compact_log_no_op_when_small() {
        // With only a few entries and a large trailing window, nothing
        // should be compacted.
        let mut node = make_leader_with_committed(5, 100);
        let compacted = node.compact_log();
        assert!(!compacted, "should not compact when log is small");
    }

    #[test]
    fn test_compact_log_follower_truncates() {
        let config = make_config(1).with_log_trailing_entries(5);
        let mut node = RaftNode::new(config);
        // Node stays as follower — simulate receiving entries via
        // AppendEntries from a leader.

        let leader = NodeId::new(2);
        let term = TermId::new(1);

        // Build entries as if received from leader.
        let mut entries = Vec::new();
        for i in 1..=25u64 {
            entries.push(LogEntry::new(
                term,
                LogIndex::new(i),
                Bytes::from(format!("data-{i}")),
                Bytes::new(),
            ));
        }

        let req = AppendEntriesRequest::new(
            term,
            leader,
            node.node_id(),
            LogIndex::new(0),
            TermId::new(0),
            entries,
            LogIndex::new(25),
        );
        node.handle_message(Message::AppendEntries(req));

        // Node is follower with last_applied=25, log len=25.
        assert_eq!(node.last_applied().get(), 25);
        assert_eq!(node.log.len(), 25);

        let compacted = node.compact_log_follower();
        assert!(compacted, "follower should compact");

        // compact_to = 25 - 5 = 20. Entries 1..=20 removed.
        assert_eq!(
            node.log.first_index().get(),
            21,
            "first_index should be 21 after compaction"
        );
        assert_eq!(node.log.len(), 5, "should have 5 trailing entries");
    }

    #[test]
    fn test_compact_log_follower_no_op_when_not_leader() {
        // compact_log() (leader path) should be a no-op for followers.
        let mut node = make_leader_with_committed(20, 5);

        // Step down.
        node.state = RaftState::Follower;
        let compacted = node.compact_log();
        assert!(!compacted, "compact_log should be no-op for follower");
    }

    #[test]
    fn test_need_entries_after_truncation() {
        let mut node = make_leader_with_committed(30, 5);

        // Compact: commit_index=31, trailing=5, so entries 1..=26 removed.
        node.compact_log();

        let peer3 = NodeId::new(3);
        // Simulate peer3 at next_index=10 (below compacted range).
        if let Some(state) = node.replication_state.get_mut(&peer3) {
            state.next_index = LogIndex::new(10);
            state.inflight_count = 0;
        }

        let outputs = node.send_append_entries(peer3);

        // Should emit NeedEntries because index 10 is below log's first_index.
        let need = outputs.iter().any(|o| {
            matches!(o, RaftOutput::NeedEntries { start_index, .. }
                if start_index.get() == 10)
        });
        assert!(
            need,
            "should emit NeedEntries for truncated entries"
        );
    }

    #[test]
    fn test_compaction_then_provide_entries() {
        let mut node = make_leader_with_committed(30, 5);

        node.compact_log();

        let peer3 = NodeId::new(3);
        // Simulate peer3 needing entries at index 10.
        if let Some(state) = node.replication_state.get_mut(&peer3) {
            state.next_index = LogIndex::new(10);
            state.inflight_count = 0;
        }

        let outputs = node.send_append_entries(peer3);

        // Find the NeedEntries output.
        let need_entries = outputs.iter().find_map(|o| {
            if let RaftOutput::NeedEntries {
                follower_id,
                start_index,
                prev_log_index,
                max_bytes,
            } = o
            {
                Some((*follower_id, *start_index, *prev_log_index, *max_bytes))
            } else {
                None
            }
        });
        assert!(need_entries.is_some(), "should emit NeedEntries");

        let (follower_id, start_index, prev_log_index, _max_bytes) =
            need_entries.unwrap();

        // Simulate WAL providing entries.
        let wal_entries: Vec<LogEntry> = (start_index.get()..=15)
            .map(|i| {
                LogEntry::new(
                    TermId::new(1),
                    LogIndex::new(i),
                    Bytes::from(format!("wal-{i}")),
                    Bytes::new(),
                )
            })
            .collect();

        let prev_term = TermId::new(1);
        let outputs = node.provide_entries(
            follower_id,
            prev_log_index,
            prev_term,
            wal_entries,
        );

        // Should produce an AppendEntries message.
        let sent = outputs.iter().any(|o| {
            matches!(o, RaftOutput::SendMessage(Message::AppendEntries(_)))
        });
        assert!(sent, "should send AppendEntries from WAL entries");
    }

    #[test]
    fn test_compaction_preserves_term_at_boundary() {
        // After compaction, term_at(compact_to) should still return
        // the correct term for prev_log_term lookups.
        let mut node = make_leader_with_committed(20, 5);

        let last_before = node.log.last_index();
        node.compact_log();

        let first_after = node.log.first_index();
        assert!(first_after.get() > 1, "should have compacted");

        // The entry just below first_after should be at compacted_index.
        let boundary = LogIndex::new(first_after.get() - 1);
        let term = node.log.term_at(boundary);
        assert!(
            term.get() > 0,
            "term_at boundary should return non-zero term, got {}",
            term.get()
        );

        // Last entry should still be accessible.
        assert!(node.log.get(last_before).is_some());
    }

    #[test]
    fn test_min_match_index_leader_only() {
        let config = make_config(1);
        let node = RaftNode::new(config);

        // Not a leader — should return None.
        assert!(node.min_match_index().is_none());
    }

    #[test]
    fn test_min_match_index_tracks_lagging_follower() {
        let config = make_config(1);
        let mut node = RaftNode::new(config);
        make_leader(&mut node);

        let peer2 = NodeId::new(2);
        let peer3 = NodeId::new(3);

        // Propose entries so there's something to replicate.
        for i in 0..5 {
            let data = Bytes::from(format!("entry-{i}"));
            node.handle_client_request(ClientRequest::new(data, Bytes::new()));
        }

        // Peer2 catches up to index 6, peer3 only to 3.
        let resp2 = AppendEntriesResponse::new(
            node.current_term(),
            peer2,
            node.node_id(),
            true,
            LogIndex::new(6),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp2));

        let resp3 = AppendEntriesResponse::new(
            node.current_term(),
            peer3,
            node.node_id(),
            true,
            LogIndex::new(3),
            LogIndex::new(0),
        );
        node.handle_message(Message::AppendEntriesResponse(resp3));

        // min_match_index should be 3 (the lagging follower).
        let min = node.min_match_index().expect("leader should have min_match");
        assert_eq!(min.get(), 3);
    }

    #[test]
    fn test_provide_snapshot_sends_install_snapshot() {
        // Leader with a follower whose next_index is below WAL floor.
        // provide_snapshot should clear wal_read_pending, advance next_index,
        // increment inflight_count, and emit InstallSnapshot.
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Simulate that NeedEntries was emitted and WAL read is pending.
        node.wal_read_pending.insert(peer);
        assert!(node.wal_read_pending.contains(&peer));

        // Record inflight_count before the call.
        let inflight_before = node.replication_state.get(&peer).unwrap().inflight_count;

        // Provide the snapshot boundary — WAL floor is at index 5, term 2.
        let outputs = node.provide_snapshot(peer, LogIndex::new(5), TermId::new(2));

        // wal_read_pending must be cleared.
        assert!(
            !node.wal_read_pending.contains(&peer),
            "provide_snapshot must clear wal_read_pending"
        );

        // next_index must advance to floor + 1.
        let repl_state = node.replication_state.get(&peer).expect("peer must exist");
        assert_eq!(
            repl_state.next_index.get(),
            6,
            "next_index must be last_included_index + 1"
        );

        // inflight_count must be incremented by exactly 1.
        assert_eq!(
            repl_state.inflight_count,
            inflight_before + 1,
            "inflight_count must be incremented by 1"
        );

        // An InstallSnapshot message must be emitted.
        let snap = outputs.iter().find_map(|o| match o {
            RaftOutput::SendMessage(Message::InstallSnapshot(req)) => Some(req),
            _ => None,
        });
        assert!(snap.is_some(), "provide_snapshot must emit InstallSnapshot");

        let req = snap.unwrap();
        assert_eq!(req.to, peer, "InstallSnapshot must target the follower");
        assert_eq!(req.last_included_index.get(), 5, "last_included_index must match floor");
        assert_eq!(req.last_included_term.get(), 2, "last_included_term must match floor term");
        assert_eq!(req.term, node.current_term(), "InstallSnapshot must carry current term");
        assert!(req.done, "single-chunk snapshot must have done=true");

        // next_index must advance to floor + 1.
        let repl_state = node.replication_state.get(&peer).expect("peer must exist");
        assert_eq!(
            repl_state.next_index.get(),
            6,
            "next_index must be last_included_index + 1"
        );
    }

    #[test]
    fn test_provide_snapshot_clears_pending_even_as_follower() {
        // provide_snapshot should clear wal_read_pending even if the node has
        // stepped down, so the flag doesn't leak across term boundaries.
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        node.wal_read_pending.insert(peer);

        // Step down to follower before calling provide_snapshot.
        node.step_down(TermId::new(5));
        assert_eq!(node.state, RaftState::Follower);

        // Re-insert the flag (step_down already clears it, so simulate a race).
        node.wal_read_pending.insert(peer);

        let outputs = node.provide_snapshot(peer, LogIndex::new(5), TermId::new(2));

        assert!(
            !node.wal_read_pending.contains(&peer),
            "provide_snapshot must clear wal_read_pending even as follower"
        );
        // Non-leader must not emit messages.
        assert!(
            outputs.iter().all(|o| !matches!(o, RaftOutput::SendMessage(_))),
            "non-leader must not send messages from provide_snapshot"
        );
    }

    #[test]
    fn test_install_snapshot_response_decrements_inflight() {
        // After the follower acknowledges the snapshot, inflight_count should
        // drop back to zero so AppendEntries pipelining can proceed.
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        // Simulate an in-flight snapshot.
        node.wal_read_pending.insert(peer);
        let inflight_before = node.replication_state.get(&peer).unwrap().inflight_count;
        let _ = node.provide_snapshot(peer, LogIndex::new(5), TermId::new(2));

        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(
            state.inflight_count,
            inflight_before + 1,
            "precondition: inflight_count must have incremented"
        );
        let inflight_after_provide = state.inflight_count;

        // Receive a success response from the follower.
        let resp = InstallSnapshotResponse::complete(
            node.current_term(),
            peer,
            node.node_id(),
        );
        node.handle_message(Message::InstallSnapshotResponse(resp));

        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(
            state.inflight_count,
            inflight_after_provide - 1,
            "inflight_count must be decremented on success response"
        );
    }

    #[test]
    fn test_install_snapshot_response_ignored_on_failure() {
        // A failed response must NOT decrement inflight_count.
        let mut node = make_recovered_leader(10, 3);
        let peer = NodeId::new(2);

        node.wal_read_pending.insert(peer);
        let _ = node.provide_snapshot(peer, LogIndex::new(5), TermId::new(2));

        let state = node.replication_state.get(&peer).unwrap();
        let inflight_after_provide = state.inflight_count;

        let resp = InstallSnapshotResponse::new(
            node.current_term(),
            peer,
            node.node_id(),
            false, // not success
            0,
        );
        node.handle_message(Message::InstallSnapshotResponse(resp));

        let state = node.replication_state.get(&peer).unwrap();
        assert_eq!(
            state.inflight_count,
            inflight_after_provide,
            "failed response must NOT decrement inflight_count"
        );
    }
}

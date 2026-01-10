-------------------------------- MODULE raft --------------------------------
\* Helix Raft Consensus Specification
\*
\* This specification models the core Raft consensus algorithm focusing on
\* leader election and log replication. It is designed to be model-checked
\* with TLC to verify safety properties before implementation.
\*
\* Based on the Raft paper by Ongaro and Ousterhout, with adaptations for
\* Helix's specific requirements.

EXTENDS Naturals, Sequences, FiniteSets, TLC

\* ============================================================================
\* CONSTANTS
\* ============================================================================

\* The set of server IDs
CONSTANTS Servers

\* The maximum term number to explore (for model checking)
CONSTANTS MaxTerm

\* The maximum log length to explore (for model checking)
CONSTANTS MaxLogLength

\* A distinguished value for "no vote"
CONSTANTS Nil

\* ============================================================================
\* VARIABLES
\* ============================================================================

\* Persistent state on all servers
VARIABLES currentTerm,  \* Latest term server has seen (persisted)
          votedFor,     \* CandidateId that received vote in current term (persisted)
          log           \* Log entries; each entry is [term |-> t, value |-> v]

\* Volatile state on all servers
VARIABLES commitIndex,  \* Index of highest log entry known to be committed
          state         \* One of: "follower", "candidate", "leader"

\* Volatile state on leaders (reinitialized after election)
VARIABLES nextIndex,    \* For each server, index of next log entry to send
          matchIndex    \* For each server, index of highest log entry known to be replicated

\* Message channels (for modeling network)
VARIABLES messages      \* Set of messages in flight

\* Auxiliary variables for specification/model checking
VARIABLES elections,    \* History of elections for property checking
          committed     \* Set of committed entries for checking

vars == <<currentTerm, votedFor, log, commitIndex, state,
          nextIndex, matchIndex, messages, elections, committed>>

\* ============================================================================
\* HELPERS
\* ============================================================================

\* Minimum of a set of numbers
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* Maximum of a set of numbers
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

\* The set of all quorums (majorities)
Quorum == {Q \in SUBSET Servers : Cardinality(Q) * 2 > Cardinality(Servers)}

\* The term of the last log entry, or 0 if log is empty
LastTerm(s) == IF Len(log[s]) > 0 THEN log[s][Len(log[s])].term ELSE 0

\* Is log A at least as up-to-date as log B?
LogIsAtLeastAsUpToDate(termA, lastIdxA, termB, lastIdxB) ==
    \/ termA > termB
    \/ (termA = termB /\ lastIdxA >= lastIdxB)

\* ============================================================================
\* MESSAGE TYPES
\* ============================================================================

\* Request vote RPC
RequestVoteRequest(src, dst, term, lastLogIndex, lastLogTerm) ==
    [type        |-> "RequestVote",
     term        |-> term,
     src         |-> src,
     dst         |-> dst,
     lastLogIdx  |-> lastLogIndex,
     lastLogTerm |-> lastLogTerm]

RequestVoteResponse(src, dst, term, granted) ==
    [type    |-> "RequestVoteResponse",
     term    |-> term,
     src     |-> src,
     dst     |-> dst,
     granted |-> granted]

\* Append entries RPC
AppendEntriesRequest(src, dst, term, prevIdx, prevTerm, entries, leaderCommit) ==
    [type         |-> "AppendEntries",
     term         |-> term,
     src          |-> src,
     dst          |-> dst,
     prevLogIdx   |-> prevIdx,
     prevLogTerm  |-> prevTerm,
     entries      |-> entries,
     leaderCommit |-> leaderCommit]

AppendEntriesResponse(src, dst, term, success, matchIdx) ==
    [type     |-> "AppendEntriesResponse",
     term     |-> term,
     src      |-> src,
     dst      |-> dst,
     success  |-> success,
     matchIdx |-> matchIdx]

\* ============================================================================
\* ACTIONS
\* ============================================================================

\* Server s times out and starts an election
StartElection(s) ==
    /\ state[s] \in {"follower", "candidate"}
    /\ currentTerm[s] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ state' = [state EXCEPT ![s] = "candidate"]
    /\ messages' = messages \cup
        {RequestVoteRequest(s, d, currentTerm[s] + 1,
                           Len(log[s]), LastTerm(s)) : d \in Servers \ {s}}
    /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, elections, committed>>

\* Server s receives a RequestVote request from candidate c
HandleRequestVote(s, m) ==
    /\ m.type = "RequestVote"
    /\ m.dst = s
    /\ LET grant == /\ m.term >= currentTerm[s]
                    /\ (votedFor[s] = Nil \/ votedFor[s] = m.src)
                    /\ LogIsAtLeastAsUpToDate(m.lastLogTerm, m.lastLogIdx,
                                              LastTerm(s), Len(log[s]))
       IN
       /\ IF m.term > currentTerm[s]
          THEN /\ currentTerm' = [currentTerm EXCEPT ![s] = m.term]
               /\ state' = [state EXCEPT ![s] = "follower"]
               /\ votedFor' = [votedFor EXCEPT ![s] = IF grant THEN m.src ELSE Nil]
          ELSE /\ votedFor' = [votedFor EXCEPT ![s] = IF grant THEN m.src ELSE votedFor[s]]
               /\ UNCHANGED <<currentTerm, state>>
       /\ messages' = (messages \ {m}) \cup
            {RequestVoteResponse(s, m.src,
                IF m.term > currentTerm[s] THEN m.term ELSE currentTerm[s],
                grant)}
    /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, elections, committed>>

\* Candidate s receives enough votes to become leader
BecomeLeader(s) ==
    /\ state[s] = "candidate"
    /\ LET votesReceived == {m.src : m \in {m \in messages :
            m.type = "RequestVoteResponse" /\ m.dst = s /\
            m.term = currentTerm[s] /\ m.granted}} \cup {s}
       IN Cardinality(votesReceived) * 2 > Cardinality(Servers)
    /\ state' = [state EXCEPT ![s] = "leader"]
    /\ nextIndex' = [nextIndex EXCEPT ![s] = [d \in Servers |-> Len(log[s]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![s] = [d \in Servers |-> 0]]
    /\ elections' = elections \cup {[term |-> currentTerm[s], leader |-> s]}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages, committed>>

\* Leader s appends a new entry to its log (client request)
ClientRequest(s, v) ==
    /\ state[s] = "leader"
    /\ Len(log[s]) < MaxLogLength
    /\ log' = [log EXCEPT ![s] = Append(@, [term |-> currentTerm[s], value |-> v])]
    /\ UNCHANGED <<currentTerm, votedFor, commitIndex, state,
                   nextIndex, matchIndex, messages, elections, committed>>

\* Leader s sends AppendEntries to follower d
SendAppendEntries(s, d) ==
    /\ state[s] = "leader"
    /\ s # d
    /\ LET prevIdx == nextIndex[s][d] - 1
           prevTerm == IF prevIdx > 0 /\ prevIdx <= Len(log[s])
                       THEN log[s][prevIdx].term ELSE 0
           entries == IF nextIndex[s][d] <= Len(log[s])
                      THEN SubSeq(log[s], nextIndex[s][d], Len(log[s]))
                      ELSE <<>>
       IN messages' = messages \cup
            {AppendEntriesRequest(s, d, currentTerm[s], prevIdx, prevTerm,
                                  entries, commitIndex[s])}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, state,
                   nextIndex, matchIndex, elections, committed>>

\* Server s handles an AppendEntries request
HandleAppendEntries(s, m) ==
    /\ m.type = "AppendEntries"
    /\ m.dst = s
    /\ IF m.term < currentTerm[s]
       THEN \* Reject stale request
            /\ messages' = (messages \ {m}) \cup
                {AppendEntriesResponse(s, m.src, currentTerm[s], FALSE, 0)}
            /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, state,
                          nextIndex, matchIndex, elections, committed>>
       ELSE \* Valid request
            /\ IF m.term > currentTerm[s]
               THEN currentTerm' = [currentTerm EXCEPT ![s] = m.term]
               ELSE UNCHANGED currentTerm
            /\ state' = [state EXCEPT ![s] = "follower"]
            /\ votedFor' = [votedFor EXCEPT ![s] = IF m.term > currentTerm[s] THEN Nil ELSE votedFor[s]]
            /\ LET logOK == \/ m.prevLogIdx = 0
                            \/ (m.prevLogIdx <= Len(log[s]) /\
                                log[s][m.prevLogIdx].term = m.prevLogTerm)
               IN IF logOK
                  THEN \* Append entries
                       /\ log' = [log EXCEPT ![s] =
                            SubSeq(@, 1, m.prevLogIdx) \o m.entries]
                       /\ commitIndex' = [commitIndex EXCEPT ![s] =
                            IF m.leaderCommit > commitIndex[s]
                            THEN Min({m.leaderCommit, Len(log'[s])})
                            ELSE commitIndex[s]]
                       /\ messages' = (messages \ {m}) \cup
                            {AppendEntriesResponse(s, m.src, m.term, TRUE,
                                m.prevLogIdx + Len(m.entries))}
                       /\ UNCHANGED <<nextIndex, matchIndex, elections, committed>>
                  ELSE \* Log mismatch
                       /\ messages' = (messages \ {m}) \cup
                            {AppendEntriesResponse(s, m.src, m.term, FALSE, 0)}
                       /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex,
                                     elections, committed>>

\* Leader s handles an AppendEntries response from follower
HandleAppendEntriesResponse(s, m) ==
    /\ m.type = "AppendEntriesResponse"
    /\ m.dst = s
    /\ state[s] = "leader"
    /\ m.term = currentTerm[s]
    /\ IF m.success
       THEN /\ nextIndex' = [nextIndex EXCEPT ![s][m.src] = m.matchIdx + 1]
            /\ matchIndex' = [matchIndex EXCEPT ![s][m.src] = m.matchIdx]
       ELSE /\ nextIndex' = [nextIndex EXCEPT ![s][m.src] =
                Max({1, nextIndex[s][m.src] - 1})]
            /\ UNCHANGED matchIndex
    /\ messages' = messages \ {m}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, state, elections, committed>>

\* Leader s advances commit index
\* An index n can be committed if:
\*   - n > current commitIndex
\*   - Entry at n is from current term
\*   - A majority have replicated index n
AdvanceCommitIndex(s) ==
    /\ state[s] = "leader"
    /\ Len(log[s]) > 0
    \* Define the set of indices that could become the new commit index
    /\ LET committableIndices == {n \in (commitIndex[s]+1)..Len(log[s]) :
               /\ log[s][n].term = currentTerm[s]
               /\ Cardinality({d \in Servers : matchIndex[s][d] >= n}) * 2 > Cardinality(Servers)}
       IN
       \* Only proceed if there's something to commit
       /\ committableIndices # {}
       /\ LET newCommitIdx == Max(committableIndices)
          IN /\ commitIndex' = [commitIndex EXCEPT ![s] = newCommitIdx]
             /\ committed' = committed \cup
                  {[idx |-> i, term |-> log[s][i].term, value |-> log[s][i].value] :
                   i \in (commitIndex[s]+1)..newCommitIdx}
    /\ UNCHANGED <<currentTerm, votedFor, log, state, nextIndex, matchIndex, messages, elections>>

\* ============================================================================
\* SPECIFICATION
\* ============================================================================

Init ==
    /\ currentTerm = [s \in Servers |-> 0]
    /\ votedFor = [s \in Servers |-> Nil]
    /\ log = [s \in Servers |-> <<>>]
    /\ commitIndex = [s \in Servers |-> 0]
    /\ state = [s \in Servers |-> "follower"]
    /\ nextIndex = [s \in Servers |-> [d \in Servers |-> 1]]
    /\ matchIndex = [s \in Servers |-> [d \in Servers |-> 0]]
    /\ messages = {}
    /\ elections = {}
    /\ committed = {}

Next ==
    \/ \E s \in Servers : StartElection(s)
    \/ \E s \in Servers, m \in messages : HandleRequestVote(s, m)
    \/ \E s \in Servers : BecomeLeader(s)
    \/ \E s \in Servers, v \in 1..3 : ClientRequest(s, v)  \* Values 1-3 for testing
    \/ \E s, d \in Servers : SendAppendEntries(s, d)
    \/ \E s \in Servers, m \in messages : HandleAppendEntries(s, m)
    \/ \E s \in Servers, m \in messages : HandleAppendEntriesResponse(s, m)
    \/ \E s \in Servers : AdvanceCommitIndex(s)

Spec == Init /\ [][Next]_vars

\* ============================================================================
\* SAFETY PROPERTIES
\* ============================================================================

\* SingleLeaderPerTerm: At most one leader per term
SingleLeaderPerTerm ==
    \A e1, e2 \in elections :
        e1.term = e2.term => e1.leader = e2.leader

\* LogMatching: If two logs contain an entry with same index and term,
\* the logs are identical up through that index
LogMatching ==
    \A s1, s2 \in Servers :
        \A i \in 1..Min({Len(log[s1]), Len(log[s2])}) :
            log[s1][i].term = log[s2][i].term =>
                \A j \in 1..i : log[s1][j] = log[s2][j]

\* LeaderCompleteness: If an entry is committed, it will be in all future leaders' logs
\* (Checked via committed set)
LeaderCompleteness ==
    \A c \in committed :
        \A e \in elections :
            e.term > c.term =>
                \E s \in Servers :
                    /\ state[s] = "leader"
                    /\ currentTerm[s] = e.term
                    => c.idx <= Len(log[s]) /\ log[s][c.idx] = [term |-> c.term, value |-> c.value]

\* StateMachineSafety: No two different values committed at the same index
StateMachineSafety ==
    \A c1, c2 \in committed :
        c1.idx = c2.idx => c1.value = c2.value

\* Combined safety property
Safety == SingleLeaderPerTerm /\ LogMatching /\ StateMachineSafety

\* ============================================================================
\* INVARIANTS FOR MODEL CHECKING
\* ============================================================================

TypeOK ==
    /\ currentTerm \in [Servers -> Nat]
    /\ votedFor \in [Servers -> Servers \cup {Nil}]
    /\ state \in [Servers -> {"follower", "candidate", "leader"}]
    /\ commitIndex \in [Servers -> Nat]

=============================================================================

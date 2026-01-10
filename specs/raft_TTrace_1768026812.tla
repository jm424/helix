---- MODULE raft_TTrace_1768026812 ----
EXTENDS Sequences, raft_TEConstants, TLCExt, Toolbox, raft, Naturals, TLC

_expression ==
    LET raft_TEExpression == INSTANCE raft_TEExpression
    IN raft_TEExpression!expression
----

_trace ==
    LET raft_TETrace == INSTANCE raft_TETrace
    IN raft_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        votedFor = ((s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"))
        /\
        committed = ({})
        /\
        currentTerm = ((s1 :> 1 @@ s2 :> 1 @@ s3 :> 0))
        /\
        log = ((s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>))
        /\
        messages = ({[term |-> 1, src |-> s2, dst |-> s1, type |-> "RequestVoteResponse", granted |-> TRUE], [term |-> 1, src |-> s1, dst |-> s3, lastLogTerm |-> 0, type |-> "RequestVote", lastLogIdx |-> 0]})
        /\
        matchIndex = ((s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)))
        /\
        state = ((s1 :> "leader" @@ s2 :> "follower" @@ s3 :> "follower"))
        /\
        nextIndex = ((s1 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s2 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s3 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1)))
        /\
        elections = ({[term |-> 1, leader |-> s1]})
        /\
        commitIndex = ((s1 :> 0 @@ s2 :> 0 @@ s3 :> 0))
    )
----

_init ==
    /\ elections = _TETrace[1].elections
    /\ messages = _TETrace[1].messages
    /\ matchIndex = _TETrace[1].matchIndex
    /\ log = _TETrace[1].log
    /\ state = _TETrace[1].state
    /\ commitIndex = _TETrace[1].commitIndex
    /\ currentTerm = _TETrace[1].currentTerm
    /\ committed = _TETrace[1].committed
    /\ nextIndex = _TETrace[1].nextIndex
    /\ votedFor = _TETrace[1].votedFor
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ elections  = _TETrace[i].elections
        /\ elections' = _TETrace[j].elections
        /\ messages  = _TETrace[i].messages
        /\ messages' = _TETrace[j].messages
        /\ matchIndex  = _TETrace[i].matchIndex
        /\ matchIndex' = _TETrace[j].matchIndex
        /\ log  = _TETrace[i].log
        /\ log' = _TETrace[j].log
        /\ state  = _TETrace[i].state
        /\ state' = _TETrace[j].state
        /\ commitIndex  = _TETrace[i].commitIndex
        /\ commitIndex' = _TETrace[j].commitIndex
        /\ currentTerm  = _TETrace[i].currentTerm
        /\ currentTerm' = _TETrace[j].currentTerm
        /\ committed  = _TETrace[i].committed
        /\ committed' = _TETrace[j].committed
        /\ nextIndex  = _TETrace[i].nextIndex
        /\ nextIndex' = _TETrace[j].nextIndex
        /\ votedFor  = _TETrace[i].votedFor
        /\ votedFor' = _TETrace[j].votedFor

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("raft_TTrace_1768026812.json", _TETrace)

=============================================================================

 Note that you can extract this module `raft_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `raft_TEExpression.tla` file takes precedence 
  over the module `raft_TEExpression` below).

---- MODULE raft_TEExpression ----
EXTENDS Sequences, raft_TEConstants, TLCExt, Toolbox, raft, Naturals, TLC

expression == 
    [
        \* To hide variables of the `raft` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        elections |-> elections
        ,messages |-> messages
        ,matchIndex |-> matchIndex
        ,log |-> log
        ,state |-> state
        ,commitIndex |-> commitIndex
        ,currentTerm |-> currentTerm
        ,committed |-> committed
        ,nextIndex |-> nextIndex
        ,votedFor |-> votedFor
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_electionsUnchanged |-> elections = elections'
        
        \* Format the `elections` variable as Json value.
        \* ,_electionsJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(elections)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_electionsModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].elections # _TETrace[s-1].elections
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE raft_TETrace ----
\*EXTENDS IOUtils, raft_TEConstants, raft, TLC
\*
\*trace == IODeserialize("raft_TTrace_1768026812.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE raft_TETrace ----
EXTENDS raft_TEConstants, raft, TLC

trace == 
    <<
    ([votedFor |-> (s1 :> "nil" @@ s2 :> "nil" @@ s3 :> "nil"),committed |-> {},currentTerm |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),messages |-> {},matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),state |-> (s1 :> "follower" @@ s2 :> "follower" @@ s3 :> "follower"),nextIndex |-> (s1 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s2 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s3 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1)),elections |-> {},commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([votedFor |-> (s1 :> s1 @@ s2 :> "nil" @@ s3 :> "nil"),committed |-> {},currentTerm |-> (s1 :> 1 @@ s2 :> 0 @@ s3 :> 0),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),messages |-> {[term |-> 1, src |-> s1, dst |-> s2, lastLogTerm |-> 0, type |-> "RequestVote", lastLogIdx |-> 0], [term |-> 1, src |-> s1, dst |-> s3, lastLogTerm |-> 0, type |-> "RequestVote", lastLogIdx |-> 0]},matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),state |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),nextIndex |-> (s1 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s2 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s3 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1)),elections |-> {},commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([votedFor |-> (s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"),committed |-> {},currentTerm |-> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 0),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),messages |-> {[term |-> 1, src |-> s2, dst |-> s1, type |-> "RequestVoteResponse", granted |-> TRUE], [term |-> 1, src |-> s1, dst |-> s3, lastLogTerm |-> 0, type |-> "RequestVote", lastLogIdx |-> 0]},matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),state |-> (s1 :> "candidate" @@ s2 :> "follower" @@ s3 :> "follower"),nextIndex |-> (s1 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s2 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s3 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1)),elections |-> {},commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)]),
    ([votedFor |-> (s1 :> s1 @@ s2 :> s1 @@ s3 :> "nil"),committed |-> {},currentTerm |-> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 0),log |-> (s1 :> <<>> @@ s2 :> <<>> @@ s3 :> <<>>),messages |-> {[term |-> 1, src |-> s2, dst |-> s1, type |-> "RequestVoteResponse", granted |-> TRUE], [term |-> 1, src |-> s1, dst |-> s3, lastLogTerm |-> 0, type |-> "RequestVote", lastLogIdx |-> 0]},matchIndex |-> (s1 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s2 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0) @@ s3 :> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)),state |-> (s1 :> "leader" @@ s2 :> "follower" @@ s3 :> "follower"),nextIndex |-> (s1 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s2 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1) @@ s3 :> (s1 :> 1 @@ s2 :> 1 @@ s3 :> 1)),elections |-> {[term |-> 1, leader |-> s1]},commitIndex |-> (s1 :> 0 @@ s2 :> 0 @@ s3 :> 0)])
    >>
----


=============================================================================

---- MODULE raft_TEConstants ----
EXTENDS raft

CONSTANTS s1, s2, s3

=============================================================================

---- CONFIG raft_TTrace_1768026812 ----
CONSTANTS
    Servers = { s1 , s2 , s3 }
    MaxTerm = 3
    MaxLogLength = 4
    Nil = "nil"
    s3 = s3
    s2 = s2
    s1 = s1

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Sat Jan 10 01:33:33 EST 2026